package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gomaja/go-m3ua"
)

var fixtureHTTPClient = &http.Client{Timeout: 5 * time.Second}

type combinedResult struct {
	Phase       string        `json:"phase"`
	Warmup      *cohortResult `json:"warmup,omitempty"`
	Measurement *cohortResult `json:"measurement,omitempty"`
	Sender      runRecord     `json:"sender"`
	Receiver    runRecord     `json:"receiver"`
	Verdict     string        `json:"verdict"`
	Error       string        `json:"error,omitempty"`
}

type cohortResult struct {
	Phase           string     `json:"phase"`
	Sender          runRecord  `json:"sender"`
	Receiver        runRecord  `json:"receiver"`
	ReverseSender   *runRecord `json:"reverse_sender,omitempty"`
	ReverseReceiver *runRecord `json:"reverse_receiver,omitempty"`
	Verdict         string     `json:"verdict"`
	Error           string     `json:"error,omitempty"`
	// validityOnly records that every error the cohort reported, in either
	// direction, is errCohortInvalid: the cohort ran and failed only its own
	// validity rules. It is not serialized.
	validityOnly bool
}

// errCohortInvalid is the whole error of a cohort that failed only its own
// validity rules. internal/cmd/perfcapacity recognises a failed warm-up by it.
var errCohortInvalid = errors.New("cohort is invalid; inspect machine-readable reasons")

type sendJob struct {
	identity  messageIdentity
	scheduled time.Time
	clock     *sharedRunClock
	offset    time.Duration
	size      int
	reverse   bool
}

// globalIndex is the unique schedule position of a message identity inside
// its cohort, matching the receiver ledger's identity arithmetic.
func globalIndex(identity messageIdentity) uint64 {
	return identity.Sequence*uint64(flowCount) + uint64(identity.Flow)
}

type senderCounters struct {
	mutex       sync.Mutex
	scheduled   uint64
	submitted   uint64
	sendErrors  uint64
	capped      uint64
	outstanding uint64
	fatal       string
	sendTime    *durationHistogram
	dispatchLag *durationHistogram
	series      []seriesPoint
	limit       uint64
	// overload is the outcome accounting of an overload measurement cohort,
	// nil for every other cohort.
	overload *overloadCounters
	// drainOutcomes makes a send the drain deadline cut off a counted
	// outcome, unsubmitted, instead of a fatal error. Only nominal cohorts set
	// it; the overload trial keeps its own contract.
	drainOutcomes bool
	unsubmitted   uint64
}

func newSenderCounters(limit int) *senderCounters {
	return &senderCounters{sendTime: newDurationHistogram(), dispatchLag: newDurationHistogram(), limit: uint64(limit)}
}

func runSender(ctx context.Context, config commandConfig) (combinedResult, error) {
	if routedMode(config.Mode) {
		return runRoutedSender(ctx, config)
	}
	endpoint, err := m3ua.NewEndpoint(senderEndpointConfig(config))
	if err != nil {
		return combinedResult{}, fmt.Errorf("create standalone ASP endpoint: %w", err)
	}
	defer func() { _ = endpoint.Close() }()
	// The listener (ASP-listen initiation) stays open for the whole run:
	// closing it closes every association it accepted.
	associations, releaseAssociations, err := establishSenderAssociations(ctx, config, m3uaConnector{endpoint: endpoint})
	if err != nil {
		return combinedResult{}, err
	}
	defer releaseAssociations()
	if err := waitForReady(ctx, config.PeerControl, config.Associations); err != nil {
		return combinedResult{}, err
	}
	config.ssnmRun, err = startSSNMLoad(ctx, config, endpoint, associations)
	if err != nil {
		return combinedResult{}, err
	}
	defer config.ssnmRun.close()
	var registry *echoRegistry
	if config.Mode == modeEcho {
		registry = newEchoRegistry()
		for index, association := range associations {
			go readEchoReplies(ctx, index, association, registry)
		}
	}
	var local *localReceiver
	if config.Mode == modeBidirectional {
		var err error
		local, err = startLocalReceiver(ctx, config, associations)
		if err != nil {
			return combinedResult{}, err
		}
		defer local.shutdown()
	}
	runCohort := func(cohortConfig commandConfig, phase, cohort string, duration time.Duration) (cohortResult, error) {
		if phase == "warmup" {
			cohortConfig.ssnmPhase = ssnmPhaseWarmup
		}
		if cohortConfig.overload != nil {
			// An overload trial warms up loss-free at the profile's recovery
			// rate; only its measurement cohort follows the phases.
			cohortConfig.overloadRole = overloadRoleMeasurement
			if phase == "warmup" {
				cohortConfig.overloadRole = overloadRoleWarmup
				cohortConfig.Rate = cohortConfig.overload.warmupRate()
			}
		}
		sender, receiver, err := runSenderCohort(ctx, cohortConfig, associations, registry, cohort, duration)
		result := newCohortResult(phase, sender, receiver, err)
		if config.Mode == modeBidirectional {
			finishBidirectionalCohort(ctx, cohortConfig, &result, local)
		}
		return result, err
	}
	return runWarmupAndMeasurement(config, runCohort, func(measurement *cohortResult) {
		config.ssnmRun.finish(ctx, measurement)
	})
}

// finishBidirectionalCohort completes a bidirectional cohort, warm-up or
// measurement: it collects the reverse direction's records from the SGP and
// fails the cohort if the ASP-local receiver of that direction reported a
// read fault.
func finishBidirectionalCohort(ctx context.Context, config commandConfig, result *cohortResult, local *localReceiver) {
	collectReverse(ctx, config, result)
	foldLocalFault(result, local.fault())
}

// foldLocalFault fails a bidirectional cohort, warm-up or measurement, whose
// ASP-local receiver, the receiver of the reverse direction, reported a read
// fault. The fault joins the cohort's other errors and is never taken for the
// validity failure of an overloaded direction.
func foldLocalFault(result *cohortResult, fault string) {
	if fault == "" {
		return
	}
	result.Verdict = verdictInvalid
	result.validityOnly = false
	result.Error = joinErrorText(result.Error, "reverse cohort local receiver: "+fault)
}

// cohortRunner runs one cohort of the configured workload against the
// receiver and returns its paired records.
type cohortRunner func(cohortConfig commandConfig, phase, cohort string, duration time.Duration) (cohortResult, error)

// runWarmupAndMeasurement runs the optional warm-up cohort and then the
// measurement cohort. A warm-up that does not drain cleanly ends the run with
// both raw warm-up records. inspect may amend the measurement cohort before
// the combined result is assembled.
func runWarmupAndMeasurement(config commandConfig, runCohort cohortRunner, inspect func(*cohortResult)) (combinedResult, error) {
	var warmup *cohortResult
	if config.Warmup > 0 {
		warmupConfig := config
		warmupConfig.Cohort += "-warmup"
		warmupResult, warmupErr := runCohort(warmupConfig, "warmup", warmupConfig.Cohort, config.Warmup)
		warmup = &warmupResult
		if warmupErr != nil || warmupResult.Verdict == verdictInvalid {
			if warmupErr == nil {
				warmupErr = errors.New("warmup cohort is invalid")
			}
			return failedWarmupResult(warmupResult, warmupErr), warmupErr
		}
	}
	measurement, err := runCohort(config, "measurement", config.Cohort, config.Duration)
	if inspect != nil {
		inspect(&measurement)
	}
	result := combinedResult{
		Phase:       "measurement",
		Warmup:      warmup,
		Measurement: &measurement,
		Sender:      measurement.Sender,
		Receiver:    measurement.Receiver,
		Verdict:     measurement.Verdict,
		Error:       measurement.Error,
	}
	return result, err
}

// localReceiver is the ASP's own receiver for the reverse direction of a
// bidirectional run: its control endpoint and the read loops that feed it.
type localReceiver struct {
	control *receiverControl
	// faults carries the read loops' faults to forwardFaults.
	faults   chan error
	shutdown func()
}

// startLocalReceiver runs the ASP's own control endpoint and read loop for
// the reverse direction of a bidirectional run. The SGP reverse driver owns
// the cohort lifecycle against it exactly as the ASP owns the forward cohort
// against the SGP.
func startLocalReceiver(ctx context.Context, config commandConfig, associations []*m3ua.Association) (*localReceiver, error) {
	control := newReceiverControl(config.Associations, maxOutstanding)
	control.enableSharedClock(config.SameHostClock)
	if control.fatal != "" {
		return nil, errors.New(control.fatal)
	}
	control.cpuStatPath = config.CPUStatPath
	httpListener, err := net.Listen("tcp", config.ControlAddress)
	if err != nil {
		return nil, fmt.Errorf("listen for local receiver control: %w", err)
	}
	httpServer := &http.Server{Handler: control.handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		_ = httpServer.Serve(httpListener)
	}()
	local := &localReceiver{control: control, faults: make(chan error, 1)}
	go local.forwardFaults(ctx)
	for index, association := range associations {
		control.setAssociationReady(index, int(association.MaxMessageStreamID()))
		go readAssociation(ctx, index, association, control, local.faults)
	}
	local.shutdown = func() {
		shutdownContext, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownContext)
	}
	return local, nil
}

// forwardFaults records the first read-loop fault on the local control, as
// runReceiver does on the SGP. The reverse receiver record then carries it as
// its fatal error, so a lost association can never pass for undelivered work.
func (local *localReceiver) forwardFaults(ctx context.Context) {
	select {
	case err := <-local.faults:
		local.control.setFatal(err.Error())
	case <-ctx.Done():
	}
}

// fault is the local receiver's fatal error, empty while it has none.
func (local *localReceiver) fault() string {
	return local.control.fatalError()
}

// collectReverse waits for the SGP reverse driver to finish the matching
// reverse cohort and folds both reverse records into the cohort verdict.
// The wait is bounded; a missing reverse cohort is an error, never a silent
// pass.
func collectReverse(ctx context.Context, config commandConfig, result *cohortResult) {
	// The reverse cohort ends with the forward one; only control round-trips
	// separate them, so a short margin past the drain is ample. A missing
	// reverse cohort after that is a failure, never a silent pass.
	deadline := time.Now().Add(config.Drain + 5*time.Second)
	for {
		receiver, err := getReceiverResult(ctx, config.PeerControl)
		if err == nil {
			switch {
			case receiver.ReverseError != "":
				result.ReverseSender = receiver.Reverse
				result.ReverseReceiver = receiver.ReverseReceiver
				result.Verdict = verdictInvalid
				result.validityOnly = result.validityOnly && receiver.ReverseError == errCohortInvalid.Error()
				result.Error = joinErrorText(result.Error, "reverse cohort: "+receiver.ReverseError)
				return
			case receiver.Reverse != nil:
				result.ReverseSender = receiver.Reverse
				result.ReverseReceiver = receiver.ReverseReceiver
				result.Verdict = combineVerdicts(result.Verdict, receiver.Reverse.Verdict)
				if receiver.ReverseReceiver != nil {
					result.Verdict = combineVerdicts(result.Verdict, receiver.ReverseReceiver.Verdict)
				}
				return
			case receiver.FatalError != "":
				result.Verdict = verdictInvalid
				result.validityOnly = false
				result.Error = joinErrorText(result.Error, "reverse cohort receiver: "+receiver.FatalError)
				return
			}
		}
		if !time.Now().Before(deadline) {
			result.Verdict = verdictInvalid
			result.validityOnly = false
			result.Error = joinErrorText(result.Error, "reverse cohort did not complete before the collection deadline")
			return
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			result.Verdict = verdictInvalid
			result.validityOnly = false
			result.Error = joinErrorText(result.Error, ctx.Err().Error())
			return
		case <-timer.C:
		}
	}
}

func combineVerdicts(first, second string) string {
	if first == verdictInvalid || second == verdictInvalid {
		return verdictInvalid
	}
	if first == verdictInconclusive || second == verdictInconclusive {
		return verdictInconclusive
	}
	return first
}

func joinErrorText(existing, addition string) string {
	if existing == "" {
		return addition
	}
	return existing + "; " + addition
}

func newCohortResult(phase string, sender, receiver runRecord, err error) cohortResult {
	result := cohortResult{Phase: phase, Sender: sender, Receiver: receiver, Verdict: verdictPass,
		validityOnly: err == nil || err.Error() == errCohortInvalid.Error()}
	if sender.Verdict == verdictInvalid || receiver.Verdict == verdictInvalid || err != nil {
		result.Verdict = verdictInvalid
	} else if sender.Verdict == verdictFail || receiver.Verdict == verdictFail {
		result.Verdict = verdictFail
	} else if sender.Verdict == verdictInconclusive || receiver.Verdict == verdictInconclusive {
		result.Verdict = verdictInconclusive
	}
	if err != nil {
		result.Error = err.Error()
	}
	return result
}

// failedWarmupResult ends a run whose warm-up failed with every record the
// warm-up produced, both directions' in a bidirectional run. A warm-up whose
// every direction failed only its own validity rules ends with the bare
// validity error, which internal/cmd/perfcapacity accepts as evidence against
// the rate whichever direction failed; any other failure keeps the text of
// every error the cohort reported, so it can never pass for overload.
func failedWarmupResult(warmup cohortResult, err error) combinedResult {
	cause := err.Error()
	switch {
	case warmup.validityOnly:
		cause = errCohortInvalid.Error()
	case warmup.Error != "":
		cause = warmup.Error
	}
	warmup.Verdict = verdictInvalid
	warmup.Error = "warmup did not drain cleanly: " + cause
	return combinedResult{
		Phase:    warmup.Phase,
		Warmup:   &warmup,
		Sender:   warmup.Sender,
		Receiver: warmup.Receiver,
		Verdict:  warmup.Verdict,
		Error:    warmup.Error,
	}
}

func runSenderCohort(ctx context.Context, config commandConfig, associations []*m3ua.Association, registry *echoRegistry, cohort string, duration time.Duration) (runRecord, runRecord, error) {
	return runSenderCohortWith(ctx, config, associations, registry, nil, cohort, duration)
}

// runSenderCohortWith runs one sender cohort. routed is nil for the direct
// workloads; for the routed modes it is the timed sender over the frozen
// routed paths, and associations are its eight sender associations in queue
// order. Everything else — the receiver control protocol, the shared clock,
// the drain, and the evidence — is the same code path for every mode.
func runSenderCohortWith(ctx context.Context, config commandConfig, associations []*m3ua.Association, registry *echoRegistry, routed *routingTimedSender, cohort string, duration time.Duration) (runRecord, runRecord, error) {
	// runSender creates the reply registry exactly when the mode is echo;
	// every other caller (throughput, the bidirectional reverse driver, tests)
	// passes nil. Name a mismatched call instead of dereferencing nil.
	if config.Mode == modeEcho && registry == nil {
		return runRecord{}, runRecord{}, errors.New("echo mode requires an echo reply registry")
	}
	var overloadProfile *overloadProfile
	if config.overload != nil && config.overloadRole == overloadRoleMeasurement {
		overloadProfile = config.overload
		if routed != nil || registry != nil || duration != overloadProfile.duration() {
			return runRecord{}, runRecord{}, errors.New("an overload measurement cohort runs the direct throughput workload over the profile's window")
		}
	}
	expected, err := scheduledMessages(config.Rate, duration)
	if overloadProfile != nil {
		expected, err = overloadProfile.expected(), nil
	}
	if err != nil {
		return runRecord{}, runRecord{}, fmt.Errorf("calculate scheduled messages: %w", err)
	}
	if expected == 0 {
		return runRecord{}, runRecord{}, errors.New("cohort schedules no messages")
	}
	effectiveDrain := config.Drain
	if config.Mode == modeEcho && effectiveDrain < echoRequestDeadline {
		effectiveDrain = echoRequestDeadline
	}
	specification := runSpec{
		Cohort: cohort, Seed: config.Seed, Associations: len(associations), Expected: expected,
		Duration: duration, Drain: effectiveDrain, Rate: config.Rate, Payload: config.Workload,
		Outstanding: config.Outstanding,
		Mode:        config.Mode, Direction: config.Direction, Initiation: config.Initiation,
		PeerControl: config.ControlURL,
	}
	if specification.Mode == "" {
		specification.Mode = modeThroughput
	}
	if specification.Direction == "" {
		specification.Direction = directionASPToSGP
	}
	if config.sgpFailureCohort {
		specification.SGPFailure = newSGPFailureSpec(config.SGPFailure, config.SGPFailureKind)
	}
	if config.RouteReferences.enabled() {
		specification.RouteReferences = config.RouteReferences.spec()
	}
	if config.overload != nil && config.overloadRole != "" {
		specification.Overload = config.overload.spec(config.overloadRole)
	}
	clock, err := prepareSharedRunClock(ctx, config, &specification)
	if err != nil {
		return runRecord{}, runRecord{}, fmt.Errorf("prepare shared clock: %w", err)
	}
	var failover *failoverTracker
	if specification.SGPFailure != nil {
		if routed == nil || clock == nil {
			return runRecord{}, runRecord{}, errors.New("an SGP failure cohort needs the routed sender and the shared clock")
		}
		if failover, err = newFailoverTracker(*specification.SGPFailure, clock, routed.plane, routed.paths); err != nil {
			return runRecord{}, runRecord{}, err
		}
		routed = routed.withFailover(failover)
	}
	if err := config.ssnmRun.attach(&specification, config.ssnmPhase); err != nil {
		return runRecord{}, runRecord{}, fmt.Errorf("declare SSNM load: %w", err)
	}
	if err := postJSON(ctx, config.PeerControl+"/reset", specification); err != nil {
		return runRecord{}, runRecord{}, fmt.Errorf("reset receiver: %w", err)
	}
	cpuBefore, cpuBeforeErr := readCPUStat(config.CPUStatPath)
	allocBefore := readRuntimeCounters()
	memory := startMemorySampler()
	defer memory.stop()
	if err := postJSON(ctx, config.PeerControl+"/start", nil); err != nil {
		return runRecord{}, runRecord{}, fmt.Errorf("start receiver: %w", err)
	}

	initialBefore := time.Now()
	initialObservation := observeSharedProgress(ctx, initialBefore, config.PeerControl, clock)
	if initialObservation.Error != "" || initialObservation.Snapshot == nil {
		return stopFailedProgress(config.PeerControl, specification, initialObservation, fmt.Errorf("read initial receiver progress: %s", initialObservation.Error))
	}
	initialProgress := initialObservation.Snapshot
	if !sameRunSpec(initialProgress.Spec, specification) || initialProgress.Generation == 0 || initialProgress.Phase != receiverMeasuring || initialProgress.Delivery.Unique != 0 || initialProgress.Delivery.Missing != expected || initialProgress.Delivery.Invalid != 0 || initialProgress.Delivery.Duplicate != 0 || initialProgress.Delivery.Reordered != 0 || initialProgress.FatalError != "" {
		return stopFailedProgress(config.PeerControl, specification, initialObservation, errors.New("receiver progress is not an empty active cohort"))
	}
	started := time.Now()
	drainDeadline := started.Add(duration + effectiveDrain)
	var watchdogEvidence *sharedClockWatchdog
	if clock == nil {
		initialObservation.Before = initialBefore.Sub(started)
		initialObservation.After = 0
	} else {
		elapsed, clockErr := clock.elapsed()
		if clockErr != nil || elapsed >= -time.Duration(clock.window.Domain.Resolution) || initialProgress.Clock == nil ||
			!clockEnvelopeContains(initialObservation.Clock.Before, initialObservation.Clock.After, initialProgress.Clock.Captured, clock.window.Domain.Resolution) || initialProgress.Clock.Domain != clock.window.Domain {
			return stopFailedProgress(config.PeerControl, specification, initialObservation, errors.New("shared clock start boundary or envelope failed"))
		}
		var deadlineErr error
		drainDeadline, watchdogEvidence, deadlineErr = clock.watchdog(time.Now)
		if deadlineErr != nil {
			return stopFailedProgress(config.PeerControl, specification, initialObservation, deadlineErr)
		}
	}
	// The drain deadline is past once the cohort ends. Left on the
	// associations it would fail the next write the library makes on its own
	// behalf, which closes the association, so every exit clears it.
	defer clearWriteDeadlines(associations)
	for _, association := range associations {
		if deadlineErr := association.SetWriteDeadline(drainDeadline); deadlineErr != nil {
			_ = postJSON(ctx, config.PeerControl+"/stop", nil)
			return runRecord{}, runRecord{}, fmt.Errorf("set association write deadline: %w", deadlineErr)
		}
	}
	var tracker *echoTracker
	sweepDone := make(chan struct{})
	if specification.Mode == modeEcho {
		tracker = newEchoTracker(cohort, config.Seed, len(associations), config.Workload, config.Outstanding, echoRequestDeadline)
		registry.register(tracker)
		go sweepEchoRequests(tracker, sweepDone)
	}
	counters := newSenderCounters(config.Outstanding)
	counters.drainOutcomes = specification.nominalDrainOutcomes()
	var queues []chan sendJob
	var routedQueues []chan routingTimedJob
	var workersDone <-chan struct{}
	var overloadEpochs []uint64
	var mark func() overloadSenderMark
	switch {
	case routed != nil:
		routedQueues, workersDone = startRoutedSendWorkers(ctx, routed, config, counters)
	case overloadProfile != nil:
		counters.overload = newOverloadCounters(overloadProfile.schedule)
		overloadEpochs = make([]uint64, len(associations))
		for index, association := range associations {
			overloadEpochs[index] = association.Epoch()
		}
		mark = counters.overloadMark
		initialObservation.SenderBefore, initialObservation.SenderAfter = &overloadSenderMark{}, &overloadSenderMark{}
		queues, workersDone = startOverloadSendWorkers(associations, config, counters, drainDeadline)
	default:
		queues, workersDone = startSendWorkers(associations, config, counters, tracker)
	}
	if failover != nil {
		failover.watch(associations)
		defer failover.finish()
	}
	sampleDone := make(chan struct{})
	go sampleSharedSender(started, counters, sampleDone, clock)
	progressDone := sampleMarkedProgress(ctx, started, duration, config.PeerControl, clock, mark)
	var fixtureQueueMax []int
	switch {
	case routed != nil:
		dispatchRouted(ctx, config, routed, cohort, duration, started, expected, routedQueues, counters, clock)
	case overloadProfile != nil:
		fixtureQueueMax = dispatchOverload(ctx, config, cohort, overloadProfile.schedule, started, queues, counters, clock)
	default:
		dispatchScheduled(ctx, config, cohort, duration, started, expected, queues, counters, tracker, clock)
	}
	outstandingAtEnd := counters.outstandingCount()
	close(sampleDone)
	observations := append([]progressObservation{initialObservation}, (<-progressDone)...)
	for _, queue := range queues {
		close(queue)
	}
	for _, queue := range routedQueues {
		close(queue)
	}
	drainStarted := time.Now()
	boundaryContext, cancelBoundary := context.WithDeadline(ctx, drainDeadline)
	observations = append(observations, observeSharedProgress(boundaryContext, started, config.PeerControl, clock))
	cancelBoundary()
	drained := waitWorkersContext(ctx, workersDone, remainingUntil(drainDeadline))
	var outstandingAtDeadline uint64
	if !drained && ctx.Err() == nil && counters.drainOutcomes {
		// The drain deadline passed with scheduled work still queued or in a
		// send call. The association write deadline is the drain deadline, so
		// every remaining send now fails at once and the workers finish within
		// a short grace; one that does not is stuck in the transport, a fault
		// handled below.
		outstandingAtDeadline = counters.outstandingCount()
		drained = waitWorkersContext(ctx, workersDone, senderDrainGrace)
	}
	if tracker != nil {
		waitEchoDrain(ctx, tracker, drainDeadline)
		close(sweepDone)
	}
	if !drained {
		if ctx.Err() != nil {
			counters.setFatal(ctx.Err().Error())
		} else {
			counters.setFatal("sender workers exceeded the drain deadline")
		}
		for _, association := range associations {
			_ = association.Close()
		}
		<-workersDone
	}
	if overloadProfile != nil {
		finalOffset := time.Since(started)
		if clock != nil {
			if elapsed, clockErr := clock.elapsed(); clockErr == nil {
				finalOffset = elapsed
			}
		}
		counters.finishOverloadSeries(finalOffset)
	}
	// Once the drain deadline cut the sender off, the deadline has passed and
	// there is no drain left to wait for; the receiver's final counts are read
	// after the stop.
	senderTimeout := counters.drainTimeout(specification.Drain, outstandingAtDeadline)
	var receiver runRecord
	var pollErr error
	if senderTimeout == nil {
		drainContext, cancelDrain := context.WithDeadline(ctx, drainDeadline)
		if failover != nil {
			receiver, pollErr = waitFailoverDrain(drainContext, config.PeerControl, failover, drainDeadline)
		} else {
			receiver, pollErr = waitReceiverDrain(drainContext, config.PeerControl, counters, drainDeadline)
		}
		observations = append(observations, observeSharedProgress(drainContext, started, config.PeerControl, clock))
		cancelDrain()
	}
	drainTimeout, pollErr := drainTimeoutOutcome(pollErr, specification, drainDeadline)
	if pollErr != nil {
		counters.setFatal(pollErr.Error())
	}
	diagnosticsContext, cancelDiagnostics := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelDiagnostics()
	stopErr := postJSON(diagnosticsContext, config.PeerControl+"/stop", nil)
	if stopErr == nil {
		finalReceiver, resultErr := getReceiverResult(diagnosticsContext, config.PeerControl)
		if resultErr == nil {
			receiver = finalReceiver
		} else {
			stopErr = fmt.Errorf("read final receiver result: %w", resultErr)
		}
	}
	drainDuration := time.Since(drainStarted)
	if clock != nil {
		elapsed, clockErr := clock.elapsed()
		if clockErr != nil {
			counters.setFatal(clockErr.Error())
		} else {
			drainDuration = max(elapsed-duration, 0)
		}
	}
	if stopErr != nil {
		counters.setFatal(fmt.Sprintf("stop receiver: %v", stopErr))
	}
	memoryObservation := memory.finish()
	cpuAfter, cpuAfterErr := readCPUStat(config.CPUStatPath)
	allocAfter := readRuntimeCounters()
	sender := counters.result(specification, duration, drainDuration, outstandingAtEnd)
	sender.Memory = &memoryObservation
	if tracker != nil {
		echo := tracker.result(counters.submittedCount())
		sender.Echo = &echo
		if fatal := registry.fatalError(); fatal != "" && sender.FatalError == "" {
			sender.FatalError = fatal
		}
	}
	sender.CPU = newCPUObservation(cpuBefore, cpuAfter, cpuBeforeErr, cpuAfterErr, receiver.Delivery.Unique)
	sender.Allocations = AllocationObservation{Scope: wholeProcessScope, Before: allocBefore, After: allocAfter, Delta: runtimeDelta(allocBefore, allocAfter)}
	sender.Delivery = receiver.Delivery
	sender.NegotiatedOutboundStreams = make([]int, len(associations))
	for index, association := range associations {
		sender.NegotiatedOutboundStreams[index] = int(association.MaxMessageStreamID()) + 1
	}
	sender.Manifest = currentManifest(config.Outstanding, config.Initiation)
	sender.Manifest.SSNMBudgets = config.SSNM.budgetsRecord()
	if routed != nil {
		sender.Manifest.FlowCount = routingRouteCount
	}
	sender.ProgressObservations = observations
	accounting := analyzeProgress(specification, observations)
	sender.SenderWindow = &accounting
	sender.ValidatedPerSecond = accounting.RateLower
	sender.BacklogAssessment = "paired interval diagnostics only; sustained-backlog acceptance is not determined"
	sender.WindowAlignment = "sender monotonic measurement window; rate is a conservative lower bound from bracketed receiver snapshots, not the receiver first-arrival diagnostic"
	if clock != nil {
		peer, clockErr := readPeerClock(diagnosticsContext, config.PeerControl, clock.source)
		domain, domainErr := clock.source.Domain()
		sender.ClockEvidence = &sharedClockEvidence{Before: clock.window.Domain, After: domain, Watchdog: watchdogEvidence}
		sender.ClockEvidence.Verified = clockErr == nil && domainErr == nil && domain == clock.window.Domain && peer.Domain == domain && receiver.ClockEvidence != nil && receiver.ClockEvidence.Verified && receiver.ClockEvidence.Before == domain && receiver.ClockEvidence.After == domain
		if !sender.ClockEvidence.Verified {
			sender.FatalError = "shared clock post-run domain verification failed"
		}
		sender.WindowAlignment = "verified same-host CLOCK_MONOTONIC window; rate and backlog retain clock-resolution bounds"
	}
	sender.OutstandingScope = "legacy counters measure sender worker queues only; sender_window bounds include all scheduled but not yet validated deliveries"
	if failover != nil {
		sender.Failover = failover.evaluate(failoverInputs{specification: specification, scheduled: sender.Scheduled, capped: sender.Capped, receiver: receiver, accounting: accounting})
	}
	if overloadProfile != nil {
		sender.Overload = collectOverloadEvidence(diagnosticsContext, config, specification, overloadProfile, counters, associations, overloadEpochs, fixtureQueueMax, receiver, initialProgress.Generation, stopErr, observations, &sender)
	}
	sender.DrainTimeout = drainTimeout
	sender.SenderDrainTimeout = senderTimeout
	sender.evaluate()
	var cohortErrors []error
	if pollErr != nil {
		cohortErrors = append(cohortErrors, pollErr)
	}
	if stopErr != nil {
		cohortErrors = append(cohortErrors, stopErr)
	}
	if sender.Verdict == verdictInvalid || receiver.Verdict == verdictInvalid {
		cohortErrors = append(cohortErrors, errCohortInvalid)
	}
	return sender, receiver, errors.Join(cohortErrors...)
}

func startSendWorkers(associations []*m3ua.Association, config commandConfig, counters *senderCounters, tracker *echoTracker) ([]chan sendJob, <-chan struct{}) {
	capacities := queueCapacities(len(associations), config.Outstanding)
	queues := make([]chan sendJob, len(associations))
	var workers sync.WaitGroup
	workers.Add(len(associations))
	for index, association := range associations {
		queues[index] = make(chan sendJob, capacities[index])
		go func(connection *m3ua.Association, jobs <-chan sendJob) {
			defer workers.Done()
			for job := range jobs {
				dispatchLag, clockErr := job.dispatchDelay()
				if clockErr != nil {
					counters.complete(clockErr, 0, 0)
					continue
				}
				payload := buildPayload(job.identity, job.size)
				tuple := tupleFor(job.identity.Flow, job.identity.Association)
				if job.reverse {
					tuple = reverseTuple(tuple)
				}
				sendStarted := time.Now()
				written, sendErr := connection.WriteData(tuple.dataRequest(payload))
				sendDuration := time.Since(sendStarted)
				if job.clock != nil {
					sendErr = errors.Join(sendErr, job.clock.withinDrain(job.offset+dispatchLag))
				}
				if sendErr == nil && written != job.size {
					sendErr = fmt.Errorf("WriteData wrote %d bytes, want %d", written, job.size)
				}
				if sendErr != nil && tracker != nil {
					tracker.fail(globalIndex(job.identity))
				}
				counters.complete(sendErr, dispatchLag, sendDuration)
			}
		}(association, queues[index])
	}
	done := make(chan struct{})
	go func() {
		workers.Wait()
		close(done)
	}()
	return queues, done
}

func dispatchScheduled(ctx context.Context, config commandConfig, cohort string, duration time.Duration, started time.Time, expected uint64, queues []chan sendJob, counters *senderCounters, tracker *echoTracker, clock *sharedRunClock) {
	dispatchOpenLoop(ctx, config.Rate, duration, started, expected, clock, counters, func(index uint64, offset time.Duration, scheduled time.Time) {
		identity := planMessage(cohort, config.Seed, index, len(queues))
		if tracker != nil {
			identity.Kind = kindEchoRequest
		}
		job := sendJob{
			identity: identity, scheduled: scheduled,
			clock: clock, offset: offset,
			size: config.Workload.size(index), reverse: config.Direction == directionSGPToASP,
		}
		if tracker != nil && !tracker.admit(index, job.scheduled) {
			counters.capOne()
			return
		}
		if counters.reserve() {
			select {
			case queues[identity.Association] <- job:
			default:
				counters.rejectReservation()
				if tracker != nil {
					tracker.fail(index)
				}
			}
		} else if tracker != nil {
			tracker.fail(index)
		}
	})
}

func (counters *senderCounters) schedule() {
	counters.mutex.Lock()
	counters.scheduled++
	counters.mutex.Unlock()
}

func (counters *senderCounters) reserve() bool {
	counters.mutex.Lock()
	defer counters.mutex.Unlock()
	if counters.outstanding >= counters.limit {
		counters.capped++
		return false
	}
	counters.outstanding++
	return true
}

func (counters *senderCounters) capOne() {
	counters.mutex.Lock()
	counters.capped++
	counters.mutex.Unlock()
}

func (counters *senderCounters) rejectReservation() {
	counters.mutex.Lock()
	counters.outstanding--
	counters.capped++
	counters.mutex.Unlock()
}

func (counters *senderCounters) complete(err error, dispatchLag, sendDuration time.Duration) {
	counters.mutex.Lock()
	defer counters.mutex.Unlock()
	if counters.outstanding > 0 {
		counters.outstanding--
	}
	if err != nil {
		counters.sendErrors++
		if counters.drainOutcomes && cutOffByDrainDeadline(err) {
			counters.unsubmitted++
		} else if counters.fatal == "" {
			counters.fatal = err.Error()
		}
	} else {
		counters.submitted++
	}
	counters.dispatchLag.record(dispatchLag)
	counters.sendTime.record(sendDuration)
}

func (counters *senderCounters) abort(remaining uint64, err error) {
	counters.mutex.Lock()
	defer counters.mutex.Unlock()
	counters.scheduled += remaining
	counters.capped += remaining
	counters.fatal = err.Error()
	if counters.overload != nil {
		counters.overload.abortRemaining(remaining)
	}
}

func (counters *senderCounters) setFatal(reason string) {
	counters.mutex.Lock()
	defer counters.mutex.Unlock()
	if counters.fatal == "" {
		counters.fatal = reason
	}
}

// senderDrainGrace bounds how long after the drain deadline the send workers
// may take to fail the work they still hold. Every send then fails at once on
// the expired write deadline, so the workers need milliseconds; one still
// running after the grace is stuck in the transport.
const senderDrainGrace = time.Second

// cutOffByDrainDeadline reports a send that failed only because the drain
// deadline passed: the association write deadline, which a nominal cohort
// sets to the drain deadline, expired, or the call completed after the shared
// drain deadline. Every cause the error wraps must be one of those; anything
// else — a lost association, a short write, a clock failure — is a fault.
func cutOffByDrainDeadline(err error) bool {
	switch wrapped := err.(type) {
	case nil:
		return false
	case interface{ Unwrap() []error }:
		parts := wrapped.Unwrap()
		for _, part := range parts {
			if !cutOffByDrainDeadline(part) {
				return false
			}
		}
		return len(parts) != 0
	case interface{ Unwrap() error }:
		if cause := wrapped.Unwrap(); cause != nil {
			return cutOffByDrainDeadline(cause)
		}
	}
	return errors.Is(err, os.ErrDeadlineExceeded) || errors.Is(err, errCompletedAfterDrain)
}

// drainTimeout records the sender side of a drain deadline outcome: work
// still queued or in a send call when the deadline passed, or sends the
// deadline cut off. It is nil when the sender submitted everything in time.
func (counters *senderCounters) drainTimeout(drain time.Duration, outstandingAtDeadline uint64) *senderDrainTimeoutRecord {
	counters.mutex.Lock()
	defer counters.mutex.Unlock()
	if !counters.drainOutcomes || outstandingAtDeadline == 0 && counters.unsubmitted == 0 {
		return nil
	}
	return &senderDrainTimeoutRecord{
		Cause: senderDrainTimeoutCause, Drain: drain, OutstandingAtDeadline: outstandingAtDeadline, Unsubmitted: counters.unsubmitted,
	}
}

func (counters *senderCounters) outstandingCount() uint64 {
	counters.mutex.Lock()
	defer counters.mutex.Unlock()
	return counters.outstanding
}

func sampleSender(started time.Time, counters *senderCounters, done <-chan struct{}) {
	sampleSharedSender(started, counters, done, nil)
}

func sampleSharedSender(started time.Time, counters *senderCounters, done <-chan struct{}, clock *sharedRunClock) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			counters.mutex.Lock()
			offset := now.Sub(started)
			if clock != nil {
				var err error
				offset, err = clock.elapsed()
				if err != nil {
					counters.fatal = err.Error()
					counters.mutex.Unlock()
					return
				}
			}
			if offset < 0 {
				counters.mutex.Unlock()
				continue
			}
			if counters.overload != nil {
				// The overload series is judged against the schedule at the
				// instant its counts were read. The tick time can be well
				// before the mutex was taken, so without a shared clock the
				// instant is read again under the mutex.
				overloadOffset := offset
				if clock == nil {
					overloadOffset = time.Since(started)
				}
				counters.sampleOverloadLocked(overloadOffset)
			}
			if len(counters.series) < 601 {
				counters.series = append(counters.series, seriesPoint{
					OffsetMillis: uint64(offset / time.Millisecond),
					Scheduled:    counters.scheduled,
					Sent:         counters.submitted,
					Submitted:    counters.submitted,
					SendErrors:   counters.sendErrors,
					Capped:       counters.capped,
					Outstanding:  counters.outstanding,
				})
			}
			counters.mutex.Unlock()
		case <-done:
			return
		}
	}
}

func waitWorkersContext(ctx context.Context, done <-chan struct{}, timeout time.Duration) bool {
	if timeout <= 0 {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-ctx.Done():
		return false
	case <-timer.C:
		return false
	}
}

// drainDeadlineError is waitReceiverDrain's report that the drain deadline
// passed while the last receiver result the sender read still showed
// submitted messages unaccounted. That is the delivery outcome of a probe
// above capacity, not a fixture fault. Every other way the wait can end — a
// failed request, a canceled run, or a deadline with no receiver result read
// before it — is reported as the error that caused it.
type drainDeadlineError struct {
	submitted uint64
	accounted uint64
	observed  time.Time
}

func (err *drainDeadlineError) Error() string {
	return fmt.Sprintf("receiver drain deadline exceeded with %d of %d submitted messages unaccounted", err.submitted-err.accounted, err.submitted)
}

// timeout records the drain outcome against the cohort's drain allowance and
// absolute deadline.
func (err *drainDeadlineError) timeout(drain time.Duration, deadline time.Time) *drainTimeoutRecord {
	return &drainTimeoutRecord{
		Cause: drainTimeoutCause, Drain: drain, Submitted: err.submitted, Accounted: err.accounted,
		Undelivered: err.submitted - err.accounted, ObservedBeforeDeadline: max(deadline.Sub(err.observed), 0),
	}
}

// drainTimeoutOutcome separates a nominal cohort's drain deadline outcome
// from a fixture fault. The overload and SGP failure trials keep their own
// contracts, in which any drain failure is a fixture failure, so their error
// is returned unchanged.
func drainTimeoutOutcome(pollErr error, specification runSpec, deadline time.Time) (*drainTimeoutRecord, error) {
	var outcome *drainDeadlineError
	if !specification.nominalDrainOutcomes() || !errors.As(pollErr, &outcome) {
		return nil, pollErr
	}
	return outcome.timeout(specification.Drain, deadline), nil
}

// drainPollInterval is how often the drain wait reads the receiver's result.
const drainPollInterval = 10 * time.Millisecond

// drainObservationBound is how recent the last receiver result must be when
// the drain deadline passes for the wait to report undelivered work rather
// than a control fault. A responsive control is read every drainPollInterval,
// so at the deadline its last result is at most one interval and one local
// request old; ten intervals leave nine for request latency and scheduling
// before a control that stopped answering is named. The observed runs read it
// within 12 ms of the deadline. internal/cmd/perfcapacity applies the same
// bound.
const drainObservationBound = 10 * drainPollInterval

func waitReceiverDrain(ctx context.Context, baseURL string, counters *senderCounters, deadline time.Time) (runRecord, error) {
	return pollReceiverDrain(ctx, baseURL, counters, deadline, drainPollInterval, drainObservationBound)
}

// pollReceiverDrain reads the receiver's result every interval until it has
// accounted for every submitted message or the deadline passes. A deadline
// that passes after unaccounted work was seen is the drainDeadlineError
// outcome only if the last result was read no more than bound before it;
// otherwise the receiver control stopped answering, a fault.
func pollReceiverDrain(ctx context.Context, baseURL string, counters *senderCounters, deadline time.Time, interval, bound time.Duration) (runRecord, error) {
	var last runRecord
	var outstanding *drainDeadlineError
	// expired reports whether the drain deadline itself, rather than a
	// failure or a canceled run, ended the wait after unaccounted work was
	// seen.
	expired := func(err error) bool {
		return outstanding != nil && errors.Is(err, context.DeadlineExceeded) && !time.Now().Before(deadline)
	}
	settle := func() (runRecord, error) {
		if stale := deadline.Sub(outstanding.observed); stale > bound {
			return runRecord{}, fmt.Errorf("receiver control did not answer during the drain: its last result was read %s before the deadline", stale)
		}
		return last, outstanding
	}
	for {
		if !time.Now().Before(deadline) {
			if outstanding != nil {
				return settle()
			}
			return runRecord{}, errors.New("receiver drain deadline exceeded")
		}
		requestContext, cancelRequest := context.WithDeadline(ctx, deadline)
		receiver, err := getReceiverResult(requestContext, baseURL)
		cancelRequest()
		if err != nil {
			if expired(err) {
				return settle()
			}
			return runRecord{}, err
		}
		submitted := counters.submittedCount()
		accounted := receiver.Delivery.Unique + receiver.Delivery.Invalid + receiver.Delivery.Duplicate
		if receiver.Overload != nil && receiver.Overload.Receiver != nil {
			// An overload receiver may discard accepted messages; each
			// discard is accounted for, not awaited.
			accounted += receiver.Overload.Receiver.Discarded
		}
		if accounted >= submitted {
			return receiver, nil
		}
		last, outstanding = receiver, &drainDeadlineError{submitted: submitted, accounted: accounted, observed: time.Now()}
		if !time.Now().Before(deadline) {
			return settle()
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			if expired(ctx.Err()) {
				return settle()
			}
			return receiver, ctx.Err()
		case <-timer.C:
		}
	}
}

// clearWriteDeadlines removes the cohort's write deadline from every
// association. An association that is already closed refuses it, which is
// harmless.
func clearWriteDeadlines(associations []*m3ua.Association) {
	for _, association := range associations {
		_ = association.SetWriteDeadline(time.Time{})
	}
}

func (counters *senderCounters) submittedCount() uint64 {
	counters.mutex.Lock()
	defer counters.mutex.Unlock()
	return counters.submitted
}

func (counters *senderCounters) result(specification runSpec, measurementDuration, drainDuration time.Duration, outstandingAtEnd uint64) runRecord {
	counters.mutex.Lock()
	defer counters.mutex.Unlock()
	series := append([]seriesPoint(nil), counters.series...)
	return runRecord{
		Side:                     "sender",
		Spec:                     specification,
		Expected:                 specification.Expected,
		Scheduled:                counters.scheduled,
		Sent:                     counters.submitted,
		Submitted:                counters.submitted,
		SendErrors:               counters.sendErrors,
		Capped:                   counters.capped,
		OutstandingAtWindowStart: 0,
		OutstandingAtWindowEnd:   outstandingAtEnd,
		OutstandingAfterDrain:    counters.outstanding,
		MeasurementDuration:      measurementDuration,
		DrainDuration:            drainDuration,
		SendDuration:             counters.sendTime.percentiles(),
		DispatchLag:              counters.dispatchLag.percentiles(),
		Series:                   series,
		FatalError:               counters.fatal,
		BacklogAssessment:        assessBacklog(series),
	}
}

func assessBacklog(series []seriesPoint) string {
	if len(series) < 8 {
		return "insufficient samples"
	}
	quarter := len(series) / 4
	var firstTotal uint64
	var lastTotal uint64
	for index := 0; index < quarter; index++ {
		firstTotal += series[index].Outstanding
		lastTotal += series[len(series)-quarter+index].Outstanding
	}
	firstAverage := float64(firstTotal) / float64(quarter)
	lastAverage := float64(lastTotal) / float64(quarter)
	if lastAverage <= firstAverage+1 {
		return "not growing"
	}
	if lastAverage > firstAverage*1.10+1 {
		return "growing"
	}
	return "uncertain"
}

func waitForReady(ctx context.Context, baseURL string, associations int) error {
	deadline := time.Now().Add(30 * time.Second)
	for {
		requestContext, cancelRequest := context.WithDeadline(ctx, deadline)
		request, err := http.NewRequestWithContext(requestContext, http.MethodGet, baseURL+"/ready", nil)
		if err != nil {
			cancelRequest()
			return err
		}
		response, err := fixtureHTTPClient.Do(request)
		if err == nil {
			var ready readyResult
			decodeErr := json.NewDecoder(response.Body).Decode(&ready)
			_ = response.Body.Close()
			if decodeErr == nil && ready.Error != "" {
				cancelRequest()
				return errors.New(ready.Error)
			}
			if decodeErr == nil && ready.Ready && ready.Associations == associations {
				cancelRequest()
				return nil
			}
		}
		cancelRequest()
		if !time.Now().Before(deadline) {
			return errors.New("receiver readiness deadline exceeded")
		}
		timer := time.NewTimer(50 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func postJSON(ctx context.Context, url string, value any) error {
	var body io.Reader
	if value != nil {
		encoded, err := json.Marshal(value)
		if err != nil {
			return err
		}
		body = bytes.NewReader(encoded)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, body)
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := fixtureHTTPClient.Do(request)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusNoContent {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("%s: %s", response.Status, bytes.TrimSpace(message))
	}
	return nil
}

func getReceiverResult(ctx context.Context, baseURL string) (runRecord, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/results", nil)
	if err != nil {
		return runRecord{}, err
	}
	response, err := fixtureHTTPClient.Do(request)
	if err != nil {
		return runRecord{}, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return runRecord{}, fmt.Errorf("receiver results: %s", response.Status)
	}
	var record runRecord
	if err := json.NewDecoder(io.LimitReader(response.Body, 4<<20)).Decode(&record); err != nil {
		return runRecord{}, err
	}
	return record, nil
}

func remainingUntil(deadline time.Time) time.Duration {
	remaining := time.Until(deadline)
	if remaining < 0 {
		return 0
	}
	return remaining
}

// readEchoReplies validates echo replies against the cohort tracker that owns
// their identity and completes the outstanding request. Replies are RTT
// evidence only; they are never counted as useful deliveries. A reply that
// parses but matches no registered cohort is counted on the active cohort; a
// read failure before shutdown is a fatal fixture error.
func readEchoReplies(ctx context.Context, transportIndex int, association *m3ua.Association, registry *echoRegistry) {
	for {
		message, err := association.ReadData(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			registry.setFatal(fmt.Sprintf("association %d echo reply ReadData: %v", transportIndex, err))
			return
		}
		received := receivedMessage{
			ProtocolData:         protocolDataFromM3UA(message.ProtocolData),
			NetworkAppearance:    message.Scope.NetworkAppearance,
			NetworkAppearanceSet: message.Scope.NetworkAppearanceSet,
			RoutingContext:       firstRoutingContext(message.Scope),
			RoutingContextSet:    message.Scope.RoutingContextSet,
		}
		identity, err := parsePayload(received.ProtocolData.Data)
		if err != nil {
			registry.unattributed()
			continue
		}
		tracker := registry.trackerFor(identity.CohortHash)
		if tracker == nil {
			registry.unattributed()
			continue
		}
		validated, err := validateMessage(received, tracker.cohort, tracker.seed, tracker.associations, tracker.workload, kindEchoReply, true)
		if err != nil {
			tracker.countInvalid()
			continue
		}
		tracker.complete(globalIndex(validated), time.Now())
	}
}

func sweepEchoRequests(tracker *echoTracker, done <-chan struct{}) {
	ticker := time.NewTicker(echoSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			tracker.sweep(now)
		case <-done:
			return
		}
	}
}

// waitEchoDrain waits until every outstanding request is answered or swept,
// bounded by the same absolute drain deadline as the rest of the cohort. A
// final sweep at the deadline counts every remaining request as a deadline
// failure instead of omitting it.
func waitEchoDrain(ctx context.Context, tracker *echoTracker, deadline time.Time) {
	for tracker.outstandingCount() > 0 {
		if !time.Now().Before(deadline) {
			break
		}
		timer := time.NewTimer(min(10*time.Millisecond, remainingUntil(deadline)))
		select {
		case <-ctx.Done():
			timer.Stop()
			tracker.sweep(time.Now())
			return
		case <-timer.C:
		}
	}
	tracker.sweep(time.Now())
}
