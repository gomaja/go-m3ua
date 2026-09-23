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
	"sync"
	"time"

	"github.com/gomaja/go-m3ua"
)

const schedulerQuantum = 100 * time.Microsecond

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
}

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
}

func newSenderCounters(limit int) *senderCounters {
	return &senderCounters{sendTime: newDurationHistogram(), dispatchLag: newDurationHistogram(), limit: uint64(limit)}
}

func runSender(ctx context.Context, config commandConfig) (combinedResult, error) {
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
	config.ssnmRun, err = startSSNMLoad(ctx, config, endpoint)
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
	var localFatal chan error
	if config.Mode == modeBidirectional {
		var shutdown func()
		var err error
		shutdown, localFatal, err = startLocalReceiver(ctx, config, associations)
		if err != nil {
			return combinedResult{}, err
		}
		defer shutdown()
	}
	runCohort := func(cohortConfig commandConfig, phase, cohort string, duration time.Duration) (cohortResult, error) {
		sender, receiver, err := runSenderCohort(ctx, cohortConfig, associations, registry, cohort, duration)
		result := newCohortResult(phase, sender, receiver, err)
		if config.Mode == modeBidirectional {
			collectReverse(ctx, cohortConfig, &result)
		}
		return result, err
	}
	var warmup *cohortResult
	if config.Warmup > 0 {
		warmupConfig := config
		warmupConfig.Cohort += "-warmup"
		warmupConfig.ssnmPhase = ssnmPhaseWarmup
		warmupResult, warmupErr := runCohort(warmupConfig, "warmup", warmupConfig.Cohort, config.Warmup)
		warmup = &warmupResult
		if warmupErr != nil || warmupResult.Verdict == verdictInvalid {
			if warmupErr == nil {
				warmupErr = errors.New("warmup cohort is invalid")
			}
			return failedCohortResult("warmup", warmupResult.Sender, warmupResult.Receiver, fmt.Errorf("warmup did not drain cleanly: %w", warmupErr)), warmupErr
		}
	}
	measurement, err := runCohort(config, "measurement", config.Cohort, config.Duration)
	config.ssnmRun.finish(ctx, &measurement)
	if config.Mode == modeBidirectional {
		select {
		case readErr := <-localFatal:
			if measurement.Error == "" {
				measurement.Error = readErr.Error()
			}
			measurement.Verdict = verdictInvalid
		default:
		}
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

// startLocalReceiver runs the ASP's own control endpoint and read loop for
// the reverse direction of a bidirectional run. The SGP reverse driver owns
// the cohort lifecycle against it exactly as the ASP owns the forward cohort
// against the SGP.
func startLocalReceiver(ctx context.Context, config commandConfig, associations []*m3ua.Association) (func(), chan error, error) {
	control := newReceiverControl(config.Associations, maxOutstanding)
	control.enableSharedClock(config.SameHostClock)
	if control.fatal != "" {
		return nil, nil, errors.New(control.fatal)
	}
	control.cpuStatPath = config.CPUStatPath
	httpListener, err := net.Listen("tcp", config.ControlAddress)
	if err != nil {
		return nil, nil, fmt.Errorf("listen for local receiver control: %w", err)
	}
	httpServer := &http.Server{Handler: control.handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		_ = httpServer.Serve(httpListener)
	}()
	fatal := make(chan error, 1)
	for index, association := range associations {
		control.setAssociationReady(index, int(association.MaxMessageStreamID()))
		go readAssociation(ctx, index, association, control, fatal)
	}
	shutdown := func() {
		shutdownContext, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownContext)
	}
	return shutdown, fatal, nil
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
				result.Error = joinErrorText(result.Error, "reverse cohort receiver: "+receiver.FatalError)
				return
			}
		}
		if !time.Now().Before(deadline) {
			result.Verdict = verdictInvalid
			result.Error = joinErrorText(result.Error, "reverse cohort did not complete before the collection deadline")
			return
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			result.Verdict = verdictInvalid
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
	result := cohortResult{Phase: phase, Sender: sender, Receiver: receiver, Verdict: verdictPass}
	if sender.Verdict == verdictInvalid || receiver.Verdict == verdictInvalid || err != nil {
		result.Verdict = verdictInvalid
	} else if sender.Verdict == verdictInconclusive || receiver.Verdict == verdictInconclusive {
		result.Verdict = verdictInconclusive
	}
	if err != nil {
		result.Error = err.Error()
	}
	return result
}

func failedCohortResult(phase string, sender, receiver runRecord, err error) combinedResult {
	cohort := newCohortResult(phase, sender, receiver, err)
	result := combinedResult{
		Phase:    phase,
		Sender:   sender,
		Receiver: receiver,
		Verdict:  cohort.Verdict,
		Error:    cohort.Error,
	}
	if phase == "warmup" {
		result.Warmup = &cohort
	} else {
		result.Measurement = &cohort
	}
	return result
}

func runSenderCohort(ctx context.Context, config commandConfig, associations []*m3ua.Association, registry *echoRegistry, cohort string, duration time.Duration) (runRecord, runRecord, error) {
	// runSender creates the reply registry exactly when the mode is echo;
	// every other caller (throughput, the bidirectional reverse driver, tests)
	// passes nil. Name a mismatched call instead of dereferencing nil.
	if config.Mode == modeEcho && registry == nil {
		return runRecord{}, runRecord{}, errors.New("echo mode requires an echo reply registry")
	}
	expected, err := scheduledMessages(config.Rate, duration)
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
	clock, err := prepareSharedRunClock(ctx, config, &specification)
	if err != nil {
		return runRecord{}, runRecord{}, fmt.Errorf("prepare shared clock: %w", err)
	}
	if err := config.ssnmRun.attach(&specification, config.ssnmPhase); err != nil {
		return runRecord{}, runRecord{}, fmt.Errorf("declare SSNM load: %w", err)
	}
	if err := postJSON(ctx, config.PeerControl+"/reset", specification); err != nil {
		return runRecord{}, runRecord{}, fmt.Errorf("reset receiver: %w", err)
	}
	cpuBefore, cpuBeforeErr := readCPUStat(config.CPUStatPath)
	allocBefore := readRuntimeCounters()
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
	queues, workersDone := startSendWorkers(associations, config, counters, tracker)
	sampleDone := make(chan struct{})
	go sampleSharedSender(started, counters, sampleDone, clock)
	progressDone := sampleSharedProgress(ctx, started, duration, config.PeerControl, clock)
	dispatchScheduled(ctx, config, cohort, duration, started, expected, queues, counters, tracker, clock)
	outstandingAtEnd := counters.outstandingCount()
	close(sampleDone)
	observations := append([]progressObservation{initialObservation}, (<-progressDone)...)
	for _, queue := range queues {
		close(queue)
	}
	drainStarted := time.Now()
	boundaryContext, cancelBoundary := context.WithDeadline(ctx, drainDeadline)
	observations = append(observations, observeSharedProgress(boundaryContext, started, config.PeerControl, clock))
	cancelBoundary()
	drained := waitWorkersContext(ctx, workersDone, remainingUntil(drainDeadline))
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
	drainContext, cancelDrain := context.WithDeadline(ctx, drainDeadline)
	receiver, pollErr := waitReceiverDrain(drainContext, config.PeerControl, counters, drainDeadline)
	observations = append(observations, observeSharedProgress(drainContext, started, config.PeerControl, clock))
	cancelDrain()
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
	cpuAfter, cpuAfterErr := readCPUStat(config.CPUStatPath)
	allocAfter := readRuntimeCounters()
	sender := counters.result(specification, duration, drainDuration, outstandingAtEnd)
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
	sender.evaluate()
	var cohortErrors []error
	if pollErr != nil {
		cohortErrors = append(cohortErrors, pollErr)
	}
	if stopErr != nil {
		cohortErrors = append(cohortErrors, stopErr)
	}
	if sender.Verdict == verdictInvalid || receiver.Verdict == verdictInvalid {
		cohortErrors = append(cohortErrors, errors.New("cohort is invalid; inspect machine-readable reasons"))
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
	var previousElapsed time.Duration
	if clock != nil {
		elapsed, err := clock.elapsed()
		if err != nil || elapsed >= 0 {
			counters.abort(expected, errors.New("shared measurement start was missed during preparation"))
			return
		}
		previousElapsed = elapsed
	}
	for index := uint64(0); index < expected; {
		if err := ctx.Err(); err != nil {
			counters.abort(expected-index, err)
			return
		}
		elapsed := time.Since(started)
		if clock != nil {
			var err error
			elapsed, err = clock.elapsed()
			if err != nil || elapsed < previousElapsed {
				counters.abort(expected-index, errors.New("shared scheduler clock failed or regressed"))
				return
			}
			previousElapsed = elapsed
		}
		due := uint64(0)
		if elapsed > 0 {
			due = uint64(elapsed)*config.Rate/uint64(time.Second) + 1
		}
		if due > expected {
			due = expected
		}
		if due <= index {
			timer := time.NewTimer(schedulerQuantum)
			select {
			case <-ctx.Done():
				timer.Stop()
				counters.abort(expected-index, ctx.Err())
				return
			case <-timer.C:
			}
			continue
		}
		for index < due {
			if index%256 == 0 {
				if err := ctx.Err(); err != nil {
					counters.abort(expected-index, err)
					return
				}
			}
			offset := time.Duration(index * uint64(time.Second) / config.Rate)
			identity := planMessage(cohort, config.Seed, index, len(queues))
			if tracker != nil {
				identity.Kind = kindEchoRequest
			}
			job := sendJob{
				identity: identity, scheduled: started.Add(offset),
				clock: clock, offset: offset,
				size: config.Workload.size(index), reverse: config.Direction == directionSGPToASP,
			}
			counters.schedule()
			if tracker != nil && !tracker.admit(index, job.scheduled) {
				counters.capOne()
				index++
				continue
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
			index++
		}
	}
	if clock != nil {
		if err := clock.waitUntil(ctx, clock.window.End); err != nil {
			counters.setFatal(err.Error())
		}
		return
	}
	remaining := time.Until(started.Add(duration))
	if remaining > 0 {
		timer := time.NewTimer(remaining)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
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
		if counters.fatal == "" {
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
}

func (counters *senderCounters) setFatal(reason string) {
	counters.mutex.Lock()
	defer counters.mutex.Unlock()
	if counters.fatal == "" {
		counters.fatal = reason
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

func waitReceiverDrain(ctx context.Context, baseURL string, counters *senderCounters, deadline time.Time) (runRecord, error) {
	for {
		if !time.Now().Before(deadline) {
			return runRecord{}, errors.New("receiver drain deadline exceeded")
		}
		requestContext, cancelRequest := context.WithDeadline(ctx, deadline)
		receiver, err := getReceiverResult(requestContext, baseURL)
		cancelRequest()
		if err != nil {
			return runRecord{}, err
		}
		submitted := counters.submittedCount()
		accounted := receiver.Delivery.Unique + receiver.Delivery.Invalid + receiver.Delivery.Duplicate
		if accounted >= submitted {
			return receiver, nil
		}
		if !time.Now().Before(deadline) {
			return receiver, errors.New("receiver drain deadline exceeded")
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return receiver, ctx.Err()
		case <-timer.C:
		}
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
