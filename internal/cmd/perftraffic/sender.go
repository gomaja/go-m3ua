package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/gomaja/go-m3ua"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
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
	Phase    string    `json:"phase"`
	Sender   runRecord `json:"sender"`
	Receiver runRecord `json:"receiver"`
	Verdict  string    `json:"verdict"`
	Error    string    `json:"error,omitempty"`
}

type sendJob struct {
	identity  messageIdentity
	scheduled time.Time
	size      int
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
	remoteAddress, err := sctp.ResolveSCTPAddr("sctp", config.SCTPAddress)
	if err != nil {
		return combinedResult{}, fmt.Errorf("resolve SGP address: %w", err)
	}
	var localAddress *sctp.SCTPAddr
	if config.LocalAddress != "" {
		localAddress, err = sctp.ResolveSCTPAddr("sctp", config.LocalAddress)
		if err != nil {
			return combinedResult{}, fmt.Errorf("resolve ASP local address: %w", err)
		}
	}
	endpoint, err := m3ua.NewEndpoint(m3ua.EndpointConfig{Role: m3ua.RoleASP, ASP: nil})
	if err != nil {
		return combinedResult{}, fmt.Errorf("create standalone ASP endpoint: %w", err)
	}
	defer func() { _ = endpoint.Close() }()
	associations := make([]*m3ua.Association, 0, config.Associations)
	for index := 0; index < config.Associations; index++ {
		association, dialErr := endpoint.Dial(ctx, "m3ua", localAddress, remoteAddress, associationConfig("asp"))
		if dialErr != nil {
			return combinedResult{}, fmt.Errorf("dial association %d: %w", index, dialErr)
		}
		associations = append(associations, association)
	}
	if err := waitForReady(ctx, config.PeerControl, config.Associations); err != nil {
		return combinedResult{}, err
	}
	var warmup *cohortResult
	if config.Warmup > 0 {
		warmupConfig := config
		warmupConfig.Cohort += "-warmup"
		warmupSender, warmupReceiver, warmupErr := runSenderCohort(ctx, warmupConfig, associations, warmupConfig.Cohort, config.Warmup)
		warmupResult := newCohortResult("warmup", warmupSender, warmupReceiver, warmupErr)
		warmup = &warmupResult
		if warmupErr != nil || warmupSender.Verdict == verdictInvalid || warmupReceiver.Verdict == verdictInvalid {
			if warmupErr == nil {
				warmupErr = errors.New("warmup cohort is invalid")
			}
			return failedCohortResult("warmup", warmupSender, warmupReceiver, fmt.Errorf("warmup did not drain cleanly: %w", warmupErr)), warmupErr
		}
	}
	sender, receiver, err := runSenderCohort(ctx, config, associations, config.Cohort, config.Duration)
	measurement := newCohortResult("measurement", sender, receiver, err)
	result := combinedResult{
		Phase:       "measurement",
		Warmup:      warmup,
		Measurement: &measurement,
		Sender:      sender,
		Receiver:    receiver,
		Verdict:     measurement.Verdict,
		Error:       measurement.Error,
	}
	return result, err
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

func runSenderCohort(ctx context.Context, config commandConfig, associations []*m3ua.Association, cohort string, duration time.Duration) (runRecord, runRecord, error) {
	expected, err := scheduledMessages(config.Rate, duration)
	if err != nil {
		return runRecord{}, runRecord{}, fmt.Errorf("calculate scheduled messages: %w", err)
	}
	if expected == 0 {
		return runRecord{}, runRecord{}, errors.New("cohort schedules no messages")
	}
	specification := runSpec{Cohort: cohort, Seed: config.Seed, Associations: len(associations), Expected: expected, Duration: duration, Rate: config.Rate, Payload: config.Workload}
	if err := postJSON(ctx, config.PeerControl+"/reset", specification); err != nil {
		return runRecord{}, runRecord{}, fmt.Errorf("reset receiver: %w", err)
	}
	cpuBefore, cpuBeforeErr := readCPUStat(config.CPUStatPath)
	allocBefore := readRuntimeCounters()
	if err := postJSON(ctx, config.PeerControl+"/start", nil); err != nil {
		return runRecord{}, runRecord{}, fmt.Errorf("start receiver: %w", err)
	}

	initialBefore := time.Now()
	initialProgress, err := getReceiverProgress(ctx, config.PeerControl)
	initialObservation := progressObservation{Before: 0, After: time.Since(initialBefore), Snapshot: &initialProgress}
	if err != nil {
		initialObservation.Snapshot = nil
		initialObservation.Error = err.Error()
		return stopFailedProgress(config.PeerControl, specification, initialObservation, fmt.Errorf("read initial receiver progress: %w", err))
	}
	if initialProgress.Spec != specification || initialProgress.Generation == 0 || initialProgress.Phase != receiverMeasuring || initialProgress.Delivery.Unique != 0 || initialProgress.Delivery.Missing != expected || initialProgress.Delivery.Invalid != 0 || initialProgress.Delivery.Duplicate != 0 || initialProgress.Delivery.Reordered != 0 || initialProgress.FatalError != "" {
		return stopFailedProgress(config.PeerControl, specification, initialObservation, errors.New("receiver progress is not an empty active cohort"))
	}
	started := time.Now()
	initialObservation.Before = initialBefore.Sub(started)
	initialObservation.After = 0
	for _, association := range associations {
		if deadlineErr := association.SetWriteDeadline(started.Add(duration + config.Drain)); deadlineErr != nil {
			_ = postJSON(ctx, config.PeerControl+"/stop", nil)
			return runRecord{}, runRecord{}, fmt.Errorf("set association write deadline: %w", deadlineErr)
		}
	}
	counters := newSenderCounters(config.Outstanding)
	queues, workersDone := startSendWorkers(associations, config, counters)
	sampleDone := make(chan struct{})
	go sampleSender(started, counters, sampleDone)
	samplingContext, cancelSampling := context.WithDeadline(ctx, started.Add(duration))
	defer cancelSampling()
	progressDone := sampleProgress(samplingContext, started, duration, config.PeerControl)
	dispatchScheduled(ctx, config, cohort, duration, started, expected, queues, counters)
	outstandingAtEnd := counters.outstandingCount()
	close(sampleDone)
	cancelSampling()
	observations := append([]progressObservation{initialObservation}, (<-progressDone)...)
	for _, queue := range queues {
		close(queue)
	}
	drainStarted := time.Now()
	drainDeadline := started.Add(duration + config.Drain)
	boundaryContext, cancelBoundary := context.WithDeadline(ctx, drainDeadline)
	observations = append(observations, observeProgress(boundaryContext, started, config.PeerControl))
	cancelBoundary()
	drained := waitWorkersContext(ctx, workersDone, remainingUntil(drainDeadline))
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
	observations = append(observations, observeProgress(drainContext, started, config.PeerControl))
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
	if stopErr != nil {
		counters.setFatal(fmt.Sprintf("stop receiver: %v", stopErr))
	}
	cpuAfter, cpuAfterErr := readCPUStat(config.CPUStatPath)
	allocAfter := readRuntimeCounters()
	sender := counters.result(specification, duration, drainDuration, outstandingAtEnd)
	sender.CPU = newCPUObservation(cpuBefore, cpuAfter, cpuBeforeErr, cpuAfterErr, receiver.Delivery.Unique)
	sender.Allocations = AllocationObservation{Scope: wholeProcessScope, Before: allocBefore, After: allocAfter, Delta: runtimeDelta(allocBefore, allocAfter)}
	sender.Delivery = receiver.Delivery
	sender.NegotiatedOutboundStreams = make([]int, len(associations))
	for index, association := range associations {
		sender.NegotiatedOutboundStreams[index] = int(association.MaxMessageStreamID()) + 1
	}
	sender.Manifest = currentManifest(config.Outstanding)
	sender.ProgressObservations = observations
	accounting := analyzeProgress(specification, observations)
	sender.SenderWindow = &accounting
	sender.ValidatedPerSecond = accounting.RateLower
	sender.BacklogAssessment = "paired interval diagnostics only; sustained-backlog acceptance is not determined"
	sender.WindowAlignment = "sender monotonic measurement window; rate is a conservative lower bound from bracketed receiver snapshots, not the receiver first-arrival diagnostic"
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

func startSendWorkers(associations []*m3ua.Association, config commandConfig, counters *senderCounters) ([]chan sendJob, <-chan struct{}) {
	capacities := queueCapacities(len(associations), config.Outstanding)
	queues := make([]chan sendJob, len(associations))
	var workers sync.WaitGroup
	workers.Add(len(associations))
	for index, association := range associations {
		queues[index] = make(chan sendJob, capacities[index])
		go func(connection *m3ua.Association, jobs <-chan sendJob) {
			defer workers.Done()
			for job := range jobs {
				dispatchTime := time.Now()
				payload := buildPayload(job.identity, job.size)
				tuple := tupleFor(job.identity.Flow, job.identity.Association)
				protocolDataParam := params.NewProtocolData(tuple.OriginatingPointCode, tuple.DestinationPointCode, tuple.ServiceIndicator, tuple.NetworkIndicator, tuple.MessagePriority, tuple.SignallingLinkSelection, payload)
				sendStarted := time.Now()
				written, sendErr := connection.WritePDWithRoutingContext(protocolDataParam, tuple.RoutingContext)
				if sendErr == nil && written != job.size {
					sendErr = fmt.Errorf("WritePDWithRoutingContext wrote %d bytes, want %d", written, job.size)
				}
				counters.complete(sendErr, dispatchTime.Sub(job.scheduled), time.Since(sendStarted))
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

func dispatchScheduled(ctx context.Context, config commandConfig, cohort string, duration time.Duration, started time.Time, expected uint64, queues []chan sendJob, counters *senderCounters) {
	for index := uint64(0); index < expected; {
		if err := ctx.Err(); err != nil {
			counters.abort(expected-index, err)
			return
		}
		elapsed := time.Since(started)
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
			job := sendJob{identity: identity, scheduled: started.Add(offset), size: config.Workload.size(index)}
			counters.schedule()
			if counters.reserve() {
				select {
				case queues[identity.Association] <- job:
				default:
					counters.rejectReservation()
				}
			}
			index++
		}
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
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			counters.mutex.Lock()
			if len(counters.series) < 601 {
				counters.series = append(counters.series, seriesPoint{
					OffsetMillis: uint64(now.Sub(started) / time.Millisecond),
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
