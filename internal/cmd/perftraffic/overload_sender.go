package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/gomaja/go-m3ua"
)

// overloadOutcome is the single class one offered message ends in.
type overloadOutcome uint8

const (
	// outcomeAccepted: WriteData accepted the message.
	outcomeAccepted overloadOutcome = iota
	// outcomeNotSent: WriteData refused it as m3ua.DataNotSent.
	outcomeNotSent
	// outcomeIndeterminate: WriteData reported m3ua.DataSendIndeterminate.
	outcomeIndeterminate
	// outcomeUnclassified: WriteData failed outside its documented
	// *m3ua.DataWriteError contract, or reported a short write.
	outcomeUnclassified
	// outcomeDeadlineExpired: the request's two-second deadline passed while
	// it waited in the fixture queue, so it was never offered to WriteData.
	outcomeDeadlineExpired
	// outcomeFixtureError: the fixture itself failed (clock or deadline
	// setup); the run is invalid.
	outcomeFixtureError
)

// notSentCause is why WriteData refused a message it reported not sent.
type notSentCause uint8

const (
	causeSendBufferFull notSentCause = iota
	causeWriteDeadline
	causeNotEstablished
	causeOther
)

type overloadNotSentCounts struct {
	SendBufferFull uint64 `json:"send_buffer_full"`
	WriteDeadline  uint64 `json:"write_deadline"`
	NotEstablished uint64 `json:"not_established"`
	Other          uint64 `json:"other"`
}

func (counts overloadNotSentCounts) total() uint64 {
	return counts.SendBufferFull + counts.WriteDeadline + counts.NotEstablished + counts.Other
}

func (counts *overloadNotSentCounts) add(cause notSentCause) {
	switch cause {
	case causeSendBufferFull:
		counts.SendBufferFull++
	case causeWriteDeadline:
		counts.WriteDeadline++
	case causeNotEstablished:
		counts.NotEstablished++
	default:
		counts.Other++
	}
}

// overloadClassCounts counts offered messages by the one class each ended in.
// Unresolved is offered work that reached no class, which the fixture must
// never leave behind.
type overloadClassCounts struct {
	Offered               uint64                `json:"offered"`
	Accepted              uint64                `json:"accepted"`
	NotSent               overloadNotSentCounts `json:"not_sent"`
	Indeterminate         uint64                `json:"indeterminate"`
	CapRefusedOutstanding uint64                `json:"cap_refused_outstanding"`
	CapRefusedQueue       uint64                `json:"cap_refused_queue"`
	DeadlineExpired       uint64                `json:"deadline_expired"`
	Unclassified          uint64                `json:"unclassified"`
	FixtureErrors         uint64                `json:"fixture_errors"`
	Aborted               uint64                `json:"aborted"`
	Unresolved            uint64                `json:"unresolved"`
}

// classified is the number of offered messages that reached a class.
func (counts overloadClassCounts) classified() uint64 {
	return counts.Accepted + counts.NotSent.total() + counts.Indeterminate + counts.CapRefusedOutstanding +
		counts.CapRefusedQueue + counts.DeadlineExpired + counts.Unclassified + counts.FixtureErrors + counts.Aborted
}

func (counts overloadClassCounts) plus(other overloadClassCounts) overloadClassCounts {
	counts.Offered += other.Offered
	counts.Accepted += other.Accepted
	counts.NotSent.SendBufferFull += other.NotSent.SendBufferFull
	counts.NotSent.WriteDeadline += other.NotSent.WriteDeadline
	counts.NotSent.NotEstablished += other.NotSent.NotEstablished
	counts.NotSent.Other += other.NotSent.Other
	counts.Indeterminate += other.Indeterminate
	counts.CapRefusedOutstanding += other.CapRefusedOutstanding
	counts.CapRefusedQueue += other.CapRefusedQueue
	counts.DeadlineExpired += other.DeadlineExpired
	counts.Unclassified += other.Unclassified
	counts.FixtureErrors += other.FixtureErrors
	counts.Aborted += other.Aborted
	counts.Unresolved += other.Unresolved
	return counts
}

// overloadSenderMark is the sender's attempt accounting at one instant. It
// brackets each receiver progress snapshot so the admitted backlog can be
// bounded: at least Accepted was admitted, at most Started minus the attempts
// definitely refused.
type overloadSenderMark struct {
	Accepted uint64 `json:"accepted"`
	Started  uint64 `json:"started"`
	Refused  uint64 `json:"refused"`
}

type overloadSenderPoint struct {
	OffsetMillis uint64 `json:"offset_ms"`
	overloadClassCounts
	Started     uint64 `json:"started"`
	Outstanding uint64 `json:"outstanding"`
}

// overloadCounters is the sender's overload accounting, guarded by the
// senderCounters mutex.
type overloadCounters struct {
	schedule       *phasedSchedule
	phases         []overloadClassCounts
	started        uint64
	refused        uint64
	maxOutstanding uint64
	accepted       bitmap
	indeterminate  bitmap
	notAdmitted    []uint64
	errorSamples   map[string]string
	series         []overloadSenderPoint
}

func newOverloadCounters(schedule *phasedSchedule) *overloadCounters {
	seconds := (schedule.duration + time.Second - 1) / time.Second
	return &overloadCounters{
		schedule:      schedule,
		phases:        make([]overloadClassCounts, len(schedule.phases)),
		accepted:      newBitmap(schedule.expected),
		indeterminate: newBitmap(schedule.expected),
		notAdmitted:   make([]uint64, seconds),
		errorSamples:  make(map[string]string),
	}
}

func (overload *overloadCounters) totals() overloadClassCounts {
	var total overloadClassCounts
	for _, phase := range overload.phases {
		total = total.plus(phase)
	}
	return total
}

func (overload *overloadCounters) phase(index uint64) *overloadClassCounts {
	return &overload.phases[overload.schedule.phaseOfIndex(index)]
}

// countNotAdmitted buckets a message that was not accepted by its scheduled
// second, so refusals after recovery can be located.
func (overload *overloadCounters) countNotAdmitted(index uint64) {
	second := int(overload.schedule.offset(index) / time.Second)
	if second >= 0 && second < len(overload.notAdmitted) {
		overload.notAdmitted[second]++
	}
}

func (overload *overloadCounters) sample(key string, err error) {
	if err == nil || len(overload.errorSamples) >= 16 {
		return
	}
	if _, present := overload.errorSamples[key]; !present {
		overload.errorSamples[key] = err.Error()
	}
}

func (counters *senderCounters) offerOverload(index uint64) {
	counters.mutex.Lock()
	counters.overload.phase(index).Offered++
	counters.mutex.Unlock()
}

// reserveOverload admits one message under the outstanding cap or counts it
// as a cap refusal.
func (counters *senderCounters) reserveOverload(index uint64) bool {
	counters.mutex.Lock()
	defer counters.mutex.Unlock()
	if counters.outstanding >= counters.limit {
		counters.capped++
		counters.overload.phase(index).CapRefusedOutstanding++
		counters.overload.countNotAdmitted(index)
		return false
	}
	counters.outstanding++
	counters.overload.maxOutstanding = max(counters.overload.maxOutstanding, counters.outstanding)
	return true
}

// rejectOverloadReservation releases a reservation whose per-association
// queue was full: the message is a cap refusal too.
func (counters *senderCounters) rejectOverloadReservation(index uint64) {
	counters.mutex.Lock()
	defer counters.mutex.Unlock()
	counters.outstanding--
	counters.capped++
	counters.overload.phase(index).CapRefusedQueue++
	counters.overload.countNotAdmitted(index)
}

func (counters *senderCounters) beginOverloadAttempt() {
	counters.mutex.Lock()
	counters.overload.started++
	counters.mutex.Unlock()
}

// completeOverload records the class one dequeued message ended in. Refusals
// are the expected outcome of an overload trial and never set the fatal
// error; only fixture failures do.
func (counters *senderCounters) completeOverload(index uint64, outcome overloadOutcome, cause notSentCause, err error, dispatchLag, sendDuration time.Duration, attempted bool) {
	counters.mutex.Lock()
	defer counters.mutex.Unlock()
	if counters.outstanding > 0 {
		counters.outstanding--
	}
	overload := counters.overload
	phase := overload.phase(index)
	switch outcome {
	case outcomeAccepted:
		counters.submitted++
		phase.Accepted++
		overload.accepted.add(index)
	case outcomeNotSent:
		counters.sendErrors++
		phase.NotSent.add(cause)
		overload.refused++
		overload.sample(fmt.Sprintf("not_sent/%d", cause), err)
	case outcomeIndeterminate:
		counters.sendErrors++
		phase.Indeterminate++
		overload.indeterminate.add(index)
		overload.sample("indeterminate", err)
	case outcomeUnclassified:
		counters.sendErrors++
		phase.Unclassified++
		overload.sample("unclassified", err)
	case outcomeDeadlineExpired:
		counters.capped++
		phase.DeadlineExpired++
	default:
		counters.sendErrors++
		phase.FixtureErrors++
		overload.sample("fixture", err)
		if counters.fatal == "" && err != nil {
			counters.fatal = err.Error()
		}
	}
	if outcome != outcomeAccepted {
		overload.countNotAdmitted(index)
	}
	counters.dispatchLag.record(dispatchLag)
	if attempted {
		counters.sendTime.record(sendDuration)
	}
}

// abortOverload attributes the never-emitted remainder of an aborted schedule
// to its phases.
func (overload *overloadCounters) abortRemaining(remaining uint64) {
	first := overload.schedule.expected - min(remaining, overload.schedule.expected)
	for phase := range overload.schedule.phases {
		scheduled := overload.schedule.phases[phase]
		begin := max(first, scheduled.first)
		end := scheduled.first + scheduled.count
		if begin < end {
			overload.phases[phase].Offered += end - begin
			overload.phases[phase].Aborted += end - begin
		}
	}
}

func (counters *senderCounters) overloadMark() overloadSenderMark {
	counters.mutex.Lock()
	defer counters.mutex.Unlock()
	return overloadSenderMark{Accepted: counters.submitted, Started: counters.overload.started, Refused: counters.overload.refused}
}

// sampleOverloadLocked appends one per-second sender overload point. It runs
// under the counters mutex from the sender sampler.
func (counters *senderCounters) sampleOverloadLocked(offset time.Duration) {
	overload := counters.overload
	if overload == nil || len(overload.series) >= 601 {
		return
	}
	overload.series = append(overload.series, overloadSenderPoint{
		OffsetMillis: uint64(offset / time.Millisecond), overloadClassCounts: overload.totals(),
		Started: overload.started, Outstanding: counters.outstanding,
	})
}

// classifyDataWrite maps one WriteData result to its overload class.
func classifyDataWrite(written, size int, err error) (overloadOutcome, notSentCause) {
	if err == nil {
		if written != size {
			return outcomeUnclassified, causeOther
		}
		return outcomeAccepted, causeOther
	}
	var dataErr *m3ua.DataWriteError
	if !errors.As(err, &dataErr) {
		return outcomeUnclassified, causeOther
	}
	switch dataErr.Outcome {
	case m3ua.DataNotSent:
		switch {
		case errors.Is(err, os.ErrDeadlineExceeded):
			return outcomeNotSent, causeWriteDeadline
		case errors.Is(err, syscall.EAGAIN), errors.Is(err, syscall.EWOULDBLOCK):
			return outcomeNotSent, causeSendBufferFull
		case errors.Is(err, m3ua.ErrNotEstablished):
			return outcomeNotSent, causeNotEstablished
		default:
			return outcomeNotSent, causeOther
		}
	case m3ua.DataSendIndeterminate:
		return outcomeIndeterminate, causeOther
	default:
		return outcomeUnclassified, causeOther
	}
}

// overloadWriter is the part of an association the overload worker uses.
type overloadWriter interface {
	WriteData(m3ua.DataRequest) (int, error)
	SetWriteDeadline(time.Time) error
}

// sendOverloadJob offers one dequeued message to WriteData under its
// two-second request deadline and records the class it ends in. A request
// whose deadline already passed in the fixture queue is not offered. The
// association's write deadline is the request deadline only for the duration
// of the call, then returns to the cohort's drain deadline, so a request
// deadline never bounds a write the library makes on its own behalf.
func sendOverloadJob(connection overloadWriter, job sendJob, drainDeadline time.Time, counters *senderCounters) {
	index := globalIndex(job.identity)
	lag, clockErr := job.dispatchDelay()
	if clockErr != nil {
		counters.completeOverload(index, outcomeFixtureError, causeOther, clockErr, 0, 0, false)
		return
	}
	if lag >= overloadRequestDeadline {
		counters.completeOverload(index, outcomeDeadlineExpired, causeOther, nil, lag, 0, false)
		return
	}
	payload := buildPayload(job.identity, job.size)
	request := tupleFor(job.identity.Flow, job.identity.Association).dataRequest(payload)
	deadline := time.Now().Add(overloadRequestDeadline - lag)
	if deadline.After(drainDeadline) {
		deadline = drainDeadline
	}
	if err := connection.SetWriteDeadline(deadline); err != nil {
		counters.completeOverload(index, outcomeFixtureError, causeOther, fmt.Errorf("set request write deadline: %w", err), lag, 0, false)
		return
	}
	counters.beginOverloadAttempt()
	sendStarted := time.Now()
	written, sendErr := connection.WriteData(request)
	sendDuration := time.Since(sendStarted)
	restoreErr := connection.SetWriteDeadline(drainDeadline)
	outcome, cause := classifyDataWrite(written, job.size, sendErr)
	if sendErr == nil && written != job.size {
		sendErr = fmt.Errorf("WriteData wrote %d bytes, want %d", written, job.size)
	}
	counters.completeOverload(index, outcome, cause, sendErr, lag, sendDuration, true)
	var fixtureErr error
	if restoreErr != nil {
		fixtureErr = fmt.Errorf("restore cohort write deadline: %w", restoreErr)
	}
	if job.clock != nil {
		if err := job.clock.withinDrain(job.offset + lag); err != nil {
			fixtureErr = errors.Join(fixtureErr, err)
		}
	}
	if fixtureErr != nil {
		counters.setFatal(fixtureErr.Error())
	}
}

func startOverloadSendWorkers(associations []*m3ua.Association, config commandConfig, counters *senderCounters, drainDeadline time.Time) ([]chan sendJob, <-chan struct{}) {
	capacities := queueCapacities(len(associations), config.Outstanding)
	queues := make([]chan sendJob, len(associations))
	var workers sync.WaitGroup
	workers.Add(len(associations))
	for index, association := range associations {
		queues[index] = make(chan sendJob, capacities[index])
		go func(connection *m3ua.Association, jobs <-chan sendJob) {
			defer workers.Done()
			for job := range jobs {
				sendOverloadJob(connection, job, drainDeadline, counters)
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

// dispatchOverload offers the phased schedule open loop through the shared
// scheduler and returns the largest occupancy each per-association fixture
// queue reached.
func dispatchOverload(ctx context.Context, config commandConfig, cohort string, schedule *phasedSchedule, started time.Time, queues []chan sendJob, counters *senderCounters, clock *sharedRunClock) []int {
	maxima := make([]int, len(queues))
	dispatchScheduleOpenLoop(ctx, schedule, schedule.duration, started, schedule.expected, clock, counters, func(index uint64, offset time.Duration, scheduled time.Time) {
		counters.offerOverload(index)
		identity := planMessage(cohort, config.Seed, index, len(queues))
		job := sendJob{identity: identity, scheduled: scheduled, clock: clock, offset: offset, size: config.Workload.size(index)}
		if !counters.reserveOverload(index) {
			return
		}
		queue := queues[identity.Association]
		select {
		case queue <- job:
			maxima[identity.Association] = max(maxima[identity.Association], len(queue))
		default:
			counters.rejectOverloadReservation(index)
		}
	})
	return maxima
}

// fetchDeliveredLedger reads the stopped receiver's per-message delivered
// ledger for the given cohort generation.
func fetchDeliveredLedger(ctx context.Context, baseURL string, generation, messages uint64) (bitmap, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/overload/delivered?generation="+strconv.FormatUint(generation, 10), nil)
	if err != nil {
		return nil, err
	}
	response, err := fixtureHTTPClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("delivered ledger: %s", response.Status)
	}
	limit := int64((messages+63)/64*8) + 1
	body, err := io.ReadAll(io.LimitReader(response.Body, limit))
	if err != nil {
		return nil, err
	}
	return decodeBitmap(body, messages)
}

// overloadAssociationState is one sender association's state at the end of
// the cohort.
func senderAssociationObservations(associations []*m3ua.Association, epochs []uint64) []overloadAssociationObservation {
	observations := make([]overloadAssociationObservation, len(associations))
	for index, association := range associations {
		state := association.State()
		stats := association.DataQueueStats()
		observations[index] = overloadAssociationObservation{
			Index: index, QueueCapacity: stats.Capacity, MaxQueued: stats.Queued, Discarded: stats.Discarded,
			CongestedAtEnd: stats.Congested, State: state.String(), Active: state == m3ua.StateASPActive,
			EpochStart: epochs[index], EpochEnd: association.Epoch(),
		}
	}
	return observations
}

// collectOverloadEvidence gathers the overload measurement cohort's evidence
// once the receiver has stopped and evaluates it.
func collectOverloadEvidence(ctx context.Context, config commandConfig, specification runSpec, profile *overloadProfile, counters *senderCounters, associations []*m3ua.Association, epochs []uint64, fixtureQueueMax []int, receiver runRecord, generation uint64, stopErr error, observations []progressObservation, sender *runRecord) *overloadRecord {
	evidence := overloadEvidence{
		specification: specification, schedule: profile.schedule, fixtureQueueMax: fixtureQueueMax,
		senderAssociations: senderAssociationObservations(associations, epochs),
		receiver:           receiver, observations: observations, senderFatal: sender.FatalError,
		senderWindow: sender.SenderWindow,
	}
	if sender.ClockEvidence != nil {
		evidence.clockVerified = sender.ClockEvidence.Verified
	}
	if stopErr != nil {
		evidence.deliveredErr = fmt.Errorf("receiver did not stop cleanly: %w", stopErr)
	} else {
		evidence.delivered, evidence.deliveredErr = fetchDeliveredLedger(ctx, config.PeerControl, generation, profile.expected())
	}
	counters.mutex.Lock()
	overload := counters.overload
	evidence.phases = append([]overloadClassCounts(nil), overload.phases...)
	evidence.maxOutstanding = overload.maxOutstanding
	evidence.accepted = overload.accepted
	evidence.indeterminate = overload.indeterminate
	evidence.notAdmitted = append([]uint64(nil), overload.notAdmitted...)
	evidence.series = append([]overloadSenderPoint(nil), overload.series...)
	evidence.errorSamples = make(map[string]string, len(overload.errorSamples))
	for key, value := range overload.errorSamples {
		evidence.errorSamples[key] = value
	}
	counters.mutex.Unlock()
	return evaluateOverloadCohort(evidence)
}
