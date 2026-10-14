package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua"
)

// overloadTestSpec is a two-phase overload measurement at a nominal 10/s:
// 20 messages at 2x (indexes 0-19) and 60 at 0.5x (indexes 20-79).
func overloadTestSpec(testContext *testing.T, associations int) runSpec {
	testContext.Helper()
	profile, err := parseOverloadProfile("2x:1s,0.5x:12s", 10)
	if err != nil {
		testContext.Fatal(err)
	}
	return runSpec{
		Cohort: "overload", Seed: 7, Associations: associations, Expected: profile.expected(), Duration: profile.duration(),
		Drain: overloadRequestDeadline + overloadDrainMargin, Rate: 10, Outstanding: maxOutstanding, Payload: workloadMix,
		Mode: modeThroughput, Direction: directionASPToSGP, Initiation: initiationASPDial,
		Overload: profile.spec(overloadRoleMeasurement),
	}
}

func overloadTestMessage(specification runSpec, index uint64) receivedMessage {
	flow := uint8(index % flowCount)
	return validReceivedMessage(specification.Cohort, specification.Seed, flow%uint8(specification.Associations), flow, index/flowCount, specification.Payload.size(index))
}

func startedOverloadControl(testContext *testing.T, specification runSpec) *receiverControl {
	testContext.Helper()
	control := newReceiverControl(specification.Associations, maxOutstanding)
	for index := 0; index < specification.Associations; index++ {
		control.setAssociationReady(index, 15)
	}
	control.cpuStatPath = writeCPUStatFixture(testContext, "usage_usec 10\nnr_throttled 0\n")
	if err := control.reset(specification); err != nil {
		testContext.Fatalf("reset: %v", err)
	}
	if control.overload == nil {
		testContext.Fatal("an overload measurement reset kept no overload state")
	}
	if err := control.start(); err != nil {
		testContext.Fatalf("start: %v", err)
	}
	return control
}

func TestOverloadReceiverAccountsDeliveriesByPhaseAndServesItsLedger(testContext *testing.T) {
	specification := overloadTestSpec(testContext, 1)
	control := startedOverloadControl(testContext, specification)
	server := httptest.NewServer(control.handler())
	defer server.Close()
	for _, index := range []uint64{0, 5, 25, 0} {
		control.record(0, overloadTestMessage(specification, index))
	}
	misscoped := overloadTestMessage(specification, 7)
	misscoped.RoutingContext++
	control.record(0, misscoped)
	corrupt := overloadTestMessage(specification, 8)
	corrupt.ProtocolData.Data = append([]byte(nil), corrupt.ProtocolData.Data...)
	corrupt.ProtocolData.Data[payloadHeaderSize] ^= 0xff
	control.record(0, corrupt)

	progress := control.progress()
	if progress.Overload == nil || progress.Delivery.Unique != 3 || progress.Delivery.Duplicate != 1 || progress.Delivery.Invalid != 2 {
		testContext.Fatalf("progress = %+v", progress)
	}
	requireHTTPStatus(testContext, http.MethodGet, fmt.Sprintf("%s/overload/delivered?generation=%d", server.URL, control.currentGeneration()), nil, http.StatusConflict)
	if err := control.stop(); err != nil {
		testContext.Fatalf("stop: %v", err)
	}
	requireHTTPStatus(testContext, http.MethodGet, fmt.Sprintf("%s/overload/delivered?generation=%d", server.URL, control.currentGeneration()+1), nil, http.StatusConflict)
	requireHTTPStatus(testContext, http.MethodGet, server.URL+"/overload/delivered", nil, http.StatusBadRequest)
	delivered, err := fetchDeliveredLedger(context.Background(), server.URL, control.currentGeneration(), specification.Expected)
	if err != nil {
		testContext.Fatal(err)
	}
	for index := uint64(0); index < specification.Expected; index++ {
		if delivered.has(index) != (index == 0 || index == 5 || index == 25) {
			testContext.Fatalf("delivered ledger membership of %d = %t", index, delivered.has(index))
		}
	}
	if _, err := fetchDeliveredLedger(context.Background(), server.URL, control.currentGeneration(), specification.Expected+64); err == nil {
		testContext.Fatal("a ledger of the wrong size was accepted")
	}
	record := control.result()
	overload := record.Overload.Receiver
	if overload == nil || overload.UniqueByPhase[0] != 2 || overload.UniqueByPhase[1] != 1 || overload.DuplicateByPhase[0] != 1 ||
		overload.Misscoped != 1 || overload.DeliveredLedger != (overloadLedgerSummary{Messages: 80, Delivered: 3}) || overload.ConfiguredQueueCapacity != dataQueueSize {
		testContext.Fatalf("receiver overload record = %+v", overload)
	}
	if record.Verdict != verdictInvalid {
		testContext.Fatalf("a receiver that saw invalid traffic passed: %+v", record.Reasons)
	}
}

func TestOverloadReceiverRejectsInvalidOverloadSpecifications(testContext *testing.T) {
	control := newReceiverControl(1, maxOutstanding)
	control.setAssociationReady(0, 15)
	specification := overloadTestSpec(testContext, 1)
	specification.Expected++
	if err := control.reset(specification); !errors.Is(err, errInvalidRunSpec) {
		testContext.Fatalf("mismatched expected: %v", err)
	}
	specification = overloadTestSpec(testContext, 1)
	specification.Overload.Phases[0].Rate = 30
	if err := control.reset(specification); !errors.Is(err, errInvalidRunSpec) {
		testContext.Fatalf("mismatched phases: %v", err)
	}
	if control.overload != nil || control.phase != receiverIdle {
		testContext.Fatal("a rejected overload reset changed the receiver")
	}
	// A nominal reset after an overload cohort clears the overload state.
	started := startedOverloadControl(testContext, overloadTestSpec(testContext, 1))
	if err := started.stop(); err != nil {
		testContext.Fatal(err)
	}
	if err := started.reset(runSpec{Cohort: "nominal", Seed: 7, Associations: 1, Expected: 10, Duration: time.Second, Payload: workload128, Rate: 10, Outstanding: maxOutstanding}); err != nil {
		testContext.Fatal(err)
	}
	if started.overload != nil {
		testContext.Fatal("a nominal cohort inherited overload state")
	}
}

// The receiver's overload accounting is shared by the read loops, the
// progress handler, the per-second sampler and the queue poller.
func TestOverloadReceiverAccountingIsSafeForConcurrentUse(testContext *testing.T) {
	specification := overloadTestSpec(testContext, 4)
	control := startedOverloadControl(testContext, specification)
	var group sync.WaitGroup
	for reader := 0; reader < 4; reader++ {
		group.Add(1)
		go func(reader int) {
			defer group.Done()
			for index := uint64(reader); index < specification.Expected; index += 4 {
				control.record(reader, overloadTestMessage(specification, index))
			}
		}(reader)
	}
	done := make(chan struct{})
	var observers sync.WaitGroup
	observers.Add(1)
	go func() {
		defer observers.Done()
		for {
			select {
			case <-done:
				return
			default:
				_ = control.progress()
				control.sample(time.Now())
				_ = control.result()
			}
		}
	}()
	group.Wait()
	close(done)
	observers.Wait()
	if err := control.stop(); err != nil {
		testContext.Fatal(err)
	}
	record := control.result()
	if record.Delivery.Unique != specification.Expected || record.Overload.Receiver.DeliveredLedger.Delivered != specification.Expected ||
		record.Overload.Receiver.UniqueByPhase[0]+record.Overload.Receiver.UniqueByPhase[1] != specification.Expected {
		testContext.Fatalf("concurrent accounting = %+v %+v", record.Delivery, record.Overload.Receiver)
	}
}

type fakeOverloadWriter struct {
	deadlines   []time.Time
	deadlineErr []error
	written     int
	err         error
	writes      int
}

func (writer *fakeOverloadWriter) WriteData(request m3ua.DataRequest) (int, error) {
	writer.writes++
	if writer.written < 0 {
		return len(request.ProtocolData.Data), writer.err
	}
	return writer.written, writer.err
}

func (writer *fakeOverloadWriter) SetWriteDeadline(deadline time.Time) error {
	writer.deadlines = append(writer.deadlines, deadline)
	if len(writer.deadlineErr) > 0 {
		err := writer.deadlineErr[0]
		writer.deadlineErr = writer.deadlineErr[1:]
		return err
	}
	return nil
}

func overloadTestCounters(testContext *testing.T) (*senderCounters, *phasedSchedule) {
	testContext.Helper()
	schedule, err := newPhasedSchedule([]overloadPhase{{Rate: 20, Duration: time.Second}, {Rate: 5, Duration: 12 * time.Second}})
	if err != nil {
		testContext.Fatal(err)
	}
	counters := newSenderCounters(8)
	counters.overload = newOverloadCounters(schedule)
	return counters, schedule
}

func overloadTestJob(index uint64, scheduled time.Time) sendJob {
	identity := planMessage("overload", 7, index, 1)
	return sendJob{identity: identity, scheduled: scheduled, size: workloadMix.size(index)}
}

func TestOverloadSendHonoursTheRequestDeadline(testContext *testing.T) {
	counters, _ := overloadTestCounters(testContext)
	drain := time.Now().Add(time.Minute)

	expired := &fakeOverloadWriter{written: -1}
	counters.reserveOverload(1)
	sendOverloadJob(expired, overloadTestJob(1, time.Now().Add(-overloadRequestDeadline)), drain, counters)
	if expired.writes != 0 || len(expired.deadlines) != 0 || counters.overload.phases[0].DeadlineExpired != 1 || counters.capped != 1 || counters.overload.started != 0 {
		testContext.Fatalf("expired request was offered: writer %+v phase %+v", expired, counters.overload.phases[0])
	}

	accepted := &fakeOverloadWriter{written: -1}
	counters.reserveOverload(2)
	scheduled := time.Now().Add(-500 * time.Millisecond)
	sendOverloadJob(accepted, overloadTestJob(2, scheduled), drain, counters)
	if accepted.writes != 1 || len(accepted.deadlines) != 2 || !accepted.deadlines[1].Equal(drain) {
		testContext.Fatalf("accepted request deadlines = %v", accepted.deadlines)
	}
	if request := accepted.deadlines[0].Sub(scheduled); request < overloadRequestDeadline-50*time.Millisecond || request > overloadRequestDeadline+50*time.Millisecond {
		testContext.Fatalf("request deadline %s after its schedule, want %s", request, overloadRequestDeadline)
	}
	if !counters.overload.accepted.has(2) || counters.submitted != 1 || counters.overload.started != 1 || counters.outstanding != 0 {
		testContext.Fatalf("accepted request not recorded: %+v", counters.overload.phases[0])
	}

	nearDrain := time.Now().Add(100 * time.Millisecond)
	capped := &fakeOverloadWriter{written: -1}
	sendOverloadJob(capped, overloadTestJob(3, time.Now()), nearDrain, counters)
	if !capped.deadlines[0].Equal(nearDrain) {
		testContext.Fatalf("request deadline %v beyond the drain deadline %v", capped.deadlines[0], nearDrain)
	}

	refused := &fakeOverloadWriter{err: &m3ua.DataWriteError{Outcome: m3ua.DataNotSent, Err: os.ErrDeadlineExceeded}}
	sendOverloadJob(refused, overloadTestJob(24, time.Now()), drain, counters)
	if counters.overload.phases[1].NotSent.WriteDeadline != 1 || counters.overload.refused != 1 || counters.fatal != "" || counters.overload.accepted.has(24) {
		testContext.Fatalf("refused request: %+v fatal %q", counters.overload.phases[1], counters.fatal)
	}

	indeterminate := &fakeOverloadWriter{err: &m3ua.DataWriteError{Outcome: m3ua.DataSendIndeterminate, Err: errors.New("reset")}}
	sendOverloadJob(indeterminate, overloadTestJob(25, time.Now()), drain, counters)
	if counters.overload.phases[1].Indeterminate != 1 || !counters.overload.indeterminate.has(25) || counters.fatal != "" {
		testContext.Fatalf("indeterminate request: %+v", counters.overload.phases[1])
	}

	broken := &fakeOverloadWriter{written: -1, deadlineErr: []error{errors.New("closed")}}
	sendOverloadJob(broken, overloadTestJob(26, time.Now()), drain, counters)
	if broken.writes != 0 || counters.overload.phases[1].FixtureErrors != 1 || !strings.Contains(counters.fatal, "set request write deadline") {
		testContext.Fatalf("deadline setup failure: %+v fatal %q", counters.overload.phases[1], counters.fatal)
	}
	var notAdmitted uint64
	for _, count := range counters.overload.notAdmitted {
		notAdmitted += count
	}
	if notAdmitted != 4 {
		testContext.Fatalf("not admitted = %d, want the expiry, refusal, indeterminate and fixture error", notAdmitted)
	}
}

func TestOverloadSendRestoresTheCohortDeadline(testContext *testing.T) {
	counters, _ := overloadTestCounters(testContext)
	writer := &fakeOverloadWriter{written: -1, deadlineErr: []error{nil, errors.New("closed")}}
	sendOverloadJob(writer, overloadTestJob(4, time.Now()), time.Now().Add(time.Minute), counters)
	if !counters.overload.accepted.has(4) || !strings.Contains(counters.fatal, "restore cohort write deadline") {
		testContext.Fatalf("restore failure: accepted %t fatal %q", counters.overload.accepted.has(4), counters.fatal)
	}
}

func TestClassifyDataWriteFollowsTheSendOutcomeContract(testContext *testing.T) {
	notSent := func(cause error) error { return &m3ua.DataWriteError{Outcome: m3ua.DataNotSent, Err: cause} }
	for _, scenario := range []struct {
		name    string
		written int
		err     error
		outcome overloadOutcome
		cause   notSentCause
	}{
		{name: "accepted", written: 128, outcome: outcomeAccepted, cause: causeOther},
		{name: "short", written: 64, outcome: outcomeUnclassified, cause: causeOther},
		{name: "deadline", err: notSent(os.ErrDeadlineExceeded), outcome: outcomeNotSent, cause: causeWriteDeadline},
		{name: "wrapped-deadline", err: fmt.Errorf("send: %w", notSent(fmt.Errorf("poll: %w", os.ErrDeadlineExceeded))), outcome: outcomeNotSent, cause: causeWriteDeadline},
		{name: "eagain", err: notSent(syscall.EAGAIN), outcome: outcomeNotSent, cause: causeSendBufferFull},
		{name: "ewouldblock", err: notSent(syscall.EWOULDBLOCK), outcome: outcomeNotSent, cause: causeSendBufferFull},
		{name: "not-established", err: notSent(m3ua.ErrNotEstablished), outcome: outcomeNotSent, cause: causeNotEstablished},
		{name: "other", err: notSent(m3ua.ErrProtocolDataTooLarge), outcome: outcomeNotSent, cause: causeOther},
		{name: "indeterminate", err: &m3ua.DataWriteError{Outcome: m3ua.DataSendIndeterminate, Err: io.ErrClosedPipe}, outcome: outcomeIndeterminate, cause: causeOther},
		{name: "unknown-outcome", err: &m3ua.DataWriteError{Outcome: 99, Err: io.ErrClosedPipe}, outcome: outcomeUnclassified, cause: causeOther},
		{name: "bare-error", err: io.ErrClosedPipe, outcome: outcomeUnclassified, cause: causeOther},
	} {
		outcome, cause := classifyDataWrite(scenario.written, 128, scenario.err)
		if outcome != scenario.outcome || cause != scenario.cause {
			testContext.Errorf("%s: outcome %d cause %d, want %d %d", scenario.name, outcome, cause, scenario.outcome, scenario.cause)
		}
	}
}

// The sender's overload counters are shared by the scheduler, the workers,
// the per-second sampler and the progress marks.
func TestOverloadSenderCountersAreSafeForConcurrentUse(testContext *testing.T) {
	counters, schedule := overloadTestCounters(testContext)
	counters.limit = schedule.expected
	var group sync.WaitGroup
	for worker := uint64(0); worker < 4; worker++ {
		group.Add(1)
		go func(worker uint64) {
			defer group.Done()
			for index := worker; index < schedule.expected; index += 4 {
				counters.offerOverload(index)
				if counters.reserveOverload(index) {
					counters.beginOverloadAttempt()
					counters.completeOverload(index, overloadOutcome(index%3), causeWriteDeadline, errors.New("x"), 0, time.Microsecond, true)
				}
			}
		}(worker)
	}
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			default:
				_ = counters.overloadMark()
				counters.mutex.Lock()
				counters.sampleOverloadLocked(time.Second)
				counters.mutex.Unlock()
			}
		}
	}()
	group.Wait()
	close(done)
	totals := counters.overload.totals()
	if totals.Offered != schedule.expected || totals.classified() != schedule.expected || counters.overload.started != schedule.expected {
		testContext.Fatalf("concurrent totals = %+v started %d", totals, counters.overload.started)
	}
}

// Library discards are accounted for, not awaited: the drain ends once every
// accepted message is either validated or counted as discarded.
func TestOverloadDrainCountsReceiverDiscards(testContext *testing.T) {
	record := runRecord{Side: "receiver", Delivery: deliveryResult{Unique: 90}, Overload: &overloadRecord{Receiver: &overloadReceiverRecord{Discarded: 10}}}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writeJSON(writer, http.StatusOK, record)
	}))
	defer server.Close()
	counters := newSenderCounters(8)
	counters.submitted = 100
	started := time.Now()
	if _, err := waitReceiverDrain(context.Background(), server.URL, counters, started.Add(time.Second)); err != nil || time.Since(started) > 500*time.Millisecond {
		testContext.Fatalf("drain with discards: %v after %s", err, time.Since(started))
	}
	counters.submitted = 101
	if _, err := waitReceiverDrain(context.Background(), server.URL, counters, time.Now().Add(100*time.Millisecond)); err == nil {
		testContext.Fatal("a missing accepted message did not hold the drain to its deadline")
	}
}
