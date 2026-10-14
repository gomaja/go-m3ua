package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua"
)

// localReadFault is what the ASP-local read loop reports when the association
// carrying the reverse direction is lost.
const localReadFault = "association 0 ReadData in receiver phase measuring: M3UA association not established"

// startFaultedLocalReceiver starts the ASP-local receiver of a bidirectional
// run and reports a read fault on it the way readAssociation does.
func startFaultedLocalReceiver(testContext *testing.T) *localReceiver {
	testContext.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	testContext.Cleanup(cancel)
	local, err := startLocalReceiver(ctx, commandConfig{Associations: 1, ControlAddress: "127.0.0.1:0"}, nil)
	if err != nil {
		testContext.Fatal(err)
	}
	testContext.Cleanup(local.shutdown)
	nonblockingError(local.faults, errors.New(localReadFault))
	for waited := 0; local.fault() == ""; waited++ {
		if waited == 10_000 {
			testContext.Fatal("the local read fault never reached the local control")
		}
		time.Sleep(time.Millisecond)
	}
	return local
}

// A read fault of the ASP-local receiver, the reverse direction's receiver in
// a bidirectional run, reaches its control as it does on the SGP: the reverse
// receiver record carries it as its fatal error and the control refuses the
// next cohort.
func TestLocalReceiverReadFaultReachesItsRecord(testContext *testing.T) {
	local := startFaultedLocalReceiver(testContext)
	if record := local.control.result(); record.FatalError != localReadFault || record.FixtureVerdict != verdictInvalid {
		testContext.Fatalf("reverse receiver record fatal %q verdict %q", record.FatalError, record.FixtureVerdict)
	}
	if ready := local.control.ready(); ready.Ready || ready.Error != localReadFault {
		testContext.Fatalf("faulted local control still ready: %+v", ready)
	}
}

// A local read fault fails the bidirectional cohort in warm-up and in
// measurement. In warm-up it can never pass for an overloaded direction; in
// measurement it joins the cohort's other errors instead of being dropped.
func TestLocalReceiverReadFaultFailsTheBidirectionalCohort(testContext *testing.T) {
	validity := errors.New(cohortValidityError)
	for _, scenario := range []struct {
		name   string
		warmup time.Duration
	}{
		{"warm-up", time.Second},
		{"measurement", 0},
	} {
		testContext.Run(scenario.name, func(testContext *testing.T) {
			local := startFaultedLocalReceiver(testContext)
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writeJSON(writer, http.StatusOK, reversePeer(true, cohortValidityError))
			}))
			defer server.Close()
			runner := func(_ commandConfig, phase, _ string, _ time.Duration) (cohortResult, error) {
				sender, receiver := directionRecords(true)
				result := newCohortResult(phase, sender, receiver, validity)
				finishBidirectionalCohort(context.Background(), commandConfig{PeerControl: server.URL, Drain: time.Second}, &result, local)
				return result, validity
			}
			result, _ := runWarmupAndMeasurement(commandConfig{Warmup: scenario.warmup, Duration: time.Second}, runner, nil)
			want := "reverse cohort local receiver: " + localReadFault
			if result.Verdict != verdictInvalid || result.Error == warmupValidityError || !strings.Contains(result.Error, want) ||
				!strings.Contains(result.Error, "reverse cohort: "+cohortValidityError) {
				testContext.Fatalf("%s result verdict %q error %q, want the local fault joined to the cohort's errors", scenario.name, result.Verdict, result.Error)
			}
		})
	}
}

// A receiver control that answers once during the drain and then stops
// answering is a fault, not undelivered work: its last result is far older
// than the drain observation bound when the deadline passes.
func TestReceiverControlHangAfterPollIsAFault(testContext *testing.T) {
	testContext.Parallel()
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if requests.Add(1) > 1 {
			<-request.Context().Done()
			return
		}
		writeJSON(writer, http.StatusOK, unaccountedReceiver())
	}))
	defer server.Close()
	counters := newSenderCounters(8)
	counters.submitted = 100
	deadline := time.Now().Add(drainWaitMargin)
	drainContext, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()
	_, err := waitReceiverDrain(drainContext, server.URL, counters, deadline)
	var outcome *drainDeadlineError
	if err == nil || errors.As(err, &outcome) || !strings.Contains(err.Error(), "receiver control did not answer during the drain") {
		testContext.Fatalf("drain wait error = %v, want a receiver control fault", err)
	}
	if timeout, fatal := drainTimeoutOutcome(err, runSpec{Drain: time.Second}, deadline); timeout != nil || fatal == nil {
		testContext.Fatalf("hang reclassified: timeout %+v fatal %v", timeout, fatal)
	}
}

// Every way the drain deadline can end the wait after unaccounted work was
// seen settles the same way: the undelivered outcome while the last result is
// within the bound, a control fault beyond it. The poll interval and bound
// are chosen so each path is taken deterministically.
func TestDrainDeadlinePathsSettleOnTheLastObservation(testContext *testing.T) {
	testContext.Parallel()
	answering := func(hangAfterFirst bool) *httptest.Server {
		var requests atomic.Int64
		return httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			if hangAfterFirst && requests.Add(1) > 1 {
				<-request.Context().Done()
				return
			}
			writeJSON(writer, http.StatusOK, unaccountedReceiver())
		}))
	}
	for _, scenario := range []struct {
		name           string
		hangAfterFirst bool
		interval       time.Duration
		bound          time.Duration
		wantOutcome    bool
	}{
		// With an hour-long interval the wait is parked on its timer when
		// the drain context's deadline fires.
		{"context deadline while waiting, recent read", false, time.Hour, time.Hour, true},
		{"context deadline while waiting, stale read", false, time.Hour, time.Nanosecond, false},
		// With no interval the second request is in flight at the deadline.
		{"request cut by the deadline, recent read", true, 0, time.Hour, true},
		{"request cut by the deadline, stale read", true, 0, time.Nanosecond, false},
	} {
		testContext.Run(scenario.name, func(testContext *testing.T) {
			testContext.Parallel()
			server := answering(scenario.hangAfterFirst)
			defer server.Close()
			counters := newSenderCounters(8)
			counters.submitted = 100
			deadline := time.Now().Add(drainWaitMargin)
			drainContext, cancel := context.WithDeadline(context.Background(), deadline)
			defer cancel()
			_, err := pollReceiverDrain(drainContext, server.URL, counters, deadline, scenario.interval, scenario.bound)
			var outcome *drainDeadlineError
			if scenario.wantOutcome != errors.As(err, &outcome) {
				testContext.Fatalf("drain wait error = %v, want the undelivered outcome = %t", err, scenario.wantOutcome)
			}
			if !scenario.wantOutcome && (err == nil || !strings.Contains(err.Error(), "receiver control did not answer during the drain")) {
				testContext.Fatalf("drain wait error = %v, want a receiver control fault", err)
			}
		})
	}
}

// A last result read at or after the deadline is recorded as read at it,
// never as a negative interval.
func TestDrainTimeoutNeverRecordsANegativeObservation(testContext *testing.T) {
	deadline := time.Unix(100, 0)
	outcome := &drainDeadlineError{submitted: 10, accounted: 9, observed: deadline.Add(time.Millisecond)}
	if timeout := outcome.timeout(time.Second, deadline); timeout.ObservedBeforeDeadline != 0 {
		testContext.Fatalf("observed before deadline = %s, want 0", timeout.ObservedBeforeDeadline)
	}
}

// The SGP failure trial, like the overload trial, keeps its own contract: a
// drain deadline outcome or a late delivery there is a fixture failure.
func TestSGPFailureCohortsKeepTheirDrainContract(testContext *testing.T) {
	failover := runSpec{Drain: time.Second, SGPFailure: &sgpFailureSpec{}}
	overload := runSpec{Drain: time.Second, Overload: &overloadSpec{Role: overloadRoleMeasurement}}
	if !(runSpec{}).nominalDrainOutcomes() || failover.nominalDrainOutcomes() || overload.nominalDrainOutcomes() {
		testContext.Fatal("nominal drain outcomes must apply to nominal cohorts only")
	}
	outcome := &drainDeadlineError{submitted: 10, accounted: 9, observed: time.Unix(100, 0)}
	if timeout, fatal := drainTimeoutOutcome(outcome, failover, time.Unix(100, 0)); timeout != nil || fatal != outcome {
		testContext.Fatalf("SGP failure drain outcome reclassified: timeout %+v fatal %v", timeout, fatal)
	}
	control, _, _ := sharedClockFixture(testContext)
	control.mutex.Lock()
	control.spec.SGPFailure = &sgpFailureSpec{}
	control.lateDeliveryLocked()
	fatal := control.fatal
	control.mutex.Unlock()
	if fatal != "delivery exceeds shared drain deadline" {
		testContext.Fatalf("SGP failure late delivery fatal %q", fatal)
	}
}

// A sender record can carry both a backlog cut off by the deadline and a
// fault: a worker stuck past the grace, or a lost association after some sends
// were cut off. The fault stays the record's fatal error beside the backlog.
func TestSenderBacklogWithAFaultKeepsTheFault(testContext *testing.T) {
	counters := newSenderCounters(8)
	counters.drainOutcomes = true
	counters.outstanding = 2
	counters.complete(writeDeadlineError(), 0, 0)
	counters.complete(&m3ua.DataWriteError{Outcome: m3ua.DataNotSent, Err: m3ua.ErrNotEstablished}, 0, 0)
	timeout := counters.drainTimeout(time.Second, 2)
	record := counters.result(runSpec{Expected: 2}, time.Second, time.Second, 0)
	record.SenderDrainTimeout = timeout
	record.evaluate()
	if timeout == nil || timeout.Unsubmitted != 1 || record.SendErrors != 2 || !strings.Contains(record.FatalError, "not established") ||
		!slices.Contains(record.Reasons, "fatal network fixture error") ||
		!slices.Contains(record.Reasons, "scheduled traffic was still unsubmitted at the sender when the drain deadline passed") {
		testContext.Fatalf("record fatal %q send errors %d timeout %+v reasons %q", record.FatalError, record.SendErrors, timeout, record.Reasons)
	}
}

// reset stops a memory sampler a cohort left running, so no sampler outlives
// its cohort.
func TestResetStopsALiveMemorySampler(testContext *testing.T) {
	control := newReceiverControl(1, maxOutstanding)
	control.setAssociationReady(0, 15)
	released := make(chan struct{})
	control.memory = newMemorySampler(func() memoryReading { return memoryReading{} }, time.Now, make(chan time.Time), func() { close(released) })
	specification := runSpec{Cohort: "sampler", Seed: 1, Associations: 1, Expected: 10, Duration: time.Second, Rate: 10, Payload: workload128, Outstanding: maxOutstanding}
	if err := control.reset(specification); err != nil {
		testContext.Fatal(err)
	}
	select {
	case <-released:
	default:
		testContext.Fatal("reset left the previous sampler running")
	}
	if control.memory != nil || control.memoryResult != nil {
		testContext.Fatalf("reset kept sampler state: %v %v", control.memory, control.memoryResult)
	}
}

// collectReverse's collection deadline and a canceled run both fail the
// cohort as faults, never as the validity failure of an overloaded direction.
func TestCollectReverseFailuresAreFaults(testContext *testing.T) {
	testContext.Parallel()
	pending := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writeJSON(writer, http.StatusOK, runRecord{Side: "receiver", Verdict: verdictInconclusive})
	}))
	defer pending.Close()
	for _, scenario := range []struct {
		name   string
		cancel bool
		want   string
	}{
		{"collection deadline", false, "reverse cohort did not complete before the collection deadline"},
		{"canceled run", true, context.Canceled.Error()},
	} {
		testContext.Run(scenario.name, func(testContext *testing.T) {
			testContext.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if scenario.cancel {
				cancel()
			}
			validity := errors.New(cohortValidityError)
			sender, receiver := directionRecords(true)
			result := newCohortResult("warmup", sender, receiver, validity)
			collectReverse(ctx, commandConfig{PeerControl: pending.URL, Drain: time.Millisecond}, &result)
			if result.validityOnly || result.Verdict != verdictInvalid || !strings.Contains(result.Error, scenario.want) {
				testContext.Fatalf("result validity-only %t verdict %q error %q", result.validityOnly, result.Verdict, result.Error)
			}
			if failed := failedWarmupResult(result, validity); failed.Error == warmupValidityError || !strings.Contains(failed.Error, scenario.want) {
				testContext.Fatalf("failed warm-up error %q, want the fault named", failed.Error)
			}
		})
	}
}
