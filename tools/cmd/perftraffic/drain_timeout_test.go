package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync/atomic"
	"testing"
	"time"
)

// drainWaitMargin is how long the drain waits below may run before their
// deadline. Only the outcome is asserted, never a duration; the margin only
// has to exceed the time a loaded runner takes to complete one local HTTP
// request.
const drainWaitMargin = time.Second

// unaccountedReceiver serves a receiver result that accounts for 93 messages:
// 90 validated, 2 invalid and 1 duplicate.
func unaccountedReceiver() runRecord {
	return runRecord{Side: "receiver", Delivery: deliveryResult{Unique: 90, Invalid: 2, Duplicate: 1}}
}

// waitDrain runs the drain wait the way runSenderCohortWith does: under a
// context that ends at the drain deadline. Its observation bound is the whole
// margin, so a loaded runner's scheduling delay before the deadline cannot
// turn a responsive receiver into a stale one; the production bound is
// exercised where the staleness is far beyond it.
func waitDrain(ctx context.Context, url string, submitted uint64, deadline time.Time) (runRecord, error) {
	counters := newSenderCounters(8)
	counters.submitted = submitted
	drainContext, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	return pollReceiverDrain(drainContext, url, counters, deadline, drainPollInterval, drainWaitMargin)
}

// A probe above capacity leaves submitted work unaccounted when the drain
// deadline passes. That is the probe's delivery outcome: the record carries
// drain_timeout with the undelivered count, the drain and the cause, fails the
// cohort, and has no fatal_error, which is reserved for fixture faults.
func TestDrainDeadlineWithUnaccountedWorkIsAFailedProbe(testContext *testing.T) {
	testContext.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writeJSON(writer, http.StatusOK, unaccountedReceiver())
	}))
	defer server.Close()
	deadline := time.Now().Add(drainWaitMargin)
	last, err := waitDrain(context.Background(), server.URL, 100, deadline)
	var outcome *drainDeadlineError
	if !errors.As(err, &outcome) || outcome.submitted != 100 || outcome.accounted != 93 || last.Delivery.Unique != 90 {
		testContext.Fatalf("drain wait = %+v, %v; want the unaccounted outcome with the last receiver result", last.Delivery, err)
	}
	specification := runSpec{Drain: 2 * time.Second}
	timeout, fatal := drainTimeoutOutcome(err, specification, deadline)
	if fatal != nil || timeout == nil {
		testContext.Fatalf("drain outcome = %+v, fatal %v; want a drain timeout and no fatal error", timeout, fatal)
	}
	if timeout.Cause != drainTimeoutCause || timeout.Drain != 2*time.Second || timeout.Submitted != 100 || timeout.Accounted != 93 ||
		timeout.Undelivered != 7 || timeout.ObservedBeforeDeadline != max(deadline.Sub(outcome.observed), 0) {
		testContext.Fatalf("drain timeout = %+v", timeout)
	}

	sender := runRecord{
		Side: "sender", Expected: 100, Scheduled: 100, Sent: 100, Submitted: 100,
		Delivery:     deliveryResult{Unique: 90, UniqueMeasurement: 90, Missing: 10, Duplicate: 1, Invalid: 2},
		DrainTimeout: timeout,
	}
	sender.evaluate()
	if sender.FatalError != "" || sender.FixtureVerdict != verdictInvalid || sender.Verdict != verdictInvalid ||
		!slices.Contains(sender.Reasons, "submitted traffic was still unaccounted at the receiver when the drain deadline passed") ||
		slices.Contains(sender.Reasons, "fatal network fixture error") {
		testContext.Fatalf("sender record = fatal %q verdict %s/%s reasons %q", sender.FatalError, sender.FixtureVerdict, sender.Verdict, sender.Reasons)
	}
	encoded, err := json.Marshal(sender)
	if err != nil {
		testContext.Fatal(err)
	}
	var decoded struct {
		FatalError   *string        `json:"fatal_error"`
		DrainTimeout map[string]any `json:"drain_timeout"`
	}
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		testContext.Fatal(err)
	}
	if decoded.FatalError != nil || decoded.DrainTimeout["cause"] != drainTimeoutCause || decoded.DrainTimeout["undelivered"] != float64(7) ||
		decoded.DrainTimeout["drain_ns"] != float64(2*time.Second) || decoded.DrainTimeout["submitted"] != float64(100) ||
		decoded.DrainTimeout["accounted"] != float64(93) {
		testContext.Fatalf("serialized record = %s", encoded)
	}
}

// Everything else that ends the drain wait is a fixture fault and stays
// fatal: an unreachable or failing receiver, a failure after unaccounted
// work was seen, a canceled run, a deadline that passed before any receiver
// result was read, and any drain failure of the overload trial, whose own
// contract treats it as a fixture failure.
func TestDrainWaitFixtureFaultsStayFatal(testContext *testing.T) {
	testContext.Parallel()
	respond := func(failAfter int64, onFailure func(*http.Request)) *httptest.Server {
		var requests atomic.Int64
		return httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			if requests.Add(1) > failAfter {
				if onFailure != nil {
					onFailure(request)
					return
				}
				http.Error(writer, "receiver failed", http.StatusServiceUnavailable)
				return
			}
			writeJSON(writer, http.StatusOK, unaccountedReceiver())
		}))
	}
	for _, scenario := range []struct {
		name   string
		server func(cancel context.CancelFunc) *httptest.Server
		margin time.Duration
	}{
		{name: "receiver unreachable", server: func(context.CancelFunc) *httptest.Server {
			server := respond(0, nil)
			server.Close()
			return server
		}, margin: drainWaitMargin},
		{name: "receiver error status", server: func(context.CancelFunc) *httptest.Server { return respond(0, nil) }, margin: drainWaitMargin},
		{name: "failure after unaccounted work", server: func(context.CancelFunc) *httptest.Server { return respond(1, nil) }, margin: drainWaitMargin},
		{name: "run canceled after unaccounted work", server: func(cancel context.CancelFunc) *httptest.Server {
			return respond(1, func(request *http.Request) {
				cancel()
				<-request.Context().Done()
			})
		}, margin: drainWaitMargin},
		{name: "no receiver result before the deadline", server: func(context.CancelFunc) *httptest.Server {
			return respond(0, func(request *http.Request) { <-request.Context().Done() })
		}, margin: 100 * time.Millisecond},
	} {
		testContext.Run(scenario.name, func(testContext *testing.T) {
			testContext.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			server := scenario.server(cancel)
			defer server.Close()
			deadline := time.Now().Add(scenario.margin)
			_, err := waitDrain(ctx, server.URL, 100, deadline)
			var outcome *drainDeadlineError
			if err == nil || errors.As(err, &outcome) {
				testContext.Fatalf("drain wait error = %v, want a fixture fault", err)
			}
			if timeout, fatal := drainTimeoutOutcome(err, runSpec{Drain: time.Second}, deadline); timeout != nil || !errors.Is(fatal, err) {
				testContext.Fatalf("fault reclassified: timeout %+v fatal %v", timeout, fatal)
			}
		})
	}
	testContext.Run("overload trial", func(testContext *testing.T) {
		deadline := time.Now()
		outcome := &drainDeadlineError{submitted: 100, accounted: 93, observed: deadline}
		overload := runSpec{Drain: 3 * time.Second, Overload: &overloadSpec{Role: overloadRoleMeasurement}}
		if timeout, fatal := drainTimeoutOutcome(outcome, overload, deadline); timeout != nil || fatal != outcome {
			testContext.Fatalf("overload drain failure reclassified: timeout %+v fatal %v", timeout, fatal)
		}
	})
}

// A record whose only failure is the drain deadline is invalid, not passing,
// even when the receiver's final counters caught up after the sender stopped
// waiting: the sender could not observe the delivery in time.
func TestDrainTimeoutAloneFailsTheCohort(testContext *testing.T) {
	record := runRecord{
		Side: "sender", Expected: 10, Scheduled: 10, Sent: 10, Submitted: 10,
		Delivery:     deliveryResult{Unique: 10, UniqueMeasurement: 10},
		DrainTimeout: &drainTimeoutRecord{Cause: drainTimeoutCause, Drain: time.Second, Submitted: 10, Accounted: 9, Undelivered: 1},
		CPU: CPUObservation{
			Scope:  wholeProcessScope,
			Before: map[string]uint64{"usage_usec": 10, "nr_throttled": 0},
			After:  map[string]uint64{"usage_usec": 20, "nr_throttled": 0},
		},
	}
	record.evaluate()
	if record.FixtureVerdict != verdictInvalid || record.Verdict != verdictInvalid || record.FatalError != "" {
		testContext.Fatalf("record = verdict %s/%s fatal %q reasons %q", record.FixtureVerdict, record.Verdict, record.FatalError, record.Reasons)
	}
}

// A delivery committed after the shared drain deadline is work that was still
// outstanding there. In a nominal cohort it is counted, not fatal; the
// overload trial keeps treating it as a fixture failure.
func TestLateDeliveryIsFatalOnlyForTheOverloadTrial(testContext *testing.T) {
	control, _, _ := sharedClockFixture(testContext)
	control.mutex.Lock()
	control.lateDeliveryLocked()
	nominalFatal, nominalLate := control.fatal, control.lateAfterDeadline
	control.spec.Overload = &overloadSpec{Role: overloadRoleMeasurement}
	control.lateDeliveryLocked()
	overloadFatal, overloadLate := control.fatal, control.lateAfterDeadline
	control.mutex.Unlock()
	if nominalFatal != "" || nominalLate != 1 {
		testContext.Fatalf("nominal late delivery: fatal %q late %d", nominalFatal, nominalLate)
	}
	if overloadFatal != "delivery exceeds shared drain deadline" || overloadLate != 1 {
		testContext.Fatalf("overload late delivery: fatal %q late %d", overloadFatal, overloadLate)
	}
}
