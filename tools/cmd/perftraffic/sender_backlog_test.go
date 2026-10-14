package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua"
)

// writeDeadlineError is what a DATA write returns when the association write
// deadline, the cohort's drain deadline, expires before the message could be
// submitted.
func writeDeadlineError() error {
	return &m3ua.DataWriteError{Outcome: m3ua.DataNotSent, Err: fmt.Errorf("write sctp: %w", os.ErrDeadlineExceeded)}
}

// A send the drain deadline cut off, by the expired write deadline or by
// completing after the shared drain deadline, is the sender's share of an
// overloaded probe: it is counted in send_errors and unsubmitted and never
// becomes a fatal error. Every other send failure, alone or joined with a
// deadline, is a fault and stays fatal.
func TestSendsTheDrainDeadlineCutOffAreCountedNotFatal(testContext *testing.T) {
	transfer := func(successful int, causes ...error) error {
		failure := &m3ua.MTPTransferError{SuccessfulPaths: make([]m3ua.MTPTransferPath, successful)}
		for _, cause := range causes {
			failure.Failures = append(failure.Failures, m3ua.MTPTransferFailure{Err: cause})
		}
		return transferOutcome(failure)
	}
	for _, scenario := range []struct {
		name   string
		err    error
		cutOff bool
	}{
		{"write deadline", writeDeadlineError(), true},
		{"completed after the drain deadline", errCompletedAfterDrain, true},
		{"both", errors.Join(writeDeadlineError(), errCompletedAfterDrain), true},
		{"routed transfer cut off on its path", transfer(0, writeDeadlineError()), true},
		{"association lost", &m3ua.DataWriteError{Outcome: m3ua.DataNotSent, Err: m3ua.ErrNotEstablished}, false},
		{"deadline joined with a loss", errors.Join(writeDeadlineError(), m3ua.ErrNotEstablished), false},
		{"late completion joined with a clock failure", errors.Join(errCompletedAfterDrain, errors.New("shared clock completion is outside the drain deadline")), false},
		{"short write", errors.New("WriteData wrote 3 bytes, want 128"), false},
		{"run context deadline", fmt.Errorf("routing timed send: %w", context.DeadlineExceeded), false},
		{"routed transfer that also succeeded", transfer(1, writeDeadlineError()), false},
		{"routed transfer lost its association", transfer(0, m3ua.ErrNotEstablished), false},
		{"routed transfer cut off on one path and lost on another", transfer(0, writeDeadlineError(), m3ua.ErrNotEstablished), false},
	} {
		testContext.Run(scenario.name, func(testContext *testing.T) {
			counters := newSenderCounters(8)
			counters.drainOutcomes = true
			counters.outstanding = 1
			counters.complete(scenario.err, 0, time.Microsecond)
			if counters.sendErrors != 1 || counters.submitted != 0 || counters.outstanding != 0 {
				testContext.Fatalf("counters: send errors %d submitted %d outstanding %d", counters.sendErrors, counters.submitted, counters.outstanding)
			}
			if scenario.cutOff != (counters.unsubmitted == 1) || scenario.cutOff != (counters.fatal == "") {
				testContext.Fatalf("unsubmitted %d fatal %q, want cut off = %t", counters.unsubmitted, counters.fatal, scenario.cutOff)
			}
			timeout := counters.drainTimeout(2*time.Second, 0)
			if scenario.cutOff != (timeout != nil) || timeout != nil && (timeout.Cause != senderDrainTimeoutCause ||
				timeout.Drain != 2*time.Second || timeout.Unsubmitted != 1 || timeout.OutstandingAtDeadline != 0) {
				testContext.Fatalf("drain timeout %+v, want one only when the send was cut off", timeout)
			}
		})
	}
	testContext.Run("overload trial keeps its contract", func(testContext *testing.T) {
		counters := newSenderCounters(8)
		counters.complete(writeDeadlineError(), 0, 0)
		if counters.fatal == "" || counters.unsubmitted != 0 || counters.drainTimeout(time.Second, 3) != nil {
			testContext.Fatalf("fatal %q unsubmitted %d: a cohort without drain outcomes reclassified a deadline", counters.fatal, counters.unsubmitted)
		}
	})
}

// Work the sender still held when the deadline passed is recorded even when
// the workers failed it without a single send reaching the transport.
func TestSenderDrainTimeoutRecordsOutstandingWork(testContext *testing.T) {
	counters := newSenderCounters(8)
	counters.drainOutcomes = true
	if counters.drainTimeout(time.Second, 0) != nil {
		testContext.Fatal("a sender that submitted everything in time reported a drain timeout")
	}
	timeout := counters.drainTimeout(time.Second, 5)
	if timeout == nil || timeout.OutstandingAtDeadline != 5 || timeout.Unsubmitted != 0 {
		testContext.Fatalf("drain timeout %+v, want 5 outstanding", timeout)
	}
}

// A completion after the shared drain deadline is reported as such, not as a
// clock failure, which stays a fault.
func TestSharedCompletionAfterTheDrainDeadlineIsDistinguished(testContext *testing.T) {
	_, source, specification := sharedClockFixture(testContext)
	clock := &sharedRunClock{source: source, window: *specification.Clock, drain: specification.Drain}
	source.now.Store(clock.window.End + int64(clock.drain))
	if err := clock.withinDrain(specification.Duration); !errors.Is(err, errCompletedAfterDrain) {
		testContext.Fatalf("late completion = %v", err)
	}
	source.now.Store(clock.window.Start)
	if err := clock.withinDrain(time.Second); err == nil || errors.Is(err, errCompletedAfterDrain) {
		testContext.Fatalf("completion before its dispatch = %v, want a clock failure", err)
	}
	source.err = errors.New("clock unavailable")
	source.now.Store(clock.window.End + int64(clock.drain))
	if err := clock.withinDrain(specification.Duration); err == nil || errors.Is(err, errCompletedAfterDrain) {
		testContext.Fatalf("clock read failure = %v, want a clock failure", err)
	}
}

// A record whose sender could not submit its work in time is invalid, with
// its own reason and no fatal error.
func TestSenderDrainTimeoutFailsTheCohort(testContext *testing.T) {
	record := runRecord{
		Side: "sender", Expected: 10, Scheduled: 10, Sent: 8, Submitted: 8, SendErrors: 2,
		Delivery:           deliveryResult{Unique: 8, UniqueMeasurement: 8, Missing: 2},
		SenderDrainTimeout: &senderDrainTimeoutRecord{Cause: senderDrainTimeoutCause, Drain: time.Second, OutstandingAtDeadline: 2, Unsubmitted: 2},
	}
	record.evaluate()
	if record.FixtureVerdict != verdictInvalid || record.FatalError != "" ||
		!slices.Contains(record.Reasons, "scheduled traffic was still unsubmitted at the sender when the drain deadline passed") {
		testContext.Fatalf("record verdict %s fatal %q reasons %q", record.FixtureVerdict, record.FatalError, record.Reasons)
	}
}
