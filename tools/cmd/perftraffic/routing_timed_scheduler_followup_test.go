package main

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestRoutingTimedSchedulerTrackerCapSkipsReservationAndQueue(testContext *testing.T) {
	const expected = uint64(4)
	duration := 4 * time.Millisecond
	queue := make(chan sendJob, expected)
	counters := newSenderCounters(int(expected))
	tracker := newEchoTracker("tracker-cap", 7, 1, workload128, 0, echoRequestDeadline)

	dispatchScheduled(context.Background(), commandConfig{Rate: 1_000, Workload: workload128, Mode: modeEcho}, "tracker-cap", duration, time.Now().Add(-duration), expected, []chan sendJob{queue}, counters, tracker, nil)

	snapshot := routingTimedCountersSnapshot(counters)
	result := tracker.result(expected)
	if len(queue) != 0 || snapshot.scheduled != expected || snapshot.capped != expected || snapshot.outstanding != 0 ||
		result.Capped != expected || result.OutstandingAfterDrain != 0 {
		testContext.Fatalf("tracker-cap queue=%d counters=%+v tracker=%+v", len(queue), snapshot, result)
	}
}

func TestRoutingTimedSchedulerFullQueueRemovesTrackerAdmission(testContext *testing.T) {
	const expected = uint64(4)
	duration := 4 * time.Millisecond
	queue := make(chan sendJob)
	counters := newSenderCounters(int(expected))
	tracker := newEchoTracker("tracker-queue", 7, 1, workload128, int(expected), echoRequestDeadline)

	dispatchScheduled(context.Background(), commandConfig{Rate: 1_000, Workload: workload128, Mode: modeEcho}, "tracker-queue", duration, time.Now().Add(-duration), expected, []chan sendJob{queue}, counters, tracker, nil)

	snapshot := routingTimedCountersSnapshot(counters)
	result := tracker.result(expected)
	if len(queue) != 0 || snapshot.scheduled != expected || snapshot.capped != expected || snapshot.outstanding != 0 ||
		result.Capped != 0 || result.OutstandingAfterDrain != 0 {
		testContext.Fatalf("tracker queue-full queue=%d counters=%+v tracker=%+v", len(queue), snapshot, result)
	}
}

func TestRoutingTimedSchedulerDueBatchCancellationStopsAtNext256Checkpoint(testContext *testing.T) {
	const expected = uint64(700)
	duration := 700 * time.Millisecond
	dispatchContext, cancel := context.WithCancel(context.Background())
	defer cancel()
	counters := newSenderCounters(int(expected))
	emitted := make([]uint64, 0, 512)

	dispatchOpenLoop(dispatchContext, 1_000, duration, time.Now().Add(-duration), expected, nil, counters, func(index uint64, _ time.Duration, _ time.Time) {
		emitted = append(emitted, index)
		if index == 300 {
			cancel()
		}
	})

	snapshot := routingTimedCountersSnapshot(counters)
	var first uint64
	var last uint64
	if len(emitted) > 0 {
		first = emitted[0]
		last = emitted[len(emitted)-1]
	}
	if len(emitted) != 512 || emitted[0] != 0 || emitted[len(emitted)-1] != 511 ||
		snapshot.scheduled != expected || snapshot.capped != expected-512 || snapshot.outstanding != 0 ||
		!strings.Contains(snapshot.fatal, context.Canceled.Error()) {
		testContext.Fatalf("due-batch emitted=%d first=%d last=%d counters=%+v", len(emitted), first, last, snapshot)
	}
}

func TestRoutingTimedSchedulerLocalFinalWaitCancellationPreservesOfferedAccounting(testContext *testing.T) {
	const expected = uint64(1)
	duration := time.Hour
	dispatchContext, cancel := context.WithCancel(context.Background())
	defer cancel()
	counters := newSenderCounters(int(expected))
	var emitted uint64

	dispatchOpenLoop(dispatchContext, 1, duration, time.Now().Add(-time.Second), expected, nil, counters, func(_ uint64, _ time.Duration, _ time.Time) {
		emitted++
		cancel()
	})

	snapshot := routingTimedCountersSnapshot(counters)
	if emitted != expected || snapshot != (routingTimedCounterSnapshot{scheduled: expected}) {
		testContext.Fatalf("local final-wait emitted=%d counters=%+v", emitted, snapshot)
	}
}
