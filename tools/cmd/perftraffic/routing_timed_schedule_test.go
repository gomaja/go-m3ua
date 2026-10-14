package main

import (
	"context"
	"strings"
	"testing"
	"time"
)

type routingTimedScheduleClock struct {
	readings []time.Duration
	index    int
	cancelAt int
	cancel   context.CancelFunc
}

func (clock *routingTimedScheduleClock) Now() (int64, error) {
	reading := clock.readings[len(clock.readings)-1]
	if clock.index < len(clock.readings) {
		reading = clock.readings[clock.index]
		clock.index++
	}
	if clock.cancelAt > 0 && clock.index == clock.cancelAt {
		clock.cancel()
	}
	return int64(time.Second + reading), nil
}

func (*routingTimedScheduleClock) Domain() (sharedClockDomain, error) {
	return sharedClockDomain{
		Clock:         "CLOCK_MONOTONIC",
		BootID:        "routing-timed-schedule",
		TimeNamespace: "time:[1]",
		Resolution:    1,
	}, nil
}

type routingTimedCounterSnapshot struct {
	scheduled   uint64
	submitted   uint64
	sendErrors  uint64
	capped      uint64
	outstanding uint64
	fatal       string
}

func routingTimedCountersSnapshot(counters *senderCounters) routingTimedCounterSnapshot {
	counters.mutex.Lock()
	defer counters.mutex.Unlock()
	return routingTimedCounterSnapshot{
		scheduled:   counters.scheduled,
		submitted:   counters.submitted,
		sendErrors:  counters.sendErrors,
		capped:      counters.capped,
		outstanding: counters.outstanding,
		fatal:       counters.fatal,
	}
}

func TestRoutingTimedCharacterizesLocalDispatchSchedule(testContext *testing.T) {
	const expected = uint64(4)
	duration := 4 * time.Millisecond
	started := time.Now().Add(-duration)
	queue := make(chan sendJob, expected)
	counters := newSenderCounters(int(expected))
	config := commandConfig{Rate: 1_000, Workload: workloadMix, Seed: 17}

	dispatchScheduled(context.Background(), config, "routing-schedule", duration, started, expected, []chan sendJob{queue}, counters, nil, nil)

	snapshot := routingTimedCountersSnapshot(counters)
	if snapshot != (routingTimedCounterSnapshot{scheduled: expected, outstanding: expected}) {
		testContext.Fatalf("dispatch counters = %+v", snapshot)
	}
	if len(queue) != int(expected) {
		testContext.Fatalf("queued jobs = %d, want %d", len(queue), expected)
	}
	for index := uint64(0); index < expected; index++ {
		job := <-queue
		wantIdentity := planMessage("routing-schedule", config.Seed, index, 1)
		wantOffset := time.Duration(index * uint64(time.Second) / config.Rate)
		if job.identity != wantIdentity || job.offset != wantOffset || !job.scheduled.Equal(started.Add(wantOffset)) ||
			job.size != config.Workload.size(index) || job.clock != nil || job.reverse {
			testContext.Fatalf("job %d = %+v, want identity=%+v offset=%s scheduled=%s size=%d", index, job, wantIdentity, wantOffset, started.Add(wantOffset), config.Workload.size(index))
		}
	}
}

func TestRoutingTimedCharacterizesCanceledDispatchAccounting(testContext *testing.T) {
	const expected = uint64(4)
	testContextCanceled, cancel := context.WithCancel(context.Background())
	cancel()
	queue := make(chan sendJob, expected)
	counters := newSenderCounters(int(expected))

	dispatchScheduled(testContextCanceled, commandConfig{Rate: 1_000, Workload: workload128}, "routing-canceled", 4*time.Millisecond, time.Now(), expected, []chan sendJob{queue}, counters, nil, nil)

	snapshot := routingTimedCountersSnapshot(counters)
	if len(queue) != 0 || snapshot.scheduled != expected || snapshot.capped != expected || snapshot.outstanding != 0 ||
		snapshot.submitted != 0 || snapshot.sendErrors != 0 || !strings.Contains(snapshot.fatal, context.Canceled.Error()) {
		testContext.Fatalf("canceled dispatch queue=%d counters=%+v", len(queue), snapshot)
	}
}

func TestRoutingTimedCharacterizesOutstandingCap(testContext *testing.T) {
	const expected = uint64(4)
	queue := make(chan sendJob, expected)
	counters := newSenderCounters(1)

	dispatchScheduled(context.Background(), commandConfig{Rate: 1_000, Workload: workload128}, "routing-cap", 4*time.Millisecond, time.Now().Add(-4*time.Millisecond), expected, []chan sendJob{queue}, counters, nil, nil)

	snapshot := routingTimedCountersSnapshot(counters)
	if len(queue) != 1 || snapshot != (routingTimedCounterSnapshot{scheduled: expected, capped: expected - 1, outstanding: 1}) {
		testContext.Fatalf("capped dispatch queue=%d counters=%+v", len(queue), snapshot)
	}
}

func TestRoutingTimedCharacterizesFullWorkerQueue(testContext *testing.T) {
	const expected = uint64(4)
	queue := make(chan sendJob)
	counters := newSenderCounters(int(expected))

	dispatchScheduled(context.Background(), commandConfig{Rate: 1_000, Workload: workload128}, "routing-queue-full", 4*time.Millisecond, time.Now().Add(-4*time.Millisecond), expected, []chan sendJob{queue}, counters, nil, nil)

	snapshot := routingTimedCountersSnapshot(counters)
	if len(queue) != 0 || snapshot != (routingTimedCounterSnapshot{scheduled: expected, capped: expected}) {
		testContext.Fatalf("full-queue dispatch queue=%d counters=%+v", len(queue), snapshot)
	}
}

func TestRoutingTimedCharacterizesSharedClockBoundaries(testContext *testing.T) {
	const expected = uint64(4)
	duration := 4 * time.Millisecond
	source := &routingTimedScheduleClock{readings: []time.Duration{-time.Millisecond, 1, time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond, duration}}
	domain, err := source.Domain()
	if err != nil {
		testContext.Fatalf("clock domain: %v", err)
	}
	clock := &sharedRunClock{source: source, window: sharedClockWindow{Domain: domain, Start: int64(time.Second), End: int64(time.Second + duration)}}
	queue := make(chan sendJob, expected)
	counters := newSenderCounters(int(expected))

	dispatchScheduled(context.Background(), commandConfig{Rate: 1_000, Workload: workload128, Seed: 19}, "routing-shared", duration, time.Now().Add(-time.Hour), expected, []chan sendJob{queue}, counters, nil, clock)

	snapshot := routingTimedCountersSnapshot(counters)
	if snapshot != (routingTimedCounterSnapshot{scheduled: expected, outstanding: expected}) || len(queue) != int(expected) {
		testContext.Fatalf("shared dispatch queue=%d counters=%+v", len(queue), snapshot)
	}
	if source.index != len(source.readings) {
		testContext.Fatalf("shared clock reads = %d, want %d including the end boundary", source.index, len(source.readings))
	}
	for index := uint64(0); index < expected; index++ {
		job := <-queue
		wantOffset := time.Duration(index * uint64(time.Second) / 1_000)
		if job.clock != clock || job.offset != wantOffset {
			testContext.Fatalf("shared job %d clock=%p offset=%s, want clock=%p offset=%s", index, job.clock, job.offset, clock, wantOffset)
		}
	}
}

func TestRoutingTimedCharacterizesSharedClockMissedStart(testContext *testing.T) {
	const expected = uint64(4)
	duration := 4 * time.Millisecond
	source := &routingTimedScheduleClock{readings: []time.Duration{0}}
	domain, err := source.Domain()
	if err != nil {
		testContext.Fatalf("clock domain: %v", err)
	}
	clock := &sharedRunClock{source: source, window: sharedClockWindow{Domain: domain, Start: int64(time.Second), End: int64(time.Second + duration)}}
	queue := make(chan sendJob, expected)
	counters := newSenderCounters(int(expected))

	dispatchScheduled(context.Background(), commandConfig{Rate: 1_000, Workload: workload128}, "routing-missed-start", duration, time.Now(), expected, []chan sendJob{queue}, counters, nil, clock)

	snapshot := routingTimedCountersSnapshot(counters)
	if len(queue) != 0 || snapshot.scheduled != expected || snapshot.capped != expected || snapshot.outstanding != 0 || !strings.Contains(snapshot.fatal, "start was missed") {
		testContext.Fatalf("missed-start dispatch queue=%d counters=%+v", len(queue), snapshot)
	}
}

func TestRoutingTimedCharacterizesSharedClockCancellationBeforeFirstDue(testContext *testing.T) {
	const expected = uint64(4)
	duration := 4 * time.Millisecond
	dispatchContext, cancel := context.WithCancel(context.Background())
	defer cancel()
	source := &routingTimedScheduleClock{
		readings: []time.Duration{-2 * time.Millisecond, -time.Millisecond},
		cancelAt: 2,
		cancel:   cancel,
	}
	domain, err := source.Domain()
	if err != nil {
		testContext.Fatalf("clock domain: %v", err)
	}
	clock := &sharedRunClock{source: source, window: sharedClockWindow{Domain: domain, Start: int64(time.Second), End: int64(time.Second + duration)}}
	queue := make(chan sendJob, expected)
	counters := newSenderCounters(int(expected))

	dispatchScheduled(dispatchContext, commandConfig{Rate: 1_000, Workload: workload128}, "routing-canceled-wait", duration, time.Now(), expected, []chan sendJob{queue}, counters, nil, clock)

	snapshot := routingTimedCountersSnapshot(counters)
	if len(queue) != 0 || snapshot.scheduled != expected || snapshot.capped != expected || snapshot.outstanding != 0 ||
		snapshot.submitted != 0 || snapshot.sendErrors != 0 || !strings.Contains(snapshot.fatal, context.Canceled.Error()) {
		testContext.Fatalf("canceled scheduler wait queue=%d counters=%+v", len(queue), snapshot)
	}
}

func TestRoutingTimedCharacterizesSharedClockCancellationAtEndBoundary(testContext *testing.T) {
	const expected = uint64(4)
	duration := 4 * time.Millisecond
	dispatchContext, cancel := context.WithCancel(context.Background())
	defer cancel()
	source := &routingTimedScheduleClock{
		readings: []time.Duration{-time.Millisecond, 1, time.Millisecond, 2 * time.Millisecond, 3 * time.Millisecond, duration - 1},
		cancelAt: 6,
		cancel:   cancel,
	}
	domain, err := source.Domain()
	if err != nil {
		testContext.Fatalf("clock domain: %v", err)
	}
	clock := &sharedRunClock{source: source, window: sharedClockWindow{Domain: domain, Start: int64(time.Second), End: int64(time.Second + duration)}}
	queue := make(chan sendJob, expected)
	counters := newSenderCounters(int(expected))

	dispatchScheduled(dispatchContext, commandConfig{Rate: 1_000, Workload: workload128}, "routing-canceled-end", duration, time.Now(), expected, []chan sendJob{queue}, counters, nil, clock)

	snapshot := routingTimedCountersSnapshot(counters)
	if len(queue) != int(expected) || snapshot.scheduled != expected || snapshot.outstanding != expected || snapshot.capped != 0 || !strings.Contains(snapshot.fatal, context.Canceled.Error()) {
		testContext.Fatalf("canceled end-boundary queue=%d counters=%+v", len(queue), snapshot)
	}
}

func TestRoutingTimedCharacterizesSharedClockRegression(testContext *testing.T) {
	const expected = uint64(4)
	duration := 4 * time.Millisecond
	source := &routingTimedScheduleClock{readings: []time.Duration{-time.Millisecond, 2 * time.Millisecond, time.Millisecond}}
	domain, err := source.Domain()
	if err != nil {
		testContext.Fatalf("clock domain: %v", err)
	}
	clock := &sharedRunClock{source: source, window: sharedClockWindow{Domain: domain, Start: int64(time.Second), End: int64(time.Second + duration)}}
	queue := make(chan sendJob, expected)
	counters := newSenderCounters(int(expected))

	dispatchScheduled(context.Background(), commandConfig{Rate: 1_000, Workload: workload128}, "routing-regression", duration, time.Now(), expected, []chan sendJob{queue}, counters, nil, clock)

	snapshot := routingTimedCountersSnapshot(counters)
	if len(queue) != 3 || snapshot.scheduled != expected || snapshot.capped != 1 || snapshot.outstanding != 3 || !strings.Contains(snapshot.fatal, "regressed") {
		testContext.Fatalf("regressed dispatch queue=%d counters=%+v", len(queue), snapshot)
	}
}
