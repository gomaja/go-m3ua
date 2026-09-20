package main

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type advancingMeasurementClock struct {
	origin time.Time
}

func (clock *advancingMeasurementClock) Now() (int64, error) {
	return int64(time.Second + time.Since(clock.origin)), nil
}

func (clock *advancingMeasurementClock) Domain() (sharedClockDomain, error) {
	return sharedClockDomain{Clock: "CLOCK_MONOTONIC", BootID: "test", TimeNamespace: "time:[1]", Resolution: 1}, nil
}

func TestSharedClockSchedulerIgnoresGoOrigin(testContext *testing.T) {
	source := &advancingMeasurementClock{origin: time.Now()}
	domain, _ := source.Domain()
	clock := &sharedRunClock{source: source, window: sharedClockWindow{Domain: domain, Start: int64(1040 * time.Millisecond), End: int64(1080 * time.Millisecond)}}
	queues := []chan sendJob{make(chan sendJob, 1)}
	counters := newSenderCounters(1)
	dispatchScheduled(context.Background(), commandConfig{Rate: 25, Workload: workload128}, "timing", 40*time.Millisecond, source.origin.Add(-time.Hour), 1, queues, counters, nil, clock)
	now, _ := source.Now()
	if now < clock.window.End {
		testContext.Fatalf("scheduler returned %v before shared end", time.Duration(clock.window.End-now))
	}
	job := <-queues[0]
	if job.clock != clock || job.offset != 0 {
		testContext.Fatalf("job lost shared schedule: %+v", job)
	}
}

func TestSharedClockDispatchDelayUsesIntegerSchedule(testContext *testing.T) {
	_, source, specification := sharedClockFixture(testContext)
	source.now.Store(specification.Clock.Start + int64(5*time.Second))
	job := sendJob{scheduled: time.Now().Add(-time.Hour), clock: &sharedRunClock{source: source, window: *specification.Clock}, offset: time.Second}
	if delay, err := job.dispatchDelay(); err != nil || delay != 4*time.Second {
		testContext.Fatalf("shared dispatch delay = %v, %v", delay, err)
	}
	source.now.Store(specification.Clock.Start)
	if _, err := job.dispatchDelay(); err == nil {
		testContext.Fatal("dispatch before scheduled instant accepted")
	}
}

func TestSenderFutureStartSeriesCannotWrap(testContext *testing.T) {
	counters := newSenderCounters(1)
	done := make(chan struct{})
	go sampleSender(time.Now().Add(2*time.Second), counters, done)
	defer close(done)
	time.Sleep(1100 * time.Millisecond)
	counters.mutex.Lock()
	defer counters.mutex.Unlock()
	if len(counters.series) != 0 {
		testContext.Fatalf("pre-start sample retained: %+v", counters.series)
	}
}

func TestSharedClockReceiverSamplesAndDrainIgnoreGoOrigin(testContext *testing.T) {
	control, clock, specification := sharedClockFixture(testContext)
	control.sample(control.started.Add(time.Hour))
	if len(control.series) != 0 {
		testContext.Fatal("receiver retained a pre-start sample")
	}
	clock.now.Store(specification.Clock.Start + int64(3*time.Second))
	control.sample(control.started.Add(time.Hour))
	if len(control.series) != 1 || control.series[0].OffsetMillis != 3000 {
		testContext.Fatalf("receiver sample uses Go origin: %+v", control.series)
	}
	clock.now.Store(specification.Clock.End + int64(500*time.Millisecond))
	if err := control.stop(); err != nil {
		testContext.Fatal(err)
	}
	if drain := control.result().DrainDuration; drain != 500*time.Millisecond {
		testContext.Fatalf("drain = %v, want 500ms", drain)
	}
}

func TestSharedClockWatchdogTranslationIsBounded(testContext *testing.T) {
	for _, width := range []time.Duration{0, time.Microsecond, sharedClockWatchdogBudget - 2, sharedClockWatchdogBudget - 1, 100 * time.Millisecond, -1} {
		testContext.Run(width.String(), func(testContext *testing.T) {
			_, source, specification := sharedClockFixture(testContext)
			clock := &sharedRunClock{source: source, window: *specification.Clock, drain: specification.Drain}
			before := source.now.Load()
			local := time.Now()
			deadline, evidence, err := clock.watchdog(func() time.Time {
				source.now.Add(int64(width))
				return local
			})
			if width < 0 || width+2 > sharedClockWatchdogBudget {
				if err == nil || evidence != nil || !deadline.IsZero() {
					testContext.Fatalf("unbounded translation accepted: %+v, %v", evidence, err)
				}
				return
			}
			if err != nil || evidence == nil {
				testContext.Fatalf("bounded translation rejected: %v", err)
			}
			want := local.Add(time.Duration(specification.Clock.End + int64(specification.Drain) - before + 1))
			if !deadline.Equal(want) || evidence.MaximumLateness != int64(width)+2 || evidence.MaximumLateness > evidence.Budget || evidence.Before != before || evidence.After != before+int64(width) {
				testContext.Fatalf("watchdog bound: %v %+v", deadline, evidence)
			}
		})
	}
}

func TestSharedClockWatchdogRejectsInvalidDeadline(testContext *testing.T) {
	for _, scenario := range []string{"overflow", "negative-drain", "expired", "clock-error", "resolution-budget"} {
		testContext.Run(scenario, func(testContext *testing.T) {
			_, source, specification := sharedClockFixture(testContext)
			clock := &sharedRunClock{source: source, window: *specification.Clock, drain: specification.Drain}
			switch scenario {
			case "overflow":
				clock.window.End = math.MaxInt64
			case "negative-drain":
				clock.drain = -1
			case "expired":
				source.now.Store(clock.window.End + int64(clock.drain))
			case "clock-error":
				source.err = errors.New("failed")
			case "resolution-budget":
				clock.window.Domain.Resolution = int64(sharedClockWatchdogBudget)
			}
			if _, _, err := clock.watchdog(time.Now); err == nil {
				testContext.Fatal("invalid watchdog accepted")
			}
		})
	}
}

func TestSharedClockDrainRejectsLateCompletion(testContext *testing.T) {
	control, source, specification := sharedClockFixture(testContext)
	clock := &sharedRunClock{source: source, window: *specification.Clock, drain: specification.Drain}
	for index, instant := range []int64{clock.window.End + int64(clock.drain) - 1, clock.window.End + int64(clock.drain)} {
		source.now.Store(instant)
		err := clock.withinDrain(specification.Duration)
		identity := planMessage(specification.Cohort, specification.Seed, uint64(index), 1)
		_, outcome := control.record(0, validReceivedMessage(identity.Cohort, identity.Seed, identity.Association, identity.Flow, identity.Sequence, 128))
		if index == 0 && (err != nil || outcome != recordUnique) || index == 1 && (err == nil || outcome != recordInvalid) {
			testContext.Fatalf("drain boundary %d: %v, %v", index, err, outcome)
		}
	}
	if result := control.result(); result.Delivery.Unique != 1 || result.Delivery.Invalid != 1 || result.FatalError == "" {
		testContext.Fatalf("late completion credited: %+v", result)
	}
	source.now.Store(clock.window.Start)
	if err := clock.withinDrain(time.Second); err == nil {
		testContext.Fatal("completion before dispatch accepted")
	}
}

type commitMeasurementClock struct {
	measurementClock
	inspect func()
}

func (clock commitMeasurementClock) Now() (int64, error) {
	clock.inspect()
	return clock.measurementClock.Now()
}

func TestSharedClockDeliveryTimestampFollowsLedgerValidation(testContext *testing.T) {
	control, source, specification := sharedClockFixture(testContext)
	control.clock = commitMeasurementClock{measurementClock: source, inspect: func() {
		if control.ledger.snapshotData.Unique != 1 {
			testContext.Error("delivery clock read preceded ledger validation")
		}
		source.now.Store(specification.Clock.End + int64(specification.Drain))
	}}
	identity := planMessage(specification.Cohort, specification.Seed, 0, 1)
	_, outcome := control.record(0, validReceivedMessage(identity.Cohort, identity.Seed, identity.Association, identity.Flow, identity.Sequence, 128))
	if outcome != recordInvalid || control.ledger.snapshotData.Unique != 0 || control.ledger.snapshotData.Invalid != 1 {
		testContext.Fatalf("late validation retained delivery credit: %+v", control.ledger.snapshotData)
	}
}

func TestSharedClockWaitRechecksWakeHint(testContext *testing.T) {
	_, source, specification := sharedClockFixture(testContext)
	clock := &sharedRunClock{source: source, window: *specification.Clock}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel()
	if err := clock.waitUntil(ctx, source.now.Load()+int64(time.Millisecond)); !errors.Is(err, context.DeadlineExceeded) {
		testContext.Fatalf("timer wake replaced shared boundary: %v", err)
	}
}

func TestSharedClockProgressSamplerIgnoresGoOrigin(testContext *testing.T) {
	_, source, specification := sharedClockFixture(testContext)
	source.now.Store(specification.Clock.Start + int64(time.Second))
	clock := &sharedRunClock{source: source, window: *specification.Clock}
	requested := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		requested <- struct{}{}
		_ = json.NewEncoder(writer).Encode(receiverProgress{Clock: &sharedClockSnapshot{Domain: source.domain, Captured: source.now.Load()}})
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := sampleSharedProgress(ctx, time.Now().Add(-time.Hour), specification.Duration, server.URL, clock)
	select {
	case <-requested:
		cancel()
	case <-time.After(time.Second):
		testContext.Fatal("Go origin suppressed a due shared-clock sample")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		testContext.Fatal("shared sampler ignored cancellation")
	}
}

func TestSharedClockSenderSamplerIgnoresGoOrigin(testContext *testing.T) {
	_, source, specification := sharedClockFixture(testContext)
	source.now.Store(specification.Clock.Start + int64(3*time.Second))
	counters := newSenderCounters(1)
	done := make(chan struct{})
	go sampleSharedSender(time.Now().Add(time.Hour), counters, done, &sharedRunClock{source: source, window: *specification.Clock})
	defer close(done)
	time.Sleep(1100 * time.Millisecond)
	counters.mutex.Lock()
	defer counters.mutex.Unlock()
	if len(counters.series) != 1 || counters.series[0].OffsetMillis != 3000 {
		testContext.Fatalf("sender sample uses Go origin: %+v", counters.series)
	}
}

func FuzzSharedClockDrainBounds(fuzzContext *testing.F) {
	fuzzContext.Add(int64(100), int64(10), int64(1))
	fuzzContext.Add(int64(math.MaxInt64), int64(1), int64(1))
	fuzzContext.Fuzz(func(testContext *testing.T, end, drain, resolution int64) {
		window := sharedClockWindow{Domain: sharedClockDomain{Clock: "CLOCK_MONOTONIC", BootID: "test", TimeNamespace: "time:[1]", Resolution: resolution}, End: end}
		if window.validDrain(time.Duration(drain)) && (end+drain < end || end+drain+resolution <= end || drain < 0 || drain > int64(maxRunWindow)) {
			testContext.Fatalf("overflowing drain accepted: %+v, %d", window, drain)
		}
	})
}
