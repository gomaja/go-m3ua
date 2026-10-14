package main

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"
)

// virtualClock advances only when the scheduler sleeps, so a test observes
// the exact planned start of every cycle.
type virtualClock struct {
	mutex     sync.Mutex
	now       time.Time
	deadlines []time.Time
}

func (clock *virtualClock) Now() time.Time {
	clock.mutex.Lock()
	defer clock.mutex.Unlock()
	return clock.now
}

func (clock *virtualClock) SleepUntil(ctx context.Context, deadline time.Time) error {
	clock.mutex.Lock()
	clock.deadlines = append(clock.deadlines, deadline)
	if deadline.After(clock.now) {
		clock.now = deadline
	}
	clock.mutex.Unlock()
	return ctx.Err()
}

func TestChurnPlanIsOpenLoopAtTheConfiguredRate(t *testing.T) {
	plan := churnPlan{FirstCycle: 10, Cycles: 7, Rate: 4, Group: 2}
	want := []time.Duration{0, 0, 500 * time.Millisecond, 500 * time.Millisecond, time.Second, time.Second, 1500 * time.Millisecond}
	for index, offset := range want {
		if got := plan.offset(index); got != offset {
			t.Errorf("cycle %d planned at %s, want %s", index, got, offset)
		}
	}
	single := churnPlan{Cycles: 3, Rate: 4, Group: 1}
	if single.offset(2) != 500*time.Millisecond {
		t.Fatalf("ungrouped cycle 2 at %s", single.offset(2))
	}
}

func TestChurnBlockCountsAttemptedCompletedAndFailedCycles(t *testing.T) {
	clock := &virtualClock{now: time.Unix(1000, 0)}
	plan := churnPlan{FirstCycle: 40, Cycles: 40, Rate: 4, Group: 2}
	var mutex sync.Mutex
	seen := make(map[int]bool)
	stats := runChurnBlock(context.Background(), plan, clock, func(_ context.Context, cycle int) cycleOutcome {
		mutex.Lock()
		seen[cycle] = true
		mutex.Unlock()
		if cycle%5 == 0 {
			return cycleOutcome{Mode: "asp-abrupt", Reason: "dial", Err: errors.New("refused")}
		}
		return cycleOutcome{Mode: []string{"asp-graceful", "asp-abrupt", "peer"}[cycle%3], Establish: 3 * time.Millisecond}
	})
	if stats.Attempted != 40 || stats.Completed != 32 || stats.Failed != 8 || stats.FailureReasons["dial"] != 8 {
		t.Fatalf("stats %+v", stats)
	}
	if stats.FirstFailure == "" {
		t.Fatalf("first failure not recorded: %+v", stats)
	}
	completedByMode := 0
	for _, count := range stats.ByMode {
		completedByMode += count
	}
	if completedByMode != stats.Completed || len(stats.EstablishMillis) != stats.Completed {
		t.Fatalf("mode and establishment accounting %+v", stats)
	}
	if len(clock.deadlines) != plan.Cycles || len(seen) != plan.Cycles {
		t.Fatalf("%d releases for %d distinct cycles, want %d", len(clock.deadlines), len(seen), plan.Cycles)
	}
	for index, deadline := range clock.deadlines {
		if want := time.Unix(1000, 0).Add(plan.offset(index)); !deadline.Equal(want) || !seen[plan.FirstCycle+index] {
			t.Fatalf("cycle %d released at %s, planned %s", plan.FirstCycle+index, deadline, want)
		}
	}
	if stats.MaxStartLatenessMillis != 0 || math.Abs(stats.AchievedRate-4) > 1e-9 {
		t.Fatalf("lateness %f ms, achieved rate %f", stats.MaxStartLatenessMillis, stats.AchievedRate)
	}
}

func TestChurnBlockDoesNotWaitForSlowCycles(t *testing.T) {
	plan := churnPlan{Cycles: 10, Rate: 50, Group: 1}
	started := time.Now()
	stats := runChurnBlock(context.Background(), plan, systemClock{}, func(ctx context.Context, _ int) cycleOutcome {
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
		}
		return cycleOutcome{Mode: "peer"}
	})
	elapsed := time.Since(started)
	if stats.Completed != 10 || stats.MaxInFlight < 3 {
		t.Fatalf("an open-loop schedule overlapped at most %d slow cycles: %+v", stats.MaxInFlight, stats)
	}
	// Closed loop would take 10 × 100 ms; open loop takes 9 × 20 ms + 100 ms.
	if elapsed > 700*time.Millisecond {
		t.Fatalf("block took %s: the schedule waited for slow cycles", elapsed)
	}
}

// steppedChurnClock releases one planned cycle start per tick the test sends,
// so a block's starts follow the test rather than the host's scheduler.
type steppedChurnClock struct {
	ticks chan struct{}
}

func (steppedChurnClock) Now() time.Time { return time.Unix(0, 0) }

func (clock steppedChurnClock) SleepUntil(ctx context.Context, _ time.Time) error {
	select {
	case <-clock.ticks:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Cancelling a block stops it starting cycles. Three starts are released; the
// third cycle to run cancels, and the fourth start, never released, must not
// happen. On the wall clock the block goes on starting cycles on schedule until
// one of them runs, so a runner that runs them late saw more than three.
func TestChurnBlockStopsStartingCyclesWhenCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	plan := churnPlan{Cycles: 100, Rate: 100, Group: 1}
	clock := steppedChurnClock{ticks: make(chan struct{})}
	var mutex sync.Mutex
	count := 0
	result := make(chan churnStats, 1)
	go func() {
		result <- runChurnBlock(ctx, plan, clock, func(context.Context, int) cycleOutcome {
			mutex.Lock()
			count++
			if count == 3 {
				cancel()
			}
			mutex.Unlock()
			return cycleOutcome{Mode: "peer"}
		})
	}()
	for release := 0; release < 3; release++ {
		clock.ticks <- struct{}{}
	}
	var stats churnStats
	select {
	case stats = <-result:
	case <-time.After(10 * time.Second):
		t.Fatal("the cancelled block did not return")
	}
	mutex.Lock()
	defer mutex.Unlock()
	if count != 3 || stats.Attempted != 3 || stats.Completed != 3 {
		t.Fatalf("attempted %d, cycles run %d: %+v", stats.Attempted, count, stats)
	}
}

func TestGaugeTracksTheMaximum(t *testing.T) {
	var level gauge
	level.inc()
	level.inc()
	level.dec()
	level.inc()
	level.inc()
	level.dec()
	if level.maximum() != 3 {
		t.Fatalf("maximum %d", level.maximum())
	}
	level.resetPeak()
	if level.maximum() != 2 {
		t.Fatalf("a reset peak starts from the current level, got %d", level.maximum())
	}
	level.inc()
	if level.maximum() != 3 {
		t.Fatalf("maximum after reset %d", level.maximum())
	}
}
