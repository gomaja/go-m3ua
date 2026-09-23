package main

import (
	"context"
	"sync"
	"time"
)

// churnPlan is one open-loop block of association lifecycle cycles. Cycles
// are released Group at a time, Group/Rate seconds apart, so the long-run
// start rate is Rate and each release point starts Group establishments
// together: that is what makes the listener's accepts overlap.
type churnPlan struct {
	FirstCycle int     `json:"first_cycle"`
	Cycles     int     `json:"cycles"`
	Rate       float64 `json:"rate"`
	Group      int     `json:"group"`
}

// offset is the planned start of relative cycle index, measured from the
// block start.
func (plan churnPlan) offset(index int) time.Duration {
	group := index / plan.Group
	return time.Duration(float64(group) * float64(plan.Group) / plan.Rate * float64(time.Second))
}

// cycleOutcome is what one cycle reports back to the scheduler. A nil Err is
// a completed cycle; Reason classifies a failure.
type cycleOutcome struct {
	Mode      string
	Establish time.Duration
	Reason    string
	Err       error
}

type churnClock interface {
	Now() time.Time
	SleepUntil(ctx context.Context, deadline time.Time) error
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

func (systemClock) SleepUntil(ctx context.Context, deadline time.Time) error {
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// runChurnBlock starts every cycle of plan at its planned time, each in its
// own goroutine, and never waits for a running cycle before starting the next
// one: a slow establishment or close delays nothing but itself. It returns
// once every started cycle has finished. Cancelling ctx stops new starts; the
// cycles already started receive the same ctx and are still waited for.
func runChurnBlock(ctx context.Context, plan churnPlan, clock churnClock, cycle func(context.Context, int) cycleOutcome) churnStats {
	stats := churnStats{FailureReasons: map[string]int{}, ByMode: map[string]int{}}
	var (
		mutex    sync.Mutex
		group    sync.WaitGroup
		inFlight gauge
	)
	start := clock.Now()
	var firstStart, lastStart time.Time
	for index := 0; index < plan.Cycles; index++ {
		planned := start.Add(plan.offset(index))
		if err := clock.SleepUntil(ctx, planned); err != nil || ctx.Err() != nil {
			break
		}
		actual := clock.Now()
		if lateness := actual.Sub(planned); lateness > 0 {
			stats.MaxStartLatenessMillis = max(stats.MaxStartLatenessMillis, float64(lateness)/float64(time.Millisecond))
		}
		if index == 0 {
			firstStart = actual
		}
		lastStart = actual
		stats.Attempted++
		inFlight.inc()
		group.Add(1)
		go func(number int) {
			defer group.Done()
			defer inFlight.dec()
			outcome := cycle(ctx, number)
			mutex.Lock()
			defer mutex.Unlock()
			if outcome.Err != nil {
				stats.Failed++
				stats.FailureReasons[outcome.Reason]++
				if stats.FirstFailure == "" {
					stats.FirstFailure = outcome.Reason + ": " + outcome.Err.Error()
				}
				return
			}
			stats.Completed++
			stats.ByMode[outcome.Mode]++
			stats.EstablishMillis = append(stats.EstablishMillis, float64(outcome.Establish)/float64(time.Millisecond))
		}(plan.FirstCycle + index)
	}
	group.Wait()
	stats.MaxInFlight = inFlight.maximum()
	if stats.Attempted > 0 {
		stats.StartSpanMillis = float64(lastStart.Sub(firstStart)) / float64(time.Millisecond)
		// The span from the first to the last release point covers every
		// release but the last, which accounts for one more Group/Rate slot.
		window := lastStart.Sub(firstStart).Seconds() + float64(plan.Group)/plan.Rate
		stats.AchievedRate = float64(stats.Attempted) / window
	}
	return stats
}

// gauge tracks a level and its maximum, for in-flight and concurrently
// establishing cycles.
type gauge struct {
	mutex   sync.Mutex
	current int
	peak    int
}

func (level *gauge) inc() {
	level.mutex.Lock()
	level.current++
	level.peak = max(level.peak, level.current)
	level.mutex.Unlock()
}

func (level *gauge) dec() {
	level.mutex.Lock()
	level.current--
	level.mutex.Unlock()
}

// resetPeak starts a new maximum from the current level.
func (level *gauge) resetPeak() {
	level.mutex.Lock()
	level.peak = level.current
	level.mutex.Unlock()
}

func (level *gauge) maximum() int {
	level.mutex.Lock()
	defer level.mutex.Unlock()
	return level.peak
}
