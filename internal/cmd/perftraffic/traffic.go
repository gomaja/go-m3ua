package main

import (
	"context"
	"errors"
	"math"
	"time"
)

const schedulerQuantum = 100 * time.Microsecond

func scheduledMessages(rate uint64, duration time.Duration) (uint64, error) {
	if rate == 0 || duration <= 0 {
		return 0, errors.New("rate and duration must be positive")
	}
	if uint64(duration) > math.MaxUint64/rate {
		return 0, errors.New("scheduled message count overflows")
	}
	return uint64(duration) * rate / uint64(time.Second), nil
}

func dispatchOpenLoop(ctx context.Context, rate uint64, duration time.Duration, started time.Time, expected uint64, clock *sharedRunClock, counters *senderCounters, emit func(index uint64, offset time.Duration, scheduled time.Time)) {
	dispatchScheduleOpenLoop(ctx, constantSchedule{rate: rate}, duration, started, expected, clock, counters, emit)
}

// dispatchScheduleOpenLoop is the open-loop scheduler. The schedule decides
// how many messages are due at each elapsed instant and each message's
// scheduled offset; everything else — the 100 microsecond quantum, the shared
// clock checks, cancellation and the wait for the window end — is the same for
// every schedule.
func dispatchScheduleOpenLoop(ctx context.Context, schedule openLoopSchedule, duration time.Duration, started time.Time, expected uint64, clock *sharedRunClock, counters *senderCounters, emit func(index uint64, offset time.Duration, scheduled time.Time)) {
	// An overload trial judges its offered shape by how late each message was
	// emitted; every other cohort records nothing here.
	var lags *emissionLags
	if counters.overload != nil {
		lags = &counters.overload.emission
	}
	var previousElapsed time.Duration
	if clock != nil {
		elapsed, err := clock.elapsed()
		if err != nil || elapsed >= 0 {
			counters.abort(expected, errors.New("shared measurement start was missed during preparation"))
			return
		}
		previousElapsed = elapsed
	}
	for index := uint64(0); index < expected; {
		if err := ctx.Err(); err != nil {
			counters.abort(expected-index, err)
			return
		}
		elapsed := time.Since(started)
		if clock != nil {
			var err error
			elapsed, err = clock.elapsed()
			if err != nil || elapsed < previousElapsed {
				counters.abort(expected-index, errors.New("shared scheduler clock failed or regressed"))
				return
			}
			previousElapsed = elapsed
		}
		due := schedule.due(elapsed)
		if due > expected {
			due = expected
		}
		if due <= index {
			timer := time.NewTimer(schedulerQuantum)
			select {
			case <-ctx.Done():
				timer.Stop()
				counters.abort(expected-index, ctx.Err())
				return
			case <-timer.C:
			}
			continue
		}
		for index < due {
			if index%256 == 0 {
				if err := ctx.Err(); err != nil {
					counters.abort(expected-index, err)
					return
				}
			}
			offset := schedule.offset(index)
			if lags != nil {
				lags.record(elapsed-offset, offset)
			}
			counters.schedule()
			emit(index, offset, started.Add(offset))
			index++
		}
	}
	if clock != nil {
		if err := clock.waitUntil(ctx, clock.window.End); err != nil {
			counters.setFatal(err.Error())
		}
		return
	}
	remaining := time.Until(started.Add(duration))
	if remaining > 0 {
		timer := time.NewTimer(remaining)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
}

func planMessage(cohort string, seed, index uint64, associations int) messageIdentity {
	flow := uint8(index % uint64(flowCount))
	return messageIdentity{
		Cohort:      cohort,
		Seed:        seed,
		Association: uint8(int(flow) % associations),
		Flow:        flow,
		Sequence:    index / uint64(flowCount),
	}
}

func queueCapacities(associations, total int) []int {
	capacities := make([]int, associations)
	for index := range capacities {
		capacities[index] = total / associations
		if index < total%associations {
			capacities[index]++
		}
	}
	return capacities
}
