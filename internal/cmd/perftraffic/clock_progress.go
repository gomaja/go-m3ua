package main

import (
	"context"
	"errors"
	"math"
	"time"
)

func observeSharedProgress(ctx context.Context, started time.Time, baseURL string, clock *sharedRunClock) progressObservation {
	if clock == nil {
		return observeProgress(ctx, started, baseURL)
	}
	before, err := clock.source.Now()
	if err != nil {
		return progressObservation{Error: err.Error()}
	}
	snapshot, requestErr := getReceiverProgress(ctx, baseURL)
	after, readErr := clock.source.Now()
	observation := progressObservation{
		Before: time.Duration(before - clock.window.Start), After: time.Duration(after - clock.window.Start),
		Clock: &sharedClockRequest{Before: before, After: after},
	}
	if err := errors.Join(requestErr, readErr); err != nil {
		observation.Error = err.Error()
	} else {
		observation.Snapshot = &snapshot
	}
	return observation
}

func analyzeSharedProgress(specification runSpec, observations []progressObservation) windowAccounting {
	unavailable := func(reason string) windowAccounting {
		return windowAccounting{Status: verdictInconclusive, Reason: reason, Duration: specification.Duration}
	}
	window := specification.Clock
	if window == nil || !window.valid(specification.Duration) || len(observations) < 2 || len(observations) > 604 {
		return unavailable("missing or invalid shared clock window or observations")
	}
	legacy := specification
	legacy.Clock = nil
	converted := make([]progressObservation, len(observations))
	var previousRequestEnd int64
	var previousLower, previousUpper uint64
	var boundary *sharedClockSnapshot
	for index, observation := range observations {
		if observation.Error != "" || observation.Snapshot == nil || observation.Snapshot.Clock == nil || observation.Clock == nil {
			return unavailable("missing shared clock snapshot or request envelope")
		}
		snapshot := observation.Snapshot
		captured := snapshot.Clock
		if !sameRunSpec(snapshot.Spec, specification) || captured.Domain != window.Domain || captured.Captured <= window.Domain.Resolution || captured.Captured > math.MaxInt64-window.Domain.Resolution ||
			!clockEnvelopeContains(observation.Clock.Before, observation.Clock.After, captured.Captured, window.Domain.Resolution) ||
			observation.Clock.Before < previousRequestEnd {
			return unavailable("shared clock domain, cohort or HTTP envelope mismatch")
		}
		if captured.MeasurementLower > captured.MeasurementUpper || captured.MeasurementUpper > snapshot.Delivery.Unique ||
			captured.MeasurementLower < previousLower || captured.MeasurementUpper < previousUpper {
			return unavailable("shared clock boundary counters are inconsistent")
		}
		if boundary != nil && (captured.MeasurementLower != boundary.MeasurementLower || captured.MeasurementUpper != boundary.MeasurementUpper) {
			return unavailable("measurement boundary counters changed after the window")
		}
		copied := *snapshot
		copied.Spec = legacy
		converted[index] = progressObservation{
			Before:   time.Duration(captured.Captured - window.Domain.Resolution - window.Start),
			After:    time.Duration(captured.Captured + window.Domain.Resolution - window.Start),
			Snapshot: &copied,
		}
		if index == 0 {
			if converted[index].After >= 0 || captured.MeasurementUpper != 0 {
				return unavailable("shared clock zero-delivery boundary was not observed before start")
			}
			converted[index].After = 0
		}
		if captured.Captured-window.Domain.Resolution >= window.End {
			boundary = captured
		}
		previousRequestEnd = observation.Clock.After
		previousLower, previousUpper = captured.MeasurementLower, captured.MeasurementUpper
	}
	result := analyzeProgress(legacy, converted)
	if result.Status != "bounded" || boundary == nil {
		return result
	}
	result.DeliveredLower = boundary.MeasurementLower
	result.DeliveredUpper = boundary.MeasurementUpper
	result.OutstandingLower = specification.Expected - result.DeliveredUpper
	result.OutstandingUpper = specification.Expected - result.DeliveredLower
	result.RateLower = float64(result.DeliveredLower) / specification.Duration.Seconds()
	result.RateUpper = float64(result.DeliveredUpper) / specification.Duration.Seconds()
	return result
}
