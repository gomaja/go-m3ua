package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/gomaja/go-m3ua/internal/perfstats"
)

// The sampler takes one observation per second and one more just before the
// measurement window closes. The final lead is deliberately short so the last
// observation brackets the boundary as tightly as possible; a sampler that
// wakes later than that skips the observation rather than sampling a window
// that has already closed.
const (
	progressSampleInterval = time.Second
	progressFinalLead      = 10 * time.Millisecond
	progressRequestTimeout = time.Second
)

type receiverProgress struct {
	Spec       runSpec                   `json:"spec"`
	Generation uint64                    `json:"generation"`
	Phase      receiverPhase             `json:"phase"`
	Delivery   ledgerSnapshot            `json:"delivery"`
	FatalError string                    `json:"fatal_error,omitempty"`
	Clock      *sharedClockSnapshot      `json:"shared_clock,omitempty"`
	Overload   *receiverOverloadProgress `json:"overload,omitempty"`
}

func stopFailedProgress(baseURL string, specification runSpec, observation progressObservation, cause error) (runRecord, runRecord, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	stopErr := postJSON(ctx, baseURL+"/stop", nil)
	receiver, resultErr := getReceiverResult(ctx, baseURL)
	sender := runRecord{
		Side: "sender", Spec: specification, Expected: specification.Expected,
		FatalError: cause.Error(), ProgressObservations: []progressObservation{observation},
	}
	sender.evaluate()
	return sender, receiver, errors.Join(cause, stopErr, resultErr)
}

type progressObservation struct {
	Before   time.Duration       `json:"before_ns"`
	After    time.Duration       `json:"after_ns"`
	Snapshot *receiverProgress   `json:"snapshot,omitempty"`
	Error    string              `json:"error,omitempty"`
	Clock    *sharedClockRequest `json:"shared_clock_request,omitempty"`
	// SenderBefore and SenderAfter bracket the request with the sender's own
	// attempt accounting; only overload measurement cohorts record them.
	SenderBefore *overloadSenderMark `json:"sender_before,omitempty"`
	SenderAfter  *overloadSenderMark `json:"sender_after,omitempty"`
}

type backlogInterval struct {
	Before       time.Duration `json:"before_ns"`
	After        time.Duration `json:"after_ns"`
	Unique       uint64        `json:"unique"`
	BacklogLower uint64        `json:"backlog_lower"`
	BacklogUpper uint64        `json:"backlog_upper"`
}

type windowAccounting struct {
	Status           string            `json:"status"`
	Reason           string            `json:"reason,omitempty"`
	Duration         time.Duration     `json:"duration_ns"`
	DeliveredLower   uint64            `json:"delivered_lower"`
	DeliveredUpper   uint64            `json:"delivered_upper"`
	OutstandingLower uint64            `json:"outstanding_lower"`
	OutstandingUpper uint64            `json:"outstanding_upper"`
	RateLower        float64           `json:"rate_lower"`
	RateUpper        float64           `json:"rate_upper"`
	Samples          []backlogInterval `json:"samples,omitempty"`
	BacklogTrend     backlogTrend      `json:"backlog_trend"`
}

// backlogTrend is the sender-window sustained-backlog evidence: the fitted
// growth bounds over the measurement window and their verdict against the
// materiality floor. Status is a perfstats.BacklogVerdict for a fitted trend,
// or perfstats.BacklogTrendInsufficientSamples or
// perfstats.BacklogTrendInvalidSamples when no trend could be fitted.
type backlogTrend struct {
	Status string `json:"status"`
	perfstats.BacklogTrend
}

func describeBacklogTrend(samples []backlogInterval, duration time.Duration, rate uint64) backlogTrend {
	observations := make([]perfstats.BacklogObservation, len(samples))
	for index, sample := range samples {
		observations[index] = perfstats.BacklogObservation{Before: sample.Before, After: sample.After, Lower: sample.BacklogLower, Upper: sample.BacklogUpper}
	}
	status, trend := perfstats.DescribeBacklogTrend(observations, duration, rate)
	return backlogTrend{Status: status, BacklogTrend: trend}
}

func (control *receiverControl) progress() receiverProgress {
	control.mutex.Lock()
	defer control.mutex.Unlock()
	result := receiverProgress{Spec: copyRunSpec(control.spec), Generation: control.generation, Phase: control.phase, FatalError: control.fatal}
	if control.overload != nil && control.overload.begun {
		overload, snapshot, present := control.overloadProgressLocked()
		result.Overload = overload
		if present {
			result.Delivery = snapshot
		}
	} else if snapshot, present := control.deliveryLocked(); present {
		result.Delivery = snapshot
	}
	if control.spec.Clock != nil {
		now, err := control.sharedNowLocked()
		if err != nil {
			result.FatalError = err.Error()
		} else {
			result.Clock = &sharedClockSnapshot{Domain: control.spec.Clock.Domain, Captured: now, MeasurementLower: control.measurementLower, MeasurementUpper: control.measurementUpper}
		}
	}
	return result
}

func offeredAt(specification runSpec, elapsed time.Duration) uint64 {
	if elapsed <= 0 {
		return 0
	}
	if schedule := specification.overloadSchedule(); schedule != nil {
		return min(schedule.due(elapsed), specification.Expected)
	}
	if elapsed >= specification.Duration {
		return specification.Expected
	}
	return min(uint64(elapsed)*specification.Rate/uint64(time.Second)+1, specification.Expected)
}

// specExpected is the number of messages a specification schedules: the
// phased total of an overload measurement cohort, otherwise rate times
// duration.
func specExpected(specification runSpec) (uint64, error) {
	if specification.overloadMeasurement() {
		schedule := specification.overloadSchedule()
		if schedule == nil || schedule.duration != specification.Duration {
			return 0, errors.New("invalid overload schedule")
		}
		return schedule.expected, nil
	}
	return scheduledMessages(specification.Rate, specification.Duration)
}

func analyzeProgress(specification runSpec, observations []progressObservation) windowAccounting {
	if specification.Clock != nil {
		return analyzeSharedProgress(specification, observations)
	}
	unavailable := func(reason string) windowAccounting {
		return windowAccounting{Status: verdictInconclusive, Reason: reason, Duration: specification.Duration}
	}
	expected, err := specExpected(specification)
	if err != nil || specification.Duration <= 0 || specification.Duration > maxRunWindow || specification.Rate == 0 || specification.Rate > maxOfferedRate || expected == 0 || expected != specification.Expected {
		return unavailable("invalid offered schedule")
	}
	if len(observations) < 2 || len(observations) > 604 {
		return unavailable("missing or excessive progress observations")
	}
	initial := observations[0]
	if initial.Snapshot == nil || initial.Before > 0 || initial.After != 0 || initial.Snapshot.Generation == 0 || initial.Snapshot.Delivery.Unique != 0 {
		return unavailable("zero-delivery start boundary was not observed")
	}
	result := windowAccounting{Status: "bounded", Duration: specification.Duration, DeliveredUpper: specification.Expected}
	var previousUnique uint64
	previousAfter := initial.Before
	endObserved := false
	for index, observation := range observations {
		if observation.Error != "" || observation.Snapshot == nil {
			return unavailable("progress request failed or returned no snapshot")
		}
		snapshot := observation.Snapshot
		if !sameRunSpec(snapshot.Spec, specification) || snapshot.Generation != initial.Snapshot.Generation || snapshot.Phase != receiverMeasuring {
			return unavailable("progress cohort, generation or phase mismatch")
		}
		if observation.Before < previousAfter || observation.After < observation.Before || index > 0 && observation.Before < 0 {
			return unavailable("progress timestamps are reversed or overlapping")
		}
		delivery := snapshot.Delivery
		if snapshot.FatalError != "" || delivery.Duplicate != 0 || delivery.Invalid != 0 || delivery.Reordered != 0 {
			return unavailable("progress contains a delivery or fixture failure")
		}
		if delivery.Unique < previousUnique || delivery.Unique > offeredAt(specification, observation.After) || delivery.Missing != specification.Expected-delivery.Unique {
			return unavailable("progress delivery counters are inconsistent with the offered schedule")
		}
		lower := uint64(0)
		if offered := offeredAt(specification, observation.Before); offered > delivery.Unique {
			lower = offered - delivery.Unique
		}
		result.Samples = append(result.Samples, backlogInterval{
			Before: observation.Before, After: observation.After, Unique: delivery.Unique,
			BacklogLower: lower, BacklogUpper: offeredAt(specification, observation.After) - delivery.Unique,
		})
		if observation.After <= specification.Duration {
			result.DeliveredLower = max(result.DeliveredLower, delivery.Unique)
		}
		if observation.Before >= specification.Duration {
			endObserved = true
			result.DeliveredUpper = min(result.DeliveredUpper, delivery.Unique)
		}
		previousAfter = observation.After
		previousUnique = delivery.Unique
	}
	if !endObserved {
		return unavailable("measurement end was not bracketed")
	}
	result.OutstandingLower = specification.Expected - result.DeliveredUpper
	result.OutstandingUpper = specification.Expected - result.DeliveredLower
	result.RateLower = float64(result.DeliveredLower) / specification.Duration.Seconds()
	result.RateUpper = float64(result.DeliveredUpper) / specification.Duration.Seconds()
	result.BacklogTrend = describeBacklogTrend(result.Samples, specification.Duration, specification.Rate)
	return result
}

func getReceiverProgress(ctx context.Context, baseURL string) (receiverProgress, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/progress", nil)
	if err != nil {
		return receiverProgress{}, err
	}
	response, err := fixtureHTTPClient.Do(request)
	if err != nil {
		return receiverProgress{}, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return receiverProgress{}, fmt.Errorf("receiver progress: %s", response.Status)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, (16<<10)+1))
	if err != nil {
		return receiverProgress{}, err
	}
	if len(body) > 16<<10 {
		return receiverProgress{}, errors.New("receiver progress exceeds 16 KiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	var progress *receiverProgress
	if err := decoder.Decode(&progress); err != nil {
		return receiverProgress{}, err
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return receiverProgress{}, errors.New("receiver progress contains trailing data")
	}
	if progress == nil {
		return receiverProgress{}, errors.New("receiver progress is null")
	}
	return *progress, nil
}

func observeProgress(ctx context.Context, started time.Time, baseURL string) progressObservation {
	before := time.Now()
	snapshot, err := getReceiverProgress(ctx, baseURL)
	observation := progressObservation{Before: before.Sub(started), After: time.Since(started)}
	if err != nil {
		observation.Error = err.Error()
	} else {
		observation.Snapshot = &snapshot
	}
	return observation
}

func sampleProgress(ctx context.Context, started time.Time, duration time.Duration, baseURL string) <-chan []progressObservation {
	return sampleSharedProgress(ctx, started, duration, baseURL, nil)
}

func sampleSharedProgress(ctx context.Context, started time.Time, duration time.Duration, baseURL string, clock *sharedRunClock) <-chan []progressObservation {
	return sampleMarkedProgress(ctx, started, duration, baseURL, clock, nil)
}

// sampleMarkedProgress is the progress sampler. When mark is set, each
// observation is bracketed by the sender's attempt accounting taken just
// before and just after its request.
func sampleMarkedProgress(ctx context.Context, started time.Time, duration time.Duration, baseURL string, clock *sharedRunClock, mark func() overloadSenderMark) <-chan []progressObservation {
	observe := func(requestContext context.Context) progressObservation {
		if mark == nil {
			return observeSharedProgress(requestContext, started, baseURL, clock)
		}
		before := mark()
		observation := observeSharedProgress(requestContext, started, baseURL, clock)
		after := mark()
		observation.SenderBefore, observation.SenderAfter = &before, &after
		return observation
	}
	done := make(chan []progressObservation, 1)
	go func() {
		offsets := progressOffsets(duration)
		observations := make([]progressObservation, 0, len(offsets))
		defer func() { done <- observations }()
		for _, offset := range offsets {
			if clock != nil {
				if err := clock.waitUntil(ctx, clock.window.Start+int64(offset)); err != nil {
					observations = append(observations, progressObservation{Error: err.Error()})
					return
				}
				elapsed, err := clock.elapsed()
				if err != nil {
					observations = append(observations, progressObservation{Error: err.Error()})
					return
				}
				if elapsed >= duration {
					return
				}
				requestContext, cancel := context.WithTimeout(ctx, progressRequestTimeout)
				observations = append(observations, observe(requestContext))
				cancel()
				continue
			}
			timer := time.NewTimer(max(time.Until(started.Add(offset)), 0))
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
				if time.Since(started) >= duration {
					return
				}
				requestContext, cancel := context.WithTimeout(ctx, progressRequestTimeout)
				observation := observe(requestContext)
				cancel()
				observations = append(observations, observation)
			}
		}
	}()
	return done
}

func progressOffsets(duration time.Duration) []time.Duration {
	if duration <= 0 || duration > maxRunWindow {
		return nil
	}
	finalOffset := duration - min(progressFinalLead, duration/2)
	offsets := make([]time.Duration, 0, int(duration/progressSampleInterval)+1)
	for offset := progressSampleInterval; offset < duration; offset += progressSampleInterval {
		if finalOffset > 0 && finalOffset < offset {
			offsets = append(offsets, finalOffset)
			finalOffset = 0
		} else if finalOffset == offset {
			finalOffset = 0
		}
		offsets = append(offsets, offset)
	}
	if finalOffset > 0 && finalOffset < duration {
		offsets = append(offsets, finalOffset)
	}
	return offsets
}
