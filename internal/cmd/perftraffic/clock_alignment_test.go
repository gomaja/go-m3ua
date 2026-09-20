package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSharedClockRequiresExplicitOptIn(testContext *testing.T) {
	config, err := parseConfig([]string{"-same-host-clock"})
	if err != nil {
		testContext.Fatalf("same-host clock opt-in: %v", err)
	}
	if !config.SameHostClock {
		testContext.Fatal("same-host clock opt-in was lost")
	}
	legacy, err := parseConfig(nil)
	if err != nil || legacy.SameHostClock {
		testContext.Fatalf("legacy mode changed: %+v, %v", legacy, err)
	}
	if _, err := parseConfig([]string{"-same-host-clock", "-mode=echo"}); err == nil {
		testContext.Fatal("shared clock silently reinterpreted echo timing")
	}
}

type fakeMeasurementClock struct {
	now    atomic.Int64
	domain sharedClockDomain
	err    error
}

func (clock *fakeMeasurementClock) Now() (int64, error) { return clock.now.Load(), clock.err }
func (clock *fakeMeasurementClock) Domain() (sharedClockDomain, error) {
	return clock.domain, clock.err
}

func sharedClockFixture(testContext *testing.T) (*receiverControl, *fakeMeasurementClock, runSpec) {
	testContext.Helper()
	domain := sharedClockDomain{Clock: "CLOCK_MONOTONIC", BootID: "test-boot", TimeNamespace: "time:[123]", Resolution: 1}
	clock := &fakeMeasurementClock{domain: domain}
	clock.now.Store(int64(99 * time.Second))
	specification := runSpec{Cohort: "aligned", Seed: 7, Associations: 1, Rate: 100, Expected: 1000, Duration: 10 * time.Second, Payload: workload128, Outstanding: maxOutstanding, Clock: &sharedClockWindow{Domain: domain, Start: int64(100 * time.Second), End: int64(110 * time.Second)}}
	control := newReceiverControl(1, maxOutstanding)
	control.clock = clock
	control.cpuStatPath = writeCPUStatFixture(testContext, "usage_usec 1\nnr_throttled 0\n")
	control.setAssociationReady(0, 15)
	if err := control.reset(specification); err != nil {
		testContext.Fatal(err)
	}
	if err := control.start(); err != nil {
		testContext.Fatal(err)
	}
	return control, clock, specification
}

func TestSharedClockCommitClassifiesBoundaryWithoutCreditingDrain(testContext *testing.T) {
	control, clock, specification := sharedClockFixture(testContext)
	for index, instant := range []int64{specification.Clock.End - 2, specification.Clock.End - 1, specification.Clock.End, specification.Clock.End + 1, specification.Clock.End + 2} {
		clock.now.Store(instant)
		identity := planMessage(specification.Cohort, specification.Seed, uint64(index), 1)
		_, outcome := control.record(0, validReceivedMessage(identity.Cohort, identity.Seed, identity.Association, identity.Flow, identity.Sequence, 128))
		if outcome != recordUnique {
			testContext.Fatalf("delivery %d: %v", index, outcome)
		}
	}
	snapshot := control.progress()
	if snapshot.Delivery.Unique != 5 || snapshot.Clock.MeasurementLower != 1 || snapshot.Clock.MeasurementUpper != 3 || control.uniqueMeasurement != 2 || control.uniqueDrain != 3 {
		testContext.Fatalf("boundary accounting: %+v, clock %+v", snapshot.Delivery, snapshot.Clock)
	}
	snapshot.Spec.Clock.Start++
	if control.spec.Clock.Start != specification.Clock.Start {
		testContext.Fatal("progress exposed mutable shared clock specification")
	}
	clock.domain.BootID = "changed"
	if err := control.stop(); err == nil || control.result().ClockEvidence.Verified {
		testContext.Fatal("changed post-run domain was accepted")
	}
}

func TestSharedClockRejectsMissingPeerOptInAndLateWindow(testContext *testing.T) {
	_, clock, specification := sharedClockFixture(testContext)
	for _, scenario := range []string{"peer-disabled", "caller-disabled", "domain", "late", "future", "invalid-resolution", "error"} {
		testContext.Run(scenario, func(testContext *testing.T) {
			control := newReceiverControl(1, 16)
			control.setAssociationReady(0, 15)
			local := &fakeMeasurementClock{domain: clock.domain}
			local.now.Store(clock.now.Load())
			control.clock = local
			candidate := copyRunSpec(specification)
			switch scenario {
			case "peer-disabled":
				control.clock = nil
			case "caller-disabled":
				candidate.Clock = nil
			case "domain":
				local.domain.TimeNamespace = "other"
			case "late":
				local.now.Store(candidate.Clock.Start)
			case "future":
				candidate.Clock.Start += int64(3 * time.Second)
				candidate.Clock.End += int64(3 * time.Second)
			case "invalid-resolution":
				candidate.Clock.Domain.Resolution = 0
			case "error":
				local.err = errors.New("clock unavailable")
			}
			if err := control.reset(candidate); err == nil || control.ledger != nil {
				testContext.Fatal("invalid shared-clock reset allocated or started a cohort")
			}
		})
	}
}

func sharedProgressFixture(testContext *testing.T) (runSpec, []progressObservation) {
	testContext.Helper()
	_, _, specification := sharedClockFixture(testContext)
	observation := func(offset time.Duration, unique, lower, upper uint64) progressObservation {
		captured := specification.Clock.Start + int64(offset)
		return progressObservation{Clock: &sharedClockRequest{Before: captured - int64(100*time.Millisecond), After: captured + int64(100*time.Millisecond)}, Snapshot: &receiverProgress{
			Spec: copyRunSpec(specification), Generation: 1, Phase: receiverMeasuring,
			Delivery: ledgerSnapshot{Unique: unique, Missing: specification.Expected - unique},
			Clock:    &sharedClockSnapshot{Domain: specification.Clock.Domain, Captured: captured, MeasurementLower: lower, MeasurementUpper: upper},
		}}
	}
	observations := []progressObservation{observation(-500*time.Millisecond, 0, 0, 0)}
	for index := 1; index <= 9; index++ {
		offset := time.Duration(index)*time.Second + 250*time.Microsecond
		unique := offeredAt(specification, offset) - 5
		observations = append(observations, observation(offset, unique, unique, unique))
	}
	observations = append(observations, observation(11*time.Second, 1000, 995, 996))
	return specification, observations
}

func TestSharedClockProgressRemovesHTTPDelayButKeepsResolution(testContext *testing.T) {
	specification, observations := sharedProgressFixture(testContext)
	result := analyzeProgress(specification, observations)
	if result.Status != "bounded" || result.DeliveredLower != 995 || result.DeliveredUpper != 996 || result.OutstandingLower != 4 || result.OutstandingUpper != 5 || result.BacklogChange.Status != "nonincrease-demonstrated" || result.BacklogChange.MeanChangeLower != 0 || result.BacklogChange.MeanChangeUpper != 0 {
		testContext.Fatalf("aligned accounting: %+v", result)
	}
	for index := 1; index < len(result.Samples)-1; index++ {
		if sample := result.Samples[index]; sample.BacklogLower != 5 || sample.BacklogUpper != 5 {
			testContext.Fatalf("HTTP delay widened timestamped backlog: %+v", sample)
		}
	}
	for index := range observations {
		observations[index].Snapshot.Spec.Clock.Domain.Resolution = int64(time.Millisecond)
		observations[index].Snapshot.Clock.Domain.Resolution = int64(time.Millisecond)
	}
	specification.Clock.Domain.Resolution = int64(time.Millisecond)
	result = analyzeProgress(specification, observations)
	if result.Status != "bounded" || result.BacklogChange.Status != "unresolved" || result.BacklogChange.MeanChangeUpper <= 0 {
		testContext.Fatalf("clock resolution uncertainty was erased: %+v", result)
	}
}

func TestSharedClockProgressFailsClosed(testContext *testing.T) {
	for _, scenario := range []string{"clock", "domain", "envelope", "generation", "counters", "missing-end", "missing-start", "too-many", "overlap"} {
		testContext.Run(scenario, func(testContext *testing.T) {
			specification, observations := sharedProgressFixture(testContext)
			switch scenario {
			case "clock":
				observations[1].Snapshot.Clock = nil
			case "domain":
				observations[1].Snapshot.Clock.Domain.BootID = "other"
			case "envelope":
				observations[1].Snapshot.Clock.Captured = observations[1].Clock.After + 100
			case "generation":
				observations[1].Snapshot.Generation++
			case "counters":
				observations[1].Snapshot.Clock.MeasurementLower = 1001
			case "missing-end":
				observations = observations[:len(observations)-1]
			case "missing-start":
				observations = observations[1:]
			case "too-many":
				observations = make([]progressObservation, 605)
			case "overlap":
				observations[1].Clock.Before = observations[0].Clock.Before
			}
			if result := analyzeProgress(specification, observations); result.Status == "bounded" {
				testContext.Fatalf("invalid %s evidence accepted: %+v", scenario, result)
			}
		})
	}
}

func TestSharedClockGrowingSeriesCannotBeHiddenByDrain(testContext *testing.T) {
	specification, observations := sharedProgressFixture(testContext)
	for index := 1; index < len(observations)-1; index++ {
		snapshot := observations[index].Snapshot
		snapshot.Delivery.Unique -= uint64(index)
		snapshot.Delivery.Missing += uint64(index)
		snapshot.Clock.MeasurementLower -= uint64(index)
		snapshot.Clock.MeasurementUpper -= uint64(index)
	}
	result := analyzeProgress(specification, observations)
	if observations[len(observations)-1].Snapshot.Delivery.Missing != 0 || result.Status != "bounded" || result.BacklogChange.Status != "increase-demonstrated" || result.BacklogChange.MeanChangeLower <= 0 {
		testContext.Fatalf("complete drain hid positive measurement growth: %+v", result)
	}
}

func TestSharedClockReadRegressionInvalidatesProgress(testContext *testing.T) {
	control, clock, specification := sharedClockFixture(testContext)
	clock.now.Store(specification.Clock.Start + int64(time.Second))
	if snapshot := control.progress(); snapshot.FatalError != "" {
		testContext.Fatal(snapshot.FatalError)
	}
	clock.now.Add(-1)
	if snapshot := control.progress(); snapshot.FatalError == "" || snapshot.Clock != nil {
		testContext.Fatal("clock regression was not retained as fatal evidence")
	}
}

func TestSharedClockSnapshotAndCommitsAreRaceSafe(testContext *testing.T) {
	control, clock, specification := sharedClockFixture(testContext)
	clock.now.Store(specification.Clock.Start + int64(time.Second))
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		for index := uint64(0); index < 100; index++ {
			identity := planMessage(specification.Cohort, specification.Seed, index, 1)
			control.record(0, validReceivedMessage(identity.Cohort, identity.Seed, identity.Association, identity.Flow, identity.Sequence, 128))
		}
	}()
	go func() {
		defer workers.Done()
		for index := 0; index < 100; index++ {
			snapshot := control.progress()
			if snapshot.Clock.MeasurementLower != snapshot.Delivery.Unique || snapshot.Clock.MeasurementUpper != snapshot.Delivery.Unique {
				testContext.Errorf("counter and timestamp snapshot tore: %+v", snapshot)
				return
			}
		}
	}()
	workers.Wait()
}

func TestSharedClockHTTPVerifiesDomainAndEnvelope(testContext *testing.T) {
	control, clock, _ := sharedClockFixture(testContext)
	server := httptest.NewServer(control.handler())
	defer server.Close()
	if _, err := readPeerClock(context.Background(), server.URL, clock); err != nil {
		testContext.Fatal(err)
	}
	other := &fakeMeasurementClock{domain: clock.domain}
	other.now.Store(clock.now.Load())
	other.domain.TimeNamespace = "other"
	if _, err := readPeerClock(context.Background(), server.URL, other); err == nil {
		testContext.Fatal("another time namespace was accepted")
	}
	other.domain = clock.domain
	other.now.Store(clock.now.Load() + int64(time.Second))
	if _, err := readPeerClock(context.Background(), server.URL, other); err == nil {
		testContext.Fatal("clock outside HTTP envelope was accepted")
	}
}

func TestSharedClockRejectsMalformedClockResponses(testContext *testing.T) {
	_, clock, _ := sharedClockFixture(testContext)
	valid, err := json.Marshal(sharedClockSnapshot{Domain: clock.domain, Captured: clock.now.Load()})
	if err != nil {
		testContext.Fatal(err)
	}
	for _, body := range []string{"null", "{}", string(valid) + " {}", string(valid) + strings.Repeat(" ", 4096)} {
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) { _, _ = writer.Write([]byte(body)) }))
		_, err := readPeerClock(context.Background(), server.URL, clock)
		server.Close()
		if err == nil {
			testContext.Fatal("malformed or oversized clock evidence accepted")
		}
	}
}

func TestSharedClockRejectsMissedSchedulerStart(testContext *testing.T) {
	_, clock, specification := sharedClockFixture(testContext)
	clock.now.Store(specification.Clock.Start)
	counters := newSenderCounters(1)
	queues := []chan sendJob{make(chan sendJob, 1)}
	dispatchScheduled(context.Background(), commandConfig{Rate: specification.Rate}, specification.Cohort, specification.Duration, time.Now(), specification.Expected, queues, counters, nil, &sharedRunClock{source: clock, window: *specification.Clock})
	if len(queues[0]) != 0 || counters.capped != specification.Expected || counters.fatal == "" {
		testContext.Fatal("missed common start silently shifted the offered schedule")
	}
}

func TestSharedClockReadsAndCommitDoNotAddAllocations(testContext *testing.T) {
	shared, clock, specification := sharedClockFixture(testContext)
	clock.now.Store(specification.Clock.Start + int64(time.Second))
	legacy, _, _ := sharedClockFixture(testContext)
	legacy.spec.Clock = nil
	legacy.clock = nil
	message := validReceivedMessage(specification.Cohort, specification.Seed, 0, 0, 0, 128)
	allocations := func(control *receiverControl) float64 {
		return testing.AllocsPerRun(1000, func() {
			control.ledger.flows[0].seen[0] = 0
			_, outcome := control.record(0, message)
			if outcome != recordUnique {
				panic("record was not unique")
			}
		})
	}
	baseline, aligned := allocations(legacy), allocations(shared)
	if aligned != baseline {
		testContext.Fatalf("shared clock commit allocations %v, legacy %v", aligned, baseline)
	}
}

func FuzzSharedClockBounds(fuzzContext *testing.F) {
	fuzzContext.Add(int64(1), int64(0), uint64(0))
	fuzzContext.Add(int64(-1), int64(100), ^uint64(0))
	fuzzContext.Fuzz(func(testContext *testing.T, resolution, shift int64, unique uint64) {
		specification, observations := sharedProgressFixture(testContext)
		specification.Clock.Domain.Resolution = resolution
		observations[1].Snapshot.Clock.Captured += shift
		observations[1].Snapshot.Delivery.Unique = unique
		result := analyzeProgress(specification, observations)
		if result.Status == "bounded" && (result.DeliveredLower > result.DeliveredUpper || result.DeliveredUpper > specification.Expected || result.OutstandingLower > result.OutstandingUpper) {
			testContext.Fatalf("impossible aligned bounds: %+v", result)
		}
	})
}
