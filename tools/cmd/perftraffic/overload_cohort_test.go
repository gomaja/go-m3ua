package main

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// syntheticCohort describes a small overload measurement cohort: 2,000/s for
// 1 s then 500/s for 12 s (nominal 1,000/s, switch at 1 s). The first 500
// messages of the 2x phase are refused at the cap, and refused lists further
// refusals in the recovery phase. Every accepted message is validated before
// the next whole second, and the receiver captures each progress snapshot
// capture after that second.
type syntheticCohort struct {
	clock      bool
	resolution time.Duration
	capture    time.Duration
	refused    map[uint64]bool
}

const syntheticClockStart = int64(10 * time.Second)

func (cohort syntheticCohort) evidence(testContext *testing.T) overloadEvidence {
	testContext.Helper()
	profile, err := parseOverloadProfile("2x:1s,0.5x:12s", 1_000)
	if err != nil {
		testContext.Fatal(err)
	}
	schedule := profile.schedule
	specification := runSpec{
		Cohort: "synthetic", Seed: 1, Associations: 1, Expected: profile.expected(), Duration: profile.duration(),
		Drain: overloadRequestDeadline + overloadDrainMargin, Rate: 1_000, Outstanding: maxOutstanding, Payload: workload128,
		Mode: modeThroughput, Direction: directionASPToSGP, Initiation: initiationASPDial,
		Overload: profile.spec(overloadRoleMeasurement),
	}
	domain := sharedClockDomain{Clock: "CLOCK_MONOTONIC", BootID: "boot", TimeNamespace: "monotonic-offset:0.000000000", Resolution: int64(cohort.resolution)}
	if cohort.clock {
		specification.Clock = &sharedClockWindow{Domain: domain, Start: syntheticClockStart, End: syntheticClockStart + int64(profile.duration())}
	}
	accepted, indeterminate := newBitmap(schedule.expected), newBitmap(schedule.expected)
	phases := make([]overloadClassCounts, len(schedule.phases))
	notAdmitted := make([]uint64, 13)
	for index := uint64(0); index < schedule.expected; index++ {
		phase := &phases[schedule.phaseOfIndex(index)]
		phase.Offered++
		switch {
		case index >= 1_500 && index < 2_000:
			phase.CapRefusedOutstanding++
			notAdmitted[schedule.offset(index)/time.Second]++
		case cohort.refused[index]:
			phase.CapRefusedQueue++
			notAdmitted[schedule.offset(index)/time.Second]++
		default:
			phase.Accepted++
			accepted.add(index)
		}
	}
	deliveredBefore := func(offset time.Duration) uint64 {
		var count uint64
		for index := uint64(0); index < schedule.expected && schedule.offset(index) < offset; index++ {
			if accepted.has(index) {
				count++
			}
		}
		return count
	}
	var observations []progressObservation
	for _, at := range []time.Duration{-time.Millisecond, time.Second, 2 * time.Second, 3 * time.Second, 4 * time.Second, 5 * time.Second,
		6 * time.Second, 7 * time.Second, 8 * time.Second, 9 * time.Second, 10 * time.Second, 11 * time.Second, 12 * time.Second, 13*time.Second - 10*time.Millisecond} {
		unique := deliveredBefore(at)
		mark := overloadSenderMark{Accepted: unique, Started: unique}
		observation := progressObservation{
			Before: at + cohort.capture - 100*time.Microsecond, After: at + cohort.capture + 100*time.Microsecond,
			SenderBefore: &mark, SenderAfter: &mark,
			Snapshot: &receiverProgress{Spec: specification, Phase: receiverMeasuring, Delivery: ledgerSnapshot{Unique: unique},
				Overload: &receiverOverloadProgress{}},
		}
		if cohort.clock {
			captured := syntheticClockStart + int64(at+cohort.capture)
			observation.Snapshot.Clock = &sharedClockSnapshot{Domain: domain, Captured: captured}
			observation.Clock = &sharedClockRequest{Before: captured - int64(100*time.Microsecond), After: captured + int64(100*time.Microsecond)}
		}
		observations = append(observations, observation)
	}
	var totals overloadClassCounts
	for _, phase := range phases {
		totals = totals.plus(phase)
	}
	var series []overloadSenderPoint
	for second := 1; second <= 12; second++ {
		offset := time.Duration(second) * time.Second
		series = append(series, overloadSenderPoint{OffsetMillis: uint64(offset / time.Millisecond), overloadClassCounts: overloadClassCounts{Offered: schedule.due(offset)}})
	}
	series = append(series, overloadSenderPoint{OffsetMillis: 15_000, overloadClassCounts: totals, Final: true})
	healthy := overloadAssociationObservation{QueueCapacity: dataQueueSize, MaxQueued: 3, State: "ASP-ACTIVE", Active: true, EpochStart: 1, EpochEnd: 1}
	receiver := runRecord{
		Side: "receiver", Spec: specification, Delivery: deliveryResult{Unique: accepted.count()},
		Overload: &overloadRecord{Receiver: &overloadReceiverRecord{
			QueuePolls: 1_300, Associations: []overloadAssociationObservation{healthy},
			UniqueByPhase: []uint64{phases[0].Accepted, phases[1].Accepted}, DuplicateByPhase: []uint64{0, 0},
		}},
	}
	return overloadEvidence{
		specification: specification, schedule: schedule, phases: phases, maxOutstanding: 5,
		accepted: accepted, indeterminate: indeterminate, notAdmitted: notAdmitted, series: series,
		emissionLag:      durationPercentiles{Count: schedule.expected, P50: 100 * time.Microsecond, P95: 500 * time.Microsecond, P99: time.Millisecond, Max: 3 * time.Millisecond},
		maxEmissionLagAt: 3 * time.Second,
		fixtureQueueMax:  []int{5}, senderAssociations: []overloadAssociationObservation{healthy},
		receiver: receiver, delivered: append(bitmap(nil), accepted...), observations: observations,
		senderWindow: &windowAccounting{Status: "bounded"}, clockVerified: cohort.clock,
	}
}

func criterionStatus(record *overloadRecord, name string) (string, string) {
	for _, criterion := range record.Acceptance.Criteria {
		if criterion.Name == name {
			return criterion.Status, criterion.Detail
		}
	}
	return "", ""
}

// The shared-clock path brackets each receiver capture by the clock
// resolution, exactly as the nominal sender window does, and places it
// relative to the shared start.
func TestOverloadPointsUseTheSharedClockCapture(testContext *testing.T) {
	cohort := syntheticCohort{clock: true, resolution: 250 * time.Microsecond, capture: 700 * time.Microsecond}
	evidence := cohort.evidence(testContext)
	points, err := overloadPoints(evidence.specification, evidence.observations)
	if err != nil || len(points) != len(evidence.observations) {
		testContext.Fatalf("points = %+v, %v", points, err)
	}
	for index, point := range points {
		captured := evidence.observations[index].Snapshot.Clock.Captured
		wantBefore := time.Duration(captured - int64(cohort.resolution) - syntheticClockStart)
		wantAfter := time.Duration(captured + int64(cohort.resolution) - syntheticClockStart)
		if point.Before != wantBefore || point.After != wantAfter {
			testContext.Fatalf("point %d = [%s, %s], want [%s, %s]", index, point.Before, point.After, wantBefore, wantAfter)
		}
	}
	if points[1].Before != time.Second+450*time.Microsecond || points[1].After != time.Second+950*time.Microsecond {
		testContext.Fatalf("switch point = [%s, %s]", points[1].Before, points[1].After)
	}
	// The sender-side request envelope is not the capture bracket.
	if points[1].Before == evidence.observations[1].Before {
		testContext.Fatal("the shared-clock path used the request envelope instead of the capture")
	}
	missing := evidence.observations
	missing[3].Snapshot.Clock = nil
	if _, err := overloadPoints(evidence.specification, missing); err == nil || !strings.Contains(err.Error(), "no receiver capture") {
		testContext.Fatalf("missing capture: %v", err)
	}
	record := evaluateOverloadCohort(evidence)
	if record.Acceptance.Verdict != verdictInvalid {
		testContext.Fatalf("a missing capture did not invalidate the cohort: %+v", record.Acceptance)
	}
	if status, detail := criterionStatus(record, "fixture"); status != overloadCriterionFail || !strings.Contains(detail, "no receiver capture") {
		testContext.Fatalf("fixture criterion = %s: %s", status, detail)
	}
}

// Whether the observation taken at the switch can start a recovery window
// depends on the capture bracket. With a fine resolution its bracket starts
// after the switch and it is the first candidate, as in the HTTP-interval
// path; once the resolution exceeds the capture delay its bracket may start
// before the switch, it is excluded, and the next observation's window is the
// only candidate left within the allowance.
func TestOverloadSharedClockRecoveryWindowEligibility(testContext *testing.T) {
	capture := 500 * time.Microsecond
	for _, scenario := range []struct {
		name     string
		cohort   syntheticCohort
		status   string
		recovery time.Duration
	}{
		{name: "http-interval", cohort: syntheticCohort{capture: capture}, status: overloadRecovered, recovery: capture + 100*time.Microsecond},
		{name: "fine-clock", cohort: syntheticCohort{clock: true, resolution: time.Nanosecond, capture: capture}, status: overloadRecovered, recovery: capture + time.Nanosecond},
		{name: "coarse-clock", cohort: syntheticCohort{clock: true, resolution: 600 * time.Microsecond, capture: capture}, status: overloadRecovered, recovery: time.Second + capture + 600*time.Microsecond},
		// Ten refusals just after 2 s leave the only window the coarse clock
		// can use 490 deliveries against 490.59 required. The other paths
		// recover in the window that starts at the switch.
		{name: "http-interval-dip", cohort: syntheticCohort{capture: capture, refused: dipAfterTwoSeconds()}, status: overloadRecovered, recovery: capture + 100*time.Microsecond},
		{name: "fine-clock-dip", cohort: syntheticCohort{clock: true, resolution: time.Nanosecond, capture: capture, refused: dipAfterTwoSeconds()}, status: overloadRecovered, recovery: capture + time.Nanosecond},
		{name: "coarse-clock-dip", cohort: syntheticCohort{clock: true, resolution: 600 * time.Microsecond, capture: capture, refused: dipAfterTwoSeconds()}, status: overloadNotRecovered},
	} {
		testContext.Run(scenario.name, func(testContext *testing.T) {
			record := evaluateOverloadCohort(scenario.cohort.evidence(testContext))
			if len(record.Recovery) != 1 {
				testContext.Fatalf("recovery = %+v", record.Recovery)
			}
			recovery := record.Recovery[0]
			if recovery.Status != scenario.status || scenario.status == overloadRecovered && recovery.RecoveryTime != scenario.recovery {
				testContext.Fatalf("recovery = %+v, want %s in %s", recovery, scenario.status, scenario.recovery)
			}
			wantVerdict := verdictPass
			if scenario.status != overloadRecovered {
				wantVerdict = overloadVerdictFail
			}
			if record.Acceptance.Verdict != wantVerdict {
				testContext.Fatalf("verdict %s, want %s: %+v", record.Acceptance.Verdict, wantVerdict, record.Acceptance.Criteria)
			}
		})
	}
}

// dipAfterTwoSeconds refuses the ten recovery-phase messages scheduled from
// 2 s on (indexes 2,500 to 2,509 at 2 ms spacing).
func dipAfterTwoSeconds() map[uint64]bool {
	refused := make(map[uint64]bool)
	for index := uint64(2_500); index < 2_510; index++ {
		refused[index] = true
	}
	return refused
}

func TestOverloadCohortFixtureFailuresAreAssembled(testContext *testing.T) {
	for _, scenario := range []struct {
		name    string
		cohort  syntheticCohort
		mutate  func(*overloadEvidence)
		verdict string
		failed  string
		detail  string
	}{
		{name: "unverified-clock", cohort: syntheticCohort{clock: true, resolution: time.Nanosecond, capture: time.Millisecond},
			mutate: func(evidence *overloadEvidence) { evidence.clockVerified = false }, verdict: verdictInvalid, failed: "fixture", detail: "shared clock evidence is not verified"},
		{name: "missing-receiver-overload", mutate: func(evidence *overloadEvidence) { evidence.receiver.Overload = nil },
			verdict: verdictInvalid, failed: "fixture", detail: "receiver record has no overload observations"},
		{name: "queues-never-polled", mutate: func(evidence *overloadEvidence) { evidence.receiver.Overload.Receiver.QueuePolls = 0 },
			verdict: verdictInvalid, failed: "fixture", detail: "never observed its DATA queues"},
		{name: "sender-fatal", mutate: func(evidence *overloadEvidence) { evidence.senderFatal = "clock regressed" },
			verdict: verdictInvalid, failed: "fixture", detail: "sender fixture failure: clock regressed"},
		{name: "unbounded-window", mutate: func(evidence *overloadEvidence) {
			evidence.senderWindow = &windowAccounting{Status: verdictInconclusive, Reason: "progress request failed"}
		}, verdict: verdictInvalid, failed: "fixture", detail: "progress request failed"},
		{name: "receiver-not-stopped", mutate: func(evidence *overloadEvidence) {
			evidence.delivered = nil
			evidence.deliveredErr = errors.New("receiver did not stop cleanly: 409 Conflict")
		}, verdict: verdictInconclusive, failed: "accounting", detail: "receiver did not stop cleanly"},
	} {
		testContext.Run(scenario.name, func(testContext *testing.T) {
			cohort := scenario.cohort
			if cohort.capture == 0 {
				cohort.capture = time.Millisecond
			}
			evidence := cohort.evidence(testContext)
			if record := evaluateOverloadCohort(evidence); record.Acceptance.Verdict != verdictPass {
				testContext.Fatalf("unmutated cohort: %+v", record.Acceptance.Criteria)
			}
			scenario.mutate(&evidence)
			record := evaluateOverloadCohort(evidence)
			status, detail := criterionStatus(record, scenario.failed)
			if record.Acceptance.Verdict != scenario.verdict || status == overloadCriterionPass || !strings.Contains(detail, scenario.detail) {
				testContext.Fatalf("verdict %s, %s %s: %s", record.Acceptance.Verdict, scenario.failed, status, detail)
			}
		})
	}
}

// The offered load must follow the phased schedule. Both bounds are tested
// on both sides: the scheduler lag at exactly the tolerance passes and a
// nanosecond more invalidates the trial; a shortfall of exactly 10 ms of the
// phase's traffic passes and one more message invalidates it; the offered
// count may never run ahead of the schedule.
func TestOverloadOfferedShapeIsJudged(testContext *testing.T) {
	for _, scenario := range []struct {
		name   string
		mutate func(*overloadEvidence)
		valid  bool
		reason string
	}{
		{name: "p99-at-tolerance", mutate: func(evidence *overloadEvidence) { evidence.emissionLag.P99 = overloadShapeTolerance }, valid: true},
		{name: "p99-beyond-tolerance", mutate: func(evidence *overloadEvidence) { evidence.emissionLag.P99 = overloadShapeTolerance + 1 }, reason: "the p99 emission lag"},
		{name: "max-at-bound", mutate: func(evidence *overloadEvidence) { evidence.emissionLag.Max = overloadShapeMaximumLag }, valid: true},
		{name: "max-beyond-bound", mutate: func(evidence *overloadEvidence) { evidence.emissionLag.Max = overloadShapeMaximumLag + 1 }, reason: "the scheduler emitted the message scheduled at 3s"},
		{name: "emissions-missing", mutate: func(evidence *overloadEvidence) { evidence.emissionLag.Count-- }, reason: "recorded the emission of"},
		// At 3 s the 500/s recovery phase allows ceil(500 x 10 ms) = 5.
		{name: "shortfall-at-allowance", mutate: func(evidence *overloadEvidence) { evidence.series[2].Offered -= 5 }, valid: true},
		{name: "shortfall-beyond-allowance", mutate: func(evidence *overloadEvidence) { evidence.series[2].Offered -= 6 }, reason: "6 short of the"},
		{name: "ahead-of-schedule", mutate: func(evidence *overloadEvidence) { evidence.series[2].Offered += 2 }, reason: "ahead of the schedule"},
		// A sample offset is truncated to the millisecond. At 2,000/s one
		// more message falls due inside the millisecond after 500 ms, so a
		// count one above due(500 ms) is on schedule and two above is not.
		{name: "within-the-sample-millisecond", mutate: func(evidence *overloadEvidence) {
			evidence.series = append([]overloadSenderPoint{{OffsetMillis: 500, overloadClassCounts: overloadClassCounts{Offered: evidence.schedule.due(500*time.Millisecond) + 1}}}, evidence.series...)
		}, valid: true},
		{name: "beyond-the-sample-millisecond", mutate: func(evidence *overloadEvidence) {
			evidence.series = append([]overloadSenderPoint{{OffsetMillis: 500, overloadClassCounts: overloadClassCounts{Offered: evidence.schedule.due(500*time.Millisecond) + 2}}}, evidence.series...)
		}, reason: "1 ahead of the schedule"},
		{name: "no-samples", mutate: func(evidence *overloadEvidence) { evidence.series = evidence.series[len(evidence.series)-1:] }, reason: "no per-second offered observation"},
	} {
		testContext.Run(scenario.name, func(testContext *testing.T) {
			evidence := syntheticCohort{capture: time.Millisecond}.evidence(testContext)
			scenario.mutate(&evidence)
			record := evaluateOverloadCohort(evidence)
			shape := record.OfferedShape
			if scenario.valid {
				if record.Acceptance.Verdict != verdictPass || shape.Status != overloadShapeFollowed {
					testContext.Fatalf("shape %+v verdict %s", shape, record.Acceptance.Verdict)
				}
				return
			}
			status, detail := criterionStatus(record, "fixture")
			if record.Acceptance.Verdict != verdictInvalid || shape.Status != overloadShapeDeviated || status != overloadCriterionFail ||
				!strings.Contains(detail, "did not follow the phased schedule") || !strings.Contains(shape.Reason, scenario.reason) {
				testContext.Fatalf("shape %+v verdict %s fixture %s", shape, record.Acceptance.Verdict, detail)
			}
		})
	}
}

// The per-second class series must end, after the drain, at the totals:
// without the final point, or with a final point that disagrees with them,
// the trial is invalid.
func TestOverloadSeriesMustEndAtTheTotals(testContext *testing.T) {
	evidence := syntheticCohort{capture: time.Millisecond}.evidence(testContext)
	if record := evaluateOverloadCohort(evidence); !record.OfferedShape.FinalMatchesTotals || record.OfferedShape.FinalPointOffset != 15*time.Second {
		testContext.Fatalf("final point = %+v", record.OfferedShape)
	}
	for name, mutate := range map[string]func(*overloadEvidence){
		"no-final-point": func(evidence *overloadEvidence) { evidence.series = evidence.series[:len(evidence.series)-1] },
		"final-point-short": func(evidence *overloadEvidence) {
			evidence.series[len(evidence.series)-1].Accepted--
		},
		"final-point-not-final": func(evidence *overloadEvidence) { evidence.series[len(evidence.series)-1].Final = false },
	} {
		evidence := syntheticCohort{capture: time.Millisecond}.evidence(testContext)
		mutate(&evidence)
		record := evaluateOverloadCohort(evidence)
		if record.Acceptance.Verdict != verdictInvalid || record.OfferedShape.FinalMatchesTotals || !strings.Contains(record.OfferedShape.Reason, "does not end, after the drain, at the cohort totals") {
			testContext.Errorf("%s: shape %+v verdict %s", name, record.OfferedShape, record.Acceptance.Verdict)
		}
	}
}

// The live sender takes the final point only after every worker finished; a
// periodic sample that races it is dropped rather than appended after it.
func TestOverloadFinalSeriesPointCarriesDrainCompletions(testContext *testing.T) {
	counters, _ := overloadTestCounters(testContext)
	for index := uint64(0); index < 4; index++ {
		counters.offerOverload(index)
		counters.reserveOverload(index)
	}
	counters.mutex.Lock()
	counters.sampleOverloadLocked(time.Second)
	counters.mutex.Unlock()
	// Completions during the drain, after the last periodic sample.
	for index := uint64(0); index < 4; index++ {
		counters.completeOverload(index, overloadOutcome(index%2), causeWriteDeadline, nil, 0, 0, true)
	}
	counters.finishOverloadSeries(1500 * time.Millisecond)
	counters.mutex.Lock()
	counters.sampleOverloadLocked(2 * time.Second)
	counters.mutex.Unlock()
	counters.finishOverloadSeries(3 * time.Second)
	series := counters.overload.series
	if len(series) != 2 || !series[1].Final || series[1].OffsetMillis != 1500 || series[1].overloadClassCounts != counters.overload.totals() ||
		series[0].Accepted != 0 || series[1].Accepted != 2 || series[1].NotSent.WriteDeadline != 2 {
		testContext.Fatalf("series = %+v", series)
	}
}

// Without a shared clock the per-second sampler's tick time can be well
// before it gets the counters mutex, while the scheduler keeps emitting; the
// overload series must be stamped with the instant its counts were read, or a
// healthy run appears to have offered messages ahead of the schedule.
func TestOverloadSeriesIsStampedWhenItsCountsAreRead(testContext *testing.T) {
	schedule, err := newPhasedSchedule([]overloadPhase{{Rate: 1_000, Duration: 5 * time.Second}, {Rate: 100, Duration: 12 * time.Second}})
	if err != nil {
		testContext.Fatal(err)
	}
	counters := newSenderCounters(8)
	counters.overload = newOverloadCounters(schedule)
	started := time.Now()
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		sampleSharedSender(started, counters, done, nil)
	}()
	// Hold the mutex across the one-second tick, emitting on schedule, as a
	// busy scheduler does.
	time.Sleep(time.Until(started.Add(900 * time.Millisecond)))
	counters.mutex.Lock()
	time.Sleep(time.Until(started.Add(1300 * time.Millisecond)))
	counters.overload.phases[0].Offered = schedule.due(time.Since(started))
	counters.mutex.Unlock()
	time.Sleep(200 * time.Millisecond)
	close(done)
	<-finished
	counters.mutex.Lock()
	series := append([]overloadSenderPoint(nil), counters.overload.series...)
	counters.mutex.Unlock()
	if len(series) != 1 {
		testContext.Fatalf("series = %+v", series)
	}
	offset := time.Duration(series[0].OffsetMillis) * time.Millisecond
	if upper := schedule.due(offset + time.Millisecond - 1); series[0].Offered > upper || offset < 1300*time.Millisecond {
		testContext.Fatalf("sample at %s carries %d offered, %d due", offset, series[0].Offered, upper)
	}
}
