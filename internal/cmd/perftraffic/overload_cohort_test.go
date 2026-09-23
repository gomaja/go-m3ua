package main

import (
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
		maxSchedulerLag: time.Millisecond, maxSchedulerLagAt: 3 * time.Second,
		fixtureQueueMax: []int{5}, senderAssociations: []overloadAssociationObservation{healthy},
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
		{name: "lag-at-tolerance", mutate: func(evidence *overloadEvidence) { evidence.maxSchedulerLag = overloadShapeTolerance }, valid: true},
		{name: "lag-beyond-tolerance", mutate: func(evidence *overloadEvidence) { evidence.maxSchedulerLag = overloadShapeTolerance + 1 }, reason: "the scheduler emitted"},
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
