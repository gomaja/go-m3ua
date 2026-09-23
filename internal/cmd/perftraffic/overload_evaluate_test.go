package main

import (
	"strings"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/internal/perfstats"
)

// recoverySeries builds per-second progress points for a 2,000/s overload
// phase of 10 s followed by a 500/s recovery phase of 12 s (nominal 1,000/s).
// admitted and delivered give the per-second increments of accepted and
// validated messages; point k is observed 2 ms after second k.
func recoverySeries(admitted, delivered func(second int) uint64) (*phasedSchedule, []overloadPoint) {
	schedule, err := newPhasedSchedule([]overloadPhase{{Rate: 2_000, Duration: 10 * time.Second}, {Rate: 500, Duration: 12 * time.Second}})
	if err != nil {
		panic(err)
	}
	var accepted, unique uint64
	points := []overloadPoint{{Before: -3 * time.Millisecond, After: -time.Millisecond}}
	for second := 1; second <= 22; second++ {
		accepted += admitted(second)
		unique += delivered(second)
		at := time.Duration(second) * time.Second
		if second == 22 {
			at = 22*time.Second - 12*time.Millisecond
		}
		points = append(points, overloadPoint{Before: at, After: at + 2*time.Millisecond, Unique: unique, AcceptedLower: accepted, AdmittedUpper: accepted})
	}
	return schedule, points
}

func overloadAdmitted(second int) uint64 {
	if second <= 10 {
		return 1_700
	}
	return 500
}

func TestOverloadRecoveryIsFoundInTheFirstWindowThatDeliversTheOfferedRate(testContext *testing.T) {
	schedule, points := recoverySeries(overloadAdmitted, func(second int) uint64 {
		switch {
		case second <= 10:
			return 1_500
		case second == 11:
			return 2_000
		default:
			return 500
		}
	})
	recoveries := evaluateRecoveries(schedule, 1_000, points, make([]uint64, 22))
	if len(recoveries) != 1 {
		testContext.Fatalf("recoveries = %+v", recoveries)
	}
	recovery := recoveries[0]
	if recovery.Status != overloadRecovered || recovery.Switch != 10*time.Second || recovery.RecoveryTime != 2*time.Millisecond ||
		recovery.Delivered != 2_000 || recovery.NonGrowthBy != "trend" || recovery.BacklogAtWindowStart != 2_000 || recovery.PeakBacklogAfter != 500 || recovery.Rate != 500 {
		testContext.Fatalf("recovery = %+v", recovery)
	}
	// 500/s over at most 1.002 s, less 1% and the 5-message floor.
	if want := 500*1.002*0.99 - 5; recovery.Required < want-1e-9 || recovery.Required > want+1e-9 {
		testContext.Fatalf("required = %v, want %v", recovery.Required, want)
	}
}

// A backlog that drains over several observations leaves the trend rule
// undecided even though every observation is lower than the last; the
// envelope shows the non-growth instead.
func TestOverloadRecoveryAcceptsAMultiSecondDrainByItsEnvelope(testContext *testing.T) {
	drain := map[int]uint64{11: 1_400, 12: 900, 13: 700}
	schedule, points := recoverySeries(overloadAdmitted, func(second int) uint64 {
		if second <= 10 {
			return 1_500
		}
		if delivered, draining := drain[second]; draining {
			return delivered
		}
		return 500
	})
	recovery := evaluateRecoveries(schedule, 1_000, points, make([]uint64, 22))[0]
	if recovery.Status != overloadRecovered || recovery.NonGrowthBy != "envelope" || recovery.Trend.Status == string(perfstats.BacklogNotGrowing) ||
		recovery.BacklogAtWindowStart != 2_000 || recovery.PeakBacklogAfter != 1_100 || recovery.RecoveryTime != 2*time.Millisecond {
		testContext.Fatalf("recovery = %+v", recovery)
	}
}

// A backlog that drains and then climbs back above where it started has not
// recovered, whatever the trend rule makes of the shape.
func TestOverloadRecoveryRejectsABacklogThatClimbsBack(testContext *testing.T) {
	schedule, points := recoverySeries(overloadAdmitted, func(second int) uint64 {
		switch {
		case second <= 10:
			return 1_500
		case second <= 12:
			return 1_200
		default:
			return 250
		}
	})
	recovery := evaluateRecoveries(schedule, 1_000, points, make([]uint64, 22))[0]
	if recovery.Status == overloadRecovered || recovery.PeakBacklogAfter <= recovery.BacklogAtWindowStart {
		testContext.Fatalf("recovery = %+v", recovery)
	}
}

func TestOverloadRecoveryFailsWhenDeliveriesLagPastTheAllowance(testContext *testing.T) {
	schedule, points := recoverySeries(overloadAdmitted, func(second int) uint64 {
		switch {
		case second <= 10:
			return 1_500
		case second <= 13:
			return 400
		default:
			return 900
		}
	})
	recovery := evaluateRecoveries(schedule, 1_000, points, make([]uint64, 22))[0]
	if recovery.Status != overloadNotRecovered || !strings.Contains(recovery.Reason, "no window starting within 2s") {
		testContext.Fatalf("recovery = %+v", recovery)
	}
}

// Deliveries that only just miss the threshold are not a recovery: the
// tolerance is exactly 1% plus the floor, not a fudge.
func TestOverloadRecoveryThresholdIsExact(testContext *testing.T) {
	required := requiredRecoveryDeliveries(500, 1002*time.Millisecond)
	for _, scenario := range []struct {
		delivered uint64
		status    string
	}{
		{delivered: uint64(required), status: overloadNotRecovered},
		{delivered: uint64(required) + 1, status: overloadRecovered},
	} {
		schedule, points := recoverySeries(overloadAdmitted, func(second int) uint64 {
			switch {
			case second <= 10:
				return 1_500
			case second <= 13:
				return scenario.delivered
			default:
				return 500 + 200
			}
		})
		recovery := evaluateRecoveries(schedule, 1_000, points, make([]uint64, 22))[0]
		if recovery.Status != scenario.status {
			testContext.Errorf("delivered %d against %.3f: %+v", scenario.delivered, required, recovery)
		}
	}
}

func TestOverloadRecoveryFailsWhenTheAdmittedBacklogKeepsGrowing(testContext *testing.T) {
	schedule, points := recoverySeries(overloadAdmitted, func(second int) uint64 {
		switch {
		case second <= 10:
			return 1_500
		case second == 11:
			return 600
		default:
			return 300
		}
	})
	recovery := evaluateRecoveries(schedule, 1_000, points, make([]uint64, 22))[0]
	if recovery.Status != overloadNotRecovered || recovery.Trend.Status != string(perfstats.BacklogGrowing) || recovery.RecoveryTime != 2*time.Millisecond {
		testContext.Fatalf("recovery = %+v", recovery)
	}
}

// A backlog that drains and then grows again stays below where it started
// for the rest of this phase, but a growing trend is never excused by the
// envelope.
func TestOverloadRecoveryNeverExcusesAGrowingTrend(testContext *testing.T) {
	schedule, points := recoverySeries(overloadAdmitted, func(second int) uint64 {
		switch {
		case second <= 10:
			return 1_500
		case second == 11:
			return 2_500
		default:
			return 400
		}
	})
	recovery := evaluateRecoveries(schedule, 1_000, points, make([]uint64, 22))[0]
	if recovery.Status != overloadNotRecovered || recovery.Trend.Status != string(perfstats.BacklogGrowing) ||
		float64(recovery.PeakBacklogAfter) > float64(recovery.BacklogAtWindowStart)+recovery.Floor {
		testContext.Fatalf("recovery = %+v", recovery)
	}
}

func TestOverloadRecoveryWithoutEvidenceIsUnavailable(testContext *testing.T) {
	schedule, points := recoverySeries(overloadAdmitted, func(second int) uint64 { return min(1_500, overloadAdmitted(second)) })
	short := evaluateRecoveries(schedule, 1_000, points[:15], make([]uint64, 22))[0]
	if short.Status != overloadCriterionUnavailable || short.Trend.Status != perfstats.BacklogTrendInsufficientSamples {
		testContext.Fatalf("short series = %+v", short)
	}
	none := evaluateRecoveries(schedule, 1_000, points[:10], make([]uint64, 22))[0]
	if none.Status != overloadCriterionUnavailable || !strings.Contains(none.Reason, "no one-second progress window") {
		testContext.Fatalf("no windows = %+v", none)
	}
}

func TestOverloadRecoveryReportsRefusalsAfterTheWindow(testContext *testing.T) {
	schedule, points := recoverySeries(overloadAdmitted, func(second int) uint64 {
		return min(1_500, overloadAdmitted(second)) + 100*uint64(min(max(second-10, 0), 1))
	})
	notAdmitted := make([]uint64, 22)
	notAdmitted[9], notAdmitted[10], notAdmitted[11], notAdmitted[12] = 300, 40, 7, 2
	recovery := evaluateRecoveries(schedule, 1_000, points, notAdmitted)[0]
	// The window ends 11.002 s in, so only whole scheduled seconds from 12 s
	// on lie after it.
	if recovery.NotAdmittedAfterWindow != 2 || recovery.LastNotAdmittedSecond != 3*time.Second {
		testContext.Fatalf("recovery = %+v", recovery)
	}
}

func TestOverloadPointsBoundTheAdmittedBacklog(testContext *testing.T) {
	point := overloadPoint{Unique: 100, DiscardedLower: 5, DiscardedUpper: 9, AcceptedLower: 130, AdmittedUpper: 150}
	if lower, upper := point.backlog(); lower != 21 || upper != 45 {
		testContext.Fatalf("backlog = [%d, %d]", lower, upper)
	}
	point = overloadPoint{Unique: 100, AcceptedLower: 90, AdmittedUpper: 95}
	if lower, upper := point.backlog(); lower != 0 || upper != 0 {
		testContext.Fatalf("saturated backlog = [%d, %d]", lower, upper)
	}
	specification := runSpec{Duration: time.Second}
	mark := func(accepted, started, refused uint64) *overloadSenderMark {
		return &overloadSenderMark{Accepted: accepted, Started: started, Refused: refused}
	}
	observation := func(before, after time.Duration, unique, discardedBefore, discardedAfter uint64, first, second *overloadSenderMark) progressObservation {
		return progressObservation{Before: before, After: after, SenderBefore: first, SenderAfter: second, Snapshot: &receiverProgress{
			Delivery: ledgerSnapshot{Unique: unique},
			Overload: &receiverOverloadProgress{DiscardedBefore: discardedBefore, DiscardedAfter: discardedAfter},
		}}
	}
	points, err := overloadPoints(specification, []progressObservation{
		observation(-2, 0, 0, 0, 0, mark(0, 0, 0), mark(0, 0, 0)),
		{Before: 1, After: 2, Error: "lost"},
		observation(10, 20, 50, 1, 2, mark(60, 80, 5), mark(70, 90, 6)),
	})
	if err != nil || len(points) != 2 || points[1].AcceptedLower != 60 || points[1].AdmittedUpper != 85 || points[1].DiscardedUpper != 2 {
		testContext.Fatalf("points = %+v, %v", points, err)
	}
	for name, observations := range map[string][]progressObservation{
		"reversed-request": {observation(20, 10, 0, 0, 0, mark(0, 0, 0), mark(0, 0, 0))},
		"reversed-marks":   {observation(10, 20, 0, 0, 0, mark(5, 5, 0), mark(4, 5, 0))},
		"reversed-discard": {observation(10, 20, 0, 3, 2, mark(0, 0, 0), mark(0, 0, 0))},
		"regressed-unique": {observation(10, 20, 9, 0, 0, mark(9, 9, 0), mark(9, 9, 0)), observation(30, 40, 8, 0, 0, mark(9, 9, 0), mark(9, 9, 0))},
		"overlapping":      {observation(10, 20, 0, 0, 0, mark(0, 0, 0), mark(0, 0, 0)), observation(15, 40, 0, 0, 0, mark(0, 0, 0), mark(0, 0, 0))},
	} {
		if _, err := overloadPoints(specification, observations); err == nil {
			testContext.Errorf("%s: accepted", name)
		}
	}
}

func overloadLedgers(expected uint64, accepted, indeterminate, delivered []uint64) (bitmap, bitmap, bitmap) {
	build := func(members []uint64) bitmap {
		set := newBitmap(expected)
		for _, member := range members {
			set.add(member)
		}
		return set
	}
	return build(accepted), build(indeterminate), build(delivered)
}

func overloadReceiver(unique, discarded uint64) runRecord {
	return runRecord{Side: "receiver", Delivery: deliveryResult{Unique: unique}, Overload: &overloadRecord{Receiver: &overloadReceiverRecord{Discarded: discarded}}}
}

func TestOverloadReconciliationAccountsForEveryOfferedMessage(testContext *testing.T) {
	totals := overloadClassCounts{Offered: 10, Accepted: 6, NotSent: overloadNotSentCounts{WriteDeadline: 1}, CapRefusedOutstanding: 2, DeadlineExpired: 1}
	accepted, indeterminate, delivered := overloadLedgers(10, []uint64{0, 1, 2, 3, 4, 5}, nil, []uint64{0, 1, 2, 4})
	result := reconcileOverload(totals, 10, accepted, indeterminate, delivered, overloadReceiver(4, 2))
	if failures := result.failures(totals); len(failures) != 0 || result.AcceptedUndelivered != 2 || result.UnexplainedMissingLower != 0 || result.Phantom != 0 {
		testContext.Fatalf("balanced run: %+v %v", result, failures)
	}
	for _, scenario := range []struct {
		name          string
		totals        overloadClassCounts
		accepted      []uint64
		indeterminate []uint64
		delivered     []uint64
		receiver      runRecord
		want          string
	}{
		{name: "unexplained-loss", totals: totals, accepted: []uint64{0, 1, 2, 3, 4, 5}, delivered: []uint64{0, 1, 2}, receiver: overloadReceiver(3, 2), want: "missing beyond"},
		{name: "phantom-delivery", totals: totals, accepted: []uint64{0, 1, 2, 3, 4, 5}, delivered: []uint64{0, 1, 2, 3, 4, 5, 9}, receiver: overloadReceiver(7, 0), want: "never accepted"},
		{name: "discard-excess", totals: totals, accepted: []uint64{0, 1, 2, 3, 4, 5}, delivered: []uint64{0, 1, 2, 3, 4, 5}, receiver: overloadReceiver(6, 1), want: "exceed"},
		{name: "ledger-mismatch", totals: totals, accepted: []uint64{0, 1, 2, 3, 4}, delivered: []uint64{0, 1, 2, 3, 4}, receiver: overloadReceiver(5, 0), want: "sender ledgers"},
		{name: "unique-mismatch", totals: totals, accepted: []uint64{0, 1, 2, 3, 4, 5}, delivered: []uint64{0, 1, 2, 3, 4, 5}, receiver: overloadReceiver(5, 0), want: "delivered ledger"},
		{name: "unclassified", totals: overloadClassCounts{Offered: 10, Accepted: 6, Unclassified: 4}, accepted: []uint64{0, 1, 2, 3, 4, 5}, delivered: []uint64{0, 1, 2, 3, 4, 5}, receiver: overloadReceiver(6, 0), want: "DataWriteError contract"},
		{name: "unresolved", totals: overloadClassCounts{Offered: 10, Accepted: 6}, accepted: []uint64{0, 1, 2, 3, 4, 5}, delivered: []uint64{0, 1, 2, 3, 4, 5}, receiver: overloadReceiver(6, 0), want: "reached no class"},
		{name: "short-offer", totals: overloadClassCounts{Offered: 6, Accepted: 6}, accepted: []uint64{0, 1, 2, 3, 4, 5}, delivered: []uint64{0, 1, 2, 3, 4, 5}, receiver: overloadReceiver(6, 0), want: "offered 6 of 10"},
		{name: "both-ledgers", totals: overloadClassCounts{Offered: 10, Accepted: 6, Indeterminate: 1, CapRefusedQueue: 3}, accepted: []uint64{0, 1, 2, 3, 4, 5}, indeterminate: []uint64{5}, delivered: []uint64{0, 1, 2, 3, 4, 5}, receiver: overloadReceiver(6, 0), want: "sender ledgers"},
		{name: "duplicate", totals: totals, accepted: []uint64{0, 1, 2, 3, 4, 5}, delivered: []uint64{0, 1, 2, 3, 4, 5}, receiver: func() runRecord {
			record := overloadReceiver(6, 0)
			record.Delivery.Duplicate = 1
			return record
		}(), want: "never resends"},
		{name: "misscoped", totals: totals, accepted: []uint64{0, 1, 2, 3, 4, 5}, delivered: []uint64{0, 1, 2, 3, 4, 5}, receiver: func() runRecord {
			record := overloadReceiver(6, 0)
			record.Delivery.Invalid = 1
			record.Overload.Receiver.Misscoped = 1
			return record
		}(), want: "1 of them mis-scoped"},
	} {
		accepted, indeterminate, delivered := overloadLedgers(10, scenario.accepted, scenario.indeterminate, scenario.delivered)
		result := reconcileOverload(scenario.totals, 10, accepted, indeterminate, delivered, scenario.receiver)
		if failures := strings.Join(result.failures(scenario.totals), "; "); !strings.Contains(failures, scenario.want) {
			testContext.Errorf("%s: failures %q, want %q (%+v)", scenario.name, failures, scenario.want, result)
		}
	}
}

func TestOverloadReconciliationBoundsIndeterminateOutcomes(testContext *testing.T) {
	totals := overloadClassCounts{Offered: 8, Accepted: 5, Indeterminate: 3}
	accepted, indeterminate, delivered := overloadLedgers(8, []uint64{0, 1, 2, 3, 4}, []uint64{5, 6, 7}, []uint64{0, 1, 2, 3, 5})
	result := reconcileOverload(totals, 8, accepted, indeterminate, delivered, overloadReceiver(5, 2))
	if result.IndeterminateDelivered != 1 || result.IndeterminateUndelivered != 2 || result.AcceptedUndelivered != 1 ||
		result.UnexplainedMissingLower != 0 || result.UnexplainedMissingUpper != 1 || result.DiscardExcess != 0 || len(result.failures(totals)) != 0 {
		testContext.Fatalf("indeterminate bounds: %+v %v", result, result.failures(totals))
	}
	if missing := reconcileOverload(totals, 8, accepted, indeterminate, nil, overloadReceiver(5, 2)); !strings.Contains(missing.Error, "ledger is unavailable") {
		testContext.Fatalf("missing ledger: %+v", missing)
	}
}

func passingOverloadRecord() *overloadRecord {
	totals := overloadClassCounts{Offered: 4, Accepted: 4}
	observation := overloadAssociationObservation{QueueCapacity: dataQueueSize, MaxQueued: 10, State: "ASP-ACTIVE", Active: true, EpochStart: 1, EpochEnd: 1}
	return &overloadRecord{
		Totals:         &totals,
		Reconciliation: &overloadReconciliation{Messages: 4, Offered: 4, Classified: 4, AcceptedLedger: 4, DeliveredLedger: 4, ReceiverUnique: 4},
		Bounds: &overloadBounds{
			OutstandingLimit: 8, MaxOutstanding: 8, FixtureQueueCapacity: []int{8}, FixtureQueueMax: []int{8},
			ConfiguredQueueCapacity: dataQueueSize, ReceiverAssociations: []overloadAssociationObservation{observation},
			SenderAssociations: []overloadAssociationObservation{observation},
		},
		Recovery: []overloadRecovery{{Status: overloadRecovered, Trend: backlogTrend{Status: string(perfstats.BacklogNotGrowing)}}},
	}
}

func TestOverloadAcceptanceCombinesItsCriteria(testContext *testing.T) {
	if acceptance := decideOverload(nil, passingOverloadRecord(), 1); acceptance.Verdict != verdictPass || len(acceptance.Criteria) != 4 {
		testContext.Fatalf("passing record: %+v", acceptance)
	}
	for _, scenario := range []struct {
		name     string
		fixture  []string
		mutate   func(*overloadRecord)
		verdict  string
		criteria string
	}{
		{name: "fixture", fixture: []string{"clock"}, mutate: func(*overloadRecord) {}, verdict: verdictInvalid, criteria: "fixture"},
		{name: "outstanding", mutate: func(record *overloadRecord) { record.Bounds.MaxOutstanding = 9 }, verdict: overloadVerdictFail, criteria: "bounds"},
		{name: "library-queue", mutate: func(record *overloadRecord) { record.Bounds.ReceiverAssociations[0].MaxQueued = dataQueueSize + 1 }, verdict: overloadVerdictFail, criteria: "bounds"},
		{name: "queue-capacity", mutate: func(record *overloadRecord) { record.Bounds.ReceiverAssociations[0].QueueCapacity = 64 }, verdict: overloadVerdictFail, criteria: "bounds"},
		{name: "torn-down", mutate: func(record *overloadRecord) { record.Bounds.SenderAssociations[0].Active = false }, verdict: overloadVerdictFail, criteria: "bounds"},
		{name: "restarted", mutate: func(record *overloadRecord) { record.Bounds.ReceiverAssociations[0].EpochEnd = 2 }, verdict: overloadVerdictFail, criteria: "bounds"},
		{name: "missing-association", mutate: func(record *overloadRecord) { record.Bounds.ReceiverAssociations = nil }, verdict: overloadVerdictFail, criteria: "bounds"},
		{name: "not-established", mutate: func(record *overloadRecord) { record.Bounds.NotEstablishedRefusals = 1 }, verdict: overloadVerdictFail, criteria: "bounds"},
		{name: "fixture-queue", mutate: func(record *overloadRecord) { record.Bounds.FixtureQueueMax[0] = 9 }, verdict: overloadVerdictFail, criteria: "bounds"},
		{name: "accounting", mutate: func(record *overloadRecord) { record.Reconciliation.Phantom = 1 }, verdict: overloadVerdictFail, criteria: "accounting"},
		{name: "ledger", mutate: func(record *overloadRecord) { record.Reconciliation.Error = "no ledger" }, verdict: verdictInconclusive, criteria: "accounting"},
		{name: "not-recovered", mutate: func(record *overloadRecord) { record.Recovery[0].Status = overloadNotRecovered }, verdict: overloadVerdictFail, criteria: "recovery"},
		{name: "recovery-unavailable", mutate: func(record *overloadRecord) { record.Recovery[0].Status = overloadCriterionUnavailable }, verdict: verdictInconclusive, criteria: "recovery"},
		{name: "no-recovery", mutate: func(record *overloadRecord) { record.Recovery = nil }, verdict: verdictInconclusive, criteria: "recovery"},
		{name: "fail-beats-unavailable", mutate: func(record *overloadRecord) {
			record.Recovery[0].Status = overloadCriterionUnavailable
			record.Bounds.MaxOutstanding = 9
		}, verdict: overloadVerdictFail, criteria: "bounds"},
	} {
		record := passingOverloadRecord()
		scenario.mutate(record)
		acceptance := decideOverload(scenario.fixture, record, 1)
		failing := ""
		for _, criterion := range acceptance.Criteria {
			if criterion.Status != overloadCriterionPass {
				failing = criterion.Name
				break
			}
		}
		if acceptance.Verdict != scenario.verdict || failing != scenario.criteria {
			testContext.Errorf("%s: verdict %s criterion %s: %+v", scenario.name, acceptance.Verdict, failing, acceptance.Criteria)
		}
	}
}

func TestOverloadRecordVerdictFollowsTheAcceptance(testContext *testing.T) {
	measurement := runSpec{Mode: modeThroughput, Overload: &overloadSpec{Role: overloadRoleMeasurement}}
	for _, scenario := range []struct {
		acceptance string
		verdict    string
		fixture    string
	}{
		{acceptance: verdictPass, verdict: verdictPass, fixture: verdictPass},
		{acceptance: overloadVerdictFail, verdict: verdictInvalid, fixture: verdictPass},
		{acceptance: verdictInconclusive, verdict: verdictInconclusive, fixture: verdictPass},
		{acceptance: verdictInvalid, verdict: verdictInvalid, fixture: verdictInvalid},
	} {
		record := runRecord{Side: "sender", Spec: measurement, SendErrors: 10, Capped: 10, Delivery: deliveryResult{Missing: 5},
			Overload: &overloadRecord{Acceptance: &overloadAcceptance{Verdict: scenario.acceptance}}}
		record.evaluate()
		if record.Verdict != scenario.verdict || record.FixtureVerdict != scenario.fixture || record.CapacityVerdict != "unavailable" {
			testContext.Errorf("acceptance %s: verdict %s fixture %s capacity %s", scenario.acceptance, record.Verdict, record.FixtureVerdict, record.CapacityVerdict)
		}
	}
	missing := runRecord{Side: "sender", Spec: measurement}
	missing.evaluate()
	if missing.Verdict != verdictInvalid {
		testContext.Fatalf("unevaluated overload sender record: %+v", missing)
	}
	fatal := runRecord{Side: "sender", Spec: measurement, FatalError: "boom", Overload: &overloadRecord{Acceptance: &overloadAcceptance{Verdict: verdictPass}}}
	fatal.evaluate()
	if fatal.Verdict != verdictInvalid {
		testContext.Fatalf("fatal overload sender record: %+v", fatal)
	}
	healthy := overloadAssociationObservation{QueueCapacity: dataQueueSize, MaxQueued: dataQueueSize, Active: true, EpochStart: 1, EpochEnd: 1}
	receiver := runRecord{Side: "receiver", Spec: measurement, Delivery: deliveryResult{Missing: 100},
		Overload: &overloadRecord{Receiver: &overloadReceiverRecord{Associations: []overloadAssociationObservation{healthy}}}}
	receiver.evaluate()
	if receiver.Verdict != verdictPass || receiver.FixtureVerdict != verdictPass {
		testContext.Fatalf("healthy overload receiver: %+v", receiver)
	}
	for name, mutate := range map[string]func(*runRecord){
		"duplicate":  func(record *runRecord) { record.Delivery.Duplicate = 1 },
		"invalid":    func(record *runRecord) { record.Delivery.Invalid = 1 },
		"reordered":  func(record *runRecord) { record.Delivery.Reordered = 1 },
		"late":       func(record *runRecord) { record.Delivery.LateAfterStop = 1 },
		"inactive":   func(record *runRecord) { record.Overload.Receiver.Associations[0].Active = false },
		"restarted":  func(record *runRecord) { record.Overload.Receiver.Associations[0].EpochEnd = 2 },
		"overflowed": func(record *runRecord) { record.Overload.Receiver.Associations[0].MaxQueued = dataQueueSize + 1 },
		"missing":    func(record *runRecord) { record.Overload = nil },
		"fatal":      func(record *runRecord) { record.FatalError = "read failed" },
	} {
		record := runRecord{Side: "receiver", Spec: measurement,
			Overload: &overloadRecord{Receiver: &overloadReceiverRecord{Associations: []overloadAssociationObservation{healthy}}}}
		mutate(&record)
		record.evaluate()
		if record.Verdict != verdictInvalid {
			testContext.Errorf("%s: receiver verdict %s", name, record.Verdict)
		}
	}
}
