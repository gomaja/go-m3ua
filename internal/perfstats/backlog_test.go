package perfstats

import (
	"math"
	"testing"
	"time"
)

func interval(lower, upper float64) *BacklogInterval {
	return &BacklogInterval{Lower: lower, Upper: upper}
}

// unstalled is stall evidence from a run whose longest send call is in the
// range the fixture actually measures: hundreds of microseconds, three orders
// of magnitude below the threshold.
func unstalled() *StallObservation {
	return &StallObservation{LongestSend: 262144 * time.Nanosecond}
}

func stalled() *StallObservation {
	return &StallObservation{LongestSend: 1200 * time.Millisecond}
}

func TestBacklogIntervalVerdictBoundaries(testContext *testing.T) {
	aboveZero := math.Nextafter(BacklogThreshold, math.Inf(1))
	belowZero := math.Nextafter(BacklogThreshold, math.Inf(-1))
	tests := []struct {
		name  string
		lower float64
		upper float64
		want  BacklogVerdict
	}{
		{name: "both zero", lower: 0, upper: 0, want: BacklogNotGrowing},
		{name: "upper exactly zero", lower: -3.4, upper: 0, want: BacklogNotGrowing},
		{name: "negative zero upper", lower: -1, upper: math.Copysign(0, -1), want: BacklogNotGrowing},
		{name: "entirely negative", lower: -10, upper: -0.5, want: BacklogNotGrowing},
		{name: "upper one float above zero", lower: -1, upper: math.SmallestNonzeroFloat64, want: BacklogIndeterminate},
		{name: "upper one float below the zero", lower: -5, upper: belowZero, want: BacklogNotGrowing},
		{name: "upper exactly at the zero", lower: -5, upper: BacklogThreshold, want: BacklogNotGrowing},
		{name: "upper one float above the zero", lower: -5, upper: aboveZero, want: BacklogIndeterminate},
		{name: "collapsed on the zero", lower: BacklogThreshold, upper: BacklogThreshold, want: BacklogNotGrowing},
		{name: "lower exactly at the zero", lower: BacklogThreshold, upper: aboveZero, want: BacklogIndeterminate},
		{name: "lower one float above the zero", lower: aboveZero, upper: aboveZero, want: BacklogGrowing},
		{name: "lower one float above the zero with a wide upper", lower: aboveZero, upper: 4, want: BacklogGrowing},
		{name: "straddles zero", lower: -3.4, upper: 3.53, want: BacklogIndeterminate},
		{name: "touches zero from below only", lower: 0, upper: 2, want: BacklogIndeterminate},
		{name: "lower one float above zero", lower: math.SmallestNonzeroFloat64, upper: 1, want: BacklogGrowing},
		{name: "fractional positive interval", lower: 0.25, upper: 0.75, want: BacklogGrowing},
		{name: "positive interval with wide bounds", lower: 0.25, upper: 4, want: BacklogGrowing},
		{name: "both positive equal above the zero", lower: 2, upper: 2, want: BacklogGrowing},
		{name: "lower NaN", lower: math.NaN(), upper: -1, want: BacklogIndeterminate},
		{name: "upper NaN", lower: -1, upper: math.NaN(), want: BacklogIndeterminate},
		{name: "lower negative infinity", lower: math.Inf(-1), upper: 0, want: BacklogIndeterminate},
		{name: "upper positive infinity", lower: -1, upper: math.Inf(1), want: BacklogIndeterminate},
		{name: "positive lower with infinite upper", lower: 1, upper: math.Inf(1), want: BacklogIndeterminate},
		{name: "negative infinite lower with positive upper", lower: math.Inf(-1), upper: 1, want: BacklogIndeterminate},
		{name: "reversed", lower: 1, upper: -1, want: BacklogIndeterminate},
	}
	for _, test := range tests {
		testContext.Run(test.name, func(testContext *testing.T) {
			got := (BacklogInterval{Lower: test.lower, Upper: test.upper}).Verdict()
			if got != test.want {
				testContext.Fatalf("Verdict() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestDecideRunPassRequiresNotGrowingLossFreeValidFixture(testContext *testing.T) {
	decision := DecideRun(RunEvidence{FixtureValid: true, Interval: interval(-3.4, 0), Stall: unstalled()})
	if decision.Decision != Pass || decision.Backlog != BacklogNotGrowing || decision.Reason != "" {
		testContext.Fatalf("DecideRun() = %+v, want clean pass", decision)
	}
}

func TestDecideRunNeverPassesWithStraddlingOrMissingEvidence(testContext *testing.T) {
	tests := []struct {
		name     string
		evidence RunEvidence
		reason   string
	}{
		{name: "straddling", evidence: RunEvidence{FixtureValid: true, Interval: interval(-3.4, 3.53), Stall: unstalled()}, reason: BacklogUnresolvedReason},
		{name: "missing interval", evidence: RunEvidence{FixtureValid: true, Stall: unstalled()}, reason: BacklogEvidenceMissingReason},
		{name: "NaN interval", evidence: RunEvidence{FixtureValid: true, Interval: interval(math.NaN(), 0), Stall: unstalled()}, reason: BacklogEvidenceMissingReason},
		{name: "reversed interval", evidence: RunEvidence{FixtureValid: true, Interval: interval(1, -1), Stall: unstalled()}, reason: BacklogEvidenceMissingReason},
	}
	for _, test := range tests {
		testContext.Run(test.name, func(testContext *testing.T) {
			decision := DecideRun(test.evidence)
			if decision.Decision != Inconclusive {
				testContext.Fatalf("DecideRun() = %+v, want inconclusive", decision)
			}
			if decision.Reason != test.reason {
				testContext.Fatalf("Reason = %q, want %q", decision.Reason, test.reason)
			}
		})
	}
}

func TestDecideRunFailsOnGrowingBacklog(testContext *testing.T) {
	decision := DecideRun(RunEvidence{FixtureValid: true, Interval: interval(1.5, 3), Stall: unstalled()})
	if decision.Decision != Fail || decision.Backlog != BacklogGrowing || decision.Reason != BacklogGrowingReason {
		testContext.Fatalf("DecideRun() = %+v, want growing failure", decision)
	}
}

func TestDecideRunFailsOnInvalidFixtureEvenWithCleanInterval(testContext *testing.T) {
	decision := DecideRun(RunEvidence{FixtureValid: false, Interval: interval(-1, 0)})
	if decision.Decision != Fail || decision.Reason != FixtureInvalidReason {
		testContext.Fatalf("DecideRun() = %+v, want fixture-invalid failure", decision)
	}
}

func TestDecideRunFailsOnAnyCountedFailure(testContext *testing.T) {
	counters := []func(*RunCounters){
		func(c *RunCounters) { c.Missing = 1 },
		func(c *RunCounters) { c.Duplicate = 1 },
		func(c *RunCounters) { c.Invalid = 1 },
		func(c *RunCounters) { c.Reordered = 1 },
		func(c *RunCounters) { c.LateAfterStop = 1 },
		func(c *RunCounters) { c.Capped = 1 },
		func(c *RunCounters) { c.SendErrors = 1 },
		func(c *RunCounters) { c.DeadlineExceeded = 1 },
	}
	for index, mutate := range counters {
		var failing RunCounters
		mutate(&failing)
		decision := DecideRun(RunEvidence{FixtureValid: true, Interval: interval(-1, 0), Counters: failing, Stall: unstalled()})
		if decision.Decision != Fail || decision.Reason != DeliveryFailuresReason {
			testContext.Fatalf("counter %d: DecideRun() = %+v, want delivery-failures failure", index, decision)
		}
	}
}

func FuzzBacklogIntervalNeverPassesOnInvalidEvidence(fuzzContext *testing.F) {
	fuzzContext.Add(-3.4, 3.53)
	fuzzContext.Add(0.0, 0.0)
	fuzzContext.Add(math.NaN(), 1.0)
	fuzzContext.Add(math.Inf(-1), math.Inf(1))
	fuzzContext.Add(-0.5, 0.5)
	fuzzContext.Add(BacklogThreshold, BacklogThreshold)
	fuzzContext.Add(BacklogThreshold, math.Nextafter(BacklogThreshold, math.Inf(1)))
	fuzzContext.Add(math.Nextafter(BacklogThreshold, math.Inf(1)), 4.0)
	fuzzContext.Fuzz(func(testContext *testing.T, lower, upper float64) {
		evidence := RunEvidence{FixtureValid: true, Interval: &BacklogInterval{Lower: lower, Upper: upper}, Stall: unstalled()}
		decision := DecideRun(evidence)
		switch decision.Backlog {
		case BacklogNotGrowing, BacklogGrowing, BacklogIndeterminate:
		default:
			testContext.Fatalf("unknown backlog verdict %q", decision.Backlog)
		}
		switch decision.Decision {
		case Pass:
			if !evidence.Interval.Valid() || evidence.Interval.Upper > BacklogThreshold {
				testContext.Fatalf("pass without a valid non-positive interval: %+v", evidence.Interval)
			}
		case Fail:
			if evidence.Interval.Valid() && evidence.Interval.Upper <= BacklogThreshold {
				testContext.Fatalf("loss-free valid run with non-positive interval failed: %+v", decision)
			}
		case Inconclusive:
		default:
			testContext.Fatalf("unknown decision %q", decision.Decision)
		}
	})
}

// Total is a sum of eight independent uint64 counters. Unsigned addition
// wraps silently in Go, so a saturating sum is the only way a lossy run
// cannot present itself as loss-free.
func TestRunCountersTotalSaturatesInsteadOfWrapping(testContext *testing.T) {
	tests := []struct {
		name     string
		counters RunCounters
	}{
		{name: "missing wraps with a duplicate", counters: RunCounters{Missing: math.MaxUint64, Duplicate: 1}},
		{name: "two counters at the limit", counters: RunCounters{Missing: math.MaxUint64, DeadlineExceeded: math.MaxUint64}},
		{name: "every counter at the limit", counters: RunCounters{
			Missing: math.MaxUint64, Duplicate: math.MaxUint64, Invalid: math.MaxUint64, Reordered: math.MaxUint64,
			LateAfterStop: math.MaxUint64, Capped: math.MaxUint64, SendErrors: math.MaxUint64, DeadlineExceeded: math.MaxUint64,
		}},
		{name: "last counter completes the wrap", counters: RunCounters{Missing: math.MaxUint64, DeadlineExceeded: 1}},
	}
	for _, test := range tests {
		testContext.Run(test.name, func(testContext *testing.T) {
			if total := test.counters.Total(); total != math.MaxUint64 {
				testContext.Fatalf("Total() = %d for %+v, want a saturated %d", total, test.counters, uint64(math.MaxUint64))
			}
		})
	}
}

// A wrapping Total lets DecideRun read a lossy run as loss-free and return
// Pass. The counters below are unreachable in a real fixture run, but the
// decision must not depend on that.
func TestDecideRunNeverPassesALossyRunWhoseCountersWrap(testContext *testing.T) {
	decision := DecideRun(RunEvidence{
		FixtureValid: true,
		Interval:     interval(-1, 0),
		Counters:     RunCounters{Missing: math.MaxUint64, Duplicate: 1},
		Stall:        unstalled(),
	})
	if decision.Decision != Fail || decision.Reason != DeliveryFailuresReason {
		testContext.Fatalf("DecideRun() = %+v, want Fail with %q", decision, DeliveryFailuresReason)
	}
}

func TestSteadyStateRunCanReachPass(testContext *testing.T) {
	decision := DecideRun(RunEvidence{FixtureValid: true, Interval: interval(0, 0), Stall: unstalled()})
	if decision.Decision != Pass || decision.Backlog != BacklogNotGrowing || decision.Reason != "" {
		testContext.Fatalf("DecideRun() = %+v, want a clean pass for a flat run", decision)
	}
}

// The rule has to be falsifiable in both directions: a backlog that genuinely
// grows must still be called growing, a genuinely flat one must be able to
// reach not-growing, and a flat one measured too noisily to tell the two apart
// must stay indeterminate.
func TestBacklogVerdictIsFalsifiableInBothDirections(testContext *testing.T) {
	tests := []struct {
		name  string
		lower float64
		upper float64
		want  BacklogVerdict
	}{
		{name: "positive growth", lower: 1.5, upper: 4, want: BacklogGrowing},
		{name: "exactly flat", lower: 0, upper: 0, want: BacklogNotGrowing},
		{name: "small uncertainty", lower: -0.5, upper: 0.5, want: BacklogIndeterminate},
		{name: "larger uncertainty", lower: -3.4, upper: 3.53, want: BacklogIndeterminate},
	}
	for _, test := range tests {
		testContext.Run(test.name, func(testContext *testing.T) {
			if got := (BacklogInterval{Lower: test.lower, Upper: test.upper}).Verdict(); got != test.want {
				testContext.Fatalf("Verdict() = %q, want %q", got, test.want)
			}
		})
	}
}
