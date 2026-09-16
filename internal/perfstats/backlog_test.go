package perfstats

import (
	"math"
	"testing"
)

func interval(lower, upper float64) *BacklogInterval {
	return &BacklogInterval{Lower: lower, Upper: upper}
}

func TestBacklogIntervalVerdictBoundaries(testContext *testing.T) {
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
		{name: "straddles zero", lower: -3.4, upper: 3.53, want: BacklogIndeterminate},
		{name: "touches zero from below only", lower: 0, upper: 2, want: BacklogIndeterminate},
		{name: "lower one float above zero", lower: math.SmallestNonzeroFloat64, upper: 1, want: BacklogGrowing},
		{name: "entirely positive", lower: 0.25, upper: 4, want: BacklogGrowing},
		{name: "both positive equal", lower: 2, upper: 2, want: BacklogGrowing},
		{name: "lower NaN", lower: math.NaN(), upper: -1, want: BacklogIndeterminate},
		{name: "upper NaN", lower: -1, upper: math.NaN(), want: BacklogIndeterminate},
		{name: "lower negative infinity", lower: math.Inf(-1), upper: 0, want: BacklogIndeterminate},
		{name: "upper positive infinity", lower: -1, upper: math.Inf(1), want: BacklogIndeterminate},
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
	decision := DecideRun(RunEvidence{FixtureValid: true, Interval: interval(-3.4, 0)})
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
		{name: "straddling", evidence: RunEvidence{FixtureValid: true, Interval: interval(-3.4, 3.53)}, reason: BacklogStraddlesReason},
		{name: "missing interval", evidence: RunEvidence{FixtureValid: true}, reason: BacklogEvidenceMissingReason},
		{name: "NaN interval", evidence: RunEvidence{FixtureValid: true, Interval: interval(math.NaN(), 0)}, reason: BacklogEvidenceMissingReason},
		{name: "reversed interval", evidence: RunEvidence{FixtureValid: true, Interval: interval(1, -1)}, reason: BacklogEvidenceMissingReason},
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
	decision := DecideRun(RunEvidence{FixtureValid: true, Interval: interval(0.5, 3)})
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
		decision := DecideRun(RunEvidence{FixtureValid: true, Interval: interval(-1, 0), Counters: failing})
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
	fuzzContext.Fuzz(func(testContext *testing.T, lower, upper float64) {
		evidence := RunEvidence{FixtureValid: true, Interval: &BacklogInterval{Lower: lower, Upper: upper}}
		decision := DecideRun(evidence)
		switch decision.Backlog {
		case BacklogNotGrowing, BacklogGrowing, BacklogIndeterminate:
		default:
			testContext.Fatalf("unknown backlog verdict %q", decision.Backlog)
		}
		switch decision.Decision {
		case Pass:
			if !evidence.Interval.Valid() || evidence.Interval.Upper > 0 {
				testContext.Fatalf("pass without a valid non-positive interval: %+v", evidence.Interval)
			}
		case Fail:
			if evidence.Interval.Valid() && evidence.Interval.Upper <= 0 {
				testContext.Fatalf("loss-free valid run with non-positive interval failed: %+v", decision)
			}
		case Inconclusive:
		default:
			testContext.Fatalf("unknown decision %q", decision.Decision)
		}
	})
}
