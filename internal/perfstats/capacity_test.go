package perfstats

import (
	"math"
	"strings"
	"testing"
)

func driveSearch(testContext *testing.T, initial, maximum, maxProbes int, passAbove int) *CapacitySearch {
	testContext.Helper()
	search, err := NewCapacitySearch(initial, maximum, maxProbes)
	if err != nil {
		testContext.Fatalf("NewCapacitySearch: %v", err)
	}
	for {
		rate, ok := search.NextRate()
		if !ok {
			return search
		}
		outcome := ProbePassing
		if rate > passAbove {
			outcome = ProbeFailing
		}
		if err := search.Record(rate, outcome); err != nil {
			testContext.Fatalf("Record(%d): %v", rate, err)
		}
	}
}

func TestCapacitySearchBracketsWithinFivePercentWithoutRepeatingRates(testContext *testing.T) {
	search := driveSearch(testContext, 25000, 1000000, 24, 37000)
	if search.Status() != SearchBracketed {
		testContext.Fatalf("Status() = %q, want %q", search.Status(), SearchBracketed)
	}
	if search.Lower() > 37000 || search.Upper() <= 37000 {
		testContext.Fatalf("bracket [%d, %d] does not straddle 37000", search.Lower(), search.Upper())
	}
	if 100*search.Upper() > 105*search.Lower() {
		testContext.Fatalf("bracket [%d, %d] is wider than five percent", search.Lower(), search.Upper())
	}
	seen := map[int]bool{}
	for _, probe := range search.Probes() {
		if seen[probe.Rate] {
			testContext.Fatalf("rate %d probed twice", probe.Rate)
		}
		seen[probe.Rate] = true
	}
}

func TestCapacitySearchInitialFailureSearchesDownward(testContext *testing.T) {
	search := driveSearch(testContext, 1000, 1000000, 24, 500)
	if search.Status() != SearchBracketed || search.Lower() > 500 {
		testContext.Fatalf("status %q lower %d, want bracketed at or below 500", search.Status(), search.Lower())
	}
}

func TestCapacitySearchInconclusiveStopsWithoutRetry(testContext *testing.T) {
	search, err := NewCapacitySearch(1000, 1000000, 24)
	if err != nil {
		testContext.Fatalf("NewCapacitySearch: %v", err)
	}
	if err := search.Record(1000, ProbeInconclusive); err != nil {
		testContext.Fatalf("Record: %v", err)
	}
	if search.Status() != SearchInconclusive {
		testContext.Fatalf("Status() = %q, want %q", search.Status(), SearchInconclusive)
	}
	if _, ok := search.NextRate(); ok {
		testContext.Fatal("NextRate() after inconclusive, want terminated search")
	}
	if len(search.Probes()) != 1 {
		testContext.Fatalf("probes = %+v, want exactly one probe", search.Probes())
	}
}

func TestCapacitySearchNoUpperFailureIsOnlyALowerBound(testContext *testing.T) {
	search := driveSearch(testContext, 1000, 2000, 24, 1000000)
	if search.Status() != SearchLowerBoundOnly || search.Lower() != 2000 || search.Upper() != 0 {
		testContext.Fatalf("status %q lower %d upper %d, want lower-bound-only at 2000", search.Status(), search.Lower(), search.Upper())
	}
}

func TestCapacitySearchNoPassIsNotAZeroCapacityClaim(testContext *testing.T) {
	search := driveSearch(testContext, 2, 1000000, 24, 0)
	if search.Status() != SearchNoPassingRate || search.Lower() != 0 {
		testContext.Fatalf("status %q lower %d, want no-passing-rate", search.Status(), search.Lower())
	}
}

func TestCapacitySearchIntegerResolutionDoesNotClaimFivePercent(testContext *testing.T) {
	search := driveSearch(testContext, 2, 1000000, 24, 1)
	if search.Status() != SearchIntegerResolutionLimit {
		testContext.Fatalf("Status() = %q, want %q", search.Status(), SearchIntegerResolutionLimit)
	}
}

func TestCapacitySearchBudgetExhaustionIsNotARefinedBracket(testContext *testing.T) {
	search := driveSearch(testContext, 1000, 1000000, 1, 1000000)
	if search.Status() != SearchProbeBudgetExhausted {
		testContext.Fatalf("Status() = %q, want %q", search.Status(), SearchProbeBudgetExhausted)
	}
}

func TestCapacitySearchRejectsInvalidInputs(testContext *testing.T) {
	for _, settings := range [][3]int{{0, 100, 24}, {100, 0, 24}, {100, 100, 0}, {-1, 100, 24}, {200, 100, 24}} {
		if _, err := NewCapacitySearch(settings[0], settings[1], settings[2]); err == nil {
			testContext.Fatalf("NewCapacitySearch(%d, %d, %d) unexpectedly succeeded", settings[0], settings[1], settings[2])
		}
	}
}

func TestCapacitySearchRejectsDeviationFromSelectedRate(testContext *testing.T) {
	search, err := NewCapacitySearch(1000, 1000000, 24)
	if err != nil {
		testContext.Fatalf("NewCapacitySearch: %v", err)
	}
	if err := search.Record(1001, ProbePassing); err == nil {
		testContext.Fatal("Record at an unselected rate unexpectedly succeeded")
	}
	if err := search.Record(1000, ProbeOutcome("maybe")); err == nil {
		testContext.Fatal("Record with an unknown outcome unexpectedly succeeded")
	}
	if err := search.Record(1000, ProbePassing); err != nil {
		testContext.Fatalf("Record: %v", err)
	}
	rate, _ := search.NextRate()
	if rate != 2000 {
		testContext.Fatalf("NextRate() = %d, want 2000 after a pass at 1000", rate)
	}
}

func TestCapacitySearchRejectsProbesAfterTermination(testContext *testing.T) {
	search := driveSearch(testContext, 1000, 2000, 24, 1000000)
	if err := search.Record(2000, ProbePassing); err == nil || !strings.Contains(err.Error(), "terminated") {
		testContext.Fatalf("Record after termination error = %v, want terminated error", err)
	}
}

// recordRepetitions records outcomes at the search's pending repetition rate.
func recordRepetitions(testContext *testing.T, search *CapacitySearch, outcomes ...ProbeOutcome) {
	testContext.Helper()
	for index, outcome := range outcomes {
		rate, pending := search.NextRepetitionRate()
		if !pending {
			testContext.Fatalf("repetition %d: none pending (status %q)", index+1, search.Status())
		}
		if err := search.RecordRepetition(rate, outcome); err != nil {
			testContext.Fatalf("RecordRepetition(%d, %q): %v", rate, outcome, err)
		}
	}
}

func fivePassing() []ProbeOutcome {
	return []ProbeOutcome{ProbePassing, ProbePassing, ProbePassing, ProbePassing, ProbePassing}
}

func TestDecideCapacityRequiresBracketThenFivePassingRepetitionsAtLowerRate(testContext *testing.T) {
	search := driveSearch(testContext, 25000, 1000000, 24, 37000)
	selected := search.Lower()
	if rate, pending := search.NextRepetitionRate(); !pending || rate != selected {
		testContext.Fatalf("NextRepetitionRate() = %d, %t, want %d pending", rate, pending, selected)
	}
	recordRepetitions(testContext, search, fivePassing()...)
	decision := DecideCapacity(search)
	if decision.Decision != Pass || decision.SelectedRate != selected {
		testContext.Fatalf("DecideCapacity() = %+v, want pass at %d", decision, selected)
	}
	if _, pending := search.NextRepetitionRate(); pending {
		testContext.Fatal("a validated search still asks for a repetition")
	}
	rounds := search.ValidationRounds()
	if len(rounds) != 1 || rounds[0].Rate != selected || len(rounds[0].Outcomes) != RequiredFullRepetitions {
		testContext.Fatalf("ValidationRounds() = %+v, want one round of five at %d", rounds, selected)
	}
}

func TestDecideCapacityRepetitionRules(testContext *testing.T) {
	tests := []struct {
		name     string
		outcomes []ProbeOutcome
		want     Decision
		reason   string
		pending  bool
	}{
		{name: "missing", outcomes: nil, want: Inconclusive, reason: RepetitionsMissingReason, pending: true},
		{name: "four only", outcomes: fivePassing()[:4], want: Inconclusive, reason: RepetitionsMissingReason, pending: true},
		{name: "five", outcomes: fivePassing(), want: Pass},
		{name: "one inconclusive", outcomes: []ProbeOutcome{ProbePassing, ProbePassing, ProbeInconclusive}, want: Inconclusive,
			reason: RepetitionInconclusiveReason},
	}
	for _, test := range tests {
		testContext.Run(test.name, func(testContext *testing.T) {
			search := driveSearch(testContext, 25000, 1000000, 24, 37000)
			selected := search.Lower()
			recordRepetitions(testContext, search, test.outcomes...)
			decision := DecideCapacity(search)
			if decision.Decision != test.want || decision.Reason != test.reason || decision.SelectedRate != selected {
				testContext.Fatalf("DecideCapacity() = %+v, want %q with reason %q at %d", decision, test.want, test.reason, selected)
			}
			if _, pending := search.NextRepetitionRate(); pending != test.pending {
				testContext.Fatalf("repetition pending = %t, want %t", pending, test.pending)
			}
		})
	}
}

// A repetition is recorded only while one is pending, at exactly the selected
// rate, with a known outcome: the replay cannot add, move or invent runs.
func TestRecordRepetitionRejectsRunsTheSearchDidNotAskFor(testContext *testing.T) {
	running, err := NewCapacitySearch(1000, 1000000, 24)
	if err != nil {
		testContext.Fatal(err)
	}
	if err := running.RecordRepetition(1000, ProbePassing); err == nil {
		testContext.Fatal("a running search accepted a repetition")
	}
	search := driveSearch(testContext, 25000, 1000000, 24, 37000)
	selected := search.Lower()
	if err := search.RecordRepetition(selected+1, ProbePassing); err == nil {
		testContext.Fatal("a repetition at the wrong rate was accepted")
	}
	if err := search.RecordRepetition(selected, ProbeOutcome("maybe")); err == nil {
		testContext.Fatal("an unknown repetition outcome was accepted")
	}
	recordRepetitions(testContext, search, fivePassing()...)
	if err := search.RecordRepetition(selected, ProbePassing); err == nil {
		testContext.Fatal("a sixth repetition was accepted")
	}
	inconclusive := driveSearch(testContext, 25000, 1000000, 24, 37000)
	recordRepetitions(testContext, inconclusive, ProbeInconclusive)
	if err := inconclusive.RecordRepetition(selected, ProbePassing); err == nil {
		testContext.Fatal("a repetition after an inconclusive one was accepted")
	}
}

// The budget's result is the lower passing rate, never a transient peak. A
// repetition that fails or does not demonstrate the selected rate bounds the
// bracket from above like a failed probe; the search resumes below it and
// validates the rate it then selects.
func TestFailedValidationMovesTheSearchBelowTheRate(testContext *testing.T) {
	for _, outcome := range []ProbeOutcome{ProbeFailing, ProbeNotDemonstrated} {
		testContext.Run(string(outcome), func(testContext *testing.T) {
			search := driveSearch(testContext, 25000, 1000000, 24, 37000)
			peak := search.Lower()
			var belowPeak int
			for _, probe := range search.Probes() {
				if probe.Outcome == ProbePassing && probe.Rate < peak && probe.Rate > belowPeak {
					belowPeak = probe.Rate
				}
			}
			recordRepetitions(testContext, search, ProbePassing, ProbePassing, outcome)
			if search.Upper() != peak || search.Lower() != belowPeak {
				testContext.Fatalf("bracket [%d, %d], want [%d, %d]", search.Lower(), search.Upper(), belowPeak, peak)
			}
			if _, pending := search.NextRepetitionRate(); pending && search.Lower() == peak {
				testContext.Fatal("the failed rate is still being validated")
			}
			// The sustained capacity is below the peak: continue the search
			// with every rate at or above the peak failing.
			for {
				rate, running := search.NextRate()
				if !running {
					break
				}
				if rate >= peak {
					testContext.Fatalf("the search probed %d, at or above the rejected rate %d", rate, peak)
				}
				if err := search.Record(rate, ProbePassing); err != nil {
					testContext.Fatal(err)
				}
			}
			if search.Status() != SearchBracketed || search.Lower() >= peak || 100*search.Upper() > 105*search.Lower() {
				testContext.Fatalf("status %q bracket [%d, %d], want a refined bracket below %d", search.Status(), search.Lower(), search.Upper(), peak)
			}
			if decision := DecideCapacity(search); decision.Decision != Inconclusive || decision.Reason != RepetitionsMissingReason ||
				decision.SelectedRate != search.Lower() {
				testContext.Fatalf("DecideCapacity() before the new round = %+v, want repetitions missing at %d", decision, search.Lower())
			}
			recordRepetitions(testContext, search, fivePassing()...)
			decision := DecideCapacity(search)
			if decision.Decision != Pass || decision.SelectedRate != search.Lower() || decision.SelectedRate >= peak {
				testContext.Fatalf("DecideCapacity() = %+v, want a pass below %d", decision, peak)
			}
			rounds := search.ValidationRounds()
			if len(rounds) != 2 || rounds[0].Rate != peak || len(rounds[0].Outcomes) != 3 || rounds[0].Outcomes[2] != outcome ||
				rounds[1].Rate != decision.SelectedRate || len(rounds[1].Outcomes) != RequiredFullRepetitions {
				testContext.Fatalf("ValidationRounds() = %+v, want the failed round at %d then five at %d", rounds, peak, decision.SelectedRate)
			}
		})
	}
}

// The recorded smoke search of 2026-09-24 (SSNM 10 x 1,024 APCs over 8
// associations, perftraffic at ae84f02): the bracket closed at 300,000 msg/s,
// and the first validation repetition there lost DATA. The old rule failed the
// whole search; the search now continues between the highest passing probe
// below it and 300,000.
func TestRecordedSmokeSearchContinuesBelowAFailedValidation(testContext *testing.T) {
	search, err := NewCapacitySearch(40000, 640000, 24)
	if err != nil {
		testContext.Fatal(err)
	}
	for _, probe := range []ProbeRecord{{40000, ProbePassing}, {80000, ProbePassing}, {160000, ProbePassing}, {320000, ProbeFailing},
		{240000, ProbePassing}, {280000, ProbePassing}, {300000, ProbePassing}, {310000, ProbeNotDemonstrated}} {
		if err := search.Record(probe.Rate, probe.Outcome); err != nil {
			testContext.Fatalf("Record(%d): %v", probe.Rate, err)
		}
	}
	if rate, pending := search.NextRepetitionRate(); search.Status() != SearchBracketed || !pending || rate != 300000 {
		testContext.Fatalf("status %q repetition %d/%t, want validation at 300000", search.Status(), rate, pending)
	}
	recordRepetitions(testContext, search, ProbeFailing)
	if next, running := search.NextRate(); !running || next != 290000 {
		testContext.Fatalf("NextRate() = %d, %t, want 290000 between 280000 and 300000", next, running)
	}
	if decision := DecideCapacity(search); decision.Decision != Inconclusive || decision.Reason != SearchIncompleteReason {
		testContext.Fatalf("DecideCapacity() = %+v, want an incomplete search", decision)
	}
}

// Validation descends at most MaxValidationRounds times; then the search is
// inconclusive, not a pass at whatever rate it reached.
func TestValidationRoundsAreBounded(testContext *testing.T) {
	search := driveSearch(testContext, 25000, 1000000, 64, 37000)
	for round := 1; ; round++ {
		if round > MaxValidationRounds {
			testContext.Fatalf("validation continued past %d rounds", MaxValidationRounds)
		}
		recordRepetitions(testContext, search, ProbeFailing)
		if search.Status() == SearchValidationRoundsExhausted {
			if round != MaxValidationRounds {
				testContext.Fatalf("exhausted after %d rounds, want %d", round, MaxValidationRounds)
			}
			break
		}
		for {
			rate, running := search.NextRate()
			if !running {
				break
			}
			if err := search.Record(rate, ProbePassing); err != nil {
				testContext.Fatal(err)
			}
		}
		if search.Status() != SearchBracketed {
			testContext.Fatalf("round %d: status %q, want a new bracket", round, search.Status())
		}
	}
	if _, running := search.NextRate(); running {
		testContext.Fatal("an exhausted search still selects probes")
	}
	if _, pending := search.NextRepetitionRate(); pending {
		testContext.Fatal("an exhausted search still asks for repetitions")
	}
	if decision := DecideCapacity(search); decision.Decision != Inconclusive || decision.Reason != ValidationRoundsExhaustedReason ||
		decision.SelectedRate != 0 {
		testContext.Fatalf("DecideCapacity() = %+v, want inconclusive with %q", decision, ValidationRoundsExhaustedReason)
	}
}

// A failed validation at the only passing rate sends the search downward from
// it; if nothing below passes either, the search has no passing rate. That is
// a failure when every run failed, and inconclusive when a repetition was only
// not demonstrated.
func TestFailedValidationAtTheOnlyPassingRateCanEndWithNoPassingRate(testContext *testing.T) {
	for _, test := range []struct {
		outcome ProbeOutcome
		want    Decision
		reason  string
	}{{ProbeFailing, Fail, NoPassingRateReason}, {ProbeNotDemonstrated, Inconclusive, NoDemonstratedRateReason}} {
		testContext.Run(string(test.outcome), func(testContext *testing.T) {
			search := driveSearch(testContext, 100, 1000000, 24, 100)
			if search.Status() != SearchBracketed || search.Lower() != 100 {
				testContext.Fatalf("status %q lower %d, want bracketed at 100", search.Status(), search.Lower())
			}
			recordRepetitions(testContext, search, test.outcome)
			for {
				rate, running := search.NextRate()
				if !running {
					break
				}
				if err := search.Record(rate, ProbeFailing); err != nil {
					testContext.Fatal(err)
				}
			}
			decision := DecideCapacity(search)
			if search.Status() != SearchNoPassingRate || decision.Decision != test.want || decision.Reason != test.reason {
				testContext.Fatalf("status %q decision %+v, want %q with %q", search.Status(), decision, test.want, test.reason)
			}
		})
	}
}

func TestDecideCapacityMapsTerminalSearchStatuses(testContext *testing.T) {
	tests := []struct {
		name      string
		initial   int
		passAbove int
		maxProbes int
		want      Decision
		reason    string
	}{
		{name: "incomplete", initial: 1000, passAbove: -1, maxProbes: 24, want: Inconclusive, reason: SearchIncompleteReason},
		{name: "no passing rate", initial: 2, passAbove: 0, maxProbes: 24, want: Fail, reason: NoPassingRateReason},
		{name: "lower bound only", initial: 1000, passAbove: 1000000, maxProbes: 24, want: Inconclusive, reason: SearchNotRefinedReason},
		{name: "integer resolution", initial: 2, passAbove: 1, maxProbes: 24, want: Inconclusive, reason: SearchNotRefinedReason},
		{name: "budget exhausted", initial: 1000, passAbove: 1000000, maxProbes: 1, want: Inconclusive, reason: SearchNotRefinedReason},
	}
	for _, test := range tests {
		testContext.Run(test.name, func(testContext *testing.T) {
			var search *CapacitySearch
			if test.name == "incomplete" {
				var err error
				search, err = NewCapacitySearch(test.initial, 1000000, test.maxProbes)
				if err != nil {
					testContext.Fatalf("NewCapacitySearch: %v", err)
				}
				if err := search.Record(test.initial, ProbeFailing); err != nil {
					testContext.Fatalf("Record: %v", err)
				}
			} else {
				search = driveSearch(testContext, test.initial, 1000000, test.maxProbes, test.passAbove)
			}
			decision := DecideCapacity(search)
			if decision.Decision != test.want || !strings.HasPrefix(decision.Reason, test.reason) {
				testContext.Fatalf("DecideCapacity() = %+v, want %q with reason %q", decision, test.want, test.reason)
			}
		})
	}
}

func TestDecideCapacityInconclusiveProbeStaysInconclusive(testContext *testing.T) {
	search, err := NewCapacitySearch(1000, 1000000, 24)
	if err != nil {
		testContext.Fatalf("NewCapacitySearch: %v", err)
	}
	if err := search.Record(1000, ProbeInconclusive); err != nil {
		testContext.Fatalf("Record: %v", err)
	}
	decision := DecideCapacity(search)
	if decision.Decision != Inconclusive || decision.Reason != SearchNotRefinedReason {
		testContext.Fatalf("DecideCapacity() = %+v, want inconclusive", decision)
	}
}

// A maximum above MaximumSearchRate wraps the search arithmetic. Recorded
// against the unbounded constructor, a pass at math.MaxInt/4 followed by a
// fail at twice that rate wrapped 100*upper to -200, which satisfied
// 100*upper <= 105*lower on a twofold bracket, reported SearchBracketed and
// let DecideCapacity return a capacity pass. Doubling from math.MaxInt/2+1
// selected a negative probe rate. The bound is what makes both unreachable.
func TestNewCapacitySearchRejectsRatesThatOverflowTheSearchArithmetic(testContext *testing.T) {
	tests := []struct {
		name    string
		initial int
		maximum int
	}{
		{name: "one above the bound", initial: 1, maximum: MaximumSearchRate + 1},
		{name: "signed limit", initial: 1, maximum: math.MaxInt},
		{name: "bracket comparison wrap", initial: math.MaxInt / 4, maximum: math.MaxInt},
		{name: "doubling wrap", initial: math.MaxInt/2 + 1, maximum: math.MaxInt},
	}
	for _, test := range tests {
		testContext.Run(test.name, func(testContext *testing.T) {
			search, err := NewCapacitySearch(test.initial, test.maximum, DefaultMaxProbes)
			if err == nil {
				testContext.Fatalf("NewCapacitySearch(%d, %d, %d) accepted an overflowing maximum: %+v",
					test.initial, test.maximum, DefaultMaxProbes, search)
			}
			if !strings.Contains(err.Error(), "maximum must not exceed") {
				testContext.Fatalf("error = %v, want the maximum-rate bound", err)
			}
		})
	}
}

// The bound must be exactly where the arithmetic stops being exact: high
// enough to admit every rate the fixture can offer, low enough that the
// widest product advance() forms still fits in an int.
func TestMaximumSearchRateKeepsEverySearchProductExact(testContext *testing.T) {
	// The products are formed from a variable so they wrap at run time the
	// way advance() wraps, instead of being rejected as untyped constants.
	bound := MaximumSearchRate
	for name, product := range map[string]int{
		"105*lower":     105 * bound,
		"100*upper":     100 * bound,
		"2*lower":       2 * bound,
		"lower + upper": bound + bound,
	} {
		if product <= 0 {
			testContext.Fatalf("%s overflowed to %d at MaximumSearchRate %d", name, product, bound)
		}
	}
	if 105*bound/105 != bound {
		testContext.Fatalf("105*%d does not round-trip", bound)
	}
	if MaximumSearchRate < DefaultMaximumRate {
		testContext.Fatalf("MaximumSearchRate %d is below the default maximum rate %d", MaximumSearchRate, DefaultMaximumRate)
	}
	if _, err := NewCapacitySearch(1, MaximumSearchRate, DefaultMaxProbes); err != nil {
		testContext.Fatalf("NewCapacitySearch at the bound: %v", err)
	}
	if _, err := NewCapacitySearch(1, DefaultMaximumRate, DefaultMaxProbes); err != nil {
		testContext.Fatalf("NewCapacitySearch at the default maximum rate: %v", err)
	}
}

// At the accepted bound the search still behaves: every selected rate is a
// positive rate inside the bounds, and a terminal SearchBracketed really is
// within five percent when the comparison is made in floating point rather
// than in the integer arithmetic under test.
func TestCapacitySearchStaysExactAtTheAcceptedMaximum(testContext *testing.T) {
	for _, passAbove := range []int{1, MaximumSearchRate / 3, MaximumSearchRate / 2, MaximumSearchRate} {
		search, err := NewCapacitySearch(1, MaximumSearchRate, 64)
		if err != nil {
			testContext.Fatalf("NewCapacitySearch: %v", err)
		}
		for {
			rate, ok := search.NextRate()
			if !ok {
				break
			}
			if rate <= 0 || rate > MaximumSearchRate {
				testContext.Fatalf("passAbove %d: selected rate %d outside (0, %d]", passAbove, rate, MaximumSearchRate)
			}
			outcome := ProbePassing
			if rate > passAbove {
				outcome = ProbeFailing
			}
			if err := search.Record(rate, outcome); err != nil {
				testContext.Fatalf("Record(%d): %v", rate, err)
			}
		}
		if search.Status() == SearchBracketed && float64(search.Upper()) > 1.05*float64(search.Lower()) {
			testContext.Fatalf("passAbove %d: bracket [%d, %d] is wider than five percent but reported %q",
				passAbove, search.Lower(), search.Upper(), SearchBracketed)
		}
	}
}

// Near and above capacity a probe is expected to stall or to leave its backlog
// trend straddling the floor. Such a probe bounds the bracket from above like a
// failure; it must not end the search.
func TestNotDemonstratedProbeBoundsTheBracketAndContinues(testContext *testing.T) {
	search, err := NewCapacitySearch(20000, 1_000_000, 24)
	if err != nil {
		testContext.Fatal(err)
	}
	schedule := []struct {
		rate    int
		outcome ProbeOutcome
	}{
		{20000, ProbePassing}, {40000, ProbePassing}, {80000, ProbePassing},
		{160000, ProbeFailing}, {120000, ProbeNotDemonstrated}, {100000, ProbePassing},
		{110000, ProbeNotDemonstrated}, {105000, ProbePassing},
	}
	for index, probe := range schedule {
		next, running := search.NextRate()
		if !running || next != probe.rate {
			testContext.Fatalf("probe %d: NextRate() = %d, %t; want %d", index+1, next, running, probe.rate)
		}
		if err := search.Record(probe.rate, probe.outcome); err != nil {
			testContext.Fatalf("probe %d: %v", index+1, err)
		}
	}
	if search.Status() != SearchBracketed || search.Lower() != 105000 || search.Upper() != 110000 {
		testContext.Fatalf("status %q bracket [%d, %d], want bracketed [105000, 110000]", search.Status(), search.Lower(), search.Upper())
	}
	probes := search.Probes()
	if probes[4].Outcome != ProbeNotDemonstrated || probes[6].Outcome != ProbeNotDemonstrated {
		testContext.Fatalf("recorded outcomes %+v lost the not-demonstrated probes", probes)
	}
	recordRepetitions(testContext, search, fivePassing()...)
	decision := DecideCapacity(search)
	if decision.Decision != Pass || decision.SelectedRate != 105000 {
		testContext.Fatalf("DecideCapacity() = %+v, want pass at 105000", decision)
	}
}

// Failure means the workload was shown not to be sustainable. A search with no
// demonstrated rate is a failure only if every probe failed; undemonstrated
// probes leave it inconclusive.
func TestNoPassingRateNeedsDemonstratedFailures(testContext *testing.T) {
	for _, scenario := range []struct {
		name     string
		outcomes func(index int) ProbeOutcome
		want     Decision
		reason   string
	}{
		{"all failed", func(int) ProbeOutcome { return ProbeFailing }, Fail, NoPassingRateReason},
		{"one undemonstrated", func(index int) ProbeOutcome {
			if index == 1 {
				return ProbeNotDemonstrated
			}
			return ProbeFailing
		}, Inconclusive, NoDemonstratedRateReason},
	} {
		testContext.Run(scenario.name, func(testContext *testing.T) {
			search, err := NewCapacitySearch(4, 100, 24)
			if err != nil {
				testContext.Fatal(err)
			}
			for index := 0; ; index++ {
				next, running := search.NextRate()
				if !running {
					break
				}
				if err := search.Record(next, scenario.outcomes(index)); err != nil {
					testContext.Fatal(err)
				}
			}
			if search.Status() != SearchNoPassingRate {
				testContext.Fatalf("status %q, want no passing rate", search.Status())
			}
			if decision := DecideCapacity(search); decision.Decision != scenario.want || decision.Reason != scenario.reason {
				testContext.Fatalf("DecideCapacity() = %+v, want %q with %q", decision, scenario.want, scenario.reason)
			}
		})
	}
}
