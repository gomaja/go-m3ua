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

func TestDecideCapacityRequiresBracketThenFivePassingRepetitionsAtLowerRate(testContext *testing.T) {
	search := driveSearch(testContext, 25000, 1000000, 24, 37000)
	selected := search.Lower()

	passing := []Decision{Pass, Pass, Pass, Pass, Pass}
	rates := []int{selected, selected, selected, selected, selected}
	decision := DecideCapacity(search, rates, passing)
	if decision.Decision != Pass || decision.SelectedRate != selected {
		testContext.Fatalf("DecideCapacity() = %+v, want pass at %d", decision, selected)
	}
}

func TestDecideCapacityRepetitionRules(testContext *testing.T) {
	newSearch := func(testContext *testing.T) (*CapacitySearch, int) {
		search := driveSearch(testContext, 25000, 1000000, 24, 37000)
		return search, search.Lower()
	}
	tests := []struct {
		name      string
		rates     func(selected int) []int
		decisions []Decision
		want      Decision
		reason    string
	}{
		{name: "missing", rates: func(int) []int { return nil }, decisions: nil, want: Inconclusive, reason: RepetitionsMissingReason},
		{name: "four only", rates: func(selected int) []int { return []int{selected, selected, selected, selected} },
			decisions: []Decision{Pass, Pass, Pass, Pass}, want: Inconclusive, reason: RepetitionCountReason},
		{name: "six", rates: func(selected int) []int { return []int{selected, selected, selected, selected, selected, selected} },
			decisions: []Decision{Pass, Pass, Pass, Pass, Pass, Pass}, want: Inconclusive, reason: RepetitionCountReason},
		{name: "wrong rate", rates: func(selected int) []int { return []int{selected, selected, selected, selected, selected + 1} },
			decisions: []Decision{Pass, Pass, Pass, Pass, Pass}, want: Inconclusive, reason: RepetitionRateReason},
		{name: "one fails", rates: func(selected int) []int { return []int{selected, selected, selected, selected, selected} },
			decisions: []Decision{Pass, Pass, Fail, Pass, Pass}, want: Fail, reason: RepetitionFailureReason},
		{name: "one inconclusive", rates: func(selected int) []int { return []int{selected, selected, selected, selected, selected} },
			decisions: []Decision{Pass, Pass, Inconclusive, Pass, Pass}, want: Inconclusive, reason: RepetitionInconclusiveReason},
	}
	for _, test := range tests {
		testContext.Run(test.name, func(testContext *testing.T) {
			search, selected := newSearch(testContext)
			decision := DecideCapacity(search, test.rates(selected), test.decisions)
			if decision.Decision != test.want || !strings.HasPrefix(decision.Reason, test.reason) {
				testContext.Fatalf("DecideCapacity() = %+v, want %q with reason %q", decision, test.want, test.reason)
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
			decision := DecideCapacity(search, nil, nil)
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
	decision := DecideCapacity(search, nil, nil)
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
	decision := DecideCapacity(search, []int{105000, 105000, 105000, 105000, 105000}, []Decision{Pass, Pass, Pass, Pass, Pass})
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
			if decision := DecideCapacity(search, nil, nil); decision.Decision != scenario.want || decision.Reason != scenario.reason {
				testContext.Fatalf("DecideCapacity() = %+v, want %q with %q", decision, scenario.want, scenario.reason)
			}
		})
	}
}
