package perfstats

import (
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
