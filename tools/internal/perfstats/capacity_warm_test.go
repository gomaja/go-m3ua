package perfstats

import (
	"encoding/json"
	"math/rand"
	"reflect"
	"testing"
)

func warmSearch(testContext *testing.T, initial, maximum, budget, upperHint int) *CapacitySearch {
	testContext.Helper()
	search, err := NewCapacitySearchWithUpperHint(initial, maximum, budget, upperHint)
	if err != nil {
		testContext.Fatal(err)
	}
	return search
}

func warmProbe(testContext *testing.T, search *CapacitySearch, rate int, outcome ProbeOutcome) {
	testContext.Helper()
	if next, pending := search.NextRate(); !pending || next != rate {
		testContext.Fatalf("next probe = %d, %t, want %d", next, pending, rate)
	}
	if err := search.Record(rate, outcome); err != nil {
		testContext.Fatal(err)
	}
}

func TestCapacityUpperHintBracketNeedsItsOwnValidation(testContext *testing.T) {
	for _, outcome := range []ProbeOutcome{ProbeFailing, ProbeNotDemonstrated} {
		testContext.Run(string(outcome), func(testContext *testing.T) {
			search := warmSearch(testContext, 1000, 10000, 24, 1050)
			if search.Lower() != 0 || search.Upper() != 0 || search.UpperHintUsed() {
				testContext.Fatal("the hint supplied evidence before any probe")
			}
			warmProbe(testContext, search, 1000, ProbePassing)
			if !search.UpperHintUsed() || search.Upper() != 0 {
				testContext.Fatal("the hint must select a probe without establishing an upper bound")
			}
			warmProbe(testContext, search, 1050, outcome)
			if search.Status() != SearchBracketed || search.Lower() != 1000 || search.Upper() != 1050 ||
				len(search.Probes()) != 2 || DecideCapacity(search).Decision != Inconclusive {
				testContext.Fatalf("search %+v, want a two-probe bracket awaiting validation", search)
			}
			recordRepetitions(testContext, search, ProbePassing, ProbePassing, ProbePassing, ProbePassing)
			if DecideCapacity(search).Decision != Inconclusive {
				testContext.Fatal("four passing repetitions validated the hinted search")
			}
			recordRepetitions(testContext, search, ProbePassing)
			if decision := DecideCapacity(search); decision.Decision != Pass || decision.SelectedRate != 1000 {
				testContext.Fatalf("decision %+v, want a validated rate of 1000", decision)
			}
		})
	}
}

func TestCapacityUpperHintInitialDoesNotPass(testContext *testing.T) {
	for _, outcome := range []ProbeOutcome{ProbeFailing, ProbeNotDemonstrated, ProbeInconclusive} {
		testContext.Run(string(outcome), func(testContext *testing.T) {
			search := warmSearch(testContext, 1000, 10000, 24, 1050)
			warmProbe(testContext, search, 1000, outcome)
			if search.UpperHintUsed() {
				testContext.Fatal("hint used after the initial probe did not pass")
			}
			if outcome == ProbeInconclusive {
				if search.Status() != SearchInconclusive {
					testContext.Fatal("missing evidence did not terminate the search")
				}
				return
			}
			warmProbe(testContext, search, 500, ProbePassing)
			if next, pending := search.NextRate(); !pending || next != 750 || search.UpperHintUsed() || search.Upper() != 1000 {
				testContext.Fatalf("search %+v, want normal bisection below the failed initial rate", search)
			}
		})
	}
}

// Reusing U after its pass would repeat 1050 instead of doubling to 2100.
func TestCapacityUpperHintIsUsedOnlyOnce(testContext *testing.T) {
	for _, maximum := range []int{10000, 1500} {
		search := warmSearch(testContext, 1000, maximum, 24, 1050)
		warmProbe(testContext, search, 1000, ProbePassing)
		warmProbe(testContext, search, 1050, ProbePassing)
		warmProbe(testContext, search, min(2100, maximum), ProbePassing)
		if !search.UpperHintUsed() || len(search.Probes()) != 3 {
			testContext.Fatalf("search %+v, want three distinct decided probes", search)
		}
		if maximum == 1500 && search.Status() != SearchLowerBoundOnly {
			testContext.Fatal("passing at maximum did not end with a lower bound")
		}
	}
}

func TestCapacityUpperHintRejectsInvalidBounds(testContext *testing.T) {
	for _, test := range []struct {
		name                    string
		initial, maximum, upper int
	}{
		{"below initial", 1000, 10000, 999},
		{"equal initial", 1000, 10000, 1000},
		{"zero", 1000, 10000, 0},
		{"negative", 1000, 10000, -1},
		{"above maximum", 1000, 1049, 1050},
		{"over five percent", 1000, 10000, 1051},
		{"integer over five percent", 19, 100, 20},
		{"overflowing maximum", 1000, MaximumSearchRate + 1, MaximumSearchRate + 1},
	} {
		testContext.Run(test.name, func(testContext *testing.T) {
			if search, err := NewCapacitySearchWithUpperHint(test.initial, test.maximum, 24, test.upper); err == nil || search != nil {
				testContext.Fatalf("accepted invalid bounds: %+v, %v", search, err)
			}
		})
	}
	for _, bounds := range [][3]int{{20, 21, 21}, {1000, 1050, 1050}, {MaximumSearchRate - 1, MaximumSearchRate, MaximumSearchRate}} {
		search := warmSearch(testContext, bounds[0], bounds[1], 24, bounds[2])
		warmProbe(testContext, search, bounds[0], ProbePassing)
		warmProbe(testContext, search, bounds[2], ProbeFailing)
		if search.Status() != SearchBracketed {
			testContext.Fatal("valid boundary hint did not bracket")
		}
	}
}

func TestCapacityUpperHintRepeatsAtBothBounds(testContext *testing.T) {
	for _, upperOutcome := range []ProbeOutcome{ProbeFailing, ProbePassing, ProbeBacklogUndecided, ProbeInconclusive} {
		testContext.Run(string(upperOutcome), func(testContext *testing.T) {
			search := warmSearch(testContext, 1000, 10000, 24, 1050)
			for attempt := 1; attempt <= 2; attempt++ {
				warmProbe(testContext, search, 1000, ProbeBacklogUndecided)
				if search.UpperHintUsed() || search.NextAttempt() != attempt+1 || len(search.Probes()) != 0 {
					testContext.Fatal("deferred L attempt used the hint or counted a probe")
				}
			}
			warmProbe(testContext, search, 1000, ProbePassing)
			for attempt := 1; attempt <= 2; attempt++ {
				warmProbe(testContext, search, 1050, ProbeBacklogUndecided)
				if !search.UpperHintUsed() || search.NextAttempt() != attempt+1 || len(search.Probes()) != 1 || search.Upper() != 0 {
					testContext.Fatal("deferred U attempt counted a bound or used another probe")
				}
			}
			warmProbe(testContext, search, 1050, upperOutcome)
			switch upperOutcome {
			case ProbePassing:
				warmProbe(testContext, search, 2100, ProbeFailing)
			case ProbeInconclusive:
				if search.Status() != SearchInconclusive {
					testContext.Fatal("inconclusive U did not terminate")
				}
			default:
				if search.Status() != SearchBracketed || search.Upper() != 1050 || len(search.Probes()) != 2 {
					testContext.Fatal("resolved non-passing U did not bracket")
				}
				if upperOutcome == ProbeBacklogUndecided && search.Probes()[1].Outcome != ProbeNotDemonstrated {
					testContext.Fatal("three U straddles did not count once as not-demonstrated")
				}
			}
		})
	}
	search := warmSearch(testContext, 1000, 10000, 24, 1050)
	for attempt := 0; attempt < 3; attempt++ {
		warmProbe(testContext, search, 1000, ProbeBacklogUndecided)
	}
	warmProbe(testContext, search, 500, ProbePassing)
	if search.UpperHintUsed() || search.Upper() != 1000 || search.Probes()[0].Outcome != ProbeNotDemonstrated {
		testContext.Fatal("three L straddles must disable the hint and halve below L")
	}
}

func TestCapacityUpperHintRejectedValidationResumesNormally(testContext *testing.T) {
	for _, outcome := range []ProbeOutcome{ProbeFailing, ProbeNotDemonstrated, ProbeBacklogUndecided} {
		search := warmSearch(testContext, 1000, 10000, 24, 1050)
		warmProbe(testContext, search, 1000, ProbePassing)
		warmProbe(testContext, search, 1050, ProbeFailing)
		recordRepetitions(testContext, search, ProbePassing)
		if outcome == ProbeBacklogUndecided {
			recordRepetitions(testContext, search, outcome, outcome)
			if search.Lower() != 1000 || search.NextAttempt() != 3 {
				testContext.Fatal("deferred validation attempt changed the selected rate")
			}
		}
		recordRepetitions(testContext, search, outcome)
		if search.Upper() != 1000 || search.Lower() != 0 || DecideCapacity(search).SelectedRate != 0 {
			testContext.Fatal("failed validation retained the old lower bound")
		}
		for _, rate := range []int{500, 750, 875, 937, 968} {
			warmProbe(testContext, search, rate, ProbePassing)
		}
		if search.Status() != SearchBracketed || search.Lower() != 968 || search.Upper() != 1000 {
			testContext.Fatalf("search %+v, want new bracket [968,1000]", search)
		}
		recordRepetitions(testContext, search, fivePassing()...)
		if decision := DecideCapacity(search); decision.Decision != Pass || decision.SelectedRate != 968 || len(search.ValidationRounds()) != 2 {
			testContext.Fatalf("decision %+v rounds %+v, want independently validated 968", decision, search.ValidationRounds())
		}
	}
}

func TestCapacityUpperHintCannotSpendExtraProbeBudget(testContext *testing.T) {
	for _, budget := range []int{1, 2} {
		search := warmSearch(testContext, 1000, 10000, budget, 1050)
		warmProbe(testContext, search, 1000, ProbePassing)
		if budget == 1 {
			if search.Status() != SearchProbeBudgetExhausted || search.UpperHintUsed() {
				testContext.Fatal("hint bypassed a one-probe budget")
			}
			continue
		}
		warmProbe(testContext, search, 1050, ProbeFailing)
		if search.Status() != SearchBracketed {
			testContext.Fatal("the final budgeted probe cannot close a bracket")
		}
	}
}

// Compare every observable state with the frozen pre-hint implementation,
// including validation backtracking, attempts, errors and JSON decisions.
func TestCapacityNoHintEquivalentToBaseline(testContext *testing.T) {
	random := rand.New(rand.NewSource(44))
	seen := make(map[SearchStatus]bool)
	validated := 0
	for sequence := 0; sequence < 10000; sequence++ {
		maximum := 2 + random.Intn(1000000)
		if sequence%10 == 0 {
			maximum = MaximumSearchRate
		}
		initial := 1 + random.Intn(maximum)
		budget := 1 + random.Intn(32)
		if sequence%8 == 0 {
			initial, maximum, budget = 1, 32, 24
		}
		search, err := NewCapacitySearch(initial, maximum, budget)
		if err != nil {
			testContext.Fatal(err)
		}
		baseline, err := BaselineNewCapacitySearch(initial, maximum, budget)
		if err != nil {
			testContext.Fatal(err)
		}
		cutoff := 1 + random.Intn(maximum)
		for step := 0; ; step++ {
			probe, pending := search.NextRate()
			oldProbe, oldPending := baseline.NextRate()
			repetition, validating := search.NextRepetitionRate()
			oldRepetition, oldValidating := baseline.NextRepetitionRate()
			currentJSON, err := json.Marshal([]any{search.Probes(), search.ValidationRounds(), DecideCapacity(search)})
			if err != nil {
				testContext.Fatal(err)
			}
			oldJSON, err := json.Marshal([]any{baseline.Probes(), baseline.ValidationRounds(), BaselineDecideCapacity(baseline)})
			if err != nil {
				testContext.Fatal(err)
			}
			if probe != oldProbe || pending != oldPending || repetition != oldRepetition || validating != oldValidating ||
				string(search.Status()) != string(baseline.Status()) || search.Lower() != baseline.Lower() || search.Upper() != baseline.Upper() ||
				search.NextAttempt() != baseline.NextAttempt() || !reflect.DeepEqual(currentJSON, oldJSON) || search.UpperHintUsed() {
				testContext.Fatalf("sequence %d step %d bounds (%d,%d,%d): no-hint search diverged\n%s\n%s",
					sequence, step, initial, maximum, budget, currentJSON, oldJSON)
			}
			if !pending && !validating {
				seen[search.Status()] = true
				if DecideCapacity(search).Decision == Pass {
					validated++
				}
				break
			}
			if step >= 3*budget+3*3*5 {
				testContext.Fatalf("sequence %d exceeded the fixture-run budget", sequence)
			}
			rate := probe
			if validating {
				rate = repetition
			}
			for _, invalid := range []ProbeRecord{{rate + 1, ProbePassing}, {rate, "unknown"}} {
				var oldErr error
				if validating {
					err = search.RecordRepetition(invalid.Rate, invalid.Outcome)
					oldErr = baseline.RecordRepetition(invalid.Rate, BaselineProbeOutcome(invalid.Outcome))
				} else {
					err = search.Record(invalid.Rate, invalid.Outcome)
					oldErr = baseline.Record(invalid.Rate, BaselineProbeOutcome(invalid.Outcome))
				}
				if err == nil || oldErr == nil || err.Error() != oldErr.Error() {
					testContext.Fatalf("sequence %d: rejected-input errors differ: %v, %v", sequence, err, oldErr)
				}
				afterJSON, err := json.Marshal([]any{search.Probes(), search.ValidationRounds(), DecideCapacity(search)})
				if err != nil {
					testContext.Fatal(err)
				}
				afterProbe, afterPending := search.NextRate()
				afterRepetition, afterValidating := search.NextRepetitionRate()
				if !reflect.DeepEqual(afterJSON, currentJSON) || search.NextAttempt() != baseline.NextAttempt() ||
					afterProbe != probe || afterPending != pending || afterRepetition != repetition || afterValidating != validating ||
					search.Lower() != baseline.Lower() || search.Upper() != baseline.Upper() {
					testContext.Fatalf("sequence %d: rejected input changed search state", sequence)
				}
			}
			outcome := ProbePassing
			if rate > cutoff {
				outcome = ProbeFailing
			}
			switch sequence % 8 {
			case 0:
				if rate > 1 {
					outcome = ProbeFailing
				}
			case 1:
				outcome = ProbePassing
			case 2:
				outcome = ProbeFailing
			case 3:
				if validating {
					outcome = ProbeFailing
				}
			case 4:
				outcome = []ProbeOutcome{ProbePassing, ProbeFailing, ProbeNotDemonstrated, ProbeInconclusive, ProbeBacklogUndecided}[random.Intn(5)]
			default:
				switch random.Intn(25) {
				case 0:
					outcome = ProbeInconclusive
				case 1, 2:
					outcome = ProbeNotDemonstrated
				case 3, 4, 5:
					outcome = ProbeBacklogUndecided
				}
			}
			var oldErr error
			if validating {
				err = search.RecordRepetition(rate, outcome)
				oldErr = baseline.RecordRepetition(rate, BaselineProbeOutcome(outcome))
			} else {
				err = search.Record(rate, outcome)
				oldErr = baseline.Record(rate, BaselineProbeOutcome(outcome))
			}
			if err != nil || oldErr != nil {
				testContext.Fatalf("sequence %d: record errors %v, %v", sequence, err, oldErr)
			}
		}
	}
	for _, status := range []SearchStatus{SearchBracketed, SearchIntegerResolutionLimit, SearchLowerBoundOnly,
		SearchNoPassingRate, SearchInconclusive, SearchProbeBudgetExhausted, SearchValidationRoundsExhausted} {
		if !seen[status] {
			testContext.Fatalf("random sequences did not exercise terminal status %q", status)
		}
	}
	if validated == 0 {
		testContext.Fatal("random sequences did not exercise a fully validated selection")
	}
	testContext.Logf("10000 seeded random outcome sequences match baseline at every step; all seven terminal statuses, %d validated selections", validated)
}
