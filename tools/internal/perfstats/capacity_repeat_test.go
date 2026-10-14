package perfstats

import (
	"math/rand"
	"reflect"
	"testing"
)

func pendingCapacityRun(search *CapacitySearch) (rate int, repetition bool) {
	if rate, pending := search.NextRate(); pending {
		return rate, false
	}
	rate, _ = search.NextRepetitionRate()
	return rate, true
}

func recordCapacityRun(search *CapacitySearch, rate int, repetition bool, outcome ProbeOutcome) error {
	if repetition {
		return search.RecordRepetition(rate, outcome)
	}
	return search.Record(rate, outcome)
}

func TestCapacityRepeatRejectedInputDoesNotSpendAttempts(testContext *testing.T) {
	for _, repetition := range []bool{false, true} {
		search, err := NewCapacitySearch(1000, 1000000, 24)
		if err != nil {
			testContext.Fatal(err)
		}
		if repetition {
			search = driveSearch(testContext, 25000, 1000000, 24, 37000)
		}
		rate, _ := pendingCapacityRun(search)
		if err := recordCapacityRun(search, rate, repetition, ProbeBacklogUndecided); err != nil {
			testContext.Fatal(err)
		}
		for _, entry := range []ProbeRecord{{rate + 1, ProbePassing}, {rate, "unknown"}} {
			if err := recordCapacityRun(search, entry.Rate, repetition, entry.Outcome); err == nil {
				testContext.Fatalf("repetition=%t accepted invalid entry %+v", repetition, entry)
			}
			if search.NextAttempt() != 2 {
				testContext.Fatalf("rejected input spent an attempt: next=%d", search.NextAttempt())
			}
		}
		if err := recordCapacityRun(search, rate, repetition, ProbeBacklogUndecided); err != nil {
			testContext.Fatal(err)
		}
		if search.NextAttempt() != 3 {
			testContext.Fatalf("next attempt=%d, want final attempt 3", search.NextAttempt())
		}
		if err := recordCapacityRun(search, rate, repetition, ProbePassing); err != nil {
			testContext.Fatal(err)
		}
		if search.NextAttempt() != 1 {
			testContext.Fatalf("decided run did not reset attempts: next=%d", search.NextAttempt())
		}
	}
}

// Inserting one or two backlog-only undecided attempts before each decided
// outcome must preserve the entire search path. Three straddles must be
// equivalent to one not-demonstrated outcome. This exercises arbitrary
// probe/validation interleavings, backtracking and all terminal statuses.
func TestCapacityRepeatSyntheticSequencesPreserveDecidedPath(testContext *testing.T) {
	random := rand.New(rand.NewSource(44))
	for sequence := 0; sequence < 2000; sequence++ {
		plain, err := NewCapacitySearch(25000, 1000000, 24)
		if err != nil {
			testContext.Fatal(err)
		}
		repeated, err := NewCapacitySearch(25000, 1000000, 24)
		if err != nil {
			testContext.Fatal(err)
		}
		cutoff := 1 + random.Intn(1000000)
		runs, decisions := 0, 0
		for {
			rate, repetition := pendingCapacityRun(plain)
			if rate == 0 {
				break
			}
			outcome := ProbePassing
			if rate > cutoff {
				outcome = ProbeFailing
			}
			switch random.Intn(20) {
			case 0:
				outcome = ProbeInconclusive
			case 1, 2:
				outcome = ProbeNotDemonstrated
			case 3:
				outcome = ProbeFailing
			}
			prefix := random.Intn(3)
			final := outcome
			if outcome == ProbeNotDemonstrated && random.Intn(2) == 0 {
				prefix, final = 2, ProbeBacklogUndecided
			}
			for attempt := 0; attempt < prefix; attempt++ {
				if err := recordCapacityRun(repeated, rate, repetition, ProbeBacklogUndecided); err != nil {
					testContext.Fatalf("sequence %d: %v", sequence, err)
				}
				runs++
				next, nextRepetition := pendingCapacityRun(repeated)
				if next != rate || nextRepetition != repetition || repeated.NextAttempt() != attempt+2 ||
					repeated.Status() != plain.Status() || repeated.Lower() != plain.Lower() || repeated.Upper() != plain.Upper() ||
					!reflect.DeepEqual(repeated.Probes(), plain.Probes()) || !reflect.DeepEqual(repeated.ValidationRounds(), plain.ValidationRounds()) {
					testContext.Fatalf("sequence %d: deferred attempt altered rate, phase, bracket or counted history", sequence)
				}
			}
			if err := recordCapacityRun(plain, rate, repetition, outcome); err != nil {
				testContext.Fatal(err)
			}
			if err := recordCapacityRun(repeated, rate, repetition, final); err != nil {
				testContext.Fatal(err)
			}
			runs++
			decisions++
			next, nextRepetition := pendingCapacityRun(repeated)
			plainNext, plainRepetition := pendingCapacityRun(plain)
			if next != plainNext || nextRepetition != plainRepetition || repeated.Status() != plain.Status() ||
				repeated.Lower() != plain.Lower() || repeated.Upper() != plain.Upper() ||
				!reflect.DeepEqual(repeated.Probes(), plain.Probes()) || !reflect.DeepEqual(repeated.ValidationRounds(), plain.ValidationRounds()) ||
				DecideCapacity(repeated) != DecideCapacity(plain) {
				testContext.Fatalf("sequence %d: repeat expansion changed the decided search path", sequence)
			}
			if (next != 0 && repeated.NextAttempt() != 1) || (next == 0 && repeated.NextAttempt() != 0) {
				testContext.Fatalf("sequence %d: attempt did not reset after a decided outcome", sequence)
			}
			// 24 probe decisions + at most 3 rounds of 5 repetitions; each
			// can use at most 3 attempts, regardless of generated outcomes.
			if decisions > 39 || runs > 117 {
				testContext.Fatalf("sequence %d: unbounded work: %d decisions in %d runs", sequence, decisions, runs)
			}
		}
	}
}

func TestCapacityRepeatStillBoundsValidationRounds(testContext *testing.T) {
	search, err := NewCapacitySearch(25000, 1000000, 24)
	if err != nil {
		testContext.Fatal(err)
	}
	runs := 0
	for {
		rate, repetition := pendingCapacityRun(search)
		if rate == 0 {
			break
		}
		outcome := ProbePassing
		if rate > 37000 {
			outcome = ProbeFailing
		}
		if repetition {
			outcome = ProbeBacklogUndecided
		}
		for attempt := 0; attempt < 2; attempt++ {
			if err := recordCapacityRun(search, rate, repetition, ProbeBacklogUndecided); err != nil {
				testContext.Fatal(err)
			}
			runs++
		}
		if err := recordCapacityRun(search, rate, repetition, outcome); err != nil {
			testContext.Fatal(err)
		}
		runs++
		if runs > 117 {
			testContext.Fatal("search exceeded its total attempt bound")
		}
	}
	if search.Status() != SearchValidationRoundsExhausted || len(search.ValidationRounds()) != 3 ||
		DecideCapacity(search).Decision != Inconclusive || search.NextAttempt() != 0 {
		testContext.Fatalf("status %q rounds %+v, want three rejected rounds and no pending attempt", search.Status(), search.ValidationRounds())
	}
	for _, round := range search.ValidationRounds() {
		if len(round.Outcomes) != 1 || round.Outcomes[0] != ProbeNotDemonstrated {
			testContext.Fatalf("round %+v, want one counted not-demonstrated outcome", round)
		}
	}
}
