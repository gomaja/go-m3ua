package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/gomaja/go-m3ua/internal/perfstats"
)

func repeatEntry(rate int, evidence string) rateRun {
	if evidence == stalledRunJSON() {
		// Use a loss-free run with an observed stall so that the existing rate
		// helper can scale its complete scheduling and delivery counters.
		evidence = strings.Replace(passingRunJSON(), sendDurationJSON(262144), sendDurationJSON(1_030_000_000), 1)
	}
	return rateRun{Rate: &rate, Run: json.RawMessage(runAtRateJSON(evidence, rate))}
}

func repeatRequest(testContext *testing.T, probes, repetitions []rateRun, budget int) string {
	testContext.Helper()
	initial, maximum := 10, 100
	return string(mustJSON(testContext, request{
		Initial: &initial, Maximum: &maximum, MaxProbes: &budget, Probes: probes, Repetitions: repetitions,
	}))
}

// A straddling window cannot shrink the bracket or spend a probe before the
// rate has a decided outcome, or all three attempts have straddled.
func TestBacklogRepeatProbeOutcomes(testContext *testing.T) {
	missing := strings.Replace(passingRunJSON(), ","+singleWindowJSON(-2.5, -0.5), "", 1)
	for _, scenario := range []struct {
		name    string
		runs    []string
		next    int
		count   int
		outcome perfstats.ProbeOutcome
	}{
		{"one straddle", []string{straddlingRunJSON()}, 10, 0, ""},
		{"two straddles", []string{straddlingRunJSON(), straddlingRunJSON()}, 10, 0, ""},
		{"straddle then pass", []string{straddlingRunJSON(), passingRunJSON()}, 20, 1, perfstats.ProbePassing},
		{"straddle then fail", []string{straddlingRunJSON(), failingRunJSON()}, 5, 1, perfstats.ProbeFailing},
		{"straddle then stall", []string{straddlingRunJSON(), stalledRunJSON()}, 5, 1, perfstats.ProbeNotDemonstrated},
		{"three straddles", []string{straddlingRunJSON(), straddlingRunJSON(), straddlingRunJSON()}, 5, 1, perfstats.ProbeNotDemonstrated},
		{"two straddles then pass", []string{straddlingRunJSON(), straddlingRunJSON(), passingRunJSON()}, 20, 1, perfstats.ProbePassing},
		{"straddle then missing", []string{straddlingRunJSON(), missing}, 0, 1, perfstats.ProbeInconclusive},
		{"stall immediately", []string{stalledRunJSON()}, 5, 1, perfstats.ProbeNotDemonstrated},
		{"failure immediately", []string{failingRunJSON()}, 5, 1, perfstats.ProbeFailing},
	} {
		testContext.Run(scenario.name, func(testContext *testing.T) {
			var probes []rateRun
			for _, evidence := range scenario.runs {
				probes = append(probes, repeatEntry(10, evidence))
			}
			status, result := runRequest(testContext, repeatRequest(testContext, probes, nil, 24))
			if status != inconclusiveExitStatus || result.NextProbeRate != scenario.next || len(result.Probes) != scenario.count ||
				len(result.ProbeDecisions) != len(scenario.runs) {
				testContext.Fatalf("status %d result %+v, want next %d, %d counted probes and %d decisions", status, result, scenario.next, scenario.count, len(scenario.runs))
			}
			if scenario.count != 0 && result.Probes[0].Outcome != scenario.outcome {
				testContext.Fatalf("probes %+v, want %q", result.Probes, scenario.outcome)
			}
		})
	}
}

func TestBacklogRepeatCannotBeSkippedOrExtended(testContext *testing.T) {
	for _, scenario := range []struct {
		name        string
		probes      []rateRun
		repetitions []rateRun
	}{
		{"skip to lower rate", []rateRun{repeatEntry(10, straddlingRunJSON()), repeatEntry(5, passingRunJSON())}, nil},
		{"skip to higher rate", []rateRun{repeatEntry(10, straddlingRunJSON()), repeatEntry(20, passingRunJSON())}, nil},
		{"skip to validation", []rateRun{repeatEntry(10, straddlingRunJSON())}, []rateRun{repeatEntry(10, passingRunJSON())}},
		{"fourth attempt", []rateRun{repeatEntry(10, straddlingRunJSON()), repeatEntry(10, straddlingRunJSON()), repeatEntry(10, straddlingRunJSON()), repeatEntry(10, passingRunJSON())}, nil},
		{"malformed repeat", []rateRun{repeatEntry(10, straddlingRunJSON()), {Rate: new(10), Run: json.RawMessage(`{}`)}}, nil},
		{"replayed earlier probe", []rateRun{repeatEntry(10, passingRunJSON()), repeatEntry(20, straddlingRunJSON()), repeatEntry(10, passingRunJSON())}, nil},
	} {
		testContext.Run(scenario.name, func(testContext *testing.T) {
			status, result := runRequest(testContext, repeatRequest(testContext, scenario.probes, scenario.repetitions, 24))
			if status != invalidInputExitStatus || result.Error == "" {
				testContext.Fatalf("status %d result %+v, want invalid input", status, result)
			}
		})
	}
}

func TestBacklogRepeatDoesNotSpendProbeBudget(testContext *testing.T) {
	for attempts := 1; attempts <= 3; attempts++ {
		probes := make([]rateRun, attempts)
		for index := range probes {
			probes[index] = repeatEntry(10, straddlingRunJSON())
		}
		_, result := runRequest(testContext, repeatRequest(testContext, probes, nil, 1))
		if attempts < 3 {
			if result.NextProbeRate != 10 || len(result.Probes) != 0 {
				testContext.Fatalf("%d attempts: %+v, want repeat at 10 without a counted probe", attempts, result)
			}
		} else if result.SearchStatus != perfstats.SearchProbeBudgetExhausted || result.NextProbeRate != 0 || len(result.Probes) != 1 {
			testContext.Fatalf("three attempts: %+v, want exhausted one-probe budget", result)
		}
	}
}

func repeatValidationRequest(testContext *testing.T, repetitions []rateRun) string {
	testContext.Helper()
	decoded, err := decodeRequest(strings.NewReader(requestJSON(10, capacity37Schedule, "")))
	if err != nil {
		testContext.Fatal(err)
	}
	decoded.Repetitions = repetitions
	return string(mustJSON(testContext, decoded))
}

func TestBacklogRepeatValidationCountsOnlyResolvedRepetitions(testContext *testing.T) {
	var repetitions []rateRun
	for repetition := 0; repetition < 5; repetition++ {
		for attempt := 0; attempt < 2; attempt++ {
			repetitions = append(repetitions, repeatEntry(37, straddlingRunJSON()))
			status, result := runRequest(testContext, repeatValidationRequest(testContext, repetitions))
			count := 0
			if len(result.ValidationRounds) != 0 {
				count = len(result.ValidationRounds[0].Outcomes)
			}
			if status != inconclusiveExitStatus || result.NextRepetitionRate != 37 || result.NextProbeRate != 0 || count != repetition ||
				result.NextAttempt != attempt+2 || result.RepetitionDecisions[len(repetitions)-1].Attempt != attempt+1 {
				testContext.Fatalf("repetition %d attempt %d: %+v, want %d counted and next repetition 37", repetition+1, attempt+1, result, repetition)
			}
		}
		repetitions = append(repetitions, repeatEntry(37, passingRunJSON()))
	}
	status, result := runRequest(testContext, repeatValidationRequest(testContext, repetitions))
	if status != passingExitStatus || result.SelectedRate != 37 || len(result.ValidationRounds) != 1 ||
		len(result.ValidationRounds[0].Outcomes) != 5 || len(result.RepetitionDecisions) != 15 || result.NextRepetitionRate != 0 ||
		result.NextAttempt != 0 || result.MaxAttempts != 0 || result.RepeatReason != "" {
		testContext.Fatalf("status %d result %+v, want five counted passes from 15 runs", status, result)
	}
}

func TestBacklogRepeatCannotClaimAnAttemptInInput(testContext *testing.T) {
	input := repeatRequest(testContext, []rateRun{repeatEntry(10, straddlingRunJSON()), repeatEntry(10, passingRunJSON())}, nil, 24)
	for _, extra := range []string{`"attempt":3,`, `"search_outcome":"pass",`, `"repeat_reason":"",`} {
		mutated := strings.Replace(input, `"rate":10,`, extra+`"rate":10,`, 1)
		status, result := runRequest(testContext, mutated)
		if status != invalidInputExitStatus || result.Error == "" {
			testContext.Fatalf("accepted client-supplied decision field %s: status %d result %+v", extra, status, result)
		}
	}
}

func TestBacklogRepeatBidirectionalEligibility(testContext *testing.T) {
	straddleAndStall := strings.Replace(bidirectionalRunJSON(10, -1, 1, -1, 0), `"max_ns":262144`, `"max_ns":1030000000`, 1)
	for _, scenario := range []struct {
		name  string
		run   string
		next  int
		count int
	}{
		{"forward straddle", bidirectionalRunJSON(10, -1, 1, -1, 0), 10, 0},
		{"reverse straddle", bidirectionalRunJSON(10, -1, 0, -1, 1), 10, 0},
		{"both straddle", bidirectionalRunJSON(10, -1, 1, -1, 1), 10, 0},
		{"failure and straddle", bidirectionalRunJSON(10, 1, 2, -1, 1), 5, 1},
		{"straddle and stall", straddleAndStall, 5, 1},
	} {
		testContext.Run(scenario.name, func(testContext *testing.T) {
			status, result := runRequest(testContext, repeatRequest(testContext, []rateRun{{Rate: new(10), Run: json.RawMessage(scenario.run)}}, nil, 24))
			if status != inconclusiveExitStatus || result.NextProbeRate != scenario.next || len(result.Probes) != scenario.count ||
				len(result.ProbeDecisions) != 1 || len(result.ProbeDecisions[0].Directions) != 2 {
				testContext.Fatalf("status %d result %+v, want next %d and %d counted", status, result, scenario.next, scenario.count)
			}
		})
	}
}

func TestBacklogRepeatValidationRejectsOrEndsAtFirstResolvedOutcome(testContext *testing.T) {
	missing := strings.Replace(passingRunJSON(), ","+singleWindowJSON(-2.5, -0.5), "", 1)
	for _, scenario := range []struct {
		name    string
		runs    []string
		next    int
		outcome perfstats.ProbeOutcome
	}{
		{"failure", []string{straddlingRunJSON(), failingRunJSON()}, 36, perfstats.ProbeFailing},
		{"stall", []string{straddlingRunJSON(), stalledRunJSON()}, 36, perfstats.ProbeNotDemonstrated},
		{"exhausted repeats", []string{straddlingRunJSON(), straddlingRunJSON(), straddlingRunJSON()}, 36, perfstats.ProbeNotDemonstrated},
		{"missing", []string{straddlingRunJSON(), missing}, 0, perfstats.ProbeInconclusive},
	} {
		testContext.Run(scenario.name, func(testContext *testing.T) {
			repetitions := []rateRun{repeatEntry(37, passingRunJSON())}
			for _, evidence := range scenario.runs {
				repetitions = append(repetitions, repeatEntry(37, evidence))
			}
			status, result := runRequest(testContext, repeatValidationRequest(testContext, repetitions))
			if status != inconclusiveExitStatus || result.NextRepetitionRate != 0 || result.NextProbeRate != scenario.next ||
				len(result.ValidationRounds) != 1 || len(result.ValidationRounds[0].Outcomes) != 2 || result.ValidationRounds[0].Outcomes[1] != scenario.outcome {
				testContext.Fatalf("status %d result %+v, want one pass then %q and next %d", status, result, scenario.outcome, scenario.next)
			}
		})
	}
}

func TestBacklogRepeatReportsAttemptAndReason(testContext *testing.T) {
	probes := []rateRun{repeatEntry(10, straddlingRunJSON()), repeatEntry(10, straddlingRunJSON())}
	_, result := runRequest(testContext, repeatRequest(testContext, probes, nil, 24))
	if result.NextAttempt != 3 || result.MaxAttempts != 3 || result.RepeatReason != perfstats.BacklogUnresolvedReason {
		testContext.Fatalf("pending attempt %d of %d, reason %q", result.NextAttempt, result.MaxAttempts, result.RepeatReason)
	}
	for index, decision := range result.ProbeDecisions {
		if decision.Attempt != index+1 || decision.MaxAttempts != 3 || decision.RepeatReason != perfstats.BacklogUnresolvedReason ||
			decision.SearchOutcome != perfstats.ProbeBacklogUndecided {
			testContext.Fatalf("attempt %d: %+v, want a deferred backlog-only outcome", index+1, decision)
		}
	}
	probes = append(probes, repeatEntry(10, straddlingRunJSON()))
	_, result = runRequest(testContext, repeatRequest(testContext, probes, nil, 24))
	last := result.ProbeDecisions[2]
	if last.Attempt != 3 || last.MaxAttempts != 3 || last.SearchOutcome != perfstats.ProbeNotDemonstrated || last.RepeatReason != "" ||
		last.Reason != perfstats.BacklogUnresolvedReason || result.NextAttempt != 1 || result.RepeatReason != "" {
		testContext.Fatalf("exhausted attempt %+v, next %d reason %q", last, result.NextAttempt, result.RepeatReason)
	}
	probes = append(probes, repeatEntry(5, straddlingRunJSON()))
	_, result = runRequest(testContext, repeatRequest(testContext, probes, nil, 24))
	if result.NextProbeRate != 5 || result.NextAttempt != 2 || result.ProbeDecisions[3].Attempt != 1 {
		testContext.Fatalf("new probe did not reset attempts: %+v", result)
	}
}

func TestBacklogRepeatValidationCannotSkipPendingAttempt(testContext *testing.T) {
	decoded, err := decodeRequest(strings.NewReader(repeatValidationRequest(testContext, []rateRun{repeatEntry(37, straddlingRunJSON())})))
	if err != nil {
		testContext.Fatal(err)
	}
	decoded.Probes = append(decoded.Probes, repeatEntry(36, passingRunJSON()))
	status, result := runRequest(testContext, string(mustJSON(testContext, decoded)))
	if status != invalidInputExitStatus || result.Error == "" {
		testContext.Fatalf("status %d result %+v, want skipped validation attempt rejected", status, result)
	}
}
