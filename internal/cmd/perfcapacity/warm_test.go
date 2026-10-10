package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/gomaja/go-m3ua/internal/perfstats"
)

func TestUpperHintFindsBracketInTwoProbes(testContext *testing.T) {
	schedule := []struct {
		rate    int
		passing bool
	}{{37, true}, {38, false}}
	input := strings.Replace(requestJSON(37, schedule, repetitionsJSON(37, 5, passingRunJSON())),
		`{"initial":37`, `{"upper_hint":38,"initial":37`, 1)
	status, result := runRequest(testContext, input)
	if status != passingExitStatus || result.SearchStatus != perfstats.SearchBracketed ||
		result.SelectedRate != 37 || len(result.Probes) != 2 || len(result.ValidationRounds) != 1 ||
		len(result.ValidationRounds[0].Outcomes) != 5 {
		testContext.Fatalf("status %d result %+v, want a validated two-probe bracket at 37", status, result)
	}
	if result.UpperHint == nil || *result.UpperHint != 38 || result.UpperHintUsed == nil || !*result.UpperHintUsed {
		testContext.Fatalf("hint metadata = %+v, want 38 used", result)
	}
}

func warmRequestJSON(testContext *testing.T, probes, repetitions []rateRun, budget int) string {
	testContext.Helper()
	initial, maximum, upper := 37, 100, 38
	return string(mustJSON(testContext, request{
		Initial: &initial, Maximum: &maximum, MaxProbes: &budget, UpperHint: &upper,
		Probes: distinctMeasurementEntries(probes, "probe", 0), Repetitions: distinctMeasurementEntries(repetitions, "repetition", len(probes)),
	}))
}

func TestUpperHintSharedClockRepeatsAndValidation(testContext *testing.T) {
	for _, mode := range []string{"unidirectional", "bidirectional"} {
		testContext.Run(mode, func(testContext *testing.T) {
			entry := func(rate int, lower, upper float64) rateRun {
				evidence := bidirectionalRunJSON(rate, lower, upper, -1, 0)
				if mode == "unidirectional" {
					evidence = unidirectionalRunJSON(testContext, rate, "asp-to-sgp", lower, upper)
				}
				return rateRun{Rate: &rate, Run: json.RawMessage(evidence)}
			}
			probes := []rateRun{
				entry(37, -1, 1), entry(37, -1, 1), entry(37, -1, 0),
				entry(38, -1, 1), entry(38, -1, 1), entry(38, 1, 2),
			}
			repetitions := []rateRun{entry(37, -1, 1), entry(37, -1, 1)}
			for range 5 {
				repetitions = append(repetitions, entry(37, -1, 0))
			}
			status, result := runRequest(testContext, warmRequestJSON(testContext, probes, repetitions, 24))
			if status != passingExitStatus || result.SelectedRate != 37 || result.SearchStatus != perfstats.SearchBracketed ||
				len(result.Probes) != 2 || len(result.ProbeDecisions) != 6 || len(result.RepetitionDecisions) != 7 ||
				len(result.ValidationRounds) != 1 || len(result.ValidationRounds[0].Outcomes) != 5 ||
				result.ProbeDecisions[2].Attempt != 3 || result.ProbeDecisions[5].Attempt != 3 ||
				result.RepetitionDecisions[2].Attempt != 3 || result.UpperHintUsed == nil || !*result.UpperHintUsed {
				testContext.Fatalf("status %d result %+v, want distinct shared-clock repeats and five validated passes at 37", status, result)
			}
		})
	}
}

func TestUpperHintRejectsReplayedSharedClockWindow(testContext *testing.T) {
	for _, mode := range []string{"unidirectional", "bidirectional"} {
		for _, renamed := range []bool{false, true} {
			name := mode + "/original cohort"
			if renamed {
				name = mode + "/renamed cohort"
			}
			testContext.Run(name, func(testContext *testing.T) {
				lower := bidirectionalRunJSON(37, -1, 0, -1, 0)
				upper := bidirectionalRunJSON(38, -1, 1, -1, 0)
				if mode == "unidirectional" {
					lower = unidirectionalRunJSON(testContext, 37, "asp-to-sgp", -1, 0)
					upper = unidirectionalRunJSON(testContext, 38, "asp-to-sgp", -1, 1)
				}
				input, err := decodeRequest(strings.NewReader(warmRequestJSON(testContext, []rateRun{
					{Rate: new(37), Run: json.RawMessage(lower)},
					{Rate: new(38), Run: json.RawMessage(upper)},
				}, nil, 24)))
				if err != nil {
					testContext.Fatal(err)
				}
				replay := input.Probes[1]
				if renamed {
					replay.Run = json.RawMessage(strings.ReplaceAll(string(replay.Run), "probe-window-2", "renamed-replay"))
				}
				input.Probes = append(input.Probes, replay)
				status, result := runRequest(testContext, string(mustJSON(testContext, input)))
				if status != invalidInputExitStatus || !strings.Contains(result.Error, "duplicate measurement") ||
					!strings.Contains(result.Error, "rate 38") || !strings.Contains(result.Error, "probe 2") {
					testContext.Fatalf("status %d error %q, want the replayed hinted-rate window rejected as probe 2 at rate 38", status, result.Error)
				}
			})
		}
	}
}

func TestUpperHintPartialSearchReportsHowItWasUsed(testContext *testing.T) {
	for _, test := range []struct {
		name       string
		probes     []rateRun
		next       int
		used       bool
		probeCount int
	}{
		{"before L", nil, 37, false, 0},
		{"L passes", []rateRun{repeatEntry(37, passingRunJSON())}, 38, true, 1},
		{"L fails", []rateRun{repeatEntry(37, failingRunJSON())}, 18, false, 1},
		{"L stalls", []rateRun{repeatEntry(37, stalledRunJSON())}, 18, false, 1},
		{"U passes", []rateRun{repeatEntry(37, passingRunJSON()), repeatEntry(38, passingRunJSON())}, 76, true, 2},
		{"L straddles", []rateRun{repeatEntry(37, straddlingRunJSON())}, 37, false, 0},
		{"U straddles", []rateRun{repeatEntry(37, passingRunJSON()), repeatEntry(38, straddlingRunJSON())}, 38, true, 1},
		{"L missing evidence", []rateRun{repeatEntry(37, strings.Replace(passingRunJSON(), ","+singleWindowJSON(-2.5, -0.5), "", 1))}, 0, false, 1},
	} {
		testContext.Run(test.name, func(testContext *testing.T) {
			status, result := runRequest(testContext, warmRequestJSON(testContext, test.probes, nil, 24))
			if status != inconclusiveExitStatus || result.NextProbeRate != test.next || len(result.Probes) != test.probeCount ||
				result.UpperHint == nil || *result.UpperHint != 38 || result.UpperHintUsed == nil || *result.UpperHintUsed != test.used {
				testContext.Fatalf("status %d result %+v, want next %d, used=%t and %d counted probes", status, result, test.next, test.used, test.probeCount)
			}
			if strings.Contains(test.name, "straddles") && result.NextAttempt != 2 {
				testContext.Fatalf("next attempt %d, want 2", result.NextAttempt)
			}
		})
	}
}

func TestUpperHintNonPassingUStartsValidation(testContext *testing.T) {
	for _, evidence := range []string{failingRunJSON(), stalledRunJSON()} {
		probes := []rateRun{repeatEntry(37, passingRunJSON()), repeatEntry(38, evidence)}
		status, result := runRequest(testContext, warmRequestJSON(testContext, probes, nil, 24))
		if status != inconclusiveExitStatus || result.NextRepetitionRate != 37 || result.NextProbeRate != 0 ||
			result.SearchStatus != perfstats.SearchBracketed || result.SelectedRate != 37 || result.Reason != perfstats.RepetitionsMissingReason {
			testContext.Fatalf("status %d result %+v, want a bracket awaiting validation at 37", status, result)
		}
	}
}

func TestUpperHintRepeatsResolveBeforeBracketing(testContext *testing.T) {
	for _, upperEvidence := range []string{straddlingRunJSON(), failingRunJSON(), passingRunJSON()} {
		probes := []rateRun{
			repeatEntry(37, straddlingRunJSON()), repeatEntry(37, straddlingRunJSON()), repeatEntry(37, passingRunJSON()),
			repeatEntry(38, straddlingRunJSON()), repeatEntry(38, straddlingRunJSON()), repeatEntry(38, upperEvidence),
		}
		status, result := runRequest(testContext, warmRequestJSON(testContext, probes, nil, 24))
		if status != inconclusiveExitStatus || len(result.Probes) != 2 || len(result.ProbeDecisions) != 6 ||
			result.ProbeDecisions[0].SearchOutcome != perfstats.ProbeBacklogUndecided ||
			result.ProbeDecisions[3].SearchOutcome != perfstats.ProbeBacklogUndecided ||
			result.ProbeDecisions[2].Attempt != 3 || result.ProbeDecisions[5].Attempt != 3 || result.NextAttempt != 1 {
			testContext.Fatalf("status %d result %+v, want six attempts and two decided probes", status, result)
		}
		if upperEvidence == passingRunJSON() {
			if result.NextProbeRate != 76 || result.NextRepetitionRate != 0 {
				testContext.Fatal("passing U did not resume normal doubling")
			}
		} else if result.NextRepetitionRate != 37 || result.NextProbeRate != 0 {
			testContext.Fatal("resolved non-pass at U did not begin validation")
		}
		if upperEvidence == straddlingRunJSON() && result.Probes[1].Outcome != perfstats.ProbeNotDemonstrated {
			testContext.Fatal("third straddle did not contribute one not-demonstrated outcome")
		}
	}
	probes := []rateRun{repeatEntry(37, straddlingRunJSON()), repeatEntry(37, straddlingRunJSON()), repeatEntry(37, straddlingRunJSON())}
	status, result := runRequest(testContext, warmRequestJSON(testContext, probes, nil, 24))
	if status != inconclusiveExitStatus || result.NextProbeRate != 18 || result.UpperHintUsed == nil || *result.UpperHintUsed ||
		len(result.Probes) != 1 || result.Probes[0].Outcome != perfstats.ProbeNotDemonstrated {
		testContext.Fatalf("status %d result %+v, want normal halving after three L straddles", status, result)
	}
}

func TestUpperHintFailedValidationRejectsL(testContext *testing.T) {
	probes := []rateRun{repeatEntry(37, passingRunJSON()), repeatEntry(38, failingRunJSON())}
	repetitions := []rateRun{repeatEntry(37, passingRunJSON()), repeatEntry(37, failingRunJSON())}
	status, result := runRequest(testContext, warmRequestJSON(testContext, probes, repetitions, 24))
	if status != inconclusiveExitStatus || result.SelectedRate != 0 || result.NextProbeRate != 18 || result.NextRepetitionRate != 0 ||
		len(result.ValidationRounds) != 1 || len(result.ValidationRounds[0].Outcomes) != 2 ||
		result.ValidationRounds[0].Outcomes[1] != perfstats.ProbeFailing || result.UpperHintUsed == nil || !*result.UpperHintUsed {
		testContext.Fatalf("status %d result %+v, want rejected L and a normal probe at 18", status, result)
	}
}

func TestUpperHintInvalidInput(testContext *testing.T) {
	for _, test := range []struct{ input, reason string }{
		{`{"initial":37,"upper_hint":36}`, "initial < upper_hint"},
		{`{"initial":37,"upper_hint":37}`, "initial < upper_hint"},
		{`{"initial":37,"upper_hint":0}`, "initial < upper_hint"},
		{`{"initial":37,"upper_hint":-1}`, "initial < upper_hint"},
		{`{"initial":37,"maximum":37,"upper_hint":38}`, "upper_hint <= maximum"},
		{`{"initial":37,"upper_hint":39}`, "within five percent"},
		{`{"upper_hint":38}`, "initial is required"},
		{`{"initial":null,"upper_hint":38}`, "initial is required"},
		{`{"initial":37,"upper_hint":38.5}`, "decode request"},
		{`{"initial":37,"upper_hint":"38"}`, "decode request"},
		{`{"initial":37,"upper_hint":38,"UPPER_HINT":38}`, "duplicate"},
	} {
		status, result := runRequest(testContext, test.input)
		if status != invalidInputExitStatus || !strings.Contains(result.Error, test.reason) {
			testContext.Fatalf("input %s: status %d error %q, want invalid input containing %q", test.input, status, result.Error, test.reason)
		}
	}
}

func TestUpperHintAbsentPreservesResponseJSON(testContext *testing.T) {
	for _, input := range []string{`{"initial":37}`, `{"initial":37,"upper_hint":null}`} {
		var output bytes.Buffer
		if status := run(strings.NewReader(input), &output); status != inconclusiveExitStatus {
			testContext.Fatalf("status %d, want inconclusive", status)
		}
		var result map[string]json.RawMessage
		if err := json.Unmarshal(output.Bytes(), &result); err != nil {
			testContext.Fatal(err)
		}
		if _, present := result["upper_hint"]; present {
			testContext.Fatal("no-hint response contains hint metadata")
		}
		if _, present := result["upper_hint_used"]; present {
			testContext.Fatal("no-hint response contains hint-used metadata")
		}
		if string(result["next_probe_rate"]) != "37" {
			testContext.Fatal("no-hint request did not start at initial")
		}
	}
}
