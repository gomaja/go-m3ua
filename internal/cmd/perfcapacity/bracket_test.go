package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/gomaja/go-m3ua/internal/perfstats"
)

// Removing the bracket, reporting the hint instead of the current upper, or
// reporting a bracket without both measured bounds must fail these cases.
func TestResponseReportsSearchBracket(testContext *testing.T) {
	lower := repeatEntry(37, passingRunJSON())
	upper := repeatEntry(38, failingRunJSON())
	validated := make([]rateRun, 5)
	for index := range validated {
		validated[index] = repeatEntry(37, passingRunJSON())
	}
	for _, test := range []struct {
		name   string
		input  string
		exit   int
		status perfstats.SearchStatus
		lower  int
		upper  int
	}{
		{"issue 161 validated 37/38", warmRequestJSON(testContext, []rateRun{lower, upper}, validated, 24), passingExitStatus, perfstats.SearchBracketed, 37, 38},
		{"validation pending", warmRequestJSON(testContext, []rateRun{lower, upper}, nil, 24), inconclusiveExitStatus, perfstats.SearchBracketed, 37, 38},
		{"hint is not an upper bound", warmRequestJSON(testContext, []rateRun{lower}, nil, 24), inconclusiveExitStatus, perfstats.SearchRunning, 0, 0},
		{"no measured bounds", warmRequestJSON(testContext, nil, nil, 24), inconclusiveExitStatus, perfstats.SearchRunning, 0, 0},
		{"only upper bound", warmRequestJSON(testContext, []rateRun{repeatEntry(37, failingRunJSON())}, nil, 24), inconclusiveExitStatus, perfstats.SearchRunning, 0, 0},
		{"rejected lower clears bracket", warmRequestJSON(testContext, []rateRun{lower, upper}, []rateRun{repeatEntry(37, failingRunJSON())}, 24), inconclusiveExitStatus, perfstats.SearchRunning, 0, 0},
		{"maximum has no upper bound", `{"initial":10,"maximum":10,"probes":[{"rate":10,"run":` + passingRunJSON() + `}]}`, inconclusiveExitStatus, perfstats.SearchLowerBoundOnly, 0, 0},
		{"terminal budget exhaustion", strings.Replace(requestJSON(10, capacity37Schedule[:3], ""), `"max_probes":24`, `"max_probes":3`, 1), inconclusiveExitStatus, perfstats.SearchProbeBudgetExhausted, 20, 40},
		{"unfinished bisection", requestJSON(10, capacity37Schedule[:3], ""), inconclusiveExitStatus, perfstats.SearchRunning, 20, 40},
		{"ordinary validated search", requestJSON(10, capacity37Schedule, repetitionsJSON(37, 5, passingRunJSON())), passingExitStatus, perfstats.SearchBracketed, 37, 38},
	} {
		testContext.Run(test.name, func(testContext *testing.T) {
			var output bytes.Buffer
			if exit := run(strings.NewReader(test.input), &output); exit != test.exit {
				testContext.Fatalf("exit = %d, want %d; output = %s", exit, test.exit, &output)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(output.Bytes(), &fields); err != nil {
				testContext.Fatal(err)
			}
			var status perfstats.SearchStatus
			if raw, present := fields["search_status"]; present {
				if err := json.Unmarshal(raw, &status); err != nil {
					testContext.Fatal(err)
				}
			}
			if status != test.status {
				testContext.Fatalf("status = %q, want %q", status, test.status)
			}
			raw, present := fields["bracket"]
			if test.lower == 0 {
				if present {
					testContext.Fatalf("bracket must be absent without both bounds: %s", raw)
				}
				return
			}
			var bracket struct {
				Lower int `json:"lower"`
				Upper int `json:"upper"`
			}
			if !present {
				testContext.Fatal("response omits the search bracket")
			}
			if err := json.Unmarshal(raw, &bracket); err != nil {
				testContext.Fatal(err)
			}
			if bracket.Lower != test.lower || bracket.Upper != test.upper {
				testContext.Fatalf("bracket = %+v, want %d/%d", bracket, test.lower, test.upper)
			}
		})
	}
}

func TestResponseBracketFollowsRejectedValidation(testContext *testing.T) {
	for _, evidence := range []struct{ name, run string }{
		{"fail", failingRunJSON()}, {"not demonstrated", stalledRunJSON()},
	} {
		testContext.Run(evidence.name, func(testContext *testing.T) {
			probes := []rateRun{repeatEntry(37, passingRunJSON()), repeatEntry(38, failingRunJSON())}
			for _, rate := range []int{18, 27, 32, 34, 35, 36} {
				probes = append(probes, repeatEntry(rate, passingRunJSON()))
			}
			repetitions := []rateRun{repeatEntry(37, evidence.run)}
			for range 5 {
				repetitions = append(repetitions, repeatEntry(36, passingRunJSON()))
			}
			input := warmRequestJSON(testContext, probes, repetitions, 24)
			var output bytes.Buffer
			if exit := run(strings.NewReader(input), &output); exit != passingExitStatus {
				testContext.Fatalf("exit = %d, want pass; output = %s", exit, &output)
			}
			var result struct {
				SelectedRate int `json:"selected_rate"`
				UpperHint    int `json:"upper_hint"`
				Bracket      *struct {
					Lower int `json:"lower"`
					Upper int `json:"upper"`
				} `json:"bracket"`
			}
			if err := json.Unmarshal(output.Bytes(), &result); err != nil {
				testContext.Fatal(err)
			}
			if result.SelectedRate != 36 || result.UpperHint != 38 || result.Bracket == nil || result.Bracket.Lower != 36 || result.Bracket.Upper != 37 {
				testContext.Fatalf("response = %+v, want selected 36, hint 38 and bracket 36/37; output = %s", result, &output)
			}
		})
	}
}
