package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

// capacity-37 probe schedule for initial=10, maximum=100, max_probes=24:
// 10p 20p 40f 30p 35p 37p 38f brackets [37, 38] within five percent.
var capacity37Schedule = []struct {
	rate    int
	passing bool
}{{10, true}, {20, true}, {40, false}, {30, true}, {35, true}, {37, true}, {38, false}}

func passingRunJSON() string {
	return `{"fixture_verdict":"pass","capped":0,"send_errors":0,` +
		`"delivery":{"unique":1000,"unique_measurement":1000,"unique_drain":0,"missing":0,"duplicate":0,"invalid":0,"reordered":0,"late_after_stop":0},` +
		`"sender_window":{"status":"bounded","backlog_change":{"status":"nonincrease-demonstrated","sample_count":120,"mean_change_lower":-2.5,"mean_change_upper":-0.5}}}`
}

func failingRunJSON() string {
	return `{"fixture_verdict":"pass","capped":0,"send_errors":0,` +
		`"delivery":{"unique":1000,"unique_measurement":1000,"unique_drain":0,"missing":0,"duplicate":0,"invalid":0,"reordered":0,"late_after_stop":0},` +
		`"sender_window":{"status":"bounded","backlog_change":{"status":"increase-demonstrated","sample_count":120,"mean_change_lower":1.5,"mean_change_upper":3.5}}}`
}

func straddlingRunJSON() string {
	return `{"fixture_verdict":"pass","capped":0,"send_errors":0,` +
		`"delivery":{"unique":1000,"unique_measurement":1000,"unique_drain":0,"missing":0,"duplicate":0,"invalid":0,"reordered":0,"late_after_stop":0},` +
		`"sender_window":{"status":"bounded","backlog_change":{"status":"unresolved","sample_count":120,"mean_change_lower":-3.4,"mean_change_upper":3.53}}}`
}

func requestJSON(initial int, schedule []struct {
	rate    int
	passing bool
}, repetitions string,
) string {
	var builder strings.Builder
	fmt.Fprintf(&builder, `{"initial":%d,"maximum":100,"max_probes":24,"probes":[`, initial)
	for index, probe := range schedule {
		if index > 0 {
			builder.WriteByte(',')
		}
		run := failingRunJSON()
		if probe.passing {
			run = passingRunJSON()
		}
		fmt.Fprintf(&builder, `{"rate":%d,"run":%s}`, probe.rate, run)
	}
	builder.WriteString(`],"repetitions":[`)
	builder.WriteString(repetitions)
	builder.WriteString(`]}`)
	return builder.String()
}

func repetitionsJSON(rate, count int, run string) string {
	entries := make([]string, 0, count)
	for index := 0; index < count; index++ {
		entries = append(entries, fmt.Sprintf(`{"rate":%d,"run":%s}`, rate, run))
	}
	return strings.Join(entries, ",")
}

func runRequest(testContext *testing.T, input string) (int, response) {
	testContext.Helper()
	var output bytes.Buffer
	status := run(strings.NewReader(input), &output)
	var decoded response
	if err := json.Unmarshal(output.Bytes(), &decoded); err != nil {
		testContext.Fatalf("output is not JSON: %v; output = %q", err, output.String())
	}
	return status, decoded
}

func TestBracketedSearchWithFivePassingRepetitionsPasses(testContext *testing.T) {
	input := requestJSON(10, capacity37Schedule, repetitionsJSON(37, 5, passingRunJSON()))
	status, decoded := runRequest(testContext, input)
	if status != passingExitStatus || decoded.Decision != "pass" {
		testContext.Fatalf("status %d decision %+v, want pass", status, decoded)
	}
	if decoded.SearchStatus != "bracketed" || decoded.SelectedRate != 37 {
		testContext.Fatalf("search = %q selected %d, want bracketed at 37", decoded.SearchStatus, decoded.SelectedRate)
	}
	if len(decoded.Probes) != len(capacity37Schedule) || len(decoded.ProbeDecisions) != len(capacity37Schedule) || len(decoded.RepetitionDecisions) != 5 {
		testContext.Fatalf("response dropped probe or repetition evidence: %+v", decoded)
	}
	for index, probe := range capacity37Schedule {
		if decoded.Probes[index].Rate != probe.rate {
			testContext.Fatalf("probe %d rate = %d, want %d", index, decoded.Probes[index].Rate, probe.rate)
		}
	}
}

func TestProbeRateDeviationIsInvalidInput(testContext *testing.T) {
	schedule := append([]struct {
		rate    int
		passing bool
	}{{10, true}, {21, true}}, capacity37Schedule[2:]...)
	input := requestJSON(10, schedule, repetitionsJSON(37, 5, passingRunJSON()))
	status, decoded := runRequest(testContext, input)
	if status != invalidInputExitStatus || decoded.Decision != "invalid-input" {
		testContext.Fatalf("status %d decision %+v, want invalid-input", status, decoded)
	}
}

func TestNoPassingRateFails(testContext *testing.T) {
	input := requestJSON(2, []struct {
		rate    int
		passing bool
	}{{2, false}, {1, false}}, "")
	status, decoded := runRequest(testContext, input)
	if status != failingExitStatus || decoded.Decision != "fail" || decoded.SearchStatus != "no-passing-rate" {
		testContext.Fatalf("status %d decision %+v, want fail with no-passing-rate", status, decoded)
	}
}

func TestInconclusiveProbeStopsTheSearch(testContext *testing.T) {
	input := fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s}]}`, straddlingRunJSON())
	status, decoded := runRequest(testContext, input)
	if status != inconclusiveExitStatus || decoded.Decision != "inconclusive" || decoded.SearchStatus != "inconclusive" {
		testContext.Fatalf("status %d decision %+v, want inconclusive", status, decoded)
	}
	if len(decoded.Probes) != 1 {
		testContext.Fatalf("probes = %+v, want the search to stop after one inconclusive probe", decoded.Probes)
	}
}

func TestLowerBoundOnlyIsNotACapacityPass(testContext *testing.T) {
	input := requestJSON(50, []struct {
		rate    int
		passing bool
	}{{50, true}, {100, true}}, repetitionsJSON(100, 5, passingRunJSON()))
	status, decoded := runRequest(testContext, input)
	if status != inconclusiveExitStatus || decoded.Decision != "inconclusive" || decoded.SearchStatus != "lower-bound-only" {
		testContext.Fatalf("status %d decision %+v, want inconclusive lower-bound-only", status, decoded)
	}
}

func TestBracketedSearchWithoutRepetitionsIsSearchOnly(testContext *testing.T) {
	input := requestJSON(10, capacity37Schedule, "")
	status, decoded := runRequest(testContext, input)
	if status != inconclusiveExitStatus || decoded.Decision != "inconclusive" || decoded.Reason != "validation-repetitions-missing" {
		testContext.Fatalf("status %d decision %+v, want inconclusive missing repetitions", status, decoded)
	}
	if decoded.SelectedRate != 37 {
		testContext.Fatalf("selected rate = %d, want 37", decoded.SelectedRate)
	}
}

func TestRepetitionAtTheWrongRateDoesNotValidate(testContext *testing.T) {
	input := requestJSON(10, capacity37Schedule, repetitionsJSON(38, 5, passingRunJSON()))
	status, decoded := runRequest(testContext, input)
	if status != inconclusiveExitStatus || !strings.HasPrefix(decoded.Reason, "validation-repetition-rate-mismatch") {
		testContext.Fatalf("status %d decision %+v, want rate-mismatch inconclusive", status, decoded)
	}
}

func TestFailingRepetitionFailsTheCapacity(testContext *testing.T) {
	runs := repetitionsJSON(37, 4, passingRunJSON()) + `,{"rate":37,"run":` + failingRunJSON() + `}`
	input := requestJSON(10, capacity37Schedule, runs)
	status, decoded := runRequest(testContext, input)
	if status != failingExitStatus || decoded.Decision != "fail" {
		testContext.Fatalf("status %d decision %+v, want fail", status, decoded)
	}
}

func TestFourRepetitionsDoNotValidate(testContext *testing.T) {
	input := requestJSON(10, capacity37Schedule, repetitionsJSON(37, 4, passingRunJSON()))
	status, decoded := runRequest(testContext, input)
	if status != inconclusiveExitStatus || decoded.Reason != "validation-repetition-count-mismatch" {
		testContext.Fatalf("status %d decision %+v, want count-mismatch inconclusive", status, decoded)
	}
}

func TestMissingWindowEvidenceNeverPasses(testContext *testing.T) {
	noWindow := `{"fixture_verdict":"pass","capped":0,"send_errors":0,` +
		`"delivery":{"unique":1000,"missing":0,"duplicate":0,"invalid":0,"reordered":0,"late_after_stop":0}}`
	input := fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s}]}`, noWindow)
	status, decoded := runRequest(testContext, input)
	if status != inconclusiveExitStatus || decoded.Decision != "inconclusive" {
		testContext.Fatalf("status %d decision %+v, want inconclusive for missing window evidence", status, decoded)
	}
}

func TestInsufficientSampleBacklogChangeIsMissingEvidence(testContext *testing.T) {
	insufficient := strings.Replace(passingRunJSON(), `"status":"nonincrease-demonstrated","sample_count":120`, `"status":"insufficient-samples","sample_count":4`, 1)
	input := fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s}]}`, insufficient)
	status, decoded := runRequest(testContext, input)
	if status != inconclusiveExitStatus || decoded.ProbeDecisions[0].Decision != "inconclusive" {
		testContext.Fatalf("status %d decision %+v, want inconclusive probe", status, decoded)
	}
}

func TestLossCountersFailEvenWithCleanInterval(testContext *testing.T) {
	lossy := strings.Replace(passingRunJSON(), `"missing":0`, `"missing":1`, 1)
	input := fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s}]}`, lossy)
	status, decoded := runRequest(testContext, input)
	if status != inconclusiveExitStatus || decoded.ProbeDecisions[0].Decision != "fail" {
		testContext.Fatalf("status %d decision %+v, want failing probe leading to an inconclusive incomplete search", status, decoded)
	}
}

func TestEchoCapAndDeadlineCountersAreLosses(testContext *testing.T) {
	echoRun := strings.Replace(passingRunJSON(), `"sender_window"`, `"echo":{"capped":0,"deadline_exceeded":2},"sender_window"`, 1)
	input := fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s}]}`, echoRun)
	_, decoded := runRequest(testContext, input)
	if decoded.ProbeDecisions[0].Decision != "fail" {
		testContext.Fatalf("decision %+v, want fail for echo deadline failures", decoded.ProbeDecisions[0])
	}
}

func TestRejectsMalformedRequests(testContext *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{name: "empty object", input: `{}`},
		{name: "null initial", input: `{"initial":null}`},
		{name: "unknown field", input: `{"initial":10,"unexpected":true}`},
		{name: "trailing value", input: `{"initial":10}{}`},
		{name: "duplicate key", input: `{"initial":10,"initial":11}`},
		{name: "zero rate", input: fmt.Sprintf(`{"initial":10,"probes":[{"rate":0,"run":%s}]}`, passingRunJSON())},
		{name: "missing run", input: `{"initial":10,"probes":[{"rate":10}]}`},
		{name: "missing verdict", input: `{"initial":10,"probes":[{"rate":10,"run":{"capped":0}}]}`},
		{name: "initial above maximum", input: `{"initial":200,"maximum":100}`},
		{name: "maximum overflows the search arithmetic", input: `{"initial":1,"maximum":9223372036854775807}`},
		{name: "negative probe budget", input: `{"initial":10,"max_probes":-1}`},
		{name: "fractional rate", input: fmt.Sprintf(`{"initial":10,"probes":[{"rate":10.5,"run":%s}]}`, passingRunJSON())},
	}
	for _, test := range tests {
		testContext.Run(test.name, func(testContext *testing.T) {
			status, decoded := runRequest(testContext, test.input)
			if status != invalidInputExitStatus || decoded.Decision != "invalid-input" {
				testContext.Fatalf("status %d decision %+v, want invalid-input", status, decoded)
			}
		})
	}
}

func FuzzRunNeverPanicsAndAlwaysWritesJSON(fuzzContext *testing.F) {
	fuzzContext.Add([]byte(`{}`))
	fuzzContext.Add([]byte(fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s}]}`, passingRunJSON())))
	fuzzContext.Add([]byte(`{"initial":10,"probes":null,"repetitions":null}`))
	fuzzContext.Add([]byte{0xff, 0x00, '{', '}'})
	fuzzContext.Fuzz(func(testContext *testing.T, input []byte) {
		var output bytes.Buffer
		status := run(bytes.NewReader(input), &output)
		if status < passingExitStatus || status > invalidInputExitStatus {
			testContext.Fatalf("run() status = %d, want %d..%d", status, passingExitStatus, invalidInputExitStatus)
		}
		if !json.Valid(output.Bytes()) {
			testContext.Fatalf("output is not JSON: %q", output.String())
		}
	})
}
