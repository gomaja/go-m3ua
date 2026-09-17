package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/gomaja/go-m3ua/internal/perfstats"
)

// capacity-37 probe schedule for initial=10, maximum=100, max_probes=24:
// 10p 20p 40f 30p 35p 37p 38f brackets [37, 38] within five percent.
var capacity37Schedule = []struct {
	rate    int
	passing bool
}{{10, true}, {20, true}, {40, false}, {30, true}, {35, true}, {37, true}, {38, false}}

// manifestJSON is one perftraffic run manifest as the fixture emits it. The
// acceptance output has to state the environment a campaign ran in, so every
// run record below carries one.
func manifestJSON() string {
	return `"manifest":{"go_version":"go1.25.4","go_os":"linux","go_arch":"amd64",` +
		`"vcs_revision":"c370d891f0f7f0c6a1f4cf1f0f6cf0c0f0f0c0f0","vcs_modified":false,` +
		`"assessed_baseline_revision":"d097e191d879efc95e36c0254814933f01aa9aee",` +
		`"sctp_module":"github.com/gomaja/go-sctp","sctp_version":"v1.0.4","gomaxprocs":4,` +
		`"sctp_nodelay":true,"sctp_sack_delay_ms":0,"sctp_sack_frequency":1,"flow_count":32,` +
		`"outstanding_limit":8192,"initiation":"asp-dial","accounting_scope":"whole-process"}`
}

// sendDurationJSON is the fixture's send-call duration summary. max_ns is the
// exact observed maximum; the percentiles are power-of-two bucket bounds.
func sendDurationJSON(maximumNanoseconds int64) string {
	return fmt.Sprintf(`"send_duration":{"count":100000,"p50_ns":32768,"p95_ns":131072,"p99_ns":262144,"max_ns":%d}`, maximumNanoseconds)
}

func passingRunJSON() string {
	return `{` + manifestJSON() + `,` + sendDurationJSON(262144) + `,"fixture_verdict":"pass","capped":0,"send_errors":0,` +
		`"delivery":{"unique":1000,"unique_measurement":1000,"unique_drain":0,"missing":0,"duplicate":0,"invalid":0,"reordered":0,"late_after_stop":0},` +
		`"sender_window":{"status":"bounded","backlog_change":{"status":"nonincrease-demonstrated","sample_count":120,"mean_change_lower":-2.5,"mean_change_upper":-0.5}}}`
}

func failingRunJSON() string {
	return `{` + manifestJSON() + `,` + sendDurationJSON(262144) + `,"fixture_verdict":"pass","capped":0,"send_errors":0,` +
		`"delivery":{"unique":1000,"unique_measurement":1000,"unique_drain":0,"missing":0,"duplicate":0,"invalid":0,"reordered":0,"late_after_stop":0},` +
		`"sender_window":{"status":"bounded","backlog_change":{"status":"increase-demonstrated","sample_count":120,"mean_change_lower":1.5,"mean_change_upper":3.5}}}`
}

func straddlingRunJSON() string {
	return `{` + manifestJSON() + `,` + sendDurationJSON(262144) + `,"fixture_verdict":"pass","capped":0,"send_errors":0,` +
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
	noWindow := `{` + manifestJSON() + `,` + sendDurationJSON(262144) + `,"fixture_verdict":"pass","capped":0,"send_errors":0,` +
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

// The reference VM intermittently stalls the SCTP transport for about a
// second. Both go-sctp v1.0.2 and v1.0.4 reproduce it, so it is environmental
// rather than a candidate regression. The fixture already records the block
// directly, as the maximum send-call duration: the offered schedule is open
// loop, so a transport block shows up first as a send call that does not
// return, and the outstanding cap refusals that follow are its consequence.
// Such a run must be reported inconclusive with the stall named and recorded,
// never dropped, never widened away, and never charged to the candidate as a
// submission failure.
func stalledRunJSON() string {
	return `{` + manifestJSON() + `,` + sendDurationJSON(1200000000) + `,` +
		`"fixture_verdict":"invalid","capped":4231,"send_errors":0,` +
		`"delivery":{"unique":95769,"unique_measurement":95769,"unique_drain":0,"missing":4231,"duplicate":0,"invalid":0,"reordered":0,"late_after_stop":0},` +
		`"sender_window":{"status":"bounded","backlog_change":{"status":"increase-demonstrated","sample_count":120,"mean_change_lower":1.5,"mean_change_upper":3.5}}}`
}

func TestDetectedTransportStallIsReportedInconclusiveAndNamed(testContext *testing.T) {
	input := fmt.Sprintf(`{"initial":10,"maximum":100,"max_probes":24,"probes":[{"rate":10,"run":%s}]}`, stalledRunJSON())
	var output bytes.Buffer
	status := run(strings.NewReader(input), &output)
	var decoded response
	if err := json.Unmarshal(output.Bytes(), &decoded); err != nil {
		testContext.Fatalf("output is not JSON: %v; output = %q", err, output.String())
	}
	if len(decoded.ProbeDecisions) != 1 {
		testContext.Fatalf("probe evidence was dropped: %+v", decoded)
	}
	probe := decoded.ProbeDecisions[0]
	if probe.Decision != "inconclusive" || probe.Reason != perfstats.TransportStallReason {
		testContext.Fatalf("probe decision = %+v, want inconclusive naming the transport stall", probe)
	}
	if !strings.Contains(output.String(), `"longest_send_ns":1200000000`) {
		testContext.Fatalf("the detected stall is missing from the report: %s", output.String())
	}
	if status != inconclusiveExitStatus {
		testContext.Fatalf("exit status = %d, want %d", status, inconclusiveExitStatus)
	}
}

func TestAcceptanceOutputStatesTheEnvironment(testContext *testing.T) {
	input := requestJSON(10, capacity37Schedule, repetitionsJSON(37, 5, passingRunJSON()))
	var output bytes.Buffer
	run(strings.NewReader(input), &output)
	for _, want := range []string{
		`"environments"`, `"go_version":"go1.25.4"`, `"go_os":"linux"`, `"go_arch":"amd64"`,
		`"gomaxprocs":4`, `"sctp_module":"github.com/gomaja/go-sctp"`, `"sctp_version":"v1.0.4"`,
	} {
		if !strings.Contains(output.String(), want) {
			testContext.Fatalf("acceptance output does not state %s: %s", want, output.String())
		}
	}
}

// A record that cannot show whether a stall contaminated it, or cannot say
// where it ran and which baseline it was assessed against, is refused rather
// than decided on the fields that happen to be present.
func TestIncompleteRunRecordsAreInvalidInput(testContext *testing.T) {
	tests := []struct {
		name string
		run  string
	}{
		{name: "no send duration", run: strings.Replace(passingRunJSON(), sendDurationJSON(262144)+",", "", 1)},
		{name: "null send duration maximum", run: strings.Replace(passingRunJSON(), `"max_ns":262144`, `"max_ns":null`, 1)},
		{name: "negative send duration maximum", run: strings.Replace(passingRunJSON(), `"max_ns":262144`, `"max_ns":-1`, 1)},
		{name: "no manifest", run: strings.Replace(passingRunJSON(), manifestJSON()+",", "", 1)},
		{name: "no assessed baseline", run: strings.Replace(passingRunJSON(), `"assessed_baseline_revision":"d097e191d879efc95e36c0254814933f01aa9aee",`, "", 1)},
		{name: "empty assessed baseline", run: strings.Replace(passingRunJSON(), `"assessed_baseline_revision":"d097e191d879efc95e36c0254814933f01aa9aee"`, `"assessed_baseline_revision":""`, 1)},
		{name: "no go version", run: strings.Replace(passingRunJSON(), `"go_version":"go1.25.4",`, "", 1)},
		{name: "no gomaxprocs", run: strings.Replace(passingRunJSON(), `"gomaxprocs":4,`, "", 1)},
		{name: "no sctp version", run: strings.Replace(passingRunJSON(), `"sctp_version":"v1.0.4",`, "", 1)},
	}
	for _, test := range tests {
		testContext.Run(test.name, func(testContext *testing.T) {
			input := fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s}]}`, test.run)
			status, decoded := runRequest(testContext, input)
			if status != invalidInputExitStatus || decoded.Decision != "invalid-input" {
				testContext.Fatalf("status %d decision %+v, want invalid-input", status, decoded)
			}
		})
	}
}

// Every run's measured longest send call reaches the report, not only the ones
// that decided a run.
func TestUnstalledRunsStillReportTheirLongestSend(testContext *testing.T) {
	input := fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s}]}`, passingRunJSON())
	var output bytes.Buffer
	run(strings.NewReader(input), &output)
	if !strings.Contains(output.String(), `"longest_send_ns":262144`) {
		testContext.Fatalf("the measured longest send is missing from the report: %s", output.String())
	}
}

// A campaign whose runs did not all come from one environment states every one
// of them rather than presenting a single environment it cannot support.
func TestDistinctEnvironmentsAreAllStated(testContext *testing.T) {
	other := strings.Replace(passingRunJSON(), `"sctp_version":"v1.0.4"`, `"sctp_version":"v1.0.2"`, 1)
	input := fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s},{"rate":20,"run":%s}]}`, passingRunJSON(), other)
	_, decoded := runRequest(testContext, input)
	if len(decoded.Environments) != 2 {
		testContext.Fatalf("environments = %+v, want both environments stated", decoded.Environments)
	}
	if decoded.Environments[0].SCTPVersion != "v1.0.4" || decoded.Environments[1].SCTPVersion != "v1.0.2" {
		testContext.Fatalf("environments = %+v, want first-appearance order", decoded.Environments)
	}
	if decoded.Environments[0].AssessedBaselineRevision != "d097e191d879efc95e36c0254814933f01aa9aee" {
		testContext.Fatalf("environment does not state the assessed baseline: %+v", decoded.Environments[0])
	}
}

// One environment repeated across every probe and repetition is stated once.
func TestOneEnvironmentIsStatedOnce(testContext *testing.T) {
	input := requestJSON(10, capacity37Schedule, repetitionsJSON(37, 5, passingRunJSON()))
	_, decoded := runRequest(testContext, input)
	if len(decoded.Environments) != 1 {
		testContext.Fatalf("environments = %+v, want exactly one", decoded.Environments)
	}
}
