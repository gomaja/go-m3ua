package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

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
		`"sctp_module":"github.com/gomaja/go-sctp","sctp_version":"v1.0.6","gomaxprocs":4,` +
		`"sctp_nodelay":true,"sctp_sack_delay_ms":0,"sctp_sack_frequency":1,"flow_count":32,` +
		`"outstanding_limit":8192,"initiation":"asp-dial","accounting_scope":"whole-process"}`
}

func specJSON(rate int) string {
	return fmt.Sprintf(`"measurement_duration_ns":120000000000,"negotiated_outbound_streams":[8,8,8,8,8,8,8,8],"spec":{"cohort":"cohort-a","seed":7,"associations":8,"expected":%d,`+
		`"duration_ns":120000000000,"drain_ns":2000000000,"rate":%d,"outstanding":8192,`+
		`"payload":"128","mode":"throughput","direction":"asp-to-sgp","initiation":"asp-dial",`+
		`"peer_control":"http://127.0.0.1:8080"},"expected":%d`, rate*120, rate, rate*120)
}

func replaceManifestField(run, old, replacement string) string {
	manifest := strings.Replace(manifestJSON(), old, replacement, 1)
	return strings.Replace(run, manifestJSON(), manifest, 1)
}

// sendDurationJSON is the fixture's send-call duration summary. max_ns is the
// exact observed maximum; the percentiles are power-of-two bucket bounds.
func sendDurationJSON(maximumNanoseconds int64) string {
	return fmt.Sprintf(`"send_duration":{"count":100000,"p50_ns":32768,"p95_ns":131072,"p99_ns":262144,"max_ns":%d}`, maximumNanoseconds)
}

// backlogWindowJSON returns the sender_window samples and backlog_trend that
// perftraffic emits for a window of the given length at rate. The per-second
// series is flat when upper <= 0, grows by one message per second when
// lower > 0, and is otherwise flat but bracketed one message wide, so the
// recomputed verdict is not-growing, growing and indeterminate respectively.
// One observation before the window and one after it exercise the in-window
// filter.
func backlogWindowJSON(rate uint64, window time.Duration, lower, upper float64) string {
	type sample struct {
		Before time.Duration `json:"before_ns"`
		After  time.Duration `json:"after_ns"`
		Unique uint64        `json:"unique"`
		Lower  uint64        `json:"backlog_lower"`
		Upper  uint64        `json:"backlog_upper"`
	}
	samples := []sample{{Before: -time.Millisecond}}
	for second := time.Second; second < window; second += time.Second {
		backlog, width := uint64(3), uint64(0)
		switch {
		case lower > 0:
			backlog += uint64(second / time.Second)
		case upper > 0:
			width = 1
		}
		offered := rate * uint64(second/time.Second)
		samples = append(samples, sample{Before: second, After: second + time.Microsecond, Unique: offered - min(offered, backlog), Lower: backlog, Upper: backlog + width})
	}
	samples = append(samples, sample{Before: window + time.Millisecond, After: window + 2*time.Millisecond, Unique: rate * uint64(window/time.Second)})
	observations := make([]perfstats.BacklogObservation, len(samples))
	for index, value := range samples {
		observations[index] = perfstats.BacklogObservation{Before: value.Before, After: value.After, Lower: value.Lower, Upper: value.Upper}
	}
	status, trend := perfstats.DescribeBacklogTrend(observations, window, rate)
	encodedSamples, err := json.Marshal(samples)
	if err != nil {
		panic(err)
	}
	encodedTrend, err := json.Marshal(struct {
		Status string `json:"status"`
		perfstats.BacklogTrend
	}{status, trend})
	if err != nil {
		panic(err)
	}
	return `"samples":` + string(encodedSamples) + `,"backlog_trend":` + string(encodedTrend)
}

func singleWindowJSON(lower, upper float64) string {
	return singleWindowAtRateJSON(10, lower, upper)
}

func singleWindowAtRateJSON(rate int, lower, upper float64) string {
	return `"sender_window":{"duration_ns":120000000000,"status":"bounded",` + backlogWindowJSON(uint64(rate), 120*time.Second, lower, upper) + `}`
}

func passingRunJSON() string {
	return `{"side":"sender",` + specJSON(10) + `,"scheduled":1200,"sent":1200,"submitted":1200,` +
		`"outstanding_at_window_start":0,"outstanding_after_drain":0,` + manifestJSON() + `,` + sendDurationJSON(262144) + `,"fixture_verdict":"pass","capped":0,"send_errors":0,` +
		`"delivery":{"unique":1200,"unique_measurement":1200,"unique_drain":0,"missing":0,"duplicate":0,"invalid":0,"reordered":0,"late_after_stop":0},` +
		singleWindowJSON(-2.5, -0.5) + `}`
}

func failingRunJSON() string {
	return `{"side":"sender",` + specJSON(10) + `,"scheduled":1200,"sent":1200,"submitted":1200,` +
		`"outstanding_at_window_start":0,"outstanding_after_drain":0,` + manifestJSON() + `,` + sendDurationJSON(262144) + `,"fixture_verdict":"pass","capped":0,"send_errors":0,` +
		`"delivery":{"unique":1200,"unique_measurement":1200,"unique_drain":0,"missing":0,"duplicate":0,"invalid":0,"reordered":0,"late_after_stop":0},` +
		singleWindowJSON(1.5, 3.5) + `}`
}

func straddlingRunJSON() string {
	return `{"side":"sender",` + specJSON(10) + `,"scheduled":1200,"sent":1200,"submitted":1200,` +
		`"outstanding_at_window_start":0,"outstanding_after_drain":0,` + manifestJSON() + `,` + sendDurationJSON(262144) + `,"fixture_verdict":"pass","capped":0,"send_errors":0,` +
		`"delivery":{"unique":1200,"unique_measurement":1200,"unique_drain":0,"missing":0,"duplicate":0,"invalid":0,"reordered":0,"late_after_stop":0},` +
		singleWindowJSON(-3.4, 3.53) + `}`
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
		fmt.Fprintf(&builder, `{"rate":%d,"run":%s}`, probe.rate, runAtRateJSON(run, probe.rate))
	}
	builder.WriteString(`],"repetitions":[`)
	builder.WriteString(repetitions)
	builder.WriteString(`]}`)
	return builder.String()
}

func repetitionsJSON(rate, count int, run string) string {
	entries := make([]string, 0, count)
	for index := 0; index < count; index++ {
		entries = append(entries, fmt.Sprintf(`{"rate":%d,"run":%s}`, rate, runAtRateJSON(run, rate)))
	}
	return strings.Join(entries, ",")
}

func runAtRateJSON(run string, rate int) string {
	run = strings.Replace(run, specJSON(10), specJSON(rate), 1)
	for _, bounds := range [][2]float64{{-2.5, -0.5}, {1.5, 3.5}, {-3.4, 3.53}} {
		run = strings.Replace(run, singleWindowJSON(bounds[0], bounds[1]), singleWindowAtRateJSON(rate, bounds[0], bounds[1]), 1)
	}
	run = strings.Replace(run, `"scheduled":1200`, fmt.Sprintf(`"scheduled":%d`, rate*120), 1)
	run = strings.Replace(run, `"sent":1200`, fmt.Sprintf(`"sent":%d`, rate*120), 1)
	run = strings.Replace(run, `"submitted":1200`, fmt.Sprintf(`"submitted":%d`, rate*120), 1)
	run = strings.Replace(run, `"unique":1200`, fmt.Sprintf(`"unique":%d`, rate*120), 1)
	return strings.Replace(run, `"unique_measurement":1200`, fmt.Sprintf(`"unique_measurement":%d`, rate*120), 1)
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

func TestWorkloadFromSpecEnforcesProducerLimits(testContext *testing.T) {
	testCases := []struct {
		name         string
		declaredRate int
		mutate       func(*fixtureSpec)
		wantError    string
	}{
		{
			name: "maximum values",
			mutate: func(spec *fixtureSpec) {
				*spec.Associations = 32
				*spec.Outstanding = 8192
			},
		},
		{
			name:         "maximum rate",
			declaredRate: 1_000_000,
			mutate: func(spec *fixtureSpec) {
				*spec.Rate = 1_000_000
				*spec.Expected = 120_000_000
			},
		},
		{
			name: "maximum duration",
			mutate: func(spec *fixtureSpec) {
				*spec.Duration = 10 * time.Minute
				*spec.Expected = 6_000
				spec.Drain = 0
			},
		},
		{
			name: "maximum combined run window",
			mutate: func(spec *fixtureSpec) {
				*spec.Duration = 2 * time.Second
				*spec.Expected = 20
				spec.Drain = 4*time.Minute + 59*time.Second
			},
		},
		{
			name: "too many associations",
			mutate: func(spec *fixtureSpec) {
				*spec.Associations = 33
			},
			wantError: "associations must not exceed 32",
		},
		{
			name:         "rate above maximum",
			declaredRate: 1_000_001,
			mutate: func(spec *fixtureSpec) {
				*spec.Rate = 1_000_001
				*spec.Expected = 120_000_120
			},
			wantError: "rate must not exceed 1000000",
		},
		{
			name: "too many outstanding sends",
			mutate: func(spec *fixtureSpec) {
				*spec.Outstanding = 8193
			},
			wantError: "outstanding must not exceed 8192",
		},
		{
			name: "duration above maximum",
			mutate: func(spec *fixtureSpec) {
				*spec.Duration = 10*time.Minute + time.Nanosecond
				*spec.Expected = 6_000
				spec.Drain = 0
			},
			wantError: "duration_ns must not exceed 10m0s",
		},
		{
			name: "drain above maximum",
			mutate: func(spec *fixtureSpec) {
				spec.Drain = 10*time.Minute + time.Nanosecond
			},
			wantError: "drain_ns must not exceed 10m0s",
		},
		{
			name: "combined run window above maximum",
			mutate: func(spec *fixtureSpec) {
				*spec.Duration = 2*time.Second + time.Nanosecond
				*spec.Expected = 20
				spec.Drain = 4*time.Minute + 59*time.Second
			},
			wantError: "duration_ns plus twice drain_ns must not exceed 10m0s",
		},
	}

	for _, testCase := range testCases {
		testContext.Run(testCase.name, func(testContext *testing.T) {
			var record fixtureEvidence
			if err := json.Unmarshal([]byte(passingRunJSON()), &record); err != nil {
				testContext.Fatal(err)
			}
			declaredRate := testCase.declaredRate
			if declaredRate == 0 {
				declaredRate = 10
			}
			testCase.mutate(record.Spec)

			_, err := workloadFromSpec(record.Spec, declaredRate)
			if testCase.wantError == "" {
				if err != nil {
					testContext.Fatalf("workloadFromSpec() error = %v, want nil", err)
				}
				return
			}
			if err == nil || err.Error() != "spec "+testCase.wantError {
				testContext.Fatalf("workloadFromSpec() error = %v, want %q", err, "spec "+testCase.wantError)
			}
		})
	}
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

// A probe that did not demonstrate its rate for a rate-related reason (here a
// backlog trend that cannot be shown not to grow) bounds the bracket from
// above, and the search continues downward.
func TestUndemonstratedProbeBoundsTheSearchFromAbove(testContext *testing.T) {
	for name, run := range map[string]string{"straddling backlog": straddlingRunJSON(), "transport stall": stalledRunJSON()} {
		testContext.Run(name, func(testContext *testing.T) {
			input := fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s}]}`, run)
			status, decoded := runRequest(testContext, input)
			if status == invalidInputExitStatus || decoded.Decision != "inconclusive" || decoded.SearchStatus != perfstats.SearchRunning {
				testContext.Fatalf("status %d decision %+v, want a running search", status, decoded)
			}
			if len(decoded.ProbeDecisions) != 1 || decoded.ProbeDecisions[0].SearchOutcome != perfstats.ProbeNotDemonstrated ||
				len(decoded.Probes) != 1 || decoded.Probes[0].Outcome != perfstats.ProbeNotDemonstrated || decoded.NextProbeRate != 5 {
				testContext.Fatalf("probe %+v next %d, want not-demonstrated at 10 and next probe 5", decoded.ProbeDecisions, decoded.NextProbeRate)
			}
		})
	}
}

// Missing evidence says nothing about the rate, so it still ends the search.
func TestEvidenceInconclusiveProbeStopsTheSearch(testContext *testing.T) {
	noWindow := strings.Replace(passingRunJSON(), ","+singleWindowJSON(-2.5, -0.5), "", 1)
	if noWindow == passingRunJSON() {
		testContext.Fatal("fixture did not remove the sender window")
	}
	input := fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s}]}`, noWindow)
	status, decoded := runRequest(testContext, input)
	if status != inconclusiveExitStatus || decoded.Decision != "inconclusive" || decoded.SearchStatus != perfstats.SearchInconclusive {
		testContext.Fatalf("status %d decision %+v, want an inconclusive search", status, decoded)
	}
	if len(decoded.Probes) != 1 || decoded.Probes[0].Outcome != perfstats.ProbeInconclusive || decoded.NextProbeRate != 0 {
		testContext.Fatalf("probes = %+v next %d, want the search to stop after one inconclusive probe", decoded.Probes, decoded.NextProbeRate)
	}
}

func TestLowerBoundOnlyIsNotACapacityPass(testContext *testing.T) {
	schedule := []struct {
		rate    int
		passing bool
	}{{50, true}, {100, true}}
	status, decoded := runRequest(testContext, requestJSON(50, schedule, ""))
	if status != inconclusiveExitStatus || decoded.Decision != "inconclusive" || decoded.SearchStatus != "lower-bound-only" ||
		decoded.NextRepetitionRate != 0 {
		testContext.Fatalf("status %d decision %+v, want inconclusive lower-bound-only without repetitions", status, decoded)
	}
	// Repetitions cannot turn it into a pass: the search never asked for them.
	status, decoded = runRequest(testContext, requestJSON(50, schedule, repetitionsJSON(100, 5, passingRunJSON())))
	if status != invalidInputExitStatus || !strings.Contains(decoded.Error, "no validation repetition is pending") {
		testContext.Fatalf("status %d decision %+v, want the unrequested repetitions refused", status, decoded)
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

// A repetition must run at exactly the rate the search selected, like a probe.
func TestRepetitionAtTheWrongRateIsInvalidInput(testContext *testing.T) {
	input := requestJSON(10, capacity37Schedule, repetitionsJSON(38, 5, passingRunJSON()))
	status, decoded := runRequest(testContext, input)
	if status != invalidInputExitStatus || !strings.Contains(decoded.Error, "repetition rate 38 does not match the selected rate 37") {
		testContext.Fatalf("status %d decision %+v, want the misplaced repetition refused", status, decoded)
	}
}

// The budget's result is the lower passing rate, never a transient peak. A
// failed repetition at 37 shows 37 was such a peak: 37 bounds the bracket from
// above, the search resumes between the highest passing probe below it (35)
// and 37, and validates what it selects there.
func TestFailingRepetitionMovesTheSearchBelowTheRate(testContext *testing.T) {
	failed := repetitionsJSON(37, 4, passingRunJSON()) + `,{"rate":37,"run":` + runAtRateJSON(failingRunJSON(), 37) + `}`
	status, decoded := runRequest(testContext, requestJSON(10, capacity37Schedule, failed))
	if status != inconclusiveExitStatus || decoded.Decision != "inconclusive" || decoded.Reason != perfstats.SearchIncompleteReason ||
		decoded.SearchStatus != perfstats.SearchRunning || decoded.NextProbeRate != 36 || decoded.NextRepetitionRate != 0 {
		testContext.Fatalf("status %d decision %+v, want the search running with next probe 36", status, decoded)
	}
	if len(decoded.ValidationRounds) != 1 || decoded.ValidationRounds[0].Rate != 37 || len(decoded.ValidationRounds[0].Outcomes) != 5 ||
		decoded.ValidationRounds[0].Outcomes[4] != perfstats.ProbeFailing || len(decoded.RepetitionDecisions) != 5 {
		testContext.Fatalf("validation rounds %+v decisions %d, want one round at 37 ending in a failure", decoded.ValidationRounds, len(decoded.RepetitionDecisions))
	}

	resumed := append(append([]struct {
		rate    int
		passing bool
	}(nil), capacity37Schedule...), struct {
		rate    int
		passing bool
	}{36, true})
	status, decoded = runRequest(testContext, requestJSON(10, resumed, failed))
	if status != inconclusiveExitStatus || decoded.Reason != perfstats.RepetitionsMissingReason || decoded.SearchStatus != perfstats.SearchBracketed ||
		decoded.SelectedRate != 36 || decoded.NextRepetitionRate != 36 || decoded.NextProbeRate != 0 {
		testContext.Fatalf("status %d decision %+v, want bracketed at 36 awaiting repetitions", status, decoded)
	}

	status, decoded = runRequest(testContext, requestJSON(10, resumed, failed+","+repetitionsJSON(36, 5, passingRunJSON())))
	if status != passingExitStatus || decoded.Decision != "pass" || decoded.SelectedRate != 36 || decoded.NextRepetitionRate != 0 {
		testContext.Fatalf("status %d decision %+v, want pass at 36", status, decoded)
	}
	if len(decoded.ValidationRounds) != 2 || decoded.ValidationRounds[1].Rate != 36 || len(decoded.ValidationRounds[1].Outcomes) != 5 ||
		len(decoded.RepetitionDecisions) != 10 || len(decoded.ProbeDecisions) != len(resumed) {
		testContext.Fatalf("response %+v, want both rounds and every run's decision", decoded)
	}
}

func TestFourRepetitionsDoNotValidate(testContext *testing.T) {
	input := requestJSON(10, capacity37Schedule, repetitionsJSON(37, 4, passingRunJSON()))
	status, decoded := runRequest(testContext, input)
	if status != inconclusiveExitStatus || decoded.Reason != perfstats.RepetitionsMissingReason || decoded.NextRepetitionRate != 37 {
		testContext.Fatalf("status %d decision %+v, want repetitions missing with the next one at 37", status, decoded)
	}
}

// Runs the search did not ask for are invalid input, never ignored: a sixth
// repetition, or a probe while a validation repetition is pending.
func TestUnrequestedRunsAreInvalidInput(testContext *testing.T) {
	status, decoded := runRequest(testContext, requestJSON(10, capacity37Schedule, repetitionsJSON(37, 6, passingRunJSON())))
	if status != invalidInputExitStatus || !strings.Contains(decoded.Error, "repetition 6: no validation repetition is pending") {
		testContext.Fatalf("status %d decision %+v, want the sixth repetition refused", status, decoded)
	}
	extra := append(append([]struct {
		rate    int
		passing bool
	}(nil), capacity37Schedule...), struct {
		rate    int
		passing bool
	}{36, true})
	status, decoded = runRequest(testContext, requestJSON(10, extra, repetitionsJSON(37, 5, passingRunJSON())))
	if status != invalidInputExitStatus || !strings.Contains(decoded.Error, "probe 8: the search did not select a probe") {
		testContext.Fatalf("status %d decision %+v, want the unselected probe refused", status, decoded)
	}
}

func TestMissingWindowEvidenceNeverPasses(testContext *testing.T) {
	noWindow := strings.Replace(passingRunJSON(), ","+singleWindowJSON(-2.5, -0.5), "", 1)
	input := fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s}]}`, noWindow)
	status, decoded := runRequest(testContext, input)
	if status != inconclusiveExitStatus || decoded.Decision != "inconclusive" {
		testContext.Fatalf("status %d decision %+v, want inconclusive for missing window evidence", status, decoded)
	}
}

// mutateRunJSON decodes one run record, applies mutate and re-encodes it.
func mutateRunJSON(testContext *testing.T, run string, mutate func(window map[string]any)) string {
	testContext.Helper()
	return mutateBidirectionalJSON(testContext, run, func(record map[string]any) {
		mutate(record["sender_window"].(map[string]any))
	})
}

func backlogTrendField(window map[string]any) map[string]any {
	return window["backlog_trend"].(map[string]any)
}

func TestInsufficientSampleBacklogTrendIsMissingEvidence(testContext *testing.T) {
	insufficient := mutateRunJSON(testContext, passingRunJSON(), func(window map[string]any) {
		window["samples"] = window["samples"].([]any)[:8]
		backlogTrendField(window)["status"] = perfstats.BacklogTrendInsufficientSamples
		for field, value := range map[string]float64{"sample_count": 7, "window_ns": 0, "floor": 0, "lag": 0, "slope_lower": 0, "slope_upper": 0, "growth_lower": 0, "growth_upper": 0} {
			backlogTrendField(window)[field] = value
		}
	})
	if _, _, err := evidenceFromFixture(json.RawMessage(insufficient), 10); err != nil {
		testContext.Fatalf("canonical insufficient-sample trend rejected: %v", err)
	}
	input := fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s}]}`, insufficient)
	status, decoded := runRequest(testContext, input)
	if status != inconclusiveExitStatus || decoded.ProbeDecisions[0].Decision != "inconclusive" {
		testContext.Fatalf("status %d decision %+v, want inconclusive probe", status, decoded)
	}
}

// The evaluator recomputes the trend from the producer's own observations; a
// reported status or bound those observations do not support is rejected, so a
// producer cannot pass a run by relabelling it.
func TestBacklogTrendMustMatchItsSamples(testContext *testing.T) {
	setTrend := func(field string, value any) func(map[string]any) {
		return func(window map[string]any) { backlogTrendField(window)[field] = value }
	}
	testCases := []struct {
		name   string
		run    string
		mutate func(map[string]any)
	}{
		{name: "growing label on a flat series", run: passingRunJSON(), mutate: setTrend("status", "growing")},
		{name: "indeterminate label on a flat series", run: passingRunJSON(), mutate: setTrend("status", "indeterminate")},
		{name: "unknown label on a flat series", run: passingRunJSON(), mutate: setTrend("status", "unknown")},
		{name: "empty label on a flat series", run: passingRunJSON(), mutate: setTrend("status", "")},
		{name: "insufficient label with enough samples", run: passingRunJSON(), mutate: setTrend("status", perfstats.BacklogTrendInsufficientSamples)},
		{name: "not-growing label on a growing series", run: failingRunJSON(), mutate: setTrend("status", "not-growing")},
		{name: "indeterminate label on a growing series", run: failingRunJSON(), mutate: setTrend("status", "indeterminate")},
		{name: "growing label on an uncertain series", run: straddlingRunJSON(), mutate: setTrend("status", "growing")},
		{name: "not-growing label on an uncertain series", run: straddlingRunJSON(), mutate: setTrend("status", "not-growing")},
		{name: "understated upper growth bound", run: straddlingRunJSON(), mutate: setTrend("growth_upper", float64(0))},
		{name: "overstated lower growth bound", run: passingRunJSON(), mutate: setTrend("growth_lower", float64(1))},
		{name: "raised floor", run: failingRunJSON(), mutate: setTrend("floor", float64(1000))},
		{name: "altered slope bound", run: failingRunJSON(), mutate: setTrend("slope_lower", float64(0))},
		{name: "altered sample count", run: passingRunJSON(), mutate: setTrend("sample_count", float64(120))},
		{name: "altered window", run: passingRunJSON(), mutate: setTrend("window_ns", float64(60000000000))},
		{name: "altered lag", run: passingRunJSON(), mutate: setTrend("lag", float64(3))},
		{name: "flattened growing samples", run: failingRunJSON(), mutate: func(window map[string]any) {
			for _, value := range window["samples"].([]any) {
				value.(map[string]any)["backlog_lower"], value.(map[string]any)["backlog_upper"] = float64(3), float64(3)
			}
		}},
		{name: "dropped samples", run: passingRunJSON(), mutate: func(window map[string]any) {
			window["samples"] = window["samples"].([]any)[:60]
		}},
	}
	for _, testCase := range testCases {
		testContext.Run(testCase.name, func(testContext *testing.T) {
			run := mutateRunJSON(testContext, testCase.run, testCase.mutate)
			if _, _, err := evidenceFromFixture(json.RawMessage(run), 10); err == nil {
				testContext.Fatal("evidenceFromFixture accepted a backlog_trend its samples contradict")
			}
		})
	}
}

func TestCanonicalAndUnavailableBacklogTrendEvidence(testContext *testing.T) {
	for _, testCase := range []struct {
		name         string
		run          string
		wantTrend    bool
		wantDecision perfstats.Decision
	}{
		{name: "not growing", run: passingRunJSON(), wantTrend: true, wantDecision: perfstats.Pass},
		{name: "growing", run: failingRunJSON(), wantTrend: true, wantDecision: perfstats.Fail},
		{name: "indeterminate", run: straddlingRunJSON(), wantTrend: true, wantDecision: perfstats.Inconclusive},
		{
			name: "missing trend object",
			run: mutateRunJSON(testContext, passingRunJSON(), func(window map[string]any) {
				delete(window, "backlog_trend")
			}),
			wantDecision: perfstats.Inconclusive,
		},
		{
			name: "missing trend bound",
			run: mutateRunJSON(testContext, passingRunJSON(), func(window map[string]any) {
				delete(backlogTrendField(window), "growth_upper")
			}),
			wantDecision: perfstats.Inconclusive,
		},
		{
			name: "missing sample bound",
			run: mutateRunJSON(testContext, passingRunJSON(), func(window map[string]any) {
				delete(window["samples"].([]any)[5].(map[string]any), "backlog_upper")
			}),
			wantDecision: perfstats.Inconclusive,
		},
	} {
		testContext.Run(testCase.name, func(testContext *testing.T) {
			evidence, _, err := evidenceFromFixture(json.RawMessage(testCase.run), 10)
			if err != nil {
				testContext.Fatal(err)
			}
			if (evidence.Trend != nil) != testCase.wantTrend {
				testContext.Fatalf("trend = %+v, want presence %t", evidence.Trend, testCase.wantTrend)
			}
			if decision := perfstats.DecideRun(evidence); decision.Decision != testCase.wantDecision {
				testContext.Fatalf("decision = %+v, want %q", decision, testCase.wantDecision)
			}
		})
	}
}

func TestLossCountersFailEvenWithCleanTrend(testContext *testing.T) {
	lossy := strings.Replace(passingRunJSON(), `"fixture_verdict":"pass"`, `"fixture_verdict":"invalid"`, 1)
	lossy = strings.Replace(lossy, `"unique":1200`, `"unique":1199`, 1)
	lossy = strings.Replace(lossy, `"unique_measurement":1200`, `"unique_measurement":1199`, 1)
	lossy = strings.Replace(lossy, `"missing":0`, `"missing":1`, 1)
	input := fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s}]}`, lossy)
	status, decoded := runRequest(testContext, input)
	if status != inconclusiveExitStatus || decoded.ProbeDecisions[0].Decision != "fail" {
		testContext.Fatalf("status %d decision %+v, want failing probe leading to an inconclusive incomplete search", status, decoded)
	}
}

func TestEchoCapAndDeadlineCountersAreLosses(testContext *testing.T) {
	echoRun := strings.Replace(echoRunJSON(), `"deadline_exceeded":0`, `"deadline_exceeded":2`, 1)
	echoRun = strings.Replace(echoRun, `"validated":1200`, `"validated":1198`, 1)
	echoRun = strings.Replace(echoRun, `"fixture_verdict":"pass"`, `"fixture_verdict":"invalid"`, 1)
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
		{name: "case-insensitive duplicate key", input: `{"initial":10,"Initial":11}`},
		{name: "case-insensitive nested duplicate key", input: fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s}]}`,
			strings.Replace(passingRunJSON(), `"rate":10`, `"rate":999999,"Rate":10`, 1))},
		{name: "unicode-folded nested duplicate key", input: fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s}]}`,
			strings.Replace(passingRunJSON(), `"side":"sender"`, `"side":"receiver","\u017fide":"sender"`, 1))},
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

func TestFoldJSONKeyMatchesEncodingJSONUnicodeFolding(testContext *testing.T) {
	for _, pair := range [][2]string{{"ſide", "side"}, {"K", "K"}} {
		if foldJSONKey(pair[0]) != foldJSONKey(pair[1]) {
			testContext.Fatalf("foldJSONKey(%q) = %q, want the same canonical key as %q (%q)",
				pair[0], foldJSONKey(pair[0]), pair[1], foldJSONKey(pair[1]))
		}
	}
}

func FuzzRunNeverPanicsAndAlwaysWritesJSON(fuzzContext *testing.F) {
	fuzzContext.Add([]byte(`{}`))
	fuzzContext.Add(fmt.Appendf(nil, `{"initial":10,"probes":[{"rate":10,"run":%s}]}`, passingRunJSON()))
	fuzzContext.Add(fmt.Appendf(nil, `{"initial":10,"probes":[{"rate":10,"run":%s}]}`, bidirectionalRunJSON(10, -1, 0, -1, 0)))
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
	return `{"side":"sender",` + specJSON(10) + `,"scheduled":1200,"sent":1000,"submitted":1000,` +
		`"outstanding_at_window_start":0,"outstanding_after_drain":0,` + manifestJSON() + `,` + sendDurationJSON(1200000000) + `,` +
		`"fixture_verdict":"invalid","capped":200,"send_errors":0,` +
		`"delivery":{"unique":1000,"unique_measurement":1000,"unique_drain":0,"missing":200,"duplicate":0,"invalid":0,"reordered":0,"late_after_stop":0},` +
		singleWindowJSON(1.5, 3.5) + `}`
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
		`"gomaxprocs":4`, `"sctp_module":"github.com/gomaja/go-sctp"`, `"sctp_version":"v1.0.6"`,
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
		{name: "dirty candidate", run: replaceManifestField(passingRunJSON(), `"vcs_modified":false`, `"vcs_modified":true`)},
		{name: "no candidate revision", run: strings.Replace(passingRunJSON(), `"vcs_revision":"c370d891f0f7f0c6a1f4cf1f0f6cf0c0f0f0c0f0",`, "", 1)},
		{name: "empty candidate revision", run: strings.Replace(passingRunJSON(), `"vcs_revision":"c370d891f0f7f0c6a1f4cf1f0f6cf0c0f0f0c0f0"`, `"vcs_revision":""`, 1)},
		{name: "no assessed baseline", run: strings.Replace(passingRunJSON(), `"assessed_baseline_revision":"d097e191d879efc95e36c0254814933f01aa9aee",`, "", 1)},
		{name: "empty assessed baseline", run: strings.Replace(passingRunJSON(), `"assessed_baseline_revision":"d097e191d879efc95e36c0254814933f01aa9aee"`, `"assessed_baseline_revision":""`, 1)},
		{name: "no go version", run: strings.Replace(passingRunJSON(), `"go_version":"go1.25.4",`, "", 1)},
		{name: "empty go version", run: replaceManifestField(passingRunJSON(), `"go_version":"go1.25.4"`, `"go_version":""`)},
		{name: "empty go os", run: replaceManifestField(passingRunJSON(), `"go_os":"linux"`, `"go_os":""`)},
		{name: "empty go arch", run: replaceManifestField(passingRunJSON(), `"go_arch":"amd64"`, `"go_arch":""`)},
		{name: "no gomaxprocs", run: strings.Replace(passingRunJSON(), `"gomaxprocs":4,`, "", 1)},
		{name: "zero gomaxprocs", run: replaceManifestField(passingRunJSON(), `"gomaxprocs":4`, `"gomaxprocs":0`)},
		{name: "empty sctp module", run: replaceManifestField(passingRunJSON(), `"sctp_module":"github.com/gomaja/go-sctp"`, `"sctp_module":""`)},
		{name: "no sctp version", run: strings.Replace(passingRunJSON(), `"sctp_version":"v1.0.6",`, "", 1)},
		{name: "empty sctp version", run: replaceManifestField(passingRunJSON(), `"sctp_version":"v1.0.6"`, `"sctp_version":""`)},
		{name: "no socket option", run: replaceManifestField(passingRunJSON(), `"sctp_nodelay":true,`, "")},
		{name: "no sack delay", run: replaceManifestField(passingRunJSON(), `"sctp_sack_delay_ms":0,`, "")},
		{name: "no sack frequency", run: replaceManifestField(passingRunJSON(), `"sctp_sack_frequency":1,`, "")},
		{name: "no flow count", run: replaceManifestField(passingRunJSON(), `"flow_count":32,`, "")},
		{name: "zero flow count", run: replaceManifestField(passingRunJSON(), `"flow_count":32`, `"flow_count":0`)},
		{name: "no outstanding limit", run: replaceManifestField(passingRunJSON(), `"outstanding_limit":8192,`, "")},
		{name: "zero outstanding limit", run: replaceManifestField(passingRunJSON(), `"outstanding_limit":8192`, `"outstanding_limit":0`)},
		{name: "empty initiation", run: replaceManifestField(passingRunJSON(), `"initiation":"asp-dial"`, `"initiation":""`)},
		{name: "empty accounting scope", run: replaceManifestField(passingRunJSON(), `"accounting_scope":"whole-process"`, `"accounting_scope":""`)},
		{name: "no measured rate", run: strings.Replace(passingRunJSON(), `"rate":10,`, "", 1)},
		{name: "null measured rate", run: strings.Replace(passingRunJSON(), `"rate":10`, `"rate":null`, 1)},
		{name: "no sender side", run: strings.Replace(passingRunJSON(), `"side":"sender",`, "", 1)},
		{name: "no scheduled count", run: strings.Replace(passingRunJSON(), `"scheduled":1200,`, "", 1)},
		{name: "no sent count", run: strings.Replace(passingRunJSON(), `"sent":1200,`, "", 1)},
		{name: "no submitted count", run: strings.Replace(passingRunJSON(), `"submitted":1200,`, "", 1)},
		{name: "no start outstanding count", run: strings.Replace(passingRunJSON(), `"outstanding_at_window_start":0,`, "", 1)},
		{name: "no final outstanding count", run: strings.Replace(passingRunJSON(), `"outstanding_after_drain":0,`, "", 1)},
		{name: "no measurement delivery count", run: strings.Replace(passingRunJSON(), `"unique_measurement":1200,`, "", 1)},
		{name: "no drain delivery count", run: strings.Replace(passingRunJSON(), `"unique_drain":0,`, "", 1)},
		{name: "inconsistent expected", run: strings.Replace(passingRunJSON(), `"expected":1200`, `"expected":1201`, 1)},
		{name: "no workload", run: strings.Replace(passingRunJSON(), `,"payload":"128"`, "", 1)},
		{name: "empty workload", run: strings.Replace(passingRunJSON(), `"payload":"128"`, `"payload":""`, 1)},
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

func TestOuterRateMustMatchMeasuredFixtureRate(testContext *testing.T) {
	run := strings.Replace(passingRunJSON(), `"rate":10`, `"rate":999999`, 1)
	input := fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s}]}`, run)
	status, decoded := runRequest(testContext, input)
	if status != invalidInputExitStatus || decoded.Decision != "invalid-input" || !strings.Contains(decoded.Error, "declared rate") {
		testContext.Fatalf("status %d decision %+v, want invalid-input", status, decoded)
	}
}

func TestCampaignIdentityMustRemainStable(testContext *testing.T) {
	tests := []struct {
		name      string
		run       string
		wantError string
	}{
		{
			name:      "candidate revision",
			wantError: "candidate vcs_revision",
			run: strings.Replace(runAtRateJSON(passingRunJSON(), 20),
				`"vcs_revision":"c370d891f0f7f0c6a1f4cf1f0f6cf0c0f0f0c0f0"`,
				`"vcs_revision":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"`, 1),
		},
		{
			name:      "assessed baseline revision",
			wantError: "assessed_baseline_revision",
			run: strings.Replace(runAtRateJSON(passingRunJSON(), 20),
				`"assessed_baseline_revision":"d097e191d879efc95e36c0254814933f01aa9aee"`,
				`"assessed_baseline_revision":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"`, 1),
		},
		{
			name:      "go arch",
			wantError: "run environment",
			run:       replaceManifestField(runAtRateJSON(passingRunJSON(), 20), `"go_arch":"amd64"`, `"go_arch":"arm64"`),
		},
		{
			name:      "workload",
			wantError: "workload",
			run:       strings.Replace(runAtRateJSON(passingRunJSON(), 20), `"payload":"128"`, `"payload":"4096"`, 1),
		},
		{name: "associations", wantError: "workload", run: strings.Replace(runAtRateJSON(passingRunJSON(), 20), `"associations":8`, `"associations":4`, 1)},
		{name: "duration", wantError: "workload", run: strings.Replace(strings.Replace(runAtRateJSON(passingRunJSON(), 20), `"duration_ns":120000000000`, `"duration_ns":60000000000`, 1), `"expected":2400`, `"expected":1200`, 1)},
		{name: "drain", wantError: "workload", run: strings.Replace(runAtRateJSON(passingRunJSON(), 20), `"drain_ns":2000000000`, `"drain_ns":3000000000`, 1)},
		{name: "outstanding", wantError: "workload", run: strings.Replace(runAtRateJSON(passingRunJSON(), 20), `"outstanding":8192`, `"outstanding":4096`, 1)},
		{name: "mode", wantError: "workload", run: echoRunAtRateJSON(20)},
		{name: "direction", wantError: "workload", run: strings.Replace(runAtRateJSON(passingRunJSON(), 20), `"direction":"asp-to-sgp"`, `"direction":"sgp-to-asp"`, 1)},
		{name: "spec initiation", wantError: "workload", run: strings.Replace(runAtRateJSON(passingRunJSON(), 20), specJSON(20), strings.Replace(specJSON(20), `"initiation":"asp-dial"`, `"initiation":"sgp-dial"`, 1), 1)},
		{name: "peer control", wantError: "workload", run: strings.Replace(runAtRateJSON(passingRunJSON(), 20), `"peer_control":"http://127.0.0.1:8080"`, `"peer_control":"http://127.0.0.1:8081"`, 1)},
		{name: "nodelay", wantError: "run environment", run: replaceManifestField(runAtRateJSON(passingRunJSON(), 20), `"sctp_nodelay":true`, `"sctp_nodelay":false`)},
		{name: "sack delay", wantError: "run environment", run: replaceManifestField(runAtRateJSON(passingRunJSON(), 20), `"sctp_sack_delay_ms":0`, `"sctp_sack_delay_ms":20`)},
		{name: "sack frequency", wantError: "run environment", run: replaceManifestField(runAtRateJSON(passingRunJSON(), 20), `"sctp_sack_frequency":1`, `"sctp_sack_frequency":2`)},
		{name: "flow count", wantError: "run environment", run: replaceManifestField(runAtRateJSON(passingRunJSON(), 20), `"flow_count":32`, `"flow_count":16`)},
		{name: "outstanding limit", wantError: "run environment", run: replaceManifestField(runAtRateJSON(passingRunJSON(), 20), `"outstanding_limit":8192`, `"outstanding_limit":4096`)},
		{name: "manifest initiation", wantError: "run environment", run: replaceManifestField(runAtRateJSON(passingRunJSON(), 20), `"initiation":"asp-dial"`, `"initiation":"sgp-dial"`)},
		{name: "accounting scope", wantError: "run environment", run: replaceManifestField(runAtRateJSON(passingRunJSON(), 20), `"accounting_scope":"whole-process"`, `"accounting_scope":"sender-only"`)},
	}
	for _, test := range tests {
		testContext.Run(test.name, func(testContext *testing.T) {
			input := fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s},{"rate":20,"run":%s}]}`,
				passingRunJSON(), test.run)
			status, decoded := runRequest(testContext, input)
			if status != invalidInputExitStatus || decoded.Decision != "invalid-input" || !strings.Contains(decoded.Error, test.wantError) {
				testContext.Fatalf("status %d decision %+v, want invalid-input", status, decoded)
			}
		})
	}
}

func TestCampaignAllowsPerRunCohortSeedRateAndExpected(testContext *testing.T) {
	second := runAtRateJSON(passingRunJSON(), 20)
	second = strings.Replace(second, `"cohort":"cohort-a"`, `"cohort":"cohort-b"`, 1)
	second = strings.Replace(second, `"seed":7`, `"seed":9`, 1)
	input := fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s},{"rate":20,"run":%s}]}`, passingRunJSON(), second)
	status, decoded := runRequest(testContext, input)
	if status == invalidInputExitStatus || decoded.Decision == "invalid-input" {
		testContext.Fatalf("status %d decision %+v, want cohort, seed, rate and derived expected to vary", status, decoded)
	}
}

func TestRepetitionMustMatchCampaignIdentity(testContext *testing.T) {
	tests := []struct {
		name      string
		run       string
		wantError string
	}{
		{name: "workload", run: strings.Replace(strings.Replace(runAtRateJSON(passingRunJSON(), 37), `"duration_ns":120000000000`, `"duration_ns":60000000000`, 1), `"expected":4440`, `"expected":2220`, 1), wantError: "workload"},
		{name: "environment", run: replaceManifestField(runAtRateJSON(passingRunJSON(), 37), `"flow_count":32`, `"flow_count":16`), wantError: "run environment"},
	}
	for _, test := range tests {
		testContext.Run(test.name, func(testContext *testing.T) {
			repetitions := repetitionsJSON(37, 4, passingRunJSON()) + `,{"rate":37,"run":` + test.run + `}`
			input := requestJSON(10, capacity37Schedule, repetitions)
			status, decoded := runRequest(testContext, input)
			if status != invalidInputExitStatus || decoded.Decision != "invalid-input" || !strings.Contains(decoded.Error, test.wantError) {
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

// One environment repeated across every probe and repetition is stated once.
func TestOneEnvironmentIsStatedOnce(testContext *testing.T) {
	input := requestJSON(10, capacity37Schedule, repetitionsJSON(37, 5, passingRunJSON()))
	_, decoded := runRequest(testContext, input)
	if len(decoded.Environments) != 1 {
		testContext.Fatalf("environments = %+v, want exactly one", decoded.Environments)
	}
}

// A campaign driver runs the search one probe at a time: every partial probe
// list must name the rate the search selects next, and a terminated search
// must name none, so the driver cannot run a probe the search did not select.
func TestPartialSearchNamesItsNextProbeRate(testContext *testing.T) {
	for completed := 0; completed <= len(capacity37Schedule); completed++ {
		input := requestJSON(10, capacity37Schedule[:completed], "")
		status, decoded := runRequest(testContext, input)
		if status == invalidInputExitStatus {
			testContext.Fatalf("%d probes: invalid input: %+v", completed, decoded)
		}
		if completed < len(capacity37Schedule) {
			if want := capacity37Schedule[completed].rate; decoded.NextProbeRate != want || decoded.SearchStatus != perfstats.SearchRunning {
				testContext.Fatalf("%d probes: next_probe_rate %d status %q, want %d while running", completed, decoded.NextProbeRate, decoded.SearchStatus, want)
			}
			continue
		}
		if decoded.NextProbeRate != 0 || decoded.SearchStatus != perfstats.SearchBracketed || decoded.SelectedRate != 37 {
			testContext.Fatalf("terminated search: next_probe_rate %d status %q selected %d", decoded.NextProbeRate, decoded.SearchStatus, decoded.SelectedRate)
		}
	}
}

// A repetition that did not demonstrate the selected rate only because of a
// transport stall is decided like such a probe: the rate is rejected and the
// search moves below it, rather than validation ending inconclusive.
func TestStalledRepetitionMovesTheSearchBelowTheRate(testContext *testing.T) {
	// stalledRunJSON at 37 msg/s: the same 200 capped and missing messages,
	// every other counter scaled to the rate's 4,440 scheduled.
	stalledRun := runAtRateJSON(stalledRunJSON(), 37)
	for _, field := range []string{"sent", "submitted", "unique", "unique_measurement"} {
		stalledRun = strings.Replace(stalledRun, fmt.Sprintf(`"%s":1000`, field), fmt.Sprintf(`"%s":%d`, field, 37*120-200), 1)
	}
	stalled := repetitionsJSON(37, 2, passingRunJSON()) + `,{"rate":37,"run":` + stalledRun + `}`
	status, decoded := runRequest(testContext, requestJSON(10, capacity37Schedule, stalled))
	if status != inconclusiveExitStatus || decoded.NextProbeRate != 36 || len(decoded.RepetitionDecisions) != 3 ||
		decoded.RepetitionDecisions[2].SearchOutcome != perfstats.ProbeNotDemonstrated ||
		len(decoded.ValidationRounds) != 1 || decoded.ValidationRounds[0].Outcomes[2] != perfstats.ProbeNotDemonstrated {
		testContext.Fatalf("status %d decision %+v, want the stalled repetition to reject 37 and the next probe at 36", status, decoded)
	}
}
