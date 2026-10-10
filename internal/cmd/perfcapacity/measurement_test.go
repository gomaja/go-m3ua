package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func withMeasurementCohort(entry rateRun, cohort string) rateRun {
	entry.Run = json.RawMessage(strings.ReplaceAll(string(entry.Run), `"cohort-a`, `"`+cohort))
	return entry
}

func TestCapacityRejectsReplayedMeasurement(testContext *testing.T) {
	legacy := repeatEntry(10, straddlingRunJSON())
	shared := rateRun{Rate: new(10), Run: json.RawMessage(bidirectionalRunJSON(10, -1, 1, -1, 0))}
	for _, scenario := range []struct {
		name   string
		first  rateRun
		second rateRun
	}{
		{"legacy window", legacy, legacy},
		{"legacy seed changed", legacy, rateRun{Rate: new(10), Run: json.RawMessage(strings.ReplaceAll(string(legacy.Run), `"seed":7`, `"seed":9`))}},
		{"legacy decision changed", legacy, repeatEntry(10, passingRunJSON())},
		{"shared-clock window", shared, shared},
		{"shared-clock seed changed", shared, rateRun{Rate: new(10), Run: json.RawMessage(strings.ReplaceAll(string(shared.Run), `"seed":7`, `"seed":9`))}},
		{"shared-clock precision changed", shared, rateRun{Rate: new(10), Run: json.RawMessage(strings.NewReplacer(
			`"resolution_ns":1`, `"resolution_ns":2`, `"maximum_lateness_ns":502`, `"maximum_lateness_ns":504`,
		).Replace(string(shared.Run)))}},
		{"shared-clock decision changed", shared, rateRun{Rate: new(10), Run: json.RawMessage(bidirectionalRunJSON(10, -1, 0, -1, 0))}},
	} {
		testContext.Run(scenario.name, func(testContext *testing.T) {
			input := request{Initial: new(10), Maximum: new(100), Probes: []rateRun{scenario.first, scenario.second}}
			status, result := runRequest(testContext, string(mustJSON(testContext, input)))
			if status != invalidInputExitStatus || !strings.Contains(result.Error, "duplicate measurement") ||
				!strings.Contains(result.Error, "probe 1") || !strings.Contains(result.Error, "rate 10") {
				testContext.Fatalf("status %d error %q, want a duplicate measurement error identifying probe 1 at rate 10", status, result.Error)
			}
		})
	}
}

func TestCapacityRejectsReplayedValidationMeasurement(testContext *testing.T) {
	base, err := decodeRequest(strings.NewReader(repeatValidationRequest(testContext, nil)))
	if err != nil {
		testContext.Fatal(err)
	}
	for _, reuseProbe := range []bool{false, true} {
		testContext.Run(fmt.Sprintf("reuse probe=%t", reuseProbe), func(testContext *testing.T) {
			input := base
			entry := withMeasurementCohort(repeatEntry(37, straddlingRunJSON()), "validation-window")
			input.Repetitions = []rateRun{entry, entry}
			if reuseProbe {
				input.Repetitions = []rateRun{base.Probes[5]}
			}
			status, result := runRequest(testContext, string(mustJSON(testContext, input)))
			if status != invalidInputExitStatus || !strings.Contains(result.Error, "duplicate measurement") {
				testContext.Fatalf("status %d error %q, want duplicate measurement", status, result.Error)
			}
		})
	}
}

func TestCapacityAllowsDistinctRepeatMeasurements(testContext *testing.T) {
	first := bidirectionalRunJSON(10, -1, 1, -1, 0)
	shifted := strings.NewReplacer(
		fmt.Sprintf(`"start_ns":%d`, bidirectionalStart), fmt.Sprintf(`"start_ns":%d`, bidirectionalStart+1_000_000_000),
		fmt.Sprintf(`"end_ns":%d`, bidirectionalEnd), fmt.Sprintf(`"end_ns":%d`, bidirectionalEnd+1_000_000_000),
		fmt.Sprintf(`"target_ns":%d`, bidirectionalEnd+2_000_000_000), fmt.Sprintf(`"target_ns":%d`, bidirectionalEnd+3_000_000_000),
		fmt.Sprintf(`"captured_ns":%d`, bidirectionalEnd+1_000_000), fmt.Sprintf(`"captured_ns":%d`, bidirectionalEnd+1_001_000_000),
	).Replace(first)
	for name, second := range map[string]string{
		"different cohort": strings.ReplaceAll(first, `"cohort-a`, `"cohort-b`),
		"different window": shifted,
	} {
		testContext.Run(name, func(testContext *testing.T) {
			input := request{Initial: new(10), Maximum: new(100), Probes: []rateRun{
				{Rate: new(10), Run: json.RawMessage(first)}, {Rate: new(10), Run: json.RawMessage(second)},
			}}
			status, result := runRequest(testContext, string(mustJSON(testContext, input)))
			if status != inconclusiveExitStatus || result.NextProbeRate != 10 || result.NextAttempt != 3 || len(result.Probes) != 0 {
				testContext.Fatalf("status %d result %+v, want two distinct undecided attempts", status, result)
			}
		})
	}
}
