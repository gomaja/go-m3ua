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

// shiftSharedClockWindow gives a synthetic run a separate absolute window.
// Watchdogs, receiver boundaries and SSNM anchors move with it; backlog
// samples remain relative to the window and retain their measured trend.
func shiftSharedClockWindow(evidence string, index int) string {
	offset := int64(index) * (bidirectionalDuration + 2_000_000_000)
	return strings.NewReplacer(
		fmt.Sprintf(`"start_ns":%d`, bidirectionalStart), fmt.Sprintf(`"start_ns":%d`, bidirectionalStart+offset),
		fmt.Sprintf(`"end_ns":%d`, bidirectionalEnd), fmt.Sprintf(`"end_ns":%d`, bidirectionalEnd+offset),
		fmt.Sprintf(`"before_ns":%d`, bidirectionalStart-2_000_000_000), fmt.Sprintf(`"before_ns":%d`, bidirectionalStart-2_000_000_000+offset),
		fmt.Sprintf(`"after_ns":%d`, bidirectionalStart-2_000_000_000+500), fmt.Sprintf(`"after_ns":%d`, bidirectionalStart-2_000_000_000+500+offset),
		fmt.Sprintf(`"target_ns":%d`, bidirectionalEnd+2_000_000_000), fmt.Sprintf(`"target_ns":%d`, bidirectionalEnd+2_000_000_000+offset),
		fmt.Sprintf(`"captured_ns":%d`, bidirectionalEnd+1_000_000), fmt.Sprintf(`"captured_ns":%d`, bidirectionalEnd+1_000_000+offset),
		fmt.Sprintf(`"anchor_ns":%d`, bidirectionalStart), fmt.Sprintf(`"anchor_ns":%d`, bidirectionalStart+offset),
	).Replace(evidence)
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

func TestCapacityRejectsRenamedSharedClockWindow(testContext *testing.T) {
	for name, evidence := range map[string]string{
		"unidirectional": unidirectionalRunJSON(testContext, 10, "asp-to-sgp", -1, 1),
		"bidirectional":  bidirectionalRunJSON(10, -1, 1, -1, 0),
	} {
		testContext.Run(name, func(testContext *testing.T) {
			entry := rateRun{Rate: new(10), Run: json.RawMessage(evidence)}
			input := request{Initial: new(10), Maximum: new(100), Probes: []rateRun{
				entry, withMeasurementCohort(entry, "renamed-b"), withMeasurementCohort(entry, "renamed-c"),
			}}
			status, result := runRequest(testContext, string(mustJSON(testContext, input)))
			if status != invalidInputExitStatus || !strings.Contains(result.Error, "duplicate measurement") ||
				!strings.Contains(result.Error, "probe 1") {
				testContext.Fatalf("status %d result %+v, want renamed copies of the same clock window rejected", status, result)
			}
		})
	}
}

func TestCapacityRejectsSharedClockWindowAtAnotherRateOrPhase(testContext *testing.T) {
	for name, second := range map[string]string{
		"rate changed":  routedRunJSON(testContext, 20, "routed"),
		"phase changed": routedWarmupJSON(testContext, 20, "routed", warmupOverloadError),
	} {
		testContext.Run(name, func(testContext *testing.T) {
			input := request{Initial: new(10), Maximum: new(100), Probes: []rateRun{
				{Rate: new(10), Run: json.RawMessage(routedRunJSON(testContext, 10, "routed"))},
				withMeasurementCohort(rateRun{Rate: new(20), Run: json.RawMessage(second)}, "renamed-b"),
			}}
			status, result := runRequest(testContext, string(mustJSON(testContext, input)))
			if status != invalidInputExitStatus || !strings.Contains(result.Error, "duplicate measurement") ||
				!strings.Contains(result.Error, "probe 1") {
				testContext.Fatalf("status %d result %+v, want the same absolute window rejected across rates and phases", status, result)
			}
		})
	}
}

func TestCapacityRejectsRenamedSharedClockValidationWindow(testContext *testing.T) {
	var probes []rateRun
	for index, probe := range capacity37Schedule {
		lower, upper := -1.0, 0.0
		if !probe.passing {
			lower, upper = 1, 2
		}
		probes = append(probes, rateRun{Rate: new(probe.rate), Run: json.RawMessage(
			shiftSharedClockWindow(bidirectionalRunJSON(probe.rate, lower, upper, lower, upper), index),
		)})
	}
	repetition := rateRun{Rate: new(37), Run: json.RawMessage(shiftSharedClockWindow(bidirectionalRunJSON(37, -1, 1, -1, 0), len(probes)))}
	for name, repetitions := range map[string][]rateRun{
		"probe reused":      {withMeasurementCohort(probes[5], "renamed-validation")},
		"repetition reused": {repetition, withMeasurementCohort(repetition, "renamed-validation")},
	} {
		testContext.Run(name, func(testContext *testing.T) {
			input := request{Initial: new(10), Maximum: new(100), Probes: probes, Repetitions: repetitions}
			status, result := runRequest(testContext, string(mustJSON(testContext, input)))
			if status != invalidInputExitStatus || !strings.Contains(result.Error, "duplicate measurement") ||
				!strings.Contains(result.Error, "repetition") {
				testContext.Fatalf("status %d result %+v, want renamed validation replay rejected", status, result)
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
	shifted := shiftSharedClockWindow(first, 1)
	legacy := repeatEntry(10, straddlingRunJSON())
	for _, scenario := range []struct {
		name   string
		first  rateRun
		second rateRun
	}{
		{"same cohort with different window", rateRun{Rate: new(10), Run: json.RawMessage(first)}, rateRun{Rate: new(10), Run: json.RawMessage(shifted)}},
		{"renamed cohort with different window", rateRun{Rate: new(10), Run: json.RawMessage(first)}, rateRun{Rate: new(10), Run: json.RawMessage(strings.ReplaceAll(shifted, `"cohort-a`, `"cohort-b`))}},
		{"legacy different cohort", legacy, withMeasurementCohort(legacy, "cohort-b")},
	} {
		testContext.Run(scenario.name, func(testContext *testing.T) {
			input := request{Initial: new(10), Maximum: new(100), Probes: []rateRun{
				scenario.first, scenario.second,
			}}
			status, result := runRequest(testContext, string(mustJSON(testContext, input)))
			if status != inconclusiveExitStatus || result.NextProbeRate != 10 || result.NextAttempt != 3 || len(result.Probes) != 0 {
				testContext.Fatalf("status %d result %+v, want two distinct undecided attempts", status, result)
			}
		})
	}
}
