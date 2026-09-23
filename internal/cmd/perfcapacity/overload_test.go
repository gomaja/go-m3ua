package main

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// Every shape of overload evidence is refused before any other validation,
// including the failed warm-up path that otherwise admits a cohort as probe
// evidence: an overload trial is never a nominal capacity probe.
func TestOverloadTrialRecordsAreNeverCapacityEvidence(testContext *testing.T) {
	overloadSpec := map[string]any{
		"profile": "2x:15s,0.5x:15s", "role": "measurement", "nominal_rate": float64(10),
		"phases":              []any{map[string]any{"multiplier": "2x", "rate": float64(20), "duration_ns": float64(15e9)}},
		"request_deadline_ns": float64(2e9),
	}
	warmupSpec := map[string]any{"profile": "2x:15s,0.5x:15s", "role": "warmup", "nominal_rate": float64(10)}
	withSpec := func(sides []string, value any) func(map[string]any) {
		return func(cohort map[string]any) {
			for _, side := range sides {
				cohort[side].(map[string]any)["spec"].(map[string]any)["overload"] = value
			}
		}
	}
	withRecord := func(side string) func(map[string]any) {
		return func(cohort map[string]any) {
			cohort[side].(map[string]any)["overload"] = map[string]any{"acceptance": map[string]any{"verdict": "pass"}}
		}
	}
	for _, scenario := range []struct {
		name string
		run  func(testContext *testing.T) string
	}{
		{name: "measurement cohort", run: func(testContext *testing.T) string {
			return mutateBidirectionalJSON(testContext, unidirectionalRunJSON(testContext, 10, "asp-to-sgp", -1, 0), withSpec([]string{"sender", "receiver"}, overloadSpec))
		}},
		{name: "sender spec only", run: func(testContext *testing.T) string {
			return mutateBidirectionalJSON(testContext, unidirectionalRunJSON(testContext, 10, "asp-to-sgp", -1, 0), withSpec([]string{"sender"}, overloadSpec))
		}},
		{name: "receiver spec only", run: func(testContext *testing.T) string {
			return mutateBidirectionalJSON(testContext, unidirectionalRunJSON(testContext, 10, "asp-to-sgp", -1, 0), withSpec([]string{"receiver"}, overloadSpec))
		}},
		{name: "null identity", run: func(testContext *testing.T) string {
			return mutateBidirectionalJSON(testContext, unidirectionalRunJSON(testContext, 10, "asp-to-sgp", -1, 0), withSpec([]string{"sender"}, nil))
		}},
		{name: "sender overload object", run: func(testContext *testing.T) string {
			return mutateBidirectionalJSON(testContext, unidirectionalRunJSON(testContext, 10, "asp-to-sgp", -1, 0), withRecord("sender"))
		}},
		{name: "receiver overload object", run: func(testContext *testing.T) string {
			return mutateBidirectionalJSON(testContext, unidirectionalRunJSON(testContext, 10, "asp-to-sgp", -1, 0), withRecord("receiver"))
		}},
		{name: "failed warm-up", run: func(testContext *testing.T) string {
			return mutateBidirectionalJSON(testContext, unidirectionalRunJSON(testContext, 10, "asp-to-sgp", -1, 0), func(cohort map[string]any) {
				withSpec([]string{"sender", "receiver"}, warmupSpec)(cohort)
				cohort["phase"] = "warmup"
				cohort["verdict"] = "invalid"
				cohort["error"] = warmupOverloadError
				cohort["sender"].(map[string]any)["send_duration"].(map[string]any)["max_ns"] = float64(1_030_000_000)
			})
		}},
		{name: "bidirectional reverse record", run: func(testContext *testing.T) string {
			return mutateBidirectionalJSON(testContext, bidirectionalRunJSON(10, 10, 10, 10, 10), withSpec([]string{"reverse_sender"}, overloadSpec))
		}},
		// An SSNM-loaded cohort that also carries the overload identity is
		// refused as overload evidence before any SSNM rule is applied.
		{name: "ssnm cohort", run: func(testContext *testing.T) string {
			return ssnmRunJSON(testContext, 10, "pass", withSpec([]string{"sender", "receiver"}, overloadSpec))
		}},
		{name: "standalone sender record", run: func(*testing.T) string {
			return strings.Replace(passingRunJSON(), `"spec":{`, `"spec":{"overload":{"profile":"2x:15s,0.5x:15s","role":"measurement"},`, 1)
		}},
	} {
		testContext.Run(scenario.name, func(testContext *testing.T) {
			run := scenario.run(testContext)
			if !strings.Contains(run, `"overload"`) {
				testContext.Fatalf("fixture carries no overload identity: %s", run)
			}
			status, decoded := runRequest(testContext, fmt.Sprintf(`{"initial":10,"maximum":20,"probes":[{"rate":10,"run":%s}]}`, run))
			if status != invalidInputExitStatus || !strings.Contains(decoded.Error, "overload trial records are not capacity evidence") {
				testContext.Fatalf("status %d error %q, want the overload identity refused", status, decoded.Error)
			}
			status, decoded = runRequest(testContext, fmt.Sprintf(`{"initial":10,"maximum":10,"probes":[{"rate":10,"run":%s}],"repetitions":[{"rate":10,"run":%s}]}`, passingOrCohort(testContext, scenario.name), run))
			if status != invalidInputExitStatus || !strings.Contains(decoded.Error, "overload trial records are not capacity evidence") {
				testContext.Fatalf("repetition: status %d error %q, want the overload identity refused", status, decoded.Error)
			}
		})
	}
	// The unmodified fixtures are accepted, so the refusal is the identity's.
	for name, run := range map[string]string{
		"cohort":     unidirectionalRunJSON(testContext, 10, "asp-to-sgp", -1, 0),
		"standalone": passingRunJSON(),
	} {
		if status, decoded := runRequest(testContext, fmt.Sprintf(`{"initial":10,"maximum":20,"probes":[{"rate":10,"run":%s}]}`, run)); status == invalidInputExitStatus {
			testContext.Fatalf("%s: nominal fixture refused: %q", name, decoded.Error)
		}
	}
}

func passingOrCohort(testContext *testing.T, name string) string {
	if strings.Contains(name, "ssnm") {
		return ssnmRunJSON(testContext, 10, "pass", nil)
	}
	if strings.Contains(name, "bidirectional") {
		return bidirectionalRunJSON(10, 10, 10, 10, 10)
	}
	if strings.Contains(name, "standalone") {
		return passingRunJSON()
	}
	return unidirectionalRunJSON(testContext, 10, "asp-to-sgp", -1, 0)
}

func TestWorkloadFromSpecRefusesTheOverloadIdentity(testContext *testing.T) {
	rate := uint64(10)
	cohort, seed, associations, expected, outstanding := "c", uint64(1), 1, uint64(1200), 8192
	payload, mode, direction, initiation := "128", "throughput", "asp-to-sgp", "asp-dial"
	duration := 120 * time.Second
	spec := fixtureSpec{
		Cohort: &cohort, Seed: &seed, Associations: &associations, Expected: &expected, Rate: &rate,
		Outstanding: &outstanding, Payload: &payload, Mode: &mode, Direction: &direction, Initiation: &initiation,
	}
	spec.Duration = &duration
	if _, err := workloadFromSpec(&spec, 10); err != nil {
		testContext.Fatalf("nominal spec refused: %v", err)
	}
	spec.Overload = []byte(`{"role":"warmup"}`)
	if _, err := workloadFromSpec(&spec, 10); err == nil || !strings.Contains(err.Error(), "not capacity evidence") {
		testContext.Fatalf("overload spec: %v", err)
	}
}
