package main

import (
	"fmt"
	"strings"
	"testing"
)

// ssnmRunJSON is a complete unidirectional cohort under SSNM load with the
// given sender ssnm verdict.
func ssnmRunJSON(testContext *testing.T, rate int, verdict string, mutate func(map[string]any)) string {
	testContext.Helper()
	return mutateBidirectionalJSON(testContext, unidirectionalRunJSON(testContext, rate, "asp-to-sgp", -1, 0), func(cohort map[string]any) {
		sender := cohort["sender"].(map[string]any)
		start := sender["spec"].(map[string]any)["shared_clock"].(map[string]any)["start_ns"]
		workload := map[string]any{
			"rate": 1000, "apcs": 1, "records": 16384, "subscribers": 8,
			"pause_offset_ns": 0, "pause_duration_ns": 0, "phase": "measurement", "anchor_ns": start,
		}
		for _, side := range []string{"sender", "receiver"} {
			cohort[side].(map[string]any)["spec"].(map[string]any)["ssnm"] = workload
		}
		sender["ssnm"] = map[string]any{"verdict": verdict, "workload": workload}
		cohort["receiver"].(map[string]any)["ssnm"] = map[string]any{"generator": map[string]any{"state": "complete"}}
		if mutate != nil {
			mutate(cohort)
		}
	})
}

func TestSSNMLoadedProbeFoldsSSNMVerdict(testContext *testing.T) {
	for verdict, want := range map[string]string{"pass": "pass", "fail": "fail", "inconclusive": "inconclusive"} {
		testContext.Run(verdict, func(testContext *testing.T) {
			input := fmt.Sprintf(`{"initial":10,"maximum":10,"probes":[{"rate":10,"run":%s}]}`, ssnmRunJSON(testContext, 10, verdict, nil))
			status, decoded := runRequest(testContext, input)
			if status == invalidInputExitStatus || len(decoded.ProbeDecisions) != 1 || decoded.ProbeDecisions[0].Decision != want {
				testContext.Fatalf("ssnm verdict %s: status=%d result=%+v, want probe %s", verdict, status, decoded, want)
			}
		})
	}
}

func TestSSNMLoadedCampaignCannotMixWithNoUpdateControl(testContext *testing.T) {
	loaded := ssnmRunJSON(testContext, 10, "pass", nil)
	control := unidirectionalRunJSON(testContext, 20, "asp-to-sgp", -1, 0)
	input := fmt.Sprintf(`{"initial":10,"maximum":40,"probes":[{"rate":10,"run":%s},{"rate":20,"run":%s}]}`, loaded, control)
	status, _ := runRequest(testContext, input)
	if status != invalidInputExitStatus {
		testContext.Fatalf("mixed SSNM and no-update campaign status = %d, want invalid input", status)
	}
	otherIntensity := ssnmRunJSON(testContext, 20, "pass", func(cohort map[string]any) {
		for _, side := range []string{"sender", "receiver"} {
			cohort[side].(map[string]any)["spec"].(map[string]any)["ssnm"].(map[string]any)["apcs"] = 1024
		}
		cohort["sender"].(map[string]any)["ssnm"].(map[string]any)["workload"].(map[string]any)["apcs"] = 1024
	})
	input = fmt.Sprintf(`{"initial":10,"maximum":40,"probes":[{"rate":10,"run":%s},{"rate":20,"run":%s}]}`, loaded, otherIntensity)
	if status, _ := runRequest(testContext, input); status != invalidInputExitStatus {
		testContext.Fatalf("campaign mixing SSNM intensities status = %d, want invalid input", status)
	}
}

func TestSSNMLoadedCohortRequiresEvidence(testContext *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(map[string]any)
	}{
		{"missing sender ssnm", func(cohort map[string]any) { delete(cohort["sender"].(map[string]any), "ssnm") }},
		{"missing receiver generator", func(cohort map[string]any) { delete(cohort["receiver"].(map[string]any), "ssnm") }},
		{"unknown verdict", func(cohort map[string]any) {
			cohort["sender"].(map[string]any)["ssnm"].(map[string]any)["verdict"] = "maybe"
		}},
		{"workload disagrees with spec", func(cohort map[string]any) {
			cohort["sender"].(map[string]any)["ssnm"].(map[string]any)["workload"] = map[string]any{
				"rate": 999, "apcs": 1, "records": 16384, "subscribers": 8, "pause_offset_ns": 0, "pause_duration_ns": 0, "phase": "measurement", "anchor_ns": 1,
			}
		}},
		{"receiver spec disagrees", func(cohort map[string]any) {
			cohort["receiver"].(map[string]any)["spec"].(map[string]any)["ssnm"] = map[string]any{
				"rate": 999, "apcs": 1, "records": 16384, "subscribers": 8, "pause_offset_ns": 0, "pause_duration_ns": 0, "phase": "measurement", "anchor_ns": 1,
			}
		}},
		{"warm-up phase", func(cohort map[string]any) {
			for _, side := range []string{"sender", "receiver"} {
				specification := cohort[side].(map[string]any)["spec"].(map[string]any)
				workload := map[string]any{}
				for key, value := range specification["ssnm"].(map[string]any) {
					workload[key] = value
				}
				workload["phase"] = "warmup"
				specification["ssnm"] = workload
			}
		}},
		{"records not multiple", func(cohort map[string]any) {
			for _, side := range []string{"sender", "receiver"} {
				specification := cohort[side].(map[string]any)["spec"].(map[string]any)
				workload := map[string]any{}
				for key, value := range specification["ssnm"].(map[string]any) {
					workload[key] = value
				}
				workload["apcs"] = 1000
				specification["ssnm"] = workload
			}
		}},
		{"ssnm evidence without load", func(cohort map[string]any) {
			for _, side := range []string{"sender", "receiver"} {
				delete(cohort[side].(map[string]any)["spec"].(map[string]any), "ssnm")
			}
		}},
	} {
		testContext.Run(test.name, func(testContext *testing.T) {
			input := fmt.Sprintf(`{"initial":10,"maximum":10,"probes":[{"rate":10,"run":%s}]}`, ssnmRunJSON(testContext, 10, "pass", test.mutate))
			status, decoded := runRequest(testContext, input)
			if status != invalidInputExitStatus {
				testContext.Fatalf("status = %d result = %+v, want invalid input", status, decoded)
			}
		})
	}
}

func TestSSNMDecisionKeepsStallInconclusive(testContext *testing.T) {
	result := probeDecision{Decision: "inconclusive", Reason: "transport-stall"}
	applySSNMDecision(&result, "fail", true)
	if result.Decision != "inconclusive" || !strings.Contains(result.Reason, "stall") {
		testContext.Fatalf("stalled probe = %+v", result)
	}
	failed := probeDecision{Decision: "fail", Reason: "data"}
	applySSNMDecision(&failed, "inconclusive", false)
	if failed.Decision != "fail" || failed.Reason != "data" {
		testContext.Fatalf("SSNM inconclusive rescued a DATA failure: %+v", failed)
	}
}
