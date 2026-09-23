package main

import (
	"fmt"
	"strings"
	"testing"

	"github.com/gomaja/go-m3ua/internal/perfstats"
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
		sender["manifest"].(map[string]any)["ssnm_budgets"] = ssnmBudgetsJSON(100_000_000)
		cohort["receiver"].(map[string]any)["ssnm"] = map[string]any{"generator": map[string]any{"state": "complete"}}
		if mutate != nil {
			mutate(cohort)
		}
	})
}

// ssnmBudgetsJSON is the ASP manifest's SSNM budget record.
func ssnmBudgetsJSON(applyP99 float64) map[string]any {
	return map[string]any{"apply_p99_ns": applyP99, "resync_ns": float64(100_000_000), "recovery_ns": float64(1_000_000_000), "scope": "contract"}
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

// setSSNMPhase declares phase in both records' spec.ssnm.
func setSSNMPhase(cohort map[string]any, phase string) {
	for _, side := range []string{"sender", "receiver"} {
		cohort[side].(map[string]any)["spec"].(map[string]any)["ssnm"].(map[string]any)["phase"] = phase
	}
}

// ssnmWarmupJSON is an SSNM-loaded warm-up cohort as perftraffic emits it
// when the warm-up ran its whole schedule and failed from overload: both
// specs declare the warm-up phase, the SGP record carries its generator view,
// and the ASP record carries no ssnm result, which perftraffic computes for
// the measurement cohort only.
func ssnmWarmupJSON(testContext *testing.T, rate int, mutate func(map[string]any)) string {
	testContext.Helper()
	return ssnmRunJSON(testContext, rate, "pass", func(cohort map[string]any) {
		cohort["phase"] = "warmup"
		cohort["verdict"] = "invalid"
		cohort["error"] = warmupOverloadError
		sender := cohort["sender"].(map[string]any)
		sender["send_duration"].(map[string]any)["max_ns"] = float64(1_030_000_000)
		delete(sender, "ssnm")
		setSSNMPhase(cohort, "warmup")
		if mutate != nil {
			mutate(cohort)
		}
	})
}

// An SSNM-loaded warm-up that failed from overload is probe evidence exactly
// like a no-update one: it bounds the bracket from above.
func TestSSNMLoadedFailedWarmupBoundsTheSearch(testContext *testing.T) {
	input := fmt.Sprintf(`{"initial":10,"maximum":20,"probes":[{"rate":10,"run":%s},{"rate":20,"run":%s}]}`,
		ssnmRunJSON(testContext, 10, "pass", nil), ssnmWarmupJSON(testContext, 20, nil))
	status, decoded := runRequest(testContext, input)
	if status == invalidInputExitStatus || decoded.Error != "" || len(decoded.ProbeDecisions) != 2 {
		testContext.Fatalf("SSNM-loaded failed warm-up rejected: status=%d result=%+v", status, decoded)
	}
	warmup := decoded.ProbeDecisions[1]
	if decoded.ProbeDecisions[0].SearchOutcome != perfstats.ProbePassing || warmup.Phase != "warmup" ||
		warmup.SearchOutcome != perfstats.ProbeNotDemonstrated || warmup.Decision == string(perfstats.Pass) ||
		decoded.SearchStatus != perfstats.SearchRunning || decoded.NextProbeRate != 15 {
		testContext.Fatalf("decisions %+v status %q next %d, want the warm-up to bound the bracket at 20 and probe 15",
			decoded.ProbeDecisions, decoded.SearchStatus, decoded.NextProbeRate)
	}
}

// The SSNM phase must be the cohort's own phase, the SGP must have run the
// generator, and a warm-up carries no SSNM result to fold.
func TestSSNMLoadedWarmupRequiresMatchingEvidence(testContext *testing.T) {
	for name, mutate := range map[string]func(map[string]any){
		"measurement phase in a warm-up": func(cohort map[string]any) { setSSNMPhase(cohort, "measurement") },
		"missing receiver generator":     func(cohort map[string]any) { delete(cohort["receiver"].(map[string]any), "ssnm") },
		"sender ssnm result": func(cohort map[string]any) {
			cohort["sender"].(map[string]any)["ssnm"] = map[string]any{"verdict": "pass", "workload": cohort["sender"].(map[string]any)["spec"].(map[string]any)["ssnm"]}
		},
		"unknown phase": func(cohort map[string]any) { setSSNMPhase(cohort, "setup") },
	} {
		testContext.Run(name, func(testContext *testing.T) {
			input := fmt.Sprintf(`{"initial":10,"maximum":20,"probes":[{"rate":10,"run":%s},{"rate":20,"run":%s}]}`,
				ssnmRunJSON(testContext, 10, "pass", nil), ssnmWarmupJSON(testContext, 20, mutate))
			status, decoded := runRequest(testContext, input)
			if status != invalidInputExitStatus || !strings.Contains(decoded.Error, "probe 2") {
				testContext.Fatalf("status %d error %q, want the warm-up rejected as probe 2 evidence", status, decoded.Error)
			}
		})
	}
}

// A failed SSNM-loaded warm-up keeps the SSNM workload identity, so it cannot
// join a no-update campaign either.
func TestSSNMLoadedWarmupCannotMixWithNoUpdateControl(testContext *testing.T) {
	input := fmt.Sprintf(`{"initial":10,"maximum":20,"probes":[{"rate":10,"run":%s},{"rate":20,"run":%s}]}`,
		unidirectionalRunJSON(testContext, 10, "asp-to-sgp", -1, 0), ssnmWarmupJSON(testContext, 20, nil))
	status, decoded := runRequest(testContext, input)
	if status != invalidInputExitStatus || !strings.Contains(decoded.Error, "workload") {
		testContext.Fatalf("SSNM warm-up joined a no-update campaign: status=%d result=%+v", status, decoded)
	}
}

// SSNM load is a direct throughput workload: a routed cohort cannot declare
// it, and an SSNM-loaded campaign cannot absorb routed or routed-direct
// probes.
func TestSSNMAndRoutedWorkloadsStayApart(testContext *testing.T) {
	for _, mode := range []string{"routed", "routed-direct"} {
		testContext.Run(mode+"/mixed campaign", func(testContext *testing.T) {
			input := fmt.Sprintf(`{"initial":10,"maximum":20,"probes":[{"rate":10,"run":%s},{"rate":20,"run":%s}]}`,
				ssnmRunJSON(testContext, 10, "pass", nil), routedRunJSON(testContext, 20, mode))
			status, decoded := runRequest(testContext, input)
			if status != invalidInputExitStatus || !strings.Contains(decoded.Error, "workload") {
				testContext.Fatalf("routed probe joined an SSNM-loaded campaign: status=%d result=%+v", status, decoded)
			}
		})
		testContext.Run(mode+"/declared load", func(testContext *testing.T) {
			run := ssnmRunJSON(testContext, 10, "pass", func(cohort map[string]any) {
				for _, side := range []string{"sender", "receiver"} {
					specification := cohort[side].(map[string]any)["spec"].(map[string]any)
					specification["mode"] = mode
					specification["payload"] = "mix"
				}
				cohort["sender"].(map[string]any)["manifest"].(map[string]any)["flow_count"] = float64(1000)
			})
			input := fmt.Sprintf(`{"initial":10,"maximum":10,"probes":[{"rate":10,"run":%s}]}`, run)
			status, decoded := runRequest(testContext, input)
			if status != invalidInputExitStatus || !strings.Contains(decoded.Error, "ssnm") {
				testContext.Fatalf("%s cohort declaring SSNM load accepted: status=%d result=%+v", mode, status, decoded)
			}
		})
	}
}

// The SSNM verdict depends on the budgets the ASP judged it against, so an
// SSNM-loaded cohort must record them and one campaign cannot mix budgets.
func TestSSNMBudgetsAreRequiredAndPartOfTheIdentity(testContext *testing.T) {
	sender := func(cohort map[string]any) map[string]any { return cohort["sender"].(map[string]any) }
	manifest := func(cohort map[string]any) map[string]any { return sender(cohort)["manifest"].(map[string]any) }
	for name, mutate := range map[string]func(map[string]any){
		"missing budgets":  func(cohort map[string]any) { delete(manifest(cohort), "ssnm_budgets") },
		"partial budgets":  func(cohort map[string]any) { delete(manifest(cohort)["ssnm_budgets"].(map[string]any), "resync_ns") },
		"zero budget":      func(cohort map[string]any) { manifest(cohort)["ssnm_budgets"].(map[string]any)["recovery_ns"] = 0 },
		"negative budget":  func(cohort map[string]any) { manifest(cohort)["ssnm_budgets"].(map[string]any)["apply_p99_ns"] = -1 },
		"stray on control": nil,
	} {
		testContext.Run(name, func(testContext *testing.T) {
			run := ssnmRunJSON(testContext, 10, "pass", mutate)
			if mutate == nil {
				run = mutateBidirectionalJSON(testContext, unidirectionalRunJSON(testContext, 10, "asp-to-sgp", -1, 0), func(cohort map[string]any) {
					manifest(cohort)["ssnm_budgets"] = ssnmBudgetsJSON(100_000_000)
				})
			}
			input := fmt.Sprintf(`{"initial":10,"maximum":10,"probes":[{"rate":10,"run":%s}]}`, run)
			if status, decoded := runRequest(testContext, input); status != invalidInputExitStatus {
				testContext.Fatalf("status = %d result = %+v, want invalid input", status, decoded)
			}
		})
	}
	loosened := ssnmRunJSON(testContext, 20, "pass", func(cohort map[string]any) {
		manifest(cohort)["ssnm_budgets"] = ssnmBudgetsJSON(200_000_000)
	})
	input := fmt.Sprintf(`{"initial":10,"maximum":40,"probes":[{"rate":10,"run":%s},{"rate":20,"run":%s}]}`, ssnmRunJSON(testContext, 10, "pass", nil), loosened)
	if status, decoded := runRequest(testContext, input); status != invalidInputExitStatus || !strings.Contains(decoded.Error, "workload") {
		testContext.Fatalf("campaign mixing SSNM budgets: status = %d result = %+v", status, decoded)
	}
}

// Bidirectional and legacy single-record evidence never carries SSNM load,
// so any ssnm evidence there is refused exactly as in a unidirectional cohort
// whose spec declares none.
func TestStraySSNMEvidenceIsRefusedOutsideLoadedCohorts(testContext *testing.T) {
	workload := map[string]any{
		"rate": 1000, "apcs": 1, "records": 16384, "subscribers": 8,
		"pause_offset_ns": 0, "pause_duration_ns": 0, "phase": "measurement", "anchor_ns": 1,
	}
	for _, side := range []string{"sender", "receiver", "reverse_sender", "reverse_receiver"} {
		testContext.Run("bidirectional "+side+" record", func(testContext *testing.T) {
			run := mutateBidirectionalJSON(testContext, bidirectionalRunJSON(10, -1, 0, -2, -1), func(cohort map[string]any) {
				cohort[side].(map[string]any)["ssnm"] = map[string]any{"verdict": "pass"}
			})
			input := fmt.Sprintf(`{"initial":10,"maximum":10,"probes":[{"rate":10,"run":%s}]}`, run)
			if status, decoded := runRequest(testContext, input); status != invalidInputExitStatus || !strings.Contains(decoded.Error, "ssnm") {
				testContext.Fatalf("stray ssnm evidence accepted: status = %d result = %+v", status, decoded)
			}
		})
	}
	// The reverse records use throughput mode, which alone would satisfy
	// the SSNM spec rules; the pair declares the load consistently.
	testContext.Run("bidirectional reverse spec", func(testContext *testing.T) {
		run := mutateBidirectionalJSON(testContext, bidirectionalRunJSON(10, -1, 0, -2, -1), func(cohort map[string]any) {
			for _, side := range []string{"reverse_sender", "reverse_receiver"} {
				cohort[side].(map[string]any)["spec"].(map[string]any)["ssnm"] = workload
			}
		})
		input := fmt.Sprintf(`{"initial":10,"maximum":10,"probes":[{"rate":10,"run":%s}]}`, run)
		if status, decoded := runRequest(testContext, input); status != invalidInputExitStatus || !strings.Contains(decoded.Error, "ssnm") {
			testContext.Fatalf("stray ssnm spec accepted: status = %d result = %+v", status, decoded)
		}
	})
	testContext.Run("bidirectional budgets", func(testContext *testing.T) {
		run := mutateBidirectionalJSON(testContext, bidirectionalRunJSON(10, -1, 0, -2, -1), func(cohort map[string]any) {
			cohort["sender"].(map[string]any)["manifest"].(map[string]any)["ssnm_budgets"] = ssnmBudgetsJSON(100_000_000)
		})
		input := fmt.Sprintf(`{"initial":10,"maximum":10,"probes":[{"rate":10,"run":%s}]}`, run)
		if status, decoded := runRequest(testContext, input); status != invalidInputExitStatus || !strings.Contains(decoded.Error, "ssnm") {
			testContext.Fatalf("stray ssnm budgets accepted: status = %d result = %+v", status, decoded)
		}
	})
	testContext.Run("legacy single record", func(testContext *testing.T) {
		run := strings.Replace(passingRunJSON(), `{"side":"sender",`, `{"side":"sender","ssnm":{"verdict":"pass"},`, 1)
		input := fmt.Sprintf(`{"initial":10,"maximum":10,"probes":[{"rate":10,"run":%s}]}`, run)
		if status, decoded := runRequest(testContext, input); status != invalidInputExitStatus || !strings.Contains(decoded.Error, "ssnm") {
			testContext.Fatalf("stray ssnm evidence accepted: status = %d result = %+v", status, decoded)
		}
	})
	testContext.Run("clean evidence still accepted", func(testContext *testing.T) {
		for _, run := range []string{bidirectionalRunJSON(10, -1, 0, -2, -1), passingRunJSON()} {
			input := fmt.Sprintf(`{"initial":10,"maximum":10,"probes":[{"rate":10,"run":%s}]}`, run)
			if status, decoded := runRequest(testContext, input); status == invalidInputExitStatus {
				testContext.Fatalf("clean evidence refused: %+v", decoded)
			}
		}
	})
}
