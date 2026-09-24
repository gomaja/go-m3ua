package main

import (
	"fmt"
	"strings"
	"testing"
)

// referenceRunJSON is a complete routed-direct cohort declaring the given
// route-reference workload, with the given sender verdict.
func referenceRunJSON(testContext *testing.T, rate int, mode, verdict string, mutate func(map[string]any)) string {
	testContext.Helper()
	// Every record gets its own copy, so a test can make them disagree.
	workload := func() map[string]any {
		if mode == "static" {
			return map[string]any{"mode": "static", "rate": 0, "peak": 0, "stable": 1000, "cycle": "static"}
		}
		return map[string]any{"mode": "churn", "rate": 1000, "peak": 1000, "stable": 1000, "cycle": "0-1-1000-0"}
	}
	return mutateBidirectionalJSON(testContext, routedRunJSON(testContext, rate, "routed-direct"), func(cohort map[string]any) {
		for _, side := range []string{"sender", "receiver"} {
			cohort[side].(map[string]any)["spec"].(map[string]any)["route_references"] = workload()
		}
		cohort["sender"].(map[string]any)["route_references"] = map[string]any{"verdict": verdict, "workload": workload()}
		if mutate != nil {
			mutate(cohort)
		}
	})
}

func referenceProbe(run string) string {
	return fmt.Sprintf(`{"initial":10,"maximum":10,"probes":[{"rate":10,"run":%s}]}`, run)
}

func TestRouteReferenceProbesFoldTheirVerdict(testContext *testing.T) {
	for _, mode := range []string{"churn", "static"} {
		for verdict, want := range map[string]string{"pass": "pass", "fail": "fail", "inconclusive": "inconclusive"} {
			testContext.Run(mode+"-"+verdict, func(testContext *testing.T) {
				status, decoded := runRequest(testContext, referenceProbe(referenceRunJSON(testContext, 10, mode, verdict, nil)))
				if status == invalidInputExitStatus || len(decoded.ProbeDecisions) != 1 || decoded.ProbeDecisions[0].Decision != want {
					testContext.Fatalf("status=%d result=%+v, want probe %s", status, decoded, want)
				}
			})
		}
	}
}

func TestRouteReferenceCampaignsCannotMix(testContext *testing.T) {
	churn := referenceRunJSON(testContext, 10, "churn", "pass", nil)
	for name, other := range map[string]string{
		"static control":      referenceRunJSON(testContext, 20, "static", "pass", nil),
		"plain routed-direct": routedRunJSON(testContext, 20, "routed-direct"),
		"other churn rate":    referenceRunJSON(testContext, 20, "churn", "pass", func(cohort map[string]any) { setReferenceField(cohort, "rate", 2000) }),
	} {
		input := fmt.Sprintf(`{"initial":10,"maximum":40,"probes":[{"rate":10,"run":%s},{"rate":20,"run":%s}]}`, churn, other)
		if status, _ := runRequest(testContext, input); status != invalidInputExitStatus {
			testContext.Errorf("%s: mixed campaign status = %d, want invalid input", name, status)
		}
	}
	control := referenceRunJSON(testContext, 10, "static", "pass", nil)
	sameControl := referenceRunJSON(testContext, 20, "static", "pass", nil)
	input := fmt.Sprintf(`{"initial":10,"maximum":40,"probes":[{"rate":10,"run":%s},{"rate":20,"run":%s}]}`, control, sameControl)
	if status, decoded := runRequest(testContext, input); status == invalidInputExitStatus {
		testContext.Fatalf("a consistent control campaign was refused: %q", decoded.Error)
	}
}

func setReferenceField(cohort map[string]any, field string, value any) {
	for _, side := range []string{"sender", "receiver"} {
		cohort[side].(map[string]any)["spec"].(map[string]any)["route_references"].(map[string]any)[field] = value
	}
	cohort["sender"].(map[string]any)["route_references"].(map[string]any)["workload"].(map[string]any)[field] = value
}

func TestRouteReferenceEvidenceIsRequiredAndConsistent(testContext *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(map[string]any)
	}{
		{name: "missing sender result", mutate: func(cohort map[string]any) { delete(cohort["sender"].(map[string]any), "route_references") }},
		{name: "missing verdict", mutate: func(cohort map[string]any) {
			delete(cohort["sender"].(map[string]any)["route_references"].(map[string]any), "verdict")
		}},
		{name: "unknown verdict", mutate: func(cohort map[string]any) {
			cohort["sender"].(map[string]any)["route_references"].(map[string]any)["verdict"] = "unavailable"
		}},
		{name: "workload differs from spec", mutate: func(cohort map[string]any) {
			cohort["sender"].(map[string]any)["route_references"].(map[string]any)["workload"].(map[string]any)["rate"] = 999
		}},
		{name: "other peak", mutate: func(cohort map[string]any) { setReferenceField(cohort, "peak", 500) }},
		{name: "zero churn rate", mutate: func(cohort map[string]any) { setReferenceField(cohort, "rate", 0) }},
		{name: "missing field", mutate: func(cohort map[string]any) {
			for _, side := range []string{"sender", "receiver"} {
				delete(cohort[side].(map[string]any)["spec"].(map[string]any)["route_references"].(map[string]any), "cycle")
			}
		}},
		{name: "receiver carries a result", mutate: func(cohort map[string]any) {
			cohort["receiver"].(map[string]any)["route_references"] = map[string]any{"verdict": "pass"}
		}},
		{name: "routed MTPTransfer mode", mutate: func(cohort map[string]any) {
			for _, side := range []string{"sender", "receiver"} {
				cohort[side].(map[string]any)["spec"].(map[string]any)["mode"] = "routed"
			}
		}},
		{name: "undeclared result", mutate: func(cohort map[string]any) {
			for _, side := range []string{"sender", "receiver"} {
				delete(cohort[side].(map[string]any)["spec"].(map[string]any), "route_references")
			}
		}},
	} {
		testContext.Run(test.name, func(testContext *testing.T) {
			status, decoded := runRequest(testContext, referenceProbe(referenceRunJSON(testContext, 10, "churn", "pass", test.mutate)))
			if status != invalidInputExitStatus {
				testContext.Fatalf("status=%d result=%+v, want invalid input", status, decoded)
			}
		})
	}
	run := strings.Replace(passingRunJSON(), `"payload":"128","mode":"throughput"`,
		`"payload":"mix","mode":"routed-direct","route_references":{"mode":"static","rate":0,"peak":0,"stable":1000,"cycle":"static"}`, 1)
	run = replaceManifestField(run, `"flow_count":32`, `"flow_count":1000`)
	if status, _ := runRequest(testContext, referenceProbe(run)); status != invalidInputExitStatus {
		testContext.Fatalf("standalone sender with route references status = %d, want invalid input", status)
	}
}
