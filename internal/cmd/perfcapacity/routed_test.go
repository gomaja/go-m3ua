package main

import (
	"fmt"
	"strings"
	"testing"
)

// routedRunJSON is one complete shared-clock cohort of the optional-router
// workload as perftraffic emits it: the fixed 2 SG x 2 SGP x 2 association
// topology (eight associations), the mixed payload, ASP-dial initiation and
// one ordered flow per configured route.
func routedRunJSON(testContext *testing.T, rate int, mode string) string {
	testContext.Helper()
	return mutateBidirectionalJSON(testContext, unidirectionalRunJSON(testContext, rate, "asp-to-sgp", -1, 0), func(cohort map[string]any) {
		for _, side := range []string{"sender", "receiver"} {
			specification := cohort[side].(map[string]any)["spec"].(map[string]any)
			specification["mode"] = mode
			specification["payload"] = "mix"
		}
		cohort["sender"].(map[string]any)["manifest"].(map[string]any)["flow_count"] = float64(1000)
	})
}

func TestRoutedCohortsAreDecidedAsOneDirection(testContext *testing.T) {
	for _, mode := range []string{"routed", "routed-direct"} {
		testContext.Run(mode, func(testContext *testing.T) {
			input := fmt.Sprintf(`{"initial":10,"maximum":10,"probes":[{"rate":10,"run":%s}]}`, routedRunJSON(testContext, 10, mode))
			status, decoded := runRequest(testContext, input)
			if status == invalidInputExitStatus || len(decoded.ProbeDecisions) != 1 {
				testContext.Fatalf("complete %s cohort rejected: status=%d result=%+v", mode, status, decoded)
			}
			probe := decoded.ProbeDecisions[0]
			if probe.Decision != "pass" || len(probe.Directions) != 1 || probe.Directions[0].Direction != "asp-to-sgp" ||
				probe.Directions[0].AchievedRateLower == nil || *probe.Directions[0].AchievedRateLower != 10 {
				testContext.Fatalf("%s decision = %+v, want one passing asp-to-sgp direction", mode, probe)
			}
		})
	}
}

func TestRoutedStandaloneSenderRecordIsAcceptedLikeThroughput(testContext *testing.T) {
	for _, mode := range []string{"routed", "routed-direct"} {
		testContext.Run(mode, func(testContext *testing.T) {
			run := strings.Replace(passingRunJSON(), `"payload":"128","mode":"throughput"`, fmt.Sprintf(`"payload":"mix","mode":%q`, mode), 1)
			run = replaceManifestField(run, `"flow_count":32`, `"flow_count":1000`)
			input := fmt.Sprintf(`{"initial":10,"maximum":10,"probes":[{"rate":10,"run":%s}]}`, run)
			status, decoded := runRequest(testContext, input)
			if status == invalidInputExitStatus || len(decoded.ProbeDecisions) != 1 || decoded.ProbeDecisions[0].Decision != "pass" {
				testContext.Fatalf("standalone %s sender rejected: status=%d result=%+v", mode, status, decoded)
			}
		})
	}
}

func TestRoutedCohortsRequireTheFixedTopology(testContext *testing.T) {
	bothSpecs := func(field string, value any) func(map[string]any) {
		return func(cohort map[string]any) {
			for _, side := range []string{"sender", "receiver"} {
				cohort[side].(map[string]any)["spec"].(map[string]any)[field] = value
			}
		}
	}
	for _, mode := range []string{"routed", "routed-direct"} {
		for _, test := range []struct {
			name      string
			mutate    func(map[string]any)
			wantError string
		}{
			{name: "four associations", wantError: "routed", mutate: func(cohort map[string]any) {
				bothSpecs("associations", float64(4))(cohort)
				cohort["sender"].(map[string]any)["negotiated_outbound_streams"] = []any{float64(8), float64(8), float64(8), float64(8)}
			}},
			{name: "fixed payload", wantError: "routed", mutate: bothSpecs("payload", "128")},
			{name: "sgp-dial initiation", wantError: "routed", mutate: func(cohort map[string]any) {
				bothSpecs("initiation", "sgp-dial")(cohort)
				cohort["sender"].(map[string]any)["manifest"].(map[string]any)["initiation"] = "sgp-dial"
			}},
			{name: "reverse direction", wantError: "direction", mutate: bothSpecs("direction", "sgp-to-asp")},
			{name: "direct flow count", wantError: "flow_count", mutate: func(cohort map[string]any) {
				cohort["sender"].(map[string]any)["manifest"].(map[string]any)["flow_count"] = float64(32)
			}},
			{name: "missing shared clock", wantError: "shared_clock", mutate: func(cohort map[string]any) {
				delete(cohort["sender"].(map[string]any)["spec"].(map[string]any), "shared_clock")
				delete(cohort["receiver"].(map[string]any)["spec"].(map[string]any), "shared_clock")
			}},
			{name: "receiver mode", wantError: "specs do not match", mutate: func(cohort map[string]any) {
				other := "routed-direct"
				if mode == other {
					other = "routed"
				}
				cohort["receiver"].(map[string]any)["spec"].(map[string]any)["mode"] = other
			}},
		} {
			testContext.Run(mode+"/"+test.name, func(testContext *testing.T) {
				mutated := mutateBidirectionalJSON(testContext, routedRunJSON(testContext, 10, mode), test.mutate)
				input := fmt.Sprintf(`{"initial":10,"maximum":10,"probes":[{"rate":10,"run":%s}]}`, mutated)
				status, decoded := runRequest(testContext, input)
				if status != invalidInputExitStatus || decoded.Decision != "invalid-input" || !strings.Contains(decoded.Error, test.wantError) {
					testContext.Fatalf("%s accepted or misreported: status=%d result=%+v", test.name, status, decoded)
				}
			})
		}
	}
}

func TestThroughputFlowCountIsUnconstrainedByRoutedRules(testContext *testing.T) {
	run := mutateBidirectionalJSON(testContext, unidirectionalRunJSON(testContext, 10, "asp-to-sgp", -1, 0), func(cohort map[string]any) {
		cohort["sender"].(map[string]any)["manifest"].(map[string]any)["flow_count"] = float64(1000)
	})
	input := fmt.Sprintf(`{"initial":10,"maximum":10,"probes":[{"rate":10,"run":%s}]}`, run)
	status, decoded := runRequest(testContext, input)
	if status == invalidInputExitStatus {
		testContext.Fatalf("throughput flow_count is not constrained by routed rules: status=%d result=%+v", status, decoded)
	}
}

func TestRoutedCampaignCannotMixRoutedAndDirectVariants(testContext *testing.T) {
	first := routedRunJSON(testContext, 10, "routed")
	second := routedRunJSON(testContext, 20, "routed-direct")
	input := fmt.Sprintf(`{"initial":10,"maximum":20,"probes":[{"rate":10,"run":%s},{"rate":20,"run":%s}]}`, first, second)
	status, decoded := runRequest(testContext, input)
	if status != invalidInputExitStatus || decoded.Decision != "invalid-input" || !strings.Contains(decoded.Error, "workload") {
		testContext.Fatalf("routed and direct variants joined one campaign: status=%d result=%+v", status, decoded)
	}
}
