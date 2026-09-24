package main

import (
	"fmt"
	"strings"
	"testing"
)

// TestSGPFailureTrialRecordsAreNotCapacityEvidence refuses perftraffic SGP
// failure trials, whose failed path deliberately loses traffic, in both
// record positions and in the standalone sender form, whichever kind of
// failure they declare: shutdown (-sgp-failure-kind=close) or abort.
func TestSGPFailureTrialRecordsAreNotCapacityEvidence(testContext *testing.T) {
	for _, kind := range []string{"shutdown", "abort"} {
		failure := map[string]any{"kind": kind, "offset_ns": float64(10e9)}
		for _, test := range []struct {
			name   string
			mutate func(map[string]any)
		}{
			{name: "declared in both specs", mutate: func(cohort map[string]any) {
				for _, side := range []string{"sender", "receiver"} {
					cohort[side].(map[string]any)["spec"].(map[string]any)["failure_trial"] = failure
				}
			}},
			{name: "sender failover evidence", mutate: func(cohort map[string]any) {
				cohort["sender"].(map[string]any)["failover"] = map[string]any{"spec": failure, "verdict": "pass"}
			}},
			{name: "receiver failover evidence", mutate: func(cohort map[string]any) {
				cohort["receiver"].(map[string]any)["failover"] = map[string]any{"spec": failure, "receiver": map[string]any{}}
			}},
		} {
			testContext.Run(kind+"/"+test.name, func(testContext *testing.T) {
				run := mutateBidirectionalJSON(testContext, routedRunJSON(testContext, 10, "routed"), test.mutate)
				status, decoded := runRequest(testContext, fmt.Sprintf(`{"initial":10,"maximum":10,"probes":[{"rate":10,"run":%s}]}`, run))
				if status != invalidInputExitStatus || !strings.Contains(decoded.Error, "SGP failure trial") {
					testContext.Fatalf("status=%d error=%q", status, decoded.Error)
				}
			})
		}
		run := strings.Replace(passingRunJSON(), `"payload":"128","mode":"throughput"`, `"payload":"mix","mode":"routed","failure_trial":{"kind":"`+kind+`"}`, 1)
		run = replaceManifestField(run, `"flow_count":32`, `"flow_count":1000`)
		status, decoded := runRequest(testContext, fmt.Sprintf(`{"initial":10,"maximum":10,"probes":[{"rate":10,"run":%s}]}`, run))
		if status != invalidInputExitStatus || !strings.Contains(decoded.Error, "SGP failure trial") {
			testContext.Fatalf("%s standalone sender: status=%d error=%q", kind, status, decoded.Error)
		}
	}
}
