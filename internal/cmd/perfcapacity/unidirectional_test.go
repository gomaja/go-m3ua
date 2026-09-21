package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func unidirectionalRunJSON(testContext *testing.T, rate int, direction string, lower, upper float64) string {
	testContext.Helper()
	return mutateBidirectionalJSON(testContext, bidirectionalRunJSON(rate, lower, upper, -2, -1), func(cohort map[string]any) {
		delete(cohort, "reverse_sender")
		delete(cohort, "reverse_receiver")
		for _, side := range []string{"sender", "receiver"} {
			specification := cohort[side].(map[string]any)["spec"].(map[string]any)
			specification["mode"] = "throughput"
			specification["direction"] = direction
		}
	})
}

func TestSharedClockUnidirectionalCohortReportsOneDirection(testContext *testing.T) {
	runJSON := unidirectionalRunJSON(testContext, 10, "asp-to-sgp", -1, 0)
	input := fmt.Sprintf(`{"initial":10,"maximum":10,"probes":[{"rate":10,"run":%s}]}`, runJSON)
	status, decoded := runRequest(testContext, input)
	if status == invalidInputExitStatus || len(decoded.ProbeDecisions) != 1 {
		testContext.Fatalf("complete unidirectional cohort rejected: status=%d result=%+v", status, decoded)
	}
	probe := decoded.ProbeDecisions[0]
	if probe.Decision != "pass" || len(probe.Directions) != 1 || probe.Directions[0].Direction != "asp-to-sgp" ||
		probe.Directions[0].Decision != "pass" || probe.Directions[0].AchievedRateLower == nil || *probe.Directions[0].AchievedRateLower != 10 ||
		probe.Directions[0].AchievedRateUpper == nil || *probe.Directions[0].AchievedRateUpper != 10 {
		testContext.Fatalf("unidirectional decision = %+v, want one passing asp-to-sgp direction", probe)
	}
	encoded, err := json.Marshal(probe)
	if err != nil {
		testContext.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		testContext.Fatal(err)
	}
	for _, field := range []string{"aggregate_offered_rate", "aggregate_achieved_rate_lower", "aggregate_achieved_rate_upper"} {
		if _, present := fields[field]; present {
			testContext.Fatalf("unidirectional decision invented %s: %s", field, encoded)
		}
	}
}

func TestSharedClockUnidirectionalCohortRequiresExactRecordSet(testContext *testing.T) {
	valid := unidirectionalRunJSON(testContext, 10, "asp-to-sgp", -1, 0)
	for _, test := range []struct {
		name   string
		mutate func(map[string]any)
	}{
		{name: "missing sender", mutate: func(cohort map[string]any) { delete(cohort, "sender") }},
		{name: "missing receiver", mutate: func(cohort map[string]any) { delete(cohort, "receiver") }},
		{name: "reverse sender object", mutate: func(cohort map[string]any) { cohort["reverse_sender"] = map[string]any{} }},
		{name: "reverse receiver object", mutate: func(cohort map[string]any) { cohort["reverse_receiver"] = map[string]any{} }},
		{name: "reverse sender null", mutate: func(cohort map[string]any) { cohort["reverse_sender"] = nil }},
		{name: "reverse receiver null", mutate: func(cohort map[string]any) { cohort["reverse_receiver"] = nil }},
	} {
		testContext.Run(test.name, func(testContext *testing.T) {
			mutated := mutateBidirectionalJSON(testContext, valid, test.mutate)
			input := fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s}]}`, mutated)
			status, decoded := runRequest(testContext, input)
			if status != invalidInputExitStatus || decoded.Decision != "invalid-input" {
				testContext.Fatalf("invalid record set accepted: status=%d result=%+v", status, decoded)
			}
		})
	}
}

func TestSharedClockUnidirectionalCohortRejectsContradictions(testContext *testing.T) {
	valid := unidirectionalRunJSON(testContext, 10, "asp-to-sgp", -1, 0)
	for _, test := range []struct {
		name   string
		mutate func(map[string]any)
	}{
		{name: "phase", mutate: func(cohort map[string]any) { cohort["phase"] = "warmup" }},
		{name: "sender mode", mutate: func(cohort map[string]any) {
			cohort["sender"].(map[string]any)["spec"].(map[string]any)["mode"] = "echo"
		}},
		{name: "receiver mode", mutate: func(cohort map[string]any) {
			cohort["receiver"].(map[string]any)["spec"].(map[string]any)["mode"] = "echo"
		}},
		{name: "extracted reverse direction", mutate: func(cohort map[string]any) {
			for _, side := range []string{"sender", "receiver"} {
				cohort[side].(map[string]any)["spec"].(map[string]any)["direction"] = "sgp-to-asp"
			}
		}},
		{name: "missing shared clock", mutate: func(cohort map[string]any) {
			delete(cohort["sender"].(map[string]any)["spec"].(map[string]any), "shared_clock")
			delete(cohort["receiver"].(map[string]any)["spec"].(map[string]any), "shared_clock")
		}},
		{name: "missing peer control", mutate: func(cohort map[string]any) {
			for _, side := range []string{"sender", "receiver"} {
				delete(cohort[side].(map[string]any)["spec"].(map[string]any), "peer_control")
			}
		}},
		{name: "empty peer control", mutate: func(cohort map[string]any) {
			for _, side := range []string{"sender", "receiver"} {
				cohort[side].(map[string]any)["spec"].(map[string]any)["peer_control"] = ""
			}
		}},
		{name: "null peer control", mutate: func(cohort map[string]any) {
			for _, side := range []string{"sender", "receiver"} {
				cohort[side].(map[string]any)["spec"].(map[string]any)["peer_control"] = nil
			}
		}},
		{name: "receiver direction", mutate: func(cohort map[string]any) {
			cohort["receiver"].(map[string]any)["spec"].(map[string]any)["direction"] = "sgp-to-asp"
		}},
		{name: "receiver cohort", mutate: func(cohort map[string]any) {
			cohort["receiver"].(map[string]any)["spec"].(map[string]any)["cohort"] = "other"
		}},
		{name: "receiver clock domain", mutate: func(cohort map[string]any) {
			setRecordClockDomain(cohort["receiver"].(map[string]any), "boot-b")
		}},
		{name: "receiver delivery", mutate: func(cohort map[string]any) {
			cohort["receiver"].(map[string]any)["delivery"].(map[string]any)["unique_measurement"] = float64(1199)
		}},
		{name: "receiver boundary", mutate: func(cohort map[string]any) {
			cohort["receiver"].(map[string]any)["shared_clock_boundary"].(map[string]any)["measurement_lower"] = float64(1199)
		}},
		{name: "sender receiver window pair", mutate: func(cohort map[string]any) {
			for _, side := range []string{"sender", "receiver"} {
				delivery := cohort[side].(map[string]any)["delivery"].(map[string]any)
				delivery["unique_measurement"] = float64(1199)
				delivery["unique_drain"] = float64(1)
			}
			receiver := cohort["receiver"].(map[string]any)
			receiver["validated_per_second"] = 1199.0 / 120.0
			boundary := receiver["shared_clock_boundary"].(map[string]any)
			boundary["measurement_lower"] = float64(1199)
			boundary["measurement_upper"] = float64(1199)
		}},
		{name: "missing sender watchdog", mutate: func(cohort map[string]any) {
			delete(cohort["sender"].(map[string]any)["shared_clock_evidence"].(map[string]any), "watchdog")
		}},
		{name: "receiver sender window", mutate: func(cohort map[string]any) { cohort["receiver"].(map[string]any)["sender_window"] = map[string]any{} }},
		{name: "cohort verdict", mutate: func(cohort map[string]any) { cohort["verdict"] = "pass" }},
	} {
		testContext.Run(test.name, func(testContext *testing.T) {
			mutated := mutateBidirectionalJSON(testContext, valid, test.mutate)
			input := fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s}]}`, mutated)
			status, decoded := runRequest(testContext, input)
			if status != invalidInputExitStatus || decoded.Decision != "invalid-input" {
				testContext.Fatalf("contradiction accepted: status=%d result=%+v", status, decoded)
			}
		})
	}
}

func TestSharedClockUnidirectionalCompletedLossIsAFailedProbe(testContext *testing.T) {
	failed := mutateBidirectionalJSON(testContext, unidirectionalRunJSON(testContext, 10, "asp-to-sgp", -1, 0), func(cohort map[string]any) {
		for _, side := range []string{"sender", "receiver"} {
			record := cohort[side].(map[string]any)
			delivery := record["delivery"].(map[string]any)
			delivery["unique"] = float64(1199)
			delivery["unique_measurement"] = float64(1199)
			delivery["missing"] = float64(1)
			record["fixture_verdict"] = "invalid"
			record["verdict"] = "invalid"
			record["validated_per_second"] = 1199.0 / 120.0
		}
		senderWindow := cohort["sender"].(map[string]any)["sender_window"].(map[string]any)
		senderWindow["delivered_lower"] = float64(1199)
		senderWindow["delivered_upper"] = float64(1199)
		senderWindow["outstanding_lower"] = float64(1)
		senderWindow["outstanding_upper"] = float64(1)
		senderWindow["rate_lower"] = 1199.0 / 120.0
		senderWindow["rate_upper"] = 1199.0 / 120.0
		boundary := cohort["receiver"].(map[string]any)["shared_clock_boundary"].(map[string]any)
		boundary["measurement_lower"] = float64(1199)
		boundary["measurement_upper"] = float64(1199)
		cohort["verdict"] = "invalid"
	})
	input := fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s}]}`, failed)
	status, decoded := runRequest(testContext, input)
	if status == invalidInputExitStatus || len(decoded.ProbeDecisions) != 1 || decoded.ProbeDecisions[0].Decision != "fail" ||
		len(decoded.ProbeDecisions[0].Directions) != 1 || decoded.ProbeDecisions[0].Directions[0].Decision != "fail" {
		testContext.Fatalf("completed unidirectional loss was not retained as failure: status=%d result=%+v", status, decoded)
	}
}

func TestSharedClockUnidirectionalCohortPreservesUnavailableBounds(testContext *testing.T) {
	unavailable := mutateBidirectionalJSON(testContext, unidirectionalRunJSON(testContext, 10, "asp-to-sgp", -1, 0), func(cohort map[string]any) {
		setUnavailableSenderWindow(cohort["sender"].(map[string]any))
	})
	input := fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s}]}`, unavailable)
	status, decoded := runRequest(testContext, input)
	if status == invalidInputExitStatus || len(decoded.ProbeDecisions) != 1 {
		testContext.Fatalf("producer-shaped unavailable window rejected: status=%d result=%+v", status, decoded)
	}
	probe := decoded.ProbeDecisions[0]
	if probe.Decision != "inconclusive" || len(probe.Directions) != 1 || probe.Directions[0].Decision != "inconclusive" ||
		probe.Directions[0].AchievedRateLower != nil || probe.Directions[0].AchievedRateUpper != nil {
		testContext.Fatalf("unavailable window became measured bounds: %+v", probe)
	}
}

func TestSharedClockUnidirectionalCohortOutcomePrecedence(testContext *testing.T) {
	valid := unidirectionalRunJSON(testContext, 10, "asp-to-sgp", -1, 0)
	for _, test := range []struct {
		name         string
		mutate       func(map[string]any)
		wantDecision string
		wantReason   string
	}{
		{
			name: "cohort error is a failed probe",
			mutate: func(cohort map[string]any) {
				cohort["verdict"] = "invalid"
				cohort["error"] = "cohort failed after measurement"
			},
			wantDecision: "fail", wantReason: "unidirectional-cohort-error",
		},
		{
			name: "stall remains inconclusive ahead of cohort error",
			mutate: func(cohort map[string]any) {
				cohort["sender"].(map[string]any)["send_duration"].(map[string]any)["max_ns"] = float64(1_200_000_000)
				cohort["verdict"] = "invalid"
				cohort["error"] = "cohort failed after measurement"
			},
			wantDecision: "inconclusive",
		},
		{
			name: "receiver failure fails direction",
			mutate: func(cohort map[string]any) {
				receiver := cohort["receiver"].(map[string]any)
				receiver["fatal_error"] = "receiver failed after delivery"
				receiver["fixture_verdict"] = "invalid"
				receiver["verdict"] = "invalid"
				cohort["verdict"] = "invalid"
			},
			wantDecision: "fail",
		},
	} {
		testContext.Run(test.name, func(testContext *testing.T) {
			mutated := mutateBidirectionalJSON(testContext, valid, test.mutate)
			input := fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s}]}`, mutated)
			status, decoded := runRequest(testContext, input)
			if status == invalidInputExitStatus || len(decoded.ProbeDecisions) != 1 {
				testContext.Fatalf("completed cohort rejected: status=%d result=%+v", status, decoded)
			}
			probe := decoded.ProbeDecisions[0]
			if probe.Decision != test.wantDecision || (test.wantReason != "" && probe.Reason != test.wantReason) || len(probe.Directions) != 1 {
				testContext.Fatalf("cohort outcome = %+v, want decision=%s reason=%q", probe, test.wantDecision, test.wantReason)
			}
		})
	}
}

func TestSharedClockUnidirectionalCohortRejectsChangedCampaignIdentity(testContext *testing.T) {
	first := unidirectionalRunJSON(testContext, 10, "asp-to-sgp", -1, 0)
	second := mutateBidirectionalJSON(testContext, unidirectionalRunJSON(testContext, 20, "asp-to-sgp", -1, 0), func(cohort map[string]any) {
		for _, side := range []string{"sender", "receiver"} {
			cohort[side].(map[string]any)["spec"].(map[string]any)["payload"] = "512"
		}
	})
	input := fmt.Sprintf(`{"initial":10,"maximum":20,"probes":[{"rate":10,"run":%s},{"rate":20,"run":%s}]}`, first, second)
	status, decoded := runRequest(testContext, input)
	if status != invalidInputExitStatus || decoded.Decision != "invalid-input" || !strings.Contains(decoded.Error, "workload") {
		testContext.Fatalf("changed unidirectional direction joined one campaign: status=%d result=%+v", status, decoded)
	}
}
