package main

import (
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"testing"
)

const (
	bidirectionalDuration = 120_000_000_000
	bidirectionalStart    = 1_000_000_000_000
	bidirectionalEnd      = bidirectionalStart + bidirectionalDuration
)

func bidirectionalClockJSON() string {
	return fmt.Sprintf(`"shared_clock":{"domain":{"clock":"CLOCK_MONOTONIC","boot_id":"boot-a","time_namespace":"time:[1]","resolution_ns":1},"start_ns":%d,"end_ns":%d}`,
		bidirectionalStart, bidirectionalEnd)
}

func bidirectionalSpecJSON(rate int, reverse bool) string {
	cohort := "cohort-a"
	mode := "bidirectional"
	direction := "asp-to-sgp"
	peerControl := `,"peer_control":"http://127.0.0.1:8080"`
	if reverse {
		cohort += "-reverse"
		mode = "throughput"
		direction = "sgp-to-asp"
		peerControl = ""
	}
	return fmt.Sprintf(`"spec":{"cohort":%q,"seed":7,"associations":8,"expected":%d,"duration_ns":%d,"drain_ns":2000000000,"rate":%d,"outstanding":8192,"payload":"128","mode":%q,"direction":%q,"initiation":"asp-dial"%s,%s}`,
		cohort, rate*120, bidirectionalDuration, rate, mode, direction, peerControl, bidirectionalClockJSON())
}

func bidirectionalClockEvidenceJSON(sender bool) string {
	watchdog := ""
	if sender {
		watchdog = fmt.Sprintf(`,"watchdog":{"before_ns":%d,"after_ns":%d,"target_ns":%d,"maximum_lateness_ns":502,"budget_ns":1000000}`,
			bidirectionalStart-2_000_000_000, bidirectionalStart-2_000_000_000+500, bidirectionalEnd+2_000_000_000)
	}
	return `"shared_clock_evidence":{"before":{"clock":"CLOCK_MONOTONIC","boot_id":"boot-a","time_namespace":"time:[1]","resolution_ns":1},"after":{"clock":"CLOCK_MONOTONIC","boot_id":"boot-a","time_namespace":"time:[1]","resolution_ns":1},"verified":true` + watchdog + `}`
}

func bidirectionalDeliveryJSON(expected int) string {
	return fmt.Sprintf(`"delivery":{"unique":%d,"unique_measurement":%d,"unique_drain":0,"missing":0,"duplicate":0,"invalid":0,"reordered":0,"late_after_stop":0}`,
		expected, expected)
}

func bidirectionalSenderJSON(rate int, reverse bool, lower, upper float64) string {
	expected := rate * 120
	return fmt.Sprintf(`{"side":"sender",%s,"expected":%d,"scheduled":%d,"sent":%d,"submitted":%d,"send_errors":0,"capped":0,"outstanding_at_window_start":0,"outstanding_at_window_end":0,"outstanding_after_drain":0,"measurement_duration_ns":%d,"drain_duration_ns":1000000,%s,%s,%s,"fixture_verdict":"pass","verdict":"inconclusive",%s,"validated_per_second":%d,"sender_window":{"status":"bounded","duration_ns":%d,"delivered_lower":%d,"delivered_upper":%d,"outstanding_lower":0,"outstanding_upper":0,"rate_lower":%d,"rate_upper":%d,"backlog_change":{"status":"nonincrease-demonstrated","sample_count":120,"mean_change_lower":%g,"mean_change_upper":%g}}}`,
		bidirectionalSpecJSON(rate, reverse), expected, expected, expected, expected, bidirectionalDuration,
		bidirectionalDeliveryJSON(expected), sendDurationJSON(262144), manifestJSON()+`,"negotiated_outbound_streams":[8,8,8,8,8,8,8,8]`, bidirectionalClockEvidenceJSON(true),
		rate, bidirectionalDuration, expected, expected, rate, rate, lower, upper)
}

func bidirectionalReceiverJSON(rate int, reverse bool) string {
	expected := rate * 120
	return fmt.Sprintf(`{"side":"receiver",%s,"expected":%d,"sent":0,"submitted":0,"send_errors":0,"capped":0,"outstanding_at_window_start":0,"outstanding_at_window_end":0,"outstanding_after_drain":0,"measurement_duration_ns":%d,"drain_duration_ns":1000000,%s,"fixture_verdict":"pass","verdict":"inconclusive",%s,"validated_per_second":%d,"shared_clock_boundary":{"domain":{"clock":"CLOCK_MONOTONIC","boot_id":"boot-a","time_namespace":"time:[1]","resolution_ns":1},"captured_ns":%d,"measurement_lower":%d,"measurement_upper":%d}}`,
		bidirectionalSpecJSON(rate, reverse), expected, bidirectionalDuration, bidirectionalDeliveryJSON(expected),
		bidirectionalClockEvidenceJSON(false), rate, bidirectionalEnd+1_000_000, expected, expected)
}

func bidirectionalRunJSON(rate int, forwardLower, forwardUpper, reverseLower, reverseUpper float64) string {
	return fmt.Sprintf(`{"phase":"measurement","sender":%s,"receiver":%s,"reverse_sender":%s,"reverse_receiver":%s,"verdict":"inconclusive"}`,
		bidirectionalSenderJSON(rate, false, forwardLower, forwardUpper), bidirectionalReceiverJSON(rate, false),
		bidirectionalSenderJSON(rate, true, reverseLower, reverseUpper), bidirectionalReceiverJSON(rate, true))
}

func TestBidirectionalCohortUsesBothDirections(testContext *testing.T) {
	runJSON := bidirectionalRunJSON(10, -1, 0, -2, -1)
	input := fmt.Sprintf(`{"initial":10,"maximum":10,"probes":[{"rate":10,"run":%s}]}`, runJSON)
	status, decoded := runRequest(testContext, input)
	if status == invalidInputExitStatus || decoded.Decision == "invalid-input" {
		testContext.Fatalf("complete bidirectional cohort rejected: %+v", decoded)
	}
	if len(decoded.ProbeDecisions) != 1 || decoded.ProbeDecisions[0].Decision != "pass" {
		testContext.Fatalf("probe decisions = %+v, want one passing bidirectional probe", decoded.ProbeDecisions)
	}
	probe := decoded.ProbeDecisions[0]
	if probe.AggregateOfferedRate != 20 || probe.AggregateAchievedRateLower != 20 || probe.AggregateAchievedRateUpper != 20 ||
		len(probe.Directions) != 2 || probe.Directions[0].Direction != "asp-to-sgp" || probe.Directions[1].Direction != "sgp-to-asp" {
		testContext.Fatalf("bidirectional detail = %+v, want both directions and aggregate offered rate 20", probe)
	}
}

func TestBidirectionalCohortRequiresAllFourRecords(testContext *testing.T) {
	valid := bidirectionalRunJSON(10, -1, 0, -2, -1)
	for _, field := range []string{"sender", "receiver", "reverse_sender", "reverse_receiver"} {
		testContext.Run(field, func(testContext *testing.T) {
			marker := fmt.Sprintf(`,"%s":`, field)
			start := strings.Index(valid, marker)
			if start < 0 {
				testContext.Fatalf("fixture does not contain %s", field)
			}
			valueStart := start + len(marker)
			depth := 0
			end := valueStart
			for ; end < len(valid); end++ {
				switch valid[end] {
				case '{':
					depth++
				case '}':
					depth--
					if depth == 0 {
						end++
						goto found
					}
				}
			}
		found:
			mutated := valid[:start] + valid[end:]
			input := fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s}]}`, mutated)
			status, decoded := runRequest(testContext, input)
			if status != invalidInputExitStatus || decoded.Decision != "invalid-input" {
				testContext.Fatalf("missing %s accepted: status %d result %+v", field, status, decoded)
			}
		})
	}
}

func TestBidirectionalDirectionCannotHideTheOther(testContext *testing.T) {
	runJSON := bidirectionalRunJSON(10, 1, 2, -100, -50)
	input := fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s}]}`, runJSON)
	_, decoded := runRequest(testContext, input)
	if len(decoded.ProbeDecisions) != 1 || decoded.ProbeDecisions[0].Decision != "fail" {
		testContext.Fatalf("probe decisions = %+v, want a growing forward direction to fail", decoded.ProbeDecisions)
	}
	if len(decoded.ProbeDecisions[0].Directions) != 2 || decoded.ProbeDecisions[0].Directions[0].Decision != "fail" || decoded.ProbeDecisions[0].Directions[1].Decision != "pass" {
		testContext.Fatalf("direction decisions = %+v, want forward fail and reverse pass", decoded.ProbeDecisions[0].Directions)
	}
}

func TestBidirectionalInconclusiveDirectionStopsTheProbe(testContext *testing.T) {
	runJSON := bidirectionalRunJSON(10, -1, 0, -1, 1)
	input := fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s}]}`, runJSON)
	_, decoded := runRequest(testContext, input)
	if len(decoded.ProbeDecisions) != 1 || decoded.ProbeDecisions[0].Decision != "inconclusive" {
		testContext.Fatalf("probe decisions = %+v, want one inconclusive bidirectional probe", decoded.ProbeDecisions)
	}
	if decoded.ProbeDecisions[0].Directions[0].Decision != "pass" || decoded.ProbeDecisions[0].Directions[1].Decision != "inconclusive" {
		testContext.Fatalf("direction decisions = %+v, want forward pass and reverse inconclusive", decoded.ProbeDecisions[0].Directions)
	}
}

func TestBidirectionalCohortRejectsContradictions(testContext *testing.T) {
	valid := bidirectionalRunJSON(10, -1, 0, -2, -1)
	for _, test := range []struct {
		name string
		old  string
		new  string
	}{
		{name: "phase", old: `"phase":"measurement"`, new: `"phase":"warmup"`},
		{name: "reverse mode", old: `"mode":"throughput"`, new: `"mode":"bidirectional"`},
		{name: "reverse direction", old: `"direction":"sgp-to-asp"`, new: `"direction":"asp-to-sgp"`},
		{name: "reverse cohort suffix", old: `"cohort":"cohort-a-reverse"`, new: `"cohort":"cohort-a-other"`},
		{name: "reverse rate", old: `"rate":10`, new: `"rate":11`},
		{name: "reverse associations", old: `"associations":8`, new: `"associations":4`},
		{name: "reverse payload", old: `"payload":"128"`, new: `"payload":"4096"`},
		{name: "reverse peer control", old: `"mode":"throughput"`, new: `"peer_control":"http://127.0.0.1:8080","mode":"throughput"`},
		{name: "clock domain", old: `"boot_id":"boot-a"`, new: `"boot_id":"boot-b"`},
		{name: "clock window", old: fmt.Sprintf(`"start_ns":%d`, bidirectionalStart), new: fmt.Sprintf(`"start_ns":%d`, bidirectionalStart+1)},
		{name: "unverified clock", old: `"verified":true`, new: `"verified":false`},
		{name: "candidate revision", old: `"vcs_revision":"c370d891f0f7f0c6a1f4cf1f0f6cf0c0f0f0c0f0"`, new: `"vcs_revision":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"`},
		{name: "receiver boundary", old: `"measurement_lower":1200`, new: `"measurement_lower":1199`},
	} {
		testContext.Run(test.name, func(testContext *testing.T) {
			mutated := strings.Replace(valid, test.old, test.new, 1)
			input := fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s}]}`, mutated)
			status, decoded := runRequest(testContext, input)
			if status != invalidInputExitStatus || decoded.Decision != "invalid-input" {
				testContext.Fatalf("contradiction accepted: status %d result %+v", status, decoded)
			}
		})
	}
	for _, test := range []struct {
		name string
		old  string
		new  string
	}{
		{name: "directional payload mismatch", old: `"payload":"128"`, new: `"payload":"4096"`},
	} {
		testContext.Run(test.name, func(testContext *testing.T) {
			mutated := strings.Replace(valid, test.old, test.new, 2)
			input := fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s}]}`, mutated)
			status, decoded := runRequest(testContext, input)
			if status != invalidInputExitStatus || decoded.Decision != "invalid-input" {
				testContext.Fatalf("cross-direction contradiction accepted: status %d result %+v", status, decoded)
			}
		})
	}
	for _, test := range []struct {
		name   string
		mutate func(map[string]any)
	}{
		{name: "directional clock-domain mismatch", mutate: func(cohort map[string]any) {
			for _, name := range []string{"sender", "receiver"} {
				setRecordClockDomain(cohort[name].(map[string]any), "boot-b")
			}
		}},
		{name: "directional clock-window mismatch", mutate: func(cohort map[string]any) {
			for _, name := range []string{"sender", "receiver"} {
				window := cohort[name].(map[string]any)["spec"].(map[string]any)["shared_clock"].(map[string]any)
				window["start_ns"] = float64(bidirectionalStart + 1_000_000_000)
				window["end_ns"] = float64(bidirectionalEnd + 1_000_000_000)
			}
		}},
	} {
		testContext.Run(test.name, func(testContext *testing.T) {
			mutated := mutateBidirectionalJSON(testContext, valid, test.mutate)
			input := fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s}]}`, mutated)
			status, decoded := runRequest(testContext, input)
			if status != invalidInputExitStatus || decoded.Decision != "invalid-input" {
				testContext.Fatalf("cross-direction clock contradiction accepted: status %d result %+v", status, decoded)
			}
		})
	}
}

func mutateBidirectionalJSON(testContext *testing.T, input string, mutate func(map[string]any)) string {
	testContext.Helper()
	var cohort map[string]any
	if err := json.Unmarshal([]byte(input), &cohort); err != nil {
		testContext.Fatal(err)
	}
	mutate(cohort)
	encoded, err := json.Marshal(cohort)
	if err != nil {
		testContext.Fatal(err)
	}
	return string(encoded)
}

func setRecordClockDomain(record map[string]any, bootID string) {
	record["spec"].(map[string]any)["shared_clock"].(map[string]any)["domain"].(map[string]any)["boot_id"] = bootID
	evidence := record["shared_clock_evidence"].(map[string]any)
	evidence["before"].(map[string]any)["boot_id"] = bootID
	evidence["after"].(map[string]any)["boot_id"] = bootID
	if boundary, ok := record["shared_clock_boundary"].(map[string]any); ok {
		boundary["domain"].(map[string]any)["boot_id"] = bootID
	}
}

func TestBidirectionalCohortRejectsRoleSpecificEvidence(testContext *testing.T) {
	valid := bidirectionalRunJSON(10, -1, 0, -2, -1)
	for _, test := range []struct {
		name   string
		mutate func(map[string]any)
	}{
		{name: "receiver sender counter", mutate: func(cohort map[string]any) {
			cohort["receiver"].(map[string]any)["sent"] = float64(1)
		}},
		{name: "receiver sender window", mutate: func(cohort map[string]any) {
			cohort["receiver"].(map[string]any)["sender_window"] = map[string]any{}
		}},
		{name: "receiver echo", mutate: func(cohort map[string]any) {
			cohort["receiver"].(map[string]any)["receiver_echo"] = map[string]any{}
		}},
		{name: "sender receiver boundary", mutate: func(cohort map[string]any) {
			cohort["sender"].(map[string]any)["shared_clock_boundary"] = map[string]any{}
		}},
	} {
		testContext.Run(test.name, func(testContext *testing.T) {
			mutated := mutateBidirectionalJSON(testContext, valid, test.mutate)
			input := fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s}]}`, mutated)
			status, decoded := runRequest(testContext, input)
			if status != invalidInputExitStatus || decoded.Decision != "invalid-input" {
				testContext.Fatalf("role-specific contradiction accepted: status %d result %+v", status, decoded)
			}
		})
	}
}

func TestBidirectionalCohortRequiresValidSenderWatchdogs(testContext *testing.T) {
	valid := bidirectionalRunJSON(10, -1, 0, -2, -1)
	for _, senderName := range []string{"sender", "reverse_sender"} {
		for _, test := range []struct {
			name   string
			mutate func(map[string]any)
		}{
			{name: "missing", mutate: func(watchdog map[string]any) { delete(watchdog, "watchdog") }},
			{name: "zero before", mutate: func(watchdog map[string]any) { watchdog["watchdog"].(map[string]any)["before_ns"] = float64(0) }},
			{name: "regressed", mutate: func(watchdog map[string]any) {
				watchdog["watchdog"].(map[string]any)["after_ns"] = float64(bidirectionalStart - 2_000_000_001)
			}},
			{name: "wrong target", mutate: func(watchdog map[string]any) {
				watchdog["watchdog"].(map[string]any)["target_ns"] = float64(bidirectionalEnd)
			}},
			{name: "expired", mutate: func(watchdog map[string]any) {
				watchdog["watchdog"].(map[string]any)["after_ns"] = float64(bidirectionalEnd + 2_000_000_000)
			}},
			{name: "wrong lateness", mutate: func(watchdog map[string]any) {
				watchdog["watchdog"].(map[string]any)["maximum_lateness_ns"] = float64(501)
			}},
			{name: "wrong budget", mutate: func(watchdog map[string]any) { watchdog["watchdog"].(map[string]any)["budget_ns"] = float64(999_999) }},
		} {
			testContext.Run(senderName+"/"+test.name, func(testContext *testing.T) {
				mutated := mutateBidirectionalJSON(testContext, valid, func(cohort map[string]any) {
					evidence := cohort[senderName].(map[string]any)["shared_clock_evidence"].(map[string]any)
					test.mutate(evidence)
				})
				input := fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s}]}`, mutated)
				status, decoded := runRequest(testContext, input)
				if status != invalidInputExitStatus || decoded.Decision != "invalid-input" {
					testContext.Fatalf("invalid sender watchdog accepted: status %d result %+v", status, decoded)
				}
			})
		}
	}
}

func TestBidirectionalBoundaryContainsMeasuredDelivery(testContext *testing.T) {
	valid := bidirectionalRunJSON(10, -1, 0, -2, -1)
	for _, pair := range [][2]string{{"sender", "receiver"}, {"reverse_sender", "reverse_receiver"}} {
		testContext.Run(pair[0], func(testContext *testing.T) {
			mutated := mutateBidirectionalJSON(testContext, valid, func(cohort map[string]any) {
				for _, recordName := range pair {
					delivery := cohort[recordName].(map[string]any)["delivery"].(map[string]any)
					delivery["unique_measurement"] = float64(0)
					delivery["unique_drain"] = float64(1200)
				}
			})
			input := fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s}]}`, mutated)
			status, decoded := runRequest(testContext, input)
			if status != invalidInputExitStatus || decoded.Decision != "invalid-input" {
				testContext.Fatalf("measurement delivery outside boundary accepted: status %d result %+v", status, decoded)
			}
		})
	}

	ambiguous := mutateBidirectionalJSON(testContext, valid, func(cohort map[string]any) {
		for _, recordName := range []string{"sender", "receiver"} {
			record := cohort[recordName].(map[string]any)
			delivery := record["delivery"].(map[string]any)
			delivery["unique_measurement"] = float64(1199)
			delivery["unique_drain"] = float64(1)
			record["validated_per_second"] = float64(1198) / 120
		}
		senderWindow := cohort["sender"].(map[string]any)["sender_window"].(map[string]any)
		senderWindow["delivered_lower"] = float64(1198)
		senderWindow["outstanding_upper"] = float64(2)
		senderWindow["rate_lower"] = float64(1198) / 120
		boundary := cohort["receiver"].(map[string]any)["shared_clock_boundary"].(map[string]any)
		boundary["measurement_lower"] = float64(1198)
	})
	input := fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s}]}`, ambiguous)
	status, decoded := runRequest(testContext, input)
	if status == invalidInputExitStatus || decoded.Decision == "invalid-input" {
		testContext.Fatalf("legitimate resolution ambiguity rejected: status %d result %+v", status, decoded)
	}
}

func TestBoundedBidirectionalWindowRequiresPostWindowReceiverCapture(testContext *testing.T) {
	valid := bidirectionalRunJSON(10, -1, 0, -2, -1)
	for _, receiverName := range []string{"receiver", "reverse_receiver"} {
		for _, test := range []struct {
			name     string
			captured int64
		}{
			{name: "before start", captured: bidirectionalStart - 1},
			{name: "inside window", captured: bidirectionalEnd - 1},
			{name: "end without resolution", captured: bidirectionalEnd},
		} {
			testContext.Run(receiverName+"/"+test.name, func(testContext *testing.T) {
				mutated := mutateBidirectionalJSON(testContext, valid, func(cohort map[string]any) {
					boundary := cohort[receiverName].(map[string]any)["shared_clock_boundary"].(map[string]any)
					boundary["captured_ns"] = float64(test.captured)
				})
				input := fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s}]}`, mutated)
				status, decoded := runRequest(testContext, input)
				if status != invalidInputExitStatus || decoded.Decision != "invalid-input" {
					testContext.Fatalf("pre-boundary receiver capture accepted: status %d result %+v", status, decoded)
				}
			})
		}
	}

	exact := strings.ReplaceAll(valid, fmt.Sprintf(`"captured_ns":%d`, bidirectionalEnd+1_000_000), fmt.Sprintf(`"captured_ns":%d`, bidirectionalEnd+1))
	input := fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s}]}`, exact)
	status, decoded := runRequest(testContext, input)
	if status == invalidInputExitStatus || decoded.Decision == "invalid-input" {
		testContext.Fatalf("exact resolution-safe capture rejected: status %d result %+v", status, decoded)
	}

	unbounded := mutateBidirectionalJSON(testContext, valid, func(cohort map[string]any) {
		cohort["sender"].(map[string]any)["sender_window"].(map[string]any)["status"] = "unavailable"
		cohort["receiver"].(map[string]any)["shared_clock_boundary"].(map[string]any)["captured_ns"] = float64(bidirectionalStart)
	})
	input = fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s}]}`, unbounded)
	status, decoded = runRequest(testContext, input)
	if status == invalidInputExitStatus || len(decoded.ProbeDecisions) != 1 || decoded.ProbeDecisions[0].Decision != "inconclusive" {
		testContext.Fatalf("non-bounded incomplete evidence rejected: status %d result %+v", status, decoded)
	}
}

func TestBidirectionalCohortVerdictAndErrorMustAgree(testContext *testing.T) {
	valid := bidirectionalRunJSON(10, -1, 0, -2, -1)
	for _, runJSON := range []string{
		strings.Replace(valid, `"verdict":"inconclusive"}`, `"verdict":"pass"}`, 1),
		strings.Replace(valid, `"verdict":"inconclusive"}`, `"verdict":"inconclusive","error":"reverse failed"}`, 1),
	} {
		input := fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s}]}`, runJSON)
		status, decoded := runRequest(testContext, input)
		if status != invalidInputExitStatus || decoded.Decision != "invalid-input" {
			testContext.Fatalf("contradictory cohort verdict accepted: status %d result %+v", status, decoded)
		}
	}

	failed := strings.Replace(valid, `"verdict":"inconclusive"}`, `"verdict":"invalid","error":"reverse failed"}`, 1)
	input := fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s}]}`, failed)
	status, decoded := runRequest(testContext, input)
	if status == invalidInputExitStatus || len(decoded.ProbeDecisions) != 1 || decoded.ProbeDecisions[0].Decision != "fail" {
		testContext.Fatalf("complete errored cohort was not retained as a failed probe: status %d result %+v", status, decoded)
	}
	if decoded.ProbeDecisions[0].Reason != "bidirectional-cohort-error" ||
		decoded.ProbeDecisions[0].Directions[0].Decision != "pass" || decoded.ProbeDecisions[0].Directions[1].Decision != "pass" {
		testContext.Fatalf("cohort error obscured directional evidence: %+v", decoded.ProbeDecisions[0])
	}
}

func TestBidirectionalCompletedLossIsAFailedProbe(testContext *testing.T) {
	runJSON := bidirectionalRunJSON(10, -1, 0, -2, -1)
	runJSON = strings.Replace(runJSON,
		`"unique":1200,"unique_measurement":1200,"unique_drain":0,"missing":0`,
		`"unique":1199,"unique_measurement":1199,"unique_drain":0,"missing":1`, 2)
	runJSON = strings.Replace(runJSON, `"fixture_verdict":"pass","verdict":"inconclusive"`, `"fixture_verdict":"invalid","verdict":"invalid"`, 2)
	runJSON = strings.Replace(runJSON,
		`"delivered_lower":1200,"delivered_upper":1200,"outstanding_lower":0,"outstanding_upper":0,"rate_lower":10,"rate_upper":10`,
		`"delivered_lower":1199,"delivered_upper":1199,"outstanding_lower":1,"outstanding_upper":1,"rate_lower":9.991666666666667,"rate_upper":9.991666666666667`, 1)
	runJSON = strings.Replace(runJSON, `"validated_per_second":10`, `"validated_per_second":9.991666666666667`, 2)
	runJSON = strings.Replace(runJSON, `"measurement_lower":1200,"measurement_upper":1200`, `"measurement_lower":1199,"measurement_upper":1199`, 1)
	runJSON = strings.Replace(runJSON, `"verdict":"inconclusive"}`, `"verdict":"invalid"}`, 1)
	input := fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s}]}`, runJSON)
	status, decoded := runRequest(testContext, input)
	if status == invalidInputExitStatus || len(decoded.ProbeDecisions) != 1 || decoded.ProbeDecisions[0].Decision != "fail" {
		testContext.Fatalf("completed loss was omitted instead of retained as failure: status %d result %+v", status, decoded)
	}
}

func TestBidirectionalReceiverFailureFailsItsDirection(testContext *testing.T) {
	valid := bidirectionalRunJSON(10, -1, 0, -2, -1)
	for _, test := range []struct {
		name      string
		receiver  string
		direction int
	}{
		{name: "forward", receiver: "receiver", direction: 0},
		{name: "reverse", receiver: "reverse_receiver", direction: 1},
	} {
		testContext.Run(test.name, func(testContext *testing.T) {
			mutated := mutateBidirectionalJSON(testContext, valid, func(cohort map[string]any) {
				receiver := cohort[test.receiver].(map[string]any)
				receiver["fatal_error"] = "receiver failed after delivery"
				receiver["fixture_verdict"] = "invalid"
				receiver["verdict"] = "invalid"
				cohort["verdict"] = "invalid"
			})
			input := fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s}]}`, mutated)
			status, decoded := runRequest(testContext, input)
			if status == invalidInputExitStatus || len(decoded.ProbeDecisions) != 1 || decoded.ProbeDecisions[0].Decision != "fail" {
				testContext.Fatalf("receiver failure did not fail the probe: status %d result %+v", status, decoded)
			}
			if decoded.ProbeDecisions[0].Directions[test.direction].Decision != "fail" {
				testContext.Fatalf("receiver failure did not fail direction %d: %+v", test.direction, decoded.ProbeDecisions[0])
			}
		})
	}
}

func TestBidirectionalCapacityReportsPerDirectionAndAggregateOfferedRate(testContext *testing.T) {
	var probes []string
	for _, probe := range capacity37Schedule {
		lower, upper := -1.0, 0.0
		if !probe.passing {
			lower, upper = 1, 2
		}
		probes = append(probes, fmt.Sprintf(`{"rate":%d,"run":%s}`, probe.rate,
			bidirectionalRunJSON(probe.rate, lower, upper, lower, upper)))
	}
	var repetitions []string
	for range 5 {
		repetitions = append(repetitions, fmt.Sprintf(`{"rate":37,"run":%s}`, bidirectionalRunJSON(37, -1, 0, -1, 0)))
	}
	input := fmt.Sprintf(`{"initial":10,"maximum":100,"max_probes":24,"probes":[%s],"repetitions":[%s]}`,
		strings.Join(probes, ","), strings.Join(repetitions, ","))
	status, decoded := runRequest(testContext, input)
	if status != passingExitStatus || decoded.Decision != "pass" || decoded.SelectedRate != 37 || decoded.AggregateOfferedRate != 74 {
		testContext.Fatalf("bidirectional campaign = status %d result %+v, want selected 37/direction and aggregate offered 74", status, decoded)
	}
}

func TestCampaignIdentityDistinguishesSharedClockInstrumentation(testContext *testing.T) {
	var record fixtureEvidence
	if err := json.Unmarshal([]byte(passingRunJSON()), &record); err != nil {
		testContext.Fatal(err)
	}
	httpWorkload, err := workloadFromSpec(record.Spec, 10)
	if err != nil {
		testContext.Fatal(err)
	}
	var clock sharedClockWindow
	if err := json.Unmarshal([]byte(`{"domain":{"clock":"CLOCK_MONOTONIC","boot_id":"boot-a","time_namespace":"time:[1]","resolution_ns":1},"start_ns":1000000000000,"end_ns":1120000000000}`), &clock); err != nil {
		testContext.Fatal(err)
	}
	record.Spec.SharedClock = &clock
	sharedWorkload, err := workloadFromSpec(record.Spec, 10)
	if err != nil {
		testContext.Fatal(err)
	}
	if httpWorkload.Instrumentation == sharedWorkload.Instrumentation {
		testContext.Fatalf("instrumentation identities both equal %q", httpWorkload.Instrumentation)
	}
	var campaign campaignIdentity
	if err := campaign.add(runIdentity{workload: httpWorkload}); err != nil {
		testContext.Fatal(err)
	}
	if err := campaign.add(runIdentity{workload: sharedWorkload}); err == nil {
		testContext.Fatal("campaign mixed HTTP and shared-clock instrumentation")
	}
}

func TestBidirectionalCampaignAllowsIndependentCohortSeedAndClockWindow(testContext *testing.T) {
	first := bidirectionalRunJSON(10, -1, 0, -1, 0)
	second := bidirectionalRunJSON(20, -1, 0, -1, 0)
	second = strings.ReplaceAll(second, `"cohort-a`, `"cohort-b`)
	second = strings.ReplaceAll(second, `"seed":7`, `"seed":9`)
	second = strings.ReplaceAll(second, `"boot_id":"boot-a"`, `"boot_id":"boot-b"`)
	second = strings.ReplaceAll(second, fmt.Sprintf(`"start_ns":%d`, bidirectionalStart), fmt.Sprintf(`"start_ns":%d`, bidirectionalStart+1_000_000_000))
	second = strings.ReplaceAll(second, fmt.Sprintf(`"end_ns":%d`, bidirectionalEnd), fmt.Sprintf(`"end_ns":%d`, bidirectionalEnd+1_000_000_000))
	second = strings.ReplaceAll(second, fmt.Sprintf(`"target_ns":%d`, bidirectionalEnd+2_000_000_000), fmt.Sprintf(`"target_ns":%d`, bidirectionalEnd+3_000_000_000))
	second = strings.ReplaceAll(second, fmt.Sprintf(`"captured_ns":%d`, bidirectionalEnd+1_000_000), fmt.Sprintf(`"captured_ns":%d`, bidirectionalEnd+1_001_000_000))
	input := fmt.Sprintf(`{"initial":10,"maximum":100,"probes":[{"rate":10,"run":%s},{"rate":20,"run":%s}]}`, first, second)
	status, decoded := runRequest(testContext, input)
	if status == invalidInputExitStatus || decoded.Decision == "invalid-input" {
		testContext.Fatalf("independent cohort, seed and clock window were treated as campaign identity: %+v", decoded)
	}
}

func TestBidirectionalCampaignTracksBothStreamInventories(testContext *testing.T) {
	first := bidirectionalRunJSON(10, -1, 0, -1, 0)
	second := bidirectionalRunJSON(20, -1, 0, -1, 0)
	changedReverse := mutateBidirectionalJSON(testContext, second, func(cohort map[string]any) {
		streams := cohort["reverse_sender"].(map[string]any)["negotiated_outbound_streams"].([]any)
		streams[0] = float64(9)
	})
	input := fmt.Sprintf(`{"initial":10,"maximum":100,"probes":[{"rate":10,"run":%s},{"rate":20,"run":%s}]}`, first, changedReverse)
	status, decoded := runRequest(testContext, input)
	if status != invalidInputExitStatus || decoded.Decision != "invalid-input" {
		testContext.Fatalf("changed reverse stream inventory accepted: status %d result %+v", status, decoded)
	}

	for _, run := range []*string{&first, &second} {
		*run = mutateBidirectionalJSON(testContext, *run, func(cohort map[string]any) {
			streams := cohort["reverse_sender"].(map[string]any)["negotiated_outbound_streams"].([]any)
			for index := range streams {
				streams[index] = float64(16)
			}
		})
	}
	input = fmt.Sprintf(`{"initial":10,"maximum":100,"probes":[{"rate":10,"run":%s},{"rate":20,"run":%s}]}`, first, second)
	status, decoded = runRequest(testContext, input)
	if status == invalidInputExitStatus || decoded.Decision == "invalid-input" {
		testContext.Fatalf("stable asymmetric directional stream inventories rejected: status %d result %+v", status, decoded)
	}
}

func TestAggregateArithmeticRejectsOverflow(testContext *testing.T) {
	if _, ok := checkedAdd(math.MaxUint64, 1); ok {
		testContext.Fatal("checkedAdd accepted overflow")
	}
	if _, ok := checkedDouble(math.MaxUint64); ok {
		testContext.Fatal("checkedDouble accepted overflow")
	}
}

func TestBidirectionalCohortAllowsBoundedDrainWithoutCreditingIt(testContext *testing.T) {
	runJSON := bidirectionalRunJSON(10, -1, 0, -2, -1)
	runJSON = strings.Replace(runJSON, `"unique_measurement":1200,"unique_drain":0`, `"unique_measurement":1199,"unique_drain":1`, 2)
	runJSON = strings.Replace(runJSON, `"delivered_lower":1200,"delivered_upper":1200,"outstanding_lower":0,"outstanding_upper":0,"rate_lower":10,"rate_upper":10`,
		`"delivered_lower":1199,"delivered_upper":1199,"outstanding_lower":1,"outstanding_upper":1,"rate_lower":9.991666666666667,"rate_upper":9.991666666666667`, 1)
	runJSON = strings.Replace(runJSON, `"validated_per_second":10`, `"validated_per_second":9.991666666666667`, 2)
	runJSON = strings.Replace(runJSON, `"measurement_lower":1200,"measurement_upper":1200`, `"measurement_lower":1199,"measurement_upper":1199`, 1)
	input := fmt.Sprintf(`{"initial":10,"maximum":10,"probes":[{"rate":10,"run":%s}]}`, runJSON)
	status, decoded := runRequest(testContext, input)
	if status == invalidInputExitStatus || decoded.Decision == "invalid-input" || decoded.ProbeDecisions[0].Decision != "pass" {
		testContext.Fatalf("bounded drain rejected or credited incorrectly: status %d result %+v", status, decoded)
	}
	if decoded.ProbeDecisions[0].Directions[0].AchievedRateLower >= 10 {
		testContext.Fatalf("drain delivery was credited to achieved rate: %+v", decoded.ProbeDecisions[0].Directions[0])
	}
	if decoded.ProbeDecisions[0].AggregateAchievedRateLower >= float64(decoded.ProbeDecisions[0].AggregateOfferedRate) {
		testContext.Fatalf("aggregate drain delivery was credited to achieved rate: %+v", decoded.ProbeDecisions[0])
	}
}
