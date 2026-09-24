package main

import (
	"fmt"
	"strings"
	"testing"

	"github.com/gomaja/go-m3ua/internal/perfstats"
)

// loseOneDelivery makes one direction of a bidirectional cohort lose one of its
// 1,200 messages, as an overloaded direction does: both of its records show
// the missing delivery and are invalid, and its bounds count one fewer.
func loseOneDelivery(cohort map[string]any, senderKey, receiverKey string) {
	for _, key := range []string{senderKey, receiverKey} {
		record := cohort[key].(map[string]any)
		delivery := record["delivery"].(map[string]any)
		delivery["unique"], delivery["unique_measurement"], delivery["missing"] = 1199.0, 1199.0, 1.0
		record["fixture_verdict"], record["verdict"] = "invalid", "invalid"
		record["validated_per_second"] = 1199.0 / 120
	}
	window := cohort[senderKey].(map[string]any)["sender_window"].(map[string]any)
	window["delivered_lower"], window["delivered_upper"] = 1199.0, 1199.0
	window["outstanding_lower"], window["outstanding_upper"] = 1.0, 1.0
	window["rate_lower"], window["rate_upper"] = 1199.0/120, 1199.0/120
	boundary := cohort[receiverKey].(map[string]any)["shared_clock_boundary"].(map[string]any)
	boundary["measurement_lower"], boundary["measurement_upper"] = 1199.0, 1199.0
}

// failedBidirectionalWarmup is the four-record warm-up cohort perftraffic
// keeps when a bidirectional warm-up fails from overload in the named
// directions.
func failedBidirectionalWarmup(testContext *testing.T, forward, reverse bool) string {
	return mutateBidirectionalJSON(testContext, bidirectionalRunJSON(10, -1, 0, -2, -1), func(cohort map[string]any) {
		if forward {
			loseOneDelivery(cohort, "sender", "receiver")
		}
		if reverse {
			loseOneDelivery(cohort, "reverse_sender", "reverse_receiver")
		}
		cohort["phase"], cohort["verdict"], cohort["error"] = "warmup", "invalid", warmupOverloadError
	})
}

// An overloaded bidirectional warm-up is evidence against the rate whichever
// direction failed: the probe fails and the search continues below it.
func TestFailedBidirectionalWarmupFailsItsProbe(testContext *testing.T) {
	for _, scenario := range []struct {
		name             string
		forward, reverse bool
	}{
		{"forward direction overloaded", true, false},
		{"reverse direction overloaded", false, true},
		{"both directions overloaded", true, true},
	} {
		testContext.Run(scenario.name, func(testContext *testing.T) {
			input := fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s}]}`, failedBidirectionalWarmup(testContext, scenario.forward, scenario.reverse))
			status, decoded := runRequest(testContext, input)
			if status == invalidInputExitStatus || decoded.Error != "" || len(decoded.ProbeDecisions) != 1 {
				testContext.Fatalf("bidirectional warm-up rejected: status %d %+v", status, decoded)
			}
			probe := decoded.ProbeDecisions[0]
			if probe.Phase != "warmup" || probe.Decision != string(perfstats.Fail) || probe.SearchOutcome != perfstats.ProbeFailing || len(probe.Directions) != 2 {
				testContext.Fatalf("probe %+v, want a failed warm-up with both directions decided", probe)
			}
			if decoded.SearchStatus != perfstats.SearchRunning || decoded.NextProbeRate != 5 {
				testContext.Fatalf("search %q next %d, want running with next probe 5", decoded.SearchStatus, decoded.NextProbeRate)
			}
		})
	}
}

// A fixture fault in either direction, or a warm-up without its reverse
// records, is still refused: it says nothing about the rate.
func TestFailedBidirectionalWarmupRefusesFaultsInEitherDirection(testContext *testing.T) {
	for name, mutate := range map[string]func(map[string]any){
		"forward sender fault": func(cohort map[string]any) {
			cohort["sender"].(map[string]any)["fatal_error"] = "stop receiver: connection refused"
		},
		"reverse sender fault": func(cohort map[string]any) {
			cohort["reverse_sender"].(map[string]any)["fatal_error"] = "stop receiver: connection refused"
		},
		"reverse receiver fault": func(cohort map[string]any) {
			cohort["reverse_receiver"].(map[string]any)["fatal_error"] = "association 0 ReadData in receiver phase measuring: EOF"
		},
		"reverse fault named in the error": func(cohort map[string]any) {
			cohort["error"] = "warmup did not drain cleanly: cohort is invalid; inspect machine-readable reasons; reverse cohort: stop receiver: connection refused"
		},
		"reverse records dropped": func(cohort map[string]any) {
			delete(cohort, "reverse_sender")
			delete(cohort, "reverse_receiver")
		},
	} {
		testContext.Run(name, func(testContext *testing.T) {
			run := mutateBidirectionalJSON(testContext, failedBidirectionalWarmup(testContext, true, true), mutate)
			status, decoded := runRequest(testContext, fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s}]}`, run))
			if status != invalidInputExitStatus || !strings.Contains(decoded.Error, "probe 1") {
				testContext.Fatalf("status %d error %q, want the warm-up refused", status, decoded.Error)
			}
		})
	}
}
