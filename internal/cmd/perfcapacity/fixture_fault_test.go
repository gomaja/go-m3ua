package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/gomaja/go-m3ua/internal/perfstats"
)

// A fixture fault is not capacity evidence in either phase. A measurement
// cohort any of whose records carries a fatal_error, or whose error is not
// only its directions' validity failures, is refused as invalid input, just
// as such a warm-up is; it is never decided as a failed probe that moves the
// search.
func TestFixtureFaultsAreRefusedInMeasurement(testContext *testing.T) {
	measurement := func(testContext *testing.T) map[string]any {
		return realDrainTimeoutRecord(testContext, "measurement")
	}
	for _, scenario := range []struct {
		name   string
		mutate func(map[string]any)
	}{
		{"sender control failure", func(run map[string]any) {
			run["sender"].(map[string]any)["fatal_error"] = "stop receiver: Post \"http://sgp:8080/stop\": connection refused"
		}},
		{"receiver read failure", func(run map[string]any) {
			run["receiver"].(map[string]any)["fatal_error"] = "association 3 ReadData in receiver phase measuring: EOF"
		}},
		{"stuck worker beside a sender backlog", func(run map[string]any) {
			withSenderDrainTimeout(run, 4954, 120)
			run["sender"].(map[string]any)["fatal_error"] = "sender workers exceeded the drain deadline"
		}},
		{"cohort error naming a fault", func(run map[string]any) {
			run["error"] = "stop receiver: connection refused\n" + cohortValidityError
		}},
	} {
		testContext.Run(scenario.name, func(testContext *testing.T) {
			run := measurement(testContext)
			scenario.mutate(run)
			status, decoded := runRequest(testContext, singleProbeRequest(testContext, 400000, run))
			if status != invalidInputExitStatus || !strings.Contains(decoded.Error, errFixtureFault.Error()) {
				testContext.Fatalf("status %d error %q, want the faulted measurement refused", status, decoded.Error)
			}
		})
	}
	testContext.Run("validity failure is still a failed probe", func(testContext *testing.T) {
		run := measurement(testContext)
		run["sender"].(map[string]any)["send_duration"].(map[string]any)["max_ns"] = float64(4_000_000)
		status, decoded := runRequest(testContext, singleProbeRequest(testContext, 400000, run))
		if status == invalidInputExitStatus || len(decoded.ProbeDecisions) != 1 || decoded.ProbeDecisions[0].SearchOutcome != perfstats.ProbeFailing {
			testContext.Fatalf("status %d %+v, want a failed probe", status, decoded)
		}
	})
}

// A legacy single sender record with a fatal_error is refused too.
func TestLegacySenderRecordWithAFaultIsRefused(testContext *testing.T) {
	run := strings.Replace(passingRunJSON(), `"fixture_verdict":"pass"`, `"fatal_error":"receiver result unavailable","fixture_verdict":"invalid"`, 1)
	input := fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s}]}`, run)
	if status, decoded := runRequest(testContext, input); status != invalidInputExitStatus || !strings.Contains(decoded.Error, errFixtureFault.Error()) {
		testContext.Fatalf("status %d error %q, want the faulted legacy record refused", status, decoded.Error)
	}
}

// drain_timeout must name a last receiver result read within the drain
// observation bound before the deadline: an older one is a control that
// stopped answering, which perftraffic reports as a fatal error.
func TestDrainTimeoutObservationMustBeRecent(testContext *testing.T) {
	timeoutOf := func(run map[string]any) map[string]any {
		return run["sender"].(map[string]any)["drain_timeout"].(map[string]any)
	}
	for _, scenario := range []struct {
		name     string
		observed any
		accepted bool
	}{
		{"at the deadline", 0.0, true},
		{"at the bound", float64(drainObservationBound), true},
		{"beyond the bound", float64(drainObservationBound + 1), false},
		{"negative", -1.0, false},
		{"missing", nil, false},
	} {
		testContext.Run(scenario.name, func(testContext *testing.T) {
			run := realWarmupWithDrainTimeout(testContext)
			run["sender"].(map[string]any)["send_duration"].(map[string]any)["max_ns"] = float64(4_000_000)
			if scenario.observed == nil {
				delete(timeoutOf(run), "observed_before_deadline_ns")
			} else {
				timeoutOf(run)["observed_before_deadline_ns"] = scenario.observed
			}
			status, decoded := runRequest(testContext, searchRequest(testContext, run))
			if scenario.accepted == (status == invalidInputExitStatus) {
				testContext.Fatalf("status %d error %q, want accepted = %t", status, decoded.Error, scenario.accepted)
			}
		})
	}
}

// late_after_deadline is part of the delivery evidence: it is a subset of
// invalid, the sender and receiver must agree on it, and a failed direction
// with late deliveries names them, so a receive-side stall is told apart from
// a discard.
func TestLateDeliveriesAreEvidence(testContext *testing.T) {
	backlog := func(testContext *testing.T) map[string]any {
		run := searchRecord(testContext, "sender-backlog-400000-warmup.json")
		run["sender"].(map[string]any)["send_duration"].(map[string]any)["max_ns"] = float64(4_000_000)
		return run
	}
	deliveryOf := func(run map[string]any, key string) map[string]any {
		return run[key].(map[string]any)["delivery"].(map[string]any)
	}
	for name, mutate := range map[string]func(map[string]any){
		"more late than invalid": func(run map[string]any) {
			for _, key := range []string{"sender", "receiver"} {
				deliveryOf(run, key)["late_after_deadline"] = deliveryOf(run, key)["invalid"].(float64) + 1
			}
		},
		"sender and receiver disagree": func(run map[string]any) {
			deliveryOf(run, "sender")["late_after_deadline"] = deliveryOf(run, "sender")["late_after_deadline"].(float64) - 1
		},
		"only one record counts it": func(run map[string]any) {
			delete(deliveryOf(run, "receiver"), "late_after_deadline")
		},
	} {
		testContext.Run(name, func(testContext *testing.T) {
			run := backlog(testContext)
			mutate(run)
			if status, decoded := runRequest(testContext, singleProbeRequest(testContext, 400000, run)); status != invalidInputExitStatus {
				testContext.Fatalf("status %d %+v, want contradictory late deliveries refused", status, decoded)
			}
		})
	}
	testContext.Run("failed probe names late deliveries", func(testContext *testing.T) {
		status, decoded := runRequest(testContext, singleProbeRequest(testContext, 400000, backlog(testContext)))
		if status == invalidInputExitStatus || len(decoded.ProbeDecisions) != 1 {
			testContext.Fatalf("status %d %+v", status, decoded)
		}
		probe := decoded.ProbeDecisions[0]
		if probe.Decision != string(perfstats.Fail) || probe.Reason != lateAfterDeadlineReason || probe.Directions[0].Reason != lateAfterDeadlineReason {
			testContext.Fatalf("probe %+v, want the late-delivery reason", probe)
		}
	})
	testContext.Run("failed probe without late deliveries keeps its reason", func(testContext *testing.T) {
		run := realWarmupWithDrainTimeout(testContext)
		run["sender"].(map[string]any)["send_duration"].(map[string]any)["max_ns"] = float64(4_000_000)
		status, decoded := runRequest(testContext, searchRequest(testContext, run))
		if status == invalidInputExitStatus || decoded.ProbeDecisions[3].Reason == lateAfterDeadlineReason {
			testContext.Fatalf("status %d %+v", status, decoded)
		}
	})
	testContext.Run("bidirectional direction names its late deliveries", func(testContext *testing.T) {
		run := mutateBidirectionalJSON(testContext, failedBidirectionalWarmup(testContext, false, true), func(cohort map[string]any) {
			for _, key := range []string{"reverse_sender", "reverse_receiver"} {
				delivery := cohort[key].(map[string]any)["delivery"].(map[string]any)
				delivery["invalid"], delivery["late_after_deadline"] = 1.0, 1.0
			}
		})
		status, decoded := runRequest(testContext, fmt.Sprintf(`{"initial":10,"probes":[{"rate":10,"run":%s}]}`, run))
		if status == invalidInputExitStatus || len(decoded.ProbeDecisions) != 1 {
			testContext.Fatalf("status %d %+v", status, decoded)
		}
		if directions := decoded.ProbeDecisions[0].Directions; directions[1].Reason != lateAfterDeadlineReason || directions[0].Reason == lateAfterDeadlineReason {
			testContext.Fatalf("directions %+v, want only the reverse direction to name late deliveries", directions)
		}
	})
}

// Every record of the pre-fix testdata is still decided as before: none
// carries a fault.
func TestTestdataRecordsCarryNoFault(testContext *testing.T) {
	for _, name := range []string{
		"search-20000-measurement.json", "search-40000-measurement.json", "search-80000-measurement.json", "search-160000-warmup.json",
		"drain-timeout-400000-warmup.json", "drain-timeout-400000-measurement.json", "bidirectional-200000-warmup.json", "sender-backlog-400000-warmup.json",
	} {
		var cohort fixtureCohort
		if err := json.Unmarshal(mustJSON(testContext, searchRecord(testContext, name)), &cohort); err != nil {
			testContext.Fatal(err)
		}
		if err := refuseCohortFaults(&cohort); err != nil {
			testContext.Errorf("%s: %v", name, err)
		}
	}
}
