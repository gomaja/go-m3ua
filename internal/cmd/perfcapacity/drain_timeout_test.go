package main

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/gomaja/go-m3ua/internal/perfstats"
)

// withDrainTimeout records on the cohort's sender the drain deadline outcome
// perftraffic reports when submitted work is still unaccounted at the
// receiver when the deadline passes: drain_timeout with undelivered of the
// sender's submissions outstanding, and an invalid sender record. It leaves
// the delivery counters alone.
func withDrainTimeout(run map[string]any, undelivered uint64) map[string]any {
	sender := run["sender"].(map[string]any)
	submitted := sender["submitted"].(float64)
	sender["drain_timeout"] = map[string]any{
		"cause": drainTimeoutCause, "drain_ns": sender["spec"].(map[string]any)["drain_ns"],
		"submitted": submitted, "accounted": submitted - float64(undelivered), "undelivered": float64(undelivered),
		"observed_before_deadline_ns": float64(4_000_000),
	}
	sender["fixture_verdict"], sender["verdict"] = "invalid", "invalid"
	sender["reasons"] = []any{"submitted traffic was still unaccounted at the receiver when the drain deadline passed"}
	run["verdict"] = "invalid"
	return run
}

// cleanWarmupWithDrainTimeout is a real loss-free cohort at 80,000 messages/s
// re-expressed as a warm-up whose only failure is 7 messages the sender's last
// read before the drain deadline had not seen accounted for: the receiver's
// final counters show every message, because they arrived in the moments
// between that read and the deadline.
func cleanWarmupWithDrainTimeout(testContext *testing.T) map[string]any {
	run := withDrainTimeout(searchRecord(testContext, "search-80000-measurement.json"), 7)
	run["phase"] = "warmup"
	run["error"] = warmupOverloadError
	return run
}

// realWarmupWithDrainTimeout is the real warm-up at 160,000 messages/s with
// 238 more of its submitted messages undelivered at the drain deadline, as a
// receiver that discards under overload leaves them.
func realWarmupWithDrainTimeout(testContext *testing.T) map[string]any {
	run := searchRecord(testContext, "search-160000-warmup.json")
	for _, side := range []string{"sender", "receiver"} {
		delivery := run[side].(map[string]any)["delivery"].(map[string]any)
		delivery["unique"] = delivery["unique"].(float64) - 238
		delivery["unique_drain"] = delivery["unique_drain"].(float64) - 238
		delivery["missing"] = delivery["missing"].(float64) + 238
	}
	run["receiver"].(map[string]any)["reasons"] = []any{"receiver did not validate every delivery before the drain deadline"}
	return withDrainTimeout(run, 238)
}

func singleProbeRequest(testContext *testing.T, rate int, run map[string]any) string {
	testContext.Helper()
	encoded, err := json.Marshal(map[string]any{"initial": rate, "maximum": 1000000, "max_probes": 24,
		"probes": []map[string]any{{"rate": rate, "run": run}}})
	if err != nil {
		testContext.Fatal(err)
	}
	return string(encoded)
}

// A probe whose submitted work was still undelivered at the drain deadline
// failed at its rate. In warm-up that is overload evidence like cap refusals
// and missing deliveries: the rate bounds the bracket from above and the
// search continues. Before drain_timeout existed a warm-up whose only loss
// was that outstanding work was refused as invalid input, aborting the search.
func TestDrainTimeoutWarmupIsFailingEvidence(testContext *testing.T) {
	for _, scenario := range []struct {
		name    string
		request func(*testing.T) string
		index   int
		next    int
	}{
		{"only undelivered at the deadline", func(testContext *testing.T) string {
			return singleProbeRequest(testContext, 80000, cleanWarmupWithDrainTimeout(testContext))
		}, 0, 40000},
		{"undelivered and missing", func(testContext *testing.T) string {
			run := realWarmupWithDrainTimeout(testContext)
			run["sender"].(map[string]any)["send_duration"].(map[string]any)["max_ns"] = float64(4_000_000)
			return searchRequest(testContext, run)
		}, 3, 120000},
	} {
		testContext.Run(scenario.name, func(testContext *testing.T) {
			status, decoded := runRequest(testContext, scenario.request(testContext))
			if status == invalidInputExitStatus || decoded.Error != "" || len(decoded.ProbeDecisions) != scenario.index+1 {
				testContext.Fatalf("drain-timeout warm-up rejected: status %d %+v", status, decoded)
			}
			probe := decoded.ProbeDecisions[scenario.index]
			if probe.Phase != "warmup" || probe.Decision != string(perfstats.Fail) || probe.SearchOutcome != perfstats.ProbeFailing {
				testContext.Fatalf("probe %+v, want a failed warm-up", probe)
			}
			if decoded.SearchStatus != perfstats.SearchRunning || decoded.NextProbeRate != scenario.next {
				testContext.Fatalf("search %q next %d, want running with next probe %d", decoded.SearchStatus, decoded.NextProbeRate, scenario.next)
			}
		})
	}
}

// A measurement cohort with work undelivered at the drain deadline is a failed
// probe, as a measurement with cap refusals or missing deliveries is.
func TestDrainTimeoutMeasurementIsAFailedProbe(testContext *testing.T) {
	run := withDrainTimeout(searchRecord(testContext, "search-80000-measurement.json"), 7)
	run["error"] = "cohort is invalid; inspect machine-readable reasons"
	status, decoded := runRequest(testContext, singleProbeRequest(testContext, 80000, run))
	if status == invalidInputExitStatus || decoded.Error != "" || len(decoded.ProbeDecisions) != 1 {
		testContext.Fatalf("drain-timeout measurement rejected: status %d %+v", status, decoded)
	}
	if probe := decoded.ProbeDecisions[0]; probe.Phase != "" || probe.Decision != string(perfstats.Fail) || probe.SearchOutcome != perfstats.ProbeFailing {
		testContext.Fatalf("probe %+v, want a failed measurement", probe)
	}
	if decoded.SearchStatus != perfstats.SearchRunning || decoded.NextProbeRate != 40000 {
		testContext.Fatalf("search %q next %d, want running with next probe 40000", decoded.SearchStatus, decoded.NextProbeRate)
	}
}

// A drain timeout never excuses a fixture fault. A warm-up that also failed a
// control request, whose receiver failed, or that carries the pre-fix record
// shape (the drain deadline reported as a fatal error, which cannot be told
// apart from a control request that timed out) is still refused.
func TestDrainTimeoutDoesNotExcuseFixtureFaults(testContext *testing.T) {
	for name, mutate := range map[string]func(map[string]any){
		"sender control failure": func(run map[string]any) {
			run["sender"].(map[string]any)["fatal_error"] = "stop receiver: connection refused"
			run["error"] = "warmup did not drain cleanly: stop receiver: connection refused\ncohort is invalid; inspect machine-readable reasons"
		},
		"receiver failure": func(run map[string]any) {
			run["receiver"].(map[string]any)["fatal_error"] = "shared monotonic clock failed or regressed"
		},
		"pre-fix record shape": func(run map[string]any) {
			sender := run["sender"].(map[string]any)
			delete(sender, "drain_timeout")
			sender["fatal_error"] = "context deadline exceeded"
			run["error"] = "warmup did not drain cleanly: context deadline exceeded\ncohort is invalid; inspect machine-readable reasons"
		},
	} {
		testContext.Run(name, func(testContext *testing.T) {
			run := realWarmupWithDrainTimeout(testContext)
			mutate(run)
			status, decoded := runRequest(testContext, searchRequest(testContext, run))
			if status != invalidInputExitStatus || !strings.Contains(decoded.Error, "probe 4") || !strings.Contains(decoded.Error, "not probe evidence") {
				testContext.Fatalf("status %d error %q, want the faulted warm-up rejected", status, decoded.Error)
			}
		})
	}
}

// drain_timeout is evidence only when it is the producer's outcome and agrees
// with the record carrying it. Before this rule the field was ignored, so a
// contradictory outcome passed unchecked.
func TestDrainTimeoutMustReconcileWithItsRecord(testContext *testing.T) {
	timeoutOf := func(run map[string]any) map[string]any {
		return run["sender"].(map[string]any)["drain_timeout"].(map[string]any)
	}
	for name, mutate := range map[string]func(map[string]any){
		"another cause":     func(run map[string]any) { timeoutOf(run)["cause"] = "receiver drain deadline exceeded" },
		"missing field":     func(run map[string]any) { delete(timeoutOf(run), "accounted") },
		"another drain":     func(run map[string]any) { timeoutOf(run)["drain_ns"] = float64(1_000_000_000) },
		"another submitted": func(run map[string]any) { timeoutOf(run)["submitted"] = timeoutOf(run)["submitted"].(float64) - 1 },
		"nothing undelivered": func(run map[string]any) {
			timeoutOf(run)["accounted"], timeoutOf(run)["undelivered"] = timeoutOf(run)["submitted"], 0
		},
		"unreconciled count": func(run map[string]any) { timeoutOf(run)["undelivered"] = float64(8) },
		"accounted above final": func(run map[string]any) {
			timeoutOf(run)["accounted"], timeoutOf(run)["undelivered"] = float64(359330), float64(237)
		},
		"receiver record": func(run map[string]any) {
			run["receiver"].(map[string]any)["drain_timeout"] = timeoutOf(run)
		},
	} {
		testContext.Run(name, func(testContext *testing.T) {
			run := realWarmupWithDrainTimeout(testContext)
			mutate(run)
			if status, decoded := runRequest(testContext, searchRequest(testContext, run)); status != invalidInputExitStatus || !strings.Contains(decoded.Error, "probe 4") {
				testContext.Fatalf("status %d error %q, want the contradictory drain timeout rejected", status, decoded.Error)
			}
		})
	}
}

// A record the fixture judged passing cannot carry a drain timeout: the
// outcome fails the record that reports it.
func TestPassingRecordCannotCarryADrainTimeout(testContext *testing.T) {
	run := withDrainTimeout(searchRecord(testContext, "search-80000-measurement.json"), 7)
	sender := run["sender"].(map[string]any)
	sender["fixture_verdict"], sender["verdict"] = "pass", "inconclusive"
	run["verdict"] = "inconclusive"
	status, decoded := runRequest(testContext, singleProbeRequest(testContext, 80000, run))
	if status != invalidInputExitStatus || !strings.Contains(decoded.Error, "fixture_verdict contradicts") {
		testContext.Fatalf("status %d error %q, want a passing record with a drain timeout rejected", status, decoded.Error)
	}
}
