package main

import (
	"strings"
	"testing"

	"github.com/gomaja/go-m3ua/internal/perfstats"
)

// withSenderDrainTimeout records on the cohort's sender the outcome perftraffic
// reports when the drain deadline passes while the sender still holds
// scheduled work: outstanding messages were queued or in a send call, and
// unsubmitted sends were cut off, taken out of the submissions and counted as
// send errors. The receiver wait is then skipped, so no drain_timeout is
// recorded.
func withSenderDrainTimeout(run map[string]any, outstanding, unsubmitted float64) map[string]any {
	sender := run["sender"].(map[string]any)
	delete(sender, "drain_timeout")
	sender["submitted"] = sender["submitted"].(float64) - unsubmitted
	sender["sent"] = sender["sent"].(float64) - unsubmitted
	sender["send_errors"] = sender["send_errors"].(float64) + unsubmitted
	sender["sender_drain_timeout"] = map[string]any{
		"cause": senderDrainTimeoutCause, "drain_ns": sender["spec"].(map[string]any)["drain_ns"],
		"outstanding_at_deadline": outstanding, "unsubmitted": unsubmitted,
	}
	sender["fixture_verdict"], sender["verdict"] = "invalid", "invalid"
	sender["reasons"] = []any{"scheduled traffic was still unsubmitted at the sender when the drain deadline passed"}
	run["verdict"] = "invalid"
	return run
}

// realWarmupWithSenderBacklog is the real overloaded warm-up at 400,000
// messages/s with 120 of its sends cut off by the drain deadline while 4,954
// messages were still in the sender's queues.
func realWarmupWithSenderBacklog(testContext *testing.T) map[string]any {
	return withSenderDrainTimeout(realDrainTimeoutRecord(testContext, "warmup"), 4954, 120)
}

// A probe whose sender still held scheduled work at the drain deadline failed
// at its rate, in warm-up and in measurement. The search continues below it.
// Before sender_drain_timeout existed the fixture reported that backlog as a
// fatal error; a record whose only failure was the backlog was refused.
func TestSenderDrainTimeoutIsFailingEvidence(testContext *testing.T) {
	for _, scenario := range []struct {
		name  string
		run   func(*testing.T) map[string]any
		rate  int
		phase string
		next  int
	}{
		{"warm-up with cut-off sends and cap refusals", func(testContext *testing.T) map[string]any {
			run := realWarmupWithSenderBacklog(testContext)
			run["sender"].(map[string]any)["send_duration"].(map[string]any)["max_ns"] = float64(4_000_000)
			return run
		}, 400000, "warmup", 200000},
		{"warm-up with only outstanding work", func(testContext *testing.T) map[string]any {
			run := withSenderDrainTimeout(searchRecord(testContext, "search-80000-measurement.json"), 3, 0)
			run["phase"] = "warmup"
			run["error"] = warmupOverloadError
			return run
		}, 80000, "warmup", 40000},
		{"measurement with only outstanding work", func(testContext *testing.T) map[string]any {
			run := withSenderDrainTimeout(searchRecord(testContext, "search-80000-measurement.json"), 3, 0)
			run["error"] = "cohort is invalid; inspect machine-readable reasons"
			return run
		}, 80000, "", 40000},
	} {
		testContext.Run(scenario.name, func(testContext *testing.T) {
			status, decoded := runRequest(testContext, singleProbeRequest(testContext, scenario.rate, scenario.run(testContext)))
			if status == invalidInputExitStatus || decoded.Error != "" || len(decoded.ProbeDecisions) != 1 {
				testContext.Fatalf("sender backlog rejected: status %d %+v", status, decoded)
			}
			if probe := decoded.ProbeDecisions[0]; probe.Phase != scenario.phase || probe.Decision != string(perfstats.Fail) || probe.SearchOutcome != perfstats.ProbeFailing {
				testContext.Fatalf("probe %+v, want a failed probe in phase %q", probe, scenario.phase)
			}
			if decoded.SearchStatus != perfstats.SearchRunning || decoded.NextProbeRate != scenario.next {
				testContext.Fatalf("search %q next %d, want running with next probe %d", decoded.SearchStatus, decoded.NextProbeRate, scenario.next)
			}
		})
	}
}

// A sender backlog never excuses a fault: with a fatal error the warm-up is
// still refused.
func TestSenderDrainTimeoutDoesNotExcuseFixtureFaults(testContext *testing.T) {
	run := realWarmupWithSenderBacklog(testContext)
	run["sender"].(map[string]any)["fatal_error"] = "m3ua: DATA write failed (not sent): M3UA association not established"
	run["error"] = "warmup did not drain cleanly: cohort is invalid; inspect machine-readable reasons"
	status, decoded := runRequest(testContext, singleProbeRequest(testContext, 400000, run))
	if status != invalidInputExitStatus || !strings.Contains(decoded.Error, "not probe evidence") {
		testContext.Fatalf("status %d error %q, want the faulted warm-up refused", status, decoded.Error)
	}
}

// sender_drain_timeout is evidence only when it is the producer's outcome and
// agrees with the record carrying it. Before this rule the field was ignored.
func TestSenderDrainTimeoutMustReconcileWithItsRecord(testContext *testing.T) {
	timeoutOf := func(run map[string]any) map[string]any {
		return run["sender"].(map[string]any)["sender_drain_timeout"].(map[string]any)
	}
	for name, mutate := range map[string]func(map[string]any){
		"another cause": func(run map[string]any) { timeoutOf(run)["cause"] = "sender workers exceeded the drain deadline" },
		"missing field": func(run map[string]any) { delete(timeoutOf(run), "unsubmitted") },
		"another drain": func(run map[string]any) { timeoutOf(run)["drain_ns"] = float64(1_000_000_000) },
		"nothing cut off": func(run map[string]any) {
			timeoutOf(run)["outstanding_at_deadline"], timeoutOf(run)["unsubmitted"] = 0.0, 0.0
		},
		"more outstanding than the limit": func(run map[string]any) {
			timeoutOf(run)["outstanding_at_deadline"] = float64(8193)
		},
		"more unsubmitted than send errors": func(run map[string]any) {
			timeoutOf(run)["unsubmitted"] = float64(121)
		},
		"send errors it does not explain": func(run map[string]any) {
			timeoutOf(run)["unsubmitted"] = float64(119)
		},
		"receiver record": func(run map[string]any) {
			run["receiver"].(map[string]any)["sender_drain_timeout"] = timeoutOf(run)
		},
	} {
		testContext.Run(name, func(testContext *testing.T) {
			run := realWarmupWithSenderBacklog(testContext)
			mutate(run)
			if status, decoded := runRequest(testContext, singleProbeRequest(testContext, 400000, run)); status != invalidInputExitStatus {
				testContext.Fatalf("status %d decision %+v, want the contradictory sender drain timeout rejected", status, decoded)
			}
		})
	}
	testContext.Run("passing record", func(testContext *testing.T) {
		run := withSenderDrainTimeout(searchRecord(testContext, "search-80000-measurement.json"), 3, 0)
		sender := run["sender"].(map[string]any)
		sender["fixture_verdict"], sender["verdict"] = "pass", "inconclusive"
		run["verdict"] = "inconclusive"
		if status, decoded := runRequest(testContext, singleProbeRequest(testContext, 80000, run)); status != invalidInputExitStatus ||
			!strings.Contains(decoded.Error, "fixture_verdict contradicts") {
			testContext.Fatalf("status %d error %q, want a passing record with a sender drain timeout rejected", status, decoded.Error)
		}
	})
}
