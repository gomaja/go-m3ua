package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/gomaja/go-m3ua/internal/perfstats"
)

// The testdata records are one real capacity search on the reference
// environment (shared clock, one association, 128-byte payload, 10-second
// windows): three passing measurement cohorts, then a probe at 160,000
// messages/s whose warm-up failed with 120,433 outstanding-cap refusals and a
// 1.03-second send block.
func searchRecord(testContext *testing.T, name string) map[string]any {
	testContext.Helper()
	raw, err := os.ReadFile("testdata/" + name)
	if err != nil {
		testContext.Fatal(err)
	}
	var record map[string]any
	if err := json.Unmarshal(raw, &record); err != nil {
		testContext.Fatal(err)
	}
	return record
}

func searchRequest(testContext *testing.T, lastRun map[string]any) string {
	testContext.Helper()
	probes := []map[string]any{
		{"rate": 20000, "run": searchRecord(testContext, "search-20000-measurement.json")},
		{"rate": 40000, "run": searchRecord(testContext, "search-40000-measurement.json")},
		{"rate": 80000, "run": searchRecord(testContext, "search-80000-measurement.json")},
		{"rate": 160000, "run": lastRun},
	}
	encoded, err := json.Marshal(map[string]any{"initial": 20000, "maximum": 1000000, "max_probes": 24, "probes": probes})
	if err != nil {
		testContext.Fatal(err)
	}
	return string(encoded)
}

// A warm-up that failed with demonstrated loss or a stall never reached
// measurement at that rate: it bounds the bracket from above and the search
// continues. With a stall it did not demonstrate the rate; the search carries
// the warm-up phase and the stall into its report.
func TestFailedWarmupBoundsTheSearchFromAbove(testContext *testing.T) {
	status, decoded := runRequest(testContext, searchRequest(testContext, searchRecord(testContext, "search-160000-warmup.json")))
	if status == invalidInputExitStatus || decoded.Error != "" {
		testContext.Fatalf("real failed warm-up rejected: status %d %+v", status, decoded)
	}
	if len(decoded.ProbeDecisions) != 4 || decoded.NextProbeRate != 120000 || decoded.SearchStatus != perfstats.SearchRunning {
		testContext.Fatalf("decisions %+v next %d status %q, want next probe 120000", decoded.ProbeDecisions, decoded.NextProbeRate, decoded.SearchStatus)
	}
	for _, passing := range decoded.ProbeDecisions[:3] {
		if passing.SearchOutcome != perfstats.ProbePassing || passing.Phase != "" {
			testContext.Fatalf("measurement probe %+v, want pass", passing)
		}
	}
	warmup := decoded.ProbeDecisions[3]
	if warmup.Phase != "warmup" || warmup.Decision != string(perfstats.Inconclusive) || warmup.Reason != perfstats.TransportStallReason ||
		warmup.SearchOutcome != perfstats.ProbeNotDemonstrated || warmup.Stall == nil || !warmup.Stall.Stalled() {
		testContext.Fatalf("warm-up probe %+v, want a not-demonstrated stalled warm-up", warmup)
	}
}

// Without the stall, the same warm-up is a demonstrated failure.
func TestFailedWarmupWithoutStallIsAFailure(testContext *testing.T) {
	record := searchRecord(testContext, "search-160000-warmup.json")
	record["sender"].(map[string]any)["send_duration"].(map[string]any)["max_ns"] = float64(4_000_000)
	status, decoded := runRequest(testContext, searchRequest(testContext, record))
	if status == invalidInputExitStatus || len(decoded.ProbeDecisions) != 4 {
		testContext.Fatalf("status %d %+v", status, decoded)
	}
	if warmup := decoded.ProbeDecisions[3]; warmup.Decision != string(perfstats.Fail) || warmup.SearchOutcome != perfstats.ProbeFailing || warmup.Phase != "warmup" {
		testContext.Fatalf("warm-up probe %+v, want a failed warm-up", warmup)
	}
	if decoded.NextProbeRate != 120000 {
		testContext.Fatalf("next probe %d, want 120000", decoded.NextProbeRate)
	}
}

// A warm-up is probe evidence only when it failed with demonstrated loss or a
// stall. A warm-up that did not fail, failed without an error, or failed for
// a reason unrelated to the offered rate says nothing about the rate.
func TestWarmupWithoutDemonstratedOverloadIsNotEvidence(testContext *testing.T) {
	for name, mutate := range map[string]func(map[string]any){
		"unknown phase":         func(run map[string]any) { run["phase"] = "setup" },
		"warm-up did not fail":  func(run map[string]any) { run["verdict"] = "inconclusive" },
		"failure without error": func(run map[string]any) { delete(run, "error") },
	} {
		testContext.Run(name, func(testContext *testing.T) {
			record := searchRecord(testContext, "search-160000-warmup.json")
			mutate(record)
			status, decoded := runRequest(testContext, searchRequest(testContext, record))
			if status != invalidInputExitStatus || !strings.Contains(decoded.Error, "probe 4") {
				testContext.Fatalf("status %d error %q, want the warm-up rejected as probe 4 evidence", status, decoded.Error)
			}
		})
	}
}

// A warm-up that ran its whole schedule and failed only its own validity rules,
// but with every message delivered and no send blocked, says nothing about the
// rate. The record here is a real loss-free cohort relabelled as such a warm-up.
func TestWarmupFailureWithoutLossOrStallIsNotEvidence(testContext *testing.T) {
	record := searchRecord(testContext, "search-80000-measurement.json")
	record["phase"] = "warmup"
	record["verdict"] = "invalid"
	record["error"] = warmupOverloadError
	status, decoded := runRequest(testContext, searchRequest(testContext, record))
	if status != invalidInputExitStatus || !strings.Contains(decoded.Error, "without demonstrated loss or a stall") {
		testContext.Fatalf("status %d error %q, want a loss-free failed warm-up rejected", status, decoded.Error)
	}
}

// Loss counts alone do not show that the rate was not sustained: an abort for
// another reason also strands messages. Only a warm-up that offered its whole
// schedule, had no fatal read or control failure and failed only its own
// validity rules is evidence against the rate.
func TestWarmupAbortedForAnotherReasonIsNotEvidence(testContext *testing.T) {
	for name, mutate := range map[string]func(map[string]any){
		"control request failure": func(run map[string]any) { run["error"] = "reset receiver: connection refused" },
		"joined poll failure": func(run map[string]any) {
			run["error"] = "warmup did not drain cleanly: progress request failed\ncohort is invalid; inspect machine-readable reasons"
		},
		"receiver read failure": func(run map[string]any) {
			run["receiver"].(map[string]any)["fatal_error"] = "M3UA association not established"
		},
		"sender fatal failure": func(run map[string]any) {
			run["sender"].(map[string]any)["fatal_error"] = "write deadline exceeded"
		},
		"schedule cut short": func(run map[string]any) {
			sender := run["sender"].(map[string]any)
			sender["scheduled"] = sender["expected"].(float64) - 1
		},
	} {
		testContext.Run(name, func(testContext *testing.T) {
			record := searchRecord(testContext, "search-160000-warmup.json")
			mutate(record)
			status, decoded := runRequest(testContext, searchRequest(testContext, record))
			if status != invalidInputExitStatus || !strings.Contains(decoded.Error, "probe 4") || !strings.Contains(decoded.Error, "not probe evidence") {
				testContext.Fatalf("status %d error %q, want the aborted warm-up rejected", status, decoded.Error)
			}
		})
	}
}

// The reverse direction of a bidirectional warm-up is checked like the forward
// one: loss or a stall in either direction is evidence against the rate.
func TestWarmupOverloadIsDetectedInEitherDirection(testContext *testing.T) {
	count := func(value uint64) *uint64 { return &value }
	record := func(capped, missing uint64) *fixtureEvidence {
		return &fixtureEvidence{Scheduled: count(100), Expected: count(100), Capped: count(capped), Delivery: &deliveryEvidence{Missing: count(missing)}}
	}
	phase, verdict := "warmup", "invalid"
	for _, scenario := range []struct {
		name             string
		forward, reverse *fixtureEvidence
		accepted         bool
	}{
		{"reverse loss", record(0, 0), record(0, 7), true},
		{"reverse cap refusals", record(0, 0), record(3, 3), true},
		{"forward loss", record(2, 2), record(0, 0), true},
		{"neither", record(0, 0), record(0, 0), false},
	} {
		cohort := &fixtureCohort{Phase: &phase, Verdict: &verdict, Error: warmupOverloadError,
			Sender: scenario.forward, Receiver: &fixtureEvidence{}, ReverseSender: scenario.reverse, ReverseReceiver: &fixtureEvidence{}}
		warmup, err := cohortPhase(cohort)
		if (err == nil) != scenario.accepted || err == nil && !warmup {
			testContext.Errorf("%s: warmup=%t err=%v, want accepted=%t", scenario.name, warmup, err, scenario.accepted)
		}
	}
}

// A validation repetition whose warm-up failed from overload never reached a
// full run at the selected rate. The repetition path decides it like any run,
// so it is a failed repetition (or not demonstrated with a stall), never a
// pass: DecideCapacity requires every repetition to pass.
func TestFailedWarmupRepetitionIsNeverAPass(testContext *testing.T) {
	for name, maximumSend := range map[string]float64{"loss": 4_000_000, "loss and stall": 1_030_000_000} {
		warmup := searchRecord(testContext, "search-160000-warmup.json")
		warmup["sender"].(map[string]any)["send_duration"].(map[string]any)["max_ns"] = maximumSend
		fixture, err := fixtureRunFromJSON(mustJSON(testContext, warmup), 160000)
		if err != nil {
			testContext.Fatalf("%s: %v", name, err)
		}
		decision := decideFixtureRun(fixture, 160000)
		if decision.Phase != "warmup" || decision.Decision == string(perfstats.Pass) {
			testContext.Fatalf("%s: failed warm-up repetition decided %+v, want a non-passing warm-up", name, decision)
		}
		capacity := perfstats.DecideCapacity(bracketedAt(testContext, 160000), []int{160000, 160000, 160000, 160000, 160000},
			[]perfstats.Decision{perfstats.Pass, perfstats.Pass, perfstats.Pass, perfstats.Pass, perfstats.Decision(decision.Decision)})
		if capacity.Decision == perfstats.Pass {
			testContext.Fatalf("%s: campaign %+v passed with a failed warm-up repetition", name, capacity)
		}
	}
}

// bracketedAt returns a search bracketed with lower bound rate.
func bracketedAt(testContext *testing.T, rate int) *perfstats.CapacitySearch {
	testContext.Helper()
	search, err := perfstats.NewCapacitySearch(rate, 2*rate, 24)
	if err != nil {
		testContext.Fatal(err)
	}
	for {
		next, running := search.NextRate()
		if !running {
			return search
		}
		outcome := perfstats.ProbeFailing
		if next <= rate {
			outcome = perfstats.ProbePassing
		}
		if err := search.Record(next, outcome); err != nil {
			testContext.Fatal(err)
		}
	}
}

func mustJSON(testContext *testing.T, value any) json.RawMessage {
	testContext.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		testContext.Fatal(err)
	}
	return encoded
}

// A campaign may meet its first failed warm-up before any measurement fixes
// its duration; every other identity field is still compared.
func TestCampaignIdentityIgnoresOnlyTheWarmupDuration(testContext *testing.T) {
	measurement := runIdentity{workload: workloadIdentity{Associations: 1, Duration: 120_000_000_000, Payload: "128", Mode: "throughput"}}
	warmup := measurement
	warmup.workload.Duration = 30_000_000_000
	for _, order := range [][]struct {
		run    runIdentity
		warmup bool
	}{
		{{warmup, true}, {measurement, false}, {warmup, true}},
		{{measurement, false}, {warmup, true}, {measurement, false}},
	} {
		var campaign campaignIdentity
		for index, step := range order {
			if err := campaign.add(step.run, step.warmup); err != nil {
				testContext.Fatalf("order %v step %d: %v", order, index, err)
			}
		}
		if campaign.workload.Duration != measurement.workload.Duration {
			testContext.Fatalf("campaign duration %v, want the measurement duration", campaign.workload.Duration)
		}
		otherPayload := warmup
		otherPayload.workload.Payload = "4096"
		if err := campaign.add(otherPayload, true); err == nil {
			testContext.Fatal("a warm-up with another payload joined the campaign")
		}
		otherDuration := measurement
		otherDuration.workload.Duration = 60_000_000_000
		if err := campaign.add(otherDuration, false); err == nil {
			testContext.Fatal("a measurement with another duration joined the campaign")
		}
	}
}

func TestSearchOutcomeSeparatesRateEvidenceFromMissingEvidence(testContext *testing.T) {
	direction := func(decision perfstats.Decision, reason string) directionDecision {
		return directionDecision{Decision: string(decision), Reason: reason}
	}
	for _, scenario := range []struct {
		decision probeDecision
		want     perfstats.ProbeOutcome
	}{
		{probeDecision{Decision: "pass"}, perfstats.ProbePassing},
		{probeDecision{Decision: "fail", Reason: perfstats.DeliveryFailuresReason}, perfstats.ProbeFailing},
		{probeDecision{Decision: "inconclusive", Reason: perfstats.TransportStallReason}, perfstats.ProbeNotDemonstrated},
		{probeDecision{Decision: "inconclusive", Reason: perfstats.BacklogUnresolvedReason}, perfstats.ProbeNotDemonstrated},
		{probeDecision{Decision: "inconclusive", Reason: perfstats.BacklogEvidenceMissingReason}, perfstats.ProbeInconclusive},
		{probeDecision{Decision: "inconclusive", Reason: perfstats.StallEvidenceMissingReason}, perfstats.ProbeInconclusive},
		{probeDecision{Decision: "inconclusive", Reason: "bidirectional-direction-inconclusive", Directions: []directionDecision{
			direction(perfstats.Inconclusive, perfstats.TransportStallReason), direction(perfstats.Inconclusive, perfstats.BacklogUnresolvedReason)}},
			perfstats.ProbeNotDemonstrated},
		{probeDecision{Decision: "inconclusive", Reason: "bidirectional-direction-inconclusive", Directions: []directionDecision{
			direction(perfstats.Pass, ""), direction(perfstats.Inconclusive, perfstats.TransportStallReason)}},
			perfstats.ProbeNotDemonstrated},
		{probeDecision{Decision: "inconclusive", Reason: "bidirectional-direction-inconclusive", Directions: []directionDecision{
			direction(perfstats.Inconclusive, perfstats.TransportStallReason), direction(perfstats.Inconclusive, perfstats.BacklogEvidenceMissingReason)}},
			perfstats.ProbeInconclusive},
		{probeDecision{Decision: "inconclusive", Reason: "bidirectional-direction-inconclusive", Directions: []directionDecision{
			direction(perfstats.Pass, ""), direction(perfstats.Pass, "")}},
			perfstats.ProbeInconclusive},
		// A demonstrated failure in one direction with a stall in the other is
		// decided stall-first (inconclusive) at the cohort level and so is not
		// demonstrated: it still bounds the bracket from above.
		{probeDecision{Decision: "inconclusive", Reason: "bidirectional-direction-inconclusive", Directions: []directionDecision{
			direction(perfstats.Fail, perfstats.DeliveryFailuresReason), direction(perfstats.Inconclusive, perfstats.TransportStallReason)}},
			perfstats.ProbeNotDemonstrated},
		{probeDecision{Decision: "surprise"}, perfstats.ProbeInconclusive},
	} {
		if got := searchOutcome(scenario.decision); got != scenario.want {
			testContext.Errorf("searchOutcome(%s) = %q, want %q", fmt.Sprintf("%+v", scenario.decision), got, scenario.want)
		}
	}
}
