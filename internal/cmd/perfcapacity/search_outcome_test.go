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

// A warm-up that failed for a reason unrelated to the offered rate, with every
// message delivered and no send blocked, says nothing about the rate. The
// record here is a real loss-free cohort whose run then failed after it.
func TestWarmupFailureWithoutLossOrStallIsNotEvidence(testContext *testing.T) {
	for _, reason := range []string{"prepare shared clock: peer clock domain or request envelope mismatch", "reset receiver: connection refused"} {
		testContext.Run(reason, func(testContext *testing.T) {
			record := searchRecord(testContext, "search-80000-measurement.json")
			record["phase"] = "warmup"
			record["verdict"] = "invalid"
			record["error"] = reason
			status, decoded := runRequest(testContext, searchRequest(testContext, record))
			if status != invalidInputExitStatus || !strings.Contains(decoded.Error, "without demonstrated loss or a stall") {
				testContext.Fatalf("status %d error %q, want a loss-free failed warm-up rejected", status, decoded.Error)
			}
		})
	}
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
		{probeDecision{Decision: "surprise"}, perfstats.ProbeInconclusive},
	} {
		if got := searchOutcome(scenario.decision); got != scenario.want {
			testContext.Errorf("searchOutcome(%s) = %q, want %q", fmt.Sprintf("%+v", scenario.decision), got, scenario.want)
		}
	}
}
