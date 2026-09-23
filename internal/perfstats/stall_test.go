package perfstats

import (
	"testing"
	"time"
)

// StallThreshold is derived in stall.go from SCTP's minimum retransmission
// timeout, not chosen from results. Pinning the derived value here makes a
// post-hoc change to it trip a named test instead of quietly moving verdicts.
func TestStallThresholdIsOneSecond(testContext *testing.T) {
	if StallThreshold != time.Second {
		testContext.Fatalf("StallThreshold = %v, want 1s: RFC 9260 Section 16 puts RTO.Min and RTO.Initial at one second", StallThreshold)
	}
}

// The comparison is >= StallThreshold, so the boundary is probed from both
// sides by a single nanosecond as well as at the boundary itself.
func TestStallDetectionBoundary(testContext *testing.T) {
	tests := []struct {
		name        string
		longestSend time.Duration
		want        bool
	}{
		{name: "a measured send call", longestSend: 262144 * time.Nanosecond, want: false},
		{name: "half a second, below the retransmission floor", longestSend: 500 * time.Millisecond, want: false},
		{name: "one nanosecond below the threshold", longestSend: StallThreshold - time.Nanosecond, want: false},
		{name: "exactly at the threshold", longestSend: StallThreshold, want: true},
		{name: "one nanosecond above the threshold", longestSend: StallThreshold + time.Nanosecond, want: true},
		{name: "the observed reference stall", longestSend: 1200 * time.Millisecond, want: true},
		{name: "no sends recorded", longestSend: 0, want: false},
	}
	for _, test := range tests {
		testContext.Run(test.name, func(testContext *testing.T) {
			if got := (StallObservation{LongestSend: test.longestSend}).Stalled(); got != test.want {
				testContext.Fatalf("Stalled() = %v for %v, want %v", got, test.longestSend, test.want)
			}
		})
	}
}

// A detected stall contaminates every quantity measured through it, so it is
// decided ahead of every other gate. The run is reported inconclusive with the
// stall named; it is neither dropped nor charged to the candidate.
func TestDetectedStallIsInconclusiveAheadOfEveryOtherGate(testContext *testing.T) {
	tests := []struct {
		name     string
		evidence RunEvidence
	}{
		{name: "otherwise a clean pass", evidence: RunEvidence{FixtureValid: true, Trend: trend(-1, 0, 250), Stall: stalled()}},
		{name: "otherwise fixture-invalid", evidence: RunEvidence{FixtureValid: false, Trend: trend(-1, 0, 250), Stall: stalled()}},
		{name: "otherwise a capped submission failure", evidence: RunEvidence{
			FixtureValid: true, Trend: trend(-1, 0, 250), Counters: RunCounters{Capped: 4231}, Stall: stalled(),
		}},
		{name: "otherwise a missing delivery failure", evidence: RunEvidence{
			FixtureValid: false, Trend: trend(251, 300, 250), Counters: RunCounters{Missing: 4231}, Stall: stalled(),
		}},
		{name: "otherwise a growing backlog", evidence: RunEvidence{FixtureValid: true, Trend: trend(251, 300, 250), Stall: stalled()}},
		{name: "otherwise missing backlog evidence", evidence: RunEvidence{FixtureValid: true, Stall: stalled()}},
	}
	for _, test := range tests {
		testContext.Run(test.name, func(testContext *testing.T) {
			decision := DecideRun(test.evidence)
			if decision.Decision != Inconclusive || decision.Reason != TransportStallReason {
				testContext.Fatalf("DecideRun() = %+v, want inconclusive with %q", decision, TransportStallReason)
			}
			if decision.Stall == nil || *decision.Stall != *test.evidence.Stall {
				testContext.Fatalf("decision dropped the stall evidence: %+v", decision)
			}
		})
	}
}

// The stall is recorded in the run's evidence so it reaches the report, not
// only when it decided the run.
func TestStallEvidenceIsEchoedIntoEveryDecision(testContext *testing.T) {
	tests := []struct {
		name     string
		evidence RunEvidence
		want     Decision
	}{
		{name: "pass", evidence: RunEvidence{FixtureValid: true, Trend: trend(-1, 0, 250), Stall: unstalled()}, want: Pass},
		{name: "fixture-invalid failure", evidence: RunEvidence{FixtureValid: false, Trend: trend(-1, 0, 250), Stall: unstalled()}, want: Fail},
		{name: "growing failure", evidence: RunEvidence{FixtureValid: true, Trend: trend(251, 300, 250), Stall: unstalled()}, want: Fail},
		{name: "unresolved trend", evidence: RunEvidence{FixtureValid: true, Trend: trend(240, 260, 250), Stall: unstalled()}, want: Inconclusive},
	}
	for _, test := range tests {
		testContext.Run(test.name, func(testContext *testing.T) {
			decision := DecideRun(test.evidence)
			if decision.Decision != test.want {
				testContext.Fatalf("DecideRun() = %+v, want %q", decision, test.want)
			}
			if decision.Stall == nil || *decision.Stall != *test.evidence.Stall {
				testContext.Fatalf("decision dropped the stall evidence: %+v", decision)
			}
		})
	}
}

// A run whose freedom from stalls was never observed cannot be credited with a
// sustained rate.
func TestMissingStallEvidenceCannotPass(testContext *testing.T) {
	decision := DecideRun(RunEvidence{FixtureValid: true, Trend: trend(-1, 0, 250)})
	if decision.Decision != Inconclusive || decision.Reason != StallEvidenceMissingReason {
		testContext.Fatalf("DecideRun() = %+v, want inconclusive with %q", decision, StallEvidenceMissingReason)
	}
	if decision.Stall != nil {
		testContext.Fatalf("decision invented stall evidence: %+v", decision)
	}
}

// Missing stall evidence is not a stall. It cannot excuse demonstrated loss or
// an invalid fixture, both of which are decided ahead of it.
func TestMissingStallEvidenceDoesNotMaskDemonstratedFailures(testContext *testing.T) {
	tests := []struct {
		name     string
		evidence RunEvidence
		reason   string
	}{
		{name: "invalid fixture", evidence: RunEvidence{FixtureValid: false, Trend: trend(-1, 0, 250)}, reason: FixtureInvalidReason},
		{name: "counted failure", evidence: RunEvidence{
			FixtureValid: true, Trend: trend(-1, 0, 250), Counters: RunCounters{Capped: 1},
		}, reason: DeliveryFailuresReason},
	}
	for _, test := range tests {
		testContext.Run(test.name, func(testContext *testing.T) {
			decision := DecideRun(test.evidence)
			if decision.Decision != Fail || decision.Reason != test.reason {
				testContext.Fatalf("DecideRun() = %+v, want Fail with %q", decision, test.reason)
			}
		})
	}
}

// The decision is reported and encoded elsewhere. Echoing the caller's own
// pointer would let either side alter the other's record of the run.
func TestEchoedStallEvidenceIsACopy(testContext *testing.T) {
	evidence := RunEvidence{FixtureValid: true, Trend: trend(-1, 0, 250), Stall: unstalled()}
	decision := DecideRun(evidence)
	if decision.Stall == nil {
		testContext.Fatalf("DecideRun() = %+v, want the stall echoed", decision)
	}
	if decision.Stall == evidence.Stall {
		testContext.Fatal("DecideRun echoed the caller's own stall observation instead of a copy")
	}
	decision.Stall.LongestSend = 99 * time.Second
	if evidence.Stall.LongestSend != unstalled().LongestSend {
		testContext.Fatalf("mutating the decision changed the evidence: %v", evidence.Stall.LongestSend)
	}
}
