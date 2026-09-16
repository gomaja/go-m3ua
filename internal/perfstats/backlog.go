package perfstats

import "math"

// This file fixes the sustained-backlog decision method BEFORE any capacity
// campaign run. The rule is pure interval arithmetic over the sender-window
// backlog-change interval and admits no numeric tolerance: an interval that
// straddles zero is indeterminate, not "approximately stable". Do not add
// allowances here after observing campaign results; budgets section 5 forbids
// repeat-until-pass and silent threshold changes.
//
// Inputs come from the perftraffic fixture's sender-window accounting: the
// first-to-last-quarter mean backlog-change interval plus the fixture-validity
// and loss counters. A capacity or throughput row may pass only with a
// not-growing interval, a loss-free run and a valid fixture.

type BacklogVerdict string

const (
	BacklogNotGrowing    BacklogVerdict = "not-growing"
	BacklogGrowing       BacklogVerdict = "growing"
	BacklogIndeterminate BacklogVerdict = "indeterminate"
)

const (
	FixtureInvalidReason         = "fixture-invalid"
	DeliveryFailuresReason       = "delivery-or-submission-failures"
	BacklogEvidenceMissingReason = "backlog-evidence-missing-or-invalid"
	BacklogGrowingReason         = "backlog-growing"
	BacklogStraddlesReason       = "backlog-change-interval-straddles-zero"
)

// BacklogInterval is the sender-window first-to-last-quarter mean
// backlog-change interval [Lower, Upper] in messages.
type BacklogInterval struct {
	Lower float64 `json:"lower"`
	Upper float64 `json:"upper"`
}

// Valid reports whether the interval is usable evidence: both bounds finite,
// not NaN, and Lower not above Upper.
func (interval BacklogInterval) Valid() bool {
	if math.IsNaN(interval.Lower) || math.IsNaN(interval.Upper) {
		return false
	}
	if math.IsInf(interval.Lower, 0) || math.IsInf(interval.Upper, 0) {
		return false
	}
	return interval.Lower <= interval.Upper
}

// Verdict applies the predeclared interval rule: not-growing iff Upper <= 0,
// growing iff Lower > 0, otherwise indeterminate. Invalid evidence is
// indeterminate; it is never resolved in favor of either side.
func (interval BacklogInterval) Verdict() BacklogVerdict {
	if !interval.Valid() {
		return BacklogIndeterminate
	}
	if interval.Upper <= 0 {
		return BacklogNotGrowing
	}
	if interval.Lower > 0 {
		return BacklogGrowing
	}
	return BacklogIndeterminate
}

// RunCounters are the per-run failure counters that must all be zero for a
// capacity or throughput row to pass. DeadlineExceeded covers echo-request
// deadline failures; Capped covers outstanding-cap refusals.
type RunCounters struct {
	Missing          uint64 `json:"missing"`
	Duplicate        uint64 `json:"duplicate"`
	Invalid          uint64 `json:"invalid"`
	Reordered        uint64 `json:"reordered"`
	LateAfterStop    uint64 `json:"late_after_stop"`
	Capped           uint64 `json:"capped"`
	SendErrors       uint64 `json:"send_errors"`
	DeadlineExceeded uint64 `json:"deadline_exceeded"`
}

// Total is the number of counted failures; a loss-free run has Total() == 0.
func (counters RunCounters) Total() uint64 {
	return counters.Missing + counters.Duplicate + counters.Invalid + counters.Reordered +
		counters.LateAfterStop + counters.Capped + counters.SendErrors + counters.DeadlineExceeded
}

// RunEvidence is one full run's evidence for the sustained-rate decision.
// Interval is nil when the fixture could not produce a bounded sender-window
// backlog-change interval; missing evidence is inconclusive, never a pass.
type RunEvidence struct {
	FixtureValid bool
	Interval     *BacklogInterval
	Counters     RunCounters
}

// RunDecision is the per-run sustained-rate outcome.
type RunDecision struct {
	Decision Decision       `json:"decision"`
	Backlog  BacklogVerdict `json:"backlog"`
	Reason   string         `json:"reason,omitempty"`
}

// DecideRun decides whether one run demonstrates a sustained, loss-free rate.
// The evaluation order is fixed: fixture validity and loss counters first
// (failures), then evidence presence and validity (inconclusive), then the
// predeclared interval rule.
func DecideRun(evidence RunEvidence) RunDecision {
	backlog := BacklogIndeterminate
	if evidence.Interval != nil {
		backlog = evidence.Interval.Verdict()
	}
	if !evidence.FixtureValid {
		return RunDecision{Decision: Fail, Backlog: backlog, Reason: FixtureInvalidReason}
	}
	if evidence.Counters.Total() != 0 {
		return RunDecision{Decision: Fail, Backlog: backlog, Reason: DeliveryFailuresReason}
	}
	if evidence.Interval == nil || !evidence.Interval.Valid() {
		return RunDecision{Decision: Inconclusive, Backlog: backlog, Reason: BacklogEvidenceMissingReason}
	}
	switch backlog {
	case BacklogGrowing:
		return RunDecision{Decision: Fail, Backlog: backlog, Reason: BacklogGrowingReason}
	case BacklogNotGrowing:
		return RunDecision{Decision: Pass, Backlog: backlog}
	default:
		return RunDecision{Decision: Inconclusive, Backlog: backlog, Reason: BacklogStraddlesReason}
	}
}
