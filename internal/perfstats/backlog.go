package perfstats

import "math"

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
	BacklogUnresolvedReason      = "backlog-change-interval-spans-zero"
)

// BacklogThreshold is zero: a difference between means of integer counts can
// be fractional. Measurement uncertainty belongs in the interval bounds, not
// in an additional tolerance that can admit demonstrated positive growth.
const BacklogThreshold = 0.0

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

// Verdict reports nonincrease only when the whole valid interval is at or
// below zero, and growth only when it is strictly above zero. An interval
// spanning zero remains indeterminate. This finite-run comparison does not
// establish stability outside the observed measurement window.
func (interval BacklogInterval) Verdict() BacklogVerdict {
	if !interval.Valid() {
		return BacklogIndeterminate
	}
	if interval.Upper <= BacklogThreshold {
		return BacklogNotGrowing
	}
	if interval.Lower > BacklogThreshold {
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
// The eight counters are independent uint64 values, and unsigned addition
// wraps silently in Go, so the sum saturates at math.MaxUint64 instead: a run
// whose counters would overflow is still a lossy run, and must never be able
// to present a zero total to DecideRun.
func (counters RunCounters) Total() uint64 {
	total := uint64(0)
	for _, counter := range [...]uint64{
		counters.Missing, counters.Duplicate, counters.Invalid, counters.Reordered,
		counters.LateAfterStop, counters.Capped, counters.SendErrors, counters.DeadlineExceeded,
	} {
		if counter > math.MaxUint64-total {
			return math.MaxUint64
		}
		total += counter
	}
	return total
}

// RunEvidence is one full run's evidence for the sustained-rate decision.
// Interval is nil when the fixture could not produce a bounded sender-window
// backlog-change interval; missing evidence is inconclusive, never a pass.
//
// Stall is the run's transport-stall evidence. It is nil when the caller
// supplied none, and a run whose freedom from stalls was never observed cannot
// be credited with a sustained rate, so nil is inconclusive rather than read
// as "no stall".
type RunEvidence struct {
	FixtureValid bool
	Interval     *BacklogInterval
	Counters     RunCounters
	Stall        *StallObservation
}

// RunDecision is the per-run sustained-rate outcome. Stall echoes whatever
// stall evidence the run carried, whichever gate decided it, so a detected
// stall reaches the report instead of disappearing behind another reason.
type RunDecision struct {
	Decision Decision          `json:"decision"`
	Backlog  BacklogVerdict    `json:"backlog"`
	Reason   string            `json:"reason,omitempty"`
	Stall    *StallObservation `json:"stall,omitempty"`
}

// DecideRun decides whether one run demonstrates a sustained, loss-free rate.
// The evaluation order is fixed:
//
//  1. A detected transport stall is inconclusive, ahead of every other gate.
//     The fixture offers its schedule open loop, so a transport block of at
//     least one minimum retransmission timeout propagates into everything
//     measured through it: the outstanding-cap refusals, the delivery counters
//     and the backlog interval all sit downstream of the block, and none of
//     them can be attributed to the candidate. The run is not dropped and no
//     threshold moves for it; it is reported inconclusive with the stall
//     named. The cost of this order is stated rather than hidden: a stall the
//     candidate itself caused is reported inconclusive too. It is never
//     reported as a pass and the stall is always carried into the report, so a
//     campaign of such runs certifies nothing.
//  2. Fixture validity, then the loss counters: failures. A stall that was
//     merely never observed cannot excuse demonstrated loss.
//  3. Missing stall evidence: inconclusive. An unobserved run is not credited.
//  4. Backlog evidence presence and validity: inconclusive.
//  5. The predeclared interval rule.
func DecideRun(evidence RunEvidence) RunDecision {
	backlog := BacklogIndeterminate
	if evidence.Interval != nil {
		backlog = evidence.Interval.Verdict()
	}
	// The echoed observation is a copy. The decision is reported and encoded
	// elsewhere, and handing back the caller's own pointer would let either
	// side alter the other's record of the run after the fact.
	decide := func(decision Decision, reason string) RunDecision {
		result := RunDecision{Decision: decision, Backlog: backlog, Reason: reason}
		if evidence.Stall != nil {
			echoed := *evidence.Stall
			result.Stall = &echoed
		}
		return result
	}
	if evidence.Stall != nil && evidence.Stall.Stalled() {
		return decide(Inconclusive, TransportStallReason)
	}
	if !evidence.FixtureValid {
		return decide(Fail, FixtureInvalidReason)
	}
	if evidence.Counters.Total() != 0 {
		return decide(Fail, DeliveryFailuresReason)
	}
	if evidence.Stall == nil {
		return decide(Inconclusive, StallEvidenceMissingReason)
	}
	if evidence.Interval == nil || !evidence.Interval.Valid() {
		return decide(Inconclusive, BacklogEvidenceMissingReason)
	}
	switch backlog {
	case BacklogGrowing:
		return decide(Fail, BacklogGrowingReason)
	case BacklogNotGrowing:
		return decide(Pass, "")
	default:
		return decide(Inconclusive, BacklogUnresolvedReason)
	}
}
