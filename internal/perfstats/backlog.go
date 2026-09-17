package perfstats

import "math"

// This file fixes the sustained-backlog decision method BEFORE any capacity
// campaign run. The rule is pure interval arithmetic over the sender-window
// backlog-change interval and admits no numeric tolerance. It compares that
// interval against the instrument's own resolution, BacklogResolution, which
// is derived below from how the fixture counts and not from any run's outcome:
// an interval that spans that resolution is indeterminate, not "approximately
// stable".
//
// BacklogResolution is predeclared for the same reason the comparisons are.
// Tuning it after observing campaign results is forbidden: raising it so that
// a growing or unresolved row reports not-growing, and lowering it so that an
// inconvenient row reports growing, are both the post-hoc threshold change
// budgets section 5 forbids, exactly as adding a percentage allowance for
// boundary uncertainty or repeating until pass would be. The only admissible
// reason to change it is a change in how the fixture counts, and that change
// must be stated in the derivation below. Do not add any further allowance
// here.
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
	BacklogUnresolvedReason      = "backlog-change-interval-spans-instrument-resolution"
)

// BacklogResolution is the least count of the instrument that produces the
// backlog-change interval, expressed in that interval's own unit: messages.
// It is one message, derived from the perftraffic fixture's counting and from
// nothing else:
//
//  1. The measured quantity is a count of messages. analyzeProgress brackets
//     the work outstanding at a progress snapshot as
//     [max(0, offered(before)-unique), offered(after)-unique], where offered()
//     and unique are uint64 message counts. The fixture offers, transports and
//     validates whole messages and holds no sub-message state, so it has
//     nothing to record between "n outstanding" and "n+1 outstanding".
//  2. The offered schedule is quantised to whole messages by construction.
//     offeredAt is floor(elapsed*rate/second)+1, the same integer expression
//     dispatchScheduled uses to decide how many messages are due, so two
//     instants inside one message-emission period are indistinguishable to it
//     and its least count is exactly one message.
//  3. describeBacklogChange differences means of those counts: it sums a
//     quarter of the samples' bounds on each side and divides by the number of
//     samples in a quarter. Dividing by a positive integer rescales the bounds
//     but cannot manufacture a distinction the counted quantity does not
//     carry. The bounds describe a level difference and not a rate: "the
//     backlog grew" means at least one more message is outstanding at the end
//     of the window than at its start, and below that the two levels are the
//     same count.
//
// So the smallest backlog change this instrument can tell apart from no change
// is one message. The value is a property of the fixture's counting alone: it
// does not depend on the offered rate, the run duration, the sample count or
// any observed interval, and no campaign result was consulted to obtain it.
//
// The sampling method's own uncertainty is deliberately not used as the
// resolution. Each sample's bracket is as wide as the offered schedule
// advances during one progress round trip, and describeBacklogChange already
// carries that width into the interval: it is exactly Upper - Lower. Using it
// as the threshold as well would make the rule self-referential, because
// Upper <= Upper - Lower is only Lower <= 0, and would leave indeterminate
// with nothing to cover. The measurement window length and the number of
// samples in a quarter scale the arithmetic but likewise put no floor on how
// finely two message counts can differ. The threshold has to be a property of
// the instrument that does not vary with the run, and the message granularity
// is the only such property the fixture has.
//
// The fixture's own coarse series diagnostic, assessBacklog, independently
// treats a first-to-last-quarter mean difference of at most one outstanding
// message as "not growing". That corroborates the least count; its additional
// 1.10 proportional allowance is exactly the kind of percentage tolerance this
// rule forbids and is deliberately not adopted here.
const BacklogResolution = 1.0

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

// Verdict applies the predeclared interval rule against the instrument's
// resolution: not-growing iff Upper <= BacklogResolution, growing iff
// Lower > BacklogResolution, otherwise indeterminate. Invalid evidence is
// indeterminate; it is never resolved in favor of either side.
//
// The comparison is against BacklogResolution rather than zero because the
// quantity being bounded is a difference of message counts whose least count
// is one message. Comparing it against zero demands a strict negative about a
// quantity whose correct steady-state value is exactly zero: a genuinely flat
// run's interval brackets zero from both sides, so not-growing would be
// unreachable for exactly the runs the rule exists to accept. That is not a
// strict rule, it is an unfalsifiable one.
//
// The two comparisons cannot both hold. Lower <= Upper on valid evidence, so
// Upper <= BacklogResolution implies Lower <= BacklogResolution.
func (interval BacklogInterval) Verdict() BacklogVerdict {
	if !interval.Valid() {
		return BacklogIndeterminate
	}
	if interval.Upper <= BacklogResolution {
		return BacklogNotGrowing
	}
	if interval.Lower > BacklogResolution {
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
		return RunDecision{Decision: Inconclusive, Backlog: backlog, Reason: BacklogUnresolvedReason}
	}
}
