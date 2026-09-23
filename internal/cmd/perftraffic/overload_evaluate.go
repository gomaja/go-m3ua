package main

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/gomaja/go-m3ua/internal/perfstats"
)

const (
	overloadCriterionPass        = "pass"
	overloadCriterionFail        = "fail"
	overloadCriterionUnavailable = "unavailable"

	overloadVerdictFail = "fail"

	overloadRecovered    = "recovered"
	overloadNotRecovered = "not-recovered"
)

const overloadAcceptanceScope = "DATA overload row of performance budgets section 4 for this fixture run only: every offered message classified, configured bounds held, admitted throughput recovered; never nominal capacity evidence"

const overloadReconciliationScope = "per-message: the sender's accepted and indeterminate ledgers against the receiver's delivered ledger; the receiver's library discards are counted per association, not per message, so they explain undelivered accepted messages by count"

// overloadRecord is the overload object of a sender or receiver record. The
// receiver fills Receiver only; the sender fills everything else from its own
// accounting, the receiver's record and ledger, and the progress series.
type overloadRecord struct {
	Receiver        *overloadReceiverRecord `json:"receiver,omitempty"`
	Profile         string                  `json:"profile,omitempty"`
	NominalRate     uint64                  `json:"nominal_rate,omitempty"`
	RequestDeadline time.Duration           `json:"request_deadline_ns,omitempty"`
	Phases          []overloadPhaseResult   `json:"phases,omitempty"`
	Totals          *overloadClassCounts    `json:"totals,omitempty"`
	Reconciliation  *overloadReconciliation `json:"reconciliation,omitempty"`
	Bounds          *overloadBounds         `json:"bounds,omitempty"`
	Windows         []overloadWindow        `json:"windows,omitempty"`
	Recovery        []overloadRecovery      `json:"recovery,omitempty"`
	Series          []overloadSenderPoint   `json:"series,omitempty"`
	NotAdmitted     []uint64                `json:"not_admitted_by_scheduled_second,omitempty"`
	ErrorSamples    map[string]string       `json:"error_samples,omitempty"`
	OfferedShape    *overloadOfferedShape   `json:"offered_shape,omitempty"`
	SenderPeakRSS   uint64                  `json:"sender_peak_rss_bytes,omitempty"`
	SenderRSSError  string                  `json:"sender_peak_rss_error,omitempty"`
	Acceptance      *overloadAcceptance     `json:"acceptance,omitempty"`
}

// overloadPhaseResult is one phase's outcome accounting: the sender classes of
// the messages scheduled in it, and the receiver's view of the same messages.
type overloadPhaseResult struct {
	Multiplier string        `json:"multiplier"`
	Rate       uint64        `json:"rate"`
	Start      time.Duration `json:"start_ns"`
	Duration   time.Duration `json:"duration_ns"`
	FirstIndex uint64        `json:"first_index"`
	Scheduled  uint64        `json:"scheduled"`
	overloadClassCounts
	Delivered              uint64 `json:"delivered"`
	Duplicates             uint64 `json:"duplicates"`
	AcceptedUndelivered    uint64 `json:"accepted_undelivered"`
	IndeterminateDelivered uint64 `json:"indeterminate_delivered"`
}

type overloadReconciliation struct {
	Scope                    string `json:"scope"`
	Messages                 uint64 `json:"messages"`
	Offered                  uint64 `json:"offered"`
	Classified               uint64 `json:"classified"`
	AcceptedLedger           uint64 `json:"accepted_ledger"`
	IndeterminateLedger      uint64 `json:"indeterminate_ledger"`
	AcceptedAndIndeterminate uint64 `json:"accepted_and_indeterminate"`
	DeliveredLedger          uint64 `json:"delivered_ledger"`
	ReceiverUnique           uint64 `json:"receiver_unique"`
	Phantom                  uint64 `json:"phantom"`
	AcceptedUndelivered      uint64 `json:"accepted_undelivered"`
	IndeterminateDelivered   uint64 `json:"indeterminate_delivered"`
	IndeterminateUndelivered uint64 `json:"indeterminate_undelivered"`
	LibraryDiscarded         uint64 `json:"library_discarded"`
	UnexplainedMissingLower  uint64 `json:"unexplained_missing_lower"`
	UnexplainedMissingUpper  uint64 `json:"unexplained_missing_upper"`
	DiscardExcess            uint64 `json:"discard_excess"`
	Duplicates               uint64 `json:"duplicates"`
	Invalid                  uint64 `json:"invalid"`
	Misscoped                uint64 `json:"misscoped"`
	Reordered                uint64 `json:"reordered"`
	LateAfterStop            uint64 `json:"late_after_stop"`
	Error                    string `json:"error,omitempty"`
}

type overloadBounds struct {
	OutstandingLimit        int                              `json:"outstanding_limit"`
	MaxOutstanding          uint64                           `json:"max_outstanding"`
	FixtureQueueCapacity    []int                            `json:"fixture_queue_capacity"`
	FixtureQueueMax         []int                            `json:"fixture_queue_max"`
	ConfiguredQueueCapacity int                              `json:"configured_library_queue_capacity"`
	ReceiverAssociations    []overloadAssociationObservation `json:"receiver_associations"`
	SenderAssociations      []overloadAssociationObservation `json:"sender_associations"`
	NotEstablishedRefusals  uint64                           `json:"not_established_refusals"`
	ReceiverFatal           string                           `json:"receiver_fatal,omitempty"`
}

// overloadWindow is the interval between two consecutive progress
// observations. Its start and end are each bracketed; delivered is exact
// because both counts are atomic receiver snapshots.
type overloadWindow struct {
	StartLower     time.Duration `json:"start_lower_ns"`
	StartUpper     time.Duration `json:"start_upper_ns"`
	EndLower       time.Duration `json:"end_lower_ns"`
	EndUpper       time.Duration `json:"end_upper_ns"`
	Phase          int           `json:"phase"`
	Delivered      uint64        `json:"delivered"`
	OfferedLower   uint64        `json:"offered_lower"`
	OfferedUpper   uint64        `json:"offered_upper"`
	BacklogLower   uint64        `json:"admitted_backlog_lower"`
	BacklogUpper   uint64        `json:"admitted_backlog_upper"`
	DiscardedLower uint64        `json:"discarded_lower"`
	DiscardedUpper uint64        `json:"discarded_upper"`
}

// overloadRecovery is the recovery evaluation at one switch from a phase above
// the nominal rate to one at or below it.
type overloadRecovery struct {
	Switch           time.Duration `json:"switch_ns"`
	FromPhase        int           `json:"from_phase"`
	ToPhase          int           `json:"to_phase"`
	Rate             uint64        `json:"rate"`
	Allowance        time.Duration `json:"allowance_ns"`
	Tolerance        float64       `json:"tolerance"`
	Floor            float64       `json:"floor"`
	Status           string        `json:"status"`
	Reason           string        `json:"reason,omitempty"`
	RecoveryTime     time.Duration `json:"recovery_time_ns"`
	WindowStartLower time.Duration `json:"window_start_lower_ns"`
	WindowStartUpper time.Duration `json:"window_start_upper_ns"`
	WindowEndLower   time.Duration `json:"window_end_lower_ns"`
	WindowEndUpper   time.Duration `json:"window_end_upper_ns"`
	Delivered        uint64        `json:"delivered"`
	Required         float64       `json:"required"`
	Trend            backlogTrend  `json:"backlog_trend"`
	// NonGrowthBy says how non-growth after the window was shown: "trend"
	// (the trend rule's not-growing verdict) or "envelope" (no later admitted
	// backlog above the backlog at the window's start plus the floor).
	NonGrowthBy          string `json:"non_growth_by,omitempty"`
	BacklogAtWindowStart uint64 `json:"admitted_backlog_at_window_start"`
	PeakBacklogAfter     uint64 `json:"peak_admitted_backlog_after"`
	// Informational: messages scheduled after the recovery window in this
	// phase that were not admitted, receiver discards after it, and when the
	// last refusal of the phase was scheduled relative to the switch.
	NotAdmittedAfterWindow uint64        `json:"not_admitted_after_window"`
	DiscardedAfterWindow   uint64        `json:"discarded_after_window"`
	LastNotAdmittedSecond  time.Duration `json:"last_not_admitted_second_end_ns"`
}

// overloadShapeTolerance bounds how far the offered load may lag the phased
// schedule: the backlog trend rule's floor duration, 10 ms of offered
// traffic. At least 99% of messages must be emitted within it of their
// scheduled instant, and each per-second sample may fall at most this much of
// the phase's traffic short. The scheduler never emits early.
const overloadShapeTolerance = perfstats.BacklogFloorDuration

// overloadShapeMaximumLag bounds the single latest emission: a tenth of the
// one-second windows recovery is judged over, so no message is ever carried
// across a material share of a window. A rare scheduling hiccup of a few tens
// of milliseconds is disclosed in the record but does not change the phased
// shape.
const overloadShapeMaximumLag = 100 * time.Millisecond

// overloadOfferedShape judges whether the offered load followed the phased
// schedule. The scheduler records how long past its scheduled instant it
// emitted each message; the per-second series records the offered count at
// each sample, which must lie between schedule.due at that instant less the
// tolerance's worth of the current phase's traffic and schedule.due itself.
type overloadOfferedShape struct {
	Status             string              `json:"status"`
	Reason             string              `json:"reason,omitempty"`
	Tolerance          time.Duration       `json:"tolerance_ns"`
	MaximumLag         time.Duration       `json:"maximum_lag_ns"`
	EmissionLag        durationPercentiles `json:"emission_lag"`
	MaxEmissionLagAt   time.Duration       `json:"max_emission_lag_at_ns"`
	Samples            int                 `json:"samples"`
	MaxShortfall       uint64              `json:"max_shortfall"`
	MaxShortfallAt     time.Duration       `json:"max_shortfall_at_ns"`
	ShortfallAllowance uint64              `json:"shortfall_allowance_at_max"`
	MaxExcess          uint64              `json:"max_excess"`
	FinalMatchesTotals bool                `json:"final_point_matches_totals"`
	FinalPointOffset   time.Duration       `json:"final_point_offset_ns"`
}

const (
	overloadShapeFollowed = "followed"
	overloadShapeDeviated = "deviated"
)

// judgeOfferedShape applies the offered-shape rule to the scheduler lag and
// the per-second series, and checks that the series ends, after the drain, at
// the cohort's totals.
func judgeOfferedShape(schedule *phasedSchedule, series []overloadSenderPoint, totals overloadClassCounts, lag durationPercentiles, maxLagAt time.Duration) *overloadOfferedShape {
	shape := &overloadOfferedShape{
		Status: overloadShapeFollowed, Tolerance: overloadShapeTolerance, MaximumLag: overloadShapeMaximumLag,
		EmissionLag: lag, MaxEmissionLagAt: maxLagAt,
	}
	var reasons []string
	switch {
	case lag.Count != schedule.expected:
		reasons = append(reasons, fmt.Sprintf("the scheduler recorded the emission of %d of %d messages", lag.Count, schedule.expected))
	case lag.P99 > overloadShapeTolerance:
		reasons = append(reasons, fmt.Sprintf("the p99 emission lag %s exceeds %s", lag.P99, overloadShapeTolerance))
	}
	if lag.Max > overloadShapeMaximumLag {
		reasons = append(reasons, fmt.Sprintf("the scheduler emitted the message scheduled at %s %s late, beyond %s", maxLagAt, lag.Max, overloadShapeMaximumLag))
	}
	for _, point := range series {
		offset := time.Duration(point.OffsetMillis) * time.Millisecond
		if point.Final || offset <= 0 || offset >= schedule.duration {
			continue
		}
		shape.Samples++
		// The series offset is truncated to the millisecond: the sample was
		// taken within the millisecond after it, so the count due is at least
		// due(offset) and at most due at the end of that millisecond.
		due := schedule.due(offset)
		upper := schedule.due(offset + time.Millisecond - 1)
		allowance := max(uint64(math.Ceil(float64(schedule.phases[schedule.phaseAt(offset)].rate)*overloadShapeTolerance.Seconds())), 1)
		if point.Offered > upper {
			shape.MaxExcess = max(shape.MaxExcess, point.Offered-upper)
		}
		if shortfall := subtractFloor(due, point.Offered); shortfall > shape.MaxShortfall {
			shape.MaxShortfall, shape.MaxShortfallAt, shape.ShortfallAllowance = shortfall, offset, allowance
		}
		if shortfall := subtractFloor(due, point.Offered); shortfall > allowance {
			reasons = append(reasons, fmt.Sprintf("at %s %d messages were offered, %d short of the %d due, beyond %d", offset, point.Offered, shortfall, due, allowance))
		}
	}
	if shape.MaxExcess != 0 {
		reasons = append(reasons, fmt.Sprintf("the offered count ran %d ahead of the schedule", shape.MaxExcess))
	}
	if shape.Samples == 0 {
		reasons = append(reasons, "no per-second offered observation inside the measurement window")
	}
	if count := len(series); count > 0 && series[count-1].Final {
		final := series[count-1]
		totals.Unresolved = 0
		shape.FinalPointOffset = time.Duration(final.OffsetMillis) * time.Millisecond
		shape.FinalMatchesTotals = final.overloadClassCounts == totals
	}
	if !shape.FinalMatchesTotals {
		reasons = append(reasons, "the per-second class series does not end, after the drain, at the cohort totals")
	}
	if len(reasons) != 0 {
		shape.Status = overloadShapeDeviated
		if len(reasons) > 4 {
			reasons = append(reasons[:4], fmt.Sprintf("and %d more", len(reasons)-4))
		}
		shape.Reason = strings.Join(reasons, "; ")
	}
	return shape
}

type overloadCriterion struct {
	Name   string `json:"name"`
	Status string `json:"status"`
	Detail string `json:"detail"`
}

type overloadAcceptance struct {
	Scope    string              `json:"scope"`
	Verdict  string              `json:"verdict"`
	Criteria []overloadCriterion `json:"criteria"`
}

// overloadPoint is one progress observation reduced to what the overload
// evaluation needs, with its instant bracketed by [Before, After] relative to
// the measurement start.
type overloadPoint struct {
	Before         time.Duration
	After          time.Duration
	Unique         uint64
	DiscardedLower uint64
	DiscardedUpper uint64
	AcceptedLower  uint64
	AdmittedUpper  uint64
}

func (point overloadPoint) backlog() (uint64, uint64) {
	lower := subtractFloor(point.AcceptedLower, point.Unique+point.DiscardedUpper)
	upper := subtractFloor(point.AdmittedUpper, point.Unique+point.DiscardedLower)
	return lower, max(upper, lower)
}

// overloadPoints extracts the in-window observations that carry both the
// receiver's overload snapshot and the sender's bracketing marks.
func overloadPoints(specification runSpec, observations []progressObservation) ([]overloadPoint, error) {
	points := make([]overloadPoint, 0, len(observations))
	for _, observation := range observations {
		snapshot := observation.Snapshot
		if observation.Error != "" || snapshot == nil || snapshot.Overload == nil || observation.SenderBefore == nil || observation.SenderAfter == nil {
			continue
		}
		before, after := observation.Before, observation.After
		if specification.Clock != nil {
			if snapshot.Clock == nil {
				return nil, errors.New("shared-clock observation has no receiver capture")
			}
			resolution := specification.Clock.Domain.Resolution
			before = time.Duration(snapshot.Clock.Captured - resolution - specification.Clock.Start)
			after = time.Duration(snapshot.Clock.Captured + resolution - specification.Clock.Start)
		}
		if after < before || snapshot.Overload.DiscardedAfter < snapshot.Overload.DiscardedBefore ||
			observation.SenderAfter.Started < observation.SenderBefore.Started || observation.SenderAfter.Accepted < observation.SenderBefore.Accepted {
			return nil, errors.New("overload observation brackets are reversed")
		}
		point := overloadPoint{
			Before: before, After: after, Unique: snapshot.Delivery.Unique,
			DiscardedLower: snapshot.Overload.DiscardedBefore, DiscardedUpper: snapshot.Overload.DiscardedAfter,
			AcceptedLower: observation.SenderBefore.Accepted,
			AdmittedUpper: subtractFloor(observation.SenderAfter.Started, observation.SenderBefore.Refused),
		}
		if count := len(points); count > 0 {
			previous := points[count-1]
			if point.Before < previous.After || point.Unique < previous.Unique || point.DiscardedUpper < previous.DiscardedLower {
				return nil, errors.New("overload observations overlap or regress")
			}
		}
		points = append(points, point)
	}
	return points, nil
}

func overloadWindows(specification runSpec, schedule *phasedSchedule, points []overloadPoint) []overloadWindow {
	windows := make([]overloadWindow, 0, len(points))
	for index := 1; index < len(points); index++ {
		start, end := points[index-1], points[index]
		lower, upper := end.backlog()
		windows = append(windows, overloadWindow{
			StartLower: start.Before, StartUpper: start.After, EndLower: end.Before, EndUpper: end.After,
			Phase:        schedule.phaseAt(max(start.After, 0)),
			Delivered:    end.Unique - start.Unique,
			OfferedLower: subtractFloor(offeredAt(specification, end.Before), offeredAt(specification, start.After)),
			OfferedUpper: subtractFloor(offeredAt(specification, end.After), offeredAt(specification, start.Before)),
			BacklogLower: lower, BacklogUpper: upper,
			DiscardedLower: end.DiscardedLower, DiscardedUpper: end.DiscardedUpper,
		})
	}
	return windows
}

// evaluateRecoveries applies the recovery rule at every switch from a phase
// above the nominal rate to a phase at or below it.
//
// A recovery window is a one-second interval between consecutive progress
// observations that starts after the switch and no later than the allowance
// after it (taking the latest instant its start could have been). The first
// window counts that both delivers at least the offered rate over the longest
// interval it could have spanned, less the tolerance and the backlog trend
// rule's floor, and after which the admitted backlog does not grow.
//
// Non-growth is shown by the trend rule's not-growing verdict over the
// observations from the window to the end of the phase. The trend rule was
// built for stationary windows: across the drain of the backlog an overload
// phase leaves behind, its autocorrelation-robust bounds straddle the floor
// even when every observation is lower than the one before. So a window whose
// trend is not decided either way also counts when no later observation's
// admitted backlog exceeds the backlog at the window's start by more than the
// floor. A growing verdict is never excused.
func evaluateRecoveries(schedule *phasedSchedule, nominal uint64, points []overloadPoint, notAdmitted []uint64) []overloadRecovery {
	var recoveries []overloadRecovery
	for phase := 1; phase < len(schedule.phases); phase++ {
		previous, current := schedule.phases[phase-1], schedule.phases[phase]
		if previous.rate <= nominal || current.rate > nominal {
			continue
		}
		recoveries = append(recoveries, evaluateRecovery(phase, current, points, notAdmitted))
	}
	return recoveries
}

func evaluateRecovery(phase int, current schedulePhase, points []overloadPoint, notAdmitted []uint64) overloadRecovery {
	floor := perfstats.BacklogFloor(current.rate)
	base := overloadRecovery{
		Switch: current.start, FromPhase: phase - 1, ToPhase: phase, Rate: current.rate,
		Allowance: overloadRecoveryAllowance, Tolerance: overloadRecoveryTolerance, Floor: floor,
		Status: overloadCriterionUnavailable,
	}
	last := -1
	for second := int(current.start / time.Second); second < int(current.end/time.Second) && second < len(notAdmitted); second++ {
		if notAdmitted[second] > 0 {
			last = second
		}
	}
	if last >= 0 {
		base.LastNotAdmittedSecond = time.Duration(last+1)*time.Second - current.start
	}
	candidates := 0
	var growing, undecided *overloadRecovery
	for index := 0; index+1 < len(points); index++ {
		start, end := points[index], points[index+1]
		if start.Before < current.start || end.After > current.end || end.Before-start.After < overloadMinimumWindow {
			continue
		}
		if start.After-current.start > overloadRecoveryAllowance {
			break
		}
		candidates++
		required := requiredRecoveryDeliveries(current.rate, end.After-start.Before)
		delivered := end.Unique - start.Unique
		if float64(delivered) < required {
			continue
		}
		result := base
		result.WindowStartLower, result.WindowStartUpper = start.Before, start.After
		result.WindowEndLower, result.WindowEndUpper = end.Before, end.After
		result.Delivered, result.Required = delivered, required
		result.RecoveryTime = start.After - current.start
		result.BacklogAtWindowStart, _ = start.backlog()
		origin := start.After
		observations := make([]perfstats.BacklogObservation, 0, len(points))
		lastInPhase := end
		for _, point := range points[index+1:] {
			if point.After > current.end {
				break
			}
			lower, upper := point.backlog()
			observations = append(observations, perfstats.BacklogObservation{Before: point.Before - origin, After: point.After - origin, Lower: lower, Upper: upper})
			result.PeakBacklogAfter = max(result.PeakBacklogAfter, upper)
			lastInPhase = point
		}
		status, trend := perfstats.DescribeBacklogTrend(observations, current.end-origin, current.rate)
		result.Trend = backlogTrend{Status: status, BacklogTrend: trend}
		result.DiscardedAfterWindow = subtractFloor(lastInPhase.DiscardedUpper, end.DiscardedLower)
		for second := int((end.After + time.Second - 1) / time.Second); second < int(current.end/time.Second) && second < len(notAdmitted); second++ {
			result.NotAdmittedAfterWindow += notAdmitted[second]
		}
		envelope := len(observations) >= perfstats.MinimumBacklogSamples &&
			float64(result.PeakBacklogAfter) <= float64(result.BacklogAtWindowStart)+floor
		switch {
		case status == string(perfstats.BacklogNotGrowing):
			result.Status, result.NonGrowthBy = overloadRecovered, "trend"
			return result
		case status == string(perfstats.BacklogGrowing):
			result.Status = overloadNotRecovered
			result.Reason = "the admitted backlog trend after the recovery window is growing"
			if growing == nil {
				growing = &result
			}
		case envelope:
			result.Status, result.NonGrowthBy = overloadRecovered, "envelope"
			return result
		default:
			result.Reason = fmt.Sprintf("the admitted backlog trend after the recovery window is %s and the backlog later reached %d, above %d plus the floor", status, result.PeakBacklogAfter, result.BacklogAtWindowStart)
			if undecided == nil {
				undecided = &result
			}
		}
	}
	switch {
	case growing != nil:
		return *growing
	case undecided != nil:
		return *undecided
	case candidates == 0:
		base.Reason = "no one-second progress window starts within the allowance after the switch"
		return base
	}
	base.Status = overloadNotRecovered
	base.Reason = fmt.Sprintf("no window starting within %s of the switch delivered the offered rate", overloadRecoveryAllowance)
	return base
}

// reconcileOverload checks the per-message outcome identity. Every offered
// message must end in exactly one sender class; every delivered message must
// be one the sender accepted or reported indeterminate; and every accepted
// message the receiver never delivered must be explained by the receiver
// library's counted discards.
func reconcileOverload(totals overloadClassCounts, expected uint64, accepted, indeterminate, delivered bitmap, receiver runRecord) overloadReconciliation {
	result := overloadReconciliation{
		Scope: overloadReconciliationScope, Messages: expected, Offered: totals.Offered, Classified: totals.classified(),
		AcceptedLedger: accepted.count(), IndeterminateLedger: indeterminate.count(),
		AcceptedAndIndeterminate: accepted.countAnd(indeterminate),
		ReceiverUnique:           receiver.Delivery.Unique, Duplicates: receiver.Delivery.Duplicate,
		Invalid: receiver.Delivery.Invalid, Reordered: receiver.Delivery.Reordered, LateAfterStop: receiver.Delivery.LateAfterStop,
	}
	if receiver.Overload != nil && receiver.Overload.Receiver != nil {
		result.LibraryDiscarded = receiver.Overload.Receiver.Discarded
		result.Misscoped = receiver.Overload.Receiver.Misscoped
	} else {
		result.Error = "receiver record has no overload observations"
	}
	if delivered == nil {
		result.Error = strings.TrimPrefix(result.Error+"; receiver delivered ledger is unavailable", "; ")
		return result
	}
	result.DeliveredLedger = delivered.count()
	result.Phantom = delivered.countAndNot(accepted.union(indeterminate))
	result.AcceptedUndelivered = accepted.countAndNot(delivered)
	result.IndeterminateDelivered = indeterminate.countAnd(delivered)
	result.IndeterminateUndelivered = result.IndeterminateLedger - result.IndeterminateDelivered
	undelivered := result.AcceptedUndelivered + result.IndeterminateUndelivered
	result.UnexplainedMissingLower = subtractFloor(result.AcceptedUndelivered, result.LibraryDiscarded)
	result.UnexplainedMissingUpper = min(result.AcceptedUndelivered, subtractFloor(undelivered, result.LibraryDiscarded))
	result.DiscardExcess = subtractFloor(result.LibraryDiscarded, undelivered)
	return result
}

func (result overloadReconciliation) failures(totals overloadClassCounts) []string {
	var failures []string
	check := func(failed bool, format string, arguments ...any) {
		if failed {
			failures = append(failures, fmt.Sprintf(format, arguments...))
		}
	}
	check(result.Offered != result.Messages, "offered %d of %d scheduled messages", result.Offered, result.Messages)
	check(result.Classified != result.Offered || totals.Unresolved != 0, "%d offered messages reached no class", subtractFloor(result.Offered, result.Classified)+totals.Unresolved)
	check(totals.Aborted != 0, "%d messages were aborted by the scheduler", totals.Aborted)
	check(totals.Unclassified != 0, "%d WriteData results fell outside the DataWriteError contract", totals.Unclassified)
	check(totals.FixtureErrors != 0, "%d messages ended in fixture errors", totals.FixtureErrors)
	check(result.AcceptedLedger != totals.Accepted || result.IndeterminateLedger != totals.Indeterminate || result.AcceptedAndIndeterminate != 0,
		"sender ledgers disagree with the class counts (accepted %d/%d, indeterminate %d/%d, both %d)",
		result.AcceptedLedger, totals.Accepted, result.IndeterminateLedger, totals.Indeterminate, result.AcceptedAndIndeterminate)
	check(result.DeliveredLedger != result.ReceiverUnique, "delivered ledger %d disagrees with receiver unique %d", result.DeliveredLedger, result.ReceiverUnique)
	check(result.Phantom != 0, "%d delivered messages were never accepted or indeterminate", result.Phantom)
	check(result.UnexplainedMissingLower != 0, "%d accepted messages are missing beyond %d library discards", result.UnexplainedMissingLower, result.LibraryDiscarded)
	check(result.DiscardExcess != 0, "%d library discards exceed the undelivered attempted messages", result.DiscardExcess)
	check(result.Duplicates != 0, "%d duplicates (the fixture never resends, so none is attributable to an indeterminate send)", result.Duplicates)
	check(result.Invalid != 0, "%d invalid deliveries, %d of them mis-scoped", result.Invalid, result.Misscoped)
	check(result.Reordered != 0, "%d reordered deliveries", result.Reordered)
	check(result.LateAfterStop != 0, "%d deliveries after the receiver stopped", result.LateAfterStop)
	return failures
}

func (bounds overloadBounds) failures(associations int) []string {
	var failures []string
	check := func(failed bool, format string, arguments ...any) {
		if failed {
			failures = append(failures, fmt.Sprintf(format, arguments...))
		}
	}
	check(bounds.MaxOutstanding > uint64(bounds.OutstandingLimit), "fixture outstanding reached %d over the %d cap", bounds.MaxOutstanding, bounds.OutstandingLimit)
	for index := range bounds.FixtureQueueMax {
		check(index >= len(bounds.FixtureQueueCapacity) || bounds.FixtureQueueMax[index] > bounds.FixtureQueueCapacity[index],
			"fixture queue %d exceeded its capacity", index)
	}
	check(len(bounds.ReceiverAssociations) != associations || len(bounds.SenderAssociations) != associations,
		"observed %d receiver and %d sender associations, want %d each", len(bounds.ReceiverAssociations), len(bounds.SenderAssociations), associations)
	for _, association := range bounds.ReceiverAssociations {
		check(association.QueueCapacity != bounds.ConfiguredQueueCapacity || association.MaxQueued > association.QueueCapacity,
			"receiver association %d library DATA queue reached %d of capacity %d (configured %d)", association.Index, association.MaxQueued, association.QueueCapacity, bounds.ConfiguredQueueCapacity)
		check(!association.Active || association.EpochEnd != association.EpochStart,
			"receiver association %d ended %s at epoch %d (started at %d)", association.Index, association.State, association.EpochEnd, association.EpochStart)
	}
	for _, association := range bounds.SenderAssociations {
		check(!association.Active || association.EpochEnd != association.EpochStart,
			"sender association %d ended %s at epoch %d (started at %d)", association.Index, association.State, association.EpochEnd, association.EpochStart)
	}
	check(bounds.NotEstablishedRefusals != 0, "%d sends were refused because the association was not established", bounds.NotEstablishedRefusals)
	check(bounds.ReceiverFatal != "", "receiver failed: %s", bounds.ReceiverFatal)
	return failures
}

// decideOverload combines the criteria. A fixture failure makes the trial
// invalid; otherwise any failed criterion fails it, any criterion without
// evidence leaves it inconclusive, and only all four passing passes it.
func decideOverload(fixtureFailures []string, record *overloadRecord, associations int) *overloadAcceptance {
	acceptance := &overloadAcceptance{Scope: overloadAcceptanceScope}
	criterion := func(name string, failures []string, unavailable string, pass string) {
		entry := overloadCriterion{Name: name, Status: overloadCriterionPass, Detail: pass}
		switch {
		case len(failures) != 0:
			entry.Status, entry.Detail = overloadCriterionFail, strings.Join(failures, "; ")
		case unavailable != "":
			entry.Status, entry.Detail = overloadCriterionUnavailable, unavailable
		}
		acceptance.Criteria = append(acceptance.Criteria, entry)
	}
	criterion("fixture", fixtureFailures, "", "fixture, clock and progress evidence complete")

	reconciliation := record.Reconciliation
	var accountingFailures []string
	accountingUnavailable := ""
	if reconciliation == nil || reconciliation.Error != "" {
		accountingUnavailable = "per-message reconciliation unavailable"
		if reconciliation != nil {
			accountingUnavailable += ": " + reconciliation.Error
		}
	} else {
		accountingFailures = reconciliation.failures(*record.Totals)
	}
	accountingPass := ""
	if reconciliation != nil {
		accountingPass = fmt.Sprintf("%d offered = %d accepted + %d not sent + %d indeterminate + %d cap refusals + %d deadline expiries; %d delivered, %d accepted undelivered all explained by %d library discards; 0 phantom, 0 duplicate, 0 invalid",
			reconciliation.Offered, record.Totals.Accepted, record.Totals.NotSent.total(), record.Totals.Indeterminate,
			record.Totals.CapRefusedOutstanding+record.Totals.CapRefusedQueue, record.Totals.DeadlineExpired,
			reconciliation.DeliveredLedger, reconciliation.AcceptedUndelivered, reconciliation.LibraryDiscarded)
	}
	criterion("accounting", accountingFailures, accountingUnavailable, accountingPass)

	var boundsFailures []string
	boundsUnavailable := ""
	boundsPass := ""
	if record.Bounds == nil {
		boundsUnavailable = "bound observations unavailable"
	} else {
		boundsFailures = record.Bounds.failures(associations)
		maximum := 0
		for _, association := range record.Bounds.ReceiverAssociations {
			maximum = max(maximum, association.MaxQueued)
		}
		boundsPass = fmt.Sprintf("fixture outstanding peaked at %d of %d; library DATA queues peaked at %d of %d; every association stayed ASP-ACTIVE on its starting epoch",
			record.Bounds.MaxOutstanding, record.Bounds.OutstandingLimit, maximum, record.Bounds.ConfiguredQueueCapacity)
	}
	criterion("bounds", boundsFailures, boundsUnavailable, boundsPass)

	var recoveryFailures []string
	var recoveryUnavailable []string
	var recoveryPass []string
	for _, recovery := range record.Recovery {
		switch recovery.Status {
		case overloadRecovered:
			recoveryPass = append(recoveryPass, fmt.Sprintf("switch at %s recovered in %s (%d delivered, %.0f required), backlog trend %s",
				recovery.Switch, recovery.RecoveryTime, recovery.Delivered, recovery.Required, recovery.Trend.Status))
		case overloadNotRecovered:
			recoveryFailures = append(recoveryFailures, fmt.Sprintf("switch at %s: %s", recovery.Switch, recovery.Reason))
		default:
			recoveryUnavailable = append(recoveryUnavailable, fmt.Sprintf("switch at %s: %s", recovery.Switch, recovery.Reason))
		}
	}
	if len(record.Recovery) == 0 {
		recoveryUnavailable = append(recoveryUnavailable, "the profile has no recovery switch")
	}
	criterion("recovery", recoveryFailures, strings.Join(recoveryUnavailable, "; "), strings.Join(recoveryPass, "; "))

	acceptance.Verdict = verdictPass
	for _, entry := range acceptance.Criteria {
		switch {
		case entry.Name == "fixture" && entry.Status != overloadCriterionPass:
			acceptance.Verdict = verdictInvalid
			return acceptance
		case entry.Status == overloadCriterionFail:
			acceptance.Verdict = overloadVerdictFail
		case entry.Status == overloadCriterionUnavailable && acceptance.Verdict == verdictPass:
			acceptance.Verdict = verdictInconclusive
		}
	}
	return acceptance
}

// overloadEvidence is everything the sender gathered for one overload
// measurement cohort.
type overloadEvidence struct {
	specification      runSpec
	schedule           *phasedSchedule
	phases             []overloadClassCounts
	maxOutstanding     uint64
	accepted           bitmap
	indeterminate      bitmap
	notAdmitted        []uint64
	series             []overloadSenderPoint
	emissionLag        durationPercentiles
	maxEmissionLagAt   time.Duration
	errorSamples       map[string]string
	fixtureQueueMax    []int
	senderAssociations []overloadAssociationObservation
	receiver           runRecord
	delivered          bitmap
	deliveredErr       error
	observations       []progressObservation
	senderFatal        string
	senderWindow       *windowAccounting
	clockVerified      bool
}

// evaluateOverloadCohort assembles the sender's overload object and decides
// the acceptance criteria.
func evaluateOverloadCohort(evidence overloadEvidence) *overloadRecord {
	specification := evidence.specification
	schedule := evidence.schedule
	record := &overloadRecord{
		Profile: specification.Overload.Profile, NominalRate: specification.Overload.NominalRate,
		RequestDeadline: specification.Overload.RequestDeadline, Series: evidence.series,
		NotAdmitted: evidence.notAdmitted, ErrorSamples: evidence.errorSamples,
	}
	if peak, err := peakRSSBytes(); err != nil {
		record.SenderRSSError = err.Error()
	} else {
		record.SenderPeakRSS = peak
	}
	var totals overloadClassCounts
	receiverOverload := (*overloadReceiverRecord)(nil)
	if evidence.receiver.Overload != nil {
		receiverOverload = evidence.receiver.Overload.Receiver
	}
	for index, phase := range schedule.phases {
		counts := evidence.phases[index]
		counts.Unresolved = subtractFloor(counts.Offered, counts.classified())
		totals = totals.plus(counts)
		result := overloadPhaseResult{
			Multiplier: specification.Overload.Phases[index].Multiplier, Rate: phase.rate, Start: phase.start,
			Duration: phase.end - phase.start, FirstIndex: phase.first, Scheduled: phase.count, overloadClassCounts: counts,
		}
		if receiverOverload != nil && index < len(receiverOverload.UniqueByPhase) && index < len(receiverOverload.DuplicateByPhase) {
			result.Delivered = receiverOverload.UniqueByPhase[index]
			result.Duplicates = receiverOverload.DuplicateByPhase[index]
		}
		if evidence.delivered != nil {
			for message := phase.first; message < phase.first+phase.count; message++ {
				if evidence.accepted.has(message) && !evidence.delivered.has(message) {
					result.AcceptedUndelivered++
				}
				if evidence.indeterminate.has(message) && evidence.delivered.has(message) {
					result.IndeterminateDelivered++
				}
			}
		}
		record.Phases = append(record.Phases, result)
	}
	record.Totals = &totals
	reconciliation := reconcileOverload(totals, schedule.expected, evidence.accepted, evidence.indeterminate, evidence.delivered, evidence.receiver)
	if evidence.deliveredErr != nil {
		reconciliation.Error = strings.TrimPrefix(reconciliation.Error+"; "+evidence.deliveredErr.Error(), "; ")
	}
	record.Reconciliation = &reconciliation

	bounds := overloadBounds{
		OutstandingLimit: specification.Outstanding, MaxOutstanding: evidence.maxOutstanding,
		FixtureQueueCapacity: queueCapacities(specification.Associations, specification.Outstanding),
		FixtureQueueMax:      evidence.fixtureQueueMax, ConfiguredQueueCapacity: dataQueueSize,
		SenderAssociations: evidence.senderAssociations, NotEstablishedRefusals: totals.NotSent.NotEstablished,
		ReceiverFatal: evidence.receiver.FatalError,
	}
	if receiverOverload != nil {
		bounds.ReceiverAssociations = receiverOverload.Associations
	}
	record.Bounds = &bounds

	var fixtureFailures []string
	record.OfferedShape = judgeOfferedShape(schedule, evidence.series, totals, evidence.emissionLag, evidence.maxEmissionLagAt)
	if record.OfferedShape.Status != overloadShapeFollowed {
		fixtureFailures = append(fixtureFailures, "the offered load did not follow the phased schedule: "+record.OfferedShape.Reason)
	}
	if evidence.senderFatal != "" {
		fixtureFailures = append(fixtureFailures, "sender fixture failure: "+evidence.senderFatal)
	}
	if evidence.senderWindow == nil || evidence.senderWindow.Status != "bounded" {
		reason := "missing"
		if evidence.senderWindow != nil {
			reason = evidence.senderWindow.Reason
		}
		fixtureFailures = append(fixtureFailures, "progress observations are not bounded: "+reason)
	}
	if specification.Clock != nil && !evidence.clockVerified {
		fixtureFailures = append(fixtureFailures, "shared clock evidence is not verified")
	}
	if receiverOverload == nil {
		fixtureFailures = append(fixtureFailures, "receiver record has no overload observations")
	} else if receiverOverload.QueuePolls == 0 {
		fixtureFailures = append(fixtureFailures, "receiver never observed its DATA queues")
	}
	points, err := overloadPoints(specification, evidence.observations)
	if err != nil {
		fixtureFailures = append(fixtureFailures, "overload progress: "+err.Error())
	} else {
		record.Windows = overloadWindows(specification, schedule, points)
		record.Recovery = evaluateRecoveries(schedule, specification.Overload.NominalRate, points, evidence.notAdmitted)
	}
	record.Acceptance = decideOverload(fixtureFailures, record, specification.Associations)
	return record
}

// evaluateOverloadRecord sets an overload measurement record's verdicts. The
// sender record carries the acceptance decision; the receiver record checks
// only what the receiver alone can see. Neither is ever capacity evidence.
func (record *runRecord) evaluateOverloadRecord() {
	record.CapacityVerdict = "unavailable"
	record.BacklogAssessment = "overload trial: the admitted-backlog trend after each recovery switch is in overload.recovery; not a nominal backlog assessment"
	invalid := func(reason string) {
		record.Reasons = append(record.Reasons, reason)
		record.Verdict = verdictInvalid
		record.FixtureVerdict = verdictInvalid
	}
	if record.FatalError != "" {
		invalid("fatal network fixture error")
	}
	if record.Side == "receiver" {
		if record.Delivery.Duplicate != 0 || record.Delivery.Invalid != 0 || record.Delivery.Reordered != 0 || record.Delivery.LateAfterStop != 0 {
			invalid("receiver observed duplicate, invalid, reordered, or late traffic")
		}
		if record.Overload == nil || record.Overload.Receiver == nil {
			invalid("receiver overload observations are missing")
		} else {
			for _, association := range record.Overload.Receiver.Associations {
				if association.MaxQueued > association.QueueCapacity || association.QueueCapacity != dataQueueSize {
					invalid(fmt.Sprintf("association %d inbound DATA queue exceeded its configured bound", association.Index))
				}
				if !association.Active || association.EpochEnd != association.EpochStart {
					invalid(fmt.Sprintf("association %d did not stay ASP-ACTIVE on its starting epoch", association.Index))
				}
			}
		}
		if record.Verdict == verdictInvalid {
			return
		}
		record.Verdict = verdictPass
		record.FixtureVerdict = verdictPass
		record.Reasons = append(record.Reasons, "receiver-side overload checks only; the sender record's overload.acceptance decides the trial")
		return
	}
	if record.Overload == nil || record.Overload.Acceptance == nil {
		invalid("overload acceptance was not evaluated")
		return
	}
	if record.Verdict == verdictInvalid {
		return
	}
	acceptance := record.Overload.Acceptance
	var failed []string
	for _, criterion := range acceptance.Criteria {
		if criterion.Status != overloadCriterionPass {
			failed = append(failed, criterion.Name+" "+criterion.Status)
		}
	}
	switch acceptance.Verdict {
	case verdictPass:
		record.Verdict = verdictPass
		record.FixtureVerdict = verdictPass
		record.Reasons = append(record.Reasons, "overload acceptance passed; an overload trial is never capacity evidence")
	case verdictInconclusive:
		record.Verdict = verdictInconclusive
		record.FixtureVerdict = verdictPass
		record.Reasons = append(record.Reasons, "overload acceptance inconclusive: "+strings.Join(failed, ", "))
	case overloadVerdictFail:
		record.Verdict = verdictInvalid
		record.FixtureVerdict = verdictPass
		record.Reasons = append(record.Reasons, "overload acceptance failed: "+strings.Join(failed, ", "))
	default:
		invalid("overload fixture evidence is invalid: " + strings.Join(failed, ", "))
	}
}

// requiredRecoveryDeliveries is the recovery threshold for a window that may
// have spanned up to span at the given rate.
func requiredRecoveryDeliveries(rate uint64, span time.Duration) float64 {
	return math.Max(0, float64(rate)*span.Seconds()*(1-overloadRecoveryTolerance)-perfstats.BacklogFloor(rate))
}
