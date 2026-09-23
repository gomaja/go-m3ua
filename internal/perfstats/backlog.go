package perfstats

import (
	"errors"
	"math"
	"time"
)

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
	BacklogUnresolvedReason      = "backlog-growth-bounds-straddle-floor"
)

// BacklogFloorDuration is the materiality floor of the sustained-backlog rule.
// Growth over the measurement window is material only when it exceeds the
// traffic offered in this long: at 25,000 messages/s the floor is 250
// messages, at 5,000 messages/s it is 50. A stationary queue wobbles by a few
// messages from sample to sample, so any rule without a floor classifies a
// healthy run as growing whenever its last samples happen to sit above its
// first ones.
const BacklogFloorDuration = 10 * time.Millisecond

// BacklogTrendConfidence is the one-sided confidence of each growth bound.
const BacklogTrendConfidence = 0.99

// MinimumBacklogSamples is the fewest in-window samples a trend is fitted to.
const MinimumBacklogSamples = 8

// normalQuantile99 is the standard normal 0.99 quantile.
const normalQuantile99 = 2.3263478740408408

// BacklogSample is one bracketed observation of outstanding offered work at an
// offset inside the measurement window. The true backlog at At lies within
// [Lower, Upper]; the width is the offered traffic that arrived while the
// observation was being taken.
type BacklogSample struct {
	At    time.Duration
	Lower float64
	Upper float64
}

// BacklogTrend bounds the backlog growth over the measurement window, in
// messages. Growth bounds are the least-squares slope bounds multiplied by the
// window length. The slope bounds combine two separate uncertainties: the
// exact range of least-squares slopes over every backlog path that stays
// inside the sample brackets, widened on each side by the one-sided 99%
// Newey-West (autocorrelation-robust) standard error of the slope fitted to
// the bracket midpoints.
type BacklogTrend struct {
	SampleCount int           `json:"sample_count"`
	Window      time.Duration `json:"window_ns"`
	Floor       float64       `json:"floor"`
	Lag         int           `json:"lag"`
	SlopeLower  float64       `json:"slope_lower"`
	SlopeUpper  float64       `json:"slope_upper"`
	GrowthLower float64       `json:"growth_lower"`
	GrowthUpper float64       `json:"growth_upper"`
}

// Valid reports whether the trend is usable evidence: enough samples, a
// positive window, finite bounds in order and a finite non-negative floor.
func (trend BacklogTrend) Valid() bool {
	for _, value := range [...]float64{trend.Floor, trend.SlopeLower, trend.SlopeUpper, trend.GrowthLower, trend.GrowthUpper} {
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return false
		}
	}
	return trend.SampleCount >= MinimumBacklogSamples && trend.Window > 0 && trend.Floor >= 0 &&
		trend.Lag >= 0 && trend.SlopeLower <= trend.SlopeUpper && trend.GrowthLower <= trend.GrowthUpper
}

// Verdict reports growth only when the whole growth interval lies above the
// floor, and non-growth only when it lies at or below it. An interval that
// straddles the floor remains indeterminate. This finite-run comparison does
// not establish stability outside the observed measurement window.
func (trend BacklogTrend) Verdict() BacklogVerdict {
	if !trend.Valid() {
		return BacklogIndeterminate
	}
	if trend.GrowthUpper <= trend.Floor {
		return BacklogNotGrowing
	}
	if trend.GrowthLower > trend.Floor {
		return BacklogGrowing
	}
	return BacklogIndeterminate
}

var (
	errBacklogSamples = errors.New("backlog trend needs at least eight in-window samples")
	errBacklogWindow  = errors.New("backlog trend needs a positive window and offered rate")
	errBacklogSample  = errors.New("backlog samples must lie inside the window in strictly increasing order with finite, ordered, non-negative bounds")
)

// BacklogFloor is the materiality floor for an offered rate in messages per
// second.
func BacklogFloor(rate uint64) float64 {
	return float64(rate) * BacklogFloorDuration.Seconds()
}

// NeweyWestLag is the Bartlett-kernel truncation lag for n observations,
// floor(4*(n/100)^(2/9)).
func NeweyWestLag(n int) int {
	if n <= 0 {
		return 0
	}
	return int(math.Floor(4 * math.Pow(float64(n)/100, 2.0/9.0)))
}

// EstimateBacklogTrend fits the sustained-backlog trend of one measurement
// window. Samples must lie strictly inside the window, in strictly increasing
// time order, and rate is the offered rate in messages per second.
func EstimateBacklogTrend(samples []BacklogSample, window time.Duration, rate uint64) (BacklogTrend, error) {
	if window <= 0 || rate == 0 {
		return BacklogTrend{}, errBacklogWindow
	}
	n := len(samples)
	if n < MinimumBacklogSamples {
		return BacklogTrend{}, errBacklogSamples
	}
	times := make([]float64, n)
	midpoints := make([]float64, n)
	var meanTime, meanMidpoint float64
	for index, sample := range samples {
		if sample.At <= 0 || sample.At >= window || index > 0 && sample.At <= samples[index-1].At ||
			!(sample.Lower >= 0) || !(sample.Upper >= sample.Lower) || math.IsInf(sample.Upper, 0) {
			return BacklogTrend{}, errBacklogSample
		}
		times[index] = sample.At.Seconds()
		midpoints[index] = sample.Lower + (sample.Upper-sample.Lower)/2
		meanTime += times[index]
		meanMidpoint += midpoints[index]
	}
	meanTime /= float64(n)
	meanMidpoint /= float64(n)

	deviations := make([]float64, n)
	var sxx, sxy float64
	for index := range samples {
		deviations[index] = times[index] - meanTime
		sxx += deviations[index] * deviations[index]
		sxy += deviations[index] * (midpoints[index] - meanMidpoint)
	}
	if !(sxx > 0) {
		return BacklogTrend{}, errBacklogSample
	}
	slope := sxy / sxx

	// The least-squares slope is linear in the observations, so its extremes
	// over every path inside the brackets take the upper bracket where the
	// weight is positive and the lower bracket where it is negative, and the
	// reverse. The weights sum to zero, so centring the observations on their
	// mean midpoint leaves the slope unchanged and keeps a constant series at
	// exactly zero instead of rounding residue.
	var slopeHigh, slopeLow float64
	for index, sample := range samples {
		weight := deviations[index] / sxx
		upper, lower := sample.Upper-meanMidpoint, sample.Lower-meanMidpoint
		if weight > 0 {
			slopeHigh += weight * upper
			slopeLow += weight * lower
		} else {
			slopeHigh += weight * lower
			slopeLow += weight * upper
		}
	}

	lag := NeweyWestLag(n)
	scores := make([]float64, n)
	for index := range samples {
		fitted := meanMidpoint + slope*(times[index]-meanTime)
		scores[index] = deviations[index] * (midpoints[index] - fitted)
	}
	var variance float64
	for index := range scores {
		variance += scores[index] * scores[index]
	}
	for distance := 1; distance <= lag; distance++ {
		weight := 1 - float64(distance)/float64(lag+1)
		var covariance float64
		for index := distance; index < n; index++ {
			covariance += scores[index] * scores[index-distance]
		}
		variance += 2 * weight * covariance
	}
	variance = max(variance, 0) * float64(n) / float64(n-2) / (sxx * sxx)
	margin := normalQuantile99 * math.Sqrt(variance)

	seconds := window.Seconds()
	trend := BacklogTrend{
		SampleCount: n,
		Window:      window,
		Floor:       BacklogFloor(rate),
		Lag:         lag,
		SlopeLower:  slopeLow - margin,
		SlopeUpper:  slopeHigh + margin,
	}
	trend.GrowthLower = trend.SlopeLower * seconds
	trend.GrowthUpper = trend.SlopeUpper * seconds
	if !trend.Valid() {
		return BacklogTrend{}, errBacklogSample
	}
	return trend, nil
}

// Trend statuses for windows that do not yield a fitted trend. A fitted trend
// reports its BacklogVerdict instead.
const (
	BacklogTrendInsufficientSamples = "insufficient-samples"
	BacklogTrendInvalidSamples      = "invalid-samples"
)

// BacklogObservation is one producer observation of outstanding offered work:
// the request bracket [Before, After], relative to the start of the
// measurement window, and the backlog bounds observed within it.
type BacklogObservation struct {
	Before time.Duration
	After  time.Duration
	Lower  uint64
	Upper  uint64
}

// DescribeBacklogTrend fits the trend of the observations taken strictly
// inside the measurement window, each placed at the midpoint of its request
// bracket. Producer and evaluator both call it, so the evaluator can recompute
// a producer's reported trend from its raw observations. The status is the
// fitted trend's verdict, or BacklogTrendInsufficientSamples or
// BacklogTrendInvalidSamples with only the sample count set.
func DescribeBacklogTrend(observations []BacklogObservation, window time.Duration, rate uint64) (string, BacklogTrend) {
	samples := make([]BacklogSample, 0, len(observations))
	for _, observation := range observations {
		if observation.Before > 0 && observation.After < window {
			samples = append(samples, BacklogSample{
				At:    observation.Before + (observation.After-observation.Before)/2,
				Lower: float64(observation.Lower),
				Upper: float64(observation.Upper),
			})
		}
	}
	if len(samples) < MinimumBacklogSamples {
		return BacklogTrendInsufficientSamples, BacklogTrend{SampleCount: len(samples)}
	}
	trend, err := EstimateBacklogTrend(samples, window, rate)
	if err != nil {
		return BacklogTrendInvalidSamples, BacklogTrend{SampleCount: len(samples)}
	}
	return string(trend.Verdict()), trend
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
// Trend is nil when the fixture could not produce a bounded sender-window
// backlog trend; missing evidence is inconclusive, never a pass.
//
// Stall is the run's transport-stall evidence. It is nil when the caller
// supplied none, and a run whose freedom from stalls was never observed cannot
// be credited with a sustained rate, so nil is inconclusive rather than read
// as "no stall".
type RunEvidence struct {
	FixtureValid bool
	Trend        *BacklogTrend
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
//     and the backlog trend all sit downstream of the block, and none of
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
//  5. The predeclared trend rule.
func DecideRun(evidence RunEvidence) RunDecision {
	backlog := BacklogIndeterminate
	if evidence.Trend != nil {
		backlog = evidence.Trend.Verdict()
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
	if evidence.Trend == nil || !evidence.Trend.Valid() {
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
