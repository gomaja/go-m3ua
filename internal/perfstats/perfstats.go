package perfstats

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
)

const (
	// RequiredPairCount is the default for requests that omit pair_count.
	RequiredPairCount       = 20
	studentTCritical975DF19 = 2.093024054408263
	maximumBaselineSpread   = 0.10
)

// studentTCritical975DF4 is the binary64 rounding of t(0.975, 4).
// Source: NIST/SEMATECH e-Handbook §1.3.6.6.4, probability density function:
// https://www.itl.nist.gov/div898/handbook/eda/section3/eda3664.htm
// For df=4, integrating f(t)=3/8*(1+t²/4)^(-5/2) gives
// F(t)=1/2+(3u-u³)/4, u=t/sqrt(t²+4). Inverting F(t)=0.975 by
// 90-digit decimal bisection gives 2.77644510519779435780310484674862756...
const studentTCritical975DF4 = 2.7764451051977943

type Direction string

const (
	UpperBound Direction = "upper"
	LowerBound Direction = "lower"
)

type Decision string

const (
	Pass         Decision = "pass"
	Fail         Decision = "fail"
	Inconclusive Decision = "inconclusive"
)

type Mode string

const (
	RatioMode    Mode = "ratio"
	ZeroCostMode Mode = "zero-cost"
)

type Range string

const (
	FiniteRange       Range = "finite"
	BelowFloat64Range Range = "below-float64"
	AboveFloat64Range Range = "above-float64"
	UndefinedRange    Range = "undefined"
)

const (
	IntervalStraddlesBoundaryReason = "interval-straddles-boundary"
	HighBaselineSpreadReason        = "baseline-spread-exceeds-10-percent"
	ZeroBaselineRegressionReason    = "positive-candidate-with-zero-baseline"
)

type Pair struct {
	ID        string  `json:"id"`
	Baseline  float64 `json:"baseline"`
	Candidate float64 `json:"candidate"`
}

type Gate struct {
	Direction Direction `json:"direction"`
	Boundary  float64   `json:"boundary"`
}

type RatioValue struct {
	Log   float64  `json:"log"`
	Ratio *float64 `json:"ratio"`
	Range Range    `json:"range"`
}

type LogRatio struct {
	PairID string  `json:"pair_id"`
	Value  float64 `json:"value"`
}

type Result struct {
	Decision             Decision   `json:"decision"`
	Mode                 Mode       `json:"mode"`
	PairCount            int        `json:"pair_count"`
	TCritical975         float64    `json:"t_critical_975"`
	Estimate             RatioValue `json:"estimate"`
	Lower                RatioValue `json:"lower"`
	Upper                RatioValue `json:"upper"`
	MeanLog              float64    `json:"mean_log"`
	StandardDeviationLog float64    `json:"standard_deviation_log"`
	BaselineSpread       float64    `json:"baseline_spread"`
	LogRatios            []LogRatio `json:"log_ratios,omitempty"`
	Reason               string     `json:"reason,omitempty"`
}

func Compare(pairs []Pair, gate Gate) (Result, error) {
	return CompareWithPairCount(pairs, gate, RequiredPairCount)
}

// CompareWithPairCount compares exactly pairCount matched pairs. Only 5 and 20
// are supported; Compare retains the default twenty-pair contract.
func CompareWithPairCount(pairs []Pair, gate Gate, pairCount int) (Result, error) {
	criticalValue, err := studentTCritical975(pairCount)
	if err != nil {
		return Result{}, err
	}
	if err := validateGate(gate); err != nil {
		return Result{}, err
	}
	if err := validatePairs(pairs, pairCount); err != nil {
		return Result{}, err
	}

	zeroBaselineCount := 0
	zeroBaselineRegression := false
	for _, pair := range pairs {
		if pair.Baseline == 0 {
			zeroBaselineCount++
			zeroBaselineRegression = zeroBaselineRegression || pair.Candidate > 0
		}
	}

	if zeroBaselineRegression {
		return zeroCostResult(Fail, ZeroBaselineRegressionReason, pairCount, criticalValue), nil
	}
	if zeroBaselineCount == pairCount {
		return zeroCostResult(Pass, "", pairCount, criticalValue), nil
	}
	if zeroBaselineCount > 0 {
		return Result{}, fmt.Errorf("mixed zero and positive baselines cannot form %d log ratios", pairCount)
	}

	return comparePositivePairs(pairs, gate, criticalValue), nil
}

func studentTCritical975(pairCount int) (float64, error) {
	switch pairCount {
	case 5:
		return studentTCritical975DF4, nil
	case RequiredPairCount:
		return studentTCritical975DF19, nil
	default:
		return 0, fmt.Errorf("pair_count must be 5 or 20, got %d", pairCount)
	}
}

func validateGate(gate Gate) error {
	if gate.Direction != UpperBound && gate.Direction != LowerBound {
		return fmt.Errorf("direction must be %q or %q", UpperBound, LowerBound)
	}
	if !isPositiveFinite(gate.Boundary) {
		return errors.New("boundary must be positive and finite")
	}
	return nil
}

func validatePairs(pairs []Pair, pairCount int) error {
	if len(pairs) != pairCount {
		return fmt.Errorf("exactly %d matched pairs are required, got %d", pairCount, len(pairs))
	}

	seenIDs := make(map[string]struct{}, pairCount)
	for index, pair := range pairs {
		pairID := strings.TrimSpace(pair.ID)
		if pairID == "" {
			return fmt.Errorf("pair %d has an empty ID", index+1)
		}
		if _, exists := seenIDs[pairID]; exists {
			return fmt.Errorf("duplicate pair ID %q", pairID)
		}
		seenIDs[pairID] = struct{}{}

		if !isNonnegativeFinite(pair.Baseline) {
			return fmt.Errorf("pair %q baseline must be nonnegative and finite", pairID)
		}
		if !isNonnegativeFinite(pair.Candidate) {
			return fmt.Errorf("pair %q candidate must be nonnegative and finite", pairID)
		}
		if pair.Baseline > 0 && pair.Candidate == 0 {
			return fmt.Errorf("pair %q candidate must be positive when baseline is positive", pairID)
		}
	}
	return nil
}

func comparePositivePairs(pairs []Pair, gate Gate, criticalValue float64) Result {
	pairCount := len(pairs)
	logRatios := make([]LogRatio, 0, pairCount)
	meanLog := 0.0
	sumSquaredDifferences := 0.0
	for index, pair := range pairs {
		logRatio := calculateLogRatio(pair.Candidate, pair.Baseline)
		logRatios = append(logRatios, LogRatio{PairID: pair.ID, Value: logRatio})
		count := float64(index + 1)
		difference := logRatio - meanLog
		meanLog += difference / count
		sumSquaredDifferences += difference * (logRatio - meanLog)
	}

	// NIST/SEMATECH §1.3.5.2: mean ± t(0.975, n-1)*s/sqrt(n),
	// applied to paired log ratios and exponentiated below.
	// https://www.itl.nist.gov/div898/handbook/eda/section3/eda352.htm
	standardDeviationLog := math.Sqrt(sumSquaredDifferences / float64(pairCount-1))
	margin := criticalValue * standardDeviationLog / math.Sqrt(float64(pairCount))
	lowerLog := meanLog - margin
	upperLog := meanLog + margin
	boundaryLog := math.Log(gate.Boundary)
	decision, reason := decide(gate.Direction, lowerLog, upperLog, boundaryLog)
	baselineSpread := calculateBaselineSpread(pairs)
	if baselineSpread > maximumBaselineSpread {
		decision = Inconclusive
		reason = HighBaselineSpreadReason
	}

	return Result{
		Decision:             decision,
		Mode:                 RatioMode,
		PairCount:            pairCount,
		TCritical975:         criticalValue,
		Estimate:             ratioValue(meanLog),
		Lower:                ratioValue(lowerLog),
		Upper:                ratioValue(upperLog),
		MeanLog:              meanLog,
		StandardDeviationLog: standardDeviationLog,
		BaselineSpread:       baselineSpread,
		LogRatios:            logRatios,
		Reason:               reason,
	}
}

func calculateLogRatio(candidate float64, baseline float64) float64 {
	ratio := candidate / baseline
	if ratio > 0 && !math.IsInf(ratio, 1) {
		differenceFromOne := ratio - 1
		if math.Abs(differenceFromOne) <= 0.5 {
			return math.Log1p(differenceFromOne)
		}
		return math.Log(ratio)
	}
	return math.Log(candidate) - math.Log(baseline)
}

func decide(direction Direction, lowerLog float64, upperLog float64, boundaryLog float64) (Decision, string) {
	switch direction {
	case UpperBound:
		if upperLog <= boundaryLog {
			return Pass, ""
		}
		if lowerLog > boundaryLog {
			return Fail, ""
		}
	case LowerBound:
		if lowerLog >= boundaryLog {
			return Pass, ""
		}
		if upperLog < boundaryLog {
			return Fail, ""
		}
	}
	return Inconclusive, IntervalStraddlesBoundaryReason
}

func zeroCostResult(decision Decision, reason string, pairCount int, criticalValue float64) Result {
	undefined := RatioValue{Range: UndefinedRange}
	return Result{
		Decision:     decision,
		Mode:         ZeroCostMode,
		PairCount:    pairCount,
		TCritical975: criticalValue,
		Estimate:     undefined,
		Lower:        undefined,
		Upper:        undefined,
		Reason:       reason,
	}
}

func calculateBaselineSpread(pairs []Pair) float64 {
	baselines := make([]float64, len(pairs))
	for index, pair := range pairs {
		baselines[index] = pair.Baseline
	}
	sort.Float64s(baselines)
	middle := len(baselines) / 2
	median := baselines[middle]
	if len(baselines)%2 == 0 {
		median = (baselines[middle-1] + baselines[middle]) / 2
		if math.IsInf(median, 0) {
			median = baselines[middle-1]/2 + baselines[middle]/2
		}
	}
	spread := (baselines[len(baselines)-1] - baselines[0]) / median
	if math.IsInf(spread, 1) {
		return math.MaxFloat64
	}
	return spread
}

func ratioValue(logValue float64) RatioValue {
	ratio := math.Exp(logValue)
	if math.IsInf(ratio, 1) {
		return RatioValue{Log: logValue, Range: AboveFloat64Range}
	}
	if ratio == 0 {
		return RatioValue{Log: logValue, Range: BelowFloat64Range}
	}
	return RatioValue{Log: logValue, Ratio: &ratio, Range: FiniteRange}
}

func isPositiveFinite(value float64) bool {
	return value > 0 && !math.IsInf(value, 0) && !math.IsNaN(value)
}

func isNonnegativeFinite(value float64) bool {
	return value >= 0 && !math.IsInf(value, 0) && !math.IsNaN(value)
}
