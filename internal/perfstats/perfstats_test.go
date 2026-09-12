package perfstats

import (
	"math"
	"strconv"
	"strings"
	"testing"
)

func TestCompareKnownReference(testContext *testing.T) {
	pairs := make([]Pair, RequiredPairCount)
	for index := range pairs {
		pairs[index] = Pair{
			ID:        "pair-" + strconv.Itoa(index+1),
			Baseline:  100,
			Candidate: float64(101 + index),
		}
	}

	result, err := Compare(pairs, Gate{Direction: UpperBound, Boundary: 1.25})
	if err != nil {
		testContext.Fatalf("Compare() error = %v", err)
	}

	assertClose(testContext, "mean log", result.MeanLog, 0.09848043529764319, 1e-15)
	assertClose(testContext, "standard deviation log", result.StandardDeviationLog, 0.05364123053787502, 1e-15)
	assertRatioClose(testContext, "estimate", result.Estimate, 1.1034928146747218, 1e-15)
	assertRatioClose(testContext, "lower", result.Lower, 1.0761346212804537, 1e-15)
	assertRatioClose(testContext, "upper", result.Upper, 1.1315465258332151, 1e-15)
	if result.Decision != Pass {
		testContext.Fatalf("Decision = %q, want %q", result.Decision, Pass)
	}
	if result.PairCount != RequiredPairCount {
		testContext.Fatalf("PairCount = %d, want %d", result.PairCount, RequiredPairCount)
	}
	if result.Mode != RatioMode {
		testContext.Fatalf("Mode = %q, want %q", result.Mode, RatioMode)
	}
	if len(result.LogRatios) != RequiredPairCount || result.LogRatios[0].PairID != "pair-1" || result.LogRatios[RequiredPairCount-1].PairID != "pair-20" {
		testContext.Fatalf("LogRatios did not preserve pair order: %#v", result.LogRatios)
	}
}

func TestCompareRequiresExactlyTwentyPairs(testContext *testing.T) {
	tests := []struct {
		name  string
		count int
	}{
		{name: "nil", count: 0},
		{name: "nineteen", count: RequiredPairCount - 1},
		{name: "twenty one", count: RequiredPairCount + 1},
	}

	for _, test := range tests {
		testContext.Run(test.name, func(testContext *testing.T) {
			var pairs []Pair
			if test.name != "nil" {
				pairs = constantPairs(test.count, 1, 1)
			}

			_, err := Compare(pairs, Gate{Direction: UpperBound, Boundary: 1.1})
			if err == nil || !strings.Contains(err.Error(), "exactly 20") {
				testContext.Fatalf("Compare() error = %v, want exact pair-count error", err)
			}
		})
	}
}

func TestCompareRejectsMissingAndRepeatedPairIDs(testContext *testing.T) {
	tests := []struct {
		name   string
		mutate func([]Pair)
		want   string
	}{
		{
			name: "missing",
			mutate: func(pairs []Pair) {
				pairs[4].ID = ""
			},
			want: "pair 5 has an empty ID",
		},
		{
			name: "whitespace",
			mutate: func(pairs []Pair) {
				pairs[4].ID = "  \t"
			},
			want: "pair 5 has an empty ID",
		},
		{
			name: "repeated",
			mutate: func(pairs []Pair) {
				pairs[4].ID = pairs[1].ID
			},
			want: `duplicate pair ID "pair-2"`,
		},
	}

	for _, test := range tests {
		testContext.Run(test.name, func(testContext *testing.T) {
			pairs := constantPairs(RequiredPairCount, 1, 1)
			test.mutate(pairs)

			_, err := Compare(pairs, Gate{Direction: UpperBound, Boundary: 1.1})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				testContext.Fatalf("Compare() error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestCompareRejectsInvalidNumbers(testContext *testing.T) {
	tests := []struct {
		name   string
		mutate func([]Pair, *Gate)
	}{
		{name: "baseline negative", mutate: func(pairs []Pair, _ *Gate) { pairs[0].Baseline = -1 }},
		{name: "candidate negative", mutate: func(pairs []Pair, _ *Gate) { pairs[0].Candidate = -1 }},
		{name: "candidate zero with positive baseline", mutate: func(pairs []Pair, _ *Gate) { pairs[0].Candidate = 0 }},
		{name: "baseline NaN", mutate: func(pairs []Pair, _ *Gate) { pairs[0].Baseline = math.NaN() }},
		{name: "candidate positive infinity", mutate: func(pairs []Pair, _ *Gate) { pairs[0].Candidate = math.Inf(1) }},
		{name: "candidate negative infinity", mutate: func(pairs []Pair, _ *Gate) { pairs[0].Candidate = math.Inf(-1) }},
		{name: "boundary zero", mutate: func(_ []Pair, gate *Gate) { gate.Boundary = 0 }},
		{name: "boundary NaN", mutate: func(_ []Pair, gate *Gate) { gate.Boundary = math.NaN() }},
		{name: "boundary infinity", mutate: func(_ []Pair, gate *Gate) { gate.Boundary = math.Inf(1) }},
	}

	for _, test := range tests {
		testContext.Run(test.name, func(testContext *testing.T) {
			pairs := constantPairs(RequiredPairCount, 1, 1)
			gate := Gate{Direction: UpperBound, Boundary: 1.1}
			test.mutate(pairs, &gate)

			if _, err := Compare(pairs, gate); err == nil {
				testContext.Fatal("Compare() error = nil, want invalid-number error")
			}
		})
	}
}

func TestCompareRejectsInvalidDirection(testContext *testing.T) {
	_, err := Compare(constantPairs(RequiredPairCount, 1, 1), Gate{Direction: Direction("sideways"), Boundary: 1.1})
	if err == nil || !strings.Contains(err.Error(), "direction") {
		testContext.Fatalf("Compare() error = %v, want direction error", err)
	}
}

func TestCompareDecidesUpperBound(testContext *testing.T) {
	tests := []struct {
		name     string
		ratio    float64
		boundary float64
		want     Decision
	}{
		{name: "pass below", ratio: 1.09, boundary: 1.1, want: Pass},
		{name: "pass immediately below", ratio: 1.1 - 1e-12, boundary: 1.1, want: Pass},
		{name: "pass one float below", ratio: math.Nextafter(1.1, 0), boundary: 1.1, want: Pass},
		{name: "pass on boundary", ratio: 1.1, boundary: 1.1, want: Pass},
		{name: "fail one float above", ratio: math.Nextafter(1.1, math.Inf(1)), boundary: 1.1, want: Fail},
		{name: "fail immediately above", ratio: 1.1 + 1e-12, boundary: 1.1, want: Fail},
		{name: "fail above", ratio: 1.11, boundary: 1.1, want: Fail},
	}

	for _, test := range tests {
		testContext.Run(test.name, func(testContext *testing.T) {
			result, err := Compare(constantPairs(RequiredPairCount, 1, test.ratio), Gate{Direction: UpperBound, Boundary: test.boundary})
			if err != nil {
				testContext.Fatalf("Compare() error = %v", err)
			}
			if result.Decision != test.want {
				testContext.Fatalf("Decision = %q, want %q", result.Decision, test.want)
			}
		})
	}
}

func TestCompareDecidesLowerBound(testContext *testing.T) {
	tests := []struct {
		name     string
		ratio    float64
		boundary float64
		want     Decision
	}{
		{name: "pass above", ratio: 0.96, boundary: 0.95, want: Pass},
		{name: "pass immediately above", ratio: 0.95 + 1e-12, boundary: 0.95, want: Pass},
		{name: "pass one float above", ratio: math.Nextafter(0.95, math.Inf(1)), boundary: 0.95, want: Pass},
		{name: "pass on boundary", ratio: 0.95, boundary: 0.95, want: Pass},
		{name: "fail one float below", ratio: math.Nextafter(0.95, 0), boundary: 0.95, want: Fail},
		{name: "fail immediately below", ratio: 0.95 - 1e-12, boundary: 0.95, want: Fail},
		{name: "fail below", ratio: 0.94, boundary: 0.95, want: Fail},
	}

	for _, test := range tests {
		testContext.Run(test.name, func(testContext *testing.T) {
			result, err := Compare(constantPairs(RequiredPairCount, 1, test.ratio), Gate{Direction: LowerBound, Boundary: test.boundary})
			if err != nil {
				testContext.Fatalf("Compare() error = %v", err)
			}
			if result.Decision != test.want {
				testContext.Fatalf("Decision = %q, want %q", result.Decision, test.want)
			}
		})
	}
}

func TestCompareScaledBoundaryPrecision(testContext *testing.T) {
	scales := []struct {
		baseline          float64
		lowerCenterResult Decision
	}{
		{baseline: 1e-200, lowerCenterResult: Fail},
		{baseline: 1e200, lowerCenterResult: Pass},
	}
	for _, scale := range scales {
		baseline := scale.baseline
		upperBoundary := 1.1
		upperCandidate := upperBoundary * baseline
		upperTests := []struct {
			name      string
			candidate float64
			want      Decision
		}{
			{name: "below", candidate: math.Nextafter(upperCandidate, 0), want: Pass},
			{name: "center", candidate: upperCandidate, want: Pass},
			{name: "above", candidate: math.Nextafter(upperCandidate, math.Inf(1)), want: Fail},
		}
		for _, test := range upperTests {
			testContext.Run("upper "+strconv.FormatFloat(baseline, 'g', -1, 64)+" "+test.name, func(testContext *testing.T) {
				result, err := Compare(constantPairs(RequiredPairCount, baseline, test.candidate), Gate{Direction: UpperBound, Boundary: upperBoundary})
				if err != nil {
					testContext.Fatalf("Compare() error = %v", err)
				}
				if result.Decision != test.want {
					testContext.Fatalf("Decision = %q, want %q", result.Decision, test.want)
				}
			})
		}

		lowerBoundary := 0.95
		lowerCandidate := lowerBoundary * baseline
		lowerTests := []struct {
			name      string
			candidate float64
			want      Decision
		}{
			{name: "below", candidate: math.Nextafter(lowerCandidate, 0), want: Fail},
			{name: "center", candidate: lowerCandidate, want: scale.lowerCenterResult},
			{name: "above", candidate: math.Nextafter(lowerCandidate, math.Inf(1)), want: Pass},
		}
		for _, test := range lowerTests {
			testContext.Run("lower "+strconv.FormatFloat(baseline, 'g', -1, 64)+" "+test.name, func(testContext *testing.T) {
				result, err := Compare(constantPairs(RequiredPairCount, baseline, test.candidate), Gate{Direction: LowerBound, Boundary: lowerBoundary})
				if err != nil {
					testContext.Fatalf("Compare() error = %v", err)
				}
				if result.Decision != test.want {
					testContext.Fatalf("Decision = %q, want %q", result.Decision, test.want)
				}
			})
		}
	}
}

func TestCompareMatchesHighPrecisionScaledReference(testContext *testing.T) {
	pairs := make([]Pair, RequiredPairCount)
	for index := range pairs {
		baseline := 1e200
		if index%2 == 1 {
			baseline = 1e-200
		}
		pairs[index] = Pair{
			ID:        "pair-" + strconv.Itoa(index+1),
			Baseline:  baseline,
			Candidate: baseline * float64(101+index) / 100,
		}
	}

	result, err := Compare(pairs, Gate{Direction: UpperBound, Boundary: 1.25})
	if err != nil {
		testContext.Fatalf("Compare() error = %v", err)
	}

	assertClose(testContext, "mean log", result.MeanLog, 0.09848043529764318, 2e-15)
	assertClose(testContext, "standard deviation log", result.StandardDeviationLog, 0.05364123053787502, 2e-15)
	assertRatioClose(testContext, "estimate", result.Estimate, 1.1034928146747219, 2e-15)
	assertRatioClose(testContext, "lower", result.Lower, 1.0761346212804537, 2e-15)
	assertRatioClose(testContext, "upper", result.Upper, 1.1315465258332151, 2e-15)
	assertClose(testContext, "first log ratio", result.LogRatios[0].Value, 0.009950330853168019, 5e-16)
	assertClose(testContext, "last log ratio", result.LogRatios[19].Value, 0.18232155679395453, 5e-16)
}

func TestCompareIdenticalRatiosHaveZeroWidthInterval(testContext *testing.T) {
	result, err := Compare(constantPairs(RequiredPairCount, 8, 10), Gate{Direction: UpperBound, Boundary: 1.3})
	if err != nil {
		testContext.Fatalf("Compare() error = %v", err)
	}
	if result.StandardDeviationLog != 0 {
		testContext.Fatalf("StandardDeviationLog = %g, want 0", result.StandardDeviationLog)
	}
	estimate := finiteRatio(testContext, result.Estimate)
	if finiteRatio(testContext, result.Lower) != estimate || finiteRatio(testContext, result.Upper) != estimate {
		testContext.Fatalf("interval = [%v, %v], estimate = %v; want zero width", finiteRatio(testContext, result.Lower), finiteRatio(testContext, result.Upper), estimate)
	}
}

func TestCompareReportsStraddlingIntervalsAsInconclusive(testContext *testing.T) {
	pairs := alternatingPairs(0.9, 1.3)

	for _, gate := range []Gate{
		{Direction: UpperBound, Boundary: 1.1},
		{Direction: LowerBound, Boundary: 1.05},
	} {
		result, err := Compare(pairs, gate)
		if err != nil {
			testContext.Fatalf("Compare() error = %v", err)
		}
		if result.Decision != Inconclusive {
			testContext.Fatalf("Compare(%q) decision = %q, want %q", gate.Direction, result.Decision, Inconclusive)
		}
	}
}

func TestCompareReportsEntirelyBeyondBoundaryAsFailure(testContext *testing.T) {
	upperResult, err := Compare(alternatingPairs(1.20, 1.21), Gate{Direction: UpperBound, Boundary: 1.1})
	if err != nil {
		testContext.Fatalf("upper Compare() error = %v", err)
	}
	if upperResult.Decision != Fail {
		testContext.Fatalf("upper decision = %q, want %q", upperResult.Decision, Fail)
	}

	lowerResult, err := Compare(alternatingPairs(0.80, 0.81), Gate{Direction: LowerBound, Boundary: 0.95})
	if err != nil {
		testContext.Fatalf("lower Compare() error = %v", err)
	}
	if lowerResult.Decision != Fail {
		testContext.Fatalf("lower decision = %q, want %q", lowerResult.Decision, Fail)
	}
}

func TestCompareHighBaselineSpreadIsInconclusive(testContext *testing.T) {
	pairs := constantPairs(RequiredPairCount, 100, 100)
	pairs[0].Baseline = 80
	pairs[0].Candidate = 80
	pairs[1].Baseline = 120
	pairs[1].Candidate = 120

	result, err := Compare(pairs, Gate{Direction: UpperBound, Boundary: 1.1})
	if err != nil {
		testContext.Fatalf("Compare() error = %v", err)
	}
	if result.Decision != Inconclusive {
		testContext.Fatalf("Decision = %q, want %q", result.Decision, Inconclusive)
	}
	if result.Reason != HighBaselineSpreadReason {
		testContext.Fatalf("Reason = %q, want %q", result.Reason, HighBaselineSpreadReason)
	}
	assertClose(testContext, "baseline spread", result.BaselineSpread, 0.4, 1e-15)
}

func TestCompareAllowsBaselineSpreadAtBoundary(testContext *testing.T) {
	pairs := constantPairs(RequiredPairCount, 100, 100)
	pairs[0].Baseline = 95
	pairs[0].Candidate = 95
	pairs[1].Baseline = 105
	pairs[1].Candidate = 105

	result, err := Compare(pairs, Gate{Direction: UpperBound, Boundary: 1.1})
	if err != nil {
		testContext.Fatalf("Compare() error = %v", err)
	}
	if result.Decision != Pass {
		testContext.Fatalf("Decision = %q, want %q", result.Decision, Pass)
	}
}

func TestCompareZeroCostPolicy(testContext *testing.T) {
	for _, direction := range []Direction{UpperBound, LowerBound} {
		testContext.Run(string(direction)+" unchanged", func(testContext *testing.T) {
			result, err := Compare(constantPairs(RequiredPairCount, 0, 0), Gate{Direction: direction, Boundary: 1})
			if err != nil {
				testContext.Fatalf("Compare() error = %v", err)
			}
			if result.Decision != Pass || result.Mode != ZeroCostMode {
				testContext.Fatalf("result = %#v, want zero-cost pass", result)
			}
			if result.Estimate.Ratio != nil || result.Lower.Ratio != nil || result.Upper.Ratio != nil {
				testContext.Fatalf("zero-cost result invented a ratio: %#v", result)
			}
		})

		testContext.Run(string(direction)+" positive candidate", func(testContext *testing.T) {
			pairs := constantPairs(RequiredPairCount, 0, 0)
			pairs[7].Candidate = 0.001
			result, err := Compare(pairs, Gate{Direction: direction, Boundary: 100})
			if err != nil {
				testContext.Fatalf("Compare() error = %v", err)
			}
			if result.Decision != Fail || result.Mode != ZeroCostMode {
				testContext.Fatalf("result = %#v, want direction-neutral zero-cost failure", result)
			}
			if result.Reason != ZeroBaselineRegressionReason {
				testContext.Fatalf("Reason = %q, want %q", result.Reason, ZeroBaselineRegressionReason)
			}
		})
	}
}

func TestCompareRejectsMixedZeroAndPositiveBaselines(testContext *testing.T) {
	pairs := constantPairs(RequiredPairCount, 1, 1)
	pairs[0].Baseline = 0
	pairs[0].Candidate = 0

	_, err := Compare(pairs, Gate{Direction: UpperBound, Boundary: 1.1})
	if err == nil || !strings.Contains(err.Error(), "mixed zero and positive baselines") {
		testContext.Fatalf("Compare() error = %v, want mixed-baseline error", err)
	}
}

func TestCompareUsesStableLogRatiosForExtremeInputs(testContext *testing.T) {
	pairs := constantPairs(RequiredPairCount, math.SmallestNonzeroFloat64, math.MaxFloat64)

	result, err := Compare(pairs, Gate{Direction: UpperBound, Boundary: math.MaxFloat64})
	if err != nil {
		testContext.Fatalf("Compare() error = %v", err)
	}
	if math.IsNaN(result.MeanLog) || math.IsInf(result.MeanLog, 0) {
		testContext.Fatalf("MeanLog = %v, want finite stable log difference", result.MeanLog)
	}
	if result.Estimate.Range != AboveFloat64Range || result.Estimate.Ratio != nil {
		testContext.Fatalf("Estimate = %#v, want explicit overflow", result.Estimate)
	}
	if result.Decision != Fail {
		testContext.Fatalf("Decision = %q, want %q", result.Decision, Fail)
	}
}

func TestCompareRepresentsUnderflowWithoutFakeZeroRatio(testContext *testing.T) {
	pairs := constantPairs(RequiredPairCount, math.MaxFloat64, math.SmallestNonzeroFloat64)

	result, err := Compare(pairs, Gate{Direction: LowerBound, Boundary: math.SmallestNonzeroFloat64})
	if err != nil {
		testContext.Fatalf("Compare() error = %v", err)
	}
	if result.Estimate.Range != BelowFloat64Range || result.Estimate.Ratio != nil {
		testContext.Fatalf("Estimate = %#v, want explicit underflow", result.Estimate)
	}
	if result.Decision != Fail {
		testContext.Fatalf("Decision = %q, want %q", result.Decision, Fail)
	}
}

func TestCompareReciprocalSymmetry(testContext *testing.T) {
	pairs := make([]Pair, RequiredPairCount)
	reciprocalPairs := make([]Pair, RequiredPairCount)
	for index := range pairs {
		baseline := float64(10 + index)
		candidate := float64(15 + 2*index)
		pairID := "pair-" + strconv.Itoa(index+1)
		pairs[index] = Pair{ID: pairID, Baseline: baseline, Candidate: candidate}
		reciprocalPairs[index] = Pair{ID: pairID, Baseline: candidate, Candidate: baseline}
	}

	result, err := Compare(pairs, Gate{Direction: UpperBound, Boundary: 2})
	if err != nil {
		testContext.Fatalf("Compare() error = %v", err)
	}
	reciprocal, err := Compare(reciprocalPairs, Gate{Direction: LowerBound, Boundary: 0.5})
	if err != nil {
		testContext.Fatalf("reciprocal Compare() error = %v", err)
	}

	assertClose(testContext, "reciprocal mean log", reciprocal.MeanLog, -result.MeanLog, 1e-15)
	assertClose(testContext, "reciprocal standard deviation", reciprocal.StandardDeviationLog, result.StandardDeviationLog, 1e-15)
	assertRatioClose(testContext, "reciprocal estimate", reciprocal.Estimate, 1/finiteRatio(testContext, result.Estimate), 1e-15)
	assertRatioClose(testContext, "reciprocal lower", reciprocal.Lower, 1/finiteRatio(testContext, result.Upper), 1e-15)
	assertRatioClose(testContext, "reciprocal upper", reciprocal.Upper, 1/finiteRatio(testContext, result.Lower), 1e-15)
}

func constantPairs(count int, baseline float64, candidate float64) []Pair {
	pairs := make([]Pair, count)
	for index := range pairs {
		pairs[index] = Pair{
			ID:        "pair-" + strconv.Itoa(index+1),
			Baseline:  baseline,
			Candidate: candidate,
		}
	}
	return pairs
}

func alternatingPairs(first float64, second float64) []Pair {
	pairs := constantPairs(RequiredPairCount, 1, first)
	for index := 1; index < len(pairs); index += 2 {
		pairs[index].Candidate = second
	}
	return pairs
}

func assertRatioClose(testContext *testing.T, name string, got RatioValue, want float64, tolerance float64) {
	testContext.Helper()
	if got.Range != FiniteRange || got.Ratio == nil {
		testContext.Fatalf("%s = %#v, want finite ratio %v", name, got, want)
	}
	assertClose(testContext, name, *got.Ratio, want, tolerance)
}

func finiteRatio(testContext *testing.T, value RatioValue) float64 {
	testContext.Helper()
	if value.Range != FiniteRange || value.Ratio == nil {
		testContext.Fatalf("ratio value = %#v, want finite", value)
	}
	return *value.Ratio
}

func assertClose(testContext *testing.T, name string, got float64, want float64, tolerance float64) {
	testContext.Helper()
	if math.Abs(got-want) > tolerance {
		testContext.Fatalf("%s = %.17g, want %.17g (tolerance %g)", name, got, want, tolerance)
	}
}
