package perfstats

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
)

func TestCompareFivePairsHandComputedInterval(testContext *testing.T) {
	// Ratios exp(-0.2), exp(-0.1), 1, exp(0.1), exp(0.2) have mean log 0.
	// Sum of squared log deviations = 0.04 + 0.01 + 0 + 0.01 + 0.04 = 0.10.
	// Sample variance = 0.10 / (5-1) = 0.025; s = sqrt(0.025).
	// SE = sqrt(0.025 / 5) = sqrt(0.005) = 0.07071067811865475.
	// Margin = 2.7764451051977943 * SE = 0.19632431614775577.
	// Geometric mean = exp(0) = 1; interval = exp(+-margin).
	pairs := symmetricLogPairs(5)
	result, err := CompareWithPairCount(pairs, Gate{Direction: LowerBound, Boundary: 0.8}, 5)
	if err != nil {
		testContext.Fatal(err)
	}
	assertClose(testContext, "mean log", result.MeanLog, 0, 1e-16)
	assertClose(testContext, "sample deviation", result.StandardDeviationLog, 0.15811388300841897, 1e-15)
	assertClose(testContext, "lower log", result.Lower.Log, -0.19632431614775577, 1e-15)
	assertClose(testContext, "upper log", result.Upper.Log, 0.19632431614775577, 1e-15)
	assertRatioClose(testContext, "estimate", result.Estimate, 1, 1e-15)
	assertRatioClose(testContext, "lower", result.Lower, 0.8217456860621741, 1e-15)
	assertRatioClose(testContext, "upper", result.Upper, 1.2169215086385485, 1e-15)
	if result.Decision != Pass || result.Mode != RatioMode || result.PairCount != 5 || result.TCritical975 != 2.7764451051977943 {
		testContext.Fatalf("unexpected comparison metadata: %+v", result)
	}
	if len(result.LogRatios) != 5 {
		testContext.Fatalf("log ratio count = %d, want 5", len(result.LogRatios))
	}
	for index, ratio := range result.LogRatios {
		if ratio.PairID != pairs[index].ID {
			testContext.Fatalf("pair %d ID = %q, want %q", index, ratio.PairID, pairs[index].ID)
		}
		assertClose(testContext, "paired log ratio", ratio.Value, float64(index-2)/10, 1e-15)
	}
	// Integrating the NIST df=4 density gives F(t)=1/2+(3u-u^3)/4,
	// u=t/sqrt(t^2+4). This independently checks the selected quantile.
	u := result.TCritical975 / math.Sqrt(result.TCritical975*result.TCritical975+4)
	assertClose(testContext, "df4 CDF at selected quantile", 0.5+(3*u-u*u*u)/4, 0.975, 1e-16)
}

func TestCompareExplicitTwentyMatchesDefault(testContext *testing.T) {
	pairs := symmetricLogPairs(20)
	gate := Gate{Direction: LowerBound, Boundary: 0.9}
	implicit, err := Compare(pairs, gate)
	if err != nil {
		testContext.Fatal(err)
	}
	explicit, err := CompareWithPairCount(pairs, gate, 20)
	if err != nil {
		testContext.Fatal(err)
	}
	if !reflect.DeepEqual(explicit, implicit) || explicit.TCritical975 != 2.093024054408263 {
		testContext.Fatalf("explicit twenty changed the default: explicit=%+v, implicit=%+v", explicit, implicit)
	}
}

func TestCompareFiveIdenticalRatios(testContext *testing.T) {
	// Each ratio is 10/8 = 1.25: mean log = log(1.25), sample deviation 0,
	// so the geometric mean and both interval bounds must equal 1.25.
	result, err := CompareWithPairCount(constantPairs(5, 8, 10), Gate{Direction: LowerBound, Boundary: 1.2}, 5)
	if err != nil {
		testContext.Fatal(err)
	}
	assertClose(testContext, "mean log", result.MeanLog, 0.22314355131420976, 1e-15)
	assertClose(testContext, "sample deviation", result.StandardDeviationLog, 0, 0)
	assertRatioClose(testContext, "estimate", result.Estimate, 1.25, 1e-15)
	assertRatioClose(testContext, "lower", result.Lower, 1.25, 1e-15)
	assertRatioClose(testContext, "upper", result.Upper, 1.25, 1e-15)
	if result.Decision != Pass {
		testContext.Fatalf("decision = %s, want pass", result.Decision)
	}
}

func TestCompareRejectsUnsupportedPairCounts(testContext *testing.T) {
	for _, count := range []int{-1, 0, 4, 6, 19, 21} {
		testContext.Run(fmt.Sprint(count), func(testContext *testing.T) {
			_, err := CompareWithPairCount(constantPairs(20, 1, 1), Gate{Direction: LowerBound, Boundary: 0.95}, count)
			if err == nil || !strings.Contains(err.Error(), "pair_count must be 5 or 20") {
				testContext.Fatalf("error = %v, want supported-count error", err)
			}
		})
	}
}

func TestCompareFiveRequiresExactlyFivePairs(testContext *testing.T) {
	for _, count := range []int{0, 4, 6, 20} {
		testContext.Run(fmt.Sprint(count), func(testContext *testing.T) {
			_, err := CompareWithPairCount(constantPairs(count, 1, 1), Gate{Direction: LowerBound, Boundary: 0.95}, 5)
			if err == nil || !strings.Contains(err.Error(), "exactly 5 matched pairs") {
				testContext.Fatalf("error = %v, want exact-count error", err)
			}
		})
	}
}

func TestCompareFiveValidatesEveryPair(testContext *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func([]Pair)
		want   string
	}{
		{name: "missing ID", mutate: func(pairs []Pair) { pairs[4].ID = "" }, want: "empty ID"},
		{name: "repeated ID", mutate: func(pairs []Pair) { pairs[4].ID = pairs[0].ID }, want: "duplicate pair ID"},
		{name: "negative baseline", mutate: func(pairs []Pair) { pairs[4].Baseline = -1 }, want: "baseline must be nonnegative and finite"},
		{name: "NaN baseline", mutate: func(pairs []Pair) { pairs[4].Baseline = math.NaN() }, want: "baseline must be nonnegative and finite"},
		{name: "infinite baseline", mutate: func(pairs []Pair) { pairs[4].Baseline = math.Inf(1) }, want: "baseline must be nonnegative and finite"},
		{name: "negative candidate", mutate: func(pairs []Pair) { pairs[4].Candidate = -1 }, want: "candidate must be nonnegative and finite"},
		{name: "NaN candidate", mutate: func(pairs []Pair) { pairs[4].Candidate = math.NaN() }, want: "candidate must be nonnegative and finite"},
		{name: "infinite candidate", mutate: func(pairs []Pair) { pairs[4].Candidate = math.Inf(1) }, want: "candidate must be nonnegative and finite"},
		{name: "zero candidate", mutate: func(pairs []Pair) { pairs[4].Candidate = 0 }, want: "candidate must be positive"},
	} {
		testContext.Run(test.name, func(testContext *testing.T) {
			pairs := constantPairs(5, 1, 1)
			test.mutate(pairs)
			_, err := CompareWithPairCount(pairs, Gate{Direction: LowerBound, Boundary: 0.95}, 5)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				testContext.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestCompareFiveDecidesIntervalBounds(testContext *testing.T) {
	for _, test := range []struct {
		direction Direction
		boundary  float64
		decision  Decision
	}{
		{direction: UpperBound, boundary: 1.3, decision: Pass},
		{direction: UpperBound, boundary: 0.8, decision: Fail},
		{direction: UpperBound, boundary: 1.1, decision: Inconclusive},
		{direction: LowerBound, boundary: 0.8, decision: Pass},
		{direction: LowerBound, boundary: 1.3, decision: Fail},
		{direction: LowerBound, boundary: 0.9, decision: Inconclusive},
	} {
		testContext.Run(fmt.Sprintf("%s/%g", test.direction, test.boundary), func(testContext *testing.T) {
			result, err := CompareWithPairCount(symmetricLogPairs(5), Gate{Direction: test.direction, Boundary: test.boundary}, 5)
			if err != nil {
				testContext.Fatal(err)
			}
			if result.Decision != test.decision {
				testContext.Fatalf("decision = %s, want %s", result.Decision, test.decision)
			}
		})
	}
}

func TestCompareFiveZeroBaselinePolicy(testContext *testing.T) {
	for _, direction := range []Direction{UpperBound, LowerBound} {
		for _, test := range []struct {
			name     string
			pairs    []Pair
			decision Decision
			reason   string
		}{
			{name: "all zero", pairs: constantPairs(5, 0, 0), decision: Pass},
			{name: "all positive candidates", pairs: constantPairs(5, 0, 0.001), decision: Fail, reason: ZeroBaselineRegressionReason},
			{name: "last candidate positive", pairs: []Pair{
				{ID: "1"}, {ID: "2"}, {ID: "3"}, {ID: "4"}, {ID: "5", Candidate: 0.001},
			}, decision: Fail, reason: ZeroBaselineRegressionReason},
			{name: "mixed with regression", pairs: []Pair{
				{ID: "1", Baseline: 1, Candidate: 1}, {ID: "2"}, {ID: "3"}, {ID: "4"}, {ID: "5", Candidate: 0.001},
			}, decision: Fail, reason: ZeroBaselineRegressionReason},
		} {
			testContext.Run(string(direction)+"/"+test.name, func(testContext *testing.T) {
				result, err := CompareWithPairCount(test.pairs, Gate{Direction: direction, Boundary: 100}, 5)
				if err != nil {
					testContext.Fatal(err)
				}
				if result.Decision != test.decision || result.Reason != test.reason || result.Mode != ZeroCostMode || result.PairCount != 5 || result.TCritical975 != 2.7764451051977943 {
					testContext.Fatalf("unexpected zero-cost result: %+v", result)
				}
				for _, ratio := range []RatioValue{result.Estimate, result.Lower, result.Upper} {
					if ratio.Range != UndefinedRange || ratio.Ratio != nil {
						testContext.Fatalf("zero-cost result invented a ratio: %+v", ratio)
					}
				}
				if len(result.LogRatios) != 0 {
					testContext.Fatalf("zero-cost result invented log ratios: %+v", result.LogRatios)
				}
			})
		}
		for _, zeros := range []int{1, 4} {
			testContext.Run(fmt.Sprintf("%s/mixed %d zero", direction, zeros), func(testContext *testing.T) {
				pairs := constantPairs(5, 1, 1)
				for index := 0; index < zeros; index++ {
					pairs[index].Baseline, pairs[index].Candidate = 0, 0
				}
				_, err := CompareWithPairCount(pairs, Gate{Direction: direction, Boundary: 1}, 5)
				if err == nil || !strings.Contains(err.Error(), "cannot form 5 log ratios") {
					testContext.Fatalf("error = %v, want mixed-baseline error with selected count", err)
				}
			})
		}
	}
}

func TestCompareFiveBaselineMedian(testContext *testing.T) {
	for _, test := range []struct {
		name      string
		baselines []float64
		spread    float64
		decision  Decision
	}{
		// Sorted: [91, 95, 100, 100, 101]; median 100, spread (101-91)/100.
		{name: "odd median permits exact boundary", baselines: []float64{100, 95, 101, 91, 100}, spread: 0.10, decision: Pass},
		{name: "above boundary", baselines: []float64{100, 95, 102, 91, 100}, spread: 0.11, decision: Inconclusive},
		{name: "large values", baselines: []float64{math.MaxFloat64, math.MaxFloat64, math.MaxFloat64, math.MaxFloat64, math.MaxFloat64}, decision: Pass},
	} {
		testContext.Run(test.name, func(testContext *testing.T) {
			pairs := constantPairs(5, 1, 1)
			for index, baseline := range test.baselines {
				pairs[index].Baseline, pairs[index].Candidate = baseline, baseline
			}
			result, err := CompareWithPairCount(pairs, Gate{Direction: LowerBound, Boundary: 0.95}, 5)
			if err != nil {
				testContext.Fatal(err)
			}
			assertClose(testContext, "baseline spread", result.BaselineSpread, test.spread, 1e-15)
			if result.Decision != test.decision || (test.decision == Inconclusive && result.Reason != HighBaselineSpreadReason) {
				testContext.Fatalf("unexpected spread decision: %+v", result)
			}
		})
	}
}

func TestCompareFiveVersusTwentyIntervalWidth(testContext *testing.T) {
	// Twenty observations repeat the same five log ratios four times. The first
	// five are the subset, so both means are 0 and both sums/n are 0.02.
	// n=20: sum squares=0.4; sample variance=0.4/19; SE=sqrt(0.4/19/20).
	// Margin20=2.093024054408263*SE=0.06790666731339578.
	// Margin5=0.19632431614775577 (arithmetic in the five-pair reference test).
	// Log width factor = Margin5/Margin20 = 2.891090432132389.
	// Ratio width factor = (exp(Margin5)-exp(-Margin5)) /
	//                      (exp(Margin20)-exp(-Margin20)) = 2.9074631999781765.
	pairs := symmetricLogPairs(20)
	for _, direction := range []Direction{UpperBound, LowerBound} {
		boundary := 1.1
		if direction == LowerBound {
			boundary = 0.9
		}
		gate := Gate{Direction: direction, Boundary: boundary}
		five, err := CompareWithPairCount(pairs[:5], gate, 5)
		if err != nil {
			testContext.Fatal(err)
		}
		twenty, err := CompareWithPairCount(pairs, gate, 20)
		if err != nil {
			testContext.Fatal(err)
		}
		assertRatioClose(testContext, "twenty lower", twenty.Lower, 0.9343476746864942, 1e-15)
		assertRatioClose(testContext, "twenty upper", twenty.Upper, 1.070265413070712, 1e-15)
		logFactor := (five.Upper.Log - five.Lower.Log) / (twenty.Upper.Log - twenty.Lower.Log)
		ratioFactor := (finiteRatio(testContext, five.Upper) - finiteRatio(testContext, five.Lower)) /
			(finiteRatio(testContext, twenty.Upper) - finiteRatio(testContext, twenty.Lower))
		assertClose(testContext, "log width factor", logFactor, 2.891090432132389, 1e-13)
		assertClose(testContext, "ratio width factor", ratioFactor, 2.9074631999781765, 1e-13)
		if twenty.Decision != Pass || five.Decision != Inconclusive || five.Reason != IntervalStraddlesBoundaryReason {
			testContext.Fatalf("%s: twenty=%s, five=%s/%s; want pass and inconclusive", direction, twenty.Decision, five.Decision, five.Reason)
		}
		testContext.Logf("%s: five=[%.16g, %.16g], twenty=[%.16g, %.16g], ratio width factor=%.16g, log width factor=%.16g",
			direction, finiteRatio(testContext, five.Lower), finiteRatio(testContext, five.Upper),
			finiteRatio(testContext, twenty.Lower), finiteRatio(testContext, twenty.Upper), ratioFactor, logFactor)
	}
}

func symmetricLogPairs(count int) []Pair {
	pairs := constantPairs(count, 1, 1)
	for index := range pairs {
		pairs[index].Candidate = math.Exp(float64(index%5-2) / 10)
	}
	return pairs
}
