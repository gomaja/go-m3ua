package perfstats

import (
	"math"
	"math/rand/v2"
	"testing"
	"time"
)

// trend is a valid 120-sample, 120-second trend with the given growth bounds
// and floor.
func trend(lower, upper, floor float64) *BacklogTrend {
	return &BacklogTrend{
		SampleCount: 120, Window: 120 * time.Second, Floor: floor, Lag: 4,
		SlopeLower: lower / 120, SlopeUpper: upper / 120, GrowthLower: lower, GrowthUpper: upper,
	}
}

// unstalled is stall evidence from a run whose longest send call is in the
// range the fixture actually measures: hundreds of microseconds, three orders
// of magnitude below the threshold.
func unstalled() *StallObservation {
	return &StallObservation{LongestSend: 262144 * time.Nanosecond}
}

func stalled() *StallObservation {
	return &StallObservation{LongestSend: 1200 * time.Millisecond}
}

func TestBacklogTrendVerdictBoundaries(testContext *testing.T) {
	const floor = 250.0
	above := math.Nextafter(floor, math.Inf(1))
	below := math.Nextafter(floor, math.Inf(-1))
	tests := []struct {
		name  string
		trend BacklogTrend
		want  BacklogVerdict
	}{
		{name: "flat", trend: *trend(0, 0, floor), want: BacklogNotGrowing},
		{name: "straddles zero well below the floor", trend: *trend(-3.4, 3.53, floor), want: BacklogNotGrowing},
		{name: "upper exactly at the floor", trend: *trend(-5, floor, floor), want: BacklogNotGrowing},
		{name: "upper one float below the floor", trend: *trend(-5, below, floor), want: BacklogNotGrowing},
		{name: "upper one float above the floor", trend: *trend(-5, above, floor), want: BacklogIndeterminate},
		{name: "lower exactly at the floor", trend: *trend(floor, above, floor), want: BacklogIndeterminate},
		{name: "collapsed on the floor", trend: *trend(floor, floor, floor), want: BacklogNotGrowing},
		{name: "lower one float above the floor", trend: *trend(above, above, floor), want: BacklogGrowing},
		{name: "wide interval above the floor", trend: *trend(260, 4000, floor), want: BacklogGrowing},
		{name: "zero floor keeps positive growth growing", trend: *trend(0.25, 0.75, 0), want: BacklogGrowing},
		{name: "zero floor with a flat run", trend: *trend(0, 0, 0), want: BacklogNotGrowing},
		{name: "lower NaN", trend: *trend(math.NaN(), 1, floor), want: BacklogIndeterminate},
		{name: "upper NaN", trend: *trend(-1, math.NaN(), floor), want: BacklogIndeterminate},
		{name: "upper infinite", trend: *trend(-1, math.Inf(1), floor), want: BacklogIndeterminate},
		{name: "lower negative infinity", trend: *trend(math.Inf(-1), 0, floor), want: BacklogIndeterminate},
		{name: "reversed", trend: *trend(1, -1, floor), want: BacklogIndeterminate},
		{name: "NaN floor", trend: *trend(0, 0, math.NaN()), want: BacklogIndeterminate},
		{name: "negative floor", trend: *trend(0, 0, -1), want: BacklogIndeterminate},
		{name: "too few samples", trend: BacklogTrend{SampleCount: 7, Window: time.Minute, Floor: floor}, want: BacklogIndeterminate},
		{name: "no window", trend: BacklogTrend{SampleCount: 120, Floor: floor}, want: BacklogIndeterminate},
		{name: "negative lag", trend: BacklogTrend{SampleCount: 120, Window: time.Minute, Floor: floor, Lag: -1}, want: BacklogIndeterminate},
		{name: "reversed slopes", trend: BacklogTrend{SampleCount: 120, Window: time.Minute, Floor: floor, SlopeLower: 1, SlopeUpper: -1}, want: BacklogIndeterminate},
	}
	for _, test := range tests {
		testContext.Run(test.name, func(testContext *testing.T) {
			if got := test.trend.Verdict(); got != test.want {
				testContext.Fatalf("Verdict() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestBacklogFloorIsTenMillisecondsOfOfferedTraffic(testContext *testing.T) {
	for rate, want := range map[uint64]float64{25_000: 250, 5_000: 50, 50_000: 500, 1: 0.01} {
		if got := BacklogFloor(rate); math.Abs(got-want) > 1e-12 {
			testContext.Fatalf("BacklogFloor(%d) = %v, want %v", rate, got, want)
		}
	}
}

func TestNeweyWestLag(testContext *testing.T) {
	for n, want := range map[int]int{0: 0, -1: 0, 8: 2, 10: 2, 100: 4, 120: 4, 600: 5, 604: 5} {
		if got := NeweyWestLag(n); got != want {
			testContext.Fatalf("NeweyWestLag(%d) = %d, want %d", n, got, want)
		}
	}
}

// The expected values come from an independent implementation of the same
// estimator: least-squares slope range over the brackets, Bartlett-kernel
// Newey-West variance with lag floor(4*(n/100)^(2/9)) and an n/(n-2) degrees
// of freedom correction, one-sided 99% normal quantile.
func TestEstimateBacklogTrendKnownValues(testContext *testing.T) {
	lower := []float64{5, 7, 6, 9, 8, 10, 9, 12, 11, 13}
	width := []float64{1, 0, 2, 1, 1, 0, 3, 1, 2, 1}
	samples := make([]BacklogSample, len(lower))
	for index := range lower {
		samples[index] = BacklogSample{At: time.Duration(index+1) * time.Second, Lower: lower[index], Upper: lower[index] + width[index]}
	}
	got, err := EstimateBacklogTrend(samples, 12*time.Second, 1000)
	if err != nil {
		testContext.Fatalf("EstimateBacklogTrend: %v", err)
	}
	want := BacklogTrend{
		SampleCount: 10, Window: 12 * time.Second, Floor: 10, Lag: 2,
		SlopeLower: 0.5927890192028202, SlopeUpper: 1.0920594656456646,
		GrowthLower: 7.113468230433843, GrowthUpper: 13.104713587747975,
	}
	if got.SampleCount != want.SampleCount || got.Window != want.Window || got.Lag != want.Lag {
		testContext.Fatalf("EstimateBacklogTrend() = %+v, want %+v", got, want)
	}
	for name, pair := range map[string][2]float64{
		"floor":        {got.Floor, want.Floor},
		"slope lower":  {got.SlopeLower, want.SlopeLower},
		"slope upper":  {got.SlopeUpper, want.SlopeUpper},
		"growth lower": {got.GrowthLower, want.GrowthLower},
		"growth upper": {got.GrowthUpper, want.GrowthUpper},
	} {
		if math.Abs(pair[0]-pair[1]) > 1e-12*math.Max(1, math.Abs(pair[1])) {
			testContext.Fatalf("%s = %.17g, want %.17g", name, pair[0], pair[1])
		}
	}
	if got.Verdict() != BacklogIndeterminate {
		testContext.Fatalf("Verdict() = %q for growth [%v, %v] around floor %v", got.Verdict(), got.GrowthLower, got.GrowthUpper, got.Floor)
	}
}

func TestEstimateBacklogTrendExactLine(testContext *testing.T) {
	samples := make([]BacklogSample, 10)
	for index := range samples {
		at := float64(index + 1)
		samples[index] = BacklogSample{At: time.Duration(index+1) * time.Second, Lower: 3 + 2*at, Upper: 3 + 2*at}
	}
	got, err := EstimateBacklogTrend(samples, 12*time.Second, 1000)
	if err != nil {
		testContext.Fatalf("EstimateBacklogTrend: %v", err)
	}
	if math.Abs(got.SlopeLower-2) > 1e-12 || math.Abs(got.SlopeUpper-2) > 1e-12 ||
		math.Abs(got.GrowthLower-24) > 1e-10 || math.Abs(got.GrowthUpper-24) > 1e-10 {
		testContext.Fatalf("exact line trend = %+v, want slope 2 and growth 24 on both bounds", got)
	}
	if got.Verdict() != BacklogGrowing {
		testContext.Fatalf("Verdict() = %q, want growing above floor %v", got.Verdict(), got.Floor)
	}
}

// Every backlog path inside the brackets must be covered: with a constant
// midpoint the slope range is exactly the extreme least-squares slope of the
// bracket widths, and the zero residuals add no statistical margin.
func TestEstimateBacklogTrendCoversEveryPathInsideTheBrackets(testContext *testing.T) {
	samples := make([]BacklogSample, 9)
	for index := range samples {
		samples[index] = BacklogSample{At: time.Duration(index+1) * time.Second, Lower: 10, Upper: 14}
	}
	got, err := EstimateBacklogTrend(samples, 10*time.Second, 100)
	if err != nil {
		testContext.Fatalf("EstimateBacklogTrend: %v", err)
	}
	// Times 1..9 have mean 5 and Sxx 60; positive deviations sum to 10, so the
	// steepest path moves 4 messages across weights 10/60: slope 2/3.
	if math.Abs(got.SlopeUpper-2.0/3.0) > 1e-12 || math.Abs(got.SlopeLower+2.0/3.0) > 1e-12 {
		testContext.Fatalf("bracket slope range = [%v, %v], want [-2/3, 2/3]", got.SlopeLower, got.SlopeUpper)
	}
	steepest := make([]BacklogSample, len(samples))
	for index := range samples {
		value := 10.0
		if index > 4 {
			value = 14
		}
		steepest[index] = BacklogSample{At: samples[index].At, Lower: value, Upper: value}
	}
	path, err := EstimateBacklogTrend(steepest, 10*time.Second, 100)
	if err != nil {
		testContext.Fatalf("EstimateBacklogTrend(steepest path): %v", err)
	}
	// Exact samples have no bracket range, so the point slope is the centre
	// of the statistical interval.
	if pointSlope := (path.SlopeLower + path.SlopeUpper) / 2; math.Abs(pointSlope-2.0/3.0) > 1e-12 || pointSlope > got.SlopeUpper+1e-12 {
		testContext.Fatalf("steepest in-bracket path slope %v, bracket range [%v, %v]", pointSlope, got.SlopeLower, got.SlopeUpper)
	}
}

// stationary returns a per-second series around a constant backlog with
// bounded noise and the given bracket width, as a loss-free run at a
// sustainable rate produces.
func stationary(seed uint64, n int, level, noise, width, slope float64) []BacklogSample {
	random := rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))
	samples := make([]BacklogSample, n)
	for index := range samples {
		at := float64(index + 1)
		value := math.Max(0, math.Round(level+slope*at+noise*(2*random.Float64()-1)))
		samples[index] = BacklogSample{At: time.Duration(index+1) * time.Second, Lower: value, Upper: value + width}
	}
	return samples
}

// A healthy run's queue wobbles by a few messages, and the difference between
// its early and late samples is as likely positive as negative. None of those
// runs may be called growing, and they must reach a decision rather than stay
// indeterminate.
func TestStationaryRunsAreNotGrowing(testContext *testing.T) {
	for seed := uint64(1); seed <= 200; seed++ {
		for _, width := range []float64{1, 12} {
			samples := stationary(seed, 119, 40, 6, width, 0)
			got, err := EstimateBacklogTrend(samples, 120*time.Second, 25_000)
			if err != nil {
				testContext.Fatalf("seed %d: %v", seed, err)
			}
			if got.Verdict() != BacklogNotGrowing {
				testContext.Fatalf("seed %d width %v: stationary run = %q with growth [%v, %v], floor %v",
					seed, width, got.Verdict(), got.GrowthLower, got.GrowthUpper, got.Floor)
			}
		}
	}
}

// 5,000 messages/s offered against 4,998 served accumulates two messages per
// second, 240 over the window, against a 50-message floor.
func TestUnderServedControlIsGrowing(testContext *testing.T) {
	for seed := uint64(1); seed <= 200; seed++ {
		samples := stationary(seed, 119, 30, 6, 1, 2)
		got, err := EstimateBacklogTrend(samples, 120*time.Second, 5_000)
		if err != nil {
			testContext.Fatalf("seed %d: %v", seed, err)
		}
		if got.Verdict() != BacklogGrowing {
			testContext.Fatalf("seed %d: under-served control = %q with growth [%v, %v], floor %v",
				seed, got.Verdict(), got.GrowthLower, got.GrowthUpper, got.Floor)
		}
	}
}

func TestGrowthAtTheFloorIsIndeterminate(testContext *testing.T) {
	samples := stationary(7, 119, 30, 6, 1, 50.0/120)
	got, err := EstimateBacklogTrend(samples, 120*time.Second, 5_000)
	if err != nil {
		testContext.Fatalf("EstimateBacklogTrend: %v", err)
	}
	if got.Verdict() != BacklogIndeterminate {
		testContext.Fatalf("growth at the floor = %q with growth [%v, %v], floor %v", got.Verdict(), got.GrowthLower, got.GrowthUpper, got.Floor)
	}
}

func TestEstimateBacklogTrendRejectsInvalidInput(testContext *testing.T) {
	valid := func() []BacklogSample { return stationary(3, 10, 20, 2, 1, 0) }
	tests := []struct {
		name    string
		samples func() []BacklogSample
		window  time.Duration
		rate    uint64
	}{
		{name: "seven samples", samples: func() []BacklogSample { return valid()[:7] }, window: 12 * time.Second, rate: 1000},
		{name: "no samples", samples: func() []BacklogSample { return nil }, window: 12 * time.Second, rate: 1000},
		{name: "zero window", samples: valid, rate: 1000},
		{name: "negative window", samples: valid, window: -time.Second, rate: 1000},
		{name: "zero rate", samples: valid, window: 12 * time.Second},
		{name: "sample at window end", samples: valid, window: 10 * time.Second, rate: 1000},
		{name: "sample at zero", samples: func() []BacklogSample { s := valid(); s[0].At = 0; return s }, window: 12 * time.Second, rate: 1000},
		{name: "repeated time", samples: func() []BacklogSample { s := valid(); s[4].At = s[3].At; return s }, window: 12 * time.Second, rate: 1000},
		{name: "reversed time", samples: func() []BacklogSample { s := valid(); s[4].At, s[3].At = s[3].At, s[4].At; return s }, window: 12 * time.Second, rate: 1000},
		{name: "negative lower", samples: func() []BacklogSample { s := valid(); s[2].Lower = -1; return s }, window: 12 * time.Second, rate: 1000},
		{name: "reversed bracket", samples: func() []BacklogSample { s := valid(); s[2].Upper = s[2].Lower - 1; return s }, window: 12 * time.Second, rate: 1000},
		{name: "NaN lower", samples: func() []BacklogSample { s := valid(); s[2].Lower = math.NaN(); return s }, window: 12 * time.Second, rate: 1000},
		{name: "NaN upper", samples: func() []BacklogSample { s := valid(); s[2].Upper = math.NaN(); return s }, window: 12 * time.Second, rate: 1000},
		{name: "infinite upper", samples: func() []BacklogSample { s := valid(); s[2].Upper = math.Inf(1); return s }, window: 12 * time.Second, rate: 1000},
	}
	for _, test := range tests {
		testContext.Run(test.name, func(testContext *testing.T) {
			if got, err := EstimateBacklogTrend(test.samples(), test.window, test.rate); err == nil {
				testContext.Fatalf("EstimateBacklogTrend() = %+v, want an error", got)
			}
		})
	}
}

func TestDecideRunPassRequiresNotGrowingLossFreeValidFixture(testContext *testing.T) {
	decision := DecideRun(RunEvidence{FixtureValid: true, Trend: trend(-3.4, 3.53, 250), Stall: unstalled()})
	if decision.Decision != Pass || decision.Backlog != BacklogNotGrowing || decision.Reason != "" {
		testContext.Fatalf("DecideRun() = %+v, want clean pass", decision)
	}
}

func TestDecideRunNeverPassesWithStraddlingOrMissingEvidence(testContext *testing.T) {
	tests := []struct {
		name     string
		evidence RunEvidence
		reason   string
	}{
		{name: "straddling the floor", evidence: RunEvidence{FixtureValid: true, Trend: trend(240, 260, 250), Stall: unstalled()}, reason: BacklogUnresolvedReason},
		{name: "missing trend", evidence: RunEvidence{FixtureValid: true, Stall: unstalled()}, reason: BacklogEvidenceMissingReason},
		{name: "NaN trend", evidence: RunEvidence{FixtureValid: true, Trend: trend(math.NaN(), 0, 250), Stall: unstalled()}, reason: BacklogEvidenceMissingReason},
		{name: "reversed trend", evidence: RunEvidence{FixtureValid: true, Trend: trend(1, -1, 250), Stall: unstalled()}, reason: BacklogEvidenceMissingReason},
	}
	for _, test := range tests {
		testContext.Run(test.name, func(testContext *testing.T) {
			decision := DecideRun(test.evidence)
			if decision.Decision != Inconclusive || decision.Reason != test.reason {
				testContext.Fatalf("DecideRun() = %+v, want inconclusive with %q", decision, test.reason)
			}
		})
	}
}

func TestDecideRunFailsOnGrowingBacklog(testContext *testing.T) {
	decision := DecideRun(RunEvidence{FixtureValid: true, Trend: trend(251, 300, 250), Stall: unstalled()})
	if decision.Decision != Fail || decision.Backlog != BacklogGrowing || decision.Reason != BacklogGrowingReason {
		testContext.Fatalf("DecideRun() = %+v, want growing failure", decision)
	}
}

func TestDecideRunFailsOnInvalidFixtureEvenWithCleanTrend(testContext *testing.T) {
	decision := DecideRun(RunEvidence{FixtureValid: false, Trend: trend(-1, 0, 250)})
	if decision.Decision != Fail || decision.Reason != FixtureInvalidReason {
		testContext.Fatalf("DecideRun() = %+v, want fixture-invalid failure", decision)
	}
}

func TestDecideRunFailsOnAnyCountedFailure(testContext *testing.T) {
	counters := []func(*RunCounters){
		func(c *RunCounters) { c.Missing = 1 },
		func(c *RunCounters) { c.Duplicate = 1 },
		func(c *RunCounters) { c.Invalid = 1 },
		func(c *RunCounters) { c.Reordered = 1 },
		func(c *RunCounters) { c.LateAfterStop = 1 },
		func(c *RunCounters) { c.Capped = 1 },
		func(c *RunCounters) { c.SendErrors = 1 },
		func(c *RunCounters) { c.DeadlineExceeded = 1 },
	}
	for index, mutate := range counters {
		var failing RunCounters
		mutate(&failing)
		decision := DecideRun(RunEvidence{FixtureValid: true, Trend: trend(-1, 0, 250), Counters: failing, Stall: unstalled()})
		if decision.Decision != Fail || decision.Reason != DeliveryFailuresReason {
			testContext.Fatalf("counter %d: DecideRun() = %+v, want delivery-failures failure", index, decision)
		}
	}
}

func FuzzBacklogTrendNeverPassesOnInvalidEvidence(fuzzContext *testing.F) {
	fuzzContext.Add(-3.4, 3.53, 250.0)
	fuzzContext.Add(0.0, 0.0, 0.0)
	fuzzContext.Add(math.NaN(), 1.0, 250.0)
	fuzzContext.Add(math.Inf(-1), math.Inf(1), 250.0)
	fuzzContext.Add(250.0, 250.0, 250.0)
	fuzzContext.Add(250.0, math.Nextafter(250, math.Inf(1)), 250.0)
	fuzzContext.Add(1.0, 2.0, -1.0)
	fuzzContext.Fuzz(func(testContext *testing.T, lower, upper, floor float64) {
		evidence := RunEvidence{FixtureValid: true, Trend: trend(lower, upper, floor), Stall: unstalled()}
		decision := DecideRun(evidence)
		switch decision.Backlog {
		case BacklogNotGrowing, BacklogGrowing, BacklogIndeterminate:
		default:
			testContext.Fatalf("unknown backlog verdict %q", decision.Backlog)
		}
		switch decision.Decision {
		case Pass:
			if !evidence.Trend.Valid() || evidence.Trend.GrowthUpper > evidence.Trend.Floor {
				testContext.Fatalf("pass without a valid trend at or below its floor: %+v", evidence.Trend)
			}
		case Fail:
			if !evidence.Trend.Valid() || evidence.Trend.GrowthLower <= evidence.Trend.Floor {
				testContext.Fatalf("loss-free run failed without growth demonstrated above the floor: %+v", decision)
			}
		case Inconclusive:
		default:
			testContext.Fatalf("unknown decision %q", decision.Decision)
		}
	})
}

// The estimator must never panic and must return either an error or a valid
// trend whose verdict is consistent with its bounds, for any series.
func FuzzEstimateBacklogTrend(fuzzContext *testing.F) {
	fuzzContext.Add(uint64(1), uint8(119), 40.0, 6.0, 1.0, 0.0, uint64(25_000))
	fuzzContext.Add(uint64(2), uint8(8), 0.0, 0.0, 0.0, 0.0, uint64(1))
	fuzzContext.Add(uint64(3), uint8(119), 30.0, 6.0, 12.0, 2.0, uint64(5_000))
	fuzzContext.Add(uint64(4), uint8(200), 1e12, 1e9, 1e6, -5e3, uint64(1_000_000))
	fuzzContext.Fuzz(func(testContext *testing.T, seed uint64, count uint8, level, noise, width, slope float64, rate uint64) {
		samples := stationary(seed, int(count), level, noise, width, slope)
		window := time.Duration(int(count)+1) * time.Second
		got, err := EstimateBacklogTrend(samples, window, rate)
		if err != nil {
			return
		}
		if !got.Valid() {
			testContext.Fatalf("estimator returned an invalid trend without an error: %+v", got)
		}
		switch got.Verdict() {
		case BacklogNotGrowing:
			if got.GrowthUpper > got.Floor {
				testContext.Fatalf("not growing above the floor: %+v", got)
			}
		case BacklogGrowing:
			if got.GrowthLower <= got.Floor {
				testContext.Fatalf("growing at or below the floor: %+v", got)
			}
		}
	})
}

// Total is a sum of eight independent uint64 counters. Unsigned addition
// wraps silently in Go, so a saturating sum is the only way a lossy run
// cannot present itself as loss-free.
func TestRunCountersTotalSaturatesInsteadOfWrapping(testContext *testing.T) {
	tests := []struct {
		name     string
		counters RunCounters
	}{
		{name: "missing wraps with a duplicate", counters: RunCounters{Missing: math.MaxUint64, Duplicate: 1}},
		{name: "two counters at the limit", counters: RunCounters{Missing: math.MaxUint64, DeadlineExceeded: math.MaxUint64}},
		{name: "every counter at the limit", counters: RunCounters{
			Missing: math.MaxUint64, Duplicate: math.MaxUint64, Invalid: math.MaxUint64, Reordered: math.MaxUint64,
			LateAfterStop: math.MaxUint64, Capped: math.MaxUint64, SendErrors: math.MaxUint64, DeadlineExceeded: math.MaxUint64,
		}},
		{name: "last counter completes the wrap", counters: RunCounters{Missing: math.MaxUint64, DeadlineExceeded: 1}},
	}
	for _, test := range tests {
		testContext.Run(test.name, func(testContext *testing.T) {
			if total := test.counters.Total(); total != math.MaxUint64 {
				testContext.Fatalf("Total() = %d for %+v, want a saturated %d", total, test.counters, uint64(math.MaxUint64))
			}
		})
	}
}

// A wrapping Total lets DecideRun read a lossy run as loss-free and return
// Pass. The counters below are unreachable in a real fixture run, but the
// decision must not depend on that.
func TestDecideRunNeverPassesALossyRunWhoseCountersWrap(testContext *testing.T) {
	decision := DecideRun(RunEvidence{
		FixtureValid: true,
		Trend:        trend(-1, 0, 250),
		Counters:     RunCounters{Missing: math.MaxUint64, Duplicate: 1},
		Stall:        unstalled(),
	})
	if decision.Decision != Fail || decision.Reason != DeliveryFailuresReason {
		testContext.Fatalf("DecideRun() = %+v, want Fail with %q", decision, DeliveryFailuresReason)
	}
}
