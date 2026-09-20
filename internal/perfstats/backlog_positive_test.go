package perfstats

import "testing"

func TestPositiveMeanBacklogChangeNeverPasses(testContext *testing.T) {
	for _, change := range []float64{0.25, 0.5, 1} {
		decision := DecideRun(RunEvidence{
			FixtureValid: true,
			Interval:     &BacklogInterval{Lower: change, Upper: change},
			Stall:        unstalled(),
		})
		if decision.Decision != Fail || decision.Backlog != BacklogGrowing {
			testContext.Fatalf("exact positive mean change %v: %+v", change, decision)
		}
	}
}

func TestSmallUncertainBacklogChangeRemainsInconclusive(testContext *testing.T) {
	decision := DecideRun(RunEvidence{
		FixtureValid: true,
		Interval:     &BacklogInterval{Lower: -0.25, Upper: 0.25},
		Stall:        unstalled(),
	})
	if decision.Decision != Inconclusive {
		testContext.Fatalf("uncertain mean change: %+v", decision)
	}
}
