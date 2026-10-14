package perfstats

import "testing"

func TestCapacityBracketIsAnIndependentSnapshot(testContext *testing.T) {
	search := warmSearch(testContext, 37, 100, 24, 38)
	if bracket := search.Bracket(); bracket != nil {
		testContext.Fatalf("unmeasured hint supplied a bracket: %+v", bracket)
	}
	warmProbe(testContext, search, 37, ProbePassing)
	if bracket := search.Bracket(); bracket != nil {
		testContext.Fatalf("passing lower alone supplied a bracket: %+v", bracket)
	}
	warmProbe(testContext, search, 38, ProbeNotDemonstrated)
	bracket := search.Bracket()
	if bracket == nil || *bracket != (CapacityBracket{Lower: 37, Upper: 38}) {
		testContext.Fatalf("bracket = %+v, want 37/38", bracket)
	}
	// Changing a returned snapshot must not alter the search's evidence.
	bracket.Lower, bracket.Upper = 1, 2
	snapshot := search.Bracket()
	if snapshot == nil || *snapshot != (CapacityBracket{Lower: 37, Upper: 38}) {
		testContext.Fatalf("caller changed search bounds: %+v", snapshot)
	}
	recordRepetitions(testContext, search, ProbeFailing)
	if bracket := search.Bracket(); bracket != nil {
		testContext.Fatalf("rejected lower supplied a bracket: %+v", bracket)
	}
	if *snapshot != (CapacityBracket{Lower: 37, Upper: 38}) {
		testContext.Fatalf("resumed search changed an earlier snapshot: %+v", snapshot)
	}
	warmProbe(testContext, search, 18, ProbePassing)
	if bracket := search.Bracket(); bracket == nil || *bracket != (CapacityBracket{Lower: 18, Upper: 37}) {
		testContext.Fatalf("resumed bracket = %+v, want 18/37", bracket)
	}
}
