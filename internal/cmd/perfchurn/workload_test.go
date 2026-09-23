package main

import "testing"

func TestMixIsSectionTwoDeterministicMix(t *testing.T) {
	counts := map[int]int{}
	for sequence := uint64(1000); sequence < 1100; sequence++ {
		counts[workloadMix.size(sequence)]++
	}
	if counts[128] != 90 || counts[512] != 9 || counts[4096] != 1 || len(counts) != 3 {
		t.Fatalf("100 consecutive messages of the mix: %v, want 90 x 128, 9 x 512, 1 x 4096", counts)
	}
	for _, name := range []string{"128", "512", "4096"} {
		workload, err := parseWorkload(name)
		if err != nil || workload.size(7) != map[string]int{"128": 128, "512": 512, "4096": 4096}[name] {
			t.Fatalf("%s: %v %d", name, err, workload.size(7))
		}
	}
	for _, name := range []string{"", "256", "MIX"} {
		if _, err := parseWorkload(name); err == nil {
			t.Fatalf("workload %q accepted", name)
		}
	}
}

func TestFlowsGiveEachAssociationSeveralOrderedSLS(t *testing.T) {
	if flowsPerAssociation < 2 {
		t.Fatalf("%d flows per association", flowsPerAssociation)
	}
	seen := map[int]bool{}
	for association := 0; association < stableAssociations; association++ {
		selections := map[uint8]bool{}
		for flow := 0; flow < flowsPerAssociation; flow++ {
			index := flowIndex(association, flow)
			if seen[index] || index < 0 || index >= flowCount {
				t.Fatalf("flow index %d for %d/%d", index, association, flow)
			}
			seen[index] = true
			keyForward, forward := dataTuple(association, flow, true)
			keyReverse, reverse := dataTuple(association, flow, false)
			if keyForward != keyReverse || forward.SignallingLinkSelection != reverse.SignallingLinkSelection ||
				forward.OriginatingPointCode != reverse.DestinationPointCode {
				t.Fatalf("directions of %d/%d disagree", association, flow)
			}
			selections[forward.SignallingLinkSelection] = true
		}
		if len(selections) != flowsPerAssociation {
			t.Fatalf("association %d flows share an SLS: %v", association, selections)
		}
	}
}
