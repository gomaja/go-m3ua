package m3ua

import "testing"

// The oracle expands only a fixed eight-point synthetic network. Production
// retains the received prefixes, including grouped and omitted RC applicability.
func FuzzAuditedScopedProjectionMatchesPointOracle(f *testing.F) {
	f.Add([]byte{0, 0, 3, 2, 1, 0, 0, 0, 1, 1, 0, 1, 1, 1, 0, 0})
	f.Add([]byte{3, 0, 3, 2, 4, 0, 0, 0, 4, 1, 0, 1, 4, 1, 0, 0})
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) > 64 {
			input = input[:64]
		}
		state, err := newSSNMState(nil)
		if err != nil {
			t.Fatal(err)
		}
		state.auditing = &DestinationAuditConfig{MaxPending: 256}
		const association AssociationID = 1
		partition := SSNMPartition{Kind: SSNMStandalonePartition, Association: association}
		state.registerSource(association)
		if err := state.bind(partition, association, false); err != nil {
			t.Fatal(err)
		}
		type expected struct {
			availabilitySet          bool
			availability             DestinationAvailability
			congestionSet, congested bool
		}
		var oracle [2][8]expected
		keys := []ASKey{{RoutingContext: 1, RoutingContextSet: true, NetworkAppearance: 7, NetworkAppearanceSet: true}, {RoutingContext: 2, RoutingContextSet: true, NetworkAppearance: 7, NetworkAppearanceSet: true}}
		for offset := 0; offset+3 < len(input); offset += 4 {
			mask := input[offset+2] % 4
			pc := uint32(input[offset+1]%8) &^ ((uint32(1) << mask) - 1)
			scope := WireScope{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContextSet: true}
			resolved := keys
			switch input[offset+3] % 4 {
			case 0:
				resolved = keys[:1]
				scope.RoutingContexts = []uint32{1}
			case 1:
				resolved = keys[1:]
				scope.RoutingContexts = []uint32{2}
			case 2:
				scope.RoutingContexts = []uint32{1, 2}
			case 3:
				scope.RoutingContextSet = false
			}
			kind := SSNMDestinationUnavailableReport
			switch input[offset] % 5 {
			case 1:
				kind = SSNMDestinationAvailableReport
			case 2:
				kind = SSNMDestinationRestrictedReport
			case 3, 4:
				kind = SSNMSignallingCongestionReport
			}
			report := SSNMReport{Kind: kind, Source: SSNMPeerReport, Association: association, Partition: partition, Scope: scope, Destinations: []PointCodeRange{{PointCode: pc, Mask: mask}}, CongestionLevelSet: true, CongestionLevel: 2}
			if input[offset]%5 == 4 {
				report.CongestionLevel = 0
			}
			if err := state.applyPrepared(report, resolved); err != nil {
				t.Fatal(err)
			}
			for _, key := range resolved {
				index := key.RoutingContext - 1
				for point := pc; point < pc+(uint32(1)<<mask); point++ {
					old := &oracle[index][point]
					if kind == SSNMSignallingCongestionReport {
						old.congestionSet = true
						old.congested = report.CongestionLevel != 0
					} else {
						old.availabilitySet = true
						old.availability = report.availabilityState()
					}
				}
			}
			for as, key := range keys {
				for point := range oracle[as] {
					want := oracle[as][point]
					got := state.destinationKnowledge(partition, uint32(point), key)
					if got.availabilitySet != want.availabilitySet || want.availabilitySet && got.availability != want.availability || got.congestionSet != want.congestionSet || want.congestionSet && got.congested != want.congested || got.receivedUnknown {
						t.Fatalf("scoped projection differs from per-AS point oracle: AS%d PC%d got%+v want%+v input%x", as+1, point, got, want, input[:offset+4])
					}
				}
			}
		}
	})
}
