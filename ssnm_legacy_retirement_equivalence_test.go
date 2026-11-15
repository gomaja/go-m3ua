package m3ua

import (
	"math/rand/v2"
	"reflect"
	"testing"
)

func TestGroupedLegacyRetirementMatchesSequentialScopes(t *testing.T) {
	rng := rand.New(rand.NewPCG(81, 19))
	scopes := [][]uint32{nil, {1}, {2}, {3}, {4}, {1, 2}, {2, 3}, {1, 2, 3, 4}}
	for trial := 0; trial < 64; trial++ {
		grouped, sequential := newDestinations(), newDestinations()
		for report := 0; report < 48; report++ {
			scope := scopes[rng.IntN(len(scopes))]
			entry := destinationRange{PointCode: 100 + uint32(rng.IntN(8)), Mask: uint8(rng.IntN(3)), NetworkAppearance: uint32(rng.IntN(2)), NetworkAppearanceSet: rng.IntN(2) == 1,
				State: DestinationNetworkState{Availability: DestinationAvailability(1 + rng.IntN(3)), Congestion: CongestionState{Congested: true, Level: uint8(1 + rng.IntN(3)), LevelSet: true}}}
			congestion := rng.IntN(2) == 1
			for _, d := range []*destinations{grouped, sequential} {
				var err error
				if congestion {
					err = d.setScopedCongestionRangesWithinBudget(scope, []destinationRange{entry})
				} else {
					err = d.setScopedRangesWithinBudget(scope, []destinationRange{entry})
				}
				if err != nil {
					t.Fatal(err)
				}
			}
		}
		grouped.forgetRoutingContexts(map[uint32]struct{}{1: {}, 2: {}, 3: {}})
		for _, rc := range []uint32{1, 2, 3} {
			sequential.forgetRoutingContext(rc)
		}
		for _, rc := range []uint32{1, 2, 3, 4, 5} {
			for _, appearance := range []uint32{0, 1} {
				scope := destinationKey{routingContext: rc, routingContextSet: true, networkAppearance: appearance, networkAppearanceSet: true}
				if a, b := grouped.rangesForScope(scope), sequential.rangesForScope(scope); !reflect.DeepEqual(a, b) {
					t.Fatalf("trial%d RC%d appearance%d: grouped=%+v sequential=%+v", trial, rc, appearance, a, b)
				}
			}
		}
		if len(grouped.state) != len(sequential.state) {
			t.Fatalf("trial%d retained records=%d want%d", trial, len(grouped.state), len(sequential.state))
		}
	}
}
