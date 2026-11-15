package m3ua

import "testing"

func TestCompiledSSNMScopeMatcherMatchesCapturedPredicate(t *testing.T) {
	keys := []ASKey{{}, {RoutingContext: 3}, {RoutingContext: 1, RoutingContextSet: true}, *staticASKey(0, 1), *staticASKey(7, 1), *staticASKey(8, 2), {NetworkAppearance: 17, RoutingContext: 1, RoutingContextSet: true}}
	scopes := []WireScope{{}, testWireScope(0, true), testWireScope(7, true), testWireScope(8, true, 1, 2), testWireScope(0, false, 1), {RoutingContextSet: true}, testWireScope(7, true, 2, 1, 2)}
	held := [][]ASKey{nil, {}, keys, keys[:1], keys[1:2], keys[2:3], keys[3:4], keys[4:5], keys[5:6]}
	partitions := []SSNMPartition{canonicalSSNMPartition("match-sg", "match-as"), {Kind: SSNMStandalonePartition, Association: 1}}
	for _, partition := range partitions {
		for mode := 0; mode < 4; mode++ {
			for subset := 0; subset < 1<<len(keys); subset++ {
				refusal := ssnmDestinationRefusal{resolved: mode != 0}
				for i, key := range keys {
					if subset&(1<<i) != 0 {
						refusal.keys = append(refusal.keys, key)
					}
				}
				if mode == 2 {
					refusal.byPartition = map[SSNMPartition][]ASKey{partition: refusal.keys}
				}
				if mode == 3 {
					refusal.byPartition = map[SSNMPartition][]ASKey{}
				}
				matcher := refusal.matchers([]SSNMPartition{partition})[partition]
				for _, scope := range scopes {
					for _, retained := range held {
						if got, want := matcher.matches(scope, retained), refusal.matches(partition, scope, retained); got != want {
							t.Fatalf("partition%+v mode%d affected%v scope%+v retained%v: indexed%v scan%v", partition, mode, refusal.keys, scope, retained, got, want)
						}
					}
				}
			}
		}
	}
}
