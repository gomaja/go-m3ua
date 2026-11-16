package m3ua

import "testing"

func TestPeerCongestionIndexSurvivesRefreshAndReclaimsRetiredScopes(t *testing.T) {
	first, second := routingContextASKey(7), routingContextASKey(8)
	store, report, bindings := peerRetentionFixture(t, nil, 1, first, second)
	report.Destinations = []PointCodeRange{{PointCode: 100}, {PointCode: 200}}
	check := func(want int) {
		t.Helper()
		if got := len(store.snapshot().PeerCongestion); got != want {
			t.Fatalf("retained peer records = %d, want %d", got, want)
		}
		store.mu.Lock()
		defer store.mu.Unlock()
		indexed := 0
		for _, source := range store.peerSources {
			for _, bucket := range source.records {
				if len(bucket) == 0 {
					t.Fatal("retained an empty AS record bucket")
				}
				indexed += len(bucket)
			}
			if want == 0 && source.records != nil {
				t.Fatal("retained the source record index after its last removal")
			}
		}
		if indexed != want || store.records != want {
			t.Fatalf("indexed records = %d, accounted records = %d, want %d", indexed, store.records, want)
		}
	}
	peerRetentionApply(t, store, report, 1, bindings)
	peerRetentionApply(t, store, report, 1, bindings)
	check(4)
	store.syncPeerCongestion(1, 1, bindings)
	store.retirePeerCongestionAS(1, first)
	check(2)

	bindings[first] = 2
	store.syncPeerCongestion(1, 1, bindings)
	peerRetentionApply(t, store, report, 1, bindings)
	check(4)
	store.retirePeerCongestionContexts(1, map[uint32]struct{}{8: {}})
	check(2)
	store.mu.Lock()
	store.retireRefusedPeerCongestionLocked(1, peerCongestionRefusal{
		admission: peerCongestionAdmission{epoch: 1, bindings: map[ASKey]uint64{first: 2}},
	})
	store.mu.Unlock()
	check(0)

	bindings = map[ASKey]uint64{first: 2}
	peerRetentionApply(t, store, report, 1, bindings)
	check(2)
	store.syncPeerCongestion(1, 2, bindings)
	check(0)
	peerRetentionApply(t, store, report, 2, bindings)
	check(2)
	store.retireAssociation(1)
	check(0)
	if len(store.peerSources) != 0 || len(store.partitions) != 0 || store.bytes != 0 {
		t.Fatal("association retirement retained peer authority, partitions, or bytes")
	}
}
