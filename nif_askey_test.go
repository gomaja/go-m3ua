package m3ua

import "testing"

func TestNIFAvailabilityKeysPartialIsolationByASKey(t *testing.T) {
	nif := &nifAvailability{}
	key10 := ASKey{NetworkAppearance: 10, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true}
	key20 := ASKey{NetworkAppearance: 20, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true}

	nif.setASAvailableForAS(key10, false)

	if nif.servicableASKeys([]ASKey{key10}) {
		t.Fatalf("AS %v remained serviceable after partial NIF isolation", key10)
	}
	if !nif.servicableASKeys([]ASKey{key20}) {
		t.Fatalf("AS %v was isolated with same bare RC but different Network Appearance", key20)
	}
}
