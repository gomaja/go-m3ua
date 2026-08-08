package m3ua

import (
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
)

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

func TestSetASAvailableIgnoresAmbiguousLegacyRoutingContext(t *testing.T) {
	config := mcServerConfig()
	config.NetworkAppearance = params.NewNetworkAppearance(10)
	listener := newListener(NewListenerConfig(config))
	registry, nif, _ := listener.registry()

	key10 := ASKey{NetworkAppearance: 10, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true}
	key20 := ASKey{NetworkAppearance: 20, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true}
	first, _ := newTestConnWithContexts(t, StateAspActive, modeServer, 1)
	first.cfg.NetworkAppearance = params.NewNetworkAppearance(10)
	first.as = registry
	first.listener = listener
	first.noteRoutingContextsActive([]uint32{1})
	first.setState(StateAspActive)
	second, _ := newTestConnWithContexts(t, StateAspActive, modeServer, 1)
	second.cfg.NetworkAppearance = params.NewNetworkAppearance(20)
	second.as = registry
	second.listener = listener
	second.noteRoutingContextsActive([]uint32{1})
	second.setState(StateAspActive)
	if !listener.track(first) || !listener.track(second) {
		t.Fatal("track refused an association")
	}
	registry.get(key10).setASPState(first, StateAspActive, time.Hour)
	registry.get(key20).setASPState(second, StateAspActive, time.Hour)

	listener.SetASAvailable(1, false)

	if !nif.servicableASKeys([]ASKey{key10}) {
		t.Fatalf("ambiguous legacy availability isolated %v", key10)
	}
	if !nif.servicableASKeys([]ASKey{key20}) {
		t.Fatalf("ambiguous legacy availability isolated %v", key20)
	}
}
