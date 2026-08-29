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
	config := mcSGPConfig()
	config.NetworkAppearance = params.NewNetworkAppearance(10)
	listener := newSGPListener(NewListenerConfig(config))
	registry, nif, _ := listener.registry()

	key10 := ASKey{NetworkAppearance: 10, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true}
	key20 := ASKey{NetworkAppearance: 20, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true}
	first, _ := newTestConnWithContexts(t, StateASPActive, RoleSGP, 1)
	first.cfg.NetworkAppearance = params.NewNetworkAppearance(10)
	first.as = registry
	first.listener = listener
	first.noteRoutingContextsActive([]uint32{1})
	first.setState(StateASPActive)
	second, _ := newTestConnWithContexts(t, StateASPActive, RoleSGP, 1)
	second.cfg.NetworkAppearance = params.NewNetworkAppearance(20)
	second.as = registry
	second.listener = listener
	second.noteRoutingContextsActive([]uint32{1})
	second.setState(StateASPActive)
	if !listener.track(first) || !listener.track(second) {
		t.Fatal("track refused an association")
	}
	registry.get(key10).setASPState(first, StateASPActive, time.Hour)
	registry.get(key20).setASPState(second, StateASPActive, time.Hour)

	if err := listener.SetASAvailable(1, false); err != nil {
		t.Fatalf("SetASAvailable: %v", err)
	}

	if !nif.servicableASKeys([]ASKey{key10}) {
		t.Fatalf("ambiguous legacy availability isolated %v", key10)
	}
	if !nif.servicableASKeys([]ASKey{key20}) {
		t.Fatalf("ambiguous legacy availability isolated %v", key20)
	}
}

func TestSetASAvailableIgnoresLegacyRoutingContextWhenRegistryAndTrackedDisagree(t *testing.T) {
	config := mcSGPConfig()
	config.NetworkAppearance = params.NewNetworkAppearance(10)
	listener := newSGPListener(NewListenerConfig(config))
	registry, nif, _ := listener.registry()

	registryKey := ASKey{NetworkAppearance: 10, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true}
	trackedKey := ASKey{NetworkAppearance: 20, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true}
	registry.get(registryKey).setTrafficMode(params.TrafficModeLoadshare)

	conn, _ := newTestConnWithContexts(t, StateASPActive, RoleSGP, 1)
	conn.cfg.NetworkAppearance = params.NewNetworkAppearance(20)
	conn.listener = listener
	conn.noteRoutingContextsActive([]uint32{1})
	conn.setState(StateASPActive)
	if !listener.track(conn) {
		t.Fatal("track refused an association")
	}

	if err := listener.SetASAvailable(1, false); err != nil {
		t.Fatalf("SetASAvailable: %v", err)
	}

	if !nif.servicableASKeys([]ASKey{registryKey}) {
		t.Fatalf("disagreeing legacy availability isolated registry key %v", registryKey)
	}
	if !nif.servicableASKeys([]ASKey{trackedKey}) {
		t.Fatalf("disagreeing legacy availability isolated tracked key %v", trackedKey)
	}
}
