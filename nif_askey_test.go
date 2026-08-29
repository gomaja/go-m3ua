package m3ua

import (
	"errors"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
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

func TestDialingSGPAssociationControlsNIFAvailability(t *testing.T) {
	association, sent := newTestConnWithContexts(t, StateASPActive, RoleSGP, 1)
	association.nif = &nifAvailability{}
	association.as = newApplicationServers(time.Hour, association.cfg)
	association.noteRoutingContextsActive([]uint32{1})
	association.as.get(1).setASPState(association, StateASPActive, time.Hour)

	if err := association.SetNIFAvailable(false); err != nil {
		t.Fatalf("SetNIFAvailable(false): %v", err)
	}
	if !association.nif.isolatedEntirely() {
		t.Fatal("dialing SGP NIF did not enter isolation")
	}
	if got := association.State(); got != StateASPDown {
		t.Fatalf("dialing SGP Association state = %v, want ASP-DOWN", got)
	}
	downAcks := 0
	for _, message := range *sent {
		if _, ok := message.(*messages.AspDownAck); ok {
			downAcks++
		}
	}
	if downAcks != 1 {
		t.Fatalf("NIF isolation messages = %v, want one ASP Down Ack", typeNames(*sent))
	}

	if err := association.SetNIFAvailable(true); err != nil {
		t.Fatalf("SetNIFAvailable(true): %v", err)
	}
	if association.nif.isolatedEntirely() {
		t.Fatal("dialing SGP NIF remained isolated after recovery")
	}
}

func TestDialingSGPAssociationControlsASAvailability(t *testing.T) {
	association, sent := newTestConnWithContexts(t, StateASPActive, RoleSGP, 1)
	association.nif = &nifAvailability{}
	association.as = newApplicationServers(time.Hour, association.cfg)
	association.noteRoutingContextsActive([]uint32{1})
	key := routingContextASKey(1)
	association.as.get(key).setASPState(association, StateASPActive, time.Hour)

	if err := association.SetASAvailableForAS(key, false); err != nil {
		t.Fatalf("SetASAvailableForAS(false): %v", err)
	}
	if association.nif.servicableASKeys([]ASKey{key}) {
		t.Fatal("dialing SGP Application Server remained serviceable")
	}
	if association.activeForRoutingContext(1) {
		t.Fatal("dialing SGP Association remained active for isolated Routing Context")
	}
	inactiveAcks := 0
	for _, message := range *sent {
		if _, ok := message.(*messages.AspInactiveAck); ok {
			inactiveAcks++
		}
	}
	if inactiveAcks != 1 {
		t.Fatalf("AS isolation messages = %v, want one ASP Inactive Ack", typeNames(*sent))
	}

	if err := association.SetASAvailable(1, true); err != nil {
		t.Fatalf("SetASAvailable(true): %v", err)
	}
	if !association.nif.servicableASKeys([]ASKey{key}) {
		t.Fatal("dialing SGP Application Server remained isolated after recovery")
	}
}

func TestASPAssociationRejectsSGPAvailabilityControls(t *testing.T) {
	association, _ := newTestConn(t, StateASPActive, RoleASP)
	for name, call := range map[string]func() error{
		"NIF": func() error { return association.SetNIFAvailable(false) },
		"AS":  func() error { return association.SetASAvailable(1, false) },
		"ASKey": func() error {
			return association.SetASAvailableForAS(routingContextASKey(1), false)
		},
	} {
		if err := call(); !errors.Is(err, ErrUnsupportedRole) {
			t.Errorf("%s availability error = %v, want ErrUnsupportedRole", name, err)
		}
	}
}

func TestAcceptedSGPAssociationDelegatesAvailabilityToListener(t *testing.T) {
	listener, applicationServer, association, sent := restartFixture(t, 1)
	association.as, association.nif, association.destinations = listener.registry()
	restartActivateASP(applicationServer, association, 1)
	if !listener.track(association) {
		t.Fatal("failed to track accepted SGP Association")
	}
	sent.reset()
	key := ASKey{
		NetworkAppearance:    7,
		NetworkAppearanceSet: true,
		RoutingContext:       1,
		RoutingContextSet:    true,
	}

	if err := association.SetASAvailableForAS(key, false); err != nil {
		t.Fatalf("accepted SGP SetASAvailableForAS(false): %v", err)
	}
	_, nif, _ := listener.registry()
	if nif.servicableASKeys([]ASKey{key}) {
		t.Fatal("accepted SGP Association did not update Listener-wide AS availability")
	}
	if association.activeForRoutingContext(1) {
		t.Fatal("accepted SGP Association retained active traffic scope after AS isolation")
	}
	inactiveAcks := 0
	written := sent.snapshot()
	for _, message := range written {
		if _, ok := message.(*messages.AspInactiveAck); ok {
			inactiveAcks++
		}
	}
	if inactiveAcks != 1 {
		t.Fatalf("accepted SGP AS isolation messages = %v, want one ASP Inactive Ack", typeNames(written))
	}
}

func TestSGPAssociationAvailabilityRequiresEstablishedOpenAssociation(t *testing.T) {
	association := newAssociation(RoleSGP, mcSGPConfig())
	calls := map[string]func() error{
		"NIF": func() error { return association.SetNIFAvailable(false) },
		"AS":  func() error { return association.SetASAvailable(1, false) },
		"ASKey": func() error {
			return association.SetASAvailableForAS(routingContextASKey(1), false)
		},
	}
	for name, call := range calls {
		if err := call(); !errors.Is(err, ErrNotEstablished) {
			t.Errorf("unestablished %s availability error = %v, want ErrNotEstablished", name, err)
		}
	}

	if err := association.Close(); err != nil {
		t.Fatalf("close unestablished SGP Association: %v", err)
	}
	for name, call := range calls {
		if err := call(); !errors.Is(err, ErrAssociationClosed) {
			t.Errorf("closed %s availability error = %v, want ErrAssociationClosed", name, err)
		}
	}
}
