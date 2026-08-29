package m3ua

import (
	"errors"
	"sync"
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
	if !listener.promoteAcceptedAssociation(first) || !listener.promoteAcceptedAssociation(second) {
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
	if !listener.promoteAcceptedAssociation(conn) {
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
	association.as = newApplicationServers(time.Hour)
	association.noteRoutingContextsActive([]uint32{1})
	association.as.get(associationConfigASKey(association.cfg, 1)).setASPState(association, StateASPActive, time.Hour)

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

func TestSGPEndpointNIFIsolationQuiescesEveryAssociation(t *testing.T) {
	endpoint, listeners, associations, sent := endpointAvailabilityFixture(t)
	defer func() { _ = endpoint.Close() }()

	if err := listeners[0].SetNIFAvailable(false); err != nil {
		t.Fatalf("SetNIFAvailable(false): %v", err)
	}

	for index, association := range associations {
		if got := association.State(); got != StateASPDown {
			t.Errorf("Association %d state = %v, want ASP-DOWN", index, got)
		}
		written := sent[index].snapshot()
		if got := countMessageType(written, "ASP Down Ack"); got != 1 {
			t.Errorf("Association %d ASP Down Ack count = %d, want 1 (sent %v)", index, got, typeNames(written))
		}
	}
	if active := endpoint.as.get(routingContextASKey(1)).activeASPs(); len(active) != 0 {
		t.Fatalf("isolated Endpoint retained %d active ASPs", len(active))
	}
}

func TestDialingSGPAssociationControlsASAvailability(t *testing.T) {
	association, sent := newTestConnWithContexts(t, StateASPActive, RoleSGP, 1)
	association.nif = &nifAvailability{}
	association.as = newApplicationServers(time.Hour)
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

func TestSGPEndpointASIsolationQuiescesEveryAffectedAssociation(t *testing.T) {
	endpoint, _, associations, sent := endpointAvailabilityFixture(t)
	defer func() { _ = endpoint.Close() }()
	key := routingContextASKey(1)

	if err := associations[2].SetASAvailableForAS(key, false); err != nil {
		t.Fatalf("SetASAvailableForAS(false): %v", err)
	}

	for index, association := range associations {
		if association.activeForRoutingContext(1) {
			t.Errorf("Association %d remained active for isolated Routing Context", index)
		}
		written := sent[index].snapshot()
		if got := countMessageType(written, "ASP Inactive Ack"); got != 1 {
			t.Errorf("Association %d ASP Inactive Ack count = %d, want 1 (sent %v)", index, got, typeNames(written))
		}
	}
	if active := endpoint.as.get(key).activeASPs(); len(active) != 0 {
		t.Fatalf("isolated Application Server retained %d active ASPs", len(active))
	}
}

func endpointAvailabilityFixture(t *testing.T) (*Endpoint, []*Listener, []*Association, []*distributionCapture) {
	t.Helper()
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatalf("NewEndpoint(RoleSGP): %v", err)
	}
	listeners := []*Listener{
		newListener(endpoint, NewListenerConfig(mcSGPConfig())),
		newListener(endpoint, NewListenerConfig(mcSGPConfig())),
	}
	for _, listener := range listeners {
		if !endpoint.trackListener(listener) {
			t.Fatal("failed to attach Listener")
		}
	}

	associations := make([]*Association, 0, 3)
	sent := make([]*distributionCapture, 0, 3)
	key := routingContextASKey(1)
	for _, listener := range listeners {
		association, _ := newTestConnWithContexts(t, StateASPActive, RoleSGP, 1)
		written := new(distributionCapture)
		association.signalWriter = written.write
		association.listener = listener
		if !listener.promoteAcceptedAssociation(association) {
			t.Fatal("failed to attach accepted Association")
		}
		association.noteRoutingContextsActive([]uint32{1})
		endpoint.as.get(key).setASPState(association, StateASPActive, time.Hour)
		associations = append(associations, association)
		sent = append(sent, written)
	}

	initiating, _ := newTestConnWithContexts(t, StateASPActive, RoleSGP, 1)
	written := new(distributionCapture)
	initiating.signalWriter = written.write
	initiating.as, initiating.nif, initiating.destinations, initiating.mtp3Restarts = endpoint.sgpRegistry()
	initiating.as.register(initiating.configuredASKeys())
	if !endpoint.trackAssociation(initiating) {
		t.Fatal("failed to attach SCTP-initiating Association")
	}
	initiating.noteRoutingContextsActive([]uint32{1})
	endpoint.as.get(key).setASPState(initiating, StateASPActive, time.Hour)
	associations = append(associations, initiating)
	sent = append(sent, written)

	return endpoint, listeners, associations, sent
}

func countMessageType(written []messages.M3UA, name string) int {
	count := 0
	for _, message := range written {
		if message.MessageTypeName() == name {
			count++
		}
	}
	return count
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
	if !listener.promoteAcceptedAssociation(association) {
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
		t.Fatal("accepted SGP Association did not update Endpoint-wide AS availability")
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

func TestClosedSGPListenerRejectsAvailabilityControls(t *testing.T) {
	listener := newSGPListener(NewListenerConfig(mcSGPConfig()))
	endpoint := listener.endpoint
	applicationServers := listener.as
	nif := listener.nif
	destinations := listener.destinations
	defer func() { _ = endpoint.Close() }()
	if err := listener.Close(); err != nil {
		t.Fatalf("close SGP Listener: %v", err)
	}
	calls := map[string]func() error{
		"NIF": func() error { return listener.SetNIFAvailable(false) },
		"AS":  func() error { return listener.SetASAvailable(1, false) },
		"ASKey": func() error {
			return listener.SetASAvailableForAS(routingContextASKey(1), false)
		},
	}
	for name, call := range calls {
		if err := call(); !errors.Is(err, ErrAssociationClosed) {
			t.Errorf("closed %s availability error = %v, want ErrAssociationClosed", name, err)
		}
	}
	if listener.as != applicationServers || listener.nif != nif || listener.destinations != destinations {
		t.Fatal("closed availability control replaced Endpoint-owned state")
	}
	if nif.isolatedEntirely() || !nif.servicableASKeys([]ASKey{routingContextASKey(1)}) {
		t.Fatal("closed availability control mutated Endpoint NIF availability")
	}
	if len(applicationServers.keys()) != 0 {
		t.Fatal("closed availability control registered an Application Server")
	}
	if len(destinations.rangesForScope(destinationKey{})) != 0 {
		t.Fatal("closed availability control mutated Endpoint destination state")
	}
}

func TestSGPListenerAvailabilityControlsRaceClose(t *testing.T) {
	for iteration := 0; iteration < 100; iteration++ {
		listener := newSGPListener(NewListenerConfig(mcSGPConfig()))
		start := make(chan struct{})
		results := make(chan error, 4)
		var calls sync.WaitGroup
		for _, call := range []func() error{
			func() error { return listener.SetNIFAvailable(false) },
			func() error { return listener.SetASAvailable(1, false) },
			func() error { return listener.SetASAvailableForAS(routingContextASKey(1), false) },
			listener.Close,
		} {
			calls.Add(1)
			go func(call func() error) {
				defer calls.Done()
				<-start
				results <- call()
			}(call)
		}
		close(start)
		calls.Wait()
		close(results)

		for err := range results {
			if err != nil && !errors.Is(err, ErrAssociationClosed) {
				t.Fatalf("iteration %d availability/close error = %v", iteration, err)
			}
		}
		if err := listener.SetNIFAvailable(true); !errors.Is(err, ErrAssociationClosed) {
			t.Fatalf("iteration %d post-close availability error = %v, want ErrAssociationClosed", iteration, err)
		}
	}
}
