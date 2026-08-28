package m3ua

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestListenerDestinationUpdatesNotifyOnlyConcernedActiveASPs(t *testing.T) {
	tests := []struct {
		name  string
		state DestinationState
		kind  any
	}{
		{"unavailable", DestinationUnavailable, (*messages.DestinationUnavailable)(nil)},
		{"available", DestinationAvailable, (*messages.DestinationAvailable)(nil)},
		{"restricted", DestinationRestricted, (*messages.DestinationRestricted)(nil)},
		{"congested", DestinationCongested, (*messages.SignallingCongestion)(nil)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			listener, _, first, firstSent := distributionFixtureForContexts(
				t, params.TrafficModeLoadshare, []uint32{1, 2}, nil,
			)
			second, secondSent := addDistributionASP(t, listener, StateASPInactive, 1, 2)
			first.cfg.NetworkAppearance = params.NewNetworkAppearance(7)
			second.cfg.NetworkAppearance = params.NewNetworkAppearance(7)
			firstApplicationServer := proactiveSSNMApplicationServer(listener, 7, 1)
			secondApplicationServer := proactiveSSNMApplicationServer(listener, 7, 2)
			first.noteRoutingContextsActive([]uint32{1})
			first.setState(StateASPActive)
			second.noteRoutingContextsActive([]uint32{2})
			second.setState(StateASPActive)
			firstApplicationServer.setASPState(first, StateASPActive, time.Hour)
			secondApplicationServer.setASPState(second, StateASPActive, time.Hour)
			firstSent.reset()
			secondSent.reset()

			if err := listener.ReportDestinationRangeForNetworkAndRoutingContext(
				7, 1, 0x123456, 8, test.state,
			); err != nil {
				t.Fatalf("set destination state: %v", err)
			}
			firstMessages := ssnmMessages(firstSent.snapshot())
			if len(firstMessages) != 1 {
				t.Fatalf("concerned ASP received %d SSNM messages, want 1", len(firstMessages))
			}
			if !sameSSNMKind(firstMessages[0], test.kind) {
				t.Fatalf("destination state %v emitted %T, want %T", test.state, firstMessages[0], test.kind)
			}
			networkAppearance, routingContext, affectedPointCode := ssnmScope(t, firstMessages[0])
			if networkAppearance == nil || networkAppearance.NetworkAppearance() != 7 {
				t.Fatalf("SSNM Network Appearance = %v, want 7", networkAppearance)
			}
			if got := routingContext.RoutingContexts(); !equalTrafficModeContexts(got, []uint32{1}) {
				t.Fatalf("SSNM Routing Contexts = %v, want [1]", got)
			}
			if got := affectedPointCode.AffectedPointCodes(); len(got) != 1 || got[0] != 0x123456 {
				t.Fatalf("SSNM Affected Point Codes = %v, want [0x123456]", got)
			}
			if got := affectedPointCode.AffectedPointCodeMasks(); len(got) != 1 || got[0] != 8 {
				t.Fatalf("SSNM Affected Point Code masks = %v, want [8]", got)
			}
			if got := len(ssnmMessages(secondSent.snapshot())); got != 0 {
				t.Fatalf("unconcerned ASP received %d SSNM messages, want 0", got)
			}
		})
	}
}

func TestAllContextDestinationUpdateDeduplicatesAnASPAndNamesItsActiveScopes(t *testing.T) {
	listener, _, asp, sent := distributionFixtureForContexts(
		t, params.TrafficModeLoadshare, []uint32{1, 2}, nil,
	)
	asp.cfg.NetworkAppearance = params.NewNetworkAppearance(7)
	firstApplicationServer := proactiveSSNMApplicationServer(listener, 7, 1)
	secondApplicationServer := proactiveSSNMApplicationServer(listener, 7, 2)
	asp.noteRoutingContextsActive([]uint32{1, 2})
	asp.setState(StateASPActive)
	firstApplicationServer.setASPState(asp, StateASPActive, time.Hour)
	secondApplicationServer.setASPState(asp, StateASPActive, time.Hour)
	sent.reset()

	if err := listener.ReportDestinationRangeForNetwork(7, 0x123456, 4, DestinationUnavailable); err != nil {
		t.Fatalf("set destination state: %v", err)
	}
	got := ssnmMessages(sent.snapshot())
	if len(got) != 1 {
		t.Fatalf("multi-AS ASP received %d copies of one DUNA, want 1", len(got))
	}
	_, routingContext, _ := ssnmScope(t, got[0])
	if contexts := routingContext.RoutingContexts(); !equalTrafficModeContexts(contexts, []uint32{1, 2}) {
		t.Fatalf("all-context DUNA Routing Contexts = %v, want [1 2]", contexts)
	}
}

func TestAllContextDestinationUpdateScopesTargetsByNetworkAppearance(t *testing.T) {
	listener, _, first, firstSent := distributionFixtureForContexts(
		t, params.TrafficModeLoadshare, []uint32{1}, nil,
	)
	first.cfg.NetworkAppearance = params.NewNetworkAppearance(10)
	first.noteRoutingContextsActive([]uint32{1})
	first.setState(StateASPActive)
	key10 := ASKey{NetworkAppearance: 10, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true}
	applicationServer10 := listener.as.get(key10)
	applicationServer10.setTrafficMode(params.TrafficModeLoadshare)
	applicationServer10.setASPState(first, StateASPActive, time.Hour)

	second, secondSent := addDistributionASP(t, listener, StateASPInactive, 1)
	second.cfg.NetworkAppearance = params.NewNetworkAppearance(20)
	second.noteRoutingContextsActive([]uint32{1})
	second.setState(StateASPActive)
	key20 := ASKey{NetworkAppearance: 20, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true}
	applicationServer20 := listener.as.get(key20)
	applicationServer20.setTrafficMode(params.TrafficModeLoadshare)
	applicationServer20.setASPState(second, StateASPActive, time.Hour)
	firstSent.reset()
	secondSent.reset()

	if err := listener.ReportDestinationRangeForNetwork(10, 0x123456, 4, DestinationUnavailable); err != nil {
		t.Fatalf("set destination state: %v", err)
	}
	if got := len(ssnmMessages(firstSent.snapshot())); got != 1 {
		t.Fatalf("matching Network Appearance ASP received %d SSNM messages, want 1", got)
	}
	if got := len(ssnmMessages(secondSent.snapshot())); got != 0 {
		t.Fatalf("foreign Network Appearance ASP received %d SSNM messages, want 0", got)
	}
}

func TestDestinationUpdateScopesSameASPRoutingContextByNetworkAppearance(t *testing.T) {
	listener, _, asp, sent := distributionFixtureForContexts(
		t, params.TrafficModeLoadshare, []uint32{1}, nil,
	)
	asp.cfg.NetworkAppearance = params.NewNetworkAppearance(10)
	asp.noteRoutingContextsActive([]uint32{1})
	asp.setState(StateASPActive)
	for _, key := range []ASKey{
		{NetworkAppearance: 10, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true},
		{NetworkAppearance: 20, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true},
	} {
		applicationServer := listener.as.get(key)
		applicationServer.setTrafficMode(params.TrafficModeLoadshare)
		applicationServer.setASPState(asp, StateASPActive, time.Hour)
	}
	sent.reset()

	if err := listener.ReportDestinationRangeForNetworkAndRoutingContext(10, 1, 0x123456, 4, DestinationUnavailable); err != nil {
		t.Fatalf("set destination state: %v", err)
	}
	got := ssnmMessages(sent.snapshot())
	if len(got) != 1 {
		t.Fatalf("ASP received %d SSNM messages, want 1", len(got))
	}
	networkAppearance, routingContext, _ := ssnmScope(t, got[0])
	if networkAppearance == nil || networkAppearance.NetworkAppearance() != 10 {
		t.Fatalf("DUNA Network Appearance = %v, want 10", networkAppearance)
	}
	if contexts := routingContext.RoutingContexts(); !equalTrafficModeContexts(contexts, []uint32{1}) {
		t.Fatalf("DUNA Routing Contexts = %v, want one [1]", contexts)
	}
}

func TestDestinationUpdateContinuesAfterOneASPWriteFails(t *testing.T) {
	listener, _, first, _ := distributionFixture(
		t, params.TrafficModeLoadshare,
	)
	second, secondSent := addDistributionASP(t, listener, StateASPInactive, 1)
	first.cfg.NetworkAppearance = params.NewNetworkAppearance(7)
	second.cfg.NetworkAppearance = params.NewNetworkAppearance(7)
	applicationServer := proactiveSSNMApplicationServer(listener, 7, 1)
	first.noteRoutingContextsActive([]uint32{1})
	first.setState(StateASPActive)
	second.noteRoutingContextsActive([]uint32{1})
	second.setState(StateASPActive)
	applicationServer.setASPState(first, StateASPActive, time.Hour)
	applicationServer.setASPState(second, StateASPActive, time.Hour)

	writeFailure := errors.New("injected SSNM write failure")
	first.signalWriter = func(messages.M3UA) (int, error) { return 0, writeFailure }
	if err := listener.ReportDestinationRangeForNetworkAndRoutingContext(
		7, 1, 0x123456, 0, DestinationUnavailable,
	); !errors.Is(err, writeFailure) {
		t.Fatalf("set destination state error = %v, want injected write failure", err)
	}
	if got := len(ssnmMessages(secondSent.snapshot())); got != 1 {
		t.Fatalf("healthy ASP received %d SSNM messages after peer failure, want 1", got)
	}
	if state, known := listener.DestinationStateForNetworkAndRoutingContext(
		7, 1, 0x123456,
	); !known || state != DestinationUnavailable {
		t.Fatalf("recorded destination = (%v, %v), want (Unavailable, true)", state, known)
	}
}

func TestDestinationUpdateRejectsUnknownStateAtomically(t *testing.T) {
	listener, applicationServer, asp, sent := distributionFixture(
		t, params.TrafficModeLoadshare,
	)
	asp.noteRoutingContextsActive([]uint32{1})
	asp.setState(StateASPActive)
	applicationServer.setASPState(asp, StateASPActive, time.Hour)
	sent.reset()

	err := listener.ReportDestinationRangeForNetworkAndRoutingContext(
		7, 1, 0x123456, 0, DestinationState(255),
	)
	if !errors.Is(err, ErrInvalidParameterValue) {
		t.Fatalf("unknown destination state error = %v, want ErrInvalidParameterValue", err)
	}
	if _, known := listener.DestinationStateForNetworkAndRoutingContext(7, 1, 0x123456); known {
		t.Fatal("unknown destination state was recorded")
	}
	if got := len(ssnmMessages(sent.snapshot())); got != 0 {
		t.Fatalf("unknown destination state emitted %d SSNM messages, want 0", got)
	}
}

func TestAcceptedAssociationDestinationUpdateUsesListenerWideBroadcast(t *testing.T) {
	listener, _, first, firstSent := distributionFixture(
		t, params.TrafficModeLoadshare,
	)
	second, secondSent := addDistributionASP(t, listener, StateASPInactive, 1)
	first.cfg.NetworkAppearance = params.NewNetworkAppearance(7)
	second.cfg.NetworkAppearance = params.NewNetworkAppearance(7)
	applicationServer := proactiveSSNMApplicationServer(listener, 7, 1)
	first.noteRoutingContextsActive([]uint32{1})
	first.setState(StateASPActive)
	second.noteRoutingContextsActive([]uint32{1})
	second.setState(StateASPActive)
	applicationServer.setASPState(first, StateASPActive, time.Hour)
	applicationServer.setASPState(second, StateASPActive, time.Hour)
	listener.destinations = newDestinations()
	first.destinations = listener.destinations
	second.destinations = listener.destinations
	first.listener = listener
	second.listener = listener
	firstSent.reset()
	secondSent.reset()

	if err := first.ReportDestinationStateForNetworkAndRoutingContext(
		7, 1, 0x123456, DestinationUnavailable,
	); err != nil {
		t.Fatalf("set destination state through accepted Association: %v", err)
	}
	if got := len(ssnmMessages(firstSent.snapshot())); got != 1 {
		t.Fatalf("calling ASP received %d SSNM messages, want 1", got)
	}
	if got := len(ssnmMessages(secondSent.snapshot())); got != 1 {
		t.Fatalf("peer ASP received %d SSNM messages, want 1", got)
	}
}

func TestDestinationSetterMethodCompatibility(t *testing.T) {
	assertDestinationSetter := func(func(uint32, DestinationState)) {}
	assertDestinationSetter(new(Listener).SetDestinationState)
	assertDestinationSetter(new(Association).SetDestinationState)
}

func TestDestinationSetterDoesNotBlockHealthyPeersBehindOneASP(t *testing.T) {
	listener, _, blocked, _ := distributionFixture(
		t, params.TrafficModeLoadshare,
	)
	healthy, healthySent := addDistributionASP(t, listener, StateASPInactive, 1)
	blocked.cfg.NetworkAppearance = params.NewNetworkAppearance(7)
	healthy.cfg.NetworkAppearance = params.NewNetworkAppearance(7)
	applicationServer := proactiveSSNMApplicationServer(listener, 7, 1)
	blocked.noteRoutingContextsActive([]uint32{1})
	blocked.setState(StateASPActive)
	healthy.noteRoutingContextsActive([]uint32{1})
	healthy.setState(StateASPActive)
	applicationServer.setASPState(blocked, StateASPActive, time.Hour)
	applicationServer.setASPState(healthy, StateASPActive, time.Hour)

	writeEntered := make(chan struct{})
	writeRelease := make(chan struct{})
	blocked.notificationQueue = make(chan mandatoryControl, defaultNotificationQueueSize)
	blocked.notificationWriter = func(message messages.M3UA) (int, error) {
		close(writeEntered)
		<-writeRelease
		return message.MarshalLen(), nil
	}
	returned := make(chan struct{})
	go func() {
		listener.SetDestinationRangeForNetworkAndRoutingContext(
			7, 1, 0x123456, 0, DestinationUnavailable,
		)
		close(returned)
	}()
	select {
	case <-writeEntered:
	case <-time.After(time.Second):
		close(writeRelease)
		t.Fatal("blocked ASP never entered its control write")
	}
	select {
	case <-returned:
	case <-time.After(100 * time.Millisecond):
		close(writeRelease)
		t.Fatal("nonblocking destination setter waited for a blocked ASP")
	}
	if got := len(ssnmMessages(healthySent.snapshot())); got != 1 {
		close(writeRelease)
		t.Fatalf("healthy ASP received %d messages while peer was blocked, want 1", got)
	}
	if state, known := listener.DestinationStateForNetworkAndRoutingContext(7, 1, 0x123456); !known || state != DestinationUnavailable {
		close(writeRelease)
		t.Fatalf("state committed before enqueue = (%v, %v), want (Unavailable, true)", state, known)
	}
	close(writeRelease)
}

func TestDestinationCongestionAndAbatementWireOrder(t *testing.T) {
	listener, _, asp, sent := distributionFixture(
		t, params.TrafficModeLoadshare,
	)
	asp.cfg.NetworkAppearance = params.NewNetworkAppearance(7)
	applicationServer := proactiveSSNMApplicationServer(listener, 7, 1)
	asp.noteRoutingContextsActive([]uint32{1})
	asp.setState(StateASPActive)
	applicationServer.setASPState(asp, StateASPActive, time.Hour)
	sent.reset()

	if err := listener.ReportDestinationRangeForNetworkAndRoutingContext(
		7, 1, 0x123456, 0, DestinationCongested,
	); err != nil {
		t.Fatalf("report congestion: %v", err)
	}
	congested := ssnmMessages(sent.snapshot())
	if len(congested) != 1 {
		t.Fatalf("ordinary congestion report emitted %d messages, want one SCON", len(congested))
	}
	if _, ok := congested[0].(*messages.SignallingCongestion); !ok {
		t.Fatalf("ordinary congestion report = %T, want SCON", congested[0])
	}

	sent.reset()
	if err := listener.ReportDestinationRangeForNetworkAndRoutingContext(
		7, 1, 0x123456, 0, DestinationAvailable,
	); err != nil {
		t.Fatalf("report congestion abatement: %v", err)
	}
	abated := ssnmMessages(sent.snapshot())
	if len(abated) != 2 {
		t.Fatalf("congestion abatement emitted %d messages, want SCON(0), DAVA", len(abated))
	}
	scon, ok := abated[0].(*messages.SignallingCongestion)
	if !ok || scon.CongestionIndications == nil || scon.CongestionIndications.CongestionLevel() != 0 {
		t.Fatalf("first abatement message = %#v, want SCON with level 0", abated[0])
	}
	if _, ok := abated[1].(*messages.DestinationAvailable); !ok {
		t.Fatalf("second abatement message = %T, want DAVA", abated[1])
	}
}

func TestProactiveSSNMQueueOverflowClosesAssociation(t *testing.T) {
	listener, _, asp, _ := distributionFixture(
		t, params.TrafficModeLoadshare,
	)
	asp.cfg.NetworkAppearance = params.NewNetworkAppearance(7)
	applicationServer := proactiveSSNMApplicationServer(listener, 7, 1)
	asp.noteRoutingContextsActive([]uint32{1})
	asp.setState(StateASPActive)
	applicationServer.setASPState(asp, StateASPActive, time.Hour)
	asp.notificationQueue = make(chan mandatoryControl, 1)
	asp.notificationOnce.Do(func() {})

	listener.SetDestinationStateForNetworkAndRoutingContext(7, 1, 0x111111, DestinationUnavailable)
	listener.SetDestinationStateForNetworkAndRoutingContext(7, 1, 0x222222, DestinationUnavailable)
	select {
	case <-asp.Done():
	case <-time.After(time.Second):
		t.Fatal("association remained open after mandatory SSNM queue overflow")
	}
	if !errors.Is(asp.Err(), ErrNotificationQueueFull) {
		t.Fatalf("association close error = %v, want ErrNotificationQueueFull", asp.Err())
	}
}

func TestDestinationReportValidatesScopeBeforeConcurrentCommit(t *testing.T) {
	listener, _, asp, sent := distributionFixture(
		t, params.TrafficModeLoadshare,
	)
	asp.cfg.NetworkAppearance = params.NewNetworkAppearance(7)
	applicationServer := proactiveSSNMApplicationServer(listener, 7, 1)
	asp.noteRoutingContextsActive([]uint32{1})
	asp.setState(StateASPActive)
	applicationServer.setASPState(asp, StateASPActive, time.Hour)
	sent.reset()

	var waitGroup sync.WaitGroup
	errorsSeen := make(chan error, 200)
	for iteration := 0; iteration < 100; iteration++ {
		waitGroup.Add(2)
		go func(pointCode uint32) {
			defer waitGroup.Done()
			if err := listener.ReportDestinationStateForNetworkAndRoutingContext(
				7, 1, pointCode, DestinationUnavailable,
			); err != nil {
				errorsSeen <- err
			}
		}(uint32(0x100000 + iteration))
		go func(pointCode uint32) {
			defer waitGroup.Done()
			err := listener.ReportDestinationStateForNetworkAndRoutingContext(
				7, 2, pointCode, DestinationUnavailable,
			)
			if !errors.Is(err, ErrInvalidRoutingContext) {
				errorsSeen <- err
			}
		}(uint32(0x200000 + iteration))
	}
	waitGroup.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		t.Errorf("unexpected concurrent report result: %v", err)
	}
	for iteration := 0; iteration < 100; iteration++ {
		if _, known := listener.DestinationStateForNetworkAndRoutingContext(
			7, 2, uint32(0x200000+iteration),
		); known {
			t.Fatalf("invalid RC 2 destination %#x was committed", 0x200000+iteration)
		}
	}
	for _, message := range ssnmMessages(sent.snapshot()) {
		_, routingContext, _ := ssnmScope(t, message)
		if routingContext == nil || !equalTrafficModeContexts(routingContext.RoutingContexts(), []uint32{1}) {
			t.Fatalf("invalid scope reached wire in %T: %v", message, routingContext)
		}
	}
}

func TestQueuedProactiveSSNMPrecedesAspInactiveAck(t *testing.T) {
	listener, _, asp, _ := distributionFixture(
		t, params.TrafficModeLoadshare,
	)
	asp.cfg.NetworkAppearance = params.NewNetworkAppearance(7)
	applicationServer := proactiveSSNMApplicationServer(listener, 7, 1)
	asp.noteRoutingContextsActive([]uint32{1})
	asp.setState(StateASPActive)
	applicationServer.setASPState(asp, StateASPActive, time.Hour)
	asp.notificationQueue = make(chan mandatoryControl, defaultNotificationQueueSize)

	writeEntered := make(chan struct{})
	writeRelease := make(chan struct{})
	var enteredOnce sync.Once
	var eventsMu sync.Mutex
	var events []string
	asp.notificationWriter = func(message messages.M3UA) (int, error) {
		eventsMu.Lock()
		events = append(events, message.MessageTypeName())
		eventsMu.Unlock()
		if _, ok := message.(*messages.DestinationUnavailable); ok {
			enteredOnce.Do(func() { close(writeEntered) })
			<-writeRelease
		}
		return message.MarshalLen(), nil
	}
	listener.SetDestinationStateForNetworkAndRoutingContext(
		7, 1, 0x123456, DestinationUnavailable,
	)
	select {
	case <-writeEntered:
	case <-time.After(time.Second):
		close(writeRelease)
		t.Fatal("proactive DUNA never entered the control worker")
	}

	inactiveDone := make(chan error, 1)
	go func() {
		inactiveDone <- asp.handleAspInactive(messages.NewAspInactive(
			params.NewRoutingContext(1), nil,
		))
	}()
	select {
	case err := <-inactiveDone:
		close(writeRelease)
		t.Fatalf("ASP Inactive completed before queued DUNA: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	close(writeRelease)
	if err := <-inactiveDone; err != nil {
		t.Fatalf("handle ASP Inactive: %v", err)
	}
	eventsMu.Lock()
	got := append([]string(nil), events...)
	eventsMu.Unlock()
	if len(got) < 2 || got[0] != "Destination Unavailable" || got[1] != "ASP Inactive Ack" {
		t.Fatalf("control order = %v, want Destination Unavailable before ASP Inactive Ack", got)
	}
}

func ssnmMessages(sent []messages.M3UA) []messages.M3UA {
	out := make([]messages.M3UA, 0, len(sent))
	for _, message := range sent {
		if message.MessageClass() == messages.MsgClassSSNM {
			out = append(out, message)
		}
	}
	return out
}

func proactiveSSNMApplicationServer(listener *Listener, networkAppearance, routingContext uint32) *applicationServer {
	key := ASKey{
		NetworkAppearance:    networkAppearance,
		NetworkAppearanceSet: true,
		RoutingContext:       routingContext,
		RoutingContextSet:    true,
	}
	applicationServer := listener.as.get(key)
	applicationServer.setTrafficMode(params.TrafficModeLoadshare)
	return applicationServer
}

func sameSSNMKind(message messages.M3UA, kind any) bool {
	switch kind.(type) {
	case *messages.DestinationUnavailable:
		_, ok := message.(*messages.DestinationUnavailable)
		return ok
	case *messages.DestinationAvailable:
		_, ok := message.(*messages.DestinationAvailable)
		return ok
	case *messages.DestinationRestricted:
		_, ok := message.(*messages.DestinationRestricted)
		return ok
	case *messages.SignallingCongestion:
		_, ok := message.(*messages.SignallingCongestion)
		return ok
	default:
		return false
	}
}

func ssnmScope(t *testing.T, message messages.M3UA) (*params.Param, *params.Param, *params.Param) {
	t.Helper()
	switch message := message.(type) {
	case *messages.DestinationUnavailable:
		return message.NetworkAppearance, message.RoutingContext, message.AffectedPointCode
	case *messages.DestinationAvailable:
		return message.NetworkAppearance, message.RoutingContext, message.AffectedPointCode
	case *messages.DestinationRestricted:
		return message.NetworkAppearance, message.RoutingContext, message.AffectedPointCode
	case *messages.SignallingCongestion:
		return message.NetworkAppearance, message.RoutingContext, message.AffectedPointCode
	default:
		t.Fatalf("message = %T, want destination SSNM", message)
		return nil, nil, nil
	}
}
