package m3ua

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

func TestTransportCongestionRejectsLateEpochAndClosedAssociation(t *testing.T) {
	e, err := NewEndpoint(EndpointConfig{Role: RoleASP})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	a, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
	if !e.trackAssociation(a) {
		t.Fatal("track")
	}
	job := transportCongestionJob{association: a, id: a.ID(), epoch: a.Epoch()}
	now := time.Unix(1000, 0)
	e.transport.now = func() time.Time { return now }
	e.transport.read = func(*Association) (*sctp.SendBacklog, error) {
		a.resetTransportCongestionEpoch()
		return &sctp.SendBacklog{Charged: 99, Limit: 100}, nil
	}
	e.transport.sample(e, job)
	if s := a.TransportCongestion(); s.SampleValid || s.Known {
		t.Fatalf("old epoch committed: %+v", s)
	}
	if s := a.transportSnapshot.Load(); s != nil {
		t.Fatalf("late result installed a snapshot in the unsampled epoch: %+v", s)
	}
	e.transport.read = func(*Association) (*sctp.SendBacklog, error) { return &sctp.SendBacklog{Charged: 99, Limit: 100}, nil }
	job.epoch = a.Epoch()
	now = now.Add(300 * time.Millisecond)
	e.transport.sample(e, job)
	if state := a.TransportCongestion(); !state.SampleValid || state.Level != 0 {
		t.Fatalf("fresh epoch reused old dwell or was not sampled: %+v", state)
	}
	_ = a.Close()
	now = now.Add(time.Second)
	e.transport.sample(e, job)
	if s := a.TransportCongestion(); s.SampledAt.Equal(now) {
		t.Fatal("closed association accepted sample")
	}
}

func TestTransportCongestionManagementReportsChangesWithoutSCON(t *testing.T) {
	e, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	a, _ := newTestConnWithContexts(t, StateASPActive, RoleSGP, 1)
	if !e.trackAssociation(a) {
		t.Fatal("track")
	}
	ch := a.ManagementIndications()
	now := time.Unix(1000, 0)
	e.transport.now = func() time.Time { return now }
	e.transport.read = func(*Association) (*sctp.SendBacklog, error) { return &sctp.SendBacklog{Charged: 95, Limit: 100}, nil }
	job := transportCongestionJob{a, a.ID(), a.Epoch()}
	e.transport.sample(e, job)
	for range 3 {
		now = now.Add(100 * time.Millisecond)
		e.transport.sample(e, job)
	}
	if s := a.TransportCongestion(); s.Level != 3 || !s.Known {
		t.Fatalf("onset=%+v", s)
	}
	found := false
	for len(ch) > 0 {
		v := <-ch
		if v.Kind == ManagementTransportCongestion && v.TransportCongestion != nil && v.TransportCongestion.Level == 3 {
			found = true
		}
	}
	if !found {
		t.Fatal("management missed transport onset")
	}
	e.transport.read = func(*Association) (*sctp.SendBacklog, error) { return nil, errors.New("unavailable") }
	now = now.Add(time.Second)
	e.transport.sample(e, job)
	if s := a.TransportCongestion(); s.SampleValid || s.Level != 3 || !s.Known {
		t.Fatalf("error cleared pressure: %+v", s)
	}
	now = now.Add(3 * time.Second)
	e.transport.sample(e, job)
	if s := a.TransportCongestion(); s.Known || s.Level != 3 {
		t.Fatalf("stale pressure was clear or pinned: %+v", s)
	}
}

func TestTransportCongestionAggregatesAlternativesAndPreservesSCON(t *testing.T) {
	cfg := validASPConfig()
	cfg.Routing.AllowUnknownDestinations = true
	e, associations, _ := newUnreportedASPTransferFixture(t, cfg)
	a := associations["sg-a/sgp-a1"]
	b := associations["sg-b/sgp-b1"]
	set := func(c *Association, level uint8) {
		s := TransportCongestionState{Association: c.ID(), Epoch: c.Epoch(), Known: true, SampleValid: true, SampledAt: time.Unix(1000, 0), Level: level}
		c.transportSnapshot.Store(&s)
		e.aspRoutes.transportChanged(c)
	}
	for _, c := range associations {
		set(c, 0)
	}
	set(a, 3)
	d := MTPDestination{MTPRoute: "sccp-a", PointCode: 0x123456}
	if s, ok := e.MTPDestinationStatus(d); !ok || s.Congested {
		t.Fatalf("healthy alternate hidden: %+v %v", s, ok)
	}
	for _, c := range associations {
		set(c, 2)
	}
	if s, ok := e.MTPDestinationStatus(d); !ok || !s.Congested || s.CongestionLevel != 2 {
		t.Fatalf("all-congested=%+v %v", s, ok)
	}
	for _, c := range associations {
		if err := c.handleSignallingCongestion(messages.NewSignallingCongestion(params.NewNetworkAppearance(c.cfg.ApplicationServers[0].ASKey.NetworkAppearance), params.NewRoutingContext(c.cfg.ApplicationServers[0].ASKey.RoutingContext), params.NewAffectedPointCode(0x123456), nil, params.NewCongestionIndications(3), nil)); err != nil {
			t.Fatal(err)
		}
	}
	set(a, 0)
	set(b, 0)
	if s, ok := e.MTPDestinationStatus(d); !ok || !s.Congested || s.CongestionLevel != 3 {
		t.Fatalf("transport clear erased received SCON: %+v %v", s, ok)
	}
}

func setAssociationTransport(t *testing.T, e *Endpoint, a *Association, level uint8) {
	t.Helper()
	s := TransportCongestionState{Association: a.ID(), Epoch: a.Epoch(), Known: true, SampleValid: true, SampledAt: time.Unix(1000, 0), Level: level}
	a.transportSnapshot.Store(&s)
	if e.aspRoutes != nil {
		e.aspRoutes.transportChanged(a)
	}
	e.refreshIPSPDestinations()
}

func TestTransportCongestionRoutingPolicyAndPreference(t *testing.T) {
	for _, prefer := range []bool{false, true} {
		t.Run(fmt.Sprint(prefer), func(t *testing.T) {
			cfg := validASPConfig()
			cfg.Routing.SignallingGatewaySelection = RouteSelectionPrimaryBackup
			cfg.Routing.PreferUncongestedTransport = prefer
			e, associations, _ := newASPTransferFixture(t, cfg)
			a := associations["sg-a/sgp-a1"]
			b := associations["sg-b/sgp-b1"]
			setAssociationTransport(t, e, a, 0)
			setAssociationTransport(t, e, b, 0)
			req := MTPTransferRequest{ProtocolData: transferProtocolData(0x123456, 1, nil)}
			before, err := e.MTPTransfer(req)
			if err != nil {
				t.Fatal(err)
			}
			setAssociationTransport(t, e, a, 3)
			after, err := e.MTPTransfer(req)
			if err != nil {
				t.Fatal(err)
			}
			if !prefer && before.SuccessfulPaths[0].Association != after.SuccessfulPaths[0].Association {
				t.Fatal("default moved SLS flow on pressure")
			}
			if prefer && after.SuccessfulPaths[0].Association != b.ID() {
				t.Fatal("opt-in did not prefer healthy alternate")
			}
			e.aspRoutes.config.congestionPolicy = func(_, level uint8, set bool) bool { return set && level < 3 }
			_, err = e.MTPTransfer(req)
			if err != nil {
				t.Fatal(err)
			}
			setAssociationTransport(t, e, b, 3)
			if _, err = e.MTPTransfer(req); !errors.Is(err, ErrNoMTPRoute) {
				t.Fatalf("policy ignored transport: %v", err)
			}
		})
	}
}

func TestTransportCongestionIPSPChannelMappingAndTypedSCON(t *testing.T) {
	key := ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true}
	d := MTPDestination{MTPRoute: "peer", PointCode: 0x123456}
	e, err := NewEndpoint(EndpointConfig{Role: RoleIPSP, IPSPDestinations: []IPSPDestinationConfig{{Destination: d, AS: key}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	if e.MTPIndications() == nil {
		t.Fatal("IPSP has no MTP-STATUS channel")
	}
	a, _ := newTestConnWithContexts(t, StateASPActive, RoleIPSP, 1)
	setInventoryNetworkAppearance(&a.cfg.ApplicationServers, params.NewNetworkAppearance(7))
	a.noteRoutingContextsActive([]uint32{1})
	if !e.trackAssociation(a) {
		t.Fatal("track")
	}
	e.refreshIPSPDestinations()
	for len(e.MTPIndications()) > 0 {
		<-e.MTPIndications()
	}
	setAssociationTransport(t, e, a, 2)
	select {
	case ind := <-e.MTPIndications():
		if ind.Kind != MTPStatusIndication || !ind.Destination.Congested || ind.Destination.CongestionLevel != 2 {
			t.Fatalf("IPSP onset=%+v", ind)
		}
	default:
		t.Fatal("missing IPSP onset")
	}
	setAssociationTransport(t, e, a, 0)
	select {
	case ind := <-e.MTPIndications():
		if ind.Kind != MTPStatusIndication || ind.Destination.Congested {
			t.Fatalf("IPSP abatement=%+v", ind)
		}
	default:
		t.Fatal("missing IPSP abatement")
	}
	writes := 0
	a.signalWriter = func(m messages.M3UA) (int, error) { writes++; return m.MarshalLen(), nil }
	if err = a.SignallingCongestion(SignallingCongestionRequest{Scope: WireScope{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContexts: []uint32{1}, RoutingContextSet: true}, Destinations: []PointCodeRange{{PointCode: 0x111111}}, CongestionLevel: 2, CongestionLevelSet: true}); err != nil {
		t.Fatal(err)
	}
	if writes != 1 {
		t.Fatalf("SCON writes=%d", writes)
	}
	if _, ok := e.MTPDestinationStatus(d); !ok {
		t.Fatal("IPSP snapshot missing")
	}
	_ = e.Close()
	for range e.MTPIndications() {
	} // Drain the final MTP-PAUSE before observing closure.
	if status, ok := e.MTPDestinationStatus(d); !ok || status.Availability != DestinationUnavailable {
		t.Fatalf("closed IPSP still available: %+v %v", status, ok)
	}
}

func TestTransportCongestionRestartIsUnknownAndBroadcastIncludesEveryCopy(t *testing.T) {
	cfg := validASPConfig()
	cfg.Routing.SignallingGatewaySelection = RouteSelectionBroadcast
	e, associations, _ := newASPTransferFixture(t, cfg)
	a := associations["sg-a/sgp-a1"]
	b := associations["sg-b/sgp-b1"]
	setAssociationTransport(t, e, a, 3)
	setAssociationTransport(t, e, b, 0)
	d := MTPDestination{MTPRoute: "sccp-a", PointCode: 0x123456}
	s, _ := e.MTPDestinationStatus(d)
	if !s.Congested || s.CongestionLevel != 3 {
		t.Fatalf("broadcast hid required transport copy: %+v", s)
	}
	a.resetTransportCongestionEpoch()
	s, _ = e.MTPDestinationStatus(d)
	if !s.TransportCongestionUnknown {
		t.Fatalf("restart fabricated clear: %+v", s)
	}
	setAssociationTransport(t, e, a, 0)
	applyASPSCON(t, a, 7, 1, 0x123456, 0, params.NewCongestionIndications(2))
	s, _ = e.MTPDestinationStatus(d)
	if !s.Congested || s.CongestionLevel != 2 {
		t.Fatalf("broadcast hid required received SCON: %+v", s)
	}
}

func TestTransportCongestionIPSPScopedLifecycleWithoutDetector(t *testing.T) {
	key := ASKey{RoutingContext: 1, RoutingContextSet: true}
	e, err := NewEndpoint(EndpointConfig{Role: RoleIPSP, TransportCongestion: &TransportCongestionConfig{Disabled: true}, IPSPDestinations: []IPSPDestinationConfig{{Destination: MTPDestination{MTPRoute: "peer", PointCode: 0x120000, Mask: 16}, AS: key}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	a, _ := newTestConnWithContexts(t, StateASPActive, RoleIPSP, 1, 2)
	a.noteRoutingContextsActive([]uint32{1, 2})
	if !e.trackAssociation(a) {
		t.Fatal("track")
	}
	d := MTPDestination{MTPRoute: "peer", PointCode: 0x123456}
	if s, ok := e.MTPDestinationStatus(d); !ok || s.Availability != DestinationAvailable {
		t.Fatalf("initial=%+v %v", s, ok)
	}
	a.noteRoutingContextsInactive([]uint32{1})
	if s, ok := e.MTPDestinationStatus(d); !ok || s.Availability != DestinationUnavailable {
		t.Fatalf("partial inactive stale=%+v %v", s, ok)
	}
	a.noteRoutingContextsActive([]uint32{1})
	if err = a.handleSignallingCongestion(messages.NewSignallingCongestion(nil, params.NewRoutingContext(1), params.NewAffectedPointCode(0x123456), nil, params.NewCongestionIndications(2), nil)); err != nil {
		t.Fatal(err)
	}
	if s, ok := e.MTPDestinationStatus(d); !ok || !s.Congested || s.CongestionLevel != 2 {
		t.Fatalf("ranged SCON=%+v %v", s, ok)
	}
	a.pauseDestinations()
	e.refreshIPSPDestinations()
	if s, ok := e.MTPDestinationStatus(d); !ok || s.Availability != DestinationAvailable || s.Congested {
		t.Fatalf("old epoch poisoned IPSP=%+v %v", s, ok)
	}
}

func TestTransportCongestionSamplingReleasesLocksAndRejectsIdentity(t *testing.T) {
	e, err := NewEndpoint(EndpointConfig{Role: RoleASP})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	a, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
	if !e.trackAssociation(a) {
		t.Fatal("track")
	}
	e.transport.read = func(*Association) (*sctp.SendBacklog, error) {
		if !e.mu.TryLock() {
			t.Fatal("Endpoint lock held across query")
		}
		e.mu.Unlock()
		if !a.transportMu.TryLock() {
			t.Fatal("detector lock held across query")
		}
		a.transportMu.Unlock()
		if !e.aspRoutes.mu.TryLock() {
			t.Fatal("route lock held across query")
		}
		e.aspRoutes.mu.Unlock()
		return &sctp.SendBacklog{Charged: 0, Limit: 100}, nil
	}
	e.transport.sample(e, transportCongestionJob{a, a.ID() + 1, a.Epoch()})
	if a.TransportCongestion().SampleValid {
		t.Fatal("wrong identity accepted")
	}
	e.transport.sample(e, transportCongestionJob{a, a.ID(), a.Epoch()})
	if !a.TransportCongestion().Known {
		t.Fatal("fresh sample not committed")
	}
}

func TestTransportCongestionIPSPDoubleExchangeTypedSCONUsesLocalScope(t *testing.T) {
	a, sent := newDoubleExchangeIPSPForTest(t)
	a.setIPSPState(IPSPState{TrafficToLocal: StateASPActive, TrafficToPeer: StateASPActive})
	a.noteRoutingContextsAcked(params.NewRoutingContext(11))
	a.noteRoutingContextsActive([]uint32{22})
	request := SignallingCongestionRequest{Scope: WireScope{NetworkAppearance: 10, NetworkAppearanceSet: true, RoutingContexts: []uint32{11}, RoutingContextSet: true}, Destinations: []PointCodeRange{{PointCode: 0x111111}}, CongestionLevel: 1, CongestionLevelSet: true}
	if err := a.SignallingCongestion(request); err != nil {
		t.Fatal(err)
	}
	if len(*sent) != 1 {
		t.Fatalf("sent=%d", len(*sent))
	}
	request.Scope.NetworkAppearance = 20
	request.Scope.RoutingContexts = []uint32{22}
	if err := a.SignallingCongestion(request); !errors.Is(err, ErrInvalidNetworkAppearance) {
		t.Fatalf("peer-directed local report accepted: %v", err)
	}
	request.Scope.NetworkAppearance = 10
	request.Scope.RoutingContexts = []uint32{11}
	a.setIPSPState(IPSPState{TrafficToLocal: StateASPInactive, TrafficToPeer: StateASPActive})
	if err := a.SignallingCongestion(request); !errors.Is(err, ErrNotEstablished) {
		t.Fatalf("inactive local direction report accepted: %v", err)
	}
}

func TestTransportCongestionBroadcastSelectionUsesWorstRequiredSGP(t *testing.T) {
	cfg := validASPConfig()
	cfg.Routing.SignallingGatewaySelection = RouteSelectionPrimaryBackup
	cfg.Routing.PreferUncongestedTransport = true
	cfg.Routing.SignallingGatewayProcessSelection["sg-a"] = RouteSelectionBroadcast
	second := cfg.SignallingGateways[0].SGPs[0]
	second.ID = "sgp-a2"
	cfg.SignallingGateways[0].SGPs = append(cfg.SignallingGateways[0].SGPs, second)
	e, associations, _ := newASPTransferFixture(t, cfg)
	setAssociationTransport(t, e, associations["sg-a/sgp-a1"], 0)
	setAssociationTransport(t, e, associations["sg-a/sgp-a2"], 3)
	setAssociationTransport(t, e, associations["sg-b/sgp-b1"], 1)
	result, err := e.MTPTransfer(MTPTransferRequest{ProtocolData: transferProtocolData(0x123456, 1, nil)})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.SuccessfulPaths) != 1 || result.SuccessfulPaths[0].SGP.SignallingGateway != "sg-b" {
		t.Fatalf("broadcast pressure hidden by member: %+v", result)
	}
}

func TestTransportCongestionBroadcastIncludesRestrictedCopy(t *testing.T) {
	cfg := validASPConfig()
	cfg.Routing.SignallingGatewaySelection = RouteSelectionBroadcast
	e, associations, _ := newASPTransferFixture(t, cfg)
	setAssociationTransport(t, e, associations["sg-a/sgp-a1"], 0)
	setAssociationTransport(t, e, associations["sg-b/sgp-b1"], 3)
	applyASPDRST(t, associations["sg-b/sgp-b1"], 9, 42, 0x123456, 0)
	status, ok := e.MTPDestinationStatus(MTPDestination{MTPRoute: "sccp-a", PointCode: 0x123456})
	if !ok || status.Availability != DestinationAvailable || !status.Congested || status.CongestionLevel != 3 {
		t.Fatalf("restricted copy hidden: %+v %v", status, ok)
	}
}

func TestTransportCongestionDelayedDiagnosticsCannotRestoreStaleBacklog(t *testing.T) {
	e, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	a, _ := newTestConnWithContexts(t, StateASPActive, RoleSGP, 1)
	if !e.trackAssociation(a) {
		t.Fatal("track")
	}
	now := time.Unix(1000, 0)
	e.transport.now = func() time.Time { return now }
	e.transport.read = func(*Association) (*sctp.SendBacklog, error) { return &sctp.SendBacklog{Charged: 95, Limit: 100}, nil }
	job := transportCongestionJob{a, a.ID(), a.Epoch()}
	e.transport.sample(e, job)
	for range 2 {
		now = now.Add(100 * time.Millisecond)
		e.transport.sample(e, job)
	}
	now = now.Add(100 * time.Millisecond)
	e.transport.readStatus = func(*Association) (*sctp.Status, error) {
		now = now.Add(4 * time.Second)
		e.transport.missed(e, job)
		return &sctp.Status{PeerRwnd: 42}, nil
	}
	e.transport.sample(e, job)
	state := a.TransportCongestion()
	if state.Known || state.SampleValid || state.MissedSamples != 1 || state.Level != 3 || !state.DiagnosticAt.Equal(now) || state.ReceiverWindow != 42 {
		t.Fatalf("late status replayed old measurement: %+v", state)
	}
}

func TestTransportCongestionUnavailablePolicyDoesNotPreferUnknown(t *testing.T) {
	cfg := validASPConfig()
	cfg.Routing.PreferUncongestedTransport = true
	cfg.Routing.SignallingGatewaySelection = RouteSelectionPrimaryBackup
	e, associations, _ := newASPTransferFixture(t, cfg)
	a := associations["sg-a/sgp-a1"]
	b := associations["sg-b/sgp-b1"]
	setAssociationTransport(t, e, b, 0)
	unknown := TransportCongestionState{Association: a.ID(), Epoch: a.Epoch(), SampledAt: time.Unix(1000, 0), Error: "unsupported"}
	a.transportSnapshot.Store(&unknown)
	e.aspRoutes.transportChanged(a)
	req := MTPTransferRequest{ProtocolData: transferProtocolData(0x123456, 1, nil)}
	result, err := e.MTPTransfer(req)
	if err != nil || len(result.SuccessfulPaths) != 1 || result.SuccessfulPaths[0].Association != b.ID() {
		t.Fatalf("unknown preferred over measured clear: %+v %v", result, err)
	}
	e.aspRoutes.config.congestionPolicy = func(_, _ uint8, set bool) bool { return set }
	b.transportSnapshot.Store(&TransportCongestionState{Association: b.ID(), Epoch: b.Epoch(), SampledAt: time.Unix(1000, 0), Error: "unavailable"})
	e.aspRoutes.transportChanged(b)
	if _, err = e.MTPTransfer(req); !errors.Is(err, ErrNoMTPRoute) {
		t.Fatalf("unknown-level policy ignored: %v", err)
	}
}

func TestTransportCongestionFirstMissExposesUnknown(t *testing.T) {
	cfg := validASPConfig()
	useSignallingGateways(cfg, "sg-a")
	e, associations, _ := newASPTransferFixture(t, cfg)
	a := associations["sg-a/sgp-a1"]
	if a.transportSnapshot.Load() != nil {
		t.Fatal("fixture unexpectedly sampled")
	}
	e.transport.missed(e, transportCongestionJob{a, a.ID(), a.Epoch()})
	status, ok := e.MTPDestinationStatus(MTPDestination{MTPRoute: "sccp-a", PointCode: 0x123456})
	if !ok || !status.TransportCongestionUnknown || a.TransportCongestion().MissedSamples != 1 {
		t.Fatalf("missed detector appeared clear: %+v %v", status, ok)
	}
}

func TestTransportCongestionConcurrentPreferenceChangesKeepUsableMember(t *testing.T) {
	cfg := validASPConfig()
	useSignallingGateways(cfg, "sg-a")
	cfg.Routing.SignallingGatewaySelection = RouteSelectionPrimaryBackup
	cfg.Routing.PreferUncongestedTransport = true
	e, associations, _ := newASPTransferFixture(t, cfg)
	a := associations["sg-a/sgp-a1"]
	a.dataWriter = func(frame []byte, _ *sctp.SndInfo) (int, error) { return len(frame), nil }
	low := TransportCongestionState{Association: a.ID(), Epoch: a.Epoch(), Known: true, SampleValid: true, SampledAt: time.Unix(1000, 0)}
	high := low
	high.Level = 3
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			a.transportSnapshot.Store(&low)
			a.transportSnapshot.Store(&high)
		}
	}()
	t.Cleanup(func() { close(stop); <-done })
	for range 10000 {
		result, err := e.MTPTransfer(MTPTransferRequest{ProtocolData: transferProtocolData(0x123456, 1, nil)})
		if err != nil || len(result.SuccessfulPaths) != 1 {
			t.Fatalf("lost usable member during pressure update: %+v %v", result, err)
		}
	}
}
