package m3ua

import (
	"context"
	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"testing"
)

func TestIPSPDynamicRetirementDoesNotReuseReceivedCongestion(t *testing.T) {
	for _, tc := range []struct{ requester, grouped bool }{{false, false}, {true, false}, {false, true}, {true, true}} {
		requester, grouped := tc.requester, tc.grouped
		name := "responder"
		if requester {
			name = "requester"
		}
		if grouped {
			name += "-grouped"
		}
		t.Run(name, func(t *testing.T) {
			key, other := *staticASKey(10, 7), *staticASKey(10, 8)
			e, err := NewEndpoint(EndpointConfig{Role: RoleIPSP, IPSPDestinations: []IPSPDestinationConfig{{Destination: MTPDestination{MTPRoute: "retired", PointCode: 100}, AS: key}, {Destination: MTPDestination{MTPRoute: "other", PointCode: 100}, AS: other}}})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = e.Close() })
			a, _ := newTestConnWithContexts(t, StateASPActive, RoleIPSP, 8)
			a.cfg.IPSP = &IPSPConfig{ExchangeModel: IPSPExchangeSingle}
			setInventoryNetworkAppearance(&a.cfg.ApplicationServers, params.NewNetworkAppearance(10))
			if !e.trackAssociation(a) {
				t.Fatal("track")
			}
			register := func() {
				t.Helper()
				if err := a.applyRegistrationResults([]registrationResultApplication{{request: RoutingKeyRegistrationRequest{RoutingKey: testRoutingKey(10, 100, 3)}, result: RoutingKeyRegistrationResult{Status: RegistrationSuccessfullyRegistered, RoutingContext: 7}}}); err != nil {
					t.Fatal(err)
				}
				a.noteRoutingContextsActive([]uint32{7, 8})
				a.noteRoutingContextsAcked(params.NewRoutingContext(7, 8))
			}
			register()
			setAssociationTransport(t, e, a, 0)
			if grouped {
				if err := a.handleSignallingCongestion(messages.NewSignallingCongestion(params.NewNetworkAppearance(10), params.NewRoutingContext(7, 8), params.NewAffectedPointCode(100), nil, params.NewCongestionIndications(2), nil)); err != nil {
					t.Fatal(err)
				}
			} else {
				applyASPSCON(t, a, 10, 7, 100, 0, params.NewCongestionIndications(2))
			}
			applyASPSCON(t, a, 10, 8, 100, 0, params.NewCongestionIndications(1))
			status := func(route MTPRouteID) MTPDestinationStatus {
				s, ok := e.MTPDestinationStatus(MTPDestination{MTPRoute: route, PointCode: 100})
				if !ok {
					t.Fatal("missing destination")
				}
				return s
			}
			if s := status("retired"); !s.Congested || s.CongestionLevel != 2 {
				t.Fatalf("missing initial received congestion: %+v", s)
			}
			if err := a.handleAspInactive(messages.NewAspInactive(params.NewRoutingContext(7), nil)); err != nil {
				t.Fatal(err)
			}
			if a.State() != StateASPActive {
				t.Fatal("unaffected AS did not retain association activity")
			}
			if requester {
				a.removeRequesterRoutingKeyVersion(7, a.dynamicASKeyVersionFor(7, false))
			} else {
				a.applyDeregistrationResults([]RoutingKeyDeregistrationResult{{ASKey: key, RoutingContext: 7, Status: DeregistrationSuccessfullyDeregistered}})()
			}
			// A delayed association publication must resolve current membership.
			a.transportProjectionPending.Store(true)
			if a.transportProjectionPending.Swap(false) {
				e.transport.project(e, a)
			}
			if s := status("retired"); s.Availability != DestinationUnavailable || s.Congested {
				t.Fatalf("retired AS retained a contribution: %+v", s)
			}
			register()
			if s := status("retired"); s.Availability != DestinationAvailable || s.Congested {
				t.Errorf("re-registration resurrected old SCON: %+v", s)
			}
			if s := status("other"); !s.Congested || s.CongestionLevel != 1 {
				t.Errorf("retirement changed unaffected AS: %+v", s)
			}
			if s := a.TransportCongestion(); !s.Known || s.Level != 0 {
				t.Errorf("retirement reset the shared physical detector: %+v", s)
			}
			applyASPSCON(t, a, 10, 7, 100, 0, params.NewCongestionIndications(3))
			if s := status("retired"); !s.Congested || s.CongestionLevel != 3 {
				t.Errorf("new registration did not accept fresh SCON: %+v", s)
			}
		})
	}
}

func TestCongestionRetirementPreservesStaticScope(t *testing.T) {
	for _, role := range []Role{RoleASP, RoleIPSP} {
		t.Run(role.String(), func(t *testing.T) {
			e, err := NewEndpoint(EndpointConfig{Role: role})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = e.Close() })
			c, _ := newTestConnWithContexts(t, StateASPActive, role, 7)
			if role == RoleIPSP {
				c.cfg.IPSP = &IPSPConfig{ExchangeModel: IPSPExchangeSingle}
			}
			setInventoryNetworkAppearance(&c.cfg.ApplicationServers, params.NewNetworkAppearance(10))
			if !e.trackAssociation(c) {
				t.Fatal("track")
			}
			key := *staticASKey(10, 7)
			c.addDynamicASKey(key, testRoutingKey(10, 100, 3), false)
			c.noteRoutingContextsActive([]uint32{7})
			c.noteRoutingContextsAcked(params.NewRoutingContext(7))
			applyASPSCON(t, c, 10, 7, 100, 0, params.NewCongestionIndications(2))
			c.applyDeregistrationResults([]RoutingKeyDeregistrationResult{{ASKey: key, RoutingContext: 7, Status: DeregistrationSuccessfullyDeregistered}})()
			state, known := c.destinations.lookup(destinationKey{networkAppearance: 10, networkAppearanceSet: true, routingContext: 7, routingContextSet: true, pointCode: 100})
			if !known || !state.Congestion.Congested || state.Congestion.Level != 2 {
				t.Fatalf("static scope lost received congestion: %+v known=%v", state, known)
			}
		})
	}
}

func TestRetiredRoutingContextCannotInheritGroupedDimensions(t *testing.T) {
	for _, congestion := range []bool{false, true} {
		name := "availability"
		if congestion {
			name = "congestion"
		}
		t.Run(name, func(t *testing.T) {
			d := &destinations{}
			value := destinationRange{NetworkAppearance: 10, NetworkAppearanceSet: true, PointCode: 100}
			if congestion {
				value.State.Congestion = CongestionState{Congested: true, Level: 2, LevelSet: true}
				if err := d.setScopedCongestionRangesWithinBudget([]uint32{7, 8}, []destinationRange{value}); err != nil {
					t.Fatal(err)
				}
			} else {
				value.State.Availability = DestinationUnavailable
				if err := d.setScopedRangesWithinBudget([]uint32{7, 8}, []destinationRange{value}); err != nil {
					t.Fatal(err)
				}
			}
			d.forgetRoutingContext(7)
			fresh := destinationRange{NetworkAppearance: 10, NetworkAppearanceSet: true, PointCode: 100}
			if congestion {
				if err := d.setScopedRangesWithinBudget([]uint32{7, 8}, []destinationRange{fresh}); err != nil {
					t.Fatal(err)
				}
			} else {
				fresh.State.Congestion = CongestionState{Congested: true, Level: 2, LevelSet: true}
				if err := d.setScopedCongestionRangesWithinBudget([]uint32{7, 8}, []destinationRange{fresh}); err != nil {
					t.Fatal(err)
				}
			}
			lookup := func(rc uint32) DestinationNetworkState {
				s, _ := d.lookup(destinationKey{networkAppearance: 10, networkAppearanceSet: true, routingContext: rc, routingContextSet: true, pointCode: 100})
				return s
			}
			first, other := lookup(7), lookup(8)
			if congestion {
				if first.Congestion.Congested || !other.Congestion.Congested || other.Congestion.Level != 2 {
					t.Fatalf("retired congestion dimension was inherited: new=%+v other=%+v", first, other)
				}
			} else {
				if first.Availability != DestinationAvailable || other.Availability != DestinationUnavailable {
					t.Fatalf("retired availability dimension was inherited: new=%+v other=%+v", first, other)
				}
			}
		})
	}
}

func TestASPDynamicRetirementRefreshesTransportProjection(t *testing.T) {
	e, a := newDynamicSSNMBinding(t, false)
	a.handleSignals(context.Background(), messages.NewAspActiveAck(nil, params.NewRoutingContext(7), nil))
	setAssociationTransport(t, e, a, 3)
	applyASPDAVA(t, a, 10, 7, 0x123456, 0)
	destination := MTPDestination{MTPRoute: "sccp-a", PointCode: 0x123456}
	if s, _ := e.MTPDestinationStatus(destination); !s.Congested {
		t.Fatal("missing initial transport contribution")
	}
	// Successful response application is authoritative even if an activation or
	// state publication raced the response. Retiring membership must refresh now.
	a.removeRequesterRoutingKeyVersion(7, a.dynamicASKeyVersionFor(7, false))
	if s, _ := e.MTPDestinationStatus(destination); s.Availability != DestinationUnavailable || s.Congested {
		t.Fatalf("retired dynamic AS kept transport projection: %+v", s)
	}
	e.transport.project(e, a)
	if s, _ := e.MTPDestinationStatus(destination); s.Availability != DestinationUnavailable || s.Congested {
		t.Fatalf("delayed publication restored retired AS: %+v", s)
	}
}

func TestRegistrationRetirementPreservesSGPAuthority(t *testing.T) {
	e := newRegistrationLifetimeEndpoint(t, false)
	a, aSent := newRegistrationLifetimeASP(t, e)
	b, bSent := newRegistrationLifetimeASP(t, e)
	a.destinations, b.destinations = e.destinations, e.destinations
	key := testRoutingKey(10, 100, 3)
	rc := registerLifetimeKey(t, a, aSent, 1, key)
	if got := registerLifetimeKey(t, b, bSent, 2, key); got != rc {
		t.Fatal("peers did not share an AS")
	}
	value := destinationRange{NetworkAppearance: 10, NetworkAppearanceSet: true, PointCode: 100, State: DestinationNetworkState{Availability: DestinationUnavailable}}
	if err := e.destinations.setScopedRangesWithinBudget([]uint32{rc}, []destinationRange{value}); err != nil {
		t.Fatal(err)
	}
	value.State.Congestion = CongestionState{Congested: true, Level: 2, LevelSet: true}
	if err := e.destinations.setScopedCongestionRangesWithinBudget([]uint32{rc}, []destinationRange{value}); err != nil {
		t.Fatal(err)
	}
	if err := a.handleDeregistrationRequest(messages.NewDeregistrationRequest(params.NewRoutingContext(rc))); err != nil {
		t.Fatal(err)
	}
	state, known := e.destinations.lookup(destinationKey{networkAppearance: 10, networkAppearanceSet: true, routingContext: rc, routingContextSet: true, pointCode: 100})
	if !known || state.Availability != DestinationUnavailable || !state.Congestion.Congested || state.Congestion.Level != 2 {
		t.Fatalf("peer retirement erased shared SGP authority: %+v known=%v", state, known)
	}
	assertLifetimeRegistered(t, b, rc)
}
