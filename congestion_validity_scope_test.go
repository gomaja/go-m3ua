package m3ua

import (
	"fmt"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestValidityKeepsStandaloneASAndNetworkScopesIndependent(t *testing.T) {
	for _, role := range []Role{RoleASP, RoleIPSP} {
		for _, secondNA := range []uint32{7, 9} {
			t.Run(fmt.Sprintf("%s/NA%d", role, secondNA), func(t *testing.T) {
				first := ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true}
				second := ASKey{NetworkAppearance: secondNA, NetworkAppearanceSet: true, RoutingContext: 2, RoutingContextSet: true}
				destinations := []IPSPDestinationConfig(nil)
				if role == RoleIPSP {
					destinations = []IPSPDestinationConfig{{Destination: MTPDestination{MTPRoute: "one", PointCode: 100}, AS: first}, {Destination: MTPDestination{MTPRoute: "two", PointCode: 100}, AS: second}}
				}
				e, err := NewEndpoint(EndpointConfig{Role: role, Congestion: &CongestionConfig{Default: CongestionProfile{Method: CongestionNationalWithPriorities, MaxLevel: 3}}, IPSPDestinations: destinations})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = e.Close() })
				now := time.Now()
				e.ssnm.validity = 10 * time.Second
				e.ssnm.now = func() time.Time { return now }
				a, _ := newTestConnWithContexts(t, StateASPActive, role, 1, 2)
				a.cfg.ApplicationServers = []ASConfig{{ASKey: first, TrafficMode: params.TrafficModeLoadshare}, {ASKey: second, TrafficMode: params.TrafficModeLoadshare}}
				if role == RoleIPSP {
					a.cfg.IPSP = &IPSPConfig{ExchangeModel: IPSPExchangeSingle}
				}
				a.noteRoutingContextsActive([]uint32{1, 2})
				a.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
				if !e.trackAssociation(a) {
					t.Fatal("track")
				}
				t.Cleanup(func() { _ = a.Close() })
				receive := func(key ASKey, level uint8) {
					t.Helper()
					if err := a.handleSignallingCongestion(messages.NewSignallingCongestion(params.NewNetworkAppearance(key.NetworkAppearance), params.NewRoutingContext(key.RoutingContext), params.NewAffectedPointCode(100), nil, params.NewCongestionIndications(level), nil)); err != nil {
						t.Fatal(err)
					}
				}
				payload := &params.ProtocolDataPayload{DestinationPointCode: 100, MessagePriority: 0}
				receive(first, 3)
				now = now.Add(time.Second)
				receive(second, 0)
				if err := a.validateMethodData(first, payload); err == nil {
					t.Fatal("another AS clear removed first AS congestion")
				}
				if err := a.validateMethodData(second, payload); err != nil {
					t.Fatalf("first AS congestion leaked to clear AS: %v", err)
				}
				if role == RoleIPSP {
					status, _ := e.MTPDestinationStatus(MTPDestination{MTPRoute: "one", PointCode: 100})
					if !status.Congested || status.ReceivedCongestionUnknown {
						t.Fatalf("first scoped projection = %+v", status)
					}
				}
				now = now.Add(9 * time.Second)
				e.runCongestionAutomation(now)
				if err := a.validateMethodData(first, payload); err == nil {
					t.Fatal("expired first AS was admitted as measured clear")
				}
				if err := a.validateMethodData(second, payload); err != nil {
					t.Fatalf("first AS expiry leaked to clear AS: %v", err)
				}
				if role == RoleIPSP {
					one, _ := e.MTPDestinationStatus(MTPDestination{MTPRoute: "one", PointCode: 100})
					two, _ := e.MTPDestinationStatus(MTPDestination{MTPRoute: "two", PointCode: 100})
					if one.Congested || !one.ReceivedCongestionUnknown || two.Congested || two.ReceivedCongestionUnknown {
						t.Fatalf("expiry crossed AS/network: one=%+v two=%+v", one, two)
					}
				}
			})
		}
	}
}

func TestRoutingContextListValidityPreservesResolvedApplicability(t *testing.T) {
	first := ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true}
	second := first
	second.RoutingContext = 2
	e, err := NewEndpoint(EndpointConfig{Role: RoleIPSP, Congestion: &CongestionConfig{Default: CongestionProfile{Method: CongestionNationalWithPriorities, MaxLevel: 3}}, IPSPDestinations: []IPSPDestinationConfig{{Destination: MTPDestination{MTPRoute: "one", PointCode: 100}, AS: first}, {Destination: MTPDestination{MTPRoute: "two", PointCode: 100}, AS: second}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	now := time.Now()
	e.ssnm.validity = 10 * time.Second
	e.ssnm.now = func() time.Time { return now }
	a, _ := newTestConnWithContexts(t, StateASPActive, RoleIPSP, 1, 2)
	a.cfg.IPSP = &IPSPConfig{ExchangeModel: IPSPExchangeSingle}
	setInventoryNetworkAppearance(&a.cfg.ApplicationServers, params.NewNetworkAppearance(7))
	a.noteRoutingContextsActive([]uint32{1, 2})
	a.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
	if !e.trackAssociation(a) {
		t.Fatal("track")
	}
	t.Cleanup(func() { _ = a.Close() })
	if err := a.handleSignallingCongestion(messages.NewSignallingCongestion(params.NewNetworkAppearance(7), params.NewRoutingContext(1, 2), params.NewAffectedPointCode(100), nil, params.NewCongestionIndications(3), nil)); err != nil {
		t.Fatal(err)
	}
	payload := &params.ProtocolDataPayload{DestinationPointCode: 100, MessagePriority: 0}
	for _, key := range []ASKey{first, second} {
		if a.validateMethodData(key, payload) == nil {
			t.Fatal("RC list failed to cover resolved AS", key)
		}
	}
	sources := e.SSNMKnowledge().Partitions[0].Destinations[0].CongestionSources
	if len(sources) != 2 || sources[0].AS == sources[1].AS || !sources[0].Scope.RoutingContextSet || !sources[1].Scope.RoutingContextSet || len(sources[0].Scope.RoutingContexts) != 2 || len(sources[1].Scope.RoutingContexts) != 2 {
		t.Fatalf("resolved/raw applicability lost: %+v", sources)
	}
	if err := a.handleSignallingCongestion(messages.NewSignallingCongestion(params.NewNetworkAppearance(7), params.NewRoutingContext(2), params.NewAffectedPointCode(100), nil, params.NewCongestionIndications(0), nil)); err != nil {
		t.Fatal(err)
	}
	if a.validateMethodData(first, payload) == nil || a.validateMethodData(second, payload) != nil {
		t.Fatal("partial clear crossed RC-list applicability")
	}
	now = now.Add(10 * time.Second)
	e.runCongestionAutomation(now)
	one, _ := e.MTPDestinationStatus(MTPDestination{MTPRoute: "one", PointCode: 100})
	two, _ := e.MTPDestinationStatus(MTPDestination{MTPRoute: "two", PointCode: 100})
	if !one.ReceivedCongestionUnknown || two.ReceivedCongestionUnknown || two.Congested {
		t.Fatalf("old RC-list timer crossed renewed source: one=%+v two=%+v", one, two)
	}
}

func TestOmittedRoutingContextValidityKeepsItsResolvedAS(t *testing.T) {
	key := ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true}
	destination := MTPDestination{MTPRoute: "one", PointCode: 100}
	e, err := NewEndpoint(EndpointConfig{Role: RoleIPSP, Congestion: &CongestionConfig{Default: CongestionProfile{Method: CongestionNationalWithPriorities, MaxLevel: 3}}, IPSPDestinations: []IPSPDestinationConfig{{Destination: destination, AS: key}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	now := time.Now()
	e.ssnm.validity = time.Second
	e.ssnm.now = func() time.Time { return now }
	a, _ := newTestConnWithContexts(t, StateASPActive, RoleIPSP, 1)
	a.cfg.IPSP = &IPSPConfig{ExchangeModel: IPSPExchangeSingle}
	setInventoryNetworkAppearance(&a.cfg.ApplicationServers, params.NewNetworkAppearance(7))
	a.noteRoutingContextsActive([]uint32{1})
	a.noteRoutingContextsAcked(params.NewRoutingContext(1))
	if !e.trackAssociation(a) {
		t.Fatal("track")
	}
	t.Cleanup(func() { _ = a.Close() })
	if err := a.handleSignallingCongestion(messages.NewSignallingCongestion(params.NewNetworkAppearance(7), nil, params.NewAffectedPointCode(100), nil, params.NewCongestionIndications(3), nil)); err != nil {
		t.Fatal(err)
	}
	source := e.SSNMKnowledge().Partitions[0].Destinations[0].CongestionSources[0]
	if source.AS != key || source.Scope.RoutingContextSet {
		t.Fatalf("omitted RC did not preserve binding/raw scope: %+v", source)
	}
	if status, _ := e.MTPDestinationStatus(destination); !status.Congested {
		t.Fatal("omitted RC lost its resolved contribution")
	}
	if err := a.handleSignallingCongestion(messages.NewSignallingCongestion(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCode(100), nil, params.NewCongestionIndications(0), nil)); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	e.runCongestionAutomation(now)
	if status, _ := e.MTPDestinationStatus(destination); status.Congested || status.ReceivedCongestionUnknown {
		t.Fatalf("old omitted-RC timer shadowed fresh clear: %+v", status)
	}
}
