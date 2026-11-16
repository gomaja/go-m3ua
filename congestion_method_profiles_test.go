package m3ua

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestCongestionMethodConfigurationAndSelectorOwnership(t *testing.T) {
	for _, profile := range []CongestionProfile{{}, {Method: 99}, {Method: CongestionInternational, MaxLevel: 1}, {MaxLevel: 1}, {Method: CongestionNationalWithPriorities, MaxLevel: 4}} {
		if e, err := NewEndpoint(EndpointConfig{Role: RoleASP, Congestion: &CongestionConfig{Default: profile}}); err == nil {
			_ = e.Close()
			t.Fatalf("invalid profile accepted: %+v", profile)
		}
	}
	peer := SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}
	cfg := &CongestionConfig{Default: CongestionProfile{Method: CongestionInternational}, Networks: []NetworkCongestionProfile{
		{Profile: CongestionProfile{Method: CongestionNationalWithoutPriorities}},
		{NetworkAppearance: 0, NetworkAppearanceSet: true, Profile: CongestionProfile{Method: CongestionNationalWithPriorities, MaxLevel: 2}},
		{PeerSGP: &peer, NetworkAppearance: 0, NetworkAppearanceSet: true, Profile: CongestionProfile{Method: CongestionInternational}},
	}}
	e, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: validASPConfig(), Congestion: cfg})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = e.Close() }()
	cfg.Networks[0].Profile.Method = 99
	peer.SignallingGateway = "changed"
	key := ASKey{}
	if got := e.congestion.profile(nil, key); got.Method != CongestionNationalWithoutPriorities || got.MaxLevel != 3 {
		t.Fatalf("omitted selector/default normalization=%+v", got)
	}
	key.NetworkAppearanceSet = true
	if got := e.congestion.profile(nil, key); got.Method != CongestionNationalWithPriorities || got.MaxLevel != 2 {
		t.Fatalf("explicit zero=%+v", got)
	}
	originalPeer := SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}
	if got := e.congestion.profile(&originalPeer, key); got.Method != CongestionInternational {
		t.Fatalf("peer-specific=%+v", got)
	}
	associationCfg := snapshotAssociationConfig(&AssociationConfig{CongestionProfiles: []NetworkCongestionProfile{{NetworkAppearanceSet: true, Profile: CongestionProfile{Method: CongestionNationalWithoutPriorities, MaxLevel: 1}}}})
	c, _ := newTestConn(t, StateASPActive, RoleASP)
	c.endpoint = e
	c.cfg = associationCfg
	if got := c.congestionProfile(key); got.Method != CongestionNationalWithoutPriorities || got.MaxLevel != 1 {
		t.Fatalf("association override=%+v", got)
	}
	for _, entries := range [][]NetworkCongestionProfile{
		{{}, {}}, {{NetworkAppearance: 1}}, {{PeerSGP: &SGPIdentity{SignallingGateway: "unknown"}}},
	} {
		for i := range entries {
			entries[i].Profile.Method = CongestionInternational
		}
		if _, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: validASPConfig(), Congestion: &CongestionConfig{Default: CongestionProfile{Method: CongestionInternational}, Networks: entries}}); err == nil {
			t.Fatalf("invalid selectors accepted: %+v", entries)
		}
	}
	if err := validateCongestionProfiles([]NetworkCongestionProfile{{PeerSGP: &originalPeer, Profile: CongestionProfile{Method: CongestionInternational}}}, RoleASP, e.aspRoutes, true); err == nil {
		t.Fatal("association accepted peer-qualified override")
	}
}

func TestCongestionMethodRejectsIncompatibleRouteAlternatives(t *testing.T) {
	_, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: validASPConfig(), Congestion: &CongestionConfig{Default: CongestionProfile{Method: CongestionInternational}, Networks: []NetworkCongestionProfile{{NetworkAppearance: 7, NetworkAppearanceSet: true, Profile: CongestionProfile{Method: CongestionNationalWithPriorities}}}}})
	if !errors.Is(err, ErrCongestionMethodMismatch) {
		t.Fatalf("incompatible route alternatives: %v", err)
	}
}

func TestCongestionMethodFiltersMembersBeforeChoosingAndPolicyCannotBypass(t *testing.T) {
	for _, mode := range []RouteSelectionMode{RouteSelectionPrimaryBackup, RouteSelectionLoadshare, RouteSelectionBroadcast} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			config := oneSignallingGatewayTwoSGPConfig(mode)
			config.Routing.CongestionPolicy = func(uint8, uint8, bool) bool { return true }
			e, associations, captures := newASPTransferFixtureReporting(t, config, true, &CongestionConfig{Default: CongestionProfile{Method: CongestionNationalWithPriorities, MaxLevel: 2}})
			a := associations["sg-a/sgp-a1"]
			a.transportSnapshot.Store(&TransportCongestionState{Known: true, Level: 3, Epoch: a.Epoch()})
			e.aspRoutes.transportChanged(a)
			request := MTPTransferRequest{ProtocolData: transferProtocolData(0x123456, 2, []byte("priority"))}
			request.ProtocolData.MessagePriority = 1
			_, err := e.MTPTransfer(request)
			if mode == RouteSelectionBroadcast {
				// Required broadcast copies must be reported as failures, not silently removed.
				if err == nil {
					t.Fatal("congested required recipient silently omitted")
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if captures["sg-a/sgp-a1"].count() != 0 {
				t.Fatal("congested member chosen")
			}
			if mode != RouteSelectionBroadcast && captures["sg-a/sgp-a2"].count() != 1 {
				t.Fatal("healthy alternate not chosen")
			}
		})
	}
}

func TestCongestionMethodQueueOnsetAndDrainUseOnlyQueueContribution(t *testing.T) {
	for _, method := range []uint8{1, 2, 3} {
		t.Run(fmt.Sprint(method), func(t *testing.T) {
			c, sent := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
			c.noteRoutingContextsAcked(params.NewRoutingContext(1))
			max := uint8(0)
			if method >= 2 {
				max = 2
			}
			c.endpoint = methodEndpointForTest(t, RoleASP, method, max)
			attachOverflowEndpointForTest(t, c)
			c.dataOverflowWorker = true
			if c.recordDataOverflow(&DataMessage{AS: ASKey{RoutingContext: 1, RoutingContextSet: true}, Scope: WireScope{RoutingContexts: []uint32{1}, RoutingContextSet: true}, ProtocolData: transferProtocolData(100, 1, nil)}) == nil {
				t.Fatal("missing queue onset")
			}
			runOverflowPublicationsForTest(c)
			if len(*sent) != 1 {
				t.Fatalf("onset reports=%d, want one", len(*sent))
			}
			onset := (*sent)[0].(*messages.SignallingCongestion)
			if method >= 2 {
				if onset.CongestionIndications == nil || onset.CongestionIndications.CongestionLevel() != 2 {
					t.Fatal("national onset did not use MaxLevel")
				}
			} else if onset.CongestionIndications != nil {
				t.Fatal("legacy/international onset gained numeric level")
			}
			c.muDataOverflow.Lock()
			c.dataOverflowLowSince = time.Now().Add(-2 * time.Second)
			c.muDataOverflow.Unlock()
			c.observeDataQueue()
			runOverflowPublicationsForTest(c)
			if method >= 2 {
				if len(*sent) != 2 {
					t.Fatalf("national drain omitted clear: %d reports", len(*sent))
				}
				clear := (*sent)[1].(*messages.SignallingCongestion)
				if clear.CongestionIndications == nil || clear.CongestionIndications.CongestionLevel() != 0 || clear.ConcernedDestination != nil {
					t.Fatalf("bad national clear: %+v", clear)
				}
			} else if len(*sent) != 1 {
				t.Fatal("legacy/international drain invented clear")
			}
		})
	}
}

func TestCongestionMethodProjectsTransportWithoutChangingRawSeverity(t *testing.T) {
	for _, profile := range []CongestionProfile{{Method: CongestionInternational}, {Method: CongestionNationalWithPriorities, MaxLevel: 1}, {Method: CongestionNationalWithoutPriorities, MaxLevel: 2}} {
		t.Run(fmt.Sprint(profile), func(t *testing.T) {
			config := oneSignallingGatewayTwoSGPConfig(RouteSelectionLoadshare)
			e, associations, _ := newASPTransferFixtureReporting(t, config, true, &CongestionConfig{Default: profile})
			for _, a := range associations {
				setAssociationTransport(t, e, a, 3)
			}
			status, ok := e.MTPDestinationStatus(MTPDestination{MTPRoute: "sccp-a", PointCode: 0x123456})
			if !ok || !status.Congested || status.TransportCongestionLevel != 3 {
				t.Fatalf("status=%+v, known=%v", status, ok)
			}
			if profile.Method == CongestionInternational {
				if status.CongestionLevelSet {
					t.Fatalf("international synthesized level: %+v", status)
				}
			} else if !status.CongestionLevelSet || status.CongestionLevel != profile.MaxLevel {
				t.Fatalf("national effective level=%+v", status)
			}
			for _, a := range associations {
				if a.TransportCongestion().Level != 3 {
					t.Fatal("profile rewrote raw detector")
				}
			}
		})
	}
}

func TestCongestionMethodRawGateUsesCanonicalSiblingCongestion(t *testing.T) {
	config := oneSignallingGatewayTwoSGPConfig(RouteSelectionLoadshare)
	_, associations, captures := newASPTransferFixtureReporting(t, config, true, &CongestionConfig{Default: CongestionProfile{Method: CongestionNationalWithPriorities, MaxLevel: 2}})
	applyASPSCON(t, associations["sg-a/sgp-a1"], 7, 1, 0x123456, 0, params.NewCongestionIndications(2))
	a := associations["sg-a/sgp-a2"]
	payload := transferProtocolData(0x123456, 1, []byte("shared"))
	payload.MessagePriority = 1
	_, err := a.WriteData(DataRequest{AS: writeScope(1), ProtocolData: *payload})
	requireDataWriteError(t, err, DataNotSent, ErrNoMTPRoute)
	if captures["sg-a/sgp-a2"].count() != 0 {
		t.Fatal("raw send bypassed canonical sibling knowledge")
	}
}

func TestCongestionMethodDiagnosticCopiesAndQueueByteBudget(t *testing.T) {
	event := SSNMEvent{Report: SSNMReport{ProfileMismatches: []CongestionProfileMismatch{{AS: writeScope(1), Profile: CongestionProfile{Method: CongestionInternational}}}}}
	copied := event.clone()
	copied.Report.ProfileMismatches[0].Profile.Method = 99
	if event.Report.ProfileMismatches[0].Profile.Method != CongestionInternational {
		t.Fatal("subscriber mismatch aliases another subscriber")
	}
	if _, fits := ssnmEventAccountedBytes(event, 512+31); fits {
		t.Fatal("queue byte budget ignored mismatch payload")
	}
	if bytes, fits := ssnmEventAccountedBytes(event, 512+32); !fits || bytes != 544 {
		t.Fatalf("mismatch accounted bytes=%d fits=%v", bytes, fits)
	}
}

func TestCongestionMethodBindingMismatchDoesNotWithdrawOrReactivate(t *testing.T) {
	config := oneSignallingGatewayTwoSGPConfig(RouteSelectionLoadshare)
	e, associations, _ := newASPTransferFixtureReporting(t, config, true, &CongestionConfig{Default: CongestionProfile{Method: CongestionInternational}})
	for _, a := range associations {
		a.cfg.CongestionProfiles = []NetworkCongestionProfile{{NetworkAppearance: 7, NetworkAppearanceSet: true, Profile: CongestionProfile{Method: CongestionNationalWithPriorities, MaxLevel: 2}}}
	}
	_, err := e.MTPTransfer(MTPTransferRequest{ProtocolData: transferProtocolData(0x123456, 1, nil)})
	if !errors.Is(err, ErrCongestionMethodMismatch) {
		t.Fatalf("binding mismatch=%v", err)
	}
	for _, a := range associations {
		if a.State() != StateASPActive || !a.routingContextAcked(1) {
			t.Fatal("profile mismatch changed activation")
		}
	}
}

func TestCongestionMethodInternationalQueueOmitsNumericOnsetAndClear(t *testing.T) {
	c, sent := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
	c.noteRoutingContextsAcked(params.NewRoutingContext(1))
	c.endpoint = methodEndpointForTest(t, RoleASP, uint8(CongestionInternational), 0)
	attachOverflowEndpointForTest(t, c)
	c.dataOverflowWorker = true
	if c.recordDataOverflow(&DataMessage{AS: ASKey{RoutingContext: 1, RoutingContextSet: true}, Scope: testWireScope(0, false, 1), ProtocolData: transferProtocolData(100, 1, nil)}) == nil {
		t.Fatal("missing queue onset")
	}
	runOverflowPublicationsForTest(c)
	c.muDataOverflow.Lock()
	c.dataOverflowLowSince = time.Now().Add(-2 * time.Second)
	c.muDataOverflow.Unlock()
	c.observeDataQueue()
	runOverflowPublicationsForTest(c)
	if len(*sent) != 1 || (*sent)[0].(*messages.SignallingCongestion).CongestionIndications != nil {
		t.Fatalf("International queue format changed: %+v", *sent)
	}
}

func TestCongestionMethodEndpointSCONRefusesBeforeRetainingInvalidLocalReport(t *testing.T) {
	for _, profile := range []CongestionProfile{{Method: CongestionInternational}, {Method: CongestionNationalWithPriorities, MaxLevel: 1}, {Method: CongestionNationalWithoutPriorities, MaxLevel: 2}} {
		e, err := NewEndpoint(EndpointConfig{Role: RoleSGP, Congestion: &CongestionConfig{Default: profile}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = e.Close() })
		request := SignallingCongestionRequest{Scope: testWireScope(7, true, 1), Destinations: []PointCodeRange{{PointCode: 100}}, CongestionLevel: 3, CongestionLevelSet: true}
		if err := e.SignallingCongestion(request); !errors.Is(err, ErrInvalidParameterValue) {
			t.Errorf("invalid local SCON=%v", err)
		}
		if state, known := e.destinations.lookupRange(destinationKey{networkAppearance: 7, networkAppearanceSet: true, routingContext: 1, routingContextSet: true}, 100, 0); known {
			t.Fatalf("invalid local report retained: %+v", state)
		}
	}
}

func TestCongestionMethodWithoutPrioritiesKeepsCongestedLocalOffers(t *testing.T) {
	for _, method := range []CongestionMethod{CongestionInternational, CongestionNationalWithoutPriorities} {
		config := oneSignallingGatewayTwoSGPConfig(RouteSelectionLoadshare)
		e, associations, captures := newASPTransferFixtureReporting(t, config, true, &CongestionConfig{Default: CongestionProfile{Method: method}})
		for _, a := range associations {
			applyASPSCON(t, a, 7, 1, 0x123456, 0, nil)
			setAssociationTransport(t, e, a, 3)
		}
		if _, err := e.MTPTransfer(MTPTransferRequest{ProtocolData: transferProtocolData(0x123456, 1, []byte("continue"))}); err != nil {
			t.Fatal(err)
		}
		if captures["sg-a/sgp-a1"].count()+captures["sg-a/sgp-a2"].count() != 1 {
			t.Fatal("no-priority method discarded local offer")
		}
	}
}

func TestCongestionMethodResolutionUsesBindingForOmittedWireAppearance(t *testing.T) {
	c, capture := newDataWriteAssociation(t, 1)
	c.signalWriter = nil
	e, err := NewEndpoint(EndpointConfig{Role: RoleASP, Congestion: &CongestionConfig{Default: CongestionProfile{Method: CongestionInternational}, Networks: []NetworkCongestionProfile{{NetworkAppearance: 7, NetworkAppearanceSet: true, Profile: CongestionProfile{Method: CongestionNationalWithPriorities, MaxLevel: 2}}}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	c.endpoint = nil
	if !e.trackAssociation(c) {
		t.Fatal("track")
	}
	observeSSNM(t, c)
	payload := transferProtocolData(100, 1, []byte("binding"))
	payload.MessagePriority = 2
	data := messages.NewData(nil, params.NewRoutingContext(1), params.NewProtocolData(payload.OriginatingPointCode, payload.DestinationPointCode, payload.ServiceIndicator, payload.NetworkIndicator, payload.MessagePriority, payload.SignallingLinkSelection, payload.Data), nil)
	if _, err := c.WriteSignal(data); err != nil {
		t.Fatal(err)
	}
	if capture.submissions() != 1 {
		t.Fatal("omitted NA used the wrong profile")
	}
	message := messages.NewSignallingCongestion(nil, params.NewRoutingContext(1), params.NewAffectedPointCode(100), nil, params.NewCongestionIndications(3), nil)
	if err := c.handleSignallingCongestion(message); err != nil {
		t.Fatal(err)
	}
	report := nextSSNMReport(t, c)
	if report.Scope.NetworkAppearanceSet || len(report.ProfileMismatches) != 1 || report.ProfileMismatches[0].AS.NetworkAppearance != 7 || !report.ProfileMismatches[0].AS.NetworkAppearanceSet || report.ProfileMismatches[0].Profile.Method != CongestionNationalWithPriorities {
		t.Fatalf("wire and resolved scopes confused: %+v", report)
	}
}

func TestCongestionMethodUnsubmittedEpisodesCannotSendClear(t *testing.T) {
	c, sent := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
	c.noteRoutingContextsAcked(params.NewRoutingContext(1))
	c.endpoint = methodEndpointForTest(t, RoleASP, 2, 2)
	attachOverflowEndpointForTest(t, c)
	c.dataOverflowWorker = true
	msg := &DataMessage{AS: routingContextASKey(1), Scope: testWireScope(0, false, 1), ProtocolData: transferProtocolData(100, 1, nil)}
	for range 2 {
		if c.recordDataOverflow(msg) == nil {
			t.Fatal("missing new episode")
		}
		if len(c.endpoint.CongestionPublications()) != 1 {
			t.Fatal("onset not retained before drain")
		}
		c.muDataOverflow.Lock()
		c.dataOverflowLowSince = time.Now().Add(-2 * time.Second)
		c.muDataOverflow.Unlock()
		c.observeDataQueue()
		runOverflowPublicationsForTest(c)
		if len(c.endpoint.CongestionPublications()) != 0 {
			t.Fatal("unsubmitted episode retained publication")
		}
	}
	if len(*sent) != 0 {
		t.Fatal("unsubmitted episode manufactured onset or numeric clear")
	}
}

func TestCongestionMethodQueueDrainLeavesReceivedAndTransportContributions(t *testing.T) {
	c, _ := newDataWriteAssociation(t, 1)
	c.endpoint = methodEndpointForTest(t, RoleASP, 2, 2)
	attachOverflowEndpointForTest(t, c)
	applyASPSCON(t, c, 7, 1, 100, 0, params.NewCongestionIndications(2))
	c.transportSnapshot.Store(&TransportCongestionState{Known: true, Level: 3, Epoch: c.Epoch()})
	c.dataOverflowWorker = true
	if c.recordDataOverflow(&DataMessage{AS: writeScope(1), Scope: testWireScope(7, true, 1), ProtocolData: transferProtocolData(100, 1, nil)}) == nil {
		t.Fatal("missing queue onset")
	}
	runOverflowPublicationsForTest(c)
	c.muDataOverflow.Lock()
	c.dataOverflowLowSince = time.Now().Add(-2 * time.Second)
	c.muDataOverflow.Unlock()
	c.observeDataQueue()
	runOverflowPublicationsForTest(c)
	state, known := c.destinations.lookupRange(destinationKey{networkAppearance: 7, networkAppearanceSet: true, routingContext: 1, routingContextSet: true}, 100, 0)
	if !known || state.Congestion.Level != 2 || !state.Congestion.Congested || c.TransportCongestion().Level != 3 {
		t.Fatal("queue drain cleared another contribution")
	}
	payload := transferProtocolData(100, 1, nil)
	payload.MessagePriority = 1
	_, err := c.WriteData(DataRequest{AS: writeScope(1), ProtocolData: *payload})
	requireDataWriteError(t, err, DataNotSent, ErrNoMTPRoute)
}
