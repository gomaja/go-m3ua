package m3ua

import (
	"fmt"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
)

func TestCongestionMethodDistributedOmittedAppearanceUsesResolvedNetwork(t *testing.T) {
	for _, strict := range []bool{false, true} {
		t.Run(fmt.Sprint(strict), func(t *testing.T) {
			profile, defaultProfile := CongestionProfile{Method: CongestionNationalWithPriorities, MaxLevel: 2}, CongestionProfile{Method: CongestionInternational}
			if strict {
				profile, defaultProfile = CongestionProfile{Method: CongestionInternational}, CongestionProfile{Method: CongestionNationalWithPriorities, MaxLevel: 2}
			}
			listener, as, a, capture := distributionFixtureConfigured(t, params.TrafficModeLoadshare, func(config *distributionFixtureConfig) {
				setInventoryNetworkAppearance(&config.ApplicationServers, params.NewNetworkAppearance(7))
				config.Congestion = &CongestionConfig{Default: defaultProfile, Networks: []NetworkCongestionProfile{{NetworkAppearance: 7, NetworkAppearanceSet: true, Profile: profile}}}
			})
			a.endpoint = listener.endpoint
			a.setState(StateASPActive)
			a.noteRoutingContextsActive([]uint32{1})
			as.setASPState(a, StateASPActive, time.Hour)
			message := distributionData(1, 1, "resolved")
			message.NetworkAppearance = nil
			message.ProtocolData = params.NewProtocolData(1, 100, 3, 0, 2, 1, []byte("resolved"))
			message.SetLength()
			_, err := listener.DistributeData(message)
			if err != nil {
				t.Fatal(err)
			}
			if capture.dataCount() != 1 {
				t.Fatal("SGP omitted accepted NIF transit")
			}
			sent := capture.data()[0]
			payload, err := sent.ProtocolData.ProtocolData()
			if err != nil || payload.MessagePriority != 2 || sent.NetworkAppearance != nil {
				t.Fatalf("resolved appearance changed original transit: %+v, %v", sent, err)
			}
			want := uint64(0)
			if strict {
				want = 1
			}
			if got := a.CongestionProfileDiagnostics().ReceivedDataMismatches; got != want {
				t.Fatalf("resolved-network diagnostic count=%d want=%d", got, want)
			}
		})
	}
}

func TestCongestionMethodAbsentNAIgnoresUnusedValueForProfileAndGate(t *testing.T) {
	c, capture := newDataWriteAssociation(t, 1)
	setInventoryNetworkAppearance(&c.cfg.ApplicationServers, nil)
	e, err := NewEndpoint(EndpointConfig{Role: RoleASP, Congestion: &CongestionConfig{Default: CongestionProfile{Method: CongestionNationalWithPriorities, MaxLevel: 2}, Networks: []NetworkCongestionProfile{{Profile: CongestionProfile{Method: CongestionInternational}}}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	c.endpoint = e
	payload := testProtocolData([]byte("absent"))
	payload.MessagePriority = 2
	key := ASKey{NetworkAppearance: 7, RoutingContext: 1, RoutingContextSet: true}
	_, err = c.WriteData(DataRequest{AS: key, ProtocolData: payload})
	requireDataWriteError(t, err, DataNotSent, ErrInvalidParameterValue)
	if capture.submissions() != 0 {
		t.Fatal("unused absent NA value bypassed profile")
	}
	// The same normalization must apply when looking up received congestion.
	c.endpoint = methodEndpointForTest(t, RoleASP, 2, 2)
	if err := c.destinations.setCongestionRangesWithinBudget([]destinationRange{{RoutingContext: 1, RoutingContextSet: true, PointCode: payload.DestinationPointCode, State: DestinationNetworkState{Congestion: CongestionState{Congested: true, Level: 2, LevelSet: true}}}}); err != nil {
		t.Fatal(err)
	}
	payload.MessagePriority = 1
	_, err = c.WriteData(DataRequest{AS: key, ProtocolData: payload})
	requireDataWriteError(t, err, DataNotSent, ErrNoMTPRoute)
}

func TestCongestionMethodPreferenceDoesNotTurnNoObservationIntoRefusal(t *testing.T) {
	config := oneSignallingGatewayTwoSGPConfig(RouteSelectionLoadshare)
	config.Routing.PreferUncongestedTransport = true
	e, associations, captures := newASPTransferFixtureReporting(t, config, true, &CongestionConfig{Default: CongestionProfile{Method: CongestionNationalWithPriorities, MaxLevel: 2}})
	for _, a := range associations {
		if a.transportSnapshot.Load() != nil {
			t.Fatal("fixture unexpectedly sampled")
		}
	}
	payload := transferProtocolData(0x123456, 1, []byte("absent transport"))
	payload.MessagePriority = 1
	if _, err := e.MTPTransfer(MTPTransferRequest{ProtocolData: payload}); err != nil {
		t.Fatal(err)
	}
	if captures["sg-a/sgp-a1"].count()+captures["sg-a/sgp-a2"].count() != 1 {
		t.Fatal("no-observation candidates refused")
	}
}

func TestCongestionMethodBroadcastTriesLaterApplicationServer(t *testing.T) {
	config := twoApplicationServerASPConfig()
	config.Routing.SignallingGatewaySelection = RouteSelectionBroadcast
	e, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: config, Congestion: &CongestionConfig{Default: CongestionProfile{Method: CongestionNationalWithPriorities, MaxLevel: 2}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	a, capture := attachMultiScopeASPAssociation(t, e, SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}, 7, 1, 2)
	for _, rc := range []uint32{1, 2} {
		applyASPDAVA(t, a, 7, rc, 0x123456, 0)
	}
	applyASPSCON(t, a, 7, 1, 0x123456, 0, params.NewCongestionIndications(2))
	payload := transferProtocolData(0x123456, 1, []byte("later AS"))
	payload.MessagePriority = 1
	result, err := e.MTPTransfer(MTPTransferRequest{ProtocolData: payload})
	if err != nil {
		t.Fatal(err)
	}
	if capture.count() != 1 || len(result.SuccessfulPaths) != 1 || result.SuccessfulPaths[0].ApplicationServer != "as-backup" {
		t.Fatalf("did not try later AS: %+v", result)
	}
}

func TestCongestionMethodBroadcastTriesHealthySGPBeforeFailingObligation(t *testing.T) {
	for _, mode := range []RouteSelectionMode{RouteSelectionPrimaryBackup, RouteSelectionLoadshare} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			config := oneSignallingGatewayTwoSGPConfig(mode)
			config.Routing.SignallingGatewaySelection = RouteSelectionBroadcast
			e, associations, captures := newASPTransferFixtureReporting(t, config, true, &CongestionConfig{Default: CongestionProfile{Method: CongestionNationalWithPriorities, MaxLevel: 2}})
			a := associations["sg-a/sgp-a1"]
			setAssociationTransport(t, e, a, 3)
			for sls := uint8(0); sls < 16; sls++ {
				payload := transferProtocolData(0x123456, sls, []byte("healthy SGP"))
				payload.MessagePriority = 1
				if _, err := e.MTPTransfer(MTPTransferRequest{ProtocolData: payload}); err != nil {
					t.Fatalf("sls=%d did not try healthy sibling: %v", sls, err)
				}
			}
			if captures["sg-a/sgp-a1"].count() != 0 || captures["sg-a/sgp-a2"].count() != 16 {
				t.Fatal("wrong required-copy recipient")
			}
		})
	}
}

func TestCongestionMethodExistingManagementKindNumbersRemainStable(t *testing.T) {
	kinds := []ManagementIndicationKind{ManagementNotify, ManagementError, ManagementSCTPRestart, ManagementSCTPRelease, ManagementSCTPStatus, ManagementDataQueueCongestion, ManagementTransportCongestion, ManagementASPWithdrawal}
	for expected, kind := range kinds {
		if uint8(kind) != uint8(expected) {
			t.Fatalf("existing management kind %s=%d want %d", kind, kind, expected)
		}
	}
}

func TestCongestionMethodPrefersCompleteGatewayBeforeFailedBroadcastGroup(t *testing.T) {
	config := validASPConfig()
	config.Routing.SignallingGatewaySelection = RouteSelectionPrimaryBackup
	second := config.SignallingGateways[0].SGPs[0]
	second.ID = "sgp-a2"
	config.SignallingGateways[0].SGPs = append(config.SignallingGateways[0].SGPs, second)
	setSGPSelection(config, RouteSelectionBroadcast, "sg-a")
	e, associations, captures := newASPTransferFixtureReporting(t, config, true, &CongestionConfig{Default: CongestionProfile{Method: CongestionNationalWithPriorities, MaxLevel: 2}})
	setAssociationTransport(t, e, associations["sg-a/sgp-a1"], 3)
	payload := transferProtocolData(0x123456, 1, []byte("complete gateway"))
	payload.MessagePriority = 1
	result, err := e.MTPTransfer(MTPTransferRequest{ProtocolData: payload})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.SuccessfulPaths) != 1 || result.SuccessfulPaths[0].SGP.SignallingGateway != "sg-b" || captures["sg-b/sgp-b1"].count() != 1 || captures["sg-a/sgp-a2"].count() != 0 {
		t.Fatalf("partial group chosen before complete alternate: %+v", result)
	}
}

func TestCongestionMethodDiagnosticOwnersCannotChangeRetainedSample(t *testing.T) {
	c, _ := newDataWriteAssociation(t, 1)
	c.endpoint = methodEndpointForTest(t, RoleASP, 1, 0)
	if !c.endpoint.trackAssociation(c) {
		t.Fatal("track")
	}
	management := c.ManagementIndications()
	payload := testProtocolData([]byte("owned"))
	payload.MessagePriority = 255
	frame := c.encodeDataFrame(&DataRequest{AS: writeScope(1), ProtocolData: payload})
	c.dispatchRaw(t.Context(), inbound{data: frame, stream: 1, ppid: M3UAPPID})
	delivered := <-c.dataChan
	delivered.Scope.RoutingContexts[0] = 999
	if c.CongestionProfileDiagnostics().LastDataMismatch.Scope.RoutingContexts[0] != 1 {
		t.Fatal("DATA consumer changed diagnostic scope")
	}
	indication := <-management
	indication.CongestionProfileMismatch.Scope.RoutingContexts[0] = 888
	if c.CongestionProfileDiagnostics().LastDataMismatch.Scope.RoutingContexts[0] != 1 {
		t.Fatal("management consumer changed diagnostic scope")
	}
}

func TestCongestionMethodDiagnosticSnapshotWhileReceiving(t *testing.T) {
	c, _ := newDataWriteAssociation(t, 1)
	c.endpoint = methodEndpointForTest(t, RoleASP, 1, 0)
	if !c.endpoint.trackAssociation(c) {
		t.Fatal("track")
	}
	payload := testProtocolData([]byte("concurrent"))
	payload.MessagePriority = 255
	frame := c.encodeDataFrame(&DataRequest{AS: writeScope(1), ProtocolData: payload})
	stop, done := make(chan struct{}), make(chan struct{})
	bad := make(chan string, 1)
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			snapshot := c.CongestionProfileDiagnostics()
			if snapshot.LastDataMismatch != nil {
				if snapshot.LastDataMismatch.Scope.RoutingContexts[0] != 1 {
					select {
					case bad <- "snapshot alias":
					default:
					}
					return
				}
				snapshot.LastDataMismatch.Scope.RoutingContexts[0] = 999
			}
		}
	}()
	defer func() { close(stop); <-done }()
	for range 1000 {
		c.dispatchRaw(t.Context(), inbound{data: frame, stream: 1, ppid: M3UAPPID})
		<-c.stateChan
		select {
		case <-c.dataChan:
		default:
			t.Fatal("DATA lost")
		}
	}
	if got := c.CongestionProfileDiagnostics().ReceivedDataMismatches; got != 1000 {
		t.Fatalf("mismatch count=%d", got)
	}
	select {
	case err := <-bad:
		t.Fatal(err)
	default:
	}
}
