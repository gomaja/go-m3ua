// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

func TestMTPTransferSelectsAvailableSGPAndSLSStream(t *testing.T) {
	config := validASPConfig()
	config.SignallingGatewaySelection = RouteSelectionPrimaryBackup
	endpoint, associations, captures := newASPTransferFixture(t, config)
	const pointCode = uint32(0x123456)
	applyASPDUNA(t, associations["sg-a/sgp-a1"], 7, 1, pointCode, 0)

	result, err := endpoint.MTPTransfer(MTPTransferRequest{
		ProtocolData: transferProtocolData(pointCode, 5, []byte("payload")),
	})
	if err != nil {
		t.Fatalf("MTPTransfer: %v", err)
	}
	if result.UserDataOctets != len("payload") || result.TransmittedAssociations != 1 {
		t.Fatalf("MTPTransfer result = %#v", result)
	}
	if captures["sg-a/sgp-a1"].count() != 0 || captures["sg-b/sgp-b1"].count() != 1 {
		t.Fatalf("writes = sg-a:%d sg-b:%d, want 0 and 1",
			captures["sg-a/sgp-a1"].count(), captures["sg-b/sgp-b1"].count())
	}
	data, stream := captures["sg-b/sgp-b1"].lastData(t)
	if stream != associations["sg-b/sgp-b1"].streamFor(5) {
		t.Fatalf("DATA stream = %d, want SLS-derived %d", stream, associations["sg-b/sgp-b1"].streamFor(5))
	}
	if data.NetworkAppearance == nil || data.NetworkAppearance.NetworkAppearance() != 9 ||
		data.RoutingContext == nil || len(data.RoutingContext.RoutingContexts()) != 1 ||
		data.RoutingContext.RoutingContexts()[0] != 42 {
		t.Fatalf("selected SGP DATA scope = NA %#v RC %#v", data.NetworkAppearance, data.RoutingContext)
	}
}

func TestMTPTransferValidatesEndpointAndRequest(t *testing.T) {
	var nilEndpoint *Endpoint
	if _, err := nilEndpoint.MTPTransfer(MTPTransferRequest{}); !errors.Is(err, ErrUnsupportedRole) {
		t.Fatalf("nil Endpoint error = %v, want ErrUnsupportedRole", err)
	}
	sgpEndpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatalf("NewEndpoint SGP: %v", err)
	}
	t.Cleanup(func() { _ = sgpEndpoint.Close() })
	if _, err := sgpEndpoint.MTPTransfer(MTPTransferRequest{}); !errors.Is(err, ErrUnsupportedRole) {
		t.Fatalf("SGP Endpoint error = %v, want ErrUnsupportedRole", err)
	}

	endpoint, _, _ := newASPTransferFixture(t, validASPConfig())
	if _, err := endpoint.MTPTransfer(MTPTransferRequest{}); !errors.Is(err, ErrMissingProtocolData) {
		t.Fatalf("missing Protocol Data error = %v, want ErrMissingProtocolData", err)
	}
	for _, protocolData := range []*params.ProtocolDataPayload{
		transferProtocolData(0x1000000, 1, nil),
		params.NewProtocolDataPayload(0x1000000, 0x123456, params.ServiceIndSCCP, 0, 0, 1, nil),
	} {
		if _, err := endpoint.MTPTransfer(MTPTransferRequest{ProtocolData: protocolData}); !errors.Is(err, ErrInvalidMTPTransfer) {
			t.Fatalf("invalid point code error = %v, want ErrInvalidMTPTransfer", err)
		}
	}
	if err := endpoint.Close(); err != nil {
		t.Fatalf("Endpoint.Close: %v", err)
	}
	if _, err := endpoint.MTPTransfer(MTPTransferRequest{
		ProtocolData: transferProtocolData(0x123456, 1, nil),
	}); !errors.Is(err, ErrEndpointClosed) {
		t.Fatalf("closed Endpoint error = %v, want ErrEndpointClosed", err)
	}
}

func TestMTPTransferPrefersAvailableAndLeastCongestedRoutes(t *testing.T) {
	config := validASPConfig()
	config.SignallingGatewaySelection = RouteSelectionPrimaryBackup
	endpoint, associations, captures := newASPTransferFixture(t, config)
	const pointCode = uint32(0x123456)

	applyASPDRST(t, associations["sg-a/sgp-a1"], 7, 1, pointCode, 0)
	if _, err := endpoint.MTPTransfer(MTPTransferRequest{ProtocolData: transferProtocolData(pointCode, 1, nil)}); err != nil {
		t.Fatalf("MTPTransfer with restricted primary: %v", err)
	}
	if captures["sg-b/sgp-b1"].count() != 1 {
		t.Fatal("available SG was not preferred over restricted primary SG")
	}

	applyASPDAVA(t, associations["sg-a/sgp-a1"], 7, 1, pointCode, 0)
	applyASPSCON(t, associations["sg-a/sgp-a1"], 7, 1, pointCode, 0, params.NewCongestionIndications(2))
	if _, err := endpoint.MTPTransfer(MTPTransferRequest{ProtocolData: transferProtocolData(pointCode, 2, nil)}); err != nil {
		t.Fatalf("MTPTransfer with congested primary: %v", err)
	}
	if captures["sg-b/sgp-b1"].count() != 2 {
		t.Fatal("uncongested SG was not preferred over congested primary SG")
	}
}

func TestMTPTransferExcludesInactiveApplicationServerScope(t *testing.T) {
	config := validASPConfig()
	config.SignallingGatewaySelection = RouteSelectionPrimaryBackup
	endpoint, associations, captures := newASPTransferFixture(t, config)
	associations["sg-a/sgp-a1"].noteRoutingContextsUnacked(params.NewRoutingContext(1))

	if _, err := endpoint.MTPTransfer(MTPTransferRequest{
		ProtocolData: transferProtocolData(0x123456, 3, nil),
	}); err != nil {
		t.Fatalf("MTPTransfer: %v", err)
	}
	if captures["sg-a/sgp-a1"].count() != 0 || captures["sg-b/sgp-b1"].count() != 1 {
		t.Fatal("MTPTransfer selected an inactive Application Server scope")
	}
}

func TestMTPTransferResolvesMTPRouteAndRejectsAmbiguity(t *testing.T) {
	config := validASPConfig()
	endpoint, _, _ := newASPTransferFixture(t, config)

	if _, err := endpoint.MTPTransfer(MTPTransferRequest{
		MTPRoute:     "unknown",
		ProtocolData: transferProtocolData(0x123456, 1, nil),
	}); !errors.Is(err, ErrUnknownMTPRoute) {
		t.Fatalf("unknown MTP Route error = %v, want ErrUnknownMTPRoute", err)
	}
	if _, err := endpoint.MTPTransfer(MTPTransferRequest{
		MTPRoute:     "sccp-a",
		ProtocolData: transferProtocolData(0x223456, 1, nil),
	}); !errors.Is(err, ErrMTPTransferOutsideRoute) {
		t.Fatalf("mismatched MTP Route error = %v, want ErrMTPTransferOutsideRoute", err)
	}

	ambiguous := validASPConfig()
	ambiguous.MTPRoutes = append(ambiguous.MTPRoutes, MTPRouteConfig{
		ID:                    "sccp-a-copy",
		DestinationPointCode:  0x120000,
		Mask:                  16,
		ServiceIndicators:     []uint8{params.ServiceIndSCCP},
		OriginatingPointCodes: []uint32{0x111111},
	})
	for gatewayIndex := range ambiguous.SignallingGateways {
		for sgpIndex := range ambiguous.SignallingGateways[gatewayIndex].SGPs {
			route := ambiguous.SignallingGateways[gatewayIndex].SGPs[sgpIndex].Routes[0]
			route.MTPRoute = "sccp-a-copy"
			ambiguous.SignallingGateways[gatewayIndex].SGPs[sgpIndex].Routes = append(
				ambiguous.SignallingGateways[gatewayIndex].SGPs[sgpIndex].Routes, route,
			)
		}
	}
	ambiguousEndpoint, _, _ := newASPTransferFixture(t, ambiguous)
	if _, err := ambiguousEndpoint.MTPTransfer(MTPTransferRequest{
		ProtocolData: transferProtocolData(0x123456, 1, nil),
	}); !errors.Is(err, ErrAmbiguousMTPRoute) {
		t.Fatalf("ambiguous MTP Route error = %v, want ErrAmbiguousMTPRoute", err)
	}
}

func TestMTPTransferPrimaryBackupWithinSignallingGateway(t *testing.T) {
	config := oneSignallingGatewayTwoSGPConfig(RouteSelectionPrimaryBackup)
	endpoint, associations, captures := newASPTransferFixture(t, config)
	request := MTPTransferRequest{ProtocolData: transferProtocolData(0x123456, 7, nil)}

	if _, err := endpoint.MTPTransfer(request); err != nil {
		t.Fatalf("primary MTPTransfer: %v", err)
	}
	if captures["sg-a/sgp-a1"].count() != 1 || captures["sg-a/sgp-a2"].count() != 0 {
		t.Fatal("primary/backup did not select the first configured SGP")
	}
	if err := associations["sg-a/sgp-a1"].Close(); err != nil {
		t.Fatalf("close primary SGP Association: %v", err)
	}
	if _, err := endpoint.MTPTransfer(request); err != nil {
		t.Fatalf("backup MTPTransfer: %v", err)
	}
	if captures["sg-a/sgp-a2"].count() != 1 {
		t.Fatal("primary/backup did not fail over to the backup SGP")
	}
}

func TestMTPTransferLoadshareKeepsEachFlowStable(t *testing.T) {
	config := oneSignallingGatewayTwoSGPConfig(RouteSelectionLoadshare)
	endpoint, _, captures := newASPTransferFixture(t, config)
	request := MTPTransferRequest{ProtocolData: transferProtocolData(0x123456, 9, []byte("same-flow"))}

	for range 10 {
		if _, err := endpoint.MTPTransfer(request); err != nil {
			t.Fatalf("MTPTransfer: %v", err)
		}
	}
	firstCount := captures["sg-a/sgp-a1"].count()
	secondCount := captures["sg-a/sgp-a2"].count()
	if firstCount+secondCount != 10 || firstCount > 0 && secondCount > 0 {
		t.Fatalf("one flow moved between SGPs: counts = %d and %d", firstCount, secondCount)
	}
}

func TestMTPTransferBroadcastAndPartialFailure(t *testing.T) {
	config := validASPConfig()
	config.SignallingGatewaySelection = RouteSelectionBroadcast
	endpoint, _, captures := newASPTransferFixture(t, config)
	captures["sg-b/sgp-b1"].writeErr = errors.New("write failed")

	result, err := endpoint.MTPTransfer(MTPTransferRequest{
		ProtocolData: transferProtocolData(0x123456, 4, []byte("broadcast")),
	})
	if result.TransmittedAssociations != 1 || result.UserDataOctets != len("broadcast") {
		t.Fatalf("partial broadcast result = %#v", result)
	}
	var transferErr *MTPTransferError
	if !errors.As(err, &transferErr) {
		t.Fatalf("partial broadcast error = %v, want *MTPTransferError", err)
	}
	if len(transferErr.SuccessfulSGPs) != 1 || transferErr.SuccessfulSGPs[0] != (SGPIdentity{
		SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1",
	}) || len(transferErr.Failures) != 1 || transferErr.Failures[0].SGP != (SGPIdentity{
		SignallingGateway: "sg-b", SignallingGatewayProcess: "sgp-b1",
	}) {
		t.Fatalf("partial broadcast error detail = %#v", transferErr)
	}
}

func TestMTPTransferReturnsNoRouteWhenEverySGPIsIneligible(t *testing.T) {
	endpoint, associations, _ := newASPTransferFixture(t, validASPConfig())
	associations["sg-a/sgp-a1"].noteRoutingContextsUnacked(params.NewRoutingContext(1))
	associations["sg-b/sgp-b1"].noteRoutingContextsUnacked(params.NewRoutingContext(42))

	if _, err := endpoint.MTPTransfer(MTPTransferRequest{
		ProtocolData: transferProtocolData(0x123456, 1, nil),
	}); !errors.Is(err, ErrNoMTPRoute) {
		t.Fatalf("MTPTransfer error = %v, want ErrNoMTPRoute", err)
	}
}

func TestMTPTransferReturnsNoRouteWhenEverySignallingGatewayIsUnavailable(t *testing.T) {
	endpoint, associations, _ := newASPTransferFixture(t, validASPConfig())
	applyASPDUNA(t, associations["sg-a/sgp-a1"], 7, 1, 0x123456, 0)
	applyASPDUNA(t, associations["sg-b/sgp-b1"], 9, 42, 0x123456, 0)
	if _, err := endpoint.MTPTransfer(MTPTransferRequest{
		ProtocolData: transferProtocolData(0x123456, 1, nil),
	}); !errors.Is(err, ErrNoMTPRoute) {
		t.Fatalf("all-unavailable transfer error = %v, want ErrNoMTPRoute", err)
	}
}

func TestMTPTransferBroadcastsAcrossSGPsWithinSignallingGateway(t *testing.T) {
	config := oneSignallingGatewayTwoSGPConfig(RouteSelectionBroadcast)
	endpoint, _, captures := newASPTransferFixture(t, config)
	result, err := endpoint.MTPTransfer(MTPTransferRequest{
		ProtocolData: transferProtocolData(0x123456, 6, nil),
	})
	if err != nil {
		t.Fatalf("MTPTransfer: %v", err)
	}
	if result.TransmittedAssociations != 2 || captures["sg-a/sgp-a1"].count() != 1 ||
		captures["sg-a/sgp-a2"].count() != 1 {
		t.Fatalf("SGP broadcast result = %#v, counts = %d and %d", result,
			captures["sg-a/sgp-a1"].count(), captures["sg-a/sgp-a2"].count())
	}
}

func TestMTPTransferLoadsharesAcrossSignallingGateways(t *testing.T) {
	config := validASPConfig()
	config.SignallingGatewaySelection = RouteSelectionLoadshare
	endpoint, _, captures := newASPTransferFixture(t, config)
	for sls := uint8(0); sls < 64; sls++ {
		if _, err := endpoint.MTPTransfer(MTPTransferRequest{
			ProtocolData: transferProtocolData(0x123456, sls, nil),
		}); err != nil {
			t.Fatalf("MTPTransfer SLS %d: %v", sls, err)
		}
	}
	if captures["sg-a/sgp-a1"].count() == 0 || captures["sg-b/sgp-b1"].count() == 0 {
		t.Fatalf("loadshare counts = sg-a:%d sg-b:%d, want both nonzero",
			captures["sg-a/sgp-a1"].count(), captures["sg-b/sgp-b1"].count())
	}
}

func TestMTPTransferSelectsMostSpecificMTPRoute(t *testing.T) {
	config := validASPConfig()
	config.MTPRoutes = append(config.MTPRoutes, MTPRouteConfig{
		ID:                    "sccp-specific",
		DestinationPointCode:  0x123400,
		Mask:                  8,
		ServiceIndicators:     []uint8{params.ServiceIndSCCP},
		OriginatingPointCodes: []uint32{0x111111},
	})
	config.SignallingGateways[1].SGPs[0].Routes = append(
		config.SignallingGateways[1].SGPs[0].Routes,
		SGPRoute{MTPRoute: "sccp-specific", AS: config.SignallingGateways[1].SGPs[0].Routes[0].AS},
	)
	endpoint, _, captures := newASPTransferFixture(t, config)
	if _, err := endpoint.MTPTransfer(MTPTransferRequest{
		ProtocolData: transferProtocolData(0x123456, 1, nil),
	}); err != nil {
		t.Fatalf("MTPTransfer: %v", err)
	}
	if captures["sg-a/sgp-a1"].count() != 0 || captures["sg-b/sgp-b1"].count() != 1 {
		t.Fatal("MTPTransfer did not select the most-specific MTP Route")
	}
}

func TestMTPTransferKeepsHealthyAssignmentWhenAssociationAppears(t *testing.T) {
	config := validASPConfig()
	config.SignallingGateways = config.SignallingGateways[:1]
	config.SignallingGateways[0].SGPSelection = RouteSelectionLoadshare
	endpoint, _, captures := newASPTransferFixture(t, config)
	identity := SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}
	var sls uint8
	for candidate := uint8(0); ; candidate++ {
		key := newASPTransferFlowKey("sccp-a", transferProtocolData(0x123456, candidate, nil))
		if hashASPTransferFlow(key, "sg-a/sgp-a1")%2 == 1 {
			sls = candidate
			break
		}
	}
	request := MTPTransferRequest{ProtocolData: transferProtocolData(0x123456, sls, nil)}
	if _, err := endpoint.MTPTransfer(request); err != nil {
		t.Fatalf("initial MTPTransfer: %v", err)
	}
	second := attachASPRouteAssociation(t, endpoint, identity, 7, 1)
	secondCapture := &mtpTransferCapture{}
	second.dataWriter = secondCapture.write
	if _, err := endpoint.MTPTransfer(request); err != nil {
		t.Fatalf("MTPTransfer after candidate arrival: %v", err)
	}
	if captures["sg-a/sgp-a1"].count() != 2 || secondCapture.count() != 0 {
		t.Fatalf("healthy assignment moved after candidate arrival: counts = %d and %d",
			captures["sg-a/sgp-a1"].count(), secondCapture.count())
	}
}

func TestMTPTransferAppliesCongestionPolicy(t *testing.T) {
	config := validASPConfig()
	config.CongestionPolicy = func(_ uint8, level uint8, levelSet bool) bool {
		return levelSet && level <= 1
	}
	endpoint, associations, captures := newASPTransferFixture(t, config)
	const pointCode = uint32(0x123456)
	applyASPSCON(t, associations["sg-a/sgp-a1"], 7, 1, pointCode, 0, params.NewCongestionIndications(2))
	applyASPSCON(t, associations["sg-b/sgp-b1"], 9, 42, pointCode, 0, params.NewCongestionIndications(3))

	request := MTPTransferRequest{ProtocolData: transferProtocolData(pointCode, 1, nil)}
	if _, err := endpoint.MTPTransfer(request); !errors.Is(err, ErrNoMTPRoute) {
		t.Fatalf("congestion-excluded transfer error = %v, want ErrNoMTPRoute", err)
	}
	applyASPSCON(t, associations["sg-b/sgp-b1"], 9, 42, pointCode, 0, params.NewCongestionIndications(1))
	if _, err := endpoint.MTPTransfer(request); err != nil {
		t.Fatalf("policy-permitted MTPTransfer: %v", err)
	}
	if captures["sg-a/sgp-a1"].count() != 0 || captures["sg-b/sgp-b1"].count() != 1 {
		t.Fatal("congestion policy did not select the only permitted SG route")
	}
}

func TestMTPTransferCallsCongestionPolicyOutsideRouteLock(t *testing.T) {
	config := validASPConfig()
	var endpoint *Endpoint
	called := false
	lockAvailable := false
	config.CongestionPolicy = func(_ uint8, _ uint8, _ bool) bool {
		called = true
		lockAvailable = endpoint.aspRoutes.mu.TryRLock()
		if lockAvailable {
			endpoint.aspRoutes.mu.RUnlock()
		}
		return true
	}
	createdEndpoint, associations, _ := newASPTransferFixture(t, config)
	endpoint = createdEndpoint
	applyASPSCON(t, associations["sg-a/sgp-a1"], 7, 1, 0x123456, 0, params.NewCongestionIndications(2))

	if _, err := endpoint.MTPTransfer(MTPTransferRequest{
		ProtocolData: transferProtocolData(0x123456, 1, nil),
	}); err != nil {
		t.Fatalf("MTPTransfer: %v", err)
	}
	if !called || !lockAvailable {
		t.Fatalf("congestion policy called = %v, route lock available = %v", called, lockAvailable)
	}
}

func TestMTPTransferReassignsFlowWhenSelectedRouteBecomesCongested(t *testing.T) {
	config := validASPConfig()
	config.SignallingGatewaySelection = RouteSelectionLoadshare
	endpoint, associations, captures := newASPTransferFixture(t, config)
	request := MTPTransferRequest{ProtocolData: transferProtocolData(0x123456, 12, nil)}
	if _, err := endpoint.MTPTransfer(request); err != nil {
		t.Fatalf("initial MTPTransfer: %v", err)
	}

	selected := "sg-a/sgp-a1"
	other := "sg-b/sgp-b1"
	selectedNetwork, selectedContext := uint32(7), uint32(1)
	if captures[selected].count() == 0 {
		selected, other = other, selected
		selectedNetwork, selectedContext = 9, 42
	}
	applyASPSCON(t, associations[selected], selectedNetwork, selectedContext, 0x123456, 0,
		params.NewCongestionIndications(2))
	if _, err := endpoint.MTPTransfer(request); err != nil {
		t.Fatalf("MTPTransfer after congestion: %v", err)
	}
	if captures[other].count() != 1 {
		t.Fatal("sticky flow remained on a newly congested route while an uncongested route existed")
	}
}

func TestMTPTransferSupportsContextlessApplicationServer(t *testing.T) {
	config := validASPConfig()
	config.SignallingGateways = config.SignallingGateways[:1]
	config.SignallingGateways[0].SGPs[0].Routes[0].AS = ASKey{
		NetworkAppearance: 7, NetworkAppearanceSet: true,
	}
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: config})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	identity := SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}
	association, _ := newTestConnWithContexts(t, StateASPActive, RoleASP)
	association.cfg.NetworkAppearance = params.NewNetworkAppearance(7)
	association.cfg.PeerSGP = &identity
	capture := &mtpTransferCapture{}
	association.dataWriter = capture.write
	if !endpoint.trackAssociation(association) {
		t.Fatal("failed to attach contextless ASP Association")
	}

	if _, err := endpoint.MTPTransfer(MTPTransferRequest{
		ProtocolData: transferProtocolData(0x123456, 1, nil),
	}); err != nil {
		t.Fatalf("contextless MTPTransfer: %v", err)
	}
	data, _ := capture.lastData(t)
	if data.NetworkAppearance == nil || data.NetworkAppearance.NetworkAppearance() != 7 || data.RoutingContext != nil {
		t.Fatalf("contextless DATA scope = NA %#v RC %#v", data.NetworkAppearance, data.RoutingContext)
	}
}

func TestMTPTransferKeepsSameSGPAssociationStableAndFailsOver(t *testing.T) {
	config := validASPConfig()
	config.SignallingGateways = config.SignallingGateways[:1]
	config.SignallingGateways[0].SGPSelection = RouteSelectionLoadshare
	endpoint, associations, captures := newASPTransferFixture(t, config)
	identity := SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}
	second := attachASPRouteAssociation(t, endpoint, identity, 7, 1)
	secondCapture := &mtpTransferCapture{}
	second.dataWriter = secondCapture.write
	request := MTPTransferRequest{ProtocolData: transferProtocolData(0x123456, 8, nil)}

	for range 5 {
		if _, err := endpoint.MTPTransfer(request); err != nil {
			t.Fatalf("MTPTransfer: %v", err)
		}
	}
	first := associations["sg-a/sgp-a1"]
	firstCapture := captures["sg-a/sgp-a1"]
	selected, selectedCapture, backupCapture := first, firstCapture, secondCapture
	if firstCapture.count() == 0 {
		selected, selectedCapture, backupCapture = second, secondCapture, firstCapture
	}
	if selectedCapture.count() != 5 || backupCapture.count() != 0 {
		t.Fatalf("same flow moved between Associations: counts = %d and %d",
			selectedCapture.count(), backupCapture.count())
	}
	if err := selected.Close(); err != nil {
		t.Fatalf("close selected Association: %v", err)
	}
	if _, err := endpoint.MTPTransfer(request); err != nil {
		t.Fatalf("MTPTransfer after Association loss: %v", err)
	}
	if backupCapture.count() != 1 {
		t.Fatal("flow did not fail over to the surviving Association of the same SGP")
	}
}

func TestMTPTransferFlowCacheIsBounded(t *testing.T) {
	config := validASPConfig()
	config.TransferFlowCacheEntries = 2
	endpoint, _, _ := newASPTransferFixture(t, config)
	for sls := uint8(1); sls <= 3; sls++ {
		if _, err := endpoint.MTPTransfer(MTPTransferRequest{
			ProtocolData: transferProtocolData(0x123456, sls, nil),
		}); err != nil {
			t.Fatalf("MTPTransfer SLS %d: %v", sls, err)
		}
	}
	endpoint.aspRoutes.mu.RLock()
	entries := len(endpoint.aspRoutes.transferFlows)
	lruEntries := endpoint.aspRoutes.transferFlowLRU.Len()
	endpoint.aspRoutes.mu.RUnlock()
	if entries != 2 || lruEntries != 2 {
		t.Fatalf("flow cache sizes = map:%d LRU:%d, want 2", entries, lruEntries)
	}
}

func TestMTPTransferOrdersScopeLossAfterInFlightWrite(t *testing.T) {
	config := validASPConfig()
	config.SignallingGatewaySelection = RouteSelectionPrimaryBackup
	endpoint, associations, captures := newASPTransferFixture(t, config)
	primary := associations["sg-a/sgp-a1"]
	writeStarted := make(chan struct{})
	releaseWrite := make(chan struct{})
	primary.dataWriter = func(data []byte, _ *sctp.SndRcvInfo) (int, error) {
		close(writeStarted)
		<-releaseWrite
		return len(data), nil
	}

	transferDone := make(chan error, 1)
	go func() {
		_, err := endpoint.MTPTransfer(MTPTransferRequest{
			ProtocolData: transferProtocolData(0x123456, 1, nil),
		})
		transferDone <- err
	}()
	select {
	case <-writeStarted:
	case <-time.After(time.Second):
		t.Fatal("MTPTransfer did not reach the selected Association")
	}

	scopeChanged := make(chan struct{})
	go func() {
		primary.noteRoutingContextsUnacked(params.NewRoutingContext(1))
		close(scopeChanged)
	}()
	select {
	case <-scopeChanged:
		close(releaseWrite)
		<-transferDone
		t.Fatal("Application Server scope changed before the in-flight DATA write completed")
	case <-time.After(30 * time.Millisecond):
	}
	close(releaseWrite)
	if err := <-transferDone; err != nil {
		t.Fatalf("in-flight MTPTransfer: %v", err)
	}
	select {
	case <-scopeChanged:
	case <-time.After(time.Second):
		t.Fatal("Application Server scope did not change after DATA completed")
	}
	if _, err := endpoint.MTPTransfer(MTPTransferRequest{
		ProtocolData: transferProtocolData(0x123456, 2, nil),
	}); err != nil {
		t.Fatalf("MTPTransfer after primary scope loss: %v", err)
	}
	if captures["sg-b/sgp-b1"].count() != 1 {
		t.Fatal("transfer after scope loss did not use the alternate SGP")
	}
}

type mtpTransferCapture struct {
	mu       sync.Mutex
	raw      [][]byte
	streams  []uint16
	writeErr error
}

func (capture *mtpTransferCapture) write(data []byte, info *sctp.SndRcvInfo) (int, error) {
	capture.mu.Lock()
	defer capture.mu.Unlock()
	if capture.writeErr != nil {
		return 0, capture.writeErr
	}
	capture.raw = append(capture.raw, append([]byte(nil), data...))
	capture.streams = append(capture.streams, info.Stream)
	return len(data), nil
}

func (capture *mtpTransferCapture) count() int {
	capture.mu.Lock()
	defer capture.mu.Unlock()
	return len(capture.raw)
}

func (capture *mtpTransferCapture) lastData(t *testing.T) (*messages.Data, uint16) {
	t.Helper()
	capture.mu.Lock()
	defer capture.mu.Unlock()
	if len(capture.raw) == 0 {
		t.Fatal("no captured DATA")
	}
	parsed, err := messages.Parse(capture.raw[len(capture.raw)-1])
	if err != nil {
		t.Fatalf("parse captured DATA: %v", err)
	}
	data, ok := parsed.(*messages.Data)
	if !ok {
		t.Fatalf("captured message = %T, want *messages.Data", parsed)
	}
	return data, capture.streams[len(capture.streams)-1]
}

func newASPTransferFixture(
	t *testing.T,
	config *ASPConfig,
) (*Endpoint, map[string]*Association, map[string]*mtpTransferCapture) {
	t.Helper()
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: config})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	associations := make(map[string]*Association)
	captures := make(map[string]*mtpTransferCapture)
	for _, gateway := range config.SignallingGateways {
		for _, sgp := range gateway.SGPs {
			route := sgp.Routes[0]
			association := attachASPRouteAssociation(t, endpoint, SGPIdentity{
				SignallingGateway:        gateway.ID,
				SignallingGatewayProcess: sgp.ID,
			}, route.AS.NetworkAppearance, route.AS.RoutingContext)
			capture := &mtpTransferCapture{}
			association.dataWriter = capture.write
			key := string(gateway.ID) + "/" + string(sgp.ID)
			associations[key] = association
			captures[key] = capture
		}
	}
	return endpoint, associations, captures
}

func oneSignallingGatewayTwoSGPConfig(mode RouteSelectionMode) *ASPConfig {
	config := validASPConfig()
	config.SignallingGatewaySelection = RouteSelectionPrimaryBackup
	config.SignallingGateways = config.SignallingGateways[:1]
	config.SignallingGateways[0].SGPSelection = mode
	second := config.SignallingGateways[0].SGPs[0]
	second.ID = "sgp-a2"
	config.SignallingGateways[0].SGPs = append(config.SignallingGateways[0].SGPs, second)
	return config
}

func transferProtocolData(pointCode uint32, sls uint8, data []byte) *params.ProtocolDataPayload {
	return params.NewProtocolDataPayload(
		0x111111, pointCode, params.ServiceIndSCCP, 0, 0, sls, data,
	)
}
