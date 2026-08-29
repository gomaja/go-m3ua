package m3ua

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

func TestIPSPDoubleExchangeAcrossSCTPInitiationOrientations(t *testing.T) {
	tests := []struct {
		name          string
		aspsmExchange IPSPASPSMExchangeModel
		initiateA     bool
		initiateB     bool
		fullExchange  bool
	}{
		{"double ASPSM both directions", IPSPASPSMExchangeDouble, true, true, true},
		{"double ASPSM IPSP-A direction", IPSPASPSMExchangeDouble, true, false, false},
		{"double ASPSM IPSP-B direction", IPSPASPSMExchangeDouble, false, true, false},
		{"single ASPSM initiated by IPSP-A", IPSPASPSMExchangeSingle, true, false, true},
		{"single ASPSM initiated by IPSP-B", IPSPASPSMExchangeSingle, false, true, true},
		{"single ASPSM simultaneous initiation", IPSPASPSMExchangeSingle, true, true, true},
	}
	for _, sctpInitiator := range []string{"IPSP-A", "IPSP-B"} {
		for _, test := range tests {
			name := fmt.Sprintf("SCTP_%s_%s", sctpInitiator, test.name)
			t.Run(name, func(t *testing.T) {
				exerciseIPSPDoubleExchangeIntegration(
					t, sctpInitiator, test.aspsmExchange,
					test.initiateA, test.initiateB, test.fullExchange, false,
				)
			})
		}
	}
}

func TestIPSPDoubleExchangeContextlessAcrossSCTPInitiationOrientations(t *testing.T) {
	for _, sctpInitiator := range []string{"IPSP-A", "IPSP-B"} {
		t.Run("SCTP_"+sctpInitiator, func(t *testing.T) {
			exerciseIPSPDoubleExchangeIntegration(
				t, sctpInitiator, IPSPASPSMExchangeDouble, true, true, true, true,
			)
		})
	}
}

func exerciseIPSPDoubleExchangeIntegration(
	t *testing.T,
	sctpInitiator string,
	aspsmExchange IPSPASPSMExchangeModel,
	initiateA, initiateB, fullExchange, contextless bool,
) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	endpointA, err := NewEndpoint(EndpointConfig{Role: RoleIPSP})
	if err != nil {
		t.Fatalf("NewEndpoint IPSP-A: %v", err)
	}
	t.Cleanup(func() { _ = endpointA.Close() })
	endpointB, err := NewEndpoint(EndpointConfig{Role: RoleIPSP})
	if err != nil {
		t.Fatalf("NewEndpoint IPSP-B: %v", err)
	}
	t.Cleanup(func() { _ = endpointB.Close() })

	trafficA := ipspIntegrationTraffic(10, 11)
	trafficB := ipspIntegrationTraffic(20, 22)
	if contextless {
		trafficA = ipspIntegrationContextlessTraffic(10)
		trafficB = ipspIntegrationContextlessTraffic(20)
	}
	configA := integrationIPSPDoubleExchangeConfig(
		0x111111, 0x222222, aspsmExchange, initiateA, trafficA, trafficB,
	)
	configB := integrationIPSPDoubleExchangeConfig(
		0x222222, 0x111111, aspsmExchange, initiateB, trafficB, trafficA,
	)

	listeningEndpoint, dialingEndpoint := endpointB, endpointA
	listeningConfig, dialingConfig := configB, configA
	if sctpInitiator == "IPSP-B" {
		listeningEndpoint, dialingEndpoint = endpointA, endpointB
		listeningConfig, dialingConfig = configA, configB
	}
	listener, err := listeningEndpoint.Listen(
		"m3ua", mcAddr(0, "127.0.0.1"), NewListenerConfig(listeningConfig),
	)
	if err != nil {
		if isSCTPUnsupported(err) {
			t.Skipf("skipping socket-backed test: %v", err)
		}
		t.Fatalf("Listen IPSP: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	accepted := make(chan associationResult, 1)
	go func() {
		association, acceptErr := listener.Accept(ctx)
		accepted <- associationResult{association: association, err: acceptErr}
	}()
	dialed, err := dialingEndpoint.Dial(
		ctx, "m3ua", mcAddr(0, "127.0.0.1"), listener.Addr().(*sctp.SCTPAddr), dialingConfig,
	)
	if err != nil {
		t.Fatalf("Dial IPSP: %v", err)
	}
	var acceptedAssociation *Association
	select {
	case result := <-accepted:
		if result.err != nil {
			t.Fatalf("Accept IPSP: %v", result.err)
		}
		acceptedAssociation = result.association
	case <-ctx.Done():
		t.Fatalf("Accept IPSP: %v", ctx.Err())
	}

	associationA, associationB := dialed, acceptedAssociation
	if sctpInitiator == "IPSP-B" {
		associationA, associationB = acceptedAssociation, dialed
	}
	if !fullExchange {
		exerciseOneIPSPTrafficDirection(t, ctx, associationA, associationB, initiateA)
		return
	}

	waitForIPSPState(t, associationA, IPSPState{TrafficToLocal: StateASPActive, TrafficToPeer: StateASPActive})
	waitForIPSPState(t, associationB, IPSPState{TrafficToLocal: StateASPActive, TrafficToPeer: StateASPActive})
	if contextless {
		assertIPSPDoubleExchangeContextlessTransfer(t, associationA, associationB, 20, []byte("A-to-B"))
		assertIPSPDoubleExchangeContextlessTransfer(t, associationB, associationA, 10, []byte("B-to-A"))
		if err := associationA.ShutdownContext(ctx); err != nil {
			t.Fatalf("Shutdown IPSP-A: %v", err)
		}
		select {
		case <-associationB.Done():
		case <-ctx.Done():
			t.Fatalf("IPSP-B did not observe shutdown: %v", ctx.Err())
		}
		assertNoManagementErrorCode(t, "IPSP-A", associationA, params.UnexpectedMessageError)
		assertNoManagementErrorCode(t, "IPSP-B", associationB, params.UnexpectedMessageError)
		return
	}
	assertIPSPDoubleExchangeTransfer(t, associationA, associationB, 20, 22, []byte("A-to-B"))
	assertIPSPDoubleExchangeTransfer(t, associationB, associationA, 10, 11, []byte("B-to-A"))

	if err := associationA.DeactivateRoutingContexts(11); err != nil {
		t.Fatalf("deactivate IPSP-A TrafficToLocal: %v", err)
	}
	waitForIPSPState(t, associationA, IPSPState{TrafficToLocal: StateASPInactive, TrafficToPeer: StateASPActive})
	waitForIPSPState(t, associationB, IPSPState{TrafficToLocal: StateASPActive, TrafficToPeer: StateASPInactive})
	if _, err := associationB.Write([]byte("blocked-B-to-A")); !errors.Is(err, ErrNotEstablished) {
		t.Fatalf("B-to-A DATA after directional deactivation error = %v, want ErrNotEstablished", err)
	}
	assertIPSPDoubleExchangeTransfer(t, associationA, associationB, 20, 22, []byte("A-to-B-still-active"))

	if err := associationA.ActivateRoutingContexts(11); err != nil {
		t.Fatalf("reactivate IPSP-A TrafficToLocal: %v", err)
	}
	waitForIPSPState(t, associationA, IPSPState{TrafficToLocal: StateASPActive, TrafficToPeer: StateASPActive})
	waitForIPSPState(t, associationB, IPSPState{TrafficToLocal: StateASPActive, TrafficToPeer: StateASPActive})

	if err := associationA.ShutdownContext(ctx); err != nil {
		t.Fatalf("Shutdown IPSP-A: %v", err)
	}
	select {
	case <-associationB.Done():
	case <-ctx.Done():
		t.Fatalf("IPSP-B did not observe shutdown: %v", ctx.Err())
	}
	assertNoManagementErrorCode(t, "IPSP-A", associationA, params.UnexpectedMessageError)
	assertNoManagementErrorCode(t, "IPSP-B", associationB, params.UnexpectedMessageError)
}

func exerciseOneIPSPTrafficDirection(
	t *testing.T,
	ctx context.Context,
	associationA, associationB *Association,
	initiatedByA bool,
) {
	t.Helper()
	initiator, peer := associationA, associationB
	wantInitiator := IPSPState{TrafficToLocal: StateASPActive, TrafficToPeer: StateASPDown}
	wantPeer := IPSPState{TrafficToLocal: StateASPDown, TrafficToPeer: StateASPActive}
	blocked, sender, receiver := associationA, associationB, associationA
	networkAppearance, routingContext := uint32(10), uint32(11)
	if !initiatedByA {
		initiator, peer = associationB, associationA
		blocked, sender, receiver = associationB, associationA, associationB
		networkAppearance, routingContext = 20, 22
	}
	waitForIPSPState(t, initiator, wantInitiator)
	waitForIPSPState(t, peer, wantPeer)
	if _, err := blocked.Write([]byte("inactive-direction")); !errors.Is(err, ErrNotEstablished) {
		t.Fatalf("DATA in inactive IPSP direction error = %v, want ErrNotEstablished", err)
	}
	assertIPSPDoubleExchangeTransfer(
		t, sender, receiver, networkAppearance, routingContext, []byte("active-direction"),
	)
	if err := initiator.ShutdownContext(ctx); err != nil {
		t.Fatalf("Shutdown initiating IPSP: %v", err)
	}
	select {
	case <-peer.Done():
	case <-ctx.Done():
		t.Fatalf("peer IPSP did not observe shutdown: %v", ctx.Err())
	}
	assertNoManagementErrorCode(t, "initiating IPSP", initiator, params.UnexpectedMessageError)
	assertNoManagementErrorCode(t, "peer IPSP", peer, params.UnexpectedMessageError)
}

func integrationIPSPDoubleExchangeConfig(
	opc, dpc uint32,
	aspsmExchange IPSPASPSMExchangeModel,
	initiateASPSM bool,
	trafficToLocal, trafficToPeer *IPSPTrafficConfig,
) *AssociationConfig {
	config := NewAssociationConfig(opc, dpc, params.ServiceIndSCCP, 0, 0, 1)
	config.IPSP = &IPSPConfig{
		ExchangeModel:  IPSPExchangeDouble,
		ASPSMExchange:  aspsmExchange,
		InitiateASPSM:  initiateASPSM,
		InitiateASPTM:  true,
		TrafficToLocal: trafficToLocal,
		TrafficToPeer:  trafficToPeer,
	}
	config.EstablishTimeout = 10 * time.Second
	config.TAck = 100 * time.Millisecond
	config.TAckRetries = 10
	return config
}

func ipspIntegrationTraffic(networkAppearance, routingContext uint32) *IPSPTrafficConfig {
	return &IPSPTrafficConfig{
		TrafficModeType:   params.NewTrafficModeType(params.TrafficModeLoadshare),
		NetworkAppearance: params.NewNetworkAppearance(networkAppearance),
		RoutingContexts:   params.NewRoutingContext(routingContext),
	}
}

func ipspIntegrationContextlessTraffic(networkAppearance uint32) *IPSPTrafficConfig {
	return &IPSPTrafficConfig{
		TrafficModeType:   params.NewTrafficModeType(params.TrafficModeLoadshare),
		NetworkAppearance: params.NewNetworkAppearance(networkAppearance),
	}
}

func waitForIPSPState(t *testing.T, association *Association, want IPSPState) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if got := association.IPSPState(); got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("IPSP state = %+v, want %+v", association.IPSPState(), want)
}

func assertIPSPDoubleExchangeTransfer(
	t *testing.T,
	sender, receiver *Association,
	networkAppearance, routingContext uint32,
	payload []byte,
) {
	t.Helper()
	if _, err := sender.Write(payload); err != nil {
		t.Fatalf("write IPSP DATA %q: %v", payload, err)
	}
	data := make(chan *DataMessage, 1)
	errs := make(chan error, 1)
	go func() {
		message, err := receiver.ReadData()
		if err != nil {
			errs <- err
			return
		}
		data <- message
	}()
	select {
	case message := <-data:
		if string(message.ProtocolData.Data) != string(payload) {
			t.Fatalf("received IPSP DATA %q, want %q", message.ProtocolData.Data, payload)
		}
		if !message.NetworkAppearanceSet || message.NetworkAppearance != networkAppearance ||
			!message.RoutingContextSet || message.RoutingContext != routingContext {
			t.Fatalf("received IPSP DATA scope = NA(%v,%d) RC(%v,%d), want NA(%d) RC(%d)",
				message.NetworkAppearanceSet, message.NetworkAppearance,
				message.RoutingContextSet, message.RoutingContext,
				networkAppearance, routingContext)
		}
	case err := <-errs:
		t.Fatalf("read IPSP DATA: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out reading IPSP DATA")
	}
}

func assertIPSPDoubleExchangeContextlessTransfer(
	t *testing.T,
	sender, receiver *Association,
	networkAppearance uint32,
	payload []byte,
) {
	t.Helper()
	if _, err := sender.Write(payload); err != nil {
		t.Fatalf("write contextless IPSP DATA %q: %v", payload, err)
	}
	data := make(chan *DataMessage, 1)
	errs := make(chan error, 1)
	go func() {
		message, err := receiver.ReadData()
		if err != nil {
			errs <- err
			return
		}
		data <- message
	}()
	var message *DataMessage
	select {
	case message = <-data:
	case err := <-errs:
		t.Fatalf("read contextless IPSP DATA: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out reading contextless IPSP DATA")
	}
	if string(message.ProtocolData.Data) != string(payload) {
		t.Fatalf("received contextless IPSP DATA %q, want %q", message.ProtocolData.Data, payload)
	}
	if !message.NetworkAppearanceSet || message.NetworkAppearance != networkAppearance || message.RoutingContextSet {
		t.Fatalf("received contextless IPSP DATA scope = NA(%v,%d) RC(%v,%d), want NA(%d) without RC",
			message.NetworkAppearanceSet, message.NetworkAppearance,
			message.RoutingContextSet, message.RoutingContext,
			networkAppearance)
	}
}

func assertNoManagementErrorCode(t *testing.T, name string, association *Association, code uint32) {
	t.Helper()
	for indication := range association.ManagementIndications() {
		if indication.Kind == ManagementError && indication.ErrorCode == code {
			t.Fatalf("%s received unexpected M3UA Error code 0x%02x: %+v", name, code, indication)
		}
	}
}
