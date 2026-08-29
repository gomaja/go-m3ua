package m3ua

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

func TestIPSPSingleExchangeAcrossSCTPInitiationOrientations(t *testing.T) {
	for _, sctpInitiator := range []string{"IPSP-A", "IPSP-B"} {
		for _, aspsmInitiator := range []string{"IPSP-A", "IPSP-B"} {
			for _, asptmInitiator := range []string{"IPSP-A", "IPSP-B"} {
				name := fmt.Sprintf("SCTP_%s_ASPSM_%s_ASPTM_%s", sctpInitiator, aspsmInitiator, asptmInitiator)
				t.Run(name, func(t *testing.T) {
					exerciseIPSPSingleExchangeIntegration(
						t, sctpInitiator, aspsmInitiator, asptmInitiator,
					)
				})
			}
		}
	}
}

func exerciseIPSPSingleExchangeIntegration(t *testing.T, sctpInitiator, aspsmInitiator, asptmInitiator string) {
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

	configA := integrationIPSPConfig(
		0x111111, 0x222222,
		aspsmInitiator == "IPSP-A", asptmInitiator == "IPSP-A",
	)
	configB := integrationIPSPConfig(
		0x222222, 0x111111,
		aspsmInitiator == "IPSP-B", asptmInitiator == "IPSP-B",
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
	if associationA.State() != StateASPActive || associationB.State() != StateASPActive {
		t.Fatalf("established states: IPSP-A=%s IPSP-B=%s", associationA.State(), associationB.State())
	}

	assertIPSPTransfer(t, associationA, associationB, []byte("A-to-B"))
	assertIPSPTransfer(t, associationB, associationA, []byte("B-to-A"))

	if _, err := associationA.WriteSignal(messages.NewNotify(
		params.NewStatus(params.AsStateActive), nil, params.NewRoutingContext(1), nil,
	)); err != nil {
		t.Fatalf("send IPSP Notify: %v", err)
	}
	select {
	case indication := <-associationB.ManagementIndications():
		if indication.Kind != ManagementNotify {
			t.Fatalf("Notify indication kind = %v, want ManagementNotify", indication.Kind)
		}
	case <-ctx.Done():
		t.Fatalf("receive IPSP Notify: %v", ctx.Err())
	}

	if err := associationA.ShutdownContext(ctx); err != nil {
		t.Fatalf("Shutdown IPSP-A: %v", err)
	}
	select {
	case <-associationB.Done():
	case <-ctx.Done():
		t.Fatalf("IPSP-B did not observe shutdown: %v", ctx.Err())
	}
}

func integrationIPSPConfig(opc, dpc uint32, initiateASPSM, initiateASPTM bool) *AssociationConfig {
	config := NewAssociationConfig(opc, dpc, params.ServiceIndSCCP, 0, 0, 1)
	config.IPSP = &IPSPConfig{
		ExchangeModel: IPSPExchangeSingle,
		InitiateASPSM: initiateASPSM,
		InitiateASPTM: initiateASPTM,
	}
	config.TrafficModeType = params.NewTrafficModeType(params.TrafficModeLoadshare)
	config.NetworkAppearance = params.NewNetworkAppearance(7)
	config.RoutingContexts = params.NewRoutingContext(1)
	config.EstablishTimeout = 10 * time.Second
	config.TAck = 100 * time.Millisecond
	config.TAckRetries = 10
	return config
}

func assertIPSPTransfer(t *testing.T, sender, receiver *Association, payload []byte) {
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
		if !message.NetworkAppearanceSet || message.NetworkAppearance != 7 ||
			!message.RoutingContextSet || message.RoutingContext != 1 {
			t.Fatalf("received IPSP DATA scope = NA(%v,%d) RC(%v,%d)",
				message.NetworkAppearanceSet, message.NetworkAppearance,
				message.RoutingContextSet, message.RoutingContext)
		}
	case err := <-errs:
		t.Fatalf("read IPSP DATA: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out reading IPSP DATA")
	}
}
