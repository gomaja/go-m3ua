package m3ua

import (
	"context"
	"net/netip"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

func TestRegistrationSemanticFaultSCTPBatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sgp, err := NewEndpoint(EndpointConfig{Role: RoleSGP, RoutingKeyManagement: &RoutingKeyManagementConfig{
		AuthorizeRegistration:   func(RoutingKeyRegistrationRequest) RegistrationStatus { return RegistrationSuccessfullyRegistered },
		AllowDynamicRoutingKeys: true,
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sgp.Close() }()
	cfg := NewAssociationConfig()
	cfg.HeartbeatInfo = &HeartbeatInfo{Enabled: false}
	address := &sctp.Addr{IPs: []netip.Addr{netip.MustParseAddr("127.0.0.1")}, Port: 41981}
	listener, err := sgp.Listen("m3ua", address, NewListenerConfig(cfg))
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan error, 1)
	go func() { _, err := listener.Accept(ctx); accepted <- err }()
	peer, err := (&sctp.Config{InitMsg: sctp.InitMsg{OutStreams: 4, MaxInStreams: 4}}).Dial(ctx, "sctp", nil, address)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = peer.Close() }()
	if err := peer.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	send := func(raw []byte) {
		t.Helper()
		if _, err := sendWithInfo(peer, raw, &sctp.SndInfo{PPID: M3UAPPID, Stream: 0}); err != nil {
			t.Fatal(err)
		}
	}
	receive := func() messages.M3UA {
		t.Helper()
		buf := make([]byte, 65535)
		n, _, err := recvWithInfo(peer, buf)
		if err != nil {
			t.Fatal(err)
		}
		m, err := messages.Parse(buf[:n])
		if err != nil {
			t.Fatal(err)
		}
		return m
	}
	up, err := messages.NewAspUp(nil, nil).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	send(up)
	for {
		if _, ok := receive().(*messages.AspUpAck); ok {
			break
		}
	}
	// SGP Accept returns after ASP Active. Establish the default contextless
	// AS before exercising registration of the two additional Routing Keys.
	active, err := messages.NewAspActive(params.NewTrafficModeType(params.TrafficModeOverride), nil, nil).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	send(active)
	for {
		if _, ok := receive().(*messages.AspActiveAck); ok {
			break
		}
	}
	select {
	case err := <-accepted:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	send(registrationWire(messages.MsgTypeRegistrationRequest,
		registrationTLV(params.RoutingKey, registrationWord(params.LocalRoutingKeyIdentifier, 1), registrationWord(params.DestinationPointCode, 0x01000064)),
		registrationTLV(params.RoutingKey, registrationWord(params.LocalRoutingKeyIdentifier, 2), registrationWord(params.DestinationPointCode, 200))))
	for {
		m := receive()
		if _, ok := m.(*messages.Error); ok {
			t.Fatalf("REG REQ received ERR: %v", m)
		}
		response, ok := m.(*messages.RegistrationResponse)
		if !ok {
			continue
		}
		if len(response.RegistrationResults) != 2 {
			t.Fatalf("results = %d", len(response.RegistrationResults))
		}
		for i, status := range []uint32{params.InvalidDPC, params.SuccessfullyRegistered} {
			result, err := response.RegistrationResults[i].RegistrationResult()
			if err != nil {
				t.Fatal(err)
			}
			if result.LocalRoutingKeyIdentifier.LocalRoutingKeyIdentifier() != uint32(i+1) || result.RegistrationStatus.RegistrationStatus() != status {
				t.Fatalf("result %d = %v", i, result)
			}
			rc := result.RoutingContext.RoutingContext()
			if (i == 0 && rc != 0) || (i == 1 && rc == 0) {
				t.Fatalf("result %d RC = %d", i, rc)
			}
		}
		break
	}
	if sgp.routingKeys.dynamicCount() != 1 {
		t.Fatal("valid sibling not installed")
	}
}
