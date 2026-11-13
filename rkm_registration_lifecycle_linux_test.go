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

func TestDynamicRegistrationBeforeASPActiveSCTP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP, RoutingKeyManagement: &RoutingKeyManagementConfig{AllowDynamicRoutingKeys: true, AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus { return RegistrationSuccessfullyRegistered }}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = endpoint.Close() }()
	cfg := NewAssociationConfig()
	cfg.HeartbeatInfo = &HeartbeatInfo{Enabled: false}
	address := &sctp.Addr{IPs: []netip.Addr{netip.MustParseAddr("127.0.0.1")}, Port: 41982}
	listener, err := endpoint.Listen("m3ua", address, NewListenerConfig(cfg))
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
	send := func(m messages.M3UA) {
		t.Helper()
		raw, err := m.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := sendWithInfo(peer, raw, &sctp.SndInfo{PPID: M3UAPPID, Stream: 0}); err != nil {
			t.Fatal(err)
		}
	}
	receive := func(class, typ uint8) messages.M3UA {
		t.Helper()
		buf := make([]byte, 65535)
		for {
			n, _, err := recvWithInfo(peer, buf)
			if err != nil {
				t.Fatal(err)
			}
			m, err := messages.Parse(buf[:n])
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := m.(*messages.Error); ok {
				t.Fatalf("unexpected ERR: %v", m)
			}
			if m.MessageClass() == class && m.MessageType() == typ {
				return m
			}
		}
	}
	send(messages.NewAspUp(nil, nil))
	receive(messages.MsgClassASPSM, messages.MsgTypeAspUpAck)
	key, err := routingKeyParameter(RoutingKeyRegistrationRequest{LocalRoutingKeyIdentifier: 1, RoutingKey: testRoutingKey(10, 100, 3)})
	if err != nil {
		t.Fatal(err)
	}
	send(messages.NewRegistrationRequest(key))
	response := receive(messages.MsgClassRKM, messages.MsgTypeRegistrationResponse).(*messages.RegistrationResponse)
	if len(response.RegistrationResults) != 1 {
		t.Fatalf("results=%d", len(response.RegistrationResults))
	}
	result, err := response.RegistrationResults[0].RegistrationResult()
	if err != nil {
		t.Fatal(err)
	}
	if result.RegistrationStatus.RegistrationStatus() != params.SuccessfullyRegistered || result.LocalRoutingKeyIdentifier.LocalRoutingKeyIdentifier() != 1 || result.RoutingContext.RoutingContext() == 0 {
		t.Fatalf("result=%v", result)
	}
	// SGP Accept waits for ASP Active; registration must finish before then.
	send(messages.NewAspActive(params.NewTrafficModeType(params.TrafficModeLoadshare), params.NewRoutingContext(result.RoutingContext.RoutingContext()), nil))
	receive(messages.MsgClassASPTM, messages.MsgTypeAspActiveAck)
	select {
	case err := <-accepted:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
