package m3ua

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

// These socket tests exercise the production dispatcher on an SCTP transport.
func TestSGPASPDownRetiresRegistrationOverSCTP(t *testing.T) {
	testSGPRegistrationRetirementOverSCTP(t, false)
}

func TestSGPASPUpWhileActiveRetiresRegistrationOverSCTP(t *testing.T) {
	testSGPRegistrationRetirementOverSCTP(t, true)
}

func testSGPRegistrationRetirementOverSCTP(t *testing.T, activeUp bool) {
	t.Helper()
	for _, removeUnused := range []bool{false, true} {
		t.Run(fmt.Sprintf("remove-unused=%v", removeUnused), func(t *testing.T) {
			testSGPRegistrationRetirementExchange(t, activeUp, removeUnused)
		})
	}
}

func testSGPRegistrationRetirementExchange(t *testing.T, activeUp, removeUnused bool) {
	t.Helper()
	endpoint := newRegistrationLifetimeEndpoint(t, removeUnused)
	config := NewAssociationConfig()
	config.HeartbeatInfo = &HeartbeatInfo{Enabled: false}
	listener, err := endpoint.Listen("m3ua", mcAddr(0, "127.0.0.1"), NewListenerConfig(config))
	if err != nil {
		skipIfSCTPUnsupported(t, err)
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	accepted := make(chan error, 1)
	go func() { _, err := listener.Accept(ctx); accepted <- err }()
	peer, err := sctp.Dial(ctx, "sctp", mcAddr(0, "127.0.0.2"), listener.Addr().(*sctp.Addr))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = peer.Close() }()
	if err := peer.SetDeadline(time.Now().Add(8 * time.Second)); err != nil {
		t.Fatal(err)
	}
	send := func(message messages.M3UA) {
		t.Helper()
		data, err := message.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := sendWithInfo(peer, data, &sctp.SndInfo{PPID: M3UAPPID, Stream: 0}); err != nil {
			t.Fatal(err)
		}
	}
	var retiredContext uint32
	receive := func(class, kind uint8) messages.M3UA {
		t.Helper()
		buffer := make([]byte, 65535)
		for {
			n, _, err := recvWithInfo(peer, buffer)
			if err != nil {
				t.Fatal(err)
			}
			message, err := messages.Parse(buffer[:n])
			if err != nil {
				t.Fatal(err)
			}
			if notify, ok := message.(*messages.Notify); ok {
				if retiredContext != 0 && notify.RoutingContext != nil && slices.Contains(notify.RoutingContext.RoutingContexts(), retiredContext) {
					t.Error("wire Notify named a retired Routing Context")
				}
				continue
			}
			if message.MessageClass() != class || message.MessageType() != kind {
				t.Fatalf("received %s, want class %d type %d", message.MessageTypeName(), class, kind)
			}
			return message
		}
	}
	up := func() { send(messages.NewAspUp(nil, nil)); receive(messages.MsgClassASPSM, messages.MsgTypeAspUpAck) }
	register := func(identifier uint32) uint32 {
		parameter, err := routingKeyParameter(RoutingKeyRegistrationRequest{LocalRoutingKeyIdentifier: identifier, RoutingKey: testRoutingKey(10, 100, 3)})
		if err != nil {
			t.Fatal(err)
		}
		send(messages.NewRegistrationRequest(parameter))
		response := receive(messages.MsgClassRKM, messages.MsgTypeRegistrationResponse).(*messages.RegistrationResponse)
		result, err := response.RegistrationResults[0].RegistrationResult()
		if err != nil {
			t.Fatal(err)
		}
		status := result.RegistrationStatus.RegistrationStatus()
		t.Logf("REG RSP Local RK Identifier=%d status=%d RC=%d", identifier, status, result.RoutingContext.RoutingContext())
		if status != params.SuccessfullyRegistered {
			t.Fatalf("REG RSP status=%d, want success", status)
		}
		return result.RoutingContext.RoutingContext()
	}
	up()
	rc := register(77)
	send(messages.NewAspActive(params.NewTrafficModeType(params.TrafficModeLoadshare), params.NewRoutingContext(rc), nil))
	receive(messages.MsgClassASPTM, messages.MsgTypeAspActiveAck)
	select {
	case err := <-accepted:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if activeUp {
		send(messages.NewAspUp(nil, nil))
		receive(messages.MsgClassASPSM, messages.MsgTypeAspUpAck)
		retiredContext = rc
		response := receive(messages.MsgClassManagement, messages.MsgTypeError).(*messages.Error)
		if code := response.ErrorCode.ErrorCode(); code != params.UnexpectedMessageError {
			t.Fatalf("active-state Up ERR=%#x, want Unexpected Message", code)
		}
		t.Logf("active-state Up ERR=%#x", response.ErrorCode.ErrorCode())
	} else {
		send(messages.NewAspDown(nil))
		receive(messages.MsgClassASPSM, messages.MsgTypeAspDownAck)
		up()
		retiredContext = rc
	}
	send(messages.NewAspActive(nil, params.NewRoutingContext(rc), nil))
	response := receive(messages.MsgClassManagement, messages.MsgTypeError).(*messages.Error)
	// RFC 4666 §4.3.4.3: an RK no longer defined for the ASP cannot activate.
	if code := response.ErrorCode.ErrorCode(); code != params.ErrNoConfiguredAsForAsp {
		t.Fatalf("ERR code=%#x, want No configured AS for ASP", code)
	}
	t.Logf("retired RC activation ERR=%#x", response.ErrorCode.ErrorCode())
	register(77)
}
