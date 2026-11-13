package m3ua

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

type membershipNotifyWirePeer struct {
	t              *testing.T
	transport      *sctp.Conn
	association    *Association
	retiredContext uint32
}

func (peer *membershipNotifyWirePeer) send(message messages.M3UA) {
	peer.t.Helper()
	data, err := message.MarshalBinary()
	if err != nil {
		peer.t.Fatal(err)
	}
	if _, err := sendWithInfo(peer.transport, data, &sctp.SndInfo{PPID: M3UAPPID, Stream: 0}); err != nil {
		peer.t.Fatal(err)
	}
}

func (peer *membershipNotifyWirePeer) receive(class, kind uint8) messages.M3UA {
	peer.t.Helper()
	buffer := make([]byte, 65535)
	for {
		n, _, err := recvWithInfo(peer.transport, buffer)
		if err != nil {
			peer.t.Fatal(err)
		}
		message, err := messages.Parse(buffer[:n])
		if err != nil {
			peer.t.Fatal(err)
		}
		if notify, ok := message.(*messages.Notify); ok {
			if peer.retiredContext != 0 && notify.Status.StatusType() == params.AsStateChange && notify.RoutingContext != nil && slices.Contains(notify.RoutingContext.RoutingContexts(), peer.retiredContext) {
				peer.t.Errorf("wire AS-state Notify reached retired ASP for RC %d", peer.retiredContext)
			}
			if class != messages.MsgClassManagement || kind != messages.MsgTypeNotify {
				continue
			}
		}
		if message.MessageClass() != class || message.MessageType() != kind {
			peer.t.Fatalf("received %s, want class %d type %d", message.MessageTypeName(), class, kind)
		}
		return message
	}
}

func (peer *membershipNotifyWirePeer) register(identifier, dpc uint32) uint32 {
	peer.t.Helper()
	parameter, err := routingKeyParameter(RoutingKeyRegistrationRequest{LocalRoutingKeyIdentifier: identifier, RoutingKey: testRoutingKey(10, dpc, 3)})
	if err != nil {
		peer.t.Fatal(err)
	}
	peer.send(messages.NewRegistrationRequest(parameter))
	response := peer.receive(messages.MsgClassRKM, messages.MsgTypeRegistrationResponse).(*messages.RegistrationResponse)
	result, err := response.RegistrationResults[0].RegistrationResult()
	if err != nil {
		peer.t.Fatal(err)
	}
	if status := result.RegistrationStatus.RegistrationStatus(); status != params.SuccessfullyRegistered {
		peer.t.Fatalf("REG RSP status=%d", status)
	}
	return result.RoutingContext.RoutingContext()
}

func newMembershipNotifyWirePeer(t *testing.T, ctx context.Context, listener *Listener, address string, configure func(*membershipNotifyWirePeer)) *membershipNotifyWirePeer {
	t.Helper()
	accepted := make(chan associationResult, 1)
	go func() {
		association, err := listener.Accept(ctx)
		accepted <- associationResult{association: association, err: err}
	}()
	transport, err := sctp.Dial(ctx, "sctp", mcAddr(0, address), listener.Addr().(*sctp.Addr))
	if err != nil {
		skipIfSCTPUnsupported(t, err)
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = transport.Close() })
	if err := transport.SetDeadline(time.Now().Add(12 * time.Second)); err != nil {
		t.Fatal(err)
	}
	peer := &membershipNotifyWirePeer{t: t, transport: transport}
	peer.send(messages.NewAspUp(nil, nil))
	peer.receive(messages.MsgClassASPSM, messages.MsgTypeAspUpAck)
	configure(peer)
	select {
	case result := <-accepted:
		if result.err != nil {
			t.Fatal(result.err)
		}
		peer.association = result.association
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	return peer
}

type membershipNotifyAckGate struct {
	controlSender
	started chan struct{}
	release <-chan struct{}
	once    sync.Once
}

func (gate *membershipNotifyAckGate) SendMsg(data []byte, options sctp.SendOptions) (int, error) {
	if len(data) >= 4 && data[2] == messages.MsgClassASPSM && data[3] == messages.MsgTypeAspDownAck {
		gate.once.Do(func() { close(gate.started); <-gate.release })
	}
	return gate.controlSender.SendMsg(data, options)
}

func TestSGPReadyNotifyBacklogExcludesRetiredMembershipOverSCTP(t *testing.T) {
	for _, action := range []string{"active-up", "down-then-up", "explicit-dereg"} {
		t.Run(action, func(t *testing.T) {
			endpoint := newRegistrationLifetimeEndpoint(t, false)
			endpoint.as.recoveryTimer = 100 * time.Millisecond
			config := NewAssociationConfig()
			config.HeartbeatInfo = &HeartbeatInfo{Enabled: false}
			listener, err := endpoint.Listen("m3ua", mcAddr(0, "127.0.0.1"), NewListenerConfig(config))
			if err != nil {
				skipIfSCTPUnsupported(t, err)
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = listener.Close() })
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			t.Cleanup(cancel)
			var x, y uint32
			retiring := newMembershipNotifyWirePeer(t, ctx, listener, "127.0.0.2", func(peer *membershipNotifyWirePeer) {
				x = peer.register(1, 100)
				y = peer.register(2, 200)
				peer.send(messages.NewAspActive(nil, params.NewRoutingContext(y), nil))
				peer.receive(messages.MsgClassASPTM, messages.MsgTypeAspActiveAck)
			})
			withdrawing := newMembershipNotifyWirePeer(t, ctx, listener, "127.0.0.3", func(peer *membershipNotifyWirePeer) {
				if got := peer.register(1, 100); got != x {
					t.Fatal("AS X was not shared")
				}
				peer.send(messages.NewAspActive(nil, params.NewRoutingContext(x), nil))
				peer.receive(messages.MsgClassASPTM, messages.MsgTypeAspActiveAck)
			})
			survivor := newMembershipNotifyWirePeer(t, ctx, listener, "127.0.0.4", func(peer *membershipNotifyWirePeer) {
				if got := peer.register(1, 100); got != x {
					t.Fatal("AS X was not shared")
				}
				peer.send(messages.NewAspActive(nil, params.NewRoutingContext(x), nil))
				peer.receive(messages.MsgClassASPTM, messages.MsgTypeAspActiveAck)
			})
			survivor.send(messages.NewAspInactive(params.NewRoutingContext(x), nil))
			survivor.receive(messages.MsgClassASPTM, messages.MsgTypeAspInactiveAck)
			release := make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			gate := &membershipNotifyAckGate{controlSender: withdrawing.association.sctpConn, started: make(chan struct{}), release: release}
			// Control transport lookup and submission use sendMu in production.
			withdrawing.association.sendMu.Lock()
			withdrawing.association.controlTransport = gate
			withdrawing.association.sendMu.Unlock()
			withdrawing.send(messages.NewAspDown(nil))
			select {
			case <-gate.started:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			key, _ := retiring.association.dynamicASKey(x, false)
			as, _ := endpoint.as.lookup(key)
			var later *asNotification
			if !waitFor(func() bool {
				as.mu.Lock()
				defer as.mu.Unlock()
				for _, event := range as.notifications {
					if event.status == params.AsStateInactive {
						select {
						case <-event.ready:
							later = event
							return true
						default:
						}
					}
				}
				return false
			}, 3*time.Second) {
				t.Fatal("ready AS-INACTIVE event did not queue behind blocked Ack")
			}
			switch action {
			case "active-up":
				retiring.send(messages.NewAspUp(nil, nil))
				retiring.receive(messages.MsgClassASPSM, messages.MsgTypeAspUpAck)
				retiring.retiredContext = x
				response := retiring.receive(messages.MsgClassManagement, messages.MsgTypeError).(*messages.Error)
				if response.ErrorCode.ErrorCode() != params.UnexpectedMessageError {
					t.Fatal("active Up did not report Unexpected Message")
				}
			case "down-then-up":
				retiring.send(messages.NewAspDown(nil))
				retiring.receive(messages.MsgClassASPSM, messages.MsgTypeAspDownAck)
				retiring.send(messages.NewAspUp(nil, nil))
				retiring.receive(messages.MsgClassASPSM, messages.MsgTypeAspUpAck)
				retiring.retiredContext = x
			case "explicit-dereg":
				retiring.send(messages.NewDeregistrationRequest(params.NewRoutingContext(x)))
				response := retiring.receive(messages.MsgClassRKM, messages.MsgTypeDeregistrationResponse).(*messages.DeregistrationResponse)
				result, err := response.DeregistrationResults[0].DeregistrationResult()
				if err != nil {
					t.Fatal(err)
				}
				if result.DeregistrationStatus.DeregistrationStatus() != params.SuccessfullyDeregistered {
					t.Fatal("DEREG failed")
				}
				retiring.retiredContext = x
			}
			unblock()
			withdrawing.receive(messages.MsgClassASPSM, messages.MsgTypeAspDownAck)
			var states []uint32
			for !slices.Equal(states, []uint32{params.AsStatePending, params.AsStateInactive}) {
				notify := survivor.receive(messages.MsgClassManagement, messages.MsgTypeNotify).(*messages.Notify)
				if notify.Status.StatusType() != params.AsStateChange || notify.RoutingContext == nil || notify.RoutingContext.RoutingContext() != x {
					continue
				}
				status := notify.Status.Status()
				if len(states) == 0 && status != params.AsStatePending {
					continue
				}
				states = append(states, status)
				if len(states) > 2 {
					t.Fatalf("survivor received wrong state sequence: %v", states)
				}
			}
			select {
			case <-later.done:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			// The AS event has enqueued all targets. A stream-0 control barrier now
			// follows every earlier per-peer notification at the actual socket writer.
			marker := []byte("membership delivery barrier")
			if err := retiring.association.writeMandatoryControls([]messages.M3UA{messages.NewHeartbeat(params.NewHeartbeatData(marker))}, false, true); err != nil {
				t.Fatal(err)
			}
			retiring.receive(messages.MsgClassASPSM, messages.MsgTypeHeartbeat)
			t.Logf("%s: surviving ASP received AS-PENDING then AS-INACTIVE for RC %d; retired ASP reached the ordered barrier", action, x)
		})
	}
}

var _ controlSender = (*membershipNotifyAckGate)(nil)
