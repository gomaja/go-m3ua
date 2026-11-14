// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"net"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

type stagedSCONBuffer struct {
	*fullSendBuffer
	entered, proceed chan struct{}
	once             sync.Once
}

func (b *stagedSCONBuffer) stage(options sctp.SendOptions) {
	if options.NoWait && b.entered != nil {
		b.once.Do(func() { close(b.entered); <-b.proceed })
	}
}

func (b *stagedSCONBuffer) SendMsg(frame []byte, options sctp.SendOptions) (int, error) {
	b.stage(options)
	return b.fullSendBuffer.SendMsg(frame, options)
}

func (b *stagedSCONBuffer) SendMsgContext(ctx context.Context, frame []byte, options sctp.SendOptions) (int, error) {
	b.stage(options)
	return b.fullSendBuffer.SendMsgContext(ctx, frame, options)
}

// Wait for a real lock wait in the named production call, or for that call to
// finish. The latter is the expected outcome once its lock dependency is gone.
func awaitSCONLockStage(t *testing.T, name string, completed <-chan struct{}) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-completed:
			return
		default:
		}
		for _, stack := range goroutinesBlockedIn(name) {
			if strings.Contains(stack, "sync.(*RWMutex).Lock") || strings.Contains(stack, "sync.(*RWMutex).RLock") {
				return
			}
		}
		runtime.Gosched()
	}
	t.Fatalf("%s neither reached its lock wait nor completed", name)
}

func TestSCONBufferWaitDoesNotPreventTransportRelease(t *testing.T) {
	for _, mode := range []string{"application_close", "application_abort", "application_close_with", "mandatory_watchdog"} {
		t.Run(mode, func(t *testing.T) {
			mandatory := mode == "mandatory_watchdog"
			buffer := &stagedSCONBuffer{fullSendBuffer: newFullSendBuffer()}
			c, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
			c.endpoint = methodEndpointForTest(t, RoleASP, uint8(CongestionNationalWithoutPriorities), 3)
			setInventoryNetworkAppearance(&c.cfg.ApplicationServers, params.NewNetworkAppearance(7))
			c.noteRoutingContextsAcked(params.NewRoutingContext(1))
			c.cfg.TAck = time.Hour
			c.startTAck(messages.NewAspInactive(params.NewRoutingContext(1), nil), requestAspInactive)
			// Snapshot the omitted Ack's effective scope before the REG RSP is
			// applied, exactly as handleAspInactiveAck does before claiming it.
			snapshot := c.unambiguousInactiveAckRoutingContext()
			if snapshot == nil {
				t.Fatal("missing RC1 snapshot")
			}
			c.signalWriter = nil
			c.controlTransport = buffer
			c.transportCloser = buffer.close
			c.transportAborter = buffer.close
			t.Cleanup(func() { _ = buffer.close() })
			if mandatory {
				buffer.entered, buffer.proceed = make(chan struct{}), make(chan struct{})
				c.cfg.ControlWriteTimeout = 20 * time.Millisecond
			} else {
				deadline := time.Now().Add(time.Hour)
				c.applicationWriteDeadline.Store(&deadline)
			}
			var release sync.Once
			openAttempt := func() {
				if mandatory {
					release.Do(func() { close(buffer.proceed) })
				}
			}
			defer openAttempt()
			sent := make(chan error, 1)
			go func() {
				message := messages.NewSignallingCongestion(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCode(100), nil, params.NewCongestionIndications(2), nil)
				var err error
				if mandatory {
					_, err = c.writeControl(message)
				} else {
					_, err = c.WriteSignal(message)
				}
				sent <- err
			}()
			if mandatory {
				select {
				case <-buffer.entered:
				case <-time.After(time.Second):
					t.Fatal("initial attempt did not enter")
				}
			} else {
				select {
				case <-buffer.waiting:
				case <-time.After(time.Second):
					t.Fatal("application send did not park")
				}
				c.applicationWriteDeadline.Store(nil)
			}
			registered := make(chan struct{})
			go func() {
				_ = c.applyRegistrationResults([]registrationResultApplication{{request: RoutingKeyRegistrationRequest{RoutingKey: testRoutingKey(7, 200, 3)}, result: RoutingKeyRegistrationResult{Status: RegistrationSuccessfullyRegistered, RoutingContext: 2}}})
				close(registered)
			}()
			awaitSCONLockStage(t, "applyRegistrationResults", registered)
			acknowledged := make(chan struct{})
			go func() {
				c.claimTAckAcknowledgementForScope(requestAspInactive, snapshot, nil).complete()
				close(acknowledged)
			}()
			awaitSCONLockStage(t, "claimTAckAcknowledgementForScope", acknowledged)
			select {
			case <-registered:
			case <-time.After(time.Second):
				t.Fatal("unrelated registration waited for parked SCON")
			}
			select {
			case <-acknowledged:
			case <-time.After(time.Second):
				t.Fatal("Ack scope reader waited for parked SCON")
			}
			var closed chan error
			if mandatory {
				// The watchdog is armed only after this EAGAIN is released, so
				// neither its timeout nor a sleep establishes the lock ordering.
				openAttempt()
				select {
				case <-buffer.waiting:
				case <-time.After(time.Second):
					t.Fatal("mandatory send did not park")
				}
			} else {
				closed = make(chan error, 1)
				go func() {
					switch mode {
					case "application_abort":
						closed <- c.Abort()
					case "application_close_with":
						closed <- c.closeWith(ErrControlWriteTimeout)
					default:
						closed <- c.Close()
					}
				}()
			}
			select {
			case <-buffer.closed:
			case <-time.After(time.Second):
				t.Fatal("teardown could not release the parked SCON transport")
			}
			if mandatory {
				// Join the watchdog's already-started teardown, proving its
				// completion as well as its release of the transport.
				closed = make(chan error, 1)
				go func() { closed <- c.Close() }()
				if !errors.Is(c.Err(), ErrControlWriteTimeout) {
					t.Fatalf("watchdog cause=%v", c.Err())
				}
			}
			select {
			case err := <-sent:
				want := net.ErrClosed
				if mandatory {
					want = ErrControlWriteTimeout
				}
				if !errors.Is(err, want) {
					t.Fatalf("interrupted SCON error=%v, want %v", err, want)
				}
			case <-time.After(time.Second):
				t.Fatal("SCON did not return after transport release")
			}
			if closed != nil {
				select {
				case err := <-closed:
					if err != nil {
						t.Fatalf("teardown error=%v", err)
					}
				case <-time.After(time.Second):
					t.Fatal("teardown did not return after transport release")
				}
			}
			select {
			case <-registered:
			case <-time.After(time.Second):
				t.Fatal("registration caller remained blocked")
			}
			select {
			case <-acknowledged:
			case <-time.After(time.Second):
				t.Fatal("Ack remained blocked")
			}
		})
	}
}
