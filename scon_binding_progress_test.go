// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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

func TestRKMResultsDoNotParkDispatcherBehindUnrelatedSCON(t *testing.T) {
	for _, local := range []bool{false, true} {
		for _, mode := range []string{"late_registration", "late_deregistration", "on_time_registration", "on_time_deregistration", "late_deregistration_same_binding", "on_time_deregistration_same_binding"} {
			t.Run(fmt.Sprintf("local_%t/%s", local, mode), func(t *testing.T) {
				c := newSCONBindingAssociation(t, local)
				key := ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContext: 2, RoutingContextSet: true}
				deregister := strings.Contains(mode, "deregistration")
				onTime := strings.HasPrefix(mode, "on_time")
				affected := strings.HasSuffix(mode, "same_binding")
				if deregister {
					c.addDynamicASKey(key, testRoutingKey(7, 200, 3), local)
				}
				written := make(chan messages.M3UA, 1)
				c.signalWriter = func(m messages.M3UA) (int, error) { written <- m; return m.MarshalLen(), nil }
				var requestContext context.Context
				var stopRequest context.CancelFunc
				if onTime {
					requestContext, stopRequest = context.WithCancel(t.Context())
				} else {
					requestContext, stopRequest = context.WithTimeout(t.Context(), time.Second)
				}
				defer stopRequest()
				requested := make(chan error, 1)
				go func() {
					if deregister {
						_, err := c.DeregisterApplicationServers(requestContext, key)
						requested <- err
					} else {
						_, err := c.RegisterRoutingKeys(requestContext, RoutingKeyRegistration{RoutingKey: testRoutingKey(7, 200, 3)})
						requested <- err
					}
				}()
				var response messages.M3UA
				select {
				case request := <-written:
					if deregister {
						response = messages.NewDeregistrationResponse(params.NewDeregistrationResult(params.NewDeregResultPayload(params.NewRoutingContext(2), params.NewDeregistrationStatus(params.SuccessfullyDeregistered))))
					} else {
						payload, err := request.(*messages.RegistrationRequest).RoutingKeys[0].RoutingKey()
						if err != nil {
							t.Fatal(err)
						}
						response = messages.NewRegistrationResponse(params.NewRegistrationResult(params.NewRegistrationResultPayload(payload.LocalRoutingKeyIdentifier, params.NewRegistrationStatus(params.SuccessfullyRegistered), params.NewRoutingContext(2))))
					}
				case err := <-requested:
					t.Fatalf("request returned before transport submission: %v", err)
				case <-time.After(2 * time.Second):
					t.Fatal("RKM request was not submitted")
				}
				if !onTime {
					select {
					case err := <-requested:
						if !errors.Is(err, context.DeadlineExceeded) {
							t.Fatalf("RKM timeout = %v", err)
						}
					case <-time.After(2 * time.Second):
						t.Fatal("submitted RKM request did not time out")
					}
				}
				buffer := newFullSendBuffer()
				defer func() { _ = buffer.close() }()
				c.signalWriter = nil
				c.controlTransport = buffer
				c.transportCloser = buffer.close
				deadline := time.Now().Add(time.Hour)
				c.applicationWriteDeadline.Store(&deadline)
				targetRC := uint32(1)
				if affected {
					targetRC = 2
					c.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
				}
				sent := make(chan error, 1)
				go func() {
					_, err := c.WriteSignal(messages.NewSignallingCongestion(params.NewNetworkAppearance(7), params.NewRoutingContext(targetRC), params.NewAffectedPointCode(100), nil, params.NewCongestionIndications(2), nil))
					sent <- err
				}()
				select {
				case <-buffer.waiting:
				case <-time.After(2 * time.Second):
					t.Fatal("SCON did not park with its application deadline")
				}
				// This is the same admission flag SetWriteDeadline clears, after the
				// transport has entered its buffer wait. No timer orders this race.
				c.applicationWriteDeadline.Store(nil)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				incoming := make(chan inbound)
				dispatchDone := make(chan struct{})
				go func() { defer close(dispatchDone); c.dispatchLoop(ctx, incoming) }()
				defer func() { cancel(); _ = buffer.close(); <-dispatchDone }()
				handoff := make(chan struct{})
				go func() {
					defer close(handoff)
					for _, message := range []messages.M3UA{response, inboundData(1, "dispatcher-progress")} {
						raw, err := message.MarshalBinary()
						if err != nil {
							return
						}
						stream := uint16(0)
						if _, ok := message.(*messages.Data); ok {
							stream = 1
						}
						select {
						case incoming <- newInboundMessage(raw, sctp.RcvInfo{Stream: stream, PPID: M3UAPPID}, time.Now()):
						case <-ctx.Done():
							return
						}
					}
				}()
				select {
				case data := <-c.dataChan:
					if string(data.ProtocolData.Data) != "dispatcher-progress" {
						t.Fatalf("DATA changed: %+v", data)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("dispatcher did not deliver DATA after RKM result with cleared application deadline")
				}
				<-handoff
				if onTime {
					select {
					case err := <-requested:
						if err != nil {
							t.Fatal(err)
						}
					case <-time.After(2 * time.Second):
						t.Fatal("requester result application waited for unrelated parked SCON")
					}
				}
				if _, ok := c.dynamicASKey(2, local); ok == deregister {
					t.Fatalf("RKM result not committed: deregister=%t present=%t", deregister, ok)
				}
				if affected {
					select {
					case err := <-sent:
						if !errors.Is(err, ErrRoutingContextNotActive) {
							t.Fatalf("retired SCON = %v", err)
						}
					case <-time.After(2 * time.Second):
						t.Fatal("retired SCON did not cancel")
					}
					if len(buffer.waitedFrames()) != 0 {
						t.Fatal("retired SCON submitted")
					}
				} else {
					select {
					case err := <-sent:
						t.Fatalf("unrelated binding change refused current SCON as stale: %v", err)
					default:
					}
					close(buffer.space)
					select {
					case err := <-sent:
						if err != nil {
							t.Fatalf("current SCON refused as stale: %v", err)
						}
					case <-time.After(2 * time.Second):
						t.Fatal("current SCON did not resume")
					}
					if len(buffer.waitedFrames()) != 1 {
						t.Fatal("current SCON was not submitted exactly once")
					}
				}
			})
		}
	}
}

type settlingSCONBuffer struct {
	*fullSendBuffer
	cancelled chan struct{}
	settle    chan struct{}
	accepted  bool
	once      sync.Once
}

func (b *settlingSCONBuffer) SendMsgContext(ctx context.Context, frame []byte, options sctp.SendOptions) (int, error) {
	if options.NoWait {
		return b.fullSendBuffer.SendMsgContext(ctx, frame, options)
	}
	b.waiting <- struct{}{}
	select {
	case <-ctx.Done():
		b.once.Do(func() { close(b.cancelled) })
		select {
		case <-b.settle:
		case <-b.closed:
			return 0, net.ErrClosed
		}
		if !b.accepted {
			return 0, ctx.Err()
		}
		b.mu.Lock()
		b.waited = append(b.waited, bytes.Clone(frame))
		b.mu.Unlock()
		return len(frame), nil
	case <-b.space:
		return b.fullSendBuffer.SendMsgContext(ctx, frame, options)
	case <-b.closed:
		return 0, net.ErrClosed
	}
}

// Join a real channel/lock wait in the changing procedure, or observe that it
// completed. A deliberately broken fence must reach completion before this
// helper returns, instead of winning a scheduler-dependent nonblocking select.
func awaitSCONChangeStage(t *testing.T, changed <-chan struct{}) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-changed:
			return
		default:
		}
		for _, stack := range goroutinesBlockedIn("beginSCONBindingChange") {
			if strings.Contains(stack, "[chan receive]") {
				return
			}
		}
		runtime.Gosched()
	}
	t.Fatal("binding change neither settled nor reached its cancellation join")
}

func TestSCONBindingChangeCancelsOnlyAffectedSubmission(t *testing.T) {
	for _, role := range []Role{RoleASP, RoleSGP, RoleIPSP} {
		local := role == RoleIPSP
		for _, mandatory := range []bool{false, true} {
			for _, operation := range []string{"replace", "remove", "remove_version", "wrong_version", "absent", "other_direction", "accepted_before_cancel"} {
				t.Run(fmt.Sprintf("role_%s/mandatory_%t/%s", role, mandatory, operation), func(t *testing.T) {
					c := newSCONBindingAssociation(t, local)
					c.role = role
					c.endpoint = methodEndpointForTest(t, role, uint8(CongestionNationalWithoutPriorities), 3)
					key := ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContext: 2, RoutingContextSet: true}
					c.addDynamicASKey(key, testRoutingKey(7, 200, 3), local)
					c.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
					if role == RoleSGP {
						c.noteRoutingContextsActive([]uint32{1, 2})
					}
					targetRC := uint32(2)
					if operation == "absent" {
						targetRC = 1
					}
					token := c.queueCongestionBinding(ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContext: targetRC, RoutingContextSet: true}, WireScope{})
					version := c.dynamicASKeyVersionFor(2, local)
					buffer := &settlingSCONBuffer{fullSendBuffer: newFullSendBuffer(), cancelled: make(chan struct{}), settle: make(chan struct{}), accepted: operation == "accepted_before_cancel"}
					defer func() { _ = buffer.close() }()
					c.signalWriter = nil
					c.controlTransport = buffer
					c.transportCloser = buffer.close
					c.cfg.ControlWriteTimeout = time.Hour
					deadline := time.Now().Add(time.Hour)
					c.applicationWriteDeadline.Store(&deadline)
					sent := make(chan error, 1)
					go func() {
						message := messages.NewSignallingCongestion(params.NewNetworkAppearance(7), params.NewRoutingContext(targetRC), params.NewAffectedPointCode(100), nil, params.NewCongestionIndications(2), nil)
						var err error
						if mandatory {
							_, err = c.writeControl(message)
						} else {
							_, err = c.WriteSignal(message)
						}
						sent <- err
					}()
					select {
					case <-buffer.waiting:
					case <-time.After(2 * time.Second):
						t.Fatal("SCON did not enter its buffer wait")
					}
					c.applicationWriteDeadline.Store(nil)
					if !c.muDynamicASKeys.TryLock() {
						t.Fatal("short dynamic-key lock retained across parked SCON")
					}
					c.muDynamicASKeys.Unlock()
					changed := make(chan struct{})
					go func() {
						defer close(changed)
						switch operation {
						case "replace", "accepted_before_cancel":
							c.addDynamicASKey(key, testRoutingKey(7, 201, 3), local)
						case "remove":
							c.removeDynamicASKey(2, local)
						case "remove_version":
							c.removeDynamicASKeyVersion(2, local, version)
						case "wrong_version":
							c.removeDynamicASKeyVersion(2, local, version+1)
						case "absent":
							c.removeDynamicASKeyVersion(1, local, 0)
						case "other_direction":
							c.addDynamicASKey(key, testRoutingKey(7, 201, 3), !local)
						}
					}()
					affected := operation == "replace" || operation == "remove" || operation == "remove_version" || operation == "accepted_before_cancel"
					if affected {
						select {
						case <-buffer.cancelled:
						case <-time.After(2 * time.Second):
							t.Fatal("binding change did not cancel its parked SCON")
						}
						awaitSCONChangeStage(t, changed)
						select {
						case <-changed:
							t.Fatal("binding changed before cancellation settled")
						default:
						}
						if got := c.dynamicASKeyVersionFor(2, local); got != version {
							t.Fatal("registration committed before transport completion")
						}
						close(buffer.settle)
					}
					select {
					case <-changed:
					case <-time.After(2 * time.Second):
						t.Fatal("binding change waited for peer progress")
					}
					if affected {
						select {
						case err := <-sent:
							if buffer.accepted {
								if err != nil {
									t.Fatalf("known acceptance lost to cancellation: %v", err)
								}
							} else if !errors.Is(err, ErrRoutingContextNotActive) {
								t.Fatalf("cancelled binding result=%v", err)
							}
						case <-time.After(2 * time.Second):
							t.Fatal("cancelled SCON did not settle")
						}
						if !buffer.accepted && len(buffer.waitedFrames()) != 0 {
							t.Fatal("cancelled retired SCON was submitted")
						}
						old, err := c.beginSCONSubmission([]queueCongestionBinding{token})
						if old != nil {
							c.finishSCONSubmission(old)
						}
						if !errors.Is(err, ErrRoutingContextNotActive) {
							t.Fatalf("retired captured token accepted: %v", err)
						}
					} else {
						select {
						case <-buffer.cancelled:
							t.Fatal("unrelated or no-op change cancelled current SCON")
						default:
						}
						select {
						case err := <-sent:
							t.Fatalf("current SCON refused as stale: %v", err)
						default:
						}
						close(buffer.space)
						select {
						case err := <-sent:
							if err != nil {
								t.Fatal(err)
							}
						case <-time.After(2 * time.Second):
							t.Fatal("current SCON did not resume")
						}
					}
					// A fresh submission on the current binding must remain usable. For a
					// removal, re-register it with a new version rather than reviving a token.
					if _, ok := c.dynamicASKey(2, local); !ok {
						c.addDynamicASKey(key, testRoutingKey(7, 201, 3), local)
					}
					c.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
					if role == RoleSGP {
						c.noteRoutingContextsActive([]uint32{1, 2})
					}
					buffer.mu.Lock()
					buffer.firstErr = nil
					buffer.mu.Unlock()
					_, err := c.WriteSignal(messages.NewSignallingCongestion(params.NewNetworkAppearance(7), params.NewRoutingContext(2), params.NewAffectedPointCode(100), nil, params.NewCongestionIndications(2), nil))
					if err != nil {
						t.Fatalf("current binding refused as stale: %v", err)
					}
				})
			}
		}
	}
}

func newSCONBindingAssociation(t *testing.T, local bool) *Association {
	t.Helper()
	c, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
	c.rkmRequestGate = make(chan struct{}, 1)
	setInventoryNetworkAppearance(&c.cfg.ApplicationServers, params.NewNetworkAppearance(7))
	if local {
		c.role = RoleIPSP
		c.cfg.IPSP = &IPSPConfig{ExchangeModel: IPSPExchangeDouble, ASPSMExchange: IPSPASPSMExchangeDouble,
			TrafficToLocal: &IPSPTrafficConfig{ApplicationServers: c.cfg.ApplicationServers},
			TrafficToPeer:  &IPSPTrafficConfig{ApplicationServers: []ASConfig{{ASKey: ASKey{NetworkAppearance: 9, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true}, TrafficMode: params.TrafficModeLoadshare}}}}
		c.cfg.ApplicationServers = nil
		c.freezeTrafficModePolicies()
		c.setIPSPState(IPSPState{TrafficToLocal: StateASPActive, TrafficToPeer: StateASPActive})
	}
	c.endpoint = methodEndpointForTest(t, c.role, uint8(CongestionNationalWithoutPriorities), 3)
	c.noteRoutingContextsAcked(params.NewRoutingContext(1))
	return c
}

// A queued sender can own sendMu while a change is committing. The change
// must complete with only the short key locks, and the sender must recheck its
// captured token after the gate opens.
func TestSCONSubmissionWaitsForBindingCommitWithoutBlockingIt(t *testing.T) {
	for _, replace := range []bool{false, true} {
		t.Run(fmt.Sprintf("replace_%t", replace), func(t *testing.T) {
			c := newSCONBindingAssociation(t, false)
			key := ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContext: 2, RoutingContextSet: true}
			c.addDynamicASKey(key, testRoutingKey(7, 200, 3), false)
			binding := c.queueCongestionBinding(key, WireScope{})
			// Hold the real change gate at its commit boundary. No transport is in
			// flight here; both the old and fresh token cases exercise admission.
			release, changing := c.beginSCONBindingChange(2, false, false, nil)
			if !changing {
				t.Fatal("binding change was not admitted")
			}
			var once sync.Once
			commit := func() {
				once.Do(func() {
					if replace {
						c.muDynamicASKeys.Lock()
						c.dynamicPeerASKeyVersions[2] = c.nextDynamicASKeyVersionLocked()
						c.muDynamicASKeys.Unlock()
					}
					release()
				})
			}
			defer commit()
			buffer := newFullSendBuffer()
			buffer.firstErr = nil
			c.signalWriter, c.controlTransport = nil, buffer
			message := messages.NewSignallingCongestion(params.NewNetworkAppearance(7), params.NewRoutingContext(2), params.NewAffectedPointCode(100), nil, params.NewCongestionIndications(2), nil)
			frame, err := message.MarshalBinary()
			if err != nil {
				t.Fatal(err)
			}
			sent := make(chan error, 1)
			go func() { _, err := c.writeControlFrame(frame, c.sctpInfo, nil, nil, binding); sent <- err }()
			deadline := time.Now().Add(2 * time.Second)
			waiting := false
			for time.Now().Before(deadline) {
				select {
				case err := <-sent:
					t.Fatalf("SCON passed an uncommitted binding change: %v", err)
				default:
				}
				for _, stack := range goroutinesBlockedIn("beginSCONSubmission") {
					if strings.Contains(stack, "[chan receive]") {
						waiting = true
						break
					}
				}
				if waiting {
					break
				}
				runtime.Gosched()
			}
			if !waiting {
				t.Fatal("SCON did not reach its binding change gate")
			}
			if c.sendMu.TryLock() {
				c.sendMu.Unlock()
				t.Fatal("gate fixture did not retain sendMu")
			}
			// These locks may be needed by the monitor, Ack claim and teardown.
			// Taking them while the sender owns sendMu must remain possible.
			if !c.muState.TryLock() {
				t.Fatal("binding gate retained muState")
			}
			c.muState.Unlock()
			if !c.tack.mu.TryLock() {
				t.Fatal("binding gate retained tack.mu")
			}
			c.tack.mu.Unlock()
			if !c.muStateEvent.TryLock() {
				t.Fatal("binding gate retained muStateEvent")
			}
			c.muStateEvent.Unlock()
			commit()
			select {
			case err := <-sent:
				if replace {
					if !errors.Is(err, ErrRoutingContextNotActive) {
						t.Fatalf("retired queued SCON = %v", err)
					}
				} else if err != nil {
					t.Fatalf("current queued SCON refused as stale: %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("binding commit could not release sender holding sendMu")
			}
			buffer.mu.Lock()
			attempts := len(buffer.attempted)
			buffer.mu.Unlock()
			want := 1
			if replace {
				want = 0
			}
			if attempts != want {
				t.Fatalf("transport attempts = %d, want %d", attempts, want)
			}
		})
	}
}
