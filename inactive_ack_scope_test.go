package m3ua

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

func TestInactiveAckBlockedRetryEndsBeforeWithdrawalReport(t *testing.T) {
	for _, solicited := range []bool{false, true} {
		t.Run(map[bool]string{false: "unsolicited", true: "solicited"}[solicited], func(t *testing.T) {
			c, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
			c.cfg.TAck = 5 * time.Millisecond
			c.cfg.TAckRetries = 1
			c.cfg.ControlWriteTimeout = 5 * time.Second
			c.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
			transport := newFullSendBuffer()
			c.signalWriter = nil
			c.controlTransport = transport
			c.transportCloser = transport.close
			_, _, done := startObservedRetry(t, c, messages.NewAspActive(nil, params.NewRoutingContext(2), nil), requestAspActive)
			select {
			case <-transport.waiting:
			case <-time.After(time.Second):
				t.Fatal("retry did not block inside transport")
			}
			if solicited {
				req := c.startTAck(messages.NewAspInactive(params.NewRoutingContext(2), nil), requestAspInactive)
				req.stopRetries()
			}
			indications := c.ManagementIndications()
			handled := make(chan error, 1)
			go func() {
				handled <- c.handleAspInactiveAck(messages.NewAspInactiveAck(params.NewRoutingContext(2), nil))
			}()
			select {
			case err := <-handled:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				_ = transport.close()
				t.Fatal("withdrawal did not cancel blocked send")
			}
			if !solicited {
				select {
				case ind := <-indications:
					if ind.Kind != ManagementASPWithdrawal {
						t.Fatal(ind)
					}
				default:
					t.Fatal("withdrawal was not reported")
				}
			}
			close(transport.space)
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("cancelled retry did not finish")
			}
			if len(transport.waitedFrames()) != 0 {
				t.Fatal("canceled ASP Active reached transport after withdrawal completed")
			}
			select {
			case <-c.done:
				t.Fatal("cancelling retry closed association")
			default:
			}
		})
	}
}

func dynamicAppearanceWithdrawalConn(t *testing.T) (*Association, ASKey) {
	t.Helper()
	c, _ := newTestConnWithContexts(t, StateASPActive, RoleASP)
	setInventoryNetworkAppearance(&c.cfg.ApplicationServers, params.NewNetworkAppearance(0))
	key := ASKey{NetworkAppearance: 10, NetworkAppearanceSet: true, RoutingContext: 7, RoutingContextSet: true}
	c.addDynamicASKey(key, RoutingKey{}, false)
	c.cfg.TAck = time.Hour
	c.noteRoutingContextsAcked(nil)
	return c, key
}

func TestInactiveAckUsesDynamicNetworkAppearance(t *testing.T) {
	for _, omitted := range []bool{false, true} {
		t.Run(map[bool]string{false: "explicit", true: "omitted"}[omitted], func(t *testing.T) {
			c, key := dynamicAppearanceWithdrawalConn(t)
			indications := c.ManagementIndications()
			c.startTAck(messages.NewAspActive(nil, params.NewRoutingContext(7), nil), requestAspActive)
			var scope *params.Param
			if !omitted {
				scope = params.NewRoutingContext(7)
			}
			if err := c.handleAspInactiveAck(messages.NewAspInactiveAck(scope, nil)); err != nil {
				t.Fatal(err)
			}
			var reported []ASKey
			select {
			case ind := <-indications:
				for _, key := range ind.ASKeys {
					if key.RoutingContextSet {
						reported = append(reported, key)
					}
				}
				want := []ASKey{key}
				if omitted {
					want = append([]ASKey{c.contextlessASKey(true)}, want...)
				}
				if !reflect.DeepEqual(ind.ASKeys, want) {
					t.Fatalf("withdrawn keys=%+v, want %+v", ind.ASKeys, want)
				}
			default:
				t.Fatal("withdrawal not reported")
			}
			if c.pendingTAck() != 0 || len(c.pendingTAckRoutingContexts(requestAspActive)) != 0 {
				t.Fatal("dynamic activation/early SSNM window survived withdrawal")
			}
			c.signalWriter = func(m messages.M3UA) (int, error) {
				if active, ok := m.(*messages.AspActive); ok {
					c.handleSignals(context.Background(), messages.NewAspActiveAck(active.TrafficModeType, active.RoutingContext, nil))
				}
				return 1, nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := c.ASPActive(ctx, reported...); err != nil {
				t.Fatalf("application activation with reported key: %v", err)
			}
		})
	}
}

func TestNotifyUsesDynamicNetworkAppearance(t *testing.T) {
	c, key := dynamicAppearanceWithdrawalConn(t)
	indications := c.ManagementIndications()
	if err := c.handleNotify(messages.NewNotify(params.NewStatus(params.AsStateActive), nil, params.NewRoutingContext(7), nil)); err != nil {
		t.Fatal(err)
	}
	select {
	case ind := <-indications:
		if !reflect.DeepEqual(ind.ASKeys, []ASKey{key}) {
			t.Fatalf("Notify keys=%+v", ind.ASKeys)
		}
	default:
		t.Fatal("Notify not reported")
	}
}

func TestInactiveAckMixedDuplicateWithdrawsActiveScope(t *testing.T) {
	c, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
	c.cfg.TAck = time.Hour
	c.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
	c.startTAck(messages.NewAspInactive(params.NewRoutingContext(1), nil), requestAspInactive)
	c.handleSignals(context.Background(), messages.NewAspInactiveAck(params.NewRoutingContext(1), nil))
	indications := c.ManagementIndications()
	if err := c.handleAspInactiveAck(messages.NewAspInactiveAck(params.NewRoutingContext(1, 2), nil)); err != nil {
		t.Fatalf("mixed duplicate rejected fresh withdrawal: %v", err)
	}
	if c.routingContextAcked(2) {
		t.Fatal("mixed duplicate left RC2 sendable")
	}
	select {
	case ind := <-indications:
		if !reflect.DeepEqual(ind.RoutingContexts, []uint32{2}) {
			t.Fatalf("scope=%+v", ind)
		}
	default:
		t.Fatal("fresh RC2 withdrawal not reported")
	}
}

func TestInactiveAckDuplicatePreservesApplicationActivation(t *testing.T) {
	c, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
	c.cfg.TAck = time.Hour
	c.noteRoutingContextsAcked(params.NewRoutingContext(1))
	indications := c.ManagementIndications()
	c.handleSignals(context.Background(), messages.NewAspInactiveAck(params.NewRoutingContext(1), nil))
	<-indications
	if err := c.ActivateRoutingContexts(1); err != nil {
		t.Fatal(err)
	}
	c.handleSignals(context.Background(), messages.NewAspInactiveAck(params.NewRoutingContext(1), nil))
	if c.pendingTAck() != 1 {
		t.Fatal("duplicate withdrawal canceled application activation")
	}
	select {
	case ind := <-indications:
		t.Fatalf("duplicate reported again: %+v", ind)
	default:
	}
	c.handleSignals(context.Background(), messages.NewAspActiveAck(nil, params.NewRoutingContext(1), nil))
	if !c.routingContextAcked(1) || c.pendingTAck() != 0 {
		t.Fatal("application activation did not complete")
	}
}

func TestInactiveAckOvertakingStartupPreservesInitialActivation(t *testing.T) {
	c, sent := newTestConnWithContexts(t, StateASPDown, RoleASP, 1, 2)
	c.cfg.TAck = time.Hour
	c.recvStream.Store(0)
	c.muState.Lock()
	c.appliedState = StateASPDown
	c.stateEntered = true
	c.muState.Unlock()
	c.startTAck(messages.NewAspUp(nil, nil), requestAspUp)
	c.handleSignals(context.Background(), messages.NewAspUpAck(nil, nil))
	indications := c.ManagementIndications()
	c.handleSignals(context.Background(), messages.NewAspInactiveAck(params.NewRoutingContext(1), nil))
	if err := c.handleStateUpdate(StateASPInactive); err != nil {
		t.Fatal(err)
	}
	if countType(*sent, "ASP Active") != 1 {
		t.Fatal("inactive-only Ack consumed initial activation of unaffected RC2")
	}
	got := c.pendingTAckRoutingContexts(requestAspActive)
	slices.Sort(got)
	if !reflect.DeepEqual(got, []uint32{1, 2}) {
		t.Fatalf("initial activation=%v", got)
	}
	select {
	case ind := <-indications:
		t.Fatalf("never-active scope reported withdrawn: %+v", ind)
	default:
	}
}

func TestInactiveAckCancellationExplainsSupersession(t *testing.T) {
	c, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
	c.cfg.TAck = time.Hour
	c.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
	req := c.startTAck(messages.NewAspActive(nil, params.NewRoutingContext(1), nil), requestAspActive)
	if err := c.handleAspInactiveAck(messages.NewAspInactiveAck(params.NewRoutingContext(1), nil)); err != nil {
		t.Fatal(err)
	}
	err := c.waitTAck(context.Background(), req)
	if !errors.Is(err, ErrASPProcedureSuperseded) || errors.Is(err, ErrAssociationClosed) {
		t.Fatalf("open-association cancellation error: %v", err)
	}
}

// acceptanceWinsSender models the documented SendMsgContext race: SCTP has
// accepted the complete message as cancellation arrives, so success wins.
type acceptanceWinsSender struct{ *fullSendBuffer }

func (s *acceptanceWinsSender) SendMsgContext(ctx context.Context, frame []byte, opts sctp.SendOptions) (int, error) {
	if opts.NoWait {
		return s.SendMsg(frame, opts)
	}
	s.waiting <- struct{}{}
	<-ctx.Done()
	s.mu.Lock()
	s.waited = append(s.waited, append([]byte(nil), frame...))
	s.mu.Unlock()
	return len(frame), nil
}

func cancelledAcceptedActivation(t *testing.T, role Role) (*Association, <-chan *ManagementIndication) {
	t.Helper()
	c, _ := newTestConnWithContexts(t, StateASPActive, role, 1, 2)
	c.cfg.TAck = 5 * time.Millisecond
	c.cfg.TAckRetries = 1
	c.cfg.ControlWriteTimeout = 5 * time.Second
	if role == RoleIPSP {
		c.cfg.IPSP = &IPSPConfig{ExchangeModel: IPSPExchangeSingle}
		c.cfg.ASPProcedures = &ASPProcedurePolicy{ASPUp: ASPProcedureExplicit, ASPDown: ASPProcedureExplicit, ASPActive: ASPProcedureExplicit, ASPInactive: ASPProcedureExplicit}
		c.noteRoutingContextsActive([]uint32{1, 2})
	}
	c.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
	c.startTAck(messages.NewAspActive(nil, params.NewRoutingContext(2), nil), requestAspActive)
	c.acknowledgeTAck(requestAspActive, params.NewRoutingContext(2))
	transport := &acceptanceWinsSender{newFullSendBuffer()}
	c.signalWriter = nil
	c.controlTransport = transport
	c.transportCloser = transport.close
	_, _, done := startObservedRetry(t, c, messages.NewAspActive(nil, params.NewRoutingContext(2), nil), requestAspActive)
	select {
	case <-transport.waiting:
	case <-time.After(time.Second):
		t.Fatal("retry did not enter blocking transport send")
	}
	indications := c.ManagementIndications()
	if err := c.handleAspInactiveAck(messages.NewAspInactiveAck(params.NewRoutingContext(2), nil)); err != nil {
		t.Fatal(err)
	}
	if ind := <-indications; ind.Kind != ManagementASPWithdrawal {
		t.Fatalf("missing withdrawal before acceptance: %+v", ind)
	}
	if ind := <-indications; ind.Kind != ManagementError {
		t.Fatalf("missing immediate acceptance report: %+v", ind)
	}
	close(transport.space)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("accepted send did not finish")
	}

	return c, indications
}

func TestInactiveAckForInactiveScopeHasNoSideEffects(t *testing.T) {
	for _, role := range []Role{RoleASP, RoleIPSP} {
		for _, state := range []State{StateASPDown, StateASPInactive} {
			t.Run(role.String()+"/"+state.String(), func(t *testing.T) {
				c, sent := newTestConnWithContexts(t, state, role, 1, 2)
				c.cfg.TAck = time.Hour
				c.noteNoRoutingContextsAcked()
				if role == RoleIPSP {
					c.cfg.IPSP = &IPSPConfig{ExchangeModel: IPSPExchangeSingle}
					c.noteRoutingContextsInactive(nil)
				}
				indications := c.ManagementIndications()
				c.handleSignals(context.Background(), messages.NewAspInactiveAck(params.NewRoutingContext(1), nil))
				if c.State() != state {
					t.Fatalf("inactive-only Ack changed state from %v to %v", state, c.State())
				}
				if len(*sent) != 0 || c.pendingTAck() != 0 {
					t.Fatal("inactive-only Ack sent a message or started a request")
				}
				select {
				case ind := <-indications:
					t.Fatalf("inactive-only Ack produced report: %+v", ind)
				default:
				}
			})
		}
	}
}

func TestInactiveAckCancellationFailsBlockedApplicationActivation(t *testing.T) {
	for _, blocking := range []bool{false, true} {
		t.Run(map[bool]string{false: "nonblocking API", true: "waiting API"}[blocking], func(t *testing.T) {
			c, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
			c.cfg.TAck = time.Hour
			c.cfg.ControlWriteTimeout = 5 * time.Second
			c.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
			transport := newFullSendBuffer()
			c.signalWriter = nil
			c.controlTransport = transport
			c.transportCloser = transport.close
			result := make(chan error, 1)
			go func() {
				if blocking {
					result <- c.ASPActive(context.Background(), associationConfigASKey(c.cfg, 1))
				} else {
					result <- c.ActivateRoutingContexts(1)
				}
			}()
			select {
			case <-transport.waiting:
			case <-time.After(time.Second):
				t.Fatal("application write did not block")
			}
			if err := c.handleAspInactiveAck(messages.NewAspInactiveAck(params.NewRoutingContext(1), nil)); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-result:
				if !errors.Is(err, ErrASPProcedureSuperseded) || errors.Is(err, ErrAssociationClosed) {
					t.Fatalf("cancelled activation returned %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("cancelled application call did not return")
			}
		})
	}
}

// lockedSendBuffer models the transport send lock, which even NoWait must
// acquire. SendMsgContext can cancel that lock wait at the pinned version.
type lockedSendBuffer struct{ *fullSendBuffer }

func (s *lockedSendBuffer) SendMsg(frame []byte, opts sctp.SendOptions) (int, error) {
	return s.waitSend(frame, opts.Info)
}
func (s *lockedSendBuffer) SendMsgContext(ctx context.Context, frame []byte, opts sctp.SendOptions) (int, error) {
	return s.waitSendContext(ctx, frame, opts.Info)
}

func TestInactiveAckCancelsRetryWaitingForTransportLock(t *testing.T) {
	c, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
	c.cfg.TAck = 5 * time.Millisecond
	c.cfg.TAckRetries = 1
	c.cfg.ControlWriteTimeout = time.Second
	c.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
	transport := &lockedSendBuffer{newFullSendBuffer()}
	c.signalWriter = nil
	c.controlTransport = transport
	c.transportCloser = transport.close
	_, _, done := startObservedRetry(t, c, messages.NewAspActive(nil, params.NewRoutingContext(2), nil), requestAspActive)
	select {
	case <-transport.waiting:
	case <-time.After(time.Second):
		t.Fatal("retry did not wait for transport lock")
	}
	handled := make(chan error, 1)
	go func() {
		handled <- c.handleAspInactiveAck(messages.NewAspInactiveAck(params.NewRoutingContext(2), nil))
	}()
	select {
	case err := <-handled:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		_ = transport.close()
		t.Fatal("withdrawal did not cancel transport lock wait")
	}
	close(transport.space)
	<-done
	if len(transport.waitedFrames()) != 0 {
		t.Fatal("cancelled lock waiter transmitted ASP Active")
	}
}

// joinedCancellationSender keeps an interrupted send alive until the test
// releases it, proving that cancellation alone is not the reporting boundary.
type joinedCancellationSender struct {
	*fullSendBuffer
	cancelled, release chan struct{}
}

func (s *joinedCancellationSender) SendMsgContext(ctx context.Context, frame []byte, opts sctp.SendOptions) (int, error) {
	if opts.NoWait {
		return s.noWaitSend(frame, opts.Info)
	}
	s.waiting <- struct{}{}
	<-ctx.Done()
	close(s.cancelled)
	<-s.release
	return 0, ctx.Err()
}
func TestInactiveAckJoinsCancelledSendBeforeReporting(t *testing.T) {
	c, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
	c.cfg.TAck = 5 * time.Millisecond
	c.cfg.TAckRetries = 1
	c.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
	transport := &joinedCancellationSender{fullSendBuffer: newFullSendBuffer(), cancelled: make(chan struct{}), release: make(chan struct{})}
	c.signalWriter = nil
	c.controlTransport = transport
	c.transportCloser = transport.close
	_, _, done := startObservedRetry(t, c, messages.NewAspActive(nil, params.NewRoutingContext(2), nil), requestAspActive)
	<-transport.waiting
	management := c.ManagementIndications()
	handled := make(chan error, 1)
	go func() {
		handled <- c.handleAspInactiveAck(messages.NewAspInactiveAck(params.NewRoutingContext(2), nil))
	}()
	<-transport.cancelled
	var early bool
	select {
	case <-management:
		early = true
	case <-time.After(50 * time.Millisecond):
	}
	close(transport.release)
	if err := <-handled; err != nil {
		t.Fatal(err)
	}
	<-done
	if early {
		t.Fatal("withdrawal report overtook completion of the cancelled send")
	}
	select {
	case ind := <-management:
		if ind.Kind != ManagementASPWithdrawal {
			t.Fatalf("withdrawal report=%+v", ind)
		}
	default:
		t.Fatal("missing withdrawal report after cancelled call returned")
	}
}

func TestInactiveAckCancelsRetryBeforeDrainingTraffic(t *testing.T) {
	c, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
	c.cfg.TAck = 5 * time.Millisecond
	c.cfg.TAckRetries = 1
	c.cfg.ControlWriteTimeout = 5 * time.Second
	c.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
	transport := newFullSendBuffer()
	c.signalWriter = nil
	c.controlTransport = transport
	c.transportCloser = transport.close
	req, _, done := startObservedRetry(t, c, messages.NewAspActive(nil, params.NewRoutingContext(2), nil), requestAspActive)
	<-transport.waiting
	// An admitted transfer may itself be waiting behind the blocked control
	// writer. Its barrier cannot be drained before that writer is cancelled.
	c.aspTransferMu.RLock()
	handled := make(chan error, 1)
	go func() {
		handled <- c.handleAspInactiveAck(messages.NewAspInactiveAck(params.NewRoutingContext(2), nil))
	}()
	var cancelled bool
	select {
	case <-req.stop:
		cancelled = true
	case <-time.After(time.Second):
	}
	c.aspTransferMu.RUnlock()
	if !cancelled {
		_ = transport.close()
	}
	if err := <-handled; err != nil {
		t.Fatal(err)
	}
	<-done
	if !cancelled {
		t.Fatal("withdrawal waited for traffic drain before cancelling blocked Active")
	}
}

func TestInactiveAckContextlessWithdrawalPreservesFirstDynamicActivation(t *testing.T) {
	c, key := dynamicAppearanceWithdrawalConn(t)
	c.noteRoutingContextsUnacked(params.NewRoutingContext(7))
	indications := c.ManagementIndications()
	if err := c.ActivateRoutingContexts(7); err != nil {
		t.Fatal(err)
	}
	c.handleSignals(context.Background(), messages.NewAspInactiveAck(nil, nil))
	if c.pendingTAck() != 1 || c.contextlessASAcked() {
		t.Fatal("contextless withdrawal cancelled the never-active numeric AS")
	}
	select {
	case ind := <-indications:
		if !reflect.DeepEqual(ind.ASKeys, []ASKey{c.contextlessASKey(true)}) {
			t.Fatalf("withdrawal included never-active dynamic AS: %+v", ind)
		}
	default:
		t.Fatal("contextless withdrawal was not reported")
	}
	c.handleSignals(context.Background(), messages.NewAspActiveAck(nil, params.NewRoutingContext(key.RoutingContext), nil))
	if c.pendingTAck() != 0 || !c.routingContextAcked(7) || c.contextlessASAcked() {
		t.Fatal("independent first activation did not complete")
	}
}

func TestASPDownCancellationExplainsActivationSupersession(t *testing.T) {
	c, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
	c.cfg.TAck = time.Hour
	c.recvStream.Store(0)
	c.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
	req := c.startTAck(messages.NewAspActive(nil, params.NewRoutingContext(1), nil), requestAspActive)
	c.startTAck(messages.NewAspDown(nil), requestAspDown)
	c.handleSignals(context.Background(), messages.NewAspDownAck(nil))
	err := c.waitTAck(context.Background(), req)
	if !errors.Is(err, ErrASPProcedureSuperseded) || errors.Is(err, ErrAssociationClosed) {
		t.Fatalf("Down cancelled activation on an open association with %v", err)
	}
	select {
	case <-c.done:
		t.Fatal("Down confirmation closed association")
	default:
	}
}

func TestInactiveAckDoesNotWaitForUnsubmittedRetry(t *testing.T) {
	c, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
	c.cfg.TAck = 5 * time.Millisecond
	c.cfg.TAckRetries = 1
	c.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
	transport := newFullSendBuffer()
	transport.firstErr = nil
	c.signalWriter = nil
	c.controlTransport = transport
	c.transportCloser = transport.close
	c.sendMu.Lock()
	_, entered, done := startObservedRetry(t, c, messages.NewAspActive(nil, params.NewRoutingContext(2), nil), requestAspActive)
	<-entered
	handled := make(chan error, 1)
	go func() {
		handled <- c.handleAspInactiveAck(messages.NewAspInactiveAck(params.NewRoutingContext(2), nil))
	}()
	var completed bool
	select {
	case err := <-handled:
		if err != nil {
			c.sendMu.Unlock()
			t.Fatal(err)
		}
		completed = true
	case <-time.After(time.Second):
	}
	c.sendMu.Unlock()
	if !completed {
		if err := <-handled; err != nil {
			t.Fatal(err)
		}
	}
	<-done
	if !completed {
		t.Fatal("withdrawal waited for a retry that had not entered the transport")
	}
}

func TestInactiveAckCancellationFailsAcceptedApplicationWrite(t *testing.T) {
	c, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
	c.cfg.TAck = time.Hour
	c.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
	transport := &acceptanceWinsSender{newFullSendBuffer()}
	c.signalWriter = nil
	c.controlTransport = transport
	c.transportCloser = transport.close
	result := make(chan error, 1)
	go func() { result <- c.ActivateRoutingContexts(1) }()
	<-transport.waiting
	if err := c.handleAspInactiveAck(messages.NewAspInactiveAck(params.NewRoutingContext(1), nil)); err != nil {
		t.Fatal(err)
	}
	if err := <-result; !errors.Is(err, ErrASPProcedureSuperseded) {
		t.Fatalf("accepted but cancelled application write returned %v", err)
	}
}

func TestOmittedInactiveAckConfirmsContextlessAndNumericRequest(t *testing.T) {
	c, _ := dynamicAppearanceWithdrawalConn(t)
	c.startTAck(messages.NewAspInactive(nil, nil), requestAspInactive)
	indications := c.ManagementIndications()
	if err := c.handleAspInactiveAck(messages.NewAspInactiveAck(nil, nil)); err != nil {
		t.Fatal(err)
	}
	if c.pendingTAck() != 0 || c.contextlessASAcked() || c.routingContextAcked(7) {
		t.Fatal("omitted Ack did not complete contextless and numeric scopes")
	}
	select {
	case ind := <-indications:
		t.Fatalf("own omitted request reported unsolicited: %+v", ind)
	default:
	}
}

func TestIPSPSingleExchangeWithdrawsContextlessASBesideDynamicContext(t *testing.T) {
	c, _ := newTestConnWithContexts(t, StateASPActive, RoleIPSP)
	c.cfg.IPSP = &IPSPConfig{ExchangeModel: IPSPExchangeSingle}
	c.cfg.TAck = time.Hour
	c.noteRoutingContextsActive(nil)
	c.addDynamicASKey(ASKey{NetworkAppearance: 10, NetworkAppearanceSet: true, RoutingContext: 7, RoutingContextSet: true}, RoutingKey{}, false)
	c.startTAck(messages.NewAspActive(nil, params.NewRoutingContext(7), nil), requestAspActive)
	indications := c.ManagementIndications()
	c.handleSignals(context.Background(), messages.NewAspInactiveAck(nil, nil))
	if c.State() != StateASPInactive || c.activeForASKey(c.contextlessASKey(false)) {
		t.Fatal("omitted Ack left the contextless IPSP AS active beside an inactive dynamic scope")
	}
	if c.pendingTAck() != 1 {
		t.Fatal("contextless withdrawal cancelled first activation of numeric scope")
	}
	select {
	case ind := <-indications:
		if !reflect.DeepEqual(ind.ASKeys, []ASKey{c.contextlessASKey(false)}) {
			t.Fatalf("withdrawal scope=%+v", ind)
		}
	default:
		t.Fatal("contextless IPSP withdrawal was not reported")
	}
}

func TestOmittedInactiveAckPreservesExplicitDeactivation(t *testing.T) {
	c, sent := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
	c.cfg.TAck = time.Hour
	c.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
	inactive := c.startTAck(messages.NewAspInactive(params.NewRoutingContext(2), nil), requestAspInactive)
	active := c.startTAck(messages.NewAspActive(nil, params.NewRoutingContext(1), nil), requestAspActive)
	indications := c.ManagementIndications()
	if err := c.handleAspInactiveAck(messages.NewAspInactiveAck(nil, nil)); !errors.Is(err, ErrMissingRoutingContext) {
		t.Fatalf("omitted Ack beside explicit Inactive must be rejected: %v", err)
	}
	c.handleSignals(context.Background(), messages.NewAspInactiveAck(nil, nil))
	select {
	case err := <-c.errChan:
		if !errors.Is(err, ErrMissingRoutingContext) {
			t.Fatal(err)
		}
		if err := c.handleErrors(err); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatal("ambiguous omitted Ack did not report Missing Parameter")
	}
	if c.State() != StateASPActive || !c.routingContextAcked(1) || !c.routingContextAcked(2) || c.pendingTAck() != 2 {
		t.Fatal("ambiguous omitted Ack changed traffic or pending procedures")
	}
	for _, request := range []*pendingRequest{inactive, active} {
		select {
		case <-request.stop:
			t.Fatal("ambiguous omitted Ack cancelled a pending procedure")
		default:
		}
	}
	select {
	case ind := <-indications:
		t.Fatalf("ambiguous omitted Ack reported withdrawal: %+v", ind)
	default:
	}
	if len(*sent) != 1 {
		t.Fatalf("ambiguous Ack sent %d responses", len(*sent))
	}
	response, ok := (*sent)[0].(*messages.Error)
	if !ok || response.ErrorCode.ErrorCode() != params.ErrMissingParameter {
		t.Fatalf("ambiguous Ack response = %+v", (*sent)[0])
	}
}

// RFC 4666 §4.3.4.4 and §4.5.2.2: changing an active AS member changes
// the transport contribution of that member to the shared destination.
func TestInactiveAckReactivationRefreshesTransportProjection(t *testing.T) {
	cfg := validASPConfig()
	useSignallingGateways(cfg, "sg-a")
	e, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: cfg})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	identity := SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}
	a, _ := attachMultiScopeASPAssociation(t, e, identity, 7, 1)
	b, _ := attachMultiScopeASPAssociation(t, e, identity, 7, 1)
	a.cfg.TAck = time.Hour
	setAssociationTransport(t, e, a, 0)
	setAssociationTransport(t, e, b, 3)
	const pc = uint32(0x123456)
	applyASPDAVA(t, a, 7, 1, pc, 0)
	destination := MTPDestination{MTPRoute: "sccp-a", PointCode: pc}
	if status, known := e.MTPDestinationStatus(destination); !known || status.Congested {
		t.Fatalf("clear active member did not bound transport pressure: %+v", status)
	}
	drainMTPIndications(e.MTPIndications())
	a.handleSignals(context.Background(), messages.NewAspInactiveAck(params.NewRoutingContext(1), nil))
	if status, known := e.MTPDestinationStatus(destination); !known || !status.Congested || status.CongestionLevel != 3 {
		t.Fatalf("withdrawal did not refresh transport projection: %+v", status)
	}
	requireMTPIndication(t, e.MTPIndications(), MTPStatusIndication, "sccp-a", pc, 0, DestinationAvailable, true, 3, true)
	if a.pendingTAck() != 0 {
		t.Fatal("withdrawal started automatic activation")
	}
	if err := a.ActivateRoutingContexts(1); err != nil {
		t.Fatal(err)
	}
	a.handleSignals(context.Background(), messages.NewAspActiveAck(nil, params.NewRoutingContext(1), nil))
	if status, known := e.MTPDestinationStatus(destination); !known || status.Congested {
		t.Fatalf("application reactivation did not refresh transport projection: %+v", status)
	}
	requireMTPIndication(t, e.MTPIndications(), MTPStatusIndication, "sccp-a", pc, 0, DestinationAvailable, false, 0, true)
}
