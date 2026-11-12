package m3ua

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func pendingApplicationActivation(t *testing.T, c *Association, ctx context.Context) (*pendingRequest, <-chan error) {
	t.Helper()
	written := make(chan messages.M3UA, 4)
	c.signalWriter = func(message messages.M3UA) (int, error) {
		written <- message
		return message.MarshalLen(), nil
	}
	done := make(chan error, 1)
	go func() { done <- c.ASPActive(ctx, associationConfigASKey(c.cfg, 1)) }()
	select {
	case <-written:
	case <-time.After(time.Second):
		t.Fatal("application activation did not write")
	}
	// The writer owns retryMu until its initial transport call has returned.
	c.tack.retryMu.Lock()
	defer c.tack.retryMu.Unlock()
	return applicationActiveRequest(t, c), done
}

func applicationActiveRequest(t *testing.T, c *Association) *pendingRequest {
	t.Helper()
	c.tack.mu.Lock()
	defer c.tack.mu.Unlock()
	for _, request := range c.tack.pending {
		if request.kind == requestAspActive {
			return request
		}
	}
	t.Fatal("application Active request is not pending")
	return nil
}

func assertApplicationActivationFailure(t *testing.T, ind *ManagementIndication, key ASKey) {
	t.Helper()
	if ind.Kind != ManagementError || !errors.Is(ind.Cause, ErrASPProcedureSuperseded) ||
		!strings.HasPrefix(ind.Description, "ASP Active failed:") || !reflect.DeepEqual(ind.ASKeys, []ASKey{key}) {
		t.Fatalf("application failure indication=%+v", ind)
	}
}

func TestConfirmedInactiveReportsSupersededApplicationActivation(t *testing.T) {
	testConfirmedWithdrawalReportsApplicationFailure(t, false)
}

func TestConfirmedDownReportsSupersededApplicationActivation(t *testing.T) {
	testConfirmedWithdrawalReportsApplicationFailure(t, true)
}

func testConfirmedWithdrawalReportsApplicationFailure(t *testing.T, down bool) {
	t.Helper()
	c, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
	c.cfg.TAck = time.Hour
	c.noteRoutingContextsAcked(params.NewRoutingContext(1))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	management := c.ManagementIndications()
	_, done := pendingApplicationActivation(t, c, ctx)
	if down {
		if _, err := c.initiateASPDown(); err != nil {
			t.Fatal(err)
		}
		c.recvStream.Store(0)
		if err := c.handleAspDownAck(messages.NewAspDownAck(nil)); err != nil {
			t.Fatal(err)
		}
	} else {
		if err := c.DeactivateRoutingContexts(1); err != nil {
			t.Fatal(err)
		}
		if err := c.handleAspInactiveAck(messages.NewAspInactiveAck(params.NewRoutingContext(1), nil)); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrASPProcedureSuperseded) {
			t.Fatalf("application activation returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled application activation did not return")
	}
	select {
	case ind := <-management:
		assertApplicationActivationFailure(t, ind, associationConfigASKey(c.cfg, 1))
	default:
		t.Fatal("confirmed withdrawal suppressed the application's only failure indication")
	}
	select {
	case ind := <-management:
		t.Fatalf("extra indication for confirmed withdrawal: %+v", ind)
	default:
	}
}

func TestWithdrawalReportsPrecedeSupersededApplicationFailure(t *testing.T) {
	for _, state := range []string{"written", "accepted", "unaccepted"} {
		for _, cancelContext := range []bool{false, true} {
			t.Run(state+map[bool]string{false: "/waiting", true: "/cancelled_context"}[cancelContext], func(t *testing.T) {
				c, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
				c.cfg.TAck = time.Hour
				c.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				management := c.ManagementIndications()
				var request *pendingRequest
				var done <-chan error
				if state == "written" {
					request, done = pendingApplicationActivation(t, c, ctx)
				} else {
					sender := newFullSendBuffer()
					c.signalWriter = nil
					c.controlTransport = sender
					if state == "accepted" {
						c.controlTransport = &acceptanceWinsSender{sender}
					}
					c.transportCloser = sender.close
					result := make(chan error, 1)
					done = result
					go func() { result <- c.ASPActive(ctx, associationConfigASKey(c.cfg, 1)) }()
					select {
					case <-sender.waiting:
					case <-time.After(time.Second):
						t.Fatal("application write did not enter transport")
					}
					request = applicationActiveRequest(t, c)
				}
				c.aspTransferMu.RLock()
				var release sync.Once
				unblock := func() { release.Do(c.aspTransferMu.RUnlock) }
				defer unblock()
				handled := make(chan error, 1)
				go func() {
					handled <- c.handleAspInactiveAck(messages.NewAspInactiveAck(params.NewRoutingContext(1), nil))
				}()
				select {
				case <-request.stop:
				case <-time.After(time.Second):
					t.Fatal("withdrawal did not stop activation")
				}
				if cancelContext {
					cancel()
					select {
					case err := <-done:
						if !errors.Is(err, ErrASPProcedureSuperseded) {
							t.Fatalf("claimed cancellation returned %v", err)
						}
					case <-time.After(250 * time.Millisecond):
						t.Fatal("context cancellation waited for withdrawal reports")
					}
					assertApplicationActivationFailure(t, <-management, associationConfigASKey(c.cfg, 1))
				} else {
					select {
					case err := <-done:
						t.Fatalf("application waiter released before withdrawal reports: %v", err)
					case <-time.After(25 * time.Millisecond):
					}
				}
				unblock()
				if err := <-handled; err != nil {
					t.Fatal(err)
				}
				if !cancelContext {
					select {
					case err := <-done:
						if !errors.Is(err, ErrASPProcedureSuperseded) {
							t.Fatalf("application activation returned %v", err)
						}
					case <-time.After(time.Second):
						t.Fatal("application waiter was not released after reports")
					}
				}
				if ind := <-management; ind.Kind != ManagementASPWithdrawal {
					t.Fatalf("first indication must be withdrawal: %+v", ind)
				}
				if state == "accepted" {
					if ind := <-management; ind.Kind != ManagementError || !strings.Contains(ind.Cause.Error(), "already accepted by SCTP") {
						t.Fatalf("second indication must report acceptance: %+v", ind)
					}
				}
				if !cancelContext {
					select {
					case ind := <-management:
						assertApplicationActivationFailure(t, ind, associationConfigASKey(c.cfg, 1))
					default:
						t.Fatal("missing application procedure failure after withdrawal reports")
					}
				}
			})
		}
	}
}

func TestCancellationAcceptanceReportPrecedesWaiterCompletion(t *testing.T) {
	c, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
	c.cfg.TAck = time.Hour
	c.noteRoutingContextsAcked(params.NewRoutingContext(1))
	sender := &acceptanceWinsSender{newFullSendBuffer()}
	c.signalWriter = nil
	c.controlTransport = sender
	c.transportCloser = sender.close
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.ASPActive(ctx, associationConfigASKey(c.cfg, 1)) }()
	<-sender.waiting
	request := applicationActiveRequest(t, c)
	management := c.ManagementIndications()
	keys := []ASKey{associationConfigASKey(c.cfg, 1)}
	cancelled := c.cancelASPActivations(keys, "ASP Inactive Ack")
	c.reportInactiveAckWithdrawal(keys)
	// Hold publication of the acceptance warning. Observe result directly:
	// the public method's generic error report also takes muMgmt, so observing
	// only the method's return would conceal premature waiter completion.
	c.muMgmt.Lock()
	started, reported := make(chan struct{}), make(chan struct{})
	go func() {
		close(started)
		c.reportCancelledActivations(cancelled)
		close(reported)
	}()
	<-started
	early := false
	var earlyResult error
	select {
	case earlyResult = <-request.result:
		early = true
	case <-time.After(25 * time.Millisecond):
	}
	c.muMgmt.Unlock()
	<-reported
	if err := <-done; !errors.Is(err, ErrASPProcedureSuperseded) {
		t.Fatal(err)
	}
	if early {
		t.Fatalf("request result was completed before the acceptance warning could be published: %v", earlyResult)
	}
	if ind := <-management; ind.Kind != ManagementASPWithdrawal {
		t.Fatalf("first report=%+v", ind)
	}
	if ind := <-management; !strings.Contains(ind.Cause.Error(), "already accepted by SCTP") {
		t.Fatalf("second report=%+v", ind)
	}
	assertApplicationActivationFailure(t, <-management, keys[0])
}
