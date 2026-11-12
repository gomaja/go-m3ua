package m3ua

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// activationHeldAtWithdrawalDrain keeps the handler after its claim and own
// transport join, but before its existing traffic barrier can drain.
func activationHeldAtWithdrawalDrain(t *testing.T, ctx context.Context, waiting, written bool) (*Association, <-chan error, func(), <-chan error) {
	t.Helper()
	c, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
	c.cfg.TAck = time.Hour
	c.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
	c.ManagementIndications()
	var request *pendingRequest
	var done <-chan error
	if written {
		request, done = pendingApplicationActivation(t, c, ctx)
	} else {
		sender := &acceptanceWinsSender{newFullSendBuffer()}
		c.signalWriter = nil
		c.controlTransport = sender
		c.transportCloser = sender.close
		result := make(chan error, 1)
		done = result
		go func() {
			if waiting {
				result <- c.ASPActive(ctx, associationConfigASKey(c.cfg, 1))
			} else {
				result <- c.ActivateRoutingContexts(1)
			}
		}()
		select {
		case <-sender.waiting:
		case <-time.After(time.Second):
			t.Fatal("initial activation did not enter transport")
		}
		request = applicationActiveRequest(t, c)
	}
	c.aspTransferMu.RLock()
	var release sync.Once
	unblock := func() { release.Do(c.aspTransferMu.RUnlock) }
	t.Cleanup(unblock)
	handled := make(chan error, 1)
	go func() {
		handled <- c.handleAspInactiveAck(messages.NewAspInactiveAck(params.NewRoutingContext(1), nil))
	}()
	select {
	case <-request.stop:
	case <-time.After(time.Second):
		t.Fatal("withdrawal did not claim activation")
	}
	request.joinControlWrite()
	select {
	case err := <-handled:
		t.Fatalf("fixture did not hold the handler at the traffic drain: %v", err)
	default:
	}
	return c, done, unblock, handled
}

func TestASPActiveContextCancellationEscapesWithdrawalDrain(t *testing.T) {
	for _, written := range []bool{false, true} {
		t.Run(map[bool]string{false: "initial_write", true: "awaiting_ack"}[written], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			c, done, unblock, handled := activationHeldAtWithdrawalDrain(t, ctx, true, written)
			cancel()
			var result error
			returned := false
			select {
			case result = <-done:
				returned = true
			case <-time.After(250 * time.Millisecond):
			}
			if returned {
				if !errors.Is(result, ErrASPProcedureSuperseded) {
					t.Errorf("claimed activation returned %v", result)
				}
				select {
				case ind := <-c.ManagementIndications():
					assertApplicationActivationFailure(t, ind, associationConfigASKey(c.cfg, 1))
				default:
					t.Error("context escape did not report the application failure")
				}
			}
			unblock()
			if err := <-handled; err != nil {
				t.Fatal(err)
			}
			if !returned {
				err := <-done
				t.Fatalf("ASPActive ignored its cancelled context while withdrawal waited for DATA drain; returned %v only after drain", err)
			}
		})
	}
}

func TestActivateRoutingContextsDoesNotWaitForWithdrawalDrain(t *testing.T) {
	c, done, unblock, handled := activationHeldAtWithdrawalDrain(t, context.Background(), false, false)
	var result error
	returned := false
	select {
	case result = <-done:
		returned = true
	case <-time.After(250 * time.Millisecond):
	}
	if returned {
		if !errors.Is(result, ErrASPProcedureSuperseded) {
			t.Errorf("non-waiting activation returned %v", result)
		}
		select {
		case ind := <-c.ManagementIndications():
			t.Errorf("non-waiting activation produced an early management indication: %+v", ind)
		default:
		}
	}
	unblock()
	if err := <-handled; err != nil {
		t.Fatal(err)
	}
	if !returned {
		err := <-done
		t.Fatalf("ActivateRoutingContexts waited for withdrawal DATA drain after its write completed: %v", err)
	}
}

func TestAssociationCloseReleasesASPActiveDuringWithdrawalDrain(t *testing.T) {
	for _, written := range []bool{false, true} {
		t.Run(map[bool]string{false: "initial_write", true: "awaiting_ack"}[written], func(t *testing.T) {
			c, done, unblock, handled := activationHeldAtWithdrawalDrain(t, context.Background(), true, written)
			closed := make(chan error, 1)
			go func() { closed <- c.Close() }()
			select {
			case <-c.done:
			case <-time.After(time.Second):
				t.Fatal("association close did not signal Done")
			}
			var result error
			returned := false
			select {
			case result = <-done:
				returned = true
			case <-time.After(250 * time.Millisecond):
			}
			unblock()
			if err := <-handled; err != nil {
				t.Fatal(err)
			}
			if err := <-closed; err != nil {
				t.Fatal(err)
			}
			if !returned {
				err := <-done
				t.Fatalf("association close did not release ASPActive before withdrawal DATA drain: %v", err)
			}
			if !errors.Is(result, ErrAssociationClosed) {
				t.Fatalf("closed association returned %v", result)
			}
		})
	}
}

func TestASPActiveContextCancellationBeforeWithdrawal(t *testing.T) {
	c, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
	c.cfg.TAck = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, done := pendingApplicationActivation(t, c, ctx)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("unclaimed activation returned %v", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("unclaimed activation ignored context cancellation")
	}
}

func TestTAckRetryDoesNotWaitForWithdrawalDrain(t *testing.T) {
	c, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
	c.cfg.TAck = 5 * time.Millisecond
	c.cfg.TAckRetries = 1
	c.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
	sender := newFullSendBuffer()
	c.signalWriter = nil
	c.controlTransport = sender
	c.transportCloser = sender.close
	request, _, done := startObservedRetry(t, c, messages.NewAspActive(nil, params.NewRoutingContext(2), nil), requestAspActive)
	<-sender.waiting
	c.aspTransferMu.RLock()
	var release sync.Once
	unblock := func() { release.Do(c.aspTransferMu.RUnlock) }
	defer unblock()
	handled := make(chan error, 1)
	go func() {
		handled <- c.handleAspInactiveAck(messages.NewAspInactiveAck(params.NewRoutingContext(2), nil))
	}()
	<-request.stop
	if failed := request.failure.Load(); failed == nil || !errors.Is(*failed, ErrASPProcedureSuperseded) {
		t.Fatal("fixture did not cancel the retry through withdrawal")
	}
	returned := false
	select {
	case <-done:
		returned = true
	case <-time.After(250 * time.Millisecond):
	}
	unblock()
	if err := <-handled; err != nil {
		t.Fatal(err)
	}
	if !returned {
		<-done
		t.Fatal("T(ack) retry waited for withdrawal DATA drain after its send returned")
	}
}
