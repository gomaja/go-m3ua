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
		case <-time.After(aspTestWait):
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
	case <-time.After(aspTestWait):
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
			if result := receiveASPTest(t, done, "context cancellation before DATA drain"); !errors.Is(result, context.Canceled) {
				t.Fatalf("independent cancellation exposed retirement result before publication: %v", result)
			}
			ind := receiveASPTest(t, c.ManagementIndications(), "context cancellation report")
			if ind.Kind != ManagementError || !errors.Is(ind.Cause, context.Canceled) {
				t.Fatalf("cancellation indication = %+v", ind)
			}
			unblock()
			if err := receiveASPTest(t, handled, "withdrawal after DATA drain"); err != nil {
				t.Fatal(err)
			}
			ind = receiveASPTest(t, c.ManagementIndications(), "withdrawal indication")
			if ind.Kind != ManagementASPWithdrawal {
				t.Fatalf("withdrawal report = %+v", ind)
			}
		})
	}
}

func TestActivateRoutingContextsWaitsForRetiredResultPublication(t *testing.T) {
	c, done, unblock, handled := activationHeldAtWithdrawalDrain(t, context.Background(), false, false)
	call := startASPTestCall(func() error {
		select {
		case err := <-done:
			return err
		case <-time.After(aspTestWait):
			return errors.New("activation did not complete")
		}
	})
	if !awaitASPTestStack(t, call, "(*Association).waitTAck(") {
		t.Fatalf("retired nonwaiting activation returned before withdrawal: %v", call.err)
	}
	select {
	case ind := <-c.ManagementIndications():
		t.Fatalf("early report: %+v", ind)
	default:
	}
	unblock()
	if err := receiveASPTest(t, handled, "withdrawal after DATA drain"); err != nil {
		t.Fatal(err)
	}
	if err := call.wait(t, "retired activation"); !errors.Is(err, ErrASPProcedureSuperseded) {
		t.Fatalf("retired activation = %v", err)
	}
	if ind := receiveASPTest(t, c.ManagementIndications(), "withdrawal report"); ind.Kind != ManagementASPWithdrawal {
		t.Fatalf("report = %+v", ind)
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
			case <-time.After(aspTestWait):
				t.Fatal("association close did not signal Done")
			}
			var result error
			returned := false
			select {
			case result = <-done:
				returned = true
			case <-time.After(aspTestWait):
			}
			unblock()
			if err := receiveASPTest(t, handled, "withdrawal"); err != nil {
				t.Fatal(err)
			}
			if err := receiveASPTest(t, closed, "association close"); err != nil {
				t.Fatal(err)
			}
			if !returned {
				err := receiveASPTest(t, done, "procedure completion")
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
	case <-time.After(aspTestWait):
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
	receiveASPTest(t, sender.waiting, "retry transport entry")
	c.aspTransferMu.RLock()
	var release sync.Once
	unblock := func() { release.Do(c.aspTransferMu.RUnlock) }
	defer unblock()
	handled := make(chan error, 1)
	go func() {
		handled <- c.handleAspInactiveAck(messages.NewAspInactiveAck(params.NewRoutingContext(2), nil))
	}()
	receiveASPTest(t, request.stop, "retry withdrawal claim")
	if failed := request.failure.Load(); failed == nil || !errors.Is(*failed, ErrASPProcedureSuperseded) {
		t.Fatal("fixture did not cancel the retry through withdrawal")
	}
	returned := false
	select {
	case <-done:
		returned = true
	case <-time.After(aspTestWait):
	}
	unblock()
	if err := receiveASPTest(t, handled, "withdrawal"); err != nil {
		t.Fatal(err)
	}
	if !returned {
		receiveASPTest(t, done, "retry completion")
		t.Fatal("T(ack) retry waited for withdrawal DATA drain after its send returned")
	}
}
