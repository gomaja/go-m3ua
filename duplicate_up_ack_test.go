package m3ua

import (
	"context"
	"errors"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestDuplicateUpAckLeavesActiveTrafficUnchanged(t *testing.T) {
	for _, model := range []string{"ASP", "IPSP SE", "IPSP DE", "IPSP shared ASPSM"} {
		t.Run(model, func(t *testing.T) {
			var c *Association
			var sent *[]messages.M3UA
			switch model {
			case "ASP":
				c, sent = newTestConnWithContexts(t, StateASPDown, RoleASP, 1)
			case "IPSP SE":
				c, sent = newIPSPDownAssociationForTest(t, IPSPExchangeSingle, IPSPASPSMExchangeSingle, StateASPDown)
			case "IPSP DE":
				c, sent = newIPSPDownAssociationForTest(t, IPSPExchangeDouble, IPSPASPSMExchangeDouble, StateASPDown)
			default:
				c, sent = newIPSPDownAssociationForTest(t, IPSPExchangeDouble, IPSPASPSMExchangeSingle, StateASPDown)
			}
			c.cfg.ASPProcedures = explicitASPProcedurePolicy()
			c.recvStream.Store(0)
			c.cfg.TAck = time.Hour
			request, err := c.beginASPSM()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.writePendingControl(request, request.msg); err != nil {
				t.Fatal(err)
			}
			c.handleSignals(context.Background(), messages.NewAspUpAck(nil, nil))
			rc := c.configuredLocalRoutingContextParam()
			c.startTAck(messages.NewAspActive(nil, rc, nil), requestAspActive)
			c.handleSignals(context.Background(), messages.NewAspActiveAck(nil, rc, nil))
			before := c.IPSPState()
			for len(c.stateChan) > 0 {
				if err := c.handlePublishedStateUpdate(receiveASPTest(t, c.stateChan, "state publication")); err != nil {
					t.Fatal(err)
				}
			}
			for len(c.stateEventChan) > 0 {
				receiveASPTest(t, c.stateEventChan, "state event")
			}
			for len(c.mgmtChan) > 0 {
				receiveASPTest(t, c.mgmtChan, "management indication")
			}
			writes := len(*sent)
			identifier, identifierSet := c.PeerASPIdentifier()
			c.handleSignals(context.Background(), messages.NewAspUpAck(params.NewAspIdentifier(99), nil))
			if got, set := c.PeerASPIdentifier(); got != identifier || set != identifierSet {
				t.Error("duplicate changed peer identifier")
			}
			if c.IPSPState() != before {
				t.Errorf("duplicate changed state from %+v to %+v", before, c.IPSPState())
			}
			for _, context := range rc.RoutingContexts() {
				if !c.routingContextAcked(context) {
					t.Errorf("duplicate closed RC %d", context)
				}
				if c.role == RoleIPSP && !c.isIPSPDoubleExchange() && !c.activeForRoutingContext(context) {
					t.Errorf("duplicate closed peer traffic for RC %d", context)
				}
			}
			if c.role == RoleASP {
				release, err := c.admitDataWrite(routingContextASKey(1))
				if err != nil {
					t.Errorf("duplicate stopped traffic: %v", err)
				} else {
					release()
				}
			}
			if len(c.errChan) != 0 || len(c.mgmtChan) != 0 || len(*sent) != writes {
				t.Error("duplicate caused an error, report or control write")
			}
			if got := receiveASPTest(t, c.stateChan, "duplicate publication"); got != stateUnchanged {
				t.Errorf("duplicate publication %v", got)
			} else if err := c.handlePublishedStateUpdate(got); err != nil {
				t.Fatal(err)
			}
			if len(c.stateEventChan) != 0 || len(c.mgmtChan) != 0 {
				t.Error("duplicate published a state or management report")
			}
		})
	}
}

func TestUpAckWithoutRemainingTransmissionRetainsCompletedHandling(t *testing.T) {
	c, _ := newTestConnWithContexts(t, StateASPDown, RoleASP, 1)
	completeUpAndRCOne(t, c)
	c.handleSignals(context.Background(), messages.NewAspUpAck(nil, nil))
	applyUpAckPublications(t, c)
	assertMainCompletedUpFallback(t, c)
}

func TestUpAckDuplicateBudgetCountsAcceptedCopiesAndResetsOnRestart(t *testing.T) {
	c, _ := newTestConnWithContexts(t, StateASPDown, RoleASP, 1)
	c.cfg.TAck = time.Hour
	c.recvStream.Store(0)
	request, err := c.beginASPSM()
	if err != nil {
		t.Fatal(err)
	}
	c.signalWriter = func(messages.M3UA) (int, error) { return 0, errors.New("synthetic send failure") }
	if _, err := c.writePendingControl(request, request.msg); err == nil {
		t.Fatal("failed write succeeded")
	}
	c.handleSignals(context.Background(), messages.NewAspUpAck(nil, nil))
	c.setState(StateASPActive)
	c.handleSignals(context.Background(), messages.NewAspUpAck(nil, nil))
	if c.State() != StateASPInactive || firstErr(c) != nil {
		t.Fatal("failed write created duplicate credit or changed main's fallback")
	}
	var unexpected *UnexpectedMessageError

	// A fresh fixture leaves an actual unanswered copy at the epoch boundary;
	// earlier unsolicited Acks must not spend this test's reset credit.
	c, _ = newTestConnWithContexts(t, StateASPDown, RoleASP, 1)
	c.cfg.TAck = time.Hour
	c.recvStream.Store(0)
	request, err = c.beginASPSM()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.writePendingControl(request, request.msg); err != nil {
		t.Fatal(err)
	}
	c.handleSignals(context.Background(), messages.NewAspUpAck(nil, nil))
	c.resetTAckEpoch()
	c.setState(StateASPActive)
	c.handleSignals(context.Background(), messages.NewAspUpAck(nil, nil))
	if err := firstErr(c); !errors.As(err, &unexpected) {
		t.Fatalf("old epoch duplicate credit survived: %v", err)
	}
}

func TestUpAckDuplicateBudgetIsConsumedOncePerCopy(t *testing.T) {
	c, _ := newTestConnWithContexts(t, StateASPDown, RoleASP, 1)
	c.recvStream.Store(0)
	c.cfg.TAck = time.Hour
	request, err := c.beginASPSM()
	if err != nil {
		t.Fatal(err)
	}
	// Only copies of the library procedure earn duplicate credit.
	for range 2 {
		if _, err := c.writePendingControl(request, request.msg); err != nil {
			t.Fatal(err)
		}
	}
	c.handleSignals(context.Background(), messages.NewAspUpAck(nil, nil))
	c.setState(StateASPActive)
	for range 2 {
		c.handleSignals(context.Background(), messages.NewAspUpAck(nil, nil))
		if c.State() != StateASPActive || firstErr(c) != nil {
			t.Fatal("owed duplicate was not absorbed")
		}
	}
	c.handleSignals(context.Background(), messages.NewAspUpAck(nil, nil))
	if c.State() != StateASPInactive || firstErr(c) != nil {
		t.Fatal("duplicate credit reused or changed main's fallback")
	}
}

func TestTAckRetransmissionAddsOneUpAckDuplicateCredit(t *testing.T) {
	c, _ := newTestConnWithContexts(t, StateASPDown, RoleASP, 1)
	c.recvStream.Store(0)
	c.cfg.TAck = 20 * time.Millisecond
	c.cfg.TAckRetries = 1
	writes := make(chan struct{}, 4)
	release, open := aspTestGate(t)
	retryReturned := make(chan struct{})
	var calls atomic.Int32
	c.signalWriter = func(m messages.M3UA) (int, error) {
		writes <- struct{}{}
		if calls.Add(1) == 2 {
			defer close(retryReturned)
			if err := awaitASPTestRelease(c, release); err != nil {
				return 0, err
			}
		}
		return m.MarshalLen(), nil
	}
	unlock := holdASPTestLock(t, &c.tack.retryMu)
	request := c.startTAck(messages.NewAspUp(nil, nil), requestAspUp)
	if _, err := c.writeTAckControl(request, request.msg); err != nil {
		t.Fatal(err)
	}
	unlock()
	for range 2 {
		select {
		case <-writes:
		case err := <-request.result:
			t.Fatalf("Up ended before its initial write and T(ack) resend: %v", err)
		case <-c.done:
			t.Fatal("association closed before T(ack) resend")
		case <-time.After(aspTestWait):
			t.Fatal("initial Up or T(ack) resend was not submitted")
		}
	}
	// Keep the retry's transport call in flight until its Ack is processed.
	// The last-resend timer cannot expire while the test is descheduled.
	c.handleSignals(context.Background(), messages.NewAspUpAck(nil, nil))
	c.setState(StateASPActive)
	c.handleSignals(context.Background(), messages.NewAspUpAck(nil, nil))
	if c.State() != StateASPActive || firstErr(c) != nil {
		t.Fatal("T(ack) duplicate was not absorbed")
	}
	c.handleSignals(context.Background(), messages.NewAspUpAck(nil, nil))
	if c.State() != StateASPInactive || firstErr(c) != nil {
		t.Fatal("T(ack) credit was not consumed or changed main's fallback")
	}
	open()
	receiveASPTest(t, retryReturned, "retry write release")
}

func TestUpAckBeforeSendReturnsLeavesNoDuplicateCredit(t *testing.T) {
	c, _ := newTestConn(t, StateASPDown, RoleASP)
	c.cfg.TAck = time.Hour
	entered := make(chan struct{})
	release, open := aspTestGate(t)
	c.signalWriter = func(m messages.M3UA) (int, error) {
		close(entered)
		if err := awaitASPTestRelease(c, release); err != nil {
			return 0, err
		}
		return m.MarshalLen(), nil
	}
	sent := startASPTestCall(func() error { _, err := c.beginASPSM(); return err })
	awaitASPTestSend(t, entered, sent, "initial Up")
	acked := startASPTestCall(func() error { return c.handleAspUpAck(messages.NewAspUpAck(nil, nil)) })
	if err := finishASPTestAck(t, acked, "solicited Ack waited for send accounting before stopping the request"); err != nil {
		t.Fatal(err)
	}
	open()
	if err := sent.wait(t, "initial Up write release"); err != nil {
		t.Fatal(err)
	}
	c.setState(StateASPActive)
	if err := c.handleAspUpAck(messages.NewAspUpAck(nil, nil)); err != nil {
		t.Fatalf("racing Ack left stale duplicate credit or changed main's fallback: %v", err)
	}
}

func TestUpAckCancelsBlockedRetransmissionBeforeAccounting(t *testing.T) {
	c, _ := newTestConn(t, StateASPDown, RoleASP)
	c.cfg.TAck, c.cfg.TAckRetries = 10*time.Millisecond, 1
	c.cfg.ControlWriteTimeout = time.Hour
	transport := newFullSendBuffer()
	transport.firstErr = nil
	c.signalWriter, c.controlTransport, c.transportCloser = nil, transport, transport.close
	unlock := holdASPTestLock(t, &c.tack.retryMu)
	request := c.startTAck(messages.NewAspUp(nil, nil), requestAspUp)
	if _, err := c.writeTAckControl(request, request.msg); err != nil {
		t.Fatal(err)
	}
	transport.mu.Lock()
	transport.firstErr = syscall.EAGAIN
	transport.mu.Unlock()
	unlock()
	select {
	case <-transport.waiting:
	case err := <-request.result:
		t.Fatalf("retry returned before blocking: %v", err)
	case <-c.done:
		t.Fatal("association closed before retry blocked")
	case <-time.After(aspTestWait):
		t.Fatal("retry did not enter the full send buffer")
	}
	acked := startASPTestCall(func() error { return c.handleAspUpAck(messages.NewAspUpAck(nil, nil)) })
	if err := finishASPTestAck(t, acked, "Up Ack could not cancel blocked retry before delivery accounting"); err != nil {
		t.Fatal(err)
	}
	if err := waitASPTestAck(t, c, request); err != nil {
		t.Fatal(err)
	}
	if c.State() != StateASPInactive {
		t.Fatal("Up state was not committed")
	}
}

func TestFreshUpRetiresUnsolicitedAckHistory(t *testing.T) {
	c, _ := newTestConn(t, StateASPInactive, RoleASP)
	c.cfg.TAck = time.Hour
	c.handleSignals(context.Background(), messages.NewAspUpAck(nil, nil))
	req, err := c.beginASPSM()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.writePendingControl(req, req.msg); err != nil {
		t.Fatal(err)
	}
	c.handleSignals(context.Background(), messages.NewAspUpAck(nil, nil))
	c.setState(StateASPActive)
	c.handleSignals(context.Background(), messages.NewAspUpAck(nil, nil))
	if c.State() != StateASPActive || firstErr(c) != nil {
		t.Fatal("earlier unsolicited Ack consumed the fresh procedure's duplicate credit")
	}
}

func TestStoppedUpDoesNotCreateDuplicateCredit(t *testing.T) {
	c, _ := newTestConn(t, StateASPDown, RoleASP)
	c.cfg.TAck = time.Hour
	request, err := c.beginASPSM()
	if err != nil {
		t.Fatal(err)
	}
	if err := c.handleAspUpAck(messages.NewAspUpAck(nil, nil)); err != nil {
		t.Fatal(err)
	}
	c.setState(StateASPActive)
	if n, err := c.writeTAckControl(request, request.msg); n != 0 || err != nil {
		t.Fatalf("stopped write: %d %v", n, err)
	}
	if err := c.handleAspUpAck(messages.NewAspUpAck(nil, nil)); err != nil {
		t.Fatalf("stopped write earned duplicate credit: %v", err)
	}
}

func TestOldUpSendOutcomeCannotCreditRestartedEpoch(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "accepted", true: "failed"}[fail], func(t *testing.T) {
			c, _ := newTestConn(t, StateASPDown, RoleASP)
			c.cfg.TAck = time.Hour
			entered := make(chan struct{})
			release, open := aspTestGate(t)
			var calls atomic.Int32
			c.signalWriter = func(m messages.M3UA) (int, error) {
				if calls.Add(1) == 1 {
					close(entered)
					if err := awaitASPTestRelease(c, release); err != nil {
						return 0, err
					}
					if fail {
						return 0, errors.New("old send failed")
					}
				}
				return m.MarshalLen(), nil
			}
			old := c.startTAck(messages.NewAspUp(nil, nil), requestAspUp)
			written := startASPTestCall(func() error { _, err := c.writeTAckControl(old, old.msg); return err })
			awaitASPTestSend(t, entered, written, "old Up write")
			fresh := c.startTAck(messages.NewAspUp(nil, nil), requestAspUp)
			// Exercise only ledger generations: production's retryMu serializes these
			// writes, but a fresh registration can retire the old ledger independently.
			for range 2 {
				if _, err := c.writeTAckControl(fresh, fresh.msg); err != nil {
					t.Fatal(err)
				}
			}
			if err := c.handleAspUpAck(messages.NewAspUpAck(nil, nil)); err != nil {
				t.Fatal(err)
			}
			open()
			if err := written.wait(t, "old write outcome"); (err != nil) != fail {
				t.Fatalf("old write failure=%v, want %v", err, fail)
			}
			c.setState(StateASPActive)
			if err := c.handleAspUpAck(messages.NewAspUpAck(nil, nil)); !errors.Is(err, errDuplicateASPUpAck) {
				t.Fatalf("old outcome spent fresh credit: %v", err)
			}
			if err := c.handleAspUpAck(messages.NewAspUpAck(nil, nil)); err != nil {
				t.Fatalf("old outcome credited fresh ledger: %v", err)
			}
		})
	}
}

func TestFreshUpDuplicateBudgetAfterRestart(t *testing.T) {
	c, _ := newTestConn(t, StateASPInactive, RoleASP)
	c.cfg.TAck = time.Hour
	for range 3 {
		c.handleSignals(context.Background(), messages.NewAspUpAck(nil, nil))
	}
	c.resetTAckEpoch()
	request, err := c.beginASPSM()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.writePendingControl(request, request.msg); err != nil {
		t.Fatal(err)
	}
	c.handleSignals(context.Background(), messages.NewAspUpAck(nil, nil))
	c.setState(StateASPActive)
	c.handleSignals(context.Background(), messages.NewAspUpAck(nil, nil))
	if c.State() != StateASPActive || firstErr(c) != nil {
		t.Fatal("prior epoch's received Acks consumed fresh duplicate credit")
	}
}

func TestFirstUpAckWithoutCompletedProcedureRetainsMainHandling(t *testing.T) {
	c, _ := newTestConn(t, StateASPActive, RoleASP)
	if _, err := c.WriteSignal(messages.NewAspUp(nil, nil)); err != nil {
		t.Fatal(err)
	}
	c.handleSignals(context.Background(), messages.NewAspUpAck(nil, nil))
	var unexpected *UnexpectedMessageError
	if err := firstErr(c); !errors.As(err, &unexpected) {
		t.Fatalf("first raw Up Ack changed main's unexpected handling: %v", err)
	}
	if c.State() != StateASPInactive || !c.resumeAfterStrayAck() {
		t.Fatal("first raw Up Ack changed recovery policy")
	}
}

func TestFirstUpAckDoesNotWaitForUnconfirmedRawSend(t *testing.T) {
	c, _ := newTestConn(t, StateASPActive, RoleASP)
	entered := make(chan struct{})
	release, open := aspTestGate(t)
	c.signalWriter = func(m messages.M3UA) (int, error) {
		close(entered)
		if err := awaitASPTestRelease(c, release); err != nil {
			return 0, err
		}
		return m.MarshalLen(), nil
	}
	sent := startASPTestCall(func() error { _, err := c.WriteSignal(messages.NewAspUp(nil, nil)); return err })
	awaitASPTestSend(t, entered, sent, "raw Up")
	acked := startASPTestCall(func() error { return c.handleAspUpAck(messages.NewAspUpAck(nil, nil)) })
	err := finishASPTestAck(t, acked, "first raw Ack waited for a send that cannot make it a duplicate")
	var unexpected *UnexpectedMessageError
	if !errors.As(err, &unexpected) {
		t.Fatalf("first raw Ack: %v", err)
	}
	open()
	if err := sent.wait(t, "raw Up write release"); err != nil {
		t.Fatal(err)
	}
}

func TestCompletedUpAckDoesNotWaitForApplicationSend(t *testing.T) {
	c, _ := newTestConn(t, StateASPDown, RoleASP)
	c.cfg.TAck = time.Hour
	if _, err := c.beginASPSM(); err != nil {
		t.Fatal(err)
	}
	if err := c.handleAspUpAck(messages.NewAspUpAck(nil, nil)); err != nil {
		t.Fatal(err)
	}
	c.setState(StateASPActive)
	entered := make(chan struct{})
	release, open := aspTestGate(t)
	c.signalWriter = func(m messages.M3UA) (int, error) {
		close(entered)
		if err := awaitASPTestRelease(c, release); err != nil {
			return 0, err
		}
		return m.MarshalLen(), nil
	}
	sent := startASPTestCall(func() error { _, err := c.WriteSignal(messages.NewAspUp(nil, nil)); return err })
	awaitASPTestSend(t, entered, sent, "application Up")
	acked := startASPTestCall(func() error { return c.handleAspUpAck(messages.NewAspUpAck(nil, nil)) })
	if err := finishASPTestAck(t, acked, "Ack waited for an application send"); err != nil {
		t.Errorf("application Up changed main's completed-Up fallback: %v", err)
	}
	open()
	if err := sent.wait(t, "application Up write release"); err != nil {
		t.Fatal(err)
	}
}
