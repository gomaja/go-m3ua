package m3ua

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func applyUpAckPublications(t *testing.T, c *Association) {
	t.Helper()
	for len(c.stateChan) > 0 {
		if err := c.handlePublishedStateUpdate(receiveASPTest(t, c.stateChan, "Up Ack publication")); err != nil {
			t.Fatal(err)
		}
	}
}

func completeUpAndRCOne(t *testing.T, c *Association) {
	t.Helper()
	c.recvStream.Store(0)
	c.cfg.TAck = time.Hour
	c.cfg.ASPProcedures = explicitASPProcedurePolicy()
	if _, err := c.beginASPSM(); err != nil {
		t.Fatal(err)
	}
	c.handleSignals(context.Background(), messages.NewAspUpAck(nil, nil))
	rc := params.NewRoutingContext(1)
	c.startTAck(messages.NewAspActive(nil, rc, nil), requestAspActive)
	c.handleSignals(context.Background(), messages.NewAspActiveAck(nil, rc, nil))
	applyUpAckPublications(t, c)
}

func assertMainCompletedUpFallback(t *testing.T, c *Association) {
	t.Helper()
	if c.State() != StateASPInactive {
		t.Errorf("Up Ack state %v, want main's ASP-INACTIVE", c.State())
	}
	if _, err := c.WriteData(DataRequest{AS: associationConfigASKey(c.cfg, 1), Stream: 1, ProtocolData: simpleProtocolData("after-up")}); !errors.Is(err, ErrNotEstablished) {
		t.Errorf("Up Ack DATA gate: %v, want main's ErrNotEstablished", err)
	}
	if err := firstErr(c); err != nil {
		t.Errorf("Up Ack reported %v, want main's silent fallback", err)
	}
	if c.resumeAfterStrayAck() {
		t.Error("Up Ack armed automatic recovery")
	}
}

func TestApplicationUpDoesNotEarnDuplicateCredit(t *testing.T) {
	c, _ := newTestConnWithContexts(t, StateASPDown, RoleASP, 1)
	completeUpAndRCOne(t, c)
	if _, err := c.WriteSignal(messages.NewAspUp(nil, nil)); err != nil {
		t.Fatal(err)
	}
	c.handleSignals(context.Background(), messages.NewAspUpAck(nil, nil))
	applyUpAckPublications(t, c)
	assertMainCompletedUpFallback(t, c)
}

func TestNonDuplicateUpAckKeepsExplicitScopesInactive(t *testing.T) {
	c, sent := newTestConnWithContexts(t, StateASPDown, RoleASP, 1, 2)
	completeUpAndRCOne(t, c)
	if !c.routingContextAcked(1) || c.routingContextAcked(2) {
		t.Fatal("initial activation must cover only RC1")
	}
	before := len(*sent)
	c.handleSignals(context.Background(), messages.NewAspUpAck(nil, nil))
	applyUpAckPublications(t, c)
	assertMainCompletedUpFallback(t, c)
	if c.routingContextAcked(2) {
		t.Error("Up Ack activated RC2")
	}
	for _, m := range (*sent)[before:] {
		if _, ok := m.(*messages.AspActive); ok {
			t.Errorf("Up Ack sent ASP Active: %v", m)
		}
	}
	if len(*sent) != before {
		t.Error("main's completed-Up fallback sent a control message")
	}
}

func TestInFlightLibraryUpAckDoesNotBlockInboundProcessing(t *testing.T) {
	c, _ := newTestConnWithContexts(t, StateASPDown, RoleASP, 1)
	c.recvStream.Store(0)
	c.cfg.TAck = time.Hour
	req, err := c.beginASPSM()
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release, open := aspTestGate(t)
	var beats atomic.Int32
	c.signalWriter = func(m messages.M3UA) (int, error) {
		if _, ok := m.(*messages.AspUp); ok {
			close(entered)
			if err := awaitASPTestRelease(c, release); err != nil {
				return 0, err
			}
		}
		if _, ok := m.(*messages.HeartbeatAck); ok {
			beats.Add(1)
		}
		return m.MarshalLen(), nil
	}
	written := startASPTestCall(func() error { _, err := c.writePendingControl(req, req.msg); return err })
	awaitASPTestSend(t, entered, written, "in-flight library copy")
	c.handleSignals(context.Background(), messages.NewAspUpAck(nil, nil))
	applyUpAckPublications(t, c)
	handled := startASPTestCall(func() error {
		c.handleSignals(context.Background(), messages.NewAspUpAck(nil, nil))
		c.handleSignals(context.Background(), messages.NewHeartbeat(params.NewHeartbeatData([]byte("after-up"))))
		return nil
	})
	if err := finishASPTestAck(t, handled, "Ack classification blocked inbound processing behind the library write"); err != nil {
		t.Fatal(err)
	}
	if publication := receiveASPTest(t, c.stateChan, "in-flight duplicate publication"); publication != stateUnchanged {
		t.Errorf("in-flight duplicate published %v", publication)
	}
	if beats.Load() != 1 {
		t.Error("dispatcher did not process the following heartbeat")
	}
	open()
	if err := written.wait(t, "in-flight library write release"); err != nil {
		t.Fatal(err)
	}
}

func TestCancelledFreshUpFirstAckRetainsMainHandling(t *testing.T) {
	c, _ := newTestConnWithContexts(t, StateASPDown, RoleASP, 1)
	completeUpAndRCOne(t, c)
	c.startTAck(messages.NewAspDown(nil), requestAspDown)
	c.handleSignals(context.Background(), messages.NewAspDownAck(nil))
	applyUpAckPublications(t, c)
	submitted := make(chan struct{}, 2)
	c.signalWriter = func(m messages.M3UA) (int, error) { submitted <- struct{}{}; return m.MarshalLen(), nil }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	call := startASPTestCall(func() error { return c.ASPUp(ctx) })
	awaitASPTestSend(t, submitted, call, "fresh ASPUp")
	cancel()
	if err := call.wait(t, "fresh Up cancellation"); !errors.Is(err, context.Canceled) {
		t.Fatalf("fresh Up cancellation: %v", err)
	}
	c.handleSignals(context.Background(), messages.NewAspUpAck(nil, nil))
	applyUpAckPublications(t, c)
	if c.State() != StateASPInactive {
		t.Errorf("fresh first Ack left state %v, want main's ASP-INACTIVE", c.State())
	}
	activeCtx := aspTestContext(t)
	active := startASPTestCall(func() error { return c.ASPActive(activeCtx) })
	awaitASPTestSend(t, submitted, active, "ASPActive following fresh Up Ack")
	c.handleSignals(context.Background(), messages.NewAspActiveAck(nil, params.NewRoutingContext(1), nil))
	if err := active.wait(t, "following ASPActive confirmation"); err != nil {
		t.Fatal(err)
	}
}

func TestCompletedUpFallbackPreservesMainIPSPDirections(t *testing.T) {
	for _, model := range []string{"SE", "DE", "shared ASPSM"} {
		t.Run(model, func(t *testing.T) {
			exchange, aspsm := IPSPExchangeDouble, IPSPASPSMExchangeDouble
			if model == "SE" {
				exchange = IPSPExchangeSingle
			}
			if model == "shared ASPSM" {
				aspsm = IPSPASPSMExchangeSingle
			}
			c, sent := newIPSPDownAssociationForTest(t, exchange, aspsm, StateASPDown)
			c.cfg.ASPProcedures = explicitASPProcedurePolicy()
			c.cfg.TAck = time.Hour
			c.recvStream.Store(0)
			if _, err := c.beginASPSM(); err != nil {
				t.Fatal(err)
			}
			c.handleSignals(context.Background(), messages.NewAspUpAck(nil, nil))
			rc := c.configuredLocalRoutingContextParam()
			c.startTAck(messages.NewAspActive(nil, rc, nil), requestAspActive)
			c.handleSignals(context.Background(), messages.NewAspActiveAck(nil, rc, nil))
			applyUpAckPublications(t, c)
			want := c.IPSPState()
			if model != "DE" {
				want.TrafficToPeer = StateASPInactive
			}
			if model == "SE" {
				want.TrafficToLocal = StateASPInactive
			}
			before := len(*sent)
			c.handleSignals(context.Background(), messages.NewAspUpAck(params.NewAspIdentifier(99), nil))
			applyUpAckPublications(t, c)
			if got := c.IPSPState(); got != want {
				t.Errorf("completed Up fallback %+v, want main's %+v", got, want)
			}
			if id, set := c.PeerASPIdentifier(); !set || id != 99 {
				t.Errorf("non-duplicate identifier %d/%v, want main's updated 99", id, set)
			}
			if firstErr(c) != nil || len(*sent) != before {
				t.Error("completed Up fallback reported an error or wrote control")
			}
		})
	}
}
