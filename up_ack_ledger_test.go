package m3ua

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
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

func assertUnowedUpAckWithdrawal(t *testing.T, c *Association) {
	t.Helper()
	c.dataWriter = func(b []byte, _ *sctp.SndInfo) (int, error) { return len(b), nil }
	if c.State() != StateASPInactive || c.routingContextAcked(1) {
		t.Errorf("unowed Up Ack did not close state and gates: %v", c.State())
	}
	if _, err := c.WriteData(DataRequest{AS: associationConfigASKey(c.cfg, 1), Stream: 1, ProtocolData: simpleProtocolData("after-up")}); !errors.Is(err, ErrNotEstablished) {
		t.Errorf("unowed Up Ack DATA gate: %v", err)
	}
	recoveryIndication(t, c, "ASP-WITHDRAWAL", []ASKey{routingContextASKey(1)}, StateASPActive)
}

func TestUntrackedLibraryUpDoesNotEarnDuplicateCredit(t *testing.T) {
	c, _ := newTestConnWithContexts(t, StateASPDown, RoleASP, 1)
	completeUpAndRCOne(t, c)
	if _, err := c.writeControl(messages.NewAspUp(nil, nil)); err != nil {
		t.Fatal(err)
	}
	c.handleSignals(context.Background(), messages.NewAspUpAck(nil, nil))
	applyUpAckPublications(t, c)
	assertUnowedUpAckWithdrawal(t, c)
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
	assertUnowedUpAckWithdrawal(t, c)
	if c.routingContextAcked(2) {
		t.Error("Up Ack activated RC2")
	}
	for _, m := range (*sent)[before:] {
		if _, ok := m.(*messages.AspActive); ok {
			t.Errorf("Up Ack sent ASP Active: %v", m)
		}
	}
	if len(*sent) != before+1 || countType((*sent)[before:], "Error") != 1 {
		t.Error("unowed Up Ack did not send exactly one Error")
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
	written := startASPTestCall(func() error { _, err := c.writePendingControl(context.Background(), req, req.msg); return err })
	awaitASPTestSend(t, entered, written, "in-flight library copy")
	c.handleSignals(context.Background(), messages.NewAspUpAck(nil, nil))
	applyUpAckPublications(t, c)
	c.setState(StateASPActive)
	c.noteRoutingContextsAcked(params.NewRoutingContext(1))
	handled := startASPTestCall(func() error {
		c.handleSignals(context.Background(), messages.NewAspUpAck(nil, nil))
		c.handleSignals(context.Background(), messages.NewHeartbeat(params.NewHeartbeatData([]byte("after-up"))))
		return nil
	})
	if awaitASPTestStack(t, handled, "(*Association).handleASPUpAcknowledgement(", "lockSlow(") {
		t.Fatal("in-flight duplicate processing waited for the held retry fence")
	}
	if err := handled.wait(t, "in-flight duplicate processing"); err != nil {
		t.Fatal(err)
	}
	if c.State() != StateASPActive || !c.routingContextAcked(1) {
		t.Fatal("in-flight owed duplicate withdrew active traffic")
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

func TestCancelledFreshUpAckLeavesActivationToApplication(t *testing.T) {
	c, _ := newTestConnWithContexts(t, StateASPDown, RoleASP, 1)
	completeUpAndRCOne(t, c)
	c.startTAck(messages.NewAspDown(nil), requestAspDown)
	c.handleSignals(context.Background(), messages.NewAspDownAck(nil))
	applyUpAckPublications(t, c)
	submitted := make(chan messages.M3UA, 4)
	c.signalWriter = func(m messages.M3UA) (int, error) { submitted <- m; return m.MarshalLen(), nil }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	call := startASPTestCall(func() error { return c.ASPUp(ctx) })
	if _, up := awaitASPTestSend(t, submitted, call, "fresh ASPUp").(*messages.AspUp); !up {
		t.Fatal("fresh ASPUp did not submit Up")
	}
	cancel()
	if err := call.wait(t, "fresh Up cancellation"); !errors.Is(err, context.Canceled) {
		t.Fatalf("fresh Up cancellation: %v", err)
	}
	failure := receiveASPTest(t, c.ManagementIndications(), "cancelled Up procedure report")
	if failure.Kind != ManagementError || !errors.Is(failure.Cause, context.Canceled) {
		t.Fatalf("cancelled Up report: %+v", failure)
	}
	c.handleSignals(context.Background(), messages.NewAspUpAck(nil, nil))
	applyUpAckPublications(t, c)
	if c.State() != StateASPInactive {
		t.Errorf("fresh first Ack left state %v, want main's ASP-INACTIVE", c.State())
	}
	select {
	case m := <-submitted:
		if _, ok := m.(*messages.Error); !ok {
			t.Fatal("fresh cancelled Up Ack did not send Error")
		}
	default:
		t.Fatal("fresh cancelled Up Ack did not send Error")
	}
	recoveryIndication(t, c, "ASP-WITHDRAWAL", nil, StateASPDown)
	activeCtx := aspTestContext(t)
	active := startASPTestCall(func() error { return c.ASPActive(activeCtx) })
	if _, ok := awaitASPTestSend(t, submitted, active, "ASPActive following fresh Up Ack").(*messages.AspActive); !ok {
		t.Fatal("application recovery did not send Active")
	}
	c.handleSignals(context.Background(), messages.NewAspActiveAck(nil, params.NewRoutingContext(1), nil))
	if err := active.wait(t, "following ASPActive confirmation"); err != nil {
		t.Fatal(err)
	}
}

func TestUnowedUpAckUsesIPSPProcedureDirection(t *testing.T) {
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
			want.TrafficToLocal = StateASPInactive
			before := len(*sent)
			c.handleSignals(context.Background(), messages.NewAspUpAck(params.NewAspIdentifier(99), nil))
			applyUpAckPublications(t, c)
			if got := c.IPSPState(); got != want {
				t.Errorf("unowed Up Ack %+v, want %+v", got, want)
			}
			if id, set := c.PeerASPIdentifier(); !set || id != 99 {
				t.Errorf("non-duplicate identifier %d/%v, want main's updated 99", id, set)
			}
			if firstErr(c) != nil || len(*sent) != before+1 || countType((*sent)[before:], "Error") != 1 {
				t.Error("unowed Up Ack did not submit one Error")
			}
			key := routingContextASKey(1)
			if model != "SE" {
				key = ASKey{NetworkAppearance: 10, NetworkAppearanceSet: true, RoutingContext: 11, RoutingContextSet: true}
			}
			recoveryIndication(t, c, "ASP-WITHDRAWAL", []ASKey{key}, StateASPActive)
		})
	}
}
