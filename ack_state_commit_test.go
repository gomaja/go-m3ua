package m3ua

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
)

// RFC 4666 §4.2: "Upon successful completion, a corresponding confirm
// primitive is provided". Hold the state commit, not the caller's scheduler.
func TestASPProcedureSuccessWaitsForStateCommit(t *testing.T) {
	for _, model := range []string{"ASP", "IPSP SE", "IPSP DE", "IPSP shared ASPSM"} {
		for _, operation := range []string{"Up", "Active", "Inactive", "Down"} {
			t.Run(model+"/"+operation, func(t *testing.T) {
				initial, want := StateASPDown, StateASPInactive
				switch operation {
				case "Active":
					initial, want = StateASPInactive, StateASPActive
				case "Inactive":
					initial, want = StateASPActive, StateASPInactive
				case "Down":
					initial, want = StateASPActive, StateASPDown
				}
				var c *Association
				if model == "ASP" {
					c, _ = newTestConnWithContexts(t, initial, RoleASP, 1)
				} else {
					exchange, aspsm := IPSPExchangeDouble, IPSPASPSMExchangeDouble
					if model == "IPSP SE" {
						exchange = IPSPExchangeSingle
					}
					if model == "IPSP shared ASPSM" {
						aspsm = IPSPASPSMExchangeSingle
					}
					c, _ = newIPSPDownAssociationForTest(t, exchange, aspsm, initial)
				}
				c.cfg.ASPProcedures = explicitASPProcedurePolicy()
				c.recvStream.Store(0)
				c.cfg.TAck = time.Hour
				endpoint := &Endpoint{associationsByID: map[AssociationID]*Association{1: c}}
				sent := make(chan struct{}, 1)
				c.signalWriter = func(m messages.M3UA) (int, error) { sent <- struct{}{}; return m.MarshalLen(), nil }
				ctx := aspTestContext(t)
				call := startASPTestCall(func() error {
					var err error
					switch operation {
					case "Up":
						err = c.ASPUp(ctx)
					case "Active":
						err = c.ASPActive(ctx)
					case "Inactive":
						err = c.ASPInactive(ctx)
					case "Down":
						err = c.ASPDown(ctx)
					}
					return err
				})
				awaitASPTestSend(t, sent, call, "ASP procedure")
				c.tack.mu.Lock()
				var request *pendingRequest
				for _, p := range c.tack.pending {
					request = p
				}
				c.tack.mu.Unlock()
				if request == nil {
					t.Fatal("no pending request")
				}
				rc := c.configuredLocalRoutingContextParam()
				var ack messages.M3UA
				switch operation {
				case "Up":
					ack = messages.NewAspUpAck(nil, nil)
				case "Active":
					ack = messages.NewAspActiveAck(nil, rc, nil)
				case "Inactive":
					ack = messages.NewAspInactiveAck(rc, nil)
				case "Down":
					ack = messages.NewAspDownAck(nil)
				}
				unlock := holdASPTestLock(t, c.muState.RLocker())
				dispatched := startASPTestCall(func() error { c.handleSignals(ctx, ack); return nil })
				// Match the state commit itself, not an earlier scope-inventory lock.
				commitFrame := "(*Association).commitStateGuarded("
				if c.isIPSPDoubleExchange() && (operation != "Up" || !c.usesSingleASPSMExchange()) {
					commitFrame = "(*Association).commitLocalIPSPState("
				} else if c.role == RoleIPSP && operation == "Active" {
					commitFrame = "(*Association).commitPeerRoutingContextsActive("
				}
				awaitASPTestStack(t, dispatched, commitFrame, "sync.(*RWMutex).Lock(")
				select {
				case <-request.stop:
				default:
					t.Fatal("Ack was not claimed before state commit")
				}
				// finish closes result even if the public waiter already consumed it.
				// Its readability therefore proves premature completion structurally.
				select {
				case err := <-request.result:
					t.Errorf("procedure was confirmed before state commit: %v", err)
				default:
				}
				select {
				case <-call.done:
					t.Errorf("procedure returned %v before state commit", call.err)
				default:
				}
				unlock()
				if err := call.wait(t, "ASP procedure confirmation"); err != nil {
					t.Fatal(err)
				}

				status, ok := endpoint.AssociationStatus(1)
				if !ok {
					t.Fatal("missing AssociationStatus")
				}
				if c.isIPSPDoubleExchange() {
					if c.IPSPState().TrafficToLocal != want || status.IPSPState.TrafficToLocal != want {
						t.Fatalf("local state %v status %+v, want %v", c.IPSPState(), status, want)
					}
					peerWant := initial
					if model == "IPSP shared ASPSM" && (operation == "Up" || operation == "Down") {
						peerWant = want
					}
					if c.State() != peerWant || status.State != peerWant {
						t.Fatalf("peer state %v status %+v, want %v", c.State(), status, peerWant)
					}
				} else if c.State() != want || status.State != want {
					t.Fatalf("state %v status %+v, want %v", c.State(), status, want)
				}
				if err := dispatched.wait(t, "Ack dispatch"); err != nil {
					t.Fatal(err)
				}
				if operation == "Up" {
					next := startASPTestCall(func() error { return c.ASPActive(ctx) })
					awaitASPTestSend(t, sent, next, "composed ASPActive")
					c.handleSignals(ctx, messages.NewAspActiveAck(nil, rc, nil))
					if err := next.wait(t, "composed ASPActive confirmation"); err != nil {
						t.Fatalf("composed ASPActive: %v", err)
					}
				}

			})
		}
	}
}

func TestDuplicateActiveAckDoesNotPublishOrOpenTraffic(t *testing.T) {
	for _, model := range []string{"ASP", "IPSP SE", "IPSP DE", "IPSP shared ASPSM"} {
		t.Run(model, func(t *testing.T) {
			var c *Association
			var sent *[]messages.M3UA
			if model == "ASP" {
				c, sent = newTestConnWithContexts(t, StateASPInactive, RoleASP, 1)
			} else {
				exchange, aspsm := IPSPExchangeDouble, IPSPASPSMExchangeDouble
				if model == "IPSP SE" {
					exchange = IPSPExchangeSingle
				}
				if model == "IPSP shared ASPSM" {
					aspsm = IPSPASPSMExchangeSingle
				}
				c, sent = newIPSPDownAssociationForTest(t, exchange, aspsm, StateASPInactive)
			}
			c.cfg.ASPProcedures = explicitASPProcedurePolicy()
			c.cfg.TAck = time.Hour
			rc := c.configuredLocalRoutingContextParam()
			c.startTAck(messages.NewAspActive(nil, rc, nil), requestAspActive)
			c.handleSignals(context.Background(), messages.NewAspActiveAck(nil, rc, nil))
			c.handleSignals(context.Background(), messages.NewAspInactiveAck(rc, nil))
			for len(c.stateChan) > 0 {
				if err := c.handlePublishedStateUpdate(receiveASPTest(t, c.stateChan, "state publication")); err != nil {
					t.Fatal(err)
				}
			}
			for len(c.mgmtChan) > 0 {
				receiveASPTest(t, c.mgmtChan, "management indication")
			}
			for len(c.stateEventChan) > 0 {
				receiveASPTest(t, c.stateEventChan, "state event")
			}
			before := c.IPSPState()
			writes := len(*sent)
			c.handleSignals(context.Background(), messages.NewAspActiveAck(nil, rc, nil))
			if c.IPSPState() != before {
				t.Fatalf("duplicate changed state from %+v to %+v", before, c.IPSPState())
			}
			for _, context := range rc.RoutingContexts() {
				if c.role == RoleIPSP && !c.isIPSPDoubleExchange() {
					if c.activeForRoutingContext(context) {
						t.Fatal("duplicate reopened peer traffic")
					}
				} else if c.routingContextAcked(context) {
					t.Fatal("duplicate reopened local traffic")
				}
			}
			if len(c.errChan) != 0 || len(c.mgmtChan) != 0 {
				t.Fatal("duplicate reported an event")
			}
			if len(*sent) != writes {
				t.Fatal("duplicate sent a control message")
			}
			if got := receiveASPTest(t, c.stateChan, "duplicate Active publication"); got != stateUnchanged {
				t.Fatalf("duplicate publication %v, want unchanged", got)
			} else if err := c.handlePublishedStateUpdate(got); err != nil {
				t.Fatal(err)
			}
			if len(c.stateEventChan) != 0 || len(c.mgmtChan) != 0 {
				t.Fatal("duplicate published a state or management report")
			}
		})
	}
}

func TestASPProcedureCannotSucceedAfterClosePreventsCommit(t *testing.T) {
	c, _ := newTestConn(t, StateASPDown, RoleASP)
	c.cfg.TAck = time.Hour
	request := c.startTAck(messages.NewAspUp(nil, nil), requestAspUp)
	unlock := holdASPTestLock(t, c.muState.RLocker())
	handled := startASPTestCall(func() error { c.handleSignals(context.Background(), messages.NewAspUpAck(nil, nil)); return nil })
	awaitASPTestStack(t, handled, "(*Association).commitStateGuarded(", "sync.(*RWMutex).Lock(")
	select {
	case <-request.stop:
	default:
		t.Fatal("Ack did not claim Up before the held state commit")
	}
	closed := startASPTestCall(func() error { return c.closeWith(ErrAssociationClosed) })
	awaitASPTestSend(t, c.done, closed, "association close")
	unlock()
	if err := handled.wait(t, "Ack after close"); err != nil {
		t.Fatal(err)
	}
	if err := closed.wait(t, "association close"); err != nil {
		t.Fatal(err)
	}

	for range 100 {
		if err := waitASPTestAck(t, c, request); !errors.Is(err, ErrAssociationClosed) {
			t.Fatalf("procedure result after failed state commit: %v", err)
		}
	}
}
