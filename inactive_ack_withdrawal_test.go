package m3ua

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestInactiveAckLeavesReactivationToApplication(t *testing.T) {
	for _, last := range []bool{false, true} {
		for _, lagged := range []bool{false, true} {
			t.Run(fmt.Sprintf("last=%t/lagged=%t", last, lagged), func(t *testing.T) {
				c, sent := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
				c.cfg.TAck = time.Hour
				c.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
				if last {
					c.noteRoutingContextsUnacked(params.NewRoutingContext(2))
				}
				if lagged {
					c.muState.Lock()
					c.appliedState = StateASPDown
					c.stateEntered = true
					c.muState.Unlock()
				}
				c.handleSignals(context.Background(), messages.NewAspInactiveAck(params.NewRoutingContext(1), nil))
				if err := c.handleStateUpdate(c.State()); err != nil {
					t.Fatal(err)
				}
				if countType(*sent, "ASP Active") != 0 || c.pendingTAck() != 0 || c.resumeAfterStrayAck() {
					t.Fatal("unsolicited withdrawal automatically reactivated traffic")
				}
				if c.routingContextAcked(1) || c.routingContextAcked(2) == last {
					t.Fatal("withdrawal changed the wrong traffic scope")
				}
			})
		}
	}
}

func TestInactiveAckReportsOnlyUnsolicitedScopes(t *testing.T) {
	c, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 0, 1, 2)
	setInventoryNetworkAppearance(&c.cfg.ApplicationServers, params.NewNetworkAppearance(0))
	c.cfg.TAck = time.Hour
	c.noteRoutingContextsAcked(params.NewRoutingContext(0, 1, 2))
	c.startTAck(messages.NewAspInactive(params.NewRoutingContext(2), nil), requestAspInactive)
	indications := c.ManagementIndications()
	ack := params.NewRoutingContext(0, 1, 2)
	c.handleSignals(context.Background(), messages.NewAspInactiveAck(ack, nil))
	select {
	case ind := <-indications:
		if ind.Kind.String() != "ASP-WITHDRAWAL" {
			t.Fatalf("withdrawal kind = %v", ind.Kind)
		}
		want := []ASKey{associationConfigASKey(c.cfg, 0), associationConfigASKey(c.cfg, 1)}
		if !reflect.DeepEqual(ind.ASKeys, want) || !reflect.DeepEqual(ind.RoutingContexts, []uint32{0, 1}) {
			t.Fatalf("unsolicited scope = %+v / %v, want %+v", ind.ASKeys, ind.RoutingContexts, want)
		}
		if c.routingContextAcked(0) || c.routingContextAcked(1) || c.pendingTAck() != 0 {
			t.Fatal("report preceded withdrawal or lost its solicited confirmation")
		}
	default:
		t.Fatal("unsolicited withdrawal was not reported to Layer Management")
	}
}

func TestInactiveAckConfirmationAndDuplicateDoNotReportWithdrawal(t *testing.T) {
	c, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
	c.cfg.TAck = time.Hour
	c.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
	indications := c.ManagementIndications()
	c.startTAck(messages.NewAspInactive(params.NewRoutingContext(1), nil), requestAspInactive)
	for range 2 {
		c.handleSignals(context.Background(), messages.NewAspInactiveAck(params.NewRoutingContext(1), nil))
	}
	select {
	case ind := <-indications:
		t.Fatalf("confirmation or duplicate reported as withdrawal: %+v", ind)
	default:
	}
}

func TestInactiveAckApplicationActivationUsesOrdinaryProcedure(t *testing.T) {
	for _, blocking := range []bool{false, true} {
		t.Run(fmt.Sprint(blocking), func(t *testing.T) {
			c, sent := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
			c.cfg.TAck = time.Hour
			c.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
			c.handleSignals(context.Background(), messages.NewAspInactiveAck(params.NewRoutingContext(1), nil))
			if c.pendingTAck() != 0 || countType(*sent, "ASP Active") != 0 {
				t.Fatal("withdrawal left an automatic recovery procedure")
			}
			if blocking {
				c.signalWriter = func(m messages.M3UA) (int, error) {
					*sent = append(*sent, m)
					if _, ok := m.(*messages.AspActive); ok {
						if c.pendingTAck() != 1 {
							t.Error("activation was not registered before write")
						}
						if err := c.handleAspActiveAck(messages.NewAspActiveAck(nil, params.NewRoutingContext(1), nil)); err != nil {
							t.Error(err)
						}
					}
					return m.MarshalLen(), nil
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := c.ASPActive(ctx, associationConfigASKey(c.cfg, 1)); err != nil {
					t.Fatal(err)
				}
			} else {
				if err := c.ActivateRoutingContexts(1); err != nil {
					t.Fatal(err)
				}
				if c.pendingTAck() != 1 || c.routingContextAcked(1) {
					t.Fatal("application activation did not await its Ack")
				}
				c.handleSignals(context.Background(), messages.NewAspActiveAck(nil, params.NewRoutingContext(1), nil))
			}
			if c.pendingTAck() != 0 || !c.routingContextAcked(1) || !c.routingContextAcked(2) || countType(*sent, "ASP Active") != 1 {
				t.Fatal("application activation did not behave like a first activation")
			}
		})
	}
}

func TestIPSPInactiveAckReportsWithdrawalWithoutReactivation(t *testing.T) {
	for _, double := range []bool{false, true} {
		t.Run(fmt.Sprint(double), func(t *testing.T) {
			var c *Association
			var sent *[]messages.M3UA
			rc := uint32(1)
			if double {
				c, sent = newDoubleExchangeIPSPForTest(t)
				c.setIPSPState(IPSPState{TrafficToLocal: StateASPActive, TrafficToPeer: StateASPActive})
				rc = 11
				c.noteRoutingContextsAcked(params.NewRoutingContext(rc))
			} else {
				c, sent = newSingleExchangeIPSPForTest(t, StateASPActive)
				c.noteRoutingContextsActive([]uint32{rc})
				c.cfg.ASPProcedures = ipspInitiationPolicy(false, true)
				c.muState.Lock()
				c.appliedState = StateASPDown
				c.stateEntered = true
				c.muState.Unlock()
			}
			c.cfg.TAck = time.Hour
			indications := c.ManagementIndications()
			c.handleSignals(context.Background(), messages.NewAspInactiveAck(params.NewRoutingContext(rc), nil))
			if err := c.handleStateUpdate(c.State()); err != nil {
				t.Fatal(err)
			}
			if countType(*sent, "ASP Active") != 0 || c.pendingTAck() != 0 {
				t.Fatal("IPSP withdrawal started automatic activation")
			}
			select {
			case ind := <-indications:
				if ind.Kind.String() != "ASP-WITHDRAWAL" || len(ind.ASKeys) != 1 || ind.ASKeys[0].RoutingContext != rc {
					t.Fatalf("IPSP withdrawal = %+v", ind)
				}
			default:
				t.Fatal("IPSP unsolicited withdrawal was not reported")
			}
			if double {
				if got := c.IPSPState(); got != (IPSPState{TrafficToLocal: StateASPInactive, TrafficToPeer: StateASPActive}) {
					t.Fatal(got)
				}
			} else if c.State() != StateASPInactive {
				t.Fatal(c.State())
			}
		})
	}
}

func TestOmittedInactiveAckReportsContextlessAndDynamicScopes(t *testing.T) {
	c, _ := newTestConnWithContexts(t, StateASPActive, RoleASP)
	c.cfg.TAck = time.Hour
	c.addDynamicASKey(ASKey{RoutingContext: 7, RoutingContextSet: true}, RoutingKey{}, false)
	c.noteRoutingContextsAcked(nil)
	indications := c.ManagementIndications()
	c.handleSignals(context.Background(), messages.NewAspInactiveAck(nil, nil))
	select {
	case ind := <-indications:
		if ind.Kind.String() != "ASP-WITHDRAWAL" || len(ind.ASKeys) != 2 {
			t.Fatalf("omitted scope = %+v", ind)
		}
		numeric, contextless := false, false
		for _, key := range ind.ASKeys {
			if key.RoutingContextSet && key.RoutingContext == 7 {
				numeric = true
			}
			if !key.RoutingContextSet {
				contextless = true
			}
		}
		if !numeric || !contextless {
			t.Fatalf("missing exact contextless/dynamic scope: %+v", ind.ASKeys)
		}
	default:
		t.Fatal("omitted unsolicited withdrawal was not reported")
	}
}

func TestInactiveAckDoesNotSuppressAutomaticStartup(t *testing.T) {
	c, sent := newTestConnWithContexts(t, StateASPDown, RoleASP, 1, 2)
	c.cfg.TAck = time.Hour
	c.recvStream.Store(0)
	// Production applies the initial ASP-DOWN entry before sending ASP Up.
	c.muState.Lock()
	c.appliedState = StateASPDown
	c.stateEntered = true
	c.muState.Unlock()
	c.startTAck(messages.NewAspUp(c.cfg.ASPIdentifier, nil), requestAspUp)
	c.handleSignals(context.Background(), messages.NewAspUpAck(nil, nil))
	if err := c.handleStateUpdate(StateASPInactive); err != nil {
		t.Fatal(err)
	}
	if countType(*sent, "ASP Active") != 1 || c.pendingTAck() != 1 {
		t.Fatalf("automatic activation after ASP Up Ack: sent=%v pending=%d state=%v resume=%v error=%v", typeNames(*sent), c.pendingTAck(), c.State(), c.resumeTo, firstErr(c))
	}
}

func TestInactiveAckReactivatedScopeIsNotSolicitedByOldPartialRequest(t *testing.T) {
	c, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2, 3)
	c.cfg.TAck = time.Hour
	c.noteRoutingContextsAcked(params.NewRoutingContext(1, 2, 3))
	c.startTAck(messages.NewAspInactive(params.NewRoutingContext(1, 2), nil), requestAspInactive)
	c.handleSignals(context.Background(), messages.NewAspInactiveAck(params.NewRoutingContext(1), nil))
	c.startTAck(messages.NewAspActive(nil, params.NewRoutingContext(1), nil), requestAspActive)
	c.handleSignals(context.Background(), messages.NewAspActiveAck(nil, params.NewRoutingContext(1), nil))
	indications := c.ManagementIndications()
	c.handleSignals(context.Background(), messages.NewAspInactiveAck(params.NewRoutingContext(1), nil))
	select {
	case ind := <-indications:
		if ind.Kind.String() != "ASP-WITHDRAWAL" || !reflect.DeepEqual(ind.RoutingContexts, []uint32{1}) {
			t.Fatalf("withdrawal=%+v", ind)
		}
	default:
		t.Fatal("acknowledged portion of an older request hid withdrawal")
	}
	if got := c.pendingTAckRoutingContexts(requestAspInactive); !reflect.DeepEqual(got, []uint32{2}) {
		t.Fatalf("remaining deactivation=%v", got)
	}
}

func TestIPSPPendingFirstDynamicActivationDoesNotAuthorizeWithdrawal(t *testing.T) {
	c, _ := newTestConnWithContexts(t, StateASPActive, RoleIPSP)
	c.cfg.ApplicationServers = nil
	c.cfg.IPSP = &IPSPConfig{ExchangeModel: IPSPExchangeSingle}
	c.cfg.TAck = time.Hour
	c.noteRoutingContextsActive(nil)
	for _, rc := range []uint32{1, 2} {
		c.addDynamicASKey(ASKey{RoutingContext: rc, RoutingContextSet: true}, RoutingKey{}, false)
	}
	c.startTAck(messages.NewAspActive(nil, params.NewRoutingContext(1), nil), requestAspActive)
	c.startTAck(messages.NewAspInactive(params.NewRoutingContext(2), nil), requestAspInactive)
	if c.activeForRoutingContext(1) {
		t.Fatal("fixture dynamic RC1 was already active")
	}
	if err := c.handleAspInactiveAck(messages.NewAspInactiveAck(params.NewRoutingContext(1), nil)); err != nil {
		t.Fatal(err)
	}
	if c.pendingTAck() != 2 || c.activeForRoutingContext(1) || c.State() != StateASPActive {
		t.Fatal("inactive dynamic Ack authorized a withdrawal")
	}
}

func TestIPSPWithdrawalStopsCompetingActivation(t *testing.T) {
	for _, double := range []bool{false, true} {
		t.Run(fmt.Sprint(double), func(t *testing.T) {
			var c *Association
			rc := uint32(1)
			if double {
				c, _ = newDoubleExchangeIPSPForTest(t)
				c.setIPSPState(IPSPState{TrafficToLocal: StateASPActive, TrafficToPeer: StateASPActive})
				rc = 11
				c.noteRoutingContextsAcked(params.NewRoutingContext(rc))
			} else {
				c, _ = newSingleExchangeIPSPForTest(t, StateASPActive)
				c.noteRoutingContextsActive([]uint32{rc})
			}
			c.cfg.TAck = time.Hour
			old := c.startTAck(messages.NewAspActive(nil, params.NewRoutingContext(rc), nil), requestAspActive)
			c.handleSignals(context.Background(), messages.NewAspInactiveAck(params.NewRoutingContext(rc), nil))
			select {
			case <-old.stop:
			default:
				t.Fatal("withdrawal left a competing activation retry live")
			}
		})
	}
}

func TestDelayedPartialInactiveAckDoesNotCancelApplicationReactivation(t *testing.T) {
	for _, contexts := range [][]uint32{{1}, {1, 2}} {
		t.Run(fmt.Sprint(contexts), func(t *testing.T) {
			c, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2, 3)
			c.cfg.TAck = time.Hour
			c.noteRoutingContextsAcked(params.NewRoutingContext(1, 2, 3))
			c.startTAck(messages.NewAspInactive(params.NewRoutingContext(1, 2), nil), requestAspInactive)
			c.handleSignals(context.Background(), messages.NewAspInactiveAck(params.NewRoutingContext(1), nil))
			if err := c.ActivateRoutingContexts(1); err != nil {
				t.Fatal(err)
			}
			var active *pendingRequest
			c.tack.mu.Lock()
			for _, req := range c.tack.pending {
				if req.kind == requestAspActive {
					active = req
				}
			}
			c.tack.mu.Unlock()
			if active == nil {
				t.Fatal("application activation was not started")
			}
			if len(contexts) == 1 && !c.isRepeatedASPTMAcknowledgement(requestAspInactive, params.NewRoutingContext(1)) {
				t.Fatal("acknowledged Inactive portion still counts as an outstanding request")
			}
			c.handleSignals(context.Background(), messages.NewAspInactiveAck(params.NewRoutingContext(contexts...), nil))
			select {
			case <-active.stop:
				t.Fatal("delayed partial Inactive Ack canceled application reactivation before its Ack")
			default:
			}
			c.handleSignals(context.Background(), messages.NewAspActiveAck(nil, params.NewRoutingContext(1), nil))
			want := []uint32{2}
			if len(contexts) == 2 {
				want = []uint32{}
			}
			if !c.routingContextAcked(1) || !reflect.DeepEqual(c.pendingTAckRoutingContexts(requestAspInactive), want) {
				t.Fatal("ordinary reactivation lost its scope or remaining deactivation")
			}
		})
	}
}

func TestOmittedPartialInactiveAckPreservesIndependentApplicationActivation(t *testing.T) {
	c, _ := newTestConnWithContexts(t, StateASPActive, RoleASP)
	c.cfg.ApplicationServers = nil
	c.cfg.TAck = time.Hour
	for _, rc := range []uint32{1, 2} {
		c.addDynamicASKey(ASKey{RoutingContext: rc, RoutingContextSet: true}, RoutingKey{}, false)
	}
	c.noteRoutingContextsAcked(nil)
	c.startTAck(messages.NewAspInactive(nil, nil), requestAspInactive)
	c.handleSignals(context.Background(), messages.NewAspInactiveAck(params.NewRoutingContext(1), nil))
	if err := c.ActivateRoutingContexts(1); err != nil {
		t.Fatal(err)
	}
	c.handleSignals(context.Background(), messages.NewAspInactiveAck(nil, nil))
	if !reflect.DeepEqual(c.pendingTAckRoutingContexts(requestAspActive), []uint32{1}) {
		t.Fatal("omitted confirmation canceled independent application activation")
	}
	c.handleSignals(context.Background(), messages.NewAspActiveAck(nil, params.NewRoutingContext(1), nil))
	if !c.routingContextAcked(1) || c.pendingTAck() != 0 {
		t.Fatal("application activation did not complete after omitted confirmation")
	}
}

func TestInactiveAckDuringFirstActivationPreservesRequest(t *testing.T) {
	c, _ := newTestConnWithContexts(t, StateASPInactive, RoleASP, 1)
	c.cfg.TAck = time.Hour
	indications := c.ManagementIndications()
	if err := c.ActivateRoutingContexts(1); err != nil {
		t.Fatal(err)
	}
	c.handleSignals(context.Background(), messages.NewAspInactiveAck(params.NewRoutingContext(1), nil))
	select {
	case ind := <-indications:
		t.Fatalf("inactive scope reported: %+v", ind)
	default:
	}
	if c.pendingTAck() != 1 || c.State() != StateASPInactive {
		t.Fatal("inactive-only Ack changed activation request or state")
	}
}
