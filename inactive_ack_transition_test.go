package m3ua

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// RFC 4666 §4.3.4.4: a new unsolicited withdrawal must still take effect
// after the same AS has completed an Inactive/Active cycle.
func TestASPWithdrawsOnUnsolicitedInactiveAckAfterReactivation(t *testing.T) {
	for _, contexts := range [][]uint32{nil, {0}, {1}, {1, 2}} {
		for _, omitted := range []bool{false, true} {
			name := fmt.Sprint(contexts)
			if omitted {
				name += "/omitted"
			}
			t.Run(name, func(t *testing.T) {
				conn, sent := newTestConnWithContexts(t, StateASPActive, RoleASP, contexts...)
				conn.cfg.TAck = time.Hour
				var rc *params.Param
				if len(contexts) > 0 && !omitted {
					rc = params.NewRoutingContext(contexts...)
				}
				conn.noteRoutingContextsAcked(rc)
				conn.startTAck(messages.NewAspInactive(rc, nil), requestAspInactive)
				conn.handleSignals(context.Background(), messages.NewAspInactiveAck(rc, nil))
				if conn.State() != StateASPInactive {
					t.Fatal("solicited withdrawal did not enter ASP-INACTIVE")
				}
				conn.startTAck(messages.NewAspActive(nil, rc, nil), requestAspActive)
				conn.handleSignals(context.Background(), messages.NewAspActiveAck(nil, rc, nil))
				if conn.State() != StateASPActive {
					t.Fatal("reactivation did not enter ASP-ACTIVE")
				}
				conn.handleSignals(context.Background(), messages.NewAspInactiveAck(rc, nil))
				if conn.State() != StateASPInactive || conn.stateForAcknowledgedRoutingContexts() != StateASPInactive {
					t.Fatalf("fresh withdrawal retained active scope: state=%v traffic=%v", conn.State(), conn.stateForAcknowledgedRoutingContexts())
				}
				if conn.resumeAfterStrayAck() {
					t.Fatal("fresh withdrawal armed automatic recovery")
				}
				if err := conn.handleStateUpdate(StateASPInactive); err != nil {
					t.Fatal(err)
				}
				if countType(*sent, "ASP Active") != 0 || conn.pendingTAck() != 0 {
					t.Fatal("withdrawal did not wait for application activation")
				}
			})
		}
	}
}

func TestASPApplicationReactivationNamesOnlyWithdrawnScope(t *testing.T) {
	conn, sent := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
	conn.cfg.TAck = time.Hour
	conn.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
	rc := params.NewRoutingContext(1)
	conn.startTAck(messages.NewAspInactive(rc, nil), requestAspInactive)
	conn.handleSignals(context.Background(), messages.NewAspInactiveAck(rc, nil))
	conn.startTAck(messages.NewAspActive(nil, rc, nil), requestAspActive)
	conn.handleSignals(context.Background(), messages.NewAspActiveAck(nil, rc, nil))
	conn.handleSignals(context.Background(), messages.NewAspInactiveAck(rc, nil))
	if conn.State() != StateASPActive || conn.routingContextAcked(1) || !conn.routingContextAcked(2) {
		t.Fatal("fresh withdrawal did not deactivate exactly RC 1")
	}
	if len(*sent) != 0 {
		t.Fatal("withdrawal sent automatic activation")
	}
	if err := conn.ActivateRoutingContexts(1); err != nil {
		t.Fatal(err)
	}
	if len(*sent) != 1 {
		t.Fatalf("application sent %v, want one ASP Active", typeNames(*sent))
	}
	active, ok := (*sent)[0].(*messages.AspActive)
	if !ok || len(active.RoutingContext.RoutingContexts()) != 1 || active.RoutingContext.RoutingContext() != 1 {
		t.Fatalf("recovery sent %v, want ASP Active for RC 1", (*sent)[0])
	}
	if conn.pendingTAck() != 1 {
		t.Fatal("recovery did not arm T(ack)")
	}
}

func TestRejectedActiveAckPreservesInactiveAckHistory(t *testing.T) {
	conn, sent := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
	conn.cfg.TAck = time.Hour
	conn.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
	rc := params.NewRoutingContext(1)
	conn.startTAck(messages.NewAspInactive(rc, nil), requestAspInactive)
	conn.handleSignals(context.Background(), messages.NewAspInactiveAck(rc, nil))
	conn.startTAck(messages.NewAspActive(nil, rc, nil), requestAspActive)
	if err := conn.handleAspActiveAck(messages.NewAspActiveAck(params.NewTrafficModeType(99), rc, nil)); err == nil {
		t.Fatal("invalid traffic mode was accepted")
	}
	if !conn.isRepeatedASPTMAcknowledgement(requestAspInactive, rc) {
		t.Fatal("rejected Active Ack erased duplicate recognition")
	}
	conn.handleSignals(context.Background(), messages.NewAspInactiveAck(rc, nil))
	if len(*sent) != 0 || conn.resumeAfterStrayAck() {
		t.Fatal("rejected Active Ack ended the duplicate window")
	}
}

func TestUnsolicitedActiveAckEndsInactiveAckDuplicateWindow(t *testing.T) {
	conn, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
	conn.cfg.TAck = time.Hour
	rc := params.NewRoutingContext(1)
	conn.noteRoutingContextsAcked(rc)
	conn.startTAck(messages.NewAspInactive(rc, nil), requestAspInactive)
	conn.handleSignals(context.Background(), messages.NewAspInactiveAck(rc, nil))
	// The existing handler accepts this Active Ack even without a pending
	// request; its accepted transition must also end the old duplicate window.
	conn.handleSignals(context.Background(), messages.NewAspActiveAck(nil, rc, nil))
	if conn.State() != StateASPActive || !conn.routingContextAcked(1) {
		t.Fatal("unsolicited Active Ack was not accepted")
	}
	conn.handleSignals(context.Background(), messages.NewAspInactiveAck(rc, nil))
	if conn.State() != StateASPInactive || conn.routingContextAcked(1) || conn.resumeAfterStrayAck() {
		t.Fatal("accepted unsolicited Active Ack did not end old Inactive Ack history")
	}
}

func TestIPSPWithdrawsOnInactiveAckAfterLocalReactivation(t *testing.T) {
	for _, doubleExchange := range []bool{false, true} {
		t.Run(map[bool]string{false: "single exchange", true: "double exchange"}[doubleExchange], func(t *testing.T) {
			var conn *Association
			var rc *params.Param
			if doubleExchange {
				conn, _ = newDoubleExchangeIPSPForTest(t)
				conn.setIPSPState(IPSPState{TrafficToLocal: StateASPActive, TrafficToPeer: StateASPActive})
				rc = params.NewRoutingContext(11)
				conn.noteRoutingContextsAcked(rc)
			} else {
				conn, _ = newSingleExchangeIPSPForTest(t, StateASPActive)
				rc = params.NewRoutingContext(1)
				conn.noteRoutingContextsActive([]uint32{1})
			}
			conn.cfg.TAck = time.Hour
			conn.startTAck(messages.NewAspInactive(rc, nil), requestAspInactive)
			conn.handleSignals(context.Background(), messages.NewAspInactiveAck(rc, nil))
			conn.startTAck(messages.NewAspActive(nil, rc, nil), requestAspActive)
			conn.handleSignals(context.Background(), messages.NewAspActiveAck(nil, rc, nil))
			conn.handleSignals(context.Background(), messages.NewAspInactiveAck(rc, nil))
			if doubleExchange {
				if got := conn.IPSPState(); got != (IPSPState{TrafficToLocal: StateASPInactive, TrafficToPeer: StateASPActive}) {
					t.Fatalf("withdrawal state = %+v, want only TrafficToLocal inactive", got)
				}
			} else if conn.State() != StateASPInactive {
				t.Fatalf("fresh withdrawal state = %v, want ASP-INACTIVE", conn.State())
			}
		})
	}
}

func TestASPIgnoresDelayedDuplicateInactiveAck(t *testing.T) {
	conn, sent := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
	conn.cfg.TAck = time.Hour
	conn.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
	rc := params.NewRoutingContext(1)
	conn.startTAck(messages.NewAspInactive(rc, nil), requestAspInactive)
	conn.handleSignals(context.Background(), messages.NewAspInactiveAck(rc, nil))
	// Starting activation is insufficient: only its accepted Ack ends the
	// duplicate window. RC 2 remains active throughout this procedure.
	conn.startTAck(messages.NewAspActive(nil, rc, nil), requestAspActive)
	if !conn.isRepeatedASPTMAcknowledgement(requestAspInactive, rc) {
		t.Fatal("starting activation erased duplicate recognition")
	}
	for range 2 {
		conn.handleSignals(context.Background(), messages.NewAspInactiveAck(rc, nil))
	}
	if conn.routingContextAcked(1) || !conn.routingContextAcked(2) || conn.State() != StateASPActive {
		t.Fatal("duplicate changed scope state")
	}
	if countType(*sent, "ASP Active") != 0 || conn.resumeAfterStrayAck() || conn.pendingTAck() != 1 {
		t.Fatal("delayed duplicate initiated a second withdrawal recovery")
	}
}

func TestInactiveAckHistoryRetiresOnlyReactivatedScopes(t *testing.T) {
	for _, omitted := range []bool{false, true} {
		t.Run(map[bool]string{false: "explicit", true: "omitted"}[omitted], func(t *testing.T) {
			conn, sent := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
			conn.cfg.TAck = time.Hour
			rc := params.NewRoutingContext(1, 2)
			conn.noteRoutingContextsAcked(rc)
			if omitted {
				rc = nil
			}
			conn.startTAck(messages.NewAspInactive(rc, nil), requestAspInactive)
			conn.handleSignals(context.Background(), messages.NewAspInactiveAck(rc, nil))
			conn.startTAck(messages.NewAspActive(nil, params.NewRoutingContext(1, 2), nil), requestAspActive)
			conn.handleSignals(context.Background(), messages.NewAspActiveAck(nil, params.NewRoutingContext(1), nil))
			if !conn.isRepeatedASPTMAcknowledgement(requestAspInactive, params.NewRoutingContext(2)) {
				t.Fatal("partial activation lost RC 2 duplicate recognition")
			}
			if conn.isRepeatedASPTMAcknowledgement(requestAspInactive, params.NewRoutingContext(1)) {
				t.Fatal("reactivation retained RC 1 duplicate recognition")
			}
			conn.handleSignals(context.Background(), messages.NewAspInactiveAck(params.NewRoutingContext(2), nil))
			if countType(*sent, "ASP Active") != 0 || conn.resumeAfterStrayAck() {
				t.Fatal("reactivating RC 1 forgot duplicate history for RC 2")
			}
			conn.handleSignals(context.Background(), messages.NewAspInactiveAck(params.NewRoutingContext(1), nil))
			if conn.routingContextAcked(1) || conn.resumeAfterStrayAck() {
				t.Fatal("reactivated RC 1 ignored a fresh withdrawal")
			}
		})
	}
}

// RFC 4666 §4.3.4.3.1 and §5.6.1: a successful peer Single Exchange
// activation also ends the Inactive Ack duplicate window for its scope.
func TestIPSPSingleExchangePeerReactivationRetiresInactiveAckHistory(t *testing.T) {
	conn, _ := newSingleExchangeIPSPForTest(t, StateASPActive)
	conn.cfg.TAck = time.Hour
	rc := params.NewRoutingContext(1)
	conn.noteRoutingContextsActive([]uint32{1})
	conn.startTAck(messages.NewAspInactive(rc, nil), requestAspInactive)
	conn.handleSignals(context.Background(), messages.NewAspInactiveAck(rc, nil))
	conn.handleSignals(context.Background(), messages.NewAspActive(nil, rc, nil))
	if conn.State() != StateASPActive || !conn.activeForRoutingContext(1) {
		t.Fatal("peer reactivation failed")
	}
	conn.handleSignals(context.Background(), messages.NewAspInactiveAck(rc, nil))
	if conn.State() != StateASPInactive || conn.activeForRoutingContext(1) {
		t.Fatal("fresh withdrawal after peer reactivation retained RC 1 active")
	}
}

// RFC 4666 §4.3.4.4: an independent withdrawal is not an answer to the
// outstanding deactivation of another AS.
func TestASPWithdrawsWhileAnotherScopeAwaitsInactiveAck(t *testing.T) {
	conn, sent := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
	conn.cfg.TAck = time.Hour
	conn.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
	request := conn.startTAck(messages.NewAspInactive(params.NewRoutingContext(2), nil), requestAspInactive)
	if err := conn.handleAspInactiveAck(messages.NewAspInactiveAck(params.NewRoutingContext(1), nil)); err != nil {
		t.Fatalf("valid RC 1 withdrawal rejected while RC 2 pending: %v", err)
	}
	if conn.routingContextAcked(1) || !conn.routingContextAcked(2) {
		t.Fatal("withdrawal did not close exactly RC 1 traffic eligibility")
	}
	select {
	case <-request.stop:
		t.Fatal("unrelated RC 2 request was completed")
	default:
	}
	remaining := conn.pendingTAckRoutingContexts(requestAspInactive)
	if len(remaining) != 1 || remaining[0] != 2 {
		t.Fatalf("pending deactivation scope = %v, want [2]", remaining)
	}
	if countType(*sent, "ASP Active") != 0 {
		t.Fatal("withdrawal initiated automatic activation")
	}

}

// A pending ASP Active is not evidence of an accepted activation. Only an
// active scope or a matching ASP Inactive request authorizes this Ack.
func TestASPPendingFirstActivationDoesNotAuthorizeInactiveAck(t *testing.T) {
	for _, deactivatingOther := range []bool{false, true} {
		t.Run(fmt.Sprint(deactivatingOther), func(t *testing.T) {
			conn, sent := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
			conn.cfg.TAck = time.Hour
			conn.noteRoutingContextsAcked(params.NewRoutingContext(2))
			if deactivatingOther {
				conn.startTAck(messages.NewAspInactive(params.NewRoutingContext(2), nil), requestAspInactive)
			}
			request := conn.startTAck(messages.NewAspActive(nil, params.NewRoutingContext(1), nil), requestAspActive)
			if err := conn.handleAspInactiveAck(messages.NewAspInactiveAck(params.NewRoutingContext(1), nil)); err != nil {
				t.Fatalf("known inactive RC 1 must be ignored: %v", err)
			}
			if conn.routingContextAcked(1) || !conn.routingContextAcked(2) || len(*sent) != 0 {
				t.Fatal("ignored withdrawal changed gates or sent recovery")
			}
			select {
			case <-request.stop:
				t.Fatal("ignored withdrawal stopped the activation request")
			default:
			}
		})
	}
}
