package m3ua

import (
	"context"
	"errors"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestWithdrawalReportsDynamicAndContextlessIdentity(t *testing.T) {
	for _, down := range []bool{false, true} {
		t.Run(map[bool]string{false: "Up Ack", true: "Down Ack"}[down], func(t *testing.T) {
			c, _ := recoveryASP(t, StateASPActive, false)
			// Contextless configured membership coexists with dynamically
			// registered numeric ASes, including an explicitly present zero.
			keys := []ASKey{
				{NetworkAppearance: 10, NetworkAppearanceSet: true},
				{NetworkAppearance: 0, NetworkAppearanceSet: true, RoutingContext: 0, RoutingContextSet: true},
				{NetworkAppearance: 10, NetworkAppearanceSet: true, RoutingContext: 7, RoutingContextSet: true},
				{NetworkAppearance: 20, NetworkAppearanceSet: true, RoutingContext: 8, RoutingContextSet: true},
			}
			c.cfg.ApplicationServers = []ASConfig{{ASKey: keys[0], TrafficMode: params.TrafficModeLoadshare}}
			for _, key := range keys[1:] {
				c.addDynamicASKey(key, RoutingKey{}, false)
			}
			c.noteRoutingContextsAcked(nil)
			want := keys
			m := messages.M3UA(messages.NewAspUpAck(nil, nil))
			if down {
				m = messages.NewAspDownAck(nil)
			}
			dispatchRecoveryAck(t, c, m)
			recoveryIndication(t, c, "ASP-WITHDRAWAL", want, StateASPActive)
		})
	}
}

func TestUnsolicitedDownAckRetiresPendingStartup(t *testing.T) {
	c, sent := newTestConnWithContexts(t, StateASPDown, RoleASP, 1, 2)
	c.cfg.TAck = aspTestWait * 100
	c.cfg.ASPProcedures = ipspInitiationPolicy(true, true)
	c.recvStream.Store(0)
	req, err := c.beginASPSM()
	if err != nil {
		t.Fatal(err)
	}
	*sent = nil
	dispatchRecoveryAck(t, c, messages.NewAspDownAck(nil))
	select {
	case <-req.stop:
	default:
		t.Fatal("Down Ack left startup Up pending")
	}
	select {
	case err := <-req.result:
		if !errors.Is(err, ErrASPProcedureSuperseded) {
			t.Errorf("startup result %v", err)
		}
	default:
		t.Fatal("Down Ack left ASPUp caller waiting")
	}
	if _, err := c.writePendingControl(context.Background(), req, req.msg); err != nil && !errors.Is(err, ErrASPProcedureSuperseded) {
		t.Fatal(err)
	}
	if len(*sent) != 0 {
		t.Errorf("old startup retried after withdrawal: %v", typeNames(*sent))
	}
}

func TestWithdrawalRetiresPendingActivation(t *testing.T) {
	for _, down := range []bool{false, true} {
		t.Run(map[bool]string{false: "Up Ack", true: "Down Ack"}[down], func(t *testing.T) {
			c, sent := recoveryASP(t, StateASPActive, true)
			requests, err := c.beginASPActive(params.NewRoutingContext(2))
			if err != nil {
				t.Fatal(err)
			}
			*sent = nil
			m := messages.M3UA(messages.NewAspUpAck(nil, nil))
			if down {
				m = messages.NewAspDownAck(nil)
			}
			dispatchRecoveryAck(t, c, m)
			for _, req := range requests {
				select {
				case <-req.stop:
				default:
					t.Fatal("withdrawal left an Active retry pending")
				}
				select {
				case err := <-req.result:
					if !errors.Is(err, ErrASPProcedureSuperseded) {
						t.Errorf("activation result %v", err)
					}
				default:
					t.Fatal("withdrawal left Active waiter pending")
				}
				if _, err := c.writePendingControl(context.Background(), req, req.msg); err != nil && !errors.Is(err, ErrASPProcedureSuperseded) {
					t.Fatal(err)
				}
			}
			if countType(*sent, "ASP Active") != 0 {
				t.Error("withdrawal allowed an old Active retry")
			}
			recoveryIndication(t, c, "ASP-WITHDRAWAL", []ASKey{routingContextASKey(1)}, StateASPActive)
		})
	}
}

func TestInvalidActiveAckDoesNotReportGrant(t *testing.T) {
	for _, ack := range []*messages.AspActiveAck{messages.NewAspActiveAck(nil, params.NewRoutingContext(99), nil), messages.NewAspActiveAck(params.NewTrafficModeType(99), params.NewRoutingContext(2), nil)} {
		c, _ := recoveryASP(t, StateASPInactive, false)
		dispatchRecoveryAck(t, c, ack)
		if c.State() != StateASPInactive || c.routingContextAcked(2) || len(c.mgmtChan) != 0 {
			t.Fatal("invalid Active Ack opened or reported a grant")
		}
	}
}

func FuzzUnsolicitedActiveAckScope(f *testing.F) {
	f.Add(uint32(1), false, false)
	f.Add(uint32(2), true, false)
	f.Add(uint32(0), false, true)
	f.Add(uint32(99), true, false)
	f.Fuzz(func(t *testing.T, rc uint32, active, omitted bool) {
		before := StateASPInactive
		if active {
			before = StateASPActive
		}
		c, _ := recoveryASP(t, before, false)
		scope := params.NewRoutingContext(rc)
		if omitted {
			scope = nil
		}
		c.handleSignals(t.Context(), messages.NewAspActiveAck(nil, scope, nil))
		// RC1 already has solicited duplicate history in this fixture. That copy
		// is absorbed, while an unrequested valid RC2 (or omitted inventory) is a grant.
		accepted := omitted || rc == 2
		if accepted {
			keys := []ASKey{routingContextASKey(2)}
			if omitted {
				keys = []ASKey{routingContextASKey(1), routingContextASKey(2)}
			}
			recoveryIndication(t, c, "ASP-ACTIVATION", keys, before)
			if !c.routingContextAcked(2) {
				t.Fatal("accepted unsolicited grant left RC2 closed")
			}
		} else if len(c.mgmtChan) != 0 {
			t.Fatal("invalid or duplicate Ack reported an activation")
		}
	})
}

func TestUpAckWithdrawalClosesGatesWhenErrorSubmissionFails(t *testing.T) {
	c, _ := recoveryASP(t, StateASPActive, true)
	request := c.startTAck(messages.NewAspActive(nil, params.NewRoutingContext(2), nil), requestAspActive)
	attempted := false
	c.signalWriter = func(m messages.M3UA) (int, error) {
		if _, ok := m.(*messages.Error); ok {
			attempted = true
			return 0, ErrFailedToWriteSignal
		}
		return m.MarshalLen(), nil
	}
	c.handleReceivedSignals(t.Context(), messages.NewAspUpAck(nil, nil), nil)
	if !attempted {
		t.Error("unowed Up Ack did not attempt its required Error")
	}
	if c.State() != StateASPInactive || c.routingContextAcked(1) {
		t.Errorf("failed Error left withdrawn traffic open: %v", c.State())
	}
	if len(c.mgmtChan) != 0 {
		t.Error("withdrawal indication preceded a successful Error submission")
	}
	if err := waitASPTestAck(t, c, request); !errors.Is(err, ErrFailedToWriteSignal) || errors.Is(err, ErrASPProcedureSuperseded) {
		t.Errorf("failed Error exposed an unpublished retirement result: %v", err)
	}
}

func TestUnsolicitedDownAckCommitsBeforeReleasingActiveCaller(t *testing.T) {
	c, _ := recoveryASP(t, StateASPActive, false)
	requests, err := c.beginASPActive(params.NewRoutingContext(2))
	if err != nil {
		t.Fatal(err)
	}
	req := requests[0]
	release := holdASPTestLock(t, c.muState.RLocker())
	call := startASPTestCall(func() error { return c.handleAspDownAck(messages.NewAspDownAck(nil)) })
	if !awaitASPTestStack(t, call, "(*Association).commitStateGuarded(", "sync.(*RWMutex).Lock(") {
		t.Fatal("Down Ack returned before the state commit barrier")
	}
	select {
	case err := <-req.result:
		t.Errorf("Active caller released before DOWN was committed: %v", err)
	default:
	}
	for len(c.mgmtChan) > 0 {
		if ind := <-c.mgmtChan; ind.Kind == ManagementError {
			t.Error("Active failure reported before DOWN was committed")
		}
	}
	release()
	if err := call.wait(t, "Down Ack commit"); err != nil {
		t.Fatal(err)
	}
	if c.State() != StateASPDown {
		t.Fatal("Down Ack did not commit DOWN")
	}
	recoveryIndication(t, c, "ASP-WITHDRAWAL", []ASKey{routingContextASKey(1)}, StateASPActive)
}

func TestUnowedUpAckCommitsInactiveBeforeDispatchPublication(t *testing.T) {
	c, _ := recoveryASP(t, StateASPActive, true)
	if err := c.handleAspUpAck(messages.NewAspUpAck(nil, nil)); err != nil {
		t.Fatal(err)
	}
	if c.State() != StateASPInactive || c.routingContextAcked(1) {
		t.Fatal("withdrawal reported before committing inactive state and gates")
	}
	recoveryIndication(t, c, "ASP-WITHDRAWAL", []ASKey{routingContextASKey(1)}, StateASPActive)
}
