package m3ua

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

func newSharedUpIPSPForTest(t *testing.T, double, automaticActive bool) *Association {
	t.Helper()
	var c *Association
	if double {
		cfg := newDoubleExchangeAssociationConfigForTest()
		cfg.IPSP.ASPSMExchange = IPSPASPSMExchangeSingle
		cfg.IPSP.TrafficToLocal.ApplicationServers = buildTestInventory(0, false, params.TrafficModeLoadshare, []uint32{1})
		cfg.IPSP.TrafficToPeer.ApplicationServers = buildTestInventory(0, false, params.TrafficModeLoadshare, []uint32{1})
		c, _ = newDoubleExchangeIPSPWithConfigForTest(t, cfg)
	} else {
		c, _ = newSingleExchangeIPSPForTest(t, StateASPDown)
	}
	c.cfg.ASPProcedures = ipspInitiationPolicy(true, automaticActive)
	c.cfg.TAck = time.Hour
	c.recvStream.Store(0)
	c.maxMessageStreamID = 4
	c.stateEntered = true
	c.appliedState = StateASPDown
	return c
}

func dispatchSharedUpControl(t *testing.T, c *Association, m messages.M3UA) {
	t.Helper()
	c.handleSignals(context.Background(), m)
	applyUpAckPublications(t, c)
}

func TestSimultaneousIPSPUpCannotRetryAfterActive(t *testing.T) {
	for _, double := range []bool{false, true} {
		for _, automatic := range []bool{false, true} {
			t.Run(map[bool]string{false: "SE", true: "shared DE"}[double]+map[bool]string{false: "/explicit", true: "/automatic"}[automatic], func(t *testing.T) {
				a, b := newSharedUpIPSPForTest(t, double, automatic), newSharedUpIPSPForTest(t, double, false)
				ab, ba := make(chan messages.M3UA, 32), make(chan messages.M3UA, 32)
				a.signalWriter = func(m messages.M3UA) (int, error) { ab <- m; return m.MarshalLen(), nil }
				b.signalWriter = func(m messages.M3UA) (int, error) { ba <- m; return m.MarshalLen(), nil }
				up, err := a.beginASPSM()
				if err != nil {
					t.Fatal(err)
				}
				if _, err := b.beginASPSM(); err != nil {
					t.Fatal(err)
				}
				aFirst, bFirst := receiveASPTest(t, ab, "A initial Up"), receiveASPTest(t, ba, "B initial Up")
				// Both initial Ups were submitted. Hold B's replies while A processes
				// B's Up and starts Active, just as with replies delayed past T(ack).
				dispatchSharedUpControl(t, b, aFirst)
				dispatchSharedUpControl(t, a, bFirst)
				if !automatic {
					if _, err := a.beginASPActive(params.NewRoutingContext(1)); err != nil {
						t.Fatal(err)
					}
				}
				activeSeen := false
				drain := func() {
					for len(ab) > 0 {
						m := receiveASPTest(t, ab, "A control trace")
						if _, ok := m.(*messages.AspActive); ok {
							activeSeen = true
						}
						if _, ok := m.(*messages.AspUp); ok && activeSeen {
							t.Error("A wrote ASP Up after ASP Active")
						}
						dispatchSharedUpControl(t, b, m)
					}
				}
				drain()
				if !activeSeen {
					t.Fatal("A did not send ASP Active")
				}
				// Force the same initial/retry write fence used by runTAck, without a
				// scheduler-dependent timer race. A completed request must submit nothing.
				if _, err := a.writePendingControl(up, up.msg); err != nil {
					t.Fatal(err)
				}
				drain()
				for len(ba) > 0 {
					dispatchSharedUpControl(t, a, receiveASPTest(t, ba, "B control trace"))
				}
				if !double {
					if a.State() != StateASPActive || b.State() != a.State() {
						t.Errorf("SE peers disagree: A=%v B=%v", a.State(), b.State())
					}
				} else {
					if a.IPSPState().TrafficToLocal != StateASPActive || a.IPSPState().TrafficToLocal != b.State() || b.IPSPState().TrafficToLocal != a.State() {
						t.Errorf("DE directions disagree: A=%+v B=%+v", a.IPSPState(), b.IPSPState())
					}
				}
				sender, receiver := a, b
				if double {
					sender, receiver = b, a
				} // A's Active establishes B -> A in DE.
				sender.dataWriter = func(raw []byte, info *sctp.SndInfo) (int, error) {
					m, err := messages.Parse(raw)
					if err != nil {
						return 0, err
					}
					receiver.recvStream.Store(uint32(info.Stream))
					receiver.handleData(context.Background(), m.(*messages.Data), nil)
					return len(raw), nil
				}
				if _, err := sender.WriteData(DataRequest{AS: associationConfigASKey(sender.cfg, 1), Stream: 1, ProtocolData: simpleProtocolData("after-simultaneous-up")}); err != nil {
					t.Errorf("established DATA write: %v", err)
				}
				if len(receiver.dataChan) != 1 {
					t.Error("peer did not accept DATA after simultaneous Up")
				}
			})
		}
	}
}

func TestPeerUpCompletesASPUpOnlyAfterStateCommit(t *testing.T) {
	for _, double := range []bool{false, true} {
		t.Run(map[bool]string{false: "SE", true: "shared DE"}[double], func(t *testing.T) {
			c := newSharedUpIPSPForTest(t, double, false)
			sent := make(chan struct{}, 4)
			c.signalWriter = func(m messages.M3UA) (int, error) { sent <- struct{}{}; return m.MarshalLen(), nil }
			ctx := aspTestContext(t)
			call := startASPTestCall(func() error { return c.ASPUp(ctx) })
			awaitASPTestSend(t, sent, call, "ASPUp")
			c.tack.mu.Lock()
			req := c.pendingRequestsLocked(requestAspUp)[0]
			c.tack.mu.Unlock()
			unlock := holdASPTestLock(t, c.muState.RLocker())
			handled := startASPTestCall(func() error { c.handleSignals(context.Background(), messages.NewAspUp(nil, nil)); return nil })
			// The handler has either reached the held commit lock or already returned.
			// Inspect the request itself, so a descheduled public waiter cannot hide
			// premature confirmation or cause a false failure.
			commitFrame := "(*Association).commitStateGuarded("
			if double {
				commitFrame = "(*Association).commitLocalIPSPState("
			}
			awaitASPTestStack(t, handled, commitFrame, "sync.(*RWMutex).Lock(")
			select {
			case <-req.stop:
			default:
				t.Fatal("peer Up did not retire the pending Up before the state commit")
			}
			select {
			case err := <-req.result:
				t.Errorf("ASPUp was confirmed before the peer-Up state commit: %v", err)
			default:
			}
			select {
			case <-call.done:
				t.Errorf("ASPUp returned %v before the peer-Up state commit", call.err)
			default:
			}
			unlock()
			if err := handled.wait(t, "peer Up handler"); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-req.result:
				if err != nil {
					t.Fatalf("peer Up completion: %v", err)
				}
			default:
				t.Fatal("peer Up left ASPUp waiting for its own Ack")
			}
			if err := call.wait(t, "peer-confirmed ASPUp"); err != nil {
				t.Errorf("peer Up completed ASPUp with %v", err)
			}
			ep := &Endpoint{associationsByID: map[AssociationID]*Association{1: c}}
			status, ok := ep.AssociationStatus(1)
			if !ok || c.State() != StateASPInactive || status.State != StateASPInactive {
				t.Errorf("peer Up success before visible state: state=%v status=%+v", c.State(), status)
			}
			if double && (c.IPSPState().TrafficToLocal != StateASPInactive || status.IPSPState.TrafficToLocal != StateASPInactive) {
				t.Error("shared DE local state was not committed")
			}
		})
	}
}

func TestPeerUpCompletionRetainsSentCopyAckCredit(t *testing.T) {
	for _, double := range []bool{false, true} {
		t.Run(map[bool]string{false: "SE", true: "shared DE"}[double], func(t *testing.T) {
			c := newSharedUpIPSPForTest(t, double, false)
			up, err := c.beginASPSM()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.writePendingControl(up, up.msg); err != nil {
				t.Fatal(err)
			}
			dispatchSharedUpControl(t, c, messages.NewAspUp(nil, nil))
			c.startTAck(messages.NewAspActive(nil, params.NewRoutingContext(1), nil), requestAspActive)
			dispatchSharedUpControl(t, c, messages.NewAspActiveAck(nil, params.NewRoutingContext(1), nil))
			before := c.IPSPState()
			for range 2 {
				c.handleSignals(context.Background(), messages.NewAspUpAck(nil, nil))
				if c.IPSPState() != before {
					t.Errorf("Ack for an already-sent copy changed %+v to %+v", before, c.IPSPState())
				}
				if got := receiveASPTest(t, c.stateChan, "duplicate publication"); got != stateUnchanged {
					t.Errorf("Ack for an already-sent copy published %v", got)
				}
			}
		})
	}
}

func TestASPUpRegistersBeforeInactiveCanBeCommitted(t *testing.T) {
	c := newSharedUpIPSPForTest(t, false, false)
	ctx := aspTestContext(t)
	sent := make(chan struct{}, 4)
	c.signalWriter = func(m messages.M3UA) (int, error) { sent <- struct{}{}; return m.MarshalLen(), nil }
	unlock := holdASPTestLock(t, &c.tack.mu)
	call := startASPTestCall(func() error { return c.ASPUp(ctx) })
	if !awaitASPTestStack(t, call, "(*Association).startASPSM(") {
		t.Fatalf("ASPUp returned before registration: %v", call.err)
	}
	// Registration is parked on tack.mu. A state writer must be excluded by
	// this public call's admission read lock, independent of scheduling.
	if c.muState.TryLock() {
		c.muState.Unlock()
		t.Error("INACTIVE could commit between ASPUp's DOWN check and its registration")
	}
	unlock()
	awaitASPTestSend(t, sent, call, "ASPUp registration")
	c.commitState(StateASPInactive)
	c.completeRestartASPSM().complete()
	if err := call.wait(t, "registered Up completion"); err != nil {
		t.Errorf("registered Up completion: %v", err)
	}
}

func TestUpCompletionRetiresRegistrationsBeforeStateCommit(t *testing.T) {
	for _, peerUp := range []bool{false, true} {
		models := []string{"ASP", "SE", "DE", "shared DE"}
		if peerUp {
			models = []string{"SE", "shared DE"}
		}
		for _, model := range models {
			t.Run(map[bool]string{false: "Ack/", true: "peer Up/"}[peerUp]+model, func(t *testing.T) {
				var c *Association
				switch model {
				case "ASP":
					c, _ = newTestConnWithContexts(t, StateASPDown, RoleASP, 1)
				case "SE":
					c = newSharedUpIPSPForTest(t, false, false)
				default:
					c = newSharedUpIPSPForTest(t, true, false)
					if model == "DE" {
						c.cfg.IPSP.ASPSMExchange = IPSPASPSMExchangeDouble
					}
				}
				c.cfg.TAck = time.Hour
				c.cfg.ASPProcedures = explicitASPProcedurePolicy()
				c.recvStream.Store(0)
				old, err := c.beginASPSM()
				if err != nil {
					t.Fatal(err)
				}
				unlock := holdASPTestLock(t, c.muState.RLocker())
				done := startASPTestCall(func() error {
					if peerUp {
						c.handleSignals(context.Background(), messages.NewAspUp(nil, nil))
					} else {
						c.handleSignals(context.Background(), messages.NewAspUpAck(nil, nil))
					}
					return nil
				})
				commitFrame := "(*Association).commitStateGuarded("
				if model == "DE" || (peerUp && model == "shared DE") {
					commitFrame = "(*Association).commitLocalIPSPState("
				}
				awaitASPTestStack(t, done, commitFrame, "sync.(*RWMutex).Lock(")
				select {
				case <-old.stop:
				default:
					t.Fatal("original Up was not retired before the state commit")
				}
				// A public Up that already held the state read lock can register here:
				// the original completion has claimed its request, but state is still DOWN.
				fresh, err := c.beginASPSM()
				if err != nil {
					unlock()
					t.Fatal(err)
				}
				unlock()
				if err := done.wait(t, "Up completion handler"); err != nil {
					t.Fatal(err)
				}
				select {
				case err := <-fresh.result:
					if err != nil {
						t.Fatalf("precommit Up completion: %v", err)
					}
				default:
					t.Fatal("Up registered before INACTIVE was not completed")
				}
				if err := waitASPTestAck(t, c, fresh); err != nil {
					t.Errorf("Up registered before INACTIVE was not completed: %v", err)
				}
				if c.localASPProcedureState() != StateASPInactive {
					t.Errorf("confirmed local state %v", c.localASPProcedureState())
				}
			})
		}
	}
}

func TestActiveStartRetiresUpRegisteredDuringCompletion(t *testing.T) {
	for _, model := range []string{"ASP", "SE", "DE", "shared DE"} {
		t.Run(model, func(t *testing.T) {
			var c *Association
			switch model {
			case "ASP":
				c, _ = newTestConnWithContexts(t, StateASPDown, RoleASP, 1)
			case "SE":
				c = newSharedUpIPSPForTest(t, false, false)
			default:
				c = newSharedUpIPSPForTest(t, true, false)
				if model == "DE" {
					c.cfg.IPSP.ASPSMExchange = IPSPASPSMExchangeDouble
				}
			}
			c.cfg.TAck = time.Hour
			c.recvStream.Store(0)
			sent := make(chan messages.M3UA, 16)
			c.signalWriter = func(m messages.M3UA) (int, error) { sent <- m; return m.MarshalLen(), nil }
			if _, err := c.beginASPSM(); err != nil {
				t.Fatal(err)
			}
			confirmation, _, _, err := c.claimASPUpAcknowledgement(func() error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			defer confirmation.complete()
			fresh, err := c.beginASPSM()
			if err != nil {
				t.Fatal(err)
			}
			c.commitState(StateASPInactive)
			if c.isIPSPDoubleExchange() {
				c.commitLocalIPSPState(StateASPInactive)
			}
			// The old handler has committed INACTIVE but has not yet reclaimed this
			// request. An explicit Active can start in precisely this interval.
			if _, err := c.beginASPActive(params.NewRoutingContext(1)); err != nil {
				t.Fatal(err)
			}
			if _, err := c.writePendingControl(fresh, fresh.msg); err != nil {
				t.Fatal(err)
			}
			active := false
			for len(sent) > 0 {
				switch (receiveASPTest(t, sent, "Active control trace")).(type) {
				case *messages.AspActive:
					active = true
				case *messages.AspUp:
					if active {
						t.Error("racing Up followed ASP Active")
					}
				}
			}
			select {
			case err := <-fresh.result:
				if err != nil {
					t.Fatalf("Active-completed Up: %v", err)
				}
			default:
				t.Fatal("Active did not complete the pending Up")
			}
			if err := waitASPTestAck(t, c, fresh); err != nil {
				t.Errorf("Active start left the confirmed Up pending: %v", err)
			}
		})
	}
}

func TestStaleASPActiveCannotCompleteFreshAutomaticUp(t *testing.T) {
	for _, waiting := range []bool{true, false} {
		method := "ASPActive"
		if !waiting {
			method = "ActivateRoutingContexts"
		}
		t.Run(method, func(t *testing.T) {
			c, _ := newTestConnWithContexts(t, StateASPInactive, RoleASP, 1)
			c.cfg.ASPProcedures = ipspInitiationPolicy(true, false)
			c.cfg.TAck = time.Hour
			c.recvStream.Store(0)
			c.stateEntered = true
			c.appliedState = StateASPInactive
			sent := make(chan messages.M3UA, 8)
			c.signalWriter = func(m messages.M3UA) (int, error) { sent <- m; return m.MarshalLen(), nil }
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			unlock := holdASPTestLock(t, &c.muDynamicASKeys)
			call := startASPTestCall(func() error {
				if waiting {
					return c.ASPActive(ctx)
				}
				return c.ActivateRoutingContexts()
			})
			if !awaitASPTestStack(t, call, "(*Association)."+method+"(", "(*Association).dynamicRoutingContexts(") {
				t.Fatalf("Active returned before scope resolution: %v", call.err)
			}
			c.handleSignals(context.Background(), messages.NewAspDownAck(nil))
			applyUpAckPublications(t, c)
			c.tack.mu.Lock()
			up := c.pendingRequestsLocked(requestAspUp)[0]
			c.tack.mu.Unlock()
			for len(sent) > 0 {
				if _, ok := (receiveASPTest(t, sent, "automatic Up")).(*messages.AspActive); ok {
					t.Error("stale Active was sent before scope resolution")
				}
			}
			unlock()
			// A bad admission may send and then wait forever for an Ack. Observe that
			// write as an outcome too, rather than waiting for a procedure deadline.
			select {
			case <-call.done:
			case <-sent:
				t.Error("stale Active was sent after fresh Up")
				cancel()
			case <-time.After(aspTestWait):
				t.Fatal("stale Active neither returned nor wrote a request")
			}
			if err := call.wait(t, "stale Active result"); !errors.Is(err, ErrInvalidState) {
				t.Errorf("stale Active returned %v, want ErrInvalidState", err)
			}
			select {
			case <-up.stop:
				t.Error("stale Active completed the fresh automatic Up without confirmation")
			default:
			}
			for len(sent) > 0 {
				if _, ok := (receiveASPTest(t, sent, "stale control write")).(*messages.AspActive); ok {
					t.Error("stale Active was sent after fresh Up")
				}
			}
		})
	}
}

func TestASPActiveRegistersBeforeDownCanBeCommitted(t *testing.T) {
	for _, waiting := range []bool{true, false} {
		method := "ASPActive"
		if !waiting {
			method = "ActivateRoutingContexts"
		}
		t.Run(method, func(t *testing.T) {
			c, _ := newTestConnWithContexts(t, StateASPInactive, RoleASP, 1)
			c.cfg.TAck = time.Hour
			c.cfg.ASPProcedures = explicitASPProcedurePolicy()
			c.recvStream.Store(0)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			sent := make(chan struct{}, 4)
			c.signalWriter = func(m messages.M3UA) (int, error) { sent <- struct{}{}; return m.MarshalLen(), nil }
			unlock := holdASPTestLock(t, &c.tack.mu)
			call := startASPTestCall(func() error {
				if waiting {
					return c.ASPActive(ctx)
				}
				return c.ActivateRoutingContexts()
			})
			if !awaitASPTestStack(t, call, "(*Association).claimASPUpCompletion(") {
				t.Fatalf("Active returned before registration: %v", call.err)
			}
			if c.muState.TryLock() {
				c.muState.Unlock()
				t.Error("DOWN could commit between Active validation and registration")
			}
			unlock()
			awaitASPTestSend(t, sent, call, "Active registration")
			c.commitState(StateASPDown)
			cancel()
			if err := call.wait(t, "cancel Active fixture"); err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("cancel Active fixture: %v", err)
			}
		})
	}
}

func TestGroupedActiveFailureKeepsSupersededRequestAsOwner(t *testing.T) {
	c, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
	c.cfg.TAck = time.Hour
	c.cfg.ASPProcedures = explicitASPProcedurePolicy()
	c.recvStream.Store(0)
	c.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
	setInventoryTrafficModeType(&c.cfg.ApplicationServers, nil)
	c.trafficModes.freeze(newTrafficModePolicy(&AssociationConfig{ApplicationServers: []ASConfig{
		{ASKey: routingContextASKey(1), TrafficMode: params.TrafficModeLoadshare},
		{ASKey: routingContextASKey(2), TrafficMode: params.TrafficModeBroadcast},
	}}))
	c.signalWriter = func(m messages.M3UA) (int, error) {
		if _, ok := m.(*messages.AspActive); ok {
			if err := c.handleAspInactiveAck(messages.NewAspInactiveAck(params.NewRoutingContext(1), nil)); err != nil {
				return 0, err
			}
		}
		return m.MarshalLen(), nil
	}
	ctx := aspTestContext(t)
	err := startASPTestCall(func() error { return c.ASPActive(ctx) }).wait(t, "grouped Active failure")
	if !errors.Is(err, ErrASPProcedureSuperseded) || errors.Is(err, ErrAssociationClosed) {
		t.Errorf("grouped Active returned %v, want the first group's supersession", err)
	}
	if c.pendingTAck() != 0 {
		t.Error("failed Active left a later group registered")
	}
}

func TestFreshUpStopsActivationsFromPreviousCycle(t *testing.T) {
	for _, model := range []string{"ASP", "SE", "DE", "shared DE"} {
		for _, automatic := range []bool{false, true} {
			t.Run(model+map[bool]string{false: "/public", true: "/automatic"}[automatic], func(t *testing.T) {
				var c *Association
				switch model {
				case "ASP":
					c, _ = newTestConnWithContexts(t, StateASPInactive, RoleASP, 1)
				case "SE":
					c = newSharedUpIPSPForTest(t, false, false)
					c.setState(StateASPInactive)
				default:
					c = newSharedUpIPSPForTest(t, true, false)
					if model == "DE" {
						c.cfg.IPSP.ASPSMExchange = IPSPASPSMExchangeDouble
					}
					c.setIPSPState(IPSPState{TrafficToLocal: StateASPInactive, TrafficToPeer: StateASPInactive})
				}
				c.cfg.TAck = time.Hour
				c.recvStream.Store(0)
				writes := make(chan messages.M3UA, 16)
				c.signalWriter = func(m messages.M3UA) (int, error) { writes <- m; return m.MarshalLen(), nil }
				if err := c.ActivateRoutingContexts(1); err != nil {
					t.Fatal(err)
				}
				c.tack.mu.Lock()
				old := c.pendingRequestsLocked(requestAspActive)[0]
				c.tack.mu.Unlock()
				receiveASPTest(t, writes, "old Active submission") // Submitted before withdrawal.
				// Model a registration made after a Down handler's cancellation pass but
				// before its DOWN commit. A fresh Up must retire that previous-cycle retry.
				if c.isIPSPDoubleExchange() {
					c.setIPSPState(IPSPState{TrafficToLocal: StateASPDown, TrafficToPeer: StateASPDown})
				} else {
					c.setState(StateASPDown)
				}
				ctx := aspTestContext(t)
				var call *aspTestCall
				if automatic {
					c.stateEntered = false
					if err := c.handleStateUpdate(StateASPDown); err != nil {
						t.Fatal(err)
					}
				} else {
					call = startASPTestCall(func() error { return c.ASPUp(ctx) })
				}
				var written messages.M3UA
				if automatic {
					written = receiveASPTest(t, writes, "automatic fresh Up")
				} else {
					written = awaitASPTestSend(t, writes, call, "fresh ASPUp")
				}
				if _, ok := written.(*messages.AspUp); !ok {
					t.Fatal("missing fresh Up")
				}

				c.tack.mu.Lock()
				fresh := c.pendingRequestsLocked(requestAspUp)[0]
				c.tack.mu.Unlock()
				select {
				case err := <-old.result:
					if !errors.Is(err, ErrASPProcedureSuperseded) {
						t.Errorf("old Active result: %v", err)
					}
				default:
					t.Error("previous-cycle activation waiter was not completed")
				}
				if _, err := c.writePendingControl(old, old.msg); err != nil && !errors.Is(err, ErrASPProcedureSuperseded) {
					t.Error(err)
				}
				if len(writes) > 0 {
					t.Error("previous-cycle Active followed the fresh Up")
				}
				select {
				case <-fresh.stop:
					t.Error("previous-cycle Active completed the unconfirmed fresh Up")
				default:
				}
				c.handleSignals(context.Background(), messages.NewAspUpAck(nil, nil))
				if !automatic {
					if err := call.wait(t, "fresh ASPUp result"); err != nil {
						t.Fatal(err)
					}
				}
			})
		}
	}
}
