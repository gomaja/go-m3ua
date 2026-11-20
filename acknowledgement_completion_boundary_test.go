package m3ua

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// Hold registration's write fence, then hold the state commit. The request
// has been retired, but its failure must remain private until publication.
func TestRetiredInitialWritesWaitForWithdrawalPublication(t *testing.T) {
	for _, test := range []struct {
		name  string
		state State
		call  func(*Association, context.Context) error
	}{
		{"ASPUp", StateASPDown, func(c *Association, ctx context.Context) error { return c.ASPUp(ctx) }},
		{"ASPActive", StateASPActive, func(c *Association, ctx context.Context) error { return c.ASPActive(ctx) }},
		{"ASPInactive", StateASPActive, func(c *Association, ctx context.Context) error { return c.ASPInactive(ctx) }},
		{"ActivateRoutingContexts", StateASPActive, func(c *Association, _ context.Context) error { return c.ActivateRoutingContexts(1) }},
		{"DeactivateRoutingContexts", StateASPActive, func(c *Association, _ context.Context) error { return c.DeactivateRoutingContexts(1) }},
		{"automaticInactive", StateASPActive, func(c *Association, _ context.Context) error { return c.initiateASPInactive(nil) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, sent := newTestConnWithContexts(t, test.state, RoleASP, 1)
			c.cfg.TAck = time.Hour
			c.recvStream.Store(0)
			c.ManagementIndications()
			if test.state == StateASPActive {
				c.noteRoutingContextsAcked(params.NewRoutingContext(1))
			}
			releaseWrite := holdASPTestLock(t, &c.tack.retryMu)
			ctx := aspTestContext(t)
			call := startASPTestCall(func() error { return test.call(c, ctx) })
			if !awaitASPTestStack(t, call, "(*Association).writePendingControl(", "lockSlow(") {
				t.Fatalf("procedure did not register: %v", call.err)
			}
			c.tack.mu.Lock()
			var request *pendingRequest
			for _, pending := range c.tack.pending {
				request = pending
			}
			c.tack.mu.Unlock()
			if request == nil {
				t.Fatal("procedure not registered")
			}
			releaseState := holdASPTestLock(t, c.muState.RLocker())
			handler := startASPTestCall(func() error { return c.handleAspDownAck(messages.NewAspDownAck(nil)) })
			select {
			case <-request.stop:
			case <-handler.done:
				t.Fatalf("withdrawal returned before claim: %v", handler.err)
			case <-time.After(aspTestWait):
				t.Fatal("withdrawal did not claim request")
			}
			releaseWrite()
			if !awaitASPTestStack(t, handler, "(*Association).commitStateGuarded(", "sync.(*RWMutex).Lock(") {
				t.Fatalf("withdrawal did not reach commit: %v", handler.err)
			}
			if !awaitASPTestStack(t, call, "(*Association).waitTAck(") {
				t.Fatalf("retired procedure exposed %v before state commit and withdrawal report", call.err)
			}
			select {
			case ind := <-c.ManagementIndications():
				t.Fatalf("early management report: %+v", ind)
			default:
			}
			releaseState()
			if err := handler.wait(t, "withdrawal"); err != nil {
				t.Fatal(err)
			}
			if err := call.wait(t, "retired procedure"); !errors.Is(err, ErrASPProcedureSuperseded) {
				t.Fatalf("retired procedure = %v", err)
			}
			if c.State() != StateASPDown {
				t.Fatalf("returned with state %s", c.State())
			}
			ind := receiveASPTest(t, c.ManagementIndications(), "withdrawal report")
			if ind.Kind != ManagementASPWithdrawal {
				t.Fatalf("procedure failure preceded withdrawal: %+v", ind)
			}
			if len(*sent) != 0 {
				t.Fatalf("retired initial request reached wire: %v", *sent)
			}
		})
	}
}

func acknowledgementStatusEndpoint(t *testing.T, c *Association) *Endpoint {
	t.Helper()
	e := c.endpoint
	if e == nil {
		e = newSSNMStateEndpoint(t, nil, nil)
	}
	e.as = c.as
	if !e.trackAssociation(c) {
		t.Fatal("track association")
	}
	return e
}

func TestSharedIPSPDownReportFollowsPerASDownCommit(t *testing.T) {
	for _, model := range []string{"SE", "shared DE"} {
		t.Run(model, func(t *testing.T) {
			c, _ := recoveryIPSP(t, model, StateASPActive, StateASPActive)
			c.signalWriter = func(m messages.M3UA) (int, error) { return m.MarshalLen(), nil }
			c.as = newApplicationServersForIPSP(&ApplicationServerConfig{RecoveryTimer: time.Hour})
			t.Cleanup(c.as.close)
			for _, key := range c.configuredASKeys() {
				c.as.get(key).setASPState(c, StateASPActive, time.Hour)
			}
			e := acknowledgementStatusEndpoint(t, c)
			if err := c.handleAspDownAck(messages.NewAspDownAck(nil)); err != nil {
				t.Fatal(err)
			}
			// Deliberately do not run the monitor's queued DOWN publication.
			ind := receiveASPTest(t, c.ManagementIndications(), "Down withdrawal")
			if ind.Kind != ManagementASPWithdrawal {
				t.Fatalf("report = %+v", ind)
			}
			if c.State() != StateASPDown || c.IPSPState() != (IPSPState{TrafficToLocal: StateASPDown, TrafficToPeer: StateASPDown}) {
				t.Fatalf("Down state not committed: %s %+v", c.State(), c.IPSPState())
			}
			for _, key := range c.configuredASKeys() {
				status, ok := e.ASPStatus(ASPStatusKey{Association: c.ID(), AS: key})
				if !ok || !status.PeerStateSet || status.PeerState != StateASPDown {
					t.Errorf("Down report preceded per-AS DOWN commit: %+v", status)
				}
				if status.LocalStateSet && status.LocalState != StateASPDown {
					t.Errorf("local per-AS state = %+v", status)
				}
				as := c.as.get(key)
				as.mu.Lock()
				state, timer, gen, member := as.state, as.recovery, as.recoveryGen, as.asps[c]
				as.mu.Unlock()
				if member != StateASPDown || state != ASPending || timer == nil {
					t.Errorf("Down membership/recovery = %v/%v/%v", member, state, timer != nil)
				}
				if timer != nil {
					timer.Stop()
				}
				as.recoveryExpired(gen)
				if as.State() != ASDown {
					t.Errorf("Down membership recovered to %s, want AS-DOWN", as.State())
				}
			}
		})
	}
}

func TestAutomaticEntrySerializesStateThroughSubmission(t *testing.T) {
	for _, role := range []Role{RoleASP, RoleIPSP} {
		for _, state := range []State{StateASPDown, StateASPInactive} {
			t.Run(role.String()+"/"+state.String(), func(t *testing.T) {
				c, _ := newTestConnWithContexts(t, StateASPDown, role, 1)
				if role == RoleIPSP {
					c.cfg.IPSP = &IPSPConfig{ExchangeModel: IPSPExchangeSingle}
				}
				c.cfg.ASPProcedures = ipspInitiationPolicy(true, true)
				c.cfg.TAck = time.Hour
				c.appliedState, c.stateEntered = StateASPDown, state == StateASPInactive
				wrote := false
				c.signalWriter = func(m messages.M3UA) (int, error) {
					wrote = true
					if c.muState.TryLock() {
						c.muState.Unlock()
						t.Error("automatic entry released state before submission")
					}
					return m.MarshalLen(), nil
				}
				if err := c.applyStateUpdate(state); err != nil {
					t.Fatal(err)
				}
				if !wrote {
					t.Fatal("automatic entry did not send control")
				}
			})
		}
	}
}

func TestAcknowledgementReportsFollowEveryStatusView(t *testing.T) {
	for _, model := range []string{"ASP", "SE", "DE", "shared DE"} {
		for _, event := range []string{"Up", "Down", "Active", "Inactive"} {
			t.Run(model+"/"+event, func(t *testing.T) {
				state, want, kind := StateASPActive, StateASPInactive, ManagementASPWithdrawal
				if event == "Active" {
					state, want, kind = StateASPInactive, StateASPActive, ManagementASPActivation
				}
				if event == "Down" {
					want = StateASPDown
				}
				var c *Association
				if model == "ASP" {
					if event == "Active" {
						c, _ = newTestConnWithContexts(t, state, RoleASP, 1, 2)
						c.recvStream.Store(0)
					} else {
						c, _ = recoveryASP(t, state, false)
					}
				} else {
					c, _ = recoveryIPSP(t, model, state, state)
				}
				c.signalWriter = func(m messages.M3UA) (int, error) { return m.MarshalLen(), nil }
				if model == "ASP" {
					c.as = newApplicationServersForSGP(&ApplicationServerConfig{RecoveryTimer: time.Hour}, nil)
				} else {
					c.as = newApplicationServersForIPSP(&ApplicationServerConfig{RecoveryTimer: time.Hour})
				}
				t.Cleanup(c.as.close)
				local, peer := routingContextASKey(1), routingContextASKey(1)
				if model != "ASP" {
					local, peer = recoveryIPSPKeys(model)
				}
				for _, key := range c.configuredASKeys() {
					c.as.get(key).setASPState(c, state, time.Hour)
				}
				// SE traffic has one ledger. This is the state established by receiving
				// Active/Active Ack, without inventing a second directional confirmation.
				if model == "SE" {
					c.noteNoRoutingContextsAcked()
				}
				e := acknowledgementStatusEndpoint(t, c)
				switch event {
				case "Up":
					if err := c.handleAspUpAck(messages.NewAspUpAck(nil, nil)); err != nil {
						t.Fatal(err)
					}
				case "Down":
					if err := c.handleAspDownAck(messages.NewAspDownAck(nil)); err != nil {
						t.Fatal(err)
					}
				case "Active":
					if err := c.handleAspActiveAck(messages.NewAspActiveAck(nil, params.NewRoutingContext(local.RoutingContext), nil)); err != nil {
						t.Fatal(err)
					}
				case "Inactive":
					if err := c.handleAspInactiveAck(messages.NewAspInactiveAck(params.NewRoutingContext(local.RoutingContext), nil)); err != nil {
						t.Fatal(err)
					}
				}
				ind := receiveASPTest(t, c.ManagementIndications(), "acknowledgement report")
				if ind.Kind != kind {
					t.Fatalf("report kind = %v", ind.Kind)
				}
				// No dispatcher publication or monitor repair may precede this check.
				if got := c.localASPProcedureState(); got != want {
					t.Errorf("reported local state = %s, want %s", got, want)
				}
				if c.role == RoleIPSP {
					peerWant := state
					if model == "SE" || (model == "shared DE" && (event == "Up" || event == "Down")) {
						peerWant = want
					}
					if got := c.IPSPState(); got != (IPSPState{TrafficToLocal: want, TrafficToPeer: peerWant}) {
						t.Errorf("reported IPSP state = %+v", got)
					}
					status, ok := e.ASPStatus(ASPStatusKey{Association: c.ID(), AS: peer})
					if !ok || !status.PeerStateSet || status.PeerState != peerWant {
						t.Errorf("report preceded peer membership commit: %+v, want %s", status, peerWant)
					}
				}
				status, ok := e.ASPStatus(ASPStatusKey{Association: c.ID(), AS: local})
				if !ok || !status.LocalStateSet || status.LocalState != want {
					t.Errorf("report preceded local per-AS state commit: %+v, want %s", status, want)
				}
				aggregate, ok := e.AssociationStatus(c.ID())
				if !ok || aggregate.State != c.State() {
					t.Errorf("association status disagrees at indication: %+v", aggregate)
				}
			})
		}
	}
}

func TestShutdownInitialInactiveWaitsForWithdrawalPublication(t *testing.T) {
	c, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
	c.recvStream.Store(0)
	c.cfg.TAck = time.Hour
	c.cfg.ASPProcedures = ipspInitiationPolicy(true, true)
	c.noteRoutingContextsAcked(params.NewRoutingContext(1))
	sender := newFullSendBuffer()
	c.signalWriter, c.controlTransport, c.transportCloser = nil, sender, sender.close
	ctx := aspTestContext(t)
	call := startASPTestCall(func() error { return c.ShutdownContext(ctx) })
	awaitASPTestSend(t, sender.waiting, call, "shutdown ASP Inactive")
	c.tack.mu.Lock()
	var request *pendingRequest
	for _, pending := range c.tack.pending {
		request = pending
	}
	c.tack.mu.Unlock()
	if request == nil {
		t.Fatal("shutdown Inactive was not registered")
	}
	releaseState := holdASPTestLock(t, c.muState.RLocker())
	handler := startASPTestCall(func() error { return c.handleAspDownAck(messages.NewAspDownAck(nil)) })
	select {
	case <-request.stop:
	case <-handler.done:
		t.Fatalf("withdrawal did not claim shutdown: %v", handler.err)
	case <-time.After(aspTestWait):
		t.Fatal("withdrawal did not claim shutdown")
	}
	if !awaitASPTestStack(t, call, "(*Association).waitTAck(") {
		t.Fatalf("shutdown escaped with retired result before publication: %v", call.err)
	}
	releaseState()
	if err := handler.wait(t, "Down withdrawal"); err != nil {
		t.Fatal(err)
	}
	if err := call.wait(t, "retired shutdown"); !errors.Is(err, ErrASPProcedureSuperseded) {
		t.Fatalf("shutdown = %v", err)
	}
	if ind := receiveASPTest(t, c.ManagementIndications(), "Down withdrawal report"); ind.Kind != ManagementASPWithdrawal {
		t.Fatalf("shutdown released before withdrawal: %+v", ind)
	}
}

func TestSingleExchangePartialWithdrawalStatusUsesTrafficLedger(t *testing.T) {
	c, _ := recoveryIPSP(t, "SE", StateASPActive, StateASPActive)
	setInventoryRoutingContexts(&c.cfg.ApplicationServers, params.NewRoutingContext(1, 2))
	c.noteNoRoutingContextsAcked()
	c.noteRoutingContextsActive([]uint32{1, 2})
	c.signalWriter = func(m messages.M3UA) (int, error) { return m.MarshalLen(), nil }
	c.as = newApplicationServersForIPSP(&ApplicationServerConfig{RecoveryTimer: time.Hour})
	t.Cleanup(c.as.close)
	for _, key := range c.configuredASKeys() {
		c.as.get(key).setASPState(c, StateASPActive, time.Hour)
	}
	e := acknowledgementStatusEndpoint(t, c)
	if err := c.handleAspInactiveAck(messages.NewAspInactiveAck(params.NewRoutingContext(1), nil)); err != nil {
		t.Fatal(err)
	}
	if ind := receiveASPTest(t, c.ManagementIndications(), "partial withdrawal"); ind.Kind != ManagementASPWithdrawal {
		t.Fatalf("report = %+v", ind)
	}
	if c.State() != StateASPActive {
		t.Fatalf("unaffected RC2 lost aggregate ACTIVE: %s", c.State())
	}
	for _, rc := range []uint32{1, 2} {
		want := StateASPInactive
		if rc == 2 {
			want = StateASPActive
		}
		status, ok := e.ASPStatus(ASPStatusKey{Association: c.ID(), AS: routingContextASKey(rc)})
		if !ok || status.LocalState != want || status.PeerState != want {
			t.Errorf("partial withdrawal status for RC%d = %+v, want %s", rc, status, want)
		}
	}
}

func TestTerminalCloseWinsReadyRetirementResult(t *testing.T) {
	c, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
	request := c.startTAck(messages.NewAspActive(nil, params.NewRoutingContext(1), nil), requestAspActive)
	c.tack.mu.Lock()
	delete(c.tack.pending, request.msg)
	c.tack.mu.Unlock()
	request.finish(ErrASPProcedureSuperseded)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	// Both channels stay ready. Every selection must preserve the terminal
	// outcome; repeated reads also cover the closed result channel.
	for range 128 {
		if err := c.waitTAck(context.Background(), request); !errors.Is(err, ErrAssociationClosed) {
			t.Fatalf("ready retirement result hid terminal association close: %v", err)
		}
	}
}

func TestSharedWithdrawalCommitsContextlessAndDynamicMemberships(t *testing.T) {
	for _, event := range []string{"Up", "Down", "Inactive"} {
		t.Run(event, func(t *testing.T) {
			c, _ := recoveryIPSP(t, "SE", StateASPActive, StateASPActive)
			setInventoryRoutingContexts(&c.cfg.ApplicationServers, nil)
			c.addDynamicASKey(routingContextASKey(2), RoutingKey{}, false)
			c.noteRoutingContextsActive(nil)
			c.signalWriter = func(m messages.M3UA) (int, error) { return m.MarshalLen(), nil }
			c.as = newApplicationServersForIPSP(&ApplicationServerConfig{RecoveryTimer: time.Hour})
			t.Cleanup(c.as.close)
			keys := c.configuredASKeys()
			if len(keys) != 2 {
				t.Fatalf("contextless/dynamic fixture keys = %+v", keys)
			}
			for _, key := range keys {
				c.as.get(key).setASPState(c, StateASPActive, time.Hour)
			}
			e := acknowledgementStatusEndpoint(t, c)
			if event == "Up" {
				if err := c.handleAspUpAck(messages.NewAspUpAck(nil, nil)); err != nil {
					t.Fatal(err)
				}
			} else if event == "Down" {
				if err := c.handleAspDownAck(messages.NewAspDownAck(nil)); err != nil {
					t.Fatal(err)
				}
			} else if err := c.handleAspInactiveAck(messages.NewAspInactiveAck(nil, nil)); err != nil {
				t.Fatal(err)
			}
			if ind := receiveASPTest(t, c.ManagementIndications(), "mixed-scope withdrawal"); ind.Kind != ManagementASPWithdrawal || len(ind.ASKeys) != 2 {
				t.Fatalf("report = %+v", ind)
			}
			want := StateASPInactive
			if event == "Down" {
				want = StateASPDown
			}
			for _, key := range keys {
				status, ok := e.ASPStatus(ASPStatusKey{Association: c.ID(), AS: key})
				if !ok || status.LocalState != want || status.PeerState != want {
					t.Errorf("withdrawal preceded mixed-scope membership commit: %+v", status)
				}
			}
		})
	}
}

func TestUnsolicitedSingleExchangeGrantReportsOnlyCommittedMemberships(t *testing.T) {
	c, _ := recoveryIPSP(t, "SE", StateASPInactive, StateASPInactive)
	setInventoryRoutingContexts(&c.cfg.ApplicationServers, nil)
	c.addDynamicASKey(routingContextASKey(2), RoutingKey{}, false)
	c.signalWriter = func(m messages.M3UA) (int, error) { return m.MarshalLen(), nil }
	c.as = newApplicationServersForIPSP(&ApplicationServerConfig{RecoveryTimer: time.Hour})
	t.Cleanup(c.as.close)
	for _, key := range c.configuredASKeys() {
		c.as.get(key).setASPState(c, StateASPInactive, time.Hour)
	}
	e := acknowledgementStatusEndpoint(t, c)
	if err := c.handleAspActiveAck(messages.NewAspActiveAck(nil, nil, nil)); err != nil {
		t.Fatal(err)
	}
	ind := receiveASPTest(t, c.ManagementIndications(), "unrequested grant")
	if ind.Kind != ManagementASPActivation || len(ind.ASKeys) != 1 || ind.ASKeys[0] != routingContextASKey(2) {
		t.Errorf("grant report included an uncommitted contextless membership: %+v", ind)
	}
	for _, key := range c.configuredASKeys() {
		want := StateASPInactive
		if key.RoutingContextSet {
			want = StateASPActive
		}
		status, ok := e.ASPStatus(ASPStatusKey{Association: c.ID(), AS: key})
		if !ok || status.LocalState != want || status.PeerState != want {
			t.Errorf("grant status = %+v, want %s", status, want)
		}
	}
}
