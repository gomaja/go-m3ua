package m3ua

import (
	"errors"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
)

// No exchange has run yet: callers can configure timers before any worker reads
// them, and the opening fixture starts from an actual unconfirmed DOWN epoch.
func initialSubmissionAssociation(t *testing.T, model string) *Association {
	t.Helper()
	if model == "ASP" {
		c, _ := newTestConnWithContexts(t, StateASPDown, RoleASP, 1)
		return c
	}
	exchange, aspsm := IPSPExchangeDouble, IPSPASPSMExchangeDouble
	if model == "SE" {
		exchange = IPSPExchangeSingle
	}
	if model == "shared DE" {
		aspsm = IPSPASPSMExchangeSingle
	}
	c, _ := newIPSPDownAssociationForTest(t, exchange, aspsm, StateASPDown)
	return c
}

func TestConcurrentASPUpDoesNotCloseOpeningAssociation(t *testing.T) {
	for _, model := range []string{"ASP", "SE", "DE", "shared DE"} {
		t.Run(model, func(t *testing.T) {
			c := initialSubmissionAssociation(t, model)
			c.cfg.ASPProcedures = ipspInitiationPolicy(true, false)
			c.cfg.TAck = time.Hour
			c.stateEntered = false
			c.recvStream.Store(0)
			written := make(chan messages.M3UA, 8)
			c.signalWriter = func(m messages.M3UA) (int, error) { written <- m; return m.MarshalLen(), nil }
			subscribed := make(chan *Association, 1)
			c.cfg.Subscribe = func(a *Association) { subscribed <- a }
			c.subscribeBeforeHandshake()
			exposed := receiveASPTest(t, subscribed, "Subscribe association")
			releaseWrite := holdASPTestLock(t, &c.tack.retryMu)
			// The opening monitor treats this entry error as terminal, before it
			// starts its dispatcher (monitor's socket reader is not needed here).
			opening := startASPTestCall(func() error {
				err := c.handleStateUpdate(StateASPDown)
				if err != nil {
					_ = c.closeWith(err)
				}
				return err
			})
			if !awaitASPTestStack(t, opening, "(*Association).applyStateUpdate(", "lockSlow(") {
				t.Fatalf("opening returned before initial write: %v", opening.err)
			}
			c.tack.mu.Lock()
			var automatic *pendingRequest
			for _, req := range c.tack.pending {
				if req.kind == requestAspUp {
					automatic = req
				}
			}
			c.tack.mu.Unlock()
			if automatic == nil {
				t.Fatal("opening ASP Up was not registered")
			}
			ctx := aspTestContext(t)
			explicit := startASPTestCall(func() error { return exposed.ASPUp(ctx) })
			if awaitASPTestAnyStack(t, explicit,
				[]string{"(*Association).ASPUp(", "(*Association).writePendingControl(", "lockSlow("},
				[]string{"(*Association).ASPUp(", "sync.(*RWMutex).RLock("}) < 0 {
				t.Fatalf("explicit Up returned before the fixture released submission: %v", explicit.err)
			}
			releaseWrite()
			_ = opening.wait(t, "opening transition")
			select {
			case <-c.done:
				t.Fatalf("concurrent ASPUp closed a healthy association: %v", c.Err())
			default:
			}
			for index := range 2 {
				m := awaitASPTestSend(t, written, explicit, "automatic and explicit Up submissions")
				if m.MessageTypeName() != "ASP Up" {
					t.Fatalf("initial write = %s", m.MessageTypeName())
				}
				if index == 0 && m != automatic.msg {
					t.Fatal("automatic Up did not precede replacement submission")
				}
			}
			if err := receiveASPTest(t, automatic.result, "superseded automatic procedure"); !errors.Is(err, ErrASPProcedureSuperseded) {
				t.Errorf("automatic supersession on a live association = %v", err)
			}
			if err := c.handleAspUpAck(messages.NewAspUpAck(nil, nil)); err != nil {
				t.Fatal(err)
			}
			if err := explicit.wait(t, "replacement Up confirmation"); err != nil {
				t.Fatal(err)
			}
			if c.localASPProcedureState() != StateASPInactive || c.pendingTAck() != 0 {
				t.Fatalf("replacement state/pending = %s/%d", c.localASPProcedureState(), c.pendingTAck())
			}
		})
	}
}

func TestAutomaticASPUpSupersedesExplicitCallerAccurately(t *testing.T) {
	c, _ := newTestConnWithContexts(t, StateASPDown, RoleASP, 1)
	c.cfg.ASPProcedures = ipspInitiationPolicy(true, false)
	c.cfg.TAck = time.Hour
	c.stateEntered = false
	c.recvStream.Store(0)
	written := make(chan messages.M3UA, 4)
	c.signalWriter = func(m messages.M3UA) (int, error) { written <- m; return m.MarshalLen(), nil }
	ctx := aspTestContext(t)
	explicit := startASPTestCall(func() error { return c.ASPUp(ctx) })
	awaitASPTestSend(t, written, explicit, "explicit Up")
	if err := c.handleStateUpdate(StateASPDown); err != nil {
		t.Fatal(err)
	}
	if err := explicit.wait(t, "superseded explicit Up"); !errors.Is(err, ErrASPProcedureSuperseded) {
		t.Fatalf("superseded explicit Up on live association = %v", err)
	}
	select {
	case <-c.done:
		t.Fatal("local supersession closed association")
	default:
	}
	if err := c.handleAspUpAck(messages.NewAspUpAck(nil, nil)); err != nil {
		t.Fatal(err)
	}
}

func TestAutomaticActivationPrecedesConcurrentASPDown(t *testing.T) {
	for _, model := range []string{"ASP", "SE"} {
		t.Run(model, func(t *testing.T) {
			c := initialSubmissionAssociation(t, model)
			c.cfg.ASPProcedures = ipspInitiationPolicy(false, true)
			c.cfg.TAck = time.Hour
			c.stateEntered = true
			c.appliedState = StateASPDown
			c.recvStream.Store(0)
			written := make(chan messages.M3UA, 4)
			c.signalWriter = func(m messages.M3UA) (int, error) { written <- m; return m.MarshalLen(), nil }
			release := holdASPTestLock(t, &c.tack.retryMu)
			entry := startASPTestCall(func() error { return c.handleStateUpdate(StateASPInactive) })
			if !awaitASPTestStack(t, entry, "(*Association).applyStateUpdate(", "lockSlow(") {
				t.Fatalf("entry returned before write: %v", entry.err)
			}
			down := startASPTestCall(func() error { return c.ASPDown(aspTestContext(t)) })
			if awaitASPTestAnyStack(t, down, []string{"(*Association).ASPDown(", "(*Association).waitTAck("}, []string{"(*Association).ASPDown(", "sync.(*RWMutex).RLock("}) < 0 {
				t.Fatalf("Down returned before fixture release: %v", down.err)
			}
			release()
			if err := entry.wait(t, "automatic Active submission"); err != nil {
				t.Fatal(err)
			}
			for _, want := range []string{"ASP Active", "ASP Down"} {
				if m := awaitASPTestSend(t, written, down, "Active before Down"); m.MessageTypeName() != want {
					t.Fatalf("automatic entry write order: got %s, want %s", m.MessageTypeName(), want)
				}
			}
			if err := c.handleAspActiveAck(messages.NewAspActiveAck(nil, c.configuredRoutingContextParam(), nil)); err != nil {
				t.Fatal(err)
			}
			if err := c.handleAspDownAck(messages.NewAspDownAck(nil)); err != nil {
				t.Fatal(err)
			}
			if err := down.wait(t, "Down confirmation"); err != nil {
				t.Fatal(err)
			}
			if c.pendingTAck() != 0 || c.localASPProcedureState() != StateASPDown {
				t.Fatalf("Down final state/pending = %s / %d", c.localASPProcedureState(), c.pendingTAck())
			}
		})
	}
}

func TestCloseRejectsLatePendingProcedureRegistration(t *testing.T) {
	c, _ := newTestConnWithContexts(t, StateASPDown, RoleASP, 1)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	request := c.startTAck(messages.NewAspUp(nil, nil), requestAspUp)
	if c.pendingTAck() != 0 {
		t.Fatal("registration after Close left an orphaned request")
	}
	if err := receiveASPTest(t, request.result, "closed registration result"); !errors.Is(err, ErrAssociationClosed) {
		t.Fatalf("closed registration = %v", err)
	}
}

func TestInternalSubmissionDoesNotJoinWithdrawalPublication(t *testing.T) {
	for _, kind := range []string{"Up", "Active"} {
		t.Run(kind, func(t *testing.T) {
			c, sent := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
			c.cfg.TAck = time.Hour
			c.recvStream.Store(0)
			c.ManagementIndications()
			c.noteRoutingContextsAcked(c.configuredRoutingContextParam())
			releaseWrite := holdASPTestLock(t, &c.tack.retryMu)
			call := startASPTestCall(func() error {
				if kind == "Up" {
					return c.initiateASPSM()
				}
				return c.initiateASPTM()
			})
			if !awaitASPTestStack(t, call, "lockSlow(") {
				t.Fatalf("submission returned before registration: %v", call.err)
			}
			c.tack.mu.Lock()
			var request *pendingRequest
			for _, req := range c.tack.pending {
				request = req
			}
			c.tack.mu.Unlock()
			if request == nil {
				t.Fatal("internal request not registered")
			}
			releaseState := holdASPTestLock(t, c.muState.RLocker())
			handler := startASPTestCall(func() error { return c.handleAspDownAck(messages.NewAspDownAck(nil)) })
			select {
			case <-request.stop:
			case <-handler.done:
				t.Fatalf("withdrawal returned before claim: %v", handler.err)
			case <-time.After(aspTestWait):
				t.Fatal("withdrawal did not claim internal request")
			}
			releaseWrite()
			if awaitASPTestStack(t, call, "(*Association).waitTAck(") {
				t.Fatal("internal submission joined withdrawal publication")
			}
			if err := call.wait(t, "internal submission"); err != nil {
				t.Fatalf("retirement made internal submission fatal: %v", err)
			}
			select {
			case err := <-request.result:
				t.Fatalf("internal procedure exposed %v before withdrawal commit", err)
			default:
			}
			if len(*sent) != 0 {
				t.Fatal("retired internal request reached wire")
			}
			releaseState()
			if err := handler.wait(t, "internal withdrawal"); err != nil {
				t.Fatal(err)
			}
			ind := receiveASPTest(t, c.ManagementIndications(), "internal withdrawal report")
			if ind.Kind != ManagementASPWithdrawal || c.State() != StateASPDown {
				t.Fatalf("internal completion boundary = %v / %s", ind.Kind, c.State())
			}
			if err := receiveASPTest(t, request.result, "internal procedure result"); !errors.Is(err, ErrASPProcedureSuperseded) {
				t.Fatalf("internal result = %v", err)
			}
		})
	}
}

func TestShutdownSupersedesPendingProceduresAccurately(t *testing.T) {
	c, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
	c.cfg.TAck = time.Hour
	pending := []*pendingRequest{
		c.startTAck(messages.NewAspUp(nil, nil), requestAspUp),
		c.startTAck(messages.NewAspActive(nil, nil, nil), requestAspActive),
		c.startTAck(messages.NewAspInactive(nil, nil), requestAspInactive),
		c.startTAck(messages.NewAspDown(nil), requestAspDown),
	}
	c.terminating.Store(true)
	c.startTAck(messages.NewAspDown(nil), requestAspDown)
	for _, request := range pending {
		if err := receiveASPTest(t, request.result, "shutdown supersession"); !errors.Is(err, ErrASPProcedureSuperseded) {
			t.Errorf("shutdown superseded %s on live association = %v", request.msg.MessageTypeName(), err)
		}
	}
	select {
	case <-c.done:
		t.Fatal("shutdown registration closed association before withdrawal")
	default:
	}
}
