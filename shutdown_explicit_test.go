package m3ua

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

type shutdownExplicitCall struct {
	name           string
	state          State
	call           func(*Association, context.Context) error
	reportsFailure bool
}

func shutdownExplicitCalls() []shutdownExplicitCall {
	return []shutdownExplicitCall{
		{"ASPUp", StateASPDown, func(a *Association, ctx context.Context) error { return a.ASPUp(ctx) }, true},
		{"ASPActive", StateASPActive, func(a *Association, ctx context.Context) error { return a.ASPActive(ctx) }, true},
		{"ASPInactive", StateASPActive, func(a *Association, ctx context.Context) error { return a.ASPInactive(ctx) }, true},
		{"ASPDown", StateASPActive, func(a *Association, ctx context.Context) error { return a.ASPDown(ctx) }, true},
		{"ActivateRoutingContexts", StateASPActive, func(a *Association, _ context.Context) error { return a.ActivateRoutingContexts() }, false},
		{"DeactivateRoutingContexts", StateASPActive, func(a *Association, _ context.Context) error { return a.DeactivateRoutingContexts() }, false},
	}
}

func TestShutdownRefusesEveryExplicitASPEntryPoint(t *testing.T) {
	for _, tc := range shutdownExplicitCalls() {
		t.Run(tc.name, func(t *testing.T) {
			a, sent := newTestConn(t, tc.state, RoleASP)
			a.cfg.TAck = time.Hour
			a.terminating.Store(true)
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			err := tc.call(a, ctx)
			if !errors.Is(err, ErrASPProcedureSuperseded) || err.Error() != "ASP procedure superseded by orderly shutdown" {
				t.Errorf("%s during shutdown = %v, want orderly-shutdown ErrASPProcedureSuperseded", tc.name, err)
			}
			if len(*sent) != 0 || a.pendingTAck() != 0 {
				t.Errorf("refused %s wrote %v and left %d pending requests", tc.name, typeNames(*sent), a.pendingTAck())
			}
			if tc.reportsFailure {
				var indication *ManagementIndication
				select {
				case indication = <-a.ManagementIndications():
				default:
					t.Fatal("missing procedure-failure ManagementError")
				}
				if indication.Kind != ManagementError || !errors.Is(indication.Cause, ErrASPProcedureSuperseded) {
					t.Errorf("failure indication = %+v", indication)
				}
			} else {
				select {
				case indication := <-a.ManagementIndications():
					t.Errorf("routing-context refusal published unexpected indication: %+v", indication)
				default:
				}
			}
		})
	}
}

func shutdownObservedAssociation(t *testing.T, model string) (*Association, <-chan messages.M3UA) {
	t.Helper()
	var a *Association
	if model == "IPSP-double" {
		a, _ = newDoubleExchangeIPSPForTest(t)
		a.setIPSPState(IPSPState{TrafficToLocal: StateASPActive, TrafficToPeer: StateASPActive})
	} else {
		role := RoleASP
		if model == "IPSP-single" {
			role = RoleIPSP
		}
		a, _ = newTestConn(t, StateASPActive, role)
		if role == RoleIPSP {
			a.cfg.IPSP = &IPSPConfig{ExchangeModel: IPSPExchangeSingle}
		}
	}
	a.cfg.ASPProcedures = &ASPProcedurePolicy{ASPUp: ASPProcedureAutomatic, ASPActive: ASPProcedureAutomatic, ASPInactive: ASPProcedureAutomatic, ASPDown: ASPProcedureAutomatic}
	a.cfg.TAck = time.Hour
	writes := make(chan messages.M3UA, 16)
	a.signalWriter = func(message messages.M3UA) (int, error) { writes <- message; return message.MarshalLen(), nil }
	return a, writes
}

func receiveShutdownWithdrawal(t *testing.T, writes <-chan messages.M3UA, done <-chan error) messages.M3UA {
	t.Helper()
	select {
	case message := <-writes:
		return message
	case err := <-done:
		t.Fatalf("shutdown returned before its withdrawal: %v", err)
	case <-time.After(aspTestWait):
		t.Fatal("shutdown did not send its withdrawal")
	}
	return nil
}

func startHeldWithdrawal(t *testing.T, a *Association, writes <-chan messages.M3UA) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	go func() { done <- a.ShutdownContext(ctx) }()
	if message := receiveShutdownWithdrawal(t, writes, done); message.MessageTypeName() != "ASP Inactive" {
		t.Fatalf("first withdrawal = %T, want ASP Inactive", message)
	}
	return done
}

func finishHeldWithdrawal(t *testing.T, a *Association, writes <-chan messages.M3UA, done <-chan error) {
	t.Helper()
	a.handleSignals(context.Background(), messages.NewAspInactiveAck(a.configuredLocalRoutingContextParam(), nil))
	if message := receiveShutdownWithdrawal(t, writes, done); message.MessageTypeName() != "ASP Down" {
		t.Fatalf("next shutdown message = %T, want ASP Down", message)
	}
	a.handleSignals(context.Background(), messages.NewAspDownAck(nil))
	if err := receiveASPTest(t, done, "shutdown completion"); err != nil {
		t.Fatalf("ShutdownContext: %v", err)
	}
	select {
	case message := <-writes:
		t.Errorf("extra message after shutdown: %T", message)
	default:
	}
}

func TestShutdownRefusesExplicitASPProcedureWaitingOnGate(t *testing.T) {
	a, writes := shutdownObservedAssociation(t, "ASP")
	var registrations atomic.Int32
	a.beforeASPProcedureRegistration = func() { registrations.Add(1) }
	release, err := a.acquireExplicitASPProcedure(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	observed := make(chan struct{}, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	waiting := &observedDoneContext{Context: ctx, observed: observed}
	result := make(chan error, 1)
	go func() { result <- a.ASPActive(waiting) }()
	receiveASPTest(t, observed, "caller waiting on procedure gate")
	done := startHeldWithdrawal(t, a, writes)
	release()
	if err := receiveASPTest(t, result, "queued procedure refusal"); !errors.Is(err, ErrASPProcedureSuperseded) {
		t.Fatalf("queued ASPActive = %v, want ErrASPProcedureSuperseded", err)
	}
	if got := registrations.Load(); got != 0 {
		t.Fatalf("queued call advanced to registration %d times", got)
	}
	finishHeldWithdrawal(t, a, writes, done)
}

func TestShutdownRefusesASPProcedurePausedBeforeRegistration(t *testing.T) {
	for _, tc := range shutdownExplicitCalls() {
		if tc.name == "ASPUp" {
			continue
		} // Up is valid only in DOWN; covered separately below.
		t.Run(tc.name, func(t *testing.T) {
			a, writes := shutdownObservedAssociation(t, "ASP")
			entered, resume := make(chan struct{}), make(chan struct{})
			var paused atomic.Bool
			a.beforeASPProcedureRegistration = func() {
				if paused.CompareAndSwap(false, true) {
					close(entered)
					<-resume
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- tc.call(a, ctx) }()
			receiveASPTest(t, entered, "validated caller before registration")
			done := startHeldWithdrawal(t, a, writes)
			close(resume)
			if err := receiveASPTest(t, result, "registration refusal"); !errors.Is(err, ErrASPProcedureSuperseded) {
				t.Fatalf("%s registered after withdrawal: %v", tc.name, err)
			}
			select {
			case message := <-writes:
				t.Fatalf("refused call reached writer: %T", message)
			default:
			}
			if got := a.pendingTAck(); got != 1 {
				t.Fatalf("pending requests = %d, want shutdown's own request only", got)
			}
			finishHeldWithdrawal(t, a, writes, done)
		})
	}
}

func TestShutdownRefusesASPUpPausedBeforeRegistration(t *testing.T) {
	a, sent := newTestConn(t, StateASPDown, RoleASP)
	a.cfg.TAck = time.Hour
	entered, resume := make(chan struct{}), make(chan struct{})
	a.beforeASPProcedureRegistration = func() { close(entered); <-resume }
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- a.ASPUp(ctx) }()
	receiveASPTest(t, entered, "Up before registration")
	// Keep the association open to isolate registration admission from Close.
	a.terminating.Store(true)
	close(resume)
	if err := receiveASPTest(t, result, "Up refusal"); !errors.Is(err, ErrASPProcedureSuperseded) {
		t.Fatalf("paused Up = %v", err)
	}
	if len(*sent) != 0 || a.pendingTAck() != 0 {
		t.Fatalf("refused Up wrote %v and left %d requests", typeNames(*sent), a.pendingTAck())
	}
}

func TestShutdownOwnsWithdrawalForASPAndIPSP(t *testing.T) {
	for _, model := range []string{"ASP", "IPSP-single", "IPSP-double"} {
		t.Run(model, func(t *testing.T) {
			a, writes := shutdownObservedAssociation(t, model)
			done := startHeldWithdrawal(t, a, writes)
			for _, tc := range shutdownExplicitCalls() {
				if err := tc.call(a, context.Background()); !errors.Is(err, ErrASPProcedureSuperseded) {
					t.Fatalf("%s during %s shutdown = %v", tc.name, model, err)
				}
			}
			finishHeldWithdrawal(t, a, writes, done)
		})
	}
}

func TestShutdownRefusesExplicitASPBeforeWaitingOnGate(t *testing.T) {
	for _, tc := range shutdownExplicitCalls() {
		t.Run(tc.name, func(t *testing.T) {
			a, _ := newTestConn(t, StateASPDown, RoleASP)
			release, err := a.acquireExplicitASPProcedure(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer release()
			a.terminating.Store(true)
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			if err := tc.call(a, ctx); !errors.Is(err, ErrASPProcedureSuperseded) {
				t.Fatalf("admission of %s = %v", tc.name, err)
			}
		})
	}
}

func TestShutdownClaimOrdersWithRequestRegistration(t *testing.T) {
	a, writes := shutdownObservedAssociation(t, "ASP")
	release := holdASPTestLock(t, &a.tack.mu)
	done := make(chan error, 1)
	call := startASPTestCall(func() error {
		err := a.ShutdownContext(aspTestContext(t))
		done <- err
		return err
	})
	// Observe either the blocked claim or an incorrectly admitted withdrawal.
	// This proves the goroutine ran before inspecting terminating.
	where := awaitASPTestAnyStack(t, call,
		[]string{"(*Association).claimShutdown(", "lockSlow("},
		[]string{"(*Association).ShutdownContext(", "(*Association).registerTAckLocked("},
		[]string{"(*Association).ShutdownContext(", "(*Association).startTAckRequest(", "lockSlow("},
		[]string{"(*Association).ShutdownContext(", "(*Association).startTAck(", "lockSlow("})
	if where != 0 || a.terminating.Load() {
		t.Fatal("shutdown claimed terminating before registration unlocked")
	}
	release()
	if message := receiveShutdownWithdrawal(t, writes, done); message.MessageTypeName() != "ASP Inactive" {
		t.Fatalf("first withdrawal = %T", message)
	}
	finishHeldWithdrawal(t, a, writes, done)
	if err := call.wait(t, "shutdown completion"); err != nil {
		t.Fatal(err)
	}
}

// Err is called after ShutdownContext claims terminating and releases tack.mu.
// Holding it isolates admission from the subsequent withdrawal cancel-all.
type heldShutdownClaimContext struct {
	context.Context
	claimed chan struct{}
	resume  chan struct{}
}

func (ctx *heldShutdownClaimContext) Err() error {
	select {
	case ctx.claimed <- struct{}{}:
	default:
	}
	<-ctx.resume
	return ctx.Context.Err()
}

func TestShutdownRefusesWholeMixedModeActivation(t *testing.T) {
	for _, waiting := range []bool{true, false} {
		name := "ASPActive"
		if !waiting {
			name = "ActivateRoutingContexts"
		}
		t.Run(name, func(t *testing.T) {
			a, writes := shutdownObservedAssociation(t, "ASP")
			a.trafficModes.freeze(newTrafficModePolicy(&AssociationConfig{ApplicationServers: []ASConfig{
				{ASKey: routingContextASKey(1), TrafficMode: params.TrafficModeLoadshare},
				{ASKey: routingContextASKey(2), TrafficMode: params.TrafficModeBroadcast},
			}}))
			entered, resume := make(chan struct{}), make(chan struct{})
			var preparations atomic.Int32
			a.beforeASPProcedureRegistration = func() {
				if preparations.Add(1) == 2 {
					close(entered)
					<-resume
				}
			}
			result := make(chan error, 1)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			go func() {
				if waiting {
					result <- a.ASPActive(ctx)
				} else {
					result <- a.ActivateRoutingContexts()
				}
			}()
			receiveASPTest(t, entered, "second Active group before registration")
			claim := &heldShutdownClaimContext{Context: ctx, claimed: make(chan struct{}, 1), resume: make(chan struct{})}
			releaseClaim := sync.OnceFunc(func() { close(claim.resume) })
			defer releaseClaim()
			done := make(chan error, 1)
			go func() { done <- a.ShutdownContext(claim) }()
			receiveASPTest(t, claim.claimed, "shutdown claim before cancel-all")
			close(resume)
			if err := receiveASPTest(t, result, "whole Active batch refusal"); !errors.Is(err, ErrASPProcedureSuperseded) {
				t.Errorf("mixed-mode activation = %v", err)
			}
			select {
			case message := <-writes:
				t.Errorf("refused activation wrote partial batch: %T", message)
			default:
			}
			if got := a.pendingTAck(); got != 0 {
				t.Errorf("refused activation left %d requests armed", got)
			}
			releaseClaim()
			if message := receiveShutdownWithdrawal(t, writes, done); message.MessageTypeName() != "ASP Inactive" {
				t.Fatalf("shutdown withdrawal = %T", message)
			}
			finishHeldWithdrawal(t, a, writes, done)
		})
	}
}

func TestShutdownRefusalPreservesExistingProcedureOwnership(t *testing.T) {
	t.Run("Up cannot retire activation", func(t *testing.T) {
		a, _ := newTestConn(t, StateASPDown, RoleASP)
		a.cfg.TAck = time.Hour
		old := a.startTAck(messages.NewAspActive(nil, nil, nil), requestAspActive)
		a.terminating.Store(true)
		request, cancelled := a.startASPSM()
		if err := receiveASPTest(t, request.result, "Up refusal"); !errors.Is(err, ErrASPProcedureSuperseded) {
			t.Errorf("Up refusal = %v", err)
		}
		select {
		case <-old.stop:
			t.Error("refused Up retired existing activation")
		default:
		}
		if len(cancelled) != 0 {
			t.Error("refused Up claimed activation ownership")
		}
	})
	t.Run("Active cannot confirm Up", func(t *testing.T) {
		a, _ := newTestConn(t, StateASPInactive, RoleASP)
		a.cfg.TAck = time.Hour
		old := a.startTAck(messages.NewAspUp(nil, nil), requestAspUp)
		a.terminating.Store(true)
		requests, err := a.startASPActive(nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, request := range requests {
			if err := receiveASPTest(t, request.result, "Active refusal"); !errors.Is(err, ErrASPProcedureSuperseded) {
				t.Errorf("Active refusal = %v", err)
			}
		}
		select {
		case <-old.stop:
			t.Error("refused Active completed existing Up")
		default:
		}
	})
}

func TestRoutingContextRefusalAfterPlainCloseRetainsStateError(t *testing.T) {
	for _, ending := range []string{"Close", "Abort", "ShutdownContext"} {
		t.Run(ending, func(t *testing.T) {
			a, _ := newTestConn(t, StateASPActive, RoleASP)
			switch ending {
			case "Abort":
				_ = a.Abort()
			case "ShutdownContext":
				a.setState(StateASPDown)
				_ = a.ShutdownContext(context.Background())
			default:
				_ = a.Close()
			}
			for _, call := range []func(...uint32) error{a.ActivateRoutingContexts, a.DeactivateRoutingContexts} {
				if err := call(); !errors.Is(err, ErrInvalidState) {
					t.Fatalf("routing-context call after close = %v, want ErrInvalidState", err)
				}
			}
		})
	}
}

func TestShutdownOwnedBoundaryRetiresRequestsBeforePeerRefusal(t *testing.T) {
	a, _ := newTestConn(t, StateASPActive, RoleIPSP)
	a.cfg.TAck = time.Hour
	old := a.startTAck(messages.NewAspDown(nil), requestAspDown)
	a.terminating.Store(true)
	a.tack.mu.Lock()
	a.tack.peerDownSuperseded = true
	a.tack.mu.Unlock()
	request := a.startTAckRequest(messages.NewAspInactive(nil, nil), requestAspInactive, true)
	if err := receiveASPTest(t, request.result, "peer withdrawal refusal"); !errors.Is(err, ErrASPProcedureSuperseded) {
		t.Fatalf("withdrawal error = %v", err)
	}
	select {
	case err := <-old.result:
		if !errors.Is(err, ErrASPProcedureSuperseded) || err.Error() != "ASP procedure superseded by orderly shutdown" {
			t.Errorf("old Down cancellation = %v", err)
		}
	default:
		t.Fatal("shutdown-owned boundary left old Down armed after peer refusal")
	}
	if a.pendingTAck() != 0 {
		t.Fatal("shutdown-owned boundary retained requests")
	}
}

// A peer state commit may hold muState while the public caller has already
// passed its initial validation. Keep shutdown after its claim but before
// cancel-all, so only the live admission error can account for the result.
func TestShutdownRefusalWinsStateCheckAfterValidation(t *testing.T) {
	calls := shutdownExplicitCalls()
	calls = append(calls, shutdownExplicitCall{name: "GuardedActivationRegistration", state: StateASPActive, call: func(a *Association, _ context.Context) error {
		_, err := a.startGuardedASPActive(nil)
		return err
	}})
	for _, tc := range calls {
		t.Run(tc.name, func(t *testing.T) {
			a, writes := shutdownObservedAssociation(t, "ASP")
			a.state = tc.state
			releaseState := holdASPTestLock(t, a.muState)
			call := startASPTestCall(func() error { return tc.call(a, context.Background()) })
			if !awaitASPTestStack(t, call, "sync.(*RWMutex).RLock(") {
				t.Fatal("caller did not reach state validation")
			}
			claim := &heldShutdownClaimContext{Context: context.Background(), claimed: make(chan struct{}, 1), resume: make(chan struct{})}
			releaseClaim := sync.OnceFunc(func() { close(claim.resume) })
			defer releaseClaim()
			done := make(chan error, 1)
			go func() { done <- a.ShutdownContext(claim) }()
			receiveASPTest(t, claim.claimed, "shutdown claim")
			a.state = StateASPDown
			if tc.name == "ASPUp" || tc.name == "ASPInactive" || tc.name == "DeactivateRoutingContexts" {
				a.state = StateASPInactive
			}
			releaseState()
			err := call.wait(t, "state validation after shutdown")
			// Finish this state-only fixture without sending a withdrawal request.
			a.muState.Lock()
			a.state = StateASPDown
			a.muState.Unlock()
			releaseClaim()
			if end := receiveASPTest(t, done, "shutdown completion"); end != nil {
				t.Fatal(end)
			}
			if !errors.Is(err, ErrASPProcedureSuperseded) {
				t.Errorf("validated caller after shutdown claim = %v, want ErrASPProcedureSuperseded", err)
			}
			select {
			case message := <-writes:
				t.Errorf("refused state validation wrote %T", message)
			default:
			}
		})
	}
}

func TestShutdownRefusesRoutingContextCallBeforeStateWait(t *testing.T) {
	for _, tc := range shutdownExplicitCalls() {
		if tc.reportsFailure {
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			a, _ := shutdownObservedAssociation(t, "ASP")
			a.terminating.Store(true)
			release := holdASPTestLock(t, a.muState)
			call := startASPTestCall(func() error { return tc.call(a, context.Background()) })
			waited := awaitASPTestStack(t, call, "(*Association)."+tc.name+"(", "sync.(*RWMutex).RLock(")
			release()
			if waited {
				t.Error("shutdown refusal waited behind a state commit")
			}
			if err := call.wait(t, "routing-context admission refusal"); !errors.Is(err, ErrASPProcedureSuperseded) {
				t.Fatalf("refusal = %v", err)
			}
		})
	}
}
