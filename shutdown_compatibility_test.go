package m3ua

import (
	"context"
	"errors"
	"fmt"
	"github.com/gomaja/go-sctp"
	"net"
	"sync"
	"syscall"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
)

var errCompatibilityWrite = fmt.Errorf("interrupted initial send: %w", net.ErrClosed)
var errCompatibilityFullBuffer = fmt.Errorf("independent send failure: %w", syscall.EAGAIN)

var errCompatibilityTerminal = errors.New("independent terminal failure")

// B is each row's explicit baseline match set among compatibilitySentinels.
// The result must match exactly B union L when shutdown caused this call, and
// exactly B otherwise; L = {ErrASPProcedureSuperseded, ErrOrderlyShutdown}.
// The baseline overlay runs this same table without the new-label assertions.
var compatibilitySentinels = []error{ErrASPProcedureSuperseded, ErrAssociationClosed, ErrAssociationAborted, ErrNotEstablished, ErrInvalidState, errCompatibilityWrite, net.ErrClosed, errCompatibilityTerminal, context.Canceled, ErrSCTPNotAlive, ErrUnsupportedRole, ErrContextlessApplicationServer, errCompatibilityFullBuffer, syscall.EAGAIN}

func checkShutdownCompatibility(t *testing.T, err error, baseline []error, orderly bool) {
	t.Helper()
	for _, target := range compatibilitySentinels {
		want := false
		for _, b := range baseline {
			if b == target {
				want = true
			}
		}
		// L adds both shutdown sentinels; no other baseline match may change.
		if orderly && target == ErrASPProcedureSuperseded {
			want = true
		}
		if errors.Is(err, target) != want {
			t.Errorf("error=%v; errors.Is(%v)=%v want %v; baseline=%v", err, target, errors.Is(err, target), want, baseline)
		}
	}
	if errors.Is(err, ErrOrderlyShutdown) != orderly {
		t.Errorf("error=%v; orderly=%v want %v", err, errors.Is(err, ErrOrderlyShutdown), orderly)
	}
}

type shutdownCompatibilityRow struct {
	path                   string
	operations             []string
	shutdown, close, abort []error
	priorCancellation      bool
	unrelated              bool
}

func TestShutdownErrorCompatibility(t *testing.T) {
	all := []string{"ASPUp", "ASPDown", "ASPActive", "ASPInactive", "ActivateRoutingContexts", "DeactivateRoutingContexts"}
	waiting := all[:4]
	rkm := []string{"REG", "DEREG"}
	rows := []shutdownCompatibilityRow{
		{path: "live-refusal", operations: append(append([]string{}, all...), "REG"), shutdown: []error{ErrASPProcedureSuperseded}},
		{path: "cancel-before-teardown", operations: []string{"ASPActive"}, shutdown: []error{ErrASPProcedureSuperseded}, priorCancellation: true},
		{path: "cancel-after-teardown", operations: []string{"ASPActive"}, shutdown: []error{ErrAssociationClosed}, close: []error{ErrAssociationClosed}, abort: []error{ErrAssociationAborted, ErrAssociationClosed}, priorCancellation: true},
		{path: "pending-ack", operations: waiting, shutdown: []error{ErrAssociationClosed}, close: []error{ErrAssociationClosed}, abort: []error{ErrAssociationAborted, ErrAssociationClosed}},
		{path: "pending-ack-ASP-DOWN", operations: []string{"ASPActive"}, shutdown: []error{ErrAssociationClosed}, close: []error{ErrAssociationClosed}, abort: []error{ErrAssociationAborted, ErrAssociationClosed}},
		{path: "queued-ASP", operations: waiting, shutdown: []error{ErrAssociationClosed}, close: []error{ErrAssociationClosed}, abort: []error{ErrAssociationAborted, ErrAssociationClosed}},
		{path: "late-ack-admission", operations: all, shutdown: []error{ErrAssociationClosed}, close: []error{ErrAssociationClosed}, abort: []error{ErrAssociationAborted, ErrAssociationClosed}},
		{path: "pending-RKM", operations: rkm, shutdown: []error{ErrAssociationClosed}, close: []error{ErrAssociationClosed}, abort: []error{ErrAssociationAborted, ErrAssociationClosed}},
		{path: "pending-RKM-ASP-DOWN", operations: rkm, shutdown: []error{ErrAssociationClosed}, close: []error{ErrAssociationClosed}, abort: []error{ErrAssociationAborted, ErrAssociationClosed}},
		{path: "queued-RKM", operations: rkm, shutdown: []error{ErrAssociationClosed}, close: []error{ErrAssociationClosed}, abort: []error{ErrAssociationAborted, ErrAssociationClosed}},
		{path: "post-gate-RKM", operations: rkm, shutdown: []error{ErrNotEstablished}, close: []error{ErrNotEstablished}, abort: []error{ErrNotEstablished}},
		{path: "entry-RKM", operations: []string{"REG"}, shutdown: []error{ErrASPProcedureSuperseded}, close: []error{ErrNotEstablished}, abort: []error{ErrNotEstablished}},
		{path: "entry-RKM", operations: []string{"DEREG"}, shutdown: []error{ErrNotEstablished}, close: []error{ErrNotEstablished}, abort: []error{ErrNotEstablished}},
		{path: "state-after-validation", operations: all, shutdown: []error{ErrInvalidState}, close: []error{ErrInvalidState}, abort: []error{ErrInvalidState}},
		{path: "routing-entry-after-release", operations: all[4:], shutdown: []error{ErrInvalidState}, close: []error{ErrInvalidState}, abort: []error{ErrInvalidState}},
		{path: "interrupted-write", operations: append(append([]string{}, all...), rkm...), shutdown: []error{errCompatibilityWrite, net.ErrClosed}, close: []error{errCompatibilityWrite, net.ErrClosed}, abort: []error{errCompatibilityWrite, net.ErrClosed}},
		{path: "late-REG-write-admission", operations: []string{"REG"}, shutdown: []error{ErrASPProcedureSuperseded}, close: []error{ErrAssociationClosed}, abort: []error{ErrAssociationAborted, ErrAssociationClosed}},
		{path: "retired-before-teardown", operations: []string{"Ack", "replacement", "uncommitted-Ack"}, shutdown: []error{ErrAssociationClosed}, close: []error{ErrAssociationClosed}, abort: []error{ErrAssociationAborted, ErrAssociationClosed}},
		{path: "independent-terminal", operations: []string{"Close", "Abort", "owner-context", "peer-failure", "transport-failure", "cancelled-shutdown"}},
		{path: "caller-context", operations: append(append([]string{}, waiting...), rkm...), shutdown: []error{context.Canceled}},
		{path: "late-shutdown-context", operations: append(append([]string{}, waiting...), rkm...), shutdown: []error{ErrAssociationClosed}},
		{path: "late-shutdown-context-Err", operations: []string{"Err"}, shutdown: []error{ErrAssociationClosed}, unrelated: true},
		{path: "role-before-admission", operations: append(append([]string{}, all...), rkm...), shutdown: []error{ErrUnsupportedRole}, close: []error{ErrUnsupportedRole}, abort: []error{ErrUnsupportedRole}, unrelated: true},
		{path: "argument-before-admission", operations: []string{"DEREG"}, shutdown: []error{ErrContextlessApplicationServer}, close: []error{ErrContextlessApplicationServer}, abort: []error{ErrContextlessApplicationServer}, unrelated: true},
		{path: "independent-write", operations: append(append([]string{}, all...), rkm...), shutdown: []error{errCompatibilityFullBuffer, syscall.EAGAIN}, close: []error{errCompatibilityFullBuffer, syscall.EAGAIN}, abort: []error{errCompatibilityFullBuffer, syscall.EAGAIN}, unrelated: true},

		{path: "association-Err", operations: []string{"Err"}, shutdown: []error{ErrAssociationClosed}, close: []error{ErrAssociationClosed}, abort: []error{ErrAssociationAborted, ErrAssociationClosed}},
	}
	for _, row := range rows {
		for _, op := range row.operations {
			for _, ending := range []string{"Shutdown", "Close", "Abort"} {
				baseline := row.shutdown
				if ending == "Close" {
					baseline = row.close
				}
				if ending == "Abort" {
					baseline = row.abort
				}
				if row.path == "independent-terminal" {
					if ending != "Close" {
						continue
					}
					switch op {
					case "Abort":
						baseline = []error{ErrAssociationAborted, ErrAssociationClosed}
					case "owner-context", "cancelled-shutdown":
						baseline = []error{context.Canceled}
					case "peer-failure":
						baseline = []error{ErrSCTPNotAlive}
					case "transport-failure":
						baseline = []error{errCompatibilityTerminal}
					default:
						baseline = []error{ErrAssociationClosed}
					}
				}
				if baseline == nil {
					continue
				}
				t.Run(row.path+"/"+op+"/"+ending, func(t *testing.T) {
					c, report, err := runShutdownCompatibilityPath(t, row.path, op, ending)
					orderly := (ending == "Shutdown" || row.priorCancellation) && row.path != "association-Err" && row.path != "caller-context" && !row.unrelated
					checkShutdownCompatibility(t, err, baseline, orderly)
					if report {
						checkShutdownCompatibility(t, compatibilityManagementCause(t, c), baseline, orderly)
					}
				})
			}
		}
	}
}

func compatibilityManagementCause(t *testing.T, c *Association) error {
	t.Helper()
	for {
		ind := receiveASPTest(t, c.ManagementIndications(), "procedure failure indication")
		if ind == nil {
			t.Fatal("management channel closed before failure")
		}
		if ind.Kind == ManagementError {
			return ind.Cause
		}
	}
}

func compatibilityCall(c *Association, ctx context.Context, op string) error {
	for _, call := range shutdownExplicitCalls() {
		if call.name == op {
			return call.call(c, ctx)
		}
	}
	return rkmReleaseCall(c, ctx, op == "REG")
}

func runShutdownCompatibilityPath(t *testing.T, path, op, ending string) (*Association, bool, error) {
	t.Helper()
	outcome := op
	c, w := shutdownObservedAssociation(t, "ASP")
	c.cfg.ASPProcedures = allExplicitASPPolicy()
	if op == "ASPUp" {
		c.setState(StateASPDown)
	}
	isRKM := op == "REG" || op == "DEREG"
	var rkmWriter *rkmShutdownTransport
	if isRKM {
		c, rkmWriter = newRKMShutdownAssociation(t)
		c.cfg.ASPProcedures = allExplicitASPPolicy()
	}
	c.ManagementIndications()
	report := op == "ASPUp" || op == "ASPDown" || op == "ASPActive" || op == "ASPInactive"
	ctx := context.Background()
	result := make(chan error, 1)
	start := func() { go func() { result <- compatibilityCall(c, ctx, op) }() }
	finish := func() { _ = endRequestTestAssociation(c, ending) }
	waitWrite := func() {
		if isRKM {
			receiveRKMShutdownFrame(t, rkmWriter)
		} else {
			receiveASPTest(t, w, "initial ASP write")
		}
	}
	switch path {
	case "late-shutdown-context", "late-shutdown-context-Err":
		if path == "late-shutdown-context" {
			start()
			waitWrite()
		}
		lateCancelCompatibilityShutdown(t, c)
		if path == "late-shutdown-context-Err" {
			return c, false, c.Err()
		}
	case "role-before-admission":
		c.role = RoleSGP
		finish()
		return c, false, compatibilityCall(c, ctx, op)
	case "argument-before-admission":
		finish()
		_, err := c.DeregisterApplicationServers(ctx, ASKey{})
		return c, false, err
	case "independent-write":
		entered, resume := make(chan struct{}), make(chan struct{})
		open := sync.OnceFunc(func() { close(resume) })
		defer open()
		cause := &compatibilityGatedWriteError{cause: errCompatibilityFullBuffer, entered: entered, resume: resume}
		ready := make(chan struct{})
		close(ready)
		c.signalWriter = nil
		c.sctpInfo = &sctp.SndInfo{PPID: M3UAPPID}
		c.controlTransport = &heldRKMReleaseSend{entered: make(chan struct{}), resume: ready, err: cause}
		start()
		receiveASPTest(t, entered, "independent send error selected before release")
		finish()
		open()
	case "live-refusal":
		if !c.claimShutdown() {
			t.Fatal("claim failed")
		}
		return c, report, compatibilityCall(c, ctx, op)
	case "cancel-before-teardown", "cancel-after-teardown":
		c.cfg.ASPProcedures = nil
		entered, resume := pauseActiveWait(t, c)
		start()
		waitWrite()
		receiveASPTest(t, entered, "paused cancellation waiter")
		shutdown := startHeldWithdrawal(t, c, w)
		if path == "cancel-before-teardown" {
			resume()
			err := receiveASPTest(t, result, "live cancellation")
			finishHeldWithdrawal(t, c, w, shutdown)
			return c, report, err
		}
		if ending == "Shutdown" {
			finishHeldWithdrawal(t, c, w, shutdown)
		} else {
			finish()
			if interrupted := receiveASPTest(t, shutdown, "shutdown interrupted"); !errors.Is(interrupted, c.Err()) {
				t.Fatalf("interrupted shutdown=%v want %v", interrupted, c.Err())
			}
		}
		receiveASPTest(t, c.releaseCompleted, "completed teardown")
		resume()
	case "pending-ack", "pending-ack-ASP-DOWN", "retired-before-teardown":
		entered, resume := make(chan struct{}), make(chan struct{})
		open := sync.OnceFunc(func() { close(resume) })
		defer open()
		var request *pendingRequest
		c.beforeTAckWait = func(p *pendingRequest) { request = p; close(entered); <-resume }
		if path == "retired-before-teardown" {
			op = "ASPActive"
			report = true
		}
		start()
		waitWrite()
		receiveASPTest(t, entered, "pending T(ack)")
		if path == "pending-ack-ASP-DOWN" {
			c.setState(StateASPDown)
		}
		if path == "retired-before-teardown" {
			switch outcome {
			case "replacement":
				request.finish(ErrASPProcedureSuperseded)
			case "uncommitted-Ack":
				request.stopRetries()
			default:
				request.finish(nil)
			}
		}
		finish()
		open()
	case "queued-ASP":
		release, err := c.acquireExplicitASPProcedure(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer release()
		observed := make(chan struct{}, 1)
		ctx = &observedDoneContext{Context: ctx, observed: observed}
		start()
		receiveASPTest(t, observed, "queued ASP")
		finish()
	case "late-ack-admission":
		entered, resume := make(chan struct{}), make(chan struct{})
		open := sync.OnceFunc(func() { close(resume) })
		defer open()
		c.beforeASPProcedureRegistration = func() { close(entered); <-resume }
		start()
		receiveASPTest(t, entered, "late admission")
		ended := make(chan error, 1)
		go func() { ended <- endRequestTestAssociation(c, ending) }()
		receiveASPTest(t, c.done, "release publication")
		open()
		if err := receiveASPTest(t, ended, "release completion"); err != nil {
			t.Fatal(err)
		}
	case "pending-RKM", "pending-RKM-ASP-DOWN":
		start()
		waitWrite()
		if path == "pending-RKM-ASP-DOWN" {
			c.setState(StateASPDown)
		}
		finish()
	case "queued-RKM":
		if err := c.acquireRKMRequest(ctx); err != nil {
			t.Fatal(err)
		}
		defer c.releaseRKMRequest()
		observed := make(chan struct{}, 1)
		ctx = &observedDoneContext{Context: ctx, observed: observed}
		start()
		receiveASPTest(t, observed, "queued RKM")
		finish()
	case "post-gate-RKM":
		if err := c.acquireRKMRequest(ctx); err != nil {
			t.Fatal(err)
		}
		held := &postGateRKMContext{Context: ctx, observed: make(chan struct{}), entered: make(chan struct{}), resume: make(chan struct{})}
		open := sync.OnceFunc(func() { close(held.resume) })
		defer open()
		ctx = held
		start()
		receiveASPTest(t, held.observed, "queued RKM")
		c.releaseRKMRequest()
		receiveASPTest(t, held.entered, "past RKM gate")
		finish()
		open()
	case "entry-RKM", "routing-entry-after-release":
		finish()
		return c, report, compatibilityCall(c, ctx, op)
	case "state-after-validation":
		release := holdASPTestLock(t, c.muState)
		call := startASPTestCall(func() error { return compatibilityCall(c, ctx, op) })
		if !awaitASPTestStack(t, call, "sync.(*RWMutex).RLock(") {
			t.Fatal("caller did not reach state check")
		}
		if !c.claimShutdown() {
			t.Fatal("claim failed")
		}
		ended := make(chan error, 1)
		go func() {
			if ending == "Shutdown" {
				ended <- c.releaseAfterWithdrawal()
			} else {
				ended <- endRequestTestAssociation(c, ending)
			}
		}()
		receiveASPTest(t, c.done, "release publication")
		c.state = StateASPDown
		if op == "ASPUp" || op == "ASPInactive" || op == "DeactivateRoutingContexts" {
			c.state = StateASPInactive
		}
		release()
		err := call.wait(t, "state refusal")
		if err := receiveASPTest(t, ended, "completed release"); err != nil {
			t.Fatal(err)
		}
		return c, report, err
	case "interrupted-write":
		entered, resume := make(chan struct{}), make(chan struct{})
		open := sync.OnceFunc(func() { close(resume) })
		defer open()
		if isRKM {
			c.controlTransport = &heldRKMReleaseSend{entered: entered, resume: resume, err: errCompatibilityWrite}
		} else {
			c.signalWriter = func(messages.M3UA) (int, error) { close(entered); <-resume; return 0, errCompatibilityWrite }
		}
		start()
		receiveASPTest(t, entered, "inside initial write")
		finish()
		open()
	case "late-REG-write-admission":
		release := holdASPTestLock(t, &c.sendMu)
		call := startASPTestCall(func() error { return compatibilityCall(c, ctx, op) })
		if !awaitASPTestStack(t, call, "(*Association).writeSignalFrameObserved(") {
			t.Fatal("REG did not reach write gate")
		}
		finish()
		release()
		return c, report, call.wait(t, "REG final admission")
	case "independent-terminal":
		report = true
		op = "ASPActive"
		selected := outcome
		entered, resume := pauseActiveWait(t, c)
		start()
		waitWrite()
		receiveASPTest(t, entered, "pending before independent end")
		if selected == "transport-failure" {
			_ = c.closeWith(errCompatibilityTerminal)
		} else {
			_ = endRequestTestAssociation(c, selected)
		}
		resume()
	case "caller-context":
		cancelled, cancel := context.WithCancel(ctx)
		defer cancel()
		if isRKM {
			held := &postGateRKMContext{Context: cancelled, observed: make(chan struct{}), entered: make(chan struct{}), resume: make(chan struct{})}
			ctx = held
			open := sync.OnceFunc(func() { close(held.resume) })
			defer open()
			start()
			receiveASPTest(t, held.entered, "caller before context cancellation")
			cancel()
			finish()
			open()
		} else {
			cancel()
			ctx = cancelled
			finish()
			return c, false, compatibilityCall(c, ctx, op)
		}
	case "association-Err":
		finish()
		return c, false, c.Err()
	default:
		t.Fatal(path)
	}
	return c, report, receiveASPTest(t, result, "public request result")
}

// Formatting happens after SendMsg returns and sendMu is released. Hold the
// already selected error there while the association's release wins.
type compatibilityGatedWriteError struct {
	cause   error
	entered chan struct{}
	resume  <-chan struct{}
	once    sync.Once
}

func (e *compatibilityGatedWriteError) Error() string {
	e.once.Do(func() { close(e.entered); <-e.resume })
	return "independent full-buffer failure"
}
func (e *compatibilityGatedWriteError) Unwrap() error { return e.cause }

type compatibilityShutdownCheckContext struct {
	context.Context
	checked chan struct{}
	once    sync.Once
}

func (ctx *compatibilityShutdownCheckContext) Err() error {
	err := ctx.Context.Err()
	ctx.once.Do(func() { close(ctx.checked) })
	return err
}
func lateCancelCompatibilityShutdown(t *testing.T, c *Association) {
	t.Helper()
	release := holdASPTestLock(t, c.muState)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	observed := &compatibilityShutdownCheckContext{Context: ctx, checked: make(chan struct{})}
	ended := make(chan error, 1)
	go func() { ended <- c.ShutdownContext(observed) }()
	receiveASPTest(t, observed.checked, "shutdown passed initial context check")
	cancel()
	release()
	if err := receiveASPTest(t, ended, "shutdown after late cancellation"); err != nil {
		t.Errorf("ShutdownContext=%v, baseline releases successfully", err)
	}
}
