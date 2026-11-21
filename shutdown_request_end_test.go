package m3ua

import (
	"context"
	"errors"
	"github.com/gomaja/go-m3ua/messages"
	"sync"
	"sync/atomic"
	"testing"
)

func requireShutdownClassification(t *testing.T, err error, orderly bool, _ ...bool) {
	t.Helper()
	if errors.Is(err, ErrOrderlyShutdown) != orderly {
		t.Errorf("error=%v orderly=%v want %v", err, errors.Is(err, ErrOrderlyShutdown), orderly)
	}
	if orderly && !errors.Is(err, ErrASPProcedureSuperseded) {
		t.Errorf("missing supersession: %v", err)
	}
}

func pauseActiveWait(t *testing.T, c *Association) (entered <-chan struct{}, resume func()) {
	t.Helper()
	reached, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	c.beforeTAckWait = func(request *pendingRequest) {
		if request.kind == requestAspActive {
			once.Do(func() { close(reached); <-release })
		}
	}
	resume = sync.OnceFunc(func() { close(release) })
	t.Cleanup(resume)
	return reached, resume
}

func allExplicitASPPolicy() *ASPProcedurePolicy {
	return &ASPProcedurePolicy{ASPUp: ASPProcedureExplicit, ASPActive: ASPProcedureExplicit, ASPInactive: ASPProcedureExplicit, ASPDown: ASPProcedureExplicit}
}

func endRequestTestAssociation(c *Association, ending string) error {
	switch ending {
	case "Shutdown":
		return c.ShutdownContext(context.Background())
	case "Close":
		return c.Close()
	case "Abort":
		return c.Abort()
	case "owner-context":
		return c.closeWith(context.Canceled)
	case "peer-failure":
		return c.closeWith(ErrSCTPNotAlive)
	case "transport-failure":
		return c.closeWith(errors.New("synthetic transport failure"))
	case "cancelled-shutdown":
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		return c.ShutdownContext(ctx)
	}
	panic(ending)
}

func TestOrderlyReleaseFirstWinnerClassifiesRequests(t *testing.T) {
	for _, winner := range []string{"Shutdown", "Close", "Abort"} {
		for _, loser := range []string{"Shutdown", "Close", "Abort"} {
			if loser == winner {
				continue
			}
			t.Run(winner+"-before-"+loser, func(t *testing.T) {
				c, w := newRKMShutdownAssociation(t)
				c.cfg.ASPProcedures = allExplicitASPPolicy()
				result := make(chan error, 1)
				go func() {
					_, err := c.RegisterRoutingKeys(context.Background(), RoutingKeyRegistration{RoutingKey: testRoutingKey(10, 100, 3)})
					result <- err
				}()
				receiveRKMShutdownFrame(t, w)
				entered, release := make(chan struct{}), make(chan struct{})
				resume := sync.OnceFunc(func() { close(release) })
				defer resume()
				block := func() error { close(entered); <-release; return nil }
				c.transportCloser, c.transportAborter = block, block
				first, second := make(chan error, 1), make(chan error, 1)
				go func() { first <- endRequestTestAssociation(c, winner) }()
				receiveASPTest(t, entered, "winning release")
				// The winner owns closeOnce and has published Done before the loser starts.
				go func() { second <- endRequestTestAssociation(c, loser) }()
				requireShutdownClassification(t, receiveASPTest(t, result, "request observing winner"), winner == "Shutdown", true)
				resume()
				if err := receiveASPTest(t, first, "first release completion"); err != nil {
					t.Fatal(err)
				}
				secondErr := receiveASPTest(t, second, "second release completion")
				if loser == "Shutdown" {
					if !errors.Is(secondErr, c.Err()) {
						t.Fatalf("losing shutdown = %v, want %v", secondErr, c.Err())
					}
				} else if secondErr != nil {
					t.Fatal(secondErr)
				}
				if errors.Is(c.Err(), ErrOrderlyShutdown) {
					t.Errorf("association Err was relabelled: %v", c.Err())
				}
			})
		}
	}
}

type cancelAtReleaseContext struct {
	context.Context
	cancel context.CancelFunc
	calls  atomic.Int32
}

func (ctx *cancelAtReleaseContext) Err() error {
	if ctx.calls.Add(1) == 2 {
		ctx.cancel()
	}
	return ctx.Context.Err()
}

func TestShutdownContextDoesNotRecheckContextAtRelease(t *testing.T) {
	c, _ := newTestConn(t, StateASPDown, RoleASP)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	atRelease := &cancelAtReleaseContext{Context: ctx, cancel: cancel}
	if err := c.ShutdownContext(atRelease); err != nil {
		t.Errorf("ShutdownContext=%v, baseline releases successfully", err)
	}
	if atRelease.calls.Load() != 1 {
		t.Errorf("context checked %d times, want baseline initial check only", atRelease.calls.Load())
	}
	if !errors.Is(c.Err(), ErrAssociationClosed) || errors.Is(c.Err(), context.Canceled) {
		t.Errorf("terminal error=%v, want original closure", c.Err())
	}
}

func TestUnsuccessfulAckRetainsTerminalPrecedence(t *testing.T) {
	c, _ := newTestConn(t, StateASPActive, RoleASP)
	for _, failure := range []error{nil, ErrASPProcedureSuperseded} {
		request := &pendingRequest{stop: make(chan struct{}), result: make(chan error, 1)}
		request.finish(failure)
		_ = c.Close()
		for range 128 {
			if err := c.waitTAck(context.Background(), request); !errors.Is(err, ErrAssociationClosed) || errors.Is(err, ErrOrderlyShutdown) {
				t.Fatalf("unsuccessful Ack commit = %v", err)
			}
		}
	}
}

func TestFailedShutdownDoesNotRelabelRegistration(t *testing.T) {
	for _, failure := range []string{"write", "T(ack)", "context"} {
		t.Run(failure, func(t *testing.T) {
			c, w := newRKMShutdownAssociation(t)
			c.cfg.ASPProcedures = &ASPProcedurePolicy{ASPInactive: ASPProcedureAutomatic, ASPDown: ASPProcedureAutomatic}
			result := make(chan error, 1)
			go func() {
				_, err := c.RegisterRoutingKeys(context.Background(), RoutingKeyRegistration{RoutingKey: testRoutingKey(10, 100, 3)})
				result <- err
			}()
			receiveRKMShutdownFrame(t, w)
			cause := errors.New("synthetic withdrawal failure")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if failure == "write" {
				c.signalWriter = func(messages.M3UA) (int, error) { return 0, cause }
			} else {
				c.beforeTAckWait = func(request *pendingRequest) {
					if request.kind == requestAspInactive {
						if failure == "context" {
							cancel()
						} else {
							request.finish(cause)
						}
					}
				}
			}
			err := c.ShutdownContext(ctx)
			if failure == "context" {
				cause = context.Canceled
			}
			if !errors.Is(err, cause) {
				t.Errorf("ShutdownContext = %v, want %v", err, cause)
			}
			requestErr := receiveASPTest(t, result, "registration after failed withdrawal")
			requireShutdownClassification(t, requestErr, false)
			if !errors.Is(requestErr, cause) {
				t.Errorf("registration = %v, want failure %v", requestErr, cause)
			}
		})
	}
}
