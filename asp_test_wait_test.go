package m3ua

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// aspTestWait bounds fixture progress, not protocol timing. Procedure contexts
// are cancelled explicitly so scheduling cannot expire them before submission.
const aspTestWait = 15 * time.Second

func aspTestContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return ctx
}

type aspTestCall struct {
	done chan struct{}
	err  error // Read only after done closes.
}

func startASPTestCall(fn func() error) *aspTestCall {
	call := &aspTestCall{done: make(chan struct{})}
	go func() {
		defer close(call.done)
		call.err = fn()
	}()
	return call
}

func receiveASPTest[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(aspTestWait):
		t.Fatalf("fixture did not finish %s within %v", what, aspTestWait)
		var zero T
		return zero
	}
}

func (call *aspTestCall) wait(t *testing.T, what string) error {
	t.Helper()
	receiveASPTest(t, call.done, what)
	return call.err
}

// A procedure can fail before writing. Observe that outcome as well as the
// write. A nonwaiting API may finish after enqueueing the observed write.
func awaitASPTestSend[T any](t *testing.T, sent <-chan T, call *aspTestCall, what string) T {
	t.Helper()
	select {
	case value := <-sent:
		return value
	case <-call.done:
		select {
		case value := <-sent:
			return value
		default:
			t.Fatalf("%s returned before submission: %v", what, call.err)
		}
	case <-time.After(aspTestWait):
		t.Fatalf("fixture did not submit %s within %v", what, aspTestWait)
	}
	var zero T
	return zero
}

// Return false if the call finishes instead of reaching the held-lock barrier.
// All frames must belong to one goroutine; unrelated calls cannot satisfy it.
func awaitASPTestStack(t *testing.T, call *aspTestCall, frames ...string) bool {
	t.Helper()
	timer := time.NewTimer(aspTestWait)
	defer timer.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		stack := make([]byte, 64<<10)
		for {
			n := runtime.Stack(stack, true)
			if n < len(stack) {
				stack = stack[:n]
				break
			}
			stack = make([]byte, 2*len(stack))
		}
		for _, goroutine := range strings.Split(string(stack), "\n\n") {
			matches := true
			for _, frame := range frames {
				matches = matches && strings.Contains(goroutine, frame)
			}
			if matches {
				return true
			}
		}
		select {
		case <-call.done:
			return false
		case <-tick.C:
		case <-timer.C:
			t.Fatalf("fixture did not reach held-lock barrier %v within %v", frames, aspTestWait)
		}
	}
}

func holdASPTestLock(t *testing.T, lock sync.Locker) func() {
	t.Helper()
	lock.Lock()
	var once sync.Once
	unlock := func() { once.Do(lock.Unlock) }
	t.Cleanup(unlock)
	return unlock
}

func aspTestGate(t *testing.T) (<-chan struct{}, func()) {
	t.Helper()
	release := make(chan struct{})
	var once sync.Once
	open := func() { once.Do(func() { close(release) }) }
	t.Cleanup(open)
	return release, open
}

func awaitASPTestRelease(c *Association, release <-chan struct{}) error {
	select {
	case <-release:
		return nil
	case <-c.done:
		return ErrAssociationClosed
	case <-time.After(aspTestWait):
		return fmt.Errorf("fixture write was not released within %v", aspTestWait)
	}
}

func waitASPTestAck(t *testing.T, c *Association, request *pendingRequest) error {
	t.Helper()
	ctx := aspTestContext(t)
	return startASPTestCall(func() error { return c.waitTAck(ctx, request) }).wait(t, "procedure confirmation")
}

// With the transport held by the fixture, an Ack parked on its write fence is
// a dependency violation. Observe the blocked frame or the handler's return;
// neither outcome depends on a short scheduler window.
func finishASPTestAck(t *testing.T, call *aspTestCall, blocked string) error {
	t.Helper()
	if awaitASPTestStack(t, call, "(*Association).claimASPUpAcknowledgement(", "lockSlow(") {
		t.Fatal(blocked)
	}
	return call.wait(t, "Ack processing")
}
