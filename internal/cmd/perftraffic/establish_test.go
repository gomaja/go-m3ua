package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua"
	"github.com/gomaja/go-sctp"
)

// fakeAcceptor records Accept calls and Close calls without SCTP sockets.
// Returning nil associations is sufficient: the acquisition seams under test
// only carry them through. It captures every Accept context so tests can
// assert the caller never cancels it, and its Close unblocks a blocked Accept
// exactly like the real listener.
type fakeAcceptor struct {
	acceptErr   error
	failAtCall  int
	block       bool
	accepts     int
	closes      int
	unblock     chan struct{}
	unblockOnce sync.Once
	mutex       sync.Mutex
	acceptCtxs  []context.Context
}

func newFakeAcceptor() *fakeAcceptor {
	return &fakeAcceptor{unblock: make(chan struct{})}
}

func (fake *fakeAcceptor) Accept(ctx context.Context) (*m3ua.Association, error) {
	fake.mutex.Lock()
	fake.accepts++
	fake.acceptCtxs = append(fake.acceptCtxs, ctx)
	accepts := fake.accepts
	fake.mutex.Unlock()
	if fake.block {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-fake.unblock:
			return nil, errors.New("listener closed")
		}
	}
	if fake.failAtCall > 0 && accepts >= fake.failAtCall {
		return nil, fake.acceptErr
	}
	return nil, nil
}

func (fake *fakeAcceptor) Close() error {
	fake.closes++
	fake.unblockOnce.Do(func() { close(fake.unblock) })
	return nil
}

func (fake *fakeAcceptor) capturedContexts() []context.Context {
	fake.mutex.Lock()
	defer fake.mutex.Unlock()
	return append([]context.Context(nil), fake.acceptCtxs...)
}

type fakeConnector struct {
	listener *fakeAcceptor
	dials    int
	dialErr  error
}

func (fake *fakeConnector) Listen(string, *sctp.SCTPAddr, *m3ua.ListenerConfig) (associationAcceptorCloser, error) {
	return fake.listener, nil
}

func (fake *fakeConnector) Dial(context.Context, string, *sctp.SCTPAddr, *sctp.SCTPAddr, *m3ua.AssociationConfig) (*m3ua.Association, error) {
	fake.dials++
	return nil, fake.dialErr
}

func listenConfig() commandConfig {
	return commandConfig{Role: "asp", Transport: "listen", SCTPAddress: "127.0.0.1:2905", Associations: 3}
}

// The ASP-listen acquisition must not close the listener when acquisition
// succeeds: m3ua.Listener.Close closes every accepted association, which is
// exactly the teardown that killed the SGP-dial smoke run. The caller owns
// the close through the returned release function, at the end of the run.
func TestEstablishSenderAssociationsKeepsListenerOpenUntilRelease(testContext *testing.T) {
	listener := newFakeAcceptor()
	connector := &fakeConnector{listener: listener}
	associations, release, err := establishSenderAssociations(context.Background(), listenConfig(), connector)
	if err != nil {
		testContext.Fatalf("establishSenderAssociations: %v", err)
	}
	if len(associations) != 3 || listener.accepts != 3 {
		testContext.Fatalf("accepted %d associations in %d calls, want 3/3", len(associations), listener.accepts)
	}
	if listener.closes != 0 {
		testContext.Fatalf("listener closed %d times during acquisition; closing it tears down every accepted association", listener.closes)
	}
	release()
	if listener.closes != 1 {
		testContext.Fatalf("release closed the listener %d times, want exactly 1", listener.closes)
	}
}

// The library runs every accepted association's monitor on the Accept
// context for the association's whole lifetime (listener.go starts
// monitor(ctx), and monitor closes the association when ctx ends). Cancelling
// the acquisition context — including a timeout context whose timer simply
// fires later — therefore tears down every accepted association. This is the
// regression that killed the SGP-dial re-smoke after the listener-lifetime
// fix: acquisition must pass its own context through untouched.
func TestAcceptAssociationsNeverCancelsTheAcceptContext(testContext *testing.T) {
	listener := newFakeAcceptor()
	associations, err := acceptAssociations(context.Background(), listener, 4, time.Second)
	if err != nil {
		testContext.Fatalf("acceptAssociations: %v", err)
	}
	if len(associations) != 4 {
		testContext.Fatalf("accepted %d associations, want 4", len(associations))
	}
	captured := listener.capturedContexts()
	if len(captured) != 4 {
		testContext.Fatalf("captured %d Accept contexts, want 4", len(captured))
	}
	for index, ctx := range captured {
		if ctx.Err() != nil {
			testContext.Fatalf("Accept context %d was cancelled after acquisition (%v); that cancels the association monitor", index, ctx.Err())
		}
	}
}

func TestAcceptAssociationsTimesOutWithANamedErrorAndUnblocksAccept(testContext *testing.T) {
	listener := newFakeAcceptor()
	listener.block = true
	started := time.Now()
	_, err := acceptAssociations(context.Background(), listener, 2, 30*time.Millisecond)
	if !errors.Is(err, errAcceptTimeout) {
		testContext.Fatalf("acceptAssociations error = %v, want errAcceptTimeout", err)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		testContext.Fatalf("accept wait was not bounded: %s", elapsed)
	}
	if listener.closes != 1 {
		testContext.Fatalf("listener closed %d times, want exactly 1 to unblock the parked Accept", listener.closes)
	}
}

func TestAcceptAssociationsCancellationClosesTheListener(testContext *testing.T) {
	listener := newFakeAcceptor()
	listener.block = true
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	_, err := acceptAssociations(ctx, listener, 2, time.Hour)
	if !errors.Is(err, context.Canceled) {
		testContext.Fatalf("acceptAssociations error = %v, want context.Canceled", err)
	}
	if listener.closes != 1 {
		testContext.Fatalf("listener closed %d times, want exactly 1", listener.closes)
	}
}

func TestEstablishSenderAssociationsAcceptFailureClosesListenerImmediately(testContext *testing.T) {
	listener := newFakeAcceptor()
	listener.acceptErr = errors.New("peer vanished")
	listener.failAtCall = 2
	connector := &fakeConnector{listener: listener}
	if _, _, err := establishSenderAssociations(context.Background(), listenConfig(), connector); err == nil {
		testContext.Fatal("establishSenderAssociations unexpectedly succeeded after an accept failure")
	}
	if listener.closes != 1 {
		testContext.Fatalf("listener closed %d times after a failed acquisition, want exactly 1", listener.closes)
	}
}

func TestEstablishSenderAssociationsDialPathNeverListens(testContext *testing.T) {
	connector := &fakeConnector{listener: newFakeAcceptor()}
	config := listenConfig()
	config.Transport = "dial"
	associations, release, err := establishSenderAssociations(context.Background(), config, connector)
	if err != nil {
		testContext.Fatalf("establishSenderAssociations: %v", err)
	}
	if len(associations) != 3 || connector.dials != 3 || connector.listener.accepts != 0 {
		testContext.Fatalf("dial path = %d associations, %d dials, %d accepts; want 3/3/0",
			len(associations), connector.dials, connector.listener.accepts)
	}
	release()
	if connector.listener.closes != 0 {
		testContext.Fatal("dial path release touched a listener")
	}
}

func TestRequireStateActiveAcceptsOnlyASPActive(testContext *testing.T) {
	states := []m3ua.State{m3ua.StateASPDown, m3ua.StateASPInactive, m3ua.StateASPActive, m3ua.StateSCTPCDI, m3ua.StateSCTPRI, m3ua.State(99)}
	for _, state := range states {
		err := requireStateActive(state)
		if state == m3ua.StateASPActive {
			if err != nil {
				testContext.Fatalf("requireStateActive(%s) = %v, want nil", state, err)
			}
			continue
		}
		if !errors.Is(err, errAssociationNotActive) {
			testContext.Fatalf("requireStateActive(%s) = %v, want errAssociationNotActive", state, err)
		}
	}
}

func TestDialWithRetryReturnsImmediateSuccess(testContext *testing.T) {
	dials := 0
	association, err := dialWithRetry(context.Background(), 50*time.Millisecond, time.Millisecond, func() (*m3ua.Association, error) {
		dials++
		return nil, nil
	})
	if err != nil || association != nil {
		testContext.Fatalf("dialWithRetry = %v, %v", association, err)
	}
	if dials != 1 {
		testContext.Fatalf("dials = %d, want exactly 1 for an immediate success", dials)
	}
}

func TestDialWithRetryDoesNotRetryPermanentFailures(testContext *testing.T) {
	permanent := errors.New("invalid association config")
	dials := 0
	_, err := dialWithRetry(context.Background(), 50*time.Millisecond, time.Millisecond, func() (*m3ua.Association, error) {
		dials++
		return nil, permanent
	})
	if !errors.Is(err, permanent) || dials != 1 {
		testContext.Fatalf("dialWithRetry = %v after %d dials, want the permanent error after exactly 1", err, dials)
	}
}

func TestDialWithRetryRetriesRefusalUntilSuccess(testContext *testing.T) {
	dials := 0
	_, err := dialWithRetry(context.Background(), time.Second, time.Millisecond, func() (*m3ua.Association, error) {
		dials++
		if dials < 4 {
			return nil, fmt.Errorf("dial: %w", syscall.ECONNREFUSED)
		}
		return nil, nil
	})
	if err != nil || dials != 4 {
		testContext.Fatalf("dialWithRetry = %v after %d dials, want success on the fourth attempt", err, dials)
	}
}

func TestDialWithRetryNamesPeerThatNeverAccepts(testContext *testing.T) {
	dials := 0
	started := time.Now()
	_, err := dialWithRetry(context.Background(), 30*time.Millisecond, time.Millisecond, func() (*m3ua.Association, error) {
		dials++
		return nil, fmt.Errorf("dial: %w", syscall.ECONNREFUSED)
	})
	if !errors.Is(err, errPeerNotAccepting) {
		testContext.Fatalf("dialWithRetry = %v, want errPeerNotAccepting", err)
	}
	if dials < 2 {
		testContext.Fatalf("dials = %d, want bounded retry, not a single attempt", dials)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		testContext.Fatalf("retry window was not bounded: %s", elapsed)
	}
}

func TestDialWithRetryStopsOnContextCancellation(testContext *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	dials := 0
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	_, err := dialWithRetry(ctx, time.Hour, time.Millisecond, func() (*m3ua.Association, error) {
		dials++
		return nil, fmt.Errorf("dial: %w", syscall.ECONNREFUSED)
	})
	if !errors.Is(err, context.Canceled) {
		testContext.Fatalf("dialWithRetry = %v, want context.Canceled", err)
	}
}

func TestIsTransientDialErrorRecognizesOnlyRefusal(testContext *testing.T) {
	if !isTransientDialError(fmt.Errorf("wrap: %w", syscall.ECONNREFUSED)) {
		testContext.Fatal("ECONNREFUSED was not recognized as transient")
	}
	for _, err := range []error{errors.New("boom"), context.Canceled, syscall.ECONNRESET} {
		if isTransientDialError(err) {
			testContext.Fatalf("%v misclassified as transient", err)
		}
	}
}
