// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
)

// An owner of an Association had two ways to learn it was gone: poll
// State() until it read ASP-DOWN, or wait for a Read or Write to start failing.
// Neither says why, and ErrNotEstablished comes back identically from an Association
// that never came up and one that was torn down by an expired T(beat), a read
// error, T(ack) giving up, or a deliberate Close. Those want different
// responses from an application: one is a configuration problem, one is a dead
// peer, one is its own shutdown.
//
// Done() and Err() follow context.Context's shape, which is what callers
// already know: select on Done(), then ask Err() what happened.

// Done must be open while the association is, and closed once it is not.
func TestDoneClosesWhenTheAssociationDoes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peer := newRawPeer(t, 3150, handshakeOnly)
	conn := dialRawPeer(t, ctx, peer, 3150, &HeartbeatInfo{Enabled: false})

	select {
	case <-conn.Done():
		t.Fatal("Done() was already closed on a live association")
	default:
	}
	if err := conn.Err(); err != nil {
		t.Errorf("Err() = %v on a live association, want nil", err)
	}

	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case <-conn.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("Done() never closed after Close()")
	}
}

// A deliberate Close is reported as such, and is distinguishable from a
// failure.
func TestErrReportsADeliberateClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peer := newRawPeer(t, 3152, handshakeOnly)
	conn := dialRawPeer(t, ctx, peer, 3152, &HeartbeatInfo{Enabled: false})

	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := conn.Err(); !errors.Is(got, ErrAssociationClosed) {
		t.Errorf("Err() = %v after Close(), want ErrAssociationClosed", got)
	}
}

// A peer that stops answering BEATs is reported as an expired heartbeat, not as
// a generic failure — this is the whole point of recording the cause.
func TestErrReportsHeartbeatExpiry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peer := newRawPeer(t, 3154, handshakeOnly)
	conn := dialRawPeer(t, ctx, peer, 3154, &HeartbeatInfo{
		Enabled:  true,
		Interval: 100 * time.Millisecond,
		Timer:    200 * time.Millisecond,
	})

	select {
	case <-conn.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("Done() never closed against a peer that stopped answering")
	}
	if got := conn.Err(); !errors.Is(got, ErrHeartbeatExpired) {
		t.Errorf("Err() = %v, want ErrHeartbeatExpired", got)
	}
}

// Cancelling the context that owns the association reports the cancellation,
// so an application shutting down can tell its own action from a peer failure.
func TestErrReportsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	peer := newRawPeer(t, 3156, handshakeOnly)
	conn := dialRawPeer(t, ctx, peer, 3156, &HeartbeatInfo{Enabled: false})

	cancel()

	select {
	case <-conn.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("Done() never closed after the context was cancelled")
	}
	if got := conn.Err(); !errors.Is(got, context.Canceled) {
		t.Errorf("Err() = %v after cancellation, want context.Canceled", got)
	}
}

// The first cause wins: a Close that follows a failure must not overwrite the
// reason the association actually died.
func TestErrKeepsTheFirstCause(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peer := newRawPeer(t, 3158, handshakeOnly)
	conn := dialRawPeer(t, ctx, peer, 3158, &HeartbeatInfo{
		Enabled:  true,
		Interval: 100 * time.Millisecond,
		Timer:    200 * time.Millisecond,
	})

	<-conn.Done()
	first := conn.Err()

	if err := conn.Close(); err != nil && !errors.Is(err, ErrAssociationClosed) {
		t.Logf("second Close reported %v", err)
	}
	if got := conn.Err(); got != first {
		t.Errorf("Err() changed from %v to %v on a later Close", first, got)
	}
}

// Done() must be safe to select on from several goroutines, which is how it
// will be used.
func TestDoneIsSafeForConcurrentWaiters(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peer := newRawPeer(t, 3160, handshakeOnly)
	conn := dialRawPeer(t, ctx, peer, 3160, &HeartbeatInfo{Enabled: false})

	const waiters = 8
	woken := make(chan error, waiters)
	for i := 0; i < waiters; i++ {
		go func() {
			<-conn.Done()
			woken <- conn.Err()
		}()
	}

	time.Sleep(50 * time.Millisecond)
	_ = conn.Close()

	for i := 0; i < waiters; i++ {
		select {
		case err := <-woken:
			if !errors.Is(err, ErrAssociationClosed) {
				t.Errorf("waiter saw Err() = %v, want ErrAssociationClosed", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of %d waiters woke", i, waiters)
		}
	}
}

// blockingRelease holds the SCTP release until the test opens it, the way a
// SHUTDOWN waiting for its acknowledgement does, and reports when the teardown
// reached it.
type blockingRelease struct {
	reached     chan struct{}
	gate        chan struct{}
	reachedOnce sync.Once
	openOnce    sync.Once
}

// installBlockingRelease holds conn's release. A test that fails while the
// release is held still has it opened on cleanup, before newTestConn's own
// cleanup joins the teardown, so the failure is reported instead of hanging.
func installBlockingRelease(t *testing.T, conn *Association) *blockingRelease {
	t.Helper()
	release := &blockingRelease{reached: make(chan struct{}), gate: make(chan struct{})}
	hold := func() error {
		release.reachedOnce.Do(func() { close(release.reached) })
		<-release.gate
		return nil
	}
	conn.transportCloser, conn.transportAborter = hold, hold
	t.Cleanup(release.open)
	return release
}

func (release *blockingRelease) open() {
	release.openOnce.Do(func() { close(release.gate) })
}

// Done marks the start of the teardown, not its end. It closes as soon as the
// association has begun to end -- before the SCTP release, which for a
// SHUTDOWN waits on the peer -- so every waiter and every guard that checks it
// stops at once, and Err already says why. State reaches ASP-DOWN, and
// StateChanges and ManagementIndications report the end and close, only once
// the release has finished: a caller that needs the final state ranges over
// StateChanges until it closes.
func TestDoneMarksTheStartOfTheTeardown(t *testing.T) {
	for _, test := range []struct {
		name  string
		call  func(*Association) error
		cause error
	}{
		{"Close", (*Association).Close, ErrAssociationClosed},
		{"Abort", (*Association).Abort, ErrAssociationAborted},
	} {
		t.Run(test.name, func(t *testing.T) {
			conn, _ := newTestConn(t, StateASPActive, RoleASP)
			const pointCode = 0x123456
			seedDestinationAvailability(conn, pointCode, DestinationAvailable)
			release := installBlockingRelease(t, conn)

			released := make(chan error, 1)
			go func() { released <- test.call(conn) }()
			<-release.reached
			select {
			case <-conn.Done():
			case <-time.After(time.Second):
				t.Fatal("Done is still open while the SCTP release is under way; it must close when the teardown begins")
			}
			if conn.Err() != test.cause {
				t.Errorf("Err when Done closed = %v, want %v", conn.Err(), test.cause)
			}
			select {
			case st, ok := <-conn.StateChanges():
				t.Fatalf("StateChanges delivered %v (open %v) before the release finished; the end is reported after it", st, ok)
			default:
			}

			release.open()
			var last State
			for st := range conn.StateChanges() {
				last = st
			}
			if last != StateASPDown {
				t.Errorf("the last state StateChanges reported = %v, want %v", last, StateASPDown)
			}
			if got := conn.State(); got != StateASPDown {
				t.Errorf("State once StateChanges closed = %v, want %v", got, StateASPDown)
			}
			if got := retainedDestinationState(conn, pointCode).Availability; got != DestinationUnavailable {
				t.Errorf("destination availability once StateChanges closed = %v, want %v: the end is reported after the destinations are paused",
					got, DestinationUnavailable)
			}
			reportedRelease := false
			for indication := range conn.ManagementIndications() {
				if indication.Kind == ManagementSCTPRelease && indication.Cause == test.cause {
					reportedRelease = true
				}
			}
			if !reportedRelease {
				t.Errorf("ManagementIndications closed without reporting the release caused by %v", test.cause)
			}
			if err := <-released; err != nil {
				t.Errorf("%s: %v", test.name, err)
			}
		})
	}
}

// The end is reported only after the ASP's destinations are paused (RFC 4666
// Section 4.3.3's MTP-PAUSE), so a caller that waited for StateChanges to
// close reads them paused. Holding the destinations' lock stops the teardown
// at the pause; StateChanges must stay open until it is released.
func TestStateChangesClosesOnlyOnceTheDestinationsArePaused(t *testing.T) {
	conn, _ := newTestConn(t, StateASPActive, RoleASP)
	conn.transportCloser = func() error { return nil }
	conn.destinations.mu.Lock()
	locked := true
	defer func() {
		if locked {
			conn.destinations.mu.Unlock()
		}
	}()

	closed := make(chan error, 1)
	go func() { closed <- conn.Close() }()
	<-conn.Done()
	drained := make(chan struct{})
	go func() {
		for range conn.StateChanges() {
		}
		close(drained)
	}()
	select {
	case <-drained:
		t.Fatal("StateChanges closed while the destinations were still to be paused")
	case <-time.After(100 * time.Millisecond):
	}
	conn.destinations.mu.Unlock()
	locked = false
	select {
	case <-drained:
	case <-time.After(5 * time.Second):
		t.Fatal("StateChanges never closed once the destinations could be paused")
	}
	if err := <-closed; err != nil {
		t.Errorf("Close: %v", err)
	}
}

// Once the teardown has begun no handler moves the state: a message still
// being handled -- here an ASP Down an SGP reads while its own Close is
// releasing SCTP -- finds done closed and changes nothing. The teardown alone
// takes the association to ASP-DOWN, and so reports it on StateChanges. A
// handler that committed ASP-DOWN itself instead left the teardown nothing to
// report, and ASP-DOWN never reached StateChanges.
func TestNoHandlerMovesTheStateOnceTheTeardownHasBegun(t *testing.T) {
	conn, sent := newTestConn(t, StateASPActive, RoleSGP)
	release := installBlockingRelease(t, conn)

	closed := make(chan error, 1)
	go func() { closed <- conn.Close() }()
	<-release.reached
	conn.handleSignals(context.Background(), messages.NewAspDown(nil))
	release.open()
	if err := <-closed; err != nil {
		t.Fatalf("Close: %v", err)
	}

	var reported []State
	for st := range conn.StateChanges() {
		reported = append(reported, st)
	}
	if len(reported) != 1 || reported[0] != StateASPDown {
		t.Errorf("StateChanges reported %v, want exactly the teardown's %v", reported, StateASPDown)
	}
	if names := typeNames(*sent); len(names) != 0 {
		t.Errorf("the association answered %v after its teardown began", names)
	}
}
