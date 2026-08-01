// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"testing"
	"time"
)

// An owner of a Conn had two ways to learn the association was gone: poll
// State() until it read ASP-DOWN, or wait for a Read or Write to start failing.
// Neither says why, and ErrNotEstablished comes back identically from a Conn
// that never came up and one that was torn down by an expired T(beat), a read
// error, T(ack) giving up, or a deliberate Close. Those want different
// responses from an application: one is a configuration problem, one is a dead
// peer, one is its own shutdown.
//
// Done() and Err() follow context.Context's shape, which is what callers
// already know: select on Done(), then ask Err() what happened.

// Done must be open while the association is, and closed once it is not.
func TestDoneClosesWhenTheConnDoes(t *testing.T) {
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
	if got := conn.Err(); !errors.Is(got, ErrConnClosed) {
		t.Errorf("Err() = %v after Close(), want ErrConnClosed", got)
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

	if err := conn.Close(); err != nil && !errors.Is(err, ErrConnClosed) {
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
			if !errors.Is(err, ErrConnClosed) {
				t.Errorf("waiter saw Err() = %v, want ErrConnClosed", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of %d waiters woke", i, waiters)
		}
	}
}
