// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"sync"
	"testing"
	"time"
)

// SignallingStatus hands the caller a channel and its documentation invites an
// MTP3-User to read it, so the idiomatic use is:
//
//	go func() {
//		for st := range conn.SignallingStatus() { ... }
//	}()
//
// That loop never terminated. The channel was created per Conn and never
// closed, so every association left one goroutine parked on a channel with no
// remaining sender — for the life of the process. On an SGP whose ASPs come and
// go, that is an unbounded leak of goroutines and of the Conns they keep
// reachable.
func TestSignallingStatusChannelClosesWithTheConn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peer := newRawPeer(t, 3100, handshakeOnly)
	conn := dialRawPeer(t, ctx, peer, 3100, &HeartbeatInfo{Enabled: false})

	ranged := make(chan struct{})
	go func() {
		//nolint:revive // draining is the point
		for range conn.SignallingStatus() {
		}
		close(ranged)
	}()

	// Let the reader park on the channel before the Conn goes away.
	time.Sleep(100 * time.Millisecond)
	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case <-ranged:
	case <-time.After(5 * time.Second):
		t.Fatal("a range over SignallingStatus() never terminated after Close: one goroutine leaks per association")
	}
}

// Closing the channel introduces a race the previous code did not have: a send
// on a closed channel panics, and SSNM arriving as the association is torn down
// is exactly when that happens. Run under -race.
func TestNotifyStatusRacingCloseDoesNotPanic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peer := newRawPeer(t, 3102, handshakeOnly)
	conn := dialRawPeer(t, ctx, peer, 3102, &HeartbeatInfo{Enabled: false})

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				conn.notifyStatus(&DestinationStatus{PointCode: uint32(j)})
			}
		}()
	}

	time.Sleep(20 * time.Millisecond)
	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	wg.Wait()

	// And a second Close must still be safe.
	if err := conn.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

// A caller that never reads the channel must be unaffected: the send stays
// non-blocking and lossy, and Close must not hang waiting for a reader.
func TestUnreadSignallingStatusDoesNotBlockClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peer := newRawPeer(t, 3104, handshakeOnly)
	conn := dialRawPeer(t, ctx, peer, 3104, &HeartbeatInfo{Enabled: false})

	// Overfill the buffer with nobody reading.
	for i := 0; i < 1000; i++ {
		conn.notifyStatus(&DestinationStatus{PointCode: uint32(i)})
	}

	done := make(chan error, 1)
	go func() { done <- conn.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Close blocked on an unread status channel")
	}
}

// Statuses already queued must still be readable after Close — closing a
// channel does not discard what is buffered — so a caller draining after
// teardown sees what the peer reported before it went away.
func TestQueuedStatusesSurviveClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peer := newRawPeer(t, 3106, handshakeOnly)
	conn := dialRawPeer(t, ctx, peer, 3106, &HeartbeatInfo{Enabled: false})

	const queued = 5
	for i := 0; i < queued; i++ {
		conn.notifyStatus(&DestinationStatus{PointCode: uint32(i)})
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	counted := make(chan int, 1)
	go func() {
		n := 0
		for range conn.SignallingStatus() {
			n++
		}
		counted <- n
	}()

	select {
	case got := <-counted:
		if got != queued {
			t.Errorf("drained %d queued statuses after Close, want %d", got, queued)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("draining SignallingStatus() after Close never terminated")
	}
}

// RFC 4666 Section 4.3.3, on the association going away underneath an ASP:
//
//	"If the M3UA layer subsequently receives an SCTP-COMMUNICATION_DOWN or
//	SCTP-RESTART indication primitive from the underlying SCTP layer [...] The
//	state of the ASP will be moved to ASP-DOWN. At an ASP, the MTP3-User will
//	be informed of the unavailability of any affected SS7 destinations through
//	the use of MTP-PAUSE indication primitives."
//
// Nothing was reported: the status channel was closed in silence and
// DestinationState went on answering with whatever the peer had last said,
// so an MTP3-User that had been told a destination was available kept being
// told so long after the only route to it had gone.
func TestClosingAConnReportsItsDestinationsUnavailable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peer := newRawPeer(t, 3140, handshakeOnly)
	conn := dialRawPeer(t, ctx, peer, 3140, &HeartbeatInfo{Enabled: false})

	// The peer has told us about three destinations.
	conn.SetDestinationState(0x111111, DestinationAvailable)
	conn.SetDestinationState(0x222222, DestinationRestricted)
	conn.SetDestinationState(0x333333, DestinationUnavailable)

	paused := make(chan map[uint32]DestinationState, 1)
	go func() {
		got := map[uint32]DestinationState{}
		for st := range conn.SignallingStatus() {
			got[st.PointCode] = st.State
		}
		paused <- got
	}()

	time.Sleep(100 * time.Millisecond)
	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case got := <-paused:
		for _, pc := range []uint32{0x111111, 0x222222} {
			if got[pc] != DestinationUnavailable {
				t.Errorf("point code %#x reported as %v on teardown, want %v",
					pc, got[pc], DestinationUnavailable)
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the status channel never closed")
	}

	// And the authoritative view must agree: a destination reachable only over
	// this association is not reachable once it is gone.
	for _, pc := range []uint32{0x111111, 0x222222, 0x333333} {
		if got := conn.DestinationState(pc); got != DestinationUnavailable {
			t.Errorf("DestinationState(%#x) = %v after Close, want %v", pc, got, DestinationUnavailable)
		}
	}
}

// A destination the peer never mentioned is not invented on teardown.
func TestClosingAConnDoesNotInventDestinations(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peer := newRawPeer(t, 3142, handshakeOnly)
	conn := dialRawPeer(t, ctx, peer, 3142, &HeartbeatInfo{Enabled: false})

	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	count := 0
	for range conn.SignallingStatus() {
		count++
	}
	if count != 0 {
		t.Errorf("reported %d destinations on teardown having heard about none", count)
	}
}
