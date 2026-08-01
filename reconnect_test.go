// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"
)

// An ASP that reboots is the most ordinary event on a live SGP link, and the
// recovery an application has to implement is "notice the association is gone,
// then Dial again". Nothing exercised that: the suite covers failures *during*
// the handshake and never re-establishes afterwards.
//
// A *Conn cannot be revived — Close closes done and trips a sync.Once, and every
// goroutine it owns exits on done — so reconnecting means a new Conn from a new
// Dial. These tests pin that contract, and pin that repeating it does not
// accumulate goroutines, which is what turns a reconnect loop into a leak.

// abort tears the peer's association down without a graceful shutdown, the way
// a peer that crashes or is killed does.
func (p *rawPeer) abort(t *testing.T) {
	t.Helper()

	if !waitFor(func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.conn != nil
	}, 5*time.Second) {
		t.Fatal("peer never accepted an association to abort")
	}

	p.mu.Lock()
	conn := p.conn
	p.conn = nil
	p.mu.Unlock()

	if err := conn.Abort(); err != nil {
		t.Fatalf("aborting the peer association: %v", err)
	}
}

// Once traffic resumes, a peer that aborted must take the Conn out of
// ASP-ACTIVE, so an owner polling State() learns its association is gone and
// can redial.
//
// The detection is traffic-driven, and deliberately asserted that way. go-m3ua
// never subscribes to SCTP events (there is no SubscribeEvents/SubscribeEvent
// call anywhere in the package), so an ABORT arriving at an idle association
// raises no notification: the reader stays parked in its read, and nothing
// reports the loss until either a write draws an out-of-the-blue ABORT back —
// what this test does — or M3UA's own T(beat) expires, which is the case
// TestPeerAbortWithHeartbeatIsDetectedWhileIdle covers. An association that is
// both idle and has BEAT disabled falls back on the kernel's SCTP path
// heartbeats, which on Linux defaults take minutes.
func TestPeerAbortIsDetectedOnceTrafficResumes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peer := newRawPeer(t, 3050, handshakeOnly)
	conn := dialRawPeer(t, ctx, peer, 3050, &HeartbeatInfo{Enabled: false})

	if got := conn.State(); got != StateAspActive {
		t.Fatalf("state before abort = %v, want %v", got, StateAspActive)
	}

	peer.abort(t)

	// Keep offering traffic, as a live SGP would. The first write may still be
	// accepted by the local stack; what must not happen is writes succeeding
	// indefinitely onto an association that no longer exists.
	if !waitFor(func() bool {
		_, _ = conn.Write([]byte("after-abort"))
		return conn.State() != StateAspActive
	}, 15*time.Second) {
		t.Fatalf("state is still %v fifteen seconds after the peer aborted, with traffic flowing", conn.State())
	}

	if _, err := conn.Write([]byte("after-abort")); err == nil {
		t.Error("Write still succeeds after the association was torn down")
	}
}

// With BEAT enabled, an aborted peer must be detected without any traffic at
// all: T(beat) is M3UA's liveness mechanism (RFC 4666 Section 4.3.4.6) and the
// only thing covering an idle association.
func TestPeerAbortWithHeartbeatIsDetectedWhileIdle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peer := newRawPeer(t, 3057, handshakeOnly)
	conn := dialRawPeer(t, ctx, peer, 3057, &HeartbeatInfo{
		Enabled:  true,
		Interval: 200 * time.Millisecond,
		Timer:    500 * time.Millisecond,
	})

	if got := conn.State(); got != StateAspActive {
		t.Fatalf("state before abort = %v, want %v", got, StateAspActive)
	}

	peer.abort(t)

	// No writes here: the heartbeat must find it on its own.
	if !waitFor(func() bool { return conn.State() != StateAspActive }, 15*time.Second) {
		t.Fatalf("state is still %v fifteen seconds after the peer aborted; T(beat) did not detect it", conn.State())
	}
}

// The reconnect contract: after the peer goes away, a fresh Dial re-establishes
// and the new Conn is fully usable. The old Conn stays dead — it is not revived
// by the new association.
func TestRedialAfterPeerAbortEstablishes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peer := newRawPeer(t, 3052, handshakeOnly)

	first := dialRawPeer(t, ctx, peer, 3052, &HeartbeatInfo{Enabled: false})
	peer.abort(t)
	// Traffic-driven detection, as above.
	if !waitFor(func() bool {
		_, _ = first.Write([]byte("after-abort"))
		return first.State() != StateAspActive
	}, 15*time.Second) {
		t.Fatalf("first Conn is still %v after the abort", first.State())
	}

	// A different local port, because the previous association may still be
	// lingering on the old one; a reconnecting application dials afresh anyway.
	second := dialRawPeer(t, ctx, peer, 3053, &HeartbeatInfo{Enabled: false})

	if got := second.State(); got != StateAspActive {
		t.Errorf("redialled Conn state = %v, want %v", got, StateAspActive)
	}
	if _, err := second.Write([]byte("after-redial")); err != nil {
		t.Errorf("Write on the redialled Conn: %v", err)
	}
	if got := first.State(); got == StateAspActive {
		t.Error("the aborted Conn reports ASP-ACTIVE again; a dead Conn must stay dead")
	}
}

// A reconnect loop runs for the life of the process, so a cycle that leaks even
// one goroutine is a slow death. Each cycle here is a full establish, abort and
// close.
func TestRepeatedReconnectCyclesDoNotLeakGoroutines(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping: runs several full establish/abort cycles")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peer := newRawPeer(t, 3055, handshakeOnly)

	cycle := func(port int) {
		conn := dialRawPeer(t, ctx, peer, port, &HeartbeatInfo{Enabled: false})
		if got := conn.State(); got != StateAspActive {
			t.Fatalf("port %d: state = %v, want %v", port, got, StateAspActive)
		}
		peer.abort(t)
		if !waitFor(func() bool {
			_, _ = conn.Write([]byte("after-abort"))
			return conn.State() != StateAspActive
		}, 15*time.Second) {
			t.Fatalf("port %d: Conn still %v after abort", port, conn.State())
		}
		_ = conn.Close()
	}

	// One warm-up cycle first: the package's lazily started machinery would
	// otherwise be counted as a leak.
	cycle(3060)
	settle()
	baseline := runtime.NumGoroutine()

	const cycles = 4
	for i := 0; i < cycles; i++ {
		cycle(3061 + i)
	}
	settle()

	// Allow a small margin for runtime-owned goroutines, but not for anything
	// that scales with the number of cycles.
	if got := runtime.NumGoroutine(); got > baseline+4 {
		t.Errorf("goroutines = %d after %d reconnect cycles, baseline %d: each cycle is leaking (%s)",
			got, cycles, baseline, goroutineSummary(goroutinesBlockedIn("go-m3ua")))
	}
}

// settle gives goroutines torn down by a Close a moment to actually exit before
// they are counted.
func settle() {
	for i := 0; i < 10; i++ {
		runtime.Gosched()
		time.Sleep(100 * time.Millisecond)
	}
}

// goroutineSummary renders a stack list compactly for a failure message.
func goroutineSummary(stacks []string) string {
	return fmt.Sprintf("%d go-m3ua goroutines still running", len(stacks))
}
