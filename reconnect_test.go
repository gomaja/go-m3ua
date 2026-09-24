// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

// An ASP that reboots is the most ordinary event on a live SGP link, and the
// recovery an application has to implement is "notice the association is gone,
// then Dial again". Nothing exercised that: the suite covers failures *during*
// the handshake and never re-establishes afterwards.
//
// An Association cannot be revived — Close closes done and trips a sync.Once, and every
// goroutine it owns exits on done — so reconnecting means a new Association from a new
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

// A peer that aborted must take the Association out of ASP-ACTIVE while the
// application keeps offering traffic, so an owner polling State() learns its
// association is gone and can redial.
//
// The detection does not depend on the traffic. The peer's Abort puts an ABORT
// on the wire at once, and the reader learns of it from the SCTP_COMM_LOST
// notification setUpSocket subscribes to. The writes are what make this hard:
// any of them can take the socket's pending ECONNRESET before the reader asks
// for it, and until COMM_LOST ended the read, one that did left the reader
// parked and the Association ASP-ACTIVE indefinitely.
// TestPeerAbortEndsTheReadAfterAWriteTookTheSocketError forces that
// interleaving; this test races it the way an application does.
func TestPeerAbortIsDetectedOnceTrafficResumes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peer := newRawPeer(t, 3050, handshakeOnly)
	conn := dialRawPeer(t, ctx, peer, 3050, &HeartbeatInfo{Enabled: false})

	if got := conn.State(); got != StateASPActive {
		t.Fatalf("state before abort = %v, want %v", got, StateASPActive)
	}

	peer.abort(t)

	// Keep offering traffic, as a live SGP would. The first write may still be
	// accepted by the local stack; what must not happen is writes succeeding
	// indefinitely onto an association that no longer exists.
	if !waitFor(func() bool {
		_, _ = writePayload(conn, 1, []byte("after-abort"))
		return conn.State() != StateASPActive
	}, 15*time.Second) {
		t.Fatalf("state is still %v fifteen seconds after the peer aborted, with traffic flowing", conn.State())
	}

	if _, err := writePayload(conn, 1, []byte("after-abort")); err == nil {
		t.Error("WriteData still succeeds after the association was torn down")
	}
}

// With BEAT enabled, an aborted peer must be detected without any traffic at
// all. SCTP_COMM_LOST normally reports the ABORT before T(beat) can expire:
// RFC 4666 Section 4.3.4.6 offers Heartbeat for "transport layers that do not
// have their own heartbeat mechanism for detecting loss of the transport
// association (i.e., other than SCTP)". T(beat) is what covers a peer whose
// association stays up while its M3UA goes silent;
// TestHeartbeatExpiryIsDetectedAgainstSilentPeer pins that.
func TestPeerAbortWithHeartbeatIsDetectedWhileIdle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peer := newRawPeer(t, 3057, handshakeOnly)
	conn := dialRawPeer(t, ctx, peer, 3057, &HeartbeatInfo{
		Enabled:  true,
		Interval: 200 * time.Millisecond,
		Timer:    500 * time.Millisecond,
	})

	if got := conn.State(); got != StateASPActive {
		t.Fatalf("state before abort = %v, want %v", got, StateASPActive)
	}

	peer.abort(t)

	// No writes here: the association must find it on its own.
	if !waitFor(func() bool { return conn.State() != StateASPActive }, 15*time.Second) {
		t.Fatalf("state is still %v fifteen seconds after the peer aborted, with no traffic", conn.State())
	}
}

// A peer's ABORT reaches a one-to-one SCTP socket twice: as the socket's
// pending error, ECONNRESET, and as an SCTP_COMM_LOST notification queued for
// the reader. The pending error is handed to whichever call asks first and then
// forgotten, and a write asks too: Linux turns a send's EPIPE into the pending
// error when there is one (sctp_error in net/sctp/socket.c). Once a write has
// taken it, the reader's next recvmsg finds nothing -- an aborted association
// leaves no RCV_SHUTDOWN behind, so a non-blocking read answers EAGAIN -- and
// the reader parks for good. The Association then stayed ASP-ACTIVE with every
// write failing, which is how TestPeerAbortIsDetectedOnceTrafficResumes and
// TestRedialAfterPeerAbortEstablishes timed out under load.
//
// The notification is the one report a writer cannot take, so it has to end
// the read. This forces the losing interleaving, starting the reader only after
// a write has taken the error, with the library's own socket set-up,
// notification handler and reader.
func TestPeerAbortEndsTheReadAfterAWriteTookTheSocketError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peer := newRawPeer(t, 3056, handshakeOnly)
	laddr, err := sctp.ResolveSCTPAddr("sctp", "127.0.0.1:3056")
	if err != nil {
		t.Fatal(err)
	}

	association := newAssociation(RoleASP, newASPAssociationConfigForTest(
		&HeartbeatInfo{Enabled: false}, 1, params.TrafficModeLoadshare, 0, []uint32{1, 2}))
	restarts := &restartWatcher{}
	restarts.setRoute(func(sctp.SCTPAssocID) *Association { return association })
	conn, err := dialAssociation(ctx, "sctp", laddr, peer.addr, 5*time.Second, restarts)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	association.sctpConn = conn
	if err := association.setUpSocket(); err != nil {
		t.Fatalf("setting up the socket: %v", err)
	}
	t.Cleanup(func() { _ = association.Close() })

	peer.abort(t)

	// Nothing reads yet, so the first write to fail is the one the socket
	// hands the abort to. An earlier write can still succeed if it beats the
	// ABORT in; the peer then answers it with one of its own.
	frame := association.encodeDataFrame(&DataRequest{
		AS:           associationScope(association, 1),
		ProtocolData: testProtocolData([]byte("after-abort")),
	})
	info := *association.sctpInfo
	info.Stream = 1
	var writeErr error
	if !waitFor(func() bool {
		_, writeErr = association.writeSCTPData(frame, &info)
		return writeErr != nil
	}, 5*time.Second) {
		t.Fatal("writes still succeed five seconds after the peer aborted")
	}
	if !errors.Is(writeErr, syscall.ECONNRESET) {
		t.Fatalf("the first failed write returned %v, want the abort's ECONNRESET; "+
			"without it this test is not exercising a write that took the socket error", writeErr)
	}

	readErr := make(chan error, 1)
	go association.readLoop(make(chan inbound), readErr)

	// Generous on purpose: the read ends at once or never.
	select {
	case err := <-readErr:
		if !errors.Is(err, ErrSCTPNotAlive) {
			t.Errorf("the read ended with %v, want the lost association reported as %v", err, ErrSCTPNotAlive)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the reader is still parked ten seconds after the peer aborted; " +
			"SCTP_COMM_LOST did not end the read, so nothing will report the loss")
	}
}

// The reconnect contract: after the peer goes away, a fresh Dial re-establishes
// and the new Association is fully usable. The old Association stays dead — it is not revived
// by the new association.
func TestRedialAfterPeerAbortEstablishes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peer := newRawPeer(t, 3052, handshakeOnly)

	first := dialRawPeer(t, ctx, peer, 3052, &HeartbeatInfo{Enabled: false})
	peer.abort(t)
	// Traffic keeps flowing while the abort is detected, as above.
	if !waitFor(func() bool {
		_, _ = writePayload(first, 1, []byte("after-abort"))
		return first.State() != StateASPActive
	}, 15*time.Second) {
		t.Fatalf("first Association is still %v after the abort", first.State())
	}

	// A different local port, because the previous association may still be
	// lingering on the old one; a reconnecting application dials afresh anyway.
	second := dialRawPeer(t, ctx, peer, 3053, &HeartbeatInfo{Enabled: false})

	if got := second.State(); got != StateASPActive {
		t.Errorf("redialled Association state = %v, want %v", got, StateASPActive)
	}
	if _, err := writePayload(second, 1, []byte("after-redial")); err != nil {
		t.Errorf("WriteData on the redialled Association: %v", err)
	}
	if got := first.State(); got == StateASPActive {
		t.Error("the aborted Association reports ASP-ACTIVE again; a dead Association must stay dead")
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
	heartbeat := &HeartbeatInfo{
		Enabled:  true,
		Interval: 100 * time.Millisecond,
		Timer:    300 * time.Millisecond,
	}

	cycle := func(port int) {
		conn := dialRawPeer(t, ctx, peer, port, heartbeat)
		if got := conn.State(); got != StateASPActive {
			t.Fatalf("port %d: state = %v, want %v", port, got, StateASPActive)
		}
		peer.abort(t)
		if !waitFor(func() bool { return conn.State() != StateASPActive }, 15*time.Second) {
			t.Fatalf("port %d: Association still %v after abort", port, conn.State())
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
