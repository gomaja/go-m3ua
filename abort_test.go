// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-sctp"
)

// Close and Shutdown both end in the SCTP SHUTDOWN procedure, so an
// application had no way to drop an association at once: a misbehaving peer,
// or one that never completes the shutdown, held it for the graceful
// exchange. Abort is the SCTP ABORT primitive (RFC 9260 Section 11.1.4)
// followed by the local teardown Close performs. These tests pin both halves:
// what the peer is sent, and that nothing local is released differently.

// releaseSeams counts how the association's SCTP association was released.
type releaseSeams struct {
	closes atomic.Int32
	aborts atomic.Int32
}

// installReleaseSeams routes the two transport releases to counters, so a test
// without a socket can tell which one teardown used and how often.
func installReleaseSeams(conn *Association, abortErr error) *releaseSeams {
	seams := &releaseSeams{}
	conn.transportCloser = func() error {
		seams.closes.Add(1)
		return nil
	}
	conn.transportAborter = func() error {
		seams.aborts.Add(1)
		return abortErr
	}
	return seams
}

// requireAbortedCause requires err to be Abort's recorded cause, which still
// matches ErrAssociationClosed because the owner closed the association.
func requireAbortedCause(t *testing.T, err error, what string) {
	t.Helper()
	if err != ErrAssociationAborted || !errors.Is(err, ErrAssociationClosed) {
		t.Errorf("%s = %v, want %v, which matches %v", what, err, ErrAssociationAborted, ErrAssociationClosed)
	}
}

// releaseCause is the cause a release named in a test records: Abort's for any
// name mentioning an abort, Close's otherwise.
func releaseCause(name string) error {
	if strings.Contains(strings.ToLower(name), "abort") {
		return ErrAssociationAborted
	}
	return ErrAssociationClosed
}

// Abort's cause is a Close's cause too, so a caller that asks only whether the
// owner closed the association keeps working; the converse must not hold, or a
// graceful Close would read as an abort.
func TestErrAssociationAbortedIsAnOwnerClose(t *testing.T) {
	if !errors.Is(ErrAssociationAborted, ErrAssociationClosed) {
		t.Errorf("errors.Is(%v, %v) = false, want true", ErrAssociationAborted, ErrAssociationClosed)
	}
	if errors.Is(ErrAssociationClosed, ErrAssociationAborted) {
		t.Errorf("errors.Is(%v, %v) = true, want false", ErrAssociationClosed, ErrAssociationAborted)
	}
}

// requireDone waits for the association to end.
func requireDone(t *testing.T, conn *Association, what string) {
	t.Helper()
	select {
	case <-conn.Done():
	case <-time.After(5 * time.Second):
		t.Fatalf("%s: Done is still open five seconds later", what)
	}
}

// Abort releases SCTP through the ABORT primitive, and only through it, and
// then performs exactly Close's local teardown: ASP-DOWN, the transition and
// the M-SCTP_RELEASE confirm reported to the application before both channels
// close, Done closed and Err reporting the application's own release.
func TestAbortReleasesThroughTheAbortPrimitiveThenTearsDownLikeClose(t *testing.T) {
	conn, _ := newTestConn(t, StateASPActive, RoleASP)
	seams := installReleaseSeams(conn, nil)

	if err := conn.Abort(); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	if got := seams.aborts.Load(); got != 1 {
		t.Fatalf("Abort released SCTP through the ABORT primitive %d times, want once", got)
	}
	if got := seams.closes.Load(); got != 0 {
		t.Fatalf("Abort also released SCTP through the graceful close %d times; "+
			"the peer would be sent SHUTDOWN rather than ABORT", got)
	}

	requireDone(t, conn, "Abort")
	requireAbortedCause(t, conn.Err(), "Err after Abort")
	if got := conn.State(); got != StateASPDown {
		t.Errorf("State after Abort = %v, want %v", got, StateASPDown)
	}
	if st, ok := <-conn.StateChanges(); !ok || st != StateASPDown {
		t.Errorf("first state change after Abort = %v (open %v), want %v", st, ok, StateASPDown)
	}
	if st, ok := <-conn.StateChanges(); ok {
		t.Errorf("StateChanges delivered %v after the release instead of closing", st)
	}
	indication, ok := <-conn.ManagementIndications()
	if !ok || indication.Kind != ManagementSCTPRelease {
		t.Errorf("management indication after Abort = %+v (open %v), want %v", indication, ok, ManagementSCTPRelease)
	} else {
		requireAbortedCause(t, indication.Cause, "the release indication's Cause")
		if !strings.Contains(indication.Description, "SCTP ABORT") {
			t.Errorf("the release indication's Description %q does not say the release was an SCTP ABORT", indication.Description)
		}
	}
	if extra, ok := <-conn.ManagementIndications(); ok {
		t.Errorf("ManagementIndications delivered %+v after the release instead of closing", extra)
	}
	if _, err := writePayload(conn, 1, []byte("after-abort")); !errors.Is(err, ErrNotEstablished) {
		t.Errorf("WriteData after Abort = %v, want %v", err, ErrNotEstablished)
	}
	if _, err := conn.ReadData(context.Background()); !errors.Is(err, ErrNotEstablished) {
		t.Errorf("ReadData after Abort = %v, want %v", err, ErrNotEstablished)
	}
}

// Abort is idempotent the way Close is: the first call performs the teardown
// and reports its error, and every later Abort or Close returns nil without
// touching the transport again.
func TestAbortIsIdempotentAndReportsOnlyTheFirstError(t *testing.T) {
	conn, _ := newTestConn(t, StateASPActive, RoleASP)
	abortErr := errors.New("abort failed")
	seams := installReleaseSeams(conn, abortErr)

	if err := conn.Abort(); !errors.Is(err, abortErr) {
		t.Fatalf("first Abort = %v, want the transport's %v", err, abortErr)
	}
	if err := conn.Abort(); err != nil {
		t.Errorf("second Abort = %v, want nil, as Close returns for a released association", err)
	}
	if err := conn.Close(); err != nil {
		t.Errorf("Close after Abort = %v, want nil", err)
	}
	if aborts, closes := seams.aborts.Load(), seams.closes.Load(); aborts != 1 || closes != 0 {
		t.Errorf("transport released %d times by ABORT and %d by close, want exactly one ABORT", aborts, closes)
	}
	requireAbortedCause(t, conn.Err(), "Err: the cause is the release, not its transport error; Err")
}

// An association that has already ended keeps the reason it ended for, and a
// late Abort does not reach the transport a second time.
func TestAbortAfterTheAssociationEndedIsANoOp(t *testing.T) {
	for _, first := range []struct {
		name  string
		end   func(*Association) error
		cause error
	}{
		{"closed", (*Association).Close, ErrAssociationClosed},
		{"failed", func(c *Association) error { return c.closeWith(ErrHeartbeatExpired) }, ErrHeartbeatExpired},
	} {
		t.Run(first.name, func(t *testing.T) {
			conn, _ := newTestConn(t, StateASPActive, RoleASP)
			seams := installReleaseSeams(conn, nil)

			if err := first.end(conn); err != nil {
				t.Fatalf("ending the association: %v", err)
			}
			if err := conn.Abort(); err != nil {
				t.Errorf("Abort after the association ended = %v, want nil", err)
			}
			if aborts, closes := seams.aborts.Load(), seams.closes.Load(); aborts != 0 || closes != 1 {
				t.Errorf("transport released %d times by ABORT and %d by close, want the one earlier close", aborts, closes)
			}
			if conn.Err() != first.cause {
				t.Errorf("Err = %v, want the original cause %v", conn.Err(), first.cause)
			}
		})
	}
}

// Abort, Close and ShutdownContext share one teardown. However they
// interleave, the transport is released exactly once and every caller
// returns.
func TestAbortCloseAndShutdownRaceToOneTeardown(t *testing.T) {
	for round := 0; round < 50; round++ {
		conn, _ := newTestConn(t, StateASPActive, RoleASP)
		conn.cfg.TAck = time.Hour
		seams := installReleaseSeams(conn, nil)
		var writes sync.Mutex
		conn.signalWriter = func(message messages.M3UA) (int, error) {
			writes.Lock()
			defer writes.Unlock()
			return message.MarshalLen(), nil
		}

		start := make(chan struct{})
		var wg sync.WaitGroup
		results := make(chan error, 9)
		for i := 0; i < 4; i++ {
			wg.Add(2)
			go func() { defer wg.Done(); <-start; results <- conn.Abort() }()
			go func() { defer wg.Done(); <-start; results <- conn.Close() }()
		}
		shutdown := make(chan error, 1)
		go func() { <-start; shutdown <- conn.ShutdownContext(context.Background()) }()
		close(start)
		wg.Wait()
		close(results)

		for err := range results {
			if err != nil {
				t.Fatalf("round %d: Abort or Close returned %v, want nil", round, err)
			}
		}
		select {
		case err := <-shutdown:
			if err != nil && !errors.Is(err, ErrAssociationClosed) {
				t.Fatalf("round %d: ShutdownContext = %v, want nil or %v", round, err, ErrAssociationClosed)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("round %d: ShutdownContext did not return after the association was released", round)
		}
		if released := seams.aborts.Load() + seams.closes.Load(); released != 1 {
			t.Fatalf("round %d: transport released %d times (%d ABORT, %d close), want once",
				round, released, seams.aborts.Load(), seams.closes.Load())
		}
	}
}

// ShutdownContext waits out each T(ack) exchange, and a peer that never
// answers holds it for the whole retry budget. Abort is how an application
// stops waiting: the exchange ends, ShutdownContext returns, and the release
// is the ABORT rather than the SHUTDOWN the orderly path would have ended in.
func TestAbortStopsAShutdownWaitingForItsAck(t *testing.T) {
	conn, _ := newTestConn(t, StateASPActive, RoleASP)
	conn.cfg.TAck = time.Hour
	seams := installReleaseSeams(conn, nil)
	writes := make(chan messages.M3UA, 4)
	conn.signalWriter = func(message messages.M3UA) (int, error) {
		writes <- message
		return message.MarshalLen(), nil
	}

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- conn.ShutdownContext(context.Background()) }()
	if _, ok := receiveSignal(t, writes).(*messages.AspInactive); !ok {
		t.Fatal("shutdown did not begin with ASP Inactive")
	}

	if err := conn.Abort(); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	select {
	case err := <-shutdownDone:
		if err != ErrAssociationAborted {
			t.Fatalf("ShutdownContext = %v, want %v once Abort released the association", err, ErrAssociationAborted)
		}
	case <-time.After(time.Second):
		t.Fatal("ShutdownContext is still waiting for its ASP Inactive Ack after Abort")
	}
	if aborts, closes := seams.aborts.Load(), seams.closes.Load(); aborts != 1 || closes != 0 {
		t.Errorf("transport released %d times by ABORT and %d by close, want exactly one ABORT", aborts, closes)
	}
	if conn.pendingTAck() != 0 {
		t.Error("Abort left the shutdown's T(ack) armed")
	}
	assertNoSignal(t, writes, 25*time.Millisecond, "ASP Down after Abort")
}

// newAssociationEventPeer is newRawPeer with SCTP_ASSOC_CHANGE subscribed on
// the listening socket before listen, so every association it accepts reports
// how it ended (RFC 6458 Section 6.1.1). What the peer's own SCTP layer says it
// received is the wire evidence: SCTP_COMM_LOST carrying the ABORT chunk's
// error cause for an abort, SCTP_SHUTDOWN_COMP for a completed SHUTDOWN.
func newAssociationEventPeer(t *testing.T, port int, reply func(messages.M3UA) messages.M3UA) (*rawPeer, <-chan *sctp.AssocChange) {
	t.Helper()

	addr, err := sctp.ResolveSCTPAddr("sctp", fmt.Sprintf("127.0.0.2:%d", port))
	if err != nil {
		t.Fatal(err)
	}
	events := make(chan *sctp.AssocChange, 16)
	handler := func(b []byte) error {
		notification, err := sctp.ParseNotification(b)
		if err != nil {
			return nil
		}
		if change, ok := notification.(*sctp.AssocChange); ok {
			select {
			case events <- change:
			default:
			}
		}
		return nil
	}
	ln, err := (&sctp.SocketConfig{NotificationHandler: handler}).
		WithPreAssociation(sctp.PreAssociationConfig{Notifications: associationEvents()}).
		Listen("sctp", addr)
	if err != nil {
		if isSCTPUnsupported(err) {
			t.Skipf("skipping socket-backed test: %v", err)
		}
		if errors.Is(err, syscall.ENOPROTOOPT) {
			t.Skipf("skipping: this kernel has no SCTP_EVENT, so the peer cannot report how the association ended")
		}
		t.Fatal(err)
	}

	p := &rawPeer{t: t, ln: ln, addr: addr, reply: reply}
	t.Cleanup(func() { _ = ln.Close() })
	go p.serve()

	return p, events
}

// associationEnd waits for the event that ended the peer's association: the
// first SCTP_ASSOC_CHANGE other than SCTP_COMM_UP.
func associationEnd(t *testing.T, events <-chan *sctp.AssocChange) *sctp.AssocChange {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		select {
		case change := <-events:
			if change.State != sctp.SCTP_COMM_UP {
				return change
			}
		case <-deadline:
			t.Fatal("the peer's SCTP layer reported no end of the association within ten seconds")
			return nil
		}
	}
}

// The wire difference, observed by the peer's own SCTP layer. After Abort it
// reports SCTP_COMM_LOST with the User-Initiated Abort cause RFC 9260 Section
// 9.1 says an ABORT requested by the upper layer SHOULD carry; after Close it
// reports SCTP_SHUTDOWN_COMP, the graceful end of Section 9.2. Each is also
// required not to be the other, so an Abort that fell back to Close fails here.
func TestAbortSendsABORTWhereCloseCompletesASHUTDOWN(t *testing.T) {
	for _, test := range []struct {
		name    string
		port    int
		release func(*Association) error
		state   sctp.SCTPState
	}{
		{"Close", 3931, (*Association).Close, sctp.SCTP_SHUTDOWN_COMP},
		{"Abort", 3932, (*Association).Abort, sctp.SCTP_COMM_LOST},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			peer, events := newAssociationEventPeer(t, test.port, handshakeOnly)
			conn := dialRawPeer(t, ctx, peer, test.port, &HeartbeatInfo{Enabled: false})
			requireSubscribedAssociationEvents(t, conn.sctpConn, "dialled association")
			if !waitFor(func() bool { return conn.State() == StateASPActive }, 5*time.Second) {
				t.Fatalf("never reached ASP-ACTIVE; state = %v", conn.State())
			}
			monitors := len(goroutinesBlockedIn("go-m3ua.(*Association).monitor"))

			if err := test.release(conn); err != nil {
				t.Fatalf("%s: %v", test.name, err)
			}

			end := associationEnd(t, events)
			if end.State != test.state {
				t.Fatalf("after %s the peer's SCTP layer reported %v (cause %s), want %v",
					test.name, end.State, sctp.ErrorCauseString(uint32(end.Error)), test.state)
			}
			if end.State == sctp.SCTP_COMM_LOST && end.Error != sctp.SCTP_ERROR_USER_ABORT {
				t.Errorf("the ABORT carried cause %s, want %s (RFC 9260 Section 9.1)",
					sctp.ErrorCauseString(uint32(end.Error)), sctp.ErrorCauseString(uint32(sctp.SCTP_ERROR_USER_ABORT)))
			}

			requireDone(t, conn, test.name)
			if want := releaseCause(test.name); conn.Err() != want {
				t.Errorf("Err after %s = %v, want %v", test.name, conn.Err(), want)
			}
			if got := conn.State(); got != StateASPDown {
				t.Errorf("State after %s = %v, want %v", test.name, got, StateASPDown)
			}
			if !waitFor(func() bool {
				return len(goroutinesBlockedIn("go-m3ua.(*Association).monitor")) < monitors
			}, 3*time.Second) {
				t.Errorf("monitor goroutines %d -> %d after %s; the association's goroutines did not exit",
					monitors, len(goroutinesBlockedIn("go-m3ua.(*Association).monitor")), test.name)
			}
		})
	}
}

// An association stuck in the orderly shutdown -- here a peer that never
// answers ASP Inactive -- is exactly what Abort exists to drop. The shutdown
// stops waiting at once and the peer is sent an ABORT, not the SHUTDOWN the
// orderly path would have ended in.
func TestAbortDropsAnAssociationStuckInShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const port = 3933
	peer, events := newAssociationEventPeer(t, port, handshakeOnly)
	conn := dialRawPeer(t, ctx, peer, port, &HeartbeatInfo{Enabled: false})
	if !waitFor(func() bool { return conn.State() == StateASPActive }, 5*time.Second) {
		t.Fatalf("never reached ASP-ACTIVE; state = %v", conn.State())
	}

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- conn.ShutdownContext(context.Background()) }()
	if !waitFor(func() bool { return peer.count("ASP Inactive") > 0 }, 5*time.Second) {
		t.Fatal("the peer never received the shutdown's ASP Inactive")
	}

	started := time.Now()
	if err := conn.Abort(); err != nil {
		t.Fatalf("Abort: %v", err)
	}
	select {
	case err := <-shutdownDone:
		if err != ErrAssociationAborted {
			t.Errorf("ShutdownContext = %v, want %v", err, ErrAssociationAborted)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ShutdownContext is still waiting five seconds after Abort")
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Errorf("Abort and the shutdown it ended took %v; the ABORT must not wait for the peer", elapsed)
	}
	if end := associationEnd(t, events); end.State != sctp.SCTP_COMM_LOST {
		t.Errorf("the peer's SCTP layer reported %v, want %v", end.State, sctp.SCTP_COMM_LOST)
	}
	if n := peer.count("ASP Down"); n != 0 {
		t.Errorf("the peer received %d ASP Down after Abort ended the shutdown", n)
	}
}

// The same difference seen by a go-m3ua peer, in both directions: the
// association the peer loses to an ABORT ends with SCTP_COMM_LOST, which it
// reports through Err as ErrSCTPNotAlive, and one ended by Close does not.
// The accepted side's Abort is also required to leave its Listener and Endpoint
// exactly as Close does: neither tracks the association afterwards.
func TestAbortedAssociationIsLostAtItsGoM3UAPeer(t *testing.T) {
	for _, test := range []struct {
		name    string
		port    int
		release func(*Association) error
		// accepted releases the Listener's association rather than the
		// dialled one.
		accepted bool
		lost     bool
	}{
		{"dialled association closed", 3934, (*Association).Close, false, false},
		{"dialled association aborted", 3935, (*Association).Abort, false, true},
		{"accepted association closed", 3936, (*Association).Close, true, false},
		{"accepted association aborted", 3937, (*Association).Abort, true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			asp, sgp, err := setupConn(t, ctx, test.port)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				_ = asp.Close()
				_ = sgp.Close()
			})
			requireSubscribedAssociationEvents(t, asp.sctpConn, "dialled association")
			requireSubscribedAssociationEvents(t, sgp.sctpConn, "accepted association")

			local, remote := asp, sgp
			if test.accepted {
				local, remote = sgp, asp
			}
			listener, endpoint, id := sgp.listener, sgp.endpoint, sgp.ID()
			if listener == nil || endpoint == nil {
				t.Fatal("the accepted association is not tracked by a Listener and an Endpoint")
			}
			if _, tracked := endpoint.AssociationStatus(id); !tracked {
				t.Fatal("the Endpoint does not report the accepted association before the release")
			}

			if err := test.release(local); err != nil {
				t.Fatalf("%s: %v", test.name, err)
			}

			requireDone(t, remote, "the peer after "+test.name)
			peerErr := remote.Err()
			lost := errors.Is(peerErr, ErrSCTPNotAlive)
			if lost != test.lost {
				t.Fatalf("the peer ended with %v; lost to SCTP_COMM_LOST = %v, want %v", peerErr, lost, test.lost)
			}
			if test.lost {
				for _, want := range []string{"SCTP_COMM_LOST", sctp.ErrorCauseString(uint32(sctp.SCTP_ERROR_USER_ABORT))} {
					if !strings.Contains(peerErr.Error(), want) {
						t.Errorf("the peer's Err %q does not name %s", peerErr, want)
					}
				}
			} else if !errors.Is(peerErr, io.EOF) {
				t.Errorf("the peer ended with %v, want the end of stream a completed SHUTDOWN leaves", peerErr)
			}
			// The teardown closes Done before it records ASP-DOWN, so the
			// state is waited for rather than read at once.
			if !waitFor(func() bool { return remote.State() == StateASPDown }, 5*time.Second) {
				t.Errorf("the peer's state = %v, want %v", remote.State(), StateASPDown)
			}

			requireDone(t, local, test.name)
			if want := releaseCause(test.name); local.Err() != want {
				t.Errorf("Err after %s = %v, want %v", test.name, local.Err(), want)
			}
			if !waitFor(func() bool {
				listener.muConns.Lock()
				_, tracked := listener.conns[sgp]
				listener.muConns.Unlock()
				return !tracked
			}, 5*time.Second) {
				t.Error("the Listener still tracks the accepted association after it ended")
			}
			if !waitFor(func() bool {
				_, tracked := endpoint.AssociationStatus(id)
				return !tracked
			}, 5*time.Second) {
				t.Error("the Endpoint still reports the accepted association after it ended")
			}
		})
	}
}

// Abort races everything that can be in flight on a live association: the
// other releases, a blocked ReadData and a stream of WriteData. The race
// detector judges the interleavings; the test requires every call to return,
// every release to report nil, and the peer to see the association end.
//
// Which release wins a mixed race is the scheduler's choice, so the mixed
// race runs several rounds per side and logs the winner, and an Abort-only
// round per side makes sure an Abort with I/O in flight is always exercised:
// there the peer must see the SCTP_COMM_LOST an ABORT raises.
func TestAbortIsSafeAgainstConcurrentReleaseAndIO(t *testing.T) {
	const rounds = 3
	for _, side := range []struct {
		name     string
		port     int
		accepted bool
	}{
		{"dialled association", 3938, false},
		{"accepted association", 3939, true},
	} {
		for round := 0; round < rounds; round++ {
			t.Run(fmt.Sprintf("%s/mixed releases/round %d", side.name, round), func(t *testing.T) {
				raceReleasesAgainstIO(t, side.port+10*round, side.accepted, false)
			})
		}
		t.Run(side.name+"/Abort only", func(t *testing.T) {
			raceReleasesAgainstIO(t, side.port+40, side.accepted, true)
		})
	}
}

// raceReleasesAgainstIO releases one side of a live association concurrently
// with a blocked ReadData and a stream of WriteData on it. abortOnly races four
// Aborts; otherwise Aborts race Closes, ShutdownContext and, on the accepted
// side, the Listener's Close.
func raceReleasesAgainstIO(t *testing.T, port int, accepted, abortOnly bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	asp, sgp, err := setupConn(t, ctx, port)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = asp.Close()
		_ = sgp.Close()
	})
	local, remote := asp, sgp
	if accepted {
		local, remote = sgp, asp
	}
	requireSubscribedAssociationEvents(t, remote.sctpConn, "peer association")

	readDone := make(chan error, 1)
	go func() {
		_, err := local.ReadData(context.Background())
		readDone <- err
	}()
	// The writer keeps writing until the release ends it. Before that, a
	// refusal is backpressure (EAGAIN without a write deadline) or the
	// concurrent ShutdownContext withdrawing traffic, not the end of the
	// association.
	var written atomic.Int64
	writeDone := make(chan error, 1)
	go func() {
		for {
			_, err := writePayload(local, 1, []byte("in-flight"))
			if err == nil {
				written.Add(1)
				continue
			}
			select {
			case <-local.Done():
				writeDone <- err
				return
			default:
			}
		}
	}()
	// The remote end drains what the writer sends, so its receive window
	// never becomes what ends the writes.
	go func() {
		for {
			if _, err := remote.ReadData(context.Background()); err != nil {
				return
			}
		}
	}()
	if !waitFor(func() bool { return written.Load() > 0 }, 5*time.Second) {
		t.Fatal("no WriteData succeeded before the release; nothing would be in flight")
	}

	start := make(chan struct{})
	results := make(chan error, 8)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); <-start; results <- local.Abort() }()
		if !abortOnly {
			wg.Add(1)
			go func() { defer wg.Done(); <-start; results <- local.Close() }()
		}
	}
	shutdownDone := make(chan error, 1)
	ownerDone := make(chan error, 1)
	if abortOnly {
		shutdownDone <- nil
		ownerDone <- nil
	} else {
		go func() { <-start; shutdownDone <- local.ShutdownContext(context.Background()) }()
		go func() {
			<-start
			if accepted {
				ownerDone <- local.listener.Close()
				return
			}
			ownerDone <- nil
		}()
	}
	close(start)

	releasesDone := make(chan struct{})
	go func() { wg.Wait(); close(releasesDone) }()
	select {
	case <-releasesDone:
	case <-time.After(10 * time.Second):
		t.Fatal("an Abort or Close is still running ten seconds later")
	}
	close(results)
	for err := range results {
		if err != nil {
			t.Errorf("a concurrent Abort or Close returned %v, want nil", err)
		}
	}
	// ShutdownContext may lose its withdrawal to the release at any step and
	// then reports the release that won, as Err does.
	select {
	case err := <-shutdownDone:
		if err != nil && err != local.Err() {
			t.Errorf("ShutdownContext = %v, want nil or Err's %v", err, local.Err())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("ShutdownContext did not return")
	}
	select {
	case err := <-ownerDone:
		if err != nil {
			t.Errorf("the owner's Close = %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the owner's Close did not return")
	}
	for name, done := range map[string]chan error{"ReadData": readDone, "WriteData": writeDone} {
		select {
		case err := <-done:
			if err == nil {
				t.Errorf("%s in flight during the release returned nil", name)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("%s in flight during the release never returned", name)
		}
	}

	requireDone(t, local, "the concurrent releases")
	requireDone(t, remote, "the peer")
	t.Logf("%d writes before the release; %s won, and the peer ended with %v", written.Load(), local.Err(), remote.Err())
	if !errors.Is(local.Err(), ErrAssociationClosed) {
		t.Errorf("Err = %v, want %v", local.Err(), ErrAssociationClosed)
	}
	// Whichever release won is what reached the wire.
	aborted := local.Err() == ErrAssociationAborted
	if abortOnly && !aborted {
		t.Errorf("Err = %v after only Aborts, want %v", local.Err(), ErrAssociationAborted)
	}
	if lost := errors.Is(remote.Err(), ErrSCTPNotAlive) &&
		strings.Contains(remote.Err().Error(), sctp.ErrorCauseString(uint32(sctp.SCTP_ERROR_USER_ABORT))); lost != aborted {
		t.Errorf("the peer ended with %v; lost to a user ABORT = %v, want %v because %v won", remote.Err(), lost, aborted, local.Err())
	}
}
