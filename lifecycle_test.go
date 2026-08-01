// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

// Connection setup is the thinnest-covered part of the library and the place
// interop failures actually surface: a peer that is hostile, broken, or merely
// slow must never leave Dial/Accept hung, leaking goroutines, or reporting
// success on an association that never reached ASP-ACTIVE.
//
// Dial and Accept both cap establishment at 10s internally, so tests that must
// reach that path are slow by construction; they assert the timeout is honoured
// rather than trying to defeat it.

// clientCfg builds a client Config for the lifecycle tests.
func clientCfg(hb *HeartbeatInfo) *Config {
	cfg := NewClientConfig(
		hb,
		0x11111111, 0x22222222, 1, params.TrafficModeLoadshare, 0, 0,
		[]uint32{1, 2}, params.ServiceIndSCCP, 0, 0, 1,
	)
	cfg.CorrelationID = nil
	return cfg
}

// dialTo dials a raw peer without the t.Fatal-on-error behaviour of
// dialRawPeer, so a test can assert on the failure itself.
func dialTo(ctx context.Context, t *testing.T, raddr *sctp.SCTPAddr, port int, cfg *Config) (*Conn, error) {
	t.Helper()

	laddr, err := sctp.ResolveSCTPAddr("sctp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatal(err)
	}
	return Dial(ctx, "m3ua", laddr, raddr, cfg)
}

// A peer that accepts the association and then says nothing must not leave the
// caller hung forever. Dial waits for ASP-ACTIVE and must give up on its own
// deadline, reporting the failure rather than returning a Conn that was never
// established.
func TestDialAgainstMuteePeerTimesOut(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping: exercises Dial's 10s establishment timeout")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Answers nothing at all — not even ASP Up Ack.
	peer := newRawPeer(t, 2990, func(messages.M3UA) messages.M3UA { return nil })

	start := time.Now()
	conn, err := dialTo(ctx, t, peer.addr, 2990, clientCfg(&HeartbeatInfo{Enabled: false}))
	elapsed := time.Since(start)

	if err == nil {
		_ = conn.Close()
		t.Fatal("Dial returned a Conn against a peer that never acked ASP Up; want an error")
	}
	if !errors.Is(err, ErrTimeout) {
		t.Errorf("Dial error = %v, want ErrTimeout", err)
	}
	// The peer did receive the ASP Up: the client really did try.
	if got := peer.count("ASP Up"); got == 0 {
		t.Error("peer received no ASP Up; the handshake never started")
	}
	if elapsed > 20*time.Second {
		t.Errorf("Dial took %v to give up; the establishment deadline is not being honoured", elapsed)
	}
}

// Cancelling the context must abort establishment promptly rather than waiting
// out the internal 10s deadline: an application shutting down should not be
// held hostage by an unresponsive peer.
func TestDialAbortsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	peer := newRawPeer(t, 2991, func(messages.M3UA) messages.M3UA { return nil })

	done := make(chan error, 1)
	go func() {
		conn, err := dialTo(ctx, t, peer.addr, 2991, clientCfg(&HeartbeatInfo{Enabled: false}))
		if conn != nil {
			_ = conn.Close()
		}
		done <- err
	}()

	// Let the association come up and the ASP Up go out, then cancel.
	time.Sleep(300 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Error("Dial succeeded despite context cancellation")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Dial ignored context cancellation and is still waiting on its own deadline")
	}
}

// A peer that aborts the association mid-handshake must surface as a failure,
// not a hang and not a half-open Conn.
func TestDialAgainstPeerThatDropsMidHandshake(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	addr, err := sctp.ResolveSCTPAddr("sctp", "127.0.0.2:2992")
	if err != nil {
		t.Fatal(err)
	}
	ln, err := sctp.ListenSCTP("sctp", addr)
	if err != nil {
		if isSCTPUnsupported(err) {
			t.Skipf("skipping socket-backed test: %v", err)
		}
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()

	go func() {
		conn, err := ln.AcceptSCTP()
		if err != nil {
			return
		}
		// Read the ASP Up, then drop the association without answering.
		buf := make([]byte, 1500)
		_, _, _ = conn.SCTPRead(buf)
		_ = conn.Close()
	}()

	done := make(chan error, 1)
	go func() {
		conn, err := dialTo(ctx, t, addr, 2992, clientCfg(&HeartbeatInfo{Enabled: false}))
		if conn != nil {
			_ = conn.Close()
		}
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Error("Dial reported success against a peer that dropped mid-handshake")
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Dial hung after the peer dropped the association")
	}
}

// A peer that jumps straight to ASP Active without ASP Up is out of sequence.
// The client must not treat that as establishment: reaching ASP-ACTIVE requires
// the ASP Up Ack, and an out-of-order message must leave state alone.
func TestOutOfSequenceHandshakeDoesNotEstablish(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping: exercises Dial's 10s establishment timeout")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Never sends ASP Up Ack; answers the ASP Up with an ASP Active Ack, which
	// the client did not ask for at this point in the sequence.
	peer := newRawPeer(t, 2993, func(msg messages.M3UA) messages.M3UA {
		if _, ok := msg.(*messages.AspUp); ok {
			return messages.NewAspActiveAck(
				params.NewTrafficModeType(params.TrafficModeLoadshare),
				params.NewRoutingContext(1, 2), nil)
		}
		return nil
	})

	conn, err := dialTo(ctx, t, peer.addr, 2993, clientCfg(&HeartbeatInfo{Enabled: false}))
	if err == nil {
		state := conn.State()
		_ = conn.Close()
		t.Fatalf("Dial succeeded on an out-of-sequence handshake (state=%v); ASP-ACTIVE requires an ASP Up Ack", state)
	}
	if !errors.Is(err, ErrTimeout) {
		t.Errorf("Dial error = %v, want ErrTimeout", err)
	}
}

// A peer that answers with garbage must not crash the client or wedge it.
// Undecodable input is dropped with a log line; the client should still be
// running afterwards and still fail establishment cleanly.
func TestGarbageFromPeerDoesNotCrashClient(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping: exercises Dial's 10s establishment timeout")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peer := newRawPeer(t, 2994, func(messages.M3UA) messages.M3UA { return nil })

	// Fire garbage at the client as soon as it connects.
	go func() {
		for _, junk := range [][]byte{
			{0xff}, // too short for a header
			{0x02, 0x00, 0x03, 0x01, 0x00, 0x00, 0x00, 0x08}, // unsupported version
			{0x01, 0x00, 0xff, 0xff, 0x00, 0x00, 0x00, 0x08}, // unknown class/type
			{0x01, 0x00, 0x03, 0x01, 0xff, 0xff, 0xff, 0xff}, // absurd length
		} {
			peer.sendBest(junk)
			time.Sleep(50 * time.Millisecond)
		}
	}()

	conn, err := dialTo(ctx, t, peer.addr, 2994, clientCfg(&HeartbeatInfo{Enabled: false}))
	if err == nil {
		_ = conn.Close()
		t.Fatal("Dial reported success against a peer sending only garbage")
	}

	// The point is that we got here at all: no panic, no hang.
	if !errors.Is(err, ErrTimeout) {
		t.Logf("Dial error = %v (any clean failure is acceptable here)", err)
	}
}

// Accept must reject a peer that opens the association and then says nothing,
// rather than hanging forever or returning a Conn that never established.
func TestAcceptAgainstMutePeerTimesOut(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping: exercises Accept's 10s establishment timeout")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srvCfg := NewServerConfig(
		&HeartbeatInfo{Enabled: false},
		0x22222222, 0x11111111, 1, params.TrafficModeLoadshare, 0, 0,
		[]uint32{1, 2}, params.ServiceIndSCCP, 0, 0, 1,
	)
	srvCfg.AspIdentifier = nil
	srvCfg.CorrelationID = nil

	raddr, err := sctp.ResolveSCTPAddr("sctp", "127.0.0.2:2995")
	if err != nil {
		t.Fatal(err)
	}
	ln, err := Listen("m3ua", raddr, srvCfg)
	if err != nil {
		if isSCTPUnsupported(err) {
			t.Skipf("skipping socket-backed test: %v", err)
		}
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()

	accepted := make(chan error, 1)
	go func() {
		conn, err := ln.Accept(ctx)
		if conn != nil {
			_ = conn.Close()
		}
		accepted <- err
	}()

	// A bare SCTP client that connects and then stays silent: no ASP Up.
	laddr, err := sctp.ResolveSCTPAddr("sctp", "127.0.0.1:2995")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := sctp.DialSCTP("sctp", laddr, raddr)
	if err != nil {
		t.Fatalf("raw dial: %v", err)
	}
	defer func() { _ = raw.Close() }()

	select {
	case err := <-accepted:
		if err == nil {
			t.Error("Accept returned a Conn for a peer that never sent ASP Up")
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Accept hung on a silent peer")
	}
}

// A peer whose ASP Active Ack names a Routing Context the ASP never asked
// about must not put the two ends into an unbounded exchange.
//
// Rejecting such an Ack republished ASP-INACTIVE, and the client's ASP-INACTIVE
// entry action is "send ASP Active" — so every rejected Ack produced a fresh
// ASP Active, which produced a fresh Ack, as fast as the two could write. A
// capture of two ASPs with different Routing Contexts against one SGP showed
// 1797 M3UA messages and 934 Errors inside a second, on a link that should have
// carried a single refused handshake. T(ack) already bounds retransmission
// (DefaultTAckRetries), so re-running the entry action added nothing but the
// storm.
//
// This is the ordinary multi-tenancy configuration — one SGP port, each ASP
// with its own Routing Context — so a mismatch is a configuration error that
// must be reported, not amplified.
func TestRejectedAspActiveAckDoesNotStorm(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	peer := newRawPeer(t, 3040, func(msg messages.M3UA) messages.M3UA {
		switch msg.(type) {
		case *messages.AspUp:
			return messages.NewAspUpAck(nil, nil)
		case *messages.AspActive:
			// Routing Context 99 is not one the client asked about: clientCfg
			// is configured for {1, 2}.
			return messages.NewAspActiveAck(
				params.NewTrafficModeType(params.TrafficModeLoadshare),
				params.NewRoutingContext(99), nil,
			)
		default:
			return nil
		}
	})

	cfg := clientCfg(&HeartbeatInfo{Enabled: false})
	// T(ack) is set beyond the life of the test so no retransmission can occur:
	// every ASP Active the peer sees past the first is then unambiguously the
	// entry action running a second time, not a legitimate resend.
	cfg.TAck = time.Minute

	conn, err := dialTo(ctx, t, peer.addr, 3040, cfg)
	if err == nil {
		_ = conn.Close()
		t.Fatal("Dial reported success against a peer acking a Routing Context we never asked about")
	}

	// Let anything still in flight land before counting.
	time.Sleep(300 * time.Millisecond)

	if got := peer.count("ASP Active"); got != 1 {
		t.Errorf("peer received %d ASP Active messages, want exactly 1: a rejected Ack is re-triggering the ASP-INACTIVE entry action", got)
	}
	// The exchange must have happened at all, or the count above is satisfied
	// by a handshake that never started.
	if got := peer.count("ASP Up"); got != 1 {
		t.Errorf("peer received %d ASP Up messages, want exactly 1", got)
	}
}

// NewConfig is the library's own documented constructor and it leaves
// HeartbeatInfo nil, but Dial and Accept read HeartbeatInfo.Interval straight
// off the Config. A caller who did not want BEATs — the documented way to build
// a Config without them — got a nil pointer dereference before the association
// was even attempted:
//
//	panic: runtime error: invalid memory address or nil pointer dereference
//
// A library must not panic on its own constructor's output.
func TestConfigWithoutHeartbeatInfoDials(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	peer := newRawPeer(t, 3042, handshakeOnly)

	cfg := NewConfig(0x11111111, 0x22222222, params.ServiceIndSCCP, 0, 0, 1)
	cfg.SetTrafficModeType(params.TrafficModeLoadshare)
	cfg.SetRoutingContexts(1, 2)
	if cfg.HeartbeatInfo != nil {
		t.Fatalf("NewConfig set HeartbeatInfo = %+v; this test exists to cover the nil case", cfg.HeartbeatInfo)
	}

	conn, err := dialTo(ctx, t, peer.addr, 3042, cfg)
	if err != nil {
		t.Fatalf("Dial with a Config built by NewConfig: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if got := conn.State(); got != StateAspActive {
		t.Errorf("State = %v, want %v", got, StateAspActive)
	}
}

// The same for the server side: Accept read HeartbeatInfo.Interval off the
// listener's Config and panicked identically.
func TestConfigWithoutHeartbeatInfoAccepts(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const port = 3044
	srvCfg := NewConfig(0x22222222, 0x11111111, params.ServiceIndSCCP, 0, 0, 1)
	srvCfg.SetTrafficModeType(params.TrafficModeLoadshare)
	srvCfg.SetRoutingContexts(1, 2)
	if srvCfg.HeartbeatInfo != nil {
		t.Fatalf("NewConfig set HeartbeatInfo = %+v; this test exists to cover the nil case", srvCfg.HeartbeatInfo)
	}

	ln, err := Listen("m3ua", mcAddr(port, "127.0.0.1"), srvCfg)
	if err != nil {
		if isSCTPUnsupported(err) {
			t.Skipf("skipping socket-backed test: %v", err)
		}
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()

	type acceptResult struct {
		conn *Conn
		err  error
	}
	accepted := make(chan acceptResult, 1)
	go func() {
		c, err := ln.Accept(ctx)
		accepted <- acceptResult{conn: c, err: err}
	}()

	cli, err := Dial(ctx, "m3ua", mcAddr(port+1, "127.0.0.2"), mcAddr(port, "127.0.0.1"), mcClientConfig(0xDD000001))
	if err != nil {
		select {
		case result := <-accepted:
			if result.conn != nil {
				_ = result.conn.Close()
			}
		default:
		}
		t.Fatalf("Dial against a listener configured by NewConfig: %v", err)
	}
	defer func() { _ = cli.Close() }()

	select {
	case result := <-accepted:
		if result.conn != nil {
			defer func() { _ = result.conn.Close() }()
		}
		if result.err != nil {
			t.Fatalf("Accept with a Config built by NewConfig: %v", result.err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Accept never returned")
	}
}

// Listen must reject an invalid network name rather than panicking, and must
// not leave a listener behind.
func TestListenRejectsInvalidNetwork(t *testing.T) {
	addr, err := sctp.ResolveSCTPAddr("sctp", "127.0.0.2:2996")
	if err != nil {
		t.Fatal(err)
	}

	ln, err := Listen("not-a-network", addr, NewServerConfig(
		&HeartbeatInfo{Enabled: false},
		0x22222222, 0x11111111, 1, params.TrafficModeLoadshare, 0, 0,
		[]uint32{1, 2}, params.ServiceIndSCCP, 0, 0, 1,
	))
	if err == nil {
		_ = ln.Close()
		t.Fatal("Listen accepted an invalid network name")
	}
	if !strings.Contains(err.Error(), "invalid network") {
		t.Errorf("Listen error = %v, want it to name the invalid network", err)
	}
}

// Dial must reject an invalid network name before touching the socket.
func TestDialRejectsInvalidNetwork(t *testing.T) {
	ctx := context.Background()

	addr, err := sctp.ResolveSCTPAddr("sctp", "127.0.0.2:2997")
	if err != nil {
		t.Fatal(err)
	}

	conn, err := Dial(ctx, "not-a-network", nil, addr, clientCfg(&HeartbeatInfo{Enabled: false}))
	if err == nil {
		_ = conn.Close()
		t.Fatal("Dial accepted an invalid network name")
	}
	if !strings.Contains(err.Error(), "invalid network") {
		t.Errorf("Dial error = %v, want it to name the invalid network", err)
	}
}

// Close must be safe to call more than once and from several goroutines at
// once: applications routinely defer a Close alongside an explicit one, and a
// double close of the underlying socket would panic or error.
func TestCloseIsIdempotentAndConcurrencySafe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cliConn, srvConn, err := setupConn(t, ctx, 2998)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srvConn.Close() }()

	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = cliConn.Close()
		}()
	}
	wg.Wait()

	// closeOnce means exactly one call does the work; the rest must return
	// without panicking. Any error must at least not be a double-close crash.
	for i, err := range errs {
		if err != nil {
			t.Logf("Close #%d returned %v (only the first call does the work)", i, err)
		}
	}

	// A closed Conn must refuse writes rather than panicking on a dead socket.
	if _, err := cliConn.Write([]byte{0xde, 0xad}); err == nil {
		t.Error("Write on a closed Conn returned nil error")
	}
}

// Writing before the association reaches ASP-ACTIVE must be refused, not
// silently dropped: RFC 4666 only permits DATA in the ASP-ACTIVE state.
//
// This must not panic either. Write and WritePD used to choose a stream before
// checking state, and chooseStreamID feeds maxMessageStreamID to rand.Intn,
// which panics on 0 — the value every Conn holds until a peer negotiates its
// stream count.
func TestWriteBeforeActiveIsRefused(t *testing.T) {
	for _, st := range []State{StateAspDown, StateAspInactive} {
		t.Run(st.String(), func(t *testing.T) {
			conn, _ := newTestConn(t, st, modeClient)

			if _, err := conn.Write([]byte{0xde, 0xad, 0xbe, 0xef}); !errors.Is(err, ErrNotEstablished) {
				t.Errorf("Write in %v error = %v, want ErrNotEstablished", st, err)
			}

			pd := params.NewProtocolData(0x11111111, 0x22222222, 3, 0, 0, 1, []byte{0xde, 0xad})
			if _, err := conn.WritePD(pd); !errors.Is(err, ErrNotEstablished) {
				t.Errorf("WritePD in %v error = %v, want ErrNotEstablished", st, err)
			}
		})
	}
}

// Stream selection must survive every stream count a peer can negotiate.
// maxMessageStreamID is the peer's outbound stream count less one (stream 0 is
// reserved for management), so a peer offering a single stream leaves it at 0.
// The previous random selection called rand.Intn(0) there, which panics: a peer
// could crash the application remotely simply by negotiating one stream. The
// SLS-derived mapping must keep that guarantee for every SLS a routing label
// can carry.
func TestStreamSelectionHandlesEveryNegotiatedCount(t *testing.T) {
	for _, max := range []uint16{0, 1, 2, 3, 17, 65534} {
		t.Run(fmt.Sprintf("max%d", max), func(t *testing.T) {
			conn, _ := newTestConn(t, StateAspActive, modeClient)
			conn.maxMessageStreamID = max

			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("streamFor panicked at maxMessageStreamID=%d: %v", max, r)
				}
			}()

			for sls := 0; sls < 256; sls++ {
				got := conn.streamFor(uint8(sls))
				if got > max {
					t.Fatalf("streamFor(%d) = %d, want <= %d (the negotiated maximum)", sls, got, max)
				}
				// Stream 0 is reserved for management messages (RFC 4666
				// Section 1.4.6), so data must not be sent on it whenever a
				// data stream actually exists.
				if max >= 1 && got == 0 {
					t.Fatalf("streamFor(%d) = 0 with %d data stream(s) available; stream 0 is reserved for management", sls, max)
				}
			}
		})
	}
}

// Closing a Listener must take the associations it accepted down with it.
// Previously Close shut only the SCTP listener, so every Conn it had produced
// kept running: monitor and reader goroutines carried on against peers that had
// no idea the service was gone, leaking one set per client and leaving
// half-open associations behind on a server restart.
func TestListenerCloseShutsDownAcceptedConns(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cliConn, srvConn, err := setupConn(t, ctx, 2999)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cliConn.Close() }()

	if got := srvConn.State(); got != StateAspActive {
		t.Fatalf("server state = %v, want %v", got, StateAspActive)
	}

	// setupConn registers the listener for cleanup; close it early here.
	if srvConn.listener == nil {
		t.Fatal("accepted Conn has no listener reference; Close cannot reach it")
	}
	if err := srvConn.listener.Close(); err != nil {
		t.Fatalf("Listener.Close: %v", err)
	}

	if !waitFor(func() bool { return srvConn.State() == StateAspDown }, 3*time.Second) {
		t.Errorf("accepted Conn state = %v after the listener closed, want %v",
			srvConn.State(), StateAspDown)
	}

	// Closing the listener also closes the underlying sockets, so both ends
	// tear down through their read paths; the goroutine accounting for a direct
	// Close is covered by TestMonitorExitsOnDirectClose.
}

// Accept's documented contract is that a cancelled ctx does NOT interrupt an
// Accept parked waiting for a peer, and that Close is the only thing that does
// (server.go). Every caller that starts an accept goroutine and later waits for
// it to exit depends on that second half being true, because closing the
// listener is their only way to collect the goroutine.
//
// Nothing tested it. That mattered: mcConnect waited for its accept goroutine
// in a cleanup registered after mcListen's Close and therefore running before
// it, so the wait blocked on a goroutine whose only unblock was queued behind
// the wait. The suite did not report a failed assertion, it went silent for ten
// minutes and died on "panic: test timed out" naming whichever test the timer
// happened to catch, with the rest of the package unreported.
//
// This pins the half that the fix leans on. If Close ever stops interrupting a
// parked Accept, this fails in seconds and says so, instead of the suite
// hanging again somewhere unrelated.
func TestCloseInterruptsAParkedAccept(t *testing.T) {
	ln := mcListen(t, mcAddr(3181, "127.0.0.1"))

	// No peer ever dials, so this Accept parks in the accept syscall.
	returned := make(chan error, 1)
	go func() {
		_, err := ln.Accept(context.Background())
		returned <- err
	}()

	// Let it reach the syscall. Racing Close against a goroutine that has not
	// parked yet would prove nothing about interrupting a blocked Accept.
	time.Sleep(300 * time.Millisecond)
	select {
	case err := <-returned:
		t.Fatalf("Accept returned %v before anything closed the listener; "+
			"it never parked, so this test would not prove what it claims", err)
	default:
	}

	_ = ln.Close()

	select {
	case <-returned:
		// Any error is fine; that Accept returned at all is the contract.
	case <-time.After(5 * time.Second):
		t.Fatal("Accept was still parked 5s after Listener.Close; " +
			"callers that wait for an accept goroutine can no longer collect it")
	}
}

// Listener.Close takes muConns and then calls Conn.Close, which calls back into
// the listener to deregister. Holding the lock across that call would deadlock
// the shutdown path, so this pins that Close completes promptly.
func TestListenerCloseDoesNotDeadlockOnConnDeregistration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cliConn, srvConn, err := setupConn(t, ctx, 3001)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cliConn.Close() }()

	done := make(chan error, 1)
	go func() { done <- srvConn.listener.Close() }()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Listener.Close deadlocked: it holds muConns while Conn.Close reacquires it")
	}
}

// Closing a Conn must remove it from its listener's set, so a long-lived server
// does not accumulate every association it has ever accepted.
func TestClosedConnIsForgottenByItsListener(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cliConn, srvConn, err := setupConn(t, ctx, 3002)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cliConn.Close() }()

	ln := srvConn.listener
	if ln == nil {
		t.Fatal("accepted Conn has no listener reference")
	}

	ln.muConns.Lock()
	before := len(ln.conns)
	ln.muConns.Unlock()
	if before == 0 {
		t.Fatal("listener is not tracking the accepted Conn")
	}

	if err := srvConn.Close(); err != nil {
		t.Fatalf("Conn.Close: %v", err)
	}

	ln.muConns.Lock()
	after := len(ln.conns)
	ln.muConns.Unlock()
	if after != before-1 {
		t.Errorf("listener tracks %d Conns after closing one, want %d", after, before-1)
	}
}

// monitor() must exit promptly when Close() is called directly, with the peer
// still holding its side of the association open.
//
// Its select gained a c.done arm so it observes the close directly. Without it
// monitor still exits — the socket close makes the pending read fail, and the
// reader reports that through readErr — so this is a latency and clarity
// improvement rather than a leak fix: measured, the goroutine goes away inside
// 100ms either way. The test pins that Close is self-sufficient and does not
// depend on a peer or a timeout to unblock the loop.
func TestMonitorExitsOnDirectClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peer := newRawPeer(t, 3003, handshakeOnly)
	conn := dialRawPeer(t, ctx, peer, 3003, &HeartbeatInfo{Enabled: false})

	if !waitFor(func() bool { return conn.State() == StateAspActive }, 5*time.Second) {
		t.Fatalf("never reached ASP-ACTIVE; state = %v", conn.State())
	}

	before := len(goroutinesBlockedIn("go-m3ua.(*Conn).monitor"))
	if before == 0 {
		t.Fatal("no monitor goroutine running; the test cannot observe the exit")
	}

	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// The peer is deliberately left running, so nothing external provokes the
	// teardown: Close alone must be enough.
	if !waitFor(func() bool {
		return len(goroutinesBlockedIn("go-m3ua.(*Conn).monitor")) < before
	}, 3*time.Second) {
		t.Errorf("monitor goroutines %d -> %d after Close; the loop never noticed",
			before, len(goroutinesBlockedIn("go-m3ua.(*Conn).monitor")))
	}
}
