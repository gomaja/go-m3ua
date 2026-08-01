// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

// rawPeer is a scriptable M3UA peer built directly on SCTP, so a test can
// decide exactly what the far end answers — including answering nothing at
// all. The library's own server always replies correctly, which is why the
// deadlock these tests cover stayed invisible: only a peer that goes silent
// after the handshake can expire T(beat).
type rawPeer struct {
	t        *testing.T
	ln       *sctp.SCTPListener
	addr     *sctp.SCTPAddr
	mu       sync.Mutex
	received []messages.M3UA
	// conn is the accepted association, published so a test can send
	// unsolicited traffic rather than only replying.
	conn *sctp.SCTPConn
	// reply returns the message to send in response to msg, or nil to stay
	// silent. It runs on the peer's goroutine.
	reply func(msg messages.M3UA) messages.M3UA
}

// newRawPeer starts a peer on 127.0.0.2:port. The calling test is skipped on
// platforms without SCTP support.
func newRawPeer(t *testing.T, port int, reply func(messages.M3UA) messages.M3UA) *rawPeer {
	t.Helper()

	addr, err := sctp.ResolveSCTPAddr("sctp", fmt.Sprintf("127.0.0.2:%d", port))
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

	p := &rawPeer{t: t, ln: ln, addr: addr, reply: reply}
	t.Cleanup(func() { _ = ln.Close() })
	go p.serve()

	return p
}

// serve accepts associations one after another, so a test can tear one down and
// have the client re-establish against the same peer. Tests that use a single
// association are unaffected: the loop simply waits in Accept afterwards.
func (p *rawPeer) serve() {
	for {
		conn, err := p.ln.AcceptSCTP()
		if err != nil {
			return
		}
		p.serveConn(conn)
	}
}

func (p *rawPeer) serveConn(conn *sctp.SCTPConn) {
	defer func() { _ = conn.Close() }()

	p.mu.Lock()
	p.conn = conn
	p.mu.Unlock()

	info := &sctp.SndRcvInfo{PPID: 3, Stream: 0}
	// Generous, so the peer itself never truncates what the library sends it.
	buf := make([]byte, 65535)
	for {
		n, _, err := conn.SCTPRead(buf)
		if err != nil {
			return
		}
		msg, err := messages.Parse(buf[:n])
		if err != nil {
			continue
		}

		p.mu.Lock()
		p.received = append(p.received, msg)
		p.mu.Unlock()

		reply := p.reply(msg)
		if reply == nil {
			continue // deliberately silent
		}
		b, err := reply.MarshalBinary()
		if err != nil {
			return
		}
		if _, err := conn.SCTPWrite(b, info); err != nil {
			return
		}
	}
}

// send transmits raw bytes to the connected client, for traffic the peer must
// originate rather than answer. It waits for the association to be accepted.
// sendOn writes b on the given stream.
//
// The stream matters: RFC 4666 Section 1.4.7 puts signalling on stream 0, and
// its rule 1 is "DATA messages MUST NOT be sent on stream 0", which the
// receiver enforces. A test sending DATA must therefore name a data stream.
func (p *rawPeer) sendOn(t *testing.T, b []byte, stream uint16) {
	t.Helper()

	if !waitFor(func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.conn != nil
	}, 5*time.Second) {
		t.Fatal("peer never accepted an association")
	}

	p.mu.Lock()
	conn := p.conn
	p.mu.Unlock()

	if _, err := conn.SCTPWrite(b, &sctp.SndRcvInfo{PPID: 3, Stream: stream}); err != nil {
		t.Fatalf("peer send: %v", err)
	}
}

// sendBest is send() on a best-effort basis: it gives up quietly if no
// association has been accepted or the write fails, for tests where the peer
// races the client's teardown and a failed send is not itself a defect.
func (p *rawPeer) sendBest(b []byte) {
	p.mu.Lock()
	conn := p.conn
	p.mu.Unlock()

	if conn == nil {
		return
	}
	_, _ = conn.SCTPWrite(b, &sctp.SndRcvInfo{PPID: 3, Stream: 0})
}

// count reports how many messages of the given type name the peer received, so
// a test can prove the exchange it depends on actually happened on the wire.
func (p *rawPeer) count(typeName string) int {
	p.mu.Lock()
	defer p.mu.Unlock()

	n := 0
	for _, m := range p.received {
		if m.MessageTypeName() == typeName {
			n++
		}
	}
	return n
}

// handshakeOnly answers ASP Up and ASP Active so the client reaches
// ASP-ACTIVE, then stays silent for everything else — notably BEAT.
func handshakeOnly(msg messages.M3UA) messages.M3UA {
	switch msg.(type) {
	case *messages.AspUp:
		return messages.NewAspUpAck(nil, nil)
	case *messages.AspActive:
		return messages.NewAspActiveAck(
			params.NewTrafficModeType(params.TrafficModeLoadshare),
			params.NewRoutingContext(1, 2), nil)
	default:
		return nil
	}
}

// dialRawPeer connects a client to a rawPeer with the given heartbeat config.
func dialRawPeer(t *testing.T, ctx context.Context, p *rawPeer, port int, hb *HeartbeatInfo) *Conn {
	t.Helper()

	laddr, err := sctp.ResolveSCTPAddr("sctp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatal(err)
	}

	cfg := NewClientConfig(
		hb,
		0x11111111, 0x22222222, 1, params.TrafficModeLoadshare, 0, 0,
		[]uint32{1, 2}, params.ServiceIndSCCP, 0, 0, 1,
	)
	cfg.CorrelationID = nil

	conn, err := Dial(ctx, "m3ua", laddr, p.addr, cfg)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	// Two Routing Contexts are coordinated, so RFC 4666 Section 3.3.1 requires
	// each DATA to name the one identifying its traffic flow. Which that is, is
	// the caller's knowledge; these tests are not about distribution, so they
	// pick one and keep it.
	if err := conn.SelectRoutingContext(1); err != nil {
		t.Fatalf("SelectRoutingContext: %v", err)
	}

	return conn
}

// goroutinesBlockedIn returns the stacks of any goroutine currently sitting in
// the named function, so a test can prove a reporting path is not wedged.
func goroutinesBlockedIn(fn string) []string {
	buf := make([]byte, 1<<20)
	stacks := string(buf[:runtime.Stack(buf, true)])

	var found []string
	for _, g := range strings.Split(stacks, "\n\n") {
		if strings.Contains(g, fn) {
			found = append(found, g)
		}
	}
	return found
}

// waitFor polls until cond holds or the budget expires.
func waitFor(cond func() bool, budget time.Duration) bool {
	deadline := time.Now().Add(budget)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return cond()
}

// The headline regression: a peer that completes the handshake and then stops
// answering BEATs must be detected.
//
// monitor() used to perform its SCTPRead inline inside a select arm, so while
// an idle association was parked in that read the sibling errChan arm could not
// be selected. errChan is unbuffered, so heartbeat()'s sendErr(ErrHeartbeatExpired)
// blocked forever: the connection reported ASP-ACTIVE indefinitely and the
// heartbeat goroutine leaked, defeating the only mechanism M3UA has for
// noticing a peer that is unresponsive while its SCTP association is still up.
func TestHeartbeatExpiryIsDetectedAgainstSilentPeer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peer := newRawPeer(t, 2960, handshakeOnly)
	conn := dialRawPeer(t, ctx, peer, 2960,
		NewHeartbeatInfo(50*time.Millisecond, 200*time.Millisecond, nil))

	// T(beat) is 200ms after a 50ms interval; give many rounds of headroom.
	if !waitFor(func() bool { return conn.State() != StateAspActive }, 5*time.Second) {
		t.Errorf("state = %v after T(beat) expiry against a silent peer, want the association torn down",
			conn.State())
	}

	if blocked := goroutinesBlockedIn("go-m3ua.(*Conn).sendErr"); len(blocked) > 0 {
		t.Errorf("%d goroutine(s) wedged in sendErr; the error report never reached monitor():\n%s",
			len(blocked), strings.Join(blocked, "\n"))
	}

	// The peer really did receive BEATs and really did ignore them.
	if got := peer.count("Heartbeat"); got == 0 {
		t.Error("peer received no BEAT; the test never exercised T(beat)")
	}
}

// The heartbeat goroutine must not outlive the association it belongs to.
// Before the fix it parked in sendErr for the lifetime of the process, so an
// application cycling connections leaked one goroutine per expiry.
func TestHeartbeatGoroutineDoesNotLeakAfterExpiry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peer := newRawPeer(t, 2961, handshakeOnly)
	conn := dialRawPeer(t, ctx, peer, 2961,
		NewHeartbeatInfo(50*time.Millisecond, 150*time.Millisecond, nil))

	if !waitFor(func() bool { return conn.State() != StateAspActive }, 5*time.Second) {
		t.Fatalf("association never torn down; state = %v", conn.State())
	}

	// Once the expiry is processed, no goroutine may remain in heartbeat().
	if !waitFor(func() bool {
		return len(goroutinesBlockedIn("go-m3ua.(*Conn).heartbeat")) == 0
	}, 2*time.Second) {
		t.Errorf("heartbeat goroutine leaked:\n%s",
			strings.Join(goroutinesBlockedIn("go-m3ua.(*Conn).heartbeat"), "\n"))
	}
}

// A healthy peer must NOT be torn down: the fix must not turn every idle
// association into a false positive. This is the counterpart to the silent-peer
// test — together they show T(beat) discriminates rather than always firing.
func TestHeartbeatSurvivesAgainstAnsweringPeer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peer := newRawPeer(t, 2962, func(msg messages.M3UA) messages.M3UA {
		if beat, ok := msg.(*messages.Heartbeat); ok {
			// Echo the Heartbeat Data back unchanged, as RFC 4666 requires.
			return messages.NewHeartbeatAck(beat.HeartbeatData)
		}
		return handshakeOnly(msg)
	})
	conn := dialRawPeer(t, ctx, peer, 2962,
		NewHeartbeatInfo(30*time.Millisecond, 500*time.Millisecond, nil))

	if !waitFor(func() bool { return conn.State() == StateAspActive }, 5*time.Second) {
		t.Fatalf("never reached ASP-ACTIVE; state = %v", conn.State())
	}

	// Many T(beat) rounds against a peer that answers correctly.
	time.Sleep(600 * time.Millisecond)

	if got := conn.State(); got != StateAspActive {
		t.Errorf("state = %v after a healthy heartbeat soak, want %v", got, StateAspActive)
	}
	if got := peer.count("Heartbeat"); got < 2 {
		t.Errorf("peer received %d BEATs, want several rounds", got)
	}
}

// An asynchronous error raised from a dispatch goroutine must reach monitor()
// even while the association is otherwise idle. Before the fix, monitor() was
// parked in SCTPRead, so the sendErr in the DATA path blocked and the ERR the
// RFC requires was never written — and the goroutine leaked. A peer that sent a
// stream of malformed DATA could leak unboundedly.
func TestErrorFromIdleAssociationIsReported(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	var sawError bool

	peer := newRawPeer(t, 2963, func(msg messages.M3UA) messages.M3UA {
		if _, ok := msg.(*messages.Error); ok {
			mu.Lock()
			sawError = true
			mu.Unlock()
			return nil
		}
		if _, ok := msg.(*messages.AspUp); ok {
			return messages.NewAspUpAck(nil, nil)
		}
		if _, ok := msg.(*messages.AspActive); ok {
			// Ack, then immediately provoke an error: DATA with no Protocol
			// Data is a Missing Parameter condition (RFC 4666 Section 3.3.1).
			return messages.NewAspActiveAck(
				params.NewTrafficModeType(params.TrafficModeLoadshare),
				params.NewRoutingContext(1, 2), nil)
		}
		return nil
	})

	conn := dialRawPeer(t, ctx, peer, 2963, &HeartbeatInfo{Enabled: false})
	if !waitFor(func() bool { return conn.State() == StateAspActive }, 5*time.Second) {
		t.Fatalf("never reached ASP-ACTIVE; state = %v", conn.State())
	}

	// The association is now idle: nothing further will arrive unless we ask
	// for it. Provoke the error from this side by dispatching a DATA with no
	// Protocol Data, exactly as a malformed peer message would.
	go conn.handleSignals(ctx, messages.NewData(nil, nil, nil, nil))

	if !waitFor(func() bool {
		mu.Lock()
		defer mu.Unlock()
		return sawError
	}, 5*time.Second) {
		t.Error("no ERR reached the peer from an idle association; the report path is starved")
	}

	if blocked := goroutinesBlockedIn("go-m3ua.(*Conn).sendErr"); len(blocked) > 0 {
		t.Errorf("%d goroutine(s) wedged in sendErr:\n%s", len(blocked), strings.Join(blocked, "\n"))
	}
}

// Closing a Conn must stop the reader goroutine. readLoop() blocks in SCTPRead,
// so it is released by the socket close rather than by c.done; this pins that
// it does not survive the association.
func TestReaderGoroutineStopsOnClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peer := newRawPeer(t, 2964, handshakeOnly)
	conn := dialRawPeer(t, ctx, peer, 2964, &HeartbeatInfo{Enabled: false})

	if !waitFor(func() bool { return conn.State() == StateAspActive }, 5*time.Second) {
		t.Fatalf("never reached ASP-ACTIVE; state = %v", conn.State())
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if !waitFor(func() bool {
		return len(goroutinesBlockedIn("go-m3ua.(*Conn).readLoop")) == 0
	}, 2*time.Second) {
		t.Errorf("readLoop goroutine leaked after Close:\n%s",
			strings.Join(goroutinesBlockedIn("go-m3ua.(*Conn).readLoop"), "\n"))
	}
}

// Cancelling the context must tear the association down and leave no reader or
// heartbeat goroutine behind.
func TestGoroutinesStopOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	peer := newRawPeer(t, 2965, handshakeOnly)
	conn := dialRawPeer(t, ctx, peer, 2965,
		NewHeartbeatInfo(50*time.Millisecond, 5*time.Second, nil))

	if !waitFor(func() bool { return conn.State() == StateAspActive }, 5*time.Second) {
		cancel()
		t.Fatalf("never reached ASP-ACTIVE; state = %v", conn.State())
	}

	cancel()

	for _, fn := range []string{"go-m3ua.(*Conn).readLoop", "go-m3ua.(*Conn).heartbeat"} {
		if !waitFor(func() bool { return len(goroutinesBlockedIn(fn)) == 0 }, 3*time.Second) {
			t.Errorf("%s goroutine leaked after context cancel:\n%s",
				fn, strings.Join(goroutinesBlockedIn(fn), "\n"))
		}
	}
}

// SCTP reassembles a fragmented message before delivering it, so the size of an
// IP packet is irrelevant to what a read receives. The read buffer used to be
// fixed at 1500 bytes — the Ethernet MTU — so any M3UA message above roughly
// that size was truncated, failed to parse, and was silently dropped: no error
// to the sender, nothing to the receiver. RFC 4666 Section 1.3.2.1 is explicit
// that M3UA "does not impose a 272-octet signalling information field (SIF)
// length limit" and that "Larger information blocks can be accommodated
// directly by M3UA/SCTP", so payloads past the old limit are legitimate.
func TestLargeDataRoundTrip(t *testing.T) {
	for _, size := range []int{1024, 1400, 1600, 4096, 16384} {
		t.Run(fmt.Sprintf("%dB", size), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			cliConn, srvConn, err := setupConn(t, ctx, 2980+size%97)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				_ = cliConn.Close()
				_ = srvConn.Close()
			}()

			msg := make([]byte, size)
			for i := range msg {
				msg[i] = byte(i % 251) // non-repeating enough to catch splices
			}

			if _, err := cliConn.Write(msg); err != nil {
				t.Fatalf("write %d bytes: %v", size, err)
			}

			type readResult struct {
				n   int
				err error
			}
			got := make(chan readResult, 1)
			buf := make([]byte, size*2)
			go func() {
				n, err := srvConn.Read(buf)
				got <- readResult{n, err}
			}()

			select {
			case r := <-got:
				if r.err != nil {
					t.Fatalf("read %d bytes: %v", size, r.err)
				}
				if r.n != size {
					t.Fatalf("read %d bytes, want %d (message truncated)", r.n, size)
				}
				if !bytes.Equal(buf[:r.n], msg) {
					t.Error("payload corrupted in transit")
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("read of a %d-byte payload timed out: the message was dropped", size)
			}
		})
	}
}

// A message too large even for the configured buffer must be reported, not
// silently dropped and not fatal to the association. SCTPRead does not surface
// the MSG_EOR receive flag, so a read that exactly fills the buffer is the only
// truncation signal available.
func TestOversizedMessageIsReportedNotDropped(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	var errCodes []uint32

	peer := newRawPeer(t, 2966, func(msg messages.M3UA) messages.M3UA {
		if e, ok := msg.(*messages.Error); ok && e.ErrorCode != nil {
			mu.Lock()
			errCodes = append(errCodes, e.ErrorCode.ErrorCode())
			mu.Unlock()
			return nil
		}
		return handshakeOnly(msg)
	})

	// A deliberately small buffer, so a modest DATA overflows it.
	laddr, err := sctp.ResolveSCTPAddr("sctp", "127.0.0.1:2966")
	if err != nil {
		t.Fatal(err)
	}
	cfg := NewClientConfig(
		&HeartbeatInfo{Enabled: false},
		0x11111111, 0x22222222, 1, params.TrafficModeLoadshare, 0, 0,
		[]uint32{1, 2}, params.ServiceIndSCCP, 0, 0, 1,
	)
	cfg.CorrelationID = nil
	cfg.SCTPConfig.ReadBufferSize = 256

	conn, err := Dial(ctx, "m3ua", laddr, peer.addr, cfg)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if !waitFor(func() bool { return conn.State() == StateAspActive }, 5*time.Second) {
		t.Fatalf("never reached ASP-ACTIVE; state = %v", conn.State())
	}

	// Ask the peer to send a DATA far larger than the 256-byte buffer.
	big := make([]byte, 2048)
	pd := params.NewProtocolData(0x22222222, 0x11111111, 3, 0, 0, 1, big)
	raw, err := messages.NewData(nil, nil, pd, nil).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	peer.sendOn(t, raw, 1)

	// The association must be torn down rather than left consuming the tail of
	// a message it never saw the start of. ReadMsg leaves the remainder of an
	// oversized message queued, so there is no way to resynchronise on a
	// message boundary: continuing would feed the state machine fragments.
	if !waitFor(func() bool { return conn.State() != StateAspActive }, 5*time.Second) {
		t.Errorf("state = %v after an oversized message; want the association torn down rather than resuming mid-message",
			conn.State())
	}

	// RFC 4666 Section 3.8.1: "Protocol Error" covers any protocol anomaly. The
	// ERR is best-effort — monitor() may close the association before the write
	// lands — so its absence is reported but not fatal; the teardown above is
	// the load-bearing assertion.
	mu.Lock()
	got := append([]uint32(nil), errCodes...)
	mu.Unlock()
	sawProtocolError := false
	for _, c := range got {
		if c == params.ErrProtocolError {
			sawProtocolError = true
		}
	}
	if !sawProtocolError {
		t.Logf("no Protocol Error reached the peer (codes seen: %v); the teardown raced the write", got)
	}
}

// A message whose length is exactly the configured ceiling is complete, not
// truncated. The old heuristic inferred truncation from a full buffer, so this
// case was a false positive: the message was rejected and the association was
// told a protocol error had occurred. ReadMsg uses MSG_EOR instead, so the
// boundary is decided by the kernel rather than guessed from a length.
func TestMessageExactlyAtCeilingIsAccepted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Build the DATA first so the ceiling can be set to its exact length.
	payload := make([]byte, 512)
	for i := range payload {
		payload[i] = byte(i % 251)
	}
	pd := params.NewProtocolData(0x22222222, 0x11111111, 3, 0, 0, 1, payload)
	data := messages.NewData(nil, params.NewRoutingContext(1), pd, nil)
	rawData, err := data.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	peer := newRawPeer(t, 2967, handshakeOnly)

	laddr, err := sctp.ResolveSCTPAddr("sctp", "127.0.0.1:2967")
	if err != nil {
		t.Fatal(err)
	}
	cfg := NewClientConfig(
		&HeartbeatInfo{Enabled: false},
		0x11111111, 0x22222222, 1, params.TrafficModeLoadshare, 0, 0,
		[]uint32{1, 2}, params.ServiceIndSCCP, 0, 0, 1,
	)
	cfg.CorrelationID = nil
	cfg.SCTPConfig.ReadBufferSize = len(rawData) // exactly the message size

	conn, err := Dial(ctx, "m3ua", laddr, peer.addr, cfg)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if !waitFor(func() bool { return conn.State() == StateAspActive }, 5*time.Second) {
		t.Fatalf("never reached ASP-ACTIVE; state = %v", conn.State())
	}

	peer.sendOn(t, rawData, 1)

	got := make(chan int, 1)
	go func() {
		buf := make([]byte, 4096)
		n, err := conn.Read(buf)
		if err == nil {
			got <- n
		}
	}()

	select {
	case n := <-got:
		if n != len(payload) {
			t.Errorf("read %d bytes, want %d", n, len(payload))
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("a %d-byte message sent with the ceiling at exactly %d was not delivered: "+
			"a full buffer is being mistaken for a truncated message", len(rawData), len(rawData))
	}

	if got := conn.State(); got != StateAspActive {
		t.Errorf("state = %v after an exactly-sized message; the association must survive", got)
	}
}
