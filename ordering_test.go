// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// M3UA exists to carry MTP3, and MTP3 guarantees sequenced delivery for
// messages sharing a Signalling Link Selection value (RFC 4666 Section 1.4.7:
// the SCTP stream is chosen so that "the sequenced delivery of the messages
// within a particular SLS is maintained"). SCTP delivers in order within a
// stream, so the ordering arrives at the library intact — and the library then
// threw it away.
//
// monitor() handed every received message to a fresh goroutine:
//
//	case raw := <-rawChan:
//		go func() { ... c.handleSignals(ctx, msg) }()
//
// and handleSignals then spawned a second one for DATA (go c.handleData(...)).
// Two independent scheduling points, so the order in which payloads reached
// dataChan was whatever the scheduler chose. For an SS7 stack that is a
// correctness failure, not a performance detail: reordered TCAP components
// break transactions, and the reordering is invisible in any test that sends
// one message at a time.
//
// These tests are socket-backed so the whole path is exercised — SCTP receive,
// reader goroutine, dispatcher, handler, dataChan — rather than a handler in
// isolation.

// writeRetrying writes b, retrying while the SCTP send buffer is full.
//
// Sends pass MSG_DONTWAIT, so with no write deadline in force a burst larger
// than the send buffer reports syscall.EAGAIN; see
// TestWriteReportsEAGAINWhenTheSendBufferIsFull. These tests are about
// ordering, not about congestion, so they retry rather than set a deadline —
// retrying keeps them independent of how long a full buffer takes to drain.
func writeRetrying(t *testing.T, c *Conn, b []byte, stream uint16) {
	t.Helper()

	deadline := time.Now().Add(20 * time.Second)
	for {
		_, err := c.WriteToStream(b, stream)
		if err == nil {
			return
		}
		if !errors.Is(err, syscall.EAGAIN) {
			t.Fatalf("write: %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("write still blocked by a full send buffer after 20s")
		}
		time.Sleep(time.Millisecond)
	}
}

// orderedPayloads sends n sequenced payloads on one stream and returns what the
// far end read, in the order it read them. Sending and reading are concurrent so
// the send buffer drains as it fills.
func orderedPayloads(t *testing.T, from, to *Conn, n int) []string {
	t.Helper()

	go func() {
		for i := 0; i < n; i++ {
			// One stream, so SCTP itself preserves order and any reordering
			// observed is the library's own.
			writeRetrying(t, from, []byte(fmt.Sprintf("%04d", i)), 1)
		}
	}()

	got := make([]string, 0, n)
	for i := 0; i < n; i++ {
		s, err := readWithin(t, to, 10*time.Second)
		if err != nil {
			t.Fatalf("only %d of %d payloads arrived: %v", len(got), n, err)
		}
		got = append(got, s)
	}
	return got
}

// The headline invariant: payloads sent in order on one stream must be read in
// that order.
func TestDataOnOneStreamIsDeliveredInOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const port = 3080
	ln := mcListen(t, mcAddr(port, "127.0.0.1"))
	asps := mcConnect(t, ctx, ln, mcAddr(port, "127.0.0.1"), []string{"127.0.0.2"}, port)

	const n = 200
	got := orderedPayloads(t, asps[0].client, asps[0].server, n)

	for i, s := range got {
		if want := fmt.Sprintf("%04d", i); s != want {
			t.Fatalf("payload %d read as %q, want %q: DATA was reordered in the library, "+
				"not on the wire (full order: %v)", i, s, want, got[:min(len(got), 20)])
		}
	}
}

// Ordering must hold in the SGP-to-ASP direction too.
func TestDataFromServerIsDeliveredInOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const port = 3083
	ln := mcListen(t, mcAddr(port, "127.0.0.1"))
	asps := mcConnect(t, ctx, ln, mcAddr(port, "127.0.0.1"), []string{"127.0.0.2"}, port)

	const n = 200
	got := orderedPayloads(t, asps[0].server, asps[0].client, n)

	for i, s := range got {
		if want := fmt.Sprintf("%04d", i); s != want {
			t.Fatalf("payload %d read as %q, want %q (full order: %v)", i, s, want, got[:min(len(got), 20)])
		}
	}
}

// Two ASPs sending concurrently must each keep their own order; the fix must
// not serialise one association behind another, only within one.
func TestOrderingIsPerAssociation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const port = 3086
	ln := mcListen(t, mcAddr(port, "127.0.0.1"))
	asps := mcConnect(t, ctx, ln, mcAddr(port, "127.0.0.1"), []string{"127.0.0.2", "127.0.0.3"}, port)

	const n = 100
	done := make(chan []string, len(asps))
	for _, a := range asps {
		go func() {
			go func() {
				for i := 0; i < n; i++ {
					writeRetrying(t, a.client, []byte(fmt.Sprintf("%04d", i)), 1)
				}
			}()
			got := make([]string, 0, n)
			for i := 0; i < n; i++ {
				s, err := readWithin(t, a.server, 10*time.Second)
				if err != nil {
					t.Errorf("read: %v", err)
					break
				}
				got = append(got, s)
			}
			done <- got
		}()
	}

	for range asps {
		got := <-done
		if got == nil {
			t.Fatal("an association failed mid-test")
		}
		for i, s := range got {
			if want := fmt.Sprintf("%04d", i); s != want {
				t.Fatalf("payload %d read as %q, want %q: per-association ordering was lost", i, s, want)
			}
		}
	}
}

// Ordering must not come at the cost of the dispatcher's liveness: a peer that
// floods DATA must not stop the association answering an ASPSM message. This
// guards the obvious over-correction — handling everything inline on one
// goroutine and blocking on a full dataChan.
func TestSignallingIsAnsweredWhileDataFlows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const port = 3089
	ln := mcListen(t, mcAddr(port, "127.0.0.1"))
	asps := mcConnect(t, ctx, ln, mcAddr(port, "127.0.0.1"), []string{"127.0.0.2"}, port)

	// A burst nobody is reading, sized under the SCTP send buffer so this test
	// is about dispatcher starvation rather than the EAGAIN limit above.
	const burst = 150
	for i := 0; i < burst; i++ {
		if _, err := asps[0].client.WriteToStream([]byte("flood"), 1); err != nil {
			t.Fatalf("flood write %d: %v", i, err)
		}
	}

	if _, err := asps[0].client.WriteSignal(
		messages.NewHeartbeat(params.NewHeartbeatData([]byte("still there?"))),
	); err != nil {
		t.Fatalf("BEAT: %v", err)
	}

	// The server answers a BEAT with a BEAT Ack in every state; if the
	// dispatcher were wedged behind the DATA burst it would never arrive.
	if !waitFor(func() bool { return asps[0].client.State() == StateAspActive }, 5*time.Second) {
		t.Fatal("client left ASP-ACTIVE while flooding")
	}
	// Draining proves the burst really was queued rather than dropped.
	drained := 0
	for i := 0; i < burst; i++ {
		if _, err := readWithin(t, asps[0].server, 5*time.Second); err != nil {
			break
		}
		drained++
	}
	if drained != burst {
		t.Errorf("drained %d of %d flooded payloads", drained, burst)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// A full inbound DATA queue must not stop the association answering the peer.
//
// handleData runs inline on the single dispatch goroutine so that DATA keeps the
// order the peer sent it. That makes the dispatcher a shared resource: parking
// it on a full queue parks every class, so the Acks RFC 4666 Sections 4.3.4.1
// and 4.3.4.2 make mandatory, and the BEAT Ack Section 3.5.5 requires, would
// never be sent — and a healthy node would be declared dead and torn down,
// losing the whole queue along with it.
func TestFullDataQueueStillAnswersSignalling(t *testing.T) {
	conn, sent := newTestConn(t, StateAspActive, modeServer)
	// A legal arrival stream: DATA on stream 0 is refused outright by Section
	// 1.4.7's rule 1, so without this the payloads never reach the queue this
	// test is about.
	conn.recvStream.Store(1)

	// newTestConn's dataChan holds 8; fill it well past that.
	for i := 0; i < 64; i++ {
		conn.handleData(context.Background(), messages.NewData(
			nil, params.NewRoutingContext(1),
			params.NewProtocolData(0x11111111, 0x22222222, params.ServiceIndSCCP, 0, 0, 1, []byte("flood")),
			nil,
		))
	}

	// The queue is full, but a BEAT must still be answered.
	if err := conn.handleHeartbeat(messages.NewHeartbeat(params.NewHeartbeatData([]byte("alive?")))); err != nil {
		t.Fatalf("handleHeartbeat with a full DATA queue: %v", err)
	}
	// handleHeartbeat answers by flipping the received BEAT's type rather than
	// building a new message, so the Ack is a *messages.Heartbeat carrying
	// MsgTypeHeartbeatAck.
	found := false
	for _, m := range *sent {
		if h, ok := m.(*messages.Heartbeat); ok && h.Type == messages.MsgTypeHeartbeatAck {
			found = true
		}
	}
	if !found {
		t.Errorf("no BEAT Ack was sent while the inbound DATA queue was full (sent %v)", typeNames(*sent))
	}

	// The overflow must have been reported rather than silently swallowed.
	select {
	case err := <-conn.errChan:
		if !errors.Is(err, ErrDataQueueFull) {
			t.Errorf("reported %v, want ErrDataQueueFull", err)
		}
	default:
		t.Error("the DATA queue overflowed without reporting it")
	}

	// Overflow must never be fatal: the association is healthy.
	if err := conn.handleErrors(ErrDataQueueFull); err != nil {
		t.Errorf("handleErrors(ErrDataQueueFull) = %v, want nil: congestion must not close the association", err)
	}
}

// Everything queued before the overflow must still be readable, and reading
// must let the queue accept payloads again.
func TestDataQueueRecoversAfterOverflow(t *testing.T) {
	conn, _ := newTestConn(t, StateAspActive, modeServer)
	// A legal arrival stream: Section 1.4.7's rule 1 refuses DATA on stream 0,
	// so without this the payloads below never reach the handler under test.
	conn.recvStream.Store(1)

	for i := 0; i < 64; i++ {
		conn.handleData(context.Background(), messages.NewData(
			nil, params.NewRoutingContext(1),
			params.NewProtocolData(0x11111111, 0x22222222, params.ServiceIndSCCP, 0, 0, 1, []byte("queued")),
			nil,
		))
	}
	drained := 0
	for len(conn.dataChan) > 0 {
		<-conn.dataChan
		drained++
	}
	if drained != cap(conn.dataChan) {
		t.Errorf("drained %d payloads, want the queue's full %d", drained, cap(conn.dataChan))
	}

	conn.handleData(context.Background(), messages.NewData(
		nil, params.NewRoutingContext(1),
		params.NewProtocolData(0x11111111, 0x22222222, params.ServiceIndSCCP, 0, 0, 1, []byte("after")),
		nil,
	))
	if len(conn.dataChan) != 1 {
		t.Errorf("queue holds %d after draining and one more payload, want 1", len(conn.dataChan))
	}
}

// RFC 4666 Section 3.4.4: "The SCON message MAY also be sent from the M3UA
// layer of an ASP to an M3UA peer, indicating that the congestion level of the
// M3UA layer or the ASP has changed."
//
// The inbound DATA queue overflowing is exactly that condition, and it was
// reported only locally: the peer carried on sending at full rate into a queue
// this node was discarding from. SCON is the protocol's own way to ask it to
// back off.
func TestLocalCongestionTellsThePeerWithSCON(t *testing.T) {
	conn, sent := newTestConn(t, StateAspActive, modeServer)
	// A legal arrival stream: Section 1.4.7's rule 1 refuses DATA on stream 0,
	// so without this the payloads below never reach the handler under test.
	conn.recvStream.Store(1)
	conn.cfg.OriginatingPointCode = 0x11111111

	// newTestConn's dataChan holds 8; overflow it.
	for i := 0; i < 64; i++ {
		conn.handleData(context.Background(), messages.NewData(
			nil, params.NewRoutingContext(1),
			params.NewProtocolData(0x11111111, 0x22222222, params.ServiceIndSCCP, 0, 0, 1, []byte("flood")),
			nil,
		))
	}

	// The overflow is reported to monitor, which turns it into the wire message.
	select {
	case err := <-conn.errChan:
		if e := conn.handleErrors(err); e != nil {
			t.Fatalf("handleErrors(%v) = %v, want nil: congestion must not close the association", err, e)
		}
	default:
		t.Fatal("the overflow was not reported")
	}

	var scon *messages.SignallingCongestion
	for _, m := range *sent {
		if s, ok := m.(*messages.SignallingCongestion); ok {
			scon = s
		}
	}
	if scon == nil {
		t.Fatalf("no SCON was sent on local congestion (sent %v)", typeNames(*sent))
	}
	// Affected Point Code is Mandatory (Section 3.4.4), and the congested node
	// is this one.
	if scon.AffectedPointCode == nil {
		t.Fatal("SCON carried no Affected Point Code, which the RFC makes Mandatory")
	}
	// The Affected PC field is 24 bits with an 8-bit Mask above it (Section
	// 3.4.1), so the configured 0x11111111 cannot be carried whole: what goes
	// on the wire is its low 24 bits, under a Mask of 0 naming this one node.
	if got := scon.AffectedPointCode.AffectedPointCodes(); len(got) != 1 || got[0] != 0x111111 {
		t.Errorf("SCON named point codes %#v, want [0x111111] (this node's)", got)
	}
	if got := scon.AffectedPointCode.AffectedPointCodeMasks(); len(got) != 1 || got[0] != 0 {
		t.Errorf("SCON Mask = %v, want [0]: one node, not a range", got)
	}
}

// The report is once per episode, not once per discarded payload: a sustained
// overflow must not turn into an SCON flood on an association already in
// trouble.
func TestLocalCongestionSCONIsSentOncePerEpisode(t *testing.T) {
	conn, sent := newTestConn(t, StateAspActive, modeServer)
	// A legal arrival stream: Section 1.4.7's rule 1 refuses DATA on stream 0,
	// so without this the payloads below never reach the handler under test.
	conn.recvStream.Store(1)
	conn.cfg.OriginatingPointCode = 0x11111111

	drain := func() {
		for {
			select {
			case err := <-conn.errChan:
				_ = conn.handleErrors(err)
			default:
				return
			}
		}
	}

	for i := 0; i < 200; i++ {
		conn.handleData(context.Background(), messages.NewData(
			nil, params.NewRoutingContext(1),
			params.NewProtocolData(0x11111111, 0x22222222, params.ServiceIndSCCP, 0, 0, 1, []byte("flood")),
			nil,
		))
		drain()
	}

	count := 0
	for _, m := range *sent {
		if _, ok := m.(*messages.SignallingCongestion); ok {
			count++
		}
	}
	if count != 1 {
		t.Errorf("sent %d SCONs for one overflow episode, want 1", count)
	}
}
