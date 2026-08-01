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

// Conn implements net.Conn, so Write carries io.Writer's contract: on success it
// must return len(b) and nil. It returned neither. The M3UA message wrapping the
// payload was marshalled, SCTPWrite returned how many bytes it had put on the
// wire, and that count was then *added to itself*:
//
//	n, err = c.sctpConn.SCTPWrite(d, &info)
//	n += len(d)
//
// so the caller got roughly twice the encoded message length — a number larger
// than the buffer it passed in, and unrelated to it. Every wrapper that trusts
// the contract is broken by that: io.Copy treats n > len(b) as an invalid write,
// and a short-write check (n != len(b)) fires on every successful send.
func TestWriteReturnsThePayloadLength(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peer := newRawPeer(t, 3070, handshakeOnly)
	conn := dialRawPeer(t, ctx, peer, 3070, &HeartbeatInfo{Enabled: false})

	for _, payload := range [][]byte{
		[]byte("x"),
		[]byte("hello world"),
		make([]byte, 1024),
	} {
		t.Run(fmt.Sprintf("%d bytes", len(payload)), func(t *testing.T) {
			n, err := conn.Write(payload)
			if err != nil {
				t.Fatalf("Write: %v", err)
			}
			if n != len(payload) {
				t.Errorf("Write returned n = %d for a %d-byte payload, want %d (io.Writer's contract)",
					n, len(payload), len(payload))
			}
		})
	}
}

// WriteToStream carries the same contract.
func TestWriteToStreamReturnsThePayloadLength(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peer := newRawPeer(t, 3072, handshakeOnly)
	conn := dialRawPeer(t, ctx, peer, 3072, &HeartbeatInfo{Enabled: false})

	payload := []byte("stream-bound payload")
	// Stream 1, not 0: RFC 4666 Section 1.4.7 forbids DATA on stream 0.
	n, err := conn.WriteToStream(payload, 1)
	if err != nil {
		t.Fatalf("WriteToStream: %v", err)
	}
	if n != len(payload) {
		t.Errorf("WriteToStream returned n = %d, want %d", n, len(payload))
	}
}

// WritePD takes the Protocol Data parameter rather than a payload, so its
// natural count is the user octets it carried — the SS7 payload inside the
// Protocol Data — and it must never exceed them.
func TestWritePDReturnsTheUserPayloadLength(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peer := newRawPeer(t, 3074, handshakeOnly)
	conn := dialRawPeer(t, ctx, peer, 3074, &HeartbeatInfo{Enabled: false})

	payload := []byte("protocol data payload")
	pd := params.NewProtocolData(0x11111111, 0x22222222, 3, 0, 0, 1, payload)

	n, err := conn.WritePD(pd)
	if err != nil {
		t.Fatalf("WritePD: %v", err)
	}
	if n != len(payload) {
		t.Errorf("WritePD returned n = %d, want %d (the user octets carried)", n, len(payload))
	}
}

// WriteSignal is not an io.Writer — it takes a message, not a buffer — so its
// count is the encoded message length. It must still be that length once, not
// twice.
func TestWriteSignalReturnsTheEncodedLength(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peer := newRawPeer(t, 3078, handshakeOnly)
	conn := dialRawPeer(t, ctx, peer, 3078, &HeartbeatInfo{Enabled: false})

	beat := messages.NewHeartbeat(params.NewHeartbeatData([]byte("are you alive")))
	want := beat.MarshalLen()

	n, err := conn.WriteSignal(beat)
	if err != nil {
		t.Fatalf("WriteSignal: %v", err)
	}
	if n != want {
		t.Errorf("WriteSignal returned n = %d, want %d (the encoded message length, counted once)", n, want)
	}
}

// A large payload must be reported truthfully too: the doubling was
// proportional, so a small-payload-only test would understate it.
func TestWriteOfLargePayloadReportsItsOwnLength(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peer := newRawPeer(t, 3076, handshakeOnly)
	conn := dialRawPeer(t, ctx, peer, 3076, &HeartbeatInfo{Enabled: false})

	payload := make([]byte, 8000)
	for i := range payload {
		payload[i] = byte(i)
	}
	n, err := conn.Write(payload)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n != len(payload) {
		t.Errorf("Write returned n = %d for %d bytes, want %d", n, len(payload), len(payload))
	}

	// And it must actually have gone out, so the count is not satisfied by a
	// write that did nothing.
	if !waitFor(func() bool { return peer.count("Payload Data") > 0 }, 5*time.Second) {
		t.Error("the peer never received the DATA message")
	}
}

// Deliberate behaviour, pinned so a change to it is noticed.
//
// Sends pass MSG_DONTWAIT, so with no write deadline in force a full send
// buffer reports syscall.EAGAIN rather than blocking. The dependency documents
// why that stays: a blocking sendmsg to a peer that has stopped reading does
// not come back for many minutes, bounded by the retransmission backoff rather
// than by anything the caller can set, and there is no way to interrupt it.
// Reporting EAGAIN keeps the descriptor under the caller's control.
//
// The remedy is a write deadline, which is no longer inert — see
// TestWriteDeadlineTurnsAFullBufferIntoBackpressure. This test covers the
// no-deadline path, where the association survives the refusal and recovers
// once the far side drains.
func TestWriteReportsEAGAINWhenTheSendBufferIsFull(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const port = 3092
	ln := mcListen(t, mcAddr(port, "127.0.0.1"))
	asps := mcConnect(t, ctx, ln, mcAddr(port, "127.0.0.1"), []string{"127.0.0.2"}, port)

	// Nobody reads on the far side, so the send buffer fills.
	payload := make([]byte, 512)
	sent := 0
	var failure error
	for i := 0; i < 20000; i++ {
		if _, err := asps[0].client.WriteToStream(payload, 1); err != nil {
			failure = err
			break
		}
		sent++
	}

	if failure == nil {
		t.Fatalf("Write accepted %d messages without ever reporting a full send buffer. "+
			"If Write now blocks or waits for writability, this test and the writeRetrying "+
			"helper in ordering_test.go are obsolete and should be removed.", sent)
	}
	if !errors.Is(failure, syscall.EAGAIN) {
		t.Fatalf("after %d writes the failure was %v, want syscall.EAGAIN", sent, failure)
	}
	t.Logf("send buffer filled after %d messages of %d bytes", sent, len(payload))

	// Congestion must not be mistaken for a broken association.
	if got := asps[0].client.State(); got != StateAspActive {
		t.Errorf("state = %v after a full send buffer, want %v: congestion tore the association down",
			got, StateAspActive)
	}

	// And it must recover once the far side drains.
	for i := 0; i < sent; i++ {
		if _, err := readWithin(t, asps[0].server, 5*time.Second); err != nil {
			break
		}
	}
	if !waitFor(func() bool {
		_, err := asps[0].client.WriteToStream(payload, 1)
		return err == nil
	}, 10*time.Second) {
		t.Error("Write never recovered after the far side drained")
	}
}

// A write deadline now buys what net.Conn promises: carry on until the message
// is accepted, or until the deadline.
//
// Sends still pass MSG_DONTWAIT, so a full send buffer with no deadline set
// reports EAGAIN exactly as before — see the test above. What changed in the
// dependency is that SetWriteDeadline is no longer inert: with one in force the
// send waits for buffer space rather than refusing, so a burst larger than the
// send buffer becomes backpressure instead of a write failure. That is the
// remedy for the congestion the test above pins.
func TestWriteDeadlineTurnsAFullBufferIntoBackpressure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	const port = 3094
	ln := mcListen(t, mcAddr(port, "127.0.0.1"))
	asps := mcConnect(t, ctx, ln, mcAddr(port, "127.0.0.1"), []string{"127.0.0.2"}, port)

	// Drain concurrently, so the buffer refills and empties under the writer.
	drained := make(chan int, 1)
	go func() {
		n := 0
		for {
			if _, err := readWithin(t, asps[0].server, 3*time.Second); err != nil {
				break
			}
			n++
		}
		drained <- n
	}()

	// Far more than the ~330 messages that filled the buffer without a
	// deadline.
	if err := asps[0].client.SetWriteDeadline(time.Now().Add(30 * time.Second)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}
	payload := make([]byte, 512)
	const burst = 3000
	for i := 0; i < burst; i++ {
		if _, err := asps[0].client.WriteToStream(payload, 1); err != nil {
			t.Fatalf("write %d of %d failed with a deadline in force: %v "+
				"(a full send buffer should have been waited out, not refused)", i, burst, err)
		}
	}

	got := <-drained
	if got < burst/2 {
		t.Errorf("only %d of %d payloads arrived; the burst did not actually flow", got, burst)
	}
}
