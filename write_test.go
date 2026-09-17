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

// A successful send reports the SS7 user octets it carried — the payload the
// caller handed over, not the size of the M3UA message it was wrapped in.
//
// The count used to be the encoded message length added to itself, so a caller
// was told roughly twice the encoded length: a number larger than the payload
// and unrelated to it. Every wrapper that trusts a byte count is broken by that,
// and a short-write check (n != len(payload)) fires on every successful send.
func TestWriteDataReturnsThePayloadLength(t *testing.T) {
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
			n, err := writePayload(conn, 1, payload)
			if err != nil {
				t.Fatalf("WriteData: %v", err)
			}
			if n != len(payload) {
				t.Errorf("WriteData returned n = %d for a %d-byte payload, want %d",
					n, len(payload), len(payload))
			}
		})
	}
}

// The same count on an explicitly selected stream: choosing the stream does not
// change what the return value means.
func TestWriteDataToAnExplicitStreamReturnsThePayloadLength(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peer := newRawPeer(t, 3072, handshakeOnly)
	conn := dialRawPeer(t, ctx, peer, 3072, &HeartbeatInfo{Enabled: false})

	payload := []byte("stream-bound payload")
	// Stream 1, not 0: RFC 4666 Section 1.4.7 forbids DATA on stream 0.
	n, err := writePayloadToStream(conn, 1, 1, payload)
	if err != nil {
		t.Fatalf("WriteData: %v", err)
	}
	if n != len(payload) {
		t.Errorf("WriteData returned n = %d, want %d", n, len(payload))
	}
}

// WriteSignal takes a message rather than a payload, so its count is the
// encoded message length. It must still be that length once, not twice.
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
	n, err := writePayload(conn, 1, payload)
	if err != nil {
		t.Fatalf("WriteData: %v", err)
	}
	if n != len(payload) {
		t.Errorf("WriteData returned n = %d for %d bytes, want %d", n, len(payload), len(payload))
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
// buffer reports syscall.EAGAIN rather than blocking. The send classifies it as
// indeterminate — submission to the transport had begun — and errors.Is still
// reaches EAGAIN through that classification. The dependency documents
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
		if _, err := writePayloadToStream(asps[0].asp, 1, 1, payload); err != nil {
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
	if got := asps[0].asp.State(); got != StateASPActive {
		t.Errorf("state = %v after a full send buffer, want %v: congestion tore the association down",
			got, StateASPActive)
	}

	// And it must recover once the far side drains.
	for i := 0; i < sent; i++ {
		if _, err := readWithin(t, asps[0].sgp, 5*time.Second); err != nil {
			break
		}
	}
	if !waitFor(func() bool {
		_, err := writePayloadToStream(asps[0].asp, 1, 1, payload)
		return err == nil
	}, 10*time.Second) {
		t.Error("the send path never recovered after the far side drained")
	}
}

// A write deadline buys what a deadline should: carry on until the message
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
			if _, err := readWithin(t, asps[0].sgp, 3*time.Second); err != nil {
				break
			}
			n++
		}
		drained <- n
	}()

	// Far more than the ~330 messages that filled the buffer without a
	// deadline.
	if err := asps[0].asp.SetWriteDeadline(time.Now().Add(30 * time.Second)); err != nil {
		t.Fatalf("SetWriteDeadline: %v", err)
	}
	payload := make([]byte, 512)
	const burst = 3000
	for i := 0; i < burst; i++ {
		if _, err := writePayloadToStream(asps[0].asp, 1, 1, payload); err != nil {
			t.Fatalf("write %d of %d failed with a deadline in force: %v "+
				"(a full send buffer should have been waited out, not refused)", i, burst, err)
		}
	}

	got := <-drained
	if got < burst/2 {
		t.Errorf("only %d of %d payloads arrived; the burst did not actually flow", got, burst)
	}
}
