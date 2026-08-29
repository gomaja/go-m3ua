// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

// RFC 4666 Section 1.4.7 constrains which SCTP stream each message class may
// travel on, and Section 3.8.1 gives the error for getting it wrong:
//
//	"The 'Invalid Stream Identifier' error is sent if a message is received on
//	an unexpected SCTP stream (e.g., a Management message was received on a
//	stream other than '0')."
//
// aspsm.go has carried that check for ASP Up, ASP Up Ack, ASP Down and ASP Down
// Ack all along — but it read Association.StreamID(), which is the *outbound* send
// template. That template is fixed at 0 for the life of the Association, because every
// send copies it by value before setting a stream, so the guard compared 0
// against 0 and could never fire. The arrival stream was discarded by the read
// loop before any handler could see it.

// The guard must fire on the stream the message actually arrived on.
func TestManagementMessageOffStreamZeroIsRejected(t *testing.T) {
	for _, tt := range []struct {
		name  string
		role  Role
		state State
		send  func(*Association) error
	}{
		// ASP Up and ASP Down travel ASP to SGP; the Acks travel back, so each
		// is checked in the role and state that legitimately receives it —
		// otherwise an earlier role or state check fires first and the stream
		// is never reached.
		{"ASP Up", RoleSGP, StateASPInactive, func(c *Association) error { return c.handleAspUp(messages.NewAspUp(nil, nil)) }},
		{"ASP Up Ack", RoleASP, StateASPInactive, func(c *Association) error { return c.handleAspUpAck(messages.NewAspUpAck(nil, nil)) }},
		{"ASP Down", RoleSGP, StateASPInactive, func(c *Association) error { return c.handleAspDown(messages.NewAspDown(nil)) }},
		{"ASP Down Ack", RoleASP, StateASPDown, func(c *Association) error { return c.handleAspDownAck(messages.NewAspDownAck(nil)) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conn, _ := newTestConn(t, tt.state, tt.role)

			// Arrived on stream 3: not permitted for ASPSM.
			conn.recvStream.Store(3)
			err := tt.send(conn)

			var streamErr *InvalidSCTPStreamIDError
			if !errors.As(err, &streamErr) {
				t.Fatalf("%s on stream 3: error = %v, want *InvalidSCTPStreamIDError", tt.name, err)
			}
			if streamErr.ID != 3 {
				t.Errorf("%s: reported stream %d, want 3", tt.name, streamErr.ID)
			}
		})
	}
}

// And it must not fire on stream 0, where these messages belong.
func TestManagementMessageOnStreamZeroIsAccepted(t *testing.T) {
	conn, _ := newTestConn(t, StateASPInactive, RoleSGP)
	conn.recvStream.Store(0)

	if err := conn.handleAspUp(messages.NewAspUp(nil, nil)); err != nil {
		t.Errorf("ASP Up on stream 0 was rejected: %v", err)
	}
}

// The send template must stay independent of what arrives: StreamID() is
// documented as the outbound stream and callers rely on it.
func TestArrivalStreamDoesNotDisturbTheSendTemplate(t *testing.T) {
	conn, _ := newTestConn(t, StateASPActive, RoleASP)

	before := conn.StreamID()
	conn.recvStream.Store(9)
	if got := conn.StreamID(); got != before {
		t.Errorf("StreamID() = %d after a message arrived on stream 9, want %d", got, before)
	}
	if got := conn.receivedStreamID(); got != 9 {
		t.Errorf("receivedStreamID() = %d, want 9", got)
	}
}

// End to end: the read loop must carry the arrival stream as far as the
// handlers, so a peer really can be caught putting ASPSM on a data stream.
func TestArrivalStreamReachesTheHandlersOverASocket(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peer := newRawPeer(t, 3120, handshakeOnly)
	conn := dialRawPeer(t, ctx, peer, 3120, &HeartbeatInfo{Enabled: false})

	// The peer sends an ASP Down Ack on a data stream, which RFC 4666 Section
	// 1.4.7 rule 2 puts on stream 0.
	b, err := messages.NewAspDownAck(nil).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	peer.mu.Lock()
	raw := peer.conn
	peer.mu.Unlock()
	if raw == nil {
		t.Fatal("peer never accepted an association")
	}
	if _, err := raw.SCTPWrite(b, &sctp.SndRcvInfo{PPID: 3, Stream: 4}); err != nil {
		t.Fatalf("peer write on stream 4: %v", err)
	}

	// The ASP must report the stream error rather than acting on the message.
	if !waitFor(func() bool { return conn.receivedStreamID() == 4 }, 5*time.Second) {
		t.Fatalf("the arrival stream never reached the handlers (saw %d, want 4)", conn.receivedStreamID())
	}
	// It must also still be ASP-ACTIVE: a message rejected for its stream must
	// not move the state machine.
	if got := conn.State(); got != StateASPActive {
		t.Errorf("state = %v after an ASP Down Ack on a data stream, want %v", got, StateASPActive)
	}
}

// A peer that keeps to the rules is unaffected: DATA arrives on a data stream
// and is delivered normally.
func TestDataOnADataStreamIsDeliveredNormally(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	const port = 3122
	ln := mcListen(t, mcAddr(port, "127.0.0.1"))
	asps := mcConnect(t, ctx, ln, mcAddr(port, "127.0.0.1"), []string{"127.0.0.2"}, port)

	pd := params.NewProtocolData(0x11111111, 0x22222222, params.ServiceIndSCCP, 0, 0, 1, []byte("on-a-data-stream"))
	if _, err := asps[0].asp.WritePD(pd); err != nil {
		t.Fatalf("WritePD: %v", err)
	}
	got, err := readWithin(t, asps[0].sgp, 5*time.Second)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got != "on-a-data-stream" {
		t.Errorf("read %q, want %q", got, "on-a-data-stream")
	}
}
