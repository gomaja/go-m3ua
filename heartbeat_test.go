// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// RFC 4666 Section 4.3.4.6 defines peer liveness over every message, not just
// the Ack:
//
//	"If no Heartbeat Ack message (or any other M3UA message) is received from
//	the M3UA peer within 2*T(beat), the remote M3UA peer is considered
//	unavailable."
//
// heartbeat() waited only on beatAckChan, so the parenthetical was ignored: an
// association carrying continuous traffic in both directions was declared dead
// the moment a single BEAT Ack went missing, and torn down. On a busy link that
// is a spurious outage, and the busier the link the likelier it is — a BEAT Ack
// is one small message competing with everything else for the same association.

// A peer that answers everything except BEAT is alive, and must be treated as
// alive for as long as it keeps talking.
func TestPeerTalkingWithoutBeatAcksIsNotDeclaredDead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Answers ASP Up and ASP Active so the ASP establishes, then answers
	// every BEAT with a Notify instead of a BEAT Ack: plenty of evidence of
	// life, none of it an Ack.
	peer := newRawPeer(t, 3130, func(msg messages.M3UA) messages.M3UA {
		switch msg.(type) {
		case *messages.AspUp:
			return messages.NewAspUpAck(nil, nil)
		case *messages.AspActive:
			return messages.NewAspActiveAck(
				params.NewTrafficModeType(params.TrafficModeLoadshare),
				params.NewRoutingContext(1, 2), nil)
		case *messages.Heartbeat:
			return messages.NewNotify(params.NewStatus(params.AsStateActive), nil, nil, nil)
		default:
			return nil
		}
	})

	conn := dialRawPeer(t, ctx, peer, 3130, &HeartbeatInfo{
		Enabled:  true,
		Interval: 100 * time.Millisecond,
		Timer:    200 * time.Millisecond,
	})

	// Well past several T(beat) periods. The peer never acks a BEAT, but it
	// answers every one of them, so it is plainly reachable.
	time.Sleep(2 * time.Second)

	if got := conn.State(); got != StateASPActive {
		t.Fatalf("state = %v after 2s of unanswered BEATs on a talking peer, want %v: "+
			"liveness ignored every message that was not a BEAT Ack", got, StateASPActive)
	}
	if got := peer.count("Heartbeat"); got < 2 {
		t.Errorf("peer saw %d BEATs; the heartbeat was not running and the check above proves nothing", got)
	}
}

// The silent-peer case must still be detected — that is what T(beat) is for.
// Without this, the fix above is satisfied by never expiring at all.
func TestSilentPeerIsStillDeclaredDead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Completes the handshake, then says nothing at all.
	peer := newRawPeer(t, 3132, handshakeOnly)
	conn := dialRawPeer(t, ctx, peer, 3132, &HeartbeatInfo{
		Enabled:  true,
		Interval: 100 * time.Millisecond,
		Timer:    200 * time.Millisecond,
	})

	if !waitFor(func() bool { return conn.State() != StateASPActive }, 10*time.Second) {
		t.Fatalf("state is still %v against a peer that stopped answering entirely", conn.State())
	}
}

// A HeartbeatInfo with no Timer is not a request for a zero deadline. The RFC
// derives the deadline from the interval — "within 2*T(beat)" — and time.After(0)
// fires immediately, so enabling BEATs without a Timer tore the association down
// on the first round.
func TestHeartbeatTimerDefaultsToTwiceTheInterval(t *testing.T) {
	for _, tt := range []struct {
		name     string
		interval time.Duration
		timer    time.Duration
		want     time.Duration
	}{
		{"timer unset", 3 * time.Second, 0, 6 * time.Second},
		{"timer set", 3 * time.Second, time.Second, time.Second},
		{"both unset leaves BEATs off", 0, 0, 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := newASPAssociationConfigForTest(
				&HeartbeatInfo{Enabled: true, Interval: tt.interval, Timer: tt.timer},
				0x11111111, 0x22222222, 1, params.TrafficModeLoadshare, 0, 0,
				[]uint32{1}, params.ServiceIndSCCP, 0, 0, 1,
			)
			conn := newAssociation(RoleASP, cfg)

			if got := conn.hb.Timer; got != tt.want {
				t.Errorf("resolved T(beat) deadline = %v, want %v", got, tt.want)
			}
			if tt.interval == 0 && conn.hb.Enabled {
				t.Error("BEATs are enabled with no interval; heartbeat() would spin")
			}
		})
	}
}

// RFC 4666 Sections 3 and 3.5.6 permit extension parameters on BEAT and require
// the corresponding BEAT Ack to return every received parameter unchanged.
func TestHeartbeatAckPreservesExtensionParametersFromWire(t *testing.T) {
	heartbeat := messages.NewHeartbeat(params.NewHeartbeatData([]byte("heartbeat-data")))
	heartbeat.Others = []*params.Param{params.NewAspIdentifier(0x11223344)}
	wire, err := heartbeat.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal BEAT: %v", err)
	}
	parsed, err := messages.Parse(wire)
	if err != nil {
		t.Fatalf("parse BEAT: %v", err)
	}
	parsedHeartbeat, ok := parsed.(*messages.Heartbeat)
	if !ok {
		t.Fatalf("parsed message = %T, want *messages.Heartbeat", parsed)
	}

	association, sent := newTestConn(t, StateASPActive, RoleIPSP)
	if err := association.handleHeartbeat(parsedHeartbeat); err != nil {
		t.Fatalf("handleHeartbeat(): %v", err)
	}
	if len(*sent) != 1 {
		t.Fatalf("responses = %d, want 1 BEAT Ack", len(*sent))
	}
	ack, ok := (*sent)[0].(*messages.HeartbeatAck)
	if !ok {
		t.Fatalf("response = %T, want *messages.HeartbeatAck", (*sent)[0])
	}
	ackWire, err := ack.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal BEAT Ack: %v", err)
	}
	parsedAckMessage, err := messages.Parse(ackWire)
	if err != nil {
		t.Fatalf("parse BEAT Ack: %v", err)
	}
	parsedAck, ok := parsedAckMessage.(*messages.HeartbeatAck)
	if !ok {
		t.Fatalf("parsed response = %T, want *messages.HeartbeatAck", parsedAckMessage)
	}
	if len(parsedAck.Others) != 1 {
		t.Fatalf("BEAT Ack extension parameters = %d, want 1", len(parsedAck.Others))
	}
	got := parsedAck.Others[0]
	want := parsedHeartbeat.Others[0]
	if got.Tag != want.Tag || !bytes.Equal(got.Data, want.Data) {
		t.Fatalf("BEAT Ack extension parameter = {tag: %#x, data: %x}, want {tag: %#x, data: %x}",
			got.Tag, got.Data, want.Tag, want.Data)
	}
}
