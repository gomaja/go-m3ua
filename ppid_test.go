// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

func marshalPPIDTestMessage(t *testing.T, message messages.M3UA) []byte {
	t.Helper()
	raw, err := message.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal %s: %v", message.MessageTypeName(), err)
	}
	return raw
}

func TestM3UAPPIDIsExportedAndUsedForSends(t *testing.T) {
	if M3UAPPID != 3 {
		t.Fatalf("M3UAPPID = %d, want the RFC-assigned value 3", M3UAPPID)
	}

	conn := newAssociation(RoleASP, NewAssociationConfig(1, 2, params.ServiceIndSCCP, 0, 0, 1))
	if got := conn.sctpInfo.PPID; got != M3UAPPID {
		t.Errorf("send template PPID = %d, want M3UAPPID (%d)", got, M3UAPPID)
	}
}

// gomaja/sctp already converts SndRcvInfo.PPID to host order. The M3UA read
// path must preserve that value exactly rather than byte-swapping it again or
// dropping it while extracting the stream.
func TestInboundMessageCarriesHostOrderPPID(t *testing.T) {
	info := &sctp.SndRcvInfo{Stream: 7, PPID: 0x01020304}
	event := newInboundMessage([]byte("message"), info)
	if event.kind != inboundMessage || event.stream != 7 || event.ppid != 0x01020304 {
		t.Errorf("inbound metadata = {kind:%d stream:%d ppid:%#x}, want {%d 7 %#x}",
			event.kind, event.stream, event.ppid, inboundMessage, uint32(0x01020304))
	}

	unspecified := newInboundMessage([]byte("message"), nil)
	if unspecified.stream != 0 || unspecified.ppid != 0 {
		t.Errorf("nil SndRcvInfo metadata = {stream:%d ppid:%d}, want zero values",
			unspecified.stream, unspecified.ppid)
	}
}

// End to end, ReadMsg's PPID must reach dispatchRaw. Sending two Heartbeats in
// order on one SCTP stream makes the negative assertion deterministic: once
// the PPID-3 Heartbeat has been acknowledged, an incorrectly accepted PPID-4
// Heartbeat before it would already have produced its own acknowledgement.
func TestReadLoopCarriesPPIDToTheDispatcherOverSocket(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const port = 3240
	peer := newRawPeer(t, port, handshakeOnly)
	conn := dialRawPeer(t, ctx, peer, port, &HeartbeatInfo{Enabled: false})
	conn.lastRecv.Store(1)

	peer.mu.Lock()
	rawConn := peer.conn
	peer.mu.Unlock()
	if rawConn == nil {
		t.Fatal("peer never accepted an association")
	}

	invalidData := []byte("wrong upper-layer protocol")
	validData := []byte("M3UA")
	for _, send := range []struct {
		ppid uint32
		data []byte
	}{
		{ppid: 4, data: invalidData},
		{ppid: M3UAPPID, data: validData},
	} {
		message := marshalPPIDTestMessage(t,
			messages.NewHeartbeat(params.NewHeartbeatData(send.data)))
		if _, err := rawConn.SCTPWrite(message, &sctp.SndRcvInfo{
			PPID: send.ppid, Stream: 0,
		}); err != nil {
			t.Fatalf("peer write with PPID %d: %v", send.ppid, err)
		}
	}

	if !waitFor(func() bool { return peerSawHeartbeatAck(peer, validData) }, 5*time.Second) {
		t.Fatal("PPID-3 Heartbeat was not acknowledged")
	}
	if peerSawHeartbeatAck(peer, invalidData) {
		t.Error("PPID-4 Heartbeat was acknowledged instead of silently discarded")
	}
	if got := conn.lastRecv.Load(); got == 1 {
		t.Error("accepted PPID-3 Heartbeat did not refresh peer liveness")
	}
}

func peerSawHeartbeatAck(peer *rawPeer, data []byte) bool {
	peer.mu.Lock()
	defer peer.mu.Unlock()

	for _, message := range peer.received {
		ack, ok := message.(*messages.HeartbeatAck)
		if ok && ack.HeartbeatData != nil && bytes.Equal(ack.HeartbeatData.HeartbeatData(), data) {
			return true
		}
	}
	return false
}

// RFC 4666 Section 7.1 registers PPID 3 for M3UA and explicitly permits 0 as
// unspecified. Both values must reach the ordinary parser and handler rather
// than being mistaken for another adaptation layer.
func TestReceiveAcceptsM3UAAndUnspecifiedPPID(t *testing.T) {
	raw := marshalPPIDTestMessage(t,
		messages.NewHeartbeat(params.NewHeartbeatData([]byte("alive"))))

	for _, ppid := range []uint32{0, 3} {
		t.Run(fmt.Sprintf("PPID-%d", ppid), func(t *testing.T) {
			conn, sent := newTestConn(t, StateASPActive, RoleSGP)
			conn.lastRecv.Store(1)

			conn.dispatchRaw(context.Background(), inbound{
				kind: inboundMessage, data: raw, stream: 0, ppid: ppid,
			})

			if got := conn.lastRecv.Load(); got == 1 {
				t.Errorf("PPID %d did not count as peer activity", ppid)
			}
			if len(*sent) != 1 {
				t.Errorf("PPID %d produced signals %v, want one Heartbeat Ack", ppid, typeNames(*sent))
			} else if beat, ok := (*sent)[0].(*messages.HeartbeatAck); !ok ||
				beat.MessageType() != messages.MsgTypeHeartbeatAck {
				t.Errorf("PPID %d produced %T type %d, want Heartbeat Ack type %d",
					ppid, (*sent)[0], (*sent)[0].MessageType(), messages.MsgTypeHeartbeatAck)
			}
			select {
			case state := <-conn.stateChan:
				if state != stateUnchanged {
					t.Errorf("PPID %d published state %v, want unchanged", ppid, state)
				}
			default:
				t.Errorf("PPID %d message never reached the state dispatcher", ppid)
			}
			if err := firstErr(conn); err != nil {
				t.Errorf("PPID %d reported unexpected error: %v", ppid, err)
			}
		})
	}
}

// Every PPID other than 0 and 3 belongs to another upper-layer protocol. It is
// silently discarded: no ERR is reflected across protocol boundaries, no ASP
// state changes, and a payload is not delivered to the MTP3-User.
func TestReceiveSilentlyDiscardsEveryOtherPPID(t *testing.T) {
	heartbeat := marshalPPIDTestMessage(t,
		messages.NewHeartbeat(params.NewHeartbeatData([]byte("not M3UA"))))
	data := marshalPPIDTestMessage(t, messages.NewData(
		nil,
		params.NewRoutingContext(1),
		params.NewProtocolData(0x111111, 0x222222, params.ServiceIndSCCP, 0, 0, 1, []byte("payload")),
		nil,
	))

	for _, ppid := range []uint32{1, 2, 4, 0xffffffff} {
		t.Run(fmt.Sprintf("PPID-%d", ppid), func(t *testing.T) {
			conn, sent := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
			conn.lastRecv.Store(1)
			conn.recvStream.Store(9)

			conn.dispatchRaw(context.Background(), inbound{
				kind: inboundMessage, data: heartbeat, stream: 0, ppid: ppid,
			})
			conn.dispatchRaw(context.Background(), inbound{
				kind: inboundMessage, data: data, stream: 1, ppid: ppid,
			})

			if got := conn.lastRecv.Load(); got != 1 {
				t.Errorf("PPID %d refreshed peer liveness", ppid)
			}
			if got := conn.receivedStreamID(); got != 9 {
				t.Errorf("PPID %d changed receive stream to %d", ppid, got)
			}
			if got := typeNames(*sent); len(got) != 0 {
				t.Errorf("PPID %d produced signals %v", ppid, got)
			}
			if err := firstErr(conn); err != nil {
				t.Errorf("PPID %d produced ERR input: %v", ppid, err)
			}
			select {
			case state := <-conn.stateChan:
				t.Errorf("PPID %d published state %v", ppid, state)
			default:
			}
			select {
			case delivered := <-conn.dataChan:
				t.Errorf("PPID %d delivered DATA %+v to the application", ppid, delivered)
			default:
			}
			select {
			case indication := <-conn.ManagementIndications():
				t.Errorf("PPID %d produced management indication %+v", ppid, indication)
			default:
			}
		})
	}
}

// T(beat) measures valid M3UA messages, not arbitrary octets received on the
// association. A malformed frame must not keep a dead or hostile peer alive,
// including when its readable header still requires an ERR response.
func TestMalformedM3UABytesDoNotRefreshPeerLiveness(t *testing.T) {
	for _, ppid := range []uint32{0, 3} {
		for _, test := range []struct {
			name      string
			raw       []byte
			wantError bool
		}{
			{name: "too short for header", raw: []byte{0x01, 0x00}},
			{
				name: "supported header with invalid length",
				raw: []byte{
					0x01, 0x00, messages.MsgClassASPSM, messages.MsgTypeHeartbeat,
					0x00, 0x00, 0x00, 0x07,
				},
				wantError: true,
			},
		} {
			t.Run(fmt.Sprintf("PPID-%d/%s", ppid, test.name), func(t *testing.T) {
				conn, _ := newTestConn(t, StateASPActive, RoleSGP)
				conn.lastRecv.Store(1)

				conn.dispatchRaw(context.Background(), inbound{
					kind: inboundMessage, data: test.raw, stream: 0, ppid: ppid,
				})

				if got := conn.lastRecv.Load(); got != 1 {
					t.Errorf("malformed bytes refreshed peer liveness to %d", got)
				}
				if got := firstErr(conn); (got != nil) != test.wantError {
					t.Errorf("reported error = %v, wantError=%v", got, test.wantError)
				}
			})
		}
	}
}

// Parse represents a well-formed but unsupported class or type as Generic.
// It is still a valid M3UA message and therefore counts as the "any other M3UA
// message" that RFC 4666 Section 4.3.4.6 treats as proof of life, even though
// the protocol response is an Unsupported Class/Type ERR.
func TestValidGenericUnsupportedMessageRefreshesPeerLiveness(t *testing.T) {
	for _, test := range []struct {
		name  string
		class uint8
		type_ uint8
	}{
		{name: "unsupported class", class: 0xfe, type_: 1},
		{name: "unsupported type", class: messages.MsgClassASPSM, type_: 0xfe},
	} {
		t.Run(test.name, func(t *testing.T) {
			raw := marshalPPIDTestMessage(t, messages.New(1, test.class, test.type_))
			if parsed, err := messages.Parse(raw); err != nil {
				t.Fatalf("Generic did not parse: %v", err)
			} else if _, ok := parsed.(*messages.Generic); !ok {
				t.Fatalf("parsed as %T, want *messages.Generic", parsed)
			}

			conn, _ := newTestConn(t, StateASPActive, RoleSGP)
			conn.lastRecv.Store(1)
			conn.dispatchRaw(context.Background(), inbound{
				kind: inboundMessage, data: raw, stream: 0, ppid: 3,
			})

			if got := conn.lastRecv.Load(); got == 1 {
				t.Error("valid Generic message did not refresh peer liveness")
			}
			if err := firstErr(conn); err == nil {
				t.Error("unsupported Generic message produced no ERR input")
			}
		})
	}
}
