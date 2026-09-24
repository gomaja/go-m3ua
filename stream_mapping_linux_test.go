// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

// RFC 4666 Section 1.4.7 assigns M3UA messages to SCTP streams:
//
//  1. The DATA message MUST NOT be sent on stream 0.
//  2. The ASPSM, MGMT, RKM classes SHOULD be sent on stream 0 (other than
//     BEAT, BEAT ACK and NTFY messages).
//  3. The SSNM, ASPTM classes and BEAT, BEAT ACK and NTFY messages can be sent
//     on any stream.
//
// The tests below read the stream every message arrives on at a peer built
// directly on SCTP, so what they check is the stream the kernel carried the
// message on, not the one the library meant to choose.

// streamArrival is one M3UA message as a raw SCTP peer received it.
type streamArrival struct {
	class, kind uint8
	name        string
	stream      uint16
	ppid        uint32
	// errorCode is an ERR's Error Code, zero for any other message.
	errorCode uint32
}

// streamRecorder collects every message a raw peer reads, in arrival order.
type streamRecorder struct {
	mu       sync.Mutex
	arrivals []streamArrival
}

func (r *streamRecorder) record(message messages.M3UA, info *sctp.SndRcvInfo) {
	arrival := streamArrival{
		class: message.MessageClass(),
		kind:  message.MessageType(),
		name:  message.MessageClassName() + " " + message.MessageTypeName(),
	}
	if e, ok := message.(*messages.Error); ok && e.ErrorCode != nil {
		arrival.errorCode = e.ErrorCode.ErrorCode()
	}
	if info != nil {
		arrival.stream, arrival.ppid = info.Stream, info.PPID
	} else {
		arrival.stream = ^uint16(0)
	}
	r.mu.Lock()
	r.arrivals = append(r.arrivals, arrival)
	r.mu.Unlock()
}

func (r *streamRecorder) snapshot() []streamArrival {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]streamArrival(nil), r.arrivals...)
}

// errors is the Error Code of every ERR that has arrived.
func (r *streamRecorder) errors() []uint32 {
	var codes []uint32
	for _, arrival := range r.snapshot() {
		if arrival.class == messages.MsgClassManagement && arrival.kind == messages.MsgTypeError {
			codes = append(codes, arrival.errorCode)
		}
	}
	return codes
}

// count is how many messages of the class and type have arrived.
func (r *streamRecorder) count(class, kind uint8) int {
	n := 0
	for _, arrival := range r.snapshot() {
		if arrival.class == class && arrival.kind == kind {
			n++
		}
	}
	return n
}

// requireSection147 checks every recorded arrival against RFC 4666 Section
// 1.4.7 and requires each of want to have arrived at least once, so a message
// that never arrived cannot pass by being absent.
func requireSection147(t *testing.T, recorder *streamRecorder, want ...[2]uint8) {
	t.Helper()
	arrivals := recorder.snapshot()
	for _, arrival := range arrivals {
		if arrival.ppid != M3UAPPID {
			t.Errorf("%s arrived with PPID %d; want %d", arrival.name, arrival.ppid, M3UAPPID)
		}
		switch {
		case arrival.class == messages.MsgClassTransfer:
			if arrival.stream == 0 {
				t.Errorf("%s arrived on stream 0; RFC 4666 Section 1.4.7 rule 1 forbids it", arrival.name)
			}
		case streamZeroClass(arrival.class, arrival.kind):
			if arrival.stream != 0 {
				t.Errorf("%s arrived on stream %d; RFC 4666 Section 1.4.7 rule 2 puts it on stream 0", arrival.name, arrival.stream)
			}
		default:
			t.Logf("%s arrived on stream %d (rule 3: any stream)", arrival.name, arrival.stream)
		}
	}
	for _, message := range want {
		found := false
		for _, arrival := range arrivals {
			if arrival.class == message[0] && arrival.kind == message[1] {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("no message of class %d type %d arrived", message[0], message[1])
		}
	}
}

// streamZeroClass reports whether RFC 4666 Section 1.4.7 rule 2 puts the
// message on stream 0.
func streamZeroClass(class, kind uint8) bool {
	switch class {
	case messages.MsgClassASPSM:
		return kind != messages.MsgTypeHeartbeat && kind != messages.MsgTypeHeartbeatAck
	case messages.MsgClassManagement:
		return kind != messages.MsgTypeNotify
	case messages.MsgClassRKM:
		return true
	}
	return false
}

// A library SGP answers ASPSM and reports errors on stream 0 and never sends
// DATA there. It also holds its peer to rule 2: an ASP Up on another stream is
// refused with "Invalid Stream Identifier" (RFC 4666 Section 3.8.1).
func TestSGPSendsEachMessageClassOnItsSection147Stream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const port = 3293
	key := ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true}
	config := NewAssociationConfig().SetApplicationServers(ASConfig{ASKey: key, TrafficMode: params.TrafficModeLoadshare})
	config.HeartbeatInfo = &HeartbeatInfo{Enabled: false}
	address := &sctp.SCTPAddr{IPAddrs: []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}}, Port: port}

	sgp, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sgp.Close() }()
	listener, err := sgp.Listen("m3ua", address, NewListenerConfig(config))
	if err != nil {
		if isSCTPUnsupported(err) {
			t.Skipf("skipping socket-backed test: %v", err)
		}
		t.Fatal(err)
	}
	accepted := make(chan *Association, 1)
	go func() {
		if association, err := listener.Accept(ctx); err == nil {
			accepted <- association
		}
	}()

	conn, err := (&sctp.SocketConfig{InitMsg: sctp.InitMsg{NumOstreams: 16, MaxInstreams: 16}}).Dial("sctp", nil, address)
	if err != nil {
		t.Fatalf("raw ASP dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.SetRecvRcvInfo(true); err != nil {
		t.Fatal(err)
	}
	recorder := &streamRecorder{}
	go func() {
		buf := make([]byte, 65535)
		for {
			n, info, err := conn.SCTPRead(buf)
			if err != nil {
				return
			}
			if message, err := messages.Parse(buf[:n]); err == nil {
				recorder.record(message, info)
			}
		}
	}()
	send := func(b []byte, stream uint16) {
		t.Helper()
		if _, err := conn.SCTPWrite(b, &sctp.SndRcvInfo{PPID: M3UAPPID, Stream: stream}); err != nil {
			t.Fatalf("raw ASP send: %v", err)
		}
	}
	sendMessage := func(message messages.M3UA, stream uint16) {
		t.Helper()
		b, err := message.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		send(b, stream)
	}
	await := func(class, kind uint8, count int) {
		t.Helper()
		if !waitFor(func() bool { return recorder.count(class, kind) >= count }, 10*time.Second) {
			t.Fatalf("never received %d messages of class %d type %d; got %v", count, class, kind, recorder.snapshot())
		}
	}

	// ASP Up on stream 3 breaks rule 2 and is refused.
	sendMessage(messages.NewAspUp(nil, nil), 3)
	await(messages.MsgClassManagement, messages.MsgTypeError, 1)
	if codes := recorder.errors(); codes[0] != params.ErrInvalidStreamIdentifier {
		t.Fatalf("ASP Up on stream 3 drew Error Code %#x; want Invalid Stream Identifier %#x", codes[0], params.ErrInvalidStreamIdentifier)
	}
	if n := recorder.count(messages.MsgClassASPSM, messages.MsgTypeAspUpAck); n != 0 {
		t.Fatalf("ASP Up on stream 3 was acknowledged %d times", n)
	}
	sendMessage(messages.NewAspUp(nil, nil), 0)
	await(messages.MsgClassASPSM, messages.MsgTypeAspUpAck, 1)
	// Rule 3 lets ASPTM use any stream, and the library chooses its reply's
	// stream itself rather than echoing the request's.
	sendMessage(messages.NewAspActive(params.NewTrafficModeType(params.TrafficModeLoadshare), params.NewRoutingContext(1), nil), 3)
	await(messages.MsgClassASPTM, messages.MsgTypeAspActiveAck, 1)
	var association *Association
	select {
	case association = <-accepted:
	case <-time.After(10 * time.Second):
		t.Fatal("the SGP never accepted the association")
	}

	// A message of reserved class 5 draws an ERR (RFC 4666 Section 3.8.1,
	// "Unsupported Message Class"): a MGMT message the library originates.
	send([]byte{1, 0, 5, 1, 0, 0, 0, 8}, 0)
	await(messages.MsgClassManagement, messages.MsgTypeError, 2)
	if codes := recorder.errors(); codes[1] != params.UnsupportedMessageErrorClass {
		t.Fatalf("a reserved message class drew Error Code %#x; want Unsupported Message Class %#x", codes[1], params.UnsupportedMessageErrorClass)
	}

	// DATA on every Signalling Link Selection of the association's streams.
	const selections = 16
	payload := make([]byte, 8)
	for sls := 0; sls < selections; sls++ {
		binary.BigEndian.PutUint64(payload, uint64(sls))
		if _, err := association.WriteData(DataRequest{AS: key, ProtocolData: params.ProtocolDataPayload{
			OriginatingPointCode: 1, DestinationPointCode: 2, ServiceIndicator: 3, NetworkIndicator: 2,
			SignallingLinkSelection: uint8(sls), Data: payload}}); err != nil {
			t.Fatalf("DATA with SLS %d: %v", sls, err)
		}
	}
	await(messages.MsgClassTransfer, messages.MsgTypePayloadData, selections)

	sendMessage(messages.NewAspDown(nil), 0)
	await(messages.MsgClassASPSM, messages.MsgTypeAspDownAck, 1)

	requireSection147(t, recorder,
		[2]uint8{messages.MsgClassASPSM, messages.MsgTypeAspUpAck},
		[2]uint8{messages.MsgClassASPSM, messages.MsgTypeAspDownAck},
		[2]uint8{messages.MsgClassManagement, messages.MsgTypeError},
		[2]uint8{messages.MsgClassTransfer, messages.MsgTypePayloadData})
}

// A library ASP brings its association up and down on stream 0 and never
// sends DATA there.
func TestASPSendsEachMessageClassOnItsSection147Stream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const port = 3294
	addr, err := sctp.ResolveSCTPAddr("sctp", fmt.Sprintf("127.0.0.2:%d", port))
	if err != nil {
		t.Fatal(err)
	}
	ln, err := (&sctp.SocketConfig{InitMsg: sctp.InitMsg{NumOstreams: sctpStreams, MaxInstreams: sctpStreams}}).Listen("sctp", addr)
	if err != nil {
		if isSCTPUnsupported(err) {
			t.Skipf("skipping socket-backed test: %v", err)
		}
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()

	// The peer answers every request the ASP makes, on stream 0, and records
	// everything it reads.
	recorder := &streamRecorder{}
	go func() {
		conn, err := ln.AcceptSCTP()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		if err := conn.SetRecvRcvInfo(true); err != nil {
			t.Errorf("peer SCTP_RECVRCVINFO: %v", err)
			return
		}
		control := &sctp.SndRcvInfo{PPID: M3UAPPID, Stream: 0}
		buf := make([]byte, 65535)
		for {
			n, info, err := conn.SCTPRead(buf)
			if err != nil {
				return
			}
			message, err := messages.Parse(buf[:n])
			if err != nil {
				continue
			}
			recorder.record(message, info)
			var reply messages.M3UA
			switch message.(type) {
			case *messages.AspUp:
				reply = messages.NewAspUpAck(nil, nil)
			case *messages.AspActive:
				reply = messages.NewAspActiveAck(
					params.NewTrafficModeType(params.TrafficModeLoadshare),
					params.NewRoutingContext(1, 2), nil)
			case *messages.AspInactive:
				reply = messages.NewAspInactiveAck(params.NewRoutingContext(1, 2), nil)
			case *messages.AspDown:
				reply = messages.NewAspDownAck(nil)
			}
			if reply == nil {
				continue
			}
			b, err := reply.MarshalBinary()
			if err != nil {
				return
			}
			if _, err := conn.SCTPWrite(b, control); err != nil {
				return
			}
		}
	}()

	laddr, err := sctp.ResolveSCTPAddr("sctp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatal(err)
	}
	cfg := newASPAssociationConfigForTest(&HeartbeatInfo{Enabled: false}, 1, params.TrafficModeLoadshare, 0, []uint32{1, 2})
	association, err := dialASP(ctx, "m3ua", laddr, addr, cfg)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = association.Close() }()

	const selections = 16
	key := ASKey{NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true}
	payload := make([]byte, 8)
	for sls := 0; sls < selections; sls++ {
		binary.BigEndian.PutUint64(payload, uint64(sls))
		if _, err := association.WriteData(DataRequest{AS: key, ProtocolData: params.ProtocolDataPayload{
			OriginatingPointCode: 2, DestinationPointCode: 1, ServiceIndicator: 3, NetworkIndicator: 2,
			SignallingLinkSelection: uint8(sls), Data: payload}}); err != nil {
			t.Fatalf("DATA with SLS %d: %v", sls, err)
		}
	}
	if !waitFor(func() bool {
		return recorder.count(messages.MsgClassTransfer, messages.MsgTypePayloadData) >= selections
	}, 10*time.Second) {
		t.Fatalf("the peer never received %d DATA; got %v", selections, recorder.snapshot())
	}
	if err := association.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if !waitFor(func() bool { return recorder.count(messages.MsgClassASPSM, messages.MsgTypeAspDown) >= 1 }, 10*time.Second) {
		t.Fatalf("the peer never received ASP Down; got %v", recorder.snapshot())
	}

	requireSection147(t, recorder,
		[2]uint8{messages.MsgClassASPSM, messages.MsgTypeAspUp},
		[2]uint8{messages.MsgClassASPSM, messages.MsgTypeAspDown},
		[2]uint8{messages.MsgClassTransfer, messages.MsgTypePayloadData})
}
