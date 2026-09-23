// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

// These tests fill real SCTP send buffers, so they need Linux SCTP. Each
// proves on the wire what control_backpressure_test.go proves against a model
// of the transport.

// TestSSNMPublicationWaitsOutAFullSendBuffer is the reproduction from the
// defect report: an SGP whose send buffer DATA has filled publishes one
// destination state report through the Endpoint. The report used to meet
// EAGAIN in the mandatory-control worker, which closed the association.
//
// The ASP is a raw peer that stops reading after the handshake, so the buffer
// stays full until the test lets it drain: a library ASP would keep reading
// and let the report find space by chance.
func TestSSNMPublicationWaitsOutAFullSendBuffer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const port = 3261
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
	peer := dialStalledASP(t, address, 1)
	var association *Association
	select {
	case association = <-accepted:
	case <-time.After(10 * time.Second):
		t.Fatal("the SGP never accepted the association")
	}
	if err := association.sctpConn.SetWriteBuffer(stallingSocketBuffer); err != nil {
		t.Fatal(err)
	}

	payload := make([]byte, 4096)
	for sent := 0; ; sent++ {
		_, err = association.WriteData(DataRequest{AS: key, ProtocolData: params.ProtocolDataPayload{
			OriginatingPointCode: 1, DestinationPointCode: 2, ServiceIndicator: 3, NetworkIndicator: 2, Data: payload}})
		if err != nil {
			break
		}
		if sent > 100000 {
			t.Fatal("100,000 DATA were all accepted; the send buffer never filled")
		}
	}
	if !errors.Is(err, syscall.EAGAIN) {
		t.Fatalf("filling the send buffer stopped on %v; want EAGAIN", err)
	}

	const pointCode = 0x400001
	reported := make(chan error, 1)
	go func() {
		reported <- sgp.ReportDestinationAvailability(DestinationAvailabilityRequest{
			Scope: WireScope{
				NetworkAppearance: 7, NetworkAppearanceSet: true,
				RoutingContexts: []uint32{1}, RoutingContextSet: true,
			},
			Destinations: []PointCodeRange{{PointCode: pointCode}},
			Availability: DestinationUnavailable,
		})
	}()
	if !waitFor(func() bool { return libraryWriteWaiting(association) }, 5*time.Second) {
		select {
		case err := <-reported:
			t.Fatalf("the report returned %v without waiting for send-buffer space (association: %v)", err, association.Err())
		default:
		}
		t.Fatal("the report never waited for send-buffer space; the test did not reach its precondition")
	}
	select {
	case <-association.Done():
		t.Fatalf("the association ended while the report waited: %v", association.Err())
	default:
	}

	close(peer.resume)
	select {
	case err := <-reported:
		if err != nil {
			t.Fatalf("the destination report failed: %v (association: %v)", err, association.Err())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the destination report never completed after the peer drained")
	}
	if !waitFor(func() bool { return len(peer.unavailable()) > 0 }, 10*time.Second) {
		t.Fatal("the peer never received the destination report")
	}
	if duna := peer.unavailable()[0]; duna.stream != 0 || duna.ppid != M3UAPPID {
		t.Fatalf("the DUNA arrived on stream %d with PPID %d; want stream 0, PPID %d", duna.stream, duna.ppid, M3UAPPID)
	}
	select {
	case <-association.Done():
		t.Fatalf("the SGP association ended: %v", association.Err())
	case <-time.After(200 * time.Millisecond):
	}
}

// stalledASP is an ASP built directly on SCTP: it completes ASP Up and ASP
// Active against a library SGP and then reads nothing until resume is closed,
// after which it drains and records the DUNAs it receives and the sequence
// number in the first eight octets of every DATA payload.
type stalledASP struct {
	resume chan struct{}

	mu          sync.Mutex
	dunas       []stalledAck
	data        []uint64
	dataStreams []uint16
}

func dialStalledASP(t *testing.T, sgp *sctp.SCTPAddr, routingContext uint32) *stalledASP {
	t.Helper()
	return dialStalledASPWith(t, sgp, routingContext, sctp.InitMsg{NumOstreams: 16, MaxInstreams: 16})
}

// dialStalledASPWith is dialStalledASP with the INIT stream request chosen by
// the caller.
func dialStalledASPWith(t *testing.T, sgp *sctp.SCTPAddr, routingContext uint32, init sctp.InitMsg) *stalledASP {
	t.Helper()
	socket := &sctp.SocketConfig{
		InitMsg: init,
		Control: func(_, _ string, c syscall.RawConn) error {
			var setErr error
			if err := c.Control(func(fd uintptr) {
				setErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, stallingSocketBuffer)
			}); err != nil {
				return err
			}
			return setErr
		},
	}
	conn, err := socket.Dial("sctp", nil, sgp)
	if err != nil {
		t.Fatalf("raw ASP dial: %v", err)
	}
	peer := &stalledASP{resume: make(chan struct{})}
	stop := make(chan struct{})
	t.Cleanup(func() {
		close(stop)
		_ = conn.Close()
	})
	if err := conn.SetRecvRcvInfo(true); err != nil {
		t.Fatal(err)
	}
	control := &sctp.SndRcvInfo{PPID: M3UAPPID, Stream: 0}
	send := func(message messages.M3UA) {
		t.Helper()
		b, err := message.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := conn.SCTPWrite(b, control); err != nil {
			t.Fatalf("raw ASP send: %v", err)
		}
	}
	buf := make([]byte, 65535)
	await := func(match func(messages.M3UA) bool) {
		t.Helper()
		if err := conn.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
			t.Fatal(err)
		}
		for {
			n, _, err := conn.SCTPRead(buf)
			if err != nil {
				t.Fatalf("raw ASP handshake read: %v", err)
			}
			if message, err := messages.Parse(buf[:n]); err == nil && match(message) {
				return
			}
		}
	}
	send(messages.NewAspUp(nil, nil))
	await(func(m messages.M3UA) bool { _, ok := m.(*messages.AspUpAck); return ok })
	send(messages.NewAspActive(params.NewTrafficModeType(params.TrafficModeLoadshare), params.NewRoutingContext(routingContext), nil))
	await(func(m messages.M3UA) bool { _, ok := m.(*messages.AspActiveAck); return ok })
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}

	go func() {
		select {
		case <-peer.resume:
		case <-stop:
			return
		}
		for {
			n, info, err := conn.SCTPRead(buf)
			if err != nil {
				return
			}
			message, err := messages.Parse(buf[:n])
			if err != nil {
				continue
			}
			if data, ok := message.(*messages.Data); ok {
				if payload, err := data.ProtocolData.ProtocolData(); err == nil && len(payload.Data) >= 8 {
					peer.mu.Lock()
					peer.data = append(peer.data, binary.BigEndian.Uint64(payload.Data))
					if info != nil {
						peer.dataStreams = append(peer.dataStreams, info.Stream)
					}
					peer.mu.Unlock()
				}
				continue
			}
			if _, ok := message.(*messages.DestinationUnavailable); ok {
				received := stalledAck{}
				if info != nil {
					received.stream, received.ppid = info.Stream, info.PPID
				}
				peer.mu.Lock()
				peer.dunas = append(peer.dunas, received)
				peer.mu.Unlock()
			}
		}
	}()
	return peer
}

func (p *stalledASP) delivered() []uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]uint64(nil), p.data...)
}

// deliveredStreams is the stream each DATA in delivered arrived on.
func (p *stalledASP) deliveredStreams() []uint16 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]uint16(nil), p.dataStreams...)
}

func (p *stalledASP) unavailable() []stalledAck {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]stalledAck(nil), p.dunas...)
}

// stallingPeer is an SGP built directly on SCTP that completes the ASP
// handshake and then, on the first BEAT Ack it reads, stops reading until
// released. BEATs sent to the library while it is stopped make the library's
// BEAT Acks -- mandatory replies the library writes on its own behalf -- fill
// first this peer's receive window and then the library's send buffer.
type stallingPeer struct {
	t        *testing.T
	ln       *sctp.SCTPListener
	addr     *sctp.SCTPAddr
	accepted chan *sctp.SCTPConn
	paused   chan struct{}
	resume   chan struct{}
	stop     chan struct{}

	mu   sync.Mutex
	acks []stalledAck
}

type stalledAck struct {
	sequence uint32
	stream   uint16
	ppid     uint32
}

func newStallingPeer(t *testing.T, port int) *stallingPeer {
	t.Helper()
	addr, err := sctp.ResolveSCTPAddr("sctp", fmt.Sprintf("127.0.0.2:%d", port))
	if err != nil {
		t.Fatal(err)
	}
	// The receive buffer is fixed before listen, because the association's
	// advertised window is computed from the listening socket's buffer when the
	// association is set up; changing it after accept would not shrink the
	// window. Left to the kernel default, a host that raised
	// net.core.rmem_default absorbs the whole flood and nothing ever waits.
	socket := &sctp.SocketConfig{
		InitMsg: sctp.InitMsg{NumOstreams: sctpStreams, MaxInstreams: sctpStreams},
		Control: func(_, _ string, c syscall.RawConn) error {
			var setErr error
			if err := c.Control(func(fd uintptr) {
				setErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, stallingSocketBuffer)
			}); err != nil {
				return err
			}
			return setErr
		},
	}
	ln, err := socket.Listen("sctp", addr)
	if err != nil {
		if isSCTPUnsupported(err) {
			t.Skipf("skipping socket-backed test: %v", err)
		}
		t.Fatal(err)
	}
	p := &stallingPeer{
		t: t, ln: ln, addr: addr,
		accepted: make(chan *sctp.SCTPConn, 1),
		paused:   make(chan struct{}),
		resume:   make(chan struct{}),
		stop:     make(chan struct{}),
	}
	t.Cleanup(func() {
		close(p.stop)
		_ = ln.Close()
	})
	go p.serve()
	return p
}

func (p *stallingPeer) serve() {
	conn, err := p.ln.AcceptSCTP()
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()
	// The stream and PPID of each BEAT Ack are what the test checks.
	if err := conn.SetRecvRcvInfo(true); err != nil {
		p.t.Errorf("peer SCTP_RECVRCVINFO: %v", err)
		return
	}
	p.accepted <- conn

	control := &sctp.SndRcvInfo{PPID: M3UAPPID, Stream: 0}
	buf := make([]byte, 65535)
	paused := false
	for {
		n, info, err := conn.SCTPRead(buf)
		if err != nil {
			return
		}
		message, err := messages.Parse(buf[:n])
		if err != nil {
			continue
		}
		var reply messages.M3UA
		switch m := message.(type) {
		case *messages.AspUp:
			reply = messages.NewAspUpAck(nil, nil)
		case *messages.AspActive:
			reply = messages.NewAspActiveAck(
				params.NewTrafficModeType(params.TrafficModeLoadshare),
				params.NewRoutingContext(1, 2), nil)
		case *messages.HeartbeatAck:
			ack := stalledAck{sequence: binary.BigEndian.Uint32(m.HeartbeatData.HeartbeatData())}
			if info != nil {
				ack.stream, ack.ppid = info.Stream, info.PPID
			}
			p.mu.Lock()
			p.acks = append(p.acks, ack)
			p.mu.Unlock()
			if !paused {
				paused = true
				close(p.paused)
				select {
				case <-p.resume:
				case <-p.stop:
					return
				}
			}
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
}

func (p *stallingPeer) receivedAcks() []stalledAck {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]stalledAck(nil), p.acks...)
}

// stallingBeat builds a BEAT whose Heartbeat Data carries sequence in its first four
// octets and is padded to size, so its Ack is as large as the BEAT itself.
func stallingBeat(t *testing.T, sequence uint32, size int) []byte {
	t.Helper()
	data := make([]byte, size)
	binary.BigEndian.PutUint32(data, sequence)
	b, err := messages.NewHeartbeat(params.NewHeartbeatData(data)).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

const (
	stallingBeatSize = 8192
	// stallingSocketBuffer sizes both the peer's receive buffer and the
	// library's send buffer, so that how much the stall absorbs does not
	// depend on the host's socket defaults. It stays above the 64 KiB loopback
	// MTU: a smaller receive window never grows by a full MTU, so the receiver
	// withholds window updates (silly window avoidance) and the drain after the
	// stall crawls along at one delayed SACK at a time.
	stallingSocketBuffer = 128 << 10
)

// stallLibraryReplies dials a library ASP to a stalling peer, stops the peer
// reading, and starts flooding the library with BEATs. It returns once the
// peer has stopped, with the flood still running in the background.
func stallLibraryReplies(t *testing.T, port int, controlWriteTimeout time.Duration) (*stallingPeer, *Association, int, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	peer := newStallingPeer(t, port)
	laddr, err := sctp.ResolveSCTPAddr("sctp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatal(err)
	}
	cfg := newASPAssociationConfigForTest(&HeartbeatInfo{Enabled: false}, 1, params.TrafficModeLoadshare, 0, []uint32{1, 2})
	cfg.ControlWriteTimeout = controlWriteTimeout
	association, err := dialASP(ctx, "m3ua", laddr, peer.addr, cfg)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = association.Close() })

	var conn *sctp.SCTPConn
	select {
	case conn = <-peer.accepted:
	case <-time.After(5 * time.Second):
		t.Fatal("the peer never accepted the association")
	}
	if err := association.sctpConn.SetWriteBuffer(stallingSocketBuffer); err != nil {
		t.Fatal(err)
	}
	receive, err := conn.GetReadBuffer()
	if err != nil {
		t.Fatal(err)
	}
	send, err := association.sctpConn.GetWriteBuffer()
	if err != nil {
		t.Fatal(err)
	}
	// Four times what the two buffers can hold between them, as the kernel
	// actually sized them, so the library's Acks must wait.
	count := 4 * (receive + send) / stallingBeatSize
	control := &sctp.SndRcvInfo{PPID: M3UAPPID, Stream: 0}
	// The first Ack stops the peer reading.
	if _, err := conn.SCTPWrite(stallingBeat(t, 0, stallingBeatSize), control); err != nil {
		t.Fatal(err)
	}
	select {
	case <-peer.paused:
	case <-time.After(5 * time.Second):
		t.Fatal("the library never answered the first BEAT")
	}

	// The flood waits for space itself, because the library stops reading
	// while its own reply is waiting.
	if err := conn.SetWriteDeadline(time.Now().Add(30 * time.Second)); err != nil {
		t.Fatal(err)
	}
	beats := make([][]byte, 0, count)
	for sequence := uint32(1); sequence <= uint32(count); sequence++ {
		beats = append(beats, stallingBeat(t, sequence, stallingBeatSize))
	}
	flood := make(chan error, 1)
	go func() {
		for _, beat := range beats {
			if _, err := conn.SCTPWrite(beat, control); err != nil {
				flood <- err
				return
			}
		}
		flood <- nil
	}()
	return peer, association, count, flood
}

// TestLibraryRepliesWaitForAStalledPeerToResume holds a peer's receive window
// shut long enough for the library's BEAT Acks to fill its send buffer, then
// lets the peer read again. Every BEAT is answered once, in order, on stream 0
// with the M3UA PPID -- including the Acks that could only be sent by waiting,
// which leave through the socket's default send parameters rather than
// ancillary data -- and the association is never closed.
func TestLibraryRepliesWaitForAStalledPeerToResume(t *testing.T) {
	peer, association, count, flood := stallLibraryReplies(t, 3262, time.Minute)

	// Before the fix the first refused Ack closed the association here.
	select {
	case <-association.Done():
		t.Fatalf("the library closed the association while its peer was stalled: %v", association.Err())
	case <-time.After(500 * time.Millisecond):
	}
	// The stream and PPID checks below only mean something if some Acks left
	// through the waiting path.
	if !waitFor(func() bool { return libraryWriteWaiting(association) }, 5*time.Second) {
		t.Fatal("no library write waited for send-buffer space; the stall did not fill it")
	}
	close(peer.resume)

	select {
	case err := <-flood:
		if err != nil {
			t.Fatalf("the BEAT flood failed: %v (association: %v)", err, association.Err())
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the BEAT flood never completed")
	}
	if !waitFor(func() bool { return len(peer.receivedAcks()) == count+1 }, 10*time.Second) {
		t.Fatalf("the peer received %d BEAT Acks; want %d", len(peer.receivedAcks()), count+1)
	}
	for index, ack := range peer.receivedAcks() {
		if ack.sequence != uint32(index) {
			t.Fatalf("BEAT Ack %d echoes BEAT %d; want every BEAT answered once, in order", index, ack.sequence)
		}
		if ack.stream != 0 || ack.ppid != M3UAPPID {
			t.Fatalf("BEAT Ack %d arrived on stream %d with PPID %d; want stream 0, PPID %d",
				index, ack.stream, ack.ppid, M3UAPPID)
		}
	}
	select {
	case <-association.Done():
		t.Fatalf("the association ended: %v", association.Err())
	default:
	}
}

// TestStalledPeerClosesTheAssociationAfterControlWriteTimeout never lets the
// peer read again: the library waits for the configured bound and then closes
// the association with ErrControlWriteTimeout, not before and not with the
// transport's EAGAIN.
func TestStalledPeerClosesTheAssociationAfterControlWriteTimeout(t *testing.T) {
	const bound = 400 * time.Millisecond
	_, association, _, _ := stallLibraryReplies(t, 3263, bound)
	stalled := time.Now()

	select {
	case <-association.Done():
	case <-time.After(10 * time.Second):
		t.Fatal("the association outlived its control write timeout")
	}
	if elapsed := time.Since(stalled); elapsed < bound {
		t.Fatalf("the association closed %v after the peer stalled, before the %v bound", elapsed, bound)
	}
	if !errors.Is(association.Err(), ErrControlWriteTimeout) {
		t.Fatalf("the association ended with %v; want ErrControlWriteTimeout", association.Err())
	}
}

// TestCloseReleasesAWaitingLibraryWrite closes an association while a reply is
// waiting for send-buffer space. Close must not wait for the bound, and the
// waiting write must not outlive the association.
func TestCloseReleasesAWaitingLibraryWrite(t *testing.T) {
	_, association, _, _ := stallLibraryReplies(t, 3264, time.Minute)

	if !waitFor(func() bool { return libraryWriteWaiting(association) }, 5*time.Second) {
		t.Fatal("no library write ever waited for send-buffer space; the test did not reach its precondition")
	}
	select {
	case <-association.Done():
		t.Fatalf("the association ended before Close: %v", association.Err())
	default:
	}
	closed := make(chan error, 1)
	go func() { closed <- association.Close() }()
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("Close did not return while a library write was waiting")
	}
	if !errors.Is(association.Err(), ErrAssociationClosed) {
		t.Fatalf("the association ended with %v; want ErrAssociationClosed", association.Err())
	}
	if !waitFor(func() bool { return !libraryWriteWaiting(association) }, 5*time.Second) {
		t.Fatalf("a library write is still waiting after Close:\n%s", goroutinesBlockedIn("writeControlFrame"))
	}
}

// libraryWriteWaiting reports a library write on association waiting for
// send-buffer space right now.
func libraryWriteWaiting(association *Association) bool {
	return association.controlWritesWaiting.Load() > 0
}
