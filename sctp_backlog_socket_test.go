// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

// SO_MEMINFO remains available without the optional sctp_diag module.
func TestSCTPSendBacklogTracksStalledPeerAndDrain(t *testing.T) {
	a, drain := stalledSCTPSender(t)
	// Separate live reads can straddle an acknowledgement. Retry the complete
	// sample rather than treating the first mismatch as a failure.
	for deadline := time.Now().Add(5 * time.Second); ; {
		status, statusErr := a.AssociationStatus()
		backlog, backlogErr := a.SCTPSendBacklog()
		limit, limitErr := a.SocketSendBuffer()
		if statusErr == nil && backlogErr == nil && limitErr == nil &&
			status != nil && backlog != nil && status.ReceiverWindow == 0 &&
			status.SendBacklog.Charged > 0 && status.SendBacklog == *backlog && backlog.Limit == limit {
			t.Logf("stalled SendBacklog=%+v SocketSendBuffer=%d", backlog, limit)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stalled memory sample did not agree: status=%+v error=%v backlog=%+v error=%v SocketSendBuffer=%d error=%v", status, statusErr, backlog, backlogErr, limit, limitErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
	drain()
	for deadline := time.Now().Add(5 * time.Second); ; {
		status, statusErr := a.AssociationStatus()
		backlog, backlogErr := a.SCTPSendBacklog()
		if statusErr == nil && backlogErr == nil && status != nil && backlog != nil &&
			status.SendBacklog.Charged == 0 && backlog.Charged == 0 {
			t.Logf("drained SendBacklog=%+v", backlog)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("send memory did not drain: status=%+v error=%v backlog=%+v error=%v", status, statusErr, backlog, backlogErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestSCTPAssocQueuesTracksStalledPeerAndDrain(t *testing.T) {
	a, drain := stalledSCTPSender(t)
	for deadline := time.Now().Add(5 * time.Second); ; {
		queues, queuesErr := a.SCTPAssocQueues()
		backlog, backlogErr := a.SCTPSendBacklog()
		if errors.Is(queuesErr, sctp.ErrUnsupported) {
			t.Skipf("SCTP sock_diag requires sctp_diag: %v", queuesErr)
		}
		if queuesErr == nil && backlogErr == nil && queues != nil && backlog != nil &&
			queues.SendCharged == backlog.Charged && queues.Unsent > 0 {
			t.Logf("stalled AssocQueues=%+v SendBacklog=%+v", queues, backlog)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stalled queue sample did not agree: queues=%+v error=%v backlog=%+v error=%v", queues, queuesErr, backlog, backlogErr)
		}
		time.Sleep(20 * time.Millisecond)
	}
	drain()
	for deadline := time.Now().Add(5 * time.Second); ; {
		queues, err := a.SCTPAssocQueues()
		if errors.Is(err, sctp.ErrUnsupported) {
			t.Skipf("SCTP sock_diag requires sctp_diag: %v", err)
		}
		if err == nil && queues != nil && queues.SendCharged == 0 && queues.Unsent == 0 {
			t.Logf("drained AssocQueues=%+v", queues)
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("association queues did not drain: queues=%+v error=%v", queues, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// stalledSCTPSender sends DATA until a peer that has stopped reading closes
// its receive window. The returned function drains every accepted DATA message.
// Neither setup nor drain queries sock_diag.
func stalledSCTPSender(t *testing.T) (*Association, func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	small, noDelay := 16384, true
	ln, err := (&sctp.Config{ReadBuffer: &small, NoDelay: &noDelay,
		InitMsg: sctp.InitMsg{OutStreams: 16, MaxInStreams: 16}}).Listen("sctp4", mcAddr(0, "127.0.0.1"))
	if err != nil {
		skipIfSCTPUnsupported(t, err)
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	type accepted struct {
		conn *sctp.Conn
		err  error
	}
	ready := make(chan accepted, 1)
	go func() {
		peer, e := ln.AcceptSCTP()
		if e == nil {
			e = acknowledgeBacklogHandshake(peer)
		}
		ready <- accepted{peer, e}
	}()
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	key := ASKey{NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true}
	cfg := NewAssociationConfig().SetApplicationServers(ASConfig{ASKey: key, TrafficMode: params.TrafficModeLoadshare})
	cfg.HeartbeatInfo = &HeartbeatInfo{Enabled: false}
	cfg.SocketSendBuffer = 65536
	a, dialErr := endpoint.Dial(ctx, "m3ua", mcAddr(0, "127.0.0.1"), ln.Addr().(*sctp.Addr), cfg)
	if dialErr != nil {
		_ = ln.Close()
	}
	var peer accepted
	select {
	case peer = <-ready:
	case <-ctx.Done():
		t.Fatalf("waiting for peer handshake: %v (dial: %v)", ctx.Err(), dialErr)
	}
	if peer.conn != nil {
		t.Cleanup(func() { _ = peer.conn.Close() })
	}
	if dialErr != nil || peer.err != nil {
		t.Fatalf("handshake: dial=%v peer=%v", dialErr, peer.err)
	}
	request := DataRequest{AS: key, Stream: 1, ProtocolData: params.ProtocolDataPayload{
		OriginatingPointCode: 0x111111, DestinationPointCode: 0x222222,
		ServiceIndicator: params.ServiceIndSCCP, SignallingLinkSelection: 1,
		Data: bytes.Repeat([]byte("S"), 4096)}}
	sent := 0
	for ; sent < 10000; sent++ {
		if err = a.SetWriteDeadline(time.Now().Add(300 * time.Millisecond)); err != nil {
			t.Fatal(err)
		}
		if _, err = a.WriteData(request); err != nil {
			if !errors.Is(err, os.ErrDeadlineExceeded) {
				t.Fatal(err)
			}
			break
		}
	}
	if sent == 10000 {
		t.Fatal("peer did not stall the sender")
	}
	if err = a.SetWriteDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}

	for deadline := time.Now().Add(5 * time.Second); ; {
		status, err := a.AssociationStatus()
		if err == nil && status != nil && status.ReceiverWindow == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("peer receive window did not close: status=%+v error=%v", status, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	return a, func() {
		t.Helper()
		if err := peer.conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < sent; i++ {
			b, _, readErr := peer.conn.ReadMsg(1 << 20)
			if readErr != nil {
				t.Fatal(readErr)
			}
			m, parseErr := messages.Parse(b)
			if parseErr != nil || m.MessageClass() != messages.MsgClassTransfer {
				t.Fatalf("drain DATA: message=%v error=%v", m, parseErr)
			}
		}
	}
}

func acknowledgeBacklogHandshake(peer *sctp.Conn) error {
	if err := peer.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	for _, step := range []struct {
		class, kind uint8
		reply       messages.M3UA
	}{
		{messages.MsgClassASPSM, messages.MsgTypeAspUp, messages.NewAspUpAck(nil, nil)},
		{messages.MsgClassASPTM, messages.MsgTypeAspActive, messages.NewAspActiveAck(params.NewTrafficModeType(params.TrafficModeLoadshare), params.NewRoutingContext(1), nil)},
	} {
		b, _, err := peer.ReadMsg(1 << 20)
		if err != nil {
			return err
		}
		m, err := messages.Parse(b)
		if err != nil {
			return err
		}
		if m.MessageClass() != step.class || m.MessageType() != step.kind {
			return fmt.Errorf("unexpected handshake message %d/%d", m.MessageClass(), m.MessageType())
		}
		b, err = step.reply.MarshalBinary()
		if err != nil {
			return err
		}
		if _, err = peer.SendMsg(b, sctp.SendOptions{Info: &sctp.SndInfo{PPID: M3UAPPID}}); err != nil {
			return err
		}
	}
	return nil
}
