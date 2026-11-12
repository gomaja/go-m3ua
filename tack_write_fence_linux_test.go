// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

func TestTAckRetryWithdrawalFenceOverSCTP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	listener, err := sctp.Listen("sctp4", mcAddr(0, "127.0.0.1"))
	if err != nil {
		skipIfSCTPUnsupported(t, err)
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	accepted := make(chan *sctp.Conn, 1)
	acceptError := make(chan error, 1)
	go func() {
		peer, e := listener.AcceptSCTP()
		if e != nil {
			acceptError <- e
		} else {
			accepted <- peer
		}
	}()
	client, err := sctp.Dial(ctx, "sctp4", mcAddr(0, "127.0.0.1"), listener.Addr().(*sctp.Addr))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	var peer *sctp.Conn
	select {
	case peer = <-accepted:
	case err = <-acceptError:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	t.Cleanup(func() { _ = peer.Close() })
	deadline, _ := ctx.Deadline()
	if err := client.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	if err := peer.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	read := func(conn *sctp.Conn) messages.M3UA {
		t.Helper()
		raw, _, err := conn.ReadMsg(65536)
		if err != nil {
			t.Fatal(err)
		}
		message, err := messages.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		return message
	}
	sendPeer := func(message messages.M3UA) {
		t.Helper()
		raw, err := message.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		if _, err = peer.SendMsg(raw, sctp.SendOptions{Info: &sctp.SndInfo{PPID: M3UAPPID}}); err != nil {
			t.Fatal(err)
		}
	}
	a, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
	a.noteRoutingContextsAcked(params.NewRoutingContext(1))
	a.cfg.TAck = 5 * time.Millisecond
	a.cfg.TAckRetries = 1
	a.recvStream.Store(0)
	a.signalWriter = nil
	a.controlTransport = client
	a.transportCloser = client.Close
	active := messages.NewAspActive(nil, params.NewRoutingContext(2), nil)
	if _, err := a.writeControl(active); err != nil {
		t.Fatal(err)
	}
	if _, ok := read(peer).(*messages.AspActive); !ok {
		t.Fatal("peer did not receive initial ASPAC")
	}
	inactive := messages.NewAspInactive(params.NewRoutingContext(2), nil)
	inactiveRequest := a.startTAck(inactive, requestAspInactive)
	inactiveRequest.stopRetries()
	if _, err := a.writeControl(inactive); err != nil {
		t.Fatal(err)
	}
	if _, ok := read(peer).(*messages.AspInactive); !ok {
		t.Fatal("peer did not receive ASPIA")
	}
	a.sendMu.Lock()
	_, entered, done := startObservedRetry(t, a, active, requestAspActive)
	select {
	case <-entered:
	case <-ctx.Done():
		a.sendMu.Unlock()
		t.Fatal(ctx.Err())
	}
	sendPeer(messages.NewAspInactiveAck(params.NewRoutingContext(2), nil))
	ack, ok := read(client).(*messages.AspInactiveAck)
	if !ok {
		a.sendMu.Unlock()
		t.Fatal("client did not receive ASPIA Ack")
	}
	if err := a.handleAspInactiveAck(ack); err != nil {
		a.sendMu.Unlock()
		t.Fatal(err)
	}
	a.sendMu.Unlock()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	// The retry has drained. A later BEAT must be the next wire message.
	if _, err := a.writeControl(messages.NewHeartbeat(nil)); err != nil {
		t.Fatal(err)
	}
	if _, ok := read(peer).(*messages.Heartbeat); !ok {
		t.Fatal("ASPAC crossed the completed withdrawal on the wire")
	}
	sendPeer(messages.NewHeartbeatAck(nil))
	if _, ok := read(client).(*messages.HeartbeatAck); !ok {
		t.Fatal("missing wire barrier Ack")
	}
}
