// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"net"
	"syscall"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

// Deliberate behaviour, pinned so a change to it is noticed.
//
// Sends pass MSG_DONTWAIT, so with no write deadline in force a full send
// buffer reports syscall.EAGAIN rather than blocking. The kernel queues a
// message whole or not at all, so the refusal is classified DataNotSent, and
// errors.Is still reaches EAGAIN. The dependency documents why that stays: a
// blocking sendmsg to a peer that has stopped reading does not come back for
// many minutes, bounded by the retransmission backoff rather than by anything
// the caller can set. Reporting EAGAIN keeps the descriptor under the caller's
// control. The remedy is a write deadline; see
// TestWriteDeadlineTurnsAFullBufferIntoBackpressure.
//
// The far side is a raw SCTP peer that stops reading at the socket. A library
// endpoint cannot stand in for it: its reader keeps draining the socket and
// discards what its full DATA queue cannot hold, so whether the sender ever saw
// a full buffer depended on which of the two ran faster.
func TestWriteReportsEAGAINWhenTheSendBufferIsFull(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	key := ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true}
	config := NewAssociationConfig().SetApplicationServers(ASConfig{ASKey: key, TrafficMode: params.TrafficModeLoadshare})
	config.HeartbeatInfo = &HeartbeatInfo{Enabled: false}
	address := &sctp.SCTPAddr{IPAddrs: []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}}, Port: 3092}
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

	request := DataRequest{AS: key, ProtocolData: params.ProtocolDataPayload{
		OriginatingPointCode: 1, DestinationPointCode: 2, ServiceIndicator: 3, NetworkIndicator: 2, Data: make([]byte, 512)}}
	sent := 0
	var failure error
	for failure == nil {
		if sent > 200000 {
			t.Fatalf("200,000 messages were accepted by a peer that reads nothing; the send buffer never filled")
		}
		if _, failure = association.WriteData(request); failure == nil {
			sent++
		}
	}
	if !errors.Is(failure, syscall.EAGAIN) {
		t.Fatalf("after %d writes the failure was %v, want syscall.EAGAIN", sent, failure)
	}
	requireDataWriteError(t, failure, DataNotSent, syscall.EAGAIN)
	t.Logf("send buffer filled after %d messages of %d bytes", sent, len(request.ProtocolData.Data))

	// Congestion must not be mistaken for a broken association.
	if got := association.State(); got != StateASPActive {
		t.Errorf("state = %v after a full send buffer, want %v: congestion tore the association down", got, StateASPActive)
	}

	// And it must recover once the far side drains.
	close(peer.resume)
	if !waitFor(func() bool {
		_, err := association.WriteData(request)
		return err == nil
	}, 10*time.Second) {
		t.Error("the send path never recovered after the far side drained")
	}
}
