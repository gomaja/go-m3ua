// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

// A peer that asks for the SCTP maximum in both directions still gets
// sctpStreams each way: the library neither sends on more streams than M3UA can
// use nor lets the peer make this end preallocate state for 65,535 inbound
// streams.
func TestAcceptedAssociationCapsAGreedyPeersStreams(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const port = 3272
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
	peer := dialStalledASPWith(t, address, 1, sctp.InitMsg{NumOstreams: sctp.SCTP_MAX_STREAM, MaxInstreams: sctp.SCTP_MAX_STREAM})
	var association *Association
	select {
	case association = <-accepted:
	case <-time.After(10 * time.Second):
		t.Fatal("the SGP never accepted the association")
	}

	status, err := association.sctpConn.GetStatus()
	if err != nil {
		t.Fatal(err)
	}
	if status.Ostreams != sctpStreams || status.Instreams != sctpStreams {
		t.Fatalf("negotiated %d outbound and %d inbound streams with a peer asking for %d; want %d each",
			status.Ostreams, status.Instreams, sctp.SCTP_MAX_STREAM, sctpStreams)
	}
	if got := association.MaxMessageStreamID(); got != sctpStreams-1 {
		t.Fatalf("DATA streams reach %d; want %d", got, sctpStreams-1)
	}

	// The highest SLS maps to the last negotiated stream, and DATA sent there
	// reaches the peer on it.
	close(peer.resume)
	payload := make([]byte, 8)
	binary.BigEndian.PutUint64(payload, 255)
	if _, err := association.WriteData(DataRequest{AS: key, ProtocolData: params.ProtocolDataPayload{
		OriginatingPointCode: 1, DestinationPointCode: 2, ServiceIndicator: 3, NetworkIndicator: 2,
		SignallingLinkSelection: 255, Data: payload}}); err != nil {
		t.Fatal(err)
	}
	if !waitFor(func() bool { return len(peer.delivered()) == 1 }, 10*time.Second) {
		t.Fatal("the DATA on the last stream never reached the peer")
	}
	if streams := peer.deliveredStreams(); len(streams) != 1 || streams[0] != sctpStreams-1 {
		t.Fatalf("SLS 255 arrived on streams %v; want [%d]", streams, sctpStreams-1)
	}
}
