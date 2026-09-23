// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

// An accepted association must negotiate as many outbound streams as a dialled
// one. The listening socket used to ask for nothing, so the kernel default of
// 10 applied and an accepting endpoint had nine DATA streams to spread
// Signalling Link Selections over while the dialling end had many more.
func TestAcceptedAssociationNegotiatesAsManyStreamsAsADialledOne(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const port = 3271
	key := ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true}
	config := func() *AssociationConfig {
		c := NewAssociationConfig().SetApplicationServers(ASConfig{ASKey: key, TrafficMode: params.TrafficModeLoadshare})
		c.HeartbeatInfo = &HeartbeatInfo{Enabled: false}
		return c
	}
	address := &sctp.SCTPAddr{IPAddrs: []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}}, Port: port}

	sgp, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sgp.Close() }()
	asp, err := NewEndpoint(EndpointConfig{Role: RoleASP})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = asp.Close() }()

	listener, err := sgp.Listen("m3ua", address, NewListenerConfig(config()))
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
	dialled, err := asp.Dial(ctx, "m3ua", nil, address, config())
	if err != nil {
		t.Fatal(err)
	}
	var acceptedAssociation *Association
	select {
	case acceptedAssociation = <-accepted:
	case <-time.After(10 * time.Second):
		t.Fatal("the SGP never accepted the association")
	}

	// Both ends ask for sctpStreams in each direction, so each can use
	// streams 1 to 256 for DATA: one per Signalling Link Selection.
	const want = sctpStreams - 1
	if got := dialled.MaxMessageStreamID(); got != want {
		t.Errorf("the dialled association can use DATA streams up to %d; want %d", got, want)
	}
	if got := acceptedAssociation.MaxMessageStreamID(); got != want {
		t.Errorf("the accepted association can use DATA streams up to %d; want %d", got, want)
	}
}
