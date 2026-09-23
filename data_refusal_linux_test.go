// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

// dialLoopbackSGP returns an SGP association to an ASP whose application never
// reads DATA, so DATA from the SGP outruns the path and fills its send buffer.
func dialLoopbackSGP(t *testing.T, port int) (*Association, ASKey) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	key := ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true}
	config := func() *AssociationConfig {
		c := NewAssociationConfig().SetApplicationServers(ASConfig{ASKey: key, TrafficMode: params.TrafficModeLoadshare})
		c.HeartbeatInfo = &HeartbeatInfo{Enabled: false}
		return c
	}
	address := &sctp.SCTPAddr{IPAddrs: []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}}, Port: port}

	asp, err := NewEndpoint(EndpointConfig{Role: RoleASP})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = asp.Close() })
	sgp, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sgp.Close() })

	listener, err := asp.Listen("m3ua", address, NewListenerConfig(config()))
	if err != nil {
		if isSCTPUnsupported(err) {
			t.Skipf("skipping socket-backed test: %v", err)
		}
		t.Fatal(err)
	}
	accepted := make(chan struct{})
	go func() {
		if _, err := listener.Accept(ctx); err == nil {
			close(accepted)
		}
	}()
	association, err := sgp.Dial(ctx, "m3ua", nil, address, config())
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-accepted:
	case <-time.After(10 * time.Second):
		t.Fatal("the ASP never accepted the association")
	}
	return association, key
}

// floodUntilRefused writes 4,096-octet DATA until a write fails and returns
// that failure.
func floodUntilRefused(t *testing.T, association *Association, key ASKey) error {
	t.Helper()
	payload := make([]byte, 4096)
	for sent := 0; sent <= 100000; sent++ {
		_, err := association.WriteData(DataRequest{AS: key, ProtocolData: params.ProtocolDataPayload{
			OriginatingPointCode: 1, DestinationPointCode: 2, ServiceIndicator: 3, NetworkIndicator: 2, Data: payload}})
		if err != nil {
			return err
		}
	}
	t.Fatal("100,000 DATA were all accepted; the send buffer never filled")
	return nil
}

// A send the kernel refuses for a full send buffer leaves nothing queued, so
// it is not sent, and the application may resend it.
func TestFullSendBufferReportsDataAsNotSent(t *testing.T) {
	association, key := dialLoopbackSGP(t, 3265)
	err := floodUntilRefused(t, association, key)
	requireDataWriteError(t, err, DataNotSent, syscall.EAGAIN)
}

// With a write deadline the send waits for space instead, and a deadline that
// expires first leaves the message just as unsent.
func TestExpiredWriteDeadlineReportsDataAsNotSent(t *testing.T) {
	association, key := dialLoopbackSGP(t, 3266)
	if err := association.SetWriteDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	err := floodUntilRefused(t, association, key)
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("the flood stopped on %v; want the expired write deadline", err)
	}
	requireDataWriteError(t, err, DataNotSent, os.ErrDeadlineExceeded)
}

// TestRefusedDataNeverReachesThePeer proves the property DataNotSent promises
// on the wire rather than by classification: an ASP that stops reading holds
// the SGP's send buffer full, the SGP sends numbered DATA until the transport
// refuses it, and once the ASP drains, every accepted message has arrived
// exactly once and no refused one has arrived at all.
func TestRefusedDataNeverReachesThePeer(t *testing.T) {
	for _, variant := range []struct {
		name     string
		port     int
		deadline time.Duration
		cause    error
	}{
		{"full send buffer", 3267, 0, syscall.EAGAIN},
		{"expired write deadline", 3268, 200 * time.Millisecond, os.ErrDeadlineExceeded},
	} {
		t.Run(variant.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			key := ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true}
			config := NewAssociationConfig().SetApplicationServers(ASConfig{ASKey: key, TrafficMode: params.TrafficModeLoadshare})
			config.HeartbeatInfo = &HeartbeatInfo{Enabled: false}
			address := &sctp.SCTPAddr{IPAddrs: []net.IPAddr{{IP: net.ParseIP("127.0.0.1")}}, Port: variant.port}
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
			if variant.deadline > 0 {
				if err := association.SetWriteDeadline(time.Now().Add(variant.deadline)); err != nil {
					t.Fatal(err)
				}
			}

			sent := make(map[uint64]bool)
			var refused []uint64
			payload := make([]byte, 1024)
			for sequence := uint64(0); len(refused) < 16; sequence++ {
				if sequence > 200000 {
					t.Fatal("200,000 DATA never met a refusal")
				}
				binary.BigEndian.PutUint64(payload, sequence)
				_, err := association.WriteData(DataRequest{AS: key, ProtocolData: params.ProtocolDataPayload{
					OriginatingPointCode: 1, DestinationPointCode: 2, ServiceIndicator: 3, NetworkIndicator: 2, Data: payload}})
				if err == nil {
					sent[sequence] = true
					continue
				}
				requireDataWriteError(t, err, DataNotSent, variant.cause)
				refused = append(refused, sequence)
			}
			if variant.deadline > 0 {
				if err := association.SetWriteDeadline(time.Time{}); err != nil {
					t.Fatal(err)
				}
			}

			close(peer.resume)
			if !waitFor(func() bool { return len(peer.delivered()) >= len(sent) }, 20*time.Second) {
				t.Fatalf("the peer received %d of %d accepted DATA", len(peer.delivered()), len(sent))
			}
			// Anything still in flight would arrive now.
			time.Sleep(500 * time.Millisecond)
			seen := make(map[uint64]int)
			for _, sequence := range peer.delivered() {
				seen[sequence]++
			}
			for _, sequence := range refused {
				if seen[sequence] != 0 {
					t.Fatalf("DATA %d was reported not sent but reached the peer", sequence)
				}
			}
			for sequence := range sent {
				if seen[sequence] != 1 {
					t.Fatalf("accepted DATA %d reached the peer %d times", sequence, seen[sequence])
				}
			}
			if len(seen) != len(sent) {
				t.Fatalf("the peer received %d distinct DATA, want the %d accepted", len(seen), len(sent))
			}
		})
	}
}
