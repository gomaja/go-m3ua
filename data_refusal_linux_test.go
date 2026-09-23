// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
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
