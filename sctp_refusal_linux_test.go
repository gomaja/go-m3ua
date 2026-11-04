// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

//go:build linux

package m3ua

import (
	"context"
	"errors"
	"net/netip"
	"syscall"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

func shutdownDATAAssociation(t *testing.T) (*Association, *stalledASP, DataRequest) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	key := allocationRequest(0).AS
	cfg := NewAssociationConfig().SetApplicationServers(ASConfig{ASKey: key, TrafficMode: params.TrafficModeLoadshare})
	cfg.HeartbeatInfo = &HeartbeatInfo{Enabled: false}
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	listener, err := endpoint.Listen("m3ua", &sctp.Addr{IPs: []netip.Addr{netip.MustParseAddr("127.0.0.1")}}, NewListenerConfig(cfg))
	if err != nil {
		skipIfSCTPUnsupported(t, err)
		t.Fatal(err)
	}
	accepted := make(chan *Association, 1)
	acceptErrors := make(chan error, 1)
	go func() {
		c, err := listener.Accept(ctx)
		if err != nil {
			acceptErrors <- err
			return
		}
		accepted <- c
	}()
	peer := dialStalledASP(t, listener.Addr().(*sctp.Addr), 1)
	var c *Association
	select {
	case c = <-accepted:
	case err := <-acceptErrors:
		t.Fatal(err)
	case <-time.After(10 * time.Second):
		t.Fatal("SGP did not accept")
	}
	// Failure cleanup must also release a writer if a teardown regression takes
	// sendMu before closing SCTP.
	t.Cleanup(func() { _ = c.sctpConn.Abort() })
	if err := c.sctpConn.SetWriteBuffer(stallingSocketBuffer); err != nil {
		t.Fatal(err)
	}
	request := DataRequest{AS: key, ProtocolData: params.ProtocolDataPayload{
		OriginatingPointCode: 1, DestinationPointCode: 2, ServiceIndicator: 3,
		NetworkIndicator: 2, Data: make([]byte, 1024),
	}}
	until := time.Now().Add(10 * time.Second)
	for refusals := 0; ; {
		_, err := c.WriteData(request)
		if err == nil {
			refusals = 0
		} else if errors.Is(err, syscall.EAGAIN) {
			refusals++
		} else {
			t.Fatal(err)
		}
		status, err := c.sctpConn.Status()
		if err != nil {
			t.Fatal(err)
		}
		if refusals >= 16 && status.PeerRwnd == 0 {
			// The peer must have announced zero and this sender must have
			// processed that SACK before its last refill. Otherwise a late
			// SACK can free space after the apparent buffer wait starts.
			peerWindow, err := peer.conn.AssocInfo()
			if err != nil {
				t.Fatal(err)
			}
			peerStats, err := peer.conn.Stats()
			if err != nil {
				t.Fatal(err)
			}
			senderStats, err := c.sctpConn.Stats()
			if err != nil {
				t.Fatal(err)
			}
			if peerWindow.LocalRwnd == 0 && senderStats.SACKsIn >= peerStats.SACKsOut {
				break
			}
		}
		if time.Now().After(until) {
			t.Fatalf("peer window never closed with sustained EAGAIN: %+v", status)
		}
		if refusals > 0 {
			time.Sleep(5 * time.Millisecond)
		}
	}
	// Processing the zero-window SACK may itself have freed send-buffer
	// space after the last EAGAIN. Refill only after that counter barrier.
	for {
		_, err := c.WriteData(request)
		if errors.Is(err, syscall.EAGAIN) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if time.Now().After(until) {
			t.Fatal("send buffer did not refill after the zero-window SACK")
		}
	}
	return c, peer, request
}

func TestDATAParkedSendDoesNotDelayTeardown(t *testing.T) {
	for _, release := range []string{"Close", "Shutdown"} {
		t.Run(release, func(t *testing.T) {
			c, peer, request := shutdownDATAAssociation(t)
			if err := c.SetWriteDeadline(time.Now().Add(30 * time.Second)); err != nil {
				t.Fatal(err)
			}
			transport := &observedContextTransport{Conn: c.sctpConn, entered: make(chan struct{})}
			c.controlTransport = transport
			sendDone := make(chan error, 1)
			go func() { _, err := c.WriteData(request); sendDone <- err }()
			select {
			case <-transport.entered:
			case <-time.After(time.Second):
				t.Fatal("DATA did not enter transport")
			}
			select {
			case err := <-sendDone:
				t.Fatalf("DATA did not park: %v", err)
			case <-time.After(25 * time.Millisecond):
			}
			closed := make(chan error, 1)
			start := time.Now()
			go func() {
				if release == "Close" {
					closed <- c.Close()
				} else {
					closed <- c.Shutdown()
				}
			}()
			select {
			case <-c.Done():
			case <-time.After(time.Second):
				t.Fatal("teardown waited for parked DATA before publishing Done")
			}
			select {
			case err := <-sendDone:
				requireDataWriteError(t, err, DataNotSent, syscall.ESHUTDOWN)
			case <-time.After(time.Second):
				t.Fatal("teardown did not wake parked DATA")
			}
			t.Logf("%s woke the parked DATA send in %v before the peer resumed", release, time.Since(start))
			if !errors.Is(c.Err(), ErrAssociationClosed) {
				t.Fatalf("teardown cause = %v", c.Err())
			}
			// The peer is still stalled. Only after the writer has returned may the
			// shutdown drain queued traffic and finish its protocol exchange.
			close(peer.resume)
			select {
			case err := <-closed:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("teardown did not finish after peer resumed")
			}
			t.Logf("%s woke DATA and completed in %v", release, time.Since(start))
		})
	}
}

func TestDATANoWaitDuringTransportShutdown(t *testing.T) {
	c, peer, request := shutdownDATAAssociation(t)
	if err := c.sctpConn.Shutdown(); err != nil {
		t.Fatal(err)
	}
	_, err := c.WriteData(request)
	requireDataWriteError(t, err, DataNotSent, syscall.ESHUTDOWN)
	close(peer.resume)
}

func TestDATANoWaitAfterPeerShutdown(t *testing.T) {
	c, peer, request := shutdownDATAAssociation(t)
	if err := peer.conn.Shutdown(); err != nil {
		t.Fatal(err)
	}
	if !waitFor(func() bool {
		status, err := c.sctpConn.Status()
		return err == nil && status.State == sctp.StateShutdownReceived
	}, 3*time.Second) {
		t.Fatal("peer SHUTDOWN did not reach sender")
	}
	_, err := c.WriteData(request)
	requireDataWriteError(t, err, DataNotSent, syscall.ESHUTDOWN)
	close(peer.resume)
}

func TestSCTPAddressValidationCause(t *testing.T) {
	loopback := netip.MustParseAddr("127.0.0.1")
	mapped := netip.MustParseAddr("::ffff:127.0.0.1")
	for _, variant := range []struct {
		name   string
		local  *sctp.Addr
		remote *sctp.Addr
	}{
		{"remote port zero", nil, &sctp.Addr{IPs: []netip.Addr{loopback}}},
		{"duplicate local addresses", &sctp.Addr{IPs: []netip.Addr{loopback, loopback}}, &sctp.Addr{IPs: []netip.Addr{loopback}, Port: 2905}},
		{"mapped duplicate local addresses", &sctp.Addr{IPs: []netip.Addr{loopback, mapped}}, &sctp.Addr{IPs: []netip.Addr{loopback}, Port: 2905}},
	} {
		t.Run(variant.name, func(t *testing.T) {
			endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = endpoint.Close() }()
			association, err := endpoint.Dial(context.Background(), "m3ua", variant.local, variant.remote, dialCfg(time.Second))
			if association != nil || !errors.Is(err, syscall.EINVAL) {
				skipIfSCTPUnsupported(t, err)
				t.Fatalf("Dial = %v, %v; want nil, EINVAL", association, err)
			}
		})
	}
	for _, ips := range [][]netip.Addr{{loopback, loopback}, {loopback, mapped}} {
		endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
		if err != nil {
			t.Fatal(err)
		}
		listener, err := endpoint.Listen("m3ua", &sctp.Addr{IPs: ips}, NewListenerConfig(NewAssociationConfig()))
		if closeErr := endpoint.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
		if listener != nil || !errors.Is(err, syscall.EINVAL) {
			skipIfSCTPUnsupported(t, err)
			t.Fatalf("Listen = %v, %v; want nil, EINVAL", listener, err)
		}
	}
}
