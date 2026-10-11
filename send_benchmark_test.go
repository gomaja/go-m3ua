// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"sync"
	"testing"

	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

// These socket-free benchmarks measure the synchronous send path, excluding
// the kernel and the peer. The allocation tests enforce the same boundary.
func benchmarkSendAssociation() *Association {
	cfg := NewAssociationConfig().SetApplicationServers(ASConfig{
		ASKey: allocationRequest(0).AS, TrafficMode: params.TrafficModeLoadshare,
	})
	c := &Association{
		cfg: cfg, role: RoleASP, state: StateASPActive, muState: new(sync.RWMutex),
		sctpInfo: &sctp.SndInfo{PPID: M3UAPPID}, maxMessageStreamID: 4,
		dataWriter: func(b []byte, _ *sctp.SndInfo) (int, error) { return len(b), nil },
	}
	c.noteRoutingContextsAcked(params.NewRoutingContext(1))
	return c
}

func BenchmarkWriteDataContext(b *testing.B) {
	for _, size := range []int{128, 4096} {
		b.Run(payloadName(size), func(b *testing.B) {
			c, request := benchmarkSendAssociation(), allocationRequest(size)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := c.WriteDataContext(ctx, request); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkMTPTransferContext(b *testing.B) {
	for _, size := range []int{128, 4096} {
		b.Run(payloadName(size), func(b *testing.B) {
			e := benchmarkTransferEndpoint(b)
			request := MTPTransferRequest{ProtocolData: transferProtocolData(0x123456, 1, make([]byte, size))}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := e.MTPTransferContext(ctx, request); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkWriteData(b *testing.B) {
	for _, size := range []int{128, 4096} {
		b.Run(payloadName(size), func(b *testing.B) {
			c, request := benchmarkSendAssociation(), allocationRequest(size)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := c.WriteData(request); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func benchmarkTransferEndpoint(b *testing.B) *Endpoint {
	b.Helper()
	config := validASPConfig()
	config.Routing.SignallingGatewaySelection = RouteSelectionPrimaryBackup
	config.Routing.AllowUnknownDestinations = true
	e, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: config})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = e.Close() })
	c := benchmarkSendAssociation()
	c.cfg.PeerSGP = &SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}
	if !e.aspRoutes.attach(c) {
		b.Fatal("attach benchmark association")
	}
	return e
}

func BenchmarkMTPTransfer(b *testing.B) {
	for _, size := range []int{128, 4096} {
		b.Run(payloadName(size), func(b *testing.B) {
			e := benchmarkTransferEndpoint(b)
			request := MTPTransferRequest{ProtocolData: transferProtocolData(0x123456, 1, make([]byte, size))}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := e.MTPTransfer(request); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
