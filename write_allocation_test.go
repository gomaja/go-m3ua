// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

// The direct DATA send path is the hot path of a signalling gateway: every SS7
// payload crosses it once, so per-message allocations there set the garbage
// collection pace of the whole process. The end-to-end delivery budget for one
// DATA send is 16 allocations and 2*P+1024 allocated bytes per message (P = SS7
// user payload octets), measured over a whole sender process — that figure
// includes the fixed cost of the transport's SCTPWrite, which this package
// cannot influence.
//
// These tests measure everything the package itself contributes: the full
// synchronous send path from WriteData down to the transport write, with the
// transport replaced by the association's dataWriter seam so no SCTP socket is
// needed (SCTP is unavailable on darwin, and a socket would add noise besides).
// What the seam excludes is exactly SCTPWrite's own share — a handful of
// allocations and one payload-sized copy — so this package's share is held well
// under the end-to-end budget: at most 11 allocations and P+1024 bytes per
// message, leaving the transport the remainder.
//
// The measured association mirrors the benchmarked sender: ASP role, ASP
// Active, a Network Appearance configured, and one acknowledged Routing
// Context named per message.
func newSendAllocationAssociation(t *testing.T) *Association {
	t.Helper()

	conn, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
	conn.noteRoutingContextsAcked(params.NewRoutingContext(1))
	conn.cfg.NetworkAppearance = params.NewNetworkAppearance(7)
	conn.dataWriter = func(data []byte, _ *sctp.SndRcvInfo) (int, error) {
		return len(data), nil
	}
	return conn
}

// allocationRequest is the request the measured sends repeat. Building it is
// not measured: an application holds its own payload buffer and its own scope,
// exactly as the benchmarked sender does.
func allocationRequest(payloadSize int) DataRequest {
	return DataRequest{
		AS: ASKey{
			NetworkAppearance:    7,
			NetworkAppearanceSet: true,
			RoutingContext:       1,
			RoutingContextSet:    true,
		},
		ProtocolData: params.ProtocolDataPayload{
			OriginatingPointCode:    0x11111111,
			DestinationPointCode:    0x22222222,
			ServiceIndicator:        params.ServiceIndSCCP,
			SignallingLinkSelection: 1,
			Data:                    make([]byte, payloadSize),
		},
	}
}

func TestWriteDataAllocations(t *testing.T) {
	const maxAllocationsPerWrite = 11

	for _, payloadSize := range []int{128, 4096} {
		t.Run(payloadName(payloadSize), func(t *testing.T) {
			conn := newSendAllocationAssociation(t)
			request := allocationRequest(payloadSize)

			allocs := testing.AllocsPerRun(1000, func() {
				if _, err := conn.WriteData(request); err != nil {
					t.Fatalf("WriteData: %v", err)
				}
			})
			t.Logf("allocations per write: %.0f", allocs)
			if allocs > maxAllocationsPerWrite {
				t.Errorf("WriteData allocates %.0f times per message, budget is %d",
					allocs, maxAllocationsPerWrite)
			}
		})
	}
}

func TestWriteDataAllocatedBytes(t *testing.T) {
	for _, payloadSize := range []int{128, 4096} {
		t.Run(payloadName(payloadSize), func(t *testing.T) {
			conn := newSendAllocationAssociation(t)
			request := allocationRequest(payloadSize)

			write := func() {
				if _, err := conn.WriteData(request); err != nil {
					t.Fatalf("WriteData: %v", err)
				}
			}
			for i := 0; i < 200; i++ {
				write()
			}

			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			const runs = 5000
			for i := 0; i < runs; i++ {
				write()
			}
			runtime.ReadMemStats(&after)

			bytesPerWrite := float64(after.TotalAlloc-before.TotalAlloc) / runs
			budget := float64(payloadSize) + 1024
			t.Logf("allocated bytes per write: %.0f (budget %.0f)", bytesPerWrite, budget)
			if bytesPerWrite > budget {
				t.Errorf("WriteData allocates %.0f bytes per %d-octet message, budget is %.0f",
					bytesPerWrite, payloadSize, budget)
			}
		})
	}
}

func payloadName(payloadSize int) string {
	return fmt.Sprintf("payload-%d", payloadSize)
}
