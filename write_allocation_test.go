// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
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
	setInventoryNetworkAppearance(&conn.cfg.ApplicationServers, params.NewNetworkAppearance(7))
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

// The two budgets above measure WriteData, which serializes DATA with the
// association's own encoder. They therefore say nothing at all about the other
// DATA encoder this library ships — messages.Data.MarshalTo, the codec path
// that WriteSignal and the SGP distribution engine send through. Forcing that
// encoder down its staged branch leaves both budgets reading exactly what they
// read before, so a ceiling, however tight, could never have caught it.
//
// Data.MarshalTo has two branches. A Data carrying extension parameters is
// staged in a private payload buffer so a parameter that fails to marshal
// leaves the destination untouched; a Data without them marshals straight into
// the destination, which is what keeps a relayed DATA free of a second
// message-sized buffer and a copy of every payload octet. The two tests below
// pin that split at its source, one branch each.
//
// The assertion is an exact allocation count, which is usually the brittle
// choice — a count tuned to today's inlining fails on a Go release that
// changes it. It is not brittle here because the count is zero. Zero is not a
// measured figure with headroom, it is the categorical claim that the branch
// creates no heap object at all, and nothing but a new allocation in the code
// under test can move it. The staged branch's own count is its opposite
// number: the one buffer it exists to allocate, which is what makes the
// measurement capable of telling the branches apart.
//
// What the fast path costs is pinned here; what it means for the caller —
// that Header.Payload afterwards aliases the destination, so b may not be
// recycled while the Data is retained — is pinned in the messages package by
// TestDataMarshalToAliasesTheDestinationAsDocumented. Staging destroys both,
// and both are worth failing on separately.
func allocationDataMessage(payloadSize int, others []*params.Param) *messages.Data {
	message := messages.NewData(
		params.NewNetworkAppearance(7),
		params.NewRoutingContext(1),
		params.NewProtocolData(0x11111111, 0x22222222, params.ServiceIndSCCP, 0, 0, 1,
			make([]byte, payloadSize)),
		nil,
	)
	message.Others = others
	message.SetLength()
	return message
}

// marshalAllocations reports the allocations one MarshalTo into an already
// sized destination costs. The first call is outside the measurement so that
// nothing lazily built on the way in is charged to the steady state.
func marshalAllocations(t *testing.T, message *messages.Data) float64 {
	t.Helper()

	destination := make([]byte, message.MarshalLen())
	marshal := func() {
		if err := message.MarshalTo(destination); err != nil {
			t.Fatalf("MarshalTo: %v", err)
		}
	}
	marshal()
	return testing.AllocsPerRun(1000, marshal)
}

func TestDataMarshalToWithoutExtensionParametersStagesNothing(t *testing.T) {
	for _, payloadSize := range []int{128, 4096} {
		t.Run(payloadName(payloadSize), func(t *testing.T) {
			allocs := marshalAllocations(t, allocationDataMessage(payloadSize, nil))
			t.Logf("allocations per marshal: %.0f", allocs)
			if allocs != 0 {
				t.Errorf("Data.MarshalTo allocates %.0f times for a %d-octet message with "+
					"no extension parameters, want 0; the fast path marshals straight into "+
					"the destination and must stage nothing", allocs, payloadSize)
			}
		})
	}
}

func TestDataMarshalToWithExtensionParametersStagesOneBuffer(t *testing.T) {
	for _, payloadSize := range []int{128, 4096} {
		t.Run(payloadName(payloadSize), func(t *testing.T) {
			message := allocationDataMessage(payloadSize,
				[]*params.Param{params.NewInfoString("x")})
			allocs := marshalAllocations(t, message)
			t.Logf("allocations per marshal: %.0f", allocs)
			if allocs != 1 {
				t.Errorf("Data.MarshalTo allocates %.0f times for a %d-octet message "+
					"carrying an extension parameter, want 1 for the staged payload "+
					"buffer; if it is 0 the measurement can no longer tell the staged "+
					"path from the fast path that "+
					"TestDataMarshalToWithoutExtensionParametersStagesNothing pins",
					allocs, payloadSize)
			}
		})
	}
}

func payloadName(payloadSize int) string {
	return fmt.Sprintf("payload-%d", payloadSize)
}
