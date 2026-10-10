//go:build !race

package m3ua

import (
	"context"
	"testing"
)

var receiveAllocationSink *DataMessage

// At cc659cf, Go 1.25.10 measures 6/9 allocations for one/32 ASs, while
// Go 1.26.9 measures 11/14; d0c7552 also measures 11/14 with Go 1.26.9.
// Account for those five toolchain allocations while preserving the former
// three-allocation headroom for one AS. Both limits remain below the approved
// 24-allocation receive budget recorded in PR #83's acceptance evidence:
// https://github.com/gomaja/go-m3ua/pull/83#issuecomment-5792346793
const maxHandleDataReadDataAllocations = 14

const max32ASHandleDataReadDataAllocations = 14

func TestHandleDataReadDataAllocationCount(testContext *testing.T) {
	tests := []struct {
		name            string
		routingContexts []uint32
		maxAllocations  int
	}{
		{name: "one AS", routingContexts: []uint32{1}, maxAllocations: maxHandleDataReadDataAllocations},
		{name: "32 AS", routingContexts: routingContexts(32), maxAllocations: max32ASHandleDataReadDataAllocations},
	}
	for _, test := range tests {
		testContext.Run(test.name, func(testContext *testing.T) {
			conn, _ := newDataWriteAssociation(testContext, test.routingContexts...)
			data := inboundData(1, string(make([]byte, 4096)))
			ctx := context.Background()

			allocs := testing.AllocsPerRun(1000, func() {
				conn.recvStream.Store(1)
				conn.handleData(ctx, data, nil)
				message, err := conn.ReadData(ctx)
				if err != nil {
					panic(err)
				}
				receiveAllocationSink = message
			})
			testContext.Logf("allocations per handleData/ReadData: %.0f", allocs)
			if allocs > float64(test.maxAllocations) {
				testContext.Fatalf("handleData/ReadData allocates %.0f times, budget is %d", allocs, test.maxAllocations)
			}
		})
	}
}
