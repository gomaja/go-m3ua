//go:build !race

package m3ua

import (
	"context"
	"testing"
)

var receiveAllocationSink *DataMessage

const maxHandleDataReadDataAllocations = 9

const max32ASHandleDataReadDataAllocations = 9

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
