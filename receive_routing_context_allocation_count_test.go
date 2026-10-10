//go:build !race

package m3ua

import (
	"context"
	"fmt"
	"testing"
)

// Measured on Go 1.26.8/1.26.9 after the DATA scope optimization: validation
// 0 allocations, complete single-RC handleData 4. Keep the three-allocation
// headroom documented in receive_allocation_count_test.go, with separate
// gates so a collection rebuild cannot hide behind other receive allocations.
func TestDataRoutingContextAllocationCount(testContext *testing.T) {
	for _, count := range []int{1, 32, 1000} {
		for _, dynamic := range []bool{false, true} {
			testContext.Run(fmt.Sprintf("contexts_%d/dynamic_%t", count, dynamic), func(testContext *testing.T) {
				conn := newReceiveRoutingContextAssociation(testContext, count, dynamic)
				peer := inboundData(uint32(count), "allocation gate").RoutingContext
				allocs := testing.AllocsPerRun(1000, func() { _ = conn.validateDataRoutingContext(peer) })
				testContext.Logf("%.0f allocations per validation", allocs)
				if allocs > 3 {
					testContext.Fatalf("validation allocates %.0f times, budget is 3", allocs)
				}
			})
		}
	}
}

func TestHandleDataRoutingContextScopeAllocationCount(testContext *testing.T) {
	conn := newReceiveRoutingContextAssociation(testContext, 32, false)
	data := inboundData(32, string(make([]byte, 128)))
	ctx := context.Background()
	allocs := testing.AllocsPerRun(1000, func() {
		conn.handleData(ctx, data, nil)
		receiveAllocationSink = <-conn.dataChan
	})
	testContext.Logf("32 contexts: %.0f allocations per handleData", allocs)
	if allocs > 7 {
		testContext.Fatalf("handleData allocates %.0f times, budget is 7", allocs)
	}
}
