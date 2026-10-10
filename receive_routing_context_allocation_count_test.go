//go:build !race

package m3ua

import (
	"context"
	"fmt"
	"testing"

	"github.com/gomaja/go-m3ua/messages/params"
)

// Measured on Go 1.26.8/1.26.9 after the DATA scope optimization: validation
// 0 allocations, complete single-RC handleData 2, dedicated omitted-RC DATA 1.
// Keep the three-allocation
// headroom documented in receive_allocation_count_test.go, with separate
// gates so a collection rebuild cannot hide behind other receive allocations.
func TestDataRoutingContextAllocationCount(testContext *testing.T) {
	for _, count := range []int{1, 32, 1000} {
		for _, dynamic := range []bool{false, true} {
			testContext.Run(fmt.Sprintf("contexts_%d/dynamic_%t", count, dynamic), func(testContext *testing.T) {
				conn := newReceiveRoutingContextAssociation(testContext, count, dynamic)
				for _, peer := range []*params.Param{params.NewRoutingContext(uint32(count)), nil} {
					allocs := testing.AllocsPerRun(1000, func() { _ = conn.validateDataRoutingContext(peer) })
					testContext.Logf("omitted %t: %.0f allocations per validation", peer == nil, allocs)
					if allocs > 3 {
						testContext.Fatalf("validation allocates %.0f times, budget is 3", allocs)
					}
				}
			})
		}
	}
}

// Dedicated DATA inference must not materialize an inventory at any of the
// receive call sites. Apply the same three-allocation headroom to this path.
func TestHandleDataOmittedRoutingContextAllocationCount(testContext *testing.T) {
	conn := newReceiveRoutingContextAssociation(testContext, 1, false)
	data := inboundData(1, string(make([]byte, 128)))
	data.RoutingContext = nil
	allocs := testing.AllocsPerRun(1000, func() {
		conn.handleData(context.Background(), data, nil)
		receiveAllocationSink = <-conn.dataChan
	})
	testContext.Logf("omitted RC: %.0f allocations per handleData", allocs)
	if allocs > 4 {
		testContext.Fatalf("handleData allocates %.0f times, budget is 4", allocs)
	}
}

// Reading the selected scalar NA has no ownership transfer and must allocate
// nothing. This stricter query contract complements the full-handler gates,
// whose three-allocation headroom would hide the temporary two-allocation Param.
func TestDataNetworkAppearanceValidationDoesNotAllocate(testContext *testing.T) {
	conn := newReceiveRoutingContextAssociation(testContext, 1, false)
	for _, rc := range []*params.Param{nil, params.NewRoutingContext(1)} {
		for _, na := range []*params.Param{nil, params.NewNetworkAppearance(7)} {
			allocs := testing.AllocsPerRun(1000, func() { _ = conn.validateDataNetworkAppearance(na, rc) })
			testContext.Logf("omitted RC %t, omitted NA %t: %.0f NA validation allocations", rc == nil, na == nil, allocs)
			if allocs != 0 {
				testContext.Fatalf("omitted RC %t, omitted NA %t: scalar NA validation allocates %.0f times", rc == nil, na == nil, allocs)
			}
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
	if allocs > 5 {
		testContext.Fatalf("handleData allocates %.0f times, budget is 5", allocs)
	}
}
