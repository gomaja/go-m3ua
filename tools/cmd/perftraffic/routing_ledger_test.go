package main

import (
	"fmt"
	"testing"
	"unsafe"
)

func TestRoutingLedgerRingBoundaries(testContext *testing.T) {
	for _, window := range []int{1, 63, 64, 65, 8192} {
		testContext.Run(fmt.Sprint(window), func(testContext *testing.T) {
			ledger, err := newRoutingLedger(uint64(window*4)*routingRouteCount, window)
			if err != nil {
				testContext.Fatal(err)
			}
			for sequence := uint64(0); sequence < uint64(window*3); sequence++ {
				identity := routingIdentity{Route: 42, Sequence: sequence}
				if ledger.record(identity) != ledgerUnique || ledger.record(identity) != ledgerDuplicate {
					testContext.Fatalf("sequence %d not unique then duplicate", sequence)
				}
			}
			if ledger.record(routingIdentity{Route: 42, Sequence: uint64(window*2 - 1)}) != ledgerInvalid {
				testContext.Fatal("expired sequence credited")
			}
			wantedStorage := int(unsafe.Sizeof(*ledger)) + routingRouteCount*((window+63)/64)*8
			if ledger.storageBytes() != wantedStorage {
				testContext.Fatalf("storage=%d want=%d", ledger.storageBytes(), wantedStorage)
			}
		})
	}
}

func TestRoutingLedgerAccountsIndependentFlowsAndExactHistoryStorage(testContext *testing.T) {
	ledger, err := newRoutingLedger(3000, 8192)
	if err != nil {
		testContext.Fatal(err)
	}
	wantedStorage := 1_024_000 + int(unsafe.Sizeof(*ledger))
	if ledger.storageBytes() != wantedStorage || maxOutstanding != 8192 {
		testContext.Fatalf("history=%d want=%d admission=%d", ledger.storageBytes(), wantedStorage, maxOutstanding)
	}
	for index := uint64(0); index < 3000; index++ {
		if ledger.record(planRouteMessage("routes", 7, index)) != ledgerUnique {
			testContext.Fatalf("unique index %d rejected", index)
		}
	}
	if snapshot := ledger.snapshot(); snapshot != (ledgerSnapshot{Unique: 3000}) {
		testContext.Fatalf("snapshot=%+v", snapshot)
	}
	if ledger.storageBytes() != wantedStorage {
		testContext.Fatal("history grew")
	}
}

func TestRoutingLedgerRejectsDuplicateReorderedMissingAndExpiredHistory(testContext *testing.T) {
	ledger, err := newRoutingLedger(30_000, 8)
	if err != nil {
		testContext.Fatal(err)
	}
	for _, sequence := range []uint64{0, 2, 1, 1, 20, 22, 21, 21} {
		ledger.record(routingIdentity{Route: 999, Sequence: sequence})
	}
	snapshot := ledger.snapshot()
	if snapshot.Unique != 6 || snapshot.Duplicate != 2 || snapshot.Reordered != 2 || snapshot.Missing != 29_994 || snapshot.Invalid != 0 {
		testContext.Fatalf("snapshot=%+v", snapshot)
	}
	for _, identity := range []routingIdentity{{Route: 999, Sequence: 0}, {Route: 1000}, {Route: 999, Sequence: 30}, {Sequence: ^uint64(0)}} {
		if ledger.record(identity) != ledgerInvalid {
			testContext.Fatalf("invalid identity accepted: %+v", identity)
		}
	}
	if ledger.snapshot().Invalid != 4 {
		testContext.Fatalf("invalid count=%d", ledger.snapshot().Invalid)
	}
	if ledger.record(routingIdentity{Route: 0}) != ledgerUnique {
		testContext.Fatal("one route's history advancement invalidated another route")
	}
}

func TestRoutingLedgerRejectsInvalidConfiguration(testContext *testing.T) {
	for _, window := range []int{-1, 0, 8193, int(^uint(0) >> 1)} {
		if _, err := newRoutingLedger(1000, window); err == nil {
			testContext.Fatalf("window %d accepted", window)
		}
	}
	if _, err := newRoutingLedger(0, 8192); err == nil {
		testContext.Fatal("empty scheduled range accepted")
	}
}
