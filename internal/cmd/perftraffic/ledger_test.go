package main

import "testing"

func TestLedgerCountsUniqueDuplicateReorderedAndMissing(testContext *testing.T) {
	ledger := newLedger(2, 66, 8)
	for _, identity := range []messageIdentity{
		{Association: 0, Flow: 0, Sequence: 0},
		{Association: 1, Flow: 1, Sequence: 0},
		{Association: 0, Flow: 0, Sequence: 2},
		{Association: 0, Flow: 0, Sequence: 1},
		{Association: 0, Flow: 0, Sequence: 1},
	} {
		ledger.record(identity)
	}
	snapshot := ledger.snapshot()
	if snapshot.Unique != 4 || snapshot.Duplicate != 1 || snapshot.Reordered != 1 || snapshot.Missing != 62 {
		testContext.Fatalf("snapshot = %+v", snapshot)
	}
}

func TestLedgerRejectsWrongAssociationAndOutOfWindowSequence(testContext *testing.T) {
	ledger := newLedger(2, 10_000, 8)
	ledger.record(messageIdentity{Association: 1, Flow: 0, Sequence: 0})
	ledger.record(messageIdentity{Association: 0, Flow: 0, Sequence: 8192})
	snapshot := ledger.snapshot()
	if snapshot.Invalid != 2 || snapshot.Unique != 0 {
		testContext.Fatalf("snapshot = %+v", snapshot)
	}
}

func TestLedgerRejectsIdentityOutsideScheduledRange(testContext *testing.T) {
	ledger := newLedger(1, 1, 8192)
	ledger.record(messageIdentity{Association: 0, Flow: 1, Sequence: 0})
	ledger.record(messageIdentity{Association: 0, Flow: 0, Sequence: ^uint64(0)})
	snapshot := ledger.snapshot()
	if snapshot.Invalid != 2 || snapshot.Unique != 0 || snapshot.Missing != 1 {
		testContext.Fatalf("snapshot = %+v", snapshot)
	}
}

func TestLedgerStorageIsBoundedByFlowWindow(testContext *testing.T) {
	ledger := newLedger(32, 1_000_000_000, 8192)
	if got, want := ledger.storageBytes(), flowCount*8192/8; got != want {
		testContext.Fatalf("storageBytes = %d, want %d", got, want)
	}
}
