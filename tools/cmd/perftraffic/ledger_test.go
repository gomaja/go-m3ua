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

func TestLedgerMissingSubmissionDoesNotInvalidateLaterOrderedData(testContext *testing.T) {
	const expected = uint64(flowCount * 40)
	ledger := newLedger(1, expected, 8)
	for index := uint64(1); index < expected; index++ {
		ledger.record(planMessage("cohort", 1, index, 1))
	}
	snapshot := ledger.snapshot()
	if snapshot.Unique != expected-1 || snapshot.Missing != 1 || snapshot.Invalid != 0 || snapshot.Reordered != 0 {
		testContext.Fatalf("a missing submission corrupted later accounting: %+v", snapshot)
	}
	if ledger.storageBytes() != flowCount*8 {
		testContext.Fatal("ledger storage grew after a sequence gap")
	}
}

func TestLedgerRetainsRecentDuplicateAndOrderingEvidence(testContext *testing.T) {
	ledger := newLedger(1, 1000, 8)
	for _, sequence := range []uint64{0, 1, 3, 20, 22, 21, 21} {
		ledger.record(messageIdentity{Sequence: sequence})
	}
	snapshot := ledger.snapshot()
	if snapshot.Unique != 6 || snapshot.Duplicate != 1 || snapshot.Reordered != 1 || snapshot.Invalid != 0 {
		testContext.Fatalf("snapshot = %+v", snapshot)
	}
	ledger.record(messageIdentity{Sequence: 0})
	if ledger.snapshot().Invalid != 1 {
		testContext.Fatal("arrival outside retained history must not earn unique-delivery credit")
	}
}

func FuzzLedgerMonotonicLoss(fuzz *testing.F) {
	fuzz.Add([]byte{1, 0, 2, 0, 3, 2, 0, 0})
	fuzz.Fuzz(func(testContext *testing.T, choices []byte) {
		if len(choices) == 0 || len(choices) > 4096 {
			return
		}
		window := int(choices[0])%65 + 1
		ledger := newLedger(8, uint64(len(choices)), window)
		var unique, duplicates uint64
		for index, choice := range choices {
			if choice&1 != 0 {
				continue
			}
			identity := planMessage("cohort", 1, uint64(index), 8)
			ledger.record(identity)
			unique++
			if choice&2 != 0 {
				ledger.record(identity)
				duplicates++
			}
		}
		snapshot := ledger.snapshot()
		if snapshot.Unique != unique || snapshot.Missing != uint64(len(choices))-unique ||
			snapshot.Duplicate != duplicates || snapshot.Invalid != 0 || snapshot.Reordered != 0 {
			testContext.Fatalf("window %d: got %+v, unique=%d duplicates=%d", window, snapshot, unique, duplicates)
		}
	})
}
