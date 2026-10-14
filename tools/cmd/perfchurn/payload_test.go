package main

import (
	"errors"
	"testing"
)

func TestPayloadRoundTripAndValidation(t *testing.T) {
	for _, header := range []payloadHeader{
		{Kind: kindLedger, Epoch: 7, Association: 31, Flow: flowsPerAssociation - 1, Sequence: 1 << 40},
		{Kind: kindOverload, Association: 3, Sequence: 12},
	} {
		size := overloadPayloadSize
		if header.Kind == kindLedger {
			size = 512
		}
		data := encodePayload(nil, header, size)
		if len(data) != size {
			t.Fatalf("kind %d: %d bytes", header.Kind, len(data))
		}
		decoded, err := decodePayload(data)
		if err != nil || decoded != header {
			t.Fatalf("kind %d: decoded %+v, %v", header.Kind, decoded, err)
		}
		corrupt := append([]byte(nil), data...)
		corrupt[len(corrupt)-1]++
		if _, err := decodePayload(corrupt); !errors.Is(err, errInvalidPayload) {
			t.Fatalf("kind %d: corrupted pattern accepted", header.Kind)
		}
	}
	if _, err := decodePayload(encodePayload(nil, payloadHeader{Kind: kindOverload}, 128)); !errors.Is(err, errInvalidPayload) {
		t.Fatal("a short overload payload accepted")
	}
	for _, header := range []payloadHeader{
		{Kind: kindLedger, Association: stableAssociations},
		{Kind: kindLedger, Flow: flowsPerAssociation},
	} {
		if _, err := decodePayload(encodePayload(nil, header, 128)); err == nil {
			t.Fatalf("header outside the stable inventory accepted: %+v", header)
		}
	}
}

func ledgerPayload(association, flow int, epoch uint32, sequence uint64, workload payloadWorkload) []byte {
	return encodePayload(nil, payloadHeader{Kind: kindLedger, Epoch: epoch, Association: uint16(association), Flow: uint8(flow),
		Sequence: sequence}, workload.size(sequence))
}

func TestLedgerJudgesEachFlowInOrder(t *testing.T) {
	var ledger receiveLedger
	ledger.reset(3, workloadMix)
	// Two flows of one association interleave; each is in order on its own.
	for sequence := uint64(0); sequence < 5; sequence++ {
		ledger.record(0, ledgerPayload(0, 0, 3, sequence, workloadMix))
		if sequence < 3 {
			ledger.record(0, ledgerPayload(0, 1, 3, sequence, workloadMix))
		}
	}
	ledger.record(1, ledgerPayload(1, 2, 3, 0, workloadMix))
	ledger.record(1, ledgerPayload(1, 2, 3, 2, workloadMix)) // gap: 1 lost
	ledger.record(1, ledgerPayload(1, 2, 3, 1, workloadMix)) // late
	ledger.record(1, ledgerPayload(1, 2, 3, 2, workloadMix)) // duplicate
	ledger.record(2, ledgerPayload(3, 0, 3, 0, workloadMix)) // wrong association
	ledger.record(2, ledgerPayload(2, 0, 2, 0, workloadMix)) // previous epoch
	// Sequence 99 of the mix is a 4,096-byte message; a 128-byte one is invalid.
	ledger.record(2, encodePayload(nil, payloadHeader{Kind: kindLedger, Epoch: 3, Association: 2, Sequence: 99}, 128))
	ledger.record(2, encodePayload(nil, payloadHeader{Kind: kindOverload, Association: 2}, overloadPayloadSize))

	sent := make([]uint64, flowCount)
	sent[flowIndex(0, 0)], sent[flowIndex(0, 1)], sent[flowIndex(1, 2)] = 5, 3, 3
	result := ledger.result("asp-to-sgp", sent, 0, "")
	if result.UniqueTotal != 10 || result.SentTotal != 11 || result.Missing != 1 || result.Gaps != 1 || result.Late != 2 ||
		result.Invalid != 2 || result.Stale != 1 || ledger.overloadTotal() != 1 || result.Workload != string(workloadMix) {
		t.Fatalf("result %+v", result)
	}
	if result.lossFree() {
		t.Fatal("a lossy ledger is loss-free")
	}
}

func TestLedgerFoldsWhatArrivesAfterTheEpochIsJudged(t *testing.T) {
	workload, _ := parseWorkload("128")
	var ledger receiveLedger
	if addendum := ledger.reset(1, workload); addendum != (ledgerAddendum{}) {
		t.Fatalf("a first epoch has nothing to fold: %+v", addendum)
	}
	for sequence := uint64(0); sequence < 10; sequence++ {
		ledger.record(4, ledgerPayload(4, 1, 1, sequence, workload))
	}
	sent := make([]uint64, flowCount)
	sent[flowIndex(4, 1)] = 10
	result := ledger.result("sgp-to-asp", sent, 0, "")
	if !result.lossFree() {
		t.Fatalf("clean epoch %+v", result)
	}
	ledger.record(4, ledgerPayload(4, 1, 1, 5, workload))  // a duplicate after the epoch was judged
	ledger.record(4, ledgerPayload(4, 1, 1, 10, workload)) // a delivery nobody sent in this epoch
	addendum := ledger.reset(2, workload)
	if addendum.Epoch != 1 || addendum.Late != 1 || addendum.Unique != 1 {
		t.Fatalf("addendum %+v", addendum)
	}
	result.fold(addendum)
	if result.lossFree() || result.Late != 1 || result.AfterClose != 1 {
		t.Fatalf("folded result %+v", result)
	}
	if final := ledger.finalRead(); final != (ledgerAddendum{}) {
		t.Fatalf("an epoch never judged has nothing to fold: %+v", final)
	}
	mismatched := result
	mismatched.fold(ledgerAddendum{Epoch: 99, Late: 5})
	if mismatched.Late != result.Late {
		t.Fatal("an addendum of another epoch was folded")
	}
}
