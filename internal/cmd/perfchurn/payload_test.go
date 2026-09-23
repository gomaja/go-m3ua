package main

import (
	"errors"
	"testing"
)

func TestPayloadRoundTripAndValidation(t *testing.T) {
	for _, kind := range []byte{kindLedger, kindOverload} {
		header := payloadHeader{Kind: kind, Epoch: 7, Association: 31, Sequence: 1 << 40}
		data := encodePayload(nil, header)
		if len(data) != payloadSize(kind) {
			t.Fatalf("kind %d: %d bytes", kind, len(data))
		}
		decoded, err := decodePayload(data)
		if err != nil || decoded != header {
			t.Fatalf("kind %d: decoded %+v, %v", kind, decoded, err)
		}
		corrupt := append([]byte(nil), data...)
		corrupt[len(corrupt)-1]++
		if _, err := decodePayload(corrupt); !errors.Is(err, errInvalidPayload) {
			t.Fatalf("kind %d: corrupted pattern accepted", kind)
		}
		if _, err := decodePayload(data[:len(data)-1]); !errors.Is(err, errInvalidPayload) {
			t.Fatalf("kind %d: truncated payload accepted", kind)
		}
	}
	outOfRange := encodePayload(nil, payloadHeader{Kind: kindLedger, Association: stableAssociations})
	if _, err := decodePayload(outOfRange); err == nil {
		t.Fatal("association outside the stable inventory accepted")
	}
}

func TestLedgerClassifiesEveryDelivery(t *testing.T) {
	var ledger receiveLedger
	ledger.reset(3)
	payload := func(association int, epoch uint32, sequence uint64) []byte {
		return encodePayload(nil, payloadHeader{Kind: kindLedger, Epoch: epoch, Association: uint16(association), Sequence: sequence})
	}
	for sequence := uint64(0); sequence < 5; sequence++ {
		ledger.record(0, payload(0, 3, sequence))
	}
	ledger.record(1, payload(1, 3, 0))
	ledger.record(1, payload(1, 3, 2)) // gap: 1 lost
	ledger.record(1, payload(1, 3, 1)) // late
	ledger.record(1, payload(1, 3, 2)) // duplicate
	ledger.record(2, payload(3, 3, 0)) // arrived on the wrong association
	ledger.record(2, payload(2, 2, 0)) // previous epoch
	ledger.record(2, encodePayload(nil, payloadHeader{Kind: kindOverload, Association: 2}))
	ledger.record(2, []byte("short"))

	result := ledger.result("asp-to-sgp", []uint64{5, 3, 0, 1}, 0, "")
	if result.UniqueTotal != 7 || result.SentTotal != 9 || result.Missing != 2 || result.Gaps != 1 || result.Late != 2 ||
		result.Invalid != 2 || result.Stale != 1 || ledger.overloadTotal() != 1 {
		t.Fatalf("result %+v", result)
	}
	if result.lossFree() {
		t.Fatal("a lossy ledger is loss-free")
	}
	if !ledger.reached([]uint64{5, 2}) || ledger.reached([]uint64{5, 3}) {
		t.Fatal("reached")
	}

	var clean receiveLedger
	clean.reset(1)
	clean.record(4, payload(4, 1, 0))
	if got := clean.result("sgp-to-asp", []uint64{0, 0, 0, 0, 1}, 0, ""); !got.lossFree() {
		t.Fatalf("clean ledger %+v", got)
	}
	if got := clean.result("sgp-to-asp", []uint64{0, 0, 0, 0, 1}, 1, "refused"); got.lossFree() {
		t.Fatal("a write failure on a surviving association is not loss-free")
	}
	if got := clean.result("sgp-to-asp", []uint64{0, 0, 0, 0, 0}, 0, ""); got.lossFree() || got.Excess != 1 {
		t.Fatalf("an unsent delivery is not loss-free: %+v", got)
	}
}
