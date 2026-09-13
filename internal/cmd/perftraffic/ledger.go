package main

type ledgerSnapshot struct {
	Unique    uint64 `json:"unique"`
	Missing   uint64 `json:"missing"`
	Duplicate uint64 `json:"duplicate"`
	Invalid   uint64 `json:"invalid"`
	Reordered uint64 `json:"reordered"`
}

type flowLedger struct {
	base        uint64
	highestSeen uint64
	seenAny     bool
	seen        []uint64
}

type receiveLedger struct {
	associations int
	expected     uint64
	window       uint64
	flows        [flowCount]flowLedger
	snapshotData ledgerSnapshot
}

type ledgerRecordStatus uint8

const (
	ledgerInvalid ledgerRecordStatus = iota
	ledgerUnique
	ledgerDuplicate
)

func newLedger(associations int, expected uint64, window int) *receiveLedger {
	ledger := &receiveLedger{associations: associations, expected: expected, window: uint64(window)}
	words := (window + 63) / 64
	for index := range ledger.flows {
		ledger.flows[index].seen = make([]uint64, words)
	}
	return ledger
}

func (ledger *receiveLedger) record(identity messageIdentity) ledgerRecordStatus {
	if identity.Flow >= flowCount || int(identity.Association) >= ledger.associations ||
		int(identity.Association) != int(identity.Flow)%ledger.associations ||
		identity.Sequence > (^uint64(0)-uint64(identity.Flow))/uint64(flowCount) ||
		identity.Sequence*uint64(flowCount)+uint64(identity.Flow) >= ledger.expected {
		ledger.snapshotData.Invalid++
		return ledgerInvalid
	}
	flow := &ledger.flows[identity.Flow]
	if identity.Sequence < flow.base {
		ledger.snapshotData.Invalid++
		return ledgerInvalid
	}
	if identity.Sequence-flow.base >= ledger.window {
		ledger.advanceWindow(flow, identity.Sequence-ledger.window+1)
	}
	wordIndex, bitIndex := ledger.bitLocation(identity.Sequence)
	mask := uint64(1) << bitIndex
	if flow.seen[wordIndex]&mask != 0 {
		ledger.snapshotData.Duplicate++
		return ledgerDuplicate
	}
	flow.seen[wordIndex] |= mask
	ledger.snapshotData.Unique++
	if flow.seenAny && identity.Sequence < flow.highestSeen {
		ledger.snapshotData.Reordered++
	}
	if !flow.seenAny || identity.Sequence > flow.highestSeen {
		flow.highestSeen = identity.Sequence
	}
	flow.seenAny = true
	return ledgerUnique
}

func (ledger *receiveLedger) advanceWindow(flow *flowLedger, nextBase uint64) {
	if nextBase-flow.base >= ledger.window {
		clear(flow.seen)
	} else {
		for sequence := flow.base; sequence < nextBase; sequence++ {
			wordIndex, bitIndex := ledger.bitLocation(sequence)
			flow.seen[wordIndex] &^= uint64(1) << bitIndex
		}
	}
	flow.base = nextBase
}

func (ledger *receiveLedger) bitLocation(sequence uint64) (int, uint) {
	position := sequence % ledger.window
	return int(position / 64), uint(position % 64)
}

func (ledger *receiveLedger) snapshot() ledgerSnapshot {
	snapshot := ledger.snapshotData
	if snapshot.Unique < ledger.expected {
		snapshot.Missing = ledger.expected - snapshot.Unique
	}
	return snapshot
}

func (ledger *receiveLedger) storageBytes() int {
	bytes := 0
	for index := range ledger.flows {
		bytes += len(ledger.flows[index].seen) * 8
	}
	return bytes
}
