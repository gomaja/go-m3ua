package main

import (
	"errors"
	"unsafe"
)

type routingLedger struct {
	expected     uint64
	window       uint64
	flows        [routingRouteCount]flowLedger
	snapshotData ledgerSnapshot
}

func newRoutingLedger(expected uint64, window int) (*routingLedger, error) {
	if expected == 0 || window < 1 || window > maxOutstanding {
		return nil, errors.New("invalid routing history configuration")
	}
	ledger := &routingLedger{expected: expected, window: uint64(window)}
	words := (window + 63) / 64
	for index := range ledger.flows {
		ledger.flows[index].seen = make([]uint64, words)
	}
	return ledger, nil
}

func (ledger *routingLedger) record(identity routingIdentity) ledgerRecordStatus {
	index, err := routingGlobalIndex(identity)
	if err != nil || index >= ledger.expected {
		ledger.snapshotData.Invalid++
		return ledgerInvalid
	}
	flow := &ledger.flows[identity.Route]
	if identity.Sequence < flow.base {
		ledger.snapshotData.Invalid++
		return ledgerInvalid
	}
	if identity.Sequence-flow.base >= ledger.window {
		nextBase := identity.Sequence - ledger.window + 1
		if nextBase-flow.base >= ledger.window {
			clear(flow.seen)
		} else {
			for sequence := flow.base; sequence < nextBase; sequence++ {
				position := sequence % ledger.window
				flow.seen[position/64] &^= uint64(1) << (position % 64)
			}
		}
		flow.base = nextBase
	}
	position := identity.Sequence % ledger.window
	mask := uint64(1) << (position % 64)
	if flow.seen[position/64]&mask != 0 {
		ledger.snapshotData.Duplicate++
		return ledgerDuplicate
	}
	flow.seen[position/64] |= mask
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

func (ledger *routingLedger) snapshot() ledgerSnapshot {
	snapshot := ledger.snapshotData
	if snapshot.Unique < ledger.expected {
		snapshot.Missing = ledger.expected - snapshot.Unique
	}
	return snapshot
}

func (ledger *routingLedger) storageBytes() int {
	storage := int(unsafe.Sizeof(*ledger))
	for index := range ledger.flows {
		storage += cap(ledger.flows[index].seen) * 8
	}
	return storage
}
