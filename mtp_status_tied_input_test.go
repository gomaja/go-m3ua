package m3ua

import (
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
)

func TestOfferedStatusRenewalAfterPartlyExpiredCanonicalAliases(t *testing.T) {
	e, a := newStatusReassertionFixture(t, true)
	a.addDynamicASKey(*staticASKey(7, 8), RoutingKey{}, false)
	a.noteCanonicalRemoteAS(8, "as-core")
	a.noteRoutingContextsAcked(params.NewRoutingContext(1, 2, 8))
	a.notifyASPRouteStateChanged()
	drainMTPIndications(e.MTPIndications())
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
	partition := canonicalSSNMPartition("sg-a", "as-core")
	scope := WireScope{NetworkAppearance: 7, NetworkAppearanceSet: true,
		RoutingContextSet: true, RoutingContexts: []uint32{1, 8}}
	resolution := a.captureSSNMDestinationRefusal(scope, false)
	if len(resolution.byPartition[partition]) != 2 {
		t.Fatalf("grouped report did not resolve both aliases: %+v", resolution.byPartition[partition])
	}
	report := SSNMReport{Kind: SSNMSignallingCongestionReport, Source: SSNMPeerReport, Scope: scope,
		Destinations: []PointCodeRange{{PointCode: statusCommitPC}}, CongestionLevel: 2, CongestionLevelSet: true}
	if err := a.publishSSNMReport(report); err != nil {
		t.Fatal(err)
	}
	// The grouped report is one source statement with two resolved AS keys.
	// Timer commits operate one retained key at a time, so the aliases can
	// temporarily differ in freshness while sharing the newest revision.
	expired := false
	now := time.Now().Add(2 * time.Hour)
	for _, candidate := range e.ssnm.congestionExpiries(now) {
		if candidate.received.association == a.ID() && candidate.received.destination.pointCode == statusCommitPC && candidate.received.as.RoutingContext == 1 {
			expired = e.ssnm.expireCongestionStatement(candidate, now)
		}
	}
	if !expired {
		t.Fatal("first canonical alias did not expire")
	}
	e.ssnm.mu.Lock()
	var revision uint64
	fresh, stale, revisionsEqual := 0, 0, true
	for key, record := range e.ssnm.partitions[partition].receivedCongestion {
		if key.association != a.ID() || key.destination.pointCode != statusCommitPC || key.destination.mask != 0 {
			continue
		}
		if revision != 0 && revision != record.Revision {
			revisionsEqual = false
		}
		revision = record.Revision
		if record.Freshness == CongestionExpired {
			stale++
		} else {
			fresh++
		}
	}
	e.ssnm.mu.Unlock()
	if fresh != 1 || stale != 1 || !revisionsEqual {
		t.Fatalf("tied source inputs: fresh=%d expired=%d equal-revision=%v", fresh, stale, revisionsEqual)
	}
	// Consume the expiry commit while the projected status is still positive.
	// Fresh input must revoke again even if another tied alias remained fresh.
	_ = offerStatusAt(t, e, statusCommitPC)
	commitStatusReassertion(t, e, a, 0x120000, 16, 2, false)
	requireStatusReassertionInput(t, e, a, true, false)
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
	for offer := 2; offer <= 9; offer++ {
		commitStatusReassertion(t, e, a, 0x120000, 16, 2, false)
		requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), offer == 9)
	}
}

func TestOfferedStatusRenewalAfterPartlyExpiredOverlappingRanges(t *testing.T) {
	e, a := newStatusReassertionFixture(t, true)
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
	report := SSNMReport{Kind: SSNMSignallingCongestionReport, Source: SSNMPeerReport,
		Scope:           WireScope{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContextSet: true, RoutingContexts: []uint32{1}},
		Destinations:    []PointCodeRange{{PointCode: 0x120000, Mask: 16}, {PointCode: statusCommitPC}},
		CongestionLevel: 2, CongestionLevelSet: true}
	if err := a.publishSSNMReport(report); err != nil {
		t.Fatal(err)
	}
	// Both ranges are one statement revision. Expiring its broad copy must
	// remain visible when the equally new exact copy is still fresh.
	expired := false
	now := time.Now().Add(2 * time.Hour)
	for _, candidate := range e.ssnm.congestionExpiries(now) {
		if candidate.received.association == a.ID() && candidate.received.destination.pointCode == 0x120000 && candidate.received.destination.mask == 16 {
			expired = e.ssnm.expireCongestionStatement(candidate, now)
		}
	}
	if !expired {
		t.Fatal("broad copy of the tied statement did not expire")
	}
	partition := canonicalSSNMPartition("sg-a", "as-core")
	e.ssnm.mu.Lock()
	var revision uint64
	fresh, stale, revisionsEqual := 0, 0, true
	for key, record := range e.ssnm.partitions[partition].receivedCongestion {
		if key.association != a.ID() {
			continue
		}
		if revision != 0 && revision != record.Revision {
			revisionsEqual = false
		}
		revision = record.Revision
		if record.Freshness == CongestionExpired {
			stale++
		} else {
			fresh++
		}
	}
	e.ssnm.mu.Unlock()
	if fresh != 1 || stale != 1 || !revisionsEqual {
		t.Fatalf("overlapping inputs: fresh=%d expired=%d equal-revision=%v", fresh, stale, revisionsEqual)
	}
	_ = offerStatusAt(t, e, statusCommitPC)
	commitStatusReassertion(t, e, a, 0x120000, 16, 2, false)
	requireStatusReassertionInput(t, e, a, true, false)
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
	for offer := 2; offer <= 9; offer++ {
		commitStatusReassertion(t, e, a, 0x120000, 16, 2, false)
		requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), offer == 9)
	}
}

func TestOfferedStatusRemovingExpiredAliasPreservesFreshRenewalCadence(t *testing.T) {
	e, a := newStatusReassertionFixture(t, true)
	a.addDynamicASKey(*staticASKey(7, 8), RoutingKey{}, false)
	a.noteCanonicalRemoteAS(8, "as-core")
	a.noteRoutingContextsAcked(params.NewRoutingContext(1, 2, 8))
	a.notifyASPRouteStateChanged()
	drainMTPIndications(e.MTPIndications())
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
	report := SSNMReport{Kind: SSNMSignallingCongestionReport, Source: SSNMPeerReport,
		Scope:        WireScope{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContextSet: true, RoutingContexts: []uint32{1, 8}},
		Destinations: []PointCodeRange{{PointCode: statusCommitPC}}, CongestionLevel: 2, CongestionLevelSet: true}
	if err := a.publishSSNMReport(report); err != nil {
		t.Fatal(err)
	}
	expired := false
	for _, candidate := range e.ssnm.congestionExpiries(time.Now().Add(2 * time.Hour)) {
		if candidate.received.association == a.ID() && candidate.received.destination.pointCode == statusCommitPC && candidate.received.as.RoutingContext == 1 {
			expired = e.ssnm.expireCongestionStatement(candidate, time.Now().Add(2*time.Hour))
		}
	}
	if !expired {
		t.Fatal("first canonical alias did not expire")
	}
	_ = offerStatusAt(t, e, statusCommitPC)
	partition := canonicalSSNMPartition("sg-a", "as-core")
	e.ssnm.mu.Lock()
	removed, _ := e.ssnm.retireReceivedSourcesLocked(e.ssnm.partitions[partition], func(key receivedCongestionKey, record SSNMCongestion) bool {
		return key.association == a.ID() && key.destination.pointCode == statusCommitPC && key.destination.mask == 0 && record.AS.RoutingContext == 1
	}, false)
	e.ssnm.mu.Unlock()
	if len(removed) != 1 {
		t.Fatalf("removed %d inputs, want only the expired alias", len(removed))
	}
	requireStatusReassertionInput(t, e, a, true, false)
	// Consume the removal's conservative reminder. Its surviving tied alias is
	// fresh, so an equal renewal must keep the following eight-offer cadence.
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
	for offer := 2; offer <= 9; offer++ {
		commitStatusReassertion(t, e, a, 0x120000, 16, 2, false)
		requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), offer == 9)
	}
}
