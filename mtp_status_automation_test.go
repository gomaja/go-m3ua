package m3ua

import (
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
)

func TestOfferedStatusAuditedAvailabilityCommitsBeforeProjection(t *testing.T) {
	for _, removal := range []bool{false, true} {
		name := "replacement"
		if removal {
			name = "retirement"
		}
		t.Run(name, func(t *testing.T) {
			e, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: twoApplicationServerASPConfig(), Congestion: &CongestionConfig{Default: CongestionProfile{Method: CongestionNationalWithoutPriorities}}})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = e.Close() })
			e.ssnm.validity = time.Hour
			a, _ := attachMultiScopeASPAssociation(t, e, SGPIdentity{"sg-a", "sgp-a1"}, 7, 1, 2)
			for _, pc := range []uint32{statusCommitPC, statusCommitOtherPC} {
				applyASPDRST(t, a, 7, 1, pc, 0)
				applyASPSCON(t, a, 7, 1, pc, 0, params.NewCongestionIndications(2))
				applyASPDUNA(t, a, 7, 2, pc, 0)
				applyASPSCON(t, a, 7, 2, pc, 0, params.NewCongestionIndications(0))
			}
			drainMTPIndications(e.MTPIndications())
			requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
			requireOfferedStatus(t, offerStatusAt(t, e, statusCommitOtherPC), true)
			// Exercise the audited commit without a worker or timer.
			e.ssnm.auditing = &DestinationAuditConfig{Period: time.Hour, MaxPending: 256, MaxMessagesPerSecond: 10}
			partition := canonicalSSNMPartition("sg-a", "as-backup")
			commit := func(kind SSNMReportKind) {
				t.Helper()
				as := a.staticASKeyForRoutingContext(2, false)
				if err := e.ssnm.apply(SSNMReport{Kind: kind, Source: SSNMPeerReport, Association: a.ID(), Partition: partition,
					Scope: testWireScope(7, true, 2), Destinations: []PointCodeRange{{PointCode: statusCommitPC}}}, []ASKey{as}); err != nil {
					t.Fatal(err)
				}
			}
			commit(SSNMDestinationAvailableReport)
			// Consume the clear's mark while its projection is still delayed.
			_ = offerStatusAt(t, e, statusCommitPC)
			if removal {
				e.ssnm.mu.Lock()
				state := e.ssnm.partitions[partition]
				var outcomes auditOutcomeBatch
				e.ssnm.retireReceivedAvailabilityLocked(state, func(key receivedCongestionKey, _ SSNMAvailability) bool {
					return key.destination.pointCode == statusCommitPC
				}, &outcomes)
				e.ssnm.mu.Unlock()
			} else {
				commit(SSNMDestinationUnavailableReport)
			}
			requireOfferedStatus(t, offerStatusAt(t, e, statusCommitOtherPC), false)
			requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
			for offer := 2; offer <= 9; offer++ {
				if !removal {
					commit(SSNMDestinationUnavailableReport)
				}
				requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), offer == 9)
			}
		})
	}
}

func TestOfferedStatusForgetScalarClearMarksBeforeProjection(t *testing.T) {
	e, peers, _ := newASPTransferFixtureReporting(t, oneSignallingGatewayTwoSGPConfig(RouteSelectionLoadshare), true,
		&CongestionConfig{Default: CongestionProfile{Method: CongestionNationalWithoutPriorities}})
	a, b := peers["sg-a/sgp-a1"], peers["sg-a/sgp-a2"]
	applyASPSCON(t, b, 7, 1, 0x120000, 16, params.NewCongestionIndications(2))
	drainMTPIndications(e.MTPIndications())
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
	partition := canonicalSSNMPartition("sg-a", "as-core")
	commitStatusReport(t, e, a, partition, statusCommitPC, SSNMSignallingCongestionReport, 0)
	if e.ssnm.destinationKnowledge(partition, statusCommitPC).congested {
		t.Fatal("narrow scalar clear was not effective")
	}
	_ = offerStatusAt(t, e, statusCommitPC)
	// This is ForgetDestinations' canonical commit, before its final projection.
	changed := a.forgetReceivedDestinations()
	if len(changed) == 0 || !e.ssnm.destinationKnowledge(partition, statusCommitPC).congested {
		t.Fatal("forget did not expose the sibling's retained broad pressure")
	}
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
	for offer := 2; offer <= 9; offer++ {
		if changed := a.forgetReceivedDestinations(); len(changed) != 0 {
			t.Fatal("empty forget changed retained knowledge")
		}
		requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), offer == 9)
	}
}

func TestOfferedStatusCanonicalAliasesMarkSharedRoute(t *testing.T) {
	config := oneSignallingGatewayTwoSGPConfig(RouteSelectionLoadshare)
	e, peers, _ := newASPTransferFixtureReporting(t, config, true,
		&CongestionConfig{Default: CongestionProfile{Method: CongestionNationalWithoutPriorities}})
	e.ssnm.validity = time.Hour
	a, b := peers["sg-a/sgp-a1"], peers["sg-a/sgp-a2"]
	partition := canonicalSSNMPartition("sg-a", "as-core")
	applyASPSCON(t, a, 7, 1, statusCommitPC, 0, params.NewCongestionIndications(2))
	applyASPSCON(t, b, 7, 1, statusCommitPC, 0, params.NewCongestionIndications(0))
	drainMTPIndications(e.MTPIndications())
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
	// The canonical projection deliberately does not filter source-local NA.
	// Exercise that same store boundary, including a captured wire alias that
	// differs from the configured route label.
	commit := func(level uint8) {
		t.Helper()
		if err := e.ssnm.apply(SSNMReport{Kind: SSNMSignallingCongestionReport, Source: SSNMPeerReport, Association: a.ID(), Partition: partition,
			Scope: testWireScope(9, true, 1), Destinations: []PointCodeRange{{PointCode: statusCommitPC}}, CongestionLevelSet: true, CongestionLevel: level}, []ASKey{a.staticASKeyForRoutingContext(1, false)}); err != nil {
			t.Fatal(err)
		}
	}
	commit(0)
	if e.ssnm.destinationKnowledge(partition, statusCommitPC).congested {
		t.Fatal("aliased clear was not canonical")
	}
	_ = offerStatusAt(t, e, statusCommitPC)
	commit(2)
	if !e.ssnm.destinationKnowledge(partition, statusCommitPC).congested {
		t.Fatal("aliased onset was not canonical")
	}
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
	for offer := 2; offer <= 9; offer++ {
		commit(2)
		requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), offer == 9)
	}
}

func TestOfferedStatusAvailabilityIndexFollowsRetentionAndEqualInputs(t *testing.T) {
	e, a, b, partition := newStatusCommitFixture(t)
	e.ssnm.auditing = &DestinationAuditConfig{Period: time.Hour, MaxPending: 256, MaxMessagesPerSecond: 10}
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
	commit := func(peer *Association) {
		t.Helper()
		commitStatusReport(t, e, peer, partition, statusCommitPC, SSNMDestinationAvailableReport, 0)
	}
	check := func() {
		t.Helper()
		e.ssnm.mu.Lock()
		defer e.ssnm.mu.Unlock()
		for _, state := range e.ssnm.partitions {
			if state.availabilityProjection == nil {
				if len(state.receivedAvailability) != 0 {
					t.Fatal("retained availability lacks its prefix index")
				}
				continue
			}
			index := state.availabilityProjection
			links := 0
			for _, bucket := range index.prefixes {
				links += bucket.Len()
			}
			if len(index.records) != len(state.receivedAvailability) || links > 2*len(index.records) || links < len(index.records) || len(index.prefixes) > links {
				t.Fatalf("availability index retained=%d records=%d links=%d buckets=%d", len(state.receivedAvailability), len(index.records), links, len(index.prefixes))
			}
		}
	}
	for i := 0; i < 32; i++ {
		commit(a)
		commit(b)
		check()
	}
	// A still supplies the current DAVA when B's scalar and source are removed.
	e.ssnm.mu.Lock()
	var outcomes auditOutcomeBatch
	e.ssnm.retireReceivedAvailabilityLocked(e.ssnm.partitions[partition], func(key receivedCongestionKey, _ SSNMAvailability) bool { return key.association == b.ID() }, &outcomes)
	e.ssnm.mu.Unlock()
	check()
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
	for offer := 2; offer <= 9; offer++ {
		commit(a)
		check()
		requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), offer == 9)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	check()
}
