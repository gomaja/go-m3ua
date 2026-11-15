package m3ua

import (
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
)

func TestSSNMAvailabilityProjectionUsesTheSharedInputIndex(t *testing.T) {
	s, partition, work := newHistoryWorkStore(t)
	s.statusMarks = &mtpStatusMarks{} // Inert registry; shared inputs exist before any offer.
	keys := []ASKey{*staticASKey(7, 1)}
	for id := AssociationID(1); id <= 2; id++ {
		historyWorkReport(t, s, partition, id, SSNMDestinationAvailableReport, keys, historyWorkDestinations(1000, 256))
	}
	*work = ssnmWorkCount{}
	for pc := uint32(1000); pc < 1032; pc++ {
		status := s.destinationKnowledge(partition, pc)
		if !status.availabilitySet || status.availability != DestinationAvailable {
			t.Fatal("shared availability lookup lost a retained statement", status)
		}
	}
	t.Logf("availability lookup work: %+v", *work)
	if work.sourceRows != 0 || work.prefixRows > 32*25 {
		t.Fatalf("availability projection rebuilt a second history index: %+v", *work)
	}
}

func TestSSNMAvailabilityChangesPreserveTheCongestionProjection(t *testing.T) {
	s, partition, work := newHistoryWorkStore(t)
	keys := []ASKey{*staticASKey(7, 1)}
	historyWorkSeed(t, s, partition, 256)
	_ = s.destinationKnowledge(partition, 1000)
	*work = ssnmWorkCount{}
	for pc := uint32(1000); pc < 1032; pc++ {
		historyWorkReport(t, s, partition, 1, SSNMDestinationAvailableReport, keys, []PointCodeRange{{PointCode: pc}})
		status := s.destinationKnowledge(partition, pc)
		if !status.availabilitySet || status.availability != DestinationAvailable || !status.congested {
			t.Fatal("availability and congestion were not projected independently", status)
		}
	}
	t.Logf("availability changes with warm congestion: %+v", *work)
	if work.sourceRows != 0 || work.knowledgeRows != 0 || work.prefixRows > 32*50 {
		t.Fatalf("availability-only changes rebuilt unrelated congestion history: %+v", *work)
	}
}

func TestOfferedStatusSharedAvailabilityWorkFollowsTheEntryBudget(t *testing.T) {
	e, peers := newOfferedStatusFixture(t, CongestionNationalWithoutPriorities, RouteSelectionLoadshare)
	e.ssnm.auditing = &DestinationAuditConfig{Period: time.Hour, MaxPending: 256, MaxMessagesPerSecond: 10}
	a := peers["sg-a/sgp-a1"]
	for _, peer := range peers {
		applyASPSCON(t, peer, 7, 1, 0x120000, 16, params.NewCongestionIndications(2))
	}
	partition := canonicalSSNMPartition("sg-a", "as-core")
	report := SSNMReport{Kind: SSNMDestinationAvailableReport, Source: SSNMPeerReport,
		Association: a.ID(), Partition: partition, Scope: testWireScope(7, true, 1),
		Destinations: []PointCodeRange{{PointCode: 0x120000, Mask: 16}}}
	keys := []ASKey{a.staticASKeyForRoutingContext(1, false)}
	for i := 0; i < 1000; i++ {
		next := report
		if i != 0 {
			next.Association = AssociationID(1000000 + i)
			e.ssnm.registerSource(next.Association)
		}
		if err := e.ssnm.apply(next, keys); err != nil {
			t.Fatal(err)
		}
	}
	drainMTPIndications(e.MTPIndications())
	for pc := uint32(0x120000); pc < 0x121000; pc++ {
		e.aspRoutes.mu.Lock()
		route := e.aspRoutes.config.mtpRoutes[e.aspRoutes.config.mtpRouteByID["sccp-a"]]
		e.aspRoutes.offerMTPTransferLocked(route, MTPTransferRequest{ProtocolData: transferProtocolData(pc, 0, nil)})
		e.aspRoutes.mu.Unlock()
		requireOfferedStatus(t, takeOfferedIndications(e.MTPIndications()), true)
	}
	work := &ssnmWorkCount{}
	e.ssnm.mu.Lock()
	e.ssnm.work = work
	e.ssnm.partitions[partition].work = work
	// Mark comparison must not trigger even one cold congestion-history build.
	e.ssnm.partitions[partition].sourceProjection = nil
	e.ssnm.mu.Unlock()
	if err := e.ssnm.apply(report, keys); err != nil {
		t.Fatal(err)
	}
	t.Logf("4096 intervals / 1000 availability sources: %+v", *work)
	if work.sourceRows != 0 || work.knowledgeRows != 0 || work.prefixRows > 4096*25 {
		t.Fatalf("availability marks or unsubscribed deltas scan history: %+v", *work)
	}
	requireOfferedStatus(t, offerStatusAt(t, e, 0x120000), false)
}
