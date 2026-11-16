package m3ua

import (
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
)

func TestOfferedStatusEqualBroadRenewalWorkIsIndependentOfHistory(t *testing.T) {
	e, peers := newOfferedStatusFixture(t, CongestionNationalWithoutPriorities, RouteSelectionLoadshare)
	e.ssnm.validity = time.Hour
	a := peers["sg-a/sgp-a1"]
	// Replace the fixture's scalar reports with received source statements.
	for _, peer := range peers {
		applyASPSCON(t, peer, 7, 1, 0x120000, 16, params.NewCongestionIndications(2))
	}
	report := SSNMReport{Kind: SSNMSignallingCongestionReport, Source: SSNMPeerReport,
		Association: a.ID(), Partition: canonicalSSNMPartition("sg-a", "as-core"),
		Scope:        WireScope{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContextSet: true, RoutingContexts: []uint32{1}},
		Destinations: []PointCodeRange{{PointCode: 0x120000, Mask: 16}}, CongestionLevel: 2, CongestionLevelSet: true}
	as := []ASKey{a.staticASKeyForRoutingContext(1, false)}
	// Synthetic independent reporters exercise the retained-history dimension,
	// without creating sockets or associating extra transport lifetimes.
	for i := 0; i < 998; i++ {
		source := AssociationID(1000000 + i)
		e.ssnm.registerSource(source)
		next := report
		next.Association = source
		if err := e.ssnm.apply(next, as); err != nil {
			t.Fatal(err)
		}
	}
	e.ssnm.mu.Lock()
	histories := len(e.ssnm.partitions[report.Partition].receivedCongestion)
	e.ssnm.mu.Unlock()
	if histories != 1000 {
		t.Fatalf("history records=%d, want 1000", histories)
	}
	drainMTPIndications(e.MTPIndications())
	for pc := uint32(0x120000); pc < 0x121000; pc++ {
		e.aspRoutes.mu.Lock()
		route := e.aspRoutes.config.mtpRoutes[e.aspRoutes.config.mtpRouteByID["sccp-a"]]
		e.aspRoutes.offerMTPTransferLocked(route, MTPTransferRequest{ProtocolData: transferProtocolData(pc, 0, []byte("offer"))})
		e.aspRoutes.mu.Unlock()
		requireOfferedStatus(t, takeOfferedIndications(e.MTPIndications()), true)
	}
	if len(e.aspRoutes.statusProcedures) != 4096 {
		t.Fatalf("intervals=%d, want 4096", len(e.aspRoutes.statusProcedures))
	}
	var applyErr error
	allocations := testing.AllocsPerRun(1, func() { applyErr = e.ssnm.apply(report, as) })
	if applyErr != nil {
		t.Fatal(applyErr)
	}
	t.Logf("equal broad renewal: entries=4096 covering_source_records=1000 allocations=%.0f", allocations)
	// Publication may build the change's own bounded subscriber delta. It must
	// not build a temporary aggregate map for every watched exact destination.
	if allocations > 256 {
		t.Fatalf("equal broad renewal allocated %.0f objects under store commit, want <=256 independent of watched destinations times history", allocations)
	}
	requireOfferedStatus(t, offerStatusAt(t, e, 0x120000), false)
}
