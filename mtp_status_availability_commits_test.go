package m3ua

import (
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
)

func TestOfferedStatusAvailabilitySelectionCommitsBeforeProjection(t *testing.T) {
	e, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: twoApplicationServerASPConfig(), Congestion: &CongestionConfig{Default: CongestionProfile{Method: CongestionNationalWithoutPriorities}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	e.ssnm.validity = time.Hour
	a, _ := attachMultiScopeASPAssociation(t, e, SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}, 7, 1, 2)
	for _, pc := range []uint32{statusCommitPC, statusCommitOtherPC} {
		applyASPDRST(t, a, 7, 1, pc, 0)
		applyASPSCON(t, a, 7, 1, pc, 0, params.NewCongestionIndications(2))
		applyASPDUNA(t, a, 7, 2, pc, 0)
		applyASPSCON(t, a, 7, 2, pc, 0, params.NewCongestionIndications(0))
	}
	drainMTPIndications(e.MTPIndications())
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitOtherPC), true)
	r := e.aspRoutes
	route := r.config.mtpRoutes[r.config.mtpRouteByID["sccp-a"]]
	assertCongestion := func(expected bool) {
		t.Helper()
		r.mu.Lock()
		defer r.mu.Unlock()
		status, known := offeredDestinationStatus(route, statusCommitPC, r.recomputeMTPRouteLocked(route))
		if !known || status.congested != expected {
			t.Fatalf("selection congestion=%+v, want %v", status, expected)
		}
	}
	commit := func(kind SSNMReportKind) {
		t.Helper()
		err := e.ssnm.apply(SSNMReport{Kind: kind, Source: SSNMPeerReport, Association: a.ID(), Partition: canonicalSSNMPartition("sg-a", "as-backup"), Scope: WireScope{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContextSet: true, RoutingContexts: []uint32{2}}, Destinations: []PointCodeRange{{PointCode: statusCommitPC}}}, []ASKey{a.staticASKeyForRoutingContext(2, false)})
		if err != nil {
			t.Fatal(err)
		}
	}
	assertCongestion(true)
	commit(SSNMDestinationAvailableReport)
	assertCongestion(false)
	commit(SSNMDestinationUnavailableReport)
	assertCongestion(true)
	r.canonicalStateChanged([]SSNMPartition{canonicalSSNMPartition("sg-a", "as-backup")})
	drainMTPIndications(e.MTPIndications())
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitOtherPC), false)
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
	for offer := 2; offer <= 9; offer++ {
		requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), offer == 9)
	}
}
