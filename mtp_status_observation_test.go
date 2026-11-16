package m3ua

import (
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
)

func TestOfferedStatusObservationsFollowLRUBudgetAndClose(t *testing.T) {
	config := oneSignallingGatewayTwoSGPConfig(RouteSelectionLoadshare)
	e, peers, _ := newASPTransferFixtureReporting(t, config, true, &CongestionConfig{Default: CongestionProfile{Method: CongestionNationalWithoutPriorities}, MaxStatusDestinations: 2})
	e.ssnm.validity = time.Hour
	for _, a := range peers {
		applyASPSCON(t, a, 7, 1, 0x120000, 16, params.NewCongestionIndications(2))
	}
	drainMTPIndications(e.MTPIndications())
	for _, pc := range []uint32{statusCommitPC, statusCommitOtherPC, statusCommitOtherPC + 1} {
		requireOfferedStatus(t, offerStatusAt(t, e, pc), true)
	}
	e.aspRoutes.statusMarks.mu.Lock()
	count := len(e.aspRoutes.statusMarks.entries)
	e.aspRoutes.statusMarks.mu.Unlock()
	if count > 2 {
		t.Fatalf("LRU eviction retained %d canonical observations, limit 2", count)
	}

	// Changes to the evicted exact DPC must not reset either retained interval.
	for _, a := range peers {
		applyASPSCON(t, a, 7, 1, statusCommitPC, 0, params.NewCongestionIndications(0))
	}
	for _, a := range peers {
		applyASPSCON(t, a, 7, 1, statusCommitPC, 0, params.NewCongestionIndications(2))
	}
	drainMTPIndications(e.MTPIndications())
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitOtherPC), false)
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitOtherPC+1), false)
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e.aspRoutes.statusMarks.mu.Lock()
	count = len(e.aspRoutes.statusMarks.entries)
	e.aspRoutes.statusMarks.mu.Unlock()
	if count != 0 {
		t.Fatalf("Close retained %d canonical observations", count)
	}

}

func TestOfferedStatusExpiryProjectionAfterObservedCommitRestarts(t *testing.T) {
	e, peers, _ := newASPTransferFixtureReporting(t, validASPConfig(), true, &CongestionConfig{Default: CongestionProfile{Method: CongestionNationalWithoutPriorities}})
	now := time.Now()
	e.ssnm.validity = 10 * time.Second
	e.ssnm.now = func() time.Time { return now }
	a, b := peers["sg-a/sgp-a1"], peers["sg-b/sgp-b1"]
	applyASPSCON(t, a, 7, 1, statusCommitPC, 0, params.NewCongestionIndications(2))
	now = now.Add(5 * time.Second)
	applyASPSCON(t, b, 9, 42, statusCommitPC, 0, params.NewCongestionIndications(3))
	drainMTPIndications(e.MTPIndications())
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
	now = now.Add(6 * time.Second)
	changed := e.ssnm.expireCongestion(now)
	if len(changed) != 1 {
		t.Fatalf("expired partitions=%v", changed)
	}
	// An early offer consumes the commit witness using the old projected level.
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
	e.aspRoutes.canonicalStateChanged(changed, MTPStatusValidityExpired)
	got := takeOfferedIndications(e.MTPIndications())
	if len(got) != 1 || got[0].Reason != MTPStatusValidityExpired || got[0].Cause != MTPStatusCauseNone || got[0].Destination.CongestionLevel != 3 {
		t.Fatalf("expiry notification=%+v", got)
	}
	// The actual expiry notification is another reset at the MTP3-User boundary.
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
}

func TestOfferedStatusConcurrentCanonicalCommitsAndLRURetirement(t *testing.T) {
	config := oneSignallingGatewayTwoSGPConfig(RouteSelectionLoadshare)
	config.MTPIndicationQueueSize = 512
	e, peers, _ := newASPTransferFixtureReporting(t, config, true, &CongestionConfig{Default: CongestionProfile{Method: CongestionNationalWithoutPriorities}, MaxStatusDestinations: 8})
	e.ssnm.validity = time.Hour
	a := peers["sg-a/sgp-a1"]
	applyASPSCON(t, a, 7, 1, 0x120000, 16, params.NewCongestionIndications(2))
	drainMTPIndications(e.MTPIndications())
	report := SSNMReport{Kind: SSNMSignallingCongestionReport, Source: SSNMPeerReport, Association: a.ID(), Partition: canonicalSSNMPartition("sg-a", "as-core"), Scope: WireScope{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContextSet: true, RoutingContexts: []uint32{1}}, Destinations: []PointCodeRange{{PointCode: 0x120000, Mask: 16}}, CongestionLevelSet: true}
	failures := make(chan error, 2)
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		for i := 0; i < 200; i++ {
			report.CongestionLevel = uint8(i%2) * 2
			if err := e.ssnm.apply(report, []ASKey{a.staticASKeyForRoutingContext(1, false)}); err != nil {
				failures <- err
				return
			}
		}
	}()
	go func() {
		defer workers.Done()
		for i := 0; i < 200; i++ {
			if _, err := e.MTPTransfer(MTPTransferRequest{ProtocolData: transferProtocolData(statusCommitPC+uint32(i%16), 0, []byte("offer"))}); err != nil {
				failures <- err
				return
			}
		}
	}()
	workers.Wait()
	close(failures)
	for err := range failures {
		t.Fatal(err)
	}
	e.aspRoutes.statusMarks.mu.Lock()
	count := len(e.aspRoutes.statusMarks.entries)
	e.aspRoutes.statusMarks.mu.Unlock()
	if count > 8 {
		t.Fatalf("concurrent LRU left %d observations, limit 8", count)
	}
	// A settled ending/onset after the concurrent phase must start an interval.
	drainMTPIndications(e.MTPIndications())
	commitStatusReport(t, e, a, report.Partition, statusCommitPC, SSNMSignallingCongestionReport, 0)
	commitStatusReport(t, e, a, report.Partition, statusCommitPC, SSNMSignallingCongestionReport, 2)
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
}

func TestOfferedStatusInactivePeerStateCommitsPreserveInterval(t *testing.T) {
	e, _, b, _ := newStatusCommitFixture(t)
	b.setState(StateASPInactive)
	drainMTPIndications(e.MTPIndications())
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
	for offer := 2; offer <= 9; offer++ {
		if !b.commitState(StateASPDown) || !b.commitState(StateASPInactive) {
			t.Fatal("inactive peer state commit rejected")
		}
		drainMTPIndications(e.MTPIndications())
		requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), offer == 9)
	}
}

func TestOfferedStatusCanonicalAppearanceReportAndExpiryRestartInterval(t *testing.T) {
	e, a, _, partition := newStatusCommitFixture(t)
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
	// The configured SG/AS view includes source-local wire labels.
	err := e.ssnm.apply(SSNMReport{Kind: SSNMSignallingCongestionReport, Source: SSNMPeerReport, Association: a.ID(), Partition: partition, Scope: WireScope{NetworkAppearance: 9, NetworkAppearanceSet: true, RoutingContextSet: true, RoutingContexts: []uint32{1}}, Destinations: []PointCodeRange{{PointCode: statusCommitPC}}, CongestionLevel: 3, CongestionLevelSet: true}, []ASKey{a.staticASKeyForRoutingContext(1, false)})
	if err != nil {
		t.Fatal(err)
	}
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
	now := time.Now().Add(2 * time.Hour)
	for _, candidate := range e.ssnm.congestionExpiries(now) {
		e.ssnm.mu.Lock()
		record := e.ssnm.partitions[partition].receivedCongestion[candidate.received]
		e.ssnm.mu.Unlock()
		if record.Scope.NetworkAppearance == 9 {
			if !e.ssnm.expireCongestionStatement(candidate, now) {
				t.Fatal("canonical statement did not expire")
			}
		}
	}
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
	for offer := 2; offer <= 9; offer++ {
		requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), offer == 9)
	}
}

func TestOfferedStatusInactivePeerScopeCleanupPreservesInterval(t *testing.T) {
	e, _, b, _ := newStatusCommitFixture(t)
	b.setState(StateASPInactive)
	b.noteNoRoutingContextsAcked()
	drainMTPIndications(e.MTPIndications())
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
	// A down/inactive peer's old activation cleanup changes no usable route.
	b.forgetAckedRoutingContexts()
	drainMTPIndications(e.MTPIndications())
	for offer := 2; offer <= 9; offer++ {
		requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), offer == 9)
	}
}
