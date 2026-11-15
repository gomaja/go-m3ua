package m3ua

import (
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
)

func TestOfferedStatusInitiallyInactiveAlternativeCommitsBeforeProjection(t *testing.T) {
	e, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: twoApplicationServerASPConfig(),
		Congestion: &CongestionConfig{Default: CongestionProfile{Method: CongestionNationalWithoutPriorities}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	e.ssnm.validity = time.Hour // Source retention without an automation worker.
	identity := SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}
	a, _ := attachMultiScopeASPAssociation(t, e, identity, 7, 1, 2)
	pending := attachActivatingSSNMAssociation(t, e, identity, 7, 2)
	a.noteRoutingContextsOverridden([]uint32{2})
	applyASPDRST(t, a, 7, 1, statusCommitPC, 0)
	applyASPSCON(t, a, 7, 1, statusCommitPC, 0, params.NewCongestionIndications(2))
	applyASPDAVA(t, pending, 7, 2, statusCommitPC, 0)
	applyASPSCON(t, pending, 7, 2, statusCommitPC, 0, params.NewCongestionIndications(0))
	drainMTPIndications(e.MTPIndications())
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)

	partition := canonicalSSNMPartition("sg-a", "as-backup")
	backupAuthorized := func() bool {
		e.ssnm.mu.Lock()
		defer e.ssnm.mu.Unlock()
		state := e.ssnm.partitions[partition]
		return state != nil && state.trafficAuthorized()
	}
	if backupAuthorized() {
		t.Fatal("backup AS must initially have only a pending binding")
	}
	r := e.aspRoutes
	r.mu.Lock()
	locked := true
	defer func() {
		if locked {
			r.mu.Unlock()
		}
	}()
	route := r.config.mtpRoutes[r.config.mtpRouteByID["sccp-a"]]
	assertAggregate := func(congested bool) {
		t.Helper()
		// Read-only recomputation confirms the intermediate effective state;
		// do not publish it or invoke interval retirement from this test seam.
		status, known := offeredDestinationStatus(route, statusCommitPC, r.recomputeMTPRouteLocked(route))
		if !known || status.congested != congested || status.receivedUnknown || status.transportUnknown || congested && status.congestionLevel != 2 {
			t.Fatalf("intermediate aggregate=%+v known=%v, want congestion=%v", status, known, congested)
		}
	}
	assertAggregate(true)
	activated, overridden := make(chan struct{}), make(chan struct{})
	go func() { a.noteRoutingContextsAcked(params.NewRoutingContext(2)); close(activated) }()
	waitStatusCommit(t, backupAuthorized)
	assertAggregate(false)
	go func() { a.noteRoutingContextsOverridden([]uint32{2}); close(overridden) }()
	waitStatusCommit(t, func() bool { return !backupAuthorized() })
	assertAggregate(true)
	r.mu.Unlock()
	locked = false
	for _, done := range []<-chan struct{}{activated, overridden} {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("alternative AS projection did not finish")
		}
	}
	drainMTPIndications(e.MTPIndications())
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
	for offer := 2; offer <= 9; offer++ {
		requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), offer == 9)
	}
}
