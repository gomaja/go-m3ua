package m3ua

import (
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
)

func TestOfferedStatusLaterAttachedAlternativeCommitsBeforeProjection(t *testing.T) {
	for _, consumePending := range []bool{false, true} {
		name := "without-intermediate-offer"
		if consumePending {
			name = "offer-after-pending"
		}
		t.Run(name, func(t *testing.T) { testLaterAttachedStatusAlternative(t, consumePending) })
	}
}

func testLaterAttachedStatusAlternative(t *testing.T, consumePending bool) {
	t.Helper()
	e, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: validASPConfig(),
		Congestion: &CongestionConfig{Default: CongestionProfile{Method: CongestionNationalWithoutPriorities}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	e.ssnm.validity = time.Hour
	a, _ := attachMultiScopeASPAssociation(t, e, SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}, 7, 1)
	applyASPDAVA(t, a, 7, 1, statusCommitPC, 0)
	applyASPSCON(t, a, 7, 1, statusCommitPC, 0, params.NewCongestionIndications(2))
	drainMTPIndications(e.MTPIndications())
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)

	// This provisioned alternative was absent when the first interval began.
	b := attachActivatingSSNMAssociation(t, e, SGPIdentity{SignallingGateway: "sg-b", SignallingGatewayProcess: "sgp-b1"}, 9, 42)
	applyASPDAVA(t, b, 9, 42, statusCommitPC, 0)
	applyASPSCON(t, b, 9, 42, statusCommitPC, 0, params.NewCongestionIndications(0))
	drainMTPIndications(e.MTPIndications())
	if consumePending {
		// Consume any attachment or pending-report reset before the later
		// activation and withdrawal commit their separate clear/onset pair.
		_ = offerStatusAt(t, e, statusCommitPC)
	}

	partition := canonicalSSNMPartition("sg-b", "as-core")
	authorized := func() bool {
		e.ssnm.mu.Lock()
		defer e.ssnm.mu.Unlock()
		state := e.ssnm.partitions[partition]
		return state != nil && state.trafficAuthorized()
	}
	if authorized() {
		t.Fatal("pending alternative unexpectedly carries traffic")
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
	assertCongestion := func(want bool) {
		t.Helper()
		status, known := offeredDestinationStatus(route, statusCommitPC, r.recomputeMTPRouteLocked(route))
		if !known || status.congested != want || status.receivedUnknown || status.transportUnknown {
			t.Fatalf("effective status=%+v known=%v, want congestion=%v", status, known, want)
		}
	}
	assertCongestion(true)
	activated, withdrawn := make(chan bool, 1), make(chan bool, 1)
	go func() { activated <- b.commitState(StateASPActive) }()
	waitStatusCommit(t, authorized)
	assertCongestion(false)
	go func() { withdrawn <- b.commitState(StateASPInactive) }()
	waitStatusCommit(t, func() bool { return !authorized() })
	assertCongestion(true)
	r.mu.Unlock()
	locked = false
	for _, done := range []<-chan bool{activated, withdrawn} {
		select {
		case accepted := <-done:
			if !accepted {
				t.Fatal("alternative transition rejected")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("alternative projection did not finish")
		}
	}
	drainMTPIndications(e.MTPIndications())
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
	for offer := 2; offer <= 9; offer++ {
		requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), offer == 9)
	}
}
