package m3ua

import (
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
)

func TestOfferedStatusTerminalWithdrawalCommitsBeforeBindingProjection(t *testing.T) {
	e, peers, _ := newASPTransferFixtureReporting(t, validASPConfig(), true,
		&CongestionConfig{Default: CongestionProfile{Method: CongestionNationalWithoutPriorities}})
	e.ssnm.validity = time.Hour
	a, b := peers["sg-a/sgp-a1"], peers["sg-b/sgp-b1"]
	applyASPSCON(t, a, 7, 1, statusCommitPC, 0, params.NewCongestionIndications(2))
	applyASPSCON(t, b, 9, 42, statusCommitPC, 0, params.NewCongestionIndications(2))
	drainMTPIndications(e.MTPIndications())
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
	// Commit the alternative's clear while the derived view still reports
	// congestion. The intervening offer consumes any preceding interval reset.
	err := e.ssnm.apply(SSNMReport{Kind: SSNMSignallingCongestionReport, Source: SSNMPeerReport,
		Association: b.ID(), Partition: canonicalSSNMPartition("sg-b", "as-core"),
		Scope:        WireScope{NetworkAppearance: 9, NetworkAppearanceSet: true, RoutingContexts: []uint32{42}, RoutingContextSet: true},
		Destinations: []PointCodeRange{{PointCode: statusCommitPC}}, CongestionLevelSet: true}, []ASKey{b.staticASKeyForRoutingContext(42, false)})
	if err != nil {
		t.Fatal(err)
	}
	r := e.aspRoutes
	r.mu.Lock()
	route := r.config.mtpRoutes[r.config.mtpRouteByID["sccp-a"]]
	status, known := offeredDestinationStatus(route, statusCommitPC, r.recomputeMTPRouteLocked(route))
	r.mu.Unlock()
	if !known || status.congested || status.receivedUnknown || status.transportUnknown {
		t.Fatalf("alternative did not clear effective congestion: %+v known=%v", status, known)
	}
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)

	// Close publishes Done before releasing transport or reconciling bindings.
	// Hold the transport seam while offering through the surviving peer.
	release := make(chan struct{})
	b.transportCloser = func() error { <-release; return nil }
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	closed := make(chan error, 1)
	go func() { closed <- b.Close() }()
	select {
	case <-b.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("terminal state was not published")
	}
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
	close(release)
	released = true
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("terminal binding reconciliation did not finish")
	}
}
