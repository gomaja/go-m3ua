package m3ua

import (
	"errors"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
)

const statusCommitPC, statusCommitOtherPC = uint32(0x123456), uint32(0x123457)

func newStatusCommitFixture(t *testing.T) (*Endpoint, *Association, *Association, SSNMPartition) {
	t.Helper()
	e, peers, _ := newASPTransferFixtureReporting(t, oneSignallingGatewayTwoSGPConfig(RouteSelectionLoadshare), true,
		&CongestionConfig{Default: CongestionProfile{Method: CongestionNationalWithoutPriorities}})
	// Enable source retention without starting an automation worker or socket.
	e.ssnm.validity = time.Hour
	a, b := peers["sg-a/sgp-a1"], peers["sg-a/sgp-a2"]
	applyASPSCON(t, a, 7, 1, statusCommitPC, 0, params.NewCongestionIndications(2))
	applyASPSCON(t, b, 7, 1, statusCommitPC, 0, params.NewCongestionIndications(0))
	applyASPSCON(t, a, 7, 1, statusCommitOtherPC, 0, params.NewCongestionIndications(0))
	applyASPSCON(t, b, 7, 1, statusCommitOtherPC, 0, params.NewCongestionIndications(2))
	drainMTPIndications(e.MTPIndications())
	return e, a, b, canonicalSSNMPartition("sg-a", "as-core")
}

// Commit at the canonical store boundary without invoking the preceding legacy
// projection. This isolates the gap before the final canonical projection.
func commitStatusReport(t *testing.T, e *Endpoint, a *Association, partition SSNMPartition, pc uint32, kind SSNMReportKind, level uint8) {
	t.Helper()
	as := a.staticASKeyForRoutingContext(1, false)
	err := e.ssnm.apply(SSNMReport{Kind: kind, Source: SSNMPeerReport, Association: a.ID(), Partition: partition,
		Scope:        WireScope{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContexts: []uint32{1}, RoutingContextSet: true},
		Destinations: []PointCodeRange{{PointCode: pc}}, CongestionLevel: level, CongestionLevelSet: true}, []ASKey{as})
	if err != nil {
		t.Fatal(err)
	}
}

func requireStatusCommitRestart(t *testing.T, e *Endpoint, partition SSNMPartition, projectFirst, preserveOther bool) {
	t.Helper()
	if projectFirst {
		e.aspRoutes.canonicalStateChanged([]SSNMPartition{partition})
		drainMTPIndications(e.MTPIndications())
	}
	if preserveOther {
		requireOfferedStatus(t, offerStatusAt(t, e, statusCommitOtherPC), false)
	}
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
	if !projectFirst {
		e.aspRoutes.canonicalStateChanged([]SSNMPartition{partition})
		drainMTPIndications(e.MTPIndications())
	}
	for offer := 2; offer <= 9; offer++ {
		requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), offer == 9)
	}
	if preserveOther {
		for offer := 3; offer <= 9; offer++ {
			requireOfferedStatus(t, offerStatusAt(t, e, statusCommitOtherPC), offer == 9)
		}
	}
}

func TestOfferedStatusExpiryRenewalCommitsBeforeProjection(t *testing.T) {
	for _, projectFirst := range []bool{false, true} {
		name := "offer-first"
		if projectFirst {
			name = "projection-first"
		}
		t.Run(name, func(t *testing.T) {
			e, a, _, partition := newStatusCommitFixture(t)
			requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
			requireOfferedStatus(t, offerStatusAt(t, e, statusCommitOtherPC), true)
			now := time.Now().Add(2 * time.Hour)
			committed := false
			for _, candidate := range e.ssnm.congestionExpiries(now) {
				if candidate.received.association == a.ID() && candidate.received.destination.pointCode == statusCommitPC {
					committed = e.ssnm.expireCongestionStatement(candidate, now)
				}
			}
			if !committed || !e.ssnm.destinationKnowledge(partition, statusCommitPC, a.staticASKeyForRoutingContext(1, false)).receivedUnknown {
				t.Fatal("expiry did not commit uncertainty before renewal")
			}
			commitStatusReport(t, e, a, partition, statusCommitPC, SSNMSignallingCongestionReport, 2)
			requireStatusCommitRestart(t, e, partition, projectFirst, true)
		})
	}
}

func TestOfferedStatusSourceRetirementReplacementBeforeProjection(t *testing.T) {
	for _, projectFirst := range []bool{false, true} {
		name := "offer-first"
		if projectFirst {
			name = "projection-first"
		}
		t.Run(name, func(t *testing.T) {
			e, a, _, partition := newStatusCommitFixture(t)
			requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
			requireOfferedStatus(t, offerStatusAt(t, e, statusCommitOtherPC), true)
			e.ssnm.retire(partition, a.ID())
			if e.ssnm.destinationKnowledge(partition, statusCommitPC, a.staticASKeyForRoutingContext(1, false)).congested {
				t.Fatal("retirement did not end the exact destination's congestion")
			}
			if err := e.ssnm.bind(partition, a.ID(), false); err != nil {
				t.Fatal(err)
			}
			commitStatusReport(t, e, a, partition, statusCommitPC, SSNMSignallingCongestionReport, 2)
			requireStatusCommitRestart(t, e, partition, projectFirst, true)
		})
	}
}

func TestOfferedStatusScopedInvalidationRenewalBeforeProjection(t *testing.T) {
	e, a, _, partition := newStatusCommitFixture(t)
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
	scope := WireScope{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContexts: []uint32{1}, RoutingContextSet: true}
	refusal := a.captureSSNMDestinationRefusal(scope, false)
	if err := e.ssnm.oversized([]SSNMPartition{partition}, 2, 1, a.ID(), refusal); !errors.Is(err, ErrSSNMOversizedReport) {
		t.Fatal(err)
	}
	if e.ssnm.destinationKnowledge(partition, statusCommitPC, a.staticASKeyForRoutingContext(1, false)).congested {
		t.Fatal("scoped invalidation did not remove congestion")
	}
	commitStatusReport(t, e, a, partition, statusCommitPC, SSNMDestinationAvailableReport, 0)
	commitStatusReport(t, e, a, partition, statusCommitPC, SSNMSignallingCongestionReport, 2)
	requireStatusCommitRestart(t, e, partition, true, false)
}

func TestOfferedStatusOnsetAfterOfferDuringUnprojectedClear(t *testing.T) {
	for _, existing := range []bool{false, true} {
		name := "first-entry"
		if existing {
			name = "existing-entry"
		}
		t.Run(name, func(t *testing.T) {
			e, a, _, partition := newStatusCommitFixture(t)
			if existing {
				requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
			}
			commitStatusReport(t, e, a, partition, statusCommitPC, SSNMSignallingCongestionReport, 0)
			if e.ssnm.destinationKnowledge(partition, statusCommitPC, a.staticASKeyForRoutingContext(1, false)).congested {
				t.Fatal("canonical clear did not commit")
			}
			// The offer can install or consume an entry while the derived view is
			// still positive. A later onset must independently invalidate it.
			_ = offerStatusAt(t, e, statusCommitPC)
			commitStatusReport(t, e, a, partition, statusCommitPC, SSNMSignallingCongestionReport, 2)
			requireStatusCommitRestart(t, e, partition, true, false)
		})
	}
}

func TestOfferedStatusChangedPositiveLevelCommitsRestartConservatively(t *testing.T) {
	e, a, _, partition := newStatusCommitFixture(t)
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitOtherPC), true)
	commitStatusReport(t, e, a, partition, statusCommitPC, SSNMSignallingCongestionReport, 3)
	commitStatusReport(t, e, a, partition, statusCommitPC, SSNMSignallingCongestionReport, 2)
	requireStatusCommitRestart(t, e, partition, true, true)
}

func TestOfferedStatusEqualCanonicalCommitsAndStaleExpiryPreserveInterval(t *testing.T) {
	e, a, _, partition := newStatusCommitFixture(t)
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
	now := time.Now().Add(2 * time.Hour)
	candidates := e.ssnm.congestionExpiries(now)
	for offer := 2; offer <= 9; offer++ {
		commitStatusReport(t, e, a, partition, statusCommitPC, SSNMSignallingCongestionReport, 2)
		for _, candidate := range candidates {
			if candidate.received.association == a.ID() && candidate.received.destination.pointCode == statusCommitPC && e.ssnm.expireCongestionStatement(candidate, now) {
				t.Fatal("stale expiry candidate replaced a renewal")
			}
		}
		e.aspRoutes.canonicalStateChanged([]SSNMPartition{partition})
		drainMTPIndications(e.MTPIndications())
		requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), offer == 9)
	}
}

func TestOfferedStatusLifecycleCommitsBeforeBindingProjection(t *testing.T) {
	e, a, _, _ := newStatusCommitFixture(t)
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
	// Binding reconciliation first reads configured/dynamic AS keys. Hold that
	// read boundary while the production state commits retire and restore the
	// capability, so both callbacks later see the same active configuration.
	a.muDynamicASKeys.Lock()
	locked := true
	defer func() {
		if locked {
			a.muDynamicASKeys.Unlock()
		}
	}()
	inactive, active := make(chan bool, 1), make(chan bool, 1)
	go func() { inactive <- a.commitState(StateASPInactive) }()
	waitStatusCommit(t, func() bool { return a.State() == StateASPInactive })
	go func() { active <- a.commitState(StateASPActive) }()
	waitStatusCommit(t, func() bool { return a.State() == StateASPActive })
	a.muDynamicASKeys.Unlock()
	locked = false
	for _, done := range []<-chan bool{inactive, active} {
		select {
		case committed := <-done:
			if !committed {
				t.Fatal("lifecycle commit was rejected")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("lifecycle projection did not complete")
		}
	}
	drainMTPIndications(e.MTPIndications())
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
}
