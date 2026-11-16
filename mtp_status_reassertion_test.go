package m3ua

import (
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
)

func newStatusReassertionFixture(t *testing.T, validity bool) (*Endpoint, *Association) {
	t.Helper()
	config := twoApplicationServerASPConfig()
	config.Routing.Paths[0].ApplicationServers = []RemoteASID{"as-core"}
	e, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: config,
		Congestion: &CongestionConfig{Default: CongestionProfile{Method: CongestionNationalWithoutPriorities}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	if validity {
		e.ssnm.validity = time.Hour
	}
	a, _ := attachMultiScopeASPAssociation(t, e, SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}, 7, 1, 2)
	applyASPDAVA(t, a, 7, 1, 0x120000, 16)
	applyASPSCON(t, a, 7, 1, 0x120000, 16, params.NewCongestionIndications(2))
	drainMTPIndications(e.MTPIndications())
	return e, a
}

func commitStatusReassertion(t *testing.T, e *Endpoint, a *Association, pc uint32, mask, level uint8, grouped bool) {
	t.Helper()
	contexts := []uint32{1}
	if grouped {
		contexts = []uint32{1, 2}
	}
	// A grouped report resolves to both configured ASes; this is its canonical
	// as-core commit, with its original wire scope and resolved core key.
	err := e.ssnm.apply(SSNMReport{Kind: SSNMSignallingCongestionReport, Source: SSNMPeerReport,
		Association: a.ID(), Partition: canonicalSSNMPartition("sg-a", "as-core"),
		Scope:        WireScope{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContexts: contexts, RoutingContextSet: true},
		Destinations: []PointCodeRange{{PointCode: pc, Mask: mask}}, CongestionLevel: level, CongestionLevelSet: true},
		[]ASKey{a.staticASKeyForRoutingContext(1, false)})
	if err != nil {
		t.Fatal(err)
	}
}

func requireStatusReassertionInput(t *testing.T, e *Endpoint, a *Association, congested, unknown bool) {
	t.Helper()
	status := e.ssnm.destinationKnowledge(canonicalSSNMPartition("sg-a", "as-core"), statusCommitPC, a.staticASKeyForRoutingContext(1, false))
	if status.congested != congested || status.receivedUnknown != unknown {
		t.Fatalf("canonical input=%+v, want congested=%v unknown=%v", status, congested, unknown)
	}
}

func TestOfferedStatusEqualBroadReassertionSupersedesNewerClear(t *testing.T) {
	for _, validity := range []bool{false, true} {
		name := "scalar"
		if validity {
			name = "received-validity"
		}
		t.Run(name, func(t *testing.T) {
			for _, grouped := range []bool{false, true} {
				scope := "same-scope"
				if grouped {
					scope = "grouped-scope-alias"
				}
				t.Run(scope, func(t *testing.T) {
					e, a := newStatusReassertionFixture(t, validity)
					requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
					commitStatusReassertion(t, e, a, statusCommitPC, 0, 0, grouped)
					requireStatusReassertionInput(t, e, a, false, false)
					_ = offerStatusAt(t, e, statusCommitPC)
					commitStatusReassertion(t, e, a, 0x120000, 16, 2, false)
					requireStatusReassertionInput(t, e, a, true, false)
					requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
					for offer := 2; offer <= 9; offer++ {
						commitStatusReassertion(t, e, a, 0x120000, 16, 2, false)
						requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), offer == 9)
					}
				})
			}
		})
	}
}

func TestOfferedStatusRenewalAfterExpiredNewestInputRestarts(t *testing.T) {
	e, a := newStatusReassertionFixture(t, true)
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
	commitStatusReassertion(t, e, a, statusCommitPC, 0, 2, true)
	now := time.Now().Add(2 * time.Hour)
	expired := false
	for _, candidate := range e.ssnm.congestionExpiries(now) {
		if candidate.received.association == a.ID() && candidate.received.destination.pointCode == statusCommitPC && candidate.received.destination.mask == 0 {
			expired = e.ssnm.expireCongestionStatement(candidate, now)
		}
	}
	if !expired {
		t.Fatal("newest narrow input did not expire")
	}
	// The old broad positive statement stays retained, but cannot replace the
	// newest expired narrow statement until a new report actually arrives.
	requireStatusReassertionInput(t, e, a, false, true)
	_ = offerStatusAt(t, e, statusCommitPC)
	commitStatusReassertion(t, e, a, 0x120000, 16, 2, false)
	requireStatusReassertionInput(t, e, a, true, false)
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
}

func TestOfferedStatusPartialScopeRemovalRevealsOlderInput(t *testing.T) {
	e, a := newStatusReassertionFixture(t, true)
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
	commitStatusReassertion(t, e, a, statusCommitPC, 0, 0, true)
	requireStatusReassertionInput(t, e, a, false, false)
	_ = offerStatusAt(t, e, statusCommitPC)
	partition := canonicalSSNMPartition("sg-a", "as-core")
	e.ssnm.mu.Lock()
	removed, _ := e.ssnm.retireReceivedSourcesLocked(e.ssnm.partitions[partition], func(_ receivedCongestionKey, record SSNMCongestion) bool {
		return record.Association == a.ID() && len(record.Scope.RoutingContexts) == 2
	}, false)
	e.ssnm.mu.Unlock()
	if len(removed) != 1 {
		t.Fatalf("retired %d source statements, want only the grouped narrow clear", len(removed))
	}
	requireStatusReassertionInput(t, e, a, true, false)
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
	for offer := 2; offer <= 9; offer++ {
		commitStatusReassertion(t, e, a, 0x120000, 16, 2, false)
		requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), offer == 9)
	}
}

func TestOfferedStatusWholeClearSourceRetirementPreservesSurvivingInput(t *testing.T) {
	e, a, _, partition := newStatusCommitFixture(t)
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitOtherPC), true)
	e.ssnm.retire(partition, a.ID())
	if err := e.ssnm.bind(partition, a.ID(), false); err != nil {
		t.Fatal(err)
	}
	for offer := 2; offer <= 9; offer++ {
		requireOfferedStatus(t, offerStatusAt(t, e, statusCommitOtherPC), offer == 9)
	}
}
