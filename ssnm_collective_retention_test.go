package m3ua

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// The mapped view must recover when two newer scopes collectively supersede
// an old scope, even when there is no spare destination-record slot.
func TestIPSPCollectiveCongestionAbatementAtCapacity(t *testing.T) {
	for _, retirement := range []bool{false, true} {
		t.Run(fmt.Sprintf("retirement=%v", retirement), func(t *testing.T) {
			e, a := newMappedCongestionAssociation(t)
			fillCongestionRecordSlots(t, a, DefaultMaxSSNMDestinationRecords-2)
			report := func(rcs []uint32, level uint8) {
				t.Helper()
				if err := a.handleSignallingCongestion(messages.NewSignallingCongestion(params.NewNetworkAppearance(10), params.NewRoutingContext(rcs...), params.NewAffectedPointCode(100), nil, params.NewCongestionIndications(level), nil)); err != nil {
					t.Errorf("SCON %v level %d refused: %v", rcs, level, err)
				}
			}
			retire := func() {
				t.Helper()
				if err := a.handleAspInactive(messages.NewAspInactive(params.NewRoutingContext(7), nil)); err != nil {
					t.Fatal(err)
				}
				a.applyDeregistrationResults([]RoutingKeyDeregistrationResult{{ASKey: *staticASKey(10, 7), RoutingContext: 7, Status: DeregistrationSuccessfullyDeregistered}})()
			}
			register := func() {
				t.Helper()
				if err := a.applyRegistrationResults([]registrationResultApplication{{request: RoutingKeyRegistrationRequest{RoutingKey: testRoutingKey(10, 100, 3)}, result: RoutingKeyRegistrationResult{Status: RegistrationSuccessfullyRegistered, RoutingContext: 7}}}); err != nil {
					t.Fatal(err)
				}
				a.noteRoutingContextsActive([]uint32{7, 8, 9})
				a.noteRoutingContextsAcked(params.NewRoutingContext(7, 8, 9))
			}
			rcs := []uint32{8, 9}
			if retirement {
				register()
				rcs = []uint32{7, 8, 9}
			}
			report(rcs, 2)
			requireMappedCongestionEvent(t, e, 2)
			if retirement {
				retire()
				register()
			}
			rcs = []uint32{8}
			if retirement {
				rcs = []uint32{7, 8}
			}
			report(rcs, 1)
			if s, ok := e.MTPDestinationStatus(MTPDestination{MTPRoute: "mapped", PointCode: 100}); !ok || !s.Congested || s.CongestionLevel != 2 {
				t.Errorf("newer RC 8 statement erased live RC 9 congestion: %+v", s)
			}
			if retirement {
				retire()
				register()
			}
			rcs = []uint32{9}
			if retirement {
				rcs = []uint32{7, 9}
			}
			report(rcs, 0)
			requireMappedCongestionEvent(t, e, 0)
			destination := MTPDestination{MTPRoute: "mapped", PointCode: 100}
			s, ok := e.MTPDestinationStatus(destination)
			if !ok || s.Congested || !s.CongestionLevelSet || s.CongestionLevel != 0 {
				t.Errorf("mapped snapshot missed abatement: %+v", s)
			}
			all := e.MTPDestinationStatuses()
			if len(all) != 1 || all[0].Congested || !all[0].CongestionLevelSet || all[0].CongestionLevel != 0 {
				t.Errorf("mapped snapshot enumeration missed abatement: %+v", all)
			}
			a.destinations.mu.RLock()
			count := len(a.destinations.state)
			a.destinations.mu.RUnlock()
			if count != DefaultMaxSSNMDestinationRecords {
				t.Errorf("superseded group survived: %d records", count)
			}
		})
	}
}

func newMappedCongestionAssociation(t *testing.T) (*Endpoint, *Association) {
	t.Helper()
	e, err := NewEndpoint(EndpointConfig{Role: RoleIPSP, TransportCongestion: &TransportCongestionConfig{Disabled: true}, SSNMState: &SSNMStateConfig{MaxRecords: 100000, MaxRecordsPerPartition: 100000, MaxRecordsPerPeer: 100000, MaxBytes: 64 << 20}, IPSPDestinations: []IPSPDestinationConfig{{Destination: MTPDestination{MTPRoute: "mapped", PointCode: 100}, AS: *staticASKey(10, 9)}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	a, _ := newTestConnWithContexts(t, StateASPActive, RoleIPSP, 8, 9)
	a.cfg.IPSP = &IPSPConfig{ExchangeModel: IPSPExchangeSingle}
	setInventoryNetworkAppearance(&a.cfg.ApplicationServers, params.NewNetworkAppearance(10))
	if !e.trackAssociation(a) {
		t.Fatal("track")
	}
	a.noteRoutingContextsActive([]uint32{8, 9})
	a.noteRoutingContextsAcked(params.NewRoutingContext(8, 9))
	return e, a
}

func fillCongestionRecordSlots(t *testing.T, a *Association, count int) {
	t.Helper()
	ranges := make([]destinationRange, count)
	for i := range ranges {
		ranges[i] = destinationRange{NetworkAppearance: 10, NetworkAppearanceSet: true, PointCode: 1000 + uint32(i), State: DestinationNetworkState{Congestion: CongestionState{LevelSet: true}}}
	}
	if err := a.destinations.setScopedCongestionRangesWithinBudget([]uint32{8}, ranges); err != nil {
		t.Fatal(err)
	}
}

func requireMappedCongestionEvent(t *testing.T, e *Endpoint, level uint8) {
	t.Helper()
	found := false
	for {
		select {
		case indication := <-e.MTPIndications():
			if indication == nil {
				t.Fatal("closed MTP stream")
			}
			if indication.Kind == MTPStatusIndication && indication.Destination.Destination.MTPRoute == "mapped" && indication.Destination.CongestionLevelSet && indication.Destination.CongestionLevel == level && indication.Destination.Congested == (level > 0) {
				found = true
			}
		default:
			if !found {
				t.Errorf("MTP-STATUS level %d not published", level)
			}
			return
		}
	}
}

func TestASPRestartRetirementKeepsDestinationRecordsBounded(t *testing.T) {
	a, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 8)
	setInventoryNetworkAppearance(&a.cfg.ApplicationServers, params.NewNetworkAppearance(10))
	a.cfg.TAck = time.Hour
	for cycle := 0; cycle < 128; cycle++ {
		if err := a.applyRegistrationResults([]registrationResultApplication{{request: RoutingKeyRegistrationRequest{RoutingKey: testRoutingKey(10, 100, 3)}, result: RoutingKeyRegistrationResult{Status: RegistrationSuccessfullyRegistered, RoutingContext: 7}}}); err != nil {
			t.Fatal(err)
		}
		a.commitState(StateASPActive)
		a.noteRoutingContextsActive([]uint32{7, 8})
		a.noteRoutingContextsAcked(params.NewRoutingContext(7, 8))
		if err := a.handleSignallingCongestion(messages.NewSignallingCongestion(params.NewNetworkAppearance(10), params.NewRoutingContext(7, 8), params.NewAffectedPointCode(100), nil, params.NewCongestionIndications(2), nil)); err != nil {
			t.Fatal(err)
		}
		if err := a.initiateASPInactive(params.NewRoutingContext(7)); err != nil {
			t.Fatal(err)
		}
		if err := a.handleAspInactiveAck(messages.NewAspInactiveAck(params.NewRoutingContext(7), nil)); err != nil {
			t.Fatal(err)
		}
		a.applyDeregistrationResults([]RoutingKeyDeregistrationResult{{ASKey: *staticASKey(10, 7), RoutingContext: 7, Status: DeregistrationSuccessfullyDeregistered}})()
		a.handleSCTPRestart()
		select {
		case state := <-a.stateChan:
			if err := a.handleStateUpdate(state); err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatal("restart did not publish ASP-DOWN")
		}
		a.destinations.mu.RLock()
		count := len(a.destinations.state)
		a.destinations.mu.RUnlock()
		if count != 1 {
			t.Fatalf("restart/retirement cycle %d retained %d records, want one live scope", cycle, count)
		}
		s, _ := a.destinations.lookup(destinationKey{networkAppearance: 10, networkAppearanceSet: true, routingContext: 8, routingContextSet: true, pointCode: 100})
		if s.Availability != DestinationUnavailable || !s.Congestion.Congested || s.Congestion.Level != 2 {
			t.Fatalf("restart lost independent state: %+v", s)
		}
		for len(a.mgmtChan) > 0 {
			<-a.mgmtChan
		}
		for len(a.stateEventChan) > 0 {
			<-a.stateEventChan
		}
	}
}

func TestIPSPDestinationRecordRefusalReportsSSNMLoss(t *testing.T) {
	e, a := newMappedCongestionAssociation(t)
	if err := a.handleSignallingCongestion(messages.NewSignallingCongestion(params.NewNetworkAppearance(10), params.NewRoutingContext(8, 9), params.NewAffectedPointCode(100), nil, params.NewCongestionIndications(2), nil)); err != nil {
		t.Fatal(err)
	}
	fillCongestionRecordSlots(t, a, DefaultMaxSSNMDestinationRecords-1)
	// The old statement is still needed for RC 8; no record can be reclaimed.
	_, sub, err := e.SubscribeSSNM()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close() }()
	err = a.handleSignallingCongestion(messages.NewSignallingCongestion(params.NewNetworkAppearance(10), params.NewRoutingContext(9), params.NewAffectedPointCode(100), nil, params.NewCongestionIndications(0), nil))
	if !errors.Is(err, ErrSSNMDestinationRecordLimit) {
		t.Fatalf("expected live-record refusal, got %v", err)
	}
	s, ok := e.MTPDestinationStatus(MTPDestination{MTPRoute: "mapped", PointCode: 100})
	if !ok || !s.Congested || s.CongestionLevel != 2 {
		t.Errorf("refused report changed prior IPSP view: %+v", s)
	}
	statuses := e.MTPDestinationStatuses()
	if len(statuses) != 1 || !statuses[0].Congested || statuses[0].CongestionLevel != 2 {
		t.Errorf("refusal changed mapped snapshot enumeration: %+v", statuses)
	}
	canonicalClear := false
	for _, partition := range e.SSNMKnowledge().Partitions {
		for _, destination := range partition.Destinations {
			if destination.Destination.PointCode == 100 && destination.CongestionSet && destination.Congestion.LevelSet && destination.Congestion.Level == 0 && !destination.Congestion.Congested {
				canonicalClear = true
			}
		}
	}
	if !canonicalClear {
		t.Error("independent canonical store did not accept level-zero report")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	for {
		event, err := sub.Next(ctx)
		if err != nil {
			t.Fatalf("record refusal not reported through SSNM subscription: %v", err)
		}
		if event.Kind == SSNMResourceLossEvent {
			if event.Reason == "" {
				t.Fatal("empty refusal reason")
			}
			break
		}
	}
}

func TestRetirementMergesCanonicalDimensionsByTheirStamps(t *testing.T) {
	for _, availabilityLast := range []bool{false, true} {
		t.Run(fmt.Sprintf("availability-last=%v", availabilityLast), func(t *testing.T) {
			d := newDestinations()
			v := destinationRange{PointCode: 100, State: DestinationNetworkState{Availability: DestinationUnavailable, Congestion: CongestionState{Congested: true, Level: 2, LevelSet: true}}}
			if err := d.setScopedRangesWithinBudget([]uint32{7, 8}, []destinationRange{v}); err != nil {
				t.Fatal(err)
			}
			if err := d.setScopedCongestionRangesWithinBudget([]uint32{7, 8}, []destinationRange{v}); err != nil {
				t.Fatal(err)
			}
			wantA, wantC := DestinationAvailable, uint8(3)
			if availabilityLast {
				v.State.Congestion.Level = 1
				if err := d.setScopedCongestionRangesWithinBudget([]uint32{8}, []destinationRange{v}); err != nil {
					t.Fatal(err)
				}
				v.State.Availability = DestinationRestricted
				if err := d.setScopedRangesWithinBudget([]uint32{7, 8}, []destinationRange{v}); err != nil {
					t.Fatal(err)
				}
				wantA, wantC = DestinationRestricted, 1
			} else {
				v.State.Availability = DestinationAvailable
				if err := d.setScopedRangesWithinBudget([]uint32{8}, []destinationRange{v}); err != nil {
					t.Fatal(err)
				}
				v.State.Congestion.Level = 3
				if err := d.setScopedCongestionRangesWithinBudget([]uint32{7, 8}, []destinationRange{v}); err != nil {
					t.Fatal(err)
				}
			}
			d.forgetRoutingContext(7)
			if len(d.state) != 1 {
				t.Errorf("retirement left %d equivalent scopes", len(d.state))
			}
			s, known := d.lookup(destinationKey{routingContext: 8, routingContextSet: true, pointCode: 100})
			if !known || s.Availability != wantA || s.Congestion.Level != wantC {
				t.Errorf("canonical merge lost chronology: %+v", s)
			}
			if _, known := d.lookup(destinationKey{routingContext: 7, routingContextSet: true, pointCode: 100}); known {
				t.Fatal("retired scope survived canonical merge")
			}
		})
	}
}

func TestCollectiveRetentionPreservesBaselineAndOtherDimensions(t *testing.T) {
	d := newDestinations()
	d.setRecordLimit(3)
	v := destinationRange{PointCode: 100, State: DestinationNetworkState{Availability: DestinationUnavailable, Congestion: CongestionState{Congested: true, Level: 2, LevelSet: true}}}
	if err := d.setRangesWithinBudget([]destinationRange{v}); err != nil {
		t.Fatal(err)
	}
	if err := d.setCongestionRangesWithinBudget([]destinationRange{v}); err != nil {
		t.Fatal(err)
	}
	v.State.Congestion = CongestionState{LevelSet: true}
	for _, rc := range []uint32{8, 9} {
		if err := d.setScopedCongestionRangesWithinBudget([]uint32{rc}, []destinationRange{v}); err != nil {
			t.Fatal(err)
		}
	}
	if len(d.state) != 3 {
		t.Fatal("finite scopes erased unscoped knowledge")
	}
	s, _ := d.lookup(destinationKey{routingContext: 10, routingContextSet: true, pointCode: 100})
	if s.Availability != DestinationUnavailable || !s.Congestion.Congested || s.Congestion.Level != 2 {
		t.Fatalf("uncovered baseline changed: %+v", s)
	}
	if err := d.setCongestionRangesWithinBudget([]destinationRange{v}); err != nil {
		t.Fatal(err)
	}
	if len(d.state) != 1 {
		t.Errorf("new baseline failed to reclaim finite congestion: %d", len(d.state))
	}
	s, _ = d.lookup(destinationKey{routingContext: 9, routingContextSet: true, pointCode: 100})
	if s.Availability != DestinationUnavailable || s.Congestion.Congested || !s.Congestion.LevelSet {
		t.Fatalf("baseline update erased independent knowledge: %+v", s)
	}
	d.forget()
	v.State.Congestion = CongestionState{Congested: true, Level: 3, LevelSet: true}
	if err := d.setScopedCongestionRangesWithinBudget([]uint32{8}, []destinationRange{v}); err != nil {
		t.Fatal(err)
	}
	s, _ = d.lookup(destinationKey{routingContext: 8, routingContextSet: true, pointCode: 100})
	if !s.Congestion.Congested || s.Congestion.Level != 3 || len(d.state) != 1 {
		t.Fatalf("forget left stale reclamation knowledge: %+v", s)
	}
}

func TestPauseReclaimsCollectivelySupersededAvailability(t *testing.T) {
	d := newDestinations()
	v := destinationRange{PointCode: 100, State: DestinationNetworkState{Availability: DestinationAvailable, Congestion: CongestionState{Congested: true, Level: 1, LevelSet: true}}}
	for _, rc := range []uint32{8, 9} {
		if err := d.setScopedRangesWithinBudget([]uint32{rc}, []destinationRange{v}); err != nil {
			t.Fatal(err)
		}
		if err := d.setScopedCongestionRangesWithinBudget([]uint32{rc}, []destinationRange{v}); err != nil {
			t.Fatal(err)
		}
	}
	v.State.Availability = DestinationUnavailable
	if err := d.setScopedRangesWithinBudget([]uint32{8, 9}, []destinationRange{v}); err != nil {
		t.Fatal(err)
	}
	if len(d.state) != 3 {
		t.Fatal("fixture must retain independent congestion plus the newer availability")
	}
	d.pause()
	if len(d.state) != 2 {
		t.Errorf("pause retained %d records, want two collectively live scopes", len(d.state))
	}
	for _, rc := range []uint32{8, 9} {
		s, known := d.lookup(destinationKey{routingContext: rc, routingContextSet: true, pointCode: 100})
		if !known || s.Availability != DestinationUnavailable || !s.Congestion.Congested || s.Congestion.Level != 1 {
			t.Errorf("pause changed independent RC %d knowledge: %+v", rc, s)
		}
	}
}
