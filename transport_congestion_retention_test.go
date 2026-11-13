package m3ua

import (
	"fmt"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestContractedCongestionRecordsRemainBoundedAcrossRegistrations(t *testing.T) {
	for _, budget := range []int{0, 1, 4} {
		t.Run(fmt.Sprintf("budget=%d", budget), func(t *testing.T) {
			key, other := *staticASKey(10, 7), *staticASKey(10, 8)
			e, err := NewEndpoint(EndpointConfig{Role: RoleIPSP, TransportCongestion: &TransportCongestionConfig{Disabled: true}, IPSPDestinations: []IPSPDestinationConfig{{Destination: MTPDestination{MTPRoute: "other", PointCode: 100}, AS: other}}})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = e.Close() })
			a, _ := newTestConnWithContexts(t, StateASPActive, RoleIPSP, 8)
			a.cfg.IPSP = &IPSPConfig{ExchangeModel: IPSPExchangeSingle}
			setInventoryNetworkAppearance(&a.cfg.ApplicationServers, params.NewNetworkAppearance(10))
			if !e.trackAssociation(a) {
				t.Fatal("track")
			}
			// Only one record is needed: each grouped report entirely supersedes the
			// prior congestion on surviving RC 8 before the new RC 7 is retired.
			a.destinations.setRecordLimit(budget)
			var firstReportError error
			maxRetained := 0
			report := func(level uint8) error {
				if budget == 0 {
					// Exercise the complete SCON handler at its normal IPSP budget.
					return a.handleSignallingCongestion(messages.NewSignallingCongestion(params.NewNetworkAppearance(10), params.NewRoutingContext(7, 8), params.NewAffectedPointCode(100), nil, params.NewCongestionIndications(level), nil))
				}
				// The receive path stores this dimension before refreshing IPSP mapping.
				// Keep that real store/projection path, with a small retention budget.
				err := a.destinations.setScopedCongestionRangesWithinBudget([]uint32{7, 8}, []destinationRange{{NetworkAppearance: 10, NetworkAppearanceSet: true, PointCode: 100, State: DestinationNetworkState{Congestion: CongestionState{Congested: level > 0, Level: level, LevelSet: true}}}})
				e.refreshIPSPDestinations()
				return err
			}
			for cycle := 0; cycle < 128; cycle++ {
				if err := a.applyRegistrationResults([]registrationResultApplication{{request: RoutingKeyRegistrationRequest{RoutingKey: testRoutingKey(10, 100, 3)}, result: RoutingKeyRegistrationResult{Status: RegistrationSuccessfullyRegistered, RoutingContext: 7}}}); err != nil {
					t.Fatal(err)
				}
				a.noteRoutingContextsActive([]uint32{7, 8})
				a.noteRoutingContextsAcked(params.NewRoutingContext(7, 8))
				if err := report(2); err != nil {
					if firstReportError == nil {
						firstReportError = err
					}
				}
				if err := a.handleAspInactive(messages.NewAspInactive(params.NewRoutingContext(7), nil)); err != nil {
					t.Fatal(err)
				}
				a.applyDeregistrationResults([]RoutingKeyDeregistrationResult{{ASKey: key, RoutingContext: 7, Status: DeregistrationSuccessfullyDeregistered}})()
				a.destinations.mu.RLock()
				count := len(a.destinations.state)
				a.destinations.mu.RUnlock()
				maxRetained = max(maxRetained, count)
			}
			a.addDynamicASKey(key, testRoutingKey(10, 100, 3), false)
			a.noteRoutingContextsActive([]uint32{7, 8})
			a.noteRoutingContextsAcked(params.NewRoutingContext(7, 8))
			if maxRetained != 1 {
				t.Errorf("registration cycles retained up to %d records, want one surviving scope", maxRetained)
			}
			if firstReportError != nil {
				t.Errorf("fresh congestion was refused during registration cycles: %v", firstReportError)
			}
			if err := report(0); err != nil {
				t.Errorf("congestion clear refused after registration cycles: %v", err)
			}
			if s, ok := e.MTPDestinationStatus(MTPDestination{MTPRoute: "other", PointCode: 100}); !ok || s.Congested || s.CongestionLevel != 0 || !s.CongestionLevelSet {
				t.Fatalf("fresh level-zero SCON did not clear mapped IPSP destination: %+v", s)
			}
		})
	}
}

func TestContractedReclamationPreservesDimensionsAndChronology(t *testing.T) {
	d := newDestinations()
	d.setRecordLimit(2)
	v := destinationRange{NetworkAppearance: 10, NetworkAppearanceSet: true, PointCode: 100, State: DestinationNetworkState{Availability: DestinationUnavailable, Congestion: CongestionState{Congested: true, Level: 2, LevelSet: true}}}
	if err := d.setScopedRangesWithinBudget([]uint32{7, 8}, []destinationRange{v}); err != nil {
		t.Fatal(err)
	}
	if err := d.setScopedCongestionRangesWithinBudget([]uint32{7, 8}, []destinationRange{v}); err != nil {
		t.Fatal(err)
	}
	d.forgetRoutingContext(7)
	// A newer availability report must win after congestion alone supersedes
	// the contracted congestion dimension (RFC 4666 Sections 4.5.2.2 and 4.5.3).
	v.State.Availability = DestinationAvailable
	if err := d.setScopedRangesWithinBudget([]uint32{8}, []destinationRange{v}); err != nil {
		t.Fatal(err)
	}
	beforeClear, known := d.lookup(destinationKey{networkAppearance: 10, networkAppearanceSet: true, routingContext: 8, routingContextSet: true, pointCode: 100})
	if !known || beforeClear.Availability != DestinationAvailable || !beforeClear.Congestion.Congested || beforeClear.Congestion.Level != 2 {
		t.Fatalf("availability update erased contracted congestion: %+v", beforeClear)
	}
	v.State.Congestion = CongestionState{LevelSet: true}
	if err := d.setScopedCongestionRangesWithinBudget([]uint32{7, 8}, []destinationRange{v}); err != nil {
		t.Fatal(err)
	}
	for _, rc := range []uint32{7, 8} {
		s, known := d.lookup(destinationKey{networkAppearance: 10, networkAppearanceSet: true, routingContext: rc, routingContextSet: true, pointCode: 100})
		if !known || s.Availability != DestinationAvailable || s.Congestion.Congested || !s.Congestion.LevelSet {
			t.Fatalf("RC %d inherited old dimensions or chronology: %+v", rc, s)
		}
	}
	d.mu.RLock()
	count := len(d.state)
	d.mu.RUnlock()
	if count != 2 {
		t.Fatalf("superseded contracted dimensions retained %d records", count)
	}
}

func TestContractedReclamationPreservesUncoveredKnowledge(t *testing.T) {
	for _, kind := range []string{"appearance", "point-code", "mask", "scope", "partial-scope"} {
		t.Run(kind, func(t *testing.T) {
			d := newDestinations()
			old := destinationRange{NetworkAppearance: 10, NetworkAppearanceSet: true, PointCode: 100, State: DestinationNetworkState{Congestion: CongestionState{Congested: true, Level: 2, LevelSet: true}}}
			if kind == "mask" {
				old.Mask = 1
			}
			contexts := []uint32{7, 8}
			if kind == "partial-scope" {
				contexts = append(contexts, 9)
			}
			if err := d.setScopedCongestionRangesWithinBudget(contexts, []destinationRange{old}); err != nil {
				t.Fatal(err)
			}
			d.forgetRoutingContext(7)
			update := old
			update.State.Congestion = CongestionState{LevelSet: true}
			fresh := []uint32{7, 8}
			switch kind {
			case "appearance":
				update.NetworkAppearance = 20
			case "point-code":
				update.PointCode = 101
			case "mask":
				update.Mask = 0
			case "scope":
				fresh = []uint32{7}
			}
			if err := d.setScopedCongestionRangesWithinBudget(fresh, []destinationRange{update}); err != nil {
				t.Fatal(err)
			}
			d.mu.RLock()
			count := len(d.state)
			d.mu.RUnlock()
			if count != 2 {
				t.Fatalf("uncovered knowledge was discarded: %d records", count)
			}
			rc := uint32(8)
			if kind == "partial-scope" {
				rc = 9
			}
			point := uint32(100)
			if kind == "mask" {
				point = 101
			}
			s, known := d.lookup(destinationKey{networkAppearance: 10, networkAppearanceSet: true, routingContext: rc, routingContextSet: true, pointCode: point})
			if !known || !s.Congestion.Congested || s.Congestion.Level != 2 {
				t.Fatalf("uncovered RC %d changed: %+v", rc, s)
			}
		})
	}
}

func TestRefusedUpdatePreservesContractedDimensions(t *testing.T) {
	d := newDestinations()
	d.setRecordLimit(1)
	v := destinationRange{NetworkAppearance: 10, NetworkAppearanceSet: true, PointCode: 100, State: DestinationNetworkState{Availability: DestinationUnavailable, Congestion: CongestionState{Congested: true, Level: 2, LevelSet: true}}}
	if err := d.setScopedRangesWithinBudget([]uint32{7, 8}, []destinationRange{v}); err != nil {
		t.Fatal(err)
	}
	if err := d.setScopedCongestionRangesWithinBudget([]uint32{7, 8}, []destinationRange{v}); err != nil {
		t.Fatal(err)
	}
	d.forgetRoutingContext(7)
	v.State.Congestion = CongestionState{LevelSet: true}
	if err := d.setScopedCongestionRangesWithinBudget([]uint32{7, 8}, []destinationRange{v}); err == nil {
		t.Fatal("partial-dimension update exceeded one-record budget")
	}
	s, known := d.lookup(destinationKey{networkAppearance: 10, networkAppearanceSet: true, routingContext: 8, routingContextSet: true, pointCode: 100})
	if !known || s.Availability != DestinationUnavailable || !s.Congestion.Congested || s.Congestion.Level != 2 {
		t.Fatalf("refused update erased retained dimensions: %+v", s)
	}
}

func TestContractedReclamationKeepsUntouchedDimensionChronology(t *testing.T) {
	d := newDestinations()
	v := destinationRange{NetworkAppearance: 10, NetworkAppearanceSet: true, PointCode: 100, State: DestinationNetworkState{Availability: DestinationUnavailable, Congestion: CongestionState{Congested: true, Level: 2, LevelSet: true}}}
	if err := d.setScopedRangesWithinBudget([]uint32{7, 8, 9}, []destinationRange{v}); err != nil {
		t.Fatal(err)
	}
	if err := d.setScopedCongestionRangesWithinBudget([]uint32{7, 8, 9}, []destinationRange{v}); err != nil {
		t.Fatal(err)
	}
	d.forgetRoutingContext(7)
	v.State.Congestion.Level = 1
	if err := d.setScopedCongestionRangesWithinBudget([]uint32{8}, []destinationRange{v}); err != nil {
		t.Fatal(err)
	}
	v.State.Availability = DestinationAvailable
	if err := d.setScopedRangesWithinBudget([]uint32{7, 8, 9}, []destinationRange{v}); err != nil {
		t.Fatal(err)
	}
	for _, rc := range []uint32{8, 9} {
		s, _ := d.lookup(destinationKey{networkAppearance: 10, networkAppearanceSet: true, routingContext: rc, routingContextSet: true, pointCode: 100})
		want := uint8(1)
		if rc == 9 {
			want = 2
		}
		if s.Availability != DestinationAvailable || !s.Congestion.Congested || s.Congestion.Level != want {
			t.Fatalf("RC %d lost independent dimension chronology: %+v", rc, s)
		}
	}
}

func TestContractedReclamationAcceptsBaselineAndSingleContext(t *testing.T) {
	for _, kind := range []string{"baseline", "single", "unrelated-single"} {
		t.Run(kind, func(t *testing.T) {
			d := newDestinations()
			d.setRecordLimit(1)
			v := destinationRange{NetworkAppearance: 10, NetworkAppearanceSet: true, PointCode: 100, State: DestinationNetworkState{Congestion: CongestionState{Congested: true, Level: 2, LevelSet: true}}}
			if err := d.setScopedCongestionRangesWithinBudget([]uint32{7, 8}, []destinationRange{v}); err != nil {
				t.Fatal(err)
			}
			d.forgetRoutingContext(7)
			v.State.Congestion = CongestionState{LevelSet: true}
			if kind != "baseline" {
				v.RoutingContextSet = true
				v.RoutingContext = 8
			}
			if kind == "unrelated-single" {
				v.RoutingContext = 9
			}
			err := d.setCongestionRangesWithinBudget([]destinationRange{v})
			s, known := d.lookup(destinationKey{networkAppearance: 10, networkAppearanceSet: true, routingContext: 8, routingContextSet: true, pointCode: 100})
			if kind == "unrelated-single" {
				if err == nil || !known || !s.Congestion.Congested || s.Congestion.Level != 2 {
					t.Fatalf("unrelated RC erased surviving state: %+v, %v", s, err)
				}
			} else if err != nil || !known || s.Congestion.Congested || !s.Congestion.LevelSet {
				t.Fatalf("covering clear was refused or not applied: %+v, %v", s, err)
			}
		})
	}
}

func TestContractedReclamationMatchesEquivalentPrefixes(t *testing.T) {
	d := newDestinations()
	d.setRecordLimit(1)
	v := destinationRange{PointCode: 100, Mask: 1, State: DestinationNetworkState{Congestion: CongestionState{Congested: true, Level: 2, LevelSet: true}}}
	if err := d.setScopedCongestionRangesWithinBudget([]uint32{7, 8}, []destinationRange{v}); err != nil {
		t.Fatal(err)
	}
	d.forgetRoutingContext(7)
	v.PointCode = 101
	v.State.Congestion = CongestionState{LevelSet: true}
	if err := d.setScopedCongestionRangesWithinBudget([]uint32{7, 8}, []destinationRange{v}); err != nil {
		t.Fatalf("equivalent prefix clear refused: %v", err)
	}
	s, known := d.lookup(destinationKey{routingContext: 8, routingContextSet: true, pointCode: 100})
	if !known || s.Congestion.Congested || !s.Congestion.LevelSet {
		t.Fatalf("equivalent prefix clear not applied: %+v", s)
	}
}

func TestContractedReclamationDoesNotPromoteUntouchedWireDimension(t *testing.T) {
	for _, availability := range []bool{true, false} {
		t.Run(fmt.Sprintf("availability=%v", availability), func(t *testing.T) {
			d := newDestinations()
			v := destinationRange{PointCode: 100, State: DestinationNetworkState{Availability: DestinationUnavailable, Congestion: CongestionState{Congested: true, Level: 2, LevelSet: true}}}
			write := d.setScopedCongestionRangesWithinBudget
			if availability {
				write = d.setScopedRangesWithinBudget
			}
			if err := write([]uint32{7, 8}, []destinationRange{v}); err != nil {
				t.Fatal(err)
			}
			v.State.Availability = DestinationAvailable
			v.State.Congestion.Level = 1
			if err := write([]uint32{8, 9}, []destinationRange{v}); err != nil {
				t.Fatal(err)
			}
			d.forgetRoutingContext(9)
			if availability {
				v.State.Congestion.Level = 2
				write = d.setScopedCongestionRangesWithinBudget
			} else {
				v.State.Availability = DestinationUnavailable
				write = d.setScopedRangesWithinBudget
			}
			if err := write([]uint32{7, 8}, []destinationRange{v}); err != nil {
				t.Fatal(err)
			}
			for _, rc := range []uint32{7, 8} {
				s, _ := d.lookup(destinationKey{routingContext: rc, routingContextSet: true, pointCode: 100})
				if availability {
					want := DestinationUnavailable
					if rc == 8 {
						want = DestinationAvailable
					}
					if s.Availability != want {
						t.Fatalf("RC %d promoted stale availability: %+v", rc, s)
					}
				} else {
					want := uint8(2)
					if rc == 8 {
						want = 1
					}
					if s.Congestion.Level != want || !s.Congestion.Congested {
						t.Fatalf("RC %d promoted stale congestion: %+v", rc, s)
					}
				}
			}
		})
	}
}

func TestContractedDimensionChronologySurvivesCounterWrapAndPause(t *testing.T) {
	d := newDestinations()
	d.sequence = 1000
	v := destinationRange{PointCode: 100, State: DestinationNetworkState{Availability: DestinationUnavailable}}
	if err := d.setScopedRangesWithinBudget([]uint32{7, 8, 9}, []destinationRange{v}); err != nil {
		t.Fatal(err)
	}
	d.forgetRoutingContext(7)
	d.sequence = ^uint64(0)
	c := destinationRange{PointCode: 100, State: DestinationNetworkState{Congestion: CongestionState{Congested: true, Level: 2, LevelSet: true}}}
	if err := d.setScopedCongestionRangesWithinBudget([]uint32{7, 8}, []destinationRange{c}); err != nil {
		t.Fatal(err)
	}
	v.State.Availability = DestinationAvailable
	if err := d.setScopedRangesWithinBudget([]uint32{8}, []destinationRange{v}); err != nil {
		t.Fatal(err)
	}
	scope := destinationKey{routingContext: 8, routingContextSet: true, pointCode: 100}
	if s, _ := d.lookup(scope); s.Availability != DestinationAvailable {
		t.Fatalf("counter wrap promoted old availability: %+v", s)
	}
	c.State.Congestion.Level = 1
	if err := d.setScopedCongestionRangesWithinBudget([]uint32{8}, []destinationRange{c}); err != nil {
		t.Fatal(err)
	}
	v.State.Availability = DestinationUnavailable
	if err := d.setScopedRangesWithinBudget([]uint32{8}, []destinationRange{v}); err != nil {
		t.Fatal(err)
	}
	d.pause()
	check := func(s DestinationNetworkState) {
		t.Helper()
		if s.Availability != DestinationUnavailable || !s.Congestion.Congested || s.Congestion.Level != 1 {
			t.Fatalf("pause promoted stale congestion: %+v", s)
		}
	}
	s, _ := d.lookup(scope)
	check(s)
	check(d.snapshotForScope(scope)[100])
}

func TestDestinationAuditPreservesIndependentDimensionChronology(t *testing.T) {
	c, _ := newObservedSSNMConn(t, StateASPActive, RoleSGP, 1)
	v := destinationRange{NetworkAppearance: 7, NetworkAppearanceSet: true, PointCode: 100, Mask: 1, State: DestinationNetworkState{Availability: DestinationUnavailable}}
	if err := c.destinations.setScopedRangesWithinBudget([]uint32{1}, []destinationRange{v}); err != nil {
		t.Fatal(err)
	}
	v.Mask = 0
	v.State.Availability = DestinationAvailable
	if err := c.destinations.setScopedRangesWithinBudget([]uint32{1}, []destinationRange{v}); err != nil {
		t.Fatal(err)
	}
	v.Mask = 1
	v.State.Congestion = CongestionState{Congested: true, Level: 2, LevelSet: true}
	if err := c.destinations.setScopedCongestionRangesWithinBudget([]uint32{1}, []destinationRange{v}); err != nil {
		t.Fatal(err)
	}
	request := messages.NewDestinationStateAudit(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCode(100), nil)
	replies, err := c.destinationAuditReplies(request, []uint32{100}, []uint8{0})
	if err != nil || len(replies) != 1 {
		t.Fatalf("audit plan: %+v %v", replies, err)
	}
	if s := replies[0].state; s.Availability != DestinationAvailable || !s.Congestion.Congested || s.Congestion.Level != 2 {
		t.Fatalf("audit promoted untouched availability: %+v", s)
	}
}

func TestDestinationStatusesPreserveIndependentDimensionChronology(t *testing.T) {
	e, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	v := destinationRange{PointCode: 100, State: DestinationNetworkState{Availability: DestinationUnavailable}}
	if err := e.destinations.setScopedRangesWithinBudget([]uint32{7, 8}, []destinationRange{v}); err != nil {
		t.Fatal(err)
	}
	v.State.Availability = DestinationAvailable
	if err := e.destinations.setScopedRangesWithinBudget([]uint32{8, 9}, []destinationRange{v}); err != nil {
		t.Fatal(err)
	}
	v.State.Congestion = CongestionState{Congested: true, Level: 2, LevelSet: true}
	if err := e.destinations.setScopedCongestionRangesWithinBudget([]uint32{7, 8}, []destinationRange{v}); err != nil {
		t.Fatal(err)
	}
	for _, status := range e.DestinationStatuses() {
		if status.Key.RoutingContext == 8 {
			if status.State.Availability != DestinationAvailable || !status.State.Congestion.Congested || status.State.Congestion.Level != 2 {
				t.Fatalf("status promoted untouched availability: %+v", status)
			}
			return
		}
	}
	t.Fatal("missing RC 8 status")
}

func TestDestinationAuditPreservesCongestionChronology(t *testing.T) {
	c, _ := newObservedSSNMConn(t, StateASPActive, RoleSGP, 1)
	v := destinationRange{NetworkAppearance: 7, NetworkAppearanceSet: true, PointCode: 100, Mask: 1, State: DestinationNetworkState{Congestion: CongestionState{Congested: true, Level: 2, LevelSet: true}}}
	if err := c.destinations.setScopedCongestionRangesWithinBudget([]uint32{1}, []destinationRange{v}); err != nil {
		t.Fatal(err)
	}
	v.Mask = 0
	v.State.Congestion.Level = 1
	if err := c.destinations.setScopedCongestionRangesWithinBudget([]uint32{1}, []destinationRange{v}); err != nil {
		t.Fatal(err)
	}
	v.Mask = 1
	v.State.Availability = DestinationAvailable
	if err := c.destinations.setScopedRangesWithinBudget([]uint32{1}, []destinationRange{v}); err != nil {
		t.Fatal(err)
	}
	request := messages.NewDestinationStateAudit(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCode(100), nil)
	replies, err := c.destinationAuditReplies(request, []uint32{100}, []uint8{0})
	if err != nil || len(replies) != 1 {
		t.Fatalf("audit plan: %+v %v", replies, err)
	}
	if s := replies[0].state; s.Availability != DestinationAvailable || !s.Congestion.Congested || s.Congestion.Level != 1 {
		t.Fatalf("audit promoted untouched congestion: %+v", s)
	}
}

func TestDestinationStatusesPreserveCongestionChronology(t *testing.T) {
	e, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	v := destinationRange{PointCode: 100, State: DestinationNetworkState{Congestion: CongestionState{Congested: true, Level: 2, LevelSet: true}}}
	if err := e.destinations.setScopedCongestionRangesWithinBudget([]uint32{7, 8}, []destinationRange{v}); err != nil {
		t.Fatal(err)
	}
	v.State.Congestion.Level = 1
	if err := e.destinations.setScopedCongestionRangesWithinBudget([]uint32{8, 9}, []destinationRange{v}); err != nil {
		t.Fatal(err)
	}
	if err := e.destinations.setScopedRangesWithinBudget([]uint32{7, 8}, []destinationRange{v}); err != nil {
		t.Fatal(err)
	}
	for _, status := range e.DestinationStatuses() {
		if status.Key.RoutingContext == 8 {
			if !status.State.Congestion.Congested || status.State.Congestion.Level != 1 {
				t.Fatalf("status promoted untouched congestion: %+v", status)
			}
			return
		}
	}
	t.Fatal("missing RC 8 status")
}
