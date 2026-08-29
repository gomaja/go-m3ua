// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"errors"
	"sync"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestASPRouteAggregationKeepsDestinationAvailableThroughSecondSG(t *testing.T) {
	endpoint, first, second := newASPMultiSGFixture(t)
	const pointCode = uint32(0x123456)

	requireASPRouteStatus(t, endpoint, "sccp-a", pointCode, DestinationAvailable, false, 0, false)
	applyASPDUNA(t, first, 7, 1, pointCode, 0)
	requireASPRouteStatus(t, endpoint, "sccp-a", pointCode, DestinationAvailable, false, 0, false)

	applyASPDUNA(t, second, 9, 42, pointCode, 0)
	requireASPRouteStatus(t, endpoint, "sccp-a", pointCode, DestinationUnavailable, false, 0, false)

	applyASPDAVA(t, first, 7, 1, pointCode, 0)
	requireASPRouteStatus(t, endpoint, "sccp-a", pointCode, DestinationAvailable, false, 0, false)
}

func TestASPRouteAggregationPrefersAvailableOverRestricted(t *testing.T) {
	endpoint, first, second := newASPMultiSGFixture(t)
	const pointCode = uint32(0x123456)

	applyASPDRST(t, first, 7, 1, pointCode, 0)
	requireASPRouteStatus(t, endpoint, "sccp-a", pointCode, DestinationAvailable, false, 0, false)

	applyASPDRST(t, second, 9, 42, pointCode, 0)
	requireASPRouteStatus(t, endpoint, "sccp-a", pointCode, DestinationRestricted, false, 0, false)

	applyASPDUNA(t, first, 7, 1, pointCode, 0)
	requireASPRouteStatus(t, endpoint, "sccp-a", pointCode, DestinationRestricted, false, 0, false)
}

func TestASPRouteAggregationKeepsCongestionIndependentOfAvailability(t *testing.T) {
	endpoint, first, second := newASPMultiSGFixture(t)
	const pointCode = uint32(0x123456)

	applyASPSCON(t, first, 7, 1, pointCode, 0, params.NewCongestionIndications(2))
	requireASPRouteStatus(t, endpoint, "sccp-a", pointCode, DestinationAvailable, false, 0, false)

	applyASPSCON(t, second, 9, 42, pointCode, 0, params.NewCongestionIndications(3))
	requireASPRouteStatus(t, endpoint, "sccp-a", pointCode, DestinationAvailable, true, 2, true)

	// DAVA updates availability only. It must not erase the independent SCON(2).
	applyASPDAVA(t, first, 7, 1, pointCode, 0)
	requireASPRouteStatus(t, endpoint, "sccp-a", pointCode, DestinationAvailable, true, 2, true)

	applyASPSCON(t, first, 7, 1, pointCode, 0, params.NewCongestionIndications(0))
	requireASPRouteStatus(t, endpoint, "sccp-a", pointCode, DestinationAvailable, false, 0, true)
}

func TestASPRouteAggregationPreservesUnknownCongestionLevel(t *testing.T) {
	endpoint, first, second := newASPMultiSGFixture(t)
	const pointCode = uint32(0x123456)

	applyASPDUNA(t, second, 9, 42, pointCode, 0)
	applyASPSCON(t, first, 7, 1, pointCode, 0, nil)
	requireASPRouteStatus(t, endpoint, "sccp-a", pointCode, DestinationAvailable, true, 0, false)
}

func TestASPRouteAggregationClipsBroadSSNMToMTPRoute(t *testing.T) {
	endpoint, first, second := newASPMultiSGFixture(t)

	// The MTP Route covers 0x12xxxx. A broad update from SG A must affect that
	// provisioned range, but not invent route state outside it.
	applyASPDUNA(t, first, 7, 1, 0, 24)
	applyASPDUNA(t, second, 9, 42, 0x123400, 8)
	requireASPRouteStatus(t, endpoint, "sccp-a", 0x123456, DestinationUnavailable, false, 0, false)
	requireASPRouteStatus(t, endpoint, "sccp-a", 0x124456, DestinationAvailable, false, 0, false)

	if _, known := endpoint.aspRoutes.destinationStatus("sccp-a", 0x220000, 0); known {
		t.Fatal("route registry invented a destination outside the MTP Route")
	}
}

func TestASPRouteAggregationUsesLatestCoveringUpdate(t *testing.T) {
	endpoint, first, second := newASPMultiSGFixture(t)
	if err := second.Close(); err != nil {
		t.Fatalf("close alternate SG Association: %v", err)
	}

	applyASPDUNA(t, first, 7, 1, 0x123456, 0)
	applyASPDAVA(t, first, 7, 1, 0x120000, 16)
	requireASPRouteStatus(t, endpoint, "sccp-a", 0x123456, DestinationAvailable, false, 0, false)

	applyASPDUNA(t, first, 7, 1, 0x123456, 0)
	requireASPRouteStatus(t, endpoint, "sccp-a", 0x123456, DestinationUnavailable, false, 0, false)
	requireASPRouteStatus(t, endpoint, "sccp-a", 0x123457, DestinationAvailable, false, 0, false)
}

func TestASPRouteAggregationInfersOmittedSingleAssociationScope(t *testing.T) {
	for _, test := range []struct {
		name              string
		networkAppearance *params.Param
		routingContext    *params.Param
	}{
		{name: "both present", networkAppearance: params.NewNetworkAppearance(7), routingContext: params.NewRoutingContext(1)},
		{name: "Network Appearance omitted", routingContext: params.NewRoutingContext(1)},
		{name: "Routing Context omitted", networkAppearance: params.NewNetworkAppearance(7)},
		{name: "both omitted"},
	} {
		t.Run(test.name, func(t *testing.T) {
			endpoint, first, second := newASPMultiSGFixture(t)
			if err := first.handleDestinationUnavailable(messages.NewDestinationUnavailable(
				test.networkAppearance,
				test.routingContext,
				params.NewAffectedPointCode(0x123456),
				nil,
			)); err != nil {
				t.Fatalf("handleDestinationUnavailable: %v", err)
			}
			if err := second.Close(); err != nil {
				t.Fatalf("close alternate SG Association: %v", err)
			}
			status, known := endpoint.MTPDestinationStatus(MTPDestination{
				MTPRoute: "sccp-a", PointCode: 0x123456,
			})
			if !known || status.Availability != DestinationUnavailable {
				t.Fatalf("derived status = %#v, known %v, want unavailable", status, known)
			}
		})
	}
}

func TestASPRouteAggregationDoesNotApplyDifferentNetworkAppearance(t *testing.T) {
	endpoint, first, second := newASPMultiSGFixture(t)
	if err := first.handleDestinationUnavailable(messages.NewDestinationUnavailable(
		params.NewNetworkAppearance(99),
		params.NewRoutingContext(1),
		params.NewAffectedPointCode(0x123456),
		nil,
	)); err != nil {
		t.Fatalf("handleDestinationUnavailable: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatalf("close alternate SG Association: %v", err)
	}
	status, known := endpoint.MTPDestinationStatus(MTPDestination{
		MTPRoute: "sccp-a", PointCode: 0x123456,
	})
	if !known || status.Availability != DestinationAvailable {
		t.Fatalf("different Network Appearance changed configured route: %#v, known %v", status, known)
	}
}

func TestASPRouteStateRejectsOversizedSSNMAtomically(t *testing.T) {
	config := validASPConfig()
	config.MaxAffectedPointCodesPerSSNM = 1
	endpoint, first, _ := newASPMultiSGFixtureWithConfig(t, config)
	err := first.handleDestinationUnavailable(messages.NewDestinationUnavailable(
		params.NewNetworkAppearance(7),
		params.NewRoutingContext(1),
		params.NewAffectedPointCode(0x123456, 0x123457),
		nil,
	))
	if !errors.Is(err, ErrASPRouteStateLimit) {
		t.Fatalf("oversized SSNM error = %v, want ErrASPRouteStateLimit", err)
	}
	endpoint.aspRoutes.mu.RLock()
	availabilityRecords := len(endpoint.aspRoutes.availability)
	endpoint.aspRoutes.mu.RUnlock()
	if availabilityRecords != 0 {
		t.Fatalf("oversized SSNM stored %d availability records", availabilityRecords)
	}
	if state := first.DestinationStateForNetworkAndRoutingContext(7, 1, 0x123456); state != DestinationAvailable {
		t.Fatalf("oversized SSNM changed Association destination state to %v", state)
	}
}

func TestASPRouteStateRejectsPerRouteRecordOverflowAtomically(t *testing.T) {
	config := validASPConfig()
	config.MaxAffectedPointCodesPerSSNM = 8
	config.MaxSSNMStateRecordsPerRoute = 1
	config.MaxSSNMStateRecords = 8
	endpoint, first, _ := newASPMultiSGFixtureWithConfig(t, config)
	err := first.handleDestinationUnavailable(messages.NewDestinationUnavailable(
		params.NewNetworkAppearance(7),
		params.NewRoutingContext(1),
		params.NewAffectedPointCode(0x123456, 0x123457),
		nil,
	))
	if !errors.Is(err, ErrASPRouteStateLimit) {
		t.Fatalf("per-route overflow error = %v, want ErrASPRouteStateLimit", err)
	}
	endpoint.aspRoutes.mu.RLock()
	availabilityRecords := len(endpoint.aspRoutes.availability)
	endpoint.aspRoutes.mu.RUnlock()
	if availabilityRecords != 0 {
		t.Fatalf("per-route overflow partially stored %d availability records", availabilityRecords)
	}
}

func TestASPRouteStateRejectsEndpointRecordOverflowAtomically(t *testing.T) {
	config := validASPConfig()
	config.MaxAffectedPointCodesPerSSNM = 8
	config.MaxSSNMStateRecordsPerRoute = 8
	config.MaxSSNMStateRecords = 1
	endpoint, first, second := newASPMultiSGFixtureWithConfig(t, config)

	applyASPDUNA(t, first, 7, 1, 0x123456, 0)
	err := second.handleDestinationUnavailable(messages.NewDestinationUnavailable(
		params.NewNetworkAppearance(9),
		params.NewRoutingContext(42),
		params.NewAffectedPointCode(0x123456),
		nil,
	))
	if !errors.Is(err, ErrASPRouteStateLimit) {
		t.Fatalf("Endpoint overflow error = %v, want ErrASPRouteStateLimit", err)
	}
	endpoint.aspRoutes.mu.RLock()
	recordCount := endpoint.aspRoutes.stateRecordCount
	availabilityRecords := len(endpoint.aspRoutes.availability)
	endpoint.aspRoutes.mu.RUnlock()
	if recordCount != 1 || availabilityRecords != 1 {
		t.Fatalf("Endpoint overflow left count %d and %d availability records, want 1 and 1", recordCount, availabilityRecords)
	}
	if state := second.DestinationStateForNetworkAndRoutingContext(9, 42, 0x123456); state != DestinationAvailable {
		t.Fatalf("Endpoint overflow changed Association destination state to %v", state)
	}
}

func TestASPRouteStateOverwriteDoesNotConsumeRecordBudget(t *testing.T) {
	config := validASPConfig()
	config.MaxAffectedPointCodesPerSSNM = 8
	config.MaxSSNMStateRecordsPerRoute = 1
	config.MaxSSNMStateRecords = 1
	endpoint, first, _ := newASPMultiSGFixtureWithConfig(t, config)

	applyASPDUNA(t, first, 7, 1, 0x123456, 0)
	applyASPDAVA(t, first, 7, 1, 0x123456, 0)
	endpoint.aspRoutes.mu.RLock()
	recordCount := endpoint.aspRoutes.stateRecordCount
	availabilityRecords := len(endpoint.aspRoutes.availability)
	endpoint.aspRoutes.mu.RUnlock()
	if recordCount != 1 || availabilityRecords != 1 {
		t.Fatalf("overwrite left count %d and %d availability records, want 1 and 1", recordCount, availabilityRecords)
	}
	endpoint.aspRoutes.mu.RLock()
	indexNodes := countASPRouteStateIndexNodes(endpoint.aspRoutes.stateIndex["sccp-a"])
	endpoint.aspRoutes.mu.RUnlock()
	for range 64 {
		applyASPDUNA(t, first, 7, 1, 0x123456, 0)
		applyASPDAVA(t, first, 7, 1, 0x123456, 0)
	}
	endpoint.aspRoutes.mu.RLock()
	updatedIndexNodes := countASPRouteStateIndexNodes(endpoint.aspRoutes.stateIndex["sccp-a"])
	endpoint.aspRoutes.mu.RUnlock()
	if updatedIndexNodes != indexNodes {
		t.Fatalf("overwrite grew route-state index from %d to %d nodes", indexNodes, updatedIndexNodes)
	}
}

func TestASPRouteStateCountsAvailabilityAndCongestionSeparately(t *testing.T) {
	config := validASPConfig()
	config.MaxAffectedPointCodesPerSSNM = 8
	config.MaxSSNMStateRecordsPerRoute = 1
	config.MaxSSNMStateRecords = 8
	endpoint, first, _ := newASPMultiSGFixtureWithConfig(t, config)

	applyASPDUNA(t, first, 7, 1, 0x123456, 0)
	err := first.handleSignallingCongestion(messages.NewSignallingCongestion(
		params.NewNetworkAppearance(7),
		params.NewRoutingContext(1),
		params.NewAffectedPointCode(0x123456),
		nil,
		params.NewCongestionIndications(2),
		nil,
	))
	if !errors.Is(err, ErrASPRouteStateLimit) {
		t.Fatalf("congestion overflow error = %v, want ErrASPRouteStateLimit", err)
	}
	endpoint.aspRoutes.mu.RLock()
	recordCount := endpoint.aspRoutes.stateRecordCount
	congestionRecords := len(endpoint.aspRoutes.congestion)
	endpoint.aspRoutes.mu.RUnlock()
	if recordCount != 1 || congestionRecords != 0 {
		t.Fatalf("congestion overflow left count %d and %d congestion records, want 1 and 0", recordCount, congestionRecords)
	}
	if state := first.DestinationStateForNetworkAndRoutingContext(7, 1, 0x123456); state != DestinationUnavailable {
		t.Fatalf("congestion overflow changed Association destination state to %v", state)
	}
}

func TestASPRouteStateBudgetIsAtomicUnderConcurrentSSNM(t *testing.T) {
	config := validASPConfig()
	config.MaxAffectedPointCodesPerSSNM = 1
	config.MaxSSNMStateRecordsPerRoute = 8
	config.MaxSSNMStateRecords = 8
	endpoint, first, _ := newASPMultiSGFixtureWithConfig(t, config)
	const attempts = 32
	results := make(chan error, attempts)
	for offset := uint32(0); offset < attempts; offset++ {
		go func(pointCode uint32) {
			results <- first.handleDestinationUnavailable(messages.NewDestinationUnavailable(
				params.NewNetworkAppearance(7),
				params.NewRoutingContext(1),
				params.NewAffectedPointCode(pointCode),
				nil,
			))
		}(0x120000 + offset)
	}
	succeeded := 0
	rejected := 0
	for range attempts {
		err := <-results
		switch {
		case err == nil:
			succeeded++
		case errors.Is(err, ErrASPRouteStateLimit):
			rejected++
		default:
			t.Fatalf("concurrent SSNM error = %v", err)
		}
	}
	if succeeded != 8 || rejected != attempts-8 {
		t.Fatalf("concurrent SSNM results = %d succeeded, %d rejected", succeeded, rejected)
	}
	endpoint.aspRoutes.mu.RLock()
	recordCount := endpoint.aspRoutes.stateRecordCount
	availabilityRecords := len(endpoint.aspRoutes.availability)
	endpoint.aspRoutes.mu.RUnlock()
	if recordCount != 8 || availabilityRecords != 8 {
		t.Fatalf("concurrent SSNM retained count %d and %d records, want 8 and 8", recordCount, availabilityRecords)
	}
}

func TestASPRouteAggregationSeparatesMTPRoutes(t *testing.T) {
	config := validASPConfig()
	config.MTPRoutes = append(config.MTPRoutes, MTPRouteConfig{
		ID:                   "sccp-b",
		DestinationPointCode: 0x340000,
		Mask:                 16,
		ServiceIndicators:    []uint8{3},
	})
	for gatewayIndex := range config.SignallingGateways {
		for sgpIndex := range config.SignallingGateways[gatewayIndex].SGPs {
			config.SignallingGateways[gatewayIndex].SGPs[sgpIndex].Routes = append(
				config.SignallingGateways[gatewayIndex].SGPs[sgpIndex].Routes,
				SGPRoute{
					MTPRoute: "sccp-b",
					AS:       config.SignallingGateways[gatewayIndex].SGPs[sgpIndex].Routes[0].AS,
				},
			)
		}
	}
	endpoint, first, second := newASPMultiSGFixtureWithConfig(t, config)

	applyASPDUNA(t, first, 7, 1, 0x123456, 0)
	applyASPDUNA(t, second, 9, 42, 0x123456, 0)
	requireASPRouteStatus(t, endpoint, "sccp-a", 0x123456, DestinationUnavailable, false, 0, false)
	requireASPRouteStatus(t, endpoint, "sccp-b", 0x345678, DestinationAvailable, false, 0, false)
}

func TestASPRouteRenumberPreservesUpdateOrder(t *testing.T) {
	endpoint, _, _ := newASPMultiSGFixture(t)
	routes := endpoint.aspRoutes
	older := aspRouteRangeKey{
		signallingGateway: "sg-a",
		mtpRoute:          "sccp-a",
		pointCode:         0x120000,
		mask:              16,
	}
	newer := aspRouteRangeKey{
		signallingGateway: "sg-a",
		mtpRoute:          "sccp-a",
		pointCode:         0x123456,
		mask:              0,
	}
	routes.mu.Lock()
	defer routes.mu.Unlock()
	routes.availability[older] = aspAvailabilityRecord{sequence: 100}
	routes.availability[newer] = aspAvailabilityRecord{sequence: 200}
	routes.congestion[older] = aspCongestionRecord{sequence: 300}
	routes.congestion[newer] = aspCongestionRecord{sequence: 400}
	routes.renumberLocked()
	if routes.availability[older].sequence >= routes.availability[newer].sequence {
		t.Fatal("availability update order changed during sequence renumbering")
	}
	if routes.congestion[older].sequence >= routes.congestion[newer].sequence {
		t.Fatal("congestion update order changed during sequence renumbering")
	}
	olderNode := routes.stateIndexNodeForRangeLocked(older)
	newerNode := routes.stateIndexNodeForRangeLocked(newer)
	if olderNode.availability["sg-a"].sequence != routes.availability[older].sequence ||
		newerNode.availability["sg-a"].sequence != routes.availability[newer].sequence ||
		olderNode.congestion["sg-a"].sequence != routes.congestion[older].sequence ||
		newerNode.congestion["sg-a"].sequence != routes.congestion[newer].sequence {
		t.Fatal("route-state index sequence changed independently from retained records")
	}
}

func newASPMultiSGFixture(t *testing.T) (*Endpoint, *Association, *Association) {
	t.Helper()
	return newASPMultiSGFixtureWithConfig(t, validASPConfig())
}

func newASPMultiSGFixtureWithConfig(t *testing.T, config *ASPConfig) (*Endpoint, *Association, *Association) {
	t.Helper()
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: config})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	first := attachASPRouteAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}, 7, 1)
	second := attachASPRouteAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-b",
		SignallingGatewayProcess: "sgp-b1",
	}, 9, 42)
	return endpoint, first, second
}

func attachASPRouteAssociation(t *testing.T, endpoint *Endpoint, identity SGPIdentity, networkAppearance, routingContext uint32) *Association {
	t.Helper()
	association, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, routingContext)
	association.cfg.NetworkAppearance = params.NewNetworkAppearance(networkAppearance)
	association.cfg.PeerSGP = &identity
	association.noteRoutingContextsAcked(params.NewRoutingContext(routingContext))
	if !endpoint.trackAssociation(association) {
		t.Fatal("failed to attach ASP Association")
	}
	t.Cleanup(func() {
		_ = association.Close()
	})
	return association
}

func requireASPRouteStatus(
	t *testing.T,
	endpoint *Endpoint,
	mtpRoute MTPRouteID,
	pointCode uint32,
	availability DestinationState,
	congested bool,
	congestionLevel uint8,
	congestionLevelSet bool,
) {
	t.Helper()
	status, known := endpoint.aspRoutes.destinationStatus(mtpRoute, pointCode, 0)
	if !known {
		t.Fatalf("destination %#x in MTP Route %q is unknown", pointCode, mtpRoute)
	}
	if status.availability != availability ||
		status.congested != congested ||
		status.congestionLevel != congestionLevel ||
		status.congestionLevelSet != congestionLevelSet {
		t.Fatalf("destination %#x status = %#v, want availability=%v congested=%v level=%d set=%v",
			pointCode, status, availability, congested, congestionLevel, congestionLevelSet)
	}
}

func applyASPDUNA(t *testing.T, association *Association, networkAppearance, routingContext, pointCode uint32, mask uint8) {
	t.Helper()
	err := association.handleDestinationUnavailable(messages.NewDestinationUnavailable(
		params.NewNetworkAppearance(networkAppearance),
		params.NewRoutingContext(routingContext),
		params.NewAffectedPointCodeWithMask(mask, pointCode),
		nil,
	))
	if err != nil {
		t.Fatalf("handleDestinationUnavailable: %v", err)
	}
}

func applyASPDAVA(t *testing.T, association *Association, networkAppearance, routingContext, pointCode uint32, mask uint8) {
	t.Helper()
	err := association.handleDestinationAvailable(messages.NewDestinationAvailable(
		params.NewNetworkAppearance(networkAppearance),
		params.NewRoutingContext(routingContext),
		params.NewAffectedPointCodeWithMask(mask, pointCode),
		nil,
	))
	if err != nil {
		t.Fatalf("handleDestinationAvailable: %v", err)
	}
}

func applyASPDRST(t *testing.T, association *Association, networkAppearance, routingContext, pointCode uint32, mask uint8) {
	t.Helper()
	err := association.handleDestinationRestricted(messages.NewDestinationRestricted(
		params.NewNetworkAppearance(networkAppearance),
		params.NewRoutingContext(routingContext),
		params.NewAffectedPointCodeWithMask(mask, pointCode),
		nil,
	))
	if err != nil {
		t.Fatalf("handleDestinationRestricted: %v", err)
	}
}

func applyASPSCON(
	t *testing.T,
	association *Association,
	networkAppearance,
	routingContext,
	pointCode uint32,
	mask uint8,
	congestion *params.Param,
) {
	t.Helper()
	err := association.handleSignallingCongestion(messages.NewSignallingCongestion(
		params.NewNetworkAppearance(networkAppearance),
		params.NewRoutingContext(routingContext),
		params.NewAffectedPointCodeWithMask(mask, pointCode),
		nil,
		congestion,
		nil,
	))
	if err != nil {
		t.Fatalf("handleSignallingCongestion: %v", err)
	}
}

func FuzzASPRouteIntersection(f *testing.F) {
	for _, seed := range []struct {
		routePointCode  uint32
		routeMask       uint8
		statusPointCode uint32
		statusMask      uint8
	}{
		{0x123456, 0, 0x123456, 0},
		{0x120000, 16, 0x123456, 0},
		{0, 24, 0xabcdef, 8},
		{0x120000, 16, 0x340000, 16},
		{0x120000, 16, 0, 255},
	} {
		f.Add(seed.routePointCode, seed.routeMask, seed.statusPointCode, seed.statusMask)
	}

	f.Fuzz(func(t *testing.T, routePointCode uint32, routeMask uint8, statusPointCode uint32, statusMask uint8) {
		routeMask %= 25
		routePointCode = destinationRangePrefix(routePointCode, routeMask)
		statusPointCode &= 0x00ffffff
		mtpRoute := aspMTPRoute{destinationPointCode: routePointCode, mask: routeMask}

		pointCode, mask, overlaps := aspRouteIntersection(mtpRoute, statusPointCode, statusMask)
		routeLow, routeHigh := aspTestRange(routePointCode, routeMask)
		statusLow, statusHigh := aspTestRange(statusPointCode, effectiveDestinationMask(statusMask))
		wantOverlap := routeLow <= statusHigh && statusLow <= routeHigh
		if overlaps != wantOverlap {
			t.Fatalf("overlap = %v, want %v for route %#x/%d and status %#x/%d",
				overlaps, wantOverlap, routePointCode, routeMask, statusPointCode, statusMask)
		}
		if !overlaps {
			return
		}
		intersectionLow, intersectionHigh := aspTestRange(pointCode, mask)
		wantLow := routeLow
		if statusLow > wantLow {
			wantLow = statusLow
		}
		wantHigh := routeHigh
		if statusHigh < wantHigh {
			wantHigh = statusHigh
		}
		if intersectionLow != wantLow || intersectionHigh != wantHigh {
			t.Fatalf("intersection %#x-%#x, want %#x-%#x", intersectionLow, intersectionHigh, wantLow, wantHigh)
		}
	})
}

func BenchmarkASPRouteSparseOverwriteRecompute(b *testing.B) {
	config := validASPConfig()
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: config})
	if err != nil {
		b.Fatalf("NewEndpoint: %v", err)
	}
	associationConfig := NewAssociationConfig(0, 0, 0, 0, 0, 0)
	associationConfig.NetworkAppearance = params.NewNetworkAppearance(7)
	associationConfig.RoutingContexts = params.NewRoutingContext(1)
	associationConfig.PeerSGP = &SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}
	association := &Association{
		cfg:     associationConfig,
		muState: new(sync.RWMutex),
		role:    RoleASP,
		state:   StateASPActive,
		done:    make(chan struct{}),
	}
	if !endpoint.aspRoutes.attach(association) {
		b.Fatal("attach benchmark Association")
	}
	routes := endpoint.aspRoutes
	routes.mu.Lock()
	for offset := uint32(0); offset < DefaultMaxSSNMStateRecordsPerRoute; offset++ {
		routes.sequence++
		key := aspRouteRangeKey{
			signallingGateway: "sg-a",
			mtpRoute:          "sccp-a",
			pointCode:         0x120000 + offset,
			mask:              0,
		}
		record := aspAvailabilityRecord{availability: DestinationUnavailable, sequence: routes.sequence}
		routes.availability[key] = record
		routes.indexAvailabilityRecordLocked(key, record)
	}
	routes.mu.Unlock()
	b.ResetTimer()
	for range b.N {
		routes.mu.Lock()
		_ = routes.recomputeLocked(map[MTPRouteID]struct{}{"sccp-a": {}})
		routes.mu.Unlock()
	}
}

func countASPRouteStateIndexNodes(node *aspRouteStateIndexNode) int {
	if node == nil {
		return 0
	}
	return 1 + countASPRouteStateIndexNodes(node.children[0]) + countASPRouteStateIndexNodes(node.children[1])
}

func aspTestRange(pointCode uint32, mask uint8) (uint32, uint32) {
	low := destinationRangePrefix(pointCode, mask)
	return low, low | lowPointCodeBits(effectiveDestinationMask(mask))
}
