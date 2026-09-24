// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/gomaja/go-m3ua/messages/params"
)

// aspRouteLargeReport is one SSNM report of routeCount destinations, the
// point codes aspRouteFailoverConfig gives its MTP Routes, in the primary
// Application Server's scope: what one DAVA or DUNA naming every route's
// destination delivers to the route registry.
func aspRouteLargeReport(routeCount int) []*destinationStatus {
	values := make([]destinationStatus, routeCount)
	statuses := make([]*destinationStatus, routeCount)
	for route := range values {
		values[route] = destinationStatus{
			PointCode:            0x220000 + uint32(route),
			NetworkAppearance:    7,
			NetworkAppearanceSet: true,
			RoutingContexts:      []uint32{1},
			RoutingContextSet:    true,
		}
		statuses[route] = &values[route]
	}
	return statuses
}

// BenchmarkASPRouteApplyLargeReport measures the routing-lock hold of one
// large SSNM report on the GitHub issue #125 topology: 1,000 MTP Routes over
// two Signalling Gateways of two SGPs with two Associations each, every route
// carried by every SGP, and a report from sg-a/p0 naming all 1,000 route
// destinations. Each op is two reports, unavailable then available, so every
// report changes the destination state of every route.
func BenchmarkASPRouteApplyLargeReport(b *testing.B) {
	const routeCount = 1000
	routes, associations := newASPRouteFailoverFixture(b, routeCount, routeCount)
	association := associations[aspRouteFailoverSGP][0]
	statuses := aspRouteLargeReport(routeCount)
	unavailable := aspRouteUpdate{kind: aspRouteAvailabilityUpdate, availability: DestinationUnavailable}
	available := aspRouteUpdate{kind: aspRouteAvailabilityUpdate, availability: DestinationAvailable}
	b.ResetTimer()
	for range b.N {
		if err := routes.apply(association, statuses, unavailable); err != nil {
			b.Fatal(err)
		}
		if err := routes.apply(association, statuses, available); err != nil {
			b.Fatal(err)
		}
	}
}

// referenceReportRanges is the route-range resolution apply performed before
// it resolved each wire scope once: the candidate walk for every status and
// every route of the SGP.
func referenceReportRanges(
	c *aspRoutingConfig,
	association *Association,
	identity SGPIdentity,
	sgp aspSGPConfig,
	statuses []*destinationStatus,
) ([]aspRouteRangeKey, map[MTPRouteID]struct{}) {
	var keys []aspRouteRangeKey
	seen := make(map[aspRouteRangeKey]struct{})
	affected := make(map[MTPRouteID]struct{})
	for _, status := range statuses {
		if status == nil {
			continue
		}
		for _, routeID := range sgp.routeOrder {
			if !c.routeCandidateMatchesStatus(association, identity, routeID, status) {
				continue
			}
			mtpRoute, ok := c.mtpRoute(routeID)
			if !ok {
				continue
			}
			pointCode, mask, overlaps := aspRouteIntersection(mtpRoute, status.PointCode, status.Mask)
			if !overlaps {
				continue
			}
			key := aspRouteRangeKey{signallingGateway: identity.SignallingGateway, mtpRoute: routeID, pointCode: pointCode, mask: mask}
			if _, duplicate := seen[key]; duplicate {
				continue
			}
			seen[key] = struct{}{}
			keys = append(keys, key)
			affected[routeID] = struct{}{}
		}
	}
	return keys, affected
}

// Resolving each wire scope once names exactly the route ranges, in exactly
// the order, that the walk for every status named, across reports that mix
// scopes, masks, duplicates and statuses no route carries.
func TestReportRangesMatchesThePerStatusWalk(t *testing.T) {
	routes, associations := newASPRouteFailoverFixture(t, 96, 48)
	scopes := []destinationStatus{
		{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContexts: []uint32{1}, RoutingContextSet: true},
		{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContexts: []uint32{2}, RoutingContextSet: true},
		{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContexts: []uint32{1, 2}, RoutingContextSet: true},
		{NetworkAppearance: 7, NetworkAppearanceSet: true},
		{RoutingContexts: []uint32{2}, RoutingContextSet: true},
		{},
		{NetworkAppearance: 8, NetworkAppearanceSet: true, RoutingContexts: []uint32{1}, RoutingContextSet: true},
		// An unset Network Appearance is replaced by the Association's own,
		// whatever value the field holds, so this names the scope the set one
		// above does not.
		{NetworkAppearance: 8, RoutingContexts: []uint32{1}, RoutingContextSet: true},
		{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContexts: []uint32{999}, RoutingContextSet: true},
	}
	masks := []uint8{0, 0, 0, 1, 3, 7, 24}
	rng := rand.New(rand.NewSource(147))
	for step := 0; step < 400; step++ {
		identity := []SGPIdentity{
			{SignallingGateway: "sg-a", SignallingGatewayProcess: "p0"},
			{SignallingGateway: "sg-b", SignallingGatewayProcess: "p1"},
		}[rng.Intn(2)]
		association := associations[identity][rng.Intn(2)]
		sgp := routes.config.sgpByIdentity[identity]
		statuses := make([]*destinationStatus, 1+rng.Intn(40))
		for index := range statuses {
			if rng.Intn(20) == 0 {
				continue
			}
			status := scopes[rng.Intn(len(scopes))]
			if rng.Intn(3) != 0 {
				status = scopes[0]
			}
			status.PointCode = 0x220000 + uint32(rng.Intn(128))
			status.Mask = masks[rng.Intn(len(masks))]
			statuses[index] = &status
		}
		gotKeys, gotAffected := routes.config.reportRanges(association, identity, sgp, statuses)
		wantKeys, wantAffected := referenceReportRanges(&routes.config, association, identity, sgp, statuses)
		if len(gotKeys) != len(wantKeys) || (len(wantKeys) != 0 && !reflect.DeepEqual(gotKeys, wantKeys)) ||
			!reflect.DeepEqual(gotAffected, wantAffected) {
			t.Fatalf("step %d: ranges %v routes %v, want %v routes %v", step, gotKeys, gotAffected, wantKeys, wantAffected)
		}
	}
}

// A report whose Association detaches after its ranges were resolved, and
// before they are written, belongs to no route: nothing is recorded, as when
// it arrives after detach.
func TestApplyDropsAReportWhoseAssociationDetachedMeanwhile(t *testing.T) {
	routes, associations := newASPRouteFailoverFixture(t, 16, 16)
	association := associations[aspRouteFailoverSGP][0]
	routes.applyResolved = func() { routes.detach(association) }
	if err := routes.apply(association, aspRouteLargeReport(16),
		aspRouteUpdate{kind: aspRouteAvailabilityUpdate, availability: DestinationUnavailable}); err != nil {
		t.Fatal(err)
	}
	routes.mu.RLock()
	defer routes.mu.RUnlock()
	if len(routes.availability) != 0 || routes.stateRecordCount != 0 {
		t.Fatalf("a report from a detached Association left %d availability records (%d counted)",
			len(routes.availability), routes.stateRecordCount)
	}
}

// The same comparison on Associations that exercise every way a wire scope
// resolves: one configured Routing Context or none, no Network Appearance,
// Routing Contexts an RFC 4666 Section 4.4.1 registration bound to an
// Application Server, and MTP Routes with masks. A report whose Routing
// Context parameter is absent and one whose parameter is empty resolve
// differently for an Association configured with a single Routing Context.
func TestReportRangesMatchesThePerStatusWalkWithRegistrationsAndMaskedRoutes(t *testing.T) {
	config := &ASPConfig{
		SignallingGateways: []SignallingGatewayConfig{{ID: "sg-a", SGPs: []SignallingGatewayProcessConfig{{
			ID: "p0",
			ApplicationServers: []RemoteASConfig{
				{ID: "static1", ASKey: staticASKey(7, 1)},
				{ID: "dyn", RoutingKey: &RoutingKey{Groups: []RoutingKeyGroup{{DestinationPointCode: 0x120000}}}},
				{ID: "dyn2", RoutingKey: &RoutingKey{Groups: []RoutingKeyGroup{{DestinationPointCode: 0x130000}}}},
			},
		}}}},
		Routing: &ASPRoutingConfig{
			SignallingGatewaySelection:        RouteSelectionLoadshare,
			SignallingGatewayProcessSelection: map[SignallingGatewayID]RouteSelectionMode{"sg-a": RouteSelectionLoadshare},
			AllowUnknownDestinations:          true,
			Paths: []MTPRoutePath{
				{ID: "p-static", SignallingGateway: "sg-a", ApplicationServers: []RemoteASID{"static1"}},
				{ID: "p-dyn", SignallingGateway: "sg-a", ApplicationServers: []RemoteASID{"dyn", "dyn2"}},
				{ID: "p-dyn2", SignallingGateway: "sg-a", ApplicationServers: []RemoteASID{"dyn2"}},
			},
		},
	}
	// Routes on the dynamically bound paths only are those whose index is 1
	// or 2 modulo the path sets.
	pathSets := [][]MTPRoutePathID{{"p-static"}, {"p-dyn"}, {"p-dyn2"}, {"p-static", "p-dyn"}, {"p-static", "p-dyn2"}}
	routeMasks := []uint8{0, 2, 3}
	dynamicOnly := make(map[MTPRouteID]bool)
	for route := 0; route < 40; route++ {
		id := MTPRouteID(fmt.Sprintf("r-%03d", route))
		dynamicOnly[id] = route%len(pathSets) == 1 || route%len(pathSets) == 2
		config.Routing.MTPRoutes = append(config.Routing.MTPRoutes, MTPRouteConfig{
			ID:                   id,
			DestinationPointCode: 0x220000 + uint32(route)*8,
			Mask:                 routeMasks[route%len(routeMasks)],
			Paths:                pathSets[route%len(pathSets)],
		})
	}
	routes, err := newASPRoutes(config)
	if err != nil {
		t.Fatal(err)
	}
	identity := SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "p0"}
	sgp := routes.config.sgpByIdentity[identity]

	variants := []struct {
		appearance      *params.Param
		routingContexts []uint32
		registered      map[uint32]RemoteASID
	}{
		{appearance: params.NewNetworkAppearance(7), routingContexts: []uint32{1}},
		{appearance: params.NewNetworkAppearance(7), routingContexts: []uint32{1}, registered: map[uint32]RemoteASID{9: "dyn", 11: "dyn2", 13: "dyn"}},
		{routingContexts: []uint32{1}, registered: map[uint32]RemoteASID{9: "dyn"}},
		{registered: map[uint32]RemoteASID{9: "dyn"}},
		{appearance: params.NewNetworkAppearance(7), registered: map[uint32]RemoteASID{11: "dyn2"}},
	}
	var associations []*Association
	for _, variant := range variants {
		association, _ := newTestConn(t, StateASPActive, RoleASP)
		setInventoryNetworkAppearance(&association.cfg.ApplicationServers, variant.appearance)
		if variant.routingContexts != nil {
			setInventoryRoutingContexts(&association.cfg.ApplicationServers, params.NewRoutingContext(variant.routingContexts...))
		} else {
			setInventoryRoutingContexts(&association.cfg.ApplicationServers, nil)
		}
		association.cfg.PeerSGP = &identity
		for routingContext, remoteAS := range variant.registered {
			association.addDynamicASKey(ASKey{RoutingContext: routingContext, RoutingContextSet: true}, RoutingKey{}, false)
			association.noteCanonicalRemoteAS(routingContext, remoteAS)
		}
		associations = append(associations, association)
	}

	scopes := []destinationStatus{
		{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContexts: []uint32{1}, RoutingContextSet: true},
		{NetworkAppearance: 7, NetworkAppearanceSet: true},
		{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContextSet: true},
		{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContexts: []uint32{}, RoutingContextSet: true},
		{},
		{RoutingContextSet: true},
		{RoutingContexts: []uint32{9}, RoutingContextSet: true},
		{RoutingContexts: []uint32{11}, RoutingContextSet: true},
		{RoutingContexts: []uint32{13, 9}, RoutingContextSet: true},
		{RoutingContexts: []uint32{9, 13}, RoutingContextSet: true},
		{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContexts: []uint32{9}, RoutingContextSet: true},
		{RoutingContexts: []uint32{1, 11}, RoutingContextSet: true},
		{RoutingContexts: []uint32{999}, RoutingContextSet: true},
		{NetworkAppearance: 8, NetworkAppearanceSet: true},
	}
	masks := []uint8{0, 1, 2, 3, 4, 5, 8, 24}
	rng := rand.New(rand.NewSource(7))
	registeredHits := 0
	for step := 0; step < 3000; step++ {
		association := associations[rng.Intn(len(associations))]
		statuses := make([]*destinationStatus, 1+rng.Intn(30))
		for index := range statuses {
			if rng.Intn(15) == 0 {
				continue
			}
			status := scopes[rng.Intn(len(scopes))]
			status.PointCode = 0x220000 + uint32(rng.Intn(360))
			status.Mask = masks[rng.Intn(len(masks))]
			statuses[index] = &status
		}
		gotKeys, gotAffected := routes.config.reportRanges(association, identity, sgp, statuses)
		wantKeys, wantAffected := referenceReportRanges(&routes.config, association, identity, sgp, statuses)
		if len(gotKeys) != len(wantKeys) || (len(wantKeys) != 0 && !reflect.DeepEqual(gotKeys, wantKeys)) ||
			!reflect.DeepEqual(gotAffected, wantAffected) {
			t.Fatalf("step %d: ranges %v routes %v, want %v routes %v", step, gotKeys, gotAffected, wantKeys, wantAffected)
		}
		for _, key := range gotKeys {
			if dynamicOnly[key.mtpRoute] {
				registeredHits++
			}
		}
	}
	if registeredHits == 0 {
		t.Fatal("no report named a route carried only by a registered Application Server")
	}
}
