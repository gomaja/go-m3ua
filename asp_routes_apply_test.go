// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"math/rand"
	"reflect"
	"testing"
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
