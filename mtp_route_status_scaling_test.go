// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Reference implementation (GitHub issue #111).
//
// referenceMTPDestinationStatuses, referenceMTPRouteStatus and
// referenceMTPRouteStatuses are verbatim copies of the pre-fix
// aspRoutes.mtpDestinationStatuses, Endpoint.MTPRouteStatus and
// Endpoint.MTPRouteStatuses bodies, renamed so they keep running the old,
// O(routes) x O(derived) x O(routes) algorithm regardless of how the live
// implementation in asp_routes.go / endpoint_status.go changes. They are the
// equivalence oracle every fix in this file is checked against.
// ---------------------------------------------------------------------------

func referenceMTPDestinationStatuses(r *aspRoutes) []MTPDestinationStatus {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()

	statuses := make([]MTPDestinationStatus, 0, len(r.derived))
	for _, mtpRoute := range r.config.mtpRoutes {
		keys := make([]aspDerivedRangeKey, 0)
		for key := range r.derived {
			if key.mtpRoute == mtpRoute.id {
				keys = append(keys, key)
			}
		}
		sort.Slice(keys, func(first, second int) bool {
			if keys[first].pointCode != keys[second].pointCode {
				return keys[first].pointCode < keys[second].pointCode
			}
			return keys[first].mask < keys[second].mask
		})
		for _, key := range keys {
			statuses = append(statuses, newMTPDestinationStatus(MTPDestination{
				MTPRoute:  key.mtpRoute,
				PointCode: key.pointCode,
				Mask:      key.mask,
			}, r.derived[key]))
		}
	}
	return statuses
}

func referenceMTPRouteStatus(e *Endpoint, id MTPRouteID) (MTPRouteStatus, bool) {
	if e == nil || e.role != RoleASP || e.aspRoutes == nil {
		return MTPRouteStatus{}, false
	}
	routes := e.aspRoutes
	routes.mu.RLock()
	if _, exists := routes.config.mtpRouteByID[id]; !exists {
		routes.mu.RUnlock()
		return MTPRouteStatus{}, false
	}
	associations := make([]AssociationID, 0)
	for association, eligible := range routes.associationEligibleRoutes {
		if _, ok := eligible[id]; ok && association.ID() != 0 {
			associations = append(associations, association.ID())
		}
	}
	routes.mu.RUnlock()
	sort.Slice(associations, func(i, j int) bool { return associations[i] < associations[j] })

	allDestinations := referenceMTPDestinationStatuses(routes)
	destinations := make([]MTPDestinationStatus, 0)
	for _, destination := range allDestinations {
		if destination.Destination.MTPRoute == id {
			destinations = append(destinations, destination)
		}
	}
	return MTPRouteStatus{
		MTPRoute:     id,
		Destinations: destinations,
		Associations: associations,
	}, true
}

func referenceMTPRouteStatuses(e *Endpoint) []MTPRouteStatus {
	if e == nil || e.role != RoleASP || e.aspRoutes == nil {
		return nil
	}
	e.aspRoutes.mu.RLock()
	ids := make([]MTPRouteID, 0, len(e.aspRoutes.config.mtpRoutes))
	for _, route := range e.aspRoutes.config.mtpRoutes {
		ids = append(ids, route.id)
	}
	e.aspRoutes.mu.RUnlock()
	statuses := make([]MTPRouteStatus, 0, len(ids))
	for _, id := range ids {
		if status, ok := referenceMTPRouteStatus(e, id); ok {
			statuses = append(statuses, status)
		}
	}
	return statuses
}

// ---------------------------------------------------------------------------
// Equivalence oracle: the live implementation must match the reference
// implementation exactly, in order, over randomized inventories, and every
// nested slice it returns must be caller-owned.
// ---------------------------------------------------------------------------

type routeStatusGateway struct {
	id   SignallingGatewayID
	sgp  SignallingGatewayProcessID
	path MTPRoutePathID
}

// randomMTPRouteStatusInventory builds a randomized ASP Endpoint with 1-2
// Signalling Gateways, 4-10 MTP Routes (some deliberately overlapping in
// destination point code and mask), 0-3 attached Associations in varied
// states, and 0-19 directly injected SSNM-style availability/congestion
// records nested at random sub-ranges of a route (so aspRoutes.derived ends up
// with more than one leaf per route, exactly the shape the cubic defect in
// issue #111 was measured against).
func randomMTPRouteStatusInventory(t *testing.T, rng *rand.Rand) (*Endpoint, []MTPRouteID) {
	t.Helper()

	gatewayCount := 1 + rng.Intn(2)
	gatewayInfos := make([]routeStatusGateway, gatewayCount)
	gateways := make([]SignallingGatewayConfig, gatewayCount)
	paths := make([]MTPRoutePath, gatewayCount)
	sgpSelection := make(map[SignallingGatewayID]RouteSelectionMode, gatewayCount)
	for g := 0; g < gatewayCount; g++ {
		gwID := SignallingGatewayID(fmt.Sprintf("sg-%d", g))
		sgpID := SignallingGatewayProcessID(fmt.Sprintf("sgp-%d", g))
		pathID := MTPRoutePathID(fmt.Sprintf("path-%d", g))
		gatewayInfos[g] = routeStatusGateway{id: gwID, sgp: sgpID, path: pathID}
		gateways[g] = SignallingGatewayConfig{
			ID: gwID,
			SGPs: []SignallingGatewayProcessConfig{{
				ID:                 sgpID,
				ApplicationServers: []RemoteASConfig{{ID: "as-core", ASKey: &ASKey{}}},
			}},
		}
		paths[g] = MTPRoutePath{ID: pathID, SignallingGateway: gwID, ApplicationServers: []RemoteASID{"as-core"}}
		sgpSelection[gwID] = RouteSelectionLoadshare
	}

	routeCount := 4 + rng.Intn(7)
	mtpRoutes := make([]MTPRouteConfig, routeCount)
	routeIDs := make([]MTPRouteID, routeCount)
	routeGateways := make([][]routeStatusGateway, routeCount)
	usedPath := make(map[MTPRoutePathID]bool, gatewayCount)

	var lastPointCode uint32
	var lastMask uint8
	havePrevious := false

	for i := 0; i < routeCount; i++ {
		id := MTPRouteID(fmt.Sprintf("route-%02d", i))
		routeIDs[i] = id

		var pointCode uint32
		var mask uint8
		if havePrevious && rng.Intn(3) == 0 {
			// Deliberately overlap a previous route's configured range.
			pointCode, mask = lastPointCode, lastMask
		} else {
			mask = uint8(rng.Intn(25))
			pointCode = destinationRangePrefix(uint32(rng.Intn(1<<24)), mask)
		}
		lastPointCode, lastMask, havePrevious = pointCode, mask, true

		var servedBy []routeStatusGateway
		if gatewayCount == 1 || rng.Intn(2) == 0 {
			servedBy = []routeStatusGateway{gatewayInfos[rng.Intn(gatewayCount)]}
		} else {
			servedBy = append([]routeStatusGateway(nil), gatewayInfos...)
		}
		routeGateways[i] = servedBy

		routePaths := make([]MTPRoutePathID, 0, len(servedBy))
		for _, gw := range servedBy {
			routePaths = append(routePaths, gw.path)
			usedPath[gw.path] = true
		}

		mtpRoutes[i] = MTPRouteConfig{
			ID:                   id,
			DestinationPointCode: pointCode,
			Mask:                 mask,
			Paths:                routePaths,
		}
	}
	for g, info := range gatewayInfos {
		if usedPath[info.path] {
			continue
		}
		idx := rng.Intn(routeCount)
		mtpRoutes[idx].Paths = append(mtpRoutes[idx].Paths, info.path)
		routeGateways[idx] = append(routeGateways[idx], gatewayInfos[g])
	}

	config := &ASPConfig{
		SignallingGateways: gateways,
		Routing: &ASPRoutingConfig{
			SignallingGatewaySelection:        RouteSelectionLoadshare,
			SignallingGatewayProcessSelection: sgpSelection,
			Paths:                             paths,
			MTPRoutes:                         mtpRoutes,
		},
	}

	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: config})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })

	routes := endpoint.aspRoutes
	assocCount := rng.Intn(4)
	states := []State{StateASPActive, StateASPActive, StateASPDown, StateASPInactive}
	for a := 0; a < assocCount; a++ {
		gw := gatewayInfos[rng.Intn(gatewayCount)]
		association := &Association{
			cfg:     NewAssociationConfig(),
			muState: new(sync.RWMutex),
			role:    RoleASP,
			state:   states[rng.Intn(len(states))],
			done:    make(chan struct{}),
		}
		association.cfg.PeerSGP = &SGPIdentity{SignallingGateway: gw.id, SignallingGatewayProcess: gw.sgp}
		if routes.attach(association) {
			association.managementID.Store(uint64(a + 1))
		}
	}

	routes.mu.Lock()
	only := make(map[MTPRouteID]struct{}, routeCount)
	recordCount := rng.Intn(20)
	for n := 0; n < recordCount; n++ {
		routeIdx := rng.Intn(routeCount)
		route := mtpRoutes[routeIdx]
		servedBy := routeGateways[routeIdx]
		if len(servedBy) == 0 {
			continue
		}
		gw := servedBy[rng.Intn(len(servedBy))]

		subMask := uint8(rng.Intn(int(route.Mask) + 1))
		var free uint32
		if extra := route.Mask - subMask; extra > 0 {
			free = uint32(rng.Intn(1<<extra)) << subMask
		}
		pointCode := destinationRangePrefix(route.DestinationPointCode|free, subMask)

		key := aspRouteRangeKey{
			signallingGateway: gw.id,
			mtpRoute:          route.ID,
			pointCode:         pointCode,
			mask:              subMask,
		}
		routes.sequence++
		if rng.Intn(2) == 0 {
			record := aspAvailabilityRecord{
				availability: []DestinationAvailability{DestinationAvailable, DestinationUnavailable, DestinationRestricted}[rng.Intn(3)],
				sequence:     routes.sequence,
			}
			routes.availability[key] = record
			routes.indexAvailabilityRecordLocked(key, record)
		} else {
			record := aspCongestionRecord{
				congested: rng.Intn(2) == 0,
				level:     uint8(rng.Intn(4)),
				levelSet:  rng.Intn(2) == 0,
				sequence:  routes.sequence,
			}
			routes.congestion[key] = record
			routes.indexCongestionRecordLocked(key, record)
		}
		only[route.ID] = struct{}{}
	}
	_ = routes.recomputeLocked(only)
	routes.mu.Unlock()

	return endpoint, routeIDs
}

func TestMTPRouteStatusEquivalence(t *testing.T) {
	const iterations = 150
	for seed := int64(0); seed < iterations; seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			endpoint, routeIDs := randomMTPRouteStatusInventory(t, rng)

			// mtpDestinationStatuses (and its per-route helper) back both
			// MTPRouteStatus(es) and Endpoint.MTPDestinationStatuses: check the
			// latter too, since it shares the exact code this fix rewrote.
			wantDestinations := referenceMTPDestinationStatuses(endpoint.aspRoutes)
			gotDestinations := endpoint.MTPDestinationStatuses()
			if !reflect.DeepEqual(wantDestinations, gotDestinations) {
				t.Fatalf("MTPDestinationStatuses() =\n%#v\nwant\n%#v", gotDestinations, wantDestinations)
			}

			wantAll := referenceMTPRouteStatuses(endpoint)
			gotAll := endpoint.MTPRouteStatuses()
			if !reflect.DeepEqual(wantAll, gotAll) {
				t.Fatalf("MTPRouteStatuses() =\n%#v\nwant\n%#v", gotAll, wantAll)
			}

			for _, id := range routeIDs {
				wantOne, wantOK := referenceMTPRouteStatus(endpoint, id)
				gotOne, gotOK := endpoint.MTPRouteStatus(id)
				if wantOK != gotOK {
					t.Fatalf("MTPRouteStatus(%q) ok = %v, want %v", id, gotOK, wantOK)
				}
				if !reflect.DeepEqual(wantOne, gotOne) {
					t.Fatalf("MTPRouteStatus(%q) =\n%#v\nwant\n%#v", id, gotOne, wantOne)
				}
			}

			if _, ok := endpoint.MTPRouteStatus(MTPRouteID("route-does-not-exist")); ok {
				t.Fatal("MTPRouteStatus reported an unconfigured MTP Route")
			}

			// Every nested slice must be caller-owned: mutating what one call
			// returned must not affect a later call or any internal state.
			mutateA := endpoint.MTPRouteStatuses()
			for i := range mutateA {
				for d := range mutateA[i].Destinations {
					mutateA[i].Destinations[d].Availability = DestinationRestricted
					mutateA[i].Destinations[d].CongestionLevel = 0xff
				}
				for a := range mutateA[i].Associations {
					mutateA[i].Associations[a] = AssociationID(0xdeadbeef)
				}
			}
			mutateB := endpoint.MTPRouteStatuses()
			if !reflect.DeepEqual(wantAll, mutateB) {
				t.Fatalf("mutating one MTPRouteStatuses() result affected a later call:\n%#v\nwant\n%#v", mutateB, wantAll)
			}

			mutateDestA := endpoint.MTPDestinationStatuses()
			for i := range mutateDestA {
				mutateDestA[i].Availability = DestinationRestricted
				mutateDestA[i].CongestionLevel = 0xff
			}
			mutateDestB := endpoint.MTPDestinationStatuses()
			if !reflect.DeepEqual(wantDestinations, mutateDestB) {
				t.Fatalf("mutating one MTPDestinationStatuses() result affected a later call:\n%#v\nwant\n%#v", mutateDestB, wantDestinations)
			}

			if len(routeIDs) > 0 {
				target := routeIDs[0]
				single, _ := endpoint.MTPRouteStatus(target)
				for d := range single.Destinations {
					single.Destinations[d].Availability = DestinationRestricted
				}
				for a := range single.Associations {
					single.Associations[a] = AssociationID(0xdeadbeef)
				}
				again, _ := endpoint.MTPRouteStatus(target)
				want, _ := referenceMTPRouteStatus(endpoint, target)
				if !reflect.DeepEqual(want, again) {
					t.Fatalf("mutating one MTPRouteStatus() result affected a later call:\n%#v\nwant\n%#v", again, want)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Scaling benchmarks and a failing-first ceiling test (GitHub issue #111).
// ---------------------------------------------------------------------------

// buildMTPRouteStatusPerfFixture provisions one Signalling Gateway carrying
// routeCount MTP Routes, one attached and ASP-ACTIVE Association making every
// route's destination capable, and a scattering of explicit SSNM-style
// DestinationUnavailable overrides (every 7th route) so aspRoutes.derived
// holds realistic, non-uniform state rather than one trivial identical leaf
// per route.
func buildMTPRouteStatusPerfFixture(tb testing.TB, routeCount int) (*Endpoint, []MTPRouteID) {
	tb.Helper()

	config := &ASPConfig{
		SignallingGateways: []SignallingGatewayConfig{
			{
				ID: "sg-a",
				SGPs: []SignallingGatewayProcessConfig{
					{
						ID:                 "sgp-a1",
						ApplicationServers: []RemoteASConfig{{ID: "as-core", ASKey: &ASKey{}}},
					},
				},
			},
		},
		Routing: &ASPRoutingConfig{
			SignallingGatewaySelection: RouteSelectionLoadshare,
			SignallingGatewayProcessSelection: map[SignallingGatewayID]RouteSelectionMode{
				"sg-a": RouteSelectionLoadshare,
			},
			Paths: []MTPRoutePath{
				{ID: "sg-a-core", SignallingGateway: "sg-a", ApplicationServers: []RemoteASID{"as-core"}},
			},
		},
	}
	ids := make([]MTPRouteID, routeCount)
	for i := 0; i < routeCount; i++ {
		id := MTPRouteID(fmt.Sprintf("route-%05d", i))
		ids[i] = id
		config.Routing.MTPRoutes = append(config.Routing.MTPRoutes, MTPRouteConfig{
			ID:                   id,
			DestinationPointCode: uint32(i),
			Mask:                 0,
			Paths:                []MTPRoutePathID{"sg-a-core"},
		})
	}

	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: config})
	if err != nil {
		tb.Fatalf("NewEndpoint: %v", err)
	}
	tb.Cleanup(func() { _ = endpoint.Close() })

	association := &Association{
		cfg:     NewAssociationConfig(),
		muState: new(sync.RWMutex),
		role:    RoleASP,
		state:   StateASPActive,
		done:    make(chan struct{}),
	}
	association.cfg.PeerSGP = &SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}
	routes := endpoint.aspRoutes
	if !routes.attach(association) {
		tb.Fatal("attach perf fixture Association")
	}
	association.managementID.Store(1)

	routes.mu.Lock()
	only := make(map[MTPRouteID]struct{})
	for i := 0; i < routeCount; i += 7 {
		routes.sequence++
		key := aspRouteRangeKey{
			signallingGateway: "sg-a",
			mtpRoute:          ids[i],
			pointCode:         uint32(i),
			mask:              0,
		}
		record := aspAvailabilityRecord{availability: DestinationUnavailable, sequence: routes.sequence}
		routes.availability[key] = record
		routes.indexAvailabilityRecordLocked(key, record)
		only[ids[i]] = struct{}{}
	}
	_ = routes.recomputeLocked(only)
	routes.mu.Unlock()

	return endpoint, ids
}

func BenchmarkMTPRouteStatuses(b *testing.B) {
	for _, routeCount := range []int{125, 250, 500, 1000} {
		b.Run(fmt.Sprintf("routes=%d", routeCount), func(b *testing.B) {
			endpoint, _ := buildMTPRouteStatusPerfFixture(b, routeCount)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = endpoint.MTPRouteStatuses()
			}
		})
	}
}

func BenchmarkMTPRouteStatus(b *testing.B) {
	for _, routeCount := range []int{125, 250, 500, 1000} {
		b.Run(fmt.Sprintf("routes=%d", routeCount), func(b *testing.B) {
			endpoint, ids := buildMTPRouteStatusPerfFixture(b, routeCount)
			target := ids[len(ids)/2]
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, _ = endpoint.MTPRouteStatus(target)
			}
		})
	}
}

func measureMTPRouteStatuses(tb testing.TB, endpoint *Endpoint, runs int) time.Duration {
	tb.Helper()
	_ = endpoint.MTPRouteStatuses() // warm-up
	start := time.Now()
	for i := 0; i < runs; i++ {
		_ = endpoint.MTPRouteStatuses()
	}
	return time.Since(start) / time.Duration(runs)
}

// TestMTPRouteStatusesScalesWithRouteCount is the failing-first scaling gate
// for issue #111. On the pre-fix O(routes) x O(derived) x O(routes)
// implementation, 1,000 routes measured ~10.1s per call (~30s for the 3 runs
// below), which blows both the absolute ceiling and the growth-ratio check.
// After the fix both are single-digit milliseconds.
func TestMTPRouteStatusesScalesWithRouteCount(t *testing.T) {
	if testing.Short() {
		t.Skip("scaling check allocates 250- and 1,000-route inventories")
	}

	smallEndpoint, _ := buildMTPRouteStatusPerfFixture(t, 250)
	smallElapsed := measureMTPRouteStatuses(t, smallEndpoint, 5)

	largeEndpoint, _ := buildMTPRouteStatusPerfFixture(t, 1000)
	largeElapsed := measureMTPRouteStatuses(t, largeEndpoint, 3)

	t.Logf("MTPRouteStatuses: 250 routes ~%s/call, 1,000 routes ~%s/call", smallElapsed, largeElapsed)

	const ceiling = 250 * time.Millisecond
	if largeElapsed > ceiling {
		t.Fatalf("MTPRouteStatuses over 1,000 routes averaged %s/call, want under %s", largeElapsed, ceiling)
	}

	// 1,000 routes is 4x 250 routes. Linear or n*log(n) growth stays well
	// under an order of magnitude; cubic growth (the historical defect) is
	// roughly 64x. The slack keeps this from flaking under CI jitter without
	// letting a quadratic or cubic regression back in unnoticed.
	const maxGrowth = 16.0
	if smallElapsed > 0 {
		growth := float64(largeElapsed) / float64(smallElapsed)
		if growth > maxGrowth {
			t.Fatalf("MTPRouteStatuses grew %.1fx from 250 to 1,000 routes (%s -> %s/call); want under %.1fx",
				growth, smallElapsed, largeElapsed, maxGrowth)
		}
	}
}
