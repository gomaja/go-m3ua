// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
)

// aspRouteFailoverSGP is the SGP whose Associations end in the failure these
// tests model.
var aspRouteFailoverSGP = SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "p0"}

// aspRouteFailoverConfig provisions the one-SGP-failure topology of GitHub
// issue #125: two Signalling Gateways of two SGPs each, every SGP serving a
// primary and a secondary Application Server, and routeCount MTP Routes of one
// point code each. The first sharedRoutes routes may use either Signalling
// Gateway and the rest only sg-b, so an Association of sg-a carries exactly
// sharedRoutes of them.
func aspRouteFailoverConfig(routeCount, sharedRoutes int) *ASPConfig {
	config := &ASPConfig{Routing: &ASPRoutingConfig{
		SignallingGatewaySelection:        RouteSelectionLoadshare,
		SignallingGatewayProcessSelection: make(map[SignallingGatewayID]RouteSelectionMode),
		AllowUnknownDestinations:          true,
	}}
	for _, gatewayID := range []SignallingGatewayID{"sg-a", "sg-b"} {
		gateway := SignallingGatewayConfig{ID: gatewayID}
		for _, processID := range []SignallingGatewayProcessID{"p0", "p1"} {
			gateway.SGPs = append(gateway.SGPs, SignallingGatewayProcessConfig{ID: processID,
				ApplicationServers: []RemoteASConfig{
					{ID: "primary", ASKey: staticASKey(7, 1)},
					{ID: "secondary", ASKey: staticASKey(7, 2)},
				}})
		}
		config.SignallingGateways = append(config.SignallingGateways, gateway)
		config.Routing.SignallingGatewayProcessSelection[gatewayID] = RouteSelectionLoadshare
		config.Routing.Paths = append(config.Routing.Paths, MTPRoutePath{
			ID:                 MTPRoutePathID(string(gatewayID) + "-path"),
			SignallingGateway:  gatewayID,
			ApplicationServers: []RemoteASID{"primary", "secondary"},
		})
	}
	for route := 0; route < routeCount; route++ {
		paths := []MTPRoutePathID{"sg-b-path"}
		if route < sharedRoutes {
			paths = []MTPRoutePathID{"sg-a-path", "sg-b-path"}
		}
		config.Routing.MTPRoutes = append(config.Routing.MTPRoutes, MTPRouteConfig{
			ID:                   MTPRouteID(fmt.Sprintf("route-%05d", route)),
			DestinationPointCode: 0x220000 + uint32(route),
			Paths:                paths,
		})
	}
	return config
}

// newASPRouteFailoverFixture attaches two ASP-ACTIVE Associations to every SGP
// of aspRouteFailoverConfig, which is the trial's eight.
func newASPRouteFailoverFixture(
	tb testing.TB,
	routeCount int,
	sharedRoutes int,
) (*aspRoutes, map[SGPIdentity][]*Association) {
	tb.Helper()
	config := aspRouteFailoverConfig(routeCount, sharedRoutes)
	routes, err := newASPRoutes(config)
	if err != nil {
		tb.Fatalf("newASPRoutes: %v", err)
	}
	associations := make(map[SGPIdentity][]*Association)
	for _, gateway := range config.SignallingGateways {
		for _, process := range gateway.SGPs {
			identity := SGPIdentity{SignallingGateway: gateway.ID, SignallingGatewayProcess: process.ID}
			for range 2 {
				associationConfig := NewAssociationConfig()
				setInventoryNetworkAppearance(&associationConfig.ApplicationServers, params.NewNetworkAppearance(7))
				setInventoryRoutingContexts(&associationConfig.ApplicationServers, params.NewRoutingContext(1, 2))
				associationConfig.PeerSGP = &identity
				association := &Association{
					cfg:     associationConfig,
					muState: new(sync.RWMutex),
					role:    RoleASP,
					state:   StateASPActive,
					done:    make(chan struct{}),
				}
				if !routes.attach(association) {
					tb.Fatalf("attach %s/%s Association", identity.SignallingGateway, identity.SignallingGatewayProcess)
				}
				associations[identity] = append(associations[identity], association)
			}
		}
	}
	return routes, associations
}

// changeASPRouteAssociationState moves one Association in or out of ASP-ACTIVE
// and has the route registry recompute every MTP Route whose eligibility that
// changed, which is what an Association ending does under the routing lock.
func changeASPRouteAssociationState(tb testing.TB, routes *aspRoutes, association *Association, state State) {
	tb.Helper()
	association.muState.Lock()
	association.state = state
	association.muState.Unlock()
	if !routes.associationStateChanged(association) {
		tb.Fatalf("moving an Association to %v recomputed no MTP Route", state)
	}
}

// fastestASPRouteAssociationEnd returns the fastest of up to runs recomputes
// for one sg-a/p0 Association ending, each taken while no other goroutine
// wants the routing lock, so it is the time that lock is held. It stops early
// once the timed recomputes have taken budget in total.
func fastestASPRouteAssociationEnd(
	tb testing.TB,
	routeCount int,
	sharedRoutes int,
	runs int,
	budget time.Duration,
) time.Duration {
	tb.Helper()
	routes, associations := newASPRouteFailoverFixture(tb, routeCount, sharedRoutes)
	association := associations[aspRouteFailoverSGP][0]
	fastest := time.Duration(math.MaxInt64)
	var total time.Duration
	for run := 0; run < runs && total < budget; run++ {
		association.muState.Lock()
		association.state = StateASPDown
		association.muState.Unlock()
		start := time.Now()
		changed := routes.associationStateChanged(association)
		elapsed := time.Since(start)
		if !changed {
			tb.Fatal("ending an Association recomputed no MTP Route")
		}
		changeASPRouteAssociationState(tb, routes, association, StateASPActive)
		total += elapsed
		if elapsed < fastest {
			fastest = elapsed
		}
	}
	return fastest
}

// TestASPRouteAssociationEndCostIsIndependentOfUnaffectedRoutes is the
// failing-first gate for GitHub issue #125. An Association ending recomputes
// the MTP Routes it carried while holding the routing lock that every
// MTP-TRANSFER takes, so that work must be proportional to those routes.
//
// Both inventories have the same 32 routes carried by the ending Association;
// they differ only in how many other routes exist. Before the fix every
// recomputed route scanned the derived state of every route three times, so
// the lock was held about 48x longer at 4,096 routes than at 64. After it, the
// only term that grows with the inventory is one pass over the configured
// routes, and the ratio measured under 2x. Taking the fastest of repeated
// recomputes keeps scheduling and garbage collection out of the ratio.
func TestASPRouteAssociationEndCostIsIndependentOfUnaffectedRoutes(t *testing.T) {
	if testing.Short() {
		t.Skip("scaling check builds a 4,096-route inventory")
	}
	const (
		affectedRoutes = 32
		smallInventory = 64
		largeInventory = 4096
	)
	small := fastestASPRouteAssociationEnd(t, smallInventory, affectedRoutes, 20, time.Second)
	large := fastestASPRouteAssociationEnd(t, largeInventory, affectedRoutes, 20, time.Second)
	growth := float64(large) / float64(small)
	t.Logf("ending an Association carrying %d routes held the routing lock %s among %d routes and %s among %d (%.1fx)",
		affectedRoutes, small, smallInventory, large, largeInventory, growth)

	const maxGrowth = 4.0
	if growth > maxGrowth {
		t.Fatalf("ending an Association carrying %d MTP Routes held the routing lock %.1fx longer among %d routes "+
			"than among %d (%s -> %s); want under %.1fx, since routes it did not carry must not add to its recompute",
			affectedRoutes, growth, largeInventory, smallInventory, small, large, maxGrowth)
	}
}

// referenceRecomputeLocked is aspRoutes.recomputeLocked as it was before
// GitHub issue #125, with derivedStatusIndicationsLocked inlined, reading and
// replacing each route's destinations in one caller-owned map of every route
// rather than in aspRoutes.derived. It is the oracle the per-route derived sets
// are held to.
func referenceRecomputeLocked(
	r *aspRoutes,
	derived map[aspDerivedRangeKey]aspDestinationStatus,
	only map[MTPRouteID]struct{},
) []*MTPIndication {
	var indications []*MTPIndication
	for _, mtpRoute := range r.config.mtpRoutes {
		if only != nil {
			if _, exists := only[mtpRoute.id]; !exists {
				continue
			}
		}
		updated := r.recomputeMTPRouteLocked(mtpRoute)
		previousTree := derivedStatusTree(mtpRoute, derived)
		currentTree := derivedStatusTree(mtpRoute, updated)
		routeIndications := make([]*MTPIndication, 0)
		appendDerivedStatusIndications(
			mtpRoute.id,
			mtpRoute.destinationPointCode,
			mtpRoute.mask,
			&previousTree,
			1,
			&currentTree,
			1,
			aspDestinationStatus{},
			false,
			aspDestinationStatus{},
			false,
			&routeIndications,
		)
		indications = append(indications, routeIndications...)
		for key := range derived {
			if key.mtpRoute == mtpRoute.id {
				delete(derived, key)
			}
		}
		for key, status := range updated {
			derived[key] = status
		}
	}
	return indications
}

// TestASPRouteRecomputeMatchesFlatDerivedReference holds recomputeLocked to
// the pre-#125 algorithm over randomized inventories: overlapping routes, SSNM
// records nested at random sub-ranges, and Associations moving between states,
// recomputed for every route or for a random subset. Every step must publish
// the same indications, in the same order, and leave the same derived
// destinations.
func TestASPRouteRecomputeMatchesFlatDerivedReference(t *testing.T) {
	const (
		iterations = 100
		steps      = 24
	)
	states := []State{StateASPActive, StateASPInactive, StateASPDown}
	availabilities := []DestinationAvailability{DestinationAvailable, DestinationRestricted, DestinationUnavailable}
	for seed := int64(0); seed < iterations; seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			endpoint, _ := randomMTPRouteStatusInventory(t, rng)
			routes := endpoint.aspRoutes
			routes.mu.Lock()
			defer routes.mu.Unlock()

			associations := make([]*Association, 0, len(routes.associations))
			for association := range routes.associations {
				associations = append(associations, association)
			}
			sort.Slice(associations, func(first, second int) bool {
				return routes.associationOrder[associations[first]] < routes.associationOrder[associations[second]]
			})
			reference := flatASPDerived(routes)

			for step := 0; step < steps; step++ {
				if len(associations) > 0 && rng.Intn(3) == 0 {
					association := associations[rng.Intn(len(associations))]
					association.muState.Lock()
					association.state = states[rng.Intn(len(states))]
					association.muState.Unlock()
				} else {
					mtpRoute := routes.config.mtpRoutes[rng.Intn(len(routes.config.mtpRoutes))]
					gateway := routes.config.signallingGateways[rng.Intn(len(routes.config.signallingGateways))]
					mask := uint8(rng.Intn(int(mtpRoute.mask) + 1))
					var free uint32
					if extra := mtpRoute.mask - mask; extra > 0 {
						free = uint32(rng.Intn(1<<extra)) << mask
					}
					key := aspRouteRangeKey{
						signallingGateway: gateway.id,
						mtpRoute:          mtpRoute.id,
						pointCode:         destinationRangePrefix(mtpRoute.destinationPointCode|free, mask),
						mask:              mask,
					}
					routes.sequence++
					if rng.Intn(2) == 0 {
						record := aspAvailabilityRecord{
							availability: availabilities[rng.Intn(len(availabilities))],
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
				}

				var only map[MTPRouteID]struct{}
				if rng.Intn(4) != 0 {
					only = make(map[MTPRouteID]struct{})
					for _, mtpRoute := range routes.config.mtpRoutes {
						if rng.Intn(2) == 0 {
							only[mtpRoute.id] = struct{}{}
						}
					}
				}
				want := referenceRecomputeLocked(routes, reference, only)
				got := routes.recomputeLocked(only)
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("step %d: recomputeLocked indications =\n%s\nwant\n%s",
						step, describeMTPIndications(got), describeMTPIndications(want))
				}
				if derived := flatASPDerived(routes); !reflect.DeepEqual(derived, reference) {
					t.Fatalf("step %d: derived destinations =\n%#v\nwant\n%#v", step, derived, reference)
				}
				for id, destinations := range routes.derived {
					for key := range destinations {
						if key.mtpRoute != id {
							t.Fatalf("step %d: MTP Route %q holds a destination of %q", step, id, key.mtpRoute)
						}
					}
				}
			}
		})
	}
}

func describeMTPIndications(indications []*MTPIndication) string {
	description := ""
	for _, indication := range indications {
		description += fmt.Sprintf("  %+v\n", *indication)
	}
	return description
}

// BenchmarkASPRouteAssociationEndRecompute measures the routing-lock hold of
// the GitHub issue #125 trial: 1,000 MTP Routes over two Signalling Gateways of
// two SGPs with two Associations each, every route carried by every SGP. Each
// op is one sg-a/p0 Association leaving ASP-ACTIVE and returning to it, two
// recomputes of all 1,000 routes, with nothing else contending for the lock.
func BenchmarkASPRouteAssociationEndRecompute(b *testing.B) {
	const routeCount = 1000
	routes, associations := newASPRouteFailoverFixture(b, routeCount, routeCount)
	association := associations[aspRouteFailoverSGP][0]
	b.ResetTimer()
	for range b.N {
		changeASPRouteAssociationState(b, routes, association, StateASPDown)
		changeASPRouteAssociationState(b, routes, association, StateASPActive)
	}
}
