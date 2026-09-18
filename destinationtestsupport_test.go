// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"testing"

	"github.com/gomaja/go-m3ua/messages/params"
)

// The SGP destination setters, reports and queries that used to sit on
// Association and Listener are gone: the Endpoint owns that state and publishes
// it. These helpers are what the tests below use in their place — the
// publication paths through the owning Endpoint, and direct reads of the
// retained record for the assertions that are about what was retained rather
// than about what a caller can see.

// testWireScope builds the exact Network Appearance and Routing Context scope a
// locally originated SSNM statement puts on the wire.
func testWireScope(networkAppearance uint32, networkAppearanceSet bool, routingContexts ...uint32) WireScope {
	scope := WireScope{
		NetworkAppearance:    networkAppearance,
		NetworkAppearanceSet: networkAppearanceSet,
	}
	if len(routingContexts) > 0 {
		scope.RoutingContexts = routingContexts
		scope.RoutingContextSet = true
	}
	return scope
}

// reportAvailability publishes an SGP availability statement through the
// Endpoint that owns the destination state.
func reportAvailability(
	endpoint *Endpoint,
	scope WireScope,
	pointCode uint32,
	mask uint8,
	availability DestinationAvailability,
) error {
	return endpoint.ReportDestinationAvailability(DestinationAvailabilityRequest{
		Scope:        scope,
		Destinations: []PointCodeRange{{PointCode: pointCode, Mask: mask}},
		Availability: availability,
	})
}

// reportCongestion publishes an SGP congestion statement through the Endpoint
// that owns the destination state.
func reportCongestion(
	endpoint *Endpoint,
	scope WireScope,
	pointCode uint32,
	mask uint8,
	level uint8,
	levelSet bool,
) error {
	return endpoint.SignallingCongestion(SignallingCongestionRequest{
		Scope:              scope,
		Destinations:       []PointCodeRange{{PointCode: pointCode, Mask: mask}},
		CongestionLevel:    level,
		CongestionLevelSet: levelSet,
	})
}

// listenerEndpoint is the Endpoint that owns a Listener's destination state.
func listenerEndpoint(t *testing.T, listener *Listener) *Endpoint {
	t.Helper()
	if listener == nil || listener.endpoint == nil {
		t.Fatal("listener has no owning Endpoint")
	}
	return listener.endpoint
}

// associationDestinationScope is the scope the removed per-Association queries
// resolved: this association's outbound Network Appearance, narrowed to its
// single configured Routing Context when it has exactly one.
func associationDestinationScope(c *Association, networkAppearance *params.Param) destinationKey {
	scope := c.destinationKey(networkAppearance, 0)
	configured := c.configuredRoutingContexts()
	if len(configured) == 1 {
		scope.routingContext = configured[0]
		scope.routingContextSet = true
	}
	return scope
}

// retainedDestinationState reads both dimensions of what an Association has
// retained about a destination.
func retainedDestinationState(c *Association, pointCode uint32) DestinationNetworkState {
	scope := associationDestinationScope(c, nil)
	state, _ := c.destinations.lookupRange(scope, pointCode, 0)
	return state
}

// retainedAvailability is retainedDestinationState's availability dimension.
func retainedAvailability(c *Association, pointCode uint32) DestinationAvailability {
	return retainedDestinationState(c, pointCode).Availability
}

// retainedAvailabilityForNetwork reads the availability retained for a
// destination in an explicit Network Appearance.
func retainedAvailabilityForNetwork(c *Association, networkAppearance, pointCode uint32) DestinationAvailability {
	scope := associationDestinationScope(c, params.NewNetworkAppearance(networkAppearance))
	state, _ := c.destinations.lookupRange(scope, pointCode, 0)
	return state.Availability
}

// retainedStateForNetworkAndRoutingContext reads both dimensions retained for a
// destination in an exact Network Appearance and Routing Context scope.
func retainedStateForNetworkAndRoutingContext(
	c *Association,
	networkAppearance, routingContext, pointCode uint32,
) DestinationNetworkState {
	state, _ := c.destinations.lookupRange(destinationKey{
		networkAppearance:    networkAppearance,
		networkAppearanceSet: true,
		routingContext:       routingContext,
		routingContextSet:    true,
	}, pointCode, 0)
	return state
}

// retainedAvailabilityForNetworkAndRoutingContext is that scope's availability.
func retainedAvailabilityForNetworkAndRoutingContext(
	c *Association,
	networkAppearance, routingContext, pointCode uint32,
) DestinationAvailability {
	return retainedStateForNetworkAndRoutingContext(c, networkAppearance, routingContext, pointCode).Availability
}

// seedDestinationRange records a destination range directly in an Association's
// retained state, which is what the removed local setters did.
func seedDestinationRange(c *Association, rangeValue DestinationRange) {
	c.destinations.setRanges([]DestinationRange{rangeValue})
}

// seedDestinationAvailability seeds one point code's availability in the scope
// the removed per-Association setters resolved.
func seedDestinationAvailability(c *Association, pointCode uint32, availability DestinationAvailability) {
	scope := associationDestinationScope(c, nil)
	seedDestinationRange(c, DestinationRange{
		NetworkAppearance:    scope.networkAppearance,
		NetworkAppearanceSet: scope.networkAppearanceSet,
		RoutingContext:       scope.routingContext,
		RoutingContextSet:    scope.routingContextSet,
		PointCode:            pointCode,
		State:                DestinationNetworkState{Availability: availability},
	})
}

// availabilityState is the network state an availability-only statement carries.
func availabilityState(availability DestinationAvailability) DestinationNetworkState {
	return DestinationNetworkState{Availability: availability}
}

// congestedState is the network state a congestion statement carries at an
// explicit level.
func congestedState(level uint8) DestinationNetworkState {
	return DestinationNetworkState{Congestion: congestionStateFor(level, true)}
}
