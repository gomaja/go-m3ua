// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import "github.com/gomaja/go-m3ua/messages/params"

// The queries an Application Server inventory has to answer. Membership is
// asked on every outbound DATA, so the questions a send asks — does this
// inventory carry this Routing Context, and with which exact ASKey — are
// answered by scanning the declarations rather than by building anything.
//
// The scans index the inventory and read only the fields they need. Ranging
// over it by value would copy each whole declaration, which costs more per send
// and, in a test that holds a live AssociationConfig, makes an unrelated
// Traffic Mode write look like a conflict with a membership read.

// asConfigRoutingContexts returns the Routing Contexts the inventory names, in
// declaration order. The contextless Application Server names none.
func asConfigRoutingContexts(servers []ASConfig) []uint32 {
	var routingContexts []uint32
	for index := range servers {
		if servers[index].ASKey.RoutingContextSet {
			routingContexts = append(routingContexts, servers[index].ASKey.RoutingContext)
		}
	}
	return routingContexts
}

// asConfigCarriesRoutingContext reports membership without building a slice.
func asConfigCarriesRoutingContext(servers []ASConfig, rtCtx uint32) bool {
	for index := range servers {
		if servers[index].ASKey.RoutingContextSet && servers[index].ASKey.RoutingContext == rtCtx {
			return true
		}
	}
	return false
}

// asConfigASKeyFor returns the exact ASKey the inventory declares for one
// Routing Context. The Network Appearance is the declaring entry's own: two
// Application Servers on one Association may sit in different SS7 network
// contexts, which RFC 4666 Section 3.3.1 makes the Network Appearance's whole
// purpose.
func asConfigASKeyFor(servers []ASConfig, rtCtx uint32) (ASKey, bool) {
	for index := range servers {
		if servers[index].ASKey.RoutingContextSet && servers[index].ASKey.RoutingContext == rtCtx {
			return servers[index].ASKey, true
		}
	}
	return ASKey{}, false
}

// asConfigContextlessASKey returns the contextless Application Server of RFC
// 4666 Section 3.6.1, when the inventory has one. An empty inventory is that
// Application Server with nothing configured for it.
func asConfigContextlessASKey(servers []ASConfig) (ASKey, bool) {
	if len(servers) == 0 {
		return ASKey{}, true
	}
	for index := range servers {
		if !servers[index].ASKey.RoutingContextSet {
			return servers[index].ASKey, true
		}
	}
	return ASKey{}, false
}

// asConfigNetworkAppearance is the Network Appearance every declared
// Application Server shares, for the messages that carry one appearance for the
// whole Association rather than for a named Application Server. RFC 4666
// Section 3.4 gives an SSNM message a single Network Appearance parameter, and
// Section 3.8.1 the same for an Error, so an inventory whose entries disagree
// has none to offer and the optional parameter is omitted.
func asConfigNetworkAppearance(servers []ASConfig) (uint32, bool) {
	var value uint32
	var set, resolved bool
	for index := range servers {
		if !resolved {
			value = servers[index].ASKey.NetworkAppearance
			set = servers[index].ASKey.NetworkAppearanceSet
			resolved = true
			continue
		}
		if servers[index].ASKey.NetworkAppearanceSet != set ||
			set && servers[index].ASKey.NetworkAppearance != value {
			return 0, false
		}
	}
	return value, set
}

// asConfigASKeys returns the declared Application Servers, in order.
func asConfigASKeys(servers []ASConfig) []ASKey {
	if len(servers) == 0 {
		return nil
	}
	keys := make([]ASKey, 0, len(servers))
	for index := range servers {
		keys = append(keys, servers[index].ASKey)
	}
	return keys
}

// asConfigRoutingContextParam builds the Routing Context parameter naming the
// inventory, or nil when it names none.
func asConfigRoutingContextParam(servers []ASConfig) *params.Param {
	routingContexts := asConfigRoutingContexts(servers)
	if len(routingContexts) == 0 {
		return nil
	}
	return params.NewRoutingContext(routingContexts...)
}

// applicationServerInventory is the Application Server inventory governing one
// direction of this Association.
//
// Only an IPSP Double Exchange has two: RFC 4666 Section 5.6.2 keeps its
// directions independent, so each carries its own. Every other role has one
// inventory that both directions share.
func (c *Association) applicationServerInventory(local bool) []ASConfig {
	if c == nil || c.cfg == nil {
		return nil
	}
	if !c.isIPSPDoubleExchange() {
		return c.cfg.ApplicationServers
	}
	direction := c.cfg.IPSP.TrafficToPeer
	if local {
		direction = c.cfg.IPSP.TrafficToLocal
	}
	if direction == nil {
		return nil
	}
	return direction.ApplicationServers
}

// networkAppearanceParam builds the optional Network Appearance parameter from
// a resolved value, or nil when there is none to carry.
func networkAppearanceParam(appearance uint32, set bool) *params.Param {
	if !set {
		return nil
	}
	return params.NewNetworkAppearance(appearance)
}

// listenerNetworkAppearance is the Network Appearance the Listener's default
// Application Server inventory shares.
func listenerNetworkAppearance(l *Listener) (uint32, bool) {
	if l == nil || l.AssociationConfig == nil {
		return 0, false
	}
	return asConfigNetworkAppearance(l.AssociationConfig.ApplicationServers)
}
