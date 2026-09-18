// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import "github.com/gomaja/go-m3ua/messages/params"

// Test support for describing an Application Server inventory one part at a
// time. Production code takes the whole inventory at once; a test that only
// wants to vary the Routing Contexts, or only the Network Appearance, says so
// here and keeps the rest of the inventory it already described.

// buildTestInventory declares one Application Server per Routing Context, or
// the contextless one when there is no Routing Context and something to say
// about it.
func buildTestInventory(appearance uint32, appearanceSet bool, trafficMode uint32, routingContexts []uint32) []ASConfig {
	if len(routingContexts) == 0 {
		if !appearanceSet && trafficMode == 0 {
			return nil
		}
		return []ASConfig{{
			ASKey:       ASKey{NetworkAppearance: appearance, NetworkAppearanceSet: appearanceSet},
			TrafficMode: trafficMode,
		}}
	}
	servers := make([]ASConfig, 0, len(routingContexts))
	for _, routingContext := range routingContexts {
		servers = append(servers, ASConfig{
			ASKey: ASKey{
				NetworkAppearance:    appearance,
				NetworkAppearanceSet: appearanceSet,
				RoutingContext:       routingContext,
				RoutingContextSet:    true,
			},
			TrafficMode: trafficMode,
		})
	}
	return servers
}

// inventoryUniformTrafficMode is the Traffic Mode every declared Application
// Server shares, and zero when they differ.
func inventoryUniformTrafficMode(servers []ASConfig) uint32 {
	var mode uint32
	for index, server := range servers {
		if index == 0 {
			mode = server.TrafficMode
			continue
		}
		if server.TrafficMode != mode {
			return 0
		}
	}
	return mode
}

func setInventoryRoutingContexts(servers *[]ASConfig, routingContext *params.Param) {
	appearance, appearanceSet := asConfigNetworkAppearance(*servers)
	var routingContexts []uint32
	if routingContext != nil {
		routingContexts = routingContext.RoutingContexts()
	}
	*servers = buildTestInventory(appearance, appearanceSet, inventoryUniformTrafficMode(*servers), routingContexts)
}

func setInventoryNetworkAppearance(servers *[]ASConfig, networkAppearance *params.Param) {
	appearance, appearanceSet := appearanceOf(networkAppearance)
	*servers = buildTestInventory(
		appearance, appearanceSet, inventoryUniformTrafficMode(*servers), asConfigRoutingContexts(*servers))
}

func setInventoryTrafficModeType(servers *[]ASConfig, trafficModeType *params.Param) {
	var mode uint32
	if trafficModeType != nil && len(trafficModeType.Data) == 4 {
		mode = trafficModeType.TrafficModeType()
	}
	appearance, appearanceSet := asConfigNetworkAppearance(*servers)
	*servers = buildTestInventory(appearance, appearanceSet, mode, asConfigRoutingContexts(*servers))
}

// setInventoryTrafficModes gives declared Application Servers their own modes.
// A mode for a Routing Context the inventory does not declare names no
// Application Server and is dropped, which is the whole point of folding the
// old per-Routing-Context map into the declarations.
func setInventoryTrafficModes(servers *[]ASConfig, modes map[uint32]uint32) {
	inventory := *servers
	for index := range inventory {
		if !inventory[index].ASKey.RoutingContextSet {
			continue
		}
		if mode, configured := modes[inventory[index].ASKey.RoutingContext]; configured {
			inventory[index].TrafficMode = mode
		}
	}
	*servers = inventory
}

// inventoryTrafficModeParam is the Traffic Mode Type every declared
// Application Server shares, as the optional parameter a message carries.
func inventoryTrafficModeParam(servers []ASConfig) *params.Param {
	mode := inventoryUniformTrafficMode(servers)
	if mode == 0 {
		return nil
	}
	return params.NewTrafficModeType(mode)
}

// inventoryNetworkAppearanceParam is the shared Network Appearance as the
// optional parameter a message carries.
func inventoryNetworkAppearanceParam(servers []ASConfig) *params.Param {
	return networkAppearanceParam(asConfigNetworkAppearance(servers))
}

// inventoryUniformNetworkAppearance is the shared Network Appearance value.
func inventoryUniformNetworkAppearance(servers []ASConfig) uint32 {
	appearance, _ := asConfigNetworkAppearance(servers)
	return appearance
}
