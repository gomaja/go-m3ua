package main

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/gomaja/go-m3ua"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestRoutingTopologyHasApprovedInventory(testContext *testing.T) {
	for _, preferred := range []m3ua.RemoteASID{"primary", "secondary"} {
		testContext.Run(string(preferred), func(testContext *testing.T) {
			topology, err := newRoutingTopology(preferred)
			if err != nil {
				testContext.Fatal(err)
			}
			if len(topology.Peers) != 4 || len(topology.ASP.SignallingGateways) != 2 || topology.ASP.Routing == nil {
				testContext.Fatalf("wrong topology: %+v", topology)
			}
			routing := topology.ASP.Routing
			if routing.AllowUnknownDestinations || routing.SignallingGatewaySelection != m3ua.RouteSelectionLoadshare || len(routing.MTPRoutes) != 1000 || len(routing.Paths) != 2 {
				testContext.Fatalf("wrong routing inventory: %+v", routing)
			}
			for gatewayIndex, gateway := range topology.ASP.SignallingGateways {
				if len(gateway.SGPs) != 2 || routing.SignallingGatewayProcessSelection[gateway.ID] != m3ua.RouteSelectionLoadshare {
					testContext.Fatalf("wrong SG: %+v", gateway)
				}
				path := routing.Paths[gatewayIndex]
				if path.SignallingGateway != gateway.ID || len(path.ApplicationServers) != 2 || path.ApplicationServers[0] != preferred || path.ApplicationServers[0] == path.ApplicationServers[1] {
					testContext.Fatalf("wrong AS preference: %+v", path)
				}
				for processIndex, process := range gateway.SGPs {
					peerIndex := 2*gatewayIndex + processIndex
					peer := topology.Peers[peerIndex]
					if peer.Identity != (m3ua.SGPIdentity{SignallingGateway: gateway.ID, SignallingGatewayProcess: process.ID}) || peer.Associations != 2 || len(peer.ApplicationServers) != 2 || len(process.ApplicationServers) != 2 {
						testContext.Fatalf("wrong peer inventory: %+v", peer)
					}
					for scopeIndex, server := range peer.ApplicationServers {
						wanted := m3ua.ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContext: uint32(100 + 10*peerIndex + scopeIndex), RoutingContextSet: true}
						if server.ASKey != wanted || server.TrafficMode != params.TrafficModeLoadshare || process.ApplicationServers[scopeIndex].ASKey == nil || *process.ApplicationServers[scopeIndex].ASKey != wanted || process.ApplicationServers[scopeIndex].RoutingKey != nil {
							testContext.Fatalf("peer %d scope %d differs: %+v", peerIndex, scopeIndex, server)
						}
					}
				}
			}
			for routeIndex, route := range routing.MTPRoutes {
				if route.ID != m3ua.MTPRouteID(fmt.Sprintf("route-%04d", routeIndex)) || route.DestinationPointCode != 0x220000+uint32(routeIndex) || route.Mask != 0 || !reflect.DeepEqual(route.Paths, []m3ua.MTPRoutePathID{routing.Paths[0].ID, routing.Paths[1].ID}) {
					testContext.Fatalf("route %d differs: %+v", routeIndex, route)
				}
			}
			endpoint, err := m3ua.NewEndpoint(m3ua.EndpointConfig{Role: m3ua.RoleASP, ASP: topology.ASP})
			if err != nil {
				testContext.Fatalf("public Endpoint rejects topology: %v", err)
			}
			testContext.Cleanup(func() { _ = endpoint.Close() })
			if len(endpoint.MTPRouteStatuses()) != 1000 {
				testContext.Fatal("public Endpoint did not retain all routes")
			}
		})
	}
	if _, err := newRoutingTopology("unknown"); err == nil {
		testContext.Fatal("unknown preferred AS accepted")
	}
}

func TestRoutingTopologyOwnsEveryMutableConfiguration(testContext *testing.T) {
	first, err := newRoutingTopology("primary")
	if err != nil {
		testContext.Fatal(err)
	}
	second, err := newRoutingTopology("primary")
	if err != nil {
		testContext.Fatal(err)
	}
	first.ASP.Routing.MTPRoutes[0].Paths[0] = "changed"
	first.ASP.Routing.Paths[0].ApplicationServers[0] = "changed"
	first.ASP.SignallingGateways[0].SGPs[0].ApplicationServers[0].ASKey.RoutingContext = 999
	first.Peers[0].ApplicationServers[0].ASKey.RoutingContext = 888
	first.ASP.Routing.SignallingGatewayProcessSelection["sg-a"] = m3ua.RouteSelectionBroadcast
	if second.ASP.Routing.MTPRoutes[0].Paths[0] == "changed" || first.ASP.Routing.MTPRoutes[1].Paths[0] == "changed" ||
		second.ASP.Routing.Paths[0].ApplicationServers[0] != "primary" || first.ASP.Routing.Paths[1].ApplicationServers[0] != "primary" ||
		second.Peers[0].ApplicationServers[0].ASKey.RoutingContext != 100 || first.ASP.SignallingGateways[0].SGPs[1].ApplicationServers[0].ASKey.RoutingContext != 110 ||
		second.ASP.Routing.SignallingGatewayProcessSelection["sg-a"] != m3ua.RouteSelectionLoadshare {
		testContext.Fatal("mutable configuration aliases another route, peer, or topology")
	}
}

func TestRoutingTuplesCoverDistinctDestinationsWithStableLabels(testContext *testing.T) {
	for route := uint16(0); route < 1000; route++ {
		payload := []byte{1, 2, 3}
		data, err := routeProtocolData(route, payload)
		if err != nil {
			testContext.Fatal(err)
		}
		if data.DestinationPointCode != 0x220000+uint32(route) || data.OriginatingPointCode != 0x110000+uint32(route) ||
			data.ServiceIndicator != 3+2*uint8(route%2) || data.NetworkIndicator != uint8(route%4) ||
			data.MessagePriority != uint8((route/4)%4) || data.SignallingLinkSelection != uint8(route%16) || !reflect.DeepEqual(data.Data, payload) {
			testContext.Fatalf("route %d tuple differs: %+v", route, data)
		}
	}
	if _, err := routeProtocolData(1000, nil); err == nil {
		testContext.Fatal("out-of-range route accepted")
	}
}
