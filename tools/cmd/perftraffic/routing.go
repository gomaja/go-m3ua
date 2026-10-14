package main

import (
	"errors"
	"fmt"

	"github.com/gomaja/go-m3ua"
	"github.com/gomaja/go-m3ua/messages/params"
)

const routingRouteCount = 1000

type routingPeer struct {
	Identity           m3ua.SGPIdentity
	Associations       int
	ApplicationServers []m3ua.ASConfig
}

type routingTopology struct {
	ASP   *m3ua.ASPConfig
	Peers []routingPeer
}

func newRoutingTopology(preferred m3ua.RemoteASID) (routingTopology, error) {
	if preferred != "primary" && preferred != "secondary" {
		return routingTopology{}, errors.New("unknown preferred routing AS")
	}
	configuration := &m3ua.ASPConfig{Routing: &m3ua.ASPRoutingConfig{
		SignallingGatewaySelection:        m3ua.RouteSelectionLoadshare,
		SignallingGatewayProcessSelection: make(map[m3ua.SignallingGatewayID]m3ua.RouteSelectionMode),
	}}
	topology := routingTopology{ASP: configuration}
	for gatewayIndex, gatewayID := range []m3ua.SignallingGatewayID{"sg-a", "sg-b"} {
		gateway := m3ua.SignallingGatewayConfig{ID: gatewayID}
		for processIndex, processID := range []m3ua.SignallingGatewayProcessID{"p0", "p1"} {
			peerIndex := 2*gatewayIndex + processIndex
			process := m3ua.SignallingGatewayProcessConfig{ID: processID}
			peer := routingPeer{Identity: m3ua.SGPIdentity{SignallingGateway: gatewayID, SignallingGatewayProcess: processID}, Associations: 2}
			for scopeIndex, serverID := range []m3ua.RemoteASID{"primary", "secondary"} {
				key := m3ua.ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true,
					RoutingContext: uint32(100 + 10*peerIndex + scopeIndex), RoutingContextSet: true}
				process.ApplicationServers = append(process.ApplicationServers, m3ua.RemoteASConfig{ID: serverID, ASKey: &key})
				peer.ApplicationServers = append(peer.ApplicationServers, m3ua.ASConfig{ASKey: key, TrafficMode: params.TrafficModeLoadshare})
			}
			gateway.SGPs = append(gateway.SGPs, process)
			topology.Peers = append(topology.Peers, peer)
		}
		configuration.SignallingGateways = append(configuration.SignallingGateways, gateway)
		configuration.Routing.SignallingGatewayProcessSelection[gatewayID] = m3ua.RouteSelectionLoadshare
		servers := []m3ua.RemoteASID{"primary", "secondary"}
		if preferred == "secondary" {
			servers[0], servers[1] = servers[1], servers[0]
		}
		configuration.Routing.Paths = append(configuration.Routing.Paths, m3ua.MTPRoutePath{
			ID: m3ua.MTPRoutePathID(string(gatewayID) + "-path"), SignallingGateway: gatewayID, ApplicationServers: servers})
	}
	for route := 0; route < routingRouteCount; route++ {
		configuration.Routing.MTPRoutes = append(configuration.Routing.MTPRoutes, m3ua.MTPRouteConfig{
			ID: m3ua.MTPRouteID(fmt.Sprintf("route-%04d", route)), DestinationPointCode: 0x220000 + uint32(route),
			Paths: []m3ua.MTPRoutePathID{"sg-a-path", "sg-b-path"}})
	}
	return topology, nil
}

func routeProtocolData(route uint16, payload []byte) (params.ProtocolDataPayload, error) {
	if route >= routingRouteCount {
		return params.ProtocolDataPayload{}, errors.New("routing flow is out of range")
	}
	return params.ProtocolDataPayload{
		OriginatingPointCode: 0x110000 + uint32(route), DestinationPointCode: 0x220000 + uint32(route),
		ServiceIndicator: 3 + 2*uint8(route%2), NetworkIndicator: uint8(route % 4),
		MessagePriority: uint8((route / 4) % 4), SignallingLinkSelection: uint8(route % 16), Data: payload,
	}, nil
}
