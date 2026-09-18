// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

func dialASP(ctx context.Context, network string, local, remote *sctp.SCTPAddr, config *AssociationConfig) (*Association, error) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP})
	if err != nil {
		return nil, err
	}
	return endpoint.Dial(ctx, network, local, remote, config)
}

func listenSGP(network string, local *sctp.SCTPAddr, config *ListenerConfig) (*Listener, error) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		return nil, err
	}
	return endpoint.Listen(network, local, config)
}

func newSGPListener(config *ListenerConfig) *Listener {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		panic(err)
	}
	return newListener(endpoint, config)
}

func associationConfigASKey(config *AssociationConfig, routingContext uint32) ASKey {
	key := routingContextASKey(routingContext)
	if config != nil {
		key.NetworkAppearance, key.NetworkAppearanceSet = appearanceOf(config.NetworkAppearance)
	}
	return key
}

func newASPAssociationConfigForTest(heartbeat *HeartbeatInfo, aspID, trafficMode, networkAppearance uint32, routingContexts []uint32) *AssociationConfig {
	return newAssociationConfigForTest(heartbeat, aspID, trafficMode, networkAppearance, routingContexts)
}

func newSGPAssociationConfigForTest(heartbeat *HeartbeatInfo, aspID, trafficMode, networkAppearance uint32, routingContexts []uint32) *AssociationConfig {
	config := newAssociationConfigForTest(heartbeat, aspID, trafficMode, networkAppearance, routingContexts)
	config.ASPIdentifier = nil
	return config
}

func newAssociationConfigForTest(heartbeat *HeartbeatInfo, aspID, trafficMode, networkAppearance uint32, routingContexts []uint32) *AssociationConfig {
	return &AssociationConfig{
		HeartbeatInfo:     heartbeat,
		SCTPConfig:        &SCTPConfig{},
		ASPIdentifier:     params.NewAspIdentifier(aspID),
		TrafficModeType:   params.NewTrafficModeType(trafficMode),
		NetworkAppearance: params.NewNetworkAppearance(networkAppearance),
		RoutingContexts:   params.NewRoutingContext(routingContexts...),
	}
}

// wireRoutingContext is the single Routing Context a received DATA named. RFC
// 4666 Section 3.3.1 defines exactly one for DATA, and delivery is refused for
// any other count, so a scope that carried none was sent without the parameter.
func wireRoutingContext(scope WireScope) uint32 {
	if len(scope.RoutingContexts) == 0 {
		return 0
	}
	return scope.RoutingContexts[0]
}

// associationScope is one of an Association's own configured Application
// Servers: the Network Appearance it was built with, and the named Routing
// Context unless the association coordinates none at all.
//
// Tests whose subject is the scope itself name it explicitly instead.
func associationScope(c *Association, routingContext uint32) ASKey {
	var key ASKey
	key.NetworkAppearance, key.NetworkAppearanceSet = appearanceOf(c.applicationServerNetworkAppearance())
	if len(c.configuredRoutingContexts()) > 0 {
		key.RoutingContext, key.RoutingContextSet = routingContext, true
	}
	return key
}

// testProtocolData is a recognizable MTP3 routing label for tests that care
// about the payload rather than the label.
func testProtocolData(payload []byte) params.ProtocolDataPayload {
	return params.ProtocolDataPayload{
		OriginatingPointCode:    0x11111111,
		DestinationPointCode:    0x22222222,
		ServiceIndicator:        params.ServiceIndSCCP,
		SignallingLinkSelection: 1,
		Data:                    payload,
	}
}

// writePayload sends one DATA for the association's own Application Server.
func writePayload(c *Association, routingContext uint32, payload []byte) (int, error) {
	return c.WriteData(DataRequest{
		AS:           associationScope(c, routingContext),
		ProtocolData: testProtocolData(payload),
	})
}

// writePayloadToStream is writePayload with the SCTP stream chosen explicitly.
func writePayloadToStream(c *Association, routingContext uint32, stream uint16, payload []byte) (int, error) {
	return c.WriteData(DataRequest{
		AS:           associationScope(c, routingContext),
		ProtocolData: testProtocolData(payload),
		Stream:       stream,
	})
}

// readPayload reads one DATA within the given budget and returns its user
// octets.
func readPayload(c *Association, within time.Duration) (*DataMessage, error) {
	ctx, cancel := context.WithTimeout(context.Background(), within)
	defer cancel()
	message, err := c.ReadData(ctx)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, fmt.Errorf("nothing arrived within %v", within)
		}
		return nil, err
	}
	return message, nil
}
