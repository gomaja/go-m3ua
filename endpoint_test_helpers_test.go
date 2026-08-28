// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"

	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

func dialASP(ctx context.Context, network string, local, remote *sctp.SCTPAddr, config *AssociationConfig) (*Association, error) {
	endpoint, err := NewEndpoint(RoleASP)
	if err != nil {
		return nil, err
	}
	return endpoint.Dial(ctx, network, local, remote, config)
}

func listenSGP(network string, local *sctp.SCTPAddr, config *ListenerConfig) (*Listener, error) {
	endpoint, err := NewEndpoint(RoleSGP)
	if err != nil {
		return nil, err
	}
	return endpoint.Listen(network, local, config)
}

func newSGPListener(config *ListenerConfig) *Listener {
	endpoint, err := NewEndpoint(RoleSGP)
	if err != nil {
		panic(err)
	}
	role, err := endpoint.associationRole()
	if err != nil {
		panic(err)
	}
	return newListener(endpoint, role, config)
}

func newASPAssociationConfigForTest(heartbeat *HeartbeatInfo, opc, dpc, aspID, trafficMode, networkAppearance, correlationID uint32, routingContexts []uint32, si, ni, priority, sls uint8) *AssociationConfig {
	return newAssociationConfigForTest(
		heartbeat, opc, dpc, aspID, trafficMode, networkAppearance,
		correlationID, routingContexts, si, ni, priority, sls,
	)
}

func newSGPAssociationConfigForTest(heartbeat *HeartbeatInfo, opc, dpc, aspID, trafficMode, networkAppearance, correlationID uint32, routingContexts []uint32, si, ni, priority, sls uint8) *AssociationConfig {
	config := newAssociationConfigForTest(
		heartbeat, opc, dpc, aspID, trafficMode, networkAppearance,
		correlationID, routingContexts, si, ni, priority, sls,
	)
	config.ASPIdentifier = nil
	return config
}

func newAssociationConfigForTest(heartbeat *HeartbeatInfo, opc, dpc, aspID, trafficMode, networkAppearance, correlationID uint32, routingContexts []uint32, si, ni, priority, sls uint8) *AssociationConfig {
	return &AssociationConfig{
		HeartbeatInfo:          heartbeat,
		SCTPConfig:             &SCTPConfig{},
		ASPIdentifier:          params.NewAspIdentifier(aspID),
		TrafficModeType:        params.NewTrafficModeType(trafficMode),
		NetworkAppearance:      params.NewNetworkAppearance(networkAppearance),
		RoutingContexts:        params.NewRoutingContext(routingContexts...),
		CorrelationID:          params.NewCorrelationID(correlationID),
		OriginatingPointCode:   opc,
		DestinationPointCode:   dpc,
		ServiceIndicator:       si,
		NetworkIndicator:       ni,
		MessagePriority:        priority,
		SignallingLinkSelection: sls,
	}
}
