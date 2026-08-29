// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

/*
Command m3ua-asp runs an M3UA ASP that initiates the SCTP association.
*/
package main

import (
	"context"
	"encoding/hex"
	"flag"
	"log"
	"math"
	"time"

	"github.com/gomaja/go-m3ua"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

func main() {
	var (
		addr    = flag.String("addr", "127.0.0.1:2905", "Remote SGP SCTP address.")
		data    = flag.String("data", "deadbeef", "MTP3-User payload in hexadecimal.")
		hbInt   = flag.Duration("hb-interval", 0, "M3UA T(beat) interval; zero disables M3UA BEAT.")
		network = flag.Uint64("network-appearance", 0, "Peer Network Appearance.")
		rtCtx   = flag.Uint64("routing-context", 1, "Peer Routing Context.")
		gateway = flag.String("signalling-gateway", "sg-a", "Peer Signalling Gateway identity.")
		process = flag.String("signalling-gateway-process", "sgp-a1", "Peer SGP identity.")
	)
	flag.Parse()
	if *network > math.MaxUint32 {
		log.Fatalf("Network Appearance %d exceeds 32 bits", *network)
	}
	if *rtCtx > math.MaxUint32 {
		log.Fatalf("Routing Context %d exceeds 32 bits", *rtCtx)
	}
	networkAppearance := uint32(*network)
	routingContext := uint32(*rtCtx)

	payload, err := hex.DecodeString(*data)
	if err != nil {
		log.Fatalf("Failed to decode hexadecimal payload: %s", err)
	}

	peer := m3ua.SGPIdentity{
		SignallingGateway:        m3ua.SignallingGatewayID(*gateway),
		SignallingGatewayProcess: m3ua.SignallingGatewayProcessID(*process),
	}
	asKey := m3ua.ASKey{
		NetworkAppearance:    networkAppearance,
		NetworkAppearanceSet: true,
		RoutingContext:       routingContext,
		RoutingContextSet:    true,
	}

	associationConfig := m3ua.NewAssociationConfig(
		0x111111,
		0x222222,
		params.ServiceIndSCCP,
		0,
		0,
		1,
	)
	associationConfig.
		EnableHeartbeat(*hbInt, 10*time.Second).
		SetASPIdentifier(1).
		SetTrafficModeType(params.TrafficModeLoadshare).
		SetNetworkAppearance(networkAppearance).
		SetRoutingContexts(routingContext)
	associationConfig.PeerSGP = &peer

	endpoint, err := m3ua.NewEndpoint(m3ua.EndpointConfig{
		Role: m3ua.RoleASP,
		ASP: &m3ua.ASPConfig{
			SignallingGatewaySelection: m3ua.RouteSelectionPrimaryBackup,
			MTPRoutes: []m3ua.MTPRouteConfig{
				{
					ID:                    "sccp",
					DestinationPointCode:  0x222222,
					ServiceIndicators:     []uint8{params.ServiceIndSCCP},
					OriginatingPointCodes: []uint32{0x111111},
				},
			},
			SignallingGateways: []m3ua.SignallingGatewayConfig{
				{
					ID:           peer.SignallingGateway,
					SGPSelection: m3ua.RouteSelectionPrimaryBackup,
					SGPs: []m3ua.SignallingGatewayProcessConfig{
						{
							ID: peer.SignallingGatewayProcess,
							Routes: []m3ua.SGPRoute{
								{MTPRoute: "sccp", AS: asKey},
							},
						},
					},
				},
			},
		},
	})
	if err != nil {
		log.Fatalf("Failed to create M3UA ASP Endpoint: %s", err)
	}
	defer func() { _ = endpoint.Close() }()

	remote, err := sctp.ResolveSCTPAddr("sctp", *addr)
	if err != nil {
		log.Fatalf("Failed to resolve SGP SCTP address: %s", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	association, err := endpoint.Dial(ctx, "m3ua", nil, remote, associationConfig)
	if err != nil {
		log.Fatalf("Failed to establish M3UA Association: %s", err)
	}
	defer func() { _ = association.Close() }()

	go func() {
		for indication := range endpoint.MTPIndications() {
			if indication.ResyncRequired {
				log.Printf("MTP indication queue requires destination-state resynchronization")
				continue
			}
			log.Printf("%s: MTP Route %q, destination %#x/%d, availability %s",
				indication.Kind,
				indication.Destination.Destination.MTPRoute,
				indication.Destination.Destination.PointCode,
				indication.Destination.Destination.Mask,
				indication.Destination.Availability,
			)
		}
	}()

	for {
		result, err := endpoint.MTPTransfer(m3ua.MTPTransferRequest{
			MTPRoute: "sccp",
			ProtocolData: params.NewProtocolDataPayload(
				0x111111, 0x222222, params.ServiceIndSCCP, 0, 0, 1, payload,
			),
		})
		if err != nil {
			log.Fatalf("MTP-TRANSFER failed: %s", err)
		}
		log.Printf("MTP-TRANSFER sent %d user octets through %d Association(s)",
			result.UserDataOctets, result.TransmittedAssociations)
		time.Sleep(3 * time.Second)
	}
}
