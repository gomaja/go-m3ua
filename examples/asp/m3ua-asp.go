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
	"errors"
	"flag"
	"log"
	"math"
	"os"
	"os/signal"
	"syscall"
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

	associationConfig := m3ua.NewAssociationConfig()
	associationConfig.
		EnableHeartbeat(*hbInt, 10*time.Second).
		SetASPIdentifier(1).
		SetApplicationServers(m3ua.ASConfig{
			ASKey:       asKey,
			TrafficMode: params.TrafficModeLoadshare,
		})
	associationConfig.PeerSGP = &peer

	endpoint, err := m3ua.NewEndpoint(m3ua.EndpointConfig{
		Role: m3ua.RoleASP,
		ASP: &m3ua.ASPConfig{
			SignallingGateways: []m3ua.SignallingGatewayConfig{
				{
					ID: peer.SignallingGateway,
					SGPs: []m3ua.SignallingGatewayProcessConfig{
						{
							ID: peer.SignallingGatewayProcess,
							ApplicationServers: []m3ua.RemoteASConfig{
								{ID: "as-core", ASKey: &asKey},
							},
						},
					},
				},
			},
			Routing: &m3ua.ASPRoutingConfig{
				SignallingGatewaySelection: m3ua.RouteSelectionPrimaryBackup,
				SignallingGatewayProcessSelection: map[m3ua.SignallingGatewayID]m3ua.RouteSelectionMode{
					peer.SignallingGateway: m3ua.RouteSelectionPrimaryBackup,
				},
				Paths: []m3ua.MTPRoutePath{
					{
						ID:                 "via-sg-1",
						SignallingGateway:  peer.SignallingGateway,
						ApplicationServers: []m3ua.RemoteASID{"as-core"},
					},
				},
				MTPRoutes: []m3ua.MTPRouteConfig{
					{
						ID:                    "sccp",
						DestinationPointCode:  0x222222,
						ServiceIndicators:     []uint8{params.ServiceIndSCCP},
						OriginatingPointCodes: []uint32{0x111111},
						Paths:                 []m3ua.MTPRoutePathID{"via-sg-1"},
					},
				},
				// The Signalling Gateway has reported nothing about the
				// destination when the first request is made, and this example
				// has no discovery of its own, so it opts in to sending before
				// a report arrives rather than failing closed.
				AllowUnknownDestinations: true,
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

	// ctx is the association's lifetime, not just its handshake: cancelling it
	// closes the association. An interrupt therefore ends the traffic loop and
	// the association together.
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	association, err := endpoint.Dial(ctx, "m3ua", nil, remote, associationConfig)
	if err != nil {
		log.Fatalf("Failed to establish M3UA Association: %s", err)
	}
	// The Association is not closed here. endpoint.Close closes every
	// Association the Endpoint owns, and shutdown below withdraws this one
	// through the RFC 4666 Section 4.9 option (a) procedures first.

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

	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

traffic:
	for {
		result, err := endpoint.MTPTransfer(m3ua.MTPTransferRequest{
			MTPRoute: "sccp",
			ProtocolData: params.NewProtocolDataPayload(
				0x111111, 0x222222, params.ServiceIndSCCP, 0, 0, 1, payload,
			),
		})
		if err == nil {
			for _, path := range result.SuccessfulPaths {
				log.Printf("MTP-TRANSFER sent %d user octets to Application Server %q of SGP %q",
					result.UserDataOctets, path.ApplicationServer, path.SGP.SignallingGatewayProcess)
			}
		} else {
			// No candidate could carry the request. The selection error names
			// every candidate the route tried and why each was refused, which
			// is what an alternate-path decision is made from.
			var selection *m3ua.MTPSelectionError
			if errors.As(err, &selection) {
				for _, rejection := range selection.Rejections {
					log.Printf("MTP Route %q refused %q via path %q: %s",
						selection.MTPRoute, rejection.ApplicationServer,
						rejection.Path, rejection.Reason)
				}
			} else {
				log.Printf("MTP-TRANSFER failed: %s", err)
			}
		}

		select {
		case <-ticker.C:
		case <-ctx.Done():
			break traffic
		}
	}

	// RFC 4666 Section 4.9 option (a): tell the SGP traffic is stopping and
	// that this ASP is going down before the association disappears. Close
	// alone is option (b), which endpoint.Close then performs on whatever is
	// left.
	shutdown, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	if err := association.ShutdownContext(shutdown); err != nil {
		log.Printf("Graceful withdrawal did not complete: %s", err)
	}
}
