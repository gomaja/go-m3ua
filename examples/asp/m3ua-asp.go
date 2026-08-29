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
	"time"

	"github.com/gomaja/go-m3ua/messages/params"

	"github.com/gomaja/go-m3ua"
	"github.com/gomaja/go-sctp"
)

func main() {
	var (
		addr  = flag.String("addr", "127.0.0.1:2905", "Remote IP and Port to connect to.")
		data  = flag.String("data", "deadbeef", "Payload to send on M3UA in hex stream format.")
		hbInt = flag.Duration("hb-interval", 0, "Interval for M3UA BEAT. Put 0 to disable")
	)
	flag.Parse()

	// setup data to send
	d, err := hex.DecodeString(*data)
	if err != nil {
		log.Fatalf("Failed to decode Hex string: %s", err)
	}

	// Configure the M3UA association.
	config := m3ua.NewAssociationConfig(
		0x11111111,            // OriginatingPointCode
		0x22222222,            // DestinationPointCode
		params.ServiceIndSCCP, // ServiceIndicator
		0,                     // NetworkIndicator
		0,                     // MessagePriority
		1,                     // SignallingLinkSelection
	)
	config.
		EnableHeartbeat(*hbInt, 10*time.Second).
		SetASPIdentifier(1).
		SetTrafficModeType(params.TrafficModeLoadshare).
		SetNetworkAppearance(0).
		SetRoutingContexts(1, 2)

	// setup SCTP peer on the specified IPs and Port.
	raddr, err := sctp.ResolveSCTPAddr("sctp", *addr)
	if err != nil {
		log.Fatalf("Failed to resolve SCTP address: %s", err)
	}

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	endpoint, err := m3ua.NewEndpoint(m3ua.EndpointConfig{Role: m3ua.RoleASP})
	if err != nil {
		log.Fatalf("Failed to create M3UA ASP endpoint: %s", err)
	}
	association, err := endpoint.Dial(ctx, "m3ua", nil, raddr, config)
	if err != nil {
		log.Fatalf("Failed to establish M3UA association: %s", err)
	}
	defer func() { _ = association.Close() }()

	// send data once in 3 seconds.
	for {
		if _, err := association.Write(d); err != nil {
			log.Fatalf("Failed to write M3UA data: %s", err)
		}
		log.Printf("Sent: %x", d)

		time.Sleep(3 * time.Second)
	}
}
