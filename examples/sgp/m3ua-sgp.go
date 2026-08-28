// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

/*
Command m3ua-sgp runs an M3UA SGP that accepts SCTP associations.
*/
package main

import (
	"context"
	"flag"
	"io"
	"log"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"

	"github.com/gomaja/go-m3ua"
	"github.com/gomaja/go-sctp"
)

func serve(association *m3ua.Association) {
	defer func() { _ = association.Close() }()

	buf := make([]byte, 1500)
	for {
		n, err := association.Read(buf)
		if err != nil {
			// An EOF ends this association; the SGP continues accepting others.
			if err == io.EOF {
				log.Printf("Closed M3UA association with: %s", association.RemoteAddr())
				return
			}
			log.Printf("Error reading from M3UA association: %s", err)
			return
		}

		log.Printf("Read: %x", buf[:n])
	}
}

func main() {
	var (
		addr    = flag.String("addr", "127.0.0.1:2905", "Source IP and Port listen.")
		hbInt   = flag.Duration("hb-interval", 0, "Interval for M3UA BEAT. Put 0 to disable")
		hbTimer = flag.Duration("hb-timer", time.Duration(5*time.Second), "Expiration timer for M3UA BEAT. Ignored when hb-interval is 0")
	)
	flag.Parse()

	config := m3ua.NewAssociationConfig(
		0x22222222,            // OriginatingPointCode
		0x11111111,            // DestinationPointCode
		params.ServiceIndSCCP, // ServiceIndicator
		0,                     // NetworkIndicator
		0,                     // MessagePriority
		1,                     // SignallingLinkSelection
	).
		EnableHeartbeat(*hbInt, *hbTimer).
		SetTrafficModeType(params.TrafficModeLoadshare).
		SetNetworkAppearance(0).
		SetRoutingContexts(1, 2)

	// setup SCTP listener on the specified IPs and Port.
	laddr, err := sctp.ResolveSCTPAddr("sctp", *addr)
	if err != nil {
		log.Fatalf("Failed to resolve SCTP address: %s", err)
	}

	endpoint, err := m3ua.NewEndpoint(m3ua.RoleSGP)
	if err != nil {
		log.Fatalf("Failed to create M3UA SGP endpoint: %s", err)
	}
	listener, err := endpoint.Listen("m3ua", laddr, m3ua.NewListenerConfig(config))
	if err != nil {
		log.Fatalf("Failed to listen: %s", err)
	}
	log.Printf("Waiting for an SCTP association on: %s", listener.Addr())

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for {
		association, err := listener.Accept(ctx)
		if err != nil {
			log.Fatalf("Failed to accept M3UA association: %s", err)
		}
		log.Printf("Associated with: %s", association.RemoteAddr())

		go serve(association)
	}
}
