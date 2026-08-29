// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

/*
Command m3ua-sgp runs an M3UA SGP that accepts SCTP associations.
*/
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"

	"github.com/gomaja/go-m3ua"
	"github.com/gomaja/go-sctp"
)

func serve(association *m3ua.Association) {
	defer func() { _ = association.Close() }()

	buf := make([]byte, m3ua.DefaultReadBufferSize)
	for {
		n, err := association.Read(buf)
		if err != nil {
			if errors.Is(err, m3ua.ErrNotEstablished) {
				log.Printf("Closed M3UA association with: %s", association.RemoteAddr())
				return
			}
			log.Printf("Error reading from M3UA association: %s", err)
			return
		}

		log.Printf("Read: %x", buf[:n])
	}
}

func acceptAssociations(ctx context.Context, listener *m3ua.Listener) error {
	for {
		association, err := listener.Accept(ctx)
		if err != nil {
			var establishmentError *m3ua.AssociationEstablishmentError
			if errors.As(err, &establishmentError) {
				log.Printf("Rejected M3UA association: %s", establishmentError)
				continue
			}
			return err
		}
		log.Printf("Associated with: %s", association.RemoteAddr())
		go serve(association)
	}
}

func main() {
	var (
		addr              = flag.String("addr", "127.0.0.1:2905", "Local SCTP address")
		hbInt             = flag.Duration("hb-interval", 0, "M3UA T(beat); zero disables M3UA BEAT")
		hbTimer           = flag.Duration("hb-timer", 5*time.Second, "M3UA BEAT acknowledgement deadline; ignored when hb-interval is zero")
		acceptConcurrency = flag.Int("accept-concurrency", 4, "Concurrent Listener.Accept workers")
	)
	flag.Parse()
	if *acceptConcurrency <= 0 {
		log.Fatal("accept-concurrency must be greater than zero")
	}

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

	endpoint, err := m3ua.NewEndpoint(m3ua.EndpointConfig{Role: m3ua.RoleSGP})
	if err != nil {
		log.Fatalf("Failed to create M3UA SGP endpoint: %s", err)
	}
	listener, err := endpoint.Listen("m3ua", laddr, m3ua.NewListenerConfig(config))
	if err != nil {
		log.Fatalf("Failed to listen: %s", err)
	}
	log.Printf("Waiting for an SCTP association on: %s", listener.Addr())

	ctx := context.Background()
	listenerFailures := make(chan error, *acceptConcurrency)
	for range *acceptConcurrency {
		go func() {
			listenerFailures <- acceptAssociations(ctx, listener)
		}()
	}
	err = <-listenerFailures
	if closeErr := listener.Close(); closeErr != nil {
		log.Printf("Failed to close M3UA Listener: %s", closeErr)
	}
	log.Fatalf("M3UA Listener failed: %s", err)
}
