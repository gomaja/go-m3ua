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
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"

	"github.com/gomaja/go-m3ua"
	"github.com/gomaja/go-sctp"
)

func serve(ctx context.Context, association *m3ua.Association) {
	defer func() { _ = association.Close() }()

	for {
		message, err := association.ReadData(ctx)
		if err != nil {
			if errors.Is(err, m3ua.ErrNotEstablished) {
				log.Printf("Closed M3UA association with: %s", association.RemoteAddr())
				return
			}
			if ctx.Err() != nil {
				return
			}
			log.Printf("Error reading from M3UA association: %s", err)
			return
		}

		log.Printf("Read %d octets for AS %+v on stream %d: %x",
			len(message.ProtocolData.Data), message.AS, message.Stream, message.ProtocolData.Data)
	}
}

// acceptAssociations serves one accept worker until the Listener stops.
//
// Two of Accept's errors mean opposite things. An AssociationEstablishmentError
// concerns one peer and leaves the Listener serving, so the loop continues;
// anything else — the listening socket failing, the Listener closing, ctx
// ending — is permanent for this worker and returns. Returning on both would
// retire a worker for every peer that failed to establish, until the SGP
// silently stopped accepting.
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
		go serve(ctx, association)
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

	applicationServers := make([]m3ua.ASConfig, 0, 2)
	for _, routingContext := range []uint32{1, 2} {
		applicationServers = append(applicationServers, m3ua.ASConfig{
			ASKey: m3ua.ASKey{
				NetworkAppearance:    0,
				NetworkAppearanceSet: true,
				RoutingContext:       routingContext,
				RoutingContextSet:    true,
			},
			TrafficMode: params.TrafficModeLoadshare,
		})
	}
	config := m3ua.NewAssociationConfig().
		EnableHeartbeat(*hbInt, *hbTimer).
		SetApplicationServers(applicationServers...)

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

	// ctx is each accepted association's lifetime, not just its handshake, so
	// cancelling it here closes every association this SGP is serving. That is
	// what an interrupt should do; stopping only the accepting would be
	// listener.Close.
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	listenerFailures := make(chan error, *acceptConcurrency)
	for range *acceptConcurrency {
		go func() {
			listenerFailures <- acceptAssociations(ctx, listener)
		}()
	}

	select {
	case err = <-listenerFailures:
		log.Printf("M3UA accept worker stopped: %s", err)
	case <-ctx.Done():
		log.Print("Shutting down")
	}

	// Closing the Endpoint closes the Listener, every Association it accepted,
	// and the shared SGP state none of them owns individually. Closing only the
	// Listener would leave the Application Server registry, the destination
	// state and the MTP3 restart coordinator behind.
	if closeErr := endpoint.Close(); closeErr != nil {
		log.Printf("Failed to close M3UA Endpoint: %s", closeErr)
	}
}
