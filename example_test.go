// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/gomaja/go-m3ua"
	"github.com/gomaja/go-m3ua/messages/params"
)

// The examples below are the eight paths an application actually has to get
// right: starting an endpoint, sending directly, classifying what arrives,
// consuming SSNM, recovering from a subscription that fell behind, owning
// route selection yourself, reacting to a route that cannot carry a message,
// and shutting down. They run — the ones that would otherwise need a peer
// exercise the decision the application makes rather than pretending an
// association exists — and the same paths are driven over live associations by
// example_exercise_test.go.

// exampleSignallingGateways is the peer inventory the examples share: one
// Signalling Gateway, one SGP, one Application Server, named on the wire by
// Network Appearance 7 and Routing Context 1.
func exampleSignallingGateways() []m3ua.SignallingGatewayConfig {
	asKey := m3ua.ASKey{
		NetworkAppearance: 7, NetworkAppearanceSet: true,
		RoutingContext: 1, RoutingContextSet: true,
	}
	return []m3ua.SignallingGatewayConfig{{
		ID: "sg-a",
		SGPs: []m3ua.SignallingGatewayProcessConfig{{
			ID: "sgp-a1",
			ApplicationServers: []m3ua.RemoteASConfig{{
				ID:    "as-core",
				ASKey: &asKey,
			}},
		}},
	}}
}

// exampleRouting is the optional outbound inventory: one path candidate through
// sg-a, and one MTP Route that references it.
func exampleRouting() *m3ua.ASPRoutingConfig {
	return &m3ua.ASPRoutingConfig{
		SignallingGatewaySelection: m3ua.RouteSelectionPrimaryBackup,
		SignallingGatewayProcessSelection: map[m3ua.SignallingGatewayID]m3ua.RouteSelectionMode{
			"sg-a": m3ua.RouteSelectionPrimaryBackup,
		},
		Paths: []m3ua.MTPRoutePath{{
			ID:                 "via-sg-a",
			SignallingGateway:  "sg-a",
			ApplicationServers: []m3ua.RemoteASID{"as-core"},
		}},
		MTPRoutes: []m3ua.MTPRouteConfig{{
			ID:                   "sccp",
			DestinationPointCode: 0x220000,
			Mask:                 16,
			ServiceIndicators:    []uint8{params.ServiceIndSCCP},
			Paths:                []m3ua.MTPRoutePathID{"via-sg-a"},
		}},
	}
}

// ExampleNewEndpoint starts an ASP endpoint: an explicit RFC 4666 role, the
// peers it will talk to, and the Application Server inventory one association
// carries.
//
// The role is the Endpoint's, not the transport's. The same endpoint may Dial
// or Listen; RFC 4666 Section 1.4.8 recommends that both ASPs and SGPs support
// either orientation, so nothing about M3UA follows from which end opened the
// SCTP association.
func ExampleNewEndpoint() {
	endpoint, err := m3ua.NewEndpoint(m3ua.EndpointConfig{
		Role: m3ua.RoleASP,
		ASP: &m3ua.ASPConfig{
			SignallingGateways: exampleSignallingGateways(),
			Routing:            exampleRouting(),
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	// One close covers everything this endpoint owns.
	defer func() { _ = endpoint.Close() }()

	// Each association names the SGP it reaches and the Application Servers it
	// carries. Both are immutable once the association exists.
	peer := m3ua.SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}
	config := m3ua.NewAssociationConfig().
		EnableHeartbeat(3*time.Second, 10*time.Second).
		SetASPIdentifier(1).
		SetApplicationServers(m3ua.ASConfig{
			ASKey: m3ua.ASKey{
				NetworkAppearance: 7, NetworkAppearanceSet: true,
				RoutingContext: 1, RoutingContextSet: true,
			},
			TrafficMode: params.TrafficModeLoadshare,
		})
	config.PeerSGP = &peer

	// remote, _ := sctp.ResolveSCTPAddr("sctp", "198.51.100.7:2905")
	// association, err := endpoint.Dial(ctx, "m3ua", nil, remote, config)
	//
	// ctx is the association's lifetime, not just its handshake: cancelling it
	// closes the association.

	fmt.Println("role:", endpoint.Role())
	fmt.Println("application servers:", len(config.ApplicationServers))
	fmt.Println("peer:", config.PeerSGP.SignallingGateway, config.PeerSGP.SignallingGatewayProcess)
	// Output:
	// role: ASP
	// application servers: 1
	// peer: sg-a sgp-a1
}

// ExampleAssociation_WriteData sends one DATA directly on an association, and
// decides what to do when the send fails.
//
// Everything the message carries is in the request, because RFC 4666 Section
// 3.3.1 makes all of it per-message. Nothing is completed from association-wide
// state, so concurrent senders cannot take each other's scope or routing label.
func ExampleAssociation_WriteData() {
	request := m3ua.DataRequest{
		AS: m3ua.ASKey{
			NetworkAppearance: 7, NetworkAppearanceSet: true,
			RoutingContext: 1, RoutingContextSet: true,
		},
		ProtocolData: params.ProtocolDataPayload{
			OriginatingPointCode:    0x111111,
			DestinationPointCode:    0x222222,
			ServiceIndicator:        params.ServiceIndSCCP,
			SignallingLinkSelection: 1,
			Data:                    []byte("payload"),
		},
		// Stream zero asks for the stream this message's own SLS maps to,
		// which is what keeps one SLS in sequence.
	}

	send := func(association *m3ua.Association) (int, error) {
		// Success means the local transport accepted the message. RFC 4666
		// defines no acknowledgement for DATA, so it is never a claim about
		// delivery.
		return association.WriteData(request)
	}
	_ = send

	// Every failure is a *DataWriteError, and Outcome is what the retry
	// decision is made from.
	for _, err := range []error{
		&m3ua.DataWriteError{Outcome: m3ua.DataNotSent, AS: request.AS, Err: m3ua.ErrNotEstablished},
		&m3ua.DataWriteError{Outcome: m3ua.DataSendIndeterminate, AS: request.AS, Err: m3ua.ErrPartialDataWrite},
	} {
		var writeErr *m3ua.DataWriteError
		if !errors.As(err, &writeErr) {
			continue
		}
		switch writeErr.Outcome {
		case m3ua.DataNotSent:
			fmt.Println("not sent: resending cannot duplicate SS7 traffic")
		case m3ua.DataSendIndeterminate:
			fmt.Println("indeterminate: a resend may duplicate; the application decides")
		}
		// The cause stays matchable.
		fmt.Println("  established:", !errors.Is(err, m3ua.ErrNotEstablished))
	}
	// Output:
	// not sent: resending cannot duplicate SS7 traffic
	//   established: false
	// indeterminate: a resend may duplicate; the application decides
	//   established: true
}

// ExampleAssociation_ReadData classifies an inbound DATA message.
//
// Three things are reported separately and the separation is the point. Scope
// is what the peer put on the wire, presence bits included, which is what an
// answer sent back in the same scope must use. AS is the Application Server it
// resolved to, which is what distributing work must use. Epoch is the SCTP
// association generation, so traffic from before a peer restart is
// distinguishable from traffic after one.
func ExampleAssociation_ReadData() {
	read := func(ctx context.Context, association *m3ua.Association) (*m3ua.DataMessage, error) {
		// Cancelling ctx ends this one read. The association stays open and
		// nothing queued is discarded.
		return association.ReadData(ctx)
	}
	_ = read

	core := m3ua.ASKey{
		NetworkAppearance: 7, NetworkAppearanceSet: true,
		RoutingContext: 1, RoutingContextSet: true,
	}
	messages := []*m3ua.DataMessage{
		{
			Scope: m3ua.WireScope{
				NetworkAppearance: 7, NetworkAppearanceSet: true,
				RoutingContexts: []uint32{1}, RoutingContextSet: true,
			},
			AS:     core,
			Stream: 3,
			Epoch:  1,
		},
		{
			// The peer omitted the Routing Context, so this is the single
			// contextless Application Server the association coordinates.
			Scope:  m3ua.WireScope{},
			AS:     m3ua.ASKey{},
			Stream: 2,
			Epoch:  2,
		},
	}

	for _, message := range messages {
		switch {
		case !message.Scope.RoutingContextSet:
			fmt.Printf("contextless AS, stream %d, epoch %d\n", message.Stream, message.Epoch)
		case message.AS == core:
			fmt.Printf("as-core, wire RC %v, stream %d, epoch %d\n",
				message.Scope.RoutingContexts, message.Stream, message.Epoch)
		default:
			fmt.Printf("unexpected AS %+v\n", message.AS)
		}
	}
	// Output:
	// as-core, wire RC [1], stream 3, epoch 1
	// contextless AS, stream 2, epoch 2
}

// ExampleEndpoint_SubscribeSSNM consumes the Endpoint's route-independent SSNM
// knowledge.
//
// The snapshot and the subscription are atomic with respect to each other: no
// report is both in the snapshot and in the stream, and none is in neither.
func ExampleEndpoint_SubscribeSSNM() {
	endpoint, err := m3ua.NewEndpoint(m3ua.EndpointConfig{
		Role: m3ua.RoleASP,
		ASP:  &m3ua.ASPConfig{SignallingGateways: exampleSignallingGateways()},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = endpoint.Close() }()

	snapshot, subscription, err := endpoint.SubscribeSSNM()
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = subscription.Close() }()

	// The snapshot is the application's starting view and is owned by it.
	fmt.Println("partitions at subscribe:", len(snapshot.Partitions))

	// Knowledge is owned by canonical identity — one Signalling Gateway and one
	// Application Server — not by the Routing Context that carried it, because
	// that value means something else at another Signalling Gateway.
	for _, partition := range snapshot.Partitions {
		fmt.Println(partition.Partition.SignallingGateway, partition.Partition.ApplicationServer)
	}

	// Next blocks until something changes. A caller that wants to stop waiting
	// cancels its context; the subscription stays usable.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := subscription.Next(ctx); errors.Is(err, context.Canceled) {
		fmt.Println("next: cancelled, subscription still open")
	}

	// Output:
	// partitions at subscribe: 0
	// next: cancelled, subscription still open
}

// ExampleSSNMSubscription_Resync recovers a subscription whose backlog the
// store refused.
//
// The store is bounded in every dimension a peer controls, and it refuses
// rather than evicting: a consumer that falls behind is told so with
// ContinuityLost instead of being handed a stream with a silent hole in it. The
// answer is to replace the view, not to patch it.
func ExampleSSNMSubscription_Resync() {
	endpoint, err := m3ua.NewEndpoint(m3ua.EndpointConfig{
		Role: m3ua.RoleASP,
		ASP:  &m3ua.ASPConfig{SignallingGateways: exampleSignallingGateways()},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = endpoint.Close() }()

	snapshot, subscription, err := endpoint.SubscribeSSNM()
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = subscription.Close() }()

	consume := func(ctx context.Context) error {
		for {
			event, err := subscription.Next(ctx)
			if err != nil {
				return err
			}
			if event.ContinuityLost {
				resynced, err := subscription.Resync()
				if err != nil {
					return err
				}
				replaceView(resynced)
				continue
			}
			applyDelta(event)
		}
	}
	_ = consume

	// Resync is available at any time, and returns the store's current view
	// with the revision every later event will be greater than.
	resynced, err := subscription.Resync()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("resync matches the subscribe revision:", resynced.Revision == snapshot.Revision)
	fmt.Println("records refused by a bound:", resynced.RecordsRefused)
	fmt.Println("reports refused by a bound:", resynced.ReportsRefused)
	// Output:
	// resync matches the subscribe revision: true
	// records refused by a bound: 0
	// reports refused by a bound: 0
}

func replaceView(m3ua.SSNMSnapshot) {}
func applyDelta(m3ua.SSNMEvent)     {}

// ExampleASPConfig_applicationManagedRouting owns outbound selection in the
// application.
//
// Leaving ASPConfig.Routing nil keeps the peer and Application Server
// inventory, the procedures and the authorization, and hands candidate
// selection to the application. Nothing has to be invented to get there: no
// dummy MTP Route, and no adopting Endpoint.MTPTransfer.
func ExampleASPConfig_applicationManagedRouting() {
	endpoint, err := m3ua.NewEndpoint(m3ua.EndpointConfig{
		Role: m3ua.RoleASP,
		ASP: &m3ua.ASPConfig{
			SignallingGateways: exampleSignallingGateways(),
			// Routing is left nil.
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = endpoint.Close() }()

	// Discovery is the SSNM store: what each Signalling Gateway has reported,
	// per canonical Application Server, with absence distinguishable from a
	// report.
	knowledge := endpoint.SSNMKnowledge()
	reachable := func(destination uint32) []m3ua.SGASKey {
		var candidates []m3ua.SGASKey
		for _, partition := range knowledge.Partitions {
			if !partition.TrafficAuthorized {
				continue
			}
			for _, known := range partition.Destinations {
				if known.Destination.PointCode != destination || !known.AvailabilitySet {
					continue
				}
				if known.Availability.State == m3ua.DestinationUnavailable {
					continue
				}
				candidates = append(candidates, m3ua.SGASKey{
					SignallingGateway: partition.Partition.SignallingGateway,
					ApplicationServer: partition.Partition.ApplicationServer,
				})
			}
		}
		return candidates
	}

	// The application then sends on the association it chose, with WriteData.
	// The library still checks state, structure, stream and Application Server
	// binding; it does not second-guess the destination decision.
	// MTPIndications exists but stays silent: it is derived from provisioned
	// MTP Routes, and this ASP has none.
	select {
	case indication := <-endpoint.MTPIndications():
		fmt.Println("unexpected indication:", indication)
	default:
		fmt.Println("no derived MTP indications for an application-managed ASP")
	}
	fmt.Println("candidates before any report:", len(reachable(0x222222)))
	// Output:
	// no derived MTP indications for an application-managed ASP
	// candidates before any report: 0
}

// ExampleEndpoint_MTPTransfer reacts to a route that cannot carry a message.
//
// A refusal names every candidate the route tried and why, in the route's own
// candidate order. That is what an alternate-path decision is made from: the
// reasons distinguish a peer that is not there from one that is there and has
// said the destination is unreachable, and from one that has said nothing at
// all.
func ExampleEndpoint_MTPTransfer() {
	endpoint, err := m3ua.NewEndpoint(m3ua.EndpointConfig{
		Role: m3ua.RoleASP,
		ASP: &m3ua.ASPConfig{
			SignallingGateways: exampleSignallingGateways(),
			Routing:            exampleRouting(),
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = endpoint.Close() }()

	// No association has been established, so no candidate is bound.
	_, err = endpoint.MTPTransfer(m3ua.MTPTransferRequest{
		MTPRoute: "sccp",
		ProtocolData: params.NewProtocolDataPayload(
			0x111111, 0x220001, params.ServiceIndSCCP, 0, 0, 1, []byte("payload"),
		),
	})

	var selection *m3ua.MTPSelectionError
	if errors.As(err, &selection) {
		fmt.Println("route:", selection.MTPRoute)
		for _, rejection := range selection.Rejections {
			fmt.Printf("  %s via %s of %s: %s\n",
				rejection.ApplicationServer, rejection.Path,
				rejection.SGP.SignallingGatewayProcess, rejection.Reason)
		}
	}

	// Unwrap separates "this node knows nothing about the destination" from
	// "this route exists and cannot be used", because only the first is worth
	// auditing or opting out of with AllowUnknownDestinations.
	fmt.Println("state unknown:", errors.Is(err, m3ua.ErrDestinationStateUnknown))
	fmt.Println("no eligible route:", errors.Is(err, m3ua.ErrNoMTPRoute))

	// A successful transfer names every target it reached, in selection order:
	//
	//	for _, path := range result.SuccessfulPaths {
	//		log.Printf("%d octets to %q of %q", result.UserDataOctets,
	//			path.ApplicationServer, path.SGP.SignallingGatewayProcess)
	//	}

	// Output:
	// route: sccp
	//   as-core via via-sg-a of sgp-a1: not-bound
	// state unknown: false
	// no eligible route: true
}

// ExampleEndpoint_Close shuts down.
//
// Three scopes own resources and each closes exactly its own: an Association
// closes only itself, a Listener closes the Associations it accepted, and an
// Endpoint closes everything it owns. All three release SCTP without sending
// ASP Inactive or ASP Down, which is RFC 4666 Section 4.9 option (b); option
// (a) is Association.ShutdownContext, and nothing calls it for you.
func ExampleEndpoint_Close() {
	endpoint, err := m3ua.NewEndpoint(m3ua.EndpointConfig{
		Role: m3ua.RoleASP,
		ASP: &m3ua.ASPConfig{
			SignallingGateways: exampleSignallingGateways(),
			Routing:            exampleRouting(),
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	withdraw := func(association *m3ua.Association) {
		// Option (a): tell the peer traffic is stopping and that this ASP is
		// going down, before the association disappears underneath it. Only the
		// procedures ASPProcedures marks automatic are performed.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := association.ShutdownContext(ctx); err != nil {
			log.Printf("graceful withdrawal did not complete: %s", err)
		}
	}
	_ = withdraw

	// Then one close for everything the Endpoint owns: its Listeners, its
	// dialled and accepted Associations, and the shared state none of them owns
	// individually.
	if err := endpoint.Close(); err != nil {
		log.Printf("close reported: %s", err)
	}

	// Close is idempotent and every caller gets the same result.
	fmt.Println("second close:", endpoint.Close())

	// Afterwards the operations that would start work report it plainly.
	_, _, err = endpoint.SubscribeSSNM()
	fmt.Println("subscribe after close:", errors.Is(err, m3ua.ErrEndpointClosed))

	// Status snapshots stay callable and report an Endpoint that owns nothing.
	fmt.Println("associations after close:", len(endpoint.AssociationStatuses()))
	// Output:
	// second close: <nil>
	// subscribe after close: true
	// associations after close: 0
}
