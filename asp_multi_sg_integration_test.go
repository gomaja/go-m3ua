// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

func TestASPMultiSGTransferWhenASPInitiatesSCTPAssociations(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	aspEndpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: integrationASPConfig()})
	if err != nil {
		t.Fatalf("NewEndpoint ASP: %v", err)
	}
	t.Cleanup(func() { _ = aspEndpoint.Close() })

	sgpAssociations := make(map[SignallingGatewayID]*Association)
	for _, peer := range integrationPeers() {
		sgpEndpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
		if err != nil {
			t.Fatalf("NewEndpoint SGP %s: %v", peer.gateway, err)
		}
		t.Cleanup(func() { _ = sgpEndpoint.Close() })
		listener, err := sgpEndpoint.Listen("m3ua", mcAddr(0, peer.ip),
			NewListenerConfig(integrationAssociationConfig(RoleSGP, peer)))
		if err != nil {
			if isSCTPUnsupported(err) {
				t.Skipf("skipping socket-backed test: %v", err)
			}
			t.Fatalf("Listen SGP %s: %v", peer.gateway, err)
		}
		t.Cleanup(func() { _ = listener.Close() })
		accepted := make(chan associationResult, 1)
		go func() {
			association, acceptErr := listener.Accept(ctx)
			accepted <- associationResult{association: association, err: acceptErr}
		}()

		_, err = aspEndpoint.Dial(
			ctx, "m3ua", mcAddr(0, "127.0.0.1"), listener.Addr().(*sctp.Addr),
			integrationAssociationConfig(RoleASP, peer),
		)
		if err != nil {
			t.Fatalf("Dial ASP to %s: %v", peer.gateway, err)
		}
		select {
		case result := <-accepted:
			if result.err != nil {
				t.Fatalf("Accept at %s: %v", peer.gateway, result.err)
			}
			sgpAssociations[peer.gateway] = result.association
		case <-ctx.Done():
			t.Fatalf("Accept at %s: %v", peer.gateway, ctx.Err())
		}
	}

	exerciseASPMultiSGTransfer(t, aspEndpoint, sgpAssociations)
}

func TestASPMultiSGTransferWhenSGPsInitiateSCTPAssociations(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	aspEndpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: integrationASPConfig()})
	if err != nil {
		t.Fatalf("NewEndpoint ASP: %v", err)
	}
	t.Cleanup(func() { _ = aspEndpoint.Close() })

	peers := integrationPeers()
	listenerConfig := NewListenerConfig(integrationAssociationConfig(RoleASP, peers[0]))
	listenerConfig.SelectAssociationConfig = func(info AcceptInfo) (*AssociationConfig, error) {
		for _, peer := range peers {
			if acceptInfoHasRemoteIP(info, peer.ip) {
				return integrationAssociationConfig(RoleASP, peer), nil
			}
		}
		return nil, fmt.Errorf("unprovisioned SGP address %v", info.RemoteAddr)
	}
	listener, err := aspEndpoint.Listen("m3ua", mcAddr(0, "127.0.0.1"), listenerConfig)
	if err != nil {
		if isSCTPUnsupported(err) {
			t.Skipf("skipping socket-backed test: %v", err)
		}
		t.Fatalf("Listen ASP: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	accepted := make(chan associationResult, len(peers))
	for range peers {
		go func() {
			association, acceptErr := listener.Accept(ctx)
			accepted <- associationResult{association: association, err: acceptErr}
		}()
	}
	sgpAssociations := make(map[SignallingGatewayID]*Association)
	for _, peer := range peers {
		sgpEndpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
		if err != nil {
			t.Fatalf("NewEndpoint SGP %s: %v", peer.gateway, err)
		}
		t.Cleanup(func() { _ = sgpEndpoint.Close() })
		association, err := sgpEndpoint.Dial(
			ctx, "m3ua", mcAddr(0, peer.ip), listener.Addr().(*sctp.Addr),
			integrationAssociationConfig(RoleSGP, peer),
		)
		if err != nil {
			t.Fatalf("Dial SGP %s: %v", peer.gateway, err)
		}
		sgpAssociations[peer.gateway] = association
	}

	for range peers {
		select {
		case result := <-accepted:
			if result.err != nil {
				t.Fatalf("Accept at ASP: %v", result.err)
			}
		case <-ctx.Done():
			t.Fatalf("Accept at ASP: %v", ctx.Err())
		}
	}
	exerciseASPMultiSGTransfer(t, aspEndpoint, sgpAssociations)
}

func TestASPMultiSGConcurrentTransferAndRouteChanges(t *testing.T) {
	endpoint, first, second := newASPMultiSGFixture(t)
	first.dataWriter = func(data []byte, _ *sctp.SndInfo) (int, error) { return len(data), nil }
	second.dataWriter = func(data []byte, _ *sctp.SndInfo) (int, error) { return len(data), nil }

	var workers sync.WaitGroup
	workers.Add(5)
	errorsSeen := make(chan error, 500)
	for worker := 0; worker < 3; worker++ {
		go func(offset uint8) {
			defer workers.Done()
			for index := 0; index < 200; index++ {
				_, err := endpoint.MTPTransfer(MTPTransferRequest{
					ProtocolData: transferProtocolData(0x123456, uint8(index)+offset, nil),
				})
				if err != nil && !expectedConcurrentMTPTransferError(err) {
					errorsSeen <- err
				}
			}
		}(uint8(worker))
	}
	go func() {
		defer workers.Done()
		for index := 0; index < 100; index++ {
			state := DestinationUnavailable
			if index%2 == 1 {
				state = DestinationAvailable
			}
			for _, update := range []struct {
				association       *Association
				networkAppearance uint32
				routingContext    uint32
			}{{first, 7, 1}, {second, 9, 42}} {
				message := messages.NewDestinationUnavailable(
					params.NewNetworkAppearance(update.networkAppearance),
					params.NewRoutingContext(update.routingContext),
					params.NewAffectedPointCode(0x123456), nil,
				)
				var err error
				if state == DestinationAvailable {
					err = update.association.handleDestinationAvailable(messages.NewDestinationAvailable(
						message.NetworkAppearance, message.RoutingContext, message.AffectedPointCode, nil,
					))
				} else {
					err = update.association.handleDestinationUnavailable(message)
				}
				if err != nil {
					errorsSeen <- err
				}
			}
		}
	}()
	go func() {
		defer workers.Done()
		for index := 0; index < 100; index++ {
			first.noteRoutingContextsUnacked(params.NewRoutingContext(1))
			first.noteRoutingContextsAcked(params.NewRoutingContext(1))
		}
	}()
	workers.Wait()
	close(errorsSeen)
	for err := range errorsSeen {
		t.Errorf("concurrent ASP routing error: %v", err)
	}
}

type associationResult struct {
	association *Association
	err         error
}

type integrationPeer struct {
	gateway           SignallingGatewayID
	sgp               SignallingGatewayProcessID
	ip                string
	networkAppearance uint32
	routingContext    uint32
}

func integrationPeers() []integrationPeer {
	return []integrationPeer{
		{gateway: "sg-a", sgp: "sgp-a1", ip: "127.0.0.2", networkAppearance: 7, routingContext: 1},
		{gateway: "sg-b", sgp: "sgp-b1", ip: "127.0.0.3", networkAppearance: 9, routingContext: 42},
	}
}

func integrationASPConfig() *ASPConfig {
	return validASPConfig()
}

func integrationAssociationConfig(role Role, peer integrationPeer) *AssociationConfig {
	config := NewAssociationConfig()
	config.HeartbeatInfo = &HeartbeatInfo{Enabled: false}
	setInventoryTrafficModeType(&config.ApplicationServers, params.NewTrafficModeType(params.TrafficModeLoadshare))
	setInventoryNetworkAppearance(&config.ApplicationServers, params.NewNetworkAppearance(peer.networkAppearance))
	setInventoryRoutingContexts(&config.ApplicationServers, params.NewRoutingContext(peer.routingContext))
	config.EstablishTimeout = 10 * time.Second
	config.TAck = 100 * time.Millisecond
	config.TAckRetries = 10
	if role == RoleASP {
		config.ASPIdentifier = params.NewAspIdentifier(uint32(peer.routingContext))
		config.PeerSGP = &SGPIdentity{
			SignallingGateway:        peer.gateway,
			SignallingGatewayProcess: peer.sgp,
		}
	}
	return config
}

func exerciseASPMultiSGTransfer(
	t *testing.T,
	aspEndpoint *Endpoint,
	sgpAssociations map[SignallingGatewayID]*Association,
) {
	t.Helper()
	const pointCode = uint32(0x123456)
	if err := reportAvailability(
		sgpAssociations["sg-a"].endpoint, testWireScope(7, true, 1), pointCode, 0, DestinationUnavailable,
	); err != nil {
		t.Fatalf("report DUNA from sg-a: %v", err)
	}
	if !waitFor(func() bool {
		return integrationAvailabilityReady(
			aspEndpoint, "sg-a", pointCode, DestinationUnavailable,
		)
	}, 5*time.Second) {
		t.Fatal("ASP did not apply sg-a DUNA")
	}
	if err := reportAvailability(
		sgpAssociations["sg-b"].endpoint, testWireScope(9, true, 42), pointCode, 0, DestinationRestricted,
	); err != nil {
		t.Fatalf("report DRST from sg-b: %v", err)
	}
	if !waitFor(func() bool {
		return integrationAvailabilityReady(
			aspEndpoint, "sg-b", pointCode, DestinationRestricted,
		)
	}, 5*time.Second) {
		t.Fatal("ASP did not apply sg-b DRST")
	}

	request := MTPTransferRequest{ProtocolData: transferProtocolData(pointCode, 5, []byte("through-sg-b"))}
	result, err := aspEndpoint.MTPTransfer(request)
	if err != nil {
		t.Fatalf("MTPTransfer through sg-b: %v", err)
	}
	requireIntegrationGateway(t, result, "sg-b")
	requireIntegrationData(t, sgpAssociations["sg-b"], request.ProtocolData)

	if err := reportAvailability(
		sgpAssociations["sg-a"].endpoint, testWireScope(7, true, 1), pointCode, 0, DestinationAvailable,
	); err != nil {
		t.Fatalf("report DAVA from sg-a: %v", err)
	}
	if !waitFor(func() bool {
		return integrationAvailabilityReady(
			aspEndpoint, "sg-a", pointCode, DestinationAvailable,
		)
	}, 5*time.Second) {
		t.Fatal("ASP did not apply sg-a DAVA")
	}
	request = MTPTransferRequest{ProtocolData: transferProtocolData(pointCode, 5, []byte("through-sg-a"))}
	result, err = aspEndpoint.MTPTransfer(request)
	if err != nil {
		t.Fatalf("same-flow MTPTransfer through recovered sg-a: %v", err)
	}
	requireIntegrationGateway(t, result, "sg-a")
	requireIntegrationData(t, sgpAssociations["sg-a"], request.ProtocolData)
}

func requireIntegrationData(t *testing.T, association *Association, wanted *params.ProtocolDataPayload) {
	t.Helper()
	if err := association.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	data, err := association.ReadData(context.Background())
	if err != nil {
		t.Fatalf("ReadData: %v", err)
	}
	if data.ProtocolData.OriginatingPointCode != wanted.OriginatingPointCode ||
		data.ProtocolData.DestinationPointCode != wanted.DestinationPointCode ||
		data.ProtocolData.ServiceIndicator != wanted.ServiceIndicator ||
		data.ProtocolData.SignallingLinkSelection != wanted.SignallingLinkSelection ||
		string(data.ProtocolData.Data) != string(wanted.Data) {
		t.Fatalf("received Protocol Data = %#v, want %#v", data.ProtocolData, wanted)
	}
	if got, want := association.receivedStreamID(), association.streamFor(wanted.SignallingLinkSelection); got != want {
		t.Fatalf("received DATA stream = %d, want SLS-derived %d", got, want)
	}
}

func expectedConcurrentMTPTransferError(err error) bool {
	// The fixture starts with nothing reported about the destination, and the
	// route-change worker moves it in and out of unavailability, so a request
	// may legitimately find the state unknown, unavailable, or the scope
	// momentarily inactive.
	if err == nil || errors.Is(err, ErrNoMTPRoute) || errors.Is(err, ErrEndpointClosed) ||
		errors.Is(err, ErrDestinationStateUnknown) ||
		errors.Is(err, ErrRoutingContextNotActive) || errors.Is(err, ErrNotEstablished) {
		return true
	}
	var transferErr *MTPTransferError
	if !errors.As(err, &transferErr) || len(transferErr.Failures) == 0 {
		return false
	}
	for _, failure := range transferErr.Failures {
		if !errors.Is(failure.Err, ErrRoutingContextNotActive) &&
			!errors.Is(failure.Err, ErrNotEstablished) {
			return false
		}
	}
	return true
}

// integrationAvailabilityReady waits on the per-SG knowledge used by routing
// (RFC 4666 Section 4.5.2.2), which is published after the private cache update.
func integrationAvailabilityReady(
	endpoint *Endpoint,
	gateway SignallingGatewayID,
	pointCode uint32,
	want DestinationAvailability,
) bool {
	partition := canonicalSSNMPartition(gateway, "as-core")
	for _, knowledge := range endpoint.SSNMKnowledge().Partitions {
		if knowledge.Partition != partition {
			continue
		}
		for _, destination := range knowledge.Destinations {
			if destination.Destination == (PointCodeRange{PointCode: pointCode}) {
				return destination.AvailabilitySet && destination.Availability.State == want
			}
		}
	}
	return false
}

func requireIntegrationGateway(t *testing.T, result MTPTransferResult, want SignallingGatewayID) {
	t.Helper()
	if len(result.SuccessfulPaths) != 1 || result.SuccessfulPaths[0].SGP.SignallingGateway != want {
		t.Fatalf("MTPTransfer successful paths = %+v, want exactly one through %s", result.SuccessfulPaths, want)
	}
}
