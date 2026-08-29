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

	aspAssociations := make(map[SignallingGatewayID]*Association)
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

		aspAssociation, err := aspEndpoint.Dial(
			ctx, "m3ua", mcAddr(0, "127.0.0.1"), listener.Addr().(*sctp.SCTPAddr),
			integrationAssociationConfig(RoleASP, peer),
		)
		if err != nil {
			t.Fatalf("Dial ASP to %s: %v", peer.gateway, err)
		}
		aspAssociations[peer.gateway] = aspAssociation
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

	exerciseASPMultiSGTransfer(t, aspEndpoint, aspAssociations, sgpAssociations)
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
			ctx, "m3ua", mcAddr(0, peer.ip), listener.Addr().(*sctp.SCTPAddr),
			integrationAssociationConfig(RoleSGP, peer),
		)
		if err != nil {
			t.Fatalf("Dial SGP %s: %v", peer.gateway, err)
		}
		sgpAssociations[peer.gateway] = association
	}

	aspAssociations := make(map[SignallingGatewayID]*Association)
	for range peers {
		select {
		case result := <-accepted:
			if result.err != nil {
				t.Fatalf("Accept at ASP: %v", result.err)
			}
			aspAssociations[result.association.cfg.PeerSGP.SignallingGateway] = result.association
		case <-ctx.Done():
			t.Fatalf("Accept at ASP: %v", ctx.Err())
		}
	}
	exerciseASPMultiSGTransfer(t, aspEndpoint, aspAssociations, sgpAssociations)
}

func TestASPMultiSGConcurrentTransferAndRouteChanges(t *testing.T) {
	endpoint, first, second := newASPMultiSGFixture(t)
	first.dataWriter = func(data []byte, _ *sctp.SndRcvInfo) (int, error) { return len(data), nil }
	second.dataWriter = func(data []byte, _ *sctp.SndRcvInfo) (int, error) { return len(data), nil }

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
	config := NewAssociationConfig(0x111111, 0x123456, params.ServiceIndSCCP, 0, 0, 1)
	config.HeartbeatInfo = &HeartbeatInfo{Enabled: false}
	config.TrafficModeType = params.NewTrafficModeType(params.TrafficModeLoadshare)
	config.NetworkAppearance = params.NewNetworkAppearance(peer.networkAppearance)
	config.RoutingContexts = params.NewRoutingContext(peer.routingContext)
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
	aspAssociations map[SignallingGatewayID]*Association,
	sgpAssociations map[SignallingGatewayID]*Association,
) {
	t.Helper()
	const pointCode = uint32(0x123456)
	if err := sgpAssociations["sg-a"].ReportDestinationStateForNetworkAndRoutingContext(
		7, 1, pointCode, DestinationUnavailable,
	); err != nil {
		t.Fatalf("report DUNA from sg-a: %v", err)
	}
	if !waitFor(func() bool {
		return aspAssociations["sg-a"].DestinationStateForNetworkAndRoutingContext(7, 1, pointCode) == DestinationUnavailable
	}, 5*time.Second) {
		t.Fatal("ASP did not apply sg-a DUNA")
	}
	if err := sgpAssociations["sg-b"].ReportDestinationStateForNetworkAndRoutingContext(
		9, 42, pointCode, DestinationRestricted,
	); err != nil {
		t.Fatalf("report DRST from sg-b: %v", err)
	}
	if !waitFor(func() bool {
		return aspAssociations["sg-b"].DestinationStateForNetworkAndRoutingContext(9, 42, pointCode) == DestinationRestricted
	}, 5*time.Second) {
		t.Fatal("ASP did not apply sg-b DRST")
	}

	request := MTPTransferRequest{ProtocolData: transferProtocolData(pointCode, 5, []byte("through-sg-b"))}
	if _, err := aspEndpoint.MTPTransfer(request); err != nil {
		t.Fatalf("MTPTransfer through sg-b: %v", err)
	}
	requireIntegrationData(t, sgpAssociations["sg-b"], request.ProtocolData)

	if err := sgpAssociations["sg-a"].ReportDestinationStateForNetworkAndRoutingContext(
		7, 1, pointCode, DestinationAvailable,
	); err != nil {
		t.Fatalf("report DAVA from sg-a: %v", err)
	}
	if !waitFor(func() bool {
		return aspAssociations["sg-a"].DestinationStateForNetworkAndRoutingContext(7, 1, pointCode) == DestinationAvailable
	}, 5*time.Second) {
		t.Fatal("ASP did not apply sg-a DAVA")
	}
	request = MTPTransferRequest{ProtocolData: transferProtocolData(pointCode, 5, []byte("through-sg-a"))}
	if _, err := aspEndpoint.MTPTransfer(request); err != nil {
		t.Fatalf("same-flow MTPTransfer through recovered sg-a: %v", err)
	}
	requireIntegrationData(t, sgpAssociations["sg-a"], request.ProtocolData)
}

func requireIntegrationData(t *testing.T, association *Association, wanted *params.ProtocolDataPayload) {
	t.Helper()
	if err := association.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	data, err := association.ReadData()
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
	if err == nil || errors.Is(err, ErrNoMTPRoute) || errors.Is(err, ErrEndpointClosed) ||
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
