// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

func TestNewEndpointRejectsInvalidRole(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: Role(0xff)})
	if endpoint != nil {
		t.Fatalf("NewEndpoint returned endpoint %v for an invalid role", endpoint)
	}
	if !errors.Is(err, ErrUnsupportedRole) {
		t.Fatalf("NewEndpoint error = %v, want ErrUnsupportedRole", err)
	}
}

func TestNewEndpointSnapshotsSGPPolicy(t *testing.T) {
	policy := &SGPConfig{
		RecoveryTimer:                3 * time.Second,
		RecoveryQueueMessages:        7,
		RecoveryQueueBytes:           11,
		RecoveryQueueTotalMessages:   13,
		RecoveryQueueTotalBytes:      17,
		BroadcastFlowCacheEntries:    19,
		BroadcastFlowIdentifierBytes: 23,
	}
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP, SGP: policy})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	policy.RecoveryTimer = time.Hour
	policy.RecoveryQueueMessages = 100
	policy.RecoveryQueueBytes = 100
	policy.RecoveryQueueTotalMessages = 100
	policy.RecoveryQueueTotalBytes = 100
	policy.BroadcastFlowCacheEntries = 100
	policy.BroadcastFlowIdentifierBytes = 100

	registry := endpoint.as
	if registry.recoveryTimer != 3*time.Second {
		t.Fatalf("RecoveryTimer = %v, want 3s", registry.recoveryTimer)
	}
	if registry.distribution.messageLimit != 7 || registry.distribution.byteLimit != 11 {
		t.Fatalf("per-AS recovery limits = (%d, %d), want (7, 11)",
			registry.distribution.messageLimit, registry.distribution.byteLimit)
	}
	if registry.recoveryBudget.messageLimit != 13 || registry.recoveryBudget.byteLimit != 17 {
		t.Fatalf("Endpoint recovery limits = (%d, %d), want (13, 17)",
			registry.recoveryBudget.messageLimit, registry.recoveryBudget.byteLimit)
	}
	if registry.distribution.broadcastFlowCacheEntries != 19 ||
		registry.distribution.broadcastFlowIdentifierBytes != 23 {
		t.Fatalf("Broadcast limits = (%d, %d), want (19, 23)",
			registry.distribution.broadcastFlowCacheEntries,
			registry.distribution.broadcastFlowIdentifierBytes)
	}
}

func TestNewEndpointRejectsSGPPolicyForASPRole(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, SGP: &SGPConfig{}})
	if endpoint != nil {
		t.Fatalf("NewEndpoint returned Endpoint %v for ASP with SGP policy", endpoint)
	}
	if !errors.Is(err, ErrInvalidRoleConfiguration) {
		t.Fatalf("NewEndpoint error = %v, want ErrInvalidRoleConfiguration", err)
	}
}

func TestASPListenerDoesNotRunSGPAvailabilityProcedures(t *testing.T) {
	tests := []struct {
		name string
		call func(*Listener) error
	}{
		{
			name: "NIF isolation",
			call: func(listener *Listener) error {
				return listener.SetNIFAvailable(false)
			},
		},
		{
			name: "Application Server isolation",
			call: func(listener *Listener) error {
				return listener.SetASAvailableForAS(ASKey{RoutingContext: 1, RoutingContextSet: true}, false)
			},
		},
		{
			name: "legacy Routing Context Application Server isolation",
			call: func(listener *Listener) error {
				return listener.SetASAvailable(1, false)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP})
			if err != nil {
				t.Fatalf("NewEndpoint(RoleASP): %v", err)
			}
			listener := newListener(endpoint, NewListenerConfig(NewAssociationConfig(0, 0, 0, 0, 0, 0)))
			association, sent := newTestConn(t, StateASPActive, RoleASP)
			association.cfg.RoutingContexts = params.NewRoutingContext(1)
			association.noteRoutingContextsActive([]uint32{1})
			if !listener.track(association) {
				t.Fatal("failed to track test Association")
			}

			if err := test.call(listener); !errors.Is(err, ErrUnsupportedRole) {
				t.Fatalf("availability control error = %v, want ErrUnsupportedRole", err)
			}

			if got := association.State(); got != StateASPActive {
				t.Fatalf("ASP Association state = %v, want ASP-ACTIVE", got)
			}
			if got := len(*sent); got != 0 {
				t.Fatalf("ASP Listener emitted %d SGP control messages, want 0", got)
			}
			if listener.as != nil || listener.nif != nil {
				t.Fatal("ASP Listener initialized SGP shared state")
			}
		})
	}
}

func TestSGPEndpointTracksMultipleListeners(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatalf("NewEndpoint(RoleSGP): %v", err)
	}
	first := newListener(endpoint, NewListenerConfig(mcSGPConfig()))
	second := newListener(endpoint, NewListenerConfig(mcSGPConfig()))

	if !endpoint.trackListener(first) {
		t.Fatal("failed to attach first Listener")
	}
	if !endpoint.trackListener(second) {
		t.Fatal("failed to attach second Listener")
	}
	endpoint.mu.Lock()
	tracked := len(endpoint.listeners)
	endpoint.mu.Unlock()
	if tracked != 2 {
		t.Fatalf("tracked Listeners = %d, want 2", tracked)
	}
}

func TestSGPEndpointListenersShareNodeState(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatalf("NewEndpoint(RoleSGP): %v", err)
	}

	first := newListener(endpoint, NewListenerConfig(mcSGPConfig()))
	second := newListener(endpoint, NewListenerConfig(mcSGPConfig()))
	firstApplicationServers, firstNIF, firstDestinations := first.registry()
	secondApplicationServers, secondNIF, secondDestinations := second.registry()

	if firstApplicationServers != secondApplicationServers {
		t.Fatal("listeners on one SGP Endpoint received different Application Server registries")
	}
	if firstNIF != secondNIF {
		t.Fatal("listeners on one SGP Endpoint received different NIF state")
	}
	if firstDestinations != secondDestinations {
		t.Fatal("listeners on one SGP Endpoint received different destination state")
	}
}

func TestSGPEndpointDistributionUsesExactNetworkAppearance(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatalf("NewEndpoint(RoleSGP): %v", err)
	}
	firstListener := newListener(endpoint, NewListenerConfig(scopedSGPConfig(10, 1)))
	secondListener := newListener(endpoint, NewListenerConfig(scopedSGPConfig(20, 1)))
	first, firstSent := attachActiveEndpointAssociation(t, endpoint, firstListener, ASKey{
		NetworkAppearance: 10, NetworkAppearanceSet: true,
		RoutingContext: 1, RoutingContextSet: true,
	})
	second, secondSent := attachActiveEndpointAssociation(t, endpoint, secondListener, ASKey{
		NetworkAppearance: 20, NetworkAppearanceSet: true,
		RoutingContext: 1, RoutingContextSet: true,
	})
	defer func() {
		_ = first.Close()
		_ = second.Close()
		_ = endpoint.Close()
	}()
	*firstSent = nil
	*secondSent = nil

	data := messages.NewData(
		params.NewNetworkAppearance(20),
		params.NewRoutingContext(1),
		params.NewProtocolData(1, 2, params.ServiceIndSCCP, 0, 0, 1, []byte("network 20")),
		nil,
	)
	result, err := firstListener.DistributeData(data)
	if err != nil {
		t.Fatalf("DistributeData: %v", err)
	}
	if result.Delivered != 1 || result.Queued {
		t.Fatalf("distribution result = %#v, want one delivery", result)
	}
	if len(*firstSent) != 0 || len(*secondSent) != 1 {
		t.Fatalf("deliveries by Network Appearance = (%d, %d), want (0, 1)", len(*firstSent), len(*secondSent))
	}
}

func TestSGPEndpointDistributionRejectsAmbiguousNetworkAppearance(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatalf("NewEndpoint(RoleSGP): %v", err)
	}
	firstListener := newListener(endpoint, NewListenerConfig(scopedSGPConfig(10, 1)))
	secondListener := newListener(endpoint, NewListenerConfig(scopedSGPConfig(20, 1)))
	first, firstSent := attachActiveEndpointAssociation(t, endpoint, firstListener, ASKey{
		NetworkAppearance: 10, NetworkAppearanceSet: true,
		RoutingContext: 1, RoutingContextSet: true,
	})
	second, secondSent := attachActiveEndpointAssociation(t, endpoint, secondListener, ASKey{
		NetworkAppearance: 20, NetworkAppearanceSet: true,
		RoutingContext: 1, RoutingContextSet: true,
	})
	defer func() {
		_ = first.Close()
		_ = second.Close()
		_ = endpoint.Close()
	}()
	*firstSent = nil
	*secondSent = nil

	data := messages.NewData(
		nil,
		params.NewRoutingContext(1),
		params.NewProtocolData(1, 2, params.ServiceIndSCCP, 0, 0, 1, []byte("ambiguous network")),
		nil,
	)
	if _, err := firstListener.DistributeData(data); !errors.Is(err, ErrInvalidNetworkAppearance) {
		t.Fatalf("DistributeData error = %v, want ErrInvalidNetworkAppearance", err)
	}
	if len(*firstSent) != 0 || len(*secondSent) != 0 {
		t.Fatalf("ambiguous DATA deliveries = (%d, %d), want none", len(*firstSent), len(*secondSent))
	}
}

func TestSGPEndpointRegistersAcceptedAssociationScopesBeforeActivation(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatalf("NewEndpoint(RoleSGP): %v", err)
	}
	firstListener := newListener(endpoint, NewListenerConfig(scopedSGPConfig(10, 1)))
	secondListener := newListener(endpoint, NewListenerConfig(scopedSGPConfig(20, 1)))
	first, _ := newTestConn(t, StateASPDown, RoleSGP)
	first.listener = firstListener
	first.cfg = scopedSGPConfig(10, 1)
	second, _ := newTestConn(t, StateASPDown, RoleSGP)
	second.listener = secondListener
	second.cfg = scopedSGPConfig(20, 1)
	if !firstListener.promoteAcceptedAssociation(first) {
		t.Fatal("failed to attach first Association")
	}
	if !secondListener.promoteAcceptedAssociation(second) {
		t.Fatal("failed to attach second Association")
	}
	defer func() { _ = endpoint.Close() }()

	data := messages.NewData(
		nil,
		params.NewRoutingContext(1),
		params.NewProtocolData(1, 2, params.ServiceIndSCCP, 0, 0, 1, []byte("pre-activation ambiguity")),
		nil,
	)
	if _, err := firstListener.DistributeData(data); !errors.Is(err, ErrInvalidNetworkAppearance) {
		t.Fatalf("DistributeData error = %v, want ErrInvalidNetworkAppearance", err)
	}
}

func TestSGPEndpointContextlessDistributionUsesExplicitNetworkAppearance(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatalf("NewEndpoint(RoleSGP): %v", err)
	}
	firstConfig := scopedSGPConfig(10, 1)
	firstConfig.RoutingContexts = nil
	secondConfig := scopedSGPConfig(20, 1)
	secondConfig.RoutingContexts = nil
	firstListener := newListener(endpoint, NewListenerConfig(firstConfig))
	secondListener := newListener(endpoint, NewListenerConfig(secondConfig))
	first, firstSent := attachActiveEndpointAssociation(t, endpoint, firstListener, ASKey{
		NetworkAppearance: 10, NetworkAppearanceSet: true,
	})
	second, secondSent := attachActiveEndpointAssociation(t, endpoint, secondListener, ASKey{
		NetworkAppearance: 20, NetworkAppearanceSet: true,
	})
	defer func() {
		_ = first.Close()
		_ = second.Close()
		_ = endpoint.Close()
	}()
	*firstSent = nil
	*secondSent = nil

	data := messages.NewData(
		params.NewNetworkAppearance(20),
		nil,
		params.NewProtocolData(1, 2, params.ServiceIndSCCP, 0, 0, 1, []byte("network 20 contextless AS")),
		nil,
	)
	result, err := firstListener.DistributeData(data)
	if err != nil {
		t.Fatalf("DistributeData: %v", err)
	}
	if result.Delivered != 1 || result.Queued {
		t.Fatalf("distribution result = %#v, want one delivery", result)
	}
	if len(*firstSent) != 0 || len(*secondSent) != 1 {
		t.Fatalf("contextless deliveries by Network Appearance = (%d, %d), want (0, 1)",
			len(*firstSent), len(*secondSent))
	}
}

func TestSGPEndpointRejectsAmbiguousContextlessDistribution(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatalf("NewEndpoint(RoleSGP): %v", err)
	}
	firstConfig := scopedSGPConfig(10, 1)
	firstConfig.RoutingContexts = nil
	secondConfig := scopedSGPConfig(20, 1)
	secondConfig.RoutingContexts = nil
	firstListener := newListener(endpoint, NewListenerConfig(firstConfig))
	secondListener := newListener(endpoint, NewListenerConfig(secondConfig))
	first, firstSent := attachActiveEndpointAssociation(t, endpoint, firstListener, ASKey{
		NetworkAppearance: 10, NetworkAppearanceSet: true,
	})
	second, secondSent := attachActiveEndpointAssociation(t, endpoint, secondListener, ASKey{
		NetworkAppearance: 20, NetworkAppearanceSet: true,
	})
	defer func() {
		_ = first.Close()
		_ = second.Close()
		_ = endpoint.Close()
	}()
	*firstSent = nil
	*secondSent = nil

	data := messages.NewData(
		nil,
		nil,
		params.NewProtocolData(1, 2, params.ServiceIndSCCP, 0, 0, 1, []byte("ambiguous contextless AS")),
		nil,
	)
	if _, err := firstListener.DistributeData(data); !errors.Is(err, ErrInvalidNetworkAppearance) {
		t.Fatalf("DistributeData error = %v, want ErrInvalidNetworkAppearance", err)
	}
	if len(*firstSent) != 0 || len(*secondSent) != 0 {
		t.Fatalf("ambiguous contextless DATA deliveries = (%d, %d), want none",
			len(*firstSent), len(*secondSent))
	}
}

func TestSGPEndpointSharesStateAcrossAcceptedAndInitiatedAssociations(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatalf("NewEndpoint(RoleSGP): %v", err)
	}
	listener := newListener(endpoint, NewListenerConfig(scopedSGPConfig(10, 1)))
	accepted, acceptedSent := attachActiveEndpointAssociation(t, endpoint, listener, ASKey{
		NetworkAppearance: 10, NetworkAppearanceSet: true,
		RoutingContext: 1, RoutingContextSet: true,
	})
	initiated, initiatedSent := attachActiveInitiatedEndpointAssociation(t, endpoint, ASKey{
		NetworkAppearance: 20, NetworkAppearanceSet: true,
		RoutingContext: 1, RoutingContextSet: true,
	})
	defer func() {
		_ = accepted.Close()
		_ = initiated.Close()
		_ = endpoint.Close()
	}()
	*acceptedSent = nil
	*initiatedSent = nil

	if accepted.as != initiated.as || accepted.nif != initiated.nif || accepted.destinations != initiated.destinations {
		t.Fatal("accepted and SCTP-initiating SGP Associations did not share Endpoint state")
	}
	data := messages.NewData(
		params.NewNetworkAppearance(20),
		params.NewRoutingContext(1),
		params.NewProtocolData(1, 2, params.ServiceIndSCCP, 0, 0, 1, []byte("initiated association")),
		nil,
	)
	result, err := listener.DistributeData(data)
	if err != nil {
		t.Fatalf("DistributeData: %v", err)
	}
	if result.Delivered != 1 || len(*acceptedSent) != 0 || len(*initiatedSent) != 1 {
		t.Fatalf("distribution = %#v, accepted=%d initiated=%d; want initiated only",
			result, len(*acceptedSent), len(*initiatedSent))
	}
}

func scopedSGPConfig(networkAppearance, routingContext uint32) *AssociationConfig {
	config := mcSGPConfig()
	config.NetworkAppearance = params.NewNetworkAppearance(networkAppearance)
	config.RoutingContexts = params.NewRoutingContext(routingContext)
	config.TrafficModeType = params.NewTrafficModeType(params.TrafficModeLoadshare)
	return config
}

func attachActiveEndpointAssociation(t *testing.T, endpoint *Endpoint, listener *Listener, key ASKey) (*Association, *[]messages.M3UA) {
	t.Helper()
	association, sent := newTestConn(t, StateASPActive, RoleSGP)
	association.listener = listener
	if key.NetworkAppearanceSet {
		association.cfg.NetworkAppearance = params.NewNetworkAppearance(key.NetworkAppearance)
	} else {
		association.cfg.NetworkAppearance = nil
	}
	if key.RoutingContextSet {
		association.cfg.RoutingContexts = params.NewRoutingContext(key.RoutingContext)
	} else {
		association.cfg.RoutingContexts = nil
	}
	association.trafficModes = trafficModeSnapshot{}
	association.trafficModes.freeze(newTrafficModePolicy(association.cfg))
	if !listener.promoteAcceptedAssociation(association) {
		t.Fatal("failed to attach Association to Endpoint")
	}
	if key.RoutingContextSet {
		association.noteRoutingContextsActive([]uint32{key.RoutingContext})
	} else {
		association.noteRoutingContextsActive(nil)
	}
	applicationServer := endpoint.as.get(key)
	applicationServer.setTrafficMode(params.TrafficModeLoadshare)
	applicationServer.setASPState(association, StateASPActive, time.Hour)
	return association, sent
}

func attachActiveInitiatedEndpointAssociation(t *testing.T, endpoint *Endpoint, key ASKey) (*Association, *[]messages.M3UA) {
	t.Helper()
	association, sent := newTestConn(t, StateASPActive, RoleSGP)
	if key.NetworkAppearanceSet {
		association.cfg.NetworkAppearance = params.NewNetworkAppearance(key.NetworkAppearance)
	} else {
		association.cfg.NetworkAppearance = nil
	}
	if key.RoutingContextSet {
		association.cfg.RoutingContexts = params.NewRoutingContext(key.RoutingContext)
	} else {
		association.cfg.RoutingContexts = nil
	}
	association.trafficModes = trafficModeSnapshot{}
	association.trafficModes.freeze(newTrafficModePolicy(association.cfg))
	association.as, association.nif, association.destinations, association.mtp3Restarts = endpoint.sgpRegistry()
	association.as.register(association.configuredASKeys())
	if !endpoint.trackAssociation(association) {
		t.Fatal("failed to attach SCTP-initiating Association to Endpoint")
	}
	if key.RoutingContextSet {
		association.noteRoutingContextsActive([]uint32{key.RoutingContext})
	} else {
		association.noteRoutingContextsActive(nil)
	}
	applicationServer := endpoint.as.get(key)
	applicationServer.setTrafficMode(params.TrafficModeLoadshare)
	applicationServer.setASPState(association, StateASPActive, time.Hour)
	return association, sent
}

func TestClosingOneSGPAssociationKeepsEndpointAndSiblingAlive(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatalf("NewEndpoint(RoleSGP): %v", err)
	}
	listener := newListener(endpoint, NewListenerConfig(mcSGPConfig()))
	if !endpoint.trackListener(listener) {
		t.Fatal("failed to attach Listener to open Endpoint")
	}

	first := attachEndpointAssociation(t, endpoint, listener)
	second := attachEndpointAssociation(t, endpoint, listener)
	if err := first.Close(); err != nil {
		t.Fatalf("close first Association: %v", err)
	}

	select {
	case <-second.Done():
		t.Fatal("closing one Association closed its sibling")
	default:
	}
	select {
	case <-endpoint.Done():
		t.Fatal("closing one Association closed its Endpoint")
	default:
	}
	first.as.mu.Lock()
	registryClosed := first.as.closed
	first.as.mu.Unlock()
	if registryClosed {
		t.Fatal("closing one Association closed the Endpoint Application Server registry")
	}

	if err := endpoint.Close(); err != nil {
		t.Fatalf("Endpoint.Close: %v", err)
	}
}

func TestClosingOneSGPListenerKeepsSharedStateAndSiblingAlive(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatalf("NewEndpoint(RoleSGP): %v", err)
	}
	firstListener := newListener(endpoint, NewListenerConfig(mcSGPConfig()))
	secondListener := newListener(endpoint, NewListenerConfig(mcSGPConfig()))
	for _, listener := range []*Listener{firstListener, secondListener} {
		if !endpoint.trackListener(listener) {
			t.Fatal("failed to attach Listener to open Endpoint")
		}
	}
	firstListener.registry()
	secondAssociation := attachEndpointAssociation(t, endpoint, secondListener)

	if err := firstListener.Close(); err != nil {
		t.Fatalf("close first Listener: %v", err)
	}

	select {
	case <-secondAssociation.Done():
		t.Fatal("closing one Listener closed an Association accepted by its sibling")
	default:
	}
	endpoint.as.mu.Lock()
	registryClosed := endpoint.as.closed
	endpoint.as.mu.Unlock()
	if registryClosed {
		t.Fatal("closing one Listener closed the Endpoint Application Server registry")
	}
	select {
	case <-endpoint.Done():
		t.Fatal("closing one Listener closed its Endpoint")
	default:
	}

	if err := endpoint.Close(); err != nil {
		t.Fatalf("Endpoint.Close: %v", err)
	}
}

func TestConcurrentEndpointCloseWaitsForShutdownCompletion(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatalf("NewEndpoint(RoleSGP): %v", err)
	}
	registry, _, _, _ := endpoint.sgpRegistry()
	registry.mu.Lock()
	locked := true
	defer func() {
		if locked {
			registry.mu.Unlock()
		}
	}()

	firstReturned := make(chan error, 1)
	go func() { firstReturned <- endpoint.Close() }()
	if !waitFor(func() bool {
		endpoint.mu.Lock()
		defer endpoint.mu.Unlock()
		return endpoint.closed
	}, time.Second) {
		t.Fatal("first Endpoint.Close did not start")
	}

	secondReturned := make(chan error, 1)
	go func() { secondReturned <- endpoint.Close() }()
	select {
	case err := <-secondReturned:
		t.Fatalf("concurrent Endpoint.Close returned before shutdown completed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	registry.mu.Unlock()
	locked = false
	for index, result := range []<-chan error{firstReturned, secondReturned} {
		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("Endpoint.Close caller %d: %v", index, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("Endpoint.Close caller %d did not return", index)
		}
	}
}

func TestEndpointCloseWaitsForConcurrentListenerClose(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatalf("NewEndpoint(RoleSGP): %v", err)
	}
	listener := newListener(endpoint, NewListenerConfig(mcSGPConfig()))
	if !endpoint.trackListener(listener) {
		t.Fatal("failed to attach Listener")
	}
	association, _ := newTestConn(t, StateASPActive, RoleSGP)
	association.listener = listener
	if !listener.track(association) {
		t.Fatal("failed to attach Association")
	}

	association.muState.Lock()
	locked := true
	defer func() {
		if locked {
			association.muState.Unlock()
		}
	}()
	listenerClosed := make(chan error, 1)
	go func() { listenerClosed <- listener.Close() }()
	if !waitFor(func() bool {
		listener.muConns.Lock()
		defer listener.muConns.Unlock()
		return listener.closed
	}, time.Second) {
		t.Fatal("Listener.Close did not start")
	}

	endpointClosed := make(chan error, 1)
	go func() { endpointClosed <- endpoint.Close() }()
	select {
	case err := <-endpointClosed:
		t.Fatalf("Endpoint.Close returned before concurrent Listener.Close completed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	select {
	case <-endpoint.Done():
		t.Fatal("Endpoint.Done closed before concurrent Listener.Close completed")
	default:
	}

	association.muState.Unlock()
	locked = false
	for name, result := range map[string]<-chan error{
		"Listener.Close": listenerClosed,
		"Endpoint.Close": endpointClosed,
	} {
		select {
		case err := <-result:
			if err != nil {
				t.Fatalf("%s: %v", name, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("%s did not return", name)
		}
	}
}

func TestSGPEndpointQuiescesApplicationServersBeforeClosingAssociations(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatalf("NewEndpoint(RoleSGP): %v", err)
	}
	listener := newListener(endpoint, NewListenerConfig(mcSGPConfig()))
	if !endpoint.trackListener(listener) {
		t.Fatal("failed to attach Listener")
	}
	association := attachEndpointAssociation(t, endpoint, listener)

	endpoint.as.mu.Lock()
	locked := true
	defer func() {
		if locked {
			endpoint.as.mu.Unlock()
		}
	}()
	closed := make(chan error, 1)
	go func() { closed <- endpoint.Close() }()
	if !waitFor(func() bool {
		endpoint.mu.Lock()
		defer endpoint.mu.Unlock()
		return endpoint.closed
	}, time.Second) {
		t.Fatal("Endpoint.Close did not start")
	}

	select {
	case <-association.Done():
		t.Fatal("Endpoint closed an Association before quiescing shared Application Server state")
	case <-time.After(50 * time.Millisecond):
	}

	endpoint.as.mu.Unlock()
	locked = false
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Endpoint.Close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Endpoint.Close did not complete after Application Server state quiesced")
	}
	select {
	case <-association.Done():
	default:
		t.Fatal("Endpoint left Association open after shutdown")
	}
}

func TestSGPEndpointCloseClosesEveryListenerAndAssociation(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatalf("NewEndpoint(RoleSGP): %v", err)
	}
	firstListener := newListener(endpoint, NewListenerConfig(mcSGPConfig()))
	secondListener := newListener(endpoint, NewListenerConfig(mcSGPConfig()))
	for _, listener := range []*Listener{firstListener, secondListener} {
		if !endpoint.trackListener(listener) {
			t.Fatal("failed to attach Listener to open Endpoint")
		}
	}
	firstAssociation := attachEndpointAssociation(t, endpoint, firstListener)
	secondAssociation := attachEndpointAssociation(t, endpoint, secondListener)

	if err := endpoint.Close(); err != nil {
		t.Fatalf("Endpoint.Close: %v", err)
	}
	if err := endpoint.Close(); err != nil {
		t.Fatalf("second Endpoint.Close: %v", err)
	}

	for index, listener := range []*Listener{firstListener, secondListener} {
		listener.muConns.Lock()
		closed := listener.closed
		listener.muConns.Unlock()
		if !closed {
			t.Errorf("Listener %d remained open after Endpoint.Close", index)
		}
	}
	for index, association := range []*Association{firstAssociation, secondAssociation} {
		select {
		case <-association.Done():
		default:
			t.Errorf("Association %d remained open after Endpoint.Close", index)
		}
	}
	select {
	case <-endpoint.Done():
	default:
		t.Fatal("Endpoint.Done remained open after Endpoint.Close")
	}
}

func TestSGPEndpointConcurrentAttachmentQueriesAndShutdown(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatalf("NewEndpoint(RoleSGP): %v", err)
	}
	listener := newListener(endpoint, NewListenerConfig(scopedSGPConfig(10, 1)))
	if !endpoint.trackListener(listener) {
		t.Fatal("failed to attach Listener")
	}
	key := ASKey{
		NetworkAppearance: 10, NetworkAppearanceSet: true,
		RoutingContext: 1, RoutingContextSet: true,
	}
	associations := make([]*Association, 64)
	for index := range associations {
		association, _ := newTestConn(t, StateASPInactive, RoleSGP)
		association.listener = listener
		association.cfg.NetworkAppearance = params.NewNetworkAppearance(10)
		association.cfg.RoutingContexts = params.NewRoutingContext(1)
		associations[index] = association
	}

	start := make(chan struct{})
	var operations sync.WaitGroup
	var successfulAttachments atomic.Int64
	for _, association := range associations {
		operations.Add(1)
		go func() {
			defer operations.Done()
			<-start
			if listener.promoteAcceptedAssociation(association) {
				successfulAttachments.Add(1)
				_ = association.Close()
			}
		}()
	}
	for range 8 {
		operations.Add(1)
		go func() {
			defer operations.Done()
			<-start
			for {
				_ = listener.ApplicationServerStateForAS(key)
				select {
				case <-endpoint.Done():
					return
				default:
				}
			}
		}()
	}
	close(start)
	if !waitFor(func() bool { return successfulAttachments.Load() > 0 }, time.Second) {
		t.Fatal("no Association attached before shutdown")
	}
	if err := endpoint.Close(); err != nil {
		t.Fatalf("Endpoint.Close: %v", err)
	}
	operations.Wait()
	if successfulAttachments.Load() == 0 {
		t.Fatal("concurrent attachment path was not exercised")
	}
}

func attachEndpointAssociation(t *testing.T, endpoint *Endpoint, listener *Listener) *Association {
	t.Helper()
	association, _ := newTestConn(t, StateASPActive, RoleSGP)
	association.listener = listener
	if !listener.promoteAcceptedAssociation(association) {
		t.Fatal("failed to attach Association to open Listener")
	}
	if association.endpoint != endpoint {
		t.Fatal("accepted Association did not retain its Endpoint")
	}
	return association
}

func TestSGPEndpointCanInitiateMultipleAssociations(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	type acceptResult struct {
		association *Association
		err         error
	}
	accepted := make(chan acceptResult, 2)
	listeners := make([]*Listener, 0, 2)
	for index, identifier := range []uint32{0xABCD0001, 0xABCD0002} {
		aspEndpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP})
		if err != nil {
			t.Fatalf("NewEndpoint(RoleASP) %d: %v", index, err)
		}
		defer func() { _ = aspEndpoint.Close() }()
		aspConfig := mcASPConfig(identifier)
		aspConfig.RoutingContexts = params.NewRoutingContext(1)
		listener, err := aspEndpoint.Listen(
			"m3ua", mcAddr(0, "127.0.0.1"), NewListenerConfig(aspConfig),
		)
		if err != nil {
			skipIfSCTPUnsupported(t, err)
			t.Fatalf("ASP Listen %d: %v", index, err)
		}
		listeners = append(listeners, listener)
		go func() {
			association, acceptErr := listener.Accept(ctx)
			accepted <- acceptResult{association: association, err: acceptErr}
		}()
	}

	sgpEndpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatalf("NewEndpoint(RoleSGP): %v", err)
	}
	defer func() { _ = sgpEndpoint.Close() }()
	sgpConfig := mcSGPConfig()
	sgpConfig.RoutingContexts = params.NewRoutingContext(1)
	initiated := make([]*Association, 0, len(listeners))
	for index, listener := range listeners {
		association, err := sgpEndpoint.Dial(
			ctx, "m3ua", nil, listener.Addr().(*sctp.SCTPAddr), sgpConfig,
		)
		if err != nil {
			t.Fatalf("SGP Dial %d: %v", index, err)
		}
		initiated = append(initiated, association)
	}

	for index := range listeners {
		result := <-accepted
		if result.err != nil {
			t.Fatalf("ASP Accept %d: %v", index, result.err)
		}
		defer func() { _ = result.association.Close() }()
	}
	if initiated[0].as != initiated[1].as || initiated[0].nif != initiated[1].nif ||
		initiated[0].destinations != initiated[1].destinations {
		t.Fatal("SCTP-initiating SGP Associations did not share Endpoint state")
	}
	if err := initiated[0].Close(); err != nil {
		t.Fatalf("close first SGP Association: %v", err)
	}
	select {
	case <-initiated[1].Done():
		t.Fatal("closing one SCTP-initiating Association closed its sibling")
	default:
	}
}

func TestSGPListenerCloseRejectsAssociationSelectedAfterShutdown(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	selectorEntered := make(chan struct{})
	selectorRelease := make(chan struct{})
	config := NewListenerConfig(mcSGPConfig())
	config.SelectAssociationConfig = func(AcceptInfo) (*AssociationConfig, error) {
		close(selectorEntered)
		<-selectorRelease
		return mcSGPConfig(), nil
	}

	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatalf("NewEndpoint(RoleSGP): %v", err)
	}
	defer func() { _ = endpoint.Close() }()
	address := mcAddr(0, "127.0.0.1")
	listener, err := endpoint.Listen("m3ua", address, config)
	if err != nil {
		skipIfSCTPUnsupported(t, err)
		t.Fatalf("SGP Listen: %v", err)
	}

	accepted := make(chan error, 1)
	go func() {
		association, acceptErr := listener.Accept(ctx)
		if association != nil {
			_ = association.Close()
		}
		accepted <- acceptErr
	}()

	raw, err := sctp.DialSCTP("sctp", nil, listener.Addr().(*sctp.SCTPAddr))
	if err != nil {
		_ = listener.Close()
		t.Fatalf("raw SCTP association: %v", err)
	}
	defer func() { _ = raw.Close() }()

	select {
	case <-selectorEntered:
	case <-ctx.Done():
		_ = listener.Close()
		t.Fatalf("selector was not entered: %v", ctx.Err())
	}

	if err := listener.Close(); err != nil {
		t.Fatalf("close Listener: %v", err)
	}

	close(selectorRelease)
	select {
	case err := <-accepted:
		if err == nil {
			t.Fatal("Accept succeeded after Listener.Close")
		}
	case <-ctx.Done():
		t.Fatalf("Accept did not return after selector release: %v", ctx.Err())
	}
}

func TestSGPListenerCloseStopsAssociationDuringM3UAEstablishment(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatalf("NewEndpoint(RoleSGP): %v", err)
	}
	config := mcSGPConfig()
	config.EstablishTimeout = 30 * time.Second
	listener, err := endpoint.Listen(
		"m3ua", mcAddr(0, "127.0.0.1"), NewListenerConfig(config),
	)
	if err != nil {
		skipIfSCTPUnsupported(t, err)
		t.Fatalf("SGP Listen: %v", err)
	}

	accepted := make(chan error, 1)
	go func() {
		association, acceptErr := listener.Accept(ctx)
		if association != nil {
			_ = association.Close()
		}
		accepted <- acceptErr
	}()
	raw, err := sctp.DialSCTP("sctp", nil, listener.Addr().(*sctp.SCTPAddr))
	if err != nil {
		_ = listener.Close()
		t.Fatalf("raw SCTP association: %v", err)
	}
	defer func() { _ = raw.Close() }()

	if !waitFor(func() bool {
		listener.muConns.Lock()
		defer listener.muConns.Unlock()
		return listener.activeAccept == 1 && len(listener.conns) == 1
	}, 5*time.Second) {
		_ = listener.Close()
		t.Fatal("accepted SCTP association never entered M3UA establishment")
	}

	if err := listener.Close(); err != nil {
		t.Fatalf("close Listener: %v", err)
	}
	select {
	case err := <-accepted:
		if err == nil {
			t.Fatal("Accept succeeded for a peer that never sent ASP Up")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Accept remained in M3UA establishment after Listener.Close")
	}

	cancelled, cancelDial := context.WithCancel(context.Background())
	cancelDial()
	if _, err := endpoint.Dial(cancelled, "m3ua", nil, nil, mcSGPConfig()); !errors.Is(err, context.Canceled) {
		t.Fatalf("SGP Dial after in-flight establishment stopped error = %v, want context.Canceled", err)
	}
}

func TestIPSPDialAndListenRequireAnExplicitExchangeModel(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleIPSP})
	if err != nil {
		t.Fatalf("NewEndpoint(RoleIPSP): %v", err)
	}

	if _, err := endpoint.Dial(context.Background(), "m3ua", nil, nil, NewAssociationConfig(0, 0, 0, 0, 0, 0)); !errors.Is(err, ErrUnsupportedRole) {
		t.Fatalf("Dial error = %v, want ErrUnsupportedRole", err)
	}
	if _, err := endpoint.Listen("m3ua", nil, NewListenerConfig(nil)); !errors.Is(err, ErrUnsupportedRole) {
		t.Fatalf("Listen error = %v, want ErrUnsupportedRole", err)
	}
}

func TestIPSPAssociationRejectsSGPDestinationProcedures(t *testing.T) {
	association, _ := newTestConnWithContexts(t, StateASPActive, RoleIPSP, 1)
	const (
		networkAppearance = uint32(7)
		routingContext    = uint32(1)
		pointCode         = uint32(0x123456)
	)

	if err := association.ReportDestinationStateForNetworkAndRoutingContext(
		networkAppearance, routingContext, pointCode, DestinationRestricted,
	); !errors.Is(err, ErrUnsupportedRole) {
		t.Fatalf("IPSP destination report error = %v, want ErrUnsupportedRole", err)
	}
	association.SetDestinationStateForNetworkAndRoutingContext(
		networkAppearance, routingContext, pointCode, DestinationRestricted,
	)
	if got := association.DestinationStateForNetworkAndRoutingContext(
		networkAppearance, routingContext, pointCode,
	); got != DestinationAvailable {
		t.Fatalf("IPSP destination state = %v after unsupported update, want available", got)
	}
}

func TestASPAssociationRejectsSGPDestinationReports(t *testing.T) {
	association, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
	const (
		networkAppearance = uint32(7)
		routingContext    = uint32(1)
		pointCode         = uint32(0x123456)
	)
	association.SetDestinationStateForNetworkAndRoutingContext(
		networkAppearance, routingContext, pointCode, DestinationRestricted,
	)

	if err := association.ReportDestinationStateForNetworkAndRoutingContext(
		networkAppearance, routingContext, pointCode, DestinationUnavailable,
	); !errors.Is(err, ErrUnsupportedRole) {
		t.Fatalf("ASP destination report error = %v, want ErrUnsupportedRole", err)
	}
	if got := association.DestinationStateForNetworkAndRoutingContext(
		networkAppearance, routingContext, pointCode,
	); got != DestinationRestricted {
		t.Fatalf("ASP destination state = %v after rejected report, want restricted", got)
	}
}

func TestASPListenerRejectsSGPDestinationProcedures(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP})
	if err != nil {
		t.Fatalf("NewEndpoint(RoleASP): %v", err)
	}
	config := mcASPConfig(1)
	config.NetworkAppearance = params.NewNetworkAppearance(7)
	listener := newListener(endpoint, NewListenerConfig(config))
	const pointCode = uint32(0x123456)

	if err := listener.ReportDestinationStateForNetworkAndRoutingContext(
		7, 1, pointCode, DestinationRestricted,
	); !errors.Is(err, ErrUnsupportedRole) {
		t.Fatalf("ASP Listener destination report error = %v, want ErrUnsupportedRole", err)
	}
	listener.SetDestinationStateForNetworkAndRoutingContext(7, 1, pointCode, DestinationRestricted)
	if state, known := listener.DestinationStateForNetworkAndRoutingContext(7, 1, pointCode); known {
		t.Fatalf("ASP Listener destination state = (%v, %v) after unsupported update, want unknown", state, known)
	}
}

func TestDialNormalizesAZeroAssociationConfigBeforeTransport(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP})
	if err != nil {
		t.Fatalf("NewEndpoint(RoleASP): %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := endpoint.Dial(ctx, "m3ua", nil, nil, &AssociationConfig{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Dial error = %v, want context.Canceled", err)
	}
}

func TestDialRejectsNilAssociationConfigBeforeTransport(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP})
	if err != nil {
		t.Fatalf("NewEndpoint(RoleASP): %v", err)
	}
	if _, err := endpoint.Dial(context.Background(), "m3ua", nil, nil, nil); !errors.Is(err, ErrNilAssociationConfig) {
		t.Fatalf("Dial error = %v, want ErrNilAssociationConfig", err)
	}
}

func TestCancelledSGPDialDoesNotRegisterApplicationServer(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatalf("NewEndpoint(RoleSGP): %v", err)
	}
	config := scopedSGPConfig(10, 7)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := endpoint.Dial(ctx, "m3ua", nil, nil, config); !errors.Is(err, context.Canceled) {
		t.Fatalf("Dial error = %v, want context.Canceled", err)
	}
	key := ASKey{
		NetworkAppearance: 10, NetworkAppearanceSet: true,
		RoutingContext: 7, RoutingContextSet: true,
	}
	if _, registered := endpoint.as.lookup(key); registered {
		t.Fatal("cancelled Dial registered an Application Server")
	}
}

func TestAssociationConfigRejectsRoleSpecificSettings(t *testing.T) {
	tests := []struct {
		name   string
		role   Role
		config *AssociationConfig
	}{
		{
			name: "ASP with SGP authorization policy",
			role: RoleASP,
			config: func() *AssociationConfig {
				config := NewAssociationConfig(0, 0, 0, 0, 0, 0)
				config.AuthorizeASP = func(ASPIdentity) []uint32 { return nil }
				return config
			}(),
		},
		{
			name: "SGP with local ASP Identifier",
			role: RoleSGP,
			config: NewAssociationConfig(0, 0, 0, 0, 0, 0).
				SetASPIdentifier(7),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := validateAssociationConfigForRole(test.role, test.config); !errors.Is(err, ErrInvalidRoleConfiguration) {
				t.Fatalf("validation error = %v, want ErrInvalidRoleConfiguration", err)
			}
		})
	}
}

func TestEndpointRoleIsIndependentOfSCTPOrientation(t *testing.T) {
	tests := []struct {
		name         string
		port         int
		acceptRole   Role
		dialRole     Role
		acceptConfig *AssociationConfig
		dialConfig   *AssociationConfig
	}{
		{
			name:         "ASP dials SGP",
			port:         3301,
			acceptRole:   RoleSGP,
			dialRole:     RoleASP,
			acceptConfig: mcSGPConfig(),
			dialConfig:   mcASPConfig(0x11111111),
		},
		{
			name:         "SGP dials ASP",
			port:         3302,
			acceptRole:   RoleASP,
			dialRole:     RoleSGP,
			acceptConfig: mcASPConfig(0x11111111),
			dialConfig:   mcSGPConfig(),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			acceptEndpoint, err := NewEndpoint(EndpointConfig{Role: test.acceptRole})
			if err != nil {
				t.Fatalf("NewEndpoint(%v): %v", test.acceptRole, err)
			}
			dialEndpoint, err := NewEndpoint(EndpointConfig{Role: test.dialRole})
			if err != nil {
				t.Fatalf("NewEndpoint(%v): %v", test.dialRole, err)
			}

			address := mcAddr(test.port, "127.0.0.2")
			listener, err := acceptEndpoint.Listen(
				"m3ua", address, NewListenerConfig(test.acceptConfig),
			)
			if err != nil {
				skipIfSCTPUnsupported(t, err)
				t.Fatalf("Listen: %v", err)
			}
			defer func() { _ = listener.Close() }()

			type acceptResult struct {
				association *Association
				err         error
			}
			accepted := make(chan acceptResult, 1)
			go func() {
				association, acceptErr := listener.Accept(ctx)
				accepted <- acceptResult{association: association, err: acceptErr}
			}()

			dialed, err := dialEndpoint.Dial(
				ctx, "m3ua", mcAddr(test.port+100, "127.0.0.1"), address, test.dialConfig,
			)
			if err != nil {
				t.Fatalf("Dial: %v", err)
			}
			defer func() { _ = dialed.Close() }()

			var acceptedAssociation *Association
			select {
			case result := <-accepted:
				if result.err != nil {
					t.Fatalf("Accept: %v", result.err)
				}
				acceptedAssociation = result.association
				defer func() { _ = acceptedAssociation.Close() }()
				if got := acceptedAssociation.Role(); got != test.acceptRole {
					t.Errorf("accepted role = %v, want %v", got, test.acceptRole)
				}
			case <-ctx.Done():
				t.Fatalf("Accept did not complete: %v", ctx.Err())
			}

			if got := dialed.Role(); got != test.dialRole {
				t.Errorf("dialed role = %v, want %v", got, test.dialRole)
			}

			if test.dialRole == RoleSGP {
				const pointCode = 0x1234
				dialed.SetDestinationStateForNetworkAndRoutingContext(
					0, 1, pointCode, DestinationAvailable,
				)
				if _, err := acceptedAssociation.WriteSignal(
					messages.NewDestinationStateAudit(
						nil,
						params.NewRoutingContext(1),
						params.NewAffectedPointCode(pointCode),
						nil,
					),
				); err != nil {
					t.Fatalf("ASP write DAUD: %v", err)
				}
				waitForEndpointDestinationStatus(
					t, ctx, acceptedAssociation, pointCode, DestinationAvailable, "DAUD response",
				)

				restartPointCode := uint32(0x2345)
				restartDestination := AffectedDestination{
					NetworkAppearance:    0,
					NetworkAppearanceSet: true,
					RoutingContext:       1,
					RoutingContextSet:    true,
					PointCode:            restartPointCode,
				}
				restart, err := dialed.BeginMTP3Restart(restartDestination)
				if err != nil {
					t.Fatalf("dialing SGP begin MTP3 restart: %v", err)
				}
				waitForEndpointDestinationStatus(
					t, ctx, acceptedAssociation, restartPointCode, DestinationUnavailable,
					"restart isolation status",
				)
				if err := restart.Update(restartDestination, DestinationAvailable); err != nil {
					t.Fatalf("dialing SGP stage MTP3 restart recovery: %v", err)
				}
				if err := restart.Complete(); err != nil {
					t.Fatalf("dialing SGP complete MTP3 restart: %v", err)
				}
				waitForEndpointDestinationStatus(
					t, ctx, acceptedAssociation, restartPointCode, DestinationAvailable,
					"restart recovery status",
				)

				if err := dialed.SetASAvailable(1, false); err != nil {
					t.Fatalf("dialing SGP isolate Application Server: %v", err)
				}
				if dialed.activeForRoutingContext(1) {
					t.Fatal("dialing SGP retained active traffic scope after AS isolation")
				}
			}
		})
	}
}

func waitForEndpointDestinationStatus(
	t *testing.T,
	ctx context.Context,
	association *Association,
	pointCode uint32,
	state DestinationState,
	description string,
) {
	t.Helper()
	for {
		select {
		case status, ok := <-association.SignallingStatus():
			if !ok {
				t.Fatalf("Association closed before %s arrived", description)
			}
			if status != nil && status.PointCode == pointCode && status.State == state {
				return
			}
		case <-ctx.Done():
			t.Fatalf("%s for point code %#x and state %v did not arrive: %v",
				description, pointCode, state, ctx.Err())
		}
	}
}
