// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// rkmRequesterAssociation is an ASP Association whose peer answers every
// Registration Request with the Routing Contexts the test names, and every
// Deregistration Request with success.
func rkmRequesterAssociation(t *testing.T, assign ...uint32) (*Association, *[]messages.M3UA) {
	t.Helper()
	association := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	association.muState.Lock()
	association.state = StateASPInactive
	association.muState.Unlock()
	t.Cleanup(func() { _ = association.Close() })

	written := make([]messages.M3UA, 0, 4)
	sent := &written
	next := 0
	association.signalWriter = func(message messages.M3UA) (int, error) {
		*sent = append(*sent, message)
		switch request := message.(type) {
		case *messages.RegistrationRequest:
			for _, routingKey := range request.RoutingKeys {
				payload, err := routingKey.RoutingKey()
				if err != nil {
					return 0, err
				}
				routingContext := uint32(0)
				if next < len(assign) {
					routingContext = assign[next]
				}
				next++
				status := params.SuccessfullyRegistered
				if routingContext == 0 {
					status = params.InsufficientResources
				}
				response := messages.NewRegistrationResponse(params.NewRegistrationResult(
					params.NewRegistrationResultPayload(
						payload.LocalRoutingKeyIdentifier.Copy(),
						params.NewRegistrationStatus(status),
						params.NewRoutingContext(routingContext),
					),
				))
				if err := association.handleRegistrationResponse(response); err != nil {
					return 0, err
				}
			}
		case *messages.DeregistrationRequest:
			for _, routingContext := range request.RoutingContext.RoutingContexts() {
				response := messages.NewDeregistrationResponse(params.NewDeregistrationResult(
					params.NewDeregResultPayload(
						params.NewRoutingContext(routingContext),
						params.NewDeregistrationStatus(params.SuccessfullyDeregistered),
					),
				))
				if err := association.handleDeregistrationResponse(response); err != nil {
					return 0, err
				}
			}
		}
		return message.MarshalLen(), nil
	}
	return association, sent
}

// A successful registration reports the canonical Application Server it bound
// and the exact wire scope the peer assigned, so the application never has to
// rebuild either from the Routing Context alone.
func TestRegistrationResultCarriesRemoteASAndExactScope(t *testing.T) {
	association, _ := rkmRequesterAssociation(t, 10)
	association.cfg.PeerSGP = &SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}

	results, err := association.RegisterRoutingKeys(context.Background(), RoutingKeyRegistration{
		RemoteAS:   "as-core",
		RoutingKey: testRoutingKey(7, 100, 3),
	})
	if err != nil {
		t.Fatalf("RegisterRoutingKeys: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("results = %d, want 1", len(results))
	}
	want := SGASKey{SignallingGateway: "sg-a", ApplicationServer: "as-core"}
	if results[0].RemoteAS != want {
		t.Fatalf("RemoteAS = %+v, want %+v", results[0].RemoteAS, want)
	}
	wantKey := ASKey{
		NetworkAppearance:    7,
		NetworkAppearanceSet: true,
		RoutingContext:       10,
		RoutingContextSet:    true,
	}
	if results[0].ASKey != wantKey {
		t.Fatalf("ASKey = %+v, want %+v", results[0].ASKey, wantKey)
	}
	// RFC 4666 Section 4.4.1 finishes the registration before the ASP may go
	// active for the assigned Routing Context, so the binding exists already.
	if key, bound := association.dynamicASKey(10, false); !bound || key != wantKey {
		t.Fatalf("assigned Routing Context bound as %+v bound=%v, want %+v", key, bound, wantKey)
	}
}

// A batch where only some Routing Keys are accepted reports every result in
// input order and binds only the accepted ones.
func TestRegistrationPartialSuccessBindsOnlyItsSuccesses(t *testing.T) {
	association, _ := rkmRequesterAssociation(t, 10, 0, 12)
	association.cfg.PeerSGP = &SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}

	results, err := association.RegisterRoutingKeys(context.Background(),
		RoutingKeyRegistration{RemoteAS: "as-one", RoutingKey: testRoutingKey(7, 100, 3)},
		RoutingKeyRegistration{RemoteAS: "as-two", RoutingKey: testRoutingKey(7, 200, 3)},
		RoutingKeyRegistration{RemoteAS: "as-three", RoutingKey: testRoutingKey(7, 300, 3)},
	)
	if err != nil {
		t.Fatalf("RegisterRoutingKeys: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("results = %d, want 3", len(results))
	}
	if results[0].Status != RegistrationSuccessfullyRegistered ||
		results[2].Status != RegistrationSuccessfullyRegistered {
		t.Fatalf("accepted statuses = %v and %v", results[0].Status, results[2].Status)
	}
	if results[1].Status != RegistrationInsufficientResources {
		t.Fatalf("refused status = %v, want Insufficient Resources", results[1].Status)
	}
	if results[1].ASKey != (ASKey{}) {
		t.Fatalf("refused result carries scope %+v, want none", results[1].ASKey)
	}
	if results[1].RemoteAS != (SGASKey{SignallingGateway: "sg-a", ApplicationServer: "as-two"}) {
		t.Fatalf("refused result RemoteAS = %+v, want the one it requested", results[1].RemoteAS)
	}
	for _, routingContext := range []uint32{10, 12} {
		if _, bound := association.dynamicASKey(routingContext, false); !bound {
			t.Fatalf("accepted Routing Context %d was not bound", routingContext)
		}
	}
	if _, bound := association.dynamicASKey(0, false); bound {
		t.Fatal("a refused registration bound a scope")
	}
}

// Deregistration names an Application Server by the exact binding registration
// confirmed, and a scope that names no confirmed binding never reaches the
// transport.
func TestDeregisterApplicationServersNamesTheConfirmedBinding(t *testing.T) {
	association, sent := rkmRequesterAssociation(t, 10)
	association.cfg.PeerSGP = &SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}
	results, err := association.RegisterRoutingKeys(context.Background(), RoutingKeyRegistration{
		RemoteAS:   "as-core",
		RoutingKey: testRoutingKey(7, 100, 3),
	})
	if err != nil {
		t.Fatalf("RegisterRoutingKeys: %v", err)
	}
	confirmed := results[0].ASKey

	tests := []struct {
		name string
		key  ASKey
	}{
		{
			// RFC 4666 Section 3.6.3 carries Routing Context in DEREG REQ, so a
			// scope without one names nothing the peer can act on.
			name: "contextless scope",
			key:  ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true},
		},
		{
			name: "another Network Appearance",
			key:  ASKey{NetworkAppearance: 8, NetworkAppearanceSet: true, RoutingContext: 10, RoutingContextSet: true},
		},
		{
			name: "an absent Network Appearance",
			key:  ASKey{RoutingContext: 10, RoutingContextSet: true},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			before := len(*sent)
			if _, err := association.DeregisterApplicationServers(context.Background(), test.key); err == nil {
				t.Fatal("DeregisterApplicationServers accepted a scope that contradicts the confirmed binding")
			}
			if len(*sent) != before {
				t.Fatalf("%d message(s) reached the transport before the scope was rejected",
					len(*sent)-before)
			}
		})
	}

	before := len(*sent)
	deregistrations, err := association.DeregisterApplicationServers(context.Background(), confirmed)
	if err != nil {
		t.Fatalf("DeregisterApplicationServers: %v", err)
	}
	if len(deregistrations) != 1 ||
		deregistrations[0].Status != DeregistrationSuccessfullyDeregistered ||
		deregistrations[0].ASKey != confirmed {
		t.Fatalf("deregistration results = %+v, want one success for %+v", deregistrations, confirmed)
	}
	request, ok := (*sent)[before].(*messages.DeregistrationRequest)
	if !ok {
		t.Fatalf("wrote %T, want a Deregistration Request", (*sent)[before])
	}
	if contexts := request.RoutingContext.RoutingContexts(); len(contexts) != 1 || contexts[0] != 10 {
		t.Fatalf("Deregistration Request Routing Contexts = %v, want [10]", contexts)
	}
	if _, bound := association.dynamicASKey(10, false); bound {
		t.Fatal("the deregistered binding is still bound")
	}
}

// Two Application Servers cannot be named by one Routing Context on one
// Association, so a batch that assigns one twice with different scopes is
// refused rather than published.
func TestDeregisterApplicationServersRejectsDuplicateScope(t *testing.T) {
	association, sent := rkmRequesterAssociation(t, 10)
	results, err := association.RegisterRoutingKeys(context.Background(), RoutingKeyRegistration{
		RoutingKey: testRoutingKey(7, 100, 3),
	})
	if err != nil {
		t.Fatalf("RegisterRoutingKeys: %v", err)
	}
	before := len(*sent)
	if _, err := association.DeregisterApplicationServers(
		context.Background(), results[0].ASKey, results[0].ASKey,
	); err == nil {
		t.Fatal("DeregisterApplicationServers accepted one scope twice")
	}
	if len(*sent) != before {
		t.Fatal("a duplicate scope reached the transport")
	}
	if _, err := association.DeregisterApplicationServers(context.Background()); err == nil {
		t.Fatal("DeregisterApplicationServers accepted an empty request")
	}
}

// An unresolved outcome stays unresolved: once a Deregistration Request has
// been submitted and its answer lost, the next attempt on the same scope is
// refused rather than retried into an uncertain peer state.
func TestDeregisterApplicationServersKeepsUnresolvedOutcome(t *testing.T) {
	association, _ := rkmRequesterAssociation(t, 10)
	results, err := association.RegisterRoutingKeys(context.Background(), RoutingKeyRegistration{
		RoutingKey: testRoutingKey(7, 100, 3),
	})
	if err != nil {
		t.Fatalf("RegisterRoutingKeys: %v", err)
	}
	confirmed := results[0].ASKey

	// The peer accepts the request and never answers it.
	written := make(chan struct{}, 1)
	association.signalWriter = func(message messages.M3UA) (int, error) {
		written <- struct{}{}
		return message.MarshalLen(), nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() {
		_, err := association.DeregisterApplicationServers(ctx, confirmed)
		firstDone <- err
	}()
	<-written
	cancel()
	if err := <-firstDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled DeregisterApplicationServers error = %v, want context.Canceled", err)
	}
	if _, err := association.DeregisterApplicationServers(
		context.Background(), confirmed,
	); !errors.Is(err, ErrDeregistrationOutcomeUnknown) {
		t.Fatalf("second DeregisterApplicationServers error = %v, want %v",
			err, ErrDeregistrationOutcomeUnknown)
	}
}

// A provisioned ASP Endpoint authorizes registration the same way it
// authorizes everything else: the Application Server named must be one its SGP
// binds dynamically.
func TestRegisterRoutingKeysChecksProvisionedRemoteAS(t *testing.T) {
	routingKey := testRoutingKey(7, 100, 3)
	config := &ASPConfig{
		SignallingGateways: []SignallingGatewayConfig{{
			ID: "sg-a",
			SGPs: []SignallingGatewayProcessConfig{{
				ID: "sgp-a1",
				ApplicationServers: []RemoteASConfig{
					{ID: "as-dynamic", RoutingKey: &routingKey},
				},
			}},
		}},
	}
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: config})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })

	association, sent := rkmRequesterAssociation(t, 10)
	association.cfg.PeerSGP = &SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}
	if !endpoint.trackAssociation(association) {
		t.Fatal("dynamic Association was not attached")
	}

	before := len(*sent)
	if _, err := association.RegisterRoutingKeys(context.Background(), RoutingKeyRegistration{
		RemoteAS:   "as-absent",
		RoutingKey: routingKey,
	}); !errors.Is(err, ErrUnknownRemoteAS) {
		t.Fatalf("unprovisioned Application Server error = %v, want %v", err, ErrUnknownRemoteAS)
	}
	if len(*sent) != before {
		t.Fatal("an unprovisioned Application Server reached the transport")
	}

	results, err := association.RegisterRoutingKeys(context.Background(), RoutingKeyRegistration{
		RemoteAS:   "as-dynamic",
		RoutingKey: routingKey,
	})
	if err != nil {
		t.Fatalf("provisioned RegisterRoutingKeys: %v", err)
	}
	if results[0].RemoteAS != (SGASKey{SignallingGateway: "sg-a", ApplicationServer: "as-dynamic"}) {
		t.Fatalf("RemoteAS = %+v", results[0].RemoteAS)
	}
}
