package m3ua

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestSGPRegistrationAndDeregistrationProcedures(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{
		Role: RoleSGP,
		RoutingKeyManagement: &RoutingKeyManagementConfig{
			AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
				return RegistrationSuccessfullyRegistered
			},
			AllowDynamicRoutingKeys: true,
			RemoveUnusedRoutingKeys: true,
		},
	})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	defer func() { _ = endpoint.Close() }()
	association := newAssociation(RoleSGP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	association.endpoint = endpoint
	association.as = endpoint.as
	association.muState.Lock()
	association.state = StateASPInactive
	association.muState.Unlock()

	var writtenMu sync.Mutex
	var written []messages.M3UA
	association.signalWriter = func(message messages.M3UA) (int, error) {
		writtenMu.Lock()
		written = append(written, message)
		writtenMu.Unlock()
		return message.MarshalLen(), nil
	}
	snapshotWritten := func() []messages.M3UA {
		writtenMu.Lock()
		defer writtenMu.Unlock()
		return append([]messages.M3UA(nil), written...)
	}
	registrationResponse := func() *messages.RegistrationResponse {
		for _, message := range snapshotWritten() {
			if response, ok := message.(*messages.RegistrationResponse); ok {
				return response
			}
		}
		return nil
	}
	deregistrationResponse := func() *messages.DeregistrationResponse {
		for _, message := range snapshotWritten() {
			if response, ok := message.(*messages.DeregistrationResponse); ok {
				return response
			}
		}
		return nil
	}
	request := RoutingKeyRegistrationRequest{
		LocalRoutingKeyIdentifier: 77,
		RoutingKey:                testRoutingKey(10, 100, 3),
	}
	routingKey, err := routingKeyParameter(request)
	if err != nil {
		t.Fatalf("routingKeyParameter: %v", err)
	}
	if err := association.handleRegistrationRequest(messages.NewRegistrationRequest(routingKey)); err != nil {
		t.Fatalf("handleRegistrationRequest: %v", err)
	}
	response := registrationResponse()
	if response == nil {
		t.Fatalf("written messages = %T, want Registration Response", snapshotWritten())
	}
	result, err := response.RegistrationResults[0].RegistrationResult()
	if err != nil {
		t.Fatalf("RegistrationResult: %v", err)
	}
	routingContext := result.RoutingContext.RoutingContext()
	if result.RegistrationStatus.RegistrationStatus() != uint32(RegistrationSuccessfullyRegistered) || routingContext == 0 {
		t.Fatalf("Registration Result = %+v", result)
	}
	key := ASKey{NetworkAppearance: 10, NetworkAppearanceSet: true, RoutingContext: routingContext, RoutingContextSet: true}
	if dynamic, ok := association.dynamicASKey(routingContext, false); !ok || dynamic != key {
		t.Fatalf("dynamic AS key = %+v, %v; want %+v", dynamic, ok, key)
	}
	applicationServer, ok := endpoint.as.lookup(key)
	if !ok || applicationServer.State() != ASInactive {
		t.Fatalf("dynamic Application Server = %+v, %v; want AS-INACTIVE", applicationServer, ok)
	}

	writtenMu.Lock()
	written = nil
	writtenMu.Unlock()
	if err := association.handleDeregistrationRequest(messages.NewDeregistrationRequest(
		result.RoutingContext.Copy(),
	)); err != nil {
		t.Fatalf("handleDeregistrationRequest: %v", err)
	}
	deregistration := deregistrationResponse()
	if deregistration == nil {
		t.Fatalf("written messages = %T, want Deregistration Response", snapshotWritten())
	}
	deregistrationResult, err := deregistration.DeregistrationResults[0].DeregistrationResult()
	if err != nil {
		t.Fatalf("DeregistrationResult: %v", err)
	}
	if got := deregistrationResult.DeregistrationStatus.DeregistrationStatus(); got != uint32(DeregistrationSuccessfullyDeregistered) {
		t.Fatalf("Deregistration Status = %d, want success", got)
	}
	if _, ok := association.dynamicASKey(routingContext, false); ok {
		t.Fatal("successful Deregistration retained the dynamic Association scope")
	}
	if _, ok := endpoint.as.lookup(key); ok {
		t.Fatal("unused dynamically created Application Server was retained")
	}
}

func TestAssociationRegistrationAndDeregistrationAPI(t *testing.T) {
	sgpEndpoint, err := NewEndpoint(EndpointConfig{
		Role: RoleSGP,
		RoutingKeyManagement: &RoutingKeyManagementConfig{
			AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
				return RegistrationSuccessfullyRegistered
			},
			AllowDynamicRoutingKeys: true,
			RemoveUnusedRoutingKeys: true,
		},
	})
	if err != nil {
		t.Fatalf("NewEndpoint(SGP): %v", err)
	}
	defer func() { _ = sgpEndpoint.Close() }()
	aspEndpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP})
	if err != nil {
		t.Fatalf("NewEndpoint(ASP): %v", err)
	}
	defer func() { _ = aspEndpoint.Close() }()

	asp := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	asp.endpoint = aspEndpoint
	sgp := newAssociation(RoleSGP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	sgp.endpoint = sgpEndpoint
	sgp.as = sgpEndpoint.as
	for _, association := range []*Association{asp, sgp} {
		association.muState.Lock()
		association.state = StateASPInactive
		association.muState.Unlock()
	}
	asp.signalWriter = func(message messages.M3UA) (int, error) {
		switch request := message.(type) {
		case *messages.RegistrationRequest:
			return request.MarshalLen(), sgp.handleRegistrationRequest(request)
		case *messages.DeregistrationRequest:
			return request.MarshalLen(), sgp.handleDeregistrationRequest(request)
		default:
			return 0, errors.New("unexpected ASP message")
		}
	}
	sgp.signalWriter = func(message messages.M3UA) (int, error) {
		switch response := message.(type) {
		case *messages.RegistrationResponse:
			return response.MarshalLen(), asp.handleRegistrationResponse(response)
		case *messages.DeregistrationResponse:
			return response.MarshalLen(), asp.handleDeregistrationResponse(response)
		case *messages.Notify:
			return response.MarshalLen(), nil
		default:
			return 0, errors.New("unexpected SGP message")
		}
	}

	registrations, err := asp.RegisterRoutingKeys(context.Background(), RoutingKeyRegistration{
		RoutingKey: testRoutingKey(10, 100, 3),
	})
	if err != nil {
		t.Fatalf("RegisterRoutingKeys: %v", err)
	}
	if len(registrations) != 1 || registrations[0].Status != RegistrationSuccessfullyRegistered || registrations[0].RoutingContext == 0 {
		t.Fatalf("Registration results = %+v", registrations)
	}
	routingContext := registrations[0].RoutingContext
	if configured := asp.configuredRoutingContexts(); len(configured) != 1 || configured[0] != routingContext {
		t.Fatalf("ASP configured Routing Contexts = %v, want [%d]", configured, routingContext)
	}

	deregistrations, err := asp.DeregisterRoutingContexts(context.Background(), routingContext)
	if err != nil {
		t.Fatalf("DeregisterRoutingContexts: %v", err)
	}
	if len(deregistrations) != 1 || deregistrations[0].Status != DeregistrationSuccessfullyDeregistered {
		t.Fatalf("Deregistration results = %+v", deregistrations)
	}
	if configured := asp.configuredRoutingContexts(); len(configured) != 0 {
		t.Fatalf("ASP configured Routing Contexts after Deregistration = %v", configured)
	}
}

func TestRKMRoleAndDisabledPolicyHandling(t *testing.T) {
	request, err := routingKeyParameter(RoutingKeyRegistrationRequest{
		LocalRoutingKeyIdentifier: 1,
		RoutingKey:                testRoutingKey(10, 100, 3),
	})
	if err != nil {
		t.Fatalf("routingKeyParameter: %v", err)
	}
	message := messages.NewRegistrationRequest(request)

	asp := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	if err := asp.handleRegistrationRequest(message); err == nil {
		t.Fatal("ASP accepted a Registration Request")
	} else {
		var unexpected *UnexpectedMessageError
		if !errors.As(err, &unexpected) {
			t.Fatalf("ASP Registration Request error = %v, want UnexpectedMessageError", err)
		}
	}

	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	defer func() { _ = endpoint.Close() }()
	sgp := newAssociation(RoleSGP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	sgp.endpoint = endpoint
	if err := sgp.handleRegistrationRequest(message); err == nil {
		t.Fatal("SGP without RKM policy accepted a Registration Request")
	} else {
		var unsupported *UnsupportedClassError
		if !errors.As(err, &unsupported) {
			t.Fatalf("disabled RKM error = %v, want UnsupportedClassError", err)
		}
	}
}

func TestRKMResponderRejectsRequestsBeforeASPUpCompletes(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{
		Role: RoleSGP,
		RoutingKeyManagement: &RoutingKeyManagementConfig{
			AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
				return RegistrationSuccessfullyRegistered
			},
			AllowDynamicRoutingKeys: true,
		},
	})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	defer func() { _ = endpoint.Close() }()
	association := newAssociation(RoleSGP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	association.endpoint = endpoint
	association.signalWriter = func(message messages.M3UA) (int, error) {
		t.Fatalf("wrote %T before ASP Up completed", message)
		return 0, nil
	}

	requests := []struct {
		name   string
		handle func() error
	}{
		{name: "Registration Request", handle: func() error {
			return association.handleRegistrationRequest(registrationRequestMessage(t))
		}},
		{name: "Deregistration Request", handle: func() error {
			return association.handleDeregistrationRequest(deregistrationRequestMessage())
		}},
	}
	for _, request := range requests {
		t.Run(request.name, func(t *testing.T) {
			var unexpected *UnexpectedMessageError
			if err := request.handle(); !errors.As(err, &unexpected) {
				t.Fatalf("request error = %v, want UnexpectedMessageError", err)
			}
		})
	}
}

func TestRoutingKeyRegistrationAuthorizerReceivesRemotePeerRole(t *testing.T) {
	tests := []struct {
		name     string
		local    Role
		wantPeer Role
	}{
		{name: "SGP authorizes ASP", local: RoleSGP, wantPeer: RoleASP},
		{name: "IPSP authorizes IPSP", local: RoleIPSP, wantPeer: RoleIPSP},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var gotPeer RoutingKeyPeer
			endpoint, err := NewEndpoint(EndpointConfig{
				Role: test.local,
				RoutingKeyManagement: &RoutingKeyManagementConfig{
					AuthorizeRegistration: func(request RoutingKeyRegistrationRequest) RegistrationStatus {
						gotPeer = request.Peer
						return RegistrationSuccessfullyRegistered
					},
					AllowDynamicRoutingKeys: true,
				},
			})
			if err != nil {
				t.Fatalf("NewEndpoint: %v", err)
			}
			defer func() { _ = endpoint.Close() }()

			association := newAssociation(test.local, NewAssociationConfig(0, 0, 0, 0, 0, 0))
			association.endpoint = endpoint
			association.muState.Lock()
			association.state = StateASPInactive
			association.muState.Unlock()
			association.signalWriter = func(message messages.M3UA) (int, error) {
				return message.MarshalLen(), nil
			}
			if err := association.handleRegistrationRequest(registrationRequestMessage(t)); err != nil {
				t.Fatalf("handleRegistrationRequest: %v", err)
			}
			if gotPeer.Role != test.wantPeer {
				t.Fatalf("authorizer peer Role = %s, want %s", gotPeer.Role, test.wantPeer)
			}
		})
	}
}

func TestRKMResponderAppliesImpliedAssociationNetworkAppearance(t *testing.T) {
	var authorized RoutingKeyRegistrationRequest
	endpoint, err := NewEndpoint(EndpointConfig{
		Role: RoleSGP,
		RoutingKeyManagement: &RoutingKeyManagementConfig{
			AuthorizeRegistration: func(request RoutingKeyRegistrationRequest) RegistrationStatus {
				authorized = request
				return RegistrationSuccessfullyRegistered
			},
			AllowDynamicRoutingKeys: true,
		},
	})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	defer func() { _ = endpoint.Close() }()
	config := NewAssociationConfig(0, 0, 0, 0, 0, 0)
	config.NetworkAppearance = params.NewNetworkAppearance(10)
	association := newAssociation(RoleSGP, config)
	association.endpoint = endpoint
	association.as = endpoint.as
	association.muState.Lock()
	association.state = StateASPInactive
	association.muState.Unlock()
	association.signalWriter = func(message messages.M3UA) (int, error) {
		return message.MarshalLen(), nil
	}

	routingKey := testRoutingKey(0, 100, params.ServiceIndSCCP)
	routingKey.NetworkAppearanceSet = false
	parameter, err := routingKeyParameter(RoutingKeyRegistrationRequest{
		LocalRoutingKeyIdentifier: 1,
		RoutingKey:                routingKey,
	})
	if err != nil {
		t.Fatalf("routingKeyParameter: %v", err)
	}
	payload, err := parameter.RoutingKey()
	if err != nil {
		t.Fatalf("RoutingKey: %v", err)
	}
	if payload.NetworkAppearance != nil {
		t.Fatal("wire Routing Key unexpectedly contains Network Appearance")
	}
	if err := association.handleRegistrationRequest(messages.NewRegistrationRequest(parameter)); err != nil {
		t.Fatalf("handleRegistrationRequest: %v", err)
	}
	if !authorized.NetworkAppearanceImplied || !authorized.RoutingKey.NetworkAppearanceSet || authorized.RoutingKey.NetworkAppearance != 10 {
		t.Fatalf("authorization request = %+v, want implied Network Appearance 10", authorized)
	}
	contexts := association.dynamicRoutingContexts(false)
	if len(contexts) != 1 {
		t.Fatalf("dynamic Routing Contexts = %v, want one", contexts)
	}
	key, ok := association.dynamicASKey(contexts[0], false)
	if !ok || !key.NetworkAppearanceSet || key.NetworkAppearance != 10 {
		t.Fatalf("dynamic ASKey = %+v, %t, want Network Appearance 10", key, ok)
	}
}

func TestRKMRequesterUsesImpliedNetworkAppearanceWithoutAddingItToRequest(t *testing.T) {
	config := NewAssociationConfig(0, 0, 0, 0, 0, 0)
	config.NetworkAppearance = params.NewNetworkAppearance(20)
	association := newAssociation(RoleASP, config)
	association.muState.Lock()
	association.state = StateASPInactive
	association.muState.Unlock()
	association.signalWriter = func(message messages.M3UA) (int, error) {
		request := message.(*messages.RegistrationRequest)
		payload, err := request.RoutingKeys[0].RoutingKey()
		if err != nil {
			return 0, err
		}
		if payload.NetworkAppearance != nil {
			return 0, errors.New("Registration Request unexpectedly added Network Appearance")
		}
		return message.MarshalLen(), association.handleRegistrationResponse(messages.NewRegistrationResponse(
			params.NewRegistrationResult(params.NewRegistrationResultPayload(
				payload.LocalRoutingKeyIdentifier.Copy(),
				params.NewRegistrationStatus(params.SuccessfullyRegistered),
				params.NewRoutingContext(9),
			)),
		))
	}
	routingKey := testRoutingKey(0, 100, params.ServiceIndSCCP)
	routingKey.NetworkAppearanceSet = false
	if _, err := association.RegisterRoutingKeys(context.Background(), RoutingKeyRegistration{RoutingKey: routingKey}); err != nil {
		t.Fatalf("RegisterRoutingKeys: %v", err)
	}
	key, ok := association.dynamicASKey(9, false)
	if !ok || !key.NetworkAppearanceSet || key.NetworkAppearance != 20 {
		t.Fatalf("dynamic ASKey = %+v, %t, want implied Network Appearance 20", key, ok)
	}
}

func TestRKMAllNetworkAppearancesAcceptsExplicitDataAppearance(t *testing.T) {
	association := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	association.addDynamicASKey(ASKey{RoutingContext: 9, RoutingContextSet: true}, RoutingKey{
		Groups: []RoutingKeyGroup{{DestinationPointCode: 100}},
	}, false)
	association.muState.Lock()
	association.state = StateASPActive
	association.muState.Unlock()
	association.signalWriter = func(message messages.M3UA) (int, error) {
		return message.MarshalLen(), nil
	}
	if err := association.validateDataNetworkAppearance(params.NewNetworkAppearance(33), params.NewRoutingContext(9)); err != nil {
		t.Fatalf("inbound explicit Network Appearance for all-appearance Routing Key: %v", err)
	}
	data := messages.NewData(
		params.NewNetworkAppearance(33),
		params.NewRoutingContext(9),
		params.NewProtocolData(1, 100, params.ServiceIndSCCP, 0, 0, 1, []byte("payload")),
		nil,
	)
	if _, err := association.WriteSignal(data); err != nil {
		t.Fatalf("outbound explicit Network Appearance for all-appearance Routing Key: %v", err)
	}
}

func TestRegisteredRoutingKeyTrafficModeOverridesAssociationDefault(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{
		Role: RoleSGP,
		RoutingKeyManagement: &RoutingKeyManagementConfig{
			AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
				return RegistrationSuccessfullyRegistered
			},
			AllowDynamicRoutingKeys: true,
		},
	})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	defer func() { _ = endpoint.Close() }()

	config := NewAssociationConfig(0, 0, 0, 0, 0, 0)
	config.TrafficModeType = params.NewTrafficModeType(params.TrafficModeBroadcast)
	association := newAssociation(RoleSGP, config)
	association.endpoint = endpoint
	association.as = endpoint.as
	association.muState.Lock()
	association.state = StateASPInactive
	association.muState.Unlock()
	association.signalWriter = func(message messages.M3UA) (int, error) {
		return message.MarshalLen(), nil
	}
	routingKey := testRoutingKey(10, 100, 3)
	routingKey.TrafficMode = params.TrafficModeLoadshare
	routingKey.TrafficModeSet = true
	parameter, err := routingKeyParameter(RoutingKeyRegistrationRequest{
		LocalRoutingKeyIdentifier: 1,
		RoutingKey:                routingKey,
	})
	if err != nil {
		t.Fatalf("routingKeyParameter: %v", err)
	}
	if err := association.handleRegistrationRequest(messages.NewRegistrationRequest(parameter)); err != nil {
		t.Fatalf("handleRegistrationRequest: %v", err)
	}
	routingContexts := association.dynamicRoutingContexts(false)
	if len(routingContexts) != 1 {
		t.Fatalf("dynamic Routing Contexts = %v, want one", routingContexts)
	}

	agreed, err := endpoint.as.agreeTrafficModeForAssociation(
		association,
		routingContexts,
		params.NewTrafficModeType(params.TrafficModeLoadshare),
	)
	if err != nil {
		t.Fatalf("agreeTrafficModeForAssociation: %v", err)
	}
	if agreed == nil || agreed.TrafficModeType() != params.TrafficModeLoadshare {
		t.Fatalf("agreed Traffic Mode = %v, want Loadshare", agreed)
	}
}

func TestRequestedTrafficModeAppliesToProvisionedRoutingKeyWithoutConfiguredMode(t *testing.T) {
	provisioned := testRoutingKey(10, 100, 3)
	endpoint, err := NewEndpoint(EndpointConfig{
		Role: RoleSGP,
		RoutingKeyManagement: &RoutingKeyManagementConfig{
			AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
				return RegistrationSuccessfullyRegistered
			},
			ProvisionedRoutingKeys: []ProvisionedRoutingKey{{RoutingContext: 7, RoutingKey: provisioned}},
		},
	})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	defer func() { _ = endpoint.Close() }()
	config := NewAssociationConfig(0, 0, 0, 0, 0, 0)
	config.TrafficModeType = params.NewTrafficModeType(params.TrafficModeBroadcast)
	association := newAssociation(RoleSGP, config)
	association.endpoint = endpoint
	association.as = endpoint.as
	association.muState.Lock()
	association.state = StateASPInactive
	association.muState.Unlock()
	association.signalWriter = func(message messages.M3UA) (int, error) {
		return message.MarshalLen(), nil
	}
	requested := snapshotRoutingKey(provisioned)
	requested.TrafficMode = params.TrafficModeLoadshare
	requested.TrafficModeSet = true
	parameter, err := routingKeyParameter(RoutingKeyRegistrationRequest{
		LocalRoutingKeyIdentifier: 1,
		RoutingKey:                requested,
	})
	if err != nil {
		t.Fatalf("routingKeyParameter: %v", err)
	}
	if err := association.handleRegistrationRequest(messages.NewRegistrationRequest(parameter)); err != nil {
		t.Fatalf("handleRegistrationRequest: %v", err)
	}
	if _, err := endpoint.as.agreeTrafficModeForAssociation(
		association,
		[]uint32{7},
		params.NewTrafficModeType(params.TrafficModeLoadshare),
	); err != nil {
		t.Fatalf("agreeTrafficModeForAssociation: %v", err)
	}
}

func TestRKMRequesterAlreadyCanceledContextSendsNothing(t *testing.T) {
	association := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	association.muState.Lock()
	association.state = StateASPInactive
	association.muState.Unlock()
	writes := 0
	association.signalWriter = func(message messages.M3UA) (int, error) {
		writes++
		return message.MarshalLen(), nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := association.RegisterRoutingKeys(ctx, RoutingKeyRegistration{
		RoutingKey: testRoutingKey(10, 100, 3),
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("RegisterRoutingKeys error = %v, want context.Canceled", err)
	}
	if _, err := association.DeregisterRoutingContexts(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("DeregisterRoutingContexts error = %v, want context.Canceled", err)
	}
	if writes != 0 {
		t.Fatalf("writes = %d, want 0 for an already-canceled context", writes)
	}
}

func TestRKMRequesterCollectsSplitResponses(t *testing.T) {
	association := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	association.muState.Lock()
	association.state = StateASPInactive
	association.muState.Unlock()
	association.signalWriter = func(message messages.M3UA) (int, error) {
		switch request := message.(type) {
		case *messages.RegistrationRequest:
			for index, routingKey := range request.RoutingKeys {
				payload, err := routingKey.RoutingKey()
				if err != nil {
					return 0, err
				}
				response := messages.NewRegistrationResponse(params.NewRegistrationResult(
					params.NewRegistrationResultPayload(
						payload.LocalRoutingKeyIdentifier.Copy(),
						params.NewRegistrationStatus(params.SuccessfullyRegistered),
						params.NewRoutingContext(uint32(index+10)),
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
		default:
			return 0, errors.New("unexpected RKM message")
		}
		return message.MarshalLen(), nil
	}

	registrations, err := association.RegisterRoutingKeys(
		context.Background(),
		RoutingKeyRegistration{RoutingKey: testRoutingKey(10, 100, 3)},
		RoutingKeyRegistration{RoutingKey: testRoutingKey(20, 200, 5)},
	)
	if err != nil {
		t.Fatalf("RegisterRoutingKeys: %v", err)
	}
	if len(registrations) != 2 || registrations[0].RoutingContext != 10 || registrations[1].RoutingContext != 11 {
		t.Fatalf("Registration results = %+v, want Routing Contexts 10 and 11", registrations)
	}

	deregistrations, err := association.DeregisterRoutingContexts(context.Background(), 10, 11)
	if err != nil {
		t.Fatalf("DeregisterRoutingContexts: %v", err)
	}
	if len(deregistrations) != 2 ||
		deregistrations[0].Status != DeregistrationSuccessfullyDeregistered ||
		deregistrations[1].Status != DeregistrationSuccessfullyDeregistered {
		t.Fatalf("Deregistration results = %+v, want two successes", deregistrations)
	}
}

func TestRKMRequesterSerializesConcurrentProcedures(t *testing.T) {
	association := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	association.muState.Lock()
	association.state = StateASPInactive
	association.muState.Unlock()
	writes := make(chan uint32, 2)
	release := make(chan struct{}, 2)
	association.signalWriter = func(message messages.M3UA) (int, error) {
		request := message.(*messages.RegistrationRequest)
		payload, err := request.RoutingKeys[0].RoutingKey()
		if err != nil {
			return 0, err
		}
		identifier := payload.LocalRoutingKeyIdentifier.LocalRoutingKeyIdentifier()
		writes <- identifier
		<-release
		response := messages.NewRegistrationResponse(params.NewRegistrationResult(
			params.NewRegistrationResultPayload(
				params.NewLocalRoutingKeyIdentifier(identifier),
				params.NewRegistrationStatus(params.SuccessfullyRegistered),
				params.NewRoutingContext(identifier+100),
			),
		))
		return message.MarshalLen(), association.handleRegistrationResponse(response)
	}

	type answer struct {
		results []RoutingKeyRegistrationResult
		err     error
	}
	firstDone := make(chan answer, 1)
	secondDone := make(chan answer, 1)
	go func() {
		results, err := association.RegisterRoutingKeys(context.Background(), RoutingKeyRegistration{
			RoutingKey: testRoutingKey(10, 100, params.ServiceIndSCCP),
		})
		firstDone <- answer{results: results, err: err}
	}()
	firstIdentifier := <-writes
	go func() {
		results, err := association.RegisterRoutingKeys(context.Background(), RoutingKeyRegistration{
			RoutingKey: testRoutingKey(20, 200, params.ServiceIndISUP),
		})
		secondDone <- answer{results: results, err: err}
	}()
	select {
	case identifier := <-writes:
		t.Fatalf("second RKM procedure wrote Local RK Identifier %d before the first completed", identifier)
	case <-time.After(50 * time.Millisecond):
	}
	release <- struct{}{}
	first := <-firstDone
	if first.err != nil || len(first.results) != 1 || first.results[0].RoutingContext != firstIdentifier+100 {
		t.Fatalf("first RKM result = %+v, error = %v", first.results, first.err)
	}
	secondIdentifier := <-writes
	if secondIdentifier == firstIdentifier {
		t.Fatalf("concurrent RKM procedures reused Local RK Identifier %d", secondIdentifier)
	}
	release <- struct{}{}
	second := <-secondDone
	if second.err != nil || len(second.results) != 1 || second.results[0].RoutingContext != secondIdentifier+100 {
		t.Fatalf("second RKM result = %+v, error = %v", second.results, second.err)
	}
}

func TestRKMRequesterRejectsUnexpectedResultsWithoutPartialScopeMutation(t *testing.T) {
	t.Run("Registration", func(t *testing.T) {
		association := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
		association.muState.Lock()
		association.state = StateASPInactive
		association.muState.Unlock()
		association.signalWriter = func(message messages.M3UA) (int, error) {
			request := message.(*messages.RegistrationRequest)
			first, err := request.RoutingKeys[0].RoutingKey()
			if err != nil {
				return 0, err
			}
			response := messages.NewRegistrationResponse(
				params.NewRegistrationResult(params.NewRegistrationResultPayload(
					first.LocalRoutingKeyIdentifier.Copy(),
					params.NewRegistrationStatus(params.SuccessfullyRegistered),
					params.NewRoutingContext(10),
				)),
				params.NewRegistrationResult(params.NewRegistrationResultPayload(
					params.NewLocalRoutingKeyIdentifier(999),
					params.NewRegistrationStatus(params.SuccessfullyRegistered),
					params.NewRoutingContext(11),
				)),
			)
			return message.MarshalLen(), association.handleRegistrationResponse(response)
		}
		_, err := association.RegisterRoutingKeys(
			context.Background(),
			RoutingKeyRegistration{RoutingKey: testRoutingKey(10, 100, 3)},
			RoutingKeyRegistration{RoutingKey: testRoutingKey(20, 200, 5)},
		)
		if err == nil {
			t.Fatal("RegisterRoutingKeys error = nil, want unexpected result error")
		}
		if contexts := association.dynamicRoutingContexts(false); len(contexts) != 0 {
			t.Fatalf("failed Registration mutated dynamic Routing Contexts: %v", contexts)
		}
	})

	t.Run("Deregistration", func(t *testing.T) {
		association := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
		association.muState.Lock()
		association.state = StateASPInactive
		association.muState.Unlock()
		for _, routingContext := range []uint32{10, 11} {
			routingKey := testRoutingKey(routingContext, routingContext+100, 3)
			association.addDynamicASKey(ASKey{
				NetworkAppearance:    routingContext,
				NetworkAppearanceSet: true,
				RoutingContext:       routingContext,
				RoutingContextSet:    true,
			}, routingKey, false)
		}
		association.signalWriter = func(message messages.M3UA) (int, error) {
			response := messages.NewDeregistrationResponse(
				params.NewDeregistrationResult(params.NewDeregResultPayload(
					params.NewRoutingContext(10),
					params.NewDeregistrationStatus(params.SuccessfullyDeregistered),
				)),
				params.NewDeregistrationResult(params.NewDeregResultPayload(
					params.NewRoutingContext(999),
					params.NewDeregistrationStatus(params.SuccessfullyDeregistered),
				)),
			)
			return message.MarshalLen(), association.handleDeregistrationResponse(response)
		}
		if _, err := association.DeregisterRoutingContexts(context.Background(), 10, 11); err == nil {
			t.Fatal("DeregisterRoutingContexts error = nil, want unexpected result error")
		}
		if contexts := association.dynamicRoutingContexts(false); len(contexts) != 2 {
			t.Fatalf("failed Deregistration mutated dynamic Routing Contexts: %v", contexts)
		}
	})
}

func TestRKMRequesterWaitStopsOnContextAndAssociationClose(t *testing.T) {
	newWaitingAssociation := func(t *testing.T) (*Association, <-chan struct{}) {
		t.Helper()
		association := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
		association.muState.Lock()
		association.state = StateASPInactive
		association.muState.Unlock()
		written := make(chan struct{}, 1)
		association.signalWriter = func(message messages.M3UA) (int, error) {
			written <- struct{}{}
			return message.MarshalLen(), nil
		}
		return association, written
	}

	t.Run("context cancellation", func(t *testing.T) {
		association, written := newWaitingAssociation(t)
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			_, err := association.RegisterRoutingKeys(ctx, RoutingKeyRegistration{
				RoutingKey: testRoutingKey(10, 100, 3),
			})
			result <- err
		}()
		<-written
		cancel()
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("RegisterRoutingKeys error = %v, want context.Canceled", err)
		}
	})

	t.Run("association close", func(t *testing.T) {
		association, written := newWaitingAssociation(t)
		result := make(chan error, 1)
		go func() {
			_, err := association.DeregisterRoutingContexts(context.Background(), 1)
			result <- err
		}()
		<-written
		if err := association.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if err := <-result; !errors.Is(err, ErrAssociationClosed) {
			t.Fatalf("DeregisterRoutingContexts error = %v, want ErrAssociationClosed", err)
		}
	})
}

func TestDynamicRoutingKeyNetworkAppearanceScopesInboundSSNM(t *testing.T) {
	association := newAssociation(RoleSGP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	association.cfg.NetworkAppearance = params.NewNetworkAppearance(7)
	routingKey := testRoutingKey(10, 100, 3)
	association.addDynamicASKey(ASKey{
		NetworkAppearance:    10,
		NetworkAppearanceSet: true,
		RoutingContext:       9,
		RoutingContextSet:    true,
	}, routingKey, false)

	if err := association.validateSSNMNetworkAppearance(
		params.NewNetworkAppearance(10), params.NewRoutingContext(9),
	); err != nil {
		t.Fatalf("matching dynamic Network Appearance error = %v", err)
	}
	if err := association.validateSSNMNetworkAppearance(
		params.NewNetworkAppearance(7), params.NewRoutingContext(9),
	); !errors.Is(err, ErrInvalidNetworkAppearance) {
		t.Fatalf("static Network Appearance for dynamic Routing Context error = %v, want ErrInvalidNetworkAppearance", err)
	}
}

func TestIPSPDoubleExchangeRKMScopesDirectionsIndependently(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{
		Role: RoleIPSP,
		RoutingKeyManagement: &RoutingKeyManagementConfig{
			AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
				return RegistrationSuccessfullyRegistered
			},
			AllowDynamicRoutingKeys: true,
		},
	})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	defer func() { _ = endpoint.Close() }()

	inbound := newAssociation(RoleIPSP, newDoubleExchangeAssociationConfigForTest())
	inbound.endpoint = endpoint
	inbound.as = endpoint.as
	inbound.muState.Lock()
	inbound.state = StateASPInactive
	inbound.muState.Unlock()
	inbound.signalWriter = func(message messages.M3UA) (int, error) {
		return message.MarshalLen(), nil
	}
	request := RoutingKeyRegistrationRequest{
		LocalRoutingKeyIdentifier: 1,
		RoutingKey:                testRoutingKey(10, 100, 3),
	}
	parameter, err := routingKeyParameter(request)
	if err != nil {
		t.Fatalf("routingKeyParameter: %v", err)
	}
	if err := inbound.handleRegistrationRequest(messages.NewRegistrationRequest(parameter)); err != nil {
		t.Fatalf("handleRegistrationRequest: %v", err)
	}
	peerContexts := inbound.dynamicRoutingContexts(false)
	if len(peerContexts) != 1 {
		t.Fatalf("TrafficToPeer dynamic Routing Contexts = %v, want one", peerContexts)
	}
	if localContexts := inbound.dynamicRoutingContexts(true); len(localContexts) != 0 {
		t.Fatalf("inbound Registration changed TrafficToLocal contexts: %v", localContexts)
	}

	local := newAssociation(RoleIPSP, newDoubleExchangeAssociationConfigForTest())
	local.endpoint = endpoint
	local.setIPSPState(IPSPState{TrafficToLocal: StateASPInactive, TrafficToPeer: StateASPDown})
	local.signalWriter = func(message messages.M3UA) (int, error) {
		switch request := message.(type) {
		case *messages.RegistrationRequest:
			payload, err := request.RoutingKeys[0].RoutingKey()
			if err != nil {
				return 0, err
			}
			response := messages.NewRegistrationResponse(params.NewRegistrationResult(
				params.NewRegistrationResultPayload(
					payload.LocalRoutingKeyIdentifier.Copy(),
					params.NewRegistrationStatus(params.SuccessfullyRegistered),
					params.NewRoutingContext(99),
				),
			))
			return message.MarshalLen(), local.handleRegistrationResponse(response)
		case *messages.DeregistrationRequest:
			response := messages.NewDeregistrationResponse(params.NewDeregistrationResult(
				params.NewDeregResultPayload(
					request.RoutingContext.Copy(),
					params.NewDeregistrationStatus(params.SuccessfullyDeregistered),
				),
			))
			return message.MarshalLen(), local.handleDeregistrationResponse(response)
		default:
			return 0, errors.New("unexpected RKM message")
		}
	}
	if _, err := local.RegisterRoutingKeys(context.Background(), RoutingKeyRegistration{
		RoutingKey: testRoutingKey(20, 200, 5),
	}); err != nil {
		t.Fatalf("RegisterRoutingKeys: %v", err)
	}
	if localContexts := local.dynamicRoutingContexts(true); len(localContexts) != 1 || localContexts[0] != 99 {
		t.Fatalf("TrafficToLocal dynamic Routing Contexts = %v, want [99]", localContexts)
	}
	if peerContexts := local.dynamicRoutingContexts(false); len(peerContexts) != 0 {
		t.Fatalf("local Registration changed TrafficToPeer contexts: %v", peerContexts)
	}
	if _, err := local.DeregisterRoutingContexts(context.Background(), 99); err != nil {
		t.Fatalf("DeregisterRoutingContexts: %v", err)
	}
	if localContexts := local.dynamicRoutingContexts(true); len(localContexts) != 0 {
		t.Fatalf("local Deregistration retained TrafficToLocal contexts: %v", localContexts)
	}
}

func TestIPSPSingleExchangeRKMUsesSharedRoutingKeyScope(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{
		Role: RoleIPSP,
		RoutingKeyManagement: &RoutingKeyManagementConfig{
			AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
				return RegistrationSuccessfullyRegistered
			},
			AllowDynamicRoutingKeys: true,
		},
	})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	defer func() { _ = endpoint.Close() }()
	config := NewAssociationConfig(0, 0, 0, 0, 0, 0)
	config.IPSP = &IPSPConfig{ExchangeModel: IPSPExchangeSingle}
	association := newAssociation(RoleIPSP, config)
	association.endpoint = endpoint
	association.as = endpoint.as
	association.muState.Lock()
	association.state = StateASPInactive
	association.muState.Unlock()
	association.signalWriter = func(message messages.M3UA) (int, error) {
		switch request := message.(type) {
		case *messages.RegistrationResponse:
			return request.MarshalLen(), nil
		case *messages.Notify:
			return request.MarshalLen(), nil
		case *messages.RegistrationRequest:
			payload, err := request.RoutingKeys[0].RoutingKey()
			if err != nil {
				return 0, err
			}
			return request.MarshalLen(), association.handleRegistrationResponse(
				messages.NewRegistrationResponse(params.NewRegistrationResult(
					params.NewRegistrationResultPayload(
						payload.LocalRoutingKeyIdentifier.Copy(),
						params.NewRegistrationStatus(params.SuccessfullyRegistered),
						params.NewRoutingContext(20),
					),
				)),
			)
		default:
			return 0, errors.New("unexpected RKM message")
		}
	}

	inboundParameter, err := routingKeyParameter(RoutingKeyRegistrationRequest{
		LocalRoutingKeyIdentifier: 1,
		RoutingKey:                testRoutingKey(10, 100, 3),
	})
	if err != nil {
		t.Fatalf("routingKeyParameter: %v", err)
	}
	if err := association.handleRegistrationRequest(messages.NewRegistrationRequest(inboundParameter)); err != nil {
		t.Fatalf("inbound Registration: %v", err)
	}
	if _, err := association.RegisterRoutingKeys(context.Background(), RoutingKeyRegistration{
		RoutingKey: testRoutingKey(20, 200, 5),
	}); err != nil {
		t.Fatalf("local Registration: %v", err)
	}
	contexts := association.dynamicRoutingContexts(false)
	if len(contexts) != 2 || contexts[0] == contexts[1] {
		t.Fatalf("single-exchange shared dynamic Routing Contexts = %v, want two", contexts)
	}
	if localContexts := association.dynamicRoutingContexts(true); len(localContexts) != 0 {
		t.Fatalf("single exchange created a separate local scope: %v", localContexts)
	}
}

func TestRKMResponderReplayCompletesASMutationAfterResponseWriteFailure(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{
		Role: RoleSGP,
		RoutingKeyManagement: &RoutingKeyManagementConfig{
			AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
				return RegistrationSuccessfullyRegistered
			},
			AllowDynamicRoutingKeys: true,
			RemoveUnusedRoutingKeys: true,
		},
	})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	defer func() { _ = endpoint.Close() }()
	association := newAssociation(RoleSGP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	association.endpoint = endpoint
	association.as = endpoint.as
	association.muState.Lock()
	association.state = StateASPInactive
	association.muState.Unlock()
	writeErr := errors.New("response write failed")
	var writerMu sync.Mutex
	failResponseWrite := true
	var lastResponse messages.M3UA
	association.signalWriter = func(message messages.M3UA) (int, error) {
		switch message.(type) {
		case *messages.RegistrationResponse, *messages.DeregistrationResponse:
		default:
			return message.MarshalLen(), nil
		}
		writerMu.Lock()
		lastResponse = message
		fail := failResponseWrite
		writerMu.Unlock()
		if fail {
			return 0, writeErr
		}
		return message.MarshalLen(), nil
	}
	setFailResponseWrite := func(fail bool) {
		writerMu.Lock()
		failResponseWrite = fail
		writerMu.Unlock()
	}
	responseSnapshot := func() messages.M3UA {
		writerMu.Lock()
		defer writerMu.Unlock()
		return lastResponse
	}

	registration := registrationRequestMessage(t)
	if err := association.handleRegistrationRequest(registration); !errors.Is(err, writeErr) {
		t.Fatalf("first Registration error = %v, want write failure", err)
	}
	registrationResponse := responseSnapshot().(*messages.RegistrationResponse)
	registrationResult, err := registrationResponse.RegistrationResults[0].RegistrationResult()
	if err != nil {
		t.Fatalf("RegistrationResult: %v", err)
	}
	routingContext := registrationResult.RoutingContext.RoutingContext()
	key := ASKey{NetworkAppearance: 10, NetworkAppearanceSet: true, RoutingContext: routingContext, RoutingContextSet: true}
	if _, ok := endpoint.as.lookup(key); ok {
		t.Fatal("Application Server became visible before REG RSP was written")
	}
	if _, ok := association.dynamicASKey(routingContext, false); ok {
		t.Fatal("Association Routing Key scope changed before REG RSP was written")
	}
	setFailResponseWrite(false)
	if err := association.handleRegistrationRequest(registration); err != nil {
		t.Fatalf("Registration replay: %v", err)
	}
	if _, ok := endpoint.as.lookup(key); !ok {
		t.Fatal("Registration replay did not complete Application Server mutation")
	}
	if _, ok := association.dynamicASKey(routingContext, false); !ok {
		t.Fatal("Registration replay did not complete Association Routing Key scope")
	}

	deregistration := messages.NewDeregistrationRequest(params.NewRoutingContext(routingContext))
	setFailResponseWrite(true)
	if err := association.handleDeregistrationRequest(deregistration); !errors.Is(err, writeErr) {
		t.Fatalf("first Deregistration error = %v, want write failure", err)
	}
	if _, ok := endpoint.as.lookup(key); !ok {
		t.Fatal("Application Server was removed before DEREG RSP was written")
	}
	if _, ok := association.dynamicASKey(routingContext, false); !ok {
		t.Fatal("Association Routing Key scope was removed before DEREG RSP was written")
	}
	setFailResponseWrite(false)
	if err := association.handleDeregistrationRequest(deregistration); err != nil {
		t.Fatalf("Deregistration replay: %v", err)
	}
	if _, ok := endpoint.as.lookup(key); ok {
		t.Fatal("Deregistration replay did not complete Application Server removal")
	}
	if _, ok := association.dynamicASKey(routingContext, false); ok {
		t.Fatal("Deregistration replay retained Association Routing Key scope")
	}
}

func TestRKMResponderRejectsDuplicateBatchCorrelationValues(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{
		Role: RoleSGP,
		RoutingKeyManagement: &RoutingKeyManagementConfig{
			AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
				return RegistrationSuccessfullyRegistered
			},
			AllowDynamicRoutingKeys: true,
		},
	})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	defer func() { _ = endpoint.Close() }()

	association := newAssociation(RoleSGP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	association.endpoint = endpoint
	association.muState.Lock()
	association.state = StateASPInactive
	association.muState.Unlock()
	writes := 0
	association.signalWriter = func(message messages.M3UA) (int, error) {
		writes++
		return message.MarshalLen(), nil
	}

	first, err := routingKeyParameter(RoutingKeyRegistrationRequest{
		LocalRoutingKeyIdentifier: 7,
		RoutingKey:                testRoutingKey(10, 100, 3),
	})
	if err != nil {
		t.Fatalf("first routingKeyParameter: %v", err)
	}
	second, err := routingKeyParameter(RoutingKeyRegistrationRequest{
		LocalRoutingKeyIdentifier: 7,
		RoutingKey:                testRoutingKey(20, 200, 5),
	})
	if err != nil {
		t.Fatalf("second routingKeyParameter: %v", err)
	}
	if err := association.handleRegistrationRequest(messages.NewRegistrationRequest(first, second)); !errors.Is(err, ErrInvalidParameterValue) {
		t.Fatalf("duplicate Local RK Identifier error = %v, want ErrInvalidParameterValue", err)
	}
	if writes != 0 {
		t.Fatalf("duplicate Local RK Identifier wrote %d responses, want none", writes)
	}
	if endpoint.routingKeys.dynamicCount() != 0 {
		t.Fatal("duplicate Local RK Identifier partially mutated the Routing Key registry")
	}

	if err := association.handleDeregistrationRequest(
		messages.NewDeregistrationRequest(params.NewRoutingContext(11, 11)),
	); !errors.Is(err, ErrInvalidParameterValue) {
		t.Fatalf("duplicate Deregistration Routing Context error = %v, want ErrInvalidParameterValue", err)
	}
	if writes != 0 {
		t.Fatalf("duplicate Deregistration Routing Context wrote %d responses, want none", writes)
	}
}
