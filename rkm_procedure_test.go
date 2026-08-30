package m3ua

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

type rkmInitialCheckContext struct {
	context.Context
	checked chan struct{}
	once    sync.Once
}

func newRKMInitialCheckContext() *rkmInitialCheckContext {
	return &rkmInitialCheckContext{Context: context.Background(), checked: make(chan struct{})}
}

func (c *rkmInitialCheckContext) Err() error {
	c.once.Do(func() { close(c.checked) })
	return c.Context.Err()
}

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

func TestRKMRegistrationDoesNotActivateNewApplicationServer(t *testing.T) {
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
	config.NetworkAppearance = params.NewNetworkAppearance(10)
	config.RoutingContexts = params.NewRoutingContext(5000)
	association := newAssociation(RoleSGP, config)
	association.endpoint = endpoint
	association.as = endpoint.as
	var response *messages.RegistrationResponse
	association.signalWriter = func(message messages.M3UA) (int, error) {
		if registrationResponse, ok := message.(*messages.RegistrationResponse); ok {
			response = registrationResponse
		}
		return message.MarshalLen(), nil
	}
	association.muState.Lock()
	association.state = StateASPActive
	association.muState.Unlock()
	endpoint.as.aspStateChanged(association, StateASPActive)

	request := func(identifier uint32) *messages.RegistrationRequest {
		parameter, err := routingKeyParameter(RoutingKeyRegistrationRequest{
			LocalRoutingKeyIdentifier: identifier,
			RoutingKey:                testRoutingKey(10, 100, params.ServiceIndSCCP),
		})
		if err != nil {
			t.Fatalf("routingKeyParameter: %v", err)
		}
		return messages.NewRegistrationRequest(parameter)
	}

	if err := association.handleRegistrationRequest(request(1)); err != nil {
		t.Fatalf("first Registration Request: %v", err)
	}
	result, err := response.RegistrationResults[0].RegistrationResult()
	if err != nil {
		t.Fatalf("RegistrationResult: %v", err)
	}
	key := ASKey{
		NetworkAppearance:    10,
		NetworkAppearanceSet: true,
		RoutingContext:       result.RoutingContext.RoutingContext(),
		RoutingContextSet:    true,
	}
	applicationServer, ok := endpoint.as.lookup(key)
	if !ok {
		t.Fatal("newly registered Application Server is absent")
	}
	if active := applicationServer.activeASPs(); len(active) != 0 {
		t.Fatalf("newly registered Application Server has active ASPs %v before ASP Active", active)
	}
	if got := applicationServer.State(); got != ASInactive {
		t.Fatalf("newly registered Application Server state = %v, want AS-INACTIVE", got)
	}

	applicationServer.setASPState(association, StateASPActive, time.Hour)
	if err := association.handleRegistrationRequest(request(2)); err != nil {
		t.Fatalf("replayed Registration Request: %v", err)
	}
	if active := applicationServer.activeASPs(); len(active) != 1 || active[0] != association {
		t.Fatalf("replayed Registration changed existing active membership: %v", active)
	}
}

func TestRKMRegistrationAfterUnscopedActivationRequiresNewASPActive(t *testing.T) {
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
	config.NetworkAppearance = params.NewNetworkAppearance(10)
	association := newAssociation(RoleSGP, config)
	association.endpoint = endpoint
	association.as = endpoint.as
	responses := make(chan *messages.RegistrationResponse, 1)
	association.signalWriter = func(message messages.M3UA) (int, error) {
		if registrationResponse, ok := message.(*messages.RegistrationResponse); ok {
			responses <- registrationResponse
		}
		return message.MarshalLen(), nil
	}
	association.muState.Lock()
	association.state = StateASPActive
	association.muState.Unlock()
	association.noteRoutingContextsActive(nil)
	endpoint.as.aspStateChanged(association, StateASPActive)

	contextlessKey := ASKey{NetworkAppearance: 10, NetworkAppearanceSet: true}
	if !association.activeForASKey(contextlessKey) {
		t.Fatal("contextless Application Server was not active before Registration")
	}
	parameter, err := routingKeyParameter(RoutingKeyRegistrationRequest{
		LocalRoutingKeyIdentifier: 1,
		RoutingKey:                testRoutingKey(10, 100, params.ServiceIndSCCP),
	})
	if err != nil {
		t.Fatalf("routingKeyParameter: %v", err)
	}
	if err := association.handleRegistrationRequest(messages.NewRegistrationRequest(parameter)); err != nil {
		t.Fatalf("handleRegistrationRequest: %v", err)
	}
	response := <-responses
	result, err := response.RegistrationResults[0].RegistrationResult()
	if err != nil {
		t.Fatalf("RegistrationResult: %v", err)
	}
	routingContext := result.RoutingContext.RoutingContext()
	if association.activeForRoutingContext(routingContext) {
		t.Fatalf("new Routing Context %d became active without ASP Active", routingContext)
	}
	if !association.activeForASKey(contextlessKey) {
		t.Fatal("Registration inactivated the previously active contextless Application Server")
	}
	association.noteRoutingContextsActive([]uint32{routingContext})
	endpoint.as.aspStateChanged(association, StateASPActive)
	if !association.activeForASKey(contextlessKey) {
		t.Fatal("scoped ASP Active inactivated the previously active contextless Application Server")
	}
	if !association.activeForRoutingContext(routingContext) {
		t.Fatalf("scoped ASP Active did not activate Routing Context %d", routingContext)
	}
	association.noteRoutingContextsActive(nil)
	association.noteRoutingContextsInactive([]uint32{routingContext})
	endpoint.as.aspStateChanged(association, StateASPActive)
	if !association.activeForASKey(contextlessKey) {
		t.Fatal("scoped ASP Inactive inactivated the unrelated contextless Application Server")
	}
	if association.activeForRoutingContext(routingContext) {
		t.Fatalf("scoped ASP Inactive retained Routing Context %d", routingContext)
	}
	contextlessApplicationServer, ok := endpoint.as.lookup(contextlessKey)
	if !ok {
		t.Fatal("contextless Application Server disappeared after scoped activation changes")
	}
	active := contextlessApplicationServer.activeASPs()
	if len(active) != 1 || active[0] != association {
		t.Fatalf("contextless Application Server active ASPs = %v, want the original Association", active)
	}
	deregistration := endpoint.routingKeys.deregister(association, []uint32{routingContext})
	if len(deregistration) != 1 || deregistration[0].Status != DeregistrationSuccessfullyDeregistered {
		t.Fatalf("inactive new Routing Context Deregistration = %+v, want success", deregistration)
	}
}

func TestRKMContextlessIPSPRemainsActiveAfterScopedOverride(t *testing.T) {
	config := NewAssociationConfig(0, 0, 0, 0, 0, 0)
	config.IPSP = &IPSPConfig{ExchangeModel: IPSPExchangeSingle}
	association := newAssociation(RoleIPSP, config)
	association.muState.Lock()
	association.state = StateASPActive
	association.muState.Unlock()
	routingKey := testRoutingKey(10, 100, params.ServiceIndSCCP)
	association.addDynamicASKey(ASKey{
		NetworkAppearance:    10,
		NetworkAppearanceSet: true,
		RoutingContext:       7,
		RoutingContextSet:    true,
	}, routingKey, false)
	association.noteRoutingContextsActive(nil)
	association.noteRoutingContextsOverridden([]uint32{7})

	contextlessKey := ASKey{NetworkAppearance: 10, NetworkAppearanceSet: true}
	if !association.activeForASKey(contextlessKey) {
		t.Fatal("scoped override inactivated the unrelated contextless Application Server")
	}
	if association.activeForRoutingContext(7) {
		t.Fatal("scoped override retained the overridden Routing Context")
	}
	if state := association.stateForActiveRoutingContexts(); state != StateASPActive {
		t.Fatalf("Association state after scoped override = %v, want ASP-ACTIVE for contextless AS", state)
	}
}

func TestRKMAllocatorAvoidsConfiguredApplicationServerRoutingContexts(t *testing.T) {
	for _, test := range []struct {
		name      string
		allocator RoutingContextAllocator
	}{
		{name: "default allocator"},
		{name: "custom allocator", allocator: func(request RoutingContextAllocationRequest) (uint32, error) {
			if len(request.InUseRoutingContexts) != 1 || request.InUseRoutingContexts[0] != 1 {
				return 0, errors.New("configured Routing Context 1 was not reported in use")
			}
			return 2, nil
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			endpoint, err := NewEndpoint(EndpointConfig{
				Role: RoleSGP,
				RoutingKeyManagement: &RoutingKeyManagementConfig{
					AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
						return RegistrationSuccessfullyRegistered
					},
					AllocateRoutingContext:  test.allocator,
					AllowDynamicRoutingKeys: true,
				},
			})
			if err != nil {
				t.Fatalf("NewEndpoint: %v", err)
			}
			defer func() { _ = endpoint.Close() }()

			config := NewAssociationConfig(0, 0, 0, 0, 0, 0)
			config.NetworkAppearance = params.NewNetworkAppearance(10)
			config.RoutingContexts = params.NewRoutingContext(1)
			association := newAssociation(RoleSGP, config)
			association.endpoint = endpoint
			association.as = endpoint.as
			var response *messages.RegistrationResponse
			association.signalWriter = func(message messages.M3UA) (int, error) {
				if registrationResponse, ok := message.(*messages.RegistrationResponse); ok {
					response = registrationResponse
				}
				return message.MarshalLen(), nil
			}
			association.muState.Lock()
			association.state = StateASPInactive
			association.muState.Unlock()
			endpoint.as.aspStateChanged(association, StateASPInactive)

			parameter, err := routingKeyParameter(RoutingKeyRegistrationRequest{
				LocalRoutingKeyIdentifier: 1,
				RoutingKey:                testRoutingKey(10, 100, params.ServiceIndSCCP),
			})
			if err != nil {
				t.Fatalf("routingKeyParameter: %v", err)
			}
			if err := association.handleRegistrationRequest(messages.NewRegistrationRequest(parameter)); err != nil {
				t.Fatalf("handleRegistrationRequest: %v", err)
			}
			result, err := response.RegistrationResults[0].RegistrationResult()
			if err != nil {
				t.Fatalf("RegistrationResult: %v", err)
			}
			if got := result.RoutingContext.RoutingContext(); got != 2 {
				t.Fatalf("allocated Routing Context = %d, want 2 after configured Routing Context 1", got)
			}
		})
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

func TestRKMRequesterSerializationWaitRespectsContext(t *testing.T) {
	t.Run("Registration", func(t *testing.T) {
		association := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
		association.muState.Lock()
		association.state = StateASPInactive
		association.muState.Unlock()
		written := make(chan uint32, 1)
		association.signalWriter = func(message messages.M3UA) (int, error) {
			request := message.(*messages.RegistrationRequest)
			payload, err := request.RoutingKeys[0].RoutingKey()
			if err != nil {
				return 0, err
			}
			written <- payload.LocalRoutingKeyIdentifier.LocalRoutingKeyIdentifier()
			return message.MarshalLen(), nil
		}
		firstDone := make(chan error, 1)
		go func() {
			_, err := association.RegisterRoutingKeys(context.Background(), RoutingKeyRegistration{
				RoutingKey: testRoutingKey(10, 100, params.ServiceIndSCCP),
			})
			firstDone <- err
		}()
		identifier := <-written

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		secondDone := make(chan error, 1)
		go func() {
			_, err := association.RegisterRoutingKeys(ctx, RoutingKeyRegistration{
				RoutingKey: testRoutingKey(20, 200, params.ServiceIndISUP),
			})
			secondDone <- err
		}()
		var secondErr error
		returnedBeforeRelease := false
		select {
		case secondErr = <-secondDone:
			returnedBeforeRelease = true
		case <-time.After(250 * time.Millisecond):
		}
		if err := association.handleRegistrationResponse(messages.NewRegistrationResponse(
			params.NewRegistrationResult(params.NewRegistrationResultPayload(
				params.NewLocalRoutingKeyIdentifier(identifier),
				params.NewRegistrationStatus(params.SuccessfullyRegistered),
				params.NewRoutingContext(100),
			)),
		)); err != nil {
			t.Fatalf("first Registration Response: %v", err)
		}
		if err := <-firstDone; err != nil {
			t.Fatalf("first RegisterRoutingKeys: %v", err)
		}
		if !returnedBeforeRelease {
			<-secondDone
			t.Fatal("queued RegisterRoutingKeys ignored its context deadline until the first procedure completed")
		}
		if !errors.Is(secondErr, context.DeadlineExceeded) {
			t.Fatalf("queued RegisterRoutingKeys error = %v, want context.DeadlineExceeded", secondErr)
		}
	})

	t.Run("Deregistration", func(t *testing.T) {
		association := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
		association.muState.Lock()
		association.state = StateASPInactive
		association.muState.Unlock()
		for _, routingContext := range []uint32{100, 200} {
			association.addDynamicASKey(
				ASKey{RoutingContext: routingContext, RoutingContextSet: true},
				testRoutingKey(10, routingContext, params.ServiceIndSCCP),
				false,
			)
		}
		written := make(chan uint32, 1)
		association.signalWriter = func(message messages.M3UA) (int, error) {
			request := message.(*messages.DeregistrationRequest)
			written <- request.RoutingContext.RoutingContexts()[0]
			return message.MarshalLen(), nil
		}
		firstDone := make(chan error, 1)
		go func() {
			_, err := association.DeregisterRoutingContexts(context.Background(), 100)
			firstDone <- err
		}()
		<-written

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		secondDone := make(chan error, 1)
		go func() {
			_, err := association.DeregisterRoutingContexts(ctx, 200)
			secondDone <- err
		}()
		var secondErr error
		returnedBeforeRelease := false
		select {
		case secondErr = <-secondDone:
			returnedBeforeRelease = true
		case <-time.After(250 * time.Millisecond):
		}
		if err := association.handleDeregistrationResponse(messages.NewDeregistrationResponse(
			params.NewDeregistrationResult(params.NewDeregResultPayload(
				params.NewRoutingContext(100),
				params.NewDeregistrationStatus(params.SuccessfullyDeregistered),
			)),
		)); err != nil {
			t.Fatalf("first Deregistration Response: %v", err)
		}
		if err := <-firstDone; err != nil {
			t.Fatalf("first DeregisterRoutingContexts: %v", err)
		}
		if !returnedBeforeRelease {
			<-secondDone
			t.Fatal("queued DeregisterRoutingContexts ignored its context deadline until the first procedure completed")
		}
		if !errors.Is(secondErr, context.DeadlineExceeded) {
			t.Fatalf("queued DeregisterRoutingContexts error = %v, want context.DeadlineExceeded", secondErr)
		}
	})
}

func TestRKMRequesterRechecksStateAfterSerialization(t *testing.T) {
	t.Run("Registration", func(t *testing.T) {
		association := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
		association.muState.Lock()
		association.state = StateASPInactive
		association.muState.Unlock()
		written := make(chan uint32, 2)
		writeCount := 0
		association.signalWriter = func(message messages.M3UA) (int, error) {
			request := message.(*messages.RegistrationRequest)
			payload, err := request.RoutingKeys[0].RoutingKey()
			if err != nil {
				return 0, err
			}
			identifier := payload.LocalRoutingKeyIdentifier.LocalRoutingKeyIdentifier()
			writeCount++
			written <- identifier
			if writeCount == 1 {
				return message.MarshalLen(), nil
			}
			return message.MarshalLen(), association.handleRegistrationResponse(messages.NewRegistrationResponse(
				params.NewRegistrationResult(params.NewRegistrationResultPayload(
					params.NewLocalRoutingKeyIdentifier(identifier),
					params.NewRegistrationStatus(params.SuccessfullyRegistered),
					params.NewRoutingContext(200),
				)),
			))
		}
		firstDone := make(chan error, 1)
		go func() {
			_, err := association.RegisterRoutingKeys(context.Background(), RoutingKeyRegistration{
				RoutingKey: testRoutingKey(10, 100, params.ServiceIndSCCP),
			})
			firstDone <- err
		}()
		firstIdentifier := <-written

		ctx := newRKMInitialCheckContext()
		secondDone := make(chan error, 1)
		go func() {
			_, err := association.RegisterRoutingKeys(ctx, RoutingKeyRegistration{
				RoutingKey: testRoutingKey(20, 200, params.ServiceIndISUP),
			})
			secondDone <- err
		}()
		<-ctx.checked
		association.muState.Lock()
		association.state = StateASPDown
		association.muState.Unlock()
		if err := association.handleRegistrationResponse(messages.NewRegistrationResponse(
			params.NewRegistrationResult(params.NewRegistrationResultPayload(
				params.NewLocalRoutingKeyIdentifier(firstIdentifier),
				params.NewRegistrationStatus(params.SuccessfullyRegistered),
				params.NewRoutingContext(100),
			)),
		)); err != nil {
			t.Fatalf("first Registration Response: %v", err)
		}
		if err := <-firstDone; err != nil {
			t.Fatalf("first RegisterRoutingKeys: %v", err)
		}
		if err := <-secondDone; !errors.Is(err, ErrNotEstablished) {
			t.Fatalf("queued RegisterRoutingKeys error = %v, want ErrNotEstablished", err)
		}
		if writeCount != 1 {
			t.Fatalf("Registration Request writes = %d, want only the first procedure", writeCount)
		}
	})

	t.Run("Deregistration", func(t *testing.T) {
		association := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
		association.muState.Lock()
		association.state = StateASPInactive
		association.muState.Unlock()
		for _, routingContext := range []uint32{100, 200} {
			association.addDynamicASKey(
				ASKey{RoutingContext: routingContext, RoutingContextSet: true},
				testRoutingKey(10, routingContext, params.ServiceIndSCCP),
				false,
			)
		}
		written := make(chan uint32, 2)
		writeCount := 0
		association.signalWriter = func(message messages.M3UA) (int, error) {
			request := message.(*messages.DeregistrationRequest)
			routingContext := request.RoutingContext.RoutingContexts()[0]
			writeCount++
			written <- routingContext
			if writeCount == 1 {
				return message.MarshalLen(), nil
			}
			return message.MarshalLen(), association.handleDeregistrationResponse(messages.NewDeregistrationResponse(
				params.NewDeregistrationResult(params.NewDeregResultPayload(
					params.NewRoutingContext(routingContext),
					params.NewDeregistrationStatus(params.SuccessfullyDeregistered),
				)),
			))
		}
		firstDone := make(chan error, 1)
		go func() {
			_, err := association.DeregisterRoutingContexts(context.Background(), 100)
			firstDone <- err
		}()
		<-written

		ctx := newRKMInitialCheckContext()
		secondDone := make(chan error, 1)
		go func() {
			_, err := association.DeregisterRoutingContexts(ctx, 200)
			secondDone <- err
		}()
		<-ctx.checked
		association.muState.Lock()
		association.state = StateASPDown
		association.muState.Unlock()
		if err := association.handleDeregistrationResponse(messages.NewDeregistrationResponse(
			params.NewDeregistrationResult(params.NewDeregResultPayload(
				params.NewRoutingContext(100),
				params.NewDeregistrationStatus(params.SuccessfullyDeregistered),
			)),
		)); err != nil {
			t.Fatalf("first Deregistration Response: %v", err)
		}
		if err := <-firstDone; err != nil {
			t.Fatalf("first DeregisterRoutingContexts: %v", err)
		}
		if err := <-secondDone; !errors.Is(err, ErrNotEstablished) {
			t.Fatalf("queued DeregisterRoutingContexts error = %v, want ErrNotEstablished", err)
		}
		if writeCount != 1 {
			t.Fatalf("Deregistration Request writes = %d, want only the first procedure", writeCount)
		}
	})
}

func TestRKMResponseChannelPublicationIsRaceSafe(t *testing.T) {
	association := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	association.muState.Lock()
	association.state = StateASPInactive
	association.muState.Unlock()
	written := make(chan struct{}, 1)
	association.signalWriter = func(message messages.M3UA) (int, error) {
		written <- struct{}{}
		return message.MarshalLen(), nil
	}
	response := messages.NewDeregistrationResponse(params.NewDeregistrationResult(
		params.NewDeregResultPayload(
			params.NewRoutingContext(1),
			params.NewDeregistrationStatus(params.DeregNotRegistered),
		),
	))
	stop := make(chan struct{})
	responderDone := make(chan struct{})
	go func() {
		defer close(responderDone)
		for {
			select {
			case <-stop:
				return
			default:
				_ = association.handleDeregistrationResponse(response)
				runtime.Gosched()
			}
		}
	}()

	for range 1_000 {
		ctx, cancel := context.WithCancel(context.Background())
		requestDone := make(chan struct{})
		go func() {
			defer close(requestDone)
			_, _ = association.DeregisterRoutingContexts(ctx, 1)
		}()
		<-written
		cancel()
		<-requestDone
		// Cancellation can leave the response outcome unresolved. Deliver one
		// final response after the requester returns so the same Routing Context
		// is safe to use in the next publication-race iteration.
		_ = association.handleDeregistrationResponse(response)
	}
	close(stop)
	<-responderDone
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

func setRegistrationResultWriter(
	t *testing.T,
	association *Association,
	status uint32,
	routingContexts ...uint32,
) {
	t.Helper()
	association.signalWriter = func(message messages.M3UA) (int, error) {
		request, ok := message.(*messages.RegistrationRequest)
		if !ok {
			return message.MarshalLen(), nil
		}
		if len(routingContexts) != 1 && len(routingContexts) != len(request.RoutingKeys) {
			return 0, fmt.Errorf(
				"Registration Result Routing Contexts = %d, want one or %d",
				len(routingContexts),
				len(request.RoutingKeys),
			)
		}
		results := make([]*params.Param, len(request.RoutingKeys))
		for index, parameter := range request.RoutingKeys {
			payload, err := parameter.RoutingKey()
			if err != nil {
				return 0, err
			}
			routingContextIndex := index
			if len(routingContexts) == 1 {
				routingContextIndex = 0
			}
			results[index] = params.NewRegistrationResult(params.NewRegistrationResultPayload(
				payload.LocalRoutingKeyIdentifier.Copy(),
				params.NewRegistrationStatus(status),
				params.NewRoutingContext(routingContexts[routingContextIndex]),
			))
		}
		return message.MarshalLen(), association.handleRegistrationResponse(messages.NewRegistrationResponse(results...))
	}
}

func TestRKMRequesterAcceptsWideOriginatingPointCodeMask(t *testing.T) {
	association := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	association.muState.Lock()
	association.state = StateASPInactive
	association.muState.Unlock()
	written := false
	association.signalWriter = func(message messages.M3UA) (int, error) {
		request, ok := message.(*messages.RegistrationRequest)
		if !ok {
			return 0, fmt.Errorf("wrote %T, want Registration Request", message)
		}
		payload, err := request.RoutingKeys[0].RoutingKey()
		if err != nil {
			return 0, err
		}
		decoded, err := routingKeyFromPayload(payload)
		if err != nil {
			return 0, err
		}
		originatingPointCodes := decoded.RoutingKey.Groups[0].OriginatingPointCodes
		if len(originatingPointCodes) != 1 || originatingPointCodes[0].Mask != 255 {
			return 0, fmt.Errorf("written originating point codes = %+v, want mask 255", originatingPointCodes)
		}
		written = true
		response := messages.NewRegistrationResponse(params.NewRegistrationResult(
			params.NewRegistrationResultPayload(
				payload.LocalRoutingKeyIdentifier.Copy(),
				params.NewRegistrationStatus(params.SuccessfullyRegistered),
				params.NewRoutingContext(77),
			),
		))
		return message.MarshalLen(), association.handleRegistrationResponse(response)
	}

	routingKey := testRoutingKey(10, 100, params.ServiceIndSCCP)
	routingKey.Groups[0].OriginatingPointCodes = []PointCodeRange{{PointCode: 0x123456, Mask: 255}}
	results, err := association.RegisterRoutingKeys(context.Background(), RoutingKeyRegistration{RoutingKey: routingKey})
	if err != nil {
		t.Fatalf("RegisterRoutingKeys: %v", err)
	}
	if !written {
		t.Fatal("RegisterRoutingKeys did not write a Registration Request")
	}
	if len(results) != 1 || results[0].Status != RegistrationSuccessfullyRegistered || results[0].RoutingContext != 77 {
		t.Fatalf("Registration Results = %+v, want one success for Routing Context 77", results)
	}
}

func TestRKMRequesterRejectsConflictingRegistrationResultScopesAtomically(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleIPSP})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	defer func() { _ = endpoint.Close() }()
	config := NewAssociationConfig(0, 0, 0, 0, 0, 0)
	config.IPSP = &IPSPConfig{ExchangeModel: IPSPExchangeSingle}
	association := newAssociation(RoleIPSP, config)
	association.as = endpoint.as
	association.muState.Lock()
	association.state = StateASPInactive
	association.muState.Unlock()
	setRegistrationResultWriter(t, association, params.SuccessfullyRegistered, 77)

	_, err = association.RegisterRoutingKeys(
		context.Background(),
		RoutingKeyRegistration{RoutingKey: testRoutingKey(10, 100, params.ServiceIndSCCP)},
		RoutingKeyRegistration{RoutingKey: testRoutingKey(20, 200, params.ServiceIndISUP)},
	)
	if !errors.Is(err, ErrInvalidRoutingContext) {
		t.Fatalf("RegisterRoutingKeys error = %v, want ErrInvalidRoutingContext", err)
	}
	if contexts := association.dynamicRoutingContexts(false); len(contexts) != 0 {
		t.Fatalf("conflicting Registration Results installed dynamic Routing Contexts: %v", contexts)
	}
	for _, appearance := range []uint32{10, 20} {
		key := ASKey{
			NetworkAppearance:    appearance,
			NetworkAppearanceSet: true,
			RoutingContext:       77,
			RoutingContextSet:    true,
		}
		if _, ok := endpoint.as.lookup(key); ok {
			t.Fatalf("conflicting Registration Result installed Application Server %+v", key)
		}
	}
}

func TestRKMRequesterClassifiesDuplicateRegistrationResults(t *testing.T) {
	newResult := func(identifier, status, routingContext uint32) *params.Param {
		return params.NewRegistrationResult(params.NewRegistrationResultPayload(
			params.NewLocalRoutingKeyIdentifier(identifier),
			params.NewRegistrationStatus(status),
			params.NewRoutingContext(routingContext),
		))
	}

	t.Run("identical pending", func(t *testing.T) {
		association := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
		association.muState.Lock()
		association.state = StateASPInactive
		association.muState.Unlock()
		association.signalWriter = func(message messages.M3UA) (int, error) {
			request := message.(*messages.RegistrationRequest)
			payload, err := request.RoutingKeys[0].RoutingKey()
			if err != nil {
				return 0, err
			}
			identifier := payload.LocalRoutingKeyIdentifier.LocalRoutingKeyIdentifier()
			response := messages.NewRegistrationResponse(
				newResult(identifier, params.SuccessfullyRegistered, 77),
				newResult(identifier, params.SuccessfullyRegistered, 77),
			)
			return message.MarshalLen(), association.handleRegistrationResponse(response)
		}

		results, err := association.RegisterRoutingKeys(context.Background(), RoutingKeyRegistration{
			RoutingKey: testRoutingKey(10, 100, params.ServiceIndSCCP),
		})
		if err != nil {
			t.Fatalf("RegisterRoutingKeys: %v", err)
		}
		if len(results) != 1 || results[0].Status != RegistrationSuccessfullyRegistered || results[0].RoutingContext != 77 {
			t.Fatalf("Registration Results = %+v, want one success for Routing Context 77", results)
		}
		if key, ok := association.dynamicASKey(77, false); !ok || key.RoutingContext != 77 {
			t.Fatalf("dynamic ASKey = %+v, %t; want Routing Context 77", key, ok)
		}
	})

	t.Run("contradictory pending", func(t *testing.T) {
		association := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
		association.muState.Lock()
		association.state = StateASPInactive
		association.muState.Unlock()
		association.signalWriter = func(message messages.M3UA) (int, error) {
			request := message.(*messages.RegistrationRequest)
			payload, err := request.RoutingKeys[0].RoutingKey()
			if err != nil {
				return 0, err
			}
			identifier := payload.LocalRoutingKeyIdentifier.LocalRoutingKeyIdentifier()
			response := messages.NewRegistrationResponse(
				newResult(identifier, params.SuccessfullyRegistered, 77),
				newResult(identifier, params.PermissionDenied, 0),
			)
			return message.MarshalLen(), association.handleRegistrationResponse(response)
		}

		_, err := association.RegisterRoutingKeys(context.Background(), RoutingKeyRegistration{
			RoutingKey: testRoutingKey(10, 100, params.ServiceIndSCCP),
		})
		if !errors.Is(err, ErrInvalidParameterValue) {
			t.Fatalf("RegisterRoutingKeys error = %v, want ErrInvalidParameterValue", err)
		}
		if contexts := association.dynamicRoutingContexts(false); len(contexts) != 0 {
			t.Fatalf("contradictory Registration Results installed Routing Contexts %v", contexts)
		}
	})

	t.Run("contradictory late", func(t *testing.T) {
		association := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
		association.muState.Lock()
		association.state = StateASPInactive
		association.muState.Unlock()
		written := make(chan uint32, 1)
		association.signalWriter = func(message messages.M3UA) (int, error) {
			request := message.(*messages.RegistrationRequest)
			payload, err := request.RoutingKeys[0].RoutingKey()
			if err != nil {
				return 0, err
			}
			written <- payload.LocalRoutingKeyIdentifier.LocalRoutingKeyIdentifier()
			return message.MarshalLen(), nil
		}

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := association.RegisterRoutingKeys(ctx, RoutingKeyRegistration{
				RoutingKey: testRoutingKey(10, 100, params.ServiceIndSCCP),
			})
			done <- err
		}()
		identifier := <-written
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("RegisterRoutingKeys error = %v, want context.Canceled", err)
		}

		response := messages.NewRegistrationResponse(
			newResult(identifier, params.SuccessfullyRegistered, 77),
			newResult(identifier, params.PermissionDenied, 0),
		)
		if err := association.handleRegistrationResponse(response); !errors.Is(err, ErrInvalidParameterValue) {
			t.Fatalf("late Registration Response error = %v, want ErrInvalidParameterValue", err)
		}
		if contexts := association.dynamicRoutingContexts(false); len(contexts) != 0 {
			t.Fatalf("late contradictory Registration Results installed Routing Contexts %v", contexts)
		}
		association.rkmCorrelationMu.Lock()
		_, unresolved := association.rkmUnresolvedRegistrations[identifier]
		association.rkmCorrelationMu.Unlock()
		if !unresolved {
			t.Fatal("late contradictory Registration Results cleared the unresolved outcome")
		}
	})
}

func TestRKMRequesterRejectsContradictoryPreviouslyDeliveredRegistrationResult(t *testing.T) {
	association := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	requests := map[uint32]RoutingKeyRegistrationRequest{
		1: {LocalRoutingKeyIdentifier: 1, RoutingKey: testRoutingKey(10, 100, params.ServiceIndSCCP)},
		2: {LocalRoutingKeyIdentifier: 2, RoutingKey: testRoutingKey(20, 200, params.ServiceIndISUP)},
	}
	responses, err := association.beginRegistrationResponseCorrelation(map[uint32]int{1: 0, 2: 1}, requests)
	if err != nil {
		t.Fatalf("beginRegistrationResponseCorrelation: %v", err)
	}
	t.Cleanup(func() { association.endRegistrationResponseCorrelation(false, true) })
	result := func(identifier, status, routingContext uint32) *params.Param {
		return params.NewRegistrationResult(params.NewRegistrationResultPayload(
			params.NewLocalRoutingKeyIdentifier(identifier),
			params.NewRegistrationStatus(status),
			params.NewRoutingContext(routingContext),
		))
	}
	if err := association.deliverRegistrationResponse(messages.NewRegistrationResponse(
		result(1, params.SuccessfullyRegistered, 77),
	)); err != nil {
		t.Fatalf("deliver first Registration Result: %v", err)
	}
	<-responses
	if err := association.deliverRegistrationResponse(messages.NewRegistrationResponse(
		result(1, params.PermissionDenied, 0),
	)); !errors.Is(err, ErrInvalidParameterValue) {
		t.Fatalf("contradictory delivered Registration Result error = %v, want ErrInvalidParameterValue", err)
	}
	association.rkmCorrelationMu.Lock()
	delivered := association.rkmDeliveredRegistrationResults[1]
	_, secondPending := association.rkmPendingLocalIDs[2]
	association.rkmCorrelationMu.Unlock()
	if delivered.Status != RegistrationSuccessfullyRegistered || delivered.RoutingContext != 77 {
		t.Fatalf("first delivered Registration Result changed to %+v", delivered)
	}
	if !secondPending {
		t.Fatal("contradictory delivered Registration Result cleared another pending identifier")
	}
}

func TestRKMRequesterRejectsRegistrationResultConflictingWithExistingScope(t *testing.T) {
	t.Run("Static", func(t *testing.T) {
		config := NewAssociationConfig(0, 0, 0, 0, 0, 0)
		config.NetworkAppearance = params.NewNetworkAppearance(10)
		config.RoutingContexts = params.NewRoutingContext(77)
		association := newAssociation(RoleASP, config)
		association.muState.Lock()
		association.state = StateASPInactive
		association.muState.Unlock()
		setRegistrationResultWriter(t, association, params.SuccessfullyRegistered, 77)

		_, err := association.RegisterRoutingKeys(context.Background(), RoutingKeyRegistration{
			RoutingKey: testRoutingKey(20, 200, params.ServiceIndISUP),
		})
		if !errors.Is(err, ErrInvalidRoutingContext) {
			t.Fatalf("RegisterRoutingKeys error = %v, want ErrInvalidRoutingContext", err)
		}
		if _, ok := association.dynamicASKey(77, false); ok {
			t.Fatal("conflicting Registration Result replaced the static Routing Context scope")
		}
	})

	t.Run("Dynamic", func(t *testing.T) {
		association := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
		association.muState.Lock()
		association.state = StateASPInactive
		association.muState.Unlock()
		existing := ASKey{
			NetworkAppearance:    10,
			NetworkAppearanceSet: true,
			RoutingContext:       77,
			RoutingContextSet:    true,
		}
		association.addDynamicASKey(existing, testRoutingKey(10, 100, params.ServiceIndSCCP), false)
		setRegistrationResultWriter(t, association, params.SuccessfullyRegistered, 77)

		_, err := association.RegisterRoutingKeys(context.Background(), RoutingKeyRegistration{
			RoutingKey: testRoutingKey(20, 200, params.ServiceIndISUP),
		})
		if !errors.Is(err, ErrInvalidRoutingContext) {
			t.Fatalf("RegisterRoutingKeys error = %v, want ErrInvalidRoutingContext", err)
		}
		if key, ok := association.dynamicASKey(77, false); !ok || key != existing {
			t.Fatalf("dynamic ASKey = %+v, %t; want existing %+v", key, ok, existing)
		}
	})
}

func TestRKMRequesterAllowsRegistrationResultForExistingASKey(t *testing.T) {
	association := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	association.muState.Lock()
	association.state = StateASPInactive
	association.muState.Unlock()
	existing := ASKey{
		NetworkAppearance:    10,
		NetworkAppearanceSet: true,
		RoutingContext:       77,
		RoutingContextSet:    true,
	}
	association.addDynamicASKey(existing, testRoutingKey(10, 100, params.ServiceIndSCCP), false)
	setRegistrationResultWriter(t, association, params.RoutingKeyAlreadyRegistered, 77)

	results, err := association.RegisterRoutingKeys(context.Background(), RoutingKeyRegistration{
		RoutingKey: testRoutingKey(10, 100, params.ServiceIndSCCP),
	})
	if err != nil {
		t.Fatalf("RegisterRoutingKeys: %v", err)
	}
	if len(results) != 1 || results[0].Status != RegistrationRoutingKeyAlreadyRegistered {
		t.Fatalf("Registration Results = %+v, want Routing Key Already Registered", results)
	}
	if key, ok := association.dynamicASKey(77, false); !ok || key != existing {
		t.Fatalf("dynamic ASKey = %+v, %t; want existing %+v", key, ok, existing)
	}
}

func TestRKMRequesterRejectsConflictingRegistrationResultTrafficModesAtomically(t *testing.T) {
	association := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	association.muState.Lock()
	association.state = StateASPInactive
	association.muState.Unlock()
	setRegistrationResultWriter(t, association, params.SuccessfullyRegistered, 77)
	first := testRoutingKey(10, 100, params.ServiceIndSCCP)
	first.TrafficMode = params.TrafficModeOverride
	first.TrafficModeSet = true
	second := testRoutingKey(10, 200, params.ServiceIndISUP)
	second.TrafficMode = params.TrafficModeLoadshare
	second.TrafficModeSet = true

	_, err := association.RegisterRoutingKeys(
		context.Background(),
		RoutingKeyRegistration{RoutingKey: first},
		RoutingKeyRegistration{RoutingKey: second},
	)
	if !errors.Is(err, ErrInvalidRoutingContext) {
		t.Fatalf("RegisterRoutingKeys error = %v, want ErrInvalidRoutingContext", err)
	}
	if contexts := association.dynamicRoutingContexts(false); len(contexts) != 0 {
		t.Fatalf("conflicting Registration Result Traffic Modes installed dynamic Routing Contexts: %v", contexts)
	}
}

func TestRKMRequesterRejectsRegistrationResultConflictingWithExistingTrafficMode(t *testing.T) {
	request := testRoutingKey(10, 100, params.ServiceIndSCCP)
	request.TrafficMode = params.TrafficModeLoadshare
	request.TrafficModeSet = true

	t.Run("Static", func(t *testing.T) {
		config := NewAssociationConfig(0, 0, 0, 0, 0, 0)
		config.NetworkAppearance = params.NewNetworkAppearance(10)
		config.RoutingContexts = params.NewRoutingContext(77)
		config.TrafficModes = map[uint32]uint32{77: params.TrafficModeOverride}
		association := newAssociation(RoleASP, config)
		association.muState.Lock()
		association.state = StateASPInactive
		association.muState.Unlock()
		setRegistrationResultWriter(t, association, params.RoutingKeyAlreadyRegistered, 77)

		_, err := association.RegisterRoutingKeys(context.Background(), RoutingKeyRegistration{RoutingKey: request})
		if !errors.Is(err, ErrInvalidRoutingContext) {
			t.Fatalf("RegisterRoutingKeys error = %v, want ErrInvalidRoutingContext", err)
		}
		if _, ok := association.dynamicASKey(77, false); ok {
			t.Fatal("conflicting Registration Result Traffic Mode replaced the static policy")
		}
	})

	t.Run("Dynamic", func(t *testing.T) {
		association := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
		association.muState.Lock()
		association.state = StateASPInactive
		association.muState.Unlock()
		key := ASKey{
			NetworkAppearance:    10,
			NetworkAppearanceSet: true,
			RoutingContext:       77,
			RoutingContextSet:    true,
		}
		existing := testRoutingKey(10, 100, params.ServiceIndSCCP)
		existing.TrafficMode = params.TrafficModeOverride
		existing.TrafficModeSet = true
		association.addDynamicASKey(key, existing, false)
		setRegistrationResultWriter(t, association, params.RoutingKeyAlreadyRegistered, 77)

		_, err := association.RegisterRoutingKeys(context.Background(), RoutingKeyRegistration{RoutingKey: request})
		if !errors.Is(err, ErrInvalidRoutingContext) {
			t.Fatalf("RegisterRoutingKeys error = %v, want ErrInvalidRoutingContext", err)
		}
		if mode, ok := association.trafficModePolicy().configured(77); !ok || mode != params.TrafficModeOverride {
			t.Fatalf("dynamic Traffic Mode = %d, %t; want Override", mode, ok)
		}
	})
}

func TestRKMLateRegistrationResponseRejectsConflictingScopesAtomically(t *testing.T) {
	association := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	association.muState.Lock()
	association.state = StateASPInactive
	association.muState.Unlock()
	written := make(chan []uint32, 1)
	association.signalWriter = func(message messages.M3UA) (int, error) {
		request := message.(*messages.RegistrationRequest)
		identifiers := make([]uint32, len(request.RoutingKeys))
		for index, parameter := range request.RoutingKeys {
			payload, err := parameter.RoutingKey()
			if err != nil {
				return 0, err
			}
			identifiers[index] = payload.LocalRoutingKeyIdentifier.LocalRoutingKeyIdentifier()
		}
		written <- identifiers
		return message.MarshalLen(), nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := association.RegisterRoutingKeys(
			ctx,
			RoutingKeyRegistration{RoutingKey: testRoutingKey(10, 100, params.ServiceIndSCCP)},
			RoutingKeyRegistration{RoutingKey: testRoutingKey(20, 200, params.ServiceIndISUP)},
		)
		done <- err
	}()
	identifiers := <-written
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("RegisterRoutingKeys error = %v, want context.Canceled", err)
	}

	response := messages.NewRegistrationResponse(
		params.NewRegistrationResult(params.NewRegistrationResultPayload(
			params.NewLocalRoutingKeyIdentifier(identifiers[0]),
			params.NewRegistrationStatus(params.SuccessfullyRegistered),
			params.NewRoutingContext(77),
		)),
		params.NewRegistrationResult(params.NewRegistrationResultPayload(
			params.NewLocalRoutingKeyIdentifier(identifiers[1]),
			params.NewRegistrationStatus(params.SuccessfullyRegistered),
			params.NewRoutingContext(77),
		)),
	)
	if err := association.handleRegistrationResponse(response); !errors.Is(err, ErrInvalidRoutingContext) {
		t.Fatalf("late Registration Response error = %v, want ErrInvalidRoutingContext", err)
	}
	if contexts := association.dynamicRoutingContexts(false); len(contexts) != 0 {
		t.Fatalf("late conflicting Registration Results installed dynamic Routing Contexts: %v", contexts)
	}
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

func TestRKMRequesterBoundsUnresolvedRegistrationOutcomes(t *testing.T) {
	const unresolvedLimit = 1024
	association := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	defer func() { _ = association.Close() }()
	association.muState.Lock()
	association.state = StateASPInactive
	association.muState.Unlock()

	writes := 0
	for attempt := 0; attempt <= unresolvedLimit; attempt++ {
		ctx, cancel := context.WithCancel(context.Background())
		association.signalWriter = func(message messages.M3UA) (int, error) {
			writes++
			cancel()
			return message.MarshalLen(), nil
		}
		_, err := association.RegisterRoutingKeys(ctx, RoutingKeyRegistration{
			RoutingKey: testRoutingKey(10, 100, params.ServiceIndSCCP),
		})
		cancel()
		if attempt < unresolvedLimit {
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Registration attempt %d error = %v, want context.Canceled", attempt+1, err)
			}
			continue
		}
		if !errors.Is(err, ErrRKMOutcomeLimit) {
			t.Fatalf("Registration above unresolved limit error = %v, want ErrRKMOutcomeLimit", err)
		}
	}
	if writes != unresolvedLimit {
		t.Fatalf("Registration writes = %d, want bounded %d", writes, unresolvedLimit)
	}
	association.rkmCorrelationMu.Lock()
	unresolved := len(association.rkmUnresolvedRegistrations)
	association.rkmCorrelationMu.Unlock()
	if unresolved != unresolvedLimit {
		t.Fatalf("unresolved Registrations = %d, want bounded %d", unresolved, unresolvedLimit)
	}
	lateResponse := messages.NewRegistrationResponse(params.NewRegistrationResult(
		params.NewRegistrationResultPayload(
			params.NewLocalRoutingKeyIdentifier(1),
			params.NewRegistrationStatus(params.SuccessfullyRegistered),
			params.NewRoutingContext(77),
		),
	))
	if err := association.handleRegistrationResponse(lateResponse); err != nil {
		t.Fatalf("late Registration Response: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	association.signalWriter = func(message messages.M3UA) (int, error) {
		writes++
		cancel()
		return message.MarshalLen(), nil
	}
	_, err := association.RegisterRoutingKeys(ctx, RoutingKeyRegistration{
		RoutingKey: testRoutingKey(10, 200, params.ServiceIndSCCP),
	})
	cancel()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Registration after late result error = %v, want context.Canceled", err)
	}
	if writes != unresolvedLimit+1 {
		t.Fatalf("Registration writes after late result = %d, want %d", writes, unresolvedLimit+1)
	}
}

func TestRKMRequesterBoundsUnresolvedDeregistrationOutcomes(t *testing.T) {
	const unresolvedLimit = 1024
	association := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	defer func() { _ = association.Close() }()
	association.muState.Lock()
	association.state = StateASPInactive
	association.muState.Unlock()

	writes := 0
	for attempt := 0; attempt <= unresolvedLimit; attempt++ {
		ctx, cancel := context.WithCancel(context.Background())
		association.signalWriter = func(message messages.M3UA) (int, error) {
			writes++
			cancel()
			return message.MarshalLen(), nil
		}
		_, err := association.DeregisterRoutingContexts(ctx, uint32(attempt+1))
		cancel()
		if attempt < unresolvedLimit {
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Deregistration attempt %d error = %v, want context.Canceled", attempt+1, err)
			}
			continue
		}
		if !errors.Is(err, ErrRKMOutcomeLimit) {
			t.Fatalf("Deregistration above unresolved limit error = %v, want ErrRKMOutcomeLimit", err)
		}
	}
	if writes != unresolvedLimit {
		t.Fatalf("Deregistration writes = %d, want bounded %d", writes, unresolvedLimit)
	}
	association.rkmCorrelationMu.Lock()
	unresolved := len(association.rkmUnresolvedDeregistrationRCs)
	association.rkmCorrelationMu.Unlock()
	if unresolved != unresolvedLimit {
		t.Fatalf("unresolved Deregistrations = %d, want bounded %d", unresolved, unresolvedLimit)
	}
	lateResponse := messages.NewDeregistrationResponse(params.NewDeregistrationResult(
		params.NewDeregResultPayload(
			params.NewRoutingContext(1),
			params.NewDeregistrationStatus(params.SuccessfullyDeregistered),
		),
	))
	if err := association.handleDeregistrationResponse(lateResponse); err != nil {
		t.Fatalf("late Deregistration Response: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	association.signalWriter = func(message messages.M3UA) (int, error) {
		writes++
		cancel()
		return message.MarshalLen(), nil
	}
	_, err := association.DeregisterRoutingContexts(ctx, unresolvedLimit+2)
	cancel()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Deregistration after late result error = %v, want context.Canceled", err)
	}
	if writes != unresolvedLimit+1 {
		t.Fatalf("Deregistration writes after late result = %d, want %d", writes, unresolvedLimit+1)
	}
}

func TestRKMRequesterSharesUnresolvedOutcomeBudget(t *testing.T) {
	association := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	association.rkmUnresolvedRegistrations = make(map[uint32]RoutingKeyRegistrationRequest, rkmUnresolvedOutcomeLimit/2)
	association.rkmUnresolvedDeregistrationRCs = make(map[uint32]uint64, rkmUnresolvedOutcomeLimit/2)
	for index := 0; index < rkmUnresolvedOutcomeLimit/2; index++ {
		identifier := uint32(index + 1)
		association.rkmUnresolvedRegistrations[identifier] = RoutingKeyRegistrationRequest{
			LocalRoutingKeyIdentifier: identifier,
			RoutingKey:                testRoutingKey(10, uint32(index+1), params.ServiceIndSCCP),
		}
		association.rkmUnresolvedDeregistrationRCs[uint32(index+1)] = 1
	}
	if _, err := association.beginRegistrationResponseCorrelation(
		map[uint32]int{rkmUnresolvedOutcomeLimit + 1: 0},
		map[uint32]RoutingKeyRegistrationRequest{
			rkmUnresolvedOutcomeLimit + 1: {
				LocalRoutingKeyIdentifier: rkmUnresolvedOutcomeLimit + 1,
				RoutingKey:                testRoutingKey(10, 2000, params.ServiceIndSCCP),
			},
		},
	); !errors.Is(err, ErrRKMOutcomeLimit) {
		t.Fatalf("combined unresolved outcome error = %v, want ErrRKMOutcomeLimit", err)
	}
}

func TestRKMRequesterIgnoresStaleRegistrationResponseAfterCancellation(t *testing.T) {
	association := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	association.muState.Lock()
	association.state = StateASPInactive
	association.muState.Unlock()
	written := make(chan uint32, 2)
	association.signalWriter = func(message messages.M3UA) (int, error) {
		request := message.(*messages.RegistrationRequest)
		payload, err := request.RoutingKeys[0].RoutingKey()
		if err != nil {
			return 0, err
		}
		written <- payload.LocalRoutingKeyIdentifier.LocalRoutingKeyIdentifier()
		return message.MarshalLen(), nil
	}

	firstContext, cancelFirst := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() {
		_, err := association.RegisterRoutingKeys(firstContext, RoutingKeyRegistration{
			RoutingKey: testRoutingKey(10, 100, params.ServiceIndSCCP),
		})
		firstDone <- err
	}()
	firstIdentifier := <-written
	cancelFirst()
	if err := <-firstDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("first RegisterRoutingKeys error = %v, want context.Canceled", err)
	}

	type registrationAnswer struct {
		results []RoutingKeyRegistrationResult
		err     error
	}
	secondDone := make(chan registrationAnswer, 1)
	go func() {
		results, err := association.RegisterRoutingKeys(context.Background(), RoutingKeyRegistration{
			RoutingKey: testRoutingKey(20, 200, params.ServiceIndISUP),
		})
		secondDone <- registrationAnswer{results: results, err: err}
	}()
	secondIdentifier := <-written
	if secondIdentifier == firstIdentifier {
		t.Fatalf("Registration procedures reused Local RK Identifier %d", secondIdentifier)
	}

	stale := messages.NewRegistrationResponse(params.NewRegistrationResult(
		params.NewRegistrationResultPayload(
			params.NewLocalRoutingKeyIdentifier(firstIdentifier),
			params.NewRegistrationStatus(params.SuccessfullyRegistered),
			params.NewRoutingContext(100),
		),
	))
	if err := association.handleRegistrationResponse(stale); err != nil {
		t.Fatalf("deliver stale Registration Response: %v", err)
	}
	select {
	case answer := <-secondDone:
		t.Fatalf("second Registration completed from stale response: results=%+v error=%v", answer.results, answer.err)
	case <-time.After(50 * time.Millisecond):
	}

	current := messages.NewRegistrationResponse(params.NewRegistrationResult(
		params.NewRegistrationResultPayload(
			params.NewLocalRoutingKeyIdentifier(secondIdentifier),
			params.NewRegistrationStatus(params.SuccessfullyRegistered),
			params.NewRoutingContext(200),
		),
	))
	if err := association.handleRegistrationResponse(current); err != nil {
		t.Fatalf("deliver current Registration Response: %v", err)
	}
	answer := <-secondDone
	if answer.err != nil || len(answer.results) != 1 || answer.results[0].RoutingContext != 200 {
		t.Fatalf("second Registration result = %+v, error = %v", answer.results, answer.err)
	}
}

func TestRKMRequesterIgnoresStaleDeregistrationResponseAfterCancellation(t *testing.T) {
	association := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	association.muState.Lock()
	association.state = StateASPInactive
	association.muState.Unlock()
	written := make(chan uint32, 2)
	association.signalWriter = func(message messages.M3UA) (int, error) {
		request := message.(*messages.DeregistrationRequest)
		written <- request.RoutingContext.RoutingContext()
		return message.MarshalLen(), nil
	}

	firstContext, cancelFirst := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() {
		_, err := association.DeregisterRoutingContexts(firstContext, 100)
		firstDone <- err
	}()
	if got := <-written; got != 100 {
		t.Fatalf("first Deregistration Request Routing Context = %d, want 100", got)
	}
	cancelFirst()
	if err := <-firstDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("first DeregisterRoutingContexts error = %v, want context.Canceled", err)
	}

	type deregistrationAnswer struct {
		results []RoutingKeyDeregistrationResult
		err     error
	}
	secondDone := make(chan deregistrationAnswer, 1)
	go func() {
		results, err := association.DeregisterRoutingContexts(context.Background(), 200)
		secondDone <- deregistrationAnswer{results: results, err: err}
	}()
	if got := <-written; got != 200 {
		t.Fatalf("second Deregistration Request Routing Context = %d, want 200", got)
	}

	stale := messages.NewDeregistrationResponse(params.NewDeregistrationResult(
		params.NewDeregResultPayload(
			params.NewRoutingContext(100),
			params.NewDeregistrationStatus(params.SuccessfullyDeregistered),
		),
	))
	if err := association.handleDeregistrationResponse(stale); err != nil {
		t.Fatalf("deliver stale Deregistration Response: %v", err)
	}
	select {
	case answer := <-secondDone:
		t.Fatalf("second Deregistration completed from stale response: results=%+v error=%v", answer.results, answer.err)
	case <-time.After(50 * time.Millisecond):
	}

	current := messages.NewDeregistrationResponse(params.NewDeregistrationResult(
		params.NewDeregResultPayload(
			params.NewRoutingContext(200),
			params.NewDeregistrationStatus(params.SuccessfullyDeregistered),
		),
	))
	if err := association.handleDeregistrationResponse(current); err != nil {
		t.Fatalf("deliver current Deregistration Response: %v", err)
	}
	answer := <-secondDone
	if answer.err != nil || len(answer.results) != 1 || answer.results[0].RoutingContext != 200 {
		t.Fatalf("second Deregistration result = %+v, error = %v", answer.results, answer.err)
	}
}

func TestRKMRequesterRejectsAmbiguousDeregistrationRetryAfterCancellation(t *testing.T) {
	association := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	association.muState.Lock()
	association.state = StateASPInactive
	association.muState.Unlock()
	written := make(chan struct{}, 2)
	association.signalWriter = func(message messages.M3UA) (int, error) {
		written <- struct{}{}
		return message.MarshalLen(), nil
	}

	firstContext, cancelFirst := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() {
		_, err := association.DeregisterRoutingContexts(firstContext, 100)
		firstDone <- err
	}()
	<-written
	cancelFirst()
	if err := <-firstDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("first DeregisterRoutingContexts error = %v, want context.Canceled", err)
	}

	retryContext, cancelRetry := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelRetry()
	_, err := association.DeregisterRoutingContexts(retryContext, 100)
	if !errors.Is(err, ErrDeregistrationOutcomeUnknown) {
		t.Fatalf("ambiguous retry error = %v, want ErrDeregistrationOutcomeUnknown", err)
	}
	select {
	case <-written:
		t.Fatal("ambiguous Deregistration retry was written")
	default:
	}

	stale := messages.NewDeregistrationResponse(params.NewDeregistrationResult(
		params.NewDeregResultPayload(
			params.NewRoutingContext(100),
			params.NewDeregistrationStatus(params.SuccessfullyDeregistered),
		),
	))
	if err := association.handleDeregistrationResponse(stale); err != nil {
		t.Fatalf("deliver stale Deregistration Response: %v", err)
	}

	retryDone := make(chan error, 1)
	go func() {
		_, err := association.DeregisterRoutingContexts(context.Background(), 100)
		retryDone <- err
	}()
	<-written
	if err := association.handleDeregistrationResponse(stale); err != nil {
		t.Fatalf("deliver retried Deregistration Response: %v", err)
	}
	if err := <-retryDone; err != nil {
		t.Fatalf("retry after resolving the stale result: %v", err)
	}
}

func TestRKMLateSuccessfulDeregistrationRemovesDynamicKey(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleIPSP})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	defer func() { _ = endpoint.Close() }()
	config := NewAssociationConfig(0, 0, 0, 0, 0, 0)
	config.IPSP = &IPSPConfig{ExchangeModel: IPSPExchangeSingle}
	association := newAssociation(RoleIPSP, config)
	association.as = endpoint.as
	association.muState.Lock()
	association.state = StateASPInactive
	association.muState.Unlock()
	if !endpoint.trackAssociation(association) {
		t.Fatal("trackAssociation returned false")
	}
	key := ASKey{
		NetworkAppearance:    10,
		NetworkAppearanceSet: true,
		RoutingContext:       100,
		RoutingContextSet:    true,
	}
	written := make(chan struct{}, 1)
	association.signalWriter = func(message messages.M3UA) (int, error) {
		if _, deregistration := message.(*messages.DeregistrationRequest); deregistration {
			written <- struct{}{}
		}
		return message.MarshalLen(), nil
	}
	association.addDynamicASKey(key, testRoutingKey(10, 100, params.ServiceIndSCCP), false)
	endpoint.as.registerDynamicASP(association, key)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := association.DeregisterRoutingContexts(ctx, 100)
		done <- err
	}()
	<-written
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("DeregisterRoutingContexts error = %v, want context.Canceled", err)
	}
	if _, ok := association.dynamicASKey(100, false); !ok {
		t.Fatal("dynamic ASKey removed before the unresolved response arrived")
	}

	response := messages.NewDeregistrationResponse(params.NewDeregistrationResult(
		params.NewDeregResultPayload(
			params.NewRoutingContext(100),
			params.NewDeregistrationStatus(params.SuccessfullyDeregistered),
		),
	))
	if err := association.handleDeregistrationResponse(response); err != nil {
		t.Fatalf("deliver late Deregistration Response: %v", err)
	}
	if key, ok := association.dynamicASKey(100, false); ok {
		t.Fatalf("dynamic ASKey remains after successful late Deregistration Response: %+v", key)
	}
	if _, ok := endpoint.as.lookup(key); ok {
		t.Fatal("late successful Deregistration Response retained the requester-created Application Server")
	}
}

func TestRKMLateSuccessfulDeregistrationPreservesReregisteredDynamicKey(t *testing.T) {
	association := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	association.notificationWriter = func(message messages.M3UA) (int, error) {
		return message.MarshalLen(), nil
	}
	association.as = newApplicationServers(time.Hour)
	association.muState.Lock()
	association.state = StateASPInactive
	association.muState.Unlock()
	key := ASKey{
		NetworkAppearance:    10,
		NetworkAppearanceSet: true,
		RoutingContext:       100,
		RoutingContextSet:    true,
	}
	routingKey := testRoutingKey(10, 100, params.ServiceIndSCCP)
	association.addDynamicASKey(key, routingKey, false)
	association.as.registerDynamicASP(association, key)

	deregistrationWritten := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	association.signalWriter = func(message messages.M3UA) (int, error) {
		if _, ok := message.(*messages.DeregistrationRequest); ok {
			deregistrationWritten <- struct{}{}
		}
		return message.MarshalLen(), nil
	}
	deregistrationDone := make(chan error, 1)
	go func() {
		_, err := association.DeregisterRoutingContexts(ctx, 100)
		deregistrationDone <- err
	}()
	<-deregistrationWritten
	cancel()
	if err := <-deregistrationDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("DeregisterRoutingContexts error = %v, want context.Canceled", err)
	}

	association.signalWriter = func(message messages.M3UA) (int, error) {
		request, ok := message.(*messages.RegistrationRequest)
		if !ok {
			return message.MarshalLen(), nil
		}
		payload, err := request.RoutingKeys[0].RoutingKey()
		if err != nil {
			return 0, err
		}
		identifier := payload.LocalRoutingKeyIdentifier.LocalRoutingKeyIdentifier()
		return message.MarshalLen(), association.handleRegistrationResponse(messages.NewRegistrationResponse(
			params.NewRegistrationResult(params.NewRegistrationResultPayload(
				params.NewLocalRoutingKeyIdentifier(identifier),
				params.NewRegistrationStatus(params.SuccessfullyRegistered),
				params.NewRoutingContext(100),
			)),
		))
	}
	if _, err := association.RegisterRoutingKeys(context.Background(), RoutingKeyRegistration{RoutingKey: routingKey}); err != nil {
		t.Fatalf("RegisterRoutingKeys: %v", err)
	}

	late := messages.NewDeregistrationResponse(params.NewDeregistrationResult(
		params.NewDeregResultPayload(
			params.NewRoutingContext(100),
			params.NewDeregistrationStatus(params.SuccessfullyDeregistered),
		),
	))
	if err := association.handleDeregistrationResponse(late); err != nil {
		t.Fatalf("late Deregistration Response: %v", err)
	}
	if current, ok := association.dynamicASKey(100, false); !ok || current != key {
		t.Fatalf("re-registered dynamic ASKey = %+v, %v; want %+v, true", current, ok, key)
	}
	applicationServer, ok := association.as.lookup(key)
	if !ok {
		t.Fatal("late Deregistration Response removed the re-registered Application Server")
	}
	applicationServer.mu.Lock()
	_, member := applicationServer.asps[association]
	applicationServer.mu.Unlock()
	if !member {
		t.Fatal("late Deregistration Response removed the re-registered ASP membership")
	}
}

func TestRKMLateDeregistrationCleanupSerializesWithRegistrationPublication(t *testing.T) {
	association := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	association.notificationWriter = func(message messages.M3UA) (int, error) {
		return message.MarshalLen(), nil
	}
	association.as = newApplicationServers(time.Hour)
	association.muState.Lock()
	association.state = StateASPInactive
	association.muState.Unlock()
	key := ASKey{
		NetworkAppearance:    10,
		NetworkAppearanceSet: true,
		RoutingContext:       100,
		RoutingContextSet:    true,
	}
	routingKey := testRoutingKey(10, 100, params.ServiceIndSCCP)
	association.addDynamicASKey(key, routingKey, false)
	association.as.registerDynamicASP(association, key)
	version := association.dynamicASKeyVersionFor(100, false)
	association.rkmUnresolvedDeregistrationRCs = map[uint32]uint64{100: version}
	applicationServer, ok := association.as.lookup(key)
	if !ok {
		t.Fatal("dynamic Application Server was not created")
	}
	applicationServer.mu.Lock()
	locked := true
	defer func() {
		if locked {
			applicationServer.mu.Unlock()
		}
	}()

	lateDone := make(chan error, 1)
	go func() {
		lateDone <- association.handleDeregistrationResponse(messages.NewDeregistrationResponse(
			params.NewDeregistrationResult(params.NewDeregResultPayload(
				params.NewRoutingContext(100),
				params.NewDeregistrationStatus(params.SuccessfullyDeregistered),
			)),
		))
	}()
	if !waitFor(func() bool {
		_, exists := association.dynamicASKey(100, false)
		return !exists
	}, time.Second) {
		t.Fatal("late Deregistration Response did not begin cleanup")
	}
	if associationEnded(association) {
		t.Fatal("association ended while stale cleanup was blocked")
	}

	registrationDone := make(chan error, 1)
	go func() {
		registrationDone <- association.applyRegistrationResults([]registrationResultApplication{{
			request: RoutingKeyRegistrationRequest{RoutingKey: routingKey},
			result: RoutingKeyRegistrationResult{
				Status:         RegistrationSuccessfullyRegistered,
				RoutingContext: 100,
			},
		}})
	}()
	time.Sleep(100 * time.Millisecond)
	if current, exists := association.dynamicASKey(100, false); exists {
		t.Fatalf("registration published during stale cleanup: %+v", current)
	}

	applicationServer.mu.Unlock()
	locked = false
	if err := <-lateDone; err != nil {
		t.Fatalf("late Deregistration Response: %v", err)
	}
	if associationEnded(association) {
		t.Fatal("association ended during stale cleanup")
	}
	if err := <-registrationDone; err != nil {
		t.Fatalf("applyRegistrationResults: %v", err)
	}
	if current, exists := association.dynamicASKey(100, false); !exists || current != key {
		t.Fatalf("registered dynamic ASKey = %+v, %v; want %+v, true", current, exists, key)
	}
	currentApplicationServer, ok := association.as.lookup(key)
	if !ok {
		t.Fatal("registration publication did not recreate the Application Server")
	}
	currentApplicationServer.mu.Lock()
	_, member := currentApplicationServer.asps[association]
	currentApplicationServer.mu.Unlock()
	if !member {
		t.Fatal("registration publication did not restore ASP membership")
	}
}

func TestRKMLateSuccessfulRegistrationAddsDynamicKey(t *testing.T) {
	association := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	association.muState.Lock()
	association.state = StateASPInactive
	association.muState.Unlock()
	written := make(chan uint32, 1)
	association.signalWriter = func(message messages.M3UA) (int, error) {
		request := message.(*messages.RegistrationRequest)
		payload, err := request.RoutingKeys[0].RoutingKey()
		if err != nil {
			return 0, err
		}
		written <- payload.LocalRoutingKeyIdentifier.LocalRoutingKeyIdentifier()
		return message.MarshalLen(), nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := association.RegisterRoutingKeys(ctx, RoutingKeyRegistration{
			RoutingKey: testRoutingKey(10, 100, params.ServiceIndSCCP),
		})
		done <- err
	}()
	identifier := <-written
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("RegisterRoutingKeys error = %v, want context.Canceled", err)
	}

	response := messages.NewRegistrationResponse(params.NewRegistrationResult(
		params.NewRegistrationResultPayload(
			params.NewLocalRoutingKeyIdentifier(identifier),
			params.NewRegistrationStatus(params.SuccessfullyRegistered),
			params.NewRoutingContext(77),
		),
	))
	if err := association.handleRegistrationResponse(response); err != nil {
		t.Fatalf("deliver late Registration Response: %v", err)
	}
	want := ASKey{
		NetworkAppearance:    10,
		NetworkAppearanceSet: true,
		RoutingContext:       77,
		RoutingContextSet:    true,
	}
	if key, ok := association.dynamicASKey(77, false); !ok || key != want {
		t.Fatalf("late Registration dynamic ASKey = %+v, %t; want %+v", key, ok, want)
	}
}

func TestRKMCanceledRegistrationAppliesAlreadyDeliveredSuccess(t *testing.T) {
	association := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	request := RoutingKeyRegistrationRequest{
		LocalRoutingKeyIdentifier: 1,
		RoutingKey:                testRoutingKey(10, 100, params.ServiceIndSCCP),
	}
	if _, err := association.beginRegistrationResponseCorrelation(
		map[uint32]int{1: 0},
		map[uint32]RoutingKeyRegistrationRequest{1: request},
	); err != nil {
		t.Fatalf("beginRegistrationResponseCorrelation: %v", err)
	}
	response := messages.NewRegistrationResponse(params.NewRegistrationResult(
		params.NewRegistrationResultPayload(
			params.NewLocalRoutingKeyIdentifier(1),
			params.NewRegistrationStatus(params.SuccessfullyRegistered),
			params.NewRoutingContext(77),
		),
	))
	if err := association.deliverRegistrationResponse(response); err != nil {
		t.Fatalf("deliverRegistrationResponse: %v", err)
	}

	association.endRegistrationResponseCorrelation(true, false)
	if key, ok := association.dynamicASKey(77, false); !ok || key.RoutingContext != 77 {
		t.Fatalf("known successful Registration dynamic ASKey = %+v, %t; want Routing Context 77", key, ok)
	}
}

func TestRKMCanceledRegistrationRejectsConflictingDeliveredScopesAtomically(t *testing.T) {
	association := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	requests := map[uint32]RoutingKeyRegistrationRequest{
		1: {
			LocalRoutingKeyIdentifier: 1,
			RoutingKey:                testRoutingKey(10, 100, params.ServiceIndSCCP),
		},
		2: {
			LocalRoutingKeyIdentifier: 2,
			RoutingKey:                testRoutingKey(20, 200, params.ServiceIndISUP),
		},
	}
	if _, err := association.beginRegistrationResponseCorrelation(
		map[uint32]int{1: 0, 2: 1},
		requests,
	); err != nil {
		t.Fatalf("beginRegistrationResponseCorrelation: %v", err)
	}
	response := messages.NewRegistrationResponse(
		params.NewRegistrationResult(params.NewRegistrationResultPayload(
			params.NewLocalRoutingKeyIdentifier(1),
			params.NewRegistrationStatus(params.SuccessfullyRegistered),
			params.NewRoutingContext(77),
		)),
		params.NewRegistrationResult(params.NewRegistrationResultPayload(
			params.NewLocalRoutingKeyIdentifier(2),
			params.NewRegistrationStatus(params.SuccessfullyRegistered),
			params.NewRoutingContext(77),
		)),
	)
	if err := association.deliverRegistrationResponse(response); err != nil {
		t.Fatalf("deliverRegistrationResponse: %v", err)
	}

	association.endRegistrationResponseCorrelation(true, false)
	if contexts := association.dynamicRoutingContexts(false); len(contexts) != 0 {
		t.Fatalf("canceled conflicting Registration Results installed dynamic Routing Contexts: %v", contexts)
	}
}

func TestRKMLateDeregistrationResponseIsAppliedAtomically(t *testing.T) {
	association := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	association.muState.Lock()
	association.state = StateASPInactive
	association.muState.Unlock()
	association.addDynamicASKey(ASKey{
		RoutingContext:    100,
		RoutingContextSet: true,
	}, testRoutingKey(10, 100, params.ServiceIndSCCP), false)
	written := make(chan struct{}, 1)
	association.signalWriter = func(message messages.M3UA) (int, error) {
		written <- struct{}{}
		return message.MarshalLen(), nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := association.DeregisterRoutingContexts(ctx, 100)
		done <- err
	}()
	<-written
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("DeregisterRoutingContexts error = %v, want context.Canceled", err)
	}

	result := func(routingContext uint32) *params.Param {
		return params.NewDeregistrationResult(params.NewDeregResultPayload(
			params.NewRoutingContext(routingContext),
			params.NewDeregistrationStatus(params.SuccessfullyDeregistered),
		))
	}
	denied := params.NewDeregistrationResult(params.NewDeregResultPayload(
		params.NewRoutingContext(100),
		params.NewDeregistrationStatus(params.DeregPermissionDenied),
	))
	if err := association.handleDeregistrationResponse(
		messages.NewDeregistrationResponse(result(100), denied),
	); !errors.Is(err, ErrInvalidParameterValue) {
		t.Fatalf("conflicting late Deregistration Response error = %v, want ErrInvalidParameterValue", err)
	}
	if _, ok := association.dynamicASKey(100, false); !ok {
		t.Fatal("conflicting late response removed the dynamic ASKey")
	}
	if _, err := association.beginDeregistrationResponseCorrelation(map[uint32]int{100: 0}); !errors.Is(err, ErrDeregistrationOutcomeUnknown) {
		t.Fatalf("conflicting late response cleared unresolved outcome: %v", err)
	}
	if err := association.handleDeregistrationResponse(
		messages.NewDeregistrationResponse(result(100), result(999)),
	); err == nil {
		t.Fatal("mixed late Deregistration Response accepted")
	}
	if _, ok := association.dynamicASKey(100, false); !ok {
		t.Fatal("mixed late response partially removed the dynamic ASKey")
	}
	if _, err := association.beginDeregistrationResponseCorrelation(map[uint32]int{100: 0}); !errors.Is(err, ErrDeregistrationOutcomeUnknown) {
		t.Fatalf("mixed late response cleared unresolved outcome: %v", err)
	}

	if err := association.handleDeregistrationResponse(
		messages.NewDeregistrationResponse(result(100)),
	); err != nil {
		t.Fatalf("deliver valid late Deregistration Response: %v", err)
	}
	if _, ok := association.dynamicASKey(100, false); ok {
		t.Fatal("valid late response did not remove the dynamic ASKey")
	}
}

func TestRKMDeregistrationWriteFailureDoesNotMakeOutcomeUnknown(t *testing.T) {
	association := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	association.muState.Lock()
	association.state = StateASPInactive
	association.muState.Unlock()
	writeErr := errors.New("write failed before transmission")
	association.signalWriter = func(messages.M3UA) (int, error) {
		return 0, writeErr
	}

	for attempt := range 2 {
		_, err := association.DeregisterRoutingContexts(context.Background(), 100)
		if !errors.Is(err, writeErr) {
			t.Fatalf("attempt %d error = %v, want write failure", attempt+1, err)
		}
		if errors.Is(err, ErrDeregistrationOutcomeUnknown) {
			t.Fatalf("attempt %d treated an unwritten request as unresolved", attempt+1)
		}
	}
}

func TestRKMCanceledDeregistrationAppliesAlreadyDeliveredSuccess(t *testing.T) {
	association := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	association.addDynamicASKey(ASKey{
		RoutingContext:    100,
		RoutingContextSet: true,
	}, testRoutingKey(10, 100, params.ServiceIndSCCP), false)
	pending := map[uint32]int{100: 0}
	if _, err := association.beginDeregistrationResponseCorrelation(pending); err != nil {
		t.Fatalf("beginDeregistrationResponseCorrelation: %v", err)
	}
	response := messages.NewDeregistrationResponse(params.NewDeregistrationResult(
		params.NewDeregResultPayload(
			params.NewRoutingContext(100),
			params.NewDeregistrationStatus(params.SuccessfullyDeregistered),
		),
	))
	if err := association.deliverDeregistrationResponse(response); err != nil {
		t.Fatalf("deliverDeregistrationResponse: %v", err)
	}

	association.endDeregistrationResponseCorrelation(true)
	if _, exists := association.dynamicASKey(100, false); exists {
		t.Fatal("known successful Deregistration remained configured after cancellation")
	}
}

func TestRKMRequesterIgnoresDuplicateDeregistrationResults(t *testing.T) {
	association := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	association.muState.Lock()
	association.state = StateASPInactive
	association.muState.Unlock()
	written := make(chan struct{}, 1)
	association.signalWriter = func(message messages.M3UA) (int, error) {
		written <- struct{}{}
		return message.MarshalLen(), nil
	}

	type deregistrationAnswer struct {
		results []RoutingKeyDeregistrationResult
		err     error
	}
	done := make(chan deregistrationAnswer, 1)
	go func() {
		results, err := association.DeregisterRoutingContexts(context.Background(), 100, 200)
		done <- deregistrationAnswer{results: results, err: err}
	}()
	<-written

	resultFor := func(routingContext uint32) *params.Param {
		return params.NewDeregistrationResult(params.NewDeregResultPayload(
			params.NewRoutingContext(routingContext),
			params.NewDeregistrationStatus(params.SuccessfullyDeregistered),
		))
	}
	if err := association.handleDeregistrationResponse(messages.NewDeregistrationResponse(
		resultFor(100), resultFor(100),
	)); err != nil {
		t.Fatalf("deliver duplicate Deregistration Results: %v", err)
	}
	select {
	case answer := <-done:
		t.Fatalf("Deregistration completed before Routing Context 200: results=%+v error=%v", answer.results, answer.err)
	case <-time.After(50 * time.Millisecond):
	}
	if err := association.handleDeregistrationResponse(messages.NewDeregistrationResponse(
		resultFor(200),
	)); err != nil {
		t.Fatalf("deliver remaining Deregistration Result: %v", err)
	}
	answer := <-done
	if answer.err != nil {
		t.Fatalf("DeregisterRoutingContexts: %v", answer.err)
	}
	if len(answer.results) != 2 || answer.results[0].RoutingContext != 100 || answer.results[1].RoutingContext != 200 {
		t.Fatalf("Deregistration results = %+v, want Routing Contexts 100 and 200", answer.results)
	}
}

func TestRKMRequesterRejectsContradictoryPreviouslyDeliveredDeregistrationResult(t *testing.T) {
	association := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	responses, err := association.beginDeregistrationResponseCorrelation(map[uint32]int{100: 0, 200: 1})
	if err != nil {
		t.Fatalf("beginDeregistrationResponseCorrelation: %v", err)
	}
	t.Cleanup(func() { association.endDeregistrationResponseCorrelation(false) })
	result := func(routingContext, status uint32) *params.Param {
		return params.NewDeregistrationResult(params.NewDeregResultPayload(
			params.NewRoutingContext(routingContext),
			params.NewDeregistrationStatus(status),
		))
	}
	if err := association.deliverDeregistrationResponse(messages.NewDeregistrationResponse(
		result(100, params.SuccessfullyDeregistered),
	)); err != nil {
		t.Fatalf("deliver first Deregistration Result: %v", err)
	}
	<-responses
	if err := association.deliverDeregistrationResponse(messages.NewDeregistrationResponse(
		result(100, params.DeregPermissionDenied),
	)); !errors.Is(err, ErrInvalidParameterValue) {
		t.Fatalf("contradictory delivered Deregistration Result error = %v, want ErrInvalidParameterValue", err)
	}
	association.rkmCorrelationMu.Lock()
	delivered := association.rkmDeliveredDeregistrationStatus[100]
	_, secondPending := association.rkmPendingDeregistrationRCs[200]
	association.rkmCorrelationMu.Unlock()
	if delivered != DeregistrationSuccessfullyDeregistered {
		t.Fatalf("first delivered Deregistration Status changed to %d", delivered)
	}
	if !secondPending {
		t.Fatal("contradictory delivered Deregistration Result cleared another pending Routing Context")
	}
}

func TestRKMRequesterRejectsContradictoryDeregistrationResults(t *testing.T) {
	association := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	association.muState.Lock()
	association.state = StateASPInactive
	association.muState.Unlock()
	written := make(chan struct{}, 1)
	association.signalWriter = func(message messages.M3UA) (int, error) {
		written <- struct{}{}
		return message.MarshalLen(), nil
	}

	type deregistrationAnswer struct {
		results []RoutingKeyDeregistrationResult
		err     error
	}
	done := make(chan deregistrationAnswer, 1)
	go func() {
		results, err := association.DeregisterRoutingContexts(context.Background(), 100, 200)
		done <- deregistrationAnswer{results: results, err: err}
	}()
	<-written

	resultFor := func(routingContext, status uint32) *params.Param {
		return params.NewDeregistrationResult(params.NewDeregResultPayload(
			params.NewRoutingContext(routingContext),
			params.NewDeregistrationStatus(status),
		))
	}
	if err := association.handleDeregistrationResponse(messages.NewDeregistrationResponse(
		resultFor(100, params.SuccessfullyDeregistered),
		resultFor(100, params.DeregPermissionDenied),
	)); !errors.Is(err, ErrInvalidParameterValue) {
		t.Fatalf("conflicting Deregistration Response error = %v, want ErrInvalidParameterValue", err)
	}
	select {
	case answer := <-done:
		t.Fatalf("Deregistration completed after conflicting results: results=%+v error=%v", answer.results, answer.err)
	case <-time.After(50 * time.Millisecond):
	}
	if err := association.handleDeregistrationResponse(messages.NewDeregistrationResponse(
		resultFor(100, params.SuccessfullyDeregistered),
		resultFor(200, params.SuccessfullyDeregistered),
	)); err != nil {
		t.Fatalf("deliver valid Deregistration Results: %v", err)
	}
	answer := <-done
	if answer.err != nil {
		t.Fatalf("DeregisterRoutingContexts: %v", answer.err)
	}
	if len(answer.results) != 2 || answer.results[0].RoutingContext != 100 || answer.results[1].RoutingContext != 200 {
		t.Fatalf("Deregistration results = %+v, want Routing Contexts 100 and 200", answer.results)
	}
}

func TestRKMRequesterDropsStaleRegistrationResponseBeforeQueueing(t *testing.T) {
	association := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	association.muState.Lock()
	association.state = StateASPInactive
	association.muState.Unlock()
	written := make(chan uint32, 2)
	releaseSecondWrite := make(chan struct{})
	writeCount := 0
	association.signalWriter = func(message messages.M3UA) (int, error) {
		request := message.(*messages.RegistrationRequest)
		payload, err := request.RoutingKeys[0].RoutingKey()
		if err != nil {
			return 0, err
		}
		writeCount++
		written <- payload.LocalRoutingKeyIdentifier.LocalRoutingKeyIdentifier()
		if writeCount == 2 {
			<-releaseSecondWrite
		}
		return message.MarshalLen(), nil
	}

	firstContext, cancelFirst := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() {
		_, err := association.RegisterRoutingKeys(firstContext, RoutingKeyRegistration{
			RoutingKey: testRoutingKey(10, 100, params.ServiceIndSCCP),
		})
		firstDone <- err
	}()
	firstIdentifier := <-written
	cancelFirst()
	if err := <-firstDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("first RegisterRoutingKeys error = %v, want context.Canceled", err)
	}

	secondContext, cancelSecond := context.WithCancel(context.Background())
	defer cancelSecond()
	type registrationAnswer struct {
		results []RoutingKeyRegistrationResult
		err     error
	}
	secondDone := make(chan registrationAnswer, 1)
	go func() {
		results, err := association.RegisterRoutingKeys(secondContext, RoutingKeyRegistration{
			RoutingKey: testRoutingKey(20, 200, params.ServiceIndISUP),
		})
		secondDone <- registrationAnswer{results: results, err: err}
	}()
	secondIdentifier := <-written

	stale := messages.NewRegistrationResponse(params.NewRegistrationResult(
		params.NewRegistrationResultPayload(
			params.NewLocalRoutingKeyIdentifier(firstIdentifier),
			params.NewRegistrationStatus(params.SuccessfullyRegistered),
			params.NewRoutingContext(100),
		),
	))
	staleErr := association.handleRegistrationResponse(stale)
	current := messages.NewRegistrationResponse(params.NewRegistrationResult(
		params.NewRegistrationResultPayload(
			params.NewLocalRoutingKeyIdentifier(secondIdentifier),
			params.NewRegistrationStatus(params.SuccessfullyRegistered),
			params.NewRoutingContext(200),
		),
	))
	currentErr := association.handleRegistrationResponse(current)
	close(releaseSecondWrite)

	if staleErr != nil {
		t.Fatalf("deliver stale Registration Response: %v", staleErr)
	}
	if currentErr != nil {
		t.Fatalf("deliver current Registration Response: %v", currentErr)
	}
	answer := <-secondDone
	if answer.err != nil || len(answer.results) != 1 || answer.results[0].RoutingContext != 200 {
		t.Fatalf("second Registration result = %+v, error = %v", answer.results, answer.err)
	}
}

func TestLocalRoutingKeyIdentifierIssuedClassificationWraps(t *testing.T) {
	association := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	association.rkmCorrelationMu.Lock()
	association.rkmNextLocalID = 2
	association.rkmCorrelationMu.Unlock()
	for _, test := range []struct {
		identifier uint32
		want       bool
	}{
		{identifier: 0, want: false},
		{identifier: 1, want: true},
		{identifier: 2, want: true},
		{identifier: 999, want: false},
	} {
		if got := association.localRoutingKeyIdentifierWasIssued(test.identifier); got != test.want {
			t.Fatalf("identifier %d issued = %t, want %t", test.identifier, got, test.want)
		}
	}

	association.rkmCorrelationMu.Lock()
	association.rkmNextLocalID = 1
	association.rkmCorrelationMu.Unlock()
	if !association.localRoutingKeyIdentifierWasIssued(^uint32(0)) {
		t.Fatal("identifier immediately before uint32 wrap was not classified as issued")
	}
	if association.localRoutingKeyIdentifierWasIssued(2) {
		t.Fatal("identifier immediately after current sequence was classified as issued")
	}
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

func TestDynamicRoutingKeyNetworkAppearanceScopesMultiContextSSNM(t *testing.T) {
	association := newAssociation(RoleSGP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	association.cfg.NetworkAppearance = params.NewNetworkAppearance(7)
	for _, routingContext := range []uint32{9, 10} {
		association.addDynamicASKey(ASKey{
			NetworkAppearance:    10,
			NetworkAppearanceSet: true,
			RoutingContext:       routingContext,
			RoutingContextSet:    true,
		}, testRoutingKey(10, routingContext+100, params.ServiceIndSCCP), false)
	}

	contexts := params.NewRoutingContext(9, 10)
	if err := association.validateSSNMNetworkAppearance(params.NewNetworkAppearance(10), contexts); err != nil {
		t.Fatalf("matching multi-context dynamic Network Appearance error = %v", err)
	}
	if err := association.validateSSNMNetworkAppearance(params.NewNetworkAppearance(7), contexts); !errors.Is(err, ErrInvalidNetworkAppearance) {
		t.Fatalf("static Network Appearance for multi-context dynamic Routing Keys error = %v, want ErrInvalidNetworkAppearance", err)
	}
	association.as = newApplicationServers(time.Hour)
	association.signalWriter = func(message messages.M3UA) (int, error) {
		return message.MarshalLen(), nil
	}
	association.muState.Lock()
	association.state = StateASPActive
	association.muState.Unlock()
	association.noteRoutingContextsActive([]uint32{9, 10})
	for _, routingContext := range []uint32{9, 10} {
		key, _ := association.dynamicASKey(routingContext, false)
		association.as.get(key).setASPState(association, StateASPActive, time.Hour)
	}
	if _, err := association.WriteSignal(messages.NewSignallingCongestion(
		params.NewNetworkAppearance(10),
		contexts,
		params.NewAffectedPointCode(100),
		nil,
		nil,
		nil,
	)); err != nil {
		t.Fatalf("outbound multi-context SCON: %v", err)
	}

	association.addDynamicASKey(ASKey{
		NetworkAppearance:    20,
		NetworkAppearanceSet: true,
		RoutingContext:       11,
		RoutingContextSet:    true,
	}, testRoutingKey(20, 211, params.ServiceIndSCCP), false)
	if err := association.validateSSNMNetworkAppearance(
		params.NewNetworkAppearance(10), params.NewRoutingContext(9, 11),
	); !errors.Is(err, ErrInvalidNetworkAppearance) {
		t.Fatalf("mixed dynamic Network Appearances error = %v, want ErrInvalidNetworkAppearance", err)
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

func TestIPSPDoubleExchangeRKMValidatesRequesterResultsAgainstLocalScope(t *testing.T) {
	newAssociationForResult := func(t *testing.T, routingContext uint32) *Association {
		t.Helper()
		association := newAssociation(RoleIPSP, newDoubleExchangeAssociationConfigForTest())
		association.setIPSPState(IPSPState{TrafficToLocal: StateASPInactive, TrafficToPeer: StateASPDown})
		setRegistrationResultWriter(t, association, params.SuccessfullyRegistered, routingContext)
		return association
	}

	t.Run("RejectLocalCollision", func(t *testing.T) {
		association := newAssociationForResult(t, 11)
		_, err := association.RegisterRoutingKeys(context.Background(), RoutingKeyRegistration{
			RoutingKey: testRoutingKey(20, 200, params.ServiceIndISUP),
		})
		if !errors.Is(err, ErrInvalidRoutingContext) {
			t.Fatalf("RegisterRoutingKeys error = %v, want ErrInvalidRoutingContext", err)
		}
		if contexts := association.dynamicRoutingContexts(true); len(contexts) != 0 {
			t.Fatalf("conflicting local Registration Result installed Routing Contexts: %v", contexts)
		}
	})

	t.Run("AllowPeerDirectionContext", func(t *testing.T) {
		association := newAssociationForResult(t, 22)
		if _, err := association.RegisterRoutingKeys(context.Background(), RoutingKeyRegistration{
			RoutingKey: testRoutingKey(20, 200, params.ServiceIndISUP),
		}); err != nil {
			t.Fatalf("RegisterRoutingKeys: %v", err)
		}
		if contexts := association.dynamicRoutingContexts(true); len(contexts) != 1 || contexts[0] != 22 {
			t.Fatalf("TrafficToLocal dynamic Routing Contexts = %v, want [22]", contexts)
		}
		if contexts := association.dynamicRoutingContexts(false); len(contexts) != 0 {
			t.Fatalf("TrafficToPeer dynamic Routing Contexts = %v, want none", contexts)
		}
	})
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

func TestIPSPSingleExchangeRequesterCleansDynamicApplicationServer(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleIPSP})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	defer func() { _ = endpoint.Close() }()
	config := NewAssociationConfig(0, 0, 0, 0, 0, 0)
	config.IPSP = &IPSPConfig{ExchangeModel: IPSPExchangeSingle}
	association := newAssociation(RoleIPSP, config)
	association.as = endpoint.as
	if !endpoint.trackAssociation(association) {
		t.Fatal("trackAssociation returned false")
	}
	association.muState.Lock()
	association.state = StateASPInactive
	association.muState.Unlock()
	const routingContext = 77
	association.signalWriter = func(message messages.M3UA) (int, error) {
		switch request := message.(type) {
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
						params.NewRoutingContext(routingContext),
					),
				)),
			)
		case *messages.DeregistrationRequest:
			return request.MarshalLen(), association.handleDeregistrationResponse(
				messages.NewDeregistrationResponse(params.NewDeregistrationResult(
					params.NewDeregResultPayload(
						params.NewRoutingContext(routingContext),
						params.NewDeregistrationStatus(params.SuccessfullyDeregistered),
					),
				)),
			)
		case *messages.Notify:
			return message.MarshalLen(), nil
		default:
			return 0, fmt.Errorf("unexpected message %T", message)
		}
	}

	registrations, err := association.RegisterRoutingKeys(context.Background(), RoutingKeyRegistration{
		RoutingKey: testRoutingKey(10, 100, params.ServiceIndSCCP),
	})
	if err != nil {
		t.Fatalf("RegisterRoutingKeys: %v", err)
	}
	if len(registrations) != 1 || registrations[0].Status != RegistrationSuccessfullyRegistered ||
		registrations[0].RoutingContext != routingContext {
		t.Fatalf("Registration results = %+v, want successful Routing Context %d", registrations, routingContext)
	}
	key := ASKey{
		NetworkAppearance:    10,
		NetworkAppearanceSet: true,
		RoutingContext:       routingContext,
		RoutingContextSet:    true,
	}
	if _, err := endpoint.as.agreeTrafficModeForAssociation(association, []uint32{routingContext}, nil); err != nil {
		t.Fatalf("agreeTrafficModeForAssociation: %v", err)
	}
	association.noteRoutingContextsActive([]uint32{routingContext})
	association.commitState(StateASPActive)
	endpoint.as.aspStateChanged(association, StateASPActive)
	applicationServer, ok := endpoint.as.lookup(key)
	if !ok || len(applicationServer.activeASPs()) != 1 {
		t.Fatalf("dynamic Application Server = %v, %t; want one active IPSP", applicationServer, ok)
	}
	association.noteRoutingContextsInactive([]uint32{routingContext})
	association.commitState(StateASPInactive)
	endpoint.as.aspStateChanged(association, StateASPInactive)

	deregistrations, err := association.DeregisterRoutingContexts(context.Background(), routingContext)
	if err != nil {
		t.Fatalf("DeregisterRoutingContexts: %v", err)
	}
	if len(deregistrations) != 1 || deregistrations[0].Status != DeregistrationSuccessfullyDeregistered {
		t.Fatalf("Deregistration results = %+v, want success", deregistrations)
	}
	if _, ok := endpoint.as.lookup(key); ok {
		t.Fatal("successful Deregistration retained the requester-created Application Server")
	}

	registrations, err = association.RegisterRoutingKeys(context.Background(), RoutingKeyRegistration{
		RoutingKey: testRoutingKey(10, 100, params.ServiceIndSCCP),
	})
	if err != nil {
		t.Fatalf("second RegisterRoutingKeys: %v", err)
	}
	if len(registrations) != 1 || registrations[0].Status != RegistrationSuccessfullyRegistered ||
		registrations[0].RoutingContext != routingContext {
		t.Fatalf("second Registration results = %+v, want successful Routing Context %d", registrations, routingContext)
	}
	if _, ok := endpoint.as.lookup(key); !ok {
		t.Fatal("second Registration did not recreate the dynamic Application Server")
	}
	_ = association.Close()
	if _, ok := endpoint.as.lookup(key); ok {
		t.Fatal("Association close retained the requester-created Application Server")
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

func TestRKMRegistrationBatchRemainsReplayableUntilResponseWrite(t *testing.T) {
	const registrationCount = registrationReplayLimit + 1
	endpoint, err := NewEndpoint(EndpointConfig{
		Role: RoleSGP,
		RoutingKeyManagement: &RoutingKeyManagementConfig{
			AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
				return RegistrationSuccessfullyRegistered
			},
			AllowDynamicRoutingKeys: true,
			MaxDynamicRoutingKeys:   registrationCount,
		},
	})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	defer func() { _ = endpoint.Close() }()

	association := newAssociation(RoleSGP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	association.endpoint = endpoint
	association.as = endpoint.as
	association.notificationQueue = make(chan mandatoryControl, registrationCount+1)
	association.muState.Lock()
	association.state = StateASPInactive
	association.muState.Unlock()

	routingKeys := make([]*params.Param, registrationCount)
	for index := range routingKeys {
		routingKeys[index], err = routingKeyParameter(RoutingKeyRegistrationRequest{
			LocalRoutingKeyIdentifier: uint32(index + 1),
			RoutingKey:                testRoutingKey(10, uint32(index+1), params.ServiceIndSCCP),
		})
		if err != nil {
			t.Fatalf("routingKeyParameter %d: %v", index, err)
		}
	}
	request := messages.NewRegistrationRequest(routingKeys...)
	writeErr := errors.New("registration response write failed")
	var responses []*messages.RegistrationResponse
	failResponseWrite := true
	association.signalWriter = func(message messages.M3UA) (int, error) {
		response, ok := message.(*messages.RegistrationResponse)
		if !ok {
			return message.MarshalLen(), nil
		}
		responses = append(responses, response)
		if failResponseWrite {
			failResponseWrite = false
			return 0, writeErr
		}
		return message.MarshalLen(), nil
	}

	if err := association.handleRegistrationRequest(request); !errors.Is(err, writeErr) {
		t.Fatalf("first Registration error = %v, want write failure", err)
	}
	endpoint.routingKeys.mu.Lock()
	pendingState := endpoint.routingKeys.replays[association]
	if pendingState == nil {
		endpoint.routingKeys.mu.Unlock()
		t.Fatalf("pending Registration replay state missing; Association error = %v", association.Err())
	}
	pendingReplayCount := len(pendingState.order)
	pendingResponseCount := len(pendingState.pending)
	endpoint.routingKeys.mu.Unlock()
	if pendingReplayCount != registrationCount || pendingResponseCount != registrationCount {
		t.Fatalf("pending Registration replay state = %d results, %d awaiting response; want %d, %d",
			pendingReplayCount,
			pendingResponseCount,
			registrationCount,
			registrationCount,
		)
	}
	if err := association.handleRegistrationRequest(request); err != nil {
		t.Fatalf("replayed Registration: %v", err)
	}
	if len(responses) != 2 {
		t.Fatalf("Registration Responses = %d, want 2", len(responses))
	}
	for index := range responses[0].RegistrationResults {
		first, err := responses[0].RegistrationResults[index].RegistrationResult()
		if err != nil {
			t.Fatalf("first Registration Result %d: %v", index, err)
		}
		replayed, err := responses[1].RegistrationResults[index].RegistrationResult()
		if err != nil {
			t.Fatalf("replayed Registration Result %d: %v", index, err)
		}
		if first.RegistrationStatus.RegistrationStatus() != uint32(RegistrationSuccessfullyRegistered) {
			t.Fatalf("first Registration Result %d status = %d, want success", index, first.RegistrationStatus.RegistrationStatus())
		}
		if replayed.RegistrationStatus.RegistrationStatus() != first.RegistrationStatus.RegistrationStatus() ||
			replayed.RoutingContext.RoutingContext() != first.RoutingContext.RoutingContext() {
			t.Fatalf("replayed Registration Result %d = status %d, RC %d; want original status %d, RC %d",
				index,
				replayed.RegistrationStatus.RegistrationStatus(),
				replayed.RoutingContext.RoutingContext(),
				first.RegistrationStatus.RegistrationStatus(),
				first.RoutingContext.RoutingContext(),
			)
		}
	}
	endpoint.routingKeys.mu.Lock()
	confirmedState := endpoint.routingKeys.replays[association]
	if confirmedState == nil {
		endpoint.routingKeys.mu.Unlock()
		t.Fatalf("confirmed Registration replay state missing; Association error = %v", association.Err())
	}
	confirmedReplayCount := len(confirmedState.order)
	confirmedPendingCount := len(confirmedState.pending)
	endpoint.routingKeys.mu.Unlock()
	if confirmedReplayCount != registrationReplayLimit || confirmedPendingCount != 0 {
		t.Fatalf("confirmed Registration replay state = %d results, %d awaiting response; want %d, 0",
			confirmedReplayCount,
			confirmedPendingCount,
			registrationReplayLimit,
		)
	}
}

func TestRKMDeregistrationBatchRemainsReplayableUntilResponseWrite(t *testing.T) {
	const deregistrationCount = deregistrationReplayLimit + 1
	endpoint, err := NewEndpoint(EndpointConfig{
		Role: RoleSGP,
		RoutingKeyManagement: &RoutingKeyManagementConfig{
			AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
				return RegistrationSuccessfullyRegistered
			},
			AllowDynamicRoutingKeys: true,
			MaxDynamicRoutingKeys:   deregistrationCount,
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

	registrations := make([]RoutingKeyRegistrationRequest, deregistrationCount)
	for index := range registrations {
		registrations[index] = RoutingKeyRegistrationRequest{
			LocalRoutingKeyIdentifier: uint32(index + 1),
			RoutingKey:                testRoutingKey(10, uint32(index+1), params.ServiceIndSCCP),
		}
	}
	registrationResults := endpoint.routingKeys.register(association, registrations)
	routingContexts := make([]uint32, len(registrationResults))
	for index, result := range registrationResults {
		if result.Status != RegistrationSuccessfullyRegistered {
			t.Fatalf("Registration Result %d = %+v, want success", index, result)
		}
		routingContexts[index] = result.RoutingContext
	}

	request := messages.NewDeregistrationRequest(params.NewRoutingContext(routingContexts...))
	writeErr := errors.New("deregistration response write failed")
	var responses []*messages.DeregistrationResponse
	failResponseWrite := true
	association.signalWriter = func(message messages.M3UA) (int, error) {
		response, ok := message.(*messages.DeregistrationResponse)
		if !ok {
			return message.MarshalLen(), nil
		}
		responses = append(responses, response)
		if failResponseWrite {
			failResponseWrite = false
			return 0, writeErr
		}
		return message.MarshalLen(), nil
	}

	if err := association.handleDeregistrationRequest(request); !errors.Is(err, writeErr) {
		t.Fatalf("first Deregistration error = %v, want write failure", err)
	}
	endpoint.routingKeys.mu.Lock()
	pendingState := endpoint.routingKeys.deregReplays[association]
	if pendingState == nil {
		endpoint.routingKeys.mu.Unlock()
		t.Fatalf("pending Deregistration replay state missing; Association error = %v", association.Err())
	}
	pendingReplayCount := len(pendingState.order)
	pendingResponseCount := len(pendingState.pending)
	endpoint.routingKeys.mu.Unlock()
	if pendingReplayCount != deregistrationCount || pendingResponseCount != deregistrationCount {
		t.Fatalf("pending Deregistration replay state = %d results, %d awaiting response; want %d, %d",
			pendingReplayCount,
			pendingResponseCount,
			deregistrationCount,
			deregistrationCount,
		)
	}
	if err := association.handleDeregistrationRequest(request); err != nil {
		t.Fatalf("replayed Deregistration: %v", err)
	}
	if len(responses) != 2 {
		t.Fatalf("Deregistration Responses = %d, want 2", len(responses))
	}
	for index := range responses[0].DeregistrationResults {
		first, err := responses[0].DeregistrationResults[index].DeregistrationResult()
		if err != nil {
			t.Fatalf("first Deregistration Result %d: %v", index, err)
		}
		replayed, err := responses[1].DeregistrationResults[index].DeregistrationResult()
		if err != nil {
			t.Fatalf("replayed Deregistration Result %d: %v", index, err)
		}
		if first.DeregistrationStatus.DeregistrationStatus() != uint32(DeregistrationSuccessfullyDeregistered) {
			t.Fatalf("first Deregistration Result %d status = %d, want success", index, first.DeregistrationStatus.DeregistrationStatus())
		}
		if replayed.DeregistrationStatus.DeregistrationStatus() != first.DeregistrationStatus.DeregistrationStatus() ||
			replayed.RoutingContext.RoutingContext() != first.RoutingContext.RoutingContext() {
			t.Fatalf("replayed Deregistration Result %d = status %d, RC %d; want original status %d, RC %d",
				index,
				replayed.DeregistrationStatus.DeregistrationStatus(),
				replayed.RoutingContext.RoutingContext(),
				first.DeregistrationStatus.DeregistrationStatus(),
				first.RoutingContext.RoutingContext(),
			)
		}
	}
	endpoint.routingKeys.mu.Lock()
	confirmedState := endpoint.routingKeys.deregReplays[association]
	if confirmedState == nil {
		endpoint.routingKeys.mu.Unlock()
		t.Fatalf("confirmed Deregistration replay state missing; Association error = %v", association.Err())
	}
	confirmedReplayCount := len(confirmedState.order)
	confirmedPendingCount := len(confirmedState.pending)
	endpoint.routingKeys.mu.Unlock()
	if confirmedReplayCount != deregistrationReplayLimit || confirmedPendingCount != 0 {
		t.Fatalf("confirmed Deregistration replay state = %d results, %d awaiting response; want %d, 0",
			confirmedReplayCount,
			confirmedPendingCount,
			deregistrationReplayLimit,
		)
	}
}

func TestRKMRegistrationCloseRaceDoesNotRecreateMembership(t *testing.T) {
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
	association.as = endpoint.as
	association.muState.Lock()
	association.state = StateASPInactive
	association.muState.Unlock()
	if !endpoint.trackAssociation(association) {
		t.Fatal("trackAssociation returned false")
	}

	responseStarted := make(chan uint32, 1)
	releaseResponse := make(chan struct{})
	association.signalWriter = func(message messages.M3UA) (int, error) {
		response, ok := message.(*messages.RegistrationResponse)
		if !ok {
			return message.MarshalLen(), nil
		}
		result, resultErr := response.RegistrationResults[0].RegistrationResult()
		if resultErr != nil {
			return 0, resultErr
		}
		responseStarted <- result.RoutingContext.RoutingContext()
		<-releaseResponse
		return message.MarshalLen(), nil
	}

	request := registrationRequestMessage(t)
	handleDone := make(chan error, 1)
	go func() {
		handleDone <- association.handleRegistrationRequest(request)
	}()
	routingContext := <-responseStarted

	closeDone := make(chan error, 1)
	go func() { closeDone <- association.Close() }()
	<-association.Done()

	closeCompletedBeforeResponse := false
	select {
	case err := <-closeDone:
		closeCompletedBeforeResponse = true
		if err != nil {
			t.Errorf("Association.Close: %v", err)
		}
	case <-time.After(250 * time.Millisecond):
	}
	close(releaseResponse)
	if err := <-handleDone; err != nil && !errors.Is(err, ErrNotEstablished) && !errors.Is(err, ErrAssociationClosed) {
		t.Errorf("handleRegistrationRequest: %v", err)
	}
	if !closeCompletedBeforeResponse {
		if err := <-closeDone; err != nil {
			t.Errorf("Association.Close: %v", err)
		}
	}
	if closeCompletedBeforeResponse {
		t.Error("Association.Close completed before the in-flight Registration response")
	}

	key := ASKey{
		NetworkAppearance:    10,
		NetworkAppearanceSet: true,
		RoutingContext:       routingContext,
		RoutingContextSet:    true,
	}
	if _, ok := association.dynamicASKey(routingContext, false); ok {
		t.Fatal("Registration recreated Association Routing Key scope after close")
	}
	if _, ok := endpoint.routingKeys.routingKey(routingContext); ok {
		t.Fatal("Registration retained Routing Key registry membership after close")
	}
	if _, ok := endpoint.as.lookup(key); ok {
		t.Fatal("Registration recreated Application Server membership after close")
	}
}

func TestRKMRequesterRegistrationCloseRaceDoesNotRecreateMembership(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleIPSP})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	defer func() { _ = endpoint.Close() }()
	config := NewAssociationConfig(0, 0, 0, 0, 0, 0)
	config.IPSP = &IPSPConfig{ExchangeModel: IPSPExchangeSingle}
	association := newAssociation(RoleIPSP, config)
	association.as = endpoint.as
	association.muState.Lock()
	association.state = StateASPInactive
	association.muState.Unlock()
	if !endpoint.trackAssociation(association) {
		t.Fatal("trackAssociation returned false")
	}

	const routingContext = 77
	responseDelivered := make(chan struct{})
	releaseWrite := make(chan struct{})
	association.signalWriter = func(message messages.M3UA) (int, error) {
		request, ok := message.(*messages.RegistrationRequest)
		if !ok {
			return message.MarshalLen(), nil
		}
		payload, err := request.RoutingKeys[0].RoutingKey()
		if err != nil {
			return 0, err
		}
		if err := association.handleRegistrationResponse(messages.NewRegistrationResponse(
			params.NewRegistrationResult(params.NewRegistrationResultPayload(
				payload.LocalRoutingKeyIdentifier.Copy(),
				params.NewRegistrationStatus(params.SuccessfullyRegistered),
				params.NewRoutingContext(routingContext),
			)),
		)); err != nil {
			return 0, err
		}
		close(responseDelivered)
		<-releaseWrite
		return message.MarshalLen(), nil
	}

	type registrationAnswer struct {
		results []RoutingKeyRegistrationResult
		err     error
	}
	registerDone := make(chan registrationAnswer, 1)
	go func() {
		results, err := association.RegisterRoutingKeys(context.Background(), RoutingKeyRegistration{
			RoutingKey: testRoutingKey(10, 100, params.ServiceIndSCCP),
		})
		registerDone <- registrationAnswer{results: results, err: err}
	}()
	select {
	case <-responseDelivered:
	case <-time.After(time.Second):
		t.Fatal("Registration Response was not delivered")
	}
	if err := association.Close(); err != nil {
		t.Fatalf("Association.Close: %v", err)
	}
	close(releaseWrite)
	answer := <-registerDone
	if answer.err != nil && !errors.Is(answer.err, ErrAssociationClosed) && !errors.Is(answer.err, ErrNotEstablished) {
		t.Fatalf("RegisterRoutingKeys error = %v, results = %+v", answer.err, answer.results)
	}

	key := ASKey{
		NetworkAppearance:    10,
		NetworkAppearanceSet: true,
		RoutingContext:       routingContext,
		RoutingContextSet:    true,
	}
	if _, ok := association.dynamicASKey(routingContext, false); ok {
		t.Fatal("Registration recreated Association Routing Key scope after close")
	}
	if _, ok := endpoint.as.lookup(key); ok {
		t.Fatal("Registration recreated Application Server membership after close")
	}
}

func TestRKMResponderProvisionedDeregistrationReplayCompletesLocalCleanup(t *testing.T) {
	routingKey := testRoutingKey(10, 100, params.ServiceIndSCCP)
	endpoint, err := NewEndpoint(EndpointConfig{
		Role: RoleSGP,
		RoutingKeyManagement: &RoutingKeyManagementConfig{
			AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
				return RegistrationSuccessfullyRegistered
			},
			ProvisionedRoutingKeys: []ProvisionedRoutingKey{{RoutingContext: 7, RoutingKey: routingKey}},
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
	failDeregistrationResponse := false
	association.signalWriter = func(message messages.M3UA) (int, error) {
		writerMu.Lock()
		fail := failDeregistrationResponse
		writerMu.Unlock()
		if _, deregistration := message.(*messages.DeregistrationResponse); deregistration && fail {
			return 0, writeErr
		}
		return message.MarshalLen(), nil
	}
	setFailDeregistrationResponse := func(fail bool) {
		writerMu.Lock()
		failDeregistrationResponse = fail
		writerMu.Unlock()
	}

	if err := association.handleRegistrationRequest(registrationRequestMessage(t)); err != nil {
		t.Fatalf("Registration: %v", err)
	}
	key := ASKey{NetworkAppearance: 10, NetworkAppearanceSet: true, RoutingContext: 7, RoutingContextSet: true}
	applicationServer, ok := endpoint.as.lookup(key)
	if !ok {
		t.Fatal("provisioned Application Server not registered")
	}
	if _, ok := association.dynamicASKey(7, false); !ok {
		t.Fatal("Association Routing Key scope not registered")
	}

	deregistration := messages.NewDeregistrationRequest(params.NewRoutingContext(7))
	setFailDeregistrationResponse(true)
	if err := association.handleDeregistrationRequest(deregistration); !errors.Is(err, writeErr) {
		t.Fatalf("first Deregistration error = %v, want write failure", err)
	}
	applicationServer.mu.Lock()
	_, memberBeforeReplay := applicationServer.asps[association]
	applicationServer.mu.Unlock()
	if !memberBeforeReplay {
		t.Fatal("provisioned AS membership removed before DEREG RSP was written")
	}
	if _, ok := association.dynamicASKey(7, false); !ok {
		t.Fatal("Association Routing Key scope removed before DEREG RSP was written")
	}

	setFailDeregistrationResponse(false)
	if err := association.handleDeregistrationRequest(deregistration); err != nil {
		t.Fatalf("Deregistration replay: %v", err)
	}
	applicationServer.mu.Lock()
	_, memberAfterReplay := applicationServer.asps[association]
	applicationServer.mu.Unlock()
	if memberAfterReplay {
		t.Fatal("Deregistration replay retained provisioned AS membership")
	}
	if _, ok := association.dynamicASKey(7, false); ok {
		t.Fatal("Deregistration replay retained Association Routing Key scope")
	}
	if _, ok := endpoint.as.lookup(key); !ok {
		t.Fatal("Deregistration removed the provisioned Application Server")
	}
}

func TestRKMResponderDeregistrationPreservesStaticApplicationServerMembership(t *testing.T) {
	routingKey := testRoutingKey(10, 100, params.ServiceIndSCCP)
	endpoint, err := NewEndpoint(EndpointConfig{
		Role: RoleSGP,
		RoutingKeyManagement: &RoutingKeyManagementConfig{
			AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
				return RegistrationSuccessfullyRegistered
			},
			ProvisionedRoutingKeys: []ProvisionedRoutingKey{{RoutingContext: 7, RoutingKey: routingKey}},
		},
	})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	defer func() { _ = endpoint.Close() }()
	config := NewAssociationConfig(0, 0, 0, 0, 0, 0)
	config.NetworkAppearance = params.NewNetworkAppearance(10)
	config.RoutingContexts = params.NewRoutingContext(7)
	association := newAssociation(RoleSGP, config)
	association.endpoint = endpoint
	association.as = endpoint.as
	association.muState.Lock()
	association.state = StateASPInactive
	association.muState.Unlock()
	association.signalWriter = func(message messages.M3UA) (int, error) {
		return message.MarshalLen(), nil
	}
	endpoint.as.register(association.staticallyConfiguredASKeys())
	endpoint.as.aspStateChanged(association, StateASPInactive)

	if err := association.handleRegistrationRequest(registrationRequestMessage(t)); err != nil {
		t.Fatalf("Registration: %v", err)
	}
	key := ASKey{NetworkAppearance: 10, NetworkAppearanceSet: true, RoutingContext: 7, RoutingContextSet: true}
	applicationServer, ok := endpoint.as.lookup(key)
	if !ok {
		t.Fatal("provisioned Application Server not registered")
	}
	if _, ok := association.dynamicASKey(7, false); !ok {
		t.Fatal("Association Routing Key scope not registered")
	}

	if err := association.handleDeregistrationRequest(messages.NewDeregistrationRequest(params.NewRoutingContext(7))); err != nil {
		t.Fatalf("Deregistration: %v", err)
	}
	if _, ok := association.dynamicASKey(7, false); ok {
		t.Fatal("Deregistration retained the dynamic Routing Key scope")
	}
	applicationServer.mu.Lock()
	state, member := applicationServer.asps[association]
	applicationServer.mu.Unlock()
	if !member || state != StateASPInactive {
		t.Fatalf("static Application Server membership = %v, %v, want ASP-INACTIVE, true", state, member)
	}
}

func TestRKMRequesterDeregistrationPreservesStaticApplicationServerMembership(t *testing.T) {
	applicationServers := newApplicationServers(time.Hour)
	config := NewAssociationConfig(0, 0, 0, 0, 0, 0)
	config.NetworkAppearance = params.NewNetworkAppearance(10)
	config.RoutingContexts = params.NewRoutingContext(7)
	association := newAssociation(RoleASP, config)
	association.as = applicationServers
	key := ASKey{NetworkAppearance: 10, NetworkAppearanceSet: true, RoutingContext: 7, RoutingContextSet: true}
	applicationServers.register(association.staticallyConfiguredASKeys())
	applicationServers.aspStateChanged(association, StateASPInactive)
	association.addDynamicASKey(key, testRoutingKey(10, 100, params.ServiceIndSCCP), false)
	applicationServers.registerDynamicASP(association, key)

	association.removeRequesterRoutingKeyVersion(7, association.dynamicASKeyVersionFor(7, false))
	if _, ok := association.dynamicASKey(7, false); ok {
		t.Fatal("Deregistration retained the dynamic Routing Key scope")
	}
	applicationServer, ok := applicationServers.lookup(key)
	if !ok {
		t.Fatal("static Application Server was removed")
	}
	applicationServer.mu.Lock()
	state, member := applicationServer.asps[association]
	applicationServer.mu.Unlock()
	if !member || state != StateASPInactive {
		t.Fatalf("static Application Server membership = %v, %v, want ASP-INACTIVE, true", state, member)
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

func TestRKMResponderRejectsUnsupportedRoutingKeyParameterFields(t *testing.T) {
	authorizations := 0
	endpoint, err := NewEndpoint(EndpointConfig{
		Role: RoleSGP,
		RoutingKeyManagement: &RoutingKeyManagementConfig{
			AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
				authorizations++
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
	association.as = endpoint.as
	association.muState.Lock()
	association.state = StateASPInactive
	association.muState.Unlock()
	responses := make(chan *messages.RegistrationResponse, 1)
	association.signalWriter = func(message messages.M3UA) (int, error) {
		if response, ok := message.(*messages.RegistrationResponse); ok {
			responses <- response
		}
		return message.MarshalLen(), nil
	}

	unsupported, err := routingKeyParameter(RoutingKeyRegistrationRequest{
		LocalRoutingKeyIdentifier: 1,
		RoutingKey:                testRoutingKey(10, 100, params.ServiceIndSCCP),
	})
	if err != nil {
		t.Fatalf("unsupported routingKeyParameter: %v", err)
	}
	unsupportedPayload, err := unsupported.RoutingKey()
	if err != nil {
		t.Fatalf("unsupported RoutingKey: %v", err)
	}
	unsupportedPayload.Others = append(unsupportedPayload.Others, params.NewParam(0x7ffe, []byte{0xaa}))
	unsupported = params.NewRoutingKey(unsupportedPayload)

	supported, err := routingKeyParameter(RoutingKeyRegistrationRequest{
		LocalRoutingKeyIdentifier: 2,
		RoutingKey:                testRoutingKey(20, 200, params.ServiceIndISUP),
	})
	if err != nil {
		t.Fatalf("supported routingKeyParameter: %v", err)
	}
	if err := association.handleRegistrationRequest(messages.NewRegistrationRequest(unsupported, supported)); err != nil {
		t.Fatalf("handleRegistrationRequest: %v", err)
	}

	response := <-responses
	if len(response.RegistrationResults) != 2 {
		t.Fatalf("Registration Results = %d, want 2", len(response.RegistrationResults))
	}
	first, err := response.RegistrationResults[0].RegistrationResult()
	if err != nil {
		t.Fatalf("unsupported Registration Result: %v", err)
	}
	if status := first.RegistrationStatus.RegistrationStatus(); status != params.UnsupportedRKparameterField {
		t.Fatalf("unsupported Registration Status = %d, want %d", status, params.UnsupportedRKparameterField)
	}
	if routingContext := first.RoutingContext.RoutingContext(); routingContext != 0 {
		t.Fatalf("unsupported Registration Routing Context = %d, want 0", routingContext)
	}
	second, err := response.RegistrationResults[1].RegistrationResult()
	if err != nil {
		t.Fatalf("supported Registration Result: %v", err)
	}
	if status := second.RegistrationStatus.RegistrationStatus(); status != params.SuccessfullyRegistered {
		t.Fatalf("supported Registration Status = %d, want success", status)
	}
	if routingContext := second.RoutingContext.RoutingContext(); routingContext == 0 {
		t.Fatal("supported Registration Routing Context = 0")
	}
	if authorizations != 1 {
		t.Fatalf("authorization calls = %d, want only the supported Routing Key", authorizations)
	}
	if dynamic := endpoint.routingKeys.dynamicCount(); dynamic != 1 {
		t.Fatalf("dynamic Routing Keys = %d, want only the supported Routing Key", dynamic)
	}
}

func TestRKMResponderAcceptsWideOriginatingPointCodeMask(t *testing.T) {
	authorizations := 0
	endpoint, err := NewEndpoint(EndpointConfig{
		Role: RoleSGP,
		RoutingKeyManagement: &RoutingKeyManagementConfig{
			AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
				authorizations++
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
	association.as = endpoint.as
	association.muState.Lock()
	association.state = StateASPInactive
	association.muState.Unlock()
	responses := make(chan *messages.RegistrationResponse, 1)
	association.signalWriter = func(message messages.M3UA) (int, error) {
		if response, ok := message.(*messages.RegistrationResponse); ok {
			responses <- response
		}
		return message.MarshalLen(), nil
	}
	key := testRoutingKey(10, 100, params.ServiceIndSCCP)
	key.Groups[0].OriginatingPointCodes = []PointCodeRange{{PointCode: 0x123456, Mask: 25}}
	parameter, err := routingKeyParameter(RoutingKeyRegistrationRequest{
		LocalRoutingKeyIdentifier: 1,
		RoutingKey:                key,
	})
	if err != nil {
		t.Fatalf("routingKeyParameter: %v", err)
	}
	supported, err := routingKeyParameter(RoutingKeyRegistrationRequest{
		LocalRoutingKeyIdentifier: 2,
		RoutingKey:                testRoutingKey(20, 200, params.ServiceIndISUP),
	})
	if err != nil {
		t.Fatalf("supported routingKeyParameter: %v", err)
	}

	if err := association.handleRegistrationRequest(messages.NewRegistrationRequest(parameter, supported)); err != nil {
		t.Fatalf("handleRegistrationRequest: %v", err)
	}
	response := <-responses
	if len(response.RegistrationResults) != 2 {
		t.Fatalf("Registration Results = %d, want 2", len(response.RegistrationResults))
	}
	result, err := response.RegistrationResults[0].RegistrationResult()
	if err != nil {
		t.Fatalf("Registration Result: %v", err)
	}
	if status := result.RegistrationStatus.RegistrationStatus(); status != params.SuccessfullyRegistered {
		t.Fatalf("Registration Status = %d, want success", status)
	}
	if routingContext := result.RoutingContext.RoutingContext(); routingContext == 0 {
		t.Fatal("Registration Routing Context = 0")
	}
	supportedResult, err := response.RegistrationResults[1].RegistrationResult()
	if err != nil {
		t.Fatalf("supported Registration Result: %v", err)
	}
	if status := supportedResult.RegistrationStatus.RegistrationStatus(); status != params.SuccessfullyRegistered {
		t.Fatalf("supported Registration Status = %d, want success", status)
	}
	if routingContext := supportedResult.RoutingContext.RoutingContext(); routingContext == 0 {
		t.Fatal("supported Registration Routing Context = 0")
	}
	if authorizations != 2 {
		t.Fatalf("authorization calls = %d, want both Routing Keys", authorizations)
	}
	if dynamic := endpoint.routingKeys.dynamicCount(); dynamic != 2 {
		t.Fatalf("dynamic Routing Keys = %d, want both Routing Keys", dynamic)
	}
}
