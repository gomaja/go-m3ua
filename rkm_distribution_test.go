package m3ua

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestSGPDistributionResolvesOmittedRoutingContextFromRegisteredRoutingKey(t *testing.T) {
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
	association.as = endpoint.as
	association.muState.Lock()
	association.state = StateASPInactive
	association.muState.Unlock()
	var writtenMu sync.Mutex
	var written []*messages.Data
	association.signalWriter = func(message messages.M3UA) (int, error) {
		if data, ok := message.(*messages.Data); ok {
			writtenMu.Lock()
			written = append(written, data)
			writtenMu.Unlock()
		}
		return message.MarshalLen(), nil
	}
	writtenData := func() []*messages.Data {
		writtenMu.Lock()
		defer writtenMu.Unlock()
		return append([]*messages.Data(nil), written...)
	}
	routingKey := RoutingKey{
		NetworkAppearance:    10,
		NetworkAppearanceSet: true,
		TrafficMode:          params.TrafficModeLoadshare,
		TrafficModeSet:       true,
		Groups: []RoutingKeyGroup{{
			DestinationPointCode:  100,
			ServiceIndicators:     []uint8{params.ServiceIndSCCP},
			OriginatingPointCodes: []PointCodeRange{{PointCode: 50}},
		}},
	}
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
	routingContext := routingContexts[0]
	key, ok := association.dynamicASKey(routingContext, false)
	if !ok {
		t.Fatal("registered ASKey is missing")
	}
	applicationServer, ok := endpoint.as.lookup(key)
	if !ok {
		t.Fatal("registered Application Server is missing")
	}
	association.noteRoutingContextsActive([]uint32{routingContext})
	association.muState.Lock()
	association.state = StateASPActive
	association.muState.Unlock()
	applicationServer.setTrafficMode(params.TrafficModeLoadshare)
	applicationServer.setASPState(association, StateASPActive, time.Hour)
	data := messages.NewData(
		params.NewNetworkAppearance(10),
		nil,
		params.NewProtocolData(50, 100, params.ServiceIndSCCP, 0, 0, 1, []byte("payload")),
		nil,
	)
	result, err := association.DistributeData(data)
	if err != nil {
		t.Fatalf("DistributeData: %v", err)
	}
	deliveries := writtenData()
	if result.Delivered != 1 || len(deliveries) != 1 {
		t.Fatalf("distribution result = %+v, DATA writes = %d", result, len(deliveries))
	}
	delivered := deliveries[0]
	if delivered.RoutingContext == nil || delivered.RoutingContext.RoutingContext() != routingContext {
		t.Fatalf("delivered Routing Context = %v, want %d", delivered.RoutingContext, routingContext)
	}
	if data.RoutingContext != nil {
		t.Fatalf("DistributeData mutated caller DATA Routing Context to %v", data.RoutingContext)
	}

	wrongServiceIndicator := messages.NewData(
		params.NewNetworkAppearance(10),
		nil,
		params.NewProtocolData(50, 100, params.ServiceIndISUP, 0, 0, 1, []byte("wrong SI")),
		nil,
	)
	if _, err := association.DistributeData(wrongServiceIndicator); !errors.Is(err, ErrNoMatchingRoutingKey) {
		t.Fatalf("mismatched Routing Key error = %v, want ErrNoMatchingRoutingKey", err)
	}
}

func TestSGPDistributionFallsBackToStaticApplicationServerAfterRoutingKeyMiss(t *testing.T) {
	routingKeys, err := newRoutingKeyRegistry(&RoutingKeyManagementConfig{
		AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
			return RegistrationSuccessfullyRegistered
		},
		ProvisionedRoutingKeys: []ProvisionedRoutingKey{{
			RoutingContext: 2,
			RoutingKey:     testRoutingKey(10, 200, params.ServiceIndISUP),
		}},
	})
	if err != nil {
		t.Fatalf("newRoutingKeyRegistry: %v", err)
	}
	applicationServers := newApplicationServers(time.Hour)
	staticKey := ASKey{RoutingContext: 1, RoutingContextSet: true}
	applicationServers.register([]ASKey{staticKey})

	data := messages.NewData(
		nil,
		nil,
		params.NewProtocolData(50, 100, params.ServiceIndSCCP, 0, 0, 1, []byte("static")),
		nil,
	)
	prepared, _, _, key, err := prepareDistributionData(applicationServers, routingKeys, data)
	if err != nil {
		t.Fatalf("prepareDistributionData: %v", err)
	}
	if key != staticKey {
		t.Fatalf("resolved ASKey = %+v, want %+v", key, staticKey)
	}
	if prepared.RoutingContext == nil || prepared.RoutingContext.RoutingContext() != staticKey.RoutingContext {
		t.Fatalf("prepared Routing Context = %v, want %d", prepared.RoutingContext, staticKey.RoutingContext)
	}
}

func TestSGPDistributionFallsBackToStaticApplicationServerAfterDynamicOwnerDeregisters(t *testing.T) {
	routingKeys, err := newRoutingKeyRegistry(&RoutingKeyManagementConfig{
		AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
			return RegistrationSuccessfullyRegistered
		},
		ProvisionedRoutingKeys: []ProvisionedRoutingKey{{
			RoutingContext: 2,
			RoutingKey:     testRoutingKey(10, 200, params.ServiceIndISUP),
		}},
	})
	if err != nil {
		t.Fatalf("newRoutingKeyRegistry: %v", err)
	}
	applicationServers := newApplicationServers(time.Hour)
	staticKey := ASKey{RoutingContext: 1, RoutingContextSet: true}
	dynamicAssociation, _ := newTestConnWithContexts(t, StateASPInactive, RoleSGP)
	applicationServers.registerDynamicASP(dynamicAssociation, staticKey)
	reservation := applicationServers.reserve([]ASKey{staticKey})
	applicationServers.deregisterDynamicASP(dynamicAssociation, staticKey, true)
	reservation.commit()

	data := messages.NewData(
		nil,
		nil,
		params.NewProtocolData(50, 100, params.ServiceIndSCCP, 0, 0, 1, []byte("static")),
		nil,
	)
	prepared, _, _, key, err := prepareDistributionData(applicationServers, routingKeys, data)
	if err != nil {
		t.Fatalf("prepareDistributionData: %v", err)
	}
	if key != staticKey {
		t.Fatalf("resolved ASKey = %+v, want %+v", key, staticKey)
	}
	if prepared.RoutingContext == nil || prepared.RoutingContext.RoutingContext() != staticKey.RoutingContext {
		t.Fatalf("prepared Routing Context = %v, want %d", prepared.RoutingContext, staticKey.RoutingContext)
	}
}

func TestSGPDistributionRoutingKeySelectionRequiresNetworkAppearanceWhenAmbiguous(t *testing.T) {
	provisioned := []ProvisionedRoutingKey{
		{RoutingContext: 1, RoutingKey: testRoutingKey(10, 100, params.ServiceIndSCCP)},
		{RoutingContext: 2, RoutingKey: testRoutingKey(20, 100, params.ServiceIndSCCP)},
	}
	routingKeys, err := newRoutingKeyRegistry(&RoutingKeyManagementConfig{
		AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
			return RegistrationSuccessfullyRegistered
		},
		ProvisionedRoutingKeys: provisioned,
	})
	if err != nil {
		t.Fatalf("newRoutingKeyRegistry: %v", err)
	}
	applicationServers := newApplicationServers(time.Hour)
	for _, provisionedKey := range provisioned {
		applicationServers.register([]ASKey{{
			NetworkAppearance:    provisionedKey.RoutingKey.NetworkAppearance,
			NetworkAppearanceSet: provisionedKey.RoutingKey.NetworkAppearanceSet,
			RoutingContext:       provisionedKey.RoutingContext,
			RoutingContextSet:    true,
		}})
	}

	ambiguous := messages.NewData(
		nil,
		nil,
		params.NewProtocolData(50, 100, params.ServiceIndSCCP, 0, 0, 1, []byte("ambiguous")),
		nil,
	)
	if _, _, _, _, err := prepareDistributionData(applicationServers, routingKeys, ambiguous); !errors.Is(err, ErrInvalidNetworkAppearance) {
		t.Fatalf("ambiguous Routing Key selection error = %v, want ErrInvalidNetworkAppearance", err)
	}

	selected := messages.NewData(
		params.NewNetworkAppearance(20),
		nil,
		params.NewProtocolData(50, 100, params.ServiceIndSCCP, 0, 0, 1, []byte("selected")),
		nil,
	)
	owned, _, _, key, err := prepareDistributionData(applicationServers, routingKeys, selected)
	if err != nil {
		t.Fatalf("prepareDistributionData: %v", err)
	}
	if key.RoutingContext != 2 || owned.RoutingContext == nil || owned.RoutingContext.RoutingContext() != 2 {
		t.Fatalf("selected key = %+v, Routing Context = %v, want Routing Context 2", key, owned.RoutingContext)
	}
}

func TestSGPDistributionExplicitRoutingContextDoesNotReclassifyProtocolData(t *testing.T) {
	routingKey := testRoutingKey(10, 100, params.ServiceIndSCCP)
	routingKeys, err := newRoutingKeyRegistry(&RoutingKeyManagementConfig{
		AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
			return RegistrationSuccessfullyRegistered
		},
		ProvisionedRoutingKeys: []ProvisionedRoutingKey{{RoutingContext: 7, RoutingKey: routingKey}},
	})
	if err != nil {
		t.Fatalf("newRoutingKeyRegistry: %v", err)
	}
	applicationServers := newApplicationServers(time.Hour)
	applicationServers.register([]ASKey{{
		NetworkAppearance:    10,
		NetworkAppearanceSet: true,
		RoutingContext:       7,
		RoutingContextSet:    true,
	}})
	data := messages.NewData(
		params.NewNetworkAppearance(10),
		params.NewRoutingContext(7),
		params.NewProtocolData(999, 999, params.ServiceIndISUP, 0, 0, 1, []byte("explicit")),
		nil,
	)
	_, _, _, key, err := prepareDistributionData(applicationServers, routingKeys, data)
	if err != nil {
		t.Fatalf("prepareDistributionData: %v", err)
	}
	if key.RoutingContext != 7 {
		t.Fatalf("selected key = %+v, want explicit Routing Context 7", key)
	}
}
