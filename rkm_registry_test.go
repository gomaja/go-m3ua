package m3ua

import (
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
)

func TestRoutingKeyRegistryRegistrationStatusesAndReplay(t *testing.T) {
	provisionedKey := testRoutingKey(10, 100, 3)
	registry, err := newRoutingKeyRegistry(&RoutingKeyManagementConfig{
		AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
			return RegistrationSuccessfullyRegistered
		},
		AllowDynamicRoutingKeys: true,
		ProvisionedRoutingKeys:  []ProvisionedRoutingKey{{RoutingContext: 7, RoutingKey: provisionedKey}},
	})
	if err != nil {
		t.Fatalf("newRoutingKeyRegistry: %v", err)
	}
	association := newAssociation(RoleSGP, NewAssociationConfig(0, 0, 0, 0, 0, 0))

	request := RoutingKeyRegistrationRequest{LocalRoutingKeyIdentifier: 1, RoutingKey: provisionedKey}
	result := registry.register(association, []RoutingKeyRegistrationRequest{request})[0]
	if result.Status != RegistrationSuccessfullyRegistered || result.RoutingContext != 7 {
		t.Fatalf("first result = %+v, want successful Routing Context 7", result)
	}

	replay := registry.register(association, []RoutingKeyRegistrationRequest{request})[0]
	if replay != result {
		t.Fatalf("replay = %+v, want original %+v", replay, result)
	}

	alreadyRegistered := registry.register(association, []RoutingKeyRegistrationRequest{{
		LocalRoutingKeyIdentifier: 2,
		RoutingKey:                provisionedKey,
	}})[0]
	if alreadyRegistered.Status != RegistrationRoutingKeyAlreadyRegistered || alreadyRegistered.RoutingContext != 7 {
		t.Fatalf("second identifier result = %+v, want already registered Routing Context 7", alreadyRegistered)
	}

	reusedIdentifier := registry.register(association, []RoutingKeyRegistrationRequest{{
		LocalRoutingKeyIdentifier: 1,
		RoutingKey:                testRoutingKey(10, 101, 3),
	}})[0]
	if reusedIdentifier.Status != RegistrationSuccessfullyRegistered || reusedIdentifier.RoutingContext == 0 {
		t.Fatalf("identifier reuse result = %+v, want a new successful registration", reusedIdentifier)
	}
}

func TestRoutingKeyRegistryReportsStaticMembershipAlreadyRegistered(t *testing.T) {
	for _, requestedRoutingContext := range []bool{false, true} {
		t.Run(fmt.Sprintf("requested Routing Context %t", requestedRoutingContext), func(t *testing.T) {
			provisionedKey := testRoutingKey(10, 100, params.ServiceIndSCCP)
			registry, err := newRoutingKeyRegistry(&RoutingKeyManagementConfig{
				AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
					t.Fatal("authorization was called for an already registered static Routing Key")
					return RegistrationPermissionDenied
				},
				ProvisionedRoutingKeys: []ProvisionedRoutingKey{{RoutingContext: 7, RoutingKey: provisionedKey}},
			})
			if err != nil {
				t.Fatalf("newRoutingKeyRegistry: %v", err)
			}
			config := NewAssociationConfig(0, 0, 0, 0, 0, 0)
			config.NetworkAppearance = params.NewNetworkAppearance(10)
			config.RoutingContexts = params.NewRoutingContext(7)
			association := newAssociation(RoleSGP, config)
			applicationServers := newApplicationServers(time.Hour)
			association.as = applicationServers
			applicationServers.register(association.staticallyConfiguredASKeys())
			applicationServers.aspStateChanged(association, StateASPInactive)

			result := registry.register(association, []RoutingKeyRegistrationRequest{{
				LocalRoutingKeyIdentifier: 1,
				RoutingKey:                provisionedKey,
				RequestedRoutingContext:   7,
				RoutingContextRequested:   requestedRoutingContext,
			}})[0]
			if result.Status != RegistrationRoutingKeyAlreadyRegistered || result.RoutingContext != 7 {
				t.Fatalf("static registration = %+v, want Routing Key Already Registered for Routing Context 7", result)
			}
			if status := registry.deregister(association, []uint32{7})[0].Status; status != DeregistrationSuccessfullyDeregistered {
				t.Fatalf("static registration Deregistration Status = %d, want success", status)
			}
		})
	}
}

func TestRoutingKeyRegistryIgnoresUnsetNetworkAppearanceValue(t *testing.T) {
	provisionedKey := testRoutingKey(10, 100, params.ServiceIndSCCP)
	provisionedKey.NetworkAppearanceSet = false
	registry, err := newRoutingKeyRegistry(&RoutingKeyManagementConfig{
		AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
			return RegistrationSuccessfullyRegistered
		},
		ProvisionedRoutingKeys: []ProvisionedRoutingKey{{RoutingContext: 7, RoutingKey: provisionedKey}},
	})
	if err != nil {
		t.Fatalf("newRoutingKeyRegistry: %v", err)
	}
	association := newAssociation(RoleSGP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	requestKey := snapshotRoutingKey(provisionedKey)
	requestKey.NetworkAppearance = 0
	provisionedCanonical, err := canonicalizeRoutingKey(provisionedKey)
	if err != nil {
		t.Fatalf("canonicalize provisioned Routing Key: %v", err)
	}
	requestCanonical, err := canonicalizeRoutingKey(requestKey)
	if err != nil {
		t.Fatalf("canonicalize requested Routing Key: %v", err)
	}
	if !provisionedCanonical.equal(requestCanonical) {
		t.Fatal("omitted Network Appearance values changed canonical Routing Key identity")
	}

	result := registry.register(association, []RoutingKeyRegistrationRequest{{
		LocalRoutingKeyIdentifier: 1,
		RoutingKey:                requestKey,
	}})[0]
	if result.Status != RegistrationSuccessfullyRegistered || result.RoutingContext != 7 {
		t.Fatalf("registration result = %+v, want successful Routing Context 7", result)
	}
	key, ok := registry.asKey(7)
	if !ok || key.NetworkAppearanceSet || key.NetworkAppearance != 0 {
		t.Fatalf("registered ASKey = %+v, %t; want omitted Network Appearance with a zero value", key, ok)
	}
}

func TestRoutingKeyRegistryPartialBatchIsAtomicAndDeterministic(t *testing.T) {
	deniedDPC := uint32(300)
	registry, err := newRoutingKeyRegistry(&RoutingKeyManagementConfig{
		AuthorizeRegistration: func(request RoutingKeyRegistrationRequest) RegistrationStatus {
			if request.RoutingKey.Groups[0].DestinationPointCode == deniedDPC {
				return RegistrationPermissionDenied
			}
			return RegistrationSuccessfullyRegistered
		},
		AllowDynamicRoutingKeys: true,
		MaxDynamicRoutingKeys:   2,
	})
	if err != nil {
		t.Fatalf("newRoutingKeyRegistry: %v", err)
	}
	association := newAssociation(RoleSGP, NewAssociationConfig(0, 0, 0, 0, 0, 0))

	results := registry.register(association, []RoutingKeyRegistrationRequest{
		{LocalRoutingKeyIdentifier: 1, RoutingKey: testRoutingKey(10, 100, 3, 5)},
		{LocalRoutingKeyIdentifier: 2, RoutingKey: testRoutingKey(10, 100, 5)},
		{LocalRoutingKeyIdentifier: 3, RoutingKey: testRoutingKey(10, deniedDPC, 3)},
		{LocalRoutingKeyIdentifier: 4, RoutingKey: testRoutingKey(10, 400, 3)},
		{LocalRoutingKeyIdentifier: 5, RoutingKey: testRoutingKey(10, 500, 3)},
	})
	wantStatuses := []RegistrationStatus{
		RegistrationSuccessfullyRegistered,
		RegistrationCannotSupportUniqueRouting,
		RegistrationPermissionDenied,
		RegistrationSuccessfullyRegistered,
		RegistrationInsufficientResources,
	}
	for index, want := range wantStatuses {
		if results[index].Status != want {
			t.Errorf("result %d status = %d, want %d", index, results[index].Status, want)
		}
	}
	if results[0].RoutingContext == 0 || results[3].RoutingContext == 0 ||
		results[0].RoutingContext == results[3].RoutingContext {
		t.Fatalf("successful allocated Routing Contexts = %d and %d", results[0].RoutingContext, results[3].RoutingContext)
	}
	if got := registry.dynamicCount(); got != 2 {
		t.Fatalf("dynamic Routing Key count = %d, want 2", got)
	}
}

func TestRoutingKeyRegistryRequestedRoutingContextAndAllocatorFailures(t *testing.T) {
	allocatorErr := errors.New("allocator unavailable")
	registry, err := newRoutingKeyRegistry(&RoutingKeyManagementConfig{
		AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
			return RegistrationSuccessfullyRegistered
		},
		AllocateRoutingContext: func(request RoutingContextAllocationRequest) (uint32, error) {
			switch request.Registration.RoutingKey.Groups[0].DestinationPointCode {
			case 100:
				return 9, nil
			case 200:
				return 9, nil
			default:
				return 0, allocatorErr
			}
		},
		AllowDynamicRoutingKeys: true,
		ProvisionedRoutingKeys:  []ProvisionedRoutingKey{{RoutingContext: 7, RoutingKey: testRoutingKey(10, 50, 3)}},
	})
	if err != nil {
		t.Fatalf("newRoutingKeyRegistry: %v", err)
	}
	association := newAssociation(RoleSGP, NewAssociationConfig(0, 0, 0, 0, 0, 0))

	results := registry.register(association, []RoutingKeyRegistrationRequest{
		{
			LocalRoutingKeyIdentifier: 1,
			RoutingContextRequested:   true,
			RequestedRoutingContext:   8,
			RoutingKey:                testRoutingKey(10, 100, 3),
		},
		{
			LocalRoutingKeyIdentifier: 2,
			RoutingContextRequested:   true,
			RequestedRoutingContext:   7,
			RoutingKey:                testRoutingKey(10, 51, 3),
		},
		{LocalRoutingKeyIdentifier: 3, RoutingKey: testRoutingKey(10, 100, 3)},
		{LocalRoutingKeyIdentifier: 4, RoutingKey: testRoutingKey(10, 200, 3)},
		{LocalRoutingKeyIdentifier: 5, RoutingKey: testRoutingKey(10, 300, 3)},
	})
	want := []RegistrationStatus{
		RegistrationRoutingKeyChangeRefused,
		RegistrationRoutingKeyChangeRefused,
		RegistrationSuccessfullyRegistered,
		RegistrationInsufficientResources,
		RegistrationInsufficientResources,
	}
	for index := range want {
		if results[index].Status != want[index] {
			t.Errorf("result %d = %+v, want status %d", index, results[index], want[index])
		}
	}
}

func TestRoutingKeyRegistryRejectsAssociationRoutingContextAppearanceCollision(t *testing.T) {
	provisioned := testRoutingKey(20, 100, params.ServiceIndSCCP)
	registry, err := newRoutingKeyRegistry(&RoutingKeyManagementConfig{
		AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
			return RegistrationSuccessfullyRegistered
		},
		ProvisionedRoutingKeys: []ProvisionedRoutingKey{{RoutingContext: 7, RoutingKey: provisioned}},
	})
	if err != nil {
		t.Fatalf("newRoutingKeyRegistry: %v", err)
	}
	association := newAssociation(RoleSGP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	association.cfg.NetworkAppearance = params.NewNetworkAppearance(10)
	association.cfg.RoutingContexts = params.NewRoutingContext(7)

	for index, request := range []RoutingKeyRegistrationRequest{
		{LocalRoutingKeyIdentifier: 1, RoutingKey: provisioned},
		{
			LocalRoutingKeyIdentifier: 2,
			RoutingKey:                provisioned,
			RoutingContextRequested:   true,
			RequestedRoutingContext:   7,
		},
	} {
		result := registry.register(association, []RoutingKeyRegistrationRequest{request})[0]
		if result.Status != RegistrationCannotSupportUniqueRouting || result.RoutingContext != 0 {
			t.Fatalf("request %d result = %+v, want Cannot Support Unique Routing", index+1, result)
		}
		if key, ok := association.dynamicASKey(7, false); ok {
			t.Fatalf("request %d installed conflicting dynamic ASKey %+v", index+1, key)
		}
	}
}

func TestRoutingKeyRegistryAllowsSameRoutingContextAppearanceOnAnotherAssociation(t *testing.T) {
	provisioned := testRoutingKey(20, 100, params.ServiceIndSCCP)
	registry, err := newRoutingKeyRegistry(&RoutingKeyManagementConfig{
		AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
			return RegistrationSuccessfullyRegistered
		},
		ProvisionedRoutingKeys: []ProvisionedRoutingKey{{RoutingContext: 7, RoutingKey: provisioned}},
	})
	if err != nil {
		t.Fatalf("newRoutingKeyRegistry: %v", err)
	}
	association := newAssociation(RoleSGP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	association.cfg.NetworkAppearance = params.NewNetworkAppearance(20)
	association.cfg.RoutingContexts = params.NewRoutingContext(7)

	result := registry.register(association, []RoutingKeyRegistrationRequest{{
		LocalRoutingKeyIdentifier: 1,
		RoutingKey:                provisioned,
	}})[0]
	if result.Status != RegistrationSuccessfullyRegistered || result.RoutingContext != 7 {
		t.Fatalf("registration result = %+v, want successful Routing Context 7", result)
	}
}

func TestRoutingKeyRegistryRejectsConflictingTrafficModeForExistingKey(t *testing.T) {
	provisioned := testRoutingKey(10, 100, 3)
	provisioned.TrafficMode = params.TrafficModeLoadshare
	provisioned.TrafficModeSet = true
	registry, err := newRoutingKeyRegistry(&RoutingKeyManagementConfig{
		AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
			return RegistrationSuccessfullyRegistered
		},
		ProvisionedRoutingKeys: []ProvisionedRoutingKey{{RoutingContext: 7, RoutingKey: provisioned}},
	})
	if err != nil {
		t.Fatalf("newRoutingKeyRegistry: %v", err)
	}
	association := newAssociation(RoleSGP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	requested := testRoutingKey(10, 100, 3)
	requested.TrafficMode = params.TrafficModeBroadcast
	requested.TrafficModeSet = true

	for _, request := range []RoutingKeyRegistrationRequest{
		{LocalRoutingKeyIdentifier: 1, RoutingKey: requested},
		{
			LocalRoutingKeyIdentifier: 2,
			RoutingKey:                requested,
			RequestedRoutingContext:   7,
			RoutingContextRequested:   true,
		},
	} {
		result := registry.register(association, []RoutingKeyRegistrationRequest{request})[0]
		if result.Status != RegistrationUnsupportedTrafficHandlingMode || result.RoutingContext != 0 {
			t.Fatalf("registration result = %+v, want Unsupported Traffic Handling Mode", result)
		}
	}
}

func TestRoutingKeyRegistryAdoptsFirstTrafficModeForUnspecifiedProvisionedKey(t *testing.T) {
	provisioned := testRoutingKey(10, 100, 3)
	registry, err := newRoutingKeyRegistry(&RoutingKeyManagementConfig{
		AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
			return RegistrationSuccessfullyRegistered
		},
		ProvisionedRoutingKeys: []ProvisionedRoutingKey{{RoutingContext: 7, RoutingKey: provisioned}},
	})
	if err != nil {
		t.Fatalf("newRoutingKeyRegistry: %v", err)
	}
	first := newAssociation(RoleSGP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	second := newAssociation(RoleSGP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	loadshare := snapshotRoutingKey(provisioned)
	loadshare.TrafficMode = params.TrafficModeLoadshare
	loadshare.TrafficModeSet = true
	broadcast := snapshotRoutingKey(provisioned)
	broadcast.TrafficMode = params.TrafficModeBroadcast
	broadcast.TrafficModeSet = true

	registered := registry.register(first, []RoutingKeyRegistrationRequest{{
		LocalRoutingKeyIdentifier: 1,
		RoutingKey:                loadshare,
	}})[0]
	if registered.Status != RegistrationSuccessfullyRegistered {
		t.Fatalf("first registration = %+v, want success", registered)
	}
	conflict := registry.register(second, []RoutingKeyRegistrationRequest{{
		LocalRoutingKeyIdentifier: 2,
		RoutingKey:                broadcast,
	}})[0]
	if conflict.Status != RegistrationUnsupportedTrafficHandlingMode {
		t.Fatalf("second registration = %+v, want Unsupported Traffic Handling Mode", conflict)
	}
	stored, ok := registry.routingKey(7)
	if !ok || !stored.TrafficModeSet || stored.TrafficMode != params.TrafficModeLoadshare {
		t.Fatalf("stored Routing Key Traffic Mode = %+v, want Loadshare", stored)
	}
}

func TestRoutingKeyRegistryRejectsTrafficModeConflictingWithLiveApplicationServer(t *testing.T) {
	provisioned := testRoutingKey(10, 100, 3)
	registry, err := newRoutingKeyRegistry(&RoutingKeyManagementConfig{
		AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
			return RegistrationSuccessfullyRegistered
		},
		ProvisionedRoutingKeys: []ProvisionedRoutingKey{{RoutingContext: 7, RoutingKey: provisioned}},
	})
	if err != nil {
		t.Fatalf("newRoutingKeyRegistry: %v", err)
	}
	applicationServers := newApplicationServers(time.Hour)
	incumbent := newAssociation(RoleSGP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	incumbent.as = applicationServers
	registered := registry.register(incumbent, []RoutingKeyRegistrationRequest{{
		LocalRoutingKeyIdentifier: 1,
		RoutingKey:                provisioned,
	}})[0]
	if registered.Status != RegistrationSuccessfullyRegistered || registered.RoutingContext != 7 {
		t.Fatalf("incumbent registration = %+v, want successful Routing Context 7", registered)
	}
	key, ok := registry.asKey(7)
	if !ok {
		t.Fatal("Routing Context 7 has no ASKey")
	}
	applicationServers.get(key).setTrafficMode(params.TrafficModeLoadshare)

	for _, requestedRoutingContext := range []bool{false, true} {
		t.Run(fmt.Sprintf("requested Routing Context %t", requestedRoutingContext), func(t *testing.T) {
			challenger := newAssociation(RoleSGP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
			challenger.as = applicationServers
			conflicting := snapshotRoutingKey(provisioned)
			conflicting.TrafficMode = params.TrafficModeBroadcast
			conflicting.TrafficModeSet = true
			result := registry.register(challenger, []RoutingKeyRegistrationRequest{{
				LocalRoutingKeyIdentifier: 2,
				RoutingKey:                conflicting,
				RequestedRoutingContext:   7,
				RoutingContextRequested:   requestedRoutingContext,
			}})[0]
			if result.Status != RegistrationUnsupportedTrafficHandlingMode || result.RoutingContext != 0 {
				t.Fatalf("conflicting registration = %+v, want Unsupported Traffic Handling Mode", result)
			}
		})
	}
}

func TestRoutingKeyRegistryAdoptsTrafficModeIntoLiveApplicationServer(t *testing.T) {
	provisioned := testRoutingKey(10, 100, 3)
	registry, err := newRoutingKeyRegistry(&RoutingKeyManagementConfig{
		AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
			return RegistrationSuccessfullyRegistered
		},
		ProvisionedRoutingKeys: []ProvisionedRoutingKey{{RoutingContext: 7, RoutingKey: provisioned}},
	})
	if err != nil {
		t.Fatalf("newRoutingKeyRegistry: %v", err)
	}
	key, ok := registry.asKey(7)
	if !ok {
		t.Fatal("Routing Context 7 has no ASKey")
	}
	applicationServers := newApplicationServers(time.Hour)
	applicationServer := applicationServers.get(key)
	association := newAssociation(RoleSGP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	association.as = applicationServers
	requested := snapshotRoutingKey(provisioned)
	requested.TrafficMode = params.TrafficModeLoadshare
	requested.TrafficModeSet = true

	result := registry.register(association, []RoutingKeyRegistrationRequest{{
		LocalRoutingKeyIdentifier: 1,
		RoutingKey:                requested,
	}})[0]
	if result.Status != RegistrationSuccessfullyRegistered || result.RoutingContext != 7 {
		t.Fatalf("registration = %+v, want successful Routing Context 7", result)
	}
	if mode := applicationServer.TrafficMode(); mode != params.TrafficModeLoadshare {
		t.Fatalf("live AS Traffic Mode = %d, want Loadshare", mode)
	}
}

func TestRoutingKeyRegistryIsolatesLiveTrafficModeConflictWithinBatch(t *testing.T) {
	for _, conflictingFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("conflicting first %t", conflictingFirst), func(t *testing.T) {
			provisioned := testRoutingKey(10, 100, 3)
			registry, err := newRoutingKeyRegistry(&RoutingKeyManagementConfig{
				AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
					return RegistrationSuccessfullyRegistered
				},
				ProvisionedRoutingKeys: []ProvisionedRoutingKey{{RoutingContext: 7, RoutingKey: provisioned}},
			})
			if err != nil {
				t.Fatalf("newRoutingKeyRegistry: %v", err)
			}
			key, ok := registry.asKey(7)
			if !ok {
				t.Fatal("Routing Context 7 has no ASKey")
			}
			applicationServers := newApplicationServers(time.Hour)
			applicationServers.get(key).setTrafficMode(params.TrafficModeLoadshare)
			association := newAssociation(RoleSGP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
			association.as = applicationServers
			conflicting := snapshotRoutingKey(provisioned)
			conflicting.TrafficMode = params.TrafficModeBroadcast
			conflicting.TrafficModeSet = true
			requests := []RoutingKeyRegistrationRequest{
				{LocalRoutingKeyIdentifier: 1, RoutingKey: provisioned},
				{LocalRoutingKeyIdentifier: 2, RoutingKey: conflicting},
			}
			if conflictingFirst {
				requests[0], requests[1] = requests[1], requests[0]
			}

			results := registry.register(association, requests)
			byIdentifier := make(map[uint32]RoutingKeyRegistrationResult, len(results))
			for _, result := range results {
				byIdentifier[result.LocalRoutingKeyIdentifier] = result
			}
			if result := byIdentifier[1]; result.Status != RegistrationSuccessfullyRegistered || result.RoutingContext != 7 {
				t.Fatalf("mode-omitting registration = %+v, want successful Routing Context 7", result)
			}
			if result := byIdentifier[2]; result.Status != RegistrationUnsupportedTrafficHandlingMode || result.RoutingContext != 0 {
				t.Fatalf("conflicting registration = %+v, want Unsupported Traffic Handling Mode", result)
			}
		})
	}
}

func TestRoutingKeyRegistryReportsUnprovisionedWhenDynamicCreationIsDisabled(t *testing.T) {
	registry, err := newRoutingKeyRegistry(&RoutingKeyManagementConfig{
		AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
			return RegistrationSuccessfullyRegistered
		},
	})
	if err != nil {
		t.Fatalf("newRoutingKeyRegistry: %v", err)
	}
	association := newAssociation(RoleSGP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	result := registry.register(association, []RoutingKeyRegistrationRequest{{
		LocalRoutingKeyIdentifier: 1,
		RoutingKey:                testRoutingKey(10, 100, 3),
	}})[0]
	if result.Status != RegistrationRoutingKeyNotCurrentlyProvisioned || result.RoutingContext != 0 {
		t.Fatalf("registration result = %+v, want Routing Key Not Currently Provisioned", result)
	}
}

func TestRoutingKeyRegistryMatchesProvisionedTrafficSelectors(t *testing.T) {
	registry, err := newRoutingKeyRegistry(&RoutingKeyManagementConfig{
		AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
			return RegistrationSuccessfullyRegistered
		},
		ProvisionedRoutingKeys: []ProvisionedRoutingKey{{
			RoutingContext: 7,
			RoutingKey: RoutingKey{
				NetworkAppearance:    10,
				NetworkAppearanceSet: true,
				Groups: []RoutingKeyGroup{
					{
						DestinationPointCode:  100,
						ServiceIndicators:     []uint8{params.ServiceIndSCCP},
						OriginatingPointCodes: []PointCodeRange{{PointCode: 0x1200, Mask: 8}},
					},
					{
						DestinationPointCode: 200,
						ServiceIndicators:    []uint8{params.ServiceIndISUP},
					},
				},
			},
		}},
	})
	if err != nil {
		t.Fatalf("newRoutingKeyRegistry: %v", err)
	}

	tests := []struct {
		name                      string
		networkAppearance         uint32
		networkAppearanceSet      bool
		originatingPointCode      uint32
		destinationPointCode      uint32
		serviceIndicator          uint8
		wantRoutingContexts       []uint32
		wantConfiguredRoutingKeys bool
	}{
		{
			name:                      "OPC mask range",
			networkAppearance:         10,
			networkAppearanceSet:      true,
			originatingPointCode:      0x12ff,
			destinationPointCode:      100,
			serviceIndicator:          params.ServiceIndSCCP,
			wantRoutingContexts:       []uint32{7},
			wantConfiguredRoutingKeys: true,
		},
		{
			name:                      "repeated DPC group",
			networkAppearance:         10,
			networkAppearanceSet:      true,
			originatingPointCode:      999,
			destinationPointCode:      200,
			serviceIndicator:          params.ServiceIndISUP,
			wantRoutingContexts:       []uint32{7},
			wantConfiguredRoutingKeys: true,
		},
		{
			name:                      "OPC outside mask",
			networkAppearance:         10,
			networkAppearanceSet:      true,
			originatingPointCode:      0x1300,
			destinationPointCode:      100,
			serviceIndicator:          params.ServiceIndSCCP,
			wantConfiguredRoutingKeys: true,
		},
		{
			name:                      "different Service Indicator",
			networkAppearance:         10,
			networkAppearanceSet:      true,
			originatingPointCode:      0x1200,
			destinationPointCode:      100,
			serviceIndicator:          params.ServiceIndISUP,
			wantConfiguredRoutingKeys: true,
		},
		{
			name:                      "different Network Appearance",
			networkAppearance:         11,
			networkAppearanceSet:      true,
			originatingPointCode:      0x1200,
			destinationPointCode:      100,
			serviceIndicator:          params.ServiceIndSCCP,
			wantConfiguredRoutingKeys: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			matches, configured := registry.matchingASKeys(
				test.networkAppearance,
				test.networkAppearanceSet,
				test.originatingPointCode,
				test.destinationPointCode,
				test.serviceIndicator,
			)
			if configured != test.wantConfiguredRoutingKeys {
				t.Fatalf("configured = %t, want %t", configured, test.wantConfiguredRoutingKeys)
			}
			got := make([]uint32, len(matches))
			for index, match := range matches {
				got[index] = match.RoutingContext
			}
			if !slices.Equal(got, test.wantRoutingContexts) {
				t.Fatalf("matching Routing Contexts = %v, want %v", got, test.wantRoutingContexts)
			}
		})
	}
}

func TestRoutingKeyRegistryRequiresNetworkAppearanceToDisambiguateTraffic(t *testing.T) {
	provisioned := make([]ProvisionedRoutingKey, 0, 2)
	for index, networkAppearance := range []uint32{10, 20} {
		provisioned = append(provisioned, ProvisionedRoutingKey{
			RoutingContext: uint32(index + 1),
			RoutingKey:     testRoutingKey(networkAppearance, 100, params.ServiceIndSCCP),
		})
	}
	registry, err := newRoutingKeyRegistry(&RoutingKeyManagementConfig{
		AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
			return RegistrationSuccessfullyRegistered
		},
		ProvisionedRoutingKeys: provisioned,
	})
	if err != nil {
		t.Fatalf("newRoutingKeyRegistry: %v", err)
	}

	ambiguous, configured := registry.matchingASKeys(0, false, 50, 100, params.ServiceIndSCCP)
	if !configured || len(ambiguous) != 2 {
		t.Fatalf("omitted Network Appearance matches = %v, configured = %t, want two configured matches", ambiguous, configured)
	}
	selected, configured := registry.matchingASKeys(20, true, 50, 100, params.ServiceIndSCCP)
	if !configured || len(selected) != 1 || selected[0].RoutingContext != 2 {
		t.Fatalf("Network Appearance 20 matches = %v, configured = %t, want Routing Context 2", selected, configured)
	}
}

func TestRoutingKeyRegistryDeregistrationStatuses(t *testing.T) {
	registry, err := newRoutingKeyRegistry(&RoutingKeyManagementConfig{
		AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
			return RegistrationSuccessfullyRegistered
		},
		AllowDynamicRoutingKeys: true,
		RemoveUnusedRoutingKeys: true,
	})
	if err != nil {
		t.Fatalf("newRoutingKeyRegistry: %v", err)
	}
	owner := newAssociation(RoleSGP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	other := newAssociation(RoleSGP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	registered := registry.register(owner, []RoutingKeyRegistrationRequest{{
		LocalRoutingKeyIdentifier: 1,
		RoutingKey:                testRoutingKey(10, 100, 3),
	}})[0]
	if registered.Status != RegistrationSuccessfullyRegistered {
		t.Fatalf("registration = %+v", registered)
	}

	if got := registry.deregister(other, []uint32{registered.RoutingContext})[0].Status; got != DeregistrationNotRegistered {
		t.Fatalf("other ASP deregistration status = %d, want Not Registered", got)
	}
	if got := registry.deregister(owner, []uint32{999})[0].Status; got != DeregistrationInvalidRoutingContext {
		t.Fatalf("unknown Routing Context status = %d, want Invalid Routing Context", got)
	}

	owner.muState.Lock()
	owner.state = StateASPActive
	owner.muState.Unlock()
	if got := registry.deregister(owner, []uint32{registered.RoutingContext})[0].Status; got != DeregistrationASPActiveForRoutingContext {
		t.Fatalf("active ASP deregistration status = %d, want ASP Active", got)
	}

	owner.muState.Lock()
	owner.state = StateASPInactive
	owner.muState.Unlock()
	if got := registry.deregister(owner, []uint32{registered.RoutingContext})[0].Status; got != DeregistrationSuccessfullyDeregistered {
		t.Fatalf("inactive ASP deregistration status = %d, want success", got)
	}
	if got := registry.dynamicCount(); got != 0 {
		t.Fatalf("dynamic Routing Key count = %d after removal, want 0", got)
	}
}

func TestRoutingKeyRegistryReplaysSuccessfulProvisionedDeregistration(t *testing.T) {
	routingKey := testRoutingKey(10, 100, params.ServiceIndSCCP)
	registry, err := newRoutingKeyRegistry(&RoutingKeyManagementConfig{
		AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
			return RegistrationSuccessfullyRegistered
		},
		ProvisionedRoutingKeys: []ProvisionedRoutingKey{{RoutingContext: 7, RoutingKey: routingKey}},
	})
	if err != nil {
		t.Fatalf("newRoutingKeyRegistry: %v", err)
	}
	association := newAssociation(RoleSGP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	registered := registry.register(association, []RoutingKeyRegistrationRequest{{
		LocalRoutingKeyIdentifier: 1,
		RoutingKey:                routingKey,
	}})[0]
	if registered.Status != RegistrationSuccessfullyRegistered || registered.RoutingContext != 7 {
		t.Fatalf("registration = %+v, want successful Routing Context 7", registered)
	}

	first := registry.deregister(association, []uint32{7})[0]
	if first.Status != DeregistrationSuccessfullyDeregistered {
		t.Fatalf("first deregistration = %+v, want success", first)
	}
	replay := registry.deregister(association, []uint32{7})[0]
	if replay != first {
		t.Fatalf("deregistration replay = %+v, want original success %+v", replay, first)
	}
}

func TestRoutingKeyRegistryRejectsDuplicateASPIdentifierWhenScopesConverge(t *testing.T) {
	applicationServers := newApplicationServers(time.Hour)
	newPeer := func(routingContext, identifier uint32) *Association {
		association := newAssociation(RoleSGP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
		association.as = applicationServers
		association.cfg.RoutingContexts = params.NewRoutingContext(routingContext)
		association.savePeerASPIdentifier(params.NewAspIdentifier(identifier))
		applicationServers.register(association.staticallyConfiguredASKeys())
		if !applicationServers.claimASPIdentifier(association, identifier) {
			t.Fatalf("initial ASP Identifier %d claim for Routing Context %d failed", identifier, routingContext)
		}
		return association
	}
	first := newPeer(1, 7)
	second := newPeer(2, 7)
	third := newPeer(4, 8)
	registry, err := newRoutingKeyRegistry(&RoutingKeyManagementConfig{
		AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
			return RegistrationSuccessfullyRegistered
		},
		AllowDynamicRoutingKeys: true,
	})
	if err != nil {
		t.Fatalf("newRoutingKeyRegistry: %v", err)
	}
	routingKey := testRoutingKey(10, 100, params.ServiceIndSCCP)
	firstResult := registry.register(first, []RoutingKeyRegistrationRequest{{
		LocalRoutingKeyIdentifier: 1,
		RoutingKey:                routingKey,
	}})[0]
	if firstResult.Status != RegistrationSuccessfullyRegistered {
		t.Fatalf("first registration = %+v, want success", firstResult)
	}

	duplicate := registry.register(second, []RoutingKeyRegistrationRequest{{
		LocalRoutingKeyIdentifier: 2,
		RoutingKey:                routingKey,
	}})[0]
	if duplicate.Status != RegistrationPermissionDenied || duplicate.RoutingContext != 0 {
		t.Fatalf("duplicate ASP Identifier registration = %+v, want Permission Denied", duplicate)
	}
	if status := registry.deregister(second, []uint32{firstResult.RoutingContext})[0].Status; status != DeregistrationNotRegistered {
		t.Fatalf("rejected ASP membership status = %d, want Not Registered", status)
	}

	distinct := registry.register(third, []RoutingKeyRegistrationRequest{{
		LocalRoutingKeyIdentifier: 3,
		RoutingKey:                routingKey,
	}})[0]
	if distinct.Status != RegistrationSuccessfullyRegistered || distinct.RoutingContext != firstResult.RoutingContext {
		t.Fatalf("distinct ASP Identifier registration = %+v, want success in Routing Context %d",
			distinct, firstResult.RoutingContext)
	}
}

func TestRoutingKeyRegistryRejectsDuplicateASPIdentifierFromStaticApplicationServerMember(t *testing.T) {
	applicationServers := newApplicationServers(time.Hour)
	newStaticPeer := func(routingContext, identifier uint32) *Association {
		association := newAssociation(RoleSGP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
		association.as = applicationServers
		association.cfg.NetworkAppearance = params.NewNetworkAppearance(10)
		association.cfg.RoutingContexts = params.NewRoutingContext(routingContext)
		association.savePeerASPIdentifier(params.NewAspIdentifier(identifier))
		applicationServers.register(association.staticallyConfiguredASKeys())
		applicationServers.aspStateChanged(association, StateASPInactive)
		if !applicationServers.claimASPIdentifier(association, identifier) {
			t.Fatalf("initial ASP Identifier %d claim for Routing Context %d failed", identifier, routingContext)
		}
		return association
	}
	staticAssociation := newStaticPeer(7, 9)
	requestingAssociation := newStaticPeer(2, 9)
	provisionedKey := testRoutingKey(10, 100, params.ServiceIndSCCP)
	registry, err := newRoutingKeyRegistry(&RoutingKeyManagementConfig{
		AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
			return RegistrationSuccessfullyRegistered
		},
		ProvisionedRoutingKeys: []ProvisionedRoutingKey{{RoutingContext: 7, RoutingKey: provisionedKey}},
	})
	if err != nil {
		t.Fatalf("newRoutingKeyRegistry: %v", err)
	}

	result := registry.register(requestingAssociation, []RoutingKeyRegistrationRequest{{
		LocalRoutingKeyIdentifier: 1,
		RoutingKey:                provisionedKey,
	}})[0]
	if result.Status != RegistrationPermissionDenied || result.RoutingContext != 0 {
		t.Fatalf("duplicate static ASP Identifier registration = %+v, want Permission Denied", result)
	}
	if status := registry.deregister(requestingAssociation, []uint32{7})[0].Status; status != DeregistrationNotRegistered {
		t.Fatalf("rejected ASP membership status = %d, want Not Registered", status)
	}
	if identifier, ok := staticAssociation.PeerASPIdentifier(); !ok || identifier != 9 {
		t.Fatalf("static ASP Identifier = %d, %v, want 9, true", identifier, ok)
	}
}

func TestRoutingKeyRegistrySerializesASPIdentifierClaimsWithRegistration(t *testing.T) {
	newFixture := func(t *testing.T, authorize RoutingKeyRegistrationAuthorizer) (*applicationServers, *routingKeyRegistry, *Association, *Association, RoutingKey) {
		t.Helper()
		applicationServers := newApplicationServers(time.Hour)
		provisionedKey := testRoutingKey(10, 100, params.ServiceIndSCCP)
		registry, err := newRoutingKeyRegistry(&RoutingKeyManagementConfig{
			AuthorizeRegistration:  authorize,
			ProvisionedRoutingKeys: []ProvisionedRoutingKey{{RoutingContext: 7, RoutingKey: provisionedKey}},
		})
		if err != nil {
			t.Fatalf("newRoutingKeyRegistry: %v", err)
		}
		endpoint := &Endpoint{as: applicationServers, routingKeys: registry}
		newPeer := func(routingContext uint32) *Association {
			association := newAssociation(RoleSGP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
			association.endpoint = endpoint
			association.as = applicationServers
			association.cfg.NetworkAppearance = params.NewNetworkAppearance(10)
			association.cfg.RoutingContexts = params.NewRoutingContext(routingContext)
			applicationServers.register(association.staticallyConfiguredASKeys())
			applicationServers.aspStateChanged(association, StateASPInactive)
			return association
		}
		requestingAssociation := newPeer(2)
		requestingAssociation.savePeerASPIdentifier(params.NewAspIdentifier(9))
		if !applicationServers.claimASPIdentifier(requestingAssociation, 9) {
			t.Fatal("initial requesting ASP Identifier claim failed")
		}
		return applicationServers, registry, requestingAssociation, newPeer(7), provisionedKey
	}

	t.Run("claim commits before registration", func(t *testing.T) {
		authorizationEntered := make(chan struct{})
		releaseAuthorization := make(chan struct{})
		applicationServers, registry, requestingAssociation, staticAssociation, provisionedKey := newFixture(t,
			func(RoutingKeyRegistrationRequest) RegistrationStatus {
				close(authorizationEntered)
				<-releaseAuthorization
				return RegistrationSuccessfullyRegistered
			})

		result := make(chan RoutingKeyRegistrationResult, 1)
		go func() {
			result <- registry.register(requestingAssociation, []RoutingKeyRegistrationRequest{{
				LocalRoutingKeyIdentifier: 1,
				RoutingKey:                provisionedKey,
			}})[0]
		}()
		select {
		case <-authorizationEntered:
		case <-time.After(time.Second):
			t.Fatal("Registration authorization was not called")
		}
		if !applicationServers.claimASPIdentifier(staticAssociation, 9) {
			t.Fatal("static ASP Identifier claim should win while Registration authorization is pending")
		}
		staticAssociation.savePeerASPIdentifier(params.NewAspIdentifier(9))
		close(releaseAuthorization)

		select {
		case registered := <-result:
			if registered.Status != RegistrationPermissionDenied || registered.RoutingContext != 0 {
				t.Fatalf("concurrent Registration = %+v, want Permission Denied", registered)
			}
		case <-time.After(time.Second):
			t.Fatal("Registration did not finish")
		}
	})

	t.Run("registration commits before claim", func(t *testing.T) {
		applicationServers, registry, requestingAssociation, staticAssociation, provisionedKey := newFixture(t,
			func(RoutingKeyRegistrationRequest) RegistrationStatus {
				return RegistrationSuccessfullyRegistered
			})
		registered := registry.register(requestingAssociation, []RoutingKeyRegistrationRequest{{
			LocalRoutingKeyIdentifier: 1,
			RoutingKey:                provisionedKey,
		}})[0]
		if registered.Status != RegistrationSuccessfullyRegistered || registered.RoutingContext != 7 {
			t.Fatalf("Registration = %+v, want successful Routing Context 7", registered)
		}
		if applicationServers.claimASPIdentifier(staticAssociation, 9) {
			t.Fatal("static ASP Identifier claim converged with an RKM member using the same identifier")
		}
	})
}

func TestRoutingKeyRegistryDeregistrationAuthorization(t *testing.T) {
	var authorized RoutingKeyDeregistrationRequest
	allow := false
	registry, err := newRoutingKeyRegistry(&RoutingKeyManagementConfig{
		AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
			return RegistrationSuccessfullyRegistered
		},
		AuthorizeDeregistration: func(request RoutingKeyDeregistrationRequest) bool {
			authorized = request
			return allow
		},
		AllowDynamicRoutingKeys: true,
	})
	if err != nil {
		t.Fatalf("newRoutingKeyRegistry: %v", err)
	}
	association := newAssociation(RoleSGP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	registered := registry.register(association, []RoutingKeyRegistrationRequest{{
		LocalRoutingKeyIdentifier: 1,
		RoutingKey:                testRoutingKey(10, 100, 3),
	}})[0]

	denied := registry.deregister(association, []uint32{registered.RoutingContext})[0]
	if denied.Status != DeregistrationPermissionDenied {
		t.Fatalf("denied Deregistration Status = %d, want Permission Denied", denied.Status)
	}
	if authorized.Peer.Role != RoleASP || authorized.RoutingContext != registered.RoutingContext ||
		authorized.RoutingKey.Groups[0].DestinationPointCode != 100 {
		t.Fatalf("Deregistration authorization request = %+v", authorized)
	}
	allow = true
	if got := registry.deregister(association, []uint32{registered.RoutingContext})[0].Status; got != DeregistrationSuccessfullyDeregistered {
		t.Fatalf("authorized Deregistration Status = %d, want success", got)
	}
}

func TestRoutingKeyRegistryPoliciesRunOutsideStateLock(t *testing.T) {
	var registry *routingKeyRegistry
	var err error
	registry, err = newRoutingKeyRegistry(&RoutingKeyManagementConfig{
		AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
			_ = registry.dynamicCount()
			return RegistrationSuccessfullyRegistered
		},
		AuthorizeDeregistration: func(RoutingKeyDeregistrationRequest) bool {
			_ = registry.dynamicCount()
			return true
		},
		AllowDynamicRoutingKeys: true,
	})
	if err != nil {
		t.Fatalf("newRoutingKeyRegistry: %v", err)
	}
	association := newAssociation(RoleSGP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	done := make(chan struct{})
	go func() {
		defer close(done)
		result := registry.register(association, []RoutingKeyRegistrationRequest{{
			LocalRoutingKeyIdentifier: 1,
			RoutingKey:                testRoutingKey(10, 100, params.ServiceIndSCCP),
		}})[0]
		if result.Status != RegistrationSuccessfullyRegistered {
			t.Errorf("registration result = %+v, want success", result)
			return
		}
		if got := registry.deregister(association, []uint32{result.RoutingContext})[0].Status; got != DeregistrationSuccessfullyDeregistered {
			t.Errorf("deregistration status = %d, want success", got)
		}
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Routing Key policy deadlocked while reading registry state")
	}
}

func TestRoutingKeyRegistryRegistrationPoliciesCanCloseAssociation(t *testing.T) {
	for _, policy := range []string{"authorization", "allocation"} {
		t.Run(policy, func(t *testing.T) {
			var association *Association
			config := &RoutingKeyManagementConfig{
				AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
					if policy == "authorization" {
						_ = association.Close()
					}
					return RegistrationSuccessfullyRegistered
				},
				AllowDynamicRoutingKeys: true,
				RemoveUnusedRoutingKeys: true,
			}
			if policy == "allocation" {
				config.AllocateRoutingContext = func(RoutingContextAllocationRequest) (uint32, error) {
					_ = association.Close()
					return 7, nil
				}
			}
			endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP, RoutingKeyManagement: config})
			if err != nil {
				t.Fatalf("NewEndpoint: %v", err)
			}
			defer func() { _ = endpoint.Close() }()
			association = newAssociation(RoleSGP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
			association.as = endpoint.as
			if !endpoint.trackAssociation(association) {
				t.Fatal("trackAssociation returned false")
			}

			done := make(chan struct{})
			go func() {
				defer close(done)
				endpoint.routingKeys.register(association, []RoutingKeyRegistrationRequest{{
					LocalRoutingKeyIdentifier: 1,
					RoutingKey:                testRoutingKey(10, 100, params.ServiceIndSCCP),
				}})
			}()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("Registration policy deadlocked while closing the association")
			}
			if endpoint.routingKeys.dynamicCount() != 0 {
				t.Fatal("closed association retained a dynamically registered Routing Key")
			}
		})
	}
}

func TestRoutingKeyRegistryDeregistrationPolicyCanCloseAssociation(t *testing.T) {
	var association *Association
	endpoint, err := NewEndpoint(EndpointConfig{
		Role: RoleSGP,
		RoutingKeyManagement: &RoutingKeyManagementConfig{
			AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
				return RegistrationSuccessfullyRegistered
			},
			AuthorizeDeregistration: func(RoutingKeyDeregistrationRequest) bool {
				_ = association.Close()
				return true
			},
			AllowDynamicRoutingKeys: true,
			RemoveUnusedRoutingKeys: true,
		},
	})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	defer func() { _ = endpoint.Close() }()
	association = newAssociation(RoleSGP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	association.as = endpoint.as
	if !endpoint.trackAssociation(association) {
		t.Fatal("trackAssociation returned false")
	}
	registration := endpoint.routingKeys.register(association, []RoutingKeyRegistrationRequest{{
		LocalRoutingKeyIdentifier: 1,
		RoutingKey:                testRoutingKey(10, 100, params.ServiceIndSCCP),
	}})[0]
	if registration.Status != RegistrationSuccessfullyRegistered {
		t.Fatalf("Registration = %+v, want success", registration)
	}

	done := make(chan []RoutingKeyDeregistrationResult, 1)
	go func() {
		done <- endpoint.routingKeys.deregister(association, []uint32{registration.RoutingContext, 999})
	}()
	select {
	case results := <-done:
		for index, routingContext := range []uint32{registration.RoutingContext, 999} {
			if results[index].RoutingContext != routingContext || results[index].Status != DeregistrationStatusUnknown {
				t.Errorf("Deregistration result %d = %+v, want Routing Context %d with unknown status", index, results[index], routingContext)
			}
		}
	case <-time.After(time.Second):
		t.Fatal("Deregistration policy deadlocked while closing the association")
	}
	if endpoint.routingKeys.dynamicCount() != 0 {
		t.Fatal("closed association retained a dynamically registered Routing Key")
	}
}

func TestRoutingKeyRegistryRegistrationAuthorizationIsNotRepeatedAfterConcurrentTeardown(t *testing.T) {
	var victim *Association
	policyCalls := 0
	triggerTeardown := false
	endpoint, err := NewEndpoint(EndpointConfig{
		Role: RoleSGP,
		RoutingKeyManagement: &RoutingKeyManagementConfig{
			AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
				if triggerTeardown {
					policyCalls++
					_ = victim.Close()
				}
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
	victim = newTrackedRoutingKeyAssociation(t, endpoint)
	registerRoutingKeyForTest(t, endpoint.routingKeys, victim, 1, testRoutingKey(10, 100, params.ServiceIndSCCP))
	target := newTrackedRoutingKeyAssociation(t, endpoint)

	triggerTeardown = true
	result := registerRoutingKeyForTest(t, endpoint.routingKeys, target, 2, testRoutingKey(10, 200, params.ServiceIndSCCP))
	if policyCalls != 1 {
		t.Fatalf("Registration authorization calls = %d, want 1", policyCalls)
	}
	if endpoint.routingKeys.dynamicCount() != 1 {
		t.Fatalf("dynamic Routing Key count = %d, want only the target registration", endpoint.routingKeys.dynamicCount())
	}
	registered, ok := endpoint.routingKeys.routingKey(result.RoutingContext)
	if !ok || registered.Groups[0].DestinationPointCode != 200 {
		t.Fatalf("retained Routing Key = %+v, %t; want target DPC 200", registered, ok)
	}
}

func TestRoutingKeyRegistryAllocatorReevaluatesAfterConcurrentTeardown(t *testing.T) {
	var victim *Association
	allocatorCalls := 0
	triggerTeardown := false
	endpoint, err := NewEndpoint(EndpointConfig{
		Role: RoleSGP,
		RoutingKeyManagement: &RoutingKeyManagementConfig{
			AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
				return RegistrationSuccessfullyRegistered
			},
			AllocateRoutingContext: func(RoutingContextAllocationRequest) (uint32, error) {
				if triggerTeardown {
					allocatorCalls++
					_ = victim.Close()
					return 9, nil
				}
				return 7, nil
			},
			AllowDynamicRoutingKeys: true,
			RemoveUnusedRoutingKeys: true,
		},
	})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	defer func() { _ = endpoint.Close() }()
	victim = newTrackedRoutingKeyAssociation(t, endpoint)
	registerRoutingKeyForTest(t, endpoint.routingKeys, victim, 1, testRoutingKey(10, 100, params.ServiceIndSCCP))
	target := newTrackedRoutingKeyAssociation(t, endpoint)

	triggerTeardown = true
	result := registerRoutingKeyForTest(t, endpoint.routingKeys, target, 2, testRoutingKey(10, 200, params.ServiceIndSCCP))
	if allocatorCalls != 2 {
		t.Fatalf("Routing Context allocator calls = %d, want 2 snapshots", allocatorCalls)
	}
	if result.RoutingContext != 9 {
		t.Fatalf("allocated Routing Context = %d, want 9", result.RoutingContext)
	}
	if endpoint.routingKeys.dynamicCount() != 1 {
		t.Fatalf("dynamic Routing Key count = %d, want only the target registration", endpoint.routingKeys.dynamicCount())
	}
}

func TestRoutingKeyRegistryDeregistrationAuthorizationIsNotRepeatedAfterConcurrentTeardown(t *testing.T) {
	var victim *Association
	policyCalls := 0
	endpoint, err := NewEndpoint(EndpointConfig{
		Role: RoleSGP,
		RoutingKeyManagement: &RoutingKeyManagementConfig{
			AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
				return RegistrationSuccessfullyRegistered
			},
			AuthorizeDeregistration: func(RoutingKeyDeregistrationRequest) bool {
				policyCalls++
				_ = victim.Close()
				return true
			},
			AllowDynamicRoutingKeys: true,
			RemoveUnusedRoutingKeys: true,
		},
	})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	defer func() { _ = endpoint.Close() }()
	victim = newTrackedRoutingKeyAssociation(t, endpoint)
	registerRoutingKeyForTest(t, endpoint.routingKeys, victim, 1, testRoutingKey(10, 100, params.ServiceIndSCCP))
	target := newTrackedRoutingKeyAssociation(t, endpoint)
	targetRegistration := registerRoutingKeyForTest(
		t,
		endpoint.routingKeys,
		target,
		2,
		testRoutingKey(10, 200, params.ServiceIndSCCP),
	)

	result := endpoint.routingKeys.deregister(target, []uint32{targetRegistration.RoutingContext})[0]
	if result.Status != DeregistrationSuccessfullyDeregistered {
		t.Fatalf("Deregistration = %+v, want success", result)
	}
	if policyCalls != 1 {
		t.Fatalf("Deregistration authorization calls = %d, want 1", policyCalls)
	}
	if endpoint.routingKeys.dynamicCount() != 0 {
		t.Fatalf("dynamic Routing Key count = %d, want 0", endpoint.routingKeys.dynamicCount())
	}
}

func TestRoutingKeyRegistryReauthorizesDeregistrationAfterTrafficModeAdoption(t *testing.T) {
	provisionedKey := testRoutingKey(10, 100, params.ServiceIndSCCP)
	policyEntered := make(chan struct{})
	releasePolicy := make(chan struct{})
	var policyCalls atomic.Int32
	registry, err := newRoutingKeyRegistry(&RoutingKeyManagementConfig{
		AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
			return RegistrationSuccessfullyRegistered
		},
		AuthorizeDeregistration: func(request RoutingKeyDeregistrationRequest) bool {
			if policyCalls.Add(1) == 1 {
				close(policyEntered)
				<-releasePolicy
			}
			return request.RoutingKey.TrafficModeSet &&
				request.RoutingKey.TrafficMode == params.TrafficModeLoadshare
		},
		ProvisionedRoutingKeys: []ProvisionedRoutingKey{{RoutingContext: 7, RoutingKey: provisionedKey}},
	})
	if err != nil {
		t.Fatalf("newRoutingKeyRegistry: %v", err)
	}
	firstAssociation := newAssociation(RoleSGP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	registerRoutingKeyForTest(t, registry, firstAssociation, 1, provisionedKey)

	deregistrationDone := make(chan RoutingKeyDeregistrationResult, 1)
	go func() {
		deregistrationDone <- registry.deregister(firstAssociation, []uint32{7})[0]
	}()
	select {
	case <-policyEntered:
	case <-time.After(time.Second):
		t.Fatal("Deregistration authorization was not called")
	}

	secondAssociation := newAssociation(RoleSGP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	requestKey := snapshotRoutingKey(provisionedKey)
	requestKey.TrafficMode = params.TrafficModeLoadshare
	requestKey.TrafficModeSet = true
	registerRoutingKeyForTest(t, registry, secondAssociation, 2, requestKey)
	close(releasePolicy)

	select {
	case result := <-deregistrationDone:
		if result.Status != DeregistrationSuccessfullyDeregistered {
			t.Fatalf("Deregistration = %+v, want success after Traffic Mode adoption", result)
		}
	case <-time.After(time.Second):
		t.Fatal("Deregistration did not finish")
	}
	if calls := policyCalls.Load(); calls != 2 {
		t.Fatalf("Deregistration authorization calls = %d, want 2 requests with distinct Traffic Modes", calls)
	}
}

func newTrackedRoutingKeyAssociation(t *testing.T, endpoint *Endpoint) *Association {
	t.Helper()
	association := newAssociation(RoleSGP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	association.as = endpoint.as
	if !endpoint.trackAssociation(association) {
		t.Fatal("trackAssociation returned false")
	}
	return association
}

func registerRoutingKeyForTest(
	t *testing.T,
	registry *routingKeyRegistry,
	association *Association,
	identifier uint32,
	routingKey RoutingKey,
) RoutingKeyRegistrationResult {
	t.Helper()
	result := registry.register(association, []RoutingKeyRegistrationRequest{{
		LocalRoutingKeyIdentifier: identifier,
		RoutingKey:                routingKey,
	}})[0]
	if result.Status != RegistrationSuccessfullyRegistered {
		t.Fatalf("Registration = %+v, want success", result)
	}
	return result
}

func TestRoutingKeyRegistrySerializesConcurrentRegistrations(t *testing.T) {
	registry, err := newRoutingKeyRegistry(&RoutingKeyManagementConfig{
		AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
			return RegistrationSuccessfullyRegistered
		},
		AllowDynamicRoutingKeys: true,
		MaxDynamicRoutingKeys:   64,
	})
	if err != nil {
		t.Fatalf("newRoutingKeyRegistry: %v", err)
	}

	const registrations = 32
	results := make(chan RoutingKeyRegistrationResult, registrations)
	var waitGroup sync.WaitGroup
	for index := 0; index < registrations; index++ {
		waitGroup.Add(1)
		go func(index int) {
			defer waitGroup.Done()
			association := newAssociation(RoleSGP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
			results <- registry.register(association, []RoutingKeyRegistrationRequest{{
				LocalRoutingKeyIdentifier: uint32(index + 1),
				RoutingKey:                testRoutingKey(10, uint32(100+index), params.ServiceIndSCCP),
			}})[0]
		}(index)
	}
	waitGroup.Wait()
	close(results)

	seenRoutingContexts := make(map[uint32]struct{}, registrations)
	for result := range results {
		if result.Status != RegistrationSuccessfullyRegistered || result.RoutingContext == 0 {
			t.Fatalf("concurrent registration result = %+v, want success", result)
		}
		if _, duplicate := seenRoutingContexts[result.RoutingContext]; duplicate {
			t.Fatalf("Routing Context %d was allocated more than once", result.RoutingContext)
		}
		seenRoutingContexts[result.RoutingContext] = struct{}{}
	}
	if len(seenRoutingContexts) != registrations || registry.dynamicCount() != registrations {
		t.Fatalf("Routing Context count = %d, dynamic count = %d, want %d", len(seenRoutingContexts), registry.dynamicCount(), registrations)
	}
}

func TestRoutingKeyRegistryReallocatesAfterConcurrentCommit(t *testing.T) {
	var allocatorCalls atomic.Int32
	var initialCalls sync.WaitGroup
	initialCalls.Add(2)
	releaseInitial := make(chan struct{})
	registry, err := newRoutingKeyRegistry(&RoutingKeyManagementConfig{
		AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
			return RegistrationSuccessfullyRegistered
		},
		AllocateRoutingContext: func(request RoutingContextAllocationRequest) (uint32, error) {
			if allocatorCalls.Add(1) <= 2 {
				initialCalls.Done()
				<-releaseInitial
			}
			candidate := uint32(1)
			for _, inUse := range request.InUseRoutingContexts {
				if inUse == candidate {
					candidate++
				}
			}
			return candidate, nil
		},
		AllowDynamicRoutingKeys: true,
	})
	if err != nil {
		t.Fatalf("newRoutingKeyRegistry: %v", err)
	}

	results := make(chan RoutingKeyRegistrationResult, 2)
	for index := range 2 {
		go func(index int) {
			association := newAssociation(RoleSGP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
			results <- registry.register(association, []RoutingKeyRegistrationRequest{{
				LocalRoutingKeyIdentifier: uint32(index + 1),
				RoutingKey:                testRoutingKey(10, uint32(100+index), params.ServiceIndSCCP),
			}})[0]
		}(index)
	}
	initialCalls.Wait()
	close(releaseInitial)
	first := <-results
	second := <-results
	if first.Status != RegistrationSuccessfullyRegistered || second.Status != RegistrationSuccessfullyRegistered {
		t.Fatalf("concurrent custom-allocation results = %+v, %+v; want both successful", first, second)
	}
	if first.RoutingContext == second.RoutingContext {
		t.Fatalf("concurrent custom allocator reused Routing Context %d", first.RoutingContext)
	}
	if allocatorCalls.Load() < 3 {
		t.Fatalf("custom allocator calls = %d, want a retry with the updated in-use snapshot", allocatorCalls.Load())
	}
}

func TestRoutingKeyRegistryAllowsOnlyOneAllNetworkAppearancesKeyPerAssociation(t *testing.T) {
	registry, err := newRoutingKeyRegistry(&RoutingKeyManagementConfig{
		AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
			return RegistrationSuccessfullyRegistered
		},
		AllowDynamicRoutingKeys: true,
	})
	if err != nil {
		t.Fatalf("newRoutingKeyRegistry: %v", err)
	}
	association := newAssociation(RoleSGP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	first := testRoutingKey(0, 100, params.ServiceIndSCCP)
	first.NetworkAppearanceSet = false
	second := testRoutingKey(0, 200, params.ServiceIndISUP)
	second.NetworkAppearanceSet = false

	results := registry.register(association, []RoutingKeyRegistrationRequest{
		{LocalRoutingKeyIdentifier: 1, RoutingKey: first},
		{LocalRoutingKeyIdentifier: 2, RoutingKey: second},
	})
	if results[0].Status != RegistrationSuccessfullyRegistered {
		t.Fatalf("first all-appearance registration = %+v, want success", results[0])
	}
	if results[1].Status != RegistrationCannotSupportUniqueRouting || results[1].RoutingContext != 0 {
		t.Fatalf("second all-appearance registration = %+v, want Cannot Support Unique Routing", results[1])
	}
	if registry.dynamicCount() != 1 {
		t.Fatalf("dynamic Routing Key count = %d, want 1", registry.dynamicCount())
	}
	matches, configured := registry.matchingASKeys(33, true, 50, 100, params.ServiceIndSCCP)
	if !configured || len(matches) != 1 || matches[0].RoutingContext != results[0].RoutingContext {
		t.Fatalf("all-appearance traffic matches = %v, configured = %t, want Routing Context %d", matches, configured, results[0].RoutingContext)
	}

	otherAssociation := newAssociation(RoleSGP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	explicit := testRoutingKey(10, 300, params.ServiceIndSCCP)
	allAppearances := testRoutingKey(0, 400, params.ServiceIndISUP)
	allAppearances.NetworkAppearanceSet = false
	results = registry.register(otherAssociation, []RoutingKeyRegistrationRequest{
		{LocalRoutingKeyIdentifier: 3, RoutingKey: explicit},
		{LocalRoutingKeyIdentifier: 4, RoutingKey: allAppearances},
	})
	if results[0].Status != RegistrationSuccessfullyRegistered || results[1].Status != RegistrationCannotSupportUniqueRouting {
		t.Fatalf("explicit-then-all registration results = %+v, want success then Cannot Support Unique Routing", results)
	}
}

func TestRoutingKeyRegistryBoundsDeregistrationReplayState(t *testing.T) {
	nextRoutingContext := uint32(1)
	registry, err := newRoutingKeyRegistry(&RoutingKeyManagementConfig{
		AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
			return RegistrationSuccessfullyRegistered
		},
		AllocateRoutingContext: func(RoutingContextAllocationRequest) (uint32, error) {
			routingContext := nextRoutingContext
			nextRoutingContext++
			return routingContext, nil
		},
		AllowDynamicRoutingKeys: true,
		RemoveUnusedRoutingKeys: true,
	})
	if err != nil {
		t.Fatalf("newRoutingKeyRegistry: %v", err)
	}
	association := newAssociation(RoleSGP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	var firstRoutingContext uint32
	for index := 0; index <= deregistrationReplayLimit; index++ {
		registration := registry.register(association, []RoutingKeyRegistrationRequest{{
			LocalRoutingKeyIdentifier: uint32(index + 1),
			RoutingKey:                testRoutingKey(10, uint32(100+index), params.ServiceIndSCCP),
		}})[0]
		if registration.Status != RegistrationSuccessfullyRegistered {
			t.Fatalf("registration %d = %+v, want success", index, registration)
		}
		if index == 0 {
			firstRoutingContext = registration.RoutingContext
		}
		deregistration := registry.deregister(association, []uint32{registration.RoutingContext})[0]
		if deregistration.Status != DeregistrationSuccessfullyDeregistered {
			t.Fatalf("deregistration %d = %+v, want success", index, deregistration)
		}
		registry.deregistrationResponseWritten(association, []RoutingKeyDeregistrationResult{deregistration})
	}

	if replay := registry.deregister(association, []uint32{firstRoutingContext})[0]; replay.Status != DeregistrationInvalidRoutingContext {
		t.Fatalf("evicted Deregistration replay = %+v, want Invalid Routing Context", replay)
	}
}

func TestRoutingKeyRegistryNormalizesInvalidAuthorizationResults(t *testing.T) {
	for _, status := range []RegistrationStatus{
		RegistrationRoutingKeyAlreadyRegistered,
		RegistrationRoutingKeyAlreadyRegistered + 1,
	} {
		t.Run(fmt.Sprintf("status-%d", status), func(t *testing.T) {
			registry, err := newRoutingKeyRegistry(&RoutingKeyManagementConfig{
				AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
					return status
				},
				AllowDynamicRoutingKeys: true,
			})
			if err != nil {
				t.Fatalf("newRoutingKeyRegistry: %v", err)
			}
			association := newAssociation(RoleSGP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
			result := registry.register(association, []RoutingKeyRegistrationRequest{{
				LocalRoutingKeyIdentifier: 1,
				RoutingKey:                testRoutingKey(10, 100, params.ServiceIndSCCP),
			}})[0]
			if result.Status != RegistrationInvalidRoutingKey || result.RoutingContext != 0 {
				t.Fatalf("registration result = %+v, want Invalid Routing Key with Routing Context 0", result)
			}
		})
	}
}

func TestRoutingKeyRegistryAcceptsWideOriginatingPointCodeMask(t *testing.T) {
	authorizations := 0
	registry, err := newRoutingKeyRegistry(&RoutingKeyManagementConfig{
		AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
			authorizations++
			return RegistrationSuccessfullyRegistered
		},
		AllowDynamicRoutingKeys: true,
	})
	if err != nil {
		t.Fatalf("newRoutingKeyRegistry: %v", err)
	}
	association := newAssociation(RoleSGP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	key := testRoutingKey(10, 100, params.ServiceIndSCCP)
	key.Groups[0].OriginatingPointCodes = []PointCodeRange{{PointCode: 0x123456, Mask: 25}}

	result := registry.register(association, []RoutingKeyRegistrationRequest{{
		LocalRoutingKeyIdentifier: 1,
		RoutingKey:                key,
	}})[0]
	if result.Status != RegistrationSuccessfullyRegistered || result.RoutingContext == 0 {
		t.Fatalf("registration result = %+v, want success with nonzero Routing Context", result)
	}
	if authorizations != 1 {
		t.Fatalf("authorization calls = %d, want 1", authorizations)
	}
	matches, configured := registry.matchingASKeys(10, true, 0xffffff, 100, params.ServiceIndSCCP)
	if !configured || len(matches) != 1 || matches[0].RoutingContext != result.RoutingContext {
		t.Fatalf("all-OPC match = %v, configured = %t, want Routing Context %d", matches, configured, result.RoutingContext)
	}
}

func testRoutingKey(networkAppearance, destinationPointCode uint32, serviceIndicators ...uint8) RoutingKey {
	return RoutingKey{
		NetworkAppearance:    networkAppearance,
		NetworkAppearanceSet: true,
		Groups: []RoutingKeyGroup{{
			DestinationPointCode: destinationPointCode,
			ServiceIndicators:    append([]uint8(nil), serviceIndicators...),
		}},
	}
}
