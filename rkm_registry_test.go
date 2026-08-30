package m3ua

import (
	"errors"
	"fmt"
	"slices"
	"sync"
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
		if deregistration := registry.deregister(association, []uint32{registration.RoutingContext})[0]; deregistration.Status != DeregistrationSuccessfullyDeregistered {
			t.Fatalf("deregistration %d = %+v, want success", index, deregistration)
		}
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
