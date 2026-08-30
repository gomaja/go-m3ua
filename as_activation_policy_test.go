package m3ua

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestEndpointSnapshotsApplicationServerConfiguration(t *testing.T) {
	exactKey := ASKey{
		NetworkAppearance:    10,
		NetworkAppearanceSet: true,
		RoutingContext:       1,
		RoutingContextSet:    true,
	}
	policies := map[ASKey]ASActivationPolicy{
		exactKey: {RequiredActiveASPs: 3, SmoothStart: true},
	}
	config := &ApplicationServerConfig{
		RecoveryTimer:           3 * time.Second,
		DefaultActivationPolicy: ASActivationPolicy{RequiredActiveASPs: 2},
		ActivationPolicies:      policies,
	}

	endpoint, err := NewEndpoint(EndpointConfig{
		Role:               RoleSGP,
		ApplicationServers: config,
	})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })

	config.RecoveryTimer = time.Hour
	config.DefaultActivationPolicy.RequiredActiveASPs = 7
	policies[exactKey] = ASActivationPolicy{RequiredActiveASPs: 9}
	config.ActivationPolicies = nil

	if got := endpoint.as.recoveryTimer; got != 3*time.Second {
		t.Fatalf("RecoveryTimer = %v, want 3s", got)
	}
	defaultAS := endpoint.as.get(ASKey{RoutingContext: 2, RoutingContextSet: true})
	if got := defaultAS.requiredActive(); got != 2 {
		t.Fatalf("default required active ASPs = %d, want 2", got)
	}
	if defaultAS.activationPolicy.SmoothStart {
		t.Fatal("default policy unexpectedly enabled smooth start")
	}
	exactAS := endpoint.as.get(exactKey)
	if got := exactAS.requiredActive(); got != 3 {
		t.Fatalf("exact required active ASPs = %d, want 3", got)
	}
	if !exactAS.activationPolicy.SmoothStart {
		t.Fatal("exact policy did not retain smooth start")
	}
}

func TestApplicationServerConfigurationUsesExactASKey(t *testing.T) {
	firstKey := ASKey{
		NetworkAppearance:    10,
		NetworkAppearanceSet: true,
		RoutingContext:       1,
		RoutingContextSet:    true,
	}
	secondKey := ASKey{
		NetworkAppearance:    20,
		NetworkAppearanceSet: true,
		RoutingContext:       1,
		RoutingContextSet:    true,
	}
	contextlessKey := ASKey{
		NetworkAppearance:    10,
		NetworkAppearanceSet: true,
	}
	endpoint, err := NewEndpoint(EndpointConfig{
		Role: RoleIPSP,
		ApplicationServers: &ApplicationServerConfig{
			DefaultActivationPolicy: ASActivationPolicy{RequiredActiveASPs: 2},
			ActivationPolicies: map[ASKey]ASActivationPolicy{
				firstKey:       {RequiredActiveASPs: 3},
				secondKey:      {RequiredActiveASPs: 4},
				contextlessKey: {RequiredActiveASPs: 5},
			},
		},
	})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })

	for key, want := range map[ASKey]int{
		firstKey:       3,
		secondKey:      4,
		contextlessKey: 5,
		{NetworkAppearance: 30, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true}: 2,
	} {
		if got := endpoint.as.get(key).requiredActive(); got != want {
			t.Errorf("policy for %+v = %d, want %d", key, got, want)
		}
	}
}

func TestEndpointValidatesApplicationServerConfiguration(t *testing.T) {
	tests := []struct {
		name   string
		config EndpointConfig
		want   error
	}{
		{
			name: "ASP role",
			config: EndpointConfig{
				Role:               RoleASP,
				ApplicationServers: &ApplicationServerConfig{},
			},
			want: ErrInvalidRoleConfiguration,
		},
		{
			name: "negative recovery timer",
			config: EndpointConfig{
				Role: RoleSGP,
				ApplicationServers: &ApplicationServerConfig{
					RecoveryTimer: -time.Second,
				},
			},
			want: ErrInvalidApplicationServerConfig,
		},
		{
			name: "negative default n",
			config: EndpointConfig{
				Role: RoleSGP,
				ApplicationServers: &ApplicationServerConfig{
					DefaultActivationPolicy: ASActivationPolicy{RequiredActiveASPs: -1},
				},
			},
			want: ErrInvalidApplicationServerConfig,
		},
		{
			name: "negative exact n",
			config: EndpointConfig{
				Role: RoleIPSP,
				ApplicationServers: &ApplicationServerConfig{
					ActivationPolicies: map[ASKey]ASActivationPolicy{
						{RoutingContext: 1, RoutingContextSet: true}: {RequiredActiveASPs: -1},
					},
				},
			},
			want: ErrInvalidApplicationServerConfig,
		},
		{
			name: "Network Appearance value without presence",
			config: EndpointConfig{
				Role: RoleSGP,
				ApplicationServers: &ApplicationServerConfig{
					ActivationPolicies: map[ASKey]ASActivationPolicy{
						{NetworkAppearance: 10, RoutingContext: 1, RoutingContextSet: true}: {},
					},
				},
			},
			want: ErrInvalidApplicationServerConfig,
		},
		{
			name: "Routing Context value without presence",
			config: EndpointConfig{
				Role: RoleSGP,
				ApplicationServers: &ApplicationServerConfig{
					ActivationPolicies: map[ASKey]ASActivationPolicy{
						{RoutingContext: 1}: {},
					},
				},
			},
			want: ErrInvalidApplicationServerConfig,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			endpoint, err := NewEndpoint(test.config)
			if endpoint != nil {
				_ = endpoint.Close()
				t.Fatalf("NewEndpoint returned %#v", endpoint)
			}
			if !errors.Is(err, test.want) {
				t.Fatalf("NewEndpoint error = %v, want %v", err, test.want)
			}
		})
	}

	for _, role := range []Role{RoleSGP, RoleIPSP} {
		endpoint, err := NewEndpoint(EndpointConfig{
			Role:               role,
			ApplicationServers: &ApplicationServerConfig{},
		})
		if err != nil {
			t.Fatalf("NewEndpoint(%s): %v", role, err)
		}
		_ = endpoint.Close()
	}
}

func TestEndpointRejectsProvisionedOverrideWhenNExceedsOne(t *testing.T) {
	routingKey := testRoutingKey(10, 100, params.ServiceIndSCCP)
	routingKey.TrafficMode = params.TrafficModeOverride
	routingKey.TrafficModeSet = true
	endpoint, err := NewEndpoint(EndpointConfig{
		Role: RoleSGP,
		ApplicationServers: &ApplicationServerConfig{
			ActivationPolicies: map[ASKey]ASActivationPolicy{
				{
					NetworkAppearance:    10,
					NetworkAppearanceSet: true,
					RoutingContext:       7,
					RoutingContextSet:    true,
				}: {RequiredActiveASPs: 2},
			},
		},
		RoutingKeyManagement: &RoutingKeyManagementConfig{
			AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
				return RegistrationSuccessfullyRegistered
			},
			ProvisionedRoutingKeys: []ProvisionedRoutingKey{{
				RoutingContext: 7,
				RoutingKey:     routingKey,
			}},
		},
	})
	if endpoint != nil {
		_ = endpoint.Close()
		t.Fatalf("NewEndpoint returned %#v", endpoint)
	}
	if !errors.Is(err, ErrInvalidApplicationServerConfig) {
		t.Fatalf("NewEndpoint error = %v, want %v", err, ErrInvalidApplicationServerConfig)
	}
}

func TestEndpointRejectsAssociationOverrideWhenNExceedsOne(t *testing.T) {
	tests := []struct {
		name   string
		role   Role
		config *AssociationConfig
	}{
		{
			name: "SGP",
			role: RoleSGP,
			config: func() *AssociationConfig {
				config := NewAssociationConfig(0, 0, 0, 0, 0, 0)
				config.RoutingContexts = params.NewRoutingContext(1)
				config.TrafficModeType = params.NewTrafficModeType(params.TrafficModeOverride)
				return config
			}(),
		},
		{
			name: "IPSP Single Exchange",
			role: RoleIPSP,
			config: func() *AssociationConfig {
				config := NewAssociationConfig(0, 0, 0, 0, 0, 0)
				config.RoutingContexts = params.NewRoutingContext(1)
				config.TrafficModeType = params.NewTrafficModeType(params.TrafficModeOverride)
				config.IPSP = &IPSPConfig{ExchangeModel: IPSPExchangeSingle}
				return config
			}(),
		},
		{
			name: "IPSP Double Exchange peer direction",
			role: RoleIPSP,
			config: func() *AssociationConfig {
				config := NewAssociationConfig(0, 0, 0, 0, 0, 0)
				config.IPSP = &IPSPConfig{
					ExchangeModel: IPSPExchangeDouble,
					ASPSMExchange: IPSPASPSMExchangeDouble,
					TrafficToPeer: &IPSPTrafficConfig{
						RoutingContexts: params.NewRoutingContext(1),
						TrafficModeType: params.NewTrafficModeType(params.TrafficModeOverride),
					},
				}
				return config
			}(),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			endpoint, err := NewEndpoint(EndpointConfig{
				Role: test.role,
				ApplicationServers: &ApplicationServerConfig{
					DefaultActivationPolicy: ASActivationPolicy{RequiredActiveASPs: 2},
				},
			})
			if err != nil {
				t.Fatalf("NewEndpoint: %v", err)
			}
			t.Cleanup(func() { _ = endpoint.Close() })

			if err := endpoint.validateAssociationConfig(test.config); !errors.Is(err, ErrInvalidApplicationServerConfig) {
				t.Fatalf("validateAssociationConfig error = %v, want %v",
					err, ErrInvalidApplicationServerConfig)
			}
		})
	}
}

func TestAssociationOverrideValidationUsesExactASKey(t *testing.T) {
	restrictedKey := ASKey{
		NetworkAppearance:    10,
		NetworkAppearanceSet: true,
		RoutingContext:       1,
		RoutingContextSet:    true,
	}
	endpoint, err := NewEndpoint(EndpointConfig{
		Role: RoleSGP,
		ApplicationServers: &ApplicationServerConfig{
			ActivationPolicies: map[ASKey]ASActivationPolicy{
				restrictedKey: {RequiredActiveASPs: 2},
			},
		},
	})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })

	restricted := NewAssociationConfig(0, 0, 0, 0, 0, 0)
	restricted.NetworkAppearance = params.NewNetworkAppearance(10)
	restricted.RoutingContexts = params.NewRoutingContext(1)
	restricted.TrafficModes = map[uint32]uint32{1: params.TrafficModeOverride}
	if err := endpoint.validateAssociationConfig(restricted); !errors.Is(err, ErrInvalidApplicationServerConfig) {
		t.Fatalf("restricted AS error = %v, want %v", err, ErrInvalidApplicationServerConfig)
	}

	permitted := snapshotAssociationConfig(restricted)
	permitted.NetworkAppearance = params.NewNetworkAppearance(20)
	if err := endpoint.validateAssociationConfig(permitted); err != nil {
		t.Fatalf("same Routing Context in another Network Appearance: %v", err)
	}
}

func TestSGPDefersOverrideValidationUntilASPAuthorization(t *testing.T) {
	restrictedKey := ASKey{RoutingContext: 1, RoutingContextSet: true}
	endpoint, err := NewEndpoint(EndpointConfig{
		Role: RoleSGP,
		ApplicationServers: &ApplicationServerConfig{
			ActivationPolicies: map[ASKey]ASActivationPolicy{
				restrictedKey: {RequiredActiveASPs: 2},
			},
		},
	})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })

	configFor := func(authorized uint32) *AssociationConfig {
		config := NewAssociationConfig(0, 0, 0, 0, 0, 0)
		config.RoutingContexts = params.NewRoutingContext(1, 2)
		config.TrafficModeType = params.NewTrafficModeType(params.TrafficModeOverride)
		config.AuthorizeASP = func(ASPIdentity) []uint32 { return []uint32{authorized} }
		return config
	}

	permittedConfig := configFor(2)
	if err := endpoint.validateAssociationConfig(permittedConfig); err != nil {
		t.Fatalf("pre-authorization validation rejected a potentially valid peer: %v", err)
	}
	permitted := newAssociation(RoleSGP, permittedConfig)
	permitted.as = endpoint.as
	if err := permitted.resolveASPAuthorization(params.NewAspIdentifier(7)); err != nil {
		t.Fatalf("authorization for valid AS 2: %v", err)
	}
	if got := permitted.configuredRoutingContexts(); !equalTrafficModeContexts(got, []uint32{2}) {
		t.Fatalf("authorized Routing Contexts = %v, want [2]", got)
	}

	restrictedConfig := configFor(1)
	if err := endpoint.validateAssociationConfig(restrictedConfig); err != nil {
		t.Fatalf("pre-authorization validation rejected before identity resolution: %v", err)
	}
	restricted := newAssociation(RoleSGP, restrictedConfig)
	restricted.as = endpoint.as
	if err := restricted.resolveASPAuthorization(params.NewAspIdentifier(8)); !errors.Is(err, ErrInvalidApplicationServerConfig) {
		t.Fatalf("authorization for restricted AS 1 error = %v, want %v",
			err, ErrInvalidApplicationServerConfig)
	}
	if restricted.authorizationResolved {
		t.Fatal("invalid authorization was committed")
	}
}

func TestContextlessAssociationOverrideValidationUsesDefaultTrafficMode(t *testing.T) {
	tests := []struct {
		name        string
		role        Role
		defaultMode uint32
		zeroMode    uint32
		want        error
	}{
		{
			name:        "SGP rejects default Override",
			role:        RoleSGP,
			defaultMode: params.TrafficModeOverride,
			zeroMode:    params.TrafficModeLoadshare,
			want:        ErrInvalidApplicationServerConfig,
		},
		{
			name:        "SGP permits default Loadshare",
			role:        RoleSGP,
			defaultMode: params.TrafficModeLoadshare,
			zeroMode:    params.TrafficModeOverride,
		},
		{
			name:        "IPSP Double Exchange rejects peer default Override",
			role:        RoleIPSP,
			defaultMode: params.TrafficModeOverride,
			zeroMode:    params.TrafficModeLoadshare,
			want:        ErrInvalidApplicationServerConfig,
		},
		{
			name:        "IPSP Double Exchange permits peer default Loadshare",
			role:        RoleIPSP,
			defaultMode: params.TrafficModeLoadshare,
			zeroMode:    params.TrafficModeOverride,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			endpoint, err := NewEndpoint(EndpointConfig{
				Role: test.role,
				ApplicationServers: &ApplicationServerConfig{
					DefaultActivationPolicy: ASActivationPolicy{RequiredActiveASPs: 2},
				},
			})
			if err != nil {
				t.Fatalf("NewEndpoint: %v", err)
			}
			t.Cleanup(func() { _ = endpoint.Close() })

			config := NewAssociationConfig(0, 0, 0, 0, 0, 0)
			if test.role == RoleIPSP {
				config.IPSP = &IPSPConfig{
					ExchangeModel: IPSPExchangeDouble,
					ASPSMExchange: IPSPASPSMExchangeDouble,
					TrafficToPeer: &IPSPTrafficConfig{
						TrafficModeType: params.NewTrafficModeType(test.defaultMode),
						TrafficModes:    map[uint32]uint32{0: test.zeroMode},
					},
				}
			} else {
				config.TrafficModeType = params.NewTrafficModeType(test.defaultMode)
				config.TrafficModes = map[uint32]uint32{0: test.zeroMode}
			}

			err = endpoint.validateAssociationConfig(config)
			if !errors.Is(err, test.want) {
				t.Fatalf("validateAssociationConfig error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestIPSPDoubleExchangeLocalOverrideDoesNotUsePeerActivationPolicy(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{
		Role: RoleIPSP,
		ApplicationServers: &ApplicationServerConfig{
			DefaultActivationPolicy: ASActivationPolicy{RequiredActiveASPs: 2},
		},
	})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })

	config := NewAssociationConfig(0, 0, 0, 0, 0, 0)
	config.IPSP = &IPSPConfig{
		ExchangeModel: IPSPExchangeDouble,
		ASPSMExchange: IPSPASPSMExchangeDouble,
		TrafficToLocal: &IPSPTrafficConfig{
			RoutingContexts: params.NewRoutingContext(1),
			TrafficModeType: params.NewTrafficModeType(params.TrafficModeOverride),
		},
	}
	if err := endpoint.validateAssociationConfig(config); err != nil {
		t.Fatalf("local ASP direction rejected by peer AS policy: %v", err)
	}
}

func TestApplicationServerStrictStartupAndRunningThreshold(t *testing.T) {
	for _, mode := range []uint32{params.TrafficModeLoadshare, params.TrafficModeBroadcast} {
		t.Run(fmt.Sprintf("mode-%d", mode), func(t *testing.T) {
			registry := applicationServerRegistryForActivationTest(t, ASActivationPolicy{RequiredActiveASPs: 2})
			applicationServer := registry.get(1)
			applicationServer.setTrafficMode(mode)
			first, firstSent := asTestConn(t, registry, StateASPInactive, 1)
			second, secondSent := asTestConn(t, registry, StateASPInactive, 1)
			firstBefore := len(*firstSent)
			secondBefore := len(*secondSent)

			registry.aspStateChanged(first, StateASPActive)
			if got := applicationServer.State(); got != ASInactive {
				t.Fatalf("state after first active ASP = %v, want %v", got, ASInactive)
			}
			if active := applicationServer.activeASPs(); len(active) != 0 {
				t.Fatalf("traffic targets below n = %d, want 0", len(active))
			}
			assertNoNotifyStatus(t, (*firstSent)[firstBefore:], params.AsStateActive)
			assertNoNotifyStatus(t, (*secondSent)[secondBefore:], params.AsStateActive)

			registry.aspStateChanged(second, StateASPActive)
			if got := applicationServer.State(); got != ASActive {
				t.Fatalf("state after second active ASP = %v, want %v", got, ASActive)
			}
			if active := applicationServer.activeASPs(); len(active) != 2 {
				t.Fatalf("traffic targets at n = %d, want 2", len(active))
			}
			assertNotifyStatus(t, (*firstSent)[firstBefore:], params.AsStateActive)
			assertNotifyStatus(t, (*secondSent)[secondBefore:], params.AsStateActive)

			registry.aspStateChanged(second, StateASPInactive)
			if got := applicationServer.State(); got != ASActive {
				t.Fatalf("state below n after traffic started = %v, want %v", got, ASActive)
			}
			if active := applicationServer.activeASPs(); len(active) != 1 || active[0] != first {
				t.Fatalf("traffic targets below n after startup = %v, want first ASP", active)
			}

			registry.aspStateChanged(first, StateASPInactive)
			if got := applicationServer.State(); got != ASPending {
				t.Fatalf("state after last active ASP withdrew = %v, want %v", got, ASPending)
			}
			if active := applicationServer.activeASPs(); len(active) != 0 {
				t.Fatalf("traffic targets while pending = %d, want 0", len(active))
			}
		})
	}
}

func TestApplicationServerStrictStartupWithOnlyOneKnownASP(t *testing.T) {
	registry := applicationServerRegistryForActivationTest(t, ASActivationPolicy{RequiredActiveASPs: 2})
	applicationServer := registry.get(1)
	applicationServer.setTrafficMode(params.TrafficModeLoadshare)
	first, _ := asTestConn(t, registry, StateASPInactive, 1)

	registry.aspStateChanged(first, StateASPActive)
	if got := applicationServer.State(); got != ASInactive {
		t.Fatalf("state with the only known ASP active below n = %v, want %v", got, ASInactive)
	}
	if active := applicationServer.activeASPs(); len(active) != 0 {
		t.Fatalf("traffic targets with the only known ASP active below n = %d, want 0", len(active))
	}
}

func TestASPUpReceivesCurrentASStateBelowThreshold(t *testing.T) {
	registry := applicationServerRegistryForActivationTest(t, ASActivationPolicy{RequiredActiveASPs: 2})
	applicationServer := registry.get(1)
	applicationServer.setTrafficMode(params.TrafficModeLoadshare)
	first, firstSent := asTestConn(t, registry, StateASPDown, 1)
	second, secondSent := asTestConn(t, registry, StateASPDown, 1)

	for index, test := range []struct {
		association *Association
		sent        *[]messages.M3UA
	}{
		{association: first, sent: firstSent},
		{association: second, sent: secondSent},
	} {
		before := len(*test.sent)
		if err := test.association.handleAspUp(messages.NewAspUp(nil, nil)); err != nil {
			t.Fatalf("ASP #%d handleAspUp: %v", index+1, err)
		}
		written := (*test.sent)[before:]
		if len(written) < 2 {
			t.Fatalf("ASP #%d messages = %v, want ASP Up Ack then AS-INACTIVE Notify",
				index+1, typeNames(written))
		}
		if _, ok := written[0].(*messages.AspUpAck); !ok {
			t.Fatalf("ASP #%d first message = %T, want *messages.AspUpAck", index+1, written[0])
		}
		notify, ok := written[1].(*messages.Notify)
		if !ok || notify.Status == nil || notify.Status.Status() != params.AsStateInactive {
			t.Fatalf("ASP #%d second message = %#v, want AS-INACTIVE Notify", index+1, written[1])
		}
	}
	if got := applicationServer.State(); got != ASInactive {
		t.Fatalf("AS state after two ASP Up exchanges = %v, want %v", got, ASInactive)
	}
}

func TestEstablishedWaitsForApplicationServerStatePublication(t *testing.T) {
	registry := applicationServerRegistryForActivationTest(t, ASActivationPolicy{})
	association, _ := asTestConn(t, registry, StateASPInactive, 1)

	registry.mu.Lock()
	updated := make(chan error, 1)
	go func() {
		updated <- association.handleStateUpdate(StateASPActive)
	}()

	if !waitFor(func() bool { return association.State() == StateASPActive }, time.Second) {
		registry.mu.Unlock()
		t.Fatal("association did not commit ASP-ACTIVE while AS publication was blocked")
	}
	early := false
	select {
	case <-association.established:
		early = true
	case <-time.After(100 * time.Millisecond):
	}
	registry.mu.Unlock()

	if early {
		t.Fatal("association became established before its Application Server state was published")
	}
	if err := <-updated; err != nil {
		t.Fatalf("ASP-ACTIVE state update: %v", err)
	}
	select {
	case <-association.established:
	case <-time.After(time.Second):
		t.Fatal("association did not become established after Application Server state publication")
	}
	if got := association.ApplicationServerStateForAS(ASKey{RoutingContext: 1, RoutingContextSet: true}); got != ASActive {
		t.Fatalf("Application Server state = %v, want %v", got, ASActive)
	}
}

func TestApplicationServerSmoothStartIsExplicit(t *testing.T) {
	registry := applicationServerRegistryForActivationTest(t, ASActivationPolicy{
		RequiredActiveASPs: 2,
		SmoothStart:        true,
	})
	applicationServer := registry.get(1)
	applicationServer.setTrafficMode(params.TrafficModeLoadshare)
	first, firstSent := asTestConn(t, registry, StateASPInactive, 1)
	_, secondSent := asTestConn(t, registry, StateASPInactive, 1)
	firstBefore := len(*firstSent)
	secondBefore := len(*secondSent)

	registry.aspStateChanged(first, StateASPActive)
	if got := applicationServer.State(); got != ASActive {
		t.Fatalf("smooth-start state after first active ASP = %v, want %v", got, ASActive)
	}
	if active := applicationServer.activeASPs(); len(active) != 1 || active[0] != first {
		t.Fatalf("smooth-start traffic targets = %v, want first ASP", active)
	}
	assertNotifyStatus(t, (*firstSent)[firstBefore:], params.AsStateActive)
	assertNotifyStatus(t, (*secondSent)[secondBefore:], params.AsStateActive)
	assertNoNotifyStatus(t, (*firstSent)[firstBefore:], params.InsufficientAspResources)
	assertNotifyStatus(t, (*secondSent)[secondBefore:], params.InsufficientAspResources)
}

func TestApplicationServerInsufficientResourcesAndRestoration(t *testing.T) {
	for _, mode := range []uint32{params.TrafficModeLoadshare, params.TrafficModeBroadcast} {
		t.Run(fmt.Sprintf("mode-%d", mode), func(t *testing.T) {
			registry := applicationServerRegistryForActivationTest(t, ASActivationPolicy{RequiredActiveASPs: 2})
			applicationServer := registry.get(1)
			applicationServer.setTrafficMode(mode)
			first, firstSent := asTestConn(t, registry, StateASPInactive, 1)
			second, secondSent := asTestConn(t, registry, StateASPInactive, 1)
			third, thirdSent := asTestConn(t, registry, StateASPInactive, 1)
			registry.aspStateChanged(first, StateASPActive)
			registry.aspStateChanged(second, StateASPActive)
			firstBefore := len(*firstSent)
			secondBefore := len(*secondSent)
			thirdBefore := len(*thirdSent)

			registry.aspStateChanged(first, StateASPInactive)
			if got := applicationServer.State(); got != ASActive {
				t.Fatalf("state during shortage = %v, want %v", got, ASActive)
			}
			assertNotifyStatus(t, (*firstSent)[firstBefore:], params.InsufficientAspResources)
			assertNoNotifyStatus(t, (*secondSent)[secondBefore:], params.InsufficientAspResources)
			assertNotifyStatus(t, (*thirdSent)[thirdBefore:], params.InsufficientAspResources)

			firstCount := len(*firstSent)
			thirdCount := len(*thirdSent)
			registry.aspStateChanged(first, StateASPInactive)
			if got := len(*firstSent); got != firstCount {
				t.Fatalf("duplicate inactive state wrote %d additional messages", got-firstCount)
			}
			if got := len(*thirdSent); got != thirdCount {
				t.Fatalf("duplicate inactive state wrote %d additional sibling messages", got-thirdCount)
			}

			firstBefore = len(*firstSent)
			secondBefore = len(*secondSent)
			thirdBefore = len(*thirdSent)
			registry.aspStateChanged(third, StateASPActive)
			for name, capture := range map[string][]messages.M3UA{
				"first":  (*firstSent)[firstBefore:],
				"second": (*secondSent)[secondBefore:],
				"third":  (*thirdSent)[thirdBefore:],
			} {
				if !hasNotifyStatus(capture, params.AsStateActive) {
					t.Errorf("%s ASP received no AS-ACTIVE restoration Notify", name)
				}
				if hasNotifyStatus(capture, params.InsufficientAspResources) {
					t.Errorf("%s ASP received an insufficient-resources Notify after restoration", name)
				}
			}

			firstBefore = len(*firstSent)
			secondBefore = len(*secondSent)
			thirdBefore = len(*thirdSent)
			registry.aspStateChanged(third, StateASPInactive)
			assertNotifyStatus(t, (*firstSent)[firstBefore:], params.InsufficientAspResources)
			assertNoNotifyStatus(t, (*secondSent)[secondBefore:], params.InsufficientAspResources)
			assertNotifyStatus(t, (*thirdSent)[thirdBefore:], params.InsufficientAspResources)
		})
	}
}

func TestInsufficientResourcesNotifyFollowsASPInactiveAck(t *testing.T) {
	registry := applicationServerRegistryForActivationTest(t, ASActivationPolicy{RequiredActiveASPs: 2})
	applicationServer := registry.get(1)
	applicationServer.setTrafficMode(params.TrafficModeLoadshare)
	withdrawing, withdrawingSent := asTestConn(t, registry, StateASPActive, 1)
	active, activeSent := asTestConn(t, registry, StateASPActive, 1)
	_, inactiveSent := asTestConn(t, registry, StateASPInactive, 1)
	withdrawingBefore := len(*withdrawingSent)
	activeBefore := len(*activeSent)
	inactiveBefore := len(*inactiveSent)

	if err := withdrawing.handleAspInactive(messages.NewAspInactive(params.NewRoutingContext(1), nil)); err != nil {
		t.Fatalf("handleAspInactive: %v", err)
	}

	withdrawingMessages := (*withdrawingSent)[withdrawingBefore:]
	if len(withdrawingMessages) < 2 {
		t.Fatalf("withdrawing ASP messages = %v, want Ack then Notify", typeNames(withdrawingMessages))
	}
	if _, ok := withdrawingMessages[0].(*messages.AspInactiveAck); !ok {
		t.Fatalf("first withdrawing ASP message = %T, want *messages.AspInactiveAck", withdrawingMessages[0])
	}
	notify, ok := withdrawingMessages[1].(*messages.Notify)
	if !ok || notify.Status == nil || notify.Status.Status() != params.InsufficientAspResources {
		t.Fatalf("second withdrawing ASP message = %#v, want insufficient-resources Notify", withdrawingMessages[1])
	}
	assertNoNotifyStatus(t, (*activeSent)[activeBefore:], params.InsufficientAspResources)
	assertNotifyStatus(t, (*inactiveSent)[inactiveBefore:], params.InsufficientAspResources)
	if got := applicationServer.State(); got != ASActive {
		t.Fatalf("state after one of two active ASPs withdrew = %v, want %v", got, ASActive)
	}
	_ = active
}

func TestApplicationServerRecoveryInsideTRestoresActiveBelowN(t *testing.T) {
	registry := applicationServerRegistryForActivationTest(t, ASActivationPolicy{RequiredActiveASPs: 2})
	applicationServer := registry.get(1)
	applicationServer.setTrafficMode(params.TrafficModeLoadshare)
	first, firstSent := asTestConn(t, registry, StateASPInactive, 1)
	second, secondSent := asTestConn(t, registry, StateASPInactive, 1)
	registry.aspStateChanged(first, StateASPActive)
	registry.aspStateChanged(second, StateASPActive)
	registry.aspStateChanged(second, StateASPInactive)
	registry.aspStateChanged(first, StateASPInactive)
	if got := applicationServer.State(); got != ASPending {
		t.Fatalf("state after the last active ASP withdrew = %v, want %v", got, ASPending)
	}
	firstBefore := len(*firstSent)
	secondBefore := len(*secondSent)

	registry.aspStateChanged(first, StateASPActive)
	if got := applicationServer.State(); got != ASActive {
		t.Fatalf("state after recovery below n = %v, want %v", got, ASActive)
	}
	if active := applicationServer.activeASPs(); len(active) != 1 || active[0] != first {
		t.Fatalf("recovery traffic targets = %v, want first ASP", active)
	}
	assertNotifyStatus(t, (*firstSent)[firstBefore:], params.AsStateActive)
	assertNoNotifyStatus(t, (*firstSent)[firstBefore:], params.InsufficientAspResources)
	assertNotifyStatus(t, (*secondSent)[secondBefore:], params.AsStateActive)
	assertNotifyStatus(t, (*secondSent)[secondBefore:], params.InsufficientAspResources)
}

func TestApplicationServerRecoveryBelowNDrainsQueuedData(t *testing.T) {
	listener, applicationServer, first, firstCapture := distributionFixtureConfigured(
		t, params.TrafficModeLoadshare,
		func(config *distributionFixtureConfig) {
			config.DefaultActivationPolicy = ASActivationPolicy{RequiredActiveASPs: 2}
			config.RecoveryTimer = time.Second
		},
	)
	second, secondCapture := addDistributionASP(t, listener, StateASPInactive, 1)
	listener.as.aspStateChanged(first, StateASPActive)
	listener.as.aspStateChanged(second, StateASPActive)
	listener.as.aspStateChanged(second, StateASPInactive)
	listener.as.aspStateChanged(first, StateASPInactive)
	if got := applicationServer.State(); got != ASPending {
		t.Fatalf("state before queued DATA = %v, want %v", got, ASPending)
	}
	firstCapture.reset()
	secondCapture.reset()

	result, err := listener.DistributeData(distributionData(1, 7, "queued-during-shortage"))
	if err != nil {
		t.Fatalf("DistributeData while pending: %v", err)
	}
	if !result.Queued || result.Delivered != 0 {
		t.Fatalf("pending distribution = %+v, want queued", result)
	}

	listener.as.aspStateChanged(first, StateASPActive)
	if !waitFor(func() bool { return firstCapture.dataCount() == 1 }, 2*time.Second) {
		t.Fatalf("recovery delivered %d DATA messages to the reactivated ASP, want 1",
			firstCapture.dataCount())
	}
	if got := secondCapture.dataCount(); got != 0 {
		t.Fatalf("recovery delivered %d DATA messages to an inactive ASP", got)
	}
	if got := string(dataPayload(t, firstCapture.data()[0])); got != "queued-during-shortage" {
		t.Fatalf("recovered DATA payload = %q, want queued-during-shortage", got)
	}
}

func TestConcurrentApplicationServerActivationThreshold(t *testing.T) {
	const (
		required = 4
		total    = 8
		rounds   = 20
	)
	registry := applicationServerRegistryForActivationTest(t, ASActivationPolicy{RequiredActiveASPs: required})
	applicationServer := registry.get(1)
	applicationServer.setTrafficMode(params.TrafficModeLoadshare)
	associations := make([]*Association, 0, total)
	for range total {
		association, _ := newTestConn(t, StateASPInactive, RoleSGP)
		association.cfg.RoutingContexts = params.NewRoutingContext(1)
		association.as = registry
		association.signalWriter = func(message messages.M3UA) (int, error) {
			return message.MarshalLen(), nil
		}
		registry.aspStateChanged(association, StateASPInactive)
		associations = append(associations, association)
	}

	applyConcurrently := func(state State, selected []*Association) {
		t.Helper()
		start := make(chan struct{})
		var wait sync.WaitGroup
		wait.Add(len(selected))
		for _, association := range selected {
			go func() {
				defer wait.Done()
				<-start
				registry.aspStateChanged(association, state)
			}()
		}
		close(start)
		wait.Wait()
	}

	for round := range rounds {
		applyConcurrently(StateASPActive, associations)
		if got := applicationServer.State(); got != ASActive {
			t.Fatalf("round %d state after activation = %v, want %v", round, got, ASActive)
		}
		if active := applicationServer.activeASPs(); len(active) != total {
			t.Fatalf("round %d active ASPs = %d, want %d", round, len(active), total)
		}

		applyConcurrently(StateASPInactive, associations[:total-required])
		if got := applicationServer.State(); got != ASActive {
			t.Fatalf("round %d state at threshold = %v, want %v", round, got, ASActive)
		}
		applyConcurrently(StateASPInactive, associations[total-required:])
		if got := applicationServer.State(); got != ASPending {
			t.Fatalf("round %d state after all withdrawals = %v, want %v", round, got, ASPending)
		}
	}
}

func TestEndpointCloseRacesApplicationServerThresholdChanges(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{
		Role: RoleSGP,
		ApplicationServers: &ApplicationServerConfig{
			DefaultActivationPolicy: ASActivationPolicy{RequiredActiveASPs: 2},
		},
	})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	registry := endpoint.as
	applicationServer := registry.get(1)
	applicationServer.setTrafficMode(params.TrafficModeLoadshare)
	associations := make([]*Association, 0, 8)
	for range 8 {
		association, _ := newTestConn(t, StateASPInactive, RoleSGP)
		association.cfg.RoutingContexts = params.NewRoutingContext(1)
		association.as = registry
		association.signalWriter = func(message messages.M3UA) (int, error) {
			return message.MarshalLen(), nil
		}
		registry.aspStateChanged(association, StateASPInactive)
		associations = append(associations, association)
	}

	start := make(chan struct{})
	var wait sync.WaitGroup
	wait.Add(len(associations))
	for _, association := range associations {
		go func() {
			defer wait.Done()
			<-start
			for iteration := range 100 {
				state := StateASPActive
				if iteration%2 != 0 {
					state = StateASPInactive
				}
				registry.aspStateChanged(association, state)
			}
		}()
	}
	close(start)
	if err := endpoint.Close(); err != nil {
		t.Fatalf("Endpoint.Close: %v", err)
	}
	wait.Wait()
	if got := applicationServer.State(); got != ASDown {
		t.Fatalf("AS state after Endpoint.Close = %v, want %v", got, ASDown)
	}
	if active := applicationServer.activeASPs(); len(active) != 0 {
		t.Fatalf("active ASPs after Endpoint.Close = %d, want 0", len(active))
	}
}

func TestOverrideRequiresOneActiveASP(t *testing.T) {
	registry := applicationServerRegistryForActivationTest(t, ASActivationPolicy{RequiredActiveASPs: 2})
	policy := trafficModePolicy{defaultMode: params.TrafficModeOverride, defaultModeSet: true}
	key := ASKey{RoutingContext: 1, RoutingContextSet: true}
	applicationServer := registry.get(key)

	applicationServer.setTrafficMode(params.TrafficModeOverride)
	if got := applicationServer.TrafficMode(); got != 0 {
		t.Fatalf("direct Override assignment committed Traffic Mode %d", got)
	}

	_, err := registry.agreeTrafficModeForKeys(
		[]ASKey{key}, policy, params.NewTrafficModeType(params.TrafficModeOverride),
	)
	if !errors.Is(err, ErrUnsupportedTrafficMode) {
		t.Fatalf("Override agreement error = %v, want %v", err, ErrUnsupportedTrafficMode)
	}
	if got := applicationServer.TrafficMode(); got != 0 {
		t.Fatalf("rejected Override committed Traffic Mode %d", got)
	}
}

func TestRoutingKeyRegistrationRejectsOverrideWhenNExceedsOne(t *testing.T) {
	tests := []struct {
		name                string
		provisioned         bool
		requestIncludesMode bool
		requestMode         uint32
		associationMode     uint32
		wantStatus          RegistrationStatus
	}{
		{
			name:                "provisioned explicit mode",
			provisioned:         true,
			requestIncludesMode: true,
			requestMode:         params.TrafficModeOverride,
			wantStatus:          RegistrationUnsupportedTrafficHandlingMode,
		},
		{
			name:        "provisioned inherited Routing Key mode",
			provisioned: true,
			wantStatus:  RegistrationUnsupportedTrafficHandlingMode,
		},
		{
			name:                "dynamic explicit mode",
			requestIncludesMode: true,
			requestMode:         params.TrafficModeOverride,
			wantStatus:          RegistrationUnsupportedTrafficHandlingMode,
		},
		{
			name:                "dynamic explicit Loadshare overrides association default",
			requestIncludesMode: true,
			requestMode:         params.TrafficModeLoadshare,
			associationMode:     params.TrafficModeOverride,
			wantStatus:          RegistrationSuccessfullyRegistered,
		},
		{
			name:            "dynamic inherited association mode",
			associationMode: params.TrafficModeOverride,
			wantStatus:      RegistrationUnsupportedTrafficHandlingMode,
		},
		{
			name:            "dynamic inherited Loadshare",
			associationMode: params.TrafficModeLoadshare,
			wantStatus:      RegistrationSuccessfullyRegistered,
		},
		{
			name:       "dynamic unknown mode",
			wantStatus: RegistrationSuccessfullyRegistered,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			routingKey := testRoutingKey(10, 100, params.ServiceIndSCCP)
			routingKey.TrafficMode = params.TrafficModeOverride
			routingKey.TrafficModeSet = true
			routingKeyConfig := &RoutingKeyManagementConfig{
				AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
					return RegistrationSuccessfullyRegistered
				},
				AllowDynamicRoutingKeys: !test.provisioned,
			}
			if test.provisioned {
				routingKeyConfig.ProvisionedRoutingKeys = []ProvisionedRoutingKey{{
					RoutingContext: 7,
					RoutingKey:     routingKey,
				}}
			}
			registry, err := newRoutingKeyRegistry(routingKeyConfig)
			if err != nil {
				t.Fatalf("newRoutingKeyRegistry: %v", err)
			}
			routingContext := uint32(1)
			if test.provisioned {
				routingContext = 7
			}
			applicationServers := newApplicationServersForIPSP(&ApplicationServerConfig{
				ActivationPolicies: map[ASKey]ASActivationPolicy{
					{
						NetworkAppearance:    10,
						NetworkAppearanceSet: true,
						RoutingContext:       routingContext,
						RoutingContextSet:    true,
					}: {RequiredActiveASPs: 2},
				},
			})
			associationConfig := NewAssociationConfig(0, 0, 0, 0, 0, 0)
			if test.associationMode != 0 {
				associationConfig.TrafficModeType = params.NewTrafficModeType(test.associationMode)
			}
			association := newAssociation(RoleSGP, associationConfig)
			association.as = applicationServers
			t.Cleanup(func() { _ = association.Close() })

			requestedRoutingKey := routingKey
			if test.requestIncludesMode {
				requestedRoutingKey.TrafficMode = test.requestMode
				requestedRoutingKey.TrafficModeSet = true
			} else {
				requestedRoutingKey.TrafficMode = 0
				requestedRoutingKey.TrafficModeSet = false
			}
			result := registry.register(association, []RoutingKeyRegistrationRequest{{
				LocalRoutingKeyIdentifier: 1,
				RoutingKey:                requestedRoutingKey,
			}})[0]
			if result.Status != test.wantStatus {
				t.Fatalf("registration status = %v, want %v", result.Status, test.wantStatus)
			}
			if test.wantStatus == RegistrationSuccessfullyRegistered {
				if result.RoutingContext == 0 {
					t.Fatal("successful registration omitted Routing Context")
				}
				return
			}
			if result.RoutingContext != 0 {
				t.Fatalf("rejected registration returned Routing Context %d", result.RoutingContext)
			}
			if keys := applicationServers.keys(); len(keys) != 0 {
				t.Fatalf("rejected registration created Application Servers %v", keys)
			}
		})
	}
}

func TestRoutingKeyRegistrationValidatesDuplicateRequestModesIndependently(t *testing.T) {
	for _, overrideFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("Override first %t", overrideFirst), func(t *testing.T) {
			registry, err := newRoutingKeyRegistry(&RoutingKeyManagementConfig{
				AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
					return RegistrationSuccessfullyRegistered
				},
				AllowDynamicRoutingKeys: true,
			})
			if err != nil {
				t.Fatalf("newRoutingKeyRegistry: %v", err)
			}
			applicationServers := newApplicationServersForIPSP(&ApplicationServerConfig{
				DefaultActivationPolicy: ASActivationPolicy{RequiredActiveASPs: 2},
			})
			association := newAssociation(RoleSGP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
			association.as = applicationServers

			omittedMode := testRoutingKey(10, 100, params.ServiceIndSCCP)
			override := snapshotRoutingKey(omittedMode)
			override.TrafficMode = params.TrafficModeOverride
			override.TrafficModeSet = true
			requests := []RoutingKeyRegistrationRequest{
				{LocalRoutingKeyIdentifier: 1, RoutingKey: omittedMode},
				{LocalRoutingKeyIdentifier: 2, RoutingKey: override},
			}
			if overrideFirst {
				requests[0], requests[1] = requests[1], requests[0]
			}
			results := registry.register(association, requests)
			byIdentifier := make(map[uint32]RoutingKeyRegistrationResult, len(results))
			for _, result := range results {
				byIdentifier[result.LocalRoutingKeyIdentifier] = result
			}

			omittedResult := byIdentifier[1]
			if omittedResult.Status != RegistrationSuccessfullyRegistered || omittedResult.RoutingContext == 0 {
				t.Fatalf("mode-omitting registration = %+v, want successful allocated Routing Context", omittedResult)
			}
			if result := byIdentifier[2]; result.Status != RegistrationUnsupportedTrafficHandlingMode || result.RoutingContext != 0 {
				t.Fatalf("duplicate Override registration = %+v, want Unsupported Traffic Handling Mode", result)
			}
			if got := registry.dynamicCount(); got != 1 {
				t.Fatalf("dynamic Routing Key count = %d, want 1", got)
			}
			registry.mu.Lock()
			entry := registry.entries[omittedResult.RoutingContext]
			registry.mu.Unlock()
			if entry == nil {
				t.Fatal("successful registration was not published")
			}
			if entry.routingKey.TrafficModeSet {
				t.Fatalf("rejected duplicate mutated Traffic Mode to %d", entry.routingKey.TrafficMode)
			}
		})
	}
}

func TestRoutingKeyRegistrationPublishesShortageAfterAdoptingTrafficMode(t *testing.T) {
	applicationServers := applicationServerRegistryForActivationTest(t, ASActivationPolicy{RequiredActiveASPs: 2})
	applicationServer := applicationServers.get(1)
	first, firstSent := asTestConn(t, applicationServers, StateASPInactive, 1)
	second, secondSent := asTestConn(t, applicationServers, StateASPInactive, 1)
	applicationServers.aspStateChanged(first, StateASPActive)
	applicationServers.aspStateChanged(second, StateASPActive)
	applicationServers.aspStateChanged(second, StateASPInactive)
	if got := applicationServer.State(); got != ASActive {
		t.Fatalf("state below n before Traffic Mode adoption = %v, want %v", got, ASActive)
	}
	if got := applicationServer.TrafficMode(); got != 0 {
		t.Fatalf("Traffic Mode before Registration = %d, want unknown", got)
	}
	firstBefore := len(*firstSent)
	secondBefore := len(*secondSent)

	routingKey := testRoutingKey(0, 100, params.ServiceIndSCCP)
	routingKey.NetworkAppearance = 0
	routingKey.NetworkAppearanceSet = false
	routingKey.TrafficMode = params.TrafficModeLoadshare
	routingKey.TrafficModeSet = true
	registry, err := newRoutingKeyRegistry(&RoutingKeyManagementConfig{
		AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus {
			return RegistrationSuccessfullyRegistered
		},
		ProvisionedRoutingKeys: []ProvisionedRoutingKey{{
			RoutingContext: 1,
			RoutingKey:     routingKey,
		}},
	})
	if err != nil {
		t.Fatalf("newRoutingKeyRegistry: %v", err)
	}
	result := registry.register(first, []RoutingKeyRegistrationRequest{{
		LocalRoutingKeyIdentifier: 1,
		RoutingKey:                routingKey,
	}})[0]
	if result.Status != RegistrationRoutingKeyAlreadyRegistered || result.RoutingContext != 1 {
		t.Fatalf("registration result = %+v, want Routing Key Already Registered for Routing Context 1", result)
	}
	if got := applicationServer.TrafficMode(); got != params.TrafficModeLoadshare {
		t.Fatalf("Traffic Mode after Registration = %d, want Loadshare", got)
	}
	assertNoNotifyStatus(t, (*firstSent)[firstBefore:], params.InsufficientAspResources)
	assertNoNotifyStatus(t, (*secondSent)[secondBefore:], params.InsufficientAspResources)

	// The responder invokes this only after writing REG RSP. Learning the mode
	// must now publish the shortage even though this static AS membership already
	// existed and its ASP state must remain unchanged.
	applicationServers.registerDynamicASP(first, ASKey{RoutingContext: 1, RoutingContextSet: true})
	assertNoNotifyStatus(t, (*firstSent)[firstBefore:], params.InsufficientAspResources)
	assertNotifyStatus(t, (*secondSent)[secondBefore:], params.InsufficientAspResources)
	if got := applicationServer.State(); got != ASActive {
		t.Fatalf("state after post-response shortage publication = %v, want %v", got, ASActive)
	}
}

func TestASPActivePublishesShortageAfterAdoptingTrafficMode(t *testing.T) {
	applicationServers := applicationServerRegistryForActivationTest(t, ASActivationPolicy{RequiredActiveASPs: 2})
	applicationServer := applicationServers.get(1)
	active, activeSent := asTestConn(t, applicationServers, StateASPActive, 1)
	withdrawing, withdrawingSent := asTestConn(t, applicationServers, StateASPActive, 1)
	_, inactiveSent := asTestConn(t, applicationServers, StateASPInactive, 1)
	applicationServers.aspStateChanged(withdrawing, StateASPInactive)
	if got := applicationServer.State(); got != ASActive {
		t.Fatalf("state below n before Traffic Mode adoption = %v, want %v", got, ASActive)
	}
	if got := applicationServer.TrafficMode(); got != 0 {
		t.Fatalf("Traffic Mode before ASP Active = %d, want unknown", got)
	}
	activeBefore := len(*activeSent)
	withdrawingBefore := len(*withdrawingSent)
	inactiveBefore := len(*inactiveSent)

	if err := active.handleAspActive(messages.NewAspActive(
		params.NewTrafficModeType(params.TrafficModeLoadshare),
		params.NewRoutingContext(1),
		nil,
	)); err != nil {
		t.Fatalf("handleAspActive: %v", err)
	}
	if got := applicationServer.TrafficMode(); got != params.TrafficModeLoadshare {
		t.Fatalf("Traffic Mode after ASP Active = %d, want Loadshare", got)
	}
	activeMessages := (*activeSent)[activeBefore:]
	if len(activeMessages) != 1 {
		t.Fatalf("messages before ASP state publication = %v, want ASP Active Ack only", typeNames(activeMessages))
	}
	if _, ok := activeMessages[0].(*messages.AspActiveAck); !ok {
		t.Fatalf("first message = %T, want *messages.AspActiveAck", activeMessages[0])
	}
	assertNoNotifyStatus(t, (*withdrawingSent)[withdrawingBefore:], params.InsufficientAspResources)
	assertNoNotifyStatus(t, (*inactiveSent)[inactiveBefore:], params.InsufficientAspResources)

	if err := active.handleStateUpdate(StateASPActive); err != nil {
		t.Fatalf("handleStateUpdate: %v", err)
	}
	assertNoNotifyStatus(t, (*activeSent)[activeBefore:], params.InsufficientAspResources)
	assertNotifyStatus(t, (*withdrawingSent)[withdrawingBefore:], params.InsufficientAspResources)
	assertNotifyStatus(t, (*inactiveSent)[inactiveBefore:], params.InsufficientAspResources)
	if got := applicationServer.State(); got != ASActive {
		t.Fatalf("state after shortage publication = %v, want %v", got, ASActive)
	}
}

func applicationServerRegistryForActivationTest(t *testing.T, policy ASActivationPolicy) *applicationServers {
	t.Helper()
	endpoint, err := NewEndpoint(EndpointConfig{
		Role: RoleSGP,
		ApplicationServers: &ApplicationServerConfig{
			RecoveryTimer:           time.Hour,
			DefaultActivationPolicy: policy,
		},
	})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	return endpoint.as
}

func assertNotifyStatus(t *testing.T, sent []messages.M3UA, want uint32) {
	t.Helper()
	for _, message := range sent {
		notify, ok := message.(*messages.Notify)
		if ok && notify.Status != nil && notify.Status.Status() == want {
			return
		}
	}
	t.Fatalf("Notify status %#x not found in %v", want, typeNames(sent))
}

func assertNoNotifyStatus(t *testing.T, sent []messages.M3UA, unwanted uint32) {
	t.Helper()
	for _, message := range sent {
		notify, ok := message.(*messages.Notify)
		if ok && notify.Status != nil && notify.Status.Status() == unwanted {
			t.Fatalf("unexpected Notify status %#x in %v", unwanted, typeNames(sent))
		}
	}
}

func hasNotifyStatus(sent []messages.M3UA, status uint32) bool {
	for _, message := range sent {
		notify, ok := message.(*messages.Notify)
		if ok && notify.Status != nil && notify.Status.Status() == status {
			return true
		}
	}
	return false
}
