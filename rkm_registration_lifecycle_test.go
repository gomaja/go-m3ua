package m3ua

import (
	"fmt"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestDynamicRegistrationAfterASPUpWithoutConfiguredApplicationServer(t *testing.T) {
	for _, appearanceSet := range []bool{false, true} {
		for _, cycle := range []bool{false, true} {
			t.Run(fmt.Sprintf("appearance=%t/down-up=%t", appearanceSet, cycle), func(t *testing.T) {
				endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP, RoutingKeyManagement: &RoutingKeyManagementConfig{AllowDynamicRoutingKeys: true, AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus { return RegistrationSuccessfullyRegistered }}})
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = endpoint.Close() }()
				c, sent := newTestConnWithContexts(t, StateASPDown, RoleSGP)
				// The general handler fixture preserves a mode-only AS when clearing
				// its RCs. This case requires genuinely empty configuration instead.
				c.cfg = NewAssociationConfig()
				c.recvStream.Store(0)
				c.endpoint = endpoint
				c.as = endpoint.as
				if err := c.handleAspUp(messages.NewAspUp(nil, nil)); err != nil {
					t.Fatal(err)
				}
				if cycle {
					if err := c.handleAspDown(messages.NewAspDown(nil)); err != nil {
						t.Fatal(err)
					}
					if err := c.handleAspUp(messages.NewAspUp(nil, nil)); err != nil {
						t.Fatal(err)
					}
				}
				if c.State() != StateASPInactive {
					t.Fatalf("state=%v", c.State())
				}
				if len(c.cfg.ApplicationServers) != 0 {
					t.Fatalf("configuration changed: %+v", c.cfg.ApplicationServers)
				}
				key := testRoutingKey(10, 100, 3)
				key.NetworkAppearanceSet = appearanceSet
				if !appearanceSet {
					key.NetworkAppearance = 0
				}
				param, err := routingKeyParameter(RoutingKeyRegistrationRequest{LocalRoutingKeyIdentifier: 1, RoutingKey: key})
				if err != nil {
					t.Fatal(err)
				}
				if err := c.handleRegistrationRequest(messages.NewRegistrationRequest(param)); err != nil {
					t.Fatal(err)
				}
				for _, m := range *sent {
					if rsp, ok := m.(*messages.RegistrationResponse); ok {
						result, err := rsp.RegistrationResults[0].RegistrationResult()
						if err != nil {
							t.Fatal(err)
						}
						if got := result.RegistrationStatus.RegistrationStatus(); got != params.SuccessfullyRegistered {
							t.Fatalf("status=%d", got)
						}
						if result.LocalRoutingKeyIdentifier.LocalRoutingKeyIdentifier() != 1 || result.RoutingContext.RoutingContext() == 0 {
							t.Fatalf("result=%v", result)
						}
						return
					}
				}
				t.Fatal("no REG RSP")
			})
		}
	}
}

func TestRegistrationAfterASPUpPreservesConfiguredAllAppearanceConflicts(t *testing.T) {
	for _, scoped := range []bool{false, true} {
		t.Run(fmt.Sprintf("routing-context=%t", scoped), func(t *testing.T) {
			endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP, RoutingKeyManagement: &RoutingKeyManagementConfig{AllowDynamicRoutingKeys: true, AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus { return RegistrationSuccessfullyRegistered }}})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = endpoint.Close() }()
			c, sent := newTestConnWithContexts(t, StateASPDown, RoleSGP)
			c.cfg = NewAssociationConfig()
			c.cfg.ApplicationServers = []ASConfig{{TrafficMode: params.TrafficModeLoadshare}}
			if scoped {
				c.cfg.ApplicationServers[0].ASKey = ASKey{RoutingContext: 7, RoutingContextSet: true}
			}
			c.recvStream.Store(0)
			c.endpoint = endpoint
			c.as = endpoint.as
			if err := c.handleAspUp(messages.NewAspUp(nil, nil)); err != nil {
				t.Fatal(err)
			}
			param, err := routingKeyParameter(RoutingKeyRegistrationRequest{LocalRoutingKeyIdentifier: 1, RoutingKey: testRoutingKey(10, 100, 3)})
			if err != nil {
				t.Fatal(err)
			}
			if err := c.handleRegistrationRequest(messages.NewRegistrationRequest(param)); err != nil {
				t.Fatal(err)
			}
			for _, m := range *sent {
				if rsp, ok := m.(*messages.RegistrationResponse); ok {
					result, err := rsp.RegistrationResults[0].RegistrationResult()
					if err != nil {
						t.Fatal(err)
					}
					if got := result.RegistrationStatus.RegistrationStatus(); got != params.CannotSupportUniqueRouting {
						t.Fatalf("status=%d", got)
					}
					if endpoint.routingKeys.dynamicCount() != 0 {
						t.Fatal("conflicting key installed")
					}
					return
				}
			}
			t.Fatal("no REG RSP")
		})
	}
}
