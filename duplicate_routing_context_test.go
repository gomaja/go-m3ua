package m3ua

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func duplicateRoutingContextConfig(routingContext uint32) *AssociationConfig {
	cfg := newSGPAssociationConfigForTest(nil, 1, params.TrafficModeLoadshare, 7, []uint32{routingContext})
	second := cfg.ApplicationServers[0]
	second.ASKey.NetworkAppearance = 9
	cfg.ApplicationServers = append(cfg.ApplicationServers, second)
	return cfg
}

func requireDuplicateRoutingContextError(t *testing.T, err, want error, routingContext uint32) {
	t.Helper()
	if !errors.Is(err, want) || !strings.Contains(err.Error(), fmt.Sprintf("Routing Context %d", routingContext)) {
		t.Fatalf("error = %v, want %v naming Routing Context %d", err, want, routingContext)
	}
}

// RFC 4666 §§1.2, 1.4.2.1 and 3.3.1 give each Routing Context one
// Application Server in an inventory, independent of Network Appearance.
func TestAssociationConfigRejectsDuplicateRoutingContext(t *testing.T) {
	for _, path := range []string{"SGP", "ASP", "IPSP Single Exchange", "TrafficToLocal", "TrafficToPeer"} {
		t.Run(path, func(t *testing.T) {
			for _, routingContext := range []uint32{0, 17, ^uint32(0)} {
				t.Run(fmt.Sprint(routingContext), func(t *testing.T) {
					cfg := duplicateRoutingContextConfig(routingContext)
					role := RoleSGP
					switch path {
					case "ASP":
						role = RoleASP
					case "IPSP Single Exchange":
						role = RoleIPSP
						cfg.IPSP = &IPSPConfig{ExchangeModel: IPSPExchangeSingle}
					case "TrafficToLocal", "TrafficToPeer":
						role = RoleIPSP
						traffic := &IPSPTrafficConfig{ApplicationServers: cfg.ApplicationServers}
						cfg.ApplicationServers = nil
						cfg.IPSP = &IPSPConfig{ExchangeModel: IPSPExchangeDouble, ASPSMExchange: IPSPASPSMExchangeSingle}
						if path == "TrafficToLocal" {
							cfg.IPSP.TrafficToLocal = traffic
						} else {
							cfg.IPSP.TrafficToPeer = traffic
						}
					}
					if role == RoleIPSP {
						cfg.ASPProcedures = &ASPProcedurePolicy{
							ASPUp: ASPProcedureExplicit, ASPDown: ASPProcedureExplicit,
							ASPActive: ASPProcedureExplicit, ASPInactive: ASPProcedureExplicit,
						}
					}
					err := validateAssociationConfigForRole(role, cfg)
					requireDuplicateRoutingContextError(t, err, ErrInvalidApplicationServerConfig, routingContext)
					if strings.HasPrefix(path, "TrafficTo") && !strings.Contains(err.Error(), path+".ApplicationServers") {
						t.Fatalf("error does not name the direction: %v", err)
					}
				})
			}
		})
	}
}

func TestAssociationConfigDuplicateRoutingContextPresenceAndTrafficMode(t *testing.T) {
	for _, shape := range []string{"identical", "different Traffic Mode", "absent Network Appearance", "explicit zero Network Appearance"} {
		t.Run(shape, func(t *testing.T) {
			cfg := duplicateRoutingContextConfig(17)
			second := &cfg.ApplicationServers[1]
			switch shape {
			case "identical":
				*second = cfg.ApplicationServers[0]
			case "different Traffic Mode":
				*second = cfg.ApplicationServers[0]
				second.TrafficMode = params.TrafficModeOverride
			case "absent Network Appearance":
				second.ASKey.NetworkAppearance = 0
				second.ASKey.NetworkAppearanceSet = false
			case "explicit zero Network Appearance":
				second.ASKey.NetworkAppearance = 0
			}
			requireDuplicateRoutingContextError(t, validateAssociationConfigForRole(RoleSGP, cfg), ErrInvalidApplicationServerConfig, 17)
		})
	}
}

func TestEndpointAssociationPathsRejectDuplicateRoutingContext(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	cfg := duplicateRoutingContextConfig(17)
	// The invalid network keeps these tests socket-free even with validation
	// removed. The inventory must be refused before transport setup.
	t.Run("Dial", func(t *testing.T) {
		_, err := endpoint.Dial(context.Background(), "invalid", nil, nil, cfg)
		requireDuplicateRoutingContextError(t, err, ErrInvalidApplicationServerConfig, 17)
	})
	t.Run("Listen default", func(t *testing.T) {
		_, err := endpoint.Listen("invalid", nil, NewListenerConfig(cfg))
		requireDuplicateRoutingContextError(t, err, ErrInvalidApplicationServerConfig, 17)
	})
	t.Run("Listen selector", func(t *testing.T) {
		listener := newSGPListener(&ListenerConfig{SelectAssociationConfig: func(AcceptInfo) (*AssociationConfig, error) { return cfg, nil }})
		listener.endpoint = endpoint
		_, err := listener.resolveAcceptedAssociationConfig(RoleSGP, AcceptInfo{})
		requireDuplicateRoutingContextError(t, err, ErrInvalidApplicationServerConfig, 17)
	})
}

func TestASPConfigRejectsDuplicateRoutingContext(t *testing.T) {
	for _, routed := range []bool{false, true} {
		t.Run(fmt.Sprintf("routed=%t", routed), func(t *testing.T) {
			cfg := validASPConfig()
			if !routed {
				cfg.Routing = nil
			}
			sgp := &cfg.SignallingGateways[0].SGPs[0]
			sgp.ApplicationServers = []RemoteASConfig{
				{ID: "as-core", ASKey: staticASKey(7, 17)},
				{ID: "as-other", ASKey: staticASKey(9, 17)},
			}
			endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: cfg})
			if endpoint != nil {
				_ = endpoint.Close()
			}
			requireDuplicateRoutingContextError(t, err, ErrInvalidASPConfig, 17)
		})
	}
}

func TestRoutingContextUniquenessKeepsIndependentInventories(t *testing.T) {
	cfg := duplicateRoutingContextConfig(17)
	cfg.ApplicationServers[1].ASKey.RoutingContext = 18
	if err := validateAssociationConfigForRole(RoleSGP, cfg); err != nil {
		t.Fatalf("distinct Routing Contexts: %v", err)
	}
	cfg.IPSP = &IPSPConfig{
		ExchangeModel: IPSPExchangeDouble, ASPSMExchange: IPSPASPSMExchangeSingle,
		TrafficToLocal: &IPSPTrafficConfig{ApplicationServers: cfg.ApplicationServers[:1]},
		TrafficToPeer:  &IPSPTrafficConfig{ApplicationServers: cfg.ApplicationServers[1:]},
	}
	cfg.IPSP.TrafficToPeer.ApplicationServers[0].ASKey.RoutingContext = 17
	cfg.ApplicationServers = nil
	cfg.ASPProcedures = &ASPProcedurePolicy{
		ASPUp: ASPProcedureExplicit, ASPDown: ASPProcedureExplicit,
		ASPActive: ASPProcedureExplicit, ASPInactive: ASPProcedureExplicit,
	}
	if err := validateAssociationConfigForRole(RoleIPSP, cfg); err != nil {
		t.Fatalf("independent Double Exchange directions: %v", err)
	}
	asp := inventoryOnlyASPConfig()
	asp.SignallingGateways[0].SGPs = append(asp.SignallingGateways[0].SGPs, SignallingGatewayProcessConfig{
		ID: "sgp-a2", ApplicationServers: []RemoteASConfig{{ID: "as-other", ASKey: staticASKey(9, 1)}},
	})
	asp.SignallingGateways[1].SGPs[0].ApplicationServers[0].ASKey = staticASKey(11, 1)
	if _, err := snapshotASPConfig(asp); err != nil {
		t.Fatalf("independent SGP inventories: %v", err)
	}
}

func TestRKMRejectsRoutingContextSharedByCanonicalApplicationServers(t *testing.T) {
	for _, existing := range []string{"batch", "static", "dynamic"} {
		t.Run(existing, func(t *testing.T) {
			asp := &ASPConfig{SignallingGateways: []SignallingGatewayConfig{{
				ID: "sg", SGPs: []SignallingGatewayProcessConfig{{
					ID: "sgp", ApplicationServers: []RemoteASConfig{
						{ID: "first", RoutingKey: ptrRoutingKeyForDuplicateContextTest(100)},
						{ID: "second", RoutingKey: ptrRoutingKeyForDuplicateContextTest(200)},
					},
				}},
			}}}
			cfg := NewAssociationConfig()
			cfg.PeerSGP = &SGPIdentity{SignallingGateway: "sg", SignallingGatewayProcess: "sgp"}
			if existing == "static" {
				asp.SignallingGateways[0].SGPs[0].ApplicationServers[0] = RemoteASConfig{ID: "first", ASKey: staticASKey(7, 17)}
				cfg.ApplicationServers = []ASConfig{{ASKey: *staticASKey(7, 17)}}
			}
			endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: asp})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = endpoint.Close() })
			association := newAssociation(RoleASP, cfg)
			association.endpoint = endpoint
			association.state = StateASPInactive
			setRegistrationResultWriter(t, association, params.SuccessfullyRegistered, 17)
			first := RoutingKeyRegistration{RemoteAS: "first", RoutingKey: *ptrRoutingKeyForDuplicateContextTest(100)}
			second := RoutingKeyRegistration{RemoteAS: "second", RoutingKey: *ptrRoutingKeyForDuplicateContextTest(200)}
			if existing == "dynamic" {
				if _, err := association.RegisterRoutingKeys(context.Background(), first); err != nil {
					t.Fatal(err)
				}
				// Re-registering the same canonical AS remains valid.
				if _, err := association.RegisterRoutingKeys(context.Background(), first); err != nil {
					t.Fatalf("same canonical Application Server: %v", err)
				}
			}
			registrations := []RoutingKeyRegistration{second}
			if existing == "batch" {
				registrations = []RoutingKeyRegistration{first, second}
			}
			_, err = association.RegisterRoutingKeys(context.Background(), registrations...)
			requireDuplicateRoutingContextError(t, err, ErrInvalidRoutingContext, 17)
			association.rkmCorrelationMu.Lock()
			retained := len(association.rkmRegistrationRemoteAS)
			association.rkmCorrelationMu.Unlock()
			if retained != 0 {
				t.Fatalf("completed registration retained %d canonical identities", retained)
			}
			if existing == "dynamic" {
				if owner, ok := association.canonicalRemoteASBinding(17); !ok || owner != "first" {
					t.Fatalf("existing canonical binding changed to %q, %t", owner, ok)
				}
			} else if contexts := association.dynamicRoutingContexts(false); len(contexts) != 0 {
				t.Fatalf("conflicting result installed Routing Contexts %v", contexts)
			}
		})
	}
}

func ptrRoutingKeyForDuplicateContextTest(dpc uint32) *RoutingKey {
	key := testRoutingKey(7, dpc, params.ServiceIndSCCP)
	return &key
}

func TestRKMCanceledRegistrationKeepsCanonicalApplicationServer(t *testing.T) {
	for _, delivery := range []string{"late", "partial"} {
		t.Run(delivery, func(t *testing.T) {
			asp := &ASPConfig{SignallingGateways: []SignallingGatewayConfig{{
				ID: "sg", SGPs: []SignallingGatewayProcessConfig{{
					ID: "sgp", ApplicationServers: []RemoteASConfig{
						{ID: "first", RoutingKey: ptrRoutingKeyForDuplicateContextTest(100)},
						{ID: "second", RoutingKey: ptrRoutingKeyForDuplicateContextTest(200)},
					},
				}},
			}}}
			endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: asp})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = endpoint.Close() })
			cfg := NewAssociationConfig()
			cfg.PeerSGP = &SGPIdentity{SignallingGateway: "sg", SignallingGatewayProcess: "sgp"}
			association := newAssociation(RoleASP, cfg)
			association.endpoint = endpoint
			association.state = StateASPInactive
			written := make(chan []uint32, 1)
			association.signalWriter = func(message messages.M3UA) (int, error) {
				request := message.(*messages.RegistrationRequest)
				identifiers := make([]uint32, len(request.RoutingKeys))
				for index, parameter := range request.RoutingKeys {
					key, err := parameter.RoutingKey()
					if err != nil {
						return 0, err
					}
					identifiers[index] = key.LocalRoutingKeyIdentifier.LocalRoutingKeyIdentifier()
				}
				written <- identifiers
				return message.MarshalLen(), nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			finished := make(chan error, 1)
			go func() {
				_, err := association.RegisterRoutingKeys(ctx,
					RoutingKeyRegistration{RemoteAS: "first", RoutingKey: *ptrRoutingKeyForDuplicateContextTest(100)},
					RoutingKeyRegistration{RemoteAS: "second", RoutingKey: *ptrRoutingKeyForDuplicateContextTest(200)})
				finished <- err
			}()
			var identifiers []uint32
			select {
			case identifiers = <-written:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			result := func(identifier uint32) *params.Param {
				return params.NewRegistrationResult(params.NewRegistrationResultPayload(
					params.NewLocalRoutingKeyIdentifier(identifier), params.NewRegistrationStatus(params.SuccessfullyRegistered), params.NewRoutingContext(17)))
			}
			if delivery == "partial" {
				if err := association.handleRegistrationResponse(messages.NewRegistrationResponse(result(identifiers[0]))); err != nil {
					t.Fatal(err)
				}
			}
			cancel()
			if err := <-finished; !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled registration: %v", err)
			}
			if delivery == "partial" {
				if owner, ok := association.canonicalRemoteASBinding(17); !ok || owner != "first" {
					t.Fatalf("partial success lost its canonical AS: %q, %t", owner, ok)
				}
			}
			response := messages.NewRegistrationResponse(result(identifiers[1]))
			if delivery == "late" {
				response = messages.NewRegistrationResponse(result(identifiers[0]), result(identifiers[1]))
			}
			err = association.handleRegistrationResponse(response)
			requireDuplicateRoutingContextError(t, err, ErrInvalidRoutingContext, 17)
			association.rkmCorrelationMu.Lock()
			retained := len(association.rkmRegistrationRemoteAS)
			association.rkmCorrelationMu.Unlock()
			if retained != 0 {
				t.Fatalf("resolved late results retained %d canonical identities", retained)
			}
			if delivery == "late" && len(association.dynamicRoutingContexts(false)) != 0 {
				t.Fatal("conflicting late results installed an AS")
			}
		})
	}
}
