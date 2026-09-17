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

func staticASKey(networkAppearance, routingContext uint32) *ASKey {
	return &ASKey{
		NetworkAppearance:    networkAppearance,
		NetworkAppearanceSet: true,
		RoutingContext:       routingContext,
		RoutingContextSet:    true,
	}
}

// inventoryOnlyASPConfig provisions two Signalling Gateways and no outbound
// route inventory at all.
func inventoryOnlyASPConfig() *ASPConfig {
	return &ASPConfig{
		SignallingGateways: []SignallingGatewayConfig{
			{
				ID: "sg-a",
				SGPs: []SignallingGatewayProcessConfig{
					{
						ID: "sgp-a1",
						ApplicationServers: []RemoteASConfig{
							{ID: "as-core", ASKey: staticASKey(7, 1)},
						},
					},
				},
			},
			{
				ID: "sg-b",
				SGPs: []SignallingGatewayProcessConfig{
					{
						ID: "sgp-b1",
						ApplicationServers: []RemoteASConfig{
							{ID: "as-core", ASKey: staticASKey(9, 42)},
						},
					},
				},
			},
		},
	}
}

func aspAssociationForSGP(
	t *testing.T,
	identity SGPIdentity,
	networkAppearance uint32,
	routingContexts ...uint32,
) *Association {
	t.Helper()
	association, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, routingContexts...)
	if networkAppearance != 0 {
		association.cfg.NetworkAppearance = params.NewNetworkAppearance(networkAppearance)
	}
	peer := identity
	association.cfg.PeerSGP = &peer
	if len(routingContexts) > 0 {
		association.noteRoutingContextsAcked(params.NewRoutingContext(routingContexts...))
	}
	t.Cleanup(func() { _ = association.Close() })
	return association
}

// An ASP that provisions its peers but owns its outbound routing needs no MTP
// Route inventory. RFC 4666 Section 1.4.2 separates the Application Server a
// peer serves from the routes an ASP chooses among; only the former is
// protocol state the library must hold.
func TestASPInventoryWithoutRoutingEstablishesAndAuthorizes(t *testing.T) {
	config := inventoryOnlyASPConfig()
	if config.Routing != nil {
		t.Fatal("inventory-only fixture must select application-managed routing")
	}
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: config})
	if err != nil {
		t.Fatalf("NewEndpoint with peer inventory and no routing: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	if endpoint.aspRoutes.routingConfigured() {
		t.Fatal("nil Routing must select application-managed routing")
	}

	provisioned := aspAssociationForSGP(t, SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}, 7, 1)
	if err := endpoint.validateAssociationConfig(provisioned.cfg); err != nil {
		t.Fatalf("provisioned Association rejected: %v", err)
	}
	if !endpoint.trackAssociation(provisioned) {
		t.Fatal("provisioned Association was not attached")
	}

	tests := []struct {
		name        string
		association *Association
		want        error
	}{
		{
			name:        "no SGP identity",
			association: aspAssociationForSGP(t, SGPIdentity{}, 7, 1),
			want:        ErrUnknownSGP,
		},
		{
			name: "unknown SGP",
			association: aspAssociationForSGP(t, SGPIdentity{
				SignallingGateway:        "sg-a",
				SignallingGatewayProcess: "sgp-zz",
			}, 7, 1),
			want: ErrUnknownSGP,
		},
		{
			name: "Signalling Gateway of another SGP",
			association: aspAssociationForSGP(t, SGPIdentity{
				SignallingGateway:        "sg-b",
				SignallingGatewayProcess: "sgp-a1",
			}, 7, 1),
			want: ErrUnknownSGP,
		},
		{
			name: "Routing Context not provisioned for the SGP",
			association: aspAssociationForSGP(t, SGPIdentity{
				SignallingGateway:        "sg-a",
				SignallingGatewayProcess: "sgp-a1",
			}, 7, 4),
			want: ErrSGPRouteScopeMismatch,
		},
		{
			name: "Network Appearance not provisioned for the SGP",
			association: aspAssociationForSGP(t, SGPIdentity{
				SignallingGateway:        "sg-a",
				SignallingGatewayProcess: "sgp-a1",
			}, 8, 1),
			want: ErrSGPRouteScopeMismatch,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := endpoint.validateAssociationConfig(test.association.cfg); !errors.Is(err, test.want) {
				t.Fatalf("validateAssociationConfig() error = %v, want %v", err, test.want)
			}
			// A failed provisioned lookup must never fall back to the
			// permissive standalone path.
			if endpoint.trackAssociation(test.association) {
				t.Fatal("unprovisioned Association was attached")
			}
		})
	}
}

// A missing SGP identity is missing configuration, not a standalone
// association: the Endpoint provisions peers, so the Association must name one.
func TestASPInventoryRejectsAssociationWithoutSGPIdentity(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: inventoryOnlyASPConfig()})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	association, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
	t.Cleanup(func() { _ = association.Close() })
	if err := endpoint.validateAssociationConfig(association.cfg); !errors.Is(err, ErrMissingSGPIdentity) {
		t.Fatalf("validateAssociationConfig() error = %v, want %v", err, ErrMissingSGPIdentity)
	}
	if endpoint.trackAssociation(association) {
		t.Fatal("Association without an SGP identity was attached")
	}
}

// With no ASP inventory at all the existing standalone associations stay
// supported and need no SGP identity.
func TestStandaloneASPAssociationStaysSupported(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	association, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
	t.Cleanup(func() { _ = association.Close() })
	if err := endpoint.validateAssociationConfig(association.cfg); err != nil {
		t.Fatalf("standalone Association rejected: %v", err)
	}
	if !endpoint.trackAssociation(association) {
		t.Fatal("standalone Association was not attached")
	}
}

// Bullet 2: static, contextless and dynamic bindings, absence against an
// explicit zero, and every scope that cannot be told apart on the wire.
func TestASPInventoryApplicationServerBinding(t *testing.T) {
	sgp := func(servers ...RemoteASConfig) SignallingGatewayProcessConfig {
		return SignallingGatewayProcessConfig{ID: "sgp-a1", ApplicationServers: servers}
	}
	gateway := func(sgps ...SignallingGatewayProcessConfig) *ASPConfig {
		return &ASPConfig{SignallingGateways: []SignallingGatewayConfig{{ID: "sg-a", SGPs: sgps}}}
	}
	sampleRoutingKey := func(dpc uint32) *RoutingKey {
		return &RoutingKey{Groups: []RoutingKeyGroup{{DestinationPointCode: dpc}}}
	}

	tests := []struct {
		name    string
		config  *ASPConfig
		wantErr bool
	}{
		{
			name:   "static binding",
			config: gateway(sgp(RemoteASConfig{ID: "as-core", ASKey: staticASKey(7, 1)})),
		},
		{
			name:   "explicit contextless Application Server alone on its SGP",
			config: gateway(sgp(RemoteASConfig{ID: "as-core", ASKey: &ASKey{}})),
		},
		{
			name: "explicit zero Routing Context is not an absent one",
			config: gateway(sgp(RemoteASConfig{
				ID:    "as-core",
				ASKey: &ASKey{RoutingContextSet: true},
			})),
		},
		{
			// An explicit Routing Context of 0 names a scope, so it is not the
			// contextless Application Server that may not share its SGP.
			name: "explicit zero Routing Context may share its SGP",
			config: gateway(sgp(
				RemoteASConfig{ID: "as-zero", ASKey: &ASKey{RoutingContextSet: true}},
				RemoteASConfig{ID: "as-one", ASKey: staticASKey(7, 1)},
			)),
		},
		{
			name: "explicit zero Network Appearance is not an absent one",
			config: gateway(sgp(
				RemoteASConfig{ID: "as-zero", ASKey: &ASKey{NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true}},
				RemoteASConfig{ID: "as-absent", ASKey: &ASKey{RoutingContext: 1, RoutingContextSet: true}},
			)),
		},
		{
			name:   "dynamic binding",
			config: gateway(sgp(RemoteASConfig{ID: "as-core", RoutingKey: sampleRoutingKey(0x111111)})),
		},
		{
			name: "static and dynamic bindings coexist on one SGP",
			config: gateway(sgp(
				RemoteASConfig{ID: "as-static", ASKey: staticASKey(7, 1)},
				RemoteASConfig{ID: "as-dynamic", RoutingKey: sampleRoutingKey(0x222222)},
			)),
		},
		{
			name:    "no binding at all",
			config:  gateway(sgp(RemoteASConfig{ID: "as-core"})),
			wantErr: true,
		},
		{
			name: "both bindings",
			config: gateway(sgp(RemoteASConfig{
				ID:         "as-core",
				ASKey:      staticASKey(7, 1),
				RoutingKey: sampleRoutingKey(0x111111),
			})),
			wantErr: true,
		},
		{
			name:    "empty Application Server name",
			config:  gateway(sgp(RemoteASConfig{ASKey: staticASKey(7, 1)})),
			wantErr: true,
		},
		{
			name: "duplicate Application Server name on one SGP",
			config: gateway(sgp(
				RemoteASConfig{ID: "as-core", ASKey: staticASKey(7, 1)},
				RemoteASConfig{ID: "as-core", ASKey: staticASKey(7, 2)},
			)),
			wantErr: true,
		},
		{
			name: "two Application Servers share one wire scope",
			config: gateway(sgp(
				RemoteASConfig{ID: "as-one", ASKey: staticASKey(7, 1)},
				RemoteASConfig{ID: "as-two", ASKey: staticASKey(7, 1)},
			)),
			wantErr: true,
		},
		{
			name: "contextless Application Server beside a second one",
			config: gateway(sgp(
				RemoteASConfig{ID: "as-contextless", ASKey: &ASKey{}},
				RemoteASConfig{ID: "as-core", ASKey: staticASKey(7, 1)},
			)),
			wantErr: true,
		},
		{
			name: "contextless Application Server beside a dynamic one",
			config: gateway(sgp(
				RemoteASConfig{ID: "as-contextless", ASKey: &ASKey{}},
				RemoteASConfig{ID: "as-dynamic", RoutingKey: sampleRoutingKey(0x111111)},
			)),
			wantErr: true,
		},
		{
			name: "Network Appearance value without presence",
			config: gateway(sgp(RemoteASConfig{
				ID:    "as-core",
				ASKey: &ASKey{NetworkAppearance: 7, RoutingContext: 1, RoutingContextSet: true},
			})),
			wantErr: true,
		},
		{
			name: "Routing Context value without presence",
			config: gateway(sgp(RemoteASConfig{
				ID:    "as-core",
				ASKey: &ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContext: 1},
			})),
			wantErr: true,
		},
		{
			name: "overlapping dynamic Routing Keys on one SGP",
			config: gateway(sgp(
				RemoteASConfig{ID: "as-one", RoutingKey: sampleRoutingKey(0x111111)},
				RemoteASConfig{ID: "as-two", RoutingKey: sampleRoutingKey(0x111111)},
			)),
			wantErr: true,
		},
		{
			name:    "SGP without any Application Server",
			config:  gateway(sgp()),
			wantErr: true,
		},
		{
			name:    "Signalling Gateway without any SGP",
			config:  &ASPConfig{SignallingGateways: []SignallingGatewayConfig{{ID: "sg-a"}}},
			wantErr: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := snapshotASPConfig(test.config)
			if test.wantErr {
				if !errors.Is(err, ErrInvalidASPConfig) {
					t.Fatalf("snapshotASPConfig() error = %v, want %v", err, ErrInvalidASPConfig)
				}
				return
			}
			if err != nil {
				t.Fatalf("snapshotASPConfig() error = %v, want nil", err)
			}
		})
	}
}

// Bullet 3: one canonical Application Server can wear different wire labels on
// different SGPs of its Signalling Gateway, and the same local name in another
// Signalling Gateway is a different Application Server.
func TestASPInventoryCanonicalIdentityIsPerSignallingGateway(t *testing.T) {
	config := &ASPConfig{
		SignallingGateways: []SignallingGatewayConfig{
			{
				ID: "sg-a",
				SGPs: []SignallingGatewayProcessConfig{
					{ID: "sgp-a1", ApplicationServers: []RemoteASConfig{{ID: "as-core", ASKey: staticASKey(7, 1)}}},
					{ID: "sgp-a2", ApplicationServers: []RemoteASConfig{{ID: "as-core", ASKey: staticASKey(8, 2)}}},
				},
			},
			{
				ID: "sg-b",
				SGPs: []SignallingGatewayProcessConfig{
					{ID: "sgp-b1", ApplicationServers: []RemoteASConfig{{ID: "as-core", ASKey: staticASKey(9, 1)}}},
				},
			},
		},
	}
	snapshot, err := snapshotASPConfig(config)
	if err != nil {
		t.Fatalf("snapshotASPConfig: %v", err)
	}

	cases := []struct {
		identity SGPIdentity
		want     ASKey
	}{
		{SGPIdentity{"sg-a", "sgp-a1"}, *staticASKey(7, 1)},
		{SGPIdentity{"sg-a", "sgp-a2"}, *staticASKey(8, 2)},
		{SGPIdentity{"sg-b", "sgp-b1"}, *staticASKey(9, 1)},
	}
	for _, test := range cases {
		key, resolved := snapshot.staticASKeyFor(test.identity, "as-core")
		if !resolved {
			t.Fatalf("%+v has no binding for as-core", test.identity)
		}
		if key != test.want {
			t.Fatalf("%+v as-core = %+v, want %+v", test.identity, key, test.want)
		}
	}

	// The two Signalling Gateways name unrelated Application Servers, so the
	// canonical identities differ even though the local names are equal.
	if (SGASKey{SignallingGateway: "sg-a", ApplicationServer: "as-core"}) ==
		(SGASKey{SignallingGateway: "sg-b", ApplicationServer: "as-core"}) {
		t.Fatal("equal local names in distinct Signalling Gateways must not be one identity")
	}
	if _, resolved := snapshot.staticASKeyFor(SGPIdentity{"sg-a", "sgp-b1"}, "as-core"); resolved {
		t.Fatal("an SGP of another Signalling Gateway resolved an Application Server")
	}
	if _, resolved := snapshot.staticASKeyFor(SGPIdentity{"sg-a", "sgp-a1"}, "as-other"); resolved {
		t.Fatal("an unprovisioned Application Server name resolved")
	}
}

// A route names the canonical Application Server, so it reaches exactly the
// SGPs of that Signalling Gateway which serve it, under whatever wire label
// each of them uses.
func TestASPRoutingBindsRoutesToCanonicalApplicationServers(t *testing.T) {
	config := &ASPConfig{
		SignallingGateways: []SignallingGatewayConfig{
			{
				ID: "sg-a",
				SGPs: []SignallingGatewayProcessConfig{
					{ID: "sgp-a1", ApplicationServers: []RemoteASConfig{{ID: "as-core", ASKey: staticASKey(7, 1)}}},
					{ID: "sgp-a2", ApplicationServers: []RemoteASConfig{{ID: "as-core", ASKey: staticASKey(8, 2)}}},
				},
			},
			{
				ID: "sg-b",
				SGPs: []SignallingGatewayProcessConfig{
					{ID: "sgp-b1", ApplicationServers: []RemoteASConfig{{ID: "as-core", ASKey: staticASKey(9, 3)}}},
				},
			},
		},
		Routing: &ASPRoutingConfig{
			SignallingGatewaySelection: RouteSelectionLoadshare,
			SignallingGatewayProcessSelection: map[SignallingGatewayID]RouteSelectionMode{
				"sg-a": RouteSelectionPrimaryBackup,
				"sg-b": RouteSelectionLoadshare,
			},
			MTPRoutes: []MTPRouteConfig{{
				ID:                    "sccp-a",
				DestinationPointCode:  0x120000,
				Mask:                  16,
				ServiceIndicators:     []uint8{3},
				OriginatingPointCodes: []uint32{0x111111},
			}},
			Routes: []MTPRouteBinding{
				{MTPRoute: "sccp-a", AS: SGASKey{SignallingGateway: "sg-a", ApplicationServer: "as-core"}},
				{MTPRoute: "sccp-a", AS: SGASKey{SignallingGateway: "sg-b", ApplicationServer: "as-core"}},
			},
		},
	}
	snapshot, err := snapshotASPConfig(config)
	if err != nil {
		t.Fatalf("snapshotASPConfig: %v", err)
	}
	if !snapshot.routingConfigured {
		t.Fatal("a non-nil Routing must configure outbound routing")
	}
	want := map[SGPIdentity]ASKey{
		{"sg-a", "sgp-a1"}: *staticASKey(7, 1),
		{"sg-a", "sgp-a2"}: *staticASKey(8, 2),
		{"sg-b", "sgp-b1"}: *staticASKey(9, 3),
	}
	for identity, wantKey := range want {
		sgp, exists := snapshot.sgpByIdentity[identity]
		if !exists {
			t.Fatalf("%+v is not provisioned", identity)
		}
		route, routed := aspSGPRouteForMTPRoute(sgp, "sccp-a")
		if !routed {
			t.Fatalf("%+v does not carry sccp-a", identity)
		}
		if route.as != wantKey {
			t.Fatalf("%+v carries sccp-a as %+v, want %+v", identity, route.as, wantKey)
		}
	}
}

// Routing that names something the peer inventory does not provision is a
// configuration error, not a route that silently reaches nothing.
func TestASPRoutingRejectsUnprovisionedReferences(t *testing.T) {
	base := func() *ASPConfig {
		return &ASPConfig{
			SignallingGateways: []SignallingGatewayConfig{{
				ID: "sg-a",
				SGPs: []SignallingGatewayProcessConfig{{
					ID:                 "sgp-a1",
					ApplicationServers: []RemoteASConfig{{ID: "as-core", ASKey: staticASKey(7, 1)}},
				}},
			}},
			Routing: &ASPRoutingConfig{
				SignallingGatewaySelection: RouteSelectionLoadshare,
				SignallingGatewayProcessSelection: map[SignallingGatewayID]RouteSelectionMode{
					"sg-a": RouteSelectionPrimaryBackup,
				},
				MTPRoutes: []MTPRouteConfig{{ID: "sccp-a", DestinationPointCode: 0x120000, Mask: 16}},
				Routes: []MTPRouteBinding{
					{MTPRoute: "sccp-a", AS: SGASKey{SignallingGateway: "sg-a", ApplicationServer: "as-core"}},
				},
			},
		}
	}
	tests := []struct {
		name   string
		mutate func(*ASPConfig)
	}{
		{
			name:   "unknown MTP Route",
			mutate: func(c *ASPConfig) { c.Routing.Routes[0].MTPRoute = "absent" },
		},
		{
			name:   "unknown Signalling Gateway",
			mutate: func(c *ASPConfig) { c.Routing.Routes[0].AS.SignallingGateway = "sg-zz" },
		},
		{
			name:   "unknown Application Server",
			mutate: func(c *ASPConfig) { c.Routing.Routes[0].AS.ApplicationServer = "as-zz" },
		},
		{
			name:   "MTP Route with no binding",
			mutate: func(c *ASPConfig) { c.Routing.Routes = nil },
		},
		{
			name:   "duplicate binding",
			mutate: func(c *ASPConfig) { c.Routing.Routes = append(c.Routing.Routes, c.Routing.Routes[0]) },
		},
		{
			name:   "no SGP selection for a routed Signalling Gateway",
			mutate: func(c *ASPConfig) { c.Routing.SignallingGatewayProcessSelection = nil },
		},
		{
			name: "SGP selection for an unprovisioned Signalling Gateway",
			mutate: func(c *ASPConfig) {
				c.Routing.SignallingGatewayProcessSelection["sg-zz"] = RouteSelectionLoadshare
			},
		},
		{
			name:   "invalid Signalling Gateway selection",
			mutate: func(c *ASPConfig) { c.Routing.SignallingGatewaySelection = RouteSelectionMode(0xff) },
		},
		{
			name: "invalid SGP selection",
			mutate: func(c *ASPConfig) {
				c.Routing.SignallingGatewayProcessSelection["sg-a"] = RouteSelectionMode(0xff)
			},
		},
		{
			name:   "no MTP Routes",
			mutate: func(c *ASPConfig) { c.Routing.MTPRoutes = nil },
		},
		{
			name:   "negative transfer flow cache",
			mutate: func(c *ASPConfig) { c.Routing.TransferFlowCacheEntries = -1 },
		},
		{
			// A dynamically bound Application Server has no wire Routing
			// Context until RFC 4666 Section 4.4.1 registration assigns one, so
			// a route through it would never carry traffic.
			name: "route through a dynamically bound Application Server",
			mutate: func(c *ASPConfig) {
				c.SignallingGateways[0].SGPs[0].ApplicationServers[0] = RemoteASConfig{
					ID:         "as-core",
					RoutingKey: &RoutingKey{Groups: []RoutingKeyGroup{{DestinationPointCode: 0x120000}}},
				}
			},
		},
		{
			// The same Application Server statically bound on one SGP and
			// dynamically bound on another is still not routable yet.
			name: "route through an Application Server one SGP binds dynamically",
			mutate: func(c *ASPConfig) {
				c.SignallingGateways[0].SGPs = append(c.SignallingGateways[0].SGPs,
					SignallingGatewayProcessConfig{
						ID: "sgp-a2",
						ApplicationServers: []RemoteASConfig{{
							ID:         "as-core",
							RoutingKey: &RoutingKey{Groups: []RoutingKeyGroup{{DestinationPointCode: 0x120000}}},
						}},
					})
			},
		},
		{
			name: "one SGP carries a route through two Application Servers",
			mutate: func(c *ASPConfig) {
				c.SignallingGateways[0].SGPs[0].ApplicationServers = append(
					c.SignallingGateways[0].SGPs[0].ApplicationServers,
					RemoteASConfig{ID: "as-second", ASKey: staticASKey(7, 2)},
				)
				c.Routing.Routes = append(c.Routing.Routes, MTPRouteBinding{
					MTPRoute: "sccp-a",
					AS:       SGASKey{SignallingGateway: "sg-a", ApplicationServer: "as-second"},
				})
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := base()
			test.mutate(config)
			if _, err := snapshotASPConfig(config); !errors.Is(err, ErrInvalidASPConfig) {
				t.Fatalf("snapshotASPConfig() error = %v, want %v", err, ErrInvalidASPConfig)
			}
		})
	}
}

// With application-managed routing the Endpoint refuses to select an outbound
// route, and the Association keeps the inbound DATA authorization its
// provisioned scope established.
func TestASPInventoryWithoutRoutingKeepsDataAuthorization(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: inventoryOnlyASPConfig()})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	association, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
	t.Cleanup(func() { _ = association.Close() })
	association.cfg.NetworkAppearance = params.NewNetworkAppearance(7)
	association.cfg.PeerSGP = &SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}
	association.noteRoutingContextsAcked(params.NewRoutingContext(1))
	if !endpoint.trackAssociation(association) {
		t.Fatal("provisioned Association was not attached")
	}

	if _, err := endpoint.MTPTransfer(MTPTransferRequest{
		ProtocolData: params.NewProtocolDataPayload(0x111111, 0x123456, params.ServiceIndSCCP, 0, 0, 1, []byte("x")),
	}); !errors.Is(err, ErrRoutingNotConfigured) {
		t.Fatalf("MTPTransfer() error = %v, want %v", err, ErrRoutingNotConfigured)
	}

	association.handleData(context.Background(), messages.NewData(
		nil,
		params.NewRoutingContext(1),
		params.NewProtocolData(0x111111, 0x123456, params.ServiceIndSCCP, 0, 0, 1, []byte("provisioned")),
		nil,
	), nil)
	delivered, err := association.ReadData()
	if err != nil {
		t.Fatalf("ReadData: %v", err)
	}
	if !delivered.RoutingContextSet || delivered.RoutingContext != 1 {
		t.Fatalf("delivered Routing Context = %d set=%v, want 1", delivered.RoutingContext, delivered.RoutingContextSet)
	}

	// RFC 4666 Section 3.8.1 refuses DATA naming an unconfigured Routing
	// Context; provisioning peers must not relax that.
	association.handleData(context.Background(), messages.NewData(
		nil,
		params.NewRoutingContext(4),
		params.NewProtocolData(0x111111, 0x123456, params.ServiceIndSCCP, 0, 0, 1, []byte("unprovisioned")),
		nil,
	), nil)
	refusal := firstErr(association)
	if refusal == nil || !errors.Is(refusal, ErrInvalidRoutingContext) {
		t.Fatalf("DATA naming an unconfigured Routing Context produced %v, want %v",
			refusal, ErrInvalidRoutingContext)
	}
	select {
	case message := <-association.dataChan:
		t.Fatalf("unconfigured DATA was delivered: %v", message)
	default:
	}
}
