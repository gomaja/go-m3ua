// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

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
		setInventoryNetworkAppearance(&association.cfg.ApplicationServers, params.NewNetworkAppearance(networkAppearance))
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
	setInventoryNetworkAppearance(&association.cfg.ApplicationServers, params.NewNetworkAppearance(7))
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
	delivered, err := association.ReadData(context.Background())
	if err != nil {
		t.Fatalf("ReadData: %v", err)
	}
	if !delivered.Scope.RoutingContextSet || wireRoutingContext(delivered.Scope) != 1 {
		t.Fatalf("delivered Routing Context = %d set=%v, want 1", wireRoutingContext(delivered.Scope), delivered.Scope.RoutingContextSet)
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

// One canonical Application Server is one membership however many local routes
// name it and however many ASPs serve it. RFC 4666 Section 1.4.2 makes the
// Application Server the entity an ASP serves; the routes an ASP keeps towards
// it are its own bookkeeping and change nothing about that membership.
func TestASPRoutesShareOneApplicationServerAcrossRoutesAndASPs(t *testing.T) {
	config := &ASPConfig{
		SignallingGateways: []SignallingGatewayConfig{{
			ID: "sg-a",
			SGPs: []SignallingGatewayProcessConfig{
				{ID: "sgp-a1", ApplicationServers: []RemoteASConfig{{ID: "as-core", ASKey: staticASKey(7, 1)}}},
				{ID: "sgp-a2", ApplicationServers: []RemoteASConfig{{ID: "as-core", ASKey: staticASKey(8, 2)}}},
			},
		}},
		Routing: &ASPRoutingConfig{
			SignallingGatewaySelection: RouteSelectionBroadcast,
			SignallingGatewayProcessSelection: map[SignallingGatewayID]RouteSelectionMode{
				"sg-a": RouteSelectionBroadcast,
			},
			MTPRoutes: []MTPRouteConfig{
				{ID: "sccp", DestinationPointCode: 0x120000, Mask: 16, ServiceIndicators: []uint8{params.ServiceIndSCCP}},
				{ID: "isup", DestinationPointCode: 0x120000, Mask: 16, ServiceIndicators: []uint8{params.ServiceIndISUP}},
			},
			Routes: []MTPRouteBinding{
				{MTPRoute: "sccp", AS: SGASKey{SignallingGateway: "sg-a", ApplicationServer: "as-core"}},
				{MTPRoute: "isup", AS: SGASKey{SignallingGateway: "sg-a", ApplicationServer: "as-core"}},
			},
		},
	}
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: config})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })

	transfer := func(serviceIndicator uint8) error {
		_, err := endpoint.MTPTransfer(MTPTransferRequest{
			ProtocolData: params.NewProtocolDataPayload(
				0x111111, 0x123456, serviceIndicator, 0, 0, 1, []byte("x")),
		})
		return err
	}
	requireNoRoute := func(stage string) {
		t.Helper()
		for _, serviceIndicator := range []uint8{params.ServiceIndSCCP, params.ServiceIndISUP} {
			if err := transfer(serviceIndicator); !errors.Is(err, ErrNoMTPRoute) {
				t.Fatalf("%s: MTPTransfer(SI %d) error = %v, want %v",
					stage, serviceIndicator, err, ErrNoMTPRoute)
			}
		}
	}

	type member struct {
		association *Association
		capture     *mtpTransferCapture
		signals     *[]messages.M3UA
	}
	join := func(sgp SignallingGatewayProcessID, networkAppearance, routingContext uint32) member {
		t.Helper()
		association, signals := newTestConnWithContexts(t, StateASPActive, RoleASP, routingContext)
		setInventoryNetworkAppearance(&association.cfg.ApplicationServers, params.NewNetworkAppearance(networkAppearance))
		association.cfg.PeerSGP = &SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: sgp}
		association.noteRoutingContextsAcked(params.NewRoutingContext(routingContext))
		capture := &mtpTransferCapture{}
		association.dataWriter = capture.write
		if !endpoint.trackAssociation(association) {
			t.Fatalf("ASP on %s was not attached", sgp)
		}
		return member{association: association, capture: capture, signals: signals}
	}
	// Broadcast reaches every SGP serving the Application Server once per
	// route. Several ASPs on one SGP are several Associations to one peer
	// process, so exactly one of them carries each flow: RFC 4666 Appendix
	// A.2.2 broadcasts between SGPs, not between Associations to one SGP.
	requireDelivery := func(stage string, groups map[SignallingGatewayProcessID][]member) {
		t.Helper()
		before := make(map[SignallingGatewayProcessID]int, len(groups))
		for sgp, members := range groups {
			for _, joined := range members {
				before[sgp] += joined.capture.count()
			}
		}
		for _, serviceIndicator := range []uint8{params.ServiceIndSCCP, params.ServiceIndISUP} {
			if err := transfer(serviceIndicator); err != nil {
				t.Fatalf("%s: MTPTransfer(SI %d): %v", stage, serviceIndicator, err)
			}
		}
		for sgp, members := range groups {
			carried := 0
			for _, joined := range members {
				carried += joined.capture.count()
			}
			if carried-before[sgp] != 2 {
				t.Fatalf("%s: SGP %s carried %d of the 2 routes naming its Application Server",
					stage, sgp, carried-before[sgp])
			}
		}
	}
	requireNoProcedures := func(stage string, members []member) {
		t.Helper()
		for index, joined := range members {
			if len(*joined.signals) != 0 {
				t.Fatalf("%s: ASP %d ran %d procedure(s) because the routes naming its "+
					"Application Server changed: %v", stage, index, len(*joined.signals), *joined.signals)
			}
			if state := joined.association.State(); state != StateASPActive {
				t.Fatalf("%s: ASP %d left ASP-ACTIVE for %v", stage, index, state)
			}
		}
	}

	requireNoRoute("no ASP")

	first := join("sgp-a1", 7, 1)
	requireDelivery("one ASP", map[SignallingGatewayProcessID][]member{"sgp-a1": {first}})
	requireNoProcedures("one ASP", []member{first})

	second := join("sgp-a1", 7, 1)
	third := join("sgp-a2", 8, 2)
	all := []member{first, second, third}
	requireDelivery("many ASPs", map[SignallingGatewayProcessID][]member{
		"sgp-a1": {first, second},
		"sgp-a2": {third},
	})
	requireNoProcedures("many ASPs", all)

	for index, joined := range all {
		if err := joined.association.Close(); err != nil {
			t.Fatalf("close ASP %d: %v", index, err)
		}
	}
	requireNoRoute("every ASP gone")
}

// aspSignalCapture records every M3UA signal an Association writes. The
// procedure calls under test run in their own goroutine while the test feeds
// acknowledgements, so the record needs its own lock rather than the plain
// slice newTestConn installs.
type aspSignalCapture struct {
	mu   sync.Mutex
	sent []messages.M3UA
}

func (c *aspSignalCapture) write(message messages.M3UA) (int, error) {
	c.mu.Lock()
	c.sent = append(c.sent, message)
	c.mu.Unlock()
	return message.MarshalLen(), nil
}

func (c *aspSignalCapture) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.sent)
}

func (c *aspSignalCapture) snapshot() []messages.M3UA {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]messages.M3UA(nil), c.sent...)
}

// referenceCountPeerInventory provisions the one Signalling Gateway Process
// every reference-count stage shares. Only the route bindings differ between
// stages, so anything the Endpoint does differently is caused by the number of
// local references to as-core and by nothing else.
func referenceCountPeerInventory() []SignallingGatewayConfig {
	return []SignallingGatewayConfig{{
		ID: "sg-a",
		SGPs: []SignallingGatewayProcessConfig{{
			ID: "sgp-a1",
			ApplicationServers: []RemoteASConfig{
				{ID: "as-core", ASKey: staticASKey(7, 1)},
				{ID: "as-spare", ASKey: staticASKey(7, 2)},
			},
		}},
	}}
}

func referenceCountMTPRoutes() []MTPRouteConfig {
	return []MTPRouteConfig{
		{ID: "spare", DestinationPointCode: 0x130000, Mask: 16, ServiceIndicators: []uint8{params.ServiceIndSCCP}},
		{ID: "core-sccp", DestinationPointCode: 0x120000, Mask: 16, ServiceIndicators: []uint8{params.ServiceIndSCCP}},
		{ID: "core-isup", DestinationPointCode: 0x120000, Mask: 16, ServiceIndicators: []uint8{params.ServiceIndISUP}},
		{ID: "core-tup", DestinationPointCode: 0x120000, Mask: 16, ServiceIndicators: []uint8{params.ServiceIndTUP}},
	}
}

func referenceCountRouting(routes []MTPRouteID, bindings []MTPRouteBinding) *ASPRoutingConfig {
	configured := make([]MTPRouteConfig, 0, len(routes))
	for _, wanted := range routes {
		for _, mtpRoute := range referenceCountMTPRoutes() {
			if mtpRoute.ID == wanted {
				configured = append(configured, mtpRoute)
			}
		}
	}
	return &ASPRoutingConfig{
		SignallingGatewaySelection: RouteSelectionLoadshare,
		SignallingGatewayProcessSelection: map[SignallingGatewayID]RouteSelectionMode{
			"sg-a": RouteSelectionPrimaryBackup,
		},
		MTPRoutes: configured,
		Routes:    bindings,
	}
}

func referenceCountBinding(mtpRoute MTPRouteID, applicationServer RemoteASID) MTPRouteBinding {
	return MTPRouteBinding{
		MTPRoute: mtpRoute,
		AS:       SGASKey{SignallingGateway: "sg-a", ApplicationServer: applicationServer},
	}
}

// countASReferences counts the MTPRouteBinding entries naming one Application
// Server, which is exactly the application-owned reference count under test.
func countASReferences(config *ASPConfig, applicationServer RemoteASID) int {
	if config.Routing == nil {
		return 0
	}
	references := 0
	for _, binding := range config.Routing.Routes {
		if binding.AS.ApplicationServer == applicationServer {
			references++
		}
	}
	return references
}

// TestASPApplicationServerReferenceCountIsInvisibleToTheProtocol covers the
// second half of bullet 4: the number of application-owned references to one
// Application Server changes nothing an M3UA peer can observe.
//
// NewEndpoint snapshots its configuration, so the reference count cannot be
// mutated on a running Endpoint; the requirement is therefore demonstrated
// across distinct Endpoint configurations in which everything but the number
// of MTPRouteBinding entries naming as-core is held fixed. RFC 4666 Section
// 1.4.2 makes the Application Server an entity of the signalling network that
// an ASP serves, while the routes an ASP keeps towards it are local
// bookkeeping; RFC 4666 Sections 4.3.4.1 and 4.3.4.3 make establishment and
// activation functions of that membership alone.
//
// Each stage asserts on the octets the Association actually emitted, not on
// Endpoint state, because the claim is about what the peer can tell apart.
func TestASPApplicationServerReferenceCountIsInvisibleToTheProtocol(t *testing.T) {
	stages := []struct {
		name       string
		references int
		routing    *ASPRoutingConfig
	}{
		{
			// No binding names as-core, though the route inventory exists and
			// carries another Application Server of the same SGP.
			name:       "no reference",
			references: 0,
			routing: referenceCountRouting(
				[]MTPRouteID{"spare"},
				[]MTPRouteBinding{referenceCountBinding("spare", "as-spare")},
			),
		},
		{
			name:       "one reference",
			references: 1,
			routing: referenceCountRouting(
				[]MTPRouteID{"spare", "core-sccp"},
				[]MTPRouteBinding{
					referenceCountBinding("spare", "as-spare"),
					referenceCountBinding("core-sccp", "as-core"),
				},
			),
		},
		{
			name:       "many references",
			references: 3,
			routing: referenceCountRouting(
				[]MTPRouteID{"spare", "core-sccp", "core-isup", "core-tup"},
				[]MTPRouteBinding{
					referenceCountBinding("spare", "as-spare"),
					referenceCountBinding("core-sccp", "as-core"),
					referenceCountBinding("core-isup", "as-core"),
					referenceCountBinding("core-tup", "as-core"),
				},
			),
		},
		{
			// Back to no reference by a different arrangement than the first
			// stage: the routes that named as-core still exist and now name
			// as-spare instead.
			name:       "no reference again",
			references: 0,
			routing: referenceCountRouting(
				[]MTPRouteID{"spare", "core-sccp", "core-isup", "core-tup"},
				[]MTPRouteBinding{
					referenceCountBinding("spare", "as-spare"),
					referenceCountBinding("core-sccp", "as-spare"),
					referenceCountBinding("core-isup", "as-spare"),
					referenceCountBinding("core-tup", "as-spare"),
				},
			),
		},
		{
			// The other way to hold no reference: application-managed routing,
			// where the Endpoint owns no outbound route inventory at all.
			name:       "no route inventory at all",
			references: 0,
			routing:    nil,
		},
	}

	for _, stage := range stages {
		t.Run(stage.name, func(t *testing.T) {
			config := &ASPConfig{
				SignallingGateways: referenceCountPeerInventory(),
				Routing:            stage.routing,
			}
			if got := countASReferences(config, "as-core"); got != stage.references {
				t.Fatalf("stage names as-core in %d route bindings, want %d", got, stage.references)
			}
			endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: config})
			if err != nil {
				t.Fatalf("NewEndpoint: %v", err)
			}
			t.Cleanup(func() { _ = endpoint.Close() })

			// ASPSM and ASPTM messages travel on SCTP stream 0, so the
			// Association keeps its default receive stream until the DATA
			// exchange below, which must not use stream 0.
			association, _ := newTestConn(t, StateASPDown, RoleASP)
			setInventoryRoutingContexts(&association.cfg.ApplicationServers, params.NewRoutingContext(1))
			setInventoryNetworkAppearance(&association.cfg.ApplicationServers, params.NewNetworkAppearance(7))
			association.cfg.PeerSGP = &SGPIdentity{
				SignallingGateway:        "sg-a",
				SignallingGatewayProcess: "sgp-a1",
			}
			association.cfg.ASPProcedures = explicitASPProcedurePolicy()
			capture := &aspSignalCapture{}
			association.signalWriter = capture.write
			if !endpoint.trackAssociation(association) {
				t.Fatal("Association naming a provisioned SGP was not attached")
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			upResult := make(chan error, 1)
			go func() { upResult <- association.ASPUp(ctx) }()
			if !waitFor(func() bool { return capture.count() >= 1 }, time.Second) {
				t.Fatal("ASP Up was never written")
			}
			// Through the dispatcher entry point rather than the handler, so
			// the acknowledgement commits the state transition it carries.
			association.handleSignals(ctx, messages.NewAspUpAck(nil, nil))
			if err := firstErr(association); err != nil {
				t.Fatalf("ASP Up Ack reported %v", err)
			}
			if err := <-upResult; err != nil {
				t.Fatalf("ASPUp: %v", err)
			}
			if state := association.State(); state != StateASPInactive {
				t.Fatalf("state after ASP Up Ack = %v, want ASP-INACTIVE", state)
			}

			key := ASKey{
				NetworkAppearance:    7,
				NetworkAppearanceSet: true,
				RoutingContext:       1,
				RoutingContextSet:    true,
			}
			activeResult := make(chan error, 1)
			go func() { activeResult <- association.ASPActive(ctx, key) }()
			if !waitFor(func() bool { return capture.count() >= 2 }, time.Second) {
				t.Fatal("ASP Active was never written")
			}
			association.handleSignals(ctx, messages.NewAspActiveAck(
				nil, params.NewRoutingContext(1), nil,
			))
			if err := firstErr(association); err != nil {
				t.Fatalf("ASP Active Ack reported %v", err)
			}
			if err := <-activeResult; err != nil {
				t.Fatalf("ASPActive: %v", err)
			}

			// Establishment and activation emitted one ASP Up and one ASP
			// Active, whatever the reference count. A second ASP Up would be
			// the reconnect the bullet forbids.
			emitted := capture.snapshot()
			if got := typeNames(emitted); len(got) != 2 || got[0] != "ASP Up" || got[1] != "ASP Active" {
				t.Fatalf("emitted %v, want exactly [ASP Up, ASP Active]", got)
			}
			active, ok := emitted[1].(*messages.AspActive)
			if !ok {
				t.Fatalf("second signal is %T, want *messages.AspActive", emitted[1])
			}
			if got := active.RoutingContext.RoutingContexts(); len(got) != 1 || got[0] != 1 {
				t.Fatalf("ASP Active Routing Contexts = %v, want [1]", got)
			}

			// RFC 4666 Section 4.4 registration stays optional and explicit:
			// holding more or fewer local references to an Application Server
			// never asks the peer to register or deregister one.
			for index, message := range emitted {
				switch message.(type) {
				case *messages.RegistrationRequest, *messages.DeregistrationRequest:
					t.Fatalf("signal %d is %T: the reference count drove an RKM procedure",
						index, message)
				}
			}

			// The Association stayed up and stayed ASP-ACTIVE.
			if state := association.State(); state != StateASPActive {
				t.Fatalf("state after ASP Active Ack = %v, want ASP-ACTIVE", state)
			}
			select {
			case <-association.done:
				t.Fatalf("Association was torn down: %v", association.Err())
			default:
			}
			if err := association.Err(); err != nil {
				t.Fatalf("Association reported %v", err)
			}

			association.maxMessageStreamID = 4
			association.recvStream.Store(1)

			// Inbound DATA authorization is the Association's coordinated
			// scope, not the local route inventory. Routing Context 2 belongs
			// to as-spare, which this SGP also serves, so accepting it would
			// mean the inventory had widened what this Association may carry.
			association.handleData(context.Background(), messages.NewData(
				nil,
				params.NewRoutingContext(1),
				params.NewProtocolData(0x111111, 0x120000, params.ServiceIndSCCP, 0, 0, 1, []byte("core")),
				nil,
			), nil)
			delivered, err := association.ReadData(context.Background())
			if err != nil {
				t.Fatalf("ReadData: %v", err)
			}
			if !delivered.Scope.RoutingContextSet || wireRoutingContext(delivered.Scope) != 1 {
				t.Fatalf("delivered Routing Context = %d set=%v, want 1",
					wireRoutingContext(delivered.Scope), delivered.Scope.RoutingContextSet)
			}
			for _, routingContext := range []uint32{2, 9} {
				association.handleData(context.Background(), messages.NewData(
					nil,
					params.NewRoutingContext(routingContext),
					params.NewProtocolData(0x111111, 0x120000, params.ServiceIndSCCP, 0, 0, 1, []byte("other")),
					nil,
				), nil)
				refusal := firstErr(association)
				if refusal == nil || !errors.Is(refusal, ErrInvalidRoutingContext) {
					t.Fatalf("DATA naming Routing Context %d produced %v, want %v",
						routingContext, refusal, ErrInvalidRoutingContext)
				}
				select {
				case message := <-association.dataChan:
					t.Fatalf("DATA naming Routing Context %d was delivered: %v",
						routingContext, message)
				default:
				}
			}

			// Nothing above emitted another signal.
			if got := typeNames(capture.snapshot()); len(got) != 2 {
				t.Fatalf("signals after the DATA exchange = %v, want exactly [ASP Up, ASP Active]", got)
			}
		})
	}
}

// A local MTP Route may name only an Application Server that every SGP serving
// it binds statically. This is a deliberate boundary, not an omission.
//
// RFC 4666 Section 4.4.1 gives a dynamically bound Application Server its wire
// Routing Context only when the SGP assigns one in a Registration Response, and
// RFC 4666 Section 4.4 keeps registration optional and explicit. A configured
// outbound route through such an Application Server would therefore name a
// scope that does not exist at configuration time and may never exist, so the
// route would be provisioning that can never carry traffic. Rejecting it at
// configuration time reports that at the one moment the application can still
// fix it.
//
// The boundary is on routes, not on dynamic binding: a dynamically bound
// Application Server is fully supported, and an application reaches it by
// owning its own outbound selection.
func TestRouteBindingRequiresAStaticallyBoundApplicationServer(t *testing.T) {
	dynamic := func() *RemoteASConfig {
		return &RemoteASConfig{
			ID: "as-dynamic",
			RoutingKey: &RoutingKey{Groups: []RoutingKeyGroup{{
				DestinationPointCode: 0x140000,
				ServiceIndicators:    []uint8{params.ServiceIndSCCP},
			}}},
		}
	}
	base := func() *ASPConfig {
		return &ASPConfig{
			SignallingGateways: []SignallingGatewayConfig{{
				ID: "sg-a",
				SGPs: []SignallingGatewayProcessConfig{{
					ID: "sgp-a1",
					ApplicationServers: []RemoteASConfig{
						{ID: "as-core", ASKey: staticASKey(7, 1)},
						*dynamic(),
					},
				}},
			}},
			Routing: &ASPRoutingConfig{
				SignallingGatewaySelection: RouteSelectionLoadshare,
				SignallingGatewayProcessSelection: map[SignallingGatewayID]RouteSelectionMode{
					"sg-a": RouteSelectionPrimaryBackup,
				},
				MTPRoutes: []MTPRouteConfig{
					{ID: "sccp-a", DestinationPointCode: 0x120000, Mask: 16},
				},
				Routes: []MTPRouteBinding{
					{MTPRoute: "sccp-a", AS: SGASKey{SignallingGateway: "sg-a", ApplicationServer: "as-core"}},
				},
			},
		}
	}

	// Provisioned and reachable: no route names the dynamically bound
	// Application Server, so the configuration is accepted and the Association
	// that serves it is authorized without a Routing Context, which is exactly
	// what it has before registration.
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: base()})
	if err != nil {
		t.Fatalf("NewEndpoint with a dynamically bound Application Server and no route to it: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	unregistered, _ := newTestConn(t, StateASPDown, RoleASP)
	setInventoryNetworkAppearance(&unregistered.cfg.ApplicationServers, nil)
	setInventoryRoutingContexts(&unregistered.cfg.ApplicationServers, nil)
	unregistered.cfg.PeerSGP = &SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}
	if err := endpoint.validateAssociationConfig(unregistered.cfg); err != nil {
		t.Fatalf("Association serving a dynamically bound Application Server rejected: %v", err)
	}

	// Naming it from a route is the rejected case, and the rejection says which
	// Application Server and which SGP made the route unroutable.
	routed := base()
	routed.Routing.Routes = append(routed.Routing.Routes, MTPRouteBinding{
		MTPRoute: "sccp-a",
		AS:       SGASKey{SignallingGateway: "sg-a", ApplicationServer: "as-dynamic"},
	})
	_, err = snapshotASPConfig(routed)
	if !errors.Is(err, ErrInvalidASPConfig) {
		t.Fatalf("snapshotASPConfig() error = %v, want %v", err, ErrInvalidASPConfig)
	}
	for _, named := range []string{"dynamically bound", "as-dynamic", "sgp-a1", "sccp-a"} {
		if !strings.Contains(err.Error(), named) {
			t.Fatalf("rejection %q does not name %q", err, named)
		}
	}

	// One SGP binding it dynamically is enough, even where another binds the
	// same canonical Application Server statically: the route would silently
	// stop covering that SGP.
	mixed := base()
	mixed.SignallingGateways[0].SGPs = append(mixed.SignallingGateways[0].SGPs,
		SignallingGatewayProcessConfig{
			ID:                 "sgp-a2",
			ApplicationServers: []RemoteASConfig{{ID: "as-core", RoutingKey: dynamic().RoutingKey}},
		})
	if _, err := snapshotASPConfig(mixed); !errors.Is(err, ErrInvalidASPConfig) {
		t.Fatalf("snapshotASPConfig() error = %v, want %v", err, ErrInvalidASPConfig)
	}
}
