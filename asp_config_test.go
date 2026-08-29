// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"errors"
	"testing"

	"github.com/gomaja/go-m3ua/messages/params"
)

func TestNewEndpointSnapshotsASPRoutingPolicy(t *testing.T) {
	config := validASPConfig()
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: config})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}

	config.SignallingGatewaySelection = RouteSelectionBroadcast
	config.MTPRoutes[0].ID = "changed"
	config.MTPRoutes[0].ServiceIndicators[0] = 0xff
	config.MTPRoutes[0].OriginatingPointCodes[0] = 0xffffff
	config.SignallingGateways[0].ID = "changed"
	config.SignallingGateways[0].SGPSelection = RouteSelectionBroadcast
	config.SignallingGateways[0].SGPs[0].ID = "changed"
	config.SignallingGateways[0].SGPs[0].Routes[0].MTPRoute = "changed"
	config.SignallingGateways[0].SGPs[0].Routes[0].AS = ASKey{}

	snapshot := endpoint.aspRoutes.config
	if snapshot.signallingGatewaySelection != RouteSelectionLoadshare {
		t.Fatalf("SignallingGatewaySelection = %v, want loadshare", snapshot.signallingGatewaySelection)
	}
	if got := snapshot.mtpRoutes[0].id; got != "sccp-a" {
		t.Fatalf("MTP Route ID = %q, want sccp-a", got)
	}
	if got := snapshot.mtpRoutes[0].serviceIndicators[0]; got != 3 {
		t.Fatalf("Service Indicator = %d, want 3", got)
	}
	if got := snapshot.mtpRoutes[0].originatingPointCodes[0]; got != 0x111111 {
		t.Fatalf("Originating Point Code = %#x, want %#x", got, 0x111111)
	}
	if got := snapshot.signallingGateways[0].id; got != "sg-a" {
		t.Fatalf("Signalling Gateway ID = %q, want sg-a", got)
	}
	if got := snapshot.signallingGateways[0].sgpSelection; got != RouteSelectionPrimaryBackup {
		t.Fatalf("SGPSelection = %v, want primary/backup", got)
	}
	if got := snapshot.signallingGateways[0].sgps[0].id; got != "sgp-a1" {
		t.Fatalf("SGP ID = %q, want sgp-a1", got)
	}
	route := snapshot.signallingGateways[0].sgps[0].routes[0]
	if route.mtpRoute != "sccp-a" || route.as != (ASKey{
		NetworkAppearance:    7,
		NetworkAppearanceSet: true,
		RoutingContext:       1,
		RoutingContextSet:    true,
	}) {
		t.Fatalf("SGP route = %#v", route)
	}
}

func TestASPConfigValidation(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ASPConfig)
	}{
		{
			name: "invalid Signalling Gateway selection",
			mutate: func(config *ASPConfig) {
				config.SignallingGatewaySelection = RouteSelectionMode(0xff)
			},
		},
		{
			name: "no MTP Routes",
			mutate: func(config *ASPConfig) {
				config.MTPRoutes = nil
			},
		},
		{
			name: "empty MTP Route ID",
			mutate: func(config *ASPConfig) {
				config.MTPRoutes[0].ID = ""
			},
		},
		{
			name: "duplicate MTP Route ID",
			mutate: func(config *ASPConfig) {
				config.MTPRoutes = append(config.MTPRoutes, config.MTPRoutes[0])
			},
		},
		{
			name: "MTP Route point code above 24 bits",
			mutate: func(config *ASPConfig) {
				config.MTPRoutes[0].DestinationPointCode = 0x1000000
			},
		},
		{
			name: "MTP Route mask above 24 bits",
			mutate: func(config *ASPConfig) {
				config.MTPRoutes[0].Mask = 25
			},
		},
		{
			name: "MTP Route point code is not aligned to mask",
			mutate: func(config *ASPConfig) {
				config.MTPRoutes[0].DestinationPointCode = 0x123456
				config.MTPRoutes[0].Mask = 8
			},
		},
		{
			name: "MTP Route OPC above 24 bits",
			mutate: func(config *ASPConfig) {
				config.MTPRoutes[0].OriginatingPointCodes[0] = 0x1000000
			},
		},
		{
			name: "duplicate MTP Route SI",
			mutate: func(config *ASPConfig) {
				config.MTPRoutes[0].ServiceIndicators = []uint8{3, 3}
			},
		},
		{
			name: "duplicate MTP Route OPC",
			mutate: func(config *ASPConfig) {
				config.MTPRoutes[0].OriginatingPointCodes = []uint32{1, 1}
			},
		},
		{
			name: "no Signalling Gateways",
			mutate: func(config *ASPConfig) {
				config.SignallingGateways = nil
			},
		},
		{
			name: "empty Signalling Gateway ID",
			mutate: func(config *ASPConfig) {
				config.SignallingGateways[0].ID = ""
			},
		},
		{
			name: "duplicate Signalling Gateway ID",
			mutate: func(config *ASPConfig) {
				config.SignallingGateways = append(config.SignallingGateways, config.SignallingGateways[0])
			},
		},
		{
			name: "invalid SGP selection",
			mutate: func(config *ASPConfig) {
				config.SignallingGateways[0].SGPSelection = RouteSelectionMode(0xff)
			},
		},
		{
			name: "no SGPs",
			mutate: func(config *ASPConfig) {
				config.SignallingGateways[0].SGPs = nil
			},
		},
		{
			name: "empty SGP ID",
			mutate: func(config *ASPConfig) {
				config.SignallingGateways[0].SGPs[0].ID = ""
			},
		},
		{
			name: "duplicate SGP ID",
			mutate: func(config *ASPConfig) {
				config.SignallingGateways[0].SGPs = append(
					config.SignallingGateways[0].SGPs,
					config.SignallingGateways[0].SGPs[0],
				)
			},
		},
		{
			name: "SGP without routes",
			mutate: func(config *ASPConfig) {
				config.SignallingGateways[0].SGPs[0].Routes = nil
			},
		},
		{
			name: "SGP route references unknown MTP Route",
			mutate: func(config *ASPConfig) {
				config.SignallingGateways[0].SGPs[0].Routes[0].MTPRoute = "unknown"
			},
		},
		{
			name: "duplicate SGP route",
			mutate: func(config *ASPConfig) {
				config.SignallingGateways[0].SGPs[0].Routes = append(
					config.SignallingGateways[0].SGPs[0].Routes,
					config.SignallingGateways[0].SGPs[0].Routes[0],
				)
			},
		},
		{
			name: "MTP Route without any SGP mapping",
			mutate: func(config *ASPConfig) {
				config.MTPRoutes = append(config.MTPRoutes, MTPRouteConfig{
					ID:                   "orphan",
					DestinationPointCode: 0x230000,
					Mask:                 16,
				})
			},
		},
		{
			name: "negative transfer flow cache entries",
			mutate: func(config *ASPConfig) {
				config.TransferFlowCacheEntries = -1
			},
		},
		{
			name: "negative MTP indication queue size",
			mutate: func(config *ASPConfig) {
				config.MTPIndicationQueueSize = -1
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := validASPConfig()
			test.mutate(config)
			endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: config})
			if endpoint != nil {
				t.Fatalf("NewEndpoint returned %#v", endpoint)
			}
			if !errors.Is(err, ErrInvalidASPConfig) {
				t.Fatalf("NewEndpoint error = %v, want ErrInvalidASPConfig", err)
			}
		})
	}
}

func TestEndpointRejectsASPPolicyForOtherRoles(t *testing.T) {
	for _, role := range []Role{RoleSGP, RoleIPSP} {
		endpoint, err := NewEndpoint(EndpointConfig{Role: role, ASP: validASPConfig()})
		if endpoint != nil {
			t.Fatalf("NewEndpoint(%v) returned %#v", role, endpoint)
		}
		if !errors.Is(err, ErrInvalidRoleConfiguration) {
			t.Fatalf("NewEndpoint(%v) error = %v, want ErrInvalidRoleConfiguration", role, err)
		}
	}
}

func TestAssociationConfigSnapshotsPeerSGPIdentity(t *testing.T) {
	identity := &SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}
	config := NewAssociationConfig(0, 0, 0, 0, 0, 0)
	config.PeerSGP = identity

	snapshot := snapshotAssociationConfig(config)
	identity.SignallingGateway = "changed"
	identity.SignallingGatewayProcess = "changed"
	config.PeerSGP = nil

	if snapshot.PeerSGP == nil || *snapshot.PeerSGP != (SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}) {
		t.Fatalf("PeerSGP snapshot = %#v", snapshot.PeerSGP)
	}
}

func TestSGPAssociationRejectsPeerSGPIdentity(t *testing.T) {
	config := NewAssociationConfig(0, 0, 0, 0, 0, 0)
	config.PeerSGP = &SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}
	if err := validateAssociationConfigForRole(RoleSGP, config); !errors.Is(err, ErrInvalidRoleConfiguration) {
		t.Fatalf("validateAssociationConfigForRole error = %v, want ErrInvalidRoleConfiguration", err)
	}
}

func TestASPEndpointValidatesAssociationSGPIdentityAndScope(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: validASPConfig()})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	valid := NewAssociationConfig(0x111111, 0x123456, 3, 0, 0, 1)
	valid.NetworkAppearance = params.NewNetworkAppearance(7)
	valid.RoutingContexts = params.NewRoutingContext(1)
	valid.PeerSGP = &SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}

	missing := snapshotAssociationConfig(valid)
	missing.PeerSGP = nil
	if err := endpoint.validateAssociationConfig(missing); !errors.Is(err, ErrMissingSGPIdentity) {
		t.Fatalf("missing SGP identity error = %v, want ErrMissingSGPIdentity", err)
	}
	unknown := snapshotAssociationConfig(valid)
	unknown.PeerSGP.SignallingGatewayProcess = "unknown"
	if err := endpoint.validateAssociationConfig(unknown); !errors.Is(err, ErrUnknownSGP) {
		t.Fatalf("unknown SGP error = %v, want ErrUnknownSGP", err)
	}
	mismatched := snapshotAssociationConfig(valid)
	mismatched.NetworkAppearance = params.NewNetworkAppearance(9)
	if err := endpoint.validateAssociationConfig(mismatched); !errors.Is(err, ErrSGPRouteScopeMismatch) {
		t.Fatalf("mismatched SGP scope error = %v, want ErrSGPRouteScopeMismatch", err)
	}
	if err := endpoint.validateAssociationConfig(valid); err != nil {
		t.Fatalf("valid SGP identity and scope: %v", err)
	}
}

func validASPConfig() *ASPConfig {
	return &ASPConfig{
		SignallingGatewaySelection: RouteSelectionLoadshare,
		MTPRoutes: []MTPRouteConfig{
			{
				ID:                    "sccp-a",
				DestinationPointCode:  0x120000,
				Mask:                  16,
				ServiceIndicators:     []uint8{3},
				OriginatingPointCodes: []uint32{0x111111},
			},
		},
		SignallingGateways: []SignallingGatewayConfig{
			{
				ID:           "sg-a",
				SGPSelection: RouteSelectionPrimaryBackup,
				SGPs: []SignallingGatewayProcessConfig{
					{
						ID: "sgp-a1",
						Routes: []SGPRoute{
							{
								MTPRoute: "sccp-a",
								AS: ASKey{
									NetworkAppearance:    7,
									NetworkAppearanceSet: true,
									RoutingContext:       1,
									RoutingContextSet:    true,
								},
							},
						},
					},
				},
			},
			{
				ID:           "sg-b",
				SGPSelection: RouteSelectionLoadshare,
				SGPs: []SignallingGatewayProcessConfig{
					{
						ID: "sgp-b1",
						Routes: []SGPRoute{
							{
								MTPRoute: "sccp-a",
								AS: ASKey{
									NetworkAppearance:    9,
									NetworkAppearanceSet: true,
									RoutingContext:       42,
									RoutingContextSet:    true,
								},
							},
						},
					},
				},
			},
		},
	}
}
