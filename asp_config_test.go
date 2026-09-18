// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
)

func TestNewEndpointSnapshotsASPRoutingPolicy(t *testing.T) {
	config := validASPConfig()
	config.SignallingGateways[0].SGPs[0].ApplicationServers = append(
		config.SignallingGateways[0].SGPs[0].ApplicationServers,
		RemoteASConfig{ID: "as-dynamic", RoutingKey: &RoutingKey{
			Groups: []RoutingKeyGroup{{
				DestinationPointCode: 0x330000,
				ServiceIndicators:    []uint8{3},
			}},
		}},
	)
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: config})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })

	// Every reachable part of the caller's configuration is mutated after the
	// Endpoint exists, including the memory its pointers and maps reach.
	config.Routing.SignallingGatewaySelection = RouteSelectionBroadcast
	config.Routing.SignallingGatewayProcessSelection["sg-a"] = RouteSelectionBroadcast
	config.Routing.MTPRoutes[0].ID = "changed"
	config.Routing.MTPRoutes[0].ServiceIndicators[0] = 0xff
	config.Routing.MTPRoutes[0].OriginatingPointCodes[0] = 0xffffff
	config.Routing.Routes[0].MTPRoute = "changed"
	config.Routing.Routes[0].AS.ApplicationServer = "changed"
	config.SignallingGateways[0].ID = "changed"
	config.SignallingGateways[0].SGPs[0].ID = "changed"
	config.SignallingGateways[0].SGPs[0].ApplicationServers[0].ID = "changed"
	*config.SignallingGateways[0].SGPs[0].ApplicationServers[0].ASKey = ASKey{}
	config.SignallingGateways[0].SGPs[0].ApplicationServers[1].RoutingKey.Groups[0].DestinationPointCode = 0xffffff
	config.SignallingGateways[0].SGPs[0].ApplicationServers[1].RoutingKey.Groups[0].ServiceIndicators[0] = 0xff

	snapshot := endpoint.aspRoutes.config
	if !snapshot.routingConfigured {
		t.Fatal("routing inventory was not recorded")
	}
	if snapshot.signallingGatewaySelection != RouteSelectionLoadshare {
		t.Fatalf("SignallingGatewaySelection = %v, want loadshare", snapshot.signallingGatewaySelection)
	}
	if snapshot.maxAffectedPointCodesPerSSNM != DefaultMaxAffectedPointCodesPerSSNM ||
		snapshot.maxSSNMStateRecordsPerRoute != DefaultMaxSSNMStateRecordsPerRoute ||
		snapshot.maxSSNMStateRecordsPerSignallingGateway != DefaultMaxSSNMStateRecords/2 ||
		snapshot.maxSSNMStateRecords != DefaultMaxSSNMStateRecords ||
		snapshot.maxSSNMDestinationRecords != DefaultMaxSSNMDestinationRecords {
		t.Fatalf("SSNM budgets = APC:%d route:%d SG:%d Endpoint:%d destination:%d",
			snapshot.maxAffectedPointCodesPerSSNM,
			snapshot.maxSSNMStateRecordsPerRoute,
			snapshot.maxSSNMStateRecordsPerSignallingGateway,
			snapshot.maxSSNMStateRecords,
			snapshot.maxSSNMDestinationRecords)
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
		t.Fatalf("SGP selection = %v, want primary/backup", got)
	}
	if got := snapshot.signallingGateways[0].sgps[0].id; got != "sgp-a1" {
		t.Fatalf("SGP ID = %q, want sgp-a1", got)
	}
	identity := SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}
	key, resolved := snapshot.staticASKeyFor(identity, "as-core")
	if !resolved || key != *staticASKey(7, 1) {
		t.Fatalf("as-core binding = %+v resolved=%v", key, resolved)
	}
	dynamic, served := snapshot.remoteASFor(identity, "as-dynamic")
	if !served || !dynamic.routingKeySet {
		t.Fatalf("as-dynamic binding = %+v served=%v", dynamic, served)
	}
	if got := dynamic.routingKey.Groups[0].DestinationPointCode; got != 0x330000 {
		t.Fatalf("dynamic Destination Point Code = %#x, want %#x", got, 0x330000)
	}
	if got := dynamic.routingKey.Groups[0].ServiceIndicators[0]; got != 3 {
		t.Fatalf("dynamic Service Indicator = %d, want 3", got)
	}
	sgp := snapshot.sgpByIdentity[identity]
	route, routed := aspSGPRouteForMTPRoute(sgp, "sccp-a")
	if !routed || route.applicationServer != "as-core" || route.as != *staticASKey(7, 1) {
		t.Fatalf("SGP route = %#v routed=%v", route, routed)
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
				config.Routing.SignallingGatewaySelection = RouteSelectionMode(0xff)
			},
		},
		{
			name: "no MTP Routes",
			mutate: func(config *ASPConfig) {
				config.Routing.MTPRoutes = nil
			},
		},
		{
			name: "empty MTP Route ID",
			mutate: func(config *ASPConfig) {
				config.Routing.MTPRoutes[0].ID = ""
			},
		},
		{
			name: "duplicate MTP Route ID",
			mutate: func(config *ASPConfig) {
				config.Routing.MTPRoutes = append(config.Routing.MTPRoutes, config.Routing.MTPRoutes[0])
			},
		},
		{
			name: "MTP Route point code above 24 bits",
			mutate: func(config *ASPConfig) {
				config.Routing.MTPRoutes[0].DestinationPointCode = 0x1000000
			},
		},
		{
			name: "MTP Route mask above 24 bits",
			mutate: func(config *ASPConfig) {
				config.Routing.MTPRoutes[0].Mask = 25
			},
		},
		{
			name: "MTP Route point code is not aligned to mask",
			mutate: func(config *ASPConfig) {
				config.Routing.MTPRoutes[0].DestinationPointCode = 0x123456
				config.Routing.MTPRoutes[0].Mask = 8
			},
		},
		{
			name: "MTP Route OPC above 24 bits",
			mutate: func(config *ASPConfig) {
				config.Routing.MTPRoutes[0].OriginatingPointCodes[0] = 0x1000000
			},
		},
		{
			name: "duplicate MTP Route SI",
			mutate: func(config *ASPConfig) {
				config.Routing.MTPRoutes[0].ServiceIndicators = []uint8{3, 3}
			},
		},
		{
			name: "duplicate MTP Route OPC",
			mutate: func(config *ASPConfig) {
				config.Routing.MTPRoutes[0].OriginatingPointCodes = []uint32{1, 1}
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
				config.Routing.SignallingGatewayProcessSelection["sg-a"] = RouteSelectionMode(0xff)
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
			name: "SGP without Application Servers",
			mutate: func(config *ASPConfig) {
				config.SignallingGateways[0].SGPs[0].ApplicationServers = nil
			},
		},
		{
			name: "route binding references unknown MTP Route",
			mutate: func(config *ASPConfig) {
				config.Routing.Routes[0].MTPRoute = "unknown"
			},
		},
		{
			name: "duplicate route binding",
			mutate: func(config *ASPConfig) {
				config.Routing.Routes = append(config.Routing.Routes, config.Routing.Routes[0])
			},
		},
		{
			name: "Application Server has absent Network Appearance with a value",
			mutate: func(config *ASPConfig) {
				config.SignallingGateways[0].SGPs[0].ApplicationServers[0].ASKey.NetworkAppearanceSet = false
			},
		},
		{
			name: "Application Server has absent Routing Context with a value",
			mutate: func(config *ASPConfig) {
				config.SignallingGateways[0].SGPs[0].ApplicationServers[0].ASKey.RoutingContextSet = false
			},
		},
		{
			name: "MTP Route without any Application Server binding",
			mutate: func(config *ASPConfig) {
				config.Routing.MTPRoutes = append(config.Routing.MTPRoutes, MTPRouteConfig{
					ID:                   "orphan",
					DestinationPointCode: 0x230000,
					Mask:                 16,
				})
			},
		},
		{
			name: "negative transfer flow cache entries",
			mutate: func(config *ASPConfig) {
				config.Routing.TransferFlowCacheEntries = -1
			},
		},
		{
			name: "negative MTP indication queue size",
			mutate: func(config *ASPConfig) {
				config.MTPIndicationQueueSize = -1
			},
		},
		{
			name: "negative Affected Point Codes per SSNM",
			mutate: func(config *ASPConfig) {
				config.MaxAffectedPointCodesPerSSNM = -1
			},
		},
		{
			name: "negative SSNM state records per route",
			mutate: func(config *ASPConfig) {
				config.MaxSSNMStateRecordsPerRoute = -1
			},
		},
		{
			name: "negative SSNM state records per Signalling Gateway",
			mutate: func(config *ASPConfig) {
				config.MaxSSNMStateRecordsPerSignallingGateway = -1
			},
		},
		{
			name: "negative SSNM state records",
			mutate: func(config *ASPConfig) {
				config.MaxSSNMStateRecords = -1
			},
		},
		{
			name: "negative SSNM destination records",
			mutate: func(config *ASPConfig) {
				config.MaxSSNMDestinationRecords = -1
			},
		},
		{
			name: "SSNM Endpoint budget cannot reserve every Signalling Gateway",
			mutate: func(config *ASPConfig) {
				config.MaxSSNMStateRecords = 1
			},
		},
		{
			name: "SSNM Signalling Gateway reservations exceed Endpoint budget",
			mutate: func(config *ASPConfig) {
				config.MaxSSNMStateRecords = 2
				config.MaxSSNMStateRecordsPerSignallingGateway = 2
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
	config := NewAssociationConfig()
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
	config := NewAssociationConfig()
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
	valid := NewAssociationConfig()
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
		Routing: &ASPRoutingConfig{
			SignallingGatewaySelection: RouteSelectionLoadshare,
			SignallingGatewayProcessSelection: map[SignallingGatewayID]RouteSelectionMode{
				"sg-a": RouteSelectionPrimaryBackup,
				"sg-b": RouteSelectionLoadshare,
			},
			MTPRoutes: []MTPRouteConfig{
				{
					ID:                    "sccp-a",
					DestinationPointCode:  0x120000,
					Mask:                  16,
					ServiceIndicators:     []uint8{3},
					OriginatingPointCodes: []uint32{0x111111},
				},
			},
			Routes: []MTPRouteBinding{
				{MTPRoute: "sccp-a", AS: SGASKey{SignallingGateway: "sg-a", ApplicationServer: "as-core"}},
				{MTPRoute: "sccp-a", AS: SGASKey{SignallingGateway: "sg-b", ApplicationServer: "as-core"}},
			},
		},
	}
}

// useSignallingGateways narrows the fixture to the named Signalling Gateways,
// dropping the route bindings and SGP selection modes of the others.
func useSignallingGateways(config *ASPConfig, ids ...SignallingGatewayID) {
	keep := make(map[SignallingGatewayID]struct{}, len(ids))
	for _, id := range ids {
		keep[id] = struct{}{}
	}
	gateways := make([]SignallingGatewayConfig, 0, len(ids))
	for _, gateway := range config.SignallingGateways {
		if _, wanted := keep[gateway.ID]; wanted {
			gateways = append(gateways, gateway)
		}
	}
	config.SignallingGateways = gateways
	if config.Routing == nil {
		return
	}
	routes := make([]MTPRouteBinding, 0, len(config.Routing.Routes))
	for _, binding := range config.Routing.Routes {
		if _, wanted := keep[binding.AS.SignallingGateway]; wanted {
			routes = append(routes, binding)
		}
	}
	config.Routing.Routes = routes
	for id := range config.Routing.SignallingGatewayProcessSelection {
		if _, wanted := keep[id]; !wanted {
			delete(config.Routing.SignallingGatewayProcessSelection, id)
		}
	}
}

// setSGPSelection sets the SGP selection mode of the named Signalling
// Gateways, or of every provisioned one when none is named.
func setSGPSelection(config *ASPConfig, mode RouteSelectionMode, ids ...SignallingGatewayID) {
	if len(ids) == 0 {
		for _, gateway := range config.SignallingGateways {
			ids = append(ids, gateway.ID)
		}
	}
	for _, id := range ids {
		config.Routing.SignallingGatewayProcessSelection[id] = mode
	}
}

// bindMTPRouteToEveryGateway carries one MTP Route over the as-core
// Application Server of every provisioned Signalling Gateway.
func bindMTPRouteToEveryGateway(config *ASPConfig, mtpRoute MTPRouteID) {
	for _, gateway := range config.SignallingGateways {
		config.Routing.Routes = append(config.Routing.Routes, MTPRouteBinding{
			MTPRoute: mtpRoute,
			AS:       SGASKey{SignallingGateway: gateway.ID, ApplicationServer: "as-core"},
		})
	}
}

// The congestion policy is the one callback an application hands to the ASP
// inventory. NewEndpoint takes the function value it was given and nothing
// else: the Endpoint neither reads the caller's ASPRoutingConfig again at
// transfer time, nor calls the policy while holding the route state it
// protects, so a policy may ask the Endpoint what it knows before answering.
func TestEndpointOwnsTheASPCongestionPolicyItWasGiven(t *testing.T) {
	config := validASPConfig()
	useSignallingGateways(config, "sg-a")

	var endpoint *Endpoint
	var provided, replaced, reentrant atomic.Int64
	config.Routing.CongestionPolicy = func(uint8, uint8, bool) bool {
		provided.Add(1)
		// RFC 4666 Appendix A.2.2 route selection is the Endpoint's, so a
		// policy that consults the Endpoint's own view must not be blocked by
		// it. The probe runs on its own goroutine and is abandoned rather than
		// waited on, so a policy invoked under the route lock records a missed
		// probe here instead of deadlocking the whole package.
		if endpoint != nil {
			reached := make(chan struct{})
			go func() {
				defer close(reached)
				_ = endpoint.MTPDestinationStatuses()
			}()
			select {
			case <-reached:
				reentrant.Add(1)
			case <-time.After(time.Second):
			}
		}
		return true
	}

	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: config})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })

	// Repointing the caller's field afterwards is the mutation a retained
	// configuration would follow.
	config.Routing.CongestionPolicy = func(uint8, uint8, bool) bool {
		replaced.Add(1)
		return false
	}

	association, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
	t.Cleanup(func() { _ = association.Close() })
	association.cfg.NetworkAppearance = params.NewNetworkAppearance(7)
	association.cfg.PeerSGP = &SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}
	association.noteRoutingContextsAcked(params.NewRoutingContext(1))
	capture := &mtpTransferCapture{}
	association.dataWriter = capture.write
	if !endpoint.trackAssociation(association) {
		t.Fatal("provisioned Association was not attached")
	}
	if got := provided.Load() + replaced.Load(); got != 0 {
		t.Fatalf("building the inventory evaluated the congestion policy %d times", got)
	}

	if _, err := endpoint.MTPTransfer(MTPTransferRequest{
		ProtocolData: params.NewProtocolDataPayload(
			0x111111, 0x123456, params.ServiceIndSCCP, 0, 0, 1, []byte("x")),
	}); err != nil {
		t.Fatalf("MTPTransfer: %v", err)
	}

	// One evaluation for the unknown level and one for each of levels 1 to 3.
	if got := provided.Load(); got != 4 {
		t.Fatalf("the configured congestion policy was evaluated %d times, want 4", got)
	}
	if got := replaced.Load(); got != 0 {
		t.Fatalf("the Endpoint followed the caller's field to a replacement policy %d times", got)
	}
	if got := reentrant.Load(); got != 4 {
		t.Fatalf("the congestion policy reached the Endpoint %d of 4 times: it is "+
			"evaluated while the route state it consults is locked", got)
	}
	if capture.count() != 1 {
		t.Fatalf("the transfer carried %d messages, want 1", capture.count())
	}
}
