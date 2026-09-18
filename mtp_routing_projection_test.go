// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"errors"
	"testing"

	"github.com/gomaja/go-m3ua/messages/params"
)

// A route names provisioned path candidates rather than repeating a wire
// scope, so two routes may share one candidate and one route may reach several
// Signalling Gateways and Application Servers.
func TestASPRoutePathsAreSharedCandidates(t *testing.T) {
	config := validASPConfig()
	config.Routing.Paths = []MTPRoutePath{
		{ID: "via-sg-a", SignallingGateway: "sg-a", ApplicationServers: []RemoteASID{"as-core"}},
		{ID: "via-sg-b", SignallingGateway: "sg-b", ApplicationServers: []RemoteASID{"as-core"}},
	}
	config.Routing.MTPRoutes = []MTPRouteConfig{
		{
			ID:                   "sccp-a",
			DestinationPointCode: 0x120000,
			Mask:                 16,
			Paths:                []MTPRoutePathID{"via-sg-a", "via-sg-b"},
		},
		{
			ID:                   "sccp-b",
			DestinationPointCode: 0x130000,
			Mask:                 16,
			Paths:                []MTPRoutePathID{"via-sg-a"},
		},
	}

	snapshot, err := snapshotASPConfig(config)
	if err != nil {
		t.Fatalf("snapshotASPConfig: %v", err)
	}
	first, exists := snapshot.mtpRoute("sccp-a")
	if !exists {
		t.Fatal("MTP Route sccp-a was not compiled")
	}
	if len(first.gateways) != 2 {
		t.Fatalf("sccp-a gateways = %d, want 2", len(first.gateways))
	}
	second, exists := snapshot.mtpRoute("sccp-b")
	if !exists {
		t.Fatal("MTP Route sccp-b was not compiled")
	}
	if len(second.gateways) != 1 || second.gateways[0].id != "sg-a" {
		t.Fatalf("sccp-b gateways = %#v, want one sg-a", second.gateways)
	}
	if len(second.gateways[0].candidates) != 1 ||
		second.gateways[0].candidates[0].path != "via-sg-a" ||
		second.gateways[0].candidates[0].applicationServer != "as-core" {
		t.Fatalf("sccp-b candidates = %#v", second.gateways[0].candidates)
	}
}

// A destination no Signalling Gateway has reported on is not a reachable one.
// The default is to fail closed; AllowUnknownDestinations is the explicit
// opt-in, and it neither fabricates an availability report nor overrides one.
func TestMTPTransferFailsClosedOnUnknownDestinationState(t *testing.T) {
	const pointCode = uint32(0x123456)
	closed := validASPConfig()
	closed.Routing.SignallingGatewaySelection = RouteSelectionPrimaryBackup
	endpoint, _, captures := newUnreportedASPTransferFixture(t, closed)

	_, err := endpoint.MTPTransfer(MTPTransferRequest{
		ProtocolData: transferProtocolData(pointCode, 1, nil),
	})
	if !errors.Is(err, ErrDestinationStateUnknown) {
		t.Fatalf("unknown destination error = %v, want ErrDestinationStateUnknown", err)
	}
	var selection *MTPSelectionError
	if !errors.As(err, &selection) {
		t.Fatalf("error %v does not carry an MTPSelectionError", err)
	}
	if selection.MTPRoute != "sccp-a" {
		t.Fatalf("selection error names MTP Route %q, want sccp-a", selection.MTPRoute)
	}
	if len(selection.Rejections) != 2 {
		t.Fatalf("rejections = %#v, want one per candidate", selection.Rejections)
	}
	for _, rejection := range selection.Rejections {
		if rejection.Reason != MTPCandidateStateUnknown {
			t.Fatalf("rejection %#v, want MTPCandidateStateUnknown", rejection)
		}
	}
	for name, capture := range captures {
		if capture.count() != 0 {
			t.Fatalf("%s received %d messages for an unknown destination", name, capture.count())
		}
	}

	open := validASPConfig()
	open.Routing.SignallingGatewaySelection = RouteSelectionPrimaryBackup
	open.Routing.AllowUnknownDestinations = true
	permissive, _, permissiveCaptures := newUnreportedASPTransferFixture(t, open)
	if _, err := permissive.MTPTransfer(MTPTransferRequest{
		ProtocolData: transferProtocolData(pointCode, 1, nil),
	}); err != nil {
		t.Fatalf("MTPTransfer with AllowUnknownDestinations: %v", err)
	}
	if permissiveCaptures["sg-a/sgp-a1"].count() != 1 {
		t.Fatal("the opt-in did not let an unreported destination carry traffic")
	}
}

// The opt-in is not a licence to ignore what a peer did say: a reported
// unavailability still refuses the candidate that reported it.
func TestAllowUnknownDestinationsDoesNotOverrideAReportedState(t *testing.T) {
	const pointCode = uint32(0x123456)
	config := validASPConfig()
	config.Routing.SignallingGatewaySelection = RouteSelectionPrimaryBackup
	config.Routing.AllowUnknownDestinations = true
	useSignallingGateways(config, "sg-a")
	endpoint, associations, captures := newUnreportedASPTransferFixture(t, config)
	applyASPDUNA(t, associations["sg-a/sgp-a1"], 7, 1, pointCode, 0)

	_, err := endpoint.MTPTransfer(MTPTransferRequest{
		ProtocolData: transferProtocolData(pointCode, 1, nil),
	})
	if !errors.Is(err, ErrNoMTPRoute) {
		t.Fatalf("unavailable destination error = %v, want ErrNoMTPRoute", err)
	}
	var selection *MTPSelectionError
	if !errors.As(err, &selection) || len(selection.Rejections) != 1 ||
		selection.Rejections[0].Reason != MTPCandidateUnavailable {
		t.Fatalf("selection error = %v", err)
	}
	if captures["sg-a/sgp-a1"].count() != 0 {
		t.Fatal("an unavailable destination carried traffic under the unknown-state opt-in")
	}
}

// Each successful and failed target names exactly what it was: the path it came
// from, the Application Server, the wire scope, the Association, and the SSNM
// partition generation the availability decision was made under.
func TestMTPTransferResultNamesTheConcreteTargets(t *testing.T) {
	const pointCode = uint32(0x123456)
	config := validASPConfig()
	config.Routing.SignallingGatewaySelection = RouteSelectionPrimaryBackup
	useSignallingGateways(config, "sg-a")
	endpoint, associations, _ := newUnreportedASPTransferFixture(t, config)
	applyASPDAVA(t, associations["sg-a/sgp-a1"], 7, 1, pointCode, 0)

	result, err := endpoint.MTPTransfer(MTPTransferRequest{
		ProtocolData: transferProtocolData(pointCode, 1, []byte("payload")),
	})
	if err != nil {
		t.Fatalf("MTPTransfer: %v", err)
	}
	if len(result.SuccessfulPaths) != 1 {
		t.Fatalf("successful paths = %#v, want one", result.SuccessfulPaths)
	}
	target := result.SuccessfulPaths[0]
	want := MTPTransferPath{
		Path:              "sg-a-core",
		SGP:               SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"},
		ApplicationServer: "as-core",
		AS:                *staticASKey(7, 1),
		Association:       associations["sg-a/sgp-a1"].ID(),
		Epoch:             partitionEpoch(t, endpoint, "sg-a", "as-core"),
	}
	if target != want {
		t.Fatalf("successful path = %+v, want %+v", target, want)
	}
	if result.UserDataOctets != len("payload") {
		t.Fatalf("UserDataOctets = %d, want %d", result.UserDataOctets, len("payload"))
	}
}

func partitionEpoch(t *testing.T, endpoint *Endpoint, gateway SignallingGatewayID, as RemoteASID) uint64 {
	t.Helper()
	wanted := SSNMPartition{
		Kind:              SSNMCanonicalPartition,
		SignallingGateway: gateway,
		ApplicationServer: as,
	}
	for _, partition := range endpoint.SSNMKnowledge().Partitions {
		if partition.Partition == wanted {
			return partition.Epoch
		}
	}
	t.Fatalf("no SSNM partition for %q of %q", as, gateway)
	return 0
}

// twoApplicationServerASPConfig provisions one SGP serving two Application
// Servers and one path naming both, so the route has an ordered preference and
// a failback within a single SGP.
func twoApplicationServerASPConfig() *ASPConfig {
	return &ASPConfig{
		SignallingGateways: []SignallingGatewayConfig{{
			ID: "sg-a",
			SGPs: []SignallingGatewayProcessConfig{{
				ID: "sgp-a1",
				ApplicationServers: []RemoteASConfig{
					{ID: "as-core", ASKey: staticASKey(7, 1)},
					{ID: "as-backup", ASKey: staticASKey(7, 2)},
				},
			}},
		}},
		Routing: &ASPRoutingConfig{
			SignallingGatewaySelection: RouteSelectionPrimaryBackup,
			SignallingGatewayProcessSelection: map[SignallingGatewayID]RouteSelectionMode{
				"sg-a": RouteSelectionPrimaryBackup,
			},
			Paths: []MTPRoutePath{{
				ID:                 "sg-a-pair",
				SignallingGateway:  "sg-a",
				ApplicationServers: []RemoteASID{"as-core", "as-backup"},
			}},
			MTPRoutes: []MTPRouteConfig{{
				ID:                   "sccp-a",
				DestinationPointCode: 0x120000,
				Mask:                 16,
				Paths:                []MTPRoutePathID{"sg-a-pair"},
			}},
		},
	}
}

// attachMultiScopeASPAssociation attaches one ASP Association carrying several
// acknowledged Application Server scopes of one SGP.
func attachMultiScopeASPAssociation(
	t *testing.T,
	endpoint *Endpoint,
	identity SGPIdentity,
	networkAppearance uint32,
	routingContexts ...uint32,
) (*Association, *mtpTransferCapture) {
	t.Helper()
	association, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, routingContexts...)
	setInventoryNetworkAppearance(&association.cfg.ApplicationServers, params.NewNetworkAppearance(networkAppearance))
	association.cfg.PeerSGP = &identity
	association.noteRoutingContextsAcked(params.NewRoutingContext(routingContexts...))
	capture := &mtpTransferCapture{}
	association.dataWriter = capture.write
	if !endpoint.trackAssociation(association) {
		t.Fatal("failed to attach ASP Association")
	}
	t.Cleanup(func() { _ = association.Close() })
	return association, capture
}

// The candidates of one SGP are tried in the route's own order and each is
// judged on its own Application Server's retained state. One Application
// Server reported unavailable is not the SGP, the Signalling Gateway or the
// route going away.
func TestMTPTransferFailsBackToTheNextApplicationServerOfOneSGP(t *testing.T) {
	const pointCode = uint32(0x123456)
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: twoApplicationServerASPConfig()})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	identity := SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}
	association, capture := attachMultiScopeASPAssociation(t, endpoint, identity, 7, 1, 2)

	applyASPDAVA(t, association, 7, 1, pointCode, 0)
	applyASPDAVA(t, association, 7, 2, pointCode, 0)
	result, err := endpoint.MTPTransfer(MTPTransferRequest{
		ProtocolData: transferProtocolData(pointCode, 1, nil),
	})
	if err != nil {
		t.Fatalf("MTPTransfer with both Application Servers available: %v", err)
	}
	if len(result.SuccessfulPaths) != 1 || result.SuccessfulPaths[0].ApplicationServer != "as-core" {
		t.Fatalf("first selection = %#v, want as-core", result.SuccessfulPaths)
	}

	applyASPDUNA(t, association, 7, 1, pointCode, 0)
	result, err = endpoint.MTPTransfer(MTPTransferRequest{
		ProtocolData: transferProtocolData(pointCode, 1, nil),
	})
	if err != nil {
		t.Fatalf("MTPTransfer after the preferred Application Server went unavailable: %v", err)
	}
	if len(result.SuccessfulPaths) != 1 || result.SuccessfulPaths[0].ApplicationServer != "as-backup" {
		t.Fatalf("failback selection = %#v, want as-backup", result.SuccessfulPaths)
	}
	if capture.count() != 2 {
		t.Fatalf("captured %d messages, want 2", capture.count())
	}
	data, _ := capture.lastData(t)
	if data.RoutingContext == nil || len(data.RoutingContext.RoutingContexts()) != 1 ||
		data.RoutingContext.RoutingContexts()[0] != 2 {
		t.Fatalf("failback DATA Routing Context = %#v, want 2", data.RoutingContext)
	}

	// Recovery is reported, never inferred. The preferred Application Server
	// comes back only when its own DAVA arrives.
	applyASPDAVA(t, association, 7, 1, pointCode, 0)
	result, err = endpoint.MTPTransfer(MTPTransferRequest{
		ProtocolData: transferProtocolData(pointCode, 1, nil),
	})
	if err != nil {
		t.Fatalf("MTPTransfer after recovery: %v", err)
	}
	if len(result.SuccessfulPaths) != 1 || result.SuccessfulPaths[0].ApplicationServer != "as-core" {
		t.Fatalf("recovered selection = %#v, want as-core", result.SuccessfulPaths)
	}
}

// State retained before any request named the route decides that request, and
// it is retained for an Application Server the SGP bound dynamically, whose
// wire Routing Context exists only once RFC 4666 Section 4.4.1 registration
// assigned it.
func TestMTPTransferProjectsStateRetainedForADynamicallyBoundApplicationServer(t *testing.T) {
	const pointCode = uint32(0x123456)
	config := &ASPConfig{
		SignallingGateways: []SignallingGatewayConfig{{
			ID: "sg-a",
			SGPs: []SignallingGatewayProcessConfig{{
				ID: "sgp-a1",
				ApplicationServers: []RemoteASConfig{{
					ID: "as-dynamic",
					RoutingKey: &RoutingKey{Groups: []RoutingKeyGroup{{
						DestinationPointCode: 0x120000,
					}}},
				}},
			}},
		}},
		Routing: &ASPRoutingConfig{
			SignallingGatewaySelection: RouteSelectionPrimaryBackup,
			SignallingGatewayProcessSelection: map[SignallingGatewayID]RouteSelectionMode{
				"sg-a": RouteSelectionPrimaryBackup,
			},
			Paths: []MTPRoutePath{{
				ID:                 "sg-a-dynamic",
				SignallingGateway:  "sg-a",
				ApplicationServers: []RemoteASID{"as-dynamic"},
			}},
			MTPRoutes: []MTPRouteConfig{{
				ID:                   "sccp-a",
				DestinationPointCode: 0x120000,
				Mask:                 16,
				Paths:                []MTPRoutePathID{"sg-a-dynamic"},
			}},
		},
	}
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: config})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })

	association, _ := newTestConn(t, StateASPActive, RoleASP)
	setInventoryNetworkAppearance(&association.cfg.ApplicationServers, nil)
	setInventoryRoutingContexts(&association.cfg.ApplicationServers, nil)
	association.cfg.PeerSGP = &SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}
	capture := &mtpTransferCapture{}
	association.dataWriter = capture.write
	if !endpoint.trackAssociation(association) {
		t.Fatal("failed to attach ASP Association")
	}
	t.Cleanup(func() { _ = association.Close() })

	// The SGP assigns Routing Context 9 to the Application Server it binds
	// dynamically, and the ASP records the canonical identity behind it.
	assigned := ASKey{RoutingContext: 9, RoutingContextSet: true}
	association.addDynamicASKey(assigned, RoutingKey{}, false)
	association.noteCanonicalRemoteAS(9, "as-dynamic")
	association.noteRoutingContextsAcked(params.NewRoutingContext(9))
	association.syncSSNMBindings()

	applyASPDUNA(t, association, 0, 9, pointCode, 0)

	_, err = endpoint.MTPTransfer(MTPTransferRequest{
		ProtocolData: transferProtocolData(pointCode, 1, nil),
	})
	if !errors.Is(err, ErrNoMTPRoute) {
		t.Fatalf("MTPTransfer error = %v, want ErrNoMTPRoute", err)
	}
	var selection *MTPSelectionError
	if !errors.As(err, &selection) || len(selection.Rejections) != 1 ||
		selection.Rejections[0].Reason != MTPCandidateUnavailable ||
		selection.Rejections[0].ApplicationServer != "as-dynamic" {
		t.Fatalf("selection error = %v", err)
	}
	if capture.count() != 0 {
		t.Fatal("a destination reported unavailable through a dynamically bound Application Server carried traffic")
	}
}
