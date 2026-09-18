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

// Availability and congestion are separate statuses of one destination, as RFC
// 4666 Section 4.5.2.2 requires. Neither installs, clears or implies the
// other, and neither is inferred from a report about a different destination or
// a different Application Server.
func TestSelectionKeepsAvailabilityAndCongestionIndependent(t *testing.T) {
	const pointCode = uint32(0x123456)
	config := validASPConfig()
	config.Routing.SignallingGatewaySelection = RouteSelectionPrimaryBackup
	useSignallingGateways(config, "sg-a")
	endpoint, associations, captures := newUnreportedASPTransferFixture(t, config)
	association := associations["sg-a/sgp-a1"]
	request := MTPTransferRequest{ProtocolData: transferProtocolData(pointCode, 1, nil)}

	// A congestion report is not an availability report: the destination is
	// still one nobody has said is reachable.
	applyASPSCON(t, association, 7, 1, pointCode, 0, params.NewCongestionIndications(1))
	if _, err := endpoint.MTPTransfer(request); !errors.Is(err, ErrDestinationStateUnknown) {
		t.Fatalf("congestion-only error = %v, want ErrDestinationStateUnknown", err)
	}

	// An availability report about a different destination is not one about
	// this destination.
	applyASPDAVA(t, association, 7, 1, 0x654321, 0)
	if _, err := endpoint.MTPTransfer(request); !errors.Is(err, ErrDestinationStateUnknown) {
		t.Fatalf("unrelated-destination error = %v, want ErrDestinationStateUnknown", err)
	}

	applyASPDAVA(t, association, 7, 1, pointCode, 0)
	if _, err := endpoint.MTPTransfer(request); err != nil {
		t.Fatalf("MTPTransfer once the destination was reported available: %v", err)
	}
	if captures["sg-a/sgp-a1"].count() != 1 {
		t.Fatalf("captured %d messages, want 1", captures["sg-a/sgp-a1"].count())
	}

	// The congestion the SCON reported survived the availability reports: it
	// is a separate dimension and no availability message touched it.
	refused := validASPConfig()
	refused.Routing.SignallingGatewaySelection = RouteSelectionPrimaryBackup
	useSignallingGateways(refused, "sg-a")
	refused.Routing.CongestionPolicy = func(_, level uint8, levelSet bool) bool {
		return !levelSet || level == 0
	}
	strict, strictAssociations, strictCaptures := newUnreportedASPTransferFixture(t, refused)
	applyASPDAVA(t, strictAssociations["sg-a/sgp-a1"], 7, 1, pointCode, 0)
	applyASPSCON(t, strictAssociations["sg-a/sgp-a1"], 7, 1, pointCode, 0, params.NewCongestionIndications(1))
	applyASPDAVA(t, strictAssociations["sg-a/sgp-a1"], 7, 1, pointCode, 0)
	_, err := strict.MTPTransfer(request)
	var selection *MTPSelectionError
	if !errors.As(err, &selection) || len(selection.Rejections) != 1 ||
		selection.Rejections[0].Reason != MTPCandidateCongested {
		t.Fatalf("congested candidate error = %v, want a congestion rejection", err)
	}
	if strictCaptures["sg-a/sgp-a1"].count() != 0 {
		t.Fatal("a congestion policy that refused the level still carried traffic")
	}
}

// A restricted destination is one the peer says it can still reach. It carries
// traffic when it is what the route has, and loses only to a candidate the peer
// called available.
func TestSelectionUsesARestrictedCandidateWhenItIsTheOnlyOne(t *testing.T) {
	const pointCode = uint32(0x123456)
	config := validASPConfig()
	config.Routing.SignallingGatewaySelection = RouteSelectionPrimaryBackup
	useSignallingGateways(config, "sg-a")
	endpoint, associations, captures := newUnreportedASPTransferFixture(t, config)
	applyASPDRST(t, associations["sg-a/sgp-a1"], 7, 1, pointCode, 0)

	if _, err := endpoint.MTPTransfer(MTPTransferRequest{
		ProtocolData: transferProtocolData(pointCode, 1, nil),
	}); err != nil {
		t.Fatalf("MTPTransfer to a restricted destination: %v", err)
	}
	if captures["sg-a/sgp-a1"].count() != 1 {
		t.Fatal("a restricted destination carried no traffic")
	}
}

// One Signalling Gateway reporting a destination unavailable is that Signalling
// Gateway's report. The alternative keeps carrying the destination, and the
// first one comes back when it says so.
func TestSingleSignallingGatewayFailureDoesNotFailEveryPath(t *testing.T) {
	const pointCode = uint32(0x123456)
	config := validASPConfig()
	config.Routing.SignallingGatewaySelection = RouteSelectionPrimaryBackup
	endpoint, associations, captures := newASPTransferFixture(t, config)
	request := MTPTransferRequest{ProtocolData: transferProtocolData(pointCode, 1, nil)}

	applyASPDUNA(t, associations["sg-a/sgp-a1"], 7, 1, pointCode, 0)
	result, err := endpoint.MTPTransfer(request)
	if err != nil {
		t.Fatalf("MTPTransfer with the primary Signalling Gateway unavailable: %v", err)
	}
	if len(result.SuccessfulPaths) != 1 || result.SuccessfulPaths[0].Path != "sg-b-core" {
		t.Fatalf("selection = %#v, want the sg-b path", result.SuccessfulPaths)
	}
	if captures["sg-a/sgp-a1"].count() != 0 || captures["sg-b/sgp-b1"].count() != 1 {
		t.Fatalf("counts = sg-a:%d sg-b:%d, want 0 and 1",
			captures["sg-a/sgp-a1"].count(), captures["sg-b/sgp-b1"].count())
	}

	// The alternative going away does not restore the first: its own report
	// still stands and nothing inferred a recovery from the loss of a sibling.
	if err := associations["sg-b/sgp-b1"].Close(); err != nil {
		t.Fatalf("close the alternative Association: %v", err)
	}
	if _, err := endpoint.MTPTransfer(request); !errors.Is(err, ErrNoMTPRoute) {
		t.Fatalf("MTPTransfer with one path unavailable and the other gone = %v, want ErrNoMTPRoute", err)
	}
	if captures["sg-a/sgp-a1"].count() != 0 {
		t.Fatal("the unavailable Signalling Gateway carried traffic after its alternative went away")
	}

	applyASPDAVA(t, associations["sg-a/sgp-a1"], 7, 1, pointCode, 0)
	if _, err := endpoint.MTPTransfer(request); err != nil {
		t.Fatalf("MTPTransfer after the primary reported recovery: %v", err)
	}
	if captures["sg-a/sgp-a1"].count() != 1 {
		t.Fatal("the recovered Signalling Gateway carried no traffic")
	}
}

// Broadcast reaches every eligible SGP exactly once, and exactly one
// Application Server per SGP even where several are provisioned and eligible.
func TestBroadcastSelectsOneApplicationServerPerSGP(t *testing.T) {
	const pointCode = uint32(0x123456)
	config := twoApplicationServerASPConfig()
	config.Routing.SignallingGatewaySelection = RouteSelectionBroadcast
	config.Routing.SignallingGatewayProcessSelection["sg-a"] = RouteSelectionBroadcast
	config.SignallingGateways[0].SGPs = append(config.SignallingGateways[0].SGPs,
		SignallingGatewayProcessConfig{
			ID: "sgp-a2",
			ApplicationServers: []RemoteASConfig{
				{ID: "as-core", ASKey: staticASKey(7, 1)},
				{ID: "as-backup", ASKey: staticASKey(7, 2)},
			},
		})
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: config})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })

	first, firstCapture := attachMultiScopeASPAssociation(t, endpoint,
		SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}, 7, 1, 2)
	second, secondCapture := attachMultiScopeASPAssociation(t, endpoint,
		SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a2"}, 7, 1, 2)
	for _, association := range []*Association{first, second} {
		applyASPDAVA(t, association, 7, 1, pointCode, 0)
		applyASPDAVA(t, association, 7, 2, pointCode, 0)
	}

	result, err := endpoint.MTPTransfer(MTPTransferRequest{
		ProtocolData: transferProtocolData(pointCode, 1, nil),
	})
	if err != nil {
		t.Fatalf("broadcast MTPTransfer: %v", err)
	}
	if len(result.SuccessfulPaths) != 2 {
		t.Fatalf("broadcast targets = %#v, want one per SGP", result.SuccessfulPaths)
	}
	seen := make(map[SGPIdentity]struct{}, 2)
	for _, target := range result.SuccessfulPaths {
		if _, duplicate := seen[target.SGP]; duplicate {
			t.Fatalf("broadcast selected SGP %+v twice", target.SGP)
		}
		seen[target.SGP] = struct{}{}
		if target.ApplicationServer != "as-core" {
			t.Fatalf("broadcast target %+v, want the first eligible Application Server", target)
		}
	}
	if firstCapture.count() != 1 || secondCapture.count() != 1 {
		t.Fatalf("broadcast counts = sgp-a1:%d sgp-a2:%d, want 1 each",
			firstCapture.count(), secondCapture.count())
	}
}

// Targets are frozen when the request is admitted. A target that refuses the
// write is reported as it is; nothing else is tried for the same request,
// because a failed write does not prove the peer received no DATA and the next
// Application Server of the same SGP would risk a duplicate.
func TestFailedTargetIsNotRetriedThroughAnotherApplicationServer(t *testing.T) {
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
	capture.writeErr = errors.New("transport refused the write")

	result, err := endpoint.MTPTransfer(MTPTransferRequest{
		ProtocolData: transferProtocolData(pointCode, 1, []byte("x")),
	})
	if len(result.SuccessfulPaths) != 0 {
		t.Fatalf("failed transfer reported successes %#v", result.SuccessfulPaths)
	}
	var transferErr *MTPTransferError
	if !errors.As(err, &transferErr) || len(transferErr.Failures) != 1 {
		t.Fatalf("transfer error = %v, want one failure", err)
	}
	failure := transferErr.Failures[0]
	if failure.Target.ApplicationServer != "as-core" || failure.Target.Path != "sg-a-pair" ||
		failure.Target.SGP != identity || failure.Target.AS != *staticASKey(7, 1) ||
		failure.Target.Association != association.ID() ||
		failure.Target.Epoch != partitionEpoch(t, endpoint, "sg-a", "as-core") {
		t.Fatalf("failure target = %+v", failure.Target)
	}
	var writeErr *DataWriteError
	if !errors.As(failure.Err, &writeErr) || writeErr.Outcome != DataSendIndeterminate {
		t.Fatalf("failure cause = %v, want an indeterminate DataWriteError", failure.Err)
	}
	if capture.count() != 0 {
		t.Fatalf("captured %d messages, want none", capture.count())
	}
}

// Looking a route up is a read. It never audits, never probes and never
// resends: RFC 4666 Section 4.5.2 scheduling of DAUD belongs to the
// application, which owns when and how often it asks.
func TestRouteLookupSendsNothingOfItsOwn(t *testing.T) {
	const pointCode = uint32(0x123456)
	config := validASPConfig()
	config.Routing.SignallingGatewaySelection = RouteSelectionPrimaryBackup
	useSignallingGateways(config, "sg-a")
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: config})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	association, signals := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
	setInventoryNetworkAppearance(&association.cfg.ApplicationServers, params.NewNetworkAppearance(7))
	association.cfg.PeerSGP = &SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}
	association.noteRoutingContextsAcked(params.NewRoutingContext(1))
	capture := &mtpTransferCapture{}
	association.dataWriter = capture.write
	if !endpoint.trackAssociation(association) {
		t.Fatal("failed to attach ASP Association")
	}
	t.Cleanup(func() { _ = association.Close() })

	*signals = nil
	request := MTPTransferRequest{ProtocolData: transferProtocolData(pointCode, 1, nil)}
	if _, err := endpoint.MTPTransfer(request); !errors.Is(err, ErrDestinationStateUnknown) {
		t.Fatalf("unknown-state error = %v, want ErrDestinationStateUnknown", err)
	}
	applyASPDUNA(t, association, 7, 1, pointCode, 0)
	if _, err := endpoint.MTPTransfer(request); !errors.Is(err, ErrNoMTPRoute) {
		t.Fatalf("unavailable-destination error = %v, want ErrNoMTPRoute", err)
	}
	if len(*signals) != 0 {
		t.Fatalf("route lookup emitted %d message(s) of its own: %v", len(*signals), *signals)
	}
	if capture.count() != 0 {
		t.Fatalf("route lookup emitted %d DATA message(s)", capture.count())
	}
}

// Two routes naming one path are two routes, not two inventories. They select
// the same Association and the same Application Server membership, and neither
// route's use of the path changes anything the other sees.
func TestSharedRoutePathPreservesAssociationAndMembership(t *testing.T) {
	config := twoApplicationServerASPConfig()
	config.Routing.MTPRoutes = []MTPRouteConfig{
		{
			ID: "sccp", DestinationPointCode: 0x120000, Mask: 16,
			ServiceIndicators: []uint8{params.ServiceIndSCCP},
			Paths:             []MTPRoutePathID{"sg-a-pair"},
		},
		{
			ID: "isup", DestinationPointCode: 0x120000, Mask: 16,
			ServiceIndicators: []uint8{params.ServiceIndISUP},
			Paths:             []MTPRoutePathID{"sg-a-pair"},
		},
	}
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: config})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	identity := SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}
	association, capture := attachMultiScopeASPAssociation(t, endpoint, identity, 7, 1, 2)
	applyASPDAVA(t, association, 7, 1, 0x120000, 16)
	applyASPDAVA(t, association, 7, 2, 0x120000, 16)

	targets := make([]MTPTransferPath, 0, 2)
	for _, serviceIndicator := range []uint8{params.ServiceIndSCCP, params.ServiceIndISUP} {
		result, err := endpoint.MTPTransfer(MTPTransferRequest{
			ProtocolData: params.NewProtocolDataPayload(
				0x111111, 0x123456, serviceIndicator, 0, 0, 1, []byte("x")),
		})
		if err != nil {
			t.Fatalf("MTPTransfer(SI %d): %v", serviceIndicator, err)
		}
		if len(result.SuccessfulPaths) != 1 {
			t.Fatalf("MTPTransfer(SI %d) targets = %#v", serviceIndicator, result.SuccessfulPaths)
		}
		targets = append(targets, result.SuccessfulPaths[0])
	}
	if targets[0].Association != targets[1].Association ||
		targets[0].ApplicationServer != targets[1].ApplicationServer ||
		targets[0].AS != targets[1].AS || targets[0].Path != targets[1].Path {
		t.Fatalf("the shared path selected %+v and %+v", targets[0], targets[1])
	}
	if capture.count() != 2 {
		t.Fatalf("captured %d messages, want 2", capture.count())
	}
}

// Application-managed routing is not a degraded mode of the route inventory.
// An Endpoint with no Routing owns no outbound selection at all: MTPTransfer
// says so, and direct per-message WriteData is unaffected by it.
func TestApplicationManagedDirectIORemainsIndependent(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: &ASPConfig{
		SignallingGateways: []SignallingGatewayConfig{{
			ID: "sg-a",
			SGPs: []SignallingGatewayProcessConfig{{
				ID:                 "sgp-a1",
				ApplicationServers: []RemoteASConfig{{ID: "as-core", ASKey: staticASKey(7, 1)}},
			}},
		}},
	}})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	identity := SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}
	association, capture := attachMultiScopeASPAssociation(t, endpoint, identity, 7, 1)

	if _, err := endpoint.MTPTransfer(MTPTransferRequest{
		ProtocolData: transferProtocolData(0x123456, 1, nil),
	}); !errors.Is(err, ErrRoutingNotConfigured) {
		t.Fatalf("MTPTransfer without a route inventory = %v, want ErrRoutingNotConfigured", err)
	}

	// Nothing has been reported about the destination, which would refuse a
	// library-managed selection and has no bearing on a direct send.
	if _, err := association.WriteData(DataRequest{
		AS:           *staticASKey(7, 1),
		ProtocolData: *transferProtocolData(0x123456, 1, []byte("direct")),
	}); err != nil {
		t.Fatalf("WriteData: %v", err)
	}
	if capture.count() != 1 {
		t.Fatalf("captured %d messages, want 1", capture.count())
	}
}
