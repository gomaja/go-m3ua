// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
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
				ApplicationServers: []RemoteASConfig{
					{
						ID: "as-dynamic",
						RoutingKey: &RoutingKey{Groups: []RoutingKeyGroup{{
							DestinationPointCode: 0x120000,
						}}},
					},
					{
						ID: "as-other",
						RoutingKey: &RoutingKey{Groups: []RoutingKeyGroup{{
							DestinationPointCode: 0x130000,
						}}},
					},
				},
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

	// The SGP assigns Routing Context 8 to one dynamically bound Application
	// Server and 9 to the other, and the ASP records the canonical identity
	// behind each. The path names only the second, so a report scoped to the
	// first is another Application Server's business.
	for _, assigned := range []struct {
		routingContext    uint32
		applicationServer RemoteASID
	}{{8, "as-other"}, {9, "as-dynamic"}, {11, "as-dynamic"}} {
		key := ASKey{RoutingContext: assigned.routingContext, RoutingContextSet: true}
		association.addDynamicASKey(key, RoutingKey{}, false)
		association.noteCanonicalRemoteAS(assigned.routingContext, assigned.applicationServer)
	}
	association.noteRoutingContextsAcked(params.NewRoutingContext(8, 9, 11))
	association.syncSSNMBindings()

	// Routing Context 8 belongs to the Application Server the path does not
	// name, so making it unavailable must leave the route alone. Contexts 9 and
	// 11 are two registrations of the Application Server the path does name; the
	// lower one is chosen, so the choice does not depend on map ordering.
	applyASPDUNA(t, association, 0, 8, pointCode, 0)
	applyASPDAVA(t, association, 0, 9, pointCode, 0)
	applyASPDAVA(t, association, 0, 11, pointCode, 0)
	result, err := endpoint.MTPTransfer(MTPTransferRequest{
		ProtocolData: transferProtocolData(pointCode, 1, nil),
	})
	if err != nil {
		t.Fatalf("MTPTransfer with the named Application Server available: %v", err)
	}
	if len(result.SuccessfulPaths) != 1 ||
		result.SuccessfulPaths[0].ApplicationServer != "as-dynamic" ||
		result.SuccessfulPaths[0].AS != (ASKey{RoutingContext: 9, RoutingContextSet: true}) {
		t.Fatalf("selection = %#v, want as-dynamic in Routing Context 9", result.SuccessfulPaths)
	}
	if capture.count() != 1 {
		t.Fatalf("captured %d messages, want 1", capture.count())
	}

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
	if capture.count() != 1 {
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
// resends. RFC 4666 Section 4.5.3 makes auditing something "an ASP may
// optionally initiate", so when and how often a DAUD goes out belongs to the
// application.
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

// A refused configuration says which path, which Application Server and which
// Signalling Gateway made it unusable. Each rejection has its own reason: a
// configuration refused by the wrong check is a diagnostic that sends the
// reader to the wrong line.
func TestASPRoutePathValidationNamesTheFault(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*ASPConfig)
		names  []string
	}{
		{
			name:   "unprovisioned Signalling Gateway",
			mutate: func(c *ASPConfig) { c.Routing.Paths[0].SignallingGateway = "sg-zz" },
			names:  []string{"route path", "sg-a-core", "unprovisioned Signalling Gateway", "sg-zz"},
		},
		{
			name: "Application Server named twice in one path",
			mutate: func(c *ASPConfig) {
				c.Routing.Paths[0].ApplicationServers = []RemoteASID{"as-core", "as-core"}
			},
			names: []string{"route path", "sg-a-core", "as-core", "twice"},
		},
		{
			name: "Application Server no SGP serves",
			mutate: func(c *ASPConfig) {
				c.Routing.Paths[0].ApplicationServers = []RemoteASID{"as-absent"}
			},
			names: []string{"route path", "sg-a-core", "as-absent", "no SGP", "sg-a"},
		},
		{
			name:   "unprovisioned path reference",
			mutate: func(c *ASPConfig) { c.Routing.MTPRoutes[0].Paths[0] = "sg-a-typo" },
			names:  []string{"sccp-a", "unprovisioned route path", "sg-a-typo"},
		},
		{
			name: "one path referenced twice",
			mutate: func(c *ASPConfig) {
				c.Routing.MTPRoutes[0].Paths = []MTPRoutePathID{"sg-a-core", "sg-a-core", "sg-b-core"}
			},
			names: []string{"sccp-a", "sg-a-core", "twice"},
		},
		{
			// Two different paths reaching one Application Server of one
			// Signalling Gateway is one candidate provisioned twice, which
			// makes the route's preference order ambiguous.
			name: "two paths reaching one Application Server",
			mutate: func(c *ASPConfig) {
				c.Routing.Paths = append(c.Routing.Paths, MTPRoutePath{
					ID:                 "sg-a-core-again",
					SignallingGateway:  "sg-a",
					ApplicationServers: []RemoteASID{"as-core"},
				})
				c.Routing.MTPRoutes[0].Paths = append(c.Routing.MTPRoutes[0].Paths, "sg-a-core-again")
			},
			names: []string{"sccp-a", "as-core", "sg-a", "two paths"},
		},
		{
			name:   "path referenced by no MTP Route",
			mutate: func(c *ASPConfig) { c.Routing.MTPRoutes[0].Paths = []MTPRoutePathID{"sg-a-core"} },
			names:  []string{"route path", "sg-b-core", "referenced by no MTP Route"},
		},
		{
			name:   "MTP Route with no path",
			mutate: func(c *ASPConfig) { c.Routing.MTPRoutes[0].Paths = nil },
			names:  []string{"MTP Route", "sccp-a", "names no path"},
		},
		{
			name:   "path with no Application Server",
			mutate: func(c *ASPConfig) { c.Routing.Paths[0].ApplicationServers = nil },
			names:  []string{"route path", "sg-a-core", "names no Application Server"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := validASPConfig()
			test.mutate(config)
			_, err := snapshotASPConfig(config)
			if !errors.Is(err, ErrInvalidASPConfig) {
				t.Fatalf("snapshotASPConfig() error = %v, want %v", err, ErrInvalidASPConfig)
			}
			for _, named := range test.names {
				if !strings.Contains(err.Error(), named) {
					t.Fatalf("rejection %q does not name %q", err, named)
				}
			}
		})
	}
}

// One SGP lists each MTP Route it carries once, whatever number of candidates
// that route has through it. Every walk over an SGP's routes is that list.
func TestSGPRouteOrderListsEachRouteOnce(t *testing.T) {
	config := twoApplicationServerASPConfig()
	snapshot, err := snapshotASPConfig(config)
	if err != nil {
		t.Fatalf("snapshotASPConfig: %v", err)
	}
	identity := SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}
	sgp := snapshot.sgpByIdentity[identity]
	if len(sgp.candidatesFor("sccp-a")) != 2 {
		t.Fatalf("sccp-a candidates = %#v, want two", sgp.candidatesFor("sccp-a"))
	}
	if len(sgp.routeOrder) != 1 || sgp.routeOrder[0] != "sccp-a" {
		t.Fatalf("SGP route order = %#v, want one entry for sccp-a", sgp.routeOrder)
	}
}

// A candidate the route names but no Association is configured for is not
// bound; one an Association is configured for but has not activated is not
// active. The two are different answers and the rejection says which.
func TestSelectionDistinguishesUnboundFromInactiveCandidates(t *testing.T) {
	const pointCode = uint32(0x123456)
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: twoApplicationServerASPConfig()})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	identity := SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}
	// The Association serves as-core only, so as-backup is provisioned on the
	// SGP and bound by nothing.
	association, _ := attachMultiScopeASPAssociation(t, endpoint, identity, 7, 1)
	applyASPDAVA(t, association, 7, 1, pointCode, 0)

	association.noteRoutingContextsUnacked(params.NewRoutingContext(1))
	_, err = endpoint.MTPTransfer(MTPTransferRequest{
		ProtocolData: transferProtocolData(pointCode, 1, nil),
	})
	var selection *MTPSelectionError
	if !errors.As(err, &selection) || len(selection.Rejections) != 2 {
		t.Fatalf("selection error = %v, want two rejections", err)
	}
	if selection.Rejections[0].ApplicationServer != "as-core" ||
		selection.Rejections[0].Reason != MTPCandidateNotActive {
		t.Fatalf("as-core rejection = %+v, want not-active", selection.Rejections[0])
	}
	if selection.Rejections[1].ApplicationServer != "as-backup" ||
		selection.Rejections[1].Reason != MTPCandidateNotBound {
		t.Fatalf("as-backup rejection = %+v, want not-bound", selection.Rejections[1])
	}
}

// A candidate nobody has reported on loses to one a peer called available,
// even where the unknown one comes first and the deployment opted in to using
// it. The opt-in makes an unreported destination usable; it does not make it
// preferable.
func TestOptedInUnknownCandidateRanksBehindAReportedOne(t *testing.T) {
	const pointCode = uint32(0x123456)
	config := validASPConfig()
	config.Routing.SignallingGatewaySelection = RouteSelectionPrimaryBackup
	config.Routing.AllowUnknownDestinations = true
	endpoint, associations, captures := newUnreportedASPTransferFixture(t, config)
	applyASPDAVA(t, associations["sg-b/sgp-b1"], 9, 42, pointCode, 0)

	result, err := endpoint.MTPTransfer(MTPTransferRequest{
		ProtocolData: transferProtocolData(pointCode, 1, nil),
	})
	if err != nil {
		t.Fatalf("MTPTransfer: %v", err)
	}
	if len(result.SuccessfulPaths) != 1 || result.SuccessfulPaths[0].Path != "sg-b-core" {
		t.Fatalf("selection = %#v, want the reported sg-b path", result.SuccessfulPaths)
	}
	if captures["sg-a/sgp-a1"].count() != 0 || captures["sg-b/sgp-b1"].count() != 1 {
		t.Fatalf("counts = sg-a:%d sg-b:%d, want 0 and 1",
			captures["sg-a/sgp-a1"].count(), captures["sg-b/sgp-b1"].count())
	}
}

// The projection reads the partition of the Application Server it was asked
// about. A partition this Endpoint holds no binding for is knowledge it does
// not have, not knowledge that the destination is reachable.
func TestDestinationKnowledgeReadsOnlyTheNamedPartition(t *testing.T) {
	store, err := newSSNMState(nil)
	if err != nil {
		t.Fatalf("newSSNMState: %v", err)
	}
	t.Cleanup(store.close)
	held := SSNMPartition{
		Kind:              SSNMCanonicalPartition,
		SignallingGateway: "sg-a",
		ApplicationServer: "as-core",
	}
	if err := store.bind(held, 1, false); err != nil {
		t.Fatalf("bind: %v", err)
	}
	if err := store.apply(SSNMReport{
		Kind:         SSNMDestinationAvailableReport,
		Source:       SSNMPeerReport,
		Partition:    held,
		Association:  1,
		Destinations: []PointCodeRange{{PointCode: 0x123456}},
	}); err != nil {
		t.Fatalf("apply: %v", err)
	}

	status := store.destinationKnowledge(held, 0x123456)
	if !status.availabilitySet || status.availability != DestinationAvailable || status.epoch == 0 {
		t.Fatalf("held partition knowledge = %+v", status)
	}
	sibling := held
	sibling.ApplicationServer = "as-backup"
	if status := store.destinationKnowledge(sibling, 0x123456); status.availabilitySet || status.epoch != 0 {
		t.Fatalf("unbound partition knowledge = %+v, want nothing", status)
	}
	if status := store.destinationKnowledge(held, 0x654321); status.availabilitySet {
		t.Fatalf("unreported destination knowledge = %+v, want nothing", status)
	}

	// The revision moves with every change the store accepted, so a selection
	// can tell that the knowledge it decided against has since moved.
	before := store.currentRevision()
	if before == 0 {
		t.Fatal("a store that retained a report reports revision zero")
	}
	if err := store.apply(SSNMReport{
		Kind:         SSNMDestinationUnavailableReport,
		Source:       SSNMPeerReport,
		Partition:    held,
		Association:  1,
		Destinations: []PointCodeRange{{PointCode: 0x123456}},
	}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	if after := store.currentRevision(); after <= before {
		t.Fatalf("revision after a retained report = %d, want more than %d", after, before)
	}
}

// A remembered traffic-flow assignment is only as good as the membership that
// made it. An RFC 4666 Section 4.3.4.3 override stops the overridden Routing
// Context carrying traffic without changing anything a Signalling Gateway has
// said, so the assignment has to be re-made from what the Endpoint still has.
func TestRememberedAssignmentIsRemadeWhenItsScopeIsOverridden(t *testing.T) {
	const pointCode = uint32(0x123456)
	// Loadshare at both levels is the mode that remembers an assignment and
	// reuses it, so it is the mode in which a stale one would be reused.
	config := oneSignallingGatewayTwoSGPConfig(RouteSelectionLoadshare)
	config.Routing.SignallingGatewaySelection = RouteSelectionLoadshare
	endpoint, associations, captures := newASPTransferFixture(t, config)
	request := MTPTransferRequest{ProtocolData: transferProtocolData(pointCode, 1, nil)}

	result, err := endpoint.MTPTransfer(request)
	if err != nil {
		t.Fatalf("first MTPTransfer: %v", err)
	}
	if len(result.SuccessfulPaths) != 1 {
		t.Fatalf("first selection = %#v, want one target", result.SuccessfulPaths)
	}
	assigned := result.SuccessfulPaths[0].SGP
	other := SignallingGatewayProcessID("sgp-a1")
	if assigned.SignallingGatewayProcess == other {
		other = "sgp-a2"
	}
	name := func(sgp SignallingGatewayProcessID) string { return "sg-a/" + string(sgp) }
	if captures[name(assigned.SignallingGatewayProcess)].count() != 1 {
		t.Fatal("the first request did not reach the SGP it named")
	}

	// The override changes nothing any Signalling Gateway has said about the
	// destination; it changes which Association may carry it.
	associations[name(assigned.SignallingGatewayProcess)].noteRoutingContextsOverridden([]uint32{1})
	result, err = endpoint.MTPTransfer(request)
	if err != nil {
		t.Fatalf("MTPTransfer after the assigned scope was overridden: %v", err)
	}
	if len(result.SuccessfulPaths) != 1 ||
		result.SuccessfulPaths[0].SGP.SignallingGatewayProcess != other {
		t.Fatalf("selection after override = %#v, want SGP %q", result.SuccessfulPaths, other)
	}
	if captures[name(assigned.SignallingGatewayProcess)].count() != 1 || captures[name(other)].count() != 1 {
		t.Fatalf("counts = %s:%d %s:%d, want 1 each",
			assigned.SignallingGatewayProcess, captures[name(assigned.SignallingGatewayProcess)].count(),
			other, captures[name(other)].count())
	}
}

// A remembered broadcast assignment widens when a Signalling Gateway that was
// carrying nothing gains an Association. The Endpoint's membership changed, so
// the assignment made before it did is not the assignment to keep.
func TestRememberedBroadcastWidensWhenAnSGPJoins(t *testing.T) {
	const pointCode = uint32(0x123456)
	config := validASPConfig()
	config.Routing.SignallingGatewaySelection = RouteSelectionBroadcast
	useSignallingGateways(config, "sg-a")
	setSGPSelection(config, RouteSelectionBroadcast, "sg-a")
	second := config.SignallingGateways[0].SGPs[0]
	second.ID = "sgp-a2"
	config.SignallingGateways[0].SGPs = append(config.SignallingGateways[0].SGPs, second)
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: config})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	first, firstCapture := attachMultiScopeASPAssociation(t, endpoint,
		SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}, 7, 1)
	applyASPDAVA(t, first, 7, 1, pointCode, 0)

	request := MTPTransferRequest{ProtocolData: transferProtocolData(pointCode, 1, nil)}
	result, err := endpoint.MTPTransfer(request)
	if err != nil {
		t.Fatalf("first broadcast: %v", err)
	}
	if len(result.SuccessfulPaths) != 1 {
		t.Fatalf("first broadcast targets = %#v, want one", result.SuccessfulPaths)
	}

	_, secondCapture := attachMultiScopeASPAssociation(t, endpoint,
		SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a2"}, 7, 1)
	result, err = endpoint.MTPTransfer(request)
	if err != nil {
		t.Fatalf("broadcast after the second SGP joined: %v", err)
	}
	if len(result.SuccessfulPaths) != 2 {
		t.Fatalf("widened broadcast targets = %#v, want two", result.SuccessfulPaths)
	}
	if firstCapture.count() != 2 || secondCapture.count() != 1 {
		t.Fatalf("counts = sgp-a1:%d sgp-a2:%d, want 2 and 1", firstCapture.count(), secondCapture.count())
	}
}

// An SGP selection mode chooses among equals. The SGPs of one Signalling
// Gateway carry different Application Servers, so their destination states
// differ, and the better-placed SGP is preferred over the merely earlier one.
func TestSGPSelectionPrefersTheBetterPlacedSGP(t *testing.T) {
	const pointCode = uint32(0x123456)
	config := &ASPConfig{
		SignallingGateways: []SignallingGatewayConfig{{
			ID: "sg-a",
			SGPs: []SignallingGatewayProcessConfig{
				{ID: "sgp-a1", ApplicationServers: []RemoteASConfig{{ID: "as-one", ASKey: staticASKey(7, 1)}}},
				{ID: "sgp-a2", ApplicationServers: []RemoteASConfig{{ID: "as-two", ASKey: staticASKey(7, 2)}}},
			},
		}},
		Routing: &ASPRoutingConfig{
			SignallingGatewaySelection: RouteSelectionPrimaryBackup,
			SignallingGatewayProcessSelection: map[SignallingGatewayID]RouteSelectionMode{
				"sg-a": RouteSelectionPrimaryBackup,
			},
			Paths: []MTPRoutePath{{
				ID:                 "sg-a-both",
				SignallingGateway:  "sg-a",
				ApplicationServers: []RemoteASID{"as-one", "as-two"},
			}},
			MTPRoutes: []MTPRouteConfig{{
				ID: "sccp-a", DestinationPointCode: 0x120000, Mask: 16,
				Paths: []MTPRoutePathID{"sg-a-both"},
			}},
		},
	}
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: config})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	first, firstCapture := attachMultiScopeASPAssociation(t, endpoint,
		SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}, 7, 1)
	second, secondCapture := attachMultiScopeASPAssociation(t, endpoint,
		SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a2"}, 7, 2)
	applyASPDRST(t, first, 7, 1, pointCode, 0)
	applyASPDAVA(t, second, 7, 2, pointCode, 0)

	result, err := endpoint.MTPTransfer(MTPTransferRequest{
		ProtocolData: transferProtocolData(pointCode, 1, nil),
	})
	if err != nil {
		t.Fatalf("MTPTransfer: %v", err)
	}
	if len(result.SuccessfulPaths) != 1 ||
		result.SuccessfulPaths[0].SGP.SignallingGatewayProcess != "sgp-a2" {
		t.Fatalf("selection = %#v, want the available sgp-a2", result.SuccessfulPaths)
	}
	if firstCapture.count() != 0 || secondCapture.count() != 1 {
		t.Fatalf("counts = sgp-a1:%d sgp-a2:%d, want 0 and 1", firstCapture.count(), secondCapture.count())
	}
}

// A write that fails for the selected Application Server is not retried through
// the next one, even where the next one would have succeeded. The library
// cannot tell whether the peer received the first attempt, so a second one is
// the application's decision to make.
func TestFailedWriteIsNotRetriedThroughAWorkingApplicationServer(t *testing.T) {
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

	// The transport refuses the preferred Application Server's scope and would
	// accept the failback's.
	association.dataWriter = func(data []byte, info *sctp.SndRcvInfo) (int, error) {
		parsed, parseErr := messages.Parse(data)
		if parseErr != nil {
			t.Errorf("parse outbound DATA: %v", parseErr)
			return 0, parseErr
		}
		message, ok := parsed.(*messages.Data)
		if !ok {
			t.Errorf("outbound message = %T, want *messages.Data", parsed)
			return 0, errors.New("not DATA")
		}
		if message.RoutingContext != nil &&
			len(message.RoutingContext.RoutingContexts()) == 1 &&
			message.RoutingContext.RoutingContexts()[0] == 1 {
			return 0, errors.New("transport refused the preferred scope")
		}
		return capture.write(data, info)
	}

	result, err := endpoint.MTPTransfer(MTPTransferRequest{
		ProtocolData: transferProtocolData(pointCode, 1, []byte("x")),
	})
	if len(result.SuccessfulPaths) != 0 {
		t.Fatalf("failed transfer reported successes %#v", result.SuccessfulPaths)
	}
	var transferErr *MTPTransferError
	if !errors.As(err, &transferErr) || len(transferErr.Failures) != 1 ||
		transferErr.Failures[0].Target.ApplicationServer != "as-core" {
		t.Fatalf("transfer error = %v, want one as-core failure", err)
	}
	if capture.count() != 0 {
		t.Fatalf("the failback Application Server carried %d message(s) within the same request",
			capture.count())
	}
}

func TestPrimaryBackupReturnsToARecoveredScopeAfterFreshReport(t *testing.T) {
	const pointCode = uint32(0x123456)
	config := validASPConfig()
	config.Routing.SignallingGatewaySelection = RouteSelectionPrimaryBackup
	endpoint, associations, captures := newASPTransferFixture(t, config)
	request := MTPTransferRequest{ProtocolData: transferProtocolData(pointCode, 1, nil)}

	if _, err := endpoint.MTPTransfer(request); err != nil {
		t.Fatalf("first MTPTransfer: %v", err)
	}
	associations["sg-a/sgp-a1"].noteRoutingContextsOverridden([]uint32{1})
	if _, err := endpoint.MTPTransfer(request); err != nil {
		t.Fatalf("MTPTransfer while the primary scope was overridden: %v", err)
	}
	if captures["sg-a/sgp-a1"].count() != 1 || captures["sg-b/sgp-b1"].count() != 1 {
		t.Fatalf("counts after the override = sg-a:%d sg-b:%d, want 1 and 1",
			captures["sg-a/sgp-a1"].count(), captures["sg-b/sgp-b1"].count())
	}

	associations["sg-a/sgp-a1"].noteRoutingContextsAcked(params.NewRoutingContext(1))
	applyASPDAVA(t, associations["sg-a/sgp-a1"], 7, 1, pointCode, 0)
	result, err := endpoint.MTPTransfer(request)
	if err != nil {
		t.Fatalf("MTPTransfer after the override was cleared: %v", err)
	}
	if len(result.SuccessfulPaths) != 1 || result.SuccessfulPaths[0].Path != "sg-a-core" {
		t.Fatalf("selection after recovery = %#v, want the sg-a path", result.SuccessfulPaths)
	}
	if captures["sg-a/sgp-a1"].count() != 2 || captures["sg-b/sgp-b1"].count() != 1 {
		t.Fatalf("counts after recovery = sg-a:%d sg-b:%d, want 2 and 1",
			captures["sg-a/sgp-a1"].count(), captures["sg-b/sgp-b1"].count())
	}
}

// An MTP-TRANSFER request's Correlation Id reaches every DATA it produces. RFC
// 4666 Section 3.3.1 has it "uniquely identify the MSU carried in the Protocol
// Data within an AS", and Section 4.3.4.3 has a broadcast use it so a newly
// active ASP can "synchronize its processing of traffic in each traffic flow
// with the other ASPs", so a broadcast that dropped it on one path would break
// the case it exists for.
func TestMTPTransferCarriesTheCorrelationIDOnEveryPath(t *testing.T) {
	config := validASPConfig()
	config.Routing.SignallingGatewaySelection = RouteSelectionBroadcast
	endpoint, _, captures := newASPTransferFixture(t, config)

	result, err := endpoint.MTPTransfer(MTPTransferRequest{
		ProtocolData:     transferProtocolData(0x123456, 1, []byte("x")),
		CorrelationID:    4242,
		CorrelationIDSet: true,
	})
	if err != nil {
		t.Fatalf("MTPTransfer: %v", err)
	}
	if len(result.SuccessfulPaths) != 2 {
		t.Fatalf("broadcast targets = %#v, want two", result.SuccessfulPaths)
	}
	for name, capture := range captures {
		data, _ := capture.lastData(t)
		if data.CorrelationID == nil || data.CorrelationID.CorrelationID() != 4242 {
			t.Fatalf("%s DATA Correlation Id = %#v, want 4242", name, data.CorrelationID)
		}
	}
}

// Targets are frozen at admission and each is written as it was selected. A
// scope that stops carrying traffic between admission and its write refuses
// that write and says so; the request's other targets are unaffected, and the
// result reports what was sent rather than claiming delivery.
func TestScopeLostAfterAdmissionRefusesOnlyItsOwnTarget(t *testing.T) {
	const pointCode = uint32(0x123456)
	config := validASPConfig()
	config.Routing.SignallingGatewaySelection = RouteSelectionBroadcast
	endpoint, associations, captures := newASPTransferFixture(t, config)

	writeReached := make(chan struct{})
	releaseWrite := make(chan struct{})
	first := associations["sg-a/sgp-a1"]
	firstCapture := captures["sg-a/sgp-a1"]
	first.dataWriter = func(data []byte, info *sctp.SndRcvInfo) (int, error) {
		close(writeReached)
		<-releaseWrite
		return firstCapture.write(data, info)
	}

	type outcome struct {
		result MTPTransferResult
		err    error
	}
	done := make(chan outcome, 1)
	go func() {
		result, err := endpoint.MTPTransfer(MTPTransferRequest{
			ProtocolData: transferProtocolData(pointCode, 1, []byte("x")),
		})
		done <- outcome{result: result, err: err}
	}()

	select {
	case <-writeReached:
	case <-time.After(time.Second):
		t.Fatal("MTPTransfer did not reach the first selected target")
	}
	// The second target's scope is taken over while the first target's write is
	// still running, so the loss lands after admission and before its write.
	associations["sg-b/sgp-b1"].noteRoutingContextsOverridden([]uint32{42})
	close(releaseWrite)

	got := <-done
	if len(got.result.SuccessfulPaths) != 1 ||
		got.result.SuccessfulPaths[0].SGP.SignallingGateway != "sg-a" {
		t.Fatalf("result = %#v, want only the sg-a target", got.result.SuccessfulPaths)
	}
	var transferErr *MTPTransferError
	if !errors.As(got.err, &transferErr) || len(transferErr.Failures) != 1 {
		t.Fatalf("error = %v, want one failure", got.err)
	}
	failure := transferErr.Failures[0]
	if failure.Target.SGP.SignallingGateway != "sg-b" || failure.Target.AS != *staticASKey(9, 42) {
		t.Fatalf("failure target = %+v, want the sg-b target", failure.Target)
	}
	var writeErr *DataWriteError
	if !errors.As(failure.Err, &writeErr) || writeErr.Outcome != DataNotSent ||
		!errors.Is(writeErr, ErrRoutingContextNotActive) {
		t.Fatalf("failure cause = %v, want a not-sent routing-context refusal", failure.Err)
	}
	if firstCapture.count() != 1 || captures["sg-b/sgp-b1"].count() != 0 {
		t.Fatalf("counts = sg-a:%d sg-b:%d, want 1 and 0",
			firstCapture.count(), captures["sg-b/sgp-b1"].count())
	}
}

// Stickiness belongs to loadsharing at both levels. An SGP selection that is
// not loadsharing decides again every time, so the SGP it prefers takes the
// flow back as soon as it can carry it.
func TestStickinessRequiresLoadshareAtTheSGPLevelToo(t *testing.T) {
	const pointCode = uint32(0x123456)
	config := oneSignallingGatewayTwoSGPConfig(RouteSelectionPrimaryBackup)
	config.Routing.SignallingGatewaySelection = RouteSelectionLoadshare
	endpoint, associations, captures := newASPTransferFixture(t, config)
	request := MTPTransferRequest{ProtocolData: transferProtocolData(pointCode, 1, nil)}

	if _, err := endpoint.MTPTransfer(request); err != nil {
		t.Fatalf("first MTPTransfer: %v", err)
	}
	if captures["sg-a/sgp-a1"].count() != 1 {
		t.Fatal("the first request did not take the preferred SGP")
	}
	associations["sg-a/sgp-a1"].noteRoutingContextsOverridden([]uint32{1})
	if _, err := endpoint.MTPTransfer(request); err != nil {
		t.Fatalf("MTPTransfer while the preferred SGP was overridden: %v", err)
	}
	if captures["sg-a/sgp-a2"].count() != 1 {
		t.Fatal("the request did not fail over to the second SGP")
	}

	associations["sg-a/sgp-a1"].noteRoutingContextsAcked(params.NewRoutingContext(1))
	result, err := endpoint.MTPTransfer(request)
	if err != nil {
		t.Fatalf("MTPTransfer after the preferred SGP recovered: %v", err)
	}
	if len(result.SuccessfulPaths) != 1 ||
		result.SuccessfulPaths[0].SGP.SignallingGatewayProcess != "sgp-a1" {
		t.Fatalf("selection after recovery = %#v, want sgp-a1", result.SuccessfulPaths)
	}
	if captures["sg-a/sgp-a1"].count() != 2 || captures["sg-a/sgp-a2"].count() != 1 {
		t.Fatalf("counts = sgp-a1:%d sgp-a2:%d, want 2 and 1",
			captures["sg-a/sgp-a1"].count(), captures["sg-a/sgp-a2"].count())
	}
}

// A payload too large to encode is refused before the transport sees it, and
// the refusal is a not-sent one so the application may send it elsewhere.
func TestMTPTransferRefusesAnUnencodablePayload(t *testing.T) {
	const pointCode = uint32(0x123456)
	config := validASPConfig()
	config.Routing.SignallingGatewaySelection = RouteSelectionPrimaryBackup
	useSignallingGateways(config, "sg-a")
	endpoint, _, captures := newASPTransferFixture(t, config)

	_, err := endpoint.MTPTransfer(MTPTransferRequest{
		ProtocolData: transferProtocolData(pointCode, 1, make([]byte, maxProtocolDataOctets+1)),
	})
	var transferErr *MTPTransferError
	if !errors.As(err, &transferErr) || len(transferErr.Failures) != 1 {
		t.Fatalf("oversized transfer error = %v, want one failure", err)
	}
	var writeErr *DataWriteError
	if !errors.As(transferErr.Failures[0].Err, &writeErr) ||
		writeErr.Outcome != DataNotSent || !errors.Is(writeErr, ErrProtocolDataTooLarge) {
		t.Fatalf("oversized failure = %v, want a not-sent ErrProtocolDataTooLarge", transferErr.Failures[0].Err)
	}
	if captures["sg-a/sgp-a1"].count() != 0 {
		t.Fatal("an unencodable payload reached the transport")
	}
}

// A loadshared flow stays on the SGP it was assigned to when the SGP inventory
// grows. RFC 4666 Appendix A.2.2 loadsharing must not move a flow because the
// candidate list got longer; moving it is the missequencing the mode exists to
// avoid.
func TestLoadsharedFlowKeepsItsSGPWhenAThirdJoins(t *testing.T) {
	const pointCode = uint32(0x123456)
	config := oneSignallingGatewayTwoSGPConfig(RouteSelectionLoadshare)
	config.Routing.SignallingGatewaySelection = RouteSelectionLoadshare
	third := config.SignallingGateways[0].SGPs[0]
	third.ID = "sgp-a3"
	config.SignallingGateways[0].SGPs = append(config.SignallingGateways[0].SGPs, third)
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: config})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })

	captures := make(map[SignallingGatewayProcessID]*mtpTransferCapture, 3)
	join := func(sgp SignallingGatewayProcessID) {
		t.Helper()
		association, capture := attachMultiScopeASPAssociation(t, endpoint,
			SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: sgp}, 7, 1)
		applyASPDAVA(t, association, 7, 1, 0x120000, 16)
		captures[sgp] = capture
	}
	join("sgp-a1")
	join("sgp-a2")

	// The flow has to be one the hash would place differently over two
	// candidates and over three, or a flow that did move would look stable.
	var sls uint8
	found := false
	for candidate := uint8(0); candidate < 255 && !found; candidate++ {
		key := newASPTransferFlowKey("sccp-a", transferProtocolData(pointCode, candidate, nil))
		hash := hashASPTransferFlow(key, "sg-a")
		if hash%2 != hash%3 {
			sls, found = candidate, true
		}
	}
	if !found {
		t.Fatal("no Signalling Link Selection places the flow differently over two and three SGPs")
	}
	request := MTPTransferRequest{ProtocolData: transferProtocolData(pointCode, sls, nil)}

	result, err := endpoint.MTPTransfer(request)
	if err != nil {
		t.Fatalf("first MTPTransfer: %v", err)
	}
	if len(result.SuccessfulPaths) != 1 {
		t.Fatalf("first selection = %#v, want one target", result.SuccessfulPaths)
	}
	assigned := result.SuccessfulPaths[0].SGP.SignallingGatewayProcess

	join("sgp-a3")
	result, err = endpoint.MTPTransfer(request)
	if err != nil {
		t.Fatalf("MTPTransfer after a third SGP joined: %v", err)
	}
	if len(result.SuccessfulPaths) != 1 ||
		result.SuccessfulPaths[0].SGP.SignallingGatewayProcess != assigned {
		t.Fatalf("the flow moved from %q to %#v", assigned, result.SuccessfulPaths)
	}
	if captures["sgp-a3"].count() != 0 {
		t.Fatal("the joining SGP took over an established loadshared flow")
	}
	if captures[assigned].count() != 2 {
		t.Fatalf("the assigned SGP carried %d of 2 messages", captures[assigned].count())
	}
}

// Two MTP Routes are two traffic flows even where their routing labels are
// identical, because the route is what the application named. One route's
// assignment is not the other's.
func TestNamedMTPRoutesKeepSeparateFlowAssignments(t *testing.T) {
	const pointCode = uint32(0x123456)
	config := validASPConfig()
	config.Routing.SignallingGatewaySelection = RouteSelectionLoadshare
	setSGPSelection(config, RouteSelectionLoadshare)
	config.Routing.MTPRoutes = []MTPRouteConfig{
		{
			ID: "wide", DestinationPointCode: 0x120000, Mask: 16,
			ServiceIndicators: []uint8{params.ServiceIndSCCP},
			Paths:             []MTPRoutePathID{"sg-a-core"},
		},
		{
			ID: "narrow", DestinationPointCode: 0x123400, Mask: 8,
			ServiceIndicators: []uint8{params.ServiceIndSCCP},
			Paths:             []MTPRoutePathID{"sg-b-core"},
		},
	}
	endpoint, _, captures := newASPTransferFixture(t, config)

	for _, named := range []MTPRouteID{"wide", "narrow"} {
		result, err := endpoint.MTPTransfer(MTPTransferRequest{
			MTPRoute:     named,
			ProtocolData: transferProtocolData(pointCode, 1, nil),
		})
		if err != nil {
			t.Fatalf("MTPTransfer(%q): %v", named, err)
		}
		want := MTPRoutePathID("sg-a-core")
		if named == "narrow" {
			want = "sg-b-core"
		}
		if len(result.SuccessfulPaths) != 1 || result.SuccessfulPaths[0].Path != want {
			t.Fatalf("MTPTransfer(%q) selection = %#v, want path %q", named, result.SuccessfulPaths, want)
		}
	}
	if captures["sg-a/sgp-a1"].count() != 1 || captures["sg-b/sgp-b1"].count() != 1 {
		t.Fatalf("counts = sg-a:%d sg-b:%d, want 1 each",
			captures["sg-a/sgp-a1"].count(), captures["sg-b/sgp-b1"].count())
	}
}

// Broadcast within one Signalling Gateway reaches every SGP that may carry the
// destination, including one whose Application Server is merely restricted.
// Preferring the better-placed SGP is what the other modes do.
func TestBroadcastWithinASignallingGatewayIncludesARestrictedSGP(t *testing.T) {
	const pointCode = uint32(0x123456)
	config := &ASPConfig{
		SignallingGateways: []SignallingGatewayConfig{{
			ID: "sg-a",
			SGPs: []SignallingGatewayProcessConfig{
				{ID: "sgp-a1", ApplicationServers: []RemoteASConfig{{ID: "as-one", ASKey: staticASKey(7, 1)}}},
				{ID: "sgp-a2", ApplicationServers: []RemoteASConfig{{ID: "as-two", ASKey: staticASKey(7, 2)}}},
			},
		}},
		Routing: &ASPRoutingConfig{
			SignallingGatewaySelection: RouteSelectionBroadcast,
			SignallingGatewayProcessSelection: map[SignallingGatewayID]RouteSelectionMode{
				"sg-a": RouteSelectionBroadcast,
			},
			Paths: []MTPRoutePath{{
				ID:                 "sg-a-both",
				SignallingGateway:  "sg-a",
				ApplicationServers: []RemoteASID{"as-one", "as-two"},
			}},
			MTPRoutes: []MTPRouteConfig{{
				ID: "sccp-a", DestinationPointCode: 0x120000, Mask: 16,
				Paths: []MTPRoutePathID{"sg-a-both"},
			}},
		},
	}
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: config})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	first, firstCapture := attachMultiScopeASPAssociation(t, endpoint,
		SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}, 7, 1)
	second, secondCapture := attachMultiScopeASPAssociation(t, endpoint,
		SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a2"}, 7, 2)
	applyASPDRST(t, first, 7, 1, pointCode, 0)
	applyASPDAVA(t, second, 7, 2, pointCode, 0)

	result, err := endpoint.MTPTransfer(MTPTransferRequest{
		ProtocolData: transferProtocolData(pointCode, 1, nil),
	})
	if err != nil {
		t.Fatalf("broadcast MTPTransfer: %v", err)
	}
	if len(result.SuccessfulPaths) != 2 {
		t.Fatalf("broadcast targets = %#v, want both SGPs", result.SuccessfulPaths)
	}
	if firstCapture.count() != 1 || secondCapture.count() != 1 {
		t.Fatalf("counts = sgp-a1:%d sgp-a2:%d, want 1 each", firstCapture.count(), secondCapture.count())
	}
}

// Two requests with the same routing label but different Originating Point
// Codes are different traffic flows here. RFC 4666 Section 1.4.2.5 has the ASP
// choose an SGP "by observing the Destination Point Code (and possibly other
// elements of the outgoing message, such as the SLS value)" without fixing
// which; taking the whole routing label, Originating Point Code included, is
// this library's choice, and a Routing Key may be "the DPC/OPC combination"
// (Section 1.4.2.1). They are assigned separately and neither waits behind the
// other.
func TestDifferentOriginatingPointCodesAreDifferentFlows(t *testing.T) {
	const pointCode = uint32(0x123456)
	config := validASPConfig()
	config.Routing.SignallingGatewaySelection = RouteSelectionPrimaryBackup
	config.Routing.MTPRoutes[0].OriginatingPointCodes = []uint32{0x111111, 0x222222}
	useSignallingGateways(config, "sg-a")
	endpoint, associations, _ := newASPTransferFixture(t, config)

	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondStarted := make(chan struct{})
	associations["sg-a/sgp-a1"].dataWriter = func(raw []byte, _ *sctp.SndRcvInfo) (int, error) {
		payload, err := capturedMTPTransferPayload(raw)
		if err != nil {
			return 0, err
		}
		switch payload {
		case "first":
			close(firstStarted)
			<-releaseFirst
		case "second":
			close(secondStarted)
		}
		return len(raw), nil
	}
	send := func(originatingPointCode uint32, payload string) <-chan error {
		done := make(chan error, 1)
		go func() {
			_, err := endpoint.MTPTransfer(MTPTransferRequest{
				ProtocolData: params.NewProtocolDataPayload(
					originatingPointCode, pointCode, params.ServiceIndSCCP, 0, 0, 1, []byte(payload)),
			})
			done <- err
		}()
		return done
	}

	firstDone := send(0x111111, "first")
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("the first MTPTransfer did not reach the transport")
	}
	secondDone := send(0x222222, "second")
	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		close(releaseFirst)
		<-firstDone
		<-secondDone
		t.Fatal("a request with a different Originating Point Code waited behind another flow")
	}
	close(releaseFirst)
	if err := <-firstDone; err != nil {
		t.Fatalf("first MTPTransfer: %v", err)
	}
	if err := <-secondDone; err != nil {
		t.Fatalf("second MTPTransfer: %v", err)
	}
}

// The congestion policy filters candidates the route could otherwise use. It
// runs last and it runs on nothing else: a policy that permits every level
// still cannot make an unavailable destination or an inactive Application
// Server carry traffic.
func TestCongestionPolicyCannotRescueAnUnusableCandidate(t *testing.T) {
	const pointCode = uint32(0x123456)
	config := validASPConfig()
	config.Routing.SignallingGatewaySelection = RouteSelectionPrimaryBackup
	config.Routing.CongestionPolicy = func(uint8, uint8, bool) bool { return true }
	useSignallingGateways(config, "sg-a")
	endpoint, associations, captures := newUnreportedASPTransferFixture(t, config)
	request := MTPTransferRequest{ProtocolData: transferProtocolData(pointCode, 1, nil)}

	applyASPDUNA(t, associations["sg-a/sgp-a1"], 7, 1, pointCode, 0)
	_, err := endpoint.MTPTransfer(request)
	var selection *MTPSelectionError
	if !errors.As(err, &selection) || len(selection.Rejections) != 1 ||
		selection.Rejections[0].Reason != MTPCandidateUnavailable {
		t.Fatalf("unavailable candidate under a permissive policy = %v", err)
	}

	applyASPDAVA(t, associations["sg-a/sgp-a1"], 7, 1, pointCode, 0)
	associations["sg-a/sgp-a1"].noteRoutingContextsUnacked(params.NewRoutingContext(1))
	_, err = endpoint.MTPTransfer(request)
	if !errors.As(err, &selection) || len(selection.Rejections) != 1 ||
		selection.Rejections[0].Reason != MTPCandidateNotActive {
		t.Fatalf("inactive candidate under a permissive policy = %v", err)
	}
	if captures["sg-a/sgp-a1"].count() != 0 {
		t.Fatal("a permissive congestion policy carried traffic the route could not")
	}
}

// A DUPU is about an MTP3-User at a destination, not about the destination. It
// leaves the destination's own availability exactly where it was, which for a
// destination nobody has reported on is unknown.
func TestUserPartUnavailabilityIsNotADestinationState(t *testing.T) {
	const pointCode = uint32(0x123456)
	config := validASPConfig()
	config.Routing.SignallingGatewaySelection = RouteSelectionPrimaryBackup
	useSignallingGateways(config, "sg-a")
	endpoint, associations, captures := newUnreportedASPTransferFixture(t, config)
	request := MTPTransferRequest{ProtocolData: transferProtocolData(pointCode, 1, nil)}

	if err := associations["sg-a/sgp-a1"].handleDestinationUserPartUnavailable(
		messages.NewDestinationUserPartUnavailable(
			params.NewNetworkAppearance(7), params.NewRoutingContext(1),
			params.NewAffectedPointCode(pointCode), params.NewUserCause(3, 2), nil),
	); err != nil {
		t.Fatalf("handleDestinationUserPartUnavailable: %v", err)
	}
	if _, err := endpoint.MTPTransfer(request); !errors.Is(err, ErrDestinationStateUnknown) {
		t.Fatalf("error after a DUPU = %v, want ErrDestinationStateUnknown", err)
	}

	// The destination's own availability still comes from the availability
	// messages, and the DUPU neither installed nor blocks one.
	applyASPDAVA(t, associations["sg-a/sgp-a1"], 7, 1, pointCode, 0)
	if _, err := endpoint.MTPTransfer(request); err != nil {
		t.Fatalf("MTPTransfer after the destination was reported available: %v", err)
	}
	if captures["sg-a/sgp-a1"].count() != 1 {
		t.Fatalf("captured %d messages, want 1", captures["sg-a/sgp-a1"].count())
	}
}

// The rejection a candidate reports is the first thing that disqualified it,
// in the contract's order: binding and authorization, then active state, then
// the destination's availability, and only then the congestion policy. A
// candidate the peers never reported on is refused for that, not for a
// congestion level derived from a report about a different dimension.
func TestRejectionReasonReportsAvailabilityBeforeCongestion(t *testing.T) {
	const pointCode = uint32(0x123456)
	refuseEveryCongestedLevel := func(uint8, uint8, bool) bool { return false }

	unknown := validASPConfig()
	unknown.Routing.SignallingGatewaySelection = RouteSelectionPrimaryBackup
	unknown.Routing.CongestionPolicy = refuseEveryCongestedLevel
	useSignallingGateways(unknown, "sg-a")
	endpoint, associations, _ := newUnreportedASPTransferFixture(t, unknown)
	// A congestion report installs the congestion dimension and says nothing
	// about availability, so this candidate is both congested and unreported.
	applyASPSCON(t, associations["sg-a/sgp-a1"], 7, 1, pointCode, 0, params.NewCongestionIndications(2))

	_, err := endpoint.MTPTransfer(MTPTransferRequest{
		ProtocolData: transferProtocolData(pointCode, 1, nil),
	})
	var selection *MTPSelectionError
	if !errors.As(err, &selection) || len(selection.Rejections) != 1 ||
		selection.Rejections[0].Reason != MTPCandidateStateUnknown {
		t.Fatalf("unknown and congested candidate = %v, want a state-unknown rejection", err)
	}
	if !errors.Is(err, ErrDestinationStateUnknown) {
		t.Fatalf("unknown and congested candidate error = %v, want ErrDestinationStateUnknown", err)
	}

	unavailable := validASPConfig()
	unavailable.Routing.SignallingGatewaySelection = RouteSelectionPrimaryBackup
	unavailable.Routing.CongestionPolicy = refuseEveryCongestedLevel
	useSignallingGateways(unavailable, "sg-a")
	second, secondAssociations, _ := newUnreportedASPTransferFixture(t, unavailable)
	applyASPSCON(t, secondAssociations["sg-a/sgp-a1"], 7, 1, pointCode, 0, params.NewCongestionIndications(2))
	applyASPDUNA(t, secondAssociations["sg-a/sgp-a1"], 7, 1, pointCode, 0)

	_, err = second.MTPTransfer(MTPTransferRequest{
		ProtocolData: transferProtocolData(pointCode, 1, nil),
	})
	if !errors.As(err, &selection) || len(selection.Rejections) != 1 ||
		selection.Rejections[0].Reason != MTPCandidateUnavailable {
		t.Fatalf("unavailable and congested candidate = %v, want an unavailable rejection", err)
	}
}
