// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"testing"
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
