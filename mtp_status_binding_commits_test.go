package m3ua

import (
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
)

func TestOfferedStatusCanonicalBindingCommitsBeforeProjection(t *testing.T) {
	for _, kind := range []string{"canonical-replacement", "canonical-removal", "dynamic-removal", "versioned-dynamic-removal"} {
		t.Run(kind, func(t *testing.T) { testStatusBindingCommit(t, kind) })
	}
}

func testStatusBindingCommit(t *testing.T, kind string) {
	t.Helper()
	config := oneSignallingGatewayTwoSGPConfig(RouteSelectionLoadshare)
	for i := range config.SignallingGateways[0].SGPs {
		config.SignallingGateways[0].SGPs[i].ApplicationServers = []RemoteASConfig{
			{ID: "as-core", ASKey: staticASKey(7, 1)},
			{ID: "as-backup", RoutingKey: &RoutingKey{Groups: []RoutingKeyGroup{{DestinationPointCode: 0x120000}}}},
			{ID: "as-other", RoutingKey: &RoutingKey{Groups: []RoutingKeyGroup{{DestinationPointCode: 0x130000}}}},
		}
	}
	config.Routing.Paths[0].ApplicationServers = []RemoteASID{"as-core", "as-backup"}
	e, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: config,
		Congestion: &CongestionConfig{Default: CongestionProfile{Method: CongestionNationalWithoutPriorities}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	e.ssnm.validity = time.Hour
	a, _ := attachMultiScopeASPAssociation(t, e, SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}, 7, 1)
	b, _ := attachMultiScopeASPAssociation(t, e, SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a2"}, 7, 1)
	key := *staticASKey(7, 8)
	for _, peer := range []*Association{a, b} {
		peer.addDynamicASKey(key, RoutingKey{}, false)
		remote := RemoteASID("as-backup")
		if peer == a {
			remote = "as-other"
		}
		peer.noteCanonicalRemoteAS(8, remote)
		peer.noteRoutingContextsAcked(params.NewRoutingContext(1, 8))
		peer.notifyASPRouteStateChanged()
	}
	switch kind {
	case "canonical-removal":
		a.forgetCanonicalRemoteAS(8)
	case "dynamic-removal", "versioned-dynamic-removal":
		a.noteCanonicalRemoteAS(8, "as-backup")
		a.removeDynamicASKey(8, false)
	}
	// The backup's existing provider keeps its canonical permission and clear
	// report alive, but its transport pressure leaves effective congestion at 2.
	b.transportSnapshot.Store(&TransportCongestionState{Epoch: b.Epoch(), Known: true, Level: 2})
	applyASPDRST(t, a, 7, 1, statusCommitPC, 0)
	applyASPSCON(t, a, 7, 1, statusCommitPC, 0, params.NewCongestionIndications(2))
	applyASPDAVA(t, b, 7, 8, statusCommitPC, 0)
	applyASPSCON(t, b, 7, 8, statusCommitPC, 0, params.NewCongestionIndications(0))
	drainMTPIndications(e.MTPIndications())
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
	r := e.aspRoutes
	r.mu.Lock()
	locked := true
	defer func() {
		if locked {
			r.mu.Unlock()
		}
	}()
	route := r.config.mtpRoutes[r.config.mtpRouteByID["sccp-a"]]
	assertCongestion := func(want bool) {
		t.Helper()
		status, known := offeredDestinationStatus(route, statusCommitPC, r.recomputeMTPRouteLocked(route))
		if !known || status.congested != want || status.receivedUnknown || status.transportUnknown || want && status.congestionLevel != 2 {
			t.Fatalf("canonical binding selection=%+v known=%v, want congestion=%v", status, known, want)
		}
	}
	assertCongestion(true)
	// Only registration maps change. No state, acknowledgement, source report
	// or canonical binding reconciliation participates in this clear/onset pair.
	if kind == "dynamic-removal" || kind == "versioned-dynamic-removal" {
		a.addDynamicASKey(key, RoutingKey{}, false)
	} else {
		a.noteCanonicalRemoteAS(8, "as-backup")
	}
	assertCongestion(false)
	// Consume the activation mark using the still-positive coalesced projection.
	r.offerMTPTransferLocked(route, MTPTransferRequest{ProtocolData: transferProtocolData(statusCommitPC, 0, []byte("offer"))})
	_ = takeOfferedIndications(e.MTPIndications())
	switch kind {
	case "canonical-replacement":
		a.noteCanonicalRemoteAS(8, "as-other")
	case "canonical-removal":
		a.forgetCanonicalRemoteAS(8)
	case "dynamic-removal":
		a.removeDynamicASKey(8, false)
	case "versioned-dynamic-removal":
		if _, removed := a.removeDynamicASKeyVersion(8, false, a.dynamicASKeyVersionFor(8, false)); !removed {
			t.Fatal("current dynamic registration was not removed")
		}
	}
	assertCongestion(true)
	r.mu.Unlock()
	locked = false
	requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), true)
	for offer := 2; offer <= 9; offer++ {
		a.noteCanonicalRemoteAS(99, "as-other") // An AS outside this route, then equal mappings.
		requireOfferedStatus(t, offerStatusAt(t, e, statusCommitPC), offer == 9)
	}
}
