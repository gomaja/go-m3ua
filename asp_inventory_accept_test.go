// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/gomaja/go-m3ua/messages/params"
)

// twoGatewayInventory provisions two Signalling Gateways of two SGPs each, so
// an accepted peer has to be resolved to one specific SGP and one specific
// Application Server rather than merely to "something provisioned".
func twoGatewayInventory() *ASPConfig {
	return &ASPConfig{
		SignallingGateways: []SignallingGatewayConfig{
			{
				ID: "sg-a",
				SGPs: []SignallingGatewayProcessConfig{
					{ID: "sgp-a1", ApplicationServers: []RemoteASConfig{{ID: "as-core", ASKey: staticASKey(7, 1)}}},
					{ID: "sgp-a2", ApplicationServers: []RemoteASConfig{{ID: "as-core", ASKey: staticASKey(7, 2)}}},
				},
			},
			{
				ID: "sg-b",
				SGPs: []SignallingGatewayProcessConfig{
					{ID: "sgp-b1", ApplicationServers: []RemoteASConfig{{ID: "as-core", ASKey: staticASKey(9, 1)}}},
					{ID: "sgp-b2", ApplicationServers: []RemoteASConfig{{ID: "as-core", ASKey: staticASKey(9, 2)}}},
				},
			},
		},
	}
}

// acceptedASPConfig is the AssociationConfig a listener selector returns for
// one accepted peer: the SGP it decided the peer is, and the wire scope that
// SGP uses for the Application Server it serves.
func acceptedASPConfig(
	signallingGateway SignallingGatewayID,
	sgp SignallingGatewayProcessID,
	networkAppearance, routingContext uint32,
) *AssociationConfig {
	config := newASPAssociationConfigForTest(
		&HeartbeatInfo{Enabled: false},
		0x111111, 0x222222, 1, params.TrafficModeLoadshare, networkAppearance, 0,
		[]uint32{routingContext}, params.ServiceIndSCCP, 0, 0, 1,
	)
	config.CorrelationID = nil
	config.PeerSGP = &SGPIdentity{
		SignallingGateway:        signallingGateway,
		SignallingGatewayProcess: sgp,
	}
	return config
}

func acceptInfoForPeer(port int, ip string) AcceptInfo {
	return newAcceptInfo(mcAddr(port, "127.0.0.1"), mcAddr(port+1, ip))
}

// The listener selector still runs where it always ran: on the accepted peer's
// addresses alone, before anything provisioned for that peer is consulted. The
// ASP peer inventory then authorizes what the selector chose, so the selector
// decides which SGP an accepted peer is and the inventory decides whether that
// answer is one this ASP provisioned.
func TestListenerSelectorRunsBeforePeerProvisioningAndAuthorization(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: twoGatewayInventory()})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })

	selectorError := errors.New("selector refused the peer")
	tests := []struct {
		name string
		// unprovisionedDefault makes the listener default a configuration the
		// peer inventory would reject, so a passing case proves the selected
		// configuration and not the default was authorized.
		unprovisionedDefault bool
		selected             func() *AssociationConfig
		selectorErr          error
		wantErr              error
		wantSGP              *SGPIdentity
	}{
		{
			name:                 "the selected configuration is what the inventory authorizes",
			unprovisionedDefault: true,
			selected:             func() *AssociationConfig { return acceptedASPConfig("sg-b", "sgp-b2", 9, 2) },
			wantSGP:              &SGPIdentity{SignallingGateway: "sg-b", SignallingGatewayProcess: "sgp-b2"},
		},
		{
			name:     "an unprovisioned selection is rejected although the default is provisioned",
			selected: func() *AssociationConfig { return acceptedASPConfig("sg-a", "sgp-zz", 7, 1) },
			wantErr:  ErrUnknownSGP,
		},
		{
			name:     "a selection naming no SGP is rejected",
			selected: func() *AssociationConfig { c := acceptedASPConfig("sg-a", "sgp-a1", 7, 1); c.PeerSGP = nil; return c },
			wantErr:  ErrMissingSGPIdentity,
		},
		{
			name: "a selection whose wire scope the SGP does not serve is rejected",
			// sgp-a1 labels as-core Routing Context 1; 2 is sgp-a2's label for
			// the same canonical Application Server.
			selected: func() *AssociationConfig { return acceptedASPConfig("sg-a", "sgp-a1", 7, 2) },
			wantErr:  ErrSGPRouteScopeMismatch,
		},
		{
			name: "a selection invalid for the role is rejected before the inventory sees it",
			selected: func() *AssociationConfig {
				config := acceptedASPConfig("sg-a", "sgp-zz", 7, 1)
				config.AuthorizeASP = func(ASPIdentity) []uint32 { return nil }
				return config
			},
			wantErr: ErrInvalidRoleConfiguration,
		},
		{
			name:        "a refusing selector short-circuits before the inventory",
			selected:    func() *AssociationConfig { return nil },
			selectorErr: selectorError,
			wantErr:     selectorError,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			defaultConfig := acceptedASPConfig("sg-a", "sgp-a1", 7, 1)
			if test.unprovisionedDefault {
				defaultConfig = acceptedASPConfig("sg-zz", "sgp-zz", 7, 1)
			}
			calls := 0
			var seen AcceptInfo
			listener := newListener(endpoint, &ListenerConfig{
				DefaultAssociationConfig: defaultConfig,
				SelectAssociationConfig: func(info AcceptInfo) (*AssociationConfig, error) {
					calls++
					seen = info
					if test.selectorErr != nil {
						return nil, test.selectorErr
					}
					return test.selected(), nil
				},
			})

			info := acceptInfoForPeer(3400, "127.0.0.9")
			resolved, err := listener.resolveAcceptedAssociationConfig(RoleASP, info)
			if calls != 1 {
				t.Fatalf("listener selector ran %d times, want exactly 1", calls)
			}
			if seen.RemoteAddr == nil || seen.RemoteAddr.String() != info.RemoteAddr.String() {
				t.Fatalf("selector saw remote %v, want %v", seen.RemoteAddr, info.RemoteAddr)
			}
			if test.wantErr != nil {
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("resolveAcceptedAssociationConfig() error = %v, want %v", err, test.wantErr)
				}
				if resolved != nil {
					t.Fatalf("a rejected peer resolved to %+v", resolved.PeerSGP)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveAcceptedAssociationConfig(): %v", err)
			}
			if resolved.PeerSGP == nil || *resolved.PeerSGP != *test.wantSGP {
				t.Fatalf("resolved SGP = %+v, want %+v", resolved.PeerSGP, test.wantSGP)
			}
		})
	}
}

// What the selector returns is snapshotted at the moment it returns, so a
// selector may hand back configuration it keeps and reuses. Accept builds the
// Association from the snapshot, not from the caller's object.
func TestAcceptedAssociationConfigIsOwnedBeforeTheInventoryJudgesIt(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: twoGatewayInventory()})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })

	reused := acceptedASPConfig("sg-a", "sgp-a1", 7, 1)
	listener := newListener(endpoint, &ListenerConfig{
		SelectAssociationConfig: func(AcceptInfo) (*AssociationConfig, error) { return reused, nil },
	})
	resolved, err := listener.resolveAcceptedAssociationConfig(RoleASP, acceptInfoForPeer(3410, "127.0.0.9"))
	if err != nil {
		t.Fatalf("resolveAcceptedAssociationConfig(): %v", err)
	}

	// Every part of the selector's object the Endpoint could have kept a
	// reference to is repointed at another provisioned SGP and another wire
	// scope afterwards.
	reused.PeerSGP.SignallingGateway = "sg-b"
	reused.PeerSGP.SignallingGatewayProcess = "sgp-b2"
	reused.NetworkAppearance = params.NewNetworkAppearance(9)
	reused.RoutingContexts = params.NewRoutingContext(2)
	reused.ASPProcedures = explicitASPProcedurePolicy()

	if resolved.PeerSGP.SignallingGateway != "sg-a" ||
		resolved.PeerSGP.SignallingGatewayProcess != "sgp-a1" {
		t.Fatalf("resolved SGP followed the selector's object to %+v", resolved.PeerSGP)
	}
	if appearance, set := appearanceOf(resolved.NetworkAppearance); !set || appearance != 7 {
		t.Fatalf("resolved Network Appearance = %d set=%v, want 7", appearance, set)
	}
	if got := resolved.RoutingContexts.RoutingContexts(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("resolved Routing Contexts = %v, want [1]", got)
	}
	if resolved.ASPProcedures != nil {
		t.Fatalf("resolved ASP procedure policy followed the selector's object to %+v", resolved.ASPProcedures)
	}
	if err := endpoint.validateAssociationConfig(resolved); err != nil {
		t.Fatalf("the resolved snapshot stopped being authorized: %v", err)
	}
}

// Accept is documented as safe for concurrent use, and an ASP that provisions
// peers now resolves each accepted peer through shared Endpoint inventory.
// Several accepts at once must each reach their own SGP and their own
// Application Server scope, and attach to the Endpoint without corrupting the
// shared route and peer state.
//
// Run under -race. TestConcurrentAcceptResolutionRaceControl is the control
// that the detector does fire on this shape of work.
func TestConcurrentAcceptResolutionMatchesProvisionedInventory(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: twoGatewayInventory()})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })

	type peer struct {
		signallingGateway SignallingGatewayID
		sgp               SignallingGatewayProcessID
		networkAppearance uint32
		routingContext    uint32
	}
	peers := []peer{
		{"sg-a", "sgp-a1", 7, 1},
		{"sg-a", "sgp-a2", 7, 2},
		{"sg-b", "sgp-b1", 9, 1},
		{"sg-b", "sgp-b2", 9, 2},
	}
	const repeats = 8

	// One Listener, one shared selector, exactly as a real accept loop has it.
	// The selector answers from the accepted peer's port alone, so nothing but
	// the AcceptInfo decides which SGP a goroutine resolves to.
	byPort := make(map[int]peer, len(peers)*repeats)
	for index, p := range peers {
		for repeat := 0; repeat < repeats; repeat++ {
			byPort[3500+index*repeats+repeat] = p
		}
	}
	listener := newListener(endpoint, &ListenerConfig{
		SelectAssociationConfig: func(info AcceptInfo) (*AssociationConfig, error) {
			p, provisioned := byPort[info.RemoteAddr.Port-1]
			if !provisioned {
				return nil, fmt.Errorf("no peer for port %d", info.RemoteAddr.Port)
			}
			return acceptedASPConfig(p.signallingGateway, p.sgp, p.networkAppearance, p.routingContext), nil
		},
	})

	type resolution struct {
		port   int
		config *AssociationConfig
		err    error
	}
	results := make(chan resolution, len(byPort))
	start := make(chan struct{})
	var accepting sync.WaitGroup
	for port := range byPort {
		accepting.Add(1)
		go func() {
			defer accepting.Done()
			<-start
			config, err := listener.resolveAcceptedAssociationConfig(RoleASP, acceptInfoForPeer(port, "127.0.0.9"))
			results <- resolution{port: port, config: config, err: err}
		}()
	}
	close(start)
	accepting.Wait()
	close(results)

	attached := make([]*Association, 0, len(byPort))
	seen := make(map[int]struct{}, len(byPort))
	for result := range results {
		if result.err != nil {
			t.Fatalf("concurrent accept for port %d: %v", result.port, result.err)
		}
		if _, duplicate := seen[result.port]; duplicate {
			t.Fatalf("port %d resolved twice", result.port)
		}
		seen[result.port] = struct{}{}
		want := byPort[result.port]
		if result.config.PeerSGP.SignallingGateway != want.signallingGateway ||
			result.config.PeerSGP.SignallingGatewayProcess != want.sgp {
			t.Fatalf("port %d resolved to %+v, want %s/%s",
				result.port, result.config.PeerSGP, want.signallingGateway, want.sgp)
		}
		keys := associationConfigASKeys(result.config)
		if len(keys) != 1 || keys[0] != *staticASKey(want.networkAppearance, want.routingContext) {
			t.Fatalf("port %d resolved Application Server scope %+v, want %+v",
				result.port, keys, *staticASKey(want.networkAppearance, want.routingContext))
		}
		association := newAssociation(RoleASP, result.config)
		attached = append(attached, association)
	}
	if len(seen) != len(byPort) {
		t.Fatalf("%d of %d concurrent accepts returned", len(seen), len(byPort))
	}

	// Attaching is the part that writes shared Endpoint state, so it is done
	// concurrently too.
	var attaching sync.WaitGroup
	tracked := make([]bool, len(attached))
	for index, association := range attached {
		attaching.Add(1)
		go func() {
			defer attaching.Done()
			tracked[index] = endpoint.trackAssociation(association)
		}()
	}
	attaching.Wait()
	for index, ok := range tracked {
		if !ok {
			t.Fatalf("Association %d was not attached", index)
		}
		t.Cleanup(func() { _ = attached[index].Close() })
	}

	endpoint.aspRoutes.mu.RLock()
	perSGP := make(map[SGPIdentity]int, len(peers))
	for _, identity := range endpoint.aspRoutes.associations {
		perSGP[identity]++
	}
	total := len(endpoint.aspRoutes.associations)
	order := len(endpoint.aspRoutes.associationOrder)
	endpoint.aspRoutes.mu.RUnlock()
	if total != len(byPort) || order != len(byPort) {
		t.Fatalf("Endpoint holds %d associations and %d orderings, want %d of each",
			total, order, len(byPort))
	}
	for _, p := range peers {
		identity := SGPIdentity{SignallingGateway: p.signallingGateway, SignallingGatewayProcess: p.sgp}
		if perSGP[identity] != repeats {
			t.Fatalf("SGP %+v holds %d associations, want %d", identity, perSGP[identity], repeats)
		}
	}
}

// TestConcurrentAcceptResolutionRaceControl is the control for the test above:
// it runs the same fan-out over the same Endpoint, but each goroutine also
// writes an unsynchronized shared map, which is precisely the fault the
// concurrent-accept test is meant to be able to catch. It reports a data race
// under -race and is therefore opt-in:
//
//	M3UA_RACE_DETECTOR_CONTROL=1 go test -race -run TestConcurrentAcceptResolutionRaceControl -count=1 .
//
// A run of that command that does NOT report a race means the harness above
// proves nothing, and the control has found a real problem.
func TestConcurrentAcceptResolutionRaceControl(t *testing.T) {
	if os.Getenv("M3UA_RACE_DETECTOR_CONTROL") != "1" {
		t.Skip("opt-in: set M3UA_RACE_DETECTOR_CONTROL=1 to prove the race detector fires on this pattern")
	}
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: twoGatewayInventory()})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	listener := newListener(endpoint, &ListenerConfig{
		SelectAssociationConfig: func(AcceptInfo) (*AssociationConfig, error) {
			return acceptedASPConfig("sg-a", "sgp-a1", 7, 1), nil
		},
	})

	// An unsynchronized counter rather than a shared container, so the control
	// reports the race and finishes instead of aborting the process on a
	// separate runtime fault.
	resolved := 0
	var last *AssociationConfig
	start := make(chan struct{})
	var accepting sync.WaitGroup
	for port := 3600; port < 3616; port++ {
		accepting.Add(1)
		go func() {
			defer accepting.Done()
			<-start
			config, err := listener.resolveAcceptedAssociationConfig(RoleASP, acceptInfoForPeer(port, "127.0.0.9"))
			if err != nil {
				return
			}
			resolved++
			last = config
		}()
	}
	close(start)
	accepting.Wait()
	if resolved == 0 || last == nil {
		t.Fatal("the control resolved nothing, so it exercised no concurrency")
	}
}

// twoGatewayRoutedInventory adds an outbound route inventory to the peers
// above, so attaching and detaching an Association does real work on the
// Endpoint's shared route state rather than only recording membership.
func twoGatewayRoutedInventory() *ASPConfig {
	config := twoGatewayInventory()
	config.Routing = &ASPRoutingConfig{
		SignallingGatewaySelection: RouteSelectionLoadshare,
		SignallingGatewayProcessSelection: map[SignallingGatewayID]RouteSelectionMode{
			"sg-a": RouteSelectionLoadshare,
			"sg-b": RouteSelectionLoadshare,
		},
		MTPRoutes: []MTPRouteConfig{
			{ID: "sccp", DestinationPointCode: 0x120000, Mask: 16},
		},
		Routes: []MTPRouteBinding{
			{MTPRoute: "sccp", AS: SGASKey{SignallingGateway: "sg-a", ApplicationServer: "as-core"}},
			{MTPRoute: "sccp", AS: SGASKey{SignallingGateway: "sg-b", ApplicationServer: "as-core"}},
		},
	}
	return config
}

// A real accept loop does not only accept: peers it accepted earlier go away
// while it is accepting new ones, and the application reads the Endpoint's
// derived view throughout. Attaching runs under the Endpoint lock and detaching
// does not, so the two meet on the ASP route state and must be serialized
// there.
//
// Run under -race. TestConcurrentAcceptResolutionRaceControl is the control
// that the detector does fire on this shape of work.
func TestConcurrentAcceptAttachAndDetachShareRouteStateSafely(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: twoGatewayRoutedInventory()})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })

	sgps := []struct {
		signallingGateway SignallingGatewayID
		sgp               SignallingGatewayProcessID
		networkAppearance uint32
		routingContext    uint32
	}{
		{"sg-a", "sgp-a1", 7, 1},
		{"sg-a", "sgp-a2", 7, 2},
		{"sg-b", "sgp-b1", 9, 1},
		{"sg-b", "sgp-b2", 9, 2},
	}
	listener := newListener(endpoint, &ListenerConfig{
		SelectAssociationConfig: func(info AcceptInfo) (*AssociationConfig, error) {
			chosen := sgps[info.RemoteAddr.Port%len(sgps)]
			return acceptedASPConfig(
				chosen.signallingGateway, chosen.sgp,
				chosen.networkAppearance, chosen.routingContext), nil
		},
	})
	accept := func(port int) (*Association, error) {
		config, err := listener.resolveAcceptedAssociationConfig(RoleASP, acceptInfoForPeer(port, "127.0.0.9"))
		if err != nil {
			return nil, err
		}
		association := newAssociation(RoleASP, config)
		if !endpoint.trackAssociation(association) {
			return nil, fmt.Errorf("port %d was not attached", port)
		}
		return association, nil
	}

	const established = 16
	existing := make([]*Association, established)
	for index := range existing {
		association, err := accept(3700 + index)
		if err != nil {
			t.Fatalf("establishing peer %d: %v", index, err)
		}
		existing[index] = association
	}

	// Half the established peers go away while the other half of the accept
	// budget arrives and the application reads the derived destination view.
	const arriving = 16
	arrived := make([]*Association, arriving)
	acceptErrs := make([]error, arriving)
	start := make(chan struct{})
	var working sync.WaitGroup
	for index := 0; index < arriving; index++ {
		working.Add(1)
		go func() {
			defer working.Done()
			<-start
			arrived[index], acceptErrs[index] = accept(3800 + index)
		}()
	}
	departing := established / 2
	closeErrs := make([]error, departing)
	for index := 0; index < departing; index++ {
		working.Add(1)
		go func() {
			defer working.Done()
			<-start
			closeErrs[index] = existing[index].Close()
		}()
	}
	for reader := 0; reader < 4; reader++ {
		working.Add(1)
		go func() {
			defer working.Done()
			<-start
			for round := 0; round < 32; round++ {
				_ = endpoint.MTPDestinationStatuses()
				_, _ = endpoint.MTPDestinationStatus(MTPDestination{
					MTPRoute: "sccp", PointCode: 0x120000, Mask: 16,
				})
			}
		}()
	}
	close(start)
	working.Wait()

	for index, err := range acceptErrs {
		if err != nil {
			t.Fatalf("accepting peer %d during teardown: %v", index, err)
		}
	}
	for index, err := range closeErrs {
		if err != nil {
			t.Fatalf("closing peer %d during accept: %v", index, err)
		}
	}
	for index := departing; index < established; index++ {
		t.Cleanup(func() { _ = existing[index].Close() })
	}
	for index := range arrived {
		t.Cleanup(func() { _ = arrived[index].Close() })
	}

	// The shared bookkeeping agrees with what actually happened: every peer
	// that arrived is held once, every peer that departed is gone, and the
	// per-SGP index and the ordering index have not drifted apart.
	want := established - departing + arriving
	endpoint.aspRoutes.mu.RLock()
	total := len(endpoint.aspRoutes.associations)
	order := len(endpoint.aspRoutes.associationOrder)
	eligible := len(endpoint.aspRoutes.associationEligibleRoutes)
	indexed := 0
	for _, associations := range endpoint.aspRoutes.associationsBySGP {
		indexed += len(associations)
	}
	endpoint.aspRoutes.mu.RUnlock()
	if total != want || order != want || eligible != want || indexed != want {
		t.Fatalf("Endpoint holds %d associations, %d orderings, %d route sets and %d SGP entries, want %d of each",
			total, order, eligible, indexed, want)
	}
}
