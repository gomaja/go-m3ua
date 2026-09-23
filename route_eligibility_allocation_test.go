package m3ua

import (
	"fmt"
	"math/rand/v2"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gomaja/go-sctp"
)

// randomScopeAssociation builds an Association with a random role, inventory,
// SGP authorization and set of dynamically registered scopes: every input
// configuredASKeys reads.
func randomScopeAssociation(random *rand.Rand) *Association {
	randomKey := func(contextless bool) ASKey {
		key := ASKey{}
		if !contextless {
			key.RoutingContext, key.RoutingContextSet = uint32(random.IntN(5)), true
		}
		if random.IntN(3) != 0 {
			key.NetworkAppearance, key.NetworkAppearanceSet = uint32(7+random.IntN(2)), true
		}
		return key
	}
	randomInventory := func() []ASConfig {
		servers := make([]ASConfig, random.IntN(4))
		for index := range servers {
			servers[index].ASKey = randomKey(random.IntN(5) == 0)
		}
		return servers
	}
	roles := []Role{RoleASP, RoleSGP, RoleIPSP}
	association := &Association{
		cfg:     &AssociationConfig{ApplicationServers: randomInventory()},
		role:    roles[random.IntN(len(roles))],
		muState: new(sync.RWMutex),
	}
	if association.role == RoleIPSP && random.IntN(2) == 0 {
		association.cfg.IPSP = &IPSPConfig{ExchangeModel: IPSPExchangeDouble}
		if random.IntN(4) != 0 {
			association.cfg.IPSP.TrafficToPeer = &IPSPTrafficConfig{ApplicationServers: randomInventory()}
		}
		if random.IntN(2) == 0 {
			association.cfg.IPSP.TrafficToLocal = &IPSPTrafficConfig{ApplicationServers: randomInventory()}
		}
	}
	if random.IntN(2) == 0 {
		association.authorizationResolved = true
		association.authorizationExplicit = random.IntN(2) == 0
		for routingContext := uint32(1); routingContext <= 4; routingContext++ {
			if random.IntN(3) == 0 {
				association.authorizedRCs = append(association.authorizedRCs, routingContext)
			}
		}
	}
	if count := random.IntN(4); count > 0 {
		association.dynamicPeerASKeys = make(map[uint32]ASKey, count)
		for range count {
			key := randomKey(false)
			key.RoutingContext = uint32(random.IntN(7))
			association.dynamicPeerASKeys[key.RoutingContext] = key
		}
	}
	return association
}

// candidateScopeKeys returns every listed key, each with its Network
// Appearance and Routing Context perturbed, and every small key the random
// configurations can produce.
func candidateScopeKeys(listed []ASKey) []ASKey {
	candidates := append([]ASKey(nil), listed...)
	for _, key := range listed {
		candidates = append(candidates,
			ASKey{RoutingContext: key.RoutingContext, RoutingContextSet: key.RoutingContextSet, NetworkAppearance: key.NetworkAppearance + 1, NetworkAppearanceSet: key.NetworkAppearanceSet},
			ASKey{RoutingContext: key.RoutingContext, RoutingContextSet: key.RoutingContextSet, NetworkAppearance: key.NetworkAppearance, NetworkAppearanceSet: !key.NetworkAppearanceSet},
			ASKey{RoutingContext: key.RoutingContext + 1, RoutingContextSet: key.RoutingContextSet, NetworkAppearance: key.NetworkAppearance, NetworkAppearanceSet: key.NetworkAppearanceSet},
			ASKey{RoutingContext: key.RoutingContext, RoutingContextSet: !key.RoutingContextSet, NetworkAppearance: key.NetworkAppearance, NetworkAppearanceSet: key.NetworkAppearanceSet},
		)
	}
	for routingContext := uint32(0); routingContext <= 7; routingContext++ {
		for _, appearance := range []struct {
			value uint32
			set   bool
		}{{0, false}, {7, true}, {8, true}} {
			for _, routingContextSet := range []bool{false, true} {
				if !routingContextSet && routingContext != 0 {
					continue
				}
				candidates = append(candidates,
					ASKey{RoutingContext: routingContext, RoutingContextSet: routingContextSet, NetworkAppearance: appearance.value, NetworkAppearanceSet: appearance.set})
			}
		}
	}
	return candidates
}

// configuredASKeysContain replaces a scan of configuredASKeys() on the route
// selection path. It must give the same answer for every role, inventory,
// authorization and dynamic-registration shape, including the contextless and
// explicitly empty cases.
func TestConfiguredASKeysContainMatchesConfiguredASKeys(testContext *testing.T) {
	random := rand.New(rand.NewPCG(11, 29))
	for iteration := range 20000 {
		association := randomScopeAssociation(random)
		listed := association.configuredASKeys()
		for _, key := range candidateScopeKeys(listed) {
			if got, want := association.configuredASKeysContain(key), containsASKey(listed, key); got != want {
				testContext.Fatalf("iteration %d: configuredASKeysContain(%+v) = %t, configuredASKeys() = %+v\nrole=%v inventory=%+v ipsp=%+v authorization=(resolved %t explicit %t %v) dynamic=%+v",
					iteration, key, got, listed, association.role, association.cfg.ApplicationServers, association.cfg.IPSP,
					association.authorizationResolved, association.authorizationExplicit, association.authorizedRCs, association.dynamicPeerASKeys)
			}
		}
	}
	if (*Association)(nil).configuredASKeysContain(ASKey{}) {
		testContext.Fatal("a nil Association carries the contextless Application Server")
	}
}

func TestVisitASKeysMatchesAsKeysFor(testContext *testing.T) {
	endpoint, associations, _ := newHierarchicalLoadshareFixture(testContext, "primary")
	config := endpoint.aspRoutes.config
	for _, association := range associations {
		identity := *association.cfg.PeerSGP
		for _, id := range []RemoteASID{"primary", "secondary", "unknown"} {
			var visited []ASKey
			config.visitASKeys(association, identity, id, func(key ASKey) bool {
				visited = append(visited, key)
				return true
			})
			if want := config.asKeysFor(association, identity, id); fmt.Sprint(visited) != fmt.Sprint(want) {
				testContext.Fatalf("%s: visited %+v, asKeysFor %+v", id, visited, want)
			}
		}
	}
}

// Route selection asks eligibility for every candidate Association of every
// lookup and transfer; with statically provisioned Application Servers it must
// not allocate.
func TestRouteEligibilityDoesNotAllocate(testContext *testing.T) {
	endpoint, associations, _ := newHierarchicalLoadshareFixture(testContext, "primary")
	config := endpoint.aspRoutes.config
	association := associations[AssociationID(1)]
	identity := *association.cfg.PeerSGP
	key := *staticASKey(7, 1)
	if !aspAssociationBoundToAS(association, key) || !aspAssociationEligibleForAS(association, key) {
		testContext.Fatalf("fixture Association is not eligible for %+v", key)
	}
	checks := map[string]func(){
		"bound":    func() { _ = aspAssociationBoundToAS(association, key) },
		"eligible": func() { _ = aspAssociationEligibleForAS(association, key) },
		"candidate": func() {
			if _, _, eligible := config.eligibleCandidate(association, identity, "r"); !eligible {
				panic("fixture candidate is not eligible")
			}
		},
	}
	for name, check := range checks {
		if allocations := testing.AllocsPerRun(1000, check); allocations != 0 {
			testContext.Errorf("%s allocates %.1f times per call", name, allocations)
		}
	}
}

// Released transfer-sequence locks are reused for later flows. Reuse must keep
// every flow mutually exclusive, retire every lock once its last transfer
// finishes, and never hand one lock to two idle slots.
func TestMTPTransferReusesSequenceLocksSafely(testContext *testing.T) {
	endpoint, associations, _ := newASPTransferFixture(testContext, validASPConfig())
	const flows = 96
	var inFlight [flows]atomic.Int32
	var violations atomic.Int32
	for _, association := range associations {
		association.dataWriter = func(raw []byte, _ *sctp.SndRcvInfo) (int, error) {
			payload, err := capturedMTPTransferPayload(raw)
			if err != nil {
				return 0, err
			}
			flow, err := strconv.Atoi(payload)
			if err != nil {
				return 0, err
			}
			if inFlight[flow].Add(1) != 1 {
				violations.Add(1)
			}
			runtime.Gosched()
			inFlight[flow].Add(-1)
			return len(raw), nil
		}
	}
	var workers sync.WaitGroup
	errs := make(chan error, 8)
	for worker := range 8 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for transfer := range 400 {
				flow := (worker*37 + transfer) % flows
				protocolData := transferProtocolData(0x123400+uint32(flow/16), uint8(flow%16), []byte(strconv.Itoa(flow)))
				if _, err := endpoint.MTPTransfer(MTPTransferRequest{ProtocolData: protocolData}); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	workers.Wait()
	close(errs)
	for err := range errs {
		testContext.Fatalf("MTPTransfer: %v", err)
	}
	if count := violations.Load(); count != 0 {
		testContext.Fatalf("%d same-flow transfers overlapped", count)
	}
	routes := endpoint.aspRoutes
	routes.transferSequenceMu.Lock()
	defer routes.transferSequenceMu.Unlock()
	if len(routes.transferSequences) != 0 {
		testContext.Fatalf("retained transfer sequence gates = %d, want 0", len(routes.transferSequences))
	}
	if len(routes.idleTransferFlowLocks) == 0 || len(routes.idleTransferFlowLocks) > maxIdleTransferFlowLocks {
		testContext.Fatalf("idle flow locks = %d, want retired locks kept for reuse up to the %d bound", len(routes.idleTransferFlowLocks), maxIdleTransferFlowLocks)
	}
	seen := make(map[*aspTransferFlowLock]struct{}, len(routes.idleTransferFlowLocks))
	for _, flowLock := range routes.idleTransferFlowLocks {
		if _, duplicate := seen[flowLock]; duplicate {
			testContext.Fatal("one flow lock occupies two idle slots")
		}
		seen[flowLock] = struct{}{}
		if flowLock.references != 0 || !flowLock.mu.TryLock() {
			testContext.Fatalf("idle flow lock still referenced (%d) or held", flowLock.references)
		}
		flowLock.mu.Unlock()
	}
}

// An uncontended transfer takes a flow lock from the idle list and returns it,
// so the lock bookkeeping allocates nothing once one lock has been retired.
func TestTransferSequenceLockReuseDoesNotAllocate(testContext *testing.T) {
	endpoint, _, _ := newASPTransferFixture(testContext, validASPConfig())
	request := MTPTransferRequest{ProtocolData: transferProtocolData(0x123456, 1, []byte("reuse"))}
	sequence, err := endpoint.aspRoutes.lockTransferSequence(request)
	if err != nil {
		testContext.Fatal(err)
	}
	sequence.release()
	if allocations := testing.AllocsPerRun(1000, func() {
		sequence, err := endpoint.aspRoutes.lockTransferSequence(request)
		if err != nil {
			panic(err)
		}
		sequence.release()
	}); allocations != 0 {
		testContext.Fatalf("lock and release allocate %.1f times per transfer", allocations)
	}
}

// Dynamically bound Application Servers are visited in ascending Routing
// Context order and the visit stops when the visitor asks it to.
func TestVisitASKeysVisitsDynamicScopesInOrderAndStops(testContext *testing.T) {
	config := &ASPConfig{
		SignallingGateways: []SignallingGatewayConfig{{ID: "sg-a", SGPs: []SignallingGatewayProcessConfig{{
			ID: "sgp-a1",
			ApplicationServers: []RemoteASConfig{{
				ID:         "as-dynamic",
				RoutingKey: &RoutingKey{Groups: []RoutingKeyGroup{{DestinationPointCode: 0x120000}}},
			}},
		}}}},
	}
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: config})
	if err != nil {
		testContext.Fatal(err)
	}
	testContext.Cleanup(func() { _ = endpoint.Close() })
	association, _ := newTestConn(testContext, StateASPActive, RoleASP)
	identity := SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}
	association.cfg.PeerSGP = &identity
	for _, routingContext := range []uint32{11, 9, 13} {
		association.addDynamicASKey(ASKey{RoutingContext: routingContext, RoutingContextSet: true}, RoutingKey{}, false)
		association.noteCanonicalRemoteAS(routingContext, "as-dynamic")
	}
	routing := endpoint.aspRoutes.config
	var all []uint32
	routing.visitASKeys(association, identity, "as-dynamic", func(key ASKey) bool {
		all = append(all, key.RoutingContext)
		return true
	})
	if fmt.Sprint(all) != "[9 11 13]" {
		testContext.Fatalf("visited Routing Contexts %v, want [9 11 13]", all)
	}
	var visited []uint32
	routing.visitASKeys(association, identity, "as-dynamic", func(key ASKey) bool {
		visited = append(visited, key.RoutingContext)
		return key.RoutingContext < 11
	})
	if fmt.Sprint(visited) != "[9 11]" {
		testContext.Fatalf("visited Routing Contexts %v with a stop at 11, want [9 11]", visited)
	}
	if want := routing.asKeysFor(association, identity, "as-dynamic"); len(want) != 3 || want[0].RoutingContext != 9 {
		testContext.Fatalf("asKeysFor = %+v, want the three dynamic scopes in order", want)
	}
}
