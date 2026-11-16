package m3ua

import (
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func newPeerRegistrationWorkFixture(t *testing.T) (*Endpoint, *Association, []*params.Param, *[]messages.M3UA) {
	t.Helper()
	config := newSGPAssociationConfigForTest(&HeartbeatInfo{Enabled: false}, 0, params.TrafficModeLoadshare, 0, []uint32{900})
	e, err := NewEndpoint(nationalCongestionEndpointConfig(EndpointConfig{Role: RoleSGP,
		RoutingKeyManagement: &RoutingKeyManagementConfig{
			AllowDynamicRoutingKeys: true, RemoveUnusedRoutingKeys: true,
			AuthorizeRegistration: func(RoutingKeyRegistrationRequest) RegistrationStatus { return RegistrationSuccessfullyRegistered },
		}}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	listener := newListener(e, NewListenerConfig(config))
	listener.as, listener.nif, listener.destinations = listener.registry()
	sibling, _ := addDistributionASP(t, listener, StateASPInactive, 900)
	if err := sibling.handleAspActive(messages.NewAspActive(params.NewTrafficModeType(params.TrafficModeLoadshare), params.NewRoutingContext(900), nil)); err != nil {
		t.Fatal(err)
	}
	peerLifecycleGroupedSCON(t, sibling, []uint32{900})
	peer, sent := newTestConnWithContexts(t, StateASPInactive, RoleSGP)
	setInventoryNetworkAppearance(&peer.cfg.ApplicationServers, params.NewNetworkAppearance(0))
	peer.as = e.as
	if !e.trackAssociation(peer) {
		t.Fatal("track registering peer")
	}
	t.Cleanup(func() { _ = peer.Close() })
	keys := make([]*params.Param, 64)
	for i := range keys {
		keys[i], err = routingKeyParameter(RoutingKeyRegistrationRequest{LocalRoutingKeyIdentifier: uint32(i + 1), RoutingKey: testRoutingKey(0, 2000+uint32(i), params.ServiceIndSCCP)})
		if err != nil {
			t.Fatal(err)
		}
	}

	return e, peer, keys, sent
}

func TestPeerMultiContextRegistrationDoesNotRescanSiblingRows(t *testing.T) {
	// The actual inbound handler allocates and applies 64 real results.
	// No registration or registry/state side effect is mocked.
	e, peer, keys, sent := newPeerRegistrationWorkFixture(t)
	before := e.SSNMKnowledge()
	beforeRecords, beforeBytes := e.ssnm.records, e.ssnm.bytes
	w := attachLifecycleWorkProbe(e.ssnm)
	if err := peer.handleRegistrationRequest(messages.NewRegistrationRequest(keys...)); err != nil {
		t.Fatal(err)
	}
	e.ssnm.mu.Lock()
	work := *w
	e.ssnm.work = nil
	for _, state := range e.ssnm.partitions {
		state.work = nil
	}
	e.ssnm.mu.Unlock()
	successful := 0
	for _, message := range *sent {
		response, ok := message.(*messages.RegistrationResponse)
		if !ok {
			continue
		}
		for _, parameter := range response.RegistrationResults {
			result, err := parameter.RegistrationResult()
			if err != nil {
				t.Fatal(err)
			}
			if result.RegistrationStatus.RegistrationStatus() != uint32(RegistrationSuccessfullyRegistered) {
				t.Fatalf("registration refused: %+v", result)
			}
			rc := result.RoutingContext.RoutingContext()
			key, held := peer.dynamicASKey(rc, false)
			if !held {
				t.Errorf("successful registration RC%d not applied", rc)
			}
			as, exists := e.as.lookup(key)
			if !exists || as.State() != ASInactive {
				t.Errorf("RC%d lost inactive dynamic AS registration", rc)
			}
			if peer.dynamicASKeyVersionFor(rc, false) == 0 {
				t.Errorf("RC%d has no version fence", rc)
			}
			successful++
		}
	}
	if successful != 64 || len(peer.dynamicRoutingContexts(false)) != 64 {
		t.Fatalf("successful=%d dynamic=%d want64", successful, len(peer.dynamicRoutingContexts(false)))
	}
	if peer.State() != StateASPInactive {
		t.Error("registration activated peer")
	}
	after := e.SSNMKnowledge()
	if !reflect.DeepEqual(after.PeerCongestion, before.PeerCongestion) || e.ssnm.records != beforeRecords || e.ssnm.bytes != beforeBytes {
		t.Fatal("registration changed unrelated sibling authority/charges")
	}
	// Baseline addDynamicASKey scans 64 sibling rows once per result:
	// 4096 real locked peer visits. Four passes over 64 rows+64 scopes
	// permit 512 visits, while accommodating result application overhead.
	t.Logf("store-locked registration peer rows=%d active rows=%d", work.peerRows, work.peerASRows)
	if work.peerRows > 4*(64+64) || work.peerASRows > 4*64 {
		t.Fatalf("multi-result REG rescanned sibling state: %+v", work)
	}
}

// This concurrency regression targets the interval between store retirement
// and map mutation. It must be red when peerRetentionMu is unlocked before
// addDynamicASKeyChanged, even if that mutant passes sequential lifetime tests.
func TestPeerRegistrationCaptureWaitsForReplacementPublication(t *testing.T) {
	e, peer, keys, sent := newPeerRegistrationWorkFixture(t)
	if err := peer.handleRegistrationRequest(messages.NewRegistrationRequest(keys[0])); err != nil {
		t.Fatal(err)
	}
	var rc uint32
	for _, m := range *sent {
		if rsp, ok := m.(*messages.RegistrationResponse); ok {
			result, err := rsp.RegistrationResults[0].RegistrationResult()
			if err != nil {
				t.Fatal(err)
			}
			rc = result.RoutingContext.RoutingContext()
		}
	}
	if rc == 0 {
		t.Fatal("registration did not assign RC")
	}
	if err := peer.handleAspActive(messages.NewAspActive(params.NewTrafficModeType(params.TrafficModeLoadshare), params.NewRoutingContext(rc), nil)); err != nil {
		t.Fatal(err)
	}
	peerLifecycleGroupedSCON(t, peer, []uint32{rc})
	scope := WireScope{NetworkAppearanceSet: true, RoutingContextSet: true, RoutingContexts: []uint32{rc}}
	old := peer.capturePeerCongestionAdmission(scope)
	key, ok := peer.dynamicASKey(rc, false)
	if !ok || old.bindings[key] == 0 {
		t.Fatal("setup lacks active dynamic binding")
	}
	refresh, err := routingKeyParameter(RoutingKeyRegistrationRequest{LocalRoutingKeyIdentifier: 999, RoutingKey: testRoutingKey(0, 2000, params.ServiceIndSCCP)})
	if err != nil {
		t.Fatal(err)
	}
	peer.muAckedRCs.Lock()
	ackLocked := true
	defer func() {
		if ackLocked {
			peer.muAckedRCs.Unlock()
		}
	}()
	replacement := make(chan error, 1)
	replacementDone := make(chan struct{})
	go func() {
		replacement <- peer.handleRegistrationRequest(messages.NewRegistrationRequest(refresh))
		close(replacementDone)
	}()
	// Existing helper observes the actual RWMutex lock wait; no sleep is
	// used as proof that retirement has reached the map-publication point.
	awaitSCONLockStage(t, "addDynamicASKeyChanged", replacementDone)
	select {
	case err := <-replacement:
		t.Fatalf("replacement escaped held map stage: %v", err)
	default:
	}
	captured := make(chan peerCongestionAdmission, 1)
	go func() { captured <- peer.capturePeerCongestionAdmission(scope) }()
	deadline := time.Now().Add(2 * time.Second)
	blocked := false
	for !blocked && time.Now().Before(deadline) {
		select {
		case got := <-captured:
			t.Fatalf("capture crossed unfinished replacement: %+v", got)
		default:
		}
		for _, stack := range goroutinesBlockedIn("withPeerCongestionBindings") {
			if strings.Contains(stack, "sync.(*Mutex).Lock") {
				blocked = true
				break
			}
		}
		runtime.Gosched()
	}
	if !blocked {
		t.Fatal("capture did not reach replacement serialization wait")
	}
	peer.muAckedRCs.Unlock()
	ackLocked = false
	select {
	case err := <-replacement:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("replacement remained blocked")
	}
	var fresh peerCongestionAdmission
	select {
	case fresh = <-captured:
	case <-time.After(2 * time.Second):
		t.Fatal("capture remained blocked after publication")
	}
	if fresh.bindings[key] == 0 || fresh.bindings[key] == old.bindings[key] {
		t.Fatal("capture did not observe replacement generation")
	}
	before := e.SSNMKnowledge()
	if err := peer.publishPeerCongestionReport(peerRetentionLateReport(scope), old); err != nil {
		t.Fatal(err)
	}
	if after := e.SSNMKnowledge(); !reflect.DeepEqual(after.PeerCongestion, before.PeerCongestion) || after.Revision != before.Revision {
		t.Fatal("old capture revived replaced peer records")
	}
}

func TestPeerGroupedRegistrationRefreshBatchesActiveAuthorities(t *testing.T) {
	e, peer, keys, sent := newPeerRegistrationWorkFixture(t)
	if err := peer.handleRegistrationRequest(messages.NewRegistrationRequest(keys...)); err != nil {
		t.Fatal(err)
	}
	var rcs []uint32
	for _, message := range *sent {
		if rsp, ok := message.(*messages.RegistrationResponse); ok {
			for _, p := range rsp.RegistrationResults {
				result, err := p.RegistrationResult()
				if err != nil {
					t.Fatal(err)
				}
				rcs = append(rcs, result.RoutingContext.RoutingContext())
			}
		}
	}
	if len(rcs) != 64 {
		t.Fatalf("registered=%d want64", len(rcs))
	}
	if err := peer.handleAspActive(messages.NewAspActive(params.NewTrafficModeType(params.TrafficModeLoadshare), params.NewRoutingContext(rcs[:32]...), nil)); err != nil {
		t.Fatal(err)
	}
	peerLifecycleGroupedSCON(t, peer, rcs[:32])
	before := e.SSNMKnowledge()
	if len(before.PeerCongestion) != 2112 {
		t.Fatalf("setup records=%d want2112", len(before.PeerCongestion))
	}
	var survivors []PeerCongestionState
	for _, record := range before.PeerCongestion {
		if record.Association != peer.ID() || record.AS.RoutingContext == rcs[31] {
			survivors = append(survivors, record)
		}
	}
	beforeRecords, beforeBytes := e.ssnm.records, e.ssnm.bytes
	w := attachLifecycleWorkProbe(e.ssnm)
	if err := peer.handleRegistrationRequest(messages.NewRegistrationRequest(keys[:31]...)); err != nil {
		t.Fatal(err)
	}
	e.ssnm.mu.Lock()
	work := *w
	e.ssnm.work = nil
	for _, state := range e.ssnm.partitions {
		state.work = nil
	}
	e.ssnm.mu.Unlock()
	if !reflect.DeepEqual(e.SSNMKnowledge().PeerCongestion, survivors) || e.ssnm.records != beforeRecords-1984 || e.ssnm.bytes != beforeBytes-1984*(256+8*32) {
		t.Fatal("refresh did not preserve exact unrelated records/charges")
	}
	t.Logf("store-locked active-refresh peer rows=%d AS rows=%d", work.peerRows, work.peerASRows)
	if work.peerRows > 4*2112 || work.peerASRows > 4*32 {
		t.Fatalf("grouped refresh multiplied retirement work: %+v", work)
	}
}
