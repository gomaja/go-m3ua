// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"errors"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// The dispatcher is deliberately not run in these tests. That is the point:
// the window this covers is exactly the one between a peer activation becoming
// observable and the dispatcher reaching handleStateUpdate, and an application
// that writes DATA on observing ASP-ACTIVE lands in it.

func TestPeerActivationCommitAdmitsDataBeforeTheDispatcherRuns(t *testing.T) {
	_, applicationServer, asp, sent := distributionFixtureForContexts(
		t, params.TrafficModeLoadshare, []uint32{1}, nil,
	)
	sent.reset()

	asp.commitPeerRoutingContextsActive([]uint32{1})

	if got := asp.State(); got != StateASPActive {
		t.Fatalf("State() after activation commit = %v, want %v", got, StateASPActive)
	}
	applicationServer.mu.Lock()
	recorded, member := applicationServer.asps[asp]
	applicationServer.mu.Unlock()
	if !member || recorded != StateASPActive {
		t.Fatalf(
			"Application Server recorded (%v, member=%t) for an ASP whose state is already observable, want (%v, true)",
			recorded, member, StateASPActive,
		)
	}

	if _, err := asp.WriteSignal(distributionData(1, 3, "immediately after activation")); err != nil {
		t.Fatalf("WriteSignal(DATA) immediately after activation: %v", err)
	}
	if got := len(dataMessages(sent.snapshot())); got != 1 {
		t.Fatalf("DATA messages written immediately after activation = %d, want 1", got)
	}
}

func TestPeerActivationCommitCreatesTheApplicationServerItActivates(t *testing.T) {
	_, _, asp, sent := distributionFixtureForContexts(
		t, params.TrafficModeLoadshare, []uint32{1, 2}, nil,
	)
	key := associationConfigASKey(asp.cfg, 2)
	// Routing Context 2 is configured on the association but has no Application
	// Server in the registry yet, which is the state the reported probe found:
	// membership and activation correct, registry lookup false.
	asp.as.mu.Lock()
	delete(asp.as.as, key)
	asp.as.mu.Unlock()
	if _, ok := asp.as.lookup(key); ok {
		t.Fatalf("Application Server %+v exists before activation, precondition not met", key)
	}
	sent.reset()

	asp.commitPeerRoutingContextsActive([]uint32{1, 2})

	if _, ok := asp.as.lookup(key); !ok {
		t.Fatalf("Application Server %+v was not created by the activation commit", key)
	}
	if _, err := asp.WriteSignal(distributionData(2, 3, "immediately after activation")); err != nil {
		t.Fatalf("WriteSignal(DATA) for a newly activated Application Server: %v", err)
	}
	if got := len(dataMessages(sent.snapshot())); got != 1 {
		t.Fatalf("DATA messages written for a newly activated Application Server = %d, want 1", got)
	}
}

func TestPeerActivationCommitNarrowsTheRegistryToTheActivatedApplicationServers(t *testing.T) {
	_, activated, asp, _ := distributionFixtureForContexts(
		t, params.TrafficModeLoadshare, []uint32{1, 2}, nil,
	)
	untouched := asp.as.get(associationConfigASKey(asp.cfg, 2))

	asp.commitPeerRoutingContextsActive([]uint32{1})

	// RFC 4666 Section 4.3.1: an ASP Active naming a subset activates the ASP
	// in those Application Servers only, and leaves it ASP-INACTIVE in the rest.
	for _, test := range []struct {
		name              string
		applicationServer *applicationServer
		want              State
	}{
		{"activated", activated, StateASPActive},
		{"unnamed", untouched, StateASPInactive},
	} {
		test.applicationServer.mu.Lock()
		got, member := test.applicationServer.asps[asp]
		test.applicationServer.mu.Unlock()
		if !member || got != test.want {
			t.Fatalf("%s Application Server recorded (%v, member=%t), want (%v, true)",
				test.name, got, member, test.want)
		}
	}
	if _, err := asp.WriteSignal(distributionData(2, 3, "unnamed Application Server")); !errors.Is(err, ErrRoutingContextNotActive) {
		t.Fatalf("WriteSignal(DATA RC 2) error = %v, want %v", err, ErrRoutingContextNotActive)
	}
}

func TestIPSPPeerActivationAdmitsTrafficToPeerBeforeTheDispatcherRuns(t *testing.T) {
	association, sent := newDoubleExchangeIPSPForTest(t)
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleIPSP})
	if err != nil {
		t.Fatalf("NewEndpoint(RoleIPSP): %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	association.as = endpoint.applicationServerRegistry()
	association.setIPSPState(IPSPState{
		TrafficToLocal: StateASPInactive,
		TrafficToPeer:  StateASPInactive,
	})
	association.maxMessageStreamID = 4

	// RFC 4666 Section 4.3.4.3: the peer's ASP Active is what authorises this
	// node to send it traffic, and the Ack is written before the state is
	// committed. Nothing else may be required before the traffic it authorises
	// is admitted.
	if err := association.handleAspActive(messages.NewAspActive(
		params.NewTrafficModeType(params.TrafficModeLoadshare),
		params.NewRoutingContext(22), nil,
	)); err != nil {
		t.Fatalf("handle ASP Active for TrafficToPeer: %v", err)
	}
	if got := association.IPSPState(); got.TrafficToPeer != StateASPActive {
		t.Fatalf("TrafficToPeer = %v after ASP Active, want %v", got.TrafficToPeer, StateASPActive)
	}

	before := len(*sent)
	if _, err := writeToPeer(association, 22, []byte("to-peer")); err != nil {
		t.Fatalf("write TrafficToPeer DATA immediately after activation: %v", err)
	}
	if len(*sent) != before+1 {
		t.Fatalf("messages written = %d, want 1", len(*sent)-before)
	}
	data, ok := (*sent)[len(*sent)-1].(*messages.Data)
	if !ok {
		t.Fatalf("last message on the wire = %T, want *messages.Data", (*sent)[len(*sent)-1])
	}
	if got := data.RoutingContext.RoutingContexts(); len(got) != 1 || got[0] != 22 {
		t.Fatalf("outgoing DATA Routing Contexts = %v, want [22]", got)
	}
}

func TestIPSPPeerActivationAdmitsContextlessTrafficBeforeTheDispatcherRuns(t *testing.T) {
	config := newDoubleExchangeAssociationConfigForTest()
	config.IPSP.TrafficToPeer = &IPSPTrafficConfig{}
	association, sent := newDoubleExchangeIPSPWithConfigForTest(t, config)
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleIPSP})
	if err != nil {
		t.Fatalf("NewEndpoint(RoleIPSP): %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	association.as = endpoint.applicationServerRegistry()
	association.setIPSPState(IPSPState{
		TrafficToLocal: StateASPDown,
		TrafficToPeer:  StateASPInactive,
	})
	association.maxMessageStreamID = 4

	if err := association.handleAspActive(messages.NewAspActive(nil, nil, nil)); err != nil {
		t.Fatalf("handle contextless ASP Active for TrafficToPeer: %v", err)
	}

	before := len(*sent)
	if _, err := association.WriteData(DataRequest{
		ProtocolData: testProtocolData([]byte("contextless-to-peer")),
	}); err != nil {
		t.Fatalf("write contextless TrafficToPeer DATA immediately after activation: %v", err)
	}
	if len(*sent) != before+1 {
		t.Fatalf("messages written = %d, want 1", len(*sent)-before)
	}
	data, ok := (*sent)[len(*sent)-1].(*messages.Data)
	if !ok {
		t.Fatalf("last message on the wire = %T, want *messages.Data", (*sent)[len(*sent)-1])
	}
	if data.RoutingContext != nil {
		t.Fatalf("contextless DATA carried Routing Context %v", data.RoutingContext.RoutingContexts())
	}
}

func TestPeerActivationCommitDrainsRetainedTraffic(t *testing.T) {
	listener, applicationServer, asp, sent := distributionFixture(t, params.TrafficModeLoadshare)
	applicationServer.setASPState(asp, StateASPActive, time.Hour)
	applicationServer.setASPState(asp, StateASPInactive, time.Hour)
	sent.reset()
	// RFC 4666 Section 4.3.2 retains this DATA while T(r) runs, and the
	// activation that ends AS-PENDING is what releases it.
	if _, err := listener.DistributeData(distributionData(1, 2, "retained")); err != nil {
		t.Fatalf("DistributeData while AS-PENDING: %v", err)
	}
	if got := sent.dataCount(); got != 0 {
		t.Fatalf("retained DATA written while AS-PENDING = %d, want 0", got)
	}

	asp.commitPeerRoutingContextsActive([]uint32{1})

	deadline := time.Now().Add(2 * time.Second)
	for sent.dataCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := sent.dataCount(); got != 1 {
		t.Fatalf("retained DATA written after the activation commit = %d, want 1", got)
	}
}

// TestPeerActivationCommitHoldsTheStateLockAcrossTheRegistryCommit is the
// atomicity the fix is about, as opposed to the ordering the tests above cover.
//
// They call the commit and then look, which cannot tell "the registry was
// written before the state became observable" from "the registry was written
// before the call returned". This one pins the first: while the registry commit
// cannot finish, ASP-ACTIVE must not be readable out of the association at all.
func TestPeerActivationCommitHoldsTheStateLockAcrossTheRegistryCommit(t *testing.T) {
	_, applicationServer, asp, _ := distributionFixtureForContexts(
		t, params.TrafficModeLoadshare, []uint32{1}, nil,
	)

	// Holding the Application Server's own lock stalls the registry commit
	// wherever it happens, without the test needing to know when it starts.
	applicationServer.mu.Lock()
	committed := make(chan struct{})
	go func() {
		defer close(committed)
		asp.commitPeerRoutingContextsActive([]uint32{1})
	}()

	observed := func() (State, bool) {
		// A failed TryRLock is the commit holding the state lock, which is the
		// answer this test wants: nothing can read the state while the registry
		// it authorises is still being written.
		if !asp.muState.TryRLock() {
			return 0, false
		}
		defer asp.muState.RUnlock()
		return asp.state, true
	}

	deadline := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(deadline) {
		state, readable := observed()
		if readable && state == StateASPActive {
			applicationServer.mu.Unlock()
			<-committed
			t.Fatal("ASP-ACTIVE was readable while the Application Server registry commit was still outstanding")
		}
		time.Sleep(time.Millisecond)
	}

	applicationServer.mu.Unlock()
	<-committed

	applicationServer.mu.Lock()
	recorded, member := applicationServer.asps[asp]
	applicationServer.mu.Unlock()
	if !member || recorded != StateASPActive {
		t.Fatalf("Application Server recorded (%v, member=%t) after the commit, want (%v, true)",
			recorded, member, StateASPActive)
	}
	if got := asp.State(); got != StateASPActive {
		t.Fatalf("State() after the commit = %v, want %v", got, StateASPActive)
	}
}
