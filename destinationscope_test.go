// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

// RFC 4666 Section 4.5.3 has an SG answer a DAUD from what it knows of the SS7
// network. That knowledge belongs to the node: it does not arrive over any ASP's
// association, and it does not leave with one.
//
// It was kept on the Association, and Accept builds a fresh Association per SCTP association, so an
// ASP that reconnected — which Section 4.4.2 has it do precisely in order to
// resynchronise — was audited against an empty map. lookup returned not-known,
// which the handler turns into DUNA. Measured before the fix on a real pair of
// associations: DAUD for the same point code answered Available on the first and
// Unavailable on the second, with the operator having changed nothing. The ASP
// then stops traffic to a destination that is fully reachable, and nothing
// corrects it until someone sets the state again on the new association.
func TestDestinationStateSurvivesAnASPReconnecting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const port = 3225
	const pointCode = uint32(0x123456)

	srvCfg := func() *AssociationConfig {
		return newSGPAssociationConfigForTest(&HeartbeatInfo{Enabled: false}, 1, params.TrafficModeLoadshare, 0, []uint32{1})
	}
	srvAddr, err := sctp.ResolveSCTPAddr("sctp", fmt.Sprintf("127.0.0.2:%d", port))
	if err != nil {
		t.Fatal(err)
	}
	ln, err := listenSGP("m3ua", srvAddr, NewListenerConfig(srvCfg()))
	if err != nil {
		if isSCTPUnsupported(err) {
			t.Skipf("skipping socket-backed test: %v", err)
		}
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	accepted := make(chan *Association, 4)
	go func() {
		for {
			c, err := ln.Accept(ctx)
			if err != nil {
				return
			}
			accepted <- c
		}
	}()

	cliCfg := newASPAssociationConfigForTest(&HeartbeatInfo{Enabled: false}, 1, params.TrafficModeLoadshare, 0, []uint32{1})
	laddr, err := sctp.ResolveSCTPAddr("sctp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatal(err)
	}

	// The SG learns from the SS7 network that the destination is reachable, and
	// records it against the node. The Endpoint owns that record: RFC 4666
	// Section 1.2 has one management view per SG, not one per association.
	if err := reportAvailability(listenerEndpoint(t, ln), listenerDestinationScope(ln),
		pointCode, 0, DestinationAvailable); err != nil {
		t.Fatalf("recording the destination as available: %v", err)
	}

	auditState := func(t *testing.T, asp *Association) DestinationAvailability {
		t.Helper()
		audit := messages.NewDestinationStateAudit(nil,
			params.NewRoutingContext(1),
			params.NewAffectedPointCodeWithMask(0, pointCode), nil)
		if _, err := asp.WriteSignal(audit); err != nil {
			t.Fatalf("sending DAUD: %v", err)
		}
		select {
		case s := <-asp.SignallingStatus():
			return s.State.Availability
		case <-time.After(10 * time.Second):
			t.Fatal("the SG never answered the DAUD")
			return DestinationUnavailable
		}
	}

	first, err := dialASP(ctx, "m3ua", laddr, srvAddr, cliCfg)
	if err != nil {
		t.Fatal(err)
	}
	srvFirst := <-accepted
	if got := auditState(t, first); got != DestinationAvailable {
		t.Fatalf("first association: DAUD answered %v, want %v", got, DestinationAvailable)
	}
	_ = first.Close()
	_ = srvFirst.Close()

	// The ASP comes back on a new association.
	time.Sleep(300 * time.Millisecond)
	second, err := dialASP(ctx, "m3ua", laddr, srvAddr, cliCfg)
	if err != nil {
		t.Fatal(err)
	}
	srvSecond := <-accepted
	defer func() {
		_ = second.Close()
		_ = srvSecond.Close()
	}()

	if got := auditState(t, second); got != DestinationAvailable {
		t.Errorf("after the ASP reconnected, DAUD answered %v, want %v; the SG "+
			"reported a destination it knows is reachable as unreachable, and "+
			"the ASP will stop traffic to it", got, DestinationAvailable)
	}
}

// An accepted Association and its Listener resolve the same node-wide view, so
// what is recorded against one of them is what the SG answers with, and it is
// still there for the next ASP.
func TestAssociationAndListenerShareTheSGsDestinationView(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cliConn, srvConn, err := setupConn(t, ctx, 3227)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = cliConn.Close()
		_ = srvConn.Close()
	}()

	// srvConn was accepted by the listener setupConn created; recording through
	// it must be visible to the node.
	seedDestinationAvailability(srvConn, 0xabcdef, DestinationRestricted)
	if srvConn.listener == nil {
		t.Fatal("an accepted Association has no listener")
	}
	endpoint := listenerEndpoint(t, srvConn.listener)
	got, known := endpoint.DestinationStatus(listenerStatusKey(srvConn.listener, 0xabcdef))
	if !known {
		t.Fatal("a state recorded on an accepted association is invisible to the SG")
	}
	if got.State.Availability != DestinationRestricted {
		t.Errorf("retained availability = %v, want %v", got.State.Availability, DestinationRestricted)
	}

	// An ASP keeps its own view: its destination states are what a peer
	// told it, not a node-wide record, and pauseDestinations already scopes them
	// that way.
	seedDestinationAvailability(cliConn, 0x999999, DestinationAvailable)
	if _, known := endpoint.DestinationStatus(listenerStatusKey(srvConn.listener, 0x999999)); known {
		t.Error("an ASP's own destination state leaked into the SG's view")
	}
}

// Recording the SS7 network's state has to work before any association exists —
// an operator knows it at startup, not only once an ASP turns up.
func TestListenerDestinationStateBeforeAnyAssociation(t *testing.T) {
	// With an AssociationConfig, as Listen always builds it. The publication
	// path under test needs no accepted Association at all, which is the
	// point — it is usable before anything has been accepted.
	config := newSGPAssociationConfigForTest(&HeartbeatInfo{Enabled: false}, 1, params.TrafficModeLoadshare, 0, []uint32{1})
	l := newSGPListener(NewListenerConfig(config))
	endpoint := listenerEndpoint(t, l)

	if _, known := endpoint.DestinationStatus(listenerStatusKey(l, 0x111111)); known {
		t.Error("an unset destination reported as known")
	}

	// A congestion statement carries the congestion dimension alone. RFC 4666
	// Section 4.5.2.2 keeps the two statuses of a destination apart, so the
	// destination stays available and is reported congested beside that.
	if err := reportCongestion(endpoint, listenerDestinationScope(l), 0x111111, 0, 0, false); err != nil {
		t.Fatalf("recording congestion before any association: %v", err)
	}
	got, known := endpoint.DestinationStatus(listenerStatusKey(l, 0x111111))
	if !known {
		t.Fatal("the state set before any association was lost")
	}
	if !got.State.Congestion.Congested || got.State.Availability != DestinationAvailable {
		t.Errorf("the SG holds %+v, want a congested destination that is still available", got.State)
	}

	// And the association accepted later sees it.
	as, nif, dests := l.registry()
	if as == nil || nif == nil || dests == nil {
		t.Fatal("registry returned a nil member")
	}
	appearance, set := appearanceOf(inventoryNetworkAppearanceParam(l.AssociationConfig.ApplicationServers))
	if state, known := dests.lookup(destinationKey{
		networkAppearance:    appearance,
		networkAppearanceSet: set,
		pointCode:            0x111111,
	}); !known || !state.Congestion.Congested {
		t.Errorf("the shared view holds %+v (known=%v), want a congested destination",
			state, known)
	}
}

// listenerDestinationScope is the wire scope a Listener's configured
// Application Servers put on the SG's own SSNM statements: their Network
// Appearance, and no Routing Context, so the record is an all-context baseline.
func listenerDestinationScope(l *Listener) WireScope {
	appearance, set := listenerNetworkAppearance(l)
	return testWireScope(appearance, set)
}

// listenerStatusKey is the Endpoint query key for one destination in the scope
// a Listener resolves: its Network Appearance, narrowed to the single
// configured Routing Context when its Application Servers name exactly one.
func listenerStatusKey(l *Listener, pointCode uint32) DestinationStatusKey {
	appearance, set := listenerNetworkAppearance(l)
	key := DestinationStatusKey{
		NetworkAppearance:    appearance,
		NetworkAppearanceSet: set,
		PointCode:            pointCode,
	}
	if configured := asConfigRoutingContexts(l.AssociationConfig.ApplicationServers); len(configured) == 1 {
		key.RoutingContext, key.RoutingContextSet = configured[0], true
	}
	return key
}
