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
// It was kept on the Conn, and Accept builds a fresh Conn per association, so an
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

	srvCfg := func() *Config {
		return NewServerConfig(&HeartbeatInfo{Enabled: false},
			0x22222222, 0x11111111, 1, params.TrafficModeLoadshare, 0, 0,
			[]uint32{1}, params.ServiceIndSCCP, 0, 0, 1)
	}
	srvAddr, err := sctp.ResolveSCTPAddr("sctp", fmt.Sprintf("127.0.0.2:%d", port))
	if err != nil {
		t.Fatal(err)
	}
	ln, err := Listen("m3ua", srvAddr, srvCfg())
	if err != nil {
		if isSCTPUnsupported(err) {
			t.Skipf("skipping socket-backed test: %v", err)
		}
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	accepted := make(chan *Conn, 4)
	go func() {
		for {
			c, err := ln.Accept(ctx)
			if err != nil {
				return
			}
			accepted <- c
		}
	}()

	cliCfg := NewClientConfig(&HeartbeatInfo{Enabled: false},
		0x11111111, 0x22222222, 1, params.TrafficModeLoadshare, 0, 0,
		[]uint32{1}, params.ServiceIndSCCP, 0, 0, 1)
	laddr, err := sctp.ResolveSCTPAddr("sctp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		t.Fatal(err)
	}

	// The SG learns from the SS7 network that the destination is reachable, and
	// records it against the node.
	ln.SetDestinationState(pointCode, DestinationAvailable)

	auditState := func(t *testing.T, asp *Conn) DestinationState {
		t.Helper()
		audit := messages.NewDestinationStateAudit(nil,
			params.NewRoutingContext(1),
			params.NewAffectedPointCodeWithMask(0, pointCode), nil)
		if _, err := asp.WriteSignal(audit); err != nil {
			t.Fatalf("sending DAUD: %v", err)
		}
		select {
		case s := <-asp.SignallingStatus():
			return s.State
		case <-time.After(10 * time.Second):
			t.Fatal("the SG never answered the DAUD")
			return DestinationUnavailable
		}
	}

	first, err := Dial(ctx, "m3ua", laddr, srvAddr, cliCfg)
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
	second, err := Dial(ctx, "m3ua", laddr, srvAddr, cliCfg)
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

// The setter on a Conn writes the same node-wide view, so an operator holding an
// accepted association does not have to find the Listener, and what they record
// is still there for the next ASP.
func TestConnAndListenerShareTheSGsDestinationView(t *testing.T) {
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
	srvConn.SetDestinationState(0xabcdef, DestinationRestricted)
	if srvConn.listener == nil {
		t.Fatal("an accepted Conn has no listener")
	}
	got, known := srvConn.listener.DestinationState(0xabcdef)
	if !known {
		t.Fatal("a state recorded on an accepted association is invisible to the SG")
	}
	if got != DestinationRestricted {
		t.Errorf("DestinationState = %v, want %v", got, DestinationRestricted)
	}

	// A client keeps its own view: an ASP's destination states are what a peer
	// told it, not a node-wide record, and pauseDestinations already scopes them
	// that way.
	cliConn.SetDestinationState(0x999999, DestinationAvailable)
	if _, known := srvConn.listener.DestinationState(0x999999); known {
		t.Error("an ASP's own destination state leaked into the SG's view")
	}
}

// The listener's setter has to work before any association exists — an operator
// knows the SS7 network's state at startup, not only once an ASP turns up.
func TestListenerDestinationStateBeforeAnyAssociation(t *testing.T) {
	// With a Config, as Listen always builds it: registry() reads RecoveryTimer
	// through the embedded *Config and dereferences nil without one. The setter
	// under test needs no Config at all, which is the point — it is usable
	// before anything has been accepted.
	l := &Listener{Config: NewServerConfig(&HeartbeatInfo{Enabled: false},
		0x22222222, 0x11111111, 1, params.TrafficModeLoadshare, 0, 0,
		[]uint32{1}, params.ServiceIndSCCP, 0, 0, 1)}

	if _, known := l.DestinationState(0x111111); known {
		t.Error("an unset destination reported as known")
	}

	l.SetDestinationState(0x111111, DestinationCongested)
	got, known := l.DestinationState(0x111111)
	if !known {
		t.Fatal("the state set before any association was lost")
	}
	if got != DestinationCongested {
		t.Errorf("DestinationState = %v, want %v", got, DestinationCongested)
	}

	// And the association accepted later sees it.
	as, nif, dests := l.registry()
	if as == nil || nif == nil || dests == nil {
		t.Fatal("registry returned a nil member")
	}
	appearance, set := appearanceOf(l.Config.NetworkAppearance)
	if state, known := dests.lookup(destinationKey{
		networkAppearance:    appearance,
		networkAppearanceSet: set,
		pointCode:            0x111111,
	}); !known || state != DestinationCongested {
		t.Errorf("the shared view holds %v (known=%v), want %v",
			state, known, DestinationCongested)
	}
}
