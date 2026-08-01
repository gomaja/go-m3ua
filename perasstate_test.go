// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"errors"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// RFC 4666 Section 4.3.1:
//
//	The state of each remote ASP/IPSP, in each AS that it is configured to
//	operate, is maintained in the peer M3UA layer (i.e., in the SGP or peer
//	IPSP, respectively).  The state of a particular ASP/IPSP in a particular
//	AS changes due to events.
//
// Figure 3 is titled "ASP State Transition Diagram, per AS", and ASP-INACTIVE is
// defined with the same scope: "the ASP/IPSP SHOULD NOT be sent any DATA or SSNM
// messages for the AS for which the ASP/IPSP is inactive".
//
// The SGP already stored a state per Application Server, but every transition
// wrote one association-wide value into all of them. An ASP that activated for
// one Routing Context was therefore recorded ASP-ACTIVE in every Application
// Server the association was configured for, and ASPsForTraffic handed it
// traffic for Application Servers it had never asked to serve.
func TestAnASPIsActiveOnlyInTheApplicationServersItActivatedFor(t *testing.T) {
	l := &Listener{Config: NewServerConfig(&HeartbeatInfo{Enabled: false},
		0x22222222, 0x11111111, 1, params.TrafficModeLoadshare, 0, 0,
		[]uint32{1, 2}, params.ServiceIndSCCP, 0, 0, 1)}

	asp, _ := newTestConnWithContexts(t, StateAspInactive, modeServer, 1, 2)
	as, _, _ := l.registry()
	asp.as = as

	// The ASP asks to carry traffic for Routing Context 1 only.
	if err := asp.handleAspActive(messages.NewAspActive(
		params.NewTrafficModeType(params.TrafficModeLoadshare),
		params.NewRoutingContext(1), nil)); err != nil {
		t.Fatalf("handleAspActive: %v", err)
	}
	asp.setState(StateAspActive)
	as.aspStateChanged(asp, StateAspActive)

	if got := l.ActiveASPs(1); len(got) != 1 {
		t.Errorf("Routing Context 1 has %d active ASPs, want 1; the ASP asked "+
			"for this Application Server", len(got))
	}
	if got := l.ActiveASPs(2); len(got) != 0 {
		t.Errorf("Routing Context 2 has %d active ASPs, want 0; the ASP never "+
			"asked to serve this Application Server and must not be sent its traffic",
			len(got))
	}
	if got := l.ASPsForTraffic(2, 1); len(got) != 0 {
		t.Errorf("ASPsForTraffic(2) returned %d associations, want 0; DATA for "+
			"an Application Server would go to an ASP that is ASP-INACTIVE in it",
			len(got))
	}

	// And the AS states differ, which is the whole point of a per-AS machine.
	if st := l.ApplicationServerState(1); st != ASActive {
		t.Errorf("AS 1 state = %v, want %v", st, ASActive)
	}
	if st := l.ApplicationServerState(2); st == ASActive {
		t.Errorf("AS 2 state = %v with no ASP active in it", st)
	}
}

// Activating for the second Application Server as well adds to the first rather
// than replacing it: an ASP may serve several.
func TestActivatingASecondApplicationServerKeepsTheFirst(t *testing.T) {
	l := &Listener{Config: NewServerConfig(&HeartbeatInfo{Enabled: false},
		0x22222222, 0x11111111, 1, params.TrafficModeLoadshare, 0, 0,
		[]uint32{1, 2}, params.ServiceIndSCCP, 0, 0, 1)}

	asp, _ := newTestConnWithContexts(t, StateAspInactive, modeServer, 1, 2)
	as, _, _ := l.registry()
	asp.as = as

	for _, rtCtx := range []uint32{1, 2} {
		err := asp.handleAspActive(messages.NewAspActive(
			params.NewTrafficModeType(params.TrafficModeLoadshare),
			params.NewRoutingContext(rtCtx), nil))
		// The second one arrives while the ASP is already ASP-ACTIVE. Figure 3
		// defines no such edge, so the peer is told -- but Section 4.3.4.3 owes
		// the Ack regardless, "Independently of the RC ... if the ASP is already
		// marked in the ASP-ACTIVE state", and the activation still applies.
		if err != nil {
			var unexpected *UnexpectedMessageError
			if rtCtx == 1 || !errors.As(err, &unexpected) {
				t.Fatalf("handleAspActive(%d): %v", rtCtx, err)
			}
		}
		asp.setState(StateAspActive)
		as.aspStateChanged(asp, StateAspActive)
	}

	for _, rtCtx := range []uint32{1, 2} {
		if got := l.ActiveASPs(rtCtx); len(got) != 1 {
			t.Errorf("Routing Context %d has %d active ASPs, want 1", rtCtx, len(got))
		}
	}
}

// An ASP Active naming no Routing Context asks for every Application Server the
// association carries, which is what Section 4.3.4.3 means by acting on the
// configured set when the parameter is absent.
func TestAnUnscopedASPActiveCoversEveryApplicationServer(t *testing.T) {
	l := &Listener{Config: NewServerConfig(&HeartbeatInfo{Enabled: false},
		0x22222222, 0x11111111, 1, params.TrafficModeLoadshare, 0, 0,
		[]uint32{1, 2}, params.ServiceIndSCCP, 0, 0, 1)}

	asp, _ := newTestConnWithContexts(t, StateAspInactive, modeServer, 1, 2)
	as, _, _ := l.registry()
	asp.as = as

	if err := asp.handleAspActive(messages.NewAspActive(
		params.NewTrafficModeType(params.TrafficModeLoadshare), nil, nil)); err != nil {
		t.Fatalf("handleAspActive: %v", err)
	}
	asp.setState(StateAspActive)
	as.aspStateChanged(asp, StateAspActive)

	for _, rtCtx := range []uint32{1, 2} {
		if got := l.ActiveASPs(rtCtx); len(got) != 1 {
			t.Errorf("Routing Context %d has %d active ASPs, want 1", rtCtx, len(got))
		}
	}
}

// Standing down in one Application Server leaves the ASP active in the other.
func TestASPInactiveForOneApplicationServerLeavesTheOther(t *testing.T) {
	l := &Listener{Config: NewServerConfig(&HeartbeatInfo{Enabled: false},
		0x22222222, 0x11111111, 1, params.TrafficModeLoadshare, 0, 0,
		[]uint32{1, 2}, params.ServiceIndSCCP, 0, 0, 1)}

	asp, _ := newTestConnWithContexts(t, StateAspInactive, modeServer, 1, 2)
	as, _, _ := l.registry()
	asp.as = as

	if err := asp.handleAspActive(messages.NewAspActive(
		params.NewTrafficModeType(params.TrafficModeLoadshare), nil, nil)); err != nil {
		t.Fatalf("handleAspActive: %v", err)
	}
	asp.setState(StateAspActive)
	as.aspStateChanged(asp, StateAspActive)

	// Now it stands down for Routing Context 1 alone.
	if err := asp.handleAspInactive(messages.NewAspInactive(
		params.NewRoutingContext(1), nil)); err != nil {
		t.Fatalf("handleAspInactive: %v", err)
	}
	as.aspStateChanged(asp, StateAspActive)

	if got := l.ActiveASPs(1); len(got) != 0 {
		t.Errorf("Routing Context 1 still has %d active ASPs after the ASP stood "+
			"down for it", len(got))
	}
	if got := l.ActiveASPs(2); len(got) != 1 {
		t.Errorf("Routing Context 2 has %d active ASPs, want 1; standing down for "+
			"one Application Server took the ASP out of the other", len(got))
	}
}

// ASP-DOWN is not per-AS. Figure 3 reaches it by ASP Down or SCTP CDI, neither
// of which names a Routing Context, so the ASP leaves every Application Server
// and its next ASP Active decides afresh -- it must not inherit the Application
// Servers it happened to hold before it went down.
func TestGoingDownClearsEveryApplicationServer(t *testing.T) {
	l := &Listener{Config: NewServerConfig(&HeartbeatInfo{Enabled: false},
		0x22222222, 0x11111111, 1, params.TrafficModeLoadshare, 0, 0,
		[]uint32{1, 2}, params.ServiceIndSCCP, 0, 0, 1)}

	asp, _ := newTestConnWithContexts(t, StateAspInactive, modeServer, 1, 2)
	as, _, _ := l.registry()
	asp.as = as

	activate := func(rtCtx uint32) {
		t.Helper()
		err := asp.handleAspActive(messages.NewAspActive(
			params.NewTrafficModeType(params.TrafficModeLoadshare),
			params.NewRoutingContext(rtCtx), nil))
		var unexpected *UnexpectedMessageError
		if err != nil && !errors.As(err, &unexpected) {
			t.Fatalf("handleAspActive(%d): %v", rtCtx, err)
		}
		asp.setState(StateAspActive)
		as.aspStateChanged(asp, StateAspActive)
	}

	// Active in Application Server 1 only.
	activate(1)
	if got := l.ActiveASPs(1); len(got) != 1 {
		t.Fatalf("Routing Context 1 has %d active ASPs, want 1", len(got))
	}

	// The association goes down, taking the ASP out of every AS.
	if err := asp.handleStateUpdate(StateAspDown); err != nil {
		t.Fatalf("handleStateUpdate(ASP-DOWN): %v", err)
	}
	for _, rtCtx := range []uint32{1, 2} {
		if got := l.ActiveASPs(rtCtx); len(got) != 0 {
			t.Errorf("Routing Context %d still has %d active ASPs after ASP-DOWN",
				rtCtx, len(got))
		}
	}

	// It comes back and activates for the OTHER Application Server. If the old
	// scope survived, it would be recorded active in Application Server 1 too --
	// one it did not ask for this time.
	asp.setState(StateAspInactive)
	activate(2)

	if got := l.ActiveASPs(2); len(got) != 1 {
		t.Errorf("Routing Context 2 has %d active ASPs, want 1", len(got))
	}
	if got := l.ActiveASPs(1); len(got) != 0 {
		t.Errorf("Routing Context 1 has %d active ASPs, want 0; the ASP inherited "+
			"an Application Server it held before it went down and never asked "+
			"for again", len(got))
	}
}

// With no activation recorded at all, an ASP-ACTIVE transition applies to every
// Application Server the association carries. That is the pre-existing
// behaviour, kept as the fallback for a state reached without an ASP Active to
// scope it, and it is the branch a scoped record replaces.
func TestWithNothingRecordedTheASPCountsAsActiveEverywhere(t *testing.T) {
	conn, _ := newTestConnWithContexts(t, StateAspInactive, modeServer, 1, 2)

	for _, rtCtx := range []uint32{1, 2, 99} {
		if !conn.activeForRoutingContext(rtCtx) {
			t.Errorf("activeForRoutingContext(%d) = false with nothing recorded; "+
				"an ASP-ACTIVE transition would reach no Application Server at all",
				rtCtx)
		}
	}

	// Once a scope is recorded, only what it names counts.
	conn.noteRoutingContextsActive([]uint32{1})
	if !conn.activeForRoutingContext(1) {
		t.Error("activeForRoutingContext(1) = false after activating for it")
	}
	if conn.activeForRoutingContext(2) {
		t.Error("activeForRoutingContext(2) = true although the ASP never activated for it")
	}

	// And clearing it returns to the fallback.
	conn.forgetActiveRoutingContexts()
	if !conn.activeForRoutingContext(2) {
		t.Error("the scope survived forgetActiveRoutingContexts")
	}
}
