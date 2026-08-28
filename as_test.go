// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// notifies returns every Notify the Association wrote, with its Status decoded.
func notifies(sent []messages.M3UA) []*messages.Notify {
	var out []*messages.Notify
	for _, m := range sent {
		if n, ok := m.(*messages.Notify); ok {
			out = append(out, n)
		}
	}
	return out
}

// statusOf renders a Notify's Status Type and Information.
func statusOf(t *testing.T, n *messages.Notify) (uint16, uint16) {
	t.Helper()
	if n.Status == nil {
		t.Fatal("Notify carried no Status parameter, which Section 3.8.2 makes Mandatory")
	}
	return n.Status.StatusType(), n.Status.StatusInfo()
}

// asTestConn builds an SGP Association attached to a shared AS registry, as the
// Listener does on Accept.
func asTestConn(t *testing.T, reg *applicationServers, state State, rtCtxs ...uint32) (*Association, *[]messages.M3UA) {
	t.Helper()
	conn, sent := newTestConn(t, state, RoleSGP)
	conn.cfg.RoutingContexts = params.NewRoutingContext(rtCtxs...)
	conn.as = reg
	reg.aspStateChanged(conn, state)
	return conn, sent
}

// TestASStateFollowsItsASPs covers the AS state machine of RFC 4666 Section
// 4.3.2, which had no implementation at all: there was no AS type, no
// AS-ACTIVE/INACTIVE/PENDING, and NewNotify was never called outside tests.
//
//	AS-INACTIVE: The Application Server is available, but no application
//	traffic is active.
//
//	AS-ACTIVE: The Application Server is available and application
//	traffic is active.
//
//	AS-PENDING: An active ASP has transitioned to ASP-INACTIVE or ASP
//	DOWN and it was the last remaining active ASP in the AS.
func TestASStateFollowsItsASPs(t *testing.T) {
	reg := newApplicationServers(time.Hour) // T(r) long enough not to fire here
	as := reg.get(1)

	first, _ := asTestConn(t, reg, StateASPInactive, 1)
	if got := as.State(); got != ASInactive {
		t.Errorf("AS state = %v with one ASP-INACTIVE ASP, want %v", got, ASInactive)
	}

	reg.aspStateChanged(first, StateASPActive)
	if got := as.State(); got != ASActive {
		t.Errorf("AS state = %v with an ASP-ACTIVE ASP, want %v", got, ASActive)
	}

	// A second ASP joining and going active leaves the AS active.
	second, _ := asTestConn(t, reg, StateASPInactive, 1)
	reg.aspStateChanged(second, StateASPActive)
	if got := as.State(); got != ASActive {
		t.Errorf("AS state = %v with two active ASPs, want %v", got, ASActive)
	}

	// One of two going inactive is not the last: still active.
	reg.aspStateChanged(second, StateASPInactive)
	if got := as.State(); got != ASActive {
		t.Errorf("AS state = %v after one of two ASPs went inactive, want %v",
			got, ASActive)
	}

	// The last active ASP leaving is what makes it pending.
	reg.aspStateChanged(first, StateASPInactive)
	if got := as.State(); got != ASPending {
		t.Errorf("AS state = %v after the last active ASP went inactive, want %v",
			got, ASPending)
	}
}

func TestRemovingASPDoesNotBlockOnNonReadingSibling(t *testing.T) {
	registry := newApplicationServers(time.Hour)
	blocked, _ := asTestConn(t, registry, StateASPInactive, 1)
	failed, _ := asTestConn(t, registry, StateASPActive, 1)

	writeEntered := make(chan struct{})
	writeRelease := make(chan struct{})
	var once sync.Once
	blocked.signalWriter = func(message messages.M3UA) (int, error) {
		if _, ok := message.(*messages.Notify); ok {
			once.Do(func() { close(writeEntered) })
			<-writeRelease
		}
		return message.MarshalLen(), nil
	}
	blocked.notificationQueue = make(chan mandatoryControl, defaultNotificationQueueSize)
	blocked.notificationWriter = blocked.signalWriter
	blocked.signalWriter = nil

	forgotten := make(chan struct{})
	go func() {
		registry.forget(failed)
		close(forgotten)
	}()
	select {
	case <-writeEntered:
	case <-time.After(time.Second):
		close(writeRelease)
		t.Fatal("AS departure did not attempt to notify its sibling")
	}
	select {
	case <-forgotten:
	case <-time.After(100 * time.Millisecond):
		close(writeRelease)
		t.Fatal("AS departure blocked on a non-reading sibling Notify")
	}
	close(writeRelease)
}

func TestFullMandatoryNotifyQueueClosesAssociation(t *testing.T) {
	connection, _ := newTestConn(t, StateASPActive, RoleSGP)
	connection.signalWriter = nil
	connection.notificationQueue = make(chan mandatoryControl, 1)
	// Keep the queue deliberately undrained so the second mandatory event proves
	// the overflow policy rather than racing a worker.
	connection.notificationOnce.Do(func() {})
	notify := func() *messages.Notify {
		return messages.NewNotify(
			params.NewStatus(params.AsStateActive), nil,
			params.NewRoutingContext(1), nil,
		)
	}

	connection.enqueueNotify(notify())
	connection.enqueueNotify(notify())
	select {
	case <-connection.Done():
	case <-time.After(time.Second):
		t.Fatal("association remained open after its mandatory Notify queue overflowed")
	}
	if !errors.Is(connection.Err(), ErrNotificationQueueFull) {
		t.Fatalf("association close error = %v, want ErrNotificationQueueFull", connection.Err())
	}
}

// TestNotifyIsSentOnASStateChange covers RFC 4666 Section 4.3.4.5:
//
//	A Notify message reflecting a change in the AS state MUST be sent to
//	all ASPs in the AS, except those in the ASP-DOWN state, with
//	appropriate Status Information and any ASP Identifier of the failed
//	ASP.
//
// No Notify was ever constructed by non-test code, so this MUST was
// unimplemented in both of its halves: the message and the AS state behind it.
func TestNotifyIsSentOnASStateChange(t *testing.T) {
	reg := newApplicationServers(time.Hour)

	conn, sent := asTestConn(t, reg, StateASPInactive, 1)
	// Entering the AS at all is a change from AS-DOWN to AS-INACTIVE.
	if got := notifies(*sent); len(got) == 0 {
		t.Fatal("no Notify was sent when the AS became AS-INACTIVE")
	}

	before := len(notifies(*sent))
	reg.aspStateChanged(conn, StateASPActive)

	got := notifies(*sent)
	if len(got) <= before {
		t.Fatalf("no Notify was sent when the AS became AS-ACTIVE (got %d)", len(got))
	}
	last := got[len(got)-1]
	typ, info := statusOf(t, last)
	if typ != params.AsStateChange {
		t.Errorf("Status Type = %d, want %d (AS-State_Change)", typ, params.AsStateChange)
	}
	if want := uint16(params.AsStateActive & 0xffff); info != want {
		t.Errorf("Status Information = %d, want %d (AS-ACTIVE)", info, want)
	}
	if last.RoutingContext == nil {
		t.Error("Notify named no Routing Context, so the ASP cannot tell which AS changed")
	}
}

// An ASP in ASP-DOWN is excluded, exactly as the sentence says.
func TestNotifyIsNotSentToAnAspDownPeer(t *testing.T) {
	reg := newApplicationServers(time.Hour)

	active, _ := asTestConn(t, reg, StateASPInactive, 1)
	_, downSent := asTestConn(t, reg, StateASPDown, 1)

	before := len(notifies(*downSent))
	reg.aspStateChanged(active, StateASPActive)

	if got := len(notifies(*downSent)); got != before {
		t.Errorf("an ASP-DOWN peer received %d Notify messages; Section 4.3.4.5 "+
			"excludes \"those in the ASP-DOWN state\"", got-before)
	}
}

// TestLastActiveASPLeavingSendsASPending covers the same rule for the state the
// audit found most conspicuously missing.
func TestLastActiveASPLeavingSendsASPending(t *testing.T) {
	reg := newApplicationServers(time.Hour)

	conn, sent := asTestConn(t, reg, StateASPInactive, 1)
	reg.aspStateChanged(conn, StateASPActive)
	before := len(notifies(*sent))

	reg.aspStateChanged(conn, StateASPInactive)

	got := notifies(*sent)
	if len(got) <= before {
		t.Fatal("no Notify was sent when the last active ASP deactivated")
	}
	typ, info := statusOf(t, got[len(got)-1])
	if typ != params.AsStateChange {
		t.Errorf("Status Type = %d, want %d", typ, params.AsStateChange)
	}
	if want := uint16(params.AsStatePending & 0xffff); info != want {
		t.Errorf("Status Information = %d, want %d (AS-PENDING)", info, want)
	}
}

// TestRecoveryTimerResolvesPending covers T(r) from RFC 4666 Section 4.3.2:
//
//	If T(r) expires before an ASP becomes ASP-ACTIVE, and the SGP has no
//	alternative, the SGP may stop queuing messages and discard all
//	previously queued messages.  The AS will move to the AS-INACTIVE
//	state if at least one ASP is in ASP-INACTIVE; otherwise, it will move
//	to AS-DOWN state.
func TestRecoveryTimerResolvesPending(t *testing.T) {
	reg := newApplicationServers(50 * time.Millisecond)
	as := reg.get(1)

	conn, _ := asTestConn(t, reg, StateASPInactive, 1)
	reg.aspStateChanged(conn, StateASPActive)
	reg.aspStateChanged(conn, StateASPInactive)

	if got := as.State(); got != ASPending {
		t.Fatalf("AS state = %v, want %v", got, ASPending)
	}
	if !waitFor(func() bool { return as.State() == ASInactive }, 2*time.Second) {
		t.Errorf("T(r) expired with one ASP-INACTIVE ASP and the AS is %v, want %v",
			as.State(), ASInactive)
	}
}

// An ASP going active before T(r) expires takes the AS straight back to
// AS-ACTIVE: "If an ASP becomes ASP-ACTIVE before T(r) expires, the AS is moved
// to the AS-ACTIVE state, and all the queued messages will be sent to the ASP."
func TestASPActiveBeforeRecoveryTimerRestoresActive(t *testing.T) {
	reg := newApplicationServers(2 * time.Second)
	as := reg.get(1)

	conn, _ := asTestConn(t, reg, StateASPInactive, 1)
	reg.aspStateChanged(conn, StateASPActive)
	reg.aspStateChanged(conn, StateASPInactive)
	if got := as.State(); got != ASPending {
		t.Fatalf("AS state = %v, want %v", got, ASPending)
	}

	reg.aspStateChanged(conn, StateASPActive)
	if got := as.State(); got != ASActive {
		t.Errorf("AS state = %v after an ASP became active inside T(r), want %v",
			got, ASActive)
	}
}

// TestOverrideDisplacesThePreviousASP covers RFC 4666 Section 4.3.4.3:
//
//	In the case of an Override mode AS, receipt of an ASP Active message
//	at an SGP causes the (re)direction of all traffic for the AS to the
//	ASP that sent the ASP Active message.  Any previously active ASP in
//	the AS is now considered to be in the state ASP-INACTIVE and SHOULD
//	no longer receive traffic from the SGP within the AS.  The SGP or
//	IPSP then MUST send a Notify message ("Alternate ASP_Active") to the
//	previously active ASP in the AS and SHOULD stop traffic to/from that
//	ASP.
//
// None of it happened: handleAspActive treated each association in isolation,
// so an Override-mode AS quietly ran two active ASPs.
func TestOverrideDisplacesThePreviousASP(t *testing.T) {
	reg := newApplicationServers(time.Hour)

	incumbent, incumbentSent := asTestConn(t, reg, StateASPInactive, 1)
	incumbent.cfg.TrafficModeType = params.NewTrafficModeType(params.TrafficModeOverride)
	reg.aspStateChanged(incumbent, StateASPActive)

	challenger, _ := asTestConn(t, reg, StateASPInactive, 1)
	challenger.cfg.TrafficModeType = params.NewTrafficModeType(params.TrafficModeOverride)
	if err := challenger.handleAspUp(messages.NewAspUp(params.NewAspIdentifier(0xABCD), nil)); err != nil {
		t.Fatalf("handleAspUp: %v", err)
	}

	before := len(notifies(*incumbentSent))
	if err := challenger.handleAspActive(messages.NewAspActive(
		params.NewTrafficModeType(params.TrafficModeOverride),
		params.NewRoutingContext(1), nil)); err != nil {
		t.Fatalf("handleAspActive: %v", err)
	}

	got := notifies(*incumbentSent)
	if len(got) <= before {
		t.Fatal("the displaced ASP was never sent a Notify")
	}
	last := got[len(got)-1]
	typ, info := statusOf(t, last)
	if typ != params.Other {
		t.Errorf("Status Type = %d, want %d (Other) — Section 3.8.2 puts "+
			"\"Alternate ASP Active\" under Other, not AS-State_Change",
			typ, params.Other)
	}
	if want := uint16(params.AlternateAspActive & 0xffff); info != want {
		t.Errorf("Status Information = %d, want %d (Alternate ASP Active)", info, want)
	}
	// "The ASP Identifier (if available) of the [overriding ASP]".
	if last.AspIdentifier == nil {
		t.Error("Notify carried no ASP Identifier, so the displaced ASP cannot " +
			"tell which ASP took over")
	} else if got := last.AspIdentifier.AspIdentifier(); got != 0xABCD {
		t.Errorf("ASP Identifier = %#x, want 0xABCD (the overriding ASP's)", got)
	}
}

// In Loadshare mode nobody is displaced: "receipt of an ASP Active message at
// an SGP or IPSP causes direction of traffic to the ASP sending the ASP Active
// message, in addition to all the other ASPs that are active".
func TestLoadshareDoesNotDisplaceAnyASP(t *testing.T) {
	reg := newApplicationServers(time.Hour)

	incumbent, incumbentSent := asTestConn(t, reg, StateASPInactive, 1)
	reg.aspStateChanged(incumbent, StateASPActive)

	challenger, _ := asTestConn(t, reg, StateASPInactive, 1)

	before := len(notifies(*incumbentSent))
	if err := challenger.handleAspActive(messages.NewAspActive(
		params.NewTrafficModeType(params.TrafficModeLoadshare),
		params.NewRoutingContext(1), nil)); err != nil {
		t.Fatalf("handleAspActive: %v", err)
	}

	for _, n := range notifies(*incumbentSent)[before:] {
		if _, info := statusOf(t, n); info == uint16(params.AlternateAspActive&0xffff) {
			t.Error("a Loadshare-mode ASP Active displaced an already-active ASP")
		}
	}
}

// TestASPStateChangesReachTheApplicationServer covers the wiring rather than
// the arithmetic: the AS state is derived from ASP state transitions (RFC 4666
// Section 4.3.2 lists "ASP state transitions" first among the events that
// change it), so the dispatcher applying a transition has to report it.
//
// Without this, every other AS test would still pass while the registry never
// heard from a real association.
func TestASPStateChangesReachTheApplicationServer(t *testing.T) {
	reg := newApplicationServers(time.Hour)
	as := reg.get(1)

	conn, _ := newTestConn(t, StateASPDown, RoleSGP)
	conn.cfg.RoutingContexts = params.NewRoutingContext(1)
	conn.as = reg

	// Drive the state machine the way monitor() does, rather than poking the
	// registry directly.
	if err := conn.handleStateUpdate(StateASPInactive); err != nil {
		t.Fatalf("handleStateUpdate(ASP-INACTIVE): %v", err)
	}
	if got := as.State(); got != ASInactive {
		t.Errorf("AS state = %v after the Association reached ASP-INACTIVE, want %v",
			got, ASInactive)
	}

	if err := conn.handleStateUpdate(StateASPActive); err != nil {
		t.Fatalf("handleStateUpdate(ASP-ACTIVE): %v", err)
	}
	if got := as.State(); got != ASActive {
		t.Errorf("AS state = %v after the Association reached ASP-ACTIVE, want %v",
			got, ASActive)
	}
}

// TestFigure4TransitionsCellByCell walks every transition RFC 4666 Figure 4
// names, by its own label, so the AS state machine is checked against the
// diagram rather than against the prose that surrounds it.
//
//	DN2IA: One ASP moves from ASP-DOWN to ASP-INACTIVE state.
//
//	IA2DN: The last ASP in ASP-INACTIVE moves to ASP-DOWN, causing all
//	the ASPs to be in ASP-DOWN state.
//
//	IA2AC: One ASP moves to ASP-ACTIVE, causing the number of ASPs in the
//	ASP-ACTIVE state to be n.  In a special case of smooth start, this
//	transition MAY be done when the first ASP moves to ASP-ACTIVE state.
//
//	AC2PN: The last ASP in ASP-ACTIVE state moves to ASP-INACTIVE or
//	ASP-DOWN states, causing the number of ASPs in ASP-ACTIVE to drop
//	below 1.
//
//	PN2AC: One ASP moves to ASP-ACTIVE.
//
//	PN2IA: T(r) expiry; an ASP is in ASP-INACTIVE state but no ASPs are
//	in ASP-ACTIVE state.
//
//	PN2DN: T(r) expiry; all the ASPs are in ASP-DOWN state.
//
// The diagram has no edge from AS-ACTIVE to AS-INACTIVE or AS-DOWN: an active
// AS leaves only through AS-PENDING, which is what "an AS keeps the AS-ACTIVE
// state till the last ASP turns to another state different from ASP-ACTIVE,
// avoiding unnecessary traffic disturbances" is about. Those absences are
// asserted too.
func TestFigure4TransitionsCellByCell(t *testing.T) {
	t.Run("DN2IA", func(t *testing.T) {
		reg := newApplicationServers(time.Hour)
		as := reg.get(1)
		c, _ := asTestConn(t, reg, StateASPDown, 1)
		if got := as.State(); got != ASDown {
			t.Fatalf("start state = %v, want %v", got, ASDown)
		}
		reg.aspStateChanged(c, StateASPInactive)
		if got := as.State(); got != ASInactive {
			t.Errorf("DN2IA gave %v, want %v", got, ASInactive)
		}
	})

	t.Run("IA2DN", func(t *testing.T) {
		reg := newApplicationServers(time.Hour)
		as := reg.get(1)
		c, _ := asTestConn(t, reg, StateASPInactive, 1)
		reg.aspStateChanged(c, StateASPDown)
		if got := as.State(); got != ASDown {
			t.Errorf("IA2DN gave %v, want %v — the last ASP-INACTIVE ASP went "+
				"down, so all ASPs are ASP-DOWN", got, ASDown)
		}
	})

	t.Run("IA2AC", func(t *testing.T) {
		reg := newApplicationServers(time.Hour)
		as := reg.get(1)
		c, _ := asTestConn(t, reg, StateASPInactive, 1)
		reg.aspStateChanged(c, StateASPActive)
		if got := as.State(); got != ASActive {
			t.Errorf("IA2AC gave %v, want %v", got, ASActive)
		}
	})

	// AC2PN fires for either destination state the sentence names.
	for _, leaving := range []struct {
		name string
		to   State
	}{
		{"AC2PN via ASP-INACTIVE", StateASPInactive},
		{"AC2PN via ASP-DOWN", StateASPDown},
	} {
		t.Run(leaving.name, func(t *testing.T) {
			reg := newApplicationServers(time.Hour)
			as := reg.get(1)
			c, _ := asTestConn(t, reg, StateASPInactive, 1)
			reg.aspStateChanged(c, StateASPActive)
			reg.aspStateChanged(c, leaving.to)
			if got := as.State(); got != ASPending {
				t.Errorf("%s gave %v, want %v", leaving.name, got, ASPending)
			}
		})
	}

	t.Run("PN2AC", func(t *testing.T) {
		reg := newApplicationServers(time.Hour)
		as := reg.get(1)
		c, _ := asTestConn(t, reg, StateASPInactive, 1)
		reg.aspStateChanged(c, StateASPActive)
		reg.aspStateChanged(c, StateASPInactive)
		if got := as.State(); got != ASPending {
			t.Fatalf("setup: state = %v, want %v", got, ASPending)
		}
		reg.aspStateChanged(c, StateASPActive)
		if got := as.State(); got != ASActive {
			t.Errorf("PN2AC gave %v, want %v", got, ASActive)
		}
	})

	t.Run("PN2IA", func(t *testing.T) {
		reg := newApplicationServers(40 * time.Millisecond)
		as := reg.get(1)
		c, _ := asTestConn(t, reg, StateASPInactive, 1)
		reg.aspStateChanged(c, StateASPActive)
		reg.aspStateChanged(c, StateASPInactive) // one ASP left in ASP-INACTIVE
		if !waitFor(func() bool { return as.State() == ASInactive }, 2*time.Second) {
			t.Errorf("PN2IA gave %v, want %v after T(r) with an ASP-INACTIVE ASP",
				as.State(), ASInactive)
		}
	})

	t.Run("PN2DN", func(t *testing.T) {
		reg := newApplicationServers(40 * time.Millisecond)
		as := reg.get(1)
		c, _ := asTestConn(t, reg, StateASPInactive, 1)
		reg.aspStateChanged(c, StateASPActive)
		reg.aspStateChanged(c, StateASPDown) // no ASP left anywhere but ASP-DOWN
		if !waitFor(func() bool { return as.State() == ASDown }, 2*time.Second) {
			t.Errorf("PN2DN gave %v, want %v after T(r) with every ASP ASP-DOWN",
				as.State(), ASDown)
		}
	})

	t.Run("AS-ACTIVE never leaves except through AS-PENDING", func(t *testing.T) {
		reg := newApplicationServers(time.Hour)
		as := reg.get(1)
		first, _ := asTestConn(t, reg, StateASPInactive, 1)
		second, _ := asTestConn(t, reg, StateASPInactive, 1)
		reg.aspStateChanged(first, StateASPActive)
		reg.aspStateChanged(second, StateASPActive)

		// One of two leaving is not the last, so nothing changes.
		reg.aspStateChanged(second, StateASPDown)
		if got := as.State(); got != ASActive {
			t.Errorf("state = %v after one of two active ASPs went down, want %v: "+
				"an AS \"keeps the AS-ACTIVE state till the last ASP turns to "+
				"another state different from ASP-ACTIVE\"", got, ASActive)
		}

		// The last one leaving goes to AS-PENDING, never straight to
		// AS-INACTIVE or AS-DOWN: Figure 4 has no such edge.
		reg.aspStateChanged(first, StateASPDown)
		if got := as.State(); got != ASPending {
			t.Errorf("state = %v when the last active ASP left, want %v", got, ASPending)
		}
	})
}

// TestASPsForTrafficAppliesTheTrafficMode covers the distribution function of
// RFC 4666 Section 1.4.2.4 — "the SGP must perform a message distribution
// function using information from the received MTP3-User message" — with the
// per-mode rules of Section 4.3.4.3.
//
// Before this there was no way to ask the question at all: an SGP holding two
// ASPs in one Application Server had nothing to select between them, which is
// why the audit could not conclude that a multi-AS SGP routes DATA to the right
// place.
func TestASPsForTrafficAppliesTheTrafficMode(t *testing.T) {
	setup := func(t *testing.T, mode uint32, n int) (*Listener, []*Association) {
		t.Helper()
		l := &Listener{AssociationConfig: newSGPAssociationConfigForTest(
			&HeartbeatInfo{Enabled: false},
			0x111111, 0x222222, 1, mode, 0, 0, []uint32{1}, 3, 2, 1, 0,
		)}
		l.as = newApplicationServers(time.Hour)
		l.as.get(1).setTrafficMode(mode)

		conns := make([]*Association, 0, n)
		for i := 0; i < n; i++ {
			c, _ := newTestConn(t, StateASPInactive, RoleSGP)
			c.cfg.RoutingContexts = params.NewRoutingContext(1)
			c.cfg.ASPIdentifier = params.NewAspIdentifier(uint32(i + 1))
			c.as = l.as
			l.as.aspStateChanged(c, StateASPActive)
			conns = append(conns, c)
		}
		return l, conns
	}

	t.Run("Broadcast sends to every active ASP", func(t *testing.T) {
		l, conns := setup(t, params.TrafficModeBroadcast, 3)
		got := l.ASPsForTraffic(1, 0)
		if len(got) != len(conns) {
			t.Errorf("got %d ASPs, want %d — \"every message is sent to each of "+
				"the active ASPs\"", len(got), len(conns))
		}
	})

	t.Run("Override sends to exactly one", func(t *testing.T) {
		l, _ := setup(t, params.TrafficModeOverride, 3)
		if got := l.ASPsForTraffic(1, 0); len(got) != 1 {
			t.Errorf("got %d ASPs, want 1 — Override (re)directs all traffic to "+
				"a single ASP", len(got))
		}
	})

	t.Run("Loadshare sends to one, and the same one for a given SLS", func(t *testing.T) {
		l, _ := setup(t, params.TrafficModeLoadshare, 3)
		first := l.ASPsForTraffic(1, 7)
		if len(first) != 1 {
			t.Fatalf("got %d ASPs, want 1", len(first))
		}
		for i := 0; i < 20; i++ {
			again := l.ASPsForTraffic(1, 7)
			if len(again) != 1 || again[0] != first[0] {
				t.Fatal("the same SLS chose a different ASP on a later call; " +
					"consecutive messages sharing an SLS would be split across " +
					"ASPs and lose MTP3 sequencing")
			}
		}
	})

	t.Run("Loadshare spreads different SLS values", func(t *testing.T) {
		l, _ := setup(t, params.TrafficModeLoadshare, 3)
		seen := map[*Association]bool{}
		for sls := 0; sls < 12; sls++ {
			got := l.ASPsForTraffic(1, uint8(sls))
			if len(got) == 1 {
				seen[got[0]] = true
			}
		}
		if len(seen) < 2 {
			t.Errorf("every SLS chose the same ASP (%d distinct); loadsharing "+
				"is not spreading", len(seen))
		}
	})

	t.Run("an AS with no active ASP selects nobody", func(t *testing.T) {
		l, conns := setup(t, params.TrafficModeLoadshare, 1)
		l.as.aspStateChanged(conns[0], StateASPInactive)
		if got := l.ASPsForTraffic(1, 0); len(got) != 0 {
			t.Errorf("got %d ASPs for an AS with none active, want 0", len(got))
		}
	})
}
