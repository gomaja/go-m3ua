// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// TestASPTMFromAspDownIsRefused covers Figure 3, the "ASP State Transition
// Diagram, per AS" of RFC 4666 Section 4.3.1. The only edge leaving ASP-DOWN is
//
//	ASPUP/[ASPUP-Ack]
//
// to ASP-INACTIVE. There is no ASP-DOWN to ASP-ACTIVE edge at all, so an ASP
// Active arriving while the peer is ASP-DOWN names a transition the diagram
// does not define.
//
// Section 4.3.1 also forbids the reply: an ASP-DOWN peer "SHOULD NOT be sent
// any M3UA messages, with the exception of Heartbeat, ASP Down Ack, and Error
// messages", and an ASP Active Ack is none of those. The SGP nevertheless wrote
// the Ack first and only then reported the state error, so a peer that had
// never sent an ASP Up was acknowledged into carrying traffic.
func TestASPTMFromAspDownIsRefused(t *testing.T) {
	for _, tt := range []struct {
		name    string
		send    func(*Conn) error
		wantAck string
	}{
		{"ASP Active", func(c *Conn) error {
			return c.handleAspActive(messages.NewAspActive(
				params.NewTrafficModeType(params.TrafficModeLoadshare),
				params.NewRoutingContext(1), nil))
		}, "ASP Active Ack"},
		{"ASP Inactive", func(c *Conn) error {
			return c.handleAspInactive(messages.NewAspInactive(
				params.NewRoutingContext(1), nil))
		}, "ASP Inactive Ack"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			sgp, sent := newTestConn(t, StateAspDown, modeServer)

			err := tt.send(sgp)
			if err == nil {
				t.Fatal("accepted an ASPTM message from a peer in ASP-DOWN")
			}
			var unexpected *UnexpectedMessageError
			if !errors.As(err, &unexpected) {
				t.Errorf("error = %v (%T), want an UnexpectedMessageError", err, err)
			}
			for _, name := range typeNames(*sent) {
				if name == tt.wantAck {
					t.Errorf("sent a %s to a peer in ASP-DOWN; Section 4.3.1 "+
						"permits only Heartbeat, ASP Down Ack and Error there", name)
				}
			}
		})
	}
}

// The dispatcher must not publish the transition either, or the Conn ends up in
// ASP-ACTIVE for a peer that never sent an ASP Up.
func TestAspActiveFromAspDownDoesNotReachAspActive(t *testing.T) {
	sgp, _ := newTestConn(t, StateAspDown, modeServer)

	sgp.handleSignals(context.Background(), messages.NewAspActive(
		params.NewTrafficModeType(params.TrafficModeLoadshare),
		params.NewRoutingContext(1), nil))

	select {
	case got := <-sgp.stateChan:
		if got == StateAspActive {
			t.Error("an ASP Active from ASP-DOWN drove the Conn to ASP-ACTIVE, " +
				"a transition Figure 3 does not define")
		}
	default:
	}
}

// From ASP-INACTIVE the same message is the defined transition and still works.
func TestAspActiveFromAspInactiveStillActivates(t *testing.T) {
	sgp, sent := newTestConn(t, StateAspInactive, modeServer)

	if err := sgp.handleAspActive(messages.NewAspActive(
		params.NewTrafficModeType(params.TrafficModeLoadshare),
		params.NewRoutingContext(1), nil)); err != nil {
		t.Fatalf("ASP Active from ASP-INACTIVE was refused: %v", err)
	}
	if got := typeNames(*sent); len(got) != 1 || got[0] != "ASP Active Ack" {
		t.Errorf("sent %v, want [ASP Active Ack]", got)
	}
}

// TestUnexpectedAspUpAckReturnsToThePreviousState covers RFC 4666 Section
// 4.3.4.1:
//
//	If the ASP receives an unexpected ASP Up Ack message, the ASP should
//	consider itself in the ASP-INACTIVE state.  If the ASP was not in the
//	ASP-INACTIVE state, it SHOULD send an Error message and then initiate
//	procedures to return itself to its previous state.
//
// The Error was sent and the drop to ASP-INACTIVE happened, but the third
// clause did not: the ASP sat in ASP-INACTIVE indefinitely, carrying no traffic,
// because the entry action only re-activates when arriving from ASP-DOWN.
//
// That guard exists for a real reason — coming back from ASP-ACTIVE because the
// peer deliberately took traffic away, with an ASP Inactive or an "Alternate
// ASP Active" Notify, must not fight the peer's decision. A stray ASP Up Ack is
// not the peer taking traffic away, so the two causes have to be told apart.
func TestUnexpectedAspUpAckReturnsToThePreviousState(t *testing.T) {
	asp, _ := newTestConn(t, StateAspActive, modeClient)

	if err := asp.handleAspUpAck(messages.NewAspUpAck(nil, nil)); err == nil {
		t.Fatal("an ASP Up Ack while ASP-ACTIVE was accepted silently")
	}

	// Entering ASP-INACTIVE from ASP-ACTIVE because of that stray Ack must
	// re-initiate, unlike the peer-driven route into the same state.
	if !asp.resumeAfterStrayAck() {
		t.Error("the ASP did not arm the return to its previous state")
	}
}

// An Ack answering an ASP Inactive this node actually sent is the deliberate
// deactivation path. Unlike an unsolicited Ack, it must not fight the request
// by immediately activating again.
func TestSolicitedAspInactiveAckDoesNotReactivate(t *testing.T) {
	asp, _ := newTestConn(t, StateAspActive, modeClient)
	asp.startTAck(messages.NewAspInactive(asp.cfg.RoutingContexts.Copy(), nil), requestAspInactive)

	if err := asp.handleAspInactiveAck(messages.NewAspInactiveAck(asp.cfg.RoutingContexts.Copy(), nil)); err != nil {
		t.Fatalf("handleAspInactiveAck: %v", err)
	}
	if asp.resumeAfterStrayAck() {
		t.Error("a solicited ASP Inactive Ack armed an unwanted re-activation")
	}
}

// TestSecondAspActiveAckIsAccepted covers the per-Routing-Context nature of
// activation in RFC 4666 Section 4.3.4.3, where the SGP acknowledges the
// contexts it can serve and may answer separately for others.
//
// Requiring ASP-INACTIVE meant the first Ack moved the Conn to ASP-ACTIVE and
// every later one was rejected as unexpected, so an SGP acknowledging a second
// Routing Context had its message thrown away.
func TestSecondAspActiveAckIsAccepted(t *testing.T) {
	asp, _ := newTestConnWithContexts(t, StateAspInactive, modeClient, 1, 2)

	if err := asp.handleAspActiveAck(messages.NewAspActiveAck(
		params.NewTrafficModeType(params.TrafficModeLoadshare),
		params.NewRoutingContext(1), nil)); err != nil {
		t.Fatalf("first ASP Active Ack: %v", err)
	}
	asp.setState(StateAspActive)

	if err := asp.handleAspActiveAck(messages.NewAspActiveAck(
		params.NewTrafficModeType(params.TrafficModeLoadshare),
		params.NewRoutingContext(2), nil)); err != nil {
		t.Fatalf("a second ASP Active Ack, for another Routing Context, was "+
			"rejected: %v", err)
	}
}

// TestDataOnlyFlowsForAnAcknowledgedRoutingContext covers RFC 4666 Section
// 4.3.4.3, where the SGP acknowledges what it can serve:
//
//	For the Application Servers for which the ASP can be activated, the
//	SGP responds with an ASP Active Ack message
//
// Activation is per Routing Context, but the Conn tracked one state for the
// whole association, so a partial Ack took everything active and DATA went out
// for contexts the SGP had never agreed to carry.
func TestDataOnlyFlowsForAnAcknowledgedRoutingContext(t *testing.T) {
	asp, _ := newTestConnWithContexts(t, StateAspInactive, modeClient, 1, 2)

	// Only context 1 is acknowledged.
	if err := asp.handleAspActiveAck(messages.NewAspActiveAck(
		params.NewTrafficModeType(params.TrafficModeLoadshare),
		params.NewRoutingContext(1), nil)); err != nil {
		t.Fatalf("handleAspActiveAck: %v", err)
	}
	asp.setState(StateAspActive)

	if err := asp.SelectRoutingContext(1); err != nil {
		t.Fatalf("SelectRoutingContext(1): %v", err)
	}
	if _, err := asp.dataRoutingContext(); err != nil {
		t.Errorf("DATA refused for the acknowledged Routing Context: %v", err)
	}

	if err := asp.SelectRoutingContext(2); err != nil {
		t.Fatalf("SelectRoutingContext(2): %v", err)
	}
	if _, err := asp.dataRoutingContext(); err == nil {
		t.Error("DATA would go out for a Routing Context the SGP never " +
			"acknowledged in an ASP Active Ack")
	}
}

// TestTrafficModeTypeOutsideTheDefinedValuesIsRejected covers the Traffic Mode
// Type table of RFC 4666 Section 3.7.1, whose only defined values are
//
//	1  Override
//	2  Loadshare
//	3  Broadcast
//
// Section 4.3.4.3 gives the answer for anything else: "If the SGP determines
// that the mode indicated in an ASP Active message is unsupported or
// incompatible with the mode currently configured for the AS, the SGP responds
// with an Error message ('Unsupported / Invalid Traffic Handling Mode')."
//
// The check only compared the peer's value against a locally configured one and
// short-circuited to success when none was configured, so an undefined mode was
// acknowledged as though it had been agreed.
func TestTrafficModeTypeOutsideTheDefinedValuesIsRejected(t *testing.T) {
	for _, mode := range []uint32{0, 4, 99} {
		sgp, _ := newTestConn(t, StateAspInactive, modeServer)
		sgp.cfg.TrafficModeType = nil // nothing configured locally

		err := sgp.validateTrafficMode(params.NewTrafficModeType(mode))
		if err == nil {
			t.Errorf("Traffic Mode Type %d was accepted; Section 3.7.1 defines "+
				"only 1, 2 and 3", mode)
		}
	}
}

// Each defined value is still accepted when nothing is configured locally.
func TestDefinedTrafficModeTypesAreAccepted(t *testing.T) {
	for _, mode := range []uint32{
		params.TrafficModeOverride,
		params.TrafficModeLoadshare,
		params.TrafficModeBroadcast,
	} {
		sgp, _ := newTestConn(t, StateAspInactive, modeServer)
		sgp.cfg.TrafficModeType = nil

		if err := sgp.validateTrafficMode(params.NewTrafficModeType(mode)); err != nil {
			t.Errorf("Traffic Mode Type %d was rejected: %v", mode, err)
		}
	}
}

// TestUnchangedStateDoesNotRerunAnEntryAction pins the reason a handler that
// moves nothing publishes stateUnchanged rather than re-reading State().
//
// The dispatcher and the state machine run on different goroutines with a queue
// between them, so State() at dispatch time is the last state *applied*, not the
// last one *published*. A message handled while an earlier transition was still
// queued republished the state that transition had already superseded, and
// applying the stale value re-ran that state's entry action. For ASP-DOWN that
// meant sending ASP Up again on an association that had already reached
// ASP-ACTIVE — which RFC 4666 Section 4.3.4.1 has the SGP answer by dropping the
// ASP back to ASP-INACTIVE, so a healthy association went inactive with no
// message having gone wrong. It showed up as an oscillation once the Notify
// traffic of Section 4.3.4.5 gave the dispatcher more messages to handle.
func TestUnchangedStateDoesNotRerunAnEntryAction(t *testing.T) {
	conn, sent := newTestConn(t, StateAspDown, modeClient)

	// Entering ASP-DOWN runs the entry action, which starts the handshake.
	if err := conn.handleStateUpdate(StateAspDown); err != nil {
		t.Fatalf("handleStateUpdate(ASP-DOWN): %v", err)
	}
	afterEntry := len(typeNames(*sent))
	if afterEntry == 0 {
		t.Fatal("entering ASP-DOWN sent nothing; the entry action did not run")
	}

	// A handled message that changed nothing must not re-run it.
	if err := conn.handleStateUpdate(stateUnchanged); err != nil {
		t.Fatalf("handleStateUpdate(stateUnchanged): %v", err)
	}
	if got := len(typeNames(*sent)); got != afterEntry {
		t.Errorf("publishing stateUnchanged sent %d more signals (%v); it must "+
			"re-run no entry action", got-afterEntry, typeNames(*sent))
	}

	// And it must not disturb the recorded state.
	if got := conn.State(); got != StateAspDown {
		t.Errorf("state = %v after stateUnchanged, want %v", got, StateAspDown)
	}
}

// The same value must not be reported into the Application Server either: it is
// not an ASP state, so an AS derived from it would be nonsense.
func TestUnchangedStateIsNotReportedToTheApplicationServer(t *testing.T) {
	reg := newApplicationServers(time.Hour)
	as := reg.get(1)

	conn, _ := newTestConn(t, StateAspInactive, modeServer)
	conn.cfg.RoutingContexts = params.NewRoutingContext(1)
	conn.as = reg

	if err := conn.handleStateUpdate(StateAspActive); err != nil {
		t.Fatalf("handleStateUpdate: %v", err)
	}
	if got := as.State(); got != ASActive {
		t.Fatalf("AS state = %v, want %v", got, ASActive)
	}

	if err := conn.handleStateUpdate(stateUnchanged); err != nil {
		t.Fatalf("handleStateUpdate(stateUnchanged): %v", err)
	}
	if got := as.State(); got != ASActive {
		t.Errorf("AS state = %v after stateUnchanged, want it untouched at %v",
			got, ASActive)
	}
}

// TestSolicitedAspUpAckIsAbsorbed covers the difference between an
// acknowledgement this node asked for and one it did not.
//
// RFC 4666 Section 4.3.4.1 attaches consequences only to the second: "If the ASP
// receives an unexpected ASP Up Ack message, the ASP should consider itself in
// the ASP-INACTIVE state." T(ack) resends an unacknowledged ASP Up, so under
// load the association can already have climbed to ASP-ACTIVE by the time the
// retransmission is answered. Treating that answer as unexpected made the two
// ends oscillate: this node dropped to ASP-INACTIVE and re-activated, the SGP
// dropped it again on the next retransmission, and so on.
func TestSolicitedAspUpAckIsAbsorbed(t *testing.T) {
	conn, sent := newTestConn(t, StateAspActive, modeClient)

	// An ASP Up is outstanding: this is the retransmission's answer.
	conn.startTAck(messages.NewAspUp(conn.cfg.AspIdentifier, nil), requestAspUp)

	if err := conn.handleAspUpAck(messages.NewAspUpAck(nil, nil)); err != nil {
		t.Errorf("an ASP Up Ack answering our own ASP Up was reported as an "+
			"error: %v", err)
	}
	if conn.resumeAfterStrayAck() {
		t.Error("a solicited ASP Up Ack armed the stray-acknowledgement return")
	}
	if got := typeNames(*sent); len(got) != 0 {
		t.Errorf("a solicited ASP Up Ack drew %v; it should be absorbed", got)
	}
}

// With nothing outstanding the Ack really is unexpected, and Section 4.3.4.1
// applies in full.
func TestUnsolicitedAspUpAckStillDropsToInactive(t *testing.T) {
	conn, _ := newTestConn(t, StateAspActive, modeClient)

	err := conn.handleAspUpAck(messages.NewAspUpAck(nil, nil))
	if err == nil {
		t.Fatal("an ASP Up Ack nothing asked for was accepted silently")
	}
	var unexpected *UnexpectedMessageError
	if !errors.As(err, &unexpected) {
		t.Errorf("error = %T, want *UnexpectedMessageError", err)
	}
	if !conn.resumeAfterStrayAck() {
		t.Error("the return to the previous state was not armed")
	}
}

// The SGP must not refuse an ASP Active that arrives in the window between its
// own ASP Up Ack going out and its state transition being applied.
//
// handleSignals writes the Ack from inside handleAspUp and only afterwards
// publishes ASP-INACTIVE, which a separate goroutine applies. On loopback the
// ASP receives the Ack and sends ASP Active inside that window; the dispatcher
// then reads a state that is still ASP-DOWN and refuses ASPTM with "Unexpected
// Message" (Section 4.3.1 allows an ASP-DOWN peer only Heartbeat, ASP Down Ack
// and Error). The ASP resends on T(ack), is refused each time, and both ends
// give up at their establish budget -- observed as roughly one failed
// establishment in twelve when three ASPs come up at once.
//
// The window is reproduced here exactly, and without timing: newTestConn's
// stateChan is buffered and nothing drains it, so after the first handleSignals
// the transition is published but unapplied, which is the same situation the
// dispatcher is in when it picks up the next message.
func TestAspActiveInTheAckWindowIsNotRefused(t *testing.T) {
	conn, sent := newTestConn(t, StateAspDown, modeServer)
	ctx := context.Background()

	conn.handleSignals(ctx, messages.NewAspUp(nil, nil))

	// Deliberately no drain of stateChan here: that is the defect's window.
	conn.handleSignals(ctx, messages.NewAspActive(
		conn.cfg.TrafficModeType, conn.cfg.RoutingContexts, nil))

	names := typeNames(*sent)
	var sawActiveAck bool
	for _, n := range names {
		if n == "ASP Active Ack" {
			sawActiveAck = true
		}
	}
	if !sawActiveAck {
		t.Errorf("no ASP Active Ack; the ASP Active was refused inside the Ack window (sent: %v)", names)
	}
	// The refusal itself lands on errChan, not in the captured signals: the
	// dispatcher publishes the error and monitor() is what turns it into an ERR
	// on the wire. Asserting on the captured signals alone would be asserting
	// on something this harness can never produce.
	select {
	case err := <-conn.errChan:
		var unexpected *UnexpectedMessageError
		if errors.As(err, &unexpected) {
			t.Errorf("ASP Active refused with %v inside the Ack window; on the wire this is "+
				"ERR 0x06 and the peer resends on T(ack) until it gives up", err)
		} else {
			t.Errorf("unexpected error published for ASP Active in the Ack window: %v", err)
		}
	default:
		// Nothing published: the message was accepted, which is the point.
	}
}

// setState places a Conn in a state without going through the dispatcher, and
// it has to leave the bookkeeping the next transition reads consistent with the
// value it wrote.
//
// The transition that follows works out whether it is entering a state, and
// which state it came from, from appliedState rather than from state -- state
// having already been committed by sendState by then. A setState that moved
// only one of the two would make the next transition measure itself against a
// state the Conn had long since left. Here that would look like a client that
// was placed in ASP-ACTIVE, told to go ASP-INACTIVE, and then re-activated
// itself off its own back, taking traffic the peer had just removed.
//
// The first update has a seed that reads state directly, so this drives one
// transition beforehand; without it the seed would hide the difference.
func TestSetStateKeepsTheTransitionBookkeepingConsistent(t *testing.T) {
	asp, sent := newTestConn(t, StateAspDown, modeClient)

	// Get past the first update, so the seed no longer applies and the next
	// transition genuinely consults appliedState.
	if err := asp.handleStateUpdate(StateAspDown); err != nil {
		t.Fatalf("handleStateUpdate(ASP-DOWN): %v", err)
	}
	before := len(*sent)

	asp.setState(StateAspActive)

	if err := asp.handleStateUpdate(StateAspInactive); err != nil {
		t.Fatalf("handleStateUpdate(ASP-INACTIVE): %v", err)
	}

	for _, m := range (*sent)[before:] {
		if m.MessageTypeName() == "ASP Active" {
			t.Errorf("the ASP re-activated itself after being moved ASP-INACTIVE from ASP-ACTIVE; "+
				"the transition read a stale previous state (sent since: %v)",
				typeNames((*sent)[before:]))
		}
	}
}
