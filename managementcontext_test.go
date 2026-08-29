// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// RFC 4666 Errata ID 2065 (Section 3.8.2, Technical, Held for Document Update)
// asks for the Routing Context in Notify to be Conditional rather than Optional.
// The submitter's reasoning is this deployment exactly:
//
//	when an ASP is actively handling traffic for multiple Application
//	Servers and a second ASP becomes active for one of those servers, the
//	signaling gateway must send a Notify message containing the specific
//	routing context. This allows the first ASP to become inactive only for
//	that particular Application Server, rather than all of them. Without
//	this routing context information, the ASP cannot distinguish which
//	service association should transition to inactive status.
//
// This library sends it — notifyAlternateASPActive always names a context — and
// then discarded it on receipt: handleNotify read the Status and nothing else.
// Section 4.3.4.5 makes the notification advisory, "not explicitly compel[ling]
// the ASP(s) receiving the message to become active", so the decision belongs to
// the application; it could see that a decision was due but not which Application
// Server it was about.
func TestNotifyReportsWhichApplicationServerItIsAbout(t *testing.T) {
	conn, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)

	// The SGP tells this ASP that another ASP took over Routing Context 2.
	if err := conn.handleNotify(messages.NewNotify(
		params.NewStatus(params.AlternateAspActive),
		params.NewAspIdentifier(0x2a),
		params.NewRoutingContext(2),
		nil,
	)); err != nil {
		t.Fatalf("handleNotify: %v", err)
	}

	ind := <-conn.ManagementIndications()
	if !ind.RoutingContextSet {
		t.Fatal("the Notify named Routing Context 2 and the indication carried " +
			"none; an ASP serving several cannot tell which Application Server " +
			"to stand down for")
	}
	if ind.RoutingContext != 2 {
		t.Errorf("RoutingContext = %d, want 2", ind.RoutingContext)
	}
	// Section 3.8.2 lists the ASP Identifier Conditional, and an "Alternate ASP
	// Active" notification uses it to name the ASP that took over.
	if !ind.ASPIdentifierSet || ind.ASPIdentifier != 0x2a {
		t.Errorf("AspIdentifier = %d (set=%v), want 42", ind.ASPIdentifier, ind.ASPIdentifierSet)
	}
}

// The parameter is not always present, and absence must stay distinguishable
// from Routing Context zero.
func TestNotifyWithoutARoutingContextSaysSo(t *testing.T) {
	conn, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)

	if err := conn.handleNotify(messages.NewNotify(
		params.NewStatus(params.AsStatePending), nil, nil, nil)); err != nil {
		t.Fatalf("handleNotify: %v", err)
	}

	ind := <-conn.ManagementIndications()
	if ind.RoutingContextSet {
		t.Errorf("a Notify carrying no Routing Context reported one (%d)", ind.RoutingContext)
	}
	if ind.ASPIdentifierSet {
		t.Errorf("a Notify carrying no ASP Identifier reported one (%d)", ind.ASPIdentifier)
	}
	// Routing Context 0 is a context like any other.
	zeroConn, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 0)
	if err := zeroConn.handleNotify(messages.NewNotify(
		params.NewStatus(params.AsStatePending), nil, params.NewRoutingContext(0), nil)); err != nil {
		t.Fatalf("handleNotify: %v", err)
	}
	if ind := <-zeroConn.ManagementIndications(); !ind.RoutingContextSet {
		t.Error("Routing Context 0 was reported as absent")
	}
}

// RFC 4666 Section 3.8.1 lists Routing Context, Network Appearance and Affected
// Point Code as "Mandatory*" on Error — "Only mandatory for specific Error
// Codes" — because they are what makes the refusal actionable:
//
//	The "Invalid Routing Context" error is sent if a message is received
//	with an invalid or unconfigured routing context value.
//
//	[Destination Status Unknown] the invalid or unauthorized Point Code(s)
//	MUST be included along with the Network Appearance and/or Routing
//	Context associated with the Point Code(s).
//
//	[Invalid Network Appearance] the invalid (unconfigured) Network
//	Appearance MUST be included in the Network Appearance parameter.
//
// handleError read the Error Code alone, so a peer that went to the trouble of
// saying which context, which network or which destinations was refused was not
// heard: the application learned only that something had been refused.
func TestErrorReportsWhatThePeerRefused(t *testing.T) {
	conn, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)

	if err := conn.handleError(messages.NewError(
		params.NewErrorCode(params.ErrInvalidRoutingContext),
		params.NewRoutingContext(9, 10),
		params.NewNetworkAppearance(7),
		params.NewAffectedPointCodeWithMask(0, 0x123456),
		nil,
	)); err != nil {
		t.Fatalf("handleError: %v", err)
	}

	ind := <-conn.ManagementIndications()
	if !ind.RoutingContextSet || ind.RoutingContext != 9 {
		t.Errorf("RoutingContext = %d (set=%v), want 9; the peer said which "+
			"context it refused", ind.RoutingContext, ind.RoutingContextSet)
	}
	if !equalNotifyScope(ind.RoutingContexts, []uint32{9, 10}) {
		t.Errorf("RoutingContexts = %v, want [9 10]", ind.RoutingContexts)
	}
	if !ind.NetworkAppearanceSet || ind.NetworkAppearance != 7 {
		t.Errorf("NetworkAppearance = %d (set=%v), want 7",
			ind.NetworkAppearance, ind.NetworkAppearanceSet)
	}
	if len(ind.AffectedPointCodes) != 1 || ind.AffectedPointCodes[0] != 0x123456 {
		t.Errorf("AffectedPointCodes = %#v, want [0x123456]", ind.AffectedPointCodes)
	}
	if ind.ErrorCode != params.ErrInvalidRoutingContext {
		t.Errorf("ErrorCode = %#x, want %#x", ind.ErrorCode, params.ErrInvalidRoutingContext)
	}
}

// An Error carrying only the code — which most codes permit — must report the
// rest as absent rather than as zeros a caller would read as real values.
func TestErrorWithoutTheOptionalContextSaysSo(t *testing.T) {
	conn, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)

	if err := conn.handleError(messages.NewError(
		params.NewErrorCode(params.UnexpectedMessageError), nil, nil, nil, nil)); err != nil {
		t.Fatalf("handleError: %v", err)
	}

	ind := <-conn.ManagementIndications()
	if ind.RoutingContextSet {
		t.Errorf("reported Routing Context %d for an Error that named none", ind.RoutingContext)
	}
	if ind.NetworkAppearanceSet {
		t.Errorf("reported Network Appearance %d for an Error that named none", ind.NetworkAppearance)
	}
	if len(ind.AffectedPointCodes) != 0 {
		t.Errorf("reported point codes %v for an Error that named none", ind.AffectedPointCodes)
	}
}

// A malformed Routing Context is not the same as an omitted one. It must be
// rejected before an ambiguous indication reaches Layer Management.
func TestManagementRoutingContextOfAnOddLength(t *testing.T) {
	conn, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)

	for _, data := range [][]byte{{}, {0x00, 0x00, 0x09}, {0x00, 0x00, 0x00, 0x09, 0x00}} {
		err := conn.handleNotify(messages.NewNotify(
			params.NewStatus(params.AsStatePending), nil,
			params.NewParam(int(params.RoutingContext), data), nil))
		if !errors.Is(err, ErrInvalidRoutingContext) {
			t.Fatalf("handleNotify error = %v for %d value octets, want ErrInvalidRoutingContext",
				err, len(data))
		}
		if len(conn.mgmtChan) != 0 {
			t.Errorf("a malformed %d-octet Routing Context reached Layer Management", len(data))
		}
	}
}

// RFC 4666 Section 4.3.4.3 makes an overridden ASP "consider itself now in the
// ASP-INACTIVE state". Errata ID 2065 is about how far that reaches: a Notify
// naming one Routing Context out of several lets the ASP "become inactive only
// for that particular Application Server, rather than all of them".
//
// The override was applied without reading the Routing Context at all, so an ASP
// serving two Application Servers and overridden on one of them stood the whole
// association down and stopped carrying traffic for the Application Server it
// was still the active ASP for.
func TestAnOverrideReachesOnlyTheApplicationServersItNames(t *testing.T) {
	t.Run("a partial override leaves the association carrying the rest", func(t *testing.T) {
		asp, _ := newTestConnWithContexts(t, StateASPInactive, RoleASP, 1, 2)
		// Override is the mode an "Alternate ASP Active" notification arises in
		// (Section 3.7.1: "the ASP takes over all traffic in an Application
		// Server"), and the Ack's mode is checked against the configured one.
		asp.cfg.TrafficModeType = params.NewTrafficModeType(params.TrafficModeOverride)
		if err := asp.handleAspActiveAck(messages.NewAspActiveAck(
			params.NewTrafficModeType(params.TrafficModeOverride),
			params.NewRoutingContext(1, 2), nil)); err != nil {
			t.Fatalf("handleAspActiveAck: %v", err)
		}
		asp.setState(StateASPActive)

		// Another ASP takes over Routing Context 2 only.
		notify := messages.NewNotify(params.NewStatus(params.AlternateAspActive),
			params.NewAspIdentifier(7), params.NewRoutingContext(2), nil)
		if !asp.overriddenByAlternateAsp(notify) {
			t.Fatal("an Alternate ASP Active Notify was not recognised as an override")
		}
		if asp.overrideScope(notify) {
			t.Error("an override naming 1 of 2 Routing Contexts stood the whole " +
				"association down; the Application Server nobody overrode loses its ASP")
		}

		// Traffic for the overridden context stops...
		if _, err := asp.routingContextFor(2); !errors.Is(err, ErrRoutingContextNotActive) {
			t.Errorf("writing for the overridden context = %v, want ErrRoutingContextNotActive", err)
		}
		// ...and traffic for the other one does not.
		if _, err := asp.routingContextFor(1); err != nil {
			t.Errorf("writing for the Routing Context nobody overrode was refused: %v", err)
		}
	})

	t.Run("the last usable context determines association state", func(t *testing.T) {
		for _, test := range []struct {
			name      string
			solicited bool
		}{
			{name: "solicited ASP Inactive Ack", solicited: true},
			{name: "unsolicited ASP Inactive Ack"},
		} {
			t.Run(test.name, func(t *testing.T) {
				asp, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
				asp.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
				asp.noteRoutingContextsOverridden([]uint32{1})
				if test.solicited {
					asp.startTAck(
						messages.NewAspInactive(params.NewRoutingContext(2), nil),
						requestAspInactive,
					)
				}

				asp.handleSignals(context.Background(),
					messages.NewAspInactiveAck(params.NewRoutingContext(2), nil))

				if got := asp.State(); got != StateASPInactive {
					t.Fatalf("state with only overridden RC 1 remaining = %v, want ASP-INACTIVE", got)
				}
				if got := asp.resumeAfterStrayAck(); got != !test.solicited {
					t.Fatalf("resume after stray Ack = %t, want %t", got, !test.solicited)
				}
			})
		}
	})

	t.Run("an override covering every context stands the association down", func(t *testing.T) {
		asp, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
		notify := messages.NewNotify(params.NewStatus(params.AlternateAspActive),
			nil, params.NewRoutingContext(1, 2), nil)
		if !asp.overrideScope(notify) {
			t.Error("an override naming every configured Routing Context did not " +
				"move the association to ASP-INACTIVE")
		}
	})

	t.Run("an override naming no context covers every configured AS", func(t *testing.T) {
		asp, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
		notify := messages.NewNotify(params.NewStatus(params.AlternateAspActive), nil, nil, nil)
		if !asp.overrideScope(notify) {
			t.Error("a contextless override did not cover every configured AS")
		}
	})

	t.Run("an explicit single-context override stands the association down", func(t *testing.T) {
		asp, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
		notify := messages.NewNotify(params.NewStatus(params.AlternateAspActive),
			nil, params.NewRoutingContext(1), nil)
		if !asp.overrideScope(notify) {
			t.Error("the only configured context was overridden and the association stayed up")
		}
	})

	t.Run("a contextless single-AS override is unambiguous", func(t *testing.T) {
		asp, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
		notify := messages.NewNotify(params.NewStatus(params.AlternateAspActive), nil, nil, nil)
		if !asp.overrideScope(notify) {
			t.Error("the only configured Application Server was not overridden")
		}
	})

	// A fresh activation decides again which contexts this ASP may carry.
	t.Run("re-activation clears the override", func(t *testing.T) {
		asp, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
		asp.noteRoutingContextsOverridden([]uint32{2})
		if !asp.routingContextOverridden(2) {
			t.Fatal("the override was not recorded")
		}
		asp.forgetAckedRoutingContexts()
		if asp.routingContextOverridden(2) {
			t.Error("the override outlived the activation it was recorded against")
		}
	})
}

// The scope decision has to be wired into the dispatcher, which is what
// publishes state. Testing overrideScope alone leaves the call site free to be
// deleted, and the defect this fixes lives at the call site.
func TestTheDispatcherAppliesTheOverrideScope(t *testing.T) {
	activeASP := func(t *testing.T) *Association {
		t.Helper()
		asp, _ := newTestConnWithContexts(t, StateASPInactive, RoleASP, 1, 2)
		asp.cfg.TrafficModeType = params.NewTrafficModeType(params.TrafficModeOverride)
		if err := asp.handleAspActiveAck(messages.NewAspActiveAck(
			params.NewTrafficModeType(params.TrafficModeOverride),
			params.NewRoutingContext(1, 2), nil)); err != nil {
			t.Fatalf("handleAspActiveAck: %v", err)
		}
		asp.setState(StateASPActive)
		// Drain what the Ack published so the next read is the Notify's.
		for len(asp.stateChan) > 0 {
			<-asp.stateChan
		}
		return asp
	}

	t.Run("a partial override does not move the association state", func(t *testing.T) {
		asp := activeASP(t)
		asp.handleSignals(context.Background(), messages.NewNotify(
			params.NewStatus(params.AlternateAspActive),
			params.NewAspIdentifier(7), params.NewRoutingContext(2), nil))

		select {
		case got := <-asp.stateChan:
			if got != stateUnchanged {
				t.Errorf("published %v for an override of 1 of 2 Routing Contexts, "+
					"want the state held; the Application Server nobody overrode "+
					"loses its ASP", got)
			}
		default:
			t.Fatal("the dispatcher published no state at all")
		}
	})

	t.Run("a total override moves it to ASP-INACTIVE", func(t *testing.T) {
		asp := activeASP(t)
		asp.handleSignals(context.Background(), messages.NewNotify(
			params.NewStatus(params.AlternateAspActive),
			params.NewAspIdentifier(7), params.NewRoutingContext(1, 2), nil))

		select {
		case got := <-asp.stateChan:
			if got != StateASPInactive {
				t.Errorf("published %v for an override of every Routing Context, "+
					"want %v (Section 4.3.4.3)", got, StateASPInactive)
			}
		default:
			t.Fatal("the dispatcher published no state at all")
		}
	})
}

// The "set" flags mean the value was decoded, not that some parameter occupied
// the slot. A parameter carrying the wrong tag decodes to zero, which is a real
// value for every one of these fields, so reporting it as present would hand the
// caller a zero it cannot tell from a genuine one.
func TestAMismatchedParameterIsReportedAsAbsent(t *testing.T) {
	conn, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)

	// An INFO String sitting where the ASP Identifier belongs. Parse assigns by
	// tag so the wire cannot produce this, but a hand-built message can — and
	// did, in an earlier draft of this file's own Error test.
	n := messages.NewNotify(params.NewStatus(params.AsStatePending), nil, nil, nil)
	n.AspIdentifier = params.NewInfoString("not an ASP identifier")

	if err := conn.handleNotify(n); err != nil {
		t.Fatalf("handleNotify: %v", err)
	}
	ind := <-conn.ManagementIndications()
	if ind.ASPIdentifierSet {
		t.Errorf("a parameter tagged %#x was reported as ASP Identifier %d",
			n.AspIdentifier.Tag, ind.ASPIdentifier)
	}

	e := messages.NewError(params.NewErrorCode(params.UnexpectedMessageError), nil, nil, nil, nil)
	e.NetworkAppearance = params.NewInfoString("not a network appearance")
	if err := conn.handleError(e); err != nil {
		t.Fatalf("handleError: %v", err)
	}
	if ind := <-conn.ManagementIndications(); ind.NetworkAppearanceSet {
		t.Errorf("a parameter tagged %#x was reported as Network Appearance %d",
			e.NetworkAppearance.Tag, ind.NetworkAppearance)
	}
}
