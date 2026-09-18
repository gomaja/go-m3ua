// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// SSNM carries the reachability of SS7 destinations beyond the peer. Acting on
// it wrongly is what takes traffic down in production: a missed DUNA keeps an
// MTP3-User pumping messages at an unreachable point code, and a spurious one
// stops traffic that should be flowing. These messages carry no acknowledgement,
// so the only observable effects are the destination state and the status
// channel.

// ssnmConn builds an ASP in ASP-ACTIVE, the only state in which SSNM may be
// acted on (RFC 4666 Section 4.3.1).
func ssnmConn(t *testing.T) (*Association, *[]messages.M3UA) {
	t.Helper()
	return newSSNMTestConn(t, StateASPActive, RoleASP)
}

// newSSNMTestConn gives legacy SSNM tests a dedicated single-flow association.
// Those tests exercise destination behavior rather than Routing Context
// conditionality; multi-flow cases belong in ssnm_scope_test.go and always name
// their scope explicitly.
func newSSNMTestConn(t *testing.T, state State, role Role) (*Association, *[]messages.M3UA) {
	t.Helper()
	conn, sent := newTestConn(t, state, role)
	setInventoryRoutingContexts(&conn.cfg.ApplicationServers, params.NewRoutingContext(1))
	return conn, sent
}

// apc is shorthand for an Affected Point Code parameter.
func apc(pcs ...uint32) *params.Param {
	return params.NewAffectedPointCode(pcs...)
}

// seedDestinationNetworkState records both dimensions of a destination in an
// Association's retained state. Availability and congestion are separate
// statements (RFC 4666 Section 4.5.2.2) and are stored as separate records, so
// seeding a congested destination takes one of each.
func seedDestinationNetworkState(c *Association, pointCode uint32, state DestinationNetworkState) {
	scope := associationDestinationScope(c, nil)
	rangeValue := DestinationRange{
		NetworkAppearance:    scope.networkAppearance,
		NetworkAppearanceSet: scope.networkAppearanceSet,
		RoutingContext:       scope.routingContext,
		RoutingContextSet:    scope.routingContextSet,
		PointCode:            pointCode,
		State:                state,
	}
	c.destinations.setRanges([]DestinationRange{rangeValue})
	_ = c.destinations.setCongestionRangesWithinBudget([]DestinationRange{rangeValue})
}

// retainedSnapshot is the exact, Mask-zero destination records an Association
// holds in the scope its own queries resolve, optionally in an explicit Network
// Appearance.
func retainedSnapshot(c *Association, networkAppearance *params.Param) map[uint32]DestinationNetworkState {
	return c.destinations.snapshotForScope(associationDestinationScope(c, networkAppearance))
}

// retainedRanges is every range an Association holds in that same scope,
// including the ones a Mask-zero snapshot cannot represent.
func retainedRanges(c *Association) []DestinationRange {
	return c.destinations.rangesForScope(associationDestinationScope(c, nil))
}

// nextStatus returns the next SSNM status, or fails if none arrives.
func nextStatus(t *testing.T, c *Association) *DestinationStatus {
	t.Helper()

	select {
	case s := <-c.SignallingStatus():
		return s
	case <-time.After(2 * time.Second):
		t.Fatal("no SSNM status delivered to the user")
		return nil
	}
}

// RFC 4666 Section 3.4.1: DUNA tells the ASP that destinations are unreachable
// and "the MTP3-User at the ASP is expected to stop traffic to the affected
// destination via the SG". Before this was implemented, every SSNM message hit
// the default dispatch arm and was answered with "Unsupported Message", so an
// SG could not tell an ASP that a destination had gone away.
func TestDUNAMarksDestinationUnavailable(t *testing.T) {
	conn, sent := ssnmConn(t)

	if err := conn.handleDestinationUnavailable(
		messages.NewDestinationUnavailable(nil, nil, apc(0x1234), nil)); err != nil {
		t.Fatalf("handleDestinationUnavailable() error = %v, want nil", err)
	}

	if got := retainedAvailability(conn, 0x1234); got != DestinationUnavailable {
		t.Errorf("retained availability for 0x1234 = %v, want %v", got, DestinationUnavailable)
	}
	if s := nextStatus(t, conn); s.PointCode != 0x1234 || s.State.Availability != DestinationUnavailable {
		t.Errorf("status = %+v, want point code 0x1234 Unavailable", s)
	}
	// SSNM is not acknowledged.
	if len(*sent) != 0 {
		t.Errorf("answered a DUNA with %v; SSNM carries no acknowledgement", typeNames(*sent))
	}
}

// The recovery half: DAVA restores a destination a DUNA stopped. An SG that
// cannot restore traffic is as damaging as one that cannot stop it.
func TestDAVARestoresDestination(t *testing.T) {
	conn, _ := ssnmConn(t)

	if err := conn.handleDestinationUnavailable(
		messages.NewDestinationUnavailable(nil, nil, apc(0x1234), nil)); err != nil {
		t.Fatal(err)
	}
	if got := retainedAvailability(conn, 0x1234); got != DestinationUnavailable {
		t.Fatalf("setup: state = %v, want %v", got, DestinationUnavailable)
	}

	if err := conn.handleDestinationAvailable(
		messages.NewDestinationAvailable(nil, nil, apc(0x1234), nil)); err != nil {
		t.Fatalf("handleDestinationAvailable() error = %v, want nil", err)
	}
	if got := retainedAvailability(conn, 0x1234); got != DestinationAvailable {
		t.Errorf("retained availability after DAVA = %v, want %v", got, DestinationAvailable)
	}
}

// A destination the peer has never mentioned is reachable: SSNM reports
// changes, not an inventory. Treating unknown destinations as unavailable would
// black-hole all traffic until the first DAVA arrived.
func TestUnreportedDestinationIsAvailable(t *testing.T) {
	conn, _ := ssnmConn(t)

	if got := retainedAvailability(conn, 0xdeadbeef); got != DestinationAvailable {
		t.Errorf("retained availability of an unreported point code = %v, want %v", got, DestinationAvailable)
	}
}

// Affected Point Code carries a list, and an SG reporting a link failure
// routinely names every destination behind it in one message. Applying only the
// first would leave the rest black-holed.
func TestSSNMAppliesToEveryAffectedPointCode(t *testing.T) {
	conn, _ := ssnmConn(t)

	pcs := []uint32{0x1111, 0x2222, 0x3333}
	if err := conn.handleDestinationUnavailable(
		messages.NewDestinationUnavailable(nil, nil, apc(pcs...), nil)); err != nil {
		t.Fatalf("handleDestinationUnavailable() error = %v, want nil", err)
	}

	for _, pc := range pcs {
		if got := retainedAvailability(conn, pc); got != DestinationUnavailable {
			t.Errorf("retained availability for %#x = %v, want %v", pc, got, DestinationUnavailable)
		}
	}
	if got := len(retainedSnapshot(conn, nil)); got != len(pcs) {
		t.Errorf("tracked %d destinations, want %d", got, len(pcs))
	}
}

// RFC 4666 Section 3.4.4: SCON reports congestion so the MTP3-User can reduce
// traffic. The Congestion Indications parameter is Optional, so a peer that
// omits it must still be understood rather than rejected.
func TestSCONReportsCongestion(t *testing.T) {
	t.Run("with level", func(t *testing.T) {
		conn, _ := ssnmConn(t)

		if err := conn.handleSignallingCongestion(messages.NewSignallingCongestion(
			nil, nil, apc(0x1234), nil, params.NewCongestionIndications(2), nil)); err != nil {
			t.Fatalf("handleSignallingCongestion() error = %v, want nil", err)
		}
		// Congestion is a status of its own (RFC 4666 Section 4.5.2.2): the
		// destination is reported congested and stays reachable.
		if got := retainedDestinationState(conn, 0x1234); !got.Congestion.Congested ||
			got.Availability != DestinationAvailable {
			t.Errorf("state = %+v, want a congested destination that is still available", got)
		}
		if s := nextStatus(t, conn); s.State.Congestion.Level != 2 {
			t.Errorf("congestion level = %d, want 2", s.State.Congestion.Level)
		}
	})

	t.Run("without level", func(t *testing.T) {
		conn, _ := ssnmConn(t)

		if err := conn.handleSignallingCongestion(messages.NewSignallingCongestion(
			nil, nil, apc(0x1234), nil, nil, nil)); err != nil {
			t.Fatalf("handleSignallingCongestion() error = %v, want nil (the parameter is optional)", err)
		}
		if got := retainedDestinationState(conn, 0x1234); !got.Congestion.Congested {
			t.Errorf("state = %+v, want the destination reported congested", got)
		}
	})
}

// RFC 4666 Section 3.4.5: DUPU reports that a *user part* is unavailable at a
// destination that remains reachable. Marking the destination itself down would
// stop traffic to every other user part at that point code — an outage caused
// by the library, not the network.
func TestDUPULeavesDestinationReachable(t *testing.T) {
	conn, _ := ssnmConn(t)

	if err := conn.handleDestinationUserPartUnavailable(
		messages.NewDestinationUserPartUnavailable(
			nil, nil, apc(0x1234), params.NewUserCause(3, 2), nil)); err != nil {
		t.Fatalf("handleDestinationUserPartUnavailable() error = %v, want nil", err)
	}

	if got := retainedAvailability(conn, 0x1234); got != DestinationAvailable {
		t.Errorf("retained availability after DUPU = %v, want %v: the destination is still reachable",
			got, DestinationAvailable)
	}

	s := nextStatus(t, conn)
	if !s.UserPartUnavailable {
		t.Error("status.UserPartUnavailable = false, want true for a DUPU")
	}
	if s.UserCause == 0 {
		t.Error("status.UserCause = 0, want the cause the peer reported")
	}
}

// RFC 4666 Section 3.4.6: DRST marks a destination reachable but not preferred.
func TestDRSTMarksDestinationRestricted(t *testing.T) {
	conn, _ := ssnmConn(t)

	if err := conn.handleDestinationRestricted(
		messages.NewDestinationRestricted(nil, nil, apc(0x1234), nil)); err != nil {
		t.Fatalf("handleDestinationRestricted() error = %v, want nil", err)
	}
	if got := retainedAvailability(conn, 0x1234); got != DestinationRestricted {
		t.Errorf("state = %v, want %v", got, DestinationRestricted)
	}
}

// Affected Point Code is Mandatory in every SSNM message (RFC 4666 Sections
// 3.4.1 to 3.4.6). A message without it must be reported as a Missing Parameter
// rather than silently ignored or dereferenced.
func TestSSNMWithoutAffectedPointCodeIsRejected(t *testing.T) {
	for _, tt := range []struct {
		name string
		call func(*Association) error
	}{
		{"DUNA", func(c *Association) error {
			return c.handleDestinationUnavailable(messages.NewDestinationUnavailable(nil, nil, nil, nil))
		}},
		{"DAVA", func(c *Association) error {
			return c.handleDestinationAvailable(messages.NewDestinationAvailable(nil, nil, nil, nil))
		}},
		{"DRST", func(c *Association) error {
			return c.handleDestinationRestricted(messages.NewDestinationRestricted(nil, nil, nil, nil))
		}},
		{"SCON", func(c *Association) error {
			return c.handleSignallingCongestion(messages.NewSignallingCongestion(nil, nil, nil, nil, nil, nil))
		}},
		{"DUPU", func(c *Association) error {
			return c.handleDestinationUserPartUnavailable(
				messages.NewDestinationUserPartUnavailable(nil, nil, nil, nil, nil))
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conn, sent := ssnmConn(t)

			err := tt.call(conn)
			if !errors.Is(err, ErrMissingAffectedPointCode) {
				t.Fatalf("error = %v, want ErrMissingAffectedPointCode", err)
			}

			if e := conn.handleErrors(err); e != nil {
				t.Fatalf("handleErrors() error = %v, want nil", e)
			}
			codes := errorCodes(*sent)
			if len(codes) != 1 || codes[0] != params.ErrMissingParameter {
				t.Errorf("error codes = %v, want [%d] (Missing Parameter)", codes, params.ErrMissingParameter)
			}
		})
	}
}

// An Affected Point Code present but empty decodes to no point codes at all.
// Treating that as success would silently apply nothing while telling the peer
// everything was fine.
func TestSSNMWithEmptyAffectedPointCodeIsRejected(t *testing.T) {
	conn, _ := ssnmConn(t)

	err := conn.handleDestinationUnavailable(
		messages.NewDestinationUnavailable(nil, nil, params.NewAffectedPointCode(), nil))
	if !errors.Is(err, ErrMissingAffectedPointCode) {
		t.Errorf("error = %v, want ErrMissingAffectedPointCode for an empty Affected Point Code", err)
	}
}

// DUNA, DAVA, DRST, SCON and DUPU travel SGP to ASP. An SGP that receives one
// must report an Error rather than apply it: a peer must never be able to steer
// an SG's own view of the SS7 network.
func TestSGPRejectsAspBoundSSNM(t *testing.T) {
	for _, tt := range []struct {
		name string
		call func(*Association) error
	}{
		{"DUNA", func(c *Association) error {
			return c.handleDestinationUnavailable(
				messages.NewDestinationUnavailable(nil, nil, apc(0x1234), nil))
		}},
		{"DAVA", func(c *Association) error {
			return c.handleDestinationAvailable(
				messages.NewDestinationAvailable(nil, nil, apc(0x1234), nil))
		}},
		{"DRST", func(c *Association) error {
			return c.handleDestinationRestricted(
				messages.NewDestinationRestricted(nil, nil, apc(0x1234), nil))
		}},
		// SCON is deliberately absent: it is the one SSNM message the RFC
		// sends in both directions. Section 3.4.4: "The SCON message MAY also
		// be sent from the M3UA layer of an ASP to an M3UA peer, indicating
		// that the congestion level of the M3UA layer or the ASP has changed."
		// See TestSCONFromAnASPIsAcceptedAtAnSGP.
		{"DUPU", func(c *Association) error {
			return c.handleDestinationUserPartUnavailable(
				messages.NewDestinationUserPartUnavailable(nil, nil, apc(0x1234), nil, nil))
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conn, _ := newSSNMTestConn(t, StateASPActive, RoleSGP)

			var unexpected *UnexpectedMessageError
			if err := tt.call(conn); !errors.As(err, &unexpected) {
				t.Fatalf("error = %v, want *UnexpectedMessageError at an SGP", err)
			}
			if got := retainedAvailability(conn, 0x1234); got != DestinationAvailable {
				t.Errorf("an SGP applied a peer's %s: state = %v, want it untouched", tt.name, got)
			}
		})
	}
}

// DAUD is the mirror: ASP to SGP (RFC 4666 Section 3.4.3). An ASP that receives
// one must reject it.
func TestASPRejectsDAUD(t *testing.T) {
	conn, sent := ssnmConn(t)

	var unexpected *UnexpectedMessageError
	if err := conn.handleDestinationStateAudit(
		messages.NewDestinationStateAudit(nil, nil, apc(0x1234), nil)); !errors.As(err, &unexpected) {
		t.Fatalf("error = %v, want *UnexpectedMessageError at an ASP", err)
	}
	if len(*sent) != 0 {
		t.Errorf("an ASP answered a DAUD with %v; want nothing", typeNames(*sent))
	}
}

// An ASP that restarts has no idea which destinations are down. RFC 4666
// Section 3.4.3 lets it audit them with a DAUD, and the SG answers from the
// state it holds — DUNA for unavailable destinations, DAVA for reachable ones.
// Without this an ASP must wait for the next spontaneous update, during which
// it either black-holes traffic or sends into a void.
func TestDAUDIsAnsweredFromDestinationState(t *testing.T) {
	for _, tt := range []struct {
		name  string
		state DestinationNetworkState
		want  []string
	}{
		{"unavailable", availabilityState(DestinationUnavailable), []string{"Destination Unavailable"}},
		{"restricted", availabilityState(DestinationRestricted), []string{"Destination Restricted"}},
		{"available", availabilityState(DestinationAvailable), []string{"Destination Available"}},
		// Congestion is a status of its own, so a congested destination is
		// still available and the audit answers both dimensions: RFC 4666
		// Section 4.5.3 has the SCON go first — "For national networks, the SGP
		// SHOULD additionally respond with a SCON message (if the destination
		// is congested) before the DAVA or DRST."
		{"congested", congestedState(2), []string{"Signalling Congestion", "Destination Available"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conn, sent := newSSNMTestConn(t, StateASPActive, RoleSGP)
			seedDestinationNetworkState(conn, 0x1234, tt.state)

			if err := conn.handleDestinationStateAudit(
				messages.NewDestinationStateAudit(nil, nil, apc(0x1234), nil)); err != nil {
				t.Fatalf("handleDestinationStateAudit() error = %v, want nil", err)
			}

			got := typeNames(*sent)
			if len(got) != len(tt.want) {
				t.Fatalf("answered a DAUD with %v, want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("answered a DAUD with %v, want %v", got, tt.want)
					break
				}
			}
		})
	}
}

// A DAUD naming several point codes must be answered for every one of them.
func TestDAUDAnswersEveryAuditedPointCode(t *testing.T) {
	conn, sent := newSSNMTestConn(t, StateASPActive, RoleSGP)
	seedDestinationAvailability(conn, 0x1111, DestinationUnavailable)
	seedDestinationAvailability(conn, 0x2222, DestinationAvailable)

	if err := conn.handleDestinationStateAudit(
		messages.NewDestinationStateAudit(nil, nil, apc(0x1111, 0x2222), nil)); err != nil {
		t.Fatalf("handleDestinationStateAudit() error = %v, want nil", err)
	}

	got := typeNames(*sent)
	if len(got) != 2 {
		t.Fatalf("answered with %v, want one reply per audited point code", got)
	}
	if got[0] != "Destination Unavailable" || got[1] != "Destination Available" {
		t.Errorf("replies = %v, want [Destination Unavailable, Destination Available]", got)
	}
}

// RFC 4666 Section 4.3.1: while ASP-INACTIVE "the ASP/IPSP SHOULD NOT be sent
// any DATA or SSNM messages", and an ASP-DOWN peer should receive nothing but
// Heartbeat, ASP Down Ack and Error. Acting on destination state we are not
// entitled to receive would let an out-of-state peer steer traffic.
// SSNM outside the states where it means anything is rejected.
//
// ASP-INACTIVE is deliberately not in this list. RFC 4666 Section 4.5.1 opens
// that window for DUNA, DRST and SCON — "these DUNA, DRST, and SCON messages
// MAY be sent before sending the ASP Active Ack that completes the activation
// procedure" — and the ASP is in ASP-INACTIVE for the whole of it. See
// TestSSNMIsAcceptedBeforeTheAspActiveAck.
func TestSSNMOutsideActiveIsRejected(t *testing.T) {
	for _, st := range []State{StateASPDown} {
		t.Run(st.String(), func(t *testing.T) {
			conn, _ := newSSNMTestConn(t, st, RoleASP)

			var unexpected *UnexpectedMessageError
			if err := conn.handleDestinationUnavailable(
				messages.NewDestinationUnavailable(nil, nil, apc(0x1234), nil)); !errors.As(err, &unexpected) {
				t.Fatalf("error = %v in %v, want *UnexpectedMessageError", err, st)
			}
			if got := retainedAvailability(conn, 0x1234); got != DestinationAvailable {
				t.Errorf("applied a DUNA received in %v: state = %v, want it untouched", st, got)
			}
		})
	}
}

// Section 4.6 permits DAVA before ASP Active Ack while an SGP is completing an
// MTP3 restart. The window still requires an actual pending ASP Active; DUPU is
// in neither exception and remains rejected.
func TestDAVAAcceptedOnlyDuringPendingActivationAndDUPURejected(t *testing.T) {
	conn, _ := newSSNMTestConn(t, StateASPInactive, RoleASP)

	var unexpected *UnexpectedMessageError
	if err := conn.handleDestinationAvailable(
		messages.NewDestinationAvailable(nil, nil, apc(0x1234), nil)); !errors.As(err, &unexpected) {
		t.Errorf("DAVA without ASP Active pending: error = %v, want *UnexpectedMessageError", err)
	}
	conn.startTAck(messages.NewAspActive(
		inventoryTrafficModeParam(conn.cfg.ApplicationServers), asConfigRoutingContextParam(conn.cfg.ApplicationServers), nil,
	), requestAspActive)
	if err := conn.handleDestinationAvailable(
		messages.NewDestinationAvailable(nil, nil, apc(0x1234), nil)); err != nil {
		t.Fatalf("DAVA with ASP Active pending: %v", err)
	}
	_ = nextStatus(t, conn)
	if err := conn.handleDestinationUserPartUnavailable(
		messages.NewDestinationUserPartUnavailable(
			nil, nil, apc(0x1234), params.NewUserCause(3, 2), nil,
		)); !errors.As(err, &unexpected) {
		t.Errorf("DUPU in ASP-INACTIVE: error = %v, want *UnexpectedMessageError", err)
	}
}

// A peer that floods SSNM must not be able to wedge the dispatcher. The status
// channel is deliberately lossy: state stays authoritative, but a user that
// never reads must not stall message processing — that would stop the read loop
// and take the association down.
func TestSSNMFloodDoesNotWedgeDispatcher(t *testing.T) {
	conn, _ := ssnmConn(t)

	done := make(chan struct{})
	go func() {
		// Far more than the status channel can hold, with nobody reading.
		for i := range 1000 {
			_ = conn.handleDestinationUnavailable(
				messages.NewDestinationUnavailable(nil, nil, apc(uint32(i)), nil))
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("SSNM handling wedged on a full status channel; a flooding peer could stall the read loop")
	}

	// State is authoritative regardless of whether the user drained anything.
	if got := retainedAvailability(conn, 999); got != DestinationUnavailable {
		t.Errorf("retained availability for 999 = %v, want %v; state must survive a dropped notification",
			got, DestinationUnavailable)
	}
}

// The dispatcher must publish exactly one state per SSNM message, as for every
// other class: SSNM never changes the ASP state machine, so the current state
// is republished unchanged.
func TestSSNMPublishesExactlyOneStateAndHoldsIt(t *testing.T) {
	for _, tt := range []struct {
		name string
		msg  messages.M3UA
	}{
		{"DUNA", messages.NewDestinationUnavailable(nil, nil, params.NewAffectedPointCode(0x1234), nil)},
		{"DAVA", messages.NewDestinationAvailable(nil, nil, params.NewAffectedPointCode(0x1234), nil)},
		{"DRST", messages.NewDestinationRestricted(nil, nil, params.NewAffectedPointCode(0x1234), nil)},
		{"SCON", messages.NewSignallingCongestion(nil, nil, params.NewAffectedPointCode(0x1234), nil, nil, nil)},
		{"DUPU", messages.NewDestinationUserPartUnavailable(nil, nil, params.NewAffectedPointCode(0x1234), nil, nil)},
		{"DAUD", messages.NewDestinationStateAudit(nil, nil, params.NewAffectedPointCode(0x1234), nil)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conn, _ := newSSNMTestConn(t, StateASPActive, RoleASP)

			conn.handleSignals(context.Background(), tt.msg)

			if got := len(conn.stateChan); got != 1 {
				t.Errorf("published %d states for a %s, want exactly 1", got, tt.name)
			}
			select {
			case got := <-conn.stateChan:
				if got != StateASPActive && got != stateUnchanged {
					t.Errorf("state = %v after a %s, want it held at %v", got, tt.name, StateASPActive)
				}
			default:
				t.Fatal("no state published")
			}
		})
	}
}

// SSNM must be dispatched to its handlers, not left to the default arm that
// answers "Unsupported Message". That reply is what an SG saw before these
// procedures existed, and it makes an ASP look broken to any conformant peer.
//
// The assertion is on the error published to errChan rather than on anything
// written: the default arm reports through sendErr, and only handleErrors —
// which the dispatcher runs later, in monitor() — turns that into a wire
// message. A test that only inspected sent signals would pass whether or not
// the dispatch arms existed.
func TestSSNMIsDispatchedNotRejectedAsUnsupported(t *testing.T) {
	for _, tt := range []struct {
		name string
		msg  messages.M3UA
		want DestinationNetworkState
	}{
		{"DUNA", messages.NewDestinationUnavailable(nil, nil, params.NewAffectedPointCode(0x1234), nil), availabilityState(DestinationUnavailable)},
		{"DRST", messages.NewDestinationRestricted(nil, nil, params.NewAffectedPointCode(0x1234), nil), availabilityState(DestinationRestricted)},
		// A SCON without Congestion Indications reports congestion at no
		// stated level, and moves that dimension alone (RFC 4666 Section
		// 4.5.2.2), so the destination stays available.
		{"SCON", messages.NewSignallingCongestion(nil, nil, params.NewAffectedPointCode(0x1234), nil, nil, nil),
			DestinationNetworkState{Congestion: congestionStateFor(0, false)}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conn, _ := ssnmConn(t)

			conn.handleSignals(context.Background(), tt.msg)

			// Nothing may be reported as unsupported.
			for {
				select {
				case err := <-conn.errChan:
					var unsupported *UnsupportedMessageError
					if errors.As(err, &unsupported) {
						t.Fatalf("%s was answered as unsupported (%v); the dispatch arm is missing", tt.name, err)
					}
					var unsupportedClass *UnsupportedClassError
					if errors.As(err, &unsupportedClass) {
						t.Fatalf("%s was answered as an unsupported class (%v)", tt.name, err)
					}
					continue
				default:
				}
				break
			}

			// And the message must actually have been acted on.
			if got := retainedDestinationState(conn, 0x1234); got != tt.want {
				t.Errorf("retained state after %s = %+v, want %+v: the message was not dispatched to its handler",
					tt.name, got, tt.want)
			}
		})
	}
}

// RFC 4666 Section 3.2.1 defines Parameter Length as a 16-bit field that
// includes the four-octet Parameter Tag and Parameter Length header.
const maxM3UAParameterValueLength = int(^uint16(0)) - 4

func m3uaParameterValueFitsOnWire(value []byte) bool {
	return len(value) <= maxM3UAParameterValueLength
}

func TestM3UAParameterValueFitsOnWire(t *testing.T) {
	if !m3uaParameterValueFitsOnWire(make([]byte, maxM3UAParameterValueLength)) {
		t.Fatal("maximum parameter value length was rejected")
	}
	if m3uaParameterValueFitsOnWire(make([]byte, maxM3UAParameterValueLength+1)) {
		t.Fatal("parameter value longer than the 16-bit wire length was accepted")
	}
}

// SSNM handlers act on peer-controlled point code lists, so they run against
// the fuzzer as well as the table tests above. Nothing a peer can put in an
// Affected Point Code may panic a handler or corrupt the destination table:
// this is the path that decides whether traffic flows to a point code.
func FuzzSSNMHandlers(f *testing.F) {
	for _, seed := range [][]byte{
		{},
		{0x00, 0x00, 0x00, 0x01},
		{0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02},
		{0xff, 0xff, 0xff, 0xff},
		{0x00},                   // not a whole word
		{0x00, 0x00},             // still not
		{0x00, 0x00, 0x00},       // one short
		{0x80, 0x00, 0x00, 0x01}, // mask byte set
	} {
		f.Add(seed)
	}

	// One Association per role, reused across inputs. Building six per iteration made
	// the target allocation-bound (~3k execs/sec against ~100k for the parser
	// targets), which starves the fuzzer of coverage rather than testing
	// anything extra: destination state is reset between messages instead.
	aspAssociation := newFuzzConn(f, RoleASP)
	sgpAssociation := newFuzzConn(f, RoleSGP)

	f.Fuzz(func(t *testing.T, apcData []byte) {
		if !m3uaParameterValueFitsOnWire(apcData) {
			return
		}

		// An Affected Point Code carrying arbitrary bytes, as a hostile peer
		// would send, rather than one the library constructed itself.
		raw := params.NewParam(int(params.AffectedPointCode), apcData)

		for _, tt := range []struct {
			name string
			conn *Association
			call func(*Association) error
		}{
			{"DUNA", aspAssociation, func(c *Association) error {
				return c.handleDestinationUnavailable(messages.NewDestinationUnavailable(nil, nil, raw, nil))
			}},
			{"DAVA", aspAssociation, func(c *Association) error {
				return c.handleDestinationAvailable(messages.NewDestinationAvailable(nil, nil, raw, nil))
			}},
			{"DRST", aspAssociation, func(c *Association) error {
				return c.handleDestinationRestricted(messages.NewDestinationRestricted(nil, nil, raw, nil))
			}},
			{"SCON", aspAssociation, func(c *Association) error {
				return c.handleSignallingCongestion(messages.NewSignallingCongestion(nil, nil, raw, nil, nil, nil))
			}},
			{"DUPU", aspAssociation, func(c *Association) error {
				return c.handleDestinationUserPartUnavailable(
					messages.NewDestinationUserPartUnavailable(
						nil, nil, raw, params.NewUserCause(params.SCCP, params.Unequipped), nil))
			}},
			{"DAUD", sgpAssociation, func(c *Association) error {
				return c.handleDestinationStateAudit(messages.NewDestinationStateAudit(nil, nil, raw, nil))
			}},
		} {
			tt.conn.destinations = newDestinations()

			// Any error is acceptable; a panic is not.
			_ = tt.call(tt.conn)

			// Whatever happened, the destination table must remain readable and
			// self-consistent — a corrupt one silently misroutes traffic.
			for pc, state := range retainedSnapshot(tt.conn, nil) {
				if !validDestinationNetworkState(state) {
					t.Fatalf("%s produced an out-of-range state %+v for point code %#x", tt.name, state, pc)
				}
			}
			for _, destinationRange := range retainedRanges(tt.conn) {
				if !validDestinationNetworkState(destinationRange.State) {
					t.Fatalf("%s produced an out-of-range state %+v for range %#x/%d",
						tt.name, destinationRange.State, destinationRange.PointCode, destinationRange.Mask)
				}
				if destinationRange.PointCode > 0x00ffffff {
					t.Fatalf("%s retained an out-of-range point code %#x",
						tt.name, destinationRange.PointCode)
				}
			}

			// Drain the lossy status channel so a long fuzz run does not simply
			// fill it and stop exercising notifyStatus.
			drainSignallingStatuses(tt.conn.SignallingStatus())
		}
	})
}

func TestDrainSignallingStatusesReturnsWhenClosed(t *testing.T) {
	statuses := make(chan *DestinationStatus)
	close(statuses)

	drainSignallingStatuses(statuses)
}

func drainSignallingStatuses(statuses <-chan *DestinationStatus) {
	for {
		select {
		case _, open := <-statuses:
			if !open {
				return
			}
		default:
			return
		}
	}
}

// newFuzzConn builds a minimal ASP-ACTIVE Association without the per-test
// bookkeeping newTestConn does, so a fuzz target can build one per role up
// front and reuse it. Takes testing.TB so it can be called from F.
func newFuzzConn(t testing.TB, role Role) *Association {
	t.Helper()

	cfg := newASPAssociationConfigForTest(&HeartbeatInfo{Enabled: false}, 1, params.TrafficModeLoadshare, 0, []uint32{1})

	conn := &Association{
		muState:      new(sync.RWMutex),
		role:         role,
		state:        StateASPActive,
		stateChan:    make(chan State, 4),
		errChan:      make(chan error, 4),
		established:  make(chan struct{}, 1),
		beatAckChan:  make(chan struct{}, 1),
		beatStart:    make(chan struct{}),
		dataChan:     make(chan *DataMessage, 4),
		done:         make(chan struct{}),
		cfg:          cfg,
		destinations: newDestinations(),
		tack:         newTAckRetransmitter(),
		statusChan:   make(chan *DestinationStatus, 8),
		// See newTestConn: nil here drops every transition and panics on close.
		stateEventChan: make(chan State, 16),
		mgmtChan:       make(chan *ManagementIndication, 64),
	}
	conn.signalWriter = func(m3 messages.M3UA) (int, error) { return m3.MarshalLen(), nil }

	return conn
}

// RFC 4666 Section 4.5.1, on the SGP sending destination state to a newly
// activating ASP:
//
//	"For the newly activating ASP from which the SGP has received an ASP Active
//	message, these DUNA, DRST, and SCON messages MAY be sent before sending the
//	ASP Active Ack that completes the activation procedure."
//
// The purpose is stated in the same paragraph: "to prevent the ASP from sending
// traffic for destinations that it might not otherwise know that are
// inaccessible, restricted, or congested." Requiring ASP-ACTIVE threw exactly
// those messages away, since the ASP only reaches ASP-ACTIVE on the Ack that
// follows them.
func TestSSNMIsAcceptedBeforeTheAspActiveAck(t *testing.T) {
	for _, tt := range []struct {
		name string
		send func(*Association) error
	}{
		{"DUNA", func(c *Association) error {
			return c.handleDestinationUnavailable(messages.NewDestinationUnavailable(
				nil, nil, params.NewAffectedPointCode(0x111111), nil))
		}},
		{"DRST", func(c *Association) error {
			return c.handleDestinationRestricted(messages.NewDestinationRestricted(
				nil, nil, params.NewAffectedPointCode(0x111111), nil))
		}},
		{"SCON", func(c *Association) error {
			return c.handleSignallingCongestion(messages.NewSignallingCongestion(
				nil, nil, params.NewAffectedPointCode(0x111111), nil, nil, nil))
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			// ASP-INACTIVE: the ASP Active has gone out, the Ack has not come back.
			conn, _ := newSSNMTestConn(t, StateASPInactive, RoleASP)
			conn.startTAck(messages.NewAspActive(
				inventoryTrafficModeParam(conn.cfg.ApplicationServers), asConfigRoutingContextParam(conn.cfg.ApplicationServers), nil,
			), requestAspActive)

			if err := tt.send(conn); err != nil {
				t.Fatalf("%s during activation was rejected: %v", tt.name, err)
			}
			if got := retainedAvailability(conn, 0x111111); got == DestinationAvailable && tt.name != "SCON" {
				t.Errorf("%s during activation left the destination %v", tt.name, got)
			}
		})
	}
}

// Neither activation exception applies while the ASP is still ASP-DOWN.
func TestSSNMOutsideTheActivationWindowIsStillRejected(t *testing.T) {
	conn, _ := newSSNMTestConn(t, StateASPDown, RoleASP)

	err := conn.handleDestinationUnavailable(messages.NewDestinationUnavailable(
		nil, nil, params.NewAffectedPointCode(0x111111), nil))
	if err == nil {
		t.Error("DUNA in ASP-DOWN was accepted; the window is Section 4.5.1's, not unlimited")
	}
}

// RFC 4666 Section 3.4.4: "The SCON message MAY also be sent from the M3UA
// layer of an ASP to an M3UA peer, indicating that the congestion level of the
// M3UA layer or the ASP has changed."
//
// SCON is the one SSNM message that travels both ways. It was gated to the ASP
// role only, so an SGP answered a congested ASP with "Unexpected Message" and
// learned nothing about it.
func TestSCONFromAnASPIsAcceptedAtAnSGP(t *testing.T) {
	conn, _ := newSSNMTestConn(t, StateASPActive, RoleSGP)

	if err := conn.handleSignallingCongestion(messages.NewSignallingCongestion(nil, nil, params.NewAffectedPointCode(0x222222), nil, nil, nil)); err != nil {
		t.Fatalf("SCON from an ASP was rejected at an SGP: %v", err)
	}

	// Accepted, but not as SS7 state. The same sentence that permits the
	// message says what it means — "the congestion level of the M3UA layer or
	// the ASP has changed" — which is about this one peer, not about whether
	// the named destination is reachable through this SG. Recording it in the
	// SG's own destination map let any ASP fabricate congestion that every
	// other ASP would then be told about when it audited (Section 4.5.3). See
	// TestSCONFromAnASPDoesNotRewriteTheSGsRoutingState.
	if got := retainedDestinationState(conn, 0x222222); got.Congestion.Congested {
		t.Errorf("an ASP's SCON set the SG's own destination state to %+v", got)
	}

	// It is still surfaced to the user, marked as the peer's report.
	select {
	case st := <-conn.SignallingStatus():
		if !st.PeerReported {
			t.Error("the ASP's SCON was reported as SS7 state rather than as the peer's own")
		}
	default:
		t.Error("the ASP's SCON was not reported at all")
	}
}

// Invalid Network Appearance is not DATA-specific. RFC 4666 Section 3.8.1
// requires an SGP to reject any ASP message naming an unconfigured network;
// DAUD and SCON are the SSNM messages an ASP is allowed to originate.
func TestSGPRejectsInvalidNetworkAppearanceInASPSSNM(t *testing.T) {
	for _, tt := range []struct {
		name   string
		handle func(*Association) error
	}{
		{
			name: "DAUD",
			handle: func(c *Association) error {
				return c.handleDestinationStateAudit(messages.NewDestinationStateAudit(
					params.NewNetworkAppearance(8), nil, apc(0x222222), nil))
			},
		},
		{
			name: "SCON",
			handle: func(c *Association) error {
				return c.handleSignallingCongestion(messages.NewSignallingCongestion(
					params.NewNetworkAppearance(8), nil, apc(0x222222), nil, nil, nil))
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conn, sent := newSSNMTestConn(t, StateASPActive, RoleSGP)
			setInventoryNetworkAppearance(&conn.cfg.ApplicationServers, params.NewNetworkAppearance(7))

			reported := tt.handle(conn)
			if !errors.Is(reported, ErrInvalidNetworkAppearance) {
				t.Fatalf("error = %v, want ErrInvalidNetworkAppearance", reported)
			}
			var appearanceError *NetworkAppearanceError
			if !errors.As(reported, &appearanceError) || appearanceError.Appearance != 8 {
				t.Fatalf("error = %#v, want NetworkAppearanceError carrying 8", reported)
			}
			if err := conn.handleErrors(reported); err != nil {
				t.Fatal(err)
			}
			e := lastError(t, *sent)
			if e.ErrorCode == nil || e.ErrorCode.ErrorCode() != params.ErrInvalidNetworkAppearance {
				t.Fatalf("error code = %v, want Invalid Network Appearance", e.ErrorCode)
			}
			if e.NetworkAppearance == nil || e.NetworkAppearance.NetworkAppearance() != 8 {
				t.Errorf("Error Network Appearance = %v, want 8", e.NetworkAppearance)
			}
		})
	}
}

func TestSSNMPreservesNetworkAppearance(t *testing.T) {
	for _, tt := range []struct {
		name   string
		handle func(*Association, *params.Param) error
	}{
		{"DUNA", func(c *Association, na *params.Param) error {
			return c.handleDestinationUnavailable(messages.NewDestinationUnavailable(na, nil, apc(0x222222), nil))
		}},
		{"DAVA", func(c *Association, na *params.Param) error {
			return c.handleDestinationAvailable(messages.NewDestinationAvailable(na, nil, apc(0x222222), nil))
		}},
		{"DRST", func(c *Association, na *params.Param) error {
			return c.handleDestinationRestricted(messages.NewDestinationRestricted(na, nil, apc(0x222222), nil))
		}},
		{"SCON", func(c *Association, na *params.Param) error {
			return c.handleSignallingCongestion(messages.NewSignallingCongestion(na, nil, apc(0x222222), nil, nil, nil))
		}},
		{"DUPU", func(c *Association, na *params.Param) error {
			return c.handleDestinationUserPartUnavailable(messages.NewDestinationUserPartUnavailable(
				na, nil, apc(0x222222), params.NewUserCause(3, 2), nil))
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conn, _ := newSSNMTestConn(t, StateASPActive, RoleASP)
			setInventoryNetworkAppearance(&conn.cfg.ApplicationServers, params.NewNetworkAppearance(7))
			if err := tt.handle(conn, params.NewNetworkAppearance(8)); err != nil {
				t.Fatalf("valid SSNM was rejected: %v", err)
			}
			select {
			case status := <-conn.SignallingStatus():
				if !status.NetworkAppearanceSet || status.NetworkAppearance != 8 {
					t.Errorf("Network Appearance = %d (set=%v), want 8",
						status.NetworkAppearance, status.NetworkAppearanceSet)
				}
			default:
				t.Fatal("valid SSNM produced no status")
			}
		})
	}

	for _, tt := range []struct {
		name    string
		value   *params.Param
		want    uint32
		wantSet bool
	}{
		{"explicit zero", params.NewNetworkAppearance(0), 0, true},
		{"omitted", nil, 0, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conn, _ := newSSNMTestConn(t, StateASPActive, RoleASP)
			if err := conn.handleDestinationUnavailable(
				messages.NewDestinationUnavailable(tt.value, nil, apc(0x222222), nil)); err != nil {
				t.Fatal(err)
			}
			status := <-conn.SignallingStatus()
			if status.NetworkAppearance != tt.want || status.NetworkAppearanceSet != tt.wantSet {
				t.Errorf("Network Appearance = %d (set=%v), want %d (set=%v)",
					status.NetworkAppearance, status.NetworkAppearanceSet, tt.want, tt.wantSet)
			}
		})
	}
}

func TestSSNMRejectsMalformedNetworkAppearance(t *testing.T) {
	for _, tt := range []struct {
		name   string
		handle func(*Association, *params.Param) error
	}{
		{"DUNA", func(c *Association, na *params.Param) error {
			return c.handleDestinationUnavailable(messages.NewDestinationUnavailable(na, nil, apc(0x222222), nil))
		}},
		{"DAVA", func(c *Association, na *params.Param) error {
			return c.handleDestinationAvailable(messages.NewDestinationAvailable(na, nil, apc(0x222222), nil))
		}},
		{"DRST", func(c *Association, na *params.Param) error {
			return c.handleDestinationRestricted(messages.NewDestinationRestricted(na, nil, apc(0x222222), nil))
		}},
		{"SCON", func(c *Association, na *params.Param) error {
			return c.handleSignallingCongestion(messages.NewSignallingCongestion(na, nil, apc(0x222222), nil, nil, nil))
		}},
		{"DUPU", func(c *Association, na *params.Param) error {
			return c.handleDestinationUserPartUnavailable(messages.NewDestinationUserPartUnavailable(
				na, nil, apc(0x222222), params.NewUserCause(3, 2), nil))
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conn, _ := newSSNMTestConn(t, StateASPActive, RoleASP)
			reported := tt.handle(conn, params.NewParam(int(params.NetworkAppearance), []byte{0, 0, 8}))
			var parameterFault *ParameterFaultError
			if !errors.As(reported, &parameterFault) || parameterFault.Code != params.ErrParameterFieldError {
				t.Fatalf("error = %v, want Parameter Field Error", reported)
			}
			select {
			case status := <-conn.SignallingStatus():
				t.Errorf("malformed SSNM was applied: %#v", status)
			default:
			}
		})
	}
}

func TestDestinationStateIsScopedByNetworkAppearance(t *testing.T) {
	const pointCode = 0x222222
	conn, _ := newSSNMTestConn(t, StateASPActive, RoleASP)
	setInventoryNetworkAppearance(&conn.cfg.ApplicationServers, params.NewNetworkAppearance(7))

	if err := conn.handleDestinationUnavailable(messages.NewDestinationUnavailable(
		params.NewNetworkAppearance(8), nil, apc(pointCode), nil)); err != nil {
		t.Fatal(err)
	}
	if err := conn.handleDestinationRestricted(messages.NewDestinationRestricted(
		params.NewNetworkAppearance(9), nil, apc(pointCode), nil)); err != nil {
		t.Fatal(err)
	}

	if got := retainedAvailabilityForNetwork(conn, 8, pointCode); got != DestinationUnavailable {
		t.Errorf("network 8 state = %v, want unavailable", got)
	}
	if got := retainedAvailabilityForNetwork(conn, 9, pointCode); got != DestinationRestricted {
		t.Errorf("network 9 state = %v, want restricted", got)
	}
	if got := retainedAvailability(conn, pointCode); got != DestinationAvailable {
		t.Errorf("configured network 7 state = %v, want untouched/available", got)
	}
	states := retainedSnapshot(conn, params.NewNetworkAppearance(8))
	if got := states[pointCode].Availability; got != DestinationUnavailable {
		t.Errorf("network 8 snapshot state = %v, want unavailable", got)
	}
}

// RFC 4666 Section 4.5.3: "For national networks, the SGP SHOULD additionally
// respond with a SCON message (if the destination is congested) before the DAVA
// or DRST."
//
// A congested destination was folded into the DAVA arm, so an auditing ASP was
// told the destination was available and nothing about the congestion it had
// asked after.
func TestDAUDForACongestedDestinationSendsSCONBeforeDAVA(t *testing.T) {
	conn, sent := newSSNMTestConn(t, StateASPActive, RoleSGP)
	seedDestinationNetworkState(conn, 0x333333, congestedState(2))

	if err := conn.handleDestinationStateAudit(messages.NewDestinationStateAudit(
		nil, nil, params.NewAffectedPointCode(0x333333), nil,
	)); err != nil {
		t.Fatalf("handleDestinationStateAudit: %v", err)
	}

	got := typeNames(*sent)
	if len(got) != 2 || got[0] != "Signalling Congestion" || got[1] != "Destination Available" {
		t.Errorf("answered a DAUD for a congested destination with %v, want [Signalling Congestion, Destination Available]", got)
	}
}

// An uncongested destination is answered with the DAVA alone, so the SCON is
// not sent indiscriminately.
func TestDAUDForAnAvailableDestinationSendsOnlyDAVA(t *testing.T) {
	conn, sent := newSSNMTestConn(t, StateASPActive, RoleSGP)
	// Known to be available. A point code the SG has never been told about is a
	// different case, answered with DUNA under Section 4.5.3: "An SG SHOULD
	// respond with a DUNA message when DAUD was received with an unknown
	// Signalling Point Code."
	seedDestinationAvailability(conn, 0x444444, DestinationAvailable)

	if err := conn.handleDestinationStateAudit(messages.NewDestinationStateAudit(
		nil, nil, params.NewAffectedPointCode(0x444444), nil,
	)); err != nil {
		t.Fatalf("handleDestinationStateAudit: %v", err)
	}

	if got := typeNames(*sent); len(got) != 1 || got[0] != "Destination Available" {
		t.Errorf("answered a DAUD for an available destination with %v, want [Destination Available]", got)
	}
}

// A DAUD can concern one Application Server on an association carrying many.
// Every response must retain that scope: replying with the SGP's whole
// configured set tells the ASP the destination state changed in sibling ASes
// it did not audit.
func TestDAUDResponsesKeepTheRequestedRoutingContext(t *testing.T) {
	for _, tt := range []struct {
		name  string
		state DestinationNetworkState
		want  int
	}{
		{name: "unavailable", state: availabilityState(DestinationUnavailable), want: 1},
		{name: "restricted", state: availabilityState(DestinationRestricted), want: 1},
		{name: "available", state: availabilityState(DestinationAvailable), want: 1},
		// A congested destination is answered in both dimensions: the SCON
		// that reports the congestion, then the DAVA that says it is still
		// reachable (RFC 4666 Section 4.5.3).
		{name: "congested", state: congestedState(2), want: 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conn, sent := newSSNMTestConn(t, StateASPActive, RoleSGP)
			const pointCode = 0x515151
			seedDestinationNetworkState(conn, pointCode, tt.state)

			if err := conn.handleDestinationStateAudit(messages.NewDestinationStateAudit(
				nil, params.NewRoutingContext(1),
				params.NewAffectedPointCodeWithMask(0, pointCode), nil,
			)); err != nil {
				t.Fatalf("handleDestinationStateAudit: %v", err)
			}
			if len(*sent) != tt.want {
				t.Fatalf("responses = %v, want %d", typeNames(*sent), tt.want)
			}
			for _, response := range *sent {
				routingContext := routingContextOf(response)
				if routingContext == nil {
					t.Fatalf("%s response omitted the audited Routing Context", response.MessageTypeName())
				}
				if got := routingContext.RoutingContexts(); len(got) != 1 || got[0] != 1 {
					t.Errorf("%s response Routing Contexts = %v, want [1]",
						response.MessageTypeName(), got)
				}
			}
		})
	}
}

// TestSignallingStatusCarriesBothDimensionsAfterEachMessage pins what an
// MTP3-User is told: each SSNM message moves one dimension, and the status it
// produces reports the other as this node currently holds it rather than as its
// zero value. RFC 4666 Section 4.5.3 has the two answered together, so a
// receiver that is told only the dimension that moved cannot reconstruct the
// destination's state without auditing for it.
func TestSignallingStatusCarriesBothDimensionsAfterEachMessage(t *testing.T) {
	conn, _ := newSSNMTestConn(t, StateASPActive, RoleASP)
	const pointCode = 0x222222
	affected := params.NewAffectedPointCodeWithMask(0, pointCode)

	if err := conn.handleDestinationUnavailable(messages.NewDestinationUnavailable(
		nil, nil, affected, nil,
	)); err != nil {
		t.Fatalf("handleDestinationUnavailable: %v", err)
	}
	assertStatusState(t, conn, DestinationNetworkState{Availability: DestinationUnavailable})

	if err := conn.handleSignallingCongestion(messages.NewSignallingCongestion(
		nil, nil, params.NewAffectedPointCodeWithMask(0, pointCode), nil,
		params.NewCongestionIndications(2), nil,
	)); err != nil {
		t.Fatalf("handleSignallingCongestion: %v", err)
	}
	assertStatusState(t, conn, DestinationNetworkState{
		Availability: DestinationUnavailable,
		Congestion:   CongestionState{Congested: true, Level: 2, LevelSet: true},
	})

	if err := conn.handleDestinationAvailable(messages.NewDestinationAvailable(
		nil, nil, params.NewAffectedPointCodeWithMask(0, pointCode), nil,
	)); err != nil {
		t.Fatalf("handleDestinationAvailable: %v", err)
	}
	assertStatusState(t, conn, DestinationNetworkState{
		Availability: DestinationAvailable,
		Congestion:   CongestionState{Congested: true, Level: 2, LevelSet: true},
	})
}

func assertStatusState(t *testing.T, conn *Association, want DestinationNetworkState) {
	t.Helper()
	select {
	case status := <-conn.SignallingStatus():
		if status.State != want {
			t.Fatalf("status state = %+v, want %+v", status.State, want)
		}
	default:
		t.Fatal("no destination status was published")
	}
}

// TestSignallingStatusFillsEachDimensionFromItsOwnRecord covers the case a
// single covering record cannot answer: a broad DUNA and a narrow SCON are two
// statements about the same point code, and the status has to take each
// dimension from the record that actually made that statement.
func TestSignallingStatusFillsEachDimensionFromItsOwnRecord(t *testing.T) {
	conn, _ := newSSNMTestConn(t, StateASPActive, RoleASP)
	const pointCode = 0x123456

	// The mask, not the value, is what makes this range cover the point code:
	// RFC 4666 Section 3.4.1's Affected Point Code wildcards the low-order bits,
	// so 0x123456/8 and 0x123456/0 are two different records and the first
	// covers the second.
	if err := conn.handleDestinationUnavailable(messages.NewDestinationUnavailable(
		nil, nil, params.NewAffectedPointCodeWithMask(8, pointCode), nil,
	)); err != nil {
		t.Fatalf("handleDestinationUnavailable for the covering range: %v", err)
	}
	assertStatusState(t, conn, DestinationNetworkState{Availability: DestinationUnavailable})

	if err := conn.handleSignallingCongestion(messages.NewSignallingCongestion(
		nil, nil, params.NewAffectedPointCodeWithMask(0, pointCode), nil,
		params.NewCongestionIndications(2), nil,
	)); err != nil {
		t.Fatalf("handleSignallingCongestion for one point code: %v", err)
	}
	// The congestion record names only this point code and says nothing about
	// reachability; the availability still comes from the range that covers it.
	assertStatusState(t, conn, DestinationNetworkState{
		Availability: DestinationUnavailable,
		Congestion:   CongestionState{Congested: true, Level: 2, LevelSet: true},
	})
}

// TestSignallingStatusLeavesTheUntouchedDimensionUnsetAcrossRoutingContexts
// covers a message naming several Application Servers, which need not agree
// about the destination. One status carries one value, so the dimension the
// message did not move is left unset rather than taken from whichever context
// happened to come first.
func TestSignallingStatusLeavesTheUntouchedDimensionUnsetAcrossRoutingContexts(t *testing.T) {
	conn, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
	conn.noteRoutingContextsActive([]uint32{1, 2})
	const pointCode = 0x123456

	if err := conn.handleDestinationUnavailable(messages.NewDestinationUnavailable(
		nil, params.NewRoutingContext(1), params.NewAffectedPointCodeWithMask(0, pointCode), nil,
	)); err != nil {
		t.Fatalf("handleDestinationUnavailable for Routing Context 1: %v", err)
	}
	assertStatusState(t, conn, DestinationNetworkState{Availability: DestinationUnavailable})

	if err := conn.handleSignallingCongestion(messages.NewSignallingCongestion(
		nil, params.NewRoutingContext(1, 2), params.NewAffectedPointCodeWithMask(0, pointCode), nil,
		params.NewCongestionIndications(2), nil,
	)); err != nil {
		t.Fatalf("handleSignallingCongestion across two Routing Contexts: %v", err)
	}
	assertStatusState(t, conn, DestinationNetworkState{
		Congestion: CongestionState{Congested: true, Level: 2, LevelSet: true},
	})
}

// TestSignallingStatusResolvesTheDimensionInItsOwnRoutingContext pins that the
// dimension a message did not move is read back in the Application Server the
// message named. RFC 4666 Section 4.3.1 keeps state per AS, so another context's
// DUNA is not an answer about this one.
func TestSignallingStatusResolvesTheDimensionInItsOwnRoutingContext(t *testing.T) {
	conn, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
	conn.noteRoutingContextsActive([]uint32{1, 2})
	const pointCode = 0x123456

	if err := conn.handleDestinationUnavailable(messages.NewDestinationUnavailable(
		nil, params.NewRoutingContext(1), params.NewAffectedPointCodeWithMask(0, pointCode), nil,
	)); err != nil {
		t.Fatalf("handleDestinationUnavailable for Routing Context 1: %v", err)
	}
	assertStatusState(t, conn, DestinationNetworkState{Availability: DestinationUnavailable})

	if err := conn.handleSignallingCongestion(messages.NewSignallingCongestion(
		nil, params.NewRoutingContext(2), params.NewAffectedPointCodeWithMask(0, pointCode), nil,
		params.NewCongestionIndications(2), nil,
	)); err != nil {
		t.Fatalf("handleSignallingCongestion for Routing Context 2: %v", err)
	}
	// Routing Context 2 has heard nothing about reachability, so it resolves to
	// the initial assumption rather than to Routing Context 1's DUNA.
	assertStatusState(t, conn, DestinationNetworkState{
		Availability: DestinationAvailable,
		Congestion:   CongestionState{Congested: true, Level: 2, LevelSet: true},
	})
}

// TestDUPUStatusLeavesTheDestinationStateAlone pins the one status that reports
// neither dimension. RFC 4666 Section 3.4.5 has DUPU report that a user part at
// an otherwise reachable destination is unavailable, so it writes no record and
// its status carries no claim about the destination — not even the claim that
// would be read back from what an earlier DUNA left behind. The MTP3-User acts
// on UserCause, and queries the destination separately if it needs it.
func TestDUPUStatusLeavesTheDestinationStateAlone(t *testing.T) {
	conn, _ := newSSNMTestConn(t, StateASPActive, RoleASP)
	const pointCode = 0x222222

	if err := conn.handleDestinationUnavailable(messages.NewDestinationUnavailable(
		nil, nil, params.NewAffectedPointCodeWithMask(0, pointCode), nil,
	)); err != nil {
		t.Fatalf("handleDestinationUnavailable: %v", err)
	}
	assertStatusState(t, conn, DestinationNetworkState{Availability: DestinationUnavailable})

	if err := conn.handleDestinationUserPartUnavailable(
		messages.NewDestinationUserPartUnavailable(
			nil, nil, params.NewAffectedPointCodeWithMask(0, pointCode),
			params.NewUserCause(params.SCCP, params.Unequipped), nil,
		),
	); err != nil {
		t.Fatalf("handleDestinationUserPartUnavailable: %v", err)
	}

	select {
	case status := <-conn.SignallingStatus():
		if !status.UserPartUnavailable {
			t.Fatal("DUPU status did not report an unavailable user part")
		}
		if status.State != (DestinationNetworkState{}) {
			t.Fatalf("DUPU status carried destination state %+v, want none", status.State)
		}
	default:
		t.Fatal("no destination status was published for the DUPU")
	}

	// The DUNA still stands underneath it: DUPU changed nothing it is retained.
	if got := retainedAvailability(conn, pointCode); got != DestinationUnavailable {
		t.Fatalf("retained availability after the DUPU = %v, want %v", got, DestinationUnavailable)
	}
}
