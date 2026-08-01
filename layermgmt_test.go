// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

// RFC 4666 Section 4.2.1 has the M3UA layer invoke an indication primitive to
// Layer Management "upon successful state changes". State() could only answer
// when asked, so a management layer had to poll for edges the library already
// knew about, and any transition between two polls was lost outright.
func TestStateChangesReportsEveryTransitionInOrder(t *testing.T) {
	conn, _ := newTestConn(t, StateAspDown, modeServer)

	for _, s := range []State{StateAspDown, StateAspInactive, StateAspActive} {
		if err := conn.handleStateUpdate(s); err != nil {
			t.Fatalf("handleStateUpdate(%v): %v", s, err)
		}
	}

	want := []State{StateAspDown, StateAspInactive, StateAspActive}
	for i, w := range want {
		select {
		case got := <-conn.StateChanges():
			if got != w {
				t.Errorf("event %d = %v, want %v", i, got, w)
			}
		default:
			t.Fatalf("only %d of %d transitions were reported", i, len(want))
		}
	}
}

// A restatement of the state the association is already in is not an edge. A
// caller counting transitions must not be handed one that did not happen.
func TestStateChangesDoesNotReportRestatements(t *testing.T) {
	conn, _ := newTestConn(t, StateAspDown, modeServer)

	if err := conn.handleStateUpdate(StateAspInactive); err != nil {
		t.Fatalf("handleStateUpdate: %v", err)
	}
	// Drain the genuine one.
	<-conn.StateChanges()

	// The same state again, twice: neither is an entry.
	for i := 0; i < 2; i++ {
		if err := conn.handleStateUpdate(StateAspInactive); err != nil {
			t.Fatalf("handleStateUpdate: %v", err)
		}
	}

	select {
	case got := <-conn.StateChanges():
		t.Errorf("reported %v for a restatement of the state already held", got)
	default:
	}
}

// Delivery must never block the dispatcher. A management layer that stops
// reading, or never reads at all, is a nuisance and not an outage: State()
// stays authoritative and the association carries on.
func TestStateChangesNeverBlocksTheDispatcher(t *testing.T) {
	conn, _ := newTestConn(t, StateAspDown, modeServer)

	// Far more transitions than the buffer holds, with nothing reading.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 500; i++ {
			conn.notifyStateChange(StateAspInactive)
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("notifyStateChange blocked once the buffer filled; a caller that " +
			"stops reading can stall the state machine")
	}
}

// A caller ranging over the channel has to see the association end, or it parks
// forever on a Conn that is already gone.
func TestStateChangesClosesWithTheConn(t *testing.T) {
	conn, _ := newTestConn(t, StateAspDown, modeServer)

	conn.closeStateChanges()

	select {
	case _, ok := <-conn.StateChanges():
		if ok {
			// Drain whatever was buffered, then the close must follow.
			for range conn.StateChanges() {
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatal("StateChanges() did not close; a range over it would never end")
	}
}

// Closing twice must not panic on an already-closed channel. Close and a
// teardown path can both reach it.
func TestStateChangesCloseIsIdempotent(t *testing.T) {
	conn, _ := newTestConn(t, StateAspDown, modeServer)

	conn.closeStateChanges()
	conn.closeStateChanges()
}

// Teardown is itself the final ASP state transition. Writing StateAspDown
// directly and then closing the channel leaves Layer Management seeing an
// active association simply disappear with no M-ASP_DOWN indication.
func TestClosePublishesOneFinalAspDownTransition(t *testing.T) {
	for _, initial := range []State{StateAspActive, StateAspInactive, StateAspDown} {
		t.Run(initial.String(), func(t *testing.T) {
			conn, _ := newTestConn(t, initial, modeServer)
			_ = conn.Close()

			var got []State
			for state := range conn.StateChanges() {
				got = append(got, state)
			}
			if initial == StateAspDown {
				if len(got) != 0 {
					t.Errorf("closing an already ASP-DOWN association published %v", got)
				}
				return
			}
			if len(got) != 1 || got[0] != StateAspDown {
				t.Errorf("final state transitions = %v, want [AspDown]", got)
			}
		})
	}
}

// A state update already queued when teardown begins must not resurrect the
// closed association after closeWith records ASP-DOWN. Both the publisher and
// the entry-action consumer race Close in production, so each boundary is
// pinned independently.
func TestClosedConnectionCannotReenterAnASPState(t *testing.T) {
	conn, _ := newTestConn(t, StateAspActive, modeServer)
	_ = conn.Close()

	conn.sendState(StateAspActive)
	if got := conn.State(); got != StateAspDown {
		t.Errorf("State after sendState on closed Conn = %v, want AspDown", got)
	}

	if err := conn.applyStateUpdate(StateAspActive); err != nil {
		t.Fatalf("applyStateUpdate after Close: %v", err)
	}
	if got := conn.State(); got != StateAspDown {
		t.Errorf("State after applyStateUpdate on closed Conn = %v, want AspDown", got)
	}
}

// RFC 4666 Section 4.2's M-SCTP_STATUS request "supports a Layer Management
// query of the local status of a particular SCTP association", answered from
// the local SCTP layer with no peer protocol involved.
//
// setUpSocket had always read this and kept only the negotiated outbound stream
// count, discarding the round-trip time, the retransmission timeout and the
// queue depths -- the numbers that separate a link that is slow from one that
// is failing.
func TestAssociationStatusReportsTheLiveAssociation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cliConn, srvConn, err := setupConn(t, ctx, 3211)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = cliConn.Close()
		_ = srvConn.Close()
	}()

	st, err := cliConn.AssociationStatus()
	if err != nil {
		t.Fatalf("AssociationStatus: %v", err)
	}

	// The association is up and carrying an M3UA handshake, so this is the only
	// state it can be in. Asserted by name because the underlying enum starts
	// at SCTP_EMPTY rather than SCTP_CLOSED: a table numbered from CLOSED = 0
	// renders an established association as COOKIE-ECHOED, and that off-by-one
	// is exactly what this pins.
	if st.State != "ESTABLISHED" {
		t.Errorf("State = %q, want %q", st.State, "ESTABLISHED")
	}
	if st.OutboundStreams == 0 {
		t.Error("OutboundStreams = 0; the association negotiated no outbound streams")
	}
	if st.InboundStreams == 0 {
		t.Error("InboundStreams = 0")
	}
	// A real path has a non-zero MTU and a congestion window; zero for both
	// would mean the primary path was never populated, which is the failure
	// mode of reading the wrong struct offset.
	if st.PrimaryMTU == 0 && st.PrimaryCongestionWindow == 0 {
		t.Error("primary path reports MTU 0 and cwnd 0; the peer address info " +
			"was not filled in")
	}
}

// The query has to fail rather than dereference a nil association once the Conn
// has never been given one.
func TestAssociationStatusWithoutAnAssociation(t *testing.T) {
	conn, _ := newTestConn(t, StateAspDown, modeClient)

	if _, err := conn.AssociationStatus(); err == nil {
		t.Error("AssociationStatus succeeded on a Conn with no SCTP association")
	}
}

// RFC 4666 Section 4.2: "M-NOTIFY indication and M-ERROR indication primitives
// indicate to Layer Management the notification or error information contained
// in a received M3UA Notify or Error message, respectively."
//
// Both messages were decoded correctly and then written to a log line and
// dropped. Section 4.3.4.5 makes the AS-state notifications advisory -- they do
// "not explicitly compel the ASP(s) receiving the message to become active" --
// so the decision belongs to the application, which could not see there was one
// to make.
func TestNotifyReachesLayerManagement(t *testing.T) {
	conn, _ := newTestConn(t, StateAspActive, modeClient)

	if err := conn.handleNotify(messages.NewNotify(
		params.NewStatus(params.AsStatePending), nil, nil, nil)); err != nil {
		t.Fatalf("handleNotify: %v", err)
	}

	select {
	case ind := <-conn.ManagementIndications():
		if ind.Kind != ManagementNotify {
			t.Errorf("Kind = %v, want %v", ind.Kind, ManagementNotify)
		}
		// AsStatePending packs Status Type 1 with Status Info 4. RFC 4666
		// Section 3.8.2 reserves Info 1 under Type 1, so the AS states start at
		// 2: AS-INACTIVE 2, AS-ACTIVE 3, AS-PENDING 4. The package's constant
		// block encodes that by letting the blank identifier consume the
		// reserved value, which is easy to misread as an off-by-one and is not.
		if ind.StatusType != 0x01 {
			t.Errorf("StatusType = %d, want 1 (AS state change)", ind.StatusType)
		}
		if ind.StatusInfo != 4 {
			t.Errorf("StatusInfo = %d, want 4 (AS-PENDING)", ind.StatusInfo)
		}
		if ind.Description == "" {
			t.Error("Description is empty; the indication cannot be logged without a lookup table")
		}
	default:
		t.Error("no M-NOTIFY indication; the Notify was decoded and discarded")
	}
}

func TestInvalidVersionNotifiesLayerManagement(t *testing.T) {
	conn, _ := newTestConn(t, StateAspActive, modeServer)
	raw := []byte{0x02, 0x00, messages.MsgClassASPSM, messages.MsgTypeAspUp, 0, 0, 0, 8}

	conn.dispatchRaw(context.Background(), inbound{data: raw, ppid: M3UAPPID})
	select {
	case indication := <-conn.ManagementIndications():
		if indication.Kind != ManagementError || indication.ErrorCode != params.InvalidVersionError {
			t.Errorf("indication = %#v, want M-ERROR Invalid Version", indication)
		}
		if indication.Description != "Invalid Version" {
			t.Errorf("Description = %q, want Invalid Version", indication.Description)
		}
	default:
		t.Fatal("unsupported version produced no Layer Management indication")
	}
}

func TestClosePublishesSCTPReleaseBeforeClosingManagement(t *testing.T) {
	conn, _ := newTestConn(t, StateAspActive, modeClient)
	_ = conn.closeWith(ErrHeartbeatExpired)

	indication, ok := <-conn.ManagementIndications()
	if !ok {
		t.Fatal("ManagementIndications closed before M-SCTP_RELEASE")
	}
	if indication.Kind != ManagementSCTPRelease {
		t.Fatalf("Kind = %v, want M-SCTP_RELEASE", indication.Kind)
	}
	if indication.Description != ErrHeartbeatExpired.Error() {
		t.Errorf("Description = %q, want %q", indication.Description, ErrHeartbeatExpired)
	}
	if _, ok := <-conn.ManagementIndications(); ok {
		t.Error("ManagementIndications remained open after release indication")
	}
}

// The M-ERROR half. A peer's Error is deliberately not answered and deliberately
// does not tear the association down (Section 3.8.1), which is exactly why it
// has to be reported: nothing else about it is observable.
func TestPeerErrorReachesLayerManagement(t *testing.T) {
	conn, _ := newTestConn(t, StateAspActive, modeClient)

	if err := conn.handleError(messages.NewError(
		params.NewErrorCode(params.UnexpectedMessageError), nil, nil, nil, nil)); err != nil {
		t.Fatalf("handleError: %v", err)
	}

	select {
	case ind := <-conn.ManagementIndications():
		if ind.Kind != ManagementError {
			t.Errorf("Kind = %v, want %v", ind.Kind, ManagementError)
		}
		if ind.ErrorCode != params.UnexpectedMessageError {
			t.Errorf("ErrorCode = %#x, want %#x", ind.ErrorCode, params.UnexpectedMessageError)
		}
		if ind.Description == "" {
			t.Error("Description is empty; the error code is unnamed")
		}
	default:
		t.Error("no M-ERROR indication; the Error was decoded and discarded")
	}
}

// As with the other two channels, a caller that stops reading must not be able
// to stall the dispatcher.
func TestManagementIndicationsNeverBlockTheDispatcher(t *testing.T) {
	conn, _ := newTestConn(t, StateAspActive, modeClient)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 500; i++ {
			conn.notifyManagement(&ManagementIndication{Kind: ManagementNotify})
		}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("notifyManagement blocked once the buffer filled")
	}
}

// A caller ranging over the channel has to see the association end.
func TestManagementIndicationsCloseWithTheConn(t *testing.T) {
	conn, _ := newTestConn(t, StateAspActive, modeClient)

	conn.closeManagement()
	// Idempotent: Close and a teardown path can both reach it.
	conn.closeManagement()

	select {
	case _, ok := <-conn.ManagementIndications():
		if ok {
			for range conn.ManagementIndications() {
			}
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ManagementIndications() did not close; a range over it would never end")
	}
}

// The restart watcher is only reachable if the kernel is actually sending the
// events, and that depends on a setsockopt made on every association. The unit
// tests drive the watcher with a synthetic event and prove nothing about the
// subscription, so this asserts the subscription itself against a live socket.
//
// It also pins the choice of option. SubscribeEvents -- the plural form -- sets
// the whole sctp_event_subscribe struct and would clear every subscription it
// was not told about; SubscribeEvent names one and leaves the rest alone. If
// the plural form is ever substituted, the data-io subscription it silently
// clears is not something this test would catch, but the association-change one
// it sets is, and the difference in mechanism is worth recording here.
func TestAssociationEventsAreSubscribedOnALiveAssociation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cliConn, srvConn, err := setupConn(t, ctx, 3213)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = cliConn.Close()
		_ = srvConn.Close()
	}()

	for name, c := range map[string]*Conn{"client": cliConn, "server": srvConn} {
		on, err := c.sctpConn.EventSubscribed(sctp.SCTP_ASSOC_CHANGE)
		if err != nil {
			t.Errorf("%s: EventSubscribed: %v", name, err)
			continue
		}
		if !on {
			t.Errorf("%s: SCTP_ASSOC_CHANGE is not subscribed; the kernel will never "+
				"deliver an association restart and M-SCTP_RESTART can never fire", name)
		}
		// The association id is what a shared handler routes on, so a zero here
		// would send every restart to the wrong Conn, or to none.
		if c.assocID.Load() == 0 {
			t.Errorf("%s: assocID is 0; a Listener's shared handler cannot route to this Conn", name)
		}
	}
}
