// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"encoding/binary"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

// assocChangeEvent builds a struct sctp_assoc_change exactly as the kernel
// delivers one, so the watcher can be driven without persuading a real peer to
// restart an association.
//
// The layout is RFC 6458 Section 6.1.1: type, flags and length, then state,
// error, the two stream counts and the association id, twenty octets in all. It
// is written in the host's byte order because that is how the kernel writes it
// -- these are notifications from the local stack, not anything off the wire.
func assocChangeEvent(state sctp.SCTPState, assocID uint32) []byte {
	b := make([]byte, 20)
	binary.NativeEndian.PutUint16(b[0:2], uint16(sctp.SCTP_ASSOC_CHANGE))
	binary.NativeEndian.PutUint16(b[2:4], 0)
	binary.NativeEndian.PutUint32(b[4:8], 20)
	binary.NativeEndian.PutUint16(b[8:10], uint16(state))
	binary.NativeEndian.PutUint16(b[10:12], 0)
	binary.NativeEndian.PutUint16(b[12:14], 2)
	binary.NativeEndian.PutUint16(b[14:16], 2)
	binary.NativeEndian.PutUint32(b[16:20], assocID)
	return b
}

// dispatchRestartMarker runs the ordered event a production dispatchLoop would
// consume. Synthetic watcher tests use buffered queues and no dispatcher.
func dispatchRestartMarker(t *testing.T, conn *Conn) {
	t.Helper()

	select {
	case event := <-conn.inboundChan:
		if event.kind != inboundSCTPRestart {
			t.Fatalf("restart queued inbound kind %d, want %d", event.kind, inboundSCTPRestart)
		}
		conn.dispatchInbound(context.Background(), event)
	default:
		t.Fatal("restart notification was not queued for ordered dispatch")
	}
}

// applyRestartTransition additionally runs the state update monitor() would
// consume after the dispatcher publishes ASP-DOWN.
func applyRestartTransition(t *testing.T, conn *Conn) {
	t.Helper()
	dispatchRestartMarker(t, conn)

	select {
	case state := <-conn.stateChan:
		if state != StateAspDown {
			t.Fatalf("restart published %v, want %v", state, StateAspDown)
		}
		if err := conn.handleStateUpdate(state); err != nil {
			t.Fatalf("applying restart state: %v", err)
		}
	default:
		t.Fatal("restart published no ASP-DOWN transition")
	}
}

// A restart leaves the association usable, so no read fails even though the
// M3UA state is reset underneath it. RFC 4666 Section 1.6.3 gives that event a
// Layer Management primitive of its own so the reset is not silent.
func TestSCTPRestartIsReportedToLayerManagement(t *testing.T) {
	conn, _ := newTestConn(t, StateAspActive, modeClient)
	conn.assocID.Store(7)

	w := &restartWatcher{}
	w.setRoute(func(id sctp.SCTPAssocID) *Conn {
		if int32(id) == conn.assocID.Load() {
			return conn
		}
		return nil
	})

	if err := w.handle(assocChangeEvent(sctp.SCTP_RESTART, 7)); err != nil {
		t.Fatalf("handle returned %v; an event must never fail the read", err)
	}
	dispatchRestartMarker(t, conn)

	select {
	case ind := <-conn.ManagementIndications():
		if ind.Kind != ManagementSCTPRestart {
			t.Errorf("Kind = %v, want %v", ind.Kind, ManagementSCTPRestart)
		}
		if ind.Description == "" {
			t.Error("Description is empty")
		}
	default:
		t.Error("no M-SCTP_RESTART indication for an SCTP_RESTART event")
	}
}

// RFC 4666 Section 4.3.3 requires an SCTP-RESTART to move the ASP to ASP-DOWN.
// Figure 3 makes that transition association-wide, not limited to whichever
// Application Server last carried traffic, so the SGP must stop selecting the
// restarted ASP in every configured AS and discard its old activation scope.
func TestSCTPRestartMovesRemoteASPDownInEveryApplicationServer(t *testing.T) {
	conn, _ := newTestConn(t, StateAspActive, modeServer)
	conn.assocID.Store(7)
	if err := conn.handleStateUpdate(StateAspActive); err != nil {
		t.Fatalf("entering ASP-ACTIVE: %v", err)
	}
	<-conn.StateChanges()

	registry := newApplicationServers(time.Hour)
	t.Cleanup(func() {
		for _, rtCtx := range []uint32{1, 2} {
			as := registry.get(rtCtx)
			as.mu.Lock()
			as.stopRecoveryLocked()
			as.mu.Unlock()
		}
	})
	conn.as = registry
	conn.noteRoutingContextsActive([]uint32{1, 2})
	registry.aspStateChanged(conn, StateAspActive)
	for _, rtCtx := range []uint32{1, 2} {
		if got := registry.get(rtCtx).activeASPs(); len(got) != 1 || got[0] != conn {
			t.Fatalf("Routing Context %d active ASPs before restart = %v, want the peer", rtCtx, got)
		}
	}

	w := &restartWatcher{}
	w.setRoute(func(sctp.SCTPAssocID) *Conn { return conn })
	if err := w.handle(assocChangeEvent(sctp.SCTP_RESTART, 7)); err != nil {
		t.Fatalf("handle returned %v; an event must never fail the read", err)
	}

	applyRestartTransition(t, conn)
	if got := conn.State(); got != StateAspDown {
		t.Errorf("State after dispatched restart = %v, want %v", got, StateAspDown)
	}

	select {
	case got := <-conn.StateChanges():
		if got != StateAspDown {
			t.Errorf("state indication = %v, want %v", got, StateAspDown)
		}
	default:
		t.Error("Layer Management was not told that the ASP moved to ASP-DOWN")
	}

	for _, rtCtx := range []uint32{1, 2} {
		if got := registry.get(rtCtx).activeASPs(); len(got) != 0 {
			t.Errorf("Routing Context %d still has %d active ASPs after restart", rtCtx, len(got))
		}
		if got := registry.get(rtCtx).State(); got != ASPending {
			t.Errorf("Routing Context %d AS state = %v, want %v while T(r) runs", rtCtx, got, ASPending)
		}
	}

	conn.muAckedRCs.RLock()
	activeCount, scoped := len(conn.activeRCs), conn.activeRCsScoped
	conn.muAckedRCs.RUnlock()
	if activeCount != 0 || scoped {
		t.Errorf("old per-AS activation survived restart: %d contexts, scoped=%v", activeCount, scoped)
	}
}

// Section 4.3.3 further says that an ASP receiving SCTP-RESTART must start
// recovery with ASP Up, and must issue MTP-PAUSE for every affected SS7
// destination. The restarted association is still usable, so Close cannot do
// either job on this path.
func TestSCTPRestartAtASPStartsASPUpRecoveryAndPausesDestinations(t *testing.T) {
	conn, sent := newTestConn(t, StateAspActive, modeClient)
	conn.cfg.TAck = time.Hour
	conn.assocID.Store(7)
	if err := conn.handleStateUpdate(StateAspActive); err != nil {
		t.Fatalf("entering ASP-ACTIVE: %v", err)
	}
	<-conn.StateChanges()

	conn.noteRoutingContextsAcked(params.NewRoutingContext(1))
	conn.noteRoutingContextsOverridden([]uint32{2})
	available := destinationKey{networkAppearance: 8, networkAppearanceSet: true, pointCode: 0x111111}
	alreadyDown := destinationKey{networkAppearance: 8, networkAppearanceSet: true, pointCode: 0x222222}
	conn.destinations.set(available, DestinationAvailable)
	conn.destinations.set(alreadyDown, DestinationUnavailable)

	w := &restartWatcher{}
	w.setRoute(func(sctp.SCTPAssocID) *Conn { return conn })
	if err := w.handle(assocChangeEvent(sctp.SCTP_RESTART, 7)); err != nil {
		t.Fatalf("handle returned %v; an event must never fail the read", err)
	}
	applyRestartTransition(t, conn)
	if got := conn.State(); got != StateAspDown {
		t.Errorf("State after dispatched restart = %v, want %v", got, StateAspDown)
	}
	select {
	case got := <-conn.StateChanges():
		if got != StateAspDown {
			t.Errorf("state indication = %v, want %v", got, StateAspDown)
		}
	default:
		t.Error("Layer Management was not told that the local ASP moved to ASP-DOWN")
	}

	if got := typeNames(*sent); len(got) != 1 || got[0] != "ASP Up" {
		t.Errorf("signals after restart = %v, want [ASP Up]", got)
	}
	if got := conn.DestinationStateForNetwork(8, available.pointCode); got != DestinationUnavailable {
		t.Errorf("affected destination state = %v, want %v", got, DestinationUnavailable)
	}
	select {
	case status := <-conn.SignallingStatus():
		if status.PointCode != available.pointCode || status.NetworkAppearance != 8 ||
			!status.NetworkAppearanceSet || status.State != DestinationUnavailable {
			t.Errorf("MTP-PAUSE equivalent = %+v, want Network Appearance 8 point code %#x unavailable",
				status, available.pointCode)
		}
	default:
		t.Error("the affected destination produced no MTP-PAUSE equivalent")
	}
	select {
	case extra := <-conn.SignallingStatus():
		t.Errorf("already-unavailable destination produced a duplicate pause: %+v", extra)
	default:
	}

	conn.muAckedRCs.RLock()
	ackedCount, ackedScoped := len(conn.ackedRCs), conn.ackedRCsScoped
	overriddenCount := len(conn.overriddenRCs)
	conn.muAckedRCs.RUnlock()
	if ackedCount != 0 || ackedScoped || overriddenCount != 0 {
		t.Errorf("old activation survived restart: %d acknowledged (scoped=%v), %d overridden contexts",
			ackedCount, ackedScoped, overriddenCount)
	}
}

// SCTP restart starts a new association epoch even though the socket stays
// open. Requests from the old epoch cannot be retransmitted after the fresh
// ASP Up, or the peer can receive ASP Active/Inactive/Down before the new
// ASP-Up procedure has re-established the M3UA relationship.
func TestSCTPRestartCancelsEveryOldTAckBeforeFreshAspUp(t *testing.T) {
	conn, snapshot := tackConn(t, StateAspActive, 10*time.Millisecond, 100)
	conn.assocID.Store(7)
	conn.startTAck(messages.NewAspUp(nil, params.NewInfoString("old")), requestAspUp)
	conn.startTAck(messages.NewAspActive(
		conn.cfg.TrafficModeType.Copy(), params.NewRoutingContext(1), nil,
	), requestAspActive)
	conn.startTAck(messages.NewAspInactive(params.NewRoutingContext(2), nil), requestAspInactive)
	conn.startTAck(messages.NewAspDown(params.NewInfoString("old")), requestAspDown)

	w := &restartWatcher{}
	w.setRoute(func(sctp.SCTPAssocID) *Conn { return conn })
	if err := w.handle(assocChangeEvent(sctp.SCTP_RESTART, 7)); err != nil {
		t.Fatal(err)
	}
	applyRestartTransition(t, conn)

	if got := conn.pendingTAck(); got != 1 {
		t.Fatalf("pending T(ack) after restart = %d, want only the fresh ASP Up", got)
	}
	if !waitFor(func() bool { return countType(snapshot(), "ASP Up") >= 2 }, time.Second) {
		t.Fatal("fresh ASP Up was not protected by T(ack)")
	}
	for _, stale := range []string{"ASP Active", "ASP Inactive", "ASP Down"} {
		if got := countType(snapshot(), stale); got != 0 {
			t.Errorf("old-epoch %s retransmitted %d times after restart", stale, got)
		}
	}
}

// An old timer may already be inside its socket write when the SCTP restart is
// dispatched. The restart boundary must wait for that attempt to finish before
// it publishes ASP-DOWN; otherwise the old ASPTM request can land after the
// fresh ASP Up and invert the required recovery order.
func TestSCTPRestartFencesAnInFlightOldTAckWrite(t *testing.T) {
	conn, _ := newTestConn(t, StateAspActive, modeClient)
	conn.cfg.TAck = 10 * time.Millisecond
	conn.cfg.TAckRetries = 100

	oldWriteStarted := make(chan struct{})
	releaseOldWrite := make(chan struct{})
	oldWriteFinished := make(chan struct{})
	var oldWriteOnce sync.Once
	conn.signalWriter = func(message messages.M3UA) (int, error) {
		if _, ok := message.(*messages.AspActive); ok {
			oldWriteOnce.Do(func() { close(oldWriteStarted) })
			<-releaseOldWrite
			select {
			case <-oldWriteFinished:
			default:
				close(oldWriteFinished)
			}
		}
		return message.MarshalLen(), nil
	}
	conn.startTAck(messages.NewAspActive(
		conn.cfg.TrafficModeType.Copy(), conn.cfg.RoutingContexts.Copy(), nil,
	), requestAspActive)

	select {
	case <-oldWriteStarted:
	case <-time.After(time.Second):
		t.Fatal("old T(ack) did not enter its retransmission write")
	}

	restartDone := make(chan struct{})
	go func() {
		conn.handleSCTPRestart()
		close(restartDone)
	}()
	select {
	case <-restartDone:
		t.Error("restart crossed an in-flight old-epoch T(ack) write")
	case <-time.After(25 * time.Millisecond):
	}
	close(releaseOldWrite)
	select {
	case <-oldWriteFinished:
	case <-time.After(time.Second):
		t.Fatal("old T(ack) write did not finish")
	}
	select {
	case <-restartDone:
	case <-time.After(time.Second):
		t.Fatal("restart did not finish after the old write drained")
	}

	select {
	case state := <-conn.stateChan:
		if state != StateAspDown {
			t.Fatalf("restart state = %v, want ASP-DOWN", state)
		}
		if err := conn.handleStateUpdate(state); err != nil {
			t.Fatalf("apply restart: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("restart published no ASP-DOWN state")
	}
}

// Before the new ASP-Up procedure completes, no ASPTM acknowledgement can
// belong to the new association epoch. A delayed Ack from before the restart
// must therefore be reported/ignored without activating or inactivating it.
func TestSCTPRestartRejectsDelayedOldASPTMAcksUntilAspUpCompletes(t *testing.T) {
	conn, _ := newTestConn(t, StateAspActive, modeClient)
	conn.cfg.TAck = time.Hour
	conn.startTAck(messages.NewAspActive(
		conn.cfg.TrafficModeType.Copy(), params.NewRoutingContext(1), nil,
	), requestAspActive)
	conn.startTAck(messages.NewAspInactive(params.NewRoutingContext(2), nil), requestAspInactive)

	conn.handleSCTPRestart()
	select {
	case state := <-conn.stateChan:
		if err := conn.handleStateUpdate(state); err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("restart state was not published")
	}
	if got := conn.State(); got != StateAspDown {
		t.Fatalf("state after restart = %v, want ASP-DOWN", got)
	}

	conn.handleSignals(context.Background(), messages.NewAspActiveAck(
		conn.cfg.TrafficModeType.Copy(), params.NewRoutingContext(1), nil,
	))
	conn.handleSignals(context.Background(), messages.NewAspInactiveAck(
		params.NewRoutingContext(2), nil,
	))
	if got := conn.State(); got != StateAspDown {
		t.Errorf("delayed old ASPTM Ack changed state to %v before fresh ASP Up Ack", got)
	}
	conn.muAckedRCs.RLock()
	ackedCount, ackedScoped := len(conn.ackedRCs), conn.ackedRCsScoped
	conn.muAckedRCs.RUnlock()
	if ackedCount != 0 || ackedScoped {
		t.Errorf("delayed old ASP Active Ack restored activation scope: %d contexts, scoped=%t",
			ackedCount, ackedScoped)
	}
}

// A second SCTP restart can arrive while the first restart's ASP Up is still
// awaiting its Ack. ASP-DOWN is then a restatement rather than a state entry,
// so relying only on the entry action cancels the old timer and starts no
// replacement. Every SCTP epoch still requires its own fresh ASP Up.
func TestRepeatedSCTPRestartWhileAspDownStartsAnotherFreshAspUp(t *testing.T) {
	conn, snapshot := tackConn(t, StateAspActive, time.Hour, 10)

	conn.handleSCTPRestart()
	select {
	case state := <-conn.stateChan:
		if err := conn.handleStateUpdate(state); err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("first restart state was not published")
	}
	if got := countType(snapshot(), "ASP Up"); got != 1 {
		t.Fatalf("ASP Up writes after first restart = %d, want 1", got)
	}

	conn.handleSCTPRestart()
	select {
	case state := <-conn.stateChan:
		if err := conn.handleStateUpdate(state); err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("second restart state was not published")
	}
	if got := countType(snapshot(), "ASP Up"); got != 2 {
		t.Errorf("ASP Up writes after second restart = %d, want one fresh request per epoch", got)
	}
	if got := conn.pendingTAck(); got != 1 {
		t.Errorf("pending T(ack) after second restart = %d, want its fresh ASP Up", got)
	}
}

// The restart gate is temporary: once the fresh ASP Up is acknowledged, the
// new epoch's ASP Active procedure and Ack must work normally. Leaving the gate
// armed would replace stale-state protection with a permanently inactive ASP.
func TestSCTPRestartAllowsASPTMAcksAfterFreshAspUpAck(t *testing.T) {
	conn, _ := newTestConn(t, StateAspActive, modeClient)
	conn.cfg.TAck = time.Hour

	conn.handleSCTPRestart()
	select {
	case state := <-conn.stateChan:
		if err := conn.handleStateUpdate(state); err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("restart state was not published")
	}
	conn.handleSignals(context.Background(), messages.NewAspUpAck(nil, nil))
	select {
	case state := <-conn.stateChan:
		if state != StateAspInactive {
			t.Fatalf("fresh ASP Up Ack published %v, want ASP-INACTIVE", state)
		}
		if err := conn.handleStateUpdate(state); err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("fresh ASP Up Ack published no state")
	}

	conn.handleSignals(context.Background(), messages.NewAspActiveAck(
		conn.cfg.TrafficModeType.Copy(), conn.cfg.RoutingContexts.Copy(), nil,
	))
	if got := conn.State(); got != StateAspActive {
		t.Errorf("state after new-epoch ASP Active Ack = %v, want ASP-ACTIVE", got)
	}

	// Once recovery is complete, ordinary RFC 4666 Section 4.3.4.4
	// unsolicited-Ack semantics are restored. This has no matching timer with
	// which to bypass a restart gate accidentally left armed.
	conn.handleSignals(context.Background(), messages.NewAspInactiveAck(
		conn.cfg.RoutingContexts.Copy(), nil,
	))
	if got := conn.State(); got != StateAspInactive {
		t.Errorf("post-recovery unsolicited ASP Inactive Ack left state %v, want ASP-INACTIVE", got)
	}
}

// The dependency invokes the notification handler on the SCTP reader while the
// M3UA dispatcher may still be handling the message read immediately before
// it. Restart therefore has to enter that same ordered dispatch queue. Sending
// ASP-DOWN straight from the reader creates a second state publisher: this
// earlier ASP Active can then publish ASP-ACTIVE after the later restart and
// resurrect a peer whose SCTP state was reset.
func TestSCTPRestartIsSerializedAfterEarlierInboundMessage(t *testing.T) {
	conn, _ := newTestConn(t, StateAspInactive, modeServer)
	conn.assocID.Store(7)
	if err := conn.handleStateUpdate(StateAspInactive); err != nil {
		t.Fatalf("entering ASP-INACTIVE: %v", err)
	}
	<-conn.StateChanges()

	handlerStarted := make(chan struct{})
	allowHandlerToFinish := make(chan struct{})
	conn.signalWriter = func(m3 messages.M3UA) (int, error) {
		if m3.MessageTypeName() == "ASP Active Ack" {
			close(handlerStarted)
			<-allowHandlerToFinish
		}
		return m3.MarshalLen(), nil
	}

	aspActive := messages.NewAspActive(
		params.NewTrafficModeType(params.TrafficModeLoadshare),
		params.NewRoutingContext(1), nil)
	raw, err := aspActive.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal ASP Active: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go conn.dispatchLoop(ctx, conn.inboundChan)
	conn.inboundChan <- inbound{kind: inboundMessage, data: raw, stream: 0}
	select {
	case <-handlerStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("the earlier ASP Active never reached its handler")
	}

	w := &restartWatcher{}
	w.setRoute(func(sctp.SCTPAssocID) *Conn { return conn })
	if err := w.handle(assocChangeEvent(sctp.SCTP_RESTART, 7)); err != nil {
		t.Fatalf("handle restart: %v", err)
	}
	close(allowHandlerToFinish)

	want := []State{StateAspActive, StateAspDown}
	for i, expected := range want {
		select {
		case state := <-conn.stateChan:
			if state != expected {
				t.Errorf("published state %d = %v, want %v; restart was reordered around the earlier message",
					i, state, expected)
			}
			if err := conn.handleStateUpdate(state); err != nil {
				t.Fatalf("apply state %v: %v", state, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d of %d state publications arrived", i, len(want))
		}
	}
	if got := conn.State(); got != StateAspDown {
		t.Errorf("final state = %v, want %v after the later SCTP restart", got, StateAspDown)
	}
}

// Every other association state already reaches the user by another route --
// COMM_LOST and SHUTDOWN_COMP fail the read, which surfaces through Err() --
// so reporting them here would double-report them under a name that does not
// fit.
func TestNonRestartAssociationEventsAreNotReported(t *testing.T) {
	for _, st := range []sctp.SCTPState{
		sctp.SCTP_COMM_UP, sctp.SCTP_COMM_LOST,
		sctp.SCTP_SHUTDOWN_COMP, sctp.SCTP_CANT_STR_ASSOC,
	} {
		conn, _ := newTestConn(t, StateAspActive, modeClient)
		w := &restartWatcher{}
		w.setRoute(func(sctp.SCTPAssocID) *Conn { return conn })

		if err := w.handle(assocChangeEvent(st, 1)); err != nil {
			t.Fatalf("handle(%v) returned %v", st, err)
		}

		select {
		case ind := <-conn.ManagementIndications():
			t.Errorf("state %v produced a %v indication; only SCTP_RESTART should",
				st, ind.Kind)
		default:
		}
		select {
		case event := <-conn.inboundChan:
			t.Errorf("state %v queued inbound event kind %d; only SCTP_RESTART should", st, event.kind)
		default:
		}
	}
}

// A shared handler must deliver to the association the event names and to no
// other. A Listener installs one handler for every ASP it serves, so getting
// this wrong tells the wrong ASP that its peer restarted.
func TestRestartIsRoutedToTheNamedAssociationOnly(t *testing.T) {
	a, _ := newTestConn(t, StateAspActive, modeServer)
	b, _ := newTestConn(t, StateAspActive, modeServer)
	a.assocID.Store(11)
	b.assocID.Store(22)

	w := &restartWatcher{}
	w.setRoute(func(id sctp.SCTPAssocID) *Conn {
		switch int32(id) {
		case a.assocID.Load():
			return a
		case b.assocID.Load():
			return b
		}
		return nil
	})

	if err := w.handle(assocChangeEvent(sctp.SCTP_RESTART, 22)); err != nil {
		t.Fatal(err)
	}
	dispatchRestartMarker(t, b)

	select {
	case <-b.ManagementIndications():
	default:
		t.Error("the association named in the event was not told about its restart")
	}
	select {
	case ind := <-a.ManagementIndications():
		t.Errorf("an unrelated association was told about a %v it did not have", ind.Kind)
	default:
	}
	select {
	case event := <-a.inboundChan:
		t.Errorf("an unrelated association received inbound event kind %d", event.kind)
	default:
	}
}

// An event this layer cannot parse, or one for an association it does not know,
// must not fail the read: the dependency propagates a handler error out of the
// read, which would kill an association over an event that is not even ours.
func TestUnparseableOrUnknownEventsDoNotFailTheRead(t *testing.T) {
	w := &restartWatcher{}
	w.setRoute(func(sctp.SCTPAssocID) *Conn { return nil })

	for _, b := range [][]byte{
		nil,
		{0x01},
		make([]byte, 8),
		assocChangeEvent(sctp.SCTP_RESTART, 999),
	} {
		if err := w.handle(b); err != nil {
			t.Errorf("handle(%d bytes) = %v, want nil", len(b), err)
		}
	}

	// And with no route installed at all, which is the window between the
	// socket being created and Dial having a Conn to route to.
	empty := &restartWatcher{}
	if err := empty.handle(assocChangeEvent(sctp.SCTP_RESTART, 1)); err != nil {
		t.Errorf("handle with no route = %v, want nil", err)
	}
}
