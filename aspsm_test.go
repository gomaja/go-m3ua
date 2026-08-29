// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

// newTestConn builds an Association in the given state whose outbound signals are
// captured instead of being written to an SCTP association, so the ASPSM/ASPTM
// handlers can be exercised without a network.
//
// Every channel a handler might touch is initialised and done is closed on
// cleanup, so a test that reaches an unexpected arm fails or unblocks rather
// than hanging until the package timeout.
func newTestConn(t *testing.T, state State, role Role) (*Association, *[]messages.M3UA) {
	t.Helper()

	var sent []messages.M3UA

	cfg := newSGPAssociationConfigForTest(
		&HeartbeatInfo{Enabled: false},
		0x22222222,                  // OriginatingPointCode
		0x11111111,                  // DestinationPointCode
		1,                           // AspIdentifier
		params.TrafficModeLoadshare, // TrafficModeType
		0,                           // NetworkAppearance
		0,                           // CorrelationID
		[]uint32{1, 2},              // RoutingContexts
		params.ServiceIndSCCP,       // ServiceIndicator
		0,                           // NetworkIndicator
		0,                           // MessagePriority
		1,                           // SignalingLinkSelection
	)
	cfg.CorrelationID = nil
	cfg.NetworkAppearance = nil

	conn := &Association{
		// The transport is per-Association, so the send template lives here rather
		// than on the shared Config; these tests write through signalWriter and
		// never touch a socket, but StreamID() reads it.
		sctpInfo:    &sctp.SndRcvInfo{PPID: M3UAPPID, Stream: 0},
		muState:     new(sync.RWMutex),
		role:        role,
		state:       state,
		stateChan:   make(chan State, 8),
		inboundChan: make(chan inbound, 8),
		errChan:     make(chan error, 8),
		established: make(chan struct{}, 1),
		beatAckChan: make(chan struct{}, 1),
		beatStart:   make(chan struct{}),
		dataChan:    make(chan *DataMessage, 8),
		done:        make(chan struct{}),
		cfg:         cfg,
		// Matches Dial/Accept: SSNM handling needs both, and a nil map would
		// panic on the first destination update.
		destinations: newDestinations(),
		tack:         newTAckRetransmitter(),
		statusChan:   make(chan *DestinationStatus, 64),
		// Like statusChan: a handler may report a transition on it, and
		// closeStateChanges closes it on teardown, so a nil here would drop
		// every event silently and then panic closing a nil channel.
		stateEventChan: make(chan State, 16),
		mgmtChan:       make(chan *ManagementIndication, 64),
	}
	conn.signalWriter = func(m3 messages.M3UA) (int, error) {
		sent = append(sent, m3)
		return m3.MarshalLen(), nil
	}
	// Use the production once-only teardown. Closing done directly leaves
	// closeOnce unclaimed, so an asynchronous failure that later reaches
	// closeWith can close the same channel a second time and panic during test
	// cleanup rather than report the assertion that actually failed.
	t.Cleanup(func() { _ = conn.closeWith(ErrAssociationClosed) })

	return conn, &sent
}

// typeNames renders the captured signals as message type names for assertions.
func typeNames(msgs []messages.M3UA) []string {
	names := make([]string, 0, len(msgs))
	for _, m := range msgs {
		names = append(names, m.MessageTypeName())
	}
	return names
}

// errorCodes extracts the error codes of every Error message in the capture.
func errorCodes(msgs []messages.M3UA) []uint32 {
	var codes []uint32
	for _, m := range msgs {
		if e, ok := m.(*messages.Error); ok {
			codes = append(codes, e.ErrorCode.ErrorCode())
		}
	}
	return codes
}

// RFC 4666 Section 4.3.4.1:
//
//	"The SGP MUST send an ASP Up Ack message in response to a received ASP Up
//	message even if the ASP is already marked as ASP-INACTIVE at the SGP."
func TestHandleAspUpAlwaysAcks(t *testing.T) {
	for _, tt := range []struct {
		name  string
		state State
	}{
		{"from AspDown", StateASPDown},
		{"from AspInactive", StateASPInactive},
		{"from AspActive", StateASPActive},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conn, sent := newTestConn(t, tt.state, RoleSGP)

			_ = conn.handleAspUp(messages.NewAspUp(conn.cfg.ASPIdentifier, nil))

			if len(*sent) == 0 {
				t.Fatalf("no signal sent; want an AspUpAck")
			}
			if got := (*sent)[0].MessageTypeName(); got != "ASP Up Ack" {
				t.Errorf("first signal = %q, want %q (sent: %v)", got, "ASP Up Ack", typeNames(*sent))
			}
		})
	}
}

// RFC 4666 Section 4.3.4.1:
//
//	"If an ASP Up message is received and, internally, the remote ASP is
//	already in the ASP-INACTIVE state, an ASP Up Ack message is returned, and
//	no further action is taken."
func TestHandleAspUpFromInactiveSendsNoError(t *testing.T) {
	conn, sent := newTestConn(t, StateASPInactive, RoleSGP)

	if err := conn.handleAspUp(messages.NewAspUp(conn.cfg.ASPIdentifier, nil)); err != nil {
		t.Errorf("handleAspUp() error = %v, want nil (no further action)", err)
	}

	if codes := errorCodes(*sent); len(codes) != 0 {
		t.Errorf("Error message(s) sent with codes %v, want none; sent: %v", codes, typeNames(*sent))
	}
}

// RFC 4666 Section 4.3.4.1:
//
//	"If an ASP Up message is received and, internally, the remote ASP is in
//	the ASP-ACTIVE state, an ASP Up Ack message is returned, as well as an
//	Error message ("Unexpected Message")."
func TestHandleAspUpFromActiveAcksThenErrors(t *testing.T) {
	conn, sent := newTestConn(t, StateASPActive, RoleSGP)

	err := conn.handleAspUp(messages.NewAspUp(conn.cfg.ASPIdentifier, nil))
	if err == nil {
		t.Fatal("handleAspUp() error = nil, want UnexpectedMessageError")
	}
	var unexpected *UnexpectedMessageError
	if !errors.As(err, &unexpected) {
		t.Fatalf("handleAspUp() error = %T (%v), want *UnexpectedMessageError", err, err)
	}

	// The Error message itself is emitted by handleErrors, which monitor()
	// drives from errChan. Run it here so the full RFC-mandated sequence
	// (ASP Up Ack followed by Error "Unexpected Message") is asserted.
	if e := conn.handleErrors(err); e != nil {
		t.Fatalf("handleErrors() error = %v, want nil", e)
	}

	got := typeNames(*sent)
	if len(got) != 2 || got[0] != "ASP Up Ack" || got[1] != "Error" {
		t.Fatalf("signals = %v, want [ASP Up Ack Error]", got)
	}

	codes := errorCodes(*sent)
	if len(codes) != 1 || codes[0] != params.UnexpectedMessageError {
		t.Errorf("error codes = %v, want [%d] (Unexpected Message)", codes, params.UnexpectedMessageError)
	}
}

// RFC 4666 Section 4.3.4.1: receiving ASP Up while ASP-ACTIVE moves the remote
// ASP back to ASP-INACTIVE, so the peers converge instead of deadlocking.
func TestHandleAspUpFromActiveTransitionsToInactive(t *testing.T) {
	conn, _ := newTestConn(t, StateASPActive, RoleSGP)

	conn.handleSignals(context.Background(), messages.NewAspUp(conn.cfg.ASPIdentifier, nil))

	select {
	case got := <-conn.stateChan:
		if got != StateASPInactive && got != stateUnchanged {
			t.Errorf("state = %v, want %v", got, StateASPInactive)
		}
	default:
		t.Fatal("no state update published; want AspInactive")
	}
}

// RFC 4666 Section 4.3.4.1: an ASP that is already ASP-INACTIVE and receives a
// duplicate ASP Up Ack "should consider itself in the ASP-INACTIVE state" — no
// Error is warranted, otherwise a retransmitting peer is answered with errors.
func TestHandleAspUpAckFromInactiveIsAccepted(t *testing.T) {
	conn, sent := newTestConn(t, StateASPInactive, RoleASP)

	if err := conn.handleAspUpAck(messages.NewAspUpAck(conn.cfg.ASPIdentifier, nil)); err != nil {
		t.Errorf("handleAspUpAck() error = %v, want nil", err)
	}

	if got := typeNames(*sent); len(got) != 0 {
		t.Errorf("signals = %v, want none", got)
	}
}

// Every handled message must publish exactly one state. monitor() consumes
// stateChan to drive handleStateUpdate, which is what applies a transition and
// signals establishment: publishing none silently drops the transition the
// message called for, and publishing two applies a spurious extra one. (Reads
// no longer depend on this — readLoop() owns them — but the state machine
// still does.)
// It enumerates the full RFC 4666 message-class space (0-9, through RKM) and
// every message-type value any class uses (0-8): defined combinations parse to
// their concrete types, and every other combination parses as
// *messages.Generic and must hit the default dispatch arm — so a new dispatch
// arm, or an early return added to an existing one, cannot escape the
// invariant. newProdConn keeps beatAckChan and established at their production
// capacities — the ones this invariant depends on — while stateChan/errChan
// stay buffered so publishes can be counted without a monitor() reader
// draining them.
func TestExactlyOneStatePublishedPerMessage(t *testing.T) {
	writeFails := func(m3 messages.M3UA) (int, error) { return 0, errors.New("sctp write failed") }

	for class := uint8(0); class <= 9; class++ {
		for typ := uint8(0); typ <= 8; typ++ {
			msg, err := messages.Parse([]byte{1, 0, class, typ, 0, 0, 0, 8})
			if err != nil {
				msg = minimallyValidMessage(class, typ)
				if msg == nil {
					t.Fatalf("messages.Parse rejected class=%d type=%d without a valid typed fixture: %v", class, typ, err)
				}
			}
			for _, st := range []State{StateASPDown, StateASPInactive, StateASPActive} {
				for _, role := range []Role{RoleSGP, RoleASP} {
					for _, fail := range []bool{false, true} {
						name := fmt.Sprintf("%s/%v/role=%s/writefail=%t",
							msg.MessageTypeName(), st, role, fail)

						t.Run(name, func(t *testing.T) {
							conn := newProdConn(t, st, role)
							if fail {
								conn.signalWriter = writeFails
							}

							done := make(chan struct{})
							go func() {
								conn.handleSignals(context.Background(), msg)
								close(done)
							}()
							select {
							case <-done:
							case <-time.After(2 * time.Second):
								t.Fatalf("handleSignals wedged: the dispatch goroutine never returned")
							}

							states := len(conn.stateChan)
							if states != 1 {
								t.Errorf("published %d states, want exactly 1 (one transition per message)", states)
							}
						})
					}
				}
			}
		}
	}
}

func minimallyValidMessage(class, messageType uint8) messages.M3UA {
	key := uint16(class)<<8 | uint16(messageType)
	apc := func() *params.Param { return params.NewAffectedPointCodeWithMask(0, 1) }

	switch key {
	case uint16(messages.MsgClassTransfer)<<8 | uint16(messages.MsgTypePayloadData):
		return messages.NewData(nil, nil, params.NewProtocolData(1, 2, params.ServiceIndSCCP, 0, 0, 1, nil), nil)
	case uint16(messages.MsgClassSSNM)<<8 | uint16(messages.MsgTypeDestinationUnavailable):
		return messages.NewDestinationUnavailable(nil, nil, apc(), nil)
	case uint16(messages.MsgClassSSNM)<<8 | uint16(messages.MsgTypeDestinationAvailable):
		return messages.NewDestinationAvailable(nil, nil, apc(), nil)
	case uint16(messages.MsgClassSSNM)<<8 | uint16(messages.MsgTypeDestinationStateAudit):
		return messages.NewDestinationStateAudit(nil, nil, apc(), nil)
	case uint16(messages.MsgClassSSNM)<<8 | uint16(messages.MsgTypeSignallingCongestion):
		return messages.NewSignallingCongestion(nil, nil, apc(), nil, nil, nil)
	case uint16(messages.MsgClassSSNM)<<8 | uint16(messages.MsgTypeDestinationUserPartUnavailable):
		return messages.NewDestinationUserPartUnavailable(
			nil, nil, apc(), params.NewUserCause(params.SCCP, params.UnknownCause), nil,
		)
	case uint16(messages.MsgClassSSNM)<<8 | uint16(messages.MsgTypeDestinationRestricted):
		return messages.NewDestinationRestricted(nil, nil, apc(), nil)
	case uint16(messages.MsgClassManagement)<<8 | uint16(messages.MsgTypeError):
		return messages.NewError(params.NewErrorCode(params.ErrProtocolError), nil, nil, nil, nil)
	case uint16(messages.MsgClassManagement)<<8 | uint16(messages.MsgTypeNotify):
		return messages.NewNotify(params.NewStatus(params.AsStateActive), nil, nil, nil)
	default:
		return nil
	}
}

// RFC 4666 Section 4.3.4.2:
//
//	"The SGP MUST send an ASP Down Ack message in response to a received ASP
//	Down message from the ASP even if the ASP is already marked as ASP-DOWN at
//	the SGP."
//
// This is the same unconditional-Ack obligation as ASP Up. Withholding it
// leaves the peer retransmitting ASP Down until T(ack) expires, indefinitely.
func TestHandleAspDownAlwaysAcks(t *testing.T) {
	for _, st := range []State{StateASPDown, StateASPInactive, StateASPActive} {
		t.Run(st.String(), func(t *testing.T) {
			conn, sent := newTestConn(t, st, RoleSGP)

			if err := conn.handleAspDown(messages.NewAspDown(nil)); err != nil {
				t.Errorf("handleAspDown() error = %v, want nil (ASP Down Ack is a MUST)", err)
			}

			got := typeNames(*sent)
			if len(got) != 1 || got[0] != "ASP Down Ack" {
				t.Errorf("signals = %v, want [ASP Down Ack]", got)
			}
		})
	}
}

// RFC 4666 Section 3.5.5: "The receiver MUST respond with a BEAT Ack message."
// Section 4.3.4.6 adds "Upon receiving a Heartbeat message, the M3UA peer MUST
// respond with a Heartbeat Ack message", and closes with "Note: Heartbeat-
// related events are not shown in Figure 3 'ASP state transition diagram'".
// The obligation is therefore unconditional and outside the ASP state machine:
// withholding the Ack lets the peer's T(beat) expire and tears the association
// down over a message the RFC required us to answer.
func TestHeartbeatAckedInEveryState(t *testing.T) {
	for _, st := range []State{StateASPDown, StateASPInactive, StateASPActive} {
		t.Run(st.String(), func(t *testing.T) {
			conn, sent := newTestConn(t, st, RoleSGP)

			beat := messages.NewHeartbeat(params.NewHeartbeatData([]byte("beat-data")))
			unknown := params.NewParam(0xfffe, []byte{0x01, 0x02, 0x03, 0x04})
			beat.Others = []*params.Param{unknown}
			if err := conn.handleHeartbeat(beat); err != nil {
				t.Errorf("handleHeartbeat() error = %v, want nil (BEAT Ack is a MUST)", err)
			}

			if len(*sent) != 1 {
				t.Fatalf("signals = %v, want exactly one BEAT Ack", typeNames(*sent))
			}
			beatAck, ok := (*sent)[0].(*messages.HeartbeatAck)
			if !ok {
				t.Fatalf("sent %T, want *messages.HeartbeatAck", (*sent)[0])
			}
			if got := string(beatAck.HeartbeatData.HeartbeatData()); got != "beat-data" {
				t.Errorf("Heartbeat Data = %q, want %q", got, "beat-data")
			}
			if len(beatAck.Others) != 1 || beatAck.Others[0].Tag != unknown.Tag ||
				string(beatAck.Others[0].Data) != string(unknown.Data) {
				t.Errorf("other parameters = %v, want an unchanged copy of %v", beatAck.Others, unknown)
			}
		})
	}
}

// A BEAT Ack that correctly echoes our Heartbeat Data must be accepted whatever
// the ASP state, otherwise our own T(beat) expires and we tear down a healthy
// association. Only a genuinely mismatched echo is an error.
func TestHeartbeatAckAcceptedInEveryState(t *testing.T) {
	for _, st := range []State{StateASPDown, StateASPInactive, StateASPActive} {
		t.Run(st.String(), func(t *testing.T) {
			conn, _ := newTestConn(t, st, RoleASP)
			conn.setBeatData([]byte("beat-data"))

			ack := messages.NewHeartbeatAck(params.NewHeartbeatData([]byte("beat-data")))
			if err := conn.handleHeartbeatAck(ack); err != nil {
				t.Errorf("handleHeartbeatAck() error = %v, want nil for a matching echo", err)
			}
		})
	}
}

// RFC 4666 Section 4.3.4.6: "The Heartbeat message MAY optionally contain an
// opaque Heartbeat Data parameter". A peer may therefore legitimately omit it,
// and a BEAT Ack carrying no Heartbeat Data must be rejected as unexpected
// rather than dereferenced. Panicking here would let any peer crash the process.
func TestHeartbeatAckWithoutDataDoesNotPanic(t *testing.T) {
	for _, st := range []State{StateASPDown, StateASPInactive, StateASPActive} {
		t.Run(st.String(), func(t *testing.T) {
			conn, _ := newTestConn(t, st, RoleASP)
			conn.setBeatData([]byte("beat-data"))

			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panic on BEAT Ack without Heartbeat Data: %v", r)
				}
			}()

			if err := conn.handleHeartbeatAck(&messages.HeartbeatAck{}); err == nil {
				t.Error("handleHeartbeatAck() error = nil, want error for a missing echo")
			}
		})
	}
}

// RFC 4666 Section 3.3.1 lists Protocol Data as Mandatory in DATA, so only a
// broken or hostile peer omits it. handleData runs on its own goroutine and the
// library installs no recover(), so dereferencing the absent parameter would
// terminate the whole process and every association it serves. Section 3.8.1
// prescribes the correct answer instead: Error "Missing Parameter" (0x16).
func TestDataWithoutProtocolDataIsRejected(t *testing.T) {
	// DATA carrying a single parameter that is not Protocol Data, plus the
	// bare-header case. All are well-formed enough to parse.
	param := func(tag uint16, val []byte) []byte {
		plen := 4 + len(val)
		b := make([]byte, 4, plen)
		binary.BigEndian.PutUint16(b[0:2], tag)
		binary.BigEndian.PutUint16(b[2:4], uint16(plen))
		b = append(b, val...)
		for len(b)%4 != 0 {
			b = append(b, 0)
		}
		hdr := make([]byte, 8)
		hdr[0], hdr[1], hdr[2], hdr[3] = 1, 0, 1, 1 // version 1, TRANSFER / DATA
		binary.BigEndian.PutUint32(hdr[4:8], uint32(8+len(b)))
		return append(hdr, b...)
	}

	for _, tt := range []struct {
		name string
		raw  []byte
	}{
		{"bare header", []byte{1, 0, 1, 1, 0, 0, 0, 8}},
		{"NetworkAppearance only", param(0x0200, []byte{0, 0, 0, 0})},
		{"RoutingContext only", param(0x0006, []byte{0, 0, 0, 1})},
		{"CorrelationID only", param(0x0013, []byte{0, 0, 0, 7})},
	} {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := messages.Parse(tt.raw)
			if err != nil {
				t.Skipf("not parseable: %v", err)
			}
			data, ok := parsed.(*messages.Data)
			if !ok {
				t.Fatalf("parsed %T, want *messages.Data", parsed)
			}

			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("panic on DATA without Protocol Data: %v", r)
				}
			}()

			conn, _ := newTestConn(t, StateASPActive, RoleSGP)
			// A legal arrival stream, so the missing Protocol Data is what this
			// exercises: DATA on stream 0 is refused earlier, by Section
			// 1.4.7's rule 1, and would mask the guard under test.
			conn.recvStream.Store(1)
			conn.handleData(context.Background(), data)

			select {
			case err := <-conn.errChan:
				if !errors.Is(err, ErrMissingProtocolData) {
					t.Errorf("error = %v, want ErrMissingProtocolData", err)
				}
			default:
				t.Error("no error reported for DATA without Protocol Data")
			}
		})
	}
}

// The Missing Parameter error must reach the wire with the RFC 4666 Section
// 3.8.1 code 0x16, not be silently swallowed.
func TestMissingProtocolDataEmitsMissingParameterError(t *testing.T) {
	conn, sent := newTestConn(t, StateASPActive, RoleSGP)

	if e := conn.handleErrors(ErrMissingProtocolData); e != nil {
		t.Fatalf("handleErrors() error = %v, want nil", e)
	}

	codes := errorCodes(*sent)
	if len(codes) != 1 || codes[0] != params.ErrMissingParameter {
		t.Errorf("error codes = %v, want [%d] (Missing Parameter)", codes, params.ErrMissingParameter)
	}
}

// newProdConn pins beatAckChan and established to the exact capacities Dial
// and Accept use (1 and 1), so the liveness tests below exercise production
// send/receive semantics on those channels. stateChan and errChan deliberately
// DIVERGE from production (buffered here, unbuffered there), while dataChan is
// fixed at 8 rather than Config.DataQueueSize, so tests can count publishes
// without running a monitor() reader — do not rely on this fixture to catch a
// blocking sendState/sendErr or production queue capacity.
func newProdConn(t *testing.T, state State, role Role) *Association {
	t.Helper()

	conn, _ := newTestConn(t, state, role)
	conn.beatAckChan = make(chan struct{}, 1)
	conn.setBeatData([]byte("outstanding"))
	return conn
}

// A BEAT Ack must never wedge the dispatcher, even when the capacity-1 token
// buffer is already full and nothing is waiting for it — heartbeat() exited on
// ctx cancellation, or BEAT is disabled. Dropping the surplus token is
// harmless, because T(beat) is the real timer. Blocking there would wedge the
// dispatch goroutine for that message and leak it for the lifetime of the
// association.
//
// Two Acks are dispatched back to back, each answering its own BEAT (the second
// re-arms beatData, as heartbeat() does before every round) so both reach
// notifyBeatAck: the first fills the buffer, so the second proves the send has
// no blocking path. Re-arming is what makes this a token-buffer test rather
// than a replay test — a replayed Ack is rejected before notifyBeatAck and
// would never exercise the full channel.
func TestBeatAckNeverWedgesDispatcher(t *testing.T) {
	for _, st := range []State{StateASPDown, StateASPInactive, StateASPActive} {
		t.Run(st.String(), func(t *testing.T) {
			conn := newProdConn(t, st, RoleSGP)
			ack := messages.NewHeartbeatAck(params.NewHeartbeatData(conn.currentBeatData()))

			done := make(chan struct{})
			go func() {
				conn.handleSignals(context.Background(), ack)
				conn.setBeatData([]byte("outstanding")) // next BEAT goes out
				conn.handleSignals(context.Background(), ack)
				close(done)
			}()

			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatalf("handleSignals wedged on a full beatAckChan with no reader (state %v)", st)
			}

			if len(conn.stateChan) != 2 {
				t.Errorf("published %d states, want exactly 2 (one transition per message)", len(conn.stateChan))
			}
		})
	}
}

// The converse of the wedge test: a valid Ack whose round trip completes
// BEFORE heartbeat() parks on its select must not be lost. beatAckChan has
// capacity 1 precisely so the token survives until the reader arrives; on an
// unbuffered channel this ordering silently discarded the Ack, heartbeat()
// sat out the full T(beat), and a healthy association was torn down with
// ErrHeartbeatExpired.
func TestBeatAckBeforeReaderParksIsNotLost(t *testing.T) {
	conn := newProdConn(t, StateASPActive, RoleSGP)
	ack := messages.NewHeartbeatAck(params.NewHeartbeatData(conn.currentBeatData()))

	// Ack arrives first; nobody is reading yet.
	conn.handleSignals(context.Background(), ack)

	// heartbeat() parks afterwards and must still find the token.
	select {
	case <-conn.beatAckChan:
	case <-time.After(2 * time.Second):
		t.Fatal("BEAT Ack token was dropped: heartbeat() would expire T(beat) on a healthy link")
	}
}

// The token must still reach a waiting heartbeat() — making the send
// non-blocking must not break liveness detection on a healthy link.
func TestBeatAckReachesWaitingHeartbeat(t *testing.T) {
	conn := newProdConn(t, StateASPActive, RoleSGP)
	ack := messages.NewHeartbeatAck(params.NewHeartbeatData(conn.currentBeatData()))

	got := make(chan struct{})
	go func() {
		<-conn.beatAckChan // stand in for heartbeat() waiting for its Ack
		close(got)
	}()

	// Give the reader time to park on the channel before the Ack arrives.
	time.Sleep(50 * time.Millisecond)
	conn.handleSignals(context.Background(), ack)

	select {
	case <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("BEAT Ack token never reached the waiting heartbeat goroutine")
	}
}

// The operator-critical recovery sequence, exercised without a socket so it
// also runs where SCTP is unavailable (the socket-backed TestDuplicateAspUpIsAcked
// skips itself on such platforms). An SGP carrying traffic receives a duplicate ASP Up,
// answers per RFC 4666 Section 4.3.4.1 and drops to ASP-INACTIVE; the ASP then
// completes ASPTM and traffic resumes. If this breaks, a live link stops
// carrying traffic and never recovers.
func TestSGPRecoversAfterDuplicateAspUp(t *testing.T) {
	conn, sent := newTestConn(t, StateASPActive, RoleSGP)

	// Apply whatever the dispatch publishes, as monitor() would.
	apply := func() State {
		select {
		case st := <-conn.stateChan:
			conn.muState.Lock()
			conn.state = st
			conn.muState.Unlock()
			return st
		default:
			t.Fatal("no state published")
			return StateASPDown
		}
	}
	drainErr := func() {
		select {
		case err := <-conn.errChan:
			if e := conn.handleErrors(err); e != nil {
				t.Fatalf("handleErrors() error = %v", e)
			}
		default:
		}
	}

	// Step 1: the duplicate ASP Up that caused the production incident.
	conn.handleSignals(context.Background(), messages.NewAspUp(conn.cfg.ASPIdentifier, nil))
	drainErr()
	if got := apply(); got != StateASPInactive {
		t.Fatalf("state after ASP Up = %v, want %v", got, StateASPInactive)
	}
	if got := typeNames(*sent); len(got) != 2 || got[0] != "ASP Up Ack" || got[1] != "Error" {
		t.Fatalf("signals = %v, want [ASP Up Ack Error]", got)
	}

	// Step 2: a conformant ASP follows with ASP Active.
	conn.handleSignals(context.Background(), messages.NewAspActive(
		conn.cfg.TrafficModeType, conn.cfg.RoutingContexts, nil))
	drainErr()
	if got := apply(); got != StateASPActive {
		t.Fatalf("state after ASP Active = %v, want %v (traffic must resume)", got, StateASPActive)
	}
	if got := typeNames(*sent); len(got) != 3 || got[2] != "ASP Active Ack" {
		t.Errorf("signals = %v, want [... ASP Active Ack]", got)
	}
}

// monitor() dispatches every inbound message on a bare goroutine and the
// library installs no recover(), so any panic in a handler kills the whole
// process and every other association it serves. RFC 4666 marks most
// parameters optional, so a peer may legitimately send a bare 8-octet header
// for any ASPSM/ASPTM type: none of them may panic in any state or role.
func TestBareHeaderMessagesNeverPanic(t *testing.T) {
	for _, class := range []uint8{3, 4} { // ASPSM, ASPTM
		for _, typ := range []uint8{1, 2, 3, 4, 5, 6} {
			for _, st := range []State{StateASPDown, StateASPInactive, StateASPActive} {
				for _, role := range []Role{RoleSGP, RoleASP} {
					msg, err := messages.Parse([]byte{1, 0, class, typ, 0, 0, 0, 8})
					if err != nil {
						continue // not a defined message type
					}

					name := fmt.Sprintf("class%d/type%d/%v/role=%s", class, typ, st, role)
					t.Run(name, func(t *testing.T) {
						defer func() {
							if r := recover(); r != nil {
								t.Fatalf("panic on a bare %s: %v", msg.MessageTypeName(), r)
							}
						}()

						conn, _ := newTestConn(t, st, role)
						conn.setBeatData([]byte("outstanding"))
						conn.handleSignals(context.Background(), msg)
					})
				}
			}
		}
	}
}

// An unsolicited BEAT Ack must not be accepted as proof of liveness when we
// have no outstanding Heartbeat Data to match it against, otherwise a peer can
// forge liveness tokens and defeat T(beat) detection of a dead link.
func TestHeartbeatAckRejectedWhenNoBeatOutstanding(t *testing.T) {
	conn, _ := newTestConn(t, StateASPActive, RoleASP)
	conn.setBeatData(nil) // no BEAT sent yet

	ack := messages.NewHeartbeatAck(params.NewHeartbeatData([]byte{}))
	if err := conn.handleHeartbeatAck(ack); err == nil {
		t.Error("handleHeartbeatAck() error = nil, want error when no BEAT is outstanding")
	}
}

// A mismatched echo must still be rejected, whatever the state.
func TestHeartbeatAckRejectsMismatchedEcho(t *testing.T) {
	conn, _ := newTestConn(t, StateASPActive, RoleASP)
	conn.setBeatData([]byte("beat-data"))

	ack := messages.NewHeartbeatAck(params.NewHeartbeatData([]byte("different")))
	if err := conn.handleHeartbeatAck(ack); err == nil {
		t.Error("handleHeartbeatAck() error = nil, want error for a mismatched echo")
	}
}

// An Ack answers exactly one BEAT. Accepting it must retire the outstanding
// Heartbeat Data, otherwise a peer that captured a single valid Ack could
// replay that same frame to satisfy every subsequent T(beat): the link could be
// dead in both directions — the peer's M3UA stack gone, or an off-path attacker
// or middlebox echoing recorded bytes — and heartbeat() would never expire.
func TestHeartbeatAckIsNotReplayable(t *testing.T) {
	conn, _ := newTestConn(t, StateASPActive, RoleASP)
	conn.setBeatData([]byte("round-one-data"))

	ack := messages.NewHeartbeatAck(params.NewHeartbeatData([]byte("round-one-data")))
	if err := conn.handleHeartbeatAck(ack); err != nil {
		t.Fatalf("handleHeartbeatAck() error = %v, want nil for the genuine Ack", err)
	}

	if got := conn.currentBeatData(); len(got) != 0 {
		t.Errorf("beatData = %q after a completed round trip, want it retired", got)
	}

	// The captured frame, replayed once the BEAT it answered is long gone.
	if err := conn.handleHeartbeatAck(ack); err == nil {
		t.Error("handleHeartbeatAck() error = nil for a replayed Ack, want it rejected: T(beat) can no longer detect a dead link")
	}
}

// Retiring the echo must happen only once the Ack is proven genuine. A forged
// Ack carrying the wrong data must not clear the data the real Ack still needs
// to match against, or an attacker could break liveness for a healthy link by
// injecting garbage — turning a validation check into a denial of service.
func TestBogusHeartbeatAckDoesNotRetireOutstandingBeat(t *testing.T) {
	conn, _ := newTestConn(t, StateASPActive, RoleASP)
	conn.setBeatData([]byte("round-one-data"))

	bogus := messages.NewHeartbeatAck(params.NewHeartbeatData([]byte("forged-echo!!!")))
	if err := conn.handleHeartbeatAck(bogus); err == nil {
		t.Fatal("handleHeartbeatAck() error = nil for a forged echo, want it rejected")
	}

	genuine := messages.NewHeartbeatAck(params.NewHeartbeatData([]byte("round-one-data")))
	if err := conn.handleHeartbeatAck(genuine); err != nil {
		t.Errorf("handleHeartbeatAck() error = %v for the genuine Ack after a forged one, want nil", err)
	}
}

// Two Acks racing in on separate goroutines must not both claim one BEAT: the
// compare-and-clear is atomic, so exactly one wins. Run under -race this also
// covers concurrent access to beatData.
func TestConcurrentHeartbeatAcksClaimBeatOnce(t *testing.T) {
	conn, _ := newTestConn(t, StateASPActive, RoleASP)
	conn.setBeatData([]byte("round-one-data"))

	ack := messages.NewHeartbeatAck(params.NewHeartbeatData([]byte("round-one-data")))

	var (
		wg       sync.WaitGroup
		accepted atomic.Int32
	)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := conn.handleHeartbeatAck(ack); err == nil {
				accepted.Add(1)
			}
		}()
	}
	wg.Wait()

	if got := accepted.Load(); got != 1 {
		t.Errorf("%d of 2 concurrent Acks accepted, want exactly 1", got)
	}
}

// Only an UnexpectedMessageError advances the state to ASP-INACTIVE. A message
// the node could not act on at all — here a failed ASP Up Ack write — must hold
// the current state, so a transient write failure cannot silently take an
// active association out of service.
func TestHandleSignalsHoldsStateOnNonUnexpectedError(t *testing.T) {
	conn, _ := newTestConn(t, StateASPActive, RoleSGP)
	conn.signalWriter = func(m3 messages.M3UA) (int, error) {
		return 0, errors.New("sctp write failed")
	}

	conn.handleSignals(context.Background(), messages.NewAspUp(conn.cfg.ASPIdentifier, nil))

	select {
	case got := <-conn.stateChan:
		if got != StateASPActive && got != stateUnchanged {
			t.Errorf("state = %v, want %v held on a write failure", got, StateASPActive)
		}
	default:
		t.Fatal("no state published")
	}
}

// RFC 4666 Section 4.3.4.1 scopes the unexpected-ASP-Up-Ack rule to the ASP:
// "If the ASP receives an unexpected ASP Up Ack message, the ASP should
// consider itself in the ASP-INACTIVE state." An SGP never sends ASP Up ("The
// ASP is always the initiator of the ASP Up message"), so it can never
// legitimately receive an ASP Up Ack in ANY state and no clause authorises it
// to change state on one. It reports the Error and holds its state, rather
// than letting a stray message take an active association's data path down or
// silently walk an idle one to ASP-INACTIVE.
func TestSGPHoldsStateOnUnsolicitedAspUpAck(t *testing.T) {
	for _, st := range []State{StateASPDown, StateASPInactive, StateASPActive} {
		t.Run(st.String(), func(t *testing.T) {
			conn, _ := newTestConn(t, st, RoleSGP)

			conn.handleSignals(context.Background(), messages.NewAspUpAck(conn.cfg.ASPIdentifier, nil))

			select {
			case got := <-conn.stateChan:
				if got != st && got != stateUnchanged {
					t.Errorf("SGP state = %v, want %v held (RFC clause is ASP-scoped)", got, st)
				}
			default:
				t.Fatal("no state published")
			}

			select {
			case err := <-conn.errChan:
				var unexpected *UnexpectedMessageError
				if !errors.As(err, &unexpected) {
					t.Errorf("error = %T, want *UnexpectedMessageError", err)
				}
			default:
				t.Error("no Error reported for an unsolicited ASP Up Ack")
			}
		})
	}
}

// The mirror image of the SGP rules: ASP Up and ASP Down travel only from ASP
// to SGP, and their Acks are messages only an SGP may send. An ASP
// that receives either must not answer with the SGP-only Ack and must not move
// off its current state — otherwise a single stray or forged ASP Up knocks an
// ACTIVE association's data path down to ASP-INACTIVE. It reports the Error
// ("Unexpected Message") and holds, exactly as the SGP does for stray Acks.
func TestASPHoldsStateOnAspUpAndAspDown(t *testing.T) {
	for _, tt := range []struct {
		name string
		msg  messages.M3UA
	}{
		{"ASP Up", messages.NewAspUp(nil, nil)},
		{"ASP Down", messages.NewAspDown(nil)},
	} {
		for _, st := range []State{StateASPDown, StateASPInactive, StateASPActive} {
			t.Run(tt.name+"/"+st.String(), func(t *testing.T) {
				conn, sent := newTestConn(t, st, RoleASP)

				conn.handleSignals(context.Background(), tt.msg)

				if got := typeNames(*sent); len(got) != 0 {
					t.Errorf("ASP answered %s with %v, want no signal (Acks are SGP-only)", tt.name, got)
				}

				select {
				case got := <-conn.stateChan:
					if got != st && got != stateUnchanged {
						t.Errorf("ASP state = %v, want %v held", got, st)
					}
				default:
					t.Fatal("no state published")
				}

				select {
				case err := <-conn.errChan:
					var unexpected *UnexpectedMessageError
					if !errors.As(err, &unexpected) {
						t.Errorf("error = %T, want *UnexpectedMessageError", err)
					}
				default:
					t.Errorf("no Error reported for a %s received by an ASP", tt.name)
				}
			})
		}
	}
}

// established is a one-shot "connection is up" signal that Dial/Accept read
// exactly once. Re-activation after an RFC 4666 Section 4.3.4.1 drop to
// ASP-INACTIVE must not block on it, or handleStateUpdate wedges while holding
// muState and every State() caller blocks with SCTP still up.
func TestReactivationDoesNotBlockOnEstablished(t *testing.T) {
	for _, test := range []struct {
		name string
		role Role
	}{{"SGP", RoleSGP}, {"ASP", RoleASP}} {
		t.Run(test.name, func(t *testing.T) {
			conn, _ := newTestConn(t, StateASPActive, test.role)

			// Accept()/Dial() consumed the first signal already.
			for cycle := 1; cycle <= 4; cycle++ {
				done := make(chan struct{})
				go func() {
					_ = conn.handleStateUpdate(StateASPInactive)
					_ = conn.handleStateUpdate(StateASPActive)
					close(done)
				}()

				select {
				case <-done:
				case <-time.After(2 * time.Second):
					t.Fatalf("handleStateUpdate blocked on cycle %d (established full)", cycle)
				}
			}
		})
	}
}

// An ASP re-drives itself out of ASP-INACTIVE by initiating the ASPTM
// procedure, which is how an association returns to ASP-ACTIVE after RFC 4666
// Section 4.3.4.1 drops it there. (An SGP deliberately does not: per the RFC
// the ASP initiates ASPTM, and the SGP recovers when the peer's ASP Active
// arrives and handleAspActive acknowledges it.)
// An ASP demoted out of ASP-ACTIVE must NOT re-activate itself.
//
// It used to, and that is wrong in every way the demotion can happen: an ASP
// Inactive from the peer, an ASP Inactive Ack answering our own request, or an
// "Alternate ASP Active" Notify. The last is the damaging one — at an
// Override-mode AS, re-activating takes the traffic straight back off the ASP
// that just overrode us, which overrides it in turn, indefinitely. RFC 4666
// Section 4.3.4.5: a Notify "does not explicitly compel the ASP(s) receiving the
// message to become active. The ASPs remain in control of what (and when)
// traffic action is taken."
func TestASPDoesNotReactivateAfterBeingMadeInactive(t *testing.T) {
	conn, sent := newTestConn(t, StateASPActive, RoleASP)

	if err := conn.handleStateUpdate(StateASPInactive); err != nil {
		t.Fatalf("handleStateUpdate() error = %v, want nil", err)
	}

	if got := typeNames(*sent); len(got) != 0 {
		t.Errorf("signals = %v, want none: coming back from ASP-INACTIVE is the owner's call", got)
	}
}

// Climbing out of ASP-DOWN is the case that does owe an ASP Active: RFC 4666
// Section 4.3.4.3 has the ASP ask for traffic "Anytime after the ASP has
// received an ASP Up Ack message".
func TestASPAsksForTrafficAfterAspUpAck(t *testing.T) {
	conn, sent := newTestConn(t, StateASPDown, RoleASP)

	if err := conn.handleStateUpdate(StateASPInactive); err != nil {
		t.Fatalf("handleStateUpdate() error = %v, want nil", err)
	}

	got := typeNames(*sent)
	if len(got) != 1 || got[0] != "ASP Active" {
		t.Errorf("signals = %v, want [ASP Active] once the ASP is up", got)
	}
}

// RFC 4666 Section 4.3.4.1:
//
//	"If the ASP receives an unexpected ASP Up Ack message, the ASP should
//	consider itself in the ASP-INACTIVE state."
func TestHandleAspUpAckFromActiveTransitionsToInactive(t *testing.T) {
	conn, _ := newTestConn(t, StateASPActive, RoleASP)

	conn.handleSignals(context.Background(), messages.NewAspUpAck(conn.cfg.ASPIdentifier, nil))

	select {
	case got := <-conn.stateChan:
		if got != StateASPInactive && got != stateUnchanged {
			t.Errorf("state = %v, want %v", got, StateASPInactive)
		}
	default:
		t.Fatal("no state update published; want AspInactive")
	}
}

// RFC 4666 Section 4.3.4.3:
//
//	"Independently of the RC, the SGP MUST send an ASP Active Ack message in
//	response to a received ASP Active message from the ASP, if the ASP is
//	already marked in the ASP-ACTIVE state."
//
// Withholding the Ack from an already-active ASP leaves the peer retransmitting
// on T(ack) indefinitely — the ASP Up deadlock, one message class over.
//
// ASP-DOWN is deliberately not in this list. The quote above is conditional on
// the ASP already being ASP-ACTIVE, Figure 3 defines no ASPTM transition out of
// ASP-DOWN, and Section 4.3.1 allows such a peer only "Heartbeat, ASP Down Ack,
// and Error messages" — so the Ack is forbidden there rather than owed. See
// TestASPTMFromAspDownIsRefused.
func TestHandleAspActiveAlwaysAcks(t *testing.T) {
	for _, st := range []State{StateASPInactive, StateASPActive} {
		t.Run(st.String(), func(t *testing.T) {
			conn, sent := newTestConn(t, st, RoleSGP)

			_ = conn.handleAspActive(messages.NewAspActive(
				conn.cfg.TrafficModeType, conn.cfg.RoutingContexts, nil))

			if len(*sent) == 0 {
				t.Fatalf("no signal sent from %v; want an ASP Active Ack", st)
			}
			if got := (*sent)[0].MessageTypeName(); got != "ASP Active Ack" {
				t.Errorf("first signal = %q, want %q (sent: %v)", got, "ASP Active Ack", typeNames(*sent))
			}
		})
	}
}

// RFC 4666 Section 4.3.4.4:
//
//	"The SGP MUST send an ASP Inactive Ack message in response to a received
//	ASP Inactive message from the ASP; the ASP is already marked as
//	ASP-INACTIVE at the SGP."
//
// As with ASP Active, ASP-DOWN is excluded: the sentence is about an ASP the
// SGP already holds as ASP-INACTIVE, and Section 4.3.1 forbids sending the Ack
// to an ASP-DOWN peer at all.
func TestHandleAspInactiveAlwaysAcks(t *testing.T) {
	for _, st := range []State{StateASPInactive, StateASPActive} {
		t.Run(st.String(), func(t *testing.T) {
			conn, sent := newTestConn(t, st, RoleSGP)

			_ = conn.handleAspInactive(messages.NewAspInactive(conn.cfg.RoutingContexts, nil))

			if len(*sent) == 0 {
				t.Fatalf("no signal sent from %v; want an ASP Inactive Ack", st)
			}
			if got := (*sent)[0].MessageTypeName(); got != "ASP Inactive Ack" {
				t.Errorf("first signal = %q, want %q (sent: %v)", got, "ASP Inactive Ack", typeNames(*sent))
			}
		})
	}
}

// An already-active ASP that re-sends ASP Active must be acked AND left in
// ASP-ACTIVE: the Ack is what converges the peer, so the dispatcher must not
// let the accompanying "Unexpected Message" Error suppress the transition.
func TestDuplicateAspActiveAcksAndHoldsActive(t *testing.T) {
	conn, sent := newTestConn(t, StateASPActive, RoleSGP)

	conn.handleSignals(context.Background(), messages.NewAspActive(
		conn.cfg.TrafficModeType, conn.cfg.RoutingContexts, nil))

	if got := typeNames(*sent); len(got) == 0 || got[0] != "ASP Active Ack" {
		t.Errorf("sent = %v, want an ASP Active Ack first", got)
	}
	select {
	case got := <-conn.stateChan:
		if got != StateASPActive && got != stateUnchanged {
			t.Errorf("state = %v, want %v after a duplicate ASP Active", got, StateASPActive)
		}
	default:
		t.Fatal("no state update published; want AspActive")
	}
}

// The same for ASP Inactive: acked, and the SGP still moves the ASP to
// ASP-INACTIVE rather than holding the old state behind the Error.
func TestDuplicateAspInactiveAcksAndHoldsInactive(t *testing.T) {
	conn, sent := newTestConn(t, StateASPInactive, RoleSGP)

	conn.handleSignals(context.Background(), messages.NewAspInactive(conn.cfg.RoutingContexts, nil))

	if got := typeNames(*sent); len(got) == 0 || got[0] != "ASP Inactive Ack" {
		t.Errorf("sent = %v, want an ASP Inactive Ack first", got)
	}
	select {
	case got := <-conn.stateChan:
		if got != StateASPInactive && got != stateUnchanged {
			t.Errorf("state = %v, want %v after a duplicate ASP Inactive", got, StateASPInactive)
		}
	default:
		t.Fatal("no state update published; want AspInactive")
	}
}

// ASP Active and ASP Inactive are SGP procedures ("from the ASP"). An ASP
// that receives one must report the Error and hold its state: a stray or forged
// ASP Active must never move an ASP's data path.
func TestASPHoldsStateOnAspActiveAndAspInactive(t *testing.T) {
	for _, st := range []State{StateASPDown, StateASPInactive, StateASPActive} {
		for _, tt := range []struct {
			name string
			msg  func(c *Association) messages.M3UA
		}{
			{"AspActive", func(c *Association) messages.M3UA {
				return messages.NewAspActive(c.cfg.TrafficModeType, c.cfg.RoutingContexts, nil)
			}},
			{"AspInactive", func(c *Association) messages.M3UA {
				return messages.NewAspInactive(c.cfg.RoutingContexts, nil)
			}},
		} {
			t.Run(tt.name+"/"+st.String(), func(t *testing.T) {
				conn, sent := newTestConn(t, st, RoleASP)

				conn.handleSignals(context.Background(), tt.msg(conn))

				if len(*sent) != 0 {
					t.Errorf("ASP sent %v on a peer %s; want nothing (SGP-only procedure)",
						typeNames(*sent), tt.name)
				}
				select {
				case got := <-conn.stateChan:
					if got != st && got != stateUnchanged {
						t.Errorf("ASP state = %v, want it held at %v", got, st)
					}
				default:
					t.Fatal("no state update published")
				}
			})
		}
	}
}

// RFC 4666 Section 4.3.4.3:
//
//	"If the SGP determines that the mode indicated in an ASP Active message is
//	unsupported or incompatible with the mode currently configured for the AS,
//	the SGP responds with an Error message ('Unsupported / Invalid Traffic
//	Handling Mode')."
//
// The Ack must be withheld here: acking would tell the peer its incompatible
// mode was accepted.
func TestAspActiveWithIncompatibleTrafficModeIsRefused(t *testing.T) {
	conn, sent := newTestConn(t, StateASPInactive, RoleSGP)
	// Configured mode is Loadshare; the peer demands Broadcast.
	err := conn.handleAspActive(messages.NewAspActive(
		params.NewTrafficModeType(params.TrafficModeBroadcast), conn.cfg.RoutingContexts, nil))

	if !errors.Is(err, ErrUnsupportedTrafficMode) {
		t.Fatalf("handleAspActive() error = %v, want ErrUnsupportedTrafficMode", err)
	}
	for _, m := range typeNames(*sent) {
		if m == "ASP Active Ack" {
			t.Error("sent an ASP Active Ack for an incompatible traffic mode; want it refused")
		}
	}

	if e := conn.handleErrors(err); e != nil {
		t.Fatalf("handleErrors() error = %v, want nil", e)
	}
	codes := errorCodes(*sent)
	if len(codes) != 1 || codes[0] != params.ErrUnsupportedTrafficModeType {
		t.Errorf("error codes = %v, want [%d] (Unsupported Traffic Handling Mode)",
			codes, params.ErrUnsupportedTrafficModeType)
	}
}

// The Traffic Mode Type parameter is optional (RFC 4666 Section 3.7.1): a peer
// that omits it accepts the configured mode and must be acked normally.
func TestAspActiveWithoutTrafficModeIsAccepted(t *testing.T) {
	conn, sent := newTestConn(t, StateASPInactive, RoleSGP)

	if err := conn.handleAspActive(messages.NewAspActive(nil, conn.cfg.RoutingContexts, nil)); err != nil {
		t.Fatalf("handleAspActive() error = %v, want nil when the peer omits the mode", err)
	}
	if got := typeNames(*sent); len(got) == 0 || got[0] != "ASP Active Ack" {
		t.Errorf("sent = %v, want an ASP Active Ack", got)
	}
}

// RFC 4666 Section 3.8.1: "Error messages MUST NOT be generated in response to
// other Error messages." A peer ERR must be absorbed — never answered, and
// never allowed to tear the association down.
func TestPeerErrorIsNotAnsweredWithError(t *testing.T) {
	conn, sent := newTestConn(t, StateASPActive, RoleSGP)

	peerErr := messages.NewError(
		params.NewErrorCode(params.UnexpectedMessageError), nil, nil, nil, nil)
	conn.handleSignals(context.Background(), peerErr)

	if len(*sent) != 0 {
		t.Errorf("answered a peer ERR with %v; RFC 4666 Section 3.8.1 forbids it", typeNames(*sent))
	}
	select {
	case got := <-conn.stateChan:
		if got != StateASPActive && got != stateUnchanged {
			t.Errorf("state = %v, want it held at %v on a peer ERR", got, StateASPActive)
		}
	default:
		t.Fatal("no state update published; monitor() would stop reading")
	}
}

// RFC 4666 Section 4.3.4.3:
//
//	"The ASP receiving this Notify MUST consider itself now in the
//	ASP-INACTIVE state, if it is not already aware of this via inter-ASP
//	communication with the Overriding ASP."
func TestNotifyAlternateAspActiveMovesASPToInactive(t *testing.T) {
	conn, sent := newTestConn(t, StateASPActive, RoleASP)

	conn.handleSignals(context.Background(), messages.NewNotify(
		params.NewStatus(params.AlternateAspActive), nil, nil, nil))

	if len(*sent) != 0 {
		t.Errorf("answered a NTFY with %v; a Notify is not acknowledged", typeNames(*sent))
	}
	if err := firstErr(conn); err != nil {
		t.Fatalf("accepted NTFY reported error: %v", err)
	}
	select {
	case got := <-conn.stateChan:
		if got != StateASPInactive {
			t.Errorf("state = %v, want %v after an Alternate ASP Active NTFY", got, StateASPInactive)
		}
	default:
		t.Fatal("no state update published")
	}
}

// An SGP is the sender of an Override notification, never its subject, so a
// NTFY arriving at an SGP must not move its association state.
func TestNotifyAlternateAspActiveDoesNotMoveSGP(t *testing.T) {
	conn, _ := newTestConn(t, StateASPActive, RoleSGP)

	conn.handleSignals(context.Background(), messages.NewNotify(
		params.NewStatus(params.AlternateAspActive), nil, nil, nil))

	select {
	case got := <-conn.stateChan:
		if got != StateASPActive && got != stateUnchanged {
			t.Errorf("state = %v, want it held at %v", got, StateASPActive)
		}
	default:
		t.Fatal("no state update published")
	}
}

// RFC 4666 Section 4.3.4.5: an AS-state Notify "does not explicitly compel the
// ASP(s) receiving the message to become active. The ASPs remain in control of
// what (and when) traffic action is taken." These are advisory and must hold
// state, or an SGP could drive an ASP's data path from a bare notification.
func TestAdvisoryNotifyHoldsState(t *testing.T) {
	for _, status := range []uint32{
		params.AsStateInactive,
		params.AsStateActive,
		params.AsStatePending,
		params.InsufficientAspResources,
	} {
		t.Run(notifyStatusName(status), func(t *testing.T) {
			conn, sent := newTestConn(t, StateASPActive, RoleASP)

			conn.handleSignals(context.Background(), messages.NewNotify(
				params.NewStatus(status), nil, nil, nil))

			if len(*sent) != 0 {
				t.Errorf("answered an advisory NTFY with %v; want nothing", typeNames(*sent))
			}
			if err := firstErr(conn); err != nil {
				t.Fatalf("accepted advisory NTFY reported error: %v", err)
			}
			select {
			case got := <-conn.stateChan:
				if got != StateASPActive && got != stateUnchanged {
					t.Errorf("state = %v, want it held at %v for an advisory NTFY", got, StateASPActive)
				}
			default:
				t.Fatal("no state update published")
			}
		})
	}
}

// The Status parameter is Mandatory in NTFY (RFC 4666 Section 3.8.2). Section
// 3.8.1 assigns a missing mandatory parameter its own Error code; treating the
// whole message as merely unexpected hides the actual interoperability fault.
func TestNotifyWithoutStatusIsRejected(t *testing.T) {
	conn, _ := newTestConn(t, StateASPActive, RoleASP)

	if err := conn.handleNotify(messages.NewNotify(nil, nil, nil, nil)); !errors.Is(err, ErrMissingStatus) {
		t.Errorf("handleNotify() error = %v for a NTFY without Status, want ErrMissingStatus", err)
	}
}

func TestNotifyIsRejectedAtAnSGP(t *testing.T) {
	conn, _ := newTestConn(t, StateASPActive, RoleSGP)
	notify := messages.NewNotify(params.NewStatus(params.AsStateActive), nil, nil, nil)

	err := conn.handleNotify(notify)
	var unexpected *UnexpectedMessageError
	if !errors.As(err, &unexpected) {
		t.Fatalf("handleNotify() error = %v, want UnexpectedMessageError", err)
	}
	if len(conn.mgmtChan) != 0 {
		t.Error("an ASP-originated NTFY reached Layer Management at the SGP")
	}
}

func TestNotifyRejectsReservedStatusValues(t *testing.T) {
	for _, status := range []uint32{
		0,
		uint32(params.AsStateChange)<<16 | 1,
		uint32(params.AsStateChange)<<16 | 5,
		uint32(params.Other) << 16,
		uint32(params.Other)<<16 | 4,
		3<<16 | 1,
		0xffff0001,
	} {
		t.Run(fmt.Sprintf("%08x", status), func(t *testing.T) {
			conn, _ := newTestConn(t, StateASPActive, RoleASP)
			err := conn.handleNotify(messages.NewNotify(params.NewStatus(status), nil, nil, nil))
			if !errors.Is(err, ErrInvalidParameterValue) {
				t.Fatalf("handleNotify() error = %v, want ErrInvalidParameterValue", err)
			}
			if len(conn.mgmtChan) != 0 {
				t.Error("a reserved NTFY Status reached Layer Management")
			}
		})
	}
}

// The New*() message constructors call SetLength on every Param handed to them,
// which writes to the caller's Param. An Association's configuration Params are shared
// by every message it sends, so two goroutines sending concurrently used to
// write to the same Param — a data race between, for example, a DATA write and
// an ERR built by the monitor goroutine. Copying at the send sites is what makes
// concurrent sends safe; run under -race this fails if a copy is dropped.
func TestConcurrentSendsDoNotShareConfigParams(t *testing.T) {
	conn, _ := newTestConn(t, StateASPActive, RoleSGP)

	// newTestConn's capture closure appends to a slice and is single-threaded by
	// design; this is the one test that writes from several goroutines, so it
	// marshals each message (the step that touches the Params) behind a mutex of
	// its own. A race here is then a race in the production Params, not in the
	// fixture's bookkeeping.
	var muSent sync.Mutex
	conn.signalWriter = func(m3 messages.M3UA) (int, error) {
		b, err := m3.MarshalBinary()
		if err != nil {
			return 0, err
		}
		muSent.Lock()
		defer muSent.Unlock()
		return len(b), nil
	}

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// The two paths that raced in practice: an ERR from the monitor
			// goroutine and a signal carrying the same config Params.
			_ = conn.handleErrors(NewUnexpectedMessageError(
				messages.NewAspUp(nil, nil)))
			_, _ = conn.WriteSignal(messages.NewAspActiveAck(
				conn.cfg.TrafficModeType.Copy(), conn.cfg.RoutingContexts.Copy(), nil))
		}()
	}
	wg.Wait()
}

// RFC 4666 Section 4.3.4.2:
//
//	"If the ASP receives an ASP Down Ack without having sent an ASP Down
//	message, the ASP should now consider itself to be in the ASP-DOWN state.
//
//	If the ASP was previously in the ASP-ACTIVE or ASP-INACTIVE state, the ASP
//	should then initiate procedures to return itself to its previous state."
//
// The ASP reported "Unexpected Message" from exactly those two states and held,
// so it went on believing it was carrying traffic while the SGP had already put
// it down — the two ends silently disagreeing about whether a link is live,
// which is the worst state an SS7 link can be in. The Error is still owed; what
// was missing is the state change and the climb back.
func TestUnsolicitedAspDownAckReturnsTheASPToItsPreviousState(t *testing.T) {
	for _, tt := range []struct {
		name     string
		from     State
		wantBack State
	}{
		{"from ASP-ACTIVE", StateASPActive, StateASPActive},
		{"from ASP-INACTIVE", StateASPInactive, StateASPInactive},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conn, sent := newTestConn(t, tt.from, RoleASP)

			conn.handleSignals(context.Background(), messages.NewAspDownAck(nil))

			// The Error is still owed.
			select {
			case err := <-conn.errChan:
				var unexpected *UnexpectedMessageError
				if !errors.As(err, &unexpected) {
					t.Errorf("reported %v, want *UnexpectedMessageError", err)
				}
			default:
				t.Error("an unsolicited ASP Down Ack was not reported at all")
			}

			// And the ASP must now consider itself ASP-DOWN.
			select {
			case got := <-conn.stateChan:
				if got != StateASPDown {
					t.Fatalf("published %v, want %v after an unsolicited ASP Down Ack", got, StateASPDown)
				}
			default:
				t.Fatal("no state was published")
			}

			// Applying it must start the climb back: ASP Up first.
			if err := conn.handleStateUpdate(StateASPDown); err != nil {
				t.Fatalf("handleStateUpdate(AspDown): %v", err)
			}
			if got := typeNames(*sent); len(got) != 1 || got[0] != "ASP Up" {
				t.Fatalf("signals = %v, want [ASP Up] to begin returning to %v", got, tt.wantBack)
			}

			// The SGP acks, taking the ASP to ASP-INACTIVE.
			if err := conn.handleStateUpdate(StateASPInactive); err != nil {
				t.Fatalf("handleStateUpdate(AspInactive): %v", err)
			}
			got := typeNames(*sent)
			switch tt.wantBack {
			case StateASPActive:
				if len(got) != 2 || got[1] != "ASP Active" {
					t.Errorf("signals = %v, want [ASP Up, ASP Active] to return to ASP-ACTIVE", got)
				}
			case StateASPInactive:
				if len(got) != 1 {
					t.Errorf("signals = %v, want just [ASP Up]: the ASP was only ASP-INACTIVE and must not take traffic", got)
				}
			}
		})
	}
}
