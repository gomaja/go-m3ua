// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// TestAnErrorIsNeverAnsweredWithAnError covers the flat prohibition in RFC 4666
// Section 3.8.1:
//
//	Error messages MUST NOT be generated in response to other Error
//	messages.
//
// A well-formed ERR was already left alone, but one the parser rejected was
// not: the dispatcher fell back to reading the class and type octets, saw
// Management, and answered. Two peers each sending an ERR the other cannot
// parse then answer each other indefinitely, which is exactly the loop the
// sentence exists to prevent.
func TestAnErrorIsNeverAnsweredWithAnError(t *testing.T) {
	for _, tt := range []struct {
		name string
		raw  []byte
	}{
		{
			// Class 0 (Management), type 0 (ERR), with a parameter whose
			// length field runs past the end of the message.
			name: "unparseable ERR",
			raw:  []byte{0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0c, 0x00, 0x04, 0xff, 0xff},
		},
		{
			// Class 0, type 0, with two Error Code parameters: a duplicate,
			// which the decoder now refuses.
			name: "ERR with a duplicate parameter",
			raw: func() []byte {
				e := messages.NewError(params.NewErrorCode(params.ErrProtocolError), nil, nil, nil, nil)
				b, err := e.MarshalBinary()
				if err != nil {
					t.Fatalf("MarshalBinary: %v", err)
				}
				extra, err := params.NewErrorCode(params.ErrProtocolError).MarshalBinary()
				if err != nil {
					t.Fatalf("MarshalBinary: %v", err)
				}
				b = append(b, extra...)
				b[4], b[5], b[6], b[7] = byte(len(b)>>24), byte(len(b)>>16), byte(len(b)>>8), byte(len(b))
				return b
			}(),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := messages.Parse(tt.raw); err == nil {
				t.Skip("this input parses cleanly; it cannot exercise the path")
			}

			conn, sent := newTestConn(t, StateASPActive, RoleSGP)
			conn.dispatchRaw(context.Background(), inbound{data: tt.raw})

			select {
			case reported := <-conn.errChan:
				if e := conn.handleErrors(reported); e != nil {
					t.Fatal(e)
				}
			default:
			}

			if codes := errorCodes(*sent); len(codes) != 0 {
				t.Errorf("answered an unparseable ERR with error codes %v; "+
					"Section 3.8.1 forbids generating an Error in response to "+
					"an Error", codes)
			}
		})
	}
}

// A message of some other class that fails to parse is still answered, so the
// rule above is not silently swallowing everything.
func TestANonErrorMessageIsStillAnswered(t *testing.T) {
	// ASPSM (class 3), ASP Up (type 1), with a bad parameter length.
	raw := []byte{0x01, 0x00, 0x03, 0x01, 0x00, 0x00, 0x00, 0x0c, 0x00, 0x04, 0xff, 0xff}
	if _, err := messages.Parse(raw); err == nil {
		t.Skip("this input parses cleanly")
	}

	conn, sent := newTestConn(t, StateASPActive, RoleSGP)
	conn.dispatchRaw(context.Background(), inbound{data: raw})

	select {
	case reported := <-conn.errChan:
		if e := conn.handleErrors(reported); e != nil {
			t.Fatal(e)
		}
	default:
		t.Fatal("a malformed ASP Up drew no error at all")
	}
	if codes := errorCodes(*sent); len(codes) != 1 {
		t.Errorf("error codes = %v, want exactly one", codes)
	}
}

// TestUnexpectedMessageErrorQuotesThePeersRoutingContexts covers RFC 4666
// Section 3.8.1's rule for that code:
//
//	If the Unexpected message contained Routing Contexts, the Routing
//	Contexts SHOULD be included in the Error message.
//
// The Error carried this node's configured contexts instead, which tells the
// peer nothing about the message it got wrong — and, on an association serving
// several Application Servers, names contexts that had nothing to do with it.
func TestUnexpectedMessageErrorQuotesThePeersRoutingContexts(t *testing.T) {
	// ASP-DOWN, so an ASP Active is unexpected. Its Routing Context is one this
	// association serves, so the Error is about the state, not the context.
	conn, sent := newTestConn(t, StateASPDown, RoleASP)

	offending := messages.NewAspActive(
		params.NewTrafficModeType(params.TrafficModeLoadshare),
		params.NewRoutingContext(2),
		nil,
	)
	if err := conn.handleErrors(NewUnexpectedMessageError(offending)); err != nil {
		t.Fatal(err)
	}

	e := lastError(t, *sent)
	if e.RoutingContext == nil {
		t.Fatal("the Error carried no Routing Context at all")
	}
	if got := e.RoutingContext.RoutingContexts(); len(got) != 1 || got[0] != 2 {
		t.Errorf("Error named Routing Contexts %v, want [2] — the ones the "+
			"unexpected message carried, not this node's configuration", got)
	}
}

// When the unexpected message carried no Routing Context, none is invented: the
// rule is conditional on the offending message having had them.
func TestUnexpectedMessageErrorWithoutRoutingContextsInventsNone(t *testing.T) {
	conn, sent := newTestConn(t, StateASPDown, RoleASP)

	if err := conn.handleErrors(NewUnexpectedMessageError(
		messages.NewHeartbeat(params.NewHeartbeatData([]byte("x"))),
	)); err != nil {
		t.Fatal(err)
	}

	e := lastError(t, *sent)
	if e.RoutingContext != nil {
		t.Errorf("Error named Routing Contexts %v although the unexpected "+
			"message carried none", e.RoutingContext.RoutingContexts())
	}
}

// TestInvalidRoutingContextErrorAlwaysQuotesThePeers covers the same section's
// stronger rule for its own code:
//
//	The "Invalid Routing Context" error is sent if a message is received
//	from a peer with an invalid (unconfigured) Routing Context value.
//	For this error, the invalid Routing Context(s) MUST be included in
//	the Error message.
//
// There was a second path that reported the bare sentinel and filled the Error
// with this node's configured contexts — the one set of values guaranteed not
// to be the invalid ones, since they are exactly what the check compares
// against.
func TestInvalidRoutingContextErrorAlwaysQuotesThePeers(t *testing.T) {
	conn, sent := newTestConn(t, StateASPActive, RoleSGP)

	if err := conn.handleErrors(NewInvalidRoutingContextError(4242)); err != nil {
		t.Fatal(err)
	}

	e := lastError(t, *sent)
	if e.ErrorCode == nil || e.ErrorCode.ErrorCode() != params.ErrInvalidRoutingContext {
		t.Fatalf("error code = %v, want Invalid Routing Context", e.ErrorCode)
	}
	if e.RoutingContext == nil {
		t.Fatal("the Error carried no Routing Context")
	}
	if got := e.RoutingContext.RoutingContexts(); len(got) != 1 || got[0] != 4242 {
		t.Errorf("Error named Routing Contexts %v, want [4242] — the offending "+
			"value, never this node's configuration", got)
	}
}

// lastError returns the final Error message the Association wrote.
func lastError(t *testing.T, sent []messages.M3UA) *messages.Error {
	t.Helper()
	for i := len(sent) - 1; i >= 0; i-- {
		if e, ok := sent[i].(*messages.Error); ok {
			return e
		}
	}
	t.Fatalf("no Error message was sent (sent %v)", typeNames(sent))
	return nil
}

// TestInvalidVersionErrorIndicatesOurVersionAndQuotesTheMessage covers RFC 4666
// Section 4.8 — "the receiving end responds with an Error message indicating
// the version the receiving node supports" — together with Section 3.8.1's
// "The Diagnostic Information SHOULD contain the offending message."
//
// There is no Version parameter, so the indication is the Error's own common
// header. The offending octets were not quoted at all, leaving the peer told
// that some version was wrong but not which message carried it.
func TestInvalidVersionErrorIndicatesOurVersionAndQuotesTheMessage(t *testing.T) {
	// An ASP Up claiming version 2.
	raw := []byte{0x02, 0x00, 0x03, 0x01, 0x00, 0x00, 0x00, 0x08}

	conn, sent := newTestConn(t, StateASPActive, RoleSGP)
	conn.dispatchRaw(context.Background(), inbound{data: raw})

	var reported error
	select {
	case reported = <-conn.errChan:
	default:
		t.Fatal("a message with an unsupported version drew no error")
	}
	if e := conn.handleErrors(reported); e != nil {
		t.Fatal(e)
	}

	e := lastError(t, *sent)
	if e.ErrorCode == nil || e.ErrorCode.ErrorCode() != params.InvalidVersionError {
		t.Fatalf("error code = %v, want Invalid Version", e.ErrorCode)
	}
	if got := e.Version(); got != 1 {
		t.Errorf("the Error's own version = %d, want 1 — that is what tells the "+
			"peer which version this node supports", got)
	}
	if e.DiagnosticInformation == nil {
		t.Fatal("no Diagnostic Information; Section 3.8.1 says it SHOULD contain " +
			"the offending message")
	}
	if got := e.DiagnosticInformation.DiagnosticInformation(); !bytes.Equal(got, raw) {
		t.Errorf("Diagnostic Information = % x, want % x (the offending message)", got, raw)
	}
}

// The Diagnostic Information belongs to the error event, not to the
// association. The dispatcher can receive another message after handing an
// error to monitor(), so consulting Association-wide receive state while the Error is
// built can quote the later message instead of the one that failed.
func TestDiagnosticInformationSurvivesTheNextReceivedMessage(t *testing.T) {
	tests := []struct {
		name string
		raw  []byte
		code uint32
	}{
		{
			name: "invalid version",
			raw: []byte{
				0x02, 0x00, 0x03, 0x01, 0x00, 0x00, 0x00, 0x0f,
				0x00, 0x04, 0x00, 0x07, 0x61, 0x62, 0x63,
			},
			code: params.InvalidVersionError,
		},
		{
			name: "unsupported message type",
			// The final parameter's padding is deliberately elided. Re-marshalling
			// the parsed message is therefore not byte-identical to what arrived.
			raw: []byte{
				0x01, 0x00, 0x03, 0x7f, 0x00, 0x00, 0x00, 0x0f,
				0x00, 0x04, 0x00, 0x07, 0x61, 0x62, 0x63,
			},
			code: params.UnsupportedMessageErrorType,
		},
		{
			name: "unsupported message class",
			raw: []byte{
				0x01, 0x00, 0x09, 0x01, 0x00, 0x00, 0x00, 0x0f,
				0x00, 0x04, 0x00, 0x07, 0x61, 0x62, 0x63,
			},
			code: params.UnsupportedMessageErrorClass,
		},
		{
			name: "parameter field error",
			// Supported ASP Up with a parameter length that runs past the
			// message. This path already carried raw bytes before the fix and is
			// the control proving that remains true.
			raw:  []byte{0x01, 0x00, 0x03, 0x01, 0x00, 0x00, 0x00, 0x0c, 0x00, 0x04, 0xff, 0xff},
			code: params.ErrParameterFieldError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn, sent := newTestConn(t, StateASPActive, RoleSGP)
			conn.dispatchRaw(context.Background(), inbound{data: tt.raw, stream: 1})

			var reported error
			select {
			case reported = <-conn.errChan:
			default:
				t.Fatal("the offending message drew no error")
			}

			// Simulate the dispatcher advancing before monitor builds the ERR.
			// This short message has no class or type, so it replaces receive
			// state without generating a second error.
			conn.dispatchRaw(context.Background(), inbound{data: []byte{0xaa, 0xbb}, stream: 2})

			if err := conn.handleErrors(reported); err != nil {
				t.Fatal(err)
			}
			e := lastError(t, *sent)
			if e.ErrorCode == nil || e.ErrorCode.ErrorCode() != tt.code {
				t.Fatalf("error code = %v, want %d", e.ErrorCode, tt.code)
			}
			if e.DiagnosticInformation == nil {
				t.Fatal("the Error carried no Diagnostic Information")
			}
			if got := e.DiagnosticInformation.DiagnosticInformation(); !bytes.Equal(got, tt.raw) {
				t.Errorf("Diagnostic Information = % x, want % x (the offending message)", got, tt.raw)
			}
		})
	}
}

func TestDiagnosticErrorsOwnTheirReceivedBytes(t *testing.T) {
	for _, tt := range []struct {
		name string
		make func([]byte) error
	}{
		{"invalid version", func(raw []byte) error { return newInvalidVersionErrorFor(2, raw) }},
		{"unsupported class", func(raw []byte) error { return NewUnsupportedClassErrorFor(raw) }},
		{"unsupported type", func(raw []byte) error { return NewUnsupportedMessageErrorFor(raw) }},
		{"parameter fault", func(raw []byte) error { return NewParameterFaultErrorFor(raw, params.ErrInvalidLength) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			raw := []byte{0x01, 0x00, 0x09, 0x7f, 0x00, 0x00, 0x00, 0x08}
			want := append([]byte(nil), raw...)
			reported := tt.make(raw)
			for i := range raw {
				raw[i] = 0xff
			}

			conn, sent := newTestConn(t, StateASPActive, RoleSGP)
			if err := conn.handleErrors(reported); err != nil {
				t.Fatal(err)
			}
			e := lastError(t, *sent)
			if e.DiagnosticInformation == nil {
				t.Fatal("the Error carried no Diagnostic Information")
			}
			if got := e.DiagnosticInformation.DiagnosticInformation(); !bytes.Equal(got, want) {
				t.Errorf("Diagnostic Information = % x, want owned snapshot % x", got, want)
			}
		})
	}
}

// Message Length belongs to the common header, not to a variable-length
// parameter. A contradiction between that field and the received message is a
// protocol anomaly, so RFC 4666 Section 3.8.1 assigns Protocol Error rather
// than Parameter Field Error or Unexpected Parameter.
func TestInvalidCommonHeaderLengthDrawsProtocolError(t *testing.T) {
	for _, raw := range [][]byte{
		// Supported ASP Up declaring less than the common header itself.
		{0x01, 0x00, 0x03, 0x01, 0x00, 0x00, 0x00, 0x07},
		// Supported ASP Up declaring an octet that was not received.
		{0x01, 0x00, 0x03, 0x01, 0x00, 0x00, 0x00, 0x09},
	} {
		conn, sent := newTestConn(t, StateASPActive, RoleSGP)
		conn.dispatchRaw(context.Background(), inbound{data: raw})

		var reported error
		select {
		case reported = <-conn.errChan:
		default:
			t.Fatal("invalid common-header length drew no error")
		}
		if err := conn.handleErrors(reported); err != nil {
			t.Fatal(err)
		}

		if codes := errorCodes(*sent); len(codes) != 1 || codes[0] != params.ErrProtocolError {
			t.Errorf("error codes = %v, want [%d] (Protocol Error)", codes, params.ErrProtocolError)
		}
	}
}

// A syntactically complete message that omits a parameter its message format
// marks Mandatory draws Missing Parameter, not the generic Unexpected
// Parameter response used for a forbidden or duplicate TLV.
func TestMissingMandatoryParameterDrawsMissingParameterError(t *testing.T) {
	raw := []byte{
		0x01, 0x00, messages.MsgClassTransfer, messages.MsgTypePayloadData,
		0x00, 0x00, 0x00, 0x08,
	}
	if _, err := messages.Parse(raw); !errors.Is(err, messages.ErrMissingParameter) {
		t.Fatalf("Parse() error = %v, want ErrMissingParameter", err)
	}

	conn, sent := newTestConn(t, StateASPActive, RoleSGP)
	conn.dispatchRaw(context.Background(), inbound{data: raw})

	var reported error
	select {
	case reported = <-conn.errChan:
	default:
		t.Fatal("DATA without Protocol Data drew no error")
	}
	if err := conn.handleErrors(reported); err != nil {
		t.Fatal(err)
	}
	if codes := errorCodes(*sent); len(codes) != 1 || codes[0] != params.ErrMissingParameter {
		t.Errorf("error codes = %v, want [%d] (Missing Parameter)", codes, params.ErrMissingParameter)
	}
}

func TestInvalidINFOStringDrawsInvalidParameterValue(t *testing.T) {
	raw := []byte{
		0x01, 0x00, messages.MsgClassASPSM, messages.MsgTypeAspUp,
		0x00, 0x00, 0x00, 0x10,
		0x00, byte(params.InfoString), 0x00, 0x05,
		0xff, 0x00, 0x00, 0x00,
	}
	if _, err := messages.Parse(raw); !errors.Is(err, params.ErrInvalidValue) {
		t.Fatalf("Parse() error = %v, want params.ErrInvalidValue", err)
	}

	conn, sent := newTestConn(t, StateASPDown, RoleSGP)
	conn.dispatchRaw(context.Background(), inbound{data: raw, ppid: M3UAPPID})
	reported := <-conn.errChan
	if err := conn.handleErrors(reported); err != nil {
		t.Fatal(err)
	}
	if codes := errorCodes(*sent); len(codes) != 1 || codes[0] != params.ErrInvalidParameterValue {
		t.Errorf("error codes = %v, want [%d] (Invalid Parameter Value)", codes, params.ErrInvalidParameterValue)
	}
}

func TestUnsupportedMessageDiagnosticIsExactlyFirst40Octets(t *testing.T) {
	for _, size := range []int{39, 40, 41} {
		t.Run(fmt.Sprintf("%d octets", size), func(t *testing.T) {
			raw := make([]byte, size)
			for i := range raw {
				raw[i] = byte(i)
			}
			conn, sent := newTestConn(t, StateASPActive, RoleSGP)
			if err := conn.handleErrors(NewUnsupportedMessageErrorFor(raw)); err != nil {
				t.Fatal(err)
			}
			got := lastError(t, *sent).DiagnosticInformation.DiagnosticInformation()
			wantLen := size
			if wantLen > 40 {
				wantLen = 40
			}
			if len(got) != wantLen {
				t.Fatalf("Diagnostic Information length = %d, want %d", len(got), wantLen)
			}
			if !bytes.Equal(got, raw[:wantLen]) {
				t.Errorf("Diagnostic Information = % x, want first %d octets % x", got, wantLen, raw[:wantLen])
			}
		})
	}
}

func TestInvalidVersionDiagnosticKeepsTheWholeOffendingMessage(t *testing.T) {
	raw := make([]byte, 41)
	for i := range raw {
		raw[i] = byte(i)
	}
	conn, sent := newTestConn(t, StateASPActive, RoleSGP)
	if err := conn.handleErrors(newInvalidVersionErrorFor(2, raw)); err != nil {
		t.Fatal(err)
	}
	got := lastError(t, *sent).DiagnosticInformation.DiagnosticInformation()
	if !bytes.Equal(got, raw) {
		t.Errorf("Diagnostic Information = % x, want complete offending message % x", got, raw)
	}
}

func TestDiagnosticInformationRespectsTheParameterLengthBoundary(t *testing.T) {
	for _, size := range []int{maxDiagnosticInformationLen, maxDiagnosticInformationLen + 1} {
		t.Run(fmt.Sprintf("%d octets", size), func(t *testing.T) {
			raw := make([]byte, size)
			for i := range raw {
				raw[i] = byte(i)
			}
			conn, sent := newTestConn(t, StateASPActive, RoleSGP)
			if err := conn.handleErrors(newInvalidVersionErrorFor(2, raw)); err != nil {
				t.Fatal(err)
			}
			e := lastError(t, *sent)
			if got := len(e.DiagnosticInformation.DiagnosticInformation()); got != maxDiagnosticInformationLen {
				t.Fatalf("Diagnostic Information length = %d, want %d", got, maxDiagnosticInformationLen)
			}
			encoded, err := e.MarshalBinary()
			if err != nil {
				t.Fatalf("MarshalBinary at the uint16 parameter boundary: %v", err)
			}
			parsed, err := messages.Parse(encoded)
			if err != nil {
				t.Fatalf("Parse marshalled Error at the uint16 parameter boundary: %v", err)
			}
			got := parsed.(*messages.Error).DiagnosticInformation.DiagnosticInformation()
			if len(got) != maxDiagnosticInformationLen || !bytes.Equal(got, raw[:maxDiagnosticInformationLen]) {
				t.Errorf("round-trip Diagnostic Information length = %d, want first %d source octets",
					len(got), maxDiagnosticInformationLen)
			}
		})
	}
}

// TestShutdownAnnouncesBeforeClosing covers RFC 4666 Section 4.9 procedure (a):
//
//	a) Send the sequence of ASP-INACTIVE, DEREG (optionally whenever
//	   dynamic registration is used), and ASP-DOWN messages and perform
//	   the SCTP Shutdown procedure after that.
//
// Close implements procedure (b), "Just do the SCTP Shutdown procedure", which
// the section equally allows — but (a) was not available at all, so a peer
// learned an ASP had gone only when its socket vanished.
func TestShutdownAnnouncesBeforeClosing(t *testing.T) {
	conn, sent := newTestConn(t, StateASPActive, RoleASP)
	installImmediateShutdownPeer(conn, sent)
	// newTestConn closes done on cleanup; Shutdown closing it first is fine
	// because closeWith is once-only.
	_ = conn.Shutdown()

	got := typeNames(*sent)
	if len(got) < 2 {
		t.Fatalf("Shutdown sent %v, want an ASP Inactive then an ASP Down", got)
	}
	if got[0] != "ASP Inactive" {
		t.Errorf("first signal = %q, want %q", got[0], "ASP Inactive")
	}
	if got[1] != "ASP Down" {
		t.Errorf("second signal = %q, want %q", got[1], "ASP Down")
	}
}

// From ASP-INACTIVE there is no traffic to stop, so only the ASP Down is due.
func TestShutdownFromInactiveSendsOnlyAspDown(t *testing.T) {
	conn, sent := newTestConn(t, StateASPInactive, RoleASP)
	installImmediateShutdownPeer(conn, sent)
	_ = conn.Shutdown()

	got := typeNames(*sent)
	if len(got) != 1 || got[0] != "ASP Down" {
		t.Errorf("Shutdown from ASP-INACTIVE sent %v, want [ASP Down]", got)
	}
}

// ASP Inactive and ASP Down are ASP-originated procedures. An SGP choosing the
// orderly SCTP shutdown option in Section 4.9 must not impersonate its peer by
// sending either request; it simply performs SCTP Shutdown.
func TestSGPShutdownDoesNotSendASPRequests(t *testing.T) {
	for _, state := range []State{StateASPActive, StateASPInactive, StateASPDown} {
		t.Run(state.String(), func(t *testing.T) {
			conn, sent := newTestConn(t, state, RoleSGP)
			_ = conn.Shutdown()

			if got := typeNames(*sent); len(got) != 0 {
				t.Errorf("SGP Shutdown sent ASP-originated requests %v", got)
			}
		})
	}
}
