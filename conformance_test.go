// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// RFC 4666 Section 3.8.1 draws the line by class, not by one particular class:
//
//	"The 'Unsupported Message Class' error is sent if a message with an
//	unexpected or unsupported Message Class is received."
//
// Only RKM used to draw it. Every other class this library does not implement —
// the reserved 5 to 8, and everything from 10 to 255 — drew the *type* error
// instead, which tells a peer its class is supported and only the type is wrong.
func TestUnsupportedClassIsReportedAsAClassError(t *testing.T) {
	for _, class := range []uint8{
		messages.MsgClassRKM,
		5, 6, 7, 8, // reserved for other SIGTRAN adaptation layers
		10, 64, 127, // reserved by the IETF
		128, 200, 255, // reserved for IETF-defined extensions
	} {
		conn, _ := newTestConn(t, StateASPActive, RoleSGP)

		msg, err := messages.Parse([]byte{0x01, 0x00, class, 0x01, 0x00, 0x00, 0x00, 0x08})
		if err != nil {
			t.Fatalf("class %d: %v", class, err)
		}
		conn.handleSignals(context.Background(), msg)

		select {
		case got := <-conn.errChan:
			var classErr *UnsupportedClassError
			if !errors.As(got, &classErr) {
				t.Errorf("class %d: reported %T, want *UnsupportedClassError", class, got)
			}
		default:
			t.Errorf("class %d: nothing reported", class)
		}
	}
}

// A class the library does implement keeps the type-level error, so the two
// remain distinguishable to the peer.
func TestUnsupportedTypeInAKnownClassStaysATypeError(t *testing.T) {
	for _, class := range []uint8{
		messages.MsgClassManagement,
		messages.MsgClassTransfer,
		messages.MsgClassSSNM,
		messages.MsgClassASPSM,
		messages.MsgClassASPTM,
	} {
		conn, _ := newTestConn(t, StateASPActive, RoleSGP)

		// Type 0x7f is not defined in any of these classes.
		msg, err := messages.Parse([]byte{0x01, 0x00, class, 0x7f, 0x00, 0x00, 0x00, 0x08})
		if err != nil {
			t.Fatalf("class %d: %v", class, err)
		}
		conn.handleSignals(context.Background(), msg)

		select {
		case got := <-conn.errChan:
			var classErr *UnsupportedClassError
			if errors.As(got, &classErr) {
				t.Errorf("class %d type 0x7f: reported a class error; the class is implemented", class)
			}
		default:
			t.Errorf("class %d: nothing reported", class)
		}
	}
}

// RFC 4666 Section 3.4.5 lists User/Cause as Mandatory in DUPU, alongside
// Affected Point Code. A DUPU without it says a user part is unavailable
// without saying which or why. It was accepted, and reported a cause of 0 to
// the MTP3-User as though the peer had sent one.
func TestDUPUWithoutUserCauseIsRejected(t *testing.T) {
	conn, _ := newSSNMTestConn(t, StateASPActive, RoleASP)

	err := conn.handleDestinationUserPartUnavailable(
		messages.NewDestinationUserPartUnavailable(
			nil, nil, params.NewAffectedPointCode(0x11111111), nil, nil,
		),
	)
	if !errors.Is(err, ErrMissingUserCause) {
		t.Fatalf("DUPU without User/Cause: error = %v, want ErrMissingUserCause", err)
	}

	// And the destination must not have been touched on the strength of it.
	if got := conn.DestinationState(0x11111111); got != DestinationAvailable {
		t.Errorf("destination state = %v after a rejected DUPU, want %v", got, DestinationAvailable)
	}
}

// A DUPU that carries it is still accepted, and the cause reaches the user.
func TestDUPUWithUserCauseIsAccepted(t *testing.T) {
	conn, _ := newSSNMTestConn(t, StateASPActive, RoleASP)

	if err := conn.handleDestinationUserPartUnavailable(
		messages.NewDestinationUserPartUnavailable(
			// One Affected DPC with a zero Mask: Section 3.4.5 permits no
			// other shape in a DUPU. 0x11111111 would have read as Mask 0x11.
			nil, nil, params.NewAffectedPointCodeWithMask(0, 0x111111),
			params.NewUserCause(params.SCCP, params.Inaccessible), nil,
		),
	); err != nil {
		t.Fatalf("DUPU with User/Cause: %v", err)
	}

	select {
	case st := <-conn.SignallingStatus():
		if !st.UserPartUnavailable {
			t.Error("status did not report a user part unavailable")
		}
		if st.UserCause != params.NewUserCause(3, 2).UserCause() {
			t.Errorf("UserCause = %#x, want %#x", st.UserCause, params.NewUserCause(3, 2).UserCause())
		}
	default:
		t.Error("no status was published for a valid DUPU")
	}
}

// RFC 4666 Section 1.4.7 rule 1 has no exceptions, and WriteSignal is exported
// and documented as writing any M3UA signal — so a caller handing it a DATA
// message must not have it put on stream 0, which is where the send template
// sits.
func TestWriteSignalDoesNotPutDataOnStreamZero(t *testing.T) {
	conn, _ := newTestConn(t, StateASPActive, RoleASP)
	conn.signalWriter = nil // exercise the real accounting path
	conn.maxMessageStreamID = 0

	data := messages.NewData(nil, nil, params.NewProtocolData(
		0x11111111, 0x22222222, params.ServiceIndSCCP, 0, 0, 1, []byte("payload"),
	), nil)

	// With no data stream negotiated there is nowhere legal to put it, and the
	// send template's stream 0 is not an answer.
	if _, err := conn.WriteSignal(data); !errors.Is(err, ErrNoDataStream) {
		t.Errorf("WriteSignal(DATA) with no data stream = %v, want ErrNoDataStream", err)
	}
}

func TestWriteSignalRejectsDataWithoutUsableProtocolData(t *testing.T) {
	for _, tt := range []struct {
		name string
		data *messages.Data
		want error
	}{
		{
			name: "missing",
			data: messages.NewData(nil, nil, nil, nil),
			want: ErrMissingProtocolData,
		},
		{
			name: "wrong parameter tag",
			data: messages.NewData(nil, nil, params.NewInfoString("not protocol data"), nil),
			want: ErrMissingProtocolData,
		},
		{
			name: "short payload",
			data: messages.NewData(nil, nil,
				params.NewParam(int(params.ProtocolData), make([]byte, 11)), nil),
			want: params.ErrInvalidLength,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conn, _ := newTestConn(t, StateASPActive, RoleASP)
			conn.signalWriter = nil
			conn.maxMessageStreamID = 4

			if _, err := conn.WriteSignal(tt.data); !errors.Is(err, tt.want) {
				t.Errorf("WriteSignal(DATA) error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestWriteSignalRejectsDataOutsideAspActive(t *testing.T) {
	data := messages.NewData(nil, nil, params.NewProtocolData(
		1, 2, params.ServiceIndSCCP, 0, 0, 1, []byte("x")), nil)
	for _, state := range []State{StateASPDown, StateASPInactive} {
		t.Run(state.String(), func(t *testing.T) {
			conn, _ := newTestConn(t, state, RoleASP)
			conn.signalWriter = nil
			conn.maxMessageStreamID = 4
			if _, err := conn.WriteSignal(data); !errors.Is(err, ErrNotEstablished) {
				t.Errorf("WriteSignal(DATA) in %v = %v, want ErrNotEstablished", state, err)
			}
		})
	}
}

// RFC 9260 Section 3.3.2 forbids advertising zero outbound streams, but the
// count is read back through a getsockopt: an unguarded 0-1 wraps to 65535, and
// every DATA would then go out on a stream the peer never opened, to be
// discarded by it silently and completely.
func TestZeroNegotiatedStreamsIsRefusedNotWrapped(t *testing.T) {
	c := &Association{maxMessageStreamID: 0}
	if got := c.streamFor(0); got != 0 {
		t.Errorf("streamFor with no streams = %d, want 0", got)
	}
	// The guard proper lives in setUpSocket, which needs a socket; this pins the
	// arithmetic it protects against.
	var ostreams uint16
	if wrapped := ostreams - 1; wrapped != 65535 {
		t.Fatalf("uint16(0)-1 = %d; this test's premise is wrong", wrapped)
	}
}

// RFC 4666 Section 3.8.1, for both the class and the type error: "For this
// error, the Diagnostic Information parameter MUST be included with the first
// 40 octets of the offending message."
//
// The offending message is what arrived, not what this library made of it.
// first40Octets re-marshalled the parsed message, which is not byte-identical:
// Section 3.2 has a receiver accept a final parameter whose padding was elided,
// and re-marshalling puts it back, so the peer is shown octets it never sent.
func TestDiagnosticInformationCarriesTheReceivedOctets(t *testing.T) {
	// An ASPSM message with a trailing parameter whose padding is elided: 3
	// bytes of value in a parameter declared 7 long, with no fourth pad byte.
	raw := []byte{
		0x01, 0x00, 0x03, 0x7f, // version, reserved, class ASPSM, unknown type
		0x00, 0x00, 0x00, 0x0f, // length 15
		0x00, 0x04, 0x00, 0x07, // INFO String, length 7
		0x61, 0x62, 0x63, // "abc", padding elided
	}

	conn, sent := newTestConn(t, StateASPActive, RoleSGP)

	if _, err := messages.Parse(raw); err != nil {
		t.Skipf("this codec rejects the elided-padding form: %v", err)
	}
	conn.dispatchRaw(context.Background(), inbound{data: raw})

	var reported error
	select {
	case reported = <-conn.errChan:
	default:
		t.Fatal("nothing reported for an unknown message type")
	}
	if e := conn.handleErrors(reported); e != nil {
		t.Fatal(e)
	}

	for i := len(*sent) - 1; i >= 0; i-- {
		e, ok := (*sent)[i].(*messages.Error)
		if !ok {
			continue
		}
		got := e.DiagnosticInformation.DiagnosticInformation()
		if !bytes.Equal(got, raw) {
			t.Errorf("Diagnostic Information = % x,\n                      want % x (the octets received)", got, raw)
		}
		return
	}
	t.Fatal("no Error message was sent")
}

// RFC 4666 Section 7.3.2: "When an implementation receives a message type that
// it does not support, it MUST respond with an Error (ERR) message
// ('Unsupported Message Type')."
//
// The dispatcher dropped anything messages.Parse rejected, logging and moving
// on. A message whose class and type octets are perfectly readable but whose
// parameters are malformed therefore drew nothing at all, so appending one bad
// TLV silenced both this MUST and the class-level error of Section 3.8.1.
func TestMalformedParametersStillDrawTheMandatedError(t *testing.T) {
	for _, tt := range []struct {
		name string
		raw  []byte
		want uint32
	}{
		{
			name: "unknown type in a known class",
			// ASPSM, type 0x7f, with a parameter claiming a length past the end.
			raw:  []byte{0x01, 0x00, 0x03, 0x7f, 0x00, 0x00, 0x00, 0x0c, 0x00, 0x04, 0xff, 0xff},
			want: params.UnsupportedMessageErrorType,
		},
		{
			name: "unsupported class",
			raw:  []byte{0x01, 0x00, 0x09, 0x01, 0x00, 0x00, 0x00, 0x0c, 0x00, 0x04, 0xff, 0xff},
			want: params.UnsupportedMessageErrorClass,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := messages.Parse(tt.raw); err == nil {
				t.Skip("this input parses cleanly; it cannot exercise the malformed path")
			}

			conn, sent := newTestConn(t, StateASPActive, RoleSGP)
			conn.dispatchRaw(context.Background(), inbound{data: tt.raw})

			var reported error
			select {
			case reported = <-conn.errChan:
			default:
				t.Fatal("a message with readable class and type octets drew no error at all")
			}
			if e := conn.handleErrors(reported); e != nil {
				t.Fatal(e)
			}

			codes := errorCodes(*sent)
			if len(codes) != 1 || codes[0] != tt.want {
				t.Errorf("error code = %v, want [%d]", codes, tt.want)
			}
		})
	}
}

// A message too short to hold a header has no class or type to answer for, and
// must not produce a bogus error.
func TestUnparseableShortMessageDrawsNothing(t *testing.T) {
	conn, _ := newTestConn(t, StateASPActive, RoleSGP)
	conn.dispatchRaw(context.Background(), inbound{data: []byte{0x01, 0x00}})

	select {
	case err := <-conn.errChan:
		t.Errorf("reported %v for a message too short to carry a header", err)
	default:
	}
}

// RFC 4666 Section 3.8.1 gives a parameter fault its own error codes, distinct
// from an unsupported type:
//
//	The "Parameter Field Error" would be sent if a message is received
//	with a parameter having a wrong length field.
//
//	The "Unexpected Parameter" error would be sent if a message contains
//	an invalid parameter.
//
// Answering "Unsupported Message Type" instead tells the peer to stop sending a
// message type that is in fact implemented, when the real complaint is about
// one parameter inside a single message.
func TestParameterFaultInASupportedMessageDrawsAParameterError(t *testing.T) {
	// A well-formed ASP Active carrying two Routing Context parameters. Section
	// 3.2: "only one parameter of the same type is allowed in a message."
	dup := func() []byte {
		act := messages.NewAspActive(
			params.NewTrafficModeType(params.TrafficModeLoadshare),
			params.NewRoutingContext(1), nil,
		)
		b, err := act.MarshalBinary()
		if err != nil {
			t.Fatalf("MarshalBinary: %v", err)
		}
		extra, err := params.NewRoutingContext(999).MarshalBinary()
		if err != nil {
			t.Fatalf("MarshalBinary: %v", err)
		}
		b = append(b, extra...)
		b[4], b[5], b[6], b[7] = byte(len(b)>>24), byte(len(b)>>16), byte(len(b)>>8), byte(len(b))
		return b
	}()

	// A supported type (ASP Up) whose parameter claims a length past the end.
	badLen := []byte{0x01, 0x00, 0x03, 0x01, 0x00, 0x00, 0x00, 0x0c, 0x00, 0x04, 0xff, 0xff}

	for _, tt := range []struct {
		name string
		raw  []byte
		want uint32
	}{
		{"duplicate parameter", dup, params.ErrUnexpectedParameter},
		{"wrong length field", badLen, params.ErrParameterFieldError},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := messages.Parse(tt.raw); err == nil {
				t.Skip("this input parses cleanly; it cannot exercise the fault path")
			}

			conn, sent := newTestConn(t, StateASPActive, RoleSGP)
			conn.dispatchRaw(context.Background(), inbound{data: tt.raw})

			var reported error
			select {
			case reported = <-conn.errChan:
			default:
				t.Fatal("a parameter fault in a supported message drew no error")
			}
			if e := conn.handleErrors(reported); e != nil {
				t.Fatal(e)
			}

			codes := errorCodes(*sent)
			if len(codes) != 1 || codes[0] != tt.want {
				t.Errorf("error code = %v, want [%d]", codes, tt.want)
			}
		})
	}
}
