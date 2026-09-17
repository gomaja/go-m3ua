// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// TestParameterFaultErrorNeverSendsAnEmptyDiagnosticInformation covers RFC 4666
// Section 3.8.1, which lists the parameter as Conditional and says what it is
// for:
//
//	When included, the optional Diagnostic Information can be any
//	information germane to the error condition, to assist in
//	identification of the error condition.  The Diagnostic Information
//	SHOULD contain the offending message.
//
// A Network Appearance rejected while validating a message that had already
// decoded raises the fault with no received octets to quote — the dispatcher
// keeps those only for the messages it could not parse — and the parameter was
// attached regardless. What went on the wire was a Diagnostic Information with
// a header and no value: germane to nothing, and telling the peer strictly less
// than the Error Code beside it already did.
func TestParameterFaultErrorNeverSendsAnEmptyDiagnosticInformation(t *testing.T) {
	for _, tt := range []struct {
		name       string
		appearance *params.Param
		want       []byte
	}{
		{
			// Three octets where Section 3.4 defines four: "a parameter having
			// a wrong length field", which Section 3.8.1 answers with 0x12.
			name:       "wrong length",
			appearance: &params.Param{Tag: params.NetworkAppearance, Length: 7, Data: []byte{0x00, 0x00, 0x01}},
			want: []byte{
				0x01, 0x00, 0x00, 0x00, // version 1, reserved, Management, Error
				0x00, 0x00, 0x00, 0x10, // length 16: Error Code and nothing else
				0x00, 0x0c, 0x00, 0x08, // Error Code, length 8
				0x00, 0x00, 0x00, 0x12, // Parameter Field Error
			},
		},
		{
			// The right place in the message, the wrong tag: "an invalid
			// parameter", which Section 3.8.1 answers with 0x13.
			name:       "wrong tag",
			appearance: &params.Param{Tag: params.CorrelationID, Length: 8, Data: []byte{0x00, 0x00, 0x00, 0x01}},
			want: []byte{
				0x01, 0x00, 0x00, 0x00,
				0x00, 0x00, 0x00, 0x10,
				0x00, 0x0c, 0x00, 0x08,
				0x00, 0x00, 0x00, 0x13, // Unexpected Parameter
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conn, sent := newTestConnWithContexts(t, StateASPActive, RoleSGP, 7)

			conn.handleData(context.Background(), messages.NewData(
				tt.appearance,
				params.NewRoutingContext(7),
				params.NewProtocolData(0x111111, 0x222222, 3, 0, 0, 1, []byte("x")),
				nil,
			))

			reported := firstErr(conn)
			if reported == nil {
				t.Fatal("a DATA with a malformed Network Appearance was accepted")
			}
			if err := conn.handleErrors(reported); err != nil {
				t.Fatal(err)
			}

			e := lastError(t, *sent)
			if e.DiagnosticInformation != nil && len(e.DiagnosticInformation.DiagnosticInformation()) == 0 {
				t.Error("the Error carried a Diagnostic Information parameter with no value at all")
			}

			got, err := e.MarshalBinary()
			if err != nil {
				t.Fatalf("MarshalBinary: %v", err)
			}
			if !bytes.Equal(got, tt.want) {
				t.Errorf("Error on the wire =\n\t%x\nwant\n\t%x", got, tt.want)
			}
		})
	}
}

// The parameter is dropped only where there is nothing to put in it. A
// parameter fault the dispatcher raised for a message it could not parse does
// carry that message, and still quotes it.
func TestParameterFaultErrorQuotesTheOctetsItWasGiven(t *testing.T) {
	conn, sent := newTestConn(t, StateASPActive, RoleSGP)

	// A supported ASP Up whose parameter length runs past the message.
	raw := []byte{0x01, 0x00, 0x03, 0x01, 0x00, 0x00, 0x00, 0x0c, 0x00, 0x04, 0xff, 0xff}
	if err := conn.handleErrors(NewParameterFaultErrorFor(raw, params.ErrInvalidLength)); err != nil {
		t.Fatal(err)
	}

	e := lastError(t, *sent)
	if e.DiagnosticInformation == nil {
		t.Fatal("the Error quoted no offending message at all")
	}
	if got := e.DiagnosticInformation.DiagnosticInformation(); !bytes.Equal(got, raw) {
		t.Errorf("Diagnostic Information = %x, want the offending message %x", got, raw)
	}

	want := []byte{
		0x01, 0x00, 0x00, 0x00, // version 1, reserved, Management, Error
		0x00, 0x00, 0x00, 0x20, // length 32
		0x00, 0x0c, 0x00, 0x08, // Error Code, length 8
		0x00, 0x00, 0x00, 0x12, // Parameter Field Error
		0x00, 0x07, 0x00, 0x10, // Diagnostic Information, length 16
	}
	want = append(want, raw...)

	got, err := e.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("Error on the wire =\n\t%x\nwant\n\t%x", got, want)
	}
}

// The octets quoted in an Error are the error event's own, down to the
// parameter that carries them: diagnosticInformation "returns an owned copy of
// the offending message", and the copy is what keeps anything still holding the
// error — a user handler that matched it with errors.As, for one — from
// rewriting an Error that has already been decided. The constructors are
// covered by TestDiagnosticErrorsOwnTheirReceivedBytes; this pins the second
// hand-off, from the error to the parameter.
func TestDiagnosticInformationDoesNotAliasTheErrorsOctets(t *testing.T) {
	conn, sent := newTestConn(t, StateASPActive, RoleSGP)

	raw := []byte{0x01, 0x00, 0x03, 0x01, 0x00, 0x00, 0x00, 0x0c, 0x00, 0x04, 0xff, 0xff}
	reported := NewParameterFaultErrorFor(raw, params.ErrInvalidLength)
	if err := conn.handleErrors(reported); err != nil {
		t.Fatal(err)
	}

	e := lastError(t, *sent)
	if e.DiagnosticInformation == nil {
		t.Fatal("the Error carried no Diagnostic Information")
	}
	quoted := e.DiagnosticInformation.DiagnosticInformation()
	want := bytes.Clone(quoted)

	for i := range reported.Raw {
		reported.Raw[i] ^= 0xff
	}
	if !bytes.Equal(quoted, want) {
		t.Errorf("Diagnostic Information became % x when the error's own octets were "+
			"overwritten, want the unchanged % x", quoted, want)
	}
}

// Parameter faults are captured on the goroutine that dispatched the message
// and rendered on the one that answers it, which is what fsm.go's handoff
// requires them to own their octets for. Run under -race this fails if the
// responder touches storage the dispatcher is still using: the received buffer
// the fault was built from, or the decoded message behind it, whose re-marshal
// would write back into parameters the dispatcher owns.
func TestParameterFaultErrorRenderingIsRaceFree(t *testing.T) {
	const messageCount = 64

	conn, sent := newTestConn(t, StateASPActive, RoleSGP)
	quoted := []byte{0x01, 0x00, 0x03, 0x01, 0x00, 0x00, 0x00, 0x0c, 0x00, 0x04, 0xff, 0xff}

	var rendered sync.WaitGroup
	rendered.Add(1)
	go func() {
		defer rendered.Done()
		for i := 0; i < messageCount; i++ {
			if err := conn.handleErrors(<-conn.errChan); err != nil {
				return
			}
		}
	}()

	for i := 0; i < messageCount; i++ {
		// Half the faults carry the received octets; half are raised against a
		// message that decoded, and carry none.
		if i%2 == 0 {
			received := bytes.Clone(quoted)
			conn.sendErr(NewParameterFaultErrorFor(received, params.ErrInvalidLength))
			// The dispatcher goes on using the buffer it reported from while
			// the responder is still rendering.
			for j := range received {
				received[j] ^= 0xff
			}
			continue
		}

		offending := messages.NewAspUp(params.NewAspIdentifier(uint32(i)), nil)
		conn.sendErrForMessage(offending, NewParameterFaultErrorFor(nil, params.ErrInvalidLength))
		// The dispatcher goes on using the message it just reported.
		offending.SetLength()
	}

	rendered.Wait()

	for _, m := range *sent {
		e, ok := m.(*messages.Error)
		if !ok || e.DiagnosticInformation == nil {
			continue
		}
		if got := e.DiagnosticInformation.DiagnosticInformation(); !bytes.Equal(got, quoted) {
			t.Fatalf("Diagnostic Information = % x, want the octets as captured % x", got, quoted)
		}
	}
}
