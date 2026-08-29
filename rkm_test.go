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

// RKM (dynamic Routing Key registration) is not implemented, and RFC 4666 is
// specific about how a node says so. Section 4.4.1: "If the SGP does not
// support the registration procedure, the SGP returns an Error message to the
// ASP, with an error code of 'Unsupported Message Class'."
//
// Answering with the type-level error instead tells a peer that the class is
// supported but that particular type is not, so an ASP attempting dynamic
// registration cannot tell it must fall back to static configuration. It
// retries, or treats a working SGP as broken.

// rkmMessage builds a bare RKM message of the given type. RKM has no codecs
// here, so these parse as *messages.Generic — which is exactly the path a real
// REG REQ takes today.
func rkmMessage(t *testing.T, msgType uint8) messages.M3UA {
	t.Helper()

	m, err := messages.Parse([]byte{
		0x01, 0x00, messages.MsgClassRKM, msgType, 0x00, 0x00, 0x00, 0x08,
	})
	if err != nil {
		t.Fatalf("parsing a bare RKM message: %v", err)
	}
	return m
}

// Every RKM message type must be answered with "Unsupported Message Class".
func TestRKMIsAnsweredWithUnsupportedClass(t *testing.T) {
	for _, tt := range []struct {
		name    string
		msgType uint8
	}{
		{"REG REQ", messages.MsgTypeRegistrationRequest},
		{"REG RSP", messages.MsgTypeRegistrationResponse},
		{"DEREG REQ", messages.MsgTypeDeregistrationRequest},
		{"DEREG RSP", messages.MsgTypeDeregistrationResponse},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conn, sent := newTestConn(t, StateASPActive, RoleSGP)

			conn.handleSignals(context.Background(), rkmMessage(t, tt.msgType))

			// The dispatcher reports through errChan; handleErrors is what puts
			// the Error on the wire, so run it as monitor() would.
			var reported error
			select {
			case reported = <-conn.errChan:
			default:
				t.Fatal("no error reported for an RKM message")
			}

			var unsupportedClass *UnsupportedClassError
			if !errors.As(reported, &unsupportedClass) {
				t.Fatalf("reported %T (%v), want *UnsupportedClassError per RFC 4666 Section 4.4.1",
					reported, reported)
			}

			if e := conn.handleErrors(reported); e != nil {
				t.Fatalf("handleErrors() error = %v", e)
			}
			codes := errorCodes(*sent)
			if len(codes) != 1 || codes[0] != params.UnsupportedMessageErrorClass {
				t.Errorf("error code = %v, want [%d] (Unsupported Message Class); [%d] is the type-level error",
					codes, params.UnsupportedMessageErrorClass, params.UnsupportedMessageErrorType)
			}
		})
	}
}

// An unknown class that is not RKM keeps the type-level error: only RKM has the
// RFC clause requiring the class-level one.
// An unassigned class draws the class-level error, not the type-level one.
//
// RFC 4666 Section 3.8.1: "The 'Unsupported Message Class' error is sent if a
// message with an unexpected or unsupported Message Class is received." Class 7
// is "Reserved for Other SIGTRAN Adaptation Layers" (Section 3.1.2) and is not
// supported here, so the class is what is wrong with the message. This used to
// report Unsupported Message Type, which tells the peer its class was fine.
func TestUnassignedClassDrawsTheClassLevelError(t *testing.T) {
	conn, sent := newTestConn(t, StateASPActive, RoleSGP)

	msg, err := messages.Parse([]byte{0x01, 0x00, 0x07, 0x01, 0x00, 0x00, 0x00, 0x08})
	if err != nil {
		t.Fatal(err)
	}
	conn.handleSignals(context.Background(), msg)

	var reported error
	select {
	case reported = <-conn.errChan:
	default:
		t.Fatal("no error reported")
	}
	if e := conn.handleErrors(reported); e != nil {
		t.Fatal(e)
	}

	codes := errorCodes(*sent)
	if len(codes) != 1 || codes[0] != params.UnsupportedMessageErrorClass {
		t.Errorf("error code = %v, want [%d] (Unsupported Message Class) for an unassigned class",
			codes, params.UnsupportedMessageErrorClass)
	}
}

// A Generic carries the class byte it parsed, so it must report that class
// rather than "Unknown". The dispatcher decides how to answer a message by
// class, and an Error message names the class it is rejecting — both are wrong
// if every unhandled message claims to be class "Unknown".
func TestGenericReportsItsActualClass(t *testing.T) {
	for _, tt := range []struct {
		class uint8
		want  string
	}{
		{messages.MsgClassRKM, messages.MsgClassNameRKM},
		{messages.MsgClassManagement, messages.MsgClassNameManagement},
		{messages.MsgClassSSNM, messages.MsgClassNameSSNM},
		{0x07, "Unknown"}, // genuinely unassigned
	} {
		t.Run(tt.want, func(t *testing.T) {
			// Type 0x7f is undefined in every class, so this parses as Generic.
			msg, err := messages.Parse([]byte{0x01, 0x00, tt.class, 0x7f, 0x00, 0x00, 0x00, 0x08})
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if _, ok := msg.(*messages.Generic); !ok {
				t.Fatalf("parsed as %T, want *messages.Generic", msg)
			}
			if got := msg.MessageClassName(); got != tt.want {
				t.Errorf("MessageClassName() = %q, want %q", got, tt.want)
			}
		})
	}
}

// The RKM answer must not depend on role or state: an ASP and an SGP both lack
// the procedures, and a peer may send REG REQ at any point after ASP Up.
func TestRKMAnswerIsRoleAndStateIndependent(t *testing.T) {
	for _, role := range []Role{RoleASP, RoleSGP} {
		for _, st := range []State{StateASPDown, StateASPInactive, StateASPActive} {
			conn, _ := newTestConn(t, st, role)

			conn.handleSignals(context.Background(),
				rkmMessage(t, messages.MsgTypeRegistrationRequest))

			select {
			case err := <-conn.errChan:
				var unsupportedClass *UnsupportedClassError
				if !errors.As(err, &unsupportedClass) {
					t.Errorf("role %s state %v: reported %T, want *UnsupportedClassError", role, st, err)
				}
			default:
				t.Errorf("role %s state %v: no error reported for RKM", role, st)
			}

			// And it must still publish exactly one state, holding the current one.
			if got := len(conn.stateChan); got != 1 {
				t.Errorf("role %s state %v: published %d states, want 1", role, st, got)
			}
		}
	}
}

// Only ASP and SGP roles are implemented here. An Association holding any other
// role must report it rather than falling through to the ASP path: IPSP (RFC 4666 Section
// 1.4.3.4) is the same procedures with symmetric roles, so adding it means
// adding an arm here, and a missed one would run an IPSP through the ASP state
// machine — sending ASP Up to a peer that is itself waiting to send one.
func TestUnsupportedModeIsReported(t *testing.T) {
	conn, _ := newTestConn(t, StateASPDown, RoleASP)
	conn.role = Role(0xff)

	err := conn.handleStateUpdate(StateASPDown)
	if !errors.Is(err, ErrUnsupportedRole) {
		t.Errorf("handleStateUpdate() error = %v, want ErrUnsupportedRole", err)
	}
}

// The dispatcher must answer every message a peer can send, whatever its class
// and type, without panicking and without escaping the one-state-per-message
// invariant the read loop depends on. This sweeps the full class/type space
// rather than the handful of combinations the concrete codecs cover.
func FuzzDispatchAnyClassAndType(f *testing.F) {
	for _, seed := range [][2]uint8{
		{messages.MsgClassRKM, messages.MsgTypeRegistrationRequest},
		{messages.MsgClassSSNM, 1},
		{messages.MsgClassASPSM, 1},
		{messages.MsgClassASPTM, 1},
		{messages.MsgClassManagement, 0},
		{messages.MsgClassTransfer, 1},
		{0x07, 0x7f},
		{0xff, 0xff},
	} {
		f.Add(seed[0], seed[1], uint8(0))
	}

	f.Fuzz(func(t *testing.T, class, msgType, stateSeed uint8) {
		states := []State{StateASPDown, StateASPInactive, StateASPActive}
		st := states[int(stateSeed)%len(states)]

		msg, err := messages.Parse([]byte{
			0x01, 0x00, class, msgType, 0x00, 0x00, 0x00, 0x08,
		})
		if err != nil {
			return // a bare header this package rejects is not our concern
		}

		for _, role := range []Role{RoleASP, RoleSGP} {
			conn, _ := newTestConn(t, st, role)

			conn.handleSignals(context.Background(), msg)

			// Exactly one state per message: publishing none silently drops the
			// transition, publishing two applies a spurious one.
			if got := len(conn.stateChan); got != 1 {
				t.Fatalf("class=%d type=%d state=%v role=%s: published %d states, want 1",
					class, msgType, st, role, got)
			}

			// RKM must always be answered at the class level (Section 4.4.1).
			if class == messages.MsgClassRKM {
				select {
				case err := <-conn.errChan:
					var unsupportedClass *UnsupportedClassError
					if !errors.As(err, &unsupportedClass) {
						t.Fatalf("class=%d type=%d: reported %T, want *UnsupportedClassError",
							class, msgType, err)
					}
				default:
					t.Fatalf("class=%d type=%d: RKM reported no error", class, msgType)
				}
			}
		}
	})
}
