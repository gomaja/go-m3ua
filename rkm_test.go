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

func registrationRequestMessage(t *testing.T) *messages.RegistrationRequest {
	t.Helper()
	routingKey, err := routingKeyParameter(RoutingKeyRegistrationRequest{
		LocalRoutingKeyIdentifier: 1,
		RoutingKey:                testRoutingKey(10, 100, 3),
	})
	if err != nil {
		t.Fatalf("routingKeyParameter: %v", err)
	}
	return messages.NewRegistrationRequest(routingKey)
}

func deregistrationRequestMessage() *messages.DeregistrationRequest {
	return messages.NewDeregistrationRequest(params.NewRoutingContext(1))
}

// RFC 4666 Section 4.4.1 requires an SGP that does not support Registration
// to answer REG REQ with Unsupported Message Class. Deregistration belongs to
// the same optional RKM procedure and is rejected at the same class boundary.
func TestRKMDisabledResponderReturnsUnsupportedClass(t *testing.T) {
	for _, tt := range []struct {
		name    string
		message messages.M3UA
	}{
		{"REG REQ", registrationRequestMessage(t)},
		{"DEREG REQ", deregistrationRequestMessage()},
	} {
		for _, role := range []Role{RoleSGP, RoleIPSP} {
			t.Run(tt.name+"/"+role.String(), func(t *testing.T) {
				conn, sent := newTestConn(t, StateASPInactive, role)

				conn.handleSignals(context.Background(), tt.message)

				var reported error
				select {
				case reported = <-conn.errChan:
				default:
					t.Fatal("no error reported for a disabled RKM request")
				}

				var unsupportedClass *UnsupportedClassError
				if !errors.As(reported, &unsupportedClass) {
					t.Fatalf("reported %T (%v), want *UnsupportedClassError per RFC 4666 Section 4.4.1",
						reported, reported)
				}

				if err := conn.handleErrors(reported); err != nil {
					t.Fatalf("handleErrors: %v", err)
				}
				codes := errorCodes(*sent)
				if len(codes) != 1 || codes[0] != params.UnsupportedMessageErrorClass {
					t.Errorf("error code = %v, want [%d]", codes, params.UnsupportedMessageErrorClass)
				}
			})
		}
	}
}

func TestRKMDisabledResponderReturnsUnsupportedClassBeforeParameterValidation(t *testing.T) {
	conn, _ := newTestConn(t, StateASPInactive, RoleSGP)
	conn.dispatchRaw(context.Background(), inbound{
		data:   []byte{0x01, 0x00, messages.MsgClassRKM, messages.MsgTypeRegistrationRequest, 0, 0, 0, 8},
		ppid:   M3UAPPID,
		stream: 0,
	})

	select {
	case reported := <-conn.errChan:
		var unsupportedClass *UnsupportedClassError
		if !errors.As(reported, &unsupportedClass) {
			t.Fatalf("reported %T (%v), want *UnsupportedClassError", reported, reported)
		}
	default:
		t.Fatal("no error reported for malformed disabled REG REQ")
	}
}

func TestRKMUnexpectedDirectionReturnsUnexpectedMessage(t *testing.T) {
	registrationResponse := messages.NewRegistrationResponse(params.NewRegistrationResult(
		params.NewRegistrationResultPayload(
			params.NewLocalRoutingKeyIdentifier(1),
			params.NewRegistrationStatus(params.SuccessfullyRegistered),
			params.NewRoutingContext(1),
		),
	))
	tests := []struct {
		name    string
		role    Role
		message messages.M3UA
	}{
		{"ASP receives REG REQ", RoleASP, registrationRequestMessage(t)},
		{"SGP receives REG RSP", RoleSGP, registrationResponse},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conn, _ := newTestConn(t, StateASPInactive, test.role)
			conn.handleSignals(context.Background(), test.message)
			select {
			case reported := <-conn.errChan:
				var unexpected *UnexpectedMessageError
				if !errors.As(reported, &unexpected) {
					t.Fatalf("reported %T (%v), want *UnexpectedMessageError", reported, reported)
				}
			default:
				t.Fatal("no error reported for wrong RKM direction")
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

			// An unknown type inside implemented RKM is a type-level error. A
			// supported RKM type is parsed above or rejected as malformed before
			// it reaches this dispatcher sweep.
			if class == messages.MsgClassRKM {
				select {
				case err := <-conn.errChan:
					var unsupportedType *UnsupportedMessageError
					if !errors.As(err, &unsupportedType) {
						t.Fatalf("class=%d type=%d: reported %T, want *UnsupportedMessageError",
							class, msgType, err)
					}
				default:
					t.Fatalf("class=%d type=%d: RKM reported no error", class, msgType)
				}
			}
		}
	})
}
