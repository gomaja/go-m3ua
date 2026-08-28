package m3ua

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestInvalidInfoStringIsStrictByDefaultOnAssociation(t *testing.T) {
	conn, sent := newTestConn(t, StateASPDown, RoleSGP)
	conn.dispatchRaw(context.Background(), inbound{
		data: invalidInfoStringAspUp(t),
		ppid: M3UAPPID,
	})

	err := firstErr(conn)
	var parameterFault *ParameterFaultError
	if !errors.As(err, &parameterFault) {
		t.Fatalf("connection error = %v (%T), want ParameterFaultError", err, err)
	}
	if e := conn.handleErrors(err); e != nil {
		t.Fatalf("handleErrors() error = %v", e)
	}
	if codes := errorCodes(*sent); len(codes) != 1 || codes[0] != params.ErrInvalidParameterValue {
		t.Fatalf("error codes = %v, want [%d]", codes, params.ErrInvalidParameterValue)
	}
	if got := firstMessageType(*sent); got == "ASP Up Ack" {
		t.Fatal("strict connection acked ASP Up carrying invalid INFO String")
	}
}

func TestCompatibilityAcceptsInvalidInfoStringOnAssociation(t *testing.T) {
	conn, sent := newTestConn(t, StateASPDown, RoleSGP)
	conn.cfg.Compatibility = AcceptInvalidOptionalInfoString()
	conn.dispatchRaw(context.Background(), inbound{
		data: invalidInfoStringAspUp(t),
		ppid: M3UAPPID,
	})

	if got := firstMessageType(*sent); got != "ASP Up Ack" {
		t.Fatalf("first signal = %q, want ASP Up Ack; sent: %v", got, typeNames(*sent))
	}
	if codes := errorCodes(*sent); len(codes) != 0 {
		t.Fatalf("error codes = %v, want none", codes)
	}
	select {
	case got := <-conn.stateChan:
		if got != StateASPInactive {
			t.Fatalf("published state = %v, want %v", got, StateASPInactive)
		}
	default:
		t.Fatal("accepted ASP Up published no state")
	}
}

func TestCompatibilityToleratorReceivesOwnedViolationBytes(t *testing.T) {
	conn, _ := newTestConn(t, StateASPDown, RoleSGP)
	raw := invalidInfoStringAspUp(t)
	var captured ProtocolViolation
	conn.cfg.Compatibility = CompatibilityPolicy{
		Tolerator: ToleratorFunc(func(v ProtocolViolation) ProtocolDecision {
			captured = v
			return ProtocolAccept
		}),
	}

	conn.dispatchRaw(context.Background(), inbound{data: raw, ppid: M3UAPPID})
	raw[0] = 0xff

	if !bytes.HasPrefix(captured.RawMessage, []byte{0x01, 0x00, messages.MsgClassASPSM, messages.MsgTypeAspUp}) {
		t.Fatalf("RawMessage was not an owned copy of the offending message: % x", captured.RawMessage[:4])
	}
	if captured.Kind != ViolationInvalidOptionalInfoString {
		t.Fatalf("violation kind = %d, want invalid optional INFO String", captured.Kind)
	}
	if !errors.Is(captured.Cause, params.ErrInvalidValue) {
		t.Fatalf("cause = %v, want params.ErrInvalidValue", captured.Cause)
	}
}

func invalidInfoStringAspUp(t *testing.T) []byte {
	t.Helper()
	wire, err := messages.NewAspUp(params.NewAspIdentifier(7), params.NewInfoString("valid")).MarshalBinary()
	if err != nil {
		t.Fatalf("base ASP Up MarshalBinary() error = %v", err)
	}
	return replaceM3UAParameterValue(t, wire, params.InfoString, []byte{0xff, 0xfe})
}

func replaceM3UAParameterValue(t *testing.T, wire []byte, tag uint16, value []byte) []byte {
	t.Helper()
	for offset := 8; offset < len(wire); {
		if len(wire)-offset < 4 {
			t.Fatal("wire ends in a partial parameter header")
		}
		parameterTag := binary.BigEndian.Uint16(wire[offset : offset+2])
		parameterLength := int(binary.BigEndian.Uint16(wire[offset+2 : offset+4]))
		if parameterLength < 4 || parameterLength > len(wire)-offset {
			t.Fatal("wire contains an invalid parameter length")
		}
		paddedLength := parameterLength + (4-parameterLength%4)%4
		if parameterTag != tag {
			offset += paddedLength
			continue
		}

		newParameterLength := 4 + len(value)
		newPaddedLength := newParameterLength + (4-newParameterLength%4)%4
		newParameter := make([]byte, newPaddedLength)
		binary.BigEndian.PutUint16(newParameter[0:2], tag)
		binary.BigEndian.PutUint16(newParameter[2:4], uint16(newParameterLength))
		copy(newParameter[4:], value)

		replaced := make([]byte, 0, len(wire)-paddedLength+newPaddedLength)
		replaced = append(replaced, wire[:offset]...)
		replaced = append(replaced, newParameter...)
		replaced = append(replaced, wire[offset+paddedLength:]...)
		binary.BigEndian.PutUint32(replaced[4:8], uint32(len(replaced)))
		return replaced
	}
	t.Fatalf("wire has no parameter tag %#04x", tag)
	return nil
}

func firstMessageType(msgs []messages.M3UA) string {
	if len(msgs) == 0 {
		return ""
	}
	return msgs[0].MessageTypeName()
}
