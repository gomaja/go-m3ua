package messages

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/google/go-cmp/cmp"
)

func TestParseWithOptionsAcceptsInvalidOptionalInfoString(t *testing.T) {
	invalid := []byte{0xff, 0xfe}

	for _, fixture := range infoStringMessageFixtures() {
		t.Run(fixture.name, func(t *testing.T) {
			base, err := fixture.message.MarshalBinary()
			if err != nil {
				t.Fatalf("base MarshalBinary() error = %v", err)
			}
			wire := replaceParameterValue(t, base, params.InfoString, invalid)

			if _, err := Parse(wire); !errors.Is(err, params.ErrInvalidValue) {
				t.Fatalf("strict Parse() error = %v, want params.ErrInvalidValue", err)
			}

			calls := 0
			msg, err := ParseWithOptions(wire, ParseOptions{
				Tolerator: ToleratorFunc(func(v ProtocolViolation) ProtocolDecision {
					calls++
					if v.Kind != ViolationInvalidOptionalInfoString {
						t.Fatalf("violation kind = %d, want invalid optional INFO String", v.Kind)
					}
					if v.ErrorCode != params.ErrInvalidParameterValue {
						t.Fatalf("error code = %d, want Invalid Parameter Value", v.ErrorCode)
					}
					if v.MessageClass != fixture.message.MessageClass() || v.MessageType != fixture.message.MessageType() {
						t.Fatalf("message = class %d type %d, want class %d type %d",
							v.MessageClass, v.MessageType, fixture.message.MessageClass(), fixture.message.MessageType())
					}
					if v.ParamTag != params.InfoString {
						t.Fatalf("parameter tag = %#04x, want INFO String", v.ParamTag)
					}
					if !bytes.Equal(v.RawMessage, wire) {
						t.Fatal("violation RawMessage did not preserve the offending message")
					}
					if !bytes.Equal(v.RawParameter[4:], invalid) {
						t.Fatalf("violation RawParameter value = % x, want % x", v.RawParameter[4:], invalid)
					}
					if !errors.Is(v.Cause, params.ErrInvalidValue) {
						t.Fatalf("cause = %v, want params.ErrInvalidValue", v.Cause)
					}
					return ProtocolAccept
				}),
			})
			if err != nil {
				t.Fatalf("compat ParseWithOptions() error = %v", err)
			}
			if calls != 1 {
				t.Fatalf("tolerator calls = %d, want 1", calls)
			}
			decodedInfo := infoField(msg).Interface().(*params.Param)
			if !bytes.Equal(decodedInfo.Data, invalid) {
				t.Fatalf("decoded INFO String data = % x, want % x", decodedInfo.Data, invalid)
			}
			if _, err := msg.MarshalBinary(); !errors.Is(err, params.ErrInvalidValue) {
				t.Fatalf("compat message MarshalBinary() error = %v, want params.ErrInvalidValue", err)
			}
		})
	}
}

func TestParseWithOptionsMatchesStrictParseWithoutViolation(t *testing.T) {
	for _, fixture := range validTypedMessageFixtures() {
		t.Run(fixture.name, func(t *testing.T) {
			wire, err := fixture.message.MarshalBinary()
			if err != nil {
				t.Fatalf("MarshalBinary() error = %v", err)
			}
			strict, err := Parse(wire)
			if err != nil {
				t.Fatalf("strict Parse() error = %v", err)
			}
			compat, err := ParseWithOptions(wire, ParseOptions{
				Tolerator: ToleratorFunc(func(v ProtocolViolation) ProtocolDecision {
					t.Fatalf("tolerator called for valid message: %+v", v)
					return ProtocolReject
				}),
			})
			if err != nil {
				t.Fatalf("compat ParseWithOptions() error = %v", err)
			}
			if diff := cmp.Diff(strict, compat); diff != "" {
				t.Fatalf("compat decode differs from strict decode (-want +got):\n%s", diff)
			}
			strictWire, err := strict.MarshalBinary()
			if err != nil {
				t.Fatalf("strict MarshalBinary() error = %v", err)
			}
			compatWire, err := compat.MarshalBinary()
			if err != nil {
				t.Fatalf("compat MarshalBinary() error = %v", err)
			}
			if !bytes.Equal(compatWire, strictWire) {
				t.Fatalf("compat marshal = % x, want strict % x", compatWire, strictWire)
			}
		})
	}
}

func TestParseWithOptionsCanDropInvalidOptionalInfoString(t *testing.T) {
	base, err := NewAspUp(params.NewAspIdentifier(7), params.NewInfoString("valid")).MarshalBinary()
	if err != nil {
		t.Fatalf("base MarshalBinary() error = %v", err)
	}
	wire := replaceParameterValue(t, base, params.InfoString, []byte{0xff})

	msg, err := ParseWithOptions(wire, ParseOptions{
		Tolerator: ToleratorFunc(func(v ProtocolViolation) ProtocolDecision {
			if v.Kind == ViolationInvalidOptionalInfoString {
				return ProtocolDropParameter
			}
			return ProtocolReject
		}),
	})
	if err != nil {
		t.Fatalf("ParseWithOptions() error = %v", err)
	}
	aspUp := msg.(*AspUp)
	if aspUp.InfoString != nil {
		t.Fatalf("InfoString = %v, want dropped", aspUp.InfoString)
	}
	if aspUp.AspIdentifier == nil || aspUp.AspIdentifier.AspIdentifier() != 7 {
		t.Fatalf("ASP Identifier was not preserved: %v", aspUp.AspIdentifier)
	}
}

func TestParseWithOptionsRejectsWhenToleratorRejects(t *testing.T) {
	base, err := NewAspUp(params.NewAspIdentifier(7), params.NewInfoString("valid")).MarshalBinary()
	if err != nil {
		t.Fatalf("base MarshalBinary() error = %v", err)
	}
	wire := replaceParameterValue(t, base, params.InfoString, []byte{0xff})

	if _, err := ParseWithOptions(wire, ParseOptions{
		Tolerator: ToleratorFunc(func(ProtocolViolation) ProtocolDecision {
			return ProtocolReject
		}),
	}); !errors.Is(err, params.ErrInvalidValue) {
		t.Fatalf("ParseWithOptions() error = %v, want params.ErrInvalidValue", err)
	}
}

func TestParseWithOptionsDoesNotTolerateUnsafeParameterFaults(t *testing.T) {
	t.Run("DATA Network Appearance must stay first", func(t *testing.T) {
		wire, err := New(
			1,
			MsgClassTransfer,
			MsgTypePayloadData,
			params.NewRoutingContext(7),
			params.NewNetworkAppearance(8),
			params.NewProtocolData(1, 2, params.ServiceIndSCCP, 0, 0, 1, []byte("data")),
		).MarshalBinary()
		if err != nil {
			t.Fatalf("base MarshalBinary() error = %v", err)
		}

		if _, err := Parse(wire); !errors.Is(err, ErrInvalidParameter) {
			t.Fatalf("strict Parse() error = %v, want ErrInvalidParameter", err)
		}
		if _, err := ParseWithOptions(wire, ParseOptions{Tolerator: acceptEveryViolation}); !errors.Is(err, ErrInvalidParameter) {
			t.Fatalf("ParseWithOptions() error = %v, want ErrInvalidParameter", err)
		}
	})

	t.Run("invalid INFO String does not mask missing mandatory parameter", func(t *testing.T) {
		wire := NewHeader(1, MsgClassManagement, MsgTypeNotify,
			rawParameter(params.InfoString, []byte{0xff}),
		).mustMarshalForTest(t)

		if _, err := Parse(wire); !errors.Is(err, params.ErrInvalidValue) {
			t.Fatalf("strict Parse() error = %v, want params.ErrInvalidValue", err)
		}
		if _, err := ParseWithOptions(wire, ParseOptions{Tolerator: acceptEveryViolation}); !errors.Is(err, ErrMissingParameter) {
			t.Fatalf("ParseWithOptions() error = %v, want ErrMissingParameter", err)
		}
	})

	t.Run("oversized INFO String", func(t *testing.T) {
		base, err := NewAspUp(params.NewAspIdentifier(7), params.NewInfoString("valid")).MarshalBinary()
		if err != nil {
			t.Fatalf("base MarshalBinary() error = %v", err)
		}
		wire := replaceParameterValue(t, base, params.InfoString, bytes.Repeat([]byte{'x'}, 256))

		if _, err := ParseWithOptions(wire, ParseOptions{Tolerator: acceptEveryViolation}); !errors.Is(err, params.ErrInvalidValue) {
			t.Fatalf("ParseWithOptions() error = %v, want params.ErrInvalidValue", err)
		}
	})

	t.Run("invalid non-INFO String value", func(t *testing.T) {
		base, err := NewAspActive(
			params.NewTrafficModeType(params.TrafficModeLoadshare),
			nil,
			params.NewInfoString("valid"),
		).MarshalBinary()
		if err != nil {
			t.Fatalf("base MarshalBinary() error = %v", err)
		}
		wire := replaceParameterValue(t, base, params.TrafficModeType, []byte{0, 0, 0, 99})

		if _, err := ParseWithOptions(wire, ParseOptions{Tolerator: acceptEveryViolation}); !errors.Is(err, params.ErrInvalidValue) {
			t.Fatalf("ParseWithOptions() error = %v, want params.ErrInvalidValue", err)
		}
	})

	t.Run("unexpected INFO String position", func(t *testing.T) {
		wire, err := NewData(
			nil,
			nil,
			params.NewProtocolData(1, 2, params.ServiceIndSCCP, 0, 0, 1, []byte("data")),
			nil,
		).MarshalBinary()
		if err != nil {
			t.Fatalf("base MarshalBinary() error = %v", err)
		}
		wire = NewHeader(wire[0], wire[2], wire[3],
			append(append([]byte(nil), wire[8:]...), rawParameter(params.InfoString, []byte{0xff})...),
		).mustMarshalForTest(t)

		if _, err := ParseWithOptions(wire, ParseOptions{Tolerator: acceptEveryViolation}); !errors.Is(err, params.ErrInvalidValue) {
			t.Fatalf("ParseWithOptions() error = %v, want params.ErrInvalidValue", err)
		}
	})

	t.Run("duplicate INFO String", func(t *testing.T) {
		base, err := NewAspUp(params.NewAspIdentifier(7), params.NewInfoString("valid")).MarshalBinary()
		if err != nil {
			t.Fatalf("base MarshalBinary() error = %v", err)
		}
		wire := NewHeader(base[0], base[2], base[3],
			append(append([]byte(nil), base[8:]...), rawParameter(params.InfoString, []byte{0xff})...),
		).mustMarshalForTest(t)

		if _, err := ParseWithOptions(wire, ParseOptions{Tolerator: acceptEveryViolation}); !errors.Is(err, ErrInvalidParameter) {
			t.Fatalf("ParseWithOptions() error = %v, want ErrInvalidParameter", err)
		}
	})
}

var acceptEveryViolation = ToleratorFunc(func(ProtocolViolation) ProtocolDecision {
	return ProtocolAccept
})

func (h *Header) mustMarshalForTest(t *testing.T) []byte {
	t.Helper()
	wire, err := h.MarshalBinary()
	if err != nil {
		t.Fatalf("Header.MarshalBinary() error = %v", err)
	}
	return wire
}

func rawParameter(tag uint16, value []byte) []byte {
	parameterLength := 4 + len(value)
	paddedLength := parameterLength + (4-parameterLength%4)%4
	wire := make([]byte, paddedLength)
	binary.BigEndian.PutUint16(wire[0:2], tag)
	binary.BigEndian.PutUint16(wire[2:4], uint16(parameterLength))
	copy(wire[4:], value)
	return wire
}
