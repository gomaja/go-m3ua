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

// ParseWithOptions is the only receive path that may relax anything, so the
// zero value of ParseOptions has to be exactly the strict decoder -- not
// "close enough". A caller that constructs ParseOptions and leaves the
// Tolerator unset has asked for no tolerance at all, and every accepted
// message and every rejection must match Parse on the same octets.
func TestParseWithOptionsWithZeroOptionsIsStrictParse(t *testing.T) {
	t.Run("accepted messages", func(t *testing.T) {
		for _, fixture := range validTypedMessageFixtures() {
			t.Run(fixture.name, func(t *testing.T) {
				wire, err := fixture.message.MarshalBinary()
				if err != nil {
					t.Fatalf("MarshalBinary() error = %v", err)
				}
				strict, strictErr := Parse(wire)
				zero, zeroErr := ParseWithOptions(wire, ParseOptions{})
				if strictErr != nil || zeroErr != nil {
					t.Fatalf("Parse() error = %v, ParseWithOptions(zero) error = %v", strictErr, zeroErr)
				}
				if diff := cmp.Diff(strict, zero); diff != "" {
					t.Errorf("zero options decoded differently from Parse (-parse +zero):\n%s", diff)
				}
			})
		}
	})

	t.Run("rejections", func(t *testing.T) {
		for _, test := range strictlyRejectedWires(t) {
			t.Run(test.name, func(t *testing.T) {
				strict, strictErr := Parse(test.wire)
				zero, zeroErr := ParseWithOptions(test.wire, ParseOptions{})
				if strictErr == nil {
					t.Fatalf("Parse() accepted %s; the case no longer tests a rejection", test.name)
				}
				if zeroErr == nil {
					t.Fatalf("ParseWithOptions(zero) accepted what Parse rejected: %v", zero)
				}
				if strictErr.Error() != zeroErr.Error() {
					t.Errorf("ParseWithOptions(zero) error = %q, Parse error = %q", zeroErr, strictErr)
				}
				if strict != nil || zero != nil {
					t.Errorf("a rejection returned a message: Parse %v, zero options %v", strict, zero)
				}
			})
		}
	})
}

// The optional INFO String tolerance is the one relaxation this package
// offers, and it is scoped to a single parameter value on messages that define
// that parameter. It must not become a way in for anything else: not DATA's
// parameter order, not a missing mandatory parameter, and not a Routing Context
// whose value is not the "n x 32 bits" RFC 4666 Section 3.4.1 defines it as.
//
// Every case below is run under all three decisions, because the accept and
// drop paths rebuild the octets and re-decode, and a rebuild is exactly where a
// second fault could be lost.
func TestInfoStringToleranceNeverBypassesValidation(t *testing.T) {
	validStatus := rawParameter(params.Status, []byte{0x00, 0x01, 0x00, 0x02})
	malformedRoutingContext := rawParameter(params.RoutingContext, []byte{0x00, 0x00, 0x09})
	invalidInfoString := rawParameter(params.InfoString, []byte{0xff, 0xfe})
	protocolData, err := params.NewProtocolData(
		1, 2, params.ServiceIndSCCP, 0, 0, 1, []byte("data")).MarshalBinary()
	if err != nil {
		t.Fatalf("Protocol Data MarshalBinary() error = %v", err)
	}
	routingContext, err := params.NewRoutingContext(7).MarshalBinary()
	if err != nil {
		t.Fatalf("Routing Context MarshalBinary() error = %v", err)
	}
	networkAppearance, err := params.NewNetworkAppearance(8).MarshalBinary()
	if err != nil {
		t.Fatalf("Network Appearance MarshalBinary() error = %v", err)
	}

	tests := []struct {
		name         string
		wire         []byte
		toleratedErr error
	}{
		{
			name: "malformed Routing Context ahead of the invalid INFO String",
			wire: NewHeader(1, MsgClassManagement, MsgTypeNotify,
				concatParameters(validStatus, malformedRoutingContext, invalidInfoString),
			).mustMarshalForTest(t),
			toleratedErr: params.ErrInvalidLength,
		},
		{
			name: "malformed Routing Context behind the invalid INFO String",
			wire: NewHeader(1, MsgClassManagement, MsgTypeNotify,
				concatParameters(validStatus, invalidInfoString, malformedRoutingContext),
			).mustMarshalForTest(t),
			toleratedErr: params.ErrInvalidLength,
		},
		{
			name: "empty Routing Context behind the invalid INFO String",
			wire: NewHeader(1, MsgClassManagement, MsgTypeNotify,
				concatParameters(validStatus, invalidInfoString,
					rawParameter(params.RoutingContext, nil)),
			).mustMarshalForTest(t),
			toleratedErr: params.ErrInvalidLength,
		},
		{
			name: "mandatory Status missing behind the invalid INFO String",
			wire: NewHeader(1, MsgClassManagement, MsgTypeNotify,
				concatParameters(invalidInfoString, routingContext),
			).mustMarshalForTest(t),
			toleratedErr: ErrMissingParameter,
		},
		{
			name: "DATA Network Appearance out of order beside an invalid INFO String",
			wire: NewHeader(1, MsgClassTransfer, MsgTypePayloadData,
				concatParameters(routingContext, networkAppearance, protocolData, invalidInfoString),
			).mustMarshalForTest(t),
			toleratedErr: params.ErrInvalidValue,
		},
		{
			name: "DATA Network Appearance out of order behind a valid INFO String",
			wire: NewHeader(1, MsgClassTransfer, MsgTypePayloadData,
				concatParameters(routingContext, networkAppearance, protocolData,
					rawParameter(params.InfoString, []byte("info"))),
			).mustMarshalForTest(t),
			toleratedErr: ErrInvalidParameter,
		},
	}

	decisions := []struct {
		name     string
		decision ProtocolDecision
	}{
		{"accept", ProtocolAccept},
		{"drop", ProtocolDropParameter},
		{"reject", ProtocolReject},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := Parse(test.wire); err == nil {
				t.Fatal("strict Parse accepted the wire; the case no longer tests a rejection")
			}
			for _, decision := range decisions {
				t.Run(decision.name, func(t *testing.T) {
					message, err := ParseWithOptions(test.wire, ParseOptions{
						Tolerator: ToleratorFunc(func(ProtocolViolation) ProtocolDecision {
							return decision.decision
						}),
					})
					if err == nil {
						t.Fatalf("tolerance produced a message from invalid octets: %v", message)
					}
					if message != nil {
						t.Errorf("a rejected decode returned %v as well as %v", message, err)
					}
					if decision.decision == ProtocolReject {
						// Rejecting reports the classified violation itself,
						// which is a different, equally valid refusal.
						return
					}
					if !errors.Is(err, test.toleratedErr) {
						t.Errorf("ParseWithOptions() error = %v, want %v", err, test.toleratedErr)
					}
				})
			}
		})
	}
}

// strictlyRejectedWires is the malformed-input corpus the zero-options case
// re-checks: one shape per class of rejection the decoder owns.
func strictlyRejectedWires(t *testing.T) []struct {
	name string
	wire []byte
} {
	t.Helper()

	aspUp, err := NewAspUp(params.NewAspIdentifier(7), params.NewInfoString("valid")).MarshalBinary()
	if err != nil {
		t.Fatalf("ASP Up MarshalBinary() error = %v", err)
	}
	data, err := NewData(nil, nil,
		params.NewProtocolData(1, 2, params.ServiceIndSCCP, 0, 0, 1, []byte("data")), nil).MarshalBinary()
	if err != nil {
		t.Fatalf("DATA MarshalBinary() error = %v", err)
	}
	routingContext, err := params.NewRoutingContext(7).MarshalBinary()
	if err != nil {
		t.Fatalf("Routing Context MarshalBinary() error = %v", err)
	}
	networkAppearance, err := params.NewNetworkAppearance(8).MarshalBinary()
	if err != nil {
		t.Fatalf("Network Appearance MarshalBinary() error = %v", err)
	}

	return []struct {
		name string
		wire []byte
	}{
		{"invalid optional INFO String", replaceParameterValue(t, aspUp, params.InfoString, []byte{0xff, 0xfe})},
		{"oversized INFO String", replaceParameterValue(t, aspUp, params.InfoString, bytes.Repeat([]byte{'x'}, 256))},
		{"duplicate INFO String", NewHeader(aspUp[0], aspUp[2], aspUp[3],
			concatParameters(aspUp[8:], rawParameter(params.InfoString, []byte("second")))).mustMarshalForTest(t)},
		{"missing mandatory Status", NewHeader(1, MsgClassManagement, MsgTypeNotify,
			rawParameter(params.InfoString, []byte("info"))).mustMarshalForTest(t)},
		{"DATA Network Appearance out of order", NewHeader(1, MsgClassTransfer, MsgTypePayloadData,
			concatParameters(routingContext, networkAppearance, data[8:])).mustMarshalForTest(t)},
		{"malformed Routing Context", NewHeader(1, MsgClassManagement, MsgTypeNotify,
			concatParameters(rawParameter(params.Status, []byte{0x00, 0x01, 0x00, 0x02}),
				rawParameter(params.RoutingContext, []byte{0x00, 0x00, 0x09}))).mustMarshalForTest(t)},
		{"truncated common header", []byte{0x01, 0x00, 0x03, 0x01}},
		{"declared length beyond the octets received", []byte{0x01, 0x00, 0x03, 0x01, 0xff, 0xff, 0xff, 0xff}},
		{"parameter length below the TLV header", NewHeader(1, MsgClassASPSM, MsgTypeAspUp,
			[]byte{0x00, 0x11, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07}).mustMarshalForTest(t)},
	}
}

func concatParameters(parts ...[]byte) []byte {
	var out []byte
	for _, part := range parts {
		out = append(out, part...)
	}
	return out
}
