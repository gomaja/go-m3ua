// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package params

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestParams(t *testing.T) {
	cases := []struct {
		name       string
		structured *Param
		serialized []byte
	}{
		{
			"AspIdentifier",
			NewAspIdentifier(1),
			[]byte{0x00, 0x11, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01},
		},
		{
			"TrafficModeType",
			NewTrafficModeType(1),
			[]byte{0x00, 0x0b, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01},
		},
		{
			"NetworkAppearance",
			NewNetworkAppearance(1),
			[]byte{0x02, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01},
		},
		{
			"RoutingContext-single",
			NewRoutingContext(1),
			[]byte{0x00, 0x06, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01},
		},
		{
			"RoutingContext-multiple",
			NewRoutingContext(1, 2, 3),
			[]byte{
				0x00, 0x06, 0x00, 0x10, 0x00, 0x00, 0x00, 0x01,
				0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03,
			},
		},
		{
			"HeartbeatData",
			NewHeartbeatData([]byte("some information")),
			[]byte{
				0x00, 0x09, 0x00, 0x14, 0x73, 0x6f, 0x6d, 0x65,
				0x20, 0x69, 0x6e, 0x66, 0x6f, 0x72, 0x6d, 0x61,
				0x74, 0x69, 0x6f, 0x6e,
			},
		},
		{
			"ErrorCode",
			NewErrorCode(InvalidVersionError),
			[]byte{0x00, 0x0c, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01},
		},
		{
			"UserCause",
			NewUserCause(SCCP, Unequipped),
			[]byte{0x02, 0x04, 0x00, 0x08, 0x00, 0x01, 0x00, 0x03},
		},
		{
			"Status",
			NewStatus(AsStateActive),
			[]byte{0x00, 0x0d, 0x00, 0x08, 0x00, 0x01, 0x00, 0x03},
		},
		{
			"AffectedPointCode",
			NewAffectedPointCode(1, 2, 3),
			[]byte{
				0x00, 0x12, 0x00, 0x10, 0x00, 0x00, 0x00, 0x01,
				0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03,
			},
		},
		{
			"ConcernedDestination",
			NewConcernedDestination(1),
			[]byte{0x02, 0x06, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01},
		},
		{
			"CorrelationID",
			NewCorrelationID(1),
			[]byte{0x00, 0x13, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01},
		},
		{
			"InfoString",
			NewInfoString("some information"),
			[]byte{
				0x00, 0x04, 0x00, 0x14, 0x73, 0x6f, 0x6d, 0x65,
				0x20, 0x69, 0x6e, 0x66, 0x6f, 0x72, 0x6d, 0x61,
				0x74, 0x69, 0x6f, 0x6e,
			},
		},
		{
			"DiagnosticInformation",
			NewDiagnosticInformation([]byte("some information")),
			[]byte{
				0x00, 0x07, 0x00, 0x14, 0x73, 0x6f, 0x6d, 0x65,
				0x20, 0x69, 0x6e, 0x66, 0x6f, 0x72, 0x6d, 0x61,
				0x74, 0x69, 0x6f, 0x6e,
			},
		},
		{
			"CongestionIndications",
			NewCongestionIndications(1),
			[]byte{0x02, 0x05, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01},
		},
		{
			"LocalRoutingKeyIdentifier",
			NewLocalRoutingKeyIdentifier(1),
			[]byte{0x02, 0x0a, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01},
		},
		{
			"DestinationPointCode",
			NewDestinationPointCode(1),
			[]byte{0x02, 0x0b, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01},
		},
		{
			"OriginatingPointCodeList",
			NewOriginatingPointCodeList(1, 2, 3),
			[]byte{
				0x02, 0x0e, 0x00, 0x10, 0x00, 0x00, 0x00, 0x01,
				0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x03,
			},
		},
		{
			"ServiceIndicators",
			NewServiceIndicators(1, 2, 3),
			// Length 7, not 8: the trailing pad octet is not counted.
			[]byte{0x02, 0x0c, 0x00, 0x07, 0x01, 0x02, 0x03, 0x00},
		},
		{
			"RegistrationStatus",
			NewRegistrationStatus(1),
			[]byte{0x02, 0x12, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01},
		},
		{
			"DeregistrationStatus",
			NewDeregistrationStatus(1),
			[]byte{0x02, 0x13, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01},
		},
		{
			"Generic",
			NewParam(1, []byte{0xde, 0xad, 0xbe, 0xef}),
			[]byte{0x00, 0x01, 0x00, 0x08, 0xde, 0xad, 0xbe, 0xef},
		},
		{
			"ProtocolData",
			NewProtocolData(
				1, // OriginatingPointCode
				2, // DestinationPointCode
				3, // ServiceIndicator
				1, // NetworkIndicator
				0, // MessagePriority
				1, // SignalingLinkSelection
				[]byte{ // Data
					0xde, 0xad, 0xbe, 0xef,
				},
			),
			[]byte{
				// Param Header
				0x02, 0x10, 0x00, 0x14,
				// OPC
				0x00, 0x00, 0x00, 0x01,
				// DPC
				0x00, 0x00, 0x00, 0x02,
				// SI
				0x03,
				// NI
				0x01,
				// MP
				0x00,
				// SLS
				0x01,
				// Data
				0xde, 0xad, 0xbe, 0xef,
			},
		},
		{
			"RegistrationResult",
			NewRegistrationResult(
				NewRegistrationResultPayload(
					NewLocalRoutingKeyIdentifier(1),
					NewRegistrationStatus(1),
					NewRoutingContext(0),
				),
			),
			[]byte{
				// Param Header
				0x02, 0x08, 0x00, 0x1c,
				// LocalRoutingKeyIdentifier
				0x02, 0x0a, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01,
				// RegistrationStatus
				0x02, 0x12, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01,
				// RoutingContext
				0x00, 0x06, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00,
			},
		},
		{
			"DeregistrationResult",
			NewDeregistrationResult(
				NewDeregResultPayload(
					NewRoutingContext(1),
					NewDeregistrationStatus(1),
				),
			),
			[]byte{
				// Param Header
				0x02, 0x09, 0x00, 0x14,
				// RoutingContext
				0x00, 0x06, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01,
				// DeregistrationStatus
				0x02, 0x13, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01,
			},
		},
	}

	for _, c := range cases {
		t.Run("encode/"+c.name, func(t *testing.T) {
			got, err := c.structured.MarshalBinary()
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(got, c.serialized); diff != "" {
				t.Error(diff)
			}
		})

		t.Run("decode/"+c.name, func(t *testing.T) {
			got, err := Parse(c.serialized)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(got, c.structured); diff != "" {
				t.Error(diff)
			}
		})
	}

}

func TestKnownParameterValueLengths(t *testing.T) {
	tests := []struct {
		name    string
		tag     uint16
		valid   []int
		invalid []int
	}{
		{
			name:    "Routing Context",
			tag:     RoutingContext,
			valid:   []int{4, 8, 12},
			invalid: []int{0, 1, 3, 5, 6, 7, 9},
		},
		{
			name:    "Affected Point Code",
			tag:     AffectedPointCode,
			valid:   []int{4, 8, 12},
			invalid: []int{0, 1, 3, 5, 6, 7, 9},
		},
		{
			name:    "Originating Point Code List",
			tag:     OriginatingPointCodeList,
			valid:   []int{4, 8, 12},
			invalid: []int{0, 1, 3, 5, 6, 7, 9},
		},
		{
			name:    "Service Indicators",
			tag:     ServiceIndicators,
			valid:   []int{1, 2, 3, 4, 7},
			invalid: []int{0},
		},
		{
			name:    "Protocol Data",
			tag:     ProtocolData,
			valid:   []int{12, 13, 16},
			invalid: []int{0, 1, 4, 8, 11},
		},
	}

	scalarTags := []struct {
		name string
		tag  uint16
	}{
		{name: "Traffic Mode Type", tag: TrafficModeType},
		{name: "Error Code", tag: ErrorCode},
		{name: "Status", tag: Status},
		{name: "ASP Identifier", tag: AspIdentifier},
		{name: "Correlation ID", tag: CorrelationID},
		{name: "Network Appearance", tag: NetworkAppearance},
		{name: "User Cause", tag: UserCause},
		{name: "Congestion Indications", tag: CongestionIndications},
		{name: "Concerned Destination", tag: ConcernedDestination},
		{name: "Local Routing Key Identifier", tag: LocalRoutingKeyIdentifier},
		{name: "Destination Point Code", tag: DestinationPointCode},
		{name: "Registration Status", tag: RegistrationStatus},
		{name: "Deregistration Status", tag: DeregistrationStatus},
	}
	for _, scalar := range scalarTags {
		tests = append(tests, struct {
			name    string
			tag     uint16
			valid   []int
			invalid []int
		}{
			name:    scalar.name,
			tag:     scalar.tag,
			valid:   []int{4},
			invalid: []int{0, 1, 2, 3, 5, 8},
		})
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, size := range test.valid {
				t.Run(fmt.Sprintf("valid-%d", size), func(t *testing.T) {
					data := make([]byte, size)
					switch test.tag {
					case TrafficModeType:
						binary.BigEndian.PutUint32(data, TrafficModeOverride)
					case ErrorCode:
						binary.BigEndian.PutUint32(data, InvalidVersionError)
					case Status:
						binary.BigEndian.PutUint32(data, AsStateInactive)
					}
					param := &Param{Tag: test.tag, Length: 0xffff, Data: data}
					wire, err := param.MarshalBinary()
					if err != nil {
						t.Fatalf("MarshalBinary() error = %v", err)
					}
					if _, err := Parse(wire); err != nil {
						t.Fatalf("Parse() error = %v", err)
					}
				})
			}

			for _, size := range test.invalid {
				t.Run(fmt.Sprintf("invalid-%d", size), func(t *testing.T) {
					param := &Param{Tag: test.tag, Data: make([]byte, size)}
					if _, err := param.MarshalBinary(); !errors.Is(err, ErrInvalidLength) {
						t.Errorf("MarshalBinary() error = %v, want ErrInvalidLength", err)
					}

					wire := rawParam(test.tag, size)
					if _, err := Parse(wire); !errors.Is(err, ErrInvalidLength) {
						t.Errorf("Parse() error = %v, want ErrInvalidLength", err)
					}
				})
			}
		})
	}
}

func TestUnknownParameterValueLengthRemainsExtensible(t *testing.T) {
	const extensionTag = 0xeffe

	for size := 0; size <= 9; size++ {
		t.Run(fmt.Sprintf("size-%d", size), func(t *testing.T) {
			param := &Param{Tag: extensionTag, Data: make([]byte, size)}
			wire, err := param.MarshalBinary()
			if err != nil {
				t.Fatalf("MarshalBinary() error = %v", err)
			}
			if _, err := Parse(wire); err != nil {
				t.Fatalf("Parse() error = %v", err)
			}
		})
	}
}

func TestParamMarshalToRejectsEveryShortBufferWithoutPanic(t *testing.T) {
	param := NewHeartbeatData([]byte{0xde, 0xad, 0xbe, 0xef, 0x01})

	for size := 0; size < param.MarshalLen(); size++ {
		t.Run(fmt.Sprintf("size-%d", size), func(t *testing.T) {
			buffer := bytes.Repeat([]byte{0xaa}, size)
			if err := param.MarshalTo(buffer); !errors.Is(err, ErrTooShortToMarshalBinary) {
				t.Fatalf("MarshalTo(%d-byte buffer) error = %v, want ErrTooShortToMarshalBinary", size, err)
			}
			if !bytes.Equal(buffer, bytes.Repeat([]byte{0xaa}, size)) {
				t.Fatalf("MarshalTo changed short buffer: % x", buffer)
			}
		})
	}
}

func TestParamMarshalUsesCurrentDataLengthAndZeroesPadding(t *testing.T) {
	param := NewHeartbeatData([]byte{0xde, 0xad, 0xbe, 0xef})
	param.Data = []byte{0xaa}
	param.Length = 0xffff
	buffer := bytes.Repeat([]byte{0xff}, param.MarshalLen()+4)

	if err := param.MarshalTo(buffer); err != nil {
		t.Fatalf("MarshalTo() error = %v", err)
	}
	if got, want := param.Length, uint16(5); got != want {
		t.Errorf("Param.Length = %d, want %d", got, want)
	}
	if got, want := binary.BigEndian.Uint16(buffer[2:4]), uint16(5); got != want {
		t.Errorf("encoded Parameter Length = %d, want %d", got, want)
	}
	if got, want := buffer[:param.MarshalLen()], []byte{0x00, 0x09, 0x00, 0x05, 0xaa, 0x00, 0x00, 0x00}; !bytes.Equal(got, want) {
		t.Errorf("encoded parameter = % x, want % x", got, want)
	}
	if got, want := buffer[param.MarshalLen():], []byte{0xff, 0xff, 0xff, 0xff}; !bytes.Equal(got, want) {
		t.Errorf("bytes after parameter = % x, want untouched % x", got, want)
	}
}

func TestParamMarshalZeroesEveryPaddingWidth(t *testing.T) {
	for valueLength := 1; valueLength <= 4; valueLength++ {
		t.Run(fmt.Sprintf("value-%d", valueLength), func(t *testing.T) {
			param := NewHeartbeatData(bytes.Repeat([]byte{0xaa}, valueLength))
			buffer := bytes.Repeat([]byte{0xff}, param.MarshalLen())
			if err := param.MarshalTo(buffer); err != nil {
				t.Fatalf("MarshalTo() error = %v", err)
			}

			paddingStart := 4 + valueLength
			if got, want := buffer[paddingStart:], make([]byte, param.Padding()); !bytes.Equal(got, want) {
				t.Errorf("padding = % x, want % x", got, want)
			}
		})
	}
}

func TestParamMarshalLengthBoundary(t *testing.T) {
	t.Run("largest representable value", func(t *testing.T) {
		param := NewParam(0xeffe, make([]byte, 65531))
		wire, err := param.MarshalBinary()
		if err != nil {
			t.Fatalf("MarshalBinary() error = %v", err)
		}
		if got, want := len(wire), 65536; got != want {
			t.Fatalf("wire length = %d, want %d", got, want)
		}
		if got, want := binary.BigEndian.Uint16(wire[2:4]), uint16(0xffff); got != want {
			t.Errorf("encoded Parameter Length = %#04x, want %#04x", got, want)
		}
		if got := wire[len(wire)-1]; got != 0 {
			t.Errorf("final padding octet = %#02x, want zero", got)
		}
		if _, err := Parse(wire); err != nil {
			t.Fatalf("Parse(maximum parameter) error = %v", err)
		}
	})

	t.Run("first unrepresentable value", func(t *testing.T) {
		param := NewParam(0xeffe, make([]byte, 65532))
		if got := param.Length; got != 0 {
			t.Errorf("Length after SetLength = %#04x, want zero invalid marker", got)
		}
		if _, err := param.MarshalBinary(); !errors.Is(err, ErrInvalidLength) {
			t.Fatalf("MarshalBinary() error = %v, want ErrInvalidLength", err)
		}
		buffer := make([]byte, param.MarshalLen())
		if err := param.MarshalTo(buffer); !errors.Is(err, ErrInvalidLength) {
			t.Fatalf("MarshalTo() error = %v, want ErrInvalidLength", err)
		}
	})
}

func TestParseMultiParamsFinalPaddingBoundary(t *testing.T) {
	withoutPadding := []byte{0x00, 0x09, 0x00, 0x05, 0xaa}
	withPadding := append(append([]byte(nil), withoutPadding...), 0x71, 0x72, 0x73)

	for _, test := range []struct {
		name string
		wire []byte
	}{
		{name: "padding omitted", wire: withoutPadding},
		{name: "full padding present and ignored", wire: withPadding},
	} {
		t.Run(test.name, func(t *testing.T) {
			params, err := ParseMultiParams(test.wire)
			if err != nil {
				t.Fatalf("ParseMultiParams() error = %v", err)
			}
			if len(params) != 1 || !bytes.Equal(params[0].Data, []byte{0xaa}) {
				t.Fatalf("ParseMultiParams() = %#v, want one Heartbeat Data parameter", params)
			}
		})
	}

	for extra := 1; extra < 3; extra++ {
		t.Run(fmt.Sprintf("partial-padding-%d", extra), func(t *testing.T) {
			wire := append(append([]byte(nil), withoutPadding...), make([]byte, extra)...)
			if _, err := ParseMultiParams(wire); !errors.Is(err, ErrInvalidLength) {
				t.Fatalf("ParseMultiParams() error = %v, want ErrInvalidLength", err)
			}
		})
	}
}

func rawParam(tag uint16, valueLength int) []byte {
	declaredLength := 4 + valueLength
	padding := (4 - declaredLength%4) % 4
	wire := make([]byte, declaredLength+padding)
	binary.BigEndian.PutUint16(wire[0:2], tag)
	binary.BigEndian.PutUint16(wire[2:4], uint16(declaredLength))
	return wire
}

func TestParseMultiParams(t *testing.T) {
	cases := []struct {
		name       string
		structured []*Param
		serialized []byte
	}{
		{
			"rc-generic",
			[]*Param{
				NewRoutingContext(1),
				NewParam(1, []byte{0xde, 0xad, 0xbe, 0xef}),
			},
			[]byte{
				// Routing Context
				0x00, 0x06, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01,
				// Something with String
				0x00, 0x01, 0x00, 0x08, 0xde, 0xad, 0xbe, 0xef,
			},
		},
	}

	for _, c := range cases {
		got, err := ParseMultiParams(c.serialized)
		if err != nil {
			t.Fatal(err)
		}
		if diff := cmp.Diff(got, c.structured); diff != "" {
			t.Error(diff)
		}
	}
}

func TestParseMalformed(t *testing.T) {
	cases := []struct {
		data []byte
		err  error
	}{
		{[]byte{0x00}, ErrTooShortToParse},
		{[]byte{0x00, 0x00}, ErrTooShortToParse},
		{[]byte{0x00, 0x00, 0x00}, ErrTooShortToParse},
		{[]byte{0x00, 0x00, 0x00, 0x00}, ErrInvalidLength},
	}

	for _, c := range cases {
		if _, err := Parse(c.data); err != c.err {
			t.Errorf("Parse/unexpected error: got: %v, want: %v", err, c.err)
		}
		if _, err := ParseMultiParams(c.data); err != c.err {
			t.Errorf("ParseMulti/unexpected error: got: %v, want: %v", err, c.err)
		}
	}
}

// Copy must produce a Param that shares no memory with the original: the
// New*() message constructors write to the Params they are given, so a shallow
// copy would leave concurrent senders mutating the same backing array.
func TestParamCopyIsDeep(t *testing.T) {
	orig := NewRoutingContext(1, 2)
	got := orig.Copy()

	if got == orig {
		t.Fatal("Copy() returned the same pointer")
	}
	if got.Tag != orig.Tag || got.Length != orig.Length {
		t.Errorf("Copy() = {Tag:%d Length:%d}, want {Tag:%d Length:%d}",
			got.Tag, got.Length, orig.Tag, orig.Length)
	}
	if len(got.Data) != len(orig.Data) {
		t.Fatalf("Copy() Data length = %d, want %d", len(got.Data), len(orig.Data))
	}
	if len(got.Data) > 0 && &got.Data[0] == &orig.Data[0] {
		t.Fatal("Copy() shares the Data backing array with the original")
	}

	// Mutating either side must not be visible in the other.
	got.Data[0] ^= 0xff
	got.Length = 0xbeef
	if orig.Data[0] == got.Data[0] {
		t.Error("writing to the copy's Data changed the original")
	}
	if orig.Length == 0xbeef {
		t.Error("writing to the copy's Length changed the original")
	}
}

// Copy on a nil Param must be safe: config Params are optional, so the send
// sites call Copy on fields that may legitimately be nil.
func TestParamCopyNil(t *testing.T) {
	var p *Param
	if got := p.Copy(); got != nil {
		t.Errorf("(*Param)(nil).Copy() = %v, want nil", got)
	}
}
