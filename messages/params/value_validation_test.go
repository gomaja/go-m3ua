package params

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
)

func TestDefinedParameterValueDomains(t *testing.T) {
	tests := []struct {
		name    string
		tag     uint16
		valid   []uint32
		invalid []uint32
	}{
		{
			name:    "Traffic Mode Type",
			tag:     TrafficModeType,
			valid:   []uint32{TrafficModeOverride, TrafficModeLoadshare, TrafficModeBroadcast},
			invalid: []uint32{0, 4, 0xffffffff},
		},
		{
			name: "Error Code",
			tag:  ErrorCode,
			valid: []uint32{
				InvalidVersionError,
				UnsupportedMessageErrorClass,
				UnsupportedMessageErrorType,
				ErrUnsupportedTrafficModeType,
				UnexpectedMessageError,
				ErrProtocolError,
				ErrInvalidStreamIdentifier,
				ErrRefusedManagementBlocking,
				ErrAspIdentifierRequired,
				ErrInvalidAspIdentifier,
				ErrInvalidParameterValue,
				ErrParameterFieldError,
				ErrUnexpectedParameter,
				ErrDestinationStatusUnknown,
				ErrInvalidNetworkAppearance,
				ErrMissingParameter,
				ErrInvalidRoutingContext,
				ErrNoConfiguredAsForAsp,
			},
			invalid: []uint32{0, 2, 8, 10, 11, 12, 16, 23, 24, 27, 0xffffffff},
		},
		{
			name: "Status",
			tag:  Status,
			valid: []uint32{
				AsStateInactive,
				AsStateActive,
				AsStatePending,
				InsufficientAspResources,
				AlternateAspActive,
				AspFailure,
			},
			invalid: []uint32{
				0,
				uint32(AsStateChange) << 16,
				uint32(AsStateChange)<<16 | 1,
				uint32(AsStateChange)<<16 | 5,
				uint32(Other) << 16,
				uint32(Other)<<16 | 4,
				3<<16 | 1,
				0xffffffff,
			},
		},
		{
			name:    "Congestion Indications",
			tag:     CongestionIndications,
			valid:   []uint32{0, 1, 2, 3, 0xffffff03},
			invalid: []uint32{4, 0xffffff04, 0xffffffff},
		},
		{
			name:    "Registration Status",
			tag:     RegistrationStatus,
			valid:   uint32Range(0, RoutingKeyAlreadyRegistered),
			invalid: []uint32{RoutingKeyAlreadyRegistered + 1, 0xffffffff},
		},
		{
			name:    "Deregistration Status",
			tag:     DeregistrationStatus,
			valid:   uint32Range(0, DeregASPActiveForRoutingContext),
			invalid: []uint32{DeregASPActiveForRoutingContext + 1, 0xffffffff},
		},
		{
			name:    "Destination Point Code",
			tag:     DestinationPointCode,
			valid:   []uint32{0, 0x00123456, 0x00ffffff},
			invalid: []uint32{0x01000000, 0xff123456},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for _, value := range test.valid {
				t.Run(fmt.Sprintf("valid-%08x", value), func(t *testing.T) {
					assertParameterValueAccepted(t, test.tag, uint32Bytes(value))
				})
			}
			for _, value := range test.invalid {
				t.Run(fmt.Sprintf("invalid-%08x", value), func(t *testing.T) {
					assertParameterValueRejected(t, test.tag, uint32Bytes(value))
				})
			}
		})
	}
}

func TestDefinedParameterConstructorsCannotMarshalInvalidValues(t *testing.T) {
	tests := []struct {
		name  string
		param *Param
	}{
		{name: "Traffic Mode Type", param: NewTrafficModeType(0)},
		{name: "Error Code", param: NewErrorCode(0)},
		{name: "Status", param: NewStatus(uint32(AsStateChange)<<16 | 1)},
		{name: "Congestion Indications", param: NewCongestionIndications(4)},
		{name: "Registration Status", param: NewRegistrationStatus(RoutingKeyAlreadyRegistered + 1)},
		{name: "Deregistration Status", param: NewDeregistrationStatus(DeregASPActiveForRoutingContext + 1)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := test.param.MarshalBinary(); !errors.Is(err, ErrInvalidValue) {
				t.Fatalf("MarshalBinary() error = %v, want ErrInvalidValue", err)
			}
			if err := test.param.MarshalTo(make([]byte, test.param.MarshalLen())); !errors.Is(err, ErrInvalidValue) {
				t.Fatalf("MarshalTo() error = %v, want ErrInvalidValue", err)
			}
		})
	}
}

func TestReservedAndVariantSpecificFieldsRemainExtensible(t *testing.T) {
	tests := []struct {
		name  string
		tag   uint16
		value []byte
	}{
		{
			name:  "Concerned Destination reserved octet is ignored",
			tag:   ConcernedDestination,
			value: uint32Bytes(0xff123456),
		},
		{
			name:  "Congestion reserved octets are ignored",
			tag:   CongestionIndications,
			value: uint32Bytes(0xffffff03),
		},
		{
			name:  "Affected Point Code mask remains variant-dependent",
			tag:   AffectedPointCode,
			value: uint32Bytes(0xff123456),
		},
		{
			name:  "Originating Point Code mask remains variant-dependent",
			tag:   OriginatingPointCodeList,
			value: uint32Bytes(0xff123456),
		},
		{
			name:  "User Cause remains variant-dependent",
			tag:   UserCause,
			value: uint32Bytes(0xffffffff),
		},
		{
			name:  "Service Indicator remains variant-dependent",
			tag:   ServiceIndicators,
			value: []byte{0xff},
		},
		{
			name: "Protocol Data bit widths remain Network Appearance-dependent",
			tag:  ProtocolData,
			value: []byte{
				0xff, 0xff, 0xff, 0xff,
				0xff, 0xff, 0xff, 0xff,
				0xff, 0xff, 0xff, 0xff,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assertParameterValueAccepted(t, test.tag, test.value)
		})
	}
}

func TestPointCodeAndReservedFieldConstructorsAreCanonical(t *testing.T) {
	tests := []struct {
		name  string
		param *Param
		want  []byte
	}{
		{
			name:  "Destination Point Code mask",
			param: NewDestinationPointCode(0xff123456),
			want:  uint32Bytes(0x00123456),
		},
		{
			name:  "Concerned Destination reserved octet",
			param: NewConcernedDestination(0xff123456),
			want:  uint32Bytes(0x00123456),
		},
		{
			name:  "Congestion Indications reserved octets",
			param: NewCongestionIndications(3),
			want:  uint32Bytes(3),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if !bytes.Equal(test.param.Data, test.want) {
				t.Fatalf("Data = % x, want % x", test.param.Data, test.want)
			}
			if _, err := test.param.MarshalBinary(); err != nil {
				t.Fatalf("MarshalBinary() error = %v", err)
			}
		})
	}
}

func TestUnknownParameterValueDomainRemainsExtensible(t *testing.T) {
	assertParameterValueAccepted(t, 0xeffe, []byte{0xff, 0xff, 0xff, 0xff})
}

func FuzzDefinedParameterValueValidation(f *testing.F) {
	for _, seed := range []struct {
		domain uint8
		value  uint32
	}{
		{domain: 0, value: TrafficModeOverride},
		{domain: 0, value: 0},
		{domain: 1, value: ErrProtocolError},
		{domain: 1, value: 2},
		{domain: 2, value: AsStateActive},
		{domain: 2, value: uint32(AsStateChange)<<16 | 1},
		{domain: 3, value: 0xffffff03},
		{domain: 3, value: 4},
		{domain: 4, value: RoutingKeyAlreadyRegistered},
		{domain: 4, value: RoutingKeyAlreadyRegistered + 1},
		{domain: 5, value: DeregASPActiveForRoutingContext},
		{domain: 5, value: DeregASPActiveForRoutingContext + 1},
		{domain: 6, value: 0x00ffffff},
		{domain: 6, value: 0x01000000},
	} {
		f.Add(seed.domain, seed.value)
	}

	f.Fuzz(func(t *testing.T, domain uint8, value uint32) {
		tag, valid := parameterDomainExpectation(domain%7, value)
		param := &Param{Tag: tag, Data: uint32Bytes(value)}
		wire, marshalErr := param.MarshalBinary()
		_, parseErr := Parse(rawValueParam(tag, uint32Bytes(value)))

		if valid {
			if marshalErr != nil {
				t.Fatalf("valid value MarshalBinary() error = %v", marshalErr)
			}
			if parseErr != nil {
				t.Fatalf("valid value Parse() error = %v", parseErr)
			}
			decoded, err := Parse(wire)
			if err != nil {
				t.Fatalf("roundtrip Parse() error = %v", err)
			}
			if !bytes.Equal(decoded.Data, uint32Bytes(value)) {
				t.Fatalf("roundtrip Data = % x, want %08x", decoded.Data, value)
			}
			return
		}

		if !errors.Is(marshalErr, ErrInvalidValue) {
			t.Fatalf("invalid value MarshalBinary() error = %v, want ErrInvalidValue", marshalErr)
		}
		if !errors.Is(parseErr, ErrInvalidValue) {
			t.Fatalf("invalid value Parse() error = %v, want ErrInvalidValue", parseErr)
		}
	})
}

func assertParameterValueAccepted(t *testing.T, tag uint16, value []byte) {
	t.Helper()

	param := &Param{Tag: tag, Length: 0xffff, Data: bytes.Clone(value)}
	wire, err := param.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	decoded, err := Parse(wire)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	if !bytes.Equal(decoded.Data, value) {
		t.Fatalf("decoded Data = % x, want % x", decoded.Data, value)
	}
}

func assertParameterValueRejected(t *testing.T, tag uint16, value []byte) {
	t.Helper()

	param := &Param{Tag: tag, Data: bytes.Clone(value)}
	if _, err := param.MarshalBinary(); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("MarshalBinary() error = %v, want ErrInvalidValue", err)
	}
	if err := param.MarshalTo(make([]byte, param.MarshalLen())); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("MarshalTo() error = %v, want ErrInvalidValue", err)
	}

	wire := rawValueParam(tag, value)
	if _, err := Parse(wire); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("Parse() error = %v, want ErrInvalidValue", err)
	}

	receiver := &Param{Tag: InfoString, Length: 9, Data: []byte("stale")}
	if err := receiver.UnmarshalBinary(wire); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("reused UnmarshalBinary() error = %v, want ErrInvalidValue", err)
	}
	if receiver.Tag != 0 || receiver.Length != 0 || receiver.Data != nil {
		t.Fatalf("failed decode retained receiver state: %+v", receiver)
	}
}

func uint32Range(first, last uint32) []uint32 {
	values := make([]uint32, 0, last-first+1)
	for value := first; value <= last; value++ {
		values = append(values, value)
	}
	return values
}

func uint32Bytes(value uint32) []byte {
	data := make([]byte, 4)
	binary.BigEndian.PutUint32(data, value)
	return data
}

func rawValueParam(tag uint16, value []byte) []byte {
	parameterLength := 4 + len(value)
	marshalLength := parameterLength + (4-parameterLength%4)%4
	wire := make([]byte, marshalLength)
	binary.BigEndian.PutUint16(wire[0:2], tag)
	binary.BigEndian.PutUint16(wire[2:4], uint16(parameterLength))
	copy(wire[4:], value)
	return wire
}

func parameterDomainExpectation(domain uint8, value uint32) (uint16, bool) {
	switch domain {
	case 0:
		return TrafficModeType, value >= TrafficModeOverride && value <= TrafficModeBroadcast
	case 1:
		switch value {
		case InvalidVersionError,
			UnsupportedMessageErrorClass,
			UnsupportedMessageErrorType,
			ErrUnsupportedTrafficModeType,
			UnexpectedMessageError,
			ErrProtocolError,
			ErrInvalidStreamIdentifier,
			ErrRefusedManagementBlocking,
			ErrAspIdentifierRequired,
			ErrInvalidAspIdentifier,
			ErrInvalidParameterValue,
			ErrParameterFieldError,
			ErrUnexpectedParameter,
			ErrDestinationStatusUnknown,
			ErrInvalidNetworkAppearance,
			ErrMissingParameter,
			ErrInvalidRoutingContext,
			ErrNoConfiguredAsForAsp:
			return ErrorCode, true
		default:
			return ErrorCode, false
		}
	case 2:
		switch value {
		case AsStateInactive,
			AsStateActive,
			AsStatePending,
			InsufficientAspResources,
			AlternateAspActive,
			AspFailure:
			return Status, true
		default:
			return Status, false
		}
	case 3:
		return CongestionIndications, uint8(value) <= 3
	case 4:
		return RegistrationStatus, value <= RoutingKeyAlreadyRegistered
	case 5:
		return DeregistrationStatus, value <= DeregASPActiveForRoutingContext
	default:
		return DestinationPointCode, uint8(value>>24) == 0
	}
}
