package messages

import (
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
	"unicode/utf8"

	"github.com/gomaja/go-m3ua/messages/params"
)

// ProtocolDecision is the action a compatibility tolerator chooses for a
// classified protocol violation.
type ProtocolDecision uint8

const (
	// ProtocolReject keeps the RFC-strict behaviour.
	ProtocolReject ProtocolDecision = iota
	// ProtocolAccept accepts the violation and keeps the offending parameter.
	ProtocolAccept
	// ProtocolDropParameter accepts the message but discards the offending
	// optional parameter.
	ProtocolDropParameter
	// ProtocolUseLocalDefault is reserved for future classified violations whose
	// safe action is to infer a configured local value.
	ProtocolUseLocalDefault
)

// ProtocolViolationKind identifies a protocol violation known well enough for a
// caller to make an explicit compatibility decision.
type ProtocolViolationKind uint16

const (
	// ViolationInvalidOptionalInfoString is an optional INFO String whose value is
	// no more than 255 octets but is not valid UTF-8.
	ViolationInvalidOptionalInfoString ProtocolViolationKind = iota + 1
)

// ProtocolViolation describes one classified receive-side protocol violation.
type ProtocolViolation struct {
	Kind         ProtocolViolationKind
	ErrorCode    uint32
	MessageClass uint8
	MessageType  uint8
	ParamTag     uint16
	Description  string
	RawMessage   []byte
	RawParameter []byte
	Cause        error
}

// ProtocolTolerator decides whether a classified protocol violation should be
// tolerated. It is called only after structural lengths are safe.
type ProtocolTolerator interface {
	DecideProtocolViolation(ProtocolViolation) ProtocolDecision
}

// ToleratorFunc adapts a function into a ProtocolTolerator.
type ToleratorFunc func(ProtocolViolation) ProtocolDecision

// DecideProtocolViolation calls f(v).
func (f ToleratorFunc) DecideProtocolViolation(v ProtocolViolation) ProtocolDecision {
	return f(v)
}

// ParseOptions configures receive-side parsing.
type ParseOptions struct {
	Tolerator ProtocolTolerator
}

// ParseWithOptions decodes the given bytes with explicit receive-side
// compatibility policy. With zero options it is identical to Parse.
func ParseWithOptions(b []byte, options ParseOptions) (M3UA, error) {
	if options.Tolerator == nil {
		return Parse(b)
	}

	strict, strictErr := Parse(b)
	if strictErr == nil {
		return strict, nil
	}

	if len(b) < 4 {
		return nil, strictErr
	}
	m := newMessageFor(b[2], b[3])
	if m == nil {
		return nil, strictErr
	}

	header, err := parseTypedHeader(b, b[2], b[3])
	if err != nil {
		return nil, strictErr
	}

	fault, err := findTolerableParameterFault(
		header.Payload,
		b,
		header.Class,
		header.Type,
		messageHasInfoString(m),
	)
	if err != nil {
		return nil, strictErr
	}
	if fault == nil {
		return nil, strictErr
	}

	switch options.Tolerator.DecideProtocolViolation(fault.violation) {
	case ProtocolAccept:
		msg, err := Parse(fault.acceptWire(b))
		if err != nil {
			return nil, err
		}
		if err := restoreInvalidInfoString(msg, fault.value); err != nil {
			return nil, err
		}
		return msg, nil
	case ProtocolDropParameter:
		return Parse(fault.dropWire(b, header))
	default:
		return nil, fault.violation.Cause
	}
}

func messageHasInfoString(message M3UA) bool {
	value := reflect.ValueOf(message).Elem()
	return value.FieldByName("InfoString").IsValid()
}

type tolerableParameterFault struct {
	offset       int
	consumedSize int
	value        []byte
	violation    ProtocolViolation
}

func findTolerableParameterFault(payload, raw []byte, class, msgType uint8, allowInfoString bool) (*tolerableParameterFault, error) {
	for offset := 0; offset < len(payload); {
		remaining := payload[offset:]
		if len(remaining) < 4 {
			return nil, params.ErrTooShortToParse
		}

		tag := binary.BigEndian.Uint16(remaining[0:2])
		parameterSize := int(binary.BigEndian.Uint16(remaining[2:4]))
		if parameterSize < 4 || parameterSize > len(remaining) {
			return nil, params.ErrInvalidLength
		}
		consumed := consumedParameterSize(len(remaining), parameterSize)
		if consumed > len(remaining) {
			return nil, params.ErrInvalidLength
		}

		parameter := remaining[:parameterSize]
		if _, err := params.Parse(remaining); err != nil {
			return classifyInvalidOptionalInfoString(
				parameter,
				raw,
				class,
				msgType,
				tag,
				allowInfoString,
				offset,
				consumed,
				err,
			)
		}

		offset += consumed
	}
	return nil, nil
}

func consumedParameterSize(remainingLength, parameterSize int) int {
	if remainingLength == parameterSize {
		return parameterSize
	}
	return parameterSize + (4-parameterSize%4)%4
}

func classifyInvalidOptionalInfoString(
	parameter, raw []byte,
	class, msgType uint8,
	tag uint16,
	allowInfoString bool,
	offset, consumedSize int,
	cause error,
) (*tolerableParameterFault, error) {
	if !allowInfoString || tag != params.InfoString || !errors.Is(cause, params.ErrInvalidValue) {
		return nil, cause
	}
	value := parameter[4:]
	if len(value) > 255 || utf8.Valid(value) {
		return nil, cause
	}

	return &tolerableParameterFault{
		offset:       offset,
		consumedSize: consumedSize,
		value:        bytes.Clone(value),
		violation: ProtocolViolation{
			Kind:         ViolationInvalidOptionalInfoString,
			ErrorCode:    params.ErrInvalidParameterValue,
			MessageClass: class,
			MessageType:  msgType,
			ParamTag:     tag,
			Description:  "optional INFO String is not valid UTF-8",
			RawMessage:   bytes.Clone(raw),
			RawParameter: bytes.Clone(parameter),
			Cause:        cause,
		},
	}, nil
}

func (f *tolerableParameterFault) acceptWire(raw []byte) []byte {
	wire := bytes.Clone(raw)
	valueStart := 8 + f.offset + 4
	copy(wire[valueStart:valueStart+len(f.value)], bytes.Repeat([]byte{'x'}, len(f.value)))
	return wire
}

func (f *tolerableParameterFault) dropWire(raw []byte, header *Header) []byte {
	payload := header.Payload
	outPayload := make([]byte, 0, len(payload)-f.consumedSize)
	outPayload = append(outPayload, payload[:f.offset]...)
	outPayload = append(outPayload, payload[f.offset+f.consumedSize:]...)

	out := make([]byte, 8+len(outPayload))
	copy(out[:4], raw[:4])
	binary.BigEndian.PutUint32(out[4:8], uint32(len(out)))
	copy(out[8:], outPayload)
	return out
}

func restoreInvalidInfoString(message M3UA, value []byte) error {
	field := reflect.ValueOf(message).Elem().FieldByName("InfoString")
	if !field.IsValid() || field.IsNil() {
		return ErrInvalidParameter
	}

	infoString := field.Interface().(*params.Param)
	infoString.Data = bytes.Clone(value)
	infoString.Length = uint16(4 + len(value))
	return nil
}
