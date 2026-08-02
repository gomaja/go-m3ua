package messages

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
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
	if len(b) < 4 {
		return nil, ErrTooShortToParse
	}

	m := newMessageFor(b[2], b[3])
	if m == nil {
		return Parse(b)
	}

	header, err := parseTypedHeader(b, b[2], b[3])
	if err != nil {
		return nil, err
	}
	prs, err := parseParamsWithOptions(header.Payload, b, header.Class, header.Type, messageHasInfoString(m), options)
	if err != nil {
		return nil, err
	}
	if err := populateMessageFromParams(m, header, prs); err != nil {
		return nil, err
	}
	return m, nil
}

func parseParamsWithOptions(payload, raw []byte, class, msgType uint8, allowInfoString bool, options ParseOptions) ([]*params.Param, error) {
	var prms []*params.Param
	for len(payload) > 0 {
		if len(payload) < 4 {
			return nil, params.ErrTooShortToParse
		}
		tag := binary.BigEndian.Uint16(payload[0:2])
		parameterLength := int(binary.BigEndian.Uint16(payload[2:4]))
		if parameterLength < 4 || parameterLength > len(payload) {
			return nil, params.ErrInvalidLength
		}

		p, err := params.Parse(payload)
		if err != nil {
			var decision ProtocolDecision
			p, decision, err = tolerateParameterParseError(payload[:parameterLength], raw, class, msgType, tag, allowInfoString, options, err)
			if err != nil {
				return nil, err
			}
			if decision == ProtocolDropParameter {
				goto next
			}
		}
		prms = append(prms, p)

	next:
		if len(payload) == parameterLength {
			return prms, nil
		}
		paddedLength := parameterLength + (4-parameterLength%4)%4
		if len(payload) < paddedLength {
			return nil, params.ErrInvalidLength
		}
		payload = payload[paddedLength:]
	}
	return prms, nil
}

func tolerateParameterParseError(parameter, raw []byte, class, msgType uint8, tag uint16, allowInfoString bool, options ParseOptions, cause error) (*params.Param, ProtocolDecision, error) {
	if !allowInfoString || tag != params.InfoString || !errors.Is(cause, params.ErrInvalidValue) {
		return nil, ProtocolReject, cause
	}
	value := parameter[4:]
	if len(value) > 255 || utf8.Valid(value) {
		return nil, ProtocolReject, cause
	}

	violation := ProtocolViolation{
		Kind:         ViolationInvalidOptionalInfoString,
		ErrorCode:    params.ErrInvalidParameterValue,
		MessageClass: class,
		MessageType:  msgType,
		ParamTag:     tag,
		Description:  "optional INFO String is not valid UTF-8",
		RawMessage:   bytes.Clone(raw),
		RawParameter: bytes.Clone(parameter),
		Cause:        cause,
	}
	switch options.Tolerator.DecideProtocolViolation(violation) {
	case ProtocolAccept:
		return &params.Param{
			Tag:    params.InfoString,
			Length: uint16(len(parameter)),
			Data:   value,
		}, ProtocolAccept, nil
	case ProtocolDropParameter:
		return nil, ProtocolDropParameter, nil
	default:
		return nil, ProtocolReject, cause
	}
}

func messageHasInfoString(message M3UA) bool {
	value := reflect.ValueOf(message).Elem()
	return value.FieldByName("InfoString").IsValid()
}

func populateMessageFromParams(message M3UA, header *Header, prs []*params.Param) error {
	value := reflect.ValueOf(message)
	if value.Kind() != reflect.Pointer || value.IsNil() {
		return fmt.Errorf("invalid message receiver %T", message)
	}
	elem := value.Elem()
	elem.Set(reflect.Zero(elem.Type()))
	elem.FieldByName("Header").Set(reflect.ValueOf(header))

	for _, pr := range prs {
		fieldName, known := parameterFieldName(pr.Tag)
		if known {
			field := elem.FieldByName(fieldName)
			if field.IsValid() && field.Type() == paramType {
				if !field.IsNil() {
					return ErrInvalidParameter
				}
				field.Set(reflect.ValueOf(pr))
				continue
			}
		}
		others := elem.FieldByName("Others")
		if !others.IsValid() || others.Type() != paramSliceType {
			return ErrInvalidParameter
		}
		others.Set(reflect.Append(others, reflect.ValueOf(pr)))
	}

	return requireMandatoryParameters(message)
}

var (
	paramType      = reflect.TypeOf((*params.Param)(nil))
	paramSliceType = reflect.TypeOf([]*params.Param(nil))
)

func parameterFieldName(tag uint16) (string, bool) {
	switch tag {
	case params.NetworkAppearance:
		return "NetworkAppearance", true
	case params.RoutingContext:
		return "RoutingContext", true
	case params.ProtocolData:
		return "ProtocolData", true
	case params.CorrelationID:
		return "CorrelationID", true
	case params.AffectedPointCode:
		return "AffectedPointCode", true
	case params.ConcernedDestination:
		return "ConcernedDestination", true
	case params.CongestionIndications:
		return "CongestionIndications", true
	case params.UserCause:
		return "UserCause", true
	case params.AspIdentifier:
		return "AspIdentifier", true
	case params.InfoString:
		return "InfoString", true
	case params.TrafficModeType:
		return "TrafficModeType", true
	case params.ErrorCode:
		return "ErrorCode", true
	case params.DiagnosticInformation:
		return "DiagnosticInformation", true
	case params.HeartbeatData:
		return "HeartbeatData", true
	case params.Status:
		return "Status", true
	default:
		return "", false
	}
}

func requireMandatoryParameters(message M3UA) error {
	switch m := message.(type) {
	case *Data:
		return requireParameter(m.ProtocolData, "Protocol Data")
	case *DestinationUnavailable:
		return requireParameter(m.AffectedPointCode, "Affected Point Code")
	case *DestinationAvailable:
		return requireParameter(m.AffectedPointCode, "Affected Point Code")
	case *DestinationStateAudit:
		return requireParameter(m.AffectedPointCode, "Affected Point Code")
	case *SignallingCongestion:
		return requireParameter(m.AffectedPointCode, "Affected Point Code")
	case *DestinationUserPartUnavailable:
		if err := requireParameter(m.AffectedPointCode, "Affected Point Code"); err != nil {
			return err
		}
		return requireParameter(m.UserCause, "User/Cause")
	case *DestinationRestricted:
		return requireParameter(m.AffectedPointCode, "Affected Point Code")
	case *Error:
		return requireParameter(m.ErrorCode, "Error Code")
	case *Notify:
		return requireParameter(m.Status, "Status")
	default:
		return nil
	}
}
