// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package messages

import (
	"fmt"
	"log"

	"github.com/gomaja/go-m3ua/messages/params"
)

// Error is a Error type of M3UA message.
//
// Spec: 3.8.1, RFC4666.
type Error struct {
	*Header
	ErrorCode             *params.Param
	RoutingContext        *params.Param
	NetworkAppearance     *params.Param
	AffectedPointCode     *params.Param
	DiagnosticInformation *params.Param
	// Others holds parameters this version does not define.
	//
	// RFC 4666 Section 3: "For forward compatibility, all Message Types may
	// have attached parameters even if none are specified in this version."
	// They are kept rather than rejected so a peer running a later extension
	// still interoperates, and so a message that must be echoed back can be
	// echoed whole.
	Others []*params.Param
}

// NewError creates a new Error.
func NewError(code, rtCtx, nwApr, apc, info *params.Param) *Error {
	e := &Error{
		Header: &Header{
			Version:  1,
			Reserved: 0,
			Class:    MsgClassManagement,
			Type:     MsgTypeError,
		},
		ErrorCode:             code,
		RoutingContext:        rtCtx,
		NetworkAppearance:     nwApr,
		AffectedPointCode:     apc,
		DiagnosticInformation: info,
	}
	e.SetLength()

	return e
}

// MarshalBinary returns the byte sequence generated from a Error.
func (e *Error) MarshalBinary() ([]byte, error) {
	b := make([]byte, e.MarshalLen())
	if err := e.MarshalTo(b); err != nil {
		return nil, err
	}
	return b, nil
}

// MarshalTo puts the byte sequence in the byte array given as b.
func (e *Error) MarshalTo(b []byte) error {
	if err := requireParameter(e.ErrorCode, "Error Code"); err != nil {
		return err
	}
	if len(b) < e.MarshalLen() {
		return ErrTooShortToMarshalBinary
	}

	e.Header.Payload = make([]byte, e.MarshalLen()-8)

	var offset = 0
	if param := e.ErrorCode; param != nil {
		if err := param.MarshalTo(e.Header.Payload[offset:]); err != nil {
			return err
		}
		offset += param.MarshalLen()
	}

	if param := e.RoutingContext; param != nil {
		if err := param.MarshalTo(e.Header.Payload[offset:]); err != nil {
			return err
		}
		offset += param.MarshalLen()
	}

	if param := e.NetworkAppearance; param != nil {
		if err := param.MarshalTo(e.Header.Payload[offset:]); err != nil {
			return err
		}
		offset += param.MarshalLen()
	}

	if param := e.AffectedPointCode; param != nil {
		if err := param.MarshalTo(e.Header.Payload[offset:]); err != nil {
			return err
		}
		offset += param.MarshalLen()
	}

	if param := e.DiagnosticInformation; param != nil {
		if err := param.MarshalTo(e.Header.Payload[offset:]); err != nil {
			return err
		}
		offset += param.MarshalLen()
	}
	if err := marshalOtherParams(e.Header.Payload, offset, e.Others); err != nil {
		return err
	}

	return e.Header.MarshalTo(b)
}

// ParseError decodes given byte sequence as a Error.
func ParseError(b []byte) (*Error, error) {
	e := &Error{}
	if err := e.UnmarshalBinary(b); err != nil {
		return nil, err
	}
	return e, nil
}

// UnmarshalBinary sets the values retrieved from byte sequence in a M3UA common header.
func (e *Error) UnmarshalBinary(b []byte) error {
	*e = Error{}
	var err error
	e.Header, err = parseTypedHeader(b, MsgClassManagement, MsgTypeError)
	if err != nil {
		return err
	}

	prs, err := params.ParseMultiParams(e.Header.Payload)
	if err != nil {
		return err
	}
	for _, pr := range prs {
		switch pr.Tag {
		case params.ErrorCode:
			if e.ErrorCode != nil {
				return ErrInvalidParameter
			}
			e.ErrorCode = pr
		case params.RoutingContext:
			if e.RoutingContext != nil {
				return ErrInvalidParameter
			}
			e.RoutingContext = pr
		case params.NetworkAppearance:
			if e.NetworkAppearance != nil {
				return ErrInvalidParameter
			}
			e.NetworkAppearance = pr
		case params.AffectedPointCode:
			if e.AffectedPointCode != nil {
				return ErrInvalidParameter
			}
			e.AffectedPointCode = pr
		case params.DiagnosticInformation:
			if e.DiagnosticInformation != nil {
				return ErrInvalidParameter
			}
			e.DiagnosticInformation = pr
		default:
			e.Others = append(e.Others, pr)
		}
	}
	return requireParameter(e.ErrorCode, "Error Code")
}

// SetLength sets the length in Length field.
func (e *Error) SetLength() {
	if param := e.ErrorCode; param != nil {
		param.SetLength()
	}
	if param := e.RoutingContext; param != nil {
		param.SetLength()
	}
	if param := e.NetworkAppearance; param != nil {
		param.SetLength()
	}
	if param := e.AffectedPointCode; param != nil {
		param.SetLength()
	}
	if param := e.DiagnosticInformation; param != nil {
		param.SetLength()
	}
	setParamLengths(e.Others)

	e.Header.Length = uint32(e.MarshalLen())
}

// MarshalLen returns the serial length of Error.
func (e *Error) MarshalLen() int {
	l := 8
	if param := e.ErrorCode; param != nil {
		l += param.MarshalLen()
	}
	if param := e.RoutingContext; param != nil {
		l += param.MarshalLen()
	}
	if param := e.NetworkAppearance; param != nil {
		l += param.MarshalLen()
	}
	if param := e.AffectedPointCode; param != nil {
		l += param.MarshalLen()
	}
	if param := e.DiagnosticInformation; param != nil {
		l += param.MarshalLen()
	}
	l += paramsMarshalLen(e.Others)
	return l
}

// String returns the Error values in human readable format.
func (e *Error) String() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("{Header: %s, ErrorCode: %s, RoutingContext: %s, NetworkAppearance: %s, AffectedPointCode: %s, DiagnosticInformation: %s}",
		e.Header.String(),
		e.ErrorCode.String(),
		e.RoutingContext.String(),
		e.NetworkAppearance.String(),
		e.AffectedPointCode.String(),
		e.DiagnosticInformation.String(),
	)
}

// Version returns the version of M3UA in int.
func (e *Error) Version() uint8 {
	return e.Header.Version
}

// MessageType returns the message type in int.
func (e *Error) MessageType() uint8 {
	return MsgTypeError
}

// MessageClass returns the message class in int.
func (e *Error) MessageClass() uint8 {
	return MsgClassManagement
}

// MessageClassName returns the name of message class.
func (e *Error) MessageClassName() string {
	return MsgClassNameManagement
}

// MessageTypeName returns the name of message type.
func (e *Error) MessageTypeName() string {
	return "Error"
}

// Serialize returns the byte sequence generated from a Error.
//
// DEPRECATED: use MarshalBinary instead.
func (e *Error) Serialize() ([]byte, error) {
	log.Println("DEPRECATED: MarshalBinary instead")
	return e.MarshalBinary()
}

// SerializeTo puts the byte sequence in the byte array given as b.
//
// DEPRECATED: use MarshalTo instead.
func (e *Error) SerializeTo(b []byte) error {
	log.Println("DEPRECATED: MarshalTo instead")
	return e.MarshalTo(b)
}

// DecodeError decodes given byte sequence as a Error.
//
// DEPRECATED: use ParseError instead.
func DecodeError(b []byte) (*Error, error) {
	log.Println("DEPRECATED: use ParseError instead")
	return ParseError(b)
}

// DecodeFromBytes sets the values retrieved from byte sequence in a M3UA common header.
//
// DEPRECATED: use UnmarshalBinary instead.
func (e *Error) DecodeFromBytes(b []byte) error {
	log.Println("DEPRECATED: use UnmarshalBinary instead")
	return e.UnmarshalBinary(b)
}

// Len returns the serial length of Error.
//
// DEPRECATED: use MarshalLen instead.
func (e *Error) Len() int {
	log.Println("DEPRECATED: use MarshalLen instead")
	return e.MarshalLen()
}
