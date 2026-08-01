// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package messages

import (
	"fmt"
	"log"

	"github.com/gomaja/go-m3ua/messages/params"
)

// AspDown is a AspDown type of M3UA message.
//
// Spec: 3.5.3, RFC4666.
type AspDown struct {
	*Header
	AspIdentifier *params.Param
	InfoString    *params.Param
	// Others holds parameters this version does not define.
	//
	// RFC 4666 Section 3: "For forward compatibility, all Message Types may
	// have attached parameters even if none are specified in this version."
	// They are kept rather than rejected so a peer running a later extension
	// still interoperates, and so a message that must be echoed back can be
	// echoed whole.
	Others []*params.Param
}

// NewAspDown creates a new AspDown.
func NewAspDown(info *params.Param) *AspDown {
	a := &AspDown{
		Header: &Header{
			Version:  1,
			Reserved: 0,
			Class:    MsgClassASPSM,
			Type:     MsgTypeAspDown,
		},
		InfoString: info,
	}
	a.SetLength()

	return a
}

// MarshalBinary returns the byte sequence generated from a AspDown.
func (a *AspDown) MarshalBinary() ([]byte, error) {
	b := make([]byte, a.MarshalLen())
	if err := a.MarshalTo(b); err != nil {
		return nil, err
	}
	return b, nil
}

// MarshalTo puts the byte sequence in the byte array given as b.
func (a *AspDown) MarshalTo(b []byte) error {
	if len(b) < a.MarshalLen() {
		return ErrTooShortToMarshalBinary
	}

	a.Header.Payload = make([]byte, a.MarshalLen()-8)

	var offset int
	if param := a.InfoString; param != nil {
		if err := param.MarshalTo(a.Header.Payload[offset:]); err != nil {
			return err
		}
		offset += param.MarshalLen()
	}
	if err := marshalOtherParams(a.Header.Payload, offset, a.Others); err != nil {
		return err
	}

	return a.Header.MarshalTo(b)
}

// ParseAspDown decodes given byte sequence as a AspDown.
func ParseAspDown(b []byte) (*AspDown, error) {
	a := &AspDown{}
	if err := a.UnmarshalBinary(b); err != nil {
		return nil, err
	}
	return a, nil
}

// UnmarshalBinary sets the values retrieved from byte sequence in a M3UA common header.
func (a *AspDown) UnmarshalBinary(b []byte) error {
	*a = AspDown{}
	var err error
	a.Header, err = parseTypedHeader(b, MsgClassASPSM, MsgTypeAspDown)
	if err != nil {
		return err
	}

	prs, err := params.ParseMultiParams(a.Header.Payload)
	if err != nil {
		return err
	}
	for _, pr := range prs {
		switch pr.Tag {
		case params.InfoString:
			if a.InfoString != nil {
				return ErrInvalidParameter
			}
			a.InfoString = pr
		default:
			a.Others = append(a.Others, pr)
		}
	}
	return nil
}

// SetLength sets the length in Length field.
func (a *AspDown) SetLength() {
	if param := a.InfoString; param != nil {
		param.SetLength()
	}
	setParamLengths(a.Others)

	a.Header.Length = uint32(a.MarshalLen())
}

// MarshalLen returns the serial length of AspDown.
func (a *AspDown) MarshalLen() int {
	l := 8
	if param := a.InfoString; param != nil {
		l += param.MarshalLen()
	}
	l += paramsMarshalLen(a.Others)
	return l
}

// String returns the AspDown values in human readable format.
func (a *AspDown) String() string {
	if a == nil {
		return ""
	}
	return fmt.Sprintf("{Header: %s, InfoString: %s}",
		a.Header.String(),
		a.InfoString.String(),
	)
}

// Version returns the version of M3UA in int.
func (a *AspDown) Version() uint8 {
	return a.Header.Version
}

// MessageType returns the message type in int.
func (a *AspDown) MessageType() uint8 {
	return MsgTypeAspDown
}

// MessageClass returns the message class in int.
func (a *AspDown) MessageClass() uint8 {
	return MsgClassASPSM
}

// MessageClassName returns the name of message class.
func (a *AspDown) MessageClassName() string {
	return MsgClassNameASPSM
}

// MessageTypeName returns the name of message type.
func (a *AspDown) MessageTypeName() string {
	return "ASP Down"
}

// Serialize returns the byte sequence generated from a AspDown.
//
// DEPRECATED: use MarshalBinary instead.
func (a *AspDown) Serialize() ([]byte, error) {
	log.Println("DEPRECATED: MarshalBinary instead")
	return a.MarshalBinary()
}

// SerializeTo puts the byte sequence in the byte array given as b.
//
// DEPRECATED: use MarshalTo instead.
func (a *AspDown) SerializeTo(b []byte) error {
	log.Println("DEPRECATED: MarshalTo instead")
	return a.MarshalTo(b)
}

// DecodeAspDown decodes given byte sequence as a AspDown.
//
// DEPRECATED: use ParseAspDown instead.
func DecodeAspDown(b []byte) (*AspDown, error) {
	log.Println("DEPRECATED: use ParseAspDown instead")
	return ParseAspDown(b)
}

// DecodeFromBytes sets the values retrieved from byte sequence in a M3UA common header.
//
// DEPRECATED: use UnmarshalBinary instead.
func (a *AspDown) DecodeFromBytes(b []byte) error {
	log.Println("DEPRECATED: use UnmarshalBinary instead")
	return a.UnmarshalBinary(b)
}

// Len returns the serial length of AspDown.
//
// DEPRECATED: use MarshalLen instead.
func (a *AspDown) Len() int {
	log.Println("DEPRECATED: use MarshalLen instead")
	return a.MarshalLen()
}
