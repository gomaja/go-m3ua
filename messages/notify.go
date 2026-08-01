// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package messages

import (
	"fmt"
	"log"

	"github.com/gomaja/go-m3ua/messages/params"
)

// Notify is a Notify type of M3UA message.
//
// Spec: 3.8.2, RFC4666.
type Notify struct {
	*Header
	Status         *params.Param
	AspIdentifier  *params.Param
	RoutingContext *params.Param
	InfoString     *params.Param
	// Others holds parameters this version does not define.
	//
	// RFC 4666 Section 3: "For forward compatibility, all Message Types may
	// have attached parameters even if none are specified in this version."
	// They are kept rather than rejected so a peer running a later extension
	// still interoperates, and so a message that must be echoed back can be
	// echoed whole.
	Others []*params.Param
}

// NewNotify creates a new Notify.
func NewNotify(status, aspID, rtCtx, info *params.Param) *Notify {
	n := &Notify{
		Header: &Header{
			Version:  1,
			Reserved: 0,
			Class:    MsgClassManagement,
			Type:     MsgTypeNotify,
		},
		Status:         status,
		AspIdentifier:  aspID,
		RoutingContext: rtCtx,
		InfoString:     info,
	}
	n.SetLength()

	return n
}

// MarshalBinary returns the byte sequence generated from a Notify.
func (n *Notify) MarshalBinary() ([]byte, error) {
	b := make([]byte, n.MarshalLen())
	if err := n.MarshalTo(b); err != nil {
		return nil, err
	}
	return b, nil
}

// MarshalTo puts the byte sequence in the byte array given as b.
func (n *Notify) MarshalTo(b []byte) error {
	if err := requireParameter(n.Status, "Status"); err != nil {
		return err
	}
	if len(b) < n.MarshalLen() {
		return ErrTooShortToMarshalBinary
	}

	n.Header.Payload = make([]byte, n.MarshalLen()-8)

	var offset = 0

	if param := n.Status; param != nil {
		if err := param.MarshalTo(n.Header.Payload[offset:]); err != nil {
			return err
		}
		offset += param.MarshalLen()
	}

	if param := n.AspIdentifier; param != nil {
		if err := param.MarshalTo(n.Header.Payload[offset:]); err != nil {
			return err
		}
		offset += param.MarshalLen()
	}

	if param := n.RoutingContext; param != nil {
		if err := param.MarshalTo(n.Header.Payload[offset:]); err != nil {
			return err
		}
		offset += param.MarshalLen()
	}

	if param := n.InfoString; param != nil {
		if err := param.MarshalTo(n.Header.Payload[offset:]); err != nil {
			return err
		}
		offset += param.MarshalLen()
	}
	if err := marshalOtherParams(n.Header.Payload, offset, n.Others); err != nil {
		return err
	}

	return n.Header.MarshalTo(b)
}

// ParseNotify decodes given byte sequence as a Notify.
func ParseNotify(b []byte) (*Notify, error) {
	n := &Notify{}
	if err := n.UnmarshalBinary(b); err != nil {
		return nil, err
	}
	return n, nil
}

// UnmarshalBinary sets the values retrieved from byte sequence in a M3UA common header.
func (n *Notify) UnmarshalBinary(b []byte) error {
	*n = Notify{}
	var err error
	n.Header, err = parseTypedHeader(b, MsgClassManagement, MsgTypeNotify)
	if err != nil {
		return err
	}

	prs, err := params.ParseMultiParams(n.Header.Payload)
	if err != nil {
		return err
	}
	for _, pr := range prs {
		switch pr.Tag {
		case params.Status:
			if n.Status != nil {
				return ErrInvalidParameter
			}
			n.Status = pr
		case params.AspIdentifier:
			if n.AspIdentifier != nil {
				return ErrInvalidParameter
			}
			n.AspIdentifier = pr
		case params.RoutingContext:
			if n.RoutingContext != nil {
				return ErrInvalidParameter
			}
			n.RoutingContext = pr
		case params.InfoString:
			if n.InfoString != nil {
				return ErrInvalidParameter
			}
			n.InfoString = pr
		default:
			n.Others = append(n.Others, pr)
		}
	}
	return requireParameter(n.Status, "Status")
}

// SetLength sets the length in Length field.
func (n *Notify) SetLength() {
	if param := n.Status; param != nil {
		param.SetLength()
	}
	if param := n.AspIdentifier; param != nil {
		param.SetLength()
	}
	if param := n.RoutingContext; param != nil {
		param.SetLength()
	}
	if param := n.InfoString; param != nil {
		param.SetLength()
	}
	setParamLengths(n.Others)

	n.Header.Length = uint32(n.MarshalLen())
}

// MarshalLen returns the serial length of Notify.
func (n *Notify) MarshalLen() int {
	l := 8

	if param := n.Status; param != nil {
		l += param.MarshalLen()
	}
	if param := n.AspIdentifier; param != nil {
		l += param.MarshalLen()
	}
	if param := n.RoutingContext; param != nil {
		l += param.MarshalLen()
	}
	if param := n.InfoString; param != nil {
		l += param.MarshalLen()
	}
	l += paramsMarshalLen(n.Others)
	return l
}

// String returns the Notify values in human readable format.
func (n *Notify) String() string {
	if n == nil {
		return ""
	}
	return fmt.Sprintf("{Header: %s, Status: %s, AspIdentifier: %s, RoutingContext: %s, InfoString: %s}",
		n.Header.String(),
		n.Status.String(),
		n.AspIdentifier.String(),
		n.RoutingContext.String(),
		n.InfoString.String(),
	)
}

// Version returns the version of M3UA in int.
func (n *Notify) Version() uint8 {
	return n.Header.Version
}

// MessageType returns the message type in int.
func (n *Notify) MessageType() uint8 {
	return MsgTypeNotify
}

// MessageClass returns the message class in int.
func (n *Notify) MessageClass() uint8 {
	return MsgClassManagement
}

// MessageClassName returns the name of message class.
func (n *Notify) MessageClassName() string {
	return MsgClassNameManagement
}

// MessageTypeName returns the name of message type.
func (n *Notify) MessageTypeName() string {
	return "Notify"
}

// Serialize returns the byte sequence generated from a Notify.
//
// DEPRECATED: use MarshalBinary instead.
func (n *Notify) Serialize() ([]byte, error) {
	log.Println("DEPRECATED: MarshalBinary instead")
	return n.MarshalBinary()
}

// SerializeTo puts the byte sequence in the byte array given as b.
//
// DEPRECATED: use MarshalTo instead.
func (n *Notify) SerializeTo(b []byte) error {
	log.Println("DEPRECATED: MarshalTo instead")
	return n.MarshalTo(b)
}

// DecodeNotify decodes given byte sequence as a Notify.
//
// DEPRECATED: use ParseNotify instead.
func DecodeNotify(b []byte) (*Notify, error) {
	log.Println("DEPRECATED: use ParseNotify instead")
	return ParseNotify(b)
}

// DecodeFromBytes sets the values retrieved from byte sequence in a M3UA common header.
//
// DEPRECATED: use UnmarshalBinary instead.
func (n *Notify) DecodeFromBytes(b []byte) error {
	log.Println("DEPRECATED: use UnmarshalBinary instead")
	return n.UnmarshalBinary(b)
}

// Len returns the serial length of Notify.
//
// DEPRECATED: use MarshalLen instead.
func (n *Notify) Len() int {
	log.Println("DEPRECATED: use MarshalLen instead")
	return n.MarshalLen()
}
