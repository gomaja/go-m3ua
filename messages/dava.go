// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package messages

import (
	"fmt"
	"log"

	"github.com/gomaja/go-m3ua/messages/params"
)

// DestinationAvailable is a DestinationAvailable type of M3UA message.
//
// Spec: 3.4.2, RFC4666.
type DestinationAvailable struct {
	*Header
	NetworkAppearance *params.Param
	RoutingContext    *params.Param
	AffectedPointCode *params.Param
	InfoString        *params.Param
	// Others holds parameters this version does not define.
	//
	// RFC 4666 Section 3: "For forward compatibility, all Message Types may
	// have attached parameters even if none are specified in this version."
	// They are kept rather than rejected so a peer running a later extension
	// still interoperates, and so a message that must be echoed back can be
	// echoed whole.
	Others []*params.Param
}

// NewDestinationAvailable creates a new DestinationAvailable.
func NewDestinationAvailable(nwApr, rtCtx, apcs, info *params.Param) *DestinationAvailable {
	d := &DestinationAvailable{
		Header: &Header{
			Version:  1,
			Reserved: 0,
			Class:    MsgClassSSNM,
			Type:     MsgTypeDestinationAvailable,
		},
		NetworkAppearance: nwApr,
		RoutingContext:    rtCtx,
		AffectedPointCode: apcs,
		InfoString:        info,
	}
	d.SetLength()

	return d
}

// MarshalBinary returns the byte sequence generated from a DestinationAvailable.
func (d *DestinationAvailable) MarshalBinary() ([]byte, error) {
	b := make([]byte, d.MarshalLen())
	if err := d.MarshalTo(b); err != nil {
		return nil, err
	}
	return b, nil
}

// MarshalTo puts the byte sequence in the byte array given as b.
func (d *DestinationAvailable) MarshalTo(b []byte) error {
	if err := requireParameter(d.AffectedPointCode, "Affected Point Code"); err != nil {
		return err
	}
	if len(b) < d.MarshalLen() {
		return ErrTooShortToMarshalBinary
	}

	d.Header.Payload = make([]byte, d.MarshalLen()-8)

	var offset = 0
	if param := d.NetworkAppearance; param != nil {
		if err := param.MarshalTo(d.Header.Payload[offset:]); err != nil {
			return err
		}
		offset += param.MarshalLen()
	}
	if param := d.RoutingContext; param != nil {
		if err := param.MarshalTo(d.Header.Payload[offset:]); err != nil {
			return err
		}
		offset += param.MarshalLen()
	}
	if param := d.AffectedPointCode; param != nil {
		if err := param.MarshalTo(d.Header.Payload[offset:]); err != nil {
			return err
		}
		offset += param.MarshalLen()
	}
	if param := d.InfoString; param != nil {
		if err := param.MarshalTo(d.Header.Payload[offset:]); err != nil {
			return err
		}
		offset += param.MarshalLen()
	}
	if err := marshalOtherParams(d.Header.Payload, offset, d.Others); err != nil {
		return err
	}
	return d.Header.MarshalTo(b)
}

// ParseDestinationAvailable decodes given byte sequence as a DestinationAvailable.
func ParseDestinationAvailable(b []byte) (*DestinationAvailable, error) {
	d := &DestinationAvailable{}
	if err := d.UnmarshalBinary(b); err != nil {
		return nil, err
	}
	return d, nil
}

// UnmarshalBinary sets the values retrieved from byte sequence in a M3UA common header.
func (d *DestinationAvailable) UnmarshalBinary(b []byte) error {
	*d = DestinationAvailable{}
	var err error
	d.Header, err = parseTypedHeader(b, MsgClassSSNM, MsgTypeDestinationAvailable)
	if err != nil {
		return err
	}

	prs, err := params.ParseMultiParams(d.Header.Payload)
	if err != nil {
		return err
	}
	for _, pr := range prs {
		switch pr.Tag {
		case params.NetworkAppearance:
			if d.NetworkAppearance != nil {
				return ErrInvalidParameter
			}
			d.NetworkAppearance = pr
		case params.RoutingContext:
			if d.RoutingContext != nil {
				return ErrInvalidParameter
			}
			d.RoutingContext = pr
		case params.AffectedPointCode:
			if d.AffectedPointCode != nil {
				return ErrInvalidParameter
			}
			d.AffectedPointCode = pr
		case params.InfoString:
			if d.InfoString != nil {
				return ErrInvalidParameter
			}
			d.InfoString = pr
		default:
			d.Others = append(d.Others, pr)
		}
	}
	return requireParameter(d.AffectedPointCode, "Affected Point Code")
}

// SetLength sets the length in Length field.
func (d *DestinationAvailable) SetLength() {
	if param := d.NetworkAppearance; param != nil {
		param.SetLength()
	}
	if param := d.RoutingContext; param != nil {
		param.SetLength()
	}
	if param := d.AffectedPointCode; param != nil {
		param.SetLength()
	}
	if param := d.InfoString; param != nil {
		param.SetLength()
	}
	setParamLengths(d.Others)

	d.Header.Length = uint32(d.MarshalLen())
}

// MarshalLen returns the serial length of DestinationAvailable.
func (d *DestinationAvailable) MarshalLen() int {
	l := 8
	if param := d.NetworkAppearance; param != nil {
		l += param.MarshalLen()
	}
	if param := d.RoutingContext; param != nil {
		l += param.MarshalLen()
	}
	if param := d.AffectedPointCode; param != nil {
		l += param.MarshalLen()
	}
	if param := d.InfoString; param != nil {
		l += param.MarshalLen()
	}
	l += paramsMarshalLen(d.Others)
	return l
}

// String returns the DestinationAvailable values in human readable format.
func (d *DestinationAvailable) String() string {
	if d == nil {
		return ""
	}
	return fmt.Sprintf("{Header: %s, NetworkAppearance: %s, RoutingContext: %s, AffectedPointCode: %s, InfoString: %s}",
		d.Header.String(),
		d.NetworkAppearance.String(),
		d.RoutingContext.String(),
		d.AffectedPointCode.String(),
		d.InfoString.String(),
	)
}

// Version returns the version of M3UA in int.
func (d *DestinationAvailable) Version() uint8 {
	return d.Header.Version
}

// MessageType returns the message type in int.
func (d *DestinationAvailable) MessageType() uint8 {
	return MsgTypeDestinationAvailable
}

// MessageClass returns the message class in int.
func (d *DestinationAvailable) MessageClass() uint8 {
	return MsgClassSSNM
}

// MessageClassName returns the name of message class.
func (d *DestinationAvailable) MessageClassName() string {
	return MsgClassNameSSNM
}

// MessageTypeName returns the name of message type.
func (d *DestinationAvailable) MessageTypeName() string {
	return "Destination Available"
}

// Serialize returns the byte sequence generated from a DestinationAvailable.
//
// DEPRECATED: use MarshalBinary instead.
func (d *DestinationAvailable) Serialize() ([]byte, error) {
	log.Println("DEPRECATED: MarshalBinary instead")
	return d.MarshalBinary()
}

// SerializeTo puts the byte sequence in the byte array given as b.
//
// DEPRECATED: use MarshalTo instead.
func (d *DestinationAvailable) SerializeTo(b []byte) error {
	log.Println("DEPRECATED: MarshalTo instead")
	return d.MarshalTo(b)
}

// DecodeDestinationAvailable decodes given byte sequence as a DestinationAvailable.
//
// DEPRECATED: use ParseDestinationAvailable instead.
func DecodeDestinationAvailable(b []byte) (*DestinationAvailable, error) {
	log.Println("DEPRECATED: use ParseDestinationAvailable instead")
	return ParseDestinationAvailable(b)
}

// DecodeFromBytes sets the values retrieved from byte sequence in a M3UA common header.
//
// DEPRECATED: use UnmarshalBinary instead.
func (d *DestinationAvailable) DecodeFromBytes(b []byte) error {
	log.Println("DEPRECATED: use UnmarshalBinary instead")
	return d.UnmarshalBinary(b)
}

// Len returns the serial length of DestinationAvailable.
//
// DEPRECATED: use MarshalLen instead.
func (d *DestinationAvailable) Len() int {
	log.Println("DEPRECATED: use MarshalLen instead")
	return d.MarshalLen()
}
