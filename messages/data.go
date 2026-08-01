// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package messages

import (
	"fmt"
	"log"

	"github.com/gomaja/go-m3ua/messages/params"
)

// Data is a PayloadData type of M3UA message.
//
// Spec: 3.3.1, RFC4666.
type Data struct {
	*Header
	NetworkAppearance *params.Param
	RoutingContext    *params.Param
	ProtocolData      *params.Param
	CorrelationID     *params.Param
	// Others holds parameters this version does not define.
	//
	// RFC 4666 Section 3: "For forward compatibility, all Message Types may
	// have attached parameters even if none are specified in this version."
	// They are kept rather than rejected so a peer running a later extension
	// still interoperates, and so a message that must be echoed back can be
	// echoed whole.
	Others []*params.Param
}

// NewData creates a new Data.
func NewData(nwApr, rtCtx, pd, corrID *params.Param) *Data {
	d := &Data{
		Header: &Header{
			Version:  1,
			Reserved: 0,
			Class:    MsgClassTransfer,
			Type:     MsgTypePayloadData,
		},
		NetworkAppearance: nwApr,
		RoutingContext:    rtCtx,
		ProtocolData:      pd,
		CorrelationID:     corrID,
	}
	d.SetLength()

	return d
}

// MarshalBinary returns the byte sequence generated from a Data.
func (d *Data) MarshalBinary() ([]byte, error) {
	b := make([]byte, d.MarshalLen())
	if err := d.MarshalTo(b); err != nil {
		return nil, err
	}
	return b, nil
}

// MarshalTo puts the byte sequence in the byte array given as b.
func (d *Data) MarshalTo(b []byte) error {
	if err := d.validateParameters(); err != nil {
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

	if param := d.ProtocolData; param != nil {
		if err := param.MarshalTo(d.Header.Payload[offset:]); err != nil {
			return err
		}
		offset += param.MarshalLen()
	}

	if param := d.CorrelationID; param != nil {
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

// ParseData decodes given byte sequence as a Data.
func ParseData(b []byte) (*Data, error) {
	d := &Data{}
	if err := d.UnmarshalBinary(b); err != nil {
		return nil, err
	}
	return d, nil
}

// UnmarshalBinary sets the values retrieved from byte sequence in a M3UA common header.
func (d *Data) UnmarshalBinary(b []byte) error {
	*d = Data{}
	var err error
	d.Header, err = parseTypedHeader(b, MsgClassTransfer, MsgTypePayloadData)
	if err != nil {
		return err
	}

	prs, err := params.ParseMultiParams(d.Header.Payload)
	if err != nil {
		return err
	}
	for i, pr := range prs {
		switch pr.Tag {
		case params.NetworkAppearance:
			// RFC 4666 Section 3.3.1: when present, Network Appearance
			// MUST be the first parameter in DATA.
			if i != 0 {
				return ErrInvalidParameter
			}
			d.NetworkAppearance = pr
		case params.RoutingContext:
			if d.RoutingContext != nil {
				return ErrInvalidParameter
			}
			d.RoutingContext = pr
		case params.ProtocolData:
			if d.ProtocolData != nil {
				return ErrInvalidParameter
			}
			d.ProtocolData = pr
		case params.CorrelationID:
			if d.CorrelationID != nil {
				return ErrInvalidParameter
			}
			d.CorrelationID = pr
		default:
			d.Others = append(d.Others, pr)
		}
	}
	return d.validateParameters()
}

func (d *Data) validateParameters() error {
	if err := requireParameter(d.ProtocolData, "Protocol Data"); err != nil {
		return err
	}

	rules := [...]struct {
		parameter     *params.Param
		name          string
		tag           uint16
		exactLength   int
		minimumLength int
	}{
		{parameter: d.NetworkAppearance, name: "Network Appearance", tag: params.NetworkAppearance, exactLength: 4},
		{parameter: d.RoutingContext, name: "Routing Context", tag: params.RoutingContext, exactLength: 4},
		{parameter: d.ProtocolData, name: "Protocol Data", tag: params.ProtocolData, minimumLength: 12},
		{parameter: d.CorrelationID, name: "Correlation Id", tag: params.CorrelationID, exactLength: 4},
	}

	for _, rule := range rules {
		if rule.parameter == nil {
			continue
		}
		if rule.parameter.Tag != rule.tag {
			return fmt.Errorf(
				"%w: DATA %s has tag %#04x, want %#04x",
				params.ErrInvalidType,
				rule.name,
				rule.parameter.Tag,
				rule.tag,
			)
		}

		valueLength := len(rule.parameter.Data)
		if rule.exactLength != 0 && valueLength != rule.exactLength {
			return fmt.Errorf(
				"%w: DATA %s value has %d octets, requires exactly %d",
				params.ErrInvalidLength,
				rule.name,
				valueLength,
				rule.exactLength,
			)
		}
		if rule.minimumLength != 0 && valueLength < rule.minimumLength {
			return fmt.Errorf(
				"%w: DATA %s value has %d octets, requires at least %d",
				params.ErrInvalidLength,
				rule.name,
				valueLength,
				rule.minimumLength,
			)
		}
	}

	for index, parameter := range d.Others {
		if parameter == nil {
			continue
		}
		switch parameter.Tag {
		case params.NetworkAppearance, params.RoutingContext, params.ProtocolData, params.CorrelationID:
			return fmt.Errorf(
				"%w: DATA Others[%d] contains known parameter tag %#04x",
				ErrInvalidParameter,
				index,
				parameter.Tag,
			)
		}
	}

	return nil
}

// SetLength sets the length in Length field.
func (d *Data) SetLength() {
	if param := d.NetworkAppearance; param != nil {
		param.SetLength()
	}
	if param := d.RoutingContext; param != nil {
		param.SetLength()
	}
	if param := d.ProtocolData; param != nil {
		param.SetLength()
	}
	if param := d.CorrelationID; param != nil {
		param.SetLength()
	}
	setParamLengths(d.Others)

	d.Header.Length = uint32(d.MarshalLen())
}

// MarshalLen returns the serial length of Data.
func (d *Data) MarshalLen() int {
	l := 8
	if param := d.NetworkAppearance; param != nil {
		l += param.MarshalLen()
	}
	if param := d.RoutingContext; param != nil {
		l += param.MarshalLen()
	}
	if param := d.ProtocolData; param != nil {
		l += param.MarshalLen()
	}
	if param := d.CorrelationID; param != nil {
		l += param.MarshalLen()
	}
	l += paramsMarshalLen(d.Others)
	return l
}

// String returns the Data values in human readable format.
func (d *Data) String() string {
	if d == nil {
		return ""
	}
	return fmt.Sprintf("{Header: %s, NetworkAppearance: %s, RoutingContext: %s, ProtocolData %s, CorrelationID: %s}",
		d.Header.String(),
		d.NetworkAppearance.String(),
		d.RoutingContext.String(),
		d.ProtocolData.String(),
		d.CorrelationID.String(),
	)
}

// Version returns the version of M3UA in int.
func (d *Data) Version() uint8 {
	return d.Header.Version
}

// MessageType returns the message type in int.
func (d *Data) MessageType() uint8 {
	return MsgTypePayloadData
}

// MessageClass returns the message class in int.
func (d *Data) MessageClass() uint8 {
	return MsgClassTransfer
}

// MessageClassName returns the name of message class.
func (d *Data) MessageClassName() string {
	return MsgClassNameTransfer
}

// MessageTypeName returns the name of message type.
func (d *Data) MessageTypeName() string {
	return "Payload Data"
}

// Serialize returns the byte sequence generated from a Data.
//
// DEPRECATED: use MarshalBinary instead.
func (d *Data) Serialize() ([]byte, error) {
	log.Println("DEPRECATED: MarshalBinary instead")
	return d.MarshalBinary()
}

// SerializeTo puts the byte sequence in the byte array given as b.
//
// DEPRECATED: use MarshalTo instead.
func (d *Data) SerializeTo(b []byte) error {
	log.Println("DEPRECATED: MarshalTo instead")
	return d.MarshalTo(b)
}

// DecodeData decodes given byte sequence as a Data.
//
// DEPRECATED: use ParseData instead.
func DecodeData(b []byte) (*Data, error) {
	log.Println("DEPRECATED: use ParseData instead")
	return ParseData(b)
}

// DecodeFromBytes sets the values retrieved from byte sequence in a M3UA common header.
//
// DEPRECATED: use UnmarshalBinary instead.
func (d *Data) DecodeFromBytes(b []byte) error {
	log.Println("DEPRECATED: use UnmarshalBinary instead")
	return d.UnmarshalBinary(b)
}

// Len returns the serial length of Data.
//
// DEPRECATED: use MarshalLen instead.
func (d *Data) Len() int {
	log.Println("DEPRECATED: use MarshalLen instead")
	return d.MarshalLen()
}
