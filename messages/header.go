// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package messages

import (
	"encoding/binary"
	"fmt"
	"log"
)

// Header is a M3UA common header.
type Header struct {
	Version  uint8
	Reserved uint8
	Class    uint8
	Type     uint8
	Length   uint32
	Payload  []byte
}

// NewHeader creates a new Header.
func NewHeader(version, class, mtype uint8, payload []byte) *Header {
	h := &Header{
		Version:  version,
		Reserved: 0,
		Class:    class,
		Type:     mtype,
		Payload:  payload,
	}
	h.SetLength()

	return h
}

// MarshalBinary returns the byte sequence generated from a Header instance.
func (h *Header) MarshalBinary() ([]byte, error) {
	if err := h.validateMarshalLength(); err != nil {
		return nil, err
	}

	b := make([]byte, h.MarshalLen())
	if err := h.MarshalTo(b); err != nil {
		return nil, err
	}

	return b, nil
}

// MarshalTo puts the byte sequence in the byte array given as b.
func (h *Header) MarshalTo(b []byte) error {
	if err := h.validateMarshalLength(); err != nil {
		return err
	}
	if len(b) < h.MarshalLen() {
		return ErrTooShortToMarshalBinary
	}

	h.SetLength()
	b[0] = h.Version
	b[1] = h.Reserved
	b[2] = h.Class
	b[3] = h.Type
	binary.BigEndian.PutUint32(b[4:8], h.Length)
	copy(b[8:h.MarshalLen()], h.Payload)

	return nil
}

// ParseHeader decodes given byte sequence as a M3UA common header.
func ParseHeader(b []byte) (*Header, error) {
	h := &Header{}
	if err := h.UnmarshalBinary(b); err != nil {
		return nil, err
	}

	return h, nil
}

// UnmarshalBinary sets the values retrieved from byte sequence in a M3UA common header.
func (h *Header) UnmarshalBinary(b []byte) error {
	l := len(b)
	if l < 8 {
		return ErrTooShortToParse
	}

	messageLength := binary.BigEndian.Uint32(b[4:8])
	if messageLength < 8 {
		return fmt.Errorf("%w: declared %d octets, shorter than the 8-octet common header", ErrInvalidMessageLength, messageLength)
	}
	if uint64(messageLength) > uint64(l) {
		return fmt.Errorf("%w: declared %d octets, received %d", ErrInvalidMessageLength, messageLength, l)
	}

	payloadEnd := int(messageLength)
	if payloadEnd < l && !isOmittedFinalParameterPadding(b[8:payloadEnd], l-payloadEnd) {
		return fmt.Errorf("%w: declared %d octets, received %d", ErrInvalidMessageLength, messageLength, l)
	}

	h.Version = b[0]
	h.Reserved = b[1]
	h.Class = b[2]
	h.Type = b[3]
	h.Length = messageLength
	h.Payload = b[8:payloadEnd]

	return nil
}

func (h *Header) validateMarshalLength() error {
	if uint64(h.MarshalLen()) > uint64(^uint32(0)) {
		return fmt.Errorf("%w: %d octets exceeds the 32-bit field", ErrInvalidMessageLength, h.MarshalLen())
	}
	return nil
}

// isOmittedFinalParameterPadding recognizes the sole Message Length mismatch
// RFC 4666 Section 3.1.4 asks receivers to accept: all padding after the final
// parameter is physically present, but not counted in Message Length.
func isOmittedFinalParameterPadding(payload []byte, trailingLength int) bool {
	if trailingLength < 1 || trailingLength > 3 {
		return false
	}

	for offset := 0; offset < len(payload); {
		if len(payload)-offset < 4 {
			return false
		}

		parameterLength := int(binary.BigEndian.Uint16(payload[offset+2 : offset+4]))
		if parameterLength < 4 || parameterLength > len(payload)-offset {
			return false
		}

		parameterEnd := offset + parameterLength
		padding := (4 - parameterLength%4) % 4
		if parameterEnd == len(payload) {
			return padding == trailingLength
		}

		offset = parameterEnd + padding
		if offset > len(payload) {
			return false
		}
	}

	return false
}

// MarshalLen returns the serial length of Header.
func (h *Header) MarshalLen() int {
	return 8 + len(h.Payload)
}

// SetLength sets the length in Length field.
func (h *Header) SetLength() {
	h.Length = uint32(8 + len(h.Payload))
}

// String returns the M3UA common header values in human readable format.
func (h *Header) String() string {
	if h == nil {
		return ""
	}
	return fmt.Sprintf("{Version: %d, Reserved: %#x, Class: %d, Type: %d, Length: %d, Payload: %x}",
		h.Version,
		h.Reserved,
		h.Class,
		h.Type,
		h.Length,
		h.Payload,
	)
}

// Serialize returns the byte sequence generated from a Header.
//
// DEPRECATED: use MarshalBinary instead.
func (h *Header) Serialize() ([]byte, error) {
	log.Println("DEPRECATED: MarshalBinary instead")
	return h.MarshalBinary()
}

// SerializeTo puts the byte sequence in the byte array given as b.
//
// DEPRECATED: use MarshalTo instead.
func (h *Header) SerializeTo(b []byte) error {
	log.Println("DEPRECATED: MarshalTo instead")
	return h.MarshalTo(b)
}

// DecodeHeader decodes given byte sequence as a Header.
//
// DEPRECATED: use ParseHeader instead.
func DecodeHeader(b []byte) (*Header, error) {
	log.Println("DEPRECATED: use ParseHeader instead")
	return ParseHeader(b)
}

// DecodeFromBytes sets the values retrieved from byte sequence in a M3UA common header.
//
// DEPRECATED: use UnmarshalBinary instead.
func (h *Header) DecodeFromBytes(b []byte) error {
	log.Println("DEPRECATED: use UnmarshalBinary instead")
	return h.UnmarshalBinary(b)
}

// Len returns the serial length of Header.
//
// DEPRECATED: use MarshalLen instead.
func (h *Header) Len() int {
	log.Println("DEPRECATED: use MarshalLen instead")
	return h.MarshalLen()
}
