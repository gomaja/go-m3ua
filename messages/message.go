// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package messages

import (
	"errors"
	"log"
)

// Message Class definitions.
const (
	MsgClassManagement uint8 = iota
	MsgClassTransfer
	MsgClassSSNM
	MsgClassASPSM
	MsgClassASPTM
	_
	_
	_
	_
	MsgClassRKM
)

// Message Class Name definitions.
const (
	MsgClassNameManagement = "Management"
	MsgClassNameTransfer   = "Transfer"
	MsgClassNameSSNM       = "SSNM"
	MsgClassNameASPSM      = "ASPSM"
	MsgClassNameASPTM      = "ASPTM"
	MsgClassNameRKM        = "RKM"
)

// Message Type definitions (Management).
const (
	MsgTypeError = iota
	MsgTypeNotify
)

// Message Type definitions (SSNM).
const (
	_ = iota
	MsgTypeDestinationUnavailable
	MsgTypeDestinationAvailable
	MsgTypeDestinationStateAudit
	MsgTypeSignallingCongestion
	MsgTypeDestinationUserPartUnavailable
	MsgTypeDestinationRestricted
)

// Message Type definitions (Transfer).
const (
	_ uint8 = iota
	MsgTypePayloadData
)

// Message Type definitions (ASPSM).
const (
	_ uint8 = iota
	MsgTypeAspUp
	MsgTypeAspDown
	MsgTypeHeartbeat
	MsgTypeAspUpAck
	MsgTypeAspDownAck
	MsgTypeHeartbeatAck
)

// Message Type definitions (ASPTM).
const (
	_ uint8 = iota
	MsgTypeAspActive
	MsgTypeAspInactive
	MsgTypeAspActiveAck
	MsgTypeAspInactiveAck
)

// Message Type definitions (RKM).
const (
	_ uint8 = iota
	MsgTypeRegistrationRequest
	MsgTypeRegistrationResponse
	MsgTypeDeregistrationRequest
	MsgTypeDeregistrationResponse
)

// M3UA is an interface that defines M3UA messages.
type M3UA interface {
	MarshalBinary() ([]byte, error)
	MarshalTo([]byte) error
	UnmarshalBinary([]byte) error
	MarshalLen() int
	Version() uint8
	MessageClass() uint8
	MessageType() uint8
	MessageClassName() string
	MessageTypeName() string
}

// MarshalBinary returns the byte sequence generated from a M3UA instance.
// Better to use MarshalBinaryXxx instead if you know the type of data to be serialized.
func MarshalBinary(m M3UA) ([]byte, error) {
	b := make([]byte, m.MarshalLen())
	if err := m.MarshalTo(b); err != nil {
		return nil, err
	}

	return b, nil
}

// Parse decodes the given bytes.
// This function checks the Message Class and Message Type and chooses the appropriate type.
func Parse(b []byte) (M3UA, error) {
	if len(b) < 4 {
		return nil, ErrTooShortToParse
	}

	m := newMessageFor(b[2], b[3])
	if m == nil {
		// If the combination of class and type is unknown or not supported,
		// *Generic is used.
		m = &Generic{}
	}

	if err := m.UnmarshalBinary(b); err != nil {
		return nil, err
	}
	return m, nil
}

// IsSupported reports whether this package decodes the given message class and
// type into a message of its own, rather than falling back to Generic.
//
// It is what separates "this type is not supported" from "this message is one
// we implement and its parameters are at fault", which RFC 4666 Section 3.8.1
// answers with different error codes.
func IsSupported(class, msgType uint8) bool {
	return newMessageFor(class, msgType) != nil
}

// newMessageFor returns an empty message of the type the class and type octets
// name, or nil if this package does not implement that combination.
func newMessageFor(class, msgType uint8) M3UA {
	var m M3UA
	// Class and type each occupy a full octet on the wire (RFC 4666 Section
	// 3.1), so the key must be widened before it is shifted. Shifting inside
	// uint8 — `uint8(c)<<4 | t` — dropped the class entirely and folded every
	// class onto the type byte, so a peer reached any handler it liked by
	// writing the combined value into the type field alone: class 0 type 0x31
	// was parsed and reported as an ASP Up. See
	// TestParseNeverMisreportsClassOrType.
	combine := func(c, t uint8) uint16 {
		return uint16(c)<<8 | uint16(t)
	}

	switch combine(class, msgType) {
	// Transfer Messages
	case combine(MsgClassTransfer, MsgTypePayloadData):
		m = &Data{}
		// SSNM Messages
	case combine(MsgClassSSNM, MsgTypeDestinationUnavailable):
		m = &DestinationUnavailable{}
	case combine(MsgClassSSNM, MsgTypeDestinationAvailable):
		m = &DestinationAvailable{}
	case combine(MsgClassSSNM, MsgTypeDestinationStateAudit):
		m = &DestinationStateAudit{}
	case combine(MsgClassSSNM, MsgTypeSignallingCongestion):
		m = &SignallingCongestion{}
	case combine(MsgClassSSNM, MsgTypeDestinationUserPartUnavailable):
		m = &DestinationUserPartUnavailable{}
	case combine(MsgClassSSNM, MsgTypeDestinationRestricted):
		m = &DestinationRestricted{}
		// ASPSM Messages
	case combine(MsgClassASPSM, MsgTypeAspUp):
		m = &AspUp{}
	case combine(MsgClassASPSM, MsgTypeAspDown):
		m = &AspDown{}
	case combine(MsgClassASPSM, MsgTypeHeartbeat):
		m = &Heartbeat{}
	case combine(MsgClassASPSM, MsgTypeAspUpAck):
		m = &AspUpAck{}
	case combine(MsgClassASPSM, MsgTypeAspDownAck):
		m = &AspDownAck{}
	case combine(MsgClassASPSM, MsgTypeHeartbeatAck):
		m = &HeartbeatAck{}
	// ASPTM Messages
	case combine(MsgClassASPTM, MsgTypeAspActive):
		m = &AspActive{}
	case combine(MsgClassASPTM, MsgTypeAspActiveAck):
		m = &AspActiveAck{}
	case combine(MsgClassASPTM, MsgTypeAspInactive):
		m = &AspInactive{}
	case combine(MsgClassASPTM, MsgTypeAspInactiveAck):
		m = &AspInactiveAck{}
	// Management Messages
	case combine(MsgClassManagement, MsgTypeError):
		m = &Error{}
	case combine(MsgClassManagement, MsgTypeNotify):
		m = &Notify{}
	default:
		return nil
	}
	return m
}

// Decode decodes the given bytes.
// This function checks the Message Class and Message Type and chooses the appropriate type.
//
// DEPRECATED: use Parse instead.
func Decode(b []byte) (M3UA, error) {
	log.Println("DEPRECATED: use Parse instead")
	return Parse(b)
}

// Error definitions.
var (
	ErrTooShortToMarshalBinary = errors.New("insufficient buffer to serialize M3UA to")
	ErrTooShortToParse         = errors.New("too short to decode as M3UA")
	ErrInvalidMessageLength    = errors.New("invalid M3UA Message Length")
	ErrMissingParameter        = errors.New("message is missing a mandatory parameter")
	ErrUnexpectedMessageType   = errors.New("unexpected M3UA message class or type")
	ErrInvalidParameter        = errors.New("got invalid parameter inside a message")
)
