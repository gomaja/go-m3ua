// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package params

import (
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"unicode/utf8"
)

const maxParamValueLength = int(^uint16(0)) - 4
const maxNestedContainerDepth = 64

// Common Parameter Tag definitions.
const (
	_ uint16 = iota
	_
	_
	_
	InfoString
	_
	RoutingContext
	DiagnosticInformation
	_
	HeartbeatData
	_
	TrafficModeType
	ErrorCode
	Status
	_
	_
	_
	AspIdentifier
	AffectedPointCode
	CorrelationID
)

func isDefinedParameterTag(tag uint16) bool {
	switch tag {
	case InfoString,
		RoutingContext,
		DiagnosticInformation,
		HeartbeatData,
		TrafficModeType,
		ErrorCode,
		Status,
		AspIdentifier,
		AffectedPointCode,
		CorrelationID,
		NetworkAppearance,
		UserCause,
		CongestionIndications,
		ConcernedDestination,
		RoutingKey,
		RegistrationResult,
		DeregistrationResult,
		LocalRoutingKeyIdentifier,
		DestinationPointCode,
		ServiceIndicators,
		OriginatingPointCodeList,
		ProtocolData,
		RegistrationStatus,
		DeregistrationStatus:
		return true
	default:
		return false
	}
}

// M3UA-specific Parameter Tag definitions.
const (
	NetworkAppearance uint16 = uint16(0x200 | iota)
	_
	_
	_
	UserCause
	CongestionIndications
	ConcernedDestination
	RoutingKey           // specific: later
	RegistrationResult   // specific: later
	DeregistrationResult // specific: later
	LocalRoutingKeyIdentifier
	DestinationPointCode
	ServiceIndicators
	_
	OriginatingPointCodeList
	_
	ProtocolData
	_
	RegistrationStatus
	DeregistrationStatus
)

// Error definitions.
var (
	ErrInvalidType             = errors.New("got invalid type in parameter")
	ErrInvalidLength           = errors.New("parameter has invalid length value")
	ErrInvalidValue            = errors.New("parameter has invalid value")
	ErrTooShortToMarshalBinary = errors.New("insufficient buffer to serialize parameter to")
	ErrTooShortToParse         = errors.New("too short to decode as parameter")
)

// Param is a M3UA Param.
type Param struct {
	Tag    uint16
	Length uint16
	Data   []byte
}

func newUint32ValParam(t uint16, u uint32) *Param {
	p := &Param{
		Tag:    t,
		Length: 8,
		Data:   make([]byte, 4),
	}
	binary.BigEndian.PutUint32(p.Data, u)
	return p
}

func newUint24ValParam(t uint16, u uint32) *Param {
	p := &Param{
		Tag:    t,
		Length: 8,
		Data:   make([]byte, 1),
	}
	p.Data = append(p.Data, uint32To24(u)...)
	return p
}

func uint32To24(n uint32) []byte {
	return []byte{uint8(n >> 16), uint8(n >> 8), uint8(n)}
}

func newUint8ValParam(t uint16, u uint8) *Param {
	return &Param{
		Tag:    t,
		Length: 8,
		Data:   []byte{0, 0, 0, u},
	}
}

func newMultiUint32ValParam(t uint16, ux ...uint32) *Param {
	p := &Param{
		Tag: t,
	}

	p.Data = make([]byte, len(ux)*4)
	for i, u := range ux {
		binary.BigEndian.PutUint32(p.Data[i*4:(i+1)*4], u)
	}
	p.SetLength()
	return p
}

// newMultiUint8ValParam builds a parameter whose value is a sequence of single
// octets.
//
// The octets are the whole value and nothing is rounded up here. RFC 4666
// Section 3.2 keeps padding out of the length — "The length of the padding is
// NOT included in the parameter length field. A sender MUST NOT pad with more
// than 3 octets" — and Padding, MarshalLen and MarshalTo already implement that
// between them, appending the zero octets at marshal time without counting them.
//
// Rounding Data up to a multiple of four here instead put the pad octets inside
// the declared length, where a receiver reads them as value rather than
// ignoring them: this package's own decoder turned Service Indicators {1} back
// into {1, 0, 0, 0}, and a count already divisible by four gained a whole extra
// word of them.
func newMultiUint8ValParam(t uint16, ux ...uint8) *Param {
	p := &Param{
		Tag:  t,
		Data: make([]byte, len(ux)),
	}

	copy(p.Data, ux)
	p.SetLength()
	return p
}

func newVariableLenValParam(t uint16, b []byte) *Param {
	p := &Param{
		Tag:  t,
		Data: b,
	}
	p.SetLength()
	return p
}

func newNestedParam(t uint16, ps ...*Param) *Param {
	p := &Param{
		Tag: t,
	}

	for _, pr := range ps {
		if pr != nil {
			x, err := pr.MarshalBinary()
			if err != nil {
				p.Data = append(p.Data, marshalUncheckedParam(pr)...)
				p.SetLength()
				return p
			}
			p.Data = append(p.Data, x...)
		}
	}
	p.SetLength()
	return p
}

func marshalUncheckedParam(p *Param) []byte {
	if len(p.Data) > maxParamValueLength {
		wire := make([]byte, 4)
		binary.BigEndian.PutUint16(wire[0:2], p.Tag)
		return wire
	}
	parameterLength := 4 + len(p.Data)
	marshalLength := parameterLength + (4-parameterLength%4)%4
	wire := make([]byte, marshalLength)
	binary.BigEndian.PutUint16(wire[0:2], p.Tag)
	binary.BigEndian.PutUint16(wire[2:4], uint16(parameterLength))
	copy(wire[4:], p.Data)
	return wire
}

func invalidNestedParam(tag uint16) *Param {
	return &Param{Tag: tag}
}

func invalidNestedParameter(container, detail string) error {
	return fmt.Errorf("%w: %s %s", ErrInvalidValue, container, detail)
}

func duplicateNestedParameter(container, name string) error {
	return invalidNestedParameter(container, "contains duplicate "+name)
}

func invalidNestedLength(name string, got, want int) error {
	return fmt.Errorf("%w: %s value has %d octets, requires exactly %d", ErrInvalidLength, name, got, want)
}

func (p *Param) decodeUint32ValData() uint32 {
	l := len(p.Data)
	if l != 4 {
		return 0
	}
	return binary.BigEndian.Uint32(p.Data)
}

func (p *Param) decodeMultiUint32ValData() []uint32 {
	l := len(p.Data)
	if l%4 != 0 {
		return nil
	}

	var us []uint32
	for i := 0; i < l/4; i++ {
		us = append(us, binary.BigEndian.Uint32(p.Data[i*4:(i+1)*4]))
	}
	return us
}

// Copy returns a deep copy of a Param, or nil if p is nil.
//
// The New*() message constructors call SetLength on the Params handed to them,
// which writes to the caller's Param. Long-lived Params — notably those held in
// an association's configuration and reused for every outgoing message — must
// therefore be copied before being passed to a constructor, or two goroutines
// building messages concurrently will write to the same Param.
func (p *Param) Copy() *Param {
	if p == nil {
		return nil
	}

	c := &Param{Tag: p.Tag, Length: p.Length}
	if p.Data != nil {
		c.Data = make([]byte, len(p.Data))
		copy(c.Data, p.Data)
	}

	return c
}

// NewParam creates a new Param.
// This is for generic use. NewXXX(ParamName) functions are available to create the parameters defined in RFC4666.
func NewParam(tag int, data []byte) *Param {
	p := &Param{
		Tag:  uint16(tag),
		Data: data,
	}
	p.SetLength()
	return p
}

// MarshalBinary creates the byte sequence generated from a M3UA Param instance.
func (p *Param) MarshalBinary() ([]byte, error) {
	_, marshalLength, err := p.currentLengths()
	if err != nil {
		return nil, err
	}

	b := make([]byte, marshalLength)
	if err := p.MarshalTo(b); err != nil {
		return nil, err
	}
	return b, nil
}

// MarshalTo puts the byte sequence in the byte array given as b.
func (p *Param) MarshalTo(b []byte) error {
	parameterLength, marshalLength, err := p.currentLengths()
	if err != nil {
		return err
	}
	if len(b) < marshalLength {
		return ErrTooShortToMarshalBinary
	}

	p.Length = parameterLength
	binary.BigEndian.PutUint16(b[0:2], p.Tag)
	binary.BigEndian.PutUint16(b[2:4], parameterLength)
	dataEnd := 4 + len(p.Data)
	copy(b[4:dataEnd], p.Data)
	clear(b[dataEnd:marshalLength])
	return nil
}

// Parse decodes given byte sequence as a M3UA Param.
func Parse(b []byte) (*Param, error) {
	p := &Param{}
	if err := p.unmarshalBinaryAtDepth(b, 0); err != nil {
		return nil, err
	}
	return p, nil
}

// UnmarshalBinary sets the values retrieved from byte sequence in a M3UA Param.
func (p *Param) UnmarshalBinary(b []byte) error {
	return p.unmarshalBinaryAtDepth(b, 0)
}

func (p *Param) unmarshalBinaryAtDepth(b []byte, depth int) error {
	*p = Param{}
	l := len(b)
	if l < 4 {
		return ErrTooShortToParse
	}

	tag := binary.BigEndian.Uint16(b[0:2])
	parameterLength := binary.BigEndian.Uint16(b[2:4])
	if int(parameterLength) > l || parameterLength < 4 {
		return ErrInvalidLength
	}
	data := b[4:parameterLength]
	if err := validateValueLength(tag, len(data)); err != nil {
		return err
	}
	if err := validateValueAtDepth(tag, data, depth); err != nil {
		return err
	}

	p.Tag = tag
	p.Length = parameterLength
	p.Data = data
	return nil
}

func (p *Param) currentLengths() (uint16, int, error) {
	if len(p.Data) > maxParamValueLength {
		return 0, 0, fmt.Errorf("%w: tag %#04x value has %d octets, maximum is %d", ErrInvalidLength, p.Tag, len(p.Data), maxParamValueLength)
	}
	if err := validateValueLength(p.Tag, len(p.Data)); err != nil {
		return 0, 0, err
	}
	if err := validateValueAtDepth(p.Tag, p.Data, 0); err != nil {
		return 0, 0, err
	}

	parameterLength := uint16(4 + len(p.Data))
	return parameterLength, p.MarshalLen(), nil
}

func validateValueAtDepth(tag uint16, value []byte, depth int) error {
	switch tag {
	case InfoString:
		if len(value) > 255 {
			return fmt.Errorf("%w: INFO String has %d value octets, maximum is 255", ErrInvalidValue, len(value))
		}
		if !utf8.Valid(value) {
			return fmt.Errorf("%w: INFO String is not valid UTF-8", ErrInvalidValue)
		}
	case TrafficModeType:
		trafficMode := binary.BigEndian.Uint32(value)
		if trafficMode < TrafficModeOverride || trafficMode > TrafficModeBroadcast {
			return fmt.Errorf("%w: Traffic Mode Type %d is not defined", ErrInvalidValue, trafficMode)
		}
	case ErrorCode:
		errorCode := binary.BigEndian.Uint32(value)
		if !isDefinedErrorCode(errorCode) {
			return fmt.Errorf("%w: Error Code %#x is not defined for M3UA", ErrInvalidValue, errorCode)
		}
	case Status:
		status := binary.BigEndian.Uint32(value)
		if !isDefinedStatus(status) {
			return fmt.Errorf("%w: Status Type %#x and Information %#x are not a defined pair", ErrInvalidValue, status>>16, status&0xffff)
		}
	case CongestionIndications:
		congestionLevel := value[3]
		if congestionLevel > 3 {
			return fmt.Errorf("%w: Congestion Level %d is not defined", ErrInvalidValue, congestionLevel)
		}
	case RegistrationStatus:
		registrationStatus := binary.BigEndian.Uint32(value)
		if registrationStatus > RoutingKeyAlreadyRegistered {
			return fmt.Errorf("%w: Registration Status %d is not defined", ErrInvalidValue, registrationStatus)
		}
	case DeregistrationStatus:
		deregistrationStatus := binary.BigEndian.Uint32(value)
		if deregistrationStatus > DeregASPActiveForRoutingContext {
			return fmt.Errorf("%w: Deregistration Status %d is not defined", ErrInvalidValue, deregistrationStatus)
		}
	case DestinationPointCode:
		if value[0] != 0 {
			return fmt.Errorf("%w: Destination Point Code Mask is %d, requires 0", ErrInvalidValue, value[0])
		}
	case RoutingKey:
		if depth > maxNestedContainerDepth {
			return fmt.Errorf("%w: nested container depth exceeds %d", ErrInvalidValue, maxNestedContainerDepth)
		}
		if _, err := parseRoutingKeyPayloadAtDepth(value, depth); err != nil {
			return fmt.Errorf("routing key: %w", err)
		}
	case RegistrationResult:
		if depth > maxNestedContainerDepth {
			return fmt.Errorf("%w: nested container depth exceeds %d", ErrInvalidValue, maxNestedContainerDepth)
		}
		if _, err := parseRegistrationResultPayloadAtDepth(value, depth); err != nil {
			return fmt.Errorf("registration result: %w", err)
		}
	case DeregistrationResult:
		if depth > maxNestedContainerDepth {
			return fmt.Errorf("%w: nested container depth exceeds %d", ErrInvalidValue, maxNestedContainerDepth)
		}
		if _, err := parseDeregResultPayloadAtDepth(value, depth); err != nil {
			return fmt.Errorf("deregistration result: %w", err)
		}
	}
	return nil
}

func isDefinedErrorCode(errorCode uint32) bool {
	switch errorCode {
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
		return true
	default:
		return false
	}
}

func isDefinedStatus(status uint32) bool {
	switch status {
	case AsStateInactive,
		AsStateActive,
		AsStatePending,
		InsufficientAspResources,
		AlternateAspActive,
		AspFailure:
		return true
	default:
		return false
	}
}

func validateValueLength(tag uint16, valueLength int) error {
	valid := true
	requirement := ""

	switch tag {
	case TrafficModeType,
		ErrorCode,
		Status,
		AspIdentifier,
		CorrelationID,
		NetworkAppearance,
		UserCause,
		CongestionIndications,
		ConcernedDestination,
		LocalRoutingKeyIdentifier,
		DestinationPointCode,
		RegistrationStatus,
		DeregistrationStatus:
		valid = valueLength == 4
		requirement = "exactly 4"
	case RoutingContext, AffectedPointCode, OriginatingPointCodeList:
		valid = valueLength > 0 && valueLength%4 == 0
		requirement = "a non-zero multiple of 4"
	case ServiceIndicators:
		valid = valueLength > 0
		requirement = "at least 1"
	case ProtocolData:
		valid = valueLength >= 12
		requirement = "at least 12"
	}

	if !valid {
		return fmt.Errorf("%w: tag %#04x value has %d octets, requires %s", ErrInvalidLength, tag, valueLength, requirement)
	}
	return nil
}

// Padding creates the padded length of a M3UA Param.
func (p *Param) Padding() int {
	x := len(p.Data) % 4
	if x == 0 {
		return 0
	}
	return 4 - x
}

// MarshalLen returns serial length in integer.
func (p *Param) MarshalLen() int {
	return 4 + len(p.Data) + p.Padding()
}

// SetLength sets the length in Length field.
func (p *Param) SetLength() {
	if len(p.Data) > maxParamValueLength {
		p.Length = 0
		return
	}
	p.Length = uint16(4 + len(p.Data))
}

// String creates the M3UA header values in human readable format.
func (p *Param) String() string {
	if p == nil {
		return ""
	}
	return fmt.Sprintf("{Tag: %d, Length: %d, Data: %x}",
		p.Tag,
		p.Length,
		p.Data,
	)
}

// MarshalMultiParams creates the byte sequence from multiple Param instances.
func MarshalMultiParams(params []*Param) ([]byte, error) {
	var b []byte
	for _, param := range params {
		c, err := param.MarshalBinary()
		if err != nil {
			return nil, err
		}
		b = append(b, c...)
	}
	return b, nil
}

// ParseMultiParams decodes multiple Params at a time.
//
// This is easy and useful but slower than decoding one by one.
// When you don't know the number of Params, this is the only way to decode them.
// See benchmarks in diameter_test.go for the detail.
func ParseMultiParams(b []byte) ([]*Param, error) {
	return parseMultiParamsAtDepth(b, 0)
}

func parseMultiParamsAtDepth(b []byte, depth int) ([]*Param, error) {
	var prms []*Param
	for len(b) > 0 {
		p := &Param{}
		if err := p.unmarshalBinaryAtDepth(b, depth); err != nil {
			return nil, err
		}
		prms = append(prms, p)
		parameterLength := int(p.Length)
		if len(b) == parameterLength {
			return prms, nil
		}
		paddedLength := parameterLength + p.Padding()
		if len(b) < paddedLength {
			return nil, ErrInvalidLength
		}

		b = b[paddedLength:]
	}
	return prms, nil
}

// Serialize returns the byte sequence generated from a Param.
//
// DEPRECATED: use MarshalBinary instead.
func (p *Param) Serialize() ([]byte, error) {
	log.Println("DEPRECATED: MarshalBinary instead")
	return p.MarshalBinary()
}

// SerializeTo puts the byte sequence in the byte array given as b.
//
// DEPRECATED: use MarshalTo instead.
func (p *Param) SerializeTo(b []byte) error {
	log.Println("DEPRECATED: MarshalTo instead")
	return p.MarshalTo(b)
}

// Decode decodes given byte sequence as a Param.
//
// DEPRECATED: use Parse instead.
func Decode(b []byte) (*Param, error) {
	log.Println("DEPRECATED: use Parse instead")
	return Parse(b)
}

// DecodeFromBytes sets the values retrieved from byte sequence in a M3UA common header.
//
// DEPRECATED: use UnmarshalBinary instead.
func (p *Param) DecodeFromBytes(b []byte) error {
	log.Println("DEPRECATED: use UnmarshalBinary instead")
	return p.UnmarshalBinary(b)
}

// Len returns the serial length of Param.
//
// DEPRECATED: use MarshalLen instead.
func (p *Param) Len() int {
	log.Println("DEPRECATED: use MarshalLen instead")
	return p.MarshalLen()
}

// SerializeMultiParams creates the byte sequence from multiple Param instances.
//
// DEPRECATED: use MarshalMultiParams instead.
func SerializeMultiParams(params []*Param) ([]byte, error) {
	log.Println("DEPRECATED: use MarshalMultiParams instead")
	return MarshalMultiParams(params)
}

// DecodeMultiParams decodes multiple Params at a time.
//
// This is easy and useful but slower than decoding one by one.
// When you don't know the number of Params, this is the only way to decode them.
// See benchmarks in diameter_test.go for the detail.
//
// DEPRECATED: use ParseMultiParams instead.
func DecodeMultiParams(b []byte) ([]*Param, error) {
	log.Println("DEPRECATED: use ParseMultiParams instead")
	return ParseMultiParams(b)
}
