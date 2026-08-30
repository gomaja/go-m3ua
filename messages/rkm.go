package messages

import (
	"fmt"

	"github.com/gomaja/go-m3ua/messages/params"
)

// RegistrationRequest is the REG REQ message of RFC 4666 Section 3.6.1.
type RegistrationRequest struct {
	*Header
	RoutingKeys []*params.Param
	Others      []*params.Param
}

// NewRegistrationRequest creates an RFC 4666 Registration Request.
func NewRegistrationRequest(routingKeys ...*params.Param) *RegistrationRequest {
	message := &RegistrationRequest{
		Header:      newRKMHeader(MsgTypeRegistrationRequest),
		RoutingKeys: append([]*params.Param(nil), routingKeys...),
	}
	message.SetLength()
	return message
}

// MarshalBinary returns the wire representation of the Registration Request.
func (message *RegistrationRequest) MarshalBinary() ([]byte, error) {
	return marshalRKMMessage(message.Header, message.parameters(), message.validate)
}

// MarshalTo writes the Registration Request to b.
func (message *RegistrationRequest) MarshalTo(b []byte) error {
	return marshalRKMMessageTo(message.Header, message.parameters(), message.validate, b)
}

// ParseRegistrationRequest parses an RFC 4666 Registration Request.
func ParseRegistrationRequest(b []byte) (*RegistrationRequest, error) {
	message := &RegistrationRequest{}
	if err := message.UnmarshalBinary(b); err != nil {
		return nil, err
	}
	return message, nil
}

// UnmarshalBinary parses an RFC 4666 Registration Request.
func (message *RegistrationRequest) UnmarshalBinary(b []byte) error {
	*message = RegistrationRequest{}
	header, parameters, err := parseRKMParameters(b, MsgTypeRegistrationRequest)
	if err != nil {
		return err
	}
	message.Header = header
	for _, parameter := range parameters {
		if parameter.Tag == params.RoutingKey {
			message.RoutingKeys = append(message.RoutingKeys, parameter)
		} else {
			if isKnownM3UAParameterTag(parameter.Tag) {
				return fmt.Errorf("%w: Registration Request parameter tag %#04x", ErrInvalidParameter, parameter.Tag)
			}
			message.Others = append(message.Others, parameter)
		}
	}
	return message.validate()
}

func (message *RegistrationRequest) validate() error {
	if err := validateRepeatedRKMParameter(message.RoutingKeys, params.RoutingKey, "Routing Key"); err != nil {
		return err
	}
	return validateRKMExtensions(message.Others, "Registration Request")
}

func (message *RegistrationRequest) parameters() []*params.Param {
	return appendRKMParameters(message.RoutingKeys, message.Others)
}

// SetLength updates the Registration Request message and parameter lengths.
func (message *RegistrationRequest) SetLength() {
	setRKMMessageLength(message.Header, message.parameters())
}

// MarshalLen returns the Registration Request wire length.
func (message *RegistrationRequest) MarshalLen() int {
	return rkmMessageMarshalLen(message.parameters())
}

// Version returns the M3UA version.
func (message *RegistrationRequest) Version() uint8 { return message.Header.Version }

// MessageClass returns the RKM message class.
func (message *RegistrationRequest) MessageClass() uint8 { return MsgClassRKM }

// MessageType returns the Registration Request message type.
func (message *RegistrationRequest) MessageType() uint8 { return MsgTypeRegistrationRequest }

// MessageClassName returns the RKM message class name.
func (message *RegistrationRequest) MessageClassName() string { return MsgClassNameRKM }

// MessageTypeName returns the RFC message name.
func (message *RegistrationRequest) MessageTypeName() string { return "Registration Request" }

// String returns a human-readable Registration Request.
func (message *RegistrationRequest) String() string {
	if message == nil {
		return ""
	}
	return fmt.Sprintf("{Header: %s, RoutingKeys: %v, Others: %v}", message.Header, message.RoutingKeys, message.Others)
}

// RegistrationResponse is the REG RSP message of RFC 4666 Section 3.6.2.
type RegistrationResponse struct {
	*Header
	RegistrationResults []*params.Param
	Others              []*params.Param
}

// NewRegistrationResponse creates an RFC 4666 Registration Response.
func NewRegistrationResponse(results ...*params.Param) *RegistrationResponse {
	message := &RegistrationResponse{
		Header:              newRKMHeader(MsgTypeRegistrationResponse),
		RegistrationResults: append([]*params.Param(nil), results...),
	}
	message.SetLength()
	return message
}

// MarshalBinary returns the wire representation of the Registration Response.
func (message *RegistrationResponse) MarshalBinary() ([]byte, error) {
	return marshalRKMMessage(message.Header, message.parameters(), message.validate)
}

// MarshalTo writes the Registration Response to b.
func (message *RegistrationResponse) MarshalTo(b []byte) error {
	return marshalRKMMessageTo(message.Header, message.parameters(), message.validate, b)
}

// ParseRegistrationResponse parses an RFC 4666 Registration Response.
func ParseRegistrationResponse(b []byte) (*RegistrationResponse, error) {
	message := &RegistrationResponse{}
	if err := message.UnmarshalBinary(b); err != nil {
		return nil, err
	}
	return message, nil
}

// UnmarshalBinary parses an RFC 4666 Registration Response.
func (message *RegistrationResponse) UnmarshalBinary(b []byte) error {
	*message = RegistrationResponse{}
	header, parameters, err := parseRKMParameters(b, MsgTypeRegistrationResponse)
	if err != nil {
		return err
	}
	message.Header = header
	for _, parameter := range parameters {
		if parameter.Tag == params.RegistrationResult {
			message.RegistrationResults = append(message.RegistrationResults, parameter)
		} else {
			if isKnownM3UAParameterTag(parameter.Tag) {
				return fmt.Errorf("%w: Registration Response parameter tag %#04x", ErrInvalidParameter, parameter.Tag)
			}
			message.Others = append(message.Others, parameter)
		}
	}
	return message.validate()
}

func (message *RegistrationResponse) validate() error {
	if err := validateRepeatedRKMParameter(message.RegistrationResults, params.RegistrationResult, "Registration Result"); err != nil {
		return err
	}
	return validateRKMExtensions(message.Others, "Registration Response")
}

func (message *RegistrationResponse) parameters() []*params.Param {
	return appendRKMParameters(message.RegistrationResults, message.Others)
}

// SetLength updates the Registration Response message and parameter lengths.
func (message *RegistrationResponse) SetLength() {
	setRKMMessageLength(message.Header, message.parameters())
}

// MarshalLen returns the Registration Response wire length.
func (message *RegistrationResponse) MarshalLen() int {
	return rkmMessageMarshalLen(message.parameters())
}

// Version returns the M3UA version.
func (message *RegistrationResponse) Version() uint8 { return message.Header.Version }

// MessageClass returns the RKM message class.
func (message *RegistrationResponse) MessageClass() uint8 { return MsgClassRKM }

// MessageType returns the Registration Response message type.
func (message *RegistrationResponse) MessageType() uint8 { return MsgTypeRegistrationResponse }

// MessageClassName returns the RKM message class name.
func (message *RegistrationResponse) MessageClassName() string { return MsgClassNameRKM }

// MessageTypeName returns the RFC message name.
func (message *RegistrationResponse) MessageTypeName() string { return "Registration Response" }

// String returns a human-readable Registration Response.
func (message *RegistrationResponse) String() string {
	if message == nil {
		return ""
	}
	return fmt.Sprintf("{Header: %s, RegistrationResults: %v, Others: %v}", message.Header, message.RegistrationResults, message.Others)
}

// DeregistrationRequest is the DEREG REQ message of RFC 4666 Section 3.6.3.
type DeregistrationRequest struct {
	*Header
	RoutingContext *params.Param
	Others         []*params.Param
}

// NewDeregistrationRequest creates an RFC 4666 Deregistration Request.
func NewDeregistrationRequest(routingContext *params.Param) *DeregistrationRequest {
	message := &DeregistrationRequest{
		Header:         newRKMHeader(MsgTypeDeregistrationRequest),
		RoutingContext: routingContext,
	}
	message.SetLength()
	return message
}

// MarshalBinary returns the wire representation of the Deregistration Request.
func (message *DeregistrationRequest) MarshalBinary() ([]byte, error) {
	return marshalRKMMessage(message.Header, message.parameters(), message.validate)
}

// MarshalTo writes the Deregistration Request to b.
func (message *DeregistrationRequest) MarshalTo(b []byte) error {
	return marshalRKMMessageTo(message.Header, message.parameters(), message.validate, b)
}

// ParseDeregistrationRequest parses an RFC 4666 Deregistration Request.
func ParseDeregistrationRequest(b []byte) (*DeregistrationRequest, error) {
	message := &DeregistrationRequest{}
	if err := message.UnmarshalBinary(b); err != nil {
		return nil, err
	}
	return message, nil
}

// UnmarshalBinary parses an RFC 4666 Deregistration Request.
func (message *DeregistrationRequest) UnmarshalBinary(b []byte) error {
	*message = DeregistrationRequest{}
	header, parameters, err := parseRKMParameters(b, MsgTypeDeregistrationRequest)
	if err != nil {
		return err
	}
	message.Header = header
	for _, parameter := range parameters {
		if parameter.Tag == params.RoutingContext {
			if message.RoutingContext != nil {
				return fmt.Errorf("%w: duplicate Routing Context", ErrInvalidParameter)
			}
			message.RoutingContext = parameter
		} else {
			if isKnownM3UAParameterTag(parameter.Tag) {
				return fmt.Errorf("%w: Deregistration Request parameter tag %#04x", ErrInvalidParameter, parameter.Tag)
			}
			message.Others = append(message.Others, parameter)
		}
	}
	return message.validate()
}

func (message *DeregistrationRequest) validate() error {
	if err := requireParameter(message.RoutingContext, "Routing Context"); err != nil {
		return err
	}
	if message.RoutingContext.Tag != params.RoutingContext {
		return fmt.Errorf("%w: expected Routing Context", ErrInvalidParameter)
	}
	return validateRKMExtensions(message.Others, "Deregistration Request")
}

func (message *DeregistrationRequest) parameters() []*params.Param {
	parameters := make([]*params.Param, 0, 1+len(message.Others))
	parameters = append(parameters, message.RoutingContext)
	return append(parameters, message.Others...)
}

// SetLength updates the Deregistration Request message and parameter lengths.
func (message *DeregistrationRequest) SetLength() {
	setRKMMessageLength(message.Header, message.parameters())
}

// MarshalLen returns the Deregistration Request wire length.
func (message *DeregistrationRequest) MarshalLen() int {
	return rkmMessageMarshalLen(message.parameters())
}

// Version returns the M3UA version.
func (message *DeregistrationRequest) Version() uint8 { return message.Header.Version }

// MessageClass returns the RKM message class.
func (message *DeregistrationRequest) MessageClass() uint8 { return MsgClassRKM }

// MessageType returns the Deregistration Request message type.
func (message *DeregistrationRequest) MessageType() uint8 { return MsgTypeDeregistrationRequest }

// MessageClassName returns the RKM message class name.
func (message *DeregistrationRequest) MessageClassName() string { return MsgClassNameRKM }

// MessageTypeName returns the RFC message name.
func (message *DeregistrationRequest) MessageTypeName() string { return "Deregistration Request" }

// String returns a human-readable Deregistration Request.
func (message *DeregistrationRequest) String() string {
	if message == nil {
		return ""
	}
	return fmt.Sprintf("{Header: %s, RoutingContext: %v, Others: %v}", message.Header, message.RoutingContext, message.Others)
}

// DeregistrationResponse is the DEREG RSP message of RFC 4666 Section 3.6.4.
type DeregistrationResponse struct {
	*Header
	DeregistrationResults []*params.Param
	Others                []*params.Param
}

// NewDeregistrationResponse creates an RFC 4666 Deregistration Response.
func NewDeregistrationResponse(results ...*params.Param) *DeregistrationResponse {
	message := &DeregistrationResponse{
		Header:                newRKMHeader(MsgTypeDeregistrationResponse),
		DeregistrationResults: append([]*params.Param(nil), results...),
	}
	message.SetLength()
	return message
}

// MarshalBinary returns the wire representation of the Deregistration Response.
func (message *DeregistrationResponse) MarshalBinary() ([]byte, error) {
	return marshalRKMMessage(message.Header, message.parameters(), message.validate)
}

// MarshalTo writes the Deregistration Response to b.
func (message *DeregistrationResponse) MarshalTo(b []byte) error {
	return marshalRKMMessageTo(message.Header, message.parameters(), message.validate, b)
}

// ParseDeregistrationResponse parses an RFC 4666 Deregistration Response.
func ParseDeregistrationResponse(b []byte) (*DeregistrationResponse, error) {
	message := &DeregistrationResponse{}
	if err := message.UnmarshalBinary(b); err != nil {
		return nil, err
	}
	return message, nil
}

// UnmarshalBinary parses an RFC 4666 Deregistration Response.
func (message *DeregistrationResponse) UnmarshalBinary(b []byte) error {
	*message = DeregistrationResponse{}
	header, parameters, err := parseRKMParameters(b, MsgTypeDeregistrationResponse)
	if err != nil {
		return err
	}
	message.Header = header
	for _, parameter := range parameters {
		if parameter.Tag == params.DeregistrationResult {
			message.DeregistrationResults = append(message.DeregistrationResults, parameter)
		} else {
			if isKnownM3UAParameterTag(parameter.Tag) {
				return fmt.Errorf("%w: Deregistration Response parameter tag %#04x", ErrInvalidParameter, parameter.Tag)
			}
			message.Others = append(message.Others, parameter)
		}
	}
	return message.validate()
}

func (message *DeregistrationResponse) validate() error {
	if err := validateRepeatedRKMParameter(message.DeregistrationResults, params.DeregistrationResult, "Deregistration Result"); err != nil {
		return err
	}
	return validateRKMExtensions(message.Others, "Deregistration Response")
}

func (message *DeregistrationResponse) parameters() []*params.Param {
	return appendRKMParameters(message.DeregistrationResults, message.Others)
}

// SetLength updates the Deregistration Response message and parameter lengths.
func (message *DeregistrationResponse) SetLength() {
	setRKMMessageLength(message.Header, message.parameters())
}

// MarshalLen returns the Deregistration Response wire length.
func (message *DeregistrationResponse) MarshalLen() int {
	return rkmMessageMarshalLen(message.parameters())
}

// Version returns the M3UA version.
func (message *DeregistrationResponse) Version() uint8 { return message.Header.Version }

// MessageClass returns the RKM message class.
func (message *DeregistrationResponse) MessageClass() uint8 { return MsgClassRKM }

// MessageType returns the Deregistration Response message type.
func (message *DeregistrationResponse) MessageType() uint8 { return MsgTypeDeregistrationResponse }

// MessageClassName returns the RKM message class name.
func (message *DeregistrationResponse) MessageClassName() string { return MsgClassNameRKM }

// MessageTypeName returns the RFC message name.
func (message *DeregistrationResponse) MessageTypeName() string { return "Deregistration Response" }

// String returns a human-readable Deregistration Response.
func (message *DeregistrationResponse) String() string {
	if message == nil {
		return ""
	}
	return fmt.Sprintf("{Header: %s, DeregistrationResults: %v, Others: %v}", message.Header, message.DeregistrationResults, message.Others)
}

func newRKMHeader(messageType uint8) *Header {
	return &Header{Version: 1, Class: MsgClassRKM, Type: messageType}
}

func parseRKMParameters(b []byte, messageType uint8) (*Header, []*params.Param, error) {
	header, err := parseTypedHeader(b, MsgClassRKM, messageType)
	if err != nil {
		return nil, nil, err
	}
	parameters, err := params.ParseMultiParams(header.Payload)
	if err != nil {
		return nil, nil, err
	}
	return header, parameters, nil
}

func validateRepeatedRKMParameter(parameters []*params.Param, tag uint16, name string) error {
	count := 0
	for _, parameter := range parameters {
		if parameter == nil {
			continue
		}
		count++
		if parameter.Tag != tag {
			return fmt.Errorf("%w: expected %s", ErrInvalidParameter, name)
		}
	}
	if count == 0 {
		return fmt.Errorf("%w: %s", ErrMissingParameter, name)
	}
	return nil
}

func validateRKMExtensions(parameters []*params.Param, messageName string) error {
	for index, parameter := range parameters {
		if parameter == nil {
			continue
		}
		if isKnownM3UAParameterTag(parameter.Tag) {
			return fmt.Errorf(
				"%w: %s Others[%d] contains known parameter tag %#04x",
				ErrInvalidParameter,
				messageName,
				index,
				parameter.Tag,
			)
		}
	}
	return nil
}

func isKnownM3UAParameterTag(tag uint16) bool {
	switch tag {
	case params.InfoString,
		params.RoutingContext,
		params.DiagnosticInformation,
		params.HeartbeatData,
		params.TrafficModeType,
		params.ErrorCode,
		params.Status,
		params.AspIdentifier,
		params.AffectedPointCode,
		params.CorrelationID,
		params.NetworkAppearance,
		params.UserCause,
		params.CongestionIndications,
		params.ConcernedDestination,
		params.RoutingKey,
		params.RegistrationResult,
		params.DeregistrationResult,
		params.LocalRoutingKeyIdentifier,
		params.DestinationPointCode,
		params.ServiceIndicators,
		params.OriginatingPointCodeList,
		params.ProtocolData,
		params.RegistrationStatus,
		params.DeregistrationStatus:
		return true
	default:
		return false
	}
}

func appendRKMParameters(required, others []*params.Param) []*params.Param {
	parameters := make([]*params.Param, 0, len(required)+len(others))
	parameters = append(parameters, required...)
	return append(parameters, others...)
}

func marshalRKMMessage(header *Header, parameters []*params.Param, validate func() error) ([]byte, error) {
	message := make([]byte, rkmMessageMarshalLen(parameters))
	if err := marshalRKMMessageTo(header, parameters, validate, message); err != nil {
		return nil, err
	}
	return message, nil
}

func marshalRKMMessageTo(header *Header, parameters []*params.Param, validate func() error, b []byte) error {
	if err := validate(); err != nil {
		return err
	}
	if header == nil {
		return ErrTooShortToMarshalBinary
	}
	length := rkmMessageMarshalLen(parameters)
	if len(b) < length {
		return ErrTooShortToMarshalBinary
	}
	setParamLengths(parameters)
	header.Length = uint32(length)
	header.Payload = make([]byte, length-8)
	if err := marshalOtherParams(header.Payload, 0, parameters); err != nil {
		return err
	}
	return header.MarshalTo(b)
}

func setRKMMessageLength(header *Header, parameters []*params.Param) {
	if header == nil {
		return
	}
	setParamLengths(parameters)
	header.Length = uint32(rkmMessageMarshalLen(parameters))
}

func rkmMessageMarshalLen(parameters []*params.Param) int {
	return 8 + paramsMarshalLen(parameters)
}
