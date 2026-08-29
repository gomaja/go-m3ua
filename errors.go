// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

// AssociationEstablishmentError reports a peer-specific failure after an SCTP
// association was accepted but before its M3UA procedures became established.
// Listener.Accept returns transport-listener failures directly, so callers can
// continue after this error without masking a failed listening socket.
type AssociationEstablishmentError struct {
	RemoteAddr *sctp.SCTPAddr
	Err        error
}

func (e *AssociationEstablishmentError) Error() string {
	if e == nil || e.Err == nil {
		return "M3UA association establishment failed"
	}
	if e.RemoteAddr == nil {
		return fmt.Sprintf("M3UA association establishment failed: %v", e.Err)
	}
	return fmt.Sprintf("M3UA association establishment with %s failed: %v", e.RemoteAddr, e.Err)
}

func (e *AssociationEstablishmentError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// Error definitions.
var (
	ErrSCTPNotAlive      = errors.New("SCTP is no longer alive")
	ErrInvalidState      = errors.New("invalid state")
	ErrNotEstablished    = errors.New("M3UA association not established")
	ErrFailedToEstablish = errors.New("failed to establish M3UA association")
	ErrTimeout           = errors.New("timed out")
	// ErrHeartbeatExpired reports expiry of the RFC 4666 M3UA T(beat) liveness
	// timer. It is unrelated to SCTP HEARTBEAT path failure reporting.
	ErrHeartbeatExpired    = errors.New("heartbeat timer expired")
	ErrFailedToPeelOff     = errors.New("failed to peel off Protocol Data")
	ErrFailedToWriteSignal = errors.New("failed to write signal")

	// ErrManagementBlocking reports that this SGP cannot service the request
	// because it is isolated from its nodal interworking function.
	//
	// RFC 4666 Section 4.7: "Upon receiving an ASP Up message while isolated
	// from the NIF, the SGP should respond with an Error ("Refused -
	// Management Blocking")", and the same for an ASP Active naming an
	// Application Server the SGP can no longer service.
	ErrManagementBlocking = errors.New("refused: isolated from the nodal interworking function")

	// ErrRoutingContextNotActive is returned when DATA is written for a Routing
	// Context that is inactive for this association.
	//
	// RFC 4666 Section 4.3.4.3 has the SGP answer "For the Application Servers
	// for which the ASP can be activated", so a partial acknowledgement leaves
	// the rest inactive and traffic for them has nowhere to go.
	ErrRoutingContextNotActive = errors.New("routing context is not active for this association")

	// ErrInvalidParameterValue is used when a parameter is well formed but
	// carries a value the message does not permit.
	//
	// RFC 4666 Section 3.8.1 gives the DUPU case as its own example: "The
	// 'Invalid Parameter Value' error is sent if a message is received with an
	// invalid parameter value (e.g., a DUPU message was received with a Mask
	// value other than \"0\"."
	ErrInvalidParameterValue = errors.New("parameter carries a value this message does not permit")

	// ErrAmbiguousRoutingContext is returned when DATA is written on an
	// association that carries several Routing Contexts and none has been
	// chosen with SelectRoutingContext.
	//
	// RFC 4666 Section 3.3.1 requires the Routing Context "to identify the
	// traffic flow" in exactly this case, and which flow a payload belongs to
	// is the caller's knowledge. Sending every configured context instead
	// identifies none of them.
	ErrAmbiguousRoutingContext = errors.New("several Routing Contexts configured and none selected")

	// ErrMissingProtocolData is used when a DATA message arrives without the
	// Protocol Data parameter, which RFC 4666 Section 3.3.1 lists as Mandatory.
	ErrMissingProtocolData = errors.New("DATA without Protocol Data parameter")

	// ErrMissingRoutingContext is used when DATA or SSNM omits its Routing
	// Context on an association carrying multiple Routing Keys. Their RFC 4666
	// message formats make the otherwise Conditional parameter mandatory in
	// that situation so the receiver can identify the concerned traffic flow.
	ErrMissingRoutingContext = errors.New("routing context required on a multi-flow association")

	// ErrMissingStatus is used when a Notify arrives without the Status
	// parameter, which RFC 4666 Section 3.8.2 lists as Mandatory.
	ErrMissingStatus = errors.New("notify without Status parameter")

	// ErrASPIdentifierRequired is used by an SGP in response to an ASP Up message that
	// does not contain an ASP Identifier parameter when the SGP requires one.
	ErrASPIdentifierRequired = errors.New("ASP Identifier required")

	// ErrInvalidASPIdentifier is returned when another ASP supporting the same
	// Application Server already claimed the identifier supplied in ASP Up.
	ErrInvalidASPIdentifier = errors.New("non-unique ASP identifier")

	// ErrUnsupportedTrafficMode is used by an SGP whose configured traffic
	// handling mode is incompatible with the one a peer requested in ASP
	// Active, per RFC 4666 Section 4.3.4.3.
	ErrUnsupportedTrafficMode = errors.New("unsupported traffic handling mode")

	// ErrMessageTooLarge is used when a peer sends a message that does not fit
	// in the read buffer. Raise SCTPConfig.ReadBufferSize to accept it.
	ErrMessageTooLarge = errors.New("received message larger than the read buffer")

	// ErrMissingAffectedPointCode is used when an SSNM message arrives without
	// the Affected Point Code parameter, which RFC 4666 Sections 3.4.1 to
	// 3.4.6 list as Mandatory in every SSNM message.
	ErrMissingAffectedPointCode = errors.New("SSNM message without Affected Point Code parameter")

	// ErrInvalidRoutingContext is used when a peer names a Routing Context
	// this association is not configured for, per RFC 4666 Section 3.8.1.
	// RoutingContextError matches it through errors.Is.
	ErrInvalidRoutingContext = errors.New("invalid routing context")

	// ErrInvalidNetworkAppearance is used when an ASP names a network the SGP
	// has not configured, per RFC 4666 Section 3.8.1. NetworkAppearanceError
	// matches it through errors.Is and carries the value that must be returned
	// to the ASP.
	ErrInvalidNetworkAppearance = errors.New("invalid network appearance")

	// ErrInitTimeout is used when one SCTP association attempt did not complete
	// within AssociationConfig.InitTimeout. Dial does not retry; the caller decides
	// whether to attempt again.
	ErrInitTimeout = errors.New("SCTP association attempt timed out")

	// ErrAssociationClosed is reported by Association.Err when the association
	// ended because the owner closed it, rather than through any failure.
	ErrAssociationClosed = errors.New("association closed by the local endpoint")

	// ErrNilAssociationConfig reports a Dial without the immutable M3UA policy
	// required to construct the Association.
	ErrNilAssociationConfig = errors.New("nil M3UA AssociationConfig")

	// ErrInvalidRoleConfiguration reports an AssociationConfig field that has
	// no RFC 4666 meaning for the selected endpoint role.
	ErrInvalidRoleConfiguration = errors.New("invalid M3UA role configuration")

	// ErrInvalidASPConfig reports an invalid ASP-wide MTP Route, Signalling
	// Gateway, Signalling Gateway Process, or route inventory.
	ErrInvalidASPConfig = errors.New("invalid ASP routing configuration")

	// ErrInvalidMTPTransfer reports an invalid MTP-TRANSFER request primitive.
	ErrInvalidMTPTransfer = errors.New("invalid MTP-TRANSFER request")
	// ErrUnknownMTPRoute reports an explicit MTP Route not provisioned at
	// the ASP Endpoint.
	ErrUnknownMTPRoute = errors.New("unknown MTP Route")
	// ErrNoMatchingMTPRoute reports a routing label outside every provisioned
	// ASP MTP Route.
	ErrNoMatchingMTPRoute = errors.New("no MTP Route matches the MTP routing label")
	// ErrAmbiguousMTPRoute reports several equally specific MTP Routes for an
	// MTP routing label whose request omitted an explicit MTP Route.
	ErrAmbiguousMTPRoute = errors.New("MTP routing label matches several MTP Routes")
	// ErrMTPTransferOutsideRoute reports that an explicit MTP Route does
	// not match the request's MTP routing label.
	ErrMTPTransferOutsideRoute = errors.New("MTP routing label is outside the selected MTP Route")
	// ErrNoMTPRoute reports that no active, reachable, policy-permitted SGP
	// Association can carry an MTP-TRANSFER request.
	ErrNoMTPRoute = errors.New("no eligible MTP route")
	// ErrMissingSGPIdentity reports an ASP Association without the provisioned
	// remote SGP identity required for Endpoint route selection.
	ErrMissingSGPIdentity = errors.New("ASP Association has no SGP identity")
	// ErrUnknownSGP reports an ASP Association naming an SGP not provisioned at
	// its Endpoint.
	ErrUnknownSGP = errors.New("ASP Association names an unknown SGP")
	// ErrSGPRouteScopeMismatch reports Network Appearance or Routing Context
	// configuration that is not a provisioned route of the named SGP.
	ErrSGPRouteScopeMismatch = errors.New("ASP Association scope is not provisioned for its SGP")
	// ErrASPRouteStateLimit reports that a peer SSNM message would exceed the
	// configured ASP route-state work or retention budget.
	ErrASPRouteStateLimit = errors.New("ASP SSNM route-state limit exceeded")

	// ErrMissingUserCause is used when a DUPU arrives without the User/Cause
	// parameter, which RFC 4666 Section 3.4.5 lists as Mandatory.
	ErrMissingUserCause = errors.New("DUPU message without User/Cause parameter")

	// ErrDataQueueFull is used when a received DATA payload cannot be queued
	// because the application is not reading. It is reported, never fatal: the
	// association is healthy and the peer is answerable, which is precisely
	// what would be lost by blocking instead.
	ErrDataQueueFull = errors.New("inbound DATA queue full: payload discarded")

	// ErrRecoveryQueueFull reports that an AS-PENDING recovery queue reached
	// its configured message or byte bound. Existing queued traffic is kept;
	// the new message is not partially retained.
	ErrRecoveryQueueFull = errors.New("AS recovery queue full")

	// ErrBroadcastFlowIdentifierTooLong reports an application classifier result
	// that would make a retained Broadcast flow key exceed its configured cap.
	ErrBroadcastFlowIdentifierTooLong = errors.New("broadcast flow identifier too long")

	// ErrNotificationQueueFull reports a peer whose blocked control stream has
	// exhausted the bounded mandatory-control queue. The association is closed.
	ErrNotificationQueueFull = errors.New("mandatory control queue full")

	// ErrIndicationQueueFull reports an association whose application stopped
	// consuming lossless Layer Management indications. The association is
	// closed rather than silently losing a state or management event.
	ErrIndicationQueueFull = errors.New("layer management indication queue full")

	// ErrNoActiveASP reports that an Application Server is neither able to
	// deliver DATA now nor in AS-PENDING where T(r) permits it to be queued.
	ErrNoActiveASP = errors.New("application server has no active ASP")

	// ErrSackDelayTooLarge is used when a delayed-SACK timer above the ceiling
	// RFC 9260 Section 6.2 sets is asked for:
	//
	//	An implementation MUST NOT allow the maximum delay (protocol
	//	parameter 'SACK.Delay') to be configured to be more than 500 ms.
	//	In other words, an implementation MAY lower the value of
	//	'SACK.Delay' below 500 ms but MUST NOT raise it above 500 ms.
	//
	// The value used to go straight to setsockopt. Linux enforces this ceiling
	// itself — measured on 6.x, sack_delay up to 500 is accepted and 501 and
	// above return EINVAL — so the association was never actually configured
	// beyond it there. What the caller got was "failed to set sack timer:
	// invalid argument", indistinguishable from every other setsockopt failure
	// and impossible to match on, and the conformance depended on the host
	// stack rather than on this package. A stack that does not enforce it would
	// have applied the value.
	ErrSackDelayTooLarge = errors.New("SACK delay above the 500 ms ceiling of RFC 9260 Section 6.2")

	// ErrNoDataStream is used when there is no SCTP stream DATA may travel on.
	// RFC 4666 Section 1.4.7 rule 1: "The DATA message MUST NOT be sent on
	// stream 0", so a peer that negotiates a single outbound stream leaves the
	// association with nowhere legal to carry traffic.
	ErrNoDataStream = errors.New("no SCTP stream available for DATA: stream 0 is reserved")

	// ErrNoConfiguredAS is used when a peer asks to activate a Routing Context
	// for which no Routing Key is defined. RFC 4666 Section 4.3.4.3: "If the RC
	// parameter is included in the ASP Active message and a corresponding RK
	// has not been previously defined [...] the peer MUST respond with an ERROR
	// message with the Error Code 'No configured AS for ASP'."
	// RoutingContextError matches it through errors.Is.
	ErrNoConfiguredAS = errors.New("no configured AS for ASP")

	// ErrTAckExpired is used when an ASPSM/ASPTM request went unacknowledged
	// through every T(ack) retry, so the peer is not completing the handshake.
	ErrTAckExpired = errors.New("T(ack) expired: peer never acknowledged the request")

	// ErrUnsupportedRole reports a role for which the requested association
	// operation has no protocol procedures. IPSP roles require an explicit
	// Single Exchange model or Double Exchange model and are enabled by the
	// IPSP API rather than guessed.
	ErrUnsupportedRole = errors.New("unsupported M3UA role")
	// ErrUnsupportedIPSPExchangeModel reports an IPSP Association configured
	// with an exchange model this implementation does not provide.
	ErrUnsupportedIPSPExchangeModel = errors.New("unsupported IPSP exchange model")
	// ErrEndpointClosed reports an operation started after Endpoint.Close or
	// interrupted because the owning Endpoint closed.
	ErrEndpointClosed = errors.New("M3UA endpoint is closed")
)

// InvalidVersionError is used if a message with an unsupported version is received.
type InvalidVersionError struct {
	Ver uint8
}

// NewInvalidVersionError creates InvalidVersionError.
func NewInvalidVersionError(ver uint8) *InvalidVersionError {
	return &InvalidVersionError{Ver: ver}
}

// Error returns error string with violating version.
func (e *InvalidVersionError) Error() string {
	return fmt.Sprintf("invalid version: %d", e.Ver)
}

// invalidVersionEvent keeps wire metadata private so adding it does not break
// source compatibility for users that construct InvalidVersionError values.
// Unwrap preserves errors.As checks for the public error type.
type invalidVersionEvent struct {
	err *InvalidVersionError
	raw []byte
}

func newInvalidVersionErrorFor(ver uint8, raw []byte) error {
	return &invalidVersionEvent{
		err: NewInvalidVersionError(ver),
		raw: bytes.Clone(raw),
	}
}

func (e *invalidVersionEvent) Error() string { return e.err.Error() }

func (e *invalidVersionEvent) Unwrap() error { return e.err }

// UnsupportedClassError is used if a message with an unexpected or
// unsupported Message Class is received.
type UnsupportedClassError struct {
	// Raw is the message as received, when it could not be parsed.
	Raw []byte
	Msg messages.M3UA
}

// NewUnsupportedClassError creates UnsupportedClassError
func NewUnsupportedClassError(msg messages.M3UA) *UnsupportedClassError {
	return &UnsupportedClassError{Msg: msg}
}

// NewUnsupportedClassErrorFor reports an unsupported class for a message that
// could not be parsed, carrying the octets received so the Diagnostic
// Information parameter can quote them (RFC 4666 Section 3.8.1).
func NewUnsupportedClassErrorFor(raw []byte) *UnsupportedClassError {
	return &UnsupportedClassError{Raw: bytes.Clone(raw)}
}

func newUnsupportedClassErrorForMessage(msg messages.M3UA, raw []byte) *UnsupportedClassError {
	return &UnsupportedClassError{Raw: bytes.Clone(raw), Msg: msg}
}

// Error returns error string with message class.
func (e *UnsupportedClassError) Error() string {
	if e.Msg == nil {
		return fmt.Sprintf("message class unsupported. class: %d", rawClass(e.Raw))
	}
	return fmt.Sprintf("message class unsupported. class: %s", e.Msg.MessageClassName())
}

func (e *UnsupportedClassError) first40Octets() []byte {
	return first40(e.Raw, e.Msg)
}

// UnsupportedMessageError is used if a message with an
// unexpected or unsupported Message Type is received.
type UnsupportedMessageError struct {
	// Raw is the message as received, when it could not be parsed.
	Raw []byte
	Msg messages.M3UA
}

// NewUnsupportedMessageError creates UnsupportedMessageError
func NewUnsupportedMessageError(msg messages.M3UA) *UnsupportedMessageError {
	return &UnsupportedMessageError{Msg: msg}
}

// NewUnsupportedMessageErrorFor reports an unsupported type for a message that
// could not be parsed, carrying the octets received.
func NewUnsupportedMessageErrorFor(raw []byte) *UnsupportedMessageError {
	return &UnsupportedMessageError{Raw: bytes.Clone(raw)}
}

func newUnsupportedMessageErrorForMessage(msg messages.M3UA, raw []byte) *UnsupportedMessageError {
	return &UnsupportedMessageError{Raw: bytes.Clone(raw), Msg: msg}
}

// Error returns error string with message class and type.
func (e *UnsupportedMessageError) Error() string {
	if e.Msg == nil {
		return fmt.Sprintf("message unsupported. class: %d, type: %d", rawClass(e.Raw), rawType(e.Raw))
	}
	return fmt.Sprintf("message unsupported. class: %s, type: %s", e.Msg.MessageClassName(), e.Msg.MessageTypeName())
}

func (e *UnsupportedMessageError) first40Octets() []byte {
	return first40(e.Raw, e.Msg)
}

// ParameterFaultError is used when a message this package implements cannot be
// decoded because of its parameters rather than its class or type.
//
// RFC 4666 Section 3.8.1 keeps the two apart. An unsupported type tells the peer
// to stop sending that message altogether; a parameter fault complains about one
// message. The section names the codes:
//
//	The "Parameter Field Error" would be sent if a message is received
//	with a parameter having a wrong length field.
//
//	The "Unexpected Parameter" error would be sent if a message contains
//	an invalid parameter.
type ParameterFaultError struct {
	// Raw is the message as received; it could not be parsed, so there is no
	// decoded form to carry.
	Raw []byte
	// Code is the RFC 4666 Section 3.8.1 error code to report.
	Code uint32
	// Cause is the decode failure this was derived from.
	Cause error
}

// NewParameterFaultErrorFor reports a parameter-level decode failure for a
// message whose class and type this package implements, choosing the error code
// the fault calls for.
func NewParameterFaultErrorFor(raw []byte, cause error) *ParameterFaultError {
	// A length field that does not describe the octets present is precisely
	// "a parameter having a wrong length field". The common header's Message
	// Length is not a parameter field, so a contradiction there is the broader
	// Protocol Error. Anything else that stops a known message decoding is an
	// invalid parameter.
	code := uint32(params.ErrUnexpectedParameter)
	switch {
	case errors.Is(cause, messages.ErrMissingParameter):
		code = params.ErrMissingParameter
	case errors.Is(cause, params.ErrInvalidValue):
		code = params.ErrInvalidParameterValue
	case errors.Is(cause, messages.ErrInvalidMessageLength):
		code = params.ErrProtocolError
	case errors.Is(cause, params.ErrInvalidLength):
		code = params.ErrParameterFieldError
	}
	return &ParameterFaultError{Raw: bytes.Clone(raw), Code: code, Cause: cause}
}

// Error returns the error string with the offending message's class and type.
func (e *ParameterFaultError) Error() string {
	return fmt.Sprintf("parameter fault in message. class: %d, type: %d: %v",
		rawClass(e.Raw), rawType(e.Raw), e.Cause)
}

// Unwrap exposes the decode failure this was derived from.
func (e *ParameterFaultError) Unwrap() error { return e.Cause }

func (e *ParameterFaultError) first40Octets() []byte {
	return first40(e.Raw, nil)
}

// UnexpectedMessageError is used if a defined and recognized message is received
// that is not expected in the current state (in some cases, the ASP may optionally
// silently discard the message and not send an Error message).
type UnexpectedMessageError struct {
	Msg messages.M3UA
}

// NewUnexpectedMessageError creates UnexpectedMessageError
func NewUnexpectedMessageError(msg messages.M3UA) *UnexpectedMessageError {
	return &UnexpectedMessageError{Msg: msg}
}

// Error returns error string with message class and type.
func (e *UnexpectedMessageError) Error() string {
	return fmt.Sprintf("unexpected message. class: %s, type: %s", e.Msg.MessageClassName(), e.Msg.MessageTypeName())
}

// InvalidSCTPStreamIDError reports an SCTP stream outside the range a message
// may use, whether selected locally or received from the peer.
type InvalidSCTPStreamIDError struct {
	ID uint16
}

// NewInvalidSCTPStreamIDError creates InvalidSCTPStreamIDError
func NewInvalidSCTPStreamIDError(id uint16) *InvalidSCTPStreamIDError {
	return &InvalidSCTPStreamIDError{ID: id}
}

// Error returns error string with violating stream ID.
func (e *InvalidSCTPStreamIDError) Error() string {
	return fmt.Sprintf("invalid SCTP Stream ID: %d", e.ID)
}

// RoutingContextError reports Routing Contexts a peer named that this node
// cannot act on, and carries them.
//
// RFC 4666 Section 3.8.1 is explicit that they must travel with the report:
// "For this error, the invalid Routing Context(s) MUST be included in the Error
// message." A bare sentinel could not carry them, so the Error message named
// this node's own configured contexts instead — telling the peer that our
// contexts were the invalid ones.
type RoutingContextError struct {
	// Code is the RFC 4666 Section 3.8.1 error code to report: either
	// params.ErrInvalidRoutingContext or params.ErrNoConfiguredAsForAsp.
	Code uint32
	// Contexts are the offending Routing Contexts, as named by the peer.
	Contexts []uint32
}

// NetworkAppearanceError reports the Network Appearance an ASP named that the
// SGP has not configured. RFC 4666 Section 3.8.1 requires that exact value in
// the Error response, so a sentinel alone cannot represent the fault.
type NetworkAppearanceError struct {
	Appearance uint32
}

// NewInvalidNetworkAppearanceError reports an unconfigured Network Appearance
// named by an ASP.
func NewInvalidNetworkAppearanceError(appearance uint32) *NetworkAppearanceError {
	return &NetworkAppearanceError{Appearance: appearance}
}

func (e *NetworkAppearanceError) Error() string {
	return fmt.Sprintf("invalid network appearance %d", e.Appearance)
}

// Is lets callers match ErrInvalidNetworkAppearance while retaining the
// offending value through errors.As.
func (e *NetworkAppearanceError) Is(target error) bool {
	return target == ErrInvalidNetworkAppearance
}

// NewInvalidRoutingContextError reports contexts a peer named that this
// association is not configured for.
func NewInvalidRoutingContextError(rcs ...uint32) *RoutingContextError {
	return &RoutingContextError{Code: params.ErrInvalidRoutingContext, Contexts: rcs}
}

// NewNoConfiguredASError reports contexts a peer asked to activate for which no
// Routing Key is defined.
func NewNoConfiguredASError(rcs ...uint32) *RoutingContextError {
	return &RoutingContextError{Code: params.ErrNoConfiguredAsForAsp, Contexts: rcs}
}

func (e *RoutingContextError) Error() string {
	switch e.Code {
	case params.ErrNoConfiguredAsForAsp:
		return fmt.Sprintf("no configured AS for ASP: routing context %v", e.Contexts)
	default:
		return fmt.Sprintf("invalid routing context %v", e.Contexts)
	}
}

// Is lets callers keep testing against the sentinels.
func (e *RoutingContextError) Is(target error) bool {
	switch target {
	case ErrInvalidRoutingContext:
		return e.Code == params.ErrInvalidRoutingContext
	case ErrNoConfiguredAS:
		return e.Code == params.ErrNoConfiguredAsForAsp
	default:
		return false
	}
}

func (c *Association) handleErrors(e error) error {
	var res messages.M3UA
	var InvalidVersionError *InvalidVersionError
	if errors.As(e, &InvalidVersionError) {
		// The Error's own common header carries Version 1, which is how the
		// peer learns what this node supports: RFC 4666 Section 4.8, "the
		// receiving end responds with an Error message indicating the version
		// the receiving node supports". There is no Version parameter to put it
		// in, so the header is the indication.
		//
		// Diagnostic Information quotes the message that was rejected, per
		// Section 3.8.1: "The Diagnostic Information SHOULD contain the
		// offending message." Without it the peer is told only that some
		// version was wrong, not which message carried it.
		var event *invalidVersionEvent
		_ = errors.As(e, &event)
		var raw []byte
		if event != nil {
			raw = event.raw
		}
		var diagnostic *params.Param
		if raw := diagnosticInformation(raw, nil); len(raw) > 0 {
			diagnostic = params.NewDiagnosticInformation(raw)
		}
		res = messages.NewError(
			params.NewErrorCode(params.InvalidVersionError),
			nil, nil, nil,
			diagnostic,
		)
	}
	//nolint:errorlint
	if err, ok := e.(*UnsupportedClassError); ok {
		res = messages.NewError(
			params.NewErrorCode(params.UnsupportedMessageErrorClass),
			nil, nil, nil,
			params.NewDiagnosticInformation(err.first40Octets()),
		)
	}
	//nolint:errorlint
	if err, ok := e.(*UnsupportedMessageError); ok {
		res = messages.NewError(
			params.NewErrorCode(params.UnsupportedMessageErrorType),
			nil, nil, nil,
			params.NewDiagnosticInformation(err.first40Octets()),
		)
	}
	if errors.Is(e, ErrManagementBlocking) {
		res = messages.NewError(
			params.NewErrorCode(params.ErrRefusedManagementBlocking),
			c.configuredRoutingContextParam(),
			c.cfg.NetworkAppearance.Copy(),
			nil, nil,
		)
	}
	if errors.Is(e, ErrInvalidParameterValue) {
		res = messages.NewError(
			params.NewErrorCode(params.ErrInvalidParameterValue),
			c.configuredRoutingContextParam(),
			c.cfg.NetworkAppearance.Copy(),
			nil, nil,
		)
	}
	var parameterFaultError *ParameterFaultError
	if errors.As(e, &parameterFaultError) {
		res = messages.NewError(
			params.NewErrorCode(parameterFaultError.Code),
			nil, nil, nil,
			params.NewDiagnosticInformation(parameterFaultError.first40Octets()),
		)
	}
	var UnexpectedMessageError *UnexpectedMessageError
	if errors.As(e, &UnexpectedMessageError) {
		res = messages.NewError(
			params.NewErrorCode(params.UnexpectedMessageError),
			// Section 3.8.1: "If the Unexpected message contained Routing
			// Contexts, the Routing Contexts SHOULD be included in the Error
			// message." The peer's, not ours: quoting our configuration told it
			// nothing about the message it got wrong, and on an association
			// serving several Application Servers it named contexts that had
			// nothing to do with the fault. Nil when the offending message
			// carried none, since the rule is conditional on it having had them.
			routingContextOf(UnexpectedMessageError.Msg).Copy(),
			c.cfg.NetworkAppearance.Copy(),
			// Mask 0: this is one point code, this node's, not a range.
			params.NewAffectedPointCodeWithMask(0, c.cfg.OriginatingPointCode),
			nil,
		)
	}
	var InvalidSCTPStreamIDError *InvalidSCTPStreamIDError
	if errors.As(e, &InvalidSCTPStreamIDError) {
		res = messages.NewError(
			params.NewErrorCode(params.ErrInvalidStreamIdentifier),
			nil, nil, nil, nil,
		)
	}
	if errors.Is(e, ErrASPIdentifierRequired) {
		res = messages.NewError(
			params.NewErrorCode(params.ErrAspIdentifierRequired),
			nil, nil, nil, nil,
		)
	}
	if errors.Is(e, ErrInvalidASPIdentifier) {
		res = messages.NewError(
			params.NewErrorCode(params.ErrInvalidAspIdentifier),
			nil, nil, nil, nil,
		)
	}
	// RFC 4666 Section 4.3.4.3: the SGP "responds with an Error message
	// ('Unsupported / Invalid Traffic Handling Mode')" when the mode a peer
	// requests is incompatible with the one configured for the AS.
	if errors.Is(e, ErrUnsupportedTrafficMode) {
		res = messages.NewError(
			params.NewErrorCode(params.ErrUnsupportedTrafficModeType),
			c.configuredRoutingContextParam(),
			c.cfg.NetworkAppearance.Copy(),
			nil, nil,
		)
	}
	// RFC 4666 Section 3.8.1: "The 'Protocol Error' error is sent for any
	// protocol anomaly". A message too large for the read buffer cannot be
	// parsed or acted on, but it is one message: tell the peer and keep the
	// association up rather than letting an unmapped error close it.
	if errors.Is(e, ErrMessageTooLarge) {
		res = messages.NewError(
			params.NewErrorCode(params.ErrProtocolError),
			c.configuredRoutingContextParam(),
			c.cfg.NetworkAppearance.Copy(),
			nil, nil,
		)
	}
	// RFC 4666 Section 3.8.1: "For this error, the invalid Routing Context(s)
	// MUST be included in the Error message" — the peer's contexts, not ours.
	var routingContextError *RoutingContextError
	if errors.As(e, &routingContextError) {
		var routingContext *params.Param
		if len(routingContextError.Contexts) > 0 {
			routingContext = params.NewRoutingContext(routingContextError.Contexts...)
		}
		res = messages.NewError(
			params.NewErrorCode(routingContextError.Code),
			routingContext,
			c.cfg.NetworkAppearance.Copy(),
			nil, nil,
		)
	}
	var networkAppearanceError *NetworkAppearanceError
	if errors.As(e, &networkAppearanceError) {
		res = messages.NewError(
			params.NewErrorCode(params.ErrInvalidNetworkAppearance),
			nil,
			params.NewNetworkAppearance(networkAppearanceError.Appearance),
			nil, nil,
		)
	}
	// User/Cause is Mandatory in DUPU (RFC 4666 Section 3.4.5).
	if errors.Is(e, ErrMissingUserCause) {
		res = messages.NewError(
			params.NewErrorCode(params.ErrMissingParameter),
			c.configuredRoutingContextParam(),
			c.cfg.NetworkAppearance.Copy(),
			nil, nil,
		)
	}
	// Affected Point Code is Mandatory in every SSNM message (RFC 4666 Sections
	// 3.4.1 to 3.4.6), so its absence is the same Missing Parameter condition.
	if errors.Is(e, ErrMissingAffectedPointCode) {
		res = messages.NewError(
			params.NewErrorCode(params.ErrMissingParameter),
			c.configuredRoutingContextParam(),
			c.cfg.NetworkAppearance.Copy(),
			nil, nil,
		)
	}
	// RFC 4666 Section 3.8.1: "The 'Missing Parameter' error would be sent if a
	// mandatory parameter were not included in a message."
	if errors.Is(e, ErrMissingProtocolData) {
		res = messages.NewError(
			params.NewErrorCode(params.ErrMissingParameter),
			c.configuredRoutingContextParam(),
			c.cfg.NetworkAppearance.Copy(),
			nil, nil,
		)
	}
	if errors.Is(e, ErrMissingRoutingContext) {
		res = messages.NewError(
			params.NewErrorCode(params.ErrMissingParameter),
			nil,
			c.cfg.NetworkAppearance.Copy(),
			nil, nil,
		)
	}
	if errors.Is(e, ErrMissingStatus) {
		res = messages.NewError(
			params.NewErrorCode(params.ErrMissingParameter),
			nil, nil, nil, nil,
		)
	}

	// Local congestion is told to the peer and must not tear the association
	// down. RFC 4666 Section 3.4.4: "The SCON message MAY also be sent from the
	// M3UA layer of an ASP to an M3UA peer, indicating that the congestion
	// level of the M3UA layer or the ASP has changed." Reporting it only
	// locally left the peer sending at full rate into a queue this node was
	// discarding from.
	//
	// Affected Point Code is Mandatory in SCON, and the congested node is this
	// one, so it names the configured Originating Point Code. A deployment that
	// carries its real point codes per message — leaving OriginatingPointCode
	// at zero — should set it to this node's own point code for the report to
	// mean anything to the peer.
	if errors.Is(e, ErrDataQueueFull) {
		if _, err := c.WriteSignal(messages.NewSignallingCongestion(
			c.cfg.NetworkAppearance.Copy(),
			c.configuredRoutingContextParam(),
			params.NewAffectedPointCodeWithMask(0, c.cfg.OriginatingPointCode),
			nil, nil, nil,
		)); err != nil {
			// The peer could not be told, but the association is still up and
			// the queue is still the thing in trouble: report, do not close.
			logf("m3ua: failed to send SCON for local congestion: %v", err)
		}
		return nil
	}

	if res == nil {
		return e
	}

	if _, err := c.WriteSignal(res); err != nil {
		return err
	}

	return nil
}

// first40 returns the octets to quote in Diagnostic Information: the received
// message when the dispatcher kept it, and otherwise a re-marshal of the parsed
// one, truncated to the 40 octets RFC 4666 Section 3.8.1 asks for.
func first40(raw []byte, msg messages.M3UA) []byte {
	b := diagnosticInformation(raw, msg)
	if len(b) > 40 {
		return b[:40]
	}
	return b
}

const maxDiagnosticInformationLen = int(^uint16(0)) - 4

// diagnosticInformation returns an owned copy of the offending message. A
// Diagnostic Information parameter has a 16-bit length including its four-byte
// header, so a larger M3UA message cannot be represented in full.
func diagnosticInformation(raw []byte, msg messages.M3UA) []byte {
	b := bytes.Clone(raw)
	if len(b) == 0 && msg != nil {
		var err error
		if b, err = msg.MarshalBinary(); err != nil {
			return nil
		}
	}
	if len(b) > maxDiagnosticInformationLen {
		return b[:maxDiagnosticInformationLen]
	}
	return b
}

// rawClass and rawType read the class and type octets straight from a message
// that failed to parse. RFC 4666 Section 3.1 puts them at fixed offsets, so
// they are readable even when the parameters that follow are not.
func rawClass(raw []byte) uint8 {
	if len(raw) < 4 {
		return 0
	}
	return raw[2]
}

func rawType(raw []byte) uint8 {
	if len(raw) < 4 {
		return 0
	}
	return raw[3]
}

// routingContextOf returns the Routing Context parameter a message carried, or
// nil for the message types that have none.
//
// RFC 4666 Section 3.8.1 asks for the offending message's own contexts when
// reporting an Unexpected Message: "If the Unexpected message contained Routing
// Contexts, the Routing Contexts SHOULD be included in the Error message."
func routingContextOf(m messages.M3UA) *params.Param {
	switch v := m.(type) {
	case *messages.Data:
		return v.RoutingContext
	case *messages.DestinationUnavailable:
		return v.RoutingContext
	case *messages.DestinationAvailable:
		return v.RoutingContext
	case *messages.DestinationStateAudit:
		return v.RoutingContext
	case *messages.SignallingCongestion:
		return v.RoutingContext
	case *messages.DestinationUserPartUnavailable:
		return v.RoutingContext
	case *messages.DestinationRestricted:
		return v.RoutingContext
	case *messages.AspActive:
		return v.RoutingContext
	case *messages.AspActiveAck:
		return v.RoutingContext
	case *messages.AspInactive:
		return v.RoutingContext
	case *messages.AspInactiveAck:
		return v.RoutingContext
	case *messages.Notify:
		return v.RoutingContext
	default:
		// ASP Up, ASP Down, Heartbeat and their Acks carry no Routing Context.
		return nil
	}
}
