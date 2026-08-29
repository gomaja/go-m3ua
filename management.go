// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// handleError processes an incoming ERR.
//
// RFC 4666 Section 3.8.1: "The Error message is used to notify a peer of an
// error event associated with an incoming message." The condition is reported
// for operator visibility, but the association is left up: most error codes
// concern one message rather than the link, and the peer has already taken
// whatever action it intends.
//
// Nothing is written back. Section 3.8.1 requires that "Error messages MUST NOT
// be generated in response to other Error messages", and every non-nil error
// returned from here reaches handleErrors, which answers recognised errors with
// an ERR on the wire and closes the Association on the rest. Returning an error for a
// well-formed peer ERR would therefore either bounce an ERR back or tear down a
// healthy association.
func (c *Association) handleError(e *messages.Error) error {
	switch c.State() {
	case StateSCTPCDI, StateSCTPRI:
		return NewUnexpectedMessageError(e)
	}

	code := uint32(0)
	if e.ErrorCode != nil {
		code = e.ErrorCode.ErrorCode()
	}
	name := errorCodeName(code)
	logf("m3ua: received ERR from peer: code=0x%02x (%s)", code, name)

	// RFC 4666 Section 4.2's M-ERROR indication. The log line above was the
	// only place this went, so an application could not tell that the peer had
	// refused something, nor why.
	ind := &ManagementIndication{
		Kind:        ManagementError,
		ErrorCode:   code,
		Description: name,
	}
	// Section 3.8.1 lists Routing Context, Network Appearance and Affected
	// Point Code as "Mandatory*" — mandatory for specific Error Codes — because
	// they are what makes the refusal actionable: the "Invalid Routing Context"
	// error carries the context that was invalid, "Invalid Network Appearance"
	// carries the appearance, and "Destination Status Unknown" carries the point
	// codes. All three were parsed and dropped, so an application was told only
	// that something had been refused, never what.
	ind.RoutingContexts = routingContextsOf(e.RoutingContext)
	ind.RoutingContext, ind.RoutingContextSet = firstRoutingContext(e.RoutingContext)
	ind.NetworkAppearance, ind.NetworkAppearanceSet = uint32ParamOf(
		e.NetworkAppearance, params.NetworkAppearance, (*params.Param).NetworkAppearance)
	if e.AffectedPointCode != nil {
		ind.AffectedPointCodes = e.AffectedPointCode.AffectedPointCodes()
	}
	c.notifyManagement(ind)

	return nil
}

// errorCodeName renders an Error Code as the name RFC 4666 Section 3.8.1 gives
// it, so operators reading a log can match it against the spec without a
// lookup table. Unassigned values are reported numerically by the caller.
func errorCodeName(code uint32) string {
	switch code {
	case params.InvalidVersionError:
		return "Invalid Version"
	case params.UnsupportedMessageErrorClass:
		return "Unsupported Message Class"
	case params.UnsupportedMessageErrorType:
		return "Unsupported Message Type"
	case params.ErrUnsupportedTrafficModeType:
		return "Unsupported / Invalid Traffic Handling Mode"
	case params.UnexpectedMessageError:
		return "Unexpected Message"
	case params.ErrProtocolError:
		return "Protocol Error"
	case params.ErrInvalidStreamIdentifier:
		return "Invalid Stream Identifier"
	case params.ErrRefusedManagementBlocking:
		return "Refused - Management Blocking"
	case params.ErrAspIdentifierRequired:
		return "ASP Identifier Required"
	case params.ErrInvalidAspIdentifier:
		return "Invalid ASP Identifier"
	case params.ErrInvalidParameterValue:
		return "Invalid Parameter Value"
	case params.ErrParameterFieldError:
		return "Parameter Field Error"
	case params.ErrUnexpectedParameter:
		return "Unexpected Parameter"
	case params.ErrDestinationStatusUnknown:
		return "Destination Status Unknown"
	case params.ErrInvalidNetworkAppearance:
		return "Invalid Network Appearance"
	case params.ErrMissingParameter:
		return "Missing Parameter"
	case params.ErrInvalidRoutingContext:
		return "Invalid Routing Context"
	case params.ErrNoConfiguredAsForAsp:
		return "No Configured AS for ASP"
	default:
		return "unassigned"
	}
}

// handleNotify processes an incoming NTFY.
//
// A Notify carries the peer's view of AS or ASP state. RFC 4666 Section 4.3.4.5
// is explicit that the AS-state notifications are advisory: such a message "does
// not explicitly compel the ASP(s) receiving the message to become active. The
// ASPs remain in control of what (and when) traffic action is taken." They are
// therefore reported, not acted on.
//
// The one case the RFC makes normative for the receiver is an Override by
// another ASP. Section 4.3.4.3: "The ASP receiving this Notify MUST consider
// itself now in the ASP-INACTIVE state, if it is not already aware of this via
// inter-ASP communication with the Overriding ASP." That transition is applied
// by the dispatcher, which owns state publishing.
func (c *Association) handleNotify(n *messages.Notify) error {
	switch c.State() {
	case StateSCTPCDI, StateSCTPRI:
		return NewUnexpectedMessageError(n)
	}
	// In the SG-AS model Notify is originated by an SGP and consumed by an ASP.
	// RFC 4666 Section 4.3.4.5.1 applies the same procedure between IPSPs and
	// permits either peer to send it to a remote IPSP that is not ASP-DOWN.
	if c.role != RoleASP && c.role != RoleIPSP {
		return NewUnexpectedMessageError(n)
	}

	// The Status parameter is Mandatory (Section 3.8.2); without it there is
	// nothing to report or act on.
	if n.Status == nil {
		return ErrMissingStatus
	}

	// Both Status Type values and their Status Information ranges are closed
	// tables in Section 3.8.2. Reserved values are not state: surfacing one as
	// an ordinary indication makes a malformed peer message indistinguishable
	// from a defined transition.
	switch n.Status.Status() {
	case params.AsStateInactive,
		params.AsStateActive,
		params.AsStatePending,
		params.InsufficientAspResources,
		params.AlternateAspActive,
		params.AspFailure:
	default:
		return ErrInvalidParameterValue
	}

	// RFC 4666 Errata ID 2065 makes Routing Context Conditional in NTFY: the
	// sender must include it when a notification concerns only a subset of the
	// Application Servers this ASP serves. Omission still has the meaning
	// Section 4.3.4.5 gives it: configuration identifies the ASP's AS
	// memberships and the notification applies in each one.
	//
	// Validate before publishing M-NOTIFY or letting the dispatcher act on an
	// Alternate ASP Active status. An unknown context must be reported with the
	// peer's offending values, and rejection must not change any AS state.
	if err := c.validateRoutingContext(n.RoutingContext); err != nil {
		return err
	}
	configured := c.configuredRoutingContexts()
	if n.RoutingContext == nil && len(configured) == 0 && c.hasExplicitlyEmptyASPAuthorization() {
		// Section 3.8.1 assigns this exact case its own Error: no RC was
		// present and configuration cannot identify any referenced AS.
		return NewNoConfiguredASError()
	}
	name := notifyStatusName(n.Status.Status())
	logf("m3ua: received NTFY from peer: type=%d info=%d (%s)",
		n.Status.StatusType(), n.Status.StatusInfo(), name)

	// RFC 4666 Section 4.2's M-NOTIFY indication. Section 4.3.4.5 makes the
	// AS-state notifications advisory -- they do "not explicitly compel the
	// ASP(s) receiving the message to become active" -- which is precisely why
	// they have to reach the application: the decision is the application's to
	// make, and it could not previously see that there was one to make.
	ind := &ManagementIndication{
		Kind:        ManagementNotify,
		StatusType:  n.Status.StatusType(),
		StatusInfo:  n.Status.StatusInfo(),
		Description: name,
	}
	// Which Application Server the notification is about, and which ASP it
	// concerns. Both were parsed and dropped, so an ASP serving several Routing
	// Contexts was told that "an" AS had gone AS-PENDING, or that "an"
	// alternate ASP had become active, with no way to tell which — and the
	// decision Section 4.3.4.5 leaves to it is not one it could then make.
	// See ManagementIndication.RoutingContext and RFC 4666 Errata ID 2065.
	ind.RoutingContexts = routingContextsOf(n.RoutingContext)
	if n.RoutingContext == nil {
		ind.RoutingContexts = append([]uint32(nil), configured...)
	}
	ind.RoutingContext, ind.RoutingContextSet = firstRoutingContext(n.RoutingContext)
	ind.ASPIdentifier, ind.ASPIdentifierSet = uint32ParamOf(
		n.AspIdentifier, params.AspIdentifier, (*params.Param).AspIdentifier)
	c.notifyManagement(ind)

	return nil
}

// notifyStatusName renders a Status as the name RFC 4666 Section 3.8.2 gives it.
func notifyStatusName(status uint32) string {
	switch status {
	case params.AsStateInactive:
		return "AS-INACTIVE"
	case params.AsStateActive:
		return "AS-ACTIVE"
	case params.AsStatePending:
		return "AS-PENDING"
	case params.InsufficientAspResources:
		return "Insufficient ASP Resources Active in AS"
	case params.AlternateAspActive:
		return "Alternate ASP Active"
	case params.AspFailure:
		return "ASP Failure"
	default:
		return "unassigned"
	}
}

// overriddenByAlternateAsp reports whether a Notify tells this ASP or IPSP that
// another ASP/IPSP has taken over its traffic in an Override mode AS, which RFC
// 4666 Section 4.3.4.3 requires the receiver to treat as ASP-INACTIVE.
//
// An SGP is the sender of this notification, never its subject. Section
// 4.3.4.5.1 extends the same Notify procedure between IPSPs.
func (c *Association) overriddenByAlternateAsp(n *messages.Notify) bool {
	if (c.role != RoleASP && c.role != RoleIPSP) || n.Status == nil {
		return false
	}

	return n.Status.Status() == params.AlternateAspActive
}

// overrideScope decides how far an "Alternate ASP Active" Notify reaches, and
// records a partial override.
//
// It reports whether the whole association must go to ASP-INACTIVE. RFC 4666
// Errata ID 2065 is precisely about this: a Notify that names one Routing
// Context out of several "allows the first ASP to become inactive only for that
// particular Application Server, rather than all of them". Standing the
// association down for a context nobody overrode takes traffic off Application
// Servers this ASP is still the active one for.
//
// A Notify naming every context the association carries is the
// whole-association case and keeps the state move. A missing context has the
// same meaning: Section 4.3.4.5 says configuration identifies the ASP's AS
// memberships and the receiver takes the appropriate action in each AS.
// Anything narrower is recorded per context; see noteRoutingContextsOverridden
// for what that does and does not implement.
func (c *Association) overrideScope(n *messages.Notify) bool {
	named := n.RoutingContext.RoutingContexts()
	configured := c.configuredRoutingContexts()
	if len(named) == 0 {
		return true
	}

	if len(configured) == 0 {
		return true
	}

	overridden := make(map[uint32]struct{}, len(named))
	for _, rc := range named {
		overridden[rc] = struct{}{}
	}
	for _, rc := range configured {
		if _, ok := overridden[rc]; !ok {
			// At least one Application Server is untouched, so the association
			// carries on and only the named contexts stop.
			c.noteRoutingContextsOverridden(named)
			return false
		}
	}
	return true
}
