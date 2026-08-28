// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"errors"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// TestSCONFromAnASPDoesNotRewriteTheSGsRoutingState is the security-relevant
// one.
//
// RFC 4666 Section 3.4.4 allows SCON in both directions, but the two carry
// different meanings. From an SGP it reports SS7 congestion the SG has observed;
// from an ASP it says only that "the congestion level of the M3UA layer or the
// ASP has changed" — a statement about that ASP, not about the SS7 network.
//
// Both were written into the same destination map, which is also the map the SGP
// answers a DAUD from (Section 4.5.3). Any ASP could therefore make the SG
// report SS7 congestion that does not exist, to every other ASP that audited it.
func TestSCONFromAnASPDoesNotRewriteTheSGsRoutingState(t *testing.T) {
	sgp, _ := newSSNMTestConn(t, StateASPActive, RoleSGP)

	if err := sgp.handleSignallingCongestion(messages.NewSignallingCongestion(
		nil, nil, params.NewAffectedPointCodeWithMask(0, 0x222222),
		nil, params.NewCongestionIndications(2), nil,
	)); err != nil {
		t.Fatalf("SCON from an ASP was rejected at an SGP: %v", err)
	}

	// The SG's own view of the SS7 destination must be untouched.
	if got := sgp.DestinationState(0x222222); got == DestinationCongested {
		t.Error("an ASP's SCON marked an SS7 destination congested in the SG's " +
			"own routing state; another ASP auditing that point code would be " +
			"told of congestion the SG never observed")
	}

	// It is still recorded as what it is: the peer's congestion level.
	if got := sgp.PeerCongestionLevel(); got != 2 {
		t.Errorf("PeerCongestionLevel() = %d, want 2 — the ASP's report should "+
			"be kept, just not as SS7 state", got)
	}
}

// At an ASP the SCON does describe an SS7 destination, so it is applied.
func TestSCONFromAnSGPDoesUpdateTheDestination(t *testing.T) {
	asp, _ := newSSNMTestConn(t, StateASPActive, RoleASP)

	if err := asp.handleSignallingCongestion(messages.NewSignallingCongestion(
		nil, nil, params.NewAffectedPointCodeWithMask(0, 0x222222),
		nil, params.NewCongestionIndications(2), nil,
	)); err != nil {
		t.Fatalf("handleSignallingCongestion: %v", err)
	}
	if got := asp.DestinationState(0x222222); got != DestinationCongested {
		t.Errorf("destination state = %v after an SGP's SCON, want %v",
			got, DestinationCongested)
	}
}

// TestSCONWithCongestionLevelZeroIsNotCongestion covers the Congestion Level
// table of RFC 4666 Section 3.4.4:
//
//	0     No Congestion or Undefined
//	1     Congestion Level 1
//	2     Congestion Level 2
//	3     Congestion Level 3
//
// Level 0 is the RFC's way of saying congestion has cleared — Section 4.5.3's
// implementation note treats it as exactly that, telling an ASP not to start an
// audit "for the case of a received SCON message containing a congestion level
// value of 'no congestion' or 'undefined' (i.e., congestion Level = '0')".
// Recording it as congestion inverts the message: the destination is throttled
// by the very report that said it had recovered.
func TestSCONWithCongestionLevelZeroIsNotCongestion(t *testing.T) {
	asp, _ := newSSNMTestConn(t, StateASPActive, RoleASP)

	// Congested first, so clearing is observable.
	asp.SetDestinationState(0x222222, DestinationCongested)

	if err := asp.handleSignallingCongestion(messages.NewSignallingCongestion(
		nil, nil, params.NewAffectedPointCodeWithMask(0, 0x222222),
		nil, params.NewCongestionIndications(0), nil,
	)); err != nil {
		t.Fatalf("handleSignallingCongestion: %v", err)
	}
	if got := asp.DestinationState(0x222222); got == DestinationCongested {
		t.Error("a SCON carrying Congestion Level 0 (\"No Congestion or " +
			"Undefined\") left the destination congested")
	}
}

// A SCON with no Congestion Indications parameter at all is still congestion:
// the parameter is optional and "For MTP congestion methods without multiple
// congestion levels (e.g., the ITU international method) the parameter is not
// included."
func TestSCONWithoutCongestionIndicationsIsStillCongestion(t *testing.T) {
	asp, _ := newSSNMTestConn(t, StateASPActive, RoleASP)

	if err := asp.handleSignallingCongestion(messages.NewSignallingCongestion(
		nil, nil, params.NewAffectedPointCodeWithMask(0, 0x222222), nil, nil, nil,
	)); err != nil {
		t.Fatalf("handleSignallingCongestion: %v", err)
	}
	if got := asp.DestinationState(0x222222); got != DestinationCongested {
		t.Errorf("destination state = %v, want %v: an ITU-style SCON carries no "+
			"level and still means congestion", got, DestinationCongested)
	}
}

// TestDAUDForAnUnknownPointCodeIsAnsweredWithDUNA covers RFC 4666 Section 4.5.3:
//
//	An SG SHOULD respond with a DUNA message when DAUD was received with
//	an unknown Signalling Point Code.
//
// Unknown destinations were indistinguishable from known-available ones, since
// the lookup returned "available" for anything absent, so the SG cheerfully
// told an auditing ASP that a point code it had never heard of was reachable —
// and the ASP then sent traffic to it.
func TestDAUDForAnUnknownPointCodeIsAnsweredWithDUNA(t *testing.T) {
	sgp, sent := newSSNMTestConn(t, StateASPActive, RoleSGP)

	if err := sgp.handleDestinationStateAudit(messages.NewDestinationStateAudit(
		nil, nil, params.NewAffectedPointCodeWithMask(0, 0x999999), nil,
	)); err != nil {
		t.Fatalf("handleDestinationStateAudit: %v", err)
	}

	got := typeNames(*sent)
	if len(got) != 1 || got[0] != "Destination Unavailable" {
		t.Errorf("answered a DAUD for an unknown point code with %v, "+
			"want [Destination Unavailable]", got)
	}
}

// A point code the SG does know about, and which is available, still draws a
// DAVA.
func TestDAUDForAKnownAvailablePointCodeIsAnsweredWithDAVA(t *testing.T) {
	sgp, sent := newSSNMTestConn(t, StateASPActive, RoleSGP)
	sgp.SetDestinationState(0x999999, DestinationAvailable)

	if err := sgp.handleDestinationStateAudit(messages.NewDestinationStateAudit(
		nil, nil, params.NewAffectedPointCodeWithMask(0, 0x999999), nil,
	)); err != nil {
		t.Fatalf("handleDestinationStateAudit: %v", err)
	}

	got := typeNames(*sent)
	if len(got) != 1 || got[0] != "Destination Available" {
		t.Errorf("answered a DAUD for a known available point code with %v, "+
			"want [Destination Available]", got)
	}
}

// TestDUPUWithAMaskOrSeveralPointCodesIsRejected covers RFC 4666 Section 3.4.5:
//
//	The format and description of the Affected Point Code parameter
//	are the same as for the DUNA message (see Section 3.4.1.) except
//	that the Mask field is not used and only a single Affected DPC is
//	included.  Ranges and lists of Affected DPCs cannot be signaled in
//	a DUPU message
//
// Section 3.8.1 names the answer, and uses this very message as its example:
// "The 'Invalid Parameter Value' error is sent if a message is received with an
// invalid parameter value (e.g., a DUPU message was received with a Mask value
// other than '0'."
func TestDUPUWithAMaskOrSeveralPointCodesIsRejected(t *testing.T) {
	for _, tt := range []struct {
		name string
		apc  *params.Param
	}{
		{"non-zero Mask", params.NewAffectedPointCodeWithMask(8, 0x222222)},
		{"several Affected DPCs", params.NewAffectedPointCode(0x222222, 0x333333)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			asp, _ := newSSNMTestConn(t, StateASPActive, RoleASP)

			err := asp.handleDestinationUserPartUnavailable(
				messages.NewDestinationUserPartUnavailable(
					nil, nil, tt.apc,
					params.NewUserCause(params.SCCP, params.Unequipped), nil,
				))
			if err == nil {
				t.Fatal("accepted a DUPU the RFC says cannot be signalled")
			}
			if !errors.Is(err, ErrInvalidParameterValue) {
				t.Errorf("error = %v, want ErrInvalidParameterValue", err)
			}
		})
	}
}

// The ordinary DUPU — one destination, no mask — is still accepted.
func TestDUPUWithASingleUnmaskedPointCodeIsAccepted(t *testing.T) {
	asp, _ := newSSNMTestConn(t, StateASPActive, RoleASP)

	if err := asp.handleDestinationUserPartUnavailable(
		messages.NewDestinationUserPartUnavailable(
			nil, nil, params.NewAffectedPointCodeWithMask(0, 0x222222),
			params.NewUserCause(params.SCCP, params.Unequipped), nil,
		)); err != nil {
		t.Fatalf("a well-formed DUPU was rejected: %v", err)
	}
}

// TestSSNMValidatesTheRoutingContext covers RFC 4666 Section 3.8.1's "Invalid
// Routing Context" error, which is sent "if a message is received with an
// invalid or unconfigured routing context value".
//
// Routing Context is Conditional on every SSNM message, and the ASPTM handlers
// already checked it. The SSNM handlers did not, so a peer could report a
// destination unreachable "for" an Application Server this association does not
// serve, and have it applied anyway.
func TestSSNMValidatesTheRoutingContext(t *testing.T) {
	// newSSNMTestConn configures Routing Context 1.
	for _, tt := range []struct {
		name string
		call func(*Association, *params.Param) error
	}{
		{"DUNA", func(c *Association, rc *params.Param) error {
			return c.handleDestinationUnavailable(messages.NewDestinationUnavailable(
				nil, rc, params.NewAffectedPointCodeWithMask(0, 0x222222), nil))
		}},
		{"DAVA", func(c *Association, rc *params.Param) error {
			return c.handleDestinationAvailable(messages.NewDestinationAvailable(
				nil, rc, params.NewAffectedPointCodeWithMask(0, 0x222222), nil))
		}},
		{"DRST", func(c *Association, rc *params.Param) error {
			return c.handleDestinationRestricted(messages.NewDestinationRestricted(
				nil, rc, params.NewAffectedPointCodeWithMask(0, 0x222222), nil))
		}},
		{"SCON", func(c *Association, rc *params.Param) error {
			return c.handleSignallingCongestion(messages.NewSignallingCongestion(
				nil, rc, params.NewAffectedPointCodeWithMask(0, 0x222222), nil, nil, nil))
		}},
		{"DUPU", func(c *Association, rc *params.Param) error {
			return c.handleDestinationUserPartUnavailable(
				messages.NewDestinationUserPartUnavailable(
					nil, rc, params.NewAffectedPointCodeWithMask(0, 0x222222),
					params.NewUserCause(params.SCCP, params.Unequipped), nil))
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			asp, _ := newSSNMTestConn(t, StateASPActive, RoleASP)

			err := tt.call(asp, params.NewRoutingContext(4242))
			if err == nil {
				t.Fatal("an SSNM message naming an unconfigured Routing Context was applied")
			}
			var rcErr *RoutingContextError
			if !errors.As(err, &rcErr) {
				t.Fatalf("error = %v (%T), want a RoutingContextError", err, err)
			}

			// A configured context is accepted.
			asp2, _ := newSSNMTestConn(t, StateASPActive, RoleASP)
			if err := tt.call(asp2, params.NewRoutingContext(1)); err != nil {
				t.Errorf("a configured Routing Context was rejected: %v", err)
			}
		})
	}
}

// A DAUD naming an unconfigured Routing Context is refused at the SGP too.
func TestDAUDValidatesTheRoutingContext(t *testing.T) {
	sgp, _ := newSSNMTestConn(t, StateASPActive, RoleSGP)

	err := sgp.handleDestinationStateAudit(messages.NewDestinationStateAudit(
		nil, params.NewRoutingContext(4242),
		params.NewAffectedPointCodeWithMask(0, 0x222222), nil,
	))
	if err == nil {
		t.Fatal("a DAUD naming an unconfigured Routing Context was answered")
	}
	var rcErr *RoutingContextError
	if !errors.As(err, &rcErr) {
		t.Errorf("error = %v (%T), want a RoutingContextError", err, err)
	}
}
