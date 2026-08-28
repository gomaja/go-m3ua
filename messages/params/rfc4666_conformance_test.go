// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package params

import (
	"bytes"
	"testing"
)

// TestParameterPaddingIsNotCountedInLength covers RFC 4666 Section 3.2:
//
//	The total length of a parameter (including Tag, Parameter Length,
//	and Value fields) MUST be a multiple of 4 octets.  If the length
//	of the parameter is not a multiple of 4 octets, the sender pads
//	the Parameter at the end (i.e., after the Parameter Value field)
//	with all zero octets.  The length of the padding is NOT included
//	in the parameter length field.  A sender MUST NOT pad with more
//	than 3 octets.  The receiver MUST ignore the padding octets.
//
// Service Indicators is the only parameter this package builds from a variable
// number of single octets, so it is the only one where the rule bites.
func TestParameterPaddingIsNotCountedInLength(t *testing.T) {
	for _, c := range []struct {
		name    string
		sis     []uint8
		want    []byte
		wantLen uint16
	}{
		{
			// One SI: three pad octets, and Length counts none of them.
			name:    "one SI",
			sis:     []uint8{1},
			want:    []byte{0x02, 0x0c, 0x00, 0x05, 0x01, 0x00, 0x00, 0x00},
			wantLen: 5,
		},
		{
			name:    "three SIs",
			sis:     []uint8{1, 2, 3},
			want:    []byte{0x02, 0x0c, 0x00, 0x07, 0x01, 0x02, 0x03, 0x00},
			wantLen: 7,
		},
		{
			// Already a multiple of four: nothing is due, and padding with a
			// whole extra word would breach "MUST NOT pad with more than 3".
			name:    "four SIs needs no padding at all",
			sis:     []uint8{1, 2, 3, 4},
			want:    []byte{0x02, 0x0c, 0x00, 0x08, 0x01, 0x02, 0x03, 0x04},
			wantLen: 8,
		},
		{
			name: "eight SIs needs no padding at all",
			sis:  []uint8{1, 2, 3, 4, 5, 6, 7, 8},
			want: []byte{
				0x02, 0x0c, 0x00, 0x0c,
				0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
			},
			wantLen: 12,
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			p := NewServiceIndicators(c.sis...)
			if p.Length != c.wantLen {
				t.Errorf("Length = %d, want %d", p.Length, c.wantLen)
			}
			if p.Padding() > 3 {
				t.Errorf("Padding() = %d, RFC 4666 3.2 forbids more than 3", p.Padding())
			}
			got, err := p.MarshalBinary()
			if err != nil {
				t.Fatalf("MarshalBinary: %v", err)
			}
			if !bytes.Equal(got, c.want) {
				t.Errorf("wire =\n  % x\nwant\n  % x", got, c.want)
			}
		})
	}
}

// TestServiceIndicatorsRoundTrip is the consequence of the rule above, and the
// reason it is not cosmetic. Counting the pad octets inside Length makes a
// conformant receiver — including this package's own decoder — read them as
// Value. RFC 4666 Section 3.6.1 excludes SI 0 from a Routing Key ("excluding of
// course MTP management"), so the phantom octets do not merely pad, they
// register a Service Indicator that must never appear.
func TestServiceIndicatorsRoundTrip(t *testing.T) {
	for _, sis := range [][]uint8{{1}, {1, 2}, {1, 2, 3}, {1, 2, 3, 4}} {
		p := NewServiceIndicators(sis...)
		b, err := p.MarshalBinary()
		if err != nil {
			t.Fatalf("MarshalBinary: %v", err)
		}
		back, err := Parse(b)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		got := back.ServiceIndicators()
		if len(got) != len(sis) {
			t.Errorf("encoded %v, decoded %v — %d phantom Service Indicators",
				sis, got, len(got)-len(sis))
			continue
		}
		for i := range sis {
			if got[i] != sis[i] {
				t.Errorf("encoded %v, decoded %v", sis, got)
				break
			}
		}
	}
}

// TestAffectedPointCodeSeparatesMaskFromPointCode covers RFC 4666 Section 3.4.1,
// where each Affected Point Code word is a Mask octet followed by a 24-bit point
// code:
//
//	The Mask field can be used to identify a contiguous range of
//	Affected Destination Point Codes.
//
//	The Mask parameter is an integer representing a bit mask that can
//	be applied to the related Affected PC field.
//
// Folding the Mask into the point code produces a value no SS7 network can
// contain, so a masked DUNA — the normal way an SG announces a whole ANSI
// cluster or ITU region unavailable — is applied to a destination that does not
// exist, and the destinations that really went away are never marked down.
func TestAffectedPointCodeSeparatesMaskFromPointCode(t *testing.T) {
	for _, c := range []struct {
		name     string
		word     uint32
		wantMask uint8
		wantPC   uint32
	}{
		{"no mask", 0x00001234, 0, 0x001234},
		{"ANSI cluster, mask 8", 0x08001234, 8, 0x001234},
		{"ITU region, mask 3", 0x03003FFF, 3, 0x003FFF},
		{"network isolation, mask 24", 0x18000000, 24, 0x000000},
	} {
		t.Run(c.name, func(t *testing.T) {
			p := NewAffectedPointCode(c.word)
			if got := p.AffectedPointCode(); got != c.wantPC {
				t.Errorf("AffectedPointCode() = %#06x, want %#06x", got, c.wantPC)
			}
			if got := p.AffectedPointCodeMasks(); len(got) != 1 || got[0] != c.wantMask {
				t.Errorf("AffectedPointCodeMasks() = %v, want [%d]", got, c.wantMask)
			}
			// The point code must never carry mask bits: a 24-bit field.
			if got := p.AffectedPointCode(); got > 0xFFFFFF {
				t.Errorf("AffectedPointCode() = %#x exceeds 24 bits", got)
			}
		})
	}
}

// TestMTP3UserIdentityValues covers the DUPU MTP3-User Identity table of RFC
// 4666 Section 3.4.5. The values "align with those provided in the SS7 MTP3 User
// Part Unavailable message and Service Indicator", so a wrong constant does not
// fail locally — it tells the peer a different user part went away.
func TestMTP3UserIdentityValues(t *testing.T) {
	for _, c := range []struct {
		name string
		got  uint16
		want uint16
	}{
		{"SCCP", SCCP, 3},
		{"TUP", TUP, 4},
		{"ISUP", ISUP, 5},
		{"Broadband ISUP", BroadbandISUP, 9},
		{"Satellite ISUP", SatelliteISUP, 10},
		{"AAL type 2 Signalling", AAL2Signalling, 12},
		{"BICC", BICC, 13},
		{"Gateway Control Protocol", GatewayControlProtocol, 14},
	} {
		if c.got != c.want {
			t.Errorf("%s = %d, RFC 4666 3.4.5 assigns %d", c.name, c.got, c.want)
		}
	}
}

// TestUnavailabilityCauseValues covers the companion table in the same section:
//
//	0         Unknown
//	1         Unequipped Remote User
//	2         Inaccessible Remote User
func TestUnavailabilityCauseValues(t *testing.T) {
	for _, c := range []struct {
		name string
		got  uint16
		want uint16
	}{
		{"Unknown", UnknownCause, 0},
		{"Unequipped Remote User", Unequipped, 1},
		{"Inaccessible Remote User", Inaccessible, 2},
	} {
		if c.got != c.want {
			t.Errorf("%s = %d, RFC 4666 3.4.5 assigns %d", c.name, c.got, c.want)
		}
	}
}

// TestRoutingKeyAcceptsOnlyItsMandatorySubParameters covers the Routing Key
// format of RFC 4666 Section 3.6.1, where Local-RK-Identifier and Destination
// Point Code are mandatory and every other sub-parameter is marked "(optional)".
// A registration carrying exactly the mandatory pair is the minimal legal
// Routing Key and must decode.
func TestRoutingKeyAcceptsOnlyItsMandatorySubParameters(t *testing.T) {
	rk := NewRoutingKey(NewRoutingKeyPayload(
		NewLocalRoutingKeyIdentifier(1), nil, nil,
		NewDestinationPointCode(0x1234), nil, nil, nil,
	))
	b, err := rk.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}
	p, err := Parse(b)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	got, err := p.RoutingKey()
	if err != nil {
		t.Fatalf("RoutingKey: %v", err)
	}
	if got.LocalRoutingKeyIdentifier == nil {
		t.Error("Local-RK-Identifier was dropped")
	}
	if got.DestinationPointCode == nil {
		t.Error("Destination Point Code was dropped")
	}
}

// TestRegistrationResultDecodesByTagNotPosition covers RFC 4666 Section 3.2:
//
//	Where more than one parameter is included in a message, the
//	parameters may be in any order, except where explicitly mandated.  A
//	receiver SHOULD accept the parameters in any order.
//
// Decoding a Registration Result positionally is not merely fragile: with the
// order the RFC permits, the Registration Status lands in the Routing Context
// field and whatever occupies slot 1 is read as the status, so a rejected
// registration can be reported to the ASP as successful.
func TestRegistrationResultDecodesByTagNotPosition(t *testing.T) {
	const deniedPermission = PermissionDenied

	// The mandatory three, in an order the RFC explicitly permits.
	rr := newNestedParam(
		RegistrationResult,
		NewRoutingContext(0),
		NewRegistrationStatus(deniedPermission),
		NewLocalRoutingKeyIdentifier(1),
	)
	b, err := rr.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}
	p, err := Parse(b)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	got, err := p.RegistrationResult()
	if err != nil {
		t.Fatalf("RegistrationResult: %v", err)
	}

	if got.RegistrationStatus == nil {
		t.Fatal("Registration Status was not decoded")
	}
	if tag := got.RegistrationStatus.Tag; tag != RegistrationStatus {
		t.Errorf("RegistrationStatus field holds tag %#04x, want %#04x",
			tag, RegistrationStatus)
	}
	if s := got.RegistrationStatus.RegistrationStatus(); s != deniedPermission {
		t.Errorf("Registration Status = %d, want %d — a denied registration "+
			"must not read as anything else", s, deniedPermission)
	}
	if got.LocalRoutingKeyIdentifier == nil ||
		got.LocalRoutingKeyIdentifier.Tag != LocalRoutingKeyIdentifier {
		t.Error("Local-RK-Identifier was not decoded by tag")
	}
	if got.RoutingContext == nil || got.RoutingContext.Tag != RoutingContext {
		t.Error("Routing Context was not decoded by tag")
	}
}

// TestRoutingContextAccessorsTolerateNil pins the nil-receiver behaviour these
// accessors share with Copy.
//
// The Routing Context parameter is Conditional or Optional on nearly every
// message that can carry one, and a Config may leave it unset, so callers hold
// a nil *Param as a matter of course rather than as a mistake. Dereferencing it
// terminated the process: no recover() is installed, so one association's
// unset configuration took down every association at the SGP.
func TestRoutingContextAccessorsTolerateNil(t *testing.T) {
	var p *Param

	if got := p.RoutingContexts(); got != nil {
		t.Errorf("RoutingContexts() on nil = %v, want nil", got)
	}
	if got := p.RoutingContext(); got != 0 {
		t.Errorf("RoutingContext() on nil = %d, want 0", got)
	}
	if got := p.Copy(); got != nil {
		t.Errorf("Copy() on nil = %v, want nil", got)
	}
}
