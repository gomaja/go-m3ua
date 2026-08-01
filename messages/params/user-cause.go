// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package params

// Unavailability Cause definitions, from the table in RFC 4666 Section 3.4.5:
//
//	0         Unknown
//	1         Unequipped Remote User
//	2         Inaccessible Remote User
//
// "The values agree with those provided in the SS7 MTP3 User Part Unavailable
// message", so these travel to the peer as they are.
const (
	UnknownCause uint16 = iota
	Unequipped
	Inaccessible
)

// UserIdentityUnknown is the Unavailability Cause "Unknown".
//
// DEPRECATED: it was grouped with the MTP3-User Identity values under a
// misleading name; use UnknownCause. The value is unchanged.
const UserIdentityUnknown = UnknownCause

// MTP3-User Identity definitions, from the table in RFC 4666 Section 3.4.5:
//
//	0 to 2   Reserved
//	   3     SCCP
//	   4     TUP
//	   5     ISUP
//	6 to 8   Reserved
//	   9     Broadband ISUP
//	  10     Satellite ISUP
//	  11     Reserved
//	  12     AAL type 2 Signalling
//	  13     Bearer Independent Call Control (BICC)
//	  14     Gateway Control Protocol
//	  15     Reserved
//
// "The values align with those provided in the SS7 MTP3 User Part Unavailable
// message and Service Indicator."
//
// These constants previously started at 1 rather than 3, so every one of them
// was wrong by the width of the reserved ranges — a DUPU built with ISUP named
// SCCP to the peer, and one built with BICC named Satellite ISUP. Anything that
// stored or compared the old numeric values needs revisiting.
const (
	SCCP uint16 = iota + 3
	TUP
	ISUP
	_ // 6 to 8: Reserved
	_
	_
	BroadbandISUP
	SatelliteISUP
	_ // 11: Reserved
	AAL2Signalling
	BICC
	GatewayControlProtocol
	_ // 15: Reserved
)

// NewUserCause creates the User/Cause Parameter.
// Note that this returns *Param, as no specific structure in this parameter.
func NewUserCause(user, cause uint16) *Param {
	comb := uint32(cause)<<16 | uint32(user)
	return newUint32ValParam(UserCause, comb)
}

// UserCause returns multiple UserCause from Param.
func (p *Param) UserCause() uint32 {
	if p.Tag != UserCause {
		return 0
	}

	return p.decodeUint32ValData()
}

// UserIdentity returns multiple UserIdentity from Param.
func (p *Param) UserIdentity() uint16 {
	if p.Tag != UserCause {
		return 0
	}

	return uint16(p.decodeUint32ValData() & 0xffff)
}

// UnavailabilityCause returns multiple UnavailabilityCause from Param.
func (p *Param) UnavailabilityCause() uint16 {
	if p.Tag != UserCause {
		return 0
	}

	return uint16(p.decodeUint32ValData() >> 16)
}
