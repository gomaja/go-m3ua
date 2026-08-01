// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package params

import "log"

// RegistrationResultPayload is the payload of RegistrationResult.
type RegistrationResultPayload struct {
	LocalRoutingKeyIdentifier, RegistrationStatus, RoutingContext *Param
	Others                                                        []*Param
}

// NewRegistrationResultPayload creates a new RegistrationResultPayload.
func NewRegistrationResultPayload(rkID, regStatus, rtCtx *Param) *RegistrationResultPayload {
	return &RegistrationResultPayload{
		LocalRoutingKeyIdentifier: rkID,
		RegistrationStatus:        regStatus,
		RoutingContext:            rtCtx,
	}
}

// NewRegistrationResult creates a new RegistrationResult.
// Note that this returns *Param, as no specific structure in this parameter.
func NewRegistrationResult(rr *RegistrationResultPayload) *Param {
	if rr == nil {
		return invalidNestedParam(RegistrationResult)
	}
	parameters := []*Param{
		rr.LocalRoutingKeyIdentifier,
		rr.RegistrationStatus,
		rr.RoutingContext,
	}
	parameters = append(parameters, rr.Others...)
	return newNestedParam(RegistrationResult, parameters...)
}

// RegistrationResult returns RegistrationResultPayload.
func (p *Param) RegistrationResult() (*RegistrationResultPayload, error) {
	if p.Tag != RegistrationResult {
		return nil, ErrInvalidType
	}

	d, err := ParseRegistrationResultPayload(p.Data)
	if err != nil {
		return nil, err
	}
	return d, nil
}

// ParseRegistrationResultPayload decodes given byte sequence as a RegistrationResultPayload.
func ParseRegistrationResultPayload(b []byte) (*RegistrationResultPayload, error) {
	return parseRegistrationResultPayloadAtDepth(b, 0)
}

func parseRegistrationResultPayloadAtDepth(b []byte, depth int) (*RegistrationResultPayload, error) {
	d := &RegistrationResultPayload{}
	if err := d.unmarshalBinaryAtDepth(b, depth); err != nil {
		return nil, err
	}
	return d, nil
}

// UnmarshalBinary sets the values retrieved from byte sequence in a Param.
func (d *RegistrationResultPayload) UnmarshalBinary(b []byte) error {
	return d.unmarshalBinaryAtDepth(b, 0)
}

func (d *RegistrationResultPayload) unmarshalBinaryAtDepth(b []byte, depth int) error {
	*d = RegistrationResultPayload{}
	ps, err := parseMultiParamsAtDepth(b, depth+1)
	if err != nil {
		return err
	}

	decoded := RegistrationResultPayload{}
	// By tag, not by position. RFC 4666 Section 3.2: "Where more than one
	// parameter is included in a message, the parameters may be in any order,
	// except where explicitly mandated.  A receiver SHOULD accept the
	// parameters in any order."
	//
	// Assigning positionally was not merely brittle. Given an order the RFC
	// permits, the Registration Status landed in the Routing Context field and
	// whatever occupied the middle slot was read as the status, so a
	// registration the SG had refused could be reported to the ASP as
	// successful.
	for _, p := range ps {
		switch p.Tag {
		case LocalRoutingKeyIdentifier:
			if decoded.LocalRoutingKeyIdentifier != nil {
				return duplicateNestedParameter("Registration Result", "Local RK Identifier")
			}
			decoded.LocalRoutingKeyIdentifier = p
		case RegistrationStatus:
			if decoded.RegistrationStatus != nil {
				return duplicateNestedParameter("Registration Result", "Registration Status")
			}
			decoded.RegistrationStatus = p
		case RoutingContext:
			if decoded.RoutingContext != nil {
				return duplicateNestedParameter("Registration Result", "Routing Context")
			}
			decoded.RoutingContext = p
		default:
			decoded.Others = append(decoded.Others, p)
		}
	}

	// All three are mandatory in the Registration Result format of Section
	// 3.6.2.
	if decoded.LocalRoutingKeyIdentifier == nil {
		return invalidNestedParameter("Registration Result", "missing Local RK Identifier")
	}
	if decoded.RegistrationStatus == nil {
		return invalidNestedParameter("Registration Result", "missing Registration Status")
	}
	if decoded.RoutingContext == nil {
		return invalidNestedParameter("Registration Result", "missing Routing Context")
	}
	if len(decoded.RoutingContext.Data) != 4 {
		return invalidNestedLength("Registration Result Routing Context", len(decoded.RoutingContext.Data), 4)
	}

	status := decoded.RegistrationStatus.RegistrationStatus()
	routingContext := decoded.RoutingContext.RoutingContext()
	if status != SuccessfullyRegistered && status != RoutingKeyAlreadyRegistered && routingContext != 0 {
		return invalidNestedParameter("Registration Result", "failure status requires Routing Context 0")
	}
	*d = decoded
	return nil
}

// DecodeRegistrationResultPayload decodes given byte sequence as a RegistrationResultPayload.
//
// DEPRECATED: use ParseRegistrationResultPayload instead.
func DecodeRegistrationResultPayload(b []byte) (*RegistrationResultPayload, error) {
	log.Println("DEPRECATED: use ParseRegistrationResultPayload instead")
	return ParseRegistrationResultPayload(b)
}

// DecodeFromBytes sets the values retrieved from byte sequence in a Param.
//
// DEPRECATED: use UnmarshalBinary instead.
func (d *RegistrationResultPayload) DecodeFromBytes(b []byte) error {
	log.Println("DEPRECATED: use UnmarshalBinary instead")
	return d.UnmarshalBinary(b)
}
