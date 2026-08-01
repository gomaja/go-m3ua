// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package params

import "log"

// DeregResultPayload is the payload of DeregistrationResult.
type DeregResultPayload struct {
	RoutingContext, DeregistrationStatus *Param
	Others                               []*Param
}

// NewDeregResultPayload creates a new DeregResultPayload.
func NewDeregResultPayload(rtCtx, deregStatus *Param) *DeregResultPayload {
	return &DeregResultPayload{
		RoutingContext:       rtCtx,
		DeregistrationStatus: deregStatus,
	}
}

// NewDeregistrationResult creates a new DeregistrationResult.
// Note that this returns *Param, as no specific structure in this parameter.
func NewDeregistrationResult(dr *DeregResultPayload) *Param {
	if dr == nil {
		return invalidNestedParam(DeregistrationResult)
	}
	parameters := []*Param{
		dr.RoutingContext,
		dr.DeregistrationStatus,
	}
	parameters = append(parameters, dr.Others...)
	return newNestedParam(DeregistrationResult, parameters...)
}

// DeregistrationResult returns DeregResultPayload.
func (p *Param) DeregistrationResult() (*DeregResultPayload, error) {
	if p.Tag != DeregistrationResult {
		return nil, ErrInvalidType
	}

	d, err := ParseDeregResultPayload(p.Data)
	if err != nil {
		return nil, err
	}
	return d, nil
}

// ParseDeregResultPayload decodes given byte sequence as a DeregResultPayload.
func ParseDeregResultPayload(b []byte) (*DeregResultPayload, error) {
	return parseDeregResultPayloadAtDepth(b, 0)
}

func parseDeregResultPayloadAtDepth(b []byte, depth int) (*DeregResultPayload, error) {
	d := &DeregResultPayload{}
	if err := d.unmarshalBinaryAtDepth(b, depth); err != nil {
		return nil, err
	}
	return d, nil
}

// UnmarshalBinary sets the values retrieved from byte sequence in a Param.
func (d *DeregResultPayload) UnmarshalBinary(b []byte) error {
	return d.unmarshalBinaryAtDepth(b, 0)
}

func (d *DeregResultPayload) unmarshalBinaryAtDepth(b []byte, depth int) error {
	*d = DeregResultPayload{}
	ps, err := parseMultiParamsAtDepth(b, depth+1)
	if err != nil {
		return err
	}

	decoded := DeregResultPayload{}
	for _, p := range ps {
		switch p.Tag {
		case RoutingContext:
			if decoded.RoutingContext != nil {
				return duplicateNestedParameter("Deregistration Result", "Routing Context")
			}
			decoded.RoutingContext = p
		case DeregistrationStatus:
			if decoded.DeregistrationStatus != nil {
				return duplicateNestedParameter("Deregistration Result", "Deregistration Status")
			}
			decoded.DeregistrationStatus = p
		default:
			decoded.Others = append(decoded.Others, p)
		}
	}
	if decoded.RoutingContext == nil {
		return invalidNestedParameter("Deregistration Result", "missing Routing Context")
	}
	if decoded.DeregistrationStatus == nil {
		return invalidNestedParameter("Deregistration Result", "missing Deregistration Status")
	}
	if len(decoded.RoutingContext.Data) != 4 {
		return invalidNestedLength("Deregistration Result Routing Context", len(decoded.RoutingContext.Data), 4)
	}
	*d = decoded
	return nil
}

// DecodeDeregResultPayload decodes given byte sequence as a DeregResultPayload.
//
// DEPRECATED: use ParseDeregResultPayload instead.
func DecodeDeregResultPayload(b []byte) (*DeregResultPayload, error) {
	log.Println("DEPRECATED: use ParseDeregResultPayload instead")
	return ParseDeregResultPayload(b)
}

// DecodeFromBytes sets the values retrieved from byte sequence in a Param.
//
// DEPRECATED: use UnmarshalBinary instead.
func (d *DeregResultPayload) DecodeFromBytes(b []byte) error {
	log.Println("DEPRECATED: use UnmarshalBinary instead")
	return d.UnmarshalBinary(b)
}
