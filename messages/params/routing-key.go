// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package params

import (
	"fmt"
	"log"
)

// RoutingKeyGroup is one Destination Point Code and its optional Service
// Indicators and Originating Point Code List.
//
// RFC 4666 Section 3.6.1 permits this three-parameter grouping to repeat
// within one Routing Key. DestinationPointCode is mandatory in every group.
type RoutingKeyGroup struct {
	DestinationPointCode     *Param
	ServiceIndicators        *Param
	OriginatingPointCodeList *Param
}

// NewRoutingKeyGroup creates one repeatable Routing Key grouping.
func NewRoutingKeyGroup(dpc, si, opcs *Param) RoutingKeyGroup {
	return RoutingKeyGroup{
		DestinationPointCode:     dpc,
		ServiceIndicators:        si,
		OriginatingPointCodeList: opcs,
	}
}

// RoutingKeyPayload is the payload of RoutingKey.
type RoutingKeyPayload struct {
	LocalRoutingKeyIdentifier, RoutingContext, TrafficModeType, DestinationPointCode, NetworkAppearance, ServiceIndicators, OriginatingPointCodeList *Param

	// Groups contains every DPC-led grouping in wire order. The three legacy
	// singular fields above continue to describe the first group.
	Groups []RoutingKeyGroup
	Others []*Param
}

// NewRoutingKeyPayload creates a new RoutingKeyPayload.
func NewRoutingKeyPayload(rkID, rtCtx, tmType, dpc, nwApr, si, opcs *Param) *RoutingKeyPayload {
	return &RoutingKeyPayload{
		LocalRoutingKeyIdentifier: rkID,
		RoutingContext:            rtCtx,
		TrafficModeType:           tmType,
		DestinationPointCode:      dpc,
		NetworkAppearance:         nwApr,
		ServiceIndicators:         si,
		OriginatingPointCodeList:  opcs,
	}
}

// NewRoutingKeyPayloadWithGroups creates a Routing Key payload with one or
// more DPC-led groupings. Groups are authoritative when NewRoutingKey encodes
// this payload; the legacy singular fields are populated from the first group.
func NewRoutingKeyPayloadWithGroups(rkID, rtCtx, tmType, nwApr *Param, groups ...RoutingKeyGroup) *RoutingKeyPayload {
	payload := &RoutingKeyPayload{
		LocalRoutingKeyIdentifier: rkID,
		RoutingContext:            rtCtx,
		TrafficModeType:           tmType,
		NetworkAppearance:         nwApr,
		Groups:                    append([]RoutingKeyGroup(nil), groups...),
	}
	payload.setLegacyGroup()
	return payload
}

// Note that this parameter contains some optional parameters inside.

// NewRoutingKey creates a new RoutingKey.
// Note that this returns *Param, as no specific structure in this parameter.
func NewRoutingKey(rk *RoutingKeyPayload) *Param {
	if rk == nil {
		return invalidNestedParam(RoutingKey)
	}

	parameters := []*Param{
		rk.LocalRoutingKeyIdentifier,
		rk.RoutingContext,
		rk.TrafficModeType,
	}
	groups := rk.Groups
	if len(groups) == 0 {
		groups = []RoutingKeyGroup{{
			DestinationPointCode:     rk.DestinationPointCode,
			ServiceIndicators:        rk.ServiceIndicators,
			OriginatingPointCodeList: rk.OriginatingPointCodeList,
		}}
	}
	for _, group := range groups {
		if group.DestinationPointCode == nil {
			return invalidNestedParam(RoutingKey)
		}
	}
	for groupIndex, group := range groups {
		parameters = append(parameters, group.DestinationPointCode)
		if groupIndex == 0 {
			// Preserve the legacy constructor's RFC diagram order exactly.
			parameters = append(parameters, rk.NetworkAppearance)
		}
		parameters = append(parameters, group.ServiceIndicators, group.OriginatingPointCodeList)
	}
	parameters = append(parameters, rk.Others...)
	return newNestedParam(RoutingKey, parameters...)
}

// RoutingKey returns RoutingKeyPayload.
func (p *Param) RoutingKey() (*RoutingKeyPayload, error) {
	if p.Tag != RoutingKey {
		return nil, ErrInvalidType
	}

	r, err := ParseRoutingKeyPayload(p.Data)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// ParseRoutingKeyPayload decodes given byte sequence as a RoutingKeyPayload.
func ParseRoutingKeyPayload(b []byte) (*RoutingKeyPayload, error) {
	return parseRoutingKeyPayloadAtDepth(b, 0)
}

func parseRoutingKeyPayloadAtDepth(b []byte, depth int) (*RoutingKeyPayload, error) {
	r := &RoutingKeyPayload{}
	if err := r.unmarshalBinaryAtDepth(b, depth); err != nil {
		return nil, err
	}
	return r, nil
}

// UnmarshalBinary sets the values retrieved from byte sequence in a Param.
func (r *RoutingKeyPayload) UnmarshalBinary(b []byte) error {
	return r.unmarshalBinaryAtDepth(b, 0)
}

func (r *RoutingKeyPayload) unmarshalBinaryAtDepth(b []byte, depth int) error {
	*r = RoutingKeyPayload{}
	ps, err := parseMultiParamsAtDepth(b, depth+1)
	if err != nil {
		return err
	}

	decoded := RoutingKeyPayload{}
	for _, p := range ps {
		switch p.Tag {
		case LocalRoutingKeyIdentifier:
			if decoded.LocalRoutingKeyIdentifier != nil {
				return duplicateNestedParameter("Routing Key", "Local RK Identifier")
			}
			decoded.LocalRoutingKeyIdentifier = p
		case RoutingContext:
			if decoded.RoutingContext != nil {
				return duplicateNestedParameter("Routing Key", "Routing Context")
			}
			decoded.RoutingContext = p
		case TrafficModeType:
			if decoded.TrafficModeType != nil {
				return duplicateNestedParameter("Routing Key", "Traffic Mode Type")
			}
			decoded.TrafficModeType = p
		case DestinationPointCode:
			decoded.Groups = append(decoded.Groups, RoutingKeyGroup{DestinationPointCode: p})
		case NetworkAppearance:
			if decoded.NetworkAppearance != nil {
				return duplicateNestedParameter("Routing Key", "Network Appearance")
			}
			decoded.NetworkAppearance = p
		case ServiceIndicators:
			if len(decoded.Groups) == 0 {
				return ungroupedRoutingKeyParameter("Service Indicators")
			}
			group := &decoded.Groups[len(decoded.Groups)-1]
			if group.ServiceIndicators != nil {
				return duplicateNestedParameter("Routing Key group", "Service Indicators")
			}
			if bytesContain(p.Data, 0) {
				return invalidNestedParameter("Routing Key", "Service Indicators contains MTP management SI 0")
			}
			group.ServiceIndicators = p
		case OriginatingPointCodeList:
			if len(decoded.Groups) == 0 {
				return ungroupedRoutingKeyParameter("Originating Point Code List")
			}
			group := &decoded.Groups[len(decoded.Groups)-1]
			if group.OriginatingPointCodeList != nil {
				return duplicateNestedParameter("Routing Key group", "Originating Point Code List")
			}
			group.OriginatingPointCodeList = p
		default:
			if isDefinedParameterTag(p.Tag) {
				return invalidNestedParameter("Routing Key", fmt.Sprintf("unexpected parameter tag %#04x", p.Tag))
			}
			decoded.Others = append(decoded.Others, p)
		}
	}

	// The Routing Key format in RFC 4666 Section 3.6.1 marks every
	// sub-parameter "(optional)" except these two, so the pair on its own is
	// the smallest legal Routing Key. Counting the sub-parameters instead
	// rejected exactly that form, which is what an ASP registering a whole
	// point code without narrowing by service or originator sends.
	if decoded.LocalRoutingKeyIdentifier == nil {
		return invalidNestedParameter("Routing Key", "missing Local RK Identifier")
	}
	if len(decoded.Groups) == 0 {
		return invalidNestedParameter("Routing Key", "missing Destination Point Code")
	}
	if decoded.RoutingContext != nil && len(decoded.RoutingContext.Data) != 4 {
		return invalidNestedLength("Routing Key Routing Context", len(decoded.RoutingContext.Data), 4)
	}
	decoded.setLegacyGroup()
	*r = decoded
	return nil
}

func (r *RoutingKeyPayload) setLegacyGroup() {
	if len(r.Groups) == 0 {
		return
	}
	r.DestinationPointCode = r.Groups[0].DestinationPointCode
	r.ServiceIndicators = r.Groups[0].ServiceIndicators
	r.OriginatingPointCodeList = r.Groups[0].OriginatingPointCodeList
}

func bytesContain(value []byte, target byte) bool {
	for _, octet := range value {
		if octet == target {
			return true
		}
	}
	return false
}

func ungroupedRoutingKeyParameter(name string) error {
	return invalidNestedParameter("Routing Key", name+" precedes its Destination Point Code")
}

// DecodeRoutingKeyPayload decodes given byte sequence as a RoutingKeyPayload.
//
// DEPRECATED: use ParseRoutingKeyPayload instead.
func DecodeRoutingKeyPayload(b []byte) (*RoutingKeyPayload, error) {
	log.Println("DEPRECATED: use ParseRoutingKeyPayload instead")
	return ParseRoutingKeyPayload(b)
}

// DecodeFromBytes sets the values retrieved from byte sequence in a Param.
//
// DEPRECATED: use UnmarshalBinary instead.
func (r *RoutingKeyPayload) DecodeFromBytes(b []byte) error {
	log.Println("DEPRECATED: use UnmarshalBinary instead")
	return r.UnmarshalBinary(b)
}
