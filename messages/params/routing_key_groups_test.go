// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package params

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
)

// RFC 4666 Section 3.6.1 repeats the Destination Point Code, Service
// Indicators and Originating Point Code List grouping within one Routing Key.
// Groups is therefore the only representation of that structure: a scalar
// shortcut describing "the first group" cannot express the parameter and
// silently disagrees with Groups once a second grouping arrives.
func TestRoutingKeyPayloadExposesOnlyGroupedScope(t *testing.T) {
	payloadType := reflect.TypeOf(RoutingKeyPayload{})
	for _, name := range []string{"DestinationPointCode", "ServiceIndicators", "OriginatingPointCodeList"} {
		if field, exists := payloadType.FieldByName(name); exists {
			t.Errorf("RoutingKeyPayload still exposes scalar shortcut field %s %s; "+
				"RFC 4666 Section 3.6.1 grouping belongs only to Groups", field.Name, field.Type)
		}
	}
}

// Every grouping survives one encode and decode, and the decoded groups keep
// the wire order the peer sent.
func TestRoutingKeyGroupsRoundTrip(t *testing.T) {
	groups := []RoutingKeyGroup{
		NewRoutingKeyGroup(NewDestinationPointCode(3), NewServiceIndicators(ServiceIndSCCP), NewOriginatingPointCodeList(5)),
		NewRoutingKeyGroup(NewDestinationPointCode(11), nil, nil),
		NewRoutingKeyGroup(NewDestinationPointCode(12), NewServiceIndicators(ServiceIndISUP), nil),
	}
	payload := NewRoutingKeyPayload(
		NewLocalRoutingKeyIdentifier(7),
		NewRoutingContext(9),
		NewTrafficModeType(TrafficModeLoadshare),
		NewNetworkAppearance(4),
		groups...,
	)
	if len(payload.Groups) != len(groups) {
		t.Fatalf("constructed payload kept %d groupings, want %d", len(payload.Groups), len(groups))
	}
	encoded, err := NewRoutingKey(payload).MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	parsed, err := Parse(encoded)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	decoded, err := parsed.RoutingKey()
	if err != nil {
		t.Fatalf("RoutingKey() error = %v", err)
	}
	if len(decoded.Groups) != len(groups) {
		t.Fatalf("decoded %d groups, want %d", len(decoded.Groups), len(groups))
	}
	for index := range groups {
		assertParamEqual(t, "group DPC", decoded.Groups[index].DestinationPointCode, groups[index].DestinationPointCode)
		assertParamEqual(t, "group SI", decoded.Groups[index].ServiceIndicators, groups[index].ServiceIndicators)
		assertParamEqual(t, "group OPC list", decoded.Groups[index].OriginatingPointCodeList, groups[index].OriginatingPointCodeList)
	}
	assertParamEqual(t, "Network Appearance", decoded.NetworkAppearance, payload.NetworkAppearance)
	assertParamEqual(t, "Routing Context", decoded.RoutingContext, payload.RoutingContext)
	assertParamEqual(t, "Traffic Mode Type", decoded.TrafficModeType, payload.TrafficModeType)
}

// The encoded sub-parameter order is the RFC 4666 Section 3.6.1 diagram order:
// the singletons lead, the first grouping carries Network Appearance between
// its Destination Point Code and Service Indicators, and every further
// grouping follows as DPC, SI, OPC list.
func TestRoutingKeyGroupsKeepDiagramOrder(t *testing.T) {
	first := NewRoutingKeyGroup(NewDestinationPointCode(3), NewServiceIndicators(ServiceIndSCCP), NewOriginatingPointCodeList(5))
	second := NewRoutingKeyGroup(NewDestinationPointCode(11), NewServiceIndicators(ServiceIndISUP), NewOriginatingPointCodeList(6))
	payload := NewRoutingKeyPayload(
		NewLocalRoutingKeyIdentifier(1),
		NewRoutingContext(2),
		NewTrafficModeType(TrafficModeBroadcast),
		NewNetworkAppearance(4),
		first,
		second,
	)
	want := joinNestedParams(t,
		payload.LocalRoutingKeyIdentifier,
		payload.RoutingContext,
		payload.TrafficModeType,
		first.DestinationPointCode,
		payload.NetworkAppearance,
		first.ServiceIndicators,
		first.OriginatingPointCodeList,
		second.DestinationPointCode,
		second.ServiceIndicators,
		second.OriginatingPointCodeList,
	)
	if got := NewRoutingKey(payload).Data; !bytes.Equal(got, want) {
		t.Fatalf("grouped Routing Key value = %x, want %x", got, want)
	}
}

// The strict rejections that guarded the previous representation stay exactly
// as strict once the scalar shortcuts are gone.
func TestRoutingKeyGroupedNegativeCases(t *testing.T) {
	tests := []struct {
		name  string
		build func() *Param
		want  error
	}{
		{
			name:  "nil payload",
			build: func() *Param { return NewRoutingKey(nil) },
			want:  ErrInvalidValue,
		},
		{
			name: "no grouping",
			build: func() *Param {
				return NewRoutingKey(NewRoutingKeyPayload(NewLocalRoutingKeyIdentifier(1), nil, nil, nil))
			},
			want: ErrInvalidValue,
		},
		{
			name: "grouping without Destination Point Code",
			build: func() *Param {
				return NewRoutingKey(NewRoutingKeyPayload(
					NewLocalRoutingKeyIdentifier(1), nil, nil, nil,
					NewRoutingKeyGroup(nil, NewServiceIndicators(ServiceIndSCCP), nil),
				))
			},
			want: ErrInvalidValue,
		},
		{
			name: "second grouping without Destination Point Code",
			build: func() *Param {
				return NewRoutingKey(NewRoutingKeyPayload(
					NewLocalRoutingKeyIdentifier(1), nil, nil, nil,
					NewRoutingKeyGroup(NewDestinationPointCode(1), nil, nil),
					NewRoutingKeyGroup(nil, nil, nil),
				))
			},
			want: ErrInvalidValue,
		},
		{
			name: "invalid Traffic Mode Type",
			build: func() *Param {
				return NewRoutingKey(NewRoutingKeyPayload(
					NewLocalRoutingKeyIdentifier(1), nil, NewTrafficModeType(0), nil,
					NewRoutingKeyGroup(NewDestinationPointCode(0x1234), nil, nil),
				))
			},
			want: ErrInvalidValue,
		},
		{
			name: "empty Routing Context",
			build: func() *Param {
				return NewRoutingKey(NewRoutingKeyPayload(
					NewLocalRoutingKeyIdentifier(1), NewRoutingContext(), nil, nil,
					NewRoutingKeyGroup(NewDestinationPointCode(1), nil, nil),
				))
			},
			want: ErrInvalidLength,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := test.build().MarshalBinary(); !errors.Is(err, test.want) {
				t.Fatalf("MarshalBinary() error = %v, want %v", err, test.want)
			}
		})
	}
}

func assertParamEqual(t *testing.T, name string, got, want *Param) {
	t.Helper()
	if want == nil {
		if got != nil {
			t.Fatalf("%s = %v, want absent", name, got)
		}
		return
	}
	if got == nil {
		t.Fatalf("%s absent, want %v", name, want)
	}
	if got.Tag != want.Tag || !bytes.Equal(got.Data, want.Data) {
		t.Fatalf("%s = %#04x %x, want %#04x %x", name, got.Tag, got.Data, want.Tag, want.Data)
	}
}
