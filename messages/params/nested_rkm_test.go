package params

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestNestedConstructorsSurfaceChildErrorsAndAreNilSafe(t *testing.T) {
	tests := []struct {
		name  string
		build func() *Param
		want  error
	}{
		{
			name:  "nil Routing Key payload",
			build: func() *Param { return NewRoutingKey(nil) },
			want:  ErrInvalidValue,
		},
		{
			name: "nil Registration Result payload",
			build: func() *Param {
				return NewRegistrationResult(nil)
			},
			want: ErrInvalidValue,
		},
		{
			name: "nil Deregistration Result payload",
			build: func() *Param {
				return NewDeregistrationResult(nil)
			},
			want: ErrInvalidValue,
		},
		{
			name: "invalid nested Traffic Mode Type",
			build: func() *Param {
				return NewRoutingKey(NewRoutingKeyPayload(
					NewLocalRoutingKeyIdentifier(1),
					nil,
					NewTrafficModeType(0),
					NewDestinationPointCode(0x1234),
					nil,
					nil,
					nil,
				))
			},
			want: ErrInvalidValue,
		},
		{
			name: "invalid nested Registration Status",
			build: func() *Param {
				return NewRegistrationResult(NewRegistrationResultPayload(
					NewLocalRoutingKeyIdentifier(1),
					NewRegistrationStatus(RoutingKeyAlreadyRegistered+1),
					NewRoutingContext(0),
				))
			},
			want: ErrInvalidValue,
		},
		{
			name: "invalid nested Deregistration Status",
			build: func() *Param {
				return NewDeregistrationResult(NewDeregResultPayload(
					NewRoutingContext(1),
					NewDeregistrationStatus(DeregASPActiveForRoutingContext+1),
				))
			},
			want: ErrInvalidValue,
		},
		{
			name: "invalid nested Routing Context length",
			build: func() *Param {
				return NewRoutingKey(NewRoutingKeyPayload(
					NewLocalRoutingKeyIdentifier(1),
					NewRoutingContext(),
					nil,
					NewDestinationPointCode(1),
					nil,
					nil,
					nil,
				))
			},
			want: ErrInvalidLength,
		},
		{
			name: "oversized nested extension",
			build: func() *Param {
				payload := NewDeregResultPayload(
					NewRoutingContext(1),
					NewDeregistrationStatus(SuccessfullyDeregistered),
				)
				payload.Others = []*Param{{Tag: 0x7ffe, Data: make([]byte, maxParamValueLength+1)}}
				return NewDeregistrationResult(payload)
			},
			want: ErrInvalidLength,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			param := buildNestedWithoutPanic(t, test.build)
			if param == nil {
				t.Fatal("constructor returned nil")
			}
			if _, err := param.MarshalBinary(); !errors.Is(err, test.want) {
				t.Fatalf("MarshalBinary() error = %v, want %v", err, test.want)
			}
			if copied := param.Copy(); copied == nil {
				t.Fatal("Copy() returned nil")
			} else if _, err := copied.MarshalBinary(); !errors.Is(err, test.want) {
				t.Fatalf("copied MarshalBinary() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestNestedParametersAreValidatedAtOuterBoundary(t *testing.T) {
	invalidTrafficMode := bytes.Join([][]byte{
		joinNestedParams(t, NewLocalRoutingKeyIdentifier(1)),
		rawValueParam(TrafficModeType, uint32Bytes(0)),
		joinNestedParams(t, NewDestinationPointCode(0x1234)),
	}, nil)
	failedRegistrationWithRoutingContext := joinNestedParams(t,
		NewLocalRoutingKeyIdentifier(1),
		NewRegistrationStatus(PermissionDenied),
		NewRoutingContext(7),
	)
	duplicateDeregistrationContext := joinNestedParams(t,
		NewRoutingContext(1),
		NewRoutingContext(2),
		NewDeregistrationStatus(SuccessfullyDeregistered),
	)

	tests := []struct {
		name  string
		tag   uint16
		value []byte
		want  error
	}{
		{
			name:  "Routing Key child value",
			tag:   RoutingKey,
			value: invalidTrafficMode,
			want:  ErrInvalidValue,
		},
		{
			name:  "Routing Key missing mandatory child",
			tag:   RoutingKey,
			value: joinNestedParams(t, NewDestinationPointCode(0x1234)),
			want:  ErrInvalidValue,
		},
		{
			name:  "Registration Result cross-field value",
			tag:   RegistrationResult,
			value: failedRegistrationWithRoutingContext,
			want:  ErrInvalidValue,
		},
		{
			name:  "Deregistration Result duplicate",
			tag:   DeregistrationResult,
			value: duplicateDeregistrationContext,
			want:  ErrInvalidValue,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			param := &Param{Tag: test.tag, Data: bytes.Clone(test.value)}
			if _, err := param.MarshalBinary(); !errors.Is(err, test.want) {
				t.Fatalf("MarshalBinary() error = %v, want %v", err, test.want)
			}
			if _, err := Parse(rawValueParam(test.tag, test.value)); !errors.Is(err, test.want) {
				t.Fatalf("Parse() error = %v, want %v", err, test.want)
			}

			receiver := NewInfoString("stale")
			if err := receiver.UnmarshalBinary(rawValueParam(test.tag, test.value)); !errors.Is(err, test.want) {
				t.Fatalf("reused UnmarshalBinary() error = %v, want %v", err, test.want)
			}
			if receiver.Tag != 0 || receiver.Length != 0 || receiver.Data != nil {
				t.Fatalf("failed outer decode retained receiver state: %+v", receiver)
			}
		})
	}
}

func TestNestedTypedAccessorsRejectInvalidPayload(t *testing.T) {
	tests := []struct {
		name   string
		access func() error
	}{
		{
			name: "Routing Key",
			access: func() error {
				_, err := (&Param{Tag: RoutingKey}).RoutingKey()
				return err
			},
		},
		{
			name: "Registration Result",
			access: func() error {
				_, err := (&Param{Tag: RegistrationResult}).RegistrationResult()
				return err
			},
		},
		{
			name: "Deregistration Result",
			access: func() error {
				_, err := (&Param{Tag: DeregistrationResult}).DeregistrationResult()
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := test.access(); !errors.Is(err, ErrInvalidValue) {
				t.Fatalf("typed accessor error = %v, want ErrInvalidValue", err)
			}
		})
	}

	wrongType := NewInfoString("not nested")
	if _, err := wrongType.RoutingKey(); !errors.Is(err, ErrInvalidType) {
		t.Errorf("RoutingKey() wrong-type error = %v, want ErrInvalidType", err)
	}
	if _, err := wrongType.RegistrationResult(); !errors.Is(err, ErrInvalidType) {
		t.Errorf("RegistrationResult() wrong-type error = %v, want ErrInvalidType", err)
	}
	if _, err := wrongType.DeregistrationResult(); !errors.Is(err, ErrInvalidType) {
		t.Errorf("DeregistrationResult() wrong-type error = %v, want ErrInvalidType", err)
	}
}

func TestRoutingKeyRejectsDuplicateAndUngroupedKnownParameters(t *testing.T) {
	localIdentifier := NewLocalRoutingKeyIdentifier(1)
	destination := NewDestinationPointCode(0x1234)

	tests := []struct {
		name   string
		params []*Param
	}{
		{
			name:   "missing Destination Point Code",
			params: []*Param{localIdentifier},
		},
		{
			name:   "duplicate Local RK Identifier",
			params: []*Param{localIdentifier, NewLocalRoutingKeyIdentifier(2), destination},
		},
		{
			name:   "duplicate Routing Context",
			params: []*Param{localIdentifier, NewRoutingContext(1), NewRoutingContext(2), destination},
		},
		{
			name:   "duplicate Traffic Mode Type",
			params: []*Param{localIdentifier, NewTrafficModeType(1), NewTrafficModeType(2), destination},
		},
		{
			name:   "duplicate Network Appearance",
			params: []*Param{localIdentifier, destination, NewNetworkAppearance(1), NewNetworkAppearance(2)},
		},
		{
			name: "duplicate Service Indicators in one group",
			params: []*Param{
				localIdentifier,
				destination,
				NewServiceIndicators(ServiceIndSCCP),
				NewServiceIndicators(ServiceIndISUP),
			},
		},
		{
			name: "duplicate OPC List in one group",
			params: []*Param{
				localIdentifier,
				destination,
				NewOriginatingPointCodeList(1),
				NewOriginatingPointCodeList(2),
			},
		},
		{
			name: "Service Indicators before first DPC",
			params: []*Param{
				localIdentifier,
				NewServiceIndicators(ServiceIndSCCP),
				destination,
			},
		},
		{
			name: "OPC List before first DPC",
			params: []*Param{
				localIdentifier,
				NewOriginatingPointCodeList(1),
				destination,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := joinNestedParams(t, test.params...)
			if _, err := ParseRoutingKeyPayload(value); !errors.Is(err, ErrInvalidValue) {
				t.Fatalf("ParseRoutingKeyPayload() error = %v, want ErrInvalidValue", err)
			}
		})
	}
}

func TestRoutingKeyLegacyFieldsDescribeFirstRepeatedGroup(t *testing.T) {
	value := joinNestedParams(t,
		NewLocalRoutingKeyIdentifier(1),
		NewDestinationPointCode(0x1111),
		NewServiceIndicators(ServiceIndSCCP),
		NewOriginatingPointCodeList(0x01001111),
		NewDestinationPointCode(0x2222),
		NewServiceIndicators(ServiceIndISUP),
		NewOriginatingPointCodeList(0x02002222),
	)

	decoded, err := ParseRoutingKeyPayload(value)
	if err != nil {
		t.Fatalf("ParseRoutingKeyPayload() error = %v", err)
	}
	if got := decoded.DestinationPointCode.DestinationPointCode(); got != 0x1111 {
		t.Errorf("legacy DestinationPointCode = %#x, want first group %#x", got, 0x1111)
	}
	if got := decoded.ServiceIndicators.ServiceIndicators(); !bytes.Equal(got, []byte{ServiceIndSCCP}) {
		t.Errorf("legacy ServiceIndicators = %v, want first group [%d]", got, ServiceIndSCCP)
	}
	if got := decoded.OriginatingPointCodeList.OriginatingPointCodeList(); len(got) != 1 || got[0] != 0x01001111 {
		t.Errorf("legacy OriginatingPointCodeList = %v, want first group [0x01001111]", got)
	}
}

func TestRoutingKeyRepeatedGroupsRoundTrip(t *testing.T) {
	firstGroup := NewRoutingKeyGroup(
		NewDestinationPointCode(0x1111),
		NewServiceIndicators(ServiceIndSCCP),
		NewOriginatingPointCodeList(0x01001111),
	)
	secondGroup := NewRoutingKeyGroup(
		NewDestinationPointCode(0x2222),
		NewServiceIndicators(ServiceIndISUP, ServiceIndTUP),
		NewOriginatingPointCodeList(0x02002222, 0x03002222),
	)
	firstExtension := NewParam(0x7ffe, []byte{0xaa})
	secondExtension := NewParam(0x7ffe, []byte{0xbb, 0xcc})
	payload := NewRoutingKeyPayloadWithGroups(
		NewLocalRoutingKeyIdentifier(1),
		NewRoutingContext(2),
		NewTrafficModeType(TrafficModeLoadshare),
		NewNetworkAppearance(3),
		firstGroup,
		secondGroup,
	)
	payload.Others = []*Param{firstExtension, secondExtension}

	parameter := NewRoutingKey(payload)
	wantValue := joinNestedParams(t,
		payload.LocalRoutingKeyIdentifier,
		payload.RoutingContext,
		payload.TrafficModeType,
		firstGroup.DestinationPointCode,
		payload.NetworkAppearance,
		firstGroup.ServiceIndicators,
		firstGroup.OriginatingPointCodeList,
		secondGroup.DestinationPointCode,
		secondGroup.ServiceIndicators,
		secondGroup.OriginatingPointCodeList,
		firstExtension,
		secondExtension,
	)
	if !bytes.Equal(parameter.Data, wantValue) {
		t.Fatalf("NewRoutingKey() value = %x, want %x", parameter.Data, wantValue)
	}

	wire, err := parameter.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	parsed, err := Parse(wire)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}
	decoded, err := parsed.RoutingKey()
	if err != nil {
		t.Fatalf("RoutingKey() error = %v", err)
	}
	if len(decoded.Groups) != 2 {
		t.Fatalf("decoded group count = %d, want 2", len(decoded.Groups))
	}
	if got := decoded.Groups[0].DestinationPointCode.DestinationPointCode(); got != 0x1111 {
		t.Errorf("first group DPC = %#x, want %#x", got, 0x1111)
	}
	if got := decoded.Groups[1].DestinationPointCode.DestinationPointCode(); got != 0x2222 {
		t.Errorf("second group DPC = %#x, want %#x", got, 0x2222)
	}
	if got := decoded.Groups[1].ServiceIndicators.ServiceIndicators(); !bytes.Equal(got, []byte{ServiceIndISUP, ServiceIndTUP}) {
		t.Errorf("second group Service Indicators = %v, want [%d %d]", got, ServiceIndISUP, ServiceIndTUP)
	}
	if got := decoded.Groups[1].OriginatingPointCodeList.OriginatingPointCodeList(); len(got) != 2 || got[0] != 0x02002222 || got[1] != 0x03002222 {
		t.Errorf("second group OPC List = %v", got)
	}
	if decoded.DestinationPointCode != decoded.Groups[0].DestinationPointCode || decoded.ServiceIndicators != decoded.Groups[0].ServiceIndicators || decoded.OriginatingPointCodeList != decoded.Groups[0].OriginatingPointCodeList {
		t.Error("legacy fields do not alias the first repeated group")
	}
	if len(decoded.Others) != 2 || decoded.Others[0].Tag != 0x7ffe || decoded.Others[1].Tag != 0x7ffe {
		t.Fatalf("decoded extensions = %+v, want both repeated unknown parameters", decoded.Others)
	}

	remarshaled, err := NewRoutingKey(decoded).MarshalBinary()
	if err != nil {
		t.Fatalf("remarshal error = %v", err)
	}
	if !bytes.Equal(remarshaled, wire) {
		t.Fatalf("round-trip wire = %x, want %x", remarshaled, wire)
	}
}

func TestRoutingKeyConstructorRejectsEveryGroupWithoutDPC(t *testing.T) {
	tests := []struct {
		name   string
		groups []RoutingKeyGroup
	}{
		{name: "no groups"},
		{
			name: "later group without DPC",
			groups: []RoutingKeyGroup{
				NewRoutingKeyGroup(NewDestinationPointCode(1), nil, nil),
				NewRoutingKeyGroup(nil, nil, nil),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			payload := NewRoutingKeyPayloadWithGroups(
				NewLocalRoutingKeyIdentifier(1),
				nil,
				nil,
				nil,
				test.groups...,
			)
			if _, err := NewRoutingKey(payload).MarshalBinary(); !errors.Is(err, ErrInvalidValue) {
				t.Fatalf("MarshalBinary() error = %v, want ErrInvalidValue", err)
			}
		})
	}
}

func TestRoutingKeyLegacyConstructorWireCompatibility(t *testing.T) {
	payload := NewRoutingKeyPayload(
		NewLocalRoutingKeyIdentifier(1),
		NewRoutingContext(2),
		NewTrafficModeType(TrafficModeBroadcast),
		NewDestinationPointCode(3),
		NewNetworkAppearance(4),
		NewServiceIndicators(ServiceIndSCCP),
		NewOriginatingPointCodeList(5),
	)
	want := joinNestedParams(t,
		payload.LocalRoutingKeyIdentifier,
		payload.RoutingContext,
		payload.TrafficModeType,
		payload.DestinationPointCode,
		payload.NetworkAppearance,
		payload.ServiceIndicators,
		payload.OriginatingPointCodeList,
	)
	if got := NewRoutingKey(payload).Data; !bytes.Equal(got, want) {
		t.Fatalf("legacy constructor value = %x, want %x", got, want)
	}
}

func TestRoutingKeyAcceptsSingletonsInAnyOrder(t *testing.T) {
	value := joinNestedParams(t,
		NewNetworkAppearance(4),
		NewDestinationPointCode(3),
		NewLocalRoutingKeyIdentifier(1),
		NewServiceIndicators(ServiceIndSCCP),
		NewTrafficModeType(TrafficModeLoadshare),
		NewOriginatingPointCodeList(5),
		NewRoutingContext(2),
	)

	decoded, err := ParseRoutingKeyPayload(value)
	if err != nil {
		t.Fatalf("ParseRoutingKeyPayload() error = %v", err)
	}
	if decoded.LocalRoutingKeyIdentifier.LocalRoutingKeyIdentifier() != 1 || decoded.RoutingContext.RoutingContext() != 2 || decoded.TrafficModeType.TrafficModeType() != TrafficModeLoadshare || decoded.NetworkAppearance.NetworkAppearance() != 4 {
		t.Fatalf("decoded singleton values = %+v", decoded)
	}
}

func TestRoutingKeyServiceIndicatorRules(t *testing.T) {
	for _, serviceIndicators := range [][]uint8{{0}, {ServiceIndSCCP, 0}} {
		value := joinNestedParams(t,
			NewLocalRoutingKeyIdentifier(1),
			NewDestinationPointCode(2),
			NewServiceIndicators(serviceIndicators...),
		)
		if _, err := ParseRoutingKeyPayload(value); !errors.Is(err, ErrInvalidValue) {
			t.Errorf("ParseRoutingKeyPayload(Service Indicators %v) error = %v, want ErrInvalidValue", serviceIndicators, err)
		}
	}

	value := joinNestedParams(t,
		NewLocalRoutingKeyIdentifier(1),
		NewDestinationPointCode(2),
		NewServiceIndicators(0xff),
	)
	if _, err := ParseRoutingKeyPayload(value); err != nil {
		t.Fatalf("ParseRoutingKeyPayload(non-zero extension SI) error = %v", err)
	}
}

func TestNestedRoutingContextRequiresExactlyOneValue(t *testing.T) {
	tests := []struct {
		name  string
		tag   uint16
		value []byte
	}{
		{
			name: "Routing Key",
			tag:  RoutingKey,
			value: joinNestedParams(t,
				NewLocalRoutingKeyIdentifier(1),
				NewRoutingContext(1, 2),
				NewDestinationPointCode(3),
			),
		},
		{
			name: "Registration Result",
			tag:  RegistrationResult,
			value: joinNestedParams(t,
				NewLocalRoutingKeyIdentifier(1),
				NewRegistrationStatus(SuccessfullyRegistered),
				NewRoutingContext(1, 2),
			),
		},
		{
			name: "Deregistration Result",
			tag:  DeregistrationResult,
			value: joinNestedParams(t,
				NewRoutingContext(1, 2),
				NewDeregistrationStatus(SuccessfullyDeregistered),
			),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := Parse(rawValueParam(test.tag, test.value)); !errors.Is(err, ErrInvalidLength) {
				t.Fatalf("Parse() error = %v, want ErrInvalidLength", err)
			}
		})
	}
}

func TestRoutingKeyReceiverResetOnFailureAndReuse(t *testing.T) {
	receiver := &RoutingKeyPayload{
		LocalRoutingKeyIdentifier: NewLocalRoutingKeyIdentifier(99),
		RoutingContext:            NewRoutingContext(99),
		TrafficModeType:           NewTrafficModeType(TrafficModeBroadcast),
		DestinationPointCode:      NewDestinationPointCode(99),
		NetworkAppearance:         NewNetworkAppearance(99),
		ServiceIndicators:         NewServiceIndicators(ServiceIndISUP),
		OriginatingPointCodeList:  NewOriginatingPointCodeList(99),
		Groups: []RoutingKeyGroup{{
			DestinationPointCode: NewDestinationPointCode(99),
		}},
		Others: []*Param{NewParam(0x7ffe, []byte{1})},
	}
	if err := receiver.UnmarshalBinary([]byte{0x00}); err == nil {
		t.Fatal("UnmarshalBinary() error = nil, want malformed nested parameter error")
	}
	assertRoutingKeyPayloadZero(t, receiver)

	first := joinNestedParams(t,
		NewLocalRoutingKeyIdentifier(1),
		NewRoutingContext(1),
		NewTrafficModeType(TrafficModeLoadshare),
		NewDestinationPointCode(1),
		NewNetworkAppearance(1),
		NewServiceIndicators(ServiceIndSCCP),
		NewOriginatingPointCodeList(1),
	)
	if err := receiver.UnmarshalBinary(first); err != nil {
		t.Fatalf("first valid UnmarshalBinary() error = %v", err)
	}
	minimal := joinNestedParams(t,
		NewLocalRoutingKeyIdentifier(2),
		NewDestinationPointCode(2),
	)
	if err := receiver.UnmarshalBinary(minimal); err != nil {
		t.Fatalf("minimal UnmarshalBinary() error = %v", err)
	}
	if receiver.RoutingContext != nil || receiver.TrafficModeType != nil || receiver.NetworkAppearance != nil || receiver.ServiceIndicators != nil || receiver.OriginatingPointCodeList != nil {
		t.Fatalf("minimal decode retained absent optional fields: %+v", receiver)
	}
}

func TestRegistrationResultStructureAndCrossFieldRules(t *testing.T) {
	localIdentifier := NewLocalRoutingKeyIdentifier(1)

	valid := []struct {
		name   string
		status uint32
		ctx    uint32
	}{
		{name: "success carries assigned context", status: SuccessfullyRegistered, ctx: 7},
		{name: "ordinary failure carries zero context", status: PermissionDenied, ctx: 0},
		{name: "already registered carries actual context", status: RoutingKeyAlreadyRegistered, ctx: 7},
		{name: "already registered may carry context zero", status: RoutingKeyAlreadyRegistered, ctx: 0},
	}
	for _, test := range valid {
		t.Run("valid/"+test.name, func(t *testing.T) {
			value := joinNestedParams(t,
				localIdentifier,
				NewRegistrationStatus(test.status),
				NewRoutingContext(test.ctx),
			)
			if _, err := ParseRegistrationResultPayload(value); err != nil {
				t.Fatalf("ParseRegistrationResultPayload() error = %v", err)
			}
		})
	}

	for status := RegistrationStatusUnknown; status < RoutingKeyAlreadyRegistered; status++ {
		t.Run(fmt.Sprintf("failure-%d-with-nonzero-context", status), func(t *testing.T) {
			value := joinNestedParams(t,
				localIdentifier,
				NewRegistrationStatus(status),
				NewRoutingContext(7),
			)
			if _, err := ParseRegistrationResultPayload(value); !errors.Is(err, ErrInvalidValue) {
				t.Fatalf("ParseRegistrationResultPayload() error = %v, want ErrInvalidValue", err)
			}
		})
	}

	invalidStructures := []struct {
		name   string
		params []*Param
	}{
		{
			name:   "missing Local RK Identifier",
			params: []*Param{NewRegistrationStatus(SuccessfullyRegistered), NewRoutingContext(1)},
		},
		{
			name:   "missing Registration Status",
			params: []*Param{localIdentifier, NewRoutingContext(1)},
		},
		{
			name:   "missing Routing Context",
			params: []*Param{localIdentifier, NewRegistrationStatus(SuccessfullyRegistered)},
		},
		{
			name: "duplicate Local RK Identifier",
			params: []*Param{
				localIdentifier,
				NewLocalRoutingKeyIdentifier(2),
				NewRegistrationStatus(SuccessfullyRegistered),
				NewRoutingContext(1),
			},
		},
		{
			name: "duplicate Registration Status",
			params: []*Param{
				localIdentifier,
				NewRegistrationStatus(SuccessfullyRegistered),
				NewRegistrationStatus(SuccessfullyRegistered),
				NewRoutingContext(1),
			},
		},
		{
			name: "duplicate Routing Context",
			params: []*Param{
				localIdentifier,
				NewRegistrationStatus(SuccessfullyRegistered),
				NewRoutingContext(1),
				NewRoutingContext(2),
			},
		},
	}
	for _, test := range invalidStructures {
		t.Run("invalid/"+test.name, func(t *testing.T) {
			if _, err := ParseRegistrationResultPayload(joinNestedParams(t, test.params...)); !errors.Is(err, ErrInvalidValue) {
				t.Fatalf("ParseRegistrationResultPayload() error = %v, want ErrInvalidValue", err)
			}
		})
	}
}

func TestRegistrationResultAcceptsEveryParameterOrder(t *testing.T) {
	localIdentifier := NewLocalRoutingKeyIdentifier(1)
	status := NewRegistrationStatus(SuccessfullyRegistered)
	routingContext := NewRoutingContext(7)
	orders := [][]*Param{
		{localIdentifier, status, routingContext},
		{localIdentifier, routingContext, status},
		{status, localIdentifier, routingContext},
		{status, routingContext, localIdentifier},
		{routingContext, localIdentifier, status},
		{routingContext, status, localIdentifier},
	}

	for index, order := range orders {
		t.Run(fmt.Sprintf("order-%d", index), func(t *testing.T) {
			decoded, err := ParseRegistrationResultPayload(joinNestedParams(t, order...))
			if err != nil {
				t.Fatalf("ParseRegistrationResultPayload() error = %v", err)
			}
			if decoded.LocalRoutingKeyIdentifier.LocalRoutingKeyIdentifier() != 1 || decoded.RegistrationStatus.RegistrationStatus() != SuccessfullyRegistered || decoded.RoutingContext.RoutingContext() != 7 {
				t.Fatalf("decoded fields = %+v", decoded)
			}
		})
	}
}

func TestNestedResultUnknownParametersArePreserved(t *testing.T) {
	firstExtension := NewParam(0x7ffe, []byte{0xaa})
	secondExtension := NewParam(0x7ffe, []byte{0xbb})

	registrationValue := joinNestedParams(t,
		firstExtension,
		NewRoutingContext(7),
		NewLocalRoutingKeyIdentifier(1),
		secondExtension,
		NewRegistrationStatus(SuccessfullyRegistered),
	)
	registration, err := ParseRegistrationResultPayload(registrationValue)
	if err != nil {
		t.Fatalf("ParseRegistrationResultPayload() error = %v", err)
	}
	if len(registration.Others) != 2 || !bytes.Equal(registration.Others[0].Data, []byte{0xaa}) || !bytes.Equal(registration.Others[1].Data, []byte{0xbb}) {
		t.Fatalf("registration extensions = %+v", registration.Others)
	}
	registrationWire, err := NewRegistrationResult(registration).MarshalBinary()
	if err != nil {
		t.Fatalf("registration remarshal error = %v", err)
	}
	registrationRoundTrip, err := Parse(registrationWire)
	if err != nil {
		t.Fatalf("registration reparse error = %v", err)
	}
	if decoded, err := registrationRoundTrip.RegistrationResult(); err != nil {
		t.Fatalf("RegistrationResult() error = %v", err)
	} else if len(decoded.Others) != 2 {
		t.Fatalf("registration round-trip extensions = %d, want 2", len(decoded.Others))
	}

	deregistrationValue := joinNestedParams(t,
		firstExtension,
		NewDeregistrationStatus(SuccessfullyDeregistered),
		secondExtension,
		NewRoutingContext(7),
	)
	deregistration, err := ParseDeregResultPayload(deregistrationValue)
	if err != nil {
		t.Fatalf("ParseDeregResultPayload() error = %v", err)
	}
	if len(deregistration.Others) != 2 || !bytes.Equal(deregistration.Others[0].Data, []byte{0xaa}) || !bytes.Equal(deregistration.Others[1].Data, []byte{0xbb}) {
		t.Fatalf("deregistration extensions = %+v", deregistration.Others)
	}
	deregistrationWire, err := NewDeregistrationResult(deregistration).MarshalBinary()
	if err != nil {
		t.Fatalf("deregistration remarshal error = %v", err)
	}
	deregistrationRoundTrip, err := Parse(deregistrationWire)
	if err != nil {
		t.Fatalf("deregistration reparse error = %v", err)
	}
	if decoded, err := deregistrationRoundTrip.DeregistrationResult(); err != nil {
		t.Fatalf("DeregistrationResult() error = %v", err)
	} else if len(decoded.Others) != 2 {
		t.Fatalf("deregistration round-trip extensions = %d, want 2", len(decoded.Others))
	}
}

func TestRegistrationResultReceiverReset(t *testing.T) {
	receiver := &RegistrationResultPayload{
		LocalRoutingKeyIdentifier: NewLocalRoutingKeyIdentifier(99),
		RegistrationStatus:        NewRegistrationStatus(SuccessfullyRegistered),
		RoutingContext:            NewRoutingContext(99),
		Others:                    []*Param{NewParam(0x7ffe, []byte{1})},
	}
	if err := receiver.UnmarshalBinary([]byte{0x00}); err == nil {
		t.Fatal("UnmarshalBinary() error = nil, want malformed nested parameter error")
	}
	if receiver.LocalRoutingKeyIdentifier != nil || receiver.RegistrationStatus != nil || receiver.RoutingContext != nil || receiver.Others != nil {
		t.Fatalf("failed decode retained receiver state: %+v", receiver)
	}
}

func TestDeregistrationResultStructure(t *testing.T) {
	validOrders := [][]*Param{
		{NewRoutingContext(1), NewDeregistrationStatus(SuccessfullyDeregistered)},
		{NewDeregistrationStatus(SuccessfullyDeregistered), NewRoutingContext(1)},
	}
	for index, params := range validOrders {
		t.Run(fmt.Sprintf("valid-order-%d", index), func(t *testing.T) {
			if _, err := ParseDeregResultPayload(joinNestedParams(t, params...)); err != nil {
				t.Fatalf("ParseDeregResultPayload() error = %v", err)
			}
		})
	}

	invalid := []struct {
		name   string
		params []*Param
	}{
		{name: "missing Routing Context", params: []*Param{NewDeregistrationStatus(SuccessfullyDeregistered)}},
		{name: "missing Deregistration Status", params: []*Param{NewRoutingContext(1)}},
		{
			name: "duplicate Routing Context",
			params: []*Param{
				NewRoutingContext(1),
				NewRoutingContext(2),
				NewDeregistrationStatus(SuccessfullyDeregistered),
			},
		},
		{
			name: "duplicate Deregistration Status",
			params: []*Param{
				NewRoutingContext(1),
				NewDeregistrationStatus(SuccessfullyDeregistered),
				NewDeregistrationStatus(DeregStatusUnknown),
			},
		},
	}
	for _, test := range invalid {
		t.Run("invalid/"+test.name, func(t *testing.T) {
			if _, err := ParseDeregResultPayload(joinNestedParams(t, test.params...)); !errors.Is(err, ErrInvalidValue) {
				t.Fatalf("ParseDeregResultPayload() error = %v, want ErrInvalidValue", err)
			}
		})
	}
}

func TestDeregistrationResultReceiverReset(t *testing.T) {
	receiver := &DeregResultPayload{
		RoutingContext:       NewRoutingContext(99),
		DeregistrationStatus: NewDeregistrationStatus(SuccessfullyDeregistered),
		Others:               []*Param{NewParam(0x7ffe, []byte{1})},
	}
	if err := receiver.UnmarshalBinary([]byte{0x00}); err == nil {
		t.Fatal("UnmarshalBinary() error = nil, want malformed nested parameter error")
	}
	if receiver.RoutingContext != nil || receiver.DeregistrationStatus != nil || receiver.Others != nil {
		t.Fatalf("failed decode retained receiver state: %+v", receiver)
	}
}

func buildNestedWithoutPanic(t *testing.T, build func() *Param) (param *Param) {
	t.Helper()
	defer func() {
		if recovered := recover(); recovered != nil {
			t.Errorf("constructor panicked: %v", recovered)
		}
	}()
	return build()
}

func joinNestedParams(t testing.TB, params ...*Param) []byte {
	t.Helper()
	value, err := MarshalMultiParams(params)
	if err != nil {
		t.Fatalf("MarshalMultiParams() error = %v", err)
	}
	return value
}

func assertRoutingKeyPayloadZero(t *testing.T, payload *RoutingKeyPayload) {
	t.Helper()
	if payload.LocalRoutingKeyIdentifier != nil || payload.RoutingContext != nil || payload.TrafficModeType != nil || payload.DestinationPointCode != nil || payload.NetworkAppearance != nil || payload.ServiceIndicators != nil || payload.OriginatingPointCodeList != nil || payload.Groups != nil || payload.Others != nil {
		t.Fatalf("failed decode retained receiver state: %+v", payload)
	}
}

func FuzzNestedRKMValidation(f *testing.F) {
	f.Add(uint8(0), joinNestedParams(f,
		NewLocalRoutingKeyIdentifier(1),
		NewDestinationPointCode(2),
	))
	f.Add(uint8(0), joinNestedParams(f,
		NewLocalRoutingKeyIdentifier(1),
		NewDestinationPointCode(2),
		NewServiceIndicators(ServiceIndSCCP),
		NewDestinationPointCode(3),
		NewServiceIndicators(ServiceIndISUP),
	))
	f.Add(uint8(1), joinNestedParams(f,
		NewLocalRoutingKeyIdentifier(1),
		NewRegistrationStatus(SuccessfullyRegistered),
		NewRoutingContext(2),
	))
	f.Add(uint8(1), joinNestedParams(f,
		NewLocalRoutingKeyIdentifier(1),
		NewRegistrationStatus(PermissionDenied),
		NewRoutingContext(0),
	))
	f.Add(uint8(2), joinNestedParams(f,
		NewRoutingContext(2),
		NewDeregistrationStatus(SuccessfullyDeregistered),
	))
	f.Add(uint8(0), []byte(nil))
	f.Add(uint8(1), []byte{0x00})
	f.Add(uint8(2), rawValueParam(RoutingContext, uint32Bytes(1)))

	f.Fuzz(func(t *testing.T, kind uint8, value []byte) {
		if len(value) > 4096 {
			t.Skip()
		}

		var tag uint16
		switch kind % 3 {
		case 0:
			tag = RoutingKey
		case 1:
			tag = RegistrationResult
		default:
			tag = DeregistrationResult
		}

		parameter, err := Parse(rawValueParam(tag, value))
		if err != nil {
			return
		}

		switch tag {
		case RoutingKey:
			decoded, err := parameter.RoutingKey()
			if err != nil {
				t.Fatalf("accepted Routing Key failed typed decode: %v", err)
			}
			if decoded.LocalRoutingKeyIdentifier == nil || len(decoded.Groups) == 0 {
				t.Fatalf("accepted Routing Key lacks mandatory fields: %+v", decoded)
			}
			for _, group := range decoded.Groups {
				if group.DestinationPointCode == nil {
					t.Fatalf("accepted Routing Key contains group without DPC: %+v", decoded)
				}
				if group.ServiceIndicators != nil && bytesContain(group.ServiceIndicators.Data, 0) {
					t.Fatalf("accepted Routing Key contains SI 0: %+v", decoded)
				}
			}
		case RegistrationResult:
			decoded, err := parameter.RegistrationResult()
			if err != nil {
				t.Fatalf("accepted Registration Result failed typed decode: %v", err)
			}
			if decoded.LocalRoutingKeyIdentifier == nil || decoded.RegistrationStatus == nil || decoded.RoutingContext == nil || len(decoded.RoutingContext.Data) != 4 {
				t.Fatalf("accepted Registration Result violates structure: %+v", decoded)
			}
			status := decoded.RegistrationStatus.RegistrationStatus()
			if status != SuccessfullyRegistered && status != RoutingKeyAlreadyRegistered && decoded.RoutingContext.RoutingContext() != 0 {
				t.Fatalf("accepted failure has non-zero Routing Context: %+v", decoded)
			}
		case DeregistrationResult:
			decoded, err := parameter.DeregistrationResult()
			if err != nil {
				t.Fatalf("accepted Deregistration Result failed typed decode: %v", err)
			}
			if decoded.RoutingContext == nil || decoded.DeregistrationStatus == nil || len(decoded.RoutingContext.Data) != 4 {
				t.Fatalf("accepted Deregistration Result violates structure: %+v", decoded)
			}
		}

		wire, err := parameter.MarshalBinary()
		if err != nil {
			t.Fatalf("accepted parameter failed remarshal: %v", err)
		}
		if _, err := Parse(wire); err != nil {
			t.Fatalf("remarshaled parameter failed reparse: %v", err)
		}
	})
}

func TestMaximumNestedContainerDepthDoesNotPanic(t *testing.T) {
	value := []byte(nil)
	for len(value)+4 <= maxParamValueLength {
		value = rawValueParam(RoutingKey, value)
	}
	if _, err := Parse(rawValueParam(RoutingKey, value)); !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("Parse() error = %v, want ErrInvalidValue", err)
	}
}

func TestNestedContainerDepthIsBounded(t *testing.T) {
	value := joinNestedParams(t,
		NewLocalRoutingKeyIdentifier(1),
		NewDestinationPointCode(1),
	)
	for depth := 0; depth <= maxNestedContainerDepth+1; depth++ {
		layer := joinNestedParams(t,
			NewLocalRoutingKeyIdentifier(uint32(depth+2)),
			NewDestinationPointCode(uint32(depth+2)),
		)
		layer = append(layer, rawValueParam(RoutingKey, value)...)
		value = layer
	}
	if _, err := Parse(rawValueParam(RoutingKey, value)); err == nil {
		t.Fatal("Parse() error = nil, want nested depth rejection")
	} else if !errors.Is(err, ErrInvalidValue) {
		t.Fatalf("Parse() error = %v, want ErrInvalidValue", err)
	} else if !strings.Contains(err.Error(), "nested container depth exceeds") {
		t.Fatalf("Parse() error = %v, want nested depth rejection", err)
	}
}
