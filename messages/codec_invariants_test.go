package messages

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/google/go-cmp/cmp"
)

func TestMandatoryParametersRequiredOnMarshalAndDecode(t *testing.T) {
	affectedPointCode := func() *params.Param { return params.NewAffectedPointCode(0x1234) }
	userCause := func() *params.Param { return params.NewUserCause(params.SCCP, params.Unequipped) }

	tests := []struct {
		name        string
		message     M3UA
		wireMessage *Generic
	}{
		{
			name:        "DATA Protocol Data",
			message:     NewData(nil, nil, nil, nil),
			wireMessage: New(1, MsgClassTransfer, MsgTypePayloadData),
		},
		{
			name:        "DUNA Affected Point Code",
			message:     NewDestinationUnavailable(nil, nil, nil, nil),
			wireMessage: New(1, MsgClassSSNM, MsgTypeDestinationUnavailable),
		},
		{
			name:        "DAVA Affected Point Code",
			message:     NewDestinationAvailable(nil, nil, nil, nil),
			wireMessage: New(1, MsgClassSSNM, MsgTypeDestinationAvailable),
		},
		{
			name:        "DAUD Affected Point Code",
			message:     NewDestinationStateAudit(nil, nil, nil, nil),
			wireMessage: New(1, MsgClassSSNM, MsgTypeDestinationStateAudit),
		},
		{
			name:        "SCON Affected Point Code",
			message:     NewSignallingCongestion(nil, nil, nil, nil, nil, nil),
			wireMessage: New(1, MsgClassSSNM, MsgTypeSignallingCongestion),
		},
		{
			name:    "DUPU Affected Point Code",
			message: NewDestinationUserPartUnavailable(nil, nil, nil, userCause(), nil),
			wireMessage: New(
				1,
				MsgClassSSNM,
				MsgTypeDestinationUserPartUnavailable,
				userCause(),
			),
		},
		{
			name:    "DUPU User Cause",
			message: NewDestinationUserPartUnavailable(nil, nil, affectedPointCode(), nil, nil),
			wireMessage: New(
				1,
				MsgClassSSNM,
				MsgTypeDestinationUserPartUnavailable,
				affectedPointCode(),
			),
		},
		{
			name:        "DRST Affected Point Code",
			message:     NewDestinationRestricted(nil, nil, nil, nil),
			wireMessage: New(1, MsgClassSSNM, MsgTypeDestinationRestricted),
		},
		{
			name:        "ERR Error Code",
			message:     NewError(nil, nil, nil, nil, nil),
			wireMessage: New(1, MsgClassManagement, MsgTypeError),
		},
		{
			name:        "NTFY Status",
			message:     NewNotify(nil, nil, nil, nil),
			wireMessage: New(1, MsgClassManagement, MsgTypeNotify),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := test.message.MarshalBinary(); !errors.Is(err, ErrMissingParameter) {
				t.Errorf("MarshalBinary() error = %v, want ErrMissingParameter", err)
			}
			if err := test.message.MarshalTo(make([]byte, test.message.MarshalLen())); !errors.Is(err, ErrMissingParameter) {
				t.Errorf("MarshalTo() error = %v, want ErrMissingParameter", err)
			}

			wire, err := test.wireMessage.MarshalBinary()
			if err != nil {
				t.Fatalf("building malformed peer message: %v", err)
			}
			if _, err := Parse(wire); !errors.Is(err, ErrMissingParameter) {
				t.Errorf("Parse() error = %v, want ErrMissingParameter", err)
			}
		})
	}
}

func TestUnknownParametersRoundTripInEveryTypedMessage(t *testing.T) {
	for _, fixture := range validTypedMessageFixtures() {
		t.Run(fixture.name, func(t *testing.T) {
			wire := appendUnknownParameters(t, fixture.message)
			message, err := Parse(wire)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}

			others := reflect.ValueOf(message).Elem().FieldByName("Others")
			if !others.IsValid() {
				t.Fatal("typed message has no Others field")
			}
			if got, want := others.Len(), 2; got != want {
				t.Fatalf("len(Others) = %d, want %d", got, want)
			}
			for index, tag := range []uint16{0xeffe, 0xeffd} {
				param := others.Index(index).Interface().(*params.Param)
				if param.Tag != tag {
					t.Errorf("Others[%d].Tag = %#04x, want %#04x", index, param.Tag, tag)
				}
				param.Length = 0
			}

			lengthSetter, ok := message.(interface{ SetLength() })
			if !ok {
				t.Fatal("typed message has no SetLength method")
			}
			lengthSetter.SetLength()
			for index := 0; index < others.Len(); index++ {
				param := others.Index(index).Interface().(*params.Param)
				if got, want := int(param.Length), 4+len(param.Data); got != want {
					t.Errorf("Others[%d].Length after SetLength = %d, want %d", index, got, want)
				}
			}

			remarshaled, err := message.MarshalBinary()
			if err != nil {
				t.Fatalf("MarshalBinary() error = %v", err)
			}
			if !bytes.Equal(remarshaled, wire) {
				t.Errorf("parse/remarshal changed wire:\n got % x\nwant % x", remarshaled, wire)
			}
			if got, want := message.MarshalLen(), len(wire); got != want {
				t.Errorf("MarshalLen() = %d, want %d", got, want)
			}
		})
	}
}

func TestNilUnknownParametersAreIgnoredInEveryTypedMessage(t *testing.T) {
	for _, fixture := range validTypedMessageFixtures() {
		t.Run(fixture.name, func(t *testing.T) {
			want, err := fixture.message.MarshalBinary()
			if err != nil {
				t.Fatalf("MarshalBinary() base message error = %v", err)
			}

			others := reflect.ValueOf(fixture.message).Elem().FieldByName("Others")
			if !others.IsValid() {
				t.Fatal("typed message has no Others field")
			}
			others.Set(reflect.Append(others, reflect.Zero(others.Type().Elem())))

			lengthSetter := fixture.message.(interface{ SetLength() })
			lengthSetter.SetLength()
			if got := fixture.message.MarshalLen(); got != len(want) {
				t.Errorf("MarshalLen() = %d, want %d", got, len(want))
			}
			got, err := fixture.message.MarshalBinary()
			if err != nil {
				t.Fatalf("MarshalBinary() with nil Others entry error = %v", err)
			}
			if !bytes.Equal(got, want) {
				t.Errorf("nil Others entry changed wire:\n got % x\nwant % x", got, want)
			}
		})
	}
}

func TestMarshalOtherParamsValidatesOffset(t *testing.T) {
	unknown := params.NewParam(0xeffe, []byte{0xde, 0xad, 0xbe, 0xef})
	unknownWire, err := unknown.MarshalBinary()
	if err != nil {
		t.Fatalf("Param.MarshalBinary() error = %v", err)
	}

	t.Run("exact trailing region", func(t *testing.T) {
		const prefixLength = 3
		payload := bytes.Repeat([]byte{0xaa}, prefixLength+len(unknownWire))
		if err := marshalOtherParams(payload, prefixLength, []*params.Param{nil, unknown}); err != nil {
			t.Fatalf("marshalOtherParams() error = %v", err)
		}
		want := append(bytes.Repeat([]byte{0xaa}, prefixLength), unknownWire...)
		if !bytes.Equal(payload, want) {
			t.Errorf("marshalOtherParams() payload = % x, want % x", payload, want)
		}
	})

	tests := []struct {
		name    string
		payload []byte
		offset  int
		wantErr error
	}{
		{
			name:    "negative offset",
			payload: make([]byte, len(unknownWire)),
			offset:  -1,
			wantErr: ErrTooShortToMarshalBinary,
		},
		{
			name:    "offset beyond payload",
			payload: make([]byte, len(unknownWire)),
			offset:  len(unknownWire) + 1,
			wantErr: ErrTooShortToMarshalBinary,
		},
		{
			name:    "extension exceeds payload",
			payload: make([]byte, len(unknownWire)-1),
			wantErr: ErrTooShortToMarshalBinary,
		},
		{
			name:    "unfilled payload gap",
			payload: make([]byte, len(unknownWire)+1),
			wantErr: ErrInvalidMessageLength,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := marshalOtherParams(test.payload, test.offset, []*params.Param{unknown})
			if !errors.Is(err, test.wantErr) {
				t.Errorf("marshalOtherParams() error = %v, want %v", err, test.wantErr)
			}
		})
	}
}

func TestTypedUnmarshalClearsReceiverState(t *testing.T) {
	for _, fixture := range validTypedMessageFixtures() {
		t.Run(fixture.name, func(t *testing.T) {
			wire := appendUnknownParameters(t, fixture.message)
			receiver := newMessageFor(wire[2], wire[3])
			if receiver == nil {
				t.Fatal("newMessageFor() returned nil for supported message")
			}
			seedMessageReceiver(receiver)

			if err := receiver.UnmarshalBinary(wire); err != nil {
				t.Fatalf("reused UnmarshalBinary() error = %v", err)
			}
			fresh, err := Parse(wire)
			if err != nil {
				t.Fatalf("fresh Parse() error = %v", err)
			}
			if diff := cmp.Diff(fresh, receiver); diff != "" {
				t.Errorf("reused receiver differs from fresh decode (-want +got):\n%s", diff)
			}

			seedMessageReceiver(receiver)
			if err := receiver.UnmarshalBinary([]byte{0x01}); err == nil {
				t.Fatal("UnmarshalBinary(truncated message) returned nil error")
			}
			zero := reflect.New(reflect.TypeOf(receiver).Elem()).Interface()
			if diff := cmp.Diff(zero, receiver); diff != "" {
				t.Errorf("failed decode retained receiver state (-want +got):\n%s", diff)
			}
		})
	}
}

func TestGenericUnmarshalClearsReceiverState(t *testing.T) {
	wire, err := New(
		1,
		0xfe,
		0xfd,
		params.NewParam(0xeffe, []byte{0xde, 0xad, 0xbe, 0xef}),
	).MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}

	receiver := &Generic{}
	seedMessageReceiver(receiver)
	if err := receiver.UnmarshalBinary(wire); err != nil {
		t.Fatalf("reused UnmarshalBinary() error = %v", err)
	}
	fresh, err := ParseGeneric(wire)
	if err != nil {
		t.Fatalf("fresh ParseGeneric() error = %v", err)
	}
	if diff := cmp.Diff(fresh, receiver); diff != "" {
		t.Errorf("reused receiver differs from fresh decode (-want +got):\n%s", diff)
	}

	seedMessageReceiver(receiver)
	if err := receiver.UnmarshalBinary([]byte{0x01}); err == nil {
		t.Fatal("UnmarshalBinary(truncated message) returned nil error")
	}
	if diff := cmp.Diff(&Generic{}, receiver); diff != "" {
		t.Errorf("failed decode retained receiver state (-want +got):\n%s", diff)
	}
}

func TestEveryMessageStringIsNilSafe(t *testing.T) {
	type stringer interface {
		String() string
	}

	tests := []struct {
		name      string
		nilValue  stringer
		zeroValue stringer
	}{
		{name: "Header", nilValue: (*Header)(nil), zeroValue: &Header{}},
		{name: "Generic", nilValue: (*Generic)(nil), zeroValue: &Generic{}},
		{name: "DATA", nilValue: (*Data)(nil), zeroValue: &Data{}},
		{name: "DUNA", nilValue: (*DestinationUnavailable)(nil), zeroValue: &DestinationUnavailable{}},
		{name: "DAVA", nilValue: (*DestinationAvailable)(nil), zeroValue: &DestinationAvailable{}},
		{name: "DAUD", nilValue: (*DestinationStateAudit)(nil), zeroValue: &DestinationStateAudit{}},
		{name: "SCON", nilValue: (*SignallingCongestion)(nil), zeroValue: &SignallingCongestion{}},
		{name: "DUPU", nilValue: (*DestinationUserPartUnavailable)(nil), zeroValue: &DestinationUserPartUnavailable{}},
		{name: "DRST", nilValue: (*DestinationRestricted)(nil), zeroValue: &DestinationRestricted{}},
		{name: "ERR", nilValue: (*Error)(nil), zeroValue: &Error{}},
		{name: "NTFY", nilValue: (*Notify)(nil), zeroValue: &Notify{}},
		{name: "ASPUP", nilValue: (*AspUp)(nil), zeroValue: &AspUp{}},
		{name: "ASPUP Ack", nilValue: (*AspUpAck)(nil), zeroValue: &AspUpAck{}},
		{name: "ASPDN", nilValue: (*AspDown)(nil), zeroValue: &AspDown{}},
		{name: "ASPDN Ack", nilValue: (*AspDownAck)(nil), zeroValue: &AspDownAck{}},
		{name: "BEAT", nilValue: (*Heartbeat)(nil), zeroValue: &Heartbeat{}},
		{name: "BEAT Ack", nilValue: (*HeartbeatAck)(nil), zeroValue: &HeartbeatAck{}},
		{name: "ASPAC", nilValue: (*AspActive)(nil), zeroValue: &AspActive{}},
		{name: "ASPAC Ack", nilValue: (*AspActiveAck)(nil), zeroValue: &AspActiveAck{}},
		{name: "ASPIA", nilValue: (*AspInactive)(nil), zeroValue: &AspInactive{}},
		{name: "ASPIA Ack", nilValue: (*AspInactiveAck)(nil), zeroValue: &AspInactiveAck{}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Run("nil receiver", func(t *testing.T) {
				if got := test.nilValue.String(); got != "" {
					t.Errorf("String() = %q, want empty string", got)
				}
			})
			t.Run("zero value", func(t *testing.T) {
				if got := test.zeroValue.String(); got == "" {
					t.Error("String() returned an empty representation")
				}
			})
		})
	}
}

func TestTypedUnmarshalRejectsWrongClassAndType(t *testing.T) {
	for _, fixture := range validTypedMessageFixtures() {
		t.Run(fixture.name, func(t *testing.T) {
			wire, err := fixture.message.MarshalBinary()
			if err != nil {
				t.Fatalf("MarshalBinary() error = %v", err)
			}

			tests := []struct {
				name  string
				octet int
			}{
				{name: "wrong class", octet: 2},
				{name: "wrong type", octet: 3},
			}
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					malformed := append([]byte(nil), wire...)
					malformed[test.octet] ^= 0xff

					receiver := newMessageFor(fixture.message.MessageClass(), fixture.message.MessageType())
					if receiver == nil {
						t.Fatal("newMessageFor() returned nil for supported message")
					}
					seedMessageReceiver(receiver)
					if err := receiver.UnmarshalBinary(malformed); !errors.Is(err, ErrUnexpectedMessageType) {
						t.Fatalf("UnmarshalBinary() error = %v, want ErrUnexpectedMessageType", err)
					}

					zero := reflect.New(reflect.TypeOf(receiver).Elem()).Interface()
					if diff := cmp.Diff(zero, receiver); diff != "" {
						t.Errorf("rejected message retained receiver state (-want +got):\n%s", diff)
					}
				})
			}
		})
	}
}

type typedMessageFixture struct {
	name    string
	message M3UA
}

func validTypedMessageFixtures() []typedMessageFixture {
	networkAppearance := func() *params.Param { return params.NewNetworkAppearance(1) }
	routingContext := func() *params.Param { return params.NewRoutingContext(7) }
	affectedPointCode := func() *params.Param { return params.NewAffectedPointCode(0x1234) }
	infoString := func() *params.Param { return params.NewInfoString("info") }
	trafficMode := func() *params.Param { return params.NewTrafficModeType(params.TrafficModeLoadshare) }
	heartbeatData := func() *params.Param { return params.NewHeartbeatData([]byte("beat")) }

	return []typedMessageFixture{
		{
			name: "DATA",
			message: NewData(
				networkAppearance(),
				routingContext(),
				params.NewProtocolData(1, 2, params.ServiceIndSCCP, 0, 0, 1, []byte("data")),
				params.NewCorrelationID(9),
			),
		},
		{
			name:    "DUNA",
			message: NewDestinationUnavailable(networkAppearance(), routingContext(), affectedPointCode(), infoString()),
		},
		{
			name:    "DAVA",
			message: NewDestinationAvailable(networkAppearance(), routingContext(), affectedPointCode(), infoString()),
		},
		{
			name:    "DAUD",
			message: NewDestinationStateAudit(networkAppearance(), routingContext(), affectedPointCode(), infoString()),
		},
		{
			name: "SCON",
			message: NewSignallingCongestion(
				networkAppearance(),
				routingContext(),
				affectedPointCode(),
				params.NewConcernedDestination(0x4321),
				params.NewCongestionIndications(1),
				infoString(),
			),
		},
		{
			name: "DUPU",
			message: NewDestinationUserPartUnavailable(
				networkAppearance(),
				routingContext(),
				affectedPointCode(),
				params.NewUserCause(params.SCCP, params.Unequipped),
				infoString(),
			),
		},
		{
			name:    "DRST",
			message: NewDestinationRestricted(networkAppearance(), routingContext(), affectedPointCode(), infoString()),
		},
		{
			name: "ERR",
			message: NewError(
				params.NewErrorCode(params.UnexpectedMessageError),
				routingContext(),
				networkAppearance(),
				affectedPointCode(),
				params.NewDiagnosticInformation([]byte("diag")),
			),
		},
		{
			name: "NTFY",
			message: NewNotify(
				params.NewStatus(params.AsStateActive),
				params.NewAspIdentifier(11),
				routingContext(),
				infoString(),
			),
		},
		{name: "ASPUP", message: NewAspUp(params.NewAspIdentifier(11), infoString())},
		{name: "ASPUP Ack", message: NewAspUpAck(params.NewAspIdentifier(11), infoString())},
		{name: "ASPDN", message: NewAspDown(infoString())},
		{name: "ASPDN Ack", message: NewAspDownAck(infoString())},
		{name: "BEAT", message: NewHeartbeat(heartbeatData())},
		{name: "BEAT Ack", message: NewHeartbeatAck(heartbeatData())},
		{name: "ASPAC", message: NewAspActive(trafficMode(), routingContext(), infoString())},
		{name: "ASPAC Ack", message: NewAspActiveAck(trafficMode(), routingContext(), infoString())},
		{name: "ASPIA", message: NewAspInactive(routingContext(), infoString())},
		{name: "ASPIA Ack", message: NewAspInactiveAck(routingContext(), infoString())},
		{name: "REG REQ", message: NewRegistrationRequest(validRoutingKeyParam(1, 7))},
		{name: "REG RSP", message: NewRegistrationResponse(validRegistrationResultParam(1, 7))},
		{name: "DEREG REQ", message: NewDeregistrationRequest(routingContext())},
		{name: "DEREG RSP", message: NewDeregistrationResponse(validDeregistrationResultParam(7))},
	}
}

func appendUnknownParameters(t *testing.T, message M3UA) []byte {
	t.Helper()

	wire, err := message.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() base message error = %v", err)
	}
	unknowns, err := params.MarshalMultiParams([]*params.Param{
		params.NewParam(0xeffe, []byte{0xde, 0xad, 0xbe, 0xef}),
		params.NewParam(0xeffd, []byte{0xaa}),
	})
	if err != nil {
		t.Fatalf("MarshalMultiParams() error = %v", err)
	}

	header := NewHeader(wire[0], wire[2], wire[3], append(append([]byte(nil), wire[8:]...), unknowns...))
	header.Reserved = wire[1]
	withUnknowns, err := header.MarshalBinary()
	if err != nil {
		t.Fatalf("Header.MarshalBinary() error = %v", err)
	}
	return withUnknowns
}

func seedMessageReceiver(message M3UA) {
	paramType := reflect.TypeOf((*params.Param)(nil))
	headerType := reflect.TypeOf((*Header)(nil))
	paramSliceType := reflect.TypeOf([]*params.Param(nil))

	value := reflect.ValueOf(message).Elem()
	for index := 0; index < value.NumField(); index++ {
		field := value.Field(index)
		switch field.Type() {
		case headerType:
			field.Set(reflect.ValueOf(NewHeader(9, 9, 9, []byte{0xaa})))
		case paramType:
			field.Set(reflect.ValueOf(params.NewParam(0xeffc, []byte{0xaa})))
		case paramSliceType:
			field.Set(reflect.ValueOf([]*params.Param{
				params.NewParam(0xeffb, []byte{0xbb}),
			}))
		}
	}
}
