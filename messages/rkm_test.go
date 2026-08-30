package messages

import (
	"errors"
	"testing"

	"github.com/gomaja/go-m3ua/messages/params"
)

func TestRKMMessageRoundTrip(t *testing.T) {
	tests := []struct {
		name    string
		message M3UA
		assert  func(*testing.T, M3UA)
	}{
		{
			name: "Registration Request",
			message: NewRegistrationRequest(
				validRoutingKeyParam(1, 101),
				validRoutingKeyParam(2, 102),
			),
			assert: func(t *testing.T, message M3UA) {
				request := message.(*RegistrationRequest)
				if len(request.RoutingKeys) != 2 {
					t.Fatalf("Routing Keys = %d, want 2", len(request.RoutingKeys))
				}
			},
		},
		{
			name: "Registration Response",
			message: NewRegistrationResponse(
				validRegistrationResultParam(1, 101),
				validRegistrationResultParam(2, 102),
			),
			assert: func(t *testing.T, message M3UA) {
				response := message.(*RegistrationResponse)
				if len(response.RegistrationResults) != 2 {
					t.Fatalf("Registration Results = %d, want 2", len(response.RegistrationResults))
				}
			},
		},
		{
			name:    "Deregistration Request",
			message: NewDeregistrationRequest(params.NewRoutingContext(101, 102)),
			assert: func(t *testing.T, message M3UA) {
				request := message.(*DeregistrationRequest)
				if got := request.RoutingContext.RoutingContexts(); len(got) != 2 || got[0] != 101 || got[1] != 102 {
					t.Fatalf("Routing Contexts = %v, want [101 102]", got)
				}
			},
		},
		{
			name: "Deregistration Response",
			message: NewDeregistrationResponse(
				validDeregistrationResultParam(101),
				validDeregistrationResultParam(102),
			),
			assert: func(t *testing.T, message M3UA) {
				response := message.(*DeregistrationResponse)
				if len(response.DeregistrationResults) != 2 {
					t.Fatalf("Deregistration Results = %d, want 2", len(response.DeregistrationResults))
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			wire, err := test.message.MarshalBinary()
			if err != nil {
				t.Fatalf("MarshalBinary: %v", err)
			}
			parsed, err := Parse(wire)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if parsed.MessageClass() != MsgClassRKM || parsed.MessageType() != test.message.MessageType() {
				t.Fatalf("parsed class/type = %d/%d, want %d/%d", parsed.MessageClass(), parsed.MessageType(), MsgClassRKM, test.message.MessageType())
			}
			test.assert(t, parsed)
		})
	}
}

func TestRKMMessageCardinality(t *testing.T) {
	tests := []struct {
		name    string
		message M3UA
		want    error
	}{
		{name: "Registration Request missing Routing Key", message: NewRegistrationRequest(), want: ErrMissingParameter},
		{name: "Registration Response missing result", message: NewRegistrationResponse(), want: ErrMissingParameter},
		{name: "Deregistration Request missing Routing Context", message: NewDeregistrationRequest(nil), want: ErrMissingParameter},
		{name: "Deregistration Response missing result", message: NewDeregistrationResponse(), want: ErrMissingParameter},
		{name: "Registration Request wrong parameter", message: NewRegistrationRequest(params.NewRoutingContext(1)), want: ErrInvalidParameter},
		{name: "Registration Response wrong parameter", message: NewRegistrationResponse(validDeregistrationResultParam(1)), want: ErrInvalidParameter},
		{name: "Deregistration Request wrong parameter", message: NewDeregistrationRequest(params.NewInfoString("extension")), want: ErrInvalidParameter},
		{name: "Deregistration Response wrong parameter", message: NewDeregistrationResponse(validRegistrationResultParam(1, 1)), want: ErrInvalidParameter},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := test.message.MarshalBinary()
			if !errors.Is(err, test.want) {
				t.Fatalf("MarshalBinary error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestRKMMessageWireParsingRejectsMissingAndWrongKnownParameters(t *testing.T) {
	tests := []struct {
		name        string
		messageType uint8
		parameters  []*params.Param
		want        error
	}{
		{name: "Registration Request missing Routing Key", messageType: MsgTypeRegistrationRequest, want: ErrMissingParameter},
		{
			name:        "Registration Request has unrelated known parameter",
			messageType: MsgTypeRegistrationRequest,
			parameters:  []*params.Param{validRoutingKeyParam(1, 101), params.NewRoutingContext(101)},
			want:        ErrInvalidParameter,
		},
		{name: "Registration Response missing result", messageType: MsgTypeRegistrationResponse, want: ErrMissingParameter},
		{
			name:        "Registration Response has unrelated known parameter",
			messageType: MsgTypeRegistrationResponse,
			parameters:  []*params.Param{validRegistrationResultParam(1, 101), validRoutingKeyParam(2, 102)},
			want:        ErrInvalidParameter,
		},
		{name: "Deregistration Request missing Routing Context", messageType: MsgTypeDeregistrationRequest, want: ErrMissingParameter},
		{
			name:        "Deregistration Request has unrelated known parameter",
			messageType: MsgTypeDeregistrationRequest,
			parameters:  []*params.Param{params.NewRoutingContext(101), params.NewInfoString("not defined for DEREG REQ")},
			want:        ErrInvalidParameter,
		},
		{name: "Deregistration Response missing result", messageType: MsgTypeDeregistrationResponse, want: ErrMissingParameter},
		{
			name:        "Deregistration Response has unrelated known parameter",
			messageType: MsgTypeDeregistrationResponse,
			parameters:  []*params.Param{validDeregistrationResultParam(101), params.NewRoutingContext(101)},
			want:        ErrInvalidParameter,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			wire, err := New(1, MsgClassRKM, test.messageType, test.parameters...).MarshalBinary()
			if err != nil {
				t.Fatalf("marshal wire fixture: %v", err)
			}
			if _, err := Parse(wire); !errors.Is(err, test.want) {
				t.Fatalf("Parse error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestRKMMessageParsingPreservesExtensionsAndRejectsDuplicateRoutingContext(t *testing.T) {
	extension := &params.Param{Tag: 0xfffe, Data: []byte{1, 2, 3}}
	extension.SetLength()

	request := NewRegistrationRequest(validRoutingKeyParam(1, 101))
	request.Others = []*params.Param{extension}
	wire, err := request.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}
	parsed, err := ParseRegistrationRequest(wire)
	if err != nil {
		t.Fatalf("ParseRegistrationRequest: %v", err)
	}
	if len(parsed.Others) != 1 || parsed.Others[0].Tag != extension.Tag {
		t.Fatalf("Others = %+v, want extension tag %#x", parsed.Others, extension.Tag)
	}

	duplicate := New(1, MsgClassRKM, MsgTypeDeregistrationRequest,
		params.NewRoutingContext(101),
		params.NewRoutingContext(102),
	)
	wire, err = duplicate.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal duplicate Routing Context: %v", err)
	}
	if _, err := ParseDeregistrationRequest(wire); !errors.Is(err, ErrInvalidParameter) {
		t.Fatalf("ParseDeregistrationRequest error = %v, want ErrInvalidParameter", err)
	}
}

func TestRKMMessageTypedHeaderValidation(t *testing.T) {
	wire, err := NewRegistrationRequest(validRoutingKeyParam(1, 101)).MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}
	wire[3] = MsgTypeRegistrationResponse
	if _, err := ParseRegistrationRequest(wire); !errors.Is(err, ErrUnexpectedMessageType) {
		t.Fatalf("ParseRegistrationRequest error = %v, want ErrUnexpectedMessageType", err)
	}
}

func FuzzRKMMessageValidation(f *testing.F) {
	seeds := []M3UA{
		NewRegistrationRequest(validRoutingKeyParam(1, 101)),
		NewRegistrationResponse(validRegistrationResultParam(1, 101)),
		NewDeregistrationRequest(params.NewRoutingContext(101)),
		NewDeregistrationResponse(validDeregistrationResultParam(101)),
	}
	for _, seed := range seeds {
		wire, err := seed.MarshalBinary()
		if err != nil {
			f.Fatalf("seed MarshalBinary: %v", err)
		}
		f.Add(wire)
	}

	f.Fuzz(func(t *testing.T, wire []byte) {
		message, err := Parse(wire)
		if err != nil || message.MessageClass() != MsgClassRKM {
			return
		}
		roundTrip, err := message.MarshalBinary()
		if err != nil {
			t.Fatalf("accepted RKM message failed to marshal: %v", err)
		}
		if _, err := Parse(roundTrip); err != nil {
			t.Fatalf("accepted RKM round trip failed to parse: %v", err)
		}
	})
}

func validRoutingKeyParam(identifier, routingContext uint32) *params.Param {
	return params.NewRoutingKey(params.NewRoutingKeyPayload(
		params.NewLocalRoutingKeyIdentifier(identifier),
		params.NewRoutingContext(routingContext),
		params.NewTrafficModeType(params.TrafficModeLoadshare),
		params.NewDestinationPointCode(1),
		params.NewNetworkAppearance(10),
		params.NewServiceIndicators(3),
		params.NewOriginatingPointCodeList(2),
	))
}

func validRegistrationResultParam(identifier, routingContext uint32) *params.Param {
	return params.NewRegistrationResult(params.NewRegistrationResultPayload(
		params.NewLocalRoutingKeyIdentifier(identifier),
		params.NewRegistrationStatus(params.SuccessfullyRegistered),
		params.NewRoutingContext(routingContext),
	))
}

func validDeregistrationResultParam(routingContext uint32) *params.Param {
	return params.NewDeregistrationResult(params.NewDeregResultPayload(
		params.NewRoutingContext(routingContext),
		params.NewDeregistrationStatus(params.SuccessfullyDeregistered),
	))
}
