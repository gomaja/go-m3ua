package messages

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/gomaja/go-m3ua/messages/params"
)

func TestDataMarshalRejectsWrongNamedParameterTags(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Data)
	}{
		{
			name: "Network Appearance",
			mutate: func(data *Data) {
				data.NetworkAppearance = params.NewRoutingContext(1)
			},
		},
		{
			name: "Routing Context",
			mutate: func(data *Data) {
				data.RoutingContext = params.NewNetworkAppearance(1)
			},
		},
		{
			name: "Protocol Data",
			mutate: func(data *Data) {
				data.ProtocolData = params.NewInfoString("not Protocol Data")
			},
		},
		{
			name: "Correlation Id",
			mutate: func(data *Data) {
				data.CorrelationID = params.NewNetworkAppearance(1)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data := validDataForValidation()
			test.mutate(data)
			data.Header.Payload = []byte{0xde, 0xad, 0xbe, 0xef}
			wantPayload := bytes.Clone(data.Header.Payload)

			if _, err := data.MarshalBinary(); !errors.Is(err, params.ErrInvalidType) {
				t.Fatalf("MarshalBinary() error = %v, want params.ErrInvalidType", err)
			}
			if !bytes.Equal(data.Header.Payload, wantPayload) {
				t.Fatal("MarshalBinary() modified Header.Payload before rejecting the wrong parameter tag")
			}

			buffer := bytes.Repeat([]byte{0xa5}, data.MarshalLen())
			wantBuffer := bytes.Clone(buffer)
			if err := data.MarshalTo(buffer); !errors.Is(err, params.ErrInvalidType) {
				t.Fatalf("MarshalTo() error = %v, want params.ErrInvalidType", err)
			}
			if !bytes.Equal(buffer, wantBuffer) {
				t.Fatal("MarshalTo() modified its destination before rejecting the wrong parameter tag")
			}
		})
	}
}

func TestDataMarshalRejectsMalformedKnownParameterLengths(t *testing.T) {
	tests := []struct {
		name   string
		sizes  []int
		mutate func(*Data, int)
	}{
		{
			name:  "Network Appearance",
			sizes: []int{0, 1, 3, 5, 8},
			mutate: func(data *Data, size int) {
				data.NetworkAppearance = &params.Param{Tag: params.NetworkAppearance, Data: make([]byte, size)}
			},
		},
		{
			name:  "Routing Context",
			sizes: []int{0, 1, 3, 5, 8},
			mutate: func(data *Data, size int) {
				data.RoutingContext = &params.Param{Tag: params.RoutingContext, Data: make([]byte, size)}
			},
		},
		{
			name:  "Protocol Data",
			sizes: []int{0, 1, 3, 4, 8, 11},
			mutate: func(data *Data, size int) {
				data.ProtocolData = &params.Param{Tag: params.ProtocolData, Data: make([]byte, size)}
			},
		},
		{
			name:  "Correlation Id",
			sizes: []int{0, 1, 3, 5, 8},
			mutate: func(data *Data, size int) {
				data.CorrelationID = &params.Param{Tag: params.CorrelationID, Data: make([]byte, size)}
			},
		},
	}

	for _, test := range tests {
		for _, size := range test.sizes {
			t.Run(fmt.Sprintf("%s/%d", test.name, size), func(t *testing.T) {
				data := validDataForValidation()
				test.mutate(data, size)
				data.Header.Payload = []byte{0xde, 0xad, 0xbe, 0xef}
				wantPayload := bytes.Clone(data.Header.Payload)

				if _, err := data.MarshalBinary(); !errors.Is(err, params.ErrInvalidLength) {
					t.Fatalf("MarshalBinary() error = %v, want params.ErrInvalidLength", err)
				}
				if !bytes.Equal(data.Header.Payload, wantPayload) {
					t.Fatal("MarshalBinary() modified Header.Payload before rejecting the wrong parameter length")
				}

				buffer := bytes.Repeat([]byte{0xa5}, data.MarshalLen())
				wantBuffer := bytes.Clone(buffer)
				if err := data.MarshalTo(buffer); !errors.Is(err, params.ErrInvalidLength) {
					t.Fatalf("MarshalTo() error = %v, want params.ErrInvalidLength", err)
				}
				if !bytes.Equal(buffer, wantBuffer) {
					t.Fatal("MarshalTo() modified its destination before rejecting the wrong parameter length")
				}
			})
		}
	}
}

func TestDataUnmarshalRejectsMalformedKnownParameterLengths(t *testing.T) {
	validProtocolData := rawDataParameter(params.ProtocolData, make([]byte, 12))
	tests := []struct {
		name  string
		tag   uint16
		sizes []int
	}{
		{name: "Network Appearance", tag: params.NetworkAppearance, sizes: []int{0, 1, 3, 5, 8}},
		{name: "Routing Context", tag: params.RoutingContext, sizes: []int{0, 1, 3, 5, 8}},
		{name: "Protocol Data", tag: params.ProtocolData, sizes: []int{0, 1, 3, 4, 8, 11}},
		{name: "Correlation Id", tag: params.CorrelationID, sizes: []int{0, 1, 3, 5, 8}},
	}

	for _, test := range tests {
		for _, size := range test.sizes {
			t.Run(fmt.Sprintf("%s/%d", test.name, size), func(t *testing.T) {
				payload := rawDataParameter(test.tag, make([]byte, size))
				if test.tag != params.ProtocolData {
					payload = append(payload, validProtocolData...)
				}

				_, err := ParseData(rawDataMessage(t, payload))
				if !errors.Is(err, params.ErrInvalidLength) {
					t.Fatalf("ParseData() error = %v, want params.ErrInvalidLength", err)
				}
				if errors.Is(err, ErrInvalidParameter) || errors.Is(err, ErrMissingParameter) {
					t.Fatalf("ParseData() classified a wrong length as %v", err)
				}
			})
		}
	}
}

func TestDataProtocolDataLengthBoundaries(t *testing.T) {
	for _, size := range []int{12, 13} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			data := NewData(nil, nil, &params.Param{
				Tag:  params.ProtocolData,
				Data: make([]byte, size),
			}, nil)

			wire, err := data.MarshalBinary()
			if err != nil {
				t.Fatalf("MarshalBinary() error = %v", err)
			}
			decoded, err := ParseData(wire)
			if err != nil {
				t.Fatalf("ParseData() error = %v", err)
			}
			if got := len(decoded.ProtocolData.Data); got != size {
				t.Fatalf("decoded Protocol Data length = %d, want %d", got, size)
			}
		})
	}
}

func TestDataUnmarshalRejectsDuplicateParameters(t *testing.T) {
	validProtocolData := rawDataParameter(params.ProtocolData, make([]byte, 12))
	tests := []struct {
		name       string
		parameters [][]byte
	}{
		{
			name: "Network Appearance",
			parameters: [][]byte{
				rawDataParameter(params.NetworkAppearance, make([]byte, 4)),
				rawDataParameter(params.NetworkAppearance, make([]byte, 4)),
				validProtocolData,
			},
		},
		{
			name: "Routing Context",
			parameters: [][]byte{
				rawDataParameter(params.RoutingContext, make([]byte, 4)),
				rawDataParameter(params.RoutingContext, make([]byte, 4)),
				validProtocolData,
			},
		},
		{
			name: "Protocol Data",
			parameters: [][]byte{
				validProtocolData,
				validProtocolData,
			},
		},
		{
			name: "Correlation Id",
			parameters: [][]byte{
				validProtocolData,
				rawDataParameter(params.CorrelationID, make([]byte, 4)),
				rawDataParameter(params.CorrelationID, make([]byte, 4)),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			payload := bytes.Join(test.parameters, nil)
			if _, err := ParseData(rawDataMessage(t, payload)); !errors.Is(err, ErrInvalidParameter) {
				t.Fatalf("ParseData() error = %v, want ErrInvalidParameter", err)
			}
		})
	}
}

func TestDataMarshalRejectsKnownParametersInOthers(t *testing.T) {
	tests := []*params.Param{
		params.NewNetworkAppearance(1),
		params.NewRoutingContext(1),
		params.NewProtocolData(1, 2, params.ServiceIndSCCP, 0, 0, 1, nil),
		params.NewCorrelationID(1),
	}

	for _, parameter := range tests {
		t.Run(fmt.Sprintf("tag-%#04x", parameter.Tag), func(t *testing.T) {
			data := validDataForValidation()
			data.Others = []*params.Param{parameter}

			if _, err := data.MarshalBinary(); !errors.Is(err, ErrInvalidParameter) {
				t.Fatalf("MarshalBinary() error = %v, want ErrInvalidParameter", err)
			}
		})
	}
}

func TestDataNilAndUnknownParameters(t *testing.T) {
	unknown := params.NewParam(0xeffe, []byte{0xde, 0xad, 0xbe, 0xef})
	data := NewData(nil, nil, validProtocolDataForValidation(), nil)
	data.Others = []*params.Param{nil, unknown, nil}

	wire, err := data.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	decoded, err := ParseData(wire)
	if err != nil {
		t.Fatalf("ParseData() error = %v", err)
	}
	if decoded.NetworkAppearance != nil || decoded.RoutingContext != nil || decoded.CorrelationID != nil {
		t.Fatalf("nil optional parameters decoded as present: %#v", decoded)
	}
	if len(decoded.Others) != 1 || decoded.Others[0].Tag != unknown.Tag || !bytes.Equal(decoded.Others[0].Data, unknown.Data) {
		t.Fatalf("decoded unknown parameters = %#v, want only %#v", decoded.Others, unknown)
	}

	roundTrip, err := decoded.MarshalBinary()
	if err != nil {
		t.Fatalf("round-trip MarshalBinary() error = %v", err)
	}
	if !bytes.Equal(roundTrip, wire) {
		t.Fatalf("unknown parameter changed on round trip:\n got % x\nwant % x", roundTrip, wire)
	}
}

func TestDataNonProtocolDataTagDoesNotSatisfyProtocolData(t *testing.T) {
	tests := []struct {
		name      string
		parameter []byte
	}{
		{name: "unknown", parameter: rawDataParameter(params.InfoString, []byte("not Protocol Data"))},
		{name: "Network Appearance", parameter: rawDataParameter(params.NetworkAppearance, make([]byte, 4))},
		{name: "Routing Context", parameter: rawDataParameter(params.RoutingContext, make([]byte, 4))},
		{name: "Correlation Id", parameter: rawDataParameter(params.CorrelationID, make([]byte, 4))},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ParseData(rawDataMessage(t, test.parameter)); !errors.Is(err, ErrMissingParameter) {
				t.Fatalf("ParseData() error = %v, want ErrMissingParameter", err)
			}
		})
	}
}

func FuzzDataNamedParameterValidation(f *testing.F) {
	type parameterRule struct {
		tag         uint16
		validLength func(int) bool
	}
	rules := [...]parameterRule{
		{tag: params.NetworkAppearance, validLength: func(length int) bool { return length == 4 }},
		{tag: params.RoutingContext, validLength: func(length int) bool { return length == 4 }},
		{tag: params.ProtocolData, validLength: func(length int) bool { return length >= 12 }},
		{tag: params.CorrelationID, validLength: func(length int) bool { return length == 4 }},
	}
	seedLengths := [][]int{
		{0, 3, 4, 5, 8},
		{0, 3, 4, 5, 8},
		{0, 4, 8, 11, 12, 13},
		{0, 3, 4, 5, 8},
	}

	for slot, rule := range rules {
		for _, length := range seedLengths[slot] {
			f.Add(uint8(slot), rule.tag, make([]byte, length))
		}
		f.Add(uint8(slot), params.InfoString, bytes.Repeat([]byte{'x'}, 12))
	}

	f.Fuzz(func(t *testing.T, rawSlot uint8, tag uint16, value []byte) {
		if len(value) > 128 {
			return
		}

		slot := int(rawSlot % uint8(len(rules)))
		rule := rules[slot]
		parameter := &params.Param{Tag: tag, Data: bytes.Clone(value)}
		data := validDataForValidation()
		setDataParameter(data, slot, parameter)

		wire, err := data.MarshalBinary()
		switch {
		case tag != rule.tag:
			if !errors.Is(err, params.ErrInvalidType) {
				t.Fatalf("slot %d tag %#04x length %d: MarshalBinary() error = %v, want params.ErrInvalidType", slot, tag, len(value), err)
			}
		case !rule.validLength(len(value)):
			if !errors.Is(err, params.ErrInvalidLength) {
				t.Fatalf("slot %d tag %#04x length %d: MarshalBinary() error = %v, want params.ErrInvalidLength", slot, tag, len(value), err)
			}
		default:
			if err != nil {
				t.Fatalf("slot %d tag %#04x length %d: MarshalBinary() error = %v", slot, tag, len(value), err)
			}
			decoded, parseErr := ParseData(wire)
			if parseErr != nil {
				t.Fatalf("slot %d tag %#04x length %d: ParseData(marshaled) error = %v", slot, tag, len(value), parseErr)
			}
			if got := dataParameter(decoded, slot); got == nil || !bytes.Equal(got.Data, value) {
				t.Fatalf("slot %d tag %#04x length %d: decoded parameter = %#v", slot, tag, len(value), got)
			}
		}

		knownParameter := rawDataParameter(rule.tag, value)
		if slot != 2 {
			knownParameter = append(knownParameter, rawDataParameter(params.ProtocolData, make([]byte, 12))...)
		}
		decoded, parseErr := ParseData(rawDataMessage(t, knownParameter))
		if !rule.validLength(len(value)) {
			if !errors.Is(parseErr, params.ErrInvalidLength) {
				t.Fatalf("slot %d known tag %#04x length %d: ParseData() error = %v, want params.ErrInvalidLength", slot, rule.tag, len(value), parseErr)
			}
			return
		}
		if parseErr != nil {
			t.Fatalf("slot %d known tag %#04x length %d: ParseData() error = %v", slot, rule.tag, len(value), parseErr)
		}
		if got := dataParameter(decoded, slot); got == nil || !bytes.Equal(got.Data, value) {
			t.Fatalf("slot %d known tag %#04x length %d: decoded parameter = %#v", slot, rule.tag, len(value), got)
		}
	})
}

func validDataForValidation() *Data {
	return NewData(
		params.NewNetworkAppearance(1),
		params.NewRoutingContext(2),
		validProtocolDataForValidation(),
		params.NewCorrelationID(3),
	)
}

func validProtocolDataForValidation() *params.Param {
	return params.NewProtocolData(1, 2, params.ServiceIndSCCP, 0, 0, 1, []byte("payload"))
}

func setDataParameter(data *Data, slot int, parameter *params.Param) {
	switch slot {
	case 0:
		data.NetworkAppearance = parameter
	case 1:
		data.RoutingContext = parameter
	case 2:
		data.ProtocolData = parameter
	case 3:
		data.CorrelationID = parameter
	}
}

func dataParameter(data *Data, slot int) *params.Param {
	switch slot {
	case 0:
		return data.NetworkAppearance
	case 1:
		return data.RoutingContext
	case 2:
		return data.ProtocolData
	case 3:
		return data.CorrelationID
	default:
		return nil
	}
}

func rawDataParameter(tag uint16, value []byte) []byte {
	parameterLength := 4 + len(value)
	padding := (4 - parameterLength%4) % 4
	wire := make([]byte, parameterLength+padding)
	binary.BigEndian.PutUint16(wire[0:2], tag)
	binary.BigEndian.PutUint16(wire[2:4], uint16(parameterLength))
	copy(wire[4:], value)
	return wire
}

func rawDataMessage(t *testing.T, payload []byte) []byte {
	t.Helper()

	wire, err := NewHeader(1, MsgClassTransfer, MsgTypePayloadData, payload).MarshalBinary()
	if err != nil {
		t.Fatalf("Header.MarshalBinary() error = %v", err)
	}
	return wire
}
