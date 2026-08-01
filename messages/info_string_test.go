package messages

import (
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/gomaja/go-m3ua/messages/params"
)

func TestInfoStringRoundTripsInEveryMessagePosition(t *testing.T) {
	values := []struct {
		name  string
		value string
	}{
		{name: "zero octets", value: ""},
		{name: "255 ASCII octets", value: strings.Repeat("a", 255)},
		{name: "255 multibyte octets", value: strings.Repeat("€", 85)},
	}

	for _, fixture := range infoStringMessageFixtures() {
		t.Run(fixture.name, func(t *testing.T) {
			for _, test := range values {
				t.Run(test.name, func(t *testing.T) {
					infoField(fixture.message).Set(reflect.ValueOf(params.NewInfoString(test.value)))
					fixture.message.(interface{ SetLength() }).SetLength()
					wire, err := fixture.message.MarshalBinary()
					if err != nil {
						t.Fatalf("MarshalBinary() error = %v", err)
					}
					decoded, err := Parse(wire)
					if err != nil {
						t.Fatalf("Parse() error = %v", err)
					}
					decodedInfo := infoField(decoded).Interface().(*params.Param)
					if got := decodedInfo.InfoString(); got != test.value {
						t.Errorf("InfoString() = %q, want %q", got, test.value)
					}
					remarshaled, err := decoded.MarshalBinary()
					if err != nil {
						t.Fatalf("decoded MarshalBinary() error = %v", err)
					}
					if !bytes.Equal(remarshaled, wire) {
						t.Errorf("parse/remarshal changed wire:\n got % x\nwant % x", remarshaled, wire)
					}
				})
			}
		})
	}
}

func TestInvalidInfoStringRejectedInEveryMessagePosition(t *testing.T) {
	invalid := []struct {
		name  string
		value []byte
	}{
		{name: "256 ASCII octets", value: bytes.Repeat([]byte{'a'}, 256)},
		{name: "256 multibyte octets", value: []byte(strings.Repeat("é", 128))},
		{name: "invalid leading octet", value: []byte{0xff}},
		{name: "overlong encoding", value: []byte{0xc0, 0xaf}},
		{name: "truncated sequence", value: []byte{0xe2, 0x82}},
		{name: "UTF-16 surrogate", value: []byte{0xed, 0xa0, 0x80}},
	}

	for _, fixture := range infoStringMessageFixtures() {
		t.Run(fixture.name, func(t *testing.T) {
			validWire, err := fixture.message.MarshalBinary()
			if err != nil {
				t.Fatalf("base MarshalBinary() error = %v", err)
			}

			for _, test := range invalid {
				t.Run(test.name, func(t *testing.T) {
					infoField(fixture.message).Set(reflect.ValueOf(params.NewInfoString(string(test.value))))
					fixture.message.(interface{ SetLength() }).SetLength()
					if _, err := fixture.message.MarshalBinary(); !errors.Is(err, params.ErrInvalidValue) {
						t.Fatalf("MarshalBinary() error = %v, want params.ErrInvalidValue", err)
					}

					wire := replaceParameterValue(t, validWire, params.InfoString, test.value)
					if _, err := Parse(wire); !errors.Is(err, params.ErrInvalidValue) {
						t.Fatalf("Parse() error = %v, want params.ErrInvalidValue", err)
					}

					receiver := newMessageFor(fixture.message.MessageClass(), fixture.message.MessageType())
					seedMessageReceiver(receiver)
					if err := receiver.UnmarshalBinary(wire); !errors.Is(err, params.ErrInvalidValue) {
						t.Fatalf("reused UnmarshalBinary() error = %v, want params.ErrInvalidValue", err)
					}
					value := reflect.ValueOf(receiver).Elem()
					if value.FieldByName("Header").IsNil() {
						t.Fatal("valid common header was not retained")
					}
					for fieldIndex := 1; fieldIndex < value.NumField(); fieldIndex++ {
						if !value.Field(fieldIndex).IsZero() {
							t.Errorf("field %s retained stale state", value.Type().Field(fieldIndex).Name)
						}
					}
				})
			}
		})
	}
}

func infoStringMessageFixtures() []typedMessageFixture {
	fixtures := validTypedMessageFixtures()
	withInfo := fixtures[:0]
	for _, fixture := range fixtures {
		field := infoField(fixture.message)
		if field.IsValid() && !field.IsNil() {
			withInfo = append(withInfo, fixture)
		}
	}
	return withInfo
}

func infoField(message M3UA) reflect.Value {
	return reflect.ValueOf(message).Elem().FieldByName("InfoString")
}

func replaceParameterValue(t *testing.T, wire []byte, tag uint16, value []byte) []byte {
	t.Helper()

	for offset := 8; offset < len(wire); {
		if len(wire)-offset < 4 {
			t.Fatal("base wire ends in a partial parameter header")
		}
		parameterTag := binary.BigEndian.Uint16(wire[offset : offset+2])
		parameterLength := int(binary.BigEndian.Uint16(wire[offset+2 : offset+4]))
		if parameterLength < 4 || parameterLength > len(wire)-offset {
			t.Fatal("base wire contains an invalid parameter length")
		}
		paddedLength := parameterLength + (4-parameterLength%4)%4
		if parameterTag != tag {
			offset += paddedLength
			continue
		}

		newParameterLength := 4 + len(value)
		newPaddedLength := newParameterLength + (4-newParameterLength%4)%4
		newParameter := make([]byte, newPaddedLength)
		binary.BigEndian.PutUint16(newParameter[0:2], tag)
		binary.BigEndian.PutUint16(newParameter[2:4], uint16(newParameterLength))
		copy(newParameter[4:], value)

		replaced := make([]byte, 0, len(wire)-paddedLength+newPaddedLength)
		replaced = append(replaced, wire[:offset]...)
		replaced = append(replaced, newParameter...)
		replaced = append(replaced, wire[offset+paddedLength:]...)
		binary.BigEndian.PutUint32(replaced[4:8], uint32(len(replaced)))
		return replaced
	}

	t.Fatalf("base wire has no parameter tag %#04x", tag)
	return nil
}
