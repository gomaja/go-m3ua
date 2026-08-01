package params

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestInfoStringValueRules(t *testing.T) {
	valid := []struct {
		name  string
		value string
	}{
		{name: "zero octets", value: ""},
		{name: "255 ASCII octets", value: strings.Repeat("a", 255)},
		{name: "255 multibyte octets", value: strings.Repeat("€", 85)},
	}
	for _, test := range valid {
		t.Run("valid/"+test.name, func(t *testing.T) {
			param := NewInfoString(test.value)
			wire, err := param.MarshalBinary()
			if err != nil {
				t.Fatalf("MarshalBinary() error = %v", err)
			}
			decoded, err := Parse(wire)
			if err != nil {
				t.Fatalf("Parse() error = %v", err)
			}
			if got := decoded.InfoString(); got != test.value {
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
	for _, test := range invalid {
		t.Run("invalid/"+test.name, func(t *testing.T) {
			param := NewInfoString(string(test.value))
			if _, err := param.MarshalBinary(); !errors.Is(err, ErrInvalidValue) {
				t.Fatalf("MarshalBinary() error = %v, want ErrInvalidValue", err)
			}
			if err := param.MarshalTo(make([]byte, param.MarshalLen())); !errors.Is(err, ErrInvalidValue) {
				t.Fatalf("MarshalTo() error = %v, want ErrInvalidValue", err)
			}

			wire := rawInfoStringParam(test.value)
			if _, err := Parse(wire); !errors.Is(err, ErrInvalidValue) {
				t.Fatalf("Parse() error = %v, want ErrInvalidValue", err)
			}

			receiver := NewInfoString("stale")
			if err := receiver.UnmarshalBinary(wire); !errors.Is(err, ErrInvalidValue) {
				t.Fatalf("reused UnmarshalBinary() error = %v, want ErrInvalidValue", err)
			}
			if receiver.Tag != 0 || receiver.Length != 0 || receiver.Data != nil {
				t.Errorf("failed decode retained receiver state: %+v", receiver)
			}
		})
	}
}

func FuzzInfoStringValidation(f *testing.F) {
	for _, seed := range [][]byte{
		nil,
		[]byte("info"),
		[]byte(strings.Repeat("€", 85)),
		bytes.Repeat([]byte{'a'}, 256),
		{0xff},
		{0xc0, 0xaf},
		{0xe2, 0x82},
		{0xed, 0xa0, 0x80},
	} {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, value []byte) {
		valid := len(value) <= 255 && utf8.Valid(value)
		param := NewInfoString(string(value))
		wire, marshalErr := param.MarshalBinary()
		if valid {
			if marshalErr != nil {
				t.Fatalf("valid value MarshalBinary() error = %v", marshalErr)
			}
			decoded, err := Parse(wire)
			if err != nil {
				t.Fatalf("valid value Parse() error = %v", err)
			}
			if !bytes.Equal(decoded.Data, value) {
				t.Fatalf("roundtrip Data = % x, want % x", decoded.Data, value)
			}
			return
		}
		if len(value) <= maxParamValueLength && !errors.Is(marshalErr, ErrInvalidValue) {
			t.Fatalf("invalid value MarshalBinary() error = %v, want ErrInvalidValue", marshalErr)
		}
		if len(value) > maxParamValueLength && !errors.Is(marshalErr, ErrInvalidLength) {
			t.Fatalf("oversized value MarshalBinary() error = %v, want ErrInvalidLength", marshalErr)
		}

		if len(value) <= maxParamValueLength {
			if _, err := Parse(rawInfoStringParam(value)); !errors.Is(err, ErrInvalidValue) {
				t.Fatalf("invalid value Parse() error = %v, want ErrInvalidValue", err)
			}
		}
	})
}

func rawInfoStringParam(value []byte) []byte {
	parameterLength := 4 + len(value)
	marshalLength := parameterLength + (4-parameterLength%4)%4
	wire := make([]byte, marshalLength)
	binary.BigEndian.PutUint16(wire[0:2], InfoString)
	binary.BigEndian.PutUint16(wire[2:4], uint16(parameterLength))
	copy(wire[4:], value)
	return wire
}
