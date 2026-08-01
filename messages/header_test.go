// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package messages

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
)

func TestHeader(t *testing.T) {
	cases := []testCase{
		{
			"has-all",
			NewHeader(
				1,  // Version
				16, // Class
				16, // Type
				[]byte{
					0xde, 0xad, 0xbe, 0xef,
				},
			),
			[]byte{
				// Header
				0x01, 0x00, 0x10, 0x10, 0x00, 0x00, 0x00, 0x0c,
				// dummy Payload
				0xde, 0xad, 0xbe, 0xef,
			},
		},
	}

	runTests(t, cases, func(b []byte) (serializeable, error) {
		v, err := ParseHeader(b)
		if err != nil {
			return nil, err
		}
		return v, nil
	})
}

func TestHeaderRejectsInvalidMessageLength(t *testing.T) {
	tests := []struct {
		name string
		wire []byte
	}{
		{
			name: "declared length below common header",
			wire: []byte{0x01, 0x00, 0x03, 0x01, 0x00, 0x00, 0x00, 0x07},
		},
		{
			name: "declared length exceeds received message",
			wire: []byte{0x01, 0x00, 0x03, 0x01, 0x00, 0x00, 0x00, 0x09},
		},
		{
			name: "trailing byte without final parameter",
			wire: []byte{0x01, 0x00, 0x03, 0x01, 0x00, 0x00, 0x00, 0x08, 0xff},
		},
		{
			name: "complete undeclared parameter",
			wire: []byte{
				0x01, 0x00, 0x03, 0x01, 0x00, 0x00, 0x00, 0x08,
				0x00, 0x11, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01,
			},
		},
		{
			name: "only part of omitted final padding present",
			wire: []byte{
				0x01, 0x00, 0x03, 0x01, 0x00, 0x00, 0x00, 0x0d,
				0x00, 0x09, 0x00, 0x05, 0xaa, 0x00,
			},
		},
		{
			name: "declared payload does not end at final parameter value",
			wire: []byte{
				0x01, 0x00, 0x03, 0x01, 0x00, 0x00, 0x00, 0x0d,
				0x00, 0x09, 0x00, 0x04, 0xaa, 0x00, 0x00, 0x00,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ParseHeader(test.wire)
			if !errors.Is(err, ErrInvalidMessageLength) {
				t.Fatalf("ParseHeader() error = %v, want ErrInvalidMessageLength", err)
			}
		})
	}
}

func TestHeaderAcceptsFinalParameterPaddingOmittedFromMessageLength(t *testing.T) {
	tests := []struct {
		name        string
		wire        []byte
		wantPayload []byte
	}{
		{
			name: "padding absent from both length and received bytes",
			wire: []byte{
				0x01, 0x00, 0x03, 0x03, 0x00, 0x00, 0x00, 0x0d,
				0x00, 0x09, 0x00, 0x05, 0xaa,
			},
			wantPayload: []byte{0x00, 0x09, 0x00, 0x05, 0xaa},
		},
		{
			name: "three physical pad octets omitted from length",
			wire: []byte{
				0x01, 0x00, 0x03, 0x03, 0x00, 0x00, 0x00, 0x0d,
				0x00, 0x09, 0x00, 0x05, 0xaa, 0x31, 0x32, 0x33,
			},
			wantPayload: []byte{0x00, 0x09, 0x00, 0x05, 0xaa},
		},
		{
			name: "earlier parameter remains fully padded",
			wire: []byte{
				0x01, 0x00, 0x03, 0x03, 0x00, 0x00, 0x00, 0x15,
				0x00, 0x09, 0x00, 0x05, 0xaa, 0x00, 0x00, 0x00,
				0x00, 0x09, 0x00, 0x05, 0xbb, 0x00, 0x00, 0x00,
			},
			wantPayload: []byte{
				0x00, 0x09, 0x00, 0x05, 0xaa, 0x00, 0x00, 0x00,
				0x00, 0x09, 0x00, 0x05, 0xbb,
			},
		},
		{
			name: "ordinary fully counted padding",
			wire: []byte{
				0x01, 0x00, 0x03, 0x03, 0x00, 0x00, 0x00, 0x10,
				0x00, 0x09, 0x00, 0x05, 0xaa, 0x00, 0x00, 0x00,
			},
			wantPayload: []byte{0x00, 0x09, 0x00, 0x05, 0xaa, 0x00, 0x00, 0x00},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			header, err := ParseHeader(test.wire)
			if err != nil {
				t.Fatalf("ParseHeader() error = %v", err)
			}
			if !bytes.Equal(header.Payload, test.wantPayload) {
				t.Fatalf("Payload = % x, want % x", header.Payload, test.wantPayload)
			}
			if got := int(header.Length); got != 8+len(test.wantPayload) {
				t.Fatalf("Length = %d, want %d", got, 8+len(test.wantPayload))
			}
		})
	}
}

func TestHeaderMarshalUsesCurrentPayloadLength(t *testing.T) {
	header := NewHeader(1, MsgClassASPSM, MsgTypeAspUp, []byte{0xde})
	header.Payload = []byte{0xde, 0xad, 0xbe, 0xef}
	header.Length = 0xffffffff

	wire, err := header.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	if got, want := len(wire), header.MarshalLen(); got != want {
		t.Fatalf("wire length = %d, want %d", got, want)
	}
	if got, want := binary.BigEndian.Uint32(wire[4:8]), uint32(len(wire)); got != want {
		t.Errorf("encoded Message Length = %d, want %d", got, want)
	}
	if got, want := header.Length, uint32(len(wire)); got != want {
		t.Errorf("Header.Length = %d after marshal, want %d", got, want)
	}
}

func TestHeaderMarshalToRejectsEveryShortBuffer(t *testing.T) {
	header := NewHeader(1, MsgClassASPSM, MsgTypeAspUp, []byte{0xde, 0xad, 0xbe, 0xef})

	for size := 0; size < header.MarshalLen(); size++ {
		t.Run(fmt.Sprintf("size-%d", size), func(t *testing.T) {
			buffer := bytes.Repeat([]byte{0xaa}, size)
			if err := header.MarshalTo(buffer); !errors.Is(err, ErrTooShortToMarshalBinary) {
				t.Fatalf("MarshalTo(%d-byte buffer) error = %v, want ErrTooShortToMarshalBinary", size, err)
			}
			if !bytes.Equal(buffer, bytes.Repeat([]byte{0xaa}, size)) {
				t.Fatalf("MarshalTo changed short buffer: % x", buffer)
			}
		})
	}
}
