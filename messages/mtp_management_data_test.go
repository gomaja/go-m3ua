// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package messages

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/gomaja/go-m3ua/messages/params"
)

func TestMTPManagementDataCodecRoundTrip(t *testing.T) {
	// Captured synthetic SI 0 DATA, including Routing Context and padding.
	raw, err := hex.DecodeString("0100010100000024000600080000000102100013000000c8000000640000000114000000")
	if err != nil {
		t.Fatal(err)
	}
	data, err := ParseData(raw)
	if err != nil {
		t.Fatal(err)
	}
	generic, err := ParseGeneric(raw)
	if err != nil {
		t.Fatal(err)
	}
	header, err := ParseHeader(raw)
	if err != nil {
		t.Fatal(err)
	}
	for name, encoder := range map[string]interface {
		MarshalBinary() ([]byte, error)
		MarshalTo([]byte) error
		MarshalLen() int
	}{"Data": data, "Generic": generic, "Header": header} {
		t.Run(name, func(t *testing.T) {
			got, err := encoder.MarshalBinary()
			if err != nil {
				t.Fatalf("decode then encode: %v", err)
			}
			if !bytes.Equal(got, raw) {
				t.Fatalf("round trip=%x want %x", got, raw)
			}
			buf := make([]byte, encoder.MarshalLen())
			if err := encoder.MarshalTo(buf); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(buf, raw) {
				t.Fatalf("MarshalTo=%x want %x", buf, raw)
			}
		})
	}
	for _, m := range []M3UA{data, generic, encodedData{data, raw}} {
		got, err := MarshalBinary(m)
		if err != nil || !bytes.Equal(got, raw) {
			t.Fatalf("wrapper round trip=%x err=%v", got, err)
		}
	}
}

type encodedData struct {
	*Data
	raw []byte
}

func (d encodedData) MarshalTo(b []byte) error { copy(b, d.raw); return nil }
func (d encodedData) MarshalLen() int          { return len(d.raw) }

func FuzzDataEncoderServiceIndicator(f *testing.F) {
	for _, si := range []uint8{0, 1, 2, 3, 255} {
		f.Add(si, []byte{1})
	}
	f.Fuzz(func(t *testing.T, si uint8, extension []byte) {
		if len(extension) > 128 {
			return
		}
		prefix := params.NewParam(0xf001, extension)
		pd := params.NewProtocolData(200, 100, si, 0, 0, 1, []byte{1, 2, 3})
		payload, err := params.MarshalMultiParams([]*params.Param{prefix, pd})
		if err != nil {
			t.Fatal(err)
		}
		h := NewHeader(1, MsgClassTransfer, MsgTypePayloadData, payload)
		raw, err := h.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := ParseData(raw)
		if err != nil {
			t.Fatal(err)
		}
		if decoded.ProtocolData.Data[8] != si {
			t.Fatal("SI changed")
		}
		got, err := decoded.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		again, err := ParseData(got)
		if err != nil {
			t.Fatal(err)
		}
		if again.ProtocolData.Data[8] != si || len(again.Others) != 1 || !bytes.Equal(again.Others[0].Data, extension) {
			t.Fatal("round trip changed SI or extension")
		}

	})
}
