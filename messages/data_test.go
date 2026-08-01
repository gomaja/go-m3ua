// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package messages

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/gomaja/go-m3ua/messages/params"
)

func TestData(t *testing.T) {
	cases := []testCase{
		{
			"has-all",
			NewData(
				params.NewNetworkAppearance(1),
				params.NewRoutingContext(1),
				params.NewProtocolData(
					1, // OriginatingPointCode
					2, // DestinationPointCode
					3, // ServiceIndicator
					1, // NetworkIndicator
					0, // MessagePriority
					1, // SignalingLinkSelection
					[]byte{ // Data
						0xde, 0xad, 0xbe, 0xef,
					},
				),
				nil,
			),
			[]byte{
				// Header
				0x01, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x2c,
				// NetworkAppearance
				0x02, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01,
				// RoutingContext
				0x00, 0x06, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01,
				// ProtocolData
				// Param Header
				0x02, 0x10, 0x00, 0x14,
				// OPC, DPC
				0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02,
				// SI, NI, MP, SLS
				0x03, 0x01, 0x00, 0x01,
				// Data
				0xde, 0xad, 0xbe, 0xef,
			},
		},
		{
			"has-rc",
			NewData(
				params.NewNetworkAppearance(1),
				nil,
				params.NewProtocolData(
					1, // OriginatingPointCode
					2, // DestinationPointCode
					3, // ServiceIndicator
					1, // NetworkIndicator
					0, // MessagePriority
					1, // SignalingLinkSelection
					[]byte{ // Data
						0xde, 0xad, 0xbe, 0xef,
					},
				),
				nil,
			),
			[]byte{
				// Header
				0x01, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x24,
				// NetworkAppearance
				0x02, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x01,
				// ProtocolData
				// Param Header
				0x02, 0x10, 0x00, 0x14,
				// OPC, DPC
				0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02,
				// SI, NI, MP, SLS
				0x03, 0x01, 0x00, 0x01,
				// Data
				0xde, 0xad, 0xbe, 0xef,
			},
		},
		{
			"has-info",
			NewData(
				nil,
				params.NewRoutingContext(255),
				params.NewProtocolData(
					1, // OriginatingPointCode
					2, // DestinationPointCode
					3, // ServiceIndicator
					1, // NetworkIndicator
					0, // MessagePriority
					1, // SignalingLinkSelection
					[]byte{ // Data
						0xde, 0xad, 0xbe, 0xef,
					},
				),
				nil,
			),
			[]byte{
				// Header
				0x01, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x24,
				// RoutingContext
				0x00, 0x06, 0x00, 0x08,
				0x00, 0x00, 0x00, 0xff,
				// ProtocolData
				// Param Header
				0x02, 0x10, 0x00, 0x14,
				// OPC, DPC
				0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02,
				// SI, NI, MP, SLS
				0x03, 0x01, 0x00, 0x01,
				// Data
				0xde, 0xad, 0xbe, 0xef,
			},
		},
		{
			"has-none",
			NewData(
				nil, nil,
				params.NewProtocolData(
					1, // OriginatingPointCode
					2, // DestinationPointCode
					3, // ServiceIndicator
					1, // NetworkIndicator
					0, // MessagePriority
					1, // SignalingLinkSelection
					[]byte{ // Data
						0xde, 0xad, 0xbe, 0xef,
					},
				),
				nil,
			),
			[]byte{
				// Header
				0x01, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x1c,
				// ProtocolData
				// Param Header
				0x02, 0x10, 0x00, 0x14,
				// OPC, DPC
				0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02,
				// SI, NI, MP, SLS
				0x03, 0x01, 0x00, 0x01,
				// Data
				0xde, 0xad, 0xbe, 0xef,
			},
		},
	}

	runTests(t, cases, func(b []byte) (serializeable, error) {
		v, err := ParseData(b)
		if err != nil {
			return nil, err
		}
		v.Payload = nil
		return v, nil
	})
}

func TestDataRequiresNetworkAppearanceFirst(t *testing.T) {
	wrongOrder := New(
		1,
		MsgClassTransfer,
		MsgTypePayloadData,
		params.NewRoutingContext(7),
		params.NewNetworkAppearance(8),
		params.NewProtocolData(1, 2, params.ServiceIndSCCP, 0, 0, 1, []byte("x")),
	)
	raw, err := wrongOrder.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	if _, err := ParseData(raw); !errors.Is(err, ErrInvalidParameter) {
		t.Errorf("ParseData with Network Appearance after Routing Context = %v, want ErrInvalidParameter", err)
	}
}

func TestDataRejectsMalformedNetworkAppearanceLength(t *testing.T) {
	for _, size := range []int{0, 1, 3, 5, 8} {
		t.Run(string(rune('0'+size)), func(t *testing.T) {
			protocolData, err := params.NewProtocolData(
				1, 2, params.ServiceIndSCCP, 0, 0, 1, []byte("x"),
			).MarshalBinary()
			if err != nil {
				t.Fatal(err)
			}

			parameterLength := 4 + size
			padding := (4 - parameterLength%4) % 4
			networkAppearance := make([]byte, parameterLength+padding)
			binary.BigEndian.PutUint16(networkAppearance[0:2], params.NetworkAppearance)
			binary.BigEndian.PutUint16(networkAppearance[2:4], uint16(parameterLength))

			raw, err := NewHeader(
				1,
				MsgClassTransfer,
				MsgTypePayloadData,
				append(networkAppearance, protocolData...),
			).MarshalBinary()
			if err != nil {
				t.Fatal(err)
			}

			if _, err := ParseData(raw); !errors.Is(err, params.ErrInvalidLength) {
				t.Errorf("ParseData with %d-byte Network Appearance = %v, want params.ErrInvalidLength", size, err)
			}
		})
	}
}
