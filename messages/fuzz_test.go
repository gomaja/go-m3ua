// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package messages_test

import (
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// Parse is the library's untrusted-input boundary: every byte a peer puts on
// the wire reaches it, and go-m3ua's dispatcher hands whatever comes back
// straight to the state machine. These targets run under the native fuzzer
// (`go test ./messages/ -fuzz=FuzzParse`), so they execute as ordinary
// regression tests against the seed corpus on every `go test` run and grow a
// corpus when fuzzed explicitly.
//
// Three properties are checked, in increasing strength:
//
//  1. Parse never panics, whatever the input.
//  2. A message that parses must marshal back without panicking, and must
//     report a MarshalLen consistent with what it produces.
//  3. Re-parsing that output must succeed and yield the same message type --
//     a decoder that accepts its own output is the minimum bar for interop.

// seeds covers every message the library defines plus the malformed shapes that
// have historically broken parsers: truncated headers, absurd length fields,
// zero-length parameters, and unknown class/type combinations.
func seeds() [][]byte {
	var out [][]byte

	msgs := []messages.M3UA{
		messages.NewAspUp(params.NewAspIdentifier(1), nil),
		messages.NewAspUpAck(params.NewAspIdentifier(1), nil),
		messages.NewAspDown(nil),
		messages.NewAspDownAck(nil),
		messages.NewHeartbeat(params.NewHeartbeatData([]byte("beat"))),
		messages.NewHeartbeatAck(params.NewHeartbeatData([]byte("beat"))),
		messages.NewAspActive(
			params.NewTrafficModeType(params.TrafficModeLoadshare),
			params.NewRoutingContext(1, 2), nil),
		messages.NewAspActiveAck(
			params.NewTrafficModeType(params.TrafficModeLoadshare),
			params.NewRoutingContext(1, 2), nil),
		messages.NewAspInactive(params.NewRoutingContext(1), nil),
		messages.NewAspInactiveAck(params.NewRoutingContext(1), nil),
		messages.NewError(params.NewErrorCode(params.UnexpectedMessageError), nil, nil, nil, nil),
		messages.NewNotify(params.NewStatus(params.AsStateActive), nil, nil, nil),
		messages.NewData(nil, nil,
			params.NewProtocolData(1, 2, 3, 0, 0, 1, []byte{0xde, 0xad, 0xbe, 0xef}), nil),
		messages.NewDestinationUnavailable(nil, nil, params.NewAffectedPointCode(1), nil),
		messages.NewDestinationAvailable(nil, nil, params.NewAffectedPointCode(1), nil),
		messages.NewDestinationStateAudit(nil, nil, params.NewAffectedPointCode(1), nil),
		messages.NewSignallingCongestion(nil, nil, params.NewAffectedPointCode(1), nil, nil, nil),
		messages.NewDestinationUserPartUnavailable(
			nil, nil, params.NewAffectedPointCode(1), params.NewUserCause(params.SCCP, params.Unequipped), nil,
		),
		messages.NewDestinationRestricted(nil, nil, params.NewAffectedPointCode(1), nil),
	}
	for _, m := range msgs {
		if b, err := m.MarshalBinary(); err == nil {
			out = append(out, b)
		}
	}

	out = append(out,
		[]byte{},                       // empty
		[]byte{0x01},                   // one byte
		[]byte{0x01, 0x00, 0x03, 0x01}, // half a header
		[]byte{0x01, 0x00, 0x03, 0x01, 0x00, 0x00, 0x00, 0x08}, // bare ASP Up
		[]byte{0x01, 0x00, 0x03, 0x01, 0x00, 0x00, 0x00, 0x00}, // length 0
		[]byte{0x01, 0x00, 0x03, 0x01, 0xff, 0xff, 0xff, 0xff}, // length overflow
		[]byte{0x01, 0x00, 0x03, 0x01, 0x00, 0x00, 0x00, 0x04}, // length below header
		[]byte{0x02, 0x00, 0x03, 0x01, 0x00, 0x00, 0x00, 0x08}, // version 2
		[]byte{0x01, 0x00, 0xff, 0xff, 0x00, 0x00, 0x00, 0x08}, // unknown class/type
		[]byte{0x01, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x0c, // DATA, param truncated
			0x02, 0x10, 0x00, 0x08},
		[]byte{0x01, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x10, // DATA, param len 0
			0x02, 0x10, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
		[]byte{0x01, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x10, // DATA, param len huge
			0x02, 0x10, 0xff, 0xff, 0x00, 0x00, 0x00, 0x00},
	)

	return out
}

// FuzzParse checks that Parse survives arbitrary input and that anything it
// accepts survives a marshal/re-parse round trip.
func FuzzParse(f *testing.F) {
	for _, s := range seeds() {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, data []byte) {
		msg, err := messages.Parse(data)
		if err != nil {
			return // rejecting malformed input is the correct outcome
		}
		if msg == nil {
			t.Fatal("Parse returned nil message and nil error")
		}

		// Accessors run on every dispatch path in go-m3ua, so they must be
		// safe on anything Parse accepted.
		_ = msg.MessageClassName()
		_ = msg.MessageTypeName()
		_ = msg.Version()

		b, err := msg.MarshalBinary()
		if err != nil {
			// A message that parsed but cannot be re-marshalled is acceptable
			// only as an explicit error, never a panic.
			return
		}

		if got := msg.MarshalLen(); got != len(b) {
			t.Errorf("MarshalLen() = %d, but MarshalBinary produced %d bytes", got, len(b))
		}

		// The decoder must accept its own output.
		again, err := messages.Parse(b)
		if err != nil {
			t.Fatalf("re-parsing our own MarshalBinary output failed: %v (input %x -> %x)", err, data, b)
		}
		if got, want := again.MessageTypeName(), msg.MessageTypeName(); got != want {
			t.Errorf("round trip changed message type: %q -> %q", want, got)
		}
	})
}

// FuzzParseParams targets the parameter decoder directly. Parameters carry the
// TLV lengths that malformed input most easily abuses, and ParseMultiParams is
// reached from every message body.
func FuzzParseParams(f *testing.F) {
	for _, p := range []*params.Param{
		params.NewAspIdentifier(1),
		params.NewRoutingContext(1, 2, 3),
		params.NewHeartbeatData([]byte("beat")),
		params.NewErrorCode(params.UnexpectedMessageError),
		params.NewStatus(params.AsStateActive),
		params.NewTrafficModeType(params.TrafficModeLoadshare),
		params.NewProtocolData(1, 2, 3, 0, 0, 1, []byte{0xde, 0xad}),
		params.NewInfoString("info"),
	} {
		if b, err := p.MarshalBinary(); err == nil {
			f.Add(b)
		}
	}
	f.Add([]byte{})
	f.Add([]byte{0x00, 0x04, 0x00, 0x08})
	f.Add([]byte{0x00, 0x04, 0xff, 0xff})
	f.Add([]byte{0x00, 0x04, 0x00, 0x00})

	f.Fuzz(func(t *testing.T, data []byte) {
		ps, err := params.ParseMultiParams(data)
		if err != nil {
			return
		}

		for _, p := range ps {
			if p == nil {
				t.Fatal("ParseMultiParams returned a nil Param with no error")
			}

			// Every typed accessor must be safe regardless of the Tag actually
			// present: go-m3ua calls these after a type switch that trusts the
			// tag, and a mismatched tag must return a zero value, not panic.
			_ = p.AspIdentifier()
			_ = p.RoutingContext()
			_ = p.HeartbeatData()
			_ = p.ErrorCode()
			_ = p.Status()
			_ = p.TrafficModeType()
			_ = p.InfoString()
			_ = p.String()

			if pd, err := p.ProtocolData(); err == nil && pd != nil {
				_ = pd.MarshalLen()
			}

			b, err := p.MarshalBinary()
			if err != nil {
				continue
			}
			if got := p.MarshalLen(); got != len(b) {
				t.Errorf("Param.MarshalLen() = %d, but MarshalBinary produced %d bytes", got, len(b))
			}
		}
	})
}
