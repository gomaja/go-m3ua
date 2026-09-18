// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package messages_test

import (
	"errors"
	"reflect"
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
		// Management messages carrying the parameters RFC 4666 Section 3.8.1
		// makes "Mandatory*": a peer reports which context, network and
		// destinations it refused, and those are the octets the receive path
		// has to survive.
		messages.NewError(
			params.NewErrorCode(params.ErrInvalidRoutingContext),
			params.NewRoutingContext(9, 10),
			params.NewNetworkAppearance(7),
			params.NewAffectedPointCodeWithMask(4, 0x123456),
			params.NewDiagnosticInformation([]byte("diag")),
		),
		messages.NewNotify(
			params.NewStatus(params.AlternateAspActive),
			params.NewAspIdentifier(42),
			params.NewRoutingContext(1, 2),
			params.NewInfoString("info"),
		),
		// Routing Key Management (Section 3.6) nests parameters inside
		// parameters, which is where a length field is most easily abused, and
		// no seed reached it before.
		messages.NewRegistrationRequest(
			params.NewRoutingKey(params.NewRoutingKeyPayload(
				params.NewLocalRoutingKeyIdentifier(1),
				params.NewRoutingContext(2),
				params.NewTrafficModeType(params.TrafficModeLoadshare),
				params.NewNetworkAppearance(3),
				params.NewRoutingKeyGroup(
					params.NewDestinationPointCode(4),
					params.NewServiceIndicators(params.ServiceIndSCCP),
					params.NewOriginatingPointCodeList(5),
				),
				params.NewRoutingKeyGroup(params.NewDestinationPointCode(6), nil, nil),
			)),
		),
		messages.NewRegistrationResponse(
			params.NewRegistrationResult(params.NewRegistrationResultPayload(
				params.NewLocalRoutingKeyIdentifier(1),
				params.NewRegistrationStatus(params.SuccessfullyRegistered),
				params.NewRoutingContext(2),
			)),
		),
		messages.NewDeregistrationRequest(params.NewRoutingContext(2)),
		messages.NewDeregistrationResponse(
			params.NewDeregistrationResult(params.NewDeregResultPayload(
				params.NewRoutingContext(2),
				params.NewDeregistrationStatus(params.SuccessfullyDeregistered),
			)),
		),
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
		// NTFY with a Routing Context whose value is not a whole number of
		// 32-bit words, which Section 3.2's length rules reject.
		[]byte{0x01, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x14,
			0x00, 0x0d, 0x00, 0x08, 0x00, 0x01, 0x00, 0x02,
			0x00, 0x06, 0x00, 0x07, 0x00, 0x00, 0x09, 0x00},
		// NTFY with an INFO String that is not valid UTF-8, the one violation
		// ParseWithOptions can be told to tolerate.
		[]byte{0x01, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x14,
			0x00, 0x0d, 0x00, 0x08, 0x00, 0x01, 0x00, 0x02,
			0x00, 0x04, 0x00, 0x06, 0xff, 0xfe, 0x00, 0x00},
		// NTFY carrying both an invalid INFO String and, behind it, a Routing
		// Context of three octets. The tolerance classifies the first fault, so
		// the second one is only caught if the accept and drop paths re-decode
		// the whole message.
		[]byte{0x01, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x20,
			0x00, 0x0d, 0x00, 0x08, 0x00, 0x01, 0x00, 0x02,
			0x00, 0x04, 0x00, 0x06, 0xff, 0xfe, 0x00, 0x00,
			0x00, 0x06, 0x00, 0x07, 0x00, 0x00, 0x09, 0x00},
		// The same pair with the order reversed, so the malformed Routing
		// Context is the fault the scan reaches first.
		[]byte{0x01, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x20,
			0x00, 0x0d, 0x00, 0x08, 0x00, 0x01, 0x00, 0x02,
			0x00, 0x06, 0x00, 0x07, 0x00, 0x00, 0x09, 0x00,
			0x00, 0x04, 0x00, 0x06, 0xff, 0xfe, 0x00, 0x00},
		// NTFY whose mandatory Status is missing behind an invalid INFO String.
		[]byte{0x01, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x10,
			0x00, 0x04, 0x00, 0x06, 0xff, 0xfe, 0x00, 0x00},
		// REG REQ whose nested Routing Key declares more octets than it holds.
		[]byte{0x01, 0x00, 0x09, 0x01, 0x00, 0x00, 0x00, 0x10,
			0x02, 0x07, 0xff, 0xff, 0x02, 0x0a, 0x00, 0x08},
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

// FuzzParseWithOptions is the receive-side compatibility policy under the same
// untrusted input as Parse. Two properties matter, and neither is expressible
// as a fixed corpus:
//
//  1. The zero ParseOptions is the strict decoder. A caller that constructs
//     the struct and sets no Tolerator has asked for no tolerance, and must
//     get byte-identical behaviour to Parse on every input.
//
//  2. Tolerance is a hole only for the violation it classifies. If a tolerant
//     decode accepts octets the strict decoder rejected, the strict rejection
//     must have been an invalid parameter value -- never a length error, a
//     missing mandatory parameter or a parameter-order violation. Those are
//     the rules a tolerated INFO String must not be able to smuggle a message
//     past.
func FuzzParseWithOptions(f *testing.F) {
	for _, s := range seeds() {
		f.Add(s)
	}

	accept := messages.ToleratorFunc(func(messages.ProtocolViolation) messages.ProtocolDecision {
		return messages.ProtocolAccept
	})
	drop := messages.ToleratorFunc(func(messages.ProtocolViolation) messages.ProtocolDecision {
		return messages.ProtocolDropParameter
	})
	reject := messages.ToleratorFunc(func(messages.ProtocolViolation) messages.ProtocolDecision {
		return messages.ProtocolReject
	})

	f.Fuzz(func(t *testing.T, data []byte) {
		strict, strictErr := messages.Parse(data)

		zero, zeroErr := messages.ParseWithOptions(data, messages.ParseOptions{})
		switch {
		case (strictErr == nil) != (zeroErr == nil):
			t.Fatalf("zero options disagreed with Parse: Parse error = %v, zero error = %v",
				strictErr, zeroErr)
		case strictErr != nil:
			if strictErr.Error() != zeroErr.Error() {
				t.Errorf("zero options error = %q, Parse error = %q", zeroErr, strictErr)
			}
		default:
			if zero.MessageTypeName() != strict.MessageTypeName() {
				t.Errorf("zero options decoded %q, Parse decoded %q",
					zero.MessageTypeName(), strict.MessageTypeName())
			}
		}

		// Rejecting every classified violation can never accept more than the
		// strict decoder does.
		if _, err := messages.ParseWithOptions(data, messages.ParseOptions{Tolerator: reject}); (err == nil) != (strictErr == nil) {
			t.Errorf("rejecting tolerator error = %v, Parse error = %v", err, strictErr)
		}

		for _, policy := range []struct {
			name      string
			tolerator messages.ProtocolTolerator
		}{{"accept", accept}, {"drop", drop}} {
			message, err := messages.ParseWithOptions(data, messages.ParseOptions{Tolerator: policy.tolerator})
			if err != nil {
				continue
			}
			if message == nil {
				t.Fatalf("%s tolerator returned nil message and nil error", policy.name)
			}
			if strictErr == nil {
				if message.MessageTypeName() != strict.MessageTypeName() {
					t.Errorf("%s tolerator decoded %q where Parse decoded %q",
						policy.name, message.MessageTypeName(), strict.MessageTypeName())
				}
				continue
			}
			if !errors.Is(strictErr, params.ErrInvalidValue) {
				t.Fatalf("%s tolerator accepted octets Parse rejected with %v; only an "+
					"invalid optional INFO String value is tolerable", policy.name, strictErr)
			}
			// An invalid parameter value is a necessary condition, not a
			// sufficient one: the INFO String is only the fault the scan
			// reaches first, and a second fault behind it must still be
			// rejected. Each policy therefore has to show it produced the
			// message its own path promises, not merely some message.
			switch policy.name {
			case "accept":
				// Accepting keeps the offending parameter, so the decoded
				// message must actually carry it.
				field := reflect.ValueOf(message).Elem().FieldByName("InfoString")
				if !field.IsValid() || field.IsNil() {
					t.Fatalf("accepting tolerator returned a message with no INFO String, "+
						"so nothing was tolerated: %v", message)
				}
			case "drop":
				// Dropping the offending parameter must leave a message the
				// strict decoder would have accepted on its own.
				wire, err := message.MarshalBinary()
				if err != nil {
					t.Fatalf("dropped-parameter message does not re-marshal: %v", err)
				}
				if _, err := messages.Parse(wire); err != nil {
					t.Fatalf("dropped-parameter message does not re-parse: %v", err)
				}
			}
		}
	})
}
