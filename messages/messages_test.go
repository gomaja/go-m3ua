// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package messages

import (
	"testing"
)

// Parse selects the concrete type from the Message Class and Message Type
// bytes. Whatever it selects must agree with the bytes that were on the wire:
// a peer must not be able to reach one message's handler by writing another
// message's class and type.
//
// The selector combined the two as `uint8(class)<<4 | type`, which discards the
// class entirely — the shift happens in 8 bits — so every class collapsed onto
// the type byte. Class 0 (Management) with type 0x31 was parsed as an ASP Up,
// class 0 with type 0x41 as an ASP Active, and each reported the class of the
// message it had been mistaken for, so nothing downstream could tell. At an SGP
// that hands a peer the ASP state machine: RFC 4666 Section 4.3.4.1 takes an
// ASP-ACTIVE association down to ASP-INACTIVE on a received ASP Up, and a
// forged one did it just as well as a real one. Class values of 16 and above
// wrapped for the same reason.
//
// The whole 8x8-bit space is swept, because the aliases are exactly the
// combinations no hand-written case list thinks to include.
func TestParseNeverMisreportsClassOrType(t *testing.T) {
	for class := 0; class <= 0xff; class++ {
		for mtype := 0; mtype <= 0xff; mtype++ {
			b := []byte{
				0x01, 0x00, uint8(class), uint8(mtype),
				0x00, 0x00, 0x00, 0x08,
			}
			m, err := Parse(b)
			if err != nil {
				// Refusing a combination is fine; misrepresenting one is not.
				continue
			}
			if got := m.MessageClass(); got != uint8(class) {
				t.Fatalf("wire class=0x%02x type=0x%02x parsed as %T reporting class 0x%02x",
					class, mtype, m, got)
			}
			if got := m.MessageType(); got != uint8(mtype) {
				t.Fatalf("wire class=0x%02x type=0x%02x parsed as %T reporting type 0x%02x",
					class, mtype, m, got)
			}
		}
	}
}

func TestParseMalformed(t *testing.T) {
	cases := []struct {
		data []byte
		err  error
	}{
		{[]byte{0x00}, ErrTooShortToParse},
		{[]byte{0x00, 0x00}, ErrTooShortToParse},
		{[]byte{0x00, 0x00, 0x00}, ErrTooShortToParse},
		{[]byte{0x00, 0x00, 0x00, 0x00}, ErrTooShortToParse},
	}

	for _, c := range cases {
		if _, err := Parse(c.data); err != c.err {
			t.Errorf("Parse/unexpected error: got: %v, want: %v", err, c.err)
		}
	}
}
