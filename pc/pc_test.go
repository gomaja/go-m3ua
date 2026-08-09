// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package pc_test

import (
	"testing"

	"github.com/gomaja/go-m3ua/pc"
)

// TODO: coverage...

func TestConvertPointCode(t *testing.T) {
	cases := []struct {
		name             string
		raw              uint32
		currVar, nextVar pc.Variant
		before, after    string
	}{
		{
			name:    "1234/3-8-3 to 4-3-7",
			raw:     1234,
			currVar: pc.Variant383,
			nextVar: pc.Variant437,
			before:  "0-154-2",
			after:   "1-1-82",
		}, {
			name:    "0xffffffff/3-8-3 to 4-3-7",
			raw:     0xffffffff,
			currVar: pc.Variant383,
			nextVar: pc.Variant437,
			before:  "7-255-7",
			after:   "15-7-127",
		}, {
			name:    "0/3-8-3 to 4-3-7",
			raw:     0,
			currVar: pc.Variant383,
			nextVar: pc.Variant437,
			before:  "0-0-0",
			after:   "0-0-0",
		},
	}

	for _, c := range cases {
		p := pc.NewPointCode(c.raw, c.currVar)
		if got, want := p.String(), c.before; got != want {
			t.Errorf("NewPointCode failed. got: %s, want: %s", got, want)
		}

		if got, want := pc.NewPointCodeFrom(c.before, c.currVar).Uint32(), p.Uint32(); got != want {
			t.Errorf("NewPointCodeFrom failed. got: %d, want: %d", got, want)
		}

		if got, err := p.ConvertTo(c.nextVar); err != nil {
			t.Fatalf("Failed to convert %s to %s", p.Variant(), c.nextVar)
		} else {
			want := c.after
			if got != want {
				t.Errorf("ConvertTo failed. got: %s, want: %s", got, want)
			}
		}

		if got, want := pc.NewPointCodeFrom(c.after, c.nextVar).Uint32(), p.Uint32(); got != want {
			t.Errorf("NewPointCodeFrom failed. got: %d, want: %d", got, want)
		}
	}
}

func TestNewPointCodeFromRejectsSegmentsOutsideVariantBitWidth(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		variant pc.Variant
	}{
		{name: "8 bit segment one above maximum", input: "256-0-0", variant: pc.Variant888},
		{name: "8 bit segment above uint32 maximum", input: "4294967296-0-0", variant: pc.Variant888},
		{name: "first 3 bit segment one above maximum", input: "8-0-0", variant: pc.Variant383},
		{name: "middle 8 bit segment one above maximum", input: "0-256-0", variant: pc.Variant383},
		{name: "last 3 bit segment one above maximum", input: "0-0-8", variant: pc.Variant383},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := pc.NewPointCodeFrom(test.input, test.variant); got != nil {
				t.Fatalf("NewPointCodeFrom(%q, %q) = raw %#x formatted %q, want nil",
					test.input, test.variant, got.Uint32(), got.String())
			}
		})
	}
}

func TestNewPointCodeFromAcceptsMaximumSegmentValues(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		variant pc.Variant
		want    uint32
	}{
		{name: "3-8-3 maximum", input: "7-255-7", variant: pc.Variant383, want: 0x3fff},
		{name: "8-8-8 maximum", input: "255-255-255", variant: pc.Variant888, want: 0xffffff},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := pc.NewPointCodeFrom(test.input, test.variant)
			if got == nil {
				t.Fatalf("NewPointCodeFrom(%q, %q) = nil, want raw %#x", test.input, test.variant, test.want)
			}
			if raw := got.Uint32(); raw != test.want {
				t.Fatalf("NewPointCodeFrom(%q, %q).Uint32() = %#x, want %#x",
					test.input, test.variant, raw, test.want)
			}
		})
	}
}

func TestVariant446UsesFourFourSixLayout(t *testing.T) {
	const formatted = "15-15-63"
	const raw = 0x3fff

	fromRaw := pc.NewPointCode(raw, pc.Variant446)
	if fromRaw == nil {
		t.Fatal("NewPointCode returned nil for Variant446")
	}
	if got := fromRaw.String(); got != formatted {
		t.Fatalf("NewPointCode(%#x, %q).String() = %q, want %q", raw, pc.Variant446, got, formatted)
	}

	fromFormatted := pc.NewPointCodeFrom(formatted, pc.Variant446)
	if fromFormatted == nil {
		t.Fatal("NewPointCodeFrom returned nil for Variant446")
	}
	if got := fromFormatted.Uint32(); got != raw {
		t.Fatalf("NewPointCodeFrom(%q, %q).Uint32() = %#x, want %#x",
			formatted, pc.Variant446, got, raw)
	}
}

func TestInvalidVariantBitLayoutsAreRejected(t *testing.T) {
	tests := []pc.Variant{
		pc.Variant("0"),
		pc.Variant("33"),
		pc.Variant("16-17"),
		pc.Variant("4294967296"),
	}

	for _, variant := range tests {
		t.Run(variant.String(), func(t *testing.T) {
			if got := variant.BitLength(); got != 0 {
				t.Fatalf("BitLength(%q) = %d, want 0", variant, got)
			}
			if got := pc.NewPointCode(0, variant); got != nil {
				t.Fatalf("NewPointCode accepted invalid Variant %q as %q", variant, got.String())
			}
			if got := pc.NewPointCodeFrom("0", variant); got != nil {
				t.Fatalf("NewPointCodeFrom accepted invalid Variant %q as raw %#x", variant, got.Uint32())
			}
		})
	}
}

func FuzzNewPointCodeFrom(f *testing.F) {
	variants := []pc.Variant{
		pc.Variant383,
		pc.Variant446,
		pc.Variant888,
		pc.Variant("16-16"),
		pc.Variant("0"),
		pc.Variant("33"),
	}
	seeds := []string{
		"0-0-0",
		"7-255-7",
		"15-15-63",
		"255-255-255",
		"256-0-0",
		"4294967296-0-0",
		"",
		"--",
	}
	for index, seed := range seeds {
		f.Add(seed, uint8(index))
	}

	f.Fuzz(func(t *testing.T, input string, variantIndex uint8) {
		variant := variants[int(variantIndex)%len(variants)]
		pointCode := pc.NewPointCodeFrom(input, variant)
		if pointCode == nil {
			return
		}
		if got := pointCode.Variant(); got != variant.String() {
			t.Fatalf("accepted point code Variant() = %q, want %q", got, variant)
		}
		fromRaw := pc.NewPointCode(pointCode.Uint32(), variant)
		if fromRaw == nil {
			t.Fatalf("NewPointCode rejected raw %#x accepted from %q/%q", pointCode.Uint32(), input, variant)
		}
		if got, want := fromRaw.Uint32(), pointCode.Uint32(); got != want {
			t.Fatalf("raw round trip = %#x, want %#x for %q/%q", got, want, input, variant)
		}
	})
}
