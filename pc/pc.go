// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

// Package pc provides Point Code converting from some variants and translation to IP.
package pc

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Variant is a variant of Signaling Point Code represented in string.
type Variant string

// PointCode variant definitions.
const (
	VariantNone Variant = ""
	Variant383  Variant = "3-8-3"   // ITU
	Variant437  Variant = "4-3-7"   // ITU
	Variant4343 Variant = "4-3-4-3" // ITU
	Variant446  Variant = "4-4-6"   // ??
	Variant545  Variant = "5-4-5"   // ??
	Variant662  Variant = "6-6-2"   // ??
	Variant68   Variant = "6-8"     // ??
	Variant745  Variant = "7-4-5"   // Japan
	Variant77   Variant = "7-7"     // ??
	Variant888  Variant = "8-8-8"   // ANSI & China
)

// BitLength returns the defined bit length of Variant in int.
func (v Variant) BitLength() int {
	_, bitLength, ok := v.bitFields()
	if !ok {
		return 0
	}
	return bitLength
}

func (v Variant) bitFields() ([]int, int, bool) {
	if v == VariantNone {
		return nil, 0, false
	}
	ss := strings.Split(v.String(), "-")
	fields := make([]int, len(ss))
	var total int
	for i, digit := range ss {
		width, err := strconv.Atoi(digit)
		if err != nil || width == 0 || width > 32 {
			return nil, 0, false
		}
		if total > 32-width {
			return nil, 0, false
		}
		fields[i] = width
		total += fields[i]
	}
	return fields, total, true
}

// String returns Variant in string representation.
func (v Variant) String() string {
	return string(v)
}

// PointCode represents a Signaling Point Code with its variant.
type PointCode struct {
	raw       uint32
	formatted string
	form      Variant
}

// NewPointCode creates a new PointCode from raw(uint32) value.
func NewPointCode(raw uint32, variant Variant) *PointCode {
	_, bitLength, ok := variant.bitFields()
	if !ok {
		return nil
	}
	p := &PointCode{
		raw: raw, form: variant,
	}
	// apply bitmask
	p.raw &= bitMask(bitLength)

	var err error
	p.formatted, err = p.ConvertTo(variant)
	if err != nil {
		return nil
	}
	return p
}

// NewPointCodeFrom creates a new PointCode from formatted Signaling Point Code.
func NewPointCodeFrom(pc string, variant Variant) *PointCode {
	if variant == VariantNone {
		return nil
	}

	raw, err := convStrToRaw(pc, variant)
	if err != nil {
		return nil
	}
	return &PointCode{
		raw: raw, formatted: pc, form: variant,
	}
}

// Uint32 returns PointCode values in uint32.
func (pc *PointCode) Uint32() uint32 {
	return pc.raw
}

// Variant returns the variant of PointCode in string.
func (pc *PointCode) Variant() string {
	return pc.form.String()
}

// ConvertTo converts raw Signaling Point Code into specified Variant
// and returns converted PC value in string.
// The converted value is stored in PointCode and can be retrieved with
// String() without re-calculation.
func (pc *PointCode) ConvertTo(variant Variant) (string, error) {
	str, err := convRawToStr(pc.raw, variant)
	if err != nil {
		return "", err
	}
	pc.formatted = str
	return str, nil
}

func convRawToStr(n uint32, v Variant) (string, error) {
	fields, bitLength, ok := v.bitFields()
	if !ok {
		return "", errors.New("invalid Variant given")
	}

	r := bitLength
	n &= bitMask(r) // apply bitmask

	d := make([]string, len(fields))
	for i, width := range fields {
		r -= width
		x := (n >> r) & bitMask(width)
		d[i] = strconv.FormatUint(uint64(x), 10)
	}

	return strings.Join(d, "-"), nil
}

func convStrToRaw(f string, v Variant) (uint32, error) {
	fields, bitLength, ok := v.bitFields()
	if !ok {
		return 0, errors.New("invalid Variant given")
	}

	ds := strings.Split(f, "-")
	if len(ds) == 0 {
		return 0, fmt.Errorf("PC: %s is invalid; digits should be splitted with \"-\"", f)
	}
	if len(ds) != len(fields) {
		return 0, fmt.Errorf("PC: %s and Variant: %s doesn't match", f, v)
	}

	r := bitLength
	var n uint32
	for i, d := range ds {
		x, err := parsePointCodeSegment(d, fields[i])
		if err != nil {
			return 0, err
		}
		r -= fields[i]
		n |= x << r
	}

	return n, nil
}

func parsePointCodeSegment(digit string, width int) (uint32, error) {
	value, err := strconv.ParseUint(digit, 10, 32)
	if err != nil {
		return 0, err
	}
	if value > uint64(bitMask(width)) {
		return 0, fmt.Errorf("PC digit %q exceeds %d-bit field", digit, width)
	}
	return uint32(value), nil
}

func bitMask(width int) uint32 {
	if width >= 32 {
		return ^uint32(0)
	}
	return (uint32(1) << width) - 1
}

// String returns PointCode in formatted string.
func (pc *PointCode) String() string {
	if pc.form == VariantNone {
		return ""
	}

	return pc.formatted
}
