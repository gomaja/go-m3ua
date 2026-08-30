// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package params

// PointCodeWithMask is one RFC 4666 point-code word: an 8-bit Mask followed
// by a 24-bit point code.
type PointCodeWithMask struct {
	Mask      uint8
	PointCode uint32
}

// NewOriginatingPointCodeList creates the OriginatingPointCodeList Parameter.
// Each argument is the complete 32-bit wire word, with its Mask in the most
// significant octet. Use NewOriginatingPointCodeListWithMasks to provide the
// two RFC fields separately.
func NewOriginatingPointCodeList(opcs ...uint32) *Param {
	return newMultiUint32ValParam(OriginatingPointCodeList, opcs...)
}

// NewOriginatingPointCodeListWithMasks creates an Originating Point Code List
// from separate Mask and point-code fields as defined by RFC 4666 Section
// 3.6.1. Bits above the low 24 of PointCode are not part of the field and are
// discarded.
func NewOriginatingPointCodeListWithMasks(entries ...PointCodeWithMask) *Param {
	words := make([]uint32, len(entries))
	for index, entry := range entries {
		words[index] = uint32(entry.Mask)<<24 | entry.PointCode&0x00ffffff
	}
	return NewOriginatingPointCodeList(words...)
}

// OriginatingPointCodeList returns multiple OriginatingPointCode from Param.
func (p *Param) OriginatingPointCodeList() []uint32 {
	if p.Tag != OriginatingPointCodeList {
		return nil
	}
	return p.decodeMultiUint32ValData()
}

// OriginatingPointCodeListEntries returns every Originating Point Code List
// entry with the Mask and 24-bit point code separated. It returns nil for a
// different parameter tag or malformed value length.
func (p *Param) OriginatingPointCodeListEntries() []PointCodeWithMask {
	words := p.OriginatingPointCodeList()
	if words == nil {
		return nil
	}
	entries := make([]PointCodeWithMask, len(words))
	for index, word := range words {
		entries[index].Mask, entries[index].PointCode = pointCodeMask(word)
	}
	return entries
}
