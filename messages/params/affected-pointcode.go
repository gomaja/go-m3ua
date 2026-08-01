// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package params

// pointCodeMask splits an Affected Point Code word into its Mask octet and its
// 24-bit point code.
//
// RFC 4666 Section 3.4.1 lays each word out as an 8-bit Mask followed by the
// Affected PC, for every point-code format it shows. The two are separate
// fields: "The Mask parameter is an integer representing a bit mask that can be
// applied to the related Affected PC field."
func pointCodeMask(word uint32) (mask uint8, pc uint32) {
	return uint8(word >> 24), word & 0x00FFFFFF
}

// NewAffectedPointCode creates the AffectedPointCode Parameter from whole
// 32-bit words, each carrying its Mask in the most significant octet.
//
// Use NewAffectedPointCodeWithMask to supply the two halves separately.
func NewAffectedPointCode(apcs ...uint32) *Param {
	return newMultiUint32ValParam(AffectedPointCode, apcs...)
}

// NewAffectedPointCodeWithMask creates the AffectedPointCode Parameter from a
// single point code and its Mask.
//
// The Mask names how many low-order bits of the point code are wildcarded, so
// it is how an SG reports a whole range unavailable in one message: RFC 4666
// Section 3.4.1 gives 8 for an ANSI cluster and 3 for an ITU region, and "A mask
// value equal (or greater than) the number of bits in the PC indicates that the
// entire network appearance is affected; this is used to indicate network
// isolation to the ASP."
//
// Bits of pc above the low 24 are not part of the field and are discarded.
func NewAffectedPointCodeWithMask(mask uint8, pc uint32) *Param {
	return newMultiUint32ValParam(AffectedPointCode, uint32(mask)<<24|pc&0x00FFFFFF)
}

// AffectedPointCode returns the first Affected Point Code from Param, without
// its Mask.
//
// As with RoutingContext, the value is a sequence of 32-bit words: a payload
// that is empty or not a multiple of four decodes to nothing, so there is no
// point code to return.
//
// The Mask is deliberately not folded into the result. It used to be, which
// produced values wider than any point-code format the RFC defines: a DUNA
// announcing an ANSI cluster unavailable arrived as 0x08001234 rather than
// 0x001234, so the destination that really went away was never marked down and
// one that cannot exist was. Read the Mask with AffectedPointCodeMasks.
func (p *Param) AffectedPointCode() uint32 {
	if p.Tag != AffectedPointCode {
		return 0
	}

	apcs := p.AffectedPointCodes()
	if len(apcs) == 0 {
		return 0
	}

	return apcs[0]
}

// AffectedPointCodes returns every Affected Point Code from Param, each without
// its Mask.
func (p *Param) AffectedPointCodes() []uint32 {
	if p.Tag != AffectedPointCode {
		return nil
	}

	words := p.decodeMultiUint32ValData()
	if words == nil {
		return nil
	}

	pcs := make([]uint32, len(words))
	for i, w := range words {
		_, pcs[i] = pointCodeMask(w)
	}
	return pcs
}

// AffectedPointCodeMasks returns the Mask octet of every Affected Point Code in
// Param, positionally matching AffectedPointCodes.
//
// A Mask of zero names exactly the point code beside it, which is the ordinary
// case; anything larger names a contiguous range around it.
func (p *Param) AffectedPointCodeMasks() []uint8 {
	if p.Tag != AffectedPointCode {
		return nil
	}

	words := p.decodeMultiUint32ValData()
	if words == nil {
		return nil
	}

	masks := make([]uint8, len(words))
	for i, w := range words {
		masks[i], _ = pointCodeMask(w)
	}
	return masks
}

/* TODO: Might be implemented in the following way?
// PointCodeWithMask is a set of Mask and Point Code.
type PointCodeWithMask struct {
	Mask      uint8
	PointCode uint32
}

// MarshalBinary creates the 32bit-sized []byte from PointCodeWithMask.
func (p *PointCodeWithMask) MarshalBinary() ([]byte, error) {
	b := make([]byte, 4)
	// to be written?
}

func (p *PointCodeWithMask) MarshalTo(b []bytes) error {
	// to be written?
}

func (p *PointCodeWithMask) Parse(b []bytes) (*PointCodeWithMask, error) {
	// to be written?
}

func (p *PointCodeWithMask) UnmarshalBinary(b []bytes) error {
	// to be written?
}
*/
