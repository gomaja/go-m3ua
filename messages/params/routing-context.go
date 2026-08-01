// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package params

// NewRoutingContext creates the RoutingContext Parameter.
// Multiple number of RoutingContext will be accepted.
// Note that this returns *Param, as no specific structure in this parameter.
func NewRoutingContext(rtCxts ...uint32) *Param {
	return newMultiUint32ValParam(RoutingContext, rtCxts...)
}

// RoutingContext returns single RoutingContext from Param.
//
// The value is carried as a sequence of 32-bit words, so a peer can send a
// payload whose length is not a multiple of four, or is empty; the decoder
// yields nothing in that case and there is no context to return.
func (p *Param) RoutingContext() uint32 {
	if p == nil || p.Tag != RoutingContext {
		return 0
	}

	rcs := p.RoutingContexts()
	if len(rcs) == 0 {
		return 0
	}

	return rcs[0]
}

// RoutingContexts returns multiple RoutingContexts from Param.
//
// A nil Param has no contexts rather than being a programming error: the
// parameter is Conditional or Optional on most messages and a Config may leave
// it unset, so callers hold nil routinely. Copy takes the same view.
func (p *Param) RoutingContexts() []uint32 {
	if p == nil || p.Tag != RoutingContext {
		return nil
	}

	return p.decodeMultiUint32ValData()
}
