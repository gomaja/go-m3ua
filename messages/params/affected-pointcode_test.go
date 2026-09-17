// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package params

import "testing"

// A receiver bounds an SSNM message's Affected Point Code list before it pays
// to expand it, so the count has to be available from the encoded value and
// has to agree with what the accessors would decode.
func TestAffectedPointCodeCount(t *testing.T) {
	for _, tt := range []struct {
		name  string
		param *Param
		want  int
	}{
		{"nil parameter", nil, 0},
		{"another parameter", NewRoutingContext(1, 2, 3), 0},
		{"one point code", NewAffectedPointCode(0x123456), 1},
		{"three point codes", NewAffectedPointCode(0x123456, 0x123457, 0x123458), 3},
		{"one masked range", NewAffectedPointCodeWithMask(8, 0x123400), 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.param.AffectedPointCodeCount(); got != tt.want {
				t.Fatalf("count = %d, want %d", got, tt.want)
			}
			if tt.param == nil {
				return
			}
			if got := len(tt.param.AffectedPointCodes()); got != tt.want {
				t.Fatalf("count %d disagrees with the %d decoded point codes", tt.want, got)
			}
		})
	}
}

// A value that is not a whole number of 32-bit words decodes to no point code,
// and the count has to report that rather than round up into it.
func TestAffectedPointCodeCountRejectsARaggedValue(t *testing.T) {
	ragged := NewAffectedPointCode(0x123456, 0x123457)
	ragged.Data = ragged.Data[:5]
	if got := ragged.AffectedPointCodeCount(); got != 0 {
		t.Fatalf("count = %d, want 0", got)
	}
	if got := len(ragged.AffectedPointCodes()); got != 0 {
		t.Fatalf("decoded %d point codes from a ragged value", got)
	}
}
