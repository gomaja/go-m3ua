// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"errors"
	"testing"
)

func TestSCTPPartialReliabilitySelection(t *testing.T) {
	for _, offered := range []bool{false, true} {
		for _, selected := range []bool{false, true} {
			for _, selector := range []bool{false, true} {
				cfg := NewAssociationConfig()
				cfg.PartialReliability = offered
				lc := NewListenerConfig(cfg)
				if selector {
					lc.SelectAssociationConfig = func(AcceptInfo) (*AssociationConfig, error) { return cfg, nil }
				}
				l := newListener(nil, lc)
				cfg.PartialReliability = !offered
				l.PartialReliability = !offered
				chosen := NewAssociationConfig()
				chosen.PartialReliability = selected
				err := l.validateSelectedPartialReliability(chosen)
				if want := selector && offered != selected; errors.Is(err, ErrInvalidSCTPConfig) != want {
					t.Fatalf("offered=%t selected=%t selector=%t error=%v; want invalid=%t", offered, selected, selector, err, want)
				}
			}
		}
	}
}

func TestSCTPPRSupportedWithoutAssociation(t *testing.T) {
	a := newAssociation(RoleASP, NewAssociationConfig())
	if got, err := a.SCTPPRSupported(); got || !errors.Is(err, ErrAssociationClosed) {
		t.Fatalf("PRSupported without socket = %t, %v", got, err)
	}
}
