// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import "testing"

func TestSCTPNagleConfig(t *testing.T) {
	for _, cfg := range []*AssociationConfig{{}, NewAssociationConfig()} {
		snapshot := snapshotAssociationConfig(cfg)
		if snapshot.Nagle {
			t.Fatal("zero configuration enables Nagle")
		}
		for _, enabled := range []bool{true, false} {
			if got := cfg.SetSCTPNagle(enabled); got != cfg {
				t.Fatal("setter did not return its receiver")
			}
			if cfg.Nagle != enabled {
				t.Fatalf("Nagle = %t, want %t", cfg.Nagle, enabled)
			}
			snapshot := snapshotAssociationConfig(cfg)
			cfg.Nagle = !enabled
			if snapshot.Nagle != enabled {
				t.Fatalf("snapshot Nagle = %t, want %t", snapshot.Nagle, enabled)
			}
		}
	}
}

func TestSCTPNagleSelectedConfig(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		selected := NewAssociationConfig().SetSCTPNagle(enabled)
		lc := NewListenerConfig(NewAssociationConfig().SetSCTPNagle(!enabled))
		lc.SelectAssociationConfig = func(AcceptInfo) (*AssociationConfig, error) { return selected, nil }
		snapshot, err := lc.associationConfigForAccept(AcceptInfo{})
		if err != nil {
			t.Fatal(err)
		}
		selected.Nagle = !enabled
		if snapshot.Nagle != enabled {
			t.Fatalf("selected snapshot Nagle = %t, want %t", snapshot.Nagle, enabled)
		}
	}
}
