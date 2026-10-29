// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"errors"
	"testing"
	"time"

	"github.com/gomaja/go-sctp"
)

func TestSCTPRTOConfigSnapshot(t *testing.T) {
	cfg := NewAssociationConfig()
	want := sctp.RTOInfo{Initial: 1500 * time.Millisecond, Min: 300 * time.Millisecond, Max: 2500 * time.Millisecond}
	cfg.RTOInfo = &want
	snapshot := snapshotAssociationConfig(cfg)
	cfg.RTOInfo.Initial = time.Second
	if snapshot.RTOInfo == cfg.RTOInfo || snapshot.RTOInfo.Initial != 1500*time.Millisecond {
		t.Fatalf("snapshot aliases caller: %+v", snapshot.RTOInfo)
	}
	if snapshotAssociationConfig(NewAssociationConfig()).RTOInfo != nil {
		t.Fatal("nil RTOInfo changed")
	}
}

func TestSCTPRTOSelectionComparison(t *testing.T) {
	base := sctp.RTOInfo{Initial: 2200 * time.Millisecond, Min: 400 * time.Millisecond, Max: 3500 * time.Millisecond}
	for _, tt := range []struct {
		name  string
		a, b  *sctp.RTOInfo
		equal bool
	}{
		{"nil", nil, nil, true}, {"zero", nil, &sctp.RTOInfo{}, true}, {"reverse zero", &sctp.RTOInfo{}, nil, true},
		{"equal", &base, &sctp.RTOInfo{Initial: 2200 * time.Millisecond, Min: 400 * time.Millisecond, Max: 3500 * time.Millisecond}, true},
		{"initial", &base, &sctp.RTOInfo{Initial: 1500 * time.Millisecond, Min: 400 * time.Millisecond, Max: 3500 * time.Millisecond}, false},
		{"min", &base, &sctp.RTOInfo{Initial: 2200 * time.Millisecond, Min: 300 * time.Millisecond, Max: 3500 * time.Millisecond}, false},
		{"max", &base, &sctp.RTOInfo{Initial: 2200 * time.Millisecond, Min: 400 * time.Millisecond, Max: 2500 * time.Millisecond}, false},
		{"partial", &base, &sctp.RTOInfo{Min: 300 * time.Millisecond}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := equalRTOInfo(tt.a, tt.b); got != tt.equal {
				t.Fatalf("equal=%v want=%v", got, tt.equal)
			}
		})
	}
}

func TestSCTPRTOSocketConfigs(t *testing.T) {
	want := &sctp.RTOInfo{Initial: 1500 * time.Millisecond, Min: 300 * time.Millisecond, Max: 2500 * time.Millisecond}
	p := oneShotSCTPDialPolicy(DefaultInitTimeout)
	p.rtoInfo = want
	if got := p.socketConfig(&restartWatcher{}); got.RTOInfo == nil || *got.RTOInfo != *want || got.SetupRTOInitial != 6*time.Second {
		t.Fatalf("dial config=%+v", got)
	}
	if got := sctpListenConfig(socketBuffers{}, want, &restartWatcher{}); got.RTOInfo == nil || *got.RTOInfo != *want {
		t.Fatalf("listen config=%+v", got)
	}
}

func TestSCTPRTOClosedAssociation(t *testing.T) {
	a := newAssociation(RoleASP, NewAssociationConfig())
	if _, err := a.SCTPRTOInfo(); !errors.Is(err, ErrAssociationClosed) {
		t.Fatal(err)
	}
	if err := a.SetSCTPRTOInfo(nil); !errors.Is(err, ErrAssociationClosed) {
		t.Fatal(err)
	}
	if _, err := a.SCTPAssociationMaxRetrans(); !errors.Is(err, ErrAssociationClosed) {
		t.Fatal(err)
	}
	if err := a.SetSCTPAssociationMaxRetrans(3); !errors.Is(err, ErrAssociationClosed) {
		t.Fatal(err)
	}
}
