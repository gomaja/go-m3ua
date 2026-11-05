// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"fmt"
	"testing"

	"github.com/gomaja/go-sctp"
)

func TestSCTPReliableReadSocketConfigs(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		policy := oneShotSCTPDialPolicy(DefaultInitTimeout)
		policy.partialReliability = enabled
		for name, cfg := range map[string]*sctp.Config{
			"dial":   policy.socketConfig(&restartWatcher{}),
			"listen": sctpListenConfig(socketBuffers{}, nil, enabled, &restartWatcher{}),
		} {
			t.Run(fmt.Sprintf("%s/%t", name, enabled), func(t *testing.T) {
				if cfg.MessageInterleaving {
					t.Fatalf("MessageInterleaving = %v; ReadMsg requires the socket default without I-DATA", cfg.MessageInterleaving)
				}
				if cfg.FragmentInterleave != nil {
					t.Fatalf("FragmentInterleave = %v, want the kernel default", cfg.FragmentInterleave)
				}
				if cfg.PartialReliability == nil || *cfg.PartialReliability != enabled {
					t.Fatalf("PartialReliability = %v; want explicit %t", cfg.PartialReliability, enabled)
				}
				if cfg.DefaultPrInfo != nil {
					t.Fatalf("DefaultPrInfo = %v; M3UA sends must remain reliable", cfg.DefaultPrInfo)
				}
			})
		}
	}
}
