// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"testing"

	"github.com/gomaja/go-sctp"
)

func TestSCTPReliableReadSocketConfigs(t *testing.T) {
	policy := oneShotSCTPDialPolicy(DefaultInitTimeout)
	for name, cfg := range map[string]*sctp.Config{
		"dial":   policy.socketConfig(&restartWatcher{}),
		"listen": sctpListenConfig(socketBuffers{}, nil, &restartWatcher{}),
	} {
		t.Run(name, func(t *testing.T) {
			if cfg.MessageInterleaving {
				t.Fatalf("MessageInterleaving = %v; ReadMsg requires the socket default without I-DATA", cfg.MessageInterleaving)
			}
			if cfg.FragmentInterleave != nil {
				t.Fatalf("FragmentInterleave = %v, want the kernel default", cfg.FragmentInterleave)
			}
			if cfg.PartialReliability != nil {
				t.Fatalf("PartialReliability = %v; negotiation must retain the kernel default", cfg.PartialReliability)
			}
			if cfg.DefaultPrInfo != nil {
				t.Fatalf("DefaultPrInfo = %v; M3UA sends must remain reliable", cfg.DefaultPrInfo)
			}
		})
	}
}
