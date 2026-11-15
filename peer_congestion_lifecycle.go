// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"errors"
	"slices"
)

type peerRetentionBinding struct {
	epoch, membership, dynamic, token uint64
	server                            *applicationServer
}

type peerCongestionAdmission struct {
	epoch    uint64
	bindings map[ASKey]uint64
}

// withPeerCongestionBindings holds only short bookkeeping locks. The order is
// peerRetentionMu -> ordered AS.mu -> transportMu -> muDynamicASKeys -> SSNM
// store.mu. Registry lookups finish before AS locking. No delivery barrier,
// subscriber, callback, socket query or write is joined by this receive path.
func (c *Association) withPeerCongestionBindings(commit func(uint64, map[ASKey]uint64)) {
	if c == nil || c.role != RoleSGP || c.ssnmStore() == nil {
		return
	}
	c.peerRetentionMu.Lock()
	defer c.peerRetentionMu.Unlock()
	keys := c.configuredASKeys()
	slices.SortFunc(keys, compareASKey)
	servers := make([]*applicationServer, 0, len(keys))
	for _, key := range keys {
		if as, exists := c.as.lookup(key); exists {
			servers = append(servers, as)
		}
	}
	for _, as := range servers {
		as.mu.Lock()
	}
	defer func() {
		for i := len(servers) - 1; i >= 0; i-- {
			servers[i].mu.Unlock()
		}
	}()
	c.transportMu.Lock()
	defer c.transportMu.Unlock()
	c.muDynamicASKeys.RLock()
	defer c.muDynamicASKeys.RUnlock()
	epoch := c.Epoch()
	next := make(map[ASKey]peerRetentionBinding, len(servers))
	active := make(map[ASKey]uint64, len(servers))
	for _, as := range servers {
		if as.closed || as.asps[c] != StateASPActive || c.terminating.Load() {
			continue
		}
		key := as.key
		version := uint64(0)
		if key.RoutingContextSet {
			if dynamic, exists := c.dynamicPeerASKeys[key.RoutingContext]; exists {
				if dynamic != key {
					continue
				}
				version = c.dynamicPeerASKeyVersions[key.RoutingContext]
			} else if old := c.peerRetentionBindings[key]; old.dynamic != 0 {
				// A removed registration is not an implicit static replacement.
				continue
			}
		}
		binding := c.peerRetentionBindings[key]
		if binding.token == 0 || binding.epoch != epoch || binding.dynamic != version || binding.membership != as.memberGenerations[c] || binding.server != as {
			c.peerRetentionVersion++
			binding = peerRetentionBinding{epoch: epoch, membership: as.memberGenerations[c], dynamic: version, token: c.peerRetentionVersion, server: as}
		}
		next[key] = binding
		active[key] = binding.token
	}
	c.peerRetentionBindings = next
	c.ssnmStore().syncPeerCongestion(c.ID(), epoch, active)
	commit(epoch, active)
}

func (c *Association) syncPeerCongestionBindings() {
	c.withPeerCongestionBindings(func(uint64, map[ASKey]uint64) {})
}

func (c *Association) capturePeerCongestionAdmission(scope WireScope) peerCongestionAdmission {
	keys := c.ssnmASKeys(scope)
	admission := peerCongestionAdmission{bindings: make(map[ASKey]uint64, len(keys))}
	c.withPeerCongestionBindings(func(epoch uint64, active map[ASKey]uint64) {
		admission.epoch = epoch
		for _, key := range keys {
			// An all-appearance registration retains the resolved AS key while
			// its report's exact network remains in WireScope (RFC 4666 §3.6.1).
			if _, held := active[key]; !held && key.RoutingContextSet {
				for candidate := range active {
					if candidate.RoutingContextSet && candidate.RoutingContext == key.RoutingContext && !candidate.NetworkAppearanceSet {
						key = candidate
						break
					}
				}
			}
			if token, held := active[key]; held {
				admission.bindings[key] = token
			}
		}
	})
	return admission
}

func (c *Association) publishPeerCongestionReport(report SSNMReport, admission peerCongestionAdmission) error {
	store := c.ssnmStore()
	if store == nil {
		return nil
	}
	report.Association = c.ID()
	report.Partition = SSNMPartition{Kind: SSNMStandalonePartition, Association: c.ID()}
	var result error
	c.withPeerCongestionBindings(func(epoch uint64, active map[ASKey]uint64) {
		if epoch != admission.epoch || len(admission.bindings) == 0 {
			return
		}
		for key, token := range admission.bindings {
			if current, held := active[key]; !held || current != token {
				return
			}
		}
		if err := store.bindPeerReport(report, admission.bindings); err != nil {
			result = err
			return
		}
		result = store.applyPeerReport(report, admission.epoch, admission.bindings)
	})
	// Withdrawal/restart racing local admission is not a protocol violation.
	if errors.Is(result, ErrAssociationClosed) || errors.Is(result, ErrRoutingContextNotActive) {
		return nil
	}
	return result
}
