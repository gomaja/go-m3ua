// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

// ssnmScopeResolution freezes configuration before entering the store lock.
// Omitted parameters inherit configuration (RFC 4666 §§3.4.1, 4.3.4.3).
// An unresolved omitted RC on a multi-AS association is deliberately global.
type ssnmScopeResolution struct {
	configured []ASKey
	implicit   []ASKey
	unresolved bool
}

func (c *Association) ssnmScopeResolution() ssnmScopeResolution {
	configured := c.configuredASKeys()
	implicit := c.ssnmASKeys(WireScope{})
	return ssnmScopeResolution{configured: configured, implicit: implicit,
		unresolved: len(configured) > 1 && !c.hasConfiguredLocalContextlessAS()}
}

func (r ssnmScopeResolution) resolve(scope WireScope) ([]ASKey, bool) {
	if !scope.RoutingContextSet {
		if r.unresolved {
			return nil, false
		}
		keys := append([]ASKey(nil), r.implicit...)
		for i := range keys {
			if scope.NetworkAppearanceSet {
				keys[i].NetworkAppearance, keys[i].NetworkAppearanceSet = scope.NetworkAppearance, true
			}
		}
		return keys, len(keys) != 0
	}
	keys := make([]ASKey, 0, len(scope.RoutingContexts))
	for _, rc := range scope.RoutingContexts {
		key := ASKey{RoutingContext: rc, RoutingContextSet: true, NetworkAppearance: scope.NetworkAppearance, NetworkAppearanceSet: scope.NetworkAppearanceSet}
		if !scope.NetworkAppearanceSet {
			for _, configured := range r.configured {
				if configured.RoutingContextSet && configured.RoutingContext == rc {
					key = configured
					break
				}
			}
		}
		keys = append(keys, key)
	}
	return keys, len(keys) != 0
}

// ssnmDestinationRefusal holds only the incoming report's resolved scopes.
// Retained records carry their own immutable admission-time resolution.
type ssnmDestinationRefusal struct {
	work        *ssnmWorkCount
	forget      receivedForgetFence
	query       bool
	keys        []ASKey
	byPartition map[SSNMPartition][]ASKey
	resolved    bool
}

func (c *Association) captureSSNMDestinationRefusal(scope WireScope, query bool) ssnmDestinationRefusal {
	refusal := ssnmDestinationRefusal{query: query}
	if query {
		return refusal
	}
	refusal.keys, refusal.resolved = c.ssnmScopeResolution().resolve(scope)
	refusal.byPartition = make(map[SSNMPartition][]ASKey)
	for _, key := range refusal.keys {
		partition := c.ssnmPartitionFor(key)
		refusal.byPartition[partition] = append(refusal.byPartition[partition], key)
	}
	return refusal
}

func (r ssnmDestinationRefusal) matches(partition SSNMPartition, scope WireScope, retained []ASKey) bool {
	if !r.resolved {
		return true
	}
	affected := r.keys
	if r.byPartition != nil {
		affected = r.byPartition[partition]
	}
	// Direct store inputs without an admission resolution remain uncertain.
	// Exact wire NA/RC values can still prove disjointness; omission cannot.
	if len(retained) == 0 {
		if !scope.RoutingContextSet {
			for _, key := range affected {
				if !key.NetworkAppearanceSet || !scope.NetworkAppearanceSet || key.NetworkAppearance == scope.NetworkAppearance {
					return true
				}
			}
			return false
		}
		retained, _ = (ssnmScopeResolution{}).resolve(scope)
	}
	for _, key := range affected {
		for _, held := range retained {
			r.work.visitAS()
			if partition.Kind == SSNMStandalonePartition && (key.RoutingContextSet != held.RoutingContextSet || key.RoutingContext != held.RoutingContext) {
				continue
			}
			// Canonical ownership identifies the AS across sibling RC labels
			// (RFC 4666 §§1.2, 1.4.2.1). Network overlap remains independent.
			if !key.NetworkAppearanceSet || !held.NetworkAppearanceSet || key.NetworkAppearance == held.NetworkAppearance {
				return true
			}
		}
	}
	return false
}

// ssnmPartitionsForResolution uses the same captured resolution for ownership
// and overlap. An unresolved report keeps the existing conservative fallback.
func (c *Association) ssnmPartitionsForResolution(scope WireScope, resolution ssnmDestinationRefusal) []SSNMPartition {
	if !resolution.resolved {
		return c.ssnmPartitionsForScope(scope)
	}
	partitions := make([]SSNMPartition, 0, len(resolution.byPartition))
	for partition := range resolution.byPartition {
		partitions = append(partitions, partition)
	}
	sortSSNMPartitions(partitions)
	return partitions
}
