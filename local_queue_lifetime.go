package m3ua

import "sync/atomic"

// A captured queue trigger belongs to one active local binding lifetime. A
// retired token never becomes live again, including static-AS reactivation.
type localQueueEpisode struct{ live atomic.Bool }

type localQueueLifetime struct {
	binding queueCongestionBinding
	live    atomic.Bool
}

type localQueueLifetimeIndex struct {
	revision uint64
	bindings map[ASKey]*localQueueLifetime
}

// Capture uses immutable current configuration only. An index rebuilt across a
// concurrent mark is not admission evidence; existing unrelated tokens remain
// valid because retirement changes their live bit only for the named scope.
func (c *Association) captureQueueCongestionLifetime(binding queueCongestionBinding) *localQueueLifetime {
	if c == nil || c.endpoint == nil || c.ID() == 0 || c.terminating.Load() {
		return nil
	}
	select {
	case <-c.done:
		return nil
	default:
	}
	index := c.localQueueLifetimes.Load()
	if index == nil || index.revision != c.localQueueLifetimeRevision.Load() {
		return nil
	}
	token := index.bindings[binding.key]
	if token == nil || token.binding != binding || !token.live.Load() {
		return nil
	}
	return token
}

// Call at the scope mark, without acquiring overflow, delivery, transport or
// store locks. Its callers supply internal scope predicates only. The current
// immutable table contains configured ASs, never retired registration history.
func (c *Association) retireQueueCongestionLifetimes(matches func(ASKey) bool) {
	if c == nil {
		return
	}
	c.beginLocalInputChange()
	defer c.finishLocalInputChange()
	c.localQueueLifetimeRevision.Add(1)
	if index := c.localQueueLifetimes.Load(); index != nil {
		for key, token := range index.bindings {
			if matches == nil || matches(key) {
				token.live.Store(false)
			}
		}
	}
}

// The caller owns muState and muAckedRCs for this bounded classification.
func (c *Association) localCongestionScopeActiveLocked(binding queueCongestionBinding) bool {
	state := c.state
	if binding.local {
		state = c.localIPSPState
	}
	if state != StateASPActive || c.terminating.Load() {
		return false
	}
	select {
	case <-c.done:
		return false
	default:
	}
	key := binding.key
	if c.role == RoleSGP || c.role == RoleIPSP && !c.isIPSPDoubleExchange() {
		if !key.RoutingContextSet {
			return !c.activeRCsScoped || c.contextlessASActive
		}
		if !c.activeRCsScoped {
			_, inactive := c.inactiveDynamicRCs[key.RoutingContext]
			return !inactive
		}
		_, active := c.activeRCs[key.RoutingContext]
		return active
	}
	if !key.RoutingContextSet {
		return !c.ackedRCsScoped || c.ackedContextlessAS
	}
	_, acked := c.ackedRCs[key.RoutingContext]
	_, overridden := c.overriddenRCs[key.RoutingContext]
	return (!c.ackedRCsScoped || acked) && !overridden
}

// Rebuilds serialize only with each other. All state/ledger/AS acquisitions are
// try operations: a blocked state entry cannot make the receive path wait for
// its send deadline. A later worker pass retries a missed reconciliation.
func (c *Association) reconcileQueueCongestionLifetimes() {
	if c == nil || c.endpoint == nil || c.ID() == 0 || !c.localQueueLifetimeMu.TryLock() {
		return
	}
	defer c.localQueueLifetimeMu.Unlock()
	revision := c.localQueueLifetimeRevision.Load()
	keys := c.configuredLocalASKeysForStatus()
	bindings := make([]queueCongestionBinding, 0, len(keys))
	servers := make(map[ASKey]*applicationServer)
	for _, key := range keys {
		binding := c.queueCongestionBinding(key, WireScope{})
		bindings = append(bindings, binding)
		if c.role == RoleSGP || c.role == RoleIPSP && !c.isIPSPDoubleExchange() {
			server, _, acquired := c.as.tryLookupOptionalAS(binding.key)
			if !acquired {
				return
			}
			servers[binding.key] = server
		}
	}
	if !c.muState.TryRLock() {
		return
	}
	defer c.muState.RUnlock()
	if !c.muAckedRCs.TryRLock() {
		return
	}
	defer c.muAckedRCs.RUnlock()
	old := c.localQueueLifetimes.Load()
	next := &localQueueLifetimeIndex{revision: revision, bindings: make(map[ASKey]*localQueueLifetime, len(bindings))}
	for _, binding := range bindings {
		if !c.localCongestionScopeActiveLocked(binding) {
			continue
		}
		if c.role == RoleSGP || c.role == RoleIPSP && !c.isIPSPDoubleExchange() {
			server := servers[binding.key]
			if server == nil || !server.mu.TryLock() {
				return
			}
			active := !server.closed && server.asps[c] == StateASPActive
			server.mu.Unlock()
			if !active {
				continue
			}
		}
		var token *localQueueLifetime
		if old != nil {
			token = old.bindings[binding.key]
		}
		if token == nil || token.binding != binding || !token.live.Load() {
			token = &localQueueLifetime{binding: binding}
			token.live.Store(true)
		}
		next.bindings[binding.key] = token
	}
	if c.localQueueLifetimeRevision.Load() != revision {
		return
	}
	c.localQueueLifetimes.Store(next)
}
