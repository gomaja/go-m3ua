// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-pointcode"
)

// These are receive-queue engineering defaults, not RFC-assigned values.
// Capacity is association-wide; the scopes identify discarded traffic, not
// independently measured per-AS queues. Retain at most 32 scopes per episode.
const (
	dataOverflowScopeLimit    = 32
	dataOverflowDrainDivisor  = 2 // at or below 50% of message capacity
	dataOverflowDrainDwell    = time.Second
	dataOverflowCheckInterval = 100 * time.Millisecond
)

// DataQueueCongestionIndication reports a local receive-queue transition to
// Layer Management, independently of whether an overflow SCON reaches the peer.
// This is local queue evidence under RFC 4666 Section 1.4.6, not SS7 network
// congestion and not an independently measured queue for the named AS.
type DataQueueCongestionIndication struct {
	// Scope is the exact received NA/RC, including omitted versus explicit zero.
	Scope WireScope
	// AS is the resolved Application Server of the discarded DATA.
	AS ASKey
	// DestinationPointCode is the discarded DATA's DPC, the affected destination.
	DestinationPointCode uint32
	// OriginatingPointCode is the discarded DATA's OPC.
	OriginatingPointCode uint32
	// NetworkIndicator is the discarded DATA's NI.
	NetworkIndicator pointcode.NetworkIndicator
	// Congested is true on onset and false after a sustained drain.
	Congested bool
	// TrackingLost marks the first untracked discard when the 32-scope episode
	// limit is exceeded. No per-scope end is promised for untracked traffic.
	// DataQueueStats.ScopeTrackingLosses counts every such discard.
	TrackingLost bool
}

type dataOverflowKey struct {
	networkAppearance, routingContext, dpc, opc uint32
	networkAppearanceSet, routingContextSet     bool
}

type queueCongestionBinding struct {
	key     ASKey
	version uint64
	epoch   uint64
	local   bool
}

type dataOverflowScope struct {
	DataQueueCongestionIndication
	binding             queueCongestionBinding
	lifetime            *localQueueLifetime
	episode             *localQueueEpisode
	knowledgeGeneration uint64
	lifetimeRevision    uint64
	generation          uint64
}

func (c *Association) queueCongestionBinding(key ASKey, scope WireScope) queueCongestionBinding {
	if !key.RoutingContextSet && scope.RoutingContextSet && len(scope.RoutingContexts) == 1 {
		key.RoutingContext, key.RoutingContextSet = scope.RoutingContexts[0], true
	}
	local := c.isIPSPDoubleExchange()
	key = normalizeOutboundASKey(key)
	// DATA preserves its received wire appearance separately. An all-appearance
	// static AS still owns one local binding, rather than a new AS per wire NA
	// (RFC 4666 §§3.3.1, 3.6.1). Exact configured ASs take precedence.
	if key.NetworkAppearanceSet {
		var wildcard ASKey
		hasWildcard, exact := false, false
		var owner ASKey
		owners := 0
		for _, configured := range c.configuredLocalASKeysForStatus() {
			if configured.RoutingContextSet == key.RoutingContextSet && configured.RoutingContext == key.RoutingContext {
				owner = configured
				owners++
			}
			if configured == key {
				exact = true
				break
			}
			if configured.RoutingContextSet == key.RoutingContextSet && configured.RoutingContext == key.RoutingContext && !configured.NetworkAppearanceSet {
				wildcard, hasWildcard = configured, true
			}
		}
		if !exact && hasWildcard {
			key = wildcard
		} else if !exact && c.role == RoleASP && owners == 1 {
			// The ASP receive policy permits an active foreign wire appearance. Its
			// configured RC owner and immutable method still govern local evidence.
			key = owner
		}
	}
	version := uint64(0)
	c.muDynamicASKeys.RLock()
	keys, versions := c.dynamicPeerASKeys, c.dynamicPeerASKeyVersions
	if local {
		keys, versions = c.dynamicLocalASKeys, c.dynamicLocalASKeyVersions
	}
	if key.RoutingContextSet {
		if dynamic, ok := keys[key.RoutingContext]; ok {
			key, version = dynamic, versions[key.RoutingContext]
		}
	}
	c.muDynamicASKeys.RUnlock()
	return queueCongestionBinding{key: key, version: version, epoch: c.Epoch(), local: local}
}

// Called with muDynamicASKeys held for this short check only.
func (c *Association) queueCongestionBindingCurrent(binding queueCongestionBinding) bool {
	if binding.epoch != c.Epoch() {
		return false
	}
	keys, versions := c.dynamicPeerASKeys, c.dynamicPeerASKeyVersions
	if binding.local {
		keys, versions = c.dynamicLocalASKeys, c.dynamicLocalASKeyVersions
	}
	if !binding.key.RoutingContextSet {
		return true // Contextless provisioned bindings are immutable.
	}
	return versions[binding.key.RoutingContext] == binding.version &&
		(binding.version == 0 || keys[binding.key.RoutingContext] == binding.key)
}

func (c *Association) sconPublicationBindings(message *messages.SignallingCongestion) []queueCongestionBinding {
	keys := c.congestionSCONKeys(message.NetworkAppearance, message.RoutingContext, c.isIPSPDoubleExchange())
	bindings := make([]queueCongestionBinding, 0, len(keys))
	for _, key := range keys {
		bindings = append(bindings, c.queueCongestionBinding(key, WireScope{}))
	}
	return bindings
}

func overflowKey(s WireScope, dpc, opc uint32) dataOverflowKey {
	key := dataOverflowKey{networkAppearance: s.NetworkAppearance, networkAppearanceSet: s.NetworkAppearanceSet,
		routingContextSet: s.RoutingContextSet, dpc: dpc, opc: opc}
	if s.RoutingContextSet && len(s.RoutingContexts) == 1 {
		key.routingContext = s.RoutingContexts[0]
	}
	return key
}

func (c *Association) recordDataOverflow(msg *DataMessage) *DataQueueOverflowError {
	binding := c.queueCongestionBinding(msg.AS, msg.Scope)
	lifetimeRevision := c.localQueueLifetimeRevision.Load()
	lifetime := c.captureQueueCongestionLifetime(binding)
	knowledge := c.sconSubmissions.knowledgeGeneration.Load()
	c.muDataOverflowEvents.Lock()
	defer c.muDataOverflowEvents.Unlock()
	c.muDataOverflow.Lock()
	overflow, indication, start := c.recordDataOverflowLocked(msg, binding, lifetime, knowledge, lifetimeRevision)
	c.muDataOverflow.Unlock()
	if start {
		go c.watchDataOverflow()
	}
	if indication != nil {
		if !indication.TrackingLost {
			c.muDataOverflow.Lock()
			scope := c.dataOverflowScopes[overflowKey(msg.Scope, msg.ProtocolData.DestinationPointCode, msg.ProtocolData.OriginatingPointCode)]
			c.muDataOverflow.Unlock()
			c.reportQueueLocalCongestion(scope)
			if c.endpoint != nil {
				c.endpoint.ensureCongestionAutomation()
			}
		}
		c.notifyDataQueueCongestion(*indication)
	}
	return overflow
}

func (c *Association) recordDataOverflowLocked(msg *DataMessage, binding queueCongestionBinding, lifetime *localQueueLifetime, knowledge, lifetimeRevision uint64) (*DataQueueOverflowError, *DataQueueCongestionIndication, bool) {
	if !c.dataOverflow.Load() {
		c.dataOverflowGeneration++
		c.dataOverflowEpisode = &localQueueEpisode{}
		c.dataOverflowEpisode.live.Store(true)
		c.dataOverflowSeenRefill = c.dataOverflowRefills.Add(1)
	}
	c.dataOverflow.Store(true)
	// Every discard interrupts a low-occupancy dwell, even if a concurrent
	// consumer has drained the queue before this observation.
	c.dataOverflowLowSince = time.Time{}
	key := overflowKey(msg.Scope, msg.ProtocolData.DestinationPointCode, msg.ProtocolData.OriginatingPointCode)
	previous, exists := c.dataOverflowScopes[key]
	if exists && previous.binding == binding && previous.lifetime == lifetime && previous.knowledgeGeneration == knowledge && (lifetime != nil || previous.lifetimeRevision == lifetimeRevision) {
		return nil, nil, false
	}
	if !exists && len(c.dataOverflowScopes) == dataOverflowScopeLimit {
		c.dataScopeTrackingLosses.Add(1)
		if !c.dataOverflowTrackingLost {
			c.dataOverflowTrackingLost = true
			ind := congestionFromData(msg)
			ind.TrackingLost = true
			return nil, &ind, false
		}
		return nil, nil, false
	}
	if c.dataOverflowScopes == nil {
		c.dataOverflowScopes = make(map[dataOverflowKey]dataOverflowScope)
	}
	ind := congestionFromData(msg)
	c.dataOverflowScopes[key] = dataOverflowScope{DataQueueCongestionIndication: ind, binding: binding, lifetime: lifetime, episode: c.dataOverflowEpisode, knowledgeGeneration: knowledge, lifetimeRevision: lifetimeRevision, generation: c.dataOverflowGeneration}
	start := !c.dataOverflowWorker
	c.dataOverflowWorker = true
	return &DataQueueOverflowError{generation: c.dataOverflowGeneration, binding: binding, Scope: msg.Scope.clone(),
		DestinationPointCode: ind.DestinationPointCode, OriginatingPointCode: ind.OriginatingPointCode,
		NetworkIndicator: ind.NetworkIndicator}, &ind, start
}

func congestionFromData(msg *DataMessage) DataQueueCongestionIndication {
	return DataQueueCongestionIndication{Scope: msg.Scope.clone(), AS: msg.AS,
		DestinationPointCode: msg.ProtocolData.DestinationPointCode,
		OriginatingPointCode: msg.ProtocolData.OriginatingPointCode,
		NetworkIndicator:     msg.ProtocolData.NetworkIndicator, Congested: true}
}

func (c *Association) notifyDataQueueCongestion(ind DataQueueCongestionIndication) {
	c.notifyManagement(&ManagementIndication{Kind: ManagementDataQueueCongestion,
		DataQueueCongestion: &ind, Description: "inbound DATA queue congestion"})
}

// recordDataQueueRefill retains an actual send-side high-water observation.
// Revision changes precede the tracker wait; end uses CAS so it cannot pass a
// concurrently recorded refill. The even revision excludes an already ended
// episode, including the brief interval before its active flag is cleared.
func (c *Association) recordDataQueueRefill() {
	for {
		revision := c.dataOverflowRefills.Load()
		if revision&1 == 0 {
			return
		}
		if c.dataOverflowRefills.CompareAndSwap(revision, revision+2) {
			return
		}
	}
}

func (c *Association) observeDataQueue() {
	if !c.dataOverflow.Load() {
		return
	}
	c.muDataOverflow.Lock()
	now, queued := time.Now(), len(c.dataChan)
	// Routine observations do not wait for a management publisher. Only an
	// end transition needs the ordering gate, acquired without the tracker lock.
	if queued > cap(c.dataChan)/dataOverflowDrainDivisor || c.dataOverflowLowSince.IsZero() || now.Sub(c.dataOverflowLowSince) < dataOverflowDrainDwell {
		c.observeDataQueueLocked(now, queued)
		c.muDataOverflow.Unlock()
		return
	}
	c.muDataOverflow.Unlock()
	if !c.muDataOverflowEvents.TryLock() {
		return
	}
	defer c.muDataOverflowEvents.Unlock()
	c.muDataOverflow.Lock()
	// Recheck occupancy and dwell after admission: another discard/refill may
	// have cancelled the candidate while the ordering gate was unavailable.
	ended := c.observeDataQueueLocked(time.Now(), len(c.dataChan))
	c.muDataOverflow.Unlock()
	for _, ind := range ended {
		c.clearQueueLocalCongestion(ind)
		c.notifyDataQueueCongestion(ind.DataQueueCongestionIndication)
	}
}

func (c *Association) observeDataQueueLocked(now time.Time, queued int) []dataOverflowScope {
	if !c.dataOverflow.Load() {
		return nil
	}
	revision := c.dataOverflowRefills.Load()
	if revision != c.dataOverflowSeenRefill {
		c.dataOverflowSeenRefill = revision
		c.dataOverflowLowSince = time.Time{}
	}
	if queued > cap(c.dataChan)/dataOverflowDrainDivisor {
		c.dataOverflowLowSince = time.Time{}
		return nil
	}
	if c.dataOverflowLowSince.IsZero() {
		c.dataOverflowLowSince = now
		return nil
	}
	if now.Sub(c.dataOverflowLowSince) < dataOverflowDrainDwell {
		return nil
	}
	// A refill can arrive after the sample above without taking the tracker.
	// Commit end only if that sample's revision is still current.
	if !c.dataOverflowRefills.CompareAndSwap(revision, revision+1) {
		c.dataOverflowLowSince = time.Time{}
		return nil
	}
	if c.dataOverflowEpisode != nil {
		c.beginLocalInputChange()
		c.dataOverflowEpisode.live.Store(false)
		c.finishLocalInputChange()
	}
	c.dataOverflow.Store(false)
	ended := make([]dataOverflowScope, 0, len(c.dataOverflowScopes))
	for _, ind := range c.dataOverflowScopes {
		ind.Congested = false
		ended = append(ended, ind)
	}
	clear(c.dataOverflowScopes)
	c.dataOverflowTrackingLost = false
	c.dataOverflowLowSince = time.Time{}
	// RFC 4666 Section 3.4.4: the international method omits Congestion
	// Indications, so it cannot encode a level-zero clear. Local recovery sends
	// nothing in that method; national end publication runs outside this lock.
	// The peer otherwise needs its validity timer or audit/current MTP procedure.
	return ended
}

func (c *Association) watchDataOverflow() {
	ticker := time.NewTicker(dataOverflowCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.done:
			c.muDataOverflow.Lock()
			c.dataOverflowWorker = false
			c.muDataOverflow.Unlock()
			return
		case <-ticker.C:
			c.observeDataQueue()
			c.muDataOverflow.Lock()
			if !c.dataOverflow.Load() {
				c.dataOverflowWorker = false
				c.muDataOverflow.Unlock()
				return
			}
			c.muDataOverflow.Unlock()
			// Repair at most the fixed episode scope limit using the existing
			// watcher. Only the common worker submits optional transport.
			c.reconcileQueueCongestionLifetimes()
			c.reconcileQueueLocalCongestion()
		}
	}
}

// Q.704 (07/96) §11.2.6 leaves local overload detection implementation dependent.
// Mapping actual queue discard to MaxLevel is an engineering choice. This
// contribution never changes received SS7 SCON or outbound transport pressure.
func (c *Association) overflowCongestionProfile(scope WireScope) CongestionProfile {
	keys := c.congestionSCONKeys(paramAppearance(scope), paramContexts(scope), c.isIPSPDoubleExchange())
	return c.congestionProfile(keys[0])
}
