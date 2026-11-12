// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"sort"

	"github.com/gomaja/go-m3ua/messages/params"
)

// ssnmStore reports the Endpoint-owned SSNM state store, or nil for an
// Association assembled without an Endpoint.
func (c *Association) ssnmStore() *ssnmState {
	if c == nil || c.endpoint == nil {
		return nil
	}
	return c.endpoint.ssnm
}

// ssnmWireScope reads the exact scope a Section 3.4 message carried. Both
// presence bits come from the parameters themselves: a Network Appearance of
// zero and an empty Routing Context list are legitimate wire values.
func (c *Association) ssnmWireScope(networkAppearance, routingContext *params.Param) WireScope {
	var scope WireScope
	if networkAppearance != nil {
		scope.NetworkAppearance = networkAppearance.NetworkAppearance()
		scope.NetworkAppearanceSet = true
	}
	if routingContext != nil {
		scope.RoutingContexts = append([]uint32(nil), routingContext.RoutingContexts()...)
		scope.RoutingContextSet = true
	}
	return scope
}

// ssnmASKeys resolves one wire scope into the exact Application Server scopes
// it names on this Association.
//
// An omitted parameter is not an absent scope. RFC 4666 Section 3.4 makes
// Network Appearance optional and Routing Context conditional, and Section
// 4.3.4.3 has the receiver of a message without a Routing Context "know, via
// configuration data, which Application Server(s) the ASP is a member", so the
// Association's own configuration supplies what the message left out.
func (c *Association) ssnmASKeys(scope WireScope) []ASKey {
	appearance, appearanceSet := scope.NetworkAppearance, scope.NetworkAppearanceSet
	if !appearanceSet {
		appearance, appearanceSet = c.outboundNetworkAppearance()
	}
	if c.role == RoleASP && !scope.RoutingContextSet && c.hasConfiguredLocalContextlessAS() {
		return []ASKey{{NetworkAppearance: appearance, NetworkAppearanceSet: appearanceSet}}
	}
	routingContexts := scope.RoutingContexts
	routingContextSet := scope.RoutingContextSet
	if !routingContextSet {
		routingContexts, routingContextSet = c.destinationRoutingContexts(nil)
	}
	if !routingContextSet || len(routingContexts) == 0 {
		return []ASKey{{NetworkAppearance: appearance, NetworkAppearanceSet: appearanceSet}}
	}
	keys := make([]ASKey, 0, len(routingContexts))
	seen := make(map[ASKey]struct{}, len(routingContexts))
	for _, routingContext := range routingContexts {
		key := ASKey{
			NetworkAppearance:    appearance,
			NetworkAppearanceSet: appearanceSet,
			RoutingContext:       routingContext,
			RoutingContextSet:    true,
		}
		if !scope.NetworkAppearanceSet {
			key = c.staticASKeyForRoutingContext(routingContext, false)
			if dynamic, ok := c.dynamicASKey(routingContext, false); ok {
				key = dynamic
			}
		}
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	return keys
}

// ssnmPartitionFor resolves one exact wire scope into the canonical identity
// that owns knowledge carried in it.
//
// A Routing Context is "an index into a sending node's Message Distribution
// Table" (RFC 4666 Section 1.4.2.1), so two SGPs of one Signalling Gateway may
// label one Application Server differently, while Section 1.2 has the SGPs of
// one SG "coordinated into a single management view ... to the supported
// Application Servers". The owner is therefore the Signalling Gateway and
// Application Server pair rather than the Association or its label. An
// Association that resolves to no provisioned Application Server owns its
// knowledge alone.
func (c *Association) ssnmPartitionFor(key ASKey) SSNMPartition {
	if id, resolved := c.canonicalRemoteASFor(key); resolved {
		return SSNMPartition{
			Kind:              SSNMCanonicalPartition,
			SignallingGateway: c.cfg.PeerSGP.SignallingGateway,
			ApplicationServer: id,
		}
	}
	return SSNMPartition{Kind: SSNMStandalonePartition, Association: c.ID()}
}

func (c *Association) canonicalRemoteASFor(key ASKey) (RemoteASID, bool) {
	if c == nil || c.cfg == nil || c.cfg.PeerSGP == nil {
		return "", false
	}
	inventory := c.aspPeerInventory()
	if inventory == nil {
		return "", false
	}
	if key.RoutingContextSet {
		if id, bound := c.canonicalRemoteASBinding(key.RoutingContext); bound {
			return id, true
		}
	}
	sgp, provisioned := inventory.config.sgpByIdentity[*c.cfg.PeerSGP]
	if !provisioned {
		return "", false
	}
	id, served := sgp.asByStaticKey[key]
	return id, served
}

// canonicalRemoteASBinding reports the canonical Application Server a dynamic
// registration bound to one Routing Context on this Association.
func (c *Association) canonicalRemoteASBinding(routingContext uint32) (RemoteASID, bool) {
	if c == nil {
		return "", false
	}
	c.muCanonicalAS.RLock()
	defer c.muCanonicalAS.RUnlock()
	id, bound := c.canonicalRemoteAS[routingContext]
	return id, bound
}

func (c *Association) noteCanonicalRemoteAS(routingContext uint32, id RemoteASID) {
	if c == nil || id == "" {
		return
	}
	c.muCanonicalAS.Lock()
	if c.canonicalRemoteAS == nil {
		c.canonicalRemoteAS = make(map[uint32]RemoteASID)
	}
	c.canonicalRemoteAS[routingContext] = id
	c.muCanonicalAS.Unlock()
}

func (c *Association) forgetCanonicalRemoteAS(routingContext uint32) {
	if c == nil {
		return
	}
	c.muCanonicalAS.Lock()
	delete(c.canonicalRemoteAS, routingContext)
	c.muCanonicalAS.Unlock()
}

// ssnmPartitionsForScope resolves the partitions one wire scope concerns, in a
// deterministic order.
func (c *Association) ssnmPartitionsForScope(scope WireScope) []SSNMPartition {
	keys := c.ssnmASKeys(scope)
	partitions := make([]SSNMPartition, 0, len(keys))
	seen := make(map[SSNMPartition]struct{}, len(keys))
	for _, key := range keys {
		partition := c.ssnmPartitionFor(key)
		if _, duplicate := seen[partition]; duplicate {
			continue
		}
		seen[partition] = struct{}{}
		partitions = append(partitions, partition)
	}
	sortSSNMPartitions(partitions)
	return partitions
}

type ssnmBindingScope struct {
	partition SSNMPartition
	pending   bool
}

// ssnmBindingScopes reports the partitions this Association is currently
// entitled to contribute knowledge to.
//
// An acknowledged Routing Context is an active binding. A Routing Context in
// an outstanding ASP Active request is a pending binding: RFC 4666 Section
// 4.5.1 lets the SGP send DUNA, DRST, and SCON "before sending the ASP Active
// Ack that completes the activation procedure", while Section 4.3.4.3 keeps
// the ASP from sending "Data or SSNM messages for the related Routing
// Context(s) before receiving an ASP Active Ack message". The knowledge is
// admitted; the traffic it concerns is not yet authorized.
func (c *Association) ssnmBindingScopes() []ssnmBindingScope {
	if c == nil {
		return nil
	}
	routingContexts := c.configuredRoutingContexts()
	// RFC 4666 §3.4.1 permits omitted RC; §4.5.1 and §4.6 admit early state.
	contextless := c.hasConfiguredLocalContextlessAS() || len(routingContexts) == 0
	pending := make(map[uint32]struct{})
	admitted := make(map[uint32]bool)
	var contextlessAdmitted, contextlessPending bool
	// State entry already uses muState before tack.mu. Keep that order, and
	// release both before partition resolution or publication. No socket write
	// or Endpoint-wide lock belongs to this classification snapshot.
	c.muState.RLock()
	if c.role == RoleASP && c.tack != nil {
		c.tack.mu.Lock()
	}
	active := c.state == StateASPActive
	legalWindow := c.state == StateASPInactive || active
	pendingContextless := false
	if c.role == RoleASP && c.tack != nil && legalWindow {
		for _, request := range c.tack.pending {
			if request.kind != requestAspActive {
				continue
			}
			pendingContextless = pendingContextless || request.routingContextOmitted
			for rc := range request.remaining {
				pending[rc] = struct{}{}
			}
		}
	}
	if contextless {
		switch {
		case active && c.contextlessASAcked():
			contextlessAdmitted = true
		case c.role == RoleASP && pendingContextless:
			contextlessAdmitted, contextlessPending = true, true
		case c.role != RoleASP && active:
			contextlessAdmitted, contextlessPending = true, true
		}
	}
	for _, rc := range routingContexts {
		_, awaiting := pending[rc]
		if c.role == RoleASP {
			switch {
			case active && c.routingContextAcked(rc) && !c.routingContextOverridden(rc):
				admitted[rc] = false
			case awaiting:
				admitted[rc] = true
			}
		} else if active && c.activeForRoutingContext(rc) {
			admitted[rc] = false
		}
	}
	if c.role == RoleASP && c.tack != nil {
		c.tack.mu.Unlock()
	}
	c.muState.RUnlock()

	scopes := make([]ssnmBindingScope, 0, len(admitted)+1)
	seen := make(map[SSNMPartition]int)
	appendScope := func(key ASKey, pending bool) {
		partition := c.ssnmPartitionFor(key)
		if index, exists := seen[partition]; exists {
			if !pending {
				scopes[index].pending = false
			}
			return
		}
		seen[partition] = len(scopes)
		scopes = append(scopes, ssnmBindingScope{partition: partition, pending: pending})
	}
	if contextlessAdmitted {
		appendScope(contextlessASKeyForConfig(c.cfg), contextlessPending)
	}
	for rc, pending := range admitted {
		appendScope(asKeyForConfigRoutingContext(c.cfg, rc), pending)
	}
	sort.Slice(scopes, func(i, j int) bool { return lessSSNMPartition(scopes[i].partition, scopes[j].partition) })
	return scopes
}

// syncSSNMBindings reconciles the store with the Association's current
// entitlement. It is idempotent, so every transition that can change that
// entitlement may call it without tracking what changed.
func (c *Association) syncSSNMBindings() {
	store := c.ssnmStore()
	if store == nil {
		return
	}
	scopes := c.ssnmBindingScopes()
	held := make(map[SSNMPartition]struct{}, len(scopes))
	for _, scope := range scopes {
		held[scope.partition] = struct{}{}
		_ = store.bind(scope.partition, c.ID(), scope.pending)
	}
	for _, partition := range store.partitionsBoundTo(c.ID()) {
		if _, retained := held[partition]; retained {
			continue
		}
		store.retire(partition, c.ID())
	}
}

// retireSSNMBindings withdraws every binding this Association holds. A
// partition losing its last binding is retired with its knowledge, in one
// critical section.
func (c *Association) retireSSNMBindings() {
	if store := c.ssnmStore(); store != nil {
		store.retireAssociation(c.ID())
	}
}

// partitionsBoundTo reports the partitions one Association currently binds.
func (s *ssnmState) partitionsBoundTo(association AssociationID) []SSNMPartition {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	partitions := make([]SSNMPartition, 0, len(s.partitions))
	for partition, state := range s.partitions {
		if _, bound := state.bindings[association]; bound {
			partitions = append(partitions, partition)
		}
	}
	sortSSNMPartitions(partitions)
	return partitions
}

// publishSSNMReport admits the Association to every partition the report
// concerns and applies the report to each.
//
// The report is applied per canonical partition, not per Routing Context: the
// label is preserved in Scope, while the knowledge is owned by the identity it
// resolved to.
func (c *Association) publishSSNMReport(report SSNMReport) error {
	store := c.ssnmStore()
	if store == nil {
		return nil
	}
	report.Association = c.ID()
	partitions := c.ssnmPartitionsForScope(report.Scope)
	bindings := make(map[SSNMPartition]bool)
	for _, binding := range c.ssnmBindingScopes() {
		bindings[binding.partition] = binding.pending
	}
	var firstErr error
	for _, partition := range partitions {
		// Admission may have preceded cancellation. Retain that report as
		// knowledge without inferring authorization from a now-absent request.
		admittedPending := c.role == RoleASP
		if pending, classified := bindings[partition]; classified {
			admittedPending = pending
		}
		if err := store.bind(partition, c.ID(), admittedPending); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		scoped := report
		scoped.Partition = partition
		if err := store.apply(scoped); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// ssnmEarlyNetworkAppearanceMatches checks the activating AS's network before
// any report mutation. RFC 4666 §3.4.1 scopes the report; §3.8.1 permits silent
// discard of some unexpected messages, but does not prescribe NA-mismatch
// discard. This implementation uses that local policy for a pre-Ack mismatch.
func (c *Association) ssnmEarlyNetworkAppearanceMatches(scope WireScope) bool {
	for _, key := range c.ssnmASKeys(scope) {
		configured := contextlessASKeyForConfig(c.cfg)
		if key.RoutingContextSet {
			configured = c.staticASKeyForRoutingContext(key.RoutingContext, false)
			if dynamic, ok := c.dynamicASKey(key.RoutingContext, false); ok {
				configured = dynamic
			}
		}
		if key.NetworkAppearanceSet != configured.NetworkAppearanceSet || key.NetworkAppearance != configured.NetworkAppearance {
			return false
		}
	}
	return true
}

// ssnmAffectedPointCodeLimit reports the Affected Point Code bound applied
// before an SSNM message's point codes are expanded.
func (c *Association) ssnmAffectedPointCodeLimit() int {
	if store := c.ssnmStore(); store != nil {
		return store.affectedPointCodeLimit()
	}
	return DefaultMaxAffectedPointCodesPerSSNM
}

// refuseOversizedSSNM turns an otherwise valid but oversized SSNM message into
// a local resource event.
//
// Nothing is sent to the peer: the message is well formed, and RFC 4666
// Section 3.8.1 has no Error condition for a receiver unwilling to expand it.
// The partitions it concerned are invalidated because their retained state may
// now contradict a report this node declined to read; every other partition is
// left alone.
func (c *Association) refuseOversizedSSNM(scope WireScope, count, limit int) error {
	store := c.ssnmStore()
	if store == nil {
		return ErrSSNMOversizedReport
	}
	return store.oversized(c.ssnmPartitionsForScope(scope), count, limit, c.ID())
}

// ssnmDestinationsFrom expands the Affected Point Code parameter into ranges.
// The caller has already bounded the count.
func ssnmDestinationsFrom(pointCodes []uint32, masks []uint8) []PointCodeRange {
	destinations := make([]PointCodeRange, len(pointCodes))
	for index, pointCode := range pointCodes {
		destinations[index] = PointCodeRange{PointCode: pointCode, Mask: masks[index]}
	}
	return destinations
}
