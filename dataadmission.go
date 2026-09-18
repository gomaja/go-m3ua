// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

// admitDataWrite decides whether one DATA may be sent for the exact
// Application Server scope it names, and holds the delivery barrier for as long
// as the caller keeps the returned release.
//
// The order is the contract. Withdrawal — ASP Inactive, ASP Down, an Override,
// a loss of the association — marks the binding first and only then waits for
// the barrier, because RFC 4666 Section 4.3.4.4 sends the acknowledgement
// "after all traffic is halted". Admission is the mirror: it takes the barrier
// and re-reads the binding under it, so a write that arrives after the mark is
// refused, and a write admitted before it completes before the withdrawal does.
func (c *Association) admitDataWrite(requested ASKey) (func(), error) {
	binding, err := c.resolveOutboundDataBinding(normalizeOutboundASKey(requested))
	if err != nil {
		return nil, err
	}
	if !c.outboundASKeyActive(binding) {
		return nil, ErrRoutingContextNotActive
	}
	return c.lockOutboundDataScopeForKey(binding)
}

// normalizeOutboundASKey discards the value of a parameter the caller did not
// mark as present, so an omitted Network Appearance carrying leftover data
// cannot make two equal scopes compare unequal. The presence flag is what the
// wire and the binding both follow.
func normalizeOutboundASKey(key ASKey) ASKey {
	if !key.NetworkAppearanceSet {
		key.NetworkAppearance = 0
	}
	if !key.RoutingContextSet {
		key.RoutingContext = 0
	}
	return key
}

// resolveOutboundDataBinding reports the Application Server a scope belongs to,
// or why it belongs to none of this association's.
//
// It is the requested scope itself in every case but one: a Routing Key
// registered without a Network Appearance serves them all, and RFC 4666 Section
// 3.6.1 says what that means for traffic — "If the Network Appearance is not
// specified and the Routing Key applies to all Network Appearances, then this
// Routing Key MUST be the only one registered for the association; that is,
// Routing Context is implied, and DATA and SSNM messages are discriminated on
// Network Appearance rather than on Routing Context." The message carries the
// appearance its traffic belongs to, and the binding it is admitted against is
// the registered one.
//
// Section 3.6.1 also makes the Network Appearance part of Routing Key identity,
// so everywhere else both halves of the pair have to match, and Section 4.3.4.3
// makes activation per Routing Context, so a matched binding is still not a
// permission to send.
func (c *Association) resolveOutboundDataBinding(key ASKey) (ASKey, error) {
	if c.hasExplicitlyEmptyASPAuthorization() && len(c.dynamicRoutingContexts(false)) == 0 {
		// The peer ASP was authorized for no traffic at all on this association.
		return key, ErrUnknownApplicationServerScope
	}
	if c.isIPSPDoubleExchange() && !c.hasPeerIPSPTrafficDirection() {
		// Section 5.6.2 keeps the two Double Exchange directions independent:
		// with no peer-directed traffic configured there is no scope to send in.
		return key, ErrUnknownApplicationServerScope
	}

	if !key.RoutingContextSet {
		// Section 3.3.1: "Where a Routing Key has not been coordinated between
		// the SGP and ASP, sending of Routing Context is not required." That is
		// the only case in which a message may omit it — where contexts have
		// been coordinated, omitting one names no Application Server rather
		// than naming the single one by implication.
		if !c.hasStaticallyConfiguredContextlessAS() {
			return key, ErrMissingRoutingContext
		}
		appearance, set := c.applicationServerAppearance()
		return key, c.checkOutboundNetworkAppearance(key, appearance, set)
	}

	if dynamic, ok := c.dynamicASKey(key.RoutingContext, false); ok {
		if !dynamic.NetworkAppearanceSet {
			// Registered for all Network Appearances: the message names the one
			// its traffic belongs to, and the binding is the registered key.
			return dynamic, nil
		}
		if dynamic.NetworkAppearance != key.NetworkAppearance || !key.NetworkAppearanceSet {
			return key, c.appearanceMismatchError(key)
		}
		return dynamic, nil
	}

	// At an SGP this is the immutable per-peer authorization resolved at ASP
	// Up, so a context the peer may not serve is refused here.
	if !c.staticRoutingContextConfigured(key.RoutingContext) {
		return key, NewInvalidRoutingContextError(key.RoutingContext)
	}
	// Section 3.6.1 makes the Network Appearance part of Routing Key identity,
	// so the appearance checked is the one the declaring entry gives this exact
	// Routing Context, not one the whole Association is assumed to share.
	declared := c.staticASKeyForRoutingContext(key.RoutingContext, false)
	return key, c.checkOutboundNetworkAppearance(key, declared.NetworkAppearance, declared.NetworkAppearanceSet)
}

// applicationServerAppearance is the Network Appearance of this association's
// contextless Application Server, with its presence flag.
func (c *Association) applicationServerAppearance() (uint32, bool) {
	key := c.contextlessASKey(false)
	return key.NetworkAppearance, key.NetworkAppearanceSet
}

func (c *Association) checkOutboundNetworkAppearance(key ASKey, appearance uint32, set bool) error {
	if key.NetworkAppearanceSet == set && key.NetworkAppearance == appearance {
		return nil
	}
	return c.appearanceMismatchError(key)
}

// appearanceMismatchError distinguishes naming an appearance this association
// does not have from naming an incomplete scope. Only the first is the Section
// 3.8.1 "Invalid Network Appearance" condition.
func (c *Association) appearanceMismatchError(key ASKey) error {
	if key.NetworkAppearanceSet {
		return NewInvalidNetworkAppearanceError(key.NetworkAppearance)
	}
	return ErrUnknownApplicationServerScope
}

// outboundASKeyActive reports whether traffic may flow for this scope now.
func (c *Association) outboundASKeyActive(key ASKey) bool {
	if key.RoutingContextSet {
		return c.outboundRoutingContextActive(key.RoutingContext)
	}
	if c.role == RoleSGP || c.role == RoleIPSP {
		return c.activeForASKey(key)
	}
	// At an ASP the SGP's acknowledgement is what starts traffic; with no
	// Routing Context coordinated the contextless Application Server is the one
	// the acknowledgement covered.
	return c.contextlessASAcked()
}

// lockOutboundDataScopeForKey holds the delivery barriers a send must own
// across the transport write. It is lockResolvedOutboundDataScope for one exact
// Application Server, taking a single barrier rather than building the scope's
// key set per message.
func (c *Association) lockOutboundDataScopeForKey(key ASKey) (func(), error) {
	if c.role != RoleSGP && c.role != RoleIPSP {
		return noDeliveryBarrier, nil
	}

	releaseApplicationServer, err := c.lockOutboundApplicationServerForKey(key)
	if err != nil {
		if c.State() != StateASPActive {
			return nil, ErrNotEstablished
		}
		return nil, err
	}
	if c.role == RoleSGP && key.RoutingContextSet {
		return releaseApplicationServer, nil
	}

	// A contextless SGP scope and every IPSP scope also use the association's
	// own barrier, which is what an ASP Down Ack and an ASP Inactive Ack wait
	// on when there is no Application Server to drain.
	c.unscopedDeliveryMu.Lock()
	release := func() {
		c.unscopedDeliveryMu.Unlock()
		releaseApplicationServer()
	}
	if c.State() != StateASPActive {
		release()
		return nil, ErrNotEstablished
	}
	if c.role == RoleIPSP && key.RoutingContextSet && !c.outboundRoutingContextActive(key.RoutingContext) {
		release()
		return nil, ErrRoutingContextNotActive
	}
	return release, nil
}

// lockOutboundApplicationServerForKey holds one Application Server's delivery
// barrier while the AS is admitting traffic.
//
// The Application Server's own state is part of the decision, not just this
// ASP's: RFC 4666 Section 4.3.4.3 has an SGP "withhold the Notify (AS-ACTIVE)
// until there are sufficient resources", and for the n+k redundancy case ASPs
// "should start sending traffic only after n ASPs are active". A direct write
// therefore honors the same aggregate state as Endpoint distribution.
func (c *Association) lockOutboundApplicationServerForKey(key ASKey) (func(), error) {
	if (c.role != RoleSGP && c.role != RoleIPSP) || c.as == nil {
		return noDeliveryBarrier, nil
	}
	applicationServer, ok := c.as.lookup(key)
	if !ok {
		return nil, ErrRoutingContextNotActive
	}

	applicationServer.deliveryMu.Lock()
	applicationServer.mu.Lock()
	closed := applicationServer.closed
	applicationServerActive := applicationServer.state == ASActive
	associationActive := applicationServer.asps[c] == StateASPActive
	applicationServer.mu.Unlock()

	switch {
	case closed:
		applicationServer.deliveryMu.Unlock()
		return nil, ErrAssociationClosed
	case !associationActive || !c.activeForASKey(key):
		applicationServer.deliveryMu.Unlock()
		return nil, ErrRoutingContextNotActive
	case !applicationServerActive:
		applicationServer.deliveryMu.Unlock()
		return nil, ErrNoActiveASP
	}
	return applicationServer.deliveryMu.Unlock, nil
}

// noDeliveryBarrier is the release for a role that holds none.
func noDeliveryBarrier() {}
