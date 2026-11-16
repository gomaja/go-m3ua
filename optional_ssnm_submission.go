package m3ua

import (
	"context"
	"slices"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-sctp"
)

// tryOptionalSSNMFrame is the shared automatic submission primitive. Traffic
// barriers precede sendMu; short state/AS checks precede tracker -> dynamic ->
// active/Ack ledger -> local revision. All admission acquisitions on this path are tries.
// No state, tracker or store lock crosses the single NoWait transport call.
// Retirement may join its existing cancellation completion; completion needs
// only the tracker; that short completion acquisition can wait for bookkeeping.
// Explicit SSNM APIs retain their synchronous writer.
// RFC 4666 §§1.4.6, 1.4.7, 3.4.4 and 4.5.3 permit these optional operations;
// neither streams nor this admission reserve kernel send-buffer memory.
func (c *Association) tryOptionalSSNMFrame(message messages.M3UA, bindings []queueCongestionBinding, epoch uint64, current func() bool) (bool, error) {
	if len(bindings) == 0 || c.role == RoleSGP {
		return false, ErrInvalidState
	}
	if c.controlWritesPending.Load() > 0 || c.controlWritesWaiting.Load() > 0 || len(c.notificationQueue) > 0 {
		return false, nil
	}
	release, ok := c.tryOptionalTraffic(bindings)
	if !ok {
		return false, nil
	}
	defer release()
	if !c.sendMu.TryLock() {
		return false, nil
	}
	defer c.sendMu.Unlock()
	if c.controlWritesPending.Load() > 0 || c.controlWritesWaiting.Load() > 0 || len(c.notificationQueue) > 0 {
		return false, nil
	}
	if !c.muState.TryRLock() {
		return false, nil
	}
	state := c.state
	if c.isIPSPDoubleExchange() {
		state = c.localIPSPState
	}
	active := state == StateASPActive && !c.terminating.Load()
	c.muState.RUnlock()
	if !active || epoch != c.Epoch() {
		return false, ErrInvalidState
	}
	if !current() {
		return false, nil
	}
	select {
	case <-c.done:
		return false, ErrAssociationClosed
	default:
	}
	if c.endpoint != nil && c.endpoint.ctx.Err() != nil {
		return false, ErrEndpointClosed
	}
	frame, err := message.MarshalBinary()
	if err != nil {
		return false, err
	}
	tracker := &c.sconSubmissions
	if !tracker.mu.TryLock() {
		return false, nil
	}
	if !c.muDynamicASKeys.TryRLock() {
		tracker.mu.Unlock()
		return false, nil
	}
	if !c.muAckedRCs.TryRLock() {
		c.muDynamicASKeys.RUnlock()
		tracker.mu.Unlock()
		return false, nil
	}
	bindingsCurrent := c.Epoch() == epoch
	scopeActive := true
	changing := tracker.optionalChanging != nil
	for _, binding := range bindings {
		changing = changing || tracker.changing[sconBindingID{binding.key.RoutingContext, binding.local}] != nil
		bindingsCurrent = bindingsCurrent && c.queueCongestionBindingCurrent(binding)
		if c.role == RoleIPSP && !c.isIPSPDoubleExchange() {
			if binding.key.RoutingContextSet {
				_, held := c.activeRCs[binding.key.RoutingContext]
				_, inactive := c.inactiveDynamicRCs[binding.key.RoutingContext]
				scopeActive = scopeActive && (!c.activeRCsScoped || held) && !inactive
			} else {
				scopeActive = scopeActive && (!c.activeRCsScoped || c.contextlessASActive)
			}
		} else {
			if binding.key.RoutingContextSet {
				_, held := c.ackedRCs[binding.key.RoutingContext]
				_, overridden := c.overriddenRCs[binding.key.RoutingContext]
				scopeActive = scopeActive && (!c.ackedRCsScoped || held) && !overridden
			} else {
				scopeActive = scopeActive && (!c.ackedRCsScoped || c.ackedContextlessAS)
			}
		}
	}
	c.muAckedRCs.RUnlock()
	c.muDynamicASKeys.RUnlock()
	if changing {
		tracker.mu.Unlock()
		return false, nil
	}
	if !bindingsCurrent {
		tracker.mu.Unlock()
		return false, ErrRoutingContextNotActive
	}
	if !scopeActive {
		tracker.mu.Unlock()
		return false, ErrNotEstablished
	}
	if !current() {
		tracker.mu.Unlock()
		return false, nil
	}
	// Final admission includes mandatory ownership acquired during revision checks.
	if c.controlWritesPending.Load() > 0 || c.controlWritesWaiting.Load() > 0 || len(c.notificationQueue) > 0 {
		tracker.mu.Unlock()
		return false, nil
	}
	parent := context.Background()
	if c.endpoint != nil {
		parent = c.endpoint.ctx
	}
	ctx, cancel := context.WithCancelCause(parent)
	submission := &sconSubmission{ctx: ctx, cancel: cancel, done: make(chan struct{}), bindings: bindings, optionalAudit: true}
	if tracker.active == nil {
		tracker.active = make(map[*sconSubmission]struct{})
	}
	tracker.active[submission] = struct{}{}
	tracker.mu.Unlock()
	defer c.finishSCONSubmission(submission)
	if c.signalWriter != nil {
		_, err = c.signalWriter(message)
	} else {
		info := *c.sctpInfo
		_, err = c.controlSender().SendMsgContext(ctx, frame, sctp.SendOptions{Info: &info, NoWait: true})
	}
	if sendBufferFull(err) {
		return false, err
	}
	return true, err
}

func (c *Association) tryOptionalTraffic(bindings []queueCongestionBinding) (func(), bool) {
	if c.role == RoleASP {
		if !c.aspTransferMu.TryRLock() {
			return nil, false
		}
		return c.aspTransferMu.RUnlock, true
	}
	if c.isIPSPDoubleExchange() {
		if !c.localIPSPSSNMDeliveryMu.TryLock() {
			return nil, false
		}
		return c.localIPSPSSNMDeliveryMu.Unlock, true
	}
	var servers []*applicationServer
	if c.as != nil {
		keys := make([]ASKey, 0, len(bindings))
		for _, binding := range bindings {
			keys = append(keys, binding.key)
		}
		slices.SortFunc(keys, compareASKey)
		for _, key := range keys {
			as, exists, acquired := c.as.tryLookupOptionalAS(key)
			if !acquired || !exists {
				return nil, false
			}
			if len(servers) == 0 || servers[len(servers)-1] != as {
				servers = append(servers, as)
			}
		}
	}
	locked := 0
	unlock := func() {
		for i := locked - 1; i >= 0; i-- {
			servers[i].deliveryMu.Unlock()
		}
	}
	for _, as := range servers {
		if !as.deliveryMu.TryLock() {
			unlock()
			return nil, false
		}
		locked++
		if !as.mu.TryLock() {
			unlock()
			return nil, false
		}
		active := !as.closed && as.state == ASActive && as.asps[c] == StateASPActive
		as.mu.Unlock()
		if !active {
			unlock()
			return nil, false
		}
	}
	if !c.unscopedDeliveryMu.TryLock() {
		unlock()
		return nil, false
	}
	return func() { c.unscopedDeliveryMu.Unlock(); unlock() }, true
}

// tryLookupOptionalAS reads an exact immutable binding key without waiting for
// registry mutation. Absence and registry closure are permanent for this token;
// a busy registry is a deferral, not permission to bypass the AS barrier.
func (r *applicationServers) tryLookupOptionalAS(key ASKey) (*applicationServer, bool, bool) {
	if r == nil {
		return nil, false, true
	}
	if !r.mu.TryLock() {
		return nil, false, false
	}
	defer r.mu.Unlock()
	if r.closed {
		return nil, false, true
	}
	as, exists := r.as[normalizeOutboundASKey(key)]
	return as, exists, true
}
