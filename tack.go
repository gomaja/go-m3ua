// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"sync"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// DefaultTAck is the T(ack) timer's default value.
//
// RFC 4666 Sections 4.3.4.1 to 4.3.4.4 each specify the same timer for the
// request they describe: "T(ack) is provisionable, with a default of 2
// seconds."
const DefaultTAck = 2 * time.Second

// pendingRequest is an ASPSM/ASPTM request awaiting its acknowledgement.
type pendingRequest struct {
	msg  messages.M3UA
	kind requestKind
	// routingContexts is the explicit RC set the ASPTM request named.
	// remaining is reduced by each independently received Ack. Both are nil
	// for ASPSM requests and for an ASPTM request that omitted RC.
	routingContexts map[uint32]struct{}
	remaining       map[uint32]struct{}
	// routingContextOmitted distinguishes an unscoped wire request whose known
	// configured AS set is tracked internally from an explicitly scoped request.
	routingContextOmitted bool
	stop                  chan struct{}
	result                chan error
	once                  sync.Once
}

// tackRetransmitter resends unacknowledged ASPSM/ASPTM requests until their Ack
// arrives.
//
// RFC 4666 Section 4.3.4.1 for ASP Up, 4.3.4.2 for ASP Down, 4.3.4.3 for ASP
// Active and 4.3.4.4 for ASP Inactive all say the same thing: "If the ASP does
// not receive a response ... within T(ack), the ASP MAY restart T(ack) and
// resend [the request] until it receives [the] Ack message."
//
// Without this, a request lost in transit — or dropped by a peer that was
// briefly out of state — strands the association forever: the ASP sits waiting
// for an Ack that will never come while the SGP waits for a request it never
// saw. That is the same class of deadlock as the withheld Acks fixed elsewhere
// in this package, seen from the other end.
type tackRetransmitter struct {
	mu sync.Mutex
	// retryMu orders the small number of T(ack) retransmission writes against
	// association-epoch boundaries. It does not serialize ordinary Association writes:
	// only retries take it. A restart or orderly termination can therefore wait
	// for an already-started old request to leave the wire, cancel the rest, and
	// then send the new epoch's first request without an old retry overtaking it.
	retryMu sync.Mutex
	pending map[messages.M3UA]*pendingRequest
	// awaitingRestartAspUp rejects ASPTM Acks left over from the prior SCTP
	// epoch until the mandatory fresh ASP-Up procedure completes. Stream 0 has
	// no request identifier with which an old ASPTM Ack could otherwise be
	// distinguished from an unsolicited Ack whose RFC procedure changes state.
	awaitingRestartAspUp bool
}

func newTAckRetransmitter() *tackRetransmitter {
	return &tackRetransmitter{pending: make(map[messages.M3UA]*pendingRequest)}
}

// tackInterval reports the configured T(ack), or the RFC default.
func (c *Association) tackInterval() time.Duration {
	if c.cfg != nil && c.cfg.TAck > 0 {
		return c.cfg.TAck
	}
	return DefaultTAck
}

// startTAck begins retransmitting msg every T(ack) until stopTAck is called for
// the matching acknowledgement, the association closes, or the retry budget is
// exhausted.
//
// Retransmission is deliberately bounded. The RFC's "until it receives the Ack"
// is unbounded, but an ASP that resends forever against a peer that will never
// answer just adds load to a network already in trouble; the budget converts a
// silent hang into a reportable failure. TAckRetries configures it.
func (c *Association) startTAck(msg messages.M3UA, ackFor requestKind) *pendingRequest {
	if c.tack == nil {
		return nil
	}

	retryMessage, err := cloneTAckMessage(msg)
	req := &pendingRequest{
		msg:    retryMessage,
		kind:   ackFor,
		stop:   make(chan struct{}),
		result: make(chan error, 1),
	}
	if err != nil {
		// Production callers hand startTAck one of the package's four concrete
		// request types, all of which have already been constructed and validated.
		// Still fail closed for a custom M3UA implementation: sharing the original
		// after its snapshot failed would recreate the very race this clone closes.
		req.finish(err)
		return req
	}
	routingContext := requestRoutingContext(retryMessage)
	if routingContext == nil && (ackFor == requestAspActive || ackFor == requestAspInactive) {
		req.routingContextOmitted = true
		if c.cfg != nil {
			routingContext = c.cfg.RoutingContexts
		}
	}
	if routingContext != nil {
		req.routingContexts = make(map[uint32]struct{})
		req.remaining = make(map[uint32]struct{})
		for _, rtCtx := range routingContext.RoutingContexts() {
			req.routingContexts[rtCtx] = struct{}{}
			req.remaining[rtCtx] = struct{}{}
		}
	}

	// Only an orderly-termination boundary waits behind an in-flight retry.
	// Ordinary request registration must remain independent: a newer ASP Up can
	// supersede an old timer whose write is blocked, and waiting for that write
	// here would deadlock the caller that owns the release/cancellation path.
	terminationBoundary := c.terminating.Load() &&
		(ackFor == requestAspInactive || ackFor == requestAspDown)
	if terminationBoundary {
		c.tack.retryMu.Lock()
	}
	c.tack.mu.Lock()
	if terminationBoundary {
		c.cancelAllTAckLocked()
	}
	// Association-wide ASPSM procedures have one current epoch and a newer one
	// supersedes it. ASPTM is per AS: RFC 4666 explicitly permits multiple Active
	// or Inactive messages for different Routing Context sets, so those timers are
	// independent and an Ack is matched by scope.
	if ackFor == requestAspUp || ackFor == requestAspDown {
		for message, pending := range c.tack.pending {
			if pending.kind == ackFor {
				pending.cancel()
				delete(c.tack.pending, message)
			}
		}
	}
	c.tack.pending[msg] = req
	c.tack.mu.Unlock()
	if terminationBoundary {
		c.tack.retryMu.Unlock()
	}

	go c.runTAck(req, ackFor)
	return req
}

func (p *pendingRequest) cancel() {
	p.finish(ErrAssociationClosed)
}

func (p *pendingRequest) acknowledge() {
	p.finish(nil)
}

func (p *pendingRequest) finish(err error) {
	p.once.Do(func() {
		p.result <- err
		close(p.result)
		close(p.stop)
	})
}

func (c *Association) runTAck(req *pendingRequest, kind requestKind) {
	interval := c.tackInterval()
	retries := DefaultTAckRetries
	if c.cfg != nil && c.cfg.TAckRetries > 0 {
		retries = c.cfg.TAckRetries
	}

	for attempt := 0; attempt < retries; attempt++ {
		select {
		case <-req.stop:
			return
		case <-c.done:
			return
		case <-time.After(interval):
		}

		c.tack.retryMu.Lock()
		select {
		case <-req.stop:
			c.tack.retryMu.Unlock()
			return
		case <-c.done:
			c.tack.retryMu.Unlock()
			return
		default:
		}

		_, err := c.WriteSignal(req.msg)
		c.tack.retryMu.Unlock()
		if err != nil {
			// The association is going away; monitor() will act on the write
			// failure reported by whoever owns it.
			c.forgetTAckRequest(req, kind, err)
			return
		}
	}

	// The peer never answered. Report it rather than retrying silently forever:
	// an operator needs to see that the far end is not completing the handshake.
	if c.forgetTAckRequest(req, kind, ErrTAckExpired) {
		c.sendErr(ErrTAckExpired)
	}
}

// cloneTAckMessage snapshots a request before its retransmission goroutine can
// run. MarshalTo methods build Header.Payload in place, so using the initiating
// message itself from both goroutines races even though both are only writing
// the same logical bytes. Parsing the wire snapshot also deep-copies every
// parameter, including forward-compatible Others.
func cloneTAckMessage(message messages.M3UA) (messages.M3UA, error) {
	raw, err := message.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return messages.Parse(raw)
}

// stopTAck cancels retransmission of whichever request the given kind of
// acknowledgement answers, and reports whether there was one outstanding.
//
// The report is what separates an acknowledgement of something this node asked
// for from one it never asked for. RFC 4666 Section 4.3.4.1 attaches
// consequences to the second — "If the ASP receives an unexpected ASP Up Ack
// message..." — and none to the first.
func (c *Association) stopTAck(kind requestKind) bool {
	return c.forgetTAck(kind)
}

// validateTAckRoutingContexts checks that an ASPTM Ack answers the explicit RC
// set in the outstanding request. Configuration membership alone is
// insufficient: an association can carry several Application Servers while
// one request activates or deactivates only a subset.
func (c *Association) validateTAckRoutingContexts(kind requestKind, acknowledged *params.Param) error {
	if c.tack == nil {
		return nil
	}

	c.tack.mu.Lock()
	defer c.tack.mu.Unlock()
	requests := c.pendingRequestsLocked(kind)
	if len(requests) == 0 {
		return nil
	}
	if acknowledged == nil {
		for _, req := range requests {
			if req.routingContextOmitted || req.routingContexts == nil {
				return nil
			}
		}
		return ErrMissingRoutingContext
	}

	var offending []uint32
	for _, rtCtx := range acknowledged.RoutingContexts() {
		known := false
		for _, req := range requests {
			if req.routingContexts == nil {
				known = true
				break
			}
			if _, ok := req.routingContexts[rtCtx]; ok {
				known = true
				break
			}
		}
		if !known {
			offending = append(offending, rtCtx)
		}
	}
	if len(offending) > 0 {
		return NewInvalidRoutingContextError(offending...)
	}
	return nil
}

// acknowledgeTAck retires the portion of an outstanding request an Ack
// covered. RFC 4666 permits multiple ASP Active and ASP Inactive Acks for
// different RC subsets, so the retransmitter remains armed until all explicit
// contexts have been acknowledged.
func (c *Association) acknowledgeTAck(kind requestKind, acknowledged *params.Param) bool {
	if c.tack == nil {
		return false
	}

	c.tack.mu.Lock()
	defer c.tack.mu.Unlock()
	requests := c.pendingRequestEntriesLocked(kind)
	if len(requests) == 0 {
		return false
	}

	solicited := false
	for _, entry := range requests {
		req := entry.request
		matched := false
		if acknowledged == nil {
			matched = req.routingContextOmitted || req.routingContexts == nil
			if matched {
				req.remaining = nil
			}
		} else if req.routingContexts == nil {
			matched = true
		} else {
			for _, rtCtx := range acknowledged.RoutingContexts() {
				if _, ok := req.routingContexts[rtCtx]; !ok {
					continue
				}
				matched = true
				delete(req.remaining, rtCtx)
			}
		}
		if !matched {
			continue
		}
		solicited = true
		if len(req.remaining) > 0 {
			continue
		}
		req.acknowledge()
		delete(c.tack.pending, entry.message)
	}
	return solicited
}

// pendingTAckRoutingContexts returns the explicit RCs still awaiting an Ack.
// It is also the activation-window scope in which an SGP may send DUNA, DRST,
// and SCON before the corresponding ASP Active Ack.
func (c *Association) pendingTAckRoutingContexts(kind requestKind) []uint32 {
	if c.tack == nil {
		return nil
	}
	c.tack.mu.Lock()
	defer c.tack.mu.Unlock()
	set := make(map[uint32]struct{})
	for _, req := range c.pendingRequestsLocked(kind) {
		for rtCtx := range req.remaining {
			set[rtCtx] = struct{}{}
		}
	}
	routingContexts := make([]uint32, 0, len(set))
	for rtCtx := range set {
		routingContexts = append(routingContexts, rtCtx)
	}
	return routingContexts
}

type pendingRequestEntry struct {
	message messages.M3UA
	request *pendingRequest
}

func (c *Association) pendingRequestEntriesLocked(kind requestKind) []pendingRequestEntry {
	requests := make([]pendingRequestEntry, 0)
	for message, request := range c.tack.pending {
		if request.kind == kind {
			requests = append(requests, pendingRequestEntry{message: message, request: request})
		}
	}
	return requests
}

func (c *Association) pendingRequestsLocked(kind requestKind) []*pendingRequest {
	entries := c.pendingRequestEntriesLocked(kind)
	requests := make([]*pendingRequest, 0, len(entries))
	for _, entry := range entries {
		requests = append(requests, entry.request)
	}
	return requests
}

func (c *Association) forgetTAck(kind requestKind) bool {
	if c.tack == nil {
		return false
	}

	c.tack.mu.Lock()
	defer c.tack.mu.Unlock()

	found := false
	for m, p := range c.tack.pending {
		if p.kind == kind {
			p.acknowledge()
			delete(c.tack.pending, m)
			found = true
		}
	}
	if found && kind == requestAspUp {
		c.tack.awaitingRestartAspUp = false
	}
	return found
}

// rejectStaleASPTMAck reports an ASPTM Ack that cannot belong to the current
// control procedure. After SCTP restart, RFC 4666 Section 4.3.3 requires
// recovery to begin with ASP Up, so no ASP Active/Inactive Ack is actionable
// until that fresh ASP Up is acknowledged. During orderly termination, only
// the request currently being awaited may change state; all earlier timers were
// cancelled before the first withdrawal message was written.
//
// A pending request of this kind is intentionally left to the normal scoped
// validator. Its Ack may be partial, malformed, or for the wrong RC, and those
// cases already produce the precise RFC error without changing state.
func (c *Association) rejectStaleASPTMAck(kind requestKind) bool {
	if c.tack == nil {
		return false
	}
	c.tack.mu.Lock()
	defer c.tack.mu.Unlock()
	if !c.tack.awaitingRestartAspUp && !c.terminating.Load() {
		return false
	}
	return len(c.pendingRequestsLocked(kind)) == 0
}

// forgetTAckRequest removes req only if it is still the current request of its
// kind. A superseded retransmitter can finish an in-flight write after its
// replacement was armed; it must not retire or report expiry for that newer
// request.
func (c *Association) forgetTAckRequest(req *pendingRequest, kind requestKind, result error) bool {
	if c.tack == nil {
		return false
	}

	c.tack.mu.Lock()
	defer c.tack.mu.Unlock()

	for message, pending := range c.tack.pending {
		if pending == req && pending.kind == kind {
			pending.finish(result)
			delete(c.tack.pending, message)
			return true
		}
	}
	return false
}

// cancelTAckRequest removes one exact request without mistaking it for an Ack.
// It is used when the initial write fails or a caller abandons a wait.
func (c *Association) cancelTAckRequest(req *pendingRequest) {
	if c.tack == nil || req == nil {
		return
	}

	c.tack.mu.Lock()
	defer c.tack.mu.Unlock()
	for message, pending := range c.tack.pending {
		if pending == req {
			pending.cancel()
			delete(c.tack.pending, message)
			return
		}
	}
}

// waitTAck waits for one request to be acknowledged, fail, or be cancelled.
func (c *Association) waitTAck(ctx context.Context, req *pendingRequest) error {
	if req == nil {
		return nil
	}
	select {
	case err := <-req.result:
		return err
	case <-ctx.Done():
		c.cancelTAckRequest(req)
		return ctx.Err()
	case <-c.done:
		if err := c.Err(); err != nil {
			return err
		}
		return ErrAssociationClosed
	}
}

// stopAllTAck cancels every outstanding retransmission, used when the
// association goes down.
func (c *Association) stopAllTAck() {
	if c.tack == nil {
		return
	}

	c.tack.mu.Lock()
	defer c.tack.mu.Unlock()

	c.cancelAllTAckLocked()
}

// resetTAckEpoch retires every request from the prior SCTP association epoch.
// The retry fence is acquired before the pending-map lock: runTAck and
// startTAck use the same order, so an already-started retry drains first and a
// not-yet-started one observes its closed stop channel before writing.
func (c *Association) resetTAckEpoch() {
	if c.tack == nil {
		return
	}
	c.tack.retryMu.Lock()
	c.tack.mu.Lock()
	c.tack.awaitingRestartAspUp = c.role == RoleASP || c.role == RoleIPSP
	c.cancelAllTAckLocked()
	c.tack.mu.Unlock()
	c.tack.retryMu.Unlock()
}

func (c *Association) completeRestartASPSM() {
	if c.tack == nil {
		return
	}
	c.tack.mu.Lock()
	c.tack.awaitingRestartAspUp = false
	c.tack.mu.Unlock()
}

func (c *Association) cancelAllTAckLocked() {
	for message, request := range c.tack.pending {
		request.cancel()
		delete(c.tack.pending, message)
	}
}

// pendingTAck reports how many requests are awaiting acknowledgement, for tests
// and for callers that want to surface handshake progress.
func (c *Association) pendingTAck() int {
	if c.tack == nil {
		return 0
	}

	c.tack.mu.Lock()
	defer c.tack.mu.Unlock()
	return len(c.tack.pending)
}

// requestKind identifies which ASPSM/ASPTM request an acknowledgement answers.
type requestKind uint8

// Request kind definitions.
const (
	requestNone requestKind = iota
	requestAspUp
	requestAspDown
	requestAspActive
	requestAspInactive
)

func requestKindOf(m messages.M3UA) requestKind {
	switch m.(type) {
	case *messages.AspUp:
		return requestAspUp
	case *messages.AspDown:
		return requestAspDown
	case *messages.AspActive:
		return requestAspActive
	case *messages.AspInactive:
		return requestAspInactive
	default:
		return requestNone
	}
}

func requestRoutingContext(message messages.M3UA) *params.Param {
	switch request := message.(type) {
	case *messages.AspActive:
		return request.RoutingContext
	case *messages.AspInactive:
		return request.RoutingContext
	default:
		return nil
	}
}
