// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
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

// pendingControlWrite describes one transport call, never an expected Ack.
// Closing done publishes its immutable outcome and original per-AS scope.
type pendingControlWrite struct {
	done     chan struct{}
	keys     []ASKey
	accepted bool
}

func (p *pendingRequest) beginControlWrite(keys []ASKey) *pendingControlWrite {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	select {
	case <-p.stop:
		return nil
	default:
	}
	call := &pendingControlWrite{done: make(chan struct{}), keys: keys}
	p.write = call
	return call
}

func (call *pendingControlWrite) complete(submitted bool, stopped <-chan struct{}) {
	if submitted {
		select {
		case <-stopped:
			call.accepted = true
		default:
		}
	}
	close(call.done)
}

func (p *pendingRequest) joinControlWrite() *pendingControlWrite {
	p.writeMu.Lock()
	call := p.write
	if call == nil {
		// Stop is already closed. A writer still waiting for sendMu cannot
		// enter the transport; publish that completed outcome immediately.
		call = &pendingControlWrite{done: make(chan struct{})}
		close(call.done)
		p.write = call
	}
	p.writeMu.Unlock()
	<-call.done
	return call
}

// pendingRequest is an ASPSM/ASPTM request awaiting its acknowledgement.
type pendingRequest struct {
	msg  messages.M3UA
	kind requestKind
	// routingContexts is the transmitted RC set, or the configured/registered
	// membership an omitted-RC ASPTM request covers (RFC 4666 §4.3.4.3).
	// remaining is reduced by each independently received Ack. Both are nil
	// for ASPSM and for a contextless ASPTM scope without any RC labels.
	routingContexts map[uint32]struct{}
	remaining       map[uint32]struct{}
	// routingContextOmitted distinguishes an unscoped wire request whose known
	// configured AS set is tracked internally from an explicitly scoped request.
	routingContextOmitted bool
	stop                  chan struct{}
	result                chan error
	stopOnce              sync.Once
	resultOnce            sync.Once
	writeMu               sync.Mutex
	write                 *pendingControlWrite
	// failure is visible before stop closes, including to an initial write
	// that cancellation prevents from being submitted.
	failure atomic.Pointer[error]
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
	// retryMu orders initial pending-procedure writes and T(ack) retries against
	// association-epoch and peer-withdrawal boundaries. Ordinary application
	// writes remain independent. A restart or orderly termination can wait
	// for an already-started old request to leave the wire, cancel the rest, and
	// then send the new epoch's first request without an old retry overtaking it.
	retryMu sync.Mutex
	pending map[messages.M3UA]*pendingRequest
	// acknowledgedASPTM retains the ASPTM scopes acknowledged during the current
	// SCTP association epoch, with Inactive history retired per reactivated scope.
	// RFC 4666 Sections 4.3.4.3 and 4.3.4.4 permit
	// T(ack) retransmission and require the peer to answer each repeated request.
	// Because ASPTM has no request identifier, an Ack for a retransmission can
	// arrive after its first Ack completed the procedure. Retaining the bounded
	// configured RC set lets handlers distinguish that repeated Ack from an
	// unsolicited state transition without accepting an unrequested scope.
	acknowledgedASPTM map[requestKind]*acknowledgedASPTMScope
	// aspUpAcknowledged records completion of the current ASP Up procedure.
	// ASPSM has no request identifier, so a later Ack for a T(ack)
	// retransmission is distinguishable from an unsolicited Ack only by this
	// bounded current-epoch evidence.
	aspUpAcknowledged bool
	// RFC 4666 Section 4.3.4.2 requires one ASP Down Ack for every ASP Down,
	// including retransmissions. This counts successfully sent messages still
	// owed an Ack in the current SCTP association.
	aspDownAcksOutstanding uint64
	// Serializes an ASP Down write and its count update with Ack classification.
	// An Ack may arrive before the sending goroutine resumes from SendMsg.
	aspDownDeliveryMu sync.Mutex
	// awaitingAspUp rejects ASPTM Acks after SCTP restart or a shared-ASPSM
	// peer withdrawal until a fresh ASP-Up procedure completes. Stream 0 has
	// no request identifier with which an old ASPTM Ack could otherwise be
	// distinguished from an unsolicited Ack whose RFC procedure changes state.
	awaitingAspUp bool
	// peerDownSuperseded distinguishes an Up Ack following the peer's shared
	// withdrawal from an ordinary unsolicited Ack. A fresh Up procedure clears it.
	peerDownSuperseded bool
	// peerDownHandling refuses a new shared procedure until Down Ack is written.
	peerDownHandling bool
}

type acknowledgedASPTMScope struct {
	routingContextOmitted bool
	routingContexts       map[uint32]struct{}
}

func newTAckRetransmitter() *tackRetransmitter {
	return &tackRetransmitter{
		pending:           make(map[messages.M3UA]*pendingRequest),
		acknowledgedASPTM: make(map[requestKind]*acknowledgedASPTMScope),
	}
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
// Retransmission is deliberately bounded. The RFC's "until it receives an ASP
// Up Ack message", and its ASP Down, ASP Active and ASP Inactive counterparts,
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
			if c.role == RoleASP {
				routingContext = c.configuredRoutingContextParam()
			} else {
				routingContext = asConfigRoutingContextParam(c.cfg.ApplicationServers)
			}
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
	if c.tack.peerDownHandling && ackFor != requestAspDown ||
		c.tack.peerDownSuperseded && (ackFor == requestAspActive || ackFor == requestAspInactive) {
		c.tack.mu.Unlock()
		if terminationBoundary {
			c.tack.retryMu.Unlock()
		}
		req.finish(fmt.Errorf("%w by peer ASP Down", ErrASPProcedureSuperseded))
		return req
	}
	if ackFor == requestAspUp {
		c.tack.aspUpAcknowledged = false
		c.tack.peerDownSuperseded = false
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

// writePendingControl orders the initial write with the retry/withdrawal fence.
// Registration remains independent so a newer request can supersede a blocked
// timer. A superseded request returns its result through waitTAck; it must not
// put its initial message on the wire after the peer's Down Ack (§4.3.4.2).
func (c *Association) writePendingControl(request *pendingRequest, message messages.M3UA) (int, error) {
	if c.tack == nil || request == nil {
		return c.writeControl(message)
	}
	c.tack.retryMu.Lock()
	var n int
	var err error
	select {
	case <-request.stop:
	default:
		n, err = c.writeTAckControl(request, message)
	}
	c.tack.retryMu.Unlock()
	if err == nil {
		if failed := request.failure.Load(); failed != nil {
			return 0, *failed
		}
	}
	return n, err
}

// writeTAckControl runs under retryMu, including the accepted-result record.
func (c *Association) writeTAckControl(req *pendingRequest, message messages.M3UA) (int, error) {
	return c.writeControlUntilStopped(message, req.stop, req)
}

func (p *pendingRequest) acknowledge() {
	p.finish(nil)
}

func (p *pendingRequest) stopRetries() {
	p.stopOnce.Do(func() { close(p.stop) })
}

func (p *pendingRequest) finish(err error) {
	p.resultOnce.Do(func() {
		if err != nil {
			p.failure.Store(&err)
		}
		p.stopRetries()
		p.result <- err
		close(p.result)
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

		_, err := c.writeTAckControl(req, req.msg)
		c.tack.retryMu.Unlock()
		if err != nil {
			// The association is going away; monitor() will act on the write
			// failure reported by whoever owns it.
			c.forgetTAckRequest(req, kind, err)
			return
		}
	}

	// The last resend is owed a full T(ack) too. RFC 4666 Section 4.3.4.1 has
	// the sender "restart T(ack) and resend", so the request has expired only
	// when the timer the final resend started runs out unanswered; giving up
	// the moment it is written leaves that resend no chance to be answered.
	select {
	case <-req.stop:
		return
	case <-c.done:
		return
	case <-time.After(interval):
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
	acknowledgement := c.claimTAckAcknowledgement(kind, acknowledged)
	acknowledgement.complete()
	return acknowledgement.solicited
}

type pendingTAckAcknowledgement struct {
	solicited bool
	completed []*pendingRequest
}

func (acknowledgement pendingTAckAcknowledgement) complete() {
	for _, request := range acknowledgement.completed {
		request.acknowledge()
	}
}

// claimTAckAcknowledgement applies an Ack to the retransmission inventory and
// immediately stops every request it completes, but deliberately leaves its
// waiter blocked. A state handler can then close traffic gates and drain writes
// admitted before the Ack before calling complete.
func (c *Association) claimTAckAcknowledgement(kind requestKind, acknowledged *params.Param) pendingTAckAcknowledgement {
	return c.claimTAckAcknowledgementForScope(kind, acknowledged, acknowledged)
}

func (c *Association) claimTAckAcknowledgementForScope(kind requestKind, acknowledged, wireRoutingContext *params.Param) pendingTAckAcknowledgement {
	if c.tack == nil {
		return pendingTAckAcknowledgement{}
	}

	c.tack.mu.Lock()
	defer c.tack.mu.Unlock()
	return c.claimTAckAcknowledgementLocked(kind, acknowledged, wireRoutingContext)
}

func (c *Association) claimTAckAcknowledgementLocked(kind requestKind, acknowledged, wireRoutingContext *params.Param) pendingTAckAcknowledgement {
	if kind == requestAspActive {
		c.retireInactiveAcknowledgementLocked(acknowledged)
	}
	requests := c.pendingRequestEntriesLocked(kind)
	if len(requests) == 0 {
		return pendingTAckAcknowledgement{}
	}

	result := pendingTAckAcknowledgement{}
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
		result.solicited = true
		// An explicit partial Ack cannot acknowledge an omitted contextless AS.
		// Its unkeyed Ack completes that scope along with any remaining RCs
		// (RFC 4666 §4.3.4.3). Inactive Ack may omit RC (§3.7.4, §4.3.4.4),
		// confirming an omitted request without materializing an explicit scope.
		contextlessOutstanding := c.role == RoleASP && req.routingContextOmitted && acknowledged != nil && c.hasConfiguredLocalContextlessAS()
		if len(req.remaining) > 0 || contextlessOutstanding {
			continue
		}
		req.stopRetries()
		delete(c.tack.pending, entry.message)
		result.completed = append(result.completed, req)
	}
	if result.solicited {
		c.recordASPTMAcknowledgementLocked(kind, acknowledged)
		if wireRoutingContext == nil {
			c.recordASPTMAcknowledgementLocked(kind, nil)
		}
	}
	return result
}

func (c *Association) recordASPTMAcknowledgementLocked(kind requestKind, acknowledged *params.Param) {
	if kind != requestAspActive && kind != requestAspInactive {
		return
	}
	scope := c.tack.acknowledgedASPTM[kind]
	if scope == nil {
		scope = &acknowledgedASPTMScope{routingContexts: make(map[uint32]struct{})}
		c.tack.acknowledgedASPTM[kind] = scope
	}
	if acknowledged == nil {
		scope.routingContextOmitted = true
		if kind == requestAspInactive {
			// Preserve the individually acknowledged ASes if a later scoped
			// activation ends only part of this duplicate window (RFC 4666 §4.3.4.4).
			for _, routingContext := range c.configuredLocalRoutingContexts() {
				scope.routingContexts[routingContext] = struct{}{}
			}
		}
		return
	}
	for _, routingContext := range acknowledged.RoutingContexts() {
		scope.routingContexts[routingContext] = struct{}{}
	}
}

// pendingTAckContextless reports an omitted-RC request covering a contextless AS,
// including alongside dynamically registered RCs. RFC 4666 §3.4.1 permits
// that scope; §4.5.1 and §4.6 allow early SSNM while its ASP Active is
// outstanding, just as for explicit RCs.
func (c *Association) pendingTAckContextless(kind requestKind) bool {
	if c.tack == nil || !c.hasConfiguredLocalContextlessAS() {
		return false
	}
	c.tack.mu.Lock()
	defer c.tack.mu.Unlock()
	for _, req := range c.pendingRequestsLocked(kind) {
		if req.routingContextOmitted {
			return true
		}
	}
	return false
}

// retireInactiveAcknowledgementLocked ends duplicate recognition only for
// scopes whose Active Ack has been accepted or sent in IPSP Single Exchange
// (RFC 4666 §4.3.4.3.1 and §5.6.1). RFC 4666 §4.3.4.4 describes
// withdrawing those ASes on a subsequent unsolicited Inactive Ack; §4.7
// describes partial NIF failure as one source of such a fresh withdrawal.
func (c *Association) retireInactiveAcknowledgementLocked(activated *params.Param) {
	if activated == nil {
		delete(c.tack.acknowledgedASPTM, requestAspInactive)
		return
	}
	scope := c.tack.acknowledgedASPTM[requestAspInactive]
	if scope == nil {
		return
	}
	// An omitted Ack can no longer be a duplicate for the entire association.
	// Explicit history for ASes that have not reactivated remains valid.
	scope.routingContextOmitted = false
	for _, routingContext := range activated.RoutingContexts() {
		delete(scope.routingContexts, routingContext)
	}
}

func (c *Association) retireInactiveAcknowledgement(activated *params.Param) {
	if c.tack == nil {
		return
	}
	c.tack.mu.Lock()
	defer c.tack.mu.Unlock()
	c.retireInactiveAcknowledgementLocked(activated)

}

// pendingTAckRoutingContexts returns the explicit RCs still awaiting an Ack.
// It supplies the explicit-RC part of the RFC 4666 §4.5.1 / §4.6 window;
// pendingTAckContextless supplies its omitted-RC counterpart.
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
		c.tack.aspUpAcknowledged = true
		c.tack.awaitingAspUp = false
	}
	return found
}

func (c *Association) isRepeatedASPUpAcknowledgement() bool {
	if c.tack == nil {
		return false
	}
	c.tack.mu.Lock()
	defer c.tack.mu.Unlock()
	return c.tack.aspUpAcknowledged
}

// supersedePeerDownProcedures retires the procedures invalidated by a shared
// withdrawal (RFC 4666 §4.3, §4.3.4.2 and §5.6.1). Normal Double Exchange
// retains its independent local procedures (§5.6.2). Drain an in-flight retry
// or initial write before Down Ack; queued writes observe their closed stop channel.
// Waiters are released by the handler after state and acknowledgement settle.
func (c *Association) supersedePeerDownProcedures() []*pendingRequest {
	if c.tack == nil || c.role != RoleIPSP ||
		(c.isIPSPDoubleExchange() && !c.usesSingleASPSMExchange()) {
		return nil
	}
	c.tack.retryMu.Lock()
	defer c.tack.retryMu.Unlock()
	c.tack.mu.Lock()
	defer c.tack.mu.Unlock()
	c.tack.peerDownSuperseded = true
	c.tack.peerDownHandling = true
	c.tack.awaitingAspUp = true
	c.tack.aspUpAcknowledged = false
	clear(c.tack.acknowledgedASPTM)
	var superseded []*pendingRequest
	for message, request := range c.tack.pending {
		if request.kind == requestAspUp || request.kind == requestAspActive || request.kind == requestAspInactive {
			request.stopRetries()
			delete(c.tack.pending, message)
			superseded = append(superseded, request)
		}
	}
	return superseded
}

func (c *Association) finishPeerDownProcedures(requests []*pendingRequest) {
	if c.tack != nil {
		c.tack.mu.Lock()
		c.tack.peerDownHandling = false
		c.tack.mu.Unlock()
	}
	for _, request := range requests {
		request.finish(fmt.Errorf("%w by peer ASP Down", ErrASPProcedureSuperseded))
	}
}

func (c *Association) isPeerDownSupersededASPUpAck() bool {
	if c.tack == nil {
		return false
	}
	c.tack.mu.Lock()
	defer c.tack.mu.Unlock()
	return c.tack.peerDownSuperseded
}

// claimASPDownAcknowledgement consumes one owed response whether or not a
// T(ack) request remains. Only an Ack without a pending request and with an
// outstanding transmission is a duplicate of an earlier ASP Down.
func (c *Association) claimASPDownAcknowledgement() (pendingTAckAcknowledgement, bool) {
	if c.tack == nil {
		return pendingTAckAcknowledgement{}, false
	}
	c.tack.aspDownDeliveryMu.Lock()
	defer c.tack.aspDownDeliveryMu.Unlock()
	acknowledgement := c.claimTAckAcknowledgement(requestAspDown, nil)
	c.tack.mu.Lock()
	duplicate := !acknowledgement.solicited && c.tack.aspDownAcksOutstanding > 0
	if c.tack.aspDownAcksOutstanding > 0 {
		c.tack.aspDownAcksOutstanding--
	}
	c.tack.mu.Unlock()
	return acknowledgement, duplicate
}

// rejectStaleASPTMAck reports an ASPTM Ack that cannot belong to the current
// control procedure. After SCTP restart or a shared-ASPSM peer withdrawal,
// RFC 4666 §4.3.3 requires ASP Up after restart; §4.3.1 and §4.3.4.2
// leave withdrawn communication ASP-DOWN, where ASPTM cannot establish service.
// No ASP Active/Inactive Ack is actionable
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
	if !c.tack.awaitingAspUp && !c.terminating.Load() {
		return false
	}
	return len(c.pendingRequestsLocked(kind)) == 0
}

// isRepeatedASPTMAcknowledgement reports an Ack whose entire scope was already
// acknowledged during this SCTP association epoch and does not match a current
// request of the same kind. Inactive history ends on accepted activation or
// peer Single Exchange activation (RFC 4666 §4.3.4.3.1, §4.3.4.4, §5.6.1).
//
// RFC 4666 Sections 4.3.4.3 and 4.3.4.4 permit the local IPSP to retransmit ASP
// Active or ASP Inactive until an Ack arrives. The peer answers every copy, so
// a later Ack before reactivation must not provoke a second withdrawal or an
// Error for a request this association did send. A current request wins because the
// protocol provides no request identifier that could distinguish its Ack from
// an older retransmission's Ack. An acknowledged Inactive portion is no
// longer outstanding, even while other scopes of that request remain pending.
func (c *Association) isRepeatedASPTMAcknowledgement(kind requestKind, acknowledged *params.Param) bool {
	return c.isRepeatedASPTMAcknowledgementForScope(kind, acknowledged, acknowledged)
}

func (c *Association) isRepeatedASPTMAcknowledgementForScope(kind requestKind, acknowledged, wireRoutingContext *params.Param) bool {
	if c.tack == nil {
		return false
	}
	c.tack.mu.Lock()
	defer c.tack.mu.Unlock()
	for _, request := range c.pendingRequestsLocked(kind) {
		if kind == requestAspInactive && wireRoutingContext == nil {
			return false
		}
		if acknowledged == nil {
			if request.routingContextOmitted || request.routingContexts == nil {
				return false
			}
			continue
		}
		if request.routingContexts == nil {
			return false
		}
		pendingContexts := request.routingContexts
		if kind == requestAspInactive {
			pendingContexts = request.remaining
		}
		for _, routingContext := range acknowledged.RoutingContexts() {
			if _, pending := pendingContexts[routingContext]; pending {
				return false
			}
		}
	}
	scope := c.tack.acknowledgedASPTM[kind]
	if scope == nil {
		return false
	}
	if wireRoutingContext == nil && scope.routingContextOmitted {
		return true
	}
	if acknowledged == nil {
		return scope.routingContextOmitted
	}
	routingContexts := acknowledged.RoutingContexts()
	if len(routingContexts) == 0 {
		return false
	}
	for _, routingContext := range routingContexts {
		if _, acknowledgedBefore := scope.routingContexts[routingContext]; !acknowledgedBefore {
			return false
		}
	}
	return true
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

// stopCompetingASPActive stops procedures whose transmitted ASPAC can reactivate
// a withdrawn scope (RFC 4666 §4.3.4.3 and §4.3.4.4). Partial Acks reduce
// remaining, but retries still carry the original message's full scope.
func (c *Association) stopCompetingASPActive(scope *params.Param) {
	var keys []ASKey
	if scope != nil {
		keys = c.managementASKeys(nil, scope)
	}
	cancelled := c.cancelASPActivations(keys, "ASP Down")
	c.reportCancelledActivations(cancelled)
}

// waitTAck waits for one request to be acknowledged, fail, or be cancelled.
func (c *Association) waitTAck(ctx context.Context, req *pendingRequest) error {
	if req == nil {
		return nil
	}
	select {
	case err := <-req.result:
		if failed := req.failure.Load(); failed != nil {
			return *failed
		}
		return err
	case <-ctx.Done():
		c.cancelTAckRequest(req)
		// A claimed withdrawal supplies the cause, but cannot extend ctx
		// while its handler waits for admitted DATA to drain.
		if failed := req.failure.Load(); failed != nil && errors.Is(*failed, ErrASPProcedureSuperseded) {
			return *failed
		}
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
	c.tack.aspDownDeliveryMu.Lock()
	c.tack.mu.Lock()
	c.tack.awaitingAspUp = c.role == RoleASP || c.role == RoleIPSP
	c.tack.peerDownSuperseded = false
	c.tack.aspUpAcknowledged = false
	c.tack.aspDownAcksOutstanding = 0
	c.cancelAllTAckLocked()
	clear(c.tack.acknowledgedASPTM)
	c.tack.mu.Unlock()
	c.tack.aspDownDeliveryMu.Unlock()
	c.tack.retryMu.Unlock()
}

func (c *Association) completeRestartASPSM() {
	if c.tack == nil {
		return
	}
	c.tack.mu.Lock()
	c.tack.awaitingAspUp = false
	c.tack.peerDownSuperseded = false
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
