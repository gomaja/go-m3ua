package m3ua

import (
	"bytes"
	"fmt"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// aspAcknowledgementState is a dispatch-time snapshot, not request correlation.
// In DE the locally initiated exchange controls TrafficToLocal; shared ASPSM
// also withdraws TrafficToPeer (RFC 4666 §§4.3.4.1.2, 5.6.2).
type aspAcknowledgementState struct {
	previous State
	ipsp     IPSPState
	keys     []ASKey
}

func (c *Association) acknowledgementState(shared bool) aspAcknowledgementState {
	before := aspAcknowledgementState{previous: c.localASPProcedureState()}
	if c.role == RoleIPSP {
		before.ipsp = c.IPSPState()
	}
	if before.previous == StateASPActive {
		for _, key := range c.configuredLocalASKeysForStatus() {
			active := c.contextlessASAcked()
			if key.RoutingContextSet {
				active = c.routingContextAcked(key.RoutingContext) && !c.routingContextOverridden(key.RoutingContext)
			}
			if c.role == RoleIPSP && !c.isIPSPDoubleExchange() {
				active = c.activeForASKey(key)
			}
			if active {
				before.keys = append(before.keys, key)
			}
		}
	}
	if shared && c.isIPSPDoubleExchange() && c.usesSingleASPSMExchange() && before.ipsp.TrafficToPeer == StateASPActive {
		for _, key := range c.configuredASKeys() {
			if c.activeForASKey(key) {
				before.keys = append(before.keys, key)
			}
		}
	}
	before.keys = uniqueASKeys(before.keys)
	return before
}

func (c *Association) reportASPAcknowledgement(kind ManagementIndicationKind, before aspAcknowledgementState, keys []ASKey, description string) {
	select {
	case <-c.done:
		return
	default:
	}
	c.notifyManagement(&ManagementIndication{Kind: kind, PreviousState: before.previous, PreviousIPSPState: before.ipsp, ASKeys: keys, RoutingContexts: inactiveAckRoutingContexts(keys), Description: description})
}

// Stop local retries before admitting the peer's withdrawal. The existing
// transient ASPSM withdrawal fence also rejects registrations during this
// snapshot/commit interval; it is released before publishing the LM report.
func (c *Association) beginAcknowledgementWithdrawal(down bool, cause string) []*pendingRequest {
	if c.tack == nil {
		return nil
	}
	c.tack.mu.Lock()
	c.tack.aspWithdrawalHandling = true
	var stopped []*pendingRequest
	for message, request := range c.tack.pending {
		if request.kind != requestAspActive && (!down || (request.kind != requestAspUp && request.kind != requestAspInactive)) {
			continue
		}
		failure := fmt.Errorf("%w by %s", ErrASPProcedureSuperseded, cause)
		request.failure.Store(&failure)
		request.stopRetries()
		delete(c.tack.pending, message)
		stopped = append(stopped, request)
	}
	c.tack.mu.Unlock()
	// A stopped queued copy never enters the transport; an entered copy must
	// leave the shared write fence before the Error/withdrawal is published.
	c.tack.retryMu.Lock()
	for _, request := range stopped {
		request.joinControlWrite()
	}
	c.tack.retryMu.Unlock()
	return stopped
}

func (c *Association) endAcknowledgementWithdrawal() {
	if c.tack != nil {
		c.tack.mu.Lock()
		c.tack.aspWithdrawalHandling = false
		c.tack.mu.Unlock()
	}
}

func (c *Association) finishAcknowledgementWithdrawal(stopped []*pendingRequest) {
	var active []*pendingRequest
	for _, request := range stopped {
		if request.kind == requestAspActive {
			active = append(active, request)
		} else {
			request.finish(*request.failure.Load())
		}
	}
	c.reportCancelledActivations(active)
}

// RFC 4666 §4.3.4.1: an unexpected Up Ack "should consider itself in the
// ASP-INACTIVE state"; outside INACTIVE it "SHOULD send an Error message and
// then initiate procedures to return itself to its previous state". This
// library deliberately delegates that SHOULD return to Layer Management.
// §4.3.4.3 permits an "M-ASP_ACTIVE request primitive from Layer Management";
// §4.3.4.1 says "all registered Routing Keys are considered deregistered".
// The application must re-register dynamic keys, then ASPActive the reported
// keys. Automatic startup policy is not a recovery policy.
func (c *Association) handleUnowedASPUpAck(ack *messages.AspUpAck, raw []byte) (State, error) {
	before := c.acknowledgementState(true)
	sharedPeerActive := c.isIPSPDoubleExchange() && c.usesSingleASPSMExchange() && before.ipsp.TrafficToPeer != StateASPInactive
	if before.previous == StateASPInactive && !sharedPeerActive {
		return stateUnchanged, nil
	}
	stopped := c.beginAcknowledgementWithdrawal(false, "unsolicited ASP Up Ack")
	var submissionErr error
	defer func() {
		if submissionErr != nil {
			// No withdrawal indication may precede its required Error submission.
			// Release callers with that independent write failure, never an
			// unpublished retirement cause or accepted-cancellation warning.
			for _, request := range stopped {
				request.finish(submissionErr)
			}
			return
		}
		c.finishAcknowledgementWithdrawal(stopped)
	}()
	defer c.endAcknowledgementWithdrawal()
	submissionErr = c.submitUnowedUpAckError(ack, raw)
	publication := StateASPInactive
	if c.isIPSPDoubleExchange() {
		if c.usesSingleASPSMExchange() {
			c.commitInactiveAckState(StateASPInactive)
			c.quiesceAcknowledgementPeer()
		} else {
			publication = stateUnchanged
		}
		c.noteNoRoutingContextsAcked()
		c.commitLocalIPSPState(StateASPInactive)
		c.quiesceLocalIPSPSSNMTraffic()
	} else {
		c.commitInactiveAckState(StateASPInactive)
		c.noteNoRoutingContextsAcked()
		if c.role == RoleIPSP {
			c.quiesceAcknowledgementPeer()
		}
	}
	c.endAcknowledgementWithdrawal()
	if submissionErr != nil {
		return publication, submissionErr
	}
	c.reportASPAcknowledgement(ManagementASPWithdrawal, before, before.keys, fmt.Sprintf("unsolicited ASP Up Ack withdrew service (previous local state %s); recovery is an application decision: re-register dynamic Routing Keys, then request ASP Active for the reported ASKeys", before.previous))
	return publication, nil
}

func (c *Association) quiesceAcknowledgementPeer() {
	c.noteRoutingContextsInactive(nil)
	notify := func() {}
	if c.as != nil {
		notify = c.as.quiesceASPKeysInactive(c, c.configuredASKeys())
	}
	c.quiesceUnscopedTraffic()
	notify()
}

// RFC 4666 §4.3.4.1.2 permits IPSP "ASP-DOWN state after an ASP Down or
// ASP Down Ack has been received from it"; §4.3.4.2 removes service "in all
// Application Servers". Mark existing memberships DOWN before the indication,
// unlike Up withdrawal's INACTIVE marking (§4.3.4.1).
func (c *Association) quiesceAcknowledgementPeerDown() {
	c.noteRoutingContextsInactive(nil)
	notify := func() {}
	if c.as != nil {
		notify = c.as.quiesceASPDown(c)
	}
	c.quiesceUnscopedTraffic()
	notify()
}

// RFC 4666 §4.3.4.2 uses lowercase "should" for returning to the old state.
// §3.8.1 says Unexpected Message Error "MAY be sent". Adopt DOWN without an
// Error or recovery request, and give the prior exact scope to the application.
func (c *Association) handleUnsolicitedASPDownAck() {
	before := c.acknowledgementState(true)
	stopped := c.beginAcknowledgementWithdrawal(true, "unsolicited ASP Down Ack")
	defer c.finishAcknowledgementWithdrawal(stopped)
	defer c.endAcknowledgementWithdrawal()
	if c.isIPSPDoubleExchange() {
		c.commitLocalIPSPState(StateASPDown)
		c.noteNoRoutingContextsAcked()
		c.quiesceLocalIPSPSSNMTraffic()
		if c.usesSingleASPSMExchange() {
			c.commitPeerDownState()
			c.quiesceAcknowledgementPeerDown()
		}
	} else {
		c.commitPeerDownState()
		c.noteNoRoutingContextsAcked()
		if c.role == RoleIPSP {
			c.quiesceAcknowledgementPeerDown()
		}
	}
	c.endAcknowledgementWithdrawal()
	c.reportASPAcknowledgement(ManagementASPWithdrawal, before, before.keys, fmt.Sprintf("unsolicited ASP Down Ack withdrew service (previous local state %s); recovery is an application decision", before.previous))
}

// RFC 4666 §3.8.1 says Unexpected Message Error "MAY be sent". Retain the
// validated grant (§4.3.4.3) without an Error or an inferred request identity.
// Cancellation or T(ack) expiry cannot retract an ASP Active already received
// by the peer; whether to retain the activation is an application decision.
func (c *Association) reportUnsolicitedASPActivation(ack pendingTAckAcknowledgement, before aspAcknowledgementState, scope *params.Param) {
	if ack.solicited {
		return
	}
	c.reportASPAcknowledgement(ManagementASPActivation, before, c.managementASKeys(nil, scope), "unsolicited ASP Active Ack reported these Application Servers active; retaining activation or requesting ASP Inactive is an application decision")
}

func (c *Association) submitUnowedUpAckError(ack *messages.AspUpAck, raw []byte) error {
	fault := NewUnexpectedMessageError(ack)
	fault.Raw = bytes.Clone(raw)
	return c.handleErrors(&receivedMessageError{Message: ack, Err: fault})
}
