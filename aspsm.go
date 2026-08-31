// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func (c *Association) initiateASPSM() error {
	_, err := c.beginASPSM()
	return err
}

func (c *Association) beginASPSM() (*pendingRequest, error) {
	// RFC 4666 Section 4.3.4.1: "When the ASP sends an ASP Up message, it
	// starts timer T(ack)", and resends until the Ack arrives. Without that, an
	// ASP Up lost in transit strands the association: we wait for an Ack that
	// will never come while the SGP waits for a request it never saw.
	aspUp := messages.NewAspUp(c.cfg.ASPIdentifier.Copy(), nil)
	request := c.startTAck(aspUp, requestAspUp)
	if _, err := c.WriteSignal(aspUp); err != nil {
		c.cancelTAckRequest(request)
		return nil, err
	}

	return request, nil
}

// handleAspUp handles an incoming ASP Up.
//
// Per RFC 4666 Section 4.3.4.1, an ASP Up Ack MUST be sent in response to a
// received ASP Up regardless of the state the remote ASP is currently in:
//
//   - ASP-DOWN: the ASP Up Ack moves the remote ASP to ASP-INACTIVE.
//   - ASP-INACTIVE: "an ASP Up Ack message is returned, and no further action
//     is taken."
//   - ASP-ACTIVE: "an ASP Up Ack message is returned, as well as an Error
//     message ("Unexpected Message")", and the remote ASP falls back to
//     ASP-INACTIVE.
//
// Withholding the Ack would leave the peer retransmitting ASP Up until T(ack)
// expires, indefinitely.
//
// Those are the SG-AS procedures: "The ASP is always the initiator of the ASP
// Up message", so an ASP cannot legitimately receive one. RFC 4666 Section
// 4.3.4.1.2 separately permits an IPSP to receive ASP Up and consider the
// remote IPSP ASP-INACTIVE after the request or acknowledgement.
func (c *Association) handleAspUp(aspUp *messages.AspUp) error {
	if c.role != RoleSGP && c.role != RoleIPSP {
		return NewUnexpectedMessageError(aspUp)
	}

	// RFC 4666 Section 4.7: "Upon receiving an ASP Up message while isolated
	// from the NIF, the SGP should respond with an Error ("Refused -
	// Management Blocking")." There is nothing to bring up: the SGP cannot
	// reach the SS7 network at all.
	if c.role == RoleSGP && c.nif.isolatedEntirely() {
		return ErrManagementBlocking
	}
	if c.receivedStreamID() != 0 {
		return NewInvalidSCTPStreamIDError(c.receivedStreamID())
	}

	if aspUp.AspIdentifier != nil {
		if aspUp.AspIdentifier.Tag != params.AspIdentifier || len(aspUp.AspIdentifier.Data) != 4 {
			return ErrInvalidParameterValue
		}
	}
	if c.role == RoleSGP {
		if err := c.resolveASPAuthorization(aspUp.AspIdentifier); err != nil {
			return err
		}
		if c.as != nil {
			c.as.restrictASP(c)
		}
	}
	if err := c.claimPeerASPIdentifier(aspUp.AspIdentifier); err != nil {
		return err
	}

	previousState := c.State()
	if c.isIPSPDoubleExchange() {
		return c.handleAspUpDoubleExchange(previousState, aspUp)
	}

	// RFC 4666 Section 4.3.4.1 makes a received ASP Up establish the remote
	// ASP/IPSP as ASP-INACTIVE. If it was ASP-ACTIVE, the same section removes
	// it from every relevant AS. Commit that state and halt traffic before the
	// Ack: once the peer receives ASP Up Ack, both ends may act on the new
	// state, so no DATA admitted under the previous state may follow it.
	c.commitState(StateASPInactive)
	c.noteRoutingContextsInactive(nil)
	postAckNotify := func() {}
	if c.as != nil {
		postAckNotify = c.as.quiesceASPFor(c, c.configuredRoutingContexts())
	}
	c.quiesceUnscopedTraffic()

	ackErr := c.writeMandatoryControls([]messages.M3UA{
		messages.NewAspUpAck(
			c.cfg.ASPIdentifier.Copy(),
			nil,
		),
	}, false, true)
	postAckNotify()
	if ackErr != nil {
		return ackErr
	}
	if c.role == RoleIPSP {
		c.completeRestartASPSM()
	}

	// Only ASP-ACTIVE additionally warrants an Error ("Unexpected Message").
	// The caller keeps the resulting state at ASP-INACTIVE either way.
	if c.role == RoleSGP && previousState == StateASPActive {
		return NewUnexpectedMessageError(aspUp)
	}

	return nil
}

func (c *Association) handleAspUpDoubleExchange(previousState State, aspUp *messages.AspUp) error {
	startLocalASPTM := false
	if c.usesSingleASPSMExchange() {
		previousLocalState := c.localIPSPStateValue()
		c.noteNoRoutingContextsAcked()
		c.commitLocalIPSPState(StateASPInactive)
		c.quiesceLocalIPSPSSNMTraffic()
		startLocalASPTM = previousLocalState == StateASPDown &&
			c.aspProcedureMode(aspProcedureActive) == ASPProcedureAutomatic &&
			!c.terminating.Load()
	}
	c.commitState(StateASPInactive)
	c.noteRoutingContextsInactive(nil)
	postAckNotify := func() {}
	if c.as != nil {
		postAckNotify = c.as.quiesceASPFor(c, c.configuredRoutingContexts())
	}
	c.quiesceUnscopedTraffic()

	ackErr := c.writeMandatoryControls([]messages.M3UA{
		messages.NewAspUpAck(c.cfg.ASPIdentifier.Copy(), nil),
	}, false, true)
	postAckNotify()
	if ackErr != nil {
		return ackErr
	}
	if c.usesSingleASPSMExchange() {
		c.completeRestartASPSM()
		if startLocalASPTM {
			return c.initiateASPTM()
		}
	}
	if previousState == StateASPActive {
		return NewUnexpectedMessageError(aspUp)
	}
	return nil
}

// handleAspUpAck handles an incoming ASP Up Ack.
//
// Per RFC 4666 Section 4.3.4.1, "If the ASP receives an unexpected ASP Up Ack
// message, the ASP should consider itself in the ASP-INACTIVE state. If the ASP
// was not in the ASP-INACTIVE state, it SHOULD send an Error message and then
// initiate procedures to return itself to its previous state."
//
// The move to ASP-INACTIVE therefore happens even when the Ack is unexpected,
// so both ends converge instead of holding conflicting views of the state.
//
// That clause is scoped to the ASP. An SGP never sends ASP Up, so it cannot
// legitimately receive ASP Up Ack. RFC 4666 Section 4.3.4.1.2 separately
// permits an IPSP to receive the acknowledgement and consider the remote IPSP
// ASP-INACTIVE.
func (c *Association) handleAspUpAck(aspUpAck *messages.AspUpAck) error {
	// Validated first: a message that arrived on a stream it is not allowed to
	// use is rejected before anything acts on it, and in particular before it
	// can retire the T(ack) of a request it is not a valid answer to.
	if c.receivedStreamID() != 0 {
		return NewInvalidSCTPStreamIDError(c.receivedStreamID())
	}
	if c.role == RoleIPSP && aspUpAck.AspIdentifier != nil &&
		(aspUpAck.AspIdentifier.Tag != params.AspIdentifier || len(aspUpAck.AspIdentifier.Data) != 4) {
		return ErrInvalidParameterValue
	}
	if c.role == RoleIPSP {
		if err := c.claimPeerASPIdentifier(aspUpAck.AspIdentifier); err != nil {
			return err
		}
	}

	// The request this answers is complete: stop resending it (T(ack)). Whether
	// one was outstanding decides what follows.
	solicited := c.stopTAck(requestAspUp)

	if c.role != RoleASP && c.role != RoleIPSP {
		return NewUnexpectedMessageError(aspUpAck)
	}
	// RFC 4666 Section 4.3.4.1 permits ASP Up retransmission under T(ack), and
	// the peer answers every copy. Once this SCTP epoch completed the procedure,
	// a later Ack is that request's delayed duplicate rather than a new state
	// transition.
	if !solicited && c.isRepeatedASPUpAcknowledgement() {
		return nil
	}
	if c.role == RoleIPSP {
		if c.isIPSPDoubleExchange() {
			previousLocalState := c.localIPSPStateValue()
			if c.usesSingleASPSMExchange() {
				c.commitState(StateASPInactive)
				c.noteRoutingContextsInactive(nil)
				postTransitionNotify := func() {}
				if c.as != nil {
					postTransitionNotify = c.as.quiesceASPFor(c, c.configuredRoutingContexts())
				}
				c.quiesceUnscopedTraffic()
				postTransitionNotify()
			}
			c.noteNoRoutingContextsAcked()
			c.commitLocalIPSPState(StateASPInactive)
			c.quiesceLocalIPSPSSNMTraffic()
			if previousLocalState == StateASPDown &&
				c.aspProcedureMode(aspProcedureActive) == ASPProcedureAutomatic &&
				!c.terminating.Load() {
				return c.initiateASPTM()
			}
			return nil
		}
		// RFC 4666 Section 3.5.2 makes the optional ASP Identifier in ASP Up
		// Ack specifically useful for IPSP communication: the answering IPSP
		// may identify itself independently of the initiator's ASP Up.
		// RFC 4666 Section 4.3.4.1.2 permits the receiving IPSP to consider
		// its peer ASP-INACTIVE when the ASP Up Ack arrives. In Single Exchange
		// a simultaneous procedure can already have made this association active,
		// so commit the inactive state and drain traffic here, before returning
		// to the dispatcher. Otherwise a concurrent DATA write can be admitted
		// after the peer has sent the acknowledgement that ended the ASPSM
		// procedure.
		c.commitState(StateASPInactive)
		c.noteRoutingContextsInactive(nil)
		postTransitionNotify := func() {}
		if c.as != nil {
			postTransitionNotify = c.as.quiesceASPFor(c, c.configuredRoutingContexts())
		}
		c.quiesceUnscopedTraffic()
		postTransitionNotify()
		return nil
	}

	// An Ack that answers an ASP Up this node actually sent is not "an
	// unexpected ASP Up Ack" in the sense of Section 4.3.4.1, whatever state the
	// answer happens to arrive in. T(ack) resends an unacknowledged ASP Up, so
	// under load the association can already have climbed to ASP-ACTIVE by the
	// time the retransmission is answered — and treating that answer as a stray
	// made the two ends oscillate: this node dropped to ASP-INACTIVE and
	// re-activated, the SGP dropped it again on the next retransmission, and so
	// on. Absorb it instead: the request it answers is complete either way.
	if solicited {
		return nil
	}

	if c.State() == StateASPActive {
		// Section 4.3.4.1: "If the ASP receives an unexpected ASP Up Ack
		// message, the ASP should consider itself in the ASP-INACTIVE state.
		// If the ASP was not in the ASP-INACTIVE state, it SHOULD send an Error
		// message and then initiate procedures to return itself to its previous
		// state." The Error and the drop to ASP-INACTIVE follow from returning
		// this; the return to ASP-ACTIVE is armed here, because the entry
		// action cannot otherwise tell this cause apart from the peer
		// deliberately taking traffic away, where resuming would be wrong.
		c.armResumeAfterStrayAck()
		return NewUnexpectedMessageError(aspUpAck)
	}

	return nil
}

func (c *Association) claimPeerASPIdentifier(identifier *params.Param) error {
	if identifier == nil {
		return nil
	}
	if c.as != nil && (c.role == RoleIPSP || !c.hasExplicitlyEmptyASPAuthorization()) &&
		!c.as.claimASPIdentifier(c, identifier.AspIdentifier()) {
		return ErrInvalidASPIdentifier
	}
	c.savePeerASPIdentifier(identifier)
	if c.as != nil {
		c.as.refreshASPOrdering(c)
	}
	return nil
}

// handleAspDown handles an incoming ASP Down.
//
// Per RFC 4666 Section 4.3.4.2, "The SGP MUST send an ASP Down Ack message in
// response to a received ASP Down message from the ASP even if the ASP is
// already marked as ASP-DOWN at the SGP." As with ASP Up, withholding the Ack
// leaves the peer retransmitting ASP Down until T(ack) expires, indefinitely,
// so the Ack is written unconditionally.
//
// In the SG-AS model ASP Down travels from ASP to SGP and ASP Down Ack is the
// SGP's response. RFC 4666 Section 4.3.4.1.2 also permits either message from
// an IPSP and allows the receiving IPSP to consider the remote IPSP ASP-DOWN.
func (c *Association) handleAspDown(aspDown *messages.AspDown) error {
	if c.receivedStreamID() != 0 {
		return NewInvalidSCTPStreamIDError(c.receivedStreamID())
	}

	if c.role != RoleSGP && c.role != RoleIPSP {
		return NewUnexpectedMessageError(aspDown)
	}
	if c.isIPSPDoubleExchange() {
		return c.handleAspDownDoubleExchange()
	}

	// ASP Down carries only an optional INFO String (RFC 4666 Section 3.5.3),
	// so there is nothing further to validate: any parameter combination is
	// well formed, and the Ack is owed regardless of state.
	//
	// Section 4.3.4.2 orders the state change before the Ack: "The SGP marks
	// the ASP as ASP-DOWN, informs Layer Management with an M-ASP_Down
	// indication primitive, and returns an ASP Down Ack message to the ASP."
	// Commit ASP-DOWN before closing its AS traffic gates: direct write paths
	// must stop admitting DATA too. Mark every AS and drain DATA that already
	// selected this ASP, but hold the resulting AS-state Notify messages until
	// the Ack is on the wire. The dispatcher publishes the association-level
	// entry action after this returns.
	c.commitState(StateASPDown)
	postAckNotify := func() {}
	if c.as != nil {
		postAckNotify = c.as.quiesceASPDown(c)
	}
	c.quiesceUnscopedTraffic()

	ackErr := c.writeMandatoryControls([]messages.M3UA{
		messages.NewAspDownAck(nil),
	}, false, true)
	postAckNotify()
	return ackErr
}

func (c *Association) handleAspDownDoubleExchange() error {
	c.commitState(StateASPDown)
	if c.usesSingleASPSMExchange() {
		c.commitLocalIPSPState(StateASPDown)
		c.noteNoRoutingContextsAcked()
	}
	postAckNotify := func() {}
	if c.as != nil {
		postAckNotify = c.as.quiesceASPDown(c)
	}
	c.quiesceUnscopedTraffic()
	if c.usesSingleASPSMExchange() {
		c.quiesceLocalIPSPSSNMTraffic()
	}
	ackErr := c.writeMandatoryControls([]messages.M3UA{
		messages.NewAspDownAck(nil),
	}, false, true)
	postAckNotify()
	return ackErr
}

// handleAspDownAck validates the ASP Down Ack that answers our ASP Down.
//
// In the SG-AS model this travels SGP to ASP (RFC 4666 Section 4.3.4.2), so an
// SGP that receives one reports an Error. RFC 4666 Section 4.3.4.1.2 also
// permits an IPSP to receive it and consider the remote IPSP ASP-DOWN.
func (c *Association) handleAspDownAck(aspDownAck *messages.AspDownAck) error {
	// Validated first, as in handleAspUpAck. This one matters most: below, an
	// unsolicited ASP Down Ack takes the ASP down, so accepting one that
	// arrived on a data stream would let a peer drop an active link with a
	// message it was never allowed to send there.
	if c.receivedStreamID() != 0 {
		return NewInvalidSCTPStreamIDError(c.receivedStreamID())
	}

	if c.role != RoleASP && c.role != RoleIPSP {
		return NewUnexpectedMessageError(aspDownAck)
	}
	if c.role == RoleIPSP {
		if c.isIPSPDoubleExchange() {
			acknowledgement := c.claimTAckAcknowledgement(requestAspDown, nil)
			c.commitLocalIPSPState(StateASPDown)
			c.noteNoRoutingContextsAcked()
			c.quiesceLocalIPSPSSNMTraffic()
			if c.usesSingleASPSMExchange() {
				c.commitState(StateASPDown)
				postTransitionNotify := func() {}
				if c.as != nil {
					postTransitionNotify = c.as.quiesceASPDown(c)
				}
				c.quiesceUnscopedTraffic()
				postTransitionNotify()
			}
			acknowledgement.complete()
			return nil
		}
		// Claim the acknowledgement first to stop retransmissions, but do not
		// release a Shutdown or other T(ack) waiter until the peer is ASP-DOWN
		// and all traffic admitted under its earlier state has drained.
		acknowledgement := c.claimTAckAcknowledgement(requestAspDown, nil)
		c.commitState(StateASPDown)
		postTransitionNotify := func() {}
		if c.as != nil {
			postTransitionNotify = c.as.quiesceASPDown(c)
		}
		c.quiesceUnscopedTraffic()
		postTransitionNotify()
		acknowledgement.complete()
		return nil
	}

	solicited := c.stopTAck(requestAspDown)
	if solicited {
		return nil
	}
	// RFC 4666 Section 4.3.4.2: "If the ASP receives an ASP Down Ack without
	// having sent an ASP Down message, the ASP should now consider itself to be
	// in the ASP-DOWN state. If the ASP was previously in the ASP-ACTIVE or
	// ASP-INACTIVE state, the ASP should then initiate procedures to return
	// itself to its previous state."
	//
	// The Error is still owed — the message was unexpected — but the ASP must
	// go down with it, or the two ends disagree about whether the link carries
	// traffic. The caller publishes ASP-DOWN, whose entry action starts the
	// climb back; resumeTo records how far.
	switch st := c.State(); st {
	case StateASPInactive, StateASPActive:
		c.setResumeTo(st)
		return NewUnexpectedMessageError(aspDownAck)
	}
	return nil
}

func (c *Association) initiateASPDown() (*pendingRequest, error) {
	aspDown := messages.NewAspDown(nil)
	request := c.startTAck(aspDown, requestAspDown)
	if _, err := c.WriteSignal(aspDown); err != nil {
		c.cancelTAckRequest(request)
		return nil, err
	}
	return request, nil
}
