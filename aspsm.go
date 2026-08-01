// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func (c *Conn) initiateASPSM() error {
	// RFC 4666 Section 4.3.4.1: "When the ASP sends an ASP Up message, it
	// starts timer T(ack)", and resends until the Ack arrives. Without that, an
	// ASP Up lost in transit strands the association: we wait for an Ack that
	// will never come while the SGP waits for a request it never saw.
	aspUp := messages.NewAspUp(c.cfg.AspIdentifier.Copy(), nil)
	request := c.startTAck(aspUp, requestAspUp)
	if _, err := c.WriteSignal(aspUp); err != nil {
		c.cancelTAckRequest(request)
		return err
	}

	return nil
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
// All of those rules are SGP procedures: "The ASP is always the initiator of
// the ASP Up message", so an ASP can never legitimately receive one, and ASP
// Up Ack is a message only an SGP may send. An ASP therefore reports the
// Error and holds its state instead of acking.
func (c *Conn) handleAspUp(aspUp *messages.AspUp) error {

	// RFC 4666 Section 4.7: "Upon receiving an ASP Up message while isolated
	// from the NIF, the SGP should respond with an Error ("Refused -
	// Management Blocking")." There is nothing to bring up: the SGP cannot
	// reach the SS7 network at all.
	if c.nif.isolatedEntirely() {
		return ErrManagementBlocking
	}
	if c.receivedStreamID() != 0 {
		return NewInvalidSCTPStreamIDError(c.receivedStreamID())
	}

	if c.mode != modeServer {
		return NewUnexpectedMessageError(aspUp)
	}
	if aspUp.AspIdentifier != nil {
		if aspUp.AspIdentifier.Tag != params.AspIdentifier || len(aspUp.AspIdentifier.Data) != 4 {
			return ErrInvalidParameterValue
		}
	}
	if err := c.resolveASPAuthorization(aspUp.AspIdentifier); err != nil {
		return err
	}
	if c.as != nil {
		c.as.restrictASP(c)
	}
	if aspUp.AspIdentifier != nil {
		if c.as != nil && !c.hasExplicitlyEmptyASPAuthorization() &&
			!c.as.claimASPIdentifier(c, aspUp.AspIdentifier.AspIdentifier()) {
			return ErrInvalidAspIdentifier
		}
		c.savePeerASPIdentifier(aspUp.AspIdentifier)
		if c.as != nil {
			c.as.refreshASPOrdering(c)
		}
	}

	if _, err := c.WriteSignal(
		messages.NewAspUpAck(
			c.cfg.AspIdentifier.Copy(),
			nil,
		),
	); err != nil {
		return err
	}

	// Only ASP-ACTIVE additionally warrants an Error ("Unexpected Message").
	// The caller keeps the resulting state at ASP-INACTIVE either way.
	if c.State() == StateAspActive {
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
// That clause is scoped to the ASP. An SGP never sends ASP Up, so it can never
// legitimately receive an ASP Up Ack in any state: it reports the Error and
// the dispatcher holds its state.
func (c *Conn) handleAspUpAck(aspUpAck *messages.AspUpAck) error {
	// Validated first: a message that arrived on a stream it is not allowed to
	// use is rejected before anything acts on it, and in particular before it
	// can retire the T(ack) of a request it is not a valid answer to.
	if c.receivedStreamID() != 0 {
		return NewInvalidSCTPStreamIDError(c.receivedStreamID())
	}

	// The request this answers is complete: stop resending it (T(ack)). Whether
	// one was outstanding decides what follows.
	solicited := c.stopTAck(requestAspUp)

	if c.mode != modeClient {
		return NewUnexpectedMessageError(aspUpAck)
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

	if c.State() == StateAspActive {
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

// handleAspDown handles an incoming ASP Down.
//
// Per RFC 4666 Section 4.3.4.2, "The SGP MUST send an ASP Down Ack message in
// response to a received ASP Down message from the ASP even if the ASP is
// already marked as ASP-DOWN at the SGP." As with ASP Up, withholding the Ack
// leaves the peer retransmitting ASP Down until T(ack) expires, indefinitely,
// so the Ack is written unconditionally.
//
// Like ASP Up, ASP Down travels only from ASP to SGP and ASP Down Ack is the
// SGP's response, so an ASP that receives an ASP Down reports the Error and
// holds its state instead of acking.
func (c *Conn) handleAspDown(aspDown *messages.AspDown) error {
	if c.receivedStreamID() != 0 {
		return NewInvalidSCTPStreamIDError(c.receivedStreamID())
	}

	if c.mode != modeServer {
		return NewUnexpectedMessageError(aspDown)
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
	c.commitState(StateAspDown)
	postAckNotify := func() {}
	if c.as != nil {
		postAckNotify = c.as.quiesceASPDown(c)
	}
	c.quiesceUnscopedTraffic()

	_, ackErr := c.WriteSignal(messages.NewAspDownAck(nil))
	postAckNotify()
	return ackErr
}

// handleAspDownAck validates the ASP Down Ack that answers our ASP Down.
//
// Like the other Acks this travels SGP to ASP (RFC 4666 Section 4.3.4.2), so an
// SGP that receives one reports an Error rather than acting on it.
func (c *Conn) handleAspDownAck(aspDownAck *messages.AspDownAck) error {
	// Validated first, as in handleAspUpAck. This one matters most: below, an
	// unsolicited ASP Down Ack takes the ASP down, so accepting one that
	// arrived on a data stream would let a peer drop an active link with a
	// message it was never allowed to send there.
	if c.receivedStreamID() != 0 {
		return NewInvalidSCTPStreamIDError(c.receivedStreamID())
	}

	solicited := c.stopTAck(requestAspDown)

	if c.mode != modeClient {
		return NewUnexpectedMessageError(aspDownAck)
	}
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
	case StateAspInactive, StateAspActive:
		c.setResumeTo(st)
		return NewUnexpectedMessageError(aspDownAck)
	}
	return nil
}

func (c *Conn) initiateASPDown() (*pendingRequest, error) {
	aspDown := messages.NewAspDown(nil)
	request := c.startTAck(aspDown, requestAspDown)
	if _, err := c.WriteSignal(aspDown); err != nil {
		c.cancelTAckRequest(request)
		return nil, err
	}
	return request, nil
}
