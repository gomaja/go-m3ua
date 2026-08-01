// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

// State represents ASP State.
type State uint8

// M3UA status definitions.
const (
	StateAspDown State = iota
	StateAspInactive
	StateAspActive
	StateSCTPCDI
	StateSCTPRI

	// stateUnchanged is published by a handler that did not move the ASP state
	// machine, in place of re-reading the current state.
	//
	// Re-reading it was a stale snapshot. The dispatcher and the state machine
	// run on different goroutines with a queue between them, so c.State() at
	// dispatch time is the last state *applied*, not the last one *published*:
	// a message handled while an earlier transition was still in the queue
	// republished the state that transition had already superseded. Applying
	// the stale value afterwards re-ran that state's entry action — for
	// ASP-DOWN, re-sending ASP Up on an association that was already
	// ASP-ACTIVE, which the SGP answered by dropping the ASP to ASP-INACTIVE
	// per Section 4.3.4.1. The association ended up inactive with no message
	// having gone wrong.
	//
	// It stays a published value rather than publishing nothing so that every
	// handled message still accounts for exactly one state update.
	stateUnchanged
)

func (s State) String() string {
	switch s {
	case StateAspDown:
		return "AspDown"
	case StateAspInactive:
		return "AspInactive"
	case StateAspActive:
		return "AspActive"
	case StateSCTPCDI:
		return "SCTPCDI"
	case StateSCTPRI:
		return "SCTPRI"
	case stateUnchanged:
		return "unchanged"
	default:
		return "Unknown"
	}
}

// handleStateUpdate applies a published state to the Conn and runs the entry
// action for it, if this is an entry rather than a restatement.
//
// Most handlers publish c.State() unchanged to mean "hold". That restatement
// used to re-run the entry action, and the client's ASP-INACTIVE entry action
// is "send ASP Active": a peer answering with a Routing Context we did not ask
// about made the ASP reject the Ack, restate ASP-INACTIVE, and send another ASP
// Active — a self-sustaining storm, unbounded and at wire speed, from a mere
// configuration mismatch. Retransmission is T(ack)'s job and is bounded by
// TAckRetries; the entry action's is to run once per entry.
func (c *Conn) handleStateUpdate(current State) error {
	return c.handleStateUpdateFrom(current, false)
}

// handlePublishedStateUpdate applies a state received from stateChan only if it
// is still the latest state committed by the dispatcher. A handler may commit a
// newer state while monitor is finishing an older entry action; applying that
// older publication afterwards would resurrect an ASP the peer already took
// down.
func (c *Conn) handlePublishedStateUpdate(current State) error {
	return c.handleStateUpdateFrom(current, true)
}

func (c *Conn) handleStateUpdateFrom(current State, published bool) error {
	// Nothing moved: no transition to apply, no entry action to run, and
	// nothing for the Application Server to hear about.
	if current == stateUnchanged {
		return nil
	}

	var (
		applied = true
		err     error
	)
	if published {
		applied, err = c.applyPublishedStateUpdate(current)
	} else {
		err = c.applyStateUpdate(current)
	}
	if !applied {
		return err
	}

	// The Application Server state is derived from its ASPs (RFC 4666 Section
	// 4.3.2), so every applied transition is reported into the registry.
	//
	// Outside the state lock, and that is not incidental. Reporting it writes a
	// Notify to every ASP in the Application Server, and a socket write blocks
	// when the send buffer is full. Holding muState across that stalled every
	// State() call on this association behind a peer that was not reading,
	// which under load looked like an association that never established.
	//
	// After the entry actions, which keeps the ordering Section 4.3.4.5
	// requires: "the Notify message MUST be sent after any related
	// acknowledgement messages (e.g., ASP Up Ack, ASP Down Ack, ASP Active Ack,
	// or ASP Inactive Ack)." Those acknowledgements are written by the handlers,
	// before the state is published at all.
	if c.as != nil {
		if published {
			c.as.aspStateChangedPublished(c, current)
		} else {
			c.as.aspStateChanged(c, current)
		}
	}

	return err
}

// applyStateUpdate records the transition and runs the entry action for it,
// with the state lock held throughout so an entry action sees the state it was
// entered for.
func (c *Conn) applyStateUpdate(current State) error {
	c.muState.Lock()
	defer c.muState.Unlock()
	return c.applyStateUpdateLocked(current)
}

// applyPublishedStateUpdate atomically rejects a queued publication when a
// newer dispatcher decision has already replaced it in c.state.
func (c *Conn) applyPublishedStateUpdate(current State) (bool, error) {
	c.muState.Lock()
	defer c.muState.Unlock()
	if c.state != current {
		return false, nil
	}
	return true, c.applyStateUpdateLocked(current)
}

func (c *Conn) applyStateUpdateLocked(current State) error {
	select {
	case <-c.done:
		// closeWith is the final authority once teardown begins. A state update
		// already queued before done closed must not resurrect the association.
		return nil
	default:
	}

	// The state being moved away from comes from appliedState rather than from
	// state, because sendState has already committed the new value to state by
	// the time this runs. Reading state here would compare a transition against
	// itself, make every transition look like a restatement, and silently stop
	// every entry action -- the handshake among them.
	previous := c.appliedState
	if !c.stateEntered {
		// Nothing has been applied yet, so appliedState holds only its zero
		// value and carries no information. state is the honest answer on the
		// first pass, and is also what a Conn placed directly into a state --
		// by a test, or by Accept before the first publish -- was set to.
		previous = c.state
	}
	// The first update is always an entry. The state starts at its zero value,
	// which is StateAspDown, so the ASP-DOWN entry action that starts the
	// handshake would otherwise look like a restatement and never run.
	entering := current != previous || !c.stateEntered
	c.stateEntered = true
	c.appliedState = current
	if entering {
		// Reported here rather than in handleStateUpdate because this is where
		// a genuine entry is distinguishable from a restatement of the state
		// the association is already in; StateChanges() promises edges, and a
		// caller counting them must not see one that did not happen. The send
		// is non-blocking and takes a different lock, so holding muState across
		// it is safe.
		c.notifyStateChange(current)
	}
	// Still written here as well as in sendState: applyStateUpdate is reached
	// directly, without a publish, by callers that place a Conn in a state
	// outright, and it must remain correct for them.
	c.state = current

	switch c.mode {
	case modeClient:
		if err := c.handleStateUpdateAsClient(current, previous, entering); err != nil {
			return err
		}
		return nil
	case modeServer:
		if err := c.handleStateUpdateAsServer(current, entering); err != nil {
			return err
		}
		return nil
	default:
		// Only client and server modes exist, so this is unreachable today. It
		// is kept explicit rather than silently falling through to the client
		// path: IPSP (RFC 4666 Section 1.4.3.4) is the same procedures with
		// symmetric roles, so adding it means adding a mode here, and a missed
		// arm would otherwise run an IPSP through the ASP state machine.
		return fmt.Errorf("%w: mode %d", ErrUnsupportedMode, c.mode)
	}
}

func (c *Conn) handleStateUpdateAsClient(current, previous State, entering bool) error {
	switch current {
	case StateAspDown:
		// A fresh climb: nothing the peer acknowledged before survives the
		// association going down, which matches Section 4.3.4.1's treatment of
		// an ASP Up while ASP-ACTIVE, where "all registered Routing Keys are
		// considered deregistered".
		c.forgetAckedRoutingContexts()
		// RFC 4666 Section 4.3.4.1: the ASP is always the initiator of ASP Up.
		if !entering || c.terminating.Load() {
			return nil
		}
		return c.initiateASPSM()
	case StateAspInactive:
		// RFC 4666 Section 4.3.4.3: the ASP asks to carry traffic "Anytime
		// after the ASP has received an ASP Up Ack", so ASP Active follows the
		// climb out of ASP-DOWN. Resending it while already ASP-INACTIVE is
		// T(ack)'s job.
		//
		// Arriving here from ASP-ACTIVE is the opposite situation: the peer has
		// just taken traffic away, through an ASP Inactive, an ASP Inactive Ack
		// answering our own request, or an "Alternate ASP Active" Notify.
		// Re-activating then fights the peer's decision — at an Override-mode
		// AS it takes the traffic straight back off the ASP that overrode us,
		// which overrides it in turn, indefinitely. Section 4.3.4.5 is explicit
		// that a Notify "does not explicitly compel the ASP(s) receiving the
		// message to become active. The ASPs remain in control of what (and
		// when) traffic action is taken." Coming back is the owner's call.
		//
		// The exception is a stray acknowledgement. Section 4.3.4.1 says of one:
		// "If the ASP was not in the ASP-INACTIVE state, it SHOULD send an
		// Error message and then initiate procedures to return itself to its
		// previous state." Nothing was taken away deliberately there, so the
		// return is owed and the ping-pong reasoning above does not apply.
		if entering && c.resumeAfterStrayAck() {
			c.clearResumeAfterStrayAck()
			return c.initiateASPTM()
		}
		if !entering || previous != StateAspDown {
			return nil
		}
		// Section 4.3.4.2 has an ASP put down by a stray ASP Down Ack "return
		// itself to its previous state" — which, if that state was only
		// ASP-INACTIVE, stops here rather than taking traffic it never had.
		if c.resumeTo == StateAspInactive {
			return nil
		}
		return c.initiateASPTM()
	case StateAspActive:
		if entering {
			c.notifyEstablished()
			c.beatAllow.Broadcast()
		}
		return nil
	case StateSCTPCDI, StateSCTPRI:
		return ErrSCTPNotAlive
	default:
		return ErrInvalidState
	}
}

func (c *Conn) handleStateUpdateAsServer(current State, entering bool) error {
	switch current {
	case StateAspDown:
		// The record of which Application Servers this ASP was active in does
		// not survive the association going down. Section 4.3.1's Figure 3
		// reaches ASP-DOWN by ASP Down or SCTP CDI, neither of which is per-AS,
		// so the peer is ASP-DOWN in every AS and must ask again for each.
		if entering {
			c.forgetActiveRoutingContexts()
		}
		return nil
	case StateAspInactive:
		// Likewise: an ASP that has left ASP-ACTIVE altogether is active in no
		// Application Server, and its next ASP Active decides afresh.
		if entering {
			c.forgetActiveRoutingContexts()
		}
		// XXX - send DAVA to notify peer?
		return nil
	case StateAspActive:
		if entering {
			c.notifyEstablished()
			c.beatAllow.Broadcast()
		}
		return nil
	case StateSCTPCDI, StateSCTPRI:
		return ErrSCTPNotAlive
	default:
		return ErrInvalidState
	}
}

// notifyEstablished signals Dial/Accept that the association reached
// ASP-ACTIVE. established is a one-shot, capacity-1 signal that is read exactly
// once at setup, so the send must never block: an association that returns to
// ASP-ACTIVE (for example after RFC 4666 Section 4.3.4.1 drops it to
// ASP-INACTIVE on a received ASP Up) would otherwise wedge handleStateUpdate
// while it holds muState, blocking every State() caller with SCTP still up.
func (c *Conn) notifyEstablished() {
	select {
	case c.established <- struct{}{}:
	default:
	}
}

// notifyBeatAck hands a validated BEAT Ack to the heartbeat goroutine.
//
// beatAckChan has capacity 1 so an Ack that completes its round trip before
// heartbeat() reaches its select — the goroutine can be descheduled between
// writing the BEAT and parking — is buffered rather than lost; a lost token
// would let T(beat) expire and tear down a healthy association. The send must
// still never block: if BEAT is disabled, or heartbeat() has exited on context
// cancellation, or a duplicate Ack arrives while a token is already pending,
// there is nobody to receive. Dropping the surplus token is harmless because
// T(beat) remains the authority on peer liveness, whereas blocking here would
// wedge the dispatch goroutine for this message indefinitely.
func (c *Conn) notifyBeatAck() {
	select {
	case c.beatAckChan <- struct{}{}:
	default:
	}
}

// sendErr safely sends an error on errChan, aborting if the connection is closed.
func (c *Conn) sendErr(err error) {
	select {
	case c.errChan <- err:
	case <-c.done:
	}
}

// sendState commits a transition and then publishes it for its entry actions,
// aborting if the connection is closed.
//
// The commit happens here, synchronously, on the goroutine that dispatched the
// message. That is deliberate and it is what closes a race the split between
// the two used to leave open: the handlers write their acknowledgement from
// inside the handler, and the transition was only recorded later, by monitor(),
// on its own goroutine. A peer on a fast path -- loopback, or simply an idle
// link -- answers the acknowledgement before that happens, and the dispatcher
// then judges the answer against a state this end had already decided to leave.
// At an SGP that meant a perfectly ordinary ASP Active, arriving just after our
// ASP Up Ack, being refused with "Unexpected Message" because the association
// still read as ASP-DOWN. The peer resends on T(ack), is refused again, and
// both ends abandon the association at their establish budget.
//
// Publishing still happens afterwards, so the entry actions and the Notify that
// follows them keep running off the dispatch path: applyStateUpdate writes to
// the socket, and Section 4.3.4.5 requires the Notify to follow the related
// acknowledgement, which it still does.
func (c *Conn) sendState(s State) {
	if !c.commitState(s) {
		return
	}

	select {
	case c.stateChan <- s:
	case <-c.done:
	}
}

// commitState records the dispatcher's decision before it is published for
// entry actions. stateUnchanged is only a publication token and changes no
// state.
func (c *Conn) commitState(s State) bool {
	if s == stateUnchanged {
		return true
	}
	c.muState.Lock()
	defer c.muState.Unlock()
	select {
	case <-c.done:
		return false
	default:
	}
	c.state = s
	return true
}

func (c *Conn) handleSignals(ctx context.Context, m3 messages.M3UA) {
	c.handleReceivedSignals(ctx, m3, nil)
}

// handleReceivedSignals handles a decoded message together with its original
// octets. Error responses must own those octets before they cross errChan;
// looking them up later from Conn can attribute a subsequent message to the
// earlier fault.
func (c *Conn) handleReceivedSignals(ctx context.Context, m3 messages.M3UA, raw []byte) {
	select {
	case <-ctx.Done():
		return
	case <-c.done:
		return
	default:
	}

	// Signal validations
	if m3.Version() != 1 {
		c.notifyManagement(&ManagementIndication{
			Kind:        ManagementError,
			ErrorCode:   params.InvalidVersionError,
			Description: errorCodeName(params.InvalidVersionError),
		})
		if len(raw) > 0 {
			c.sendErr(newInvalidVersionErrorFor(m3.Version(), raw))
		} else {
			c.sendErr(NewInvalidVersionError(m3.Version()))
		}
		return
	}

	switch msg := m3.(type) {
	// Transfer message
	case *messages.Data:
		// Inline, not on its own goroutine: this is the payload path whose
		// order MTP3 depends on. handleData blocks only when dataChan is full,
		// which is backpressure onto the peer rather than silent reordering.
		c.handleData(ctx, msg)
		c.sendState(stateUnchanged)
	// ASPSM
	case *messages.AspUp:
		// RFC 4666 Section 4.3.4.1: at an SGP, ASP Up always drives the remote
		// ASP to ASP-INACTIVE, including from ASP-ACTIVE, where an Error
		// ("Unexpected Message") accompanies the Ack. Those are SGP procedures
		// ("The ASP is always the initiator of the ASP Up message"), so an ASP
		// that receives one reports the Error and holds its state — a stray or
		// forged ASP Up must not take a client's active data path down. A
		// genuinely unusable message (e.g. wrong SCTP stream) also holds state.
		if err := c.handleAspUp(msg); err != nil {
			c.sendErr(err)

			var unexpected *UnexpectedMessageError
			if !errors.As(err, &unexpected) || c.mode != modeServer {
				c.sendState(stateUnchanged)
				return
			}
		}
		c.sendState(StateAspInactive)
	case *messages.AspUpAck:
		// RFC 4666 Section 4.3.4.1: "If the ASP receives an unexpected ASP Up
		// Ack message, the ASP should consider itself in the ASP-INACTIVE
		// state." That clause is scoped to the ASP. An SGP is never the
		// initiator of ASP Up, so it cannot legitimately receive an ASP Up Ack
		// and nothing authorises it to change state on one: it reports the
		// Error and holds, rather than letting a stray message take an active
		// association's data path down.
		if err := c.handleAspUpAck(msg); err != nil {
			c.sendErr(err)

			var unexpected *UnexpectedMessageError
			if !errors.As(err, &unexpected) || c.mode != modeClient {
				c.sendState(stateUnchanged)
				return
			}
		}
		c.sendState(StateAspInactive)
	case *messages.AspDown:
		if err := c.handleAspDown(msg); err != nil {
			c.sendErr(err)
			c.sendState(stateUnchanged)
		} else {
			c.sendState(StateAspDown)
		}
	case *messages.AspDownAck:
		// RFC 4666 Section 4.3.4.2 puts the ASP in ASP-DOWN on an ASP Down Ack
		// whether or not it asked for one, so the Error accompanies the
		// transition instead of replacing it. An ASP that reported the Error and
		// held would go on believing it was carrying traffic the SGP had
		// already taken away.
		if err := c.handleAspDownAck(msg); err != nil {
			c.sendErr(err)

			var unexpected *UnexpectedMessageError
			if !errors.As(err, &unexpected) || c.mode != modeClient {
				c.sendState(stateUnchanged)
				return
			}
		}
		c.sendState(StateAspDown)
	// ASPTM
	case *messages.AspActive:
		// RFC 4666 Section 4.3.4.3: the SGP owes an ASP Active Ack even when the
		// ASP is already ASP-ACTIVE, so an accompanying "Unexpected Message"
		// Error does not stop the transition — the Ack has gone out and the peer
		// will act on it. An ASP that receives an ASP Active, or a request the
		// node could not act on (unsupported traffic mode, failed write), holds
		// its state instead.
		if err := c.handleAspActive(msg); err != nil {
			c.sendErr(err)

			var unexpected *UnexpectedMessageError
			// An ASP-DOWN peer is refused outright: no Ack was written, and
			// Figure 3 has no edge from ASP-DOWN to ASP-ACTIVE, so there is
			// nothing for the peer to act on and no transition to publish.
			if errors.As(err, &unexpected) && c.mode == modeServer &&
				c.State() != StateAspDown {
				c.sendState(StateAspActive)
				return
			}

			// A request may mix served and unserved Routing Contexts. The Acked
			// subset has already been recorded by the handler and must become
			// active even though the unserved subset also produces an Error.
			var routingContextError *RoutingContextError
			if c.mode == modeServer && errors.As(err, &routingContextError) &&
				c.stateForActiveRoutingContexts() == StateAspActive {
				c.sendState(StateAspActive)
				return
			}
			c.sendState(stateUnchanged)
			return
		}
		c.sendState(StateAspActive)
	case *messages.AspActiveAck:
		if err := c.handleAspActiveAck(msg); err != nil {
			c.sendErr(err)
			c.sendState(stateUnchanged)
		} else {
			c.sendState(StateAspActive)
		}
	case *messages.AspInactive:
		// RFC 4666 Section 4.3.4.4: as with ASP Active, the Ack is owed even when
		// the ASP is already ASP-INACTIVE, so the transition stands alongside the
		// Error. An ASP receiving an ASP Inactive holds its state.
		if err := c.handleAspInactive(msg); err != nil {
			c.sendErr(err)

			var unexpected *UnexpectedMessageError
			if errors.As(err, &unexpected) && c.mode == modeServer &&
				c.State() != StateAspDown {
				c.sendState(c.stateForActiveRoutingContexts())
				return
			}

			// As for ASP Active, an Error for unserved contexts accompanies the
			// successful transition of every context named in the Ack.
			var routingContextError *RoutingContextError
			if c.mode == modeServer && errors.As(err, &routingContextError) {
				c.sendState(c.stateForActiveRoutingContexts())
				return
			}
			c.sendState(stateUnchanged)
			return
		}
		c.sendState(c.stateForActiveRoutingContexts())
	case *messages.AspInactiveAck:
		if err := c.handleAspInactiveAck(msg); err != nil {
			c.sendErr(err)
			c.sendState(stateUnchanged)
		} else if c.hasAcknowledgedRoutingContexts() {
			c.sendState(StateAspActive)
		} else {
			c.sendState(StateAspInactive)
		}
	case *messages.Heartbeat:
		if err := c.handleHeartbeat(msg); err != nil {
			c.sendErr(err)
		}
		c.sendState(stateUnchanged)
	case *messages.HeartbeatAck:
		if err := c.handleHeartbeatAck(msg); err != nil {
			c.sendErr(err)
		} else {
			c.notifyBeatAck()
		}
		c.sendState(stateUnchanged)
		// Management
	case *messages.Error:
		if err := c.handleError(msg); err != nil {
			c.sendErr(err)
		}
		c.sendState(stateUnchanged)
	case *messages.Notify:
		if err := c.handleNotify(msg); err != nil {
			c.sendErr(err)
			c.sendState(stateUnchanged)
			return
		}
		// RFC 4666 Section 4.3.4.3: an ASP told that an alternate ASP has
		// overridden it "MUST consider itself now in the ASP-INACTIVE state".
		// Every other Notify is advisory (Section 4.3.4.5) and holds state.
		//
		// How far it reaches depends on which Routing Contexts it names:
		// overrideScope moves the whole association only when the override
		// covers everything the association carries, and otherwise stops just
		// the named contexts. See RFC 4666 Errata ID 2065.
		if c.overriddenByAlternateAsp(msg) && c.overrideScope(msg) {
			c.sendState(StateAspInactive)
			return
		}
		c.sendState(stateUnchanged)
	// SSNM: destination availability. These carry no acknowledgement and never
	// change the ASP state machine — they report the reachability of SS7
	// destinations beyond the peer (RFC 4666 Section 4.5).
	case *messages.DestinationUnavailable:
		if err := c.handleDestinationUnavailable(msg); err != nil {
			c.sendErr(err)
		}
		c.sendState(stateUnchanged)
	case *messages.DestinationAvailable:
		if err := c.handleDestinationAvailable(msg); err != nil {
			c.sendErr(err)
		}
		c.sendState(stateUnchanged)
	case *messages.DestinationRestricted:
		if err := c.handleDestinationRestricted(msg); err != nil {
			c.sendErr(err)
		}
		c.sendState(stateUnchanged)
	case *messages.SignallingCongestion:
		if err := c.handleSignallingCongestion(msg); err != nil {
			c.sendErr(err)
		}
		c.sendState(stateUnchanged)
	case *messages.DestinationUserPartUnavailable:
		if err := c.handleDestinationUserPartUnavailable(msg); err != nil {
			c.sendErr(err)
		}
		c.sendState(stateUnchanged)
	case *messages.DestinationStateAudit:
		if err := c.handleDestinationStateAudit(msg); err != nil {
			c.sendErr(err)
		}
		c.sendState(stateUnchanged)
	default:
		// RFC 4666 Section 3.8.1 draws the distinction by class, not by one
		// particular class: "The 'Unsupported Message Class' error is sent if a
		// message with an unexpected or unsupported Message Class is received."
		// Only RKM used to get it, so every other class this library does not
		// implement — the reserved 5 to 8, and everything from 10 to 255 — drew
		// the type-level error instead, telling the peer its class was fine and
		// only the type was wrong.
		//
		// Section 4.4.1 is the concrete case that matters: "If the SGP does not
		// support the registration procedure, the SGP returns an Error message
		// to the ASP, with an error code of 'Unsupported Message Class'", so an
		// ASP attempting dynamic registration can tell it must fall back to
		// static configuration.
		if implementedClass(m3.MessageClass()) {
			if len(raw) > 0 {
				c.sendErr(newUnsupportedMessageErrorForMessage(m3, raw))
			} else {
				c.sendErr(NewUnsupportedMessageError(m3))
			}
		} else {
			if len(raw) > 0 {
				c.sendErr(newUnsupportedClassErrorForMessage(m3, raw))
			} else {
				c.sendErr(NewUnsupportedClassError(m3))
			}
		}
		c.sendState(stateUnchanged)
	}
}

// dispatchRaw handles one received message.
//
// A message that fails to parse is not simply dropped. RFC 4666 Section 7.3.2:
// "When an implementation receives a message type that it does not support, it
// MUST respond with an Error (ERR) message ('Unsupported Message Type')", and
// Section 3.8.1 says the same for an unsupported class. The class and type
// octets sit at fixed offsets in the common header (Section 3.1), so they are
// readable even when the parameters after them are not — and dropping the whole
// message meant one malformed TLV silenced both requirements.
func (c *Conn) dispatchRaw(ctx context.Context, raw inbound) {
	// RFC 4666 Section 7.1 permits only the registered M3UA PPID 3 and the
	// unspecified PPID 0. Anything else belongs to another upper-layer
	// protocol and is silently discarded: reflecting M3UA ERR or state across
	// that boundary would be both misleading and an amplification path.
	if raw.ppid != 0 && raw.ppid != M3UAPPID {
		return
	}

	// Published before the handlers run, on this same goroutine, so a handler
	// validating the arrival stream sees this message's.
	c.recvStream.Store(uint32(raw.stream))

	msg, err := messages.Parse(raw.data)
	if err != nil {
		c.logMalformedInput(err, raw.data)

		// Too short to carry a header: there is no class or type to answer for.
		if len(raw.data) < 8 {
			return
		}

		// RFC 4666 Section 3.8.1: "Error messages MUST NOT be generated in
		// response to other Error messages." A well-formed ERR was already left
		// alone, but one the parser rejected reached the fallback below, which
		// reads the class and type octets — and those octets say Management,
		// Error. Two peers each sending an ERR the other cannot parse would
		// then answer one another indefinitely, which is the loop the sentence
		// exists to prevent.
		if rawClass(raw.data) == messages.MsgClassManagement &&
			rawType(raw.data) == messages.MsgTypeError {
			return
		}

		switch {
		case messages.IsSupported(rawClass(raw.data), rawType(raw.data)):
			// The class and type name a message this package implements, so
			// the complaint is about the parameters inside it, not about the
			// type. Section 3.8.1 gives that its own codes; answering
			// "Unsupported Message Type" would tell the peer to stop sending a
			// message that is in fact supported.
			c.sendErr(NewParameterFaultErrorFor(raw.data, err))
		case implementedClass(rawClass(raw.data)):
			c.sendErr(NewUnsupportedMessageErrorFor(raw.data))
		default:
			c.sendErr(NewUnsupportedClassErrorFor(raw.data))
		}
		return
	}

	// Only a successfully decoded M3UA message is evidence the peer M3UA layer
	// is alive. Arbitrary or malformed octets on the association must not keep
	// T(beat) from expiring; a valid Generic for an unsupported class or type
	// does count, because Parse succeeded and handleReceivedSignals will issue
	// the required ERR.
	c.notePeerActivity()
	c.handleReceivedSignals(ctx, msg, raw.data)
}

// implementedClass reports whether this library implements the message class,
// and so whether an unrecognised message within it is a type-level or a
// class-level failure. RKM is deliberately absent: its message types are
// defined by RFC 4666 Section 3.6 but no codec here handles them.
func implementedClass(class uint8) bool {
	switch class {
	case messages.MsgClassManagement,
		messages.MsgClassTransfer,
		messages.MsgClassSSNM,
		messages.MsgClassASPSM,
		messages.MsgClassASPTM:
		return true
	default:
		return false
	}
}

// readLoop reads from the SCTP association and hands each datagram to
// monitor(). It runs in its own goroutine so that monitor() never blocks in a
// read: a read parked inside monitor()'s select would starve the sibling arms,
// and because errChan is unbuffered, sendErr() would then block forever.
//
// That starvation is what made T(beat) useless. heartbeat() reports an expiry
// through sendErr, so against a peer that stops answering BEATs — the exact
// condition the timer exists to detect — the report could never be delivered:
// the connection stayed ASP-ACTIVE and the heartbeat goroutine leaked for the
// lifetime of the process. Every other asynchronous sendErr caller (malformed
// DATA, unsupported messages, write failures) was silently affected the same
// way, so the mandated Error response was never emitted either.
type inboundKind uint8

const (
	// inboundMessage carries one complete M3UA message.
	inboundMessage inboundKind = iota
	// inboundSCTPRestart carries the association restart notification that was
	// observed between the M3UA messages before and after it.
	inboundSCTPRestart
)

// inbound is one ordered event read from the SCTP association. Message events
// carry their octets, arrival stream and host-order PPID; notification events
// use kind and leave those fields empty. Keeping one extensible value for both
// preserves receive order and per-message SCTP metadata.
type inbound struct {
	kind   inboundKind
	data   []byte
	stream uint16
	ppid   uint32
}

// newInboundMessage preserves the ancillary data ReadMsg returned for one
// complete message. gomaja/sctp has already converted PPID to host order, so it
// is copied without another byte swap; nil information is SCTP's unspecified
// stream and PPID zero.
func newInboundMessage(data []byte, info *sctp.SndRcvInfo) inbound {
	event := inbound{kind: inboundMessage, data: data}
	if info != nil {
		event.stream = info.Stream
		event.ppid = info.PPID
	}
	return event
}

func (c *Conn) readLoop(raw chan<- inbound, readErr chan<- error) {
	max := c.cfg.ReadBufferSize
	if max <= 0 {
		max = DefaultReadBufferSize
	}

	for {
		// ReadMsg reassembles a message across as many reads as the kernel
		// needs, using MSG_EOR to know where it ends, so the buffer no longer
		// has to be sized against the largest PDU a peer might send: max is
		// purely a ceiling on how much one message may allocate, which is what
		// keeps a peer claiming an enormous length from exhausting memory.
		b, info, err := c.sctpConn.ReadMsg(max)
		if err != nil {
			// A message beyond our ceiling leaves its remainder queued, so the
			// next read would resume mid-message and parse as garbage. There is
			// no way to resynchronise on a message boundary from here: report it
			// and let monitor() close the association rather than feed the state
			// machine fragments.
			if errors.Is(err, sctp.ErrMsgTooLong) {
				c.sendErr(ErrMessageTooLarge)
			}
			select {
			case readErr <- err:
			case <-c.done:
			}
			return
		}

		select {
		case raw <- newInboundMessage(b, info):
		case <-c.done:
			return
		}
	}
}

func (c *Conn) monitor(ctx context.Context) {

	c.beatAllow = sync.NewCond(&sync.Mutex{})
	c.beatAllow.L.Lock()
	go c.heartbeat(ctx)
	defer c.beatAllow.Broadcast()

	// readErr is buffered so the reader can report a failed read and exit even
	// if monitor() is already on its way out; inboundChan is unbuffered, which
	// keeps the reader in lockstep with the dispatcher and orders SCTP
	// notifications among the M3UA messages surrounding them.
	inboundChan := c.inboundChan
	readErr := make(chan error, 1)
	go c.readLoop(inboundChan, readErr)

	// The opening transition is applied here, synchronously, before the
	// dispatcher exists and therefore before any received message can be acted
	// on. Ordering it that way is the whole point.
	//
	// It used to be published from a goroutine that Dial and Accept started
	// alongside this one, which raced the reader for no reason. Losing that race
	// is not harmless: the peer's ASP Up arrives first, is answered, and moves
	// the association to ASP-INACTIVE -- and then the late ASP-DOWN publish
	// lands and overwrites it. The ASP Active that follows is judged against
	// ASP-DOWN, where Section 4.3.1 permits an SGP only Heartbeat, ASP Down Ack
	// and Error, so it is refused with "Unexpected Message". Nothing moves the
	// state again, so every T(ack) retransmission meets the same refusal and
	// both ends abandon an association whose handshake had in fact succeeded.
	// On a capture that is an ASP Up Ack and a Notify, then five ASP Actives two
	// seconds apart each answered with error code 6.
	//
	// Applying it inline also means the client's ASP-DOWN entry action has
	// written its ASP Up before the reader can deliver the answer to it.
	if err := c.handleStateUpdate(StateAspDown); err != nil {
		_ = c.closeWith(err)
		return
	}

	// One dispatcher, so received messages are handled in the order the peer
	// sent them. It cannot live in this loop: handlers publish states and
	// errors on channels this loop consumes, so handling inline here would
	// deadlock against itself — which is why each message used to be handed to
	// its own goroutine, and why ordering was lost. A separate goroutine keeps
	// this loop free to drain stateChan and errChan while messages are handled
	// one at a time.
	go c.dispatchLoop(ctx, inboundChan)

	// Every closeWith below discards its return, which is the error from closing
	// the SCTP socket, and does so deliberately: each of these arms is already
	// unwinding because of a prior cause, that cause is the one stored for
	// Close() to report, and a failure to close a socket the peer has already
	// stopped talking on has no one left to be reported to. The descriptor is
	// released either way. Written explicitly so the discard reads as a decision
	// rather than an oversight, matching the call sites in client.go and
	// server.go.
	for {
		select {
		case <-ctx.Done():
			_ = c.closeWith(ctx.Err())
			return
		case <-c.done:
			// Close() was called directly rather than through this loop. Without
			// this arm the loop still unwinds — closing the socket fails the
			// pending read, and the reader reports that through readErr — but it
			// gets there indirectly, by way of an error raised on a connection
			// that was closed deliberately. Observing done makes the shutdown
			// self-sufficient and independent of the reader's timing.
			return
		case err := <-readErr:
			_ = c.closeWith(err)
			return
		case err := <-c.errChan:
			if e := c.handleErrors(err); e != nil {
				_ = c.closeWith(e)
				return
			}
		case state := <-c.stateChan:
			// Act properly based on current state.
			if err := c.handlePublishedStateUpdate(state); err != nil {
				if errors.Is(err, ErrSCTPNotAlive) {
					_ = c.closeWith(err)
					return
				}
			}
		}
	}
}

// dispatchLoop handles received M3UA messages and SCTP notifications strictly
// in arrival order.
//
// M3UA carries MTP3, and MTP3 guarantees sequenced delivery within a Signalling
// Link Selection (RFC 4666 Section 1.4.7). SCTP preserves that order within a
// stream, so it reaches the library intact; handing each message to its own
// goroutine handed it straight back to the scheduler, and reordered DATA breaks
// the transactions above it.
func (c *Conn) dispatchLoop(ctx context.Context, inboundChan <-chan inbound) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.done:
			return
		case event, ok := <-inboundChan:
			if !ok {
				return
			}
			c.dispatchInbound(ctx, event)
		}
	}
}

func (c *Conn) dispatchInbound(ctx context.Context, event inbound) {
	switch event.kind {
	case inboundMessage:
		c.dispatchRaw(ctx, event)
	case inboundSCTPRestart:
		c.handleSCTPRestart()
	}
}
