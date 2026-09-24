// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"errors"
	"fmt"
	"sync"
	"syscall"

	"github.com/gomaja/go-sctp"
)

// restartWatcher handles SCTP_ASSOC_CHANGE, the one notification this package
// subscribes to (see associationEvents), and acts on two of its states.
//
// SCTP_RESTART becomes the M-SCTP_RESTART indication of RFC 4666 Section
// 1.6.3: "M3UA informs LM that an SCTP restart indication has been received."
// An SCTP restart is the peer re-establishing the same association without
// tearing the old one down first -- a peer process that died and came back on
// the same five-tuple, most often. RFC 9260 Section 5.2.4 has the receiver keep
// the association and reset it, but RFC 4666 Section 4.3.3 still requires M3UA
// to move the ASP to ASP-DOWN. At an ASP it must also pause affected SS7
// destinations and begin recovery with ASP Up; at an SGP the transition removes
// the remote ASP from every Application Server it previously served.
//
// SCTP_COMM_LOST ends the read, which is what closes the Association when the
// SCTP association fails; handle says why the read cannot be left to do that
// by itself.
//
// A watcher is shared by every association a Listener accepts, because the
// dependency fixes a listener's notification handler at construction and hands
// the same one to each accepted association. Routing by association ID is what
// makes a shared handler usable: the kernel names the association in the event.
type restartWatcher struct {
	mu sync.Mutex
	// route maps an association ID to the Association that owns it. Held behind
	// the mutex because Dial sets it after the association exists, and a
	// listener consults it from a reader goroutine.
	route func(sctp.SCTPAssocID) *Association
}

// setRoute installs the lookup. Dial sets it once after building the
// Association; a Listener sets it before it can accept anything.
func (w *restartWatcher) setRoute(f func(sctp.SCTPAssocID) *Association) {
	w.mu.Lock()
	w.route = f
	w.mu.Unlock()
}

// handle is the sctp.NotificationHandler. It runs on the goroutine that read
// the notification, which is the reader of the association it concerns.
//
// It returns an error for SCTP_COMM_LOST alone. The dependency propagates one
// out of the read, so returning it for an unparseable event -- something this
// layer neither caused nor can fix -- would turn it into a failed read and a
// dead association. An event we cannot make sense of is worth strictly less
// than the association carrying it.
func (w *restartWatcher) handle(b []byte) error {
	n, err := sctp.ParseNotification(b)
	if err != nil {
		return nil
	}

	ac, ok := n.(*sctp.AssocChange)
	if ok && ac.State == sctp.SCTP_COMM_LOST {
		// RFC 6458 Section 6.1.1: "The association has failed. The association
		// is now in the closed state." RFC 4666 Section 4.3.1: "SCTP CDI is
		// understood as either a SHUTDOWN_COMPLETE notification or a
		// COMMUNICATION_LOST notification from the SCTP layer." The read has to
		// end on it.
		//
		// It used to be left to the read that follows, which was expected to
		// fail with the socket's pending error. Linux hands that error to
		// whichever call asks first, a send included (sctp_error in
		// net/sctp/socket.c), and then forgets it. When a write asked first,
		// the next read found nothing -- an abort leaves no RCV_SHUTDOWN, so a
		// non-blocking read answers EAGAIN -- and the reader parked for good:
		// ASP-ACTIVE indefinitely, every write failing. This notification is
		// the one report no other call can take.
		//
		// A one-to-one socket carries one association, so the event concerns
		// the association being read whatever the route knows. SHUTDOWN_COMP,
		// the other half of SCTP CDI, needs no such help: the SHUTDOWN before it
		// set RCV_SHUTDOWN, and the read then returns EOF whatever other calls
		// were made.
		return fmt.Errorf("%w: SCTP association lost (SCTP_COMM_LOST, %s)",
			ErrSCTPNotAlive, sctp.ErrorCauseString(uint32(ac.Error)))
	}
	if !ok || ac.State != sctp.SCTP_RESTART {
		// Every other association event is either followed by a read that
		// fails on its own, as SHUTDOWN_COMP is, or leaves nothing to act on.
		// Only a restart leaves the association usable and would otherwise
		// pass unmentioned.
		return nil
	}

	w.mu.Lock()
	route := w.route
	w.mu.Unlock()
	if route == nil {
		return nil
	}

	if c := route(ac.AssocID); c != nil {
		c.enqueueSCTPRestart()
	}
	return nil
}

// enqueueSCTPRestart places the notification in the same ordered queue as the
// M3UA messages read around it. The SCTP dependency calls this from inside the
// association's reader; using the dispatch queue therefore blocks that reader
// from reaching a later message until the restart marker follows every earlier
// one.
func (c *Association) enqueueSCTPRestart() {
	// An Association built outside newAssociation has no live reader or dispatcher,
	// so there is
	// nowhere ordered to deliver the event. Every Dial and Accept path
	// initialises inboundChan; silently ignore an unusable zero Association rather than
	// reintroduce an out-of-order direct state transition as a fallback.
	if c.inboundChan == nil {
		return
	}

	select {
	case <-c.done:
		return
	default:
	}
	select {
	case c.inboundChan <- inbound{kind: inboundSCTPRestart}:
	case <-c.done:
	}
}

// handleSCTPRestart applies the M3UA procedure for an SCTP restart while
// leaving the still-usable association open.
//
// sendState commits ASP-DOWN before it publishes the transition, so the next
// M3UA message read after the notification is judged against the reset state.
// The monitor consumes the unbuffered publication before a following ASP Up
// can publish ASP-INACTIVE, which also serialises the per-AS cleanup ahead of
// the peer's recovery. For an ASP role, the ASP-DOWN entry action sends ASP Up
// and starts T(ack).
func (c *Association) handleSCTPRestart() {
	// When recovery is already waiting in ASP-DOWN, publishing ASP-DOWN again
	// is deliberately a state restatement and its entry action will not run.
	// Remember that case so this new SCTP epoch still gets a new ASP Up after
	// the old epoch's timer is retired below.
	c.muState.RLock()
	restartWhileDown := c.initiatesASPSM() && c.state == StateASPDown && c.stateEntered
	c.muState.RUnlock()
	// The SCTP association remains usable, but its peer state is a new epoch.
	// Advance it before anything else, so a DATA delivered from here on is
	// reported in the epoch it actually arrived in.
	c.epoch.Add(1)
	// Drain any retry already entering the writer and cancel every old T(ack)
	// before ASP-DOWN can start the mandatory fresh ASP-Up procedure.
	c.resetTAckEpoch()
	if c.isIPSPDoubleExchange() {
		c.commitLocalIPSPState(StateASPDown)
		c.noteNoRoutingContextsAcked()
		c.forgetActiveRoutingContexts()
	}
	if c.role == RoleIPSP {
		// Close admission for the peer-directed traffic flow before waiting for
		// DATA already admitted in the old SCTP epoch. RFC 4666 Section 4.3.3
		// moves the peer IPSP to ASP-DOWN at the restart boundary; publishing
		// that transition may start the fresh ASP Up procedure, so it cannot
		// precede the old traffic barrier.
		c.commitState(StateASPDown)
		c.quiesceLocalIPSPSSNMTraffic()
		c.quiesceUnscopedTraffic()
	}
	// Section 4.3.3 requires this only at an ASP; pauseDestinations enforces the
	// role and leaves an SGP's node-wide destination view untouched.
	c.pauseDestinations()
	c.sendState(StateASPDown)
	c.notifyManagement(&ManagementIndication{
		Kind:   ManagementSCTPRestart,
		ASKeys: endpointASPStatusKeys(c),
		Description: "the peer restarted the SCTP association; " +
			"the remote ASP/IPSP state moved to ASP-DOWN and recovery state was cleared",
	})
	if restartWhileDown && !c.terminating.Load() {
		if err := c.initiateASPSM(); err != nil {
			c.sendErr(err)
		}
	}
}

func (c *Association) initiatesASPSM() bool {
	if c == nil {
		return false
	}
	if c.role == RoleASP {
		return c.aspProcedureMode(aspProcedureUp) == ASPProcedureAutomatic
	}
	return c.role == RoleIPSP && c.cfg != nil && c.cfg.IPSP != nil &&
		c.aspProcedureMode(aspProcedureUp) == ASPProcedureAutomatic
}

// associationEvents is the subscription every socket this package opens makes
// before it connects or listens: SCTP_ASSOC_CHANGE, which carries both states
// restartWatcher acts on, through RFC 6458 Section 6.2.2's SCTP_EVENT. That
// option names one event and leaves the others alone; the deprecated
// SCTP_EVENTS rewrites them all.
//
// Before, not after. Linux gives an association a copy of its socket's
// subscriptions when it creates it (sctp_association_init in
// net/sctp/associola.c) and filters each event against that copy
// (sctp_ulpq_tail_event in net/sctp/ulpqueue.c). A subscription made once Dial
// or Accept had the association applied only from then on, so an ABORT that
// arrived first raised no SCTP_COMM_LOST. An accepted association is created
// on the listening socket, so the listener's subscription is the one it
// carries into accept.
func associationEvents() []sctp.NotificationSubscription {
	return []sctp.NotificationSubscription{
		{Type: sctp.SCTP_ASSOC_CHANGE, State: sctp.SocketOptionEnable},
	}
}

// withAssociationEvents opens a socket with associationEvents, and again
// without them if the kernel has no SCTP_EVENT: Linux added the option in 5.0
// and refuses it with ENOPROTOOPT before that. The dependency applies the
// subscription before bind, connect or listen, so the refused attempt put
// nothing on the wire; Dial still sends at most one INIT.
//
// Such a kernel still serves traffic, as it did when the subscription was made
// on the established association and allowed to fail. What it loses is logged
// rather than refused: no restart is reported as M-SCTP_RESTART, and a lost
// association is noticed only through the socket's pending error, which a
// concurrent write can take before the reader does.
func withAssociationEvents[T any](open func(subscribe bool) (T, error)) (T, error) {
	opened, err := open(true)
	if err == nil || !errors.Is(err, syscall.ENOPROTOOPT) {
		return opened, err
	}
	logf("m3ua: this kernel cannot subscribe to SCTP association events (%v); "+
		"M-SCTP_RESTART will not be reported and a lost association "+
		"may go unnoticed while writes fail", err)
	return open(false)
}
