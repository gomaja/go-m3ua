// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gomaja/go-sctp"
)

// oneShotRTOMillisFor is the setup RTO.Initial to use for one attempt.
//
// The kernel schedules its first INIT retransmission at this value, so putting
// it beyond the caller's budget is what keeps the attempt to a single INIT.
// SCTP_INITMSG cannot express that on its own: its MaxAttempts counts
// retransmissions and zero selects the kernel default, so the smallest usable
// value still puts a second INIT on the wire at net.sctp.rto_initial.
//
// A margin is added so the retransmission timer never races the deadline. The
// result saturates at the setup value's uint32 millisecond limit; that still covers
// any operationally useful InitTimeout by weeks.
func oneShotRTOMillisFor(timeout time.Duration) uint32 {
	if timeout <= 0 {
		return 1
	}

	ms := uint64(timeout / time.Millisecond)
	if timeout%time.Millisecond != 0 {
		ms++
	}
	if ms >= maxSetupRTOMillis-uint64(rtoMarginMillis) {
		return ^uint32(0)
	}
	return uint32(ms + uint64(rtoMarginMillis))
}

// rtoMarginMillis keeps the first retransmission clear of the deadline.
const rtoMarginMillis = 1000

const maxSetupRTOMillis = uint64(^uint32(0))

// sctpStreams is how many SCTP streams an association asks for in each
// direction: stream 0, where RFC 4666 Section 1.4.7 rule 2 puts the management
// classes, and one stream per value of the 8-bit Signalling Link Selection the
// Protocol Data parameter carries (Section 3.3.1). streamFor maps SLS n to
// stream n+1, so with 257 streams every SLS keeps an ordered stream of its own;
// Section 1.4.7 bounds that mapping by "the maximum number of streams supported
// by the underlying SCTP association", and M3UA has no use for more.
//
// Asking for more is not free. Linux preallocates per-stream state for every
// negotiated stream for the life of the association: 65,535 outbound streams,
// the SCTP maximum these associations used to request, pinned about 2.9 MB of
// unreclaimable kernel memory per association on loopback against 11.5 kB for
// 257. The limit applies inbound as well, so a peer that asks for the maximum
// cannot make this end preallocate it either.
const sctpStreams = 257

type sctpDialPolicy struct {
	init            sctp.InitMsg
	setupRTOInitial time.Duration
	abandon         sctp.AbandonPolicy
	// events are subscribed before the socket connects; see associationEvents.
	events []sctp.EventType
	// buffers are sized before the socket connects, so the INIT announces the
	// receive window they give; see socketBuffers.apply.
	buffers socketBuffers
	rtoInfo *sctp.RTOInfo
}

func oneShotSCTPDialPolicy(timeout time.Duration) sctpDialPolicy {
	rtoMillis := oneShotRTOMillisFor(timeout)
	return sctpDialPolicy{
		init: sctp.InitMsg{
			OutStreams:   sctpStreams,
			MaxInStreams: sctpStreams,
			// Belt and braces: the deadline above ends the attempt first, and
			// the raised RTO keeps the kernel from retransmitting inside it.
			MaxAttempts:    1,
			MaxInitTimeout: time.Millisecond,
		},
		setupRTOInitial: time.Duration(rtoMillis) * time.Millisecond,
		abandon:         sctp.AbandonQuiet,
		events:          associationEvents(),
	}
}

func (p sctpDialPolicy) socketConfig(restarts *restartWatcher) *sctp.Config {
	base := &sctp.Config{
		// A Dial watcher serves one association, so its route ignores the
		// association ID; it is set once Dial has an Association to route to.
		NotificationHandler: restarts.handle,
		InitMsg:             p.init,
		SetupRTOInitial:     p.setupRTOInitial,
		RTOInfo:             p.rtoInfo,
		Notifications:       p.events,
		AbandonPolicy:       p.abandon,
	}
	p.buffers.apply(base)
	return base
}

type sctpAbandonPolicyDialer interface {
	Dial(
		context.Context,
		string,
		*sctp.Addr,
		*sctp.Addr,
	) (*sctp.Conn, error)
}

func (p sctpDialPolicy) dialContext(
	ctx context.Context,
	dialer sctpAbandonPolicyDialer,
	network string,
	laddr, raddr *sctp.Addr,
) (*sctp.Conn, error) {
	return dialer.Dial(ctx, network, laddr, raddr)
}

// dialAssociation makes exactly one SCTP association attempt, bounded by
// timeout and quietly abandoned as soon as ctx is done.
//
// go-sctp's quiet abandon policy does the hard part: it releases the
// non-established socket before returning without intentionally emitting a local
// ABORT. What is left here is keeping the attempt to a single INIT, which needs
// the initial RTO raised past the budget before the socket is connected.
// Config applies RTOInfo, SetupRTOInitial, notification subscriptions and
// socket buffers before connect (RFC 6458 Sections 6.2.2, 8.1.1 and 8.1.3).
// go-sctp restores the association's RTO.Initial to the socket's configured
// value once established; the setup override leaves the configured RTO.Min and
// RTO.Max unchanged. Linux does not bound T1-init or T1-cookie by RTO.Max, so
// raising it is unnecessary. AbandonQuiet releases an unfinished attempt
// without a locally initiated ABORT.
func dialAssociation(ctx context.Context, network string, laddr, raddr *sctp.Addr, timeout time.Duration, buffers socketBuffers, rtoInfo *sctp.RTOInfo, restarts *restartWatcher) (*sctp.Conn, error) {
	attemptCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	policy := oneShotSCTPDialPolicy(timeout)
	policy.buffers = buffers
	policy.rtoInfo = rtoInfo
	// There is one constructor call. Linux can report an ICMP Protocol
	// Unreachable for the INIT as ENOPROTOOPT (RFC 9260 Section 10, ICMP8);
	// return that dial error without trying a second socket or INIT.
	sctpAssociation, err := policy.dialContext(attemptCtx, policy.socketConfig(restarts), network, laddr, raddr)
	if err != nil {
		// Our own budget expiring is reported as such; the caller's context
		// ending is reported as the caller's error.
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			return nil, ErrInitTimeout
		}
		return nil, err
	}
	return sctpAssociation, nil
}

// Dial establishes an SCTP association and runs this Endpoint's M3UA role.
// After successfully establishing the association with the peer, state-changing
// M3UA signals and M3UA BEAT messages are automatically handled in background
// goroutines.
//
// StateChanges and ManagementIndications are optional subscriptions. Calling
// either starts delivery of future events on that channel; earlier events are
// not replayed. To see the handshake, call them from AssociationConfig.Subscribe,
// which runs on this goroutine before the handshake and its EstablishTimeout
// start. State gives the current ASP/IPSP state. Drain each subscribed channel
// for the association's lifetime: a full queue closes it with
// ErrIndicationQueueFull.
//
// Dial makes at most one SCTP association attempt — one INIT if ctx is live,
// none if ctx is already done — bounded by AssociationConfig.InitTimeout, and never
// retries. A caller that wants to keep trying loops over Dial and chooses its
// own cadence, which is the point: left to the kernel's defaults an unanswered
// attempt runs to nine INIT chunks over 342 seconds, far too long for an
// application to react to anything.
//
// The attempt uses an RTO.Initial beyond InitTimeout to prevent an INIT
// retransmission inside the budget. Once established, RTO.Initial returns to
// the socket's value, including RTOInfo when configured; the setup override
// leaves the configured RTO.Min and RTO.Max unchanged (RFC 6458 Section
// 8.1.1). On Linux each path keeps the setup value as its current RTO until its
// first RTT measurement, retransmission timeout or association restart (RFC
// 9260 Section 6.3.1 rule C1). A DATA chunk lost before that first measurement,
// normally the first M3UA message, therefore waits about InitTimeout + 1 s for
// retransmission. See https://github.com/sctp/lksctp-tools/issues/74.
//
// Every path releases the socket before returning. On a timeout or a cancelled
// context the non-established attempt is quietly abandoned at the deadline, so
// no local ABORT is intentionally emitted and the descriptor and kernel-side
// association are gone by the time Dial returns.
//
// The M3UA handshake that follows has its own budget,
// AssociationConfig.EstablishTimeout, and observes ctx as well.
func (e *Endpoint) Dial(ctx context.Context, network string, laddr, raddr *sctp.Addr, cfg *AssociationConfig) (*Association, error) {
	role, err := e.associationRole()
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		return nil, ErrNilAssociationConfig
	}
	cfg = snapshotAssociationConfig(cfg)
	if err := validateAssociationConfigForRole(role, cfg); err != nil {
		return nil, err
	}
	if err := e.validateAssociationConfig(cfg); err != nil {
		return nil, err
	}
	n, ok := netMap[network]
	if !ok {
		return nil, fmt.Errorf("invalid network: %s", network)
	}
	initTimeout := cfg.InitTimeout
	if initTimeout <= 0 {
		initTimeout = DefaultInitTimeout
	}

	// An already-cancelled context must not start an attempt at all.
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if !e.beginOperation() {
		return nil, ErrEndpointClosed
	}
	defer e.endOperation()
	operationCtx, cancelOperation, stopOperation := e.operationContext(ctx)
	keepOperationContext := false
	defer func() {
		stopOperation()
		if !keepOperationContext {
			cancelOperation(nil)
		}
	}()

	// Nothing here writes to cfg: each permitted Dial gets an independent
	// immutable AssociationConfig snapshot. An SGP or IPSP registers its
	// Application Server scope only after cancellation and Endpoint closure
	// have been ruled out, so an attempt that never starts cannot change
	// Endpoint state.
	association := newAssociation(role, cfg)
	switch role {
	case RoleSGP:
		association.as, association.nif, association.destinations, association.mtp3Restarts = e.sgpRegistry()
	case RoleIPSP:
		association.as = e.applicationServerRegistry()
	}
	registration := &applicationServerReservation{}
	if association.as != nil {
		registration = association.as.reserve(association.configuredASKeys())
	}
	defer registration.rollback()

	// The notification handler has to be installed on the socket at dial time;
	// route it to this Association, and ignore events that arrive before an SCTP
	// association identifier exists.
	restarts := &restartWatcher{}
	restarts.setRoute(association.associationForSCTPID)

	sctpConn, err := dialAssociation(operationCtx, n, laddr, raddr, initTimeout, socketBuffersFor(cfg.SCTPConfig), cfg.RTOInfo, restarts)
	if err != nil {
		if errors.Is(context.Cause(operationCtx), ErrEndpointClosed) {
			return nil, ErrEndpointClosed
		}
		return nil, err
	}
	association.sctpConn = sctpConn
	if !e.trackAssociation(association) {
		_ = sctpConn.Close()
		return nil, ErrEndpointClosed
	}

	if err := association.setUpSocket(); err != nil {
		_ = association.closeWith(err)
		return nil, err
	}

	association.subscribeBeforeHandshake()

	// The opening ASP-DOWN entry is applied inside monitor(), ahead of
	// dispatching, instead of racing the reader from here.
	go association.monitor(operationCtx)

	establishTimeout := cfg.EstablishTimeout
	if establishTimeout <= 0 {
		establishTimeout = DefaultEstablishTimeout
	}

	select {
	case <-association.established:
		registration.commit()
		keepOperationContext = true
		return association, nil
	case <-association.done:
		// done closes at the beginning of closeWith. Join the once-only teardown
		// before rolling back provisional ASKeys, so Endpoint membership has
		// already been removed when the reservation checks whether a scope is empty.
		_ = association.closeWith(nil)
		return nil, e.dialHandshakeError(operationCtx, association)
	case <-operationCtx.Done():
		cause := context.Cause(operationCtx)
		_ = association.closeWith(cause)
		return nil, cause
	case <-time.After(establishTimeout):
		_ = association.closeWith(ErrTimeout)
		return nil, ErrTimeout
	}
}

func (e *Endpoint) dialHandshakeError(operationCtx context.Context, association *Association) error {
	// Endpoint.Close cancels e.ctx before Association.Close closes done.
	// Dial's receive from done therefore observes that cancellation even if
	// operationContext's AfterFunc has not run yet.
	if errors.Is(context.Cause(e.ctx), ErrEndpointClosed) ||
		errors.Is(context.Cause(operationCtx), ErrEndpointClosed) {
		return ErrEndpointClosed
	}
	if err := association.Err(); err != nil {
		return err
	}
	return ErrFailedToEstablish
}
