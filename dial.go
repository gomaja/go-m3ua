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

// oneShotRTOMillisFor is the SCTP_RTOINFO RTO to use for one attempt.
//
// The kernel schedules its first INIT retransmission at this value, so putting
// it beyond the caller's budget is what keeps the attempt to a single INIT.
// SCTP_INITMSG cannot express that on its own: its MaxAttempts counts
// retransmissions and zero selects the kernel default, so the smallest usable
// value still puts a second INIT on the wire at net.sctp.rto_initial.
//
// A margin is added so the retransmission timer never races the deadline. The
// result saturates at SCTP_RTOINFO's uint32 millisecond limit; that still covers
// any operationally useful InitTimeout by weeks.
func oneShotRTOMillisFor(timeout time.Duration) uint32 {
	if timeout <= 0 {
		return 1
	}

	ms := uint64(timeout / time.Millisecond)
	if timeout%time.Millisecond != 0 {
		ms++
	}
	if ms >= maxRTOInfoMillis-uint64(rtoMarginMillis) {
		return ^uint32(0)
	}
	return uint32(ms + uint64(rtoMarginMillis))
}

// rtoMarginMillis keeps the first retransmission clear of the deadline.
const rtoMarginMillis = 1000

const maxRTOInfoMillis = uint64(^uint32(0))

type sctpDialPolicy struct {
	init    sctp.InitMsg
	rto     sctp.RtoInfo
	abandon sctp.DialAbandonPolicy
}

func oneShotSCTPDialPolicy(timeout time.Duration) sctpDialPolicy {
	rtoMillis := oneShotRTOMillisFor(timeout)
	return sctpDialPolicy{
		init: sctp.InitMsg{
			NumOstreams: sctp.SCTP_MAX_STREAM,
			// Belt and braces: the deadline above ends the attempt first, and
			// the raised RTO keeps the kernel from retransmitting inside it.
			MaxAttempts:    1,
			MaxInitTimeout: 1,
		},
		rto: sctp.RtoInfo{
			AssocID: sctp.SCTPAssocID(sctp.SCTP_FUTURE_ASSOC),
			Initial: rtoMillis,
			// Initial can exceed the kernel default maximum when a caller
			// deliberately uses a long InitTimeout. Set Max with it so the
			// one-shot invariant does not silently disappear above 60 seconds.
			Max: rtoMillis,
		},
		abandon: sctp.DialAbandonQuiet,
	}
}

func (p sctpDialPolicy) socketConfig(restarts *restartWatcher) *sctp.PreconfiguredSocket {
	base := &sctp.SocketConfig{
		// A Dial watcher serves one association, so its route ignores the
		// association ID; it is set once Dial has an Association to route to.
		NotificationHandler: restarts.handle,
		InitMsg:             p.init,
	}
	return base.WithPreAssociation(sctp.PreAssociationConfig{
		RTOInfo: &p.rto,
	})
}

type sctpAbandonPolicyDialer interface {
	DialContextWithAbandonPolicy(
		context.Context,
		string,
		*sctp.SCTPAddr,
		*sctp.SCTPAddr,
		sctp.DialAbandonPolicy,
	) (*sctp.SCTPConn, error)
}

func (p sctpDialPolicy) dialContext(
	ctx context.Context,
	dialer sctpAbandonPolicyDialer,
	network string,
	laddr, raddr *sctp.SCTPAddr,
) (*sctp.SCTPConn, error) {
	return dialer.DialContextWithAbandonPolicy(ctx, network, laddr, raddr, p.abandon)
}

// dialAssociation makes exactly one SCTP association attempt, bounded by
// timeout and quietly abandoned as soon as ctx is done.
//
// go-sctp's quiet abandon policy does the hard part: it releases the
// non-established socket before returning without intentionally emitting a local
// ABORT. What is left here is keeping the attempt to a single INIT, which needs
// the initial RTO raised past the budget before the socket is connected.
// PreAssociation.RTOInfo applies that through SCTP_FUTURE_ASSOC without
// wrapping or taking ownership of the raw descriptor that go-sctp still owns.
func dialAssociation(ctx context.Context, network string, laddr, raddr *sctp.SCTPAddr, timeout time.Duration, restarts *restartWatcher) (*sctp.SCTPConn, error) {
	attemptCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	policy := oneShotSCTPDialPolicy(timeout)
	cfg := policy.socketConfig(restarts)

	sctpAssociation, err := policy.dialContext(attemptCtx, cfg, network, laddr, raddr)
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
// Dial makes at most one SCTP association attempt — one INIT if ctx is live,
// none if ctx is already done — bounded by AssociationConfig.InitTimeout, and never
// retries. A caller that wants to keep trying loops over Dial and chooses its
// own cadence, which is the point: left to the kernel's defaults an unanswered
// attempt runs to nine INIT chunks over 342 seconds, far too long for an
// application to react to anything.
//
// Every path releases the socket before returning. On a timeout or a cancelled
// context the non-established attempt is quietly abandoned at the deadline, so
// no local ABORT is intentionally emitted and the descriptor and kernel-side
// association are gone by the time Dial returns.
//
// The M3UA handshake that follows has its own budget,
// AssociationConfig.EstablishTimeout, and observes ctx as well.
func (e *Endpoint) Dial(ctx context.Context, network string, laddr, raddr *sctp.SCTPAddr, cfg *AssociationConfig) (*Association, error) {
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
	if association.as != nil {
		association.as.register(association.configuredASKeys())
	}

	// The notification handler has to be installed on the socket at dial time;
	// route it to this Association, and ignore events that arrive before an SCTP
	// association identifier exists.
	restarts := &restartWatcher{}
	restarts.setRoute(func(sctp.SCTPAssocID) *Association { return association })

	sctpConn, err := dialAssociation(operationCtx, n, laddr, raddr, initTimeout, restarts)
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

	// The opening ASP-DOWN transition is applied inside monitor(), ahead of
	// dispatching, instead of racing the reader from here.
	go association.monitor(operationCtx)

	establishTimeout := cfg.EstablishTimeout
	if establishTimeout <= 0 {
		establishTimeout = DefaultEstablishTimeout
	}

	select {
	case <-association.established:
		keepOperationContext = true
		return association, nil
	case <-association.done:
		if errors.Is(context.Cause(operationCtx), ErrEndpointClosed) {
			return nil, ErrEndpointClosed
		}
		if err := association.Err(); err != nil {
			return nil, err
		}
		return nil, ErrFailedToEstablish
	case <-operationCtx.Done():
		cause := context.Cause(operationCtx)
		_ = association.closeWith(cause)
		return nil, cause
	case <-time.After(establishTimeout):
		_ = association.closeWith(ErrTimeout)
		return nil, ErrTimeout
	}
}
