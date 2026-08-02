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
		// A client's watcher serves one association, so its route ignores the
		// association ID; it is set once Dial has a Conn to route to.
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

	conn, err := policy.dialContext(attemptCtx, cfg, network, laddr, raddr)
	if err != nil {
		// Our own budget expiring is reported as such; the caller's context
		// ending is reported as the caller's error.
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			return nil, ErrInitTimeout
		}
		return nil, err
	}
	return conn, nil
}

// Dial establishes a M3UA connection as a client.
// After successfully establishing the connection with peer, state-changing
// M3UA signals and M3UA BEAT messages are automatically handled in background
// goroutines.
//
// Dial makes at most one SCTP association attempt — one INIT if ctx is live,
// none if ctx is already done — bounded by Config.InitTimeout, and never
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
// The M3UA handshake that follows has its own budget, Config.EstablishTimeout,
// and observes ctx as well.
func Dial(ctx context.Context, net string, laddr, raddr *sctp.SCTPAddr, cfg *Config) (*Conn, error) {
	n, ok := netMap[net]
	if !ok {
		return nil, fmt.Errorf("invalid network: %s", net)
	}

	// Nothing here writes to cfg: a caller that reuses one *Config across
	// several Dials gets independent Conns, as the Listener does across Accepts.
	conn := newConn(modeClient, cfg)

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

	// Created before the association, because the handler has to be installed
	// on the socket at dial time; the route to this Conn is set immediately
	// after, and the handler ignores events that arrive before it exists.
	restarts := &restartWatcher{}
	restarts.setRoute(func(sctp.SCTPAssocID) *Conn { return conn })

	sctpConn, err := dialAssociation(ctx, n, laddr, raddr, initTimeout, restarts)
	if err != nil {
		return nil, err
	}
	conn.sctpConn = sctpConn

	if err := conn.setUpSocket(); err != nil {
		return nil, err
	}

	// As in server.go: the opening ASP-DOWN transition is applied inside
	// monitor(), ahead of dispatching, instead of racing the reader from here.
	go conn.monitor(ctx)

	establishTimeout := cfg.EstablishTimeout
	if establishTimeout <= 0 {
		establishTimeout = DefaultEstablishTimeout
	}

	select {
	case <-conn.established:
		return conn, nil
	case <-conn.done:
		if err := conn.Err(); err != nil {
			return nil, err
		}
		return nil, ErrFailedToEstablish
	case <-ctx.Done():
		_ = conn.closeWith(ctx.Err())
		return nil, ctx.Err()
	case <-time.After(establishTimeout):
		_ = conn.closeWith(ErrTimeout)
		return nil, ErrTimeout
	}
}
