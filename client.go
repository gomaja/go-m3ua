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

// initialRTOFor is the SCTP_RTOINFO initial RTO to use for one attempt.
//
// The kernel schedules its first INIT retransmission at this value, so putting
// it beyond the caller's budget is what keeps the attempt to a single INIT.
// SCTP_INITMSG cannot express that on its own: its MaxAttempts counts
// retransmissions and zero selects the kernel default, so the smallest usable
// value still puts a second INIT on the wire at net.sctp.rto_initial.
//
// A margin is added so the retransmission timer never races the deadline.
func initialRTOFor(timeout time.Duration) uint32 {
	ms := timeout.Milliseconds() + rtoMarginMillis
	if ms < 1 {
		ms = 1
	}
	// srto_initial must stay under srto_max, whose default is 60 seconds.
	if ms > 59000 {
		ms = 59000
	}
	return uint32(ms)
}

// rtoMarginMillis keeps the first retransmission clear of the deadline.
const rtoMarginMillis = 1000

// dialAssociation makes exactly one SCTP association attempt, bounded by
// timeout and abandoned as soon as ctx is done.
//
// DialContext does the hard part: it aborts the association and releases the
// socket before returning, so an abandoned attempt puts nothing further on the
// wire. What is left here is keeping the attempt to a single INIT, which needs
// the initial RTO raised past the budget before the socket is connected.
// PreAssociation.RTOInfo applies that through SCTP_FUTURE_ASSOC without
// wrapping or taking ownership of the raw descriptor that go-sctp still owns.
func dialAssociation(ctx context.Context, network string, laddr, raddr *sctp.SCTPAddr, timeout time.Duration, restarts *restartWatcher) (*sctp.SCTPConn, error) {
	attemptCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	base := &sctp.SocketConfig{
		// A client's watcher serves one association, so its route ignores the
		// association ID; it is set once Dial has a Conn to route to.
		NotificationHandler: restarts.handle,
		InitMsg: sctp.InitMsg{
			NumOstreams: sctp.SCTP_MAX_STREAM,
			// Belt and braces: the deadline above ends the attempt first, and
			// the raised RTO keeps the kernel from retransmitting inside it.
			MaxAttempts:    1,
			MaxInitTimeout: 1,
		},
	}
	cfg := base.WithPreAssociation(sctp.PreAssociationConfig{
		RTOInfo: &sctp.RtoInfo{
			AssocID: sctp.SCTPAssocID(sctp.SCTP_FUTURE_ASSOC),
			Initial: initialRTOFor(timeout),
		},
	})

	conn, err := cfg.DialContext(attemptCtx, network, laddr, raddr)
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
// signals and heartbeats are automatically handled background in another goroutine.
//
// Dial makes exactly one SCTP association attempt — one INIT — bounded by
// Config.InitTimeout, and never retries. A caller that wants to keep trying
// loops over Dial and chooses its own cadence, which is the point: left to the
// kernel's defaults an unanswered attempt runs to nine INIT chunks over 342
// seconds, far too long for an application to react to anything.
//
// Every path releases the socket before returning. On a timeout or a cancelled
// context the association is aborted at the deadline, so nothing further is put
// on the wire and the descriptor and kernel-side association are gone by the
// time Dial returns.
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
		return nil, ErrFailedToEstablish
	case <-ctx.Done():
		_ = conn.closeWith(ctx.Err())
		return nil, ctx.Err()
	case <-time.After(establishTimeout):
		_ = conn.closeWith(ErrTimeout)
		return nil, ErrTimeout
	}
}
