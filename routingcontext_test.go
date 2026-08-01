// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

// The Routing Context identifies which Application Server an ASP is asking to
// carry traffic for, and an SGP fronting several tenants is configured for all
// of their contexts while each ASP is configured for only its own. RFC 4666
// Section 4.3.4.3: "The Routing Context parameter MUST be included in the ASP
// Active Ack message(s) if the received ASP Active message contained any
// Routing Contexts", and the Ack names "the associated Routing Context(s)" —
// those being activated, not the receiver's whole inventory.
//
// The SGP answered ASP Active by echoing its *own* whole configured set,
// whatever the ASP had asked about. The ASP then checks the Ack against the
// contexts it asked for (see validateRoutingContext), finds the other tenants'
// contexts in it, and refuses — so in the multi-tenant configuration this
// library is being built for, no ASP could ever reach ASP-ACTIVE. A single
// tenant hid it, because there the two sets are identical.

// rcServerConfig is an SGP configured for several tenants' contexts.
func rcServerConfig(rcs ...uint32) *Config {
	cfg := NewServerConfig(
		&HeartbeatInfo{Enabled: false},
		0x22222222, 0x11111111, 1, params.TrafficModeLoadshare, 0, 0,
		rcs, params.ServiceIndSCCP, 0, 0, 1,
	)
	cfg.AspIdentifier = nil
	cfg.CorrelationID = nil
	served := make(map[uint32]struct{}, len(rcs))
	for _, rc := range rcs {
		served[rc] = struct{}{}
	}
	cfg.AuthorizeASP = func(identity ASPIdentity) []uint32 {
		if !identity.ASPIdentifierSet {
			return append([]uint32(nil), rcs...)
		}
		if _, ok := served[identity.ASPIdentifier]; ok {
			return []uint32{identity.ASPIdentifier}
		}
		return nil
	}
	return cfg
}

// rcClientConfig is one tenant's ASP, registered for its own context only.
func rcClientConfig(opc uint32, rcs ...uint32) *Config {
	aspID := opc
	if len(rcs) == 1 {
		aspID = rcs[0]
	}
	cfg := NewClientConfig(
		&HeartbeatInfo{Enabled: false},
		opc, 0x22222222, aspID, params.TrafficModeLoadshare, 0, 0,
		rcs, params.ServiceIndSCCP, 0, 0, 1,
	)
	cfg.CorrelationID = nil
	return cfg
}

// The headline case: an ASP registered for one of the SGP's contexts must be
// able to activate.
func TestASPWithASubsetOfTheSGPsRoutingContextsActivates(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	ln, err := Listen("m3ua", mcAddr(0, "127.0.0.1"), rcServerConfig(1, 2, 3))
	if err != nil {
		if isUnsupportedSCTP(err) {
			t.Skipf("skipping socket-backed test: %v", err)
		}
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	srvAddr := ln.Addr().(*sctp.SCTPAddr)

	type acceptResult struct {
		conn *Conn
		err  error
	}
	accepted := make(chan acceptResult, 1)
	go func() {
		c, err := ln.Accept(ctx)
		accepted <- acceptResult{conn: c, err: err}
	}()

	// This tenant is registered for context 2 only.
	cli, err := Dial(ctx, "m3ua", mcAddr(0, "127.0.0.2"), srvAddr, rcClientConfig(0xAA000001, 2))
	if err != nil {
		t.Fatalf("an ASP registered for context 2 could not activate against an SGP serving {1,2,3}: %v", err)
	}
	defer func() { _ = cli.Close() }()

	if got := cli.State(); got != StateAspActive {
		t.Errorf("ASP state = %v, want %v", got, StateAspActive)
	}
	select {
	case result := <-accepted:
		if result.conn != nil {
			t.Cleanup(func() { _ = result.conn.Close() })
		}
		if result.err != nil {
			t.Fatalf("Accept: %v", result.err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Accept never returned")
	}
}

// Two tenants, each with their own context, must both activate against the one
// SGP — the actual production shape.
func TestTwoASPsWithDistinctRoutingContextsBothActivate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	ln, err := Listen("m3ua", mcAddr(0, "127.0.0.1"), rcServerConfig(1, 2, 3))
	if err != nil {
		if isUnsupportedSCTP(err) {
			t.Skipf("skipping socket-backed test: %v", err)
		}
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	srvAddr := ln.Addr().(*sctp.SCTPAddr)

	type acceptResult struct {
		conn *Conn
		err  error
	}
	accepts := make(chan acceptResult, 2)
	go func() {
		for i := 0; i < 2; i++ {
			c, err := ln.Accept(ctx)
			accepts <- acceptResult{conn: c, err: err}
		}
	}()

	for i, tenant := range []struct {
		ip string
		rc uint32
	}{
		{"127.0.0.2", 1},
		{"127.0.0.3", 3},
	} {
		cli, err := Dial(ctx, "m3ua", mcAddr(0, tenant.ip), srvAddr,
			rcClientConfig(0xBB000000+uint32(i), tenant.rc))
		if err != nil {
			t.Fatalf("tenant with routing context %d could not activate: %v", tenant.rc, err)
		}
		defer func() { _ = cli.Close() }()

		if got := cli.State(); got != StateAspActive {
			t.Errorf("tenant %d state = %v, want %v", tenant.rc, got, StateAspActive)
		}
		select {
		case result := <-accepts:
			if result.conn != nil {
				t.Cleanup(func() { _ = result.conn.Close() })
			}
			if result.err != nil {
				t.Fatalf("Accept for tenant %d: %v", tenant.rc, result.err)
			}
		case <-time.After(15 * time.Second):
			t.Fatalf("Accept for tenant %d never returned", tenant.rc)
		}
	}
}

// The Ack must name what the ASP asked about, not the SGP's whole inventory.
func TestAspActiveAckEchoesTheASPsRoutingContext(t *testing.T) {
	conn, sent := newTestConn(t, StateAspInactive, modeServer)
	conn.cfg.RoutingContexts = params.NewRoutingContext(1, 2, 3)

	if err := conn.handleAspActive(
		messages.NewAspActive(params.NewTrafficModeType(params.TrafficModeLoadshare), params.NewRoutingContext(2), nil),
	); err != nil {
		t.Fatalf("handleAspActive: %v", err)
	}

	ack := lastAspActiveAck(t, *sent)
	if got := ack.RoutingContext.RoutingContexts(); len(got) != 1 || got[0] != 2 {
		t.Errorf("ASP Active Ack named routing contexts %v, want [2]: the SGP echoed its own inventory", got)
	}
}

// An ASP that omits the parameter is answered with the SGP's configured set, as
// before: there is nothing else to name.
func TestAspActiveAckWithoutARequestedContextUsesTheConfiguredSet(t *testing.T) {
	conn, sent := newTestConn(t, StateAspInactive, modeServer)
	conn.cfg.RoutingContexts = params.NewRoutingContext(1, 2, 3)

	if err := conn.handleAspActive(
		messages.NewAspActive(params.NewTrafficModeType(params.TrafficModeLoadshare), nil, nil),
	); err != nil {
		t.Fatalf("handleAspActive: %v", err)
	}

	ack := lastAspActiveAck(t, *sent)
	got := ack.RoutingContext.RoutingContexts()
	if len(got) != 3 {
		t.Errorf("ASP Active Ack named %v, want the configured [1 2 3]", got)
	}
}

// RFC 4666 Section 4.3.4.3: "If the RC parameter is included in the ASP Active
// message and a corresponding RK has not been previously defined (by either
// static configuration or dynamic registration), the peer MUST respond with an
// ERROR message with the Error Code 'No configured AS for ASP'."
//
// Not "Invalid Routing Context": that code has its own, more general use in
// Section 3.8.1, and this rule is the specific one for ASP Active.
func TestAspActiveForAnUnservedRoutingContextIsRefused(t *testing.T) {
	conn, sent := newTestConn(t, StateAspInactive, modeServer)
	conn.cfg.RoutingContexts = params.NewRoutingContext(1, 2, 3)

	err := conn.handleAspActive(
		messages.NewAspActive(params.NewTrafficModeType(params.TrafficModeLoadshare), params.NewRoutingContext(9), nil),
	)
	if err == nil {
		t.Fatal("handleAspActive accepted a routing context the SGP does not serve")
	}
	if !errors.Is(err, ErrNoConfiguredAS) {
		t.Errorf("error = %v, want a No Configured AS for ASP error", err)
	}
	for _, m := range *sent {
		if _, ok := m.(*messages.AspActiveAck); ok {
			t.Error("the SGP acknowledged activation for a context it does not serve")
		}
	}
}

// RFC 4666 Section 3.8.1: "For this error, the invalid Routing Context(s) MUST
// be included in the Error message." The Error used to carry the SGP's own
// configured contexts, telling the peer that ours were the invalid ones.
func TestErrorForAnUnservedRoutingContextNamesTheOffendingContexts(t *testing.T) {
	conn, sent := newTestConn(t, StateAspInactive, modeServer)
	conn.cfg.RoutingContexts = params.NewRoutingContext(1, 2, 3)

	err := conn.handleAspActive(
		messages.NewAspActive(params.NewTrafficModeType(params.TrafficModeLoadshare), params.NewRoutingContext(9, 2, 8), nil),
	)
	if err == nil {
		t.Fatal("handleAspActive accepted contexts the SGP does not serve")
	}

	// The dispatcher turns the error into the Error message.
	_ = conn.handleErrors(err)

	for i := len(*sent) - 1; i >= 0; i-- {
		e, ok := (*sent)[i].(*messages.Error)
		if !ok {
			continue
		}
		if got := e.ErrorCode.ErrorCode(); got != params.ErrNoConfiguredAsForAsp {
			t.Errorf("Error code = %#x, want %#x (No Configured AS for ASP)", got, params.ErrNoConfiguredAsForAsp)
		}
		got := e.RoutingContext.RoutingContexts()
		want := map[uint32]bool{9: true, 8: true}
		if len(got) != 2 || !want[got[0]] || !want[got[1]] {
			t.Errorf("Error named routing contexts %v, want exactly the unserved ones [9 8]", got)
		}
		return
	}
	t.Fatal("no Error message was sent")
}

// RFC 4666 Section 4.3.4.3: "Independently of the RC, the SGP MUST send an ASP
// Active Ack message in response to a received ASP Active message from the ASP,
// if the ASP is already marked in the APS-ACTIVE state."
//
// So an unserved Routing Context withholds the Ack only while the ASP is not
// already active. Refusing it once the ASP *is* active contradicts the MUST —
// which the first version of this change did.
func TestAspActiveWhileAlreadyActiveIsAckedWhateverTheRoutingContext(t *testing.T) {
	conn, sent := newTestConn(t, StateAspActive, modeServer)
	conn.cfg.RoutingContexts = params.NewRoutingContext(1, 2, 3)

	err := conn.handleAspActive(
		messages.NewAspActive(params.NewTrafficModeType(params.TrafficModeLoadshare), params.NewRoutingContext(9), nil),
	)
	if err == nil {
		t.Error("the unserved routing context was not reported at all")
	} else if !errors.Is(err, ErrNoConfiguredAS) {
		t.Errorf("error = %v, want a No Configured AS for ASP error", err)
	}

	found := false
	for _, m := range *sent {
		if _, ok := m.(*messages.AspActiveAck); ok {
			found = true
		}
	}
	if !found {
		t.Error("no ASP Active Ack was sent to an already-ASP-ACTIVE peer; RFC 4666 Section 4.3.4.3 requires one independently of the RC")
	}
}

// The same rule for ASP Inactive, whose Ack Section 4.3.4.4 owes even when the
// ASP is already ASP-INACTIVE.
func TestAspInactiveWhileAlreadyInactiveIsAckedWhateverTheRoutingContext(t *testing.T) {
	conn, sent := newTestConn(t, StateAspInactive, modeServer)
	conn.cfg.RoutingContexts = params.NewRoutingContext(1, 2, 3)

	err := conn.handleAspInactive(messages.NewAspInactive(params.NewRoutingContext(9), nil))
	if err == nil {
		t.Error("the unserved routing context was not reported at all")
	}

	found := false
	for _, m := range *sent {
		if _, ok := m.(*messages.AspInactiveAck); ok {
			found = true
		}
	}
	if !found {
		t.Error("no ASP Inactive Ack was sent to an already-ASP-INACTIVE peer")
	}
}

// ASP Inactive Ack carries the Routing Context too, and must follow the same
// rule.
func TestAspInactiveAckEchoesTheASPsRoutingContext(t *testing.T) {
	conn, sent := newTestConn(t, StateAspActive, modeServer)
	conn.cfg.RoutingContexts = params.NewRoutingContext(1, 2, 3)

	if err := conn.handleAspInactive(
		messages.NewAspInactive(params.NewRoutingContext(3), nil),
	); err != nil {
		t.Fatalf("handleAspInactive: %v", err)
	}

	for i := len(*sent) - 1; i >= 0; i-- {
		if ack, ok := (*sent)[i].(*messages.AspInactiveAck); ok {
			if got := ack.RoutingContext.RoutingContexts(); len(got) != 1 || got[0] != 3 {
				t.Errorf("ASP Inactive Ack named %v, want [3]", got)
			}
			return
		}
	}
	t.Fatal("no ASP Inactive Ack was sent")
}

func lastAspActiveAck(t *testing.T, sent []messages.M3UA) *messages.AspActiveAck {
	t.Helper()

	for i := len(sent) - 1; i >= 0; i-- {
		if ack, ok := sent[i].(*messages.AspActiveAck); ok {
			return ack
		}
	}
	t.Fatalf("no ASP Active Ack was sent (got %v)", typeNames(sent))
	return nil
}

// isUnsupportedSCTP reports whether an error is the sctp package's
// platform-support refusal, which is how the socket tests skip off Linux.
func isUnsupportedSCTP(err error) bool {
	return err != nil && isSCTPUnsupported(err)
}

// The two ASPTM messages are given different error codes by the RFC for the
// same condition, so a single shared code cannot satisfy both.
//
//	Section 4.3.4.3 (ASP Active):   "the peer MUST respond with an ERROR
//	  message with the Error Code 'No configured AS for ASP'."
//	Section 4.3.4.4 (ASP Inactive): "the SGP/IPSP MUST respond with an ERROR
//	  message with the Error Code 'Invalid Routing Context'."
func TestUnservedRoutingContextUsesThePerMessageErrorCode(t *testing.T) {
	for _, tt := range []struct {
		name  string
		state State
		send  func(*Conn) error
		want  uint32
	}{
		{
			name:  "ASP Active",
			state: StateAspInactive,
			send: func(c *Conn) error {
				return c.handleAspActive(messages.NewAspActive(
					params.NewTrafficModeType(params.TrafficModeLoadshare),
					params.NewRoutingContext(9), nil))
			},
			want: params.ErrNoConfiguredAsForAsp,
		},
		{
			name:  "ASP Inactive",
			state: StateAspActive,
			send: func(c *Conn) error {
				return c.handleAspInactive(messages.NewAspInactive(params.NewRoutingContext(9), nil))
			},
			want: params.ErrInvalidRoutingContext,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conn, sent := newTestConn(t, tt.state, modeServer)
			conn.cfg.RoutingContexts = params.NewRoutingContext(1, 2, 3)

			err := tt.send(conn)
			if err == nil {
				t.Fatalf("%s with an unserved routing context was accepted", tt.name)
			}
			_ = conn.handleErrors(err)

			for i := len(*sent) - 1; i >= 0; i-- {
				e, ok := (*sent)[i].(*messages.Error)
				if !ok {
					continue
				}
				if got := e.ErrorCode.ErrorCode(); got != tt.want {
					t.Errorf("%s error code = %#x, want %#x", tt.name, got, tt.want)
				}
				if got := e.RoutingContext.RoutingContexts(); len(got) != 1 || got[0] != 9 {
					t.Errorf("%s Error named %v, want the offending [9]", tt.name, got)
				}
				return
			}
			t.Fatalf("%s sent no Error message", tt.name)
		})
	}
}

// A request naming both a served and an unserved Routing Context still owes an
// Ack for the served one.
//
// RFC 4666 Section 4.3.4.3: "If the RC parameter is included in the ASP Active
// message and the corresponding RK has been previously defined (by either
// static configuration or dynamic registration), the peer node MUST respond
// with an ASP Active Ack message", and "Multiple ASP Active Ack messages MAY be
// used in response to an ASP Active message containing multiple Routing
// Contexts, allowing the SGP or IPSP to independently acknowledge the ASP
// Active message for different (sets of) Routing Contexts."
//
// The first unserved context aborted the whole request, so an ASP asking about
// one context it is registered for and one it is not got an Error and nothing
// else — and never activated for the context it was entitled to.
func TestMixedRoutingContextAcksTheServedOnesAndRefusesTheRest(t *testing.T) {
	conn, sent := newTestConn(t, StateAspInactive, modeServer)
	conn.cfg.RoutingContexts = params.NewRoutingContext(1, 2, 3)

	err := conn.handleAspActive(messages.NewAspActive(
		params.NewTrafficModeType(params.TrafficModeLoadshare),
		params.NewRoutingContext(2, 9), nil,
	))

	// The unserved context is still reported.
	if !errors.Is(err, ErrNoConfiguredAS) {
		t.Errorf("error = %v, want a No Configured AS for ASP error for context 9", err)
	}

	ack := lastAspActiveAck(t, *sent)
	got := ack.RoutingContext.RoutingContexts()
	if len(got) != 1 || got[0] != 2 {
		t.Errorf("ASP Active Ack named %v, want [2]: the served context was refused along with the unserved one", got)
	}

	// And the Error must name only the unserved one.
	_ = conn.handleErrors(err)
	for i := len(*sent) - 1; i >= 0; i-- {
		e, ok := (*sent)[i].(*messages.Error)
		if !ok {
			continue
		}
		if rcs := e.RoutingContext.RoutingContexts(); len(rcs) != 1 || rcs[0] != 9 {
			t.Errorf("Error named %v, want only the unserved [9]", rcs)
		}
		return
	}
	t.Fatal("no Error message was sent")
}

// A request naming only unserved contexts gets no Ack at all: there is nothing
// to acknowledge.
func TestWhollyUnservedRoutingContextGetsNoAck(t *testing.T) {
	conn, sent := newTestConn(t, StateAspInactive, modeServer)
	conn.cfg.RoutingContexts = params.NewRoutingContext(1, 2, 3)

	err := conn.handleAspActive(messages.NewAspActive(
		params.NewTrafficModeType(params.TrafficModeLoadshare),
		params.NewRoutingContext(8, 9), nil,
	))
	if !errors.Is(err, ErrNoConfiguredAS) {
		t.Errorf("error = %v, want a No Configured AS for ASP error", err)
	}
	for _, m := range *sent {
		if _, ok := m.(*messages.AspActiveAck); ok {
			t.Error("acknowledged activation for contexts the SGP does not serve")
		}
	}
}

// RFC 4666 Section 4.3.4.3: "If the RC parameter is not included in the ASP
// Active message and there are no RKs defined, the peer node SHOULD respond
// with and ERROR message with the Error Code 'Invalid Routing Context'."
//
// A Config with no Routing Contexts has no Routing Keys, so an ASP Active
// without the parameter cannot be resolved to any Application Server. It was
// acknowledged anyway, activating the ASP for nothing.
func TestAspActiveWithNoContextAndNoRoutingKeysIsRefused(t *testing.T) {
	conn, sent := newTestConn(t, StateAspInactive, modeServer)
	conn.cfg.RoutingContexts = nil

	err := conn.handleAspActive(messages.NewAspActive(
		params.NewTrafficModeType(params.TrafficModeLoadshare), nil, nil,
	))
	if !errors.Is(err, ErrInvalidRoutingContext) {
		t.Errorf("error = %v, want an Invalid Routing Context error", err)
	}
	for _, m := range *sent {
		if _, ok := m.(*messages.AspActiveAck); ok {
			t.Error("acknowledged an ASP Active that names no context against a config with no routing keys")
		}
	}
}

// Section 4.3.4.4 gives ASP Inactive the other code for the same shape:
// "the SGP/IPSP MUST respond with an ERROR message with the Error Code
// 'No configured AS for ASP'."
func TestAspInactiveWithNoContextAndNoRoutingKeysIsRefused(t *testing.T) {
	conn, _ := newTestConn(t, StateAspActive, modeServer)
	conn.cfg.RoutingContexts = nil

	err := conn.handleAspInactive(messages.NewAspInactive(nil, nil))
	if !errors.Is(err, ErrNoConfiguredAS) {
		t.Errorf("error = %v, want a No Configured AS for ASP error", err)
	}
}

func TestExplicitContextCannotCreateItsOwnRoutingKey(t *testing.T) {
	for _, test := range []struct {
		name    string
		state   State
		handle  func(*Conn) error
		want    error
		ackType string
	}{
		{
			name:  "ASP Active",
			state: StateAspInactive,
			handle: func(connection *Conn) error {
				return connection.handleAspActive(messages.NewAspActive(
					params.NewTrafficModeType(params.TrafficModeLoadshare),
					params.NewRoutingContext(77), nil,
				))
			},
			want:    ErrNoConfiguredAS,
			ackType: "ASP Active Ack",
		},
		{
			name:  "ASP Inactive",
			state: StateAspActive,
			handle: func(connection *Conn) error {
				return connection.handleAspInactive(messages.NewAspInactive(
					params.NewRoutingContext(77), nil,
				))
			},
			want:    ErrInvalidRoutingContext,
			ackType: "ASP Inactive Ack",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			for _, empty := range []bool{false, true} {
				connection, sent := newTestConn(t, test.state, modeServer)
				connection.cfg.RoutingContexts = nil
				if empty {
					connection.cfg.RoutingContexts = params.NewRoutingContext()
				}
				if err := test.handle(connection); !errors.Is(err, test.want) {
					t.Fatalf("empty=%t error = %v, want %v", empty, err, test.want)
				}
				for _, signal := range *sent {
					if signal.MessageTypeName() == test.ackType {
						t.Fatalf("empty=%t sent %s for an unconfigured explicit context", empty, test.ackType)
					}
				}
			}
		})
	}
}
