// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"reflect"
	"strconv"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

const ssnmScopePointCode = 0x252525

type ssnmScopeCase struct {
	name string
	call func(*Conn, *params.Param) error
}

func aspBoundSSNMScopeCases() []ssnmScopeCase {
	return []ssnmScopeCase{
		{name: "DUNA", call: func(c *Conn, routingContext *params.Param) error {
			return c.handleDestinationUnavailable(messages.NewDestinationUnavailable(
				nil, routingContext, params.NewAffectedPointCodeWithMask(0, ssnmScopePointCode), nil))
		}},
		{name: "DAVA", call: func(c *Conn, routingContext *params.Param) error {
			return c.handleDestinationAvailable(messages.NewDestinationAvailable(
				nil, routingContext, params.NewAffectedPointCodeWithMask(0, ssnmScopePointCode), nil))
		}},
		{name: "DRST", call: func(c *Conn, routingContext *params.Param) error {
			return c.handleDestinationRestricted(messages.NewDestinationRestricted(
				nil, routingContext, params.NewAffectedPointCodeWithMask(0, ssnmScopePointCode), nil))
		}},
		{name: "SCON", call: func(c *Conn, routingContext *params.Param) error {
			return c.handleSignallingCongestion(messages.NewSignallingCongestion(
				nil, routingContext, params.NewAffectedPointCodeWithMask(0, ssnmScopePointCode),
				nil, params.NewCongestionIndications(2), nil))
		}},
		{name: "DUPU", call: func(c *Conn, routingContext *params.Param) error {
			return c.handleDestinationUserPartUnavailable(messages.NewDestinationUserPartUnavailable(
				nil, routingContext, params.NewAffectedPointCodeWithMask(0, ssnmScopePointCode),
				params.NewUserCause(params.SCCP, params.Unequipped), nil))
		}},
	}
}

func sgpBoundSSNMScopeCases() []ssnmScopeCase {
	return []ssnmScopeCase{
		{name: "DAUD", call: func(c *Conn, routingContext *params.Param) error {
			return c.handleDestinationStateAudit(messages.NewDestinationStateAudit(
				nil, routingContext, params.NewAffectedPointCodeWithMask(0, ssnmScopePointCode), nil))
		}},
		{name: "SCON", call: func(c *Conn, routingContext *params.Param) error {
			return c.handleSignallingCongestion(messages.NewSignallingCongestion(
				nil, routingContext, params.NewAffectedPointCodeWithMask(0, ssnmScopePointCode),
				nil, params.NewCongestionIndications(2), nil))
		}},
	}
}

func assertNoSSNMScopeStatus(t *testing.T, c *Conn) {
	t.Helper()
	select {
	case status := <-c.SignallingStatus():
		t.Fatalf("rejected SSNM published status %#v", status)
	default:
	}
}

// Routing Context is Conditional in every SSNM message. RFC 4666 Section
// 3.4.1 makes the condition concrete: on an association carrying several
// Routing Keys/Contexts, it MUST be present to identify the traffic flow. The
// remaining SSNM sections adopt DUNA's parameter description.
func TestSSNMRoutingContextConditionality(t *testing.T) {
	t.Run("multi-flow association requires it", func(t *testing.T) {
		for _, role := range []struct {
			name  string
			mode  mode
			cases []ssnmScopeCase
		}{
			{name: "ASP receives", mode: modeClient, cases: aspBoundSSNMScopeCases()},
			{name: "SGP receives", mode: modeServer, cases: sgpBoundSSNMScopeCases()},
		} {
			t.Run(role.name, func(t *testing.T) {
				for _, message := range role.cases {
					t.Run(message.name, func(t *testing.T) {
						conn, sent := newTestConnWithContexts(t, StateAspActive, role.mode, 1, 2)
						err := message.call(conn, nil)
						if !errors.Is(err, ErrMissingRoutingContext) {
							t.Fatalf("error = %v, want ErrMissingRoutingContext", err)
						}
						if len(*sent) != 0 {
							t.Errorf("rejected %s wrote %v", message.name, typeNames(*sent))
						}
						assertNoSSNMScopeStatus(t, conn)
					})
				}
			})
		}
	})

	t.Run("single-flow association permits omission", func(t *testing.T) {
		for _, role := range []struct {
			name  string
			mode  mode
			cases []ssnmScopeCase
		}{
			{name: "ASP receives", mode: modeClient, cases: aspBoundSSNMScopeCases()},
			{name: "SGP receives", mode: modeServer, cases: sgpBoundSSNMScopeCases()},
		} {
			t.Run(role.name, func(t *testing.T) {
				for _, message := range role.cases {
					t.Run(message.name, func(t *testing.T) {
						conn, _ := newTestConnWithContexts(t, StateAspActive, role.mode, 7)
						if err := message.call(conn, nil); err != nil {
							t.Fatalf("single-flow %s without Routing Context: %v", message.name, err)
						}
					})
				}
			})
		}
	})
}

// RFC 4666 Section 4.3.1 maintains ASP state per Application Server and says
// an inactive ASP should not receive DATA or SSNM for that AS. At an ASP, an
// out-of-scope SGP report is silently discarded so it cannot steer another
// flow. At an SGP, an ASP-originated DAUD/SCON for an inactive flow is an
// unexpected message and is rejected. A mixed list is atomic: accepting its
// active member must not apply the same report to its inactive member.
func TestSSNMHonoursPerRoutingContextActiveState(t *testing.T) {
	t.Run("ASP silently ignores inactive scopes", func(t *testing.T) {
		for _, message := range aspBoundSSNMScopeCases() {
			for _, scope := range []struct {
				name string
				rcs  []uint32
			}{
				{name: "inactive only", rcs: []uint32{2}},
				{name: "mixed active and inactive", rcs: []uint32{1, 2}},
			} {
				t.Run(message.name+"/"+scope.name, func(t *testing.T) {
					conn, sent := newTestConnWithContexts(t, StateAspActive, modeClient, 1, 2)
					conn.noteRoutingContextsAcked(params.NewRoutingContext(1))

					if err := message.call(conn, params.NewRoutingContext(scope.rcs...)); err != nil {
						t.Fatalf("inactive-flow %s was not silently ignored: %v", message.name, err)
					}
					if len(*sent) != 0 {
						t.Errorf("silently ignored %s wrote %v", message.name, typeNames(*sent))
					}
					assertNoSSNMScopeStatus(t, conn)
					if got := conn.DestinationState(ssnmScopePointCode); got != DestinationAvailable {
						t.Errorf("inactive-flow %s changed destination state to %v", message.name, got)
					}
				})
			}
		}
	})

	t.Run("ASP still applies active scope", func(t *testing.T) {
		for _, message := range aspBoundSSNMScopeCases() {
			t.Run(message.name, func(t *testing.T) {
				conn, _ := newTestConnWithContexts(t, StateAspActive, modeClient, 1, 2)
				conn.noteRoutingContextsAcked(params.NewRoutingContext(1))
				if err := message.call(conn, params.NewRoutingContext(1)); err != nil {
					t.Fatalf("active-flow %s: %v", message.name, err)
				}
				_ = nextStatus(t, conn)
			})
		}
	})

	t.Run("SGP rejects inactive scopes", func(t *testing.T) {
		for _, message := range sgpBoundSSNMScopeCases() {
			for _, scope := range []struct {
				name string
				rcs  []uint32
			}{
				{name: "inactive only", rcs: []uint32{2}},
				{name: "mixed active and inactive", rcs: []uint32{1, 2}},
			} {
				t.Run(message.name+"/"+scope.name, func(t *testing.T) {
					conn, sent := newTestConnWithContexts(t, StateAspActive, modeServer, 1, 2)
					conn.noteRoutingContextsActive([]uint32{1})

					err := message.call(conn, params.NewRoutingContext(scope.rcs...))
					var unexpected *UnexpectedMessageError
					if !errors.As(err, &unexpected) {
						t.Fatalf("error = %v, want UnexpectedMessageError", err)
					}
					if len(*sent) != 0 {
						t.Errorf("rejected %s wrote %v", message.name, typeNames(*sent))
					}
					assertNoSSNMScopeStatus(t, conn)
					if got := conn.PeerCongestionLevel(); got != 0 {
						t.Errorf("rejected %s changed peer congestion to %d", message.name, got)
					}
				})
			}
		}
	})

	t.Run("SGP still handles active scope", func(t *testing.T) {
		for _, message := range sgpBoundSSNMScopeCases() {
			t.Run(message.name, func(t *testing.T) {
				conn, sent := newTestConnWithContexts(t, StateAspActive, modeServer, 1, 2)
				conn.noteRoutingContextsActive([]uint32{1})
				if err := message.call(conn, params.NewRoutingContext(1)); err != nil {
					t.Fatalf("active-flow %s: %v", message.name, err)
				}
				if message.name == "DAUD" && len(*sent) == 0 {
					t.Fatal("active-flow DAUD produced no response")
				}
				if message.name == "SCON" {
					_ = nextStatus(t, conn)
				}
			})
		}
	})
}

// Section 4.5.1 explicitly permits DUNA, DRST and SCON before ASP Active Ack,
// and Section 4.6 permits DAVA there when an MTP3 restart is completing.
// Their named Routing Context must remain usable in that window even though no
// context has been acknowledged yet. The exception belongs to the newly
// activating ASP, not to an inactive ASP sending SCON toward an SGP.
func TestScopedSSNMActivationWindow(t *testing.T) {
	t.Run("requires an outstanding ASP Active", func(t *testing.T) {
		conn, _ := newTestConnWithContexts(t, StateAspInactive, modeClient, 1, 2)
		err := aspBoundSSNMScopeCases()[0].call(conn, params.NewRoutingContext(2))
		var unexpected *UnexpectedMessageError
		if !errors.As(err, &unexpected) {
			t.Fatalf("inactive DUNA without pending activation = %v, want UnexpectedMessageError", err)
		}
		assertNoSSNMScopeStatus(t, conn)
	})

	for _, message := range []ssnmScopeCase{
		aspBoundSSNMScopeCases()[0],
		aspBoundSSNMScopeCases()[1],
		aspBoundSSNMScopeCases()[2],
		aspBoundSSNMScopeCases()[3],
	} {
		t.Run(message.name, func(t *testing.T) {
			conn, _ := newTestConnWithContexts(t, StateAspInactive, modeClient, 1, 2)
			conn.noteRoutingContextsAcked(params.NewRoutingContext(1))
			conn.startTAck(messages.NewAspActive(
				conn.cfg.TrafficModeType.Copy(), params.NewRoutingContext(2), nil,
			), requestAspActive)
			if err := message.call(conn, params.NewRoutingContext(2)); err != nil {
				t.Fatalf("%s for the activating RC was rejected: %v", message.name, err)
			}
			_ = nextStatus(t, conn)
		})
	}

	t.Run("does not apply at the SGP", func(t *testing.T) {
		conn, _ := newTestConnWithContexts(t, StateAspInactive, modeServer, 1)
		err := sgpBoundSSNMScopeCases()[1].call(conn, nil)
		var unexpected *UnexpectedMessageError
		if !errors.As(err, &unexpected) {
			t.Fatalf("inactive ASP's SCON error = %v, want UnexpectedMessageError", err)
		}
		assertNoSSNMScopeStatus(t, conn)
	})

	t.Run("second RC while association remains active", func(t *testing.T) {
		for _, message := range []ssnmScopeCase{
			aspBoundSSNMScopeCases()[0],
			aspBoundSSNMScopeCases()[1],
			aspBoundSSNMScopeCases()[2],
			aspBoundSSNMScopeCases()[3],
		} {
			for _, scope := range []struct {
				name string
				rcs  []uint32
			}{
				{name: "pending only", rcs: []uint32{2}},
				{name: "active and pending", rcs: []uint32{1, 2}},
			} {
				t.Run(message.name+"/"+scope.name, func(t *testing.T) {
					conn, _ := newTestConnWithContexts(t, StateAspActive, modeClient, 1, 2, 3)
					conn.noteRoutingContextsAcked(params.NewRoutingContext(1))
					conn.startTAck(messages.NewAspActive(
						conn.cfg.TrafficModeType.Copy(), params.NewRoutingContext(2), nil,
					), requestAspActive)

					if err := message.call(conn, params.NewRoutingContext(scope.rcs...)); err != nil {
						t.Fatalf("%s for pending activation scope %v: %v", message.name, scope.rcs, err)
					}
					_ = nextStatus(t, conn)
				})
			}
		}
	})

	t.Run("does not widen beyond the pending RC", func(t *testing.T) {
		conn, _ := newTestConnWithContexts(t, StateAspActive, modeClient, 1, 2, 3)
		conn.noteRoutingContextsAcked(params.NewRoutingContext(1))
		conn.startTAck(messages.NewAspActive(
			conn.cfg.TrafficModeType.Copy(), params.NewRoutingContext(2), nil,
		), requestAspActive)

		if err := aspBoundSSNMScopeCases()[0].call(conn, params.NewRoutingContext(3)); err != nil {
			t.Fatalf("out-of-window RC was not silently ignored: %v", err)
		}
		assertNoSSNMScopeStatus(t, conn)
	})
}

// DestinationStatus is the public replacement for MTP-PAUSE/RESUME/STATUS
// primitives. An application serving several ASes must receive the exact RC
// list and its presence bit to distribute the indication to the right users.
func TestDestinationStatusPreservesSSNMRoutingContexts(t *testing.T) {
	for _, message := range aspBoundSSNMScopeCases() {
		t.Run("ASP receives/"+message.name, func(t *testing.T) {
			conn, _ := newTestConnWithContexts(t, StateAspActive, modeClient, 1, 2)
			if err := message.call(conn, params.NewRoutingContext(2, 1)); err != nil {
				t.Fatal(err)
			}
			status := nextStatus(t, conn)
			if !status.RoutingContextSet || !reflect.DeepEqual(status.RoutingContexts, []uint32{2, 1}) {
				t.Errorf("Routing Contexts = %v (set=%v), want [2 1] (set=true)",
					status.RoutingContexts, status.RoutingContextSet)
			}
		})
	}

	t.Run("SGP receives SCON", func(t *testing.T) {
		conn, _ := newTestConnWithContexts(t, StateAspActive, modeServer, 1, 2)
		if err := sgpBoundSSNMScopeCases()[1].call(conn, params.NewRoutingContext(2, 1)); err != nil {
			t.Fatal(err)
		}
		status := nextStatus(t, conn)
		if !status.RoutingContextSet || !reflect.DeepEqual(status.RoutingContexts, []uint32{2, 1}) {
			t.Errorf("Routing Contexts = %v (set=%v), want [2 1] (set=true)",
				status.RoutingContexts, status.RoutingContextSet)
		}
	})

	for _, presence := range []struct {
		name    string
		param   *params.Param
		want    []uint32
		wantSet bool
	}{
		{name: "explicit zero", param: params.NewRoutingContext(0), want: []uint32{0}, wantSet: true},
		{name: "omitted", param: nil, want: nil, wantSet: false},
	} {
		t.Run(presence.name, func(t *testing.T) {
			conn, _ := newTestConnWithContexts(t, StateAspActive, modeClient, 0)
			if err := aspBoundSSNMScopeCases()[0].call(conn, presence.param); err != nil {
				t.Fatal(err)
			}
			status := nextStatus(t, conn)
			if status.RoutingContextSet != presence.wantSet ||
				!reflect.DeepEqual(status.RoutingContexts, presence.want) {
				t.Errorf("Routing Contexts = %v (set=%v), want %v (set=%v)",
					status.RoutingContexts, status.RoutingContextSet, presence.want, presence.wantSet)
			}
		})
	}

	t.Run("statuses do not alias their RC lists", func(t *testing.T) {
		conn, _ := newTestConnWithContexts(t, StateAspActive, modeClient, 1, 2)
		err := conn.handleDestinationUnavailable(messages.NewDestinationUnavailable(
			nil, params.NewRoutingContext(1, 2),
			params.NewAffectedPointCode(0x111111, 0x222222), nil))
		if err != nil {
			t.Fatal(err)
		}
		first := nextStatus(t, conn)
		second := nextStatus(t, conn)
		if len(first.RoutingContexts) == 0 {
			t.Fatal("first status omitted its Routing Context list")
		}
		first.RoutingContexts[0] = 99
		if !reflect.DeepEqual(second.RoutingContexts, []uint32{1, 2}) {
			t.Errorf("mutating one status changed another's Routing Contexts to %v", second.RoutingContexts)
		}
	})

	t.Run("wire-decoded list reaches the status", func(t *testing.T) {
		conn, _ := newTestConnWithContexts(t, StateAspActive, modeClient, 1, 2)
		message := messages.NewDestinationUnavailable(
			nil, params.NewRoutingContext(2, 1),
			params.NewAffectedPointCodeWithMask(0, ssnmScopePointCode), nil,
		)
		raw, err := message.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}

		conn.dispatchRaw(context.Background(), inbound{data: raw, stream: 0, ppid: M3UAPPID})
		status := nextStatus(t, conn)
		if !status.RoutingContextSet || !reflect.DeepEqual(status.RoutingContexts, []uint32{2, 1}) {
			t.Errorf("wire Routing Contexts = %v (set=%v), want [2 1] (set=true)",
				status.RoutingContexts, status.RoutingContextSet)
		}
	})
}

// RFC 4666 Section 3.4.4 restricts Concerned Destination to SCON sent from an
// ASP to an SGP. It identifies the originator of the message that triggered
// congestion and therefore must reach the SGP-side status consumer unchanged.
func TestSCONConcernedDestinationDirectionAndStatus(t *testing.T) {
	for _, presence := range []struct {
		name    string
		param   *params.Param
		want    uint32
		wantSet bool
	}{
		{name: "non-zero", param: params.NewConcernedDestination(0x123456), want: 0x123456, wantSet: true},
		{name: "explicit zero", param: params.NewConcernedDestination(0), want: 0, wantSet: true},
		{name: "omitted", param: nil, want: 0, wantSet: false},
	} {
		t.Run("ASP to SGP/"+presence.name, func(t *testing.T) {
			conn, _ := newTestConnWithContexts(t, StateAspActive, modeServer, 1)
			err := conn.handleSignallingCongestion(messages.NewSignallingCongestion(
				nil, nil, params.NewAffectedPointCodeWithMask(0, ssnmScopePointCode),
				presence.param, params.NewCongestionIndications(1), nil))
			if err != nil {
				t.Fatal(err)
			}
			status := nextStatus(t, conn)
			if status.ConcernedDestination != presence.want ||
				status.ConcernedDestinationSet != presence.wantSet {
				t.Errorf("Concerned Destination = %#x (set=%v), want %#x (set=%v)",
					status.ConcernedDestination, status.ConcernedDestinationSet,
					presence.want, presence.wantSet)
			}
		})
	}

	t.Run("SGP to ASP is rejected", func(t *testing.T) {
		conn, _ := newTestConnWithContexts(t, StateAspActive, modeClient, 1)
		err := conn.handleSignallingCongestion(messages.NewSignallingCongestion(
			nil, nil, params.NewAffectedPointCodeWithMask(0, ssnmScopePointCode),
			params.NewConcernedDestination(0x123456), params.NewCongestionIndications(1), nil))
		if !errors.Is(err, ErrInvalidParameterValue) {
			t.Fatalf("error = %v, want ErrInvalidParameterValue", err)
		}
		assertNoSSNMScopeStatus(t, conn)
		if got := conn.DestinationState(ssnmScopePointCode); got != DestinationAvailable {
			t.Errorf("rejected SCON changed destination state to %v", got)
		}
	})
}

// The Congestion Level field has exactly four defined values, 0 through 3.
// Larger uint8 values are well-formed encodings but invalid parameter values.
func TestSCONRejectsUndefinedCongestionLevels(t *testing.T) {
	for _, role := range []struct {
		name string
		mode mode
	}{
		{name: "SGP to ASP", mode: modeClient},
		{name: "ASP to SGP", mode: modeServer},
	} {
		t.Run(role.name, func(t *testing.T) {
			for _, level := range []uint8{4, 255} {
				t.Run(strconv.Itoa(int(level)), func(t *testing.T) {
					conn, _ := newTestConnWithContexts(t, StateAspActive, role.mode, 1)
					err := conn.handleSignallingCongestion(messages.NewSignallingCongestion(
						nil, nil, params.NewAffectedPointCodeWithMask(0, ssnmScopePointCode),
						nil, params.NewCongestionIndications(level), nil))
					if !errors.Is(err, ErrInvalidParameterValue) {
						t.Fatalf("level %d error = %v, want ErrInvalidParameterValue", level, err)
					}
					assertNoSSNMScopeStatus(t, conn)
					if got := conn.DestinationState(ssnmScopePointCode); got != DestinationAvailable {
						t.Errorf("level %d changed destination state to %v", level, got)
					}
					if got := conn.PeerCongestionLevel(); got != 0 {
						t.Errorf("level %d changed peer congestion to %d", level, got)
					}
				})
			}
		})
	}

	t.Run("all defined levels remain accepted", func(t *testing.T) {
		for _, role := range []struct {
			name string
			mode mode
		}{
			{name: "SGP to ASP", mode: modeClient},
			{name: "ASP to SGP", mode: modeServer},
		} {
			for level := uint8(0); level <= 3; level++ {
				t.Run(role.name+"/"+string(rune('0'+level)), func(t *testing.T) {
					conn, _ := newTestConnWithContexts(t, StateAspActive, role.mode, 1)
					err := conn.handleSignallingCongestion(messages.NewSignallingCongestion(
						nil, nil, params.NewAffectedPointCodeWithMask(0, ssnmScopePointCode),
						nil, params.NewCongestionIndications(level), nil))
					if err != nil {
						t.Fatalf("defined congestion level %d: %v", level, err)
					}
					if got := nextStatus(t, conn).CongestionLevel; got != level {
						t.Errorf("reported level = %d, want %d", got, level)
					}
				})
			}
		}
	})
}

// Handler errors are consumed by the monitor, which builds the on-wire ERR.
// Assert the protocol codes as well as the local sentinel/type so a refactor
// cannot keep unit tests green while reporting the wrong fault to the peer.
func TestSSNMScopeErrorsProduceRFCErrorMessages(t *testing.T) {
	t.Run("missing Routing Context", func(t *testing.T) {
		conn, sent := newTestConnWithContexts(t, StateAspActive, modeClient, 1, 2)
		err := aspBoundSSNMScopeCases()[0].call(conn, nil)
		if handleErr := conn.handleErrors(err); handleErr != nil {
			t.Fatalf("handleErrors: %v", handleErr)
		}
		response := lastError(t, *sent)
		if response.ErrorCode == nil || response.ErrorCode.ErrorCode() != params.ErrMissingParameter {
			t.Fatalf("Error Code = %v, want Missing Parameter", response.ErrorCode)
		}
	})

	t.Run("invalid congestion level", func(t *testing.T) {
		conn, sent := newTestConnWithContexts(t, StateAspActive, modeClient, 1)
		err := conn.handleSignallingCongestion(messages.NewSignallingCongestion(
			nil, nil, params.NewAffectedPointCodeWithMask(0, ssnmScopePointCode),
			nil, params.NewCongestionIndications(4), nil))
		if handleErr := conn.handleErrors(err); handleErr != nil {
			t.Fatalf("handleErrors: %v", handleErr)
		}
		response := lastError(t, *sent)
		if response.ErrorCode == nil || response.ErrorCode.ErrorCode() != params.ErrInvalidParameterValue {
			t.Fatalf("Error Code = %v, want Invalid Parameter Value", response.ErrorCode)
		}
	})

	t.Run("inactive SGP scope", func(t *testing.T) {
		conn, sent := newTestConnWithContexts(t, StateAspActive, modeServer, 1, 2)
		conn.noteRoutingContextsActive([]uint32{1})
		err := sgpBoundSSNMScopeCases()[0].call(conn, params.NewRoutingContext(2))
		if handleErr := conn.handleErrors(err); handleErr != nil {
			t.Fatalf("handleErrors: %v", handleErr)
		}
		response := lastError(t, *sent)
		if response.ErrorCode == nil || response.ErrorCode.ErrorCode() != params.UnexpectedMessageError {
			t.Fatalf("Error Code = %v, want Unexpected Message", response.ErrorCode)
		}
		if response.RoutingContext == nil ||
			!reflect.DeepEqual(response.RoutingContext.RoutingContexts(), []uint32{2}) {
			t.Errorf("Error Routing Contexts = %v, want [2]",
				response.RoutingContext.RoutingContexts())
		}
	})
}
