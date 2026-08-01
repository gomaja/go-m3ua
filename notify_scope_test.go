// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

var scopedNotifyStatuses = []uint32{
	params.AsStateInactive,
	params.AsStateActive,
	params.AsStatePending,
	params.InsufficientAspResources,
	params.AlternateAspActive,
	params.AspFailure,
}

// Errata ID 2065 makes Routing Context Conditional: a sender must include it
// for a subset, while RFC 4666 Section 4.3.4.5 keeps omission meaningful when
// configuration identifies all Application Servers the ASP belongs to.
func TestContextlessNotifyUsesConfiguredScopeForEveryDefinedStatus(t *testing.T) {
	for _, status := range scopedNotifyStatuses {
		t.Run(notifyStatusName(status), func(t *testing.T) {
			conn, _ := newTestConnWithContexts(t, StateAspActive, modeClient, 10, 20)

			err := conn.handleNotify(messages.NewNotify(
				params.NewStatus(status), nil, nil, nil))
			if err != nil {
				t.Fatalf("handleNotify: %v", err)
			}
			if got := conn.State(); got != StateAspActive {
				t.Errorf("state = %v after advisory handler, want %v", got, StateAspActive)
			}
			indication := <-conn.ManagementIndications()
			if indication.RoutingContextSet {
				t.Errorf("inferred scope was reported as an explicit Routing Context %d",
					indication.RoutingContext)
			}
			if !equalNotifyScope(indication.RoutingContexts, []uint32{10, 20}) {
				t.Errorf("inferred Routing Contexts = %v, want [10 20]", indication.RoutingContexts)
			}
			binary.BigEndian.PutUint32(conn.cfg.RoutingContexts.Data[0:4], 30)
			if !equalNotifyScope(indication.RoutingContexts, []uint32{10, 20}) {
				t.Errorf("indication scope aliased configured membership: %v", indication.RoutingContexts)
			}
		})
	}
}

func TestNotifyPreservesEveryExplicitRoutingContext(t *testing.T) {
	for _, status := range scopedNotifyStatuses {
		t.Run(notifyStatusName(status), func(t *testing.T) {
			conn, _ := newTestConnWithContexts(t, StateAspActive, modeClient, 10, 20, 30)
			routing := params.NewRoutingContext(20, 10)

			if err := conn.handleNotify(messages.NewNotify(
				params.NewStatus(status), nil, routing, nil)); err != nil {
				t.Fatalf("handleNotify: %v", err)
			}
			indication := <-conn.ManagementIndications()
			if !indication.RoutingContextSet || indication.RoutingContext != 20 {
				t.Errorf("compatibility RoutingContext = %d (set=%v), want 20 (set)",
					indication.RoutingContext, indication.RoutingContextSet)
			}
			if !equalNotifyScope(indication.RoutingContexts, []uint32{20, 10}) {
				t.Errorf("explicit Routing Contexts = %v, want [20 10]", indication.RoutingContexts)
			}

			binary.BigEndian.PutUint32(routing.Data[0:4], 30)
			if !equalNotifyScope(indication.RoutingContexts, []uint32{20, 10}) {
				t.Errorf("indication scope aliased the message Param: %v", indication.RoutingContexts)
			}
		})
	}
}

func TestForeignNotifyScopeIsRejectedAtomicallyAndQuoted(t *testing.T) {
	conn, sent := newTestConnWithContexts(t, StateAspActive, modeClient, 1, 2)
	raw, err := messages.NewNotify(
		params.NewStatus(params.AlternateAspActive),
		params.NewAspIdentifier(7), params.NewRoutingContext(1, 99, 2, 100), nil,
	).MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}
	conn.dispatchRaw(context.Background(), inbound{data: raw, ppid: M3UAPPID})

	rejected := firstErr(conn)
	var routingError *RoutingContextError
	if !errors.As(rejected, &routingError) {
		t.Fatalf("dispatcher error = %v, want RoutingContextError", rejected)
	}
	if !equalNotifyScope(routingError.Contexts, []uint32{99, 100}) {
		t.Fatalf("offending Routing Contexts = %v, want [99 100]", routingError.Contexts)
	}
	select {
	case got := <-conn.stateChan:
		if got != stateUnchanged {
			t.Errorf("published state = %v, want stateUnchanged", got)
		}
	default:
		t.Fatal("rejected NTFY published no state result")
	}
	if got := conn.State(); got != StateAspActive {
		t.Errorf("state = %v after rejected override, want %v", got, StateAspActive)
	}
	for _, routingContext := range []uint32{1, 2} {
		if conn.routingContextOverridden(routingContext) {
			t.Errorf("Routing Context %d was overridden by a rejected NTFY", routingContext)
		}
	}
	if len(conn.mgmtChan) != 0 {
		t.Error("a rejected NTFY reached Layer Management")
	}

	if err := conn.handleErrors(rejected); err != nil {
		t.Fatalf("handleErrors: %v", err)
	}
	response := lastError(t, *sent)
	if response.ErrorCode == nil || response.ErrorCode.ErrorCode() != params.ErrInvalidRoutingContext {
		t.Fatalf("ERR code = %v, want Invalid Routing Context", response.ErrorCode)
	}
	if response.RoutingContext == nil ||
		!equalNotifyScope(response.RoutingContext.RoutingContexts(), []uint32{99, 100}) {
		t.Errorf("ERR Routing Contexts = %v, want [99 100]", response.RoutingContext)
	}
}

func TestEveryExplicitNotifyScopeIsInvalidWithoutConfiguredMembership(t *testing.T) {
	conn, sent := newTestConnWithContexts(t, StateAspActive, modeClient)
	conn.handleSignals(context.Background(), messages.NewNotify(
		params.NewStatus(params.AspFailure), params.NewAspIdentifier(7),
		params.NewRoutingContext(99, 100), nil))

	rejected := firstErr(conn)
	var routingError *RoutingContextError
	if !errors.As(rejected, &routingError) {
		t.Fatalf("dispatcher error = %v, want RoutingContextError", rejected)
	}
	if !equalNotifyScope(routingError.Contexts, []uint32{99, 100}) {
		t.Fatalf("offending Routing Contexts = %v, want [99 100]", routingError.Contexts)
	}
	if len(conn.mgmtChan) != 0 {
		t.Error("an unconfigured explicit NTFY reached Layer Management")
	}
	if err := conn.handleErrors(rejected); err != nil {
		t.Fatalf("handleErrors: %v", err)
	}
	response := lastError(t, *sent)
	if response.ErrorCode == nil || response.ErrorCode.ErrorCode() != params.ErrInvalidRoutingContext {
		t.Fatalf("ERR code = %v, want Invalid Routing Context", response.ErrorCode)
	}
	if response.RoutingContext == nil ||
		!equalNotifyScope(response.RoutingContext.RoutingContexts(), []uint32{99, 100}) {
		t.Errorf("ERR Routing Contexts = %v, want [99 100]", response.RoutingContext)
	}
}

func TestContextlessMultiASOverrideUsesConfiguredScopeFromTheWire(t *testing.T) {
	conn, _ := newTestConnWithContexts(t, StateAspActive, modeClient, 1, 2)
	raw, err := messages.NewNotify(
		params.NewStatus(params.AlternateAspActive), params.NewAspIdentifier(7), nil, nil,
	).MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}

	conn.dispatchRaw(context.Background(), inbound{data: raw, ppid: M3UAPPID})
	if rejected := firstErr(conn); rejected != nil {
		t.Fatalf("contextless configured NTFY error = %v", rejected)
	}
	select {
	case got := <-conn.stateChan:
		if got != StateAspInactive {
			t.Errorf("published state = %v, want %v", got, StateAspInactive)
		}
	default:
		t.Fatal("accepted override published no state result")
	}
	indication := <-conn.ManagementIndications()
	if indication.RoutingContextSet {
		t.Errorf("inferred scope was reported as explicit context %d", indication.RoutingContext)
	}
	if !equalNotifyScope(indication.RoutingContexts, []uint32{1, 2}) {
		t.Errorf("inferred override scope = %v, want [1 2]", indication.RoutingContexts)
	}
}

func TestContextlessNotifyIsAcceptedFromTheWire(t *testing.T) {
	conn, sent := newTestConnWithContexts(t, StateAspActive, modeClient)
	raw, err := messages.NewNotify(
		params.NewStatus(params.AspFailure), params.NewAspIdentifier(7), nil, nil,
	).MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary: %v", err)
	}

	conn.dispatchRaw(context.Background(), inbound{data: raw, ppid: M3UAPPID})
	if err := firstErr(conn); err != nil {
		t.Fatalf("contextless wire NTFY was rejected: %v", err)
	}
	if got := conn.State(); got != StateAspActive {
		t.Errorf("state = %v after contextless wire NTFY, want %v", got, StateAspActive)
	}
	select {
	case got := <-conn.stateChan:
		if got != stateUnchanged {
			t.Errorf("published state = %v, want stateUnchanged", got)
		}
	default:
		t.Fatal("accepted NTFY published no state result")
	}
	select {
	case indication := <-conn.ManagementIndications():
		if indication.RoutingContextSet || len(indication.RoutingContexts) != 0 {
			t.Fatalf("contextless NTFY scope = %v (set=%v), want omitted",
				indication.RoutingContexts, indication.RoutingContextSet)
		}
	default:
		t.Fatal("accepted wire NTFY did not reach Layer Management")
	}
	for _, message := range *sent {
		if _, ok := message.(*messages.Error); ok {
			t.Fatalf("accepted contextless NTFY sent Error: %v", message)
		}
	}
}

// A malformed Routing Context never reaches handleNotify: the parameter codec
// rejects it first. The receive path must still emit Parameter Field Error with
// Diagnostic Information identifying the offending NTFY, and must leave all AS
// state untouched.
func TestMalformedNotifyRoutingContextIsRejectedFromTheWire(t *testing.T) {
	for _, size := range []int{0, 1, 2, 3, 5, 6, 7, 9} {
		t.Run(string(rune('0'+size))+" value octets", func(t *testing.T) {
			conn, sent := newTestConnWithContexts(t, StateAspActive, modeClient, 1, 2)
			raw := rawNotifyWithRoutingContextValue(t, bytes.Repeat([]byte{0xa5}, size))

			conn.dispatchRaw(context.Background(), inbound{data: raw, ppid: M3UAPPID})
			rejected := firstErr(conn)
			var fault *ParameterFaultError
			if !errors.As(rejected, &fault) {
				t.Fatalf("wire NTFY error = %v, want ParameterFaultError", rejected)
			}
			if fault.Code != params.ErrParameterFieldError {
				t.Errorf("fault code = %#x, want Parameter Field Error", fault.Code)
			}
			if got := conn.State(); got != StateAspActive {
				t.Errorf("state = %v after malformed NTFY, want %v", got, StateAspActive)
			}
			if len(conn.stateChan) != 0 {
				t.Error("a NTFY rejected by the decoder published a handler state")
			}
			if len(conn.mgmtChan) != 0 {
				t.Error("a malformed wire NTFY reached Layer Management")
			}

			if err := conn.handleErrors(rejected); err != nil {
				t.Fatalf("handleErrors: %v", err)
			}
			response := lastError(t, *sent)
			if response.ErrorCode == nil || response.ErrorCode.ErrorCode() != params.ErrParameterFieldError {
				t.Fatalf("ERR code = %v, want Parameter Field Error", response.ErrorCode)
			}
			if response.DiagnosticInformation == nil {
				t.Fatal("Parameter Field Error omitted Diagnostic Information")
			}
			if got := response.DiagnosticInformation.DiagnosticInformation(); !bytes.Equal(got, raw) {
				t.Errorf("Diagnostic Information = %x, want %x", got, raw)
			}
		})
	}
}

func FuzzNotifyRoutingContextScope(f *testing.F) {
	f.Add(uint8(0), uint8(0), []byte(nil))
	f.Add(uint8(4), uint8(1), []byte{0, 0, 0, 1})
	f.Add(uint8(5), uint8(1), []byte{0, 0, 0, 99})
	f.Add(uint8(2), uint8(1), []byte{0, 0, 0})
	f.Add(uint8(3), uint8(2), []byte{0, 0, 0, 1})
	f.Add(uint8(1), uint8(0x80), []byte(nil))
	f.Add(uint8(1), uint8(0x81), []byte{0, 0, 0, 99})

	f.Fuzz(func(t *testing.T, statusIndex, routingMode uint8, data []byte) {
		if len(data) > 64 {
			t.Skip()
		}
		configured := []uint32{1, 2}
		if routingMode&0x80 != 0 {
			configured = nil
		}
		conn, _ := newTestConnWithContexts(t, StateAspActive, modeClient, configured...)
		status := scopedNotifyStatuses[int(statusIndex)%len(scopedNotifyStatuses)]

		var routing *params.Param
		switch (routingMode & 0x03) % 3 {
		case 0:
			routing = nil
		case 1:
			routing = params.NewParam(int(params.RoutingContext), data)
		case 2:
			routing = params.NewParam(int(params.InfoString), data)
		}

		err := conn.handleNotify(messages.NewNotify(params.NewStatus(status), nil, routing, nil))
		var wantOffending []uint32
		switch {
		case routing == nil:
			if err != nil {
				t.Fatalf("inferred configured scope error = %v", err)
			}
		case routing.Tag != params.RoutingContext || len(data) == 0 || len(data)%4 != 0:
			if !errors.Is(err, ErrInvalidRoutingContext) {
				t.Fatalf("malformed Routing Context error = %v, want ErrInvalidRoutingContext", err)
			}
		default:
			for offset := 0; offset < len(data); offset += 4 {
				routingContext := binary.BigEndian.Uint32(data[offset : offset+4])
				if len(configured) == 0 || routingContext != 1 && routingContext != 2 {
					wantOffending = append(wantOffending, routingContext)
				}
			}
			if len(wantOffending) == 0 {
				if err != nil {
					t.Fatalf("configured Routing Context error = %v", err)
				}
			} else {
				var routingError *RoutingContextError
				if !errors.As(err, &routingError) {
					t.Fatalf("unknown Routing Context error = %v, want RoutingContextError", err)
				}
				if !equalNotifyScope(routingError.Contexts, wantOffending) {
					t.Fatalf("offending Routing Contexts = %v, want %v", routingError.Contexts, wantOffending)
				}
			}
		}

		if got := conn.State(); got != StateAspActive {
			t.Errorf("state = %v after handleNotify, want %v", got, StateAspActive)
		}
		if err != nil && len(conn.mgmtChan) != 0 {
			t.Error("a rejected NTFY reached Layer Management")
		}
		if err == nil && len(conn.mgmtChan) != 1 {
			t.Errorf("accepted NTFY indications = %d, want 1", len(conn.mgmtChan))
		}
		if err == nil {
			indication := <-conn.mgmtChan
			wantScope := routing.RoutingContexts()
			if routing == nil {
				wantScope = configured
			}
			if !equalNotifyScope(indication.RoutingContexts, wantScope) {
				t.Errorf("indication Routing Contexts = %v, want %v", indication.RoutingContexts, wantScope)
			}
		}
	})
}

func rawNotifyWithRoutingContextValue(t *testing.T, value []byte) []byte {
	t.Helper()
	status, err := params.NewStatus(params.AsStatePending).MarshalBinary()
	if err != nil {
		t.Fatalf("marshal Status: %v", err)
	}

	parameterLength := 4 + len(value)
	routing := make([]byte, parameterLength)
	binary.BigEndian.PutUint16(routing[0:2], params.RoutingContext)
	binary.BigEndian.PutUint16(routing[2:4], uint16(parameterLength))
	copy(routing[4:], value)
	for len(routing)%4 != 0 {
		routing = append(routing, 0)
	}

	raw := make([]byte, 8, 8+len(status)+len(routing))
	raw[0] = 1
	raw[2] = messages.MsgClassManagement
	raw[3] = messages.MsgTypeNotify
	raw = append(raw, status...)
	raw = append(raw, routing...)
	binary.BigEndian.PutUint32(raw[4:8], uint32(len(raw)))
	return raw
}

func equalNotifyScope(left, right []uint32) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
