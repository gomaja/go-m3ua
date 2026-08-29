// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func applyASPTMState(t *testing.T, conn *Association) State {
	t.Helper()

	select {
	case state := <-conn.stateChan:
		if err := conn.handleStateUpdate(state); err != nil {
			t.Fatalf("handleStateUpdate(%v): %v", state, err)
		}
		return state
	default:
		t.Fatal("ASPTM handler published no state")
		return stateUnchanged
	}
}

func aspActiveAckContexts(t *testing.T, sent []messages.M3UA) []uint32 {
	t.Helper()
	for _, signal := range sent {
		if ack, ok := signal.(*messages.AspActiveAck); ok {
			if ack.RoutingContext == nil {
				return nil
			}
			return ack.RoutingContext.RoutingContexts()
		}
	}
	t.Fatal("no ASP Active Ack sent")
	return nil
}

func aspInactiveAckContexts(t *testing.T, sent []messages.M3UA) []uint32 {
	t.Helper()
	for _, signal := range sent {
		if ack, ok := signal.(*messages.AspInactiveAck); ok {
			if ack.RoutingContext == nil {
				return nil
			}
			return ack.RoutingContext.RoutingContexts()
		}
	}
	t.Fatal("no ASP Inactive Ack sent")
	return nil
}

func TestDispatcherKeepsUnaffectedApplicationServerActive(t *testing.T) {
	registry := newApplicationServers(time.Hour)
	asp, _ := asTestConn(t, registry, StateASPInactive, 1, 2)

	asp.handleSignals(context.Background(), messages.NewAspActive(
		params.NewTrafficModeType(params.TrafficModeLoadshare), nil, nil))
	if state := applyASPTMState(t, asp); state != StateASPActive {
		t.Fatalf("state after unscoped ASP Active = %v, want %v", state, StateASPActive)
	}

	asp.handleSignals(context.Background(), messages.NewAspInactive(
		params.NewRoutingContext(1), nil))
	if state := applyASPTMState(t, asp); state != StateASPActive {
		t.Fatalf("state after RC-scoped ASP Inactive = %v, want %v", state, StateASPActive)
	}

	if got := asp.State(); got != StateASPActive {
		t.Errorf("association state = %v, want %v while RC 2 remains active", got, StateASPActive)
	}
	if got := registry.get(1).activeASPs(); len(got) != 0 {
		t.Errorf("RC 1 has %d active ASPs after scoped deactivation, want 0", len(got))
	}
	if got := registry.get(2).activeASPs(); len(got) != 1 || got[0] != asp {
		t.Errorf("RC 2 active ASPs = %v, want only the unaffected ASP", got)
	}
}

func TestDispatcherMovesInactiveAfterTheLastApplicationServer(t *testing.T) {
	registry := newApplicationServers(time.Hour)
	asp, _ := asTestConn(t, registry, StateASPInactive, 1, 2)

	asp.handleSignals(context.Background(), messages.NewAspActive(
		params.NewTrafficModeType(params.TrafficModeLoadshare), nil, nil))
	applyASPTMState(t, asp)

	for index, rtCtx := range []uint32{1, 2} {
		asp.handleSignals(context.Background(), messages.NewAspInactive(
			params.NewRoutingContext(rtCtx), nil))
		want := StateASPActive
		if index == 1 {
			want = StateASPInactive
		}
		if state := applyASPTMState(t, asp); state != want {
			t.Fatalf("state after deactivating RC %d = %v, want %v", rtCtx, state, want)
		}
	}

	if got := asp.State(); got != StateASPInactive {
		t.Errorf("association state = %v, want %v after the last RC", got, StateASPInactive)
	}
	for _, rtCtx := range []uint32{1, 2} {
		if got := registry.get(rtCtx).activeASPs(); len(got) != 0 {
			t.Errorf("RC %d has %d active ASPs after complete deactivation, want 0",
				rtCtx, len(got))
		}
	}
}

func TestScopedDuplicateASPInactiveDoesNotActivateAnotherContext(t *testing.T) {
	registry := newApplicationServers(time.Hour)
	asp, _ := asTestConn(t, registry, StateASPInactive, 1, 2)

	asp.handleSignals(context.Background(), messages.NewAspInactive(
		params.NewRoutingContext(1), nil))
	if state := applyASPTMState(t, asp); state != StateASPInactive {
		t.Fatalf("state after duplicate scoped ASP Inactive = %v, want %v",
			state, StateASPInactive)
	}
	if got := asp.State(); got != StateASPInactive {
		t.Errorf("association state = %v, want %v", got, StateASPInactive)
	}
	for _, rtCtx := range []uint32{1, 2} {
		if got := registry.get(rtCtx).activeASPs(); len(got) != 0 {
			t.Errorf("RC %d has %d active ASPs after duplicate ASP Inactive, want 0",
				rtCtx, len(got))
		}
	}
}

func TestASPInactiveAckDeactivatesOnlyNamedContexts(t *testing.T) {
	asp, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
	asp.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))

	asp.handleSignals(context.Background(), messages.NewAspInactiveAck(
		params.NewRoutingContext(1), nil))
	if state := applyASPTMState(t, asp); state != StateASPActive {
		t.Fatalf("state after partial ASP Inactive Ack = %v, want %v", state, StateASPActive)
	}

	if _, err := asp.routingContextFor(1); !errors.Is(err, ErrRoutingContextNotActive) {
		t.Errorf("routingContextFor(1) error = %v, want %v", err, ErrRoutingContextNotActive)
	}
	if _, err := asp.routingContextFor(2); err != nil {
		t.Errorf("routingContextFor(2) error = %v, want nil", err)
	}
}

func TestASPInactiveAckCanDeactivateTheLastContext(t *testing.T) {
	asp, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
	asp.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))

	// Call the handler directly first. The DATA gate must close before the
	// dispatcher publishes ASP-INACTIVE, otherwise a concurrent writer can send
	// traffic in the gap between accepting the Ack and applying the state.
	if err := asp.handleAspInactiveAck(messages.NewAspInactiveAck(nil, nil)); err != nil {
		t.Fatalf("handleAspInactiveAck: %v", err)
	}
	for _, rtCtx := range []uint32{1, 2} {
		if _, err := asp.routingContextFor(rtCtx); !errors.Is(err, ErrRoutingContextNotActive) {
			t.Errorf("routingContextFor(%d) error = %v, want %v before state apply",
				rtCtx, err, ErrRoutingContextNotActive)
		}
	}

	if err := asp.handleAspActiveAck(messages.NewAspActiveAck(
		params.NewTrafficModeType(params.TrafficModeLoadshare),
		params.NewRoutingContext(2), nil)); err != nil {
		t.Fatalf("handleAspActiveAck(RC 2): %v", err)
	}
	if _, err := asp.routingContextFor(1); !errors.Is(err, ErrRoutingContextNotActive) {
		t.Errorf("routingContextFor(1) error after reactivation = %v, want %v",
			err, ErrRoutingContextNotActive)
	}
	if _, err := asp.routingContextFor(2); err != nil {
		t.Errorf("routingContextFor(2) error after reactivation = %v, want nil", err)
	}
}

func TestASPInactiveAckScopesAnUnrecordedActiveFallback(t *testing.T) {
	asp, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)

	// Some callers and legacy state restoration place an Association directly into
	// ASP-ACTIVE, where no Ack scope has been recorded and both configured
	// contexts are the compatibility fallback. A scoped deactivation must
	// materialise that set before subtracting from it.
	if err := asp.handleAspInactiveAck(messages.NewAspInactiveAck(
		params.NewRoutingContext(1), nil)); err != nil {
		t.Fatalf("handleAspInactiveAck: %v", err)
	}
	if _, err := asp.routingContextFor(1); !errors.Is(err, ErrRoutingContextNotActive) {
		t.Errorf("routingContextFor(1) error = %v, want %v", err, ErrRoutingContextNotActive)
	}
	if _, err := asp.routingContextFor(2); err != nil {
		t.Errorf("routingContextFor(2) error = %v, want nil", err)
	}
}

func TestMixedASPActiveAppliesServedSubset(t *testing.T) {
	registry := newApplicationServers(time.Hour)
	asp, sent := asTestConn(t, registry, StateASPInactive, 1, 2)

	asp.handleSignals(context.Background(), messages.NewAspActive(
		params.NewTrafficModeType(params.TrafficModeLoadshare),
		params.NewRoutingContext(1, 999), nil))

	if got, want := aspActiveAckContexts(t, *sent), []uint32{1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ASP Active Ack contexts = %v, want %v", got, want)
	}
	if err := firstErr(asp); !errors.Is(err, ErrNoConfiguredAS) {
		t.Fatalf("error = %v, want %v for unserved RC 999", err, ErrNoConfiguredAS)
	}
	if state := applyASPTMState(t, asp); state != StateASPActive {
		t.Fatalf("state after partially successful ASP Active = %v, want %v", state, StateASPActive)
	}
	if got := registry.get(1).activeASPs(); len(got) != 1 || got[0] != asp {
		t.Errorf("served RC 1 active ASPs = %v, want the requesting ASP", got)
	}
	if got := registry.get(2).activeASPs(); len(got) != 0 {
		t.Errorf("unrequested RC 2 has %d active ASPs, want 0", len(got))
	}
}

func TestMixedASPInactiveAppliesServedSubset(t *testing.T) {
	registry := newApplicationServers(time.Hour)
	asp, sent := asTestConn(t, registry, StateASPInactive, 1, 2)

	asp.handleSignals(context.Background(), messages.NewAspActive(
		params.NewTrafficModeType(params.TrafficModeLoadshare), nil, nil))
	applyASPTMState(t, asp)

	before := len(*sent)
	asp.handleSignals(context.Background(), messages.NewAspInactive(
		params.NewRoutingContext(1, 999), nil))

	if got, want := aspInactiveAckContexts(t, (*sent)[before:]), []uint32{1}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ASP Inactive Ack contexts = %v, want %v", got, want)
	}
	if err := firstErr(asp); !errors.Is(err, ErrInvalidRoutingContext) {
		t.Fatalf("error = %v, want %v for unserved RC 999", err, ErrInvalidRoutingContext)
	}
	if state := applyASPTMState(t, asp); state != StateASPActive {
		t.Fatalf("state after partially successful ASP Inactive = %v, want %v", state, StateASPActive)
	}
	if got := registry.get(1).activeASPs(); len(got) != 0 {
		t.Errorf("served RC 1 has %d active ASPs after deactivation, want 0", len(got))
	}
	if got := registry.get(2).activeASPs(); len(got) != 1 || got[0] != asp {
		t.Errorf("unaffected RC 2 active ASPs = %v, want the requesting ASP", got)
	}
}

func TestIdempotentASPTMRequestsAreAcknowledgedWithoutError(t *testing.T) {
	for _, test := range []struct {
		name    string
		state   State
		message messages.M3UA
		handle  func(*Association) error
		ackName string
	}{
		{
			name:    "ASP Active while active",
			state:   StateASPActive,
			message: messages.NewAspActive(params.NewTrafficModeType(params.TrafficModeLoadshare), params.NewRoutingContext(1), nil),
			handle: func(connection *Association) error {
				return connection.handleAspActive(messages.NewAspActive(
					params.NewTrafficModeType(params.TrafficModeLoadshare), params.NewRoutingContext(1), nil,
				))
			},
			ackName: "ASP Active Ack",
		},
		{
			name:    "ASP Inactive while inactive",
			state:   StateASPInactive,
			message: messages.NewAspInactive(params.NewRoutingContext(1), nil),
			handle: func(connection *Association) error {
				return connection.handleAspInactive(messages.NewAspInactive(params.NewRoutingContext(1), nil))
			},
			ackName: "ASP Inactive Ack",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			connection, sent := newTestConnWithContexts(t, test.state, RoleSGP, 1)
			if err := test.handle(connection); err != nil {
				t.Fatalf("idempotent %s returned %v after its mandatory Ack", test.message.MessageTypeName(), err)
			}
			if got := typeNames(*sent); len(got) != 1 || got[0] != test.ackName {
				t.Fatalf("sent messages = %v, want only %s", got, test.ackName)
			}
		})
	}
}

func TestMixedASPInactiveCanDeactivateTheLastServedContext(t *testing.T) {
	registry := newApplicationServers(time.Hour)
	asp, _ := asTestConn(t, registry, StateASPInactive, 1)

	asp.handleSignals(context.Background(), messages.NewAspActive(
		params.NewTrafficModeType(params.TrafficModeLoadshare), nil, nil))
	applyASPTMState(t, asp)

	asp.handleSignals(context.Background(), messages.NewAspInactive(
		params.NewRoutingContext(1, 999), nil))
	if err := firstErr(asp); !errors.Is(err, ErrInvalidRoutingContext) {
		t.Fatalf("error = %v, want %v for unserved RC 999", err, ErrInvalidRoutingContext)
	}
	if state := applyASPTMState(t, asp); state != StateASPInactive {
		t.Fatalf("state after deactivating the last served RC = %v, want %v",
			state, StateASPInactive)
	}
	if got := registry.get(1).activeASPs(); len(got) != 0 {
		t.Errorf("RC 1 has %d active ASPs, want 0", len(got))
	}
}

func TestOverrideIsScopedToSuccessfullyActivatedContexts(t *testing.T) {
	registry := newApplicationServers(time.Hour)
	incumbent, incumbentSent := asTestConn(t, registry, StateASPInactive, 1, 2)
	incumbent.cfg.TrafficModeType = params.NewTrafficModeType(params.TrafficModeOverride)
	incumbent.noteRoutingContextsActive(nil)
	incumbent.setState(StateASPActive)
	registry.aspStateChanged(incumbent, StateASPActive)

	challenger, _ := asTestConn(t, registry, StateASPInactive, 1, 2)
	challenger.cfg.TrafficModeType = params.NewTrafficModeType(params.TrafficModeOverride)

	before := len(notifies(*incumbentSent))
	challenger.handleSignals(context.Background(), messages.NewAspActive(
		params.NewTrafficModeType(params.TrafficModeOverride),
		params.NewRoutingContext(1), nil))
	applyASPTMState(t, challenger)

	if got := incumbent.State(); got != StateASPActive {
		t.Errorf("incumbent association state = %v, want %v while RC 2 remains active", got, StateASPActive)
	}
	if got := registry.get(1).activeASPs(); len(got) != 1 || got[0] != challenger {
		t.Errorf("RC 1 active ASPs = %v, want only challenger", got)
	}
	if got := registry.get(2).activeASPs(); len(got) != 1 || got[0] != incumbent {
		t.Errorf("RC 2 active ASPs = %v, want only incumbent", got)
	}

	var alternateContexts [][]uint32
	for _, notify := range notifies(*incumbentSent)[before:] {
		statusType, information := statusOf(t, notify)
		if statusType == params.AsStateChange &&
			information == uint16(params.AsStatePending&0xffff) {
			t.Error("Override emitted a transient AS-PENDING Notify although the " +
				"challenger had already been acknowledged active")
		}
		if information != uint16(params.AlternateAspActive&0xffff) {
			continue
		}
		alternateContexts = append(alternateContexts, notify.RoutingContext.RoutingContexts())
	}
	if got, want := alternateContexts, [][]uint32{{1}}; !reflect.DeepEqual(got, want) {
		t.Errorf("Alternate ASP Active Notify contexts = %v, want %v", got, want)
	}
}

func TestOverrideMovesIncumbentInactiveAfterItsLastContext(t *testing.T) {
	registry := newApplicationServers(time.Hour)
	incumbent, _ := asTestConn(t, registry, StateASPActive, 1)
	incumbent.cfg.TrafficModeType = params.NewTrafficModeType(params.TrafficModeOverride)
	incumbent.noteRoutingContextsActive(nil)
	registry.aspStateChanged(incumbent, StateASPActive)

	challenger, _ := asTestConn(t, registry, StateASPInactive, 1)
	challenger.cfg.TrafficModeType = params.NewTrafficModeType(params.TrafficModeOverride)
	challenger.handleSignals(context.Background(), messages.NewAspActive(
		params.NewTrafficModeType(params.TrafficModeOverride),
		params.NewRoutingContext(1), nil))
	applyASPTMState(t, challenger)

	if got := incumbent.State(); got != StateASPInactive {
		t.Errorf("incumbent state = %v, want %v after its last RC was overridden",
			got, StateASPInactive)
	}
	if state := applyASPTMState(t, incumbent); state != StateASPInactive {
		t.Errorf("incumbent published state = %v, want %v", state, StateASPInactive)
	}
	if got := registry.get(1).activeASPs(); len(got) != 1 || got[0] != challenger {
		t.Errorf("RC 1 active ASPs = %v, want only challenger", got)
	}
}

func TestConcurrentOverrideActivationsLeaveExactlyOneActiveASP(t *testing.T) {
	registry := newApplicationServers(time.Hour)
	first, _ := asTestConn(t, registry, StateASPInactive, 1, 2)
	second, _ := asTestConn(t, registry, StateASPInactive, 1, 2)
	for _, connection := range []*Association{first, second} {
		connection.cfg.TrafficModeType = params.NewTrafficModeType(params.TrafficModeOverride)
		connection.signalWriter = func(message messages.M3UA) (int, error) {
			return message.MarshalLen(), nil
		}
	}
	applicationServer := registry.get(1)
	applicationServer.setTrafficMode(params.TrafficModeOverride)
	active := messages.NewAspActive(
		params.NewTrafficModeType(params.TrafficModeOverride),
		params.NewRoutingContext(1), nil,
	)

	for attempt := 0; attempt < 2_000; attempt++ {
		first.noteRoutingContextsActive([]uint32{1, 2})
		second.noteRoutingContextsActive([]uint32{1, 2})
		applicationServer.mu.Lock()
		applicationServer.asps[first] = StateASPInactive
		applicationServer.asps[second] = StateASPInactive
		applicationServer.state = ASInactive
		applicationServer.mu.Unlock()

		start := make(chan struct{})
		var ready sync.WaitGroup
		ready.Add(2)
		var finished sync.WaitGroup
		finished.Add(2)
		for _, challenger := range []*Association{first, second} {
			go func(challenger *Association) {
				defer finished.Done()
				ready.Done()
				<-start
				challenger.overrideOtherASPs(active, []uint32{1})
			}(challenger)
		}
		ready.Wait()
		close(start)
		finished.Wait()

		if got := len(applicationServer.activeASPs()); got != 1 {
			t.Fatalf("attempt %d left %d active ASPs after simultaneous Override activations, want 1", attempt, got)
		}
	}
}

func TestDynamicTrafficModeIsEchoedAndCannotChangeAfterAgreement(t *testing.T) {
	registry := newApplicationServers(time.Hour)
	first, firstSent := asTestConn(t, registry, StateASPInactive, 1)
	first.cfg.TrafficModeType = nil
	if err := first.handleAspActive(messages.NewAspActive(
		params.NewTrafficModeType(params.TrafficModeBroadcast),
		params.NewRoutingContext(1), nil,
	)); err != nil {
		t.Fatal(err)
	}
	ack := lastAspActiveAck(t, *firstSent)
	if ack.TrafficModeType == nil || ack.TrafficModeType.TrafficModeType() != params.TrafficModeBroadcast {
		t.Fatalf("dynamic ASP Active Ack Traffic Mode = %v, want Broadcast echoed", ack.TrafficModeType)
	}
	if got := registry.get(1).TrafficMode(); got != params.TrafficModeBroadcast {
		t.Fatalf("agreed AS Traffic Mode = %d, want Broadcast", got)
	}

	second, secondSent := asTestConn(t, registry, StateASPInactive, 1)
	second.cfg.TrafficModeType = nil
	before := len(*secondSent)
	err := second.handleAspActive(messages.NewAspActive(
		params.NewTrafficModeType(params.TrafficModeOverride),
		params.NewRoutingContext(1), nil,
	))
	if !errors.Is(err, ErrUnsupportedTrafficMode) {
		t.Fatalf("conflicting second mode error = %v, want ErrUnsupportedTrafficMode", err)
	}
	for _, signal := range (*secondSent)[before:] {
		if _, ok := signal.(*messages.AspActiveAck); ok {
			t.Fatal("conflicting Traffic Mode was acknowledged before being rejected")
		}
	}
	if got := registry.get(1).TrafficMode(); got != params.TrafficModeBroadcast {
		t.Fatalf("conflicting request changed AS Traffic Mode to %d", got)
	}
}

func TestConcurrentFirstTrafficModeAgreementHasOneWinner(t *testing.T) {
	registry := newApplicationServers(time.Hour)
	first, firstSent := asTestConn(t, registry, StateASPInactive, 1)
	second, secondSent := asTestConn(t, registry, StateASPInactive, 1)
	first.cfg.TrafficModeType = nil
	second.cfg.TrafficModeType = nil

	start := make(chan struct{})
	results := make(chan error, 2)
	for _, request := range []struct {
		connection *Association
		mode       uint32
	}{
		{connection: first, mode: params.TrafficModeBroadcast},
		{connection: second, mode: params.TrafficModeOverride},
	} {
		go func(request struct {
			connection *Association
			mode       uint32
		}) {
			<-start
			results <- request.connection.handleAspActive(messages.NewAspActive(
				params.NewTrafficModeType(request.mode), params.NewRoutingContext(1), nil,
			))
		}(request)
	}
	close(start)
	accepted, rejected := 0, 0
	for range 2 {
		err := <-results
		switch {
		case err == nil:
			accepted++
		case errors.Is(err, ErrUnsupportedTrafficMode):
			rejected++
		default:
			t.Fatalf("concurrent Traffic Mode result = %v", err)
		}
	}
	if accepted != 1 || rejected != 1 {
		t.Fatalf("concurrent Traffic Mode outcomes = %d accepted/%d rejected, want 1/1", accepted, rejected)
	}
	acks := 0
	for _, sent := range []*[]messages.M3UA{firstSent, secondSent} {
		for _, signal := range *sent {
			if _, ok := signal.(*messages.AspActiveAck); ok {
				acks++
			}
		}
	}
	if acks != 1 {
		t.Fatalf("concurrent conflicting modes produced %d Acks, want exactly 1", acks)
	}
}

func TestTrafficModeCanBeConfiguredPerApplicationServer(t *testing.T) {
	registry := newApplicationServers(time.Hour)
	connection, sent := asTestConn(t, registry, StateASPInactive, 1, 2)
	connection.cfg.TrafficModeType = nil
	connection.cfg.TrafficModes = map[uint32]uint32{
		1: params.TrafficModeOverride,
		2: params.TrafficModeBroadcast,
	}

	for _, request := range []struct {
		routingContext uint32
		mode           uint32
	}{
		{routingContext: 1, mode: params.TrafficModeOverride},
		{routingContext: 2, mode: params.TrafficModeBroadcast},
	} {
		before := len(*sent)
		if err := connection.handleAspActive(messages.NewAspActive(
			params.NewTrafficModeType(request.mode),
			params.NewRoutingContext(request.routingContext), nil,
		)); err != nil {
			t.Fatalf("RC %d: %v", request.routingContext, err)
		}
		ack := lastAspActiveAck(t, (*sent)[before:])
		if ack.TrafficModeType == nil || ack.TrafficModeType.TrafficModeType() != request.mode {
			t.Fatalf("RC %d Ack mode = %v, want %d", request.routingContext, ack.TrafficModeType, request.mode)
		}
		if got := registry.get(request.routingContext).TrafficMode(); got != request.mode {
			t.Fatalf("RC %d AS mode = %d, want %d", request.routingContext, got, request.mode)
		}
	}
}

func TestExplicitEmptyASPTMRoutingContextIsRejected(t *testing.T) {
	tests := []struct {
		name  string
		state State
		new   func(*params.Param) messages.M3UA
	}{
		{
			name:  "ASP Active",
			state: StateASPInactive,
			new: func(rc *params.Param) messages.M3UA {
				return messages.NewAspActive(
					params.NewTrafficModeType(params.TrafficModeLoadshare), rc, nil)
			},
		},
		{
			name:  "ASP Inactive",
			state: StateASPActive,
			new: func(rc *params.Param) messages.M3UA {
				return messages.NewAspInactive(rc, nil)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conn, sent := newTestConnWithContexts(t, test.state, RoleSGP, 1, 2)
			conn.handleSignals(context.Background(), test.new(params.NewRoutingContext()))

			if len(*sent) != 0 {
				t.Fatalf("sent %v for explicit empty RC, want no acknowledgement", typeNames(*sent))
			}
			if err := firstErr(conn); !errors.Is(err, ErrInvalidRoutingContext) {
				t.Fatalf("error = %v, want %v", err, ErrInvalidRoutingContext)
			}
			if state := applyASPTMState(t, conn); state != stateUnchanged && state != test.state {
				t.Errorf("published state = %v, want unchanged or a restatement of %v",
					state, test.state)
			}
			if got := conn.State(); got != test.state {
				t.Errorf("association state = %v, want %v", got, test.state)
			}
		})
	}
}

func TestSetASUnavailableLeavesOtherContextsActive(t *testing.T) {
	config := newSGPAssociationConfigForTest(&HeartbeatInfo{Enabled: false},
		0x22222222, 0x11111111, 1, params.TrafficModeLoadshare, 0, 0,
		[]uint32{1, 2}, params.ServiceIndSCCP, 0, 0, 1)
	listener := newSGPListener(NewListenerConfig(config))
	registry, nif, _ := listener.registry()

	asp, _ := newTestConnWithContexts(t, StateASPActive, RoleSGP, 1, 2)
	asp.as = registry
	asp.nif = nif
	asp.noteRoutingContextsActive(nil)
	registry.aspStateChanged(asp, StateASPActive)
	if !listener.track(asp) {
		t.Fatal("listener refused test ASP")
	}

	if err := listener.SetASAvailable(1, false); err != nil {
		t.Fatalf("SetASAvailable: %v", err)
	}

	if got := asp.State(); got != StateASPActive {
		t.Errorf("association state = %v, want %v while RC 2 remains active", got, StateASPActive)
	}
	if got := registry.get(1).activeASPs(); len(got) != 0 {
		t.Errorf("unavailable RC 1 has %d active ASPs, want 0", len(got))
	}
	if got := registry.get(2).activeASPs(); len(got) != 1 || got[0] != asp {
		t.Errorf("available RC 2 active ASPs = %v, want the ASP", got)
	}
}
