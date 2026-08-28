// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestSolicitedAspDownAckIsAccepted(t *testing.T) {
	conn, _ := newTestConn(t, StateASPInactive, RoleASP)
	conn.startTAck(messages.NewAspDown(nil), requestAspDown)

	if err := conn.handleAspDownAck(messages.NewAspDownAck(nil)); err != nil {
		t.Fatalf("solicited ASP Down Ack returned %v", err)
	}
	if conn.pendingTAck() != 0 {
		t.Error("solicited ASP Down Ack left T(ack) armed")
	}
}

func TestShutdownWaitsForEachAckBeforeAdvancing(t *testing.T) {
	conn, _ := newTestConn(t, StateASPActive, RoleASP)
	conn.cfg.TAck = time.Second
	conn.cfg.TAckRetries = 2

	writes := make(chan messages.M3UA, 8)
	conn.signalWriter = func(message messages.M3UA) (int, error) {
		writes <- message
		return message.MarshalLen(), nil
	}

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- conn.Shutdown() }()

	first := receiveSignal(t, writes)
	if _, ok := first.(*messages.AspInactive); !ok {
		t.Fatalf("first shutdown signal = %T, want *messages.AspInactive", first)
	}
	if conn.pendingTAck() != 1 {
		t.Fatalf("ASP Inactive reached the writer with %d T(ack) requests, want 1", conn.pendingTAck())
	}
	assertNoSignal(t, writes, 25*time.Millisecond, "ASP Down before ASP Inactive Ack")

	conn.handleSignals(context.Background(), messages.NewAspInactiveAck(conn.cfg.RoutingContexts.Copy(), nil))
	second := receiveSignal(t, writes)
	if _, ok := second.(*messages.AspDown); !ok {
		t.Fatalf("second shutdown signal = %T, want *messages.AspDown", second)
	}
	if conn.pendingTAck() != 1 {
		t.Fatalf("ASP Down reached the writer with %d T(ack) requests, want 1", conn.pendingTAck())
	}
	select {
	case err := <-shutdownDone:
		t.Fatalf("Shutdown returned before ASP Down Ack: %v", err)
	default:
	}

	conn.handleSignals(context.Background(), messages.NewAspDownAck(nil))
	select {
	case <-shutdownDone:
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not finish after ASP Down Ack")
	}
}

// Orderly termination is a new control procedure, not another request that can
// coexist with whatever establishment or traffic-maintenance timers happened
// to be outstanding. Every old timer must be cancelled before the first
// shutdown message reaches the wire.
func TestShutdownCancelsOldTAckBeforeEachOrderlyRequest(t *testing.T) {
	conn, _ := newTestConn(t, StateASPActive, RoleASP)
	conn.cfg.TAck = time.Hour
	conn.startTAck(messages.NewAspUp(nil, params.NewInfoString("old")), requestAspUp)
	conn.startTAck(messages.NewAspActive(
		conn.cfg.TrafficModeType.Copy(), params.NewRoutingContext(1), nil,
	), requestAspActive)
	conn.startTAck(messages.NewAspInactive(params.NewRoutingContext(2), nil), requestAspInactive)
	conn.startTAck(messages.NewAspDown(params.NewInfoString("old")), requestAspDown)

	writes := make(chan messages.M3UA, 8)
	conn.signalWriter = func(message messages.M3UA) (int, error) {
		writes <- message
		return message.MarshalLen(), nil
	}
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- conn.Shutdown() }()

	if _, ok := receiveSignal(t, writes).(*messages.AspInactive); !ok {
		t.Fatal("shutdown did not begin with ASP Inactive")
	}
	if got := conn.pendingTAck(); got != 1 {
		t.Fatalf("first shutdown write retained %d pending requests, want only ASP Inactive", got)
	}
	conn.handleSignals(context.Background(), messages.NewAspInactiveAck(
		conn.cfg.RoutingContexts.Copy(), nil,
	))

	if _, ok := receiveSignal(t, writes).(*messages.AspDown); !ok {
		t.Fatal("shutdown did not continue with ASP Down")
	}
	if got := conn.pendingTAck(); got != 1 {
		t.Fatalf("second shutdown write retained %d pending requests, want only ASP Down", got)
	}
	conn.handleSignals(context.Background(), messages.NewAspDownAck(nil))
	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not finish")
	}
}

// Cancellation alone is insufficient when a timer has already entered its
// write. The shutdown ASP Inactive must wait behind that write so the old ASP
// Active cannot complete later and appear after the withdrawal on stream 0.
func TestShutdownFencesAnInFlightOldTAckBeforeAspInactive(t *testing.T) {
	conn, _ := newTestConn(t, StateASPActive, RoleASP)
	conn.cfg.TAck = 10 * time.Millisecond
	conn.cfg.TAckRetries = 100

	oldWriteStarted := make(chan struct{})
	releaseOldWrite := make(chan struct{})
	writes := make(chan messages.M3UA, 8)
	var oldWriteOnce sync.Once
	conn.signalWriter = func(message messages.M3UA) (int, error) {
		if _, ok := message.(*messages.AspActive); ok {
			oldWriteOnce.Do(func() { close(oldWriteStarted) })
			<-releaseOldWrite
		}
		writes <- message
		return message.MarshalLen(), nil
	}
	conn.startTAck(messages.NewAspActive(
		conn.cfg.TrafficModeType.Copy(), conn.cfg.RoutingContexts.Copy(), nil,
	), requestAspActive)
	select {
	case <-oldWriteStarted:
	case <-time.After(time.Second):
		t.Fatal("old T(ack) did not enter its retransmission write")
	}

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- conn.Shutdown() }()
	select {
	case signal := <-writes:
		t.Errorf("shutdown wrote %T before the old T(ack) write drained", signal)
	case <-time.After(25 * time.Millisecond):
	}
	close(releaseOldWrite)

	first := receiveSignal(t, writes)
	if _, ok := first.(*messages.AspActive); !ok {
		t.Fatalf("first completed write = %T, want the already-started ASP Active", first)
	}
	second := receiveSignal(t, writes)
	if _, ok := second.(*messages.AspInactive); !ok {
		t.Fatalf("next write = %T, want ASP Inactive after the old write", second)
	}
	conn.handleSignals(context.Background(), messages.NewAspInactiveAck(
		conn.cfg.RoutingContexts.Copy(), nil,
	))
	if _, ok := receiveSignal(t, writes).(*messages.AspDown); !ok {
		t.Fatal("shutdown did not send ASP Down after ASP Inactive Ack")
	}
	conn.handleSignals(context.Background(), messages.NewAspDownAck(nil))
	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not finish")
	}
}

// While termination is waiting for its own request, a delayed Ack for a
// cancelled establishment request belongs to the retired epoch and must not
// reverse the withdrawal state.
func TestShutdownIgnoresDelayedCancelledAspActiveAck(t *testing.T) {
	conn, _ := newTestConn(t, StateASPActive, RoleASP)
	conn.cfg.TAck = time.Hour
	conn.startTAck(messages.NewAspActive(
		conn.cfg.TrafficModeType.Copy(), conn.cfg.RoutingContexts.Copy(), nil,
	), requestAspActive)

	writes := make(chan messages.M3UA, 8)
	conn.signalWriter = func(message messages.M3UA) (int, error) {
		writes <- message
		return message.MarshalLen(), nil
	}
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- conn.Shutdown() }()
	if _, ok := receiveSignal(t, writes).(*messages.AspInactive); !ok {
		t.Fatal("shutdown did not send ASP Inactive")
	}
	conn.handleSignals(context.Background(), messages.NewAspInactiveAck(
		conn.cfg.RoutingContexts.Copy(), nil,
	))
	if _, ok := receiveSignal(t, writes).(*messages.AspDown); !ok {
		t.Fatal("shutdown did not send ASP Down")
	}
	if got := conn.State(); got != StateASPInactive {
		t.Fatalf("state while waiting for ASP Down Ack = %v, want ASP-INACTIVE", got)
	}

	conn.handleSignals(context.Background(), messages.NewAspActiveAck(
		conn.cfg.TrafficModeType.Copy(), conn.cfg.RoutingContexts.Copy(), nil,
	))
	if got := conn.State(); got != StateASPInactive {
		t.Errorf("delayed cancelled ASP Active Ack changed shutdown state to %v", got)
	}

	conn.handleSignals(context.Background(), messages.NewAspDownAck(nil))
	select {
	case <-shutdownDone:
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not finish")
	}
}

func TestShutdownWaitsForEveryPartialInactiveAck(t *testing.T) {
	conn, _ := newTestConn(t, StateASPActive, RoleASP)
	conn.cfg.TAck = time.Second

	writes := make(chan messages.M3UA, 8)
	conn.signalWriter = func(message messages.M3UA) (int, error) {
		writes <- message
		return message.MarshalLen(), nil
	}
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- conn.Shutdown() }()

	if _, ok := receiveSignal(t, writes).(*messages.AspInactive); !ok {
		t.Fatal("shutdown did not begin with ASP Inactive")
	}
	conn.handleSignals(context.Background(), messages.NewAspInactiveAck(
		params.NewRoutingContext(1), nil))
	assertNoSignal(t, writes, 25*time.Millisecond, "ASP Down after only a partial ASP Inactive Ack")

	conn.handleSignals(context.Background(), messages.NewAspInactiveAck(
		params.NewRoutingContext(2), nil))
	if _, ok := receiveSignal(t, writes).(*messages.AspDown); !ok {
		t.Fatal("shutdown did not advance to ASP Down after the final partial Ack")
	}
	conn.handleSignals(context.Background(), messages.NewAspDownAck(nil))
	select {
	case <-shutdownDone:
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not finish after the final Ack")
	}
}

func TestShutdownContextCancellationStopsTAckAndCloses(t *testing.T) {
	conn, _ := newTestConn(t, StateASPActive, RoleASP)
	conn.cfg.TAck = time.Second

	writes := make(chan messages.M3UA, 4)
	conn.signalWriter = func(message messages.M3UA) (int, error) {
		writes <- message
		return message.MarshalLen(), nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- conn.ShutdownContext(ctx) }()
	if _, ok := receiveSignal(t, writes).(*messages.AspInactive); !ok {
		t.Fatal("shutdown did not begin with ASP Inactive")
	}
	cancel()

	select {
	case err := <-shutdownDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ShutdownContext error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("ShutdownContext did not stop after cancellation")
	}
	if conn.pendingTAck() != 0 {
		t.Error("cancelled shutdown left T(ack) armed")
	}
	select {
	case <-conn.Done():
	default:
		t.Error("cancelled shutdown did not release the association")
	}
	assertNoSignal(t, writes, 25*time.Millisecond, "ASP Down after cancellation")
}

func TestShutdownRetransmitsAspDownUntilAcked(t *testing.T) {
	conn, _ := newTestConn(t, StateASPInactive, RoleASP)
	conn.cfg.TAck = 10 * time.Millisecond
	conn.cfg.TAckRetries = 20

	writes := make(chan messages.M3UA, 32)
	conn.signalWriter = func(message messages.M3UA) (int, error) {
		writes <- message
		return message.MarshalLen(), nil
	}
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- conn.Shutdown() }()

	for index := 0; index < 2; index++ {
		if _, ok := receiveSignal(t, writes).(*messages.AspDown); !ok {
			t.Fatal("shutdown emitted a non-ASP-Down signal while awaiting ASP Down Ack")
		}
	}
	conn.handleSignals(context.Background(), messages.NewAspDownAck(nil))
	select {
	case <-shutdownDone:
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not finish after retransmitted ASP Down was acknowledged")
	}
}

func TestShutdownWhileAlreadyDownUsesSCTPOnly(t *testing.T) {
	conn, sent := newTestConn(t, StateASPDown, RoleASP)
	_ = conn.Shutdown()
	if len(*sent) != 0 {
		t.Fatalf("ASP-DOWN Shutdown sent %v, want SCTP shutdown only", typeNames(*sent))
	}
}

func TestShutdownWriteFailureStopsTAckAndCloses(t *testing.T) {
	conn, _ := newTestConn(t, StateASPActive, RoleASP)
	want := errors.New("write failed")
	conn.signalWriter = func(messages.M3UA) (int, error) { return 0, want }

	err := conn.Shutdown()
	if !errors.Is(err, want) {
		t.Fatalf("Shutdown error = %v, want write failure", err)
	}
	if conn.pendingTAck() != 0 {
		t.Error("failed shutdown write left T(ack) armed")
	}
	select {
	case <-conn.Done():
	default:
		t.Error("failed shutdown write did not close the association")
	}
}

func TestShutdownDownEntryDoesNotRestartAspUp(t *testing.T) {
	conn, sent := newTestConn(t, StateASPInactive, RoleASP)
	conn.terminating.Store(true)
	conn.startTAck(messages.NewAspDown(nil), requestAspDown)
	conn.handleSignals(context.Background(), messages.NewAspDownAck(nil))

	if err := conn.handleStateUpdate(StateASPDown); err != nil {
		t.Fatalf("ASP-DOWN entry during shutdown: %v", err)
	}
	if got := countType(*sent, "ASP Up"); got != 0 {
		t.Fatalf("shutdown ASP-DOWN entry sent %d ASP Up requests", got)
	}
}

func TestShutdownContextAlreadyCanceledSendsNothing(t *testing.T) {
	conn, sent := newTestConn(t, StateASPActive, RoleASP)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := conn.ShutdownContext(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ShutdownContext error = %v, want context.Canceled", err)
	}
	if len(*sent) != 0 {
		t.Fatalf("already-cancelled shutdown sent %v", typeNames(*sent))
	}
	select {
	case <-conn.Done():
	default:
		t.Error("already-cancelled shutdown did not release SCTP")
	}
}

func installImmediateShutdownPeer(conn *Association, sent *[]messages.M3UA) {
	conn.signalWriter = func(message messages.M3UA) (int, error) {
		*sent = append(*sent, message)
		switch request := message.(type) {
		case *messages.AspInactive:
			conn.handleSignals(context.Background(), messages.NewAspInactiveAck(request.RoutingContext.Copy(), nil))
		case *messages.AspDown:
			conn.handleSignals(context.Background(), messages.NewAspDownAck(nil))
		}
		return message.MarshalLen(), nil
	}
}

func receiveSignal(t *testing.T, signals <-chan messages.M3UA) messages.M3UA {
	t.Helper()
	select {
	case signal := <-signals:
		return signal
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for shutdown signal")
		return nil
	}
}

func assertNoSignal(t *testing.T, signals <-chan messages.M3UA, duration time.Duration, description string) {
	t.Helper()
	select {
	case signal := <-signals:
		t.Fatalf("received %T: %s", signal, description)
	case <-time.After(duration):
	}
}
