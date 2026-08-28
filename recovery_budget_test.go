package m3ua

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestListenerRecoveryMessageBudgetSpansApplicationServers(t *testing.T) {
	listener, first, second, _ := pendingTwoApplicationServerFixture(t, func(config *AssociationConfig) {
		config.RecoveryQueueMessages = 10
		config.RecoveryQueueBytes = 1 << 20
		config.RecoveryQueueTotalMessages = 1
		config.RecoveryQueueTotalBytes = 1 << 20
	})

	if result, err := listener.DistributeData(distributionData(1, 1, "first AS")); err != nil || !result.Queued {
		t.Fatalf("first AS distribution = %#v, %v; want queued", result, err)
	}
	if _, err := listener.DistributeData(distributionData(2, 1, "second AS")); !errors.Is(err, ErrRecoveryQueueFull) {
		t.Fatalf("second AS aggregate-budget error = %v, want %v", err, ErrRecoveryQueueFull)
	}

	first.mu.Lock()
	firstGeneration := first.recoveryGen
	first.mu.Unlock()
	first.recoveryExpired(firstGeneration)
	if result, err := listener.DistributeData(distributionData(2, 1, "after release")); err != nil || !result.Queued {
		t.Fatalf("second AS after release = %#v, %v; want queued", result, err)
	}
	if messages, _ := recoveryBudgetUsage(listener.as.recoveryBudget); messages != 1 {
		t.Fatalf("aggregate retained messages after expiry/requeue = %d, want 1", messages)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("Listener.Close: %v", err)
	}
	if messages, bytes := recoveryBudgetUsage(listener.as.recoveryBudget); messages != 0 || bytes != 0 {
		t.Fatalf("aggregate budget after close = %d messages, %d bytes; want zero", messages, bytes)
	}
	_ = second
}

func TestListenerRecoveryByteBudgetAcceptsExactBoundaryOnly(t *testing.T) {
	firstData := distributionData(1, 1, "first")
	secondData := distributionData(2, 1, "second")
	firstWire, err := firstData.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	secondWire, err := secondData.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	totalBytes := len(firstWire) + len(secondWire)
	listener, _, _, _ := pendingTwoApplicationServerFixture(t, func(config *AssociationConfig) {
		config.RecoveryQueueMessages = 10
		config.RecoveryQueueBytes = 1 << 20
		config.RecoveryQueueTotalMessages = 10
		config.RecoveryQueueTotalBytes = totalBytes
	})

	for index, data := range []struct {
		routingContext uint32
		payload        string
	}{{1, "first"}, {2, "second"}} {
		result, err := listener.DistributeData(distributionData(data.routingContext, 1, data.payload))
		if err != nil || !result.Queued {
			t.Fatalf("boundary message %d = %#v, %v; want queued", index, result, err)
		}
	}
	if _, err := listener.DistributeData(distributionData(1, 1, "x")); !errors.Is(err, ErrRecoveryQueueFull) {
		t.Fatalf("byte beyond exact aggregate boundary error = %v, want %v", err, ErrRecoveryQueueFull)
	}
}

func TestListenerRecoveryBudgetCountsActiveInFlightDelivery(t *testing.T) {
	listener, first, asp, _ := distributionFixtureForContexts(
		t, params.TrafficModeLoadshare, []uint32{1, 2}, func(config *AssociationConfig) {
			config.RecoveryQueueMessages = 10
			config.RecoveryQueueBytes = 1 << 20
			config.RecoveryQueueTotalMessages = 1
			config.RecoveryQueueTotalBytes = 1 << 20
		},
	)
	second := listener.as.get(2)
	asp.noteRoutingContextsActive([]uint32{1, 2})
	asp.setState(StateASPActive)
	first.setASPState(asp, StateASPActive, time.Hour)
	second.setASPState(asp, StateASPActive, time.Hour)

	started := make(chan struct{})
	release := make(chan struct{})
	var firstWrite atomic.Bool
	asp.signalWriter = func(message messages.M3UA) (int, error) {
		if _, ok := message.(*messages.Data); ok && firstWrite.CompareAndSwap(false, true) {
			close(started)
			<-release
		}
		return message.MarshalLen(), nil
	}
	firstDone := make(chan error, 1)
	go func() {
		_, err := listener.DistributeData(distributionData(1, 1, "in flight"))
		firstDone <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("active delivery did not reach the writer")
	}
	if _, err := listener.DistributeData(distributionData(2, 1, "other AS")); !errors.Is(err, ErrRecoveryQueueFull) {
		t.Fatalf("other AS during aggregate in-flight delivery error = %v, want %v", err, ErrRecoveryQueueFull)
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("first delivery: %v", err)
	}
	if result, err := listener.DistributeData(distributionData(2, 1, "after release")); err != nil || result.Delivered != 1 {
		t.Fatalf("other AS after in-flight release = %#v, %v; want delivered", result, err)
	}
	if messages, bytes := recoveryBudgetUsage(listener.as.recoveryBudget); messages != 0 || bytes != 0 {
		t.Fatalf("aggregate budget after deliveries = %d messages, %d bytes; want zero", messages, bytes)
	}
}

func pendingTwoApplicationServerFixture(t *testing.T, configure func(*AssociationConfig)) (*Listener, *applicationServer, *applicationServer, *Association) {
	t.Helper()
	listener, first, asp, _ := distributionFixtureForContexts(
		t, params.TrafficModeLoadshare, []uint32{1, 2}, configure,
	)
	second := listener.as.get(2)
	asp.noteRoutingContextsActive([]uint32{1, 2})
	asp.setState(StateASPActive)
	first.setASPState(asp, StateASPActive, time.Hour)
	second.setASPState(asp, StateASPActive, time.Hour)
	first.setASPState(asp, StateASPInactive, time.Hour)
	second.setASPState(asp, StateASPInactive, time.Hour)
	return listener, first, second, asp
}

func recoveryBudgetUsage(budget *recoveryBudget) (int, int) {
	budget.mu.Lock()
	defer budget.mu.Unlock()
	return budget.messages, budget.bytes
}
