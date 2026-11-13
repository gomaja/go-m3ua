package m3ua

import (
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestSGPReadyNotifyBacklogExcludesRetiredMembership(t *testing.T) {
	for _, action := range []string{"active-up", "down-then-up", "explicit-dereg"} {
		for _, removeUnused := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/remove-unused=%v", action, removeUnused), func(t *testing.T) {
				endpoint := newRegistrationLifetimeEndpoint(t, removeUnused)
				endpoint.as.recoveryTimer = 50 * time.Millisecond
				retiring, sent := newRegistrationLifetimeASP(t, endpoint)
				withdrawing, otherSent := newRegistrationLifetimeASP(t, endpoint)
				survivor, survivorSent := newRegistrationLifetimeASP(t, endpoint)
				x := registerLifetimeKey(t, retiring, sent, 1, testRoutingKey(10, 100, 3))
				registerLifetimeKey(t, withdrawing, otherSent, 1, testRoutingKey(10, 100, 3))
				registerLifetimeKey(t, survivor, survivorSent, 1, testRoutingKey(10, 100, 3))
				y := registerLifetimeKey(t, retiring, sent, 2, testRoutingKey(10, 200, 3))
				xKey, _ := retiring.dynamicASKey(x, false)
				yKey, _ := retiring.dynamicASKey(y, false)
				asX, _ := endpoint.as.lookup(xKey)
				asY, _ := endpoint.as.lookup(yKey)
				retiring.commitState(StateASPActive)
				retiring.noteRoutingContextsActive([]uint32{y})
				asY.setASPState(retiring, StateASPActive, time.Hour)
				withdrawing.commitState(StateASPActive)
				withdrawing.noteRoutingContextsActive([]uint32{x})
				asX.setASPState(withdrawing, StateASPActive, time.Hour)

				var mu sync.Mutex
				var retiredStates, survivingStates []uint32
				recorder := func(states *[]uint32) func(messages.M3UA) (int, error) {
					return func(message messages.M3UA) (int, error) {
						if notify, ok := message.(*messages.Notify); ok && notify.Status.StatusType() == params.AsStateChange && notify.RoutingContext != nil && notify.RoutingContext.RoutingContext() == x {
							mu.Lock()
							*states = append(*states, notify.Status.Status())
							mu.Unlock()
						}
						return message.MarshalLen(), nil
					}
				}
				retiring.signalWriter = recorder(&retiredStates)
				survivor.signalWriter = recorder(&survivingStates)
				ackStarted := make(chan struct{})
				allowAck := make(chan struct{})
				var unblockOnce sync.Once
				unblock := func() { unblockOnce.Do(func() { close(allowAck) }) }
				withdrawing.signalWriter = func(message messages.M3UA) (int, error) {
					if _, ok := message.(*messages.AspDownAck); ok {
						close(ackStarted)
						<-allowAck
					}
					return message.MarshalLen(), nil
				}
				downResult := make(chan error, 1)
				downDone := make(chan struct{})
				go func() { defer close(downDone); downResult <- withdrawing.handleAspDown(messages.NewAspDown(nil)) }()
				t.Cleanup(func() {
					unblock()
					select {
					case <-downDone:
					case <-time.After(3 * time.Second):
						t.Error("blocked peer's Down did not finish")
					}
				})
				select {
				case <-ackStarted:
				case <-time.After(3 * time.Second):
					t.Fatal("peer did not reach its blocked Down Ack")
				}

				var later *asNotification
				if !waitFor(func() bool {
					asX.mu.Lock()
					defer asX.mu.Unlock()
					if len(asX.notifications) != 2 || asX.notifications[0].status != params.AsStatePending || asX.notifications[1].status != params.AsStateInactive {
						return false
					}
					later = asX.notifications[1]
					select {
					case <-later.ready:
						return true
					default:
						return false
					}
				}, 3*time.Second) {
					t.Fatal("T(r) did not queue a ready AS-INACTIVE event behind the Ack-gated AS-PENDING event")
				}

				switch action {
				case "active-up":
					if err := retiring.handleAspUp(messages.NewAspUp(nil, nil)); !isLifetimeUnexpectedMessage(err) {
						t.Fatal(err)
					}
				case "down-then-up":
					if err := retiring.handleAspDown(messages.NewAspDown(nil)); err != nil {
						t.Fatal(err)
					}
					if err := retiring.handleAspUp(messages.NewAspUp(nil, nil)); err != nil {
						t.Fatal(err)
					}
				case "explicit-dereg":
					if err := retiring.handleDeregistrationRequest(messages.NewDeregistrationRequest(params.NewRoutingContext(x))); err != nil {
						t.Fatal(err)
					}
					if _, ok := retiring.dynamicASKey(y, false); !ok {
						t.Fatal("scoped DEREG also retired the other AS")
					}
				}
				asX.mu.Lock()
				_, member := asX.asps[retiring]
				asX.mu.Unlock()
				if member {
					t.Fatal("withdrawal did not retire AS X membership")
				}
				unblock()
				select {
				case <-downDone:
				case <-time.After(3 * time.Second):
					t.Fatal("peer's Down did not finish after releasing Ack")
				}
				if err := <-downResult; err != nil {
					t.Fatal(err)
				}
				select {
				case <-later.done:
				case <-time.After(3 * time.Second):
					t.Fatal("ready event did not drain after the earlier Ack")
				}
				mu.Lock()
				defer mu.Unlock()
				if len(retiredStates) != 0 {
					t.Errorf("retired ASP received backlogged AS-state Notify: %v", retiredStates)
				}
				if !slices.Equal(survivingStates, []uint32{params.AsStatePending, params.AsStateInactive}) {
					t.Errorf("surviving ASP notifications = %v, want AS-PENDING then AS-INACTIVE", survivingStates)
				}
			})
		}
	}
}

func TestSGPQueuedNotifyRechecksMembershipAtWrite(t *testing.T) {
	endpoint := newRegistrationLifetimeEndpoint(t, false)
	retiring, sent := newRegistrationLifetimeASP(t, endpoint)
	survivor, otherSent := newRegistrationLifetimeASP(t, endpoint)
	rc := registerLifetimeKey(t, retiring, sent, 1, testRoutingKey(10, 100, 3))
	registerLifetimeKey(t, survivor, otherSent, 1, testRoutingKey(10, 100, 3))
	key, _ := retiring.dynamicASKey(rc, false)
	as, _ := endpoint.as.lookup(key)
	retiring.commitState(StateASPActive)
	retiring.noteRoutingContextsActive([]uint32{rc})
	started := make(chan struct{})
	allowWrite := make(chan struct{})
	var unblockOnce, firstWrite sync.Once
	unblock := func() { unblockOnce.Do(func() { close(allowWrite) }) }
	var mu sync.Mutex
	stale := 0
	retiring.notificationQueue = make(chan mandatoryControl, defaultNotificationQueueSize)
	retiring.notificationWriter = func(message messages.M3UA) (int, error) {
		if _, ok := message.(*messages.Heartbeat); ok {
			firstWrite.Do(func() { close(started); <-allowWrite })
		}
		if notify, ok := message.(*messages.Notify); ok && notify.Status.StatusType() == params.AsStateChange && notify.RoutingContext != nil && notify.RoutingContext.RoutingContext() == rc {
			as.mu.Lock()
			_, member := as.asps[retiring]
			as.mu.Unlock()
			if !member {
				mu.Lock()
				stale++
				mu.Unlock()
			}
		}
		return message.MarshalLen(), nil
	}
	if err := retiring.writeMandatoryControls([]messages.M3UA{messages.NewHeartbeat(nil)}, false, false); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(unblock)
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("control writer did not block")
	}
	// The AS event finishes enqueueing while the transport worker remains blocked.
	as.setASPState(retiring, StateASPActive, time.Hour)
	upResult := make(chan error, 1)
	upDone := make(chan struct{})
	go func() { defer close(upDone); upResult <- retiring.handleAspUp(messages.NewAspUp(nil, nil)) }()
	t.Cleanup(func() {
		unblock()
		select {
		case <-upDone:
		case <-time.After(3 * time.Second):
			t.Error("Up did not finish")
		}
	})
	if !waitFor(func() bool { as.mu.Lock(); defer as.mu.Unlock(); _, member := as.asps[retiring]; return !member }, 3*time.Second) {
		t.Fatal("Up did not retire membership while an earlier non-AS control blocked the writer")
	}
	unblock()
	select {
	case <-upDone:
	case <-time.After(3 * time.Second):
		t.Fatal("Up did not finish after unblocking writer")
	}
	if err := <-upResult; !isLifetimeUnexpectedMessage(err) {
		t.Fatal(err)
	}
	if err := retiring.writeMandatoryControls([]messages.M3UA{messages.NewHeartbeat(nil)}, false, true); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if stale != 0 {
		t.Errorf("retired ASP received %d AS-state Notify messages from the control backlog", stale)
	}
}

func TestSGPInFlightNotifyPrecedesMembershipRetirement(t *testing.T) {
	endpoint := newRegistrationLifetimeEndpoint(t, false)
	endpoint.as.recoveryTimer = time.Hour
	retiring, sent := newRegistrationLifetimeASP(t, endpoint)
	survivor, otherSent := newRegistrationLifetimeASP(t, endpoint)
	rc := registerLifetimeKey(t, retiring, sent, 1, testRoutingKey(10, 100, 3))
	registerLifetimeKey(t, survivor, otherSent, 1, testRoutingKey(10, 100, 3))
	key, _ := retiring.dynamicASKey(rc, false)
	as, _ := endpoint.as.lookup(key)
	survivor.signalWriter = func(message messages.M3UA) (int, error) { return message.MarshalLen(), nil }
	retiring.commitState(StateASPActive)
	retiring.noteRoutingContextsActive([]uint32{rc})
	started := make(chan struct{})
	allowWrite := make(chan struct{})
	var unblockOnce sync.Once
	unblock := func() { unblockOnce.Do(func() { close(allowWrite) }) }
	retiring.notificationQueue = make(chan mandatoryControl, defaultNotificationQueueSize)
	retiring.notificationWriter = func(message messages.M3UA) (int, error) {
		if notify, ok := message.(*messages.Notify); ok && notify.Status.Status() == params.AsStateActive {
			close(started)
			<-allowWrite
			as.mu.Lock()
			_, member := as.asps[retiring]
			as.mu.Unlock()
			if !member {
				t.Error("membership retired before in-flight AS-state Notify finished")
			}
		}
		return message.MarshalLen(), nil
	}
	t.Cleanup(unblock)
	as.setASPState(retiring, StateASPActive, time.Hour)
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("AS-state Notify did not enter the writer")
	}
	upResult := make(chan error, 1)
	upDone := make(chan struct{})
	go func() { defer close(upDone); upResult <- retiring.handleAspUp(messages.NewAspUp(nil, nil)) }()
	t.Cleanup(func() {
		unblock()
		select {
		case <-upDone:
		case <-time.After(3 * time.Second):
			t.Error("Up did not finish")
		}
	})
	// Wait for the dispatcher to reach retirement. A pending write lock stops
	// TryRLock even while the earlier transport submission still owns its read lock.
	// If that barrier is missing, membership disappears instead.
	if !waitFor(func() bool {
		if !retiring.asStateNotifyMu.TryRLock() {
			return true
		}
		retiring.asStateNotifyMu.RUnlock()
		as.mu.Lock()
		_, member := as.asps[retiring]
		as.mu.Unlock()
		return !member
	}, 3*time.Second) {
		t.Fatal("Up did not reach membership retirement")
	}
	as.mu.Lock()
	_, member := as.asps[retiring]
	as.mu.Unlock()
	if !member {
		t.Error("membership retired before in-flight AS-state Notify finished")
	}
	unblock()
	select {
	case <-upDone:
	case <-time.After(3 * time.Second):
		t.Fatal("Up did not finish after the Notify submission")
	}
	if err := <-upResult; !isLifetimeUnexpectedMessage(err) {
		t.Fatal(err)
	}
	as.mu.Lock()
	_, member = as.asps[retiring]
	as.mu.Unlock()
	if member {
		t.Fatal("Up retained membership after the Notify completed")
	}
}
