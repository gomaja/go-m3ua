// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func overflowTransportAttempts(transport *fullSendBuffer) int {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	return len(transport.attempted)
}

func TestOverflowPendingOnsetExpiresBeforeTransport(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(fmt.Sprintf("new_episode_%t", restart), func(t *testing.T) {
			c, _ := overflowAssociation(t)
			transport := newFullSendBuffer()
			transport.firstErr = nil
			c.signalWriter = nil
			c.controlTransport = transport
			c.transportCloser = transport.close
			c.sendMu.Lock()
			var release sync.Once
			unlock := func() { release.Do(c.sendMu.Unlock) }
			defer unlock()
			deliver(t, c, 1, inboundData(1, "discarded"))
			old := firstErr(c)
			if old == nil {
				t.Fatal("missing onset")
			}
			written := make(chan error, 1)
			go func() { written <- c.handleErrors(old) }()
			select {
			case err := <-written:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("overflow producer waited for writer")
			}
			c.endpoint.runCongestionAutomation(time.Now())
			if overflowTransportAttempts(transport) != 0 {
				t.Fatal("contended onset submitted")
			}
			if !nextQueueCongestion(t, c).Congested {
				t.Fatal("missing onset")
			}
			for range 2 {
				if _, err := c.ReadData(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			if nextQueueCongestion(t, c).Congested {
				t.Fatal("missing end")
			}
			if len(c.endpoint.CongestionPublications()) != 0 {
				t.Fatal("ended unsent onset kept reservation")
			}
			if restart {
				for range 3 {
					deliver(t, c, 1, inboundData(1, "new episode"))
				}
				if firstErr(c) == nil {
					t.Fatal("new onset missing")
				}
			}
			unlock()
			runOverflowPublicationsForTest(c)
			want := 0
			if restart {
				want = 1
			}
			if got := overflowTransportAttempts(transport); got != want {
				t.Fatalf("transport attempts=%d, want only current episode=%d", got, want)
			}
		})
	}
}

func TestOverflowDoubleExchangeWithdrawalSerializesPendingOnset(t *testing.T) {
	c, _ := newDoubleExchangeIPSPForTest(t)
	c.endpoint = methodEndpointForTest(t, RoleIPSP, uint8(CongestionInternational), 0)
	c.setIPSPState(IPSPState{TrafficToLocal: StateASPActive, TrafficToPeer: StateASPActive})
	c.noteRoutingContextsAcked(params.NewRoutingContext(11))
	c.noteRoutingContextsActive([]uint32{22})
	attachOverflowEndpointForTest(t, c)
	c.recvStream.Store(1)
	c.dataChan = make(chan *DataMessage, 1)
	if err := c.DeactivateRoutingContexts(11); err != nil {
		t.Fatal(err)
	}
	transport := newFullSendBuffer()
	transport.firstErr = nil
	c.signalWriter = nil
	c.controlTransport = transport
	c.transportCloser = transport.close
	c.sendMu.Lock()
	var release sync.Once
	unlock := func() { release.Do(c.sendMu.Unlock) }
	defer unlock()
	data := messages.NewData(params.NewNetworkAppearance(10), params.NewRoutingContext(11), params.NewProtocolData(1, 2, 3, 0, 0, 1, nil), nil)
	for range 2 {
		deliver(t, c, 1, data)
	}
	overflow := firstErr(c)
	if overflow == nil {
		t.Fatal("missing onset")
	}
	written := make(chan error, 1)
	go func() { written <- c.handleErrors(overflow) }()
	select {
	case err := <-written:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("optional producer waited behind writer")
	}
	c.endpoint.runCongestionAutomation(time.Now())
	withdrawn := make(chan error, 1)
	go func() {
		withdrawn <- c.handleAspInactiveAck(messages.NewAspInactiveAck(params.NewRoutingContext(11), nil))
	}()
	select {
	case err := <-withdrawn:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		unlock()
		if err := <-withdrawn; err != nil {
			t.Error(err)
		}
		t.Fatal("local withdrawal waited for optional publication")
	}
	unlock()
	runOverflowPublicationsForTest(c)
	if overflowTransportAttempts(transport) != 0 {
		t.Fatal("withdrawn local scope submitted onset")
	}
	if state := c.IPSPState(); state.TrafficToLocal != StateASPInactive || state.TrafficToPeer != StateASPActive {
		t.Fatalf("wrong direction withdrawn: %+v", state)
	}
}

func TestOverflowIdleDataPathDoesNotTakeTrackerMutex(t *testing.T) {
	for _, operation := range []string{"enqueue", "dequeue"} {
		t.Run(operation, func(t *testing.T) {
			c, _ := newDataWriteAssociation(t, 1)
			c.endpoint = methodEndpointForTest(t, RoleASP, uint8(CongestionInternational), 0)
			if operation == "dequeue" {
				deliver(t, c, 1, inboundData(1, "queued"))
			}
			c.muDataOverflow.Lock()
			finished := make(chan struct{})
			go func() {
				if operation == "enqueue" {
					deliver(t, c, 1, inboundData(1, "ordinary"))
				} else {
					if _, err := c.ReadData(context.Background()); err != nil {
						t.Error(err)
					}
				}
				close(finished)
			}()
			blocked := false
			select {
			case <-finished:
			case <-time.After(100 * time.Millisecond):
				blocked = true
			}
			c.muDataOverflow.Unlock()
			<-finished
			if blocked {
				t.Fatal("ordinary DATA path acquired the overflow tracker mutex")
			}
		})
	}
}

func TestOverflowManagementPublicationReleasesTrackerMutex(t *testing.T) {
	c, _ := overflowAssociation(t)
	c.muMgmt.Lock()
	completed := make(chan struct{})
	go func() { deliver(t, c, 1, inboundData(1, "discarded")); close(completed) }()
	blocked := waitFor(func() bool {
		for _, stack := range goroutinesBlockedIn("notifyManagement") {
			if strings.Contains(stack, "recordDataOverflow") && strings.Contains(stack, "lockSlow") {
				return true
			}
		}
		return false
	}, time.Second)
	available := c.muDataOverflow.TryLock()
	if available {
		c.muDataOverflow.Unlock()
	}
	c.muMgmt.Unlock()
	<-completed
	if !blocked {
		t.Fatal("management onset did not reach publication")
	}
	if !available {
		t.Fatal("management publication held the overflow tracker mutex")
	}
}

func TestOverflowConcurrentHandoffsAllowSustainedDrain(t *testing.T) {
	for _, capacity := range []int{1, 2} {
		t.Run(fmt.Sprint(capacity), func(t *testing.T) {
			c, _ := newDataWriteAssociation(t, 1)
			c.endpoint = methodEndpointForTest(t, RoleASP, uint8(CongestionInternational), 0)
			c.dataChan = make(chan *DataMessage, capacity)
			for range capacity + 1 {
				deliver(t, c, 1, inboundData(1, "initial overflow"))
			}
			_ = firstErr(c)
			for range capacity {
				if _, err := c.ReadData(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			received := make(chan struct{}, 1)
			stopped := make(chan struct{})
			go func() {
				defer close(stopped)
				for {
					if _, err := c.ReadData(ctx); err != nil {
						return
					}
					received <- struct{}{}
				}
			}()
			defer func() { cancel(); <-stopped }()
			limit := time.Now().Add(2 * time.Second)
			for c.DataQueueStats().Congested && time.Now().Before(limit) {
				if !waitFor(func() bool { return len(goroutinesBlockedIn("(*Association).ReadData")) != 0 }, time.Second) {
					t.Fatal("reader did not park")
				}
				deliver(t, c, 1, inboundData(1, "direct handoff"))
				select {
				case <-received:
				case <-time.After(time.Second):
					t.Fatal("reader did not receive handoff")
				}
				if queued := len(c.dataChan); queued != 0 {
					t.Fatalf("handoff left %d queued messages", queued)
				}
				time.Sleep(5 * time.Millisecond)
			}
			if c.DataQueueStats().Congested {
				t.Fatal("direct handoffs prevented recovery while actual occupancy stayed low")
			}
		})
	}
}

func TestOverflowDoubleExchangeCarriesLocalDirectionScope(t *testing.T) {
	c, sent := newDoubleExchangeIPSPForTest(t)
	c.endpoint = methodEndpointForTest(t, RoleIPSP, uint8(CongestionInternational), 0)
	c.setIPSPState(IPSPState{TrafficToLocal: StateASPActive, TrafficToPeer: StateASPActive})
	c.noteRoutingContextsAcked(params.NewRoutingContext(11))
	c.noteRoutingContextsActive([]uint32{22})
	attachOverflowEndpointForTest(t, c)
	*sent = nil
	c.recvStream.Store(1)
	c.dataChan = make(chan *DataMessage, 1)
	for range 2 {
		deliver(t, c, 1, messages.NewData(params.NewNetworkAppearance(10), params.NewRoutingContext(11), params.NewProtocolData(1, 2, 3, 0, 0, 1, nil), nil))
	}
	handleOverflowReports(t, c)
	if len(*sent) != 1 {
		t.Fatalf("sent %d messages, want one local-direction SCON", len(*sent))
	}
	scon, ok := (*sent)[0].(*messages.SignallingCongestion)
	if !ok || scon.NetworkAppearance == nil || scon.NetworkAppearance.NetworkAppearance() != 10 || scon.RoutingContext == nil || len(scon.RoutingContext.RoutingContexts()) != 1 || scon.RoutingContext.RoutingContexts()[0] != 11 || scon.ConcernedDestination != nil || scon.CongestionIndications != nil {
		t.Fatalf("local-direction SCON = %v", (*sent)[0])
	}
}

func TestOverflowTrackingLossResetsAfterRecovery(t *testing.T) {
	c, _ := overflowAssociation(t)
	overflowAll := func() {
		t.Helper()
		for i := 0; i < dataOverflowScopeLimit+1; i++ {
			deliver(t, c, 1, messages.NewData(nil, params.NewRoutingContext(1), params.NewProtocolData(uint32(i), 2, 3, 0, 0, 1, nil), nil))
			_ = firstErr(c)
			ind := nextQueueCongestion(t, c)
			if ind.TrackingLost != (i == dataOverflowScopeLimit) {
				t.Fatalf("scope %d tracking loss=%t", i, ind.TrackingLost)
			}
		}
	}
	overflowAll()
	for range 2 {
		if _, err := c.ReadData(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	for range dataOverflowScopeLimit {
		if nextQueueCongestion(t, c).Congested {
			t.Fatal("missing tracked end")
		}
	}
	if c.DataQueueStats().Congested {
		t.Fatal("association did not end episode")
	}
	for range 2 {
		deliver(t, c, 1, inboundData(1, "refill"))
	}
	overflowAll()
	if got := c.DataQueueStats().ScopeTrackingLosses; got != 2 {
		t.Fatalf("cumulative tracking losses=%d, want 2", got)
	}
}

func TestOverflowWorkersRecoverIndependentAssociations(t *testing.T) {
	const count = 32
	associations := make([]*Association, count)
	for i := range associations {
		c, _ := overflowAssociation(t)
		associations[i] = c
		deliver(t, c, 1, inboundData(1, "discarded"))
		_ = firstErr(c)
		if i%2 == 0 {
			for range 2 {
				if _, err := c.ReadData(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	if !waitFor(func() bool {
		for i, c := range associations {
			if i%2 == 0 && c.DataQueueStats().Congested {
				return false
			}
		}
		return true
	}, 2*time.Second) {
		t.Fatal("drained associations did not recover independently")
	}
	for i, c := range associations {
		if i%2 == 1 && !c.DataQueueStats().Congested {
			t.Fatalf("full association %d recovered with its drained neighbors", i)
		}
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if !waitFor(func() bool {
		for _, c := range associations {
			c.muDataOverflow.Lock()
			running := c.dataOverflowWorker
			c.muDataOverflow.Unlock()
			if running {
				return false
			}
		}
		return true
	}, time.Second) {
		t.Fatal("closed associations retained overflow workers")
	}
}

func TestOverflowEndPublicationReleasesTrackerMutex(t *testing.T) {
	c, _ := overflowAssociation(t)
	deliver(t, c, 1, inboundData(1, "discarded"))
	_ = firstErr(c)
	_ = nextQueueCongestion(t, c)
	for range 2 {
		if _, err := c.ReadData(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	c.muMgmt.Lock()
	c.muDataOverflow.Lock()
	c.dataOverflowLowSince = time.Now().Add(-dataOverflowDrainDwell)
	c.muDataOverflow.Unlock()
	completed := make(chan struct{})
	go func() { c.observeDataQueue(); close(completed) }()
	blocked := waitFor(func() bool {
		for _, stack := range goroutinesBlockedIn("notifyManagement") {
			if strings.Contains(stack, "observeDataQueue") && strings.Contains(stack, "lockSlow") {
				return true
			}
		}
		return false
	}, time.Second)
	available := c.muDataOverflow.TryLock()
	if available {
		c.muDataOverflow.Unlock()
	}
	c.muMgmt.Unlock()
	<-completed
	if !blocked {
		t.Fatal("management end did not reach publication")
	}
	if !available {
		t.Fatal("management end publication held the overflow tracker mutex")
	}
}

func TestOverflowActiveObservationsDoNotWaitForManagementPublisher(t *testing.T) {
	for _, operation := range []string{"enqueue", "dequeue"} {
		t.Run(operation, func(t *testing.T) {
			c, _ := overflowAssociation(t)
			deliver(t, c, 1, inboundData(1, "discarded"))
			_ = firstErr(c)
			if _, err := c.ReadData(context.Background()); err != nil {
				t.Fatal(err)
			}
			c.muDataOverflowEvents.Lock()
			finished := make(chan struct{})
			go func() {
				if operation == "enqueue" {
					deliver(t, c, 1, inboundData(1, "refill"))
				} else {
					if _, err := c.ReadData(context.Background()); err != nil {
						t.Error(err)
					}
				}
				close(finished)
			}()
			blocked := false
			select {
			case <-finished:
			case <-time.After(100 * time.Millisecond):
				blocked = true
			}
			c.muDataOverflowEvents.Unlock()
			<-finished
			if blocked {
				t.Fatal("active occupancy observation waited for management publication")
			}
		})
	}
}
