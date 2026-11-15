// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestPeerPreferenceUsesLatestMatchingRangeAndWireScope(t *testing.T) {
	for _, rangeFirst := range []bool{true, false} {
		t.Run(map[bool]string{true: "newer exact clear", false: "newer range clear"}[rangeFirst], func(t *testing.T) {
			l, as, first, firstSent := distributionFixtureConfigured(t, params.TrafficModeLoadshare, func(c *distributionFixtureConfig) { c.PreferUncongestedPeers = true })
			second, secondSent := addDistributionASP(t, l, StateASPInactive, 1)
			as.setASPState(first, StateASPActive, time.Hour)
			as.setASPState(second, StateASPActive, time.Hour)
			first.muState.Lock()
			first.state = StateASPActive
			first.muState.Unlock()
			rangeAPC := params.NewAffectedPointCodeWithMask(8, 0)
			exactAPC := params.NewAffectedPointCode(2)
			initial, last := rangeAPC, exactAPC
			if !rangeFirst {
				initial, last = last, initial
			}
			for _, report := range []*messages.SignallingCongestion{
				messages.NewSignallingCongestion(params.NewNetworkAppearance(0), params.NewRoutingContext(1), initial, nil, params.NewCongestionIndications(3), nil),
				messages.NewSignallingCongestion(params.NewNetworkAppearance(0), params.NewRoutingContext(1), last, nil, params.NewCongestionIndications(0), nil),
			} {
				if err := first.handleSignallingCongestion(report); err != nil {
					t.Fatal(err)
				}
			}
			reportPeerPressure(t, second, params.NewCongestionIndications(1), 2)
			firstSent.reset()
			secondSent.reset()
			if _, err := l.DistributeData(distributionData(1, 0, "latest clear")); err != nil {
				t.Fatal(err)
			}
			if firstSent.dataCount() != 1 || secondSent.dataCount() != 0 {
				t.Fatal("older overlapping congestion overrode newer peer clear")
			}
		})
	}
}

func TestPeerRetentionDoesNotHoldBindingLockWhileWaitingForSampler(t *testing.T) {
	l, as, peer, _ := distributionFixture(t, params.TrafficModeLoadshare)
	_ = l
	as.setASPState(peer, StateASPActive, time.Hour)
	peer.transportMu.Lock()
	done := make(chan struct{})
	go func() { defer close(done); peer.syncPeerCongestionBindings() }()
	deadline := time.Now().Add(time.Second)
	// The receive path reaches the AS state lock before the sampler lock.
	for time.Now().Before(deadline) {
		if !as.mu.TryLock() {
			break
		}
		as.mu.Unlock()
		runtime.Gosched()
	}
	available := peer.muDynamicASKeys.TryLock()
	if available {
		peer.muDynamicASKeys.Unlock()
	}
	peer.transportMu.Unlock()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("retention did not leave sampler wait")
	}
	if !available {
		t.Fatal("retention held binding reader while waiting for transport sampler; queued binding writer can deadlock publication")
	}
}

func TestRecoveryBroadcastNeverRetriesAnIndeterminateCopy(t *testing.T) {
	l, as, first, firstSent := distributionFixture(t, params.TrafficModeBroadcast)
	second, secondSent := addDistributionASP(t, l, StateASPInactive, 1)
	as.setASPState(first, StateASPActive, time.Hour)
	as.setASPState(first, StateASPInactive, time.Hour)
	if result, err := l.DistributeData(distributionData(1, 0, "uncertain copy")); err != nil || !result.Queued {
		t.Fatalf("queue=%+v err=%v", result, err)
	}
	var attempts atomic.Int32
	second.signalWriter = func(m messages.M3UA) (int, error) {
		if _, ok := m.(*messages.Data); ok && attempts.Add(1) == 1 {
			return 1, errors.New("acceptance unknown")
		}
		return secondSent.write(m)
	}
	firstSent.reset()
	secondSent.reset()
	as.deliveryMu.Lock()
	as.setASPState(first, StateASPActive, time.Hour)
	as.setASPState(second, StateASPActive, time.Hour)
	as.deliveryMu.Unlock()
	if !waitFor(func() bool {
		messages, bytes := recoveryBudgetUsage(l.as.recoveryBudget)
		return messages == 0 && bytes == 0
	}, time.Second) {
		t.Fatal("indeterminate copy remained queued")
	}
	if attempts.Load() != 1 || firstSent.dataCount() != 1 {
		t.Fatalf("recovery retried an indeterminate/accepted copy: uncertain=%d accepted=%d", attempts.Load(), firstSent.dataCount())
	}
}

func TestRecoveryPolicyWaitCannotReviveExpiredDataOrLoseTheDeliverySlot(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	l, as, peer, sent := distributionFixtureConfigured(t, params.TrafficModeLoadshare, func(c *distributionFixtureConfig) {
		c.CongestionPolicy = func(SGPCongestionCandidate) bool { once.Do(func() { close(entered); <-release }); return true }
	})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	as.setASPState(peer, StateASPActive, time.Hour)
	as.setASPState(peer, StateASPInactive, time.Hour)
	if result, err := l.DistributeData(distributionData(1, 0, "expired")); err != nil || !result.Queued {
		t.Fatalf("queue=%+v %v", result, err)
	}
	sent.reset()
	as.setASPState(peer, StateASPActive, time.Hour)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("queued policy never entered")
	}
	as.setASPState(peer, StateASPInactive, time.Hour)
	as.mu.Lock()
	generation := as.recoveryGen
	as.mu.Unlock()
	as.recoveryExpired(generation)
	as.setASPState(peer, StateASPActive, time.Hour)
	type answer struct {
		result TrafficDistribution
		err    error
	}
	fresh := make(chan answer, 1)
	go func() { result, err := l.DistributeData(distributionData(1, 0, "fresh")); fresh <- answer{result, err} }()
	var result TrafficDistribution
	var err error
	select {
	case a := <-fresh:
		result, err = a.result, a.err
	case <-time.After(time.Second):
		close(release)
		<-fresh
		t.Fatal("fresh DATA entered a callback instead of joining the reserved FIFO")
	}
	if err != nil || !result.Queued || result.Delivered != 0 {
		close(release)
		t.Fatalf("fresh DATA overtook reserved recovery slot: %+v %v", result, err)
	}
	close(release)
	if !waitFor(func() bool {
		messages, bytes := recoveryBudgetUsage(l.as.recoveryBudget)
		return messages == 0 && bytes == 0
	}, time.Second) {
		t.Fatal("policy wait lost recovery budget ownership")
	}
	data := sent.data()
	if len(data) != 1 || string(dataPayload(t, data[0])) != "fresh" {
		t.Fatalf("expired DATA revived or fresh DATA lost: %d submissions", len(data))
	}
}
