// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestActiveFIFOTransientFailureRetriesAndRetainsGlobalBudget(t *testing.T) {
	firstData := distributionData(1, 1, "first")
	secondData := distributionData(1, 1, "second")
	totalBytes := firstData.MarshalLen() + secondData.MarshalLen()
	listener, applicationServer, asp, sent := distributionFixtureConfigured(t, params.TrafficModeLoadshare, func(config *Config) {
		config.RecoveryQueueMessages = 2
		config.RecoveryQueueBytes = totalBytes
		config.RecoveryQueueTotalMessages = 2
		config.RecoveryQueueTotalBytes = totalBytes
	})

	firstStarted := make(chan struct{})
	firstRelease := make(chan struct{})
	retryStarted := make(chan [2]int, 1)
	retryRelease := make(chan struct{})
	var firstOnce sync.Once
	var retryReleaseOnce sync.Once
	t.Cleanup(func() {
		select {
		case <-firstRelease:
		default:
			close(firstRelease)
		}
		retryReleaseOnce.Do(func() { close(retryRelease) })
	})
	var secondAttempts atomic.Int32
	want := errors.New("transient active FIFO write failure")
	asp.signalWriter = func(message messages.M3UA) (int, error) {
		data, ok := message.(*messages.Data)
		if !ok {
			return sent.write(message)
		}
		protocolData, err := data.ProtocolData.ProtocolData()
		if err != nil {
			return 0, err
		}
		switch string(protocolData.Data) {
		case "first":
			firstOnce.Do(func() { close(firstStarted) })
			<-firstRelease
		case "second":
			if secondAttempts.Add(1) == 1 {
				return 0, want
			}
			messages, bytes := recoveryBudgetUsage(listener.as.recoveryBudget)
			retryStarted <- [2]int{messages, bytes}
			<-retryRelease
		default:
			return 0, fmt.Errorf("unexpected DATA payload %q", protocolData.Data)
		}
		return sent.write(message)
	}
	applicationServer.setASPState(asp, StateAspActive, time.Hour)
	applicationServer.setASPState(asp, StateAspInactive, time.Hour)
	applicationServer.mu.Lock()
	previousGeneration := applicationServer.recoveryGen
	applicationServer.mu.Unlock()
	applicationServer.recoveryExpired(previousGeneration)
	applicationServer.setASPState(asp, StateAspActive, time.Hour)
	sent.reset()

	type distributionAnswer struct {
		result TrafficDistribution
		err    error
	}
	firstDone := make(chan distributionAnswer, 1)
	go func() {
		result, err := listener.DistributeData(firstData)
		firstDone <- distributionAnswer{result: result, err: err}
	}()
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first active DATA did not reach the writer")
	}

	secondResult, err := listener.DistributeData(secondData)
	if err != nil || !secondResult.Queued || secondResult.Delivered != 0 {
		t.Fatalf("second active DATA = %#v, %v; want queued", secondResult, err)
	}
	if messages, bytes := recoveryBudgetUsage(listener.as.recoveryBudget); messages != 2 || bytes != totalBytes {
		t.Fatalf("aggregate budget with active FIFO = %d messages/%d bytes, want 2/%d", messages, bytes, totalBytes)
	}

	close(firstRelease)
	firstAnswer := <-firstDone
	if firstAnswer.err != nil || firstAnswer.result.Delivered != 1 || firstAnswer.result.Queued {
		t.Fatalf("first active DATA = %#v, %v; want delivered", firstAnswer.result, firstAnswer.err)
	}

	var retained [2]int
	select {
	case retained = <-retryStarted:
	case <-time.After(500 * time.Millisecond):
		messages, bytes := recoveryBudgetUsage(listener.as.recoveryBudget)
		t.Fatalf("active FIFO DATA was not retried after %d attempt(s); aggregate budget = %d messages/%d bytes",
			secondAttempts.Load(), messages, bytes)
	}
	if retained[0] != 1 || retained[1] != secondData.MarshalLen() {
		t.Fatalf("aggregate budget during retry = %d messages/%d bytes, want 1/%d",
			retained[0], retained[1], secondData.MarshalLen())
	}
	retryReleaseOnce.Do(func() { close(retryRelease) })
	if !waitFor(func() bool { return sent.dataCount() == 2 }, time.Second) {
		t.Fatalf("delivered DATA after retry = %d, want 2", sent.dataCount())
	}
	if !waitFor(func() bool {
		messages, bytes := recoveryBudgetUsage(listener.as.recoveryBudget)
		return messages == 0 && bytes == 0
	}, time.Second) {
		messages, bytes := recoveryBudgetUsage(listener.as.recoveryBudget)
		t.Fatalf("aggregate budget after successful retry = %d messages/%d bytes, want zero", messages, bytes)
	}
}

func TestActiveFIFOFailureEnteringPendingIsRetainedBeforeRecoveryExpiry(t *testing.T) {
	firstData := distributionData(1, 1, "first")
	secondData := distributionData(1, 1, "second")
	totalBytes := firstData.MarshalLen() + secondData.MarshalLen()
	listener, applicationServer, asp, sent := distributionFixtureConfigured(t, params.TrafficModeLoadshare, func(config *Config) {
		config.RecoveryQueueMessages = 2
		config.RecoveryQueueBytes = totalBytes
		config.RecoveryQueueTotalMessages = 2
		config.RecoveryQueueTotalBytes = totalBytes
	})

	firstStarted := make(chan struct{})
	firstRelease := make(chan struct{})
	secondStarted := make(chan struct{})
	secondFailureRelease := make(chan struct{})
	var firstOnce sync.Once
	var secondOnce sync.Once
	t.Cleanup(func() {
		firstOnce.Do(func() { close(firstStarted) })
		secondOnce.Do(func() { close(secondStarted) })
		select {
		case <-firstRelease:
		default:
			close(firstRelease)
		}
		select {
		case <-secondFailureRelease:
		default:
			close(secondFailureRelease)
		}
	})
	var secondAttempts atomic.Int32
	want := errors.New("active FIFO write failed while AS-PENDING")
	asp.signalWriter = func(message messages.M3UA) (int, error) {
		data, ok := message.(*messages.Data)
		if !ok {
			return sent.write(message)
		}
		protocolData, err := data.ProtocolData.ProtocolData()
		if err != nil {
			return 0, err
		}
		switch string(protocolData.Data) {
		case "first":
			firstOnce.Do(func() { close(firstStarted) })
			<-firstRelease
		case "second":
			if secondAttempts.Add(1) == 1 {
				secondOnce.Do(func() { close(secondStarted) })
				<-secondFailureRelease
				return 0, want
			}
		default:
			return 0, fmt.Errorf("unexpected DATA payload %q", protocolData.Data)
		}
		return sent.write(message)
	}
	applicationServer.setASPState(asp, StateAspActive, time.Hour)
	sent.reset()

	firstDone := make(chan error, 1)
	go func() {
		_, err := listener.DistributeData(firstData)
		firstDone <- err
	}()
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first active DATA did not reach the writer")
	}
	if result, err := listener.DistributeData(secondData); err != nil || !result.Queued {
		t.Fatalf("second active DATA = %#v, %v; want queued", result, err)
	}
	close(firstRelease)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		t.Fatal("queued active DATA did not begin draining")
	}

	applicationServer.setASPState(asp, StateAspInactive, time.Hour)
	if got := applicationServer.State(); got != ASPending {
		t.Fatalf("AS state = %v, want AS-PENDING", got)
	}
	close(secondFailureRelease)
	if !waitFor(func() bool {
		applicationServer.mu.Lock()
		defer applicationServer.mu.Unlock()
		return len(applicationServer.recoveryQueue) == 1 && !applicationServer.draining
	}, 500*time.Millisecond) {
		applicationServer.mu.Lock()
		queued := len(applicationServer.recoveryQueue)
		applicationServer.mu.Unlock()
		messages, bytes := recoveryBudgetUsage(listener.as.recoveryBudget)
		t.Fatalf("pre-expiry active FIFO failure was not retained: queue=%d budget=%d messages/%d bytes",
			queued, messages, bytes)
	}
	if messages, bytes := recoveryBudgetUsage(listener.as.recoveryBudget); messages != 1 || bytes != secondData.MarshalLen() {
		t.Fatalf("retained pending budget = %d messages/%d bytes, want 1/%d",
			messages, bytes, secondData.MarshalLen())
	}

	applicationServer.setASPState(asp, StateAspActive, time.Hour)
	if !waitFor(func() bool { return sent.dataCount() == 2 }, time.Second) {
		t.Fatalf("reactivated DATA deliveries = %d after %d second attempts, want 2",
			sent.dataCount(), secondAttempts.Load())
	}
	if !waitFor(func() bool {
		messages, bytes := recoveryBudgetUsage(listener.as.recoveryBudget)
		return messages == 0 && bytes == 0
	}, time.Second) {
		messages, bytes := recoveryBudgetUsage(listener.as.recoveryBudget)
		t.Fatalf("aggregate budget after reactivation = %d messages/%d bytes, want zero", messages, bytes)
	}
}

func TestActiveFIFOFailureAfterRecoveryExpiryIsDiscardedAndReleasesGlobalBudget(t *testing.T) {
	firstData := distributionData(1, 1, "first")
	secondData := distributionData(1, 1, "second")
	totalBytes := firstData.MarshalLen() + secondData.MarshalLen()
	listener, applicationServer, asp, sent := distributionFixtureConfigured(t, params.TrafficModeLoadshare, func(config *Config) {
		config.RecoveryQueueMessages = 2
		config.RecoveryQueueBytes = totalBytes
		config.RecoveryQueueTotalMessages = 2
		config.RecoveryQueueTotalBytes = totalBytes
	})

	firstStarted := make(chan struct{})
	firstRelease := make(chan struct{})
	secondStarted := make(chan struct{})
	secondFailureRelease := make(chan struct{})
	var firstOnce sync.Once
	var secondOnce sync.Once
	t.Cleanup(func() {
		select {
		case <-firstRelease:
		default:
			close(firstRelease)
		}
		select {
		case <-secondFailureRelease:
		default:
			close(secondFailureRelease)
		}
	})
	var secondAttempts atomic.Int32
	want := errors.New("active FIFO write failed after T(r)")
	asp.signalWriter = func(message messages.M3UA) (int, error) {
		data, ok := message.(*messages.Data)
		if !ok {
			return sent.write(message)
		}
		protocolData, err := data.ProtocolData.ProtocolData()
		if err != nil {
			return 0, err
		}
		switch string(protocolData.Data) {
		case "first":
			firstOnce.Do(func() { close(firstStarted) })
			<-firstRelease
		case "second":
			secondAttempts.Add(1)
			secondOnce.Do(func() { close(secondStarted) })
			<-secondFailureRelease
			return 0, want
		default:
			return 0, fmt.Errorf("unexpected DATA payload %q", protocolData.Data)
		}
		return sent.write(message)
	}
	applicationServer.setASPState(asp, StateAspActive, time.Hour)
	sent.reset()

	firstDone := make(chan error, 1)
	go func() {
		_, err := listener.DistributeData(firstData)
		firstDone <- err
	}()
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first active DATA did not reach the writer")
	}
	if result, err := listener.DistributeData(secondData); err != nil || !result.Queued {
		t.Fatalf("second active DATA = %#v, %v; want queued", result, err)
	}
	close(firstRelease)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	select {
	case <-secondStarted:
	case <-time.After(time.Second):
		t.Fatal("queued active DATA did not begin draining")
	}

	applicationServer.setASPState(asp, StateAspInactive, time.Hour)
	applicationServer.mu.Lock()
	recoveryGeneration := applicationServer.recoveryGen
	applicationServer.mu.Unlock()
	applicationServer.recoveryExpired(recoveryGeneration)
	if got := applicationServer.State(); got != ASInactive {
		t.Fatalf("AS state after T(r) = %v, want AS-INACTIVE", got)
	}
	if messages, bytes := recoveryBudgetUsage(listener.as.recoveryBudget); messages != 1 || bytes != secondData.MarshalLen() {
		t.Fatalf("in-flight budget at T(r) expiry = %d messages/%d bytes, want 1/%d",
			messages, bytes, secondData.MarshalLen())
	}

	applicationServer.setASPState(asp, StateAspActive, time.Hour)
	close(secondFailureRelease)
	if !waitFor(func() bool {
		messages, bytes := recoveryBudgetUsage(listener.as.recoveryBudget)
		return messages == 0 && bytes == 0
	}, time.Second) {
		messages, bytes := recoveryBudgetUsage(listener.as.recoveryBudget)
		t.Fatalf("aggregate budget after expired write failed = %d messages/%d bytes, want zero", messages, bytes)
	}
	time.Sleep(2 * initialRecoveryDrainRetry)
	if got := secondAttempts.Load(); got != 1 {
		t.Fatalf("expired active FIFO DATA attempts = %d, want no retry", got)
	}
	if got := sent.dataCount(); got != 1 {
		t.Fatalf("delivered DATA after T(r) expiry = %d, want only first DATA", got)
	}
	applicationServer.mu.Lock()
	queued := len(applicationServer.recoveryQueue)
	queuedBytes := applicationServer.recoveryQueueBytes
	inFlightBytes := applicationServer.deliveryInFlightBytes
	applicationServer.mu.Unlock()
	if queued != 0 || queuedBytes != 0 || inFlightBytes != 0 {
		t.Fatalf("expired active FIFO retained state: queue=%d bytes=%d in-flight=%d",
			queued, queuedBytes, inFlightBytes)
	}
}
