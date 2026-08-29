// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestBroadcastDistributionTagsFirstDataAfterEveryActivation(t *testing.T) {
	listener, applicationServer, first, firstSent := distributionFixture(t, params.TrafficModeBroadcast)
	applicationServer.setASPState(first, StateASPActive, time.Hour)

	firstResult, err := listener.DistributeData(distributionData(1, 3, "first epoch"))
	if err != nil {
		t.Fatal(err)
	}
	if firstResult.Delivered != 1 || firstResult.Queued {
		t.Fatalf("first distribution result = %#v, want one immediate delivery", firstResult)
	}
	firstCorrelation := onlyData(t, firstSent.data()).CorrelationID
	if firstCorrelation == nil {
		t.Fatal("first DATA after the first Broadcast ASP became active had no Correlation ID")
	}

	firstSent.reset()
	if _, err := listener.DistributeData(distributionData(1, 3, "ordinary")); err != nil {
		t.Fatal(err)
	}
	if correlation := onlyData(t, firstSent.data()).CorrelationID; correlation != nil {
		t.Fatalf("ordinary Broadcast DATA carried Correlation ID %d; only the first after activation is tagged",
			correlation.CorrelationID())
	}

	second, secondSent := addDistributionASP(t, listener, StateASPInactive, 1)
	applicationServer.setASPState(second, StateASPActive, time.Hour)
	firstSent.reset()

	secondResult, err := listener.DistributeData(distributionData(1, 3, "second epoch"))
	if err != nil {
		t.Fatal(err)
	}
	if secondResult.Delivered != 2 || secondResult.Queued {
		t.Fatalf("second distribution result = %#v, want two immediate deliveries", secondResult)
	}
	firstCopy := onlyData(t, firstSent.data())
	secondCopy := onlyData(t, secondSent.data())
	if firstCopy.CorrelationID == nil || secondCopy.CorrelationID == nil {
		t.Fatal("first DATA after the second ASP activated was not tagged for every Broadcast recipient")
	}
	if got, want := secondCopy.CorrelationID.CorrelationID(), firstCopy.CorrelationID.CorrelationID(); got != want {
		t.Fatalf("Broadcast copies used Correlation IDs %d and %d for the same MSU", want, got)
	}
	if got := firstCopy.CorrelationID.CorrelationID(); got == firstCorrelation.CorrelationID() {
		t.Fatalf("two activation epochs reused Correlation ID %d", got)
	}
}

func TestBroadcastDistributionTagsEveryRoutingLabelFlow(t *testing.T) {
	listener, applicationServer, asp, sent := distributionFixture(t, params.TrafficModeBroadcast)
	applicationServer.setASPState(asp, StateASPActive, time.Hour)
	sent.reset()

	first := distributionData(1, 9, "first label")
	second := distributionData(1, 9, "second label")
	second.ProtocolData = params.NewProtocolData(10, 20, params.ServiceIndSCCP, 0, 0, 9, []byte("second label"))
	for _, data := range []*messages.Data{first, second} {
		if _, err := listener.DistributeData(data); err != nil {
			t.Fatal(err)
		}
	}

	delivered := sent.data()
	if len(delivered) != 2 {
		t.Fatalf("delivered %d DATA messages, want 2", len(delivered))
	}
	for index, data := range delivered {
		if data.CorrelationID == nil {
			t.Fatalf("first DATA in routing-label flow %d had no Correlation ID", index)
		}
	}
	if firstID, secondID := delivered[0].CorrelationID.CorrelationID(), delivered[1].CorrelationID.CorrelationID(); firstID == secondID {
		t.Fatalf("distinct traffic-flow boundaries reused Correlation ID %d", firstID)
	}
}

func TestBroadcastActivationDropsThePreviousFlowEpoch(t *testing.T) {
	listener, applicationServer, first, _ := distributionFixture(t, params.TrafficModeBroadcast)
	applicationServer.setASPState(first, StateASPActive, time.Hour)
	if _, err := listener.DistributeData(distributionData(1, 1, "old epoch")); err != nil {
		t.Fatal(err)
	}
	applicationServer.mu.Lock()
	before := len(applicationServer.broadcastTagged)
	applicationServer.mu.Unlock()
	if before != 1 {
		t.Fatalf("old epoch flow entries = %d, want 1", before)
	}

	second, _ := addDistributionASP(t, listener, StateASPInactive, 1)
	applicationServer.setASPState(second, StateASPActive, time.Hour)
	applicationServer.mu.Lock()
	after := len(applicationServer.broadcastTagged)
	applicationServer.mu.Unlock()
	if after != 0 {
		t.Fatalf("new Broadcast activation retained %d obsolete flow entries, want 0", after)
	}
}

func TestBroadcastFlowCacheIsBoundedAndEvictionRetags(t *testing.T) {
	listener, applicationServer, asp, sent := distributionFixtureConfigured(t, params.TrafficModeBroadcast, func(config *AssociationConfig) {
		config.BroadcastFlowCacheEntries = 2
	})
	applicationServer.setASPState(asp, StateASPActive, time.Hour)
	sent.reset()

	for sls := uint8(1); sls <= 3; sls++ {
		if _, err := listener.DistributeData(distributionData(1, sls, fmt.Sprintf("flow %d", sls))); err != nil {
			t.Fatal(err)
		}
	}
	applicationServer.mu.Lock()
	entries := len(applicationServer.broadcastTagged)
	applicationServer.mu.Unlock()
	if entries != 1 {
		t.Fatalf("flow cache entries after three distinct flows with cap 2 = %d, want 1", entries)
	}

	sent.reset()
	if _, err := listener.DistributeData(distributionData(1, 1, "evicted flow")); err != nil {
		t.Fatal(err)
	}
	if correlation := onlyData(t, sent.data()).CorrelationID; correlation == nil {
		t.Fatal("evicted Broadcast flow was not conservatively tagged again")
	}
}

func TestBroadcastFlowIdentifierLengthIsBounded(t *testing.T) {
	listener, applicationServer, asp, sent := distributionFixtureConfigured(t, params.TrafficModeBroadcast, func(config *AssociationConfig) {
		config.BroadcastFlowIdentifierBytes = 4
		config.BroadcastFlowIdentifier = func(*params.ProtocolDataPayload) (string, error) {
			return "12345", nil
		}
	})
	applicationServer.setASPState(asp, StateASPActive, time.Hour)
	sent.reset()

	if _, err := listener.DistributeData(distributionData(1, 1, "oversized key")); !errors.Is(err, ErrBroadcastFlowIdentifierTooLong) {
		t.Fatalf("oversized flow identifier error = %v, want ErrBroadcastFlowIdentifierTooLong", err)
	}
	if sent.dataCount() != 0 {
		t.Fatalf("oversized flow identifier delivered %d DATA messages", sent.dataCount())
	}
}

func TestBroadcastPartialWriteKeepsSynchronizationPending(t *testing.T) {
	listener, applicationServer, first, firstSent := distributionFixture(t, params.TrafficModeBroadcast)
	second, secondSent := addDistributionASP(t, listener, StateASPInactive, 1)
	applicationServer.setASPState(first, StateASPActive, time.Hour)
	applicationServer.setASPState(second, StateASPActive, time.Hour)
	firstSent.reset()
	secondSent.reset()

	want := errors.New("second ASP write failed")
	failed := false
	second.signalWriter = func(message messages.M3UA) (int, error) {
		if _, ok := message.(*messages.Data); ok && !failed {
			failed = true
			return 0, want
		}
		return secondSent.write(message)
	}
	result, err := listener.DistributeData(distributionData(1, 4, "partial"))
	if !errors.Is(err, want) {
		t.Fatalf("partial Broadcast error = %v, want %v", err, want)
	}
	if result.Delivered != 1 {
		t.Fatalf("partial Broadcast result = %#v, want one delivery", result)
	}
	partial := onlyData(t, firstSent.data())
	if partial.CorrelationID == nil {
		t.Fatal("partial Broadcast's successful copy had no Correlation ID")
	}

	firstSent.reset()
	if _, err := listener.DistributeData(distributionData(1, 4, "retry boundary")); err != nil {
		t.Fatal(err)
	}
	firstRetry := onlyData(t, firstSent.data())
	secondRetry := onlyData(t, secondSent.data())
	if firstRetry.CorrelationID == nil || secondRetry.CorrelationID == nil {
		t.Fatal("partial write incorrectly retired the synchronization boundary")
	}
	if got, want := secondRetry.CorrelationID.CorrelationID(), firstRetry.CorrelationID.CorrelationID(); got != want {
		t.Fatalf("retry copies used Correlation IDs %d and %d", want, got)
	}
	if got := firstRetry.CorrelationID.CorrelationID(); got == partial.CorrelationID.CorrelationID() {
		t.Fatalf("retry boundary reused attempted Correlation ID %d", got)
	}
}

func TestBroadcastFlowIdentifierRefinesRoutingLabelAndOwnsInput(t *testing.T) {
	listener, applicationServer, asp, sent := distributionFixtureConfigured(t, params.TrafficModeBroadcast, func(config *AssociationConfig) {
		config.BroadcastFlowIdentifier = func(protocolData *params.ProtocolDataPayload) (string, error) {
			identifier := fmt.Sprintf("circuit-%d", protocolData.Data[0])
			protocolData.Data[0] = 0xff
			return identifier, nil
		}
	})
	applicationServer.setASPState(asp, StateASPActive, time.Hour)
	sent.reset()

	for circuit := byte(1); circuit <= 2; circuit++ {
		data := distributionData(1, 6, string([]byte{circuit, 0xaa}))
		if _, err := listener.DistributeData(data); err != nil {
			t.Fatal(err)
		}
	}
	delivered := sent.data()
	if len(delivered) != 2 {
		t.Fatalf("delivered %d DATA messages, want 2", len(delivered))
	}
	for index, data := range delivered {
		if data.CorrelationID == nil {
			t.Fatalf("application flow %d had no Correlation ID", index)
		}
		if got := dataPayload(t, data)[0]; got != byte(index+1) {
			t.Fatalf("classifier mutated delivered payload to %#x", got)
		}
	}
}

func TestBroadcastFlowIdentifierOnlyRunsForBroadcast(t *testing.T) {
	want := errors.New("classifier should not run")
	listener, applicationServer, asp, sent := distributionFixtureConfigured(t, params.TrafficModeLoadshare, func(config *AssociationConfig) {
		config.BroadcastFlowIdentifier = func(*params.ProtocolDataPayload) (string, error) {
			return "", want
		}
	})
	applicationServer.setASPState(asp, StateASPActive, time.Hour)
	sent.reset()

	if _, err := listener.DistributeData(distributionData(1, 1, "loadshare")); err != nil {
		t.Fatalf("Loadshare distribution invoked Broadcast classifier: %v", err)
	}
	if sent.dataCount() != 1 {
		t.Fatalf("Loadshare delivered %d DATA messages, want 1", sent.dataCount())
	}
}

func TestBroadcastFlowIdentifierFailuresDoNotDeliver(t *testing.T) {
	for _, test := range []struct {
		name       string
		classifier BroadcastFlowIdentifier
	}{
		{
			name: "error",
			classifier: func(*params.ProtocolDataPayload) (string, error) {
				return "", errors.New("cannot classify")
			},
		},
		{
			name: "panic",
			classifier: func(*params.ProtocolDataPayload) (string, error) {
				panic("bad classifier")
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			listener, applicationServer, asp, sent := distributionFixtureConfigured(t, params.TrafficModeBroadcast, func(config *AssociationConfig) {
				config.BroadcastFlowIdentifier = test.classifier
			})
			applicationServer.setASPState(asp, StateASPActive, time.Hour)
			sent.reset()
			if _, err := listener.DistributeData(distributionData(1, 1, "payload")); err == nil {
				t.Fatal("classifier failure returned nil error")
			}
			if sent.dataCount() != 0 {
				t.Fatalf("classifier failure delivered %d DATA messages", sent.dataCount())
			}
		})
	}
}

func TestRecoveryQueuePolicyIsSnapshotted(t *testing.T) {
	listener, applicationServer, asp, _ := distributionFixtureConfigured(t, params.TrafficModeLoadshare, func(config *AssociationConfig) {
		config.RecoveryQueueMessages = 1
	})
	listener.AssociationConfig.RecoveryQueueMessages = 100
	applicationServer.setASPState(asp, StateASPActive, time.Hour)
	applicationServer.setASPState(asp, StateASPInactive, time.Hour)
	if _, err := listener.DistributeData(distributionData(1, 1, "first")); err != nil {
		t.Fatal(err)
	}
	if _, err := listener.DistributeData(distributionData(1, 1, "second")); !errors.Is(err, ErrRecoveryQueueFull) {
		t.Fatalf("mutated Config changed live queue policy: %v", err)
	}
}

func TestPendingApplicationServerQueuesAndFlushesInOrder(t *testing.T) {
	listener, applicationServer, asp, sent := distributionFixture(t, params.TrafficModeLoadshare)
	applicationServer.setASPState(asp, StateASPActive, time.Hour)
	applicationServer.setASPState(asp, StateASPInactive, time.Hour)
	if got := applicationServer.State(); got != ASPending {
		t.Fatalf("AS state = %v, want AS-PENDING", got)
	}
	sent.reset()

	for _, payload := range []string{"one", "two", "three"} {
		result, err := listener.DistributeData(distributionData(1, 5, payload))
		if err != nil {
			t.Fatalf("queue %q: %v", payload, err)
		}
		if !result.Queued || result.Delivered != 0 {
			t.Fatalf("queue %q result = %#v, want queued", payload, result)
		}
	}
	if sent.dataCount() != 0 {
		t.Fatalf("AS-PENDING traffic was sent immediately: %d messages", sent.dataCount())
	}

	applicationServer.setASPState(asp, StateASPActive, time.Hour)
	if !waitFor(func() bool { return sent.dataCount() == 3 }, time.Second) {
		t.Fatalf("queued DATA count after recovery = %d, want 3", sent.dataCount())
	}
	flushed := sent.data()
	for index, want := range []string{"one", "two", "three"} {
		got := dataPayload(t, flushed[index])
		if string(got) != want {
			t.Fatalf("flushed payload %d = %q, want %q", index, got, want)
		}
	}
}

func TestRecoveryDrainIsAsyncAndQueuesNewTrafficBehindBacklog(t *testing.T) {
	listener, applicationServer, asp, sent := distributionFixture(t, params.TrafficModeLoadshare)
	applicationServer.setASPState(asp, StateASPActive, time.Hour)
	applicationServer.setASPState(asp, StateASPInactive, time.Hour)
	sent.reset()
	for _, payload := range []string{"old one", "old two"} {
		if _, err := listener.DistributeData(distributionData(1, 2, payload)); err != nil {
			t.Fatal(err)
		}
	}

	writeEntered := make(chan struct{})
	writeRelease := make(chan struct{})
	var enterOnce sync.Once
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(writeRelease) }) }
	t.Cleanup(release)
	asp.signalWriter = func(message messages.M3UA) (int, error) {
		if _, ok := message.(*messages.Data); ok {
			enterOnce.Do(func() { close(writeEntered) })
			<-writeRelease
		}
		return sent.write(message)
	}

	activationDone := make(chan struct{})
	go func() {
		applicationServer.setASPState(asp, StateASPActive, time.Hour)
		close(activationDone)
	}()
	select {
	case <-writeEntered:
	case <-time.After(time.Second):
		t.Fatal("recovery drain never attempted its first DATA")
	}
	select {
	case <-activationDone:
	case <-time.After(100 * time.Millisecond):
		t.Error("ASP activation blocked on recovery DATA write")
	}

	type distributionAnswer struct {
		result TrafficDistribution
		err    error
	}
	answer := make(chan distributionAnswer, 1)
	go func() {
		result, err := listener.DistributeData(distributionData(1, 2, "new"))
		answer <- distributionAnswer{result: result, err: err}
	}()
	var newTraffic distributionAnswer
	select {
	case newTraffic = <-answer:
	case <-time.After(100 * time.Millisecond):
		t.Error("new DATA blocked behind the recovery writer instead of joining the drain")
	}

	release()
	select {
	case <-activationDone:
	case <-time.After(time.Second):
		t.Fatal("activation did not complete after releasing DATA writes")
	}
	if newTraffic.result == (TrafficDistribution{}) && newTraffic.err == nil {
		select {
		case newTraffic = <-answer:
		case <-time.After(time.Second):
			t.Fatal("new DATA distribution did not complete")
		}
	}
	if newTraffic.err != nil {
		t.Fatal(newTraffic.err)
	}
	if !newTraffic.result.Queued || newTraffic.result.Delivered != 0 {
		t.Errorf("new DATA result = %#v, want queued behind recovery backlog", newTraffic.result)
	}
	if !waitFor(func() bool { return sent.dataCount() == 3 }, time.Second) {
		t.Fatalf("drained DATA count = %d, want 3", sent.dataCount())
	}
	for index, want := range []string{"old one", "old two", "new"} {
		if got := string(dataPayload(t, sent.data()[index])); got != want {
			t.Fatalf("drained payload %d = %q, want %q", index, got, want)
		}
	}
}

func TestBlockedActiveDeliveryUsesTheBoundedFIFO(t *testing.T) {
	message := distributionData(1, 2, "same size")
	listener, applicationServer, asp, sent := distributionFixtureConfigured(t, params.TrafficModeLoadshare, func(config *AssociationConfig) {
		config.RecoveryQueueMessages = 2
		config.RecoveryQueueBytes = 2 * message.MarshalLen()
	})
	applicationServer.setASPState(asp, StateASPActive, time.Hour)
	sent.reset()

	writeEntered := make(chan struct{})
	writeRelease := make(chan struct{})
	var enterOnce sync.Once
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(writeRelease) }) }
	t.Cleanup(release)
	asp.signalWriter = func(message messages.M3UA) (int, error) {
		if _, ok := message.(*messages.Data); ok {
			enterOnce.Do(func() { close(writeEntered) })
			<-writeRelease
		}
		return sent.write(message)
	}

	firstDone := make(chan error, 1)
	go func() {
		_, err := listener.DistributeData(distributionData(1, 2, "same size"))
		firstDone <- err
	}()
	select {
	case <-writeEntered:
	case <-time.After(time.Second):
		t.Fatal("active DATA write did not block in the test writer")
	}

	type answer struct {
		result TrafficDistribution
		err    error
	}
	distribute := func() answer {
		completed := make(chan answer, 1)
		go func() {
			result, err := listener.DistributeData(distributionData(1, 2, "same size"))
			completed <- answer{result: result, err: err}
		}()
		select {
		case got := <-completed:
			return got
		case <-time.After(100 * time.Millisecond):
			t.Fatal("DATA blocked behind an active writer instead of using bounded admission")
			return answer{}
		}
	}

	second := distribute()
	if second.err != nil || !second.result.Queued || second.result.Delivered != 0 {
		t.Fatalf("second DATA = %#v, %v, want queued", second.result, second.err)
	}
	third := distribute()
	if !errors.Is(third.err, ErrRecoveryQueueFull) {
		t.Fatalf("third DATA error = %v, want ErrRecoveryQueueFull", third.err)
	}

	applicationServer.mu.Lock()
	retainedMessages := len(applicationServer.recoveryQueue)
	if applicationServer.deliveryInFlightBytes > 0 {
		retainedMessages++
	}
	retainedBytes := applicationServer.recoveryQueueBytes
	applicationServer.mu.Unlock()
	if retainedMessages != 2 || retainedBytes != 2*message.MarshalLen() {
		t.Fatalf("retained active backlog = %d messages/%d bytes, want 2/%d",
			retainedMessages, retainedBytes, 2*message.MarshalLen())
	}

	release()
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if !waitFor(func() bool { return sent.dataCount() == 2 }, time.Second) {
		t.Fatalf("delivered DATA count = %d, want 2", sent.dataCount())
	}
}

func TestRecoveryQueueBoundsIncludeBlockedInFlightData(t *testing.T) {
	message := distributionData(1, 2, "same size")
	listener, applicationServer, asp, sent := distributionFixtureConfigured(t, params.TrafficModeLoadshare, func(config *AssociationConfig) {
		config.RecoveryQueueMessages = 2
		config.RecoveryQueueBytes = 2 * message.MarshalLen()
	})
	applicationServer.setASPState(asp, StateASPActive, time.Hour)
	applicationServer.setASPState(asp, StateASPInactive, time.Hour)
	for range 2 {
		if _, err := listener.DistributeData(distributionData(1, 2, "same size")); err != nil {
			t.Fatal(err)
		}
	}

	writeEntered := make(chan struct{})
	writeRelease := make(chan struct{})
	var once sync.Once
	asp.signalWriter = func(message messages.M3UA) (int, error) {
		if _, ok := message.(*messages.Data); ok {
			once.Do(func() { close(writeEntered) })
			<-writeRelease
		}
		return sent.write(message)
	}
	applicationServer.setASPState(asp, StateASPActive, time.Hour)
	select {
	case <-writeEntered:
	case <-time.After(time.Second):
		close(writeRelease)
		t.Fatal("drain did not begin")
	}
	if _, err := listener.DistributeData(distributionData(1, 2, "same size")); !errors.Is(err, ErrRecoveryQueueFull) {
		close(writeRelease)
		t.Fatalf("blocked in-flight DATA was omitted from queue bounds: %v", err)
	}
	close(writeRelease)
	if !waitFor(func() bool { return sent.dataCount() == 2 }, time.Second) {
		t.Fatalf("drained DATA count = %d, want 2", sent.dataCount())
	}
}

func TestExpiredInFlightRecoveryDataIsNeverResurrected(t *testing.T) {
	listener, applicationServer, asp, sent := distributionFixture(t, params.TrafficModeLoadshare)
	applicationServer.setASPState(asp, StateASPActive, 20*time.Millisecond)
	applicationServer.setASPState(asp, StateASPInactive, 20*time.Millisecond)
	if _, err := listener.DistributeData(distributionData(1, 2, "must expire")); err != nil {
		t.Fatal(err)
	}

	writeEntered := make(chan struct{})
	writeRelease := make(chan struct{})
	var once sync.Once
	want := errors.New("blocked recovery write failed")
	asp.signalWriter = func(message messages.M3UA) (int, error) {
		if _, ok := message.(*messages.Data); ok {
			once.Do(func() { close(writeEntered) })
			<-writeRelease
			return 0, want
		}
		return sent.write(message)
	}
	applicationServer.setASPState(asp, StateASPActive, 20*time.Millisecond)
	select {
	case <-writeEntered:
	case <-time.After(time.Second):
		close(writeRelease)
		t.Fatal("drain did not begin")
	}

	applicationServer.setASPState(asp, StateASPInactive, 20*time.Millisecond)
	if !waitFor(func() bool { return applicationServer.State() == ASInactive }, time.Second) {
		close(writeRelease)
		t.Fatalf("AS state after T(r) = %v, want AS-INACTIVE", applicationServer.State())
	}
	applicationServer.setASPState(asp, StateASPActive, time.Hour)
	close(writeRelease)
	time.Sleep(25 * time.Millisecond)

	applicationServer.mu.Lock()
	queued, queuedBytes := len(applicationServer.recoveryQueue), applicationServer.recoveryQueueBytes
	applicationServer.mu.Unlock()
	if queued != 0 || queuedBytes != 0 {
		t.Fatalf("expired in-flight DATA was resurrected: messages=%d bytes=%d", queued, queuedBytes)
	}
}

func TestRecoveryDrainRetriesWithoutNewTraffic(t *testing.T) {
	listener, applicationServer, asp, sent := distributionFixture(t, params.TrafficModeLoadshare)
	applicationServer.setASPState(asp, StateASPActive, time.Hour)
	applicationServer.setASPState(asp, StateASPInactive, time.Hour)
	if _, err := listener.DistributeData(distributionData(1, 2, "retry me")); err != nil {
		t.Fatal(err)
	}

	firstAttempt := make(chan struct{})
	var attempts atomic.Int32
	want := errors.New("transient recovery write failure")
	asp.signalWriter = func(message messages.M3UA) (int, error) {
		if _, ok := message.(*messages.Data); ok && attempts.Add(1) == 1 {
			close(firstAttempt)
			return 0, want
		}
		return sent.write(message)
	}
	applicationServer.setASPState(asp, StateASPActive, time.Hour)
	select {
	case <-firstAttempt:
	case <-time.After(time.Second):
		t.Fatal("drain never made its first attempt")
	}
	if !waitFor(func() bool { return sent.dataCount() == 1 }, time.Second) {
		t.Fatalf("retained DATA was stranded after %d attempts", attempts.Load())
	}
	if attempts.Load() < 2 {
		t.Fatalf("drain attempts = %d, want retry", attempts.Load())
	}
}

func TestBroadcastRecoveryRetriesOnlyFailedRecipients(t *testing.T) {
	listener, applicationServer, first, firstSent := distributionFixture(t, params.TrafficModeBroadcast)
	second, secondSent := addDistributionASP(t, listener, StateASPInactive, 1)
	applicationServer.setASPState(first, StateASPActive, time.Hour)
	applicationServer.setASPState(second, StateASPActive, time.Hour)
	applicationServer.setASPState(first, StateASPInactive, time.Hour)
	applicationServer.setASPState(second, StateASPInactive, time.Hour)
	firstSent.reset()
	secondSent.reset()
	if _, err := listener.DistributeData(distributionData(1, 8, "recover once")); err != nil {
		t.Fatal(err)
	}

	want := errors.New("transient second-recipient failure")
	var secondAttempts atomic.Int32
	second.signalWriter = func(message messages.M3UA) (int, error) {
		if _, ok := message.(*messages.Data); ok && secondAttempts.Add(1) == 1 {
			return 0, want
		}
		return secondSent.write(message)
	}

	// Hold the delivery lock until both ASPs are active, otherwise the first
	// activation may legitimately start draining before the second joins.
	applicationServer.deliveryMu.Lock()
	applicationServer.setASPState(first, StateASPActive, time.Hour)
	applicationServer.setASPState(second, StateASPActive, time.Hour)
	applicationServer.deliveryMu.Unlock()

	if !waitFor(func() bool { return secondSent.dataCount() == 1 }, time.Second) {
		t.Fatalf("failed Broadcast recipient was attempted %d times and delivered %d copies, want an automatic retry",
			secondAttempts.Load(), secondSent.dataCount())
	}
	if got := firstSent.dataCount(); got != 1 {
		t.Fatalf("successful Broadcast recipient received %d copies, want exactly 1", got)
	}
	firstCopy := onlyData(t, firstSent.data())
	secondCopy := onlyData(t, secondSent.data())
	if firstCopy.CorrelationID == nil || secondCopy.CorrelationID == nil {
		t.Fatal("recovered Broadcast copies did not carry the activation Correlation ID")
	}
	if got, want := secondCopy.CorrelationID.CorrelationID(), firstCopy.CorrelationID.CorrelationID(); got != want {
		t.Fatalf("retried Broadcast copy Correlation ID = %d, want %d", got, want)
	}
}

func TestRecoveryExpiryDiscardsQueuedTraffic(t *testing.T) {
	listener, applicationServer, asp, sent := distributionFixture(t, params.TrafficModeLoadshare)
	applicationServer.setASPState(asp, StateASPActive, 30*time.Millisecond)
	applicationServer.setASPState(asp, StateASPInactive, 30*time.Millisecond)
	if _, err := listener.DistributeData(distributionData(1, 1, "discard me")); err != nil {
		t.Fatal(err)
	}
	if !waitFor(func() bool { return applicationServer.State() == ASInactive }, time.Second) {
		t.Fatalf("AS state after T(r) = %v, want AS-INACTIVE", applicationServer.State())
	}

	applicationServer.setASPState(asp, StateASPActive, time.Hour)
	time.Sleep(25 * time.Millisecond)
	if sent.dataCount() != 0 {
		t.Fatalf("T(r)-expired queue flushed %d stale messages", sent.dataCount())
	}
}

func TestRecoveryQueueIsBoundedAndOwnsMessages(t *testing.T) {
	first := distributionData(1, 1, "owned")
	second := distributionData(1, 1, "second")
	listener, applicationServer, asp, sent := distributionFixtureConfigured(t, params.TrafficModeLoadshare, func(config *AssociationConfig) {
		config.RecoveryQueueMessages = 2
		config.RecoveryQueueBytes = first.MarshalLen() + second.MarshalLen()
	})
	applicationServer.setASPState(asp, StateASPActive, time.Hour)
	applicationServer.setASPState(asp, StateASPInactive, time.Hour)

	if _, err := listener.DistributeData(first); err != nil {
		t.Fatal(err)
	}
	for index := range first.ProtocolData.Data {
		first.ProtocolData.Data[index] = 0xff
	}
	if _, err := listener.DistributeData(second); err != nil {
		t.Fatal(err)
	}
	if _, err := listener.DistributeData(distributionData(1, 1, "third")); !errors.Is(err, ErrRecoveryQueueFull) {
		t.Fatalf("third queued message error = %v, want ErrRecoveryQueueFull", err)
	}

	applicationServer.setASPState(asp, StateASPActive, time.Hour)
	if !waitFor(func() bool { return sent.dataCount() == 2 }, time.Second) {
		t.Fatalf("flushed DATA count = %d, want 2", sent.dataCount())
	}
	if got := dataPayload(t, sent.data()[0]); !bytes.Equal(got, []byte("owned")) {
		t.Fatalf("queued DATA aliased caller memory: % x", got)
	}
}

func TestListenerCloseStopsDistributionAndReleasesRecoveryState(t *testing.T) {
	var classifierCalls atomic.Int32
	listener, applicationServer, asp, _ := distributionFixtureConfigured(t, params.TrafficModeBroadcast, func(config *AssociationConfig) {
		config.BroadcastFlowIdentifier = func(*params.ProtocolDataPayload) (string, error) {
			classifierCalls.Add(1)
			return "flow", nil
		}
	})
	applicationServer.setASPState(asp, StateASPActive, time.Hour)
	applicationServer.setASPState(asp, StateASPInactive, time.Hour)
	if _, err := listener.DistributeData(distributionData(1, 1, "queued")); err != nil {
		t.Fatal(err)
	}
	classifierCalls.Store(0)

	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := listener.DistributeData(distributionData(1, 1, "after close")); !errors.Is(err, ErrAssociationClosed) {
		t.Fatalf("distribution after Listener.Close error = %v, want ErrAssociationClosed", err)
	}
	if got := classifierCalls.Load(); got != 0 {
		t.Fatalf("closed Listener invoked BroadcastFlowIdentifier %d times", got)
	}

	applicationServer.mu.Lock()
	closed := applicationServer.closed
	queued := len(applicationServer.recoveryQueue)
	queuedBytes := applicationServer.recoveryQueueBytes
	recovery := applicationServer.recovery
	retry := applicationServer.drainRetry
	applicationServer.mu.Unlock()
	if !closed || queued != 0 || queuedBytes != 0 || recovery != nil || retry != nil {
		t.Fatalf("closed AS retained state: closed=%t messages=%d bytes=%d recovery=%v retry=%v",
			closed, queued, queuedBytes, recovery, retry)
	}
}

func TestDistributionLoadsharesBySLSAndRejectsInactiveAS(t *testing.T) {
	listener, applicationServer, first, firstSent := distributionFixture(t, params.TrafficModeLoadshare)
	second, secondSent := addDistributionASP(t, listener, StateASPInactive, 1)
	applicationServer.setASPState(first, StateASPActive, time.Hour)
	applicationServer.setASPState(second, StateASPActive, time.Hour)

	for range 10 {
		result, err := listener.DistributeData(distributionData(1, 7, "same flow"))
		if err != nil {
			t.Fatal(err)
		}
		if result.Delivered != 1 {
			t.Fatalf("Loadshare delivered to %d ASPs, want 1", result.Delivered)
		}
	}
	if (firstSent.dataCount() == 0) == (secondSent.dataCount() == 0) {
		t.Fatalf("one SLS did not stay on exactly one ASP: first=%d second=%d", firstSent.dataCount(), secondSent.dataCount())
	}

	applicationServer.setASPState(first, StateASPDown, time.Hour)
	applicationServer.setASPState(second, StateASPDown, time.Hour)
	applicationServer.recoveryExpired(applicationServer.recoveryGen)
	if _, err := listener.DistributeData(distributionData(1, 7, "nobody")); !errors.Is(err, ErrNoActiveASP) {
		t.Fatalf("inactive AS distribution error = %v, want ErrNoActiveASP", err)
	}
}

func TestConcurrentDistributionAndActivationIsRaceFree(t *testing.T) {
	listener, applicationServer, first, _ := distributionFixture(t, params.TrafficModeBroadcast)
	second, _ := addDistributionASP(t, listener, StateASPInactive, 1)
	applicationServer.setASPState(first, StateASPActive, time.Hour)

	var wait sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			for iteration := 0; iteration < 50; iteration++ {
				_, _ = listener.DistributeData(distributionData(1, uint8(worker), "race"))
			}
		}(worker)
	}
	for iteration := 0; iteration < 50; iteration++ {
		applicationServer.setASPState(second, StateASPActive, time.Hour)
		applicationServer.setASPState(second, StateASPInactive, time.Hour)
	}
	wait.Wait()
}

func TestLoadshareTargetSelectionDoesNotRebuildMembership(t *testing.T) {
	applicationServer := &applicationServer{
		asps:        make(map[*Association]State),
		trafficMode: params.TrafficModeLoadshare,
	}
	for range 128 {
		applicationServer.asps[&Association{}] = StateASPActive
	}
	applicationServer.mu.Lock()
	applicationServer.rebuildActiveLocked()
	applicationServer.mu.Unlock()

	allocations := testing.AllocsPerRun(1_000, func() {
		applicationServer.mu.Lock()
		targets, err := applicationServer.targetsLocked(7)
		applicationServer.mu.Unlock()
		if err != nil || len(targets) != 1 {
			panic("invalid Loadshare target selection")
		}
	})
	if allocations != 0 {
		t.Fatalf("steady-state Loadshare target selection allocated %.2f objects per call, want 0", allocations)
	}
}

func TestASPInactiveAckWaitsUntilScopedTrafficIsHalted(t *testing.T) {
	type distributionAnswer struct {
		result TrafficDistribution
		err    error
	}
	listener, applicationServer, asp, sent := distributionFixtureForContexts(t, params.TrafficModeLoadshare, []uint32{1, 2}, nil)
	secondApplicationServer := listener.as.get(2)
	secondApplicationServer.setTrafficMode(params.TrafficModeLoadshare)
	asp.noteRoutingContextsActive([]uint32{1, 2})
	asp.setState(StateASPActive)
	applicationServer.setASPState(asp, StateASPActive, time.Hour)
	secondApplicationServer.setASPState(asp, StateASPActive, time.Hour)
	sent.reset()

	events := make(chan string, 16)
	writeRelease := make(chan struct{})
	var firstContextWrites atomic.Int32
	asp.signalWriter = func(message messages.M3UA) (int, error) {
		switch message := message.(type) {
		case *messages.Data:
			if message.RoutingContext.RoutingContext() == 1 && firstContextWrites.Add(1) == 1 {
				events <- "data-start"
				<-writeRelease
				events <- "data-end"
			}
		case *messages.AspInactiveAck:
			events <- "inactive-ack"
		}
		return sent.write(message)
	}

	firstDone := make(chan error, 1)
	go func() {
		_, err := listener.DistributeData(distributionData(1, 3, "in flight"))
		firstDone <- err
	}()
	select {
	case event := <-events:
		if event != "data-start" {
			t.Fatalf("first event = %q, want data-start", event)
		}
	case <-time.After(time.Second):
		t.Fatal("DATA write did not start")
	}

	inactiveDone := make(chan error, 1)
	go func() {
		inactiveDone <- asp.handleAspInactive(messages.NewAspInactive(params.NewRoutingContext(1), nil))
	}()
	ackBeforeRelease := false
	select {
	case event := <-events:
		if event == "inactive-ack" {
			ackBeforeRelease = true
		}
	case <-time.After(100 * time.Millisecond):
	}

	stateAnswer := make(chan ASState, 1)
	go func() { stateAnswer <- listener.ApplicationServerState(1) }()
	select {
	case state := <-stateAnswer:
		if state != ASPending {
			t.Errorf("AS state during inactive barrier = %v, want AS-PENDING", state)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("ApplicationServerState blocked behind in-flight DATA")
	}
	if got := listener.ApplicationServerState(2); got != ASActive {
		t.Errorf("unaffected RC 2 state = %v, want AS-ACTIVE", got)
	}
	if got := asp.State(); got != StateASPActive {
		t.Errorf("association state = %v, want ASP-ACTIVE while RC 2 remains active", got)
	}
	rc2Answer := make(chan distributionAnswer, 1)
	go func() {
		result, err := listener.DistributeData(distributionData(2, 3, "unaffected"))
		rc2Answer <- distributionAnswer{result: result, err: err}
	}()
	select {
	case answer := <-rc2Answer:
		if answer.err != nil || answer.result.Delivered != 1 || answer.result.Queued {
			t.Errorf("RC 2 distribution = %#v, %v, want one immediate delivery", answer.result, answer.err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("unaffected RC 2 DATA blocked behind RC 1 barrier")
	}

	secondDone := make(chan distributionAnswer, 1)
	go func() {
		result, err := listener.DistributeData(distributionData(1, 3, "after inactive"))
		secondDone <- distributionAnswer{result: result, err: err}
	}()
	var second distributionAnswer
	select {
	case second = <-secondDone:
	case <-time.After(100 * time.Millisecond):
		t.Error("post-inactive DATA blocked instead of joining AS-PENDING queue")
	}

	close(writeRelease)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if second == (distributionAnswer{}) {
		second = <-secondDone
	}
	if second.err != nil {
		t.Fatal(second.err)
	}
	if !second.result.Queued || second.result.Delivered != 0 {
		t.Errorf("post-inactive DATA result = %#v, want queued", second.result)
	}
	if err := <-inactiveDone; err != nil {
		t.Fatal(err)
	}
	if ackBeforeRelease {
		t.Fatal("ASP Inactive Ack was written before in-flight DATA halted")
	}
	firstAfterRelease := <-events
	secondAfterRelease := <-events
	if firstAfterRelease != "data-end" || secondAfterRelease != "inactive-ack" {
		t.Fatalf("post-release events = %q, %q, want data-end then inactive-ack", firstAfterRelease, secondAfterRelease)
	}
	if got := firstContextWrites.Load(); got != 1 {
		t.Fatalf("inactive ASP received %d DATA writes, want only the already in-flight one", got)
	}
}

func TestASStateNotifiesRemainOrderedAcrossInactiveBarrier(t *testing.T) {
	listener, applicationServer, first, firstSent := distributionFixture(t, params.TrafficModeLoadshare)
	second, secondSent := addDistributionASP(t, listener, StateASPInactive, 1)
	applicationServer.setASPState(first, StateASPActive, time.Hour)
	applicationServer.setASPState(second, StateASPInactive, time.Hour)
	first.noteRoutingContextsActive([]uint32{1})
	first.setState(StateASPActive)
	firstSent.reset()
	secondSent.reset()

	writeEntered := make(chan struct{})
	writeRelease := make(chan struct{})
	var once sync.Once
	first.signalWriter = func(message messages.M3UA) (int, error) {
		if _, ok := message.(*messages.Data); ok {
			once.Do(func() { close(writeEntered) })
			<-writeRelease
		}
		return firstSent.write(message)
	}
	dataDone := make(chan error, 1)
	go func() {
		_, err := listener.DistributeData(distributionData(1, 3, "in flight"))
		dataDone <- err
	}()
	select {
	case <-writeEntered:
	case <-time.After(time.Second):
		close(writeRelease)
		t.Fatal("DATA write did not start")
	}

	inactiveDone := make(chan error, 1)
	go func() {
		inactiveDone <- first.handleAspInactive(messages.NewAspInactive(params.NewRoutingContext(1), nil))
	}()
	if !waitFor(func() bool { return applicationServer.State() == ASPending }, time.Second) {
		close(writeRelease)
		t.Fatalf("AS state = %v, want AS-PENDING", applicationServer.State())
	}

	applicationServer.setASPState(second, StateASPActive, time.Hour)
	time.Sleep(20 * time.Millisecond)
	if got := len(notifies(secondSent.snapshot())); got != 0 {
		close(writeRelease)
		t.Fatalf("later AS-ACTIVE Notify overtook Ack-gated AS-PENDING: %d early Notifies", got)
	}

	close(writeRelease)
	if err := <-dataDone; err != nil {
		t.Fatal(err)
	}
	if err := <-inactiveDone; err != nil {
		t.Fatal(err)
	}
	if !waitFor(func() bool { return len(notifies(secondSent.snapshot())) == 2 }, time.Second) {
		t.Fatalf("ordered state Notifies = %d, want 2", len(notifies(secondSent.snapshot())))
	}
	gotNotifies := notifies(secondSent.snapshot())
	_, firstInformation := statusOf(t, gotNotifies[0])
	_, secondInformation := statusOf(t, gotNotifies[1])
	if firstInformation != uint16(params.AsStatePending&0xffff) || secondInformation != uint16(params.AsStateActive&0xffff) {
		t.Fatalf("AS state Notify order = %#x, %#x, want PENDING then ACTIVE",
			firstInformation, secondInformation)
	}
}

func distributionFixture(t *testing.T, trafficMode uint32) (*Listener, *applicationServer, *Association, *distributionCapture) {
	return distributionFixtureConfigured(t, trafficMode, nil)
}

func distributionFixtureConfigured(t *testing.T, trafficMode uint32, configure func(*AssociationConfig)) (*Listener, *applicationServer, *Association, *distributionCapture) {
	return distributionFixtureForContexts(t, trafficMode, []uint32{1}, configure)
}

func distributionFixtureForContexts(t *testing.T, trafficMode uint32, routingContexts []uint32, configure func(*AssociationConfig)) (*Listener, *applicationServer, *Association, *distributionCapture) {
	t.Helper()
	config := newSGPAssociationConfigForTest(
		&HeartbeatInfo{Enabled: false}, 1, 2, 0, trafficMode, 0, 0,
		routingContexts, params.ServiceIndSCCP, 0, 0, 1,
	)
	listener := newSGPListener(NewListenerConfig(config))
	listener.AssociationConfig.CorrelationID = nil
	if configure != nil {
		configure(listener.AssociationConfig)
	}
	listener.as = newApplicationServers(time.Hour, listener.AssociationConfig)
	applicationServer := listener.as.get(1)
	applicationServer.setTrafficMode(trafficMode)
	asp, sent := addDistributionASP(t, listener, StateASPInactive, routingContexts...)
	return listener, applicationServer, asp, sent
}

func addDistributionASP(t *testing.T, listener *Listener, state State, routingContexts ...uint32) (*Association, *distributionCapture) {
	t.Helper()
	asp, _ := newTestConn(t, state, RoleSGP)
	asp.cfg.RoutingContexts = params.NewRoutingContext(routingContexts...)
	asp.cfg.CorrelationID = nil
	asp.cfg.NetworkAppearance = listener.AssociationConfig.NetworkAppearance.Copy()
	asp.as = listener.as
	capture := new(distributionCapture)
	asp.signalWriter = capture.write
	return asp, capture
}

func distributionData(routingContext uint32, sls uint8, payload string) *messages.Data {
	return messages.NewData(
		params.NewNetworkAppearance(0),
		params.NewRoutingContext(routingContext),
		params.NewProtocolData(1, 2, params.ServiceIndSCCP, 0, 0, sls, []byte(payload)),
		nil,
	)
}

func onlyData(t *testing.T, sent []*messages.Data) *messages.Data {
	t.Helper()
	if len(sent) != 1 {
		t.Fatalf("sent %d messages, want one DATA", len(sent))
	}
	return sent[0]
}

func dataPayload(t *testing.T, message messages.M3UA) []byte {
	t.Helper()
	data, ok := message.(*messages.Data)
	if !ok {
		t.Fatalf("message = %T, want *messages.Data", message)
	}
	payload, err := data.ProtocolData.ProtocolData()
	if err != nil {
		t.Fatal(err)
	}
	return payload.Data
}

type distributionCapture struct {
	mu       sync.Mutex
	messages []messages.M3UA
}

func (capture *distributionCapture) write(message messages.M3UA) (int, error) {
	capture.mu.Lock()
	capture.messages = append(capture.messages, message)
	capture.mu.Unlock()
	return message.MarshalLen(), nil
}

func (capture *distributionCapture) reset() {
	capture.mu.Lock()
	capture.messages = nil
	capture.mu.Unlock()
}

func (capture *distributionCapture) snapshot() []messages.M3UA {
	capture.mu.Lock()
	defer capture.mu.Unlock()
	return append([]messages.M3UA(nil), capture.messages...)
}

func (capture *distributionCapture) data() []*messages.Data {
	capture.mu.Lock()
	defer capture.mu.Unlock()
	data := make([]*messages.Data, 0, len(capture.messages))
	for _, message := range capture.messages {
		if payload, ok := message.(*messages.Data); ok {
			data = append(data, payload)
		}
	}
	return data
}

func (capture *distributionCapture) dataCount() int {
	return len(capture.data())
}
