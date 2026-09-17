package main

import (
	"testing"
	"time"
)

func TestEchoTrackerAdmissionCapAndValidation(testContext *testing.T) {
	scheduled := time.Unix(100, 0)
	tracker := newEchoTracker("echo-cohort", 7, 1, workload128, 2, echoRequestDeadline)
	if !tracker.admit(0, scheduled) || !tracker.admit(1, scheduled) {
		testContext.Fatal("admission within the cap failed")
	}
	if tracker.admit(2, scheduled) {
		testContext.Fatal("admission above the cap succeeded")
	}
	if tracker.outstandingCount() != 2 {
		testContext.Fatalf("outstanding = %d, want 2", tracker.outstandingCount())
	}
	tracker.complete(0, scheduled.Add(3*time.Millisecond))
	tracker.complete(9, scheduled.Add(time.Millisecond))
	tracker.complete(0, scheduled.Add(time.Millisecond))
	result := tracker.result(2)
	if result.Validated != 1 || result.Capped != 1 || result.Invalid != 2 || result.DeadlineExceeded != 0 {
		testContext.Fatalf("result = %+v", result)
	}
	if result.RTT.Count != 1 || result.RTT.Max < 3*time.Millisecond || result.RTT.Max >= 4*time.Millisecond {
		testContext.Fatalf("rtt = %+v, want one 3 ms observation", result.RTT)
	}
}

func TestEchoTrackerDeadlineFailuresAreCountedNotOmitted(testContext *testing.T) {
	scheduled := time.Unix(100, 0)
	tracker := newEchoTracker("echo-cohort", 7, 1, workload128, 4, echoRequestDeadline)
	tracker.admit(0, scheduled)
	tracker.admit(1, scheduled)
	tracker.sweep(scheduled.Add(echoRequestDeadline + time.Millisecond))
	if tracker.outstandingCount() != 0 {
		testContext.Fatalf("outstanding = %d, want 0 after the sweep", tracker.outstandingCount())
	}
	tracker.complete(0, scheduled.Add(echoRequestDeadline+2*time.Millisecond))
	result := tracker.result(2)
	if result.DeadlineExceeded != 2 || result.Validated != 0 || result.Invalid != 1 || result.RTT.Count != 0 {
		testContext.Fatalf("result = %+v, want two deadline failures and no RTT credit", result)
	}
}

func TestEchoTrackerLateCompletionWithinMapIsADeadlineFailure(testContext *testing.T) {
	scheduled := time.Unix(100, 0)
	tracker := newEchoTracker("echo-cohort", 7, 1, workload128, 4, echoRequestDeadline)
	tracker.admit(0, scheduled)
	tracker.complete(0, scheduled.Add(echoRequestDeadline+time.Nanosecond))
	result := tracker.result(1)
	if result.DeadlineExceeded != 1 || result.Validated != 0 || result.RTT.Count != 0 {
		testContext.Fatalf("result = %+v, want deadline failure without RTT credit", result)
	}
}

func TestEchoTrackerFailRemovesWithoutACounter(testContext *testing.T) {
	scheduled := time.Unix(100, 0)
	tracker := newEchoTracker("echo-cohort", 7, 1, workload128, 4, echoRequestDeadline)
	tracker.admit(0, scheduled)
	tracker.fail(0)
	tracker.complete(0, scheduled.Add(time.Millisecond))
	result := tracker.result(0)
	if result.Invalid != 1 || result.Validated != 0 || result.OutstandingAfterDrain != 0 {
		testContext.Fatalf("result = %+v", result)
	}
}

func TestEchoRegistryRoutesByCohortAndCountsUnattributed(testContext *testing.T) {
	registry := newEchoRegistry()
	first := newEchoTracker("cohort-a", 1, 1, workload128, 4, echoRequestDeadline)
	second := newEchoTracker("cohort-b", 1, 1, workload128, 4, echoRequestDeadline)
	registry.register(first)
	registry.register(second)
	if registry.trackerFor(first.cohortHash) != first || registry.trackerFor(second.cohortHash) != second {
		testContext.Fatal("registry did not route by cohort hash")
	}
	registry.unattributed()
	if result := second.result(0); result.Invalid != 1 {
		testContext.Fatalf("second result = %+v, want the unattributed reply on the newest cohort", result)
	}
	if result := first.result(0); result.Invalid != 0 {
		testContext.Fatalf("first result = %+v, want no cross-cohort attribution", result)
	}
}

func TestPayloadKindsRoundTripAndRejectUnknownKinds(testContext *testing.T) {
	for _, kind := range []byte{kindData, kindEchoRequest, kindEchoReply} {
		payload := buildPayload(messageIdentity{Cohort: "k", Seed: 1, Association: 1, Flow: 2, Sequence: 3, Kind: kind}, 128)
		identity, err := parsePayload(payload)
		if err != nil {
			testContext.Fatalf("parsePayload kind %d: %v", kind, err)
		}
		if identity.Kind != kind {
			testContext.Fatalf("kind = %d, want %d", identity.Kind, kind)
		}
	}
	payload := buildPayload(messageIdentity{Cohort: "k", Seed: 1, Sequence: 0}, 128)
	payload[7] = 3
	if _, err := parsePayload(payload); err == nil {
		testContext.Fatal("parsePayload accepted an unknown kind")
	}
}

func TestValidateMessageRequiresMatchingKindAndDirection(testContext *testing.T) {
	identity := messageIdentity{Cohort: "echo", Seed: 5, Association: 1, Flow: 3, Sequence: 3, Kind: kindEchoReply}
	tuple := reverseTuple(tupleFor(identity.Flow, identity.Association))
	message := receivedMessage{
		ProtocolData: protocolData{
			OriginatingPointCode:    tuple.OriginatingPointCode,
			DestinationPointCode:    tuple.DestinationPointCode,
			ServiceIndicator:        tuple.ServiceIndicator,
			NetworkIndicator:        tuple.NetworkIndicator,
			MessagePriority:         tuple.MessagePriority,
			SignallingLinkSelection: tuple.SignallingLinkSelection,
			Data:                    buildPayload(identity, 128),
		},
		NetworkAppearance:    testNetworkAppearance,
		NetworkAppearanceSet: true,
		RoutingContext:       tuple.RoutingContext,
		RoutingContextSet:    true,
	}
	if _, err := validateMessage(message, "echo", 5, 2, workload128, kindEchoReply, true); err != nil {
		testContext.Fatalf("validateMessage rejected a valid echo reply: %v", err)
	}
	if _, err := validateMessage(message, "echo", 5, 2, workload128, kindEchoRequest, true); err == nil {
		testContext.Fatal("validateMessage accepted a reply as a request")
	}
	if _, err := validateMessage(message, "echo", 5, 2, workload128, kindEchoReply, false); err == nil {
		testContext.Fatal("validateMessage accepted a reversed tuple in the forward direction")
	}
}

func TestReceiverEchoResultAndReplyAccounting(testContext *testing.T) {
	control := newReceiverControl(1, 16)
	control.setAssociationReady(0, 15)
	control.cpuStatPath = writeCPUStatFixture(testContext, "usage_usec 10\nnr_throttled 0\n")
	specification := runSpec{
		Cohort: "echo-cohort", Seed: 7, Associations: 1, Expected: 1, Duration: time.Second,
		Drain: 2 * time.Second, Payload: workload128, Rate: 1, Outstanding: maxOutstanding, Mode: modeEcho, Direction: directionASPToSGP,
	}
	if err := control.reset(specification); err != nil {
		testContext.Fatalf("reset: %v", err)
	}
	if err := control.start(); err != nil {
		testContext.Fatalf("start: %v", err)
	}
	if !control.echoMode() {
		testContext.Fatal("echoMode() = false in an echo cohort")
	}
	request := validReceivedMessage("echo-cohort", 7, 0, 0, 0, 128)
	request.ProtocolData.Data[7] = kindEchoRequest
	received, outcome := control.record(0, request)
	if outcome != recordUnique || received.identity.Cohort != "echo-cohort" || received.identity.Kind != kindEchoRequest {
		testContext.Fatalf("record = %+v, %d; want a unique echo request with its cohort", received, outcome)
	}
	control.recordEchoReply(control.currentGeneration(), nil)
	control.recordEchoReply(control.currentGeneration(), nil)
	if err := control.stop(); err != nil {
		testContext.Fatalf("stop: %v", err)
	}
	record := control.result()
	if record.ReceiverEcho == nil || record.ReceiverEcho.Replies != 2 || record.ReceiverEcho.ReplyErrors != 0 {
		testContext.Fatalf("receiver echo = %+v", record.ReceiverEcho)
	}
	if record.ReceiverEcho.Scope != echoReceiverScope {
		testContext.Fatalf("scope = %q", record.ReceiverEcho.Scope)
	}
}

func TestReceiverEchoReplyErrorIsInvalid(testContext *testing.T) {
	control := newReceiverControl(1, 16)
	control.setAssociationReady(0, 15)
	control.cpuStatPath = writeCPUStatFixture(testContext, "usage_usec 10\nnr_throttled 0\n")
	specification := runSpec{
		Cohort: "echo-cohort", Seed: 7, Associations: 1, Expected: 1, Duration: time.Second,
		Payload: workload128, Rate: 1, Outstanding: maxOutstanding, Mode: modeEcho,
	}
	if err := control.reset(specification); err != nil {
		testContext.Fatalf("reset: %v", err)
	}
	if err := control.start(); err != nil {
		testContext.Fatalf("start: %v", err)
	}
	request := validReceivedMessage("echo-cohort", 7, 0, 0, 0, 128)
	request.ProtocolData.Data[7] = kindEchoRequest
	control.record(0, request)
	control.recordEchoReply(control.currentGeneration(), assertionError("write failed"))
	if err := control.stop(); err != nil {
		testContext.Fatalf("stop: %v", err)
	}
	if record := control.result(); record.FixtureVerdict != verdictInvalid {
		testContext.Fatalf("fixture verdict = %q, want invalid after a reply error", record.FixtureVerdict)
	}
}

func TestThroughputRejectsEchoRequestKind(testContext *testing.T) {
	control := newReceiverControl(1, 16)
	control.setAssociationReady(0, 15)
	if err := control.reset(runSpec{Cohort: "cohort-a", Seed: 7, Associations: 1, Expected: 1, Duration: time.Second, Payload: workload128, Rate: 1, Outstanding: maxOutstanding}); err != nil {
		testContext.Fatalf("reset: %v", err)
	}
	if err := control.start(); err != nil {
		testContext.Fatalf("start: %v", err)
	}
	message := validReceivedMessage("cohort-a", 7, 0, 0, 0, 128)
	message.ProtocolData.Data[7] = kindEchoRequest
	if _, outcome := control.record(0, message); outcome != recordInvalid {
		testContext.Fatalf("outcome = %d, want invalid for an echo request in a throughput cohort", outcome)
	}
}

func TestSenderEchoResultRequiresEveryRequestAnswered(testContext *testing.T) {
	record := runRecord{Expected: 10, Echo: &echoResult{Validated: 9}}
	record.evaluate()
	if record.FixtureVerdict != verdictInvalid {
		testContext.Fatalf("fixture verdict = %q, want invalid when a reply is missing", record.FixtureVerdict)
	}
	record = runRecord{Expected: 10, Echo: &echoResult{Validated: 10, DeadlineExceeded: 1}}
	record.evaluate()
	if record.FixtureVerdict != verdictInvalid {
		testContext.Fatalf("fixture verdict = %q, want invalid with a deadline failure", record.FixtureVerdict)
	}
}

type assertionError string

func (err assertionError) Error() string { return string(err) }

func FuzzEchoTrackerInvariants(fuzzContext *testing.F) {
	fuzzContext.Add(uint64(0), uint8(0), int64(0))
	fuzzContext.Add(^uint64(0), uint8(2), int64(3_000_000_000))
	fuzzContext.Fuzz(func(testContext *testing.T, index uint64, operation uint8, advance int64) {
		scheduled := time.Unix(100, 0)
		tracker := newEchoTracker("fuzz", 1, 1, workload128, 4, echoRequestDeadline)
		admitted := map[uint64]bool{}
		for step := 0; step < 64; step++ {
			now := scheduled.Add(time.Duration(advance) * time.Duration(step))
			current := index + uint64(step)
			switch (operation + uint8(step)) % 4 {
			case 0:
				if tracker.admit(current, scheduled) {
					admitted[current] = true
					if tracker.outstandingCount() > 4 {
						testContext.Fatalf("outstanding exceeded the cap")
					}
				}
			case 1:
				tracker.complete(current, now)
				delete(admitted, current)
			case 2:
				tracker.sweep(now)
			case 3:
				tracker.fail(current)
				delete(admitted, current)
			}
			result := tracker.result(0)
			if result.Validated > 64 || result.OutstandingAfterDrain > 4 {
				testContext.Fatalf("impossible tracker state: %+v", result)
			}
		}
	})
}
