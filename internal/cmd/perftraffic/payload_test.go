package main

import (
	"testing"
)

func TestWorkloadSizeDeterministicMix(testContext *testing.T) {
	counts := map[int]int{}
	for sequence := uint64(0); sequence < 100; sequence++ {
		counts[workloadMix.size(sequence)]++
	}
	if counts[128] != 90 || counts[512] != 9 || counts[4096] != 1 {
		testContext.Fatalf("mix counts = %#v, want 90/9/1", counts)
	}
}

func TestBuildAndValidateMessage(testContext *testing.T) {
	tuple := tupleFor(11, 3)
	payload := buildPayload(messageIdentity{
		Cohort:      "cohort-a",
		Seed:        1234,
		Association: 3,
		Flow:        11,
		Sequence:    47,
	}, 512)

	identity, err := validateMessage(receivedMessage{
		ProtocolData: protocolData{
			OriginatingPointCode:    tuple.OriginatingPointCode,
			DestinationPointCode:    tuple.DestinationPointCode,
			ServiceIndicator:        tuple.ServiceIndicator,
			NetworkIndicator:        tuple.NetworkIndicator,
			MessagePriority:         tuple.MessagePriority,
			SignallingLinkSelection: tuple.SignallingLinkSelection,
			Data:                    payload,
		},
		NetworkAppearance:    testNetworkAppearance,
		NetworkAppearanceSet: true,
		RoutingContext:       tuple.RoutingContext,
		RoutingContextSet:    true,
	}, "cohort-a", 1234, 8, workload512)
	if err != nil {
		testContext.Fatalf("validateMessage: %v", err)
	}
	if identity.Association != 3 || identity.Flow != 11 || identity.Sequence != 47 {
		testContext.Fatalf("identity = %+v", identity)
	}
}

func TestValidateMessageRejectsEveryScopedFieldAndPayloadCorruption(testContext *testing.T) {
	identity := messageIdentity{Cohort: "cohort-a", Seed: 1234, Association: 1, Flow: 5, Sequence: 47}
	tuple := tupleFor(identity.Flow, identity.Association)
	valid := receivedMessage{
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
	testCases := []struct {
		name   string
		mutate func(*receivedMessage)
	}{
		{name: "originating point code", mutate: func(message *receivedMessage) { message.ProtocolData.OriginatingPointCode++ }},
		{name: "destination point code", mutate: func(message *receivedMessage) { message.ProtocolData.DestinationPointCode++ }},
		{name: "service indicator", mutate: func(message *receivedMessage) { message.ProtocolData.ServiceIndicator++ }},
		{name: "network indicator", mutate: func(message *receivedMessage) { message.ProtocolData.NetworkIndicator++ }},
		{name: "message priority", mutate: func(message *receivedMessage) { message.ProtocolData.MessagePriority++ }},
		{name: "signalling link selection", mutate: func(message *receivedMessage) { message.ProtocolData.SignallingLinkSelection++ }},
		{name: "network appearance", mutate: func(message *receivedMessage) { message.NetworkAppearance++ }},
		{name: "network appearance omitted", mutate: func(message *receivedMessage) { message.NetworkAppearanceSet = false }},
		{name: "routing context", mutate: func(message *receivedMessage) { message.RoutingContext++ }},
		{name: "routing context omitted", mutate: func(message *receivedMessage) { message.RoutingContextSet = false }},
		{name: "payload", mutate: func(message *receivedMessage) { message.ProtocolData.Data[len(message.ProtocolData.Data)-1] ^= 0xff }},
	}
	for _, testCase := range testCases {
		testContext.Run(testCase.name, func(testContext *testing.T) {
			message := valid.clone()
			testCase.mutate(&message)
			if _, err := validateMessage(message, identity.Cohort, identity.Seed, 8, workload128); err == nil {
				testContext.Fatal("validateMessage unexpectedly succeeded")
			}
		})
	}
}

func TestValidateMessageRejectsWrongFixedWorkloadSize(testContext *testing.T) {
	message := validReceivedMessage("cohort-a", 7, 0, 0, 0, 128)
	if _, err := validateMessage(message, "cohort-a", 7, 1, workload4096); err == nil {
		testContext.Fatal("validateMessage unexpectedly accepted 128 bytes for the 4096-byte workload")
	}
}

func TestValidateMessageRejectsWrongMixPositionSize(testContext *testing.T) {
	message := validReceivedMessage("cohort-a", 7, 0, 3, 3, 128)
	if _, err := validateMessage(message, "cohort-a", 7, 1, workloadMix); err == nil {
		testContext.Fatal("validateMessage unexpectedly accepted 128 bytes at mixed-workload global position 99")
	}
}

func FuzzValidatePayload(fuzz *testing.F) {
	fuzz.Add([]byte{})
	fuzz.Add(buildPayload(messageIdentity{Cohort: "seed", Seed: 1, Association: 0, Flow: 0, Sequence: 0}, 128))
	fuzz.Fuzz(func(testContext *testing.T, payload []byte) {
		_, _ = parsePayload(payload)
	})
}

func validReceivedMessage(cohort string, seed uint64, association, flow uint8, sequence uint64, size int) receivedMessage {
	identity := messageIdentity{
		Cohort:      cohort,
		Seed:        seed,
		Association: association,
		Flow:        flow,
		Sequence:    sequence,
	}
	tuple := tupleFor(flow, association)
	return receivedMessage{
		ProtocolData: protocolData{
			OriginatingPointCode:    tuple.OriginatingPointCode,
			DestinationPointCode:    tuple.DestinationPointCode,
			ServiceIndicator:        tuple.ServiceIndicator,
			NetworkIndicator:        tuple.NetworkIndicator,
			MessagePriority:         tuple.MessagePriority,
			SignallingLinkSelection: tuple.SignallingLinkSelection,
			Data:                    buildPayload(identity, size),
		},
		NetworkAppearance:    testNetworkAppearance,
		NetworkAppearanceSet: true,
		RoutingContext:       tuple.RoutingContext,
		RoutingContextSet:    true,
	}
}
