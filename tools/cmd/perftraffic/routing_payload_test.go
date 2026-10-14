package main

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestRoutePayloadPreservesAllThousandIndependentFlowIdentities(testContext *testing.T) {
	counts := make([]int, 1000)
	for index := uint64(0); index < 3000; index++ {
		identity := planRouteMessage("routes", 7, index)
		if identity.Route != uint16(index%1000) || identity.Sequence != index/1000 || identity.Cohort != "routes" || identity.Seed != 7 {
			testContext.Fatalf("index %d identity=%+v", index, identity)
		}
		counts[identity.Route]++
		payload, err := buildRoutePayload(identity, workloadMix.size(index))
		if err != nil {
			testContext.Fatal(err)
		}
		if payload[4] != 2 || payload[5] != 0 || payload[6] != 0 || payload[7] != kindData || binary.BigEndian.Uint32(payload[44:48]) != uint32(identity.Route) {
			testContext.Fatalf("route identity header differs: %x", payload[:48])
		}
		parsed, err := parseRoutePayload(payload)
		if err != nil || parsed.Route != identity.Route || parsed.Sequence != identity.Sequence || parsed.Seed != identity.Seed || parsed.CohortHash != cohortHash(identity.Cohort) {
			testContext.Fatalf("route payload round trip=%+v error=%v", parsed, err)
		}
		if _, err := parsePayload(payload); err == nil {
			testContext.Fatal("route payload silently accepted by direct parser")
		}
	}
	for route, count := range counts {
		if count != 3 {
			testContext.Fatalf("route %d count=%d", route, count)
		}
	}
	maximum := planRouteMessage("max", 1, ^uint64(0))
	if maximum.Sequence*1000+uint64(maximum.Route) != ^uint64(0) {
		testContext.Fatal("maximum global index does not round trip")
	}
}

func TestRoutePayloadRejectsMalformedHeadersAndBounds(testContext *testing.T) {
	identity := planRouteMessage("routes", 7, 999)
	payload, err := buildRoutePayload(identity, 128)
	if err != nil {
		testContext.Fatal(err)
	}
	for _, mutate := range []func([]byte){
		func(value []byte) { value[0] ^= 1 },
		func(value []byte) { value[4] = 1 },
		func(value []byte) { value[5] = 1 },
		func(value []byte) { value[6] = 1 },
		func(value []byte) { value[7] = kindEchoReply },
		func(value []byte) { binary.BigEndian.PutUint32(value[40:44], 127) },
		func(value []byte) { binary.BigEndian.PutUint32(value[44:48], 1000) },
		func(value []byte) { binary.BigEndian.PutUint64(value[32:40], ^uint64(0)) },
	} {
		changed := bytes.Clone(payload)
		mutate(changed)
		if _, err := parseRoutePayload(changed); err == nil {
			testContext.Fatal("malformed route payload accepted")
		}
	}
	for length := 0; length < payloadHeaderSize; length++ {
		if _, err := parseRoutePayload(payload[:length]); err == nil {
			testContext.Fatalf("short payload length %d accepted", length)
		}
	}
	for _, size := range []int{-1, 0, 47, 4097} {
		if _, err := buildRoutePayload(identity, size); err == nil {
			testContext.Fatalf("size %d accepted", size)
		}
	}
	identity.Route = 1000
	if _, err := buildRoutePayload(identity, 128); err == nil {
		testContext.Fatal("out-of-range identity built")
	}
	direct := buildPayload(messageIdentity{Cohort: "direct", Seed: 1, Flow: 0}, 128)
	if _, err := parseRoutePayload(direct); err == nil {
		testContext.Fatal("direct payload silently accepted by route parser")
	}
	if _, err := parsePayload(direct); err != nil {
		testContext.Fatalf("direct payload behavior changed: %v", err)
	}
}

func FuzzRoutePayload(fuzzContext *testing.F) {
	for _, index := range []uint64{0, 999, 1000, ^uint64(0)} {
		payload, err := buildRoutePayload(planRouteMessage("routes", 7, index), 128)
		if err != nil {
			fuzzContext.Fatal(err)
		}
		fuzzContext.Add(payload)
	}
	fuzzContext.Add([]byte{})
	fuzzContext.Fuzz(func(testContext *testing.T, payload []byte) {
		identity, err := parseRoutePayload(payload)
		if err != nil {
			return
		}
		index, err := routingGlobalIndex(identity)
		if err != nil || index%routingRouteCount != uint64(identity.Route) || index/routingRouteCount != identity.Sequence {
			testContext.Fatalf("accepted identity cannot map to schedule: %+v", identity)
		}
		identity.Cohort = "routes"
		rebuilt, err := buildRoutePayload(identity, len(payload))
		if err != nil {
			testContext.Fatal(err)
		}
		roundTrip, err := parseRoutePayload(rebuilt)
		if err != nil || roundTrip.Route != identity.Route || roundTrip.Sequence != identity.Sequence || roundTrip.Seed != identity.Seed || roundTrip.CohortHash != cohortHash("routes") {
			testContext.Fatalf("accepted identity cannot round trip: %+v %v", roundTrip, err)
		}
	})
}
