package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestBidirectionalResetRequiresPeerControlURL(testContext *testing.T) {
	control := newReceiverControl(1, 16)
	control.setAssociationReady(0, 15)
	server := httptest.NewServer(control.handler())
	defer server.Close()

	specification := runSpec{
		Cohort: "bidi", Seed: 1, Associations: 1, Expected: 10, Duration: time.Second,
		Drain: 2 * time.Second, Outstanding: maxOutstanding, Rate: 10, Payload: workload128, Mode: modeBidirectional, Direction: directionASPToSGP,
	}
	body, err := json.Marshal(specification)
	if err != nil {
		testContext.Fatalf("Marshal: %v", err)
	}
	requireHTTPStatus(testContext, http.MethodPost, server.URL+"/reset", body, http.StatusBadRequest)

	specification.PeerControl = "http://asp.example:8080"
	body, err = json.Marshal(specification)
	if err != nil {
		testContext.Fatalf("Marshal: %v", err)
	}
	requireHTTPStatus(testContext, http.MethodPost, server.URL+"/reset", body, http.StatusNoContent)
}

func TestReverseDirectionValidatesReversedTuples(testContext *testing.T) {
	control := newReceiverControl(1, 16)
	control.setAssociationReady(0, 15)
	specification := runSpec{
		Cohort: "bidi-reverse", Seed: 7, Associations: 1, Expected: 2, Duration: time.Second,
		Drain: 2 * time.Second, Outstanding: maxOutstanding, Rate: 2, Payload: workload128, Mode: modeThroughput, Direction: directionSGPToASP,
	}
	if err := control.reset(specification); err != nil {
		testContext.Fatalf("reset: %v", err)
	}
	if err := control.start(); err != nil {
		testContext.Fatalf("start: %v", err)
	}
	forward := validReceivedMessage("bidi-reverse", 7, 0, 0, 0, 128)
	if _, outcome := control.record(0, forward); outcome != recordInvalid {
		testContext.Fatalf("forward tuple outcome = %d, want invalid in the reverse direction", outcome)
	}
	reverse := validReceivedMessage("bidi-reverse", 7, 0, 0, 0, 128)
	reverse.ProtocolData.OriginatingPointCode, reverse.ProtocolData.DestinationPointCode =
		reverse.ProtocolData.DestinationPointCode, reverse.ProtocolData.OriginatingPointCode
	received, outcome := control.record(0, reverse)
	if outcome != recordUnique || received.identity.Sequence != 0 {
		testContext.Fatalf("reverse tuple record = %+v, %d; want unique", received, outcome)
	}
}

func TestReverseDriverRecordsFailureWithoutReplacingForwardEvidence(testContext *testing.T) {
	control := newReceiverControl(1, 16)
	control.setAssociationReady(0, 15)
	control.cpuStatPath = writeCPUStatFixture(testContext, "usage_usec 10\nnr_throttled 0\n")
	peer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Error(writer, "conflict", http.StatusConflict)
	}))
	defer peer.Close()
	specification := runSpec{
		Cohort: "bidi", Seed: 1, Associations: 1, Expected: 2, Duration: time.Second,
		Drain: 2 * time.Second, Outstanding: maxOutstanding, Rate: 2, Payload: workload128, Mode: modeBidirectional,
		Direction: directionASPToSGP, PeerControl: peer.URL,
	}
	if err := control.reset(specification); err != nil {
		testContext.Fatalf("reset: %v", err)
	}
	driver := &reverseDriver{ctx: context.Background(), cpuStatPath: control.cpuStatPath}
	control.runReverseCohort(driver, specification)
	if control.reverseError == "" || !strings.Contains(control.reverseError, "reset receiver") {
		testContext.Fatalf("reverseError = %q, want the reset failure preserved", control.reverseError)
	}
	if control.reverseSender == nil || control.reverseReceiver == nil {
		testContext.Fatal("reverse records were not preserved after the failure")
	}
	record := control.result()
	if record.Reverse == nil || record.ReverseError == "" {
		testContext.Fatalf("result lost reverse evidence: %+v", record)
	}
}

func TestCollectReverseFoldsReverseVerdicts(testContext *testing.T) {
	tests := []struct {
		name            string
		reverse         *runRecord
		reverseReceiver *runRecord
		reverseError    string
		wantVerdict     string
	}{
		{name: "both pass", reverse: &runRecord{Verdict: verdictInconclusive}, reverseReceiver: &runRecord{Verdict: verdictInconclusive}, wantVerdict: verdictInconclusive},
		{name: "reverse invalid", reverse: &runRecord{Verdict: verdictInvalid}, reverseReceiver: &runRecord{Verdict: verdictPass}, wantVerdict: verdictInvalid},
		{name: "reverse receiver invalid", reverse: &runRecord{Verdict: verdictPass}, reverseReceiver: &runRecord{Verdict: verdictInvalid}, wantVerdict: verdictInvalid},
		{name: "reverse error", reverseError: "reverse cohort is invalid", wantVerdict: verdictInvalid},
	}
	for _, test := range tests {
		testContext.Run(test.name, func(testContext *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				writeJSON(writer, http.StatusOK, runRecord{
					Verdict: verdictPass, Reverse: test.reverse, ReverseReceiver: test.reverseReceiver, ReverseError: test.reverseError,
				})
			}))
			defer server.Close()
			result := cohortResult{Verdict: verdictPass}
			collectReverse(context.Background(), commandConfig{PeerControl: server.URL, Drain: time.Second}, &result)
			if result.Verdict != test.wantVerdict {
				testContext.Fatalf("verdict = %q, want %q", result.Verdict, test.wantVerdict)
			}
			if test.reverseError != "" && !strings.Contains(result.Error, test.reverseError) {
				testContext.Fatalf("error = %q, want the reverse error preserved", result.Error)
			}
		})
	}
}

func TestCollectReverseMissingCohortNeverPassesSilently(testContext *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writeJSON(writer, http.StatusOK, runRecord{Verdict: verdictPass})
	}))
	defer server.Close()
	config := commandConfig{PeerControl: server.URL, Drain: time.Millisecond}
	result := cohortResult{Verdict: verdictPass}
	started := time.Now()
	collectReverse(context.Background(), config, &result)
	if result.Verdict != verdictInvalid || !strings.Contains(result.Error, "did not complete") {
		testContext.Fatalf("result = %+v, want invalid with a collection-deadline error", result)
	}
	if time.Since(started) > 30*time.Second {
		testContext.Fatalf("collection waited too long: %s", time.Since(started))
	}
}

func TestCollectReverseWaitsForTheReverseCohort(testContext *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		record := runRecord{Verdict: verdictPass}
		if calls.Add(1) >= 3 {
			record.Reverse = &runRecord{Verdict: verdictInconclusive}
			record.ReverseReceiver = &runRecord{Verdict: verdictPass}
		}
		writeJSON(writer, http.StatusOK, record)
	}))
	defer server.Close()
	result := cohortResult{Verdict: verdictInconclusive}
	collectReverse(context.Background(), commandConfig{PeerControl: server.URL, Drain: 5 * time.Second}, &result)
	if result.ReverseSender == nil || result.ReverseReceiver == nil {
		testContext.Fatal("collectReverse returned before the reverse cohort completed")
	}
	if result.Verdict != verdictInconclusive {
		testContext.Fatalf("verdict = %q, want inconclusive", result.Verdict)
	}
}

func TestCombineVerdicts(testContext *testing.T) {
	if combineVerdicts(verdictPass, verdictPass) != verdictPass ||
		combineVerdicts(verdictPass, verdictInconclusive) != verdictInconclusive ||
		combineVerdicts(verdictInconclusive, verdictInvalid) != verdictInvalid ||
		combineVerdicts(verdictPass, verdictInvalid) != verdictInvalid {
		testContext.Fatal("combineVerdicts did not take the worst verdict")
	}
}

// bidirectionalSpecBody builds the /reset body as raw JSON so the test states
// the wire contract the peer actually sends, field names included.
func bidirectionalSpecBody(testContext *testing.T, peerControl string, outstanding int) []byte {
	testContext.Helper()
	body, err := json.Marshal(map[string]any{
		"cohort": "bidi", "seed": 1, "associations": 1, "expected": 2,
		"duration_ns": time.Second, "drain_ns": 2 * time.Second, "rate": 2,
		"payload": string(workload128), "mode": modeBidirectional,
		"direction": directionASPToSGP, "peer_control": peerControl,
		"outstanding": outstanding,
	})
	if err != nil {
		testContext.Fatalf("Marshal: %v", err)
	}
	return body
}

// capturingPeer answers a reverse driver's first control call and hands the
// request body back to the test. Failing the call keeps the reverse cohort
// short; the body is the evidence.
func capturingPeer(testContext *testing.T, path string) (*httptest.Server, <-chan map[string]any) {
	testContext.Helper()
	bodies := make(chan map[string]any, 4)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == path {
			var body map[string]any
			if err := json.NewDecoder(request.Body).Decode(&body); err == nil {
				select {
				case bodies <- body:
				default:
				}
			}
		}
		http.Error(writer, "conflict", http.StatusConflict)
	}))
	testContext.Cleanup(server.Close)
	return server, bodies
}

// Both directions of a bidirectional run must be bounded by the same
// outstanding limit. The reverse cohort took the compile-time maximum instead
// of the limit the run was configured with, so a run with a smaller
// -outstanding measured each direction under a different bound and reported a
// different manifest for each.
func TestReverseCohortUsesTheRunOutstandingLimit(testContext *testing.T) {
	const outstanding = 7
	peer, resets := capturingPeer(testContext, "/reset")

	control := newReceiverControl(1, 16)
	control.setAssociationReady(0, 15)
	control.driver = &reverseDriver{ctx: context.Background()}
	server := httptest.NewServer(control.handler())
	defer server.Close()

	requireHTTPStatus(testContext, http.MethodPost, server.URL+"/reset",
		bidirectionalSpecBody(testContext, peer.URL, outstanding), http.StatusNoContent)
	requireHTTPStatus(testContext, http.MethodPost, server.URL+"/start", nil, http.StatusNoContent)

	select {
	case body := <-resets:
		if body["outstanding"] != float64(outstanding) {
			testContext.Fatalf("reverse run spec outstanding = %v, want the forward limit %d", body["outstanding"], outstanding)
		}
	case <-time.After(10 * time.Second):
		testContext.Fatal("the reverse cohort never reset its peer")
	}

	if forward, reverse := currentManifest(outstanding, ""), currentManifest(outstanding, ""); forward != reverse {
		testContext.Fatalf("manifests differ across directions at the same limit: %+v vs %+v", forward, reverse)
	}
	if currentManifest(outstanding, "").OutstandingLimit != outstanding {
		testContext.Fatalf("manifest outstanding limit = %d, want %d", currentManifest(outstanding, "").OutstandingLimit, outstanding)
	}
}

// The limit crosses the control boundary, so it is bounded there exactly as
// the command-line flag is.
func TestResetBoundsTheOutstandingLimit(testContext *testing.T) {
	for _, outstanding := range []int{0, -1, maxOutstanding + 1, int(^uint(0) >> 1)} {
		control := newReceiverControl(1, 16)
		control.setAssociationReady(0, 15)
		server := httptest.NewServer(control.handler())
		requireHTTPStatus(testContext, http.MethodPost, server.URL+"/reset",
			bidirectionalSpecBody(testContext, "http://peer.example:8080", outstanding), http.StatusBadRequest)
		if control.ledger != nil {
			testContext.Fatalf("outstanding %d armed a cohort", outstanding)
		}
		server.Close()
	}
}
