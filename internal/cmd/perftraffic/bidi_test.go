package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBidirectionalResetRequiresPeerControlURL(testContext *testing.T) {
	control := newReceiverControl(1, 16)
	control.setAssociationReady(0, 15)
	control.reverseControl = "http://asp.example:8080"
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

	specification.PeerControl = control.reverseControl
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
	control.reverseControl = peer.URL
	if err := control.reset(specification); err != nil {
		testContext.Fatalf("reset: %v", err)
	}
	driver := &reverseDriver{ctx: context.Background(), cpuStatPath: control.cpuStatPath}
	control.runReverseCohort(driver, reverseRun{specification: specification, reverseControl: peer.URL, generation: control.currentGeneration()})
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
	control.reverseControl = peer.URL
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
		control.reverseControl = "http://peer.example:8080"
		server := httptest.NewServer(control.handler())
		requireHTTPStatus(testContext, http.MethodPost, server.URL+"/reset",
			bidirectionalSpecBody(testContext, control.reverseControl, outstanding), http.StatusBadRequest)
		if control.ledger != nil {
			testContext.Fatalf("outstanding %d armed a cohort", outstanding)
		}
		server.Close()
	}
}

// countingPeer records every request it receives, whatever the path, so a
// test can assert that a host was never contacted at all.
func countingPeer(testContext *testing.T) (*httptest.Server, *atomic.Int64) {
	testContext.Helper()
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		http.Error(writer, "conflict", http.StatusConflict)
	}))
	testContext.Cleanup(server.Close)
	return server, &requests
}

// The control listener is unauthenticated and binds every interface, and
// /start makes the receiver issue HTTP requests to the reverse control
// destination. That destination must therefore come from this process's own
// configuration and never from the request body, or anyone who can reach the
// control port can aim the receiver at a host of their choosing.
func TestResetRefusesAReverseControlDestinationTheReceiverWasNotConfiguredWith(testContext *testing.T) {
	elsewhere, requests := countingPeer(testContext)
	control := newReceiverControl(1, 16)
	control.setAssociationReady(0, 15)
	control.driver = &reverseDriver{ctx: context.Background()}
	server := httptest.NewServer(control.handler())
	defer server.Close()

	requireHTTPStatus(testContext, http.MethodPost, server.URL+"/reset",
		bidirectionalSpecBody(testContext, elsewhere.URL, maxOutstanding), http.StatusBadRequest)
	// The refused reset armed nothing, so a start cannot drive a cohort either.
	requireHTTPStatus(testContext, http.MethodPost, server.URL+"/start", nil, http.StatusConflict)
	time.Sleep(250 * time.Millisecond)
	if count := requests.Load(); count != 0 {
		testContext.Fatalf("the receiver issued %d request(s) to a host it was never configured with", count)
	}
}

// Pinning the destination at reset is the check an operator sees; taking it
// from configuration inside the cohort is what makes a redirect impossible.
// A specification that names another host must not reach that host even when
// it is handed straight to the cohort.
func TestReverseCohortDrivesOnlyTheConfiguredDestination(testContext *testing.T) {
	configured, configuredRequests := countingPeer(testContext)
	elsewhere, elsewhereRequests := countingPeer(testContext)

	control := newReceiverControl(1, 16)
	control.setAssociationReady(0, 15)
	control.cpuStatPath = writeCPUStatFixture(testContext, "usage_usec 10\nnr_throttled 0\n")
	control.reverseControl = configured.URL
	specification := runSpec{
		Cohort: "bidi", Seed: 1, Associations: 1, Expected: 2, Duration: time.Second,
		Drain: 2 * time.Second, Outstanding: maxOutstanding, Rate: 2, Payload: workload128,
		Mode: modeBidirectional, Direction: directionASPToSGP, PeerControl: elsewhere.URL,
	}
	driver := &reverseDriver{ctx: context.Background(), cpuStatPath: control.cpuStatPath}
	control.runReverseCohort(driver, reverseRun{specification: specification, reverseControl: control.reverseControl, generation: control.currentGeneration()})

	if elsewhereRequests.Load() != 0 {
		testContext.Fatalf("the reverse cohort made %d request(s) to the host named in the specification", elsewhereRequests.Load())
	}
	if configuredRequests.Load() == 0 {
		testContext.Fatal("the reverse cohort never reached the configured destination")
	}
}

// Each refusal reason is distinct, because they tell an operator different
// things: one receiver was never given a reverse destination, the other was
// given a different one than the specification names.
func TestResetRefusesAnyReverseDestinationButTheConfiguredOne(testContext *testing.T) {
	const configured = "http://asp.example:8080"
	tests := []struct {
		name        string
		reverse     string
		peerControl string
		wantReason  string
	}{
		{name: "receiver has no configured destination", reverse: "", peerControl: "http://elsewhere.example:8080",
			wantReason: "no configured reverse control destination"},
		{name: "another host", reverse: configured, peerControl: "http://elsewhere.example:8080",
			wantReason: "not this receiver's configured reverse control destination"},
		{name: "another port on the configured host", reverse: configured, peerControl: "http://asp.example:9090",
			wantReason: "not this receiver's configured reverse control destination"},
		{name: "another scheme", reverse: configured, peerControl: "https://asp.example:8080",
			wantReason: "not this receiver's configured reverse control destination"},
		{name: "trailing slash", reverse: configured, peerControl: configured + "/",
			wantReason: "not this receiver's configured reverse control destination"},
		{name: "no destination at all", reverse: configured, peerControl: "",
			wantReason: "bidirectional runs require the peer control URL"},
	}
	for _, test := range tests {
		testContext.Run(test.name, func(testContext *testing.T) {
			control := newReceiverControl(1, 16)
			control.setAssociationReady(0, 15)
			control.reverseControl = test.reverse
			err := control.reset(runSpec{
				Cohort: "bidi", Seed: 1, Associations: 1, Expected: 2, Duration: time.Second,
				Drain: 2 * time.Second, Outstanding: maxOutstanding, Rate: 2, Payload: workload128,
				Mode: modeBidirectional, Direction: directionASPToSGP, PeerControl: test.peerControl,
			})
			if err == nil {
				testContext.Fatal("reset accepted a reverse destination the receiver was not configured with")
			}
			if !errors.Is(err, errInvalidRunSpec) {
				testContext.Fatalf("error = %v, want an invalid run specification", err)
			}
			if !strings.Contains(err.Error(), test.wantReason) {
				testContext.Fatalf("error = %v, want it to name %q", err, test.wantReason)
			}
			if control.ledger != nil {
				testContext.Fatal("the refused reset armed a cohort")
			}
		})
	}
}

// The SGP receiver's reverse destination comes from its own configuration.
func TestRunReceiverControlTakesItsReverseDestinationFromConfiguration(testContext *testing.T) {
	config := commandConfig{Associations: 2, PeerControl: "http://asp.example:8080", CPUStatPath: "/fixture/cpu.stat"}
	control := newRunReceiverControl(context.Background(), config)
	if control.reverseControl != config.PeerControl {
		testContext.Fatalf("reverseControl = %q, want the configured %q", control.reverseControl, config.PeerControl)
	}
	if control.driver == nil || control.driver.cpuStatPath != config.CPUStatPath {
		testContext.Fatalf("reverse driver = %+v, want it built from the configuration", control.driver)
	}
	if control.cpuStatPath != config.CPUStatPath || control.expectedAssociations != config.Associations {
		testContext.Fatalf("control = %+v, want it built from the configuration", control)
	}
}

// A reverse cohort outlives the call that launched it. If it finishes after a
// reset, the cohort it belonged to is gone — the reset cleared the reverse
// fields and advanced the generation — so its records and its error belong
// nowhere, and certainly not to the cohort that has taken its place.
func TestReverseCohortCompletionForAnEndedCohortIsDiscarded(testContext *testing.T) {
	peer, _ := countingPeer(testContext)
	control := newReceiverControl(1, 16)
	control.setAssociationReady(0, 15)
	control.cpuStatPath = writeCPUStatFixture(testContext, "usage_usec 10\nnr_throttled 0\n")
	control.reverseControl = peer.URL
	specification := runSpec{
		Cohort: "bidi", Seed: 1, Associations: 1, Expected: 2, Duration: time.Second,
		Drain: 2 * time.Second, Outstanding: maxOutstanding, Rate: 2, Payload: workload128,
		Mode: modeBidirectional, Direction: directionASPToSGP, PeerControl: peer.URL,
	}
	if err := control.reset(specification); err != nil {
		testContext.Fatalf("reset: %v", err)
	}
	stale := control.currentGeneration()
	if err := control.start(); err != nil {
		testContext.Fatalf("start: %v", err)
	}
	if err := control.stop(); err != nil {
		testContext.Fatalf("stop: %v", err)
	}
	if err := control.reset(specification); err != nil {
		testContext.Fatalf("second reset: %v", err)
	}
	if control.currentGeneration() == stale {
		testContext.Fatal("the second reset did not advance the generation")
	}

	driver := &reverseDriver{ctx: context.Background(), cpuStatPath: control.cpuStatPath}
	control.runReverseCohort(driver, reverseRun{specification: specification, reverseControl: peer.URL, generation: stale})

	if control.reverseSender != nil || control.reverseReceiver != nil || control.reverseError != "" {
		testContext.Fatalf("an ended cohort's reverse run landed in the new cohort: sender recorded %t, receiver recorded %t, error %q",
			control.reverseSender != nil, control.reverseReceiver != nil, control.reverseError)
	}
	record := control.result()
	if record.Reverse != nil || record.ReverseReceiver != nil || record.ReverseError != "" {
		testContext.Fatalf("the new cohort's record carries the ended cohort's reverse evidence: %+v", record)
	}
}

// start must capture the generation, not leave the completion to read
// whichever generation is current when it happens to finish. The peer holds
// the reverse driver's first control call while the receiver is reset onto a
// new cohort, so the completion lands strictly after the generation advanced.
func TestStartCapturesTheGenerationTheReverseCohortBelongsTo(testContext *testing.T) {
	control := newReceiverControl(1, 16)
	control.setAssociationReady(0, 15)
	control.cpuStatPath = writeCPUStatFixture(testContext, "usage_usec 10\nnr_throttled 0\n")

	entered := make(chan struct{})
	advanced := make(chan struct{})
	var once sync.Once
	peer := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		once.Do(func() { close(entered) })
		<-advanced
		http.Error(writer, "conflict", http.StatusConflict)
	}))
	defer peer.Close()

	control.reverseControl = peer.URL
	control.driver = &reverseDriver{ctx: context.Background(), cpuStatPath: control.cpuStatPath}
	specification := runSpec{
		Cohort: "bidi", Seed: 1, Associations: 1, Expected: 2, Duration: time.Second,
		Drain: 2 * time.Second, Outstanding: maxOutstanding, Rate: 2, Payload: workload128,
		Mode: modeBidirectional, Direction: directionASPToSGP, PeerControl: peer.URL,
	}
	if err := control.reset(specification); err != nil {
		testContext.Fatalf("reset: %v", err)
	}
	if err := control.start(); err != nil {
		testContext.Fatalf("start: %v", err)
	}
	<-entered
	if err := control.stop(); err != nil {
		testContext.Fatalf("stop: %v", err)
	}
	if err := control.reset(specification); err != nil {
		testContext.Fatalf("second reset: %v", err)
	}
	close(advanced)

	// The in-flight cohort now completes: the peer has answered and the
	// driver returns straight into its completion path, so a second is far
	// more than the store would need. Its outcome must never appear in the
	// cohort that replaced it.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		control.mutex.Lock()
		sender, receiver, reverseErr := control.reverseSender, control.reverseReceiver, control.reverseError
		control.mutex.Unlock()
		if sender != nil || receiver != nil || reverseErr != "" {
			testContext.Fatalf("the previous cohort's reverse run landed in the new cohort: sender recorded %t, receiver recorded %t, error %q",
				sender != nil, receiver != nil, reverseErr)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// The counterpart to discarding a stale completion: a reverse cohort that
// finishes while its own cohort is still current must be recorded in full,
// errors included. Discarding everything would hide a failed reverse
// direction behind an apparently clean forward one.
func TestReverseCohortCompletionForTheActiveCohortIsRecorded(testContext *testing.T) {
	peer, _ := countingPeer(testContext)
	control := newReceiverControl(1, 16)
	control.setAssociationReady(0, 15)
	control.cpuStatPath = writeCPUStatFixture(testContext, "usage_usec 10\nnr_throttled 0\n")
	control.reverseControl = peer.URL
	control.driver = &reverseDriver{ctx: context.Background(), cpuStatPath: control.cpuStatPath}
	specification := runSpec{
		Cohort: "bidi", Seed: 1, Associations: 1, Expected: 2, Duration: time.Second,
		Drain: 2 * time.Second, Outstanding: maxOutstanding, Rate: 2, Payload: workload128,
		Mode: modeBidirectional, Direction: directionASPToSGP, PeerControl: peer.URL,
	}
	if err := control.reset(specification); err != nil {
		testContext.Fatalf("reset: %v", err)
	}
	if err := control.start(); err != nil {
		testContext.Fatalf("start: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		control.mutex.Lock()
		sender, receiver, reverseErr := control.reverseSender, control.reverseReceiver, control.reverseError
		control.mutex.Unlock()
		if sender != nil && receiver != nil && reverseErr != "" {
			return
		}
		if !time.Now().Before(deadline) {
			testContext.Fatalf("the active cohort's reverse run was never recorded: sender recorded %t, receiver recorded %t, error %q",
				sender != nil, receiver != nil, reverseErr)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
