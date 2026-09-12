package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func TestReceiverControlRequiresResetBeforeStartAndStopAfterStart(testContext *testing.T) {
	control := newReceiverControl(2, 16)
	control.setAssociationReady(0, 15)
	control.setAssociationReady(1, 15)
	server := httptest.NewServer(control.handler())
	defer server.Close()

	requireHTTPStatus(testContext, http.MethodPost, server.URL+"/start", nil, http.StatusConflict)
	specification := runSpec{
		Cohort:       "cohort-a",
		Seed:         7,
		Associations: 2,
		Expected:     10,
		Duration:     time.Second,
		Payload:      workload128,
		Rate:         10,
	}
	body, err := json.Marshal(specification)
	if err != nil {
		testContext.Fatalf("Marshal: %v", err)
	}
	requireHTTPStatus(testContext, http.MethodPost, server.URL+"/reset", body, http.StatusNoContent)
	requireHTTPStatus(testContext, http.MethodPost, server.URL+"/start", nil, http.StatusNoContent)
	requireHTTPStatus(testContext, http.MethodPost, server.URL+"/reset", body, http.StatusConflict)
	requireHTTPStatus(testContext, http.MethodPost, server.URL+"/stop", nil, http.StatusNoContent)
}

func TestReceiverControlRejectsUnboundedAssociationCountsAtHTTPBoundary(testContext *testing.T) {
	control := newReceiverControl(1, 16)
	control.setAssociationReady(0, 15)
	server := httptest.NewServer(control.handler())
	defer server.Close()

	for _, associations := range []int{0, maxAssociations + 1, int(^uint(0) >> 1)} {
		specification := runSpec{
			Cohort:       "malicious",
			Seed:         1,
			Associations: associations,
			Expected:     1,
			Duration:     time.Second,
			Rate:         1,
			Payload:      workload128,
		}
		body, err := json.Marshal(specification)
		if err != nil {
			testContext.Fatalf("Marshal: %v", err)
		}
		requireHTTPStatus(testContext, http.MethodPost, server.URL+"/reset", body, http.StatusBadRequest)
		if control.ledger != nil {
			testContext.Fatal("invalid association count allocated a receive ledger")
		}
	}
}

func TestReceiverControlSeparatesMeasurementAndDrainDeliveries(testContext *testing.T) {
	now := time.Unix(100, 0)
	control := newReceiverControl(1, 16)
	control.setAssociationReady(0, 15)
	control.cpuStatPath = writeCPUStatFixture(testContext, "usage_usec 10\nnr_throttled 0\n")
	control.now = func() time.Time { return now }
	if err := control.reset(runSpec{Cohort: "cohort-a", Seed: 7, Associations: 1, Expected: 2, Duration: time.Second, Payload: workload128, Rate: 2}); err != nil {
		testContext.Fatalf("reset: %v", err)
	}
	if err := control.start(); err != nil {
		testContext.Fatalf("start: %v", err)
	}
	control.record(0, validReceivedMessage("cohort-a", 7, 0, 0, 0, 128))
	now = now.Add(time.Second + time.Nanosecond)
	control.record(0, validReceivedMessage("cohort-a", 7, 0, 1, 0, 128))
	if err := os.WriteFile(control.cpuStatPath, []byte("usage_usec 20\nnr_throttled 0\n"), 0o600); err != nil {
		testContext.Fatalf("WriteFile final cpu.stat: %v", err)
	}
	if err := control.stop(); err != nil {
		testContext.Fatalf("stop: %v", err)
	}
	record := control.result()
	if record.Delivery.UniqueMeasurement != 1 || record.Delivery.UniqueDrain != 1 {
		testContext.Fatalf("delivery = %+v", record.Delivery)
	}
	if record.Verdict != verdictInconclusive || record.FixtureVerdict != verdictPass || record.CapacityVerdict != "unavailable" {
		testContext.Fatalf("record = %+v", record)
	}
}

func writeCPUStatFixture(testContext *testing.T, contents string) string {
	testContext.Helper()
	path := testContext.TempDir() + "/cpu.stat"
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		testContext.Fatalf("WriteFile: %v", err)
	}
	return path
}

func TestReceiverControlBindsLogicalAssociationToOneTransport(testContext *testing.T) {
	control := newReceiverControl(2, 16)
	control.setAssociationReady(0, 15)
	control.setAssociationReady(1, 15)
	if err := control.reset(runSpec{Cohort: "cohort-a", Seed: 7, Associations: 2, Expected: 2, Duration: time.Second, Payload: workload128, Rate: 2}); err != nil {
		testContext.Fatalf("reset: %v", err)
	}
	if err := control.start(); err != nil {
		testContext.Fatalf("start: %v", err)
	}
	control.record(0, validReceivedMessage("cohort-a", 7, 0, 0, 0, 128))
	control.record(1, validReceivedMessage("cohort-a", 7, 0, 0, 1, 128))
	result := control.result()
	if result.Delivery.Invalid != 1 {
		testContext.Fatalf("invalid = %d, want 1", result.Delivery.Invalid)
	}
}

func TestReadyReportsFatalReadFailure(testContext *testing.T) {
	control := newReceiverControl(1, 16)
	control.setAssociationReady(0, 15)
	control.setFatal("association 0: unexpected read failure")
	ready := control.ready()
	if ready.Ready || ready.Error == "" || ready.NegotiatedOutboundStreams != 16 {
		testContext.Fatalf("ready = %+v", ready)
	}
}

func requireHTTPStatus(testContext *testing.T, method, url string, body []byte, want int) {
	testContext.Helper()
	request, err := http.NewRequest(method, url, bytes.NewReader(body))
	if err != nil {
		testContext.Fatalf("NewRequest: %v", err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		testContext.Fatalf("Do: %v", err)
	}
	defer func() {
		if err := response.Body.Close(); err != nil {
			testContext.Errorf("Close response body: %v", err)
		}
	}()
	if response.StatusCode != want {
		testContext.Fatalf("status = %d, want %d", response.StatusCode, want)
	}
}
