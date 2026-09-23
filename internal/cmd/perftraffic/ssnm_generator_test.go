package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua"
)

// wallMeasurementClock is a shared clock that advances with real time from a
// large positive origin.
type wallMeasurementClock struct {
	origin time.Time
}

func (clock wallMeasurementClock) Now() (int64, error) {
	return int64(time.Second) + int64(time.Since(clock.origin)), nil
}

func (wallMeasurementClock) Domain() (sharedClockDomain, error) {
	return sharedClockDomain{Clock: "CLOCK_MONOTONIC", BootID: "test", TimeNamespace: "time:[1]", Resolution: 1}, nil
}

type recordingReporter struct {
	mutex    sync.Mutex
	requests []m3ua.DestinationAvailabilityRequest
	fail     func(index int) error
}

func (reporter *recordingReporter) ReportDestinationAvailability(request m3ua.DestinationAvailabilityRequest) error {
	reporter.mutex.Lock()
	defer reporter.mutex.Unlock()
	index := len(reporter.requests)
	request.Destinations = append([]m3ua.PointCodeRange(nil), request.Destinations...)
	reporter.requests = append(reporter.requests, request)
	if reporter.fail != nil {
		return reporter.fail(index)
	}
	return nil
}

func (reporter *recordingReporter) snapshot() []m3ua.DestinationAvailabilityRequest {
	reporter.mutex.Lock()
	defer reporter.mutex.Unlock()
	return append([]m3ua.DestinationAvailabilityRequest(nil), reporter.requests...)
}

func testGenerator(testContext *testing.T, rate uint64, apcs, records int, clock measurementClock, reporter ssnmReporter) *ssnmGenerator {
	testContext.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	testContext.Cleanup(cancel)
	generator := newSSNMGenerator(ctx, commandConfig{SSNM: ssnmConfig{Rate: rate, APCs: apcs, Records: records}}, clock)
	generator.setReporter(reporter)
	return generator
}

func TestSSNMGeneratorPreloadFillsEveryDestinationOnce(testContext *testing.T) {
	reporter := &recordingReporter{}
	generator := testGenerator(testContext, 10, 1024, 2500, wallMeasurementClock{origin: time.Now()}, reporter)
	if err := generator.preloadStore(); err != nil {
		testContext.Fatal(err)
	}
	requests := reporter.snapshot()
	if len(requests) != 3 || len(requests[0].Destinations) != 1024 || len(requests[2].Destinations) != 452 {
		testContext.Fatalf("preload requests %d", len(requests))
	}
	seen := make(map[uint32]bool)
	for _, request := range requests {
		if request.Availability != m3ua.DestinationUnavailable || !request.Scope.RoutingContextSet || request.Scope.RoutingContexts[0] != ssnmRoutingContext || request.Scope.NetworkAppearance != testNetworkAppearance {
			testContext.Fatalf("preload request %+v", request)
		}
		for _, destination := range request.Destinations {
			if seen[destination.PointCode] || destination.PointCode < ssnmPointCodeBase || destination.PointCode >= ssnmPointCodeBase+2500 {
				testContext.Fatalf("destination %#x repeated or out of range", destination.PointCode)
			}
			seen[destination.PointCode] = true
		}
	}
	if err := generator.preloadStore(); err == nil {
		testContext.Fatal("a second preload was accepted")
	}
}

func armedSpec(generator *ssnmGenerator, phase string, start, end, anchor int64) runSpec {
	return runSpec{
		Clock: &sharedClockWindow{Start: start, End: end},
		Drain: time.Second,
		SSNM:  ssnmConfig{Rate: generator.rate, APCs: generator.apcs, Records: generator.records, Subscribers: 8}.workload(phase, anchor),
	}
}

func TestSSNMGeneratorAcceptSpecRules(testContext *testing.T) {
	clock := wallMeasurementClock{origin: time.Now()}
	generator := testGenerator(testContext, 1000, 1, 64, clock, &recordingReporter{})
	start := int64(10 * time.Second)
	if err := generator.acceptSpec(armedSpec(generator, ssnmPhaseWarmup, start, start+int64(time.Second), start)); err == nil || !strings.Contains(err.Error(), "preloaded") {
		testContext.Fatalf("unpreloaded store accepted: %v", err)
	}
	if err := generator.preloadStore(); err != nil {
		testContext.Fatal(err)
	}
	mismatch := armedSpec(generator, ssnmPhaseWarmup, start, start+int64(time.Second), start)
	mismatch.SSNM.APCs = 2
	if err := generator.acceptSpec(mismatch); err == nil {
		testContext.Fatal("mismatched workload accepted")
	}
	if err := generator.acceptSpec(runSpec{Clock: mismatch.Clock}); err == nil {
		testContext.Fatal("cohort without SSNM accepted by an SSNM receiver")
	}
	if err := (*ssnmGenerator)(nil).acceptSpec(armedSpec(generator, ssnmPhaseWarmup, start, start+1, start)); err == nil {
		testContext.Fatal("SSNM cohort accepted by a receiver without SSNM load")
	}
	if err := (*ssnmGenerator)(nil).acceptSpec(runSpec{}); err != nil {
		testContext.Fatalf("plain cohort rejected: %v", err)
	}
	if err := generator.acceptSpec(armedSpec(generator, ssnmPhaseWarmup, start, start+int64(time.Second), start+1)); err == nil {
		testContext.Fatal("first cohort anchored away from its start accepted")
	}
	if err := generator.acceptSpec(armedSpec(generator, "other", start, start+int64(time.Second), start)); err == nil {
		testContext.Fatal("unknown phase accepted")
	}
	if err := generator.acceptSpec(armedSpec(generator, ssnmPhaseWarmup, start, start+int64(time.Second), start)); err != nil {
		testContext.Fatalf("warm-up rejected: %v", err)
	}
	generator.started = true
	measurementStart := start + int64(3*time.Second)
	if err := generator.acceptSpec(armedSpec(generator, ssnmPhaseMeasurement, measurementStart, measurementStart+int64(time.Second), measurementStart)); err == nil {
		testContext.Fatal("measurement with a new anchor accepted")
	}
	if err := generator.acceptSpec(armedSpec(generator, ssnmPhaseMeasurement, measurementStart, measurementStart+int64(time.Second), start)); err != nil {
		testContext.Fatalf("measurement rejected: %v", err)
	}
	if generator.end != measurementStart+int64(time.Second) || generator.stopAt != generator.end+int64(time.Second) {
		testContext.Fatalf("end %d stopAt %d", generator.end, generator.stopAt)
	}
	if err := generator.acceptSpec(armedSpec(generator, ssnmPhaseMeasurement, measurementStart, measurementStart+int64(time.Second), start)); err == nil {
		testContext.Fatal("a second measurement cohort accepted")
	}
}

// TestSSNMGeneratorRunsOpenLoopSchedule runs the generator against a real-time
// clock and checks the delivered sequence, the window accounting and the
// per-message log, concurrently with record reads for the race detector.
func TestSSNMGeneratorRunsOpenLoopSchedule(testContext *testing.T) {
	clock := wallMeasurementClock{origin: time.Now()}
	reporter := &recordingReporter{}
	generator := testGenerator(testContext, 200, 2, 16, clock, reporter)
	if err := generator.preloadStore(); err != nil {
		testContext.Fatal(err)
	}
	now, _ := clock.Now()
	anchor := now + int64(50*time.Millisecond)
	end := anchor + int64(300*time.Millisecond)
	warmup := armedSpec(generator, ssnmPhaseWarmup, anchor, anchor+int64(100*time.Millisecond), anchor)
	if err := generator.acceptSpec(warmup); err != nil {
		testContext.Fatal(err)
	}
	generator.begin()
	measurement := armedSpec(generator, ssnmPhaseMeasurement, anchor+int64(100*time.Millisecond), end, anchor)
	measurement.Clock.End = end
	if err := generator.acceptSpec(measurement); err != nil {
		testContext.Fatal(err)
	}
	stopReading := make(chan struct{})
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			select {
			case <-stopReading:
				return
			default:
				_ = generator.cohortRecord(measurement)
				_, _ = generator.reportsRange(0, 100)
			}
		}
	}()
	select {
	case <-generator.done:
	case <-time.After(5 * time.Second):
		testContext.Fatal("generator did not complete")
	}
	close(stopReading)
	<-readerDone
	requests := reporter.snapshot()
	preload := int(generator.plan.preloadMessages())
	if len(requests) != preload+60 {
		testContext.Fatalf("generator reported %d messages after the preload, want 60", len(requests)-preload)
	}
	for index, request := range requests[preload:] {
		chunk := generator.plan.chunk(uint64(preload + index))
		if request.Availability != chunk.availability || request.Destinations[0].PointCode != ssnmPointCodeBase+uint32(chunk.first) || len(request.Destinations) != 2 {
			testContext.Fatalf("message %d = %+v, want %+v", index, request, chunk)
		}
	}
	record := generator.cohortRecord(measurement)
	if record.State != ssnmGeneratorComplete || record.FirstMessage != 20 || record.EndMessage != 60 || record.Offered != 40 || record.SentTotal != 60 {
		testContext.Fatalf("window record = %+v", record)
	}
	if record.Unsent != 0 || record.Failed != 0 || record.DispatchLag.Count != 40 {
		testContext.Fatalf("window accounting = %+v", record)
	}
	log, err := generator.reportsRange(0, 1000)
	if err != nil || log.SentTotal != 60 || len(log.Reports) != 60 || len(log.Completions) != 60 {
		testContext.Fatalf("log = %+v err = %v", log, err)
	}
	for message, reported := range log.Reports {
		if reported < anchor+ssnmScheduled(200, uint64(message)) || log.Completions[message] < reported {
			testContext.Fatalf("message %d reported at %d before its schedule or completed before start", message, reported)
		}
	}
}

func TestSSNMGeneratorCountsFanoutFailures(testContext *testing.T) {
	clock := wallMeasurementClock{origin: time.Now()}
	reporter := &recordingReporter{}
	generator := testGenerator(testContext, 1000, 1, 16, clock, reporter)
	reporter.fail = func(index int) error {
		if index == 3 {
			return &m3ua.SSNMDeliveryError{Failed: []m3ua.SSNMDeliveryFailure{{Association: 1, Cause: errors.New("closed")}, {Association: 2, Cause: errors.New("closed")}}}
		}
		return nil
	}
	for message := uint64(0); message < 5; message++ {
		generator.send(reporter, message, 0)
	}
	if generator.failedMessages != 1 || generator.fanoutFailures != 2 || generator.statuses[3] != ssnmMessageFailed || !strings.Contains(generator.firstError, "message 3") {
		testContext.Fatalf("failures %d fanout %d statuses %v first %q", generator.failedMessages, generator.fanoutFailures, generator.statuses, generator.firstError)
	}
	log, err := generator.reportsRange(0, 5)
	if err != nil || len(log.Failed) != 1 || log.Failed[0] != 3 {
		testContext.Fatalf("log failed messages %v err %v", log.Failed, err)
	}
	if _, err := generator.reportsRange(5, 4); err == nil {
		testContext.Fatal("descending range accepted")
	}
}

func TestSSNMWindowSummaryClassifiesMessages(testContext *testing.T) {
	record := &ssnmGeneratorRecord{AnchorNS: 0, WindowStartNS: int64(time.Second), WindowEndNS: int64(2 * time.Second)}
	// Rate 4: window messages are 4..7. Message 5 failed, message 7 was late,
	// message 8 exists and message 9 onward was never sent.
	reports := []int64{0, 250e6, 500e6, 750e6, 1000e6, 1250e6, 1500e6, 2400e6}
	completions := append([]int64(nil), reports...)
	statuses := []uint8{1, 1, 1, 1, 1, 2, 1, 1}
	summarizeSSNMWindow(record, 4, reports, completions, statuses)
	if record.FirstMessage != 4 || record.EndMessage != 8 || record.Offered != 4 || record.Failed != 1 || record.Late != 1 || record.Unsent != 0 || record.ReportedInWindow != 2 || record.IntensityHeld {
		testContext.Fatalf("summary = %+v", record)
	}
	// A report started just after the window within one interval is late
	// but holds the intensity; one lagging beyond the tolerance does not.
	onTime := &ssnmGeneratorRecord{WindowStartNS: int64(time.Second), WindowEndNS: int64(2 * time.Second)}
	boundary := []int64{0, 250e6, 500e6, 750e6, 1000e6, 1250e6, 1500e6, 2000e6}
	summarizeSSNMWindow(onTime, 4, boundary, boundary, []uint8{1, 1, 1, 1, 1, 1, 1, 1})
	if onTime.Late != 1 || !onTime.IntensityHeld {
		testContext.Fatalf("boundary summary = %+v", onTime)
	}
	short := &ssnmGeneratorRecord{WindowStartNS: int64(time.Second), WindowEndNS: int64(2 * time.Second)}
	summarizeSSNMWindow(short, 4, reports[:6], completions[:6], statuses[:6])
	if short.Unsent != 2 || short.IntensityHeld {
		testContext.Fatalf("unsent summary = %+v", short)
	}
}

func TestSSNMControlRoutesOnlyWithGenerator(testContext *testing.T) {
	plain := newReceiverControl(1, 8)
	server := httptest.NewServer(plain.handler())
	defer server.Close()
	response, err := http.Post(server.URL+"/ssnm/preload", "application/json", nil)
	if err != nil {
		testContext.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode == http.StatusNoContent {
		testContext.Fatal("receiver without SSNM load serves /ssnm/preload")
	}
	loaded := newReceiverControl(1, 8)
	loaded.ssnm = testGenerator(testContext, 10, 1, 16, wallMeasurementClock{origin: time.Now()}, &recordingReporter{})
	loadedServer := httptest.NewServer(loaded.handler())
	defer loadedServer.Close()
	response, err = http.Post(loadedServer.URL+"/ssnm/preload", "application/json", nil)
	if err != nil {
		testContext.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		testContext.Fatalf("preload status %d", response.StatusCode)
	}
	log, err := getSSNMReports(context.Background(), loadedServer.URL, 0, 10)
	if err != nil || log.State != ssnmGeneratorIdle || log.SentTotal != 0 {
		testContext.Fatalf("reports = %+v err = %v", log, err)
	}
	response, err = http.Get(loadedServer.URL + "/ssnm/reports?from=x&to=1")
	if err != nil {
		testContext.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		testContext.Fatalf("malformed range status %d", response.StatusCode)
	}
}
