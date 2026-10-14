package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua"
	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// manualClock is a shared clock that moves only when a test or the
// generator's injected sleep advances it, so every schedule assertion is
// exact and no wall-clock time is involved.
type manualClock struct {
	mutex sync.Mutex
	now   int64
}

func (clock *manualClock) Now() (int64, error) {
	clock.mutex.Lock()
	defer clock.mutex.Unlock()
	return clock.now, nil
}

func (*manualClock) Domain() (sharedClockDomain, error) {
	return sharedClockDomain{Clock: "CLOCK_MONOTONIC", BootID: "test", TimeNamespace: "time:[1]", Resolution: 1}, nil
}

func (clock *manualClock) advance(duration time.Duration) {
	clock.mutex.Lock()
	defer clock.mutex.Unlock()
	clock.now += int64(duration)
}

// ssnmWrite is one message a recordingWriter accepted.
type ssnmWrite struct {
	at           int64
	availability m3ua.DestinationAvailability
	pointCodes   []uint32
	masked       bool
	message      messages.M3UA
}

// recordingWriter is one SGP association: it stamps every accepted message
// with the clock and can refuse writes.
type recordingWriter struct {
	clock  measurementClock
	mutex  sync.Mutex
	writes []ssnmWrite
	calls  int
	// refuse returns the error for the call-th write attempt, nil to accept.
	refuse func(call int) error
}

func (writer *recordingWriter) WriteSignal(message messages.M3UA) (int, error) {
	writer.mutex.Lock()
	defer writer.mutex.Unlock()
	call := writer.calls
	writer.calls++
	if writer.refuse != nil {
		if err := writer.refuse(call); err != nil {
			return 0, err
		}
	}
	at, _ := writer.clock.Now()
	write := ssnmWrite{at: at, message: message}
	var affected *params.Param
	switch typed := message.(type) {
	case *messages.DestinationUnavailable:
		write.availability, affected = m3ua.DestinationUnavailable, typed.AffectedPointCode
	case *messages.DestinationAvailable:
		write.availability, affected = m3ua.DestinationAvailable, typed.AffectedPointCode
	}
	if affected != nil {
		write.pointCodes = affected.AffectedPointCodes()
		for _, mask := range affected.AffectedPointCodeMasks() {
			write.masked = write.masked || mask != 0
		}
	}
	writer.writes = append(writer.writes, write)
	return message.MarshalLen(), nil
}

func (writer *recordingWriter) snapshot() []ssnmWrite {
	writer.mutex.Lock()
	defer writer.mutex.Unlock()
	return append([]ssnmWrite(nil), writer.writes...)
}

// testGenerator builds a generator over associations recording writers. The
// generator's sleeps advance clock when it is a manualClock.
func testGenerator(testContext *testing.T, rate uint64, apcs, records, associations int, clock measurementClock) (*ssnmGenerator, []*recordingWriter) {
	testContext.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	testContext.Cleanup(cancel)
	generator := newSSNMGenerator(ctx, commandConfig{Associations: associations, SSNM: ssnmConfig{TotalRate: rate, APCs: apcs, Records: records}}, clock)
	if manual, stepped := clock.(*manualClock); stepped {
		generator.sleep = func(_ context.Context, duration time.Duration) { manual.advance(duration) }
	}
	writers := make([]*recordingWriter, associations)
	for index := range writers {
		writers[index] = &recordingWriter{clock: clock}
		generator.track(index, writers[index])
	}
	return generator, writers
}

// preloadAll runs every preload step in order.
func preloadAll(testContext *testing.T, generator *ssnmGenerator) {
	testContext.Helper()
	for step := uint64(0); step < generator.plan.preloadSteps(); step++ {
		if err := generator.preloadStep(step); err != nil {
			testContext.Fatalf("preload step %d: %v", step, err)
		}
	}
}

// checkWrite asserts one written message: its kind, scope and the point codes
// of chunk on association's partition.
func checkWrite(testContext *testing.T, plan ssnmPlan, write ssnmWrite, association int, chunk ssnmChunk) {
	testContext.Helper()
	if write.availability != chunk.availability || len(write.pointCodes) != chunk.count || write.masked {
		testContext.Fatalf("write %+v, want %d APCs %v", write, chunk.count, chunk.availability)
	}
	for index, pointCode := range write.pointCodes {
		if pointCode != plan.pointCode(association, chunk.first+index) {
			testContext.Fatalf("APC %d = %#x, want %#x (association %d destination %d)", index, pointCode, plan.pointCode(association, chunk.first+index), association, chunk.first+index)
		}
	}
	var networkAppearance, routingContext uint32
	var info bool
	switch typed := write.message.(type) {
	case *messages.DestinationUnavailable:
		networkAppearance, routingContext, info = typed.NetworkAppearance.NetworkAppearance(), typed.RoutingContext.RoutingContext(), typed.InfoString != nil
	case *messages.DestinationAvailable:
		networkAppearance, routingContext, info = typed.NetworkAppearance.NetworkAppearance(), typed.RoutingContext.RoutingContext(), typed.InfoString != nil
	}
	if networkAppearance != testNetworkAppearance || routingContext != ssnmRoutingContext || info {
		testContext.Fatalf("message scope NA %d RC %d info %t, want NA %d RC %d and no info", networkAppearance, routingContext, info, testNetworkAppearance, ssnmRoutingContext)
	}
}

// The preload is requested one message at a time, association-major: each
// step writes one partition's destinations on that association only.
func TestSSNMGeneratorPreloadStepsFillEachPartitionOnItsAssociation(testContext *testing.T) {
	generator, writers := testGenerator(testContext, 10, 1024, 2500, 2, &manualClock{now: int64(time.Second)})
	if err := generator.preloadStep(1); err == nil || !strings.Contains(err.Error(), "out of order") {
		testContext.Fatalf("out-of-order step: %v", err)
	}
	preloadAll(testContext, generator)
	plan := generator.plan
	for association, writer := range writers {
		writes := writer.snapshot()
		if len(writes) != 3 {
			testContext.Fatalf("association %d received %d preload messages, want 3", association, len(writes))
		}
		for position, write := range writes {
			checkWrite(testContext, plan, write, association, plan.chunk(uint64(position)))
		}
	}
	if record := generator.preload; record.Messages != 6 || record.Updates != 5000 || record.Failed != 0 || record.CompletedAtNS == 0 {
		testContext.Fatalf("preload record %+v", record)
	}
	if err := generator.preloadStep(6); err == nil || !strings.Contains(err.Error(), "already preloaded") {
		testContext.Fatalf("a step after the preload was accepted: %v", err)
	}
}

func TestSSNMGeneratorPreloadNeedsEveryAssociation(testContext *testing.T) {
	generator := newSSNMGenerator(context.Background(), commandConfig{Associations: 2, SSNM: ssnmConfig{TotalRate: 10, APCs: 1, Records: 8}}, &manualClock{now: int64(time.Second)})
	generator.track(0, &recordingWriter{clock: &manualClock{now: int64(time.Second)}})
	if err := generator.preloadStep(0); err == nil || !strings.Contains(err.Error(), "not ready") {
		testContext.Fatalf("preload with one of two associations: %v", err)
	}
	failing, writers := testGenerator(testContext, 10, 1, 8, 2, &manualClock{now: int64(time.Second)})
	writers[1].refuse = func(int) error { return errors.New("closed") }
	if err := failing.preloadStep(0); err != nil {
		testContext.Fatal(err)
	}
	if err := failing.preloadStep(1); err == nil {
		testContext.Fatal("a failed preload write was reported as success")
	}
	if err := failing.preloadStep(1); err == nil || !strings.Contains(err.Error(), "already failed") {
		testContext.Fatalf("preload continued after a failure: %v", err)
	}
	if err := failing.acceptSpec(armedSpec(failing, ssnmPhaseWarmup, 10, 20, 10)); err == nil || !strings.Contains(err.Error(), "preloaded") {
		testContext.Fatalf("failed preload accepted: %v", err)
	}
}

func armedSpec(generator *ssnmGenerator, phase string, start, end, anchor int64) runSpec {
	return runSpec{
		Associations: generator.associations,
		Clock:        &sharedClockWindow{Start: start, End: end},
		Drain:        time.Second,
		SSNM:         workloadRef(ssnmConfig{TotalRate: generator.rate, APCs: generator.apcs, Records: generator.records, Subscribers: 8}.workload(phase, anchor)),
	}
}

func TestSSNMGeneratorAcceptSpecRules(testContext *testing.T) {
	generator, _ := testGenerator(testContext, 1000, 1, 64, 2, &manualClock{now: int64(time.Second)})
	start := int64(10 * time.Second)
	if err := generator.acceptSpec(armedSpec(generator, ssnmPhaseWarmup, start, start+int64(time.Second), start)); err == nil || !strings.Contains(err.Error(), "preloaded") {
		testContext.Fatalf("unpreloaded store accepted: %v", err)
	}
	preloadAll(testContext, generator)
	mismatch := armedSpec(generator, ssnmPhaseWarmup, start, start+int64(time.Second), start)
	mismatch.SSNM.APCs = 2
	if err := generator.acceptSpec(mismatch); err == nil {
		testContext.Fatal("mismatched workload accepted")
	}
	otherRate := armedSpec(generator, ssnmPhaseWarmup, start, start+int64(time.Second), start)
	otherRate.SSNM.TotalRate = 125
	if err := generator.acceptSpec(otherRate); err == nil || !strings.Contains(err.Error(), "in total") {
		testContext.Fatalf("mismatched total rate accepted: %v", err)
	}
	otherAssociations := armedSpec(generator, ssnmPhaseWarmup, start, start+int64(time.Second), start)
	otherAssociations.Associations = 1
	if err := generator.acceptSpec(otherAssociations); err == nil || !strings.Contains(err.Error(), "associations") {
		testContext.Fatalf("cohort over another association count accepted: %v", err)
	}
	if err := generator.acceptSpec(runSpec{Associations: 2, Clock: mismatch.Clock}); err == nil {
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

// runSchedule preloads the generator, arms a warm-up of warmup and a
// measurement of measurement from the clock's current instant, and runs the
// open-loop schedule to completion on the injected clock.
func runSchedule(testContext *testing.T, generator *ssnmGenerator, clock *manualClock, warmup, measurement time.Duration) (int64, runSpec) {
	testContext.Helper()
	preloadAll(testContext, generator)
	anchor, _ := clock.Now()
	warm := armedSpec(generator, ssnmPhaseWarmup, anchor, anchor+int64(warmup), anchor)
	if err := generator.acceptSpec(warm); err != nil {
		testContext.Fatal(err)
	}
	generator.started = true
	generator.state = ssnmGeneratorRunning
	specification := armedSpec(generator, ssnmPhaseMeasurement, anchor+int64(warmup), anchor+int64(warmup+measurement), anchor)
	if err := generator.acceptSpec(specification); err != nil {
		testContext.Fatal(err)
	}
	generator.run()
	return anchor, specification
}

// TestSSNMGeneratorRoundRobinSchedule runs the open-loop generator on an
// injected clock and checks, for one and eight associations at 1,000/s and
// 10/s, that message m was written on association m mod N only, at exactly
// its scheduled instant, carrying its partition position's content, and that
// the window offered rate x duration messages.
func TestSSNMGeneratorRoundRobinSchedule(testContext *testing.T) {
	for _, testCase := range []struct {
		rate         uint64
		associations int
		apcs         int
		warmup       time.Duration
		measurement  time.Duration
	}{
		{1000, 1, 1, 200 * time.Millisecond, time.Second},
		{1000, 8, 1, 200 * time.Millisecond, time.Second},
		{10, 1, 1024, time.Second, 3 * time.Second},
		{10, 8, 1024, time.Second, 3 * time.Second},
	} {
		testContext.Run(fmt.Sprintf("%d per second over %d", testCase.rate, testCase.associations), func(testContext *testing.T) {
			clock := &manualClock{now: int64(5 * time.Second)}
			generator, writers := testGenerator(testContext, testCase.rate, testCase.apcs, 2048, testCase.associations, clock)
			anchor, specification := runSchedule(testContext, generator, clock, testCase.warmup, testCase.measurement)
			plan := generator.plan
			preload := plan.preloadMessages()
			total := testCase.rate * uint64((testCase.warmup+testCase.measurement)/time.Millisecond) / 1000
			var written uint64
			for association, writer := range writers {
				writes := writer.snapshot()[preload:]
				want := plan.messagesFor(association, total)
				if uint64(len(writes)) != want {
					testContext.Fatalf("association %d received %d generator messages, want %d", association, len(writes), want)
				}
				for index, write := range writes {
					message := uint64(index)*uint64(testCase.associations) + uint64(association)
					if at := write.at - anchor; at != ssnmScheduled(testCase.rate, message) {
						testContext.Fatalf("message %d on association %d written at +%s, want +%s", message, association, time.Duration(at), time.Duration(ssnmScheduled(testCase.rate, message)))
					}
					checkWrite(testContext, plan, write, association, plan.chunk(preload+uint64(index)))
				}
				written += uint64(len(writes))
			}
			if written != total {
				testContext.Fatalf("wrote %d generator messages, want %d", written, total)
			}
			record := generator.cohortRecord(specification)
			windowMessages := testCase.rate * uint64(testCase.measurement/time.Millisecond) / 1000
			if record.State != ssnmGeneratorComplete || record.Offered != windowMessages || record.SentTotal != total || record.Associations != testCase.associations ||
				record.Unsent != 0 || record.Failed != 0 || record.Late != 0 || record.DispatchLag.Max != 0 || !record.IntensityHeld || record.ReportedInWindow != windowMessages {
				testContext.Fatalf("window record = %+v", record)
			}
			var offered uint64
			for association, share := range record.OfferedPerAssociation {
				if want := plan.messagesFor(association, record.EndMessage) - plan.messagesFor(association, record.FirstMessage); share != want {
					testContext.Fatalf("association %d offered %d, want %d", association, share, want)
				}
				offered += share
			}
			if offered != windowMessages || len(record.OfferedPerAssociation) != testCase.associations {
				testContext.Fatalf("per-association offers %v add up to %d, want %d", record.OfferedPerAssociation, offered, windowMessages)
			}
		})
	}
}

// The generator reads its own record concurrently with the schedule, as the
// control endpoint does, under the race detector.
func TestSSNMGeneratorRecordReadsDuringRun(testContext *testing.T) {
	clock := &manualClock{now: int64(5 * time.Second)}
	generator, _ := testGenerator(testContext, 1000, 1, 64, 4, clock)
	preloadAll(testContext, generator)
	anchor, _ := clock.Now()
	specification := armedSpec(generator, ssnmPhaseMeasurement, anchor, anchor+int64(300*time.Millisecond), anchor)
	if err := generator.acceptSpec(specification); err != nil {
		testContext.Fatal(err)
	}
	generator.begin()
	stopReading := make(chan struct{})
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			select {
			case <-stopReading:
				return
			default:
				_ = generator.cohortRecord(specification)
				_, _ = generator.reportsRange(0, 100)
			}
		}
	}()
	select {
	case <-generator.done:
	case <-time.After(10 * time.Second):
		testContext.Fatal("generator did not complete")
	}
	close(stopReading)
	<-readerDone
	log, err := generator.reportsRange(0, 1000)
	if err != nil || log.SentTotal != 300 || len(log.Reports) != 300 || len(log.Completions) != 300 {
		testContext.Fatalf("log = %d reports, sent %d, err %v", len(log.Reports), log.SentTotal, err)
	}
}

// WriteSignal reports a full SCTP send buffer at once and sends nothing, so
// the generator retries the same message until it is accepted, and gives up
// after the library's own control-write bound. Any other error is not
// retried.
func TestSSNMGeneratorWaitsForSendBufferSpace(testContext *testing.T) {
	full := fmt.Errorf("failed to write M3UA: %w", syscall.EAGAIN)
	clock := &manualClock{now: int64(time.Second)}
	generator, writers := testGenerator(testContext, 1000, 1, 16, 2, clock)
	preloadAll(testContext, generator)
	writers[1].refuse = func(call int) error {
		if call >= 1 && call < 4 {
			return full
		}
		return nil
	}
	targets, _ := generator.readyTargetsLocked()
	generator.send(targets, 0)
	generator.send(targets, 1)
	if generator.failedMessages != 0 || generator.bufferWaits != 1 || len(writers[1].snapshot()) != 2 || writers[1].calls != 5 {
		testContext.Fatalf("failed %d waits %d writes %d calls %d", generator.failedMessages, generator.bufferWaits, len(writers[1].snapshot()), writers[1].calls)
	}
	// 50 + 100 + 200 microseconds of backoff before the fourth attempt.
	if duration := generator.completions[1] - generator.reports[1]; duration != int64(350*time.Microsecond) {
		testContext.Fatalf("report duration %s, want 350us of backoff", time.Duration(duration))
	}

	writers[0].refuse = func(int) error { return full }
	generator.send(targets, 2)
	if generator.failedMessages != 1 || generator.statuses[2] != ssnmMessageFailed || !strings.Contains(generator.firstError, "stayed full") ||
		generator.completions[2]-generator.reports[2] < int64(ssnmWriteWait) {
		testContext.Fatalf("failed %d status %d first %q duration %s", generator.failedMessages, generator.statuses[2], generator.firstError, time.Duration(generator.completions[2]-generator.reports[2]))
	}
	closed := errors.New("association closed")
	calls := writers[1].calls
	writers[1].refuse = func(int) error { return closed }
	generator.send(targets, 3)
	if writers[1].calls != calls+1 || generator.failedMessages != 2 {
		testContext.Fatalf("a non-backpressure error was retried: %d calls", writers[1].calls-calls)
	}
}

func TestSSNMGeneratorCountsFailedMessages(testContext *testing.T) {
	generator, writers := testGenerator(testContext, 1000, 1, 16, 4, &manualClock{now: int64(time.Second)})
	preloadAll(testContext, generator)
	writers[3].refuse = func(int) error { return errors.New("closed") }
	targets, _ := generator.readyTargetsLocked()
	for message := uint64(0); message < 5; message++ {
		generator.send(targets, message)
	}
	if generator.failedMessages != 1 || generator.statuses[3] != ssnmMessageFailed || !strings.Contains(generator.firstError, "message 3 on association 3") {
		testContext.Fatalf("failures %d statuses %v first %q", generator.failedMessages, generator.statuses, generator.firstError)
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
	record := &ssnmGeneratorRecord{Associations: 2, AnchorNS: 0, WindowStartNS: int64(time.Second), WindowEndNS: int64(2 * time.Second)}
	// Rate 4: window messages are 4..7. Message 5 failed, message 7 was late,
	// message 8 exists and message 9 onward was never sent.
	reports := []int64{0, 250e6, 500e6, 750e6, 1000e6, 1250e6, 1500e6, 2400e6}
	completions := append([]int64(nil), reports...)
	statuses := []uint8{1, 1, 1, 1, 1, 2, 1, 1}
	summarizeSSNMWindow(record, 4, reports, completions, statuses)
	if record.FirstMessage != 4 || record.EndMessage != 8 || record.Offered != 4 || record.Failed != 1 || record.Late != 1 || record.Unsent != 0 || record.ReportedInWindow != 2 || record.IntensityHeld {
		testContext.Fatalf("summary = %+v", record)
	}
	if len(record.OfferedPerAssociation) != 2 || record.OfferedPerAssociation[0] != 2 || record.OfferedPerAssociation[1] != 2 {
		testContext.Fatalf("per-association offers = %v, want [2 2]", record.OfferedPerAssociation)
	}
	// A report started just after the window within one interval is late
	// but holds the intensity; one lagging beyond the tolerance does not.
	onTime := &ssnmGeneratorRecord{Associations: 1, WindowStartNS: int64(time.Second), WindowEndNS: int64(2 * time.Second)}
	boundary := []int64{0, 250e6, 500e6, 750e6, 1000e6, 1250e6, 1500e6, 2000e6}
	summarizeSSNMWindow(onTime, 4, boundary, boundary, []uint8{1, 1, 1, 1, 1, 1, 1, 1})
	if onTime.Late != 1 || !onTime.IntensityHeld {
		testContext.Fatalf("boundary summary = %+v", onTime)
	}
	short := &ssnmGeneratorRecord{Associations: 1, WindowStartNS: int64(time.Second), WindowEndNS: int64(2 * time.Second)}
	summarizeSSNMWindow(short, 4, reports[:6], completions[:6], statuses[:6])
	if short.Unsent != 2 || short.IntensityHeld {
		testContext.Fatalf("unsent summary = %+v", short)
	}
}

func TestSSNMControlRoutesOnlyWithGenerator(testContext *testing.T) {
	plain := newReceiverControl(1, 8)
	server := httptest.NewServer(plain.handler())
	defer server.Close()
	response, err := http.Post(server.URL+"/ssnm/preload?step=0", "application/json", nil)
	if err != nil {
		testContext.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode == http.StatusNoContent {
		testContext.Fatal("receiver without SSNM load serves /ssnm/preload")
	}
	loaded := newReceiverControl(1, 8)
	loaded.ssnm, _ = testGenerator(testContext, 10, 1, 16, 1, &manualClock{now: int64(time.Second)})
	loadedServer := httptest.NewServer(loaded.handler())
	defer loadedServer.Close()
	for _, step := range []struct {
		query string
		want  int
	}{{"", http.StatusBadRequest}, {"?step=x", http.StatusBadRequest}, {"?step=1", http.StatusConflict}, {"?step=0", http.StatusNoContent}, {"?step=1", http.StatusConflict}} {
		response, err = http.Post(loadedServer.URL+"/ssnm/preload"+step.query, "application/json", nil)
		if err != nil {
			testContext.Fatal(err)
		}
		_ = response.Body.Close()
		if response.StatusCode != step.want {
			testContext.Fatalf("preload%s status %d, want %d", step.query, response.StatusCode, step.want)
		}
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
