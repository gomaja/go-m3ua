package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestProgressHTTPRejectsMalformedOrOversizedResponse(testContext *testing.T) {
	for _, body := range []string{"", "null", "{} {}", `{"unknown":1}`, "{}" + strings.Repeat(" ", 16<<10)} {
		testContext.Run(fmt.Sprintf("length-%d", len(body)), func(testContext *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) { _, _ = writer.Write([]byte(body)) }))
			defer server.Close()
			if _, err := getReceiverProgress(context.Background(), server.URL); err == nil {
				testContext.Fatal("malformed progress response accepted")
			}
		})
	}
}

func TestProgressHTTPHonorsCancellation(testContext *testing.T) {
	entered := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		close(entered)
		<-request.Context().Done()
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan progressObservation, 1)
	go func() { done <- observeProgress(ctx, time.Now(), server.URL) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		testContext.Fatal("progress request did not start")
	}
	cancel()
	select {
	case observation := <-done:
		if observation.Error == "" || observation.Snapshot != nil || observation.After < observation.Before {
			testContext.Fatalf("canceled observation = %+v", observation)
		}
	case <-time.After(2 * time.Second):
		testContext.Fatal("progress request ignored cancellation")
	}
}

func TestProgressSamplerStopsWithoutLeaking(testContext *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	select {
	case observations := <-sampleProgress(ctx, time.Now(), maxRunWindow, "http://unused.invalid"):
		if len(observations) != 0 {
			testContext.Fatal("already canceled sampler performed work")
		}
	case <-time.After(time.Second):
		testContext.Fatal("sampler did not stop")
	}
}

func TestWorkerWaitDoesNotRetainCanceledWindowDeadline(testContext *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	workers := make(chan struct{})
	defer close(workers)
	result := make(chan bool, 1)
	go func() { result <- waitWorkersContext(ctx, workers, maxRunWindow) }()
	cancel()
	select {
	case drained := <-result:
		if drained {
			testContext.Fatal("canceled workers reported drained")
		}
	case <-time.After(2 * time.Second):
		testContext.Fatal("worker wait retained the original ten-minute deadline")
	}
}

func TestProgressSamplerPreservesRequestCancellationAndTimeout(testContext *testing.T) {
	for _, cancelCaller := range []bool{false, true} {
		testContext.Run(fmt.Sprintf("caller-cancel-%t", cancelCaller), func(testContext *testing.T) {
			entered := make(chan struct{}, 2)
			server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
				entered <- struct{}{}
				<-request.Context().Done()
			}))
			defer server.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			started := time.Now()
			done := sampleProgress(ctx, started, 500*time.Millisecond, server.URL)
			select {
			case <-entered:
			case <-time.After(2 * time.Second):
				testContext.Fatal("sampler request did not start")
			}
			if cancelCaller {
				cancel()
			}
			select {
			case observations := <-done:
				if len(observations) != 1 || observations[0].Error == "" || observations[0].Snapshot != nil {
					testContext.Fatalf("failed request was not preserved: %+v", observations)
				}
				if !cancelCaller && observations[0].After-observations[0].Before < time.Second {
					testContext.Fatalf("request timeout was shortened: %+v", observations[0])
				}
			case <-time.After(2 * time.Second):
				testContext.Fatal("sampler ignored cancellation or request timeout")
			}
			select {
			case <-entered:
				testContext.Fatal("sampler issued another request after the boundary")
			default:
			}
		})
	}
}

func TestProgressSamplerDoesNotStartAfterWindow(testContext *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		writeJSON(writer, http.StatusOK, receiverProgress{})
	}))
	defer server.Close()
	select {
	case observations := <-sampleProgress(context.Background(), time.Now().Add(-time.Second), 500*time.Millisecond, server.URL):
		if len(observations) != 0 || requests.Load() != 0 {
			testContext.Fatalf("expired window started requests: %+v", observations)
		}
	case <-time.After(2 * time.Second):
		testContext.Fatal("expired sampler did not terminate")
	}
}

func TestProgressOffsetsIncludePreBoundaryObservation(testContext *testing.T) {
	for _, duration := range []time.Duration{time.Nanosecond, time.Millisecond, time.Second, time.Second + time.Millisecond, 2010 * time.Millisecond, maxRunWindow} {
		offsets := progressOffsets(duration)
		if len(offsets) > 601 {
			testContext.Fatalf("too many samples: %d", len(offsets))
		}
		previous := time.Duration(0)
		for _, offset := range offsets {
			if offset <= previous || offset >= duration {
				testContext.Fatalf("invalid offsets for %s: %v", duration, offsets)
			}
			previous = offset
		}
		if duration >= time.Millisecond && (len(offsets) == 0 || duration-offsets[len(offsets)-1] > 10*time.Millisecond) {
			testContext.Fatalf("missing near-boundary observation: %v", offsets)
		}
	}
}

func TestInitialProgressFailureStopsReceiverAndRetainsEvidence(testContext *testing.T) {
	for _, status := range []int{http.StatusServiceUnavailable, http.StatusOK} {
		testContext.Run(fmt.Sprint(status), func(testContext *testing.T) {
			var active atomic.Bool
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				switch request.URL.Path {
				case "/start":
					active.Store(true)
					writer.WriteHeader(http.StatusNoContent)
				case "/reset":
					writer.WriteHeader(http.StatusNoContent)
				case "/stop":
					active.Store(false)
					writer.WriteHeader(http.StatusNoContent)
				case "/progress":
					writer.WriteHeader(status)
					_, _ = writer.Write([]byte("{}"))
				case "/results":
					writeJSON(writer, http.StatusOK, runRecord{Expected: 1, Side: "receiver"})
				}
			}))
			defer server.Close()
			config := commandConfig{Rate: 1, Workload: workload128, PeerControl: server.URL}
			sender, receiver, err := runSenderCohort(context.Background(), config, nil, nil, "failed-progress", time.Second)
			if err == nil || active.Load() || sender.Expected != 1 || sender.FatalError == "" || len(sender.ProgressObservations) != 1 || receiver.Expected != 1 {
				testContext.Fatalf("failure lost evidence or left receiver active: active=%v sender=%+v receiver=%+v err=%v", active.Load(), sender, receiver, err)
			}
		})
	}
}

func FuzzSenderWindowBounds(fuzzContext *testing.F) {
	fuzzContext.Add(int64(9*time.Second), int64(time.Millisecond), uint64(890))
	fuzzContext.Add(int64(-1), int64(-1), ^uint64(0))
	fuzzContext.Fuzz(func(testContext *testing.T, before, delay int64, unique uint64) {
		specification, observations := progressFixture()
		observations[1].Before = time.Duration(before)
		observations[1].After = time.Duration(before + delay)
		observations[1].Snapshot.Delivery.Unique = unique
		observations[1].Snapshot.Delivery.Missing = specification.Expected - unique
		result := analyzeProgress(specification, observations)
		if result.Status == "bounded" {
			if result.DeliveredLower > result.DeliveredUpper || result.DeliveredUpper > specification.Expected || result.OutstandingLower > result.OutstandingUpper || result.OutstandingUpper > specification.Expected {
				testContext.Fatalf("impossible bounds: %+v", result)
			}
			for _, sample := range result.Samples {
				if sample.BacklogLower > sample.BacklogUpper || sample.BacklogUpper > specification.Expected {
					testContext.Fatalf("impossible sample: %+v", sample)
				}
			}
		}
	})
}

func TestProgressReturnsAtomicCohortSnapshot(testContext *testing.T) {
	control := newReceiverControl(1, 16)
	control.setAssociationReady(0, 15)
	specification := runSpec{Cohort: "progress", Seed: 7, Associations: 1, Expected: 2, Duration: time.Second, Payload: workload128, Rate: 2}
	if err := control.reset(specification); err != nil {
		testContext.Fatal(err)
	}
	if err := control.start(); err != nil {
		testContext.Fatal(err)
	}
	control.record(0, validReceivedMessage("progress", 7, 0, 0, 0, 128))
	request := httptest.NewRequest(http.MethodGet, "/progress", nil)
	response := httptest.NewRecorder()
	control.handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		testContext.Fatalf("progress status = %d, want 200", response.Code)
	}
	var snapshot struct {
		Spec       runSpec        `json:"spec"`
		Generation uint64         `json:"generation"`
		Phase      receiverPhase  `json:"phase"`
		Delivery   ledgerSnapshot `json:"delivery"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &snapshot); err != nil {
		testContext.Fatal(err)
	}
	if snapshot.Spec != specification || snapshot.Generation != 1 || snapshot.Phase != receiverMeasuring || snapshot.Delivery.Unique != 1 || snapshot.Delivery.Missing != 1 {
		testContext.Fatalf("progress snapshot = %+v", snapshot)
	}
}

func progressFixture() (runSpec, []progressObservation) {
	specification := runSpec{Cohort: "bounds", Rate: 100, Expected: 1000, Duration: 10 * time.Second, Associations: 1, Payload: workload128}
	observation := func(before, after time.Duration, unique uint64) progressObservation {
		return progressObservation{Before: before, After: after, Snapshot: &receiverProgress{Spec: specification, Generation: 1, Phase: receiverMeasuring, Delivery: ledgerSnapshot{Unique: unique, Missing: 1000 - unique}}}
	}
	return specification, []progressObservation{
		observation(-time.Millisecond, 0, 0),
		observation(9*time.Second, 9*time.Second+time.Millisecond, 890),
		observation(10*time.Second, 10*time.Second+time.Millisecond, 995),
		observation(11*time.Second, 11*time.Second+time.Millisecond, 1000),
	}
}

func TestSenderWindowDoesNotCreditDrainOrAssumeClockAlignment(testContext *testing.T) {
	specification, observations := progressFixture()
	result := analyzeProgress(specification, observations)
	if result.Status != "bounded" || result.DeliveredLower != 890 || result.DeliveredUpper != 995 || result.OutstandingLower != 5 || result.OutstandingUpper != 110 || result.RateLower != 89 || result.RateUpper != 99.5 {
		testContext.Fatalf("sender window = %+v", result)
	}
	if result.Samples[1].BacklogLower != 11 || result.Samples[1].BacklogUpper != 11 {
		testContext.Fatalf("sample bounds = %+v", result.Samples[1])
	}
}

func TestSenderWindowRetainsBoundaryStraddlingUncertainty(testContext *testing.T) {
	specification, observations := progressFixture()
	observations[2].Before = specification.Duration - time.Millisecond
	result := analyzeProgress(specification, observations)
	if result.Status != "bounded" || result.DeliveredLower != 890 || result.DeliveredUpper != 1000 || result.OutstandingLower != 0 || result.OutstandingUpper != 110 {
		testContext.Fatalf("straddling observation narrowed unknown boundary: %+v", result)
	}
}

func TestProgressRejectsUntrustworthyEvidence(testContext *testing.T) {
	for _, scenario := range []struct {
		name  string
		alter func([]progressObservation) []progressObservation
	}{
		{"empty", func(_ []progressObservation) []progressObservation { return nil }},
		{"missing-start", func(values []progressObservation) []progressObservation { return values[1:] }},
		{"missing-end", func(values []progressObservation) []progressObservation { return values[:2] }},
		{"error", func(values []progressObservation) []progressObservation { values[1].Error = "timeout"; return values }},
		{"missing-snapshot", func(values []progressObservation) []progressObservation { values[1].Snapshot = nil; return values }},
		{"wrong-cohort", func(values []progressObservation) []progressObservation {
			values[1].Snapshot.Spec.Cohort = "other"
			return values
		}},
		{"wrong-generation", func(values []progressObservation) []progressObservation {
			values[1].Snapshot.Generation++
			return values
		}},
		{"zero-generation", func(values []progressObservation) []progressObservation {
			values[0].Snapshot.Generation = 0
			return values
		}},
		{"reversed-time", func(values []progressObservation) []progressObservation {
			values[1].After = values[1].Before - 1
			return values
		}},
		{"overlap", func(values []progressObservation) []progressObservation {
			values[2].Before = values[1].After - 1
			return values
		}},
		{"impossible-delivery", func(values []progressObservation) []progressObservation {
			values[1].Snapshot.Delivery.Unique = 950
			values[1].Snapshot.Delivery.Missing = 50
			return values
		}},
		{"inconsistent-total", func(values []progressObservation) []progressObservation {
			values[1].Snapshot.Delivery.Missing++
			return values
		}},
		{"regressed-delivery", func(values []progressObservation) []progressObservation {
			values[3].Snapshot.Delivery.Unique = 990
			values[3].Snapshot.Delivery.Missing = 10
			return values
		}},
		{"duplicate", func(values []progressObservation) []progressObservation {
			values[1].Snapshot.Delivery.Duplicate++
			return values
		}},
		{"invalid", func(values []progressObservation) []progressObservation {
			values[1].Snapshot.Delivery.Invalid++
			return values
		}},
		{"reordered", func(values []progressObservation) []progressObservation {
			values[1].Snapshot.Delivery.Reordered++
			return values
		}},
		{"fatal", func(values []progressObservation) []progressObservation {
			values[1].Snapshot.FatalError = "failure"
			return values
		}},
		{"stopped", func(values []progressObservation) []progressObservation {
			values[1].Snapshot.Phase = receiverStopped
			return values
		}},
		{"preexisting-work", func(values []progressObservation) []progressObservation {
			values[0].Snapshot.Delivery.Unique = 1
			values[0].Snapshot.Delivery.Missing = 999
			return values
		}},
	} {
		testContext.Run(scenario.name, func(testContext *testing.T) {
			specification, observations := progressFixture()
			result := analyzeProgress(specification, scenario.alter(observations))
			if result.Status != verdictInconclusive || result.Reason == "" || result.RateLower != 0 || result.RateUpper != 0 {
				testContext.Fatalf("untrustworthy evidence accepted: %+v", result)
			}
		})
	}
}

func TestIdealOfferedScheduleIncludesSchedulerBacklog(testContext *testing.T) {
	specification, _ := progressFixture()
	for _, scenario := range []struct {
		elapsed time.Duration
		want    uint64
	}{
		{-1, 0}, {0, 0}, {1, 1}, {time.Second - 1, 100}, {time.Second, 101}, {10 * time.Second, 1000}, {11 * time.Second, 1000},
	} {
		if got := offeredAt(specification, scenario.elapsed); got != scenario.want {
			testContext.Errorf("offeredAt(%s) = %d, want %d", scenario.elapsed, got, scenario.want)
		}
	}
}

func TestBacklogChangeExcludesDrainAndPreservesUncertainty(testContext *testing.T) {
	for _, scenario := range []struct {
		name                                         string
		firstLower, firstUpper, lastLower, lastUpper uint64
		want                                         string
	}{
		{"growth", 5, 10, 20, 30, "increase-demonstrated"},
		{"recovery", 20, 30, 5, 10, "nonincrease-demonstrated"},
		{"exact-constant", 5, 5, 5, 5, "nonincrease-demonstrated"},
		{"noise", 5, 10, 5, 10, "unresolved"},
	} {
		testContext.Run(scenario.name, func(testContext *testing.T) {
			samples := make([]backlogInterval, 8)
			for index := range samples {
				samples[index] = backlogInterval{Before: time.Duration(index+1) * time.Second, After: time.Duration(index+1)*time.Second + time.Millisecond, BacklogLower: scenario.firstLower, BacklogUpper: scenario.firstUpper}
				if index >= 6 {
					samples[index].BacklogLower = scenario.lastLower
					samples[index].BacklogUpper = scenario.lastUpper
				}
			}
			samples = append(samples, backlogInterval{Before: 10 * time.Second, After: 11 * time.Second})
			result := describeBacklogChange(samples, 10*time.Second)
			if result.Status != scenario.want || result.SampleCount != 8 || result.MeanChangeLower != float64(scenario.lastLower)-float64(scenario.firstUpper) || result.MeanChangeUpper != float64(scenario.lastUpper)-float64(scenario.firstLower) {
				testContext.Fatalf("change = %+v", result)
			}
		})
	}
	if result := describeBacklogChange(nil, time.Second); result.Status != "insufficient-samples" {
		testContext.Fatalf("missing samples = %+v", result)
	}
}
