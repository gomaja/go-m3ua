package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func echoSpec(cohort string) runSpec {
	return runSpec{
		Cohort: cohort, Seed: 7, Associations: 1, Expected: 100, Duration: time.Second,
		Drain: 2 * time.Second, Rate: 100, Payload: workload128, Mode: modeEcho, Direction: directionASPToSGP,
	}
}

func startedEchoControl(testContext *testing.T) *receiverControl {
	testContext.Helper()
	control := newReceiverControl(1, 16)
	control.setAssociationReady(0, 15)
	if err := control.reset(echoSpec("reply-cohort")); err != nil {
		testContext.Fatalf("reset: %v", err)
	}
	if err := control.start(); err != nil {
		testContext.Fatalf("start: %v", err)
	}
	return control
}

// replyJob builds a job for the cohort that is active right now, the way the
// read loop does: the generation the request was validated under travels with
// the job.
func replyJob(control *receiverControl, sequence uint64) echoReplyJob {
	return echoReplyJob{
		identity:   messageIdentity{Cohort: "reply-cohort", Seed: 7, Association: 0, Flow: 0, Sequence: sequence, Kind: kindEchoRequest},
		size:       128,
		generation: control.currentGeneration(),
	}
}

// A wedged peer blocks the reply writer, not the read loop: offers must keep
// succeeding until the bounded queue fills, and everything past that is a
// counted drop — never a stall, never a silent loss.
func TestBlockedReplyWriterNeverStallsOffersAndDropsAreCounted(testContext *testing.T) {
	control := startedEchoControl(testContext)
	queue := make(chan echoReplyJob, 2)
	writerBlocked := make(chan struct{})
	release := make(chan struct{})
	var blockedOnce sync.Once
	go runEchoReplyWriter(queue, control, func(echoReplyJob, time.Time, uint64) error {
		blockedOnce.Do(func() { close(writerBlocked) })
		<-release
		return nil
	})

	offerEchoReply(queue, replyJob(control, 0), control)
	<-writerBlocked
	// The writer is parked on the first job; the queue absorbs two more.
	offerEchoReply(queue, replyJob(control, 1), control)
	offerEchoReply(queue, replyJob(control, 2), control)
	// The queue is now full: further offers drop with a counter, in
	// constant time, which is what keeps the read loop responsive.
	for sequence := uint64(3); sequence < 6; sequence++ {
		offerEchoReply(queue, replyJob(control, sequence), control)
	}
	if _, _, dropped := control.echoCounts(); dropped != 3 {
		testContext.Fatalf("dropped = %d, want 3", dropped)
	}
	close(release)
	close(queue)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if replies, _, _ := control.echoCounts(); replies == 3 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if replies, _, _ := control.echoCounts(); replies != 3 {
		testContext.Fatalf("replies = %d, want 3 after the writer unblocked", replies)
	}
}

func TestReplyWriterPassesCohortDeadlineAndGeneration(testContext *testing.T) {
	control := startedEchoControl(testContext)
	queue := make(chan echoReplyJob, 1)
	type observed struct {
		deadline   time.Time
		generation uint64
	}
	seen := make(chan observed, 1)
	go runEchoReplyWriter(queue, control, func(job echoReplyJob, deadline time.Time, generation uint64) error {
		seen <- observed{deadline, generation}
		return nil
	})
	offerEchoReply(queue, replyJob(control, 0), control)
	got := <-seen
	if got.generation != control.generation {
		testContext.Fatalf("generation = %d, want %d", got.generation, control.generation)
	}
	wantDeadline := control.started.Add(control.spec.Duration + control.spec.Drain + echoRequestDeadline)
	if !got.deadline.Equal(wantDeadline) {
		testContext.Fatalf("deadline = %v, want %v", got.deadline, wantDeadline)
	}
	close(queue)
}

func TestReplyWriterDropsAfterCohortEndsAndCountsWriteErrors(testContext *testing.T) {
	control := startedEchoControl(testContext)
	queue := make(chan echoReplyJob, 8)
	writes := make(chan error, 4)
	writes <- errors.New("send buffer gone")
	writes <- nil
	go runEchoReplyWriter(queue, control, func(echoReplyJob, time.Time, uint64) error {
		return <-writes
	})
	offerEchoReply(queue, replyJob(control, 0), control)
	offerEchoReply(queue, replyJob(control, 1), control)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if replies, replyErrors, _ := control.echoCounts(); replies+replyErrors == 2 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if err := control.stop(); err != nil {
		testContext.Fatalf("stop: %v", err)
	}
	offerEchoReply(queue, replyJob(control, 2), control)
	close(queue)
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, _, dropped := control.echoCounts(); dropped == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	replies, replyErrors, dropped := control.echoCounts()
	if replyErrors != 1 || replies != 1 || dropped != 1 {
		testContext.Fatalf("errors=%d replies=%d dropped=%d, want 1/1/1", replyErrors, replies, dropped)
	}
	record := control.result()
	if record.ReceiverEcho == nil || record.ReceiverEcho.RepliesDropped != 1 || record.ReceiverEcho.ReplyErrors != 1 {
		testContext.Fatalf("receiver echo = %+v", record.ReceiverEcho)
	}
	if record.FixtureVerdict != verdictInvalid {
		testContext.Fatalf("fixture verdict = %q, want invalid with dropped replies and reply errors", record.FixtureVerdict)
	}
}

// The control endpoint must answer while every reply writer is wedged: the
// read loop and HTTP server never wait on reply writes.
func TestControlEndpointStaysResponsiveWithBlockedReplyWriter(testContext *testing.T) {
	control := startedEchoControl(testContext)
	server := httptest.NewServer(control.handler())
	defer server.Close()
	queue := make(chan echoReplyJob, 4)
	release := make(chan struct{})
	defer close(release)
	go runEchoReplyWriter(queue, control, func(echoReplyJob, time.Time, uint64) error {
		<-release
		return nil
	})
	for sequence := uint64(0); sequence < 4; sequence++ {
		offerEchoReply(queue, replyJob(control, sequence), control)
	}
	client := &http.Client{Timeout: 2 * time.Second}
	started := time.Now()
	response, err := client.Get(server.URL + "/progress")
	if err != nil {
		testContext.Fatalf("progress request failed with a wedged reply writer: %v", err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		testContext.Fatalf("progress status = %d, want 200", response.StatusCode)
	}
	if elapsed := time.Since(started); elapsed >= 2*time.Second {
		testContext.Fatalf("control endpoint was starved by the blocked reply writer: %s", elapsed)
	}
	close(queue)
}

// The sender side must not amplify a wedged peer: admitted requests that are
// never answered are all swept at their deadline, the drain wait is bounded,
// and nothing is left outstanding.
func TestWaitEchoDrainBoundsOutstandingWhenPeerWedges(testContext *testing.T) {
	tracker := newEchoTracker("wedged", 1, 1, workload128, 8, 5*time.Millisecond)
	scheduled := time.Now()
	for index := uint64(0); index < 8; index++ {
		if !tracker.admit(index, scheduled) {
			testContext.Fatalf("admission %d failed below the cap", index)
		}
	}
	drainStarted := time.Now()
	waitEchoDrain(context.Background(), tracker, time.Now().Add(500*time.Millisecond))
	if elapsed := time.Since(drainStarted); elapsed > 2*time.Second {
		testContext.Fatalf("echo drain was not bounded: %s", elapsed)
	}
	result := tracker.result(8)
	if result.OutstandingAfterDrain != 0 || result.DeadlineExceeded != 8 || result.Validated != 0 {
		testContext.Fatalf("result = %+v, want 8 deadline failures and nothing outstanding", result)
	}
}

func TestReceiverEchoResultReportsDropsAsInvalid(testContext *testing.T) {
	record := runRecord{Expected: 10, ReceiverEcho: &receiverEchoResult{Replies: 10, RepliesDropped: 1}}
	record.evaluate()
	if record.FixtureVerdict != verdictInvalid {
		testContext.Fatalf("fixture verdict = %q, want invalid with a dropped reply", record.FixtureVerdict)
	}
	record = runRecord{Expected: 10, ReceiverEcho: &receiverEchoResult{Replies: 10}}
	record.Delivery = deliveryResult{Unique: 10}
	record.evaluate()
	if record.FixtureVerdict == verdictInvalid {
		testContext.Fatalf("fixture verdict = %q for a clean echo result: %v", record.FixtureVerdict, record.Reasons)
	}
}

// resetToNextCohort ends the active cohort and arms a fresh one, which is the
// boundary every reply job and completion has to respect: the reset clears the
// echo counters and advances the generation.
func resetToNextCohort(testContext *testing.T, control *receiverControl, cohort string) {
	testContext.Helper()
	if err := control.stop(); err != nil {
		testContext.Fatalf("stop: %v", err)
	}
	if err := control.reset(echoSpec(cohort)); err != nil {
		testContext.Fatalf("reset %s: %v", cohort, err)
	}
	if err := control.start(); err != nil {
		testContext.Fatalf("start %s: %v", cohort, err)
	}
}

// A reply write that begins before a reset can finish after it. Its result
// belongs to the cohort that enqueued it, whose counters the reset has already
// cleared, and must never be recorded against the new cohort.
func TestReplyCompletionAfterAResetNeverLandsInTheNewCohort(testContext *testing.T) {
	control := startedEchoControl(testContext)
	queue := make(chan echoReplyJob, 1)
	insideWrite := make(chan struct{})
	release := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		runEchoReplyWriter(queue, control, func(echoReplyJob, time.Time, uint64) error {
			close(insideWrite)
			<-release
			return errors.New("association went away mid-write")
		})
	}()

	offerEchoReply(queue, replyJob(control, 0), control)
	<-insideWrite
	resetToNextCohort(testContext, control, "next-cohort")
	close(release)
	close(queue)
	<-writerDone

	replies, replyErrors, dropped := control.echoCounts()
	if replies != 0 || replyErrors != 0 || dropped != 0 {
		testContext.Fatalf("new cohort counters = replies %d, errors %d, dropped %d; want the previous cohort's write accounted nowhere here",
			replies, replyErrors, dropped)
	}
	record := control.result()
	if record.ReceiverEcho == nil || record.ReceiverEcho.Replies != 0 ||
		record.ReceiverEcho.ReplyErrors != 0 || record.ReceiverEcho.RepliesDropped != 0 {
		testContext.Fatalf("new cohort receiver echo = %+v, want an untouched report", record.ReceiverEcho)
	}
}

// A queue-full drop belongs to the cohort whose request it was. After a reset
// the old cohort's counters are gone, so the drop must not be charged to the
// new one either.
func TestReplyDropAfterAResetNeverLandsInTheNewCohort(testContext *testing.T) {
	control := startedEchoControl(testContext)
	stale := replyJob(control, 0)
	resetToNextCohort(testContext, control, "next-cohort")

	// No reader and no capacity, so every offer takes the drop path.
	queue := make(chan echoReplyJob)
	offerEchoReply(queue, stale, control)

	if _, _, dropped := control.echoCounts(); dropped != 0 {
		testContext.Fatalf("dropped = %d in the new cohort, want the previous cohort's drop accounted nowhere here", dropped)
	}
}

// The writer must not answer a request that belongs to a cohort that has
// already ended, even when the current cohort is measuring and would accept a
// reply of its own.
func TestReplyWriterNeverWritesAJobFromAnEarlierCohort(testContext *testing.T) {
	control := startedEchoControl(testContext)
	stale := replyJob(control, 0)
	resetToNextCohort(testContext, control, "next-cohort")

	queue := make(chan echoReplyJob, 1)
	writes := make(chan uint64, 4)
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		runEchoReplyWriter(queue, control, func(_ echoReplyJob, _ time.Time, generation uint64) error {
			writes <- generation
			return nil
		})
	}()
	queue <- stale
	close(queue)
	<-writerDone

	if len(writes) != 0 {
		testContext.Fatalf("the writer answered %d request(s) from an earlier cohort", len(writes))
	}
	replies, replyErrors, dropped := control.echoCounts()
	if replies != 0 || replyErrors != 0 || dropped != 0 {
		testContext.Fatalf("new cohort counters = replies %d, errors %d, dropped %d; want all zero", replies, replyErrors, dropped)
	}
}

// The reply path is bound to its cohort at the point of validation, so record
// must report the generation it committed the arrival under, and the reply job
// must carry that generation rather than whatever is current when it is built.
func TestRecordBindsTheArrivalToItsCohortGeneration(testContext *testing.T) {
	control := startedEchoControl(testContext)
	request := validReceivedMessage("reply-cohort", 7, 0, 0, 0, 128)
	request.ProtocolData.Data[7] = kindEchoRequest
	first, outcome := control.record(0, request)
	if outcome != recordUnique {
		testContext.Fatalf("outcome = %d, want a unique echo request", outcome)
	}
	if first.generation != control.currentGeneration() {
		testContext.Fatalf("generation = %d, want the active cohort %d", first.generation, control.currentGeneration())
	}

	resetToNextCohort(testContext, control, "next-cohort")
	next := validReceivedMessage("next-cohort", 7, 0, 0, 0, 128)
	next.ProtocolData.Data[7] = kindEchoRequest
	second, outcome := control.record(0, next)
	if outcome != recordUnique {
		testContext.Fatalf("outcome = %d after the reset, want a unique echo request", outcome)
	}
	if second.generation == first.generation {
		testContext.Fatalf("generation %d did not advance across the reset", second.generation)
	}
	if second.generation != control.currentGeneration() {
		testContext.Fatalf("generation = %d, want the active cohort %d", second.generation, control.currentGeneration())
	}

	job := first.replyJob(128)
	if job.generation != first.generation {
		testContext.Fatalf("reply job generation = %d, want the recording generation %d", job.generation, first.generation)
	}
	if job.identity != first.identity || job.size != 128 {
		testContext.Fatalf("reply job = %+v, want the recorded identity at size 128", job)
	}
}
