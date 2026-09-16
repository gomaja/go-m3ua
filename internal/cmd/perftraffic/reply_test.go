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

func replyJob(sequence uint64) echoReplyJob {
	return echoReplyJob{
		identity: messageIdentity{Cohort: "reply-cohort", Seed: 7, Association: 0, Flow: 0, Sequence: sequence, Kind: kindEchoRequest},
		size:     128,
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

	offerEchoReply(queue, replyJob(0), control)
	<-writerBlocked
	// The writer is parked on the first job; the queue absorbs two more.
	offerEchoReply(queue, replyJob(1), control)
	offerEchoReply(queue, replyJob(2), control)
	// The queue is now full: further offers drop with a counter, in
	// constant time, which is what keeps the read loop responsive.
	for sequence := uint64(3); sequence < 6; sequence++ {
		offerEchoReply(queue, replyJob(sequence), control)
	}
	if control.echoRepliesDropped != 3 {
		testContext.Fatalf("dropped = %d, want 3", control.echoRepliesDropped)
	}
	close(release)
	close(queue)
	deadline := time.Now().Add(2 * time.Second)
	for control.echoReplies != 3 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if control.echoReplies != 3 {
		testContext.Fatalf("replies = %d, want 3 after the writer unblocked", control.echoReplies)
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
	offerEchoReply(queue, replyJob(0), control)
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
	offerEchoReply(queue, replyJob(0), control)
	offerEchoReply(queue, replyJob(1), control)
	deadline := time.Now().Add(2 * time.Second)
	for control.echoReplies+control.echoReplyErrors != 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if err := control.stop(); err != nil {
		testContext.Fatalf("stop: %v", err)
	}
	offerEchoReply(queue, replyJob(2), control)
	close(queue)
	deadline = time.Now().Add(2 * time.Second)
	for control.echoRepliesDropped != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if control.echoReplyErrors != 1 || control.echoReplies != 1 || control.echoRepliesDropped != 1 {
		testContext.Fatalf("errors=%d replies=%d dropped=%d, want 1/1/1",
			control.echoReplyErrors, control.echoReplies, control.echoRepliesDropped)
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
		offerEchoReply(queue, replyJob(sequence), control)
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
