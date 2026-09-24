package main

import (
	"context"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestSGPFailureLiveOverLoopback runs the production routed entry points with
// -sgp-failure over real SCTP on loopback: the receiver closes sg-a/p0's two
// associations at the declared shared-clock instant, the ASP observes both
// end, MTPTransfer moves the 250 affected routes to sg-a/p1, and every
// criterion of the trial is judged from the records.
func TestSGPFailureLiveOverLoopback(testContext *testing.T) {
	result := runSGPFailureLive(testContext)
	failover := result.Sender.Failover
	for _, notification := range failover.Sender.Notifications {
		testContext.Logf("association %d ended: %s", notification.Association, notification.Error)
	}
}

// TestSGPAbortFailureLiveOverLoopback is the same trial with
// -sgp-failure-kind=abort on both processes. The receiver ends sg-a/p0's
// associations with Association.Abort, both records declare and record the
// abort, and the ASP's two failed associations end on the SCTP_COMM_LOST the
// ABORT raises, carrying its User-Initiated Abort cause, which is what makes
// this the abortive failure rather than the graceful one.
func TestSGPAbortFailureLiveOverLoopback(testContext *testing.T) {
	result := runSGPFailureLive(testContext, "-sgp-failure-kind=abort")
	for side, record := range map[string]runRecord{"sender": result.Sender, "receiver": result.Receiver} {
		if record.Spec.SGPFailure == nil || record.Spec.SGPFailure.Kind != sgpFailureKindAbort {
			testContext.Errorf("the %s record declares %+v, want an abort", side, record.Spec.SGPFailure)
		}
		if _, listed := record.UnsupportedModes["abortive_failure"]; listed {
			testContext.Errorf("the %s record lists the abortive failure it measured as unavailable", side)
		}
	}
	failover := result.Sender.Failover
	if failover.Receiver.Fault == nil {
		testContext.Fatal("the receiver recorded no fault")
	}
	if failover.Receiver.Fault.Kind != sgpFailureKindAbort {
		testContext.Errorf("the receiver recorded a %q fault, want %q", failover.Receiver.Fault.Kind, sgpFailureKindAbort)
	}
	failed := 0
	for _, notification := range failover.Sender.Notifications {
		testContext.Logf("association %d ended: %s", notification.Association, notification.Error)
		if notification.SGP != sgpFailureFailed {
			continue
		}
		failed++
		for _, want := range []string{"SCTP_COMM_LOST", "SCTP_ERROR_USER_ABORT"} {
			if !strings.Contains(notification.Error, want) {
				testContext.Errorf("failed-SGP association %d ended with %q, which does not name %s", notification.Association, notification.Error, want)
			}
		}
	}
	if failed != 2 {
		testContext.Errorf("%d failed-SGP associations reported their end, want 2", failed)
	}
}

// runSGPFailureLive runs one failure trial at a two-second offset, both
// processes taking the extra flags, and judges it.
func runSGPFailureLive(testContext *testing.T, extra ...string) combinedResult {
	testContext.Helper()
	if testing.Short() {
		testContext.Skip("the SGP failure trial needs a twelve-second window")
	}
	base := routedLoopbackPortBase(testContext)
	control := routedLoopbackControlAddress(testContext)
	sctpAddress := net.JoinHostPort("127.0.0.1", strconv.Itoa(base))
	receiverConfig, err := parseConfig(append([]string{
		"-role=sgp", "-transport=listen", "-mode=routed", "-sctp-address=" + sctpAddress,
		"-control-address=" + control, "-associations=8", "-same-host-clock", "-sgp-failure=2s",
	}, extra...))
	if err != nil {
		testContext.Fatal(err)
	}
	senderConfig, err := parseConfig(append([]string{
		"-role=asp", "-transport=dial", "-mode=routed", "-sctp-address=" + sctpAddress,
		"-local-address=127.0.0.1:0", "-peer-control=http://" + control, "-associations=8",
		"-payload=mix", "-rate=2000", "-warmup=1s", "-duration=12s", "-drain=2s",
		"-cohort=sgp-failure-live", "-seed=9", "-same-host-clock", "-sgp-failure=2s",
	}, extra...))
	if err != nil {
		testContext.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	receiverContext, stopReceiver := context.WithCancel(ctx)
	defer stopReceiver()
	type receiverOutcome struct {
		record runRecord
		err    error
	}
	receiverDone := make(chan receiverOutcome, 1)
	go func() {
		record, err := runRoutedReceiver(receiverContext, receiverConfig)
		receiverDone <- receiverOutcome{record: record, err: err}
	}()
	for {
		response, err := http.Get("http://" + control + "/ready")
		if err == nil {
			_ = response.Body.Close()
			break
		}
		select {
		case outcome := <-receiverDone:
			testContext.Fatalf("receiver exited before serving: %v", outcome.err)
		case <-ctx.Done():
			testContext.Fatal(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	result, senderErr := runRoutedSender(ctx, senderConfig)
	stopReceiver()
	outcome := <-receiverDone
	if senderErr != nil || result.Measurement == nil || result.Warmup == nil || result.Warmup.Verdict == verdictInvalid {
		testContext.Fatalf("sender error %v verdict %s error %q", senderErr, result.Verdict, result.Error)
	}
	if result.Warmup.Sender.Failover != nil || result.Warmup.Sender.Spec.SGPFailure != nil {
		testContext.Fatal("the warm-up cohort declared the failure")
	}
	failover := result.Sender.Failover
	if failover == nil || failover.Sender == nil || failover.Receiver == nil {
		testContext.Fatalf("measurement carries no failover evidence: %+v", result.Sender.Reasons)
	}
	// Nine post-milestone samples at 2,000 messages/s may leave the backlog
	// trend unresolved on a loaded host; its zero-loss half must still hold.
	// The race detector's slowdown voids the two wall-clock budgets, so a
	// race run judges the concurrency and accounting, not the timing. Every
	// other criterion must pass.
	timed := map[string]bool{"alternative_selection": true, "healthy_path_recovery": true}
	for _, criterion := range failover.Criteria {
		testContext.Logf("%s: %s — %s", criterion.Name, criterion.Outcome, criterion.Detail)
		unresolvedTrend := criterion.Name == "full_rate_after_recovery" && criterion.Outcome == failoverNotMeasured &&
			criterion.Measured != nil && *criterion.Measured == 0
		raceTiming := raceDetector && timed[criterion.Name]
		if criterion.Outcome != failoverPass && !unresolvedTrend && !raceTiming {
			testContext.Errorf("criterion %s = %s", criterion.Name, criterion.Outcome)
		}
	}
	if result.Sender.FixtureVerdict != verdictPass || result.Receiver.FixtureVerdict != verdictPass || failover.Verdict == failoverFail && !raceDetector {
		testContext.Fatalf("verdicts: sender %s/%s receiver %s/%s failover %s reasons %v %v", result.Sender.Verdict, result.Sender.FixtureVerdict,
			result.Receiver.Verdict, result.Receiver.FixtureVerdict, failover.Verdict, result.Sender.Reasons, result.Receiver.Reasons)
	}
	if outcome.err != nil || outcome.record.FatalError != "" {
		testContext.Fatalf("receiver exit %v fatal %q", outcome.err, outcome.record.FatalError)
	}
	testContext.Logf("notification %d ns after the fault; first alternative %+v; longest call after the fault %+v; outcomes %+v; failed path %+v; send duration %+v",
		failover.Sender.Notification-failover.Receiver.Fault.Before, *failover.Sender.FirstAlternative, failover.Sender.LongestCallAfterFault,
		failover.Sender.Outcomes, failover.Sender.FailedPath, result.Sender.SendDuration)
	return result
}
