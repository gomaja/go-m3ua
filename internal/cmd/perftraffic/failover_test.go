package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua"
	"github.com/gomaja/go-sctp"
)

func sgpFailureASPArguments(extra ...string) []string {
	return append([]string{
		"-role=asp", "-transport=dial", "-mode=routed", "-sctp-address=127.0.0.1:2905", "-local-address=127.0.0.1:0",
		"-peer-control=http://127.0.0.1:8080", "-associations=8", "-payload=mix", "-cohort=f5", "-same-host-clock",
		"-duration=30s", "-sgp-failure=10s",
	}, extra...)
}

func TestSGPFailureFlagIsOptInAndBounded(testContext *testing.T) {
	config, err := parseConfig(sgpFailureASPArguments())
	if err != nil || config.SGPFailure != 10*time.Second {
		testContext.Fatalf("valid failure trial: %+v, %v", config.SGPFailure, err)
	}
	receiver, err := parseConfig([]string{"-role=sgp", "-mode=routed", "-sctp-address=127.0.0.1:2905", "-associations=8", "-same-host-clock", "-sgp-failure=10s"})
	if err != nil || receiver.SGPFailure != 10*time.Second {
		testContext.Fatalf("valid failure receiver: %v", err)
	}
	plain, err := parseConfig([]string{"-role=sgp", "-mode=routed", "-sctp-address=127.0.0.1:2905", "-associations=8"})
	if err != nil || plain.SGPFailure != 0 || plain.sgpFailureCohort {
		testContext.Fatalf("routed receiver without the flag changed: %+v, %v", plain.SGPFailure, err)
	}
	for name, arguments := range map[string][]string{
		"routed-direct":     sgpFailureASPArguments("-mode=routed-direct"),
		"no shared clock":   {"-role=sgp", "-mode=routed", "-sctp-address=127.0.0.1:2905", "-associations=8", "-sgp-failure=10s"},
		"throughput":        {"-role=sgp", "-sgp-failure=10s", "-same-host-clock"},
		"negative":          sgpFailureASPArguments("-sgp-failure=-1s"),
		"too early":         sgpFailureASPArguments("-sgp-failure=1500ms"),
		"too little after":  sgpFailureASPArguments("-sgp-failure=21s"),
		"with SSNM load":    sgpFailureASPArguments("-ssnm-rate=10"),
		"window too short":  sgpFailureASPArguments("-duration=11s", "-sgp-failure=2s"),
		"after window ends": sgpFailureASPArguments("-sgp-failure=31s"),
	} {
		if _, err := parseConfig(arguments); err == nil {
			testContext.Errorf("%s: accepted", name)
		}
	}
}

func sgpFailureSpecFixture() runSpec {
	return sgpFailureSpecFixtureOfKind(sgpFailureKindClose)
}

func sgpFailureSpecFixtureOfKind(kind string) runSpec {
	domain := sharedClockDomain{Clock: "CLOCK_MONOTONIC", BootID: "test-boot", TimeNamespace: "monotonic-offset:0.000000000", Resolution: 1}
	specification := routedSpec(modeRouted)
	specification.Rate, specification.Duration, specification.Expected = 1000, 30*time.Second, 30000
	specification.Clock = &sharedClockWindow{Domain: domain, Start: int64(100 * time.Second), End: int64(130 * time.Second)}
	specification.SGPFailure = newSGPFailureSpec(10*time.Second, kind)
	return specification
}

func TestSGPFailureSpecIsDeclaredOnlyWhenSetAndComparedByValue(testContext *testing.T) {
	plain := routedSpec(modeRouted)
	encoded, err := json.Marshal(plain)
	if err != nil || strings.Contains(string(encoded), "failure_trial") {
		testContext.Fatalf("a nominal spec serializes a failure: %s %v", encoded, err)
	}
	record, err := json.Marshal(runRecord{Spec: plain})
	if err != nil || strings.Contains(string(record), `"failover"`) {
		testContext.Fatalf("a nominal record serializes failover evidence: %v", err)
	}
	failure := sgpFailureSpecFixture()
	copied := copyRunSpec(failure)
	if copied.SGPFailure == failure.SGPFailure || !sameRunSpec(copied, failure) {
		testContext.Fatal("copyRunSpec shares or loses the failure declaration")
	}
	copied.SGPFailure.Offset++
	if sameRunSpec(copied, failure) {
		testContext.Fatal("sameRunSpec ignores the failure offset")
	}
	withoutFailure := copyRunSpec(failure)
	withoutFailure.SGPFailure = nil
	if sameRunSpec(withoutFailure, failure) {
		testContext.Fatal("sameRunSpec equates a failure cohort with a nominal one")
	}
	if err := validateSGPFailureSpec(failure, 10*time.Second, sgpFailureKindClose); err != nil {
		testContext.Fatalf("matching declaration refused: %v", err)
	}
	for name, test := range map[string]struct {
		mutate func(*runSpec)
		offset time.Duration
	}{
		"receiver without flag": {offset: 0},
		"other offset":          {offset: 11 * time.Second},
		"other kind":            {offset: 10 * time.Second, mutate: func(specification *runSpec) { specification.SGPFailure.Kind = "abort" }},
		"other SGP":             {offset: 10 * time.Second, mutate: func(specification *runSpec) { specification.SGPFailure.SGP = sgpFailureAlternative }},
		"other budget":          {offset: 10 * time.Second, mutate: func(specification *runSpec) { specification.SGPFailure.SelectionBudget = time.Second }},
		"routed-direct":         {offset: 10 * time.Second, mutate: func(specification *runSpec) { specification.Mode = modeRoutedDirect }},
		"no shared clock":       {offset: 10 * time.Second, mutate: func(specification *runSpec) { specification.Clock = nil }},
		"short post window":     {offset: 10 * time.Second, mutate: func(specification *runSpec) { specification.Duration = 15 * time.Second }},
	} {
		specification := copyRunSpec(failure)
		if test.mutate != nil {
			test.mutate(&specification)
		}
		if err := validateSGPFailureSpec(specification, test.offset, sgpFailureKindClose); err == nil {
			testContext.Errorf("%s: accepted", name)
		}
	}
}

// TestFailoverScheduledInMatchesTheOpenLoopSchedule checks the offered count
// per interval against dispatchOpenLoop's offset floor(i*1s/rate), message by
// message.
func TestFailoverScheduledInMatchesTheOpenLoopSchedule(testContext *testing.T) {
	for _, rate := range []uint64{1, 3, 999, 1000, 7919, 20000} {
		duration := 3 * time.Second
		expected, _ := scheduledMessages(rate, duration)
		for _, bin := range []time.Duration{time.Millisecond, 7 * time.Millisecond, 100 * time.Millisecond} {
			counts := make(map[int]uint64)
			for index := range expected {
				counts[int(time.Duration(index*uint64(time.Second)/rate)/bin)]++
			}
			for from := time.Duration(0); from < duration; from += bin {
				want := counts[int(from/bin)]
				if got := failoverScheduledIn(rate, expected, from, min(from+bin, duration)); got != want {
					testContext.Fatalf("rate %d bin %s from %s: got %d, want %d", rate, bin, from, got, want)
				}
			}
		}
	}
	if failoverScheduledIn(1000, 10, time.Second, 2*time.Second) != 0 || failoverScheduledIn(1000, 10, 2*time.Second, time.Second) != 0 {
		testContext.Fatal("counts past the expected total or of an empty interval")
	}
}

// failoverTrackerFixture builds a tracker over the preflight fixture: routes
// r with r%8 in {0,1} are frozen on sg-a/p0 (sender associations 101 and
// 102), and sg-a/p1 (103 and 104) is the alternative.
func failoverTrackerFixture(testContext *testing.T) (*failoverTracker, routingPathMap, runSpec) {
	testContext.Helper()
	topology, bindings, observations := routingPreflightFixture(testContext, "primary")
	paths, err := freezeRoutingPaths(topology, bindings, observations, "preflight", 7)
	if err != nil {
		testContext.Fatal(err)
	}
	_, pairs, associations, endpoint, _ := routingDataFixture(testContext)
	plane, err := newRoutingDataSenderPlane(endpoint, associations, pairs, func() error { return nil })
	if err != nil {
		testContext.Fatal(err)
	}
	specification := sgpFailureSpecFixture()
	clock := &sharedRunClock{source: &fakeMeasurementClock{domain: specification.Clock.Domain}, window: *specification.Clock, drain: specification.Drain}
	tracker, err := newFailoverTracker(*specification.SGPFailure, clock, plane, paths)
	if err != nil {
		testContext.Fatal(err)
	}
	return tracker, paths, specification
}

func failoverAlternativeTarget(tracker *failoverTracker, association m3ua.AssociationID) m3ua.MTPTransferPath {
	return m3ua.MTPTransferPath{Path: tracker.alternativePath, SGP: sgpFailureAlternative, ApplicationServer: "primary", AS: tracker.alternativeKey, Association: association, Epoch: 500}
}

func failoverSuccess(target m3ua.MTPTransferPath, size int) m3ua.MTPTransferResult {
	return m3ua.MTPTransferResult{UserDataOctets: size, SuccessfulPaths: []m3ua.MTPTransferPath{target}}
}

func failoverWriteFailure(target m3ua.MTPTransferPath, outcome m3ua.DataSendOutcome) error {
	return &m3ua.MTPTransferError{Failures: []m3ua.MTPTransferFailure{{Target: target, Err: &m3ua.DataWriteError{Outcome: outcome, Err: errors.New("broken pipe")}}}}
}

func TestFailoverTrackerClassifiesEveryOutcome(testContext *testing.T) {
	tracker, paths, _ := failoverTrackerFixture(testContext)
	affected, healthy := uint16(8), uint16(2)
	if !tracker.affected[affected] || tracker.affected[healthy] || len(tracker.failedSenders) != 2 || !tracker.failedSenders[101] || !tracker.alternativeSenders[103] {
		testContext.Fatalf("fixture inventory: affected %v healthy %v failed %v alternative %v", tracker.affected[affected], tracker.affected[healthy], tracker.failedSenders, tracker.alternativeSenders)
	}
	frozen, _ := paths.path(affected)
	healthyPath, _ := paths.path(healthy)
	otherFailed := frozen.Target
	otherFailed.Association = 102
	before, after := tracker.due-1, tracker.due+int64(time.Millisecond)
	for _, test := range []struct {
		name   string
		route  uint16
		result m3ua.MTPTransferResult
		err    error
		at     int64
		class  string
		failed bool
	}{
		{name: "frozen affected", route: affected, result: failoverSuccess(frozen.Target, 128), at: before, class: failoverClassFrozen},
		{name: "frozen healthy after the fault", route: healthy, result: failoverSuccess(healthyPath.Target, 128), at: after, class: failoverClassFrozen},
		{name: "other failed-SGP association", route: affected, result: failoverSuccess(otherFailed, 128), at: after, class: failoverClassFailedSGP},
		{name: "alternative", route: affected, result: failoverSuccess(failoverAlternativeTarget(tracker, 103), 128), at: after, class: failoverClassAlternative},
		{name: "not sent", route: affected, err: failoverWriteFailure(frozen.Target, m3ua.DataNotSent), at: after, class: failoverClassNotSent, failed: true},
		{name: "indeterminate", route: affected, err: failoverWriteFailure(otherFailed, m3ua.DataSendIndeterminate), at: after, class: failoverClassIndeterminate, failed: true},
		{name: "unclassified write failure", route: affected, err: &m3ua.MTPTransferError{Failures: []m3ua.MTPTransferFailure{{Target: frozen.Target, Err: errors.New("closed")}}}, at: after, class: failoverClassOtherFailure, failed: true},
		{name: "selection refused", route: affected, err: &m3ua.MTPSelectionError{MTPRoute: "route-0008"}, at: after, class: failoverClassRefused, failed: true},
	} {
		class, _, err := tracker.classLocked(test.route, 128, test.result, test.err, test.at)
		if err != nil || class != test.class {
			testContext.Errorf("%s: class %q err %v, want %q", test.name, class, err, test.class)
		}
		failed, err := tracker.classify(test.route, 1, 128, test.result, test.err, test.at, test.at+1)
		if err != nil || failed != test.failed {
			testContext.Errorf("%s: classify failed=%v err=%v", test.name, failed, err)
		}
	}
	if tracker.outcomes.sum() != tracker.outcomes.Calls || tracker.outcomes.Unexpected != 0 || tracker.submitted[103] != 2 || tracker.indeterminate[102] != 1 {
		testContext.Fatalf("outcomes %+v submitted %v indeterminate %v", tracker.outcomes, tracker.submitted, tracker.indeterminate)
	}
	for _, test := range []struct {
		name   string
		route  uint16
		result m3ua.MTPTransferResult
		err    error
		at     int64
	}{
		// Route 4 is frozen on sg-b/p0; sg-a/p1 is not its alternative.
		{name: "healthy route moved", route: 4, result: failoverSuccess(failoverAlternativeTarget(tracker, 103), 128), at: after},
		{name: "alternative before the fault", route: affected + 8, result: failoverSuccess(failoverAlternativeTarget(tracker, 103), 128), at: before},
		{name: "moved route switched association", route: affected, result: failoverSuccess(failoverAlternativeTarget(tracker, 104), 128), at: after},
		{name: "alternative in the wrong AS", route: affected + 16, result: failoverSuccess(func() m3ua.MTPTransferPath {
			target := failoverAlternativeTarget(tracker, 103)
			target.AS.RoutingContext++
			return target
		}(), 128), at: after},
		{name: "short write", route: affected, result: failoverSuccess(frozen.Target, 127), at: after},
		{name: "two paths", route: affected, result: m3ua.MTPTransferResult{UserDataOctets: 128, SuccessfulPaths: []m3ua.MTPTransferPath{frozen.Target, frozen.Target}}, at: after},
		{name: "healthy route refused", route: healthy, err: &m3ua.MTPSelectionError{MTPRoute: "route-0002"}, at: after},
		{name: "surviving path failure", route: affected, err: failoverWriteFailure(failoverAlternativeTarget(tracker, 103), m3ua.DataNotSent), at: after},
		{name: "healthy route failure", route: healthy, err: failoverWriteFailure(healthyPath.Target, m3ua.DataNotSent), at: after},
		{name: "unrelated error", route: affected, err: m3ua.ErrEndpointClosed, at: after},
	} {
		calls := tracker.outcomes.Unexpected
		if _, err := tracker.classify(test.route, 2, 128, test.result, test.err, test.at, test.at+1); err == nil || tracker.outcomes.Unexpected != calls+1 {
			testContext.Errorf("%s: accepted", test.name)
		}
	}
	if tracker.unexpectedError == "" || tracker.outcomes.sum() != tracker.outcomes.Calls {
		testContext.Fatalf("unexpected outcomes are not accounted: %+v", tracker.outcomes)
	}
}

// failoverScenario is a consistent passing trial at 1,000 messages/s for 30
// s with the fault at 10 s: 250 affected routes move to sg-a/p1, three
// messages fail on the failed path and three submitted ones are lost in
// flight.
type failoverScenario struct {
	tracker      *failoverTracker
	inputs       failoverInputs
	notification int64
}

func newFailoverScenario(testContext *testing.T) *failoverScenario {
	testContext.Helper()
	tracker, _, specification := failoverTrackerFixture(testContext)
	fault := failoverFault{Kind: sgpFailureKindShutdown, SGP: sgpFailureFailed, Due: tracker.due, Before: tracker.due + 50_000, After: tracker.due + 550_000,
		Associations: []failoverClose{{Association: 1}, {Association: 2}}}
	notification := fault.Before + 1_200_000
	tracker.notifications = []failoverNotification{
		newFailoverNotification(101, sgpFailureFailed, fault.Before+1_000_000, io.EOF),
		newFailoverNotification(102, sgpFailureFailed, notification, io.EOF),
	}
	tracker.outcomes = failoverOutcomes{Calls: 30000, Frozen: 25000, Alternative: 4997, NotSent: 2, Indeterminate: 1}
	tracker.submitted = map[m3ua.AssociationID]uint64{101: 1250, 102: 1250, 103: 3750 + 2499, 104: 3750 + 2498, 105: 3750, 106: 3750, 107: 3750, 108: 3750}
	tracker.indeterminate = map[m3ua.AssociationID]uint64{101: 1}
	for route, affected := range tracker.affected {
		if affected {
			tracker.firstAlternative[route] = notification + int64(route)*100_000
		}
	}
	tracker.first = &failoverFirst{Route: 8, Association: 103, CallStarted: notification + 100_000, Returned: notification + 300_000}
	tracker.lastFailedSGPCall, tracker.lastFailureCall = notification+20_000, notification+10_000
	delivered := map[m3ua.AssociationID]uint64{1: 1248, 2: 1249}
	receiver := &failoverReceiverRecord{Fault: &fault, Bin: sgpFailureBin, AffectedRoutes: 250, MovedRoutes: 250,
		ArrivalBins: make([]uint64, 320), SurvivingBins: make([]uint64, 320), ScheduledBins: make([]uint64, 300)}
	for _, binding := range tracker.bindings {
		unique := tracker.submitted[binding.SenderAssociation]
		if binding.Peer.SGP == sgpFailureFailed {
			unique = delivered[binding.Peer.Association]
		}
		receiver.PerTransport = append(receiver.PerTransport, failoverTransportCount{SGP: binding.Peer.SGP, Association: binding.Peer.Association, Unique: unique})
	}
	for index := range receiver.ArrivalBins {
		switch {
		case index < 100:
			receiver.ArrivalBins[index], receiver.SurvivingBins[index] = 100, 75
		case index == 100:
			receiver.ArrivalBins[index], receiver.SurvivingBins[index] = 94, 90
		case index < 300:
			receiver.ArrivalBins[index], receiver.SurvivingBins[index] = 100, 100
		}
	}
	for index := range receiver.ScheduledBins {
		receiver.ScheduledBins[index] = 100
	}
	receiver.ScheduledBins[100] = 94
	var samples []backlogInterval
	for second := 1; second < 30; second++ {
		samples = append(samples, backlogInterval{Before: time.Duration(second) * time.Second, After: time.Duration(second)*time.Second + time.Millisecond, BacklogLower: 6, BacklogUpper: 7})
	}
	return &failoverScenario{
		tracker: tracker, notification: notification,
		inputs: failoverInputs{
			specification: specification, scheduled: 30000,
			receiver:   runRecord{Side: "receiver", Delivery: deliveryResult{Unique: 29994, Missing: 6}, Failover: &failoverRecord{Receiver: receiver}},
			accounting: windowAccounting{Status: "bounded", Samples: samples},
		},
	}
}

func failoverCriterionByName(record *failoverRecord, name string) failoverCriterion {
	for _, criterion := range record.Criteria {
		if criterion.Name == name {
			return criterion
		}
	}
	return failoverCriterion{}
}

func TestFailoverEvaluationPassesAConsistentTrial(testContext *testing.T) {
	scenario := newFailoverScenario(testContext)
	record := scenario.tracker.evaluate(scenario.inputs)
	if record.Verdict != failoverPass || len(record.Criteria) != 9 {
		testContext.Fatalf("verdict %s criteria %+v", record.Verdict, record.Criteria)
	}
	sender := record.Sender
	if sender.Notification != scenario.notification || sender.FirstAlternative.Latency != 300_000 || sender.RouteSwitch.MovedRoutes != 250 ||
		sender.Recovery.Recovery != int64(10200*time.Millisecond)-(scenario.notification-scenario.inputs.specification.Clock.Start) ||
		sender.Recovery.PostScheduled != sender.Recovery.PostDelivered || sender.Recovery.PostScheduled == 0 ||
		sender.FailedPath.Undelivered != 4 || sender.FailedPath.Unexplained != 0 || sender.FailedPath.Missing != 6 {
		testContext.Fatalf("sender evidence %+v recovery %+v failed path %+v", sender, sender.Recovery, sender.FailedPath)
	}
	result := runRecord{Side: "sender", Failover: record, CPU: CPUObservation{Before: map[string]uint64{"nr_throttled": 0}, After: map[string]uint64{"nr_throttled": 0}}}
	result.evaluate()
	if result.Verdict != verdictPass || result.FixtureVerdict != verdictPass || result.CapacityVerdict != "unavailable" {
		testContext.Fatalf("record verdict %s fixture %s reasons %v", result.Verdict, result.FixtureVerdict, result.Reasons)
	}
}

// A message scheduled in the guard bin just before the fault may have been in
// flight to the failed SGP; losing it is the failure's, not a pre-failure loss.
func TestFailoverPreFailureGuardBinIsTheFailures(testContext *testing.T) {
	scenario := newFailoverScenario(testContext)
	scenario.inputs.receiver.Failover.Receiver.ScheduledBins[99] = 99
	record := scenario.tracker.evaluate(scenario.inputs)
	if got := failoverCriterionByName(record, "pre_failure_nominal"); got.Outcome != failoverPass || got.Measured == nil || *got.Measured != 0 {
		testContext.Fatalf("pre_failure_nominal = %+v", got)
	}
	scenario.inputs.receiver.Failover.Receiver.ScheduledBins[98] = 99
	record = scenario.tracker.evaluate(scenario.inputs)
	if got := failoverCriterionByName(record, "pre_failure_nominal"); got.Outcome != failoverFail || got.Measured == nil || *got.Measured != 1 {
		testContext.Fatalf("a loss in the last bin before the guard = %+v", got)
	}
}

// The notification is the later of the two failed-SGP association ends, so the
// alternative may already carry traffic when it arrives.
func TestFailoverSelectionBeforeTheNotificationIsJudgedAsImmediate(testContext *testing.T) {
	scenario := newFailoverScenario(testContext)
	scenario.tracker.first.CallStarted = scenario.notification - 300_000
	scenario.tracker.first.Returned = scenario.notification - 100_000
	record := scenario.tracker.evaluate(scenario.inputs)
	got := failoverCriterionByName(record, "alternative_selection")
	if got.Outcome != failoverPass || got.Measured == nil || *got.Measured != 0 || record.Sender.FirstAlternative.Latency != -100_000 ||
		!strings.Contains(got.Detail, "returned 100000 ns before the notification") {
		testContext.Fatalf("alternative_selection = %+v, first %+v", got, record.Sender.FirstAlternative)
	}
}

func TestFailoverEvaluationFailsEveryViolatedCriterion(testContext *testing.T) {
	for _, test := range []struct {
		name      string
		criterion string
		outcome   string
		mutate    func(*failoverScenario)
	}{
		{name: "late first alternative", criterion: "alternative_selection", outcome: failoverFail, mutate: func(scenario *failoverScenario) {
			scenario.tracker.first.Returned = scenario.notification + int64(150*time.Millisecond)
		}},
		{name: "failed SGP used after the budget", criterion: "alternative_selection", outcome: failoverFail, mutate: func(scenario *failoverScenario) {
			scenario.tracker.lastFailedSGPCall = scenario.notification + int64(101*time.Millisecond)
		}},
		{name: "failure after the budget", criterion: "alternative_selection", outcome: failoverFail, mutate: func(scenario *failoverScenario) {
			scenario.tracker.lastFailureCall = scenario.notification + int64(101*time.Millisecond)
		}},
		{name: "route never moved", criterion: "alternative_selection", outcome: failoverFail, mutate: func(scenario *failoverScenario) {
			scenario.tracker.firstAlternative[8] = 0
		}},
		{name: "no alternative at all", criterion: "alternative_selection", outcome: failoverFail, mutate: func(scenario *failoverScenario) {
			scenario.tracker.first = nil
		}},
		{name: "slow recovery", criterion: "healthy_path_recovery", outcome: failoverFail, mutate: func(scenario *failoverScenario) {
			bins := scenario.inputs.receiver.Failover.Receiver.SurvivingBins
			for index := 100; index < 112; index++ {
				bins[index] = 89
			}
		}},
		{name: "never recovered", criterion: "healthy_path_recovery", outcome: failoverFail, mutate: func(scenario *failoverScenario) {
			bins := scenario.inputs.receiver.Failover.Receiver.SurvivingBins
			for index := 100; index < len(bins); index++ {
				bins[index] = 75
			}
		}},
		{name: "loss before the fault", criterion: "pre_failure_nominal", outcome: failoverFail, mutate: func(scenario *failoverScenario) {
			scenario.inputs.receiver.Failover.Receiver.ScheduledBins[50] = 99
		}},
		{name: "loss after the milestone", criterion: "full_rate_after_recovery", outcome: failoverFail, mutate: func(scenario *failoverScenario) {
			scenario.inputs.receiver.Failover.Receiver.ScheduledBins[250] = 99
		}},
		{name: "growing backlog after the milestone", criterion: "full_rate_after_recovery", outcome: failoverFail, mutate: func(scenario *failoverScenario) {
			for index := range scenario.inputs.accounting.Samples {
				scenario.inputs.accounting.Samples[index].BacklogLower = uint64(index) * 200
				scenario.inputs.accounting.Samples[index].BacklogUpper = uint64(index)*200 + 20
			}
		}},
		{name: "unexplained missing message", criterion: "failed_path_accounted", outcome: failoverFail, mutate: func(scenario *failoverScenario) {
			scenario.inputs.receiver.Delivery.Missing++
		}},
		{name: "retried or dropped call", criterion: "failed_path_accounted", outcome: failoverFail, mutate: func(scenario *failoverScenario) {
			scenario.tracker.outcomes.Calls++
		}},
		{name: "capped message", criterion: "failed_path_accounted", outcome: failoverFail, mutate: func(scenario *failoverScenario) {
			scenario.inputs.capped = 1
		}},
		{name: "failed SGP delivered more than it took", criterion: "failed_path_accounted", outcome: failoverFail, mutate: func(scenario *failoverScenario) {
			scenario.inputs.receiver.Failover.Receiver.PerTransport[0].Unique = 2000
		}},
		{name: "surviving association lost a message", criterion: "healthy_routes_nominal", outcome: failoverFail, mutate: func(scenario *failoverScenario) {
			scenario.tracker.submitted[105]++
		}},
		{name: "nominal reorder", criterion: "healthy_routes_nominal", outcome: failoverFail, mutate: func(scenario *failoverScenario) {
			scenario.inputs.receiver.Delivery.Reordered = 1
		}},
		{name: "unexpected outcome", criterion: "healthy_routes_nominal", outcome: failoverFail, mutate: func(scenario *failoverScenario) {
			scenario.tracker.outcomes.Unexpected, scenario.tracker.outcomes.Frozen = 1, 24999
		}},
		{name: "surviving association ended", criterion: "transport_failure_notified", outcome: failoverFail, mutate: func(scenario *failoverScenario) {
			scenario.tracker.notifications = append(scenario.tracker.notifications, failoverNotification{Association: 105, At: scenario.notification})
		}},
		{name: "one failed association never notified", criterion: "transport_failure_notified", outcome: failoverFail, mutate: func(scenario *failoverScenario) {
			scenario.tracker.notifications = scenario.tracker.notifications[:1]
		}},
		{name: "notified before the fault", criterion: "transport_failure_notified", outcome: failoverFail, mutate: func(scenario *failoverScenario) {
			scenario.tracker.notifications[0].At = scenario.inputs.receiver.Failover.Receiver.Fault.Before - 1
		}},
		{name: "fault not injected", criterion: "fault_injected", outcome: failoverFail, mutate: func(scenario *failoverScenario) {
			scenario.inputs.receiver.Failover.Receiver.Fault = nil
		}},
		{name: "fault early", criterion: "fault_injected", outcome: failoverFail, mutate: func(scenario *failoverScenario) {
			scenario.inputs.receiver.Failover.Receiver.Fault.Before = scenario.tracker.due - 1
		}},
		{name: "an injected call failed", criterion: "fault_injected", outcome: failoverFail, mutate: func(scenario *failoverScenario) {
			scenario.inputs.receiver.Failover.Receiver.Fault.Associations[1].Error = "setsockopt: invalid argument"
		}},
		{name: "a close that ran into the ABORT fallback", criterion: "failure_kind_observed", outcome: failoverFail, mutate: func(scenario *failoverScenario) {
			fault := scenario.inputs.receiver.Failover.Receiver.Fault
			fault.Associations[1] = failoverClose{Association: 2, Started: fault.Before, Returned: fault.Before + int64(3*time.Second)}
		}},
		{name: "a close that ended in an ABORT", criterion: "failure_kind_observed", outcome: failoverFail, mutate: func(scenario *failoverScenario) {
			scenario.tracker.notifications[0] = newFailoverNotification(101, sgpFailureFailed, scenario.tracker.notifications[0].At, errFailoverUserAbort)
		}},
		{name: "an abort that reached the ASP as a SHUTDOWN", criterion: "failure_kind_observed", outcome: failoverFail, mutate: func(scenario *failoverScenario) {
			failoverScenarioOfKind(scenario, sgpFailureKindAbort)
			scenario.tracker.notifications[1] = newFailoverNotification(102, sgpFailureFailed, scenario.notification, io.EOF)
		}},
		{name: "an abort that reached the ASP as a path failure", criterion: "failure_kind_observed", outcome: failoverFail, mutate: func(scenario *failoverScenario) {
			failoverScenarioOfKind(scenario, sgpFailureKindAbort)
			scenario.tracker.notifications[0] = newFailoverNotification(101, sgpFailureFailed, scenario.tracker.notifications[0].At,
				fmt.Errorf("%w: SCTP association lost (SCTP_COMM_LOST, %s)", m3ua.ErrSCTPNotAlive, sctp.ErrorCauseString(uint32(sctp.SCTP_ERROR_NO_ERROR))))
		}},
	} {
		testContext.Run(test.name, func(testContext *testing.T) {
			scenario := newFailoverScenario(testContext)
			test.mutate(scenario)
			record := scenario.tracker.evaluate(scenario.inputs)
			if got := failoverCriterionByName(record, test.criterion); got.Outcome != test.outcome || record.Verdict != failoverFail {
				testContext.Fatalf("%s = %s (%s), verdict %s", test.criterion, got.Outcome, got.Detail, record.Verdict)
			}
			result := runRecord{Side: "sender", Failover: record}
			result.evaluate()
			if result.Verdict != verdictFail || result.FixtureVerdict != verdictPass || len(result.Reasons) == 0 {
				testContext.Fatalf("record verdict %s fixture %s reasons %v", result.Verdict, result.FixtureVerdict, result.Reasons)
			}
			if cohort := newCohortResult("measurement", result, runRecord{Verdict: verdictPass}, nil); cohort.Verdict != verdictFail {
				testContext.Fatalf("cohort verdict %s", cohort.Verdict)
			}
		})
	}
}

// errFailoverUserAbort is how the library reports an association lost to an
// ABORT carrying the User-Initiated Abort cause.
var errFailoverUserAbort = fmt.Errorf("%w: SCTP association lost (SCTP_COMM_LOST, %s)",
	m3ua.ErrSCTPNotAlive, sctp.ErrorCauseString(uint32(sctp.SCTP_ERROR_USER_ABORT)))

// failoverScenarioOfKind turns the scenario into a trial of the given kind,
// with the failed SGP's associations ending as that kind makes them end on a
// kernel that reports association events.
func failoverScenarioOfKind(scenario *failoverScenario, kind string) {
	recorded := sgpFailureRecordedKind(kind)
	scenario.tracker.spec.Kind = recorded
	scenario.inputs.receiver.Failover.Receiver.Fault.Kind = recorded
	scenario.tracker.associationEvents = associationEventsSupported
	end := error(io.EOF)
	if kind == sgpFailureKindAbort {
		end = errFailoverUserAbort
	}
	for index, notification := range scenario.tracker.notifications {
		scenario.tracker.notifications[index] = newFailoverNotification(notification.Association, notification.SGP, notification.At, end)
	}
}

// A close trial's Close calls must return before the SCTP dependency's
// three-second ABORT fallback could have run: the ASP reads the end of stream
// as soon as the SHUTDOWN arrives, so the end it saw cannot show the fallback.
// An abort trial's Abort does not wait, and is not held to it.
func TestFailoverCloseMustReturnBeforeTheAbortFallback(testContext *testing.T) {
	for _, test := range []struct {
		name    string
		kind    string
		took    time.Duration
		outcome string
	}{
		{name: "close returning just under the bound", kind: sgpFailureKindClose, took: sgpFailureShutdownFallback - 1, outcome: failoverPass},
		{name: "close returning at the bound", kind: sgpFailureKindClose, took: sgpFailureShutdownFallback, outcome: failoverFail},
		{name: "close returning after the fallback", kind: sgpFailureKindClose, took: 3 * time.Second, outcome: failoverFail},
		{name: "abort", kind: sgpFailureKindAbort, took: 3 * time.Second, outcome: failoverPass},
	} {
		testContext.Run(test.name, func(testContext *testing.T) {
			scenario := newFailoverScenario(testContext)
			failoverScenarioOfKind(scenario, test.kind)
			fault := scenario.inputs.receiver.Failover.Receiver.Fault
			fault.Associations[0] = failoverClose{Association: 1, Started: fault.Before, Returned: fault.Before + int64(test.took)}
			record := scenario.tracker.evaluate(scenario.inputs)
			got := failoverCriterionByName(record, "failure_kind_observed")
			if got.Outcome != test.outcome {
				testContext.Fatalf("failure_kind_observed %s (%s), want %s", got.Outcome, got.Detail, test.outcome)
			}
			if test.kind == sgpFailureKindClose && (got.Measured == nil || *got.Measured != int64(test.took) || got.Budget == nil || *got.Budget != int64(sgpFailureShutdownFallback)) {
				testContext.Fatalf("close trial measured %v against budget %v, want %d against %d", got.Measured, got.Budget, test.took, sgpFailureShutdownFallback)
			}
		})
	}
}

// The probe's answer is read as the library reads it: only ENOPROTOOPT means a
// kernel without SCTP_EVENT, and any other failure leaves the question open,
// which an abort trial reports as not measured.
func TestFailoverAssociationEventsAnswer(testContext *testing.T) {
	for _, test := range []struct {
		probe error
		want  string
	}{
		{probe: nil, want: associationEventsSupported},
		{probe: syscall.ENOPROTOOPT, want: associationEventsUnsupported},
		{probe: fmt.Errorf("setsockopt: %w", syscall.ENOPROTOOPT), want: associationEventsUnsupported},
		{probe: syscall.EPROTONOSUPPORT, want: "unknown: " + syscall.EPROTONOSUPPORT.Error()},
	} {
		if got := associationEventsAnswer(test.probe); got != test.want {
			testContext.Errorf("probe %v: %q, want %q", test.probe, got, test.want)
		}
	}
}

// The kernel is asked once, when the tracker is built before the cohort's
// measured window opens, and not again when the watch starts inside it.
func TestFailoverAssociationEventsAreAskedBeforeTheWindow(testContext *testing.T) {
	asked := 0
	previous := failoverAssociationEventsProbe
	failoverAssociationEventsProbe = func() string {
		asked++
		return "probed"
	}
	defer func() { failoverAssociationEventsProbe = previous }()
	tracker, _, _ := failoverTrackerFixture(testContext)
	if asked != 1 || tracker.associationEvents != "probed" {
		testContext.Fatalf("building the tracker asked %d times and recorded %q, want once and the answer", asked, tracker.associationEvents)
	}
	tracker.watch(nil)
	tracker.finish()
	if asked != 1 {
		testContext.Fatalf("the watch asked the kernel again inside the measured window (%d probes)", asked)
	}
}

// How an association ended is classified when the watch fires, from the error
// the library reports.
func TestFailoverNotificationClassifiesHowTheAssociationEnded(testContext *testing.T) {
	for _, test := range []struct {
		name                   string
		err                    error
		eof, lost, userAborted bool
	}{
		{name: "completed SHUTDOWN", err: io.EOF, eof: true},
		{name: "ABORT", err: errFailoverUserAbort, lost: true, userAborted: true},
		{name: "path failure", err: fmt.Errorf("%w: SCTP association lost (SCTP_COMM_LOST, %s)", m3ua.ErrSCTPNotAlive,
			sctp.ErrorCauseString(uint32(sctp.SCTP_ERROR_NO_ERROR))), lost: true},
		{name: "user abort named outside a loss", err: errors.New(sctp.ErrorCauseString(uint32(sctp.SCTP_ERROR_USER_ABORT)))},
		{name: "local close", err: m3ua.ErrAssociationClosed},
		{name: "no cause", err: nil},
	} {
		got := newFailoverNotification(101, sgpFailureFailed, 7, test.err)
		if got.EndOfStream != test.eof || got.CommunicationLost != test.lost || got.UserAbort != test.userAborted || got.Error != fmt.Sprint(test.err) {
			testContext.Errorf("%s: %+v, want end of stream %v, lost %v, user abort %v", test.name, got, test.eof, test.lost, test.userAborted)
		}
	}
}

// Each kind passes on the end it produces, and an abort trial is not measured,
// rather than passed, where the kernel does not report association events.
func TestFailoverFailureKindObservedPerKind(testContext *testing.T) {
	for _, test := range []struct {
		name, kind, events, outcome, verdict string
	}{
		{name: "close", kind: sgpFailureKindClose, events: associationEventsSupported, outcome: failoverPass, verdict: failoverPass},
		{name: "close without association events", kind: sgpFailureKindClose, events: associationEventsUnsupported, outcome: failoverPass, verdict: failoverPass},
		{name: "abort", kind: sgpFailureKindAbort, events: associationEventsSupported, outcome: failoverPass, verdict: failoverPass},
		{name: "abort without association events", kind: sgpFailureKindAbort, events: associationEventsUnsupported, outcome: failoverNotMeasured, verdict: verdictInconclusive},
		{name: "abort with an unanswered probe", kind: sgpFailureKindAbort, events: "unknown: SCTP is unsupported", outcome: failoverNotMeasured, verdict: verdictInconclusive},
	} {
		testContext.Run(test.name, func(testContext *testing.T) {
			scenario := newFailoverScenario(testContext)
			failoverScenarioOfKind(scenario, test.kind)
			scenario.tracker.associationEvents = test.events
			record := scenario.tracker.evaluate(scenario.inputs)
			got := failoverCriterionByName(record, "failure_kind_observed")
			if got.Outcome != test.outcome || record.Verdict != test.verdict || record.Sender.AssociationEvents != test.events {
				testContext.Fatalf("failure_kind_observed %s (%s), verdict %s, recorded events %q", got.Outcome, got.Detail, record.Verdict, record.Sender.AssociationEvents)
			}
			for _, notification := range record.Sender.Notifications {
				if notification.EndOfStream == (test.kind == sgpFailureKindAbort) || notification.UserAbort != (test.kind == sgpFailureKindAbort) {
					testContext.Fatalf("recorded notification %+v for a %s trial", notification, test.kind)
				}
			}
		})
	}
}

func TestFailoverEvaluationIsInconclusiveWithoutEnoughEvidence(testContext *testing.T) {
	scenario := newFailoverScenario(testContext)
	scenario.inputs.accounting.Samples = scenario.inputs.accounting.Samples[:15]
	record := scenario.tracker.evaluate(scenario.inputs)
	if got := failoverCriterionByName(record, "full_rate_after_recovery"); got.Outcome != failoverNotMeasured || record.Verdict != verdictInconclusive {
		testContext.Fatalf("full rate %s (%s), verdict %s", got.Outcome, got.Detail, record.Verdict)
	}
	result := runRecord{Side: "sender", Failover: record}
	result.evaluate()
	if result.Verdict != verdictInconclusive {
		testContext.Fatalf("record verdict %s", result.Verdict)
	}
	passing := newFailoverScenario(testContext)
	fatal := runRecord{Side: "sender", Failover: passing.tracker.evaluate(passing.inputs), FatalError: "boom"}
	fatal.evaluate()
	if fatal.Verdict != verdictInvalid || fatal.FixtureVerdict != verdictInvalid {
		testContext.Fatalf("fatal record verdict %s", fatal.Verdict)
	}
}

// fakeFailoverPeerAssociation is a peer association the injection can close or
// abort.
type fakeFailoverPeerAssociation struct {
	fakeRoutingDataAssociation
	closes atomic.Int32
	aborts atomic.Int32
}

func (association *fakeFailoverPeerAssociation) Close() error {
	association.closes.Add(1)
	return nil
}

func (association *fakeFailoverPeerAssociation) Abort() error {
	association.aborts.Add(1)
	return nil
}

func failoverReceiverFixture(testContext *testing.T, offset time.Duration) (*receiverControl, *fakeMeasurementClock, routingPathMap, []*fakeFailoverPeerAssociation) {
	testContext.Helper()
	return failoverReceiverFixtureOfKind(testContext, offset, sgpFailureKindClose)
}

func failoverReceiverFixtureOfKind(testContext *testing.T, offset time.Duration, kind string) (*receiverControl, *fakeMeasurementClock, routingPathMap, []*fakeFailoverPeerAssociation) {
	testContext.Helper()
	topology, pairs, _, paths := routedPeerPathsFixture(testContext)
	control := newReceiverControl(8, maxOutstanding)
	control.enableRouted(modeRouted)
	specification := sgpFailureSpecFixture()
	clock := &fakeMeasurementClock{domain: specification.Clock.Domain}
	clock.now.Store(specification.Clock.Start - int64(time.Second))
	control.clock = clock
	control.cpuStatPath = writeCPUStatFixture(testContext, "usage_usec 1\nnr_throttled 0\n")
	control.enableFailover(context.Background(), offset, kind)
	if err := control.freezeRoutes(paths); err != nil {
		testContext.Fatal(err)
	}
	associations := make([]routingDataAssociation, len(pairs))
	fakes := make([]*fakeFailoverPeerAssociation, len(pairs))
	for index, pair := range pairs {
		fakes[index] = &fakeFailoverPeerAssociation{fakeRoutingDataAssociation: fakeRoutingDataAssociation{id: pair.Binding.Peer.Association, epoch: pair.Binding.PeerEpoch, maximum: 16}}
		associations[index] = fakes[index]
	}
	if err := control.freezeFailover(topology, pairs, associations); err != nil {
		testContext.Fatal(err)
	}
	for index := range 8 {
		control.setAssociationReady(index, 16)
	}
	return control, clock, paths, fakes
}

func failoverAlternativeMessage(testContext *testing.T, control *receiverControl, specification runSpec, index uint64, transport routingTransport) *m3ua.DataMessage {
	testContext.Helper()
	failover := control.routed.failover
	binding := failover.alternatives[transport]
	identity := planRouteMessage(specification.Cohort, specification.Seed, index)
	target := m3ua.MTPTransferPath{AS: failover.alternativeKey}
	return routingReceivedMessage(testContext, identity, specification.Payload.size(index), target, binding)
}

func TestFailoverReceiverRefusesUndeclaredOrMismatchedFailures(testContext *testing.T) {
	specification := sgpFailureSpecFixture()
	plain, _ := routedReadyControl(testContext, modeRouted)
	plainClock := &fakeMeasurementClock{domain: specification.Clock.Domain}
	plainClock.now.Store(specification.Clock.Start - int64(time.Second))
	plain.clock = plainClock
	if err := plain.reset(specification); err == nil || !errors.Is(err, errInvalidRunSpec) || !strings.Contains(err.Error(), "-sgp-failure") {
		testContext.Fatalf("receiver without -sgp-failure accepted a failure cohort: %v", err)
	}
	control, clock, _, _ := failoverReceiverFixture(testContext, 11*time.Second)
	if err := control.reset(specification); err == nil || !strings.Contains(err.Error(), "differs") {
		testContext.Fatalf("receiver accepted a failure at another offset: %v", err)
	}
	clock.now.Store(specification.Clock.Start - int64(time.Second))
	nominal := copyRunSpec(specification)
	nominal.SGPFailure = nil
	if err := control.reset(nominal); err != nil || control.routed.failover.cohort != nil {
		testContext.Fatalf("failure-capable receiver refused a nominal cohort: %v", err)
	}
}

func TestFailoverReceiverInjectsOnceAndValidatesTheMove(testContext *testing.T) {
	specification := sgpFailureSpecFixture()
	control, clock, paths, fakes := failoverReceiverFixture(testContext, 10*time.Second)
	if err := control.reset(specification); err != nil {
		testContext.Fatal(err)
	}
	failover := control.routed.failover
	failedTransports := make([]routingTransport, 0, 2)
	for transport := range failover.failed {
		failedTransports = append(failedTransports, transport)
	}
	alternatives := make([]routingTransport, 0, 2)
	for transport := range failover.alternatives {
		alternatives = append(alternatives, transport)
	}
	if control.failoverReaderEnded(failedTransports[0], errors.New("early")) {
		testContext.Fatal("a failed-SGP read error before the fault was treated as the fault")
	}
	if err := control.start(); err != nil {
		testContext.Fatal(err)
	}
	clock.now.Store(specification.Clock.Start + int64(time.Second))
	// Route 8 is frozen on sg-a/p0; before the fault it must arrive there.
	early := failoverAlternativeMessage(testContext, control, specification, 8, alternatives[0])
	if outcome := control.recordRouted(alternatives[0], early); outcome != recordInvalid {
		testContext.Fatalf("alternative arrival before the fault = %v, want invalid", outcome)
	}
	for _, index := range []uint64{1008, 3008} {
		transport, message := routedTimedMessage(testContext, paths, specification, index)
		if outcome := control.recordRouted(transport, message); outcome != recordUnique {
			testContext.Fatalf("frozen arrival %d = %v", index, outcome)
		}
	}
	clock.now.Store(sgpFailureInstant(specification))
	deadline := time.Now().Add(5 * time.Second)
	for {
		control.mutex.Lock()
		fault := failover.cohort.fault
		control.mutex.Unlock()
		if fault != nil {
			break
		}
		if time.Now().After(deadline) {
			testContext.Fatal("the fault was never recorded")
		}
		time.Sleep(time.Millisecond)
	}
	closed := 0
	for _, fake := range fakes {
		closed += int(fake.closes.Load())
		if fake.closes.Load() > 1 {
			testContext.Fatal("an association was closed twice")
		}
	}
	if closed != 2 {
		testContext.Fatalf("closed %d associations, want the failed SGP's two", closed)
	}
	if !control.failoverReaderEnded(failedTransports[0], errors.New("shutdown")) || control.failoverReaderEnded(alternatives[0], errors.New("other")) {
		testContext.Fatal("reader-end classification after the fault is wrong")
	}
	clock.now.Add(int64(50 * time.Millisecond))
	moved := failoverAlternativeMessage(testContext, control, specification, 4008, alternatives[0])
	if outcome := control.recordRouted(alternatives[0], moved); outcome != recordUnique {
		testContext.Fatalf("moved arrival = %v", outcome)
	}
	// The failed SGP hands over an older message after the move: a failover
	// reorder, not a nominal one.
	transport, older := routedTimedMessage(testContext, paths, specification, 2008)
	if outcome := control.recordRouted(transport, older); outcome != recordUnique {
		testContext.Fatalf("late failed-SGP arrival = %v", outcome)
	}
	switched := failoverAlternativeMessage(testContext, control, specification, 5008, alternatives[1])
	if outcome := control.recordRouted(alternatives[1], switched); outcome != recordInvalid {
		testContext.Fatalf("moved route on a second alternative association = %v, want invalid", outcome)
	}
	// Route 4 is frozen on sg-b/p0 and never moves.
	healthy := failoverAlternativeMessage(testContext, control, specification, 4004, alternatives[0])
	if outcome := control.recordRouted(alternatives[0], healthy); outcome != recordInvalid {
		testContext.Fatalf("healthy route on the alternative = %v, want invalid", outcome)
	}
	record := control.result()
	receiver := record.Failover.Receiver
	if receiver.Fault == nil || len(receiver.Fault.Associations) != 2 || receiver.Fault.Before < receiver.Fault.Due || len(receiver.ReaderEnds) != 1 ||
		receiver.MovedRoutes != 1 || receiver.AlternativeArrivals != 1 || receiver.FailoverReordered != 1 || receiver.AffectedRoutes != 250 {
		testContext.Fatalf("receiver evidence %+v", receiver)
	}
	if record.Delivery.Reordered != 0 || record.Delivery.Invalid != 3 || record.Delivery.Unique != 4 {
		testContext.Fatalf("delivery %+v", record.Delivery)
	}
	var arrivals, surviving, scheduled uint64
	for index := range receiver.ArrivalBins {
		arrivals += receiver.ArrivalBins[index]
		surviving += receiver.SurvivingBins[index]
	}
	for _, count := range receiver.ScheduledBins {
		scheduled += count
	}
	if arrivals != 4 || surviving != 1 || scheduled != 4 || receiver.ScheduledBins[int(time.Duration(4008*uint64(time.Second)/specification.Rate)/sgpFailureBin)] != 1 {
		testContext.Fatalf("bins: arrivals %d surviving %d scheduled %d", arrivals, surviving, scheduled)
	}
	// Three deliberately invalid arrivals make the receiver fixture invalid.
	if record.Verdict != verdictInvalid || len(record.Reasons) != 1 || record.Reasons[0] != "receiver observed duplicate, invalid, reordered, or late traffic" {
		testContext.Fatalf("receiver verdict %s reasons %v", record.Verdict, record.Reasons)
	}
	if err := control.stop(); err != nil {
		testContext.Fatal(err)
	}
	clock.now.Store(specification.Clock.End + int64(3*time.Second))
	again := copyRunSpec(specification)
	again.Clock = &sharedClockWindow{Domain: specification.Clock.Domain, Start: clock.now.Load() + int64(time.Second), End: clock.now.Load() + int64(31*time.Second)}
	if err := control.reset(again); err == nil {
		testContext.Fatal("receiver accepted a second failure cohort after the fault")
	}
}

// Only the failed SGP handing over, after the fault, a message it read before
// it is the failover's reorder. A reorder before the fault, or one the
// alternative delivers, stays nominal and fails the healthy-routes criterion.
func TestFailoverReorderExcuseIsOnlyTheFailedSGPsLateHandover(testContext *testing.T) {
	specification := sgpFailureSpecFixture()
	control, clock, paths, _ := failoverReceiverFixture(testContext, 10*time.Second)
	if err := control.reset(specification); err != nil {
		testContext.Fatal(err)
	}
	failover := control.routed.failover
	alternatives := make([]routingTransport, 0, 2)
	for transport := range failover.alternatives {
		alternatives = append(alternatives, transport)
	}
	if err := control.start(); err != nil {
		testContext.Fatal(err)
	}
	record := func(transport routingTransport, message *m3ua.DataMessage) {
		testContext.Helper()
		if outcome := control.recordRouted(transport, message); outcome != recordUnique {
			testContext.Fatalf("arrival = %v, want unique", outcome)
		}
	}
	clock.now.Store(specification.Clock.Start + int64(time.Second))
	// Route 8 is frozen on sg-a/p0: a late arrival there before the fault is
	// a nominal reorder.
	for _, index := range []uint64{3008, 1008} {
		transport, message := routedTimedMessage(testContext, paths, specification, index)
		record(transport, message)
	}
	clock.now.Store(sgpFailureInstant(specification))
	deadline := time.Now().Add(5 * time.Second)
	for {
		control.mutex.Lock()
		fault := failover.cohort.fault
		control.mutex.Unlock()
		if fault != nil {
			break
		}
		if time.Now().After(deadline) {
			testContext.Fatal("the fault was never recorded")
		}
		time.Sleep(time.Millisecond)
	}
	clock.now.Add(int64(50 * time.Millisecond))
	// After the move, a late arrival the alternative delivers is nominal too.
	for _, index := range []uint64{5008, 4008} {
		record(alternatives[0], failoverAlternativeMessage(testContext, control, specification, index, alternatives[0]))
	}
	// The failed SGP's late handover of an older message is the failover's.
	transport, older := routedTimedMessage(testContext, paths, specification, 2008)
	record(transport, older)
	result := control.result()
	if result.Failover.Receiver.FailoverReordered != 1 || result.Delivery.Reordered != 2 {
		testContext.Fatalf("failover reorders %d, nominal reorders %d; want 1 and 2", result.Failover.Receiver.FailoverReordered, result.Delivery.Reordered)
	}
	if result.Verdict != verdictInvalid {
		testContext.Fatalf("receiver verdict %s with nominal reorders", result.Verdict)
	}
}

func TestFailoverInjectionCancelsWithItsCohort(testContext *testing.T) {
	specification := sgpFailureSpecFixture()
	control, _, _, fakes := failoverReceiverFixture(testContext, 10*time.Second)
	if err := control.reset(specification); err != nil {
		testContext.Fatal(err)
	}
	if err := control.start(); err != nil {
		testContext.Fatal(err)
	}
	if err := control.stop(); err != nil {
		testContext.Fatal(err)
	}
	time.Sleep(3 * sgpFailureWaitSlice)
	for _, fake := range fakes {
		if fake.closes.Load() != 0 {
			testContext.Fatal("a stopped cohort still injected its fault")
		}
	}
	if control.routed.failover.injected.Load() {
		testContext.Fatal("a stopped cohort marked the fault injected")
	}
}

func TestSGPFailureKindFlagIsOptInAndBounded(testContext *testing.T) {
	config, err := parseConfig(sgpFailureASPArguments())
	if err != nil || config.SGPFailureKind != sgpFailureKindClose {
		testContext.Fatalf("a failure trial without -sgp-failure-kind is not a close trial: %q, %v", config.SGPFailureKind, err)
	}
	for _, kind := range []string{sgpFailureKindClose, sgpFailureKindAbort} {
		sender, err := parseConfig(sgpFailureASPArguments("-sgp-failure-kind=" + kind))
		if err != nil || sender.SGPFailureKind != kind {
			testContext.Errorf("sender -sgp-failure-kind=%s: %q, %v", kind, sender.SGPFailureKind, err)
		}
		receiver, err := parseConfig([]string{"-role=sgp", "-mode=routed", "-sctp-address=127.0.0.1:2905", "-associations=8", "-same-host-clock", "-sgp-failure=10s", "-sgp-failure-kind=" + kind})
		if err != nil || receiver.SGPFailureKind != kind {
			testContext.Errorf("receiver -sgp-failure-kind=%s: %q, %v", kind, receiver.SGPFailureKind, err)
		}
	}
	receiver := []string{"-role=sgp", "-mode=routed", "-sctp-address=127.0.0.1:2905", "-associations=8", "-same-host-clock"}
	for name, arguments := range map[string][]string{
		"unknown kind":            sgpFailureASPArguments("-sgp-failure-kind=blackhole"),
		"recorded kind name":      sgpFailureASPArguments("-sgp-failure-kind=shutdown"),
		"upper case":              sgpFailureASPArguments("-sgp-failure-kind=ABORT"),
		"empty":                   sgpFailureASPArguments("-sgp-failure-kind="),
		"abort without the trial": append(append([]string(nil), receiver...), "-sgp-failure-kind=abort"),
		"close without the trial": append(append([]string(nil), receiver...), "-sgp-failure-kind=close"),
	} {
		if _, err := parseConfig(arguments); err == nil {
			testContext.Errorf("%s: accepted", name)
		}
	}
}

// A close trial's declaration is byte-identical to the one the fixture made
// while close was its only kind, whether -sgp-failure-kind is left unset or
// names close. An abort trial's differs in its kind, so the two can never be
// taken for each other, and each receiver accepts only its own.
func TestSGPFailureKindIsPartOfTheDeclaration(testContext *testing.T) {
	const closeDeclaration = `{"kind":"shutdown","sgp":{"SignallingGateway":"sg-a","SignallingGatewayProcess":"p0"},"alternative":{"SignallingGateway":"sg-a","SignallingGatewayProcess":"p1"},"offset_ns":10000000000,"selection_budget_ns":100000000,"recovery_budget_ns":1000000000,"recovery_percent":90,"bin_ns":100000000}`
	for name, arguments := range map[string][]string{
		"unset": sgpFailureASPArguments(),
		"close": sgpFailureASPArguments("-sgp-failure-kind=close"),
		"abort": sgpFailureASPArguments("-sgp-failure-kind=abort"),
	} {
		config, err := parseConfig(arguments)
		if err != nil {
			testContext.Fatal(err)
		}
		encoded, err := json.Marshal(newSGPFailureSpec(config.SGPFailure, config.SGPFailureKind))
		want := closeDeclaration
		if name == "abort" {
			want = strings.Replace(closeDeclaration, `"kind":"shutdown"`, `"kind":"abort"`, 1)
		}
		if err != nil || string(encoded) != want {
			testContext.Errorf("%s declaration:\n got %s\nwant %s", name, encoded, want)
		}
	}

	closed, aborted := sgpFailureSpecFixture(), sgpFailureSpecFixtureOfKind(sgpFailureKindAbort)
	if sameRunSpec(closed, aborted) {
		testContext.Fatal("sameRunSpec equates an abort trial with a close trial")
	}
	if copied := copyRunSpec(aborted); !sameRunSpec(copied, aborted) || copied.SGPFailure.Kind != sgpFailureKindAbort {
		testContext.Fatalf("copyRunSpec loses the abort kind: %+v", copied.SGPFailure)
	}
	for _, receiverKind := range []string{sgpFailureKindClose, sgpFailureKindAbort} {
		for _, declaredKind := range []string{sgpFailureKindClose, sgpFailureKindAbort} {
			err := validateSGPFailureSpec(sgpFailureSpecFixtureOfKind(declaredKind), 10*time.Second, receiverKind)
			switch {
			case receiverKind == declaredKind && err != nil:
				testContext.Errorf("a %s receiver refused its own kind: %v", receiverKind, err)
			case receiverKind != declaredKind && (err == nil || !strings.Contains(err.Error(), "-sgp-failure-kind")):
				testContext.Errorf("a %s receiver answered a %s declaration with %v, want a -sgp-failure-kind refusal", receiverKind, declaredKind, err)
			}
		}
	}
}

// waitFailoverFault waits for the injection to record its fault.
func waitFailoverFault(testContext *testing.T, control *receiverControl) failoverFault {
	testContext.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		control.mutex.Lock()
		fault := control.routed.failover.cohort.fault
		control.mutex.Unlock()
		if fault != nil {
			return *fault
		}
		if time.Now().After(deadline) {
			testContext.Fatal("the fault was never recorded")
		}
		time.Sleep(time.Millisecond)
	}
}

// The receiver ends the failed SGP's associations with the call its own
// -sgp-failure-kind names, never the other, and its record carries the kind
// and names the failure the trial did not measure.
func TestFailoverReceiverInjectsItsKind(testContext *testing.T) {
	for _, test := range []struct {
		kind            string
		closes, aborts  int32
		measured, other string
	}{
		{kind: sgpFailureKindClose, closes: 2, measured: "graceful_failure", other: "abortive_failure"},
		{kind: sgpFailureKindAbort, aborts: 2, measured: "abortive_failure", other: "graceful_failure"},
	} {
		testContext.Run(test.kind, func(testContext *testing.T) {
			specification := sgpFailureSpecFixtureOfKind(test.kind)
			control, clock, _, fakes := failoverReceiverFixtureOfKind(testContext, 10*time.Second, test.kind)
			if err := control.reset(specification); err != nil {
				testContext.Fatal(err)
			}
			if err := control.start(); err != nil {
				testContext.Fatal(err)
			}
			clock.now.Store(sgpFailureInstant(specification))
			fault := waitFailoverFault(testContext, control)
			var closes, aborts int32
			for _, fake := range fakes {
				closes += fake.closes.Load()
				aborts += fake.aborts.Load()
			}
			if closes != test.closes || aborts != test.aborts {
				testContext.Fatalf("injection made %d Close and %d Abort calls, want %d and %d", closes, aborts, test.closes, test.aborts)
			}
			if want := sgpFailureRecordedKind(test.kind); fault.Kind != want || len(fault.Associations) != 2 {
				testContext.Fatalf("recorded fault %+v, want kind %q on two associations", fault, want)
			}
			record := control.result()
			if record.Failover == nil || record.Failover.Spec.Kind != sgpFailureRecordedKind(test.kind) {
				testContext.Fatalf("receiver record declares %+v", record.Failover)
			}
			if _, listed := record.UnsupportedModes[test.measured]; listed {
				testContext.Errorf("the record lists the measured %s as unavailable: %v", test.measured, record.UnsupportedModes)
			}
			if _, listed := record.UnsupportedModes[test.other]; !listed {
				testContext.Errorf("the record does not name the unmeasured %s: %v", test.other, record.UnsupportedModes)
			}
		})
	}
}

// The fault criterion names the call the fault made, and fails when either
// call returned an error. A close trial's text is the one it had while close
// was the only kind.
func TestFailoverFaultCriterionNamesTheInjectedCall(testContext *testing.T) {
	tracker, _, specification := failoverTrackerFixture(testContext)
	for _, test := range []struct {
		kind, callErr, outcome, want string
	}{
		{sgpFailureKindShutdown, "", failoverPass, "shutdown of sg-a/p0 at offset 10.00005s (50000 ns after due); both Association.Close calls returned within 500000 ns"},
		{sgpFailureKindAbort, "", failoverPass, "abort of sg-a/p0 at offset 10.00005s (50000 ns after due); both Association.Abort calls returned within 500000 ns"},
		{sgpFailureKindShutdown, "broken pipe", failoverFail, "shutdown of sg-a/p0 at offset 10.00005s (50000 ns after due); both Association.Close calls returned within 500000 ns; association 2 Close: broken pipe"},
		{sgpFailureKindAbort, "broken pipe", failoverFail, "abort of sg-a/p0 at offset 10.00005s (50000 ns after due); both Association.Abort calls returned within 500000 ns; association 2 Abort: broken pipe"},
	} {
		fault := &failoverFault{Kind: test.kind, SGP: sgpFailureFailed, Due: tracker.due, Before: tracker.due + 50_000, After: tracker.due + 550_000,
			Associations: []failoverClose{{Association: 1}, {Association: 2, Error: test.callErr}}}
		criterion := tracker.faultCriterion(&failoverReceiverRecord{Fault: fault}, specification.Clock)
		if criterion.Outcome != test.outcome || criterion.Detail != test.want {
			testContext.Errorf("%s fault criterion %s:\n got %s\nwant %s", test.kind, criterion.Outcome, criterion.Detail, test.want)
		}
	}
}
