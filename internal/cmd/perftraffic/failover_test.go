package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua"
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
	domain := sharedClockDomain{Clock: "CLOCK_MONOTONIC", BootID: "test-boot", TimeNamespace: "monotonic-offset:0.000000000", Resolution: 1}
	specification := routedSpec(modeRouted)
	specification.Rate, specification.Duration, specification.Expected = 1000, 30*time.Second, 30000
	specification.Clock = &sharedClockWindow{Domain: domain, Start: int64(100 * time.Second), End: int64(130 * time.Second)}
	specification.SGPFailure = newSGPFailureSpec(10 * time.Second)
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
	if err := validateSGPFailureSpec(failure, 10*time.Second); err != nil {
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
		if err := validateSGPFailureSpec(specification, test.offset); err == nil {
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
		{Association: 101, SGP: sgpFailureFailed, At: fault.Before + 1_000_000},
		{Association: 102, SGP: sgpFailureFailed, At: notification},
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
	if record.Verdict != failoverPass || len(record.Criteria) != 7 {
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

// fakeFailoverPeerAssociation is a peer association the injection can close.
type fakeFailoverPeerAssociation struct {
	fakeRoutingDataAssociation
	closes atomic.Int32
}

func (association *fakeFailoverPeerAssociation) Close() error {
	association.closes.Add(1)
	return nil
}

func failoverReceiverFixture(testContext *testing.T, offset time.Duration) (*receiverControl, *fakeMeasurementClock, routingPathMap, []*fakeFailoverPeerAssociation) {
	testContext.Helper()
	topology, pairs, _, paths := routedPeerPathsFixture(testContext)
	control := newReceiverControl(8, maxOutstanding)
	control.enableRouted(modeRouted)
	specification := sgpFailureSpecFixture()
	clock := &fakeMeasurementClock{domain: specification.Clock.Domain}
	clock.now.Store(specification.Clock.Start - int64(time.Second))
	control.clock = clock
	control.cpuStatPath = writeCPUStatFixture(testContext, "usage_usec 1\nnr_throttled 0\n")
	control.enableFailover(context.Background(), offset)
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
	if record.Verdict != verdictInvalid || len(record.Reasons) != 1 || record.Reasons[0] != "receiver observed duplicate, invalid or late traffic" {
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
