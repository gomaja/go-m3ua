package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua"
)

// finishFixture is a completed SSNM-loaded measurement as finish sees it: the
// SGP's per-message log behind a stub control endpoint, subscribers that
// consumed every reported position, the SGP record's generator view and the
// ASP store's final knowledge.
type finishFixture struct {
	config ssnmConfig
	// delay is how long after its report every healthy subscriber receives a
	// message.
	delay     time.Duration
	log       ssnmReportsResponse
	receiver  *ssnmRecord
	knowledge m3ua.SSNMSnapshot
	pause     *ssnmPauseRecord
	// events are extra events every subscriber observes after the plan.
	events []m3ua.SSNMEvent
}

const (
	finishAnchor   = int64(1_000_000_000)
	finishMessages = 20
	finishSent     = finishMessages + 2
)

func steadyFinishFixture() *finishFixture {
	return newFinishFixture(ssnmConfig{Rate: 1000, APCs: 1, Records: 8, Subscribers: 2}, 2*time.Millisecond)
}

func largeFinishFixture(delay time.Duration) *finishFixture {
	return newFinishFixture(ssnmConfig{Rate: 1000, APCs: ssnmMaxAPCs, Records: ssnmMaxAPCs, Subscribers: 2}, delay)
}

func pausedFinishFixture(resync, recovery time.Duration) *finishFixture {
	fixture := newFinishFixture(ssnmConfig{Rate: 1000, APCs: 1, Records: 8, Subscribers: 2, Pause: ssnmPause{Offset: time.Millisecond, Duration: time.Millisecond}}, 2*time.Millisecond)
	fixture.pause = cleanPause()
	fixture.pause.ResyncNS, fixture.pause.RecoveryNS = int64(resync), int64(recovery)
	return fixture
}

func newFinishFixture(config ssnmConfig, delay time.Duration) *finishFixture {
	config.Budgets = ssnmBudgets{ApplyP99: ssnmDefaultApplyP99Budget, Resync: ssnmDefaultResyncBudget, Recovery: ssnmDefaultRecoveryBudget}
	plan := ssnmPlan{records: config.Records, apcs: config.APCs}
	log := ssnmReportsResponse{State: ssnmGeneratorComplete, SentTotal: finishSent}
	for message := uint64(0); message < finishSent; message++ {
		reported := finishAnchor + ssnmScheduled(config.Rate, message)
		log.Reports = append(log.Reports, reported)
		log.Completions = append(log.Completions, reported+int64(10*time.Microsecond))
	}
	return &finishFixture{
		config: config, delay: delay, log: log,
		receiver:  &ssnmRecord{Generator: &ssnmGeneratorRecord{State: ssnmGeneratorComplete}},
		knowledge: snapshotAfter(plan, testPartition, plan.preloadMessages()+finishSent),
	}
}

// run builds the sender run, feeds the subscribers and calls finish.
func (fixture *finishFixture) run(testContext *testing.T) *ssnmRecord {
	testContext.Helper()
	original := startupDiagnosticWriter
	startupDiagnosticWriter = io.Discard
	testContext.Cleanup(func() { startupDiagnosticWriter = original })
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/ssnm/reports" {
			http.NotFound(writer, request)
			return
		}
		_ = json.NewEncoder(writer).Encode(fixture.log)
	}))
	testContext.Cleanup(server.Close)

	plan := ssnmPlan{records: fixture.config.Records, apcs: fixture.config.APCs}
	windowEnd := finishAnchor + ssnmScheduled(fixture.config.Rate, finishMessages)
	run := &ssnmSenderRun{
		config: fixture.config, plan: plan, associations: 1, peerControl: server.URL,
		limits: ssnmStoreLimits(fixture.config, 1), cancel: func() {},
		knowledge: func() m3ua.SSNMSnapshot { return fixture.knowledge },
	}
	for index := 0; index < fixture.config.Subscribers; index++ {
		paused := index == 0 && fixture.config.Pause.enabled()
		run.subscribers = append(run.subscribers, newSSNMSubscriber(index, paused, plan, fixture.config.Rate, 1, 256, 1<<20))
	}
	specification := runSpec{Clock: &sharedClockWindow{Start: finishAnchor, End: windowEnd}}
	if err := run.attach(&specification, ssnmPhaseMeasurement); err != nil {
		testContext.Fatal(err)
	}
	preload := plan.preloadMessages()
	for _, subscriber := range run.subscribers {
		if subscriber.paused {
			subscriber.observe(m3ua.SSNMEvent{Kind: m3ua.SSNMContinuityLostEvent, ContinuityLost: true}, 0)
			if fixture.pause != nil {
				pause := *fixture.pause
				subscriber.pause = &pause
			}
		}
		for position := uint64(0); position < preload+finishSent; position++ {
			received := finishAnchor
			if position >= preload {
				received = finishAnchor + ssnmScheduled(fixture.config.Rate, position-preload) + int64(fixture.delay)
			}
			subscriber.observe(planEvent(plan, testPartition, position), received)
		}
		for _, event := range fixture.events {
			subscriber.observe(event, 0)
		}
	}
	measurement := &cohortResult{Receiver: runRecord{SSNM: fixture.receiver}}
	run.finish(context.Background(), measurement)
	if measurement.Sender.SSNM == nil {
		testContext.Fatal("finish attached no ssnm record")
	}
	return measurement.Sender.SSNM
}

func reasonsContain(reasons []string, fragment string) bool {
	for _, reason := range reasons {
		if strings.Contains(reason, fragment) {
			return true
		}
	}
	return false
}

func TestSSNMFinishVerdicts(testContext *testing.T) {
	for _, testCase := range []struct {
		name    string
		fixture func() *finishFixture
		verdict string
		reason  string
	}{
		{"clean steady run", steadyFinishFixture, ssnmVerdictPass, ""},
		{"store refusal", func() *finishFixture {
			fixture := steadyFinishFixture()
			fixture.knowledge.RecordsRefused = 1
			return fixture
		}, ssnmVerdictFail, "refused or invalidated"},
		{"store invalidation", func() *finishFixture {
			fixture := steadyFinishFixture()
			fixture.knowledge.PartitionsInvalidated = 1
			return fixture
		}, ssnmVerdictFail, "refused or invalidated"},
		{"no SGP generator evidence", func() *finishFixture {
			fixture := steadyFinishFixture()
			fixture.receiver = nil
			return fixture
		}, ssnmVerdictUnknown, "SGP record carries no SSNM generator evidence"},
		{"failed report", func() *finishFixture {
			fixture := steadyFinishFixture()
			fixture.log.FailedTotal, fixture.log.Failed = 1, []uint64{3}
			return fixture
		}, ssnmVerdictFail, "1 failed reports"},
		{"fan-out failure", func() *finishFixture {
			fixture := steadyFinishFixture()
			fixture.receiver.Generator.FanoutFailures = 1
			return fixture
		}, ssnmVerdictFail, "1 association fan-out failures"},
		{"log not from message zero", func() *finishFixture {
			fixture := steadyFinishFixture()
			fixture.log.From = 1
			fixture.log.Reports, fixture.log.Completions = fixture.log.Reports[1:], fixture.log.Completions[1:]
			return fixture
		}, ssnmVerdictUnknown, "did not report every message"},
	} {
		testContext.Run(testCase.name, func(testContext *testing.T) {
			record := testCase.fixture().run(testContext)
			if record.Verdict != testCase.verdict || testCase.reason != "" && !reasonsContain(record.Reasons, testCase.reason) {
				testContext.Fatalf("verdict %s reasons %q, want %s with %q", record.Verdict, record.Reasons, testCase.verdict, testCase.reason)
			}
			if testCase.verdict == ssnmVerdictPass && len(record.Reasons) != 0 {
				testContext.Fatalf("clean run has reasons %q", record.Reasons)
			}
		})
	}
}

// The 1,024-APC row's p99 apply-time budget is judged on the report-to-receipt
// p99, which bounds apply time from above: a p99 over the budget means the
// budget was not demonstrated and fails the run.
func TestSSNMFinishGatesLargeApplyBudget(testContext *testing.T) {
	within := largeFinishFixture(40 * time.Millisecond).run(testContext)
	if within.Verdict != ssnmVerdictPass {
		testContext.Fatalf("p99 40ms under a 100ms budget: verdict %s reasons %q", within.Verdict, within.Reasons)
	}
	over := largeFinishFixture(150 * time.Millisecond).run(testContext)
	if over.Verdict != ssnmVerdictFail || !reasonsContain(over.Reasons, "apply-time budget") || !reasonsContain(over.Reasons, "upper bound") {
		testContext.Fatalf("p99 150ms over a 100ms budget: verdict %s reasons %q", over.Verdict, over.Reasons)
	}
	check := budgetCheck(over, "apply_p99")
	if check == nil || !check.Gated || check.Outcome != ssnmBudgetExceeded || check.Budget != 100*time.Millisecond || check.Measured < 100*time.Millisecond {
		testContext.Fatalf("apply check = %+v", check)
	}
	loosened := largeFinishFixture(150 * time.Millisecond)
	loosened.config.Budgets.ApplyP99 = 200 * time.Millisecond
	if record := loosened.run(testContext); record.Verdict != ssnmVerdictPass {
		testContext.Fatalf("p99 150ms under a 200ms flag budget: verdict %s reasons %q", record.Verdict, record.Reasons)
	}
}

// The one-APC steady row has no apply-time budget in section 4: its delay is
// recorded, never gated.
func TestSSNMFinishRecordsSteadyDelayWithoutGate(testContext *testing.T) {
	fixture := steadyFinishFixture()
	fixture.delay = 150 * time.Millisecond
	record := fixture.run(testContext)
	if record.Verdict != ssnmVerdictPass {
		testContext.Fatalf("one-APC delay gated: verdict %s reasons %q", record.Verdict, record.Reasons)
	}
	check := budgetCheck(record, "apply_p99")
	if check == nil || check.Gated || check.Outcome != ssnmBudgetRecorded || check.Measured < 100*time.Millisecond {
		testContext.Fatalf("apply check = %+v", check)
	}
}

func TestSSNMFinishGatesResyncBudgets(testContext *testing.T) {
	for _, testCase := range []struct {
		name     string
		resync   time.Duration
		recovery time.Duration
		verdict  string
		reason   string
	}{
		{"within both budgets", 5 * time.Millisecond, 50 * time.Millisecond, ssnmVerdictPass, ""},
		{"resync at the budget", 100 * time.Millisecond, 500 * time.Millisecond, ssnmVerdictPass, ""},
		{"resync over budget", 150 * time.Millisecond, 500 * time.Millisecond, ssnmVerdictFail, "Resync snapshot and subscription acquisition"},
		{"recovery over budget", 5 * time.Millisecond, 1500 * time.Millisecond, ssnmVerdictFail, "retained queued indications and the Resync snapshot"},
	} {
		testContext.Run(testCase.name, func(testContext *testing.T) {
			record := pausedFinishFixture(testCase.resync, testCase.recovery).run(testContext)
			if record.Verdict != testCase.verdict || testCase.reason != "" && !reasonsContain(record.Reasons, testCase.reason) {
				testContext.Fatalf("verdict %s reasons %q, want %s with %q", record.Verdict, record.Reasons, testCase.verdict, testCase.reason)
			}
			for _, name := range []string{"resync", "recovery"} {
				if check := budgetCheck(record, name); check == nil || !check.Gated || check.Outcome == ssnmBudgetNotMeasured {
					testContext.Fatalf("%s check = %+v", name, check)
				}
			}
		})
	}
	loosened := pausedFinishFixture(150*time.Millisecond, 1500*time.Millisecond)
	loosened.config.Budgets = ssnmBudgets{ApplyP99: ssnmDefaultApplyP99Budget, Resync: 200 * time.Millisecond, Recovery: 2 * time.Second}
	if record := loosened.run(testContext); record.Verdict != ssnmVerdictPass {
		testContext.Fatalf("timings under loosened flag budgets: verdict %s reasons %q", record.Verdict, record.Reasons)
	}
}

func TestSSNMFinishAssertsFinalStore(testContext *testing.T) {
	missing := steadyFinishFixture()
	partition := &missing.knowledge.Partitions[0]
	partition.Destinations = partition.Destinations[1:]
	if record := missing.run(testContext); record.Verdict != ssnmVerdictFail || !reasonsContain(record.Reasons, "held 7 records at the end, want 8") {
		testContext.Fatalf("short store: verdict %s reasons %q", record.Verdict, record.Reasons)
	}
	wrong := steadyFinishFixture()
	flipped := &wrong.knowledge.Partitions[0].Destinations[2].Availability
	if flipped.State == m3ua.DestinationAvailable {
		flipped.State = m3ua.DestinationUnavailable
	} else {
		flipped.State = m3ua.DestinationAvailable
	}
	record := wrong.run(testContext)
	if record.Verdict != ssnmVerdictFail || !reasonsContain(record.Reasons, "1 destinations") || record.Store.RecordsAtEnd != 8 || record.Store.StateMismatches != 1 || !record.Store.StateValidated {
		testContext.Fatalf("stale store state: verdict %s reasons %q store %+v", record.Verdict, record.Reasons, record.Store)
	}
	if clean := steadyFinishFixture().run(testContext); !clean.Store.StateValidated || clean.Store.StateMismatches != 0 {
		testContext.Fatalf("clean store = %+v", clean.Store)
	}
}

func TestSSNMFinishFailsOnOtherEvents(testContext *testing.T) {
	for name, fixture := range map[string]*finishFixture{
		"steady": steadyFinishFixture(),
		"paused": pausedFinishFixture(time.Millisecond, 10*time.Millisecond),
	} {
		testContext.Run(name, func(testContext *testing.T) {
			fixture.events = []m3ua.SSNMEvent{{Kind: m3ua.SSNMBindingAdmittedEvent}}
			record := fixture.run(testContext)
			if record.Verdict != ssnmVerdictFail || !reasonsContain(record.Reasons, "subscriber 0 saw 1 other events") || !reasonsContain(record.Reasons, "subscriber 1 saw 1 other events") {
				testContext.Fatalf("other events: verdict %s reasons %q", record.Verdict, record.Reasons)
			}
		})
	}
}

func budgetCheck(record *ssnmRecord, name string) *ssnmBudgetCheck {
	for index := range record.Budgets {
		if record.Budgets[index].Name == name {
			return &record.Budgets[index]
		}
	}
	return nil
}
