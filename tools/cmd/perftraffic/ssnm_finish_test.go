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
	config       ssnmConfig
	associations int
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
	return newFinishFixture(ssnmConfig{TotalRate: 1000, APCs: 1, Records: 8, Subscribers: 2}, 1, 2*time.Millisecond)
}

func largeFinishFixture(delay time.Duration) *finishFixture {
	return newFinishFixture(ssnmConfig{TotalRate: 1000, APCs: ssnmMaxAPCs, Records: ssnmMaxAPCs, Subscribers: 2}, 1, delay)
}

func pausedFinishFixture(resync, recovery time.Duration) *finishFixture {
	fixture := newFinishFixture(ssnmConfig{TotalRate: 1000, APCs: 1, Records: 8, Subscribers: 2, Pause: ssnmPause{Offset: time.Millisecond, Duration: time.Millisecond}}, 1, 2*time.Millisecond)
	fixture.pause = cleanPause()
	fixture.pause.ResyncNS, fixture.pause.RecoveryNS = int64(resync), int64(recovery)
	return fixture
}

// The section 4 shapes over eight associations: the steady row's one-APC
// messages and the large row's 1,024-APC messages, each partition holding
// its 2,048 records.
func steadyEightFinishFixture() *finishFixture {
	return newFinishFixture(ssnmConfig{TotalRate: 1000, APCs: 1, Records: 2048, Subscribers: 3}, 8, 2*time.Millisecond)
}

func largeEightFinishFixture(delay time.Duration) *finishFixture {
	return newFinishFixture(ssnmConfig{TotalRate: 10, APCs: ssnmMaxAPCs, Records: 2048, Subscribers: 3}, 8, delay)
}

func pausedEightFinishFixture() *finishFixture {
	fixture := newFinishFixture(ssnmConfig{TotalRate: 1000, APCs: 1, Records: 256, Subscribers: 3, Pause: ssnmPause{Offset: time.Millisecond, Duration: time.Millisecond}}, 8, 2*time.Millisecond)
	fixture.pause = cleanPause()
	fixture.pause.SnapshotValidated = 8
	fixture.pause.ResyncNS, fixture.pause.RecoveryNS = int64(5*time.Millisecond), int64(50*time.Millisecond)
	return fixture
}

func newFinishFixture(config ssnmConfig, associations int, delay time.Duration) *finishFixture {
	config.Budgets = ssnmBudgets{ApplyP99: ssnmDefaultApplyP99Budget, Resync: ssnmDefaultResyncBudget, Recovery: ssnmDefaultRecoveryBudget}
	plan := newSSNMPlan(config, associations)
	log := ssnmReportsResponse{State: ssnmGeneratorComplete, SentTotal: finishSent}
	for message := uint64(0); message < finishSent; message++ {
		reported := finishAnchor + ssnmScheduled(config.TotalRate, message)
		log.Reports = append(log.Reports, reported)
		log.Completions = append(log.Completions, reported+int64(10*time.Microsecond))
	}
	return &finishFixture{
		config: config, associations: associations, delay: delay, log: log,
		receiver:  &ssnmRecord{Generator: &ssnmGeneratorRecord{State: ssnmGeneratorComplete, Associations: associations}},
		knowledge: storeAfter(plan, plan.expectedPositions(finishSent)),
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

	plan := newSSNMPlan(fixture.config, fixture.associations)
	windowEnd := finishAnchor + ssnmScheduled(fixture.config.TotalRate, finishMessages)
	run := &ssnmSenderRun{
		config: fixture.config, plan: plan, associations: fixture.associations, peerControl: server.URL,
		limits: ssnmStoreLimits(fixture.config, fixture.associations), cancel: func() {},
		knowledge: func() m3ua.SSNMSnapshot { return fixture.knowledge },
	}
	for index := 0; index < fixture.config.Subscribers; index++ {
		paused := index == 0 && fixture.config.Pause.enabled()
		run.subscribers = append(run.subscribers, newSSNMSubscriber(index, paused, plan, fixture.config.TotalRate, 256, 1<<20))
	}
	specification := runSpec{Clock: &sharedClockWindow{Start: finishAnchor, End: windowEnd}}
	if err := run.attach(&specification, ssnmPhaseMeasurement); err != nil {
		testContext.Fatal(err)
	}
	for _, subscriber := range run.subscribers {
		if subscriber.paused {
			subscriber.observe(m3ua.SSNMEvent{Kind: m3ua.SSNMContinuityLostEvent, ContinuityLost: true}, 0)
			if fixture.pause != nil {
				pause := *fixture.pause
				subscriber.pause = &pause
			}
		}
		for _, delivery := range roundRobinDeliveries(plan, fixture.config.TotalRate, finishAnchor, finishSent, fixture.delay) {
			subscriber.observe(delivery.event, delivery.received)
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
		}, ssnmVerdictFail, "saw 1 failed reports"},
		{"generator over another association count", func() *finishFixture {
			fixture := steadyFinishFixture()
			fixture.receiver.Generator.Associations = 8
			return fixture
		}, ssnmVerdictFail, "over 8 associations, this run has 1"},
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

// The steady and large rows over eight associations: every message reached
// one partition, each partition ended at its own share, the store holds
// 8 x 2,048 records and the delay join covers every measurement message
// once. Each mutation of the evidence fails the run.
func TestSSNMFinishEightAssociationRows(testContext *testing.T) {
	for _, testCase := range []struct {
		name    string
		fixture func() *finishFixture
		verdict string
		reason  string
	}{
		{"steady row", steadyEightFinishFixture, ssnmVerdictPass, ""},
		{"large row within the apply budget", func() *finishFixture { return largeEightFinishFixture(40 * time.Millisecond) }, ssnmVerdictPass, ""},
		{"large row over the apply budget", func() *finishFixture { return largeEightFinishFixture(150 * time.Millisecond) }, ssnmVerdictFail, "apply-time budget"},
		{"F3 pause over eight partitions", pausedEightFinishFixture, ssnmVerdictPass, ""},
		{"F3 pause validating seven partitions", func() *finishFixture {
			fixture := pausedEightFinishFixture()
			fixture.pause.SnapshotValidated = 7
			return fixture
		}, ssnmVerdictFail, "validated 7 of 8 resynchronized partitions"},
		{"one partition's records missing", func() *finishFixture {
			fixture := steadyEightFinishFixture()
			fixture.knowledge.Partitions = fixture.knowledge.Partitions[:7]
			return fixture
		}, ssnmVerdictFail, "held 14336 records at the end, want 16384"},
		{"one partition holding another association's state", func() *finishFixture {
			fixture := steadyEightFinishFixture()
			plan := newSSNMPlan(fixture.config, 8)
			// Partition 6 holds association 5's destinations instead of its
			// own: the store still counts 16,384 records.
			other := storeAfter(plan, plan.expectedPositions(finishSent)).Partitions[5]
			other.Partition = testPartitionOf(6)
			fixture.knowledge.Partitions[6] = other
			return fixture
		}, ssnmVerdictFail, "disagreed with the plan at 4096 destinations"},
		{"store expected at the broadcast positions", func() *finishFixture {
			fixture := steadyEightFinishFixture()
			plan := newSSNMPlan(fixture.config, 8)
			broadcast := make([]uint64, 8)
			for association := range broadcast {
				broadcast[association] = plan.preloadMessages() + finishSent
			}
			fixture.knowledge = storeAfter(plan, broadcast)
			return fixture
		}, ssnmVerdictFail, "disagreed with the plan"},
	} {
		testContext.Run(testCase.name, func(testContext *testing.T) {
			fixture := testCase.fixture()
			record := fixture.run(testContext)
			if record.Verdict != testCase.verdict || testCase.reason != "" && !reasonsContain(record.Reasons, testCase.reason) {
				testContext.Fatalf("verdict %s reasons %q, want %s with %q", record.Verdict, record.Reasons, testCase.verdict, testCase.reason)
			}
			if testCase.verdict != ssnmVerdictPass {
				return
			}
			plan := newSSNMPlan(fixture.config, 8)
			if record.Store.RecordsAtEnd != 8*fixture.config.Records || !record.Store.StateValidated || record.Store.StateMismatches != 0 {
				testContext.Fatalf("store = %+v", record.Store)
			}
			generator := record.Generator
			if generator.Offered != finishMessages || generator.Associations != 8 || len(generator.OfferedPerAssociation) != 8 {
				testContext.Fatalf("generator = %+v", generator)
			}
			for association, share := range generator.OfferedPerAssociation {
				if want := plan.messagesFor(association, finishMessages); share != want {
					testContext.Fatalf("association %d offered %d, want %d", association, share, want)
				}
			}
			healthy := 0
			for _, subscriber := range record.Subscribers {
				for association, position := range subscriber.FinalPositions {
					if want := plan.preloadMessages() + plan.messagesFor(association, finishSent); position != want || subscriber.ExpectedFinalPositions[association] != want {
						testContext.Fatalf("subscriber %d association %d ended at %d expecting %d, want %d", subscriber.Index, association, position, subscriber.ExpectedFinalPositions[association], want)
					}
				}
				if subscriber.Role == "healthy" {
					healthy++
				}
			}
			if record.Delay.Delay.Count != uint64(healthy)*finishMessages || record.Delay.Messages != finishMessages {
				testContext.Fatalf("delay join %+v, want one receipt per message per healthy subscriber", record.Delay)
			}
		})
	}
}
