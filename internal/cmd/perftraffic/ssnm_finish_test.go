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

func newFinishFixture(config ssnmConfig, delay time.Duration) *finishFixture {
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
		run.subscribers = append(run.subscribers, newSSNMSubscriber(index, paused, plan, fixture.config.Rate, 1, 256))
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
