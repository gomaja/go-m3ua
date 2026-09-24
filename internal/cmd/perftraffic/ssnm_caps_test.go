package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua"
)

// Every size below is worked by hand from the SubscriptionQueueBytes doc
// comment: 512 per event, 8 per report destination, 256 per updated
// destination, 4 per Routing Context in the report and in both dimensions of
// each update, plus the byte lengths of Reason and of the event's and the
// report's partition identity strings.
func TestSSNMEventBytesFollowsTheDocumentedFormula(testContext *testing.T) {
	scope := func(contexts ...uint32) m3ua.WireScope {
		return m3ua.WireScope{RoutingContexts: contexts, RoutingContextSet: len(contexts) != 0}
	}
	update := func(availability, congestion m3ua.WireScope) m3ua.SSNMDestinationKnowledge {
		return m3ua.SSNMDestinationKnowledge{
			Availability: m3ua.SSNMAvailability{Scope: availability}, AvailabilitySet: true,
			Congestion: m3ua.SSNMCongestion{Scope: congestion}, CongestionSet: len(congestion.RoutingContexts) != 0,
		}
	}
	canonical := m3ua.SSNMPartition{Kind: m3ua.SSNMCanonicalPartition, SignallingGateway: "sg-a", ApplicationServer: "as-core"}
	for _, testCase := range []struct {
		name  string
		event m3ua.SSNMEvent
		want  int
	}{
		// 512 + 8.
		{"report naming one destination and updating none", m3ua.SSNMEvent{Kind: m3ua.SSNMReportEvent,
			Report: m3ua.SSNMReport{Destinations: make([]m3ua.PointCodeRange, 1)}}, 520},
		// 512 + 8 + 256 + 4 (report) + 4 (availability): the one-APC F3 event.
		{"one-APC workload report", m3ua.SSNMEvent{Kind: m3ua.SSNMReportEvent,
			Report:  m3ua.SSNMReport{Scope: scope(100), Destinations: make([]m3ua.PointCodeRange, 1)},
			Updated: []m3ua.SSNMDestinationKnowledge{update(scope(100), scope())}}, 784},
		// 512 + 3*8 + 2*256 + 4*(2 report + 2+1 first update + 0+3 second).
		{"routing contexts in the report and both dimensions", m3ua.SSNMEvent{Kind: m3ua.SSNMReportEvent,
			Report: m3ua.SSNMReport{Scope: scope(100, 101), Destinations: make([]m3ua.PointCodeRange, 3)},
			Updated: []m3ua.SSNMDestinationKnowledge{
				update(scope(100, 101), scope(7)),
				update(scope(), scope(1, 2, 3)),
			}}, 512 + 24 + 512 + 32},
		// 784 + 2*(len("sg-a") + len("as-core")) = 784 + 22.
		{"canonical partition on the event and its report", m3ua.SSNMEvent{Kind: m3ua.SSNMReportEvent, Partition: canonical,
			Report:  m3ua.SSNMReport{Scope: scope(100), Partition: canonical, Destinations: make([]m3ua.PointCodeRange, 1)},
			Updated: []m3ua.SSNMDestinationKnowledge{update(scope(100), scope())}}, 806},
		// 512 + len("sg-b") + len("sg-longer-name") + len("x") = 512 + 4 + 14 + 1.
		{"event and report partitions counted separately", m3ua.SSNMEvent{Kind: m3ua.SSNMReportEvent,
			Partition: m3ua.SSNMPartition{SignallingGateway: "sg-b"},
			Report:    m3ua.SSNMReport{Partition: m3ua.SSNMPartition{SignallingGateway: "sg-longer-name", ApplicationServer: "x"}}}, 531},
		// 512 + len("record budget exhausted") = 512 + 23.
		{"resource loss reason", m3ua.SSNMEvent{Kind: m3ua.SSNMResourceLossEvent, Reason: "record budget exhausted"}, 535},
		// 512 + len("stale") + len("sg-a") + len("as-core") = 512 + 5 + 4 + 7.
		{"invalidation reason and partition", m3ua.SSNMEvent{Kind: m3ua.SSNMPartitionInvalidatedEvent, Partition: canonical, Reason: "stale"}, 528},
		// Byte lengths, not runes: "état" is five bytes.
		{"reason counted in bytes", m3ua.SSNMEvent{Kind: m3ua.SSNMResourceLossEvent, Reason: "état"}, 517},
		// 512 + 1024*(8 + 256 + 4) + 4.
		{"1,024-APC workload report", deliveredEvent(singlePlan(1024, 1024), 1), 274_948},
	} {
		testContext.Run(testCase.name, func(testContext *testing.T) {
			if got := ssnmEventBytes(testCase.event); got != testCase.want {
				testContext.Fatalf("accounted bytes = %d, want %d", got, testCase.want)
			}
		})
	}
}

// The smallest queued event of a workload is one generated message: 516 plus
// 268 per Affected Point Code (8 + 256 + 4).
func TestSSNMWorkloadEventBytes(testContext *testing.T) {
	for destinations, want := range map[int]int{1: 784, 32: 9_092, 256: 69_124, 1024: 274_948} {
		if got := ssnmWorkloadEventBytes(destinations); got != want {
			testContext.Errorf("%d-destination message = %d accounted bytes, want %d", destinations, got, want)
		}
		plan := singlePlan(1024, destinations)
		if got := ssnmEventBytes(deliveredEvent(plan, plan.preloadMessages())); got != want {
			testContext.Errorf("delivered %d-destination report = %d accounted bytes, want %d", destinations, got, want)
		}
	}
}

// capEvidence is the F3 record of a paused one-APC subscriber that retained
// queued events of 784 bytes each before its continuity loss, classified as
// the subscriber classifies it.
func capEvidence(queueLimit, queueBytes, queued int) *ssnmPauseRecord {
	pause := &ssnmPauseRecord{
		ContinuityLossObserved: true, QueueLimit: queueLimit, QueueByteLimit: queueBytes,
		QueuedAtLoss: queued, QueuedBytesAtLoss: queued * 784, SmallestEventBytes: 784, SmallestQueuedEventBytes: 784,
		SnapshotValidated: 1,
	}
	pause.CountCapEnforced, pause.ByteCapEnforced = ssnmCapsReached(pause)
	pause.BindingCap = ssnmBindingCap(pause.CountCapEnforced, pause.ByteCapEnforced)
	return pause
}

func TestSSNMCapFailureJudgesTheBindingCap(testContext *testing.T) {
	record := ssnmSubscriberRecord{Partitions: 1, FinalPositions: []uint64{10}, ExpectedFinalPositions: []uint64{10}, ssnmSubscriberCounts: ssnmSubscriberCounts{ContinuityLost: 1}}
	for _, testCase := range []struct {
		name    string
		pause   func() *ssnmPauseRecord
		binding string
		reason  string
	}{
		// 256 x 784 = 200,704 bytes under 1 MiB: the count cap binds.
		{"count cap binds under the default byte limit", func() *ssnmPauseRecord { return capEvidence(256, 1<<20, 256) }, ssnmBindingCount, ""},
		// 125 x 784 = 98,000; another 784 would make 98,784 > 98,304.
		{"byte cap binds", func() *ssnmPauseRecord { return capEvidence(256, 98_304, 125) }, ssnmBindingBytes, ""},
		{"byte cap filled exactly", func() *ssnmPauseRecord { return capEvidence(256, 98_000, 125) }, ssnmBindingBytes, ""},
		{"both caps reached count first", func() *ssnmPauseRecord { return capEvidence(256, 256*784, 256) }, ssnmBindingCount, ""},
		{"retained bytes over the byte cap", func() *ssnmPauseRecord { return capEvidence(256, 98_304, 126) }, "", "retained 98784 accounted bytes, over the 98304-byte cap"},
		{"retained events over the count cap", func() *ssnmPauseRecord {
			pause := capEvidence(256, 1<<20, 257)
			pause.ContinuityLossObserved = false
			return pause
		}, "", "retained 257 events, over the 256-event cap"},
		{"loss with neither cap reached", func() *ssnmPauseRecord { return capEvidence(256, 1<<20, 100) }, "", "below both caps"},
		// 98,000 + 784 = 98,784 fits a 98,784-byte limit, so one more event
		// could have been queued.
		{"loss one event short of the byte cap", func() *ssnmPauseRecord { return capEvidence(256, 98_784, 125) }, "", "below both caps"},
		{"byte binding recorded as count", func() *ssnmPauseRecord {
			pause := capEvidence(256, 98_304, 125)
			pause.BindingCap = ssnmBindingCount
			return pause
		}, "", `recorded binding cap "count"`},
		{"count binding recorded as bytes", func() *ssnmPauseRecord {
			pause := capEvidence(256, 1<<20, 256)
			pause.BindingCap = ssnmBindingBytes
			return pause
		}, "", `recorded binding cap "bytes"`},
		{"byte binding not recorded", func() *ssnmPauseRecord {
			pause := capEvidence(256, 98_304, 125)
			pause.BindingCap = ""
			return pause
		}, "", "recorded binding cap"},
		{"byte cap reached but not flagged", func() *ssnmPauseRecord {
			pause := capEvidence(256, 98_304, 125)
			pause.ByteCapEnforced = false
			return pause
		}, "", "recorded binding cap"},
		{"retained event below the workload's smallest", func() *ssnmPauseRecord {
			pause := capEvidence(256, 98_304, 125)
			pause.SmallestQueuedEventBytes = 520
			return pause
		}, "", "below the workload's smallest 784"},
		{"no continuity loss", func() *ssnmPauseRecord {
			pause := capEvidence(256, 1<<20, 256)
			pause.ContinuityLossObserved = false
			return pause
		}, "", "observed no continuity loss"},
	} {
		testContext.Run(testCase.name, func(testContext *testing.T) {
			pause := testCase.pause()
			failure := ssnmCapFailure(pause)
			failures := pausedSubscriberFailures(record, pause, 1)
			if testCase.reason == "" {
				if failure != "" || len(failures) != 0 || pause.BindingCap != testCase.binding {
					testContext.Fatalf("clean overflow at the %s cap: binding %q failure %q failures %q", testCase.binding, pause.BindingCap, failure, failures)
				}
				return
			}
			if !strings.Contains(failure, testCase.reason) || !reasonsContain(failures, testCase.reason) {
				testContext.Fatalf("failure %q failures %q, want %q", failure, failures, testCase.reason)
			}
		})
	}
}

// scriptedStream is a paused subscription whose retained queue is fixed:
// Next delivers the events in order and then times out like an empty queue.
type scriptedStream struct {
	events  []m3ua.SSNMEvent
	next    int
	resyncs int
}

func (stream *scriptedStream) Next(context.Context) (m3ua.SSNMEvent, error) {
	if stream.next >= len(stream.events) {
		return m3ua.SSNMEvent{}, context.DeadlineExceeded
	}
	event := stream.events[stream.next]
	stream.next++
	return event, nil
}

func (stream *scriptedStream) Resync() (m3ua.SSNMSnapshot, error) {
	stream.resyncs++
	return m3ua.SSNMSnapshot{}, nil
}

func (*scriptedStream) Close() error { return nil }

// deliveredEvent is the report the library delivers for one plan position of
// association 0's partition.
func deliveredEvent(plan ssnmPlan, position uint64) m3ua.SSNMEvent {
	return deliveredAssociationEvent(plan, 0, position)
}

// deliveredAssociationEvent is the report the library delivers for one
// position of association's partition: associationEvent's report in the
// generator's scope, with every destination it names updated in the
// availability dimension under that scope.
func deliveredAssociationEvent(plan ssnmPlan, association int, position uint64) m3ua.SSNMEvent {
	event := associationEvent(plan, testPartitionOf(association), association, position)
	event.Report.Scope = ssnmScope()
	for _, destination := range event.Report.Destinations {
		event.Updated = append(event.Updated, m3ua.SSNMDestinationKnowledge{
			Destination: destination, AvailabilitySet: true, Availability: m3ua.SSNMAvailability{Scope: ssnmScope()},
		})
	}
	return event
}

// retainedQueue is a paused queue of steady one-APC reports in schedule
// order, generator message m in association m mod N's partition, optionally
// followed by the continuity-loss marker.
func retainedQueue(plan ssnmPlan, events int, loss bool) []m3ua.SSNMEvent {
	var queue []m3ua.SSNMEvent
	for message := uint64(0); message < uint64(events); message++ {
		association, position := plan.target(message)
		queue = append(queue, deliveredAssociationEvent(plan, association, position))
	}
	if loss {
		queue = append(queue, m3ua.SSNMEvent{Kind: m3ua.SSNMContinuityLostEvent, ContinuityLost: true})
	}
	return queue
}

// The paused subscriber's drain recomputes every retained event's accounted
// bytes and classifies the loss. The clock steps a whole pause per read, so
// no real time is asserted.
func TestSSNMPauseDrainAccountsRetainedBytes(testContext *testing.T) {
	plan := singlePlan(256, 1)
	// Under the total rate the retained queue interleaves eight partitions'
	// reports; the caps count every event of the subscription alike, so the
	// judgment is the same: 256 events for the count cap, and 125 events of
	// 784 bytes under a 96 KiB byte limit.
	eight := ssnmPlan{records: 256, apcs: 1, associations: 8}
	for _, testCase := range []struct {
		name       string
		queueLimit int
		queueBytes int
		queue      []m3ua.SSNMEvent
		want       ssnmPauseRecord
		reason     string
		// eight marks a queue of the eight-association plan.
		eight bool
	}{
		{"byte cap over eight partitions", 256, 98_304, retainedQueue(eight, 125, true), ssnmPauseRecord{
			ContinuityLossObserved: true, QueuedAtLoss: 125, QueuedBytesAtLoss: 98_000, SmallestQueuedEventBytes: 784,
			ByteCapEnforced: true, BindingCap: ssnmBindingBytes, QueuedStateEntries: 125,
		}, "", true},
		{"count cap over eight partitions", 256, 1 << 20, retainedQueue(eight, 256, true), ssnmPauseRecord{
			ContinuityLossObserved: true, QueuedAtLoss: 256, QueuedBytesAtLoss: 256 * 784, SmallestQueuedEventBytes: 784,
			CountCapEnforced: true, BindingCap: ssnmBindingCount, QueuedStateEntries: 256,
		}, "", true},
		{"eight partitions over the count cap", 256, 1 << 20, retainedQueue(eight, 300, false), ssnmPauseRecord{
			QueuedAtLoss: 257, QueuedBytesAtLoss: 257 * 784, SmallestQueuedEventBytes: 784, QueuedStateEntries: 257,
		}, "retained 257 events, over the 256-event cap", true},
		{"eight partitions losing continuity early", 256, 1 << 20, retainedQueue(eight, 40, true), ssnmPauseRecord{
			ContinuityLossObserved: true, QueuedAtLoss: 40, QueuedBytesAtLoss: 40 * 784, SmallestQueuedEventBytes: 784, QueuedStateEntries: 40,
		}, "below both caps", true},
		{"byte cap", 256, 98_304, retainedQueue(plan, 125, true), ssnmPauseRecord{
			ContinuityLossObserved: true, QueuedAtLoss: 125, QueuedBytesAtLoss: 98_000, SmallestQueuedEventBytes: 784,
			ByteCapEnforced: true, BindingCap: ssnmBindingBytes, QueuedStateEntries: 125,
		}, "", false},
		{"count cap", 4, 1 << 20, retainedQueue(plan, 4, true), ssnmPauseRecord{
			ContinuityLossObserved: true, QueuedAtLoss: 4, QueuedBytesAtLoss: 4 * 784, SmallestQueuedEventBytes: 784,
			CountCapEnforced: true, BindingCap: ssnmBindingCount, QueuedStateEntries: 4,
		}, "", false},
		{"queue over the count cap", 4, 1 << 20, retainedQueue(plan, 6, false), ssnmPauseRecord{
			QueuedAtLoss: 5, QueuedBytesAtLoss: 5 * 784, SmallestQueuedEventBytes: 784, QueuedStateEntries: 5,
		}, "retained 5 events, over the 4-event cap", false},
		{"early loss", 256, 1 << 20, retainedQueue(plan, 3, true), ssnmPauseRecord{
			ContinuityLossObserved: true, QueuedAtLoss: 3, QueuedBytesAtLoss: 3 * 784, SmallestQueuedEventBytes: 784, QueuedStateEntries: 3,
		}, "below both caps", false},
	} {
		testContext.Run(testCase.name, func(testContext *testing.T) {
			stream := &scriptedStream{events: testCase.queue}
			subscriberPlan := plan
			if testCase.eight {
				subscriberPlan = eight
			}
			subscriber := newSSNMSubscriber(0, true, subscriberPlan, 1000, testCase.queueLimit, testCase.queueBytes)
			subscriber.subscription = stream
			pause := ssnmPause{Duration: 10 * time.Millisecond}
			subscriber.pauseAndRecover(context.Background(), &steppingMeasurementClock{step: int64(pause.Duration)}, 0, pause)
			got := subscriber.pause
			if got == nil || got.Error != "" {
				testContext.Fatalf("pause record %+v", got)
			}
			want := testCase.want
			if got.QueueLimit != testCase.queueLimit || got.QueueByteLimit != testCase.queueBytes || got.SmallestEventBytes != 784 ||
				got.ContinuityLossObserved != want.ContinuityLossObserved || got.QueuedAtLoss != want.QueuedAtLoss ||
				got.QueuedBytesAtLoss != want.QueuedBytesAtLoss || got.SmallestQueuedEventBytes != want.SmallestQueuedEventBytes ||
				got.CountCapEnforced != want.CountCapEnforced || got.ByteCapEnforced != want.ByteCapEnforced ||
				got.BindingCap != want.BindingCap || got.QueuedStateEntries != want.QueuedStateEntries {
				testContext.Fatalf("pause record %+v, want %+v", got, want)
			}
			if resynced := stream.resyncs == 1; resynced != want.ContinuityLossObserved {
				testContext.Fatalf("Resync called %d times after loss %t", stream.resyncs, want.ContinuityLossObserved)
			}
			failure := ssnmCapFailure(got)
			if testCase.reason == "" && failure != "" || testCase.reason != "" && !strings.Contains(failure, testCase.reason) {
				testContext.Fatalf("cap failure %q, want %q", failure, testCase.reason)
			}
			// The queue starts after the preload, which counts as a gap; its
			// reports must still belong to their partitions.
			if counts := subscriber.counts; counts.Unexpected != 0 || counts.MisScoped != 0 {
				testContext.Fatalf("the retained queue did not follow the plan: %+v", counts)
			}
		})
	}
}
