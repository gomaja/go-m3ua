package main

import (
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua"
)

var testPartition = testPartitionOf(0)

// testPartitionOf is the ASP partition of one association: a standalone
// partition per association, whose identifiers need not follow the SGP's
// association order.
func testPartitionOf(association int) m3ua.SSNMPartition {
	return m3ua.SSNMPartition{Kind: m3ua.SSNMStandalonePartition, Association: m3ua.AssociationID(101 + 7*association)}
}

// singlePlan is a one-association plan.
func singlePlan(records, apcs int) ssnmPlan {
	return ssnmPlan{records: records, apcs: apcs, associations: 1}
}

// planEvent builds the report a healthy SGP delivers for one position of
// association 0's partition.
func planEvent(plan ssnmPlan, partition m3ua.SSNMPartition, position uint64) m3ua.SSNMEvent {
	return associationEvent(plan, partition, 0, position)
}

// associationEvent builds the report a healthy SGP delivers for one position
// of association's partition, delivered in partition.
func associationEvent(plan ssnmPlan, partition m3ua.SSNMPartition, association int, position uint64) m3ua.SSNMEvent {
	chunk := plan.chunk(position)
	kind := m3ua.SSNMDestinationAvailableReport
	if chunk.availability == m3ua.DestinationUnavailable {
		kind = m3ua.SSNMDestinationUnavailableReport
	}
	destinations := make([]m3ua.PointCodeRange, chunk.count)
	for index := range destinations {
		destinations[index] = m3ua.PointCodeRange{PointCode: plan.pointCode(association, chunk.first+index)}
	}
	return m3ua.SSNMEvent{
		Kind:      m3ua.SSNMReportEvent,
		Partition: partition,
		ReportSet: true,
		Report:    m3ua.SSNMReport{Kind: kind, Partition: partition, Destinations: destinations},
	}
}

func TestSSNMSubscriberAcceptsInOrderStream(testContext *testing.T) {
	plan := singlePlan(8, 2)
	subscriber := newSSNMSubscriber(0, false, plan, 1000, 256, 1<<20)
	for position := uint64(0); position < 40; position++ {
		subscriber.observe(planEvent(plan, testPartition, position), int64(position))
	}
	record := subscriber.record([]uint64{40})
	if record.Accepted != 40 || record.Gaps != 0 || record.Duplicates != 0 || record.Unexpected != 0 {
		testContext.Fatalf("in-order stream accounting = %+v", record)
	}
	if failures := healthySubscriberFailures(record, 1); len(failures) != 0 {
		testContext.Fatalf("healthy subscriber failures = %v", failures)
	}
}

func TestSSNMSubscriberDetectsDuplicateGapAndUnexpected(testContext *testing.T) {
	plan := singlePlan(8, 2)
	subscriber := newSSNMSubscriber(0, false, plan, 1000, 256, 1<<20)
	for _, position := range []uint64{0, 1, 2, 2, 5, 6} {
		subscriber.observe(planEvent(plan, testPartition, position), 0)
	}
	corrupted := planEvent(plan, testPartition, 7)
	corrupted.Report.Destinations[0].Mask = 3
	subscriber.observe(corrupted, 0)
	wrongKind := planEvent(plan, testPartition, 7)
	wrongKind.Report.Kind = m3ua.SSNMSignallingCongestionReport
	subscriber.observe(wrongKind, 0)
	outside := planEvent(plan, testPartition, 7)
	outside.Report.Destinations[0].PointCode = 0x220000
	subscriber.observe(outside, 0)
	record := subscriber.record([]uint64{7})
	if record.Duplicates != 1 || record.Gaps != 2 || record.Unexpected != 3 || record.Accepted != 5 {
		testContext.Fatalf("anomaly accounting = %+v", record)
	}
	if failures := healthySubscriberFailures(record, 1); len(failures) == 0 {
		testContext.Fatal("a lossy healthy subscriber reported no failure")
	}
}

func TestSSNMSubscriberCountsLifecycleEventsAndPartitionCap(testContext *testing.T) {
	plan := singlePlan(8, 2)
	subscriber := newSSNMSubscriber(0, false, plan, 1000, 256, 1<<20)
	subscriber.observe(m3ua.SSNMEvent{Kind: m3ua.SSNMContinuityLostEvent, ContinuityLost: true}, 0)
	subscriber.observe(m3ua.SSNMEvent{Kind: m3ua.SSNMResourceLossEvent}, 0)
	subscriber.observe(m3ua.SSNMEvent{Kind: m3ua.SSNMPartitionInvalidatedEvent}, 0)
	subscriber.observe(m3ua.SSNMEvent{Kind: m3ua.SSNMBindingAdmittedEvent}, 0)
	subscriber.observe(planEvent(plan, testPartition, 0), 0)
	// A second partition carrying association 0's destinations is
	// mis-scoped: each association's messages reach its own partition only.
	other := testPartitionOf(1)
	subscriber.observe(planEvent(plan, other, 0), 0)
	record := subscriber.record([]uint64{1})
	if record.ContinuityLost != 1 || record.ResourceLoss != 1 || record.Invalidated != 1 || record.OtherEvents != 1 || record.MisScoped != 1 || record.Partitions != 1 {
		testContext.Fatalf("lifecycle accounting = %+v", record)
	}
	if failures := healthySubscriberFailures(record, 2); len(failures) < 4 {
		testContext.Fatalf("expected continuity, mis-scoped, lifecycle and partition failures, got %v", failures)
	}
}

func TestSSNMSubscriberJoinsReceiptsWithReports(testContext *testing.T) {
	plan := singlePlan(8, 2)
	preload := plan.preloadMessages()
	anchor := int64(1_000_000_000)
	subscriber := newSSNMSubscriber(0, false, plan, 1000, 256, 1<<20)
	subscriber.armReceipts(anchor, 2, 6, true)
	for position := uint64(0); position < preload+5; position++ {
		message := position - preload
		received := anchor + int64(message)*int64(time.Millisecond) + int64(3*time.Millisecond)
		subscriber.observe(planEvent(plan, testPartition, position), received)
	}
	log := ssnmReportsResponse{From: 0, Reports: make([]int64, 8), Completions: make([]int64, 8)}
	for message := range log.Reports {
		log.Reports[message] = anchor + int64(message)*int64(time.Millisecond)
	}
	histogram := newDurationHistogram()
	missing := subscriber.joinDelays(histogram, log, map[uint64]bool{})
	percentiles := histogram.percentiles()
	if missing != 1 || percentiles.Count != 3 || percentiles.Max != 3*time.Millisecond {
		testContext.Fatalf("join missing=%d percentiles=%+v, want one missing message and three 3ms delays", missing, percentiles)
	}
	failed := subscriber.joinDelays(newDurationHistogram(), log, map[uint64]bool{5: true})
	if failed != 0 {
		testContext.Fatalf("a failed report was counted missing: %d", failed)
	}
}

func snapshotAfter(plan ssnmPlan, partition m3ua.SSNMPartition, positions uint64) m3ua.SSNMSnapshot {
	knowledge := m3ua.SSNMPartitionKnowledge{Partition: partition}
	for destination := 0; destination < plan.records; destination++ {
		state, held := plan.expectedState(destination, positions)
		if !held {
			continue
		}
		knowledge.Destinations = append(knowledge.Destinations, m3ua.SSNMDestinationKnowledge{
			Destination:     m3ua.PointCodeRange{PointCode: ssnmPointCodeBase + uint32(destination)},
			Availability:    m3ua.SSNMAvailability{State: state},
			AvailabilitySet: true,
		})
	}
	return m3ua.SSNMSnapshot{Partitions: []m3ua.SSNMPartitionKnowledge{knowledge}}
}

func TestSSNMSubscriberLocksOnAfterResync(testContext *testing.T) {
	plan := singlePlan(16, 1)
	preload := plan.preloadMessages()
	anchor := int64(1_000_000_000)
	subscriber := newSSNMSubscriber(0, true, plan, 1000, 4, 1<<20)
	subscriber.pause = &ssnmPauseRecord{}
	subscriber.armReceipts(anchor, 0, 0, false)
	for position := uint64(0); position < preload+10; position++ {
		subscriber.observe(planEvent(plan, testPartition, position), 0)
	}
	// Positions preload+10 .. preload+24 were lost; the snapshot holds them.
	resumed := preload + 25
	partitions, destinations := subscriber.captureSnapshot(snapshotAfter(plan, testPartition, resumed))
	if partitions != 1 || destinations != 16 {
		testContext.Fatalf("snapshot consumed %d partitions and %d destinations", partitions, destinations)
	}
	received := anchor + ssnmScheduled(1000, resumed-preload) + int64(2*time.Millisecond)
	subscriber.observe(planEvent(plan, testPartition, resumed), received)
	subscriber.observe(planEvent(plan, testPartition, resumed+1), received)
	record := subscriber.record([]uint64{resumed + 2})
	if record.SnapshotMismatches != 0 || record.Gaps != 0 || record.Unexpected != 0 || subscriber.pause.SnapshotValidated != 1 {
		testContext.Fatalf("resync accounting = %+v pause = %+v", record, subscriber.pause)
	}
	if len(subscriber.pause.LockedOnPositions) != 1 || subscriber.pause.LockedOnPositions[0] != resumed {
		testContext.Fatalf("locked on %v, want %d", subscriber.pause.LockedOnPositions, resumed)
	}
	if failures := positionFailures("", record, 1); len(failures) != 0 {
		testContext.Fatalf("position failures %v", failures)
	}
	if failures := pausedSubscriberFailures(record, cleanPause(), 1); len(failures) != 0 {
		testContext.Fatalf("clean resync failures %v", failures)
	}
}

// cleanPause is the F3 evidence of a one-APC pause that overflowed at the
// count cap under the default byte limit and validated its one
// resynchronized partition: 256 events of 784 accounted bytes each.
func cleanPause() *ssnmPauseRecord {
	return &ssnmPauseRecord{
		ContinuityLossObserved: true, QueueLimit: 256, QueuedAtLoss: 256, CountCapEnforced: true,
		QueueByteLimit: 1 << 20, QueuedBytesAtLoss: 256 * 784, SmallestEventBytes: 784, SmallestQueuedEventBytes: 784,
		BindingCap: ssnmBindingCount, SnapshotValidated: 1,
	}
}

// After a successful Resync and lock-on the paused subscriber must be
// lossless again: a duplicate or a gap after the lock-on fails F3.
func TestSSNMSubscriberAfterResyncMustStayLossless(testContext *testing.T) {
	plan := singlePlan(16, 1)
	preload := plan.preloadMessages()
	anchor := int64(1_000_000_000)
	resumed := preload + 25
	received := anchor + ssnmScheduled(1000, resumed-preload) + int64(2*time.Millisecond)
	for name, positions := range map[string][]uint64{
		"duplicate": {resumed, resumed + 1, resumed + 1, resumed + 2},
		"gap":       {resumed, resumed + 1, resumed + 3},
	} {
		testContext.Run(name, func(testContext *testing.T) {
			subscriber := newSSNMSubscriber(0, true, plan, 1000, 4, 1<<20)
			subscriber.pause = &ssnmPauseRecord{}
			subscriber.armReceipts(anchor, 0, 0, false)
			for position := uint64(0); position < preload+10; position++ {
				subscriber.observe(planEvent(plan, testPartition, position), 0)
			}
			subscriber.observe(m3ua.SSNMEvent{Kind: m3ua.SSNMContinuityLostEvent, ContinuityLost: true}, 0)
			subscriber.captureSnapshot(snapshotAfter(plan, testPartition, resumed))
			for _, position := range positions {
				subscriber.observe(planEvent(plan, testPartition, position), received)
			}
			record := subscriber.record([]uint64{positions[len(positions)-1] + 1})
			if subscriber.pause.SnapshotValidated != 1 || len(subscriber.pause.LockedOnPositions) != 1 || subscriber.pause.LockedOnPositions[0] != resumed {
				testContext.Fatalf("lock-on did not succeed: %+v", subscriber.pause)
			}
			if failures := pausedSubscriberFailures(record, cleanPause(), 1); len(failures) == 0 {
				testContext.Fatalf("post-resync %s not reported: %+v", name, record)
			}
		})
	}
}

func TestSSNMSubscriberOtherEventsFail(testContext *testing.T) {
	record := ssnmSubscriberRecord{Partitions: 1, FinalPositions: []uint64{10}, ExpectedFinalPositions: []uint64{10}, ssnmSubscriberCounts: ssnmSubscriberCounts{OtherEvents: 1}}
	if failures := healthySubscriberFailures(record, 1); len(failures) != 1 {
		testContext.Fatalf("healthy subscriber with an unexpected lifecycle event: %v", failures)
	}
	record.ContinuityLost = 1
	if failures := pausedSubscriberFailures(record, cleanPause(), 1); len(failures) != 1 {
		testContext.Fatalf("paused subscriber with an unexpected lifecycle event: %v", failures)
	}
}

func TestSSNMSubscriberRejectsStaleSnapshot(testContext *testing.T) {
	plan := singlePlan(16, 1)
	preload := plan.preloadMessages()
	anchor := int64(1_000_000_000)
	subscriber := newSSNMSubscriber(0, true, plan, 1000, 4, 1<<20)
	subscriber.armReceipts(anchor, 0, 0, false)
	for position := uint64(0); position < preload+10; position++ {
		subscriber.observe(planEvent(plan, testPartition, position), 0)
	}
	resumed := preload + 25
	// A snapshot missing the last three reports is not authoritative.
	subscriber.captureSnapshot(snapshotAfter(plan, testPartition, resumed-3))
	received := anchor + ssnmScheduled(1000, resumed-preload)
	subscriber.observe(planEvent(plan, testPartition, resumed), received)
	if record := subscriber.record([]uint64{resumed + 1}); record.SnapshotMismatches != 3 {
		testContext.Fatalf("stale snapshot mismatches = %d, want 3", record.SnapshotMismatches)
	}
}

func TestSSNMPausedSubscriberContract(testContext *testing.T) {
	record := ssnmSubscriberRecord{Partitions: 1, FinalPositions: []uint64{10}, ExpectedFinalPositions: []uint64{10}, ssnmSubscriberCounts: ssnmSubscriberCounts{ContinuityLost: 1}}
	good := cleanPause()
	if failures := pausedSubscriberFailures(record, good, 1); len(failures) != 0 {
		testContext.Fatalf("clean F3 pause failed: %v", failures)
	}
	for name, pause := range map[string]*ssnmPauseRecord{
		"never paused":   nil,
		"no loss":        {QueueLimit: 256, QueuedAtLoss: 256},
		"cap exceeded":   {ContinuityLossObserved: true, QueueLimit: 256, QueuedAtLoss: 257},
		"resync failure": {Error: "boom"},
		"not validated":  {ContinuityLossObserved: true, QueueLimit: 256, QueuedAtLoss: 256, CountCapEnforced: true},
	} {
		if failures := pausedSubscriberFailures(record, pause, 1); len(failures) == 0 {
			testContext.Fatalf("%s: F3 contract violation not reported", name)
		}
	}
	lossy := record
	lossy.Gaps = 1
	if failures := pausedSubscriberFailures(lossy, good, 1); len(failures) == 0 {
		testContext.Fatal("post-resync loss not reported")
	}
}

func TestSSNMVerdict(testContext *testing.T) {
	if verdict, _ := ssnmVerdict(nil); verdict != ssnmVerdictPass {
		testContext.Fatalf("no reasons = %s", verdict)
	}
	if verdict, _ := ssnmVerdict([]string{"inconclusive: x"}); verdict != ssnmVerdictUnknown {
		testContext.Fatalf("inconclusive reason = %s", verdict)
	}
	if verdict, _ := ssnmVerdict([]string{"inconclusive: x", "fail: y"}); verdict != ssnmVerdictFail {
		testContext.Fatalf("fail reason = %s", verdict)
	}
}

// TestSSNMSubscriberConcurrentAccounting exercises the subscriber goroutine's
// accounting against concurrent coordinator reads under the race detector.
func TestSSNMSubscriberConcurrentAccounting(testContext *testing.T) {
	plan := singlePlan(64, 1)
	subscriber := newSSNMSubscriber(0, false, plan, 1000, 256, 1<<20)
	subscriber.armReceipts(0, 0, 1000, true)
	var group sync.WaitGroup
	group.Add(2)
	go func() {
		defer group.Done()
		for position := uint64(0); position < 1000; position++ {
			subscriber.observe(planEvent(plan, testPartition, position), int64(position))
		}
	}()
	go func() {
		defer group.Done()
		for !subscriber.reachedAll([]uint64{1000}) {
			_ = subscriber.record([]uint64{1000})
		}
	}()
	group.Wait()
	if record := subscriber.record([]uint64{1000}); record.Accepted != 1000 {
		testContext.Fatalf("accepted %d", record.Accepted)
	}
}

// TestSSNMAttachArmsEverySubscriberAnchor pins the anchor on the paused
// subscriber too: its post-Resync lock-on estimates the position from the
// anchor, and a zero anchor locks on whole periods away from the truth.
func TestSSNMAttachArmsEverySubscriberAnchor(testContext *testing.T) {
	plan := singlePlan(16, 1)
	run := &ssnmSenderRun{config: ssnmConfig{TotalRate: 1000, APCs: 1, Records: 16, Subscribers: 2, Pause: ssnmPause{Offset: time.Second, Duration: time.Second}}, plan: plan, associations: 1}
	run.subscribers = []*ssnmSubscriber{newSSNMSubscriber(0, true, plan, 1000, 4, 1<<20), newSSNMSubscriber(1, false, plan, 1000, 4, 1<<20)}
	warmup := runSpec{Clock: &sharedClockWindow{Start: 5_000_000_000, End: 6_000_000_000}}
	if err := run.attach(&warmup, ssnmPhaseWarmup); err != nil || warmup.SSNM.Anchor != 5_000_000_000 || warmup.SSNM.Phase != ssnmPhaseWarmup {
		testContext.Fatalf("warm-up attach = %+v, %v", warmup.SSNM, err)
	}
	measurement := runSpec{Clock: &sharedClockWindow{Start: 9_000_000_000, End: 12_000_000_000}}
	if err := run.attach(&measurement, ""); err != nil || measurement.SSNM.Anchor != 5_000_000_000 || measurement.SSNM.Phase != ssnmPhaseMeasurement {
		testContext.Fatalf("measurement attach = %+v, %v", measurement.SSNM, err)
	}
	for _, subscriber := range run.subscribers {
		if subscriber.anchor != 5_000_000_000 {
			testContext.Fatalf("subscriber %d anchor = %d", subscriber.index, subscriber.anchor)
		}
	}
	if run.subscribers[0].receipts != nil || len(run.subscribers[1].receipts) != 3000 {
		testContext.Fatal("receipts must be stored for healthy subscribers only")
	}
	if run.pauseAt.Load() != 10_000_000_000 || run.first != 4000 || run.last != 7000 {
		testContext.Fatalf("pauseAt %d window [%d, %d)", run.pauseAt.Load(), run.first, run.last)
	}
	if err := run.attach(&runSpec{}, ""); err == nil {
		testContext.Fatal("attach without a shared clock accepted")
	}
}

// ssnmDelivery is one report as a subscription delivers it.
type ssnmDelivery struct {
	message  uint64
	preload  bool
	event    m3ua.SSNMEvent
	received int64
}

// roundRobinDeliveries is what a healthy SGP delivers under the total-rate
// schedule: every partition's preload, then generator message m on
// association m mod N's partition only, received delay after its schedule.
func roundRobinDeliveries(plan ssnmPlan, rate uint64, anchor int64, messages uint64, delay time.Duration) []ssnmDelivery {
	var deliveries []ssnmDelivery
	for step := uint64(0); step < plan.preloadSteps(); step++ {
		association, position := plan.preloadStep(step)
		deliveries = append(deliveries, ssnmDelivery{preload: true, event: associationEvent(plan, testPartitionOf(association), association, position), received: anchor})
	}
	for message := uint64(0); message < messages; message++ {
		association, position := plan.target(message)
		deliveries = append(deliveries, ssnmDelivery{
			message:  message,
			event:    associationEvent(plan, testPartitionOf(association), association, position),
			received: anchor + ssnmScheduled(rate, message) + int64(delay),
		})
	}
	return deliveries
}

// storeAfter is the store a healthy ASP holds once every partition has
// applied its expected positions.
func storeAfter(plan ssnmPlan, expected []uint64) m3ua.SSNMSnapshot {
	var snapshot m3ua.SSNMSnapshot
	for association, positions := range expected {
		knowledge := m3ua.SSNMPartitionKnowledge{Partition: testPartitionOf(association)}
		for destination := 0; destination < plan.records; destination++ {
			state, held := plan.expectedState(destination, positions)
			if !held {
				continue
			}
			knowledge.Destinations = append(knowledge.Destinations, m3ua.SSNMDestinationKnowledge{
				Destination:     m3ua.PointCodeRange{PointCode: plan.pointCode(association, destination)},
				Availability:    m3ua.SSNMAvailability{State: state},
				AvailabilitySet: true,
			})
		}
		snapshot.Partitions = append(snapshot.Partitions, knowledge)
	}
	return snapshot
}

// A healthy subscriber of the round-robin schedule sees each association's
// messages in its own partition, every partition in order, and ends each
// partition at the preload plus that association's share. Each mutation of
// the delivery is caught by the healthy-subscriber oracle.
func TestSSNMSubscriberRoundRobinStream(testContext *testing.T) {
	const (
		associations = 8
		rate         = 1000
		sent         = 200
		delay        = 3 * time.Millisecond
	)
	anchor := int64(1_000_000_000)
	plan := ssnmPlan{records: 16, apcs: 1, associations: associations}
	expected := plan.expectedPositions(sent)
	for association, position := range expected {
		if position != plan.preloadMessages()+sent/associations {
			testContext.Fatalf("association %d expected position %d, want %d", association, position, plan.preloadMessages()+sent/associations)
		}
	}
	for _, testCase := range []struct {
		name     string
		mutate   func([]ssnmDelivery) []ssnmDelivery
		expected []uint64
		failure  string
	}{
		{"clean", nil, expected, ""},
		{"message on the next association's partition", func(deliveries []ssnmDelivery) []ssnmDelivery {
			for index := range deliveries {
				if !deliveries[index].preload && deliveries[index].message == 57 {
					deliveries[index].event.Partition = testPartitionOf(int(58 % associations))
				}
			}
			return deliveries
		}, expected, "in another association's partition"},
		{"broadcast to every partition", func(deliveries []ssnmDelivery) []ssnmDelivery {
			var broadcast []ssnmDelivery
			for _, delivery := range deliveries {
				if delivery.preload {
					broadcast = append(broadcast, delivery)
					continue
				}
				for association := 0; association < associations; association++ {
					copied := delivery
					copied.event.Partition = testPartitionOf(association)
					broadcast = append(broadcast, copied)
				}
			}
			return broadcast
		}, expected, "in another association's partition"},
		{"one message lost", func(deliveries []ssnmDelivery) []ssnmDelivery {
			var kept []ssnmDelivery
			for _, delivery := range deliveries {
				if delivery.preload || delivery.message != 57 {
					kept = append(kept, delivery)
				}
			}
			return kept
		}, expected, "saw 1 missing"},
		{"one message duplicated", func(deliveries []ssnmDelivery) []ssnmDelivery {
			for index, delivery := range deliveries {
				if !delivery.preload && delivery.message == 57 {
					return append(deliveries[:index+1], deliveries[index:]...)
				}
			}
			return deliveries
		}, expected, "1 duplicate"},
		{"one association never delivered", func(deliveries []ssnmDelivery) []ssnmDelivery {
			var kept []ssnmDelivery
			for _, delivery := range deliveries {
				if delivery.event.Partition != testPartitionOf(5) {
					kept = append(kept, delivery)
				}
			}
			return kept
		}, expected, "saw 7 partitions, want 8"},
		{"broadcast expectation", nil, singlePlan(16, 1).expectedPositions(sent)[:1], "recorded 8 final and 1 expected positions"},
		{"every partition expected at the total", nil, []uint64{sent + 1, sent + 1, sent + 1, sent + 1, sent + 1, sent + 1, sent + 1, sent + 1}, "want 201"},
	} {
		testContext.Run(testCase.name, func(testContext *testing.T) {
			deliveries := roundRobinDeliveries(plan, rate, anchor, sent, delay)
			if testCase.mutate != nil {
				deliveries = testCase.mutate(deliveries)
			}
			subscriber := newSSNMSubscriber(0, false, plan, rate, 256, 1<<20)
			subscriber.armReceipts(anchor, 40, 160, true)
			for _, delivery := range deliveries {
				subscriber.observe(delivery.event, delivery.received)
			}
			record := subscriber.record(testCase.expected)
			failures := healthySubscriberFailures(record, associations)
			if testCase.failure == "" {
				if len(failures) != 0 || record.Accepted != associations*plan.preloadMessages()+sent || record.MisScoped != 0 {
					testContext.Fatalf("clean round-robin stream: failures %q record %+v", failures, record)
				}
				log := ssnmReportsResponse{Reports: make([]int64, sent), Completions: make([]int64, sent)}
				for message := range log.Reports {
					log.Reports[message] = anchor + ssnmScheduled(rate, uint64(message))
				}
				histogram := newDurationHistogram()
				missing := subscriber.joinDelays(histogram, log, map[uint64]bool{})
				if percentiles := histogram.percentiles(); missing != 0 || percentiles.Count != 120 || percentiles.Max != delay {
					testContext.Fatalf("join missing %d percentiles %+v, want 120 receipts %s after their reports", missing, percentiles, delay)
				}
				return
			}
			if !reasonsContain(failures, testCase.failure) {
				testContext.Fatalf("mutated stream failures %q, want %q; record %+v", failures, testCase.failure, record)
			}
		})
	}
}

// A lost measurement message is also a missing receipt in the delay join.
func TestSSNMSubscriberRoundRobinJoinCountsLostMessage(testContext *testing.T) {
	anchor := int64(1_000_000_000)
	plan := ssnmPlan{records: 16, apcs: 1, associations: 8}
	subscriber := newSSNMSubscriber(0, false, plan, 1000, 256, 1<<20)
	subscriber.armReceipts(anchor, 40, 160, true)
	for _, delivery := range roundRobinDeliveries(plan, 1000, anchor, 200, time.Millisecond) {
		if delivery.preload || delivery.message != 57 {
			subscriber.observe(delivery.event, delivery.received)
		}
	}
	log := ssnmReportsResponse{Reports: make([]int64, 200), Completions: make([]int64, 200)}
	if missing := subscriber.joinDelays(newDurationHistogram(), log, map[uint64]bool{}); missing != 1 {
		testContext.Fatalf("missing receipts %d, want message 57", missing)
	}
	if missing := subscriber.joinDelays(newDurationHistogram(), log, map[uint64]bool{57: true}); missing != 0 {
		testContext.Fatalf("a failed report was counted missing: %d", missing)
	}
}

// After a Resync each of the eight partitions locks on at the first report
// its association receives after the pause, and the retained snapshot is
// validated per association.
func TestSSNMSubscriberLocksOnPerAssociationAfterResync(testContext *testing.T) {
	const associations = 8
	anchor := int64(1_000_000_000)
	plan := ssnmPlan{records: 16, apcs: 1, associations: associations}
	for _, stale := range []bool{false, true} {
		subscriber := newSSNMSubscriber(0, true, plan, 1000, 4, 1<<20)
		subscriber.pause = &ssnmPauseRecord{}
		subscriber.armReceipts(anchor, 0, 0, false)
		deliveries := roundRobinDeliveries(plan, 1000, anchor, 216, 2*time.Millisecond)
		for _, delivery := range deliveries {
			if delivery.preload || delivery.message < 80 {
				subscriber.observe(delivery.event, delivery.received)
			}
		}
		// Messages 80 .. 199 were lost; the snapshot holds the state after
		// them, or, stale, misses association 3's last message.
		held := plan.expectedPositions(200)
		if stale {
			held[3]--
		}
		partitions, destinations := subscriber.captureSnapshot(storeAfter(plan, held))
		if partitions != associations || destinations != associations*16 {
			testContext.Fatalf("snapshot consumed %d partitions and %d destinations", partitions, destinations)
		}
		for _, delivery := range deliveries {
			if !delivery.preload && delivery.message >= 200 {
				subscriber.observe(delivery.event, delivery.received)
			}
		}
		record := subscriber.record(plan.expectedPositions(216))
		if stale {
			if record.SnapshotMismatches == 0 {
				testContext.Fatal("a snapshot missing one association's last report validated")
			}
			continue
		}
		if record.SnapshotMismatches != 0 || record.Gaps != 0 || record.Unexpected != 0 || record.MisScoped != 0 || subscriber.pause.SnapshotValidated != associations {
			testContext.Fatalf("resync accounting = %+v pause = %+v", record, subscriber.pause)
		}
		want := plan.preloadMessages() + 25
		if len(subscriber.pause.LockedOnPositions) != associations {
			testContext.Fatalf("locked on %v, want %d partitions at %d", subscriber.pause.LockedOnPositions, associations, want)
		}
		for _, position := range subscriber.pause.LockedOnPositions {
			if position != want {
				testContext.Fatalf("locked on %v, want every partition at %d", subscriber.pause.LockedOnPositions, want)
			}
		}
		pause := cleanPause()
		pause.SnapshotValidated = associations
		if failures := pausedSubscriberFailures(record, pause, associations); len(failures) != 0 {
			testContext.Fatalf("clean eight-partition resync failures %v", failures)
		}
	}
}
