package main

import (
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua"
)

var testPartition = m3ua.SSNMPartition{Kind: m3ua.SSNMStandalonePartition, Association: 1}

// planEvent builds the report a healthy SGP delivers for one plan position.
func planEvent(plan ssnmPlan, partition m3ua.SSNMPartition, position uint64) m3ua.SSNMEvent {
	chunk := plan.chunk(position)
	kind := m3ua.SSNMDestinationAvailableReport
	if chunk.availability == m3ua.DestinationUnavailable {
		kind = m3ua.SSNMDestinationUnavailableReport
	}
	destinations := ssnmDestinations(plan.records)[chunk.first : chunk.first+chunk.count]
	return m3ua.SSNMEvent{
		Kind:      m3ua.SSNMReportEvent,
		Partition: partition,
		ReportSet: true,
		Report:    m3ua.SSNMReport{Kind: kind, Partition: partition, Destinations: append([]m3ua.PointCodeRange(nil), destinations...)},
	}
}

func TestSSNMSubscriberAcceptsInOrderStream(testContext *testing.T) {
	plan := ssnmPlan{records: 8, apcs: 2}
	subscriber := newSSNMSubscriber(0, false, plan, 1000, 1, 256)
	for position := uint64(0); position < 40; position++ {
		subscriber.observe(planEvent(plan, testPartition, position), int64(position))
	}
	record := subscriber.record(40)
	if record.Accepted != 40 || record.Gaps != 0 || record.Duplicates != 0 || record.Unexpected != 0 {
		testContext.Fatalf("in-order stream accounting = %+v", record)
	}
	if failures := healthySubscriberFailures(record, 1); len(failures) != 0 {
		testContext.Fatalf("healthy subscriber failures = %v", failures)
	}
}

func TestSSNMSubscriberDetectsDuplicateGapAndUnexpected(testContext *testing.T) {
	plan := ssnmPlan{records: 8, apcs: 2}
	subscriber := newSSNMSubscriber(0, false, plan, 1000, 1, 256)
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
	record := subscriber.record(7)
	if record.Duplicates != 1 || record.Gaps != 2 || record.Unexpected != 3 || record.Accepted != 5 {
		testContext.Fatalf("anomaly accounting = %+v", record)
	}
	if failures := healthySubscriberFailures(record, 1); len(failures) == 0 {
		testContext.Fatal("a lossy healthy subscriber reported no failure")
	}
}

func TestSSNMSubscriberCountsLifecycleEventsAndPartitionCap(testContext *testing.T) {
	plan := ssnmPlan{records: 8, apcs: 2}
	subscriber := newSSNMSubscriber(0, false, plan, 1000, 1, 256)
	subscriber.observe(m3ua.SSNMEvent{Kind: m3ua.SSNMContinuityLostEvent, ContinuityLost: true}, 0)
	subscriber.observe(m3ua.SSNMEvent{Kind: m3ua.SSNMResourceLossEvent}, 0)
	subscriber.observe(m3ua.SSNMEvent{Kind: m3ua.SSNMPartitionInvalidatedEvent}, 0)
	subscriber.observe(m3ua.SSNMEvent{Kind: m3ua.SSNMBindingAdmittedEvent}, 0)
	subscriber.observe(planEvent(plan, testPartition, 0), 0)
	other := m3ua.SSNMPartition{Kind: m3ua.SSNMStandalonePartition, Association: 2}
	subscriber.observe(planEvent(plan, other, 0), 0)
	record := subscriber.record(1)
	if record.ContinuityLost != 1 || record.ResourceLoss != 1 || record.Invalidated != 1 || record.OtherEvents != 1 || record.Unexpected != 1 || record.Partitions != 1 {
		testContext.Fatalf("lifecycle accounting = %+v", record)
	}
	if failures := healthySubscriberFailures(record, 2); len(failures) < 3 {
		testContext.Fatalf("expected continuity, unexpected and partition failures, got %v", failures)
	}
}

func TestSSNMSubscriberJoinsReceiptsWithReports(testContext *testing.T) {
	plan := ssnmPlan{records: 8, apcs: 2}
	preload := plan.preloadMessages()
	anchor := int64(1_000_000_000)
	subscriber := newSSNMSubscriber(0, false, plan, 1000, 1, 256)
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
	plan := ssnmPlan{records: 16, apcs: 1}
	preload := plan.preloadMessages()
	anchor := int64(1_000_000_000)
	subscriber := newSSNMSubscriber(0, true, plan, 1000, 1, 4)
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
	record := subscriber.record(resumed + 2)
	if record.SnapshotMismatches != 0 || record.Gaps != 0 || record.Unexpected != 0 || subscriber.pause.SnapshotValidated != 1 {
		testContext.Fatalf("resync accounting = %+v pause = %+v", record, subscriber.pause)
	}
	if len(subscriber.pause.LockedOnPositions) != 1 || subscriber.pause.LockedOnPositions[0] != resumed {
		testContext.Fatalf("locked on %v, want %d", subscriber.pause.LockedOnPositions, resumed)
	}
	if failures := positionFailures("", record, 1); len(failures) != 0 {
		testContext.Fatalf("position failures %v", failures)
	}
}

func TestSSNMSubscriberRejectsStaleSnapshot(testContext *testing.T) {
	plan := ssnmPlan{records: 16, apcs: 1}
	preload := plan.preloadMessages()
	anchor := int64(1_000_000_000)
	subscriber := newSSNMSubscriber(0, true, plan, 1000, 1, 4)
	subscriber.armReceipts(anchor, 0, 0, false)
	for position := uint64(0); position < preload+10; position++ {
		subscriber.observe(planEvent(plan, testPartition, position), 0)
	}
	resumed := preload + 25
	// A snapshot missing the last three reports is not authoritative.
	subscriber.captureSnapshot(snapshotAfter(plan, testPartition, resumed-3))
	received := anchor + ssnmScheduled(1000, resumed-preload)
	subscriber.observe(planEvent(plan, testPartition, resumed), received)
	if record := subscriber.record(resumed + 1); record.SnapshotMismatches != 3 {
		testContext.Fatalf("stale snapshot mismatches = %d, want 3", record.SnapshotMismatches)
	}
}

func TestSSNMPausedSubscriberContract(testContext *testing.T) {
	record := ssnmSubscriberRecord{Partitions: 1, FinalPositions: []uint64{10}, ExpectedFinalPosition: 10, ssnmSubscriberCounts: ssnmSubscriberCounts{ContinuityLost: 1}}
	good := &ssnmPauseRecord{ContinuityLossObserved: true, QueueLimit: 256, QueuedAtLoss: 256, CountCapEnforced: true, SnapshotValidated: 1}
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
	plan := ssnmPlan{records: 64, apcs: 1}
	subscriber := newSSNMSubscriber(0, false, plan, 1000, 1, 256)
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
		for !subscriber.reachedAll(1000, 1) {
			_ = subscriber.record(1000)
		}
	}()
	group.Wait()
	if record := subscriber.record(1000); record.Accepted != 1000 {
		testContext.Fatalf("accepted %d", record.Accepted)
	}
}

// TestSSNMAttachArmsEverySubscriberAnchor pins the anchor on the paused
// subscriber too: its post-Resync lock-on estimates the position from the
// anchor, and a zero anchor locks on whole periods away from the truth.
func TestSSNMAttachArmsEverySubscriberAnchor(testContext *testing.T) {
	plan := ssnmPlan{records: 16, apcs: 1}
	run := &ssnmSenderRun{config: ssnmConfig{Rate: 1000, APCs: 1, Records: 16, Subscribers: 2, Pause: ssnmPause{Offset: time.Second, Duration: time.Second}}, plan: plan, associations: 1}
	run.subscribers = []*ssnmSubscriber{newSSNMSubscriber(0, true, plan, 1000, 1, 4), newSSNMSubscriber(1, false, plan, 1000, 1, 4)}
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
	if run.subscribers[0].receipts != nil || len(run.subscribers[1].receipts) != 1 || len(run.subscribers[1].receipts[0]) != 3000 {
		testContext.Fatal("receipts must be stored for healthy subscribers only")
	}
	if run.pauseAt.Load() != 10_000_000_000 || run.first != 4000 || run.last != 7000 {
		testContext.Fatalf("pauseAt %d window [%d, %d)", run.pauseAt.Load(), run.first, run.last)
	}
	if err := run.attach(&runSpec{}, ""); err == nil {
		testContext.Fatal("attach without a shared clock accepted")
	}
}
