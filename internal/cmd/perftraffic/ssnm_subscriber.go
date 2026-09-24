package main

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/gomaja/go-m3ua"
)

// ssnmEventStream is the part of an m3ua.SSNMSubscription a subscriber uses.
type ssnmEventStream interface {
	Next(ctx context.Context) (m3ua.SSNMEvent, error)
	Resync() (m3ua.SSNMSnapshot, error)
	Close() error
}

// ssnmSubscriber is one SubscribeSSNM consumer on the ASP Endpoint. It checks
// every delivered report against the deterministic plan: every partition
// carries exactly one association's destinations, and per partition a healthy
// subscriber must see every position exactly once and in order.
type ssnmSubscriber struct {
	index        int
	paused       bool
	subscription ssnmEventStream
	plan         ssnmPlan
	rate         uint64
	// queueLimit and queueBytes are the subscription's event and accounted
	// byte limits in force.
	queueLimit int
	queueBytes int

	mutex      sync.Mutex
	partitions map[m3ua.SSNMPartition]*ssnmPartitionProgress
	// byAssociation is the partition carrying each association's
	// destinations, bound by the first report naming them.
	byAssociation []*ssnmPartitionProgress
	counts        ssnmSubscriberCounts
	// receipts[m-receiptFirst] is 1 + the microseconds between the anchor and
	// this subscriber's receipt of generator message m, zero when not
	// received. Every message reaches one partition, so one slot per message
	// suffices. It is pointer-free and allocated once per measurement.
	receipts     []uint32
	receiptFirst uint64
	receiptLast  uint64
	anchor       int64
	// snapshot holds a Resync snapshot's states by association until each
	// partition's first report after the Resync validates them.
	snapshot  map[int][]uint8
	pause     *ssnmPauseRecord
	pauseDone bool
	failure   string
}

type ssnmPartitionProgress struct {
	association int
	next        uint64
	relock      bool
}

// ssnmSubscriberCounts are one subscriber's event counts. MisScoped counts
// reports whose destinations belong to another association's partition than
// the one that delivered them, including a second partition naming an
// association another partition already carries.
type ssnmSubscriberCounts struct {
	Events             uint64 `json:"events"`
	Reports            uint64 `json:"reports"`
	Accepted           uint64 `json:"accepted"`
	Gaps               uint64 `json:"gaps"`
	Duplicates         uint64 `json:"duplicates"`
	Unexpected         uint64 `json:"unexpected"`
	MisScoped          uint64 `json:"mis_scoped"`
	ContinuityLost     uint64 `json:"continuity_lost"`
	ResourceLoss       uint64 `json:"resource_loss"`
	Invalidated        uint64 `json:"invalidated"`
	OtherEvents        uint64 `json:"other_events"`
	SnapshotMismatches uint64 `json:"snapshot_mismatches"`
}

// Snapshot availability codes retained for post-Resync validation.
const (
	ssnmStateAbsent      = uint8(0)
	ssnmStateUnavailable = uint8(1)
	ssnmStateAvailable   = uint8(2)
)

// newSSNMSubscriber checks a subscription against plan, whose total rate is
// rate, under the subscription's event and byte limits.
func newSSNMSubscriber(index int, paused bool, plan ssnmPlan, rate uint64, queueLimit, queueBytes int) *ssnmSubscriber {
	return &ssnmSubscriber{
		index:         index,
		paused:        paused,
		plan:          plan,
		rate:          rate,
		queueLimit:    queueLimit,
		queueBytes:    queueBytes,
		partitions:    make(map[m3ua.SSNMPartition]*ssnmPartitionProgress),
		byAssociation: make([]*ssnmPartitionProgress, plan.associations),
	}
}

// armReceipts pins the generator anchor, which the post-Resync lock-on needs,
// and allocates the measurement-window receipt store when store is set.
func (subscriber *ssnmSubscriber) armReceipts(anchor int64, first, last uint64, store bool) {
	subscriber.mutex.Lock()
	defer subscriber.mutex.Unlock()
	subscriber.anchor = anchor
	if !store {
		return
	}
	subscriber.receiptFirst, subscriber.receiptLast = first, last
	subscriber.receipts = make([]uint32, last-first)
}

// observe accounts one delivered event received at the shared-clock instant.
func (subscriber *ssnmSubscriber) observe(event m3ua.SSNMEvent, received int64) {
	subscriber.mutex.Lock()
	defer subscriber.mutex.Unlock()
	subscriber.observeLocked(event, received)
}

func (subscriber *ssnmSubscriber) observeLocked(event m3ua.SSNMEvent, received int64) {
	subscriber.counts.Events++
	switch event.Kind {
	case m3ua.SSNMReportEvent:
		subscriber.counts.Reports++
		subscriber.observeReportLocked(event, received)
	case m3ua.SSNMContinuityLostEvent:
		subscriber.counts.ContinuityLost++
	case m3ua.SSNMResourceLossEvent:
		subscriber.counts.ResourceLoss++
	case m3ua.SSNMPartitionInvalidatedEvent:
		subscriber.counts.Invalidated++
	default:
		subscriber.counts.OtherEvents++
	}
}

// decodeSSNMReport maps a delivered report back to plan content: a contiguous
// run of one association's generated destinations with one availability.
func decodeSSNMReport(event m3ua.SSNMEvent, plan ssnmPlan) (int, ssnmChunk, bool) {
	if !event.ReportSet || len(event.Report.Destinations) == 0 {
		return 0, ssnmChunk{}, false
	}
	var availability m3ua.DestinationAvailability
	switch event.Report.Kind {
	case m3ua.SSNMDestinationUnavailableReport:
		availability = m3ua.DestinationUnavailable
	case m3ua.SSNMDestinationAvailableReport:
		availability = m3ua.DestinationAvailable
	default:
		return 0, ssnmChunk{}, false
	}
	destinations := event.Report.Destinations
	association, first, found := plan.locate(destinations[0].PointCode)
	if !found {
		return 0, ssnmChunk{}, false
	}
	for index, destination := range destinations {
		if destination.Mask != 0 || destination.PointCode != destinations[0].PointCode+uint32(index) {
			return 0, ssnmChunk{}, false
		}
	}
	if first+len(destinations) > plan.records {
		return 0, ssnmChunk{}, false
	}
	return association, ssnmChunk{first: first, count: len(destinations), availability: availability}, true
}

func (subscriber *ssnmSubscriber) observeReportLocked(event m3ua.SSNMEvent, received int64) {
	association, chunk, ok := decodeSSNMReport(event, subscriber.plan)
	if !ok {
		subscriber.counts.Unexpected++
		return
	}
	progress := subscriber.partitions[event.Partition]
	if progress == nil {
		if subscriber.byAssociation[association] != nil {
			subscriber.counts.MisScoped++
			return
		}
		progress = &ssnmPartitionProgress{association: association}
		subscriber.byAssociation[association] = progress
		subscriber.partitions[event.Partition] = progress
	}
	if progress.association != association {
		subscriber.counts.MisScoped++
		return
	}
	if progress.relock {
		position, found := subscriber.lockOnLocked(progress, chunk, received)
		if !found {
			subscriber.counts.Unexpected++
			return
		}
		subscriber.validateSnapshotLocked(association, position)
		progress.relock = false
		if subscriber.pause != nil {
			subscriber.pause.LockedOnPositions = append(subscriber.pause.LockedOnPositions, position)
		}
		subscriber.acceptLocked(progress, position, received)
		return
	}
	if subscriber.plan.chunk(progress.next) == chunk {
		subscriber.acceptLocked(progress, progress.next, received)
		return
	}
	if progress.next > 0 && subscriber.plan.chunk(progress.next-1) == chunk {
		subscriber.counts.Duplicates++
		return
	}
	horizon := progress.next + subscriber.plan.period() + subscriber.plan.preloadMessages()
	for position := progress.next + 1; position < horizon; position++ {
		if subscriber.plan.chunk(position) == chunk {
			subscriber.counts.Gaps += position - progress.next
			subscriber.acceptLocked(progress, position, received)
			return
		}
	}
	subscriber.counts.Unexpected++
}

func (subscriber *ssnmSubscriber) acceptLocked(progress *ssnmPartitionProgress, position uint64, received int64) {
	progress.next = position + 1
	subscriber.counts.Accepted++
	message, generated := subscriber.plan.message(progress.association, position)
	if !generated || subscriber.receipts == nil || message < subscriber.receiptFirst || message >= subscriber.receiptLast {
		return
	}
	elapsed := (received - subscriber.anchor) / int64(time.Microsecond)
	if elapsed < 0 {
		elapsed = 0
	}
	subscriber.receipts[message-subscriber.receiptFirst] = uint32(min(elapsed, math.MaxUint32-1) + 1)
}

// lockOnLocked finds the position of the first report after a Resync. The
// generator never reports before its schedule, so the position is the latest
// one at or before the scheduled estimate that carries this content and lies
// after everything consumed before the loss. The estimate is the partition
// position of the latest message due for this association: of the due
// messages 0 .. due-1, association a carries a, a+N, a+2N, ... The pattern
// repeats every period positions, so the search spans one period.
func (subscriber *ssnmSubscriber) lockOnLocked(progress *ssnmPartitionProgress, chunk ssnmChunk, received int64) (uint64, bool) {
	preload := subscriber.plan.preloadMessages()
	estimate := preload
	if due := ssnmDue(subscriber.rate, received-subscriber.anchor); due > uint64(progress.association) {
		estimate = preload + (due-1-uint64(progress.association))/uint64(subscriber.plan.associations)
	}
	period := subscriber.plan.period()
	for step := uint64(0); step < period; step++ {
		if step > estimate {
			break
		}
		position := estimate - step
		if position < progress.next {
			break
		}
		if subscriber.plan.chunk(position) == chunk {
			return position, true
		}
	}
	return 0, false
}

// captureSnapshot retains a Resync snapshot's generated destinations for
// validation once each partition's next position is known.
func (subscriber *ssnmSubscriber) captureSnapshot(snapshot m3ua.SSNMSnapshot) (partitions, destinations int) {
	subscriber.mutex.Lock()
	defer subscriber.mutex.Unlock()
	subscriber.snapshot = make(map[int][]uint8)
	for _, knowledge := range snapshot.Partitions {
		progress := subscriber.partitions[knowledge.Partition]
		if progress == nil {
			if len(knowledge.Destinations) != 0 {
				subscriber.counts.SnapshotMismatches++
			}
			continue
		}
		partitions++
		destinations += len(knowledge.Destinations)
		states, invalid := ssnmKnowledgeStates(knowledge, subscriber.plan, progress.association)
		subscriber.counts.SnapshotMismatches += invalid
		subscriber.snapshot[progress.association] = states
	}
	for _, progress := range subscriber.partitions {
		progress.relock = true
	}
	return partitions, destinations
}

// ssnmKnowledgeStates maps one partition's retained knowledge onto the
// association's plan destinations and counts entries no plan position of that
// partition can produce: a masked range, a destination outside the
// association's range, or no availability.
func ssnmKnowledgeStates(knowledge m3ua.SSNMPartitionKnowledge, plan ssnmPlan, association int) ([]uint8, uint64) {
	states := make([]uint8, plan.records)
	var invalid uint64
	for _, destination := range knowledge.Destinations {
		owner, offset, found := plan.locate(destination.Destination.PointCode)
		if destination.Destination.Mask != 0 || !found || owner != association || !destination.AvailabilitySet {
			invalid++
			continue
		}
		switch destination.Availability.State {
		case m3ua.DestinationUnavailable:
			states[offset] = ssnmStateUnavailable
		case m3ua.DestinationAvailable:
			states[offset] = ssnmStateAvailable
		default:
			invalid++
		}
	}
	return states, invalid
}

// stateMismatches counts the destinations whose retained state differs from
// the plan's after exactly positions, an absent destination included.
func (plan ssnmPlan) stateMismatches(states []uint8, positions uint64) uint64 {
	var mismatches uint64
	for destination, state := range states {
		expected, present := plan.expectedState(destination, positions)
		want := ssnmStateAbsent
		if present {
			want = ssnmStateAvailable
			if expected == m3ua.DestinationUnavailable {
				want = ssnmStateUnavailable
			}
		}
		if state != want {
			mismatches++
		}
	}
	return mismatches
}

// validateSnapshotLocked checks that the retained snapshot equals the plan's
// state after exactly the positions before the first post-Resync report.
func (subscriber *ssnmSubscriber) validateSnapshotLocked(association int, position uint64) {
	states, held := subscriber.snapshot[association]
	if !held {
		subscriber.counts.SnapshotMismatches++
		return
	}
	delete(subscriber.snapshot, association)
	subscriber.counts.SnapshotMismatches += subscriber.plan.stateMismatches(states, position)
	if subscriber.pause != nil {
		subscriber.pause.SnapshotValidated++
	}
}

// positions reports the next expected position of every association's
// partition, zero for an association no partition has carried yet.
func (subscriber *ssnmSubscriber) positions() []uint64 {
	subscriber.mutex.Lock()
	defer subscriber.mutex.Unlock()
	positions := make([]uint64, len(subscriber.byAssociation))
	for association, progress := range subscriber.byAssociation {
		if progress != nil {
			positions[association] = progress.next
		}
	}
	return positions
}

// reachedAll reports whether every association's partition has reached its
// expected next position.
func (subscriber *ssnmSubscriber) reachedAll(expected []uint64) bool {
	reached := subscriber.positions()
	if len(reached) != len(expected) {
		return false
	}
	for association, next := range reached {
		if next < expected[association] {
			return false
		}
	}
	return true
}

// reached reports whether one association's partition has reached position.
func (subscriber *ssnmSubscriber) reached(association int, position uint64) bool {
	subscriber.mutex.Lock()
	defer subscriber.mutex.Unlock()
	progress := subscriber.byAssociation[association]
	return progress != nil && progress.next >= position
}

func (subscriber *ssnmSubscriber) setFailure(err error) {
	subscriber.mutex.Lock()
	defer subscriber.mutex.Unlock()
	if subscriber.failure == "" {
		subscriber.failure = err.Error()
	}
}

// run consumes the subscription until ctx ends. The paused subscriber polls
// its schedule between events and runs its F3 pause and recovery once.
func (subscriber *ssnmSubscriber) run(ctx context.Context, clock measurementClock, pauseAt func() int64, pause ssnmPause) {
	for {
		callContext, cancel := ctx, context.CancelFunc(func() {})
		if subscriber.paused && !subscriber.pauseDone {
			if at := pauseAt(); at != 0 {
				now, err := clock.Now()
				if err != nil {
					subscriber.setFailure(err)
					return
				}
				if now >= at {
					subscriber.pauseAndRecover(ctx, clock, at, pause)
					continue
				}
			}
			callContext, cancel = context.WithTimeout(ctx, 10*time.Millisecond)
		}
		event, err := subscriber.subscription.Next(callContext)
		cancel()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, m3ua.ErrSSNMSubscriptionClosed) {
				return
			}
			if errors.Is(err, context.DeadlineExceeded) {
				continue
			}
			subscriber.setFailure(err)
			return
		}
		received, err := clock.Now()
		if err != nil {
			subscriber.setFailure(err)
			return
		}
		subscriber.observe(event, received)
	}
}

// ssnmPauseRecord is the F3 overflow and recovery evidence of the paused
// subscriber. QueueLimit and QueueByteLimit are the subscription's caps in
// force; QueuedAtLoss and QueuedBytesAtLoss are the events it retained before
// the continuity-loss marker and their accounted bytes, recomputed from the
// delivered events by the documented SubscriptionQueueBytes formula.
// SmallestEventBytes is the smallest event the generator can queue after the
// preload, which decides whether another event could have fitted, and
// SmallestQueuedEventBytes the smallest retained one, which must not be
// below it. CountCapEnforced and ByteCapEnforced report each cap reached at
// the loss, and BindingCap names the one that bound (ssnmCapsReached).
type ssnmPauseRecord struct {
	Subscriber               int      `json:"subscriber"`
	ScheduledPauseNS         int64    `json:"scheduled_pause_ns"`
	PausedAtNS               int64    `json:"paused_at_ns"`
	ResumedAtNS              int64    `json:"resumed_at_ns"`
	ContinuityLossObserved   bool     `json:"continuity_loss_observed"`
	QueueLimit               int      `json:"queue_limit"`
	QueuedAtLoss             int      `json:"queued_at_loss"`
	CountCapEnforced         bool     `json:"count_cap_enforced"`
	QueueByteLimit           int      `json:"queue_byte_limit"`
	QueuedBytesAtLoss        int      `json:"queued_bytes_at_loss"`
	SmallestEventBytes       int      `json:"smallest_event_bytes"`
	SmallestQueuedEventBytes int      `json:"smallest_queued_event_bytes"`
	ByteCapEnforced          bool     `json:"byte_cap_enforced"`
	BindingCap               string   `json:"binding_cap"`
	QueuedStateEntries       int      `json:"queued_state_entries"`
	DrainQueuedNS            int64    `json:"drain_queued_ns"`
	ResyncNS                 int64    `json:"resync_ns"`
	SnapshotConsumeNS        int64    `json:"snapshot_consume_ns"`
	RecoveryNS               int64    `json:"recovery_ns"`
	SnapshotPartitions       int      `json:"snapshot_partitions"`
	SnapshotDestinations     int      `json:"snapshot_destinations"`
	SnapshotValidated        int      `json:"snapshot_partitions_validated"`
	LockedOnPositions        []uint64 `json:"locked_on_positions,omitempty"`
	Error                    string   `json:"error,omitempty"`
}

func (subscriber *ssnmSubscriber) pauseAndRecover(ctx context.Context, clock measurementClock, pauseAt int64, pause ssnmPause) {
	subscriber.pauseDone = true
	record := &ssnmPauseRecord{
		Subscriber: subscriber.index, ScheduledPauseNS: pauseAt,
		QueueLimit: subscriber.queueLimit, QueueByteLimit: subscriber.queueBytes,
		SmallestEventBytes: ssnmWorkloadEventBytes(subscriber.plan.apcs),
	}
	subscriber.mutex.Lock()
	subscriber.pause = record
	subscriber.mutex.Unlock()
	fail := func(err error) {
		subscriber.mutex.Lock()
		record.Error = err.Error()
		subscriber.mutex.Unlock()
	}
	paused, err := clock.Now()
	if err != nil {
		fail(err)
		return
	}
	resumeAt := pauseAt + int64(pause.Duration)
	if err := waitSharedUntil(ctx, clock, resumeAt); err != nil {
		fail(err)
		return
	}
	resumed, err := clock.Now()
	if err != nil {
		fail(err)
		return
	}
	// The drain stops one event past the count cap, which is enough to show
	// the cap exceeded, and never runs unbounded.
	queued, queuedBytes, smallest, entries, lossSeen := 0, 0, 0, 0, false
	for queued <= subscriber.queueLimit {
		callContext, cancel := context.WithTimeout(ctx, time.Second)
		event, nextErr := subscriber.subscription.Next(callContext)
		cancel()
		if nextErr != nil {
			fail(nextErr)
			return
		}
		received, clockErr := clock.Now()
		if clockErr != nil {
			fail(clockErr)
			return
		}
		if event.Kind == m3ua.SSNMContinuityLostEvent {
			subscriber.observe(event, received)
			lossSeen = true
			break
		}
		size := ssnmEventBytes(event)
		if queued == 0 || size < smallest {
			smallest = size
		}
		queued++
		queuedBytes += size
		entries += len(event.Updated)
		subscriber.observe(event, received)
	}
	drained, _ := clock.Now()
	subscriber.mutex.Lock()
	record.PausedAtNS, record.ResumedAtNS = paused, resumed
	record.QueuedAtLoss, record.QueuedStateEntries = queued, entries
	record.QueuedBytesAtLoss, record.SmallestQueuedEventBytes = queuedBytes, smallest
	record.ContinuityLossObserved = lossSeen
	record.CountCapEnforced, record.ByteCapEnforced = ssnmCapsReached(record)
	record.BindingCap = ssnmBindingCap(record.CountCapEnforced, record.ByteCapEnforced)
	record.DrainQueuedNS = drained - resumed
	subscriber.mutex.Unlock()
	if !lossSeen {
		return
	}
	before, _ := clock.Now()
	snapshot, resyncErr := subscriber.subscription.Resync()
	after, _ := clock.Now()
	if resyncErr != nil {
		fail(resyncErr)
		return
	}
	partitions, destinations := subscriber.captureSnapshot(snapshot)
	consumed, _ := clock.Now()
	subscriber.mutex.Lock()
	record.ResyncNS = after - before
	record.SnapshotConsumeNS = consumed - after
	record.RecoveryNS = consumed - resumed
	record.SnapshotPartitions, record.SnapshotDestinations = partitions, destinations
	subscriber.mutex.Unlock()
}

// waitSharedUntil sleeps until the shared clock reaches target, using timers
// only as wake-up hints.
func waitSharedUntil(ctx context.Context, clock measurementClock, target int64) error {
	for {
		now, err := clock.Now()
		if err != nil {
			return err
		}
		if now >= target {
			return nil
		}
		timer := time.NewTimer(min(time.Duration(target-now), 50*time.Millisecond))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// ssnmSubscriberRecord is one subscriber's accounting in the sender record.
// FinalPositions and ExpectedFinalPositions are indexed by association: each
// partition ends at the preload plus the generator messages its association
// was sent.
type ssnmSubscriberRecord struct {
	Index int    `json:"index"`
	Role  string `json:"role"`
	ssnmSubscriberCounts
	Partitions             int      `json:"partitions"`
	FinalPositions         []uint64 `json:"final_positions"`
	ExpectedFinalPositions []uint64 `json:"expected_final_positions"`
	MissingReceipts        uint64   `json:"missing_receipts"`
	Error                  string   `json:"error,omitempty"`
}

// joinDelays records report-to-receipt delays for the measurement messages
// this subscriber received and reports how many successfully reported
// messages it never received.
func (subscriber *ssnmSubscriber) joinDelays(histogram *durationHistogram, log ssnmReportsResponse, failed map[uint64]bool) (missing uint64) {
	subscriber.mutex.Lock()
	defer subscriber.mutex.Unlock()
	if subscriber.receipts == nil {
		return 0
	}
	for message := subscriber.receiptFirst; message < subscriber.receiptLast; message++ {
		index := message - log.From
		if message < log.From || index >= uint64(len(log.Reports)) || failed[message] {
			continue
		}
		receipt := subscriber.receipts[message-subscriber.receiptFirst]
		if receipt == 0 {
			missing++
			continue
		}
		received := subscriber.anchor + int64(receipt-1)*int64(time.Microsecond)
		histogram.record(time.Duration(received - log.Reports[index]))
	}
	return missing
}

func (subscriber *ssnmSubscriber) record(expected []uint64) ssnmSubscriberRecord {
	positions := subscriber.positions()
	subscriber.mutex.Lock()
	defer subscriber.mutex.Unlock()
	role := "healthy"
	if subscriber.paused {
		role = "paused"
	}
	return ssnmSubscriberRecord{
		Index:                  subscriber.index,
		Role:                   role,
		ssnmSubscriberCounts:   subscriber.counts,
		Partitions:             len(subscriber.partitions),
		FinalPositions:         positions,
		ExpectedFinalPositions: append([]uint64(nil), expected...),
		Error:                  subscriber.failure,
	}
}
