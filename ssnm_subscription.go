// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// SSNMEventKind names what one subscription delta reports.
type SSNMEventKind uint8

const (
	// SSNMReportEvent carries one locally validated RFC 4666 Section 3.4
	// report, together with the destination knowledge it wrote.
	SSNMReportEvent SSNMEventKind = iota + 1
	// SSNMBindingAdmittedEvent reports an Association admitted to a partition,
	// active or pending activation.
	SSNMBindingAdmittedEvent
	// SSNMBindingActivatedEvent reports a pending binding completed by the
	// activation acknowledgment.
	SSNMBindingActivatedEvent
	// SSNMBindingRetiredEvent reports one binding withdrawn from a partition
	// that still has others; its knowledge is preserved by the siblings.
	SSNMBindingRetiredEvent
	// SSNMPartitionRetiredEvent reports the last binding of a partition
	// withdrawn. The partition and its knowledge were discarded atomically.
	SSNMPartitionRetiredEvent
	// SSNMPartitionInvalidatedEvent reports one partition's knowledge
	// conservatively discarded while its bindings remain.
	SSNMPartitionInvalidatedEvent
	// SSNMResourceLossEvent reports a local resource bound refusing otherwise
	// valid work. The association it arrived on is unaffected.
	SSNMResourceLossEvent
	// SSNMContinuityLostEvent reports that this subscription's queue
	// overflowed. Deltas are suppressed until Resync succeeds.
	SSNMContinuityLostEvent
)

func (k SSNMEventKind) String() string {
	switch k {
	case SSNMReportEvent:
		return "report"
	case SSNMBindingAdmittedEvent:
		return "binding-admitted"
	case SSNMBindingActivatedEvent:
		return "binding-activated"
	case SSNMBindingRetiredEvent:
		return "binding-retired"
	case SSNMPartitionRetiredEvent:
		return "partition-retired"
	case SSNMPartitionInvalidatedEvent:
		return "partition-invalidated"
	case SSNMResourceLossEvent:
		return "resource-loss"
	case SSNMContinuityLostEvent:
		return "continuity-lost"
	default:
		return "unknown"
	}
}

// SSNMEvent is one owned delta from an SSNM subscription.
//
// Revision is strictly greater than the revision of the snapshot the
// subscription started from, and strictly increases across the events one
// subscription delivers.
//
// An event carries what it changed, never the rest of its partition. Applied
// in order to the snapshot the subscription started from (SubscribeSSNM, or
// the latest successful Resync), the events reproduce the store's
// SSNMPartitionKnowledge exactly:
//
//   - SSNMReportEvent: for each entry of Updated, replace the partition's
//     entry for that Destination. No other destination changed.
//   - SSNMBindingAdmittedEvent: set Binding in Partition, creating the
//     partition with this Epoch and no destinations if it is not held.
//   - SSNMBindingActivatedEvent: mark Binding no longer pending.
//   - SSNMBindingRetiredEvent: remove Binding. Destinations are unchanged.
//   - SSNMPartitionRetiredEvent: remove Partition with its bindings and
//     destinations.
//   - SSNMPartitionInvalidatedEvent: remove every destination of Partition.
//     Its bindings are unchanged.
//   - SSNMResourceLossEvent: no knowledge changed.
//   - SSNMContinuityLostEvent: the view can no longer be patched; replace it
//     with the snapshot Resync returns.
//
// A report never removes one destination's knowledge: DAVA and a level-zero
// SCON are retained as statements about the destination, and a resource bound
// refuses a report rather than evicting what is held. Knowledge leaves the
// store only a whole partition at a time, which the event kind signals.
type SSNMEvent struct {
	Kind      SSNMEventKind
	Revision  uint64
	Partition SSNMPartition
	Epoch     uint64
	// Report is set for SSNMReportEvent only; ReportSet says so explicitly
	// because the zero report is indistinguishable from an absent one.
	Report    SSNMReport
	ReportSet bool
	// Binding is set for the binding lifecycle events.
	Binding SSNMBinding
	// Updated is set for SSNMReportEvent only, and only when the report was
	// retained: the destinations the report wrote, each once, in point-code
	// then mask order and keyed exactly as SSNMPartitionKnowledge.Destinations.
	// Each entry carries both dimensions as retained after this event: the one
	// the report wrote, with this Revision, and the other one unchanged, so an
	// entry replaces the consumer's entry for its Destination whole. It is
	// empty for a report retained by nobody -- an unbound partition, DUPU,
	// DAUD, or a peer-reported SCON -- and for every other kind: binding
	// lifecycle changes no destination knowledge, and a retired or
	// invalidated partition is discarded whole. Owned by the caller.
	Updated []SSNMDestinationKnowledge
	// Reason is a bounded diagnostic for invalidation and resource loss.
	Reason string
	// ContinuityLost is true on SSNMContinuityLostEvent, and on nothing else.
	ContinuityLost bool
}

// clone returns an event that shares no storage with the receiver.
//
// Every Routing Context list of the copy lives in one array, each capped at
// its own length so that appending to one reallocates instead of writing into
// the next. A one-destination report therefore costs three allocations per
// subscriber however many lists it carries.
func (e SSNMEvent) clone() SSNMEvent {
	contexts := len(e.Report.Scope.RoutingContexts)
	for _, update := range e.Updated {
		contexts += len(update.Availability.Scope.RoutingContexts) + len(update.Congestion.Scope.RoutingContexts)
	}
	var arena ssnmRoutingContextArena
	if contexts > 0 {
		arena.free = make([]uint32, contexts)
	}
	e.Report.Scope.RoutingContexts = arena.own(e.Report.Scope.RoutingContexts)
	if e.Report.Destinations != nil {
		e.Report.Destinations = append([]PointCodeRange(nil), e.Report.Destinations...)
	}
	if e.Updated != nil {
		updated := make([]SSNMDestinationKnowledge, len(e.Updated))
		for index, update := range e.Updated {
			update.Availability.Scope.RoutingContexts = arena.own(update.Availability.Scope.RoutingContexts)
			update.Congestion.Scope.RoutingContexts = arena.own(update.Congestion.Scope.RoutingContexts)
			updated[index] = update
		}
		e.Updated = updated
	}
	return e
}

// ssnmRoutingContextArena hands out owned Routing Context lists from one
// preallocated array. An empty list is returned as nil, as WireScope.clone
// returns it.
type ssnmRoutingContextArena struct {
	free []uint32
}

func (a *ssnmRoutingContextArena) own(values []uint32) []uint32 {
	if len(values) == 0 {
		return nil
	}
	owned := a.free[:len(values):len(values)]
	copy(owned, values)
	a.free = a.free[len(values):]
	return owned
}

const (
	ssnmEventBaseBytes        = 512
	ssnmEventDestinationBytes = 8
	ssnmEventStateBytes       = 256
)

func ssnmEventAccountedBytes(event SSNMEvent, limit int) (int, bool) {
	remaining := limit
	if !reserveSSNMEventBytes(&remaining, 1, ssnmEventBaseBytes) ||
		!reserveSSNMEventBytes(&remaining, len(event.Report.Destinations), ssnmEventDestinationBytes) ||
		!reserveSSNMEventBytes(&remaining, len(event.Updated), ssnmEventStateBytes) ||
		!reserveSSNMEventBytes(&remaining, len(event.Report.Scope.RoutingContexts), ssnmRoutingContextBytes) {
		return 0, false
	}
	for _, value := range []string{
		event.Reason,
		string(event.Partition.SignallingGateway),
		string(event.Partition.ApplicationServer),
		string(event.Report.Partition.SignallingGateway),
		string(event.Report.Partition.ApplicationServer),
	} {
		if !reserveSSNMEventBytes(&remaining, len(value), 1) {
			return 0, false
		}
	}
	for _, update := range event.Updated {
		if !reserveSSNMEventBytes(&remaining, len(update.Availability.Scope.RoutingContexts), ssnmRoutingContextBytes) ||
			!reserveSSNMEventBytes(&remaining, len(update.Congestion.Scope.RoutingContexts), ssnmRoutingContextBytes) {
			return 0, false
		}
	}
	return limit - remaining, true
}

func reserveSSNMEventBytes(remaining *int, count, width int) bool {
	if *remaining < 0 || count < 0 || width <= 0 || count > *remaining/width {
		return false
	}
	*remaining -= count * width
	return true
}

// ssnmSpareQueueSlots bounds the drained queue array a subscription keeps
// for reuse.
const ssnmSpareQueueSlots = 8

type queuedSSNMEvent struct {
	event SSNMEvent
	bytes int
}

// Subscription errors.
var (
	// ErrSSNMSubscriptionClosed reports use of a subscription after Close, or
	// after the Endpoint that owned it closed. It is terminal: no later call
	// on the same subscription succeeds.
	ErrSSNMSubscriptionClosed = errors.New("SSNM subscription closed")
	// ErrSSNMSubscriptionBusy reports a concurrent Next or Resync on one
	// subscription. One subscription is a single consumer; two consumers would
	// divide its deltas between them and neither would hold a complete stream.
	ErrSSNMSubscriptionBusy = errors.New("SSNM subscription is already in use")
	// ErrSSNMSubscriberLimit reports that the configured concurrent
	// subscription limit is reached.
	ErrSSNMSubscriberLimit = fmt.Errorf("%w: SSNM subscriber limit exceeded", ErrSSNMResourceLoss)
)

// SSNMSubscription is one consumer's delta stream over the SSNM state store.
//
// It is a single-consumer stream. Next and Resync reject each other and
// themselves while one is running; Close may be called at any time, from any
// goroutine, and wakes a blocked Next.
type SSNMSubscription struct {
	state *ssnmState

	// busy admits one Next or Resync at a time. It is a rejection rather than
	// a wait: a second consumer that queued would silently take deltas the
	// first consumer needed.
	busy chan struct{}

	mu    sync.Mutex
	queue []queuedSSNMEvent
	// spare is the array of a queue that was drained, kept for the next event
	// while it is small. A consumer that keeps up then costs no allocation to
	// queue for; the larger array a burst grew is released once drained. Every
	// slot in it has been cleared.
	spare []queuedSSNMEvent
	// queueArrayCap is the whole capacity of the array queue lives in, from
	// its first slot. cap(queue) cannot stand in for it: take advances queue
	// through the array, so a drained queue reports only the slots after its
	// last event, and a burst that exactly filled a large array would look
	// small enough to keep.
	queueArrayCap  int
	queuedBytes    int
	closed         bool
	terminal       error
	continuityLost bool
	// pendingLoss is the not-yet-delivered continuity marker. It is delivered
	// once, after whatever was already queued.
	pendingLoss bool
	limit       int
	byteLimit   int
	wake        chan struct{}
}

func newSSNMSubscription(state *ssnmState, limit, byteLimit int) *SSNMSubscription {
	return &SSNMSubscription{
		state:     state,
		busy:      make(chan struct{}, 1),
		limit:     limit,
		byteLimit: byteLimit,
		wake:      make(chan struct{}, 1),
	}
}

// publishLocked hands one event to every subscriber. It runs under the store
// lock, so the snapshot a subscription started from and the deltas that follow
// it cannot interleave.
func (s *ssnmState) publishLocked(event SSNMEvent) {
	for subscription := range s.subscribers {
		subscription.enqueue(event)
	}
}

func (s *SSNMSubscription) enqueue(event SSNMEvent) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	// A subscription that has already lost continuity is not told about
	// individual deltas it can no longer place: the hole makes the sequence
	// meaningless until Resync replaces it with a snapshot.
	if s.continuityLost {
		s.mu.Unlock()
		return
	}
	accountedBytes, fits := 0, false
	if len(s.queue) < s.limit {
		accountedBytes, fits = ssnmEventAccountedBytes(event, s.byteLimit-s.queuedBytes)
	}
	if !fits {
		s.continuityLost = true
		s.pendingLoss = true
		s.mu.Unlock()
		s.signal()
		return
	}
	if len(s.queue) == 0 && s.spare != nil {
		s.queue, s.spare = s.spare, nil
	}
	previousCap := cap(s.queue)
	s.queue = append(s.queue, queuedSSNMEvent{event: event.clone(), bytes: accountedBytes})
	if cap(s.queue) != previousCap {
		// append moved the queue to a new array, and the queue starts at its
		// first slot.
		s.queueArrayCap = cap(s.queue)
	}
	s.queuedBytes += accountedBytes
	s.mu.Unlock()
	s.signal()
}

func (s *SSNMSubscription) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *SSNMSubscription) acquire() bool {
	select {
	case s.busy <- struct{}{}:
		return true
	default:
		return false
	}
}

func (s *SSNMSubscription) release() {
	select {
	case <-s.busy:
	default:
	}
}

// Next returns the subscription's next delta, blocking until one is available,
// the context ends, or the subscription reaches its terminal state.
//
// A concurrent Next or Resync is rejected with ErrSSNMSubscriptionBusy rather
// than queued.
func (s *SSNMSubscription) Next(ctx context.Context) (SSNMEvent, error) {
	if s == nil {
		return SSNMEvent{}, ErrSSNMSubscriptionClosed
	}
	if !s.acquire() {
		return SSNMEvent{}, ErrSSNMSubscriptionBusy
	}
	defer s.release()
	for {
		event, ready, err := s.take()
		if ready {
			return event, err
		}
		select {
		case <-ctx.Done():
			return SSNMEvent{}, ctx.Err()
		case <-s.wake:
		}
	}
}

// take removes one deliverable event without blocking. The second result
// reports whether there was anything to deliver.
func (s *SSNMSubscription) take() (SSNMEvent, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.queue) > 0 {
		queued := s.queue[0]
		s.queue[0] = queuedSSNMEvent{}
		s.queuedBytes -= queued.bytes
		if len(s.queue) > 1 {
			s.queue = s.queue[1:]
			return queued.event, true, nil
		}
		if s.queueArrayCap <= ssnmSpareQueueSlots {
			s.spare = s.queue[:0]
		}
		s.queue = nil
		return queued.event, true, nil
	}
	// The terminal state is delivered only once the queue that preceded it has
	// been drained, so closing does not discard deltas already accepted.
	if s.closed {
		return SSNMEvent{}, true, s.terminal
	}
	if s.pendingLoss {
		s.pendingLoss = false
		return SSNMEvent{
			Kind:           SSNMContinuityLostEvent,
			ContinuityLost: true,
		}, true, nil
	}
	return SSNMEvent{}, false, nil
}

// Resync returns a fresh owned snapshot and resumes the delta stream from it.
//
// It is the only thing that clears continuity loss, and it clears it only on
// success. Queued deltas are dropped because the snapshot already contains
// them: keeping both would replay state the caller has just been given.
func (s *SSNMSubscription) Resync() (SSNMSnapshot, error) {
	if s == nil {
		return SSNMSnapshot{}, ErrSSNMSubscriptionClosed
	}
	if !s.acquire() {
		return SSNMSnapshot{}, ErrSSNMSubscriptionBusy
	}
	defer s.release()

	s.mu.Lock()
	if s.closed {
		terminal := s.terminal
		s.mu.Unlock()
		return SSNMSnapshot{}, terminal
	}
	s.mu.Unlock()

	state := s.state
	if state == nil {
		return SSNMSnapshot{}, ErrSSNMSubscriptionClosed
	}
	state.mu.Lock()
	if state.closed {
		state.mu.Unlock()
		s.terminate(ErrEndpointClosed)
		return SSNMSnapshot{}, ErrEndpointClosed
	}
	// The snapshot and the queue reset happen together under the store lock,
	// so a report concurrent with Resync is either inside the snapshot or
	// queued after it, never both and never neither.
	snapshot := state.snapshotLocked()
	s.mu.Lock()
	if s.closed {
		terminal := s.terminal
		s.mu.Unlock()
		state.mu.Unlock()
		return SSNMSnapshot{}, terminal
	}
	clear(s.queue)
	s.queue = nil
	s.queuedBytes = 0
	s.continuityLost = false
	s.pendingLoss = false
	s.mu.Unlock()
	state.mu.Unlock()
	return snapshot, nil
}

// Close releases the subscription. It is idempotent, safe to call from any
// goroutine, and wakes a blocked Next.
func (s *SSNMSubscription) Close() error {
	if s == nil {
		return nil
	}
	if s.state != nil {
		s.state.mu.Lock()
		delete(s.state.subscribers, s)
		s.state.mu.Unlock()
	}
	s.terminate(ErrSSNMSubscriptionClosed)
	return nil
}

func (s *SSNMSubscription) terminate(cause error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.terminal = cause
	s.mu.Unlock()
	s.signal()
}

// SubscribeSSNM returns an owned snapshot of this Endpoint's route-independent
// SSNM knowledge together with a subscription delivering every later change.
//
// The two are atomic with respect to each other: the snapshot is taken and the
// subscription registered in one critical section, so no report is both in the
// snapshot and in the stream, and none is in neither.
func (e *Endpoint) SubscribeSSNM() (SSNMSnapshot, *SSNMSubscription, error) {
	if e == nil {
		return SSNMSnapshot{}, nil, ErrEndpointClosed
	}
	state := e.ssnm
	if state == nil {
		return SSNMSnapshot{}, nil, ErrEndpointClosed
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.closed {
		return SSNMSnapshot{}, nil, ErrEndpointClosed
	}
	if len(state.subscribers) >= state.limits.MaxSubscribers {
		return SSNMSnapshot{}, nil, fmt.Errorf("%w: %d subscriptions open, limit %d",
			ErrSSNMSubscriberLimit, len(state.subscribers), state.limits.MaxSubscribers)
	}
	subscription := newSSNMSubscription(state, state.limits.SubscriptionQueueSize, state.limits.SubscriptionQueueBytes)
	state.subscribers[subscription] = struct{}{}
	return state.snapshotLocked(), subscription, nil
}

// SSNMKnowledge returns an owned snapshot of this Endpoint's route-independent
// SSNM knowledge without opening a subscription.
func (e *Endpoint) SSNMKnowledge() SSNMSnapshot {
	if e == nil {
		return SSNMSnapshot{}
	}
	return e.ssnm.snapshot()
}
