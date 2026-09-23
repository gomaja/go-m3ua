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
	// report, together with the partition knowledge that resulted from it.
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
	// States is the partition's retained knowledge after this event, owned by
	// the caller.
	States []SSNMDestinationKnowledge
	// Reason is a bounded diagnostic for invalidation and resource loss.
	Reason string
	// ContinuityLost is true on SSNMContinuityLostEvent, and on nothing else.
	ContinuityLost bool
}

func (e SSNMEvent) clone() SSNMEvent {
	e.Report = e.Report.clone()
	if e.States != nil {
		states := make([]SSNMDestinationKnowledge, len(e.States))
		for index, state := range e.States {
			state.Availability.Scope = state.Availability.Scope.clone()
			state.Congestion.Scope = state.Congestion.Scope.clone()
			states[index] = state
		}
		e.States = states
	}
	return e
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
		!reserveSSNMEventBytes(&remaining, len(event.States), ssnmEventStateBytes) ||
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
	for _, state := range event.States {
		if !reserveSSNMEventBytes(&remaining, len(state.Availability.Scope.RoutingContexts), ssnmRoutingContextBytes) ||
			!reserveSSNMEventBytes(&remaining, len(state.Congestion.Scope.RoutingContexts), ssnmRoutingContextBytes) {
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

	mu             sync.Mutex
	queue          []queuedSSNMEvent
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
	s.queue = append(s.queue, queuedSSNMEvent{event: event.clone(), bytes: accountedBytes})
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
		s.queue = s.queue[1:]
		s.queuedBytes -= queued.bytes
		if len(s.queue) == 0 {
			s.queue = nil
		}
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
