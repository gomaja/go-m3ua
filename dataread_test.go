// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// deliver hands one DATA to the association exactly as the dispatcher would,
// on the arrival stream given.
func deliver(t *testing.T, conn *Association, stream uint16, data *messages.Data) {
	t.Helper()
	conn.recvStream.Store(uint32(stream))
	conn.handleData(context.Background(), data, nil)
}

func inboundData(routingContext uint32, payload string) *messages.Data {
	return messages.NewData(
		params.NewNetworkAppearance(7),
		params.NewRoutingContext(routingContext),
		params.NewProtocolData(0x111111, 0x222222, params.ServiceIndSCCP, 0, 0, 1, []byte(payload)),
		nil,
	)
}

// Acceptance bullet 5: cancelling a read is a caller-scoped operation. It ends
// that one read and nothing else — the association stays up and the queue keeps
// everything already in it and everything that arrives later.
func TestReadDataCancellationLeavesTheAssociationAndQueueIntact(t *testing.T) {
	conn, _ := newDataWriteAssociation(t, 1)

	ctx, cancel := context.WithCancel(context.Background())
	readReturned := make(chan error, 1)
	go func() {
		_, err := conn.ReadData(ctx)
		readReturned <- err
	}()
	// The read is parked on an empty queue.
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-readReturned:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled ReadData returned %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelling the context did not end the read")
	}

	select {
	case <-conn.Done():
		t.Fatal("cancelling a read closed the association")
	default:
	}
	if state := conn.State(); state != StateASPActive {
		t.Errorf("state after a cancelled read = %v, want %v", state, StateASPActive)
	}
	if err := conn.Err(); err != nil {
		t.Errorf("Err() after a cancelled read = %v, want nil", err)
	}

	// A read made with an already-cancelled context reports the cancellation
	// and takes nothing, every time: a shutdown path that reads once more must
	// not swallow a message nobody will see. Repeated, because a queued message
	// and a cancelled context are both ready at once — deciding between them by
	// a select would be right only half the time.
	deliver(t, conn, 1, inboundData(1, "after cancellation"))
	for round := 0; round < 50; round++ {
		if _, err := conn.ReadData(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("round %d: ReadData with an already-cancelled context = %v, want context.Canceled",
				round, err)
		}
		if queued := len(conn.dataChan); queued != 1 {
			t.Fatalf("round %d: %d messages are queued after a cancelled read, want the one delivered",
				round, queued)
		}
	}

	// And it is still there for the next read.
	message, err := conn.ReadData(context.Background())
	if err != nil {
		t.Fatalf("ReadData after a cancelled read: %v", err)
	}
	if got := string(message.ProtocolData.Data); got != "after cancellation" {
		t.Errorf("payload = %q, want %q", got, "after cancellation")
	}
}

// The race the contract is really about: a cancellation that lands at the same
// moment as a delivery must not consume the message. Either the read returns
// it, or the read reports the cancellation and the message is still queued.
func TestReadDataCancellationRaceNeverLosesAMessage(t *testing.T) {
	conn, _ := newDataWriteAssociation(t, 1)

	const rounds = 300
	delivered := 0
	for round := 0; round < rounds; round++ {
		payload := fmt.Sprintf("round-%d", round)
		ctx, cancel := context.WithCancel(context.Background())
		var wait sync.WaitGroup
		wait.Add(2)
		var readErr error
		var message *DataMessage
		go func() {
			defer wait.Done()
			message, readErr = conn.ReadData(ctx)
		}()
		go func() {
			defer wait.Done()
			deliver(t, conn, 1, inboundData(1, payload))
		}()
		cancel()
		wait.Wait()

		switch {
		case readErr == nil:
			if got := string(message.ProtocolData.Data); got != payload {
				t.Fatalf("round %d read %q, want %q", round, got, payload)
			}
			delivered++
		case errors.Is(readErr, context.Canceled):
			// The message must still be queued: take it with a live context.
			queued, err := conn.ReadData(context.Background())
			if err != nil {
				t.Fatalf("round %d: the message was lost with the cancellation: %v", round, err)
			}
			if got := string(queued.ProtocolData.Data); got != payload {
				t.Fatalf("round %d recovered %q, want %q", round, got, payload)
			}
			delivered++
		default:
			t.Fatalf("round %d: ReadData returned %v", round, readErr)
		}
	}
	if delivered != rounds {
		t.Errorf("%d of %d messages survived the cancellation race", delivered, rounds)
	}
}

// Several readers may share one association's queue. Each message goes to
// exactly one of them: none is duplicated and none is dropped.
func TestReadDataDeliversEachMessageToExactlyOneReader(t *testing.T) {
	conn, _ := newDataWriteAssociation(t, 1)
	conn.dataChan = make(chan *DataMessage, 64)

	const readers, messages = 4, 200
	received := make(chan string, messages)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wait sync.WaitGroup
	for reader := 0; reader < readers; reader++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for {
				message, err := conn.ReadData(ctx)
				if err != nil {
					return
				}
				received <- string(message.ProtocolData.Data)
			}
		}()
	}

	for index := 0; index < messages; index++ {
		deliver(t, conn, 1, inboundData(1, fmt.Sprintf("message-%d", index)))
	}

	// A full queue discards, by design: that is the local-congestion path this
	// package reports through DataQueueStats. On a single processor the
	// deliveries outrun the readers and some are dropped, so what this test
	// pins is exactly-once delivery of whatever was accepted, not that nothing
	// was dropped. Demanding all of them back would be testing the scheduler.
	discarded := int(conn.DataQueueStats().Discarded)
	accepted := messages - discarded
	if accepted <= 0 {
		t.Fatalf("every message was discarded (%d of %d); the queue never accepted one", discarded, messages)
	}

	seen := make(map[string]int, accepted)
	for index := 0; index < accepted; index++ {
		select {
		case payload := <-received:
			seen[payload]++
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of the %d accepted messages were delivered (%d discarded)",
				index, accepted, discarded)
		}
	}
	cancel()
	wait.Wait()

	// Nothing may arrive twice, and nothing may arrive that was never sent.
	delivered := 0
	for payload, count := range seen {
		if count != 1 {
			t.Errorf("%q was delivered to %d readers, want exactly 1", payload, count)
		}
		var index int
		if _, err := fmt.Sscanf(payload, "message-%d", &index); err != nil || index < 0 || index >= messages {
			t.Errorf("delivered %q, which was never sent", payload)
		}
		delivered += count
	}
	if delivered != accepted {
		t.Errorf("delivered %d distinct messages, want the %d the queue accepted", delivered, accepted)
	}
	if queued := conn.DataQueueStats().Queued; queued != 0 {
		t.Errorf("%d messages left queued after every reader stopped", queued)
	}
}

// The payload a read returns belongs to the caller: writing to it changes
// nothing the library or another message can see, and no two messages share
// their storage.
func TestReadDataReturnsCallerOwnedData(t *testing.T) {
	conn, _ := newDataWriteAssociation(t, 1, 2)

	deliver(t, conn, 1, inboundData(1, "first payload"))
	deliver(t, conn, 1, inboundData(2, "second payload"))

	first, err := conn.ReadData(context.Background())
	if err != nil {
		t.Fatalf("ReadData: %v", err)
	}
	for index := range first.ProtocolData.Data {
		first.ProtocolData.Data[index] = 'x'
	}
	first.Scope.RoutingContexts[0] = 999

	second, err := conn.ReadData(context.Background())
	if err != nil {
		t.Fatalf("ReadData: %v", err)
	}
	if got := string(second.ProtocolData.Data); got != "second payload" {
		t.Errorf("the second payload reads %q; the first message shared its storage", got)
	}
	if got := second.Scope.RoutingContexts; len(got) != 1 || got[0] != 2 {
		t.Errorf("the second scope reads %v; the first message shared its storage", got)
	}
	if &first.ProtocolData.Data[0] == &second.ProtocolData.Data[0] {
		t.Error("two messages were delivered on the same backing array")
	}
}

// What a read reports about a message: the scope exactly as the peer put it on
// the wire, the Application Server it resolves to, the stream it arrived on,
// the correlation the peer attached, and the association and SCTP epoch that
// carried it.
//
// RFC 4666 Section 3.3.1 keeps Network Appearance and Routing Context "of local
// significance only, coordinated between the SGP and ASP", so the exact wire
// scope and the resolved identity are reported separately.
func TestReadDataReportsTheReceivedScopeStreamAndEpoch(t *testing.T) {
	conn, _ := newDataWriteAssociation(t, 1)
	conn.managementID.Store(11)

	deliver(t, conn, 3, messages.NewData(
		params.NewNetworkAppearance(7),
		params.NewRoutingContext(1),
		params.NewProtocolData(0x111111, 0x222222, params.ServiceIndSCCP, 2, 1, 5, []byte("scoped")),
		params.NewCorrelationID(4242),
	))

	message, err := conn.ReadData(context.Background())
	if err != nil {
		t.Fatalf("ReadData: %v", err)
	}
	if !message.Scope.NetworkAppearanceSet || message.Scope.NetworkAppearance != 7 {
		t.Errorf("wire Network Appearance = %v/%v, want 7/true",
			message.Scope.NetworkAppearance, message.Scope.NetworkAppearanceSet)
	}
	if !message.Scope.RoutingContextSet ||
		len(message.Scope.RoutingContexts) != 1 || message.Scope.RoutingContexts[0] != 1 {
		t.Errorf("wire Routing Contexts = %v/%v, want [1]/true",
			message.Scope.RoutingContexts, message.Scope.RoutingContextSet)
	}
	wantAS := ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true}
	if message.AS != wantAS {
		t.Errorf("resolved Application Server = %+v, want %+v", message.AS, wantAS)
	}
	if message.Stream != 3 {
		t.Errorf("arrival stream = %d, want 3", message.Stream)
	}
	if !message.CorrelationIDSet || message.CorrelationID != 4242 {
		t.Errorf("Correlation ID = %v/%v, want 4242/true", message.CorrelationID, message.CorrelationIDSet)
	}
	if message.Association != conn.ID() {
		t.Errorf("association = %d, want %d", message.Association, conn.ID())
	}
	if message.Epoch != conn.Epoch() {
		t.Errorf("epoch = %d, want %d", message.Epoch, conn.Epoch())
	}
	if message.ProtocolData.MessagePriority != 1 || message.ProtocolData.NetworkIndicator != 2 ||
		message.ProtocolData.SignallingLinkSelection != 5 {
		t.Errorf("routing label = %s, want NI 2, MP 1, SLS 5", message.ProtocolData.String())
	}

	// A peer that omits both parameters is reported as having omitted them.
	deliver(t, conn, 2, messages.NewData(
		nil, nil,
		params.NewProtocolData(0x111111, 0x222222, params.ServiceIndSCCP, 0, 0, 1, []byte("bare")),
		nil,
	))
	bare, err := conn.ReadData(context.Background())
	if err != nil {
		t.Fatalf("ReadData: %v", err)
	}
	if bare.Scope.NetworkAppearanceSet || bare.Scope.RoutingContextSet {
		t.Errorf("an omitted scope was reported as present: %+v", bare.Scope)
	}
	if bare.CorrelationIDSet {
		t.Error("an omitted Correlation ID was reported as present")
	}
	// The single configured flow is what an omitted Routing Context resolves
	// to; the wire scope above still says the peer named nothing.
	if !bare.AS.RoutingContextSet || bare.AS.RoutingContext != 1 {
		t.Errorf("resolved Application Server = %+v, want the one configured flow", bare.AS)
	}
}

// An SCTP restart starts a new epoch, so a message read after it is
// distinguishable from one read before.
func TestReadDataReportsTheEpochTheMessageArrivedIn(t *testing.T) {
	conn, _ := newDataWriteAssociation(t, 1)
	deliver(t, conn, 1, inboundData(1, "before"))
	before, err := conn.ReadData(context.Background())
	if err != nil {
		t.Fatalf("ReadData: %v", err)
	}
	// Epochs are one-based: the first is a generation, not the absence of one.
	if before.Epoch != 1 {
		t.Errorf("epoch of the first message = %d, want 1", before.Epoch)
	}

	// The restart publishes ASP-DOWN, which a hand-built association has no
	// dispatcher to consume, so the recovery the peer would drive is applied
	// here directly.
	conn.handleSCTPRestart()
	select {
	case <-conn.stateChan:
	default:
		t.Fatal("the SCTP restart did not publish a state transition")
	}
	conn.setState(StateASPActive)
	conn.noteRoutingContextsAcked(params.NewRoutingContext(1))

	deliver(t, conn, 1, inboundData(1, "after"))
	after, err := conn.ReadData(context.Background())
	if err != nil {
		t.Fatalf("ReadData: %v", err)
	}
	if after.Epoch != before.Epoch+1 {
		t.Errorf("epoch after an SCTP restart = %d, want %d", after.Epoch, before.Epoch+1)
	}
}

// Acceptance bullet 5, last clause: sustained overload is observable rather
// than silent. RFC 4666 Section 3.4.4 is how the peer is told — "The SCON
// message MAY also be sent from the M3UA layer of an ASP to an M3UA peer,
// indicating that the congestion level of the M3UA layer or the ASP has
// changed" — and the local application is told by the queue's own report.
func TestDataQueueOverloadIsObservable(t *testing.T) {
	conn, _ := newDataWriteAssociation(t, 1)
	conn.dataChan = make(chan *DataMessage, 2)

	if stats := conn.DataQueueStats(); stats.Capacity != 2 || stats.Queued != 0 ||
		stats.Discarded != 0 || stats.Congested {
		t.Fatalf("a fresh queue reports %+v", stats)
	}

	for index := 0; index < 5; index++ {
		deliver(t, conn, 1, inboundData(1, fmt.Sprintf("payload-%d", index)))
	}

	stats := conn.DataQueueStats()
	if stats.Queued != 2 {
		t.Errorf("queued = %d, want 2", stats.Queued)
	}
	if stats.Discarded != 3 {
		t.Errorf("discarded = %d, want 3", stats.Discarded)
	}
	if !stats.Congested {
		t.Error("the queue does not report itself congested while it is discarding")
	}

	// The peer is told once per episode, and the report names the destination
	// whose traffic is being discarded.
	select {
	case err := <-conn.errChan:
		if !errors.Is(err, ErrDataQueueFull) {
			t.Fatalf("reported %v, want ErrDataQueueFull", err)
		}
		var overflow *DataQueueOverflowError
		if !errors.As(err, &overflow) {
			t.Fatalf("error %v does not carry the discarded destination", err)
		}
		if overflow.DestinationPointCode != 0x222222 {
			t.Errorf("congestion named point code %#x, want %#x",
				overflow.DestinationPointCode, 0x222222)
		}
	default:
		t.Fatal("overload was not reported")
	}

	// Draining clears the congestion, and the discard count is cumulative.
	for index := 0; index < 2; index++ {
		if _, err := conn.ReadData(context.Background()); err != nil {
			t.Fatalf("ReadData: %v", err)
		}
	}
	deliver(t, conn, 1, inboundData(1, "recovered"))
	stats = conn.DataQueueStats()
	if stats.Congested {
		t.Error("the queue still reports congestion after draining and accepting a message")
	}
	if stats.Discarded != 3 {
		t.Errorf("discarded = %d after recovery, want the cumulative 3", stats.Discarded)
	}
}

// The read deadline survives the move to a context-scoped read: it still bounds
// a read, still reports os.ErrDeadlineExceeded, and still leaves the
// association usable.
func TestReadDataStillHonoursTheReadDeadline(t *testing.T) {
	conn, _ := newDataWriteAssociation(t, 1)
	if err := conn.SetReadDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	if _, err := conn.ReadData(context.Background()); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("ReadData with an expired deadline = %v, want os.ErrDeadlineExceeded", err)
	}
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		t.Fatalf("SetReadDeadline(zero): %v", err)
	}
	deliver(t, conn, 1, inboundData(1, "after the deadline"))
	message, err := conn.ReadData(context.Background())
	if err != nil {
		t.Fatalf("ReadData after clearing the deadline: %v", err)
	}
	if got := string(message.ProtocolData.Data); got != "after the deadline" {
		t.Errorf("payload = %q", got)
	}
}

// A closed association ends every parked read, which is not cancellation.
func TestReadDataOnAClosedAssociationReportsItAsClosed(t *testing.T) {
	conn, _ := newDataWriteAssociation(t, 1)
	var readErr atomic.Value
	done := make(chan struct{})
	go func() {
		_, err := conn.ReadData(context.Background())
		readErr.Store(err)
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	_ = conn.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("closing the association did not end the parked read")
	}
	if err, _ := readErr.Load().(error); !errors.Is(err, ErrNotEstablished) {
		t.Errorf("ReadData on a closed association = %v, want ErrNotEstablished", err)
	}
}

// lateCancelContext is cancelled after its first Err check: the interleaving in
// which a read has already been admitted and is inside its select when the
// caller cancels. Done is never closed, so the read can only complete through
// the queue — which is exactly the case where a message could be taken and
// then thrown away.
type lateCancelContext struct {
	context.Context
	asked atomic.Bool
	done  chan struct{}
}

func newLateCancelContext() *lateCancelContext {
	return &lateCancelContext{Context: context.Background(), done: make(chan struct{})}
}

func (c *lateCancelContext) Done() <-chan struct{} { return c.done }

func (c *lateCancelContext) Err() error {
	if c.asked.Swap(true) {
		return context.Canceled
	}
	return nil
}

// A message that has left the queue belongs to the caller of the read that took
// it, whatever happens to that read's context afterwards. Checking cancellation
// after taking one would discard it: it is no longer in the queue for the next
// read to find, and no one else can be given it.
func TestReadDataNeverDiscardsAMessageItHasTaken(t *testing.T) {
	conn, _ := newDataWriteAssociation(t, 1)
	deliver(t, conn, 1, inboundData(1, "taken"))

	message, err := conn.ReadData(newLateCancelContext())
	if err != nil {
		t.Fatalf("ReadData returned %v after taking a message from the queue", err)
	}
	if got := string(message.ProtocolData.Data); got != "taken" {
		t.Errorf("payload = %q, want %q", got, "taken")
	}
	if queued := len(conn.dataChan); queued != 0 {
		t.Errorf("%d messages are still queued, want 0", queued)
	}
}
