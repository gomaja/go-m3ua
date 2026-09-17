package m3ua

import (
	"context"
	"errors"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func drainSSNMEvent(t *testing.T, subscription *SSNMSubscription) (SSNMEvent, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return subscription.Next(ctx)
}

// newSGPSSNMFixture returns an SGP Endpoint and one active Association, which
// is what an ASP-originated SCON or DAUD arrives on.
func newSGPSSNMFixture(t *testing.T) (*Endpoint, *Association) {
	t.Helper()
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	association, _ := newTestConnWithContexts(t, StateASPActive, RoleSGP, 1)
	association.cfg.NetworkAppearance = params.NewNetworkAppearance(7)
	if !endpoint.trackAssociation(association) {
		t.Fatal("failed to attach SGP Association")
	}
	association.noteRoutingContextsActive([]uint32{1})
	t.Cleanup(func() { _ = association.Close() })
	return endpoint, association
}

// Bullet: snapshot and delta do not race.
//
// The snapshot is taken and the subscriber registered in one critical section,
// so each report is either inside the snapshot or in the stream that follows
// it, never both and never neither.
func TestSubscribeSSNMSnapshotAndDeltasCoverEveryReportExactlyOnce(t *testing.T) {
	const reports = 200
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), nil)
	association := attachSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}, 7, 1)

	started := make(chan struct{})
	var producer sync.WaitGroup
	producer.Add(1)
	go func() {
		defer producer.Done()
		close(started)
		for index := range reports {
			sendDUNA(t, association, 7, 1, 0x200000+uint32(index))
		}
	}()
	<-started

	snapshot, subscription, err := endpoint.SubscribeSSNM()
	if err != nil {
		t.Fatalf("SubscribeSSNM: %v", err)
	}
	defer func() { _ = subscription.Close() }()

	seen := make(map[uint32]string, reports)
	for _, knowledge := range snapshot.Partitions {
		for _, destination := range knowledge.Destinations {
			seen[destination.Destination.PointCode] = "snapshot"
		}
	}
	for len(seen) < reports {
		event, err := drainSSNMEvent(t, subscription)
		if err != nil {
			t.Fatalf("Next after %d of %d reports: %v", len(seen), reports, err)
		}
		if event.Kind != SSNMReportEvent {
			continue
		}
		if event.Revision <= snapshot.Revision {
			t.Fatalf("delta revision %d is not after the snapshot revision %d",
				event.Revision, snapshot.Revision)
		}
		for _, destination := range event.Report.Destinations {
			if where, duplicate := seen[destination.PointCode]; duplicate {
				t.Fatalf("point code %#x delivered twice: first in the %s", destination.PointCode, where)
			}
			seen[destination.PointCode] = "stream"
		}
	}
	producer.Wait()
	for index := range reports {
		if _, covered := seen[0x200000+uint32(index)]; !covered {
			t.Fatalf("point code %#x reached neither the snapshot nor the stream", 0x200000+uint32(index))
		}
	}
}

// The sharper form of the same property. The store revision advances once per
// retained report, so a subscription opened at revision R must be handed R+1
// next. A report that slipped between the snapshot and the registration would
// leave a gap there instead.
func TestSubscribeSSNMLosesNoReportToAConcurrentReporter(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), nil)
	association := attachSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}, 7, 1)

	stop := make(chan struct{})
	var reporter sync.WaitGroup
	reporter.Add(1)
	go func() {
		defer reporter.Done()
		for index := 0; ; index++ {
			select {
			case <-stop:
				return
			default:
			}
			if err := association.handleDestinationUnavailable(messages.NewDestinationUnavailable(
				params.NewNetworkAppearance(7),
				params.NewRoutingContext(1),
				params.NewAffectedPointCode(0x700000+uint32(index%64)),
				nil,
			)); err != nil {
				return
			}
		}
	}()
	defer func() {
		close(stop)
		reporter.Wait()
	}()

	for round := range 300 {
		snapshot, subscription, err := endpoint.SubscribeSSNM()
		if err != nil {
			t.Fatalf("round %d: SubscribeSSNM: %v", round, err)
		}
		event, err := drainSSNMEvent(t, subscription)
		_ = subscription.Close()
		if err != nil {
			t.Fatalf("round %d: Next: %v", round, err)
		}
		if event.Revision != snapshot.Revision+1 {
			t.Fatalf("round %d: first delta is revision %d after a snapshot at %d; "+
				"a report fell between the snapshot and the subscription",
				round, event.Revision, snapshot.Revision)
		}
	}
}

// A deliberate control for the test above: one goroutine reporting while
// another snapshots, with the store's lock removed. It exists to fail, so it
// is skipped by default.
//
// Run it with M3UA_SSNM_RACE_CONTROL=1 under -race before trusting a clean run
// of the concurrency tests here: a race detector that is not firing reports
// correct code and broken code identically.
func TestSSNMRaceDetectorControl(t *testing.T) {
	if os.Getenv("M3UA_SSNM_RACE_CONTROL") != "1" {
		t.Skip("control for the race detector; set M3UA_SSNM_RACE_CONTROL=1 to run it")
	}
	// The shape of ssnmState without its mutex: a reporter writing retained
	// state while a subscriber snapshots it.
	unguarded := make(map[uint32]DestinationState)
	var reporters sync.WaitGroup
	reporters.Add(2)
	go func() {
		defer reporters.Done()
		for index := range 1000 {
			unguarded[uint32(index)] = DestinationUnavailable
		}
	}()
	go func() {
		defer reporters.Done()
		for range 1000 {
			snapshot := make(map[uint32]DestinationState, len(unguarded))
			for pointCode, state := range unguarded {
				snapshot[pointCode] = state
			}
		}
	}()
	reporters.Wait()
	t.Fatal("the control completed without the race detector firing")
}

// Bullet: independent subscribers.
func TestSSNMSubscribersAreIndependent(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), nil)
	association := attachSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}, 7, 1)

	_, first, err := endpoint.SubscribeSSNM()
	if err != nil {
		t.Fatalf("first SubscribeSSNM: %v", err)
	}
	defer func() { _ = first.Close() }()
	_, second, err := endpoint.SubscribeSSNM()
	if err != nil {
		t.Fatalf("second SubscribeSSNM: %v", err)
	}
	defer func() { _ = second.Close() }()

	sendDUNA(t, association, 7, 1, 0x123456)
	for name, subscription := range map[string]*SSNMSubscription{"first": first, "second": second} {
		event, err := drainSSNMEvent(t, subscription)
		if err != nil {
			t.Fatalf("%s subscriber: %v", name, err)
		}
		if !event.ReportSet || event.Report.Kind != SSNMDestinationUnavailableReport {
			t.Fatalf("%s subscriber received %+v, want the DUNA report", name, event)
		}
	}

	// Closing one leaves the other running.
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	sendDAVA(t, association, 7, 1, 0x123456)
	event, err := drainSSNMEvent(t, second)
	if err != nil {
		t.Fatalf("surviving subscriber: %v", err)
	}
	if event.Report.Kind != SSNMDestinationAvailableReport {
		t.Fatalf("surviving subscriber received %+v, want the DAVA report", event)
	}
}

// Bullet: concurrent Next and Resync rejection.
func TestSSNMSubscriptionRejectsConcurrentConsumers(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), nil)
	_, subscription, err := endpoint.SubscribeSSNM()
	if err != nil {
		t.Fatalf("SubscribeSSNM: %v", err)
	}
	defer func() { _ = subscription.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	blocked := make(chan struct{})
	go func() {
		defer close(blocked)
		_, _ = subscription.Next(ctx)
	}()

	// Wait for the blocked Next to claim the subscription. Probing with
	// another Next would itself claim it whenever this goroutine has not run
	// yet, and then block with nothing to deliver.
	deadline := time.Now().Add(2 * time.Second)
	for len(subscription.busy) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("Next did not claim the subscription")
		}
		runtime.Gosched()
	}

	// The probe is bounded: a second consumer that was admitted rather than
	// rejected would block on an empty stream, and a hung suite reports the
	// fault far less clearly than a failed assertion.
	probe, stopProbe := context.WithTimeout(context.Background(), 2*time.Second)
	defer stopProbe()
	if _, err := subscription.Next(probe); !errors.Is(err, ErrSSNMSubscriptionBusy) {
		t.Fatalf("concurrent Next: error = %v, want ErrSSNMSubscriptionBusy", err)
	}
	if _, err := subscription.Resync(); !errors.Is(err, ErrSSNMSubscriptionBusy) {
		t.Fatalf("Resync during Next: error = %v, want ErrSSNMSubscriptionBusy", err)
	}
	cancel()
	select {
	case <-blocked:
	case <-time.After(2 * time.Second):
		t.Fatal("the blocked Next did not return when its context ended")
	}

	// The rejection is not sticky: the subscription is usable again once the
	// consumer that held it has finished.
	if _, err := subscription.Resync(); err != nil {
		t.Fatalf("Resync after the first consumer returned: %v", err)
	}
}

// Resync rejects a concurrent Resync for the same reason Next does.
func TestSSNMSubscriptionRejectsConcurrentResync(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), nil)
	_, subscription, err := endpoint.SubscribeSSNM()
	if err != nil {
		t.Fatalf("SubscribeSSNM: %v", err)
	}
	defer func() { _ = subscription.Close() }()

	// Hold the store lock so the in-flight Resync cannot finish.
	endpoint.ssnm.mu.Lock()
	started := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		close(started)
		_, _ = subscription.Resync()
	}()
	<-started
	deadline := time.Now().Add(2 * time.Second)
	for len(subscription.busy) == 0 {
		if time.Now().After(deadline) {
			endpoint.ssnm.mu.Unlock()
			t.Fatal("Resync did not claim the subscription")
		}
		runtime.Gosched()
	}
	_, err = subscription.Resync()
	endpoint.ssnm.mu.Unlock()
	<-finished
	if !errors.Is(err, ErrSSNMSubscriptionBusy) {
		t.Fatalf("concurrent Resync: error = %v, want ErrSSNMSubscriptionBusy", err)
	}
}

// Bullet: close wakeup.
func TestSSNMSubscriptionCloseWakesBlockedNext(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), nil)
	_, subscription, err := endpoint.SubscribeSSNM()
	if err != nil {
		t.Fatalf("SubscribeSSNM: %v", err)
	}

	result := make(chan error, 1)
	entered := make(chan struct{})
	go func() {
		close(entered)
		_, err := subscription.Next(context.Background())
		result <- err
	}()
	<-entered
	time.Sleep(20 * time.Millisecond)
	if err := subscription.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, ErrSSNMSubscriptionClosed) {
			t.Fatalf("blocked Next woke with %v, want ErrSSNMSubscriptionClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not wake the blocked Next")
	}
}

// The closed state is terminal: no later call on the subscription succeeds,
// and Close itself stays safe to repeat.
func TestSSNMSubscriptionIsTerminalAfterClose(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), nil)
	association := attachSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}, 7, 1)
	_, subscription, err := endpoint.SubscribeSSNM()
	if err != nil {
		t.Fatalf("SubscribeSSNM: %v", err)
	}
	if err := subscription.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := subscription.Close(); err != nil {
		t.Fatalf("repeated Close: %v", err)
	}
	sendDUNA(t, association, 7, 1, 0x123456)
	for attempt := range 3 {
		if _, err := subscription.Next(context.Background()); !errors.Is(err, ErrSSNMSubscriptionClosed) {
			t.Fatalf("Next %d after Close: error = %v, want ErrSSNMSubscriptionClosed", attempt, err)
		}
		if _, err := subscription.Resync(); !errors.Is(err, ErrSSNMSubscriptionClosed) {
			t.Fatalf("Resync %d after Close: error = %v, want ErrSSNMSubscriptionClosed", attempt, err)
		}
	}
}

// Closing the Endpoint moves every open subscription to its terminal state.
func TestSSNMEndpointCloseTerminatesSubscriptions(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), nil)
	_, subscription, err := endpoint.SubscribeSSNM()
	if err != nil {
		t.Fatalf("SubscribeSSNM: %v", err)
	}
	result := make(chan error, 1)
	entered := make(chan struct{})
	go func() {
		close(entered)
		_, err := subscription.Next(context.Background())
		result <- err
	}()
	<-entered
	time.Sleep(20 * time.Millisecond)
	if err := endpoint.Close(); err != nil {
		t.Fatalf("Endpoint.Close: %v", err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, ErrEndpointClosed) {
			t.Fatalf("blocked Next woke with %v, want ErrEndpointClosed", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Endpoint.Close did not wake the blocked Next")
	}
	if _, _, err := endpoint.SubscribeSSNM(); !errors.Is(err, ErrEndpointClosed) {
		t.Fatalf("SubscribeSSNM on a closed Endpoint: error = %v, want ErrEndpointClosed", err)
	}
}

// Bullet: owned copies. Nothing a caller is handed shares storage with the
// store, so mutating it cannot reach another reader.
func TestSSNMSnapshotAndEventsAreOwnedByTheCaller(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), nil)
	association := attachSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}, 7, 1)
	_, subscription, err := endpoint.SubscribeSSNM()
	if err != nil {
		t.Fatalf("SubscribeSSNM: %v", err)
	}
	defer func() { _ = subscription.Close() }()
	sendDUNA(t, association, 7, 1, 0x123456)

	snapshot := endpoint.SSNMKnowledge()
	knowledge := ssnmPartitionKnowledge(t, snapshot, canonicalSSNMPartition("sg-a", "as-core"))
	knowledge.Destinations[0].Availability.State = DestinationRestricted
	knowledge.Destinations[0].Availability.Scope.RoutingContexts[0] = 999
	knowledge.Bindings[0].Association = 0

	event, err := drainSSNMEvent(t, subscription)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	event.Report.Destinations[0].PointCode = 0
	event.Report.Scope.RoutingContexts[0] = 888
	event.States[0].Availability.State = DestinationAvailable

	fresh := ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(), canonicalSSNMPartition("sg-a", "as-core"))
	destination := ssnmDestination(t, fresh, 0x123456, 0)
	if destination.Availability.State != DestinationUnavailable {
		t.Fatalf("a caller's mutation reached the store: %v", destination.Availability.State)
	}
	if destination.Availability.Scope.RoutingContexts[0] != 1 {
		t.Fatalf("a caller's mutation reached the retained wire scope: %+v",
			destination.Availability.Scope)
	}
	if fresh.Bindings[0].Association != association.ID() {
		t.Fatalf("a caller's mutation reached the bindings: %+v", fresh.Bindings)
	}
}

// Two subscribers receive two copies, not two views of one. Events are fanned
// out to every subscriber, so sharing their storage would let one consumer
// rewrite what another is about to read.
func TestSSNMEventsAreNotSharedBetweenSubscribers(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), nil)
	association := attachSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}, 7, 1)
	first := mustSubscribeSSNM(t, endpoint)
	second := mustSubscribeSSNM(t, endpoint)

	sendDUNA(t, association, 7, 1, 0x123456)

	firstEvent, err := drainSSNMEvent(t, first)
	if err != nil {
		t.Fatalf("first Next: %v", err)
	}
	firstEvent.Report.Destinations[0].PointCode = 0
	firstEvent.Report.Scope.RoutingContexts[0] = 777
	firstEvent.States[0].Availability.State = DestinationAvailable
	firstEvent.States[0].Availability.Scope.RoutingContexts[0] = 777

	secondEvent, err := drainSSNMEvent(t, second)
	if err != nil {
		t.Fatalf("second Next: %v", err)
	}
	if secondEvent.Report.Destinations[0].PointCode != 0x123456 {
		t.Fatalf("one subscriber rewrote another's Affected Point Code: %+v",
			secondEvent.Report.Destinations)
	}
	if secondEvent.Report.Scope.RoutingContexts[0] != 1 {
		t.Fatalf("one subscriber rewrote another's wire scope: %+v", secondEvent.Report.Scope)
	}
	if secondEvent.States[0].Availability.State != DestinationUnavailable ||
		secondEvent.States[0].Availability.Scope.RoutingContexts[0] != 1 {
		t.Fatalf("one subscriber rewrote another's retained state: %+v", secondEvent.States)
	}
}

// Bullet: overflow is observable, and only a successful Resync clears
// continuity loss.
func TestSSNMSubscriptionQueueOverflowReportsContinuityLossUntilResync(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(),
		&SSNMStateConfig{SubscriptionQueueSize: 2})
	association := attachSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}, 7, 1)
	_, subscription, err := endpoint.SubscribeSSNM()
	if err != nil {
		t.Fatalf("SubscribeSSNM: %v", err)
	}
	defer func() { _ = subscription.Close() }()

	for index := range 6 {
		sendDUNA(t, association, 7, 1, 0x300000+uint32(index))
	}

	// What was queued before the overflow is still delivered, and the marker
	// follows it exactly once.
	var delivered int
	var marker SSNMEvent
	for {
		event, err := drainSSNMEvent(t, subscription)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if event.Kind == SSNMContinuityLostEvent {
			marker = event
			break
		}
		delivered++
		if delivered > 2 {
			t.Fatalf("queue of 2 delivered %d events before reporting loss", delivered)
		}
	}
	if !marker.ContinuityLost {
		t.Fatalf("continuity marker = %+v, want ContinuityLost", marker)
	}

	// Further reports stay suppressed: the sequence is meaningless across the
	// hole until a snapshot replaces it.
	sendDUNA(t, association, 7, 1, 0x3000FF)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	if _, err := subscription.Next(ctx); !errors.Is(err, context.DeadlineExceeded) {
		cancel()
		t.Fatalf("a delta was delivered across a continuity hole: %v", err)
	}
	cancel()

	snapshot, err := subscription.Resync()
	if err != nil {
		t.Fatalf("Resync: %v", err)
	}
	knowledge := ssnmPartitionKnowledge(t, snapshot, canonicalSSNMPartition("sg-a", "as-core"))
	if len(knowledge.Destinations) != 7 {
		t.Fatalf("Resync returned %d destinations, want every report the stream lost", len(knowledge.Destinations))
	}

	// The stream resumes from the snapshot.
	sendDAVA(t, association, 7, 1, 0x300000)
	event, err := drainSSNMEvent(t, subscription)
	if err != nil {
		t.Fatalf("Next after Resync: %v", err)
	}
	if event.Kind != SSNMReportEvent || event.Report.Kind != SSNMDestinationAvailableReport {
		t.Fatalf("after Resync the stream delivered %+v, want the DAVA report", event)
	}
	if event.Revision <= snapshot.Revision {
		t.Fatalf("resumed delta revision %d is not after the Resync revision %d",
			event.Revision, snapshot.Revision)
	}
}

func TestSSNMSubscriberLimitBelowAtAndAboveTheCap(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(),
		&SSNMStateConfig{MaxSubscribers: 2})
	first, second := mustSubscribeSSNM(t, endpoint), mustSubscribeSSNM(t, endpoint)
	if _, _, err := endpoint.SubscribeSSNM(); !errors.Is(err, ErrSSNMSubscriberLimit) {
		t.Fatalf("third subscription: error = %v, want ErrSSNMSubscriberLimit", err)
	}
	// Closing one frees its reservation.
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	third := mustSubscribeSSNM(t, endpoint)
	_ = second.Close()
	_ = third.Close()
}

func mustSubscribeSSNM(t *testing.T, endpoint *Endpoint) *SSNMSubscription {
	t.Helper()
	_, subscription, err := endpoint.SubscribeSSNM()
	if err != nil {
		t.Fatalf("SubscribeSSNM: %v", err)
	}
	t.Cleanup(func() { _ = subscription.Close() })
	return subscription
}

// Bullet: bounded retention after churn. Bindings that come and go must not
// leave partitions or records behind.
func TestSSNMStateRetentionStaysBoundedAcrossBindingChurn(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), nil)
	for round := range 50 {
		association, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
		association.cfg.NetworkAppearance = params.NewNetworkAppearance(7)
		association.cfg.PeerSGP = &SGPIdentity{
			SignallingGateway:        "sg-a",
			SignallingGatewayProcess: "sgp-a1",
		}
		association.noteRoutingContextsAcked(params.NewRoutingContext(1))
		if !endpoint.trackAssociation(association) {
			t.Fatalf("round %d: failed to attach", round)
		}
		sendDUNA(t, association, 7, 1, 0x400000+uint32(round))
		if err := association.Close(); err != nil {
			t.Fatalf("round %d: close: %v", round, err)
		}
		endpoint.forgetAssociation(association)
	}
	snapshot := endpoint.SSNMKnowledge()
	if len(snapshot.Partitions) != 0 {
		t.Fatalf("churn left %d partitions behind: %+v", len(snapshot.Partitions), snapshot.Partitions)
	}
	endpoint.ssnm.mu.Lock()
	records, bytes, peers := endpoint.ssnm.records, endpoint.ssnm.bytes, len(endpoint.ssnm.peerRecords)
	endpoint.ssnm.mu.Unlock()
	if records != 0 || bytes != 0 || peers != 0 {
		t.Fatalf("churn left records=%d bytes=%d peer budgets=%d behind", records, bytes, peers)
	}
}

// Bullet: Resync reconstructs retained dimensions and bindings.
func TestResyncReconstructsRetainedDimensionsAndBindings(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), nil)
	first := attachSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}, 7, 1)
	second := attachSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a2",
	}, 7, 2)
	sendDUNA(t, first, 7, 1, 0x123456)
	sendSCON(t, first, 7, 1, congestionLevel(2), 0x123456)

	_, subscription, err := endpoint.SubscribeSSNM()
	if err != nil {
		t.Fatalf("SubscribeSSNM: %v", err)
	}
	defer func() { _ = subscription.Close() }()

	snapshot, err := subscription.Resync()
	if err != nil {
		t.Fatalf("Resync: %v", err)
	}
	knowledge := ssnmPartitionKnowledge(t, snapshot, canonicalSSNMPartition("sg-a", "as-core"))
	if len(knowledge.Bindings) != 2 ||
		knowledge.Bindings[0].Association != first.ID() ||
		knowledge.Bindings[1].Association != second.ID() {
		t.Fatalf("Resync bindings = %+v, want both Associations", knowledge.Bindings)
	}
	destination := ssnmDestination(t, knowledge, 0x123456, 0)
	if !destination.AvailabilitySet || destination.Availability.State != DestinationUnavailable {
		t.Fatalf("Resync lost the availability dimension: %+v", destination)
	}
	if !destination.CongestionSet || destination.Congestion.Level != 2 {
		t.Fatalf("Resync lost the congestion dimension: %+v", destination)
	}
}

// Bullet: Resync does not reconstruct lost DUPU or peer congestion. Neither is
// destination state: RFC 4666 Section 3.4.5 reports an unavailable user part
// at a destination that remains reachable, and Section 3.4.4's ASP-originated
// SCON describes the peer's own M3UA layer.
func TestResyncDoesNotReconstructDUPUOrPeerCongestion(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), nil)
	association := attachSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}, 7, 1)
	_, subscription, err := endpoint.SubscribeSSNM()
	if err != nil {
		t.Fatalf("SubscribeSSNM: %v", err)
	}
	defer func() { _ = subscription.Close() }()

	if err := association.handleDestinationUserPartUnavailable(
		messages.NewDestinationUserPartUnavailable(
			params.NewNetworkAppearance(7), params.NewRoutingContext(1),
			params.NewAffectedPointCode(0x123456), params.NewUserCause(3, 2), nil)); err != nil {
		t.Fatalf("DUPU: %v", err)
	}

	// The DUPU is delivered as an event, with its cause.
	event, err := drainSSNMEvent(t, subscription)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if event.Report.Kind != SSNMDestinationUserPartUnavailableReport || !event.Report.UserCauseSet {
		t.Fatalf("DUPU event = %+v, want a DUPU carrying its User/Cause", event.Report)
	}

	snapshot, err := subscription.Resync()
	if err != nil {
		t.Fatalf("Resync: %v", err)
	}
	knowledge := ssnmPartitionKnowledge(t, snapshot, canonicalSSNMPartition("sg-a", "as-core"))
	if len(knowledge.Destinations) != 0 {
		t.Fatalf("Resync invented destination state from a DUPU: %+v", knowledge.Destinations)
	}

	// The peer-only SCON an SGP receives is the same kind of event.
	sgpEndpoint, sgp := newSGPSSNMFixture(t)
	_, sgpSubscription, err := sgpEndpoint.SubscribeSSNM()
	if err != nil {
		t.Fatalf("SGP SubscribeSSNM: %v", err)
	}
	defer func() { _ = sgpSubscription.Close() }()
	if err := sgp.handleSignallingCongestion(messages.NewSignallingCongestion(
		params.NewNetworkAppearance(7), params.NewRoutingContext(1),
		params.NewAffectedPointCode(0x123456), params.NewConcernedDestination(0x654321),
		params.NewCongestionIndications(2), nil)); err != nil {
		t.Fatalf("peer SCON: %v", err)
	}
	peerEvent, err := drainSSNMEvent(t, sgpSubscription)
	if err != nil {
		t.Fatalf("SGP Next: %v", err)
	}
	if !peerEvent.Report.PeerReported || !peerEvent.Report.ConcernedDestinationSet {
		t.Fatalf("peer SCON event = %+v, want a peer-reported SCON with its Concerned Destination",
			peerEvent.Report)
	}
	sgpSnapshot, err := sgpSubscription.Resync()
	if err != nil {
		t.Fatalf("SGP Resync: %v", err)
	}
	for _, partition := range sgpSnapshot.Partitions {
		if len(partition.Destinations) != 0 {
			t.Fatalf("Resync turned a peer's own congestion into destination state: %+v",
				partition.Destinations)
		}
	}
}

// Bullet: DAUD is not an event replay or completion primitive.
//
// RFC 4666 Section 3.4.3 has the ASP ask what the SG holds, and Section 4.4.2
// has the SG answer from state it already had. An audit neither replays what a
// subscriber missed nor completes its recovery.
func TestDAUDIsNotAnEventReplayOrCompletionPrimitive(t *testing.T) {
	endpoint, sgp := newSGPSSNMFixture(t)
	endpoint.destinations.setRanges([]DestinationRange{{
		RoutingContext: 1, RoutingContextSet: true,
		PointCode: 0x123456, State: DestinationUnavailable,
	}})
	sgp.destinations = endpoint.destinations

	_, subscription, err := endpoint.SubscribeSSNM()
	if err != nil {
		t.Fatalf("SubscribeSSNM: %v", err)
	}
	defer func() { _ = subscription.Close() }()

	audit := func() {
		t.Helper()
		if err := sgp.handleDestinationStateAudit(messages.NewDestinationStateAudit(
			params.NewNetworkAppearance(7), params.NewRoutingContext(1),
			params.NewAffectedPointCode(0x123456), nil)); err != nil {
			t.Fatalf("DAUD: %v", err)
		}
	}

	audit()
	event, err := drainSSNMEvent(t, subscription)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if event.Report.Kind != SSNMDestinationStateAuditReport {
		t.Fatalf("event = %+v, want the audit itself", event.Report)
	}
	// One audit, one event: it did not replay anything.
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	if _, err := subscription.Next(ctx); !errors.Is(err, context.DeadlineExceeded) {
		cancel()
		t.Fatalf("the audit replayed further events: %v", err)
	}
	cancel()
	// It retained nothing either: an audit is a request, not knowledge.
	for _, partition := range endpoint.SSNMKnowledge().Partitions {
		if len(partition.Destinations) != 0 {
			t.Fatalf("the audit was retained as knowledge: %+v", partition.Destinations)
		}
	}

	// An audit does not clear continuity loss. Only Resync does.
	subscription.mu.Lock()
	subscription.continuityLost = true
	subscription.pendingLoss = true
	subscription.mu.Unlock()
	audit()
	marker, err := drainSSNMEvent(t, subscription)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if marker.Kind != SSNMContinuityLostEvent {
		t.Fatalf("event = %+v, want the continuity marker", marker)
	}
	subscription.mu.Lock()
	stillLost := subscription.continuityLost
	subscription.mu.Unlock()
	if !stillLost {
		t.Fatal("the audit cleared continuity loss")
	}
	if _, err := subscription.Resync(); err != nil {
		t.Fatalf("Resync: %v", err)
	}
	subscription.mu.Lock()
	cleared := !subscription.continuityLost
	subscription.mu.Unlock()
	if !cleared {
		t.Fatal("Resync did not clear continuity loss")
	}
}

// The binding lifecycle is observable, so an application can tell an
// activation from an admission and a sibling handover from a retirement.
func TestSSNMBindingLifecycleIsPublishedToSubscribers(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), nil)
	_, subscription, err := endpoint.SubscribeSSNM()
	if err != nil {
		t.Fatalf("SubscribeSSNM: %v", err)
	}
	defer func() { _ = subscription.Close() }()

	activating := attachActivatingSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}, 7, 1)
	admitted, err := drainSSNMEvent(t, subscription)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if admitted.Kind != SSNMBindingAdmittedEvent || !admitted.Binding.Pending {
		t.Fatalf("event = %+v, want a pending admission", admitted)
	}

	activating.noteRoutingContextsAcked(params.NewRoutingContext(1))
	activating.sendState(StateASPActive)
	activated, err := drainSSNMEvent(t, subscription)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if activated.Kind != SSNMBindingActivatedEvent || activated.Binding.Pending {
		t.Fatalf("event = %+v, want a completed activation", activated)
	}
	if activated.Revision <= admitted.Revision {
		t.Fatalf("revision %d did not advance past %d", activated.Revision, admitted.Revision)
	}

	// A sibling joins, then the first one leaves: the partition survives and
	// says so.
	sibling := attachSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a2",
	}, 7, 2)
	if _, err := drainSSNMEvent(t, subscription); err != nil {
		t.Fatalf("Next: %v", err)
	}
	if err := activating.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	retired, err := drainSSNMEvent(t, subscription)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if retired.Kind != SSNMBindingRetiredEvent || retired.Binding.Association != activating.ID() {
		t.Fatalf("event = %+v, want the first binding retired", retired)
	}

	// The last one leaves and the partition goes with it.
	if err := sibling.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for {
		event, err := drainSSNMEvent(t, subscription)
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if event.Kind == SSNMPartitionRetiredEvent {
			if event.Partition != canonicalSSNMPartition("sg-a", "as-core") {
				t.Fatalf("retired partition = %+v", event.Partition)
			}
			return
		}
	}
}
