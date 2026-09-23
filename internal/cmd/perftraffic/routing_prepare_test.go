package main

import (
	"context"
	"errors"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua"
)

type fakeRoutingPreparationStream struct {
	mutex      sync.Mutex
	events     chan m3ua.SSNMEvent
	closed     chan struct{}
	closeOne   sync.Once
	closeCount int
}

func newFakeRoutingPreparationStream() *fakeRoutingPreparationStream {
	return &fakeRoutingPreparationStream{
		events: make(chan m3ua.SSNMEvent, routingSSNMReceiptCount+1),
		closed: make(chan struct{}),
	}
}

func (stream *fakeRoutingPreparationStream) Next(ctx context.Context) (m3ua.SSNMEvent, error) {
	for {
		select {
		case event := <-stream.events:
			return event, nil
		default:
		}
		select {
		case event := <-stream.events:
			return event, nil
		case <-stream.closed:
			select {
			case event := <-stream.events:
				return event, nil
			default:
				return m3ua.SSNMEvent{}, m3ua.ErrSSNMSubscriptionClosed
			}
		case <-ctx.Done():
			return m3ua.SSNMEvent{}, ctx.Err()
		}
	}
}

func (stream *fakeRoutingPreparationStream) Close() error {
	stream.mutex.Lock()
	stream.closeCount++
	stream.mutex.Unlock()
	stream.closeOne.Do(func() { close(stream.closed) })
	return nil
}

func (stream *fakeRoutingPreparationStream) enqueue(event m3ua.SSNMEvent) {
	stream.events <- event
}

type fakeRoutingPreparationSender struct {
	inventory      []routingSenderInventory
	statuses       []m3ua.ASPStatus
	initial        m3ua.SSNMSnapshot
	final          m3ua.SSNMSnapshot
	stream         *fakeRoutingPreparationStream
	closeCount     int
	knowledgeCalls int
	subscribeCalls int
}

func (sender *fakeRoutingPreparationSender) Inventory() ([]routingSenderInventory, error) {
	return append([]routingSenderInventory(nil), sender.inventory...), nil
}

func (sender *fakeRoutingPreparationSender) ASPStatuses() []m3ua.ASPStatus {
	return append([]m3ua.ASPStatus(nil), sender.statuses...)
}

func (sender *fakeRoutingPreparationSender) SubscribeSSNM() (m3ua.SSNMSnapshot, routingSSNMEventStream, error) {
	sender.subscribeCalls++
	return sender.initial, sender.stream, nil
}

func (sender *fakeRoutingPreparationSender) SSNMKnowledge() m3ua.SSNMSnapshot {
	sender.knowledgeCalls++
	return sender.final
}

func (sender *fakeRoutingPreparationSender) Close() error {
	sender.closeCount++
	return nil
}

type fakeRoutingPreparationControl struct {
	inventory         routingInventoryDTO
	stream            *fakeRoutingPreparationStream
	plan              routingSSNMPreparationPlan
	initial           m3ua.SSNMSnapshot
	prepared          bool
	inventoryCalls    int
	prepareCalls      int
	publishCalls      []uint8
	stopCalls         int
	publishErr        map[uint8]error
	publishGate       map[uint8]<-chan struct{}
	receiptLimit      map[uint8]int
	beforeReturn      func(uint8)
	extraAfterOrdinal map[uint8][]m3ua.SSNMEvent
}

func (control *fakeRoutingPreparationControl) Inventory(context.Context) (routingInventoryDTO, error) {
	control.inventoryCalls++
	return control.inventory, nil
}

func (control *fakeRoutingPreparationControl) Prepare(_ context.Context, transports []routingTransportDTO) error {
	control.prepareCalls++
	control.prepared = len(transports) == 8
	return nil
}

func (control *fakeRoutingPreparationControl) Publish(ctx context.Context, ordinal uint8) error {
	control.publishCalls = append(control.publishCalls, ordinal)
	if gate := control.publishGate[ordinal]; gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	publication, err := control.plan.publication(ordinal)
	if err != nil {
		return err
	}
	epoch := routingPreparationPartitionEpoch(control.initial, publication.partition())
	base := control.initial.Revision + 1 + uint64(ordinal)*routingSSNMReceiptsPerPublication
	limit := routingSSNMReceiptsPerPublication
	if configured, present := control.receiptLimit[ordinal]; present {
		limit = configured
	}
	for index := len(publication.ExpectedSenderAssociations) - 1; index >= len(publication.ExpectedSenderAssociations)-limit; index-- {
		control.stream.enqueue(routingSSNMTestEvent(publication, publication.ExpectedSenderAssociations[index], epoch, base+uint64(1-index)))
	}
	for _, event := range control.extraAfterOrdinal[ordinal] {
		control.stream.enqueue(event)
	}
	if control.beforeReturn != nil {
		control.beforeReturn(ordinal)
	}
	return control.publishErr[ordinal]
}

func (control *fakeRoutingPreparationControl) Stop(context.Context) error {
	control.stopCalls++
	return nil
}

type routingPreparationFixture struct {
	topology routingTopology
	senders  []routingSenderInventory
	peers    []routingPeerInventory
	plan     routingSSNMPreparationPlan
	sender   *fakeRoutingPreparationSender
	control  *fakeRoutingPreparationControl
}

func newRoutingPreparationFixture(testContext *testing.T) routingPreparationFixture {
	testContext.Helper()
	topology, senders, peers := routingInventoryFixture(testContext)
	pairs, err := pairRoutingInventory(topology, senders, peers)
	if err != nil {
		testContext.Fatal(err)
	}
	plan, err := newRoutingSSNMPreparationPlan(topology, pairs)
	if err != nil {
		testContext.Fatal(err)
	}
	statuses := make([]m3ua.ASPStatus, 0, 16)
	for _, association := range plan.associations {
		for _, server := range []m3ua.RemoteASID{"primary", "secondary"} {
			publication, _ := plan.publicationFor(association.SGP, server)
			statuses = append(statuses, m3ua.ASPStatus{
				Key:        m3ua.ASPStatusKey{Association: association.Association, AS: publication.AS},
				LocalState: m3ua.StateASPActive, LocalStateSet: true,
				LocalASPIdentifier: association.ASPIdentifier, LocalASPIdentifierSet: true,
			})
		}
	}
	initial := m3ua.SSNMSnapshot{Revision: 40}
	for index, partition := range plan.partitions {
		bindings := make([]m3ua.SSNMBinding, len(partition.Associations))
		for bindingIndex, association := range partition.Associations {
			bindings[bindingIndex] = m3ua.SSNMBinding{Association: association}
		}
		initial.Partitions = append(initial.Partitions, m3ua.SSNMPartitionKnowledge{
			Partition: partition.Partition, Epoch: uint64(500 + index),
			Bindings: bindings, TrafficAuthorized: true,
		})
	}
	reference, err := plan.begin(statuses, initial)
	if err != nil {
		testContext.Fatal(err)
	}
	for ordinal := uint8(0); ordinal < routingSSNMPublicationCount; ordinal++ {
		publication, _ := plan.publication(ordinal)
		epoch := reference.partitionEpoch(publication.partition())
		for index := len(publication.ExpectedSenderAssociations) - 1; index >= 0; index-- {
			event := routingSSNMTestEvent(publication, publication.ExpectedSenderAssociations[index], epoch, reference.nextRevision)
			if err := reference.observe(ordinal, event); err != nil {
				testContext.Fatal(err)
			}
		}
		if err := reference.publicationComplete(ordinal); err != nil {
			testContext.Fatal(err)
		}
	}
	stream := newFakeRoutingPreparationStream()
	sender := &fakeRoutingPreparationSender{
		inventory: senders, statuses: statuses, initial: initial,
		final: routingSSNMFinalSnapshot(testContext, reference), stream: stream,
	}
	peerDTOs := make([]routingTransportDTO, len(peers))
	for index, peer := range peers {
		peerDTOs[index], err = routingTransportDTOFromPeer(peer)
		if err != nil {
			testContext.Fatal(err)
		}
	}
	control := &fakeRoutingPreparationControl{
		inventory: routingInventoryDTO{Ready: true, Transports: peerDTOs, ASPStatuses: routingPreparationPeerStatuses(testContext, topology, peers)},
		stream:    stream, plan: plan, initial: initial, publishErr: make(map[uint8]error),
		publishGate: make(map[uint8]<-chan struct{}), receiptLimit: make(map[uint8]int),
		extraAfterOrdinal: make(map[uint8][]m3ua.SSNMEvent),
	}
	return routingPreparationFixture{topology: topology, senders: senders, peers: peers, plan: plan, sender: sender, control: control}
}

func routingPreparationPeerStatuses(testContext *testing.T, topology routingTopology, peers []routingPeerInventory) []routingScopedASPStatusDTO {
	testContext.Helper()
	statuses := make([]routingScopedASPStatusDTO, 0, 16)
	for _, peer := range peers {
		var configured routingPeer
		for _, candidate := range topology.Peers {
			if candidate.Identity == peer.SGP {
				configured = candidate
				break
			}
		}
		for _, server := range configured.ApplicationServers {
			statuses = append(statuses, routingScopedASPStatusDTO{SGP: peer.SGP, Status: m3ua.ASPStatus{
				Key:       m3ua.ASPStatusKey{Association: peer.Snapshot.Association, AS: server.ASKey},
				PeerState: m3ua.StateASPActive, PeerStateSet: true,
				PeerASPIdentifier: peer.Snapshot.PeerASPIdentifier, PeerASPIdentifierSet: true,
			}})
		}
	}
	return statuses
}

func routingPreparationPartitionEpoch(snapshot m3ua.SSNMSnapshot, partition m3ua.SSNMPartition) uint64 {
	for _, knowledge := range snapshot.Partitions {
		if knowledge.Partition == partition {
			return knowledge.Epoch
		}
	}
	return 0
}

func TestRoutingPreparationUsesEightSerialPublicationsAndSixteenReceipts(testContext *testing.T) {
	fixture := newRoutingPreparationFixture(testContext)
	evidence, err := prepareRoutingSSNM(context.Background(), fixture.topology, fixture.sender, fixture.control)
	if err != nil {
		testContext.Fatal(err)
	}
	if fixture.control.prepareCalls != 1 || !fixture.control.prepared ||
		!reflect.DeepEqual(fixture.control.publishCalls, []uint8{0, 1, 2, 3, 4, 5, 6, 7}) ||
		fixture.control.stopCalls != 0 || fixture.sender.closeCount != 0 ||
		fixture.sender.subscribeCalls != 1 || fixture.sender.knowledgeCalls != 1 ||
		evidence.ReceiptCount != routingSSNMReceiptCount || evidence.FinalRevision-evidence.InitialRevision != routingSSNMReceiptCount {
		testContext.Fatalf("unexpected preparation evidence or lifecycle: %+v sender=%+v control=%+v", evidence, fixture.sender, fixture.control)
	}
	if fixture.sender.stream.closeCount != 1 {
		testContext.Fatalf("subscription close count=%d", fixture.sender.stream.closeCount)
	}
}

func TestRoutingPreparationAlreadyCanceledContextStillCleansOwnedResources(testContext *testing.T) {
	fixture := newRoutingPreparationFixture(testContext)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := prepareRoutingSSNM(ctx, fixture.topology, fixture.sender, fixture.control); !errors.Is(err, context.Canceled) {
		testContext.Fatalf("error=%v", err)
	}
	if fixture.control.inventoryCalls != 0 || fixture.control.prepareCalls != 0 || len(fixture.control.publishCalls) != 0 ||
		fixture.sender.subscribeCalls != 0 || fixture.control.stopCalls != 1 || fixture.sender.closeCount != 1 {
		testContext.Fatalf("inventory=%d prepare=%d publish=%d subscribe=%d stop=%d close=%d",
			fixture.control.inventoryCalls, fixture.control.prepareCalls, len(fixture.control.publishCalls),
			fixture.sender.subscribeCalls, fixture.control.stopCalls, fixture.sender.closeCount)
	}
}

func TestRoutingPreparationAcceptsReceiptsBeforePublicationResponse(testContext *testing.T) {
	fixture := newRoutingPreparationFixture(testContext)
	observedBeforeReturn := make(chan struct{}, 1)
	fixture.control.beforeReturn = func(ordinal uint8) {
		if ordinal == 0 {
			time.Sleep(time.Millisecond)
			observedBeforeReturn <- struct{}{}
		}
	}
	if _, err := prepareRoutingSSNM(context.Background(), fixture.topology, fixture.sender, fixture.control); err != nil {
		testContext.Fatal(err)
	}
	select {
	case <-observedBeforeReturn:
	default:
		testContext.Fatal("publication response returned without exercising early receipts")
	}
}

func TestRoutingPreparationRequiresHTTPAndBothReceiptsWithoutRetry(testContext *testing.T) {
	testContext.Run("HTTP failure after actual receipts", func(testContext *testing.T) {
		fixture := newRoutingPreparationFixture(testContext)
		fixture.control.publishErr[2] = errors.New("ambiguous HTTP failure")
		if _, err := prepareRoutingSSNM(context.Background(), fixture.topology, fixture.sender, fixture.control); err == nil {
			testContext.Fatal("accepted failed HTTP publication after actual receipts")
		}
		if !reflect.DeepEqual(fixture.control.publishCalls, []uint8{0, 1, 2}) || fixture.control.stopCalls != 1 || fixture.sender.closeCount != 1 {
			testContext.Fatalf("retried or failed cleanup: publishes=%v stop=%d close=%d", fixture.control.publishCalls, fixture.control.stopCalls, fixture.sender.closeCount)
		}
	})
	testContext.Run("missing second receipt", func(testContext *testing.T) {
		fixture := newRoutingPreparationFixture(testContext)
		fixture.control.receiptLimit[3] = 1
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
		defer cancel()
		if _, err := prepareRoutingSSNM(ctx, fixture.topology, fixture.sender, fixture.control); err == nil {
			testContext.Fatal("accepted an incomplete publication")
		}
	})
}

func TestRoutingPreparationReaderCancellationCannotBlockOnFullEventHandoff(testContext *testing.T) {
	fixture := newRoutingPreparationFixture(testContext)
	publication, _ := fixture.plan.publication(0)
	epoch := routingPreparationPartitionEpoch(fixture.sender.initial, publication.partition())
	for index := range 3 {
		fixture.sender.stream.enqueue(routingSSNMTestEvent(publication, publication.ExpectedSenderAssociations[index%2], epoch, fixture.sender.initial.Revision+uint64(index)+1))
	}
	gate := make(chan struct{})
	fixture.control.publishGate[0] = gate
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := prepareRoutingSSNM(ctx, fixture.topology, fixture.sender, fixture.control)
		done <- err
	}()
	time.Sleep(time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err == nil {
			testContext.Fatal("cancellation returned success")
		}
	case <-time.After(time.Second):
		testContext.Fatal("reader remained blocked on full event handoff")
	}
}

func TestRoutingPreparationRejectsUnexpectedQueuedSeventeenthEvent(testContext *testing.T) {
	fixture := newRoutingPreparationFixture(testContext)
	publication, _ := fixture.plan.publication(7)
	epoch := routingPreparationPartitionEpoch(fixture.sender.initial, publication.partition())
	fixture.control.extraAfterOrdinal[7] = []m3ua.SSNMEvent{
		routingSSNMTestEvent(publication, publication.ExpectedSenderAssociations[0], epoch, fixture.sender.initial.Revision+routingSSNMReceiptCount+1),
	}
	if _, err := prepareRoutingSSNM(context.Background(), fixture.topology, fixture.sender, fixture.control); err == nil {
		testContext.Fatal("accepted an unexpected queued seventeenth event")
	}
	if fixture.sender.knowledgeCalls != 0 || fixture.control.stopCalls != 1 || fixture.sender.closeCount != 1 {
		testContext.Fatalf("finalized or skipped cleanup after extra event: knowledge=%d stop=%d close=%d", fixture.sender.knowledgeCalls, fixture.control.stopCalls, fixture.sender.closeCount)
	}
}

func TestRoutingPreparationRejectsTerminalSubscriptionEvents(testContext *testing.T) {
	tests := map[string]m3ua.SSNMEvent{
		"continuity loss": {Kind: m3ua.SSNMContinuityLostEvent, ContinuityLost: true},
		"resource loss":   {Kind: m3ua.SSNMResourceLossEvent, Reason: "bounded resource loss"},
		"binding retired": {Kind: m3ua.SSNMBindingRetiredEvent},
	}
	for name, event := range tests {
		testContext.Run(name, func(testContext *testing.T) {
			fixture := newRoutingPreparationFixture(testContext)
			fixture.control.extraAfterOrdinal[0] = []m3ua.SSNMEvent{event}
			if _, err := prepareRoutingSSNM(context.Background(), fixture.topology, fixture.sender, fixture.control); err == nil {
				testContext.Fatal("accepted terminal subscription event")
			}
		})
	}
}

func TestRoutingPreparationRejectsRemoteInventoryAndFinalKnowledgeContradictions(testContext *testing.T) {
	tests := map[string]func(*routingPreparationFixture){
		"missing transport": func(fixture *routingPreparationFixture) {
			fixture.control.inventory.Transports = fixture.control.inventory.Transports[:7]
		},
		"missing status": func(fixture *routingPreparationFixture) {
			fixture.control.inventory.ASPStatuses = fixture.control.inventory.ASPStatuses[:15]
		},
		"duplicate status": func(fixture *routingPreparationFixture) {
			fixture.control.inventory.ASPStatuses[1] = fixture.control.inventory.ASPStatuses[0]
		},
		"wrong SGP": func(fixture *routingPreparationFixture) {
			fixture.control.inventory.ASPStatuses[0].SGP = fixture.topology.Peers[3].Identity
		},
		"inactive peer": func(fixture *routingPreparationFixture) {
			fixture.control.inventory.ASPStatuses[0].Status.PeerState = m3ua.StateASPInactive
		},
		"wrong identifier": func(fixture *routingPreparationFixture) {
			fixture.control.inventory.ASPStatuses[0].Status.PeerASPIdentifier++
		},
		"final resource loss": func(fixture *routingPreparationFixture) { fixture.sender.final.ReportsRefused++ },
		"final revision":      func(fixture *routingPreparationFixture) { fixture.sender.final.Revision++ },
	}
	for name, mutate := range tests {
		testContext.Run(name, func(testContext *testing.T) {
			fixture := newRoutingPreparationFixture(testContext)
			mutate(&fixture)
			if _, err := prepareRoutingSSNM(context.Background(), fixture.topology, fixture.sender, fixture.control); err == nil {
				testContext.Fatal("accepted contradictory preparation evidence")
			}
		})
	}
}

type fakeRoutingPeerPreparationSource struct {
	mutex       sync.Mutex
	inventory   []routingPeerInventory
	statuses    map[m3ua.SGPIdentity][]m3ua.ASPStatus
	reportedSGP []m3ua.SGPIdentity
	requests    []m3ua.DestinationAvailabilityRequest
	reportStart chan struct{}
	reportGate  chan struct{}
	startOne    sync.Once
	closeOne    sync.Once
	closeCount  int
}

func (source *fakeRoutingPeerPreparationSource) Inventory() ([]routingPeerInventory, error) {
	return append([]routingPeerInventory(nil), source.inventory...), nil
}

func (source *fakeRoutingPeerPreparationSource) ASPStatuses(sgp m3ua.SGPIdentity) []m3ua.ASPStatus {
	return append([]m3ua.ASPStatus(nil), source.statuses[sgp]...)
}

func (source *fakeRoutingPeerPreparationSource) ReportDestinationAvailability(sgp m3ua.SGPIdentity, request m3ua.DestinationAvailabilityRequest) error {
	source.mutex.Lock()
	source.reportedSGP = append(source.reportedSGP, sgp)
	source.requests = append(source.requests, request)
	source.mutex.Unlock()
	source.startOne.Do(func() { close(source.reportStart) })
	<-source.reportGate
	return nil
}

func (source *fakeRoutingPeerPreparationSource) Close() error {
	source.closeOne.Do(func() {
		source.closeCount++
		close(source.reportGate)
	})
	return nil
}

func TestRoutingPeerPreparationCancellationClosesOwnerAndUnblocksSynchronousPublication(testContext *testing.T) {
	fixture := newRoutingPreparationFixture(testContext)
	source := &fakeRoutingPeerPreparationSource{
		inventory: fixture.peers, statuses: make(map[m3ua.SGPIdentity][]m3ua.ASPStatus),
		reportStart: make(chan struct{}), reportGate: make(chan struct{}),
	}
	for _, scoped := range fixture.control.inventory.ASPStatuses {
		source.statuses[scoped.SGP] = append(source.statuses[scoped.SGP], scoped.Status)
	}
	peer, err := newRoutingPeerPreparation(fixture.topology, source)
	if err != nil {
		testContext.Fatal(err)
	}
	operations := peer.operations()
	senders := make([]routingTransportDTO, len(fixture.senders))
	for index, sender := range fixture.senders {
		senders[index], err = routingTransportDTOFromSender(sender)
		if err != nil {
			testContext.Fatal(err)
		}
	}
	if err := operations.Prepare(context.Background(), senders); err != nil {
		testContext.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- operations.Publish(ctx, 0) }()
	<-source.reportStart
	cancel()
	select {
	case err := <-done:
		if err == nil || source.closeCount != 1 {
			testContext.Fatalf("publish error=%v close count=%d", err, source.closeCount)
		}
	case <-time.After(time.Second):
		testContext.Fatal("owner close did not unblock the synchronous publication")
	}
}

func TestRoutingPeerPreparationSuccessfulPublishDoesNotCloseOnRequestCleanup(testContext *testing.T) {
	fixture := newRoutingPreparationFixture(testContext)
	source := &fakeRoutingPeerPreparationSource{
		inventory: fixture.peers, statuses: make(map[m3ua.SGPIdentity][]m3ua.ASPStatus),
		reportStart: make(chan struct{}), reportGate: make(chan struct{}),
	}
	for _, scoped := range fixture.control.inventory.ASPStatuses {
		source.statuses[scoped.SGP] = append(source.statuses[scoped.SGP], scoped.Status)
	}
	peer, err := newRoutingPeerPreparation(fixture.topology, source)
	if err != nil {
		testContext.Fatal(err)
	}
	operations := peer.operations()
	senders := make([]routingTransportDTO, len(fixture.senders))
	for index, sender := range fixture.senders {
		senders[index], _ = routingTransportDTOFromSender(sender)
	}
	if err := operations.Prepare(context.Background(), senders); err != nil {
		testContext.Fatal(err)
	}
	close(source.reportGate)
	ctx, cancel := context.WithCancel(context.Background())
	if err := operations.Publish(ctx, 0); err != nil {
		testContext.Fatal(err)
	}
	cancel()
	time.Sleep(time.Millisecond)
	if source.closeCount != 0 {
		testContext.Fatal("successful request cleanup closed the peer owner")
	}
}

func TestRoutingPeerPreparationConcurrentStopDoesNotDeadlockPublication(testContext *testing.T) {
	fixture := newRoutingPreparationFixture(testContext)
	source := &fakeRoutingPeerPreparationSource{
		inventory: fixture.peers, statuses: make(map[m3ua.SGPIdentity][]m3ua.ASPStatus),
		reportStart: make(chan struct{}), reportGate: make(chan struct{}),
	}
	for _, scoped := range fixture.control.inventory.ASPStatuses {
		source.statuses[scoped.SGP] = append(source.statuses[scoped.SGP], scoped.Status)
	}
	peer, err := newRoutingPeerPreparation(fixture.topology, source)
	if err != nil {
		testContext.Fatal(err)
	}
	operations := peer.operations()
	senders := make([]routingTransportDTO, len(fixture.senders))
	for index, sender := range fixture.senders {
		senders[index], _ = routingTransportDTOFromSender(sender)
	}
	if err := operations.Prepare(context.Background(), senders); err != nil {
		testContext.Fatal(err)
	}
	published := make(chan error, 1)
	go func() { published <- operations.Publish(context.Background(), 0) }()
	<-source.reportStart
	stopped := make(chan error, 1)
	go func() { stopped <- operations.Stop(context.Background()) }()
	for name, result := range map[string]<-chan error{"publish": published, "stop": stopped} {
		select {
		case <-result:
		case <-time.After(time.Second):
			testContext.Fatalf("%s deadlocked", name)
		}
	}
	if source.closeCount != 1 {
		testContext.Fatalf("close count=%d", source.closeCount)
	}
}

func TestRoutingPeerPreparationRejectsConcurrentPublication(testContext *testing.T) {
	fixture := newRoutingPreparationFixture(testContext)
	source := &fakeRoutingPeerPreparationSource{
		inventory: fixture.peers, statuses: make(map[m3ua.SGPIdentity][]m3ua.ASPStatus),
		reportStart: make(chan struct{}), reportGate: make(chan struct{}),
	}
	for _, scoped := range fixture.control.inventory.ASPStatuses {
		source.statuses[scoped.SGP] = append(source.statuses[scoped.SGP], scoped.Status)
	}
	peer, err := newRoutingPeerPreparation(fixture.topology, source)
	if err != nil {
		testContext.Fatal(err)
	}
	operations := peer.operations()
	senders := make([]routingTransportDTO, len(fixture.senders))
	for index, sender := range fixture.senders {
		senders[index], _ = routingTransportDTOFromSender(sender)
	}
	if err := operations.Prepare(context.Background(), senders); err != nil {
		testContext.Fatal(err)
	}
	first := make(chan error, 1)
	go func() { first <- operations.Publish(context.Background(), 0) }()
	<-source.reportStart
	if err := operations.Publish(context.Background(), 0); err == nil {
		testContext.Fatal("accepted a concurrent publication")
	}
	if err := operations.Stop(context.Background()); err != nil {
		testContext.Fatal(err)
	}
	select {
	case <-first:
	case <-time.After(time.Second):
		testContext.Fatal("first publication did not exit after stop")
	}
	if len(source.requests) != 1 {
		testContext.Fatalf("report calls=%d", len(source.requests))
	}
}

func TestRoutingPeerPreparationPublishesExactDerivedRequestAndRevalidatesStatuses(testContext *testing.T) {
	fixture := newRoutingPreparationFixture(testContext)
	source := &fakeRoutingPeerPreparationSource{
		inventory: fixture.peers, statuses: make(map[m3ua.SGPIdentity][]m3ua.ASPStatus),
		reportStart: make(chan struct{}), reportGate: make(chan struct{}),
	}
	for _, scoped := range fixture.control.inventory.ASPStatuses {
		source.statuses[scoped.SGP] = append(source.statuses[scoped.SGP], scoped.Status)
	}
	peer, err := newRoutingPeerPreparation(fixture.topology, source)
	if err != nil {
		testContext.Fatal(err)
	}
	operations := peer.operations()
	if inventory, err := operations.Inventory(context.Background()); err != nil || !inventory.Ready || len(inventory.Transports) != 8 || len(inventory.ASPStatuses) != 16 {
		testContext.Fatalf("inventory=%+v error=%v", inventory, err)
	}
	senders := make([]routingTransportDTO, len(fixture.senders))
	for index, sender := range fixture.senders {
		senders[index], err = routingTransportDTOFromSender(sender)
		if err != nil {
			testContext.Fatal(err)
		}
	}
	firstSGP := fixture.topology.Peers[0].Identity
	source.statuses[firstSGP][0].PeerState = m3ua.StateASPInactive
	if err := operations.Prepare(context.Background(), senders); err == nil {
		testContext.Fatal("prepare accepted status that changed after inventory")
	}

	source = &fakeRoutingPeerPreparationSource{
		inventory: fixture.peers, statuses: make(map[m3ua.SGPIdentity][]m3ua.ASPStatus),
		reportStart: make(chan struct{}), reportGate: make(chan struct{}),
	}
	for _, scoped := range fixture.control.inventory.ASPStatuses {
		source.statuses[scoped.SGP] = append(source.statuses[scoped.SGP], scoped.Status)
	}
	peer, err = newRoutingPeerPreparation(fixture.topology, source)
	if err != nil {
		testContext.Fatal(err)
	}
	operations = peer.operations()
	if err := operations.Prepare(context.Background(), senders); err != nil {
		testContext.Fatal(err)
	}
	close(source.reportGate)
	publication, _ := fixture.plan.publication(0)
	if err := operations.Publish(context.Background(), 0); err != nil {
		testContext.Fatal(err)
	}
	source.mutex.Lock()
	defer source.mutex.Unlock()
	if !reflect.DeepEqual(source.reportedSGP, []m3ua.SGPIdentity{publication.SGP}) ||
		!reflect.DeepEqual(source.requests, []m3ua.DestinationAvailabilityRequest{publication.request()}) {
		testContext.Fatalf("SGPs=%+v requests=%+v", source.reportedSGP, source.requests)
	}
}

func TestRoutingPreparationHTTPClientRoundTripsInventoryAndCommands(testContext *testing.T) {
	operations, probe, senders := routingControlFixture(testContext)
	control, err := newRoutingControl("prep-a", operations)
	if err != nil {
		testContext.Fatal(err)
	}
	server := httptest.NewServer(control.handler())
	defer server.Close()
	client, err := newRoutingPreparationHTTPClient(server.URL, "prep-a")
	if err != nil {
		testContext.Fatal(err)
	}
	inventory, err := client.Inventory(context.Background())
	if err != nil || !inventory.Ready || len(inventory.Transports) != 8 || len(inventory.ASPStatuses) != 16 {
		testContext.Fatalf("inventory=%+v error=%v", inventory, err)
	}
	if err := client.Prepare(context.Background(), senders); err != nil {
		testContext.Fatal(err)
	}
	for ordinal := uint8(0); ordinal < routingSSNMPublicationCount; ordinal++ {
		if err := client.Publish(context.Background(), ordinal); err != nil {
			testContext.Fatal(err)
		}
	}
	if err := client.Stop(context.Background()); err != nil {
		testContext.Fatal(err)
	}
	if probe.prepared != 1 || probe.stopped != 1 || !reflect.DeepEqual(probe.publications, []uint8{0, 1, 2, 3, 4, 5, 6, 7}) {
		testContext.Fatalf("probe=%+v", probe)
	}
}

func TestRoutingPreparationDTOsReconstructPairableOwnedInventory(testContext *testing.T) {
	topology, senders, peers := routingInventoryFixture(testContext)
	senderDTOs := make([]routingTransportDTO, len(senders))
	peerDTOs := make([]routingTransportDTO, len(peers))
	for index := range senders {
		var err error
		senderDTOs[index], err = routingTransportDTOFromSender(senders[index])
		if err != nil {
			testContext.Fatal(err)
		}
		peerDTOs[index], err = routingTransportDTOFromPeer(peers[index])
		if err != nil {
			testContext.Fatal(err)
		}
	}
	reconstructedSenders, err := routingSenderInventoryFromDTOs(senderDTOs)
	if err != nil {
		testContext.Fatal(err)
	}
	reconstructedPeers, err := routingPeerInventoryFromDTOs(peerDTOs)
	if err != nil {
		testContext.Fatal(err)
	}
	pairs, err := pairRoutingInventory(topology, reconstructedSenders, reconstructedPeers)
	if err != nil || len(pairs) != 8 {
		testContext.Fatalf("pairs=%+v error=%v", pairs, err)
	}
	before := senderDTOs[0].Local.Address
	reconstructedSenders[0].Snapshot.LocalAddr.IPAddrs[0].IP[0] ^= 0xff
	if senderDTOs[0].Local.Address != before {
		testContext.Fatal("reconstructed inventory aliases its transport DTO")
	}
}

func TestRoutingPreparationProductionAdaptersRejectIncompleteSetup(testContext *testing.T) {
	topology, err := newRoutingTopology("primary")
	if err != nil {
		testContext.Fatal(err)
	}
	if _, err := newRoutingM3UASenderPreparation(nil); err == nil {
		testContext.Fatal("accepted a nil sender set")
	}
	if _, err := newRoutingM3UAPeerPreparationSource(topology, nil); err == nil {
		testContext.Fatal("accepted a nil peer set")
	}
}

func FuzzRoutingPreparationHTTPClientURL(fuzzContext *testing.F) {
	for _, seed := range [][2]string{{"http://127.0.0.1:8080", "prep-a"}, {"https://example.invalid/", "prep-b"}, {"", ""}, {"http://user@example.invalid", "prep"}} {
		fuzzContext.Add(seed[0], seed[1])
	}
	fuzzContext.Fuzz(func(testContext *testing.T, address, preparationID string) {
		client, err := newRoutingPreparationHTTPClient(address, preparationID)
		if err == nil && (client == nil || client.baseURL == "" || client.preparationID != preparationID) {
			testContext.Fatalf("accepted client lost validated identity: %+v", client)
		}
	})
}
