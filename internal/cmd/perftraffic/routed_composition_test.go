package main

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua"
	"github.com/gomaja/go-sctp"
)

// routedTestPeerAssociation is one SGP-side association of the routed
// receiver. It records every overlap of two ReadData calls, so a test can
// prove no association is ever read by the preflight session and a timed
// reader at once, and counts the reads made with the receiver lifetime, which
// only the timed readers use.
type routedTestPeerAssociation struct {
	id       m3ua.AssociationID
	epoch    uint64
	maximum  uint16
	reads    chan *m3ua.DataMessage
	lifetime context.Context

	mutex      sync.Mutex
	active     int
	overlapped bool
	timedReads atomic.Int32
}

func (association *routedTestPeerAssociation) ID() m3ua.AssociationID { return association.id }
func (association *routedTestPeerAssociation) Epoch() uint64          { return association.epoch }
func (association *routedTestPeerAssociation) MaxMessageStreamID() uint16 {
	return association.maximum
}

func (association *routedTestPeerAssociation) ReadData(ctx context.Context) (*m3ua.DataMessage, error) {
	association.mutex.Lock()
	association.active++
	association.overlapped = association.overlapped || association.active > 1
	association.mutex.Unlock()
	defer func() {
		association.mutex.Lock()
		association.active--
		association.mutex.Unlock()
	}()
	if ctx == association.lifetime {
		association.timedReads.Add(1)
	}
	select {
	case message := <-association.reads:
		return message, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (association *routedTestPeerAssociation) WriteData(m3ua.DataRequest) (int, error) {
	return 0, errors.New("the routed receiver never writes")
}

func (*routedTestPeerAssociation) DataQueueStats() m3ua.DataQueueStats { return m3ua.DataQueueStats{} }

func (association *routedTestPeerAssociation) readers() (int, bool) {
	association.mutex.Lock()
	defer association.mutex.Unlock()
	return association.active, association.overlapped
}

// routedTestPeerEndpoint is one SGP Endpoint of the routed receiver: the
// setup fake plus the SSNM operations the production preparation source uses.
type routedTestPeerEndpoint struct {
	*fakeRoutingEndpoint
	statuses []m3ua.ASPStatus
	reports  atomic.Int32
}

func (endpoint *routedTestPeerEndpoint) ASPStatuses() []m3ua.ASPStatus {
	return append([]m3ua.ASPStatus(nil), endpoint.statuses...)
}

func (endpoint *routedTestPeerEndpoint) ReportDestinationAvailability(m3ua.DestinationAvailabilityRequest) error {
	endpoint.reports.Add(1)
	return nil
}

// routedTestReceiver is the production routed receiver composition —
// routedPeerOperations over a real routingPeerSet, the production SSNM
// preparation source and preparation, and a routed receiverControl — with
// only the M3UA endpoints and associations faked.
type routedTestReceiver struct {
	lifetime     context.Context
	cancel       context.CancelFunc
	topology     routingTopology
	peers        *routingPeerSet
	endpoints    []*routedTestPeerEndpoint
	associations map[routingTransport]*routedTestPeerAssociation
	control      *receiverControl
	operations   *routedPeerOperations
	fatal        chan error
	senders      []routingTransportDTO
}

func newRoutedTestReceiver(testContext *testing.T, mode string) *routedTestReceiver {
	testContext.Helper()
	topology, senders, peers := routingInventoryFixture(testContext)
	lifetime, cancel := context.WithCancel(context.Background())
	testContext.Cleanup(cancel)
	receiver := &routedTestReceiver{
		lifetime: lifetime, cancel: cancel, topology: topology,
		associations: make(map[routingTransport]*routedTestPeerAssociation, len(peers)),
		fatal:        make(chan error, 1),
	}
	statuses := make(map[m3ua.SGPIdentity][]m3ua.ASPStatus)
	for _, scoped := range routingPreparationPeerStatuses(testContext, topology, peers) {
		statuses[scoped.SGP] = append(statuses[scoped.SGP], scoped.Status)
	}
	addresses := routingSetupAddresses()
	for peerIndex, peer := range topology.Peers {
		endpoint := &routedTestPeerEndpoint{
			fakeRoutingEndpoint: &fakeRoutingEndpoint{
				listener:  &fakeRoutingListener{incoming: make(chan routingSetupAssociation, 3), closed: make(chan struct{})},
				snapshots: make(map[m3ua.AssociationID]m3ua.AssociationSnapshot), closed: make(chan struct{}),
			},
			statuses: statuses[peer.Identity],
		}
		for member := range 2 {
			inventory := peers[peerIndex*2+member]
			association := &routedTestPeerAssociation{
				id: inventory.Snapshot.Association, epoch: inventory.Epoch, maximum: inventory.MaxMessageStreamID,
				reads: make(chan *m3ua.DataMessage, 2*routingRouteCount), lifetime: lifetime,
			}
			endpoint.listener.incoming <- association
			endpoint.snapshots[association.id] = inventory.Snapshot
			receiver.associations[routingTransport{SGP: peer.Identity, Association: association.id}] = association
		}
		receiver.endpoints = append(receiver.endpoints, endpoint)
	}
	created := 0
	factory := func(m3ua.EndpointConfig) (routingSetupEndpoint, error) {
		if created >= len(receiver.endpoints) {
			return nil, errors.New("unexpected extra endpoint")
		}
		created++
		return receiver.endpoints[created-1], nil
	}
	set, err := startRoutingPeerSet(lifetime, topology, addresses, factory)
	if err != nil {
		testContext.Fatal(err)
	}
	testContext.Cleanup(func() { _ = set.Close() })
	readyContext, cancelReady := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelReady()
	if err := set.WaitReady(readyContext); err != nil {
		testContext.Fatal(err)
	}
	receiver.peers = set
	source, err := newRoutingM3UAPeerPreparationSource(topology, set)
	if err != nil {
		testContext.Fatal(err)
	}
	preparation, err := newRoutingPeerPreparation(topology, source)
	if err != nil {
		testContext.Fatal(err)
	}
	receiver.control = newReceiverControl(routedAssociations, maxOutstanding)
	receiver.control.enableRouted(mode)
	receiver.operations = &routedPeerOperations{
		ctx: lifetime, topology: topology, peers: set, preparation: preparation,
		control: receiver.control, fatal: receiver.fatal,
	}
	testContext.Cleanup(func() { _ = receiver.operations.shutdown() })
	receiver.senders = make([]routingTransportDTO, len(senders))
	for index, sender := range senders {
		if receiver.senders[index], err = routingTransportDTOFromSender(sender); err != nil {
			testContext.Fatal(err)
		}
	}
	return receiver
}

func routingSetupAddresses() []*sctp.SCTPAddr {
	addresses := make([]*sctp.SCTPAddr, 4)
	for index := range addresses {
		addresses[index] = routingTestAddress("192.0.2.2", 2905+index)
	}
	return addresses
}

// prepareAndPublish commits the preparation and the eight SSNM publications.
func (receiver *routedTestReceiver) prepareAndPublish(testContext *testing.T) {
	testContext.Helper()
	if err := receiver.operations.prepare(context.Background(), receiver.senders); err != nil {
		testContext.Fatal(err)
	}
	for ordinal := uint8(0); ordinal < routingSSNMPublicationCount; ordinal++ {
		if err := receiver.operations.publish(context.Background(), ordinal); err != nil {
			testContext.Fatalf("publication %d: %v", ordinal, err)
		}
	}
}

// deliverPreflight queues the one preflight DATA per route that the sender's
// MTPTransfer would deliver, on the peer transport paired for each route.
func (receiver *routedTestReceiver) deliverPreflight(testContext *testing.T, cohort string, seed uint64) {
	testContext.Helper()
	receiver.operations.mutex.Lock()
	pairs := append([]routingAssociationPair(nil), receiver.operations.pairs...)
	receiver.operations.mutex.Unlock()
	if len(pairs) != routedAssociations {
		testContext.Fatalf("prepared pairs = %d", len(pairs))
	}
	peerIndex := make(map[m3ua.SGPIdentity]int, len(receiver.topology.Peers))
	for index, peer := range receiver.topology.Peers {
		peerIndex[peer.Identity] = index
	}
	for route := range routingRouteCount {
		pair := pairs[route%len(pairs)]
		index := peerIndex[pair.Binding.Peer.SGP]
		target := m3ua.MTPTransferPath{
			Path: receiver.topology.ASP.Routing.Paths[index/2].ID, SGP: pair.Binding.Peer.SGP, ApplicationServer: "primary",
			AS: receiver.topology.Peers[index].ApplicationServers[0].ASKey, Association: pair.Binding.SenderAssociation,
		}
		message := routingReceivedMessage(testContext, planRouteMessage(cohort, seed, uint64(route)), 128, target, pair.Binding)
		receiver.associations[pair.Binding.Peer].reads <- message
	}
}

// deliverTimed queues the timed messages of one cohort on each route's frozen
// receiver transport.
func (receiver *routedTestReceiver) deliverTimed(testContext *testing.T, specification runSpec) {
	testContext.Helper()
	receiver.control.mutex.Lock()
	paths := receiver.control.routed.paths
	receiver.control.mutex.Unlock()
	for index := range specification.Expected {
		transport, message := routedTimedMessage(testContext, paths, specification, index)
		receiver.associations[transport].reads <- message
	}
}

func (receiver *routedTestReceiver) pathsFrozen() bool {
	receiver.control.mutex.Lock()
	defer receiver.control.mutex.Unlock()
	return receiver.control.routed.paths.ready
}

func (receiver *routedTestReceiver) readyAssociations() int {
	receiver.control.mutex.Lock()
	defer receiver.control.mutex.Unlock()
	return receiver.control.readyAssociations
}

func (receiver *routedTestReceiver) endpointsClosed() int {
	closed := 0
	for _, endpoint := range receiver.endpoints {
		select {
		case <-endpoint.closed:
			closed++
		default:
		}
	}
	return closed
}

func waitRoutedTestCondition(testContext *testing.T, what string, condition func() bool) {
	testContext.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			testContext.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// completePreflight hands each association from its preflight reader to its
// timed reader in one order: the preflight readers are joined, the receiver's
// path map is frozen from the receipts, and only then is each timed reader
// started, before its association is reported ready.
func TestRoutedPeerPreflightHandsOffToTimedReadersInOrder(testContext *testing.T) {
	receiver := newRoutedTestReceiver(testContext, modeRouted)
	receiver.prepareAndPublish(testContext)
	var violations []string
	started := make([]bool, routedAssociations)
	var transports []routingTransport
	receiver.operations.mutex.Lock()
	for _, pair := range receiver.operations.pairs {
		transports = append(transports, pair.Binding.Peer)
	}
	receiver.operations.mutex.Unlock()
	receiver.operations.timedReaderStarted = func(index int) {
		if !receiver.pathsFrozen() {
			violations = append(violations, "timed reader started before the receiver froze its paths")
		}
		if ready := receiver.readyAssociations(); ready != index {
			violations = append(violations, "association reported ready before its timed reader started")
		}
		for later := index; later < len(transports); later++ {
			if active, _ := receiver.associations[transports[later]].readers(); active != 0 {
				violations = append(violations, "timed reader started while a preflight reader was still reading")
			}
		}
		started[index] = true
	}
	if err := receiver.operations.startPreflight(context.Background(), "routed-preflight", 7); err != nil {
		testContext.Fatal(err)
	}
	waitRoutedTestCondition(testContext, "every preflight reader", func() bool {
		for _, association := range receiver.associations {
			if active, _ := association.readers(); active != 1 {
				return false
			}
		}
		return true
	})
	if receiver.pathsFrozen() || receiver.readyAssociations() != 0 {
		testContext.Fatal("preflight start froze paths or reported associations ready")
	}
	receiver.deliverPreflight(testContext, "routed-preflight", 7)
	completeContext, cancelComplete := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelComplete()
	receipts, err := receiver.operations.completePreflight(completeContext)
	if err != nil || len(receipts) != routingRouteCount {
		testContext.Fatalf("receipts=%d error=%v", len(receipts), err)
	}
	if len(violations) != 0 {
		testContext.Fatalf("handoff order violated: %v", violations)
	}
	for index, value := range started {
		if !value {
			testContext.Fatalf("timed reader %d never started", index)
		}
	}
	if ready := receiver.control.ready(); !ready.Ready || ready.Associations != routedAssociations {
		testContext.Fatalf("ready = %+v", ready)
	}
	waitRoutedTestCondition(testContext, "every timed reader", func() bool {
		for _, association := range receiver.associations {
			if association.timedReads.Load() == 0 {
				return false
			}
		}
		return true
	})
	for transport, association := range receiver.associations {
		if active, overlapped := association.readers(); active != 1 || overlapped {
			testContext.Fatalf("%+v readers active=%d overlapped=%v, want exactly one reader at a time", transport, active, overlapped)
		}
	}
}

// The routed publication count only advances in step with the SSNM
// preparation's own ordinals, and the preflight starts once, after all eight
// publications, with the identity it was started with.
func TestRoutedPeerPublicationAndPreflightStartGates(testContext *testing.T) {
	testContext.Run("publication ordinal", func(testContext *testing.T) {
		receiver := newRoutedTestReceiver(testContext, modeRouted)
		if err := receiver.operations.prepare(context.Background(), receiver.senders); err != nil {
			testContext.Fatal(err)
		}
		// A publication the routed layer did not account for leaves the SSNM
		// preparation one ordinal ahead; the next routed publication must not
		// be counted as if it were the first.
		if err := receiver.operations.preparation.publish(context.Background(), 0); err != nil {
			testContext.Fatal(err)
		}
		if err := receiver.operations.publish(context.Background(), 1); err == nil || !strings.Contains(err.Error(), "publication count differs") {
			testContext.Fatalf("drifted publication error = %v", err)
		}
		if receiver.operations.publications != 0 {
			testContext.Fatalf("drifted publication was counted: %d", receiver.operations.publications)
		}
	})
	testContext.Run("before every publication", func(testContext *testing.T) {
		receiver := newRoutedTestReceiver(testContext, modeRouted)
		if err := receiver.operations.startPreflight(context.Background(), "routed-preflight", 7); err == nil {
			testContext.Fatal("preflight started before preparation")
		}
		if err := receiver.operations.prepare(context.Background(), receiver.senders); err != nil {
			testContext.Fatal(err)
		}
		for ordinal := uint8(0); ordinal < routingSSNMPublicationCount; ordinal++ {
			if err := receiver.operations.startPreflight(context.Background(), "routed-preflight", 7); err == nil {
				testContext.Fatalf("preflight started after %d of %d publications", ordinal, routingSSNMPublicationCount)
			}
			if err := receiver.operations.publish(context.Background(), ordinal); err != nil {
				testContext.Fatal(err)
			}
		}
		if err := receiver.operations.startPreflight(context.Background(), "routed-preflight", 7); err != nil {
			testContext.Fatal(err)
		}
	})
	testContext.Run("started once", func(testContext *testing.T) {
		receiver := newRoutedTestReceiver(testContext, modeRouted)
		receiver.prepareAndPublish(testContext)
		if err := receiver.operations.startPreflight(context.Background(), "routed-preflight", 7); err != nil {
			testContext.Fatal(err)
		}
		// A repeated start is refused and must not replace the identity the
		// running preflight session validates against.
		if err := receiver.operations.startPreflight(context.Background(), "other-preflight", 8); err == nil {
			testContext.Fatal("preflight started twice")
		}
		receiver.deliverPreflight(testContext, "routed-preflight", 7)
		completeContext, cancelComplete := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelComplete()
		if _, err := receiver.operations.completePreflight(completeContext); err != nil {
			testContext.Fatalf("the repeated start disturbed the running preflight: %v", err)
		}
		if err := receiver.operations.startPreflight(context.Background(), "routed-preflight", 7); err == nil {
			testContext.Fatal("preflight started after completion")
		}
	})
}

// The receiver's /routing/ preparation and its cohort control, served by the
// production handler, carry a whole preflight and then give every /reset a
// fresh per-route ledger: the second cohort starts empty and credits the same
// message indices as unique again.
func TestRoutedReceiverHTTPCompositionResetsAFreshRoutingLedger(testContext *testing.T) {
	receiver := newRoutedTestReceiver(testContext, modeRoutedDirect)
	handler, err := receiver.operations.handler()
	if err != nil {
		testContext.Fatal(err)
	}
	server := httptest.NewServer(handler)
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	preparation, err := newRoutingPreparationHTTPClient(server.URL, routedPreparationID)
	if err != nil {
		testContext.Fatal(err)
	}
	if err := preparation.Prepare(ctx, receiver.senders); err != nil {
		testContext.Fatal(err)
	}
	for ordinal := uint8(0); ordinal < routingSSNMPublicationCount; ordinal++ {
		if err := preparation.Publish(ctx, ordinal); err != nil {
			testContext.Fatalf("publication %d: %v", ordinal, err)
		}
	}
	for _, endpoint := range receiver.endpoints {
		if endpoint.reports.Load() != 2 {
			testContext.Fatalf("peer endpoint published %d DAVA reports, want one per AS scope", endpoint.reports.Load())
		}
	}
	data, err := newRoutingDataHTTPClient(server.URL, routedPreparationID)
	if err != nil {
		testContext.Fatal(err)
	}
	if err := data.Start(ctx, "routed-preflight", 7); err != nil {
		testContext.Fatal(err)
	}
	receiver.deliverPreflight(testContext, "routed-preflight", 7)
	receipts, err := data.Complete(ctx)
	if err != nil || len(receipts) != routingRouteCount {
		testContext.Fatalf("receipts=%d error=%v", len(receipts), err)
	}
	if err := waitForReady(ctx, server.URL, routedAssociations); err != nil {
		testContext.Fatal(err)
	}
	var ledgers []*routingLedger
	for _, cohort := range []string{"routed-cohort-a", "routed-cohort-b"} {
		specification := routedSpec(modeRoutedDirect)
		specification.Cohort = cohort
		if err := postJSON(ctx, server.URL+"/reset", specification); err != nil {
			testContext.Fatalf("%s reset: %v", cohort, err)
		}
		progress, err := getReceiverProgress(ctx, server.URL)
		if err != nil || progress.Delivery != (ledgerSnapshot{Missing: specification.Expected}) {
			testContext.Fatalf("%s progress after reset = %+v (%v), want an empty ledger", cohort, progress.Delivery, err)
		}
		receiver.control.mutex.Lock()
		ledgers = append(ledgers, receiver.control.routed.ledger)
		receiver.control.mutex.Unlock()
		if err := postJSON(ctx, server.URL+"/start", nil); err != nil {
			testContext.Fatal(err)
		}
		// Both cohorts deliver the same message indices; each credits them
		// once through the production timed readers.
		receiver.deliverTimed(testContext, specification)
		waitRoutedTestCondition(testContext, cohort+" deliveries", func() bool {
			progress, err := getReceiverProgress(ctx, server.URL)
			return err == nil && progress.Delivery.Unique == specification.Expected
		})
		if err := postJSON(ctx, server.URL+"/stop", nil); err != nil {
			testContext.Fatal(err)
		}
		record, err := getReceiverResult(ctx, server.URL)
		if err != nil {
			testContext.Fatal(err)
		}
		if record.Delivery.Unique != specification.Expected || record.Delivery.Missing != 0 || record.Delivery.Duplicate != 0 ||
			record.Delivery.Invalid != 0 || record.Delivery.Reordered != 0 || record.Spec.Cohort != cohort {
			testContext.Fatalf("%s delivery = %+v", cohort, record.Delivery)
		}
	}
	if len(ledgers) != 2 || ledgers[0] == nil || ledgers[0] == ledgers[1] {
		testContext.Fatalf("reset reused the routing ledger: %p", ledgers)
	}
}

// routedCancelAfterContext reports cancellation from its nth Err call on. The
// routed preparation consults the request context twice inside the SSNM
// preparation's prepare and then again at its own commit, so a context that
// turns canceled after two checks is canceled exactly between the SSNM
// preparation committing and the routed DATA controller committing.
type routedCancelAfterContext struct {
	context.Context
	remaining *atomic.Int32
}

func (ctx routedCancelAfterContext) Err() error {
	if ctx.remaining.Add(-1) >= 0 {
		return nil
	}
	return context.Canceled
}

// A /routing/prepare canceled after its DATA controller was built discards
// only that uncommitted controller: the SGP endpoints the SSNM preparation
// has committed to stay open, and the receiver still tears them down when it
// is stopped.
func TestRoutedPeerCanceledPrepareClosesOnlyItsUncommittedController(testContext *testing.T) {
	receiver := newRoutedTestReceiver(testContext, modeRouted)
	remaining := &atomic.Int32{}
	remaining.Store(2)
	err := receiver.operations.prepare(routedCancelAfterContext{Context: context.Background(), remaining: remaining}, receiver.senders)
	if err == nil || !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "prepared repeatedly or canceled") {
		testContext.Fatalf("prepare error = %v, want the routed commit to refuse the canceled request", err)
	}
	receiver.operations.preparation.mutex.Lock()
	committed := receiver.operations.preparation.prepared
	receiver.operations.preparation.mutex.Unlock()
	if !committed || receiver.operations.dataController != nil {
		testContext.Fatalf("SSNM preparation committed=%v routed controller=%v, want only the SSNM preparation committed", committed, receiver.operations.dataController)
	}
	select {
	case <-receiver.peers.Done():
		testContext.Fatal("the canceled preparation closed the shared SGP endpoints")
	case <-time.After(50 * time.Millisecond):
	}
	if closed := receiver.endpointsClosed(); closed != 0 {
		testContext.Fatalf("%d SGP endpoints closed by the canceled preparation", closed)
	}
	if _, err := receiver.peers.Inventory(); err != nil {
		testContext.Fatalf("SGP inventory after the canceled preparation: %v", err)
	}
	if err := receiver.operations.stop(context.Background()); err != nil {
		testContext.Fatal(err)
	}
	if closed := receiver.endpointsClosed(); closed != len(receiver.endpoints) {
		testContext.Fatalf("stop closed %d of %d SGP endpoints", closed, len(receiver.endpoints))
	}
}

// A committed controller still owns the endpoints: the sender's stop closes
// them and marks the receiver fatal, and shutdown after it is idempotent.
func TestRoutedPeerStopClosesTheCommittedTopology(testContext *testing.T) {
	receiver := newRoutedTestReceiver(testContext, modeRouted)
	receiver.prepareAndPublish(testContext)
	if err := receiver.operations.startPreflight(context.Background(), "routed-preflight", 7); err != nil {
		testContext.Fatal(err)
	}
	receiver.operations.mutex.Lock()
	controller := receiver.operations.dataController
	receiver.operations.mutex.Unlock()
	if err := controller.Close(); err != nil {
		testContext.Fatal(err)
	}
	select {
	case <-receiver.peers.Done():
	case <-time.After(5 * time.Second):
		testContext.Fatal("closing the committed controller left the SGP endpoints open")
	}
	if first, second := receiver.operations.stop(context.Background()), receiver.operations.shutdown(); first != nil || second != nil {
		testContext.Fatalf("stop=%v shutdown=%v", first, second)
	}
	if ready := receiver.control.ready(); ready.Ready || !strings.Contains(ready.Error, "stopped by the sender") {
		testContext.Fatalf("ready after stop = %+v", ready)
	}
	for transport, association := range receiver.associations {
		if active, _ := association.readers(); active != 0 {
			testContext.Fatalf("%+v still has %d readers after stop", transport, active)
		}
	}
}

// routedTestSenderPreparation wraps the SSNM preparation fake so a test can
// fail or contradict the sender inventory read after SSNM preparation.
type routedTestSenderPreparation struct {
	*fakeRoutingPreparationSender
	calls  int
	failAt int
	mutate func([]routingSenderInventory) []routingSenderInventory
}

func (sender *routedTestSenderPreparation) Inventory() ([]routingSenderInventory, error) {
	sender.calls++
	inventory, err := sender.fakeRoutingPreparationSender.Inventory()
	if sender.calls == sender.failAt {
		if sender.mutate == nil {
			return nil, errors.New("sender inventory unavailable")
		}
		inventory = sender.mutate(inventory)
	}
	return inventory, err
}

// routedTestSenderPeer is the receiver side of the routed preparation as the
// sender sees it: the production routing control over the SSNM fake, with
// every /routing/stop counted and checked against the sender's own
// associations.
type routedTestSenderPeer struct {
	control           *fakeRoutingPreparationControl
	senderEndpoint    *fakeRoutingEndpoint
	inventoryCalls    atomic.Int32
	failInventoryFrom int32
	stops             atomic.Int32
	stopsAfterClose   atomic.Int32
}

func (peer *routedTestSenderPeer) operations() routingControlOperations {
	return routingControlOperations{
		Inventory: func(ctx context.Context) (routingInventoryDTO, error) {
			if call := peer.inventoryCalls.Add(1); peer.failInventoryFrom > 0 && call >= peer.failInventoryFrom {
				return routingInventoryDTO{}, errors.New("peer inventory unavailable")
			}
			return peer.control.Inventory(ctx)
		},
		Prepare: peer.control.Prepare,
		Publish: peer.control.Publish,
		Stop: func(context.Context) error {
			peer.stops.Add(1)
			select {
			case <-peer.senderEndpoint.closed:
				peer.stopsAfterClose.Add(1)
			default:
			}
			return nil
		},
	}
}

// Every failure between starting the sender topology and the first cohort
// sends /routing/stop to the receiver, before the sender closes its own
// associations, so the receiver never waits on a sender that has given up.
// The exits past the sender DATA plane need concrete M3UA associations and
// are covered by the loopback test of the production entry points; they
// share the one deferred stop exercised here.
func TestRoutedSenderStopsThePeerOnEveryPreparationFailure(testContext *testing.T) {
	for _, test := range []struct {
		name          string
		setup         func(*routedSenderEnvironment, *routedTestSenderPeer, *routingPreparationFixture, *fakeRoutingFactory, *routedTestSenderPreparation)
		timeout       time.Duration
		want          string
		setClosedByIt bool
	}{
		{name: "start", want: "start routed associations", setup: func(environment *routedSenderEnvironment, _ *routedTestSenderPeer, _ *routingPreparationFixture, factory *fakeRoutingFactory, _ *routedTestSenderPreparation) {
			factory.failAt, factory.err = 1, errors.New("endpoint unavailable")
		}},
		{name: "establish", want: "establish routed associations", setClosedByIt: true, setup: func(_ *routedSenderEnvironment, _ *routedTestSenderPeer, _ *routingPreparationFixture, factory *fakeRoutingFactory, _ *routedTestSenderPreparation) {
			factory.endpoints[0].failDial, factory.endpoints[0].dialErr = 3, errors.New("dial refused")
		}},
		{name: "preparation adapter", want: "does not expose SSNM", setup: func(environment *routedSenderEnvironment, _ *routedTestSenderPeer, _ *routingPreparationFixture, _ *fakeRoutingFactory, _ *routedTestSenderPreparation) {
			environment.preparation = nil
		}},
		// The run context's end also ends the sender topology it owns, so
		// the associations may close before the stop is sent.
		{name: "peer never ready", want: context.DeadlineExceeded.Error(), timeout: 300 * time.Millisecond, setClosedByIt: true, setup: func(_ *routedSenderEnvironment, peer *routedTestSenderPeer, _ *routingPreparationFixture, _ *fakeRoutingFactory, _ *routedTestSenderPreparation) {
			peer.failInventoryFrom = 1
		}},
		{name: "ssnm", want: "routed SSNM preparation", setup: func(_ *routedSenderEnvironment, _ *routedTestSenderPeer, fixture *routingPreparationFixture, _ *fakeRoutingFactory, _ *routedTestSenderPreparation) {
			fixture.control.publishErr[3] = errors.New("publication refused")
		}},
		{name: "peer inventory after ssnm", want: "routed peer inventory", setup: func(_ *routedSenderEnvironment, peer *routedTestSenderPeer, _ *routingPreparationFixture, _ *fakeRoutingFactory, _ *routedTestSenderPreparation) {
			peer.failInventoryFrom = 3
		}},
		{name: "sender inventory after ssnm", want: "sender inventory unavailable", setup: func(_ *routedSenderEnvironment, _ *routedTestSenderPeer, _ *routingPreparationFixture, _ *fakeRoutingFactory, sender *routedTestSenderPreparation) {
			sender.failAt = 2
		}},
		{name: "pairing", want: "routing pairing requires", setup: func(_ *routedSenderEnvironment, _ *routedTestSenderPeer, _ *routingPreparationFixture, _ *fakeRoutingFactory, sender *routedTestSenderPreparation) {
			sender.failAt = 2
			sender.mutate = func(inventory []routingSenderInventory) []routingSenderInventory { return inventory[1:] }
		}},
		{name: "sender plane", want: "MTPTransfer", setup: func(*routedSenderEnvironment, *routedTestSenderPeer, *routingPreparationFixture, *fakeRoutingFactory, *routedTestSenderPreparation) {
		}},
	} {
		testContext.Run(test.name, func(testContext *testing.T) {
			fixture := newRoutingPreparationFixture(testContext)
			_, _, _, factory := routingSetupFixture(testContext)
			peer := &routedTestSenderPeer{control: fixture.control, senderEndpoint: factory.endpoints[0]}
			sender := &routedTestSenderPreparation{fakeRoutingPreparationSender: fixture.sender}
			environment := routedSenderEnvironment{
				factory:     factory.create,
				preparation: func(*routingSenderSet) (routingPreparationSender, error) { return sender, nil },
			}
			test.setup(&environment, peer, &fixture, factory, sender)
			routing, err := newRoutingControl(routedPreparationID, peer.operations())
			if err != nil {
				testContext.Fatal(err)
			}
			server := httptest.NewServer(routing.handler())
			defer server.Close()
			config, err := parseConfig(routedSenderArguments(modeRouted, "-sctp-address=192.0.2.2:2905", "-local-address=192.0.2.1:0", "-peer-control="+server.URL))
			if err != nil {
				testContext.Fatal(err)
			}
			timeout := test.timeout
			if timeout == 0 {
				timeout = 10 * time.Second
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			result, err := runRoutedSenderWith(ctx, config, environment)
			if err == nil || !strings.Contains(err.Error(), test.want) || result.Measurement != nil || result.Warmup != nil {
				testContext.Fatalf("warm-up=%v measurement=%v error=%v, want a %q preparation failure", result.Warmup, result.Measurement, err, test.want)
			}
			if peer.stops.Load() == 0 {
				testContext.Fatalf("the sender failed at %s without stopping the receiver's routed topology", test.name)
			}
			if test.name != "start" && !test.setClosedByIt && peer.stopsAfterClose.Load() != 0 {
				testContext.Fatal("the sender closed its associations before stopping the receiver")
			}
			if test.name != "start" {
				select {
				case <-factory.endpoints[0].closed:
				default:
					testContext.Fatal("the sender left its associations open after a failed preparation")
				}
			}
		})
	}
}
