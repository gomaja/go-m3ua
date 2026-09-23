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
