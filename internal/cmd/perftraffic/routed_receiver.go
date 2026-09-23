package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gomaja/go-m3ua"
)

// routedPreparationID names the one routed preparation a receiver serves. A
// receiver hosts one routed topology for one sender process, so a fixed
// identity is enough to reject requests meant for a different preparation
// protocol.
const routedPreparationID = "perftraffic-routed"

// routedReceiveState is the SGP receiver's view of the routed workload: the
// mode it serves, the per-route paths frozen from its own preflight receipts,
// and the active cohort's per-route ledger.
type routedReceiveState struct {
	mode   string
	paths  routingPathMap
	ledger *routingLedger
	// failover is the SGP failure trial state, nil without -sgp-failure.
	failover *failoverReceiver
}

func (control *receiverControl) enableRouted(mode string) {
	control.mutex.Lock()
	defer control.mutex.Unlock()
	control.routed = &routedReceiveState{mode: mode}
}

// freezeRoutes records the receiver's per-route path map once. The map is
// never replaced afterwards, so validation may read it without the mutex.
func (control *receiverControl) freezeRoutes(paths routingPathMap) error {
	control.mutex.Lock()
	defer control.mutex.Unlock()
	switch {
	case control.routed == nil:
		return errors.New("receiver does not host the routed topology")
	case !paths.ready:
		return errors.New("routed path map is not frozen")
	case control.routed.paths.ready:
		return errors.New("routed paths are already frozen")
	}
	control.routed.paths = paths
	return nil
}

func (control *receiverControl) validateRoutedSpecLocked(specification runSpec) error {
	switch {
	case control.routed == nil:
		return fmt.Errorf("%w: this receiver does not host the routed topology", errInvalidRunSpec)
	case specification.Mode != control.routed.mode:
		return fmt.Errorf("%w: this receiver hosts %s, not %s", errInvalidRunSpec, control.routed.mode, specification.Mode)
	case !control.routed.paths.ready:
		return errors.New("routed paths are not frozen: preflight has not completed")
	case specification.Associations != routedAssociations || specification.Payload != workloadMix ||
		specification.Direction != directionASPToSGP || specification.Initiation != initiationASPDial:
		return fmt.Errorf("%w: routed cohorts use eight associations, the mix payload, asp-to-sgp and asp-dial", errInvalidRunSpec)
	}
	return nil
}

// deliveryLocked returns the active cohort's delivery ledger snapshot, from
// the per-route ledger on a routed receiver and the direct ledger otherwise.
func (control *receiverControl) deliveryLocked() (ledgerSnapshot, bool) {
	if control.routed != nil {
		if control.routed.ledger == nil {
			return ledgerSnapshot{}, false
		}
		return control.routed.ledger.snapshot(), true
	}
	if control.ledger == nil {
		return ledgerSnapshot{}, false
	}
	return control.ledger.snapshot(), true
}

// recordRouted classifies one routed arrival. It follows record exactly,
// except that validation is route-aware: the arrival must be on the transport,
// epoch, AS scope and stream frozen for its route, and its flow is the route.
func (control *receiverControl) recordRouted(transport routingTransport, message *m3ua.DataMessage) recordOutcome {
	control.mutex.Lock()
	if control.phase == receiverStopped {
		control.lateAfterStop++
		control.mutex.Unlock()
		return recordIgnored
	}
	if control.phase != receiverMeasuring || control.routed == nil || control.routed.ledger == nil {
		control.mutex.Unlock()
		return recordIgnored
	}
	specification := control.spec
	generation := control.generation
	paths := &control.routed.paths
	failover := control.routed.failover
	control.mutex.Unlock()

	var identity routingIdentity
	var alternative bool
	var err error
	if failover != nil && specification.SGPFailure != nil {
		identity, alternative, err = failover.validate(message, transport, specification, paths)
	} else {
		identity, err = validateRouteMessage(message, transport, specification.Cohort, specification.Seed, specification.Payload, paths)
	}
	var received time.Time
	if specification.Clock == nil {
		received = control.now()
	}
	control.mutex.Lock()
	defer control.mutex.Unlock()
	ledger := control.routed.ledger
	if control.phase != receiverMeasuring || control.generation != generation {
		if ledger != nil {
			ledger.snapshotData.Invalid++
		}
		return recordIgnored
	}
	if err != nil {
		ledger.snapshotData.Invalid++
		return recordInvalid
	}
	if alternative && !failover.claimAlternativeLocked(identity.Route, transport) {
		ledger.snapshotData.Invalid++
		return recordInvalid
	}
	if control.firstArrival.IsZero() {
		control.firstArrival = received
	}
	reorderedBefore := ledger.snapshotData.Reordered
	if ledger.record(identity) != ledgerUnique {
		return recordNotUnique
	}
	if failover != nil && specification.SGPFailure != nil {
		failover.uniqueLocked(ledger, identity, reorderedBefore, alternative, transport)
	}
	if control.spec.Clock != nil {
		sharedReceived, clockErr := control.sharedNowLocked()
		if clockErr != nil || sharedReceived > control.spec.Clock.End+int64(control.spec.Drain)-control.spec.Clock.Domain.Resolution {
			ledger.snapshotData.Unique--
			ledger.snapshotData.Invalid++
			if clockErr == nil {
				control.fatal = "delivery exceeds shared drain deadline"
			}
			return recordInvalid
		}
		control.classifySharedDeliveryLocked(sharedReceived)
		if failover != nil && specification.SGPFailure != nil {
			failover.deliveredLocked(specification, sharedReceived, transport, identity)
		}
		return recordUnique
	}
	if received.Before(control.firstArrival.Add(control.spec.Duration)) {
		control.uniqueMeasurement++
	} else {
		control.uniqueDrain++
	}
	return recordUnique
}

// freezeRoutingPeerPaths builds the receiver's own per-route path map from the
// preflight DATA it actually received: each route's peer transport and epoch,
// the preferred AS scope of that SGP, and the sender binding paired with that
// transport. The sender independently checks the same receipts against its
// MTPTransfer results, so both ends validate timed traffic against one map.
func freezeRoutingPeerPaths(topology routingTopology, pairs []routingAssociationPair, receipts []routingDataReceiptDTO, cohort string, seed uint64) (routingPathMap, error) {
	if topology.ASP == nil || topology.ASP.Routing == nil || len(topology.ASP.Routing.Paths) != 2 || len(topology.ASP.Routing.Paths[0].ApplicationServers) != 2 {
		return routingPathMap{}, errors.New("routing topology is incomplete")
	}
	preferred := topology.ASP.Routing.Paths[0].ApplicationServers[0]
	expected, err := newRoutingTopology(preferred)
	if err != nil || !reflect.DeepEqual(topology, expected) {
		return routingPathMap{}, errors.New("routing topology differs from the frozen workload")
	}
	if len(pairs) != routedAssociations || len(receipts) != routingRouteCount || cohort == "" {
		return routingPathMap{}, errors.New("routing peer preflight inventory is incomplete")
	}
	peerIndex := make(map[m3ua.SGPIdentity]int, len(topology.Peers))
	for index, peer := range topology.Peers {
		peerIndex[peer.Identity] = index
	}
	bindings := make(map[routingTransport]routingBinding, len(pairs))
	for _, pair := range pairs {
		if _, known := peerIndex[pair.Binding.Peer.SGP]; !known || pair.Binding.SenderAssociation == 0 || pair.Binding.Peer.Association == 0 ||
			pair.Binding.PeerEpoch == 0 || pair.Binding.MaxMessageStreamID == 0 {
			return routingPathMap{}, errors.New("routing peer binding is invalid")
		}
		if _, duplicate := bindings[pair.Binding.Peer]; duplicate {
			return routingPathMap{}, errors.New("routing peer binding is duplicated")
		}
		bindings[pair.Binding.Peer] = pair.Binding
	}
	scopeIndex := 0
	if preferred == "secondary" {
		scopeIndex = 1
	}
	var paths routingPathMap
	var seen [routingRouteCount]bool
	used := make(map[m3ua.AssociationID]bool, len(pairs))
	for _, receipt := range receipts {
		if receipt.Route >= routingRouteCount || seen[receipt.Route] {
			return routingPathMap{}, errors.New("routing peer receipt route is invalid or duplicated")
		}
		transport := routingTransport{SGP: receipt.SGP, Association: receipt.Association}
		binding, known := bindings[transport]
		if !known || receipt.Epoch != binding.PeerEpoch {
			return routingPathMap{}, fmt.Errorf("routing peer receipt for route %d arrived on an unpaired transport or epoch", receipt.Route)
		}
		index := peerIndex[receipt.SGP]
		key := topology.Peers[index].ApplicationServers[scopeIndex].ASKey
		if receipt.AS != key {
			return routingPathMap{}, fmt.Errorf("routing peer receipt for route %d is not in the preferred AS scope", receipt.Route)
		}
		path := routingResolvedPath{
			Target: m3ua.MTPTransferPath{
				Path: topology.ASP.Routing.Paths[index/2].ID, SGP: receipt.SGP, ApplicationServer: preferred,
				AS: key, Association: binding.SenderAssociation,
			},
			Binding: binding,
		}
		identity, err := validateRoutingArrival(routingDataMessageFromReceipt(receipt), transport, cohort, seed, workload128, path)
		if err != nil {
			return routingPathMap{}, fmt.Errorf("routing peer receipt for route %d: %w", receipt.Route, err)
		}
		if identity.Route != receipt.Route || identity.Sequence != 0 {
			return routingPathMap{}, errors.New("routing peer receipt identity differs from its route")
		}
		paths.paths[receipt.Route] = path
		seen[receipt.Route] = true
		used[binding.SenderAssociation] = true
	}
	if len(used) != routedAssociations {
		return routingPathMap{}, errors.New("routing peer preflight did not use all eight associations")
	}
	paths.ready = true
	return paths, nil
}

// routedControlHandler serves the routing preparation operations under
// /routing/ and every cohort operation from the ordinary receiver control, so
// a routed run drives its cohorts exactly like the direct modes.
func routedControlHandler(receiver, routing http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if strings.HasPrefix(request.URL.Path, "/routing/") {
			routing.ServeHTTP(writer, request)
			return
		}
		receiver.ServeHTTP(writer, request)
	})
}

// routedPeerOperations owns the receiver's routed preparation: SSNM
// availability publication, the MTPTransfer preflight collection, and the
// switch to timed readers once the receiver's path map is frozen.
type routedPeerOperations struct {
	ctx         context.Context
	topology    routingTopology
	peers       *routingPeerSet
	preparation *routingPeerPreparation
	control     *receiverControl
	fatal       chan<- error
	// timedReaderStarted observes the preflight-to-timed handoff; it is nil
	// outside tests.
	timedReaderStarted func(index int)

	mutex          sync.Mutex
	dataController *routingDataPeerController
	pairs          []routingAssociationPair
	associations   []routingDataAssociation
	publications   int
	preflightSeed  uint64
	preflightName  string
	preflightDone  bool
	closeOnce      sync.Once
	closeErr       error
}

func (operations *routedPeerOperations) controlOperations() routingControlOperations {
	return routingControlOperations{
		Inventory: operations.preparation.inventory,
		Prepare:   operations.prepare,
		Publish:   operations.publish,
		Stop:      operations.stop,
	}
}

func (operations *routedPeerOperations) dataOperations() routingDataControlOperations {
	return routingDataControlOperations{Start: operations.startPreflight, Complete: operations.completePreflight}
}

// handler serves the routing preparation operations under /routing/ beside
// the receiver's ordinary cohort control.
func (operations *routedPeerOperations) handler() (http.Handler, error) {
	routing, err := newRoutingControl(routedPreparationID, operations.controlOperations())
	if err != nil {
		return nil, err
	}
	routingHandler, err := newRoutingDataControlHandler(routing.handler(), routedPreparationID, operations.dataOperations())
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.Handle(routedPeerStatePath, peerStateHandler(func() (routeReferenceState, error) {
		return capturePeerRouteReferenceState(operations.peers)
	}))
	mux.Handle("/", routedControlHandler(operations.control.handler(), routingHandler))
	return mux, nil
}

func (operations *routedPeerOperations) prepare(ctx context.Context, values []routingTransportDTO) error {
	if err := operations.preparation.prepare(ctx, values); err != nil {
		return err
	}
	senders, err := routingSenderInventoryFromDTOs(values)
	if err != nil {
		return err
	}
	peers, err := operations.peers.Inventory()
	if err != nil {
		return err
	}
	pairs, err := pairRoutingInventory(operations.topology, senders, peers)
	if err != nil {
		return err
	}
	entries, err := operations.peers.inventoryEntries()
	if err != nil {
		return err
	}
	byTransport := make(map[routingTransport]routingDataAssociation, len(entries))
	for _, entry := range entries {
		association, valid := entry.association.(routingDataAssociation)
		transport := routingTransport{SGP: entry.identity, Association: entry.association.ID()}
		if !valid || byTransport[transport] != nil {
			return errors.New("routed peer association does not expose unique DATA operations")
		}
		byTransport[transport] = association
	}
	associations := make([]routingDataAssociation, len(pairs))
	for index, pair := range pairs {
		if associations[index] = byTransport[pair.Binding.Peer]; associations[index] == nil {
			return errors.New("routed peer association is not paired")
		}
	}
	// The DATA plane owns the shared SGP endpoints only once this preparation
	// is committed. A controller discarded before that — a canceled or
	// repeated prepare — closes only itself, never the endpoints that the SSNM
	// preparation and any committed controller still use.
	var committed atomic.Bool
	plane, err := newRoutingDataPeerPlane(associations, pairs, func() error {
		if !committed.Load() {
			return nil
		}
		return operations.peers.Close()
	})
	if err != nil {
		return err
	}
	controller, err := newRoutingDataPeerController(operations.ctx, plane)
	if err != nil {
		return err
	}
	operations.mutex.Lock()
	defer operations.mutex.Unlock()
	if operations.dataController != nil || ctx.Err() != nil {
		return errors.Join(ctx.Err(), controller.Close(), errors.New("routed peer was prepared repeatedly or canceled"))
	}
	committed.Store(true)
	operations.dataController = controller
	operations.pairs = pairs
	operations.associations = associations
	return nil
}

func (operations *routedPeerOperations) publish(ctx context.Context, ordinal uint8) error {
	if err := operations.preparation.publish(ctx, ordinal); err != nil {
		return err
	}
	operations.mutex.Lock()
	defer operations.mutex.Unlock()
	if int(ordinal) != operations.publications {
		return errors.New("routed SSNM publication count differs")
	}
	operations.publications++
	return nil
}

func (operations *routedPeerOperations) startPreflight(ctx context.Context, cohort string, seed uint64) error {
	operations.mutex.Lock()
	controller := operations.dataController
	ready := operations.publications == routingSSNMPublicationCount && !operations.preflightDone && operations.preflightName == ""
	if ready {
		operations.preflightName, operations.preflightSeed = cohort, seed
	}
	operations.mutex.Unlock()
	if controller == nil || !ready {
		return errors.New("routed preflight started before SSNM preparation or repeated")
	}
	return controller.start(ctx, cohort, seed)
}

// completePreflight collects the thousand preflight receipts, freezes the
// receiver's path map from them, and only then starts the timed readers and
// reports the associations ready: the preflight session's own readers have
// been joined by the time Complete returns, so no association is ever read by
// two loops.
func (operations *routedPeerOperations) completePreflight(ctx context.Context) ([]routingDataReceiptDTO, error) {
	operations.mutex.Lock()
	controller := operations.dataController
	cohort, seed := operations.preflightName, operations.preflightSeed
	pairs := append([]routingAssociationPair(nil), operations.pairs...)
	associations := append([]routingDataAssociation(nil), operations.associations...)
	operations.mutex.Unlock()
	if controller == nil || cohort == "" {
		return nil, errors.New("routed preflight has not started")
	}
	receipts, err := controller.complete(ctx)
	if err != nil {
		return nil, err
	}
	paths, err := freezeRoutingPeerPaths(operations.topology, pairs, receipts, cohort, seed)
	if err != nil {
		return nil, err
	}
	if err := operations.control.freezeRoutes(paths); err != nil {
		return nil, err
	}
	if err := operations.control.freezeFailover(operations.topology, pairs, associations); err != nil {
		return nil, err
	}
	writeRoutedPathDiagnostic(&paths)
	operations.mutex.Lock()
	operations.preflightDone = true
	operations.mutex.Unlock()
	for index, association := range associations {
		operations.startTimedReader(index, pairs[index].Binding.Peer, association)
		operations.control.setAssociationReady(index, int(association.MaxMessageStreamID()))
	}
	return receipts, nil
}

// startTimedReader starts the one timed reader of an association whose
// preflight reader has been joined.
func (operations *routedPeerOperations) startTimedReader(index int, transport routingTransport, association routingDataAssociation) {
	if operations.timedReaderStarted != nil {
		operations.timedReaderStarted(index)
	}
	go readRoutedAssociation(operations.ctx, index, transport, association, operations.control, operations.fatal)
}

// stop is the sender's request to tear the routed topology down after a
// failed preparation. The receiver cannot serve a cohort afterwards.
func (operations *routedPeerOperations) stop(context.Context) error {
	operations.control.setFatal("routed peer topology was stopped by the sender before measurement completed")
	return operations.shutdown()
}

// shutdown closes the preflight controller and the four SGP endpoints once.
func (operations *routedPeerOperations) shutdown() error {
	operations.closeOnce.Do(func() {
		operations.mutex.Lock()
		controller := operations.dataController
		operations.mutex.Unlock()
		var controllerErr error
		if controller != nil {
			controllerErr = controller.Close()
		}
		stopContext, cancel := context.WithTimeout(context.Background(), routingControlTimeout)
		defer cancel()
		operations.closeErr = errors.Join(controllerErr, operations.preparation.stop(stopContext))
	})
	return operations.closeErr
}

// routedPathShare is how many of the 1,000 routes one association carries in
// the frozen path map.
type routedPathShare struct {
	SGP               m3ua.SGPIdentity   `json:"sgp"`
	Association       m3ua.AssociationID `json:"association"`
	SenderAssociation m3ua.AssociationID `json:"sender_association"`
	AS                m3ua.ASKey         `json:"as"`
	Routes            int                `json:"routes"`
}

// routedPathShares summarizes the frozen path map per association, in first
// route order. Every route is validated on exactly one of these transports,
// so a passing cohort delivered each association's share on that association.
func routedPathShares(paths *routingPathMap) []routedPathShare {
	var shares []routedPathShare
	index := make(map[routingTransport]int)
	for route := range routingRouteCount {
		path, err := paths.path(uint16(route))
		if err != nil {
			return nil
		}
		position, known := index[path.Binding.Peer]
		if !known {
			position = len(shares)
			index[path.Binding.Peer] = position
			shares = append(shares, routedPathShare{
				SGP: path.Binding.Peer.SGP, Association: path.Binding.Peer.Association,
				SenderAssociation: path.Binding.SenderAssociation, AS: path.Target.AS,
			})
		}
		shares[position].Routes++
	}
	return shares
}

// writeRoutedPathDiagnostic emits the frozen per-association route shares as
// one JSON line beside the startup diagnostics, outside the result record.
func writeRoutedPathDiagnostic(paths *routingPathMap) {
	_ = json.NewEncoder(startupDiagnosticWriter).Encode(map[string]any{"routed_paths_frozen": routedPathShares(paths)})
}

func readRoutedAssociation(ctx context.Context, index int, transport routingTransport, association routingDataAssociation, control *receiverControl, fatal chan<- error) {
	for {
		message, err := association.ReadData(ctx)
		if err != nil {
			if ctx.Err() != nil || control.isStopped() && errors.Is(err, m3ua.ErrNotEstablished) {
				return
			}
			if control.failoverReaderEnded(transport, err) {
				return
			}
			phase := control.phaseName()
			writeStartupDiagnostic("read-fatal", index, phase, err)
			nonblockingError(fatal, readFatalError(index, phase, err))
			return
		}
		control.recordRouted(transport, message)
	}
}

// runRoutedReceiver hosts the four SGP endpoints of the routed topology, serves
// the routing preparation operations beside the ordinary cohort control, and
// validates every timed arrival against the path frozen for its route.
func runRoutedReceiver(ctx context.Context, config commandConfig) (runRecord, error) {
	control := newReceiverControl(config.Associations, maxOutstanding)
	control.enableSharedClock(config.SameHostClock)
	if control.fatal != "" {
		return runRecord{}, errors.New(control.fatal)
	}
	control.cpuStatPath = config.CPUStatPath
	control.enableRouted(config.Mode)
	httpListener, err := net.Listen("tcp", config.ControlAddress)
	if err != nil {
		return runRecord{}, fmt.Errorf("startup control-bind: %w", err)
	}
	defer func() { _ = httpListener.Close() }()
	topology, err := newRoutingTopology("primary")
	if err != nil {
		return runRecord{}, fmt.Errorf("startup routed topology: %w", err)
	}
	addresses, err := routedPeerAddresses(config.SCTPAddress)
	if err != nil {
		return runRecord{}, fmt.Errorf("startup resolve-listen-address: %w", err)
	}
	lifetime, cancel := context.WithCancel(ctx)
	defer cancel()
	control.enableFailover(lifetime, config.SGPFailure)
	peers, err := startRoutingPeerSet(lifetime, topology, addresses, nil)
	if err != nil {
		return runRecord{}, fmt.Errorf("startup listen: %w", err)
	}
	defer func() { _ = peers.Close() }()
	source, err := newRoutingM3UAPeerPreparationSource(topology, peers)
	if err != nil {
		return runRecord{}, fmt.Errorf("startup routed preparation: %w", err)
	}
	preparation, err := newRoutingPeerPreparation(topology, source)
	if err != nil {
		return runRecord{}, fmt.Errorf("startup routed preparation: %w", err)
	}
	fatal := make(chan error, 1)
	operations := &routedPeerOperations{ctx: lifetime, topology: topology, peers: peers, preparation: preparation, control: control, fatal: fatal}
	handler, err := operations.handler()
	if err != nil {
		return runRecord{}, fmt.Errorf("startup routed control: %w", err)
	}
	httpServer := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second}
	httpFailure := make(chan error, 1)
	go func() {
		serveErr := httpServer.Serve(httpListener)
		if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			httpFailure <- serveErr
		}
	}()
	go sampleReceiver(lifetime, control)
	select {
	case <-ctx.Done():
	case err = <-fatal:
		control.setFatal(err.Error())
	case err = <-httpFailure:
		control.setFatal("HTTP control server: " + err.Error())
	case <-peers.Done():
		err = errors.New("routed SGP endpoints closed")
		control.setFatal(err.Error())
	}
	shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelShutdown()
	_ = httpServer.Shutdown(shutdownContext)
	cancel()
	_ = operations.shutdown()
	record := control.result()
	record.Manifest = currentManifest(config.Outstanding, config.Initiation)
	record.Manifest.FlowCount = routingRouteCount
	return record, err
}
