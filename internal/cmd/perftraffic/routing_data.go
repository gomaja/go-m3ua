package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/gomaja/go-m3ua"
	"github.com/gomaja/go-m3ua/messages/params"
)

type routingDataAssociation interface {
	routingSetupAssociation
	ReadData(context.Context) (*m3ua.DataMessage, error)
	WriteData(m3ua.DataRequest) (int, error)
	DataQueueStats() m3ua.DataQueueStats
}

type routingDataTransferEndpoint interface {
	MTPTransfer(m3ua.MTPTransferRequest) (m3ua.MTPTransferResult, error)
}

type routingDataControlClient interface {
	Start(context.Context, string, uint64) error
	Complete(context.Context) ([]routingDataReceiptDTO, error)
	Stop(context.Context) error
}

type routingDataReceiptDTO struct {
	Route                uint16                     `json:"route"`
	SGP                  m3ua.SGPIdentity           `json:"sgp"`
	Association          m3ua.AssociationID         `json:"association"`
	Epoch                uint64                     `json:"epoch"`
	ProtocolData         params.ProtocolDataPayload `json:"protocol_data"`
	NetworkAppearance    uint32                     `json:"network_appearance"`
	NetworkAppearanceSet bool                       `json:"network_appearance_set"`
	RoutingContext       uint32                     `json:"routing_context"`
	RoutingContextSet    bool                       `json:"routing_context_set"`
	AS                   m3ua.ASKey                 `json:"as"`
	Stream               uint16                     `json:"stream"`
	CorrelationID        uint32                     `json:"correlation_id"`
	CorrelationIDSet     bool                       `json:"correlation_id_set"`
}

type routingDataSenderPlane struct {
	endpoint     routingDataTransferEndpoint
	associations map[m3ua.AssociationID]routingDataAssociation
	epochs       map[m3ua.AssociationID]uint64
	bindings     []routingBinding
	close        func() error
}

type routingDataPeerPlane struct {
	associations map[routingTransport]routingDataAssociation
	bindings     []routingBinding
	close        func() error
}

type routingDirectWriter struct {
	paths        routingPathMap
	associations map[m3ua.AssociationID]routingDataAssociation
	epochs       map[m3ua.AssociationID]uint64
	admission    chan struct{}
}

func newRoutingDataSenderPlane(endpoint routingDataTransferEndpoint, associations []routingDataAssociation, pairs []routingAssociationPair, closePlane func() error) (*routingDataSenderPlane, error) {
	if endpoint == nil || closePlane == nil || len(associations) != 8 || len(pairs) != 8 {
		return nil, errors.New("routing DATA sender inventory is incomplete")
	}
	byID := make(map[m3ua.AssociationID]routingDataAssociation, len(associations))
	for _, association := range associations {
		if association == nil || association.ID() == 0 || byID[association.ID()] != nil {
			return nil, errors.New("routing DATA sender association is missing or duplicated")
		}
		byID[association.ID()] = association
	}
	epochs := make(map[m3ua.AssociationID]uint64, len(pairs))
	bindings := make([]routingBinding, len(pairs))
	for index, pair := range pairs {
		association := byID[pair.Binding.SenderAssociation]
		if association == nil || pair.SenderEpoch == 0 || association.Epoch() != pair.SenderEpoch ||
			association.MaxMessageStreamID() != pair.Binding.MaxMessageStreamID || epochs[pair.Binding.SenderAssociation] != 0 {
			return nil, errors.New("routing DATA sender association differs from paired inventory")
		}
		epochs[pair.Binding.SenderAssociation] = pair.SenderEpoch
		bindings[index] = pair.Binding
	}
	return &routingDataSenderPlane{endpoint: endpoint, associations: byID, epochs: epochs, bindings: bindings, close: closePlane}, nil
}

func newRoutingDataPeerPlane(associations []routingDataAssociation, pairs []routingAssociationPair, closePlane func() error) (*routingDataPeerPlane, error) {
	if closePlane == nil || len(associations) != 8 || len(pairs) != 8 {
		return nil, errors.New("routing DATA peer inventory is incomplete")
	}
	byTransport := make(map[routingTransport]routingDataAssociation, len(associations))
	bindings := make([]routingBinding, len(pairs))
	for index, pair := range pairs {
		association := associations[index]
		transport := pair.Binding.Peer
		if association == nil || transport.SGP.SignallingGateway == "" || transport.SGP.SignallingGatewayProcess == "" ||
			transport.Association == 0 || association.ID() != transport.Association || association.Epoch() != pair.Binding.PeerEpoch || byTransport[transport] != nil {
			return nil, errors.New("routing DATA peer association differs from paired inventory")
		}
		byTransport[transport] = association
		bindings[index] = pair.Binding
	}
	return &routingDataPeerPlane{associations: byTransport, bindings: bindings, close: closePlane}, nil
}

func prepareRoutingData(ctx context.Context, topology routingTopology, cohort string, seed uint64, outstanding int, sender *routingDataSenderPlane, control routingDataControlClient) (routingPathMap, *routingDirectWriter, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if sender == nil || sender.endpoint == nil || sender.close == nil || control == nil || cohort == "" || outstanding < 1 || outstanding > maxOutstanding {
		return routingPathMap{}, nil, errors.New("routing DATA preparation dependencies or admission are invalid")
	}
	var cleanupOnce sync.Once
	var cleanupErr error
	cleanup := func() error {
		cleanupOnce.Do(func() {
			cleanupContext, cancelCleanup := context.WithTimeout(context.Background(), routingControlTimeout)
			stopErr := control.Stop(cleanupContext)
			cancelCleanup()
			cleanupErr = errors.Join(stopErr, sender.close())
		})
		return cleanupErr
	}
	operationDone := make(chan struct{})
	watcherDone := make(chan error, 1)
	go func() {
		select {
		case <-ctx.Done():
			watcherDone <- errors.Join(ctx.Err(), cleanup())
		case <-operationDone:
			watcherDone <- nil
		}
	}()
	finish := func(cause error) error {
		if cause != nil {
			cause = errors.Join(cause, cleanup())
		}
		close(operationDone)
		watcherErr := <-watcherDone
		if cause == nil && ctx.Err() != nil {
			cause = errors.Join(ctx.Err(), cleanup())
		}
		return errors.Join(cause, watcherErr)
	}
	if err := control.Start(ctx, cohort, seed); err != nil {
		return routingPathMap{}, nil, finish(err)
	}
	observations := make([]routingPreflight, routingRouteCount)
	for route := range routingRouteCount {
		if err := ctx.Err(); err != nil {
			return routingPathMap{}, nil, finish(err)
		}
		identity := planRouteMessage(cohort, seed, uint64(route))
		payload, err := buildRoutePayload(identity, 128)
		if err != nil {
			return routingPathMap{}, nil, finish(err)
		}
		protocolData, err := routeProtocolData(uint16(route), payload)
		if err != nil {
			return routingPathMap{}, nil, finish(err)
		}
		result, transferErr := sender.endpoint.MTPTransfer(m3ua.MTPTransferRequest{
			MTPRoute: m3ua.MTPRouteID(fmt.Sprintf("route-%04d", route)), ProtocolData: &protocolData,
		})
		observations[route] = routingPreflight{Route: uint16(route), Result: result, Err: transferErr}
		if transferErr != nil {
			return routingPathMap{}, nil, finish(fmt.Errorf("routing preflight route %d: %w", route, transferErr))
		}
	}
	receipts, err := control.Complete(ctx)
	if err != nil {
		return routingPathMap{}, nil, finish(err)
	}
	if len(receipts) != routingRouteCount {
		return routingPathMap{}, nil, finish(errors.New("routing DATA completion does not contain exactly one thousand receipts"))
	}
	seen := make([]bool, routingRouteCount)
	for _, receipt := range receipts {
		if receipt.Route >= routingRouteCount || seen[receipt.Route] {
			return routingPathMap{}, nil, finish(errors.New("routing DATA receipt route is invalid or duplicated"))
		}
		seen[receipt.Route] = true
		observations[receipt.Route].Transport = routingTransport{SGP: receipt.SGP, Association: receipt.Association}
		observations[receipt.Route].Message = routingDataMessageFromReceipt(receipt)
	}
	paths, err := freezeRoutingPaths(topology, sender.bindings, observations, cohort, seed)
	if err != nil {
		return routingPathMap{}, nil, finish(err)
	}
	writer := &routingDirectWriter{
		paths: paths, associations: make(map[m3ua.AssociationID]routingDataAssociation, len(sender.associations)),
		epochs: make(map[m3ua.AssociationID]uint64, len(sender.epochs)), admission: make(chan struct{}, outstanding),
	}
	for association, value := range sender.associations {
		writer.associations[association] = value
		writer.epochs[association] = sender.epochs[association]
	}
	if err := finish(nil); err != nil {
		return routingPathMap{}, nil, err
	}
	return paths, writer, nil
}

func (writer *routingDirectWriter) Write(ctx context.Context, route uint16, payload []byte) (int, error) {
	return validateRoutingDirectOutcome(writer.writeOutcome(ctx, route, payload))
}

func (writer *routingDirectWriter) writeOutcome(ctx context.Context, route uint16, payload []byte) routingDirectWriteOutcome {
	write := writer.begin(ctx, route, len(payload))
	if write.ready() {
		write.submit(route, payload)
	}
	return write.finish()
}

// routingDirectWrite is one direct write split at the send clock: begin does
// the untimed admission, context and frozen-path checks, submit is the timed
// Protocol Data construction and WriteData call, and finish revalidates the
// association after the write and releases admission. The timed region is
// therefore the same as routed's: Protocol Data construction and one library
// call.
type routingDirectWrite struct {
	writer      *routingDirectWriter
	association routingDataAssociation
	as          m3ua.ASKey
	admitted    bool
	submitted   bool
	outcome     routingDirectWriteOutcome
}

// begin performs every check that precedes a direct write, before the send
// clock starts: the admission slot, the context, and the frozen path's
// association, epoch and stream bound against preflight. A write that fails
// here is never submitted; finish still releases what begin acquired.
func (writer *routingDirectWriter) begin(ctx context.Context, route uint16, requested int) routingDirectWrite {
	write := routingDirectWrite{writer: writer, outcome: routingDirectWriteOutcome{requested: requested}}
	if writer == nil || writer.admission == nil {
		write.outcome.validationErr = errors.New("routing direct writer is unavailable")
		return write
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		write.outcome.validationErr = err
		return write
	}
	select {
	case writer.admission <- struct{}{}:
		write.admitted = true
	case <-ctx.Done():
		write.outcome.validationErr = ctx.Err()
		return write
	}
	if err := ctx.Err(); err != nil {
		write.outcome.validationErr = err
		return write
	}
	path, err := writer.paths.path(route)
	if err != nil {
		write.outcome.validationErr = err
		return write
	}
	association := writer.associations[path.Target.Association]
	write.outcome.expectedEpoch = writer.epochs[path.Target.Association]
	write.outcome.expectedMaxStream = path.Binding.MaxMessageStreamID
	if association == nil || write.outcome.expectedEpoch == 0 || association.Epoch() != write.outcome.expectedEpoch ||
		association.MaxMessageStreamID() != write.outcome.expectedMaxStream {
		write.outcome.validationErr = errors.New("routing direct association changed after preflight")
		return write
	}
	if write.outcome.expectedMaxStream == 0 {
		write.outcome.validationErr = errors.New("routing direct path has no DATA stream")
		return write
	}
	write.association, write.as = association, path.Target.AS
	return write
}

// ready reports whether begin cleared the write for submission.
func (write *routingDirectWrite) ready() bool {
	return write.outcome.validationErr == nil && write.association != nil
}

// submit is the timed part of a direct write and holds nothing else: Protocol
// Data construction, the stream choice from its SLS, and the one WriteData
// call, exactly as MTPTransfer builds and submits the same DATA for routed.
func (write *routingDirectWrite) submit(route uint16, payload []byte) {
	protocolData, err := routeProtocolData(route, payload)
	if err != nil {
		write.outcome.validationErr = err
		return
	}
	stream := uint16(protocolData.SignallingLinkSelection)%write.outcome.expectedMaxStream + 1
	write.outcome.written, write.outcome.writeErr = write.association.WriteData(m3ua.DataRequest{AS: write.as, ProtocolData: protocolData, Stream: stream})
	write.submitted = true
}

// finish rereads the association after a submitted write, so a write that
// raced an association change is invalid, and releases the admission slot.
func (write *routingDirectWrite) finish() routingDirectWriteOutcome {
	if write.submitted {
		write.outcome.afterEpoch = write.association.Epoch()
		if write.outcome.afterEpoch == write.outcome.expectedEpoch {
			write.outcome.afterMaxStream = write.association.MaxMessageStreamID()
			write.outcome.afterMaxStreamRead = true
		}
	}
	if write.admitted {
		write.admitted = false
		<-write.writer.admission
	}
	return write.outcome
}

func validateRoutingDirectOutcome(outcome routingDirectWriteOutcome) (int, error) {
	if outcome.validationErr != nil {
		return outcome.written, outcome.validationErr
	}
	if outcome.afterEpoch != outcome.expectedEpoch || !outcome.afterMaxStreamRead || outcome.afterMaxStream != outcome.expectedMaxStream {
		return outcome.written, errors.Join(outcome.writeErr, errors.New("routing direct association changed during write"))
	}
	if outcome.writeErr != nil {
		return outcome.written, outcome.writeErr
	}
	if outcome.written != outcome.requested {
		return outcome.written, fmt.Errorf("routing direct write accepted %d octets, want %d", outcome.written, outcome.requested)
	}
	return outcome.written, nil
}

type routingDataPeerEvent struct {
	transport routingTransport
	message   *m3ua.DataMessage
	err       error
}

type routingDataPeerSession struct {
	ctx       context.Context
	cancel    context.CancelFunc
	plane     *routingDataPeerPlane
	cohort    string
	seed      uint64
	events    chan routingDataPeerEvent
	workers   sync.WaitGroup
	baselines map[routingTransport]m3ua.DataQueueStats
	mutex     sync.Mutex
	complete  bool
	closeOnce sync.Once
	closeErr  error
	dropped   atomic.Bool
}

func newRoutingDataPeerSession(ctx context.Context, plane *routingDataPeerPlane, cohort string, seed uint64) (*routingDataPeerSession, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if plane == nil || len(plane.associations) != 8 || cohort == "" {
		return nil, errors.New("routing DATA peer session inventory or identity is invalid")
	}
	lifetime, cancel := context.WithCancel(ctx)
	session := &routingDataPeerSession{
		ctx: lifetime, cancel: cancel, plane: plane, cohort: cohort, seed: seed,
		events: make(chan routingDataPeerEvent, routingRouteCount+1), baselines: make(map[routingTransport]m3ua.DataQueueStats, len(plane.associations)),
	}
	for transport, association := range plane.associations {
		stats := association.DataQueueStats()
		if stats.Queued != 0 || stats.Congested {
			cancel()
			return nil, errors.New("routing DATA peer queue is not empty before preflight")
		}
		session.baselines[transport] = stats
	}
	for transport, association := range plane.associations {
		session.workers.Add(1)
		go session.read(transport, association)
	}
	return session, nil
}

func (session *routingDataPeerSession) read(transport routingTransport, association routingDataAssociation) {
	defer session.workers.Done()
	for {
		message, err := association.ReadData(session.ctx)
		if err != nil {
			if session.ctx.Err() == nil {
				select {
				case session.events <- routingDataPeerEvent{transport: transport, err: err}:
				case <-session.ctx.Done():
				}
			}
			return
		}
		select {
		case session.events <- routingDataPeerEvent{transport: transport, message: message}:
		case <-session.ctx.Done():
			session.dropped.Store(true)
			return
		}
	}
}

func (session *routingDataPeerSession) Complete(ctx context.Context) ([]routingDataReceiptDTO, error) {
	if session == nil {
		return nil, errors.New("routing DATA peer session is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	session.mutex.Lock()
	if session.complete {
		session.mutex.Unlock()
		return nil, errors.New("routing DATA peer session completion was repeated")
	}
	session.complete = true
	session.mutex.Unlock()
	ledger, err := newRoutingLedger(routingRouteCount, maxOutstanding)
	if err != nil {
		return nil, session.finish(err)
	}
	receipts := make([]routingDataReceiptDTO, routingRouteCount)
	for ledger.snapshot().Unique < routingRouteCount {
		select {
		case event := <-session.events:
			if event.err != nil {
				return nil, session.finish(event.err)
			}
			receipt, identity, receiptErr := routingDataReceiptFromMessage(event.transport, event.message, session.cohort, session.seed)
			if receiptErr != nil || ledger.record(identity) != ledgerUnique {
				return nil, session.finish(errors.Join(receiptErr, errors.New("routing DATA receipt is invalid or duplicated")))
			}
			receipts[identity.Route] = receipt
		case <-ctx.Done():
			return nil, session.finish(ctx.Err())
		case <-session.ctx.Done():
			return nil, session.finish(context.Cause(session.ctx))
		}
	}
	if snapshot := ledger.snapshot(); snapshot != (ledgerSnapshot{Unique: routingRouteCount}) {
		return nil, session.finish(fmt.Errorf("routing DATA receipt ledger is incomplete: %+v", snapshot))
	}
	if err := session.finish(nil); err != nil {
		return nil, err
	}
	return receipts, nil
}

func (session *routingDataPeerSession) Close() error {
	if session == nil {
		return nil
	}
	return session.finish(nil)
}

func (session *routingDataPeerSession) finish(cause error) error {
	session.closeOnce.Do(func() {
		session.cancel()
		session.workers.Wait()
		if session.dropped.Load() {
			cause = errors.Join(cause, errors.New("routing DATA peer could not retain a dequeued message during cancellation"))
		}
		extra := false
		for {
			select {
			case event := <-session.events:
				if event.err != nil && !errors.Is(event.err, context.Canceled) {
					cause = errors.Join(cause, event.err)
				} else if event.message != nil {
					extra = true
				}
			default:
				if extra {
					cause = errors.Join(cause, errors.New("routing DATA peer observed extra queued messages"))
				}
				for transport, association := range session.plane.associations {
					stats := association.DataQueueStats()
					baseline := session.baselines[transport]
					if stats.Queued != 0 || stats.Discarded != baseline.Discarded || stats.Congested {
						cause = errors.Join(cause, errors.New("routing DATA peer queue changed during preflight"))
					}
				}
				session.closeErr = cause
				return
			}
		}
	})
	if session.closeErr != nil {
		return session.closeErr
	}
	return cause
}

func routingDataReceiptFromMessage(transport routingTransport, message *m3ua.DataMessage, cohort string, seed uint64) (routingDataReceiptDTO, routingIdentity, error) {
	if message == nil || message.ProtocolData == nil || message.Association != transport.Association {
		return routingDataReceiptDTO{}, routingIdentity{}, errors.New("routing DATA message or transport is missing")
	}
	identity, err := parseRoutePayload(message.ProtocolData.Data)
	if err != nil || identity.CohortHash != cohortHash(cohort) || identity.Seed != seed || identity.Sequence != 0 || len(message.ProtocolData.Data) != 128 {
		return routingDataReceiptDTO{}, routingIdentity{}, errors.New("routing DATA preflight identity differs")
	}
	identity.Cohort = cohort
	expected, err := buildRoutePayload(identity, 128)
	if err != nil || !bytes.Equal(expected, message.ProtocolData.Data) {
		return routingDataReceiptDTO{}, routingIdentity{}, errors.New("routing DATA preflight payload differs")
	}
	protocolData := *message.ProtocolData
	protocolData.Data = append([]byte(nil), protocolData.Data...)
	if message.Scope.RoutingContextSet && len(message.Scope.RoutingContexts) != 1 || !message.Scope.RoutingContextSet && len(message.Scope.RoutingContexts) != 0 {
		return routingDataReceiptDTO{}, routingIdentity{}, errors.New("routing DATA preflight Routing Context shape differs")
	}
	var routingContext uint32
	if len(message.Scope.RoutingContexts) > 0 {
		routingContext = message.Scope.RoutingContexts[0]
	}
	return routingDataReceiptDTO{
		Route: identity.Route, SGP: transport.SGP, Association: message.Association, Epoch: message.Epoch,
		ProtocolData: protocolData, NetworkAppearance: message.Scope.NetworkAppearance,
		NetworkAppearanceSet: message.Scope.NetworkAppearanceSet, RoutingContext: routingContext,
		RoutingContextSet: message.Scope.RoutingContextSet, AS: message.AS, Stream: message.Stream,
		CorrelationID: message.CorrelationID, CorrelationIDSet: message.CorrelationIDSet,
	}, identity, nil
}

// routingDataMessageFromReceipt rebuilds the delivered DATA a peer receipt
// describes, with owned payload bytes, so the same validation applies to it as
// to a message read from an association.
func routingDataMessageFromReceipt(receipt routingDataReceiptDTO) *m3ua.DataMessage {
	data := receipt.ProtocolData
	data.Data = append([]byte(nil), data.Data...)
	var routingContexts []uint32
	if receipt.RoutingContextSet {
		routingContexts = []uint32{receipt.RoutingContext}
	}
	return &m3ua.DataMessage{
		ProtocolData: &data,
		Scope: m3ua.WireScope{
			NetworkAppearance: receipt.NetworkAppearance, NetworkAppearanceSet: receipt.NetworkAppearanceSet,
			RoutingContexts: routingContexts, RoutingContextSet: receipt.RoutingContextSet,
		},
		AS: receipt.AS, Stream: receipt.Stream, CorrelationID: receipt.CorrelationID,
		CorrelationIDSet: receipt.CorrelationIDSet, Association: receipt.Association, Epoch: receipt.Epoch,
	}
}
