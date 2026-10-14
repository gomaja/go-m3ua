package main

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua"
)

type fakeRoutingDataAssociation struct {
	id         m3ua.AssociationID
	epoch      uint64
	maximum    uint16
	reads      chan *m3ua.DataMessage
	writes     []m3ua.DataRequest
	stats      m3ua.DataQueueStats
	mutex      sync.Mutex
	readCalls  atomic.Int32
	afterWrite func(*fakeRoutingDataAssociation)
}

func (association *fakeRoutingDataAssociation) ID() m3ua.AssociationID { return association.id }
func (association *fakeRoutingDataAssociation) Epoch() uint64          { return association.epoch }
func (association *fakeRoutingDataAssociation) MaxMessageStreamID() uint16 {
	return association.maximum
}
func (association *fakeRoutingDataAssociation) ReadData(ctx context.Context) (*m3ua.DataMessage, error) {
	association.readCalls.Add(1)
	select {
	case message := <-association.reads:
		return message, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type blockedRoutingDataAssociation struct {
	id      m3ua.AssociationID
	epoch   uint64
	maximum uint16
	message *m3ua.DataMessage
	read    chan struct{}
	release chan struct{}
}

func (association *blockedRoutingDataAssociation) ID() m3ua.AssociationID { return association.id }
func (association *blockedRoutingDataAssociation) Epoch() uint64          { return association.epoch }
func (association *blockedRoutingDataAssociation) MaxMessageStreamID() uint16 {
	return association.maximum
}
func (association *blockedRoutingDataAssociation) ReadData(context.Context) (*m3ua.DataMessage, error) {
	close(association.read)
	<-association.release
	return association.message, nil
}
func (association *blockedRoutingDataAssociation) WriteData(m3ua.DataRequest) (int, error) {
	return 0, errors.New("unexpected write")
}
func (association *blockedRoutingDataAssociation) DataQueueStats() m3ua.DataQueueStats {
	return m3ua.DataQueueStats{}
}
func (association *fakeRoutingDataAssociation) WriteData(request m3ua.DataRequest) (int, error) {
	association.mutex.Lock()
	defer association.mutex.Unlock()
	request.ProtocolData.Data = append([]byte(nil), request.ProtocolData.Data...)
	association.writes = append(association.writes, request)
	if association.afterWrite != nil {
		association.afterWrite(association)
	}
	return len(request.ProtocolData.Data), nil
}
func (association *fakeRoutingDataAssociation) DataQueueStats() m3ua.DataQueueStats {
	return association.stats
}

type fakeRoutingDataEndpoint struct {
	results map[m3ua.MTPRouteID]m3ua.MTPTransferResult
	errors  map[m3ua.MTPRouteID]error
	calls   []m3ua.MTPTransferRequest
	block   <-chan struct{}
}

func (endpoint *fakeRoutingDataEndpoint) MTPTransfer(request m3ua.MTPTransferRequest) (m3ua.MTPTransferResult, error) {
	endpoint.calls = append(endpoint.calls, request)
	if endpoint.block != nil {
		<-endpoint.block
	}
	return endpoint.results[request.MTPRoute], endpoint.errors[request.MTPRoute]
}

type fakeRoutingDataRemote struct {
	receipts      []routingDataReceiptDTO
	startCalls    int
	completeCalls int
	stopCalls     int
	startCohort   string
	startSeed     uint64
	startErr      error
	completeErr   error
}

func (remote *fakeRoutingDataRemote) Start(_ context.Context, cohort string, seed uint64) error {
	remote.startCalls++
	remote.startCohort, remote.startSeed = cohort, seed
	return remote.startErr
}
func (remote *fakeRoutingDataRemote) Complete(context.Context) ([]routingDataReceiptDTO, error) {
	remote.completeCalls++
	return append([]routingDataReceiptDTO(nil), remote.receipts...), remote.completeErr
}
func (remote *fakeRoutingDataRemote) Stop(context.Context) error {
	remote.stopCalls++
	return nil
}

func routingDataFixture(testContext *testing.T) (routingTopology, []routingAssociationPair, []routingDataAssociation, *fakeRoutingDataEndpoint, []routingDataReceiptDTO) {
	testContext.Helper()
	topology, bindings, observations := routingPreflightFixture(testContext, "primary")
	pairs := make([]routingAssociationPair, len(bindings))
	associations := make([]routingDataAssociation, len(bindings))
	endpoint := &fakeRoutingDataEndpoint{results: make(map[m3ua.MTPRouteID]m3ua.MTPTransferResult), errors: make(map[m3ua.MTPRouteID]error)}
	for index, binding := range bindings {
		pairs[index] = routingAssociationPair{Binding: binding, SenderEpoch: uint64(10 + index)}
		associations[index] = &fakeRoutingDataAssociation{id: binding.SenderAssociation, epoch: pairs[index].SenderEpoch, maximum: binding.MaxMessageStreamID, reads: make(chan *m3ua.DataMessage)}
	}
	receipts := make([]routingDataReceiptDTO, len(observations))
	for index, observation := range observations {
		routeID := m3ua.MTPRouteID(fmt.Sprintf("route-%04d", observation.Route))
		endpoint.results[routeID] = observation.Result
		message := observation.Message
		receipts[len(receipts)-1-index] = routingDataReceiptDTO{
			Route: observation.Route, SGP: observation.Transport.SGP, Association: message.Association, Epoch: message.Epoch,
			ProtocolData: *message.ProtocolData, NetworkAppearance: message.Scope.NetworkAppearance,
			NetworkAppearanceSet: message.Scope.NetworkAppearanceSet, RoutingContext: message.Scope.RoutingContexts[0],
			RoutingContextSet: message.Scope.RoutingContextSet, AS: message.AS, Stream: message.Stream,
			CorrelationID: message.CorrelationID, CorrelationIDSet: message.CorrelationIDSet,
		}
		receipts[len(receipts)-1-index].ProtocolData.Data = append([]byte(nil), message.ProtocolData.Data...)
	}
	return topology, pairs, associations, endpoint, receipts
}

func TestRoutingDataPreflightUsesActualMTPTransferAndFreezesDirectPaths(testContext *testing.T) {
	topology, pairs, associations, endpoint, receipts := routingDataFixture(testContext)
	closed := 0
	plane, err := newRoutingDataSenderPlane(endpoint, associations, pairs, func() error { closed++; return nil })
	if err != nil {
		testContext.Fatal(err)
	}
	remote := &fakeRoutingDataRemote{receipts: receipts}
	paths, writer, err := prepareRoutingData(context.Background(), topology, "preflight", 7, maxOutstanding, plane, remote)
	if err != nil {
		testContext.Fatal(err)
	}
	if len(endpoint.calls) != routingRouteCount || remote.startCalls != 1 || remote.completeCalls != 1 || remote.stopCalls != 0 || closed != 0 || remote.startCohort != "preflight" || remote.startSeed != 7 {
		testContext.Fatalf("calls transfer/start/complete/stop/close=%d/%d/%d/%d/%d identity=%q/%d", len(endpoint.calls), remote.startCalls, remote.completeCalls, remote.stopCalls, closed, remote.startCohort, remote.startSeed)
	}
	for route, request := range endpoint.calls {
		if request.MTPRoute != m3ua.MTPRouteID(fmt.Sprintf("route-%04d", route)) || request.ProtocolData == nil || len(request.ProtocolData.Data) != 128 {
			testContext.Fatalf("route %d request=%+v", route, request)
		}
	}
	frozen, err := paths.path(42)
	if err != nil {
		testContext.Fatal(err)
	}
	payload, err := buildRoutePayload(planRouteMessage("direct", 9, 42), 128)
	if err != nil {
		testContext.Fatal(err)
	}
	written, err := writer.Write(context.Background(), 42, payload)
	if err != nil || written != len(payload) {
		testContext.Fatalf("direct write=%d error=%v", written, err)
	}
	association := associations[42%8].(*fakeRoutingDataAssociation)
	if len(association.writes) != 1 {
		testContext.Fatalf("direct association writes=%d", len(association.writes))
	}
	request := association.writes[0]
	expectedStream := uint16(request.ProtocolData.SignallingLinkSelection)%frozen.Binding.MaxMessageStreamID + 1
	if request.AS != frozen.Target.AS || request.Stream != expectedStream || request.ProtocolData.SignallingLinkSelection != uint8(42%16) || !reflect.DeepEqual(request.ProtocolData.Data, payload) {
		testContext.Fatalf("direct request=%+v frozen=%+v", request, frozen)
	}
}

func TestRoutingDataPreflightRejectsContradictorySenderAndReceiverEvidence(testContext *testing.T) {
	for _, testCase := range []struct {
		name   string
		change func(*fakeRoutingDataEndpoint, *[]routingDataReceiptDTO)
	}{
		{name: "missing", change: func(_ *fakeRoutingDataEndpoint, receipts *[]routingDataReceiptDTO) { *receipts = (*receipts)[:999] }},
		{name: "duplicate", change: func(_ *fakeRoutingDataEndpoint, receipts *[]routingDataReceiptDTO) { (*receipts)[999] = (*receipts)[0] }},
		{name: "extra", change: func(_ *fakeRoutingDataEndpoint, receipts *[]routingDataReceiptDTO) {
			*receipts = append(*receipts, (*receipts)[0])
		}},
		{name: "wrong-transport", change: func(_ *fakeRoutingDataEndpoint, receipts *[]routingDataReceiptDTO) { (*receipts)[0].Association++ }},
		{name: "wrong-epoch", change: func(_ *fakeRoutingDataEndpoint, receipts *[]routingDataReceiptDTO) { (*receipts)[0].Epoch++ }},
		{name: "wrong-scope", change: func(_ *fakeRoutingDataEndpoint, receipts *[]routingDataReceiptDTO) { (*receipts)[0].RoutingContext++ }},
		{name: "wrong-stream", change: func(_ *fakeRoutingDataEndpoint, receipts *[]routingDataReceiptDTO) { (*receipts)[0].Stream++ }},
		{name: "wrong-tuple", change: func(_ *fakeRoutingDataEndpoint, receipts *[]routingDataReceiptDTO) {
			(*receipts)[0].ProtocolData.DestinationPointCode++
		}},
		{name: "wrong-payload", change: func(_ *fakeRoutingDataEndpoint, receipts *[]routingDataReceiptDTO) {
			(*receipts)[0].ProtocolData.Data[127] ^= 0xff
		}},
		{name: "partial-send", change: func(endpoint *fakeRoutingDataEndpoint, _ *[]routingDataReceiptDTO) {
			result := endpoint.results["route-0000"]
			endpoint.errors["route-0000"] = &m3ua.MTPTransferError{SuccessfulPaths: result.SuccessfulPaths, Failures: []m3ua.MTPTransferFailure{{Target: result.SuccessfulPaths[0], Err: errors.New("indeterminate")}}}
		}},
	} {
		testContext.Run(testCase.name, func(testContext *testing.T) {
			topology, pairs, associations, endpoint, receipts := routingDataFixture(testContext)
			testCase.change(endpoint, &receipts)
			closed := 0
			plane, err := newRoutingDataSenderPlane(endpoint, associations, pairs, func() error { closed++; return nil })
			if err != nil {
				testContext.Fatal(err)
			}
			remote := &fakeRoutingDataRemote{receipts: receipts}
			if _, _, err := prepareRoutingData(context.Background(), topology, "preflight", 7, maxOutstanding, plane, remote); err == nil {
				testContext.Fatal("contradictory preflight passed")
			}
			if remote.stopCalls != 1 || closed != 1 {
				testContext.Fatalf("failure cleanup stop/close=%d/%d", remote.stopCalls, closed)
			}
		})
	}
}

func TestRoutingDataDirectWriterRejectsChangedAssociationEpochAndAdmission(testContext *testing.T) {
	for _, outstanding := range []int{0, -1, maxOutstanding + 1, int(^uint(0) >> 1)} {
		topology, pairs, associations, endpoint, receipts := routingDataFixture(testContext)
		plane, err := newRoutingDataSenderPlane(endpoint, associations, pairs, func() error { return nil })
		if err != nil {
			testContext.Fatal(err)
		}
		remote := &fakeRoutingDataRemote{receipts: receipts}
		if _, _, err := prepareRoutingData(context.Background(), topology, "preflight", 7, outstanding, plane, remote); err == nil {
			testContext.Fatalf("outstanding %d accepted", outstanding)
		}
	}
	topology, pairs, associations, endpoint, receipts := routingDataFixture(testContext)
	plane, err := newRoutingDataSenderPlane(endpoint, associations, pairs, func() error { return nil })
	if err != nil {
		testContext.Fatal(err)
	}
	_, writer, err := prepareRoutingData(context.Background(), topology, "preflight", 7, maxOutstanding, plane, &fakeRoutingDataRemote{receipts: receipts})
	if err != nil {
		testContext.Fatal(err)
	}
	associations[0].(*fakeRoutingDataAssociation).epoch++
	if _, err := writer.Write(context.Background(), 0, make([]byte, 128)); err == nil {
		testContext.Fatal("stale frozen association was reused")
	}
}

func TestRoutingDataDirectWriterDoesNotTransmitForAlreadyCanceledContext(testContext *testing.T) {
	topology, pairs, associations, endpoint, receipts := routingDataFixture(testContext)
	plane, err := newRoutingDataSenderPlane(endpoint, associations, pairs, func() error { return nil })
	if err != nil {
		testContext.Fatal(err)
	}
	_, writer, err := prepareRoutingData(context.Background(), topology, "preflight", 7, maxOutstanding, plane, &fakeRoutingDataRemote{receipts: receipts})
	if err != nil {
		testContext.Fatal(err)
	}
	association := associations[0].(*fakeRoutingDataAssociation)
	for attempt := 0; attempt < 128; attempt++ {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := writer.Write(ctx, 0, make([]byte, 128)); !errors.Is(err, context.Canceled) {
			testContext.Fatalf("canceled write attempt %d error=%v", attempt, err)
		}
	}
	if len(association.writes) != 0 {
		testContext.Fatalf("already-canceled calls transmitted %d DATA messages", len(association.writes))
	}
}

func TestRoutingDataDirectWriterInvalidatesConcurrentAssociationChangeWithoutRetry(testContext *testing.T) {
	topology, pairs, associations, endpoint, receipts := routingDataFixture(testContext)
	plane, err := newRoutingDataSenderPlane(endpoint, associations, pairs, func() error { return nil })
	if err != nil {
		testContext.Fatal(err)
	}
	_, writer, err := prepareRoutingData(context.Background(), topology, "preflight", 7, maxOutstanding, plane, &fakeRoutingDataRemote{receipts: receipts})
	if err != nil {
		testContext.Fatal(err)
	}
	association := associations[0].(*fakeRoutingDataAssociation)
	association.afterWrite = func(changed *fakeRoutingDataAssociation) {
		changed.epoch++
		changed.maximum++
	}
	payload := make([]byte, 128)
	written, err := writer.Write(context.Background(), 0, payload)
	if err == nil || written != len(payload) || len(association.writes) != 1 {
		testContext.Fatalf("changed write count/error/calls=%d/%v/%d", written, err, len(association.writes))
	}
}

func TestRoutingDataPeerSessionValidatesEveryQueueBeforeStartingReaders(testContext *testing.T) {
	_, pairs, _, _, _ := routingDataFixture(testContext)
	for attempt := 0; attempt < 64; attempt++ {
		associations := make([]routingDataAssociation, len(pairs))
		for index, pair := range pairs {
			stats := m3ua.DataQueueStats{}
			if index == 7 {
				stats.Queued = 1
			}
			associations[index] = &fakeRoutingDataAssociation{
				id: pair.Binding.Peer.Association, epoch: pair.Binding.PeerEpoch,
				maximum: pair.PeerMaxMessageStreamID, reads: make(chan *m3ua.DataMessage), stats: stats,
			}
		}
		plane, err := newRoutingDataPeerPlane(associations, pairs, func() error { return nil })
		if err != nil {
			testContext.Fatal(err)
		}
		if _, err := newRoutingDataPeerSession(context.Background(), plane, "preflight", 7); err == nil {
			testContext.Fatal("nonempty peer queue was accepted")
		}
		for index, association := range associations {
			if reads := association.(*fakeRoutingDataAssociation).readCalls.Load(); reads != 0 {
				testContext.Fatalf("attempt %d association %d began %d reads before inventory validation completed", attempt, index, reads)
			}
		}
	}
}

func TestRoutingDataReceiptRejectsImpossibleRoutingContextShapes(testContext *testing.T) {
	_, bindings, observations := routingPreflightFixture(testContext, "primary")
	for _, testCase := range []struct {
		name     string
		set      bool
		contexts []uint32
	}{
		{name: "present-empty", set: true},
		{name: "present-multiple", set: true, contexts: []uint32{100, 101}},
		{name: "absent-with-value", contexts: []uint32{100}},
		{name: "absent-with-multiple", contexts: []uint32{100, 101}},
	} {
		testContext.Run(testCase.name, func(testContext *testing.T) {
			message := *observations[0].Message
			message.Scope.RoutingContextSet = testCase.set
			message.Scope.RoutingContexts = append([]uint32(nil), testCase.contexts...)
			if _, _, err := routingDataReceiptFromMessage(bindings[0].Peer, &message, "preflight", 7); err == nil {
				testContext.Fatal("impossible Routing Context shape was projected")
			}
		})
	}
}

func TestRoutingDataReaderMarksDequeuedMessageThatCannotBeHandedOff(testContext *testing.T) {
	_, bindings, observations := routingPreflightFixture(testContext, "primary")
	ctx, cancel := context.WithCancel(context.Background())
	session := &routingDataPeerSession{ctx: ctx, cancel: cancel, events: make(chan routingDataPeerEvent)}
	association := &blockedRoutingDataAssociation{
		id: bindings[0].Peer.Association, epoch: bindings[0].PeerEpoch, maximum: bindings[0].MaxMessageStreamID,
		message: observations[0].Message, read: make(chan struct{}), release: make(chan struct{}),
	}
	done := make(chan struct{})
	session.workers.Add(1)
	go func() {
		session.read(bindings[0].Peer, association)
		close(done)
	}()
	<-association.read
	cancel()
	close(association.release)
	<-done
	if !session.dropped.Load() {
		testContext.Fatal("dequeued DATA was silently dropped during reader cancellation")
	}
}

func TestRoutingDataDeadlineClosesOwnedResourcesAndJoinsBlockedTransfer(testContext *testing.T) {
	topology, pairs, associations, endpoint, receipts := routingDataFixture(testContext)
	blocked := make(chan struct{})
	endpoint.block = blocked
	closed := make(chan struct{})
	plane, err := newRoutingDataSenderPlane(endpoint, associations, pairs, func() error {
		select {
		case <-closed:
		default:
			close(closed)
			close(blocked)
		}
		return nil
	})
	if err != nil {
		testContext.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	remote := &fakeRoutingDataRemote{receipts: receipts}
	if _, _, err := prepareRoutingData(ctx, topology, "preflight", 7, maxOutstanding, plane, remote); !errors.Is(err, context.DeadlineExceeded) {
		testContext.Fatalf("deadline error=%v", err)
	}
	select {
	case <-closed:
	default:
		testContext.Fatal("deadline returned before owned close released the transfer")
	}
	if remote.stopCalls != 1 {
		testContext.Fatalf("deadline stop calls=%d", remote.stopCalls)
	}
}

func TestRoutingDataPeerSessionAccountsActualMessagesAndCancelsBlockedReaders(testContext *testing.T) {
	_, pairs, _, _, receipts := routingDataFixture(testContext)
	associations := make([]routingDataAssociation, len(pairs))
	for index, pair := range pairs {
		association := &fakeRoutingDataAssociation{id: pair.Binding.Peer.Association, epoch: pair.Binding.PeerEpoch, maximum: pair.PeerMaxMessageStreamID, reads: make(chan *m3ua.DataMessage, routingRouteCount)}
		associations[index] = association
	}
	closed := 0
	plane, err := newRoutingDataPeerPlane(associations, pairs, func() error { closed++; return nil })
	if err != nil {
		testContext.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	session, err := newRoutingDataPeerSession(ctx, plane, "preflight", 7)
	if err != nil {
		testContext.Fatal(err)
	}
	for _, receipt := range receipts {
		message := &m3ua.DataMessage{
			ProtocolData: &receipt.ProtocolData, AS: receipt.AS, Stream: receipt.Stream,
			Association: receipt.Association, Epoch: receipt.Epoch,
			Scope: m3ua.WireScope{NetworkAppearance: receipt.NetworkAppearance, NetworkAppearanceSet: receipt.NetworkAppearanceSet, RoutingContexts: []uint32{receipt.RoutingContext}, RoutingContextSet: receipt.RoutingContextSet},
		}
		for index, pair := range pairs {
			if pair.Binding.Peer == (routingTransport{SGP: receipt.SGP, Association: receipt.Association}) {
				associations[index].(*fakeRoutingDataAssociation).reads <- message
				break
			}
		}
	}
	actual, err := session.Complete(context.Background())
	if err != nil || len(actual) != routingRouteCount {
		testContext.Fatalf("complete receipts=%d error=%v", len(actual), err)
	}
	cancel()
	if err := session.Close(); err != nil || closed != 0 {
		testContext.Fatalf("session close=%v plane closes=%d", err, closed)
	}
}
