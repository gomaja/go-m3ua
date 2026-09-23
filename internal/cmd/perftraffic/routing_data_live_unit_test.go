package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua"
)

type routingDataLiveObservedAssociation struct {
	*fakeRoutingDataAssociation
	readCount     int
	ninthRetained chan struct{}
}

func (association *routingDataLiveObservedAssociation) ReadData(ctx context.Context) (*m3ua.DataMessage, error) {
	association.readCount++
	if association.readCount == 3 {
		close(association.ninthRetained)
	}
	return association.fakeRoutingDataAssociation.ReadData(ctx)
}

func routingDataLiveDirectFixture(testContext *testing.T) (routingPathMap, []routingAssociationPair, []routingDataAssociation, []*m3ua.DataMessage, []routingDataReceiptDTO) {
	testContext.Helper()
	topology, bindings, observations := routingPreflightFixture(testContext, "primary")
	paths, err := freezeRoutingPaths(topology, bindings, observations, "preflight", 7)
	if err != nil {
		testContext.Fatal(err)
	}
	pairs := make([]routingAssociationPair, len(bindings))
	associations := make([]routingDataAssociation, len(bindings))
	messages := make([]*m3ua.DataMessage, routingDataLiveDirectCount)
	for index, binding := range bindings {
		pairs[index] = routingAssociationPair{Binding: binding, SenderEpoch: uint64(10 + index), PeerMaxMessageStreamID: binding.MaxMessageStreamID}
		associations[index] = &fakeRoutingDataAssociation{
			id: binding.Peer.Association, epoch: binding.PeerEpoch, maximum: binding.MaxMessageStreamID,
			reads: make(chan *m3ua.DataMessage, routingDataLiveDirectCount+1),
		}
	}
	receipts := make([]routingDataReceiptDTO, routingDataLiveDirectCount)
	for route := range routingDataLiveDirectCount {
		path, err := paths.path(uint16(route))
		if err != nil {
			testContext.Fatal(err)
		}
		identity := planRouteMessage("direct", 9, uint64(route))
		message := routingReceivedMessage(testContext, identity, 128, path.Target, path.Binding)
		messages[route] = message
		receipt, _, err := routingDataReceiptFromMessage(path.Binding.Peer, message, "direct", 9)
		if err != nil {
			testContext.Fatal(err)
		}
		receipts[route] = receipt
	}
	return paths, pairs, associations, messages, receipts
}

func TestRoutingDataLiveSelectsOneStableRoutePerActualSenderAssociation(testContext *testing.T) {
	paths, _, _, _, _ := routingDataLiveDirectFixture(testContext)
	routes, err := routingDataLiveSelectedRoutes(&paths)
	if err != nil {
		testContext.Fatal(err)
	}
	want := []uint16{0, 1, 2, 3, 4, 5, 6, 7}
	if !reflect.DeepEqual(routes, want) {
		testContext.Fatalf("direct routes=%v want=%v", routes, want)
	}
	removed := paths.paths[7].Target.Association
	replacement := paths.paths[6].Target.Association
	for route := range routingRouteCount {
		if paths.paths[route].Target.Association == removed {
			paths.paths[route].Target.Association = replacement
		}
	}
	if _, err := routingDataLiveSelectedRoutes(&paths); err == nil {
		testContext.Fatal("duplicate frozen sender association was accepted")
	}
}

func TestRoutingDataLiveDirectSessionRetainsExactlyEightActualReceipts(testContext *testing.T) {
	_, pairs, associations, messages, want := routingDataLiveDirectFixture(testContext)
	plane, err := newRoutingDataPeerPlane(associations, pairs, func() error { return nil })
	if err != nil {
		testContext.Fatal(err)
	}
	session, err := newRoutingDataLiveDirectSession(context.Background(), plane, "direct", 9)
	if err != nil {
		testContext.Fatal(err)
	}
	defer func() { _ = session.Close() }()
	for index, message := range messages {
		associations[index].(*fakeRoutingDataAssociation).reads <- message
	}
	got, err := session.Complete(context.Background())
	if err != nil {
		testContext.Fatal(err)
	}
	if len(got) != routingDataLiveDirectCount {
		testContext.Fatalf("direct receipts=%d", len(got))
	}
	byRoute := make(map[uint16]routingDataReceiptDTO, len(got))
	for _, receipt := range got {
		byRoute[receipt.Route] = receipt
	}
	for _, receipt := range want {
		if !reflect.DeepEqual(byRoute[receipt.Route], receipt) {
			testContext.Fatalf("route %d receipt differs", receipt.Route)
		}
	}
}

func TestRoutingDataLiveDirectSessionRejectsObservedExtraMessage(testContext *testing.T) {
	paths, pairs, associations, messages, _ := routingDataLiveDirectFixture(testContext)
	path, err := paths.path(0)
	if err != nil {
		testContext.Fatal(err)
	}
	extra := routingReceivedMessage(testContext, planRouteMessage("direct", 9, 8), 128, path.Target, path.Binding)
	firstAssociation := associations[0].(*fakeRoutingDataAssociation)
	observed := &routingDataLiveObservedAssociation{
		fakeRoutingDataAssociation: firstAssociation,
		ninthRetained:              make(chan struct{}),
	}
	associations[0] = observed
	plane, err := newRoutingDataPeerPlane(associations, pairs, func() error { return nil })
	if err != nil {
		testContext.Fatal(err)
	}
	session, err := newRoutingDataLiveDirectSession(context.Background(), plane, "direct", 9)
	if err != nil {
		testContext.Fatal(err)
	}
	defer func() { _ = session.Close() }()
	for index, message := range messages {
		if index == 0 {
			firstAssociation.reads <- message
		} else {
			associations[index].(*fakeRoutingDataAssociation).reads <- message
		}
	}
	firstAssociation.reads <- extra
	select {
	case <-observed.ninthRetained:
	case <-time.After(time.Second):
		testContext.Fatal("ninth direct DATA message was not retained before completion")
	}
	if _, err := session.Complete(context.Background()); err == nil {
		testContext.Fatal("ninth observed direct DATA message was accepted")
	}
}

func TestRoutingDataLiveDirectSessionRejectsPreexistingQueueBeforeReadersStart(testContext *testing.T) {
	_, pairs, associations, _, _ := routingDataLiveDirectFixture(testContext)
	associations[7].(*fakeRoutingDataAssociation).stats.Queued = 1
	plane, err := newRoutingDataPeerPlane(associations, pairs, func() error { return nil })
	if err != nil {
		testContext.Fatal(err)
	}
	if _, err := newRoutingDataLiveDirectSession(context.Background(), plane, "direct", 9); err == nil {
		testContext.Fatal("preexisting direct DATA was assigned to the new cohort")
	}
	for index, association := range associations {
		if reads := association.(*fakeRoutingDataAssociation).readCalls.Load(); reads != 0 {
			testContext.Fatalf("association %d began %d reads before all queues were validated", index, reads)
		}
	}
}

func TestRoutingDataLiveDirectSessionCancellationAndMissingEighthAreBounded(testContext *testing.T) {
	for _, testCase := range []struct {
		name   string
		cancel bool
	}{
		{name: "canceled", cancel: true},
		{name: "missing-eighth"},
	} {
		testContext.Run(testCase.name, func(testContext *testing.T) {
			_, pairs, associations, messages, _ := routingDataLiveDirectFixture(testContext)
			plane, err := newRoutingDataPeerPlane(associations, pairs, func() error { return nil })
			if err != nil {
				testContext.Fatal(err)
			}
			session, err := newRoutingDataLiveDirectSession(context.Background(), plane, "direct", 9)
			if err != nil {
				testContext.Fatal(err)
			}
			for index := 0; index < routingDataLiveDirectCount-1; index++ {
				associations[index].(*fakeRoutingDataAssociation).reads <- messages[index]
			}
			completeContext, cancelComplete := context.WithTimeout(context.Background(), 20*time.Millisecond)
			if testCase.cancel {
				cancelComplete()
			} else {
				defer cancelComplete()
			}
			started := time.Now()
			if _, err := session.Complete(completeContext); err == nil {
				testContext.Fatal("incomplete direct collection succeeded")
			}
			if time.Since(started) > time.Second {
				testContext.Fatal("incomplete direct collection did not stop within its bound")
			}
		})
	}
}

func TestRoutingDataLiveDirectSessionCloseIsIdempotentAndJoinsReaders(testContext *testing.T) {
	_, pairs, associations, _, _ := routingDataLiveDirectFixture(testContext)
	plane, err := newRoutingDataPeerPlane(associations, pairs, func() error { return nil })
	if err != nil {
		testContext.Fatal(err)
	}
	session, err := newRoutingDataLiveDirectSession(context.Background(), plane, "direct", 9)
	if err != nil {
		testContext.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		allReading := true
		for _, association := range associations {
			allReading = allReading && association.(*fakeRoutingDataAssociation).readCalls.Load() > 0
		}
		if allReading {
			break
		}
		if time.Now().After(deadline) {
			testContext.Fatal("direct readers did not arm")
		}
		time.Sleep(time.Millisecond)
	}
	if first, second := session.Close(), session.Close(); first != nil || second != nil {
		testContext.Fatalf("idempotent close errors=%v/%v", first, second)
	}
}

func TestRoutingDataLiveDirectValidationRejectsMissingDuplicateAndContradictoryEvidence(testContext *testing.T) {
	paths, _, _, _, receipts := routingDataLiveDirectFixture(testContext)
	routes := []uint16{0, 1, 2, 3, 4, 5, 6, 7}
	if err := routingDataLiveValidateDirect(&paths, routes, receipts, "direct", 9); err != nil {
		testContext.Fatal(err)
	}
	for _, testCase := range []struct {
		name   string
		change func([]routingDataReceiptDTO) []routingDataReceiptDTO
	}{
		{name: "missing", change: func(values []routingDataReceiptDTO) []routingDataReceiptDTO { return values[:7] }},
		{name: "duplicate", change: func(values []routingDataReceiptDTO) []routingDataReceiptDTO { values[7] = values[0]; return values }},
		{name: "wrong-epoch", change: func(values []routingDataReceiptDTO) []routingDataReceiptDTO { values[0].Epoch++; return values }},
		{name: "wrong-stream", change: func(values []routingDataReceiptDTO) []routingDataReceiptDTO { values[0].Stream++; return values }},
		{name: "wrong-scope", change: func(values []routingDataReceiptDTO) []routingDataReceiptDTO {
			values[0].RoutingContext++
			return values
		}},
		{name: "wrong-payload", change: func(values []routingDataReceiptDTO) []routingDataReceiptDTO {
			values[0].ProtocolData.Data[127] ^= 0xff
			return values
		}},
	} {
		testContext.Run(testCase.name, func(testContext *testing.T) {
			changed := append([]routingDataReceiptDTO(nil), receipts...)
			for index := range changed {
				changed[index].ProtocolData.Data = append([]byte(nil), changed[index].ProtocolData.Data...)
			}
			if err := routingDataLiveValidateDirect(&paths, routes, testCase.change(changed), "direct", 9); err == nil {
				testContext.Fatal("contradictory direct evidence was accepted")
			}
		})
	}
}

func TestRoutingDataLiveDirectControlIsBoundedOrderedAndDelegatesBase(testContext *testing.T) {
	_, _, _, _, receipts := routingDataLiveDirectFixture(testContext)
	startCalls, completeCalls, baseCalls := 0, 0, 0
	base := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		baseCalls++
		writer.WriteHeader(http.StatusNoContent)
	})
	handler, err := newRoutingDataLiveDirectHandler(base, "prep-a", routingDataLiveDirectOperations{
		Start: func(_ context.Context, cohort string, seed uint64) error {
			startCalls++
			if cohort != "direct" || seed != 9 {
				testContext.Fatalf("direct identity=%q/%d", cohort, seed)
			}
			return nil
		},
		Complete: func(context.Context) ([]routingDataReceiptDTO, error) {
			completeCalls++
			return receipts, nil
		},
	})
	if err != nil {
		testContext.Fatal(err)
	}
	complete := httptest.NewRecorder()
	handler.ServeHTTP(complete, httptest.NewRequest(http.MethodPost, "/routing/data/direct/complete", strings.NewReader(`{"preparation_id":"prep-a"}`)))
	if complete.Code != http.StatusConflict || completeCalls != 0 {
		testContext.Fatalf("complete-before-start status/calls=%d/%d", complete.Code, completeCalls)
	}
	start := httptest.NewRecorder()
	handler.ServeHTTP(start, httptest.NewRequest(http.MethodPost, "/routing/data/direct/start", strings.NewReader(`{"preparation_id":"prep-a","cohort":"direct","seed":9}`)))
	if start.Code != http.StatusNoContent || startCalls != 1 {
		testContext.Fatalf("start status/calls=%d/%d", start.Code, startCalls)
	}
	complete = httptest.NewRecorder()
	handler.ServeHTTP(complete, httptest.NewRequest(http.MethodPost, "/routing/data/direct/complete", strings.NewReader(`{"preparation_id":"prep-a"}`)))
	if complete.Code != http.StatusOK || completeCalls != 1 {
		testContext.Fatalf("complete status/calls=%d/%d body=%s", complete.Code, completeCalls, complete.Body.String())
	}
	var decoded struct {
		PreparationID string                  `json:"preparation_id"`
		Receipts      []routingDataReceiptDTO `json:"receipts"`
	}
	if err := decodeRoutingDataJSON(complete.Body, &decoded); err != nil || decoded.PreparationID != "prep-a" || len(decoded.Receipts) != routingDataLiveDirectCount {
		testContext.Fatalf("direct completion=%+v error=%v", decoded, err)
	}
	delegated := httptest.NewRecorder()
	handler.ServeHTTP(delegated, httptest.NewRequest(http.MethodGet, "/routing/inventory", nil))
	if delegated.Code != http.StatusNoContent || baseCalls != 1 {
		testContext.Fatalf("delegated status/calls=%d/%d", delegated.Code, baseCalls)
	}
	oversized := `{"preparation_id":"prep-a","cohort":"` + strings.Repeat("x", routingDataControlLimit) + `","seed":9}`
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/routing/data/direct/start", strings.NewReader(oversized)))
	if response.Code < 400 {
		testContext.Fatal("oversized direct command was accepted")
	}
}

func TestRoutingDataLiveDirectControlOwnsSessionBeyondStartRequest(testContext *testing.T) {
	_, pairs, associations, messages, _ := routingDataLiveDirectFixture(testContext)
	plane, err := newRoutingDataPeerPlane(associations, pairs, func() error { return nil })
	if err != nil {
		testContext.Fatal(err)
	}
	topologyContext, cancelTopology := context.WithCancel(context.Background())
	defer cancelTopology()
	controller, err := newRoutingDataLiveDirectController(topologyContext, plane)
	if err != nil {
		testContext.Fatal(err)
	}
	handler, err := newRoutingDataLiveDirectHandler(http.NotFoundHandler(), "prep-a", controller.operations())
	if err != nil {
		testContext.Fatal(err)
	}
	requestContext, cancelRequest := context.WithCancel(context.Background())
	start := httptest.NewRecorder()
	handler.ServeHTTP(start, httptest.NewRequest(http.MethodPost, "/routing/data/direct/start", strings.NewReader(`{"preparation_id":"prep-a","cohort":"direct","seed":9}`)).WithContext(requestContext))
	cancelRequest()
	if start.Code != http.StatusNoContent {
		testContext.Fatalf("direct start status=%d body=%s", start.Code, start.Body.String())
	}
	for index, message := range messages {
		associations[index].(*fakeRoutingDataAssociation).reads <- message
	}
	complete := httptest.NewRecorder()
	handler.ServeHTTP(complete, httptest.NewRequest(http.MethodPost, "/routing/data/direct/complete", strings.NewReader(`{"preparation_id":"prep-a"}`)))
	if complete.Code != http.StatusOK {
		testContext.Fatalf("direct completion after request cancellation status=%d body=%s", complete.Code, complete.Body.String())
	}
	if err := controller.Close(); err != nil {
		testContext.Fatal(err)
	}
}

func TestRoutingDataLivePeerShutdownJoinsDirectReadersAfterOwnerCancellation(testContext *testing.T) {
	_, pairs, associations, _, _ := routingDataLiveDirectFixture(testContext)
	plane, err := newRoutingDataPeerPlane(associations, pairs, func() error { return nil })
	if err != nil {
		testContext.Fatal(err)
	}
	lifetime, cancelLifetime := context.WithCancel(context.Background())
	defer cancelLifetime()
	directController, err := newRoutingDataLiveDirectController(lifetime, plane)
	if err != nil {
		testContext.Fatal(err)
	}
	defer func() { _ = directController.Close() }()
	source := &fakeRoutingPeerPreparationSource{reportGate: make(chan struct{})}
	operations := &routingDataLivePeerOperations{
		ctx: lifetime, preparation: &routingPeerPreparation{source: source},
		directController: directController, preflightComplete: true,
	}
	if err := operations.startDirect(context.Background(), "direct", 9); err != nil {
		testContext.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		allReading := true
		for _, association := range associations {
			allReading = allReading && association.(*fakeRoutingDataAssociation).readCalls.Load() > 0
		}
		if allReading {
			break
		}
		if time.Now().After(deadline) {
			testContext.Fatal("direct readers did not arm")
		}
		time.Sleep(time.Millisecond)
	}
	cancelLifetime()
	if first, second := operations.shutdown(), operations.shutdown(); first != nil || second != nil {
		testContext.Fatalf("peer shutdown errors=%v/%v", first, second)
	}
	if source.closeCount != 1 {
		testContext.Fatalf("peer preparation close count=%d", source.closeCount)
	}
}

func TestRoutingDataLiveResultRetainsFullPreflightAndDirectEvidence(testContext *testing.T) {
	paths, _, _, _, direct := routingDataLiveDirectFixture(testContext)
	frozen := make([]routingDataLivePathEvidence, routingRouteCount)
	for route := range routingRouteCount {
		path, err := paths.path(uint16(route))
		if err != nil {
			testContext.Fatal(err)
		}
		frozen[route] = routingDataLivePathEvidence{
			Route: uint16(route), Target: path.Target, PeerSGP: path.Binding.Peer.SGP,
			PeerAssociation: path.Binding.Peer.Association, PeerEpoch: path.Binding.PeerEpoch,
			MaxMessageStreamID: path.Binding.MaxMessageStreamID,
		}
	}
	result := routingDataLiveResult{
		Role: "sender", PreparationID: "prep-a", PreflightReceipts: make([]routingDataReceiptDTO, routingRouteCount),
		FrozenPaths: frozen, DirectRoutes: []uint16{0, 1, 2, 3, 4, 5, 6, 7}, DirectReceipts: direct, OwnerClosed: true,
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		testContext.Fatal(err)
	}
	var decoded routingDataLiveResult
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		testContext.Fatal(err)
	}
	if len(decoded.PreflightReceipts) != routingRouteCount || len(decoded.FrozenPaths) != routingRouteCount || len(decoded.DirectReceipts) != routingDataLiveDirectCount || !decoded.OwnerClosed {
		testContext.Fatalf("retained result is incomplete: %+v", decoded)
	}
}
