package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gomaja/go-m3ua"
)

func TestRoutingDataControlOrdersStartAndCompleteWithoutDisturbingBaseHandler(testContext *testing.T) {
	_, _, _, _, receipts := routingDataFixture(testContext)
	startCalls, completeCalls, baseCalls := 0, 0, 0
	base := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		baseCalls++
		writer.WriteHeader(http.StatusNoContent)
	})
	handler, err := newRoutingDataControlHandler(base, "prep-a", routingDataControlOperations{
		Start: func(_ context.Context, cohort string, seed uint64) error {
			startCalls++
			if cohort != "preflight" || seed != 7 {
				testContext.Fatalf("start identity=%q/%d", cohort, seed)
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
	handler.ServeHTTP(complete, httptest.NewRequest(http.MethodPost, "/routing/data/complete", strings.NewReader(`{"preparation_id":"prep-a"}`)))
	if complete.Code != http.StatusConflict || completeCalls != 0 {
		testContext.Fatalf("complete before start status/calls=%d/%d", complete.Code, completeCalls)
	}
	start := httptest.NewRecorder()
	handler.ServeHTTP(start, httptest.NewRequest(http.MethodPost, "/routing/data/start", strings.NewReader(`{"preparation_id":"prep-a","cohort":"preflight","seed":7}`)))
	if start.Code != http.StatusNoContent || startCalls != 1 {
		testContext.Fatalf("start status/calls=%d/%d", start.Code, startCalls)
	}
	complete = httptest.NewRecorder()
	handler.ServeHTTP(complete, httptest.NewRequest(http.MethodPost, "/routing/data/complete", strings.NewReader(`{"preparation_id":"prep-a"}`)))
	if complete.Code != http.StatusOK || completeCalls != 1 {
		testContext.Fatalf("complete status/calls=%d/%d body=%s", complete.Code, completeCalls, complete.Body.String())
	}
	var decoded struct {
		PreparationID string                  `json:"preparation_id"`
		Receipts      []routingDataReceiptDTO `json:"receipts"`
	}
	if err := decodeRoutingDataJSON(complete.Body, &decoded); err != nil || decoded.PreparationID != "prep-a" || len(decoded.Receipts) != routingRouteCount {
		testContext.Fatalf("complete response=%+v error=%v", decoded, err)
	}
	delegated := httptest.NewRecorder()
	handler.ServeHTTP(delegated, httptest.NewRequest(http.MethodGet, "/routing/inventory", nil))
	if delegated.Code != http.StatusNoContent || baseCalls != 1 {
		testContext.Fatalf("delegated status/calls=%d/%d", delegated.Code, baseCalls)
	}
}

func TestRoutingDataControlHasASeparateBoundForOneThousandFullReceipts(testContext *testing.T) {
	_, _, _, _, receipts := routingDataFixture(testContext)
	encoded, err := json.Marshal(struct {
		PreparationID string                  `json:"preparation_id"`
		Receipts      []routingDataReceiptDTO `json:"receipts"`
	}{PreparationID: "prep-a", Receipts: receipts})
	if err != nil {
		testContext.Fatal(err)
	}
	if len(encoded) <= routingControlLimit || len(encoded) > routingDataControlLimit {
		testContext.Fatalf("1000-receipt encoding=%d ordinary-limit=%d data-limit=%d", len(encoded), routingControlLimit, routingDataControlLimit)
	}
	handler, err := newRoutingDataControlHandler(http.NotFoundHandler(), "prep-a", routingDataControlOperations{
		Start:    func(context.Context, string, uint64) error { return nil },
		Complete: func(context.Context) ([]routingDataReceiptDTO, error) { return receipts, nil },
	})
	if err != nil {
		testContext.Fatal(err)
	}
	oversized := `{"preparation_id":"prep-a","cohort":"` + strings.Repeat("x", routingDataControlLimit) + `","seed":7}`
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/routing/data/start", strings.NewReader(oversized)))
	if response.Code != http.StatusBadRequest {
		testContext.Fatalf("oversized request status=%d", response.Code)
	}
}

func TestRoutingDataControlRejectsDuplicateUnknownAndOutOfOrderCommands(testContext *testing.T) {
	handler, err := newRoutingDataControlHandler(http.NotFoundHandler(), "prep-a", routingDataControlOperations{
		Start:    func(context.Context, string, uint64) error { return nil },
		Complete: func(context.Context) ([]routingDataReceiptDTO, error) { return nil, nil },
	})
	if err != nil {
		testContext.Fatal(err)
	}
	for _, body := range []string{
		`{"preparation_id":"prep-a","PREPARATION_ID":"prep-a","cohort":"preflight","seed":7}`,
		`{"preparation_id":"prep-a","cohort":"preflight","seed":7,"unknown":true}`,
		`{"preparation_id":"wrong","cohort":"preflight","seed":7}`,
	} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/routing/data/start", strings.NewReader(body)))
		if response.Code < 400 {
			testContext.Fatalf("invalid command accepted: %s status=%d", body, response.Code)
		}
	}
}

func TestRoutingDataHTTPClientRejectsRedirectAndDoesNotRetry(testContext *testing.T) {
	targetCalls := 0
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { targetCalls++ }))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, target.URL, http.StatusTemporaryRedirect)
	}))
	defer redirect.Close()
	client, err := newRoutingDataHTTPClient(redirect.URL, "prep-a")
	if err != nil {
		testContext.Fatal(err)
	}
	if err := client.Start(context.Background(), "preflight", 7); err == nil || targetCalls != 0 {
		testContext.Fatalf("redirect start error=%v target-calls=%d", err, targetCalls)
	}
}

func TestRoutingDataHTTPClientRejectsOversizedOrTruncatedReceiptResponse(testContext *testing.T) {
	for _, body := range [][]byte{
		bytes.Repeat([]byte{'x'}, routingDataControlLimit+1),
		[]byte(`{"preparation_id":"prep-a","receipts":[`),
	} {
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			if request.URL.Path == "/routing/data/complete" {
				_, _ = writer.Write(body)
				return
			}
			_, _ = io.WriteString(writer, `{}`)
		}))
		client, err := newRoutingDataHTTPClient(server.URL, "prep-a")
		if err != nil {
			server.Close()
			testContext.Fatal(err)
		}
		if _, err := client.Complete(context.Background()); err == nil {
			server.Close()
			testContext.Fatal("invalid completion response accepted")
		}
		server.Close()
	}
}

func TestRoutingDataPeerControllerKeepsSessionAliveAfterStartRequestReturns(testContext *testing.T) {
	_, pairs, _, _, receipts := routingDataFixture(testContext)
	associations := make([]routingDataAssociation, len(pairs))
	for index, pair := range pairs {
		association := &fakeRoutingDataAssociation{
			id: pair.Binding.Peer.Association, epoch: pair.Binding.PeerEpoch,
			maximum: pair.PeerMaxMessageStreamID, reads: make(chan *m3ua.DataMessage, routingRouteCount),
		}
		associations[index] = association
	}
	for _, receipt := range receipts {
		message := &m3ua.DataMessage{
			ProtocolData: &receipt.ProtocolData, AS: receipt.AS, Stream: receipt.Stream,
			Association: receipt.Association, Epoch: receipt.Epoch,
			Scope: m3ua.WireScope{NetworkAppearance: receipt.NetworkAppearance, NetworkAppearanceSet: receipt.NetworkAppearanceSet,
				RoutingContexts: []uint32{receipt.RoutingContext}, RoutingContextSet: receipt.RoutingContextSet},
		}
		for index, pair := range pairs {
			if pair.Binding.Peer == (routingTransport{SGP: receipt.SGP, Association: receipt.Association}) {
				associations[index].(*fakeRoutingDataAssociation).reads <- message
				break
			}
		}
	}
	planeCloses := 0
	plane, err := newRoutingDataPeerPlane(associations, pairs, func() error { planeCloses++; return nil })
	if err != nil {
		testContext.Fatal(err)
	}
	topologyContext, cancelTopology := context.WithCancel(context.Background())
	defer cancelTopology()
	controller, err := newRoutingDataPeerController(topologyContext, plane)
	if err != nil {
		testContext.Fatal(err)
	}
	handler, err := newRoutingDataControlHandler(http.NotFoundHandler(), "prep-a", controller.operations())
	if err != nil {
		testContext.Fatal(err)
	}
	start := httptest.NewRecorder()
	handler.ServeHTTP(start, httptest.NewRequest(http.MethodPost, "/routing/data/start", strings.NewReader(`{"preparation_id":"prep-a","cohort":"preflight","seed":7}`)))
	if start.Code != http.StatusNoContent {
		testContext.Fatalf("start status=%d body=%s", start.Code, start.Body.String())
	}
	complete := httptest.NewRecorder()
	handler.ServeHTTP(complete, httptest.NewRequest(http.MethodPost, "/routing/data/complete", strings.NewReader(`{"preparation_id":"prep-a"}`)))
	if complete.Code != http.StatusOK {
		testContext.Fatalf("complete after start return status=%d body=%s", complete.Code, complete.Body.String())
	}
	if err := controller.Close(); err != nil || planeCloses != 1 {
		testContext.Fatalf("controller close=%v plane closes=%d", err, planeCloses)
	}
}

func FuzzRoutingDataControlJSON(fuzzContext *testing.F) {
	for _, seed := range []string{
		`{"preparation_id":"prep-a","cohort":"preflight","seed":7}`,
		`{"preparation_id":"prep-a"}`,
		`{"preparation_id":"prep-a","PREPARATION_ID":"prep-a"}`,
		`null`,
	} {
		fuzzContext.Add([]byte(seed))
	}
	fuzzContext.Fuzz(func(testContext *testing.T, encoded []byte) {
		var command struct {
			PreparationID string  `json:"preparation_id"`
			Cohort        *string `json:"cohort,omitempty"`
			Seed          *uint64 `json:"seed,omitempty"`
		}
		if err := decodeRoutingDataJSON(bytes.NewReader(encoded), &command); err != nil {
			return
		}
		roundTrip, err := json.Marshal(command)
		if err != nil {
			testContext.Fatal(err)
		}
		var decoded any
		if err := decodeRoutingDataJSON(bytes.NewReader(roundTrip), &decoded); err != nil {
			testContext.Fatalf("accepted command did not round trip: %v", err)
		}
	})
}
