package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua"
)

type routingControlProbe struct {
	mutex        sync.Mutex
	prepared     int
	publications []uint8
	stopped      int
}

func routingControlFixture(testContext *testing.T) (routingControlOperations, *routingControlProbe, []routingTransportDTO) {
	testContext.Helper()
	topology, senders, peers := routingInventoryFixture(testContext)
	probe := &routingControlProbe{}
	inventory := routingInventoryDTO{Ready: true}
	for index, peer := range peers {
		transport, err := routingTransportDTOFromPeer(peer)
		if err != nil {
			testContext.Fatal(err)
		}
		inventory.Transports = append(inventory.Transports, transport)
		for _, server := range topology.Peers[index/2].ApplicationServers {
			inventory.ASPStatuses = append(inventory.ASPStatuses, routingScopedASPStatusDTO{
				SGP:    peer.SGP,
				Status: m3ua.ASPStatus{Key: m3ua.ASPStatusKey{Association: peer.Snapshot.Association, AS: server.ASKey}, PeerState: m3ua.StateASPActive, PeerStateSet: true},
			})
		}
	}
	senderDTOs := make([]routingTransportDTO, 0, 8)
	for _, sender := range senders {
		transport, err := routingTransportDTOFromSender(sender)
		if err != nil {
			testContext.Fatal(err)
		}
		senderDTOs = append(senderDTOs, transport)
	}
	operations := routingControlOperations{
		Inventory: func(context.Context) (routingInventoryDTO, error) { return inventory, nil },
		Prepare: func(_ context.Context, values []routingTransportDTO) error {
			if len(values) != 8 {
				return errors.New("wrong sender count")
			}
			probe.mutex.Lock()
			defer probe.mutex.Unlock()
			probe.prepared++
			return nil
		},
		Publish: func(_ context.Context, ordinal uint8) error {
			probe.mutex.Lock()
			defer probe.mutex.Unlock()
			probe.publications = append(probe.publications, ordinal)
			return nil
		},
		Stop: func(context.Context) error {
			probe.mutex.Lock()
			defer probe.mutex.Unlock()
			probe.stopped++
			return nil
		},
	}
	return operations, probe, senderDTOs
}

func routingControlRequest(testContext *testing.T, handler http.Handler, path string, value any) *httptest.ResponseRecorder {
	testContext.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		testContext.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(encoded))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func prepareRoutingControl(testContext *testing.T, control *routingControl, senders []routingTransportDTO) {
	testContext.Helper()
	response := routingControlRequest(testContext, control.handler(), "/routing/prepare", map[string]any{"preparation_id": "prep-a", "ordinal": 0, "sender_inventory": senders})
	if response.Code != http.StatusNoContent {
		testContext.Fatalf("prepare: %d %s", response.Code, response.Body.String())
	}
}

func TestRoutingControlSerializesExactlyEightPublications(testContext *testing.T) {
	operations, probe, senders := routingControlFixture(testContext)
	control, err := newRoutingControl("prep-a", operations)
	if err != nil {
		testContext.Fatal(err)
	}
	prepareRoutingControl(testContext, control, senders)
	for ordinal := 1; ordinal <= 8; ordinal++ {
		response := routingControlRequest(testContext, control.handler(), "/routing/publication", map[string]any{"preparation_id": "prep-a", "ordinal": ordinal})
		if response.Code != http.StatusNoContent {
			testContext.Fatalf("publication %d: %d %s", ordinal, response.Code, response.Body.String())
		}
	}
	if probe.prepared != 1 || len(probe.publications) != 8 {
		testContext.Fatalf("operations=%+v", probe)
	}
	for index, ordinal := range probe.publications {
		if int(ordinal) != index {
			testContext.Fatalf("publication %d=%d", index, ordinal)
		}
	}
	for _, ordinal := range []int{8, 9, 0} {
		response := routingControlRequest(testContext, control.handler(), "/routing/publication", map[string]any{"preparation_id": "prep-a", "ordinal": ordinal})
		if response.Code != http.StatusConflict {
			testContext.Fatalf("post-completion ordinal %d accepted: %d", ordinal, response.Code)
		}
	}
	ready := httptest.NewRecorder()
	control.handler().ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/routing/ready", nil))
	var status readyResult
	if err := json.Unmarshal(ready.Body.Bytes(), &status); err != nil || status.Phase != "prepared" {
		testContext.Fatalf("ready=%s error=%v", ready.Body.String(), err)
	}
	for count := 0; count < 2; count++ {
		response := routingControlRequest(testContext, control.handler(), "/routing/stop", map[string]any{"preparation_id": "prep-a"})
		if response.Code != http.StatusNoContent {
			testContext.Fatalf("stop: %d", response.Code)
		}
	}
	if probe.stopped != 1 || len(probe.publications) != 8 {
		testContext.Fatalf("stop replayed work: %+v", probe)
	}
	unknown := httptest.NewRecorder()
	control.handler().ServeHTTP(unknown, httptest.NewRequest(http.MethodPost, "/routing/preflight/start", nil))
	if unknown.Code != http.StatusNotFound {
		testContext.Fatalf("unfinished preflight route exposed: %d", unknown.Code)
	}
}

func TestRoutingControlRejectsMalformedIdentityAndJSONWithoutConsumingOrdinal(testContext *testing.T) {
	operations, probe, senders := routingControlFixture(testContext)
	control, err := newRoutingControl("prep-a", operations)
	if err != nil {
		testContext.Fatal(err)
	}
	encoded, err := json.Marshal(map[string]any{"preparation_id": "prep-a", "ordinal": 0, "sender_inventory": senders})
	if err != nil {
		testContext.Fatal(err)
	}
	valid := string(encoded)
	if !strings.Contains(valid, `"epoch":`) {
		testContext.Fatal("transport JSON lacks explicit epoch field")
	}
	for _, body := range []string{
		strings.Replace(valid, `"ordinal":0`, `"ordinal":0,"ordinal":0`, 1),
		strings.Replace(valid, `"ordinal":0`, `"ordinal":0,"ord\u0069nal":0`, 1),
		strings.Replace(valid, `"preparation_id":"prep-a"`, `"preparation_id":"prep-b","preparation_id":"prep-a"`, 1),
		valid + ` {}`,
		strings.Replace(valid, `"ordinal":0`, `"ordinal":0,"url":"http://unapproved/"`, 1),
		strings.Replace(valid, `"epoch":`, `"epoch":0,"epoch":`, 1),
		strings.Replace(valid, `"epoch":`, `"ep\u006fch":0,"epoch":`, 1),
		strings.Replace(valid, `"ordinal":0,`, "", 1),
		strings.Replace(valid, `"ordinal":0`, `"ordinal":null`, 1),
		strings.Replace(valid, `"ordinal":0`, `"ordinal":256`, 1),
		strings.Replace(valid, `"ordinal":0`, `"ordinal":-1`, 1),
		strings.Repeat(" ", (64<<10)+1) + valid,
	} {
		response := httptest.NewRecorder()
		control.handler().ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/routing/prepare", strings.NewReader(body)))
		if response.Code < 400 || response.Code >= 500 {
			testContext.Fatalf("malformed request status=%d body=%q", response.Code, body)
		}
	}
	for _, value := range []map[string]any{
		{"preparation_id": "wrong", "ordinal": 0, "sender_inventory": senders},
		{"preparation_id": "prep-a", "ordinal": 1, "sender_inventory": senders},
	} {
		response := routingControlRequest(testContext, control.handler(), "/routing/prepare", value)
		if response.Code != http.StatusConflict {
			testContext.Fatalf("mismatched command accepted: %d", response.Code)
		}
	}
	if probe.prepared != 0 {
		testContext.Fatal("invalid command reached callback")
	}
	prepareRoutingControl(testContext, control, senders)
}

func TestRoutingControlReservedOrdinalFailureIsTerminal(testContext *testing.T) {
	operations, probe, senders := routingControlFixture(testContext)
	var calls atomic.Int32
	operations.Publish = func(context.Context, uint8) error { calls.Add(1); return errors.New("unknown remote completion") }
	control, err := newRoutingControl("prep-a", operations)
	if err != nil {
		testContext.Fatal(err)
	}
	prepareRoutingControl(testContext, control, senders)
	first := routingControlRequest(testContext, control.handler(), "/routing/publication", map[string]any{"preparation_id": "prep-a", "ordinal": 1})
	if first.Code < 500 {
		testContext.Fatalf("failed operation status=%d", first.Code)
	}
	for _, ordinal := range []int{1, 2} {
		response := routingControlRequest(testContext, control.handler(), "/routing/publication", map[string]any{"preparation_id": "prep-a", "ordinal": ordinal})
		if response.Code != http.StatusConflict {
			testContext.Fatalf("terminal command accepted: %d", response.Code)
		}
	}
	if calls.Load() != 1 || probe.prepared != 1 {
		testContext.Fatalf("operation retried: %d", calls.Load())
	}
}

func TestRoutingControlPrepareFailureCannotBeReplayed(testContext *testing.T) {
	operations, _, senders := routingControlFixture(testContext)
	var calls atomic.Int32
	operations.Prepare = func(context.Context, []routingTransportDTO) error { calls.Add(1); return errors.New("pairing failed") }
	control, err := newRoutingControl("prep-a", operations)
	if err != nil {
		testContext.Fatal(err)
	}
	command := map[string]any{"preparation_id": "prep-a", "ordinal": 0, "sender_inventory": senders}
	first := routingControlRequest(testContext, control.handler(), "/routing/prepare", command)
	if first.Code < 500 {
		testContext.Fatalf("prepare failure status=%d", first.Code)
	}
	retry := routingControlRequest(testContext, control.handler(), "/routing/prepare", command)
	if retry.Code != http.StatusConflict || calls.Load() != 1 {
		testContext.Fatalf("prepare replay status=%d calls=%d", retry.Code, calls.Load())
	}
}

func TestRoutingControlInventoryRetainsExactScopedStatusesAndIdentity(testContext *testing.T) {
	operations, _, _ := routingControlFixture(testContext)
	want, err := operations.Inventory(context.Background())
	if err != nil {
		testContext.Fatal(err)
	}
	control, err := newRoutingControl("prep-a", operations)
	if err != nil {
		testContext.Fatal(err)
	}
	response := httptest.NewRecorder()
	control.handler().ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/routing/inventory", nil))
	if response.Code != http.StatusOK {
		testContext.Fatalf("inventory: %d %s", response.Code, response.Body.String())
	}
	var envelope struct {
		PreparationID string `json:"preparation_id"`
		routingInventoryDTO
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		testContext.Fatal(err)
	}
	if envelope.PreparationID != "prep-a" || len(envelope.Transports) != 8 || len(envelope.ASPStatuses) != 16 || !reflect.DeepEqual(envelope.routingInventoryDTO, want) {
		testContext.Fatalf("inventory lost identity or actual scoped states: %+v", envelope)
	}
	wrongMethod := httptest.NewRecorder()
	control.handler().ServeHTTP(wrongMethod, httptest.NewRequest(http.MethodPost, "/routing/inventory", nil))
	if wrongMethod.Code != http.StatusMethodNotAllowed {
		testContext.Fatalf("inventory POST accepted: %d", wrongMethod.Code)
	}
}

func TestRoutingControlConcurrentCommandAndStopDoNotJoinHandler(testContext *testing.T) {
	operations, probe, senders := routingControlFixture(testContext)
	entered := make(chan struct{})
	canceled := make(chan struct{})
	release := make(chan struct{})
	operations.Publish = func(ctx context.Context, _ uint8) error {
		close(entered)
		<-ctx.Done()
		close(canceled)
		<-release
		return ctx.Err()
	}
	control, err := newRoutingControl("prep-a", operations)
	if err != nil {
		testContext.Fatal(err)
	}
	prepareRoutingControl(testContext, control, senders)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	done := make(chan struct{})
	testContext.Cleanup(func() {
		close(release)
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			testContext.Error("publication handler did not terminate")
		}
	})
	request := httptest.NewRequest(http.MethodPost, "/routing/publication", strings.NewReader(`{"preparation_id":"prep-a","ordinal":1}`)).WithContext(ctx)
	go func() { defer close(done); control.handler().ServeHTTP(httptest.NewRecorder(), request) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		testContext.Fatal("publication callback not entered")
	}
	concurrent := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		response := httptest.NewRecorder()
		control.handler().ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/routing/publication", strings.NewReader(`{"preparation_id":"prep-a","ordinal":2}`)).WithContext(ctx))
		concurrent <- response
	}()
	select {
	case response := <-concurrent:
		if response.Code != http.StatusConflict {
			testContext.Fatalf("concurrent publication status=%d", response.Code)
		}
	case <-time.After(time.Second):
		testContext.Fatal("concurrent command waited for callback completion")
	}
	stopped := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		response := httptest.NewRecorder()
		control.handler().ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/routing/stop", strings.NewReader(`{"preparation_id":"prep-a"}`)).WithContext(ctx))
		stopped <- response
	}()
	select {
	case response := <-stopped:
		if response.Code != http.StatusNoContent {
			testContext.Fatalf("stop status=%d", response.Code)
		}
	case <-time.After(time.Second):
		testContext.Fatal("stop joined the in-flight publication handler")
	}
	select {
	case <-canceled:
	case <-time.After(time.Second):
		testContext.Fatal("stop did not cancel active callback")
	}
	probe.mutex.Lock()
	stopCount := probe.stopped
	probe.mutex.Unlock()
	if stopCount != 1 {
		testContext.Fatalf("stop callback count=%d", stopCount)
	}
}

func TestRoutingControlRequestDeadlineIsPassedAndFailureRemainsTerminal(testContext *testing.T) {
	operations, _, senders := routingControlFixture(testContext)
	var observedDeadline time.Time
	operations.Publish = func(ctx context.Context, _ uint8) error {
		var present bool
		observedDeadline, present = ctx.Deadline()
		if !present {
			return errors.New("missing deadline")
		}
		<-ctx.Done()
		return ctx.Err()
	}
	control, err := newRoutingControl("prep-a", operations)
	if err != nil {
		testContext.Fatal(err)
	}
	prepareRoutingControl(testContext, control, senders)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	request := httptest.NewRequest(http.MethodPost, "/routing/publication", strings.NewReader(`{"preparation_id":"prep-a","ordinal":1}`)).WithContext(ctx)
	response := httptest.NewRecorder()
	control.handler().ServeHTTP(response, request)
	if response.Code < 500 {
		testContext.Fatalf("timeout status=%d", response.Code)
	}
	wantDeadline, _ := ctx.Deadline()
	if observedDeadline.IsZero() || observedDeadline.After(wantDeadline) {
		testContext.Fatalf("callback deadline=%v request deadline=%v", observedDeadline, wantDeadline)
	}
	retry := routingControlRequest(testContext, control.handler(), "/routing/publication", map[string]any{"preparation_id": "prep-a", "ordinal": 1})
	if retry.Code != http.StatusConflict {
		testContext.Fatal("timed-out publication could be retried")
	}
}

func TestRoutingControlClientRefusesRedirectAndDoesNotRetry(testContext *testing.T) {
	var targetCalls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		targetCalls.Add(1)
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()
	for _, status := range []int{http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther, http.StatusTemporaryRedirect, http.StatusPermanentRedirect} {
		redirect := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			http.Redirect(writer, request, target.URL, status)
		}))
		err := postRoutingControlJSON(context.Background(), redirect.URL, map[string]any{"preparation_id": "prep-a", "ordinal": 1})
		redirect.Close()
		if err == nil || targetCalls.Load() != 0 {
			testContext.Fatalf("redirect %d moved publication: calls=%d error=%v", status, targetCalls.Load(), err)
		}
	}
	var failures atomic.Int32
	failed := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		failures.Add(1)
		http.Error(writer, "ambiguous", http.StatusGatewayTimeout)
	}))
	defer failed.Close()
	if err := postRoutingControlJSON(context.Background(), failed.URL, map[string]any{"preparation_id": "prep-a", "ordinal": 1}); err == nil || failures.Load() != 1 {
		testContext.Fatalf("failed command retried: calls=%d error=%v", failures.Load(), err)
	}
}

func TestRoutingControlDTOContainsOwnedExplicitTransportOnly(testContext *testing.T) {
	_, senders, peers := routingInventoryFixture(testContext)
	for index := range senders {
		sender, err := routingTransportDTOFromSender(senders[index])
		if err != nil {
			testContext.Fatal(err)
		}
		peer, err := routingTransportDTOFromPeer(peers[index])
		if err != nil {
			testContext.Fatal(err)
		}
		encoded, err := json.Marshal([]routingTransportDTO{sender, peer})
		if err != nil || strings.Contains(string(encoded), "SCTPError") || strings.Contains(string(encoded), "ReceiverWindow") {
			testContext.Fatalf("non-DTO status escaped: %s %v", encoded, err)
		}
		clear(senders[index].Snapshot.LocalAddr.IPAddrs[0].IP)
		peers[index].Snapshot.SCTP.InboundStreams = 1
		again, err := json.Marshal([]routingTransportDTO{sender, peer})
		if err != nil || !bytes.Equal(encoded, again) {
			testContext.Fatal("DTO aliases input status")
		}
	}
	for _, preparationID := range []string{"", strings.Repeat("a", 129)} {
		operations, _, _ := routingControlFixture(testContext)
		if _, err := newRoutingControl(preparationID, operations); err == nil {
			testContext.Fatalf("invalid preparation ID accepted: %q", preparationID)
		}
	}
}

func TestRoutingControlRejectsMissingTransportFields(testContext *testing.T) {
	operations, probe, senders := routingControlFixture(testContext)
	control, err := newRoutingControl("prep-a", operations)
	if err != nil {
		testContext.Fatal(err)
	}
	encoded, err := json.Marshal(senders[0])
	if err != nil {
		testContext.Fatal(err)
	}
	var original map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &original); err != nil {
		testContext.Fatal(err)
	}
	for field := range original {
		for _, null := range []bool{false, true} {
			changed := make(map[string]json.RawMessage, len(original))
			for name, value := range original {
				changed[name] = value
			}
			if null {
				changed[field] = json.RawMessage("null")
			} else {
				delete(changed, field)
			}
			values := make([]any, len(senders))
			for index, sender := range senders {
				values[index] = sender
			}
			values[0] = changed
			response := routingControlRequest(testContext, control.handler(), "/routing/prepare", map[string]any{"preparation_id": "prep-a", "ordinal": 0, "sender_inventory": values})
			if response.Code != http.StatusBadRequest {
				testContext.Fatalf("missing/null=%v field=%s status=%d", null, field, response.Code)
			}
		}
	}
	if probe.prepared != 0 {
		testContext.Fatal("incomplete transport reached callback")
	}
	prepareRoutingControl(testContext, control, senders)
}

type routingDeadlineRecorder struct {
	*httptest.ResponseRecorder
	readDeadline  time.Time
	writeDeadline time.Time
}

func (recorder *routingDeadlineRecorder) SetReadDeadline(deadline time.Time) error {
	recorder.readDeadline = deadline
	return nil
}
func (recorder *routingDeadlineRecorder) SetWriteDeadline(deadline time.Time) error {
	recorder.writeDeadline = deadline
	return nil
}

func TestRoutingControlBoundsTransportReadAndWrite(testContext *testing.T) {
	operations, _, _ := routingControlFixture(testContext)
	control, err := newRoutingControl("prep-a", operations)
	if err != nil {
		testContext.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	request := httptest.NewRequest(http.MethodGet, "/routing/ready", nil).WithContext(ctx)
	response := &routingDeadlineRecorder{ResponseRecorder: httptest.NewRecorder()}
	control.handler().ServeHTTP(response, request)
	want, _ := ctx.Deadline()
	if response.readDeadline.IsZero() || response.writeDeadline.IsZero() || response.readDeadline.After(want) || response.writeDeadline.After(want) {
		testContext.Fatalf("unbounded transport read=%v write=%v request=%v", response.readDeadline, response.writeDeadline, want)
	}
}

func TestRoutingControlStopFailureIsRetained(testContext *testing.T) {
	operations, _, _ := routingControlFixture(testContext)
	var calls atomic.Int32
	operations.Stop = func(ctx context.Context) error {
		calls.Add(1)
		if _, present := ctx.Deadline(); !present {
			testContext.Error("cleanup context is unbounded")
		}
		return errors.New("cleanup failed")
	}
	control, err := newRoutingControl("prep-a", operations)
	if err != nil {
		testContext.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		response := routingControlRequest(testContext, control.handler(), "/routing/stop", map[string]any{"preparation_id": "prep-a"})
		if response.Code < 500 || !strings.Contains(response.Body.String(), "cleanup failed") {
			testContext.Fatalf("stop failure lost: %d %s", response.Code, response.Body.String())
		}
	}
	if calls.Load() != 1 {
		testContext.Fatal("failed cleanup was retried")
	}
}

func TestRoutingControlJSONExactBoundsAndMalformedShapes(testContext *testing.T) {
	valid := `{"text":"value"}`
	for _, size := range []int{routingControlLimit - 1, routingControlLimit, routingControlLimit + 1} {
		encoded := valid + strings.Repeat(" ", size-len(valid))
		var result struct {
			Text string `json:"text"`
		}
		err := decodeRoutingControlJSON(strings.NewReader(encoded), &result)
		if (err == nil) != (size <= routingControlLimit) {
			testContext.Fatalf("size=%d error=%v", size, err)
		}
	}
	for _, malformed := range []string{
		`{"text":"first","text":"value"}`, `{"text":"first","TEXT":"value"}`,
		`{"text":"first","te\u0078t":"value"}`, `{"text":null}`, `{"text":"value"} false`,
		`{"unknown":1}`, `[]`, `null`, `{"text":"` + string([]byte{0xff}) + `"}`,
		strings.Repeat("[", 18) + "0" + strings.Repeat("]", 18),
	} {
		var result struct {
			Text string `json:"text"`
		}
		if err := decodeRoutingControlJSON(strings.NewReader(malformed), &result); err == nil {
			testContext.Fatalf("malformed JSON accepted: %q", malformed)
		}
	}
}

func FuzzRoutingControlJSON(fuzzContext *testing.F) {
	for _, seed := range []string{`{"text":"value","number":18446744073709551615}`, `{"text":"first","te\u0078t":"second"}`, `{"number":null}`, `[]`, `{"text":"\ud800"}`} {
		fuzzContext.Add(seed)
	}
	fuzzContext.Fuzz(func(testContext *testing.T, encoded string) {
		type body struct {
			Text   string `json:"text"`
			Number uint64 `json:"number"`
		}
		var result body
		if err := decodeRoutingControlJSON(strings.NewReader(encoded), &result); err != nil {
			return
		}
		if len(encoded) > routingControlLimit {
			testContext.Fatal("oversized body accepted")
		}
		canonical, err := json.Marshal(result)
		if err != nil {
			testContext.Fatal(err)
		}
		var roundTrip body
		if err := decodeRoutingControlJSON(bytes.NewReader(canonical), &roundTrip); err != nil || result != roundTrip {
			testContext.Fatalf("JSON round-trip=%+v want=%+v error=%v", roundTrip, result, err)
		}
	})
}

func TestRoutingControlJSONRejectsUnicodeFieldAlias(testContext *testing.T) {
	var result struct {
		State uint8 `json:"state"`
	}
	err := decodeRoutingControlJSON(strings.NewReader(`{"state":1,"\u017ftate":2}`), &result)
	if err == nil {
		testContext.Fatalf("Unicode alias overwrote State=%d", result.State)
	}
	var value struct {
		Text string `json:"text"`
	}
	if err := decodeRoutingControlJSON(strings.NewReader(`{"text":"\u017f α"}`), &value); err != nil || value.Text != "ſ α" {
		testContext.Fatalf("valid Unicode value rejected: %+v %v", value, err)
	}
}

func TestRoutingControlTransportStandaloneDecodeRejectsDuplicateFields(testContext *testing.T) {
	_, senders, _ := routingInventoryFixture(testContext)
	transport, err := routingTransportDTOFromSender(senders[0])
	if err != nil {
		testContext.Fatal(err)
	}
	encoded, err := json.Marshal(transport)
	if err != nil {
		testContext.Fatal(err)
	}
	duplicate := strings.Replace(string(encoded), `"epoch":`, `"epoch":0,"epoch":`, 1)
	var decoded routingTransportDTO
	if err := json.Unmarshal([]byte(duplicate), &decoded); err == nil {
		testContext.Fatalf("standalone DTO accepted duplicate epoch=%d", decoded.Epoch)
	}
}

func TestRoutingControlBoundsMalformedRequestError(testContext *testing.T) {
	operations, _, _ := routingControlFixture(testContext)
	control, err := newRoutingControl("prep-a", operations)
	if err != nil {
		testContext.Fatal(err)
	}
	malformed := `{"` + strings.Repeat("unknown", 8000) + `":1}`
	response := httptest.NewRecorder()
	control.handler().ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/routing/prepare", strings.NewReader(malformed)))
	if response.Code != http.StatusBadRequest || response.Body.Len() > 4096 {
		testContext.Fatalf("malformed response status=%d bytes=%d", response.Code, response.Body.Len())
	}
}
