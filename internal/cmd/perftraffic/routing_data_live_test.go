package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/gomaja/go-m3ua"
	"github.com/gomaja/go-sctp"
)

const routingDataLiveDirectCount = 8
const routingDataLiveTimeout = 60 * time.Second
const routingDataLivePreflightCohort = "routing-data-preflight"
const routingDataLiveDirectCohort = "routing-data-direct"
const routingDataLivePreflightSeed uint64 = 7
const routingDataLiveDirectSeed uint64 = 9

type routingDataLiveDirectSession struct {
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

func newRoutingDataLiveDirectSession(ctx context.Context, plane *routingDataPeerPlane, cohort string, seed uint64) (*routingDataLiveDirectSession, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if plane == nil || len(plane.associations) != routingDataLiveDirectCount || !routingControlIdentity(cohort) {
		return nil, errors.New("routing DATA live direct session inventory or identity is invalid")
	}
	lifetime, cancel := context.WithCancel(ctx)
	session := &routingDataLiveDirectSession{
		ctx: lifetime, cancel: cancel, plane: plane, cohort: cohort, seed: seed,
		events:    make(chan routingDataPeerEvent, routingDataLiveDirectCount+1),
		baselines: make(map[routingTransport]m3ua.DataQueueStats, len(plane.associations)),
	}
	for transport, association := range plane.associations {
		stats := association.DataQueueStats()
		if stats.Queued != 0 || stats.Congested {
			cancel()
			return nil, errors.New("routing DATA live direct queue is not empty before collection")
		}
		session.baselines[transport] = stats
	}
	for transport, association := range plane.associations {
		session.workers.Add(1)
		go session.read(transport, association)
	}
	return session, nil
}

func (session *routingDataLiveDirectSession) read(transport routingTransport, association routingDataAssociation) {
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

func (session *routingDataLiveDirectSession) Complete(ctx context.Context) ([]routingDataReceiptDTO, error) {
	if session == nil {
		return nil, errors.New("routing DATA live direct session is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	session.mutex.Lock()
	if session.complete {
		session.mutex.Unlock()
		return nil, errors.New("routing DATA live direct completion was repeated")
	}
	session.complete = true
	session.mutex.Unlock()
	receipts := make([]routingDataReceiptDTO, 0, routingDataLiveDirectCount)
	routes := make(map[uint16]bool, routingDataLiveDirectCount)
	transports := make(map[routingTransport]bool, routingDataLiveDirectCount)
	for len(receipts) < routingDataLiveDirectCount {
		select {
		case event := <-session.events:
			if event.err != nil {
				return nil, session.finish(event.err)
			}
			receipt, identity, err := routingDataReceiptFromMessage(event.transport, event.message, session.cohort, session.seed)
			if err != nil || routes[identity.Route] || transports[event.transport] {
				return nil, session.finish(errors.Join(err, errors.New("routing DATA live direct receipt is invalid or duplicated")))
			}
			routes[identity.Route] = true
			transports[event.transport] = true
			receipts = append(receipts, receipt)
		case <-ctx.Done():
			return nil, session.finish(ctx.Err())
		case <-session.ctx.Done():
			return nil, session.finish(context.Cause(session.ctx))
		}
	}
	if len(transports) != routingDataLiveDirectCount {
		return nil, session.finish(errors.New("routing DATA live direct collection did not exercise every peer association"))
	}
	if err := session.finish(nil); err != nil {
		return nil, err
	}
	sort.Slice(receipts, func(first, second int) bool { return receipts[first].Route < receipts[second].Route })
	return receipts, nil
}

func (session *routingDataLiveDirectSession) Close() error {
	if session == nil {
		return nil
	}
	return session.finish(nil)
}

func (session *routingDataLiveDirectSession) finish(cause error) error {
	session.closeOnce.Do(func() {
		session.cancel()
		session.workers.Wait()
		if session.dropped.Load() {
			cause = errors.Join(cause, errors.New("routing DATA live direct reader could not retain a dequeued message"))
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
					cause = errors.Join(cause, errors.New("routing DATA live direct collector observed extra queued messages"))
				}
				for transport, association := range session.plane.associations {
					stats := association.DataQueueStats()
					baseline := session.baselines[transport]
					if stats.Queued != 0 || stats.Discarded != baseline.Discarded || stats.Congested {
						cause = errors.Join(cause, errors.New("routing DATA live direct queue changed during collection"))
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

type routingDataLiveDirectController struct {
	ctx       context.Context
	cancel    context.CancelFunc
	plane     *routingDataPeerPlane
	mutex     sync.Mutex
	session   *routingDataLiveDirectSession
	started   bool
	closed    bool
	closeOnce sync.Once
	closeErr  error
}

func newRoutingDataLiveDirectController(ctx context.Context, plane *routingDataPeerPlane) (*routingDataLiveDirectController, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if plane == nil {
		return nil, errors.New("routing DATA live direct controller requires a peer plane")
	}
	lifetime, cancel := context.WithCancel(ctx)
	return &routingDataLiveDirectController{ctx: lifetime, cancel: cancel, plane: plane}, nil
}

func (controller *routingDataLiveDirectController) operations() routingDataLiveDirectOperations {
	return routingDataLiveDirectOperations{Start: controller.start, Complete: controller.complete}
}

func (controller *routingDataLiveDirectController) start(requestContext context.Context, cohort string, seed uint64) error {
	if err := requestContext.Err(); err != nil {
		return err
	}
	controller.mutex.Lock()
	defer controller.mutex.Unlock()
	if controller.started || controller.closed || controller.ctx.Err() != nil {
		return errors.New("routing DATA live direct controller is already started or closed")
	}
	session, err := newRoutingDataLiveDirectSession(controller.ctx, controller.plane, cohort, seed)
	if err != nil {
		return err
	}
	if err := requestContext.Err(); err != nil {
		return errors.Join(err, session.Close())
	}
	controller.session = session
	controller.started = true
	return nil
}

func (controller *routingDataLiveDirectController) complete(requestContext context.Context) ([]routingDataReceiptDTO, error) {
	controller.mutex.Lock()
	session := controller.session
	closed := controller.closed
	controller.mutex.Unlock()
	if session == nil || closed {
		return nil, errors.New("routing DATA live direct controller is not active")
	}
	return session.Complete(requestContext)
}

func (controller *routingDataLiveDirectController) Close() error {
	if controller == nil {
		return nil
	}
	controller.closeOnce.Do(func() {
		controller.mutex.Lock()
		controller.closed = true
		session := controller.session
		controller.mutex.Unlock()
		controller.cancel()
		if session != nil {
			controller.closeErr = session.Close()
		}
	})
	return controller.closeErr
}

func routingDataLiveSelectedRoutes(paths *routingPathMap) ([]uint16, error) {
	if paths == nil || !paths.ready {
		return nil, errors.New("routing DATA live paths are not frozen")
	}
	selected := make(map[m3ua.AssociationID]uint16, routingDataLiveDirectCount)
	for route := range routingRouteCount {
		path, err := paths.path(uint16(route))
		if err != nil || path.Target.Association == 0 {
			return nil, errors.Join(err, errors.New("routing DATA live frozen path is invalid"))
		}
		if _, present := selected[path.Target.Association]; !present {
			selected[path.Target.Association] = uint16(route)
		}
	}
	if len(selected) != routingDataLiveDirectCount {
		return nil, errors.New("routing DATA live paths do not cover eight sender associations")
	}
	routes := make([]uint16, 0, routingDataLiveDirectCount)
	for _, route := range selected {
		routes = append(routes, route)
	}
	sort.Slice(routes, func(first, second int) bool { return routes[first] < routes[second] })
	return routes, nil
}

func routingDataLiveValidateDirect(paths *routingPathMap, routes []uint16, receipts []routingDataReceiptDTO, cohort string, seed uint64) error {
	expected, err := routingDataLiveSelectedRoutes(paths)
	if err != nil || !equalRoutingDataLiveRoutes(routes, expected) || len(receipts) != routingDataLiveDirectCount || !routingControlIdentity(cohort) {
		return errors.Join(err, errors.New("routing DATA live direct evidence inventory differs"))
	}
	expectedRoutes := make(map[uint16]bool, routingDataLiveDirectCount)
	for _, route := range routes {
		expectedRoutes[route] = true
	}
	seenRoutes := make(map[uint16]bool, routingDataLiveDirectCount)
	seenSenders := make(map[m3ua.AssociationID]bool, routingDataLiveDirectCount)
	seenPeers := make(map[routingTransport]bool, routingDataLiveDirectCount)
	for _, receipt := range receipts {
		if !expectedRoutes[receipt.Route] || seenRoutes[receipt.Route] {
			return errors.New("routing DATA live direct route is missing, unknown or duplicated")
		}
		data := receipt.ProtocolData
		data.Data = append([]byte(nil), data.Data...)
		var routingContexts []uint32
		if receipt.RoutingContextSet {
			routingContexts = []uint32{receipt.RoutingContext}
		}
		message := &m3ua.DataMessage{
			ProtocolData: &data, AS: receipt.AS, Stream: receipt.Stream,
			Association: receipt.Association, Epoch: receipt.Epoch,
			CorrelationID: receipt.CorrelationID, CorrelationIDSet: receipt.CorrelationIDSet,
			Scope: m3ua.WireScope{
				NetworkAppearance: receipt.NetworkAppearance, NetworkAppearanceSet: receipt.NetworkAppearanceSet,
				RoutingContexts: routingContexts, RoutingContextSet: receipt.RoutingContextSet,
			},
		}
		transport := routingTransport{SGP: receipt.SGP, Association: receipt.Association}
		identity, err := validateRouteMessage(message, transport, cohort, seed, workload128, paths)
		if err != nil || identity.Route != receipt.Route || identity.Sequence != 0 {
			return errors.Join(err, errors.New("routing DATA live direct receipt differs from its frozen path"))
		}
		path, err := paths.path(receipt.Route)
		if err != nil || seenSenders[path.Target.Association] || seenPeers[transport] {
			return errors.Join(err, errors.New("routing DATA live direct association is duplicated"))
		}
		seenRoutes[receipt.Route] = true
		seenSenders[path.Target.Association] = true
		seenPeers[transport] = true
	}
	if len(seenRoutes) != routingDataLiveDirectCount || len(seenSenders) != routingDataLiveDirectCount || len(seenPeers) != routingDataLiveDirectCount {
		return errors.New("routing DATA live direct association coverage is incomplete")
	}
	return nil
}

func equalRoutingDataLiveRoutes(first, second []uint16) bool {
	if len(first) != len(second) {
		return false
	}
	for index := range first {
		if first[index] != second[index] {
			return false
		}
	}
	return true
}

type routingDataLiveDirectOperations struct {
	Start    func(context.Context, string, uint64) error
	Complete func(context.Context) ([]routingDataReceiptDTO, error)
}

type routingDataLiveDirectHandler struct {
	base          http.Handler
	preparationID string
	operations    routingDataLiveDirectOperations
	mutex         sync.Mutex
	phase         string
}

func newRoutingDataLiveDirectHandler(base http.Handler, preparationID string, operations routingDataLiveDirectOperations) (http.Handler, error) {
	if base == nil || !routingControlIdentity(preparationID) || operations.Start == nil || operations.Complete == nil {
		return nil, errors.New("routing DATA live direct control requires a base handler, identity and operations")
	}
	return &routingDataLiveDirectHandler{base: base, preparationID: preparationID, operations: operations, phase: "idle"}, nil
}

func (handler *routingDataLiveDirectHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.URL.Path != "/routing/data/direct/start" && request.URL.Path != "/routing/data/direct/complete" {
		handler.base.ServeHTTP(writer, request)
		return
	}
	if request.Method != http.MethodPost {
		writer.Header().Set("Allow", http.MethodPost)
		routingControlStatusError(writer, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if request.URL.RawQuery != "" {
		routingControlStatusError(writer, "routing DATA live direct control does not accept query parameters", http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(request.Context(), routingControlTimeout)
	defer cancel()
	deadline, _ := ctx.Deadline()
	response := http.NewResponseController(writer)
	if err := response.SetReadDeadline(deadline); err != nil && !errors.Is(err, http.ErrNotSupported) {
		routingControlError(writer, err)
		return
	}
	if err := response.SetWriteDeadline(deadline); err != nil && !errors.Is(err, http.ErrNotSupported) {
		routingControlError(writer, err)
		return
	}
	var command struct {
		PreparationID string  `json:"preparation_id"`
		Cohort        *string `json:"cohort,omitempty"`
		Seed          *uint64 `json:"seed,omitempty"`
	}
	if err := decodeRoutingDataLiveJSON(request.Body, &command); err != nil {
		routingControlStatusError(writer, err.Error(), http.StatusBadRequest)
		return
	}
	if command.PreparationID != handler.preparationID {
		routingControlStatusError(writer, "routing DATA live direct preparation identity mismatch", http.StatusConflict)
		return
	}
	starting := request.URL.Path == "/routing/data/direct/start"
	if starting != (command.Cohort != nil && command.Seed != nil) || !starting && (command.Cohort != nil || command.Seed != nil) || starting && !routingControlIdentity(*command.Cohort) {
		routingControlStatusError(writer, "routing DATA live direct command fields are contradictory", http.StatusBadRequest)
		return
	}
	handler.mutex.Lock()
	if starting && handler.phase != "idle" || !starting && handler.phase != "started" {
		handler.mutex.Unlock()
		routingControlStatusError(writer, "routing DATA live direct command is out of order or terminal", http.StatusConflict)
		return
	}
	if starting {
		handler.phase = "starting"
	} else {
		handler.phase = "completing"
	}
	handler.mutex.Unlock()
	if starting {
		err := handler.operations.Start(ctx, *command.Cohort, *command.Seed)
		handler.mutex.Lock()
		if err != nil {
			handler.phase = "failed"
		} else {
			handler.phase = "started"
		}
		handler.mutex.Unlock()
		if err != nil {
			routingControlError(writer, err)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
		return
	}
	receipts, err := handler.operations.Complete(ctx)
	if err == nil && len(receipts) != routingDataLiveDirectCount {
		err = errors.New("routing DATA live direct completion does not contain eight receipts")
	}
	value := struct {
		PreparationID string                  `json:"preparation_id"`
		Receipts      []routingDataReceiptDTO `json:"receipts"`
	}{PreparationID: handler.preparationID, Receipts: receipts}
	var encoded []byte
	if err == nil {
		encoded, err = json.Marshal(value)
		if err == nil && len(encoded) > routingControlLimit {
			err = errors.New("routing DATA live direct completion exceeds its response bound")
		}
	}
	handler.mutex.Lock()
	if err != nil {
		handler.phase = "failed"
	} else {
		handler.phase = "complete"
	}
	handler.mutex.Unlock()
	if err != nil {
		routingControlError(writer, err)
		return
	}
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(http.StatusOK)
	_, _ = writer.Write(encoded)
}

type routingDataLiveDirectHTTPClient struct {
	baseURL       string
	preparationID string
}

func newRoutingDataLiveDirectHTTPClient(baseURL, preparationID string) (*routingDataLiveDirectHTTPClient, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") || !routingControlIdentity(preparationID) {
		return nil, errors.New("routing DATA live direct control address or identity is invalid")
	}
	return &routingDataLiveDirectHTTPClient{baseURL: strings.TrimRight(baseURL, "/"), preparationID: preparationID}, nil
}

func (client *routingDataLiveDirectHTTPClient) Start(ctx context.Context, cohort string, seed uint64) error {
	if !routingControlIdentity(cohort) {
		return errors.New("routing DATA live direct cohort is invalid")
	}
	return postRoutingDataLiveJSON(ctx, client.baseURL+"/routing/data/direct/start", struct {
		PreparationID string `json:"preparation_id"`
		Cohort        string `json:"cohort"`
		Seed          uint64 `json:"seed"`
	}{PreparationID: client.preparationID, Cohort: cohort, Seed: seed}, http.StatusNoContent, nil)
}

func (client *routingDataLiveDirectHTTPClient) Complete(ctx context.Context) ([]routingDataReceiptDTO, error) {
	var value struct {
		PreparationID *string                 `json:"preparation_id"`
		Receipts      []routingDataReceiptDTO `json:"receipts"`
	}
	err := postRoutingDataLiveJSON(ctx, client.baseURL+"/routing/data/direct/complete", struct {
		PreparationID string `json:"preparation_id"`
	}{PreparationID: client.preparationID}, http.StatusOK, &value)
	if err != nil {
		return nil, err
	}
	if value.PreparationID == nil || *value.PreparationID != client.preparationID || len(value.Receipts) != routingDataLiveDirectCount {
		return nil, errors.New("routing DATA live direct completion identity or receipt count differs")
	}
	return value.Receipts, nil
}

func decodeRoutingDataLiveJSON(reader io.Reader, target any) error {
	encoded, err := io.ReadAll(io.LimitReader(reader, routingControlLimit+1))
	if err != nil {
		return err
	}
	if len(encoded) > routingControlLimit || !utf8.Valid(encoded) {
		return errors.New("routing DATA live direct body is oversized or invalid UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := scanRoutingControlJSON(decoder, 0); err != nil {
		return err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return errors.New("routing DATA live direct body has trailing JSON")
	}
	decoder = json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	return decoder.Decode(target)
}

func postRoutingDataLiveJSON(ctx context.Context, target string, value any, expectedStatus int, result any) error {
	encoded, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if len(encoded) > routingControlLimit {
		return errors.New("routing DATA live direct request exceeds body limit")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(encoded))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	request.GetBody = nil
	client := &http.Client{Timeout: routingControlTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != expectedStatus {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("routing DATA live direct control returned %s: %s", response.Status, body)
	}
	if result == nil {
		_, err = io.Copy(io.Discard, io.LimitReader(response.Body, 1))
		return err
	}
	return decodeRoutingDataLiveJSON(response.Body, result)
}

type routingDataLivePathEvidence struct {
	Route              uint16               `json:"route"`
	Target             m3ua.MTPTransferPath `json:"target"`
	PeerSGP            m3ua.SGPIdentity     `json:"peer_sgp"`
	PeerAssociation    m3ua.AssociationID   `json:"peer_association"`
	PeerEpoch          uint64               `json:"peer_epoch"`
	MaxMessageStreamID uint16               `json:"max_message_stream_id"`
}

type routingDataLiveResult struct {
	Role              string                          `json:"role"`
	PreparationID     string                          `json:"preparation_id"`
	SenderInventory   []routingTransportDTO           `json:"sender_inventory,omitempty"`
	PeerInventory     []routingTransportDTO           `json:"peer_inventory,omitempty"`
	SSNMEvidence      *routingSSNMPreparationEvidence `json:"ssnm_evidence,omitempty"`
	FinalSnapshot     *m3ua.SSNMSnapshot              `json:"final_snapshot,omitempty"`
	PreflightReceipts []routingDataReceiptDTO         `json:"preflight_receipts,omitempty"`
	FrozenPaths       []routingDataLivePathEvidence   `json:"frozen_paths,omitempty"`
	DirectRoutes      []uint16                        `json:"direct_routes,omitempty"`
	DirectReceipts    []routingDataReceiptDTO         `json:"direct_receipts,omitempty"`
	OwnerClosed       bool                            `json:"owner_closed"`
	ServerJoined      bool                            `json:"server_joined,omitempty"`
}

type routingDataLiveConfig struct {
	Role          string
	PreparationID string
	PeerIP        net.IP
	SenderIP      net.IP
	ControlURL    string
	ResultDir     string
}

func routingDataLiveConfigFromEnvironment(testContext *testing.T) routingDataLiveConfig {
	testContext.Helper()
	value := func(name string) string {
		result := os.Getenv(name)
		if result == "" {
			testContext.Fatalf("%s is required", name)
		}
		return result
	}
	peerIP := net.ParseIP(value("ROUTING_DATA_LIVE_PEER_IP"))
	senderIP := net.ParseIP(value("ROUTING_DATA_LIVE_SENDER_IP"))
	if peerIP == nil || senderIP == nil || peerIP.IsUnspecified() || senderIP.IsUnspecified() {
		testContext.Fatal("routing DATA live addresses must be concrete IP addresses")
	}
	return routingDataLiveConfig{
		Role: value("ROUTING_DATA_LIVE_ROLE"), PreparationID: value("ROUTING_DATA_LIVE_PREPARATION_ID"),
		PeerIP: peerIP, SenderIP: senderIP, ControlURL: value("ROUTING_DATA_LIVE_CONTROL_URL"),
		ResultDir: value("ROUTING_DATA_LIVE_RESULT_DIR"),
	}
}

type routingDataLivePeerOperations struct {
	ctx         context.Context
	topology    routingTopology
	peers       *routingPeerSet
	preparation *routingPeerPreparation

	mutex             sync.Mutex
	dataController    *routingDataPeerController
	directController  *routingDataLiveDirectController
	publicationCount  int
	preflightComplete bool
	preflightReceipts []routingDataReceiptDTO
	directReceipts    []routingDataReceiptDTO
	stopOnce          sync.Once
	stopErr           error
}

func (operations *routingDataLivePeerOperations) controlOperations() routingControlOperations {
	return routingControlOperations{
		Inventory: operations.preparation.inventory,
		Prepare:   operations.prepare,
		Publish:   operations.publish,
		Stop:      operations.stop,
	}
}

func (operations *routingDataLivePeerOperations) dataOperations() routingDataControlOperations {
	return routingDataControlOperations{Start: operations.startPreflight, Complete: operations.completePreflight}
}

func (operations *routingDataLivePeerOperations) directOperations() routingDataLiveDirectOperations {
	return routingDataLiveDirectOperations{Start: operations.startDirect, Complete: operations.completeDirect}
}

func (operations *routingDataLivePeerOperations) prepare(ctx context.Context, values []routingTransportDTO) error {
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
			return errors.New("routing DATA live peer association does not expose unique DATA operations")
		}
		byTransport[transport] = association
	}
	associations := make([]routingDataAssociation, len(pairs))
	for index, pair := range pairs {
		association := byTransport[pair.Binding.Peer]
		if association == nil {
			return errors.New("routing DATA live peer association is not paired")
		}
		associations[index] = association
	}
	plane, err := newRoutingDataPeerPlane(associations, pairs, operations.peers.Close)
	if err != nil {
		return err
	}
	dataController, err := newRoutingDataPeerController(operations.ctx, plane)
	if err != nil {
		return err
	}
	directController, err := newRoutingDataLiveDirectController(operations.ctx, plane)
	if err != nil {
		return errors.Join(err, dataController.Close())
	}
	operations.mutex.Lock()
	defer operations.mutex.Unlock()
	if operations.dataController != nil || operations.directController != nil || ctx.Err() != nil {
		return errors.Join(ctx.Err(), directController.Close(), dataController.Close(), errors.New("routing DATA live peer was prepared repeatedly or canceled"))
	}
	operations.dataController = dataController
	operations.directController = directController
	return nil
}

func (operations *routingDataLivePeerOperations) publish(ctx context.Context, ordinal uint8) error {
	if err := operations.preparation.publish(ctx, ordinal); err != nil {
		return err
	}
	operations.mutex.Lock()
	defer operations.mutex.Unlock()
	if int(ordinal) != operations.publicationCount {
		return errors.New("routing DATA live SSNM publication count differs")
	}
	operations.publicationCount++
	return nil
}

func (operations *routingDataLivePeerOperations) startPreflight(ctx context.Context, cohort string, seed uint64) error {
	operations.mutex.Lock()
	controller := operations.dataController
	ready := operations.publicationCount == routingSSNMPublicationCount && !operations.preflightComplete
	operations.mutex.Unlock()
	if controller == nil || !ready {
		return errors.New("routing DATA live preflight started before SSNM preparation")
	}
	return controller.start(ctx, cohort, seed)
}

func (operations *routingDataLivePeerOperations) completePreflight(ctx context.Context) ([]routingDataReceiptDTO, error) {
	operations.mutex.Lock()
	controller := operations.dataController
	operations.mutex.Unlock()
	if controller == nil {
		return nil, errors.New("routing DATA live preflight controller is unavailable")
	}
	receipts, err := controller.complete(ctx)
	if err != nil {
		return nil, err
	}
	owned := cloneRoutingDataLiveReceipts(receipts)
	operations.mutex.Lock()
	operations.preflightComplete = true
	operations.preflightReceipts = owned
	operations.mutex.Unlock()
	return receipts, nil
}

func (operations *routingDataLivePeerOperations) startDirect(ctx context.Context, cohort string, seed uint64) error {
	operations.mutex.Lock()
	controller := operations.directController
	ready := operations.preflightComplete
	operations.mutex.Unlock()
	if controller == nil || !ready {
		return errors.New("routing DATA live direct collection started before preflight completion")
	}
	return controller.start(ctx, cohort, seed)
}

func (operations *routingDataLivePeerOperations) completeDirect(ctx context.Context) ([]routingDataReceiptDTO, error) {
	operations.mutex.Lock()
	controller := operations.directController
	operations.mutex.Unlock()
	if controller == nil {
		return nil, errors.New("routing DATA live direct controller is unavailable")
	}
	receipts, err := controller.complete(ctx)
	if err != nil {
		return nil, err
	}
	owned := cloneRoutingDataLiveReceipts(receipts)
	operations.mutex.Lock()
	operations.directReceipts = owned
	operations.mutex.Unlock()
	return receipts, nil
}

func (operations *routingDataLivePeerOperations) stop(ctx context.Context) error {
	operations.stopOnce.Do(func() {
		operations.mutex.Lock()
		directController := operations.directController
		dataController := operations.dataController
		operations.mutex.Unlock()
		operations.stopErr = errors.Join(directController.Close(), dataController.Close(), operations.preparation.stop(ctx))
	})
	return operations.stopErr
}

func (operations *routingDataLivePeerOperations) shutdown() error {
	ctx, cancel := context.WithTimeout(context.Background(), routingControlTimeout)
	defer cancel()
	return operations.stop(ctx)
}

func cloneRoutingDataLiveReceipts(receipts []routingDataReceiptDTO) []routingDataReceiptDTO {
	owned := append([]routingDataReceiptDTO(nil), receipts...)
	for index := range owned {
		owned[index].ProtocolData.Data = append([]byte(nil), owned[index].ProtocolData.Data...)
	}
	return owned
}

type routingDataLiveRecordingControl struct {
	inner    *routingDataHTTPClient
	receipts []routingDataReceiptDTO
}

func (control *routingDataLiveRecordingControl) Start(ctx context.Context, cohort string, seed uint64) error {
	return control.inner.Start(ctx, cohort, seed)
}

func (control *routingDataLiveRecordingControl) Complete(ctx context.Context) ([]routingDataReceiptDTO, error) {
	receipts, err := control.inner.Complete(ctx)
	if err == nil {
		control.receipts = cloneRoutingDataLiveReceipts(receipts)
	}
	return receipts, err
}

func (control *routingDataLiveRecordingControl) Stop(ctx context.Context) error {
	return control.inner.Stop(ctx)
}

func routingDataLiveSenderPlane(set *routingSenderSet, senderInventory []routingSenderInventory, pairs []routingAssociationPair) (*routingDataSenderPlane, error) {
	if set == nil || len(set.endpoints) != 1 {
		return nil, errors.New("routing DATA live sender set is incomplete")
	}
	endpoint, valid := set.endpoints[0].(routingDataTransferEndpoint)
	if !valid {
		return nil, errors.New("routing DATA live sender endpoint does not expose MTPTransfer")
	}
	entries, err := set.inventoryEntries()
	if err != nil {
		return nil, err
	}
	associations := make([]routingDataAssociation, len(entries))
	for index, entry := range entries {
		association, valid := entry.association.(routingDataAssociation)
		if !valid {
			return nil, errors.New("routing DATA live sender association does not expose DATA operations")
		}
		associations[index] = association
	}
	if len(senderInventory) != len(associations) {
		return nil, errors.New("routing DATA live sender inventory differs from actual associations")
	}
	return newRoutingDataSenderPlane(endpoint, associations, pairs, set.Close)
}

func routingDataLivePathEvidenceFrom(paths *routingPathMap) ([]routingDataLivePathEvidence, error) {
	evidence := make([]routingDataLivePathEvidence, routingRouteCount)
	for route := range routingRouteCount {
		path, err := paths.path(uint16(route))
		if err != nil {
			return nil, err
		}
		evidence[route] = routingDataLivePathEvidence{
			Route: uint16(route), Target: path.Target, PeerSGP: path.Binding.Peer.SGP,
			PeerAssociation: path.Binding.Peer.Association, PeerEpoch: path.Binding.PeerEpoch,
			MaxMessageStreamID: path.Binding.MaxMessageStreamID,
		}
	}
	return evidence, nil
}

func routingDataLiveWriteResult(testContext *testing.T, config routingDataLiveConfig, result routingDataLiveResult) {
	testContext.Helper()
	encoded, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		testContext.Fatal(err)
	}
	encoded = append(encoded, '\n')
	if err := routingLiveWriteAtomic(filepath.Join(config.ResultDir, config.Role+"-result.json"), encoded); err != nil {
		testContext.Fatal(err)
	}
}

func runRoutingDataLivePeer(testContext *testing.T, config routingDataLiveConfig) {
	testContext.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), routingDataLiveTimeout)
	defer cancel()
	topology, err := newRoutingTopology("primary")
	if err != nil {
		testContext.Fatal(err)
	}
	peers, err := startRoutingPeerSet(ctx, topology, routingLivePeerAddresses(config.PeerIP), nil)
	if err != nil {
		testContext.Fatal(err)
	}
	defer func() { _ = peers.Close() }()
	source, err := newRoutingM3UAPeerPreparationSource(topology, peers)
	if err != nil {
		testContext.Fatal(err)
	}
	preparation, err := newRoutingPeerPreparation(topology, source)
	if err != nil {
		testContext.Fatal(err)
	}
	operations := &routingDataLivePeerOperations{ctx: ctx, topology: topology, peers: peers, preparation: preparation}
	defer func() {
		if err := operations.shutdown(); err != nil {
			testContext.Errorf("routing DATA live peer operations cleanup: %v", err)
		}
	}()
	control, err := newRoutingControl(config.PreparationID, operations.controlOperations())
	if err != nil {
		testContext.Fatal(err)
	}
	dataHandler, err := newRoutingDataControlHandler(control.handler(), config.PreparationID, operations.dataOperations())
	if err != nil {
		testContext.Fatal(err)
	}
	handler, err := newRoutingDataLiveDirectHandler(dataHandler, config.PreparationID, operations.directOperations())
	if err != nil {
		testContext.Fatal(err)
	}
	listener, err := net.Listen("tcp", net.JoinHostPort(config.PeerIP.String(), "8080"))
	if err != nil {
		testContext.Fatal(err)
	}
	server := &http.Server{
		Handler: handler, ReadHeaderTimeout: routingControlTimeout, ReadTimeout: routingControlTimeout,
		WriteTimeout: routingControlTimeout, IdleTimeout: routingControlTimeout,
	}
	serverOwner := startRoutingLiveHTTPServer(server, listener)
	defer func() {
		if err := serverOwner.Close(); err != nil {
			testContext.Errorf("routing DATA live HTTP cleanup: %v", err)
		}
	}()
	if err := routingLiveWriteAtomic(filepath.Join(config.ResultDir, "peer-http-ready"), []byte("ready\n")); err != nil {
		testContext.Fatal(err)
	}
	if err := peers.WaitReady(ctx); err != nil {
		testContext.Fatal(err)
	}
	peerInventory, err := peers.Inventory()
	if err != nil {
		testContext.Fatal(err)
	}
	peerDTOs := routingLivePeerInventoryDTOs(testContext, peerInventory)
	select {
	case <-peers.Done():
	case <-ctx.Done():
		testContext.Fatal(ctx.Err())
	}
	if err := serverOwner.Close(); err != nil {
		testContext.Fatal(err)
	}
	operations.mutex.Lock()
	preflightReceipts := cloneRoutingDataLiveReceipts(operations.preflightReceipts)
	directReceipts := cloneRoutingDataLiveReceipts(operations.directReceipts)
	operations.mutex.Unlock()
	if len(preflightReceipts) != routingRouteCount || len(directReceipts) != routingDataLiveDirectCount {
		testContext.Fatalf("peer retained receipts=%d/%d", len(preflightReceipts), len(directReceipts))
	}
	routingDataLiveWriteResult(testContext, config, routingDataLiveResult{
		Role: config.Role, PreparationID: config.PreparationID, PeerInventory: peerDTOs,
		PreflightReceipts: preflightReceipts, DirectReceipts: directReceipts,
		OwnerClosed: true, ServerJoined: true,
	})
}

func runRoutingDataLiveSender(testContext *testing.T, config routingDataLiveConfig) {
	testContext.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), routingDataLiveTimeout)
	defer cancel()
	topology, err := newRoutingTopology("primary")
	if err != nil {
		testContext.Fatal(err)
	}
	local := &sctp.SCTPAddr{IPAddrs: []net.IPAddr{{IP: append(net.IP(nil), config.SenderIP...)}}, Port: 0}
	set, err := startRoutingSenderSet(ctx, topology, local, routingLivePeerAddresses(config.PeerIP), nil)
	if err != nil {
		testContext.Fatal(err)
	}
	defer func() { _ = set.Close() }()
	sender, err := newRoutingM3UASenderPreparation(set)
	if err != nil {
		testContext.Fatal(err)
	}
	preparationClient, err := newRoutingPreparationHTTPClient(config.ControlURL, config.PreparationID)
	if err != nil {
		testContext.Fatal(err)
	}
	remoteStopped := false
	defer func() {
		if !remoteStopped {
			stopContext, cancelStop := context.WithTimeout(context.Background(), routingControlTimeout)
			_ = preparationClient.Stop(stopContext)
			cancelStop()
		}
	}()
	if err := set.WaitReady(ctx); err != nil {
		testContext.Fatal(err)
	}
	senderInventory, err := sender.Inventory()
	if err != nil {
		testContext.Fatal(err)
	}
	senderDTOs := routingLiveSenderInventoryDTOs(testContext, senderInventory)
	routingLiveWaitPeerReady(testContext, ctx, preparationClient)
	ssnmEvidence, err := prepareRoutingSSNM(ctx, topology, sender, preparationClient)
	if err != nil {
		testContext.Fatal(err)
	}
	peerInventoryDTO, err := preparationClient.Inventory(ctx)
	if err != nil || !peerInventoryDTO.Ready {
		testContext.Fatalf("peer inventory after SSNM preparation: %+v %v", peerInventoryDTO, err)
	}
	peerInventory, err := routingPeerInventoryFromDTOs(peerInventoryDTO.Transports)
	if err != nil {
		testContext.Fatal(err)
	}
	pairs, err := pairRoutingInventory(topology, senderInventory, peerInventory)
	if err != nil {
		testContext.Fatal(err)
	}
	senderPlane, err := routingDataLiveSenderPlane(set, senderInventory, pairs)
	if err != nil {
		testContext.Fatal(err)
	}
	dataClient, err := newRoutingDataHTTPClient(config.ControlURL, config.PreparationID)
	if err != nil {
		testContext.Fatal(err)
	}
	recordingControl := &routingDataLiveRecordingControl{inner: dataClient}
	paths, writer, err := prepareRoutingData(ctx, topology, routingDataLivePreflightCohort, routingDataLivePreflightSeed, maxOutstanding, senderPlane, recordingControl)
	if err != nil {
		testContext.Fatal(err)
	}
	routes, err := routingDataLiveSelectedRoutes(&paths)
	if err != nil {
		testContext.Fatal(err)
	}
	directClient, err := newRoutingDataLiveDirectHTTPClient(config.ControlURL, config.PreparationID)
	if err != nil {
		testContext.Fatal(err)
	}
	if err := directClient.Start(ctx, routingDataLiveDirectCohort, routingDataLiveDirectSeed); err != nil {
		testContext.Fatal(err)
	}
	for _, route := range routes {
		payload, err := buildRoutePayload(planRouteMessage(routingDataLiveDirectCohort, routingDataLiveDirectSeed, uint64(route)), 128)
		if err != nil {
			testContext.Fatal(err)
		}
		if written, err := writer.Write(ctx, route, payload); err != nil || written != len(payload) {
			testContext.Fatalf("direct route %d wrote %d: %v", route, written, err)
		}
	}
	directReceipts, err := directClient.Complete(ctx)
	if err != nil {
		testContext.Fatal(err)
	}
	if err := routingDataLiveValidateDirect(&paths, routes, directReceipts, routingDataLiveDirectCohort, routingDataLiveDirectSeed); err != nil {
		testContext.Fatal(err)
	}
	finalSnapshot := sender.SSNMKnowledge()
	routingLiveValidateFinal(testContext, ssnmEvidence, finalSnapshot)
	frozenPaths, err := routingDataLivePathEvidenceFrom(&paths)
	if err != nil {
		testContext.Fatal(err)
	}
	stopContext, cancelStop := context.WithTimeout(context.Background(), routingControlTimeout)
	stopErr := preparationClient.Stop(stopContext)
	cancelStop()
	remoteStopped = stopErr == nil
	closeErr := sender.Close()
	if stopErr != nil || closeErr != nil {
		testContext.Fatalf("stop=%v sender close=%v", stopErr, closeErr)
	}
	routingDataLiveWriteResult(testContext, config, routingDataLiveResult{
		Role: config.Role, PreparationID: config.PreparationID, SenderInventory: senderDTOs,
		PeerInventory: routingLivePeerInventoryDTOs(testContext, peerInventory), SSNMEvidence: &ssnmEvidence,
		FinalSnapshot: &finalSnapshot, PreflightReceipts: recordingControl.receipts,
		FrozenPaths: frozenPaths, DirectRoutes: append([]uint16(nil), routes...),
		DirectReceipts: cloneRoutingDataLiveReceipts(directReceipts), OwnerClosed: true,
	})
}

func TestRoutingDataLiveIntegration(testContext *testing.T) {
	if os.Getenv("ROUTING_DATA_LIVE_ROLE") == "" {
		testContext.Skip("ROUTING_DATA_LIVE_ROLE is not set")
	}
	config := routingDataLiveConfigFromEnvironment(testContext)
	switch config.Role {
	case "peer":
		runRoutingDataLivePeer(testContext, config)
	case "sender":
		runRoutingDataLiveSender(testContext, config)
	default:
		testContext.Fatalf("unsupported ROUTING_DATA_LIVE_ROLE %q", config.Role)
	}
}
