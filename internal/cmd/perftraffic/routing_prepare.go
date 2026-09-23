package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"sync"

	"github.com/gomaja/go-m3ua"
)

type routingSSNMEventStream interface {
	Next(context.Context) (m3ua.SSNMEvent, error)
	Close() error
}

type routingPreparationSender interface {
	Inventory() ([]routingSenderInventory, error)
	ASPStatuses() []m3ua.ASPStatus
	SubscribeSSNM() (m3ua.SSNMSnapshot, routingSSNMEventStream, error)
	SSNMKnowledge() m3ua.SSNMSnapshot
	Close() error
}

type routingPreparationControl interface {
	Inventory(context.Context) (routingInventoryDTO, error)
	Prepare(context.Context, []routingTransportDTO) error
	Publish(context.Context, uint8) error
	Stop(context.Context) error
}

type routingPeerPreparationSource interface {
	Inventory() ([]routingPeerInventory, error)
	ASPStatuses(m3ua.SGPIdentity) []m3ua.ASPStatus
	ReportDestinationAvailability(m3ua.SGPIdentity, m3ua.DestinationAvailabilityRequest) error
	Close() error
}

type routingSenderSSNMEndpoint interface {
	ASPStatuses() []m3ua.ASPStatus
	SubscribeSSNM() (m3ua.SSNMSnapshot, *m3ua.SSNMSubscription, error)
	SSNMKnowledge() m3ua.SSNMSnapshot
}

type routingPeerSSNMEndpoint interface {
	ASPStatuses() []m3ua.ASPStatus
	ReportDestinationAvailability(m3ua.DestinationAvailabilityRequest) error
}

type routingM3UASenderPreparation struct {
	set      *routingSenderSet
	endpoint routingSenderSSNMEndpoint
}

func newRoutingM3UASenderPreparation(set *routingSenderSet) (*routingM3UASenderPreparation, error) {
	if set == nil || set.routingSetupOwner == nil || len(set.endpoints) != 1 {
		return nil, errors.New("routing preparation requires one sender endpoint")
	}
	endpoint, valid := set.endpoints[0].(routingSenderSSNMEndpoint)
	if !valid {
		return nil, errors.New("routing sender endpoint does not expose SSNM evidence")
	}
	return &routingM3UASenderPreparation{set: set, endpoint: endpoint}, nil
}

func (sender *routingM3UASenderPreparation) Inventory() ([]routingSenderInventory, error) {
	return sender.set.Inventory()
}

func (sender *routingM3UASenderPreparation) ASPStatuses() []m3ua.ASPStatus {
	return sender.endpoint.ASPStatuses()
}

func (sender *routingM3UASenderPreparation) SubscribeSSNM() (m3ua.SSNMSnapshot, routingSSNMEventStream, error) {
	initial, stream, err := sender.endpoint.SubscribeSSNM()
	if err != nil {
		return m3ua.SSNMSnapshot{}, nil, err
	}
	return initial, stream, nil
}

func (sender *routingM3UASenderPreparation) SSNMKnowledge() m3ua.SSNMSnapshot {
	return sender.endpoint.SSNMKnowledge()
}

func (sender *routingM3UASenderPreparation) Close() error {
	return sender.set.Close()
}

type routingM3UAPeerPreparationSource struct {
	set       *routingPeerSet
	endpoints map[m3ua.SGPIdentity]routingPeerSSNMEndpoint
}

func newRoutingM3UAPeerPreparationSource(topology routingTopology, set *routingPeerSet) (*routingM3UAPeerPreparationSource, error) {
	if set == nil || set.routingSetupOwner == nil || len(topology.Peers) != 4 || len(set.endpoints) != len(topology.Peers) {
		return nil, errors.New("routing preparation requires four peer endpoints")
	}
	endpoints := make(map[m3ua.SGPIdentity]routingPeerSSNMEndpoint, len(topology.Peers))
	for index, peer := range topology.Peers {
		endpoint, valid := set.endpoints[index].(routingPeerSSNMEndpoint)
		if !valid || endpoints[peer.Identity] != nil {
			return nil, errors.New("routing peer endpoint does not expose unique SSNM operations")
		}
		endpoints[peer.Identity] = endpoint
	}
	return &routingM3UAPeerPreparationSource{set: set, endpoints: endpoints}, nil
}

func (source *routingM3UAPeerPreparationSource) Inventory() ([]routingPeerInventory, error) {
	return source.set.Inventory()
}

func (source *routingM3UAPeerPreparationSource) ASPStatuses(sgp m3ua.SGPIdentity) []m3ua.ASPStatus {
	endpoint := source.endpoints[sgp]
	if endpoint == nil {
		return nil
	}
	return endpoint.ASPStatuses()
}

func (source *routingM3UAPeerPreparationSource) ReportDestinationAvailability(sgp m3ua.SGPIdentity, request m3ua.DestinationAvailabilityRequest) error {
	endpoint := source.endpoints[sgp]
	if endpoint == nil {
		return errors.New("routing SSNM publication has no peer endpoint")
	}
	return endpoint.ReportDestinationAvailability(request)
}

func (source *routingM3UAPeerPreparationSource) Close() error {
	return source.set.Close()
}

type routingPeerPreparation struct {
	topology routingTopology
	source   routingPeerPreparationSource

	mutex    sync.Mutex
	plan     routingSSNMPreparationPlan
	prepared bool
	next     uint8
	active   bool
	terminal error
	closed   bool

	closeOnce sync.Once
	closeErr  error
}

func newRoutingPeerPreparation(topology routingTopology, source routingPeerPreparationSource) (*routingPeerPreparation, error) {
	if source == nil || topology.ASP == nil || topology.ASP.Routing == nil || len(topology.ASP.Routing.Paths) != 2 || len(topology.ASP.Routing.Paths[0].ApplicationServers) != 2 {
		return nil, errors.New("routing peer preparation topology or source is incomplete")
	}
	owned, err := newRoutingTopology(topology.ASP.Routing.Paths[0].ApplicationServers[0])
	if err != nil || !reflect.DeepEqual(topology, owned) {
		return nil, errors.New("routing peer preparation topology differs from the frozen workload")
	}
	return &routingPeerPreparation{topology: owned, source: source}, nil
}

func (preparation *routingPeerPreparation) operations() routingControlOperations {
	return routingControlOperations{
		Inventory: preparation.inventory,
		Prepare:   preparation.prepare,
		Publish:   preparation.publish,
		Stop:      preparation.stop,
	}
}

func (preparation *routingPeerPreparation) inventory(ctx context.Context) (routingInventoryDTO, error) {
	if err := ctx.Err(); err != nil {
		return routingInventoryDTO{}, err
	}
	preparation.mutex.Lock()
	terminal, closed := preparation.terminal, preparation.closed
	preparation.mutex.Unlock()
	if terminal != nil || closed {
		return routingInventoryDTO{}, errors.Join(terminal, errors.New("routing peer preparation is terminal"))
	}
	peers, err := preparation.source.Inventory()
	if err != nil {
		return routingInventoryDTO{}, err
	}
	transports := make([]routingTransportDTO, len(peers))
	for index, peer := range peers {
		transports[index], err = routingTransportDTOFromPeer(peer)
		if err != nil {
			return routingInventoryDTO{}, err
		}
	}
	statuses := preparation.statuses()
	if err := validateRoutingPeerStatuses(preparation.topology, peers, statuses); err != nil {
		return routingInventoryDTO{}, err
	}
	if err := ctx.Err(); err != nil {
		return routingInventoryDTO{}, err
	}
	return routingInventoryDTO{Ready: true, Transports: transports, ASPStatuses: statuses}, nil
}

func (preparation *routingPeerPreparation) prepare(ctx context.Context, values []routingTransportDTO) error {
	if err := ctx.Err(); err != nil {
		return preparation.reject(err)
	}
	peers, err := preparation.source.Inventory()
	if err != nil {
		return preparation.reject(err)
	}
	if err := validateRoutingPeerStatuses(preparation.topology, peers, preparation.statuses()); err != nil {
		return preparation.reject(err)
	}
	senders, err := routingSenderInventoryFromDTOs(values)
	if err != nil {
		return preparation.reject(err)
	}
	pairs, err := pairRoutingInventory(preparation.topology, senders, peers)
	if err != nil {
		return preparation.reject(err)
	}
	plan, err := newRoutingSSNMPreparationPlan(preparation.topology, pairs)
	if err != nil {
		return preparation.reject(err)
	}
	preparation.mutex.Lock()
	defer preparation.mutex.Unlock()
	if preparation.closed || preparation.terminal != nil || preparation.prepared || ctx.Err() != nil {
		return preparation.rejectLocked(errors.Join(ctx.Err(), errors.New("routing peer preparation cannot be prepared")))
	}
	preparation.plan = plan
	preparation.prepared = true
	return nil
}

func (preparation *routingPeerPreparation) publish(ctx context.Context, ordinal uint8) error {
	preparation.mutex.Lock()
	if preparation.closed || preparation.terminal != nil || !preparation.prepared || preparation.active || ordinal != preparation.next {
		err := preparation.rejectLocked(errors.New("routing peer publication is out of order or terminal"))
		preparation.mutex.Unlock()
		return err
	}
	publication, err := preparation.plan.publication(ordinal)
	preparation.active = err == nil
	preparation.mutex.Unlock()
	if err != nil {
		return preparation.reject(err)
	}
	if err := ctx.Err(); err != nil {
		_ = preparation.closeSource()
		return preparation.finishPublication(err, false)
	}
	completed := make(chan struct{})
	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		select {
		case <-completed:
			return
		case <-ctx.Done():
			select {
			case <-completed:
				return
			default:
				_ = preparation.closeSource()
			}
		}
	}()
	operationErr := preparation.source.ReportDestinationAvailability(publication.SGP, publication.request())
	close(completed)
	<-watcherDone
	if contextErr := ctx.Err(); contextErr != nil {
		operationErr = errors.Join(contextErr, operationErr, preparation.closeSource())
	}
	if operationErr != nil {
		return preparation.finishPublication(operationErr, false)
	}
	return preparation.finishPublication(ctx.Err(), true)
}

func (preparation *routingPeerPreparation) statuses() []routingScopedASPStatusDTO {
	statuses := make([]routingScopedASPStatusDTO, 0, 16)
	for _, peer := range preparation.topology.Peers {
		for _, status := range preparation.source.ASPStatuses(peer.Identity) {
			statuses = append(statuses, routingScopedASPStatusDTO{SGP: peer.Identity, Status: status})
		}
	}
	return statuses
}

func (preparation *routingPeerPreparation) finishPublication(err error, successful bool) error {
	preparation.mutex.Lock()
	defer preparation.mutex.Unlock()
	preparation.active = false
	if err != nil || !successful || preparation.closed || preparation.terminal != nil {
		return preparation.rejectLocked(errors.Join(err, errors.New("routing peer publication ended terminally")))
	}
	preparation.next++
	return nil
}

func (preparation *routingPeerPreparation) stop(context.Context) error {
	preparation.mutex.Lock()
	preparation.closed = true
	preparation.mutex.Unlock()
	return preparation.closeSource()
}

func (preparation *routingPeerPreparation) closeSource() error {
	preparation.closeOnce.Do(func() { preparation.closeErr = preparation.source.Close() })
	return preparation.closeErr
}

func (preparation *routingPeerPreparation) reject(err error) error {
	preparation.mutex.Lock()
	defer preparation.mutex.Unlock()
	return preparation.rejectLocked(err)
}

func (preparation *routingPeerPreparation) rejectLocked(err error) error {
	if preparation.terminal == nil {
		preparation.terminal = err
	}
	return preparation.terminal
}

func routingSenderInventoryFromDTOs(values []routingTransportDTO) ([]routingSenderInventory, error) {
	if len(values) != 8 {
		return nil, errors.New("routing preparation requires eight sender transports")
	}
	result := make([]routingSenderInventory, len(values))
	for index, value := range values {
		if err := value.validate(m3ua.RoleASP); err != nil {
			return nil, err
		}
		result[index] = routingSenderInventory{
			SGP: value.SGP, ASPIdentifier: value.ASPIdentifier, Epoch: value.Epoch,
			MaxMessageStreamID: value.MaxMessageStreamID, Snapshot: routingSnapshotFromDTO(value),
		}
	}
	return result, nil
}

func routingPeerInventoryFromDTOs(values []routingTransportDTO) ([]routingPeerInventory, error) {
	if len(values) != 8 {
		return nil, errors.New("routing preparation requires eight peer transports")
	}
	result := make([]routingPeerInventory, len(values))
	for index, value := range values {
		if err := value.validate(m3ua.RoleSGP); err != nil {
			return nil, err
		}
		snapshot := routingSnapshotFromDTO(value)
		snapshot.PeerASPIdentifier = value.ASPIdentifier
		snapshot.PeerASPIdentifierSet = value.ASPIdentifierSet
		result[index] = routingPeerInventory{SGP: value.SGP, Epoch: value.Epoch, MaxMessageStreamID: value.MaxMessageStreamID, Snapshot: snapshot}
	}
	return result, nil
}

func routingSnapshotFromDTO(value routingTransportDTO) m3ua.AssociationSnapshot {
	return m3ua.AssociationSnapshot{
		Association: value.Association, Role: value.Role, State: value.State,
		LocalAddr: routingSCTPAddress(routingAddress(value.Local)), RemoteAddr: routingSCTPAddress(routingAddress(value.Remote)),
		SCTP: &m3ua.AssociationStatus{State: value.SCTPState, InboundStreams: value.InboundStreams, OutboundStreams: value.OutboundStreams},
	}
}

type routingPeerStatusKey struct {
	SGP         m3ua.SGPIdentity
	Association m3ua.AssociationID
	AS          m3ua.ASKey
}

func validateRoutingPeerStatuses(topology routingTopology, peers []routingPeerInventory, statuses []routingScopedASPStatusDTO) error {
	if len(peers) != 8 || len(statuses) != 16 {
		return errors.New("routing peer ASP status inventory is incomplete")
	}
	servers := make(map[m3ua.SGPIdentity][]m3ua.ASKey, len(topology.Peers))
	for _, peer := range topology.Peers {
		for _, server := range peer.ApplicationServers {
			servers[peer.Identity] = append(servers[peer.Identity], server.ASKey)
		}
	}
	expected := make(map[routingPeerStatusKey]uint32, 16)
	for _, peer := range peers {
		for _, key := range servers[peer.SGP] {
			expected[routingPeerStatusKey{SGP: peer.SGP, Association: peer.Snapshot.Association, AS: key}] = peer.Snapshot.PeerASPIdentifier
		}
	}
	if len(expected) != 16 {
		return errors.New("routing peer ASP status expectation is incomplete")
	}
	seen := make(map[routingPeerStatusKey]bool, len(expected))
	for _, scoped := range statuses {
		status := scoped.Status
		key := routingPeerStatusKey{SGP: scoped.SGP, Association: status.Key.Association, AS: status.Key.AS}
		identifier, known := expected[key]
		if !known || seen[key] || !status.PeerStateSet || status.PeerState != m3ua.StateASPActive ||
			!status.PeerASPIdentifierSet || status.PeerASPIdentifier != identifier ||
			status.LocalStateSet || status.LocalState != 0 || status.LocalASPIdentifierSet || status.LocalASPIdentifier != 0 {
			return errors.New("routing peer ASP status is missing, inactive or contradictory")
		}
		seen[key] = true
	}
	return nil
}

type routingPreparationEvent struct {
	ordinal uint8
	armed   bool
	event   m3ua.SSNMEvent
}

type routingPreparationArm struct {
	mutex   sync.Mutex
	ordinal uint8
	armed   bool
}

func (arm *routingPreparationArm) set(ordinal uint8, armed bool) {
	arm.mutex.Lock()
	arm.ordinal, arm.armed = ordinal, armed
	arm.mutex.Unlock()
}

func (arm *routingPreparationArm) tag(event m3ua.SSNMEvent) routingPreparationEvent {
	arm.mutex.Lock()
	defer arm.mutex.Unlock()
	return routingPreparationEvent{ordinal: arm.ordinal, armed: arm.armed, event: event}
}

func prepareRoutingSSNM(ctx context.Context, topology routingTopology, sender routingPreparationSender, control routingPreparationControl) (routingSSNMPreparationEvidence, error) {
	if sender == nil || control == nil {
		return routingSSNMPreparationEvidence{}, errors.New("routing preparation dependencies are nil")
	}
	if err := ctx.Err(); err != nil {
		return routingSSNMPreparationEvidence{}, routingPreparationFailed(err, sender, control)
	}
	senders, err := sender.Inventory()
	if err != nil {
		return routingSSNMPreparationEvidence{}, routingPreparationFailed(err, sender, control)
	}
	senderDTOs := make([]routingTransportDTO, len(senders))
	for index, inventory := range senders {
		senderDTOs[index], err = routingTransportDTOFromSender(inventory)
		if err != nil {
			return routingSSNMPreparationEvidence{}, routingPreparationFailed(err, sender, control)
		}
	}
	remote, err := control.Inventory(ctx)
	if err != nil {
		return routingSSNMPreparationEvidence{}, routingPreparationFailed(err, sender, control)
	}
	if !remote.Ready {
		return routingSSNMPreparationEvidence{}, routingPreparationFailed(errors.New("routing peer inventory is not ready"), sender, control)
	}
	peers, err := routingPeerInventoryFromDTOs(remote.Transports)
	if err != nil {
		return routingSSNMPreparationEvidence{}, routingPreparationFailed(err, sender, control)
	}
	if err := validateRoutingPeerStatuses(topology, peers, remote.ASPStatuses); err != nil {
		return routingSSNMPreparationEvidence{}, routingPreparationFailed(err, sender, control)
	}
	pairs, err := pairRoutingInventory(topology, senders, peers)
	if err != nil {
		return routingSSNMPreparationEvidence{}, routingPreparationFailed(err, sender, control)
	}
	plan, err := newRoutingSSNMPreparationPlan(topology, pairs)
	if err != nil {
		return routingSSNMPreparationEvidence{}, routingPreparationFailed(err, sender, control)
	}
	initial, stream, err := sender.SubscribeSSNM()
	if err != nil {
		return routingSSNMPreparationEvidence{}, routingPreparationFailed(err, sender, control)
	}
	oracle, err := plan.begin(sender.ASPStatuses(), initial)
	if err != nil {
		return routingSSNMPreparationEvidence{}, routingPreparationStreamFailed(err, stream, nil, nil, sender, control)
	}
	readerCtx, cancelReader := context.WithCancel(ctx)
	events := make(chan routingPreparationEvent, routingSSNMReceiptsPerPublication)
	readerDone := make(chan error, 1)
	var arm routingPreparationArm
	go func() {
		for {
			event, nextErr := stream.Next(readerCtx)
			if nextErr != nil {
				readerDone <- nextErr
				return
			}
			tagged := arm.tag(event)
			select {
			case events <- tagged:
			case <-readerCtx.Done():
				readerDone <- readerCtx.Err()
				return
			}
		}
	}()
	readerFinished := false
	var readerTerminal error
	fail := func(cause error) (routingSSNMPreparationEvidence, error) {
		cancelReader()
		closeErr := stream.Close()
		if !readerFinished {
			readerTerminal = <-readerDone
			readerFinished = true
		}
		return routingSSNMPreparationEvidence{}, routingPreparationFailed(errors.Join(cause, closeErr, readerTerminal), sender, control)
	}
	prepareCtx, cancelPrepare := context.WithTimeout(ctx, routingControlTimeout)
	err = control.Prepare(prepareCtx, senderDTOs)
	cancelPrepare()
	if err != nil {
		return fail(err)
	}
	for ordinal := uint8(0); ordinal < routingSSNMPublicationCount; ordinal++ {
		stepCtx, cancelStep := context.WithTimeout(ctx, routingControlTimeout)
		arm.set(ordinal, true)
		publishErr := control.Publish(stepCtx, ordinal)
		if publishErr != nil {
			arm.set(ordinal, false)
			cancelStep()
			return fail(publishErr)
		}
		for receipt := 0; receipt < routingSSNMReceiptsPerPublication; receipt++ {
			select {
			case tagged := <-events:
				if !tagged.armed || tagged.ordinal != ordinal {
					cancelStep()
					return fail(errors.New("routing SSNM event arrived outside its active publication"))
				}
				if err := oracle.observe(ordinal, tagged.event); err != nil {
					cancelStep()
					return fail(err)
				}
			case readerErr := <-readerDone:
				readerFinished, readerTerminal = true, readerErr
				cancelStep()
				return fail(readerErr)
			case <-stepCtx.Done():
				cancelStep()
				return fail(stepCtx.Err())
			}
		}
		arm.set(ordinal, false)
		if err := oracle.publicationComplete(ordinal); err != nil {
			cancelStep()
			return fail(err)
		}
		cancelStep()
	}
	arm.set(0, false)
	closeErr := stream.Close()
	var unexpected error
	ctxDone := ctx.Done()
	for !readerFinished {
		select {
		case <-events:
			unexpected = errors.New("routing SSNM stream retained an unexpected event after preparation")
		case readerErr := <-readerDone:
			readerFinished, readerTerminal = true, readerErr
			if !errors.Is(readerErr, m3ua.ErrSSNMSubscriptionClosed) {
				unexpected = errors.Join(unexpected, readerErr)
			}
			if drainRoutingPreparationEvents(events) {
				unexpected = errors.New("routing SSNM stream retained an unexpected event after preparation")
			}
		case <-ctxDone:
			cancelReader()
			unexpected = errors.Join(unexpected, ctx.Err())
			ctxDone = nil
		}
	}
	cancelReader()
	if closeErr != nil || unexpected != nil {
		return routingSSNMPreparationEvidence{}, routingPreparationFailed(errors.Join(closeErr, unexpected), sender, control)
	}
	evidence, err := oracle.finish(sender.SSNMKnowledge())
	if err != nil {
		return routingSSNMPreparationEvidence{}, routingPreparationFailed(err, sender, control)
	}
	return evidence, nil
}

func drainRoutingPreparationEvents(events <-chan routingPreparationEvent) bool {
	drained := false
	for {
		select {
		case <-events:
			drained = true
		default:
			return drained
		}
	}
}

func routingPreparationStreamFailed(cause error, stream routingSSNMEventStream, cancelReader context.CancelFunc, readerDone <-chan error, sender routingPreparationSender, control routingPreparationControl) error {
	if cancelReader != nil {
		cancelReader()
	}
	var closeErr, readerErr error
	if stream != nil {
		closeErr = stream.Close()
	}
	if readerDone != nil {
		readerErr = <-readerDone
	}
	return routingPreparationFailed(errors.Join(cause, closeErr, readerErr), sender, control)
}

func routingPreparationFailed(cause error, sender routingPreparationSender, control routingPreparationControl) error {
	cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), routingControlTimeout)
	stopErr := control.Stop(cleanupCtx)
	cancelCleanup()
	return errors.Join(cause, stopErr, sender.Close())
}

type routingPreparationHTTPClient struct {
	baseURL       string
	preparationID string
}

func newRoutingPreparationHTTPClient(baseURL, preparationID string) (*routingPreparationHTTPClient, error) {
	parsed, err := url.Parse(baseURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") || !routingControlIdentity(preparationID) {
		return nil, errors.New("routing preparation control address or identity is invalid")
	}
	return &routingPreparationHTTPClient{baseURL: strings.TrimRight(baseURL, "/"), preparationID: preparationID}, nil
}

func (client *routingPreparationHTTPClient) Inventory(ctx context.Context) (routingInventoryDTO, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, client.baseURL+"/routing/inventory", nil)
	if err != nil {
		return routingInventoryDTO{}, err
	}
	transport := &http.Client{Timeout: routingControlTimeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := transport.Do(request)
	if err != nil {
		return routingInventoryDTO{}, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return routingInventoryDTO{}, fmt.Errorf("routing control returned %s: %s", response.Status, body)
	}
	var result struct {
		PreparationID string `json:"preparation_id"`
		routingInventoryDTO
	}
	if err := decodeRoutingControlJSON(response.Body, &result); err != nil {
		return routingInventoryDTO{}, err
	}
	if result.PreparationID != client.preparationID {
		return routingInventoryDTO{}, errors.New("routing preparation identity mismatch")
	}
	return result.routingInventoryDTO, nil
}

func (client *routingPreparationHTTPClient) Prepare(ctx context.Context, transports []routingTransportDTO) error {
	ordinal := uint8(0)
	return client.post(ctx, "/routing/prepare", &ordinal, transports)
}

func (client *routingPreparationHTTPClient) Publish(ctx context.Context, ordinal uint8) error {
	if ordinal >= routingSSNMPublicationCount {
		return errors.New("routing publication ordinal is invalid")
	}
	wireOrdinal := ordinal + 1
	return client.post(ctx, "/routing/publication", &wireOrdinal, nil)
}

func (client *routingPreparationHTTPClient) Stop(ctx context.Context) error {
	return client.post(ctx, "/routing/stop", nil, nil)
}

func (client *routingPreparationHTTPClient) post(ctx context.Context, path string, ordinal *uint8, transports []routingTransportDTO) error {
	value := struct {
		PreparationID   string                `json:"preparation_id"`
		Ordinal         *uint8                `json:"ordinal,omitempty"`
		SenderInventory []routingTransportDTO `json:"sender_inventory,omitempty"`
	}{PreparationID: client.preparationID, Ordinal: ordinal, SenderInventory: transports}
	return postRoutingControlJSON(ctx, client.baseURL+path, value)
}

var _ routingSSNMEventStream = (*m3ua.SSNMSubscription)(nil)
var _ routingSetupEndpoint = routingM3UAEndpoint{}
var _ routingSenderSSNMEndpoint = routingM3UAEndpoint{}
var _ routingPeerSSNMEndpoint = routingM3UAEndpoint{}
var _ routingPreparationSender = (*routingM3UASenderPreparation)(nil)
var _ routingPeerPreparationSource = (*routingM3UAPeerPreparationSource)(nil)
