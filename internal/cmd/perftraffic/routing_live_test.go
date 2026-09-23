package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua"
	"github.com/gomaja/go-sctp"
)

const routingLiveTimeout = 30 * time.Second

type routingLiveConfig struct {
	Role          string
	Case          string
	PreparationID string
	PeerIP        net.IP
	SenderIP      net.IP
	ControlURL    string
	ResultDir     string
}

type routingLiveResult struct {
	Role                 string                          `json:"role"`
	Case                 string                          `json:"case"`
	Associations         int                             `json:"associations"`
	InitialRevision      uint64                          `json:"initial_revision"`
	FinalRevision        uint64                          `json:"final_revision"`
	Receipts             int                             `json:"receipts,omitempty"`
	Partitions           int                             `json:"partitions,omitempty"`
	Destinations         int                             `json:"destinations,omitempty"`
	PrepareCalls         uint32                          `json:"prepare_calls,omitempty"`
	PublishCalls         uint32                          `json:"publish_calls,omitempty"`
	StopCalls            uint32                          `json:"stop_calls,omitempty"`
	CancellationObserved bool                            `json:"cancellation_observed,omitempty"`
	OwnerClosed          bool                            `json:"owner_closed"`
	ServerJoined         bool                            `json:"server_joined,omitempty"`
	SenderInventory      []routingTransportDTO           `json:"sender_inventory,omitempty"`
	PeerInventory        []routingTransportDTO           `json:"peer_inventory,omitempty"`
	Evidence             *routingSSNMPreparationEvidence `json:"evidence,omitempty"`
	FinalSnapshot        *m3ua.SSNMSnapshot              `json:"final_snapshot,omitempty"`
}

func TestRoutingLiveSSNMPreparation(testContext *testing.T) {
	if os.Getenv("ROUTING_LIVE_ROLE") == "" {
		testContext.Skip("ROUTING_LIVE_ROLE is not set")
	}
	config := routingLiveConfigFromEnvironment(testContext)
	switch config.Role {
	case "peer":
		runRoutingLivePeer(testContext, config)
	case "sender":
		runRoutingLiveSender(testContext, config)
	default:
		testContext.Fatalf("unsupported ROUTING_LIVE_ROLE %q", config.Role)
	}
}

func routingLiveConfigFromEnvironment(testContext *testing.T) routingLiveConfig {
	testContext.Helper()
	value := func(name string) string {
		result := os.Getenv(name)
		if result == "" {
			testContext.Fatalf("%s is required", name)
		}
		return result
	}
	peerIP := net.ParseIP(value("ROUTING_LIVE_PEER_IP"))
	senderIP := net.ParseIP(value("ROUTING_LIVE_SENDER_IP"))
	if peerIP == nil || senderIP == nil || peerIP.IsUnspecified() || senderIP.IsUnspecified() {
		testContext.Fatal("routing live addresses must be concrete IP addresses")
	}
	config := routingLiveConfig{
		Role:          value("ROUTING_LIVE_ROLE"),
		Case:          value("ROUTING_LIVE_CASE"),
		PreparationID: value("ROUTING_LIVE_PREPARATION_ID"),
		PeerIP:        peerIP,
		SenderIP:      senderIP,
		ControlURL:    value("ROUTING_LIVE_CONTROL_URL"),
		ResultDir:     value("ROUTING_LIVE_RESULT_DIR"),
	}
	if config.Case != "success" && config.Case != "cancel-after-prepare" {
		testContext.Fatalf("unsupported ROUTING_LIVE_CASE %q", config.Case)
	}
	return config
}

func routingLivePeerAddresses(ip net.IP) []*sctp.SCTPAddr {
	addresses := make([]*sctp.SCTPAddr, 4)
	for index := range addresses {
		addresses[index] = &sctp.SCTPAddr{IPAddrs: []net.IPAddr{{IP: append(net.IP(nil), ip...)}}, Port: 2905 + index}
	}
	return addresses
}

type routingLiveHTTPServerOwner struct {
	server    *http.Server
	listener  net.Listener
	serveDone chan error
	closeOnce sync.Once
	closeErr  error
}

func startRoutingLiveHTTPServer(server *http.Server, listener net.Listener) *routingLiveHTTPServerOwner {
	owner := &routingLiveHTTPServerOwner{server: server, listener: listener, serveDone: make(chan error, 1)}
	go func() { owner.serveDone <- server.Serve(listener) }()
	return owner
}

func (owner *routingLiveHTTPServerOwner) Close() error {
	owner.closeOnce.Do(func() {
		shutdownContext, cancelShutdown := context.WithTimeout(context.Background(), routingControlTimeout)
		shutdownErr := owner.server.Shutdown(shutdownContext)
		cancelShutdown()
		var fallbackErr error
		if shutdownErr != nil {
			fallbackErr = owner.server.Close()
		}
		var serveErr error
		select {
		case serveErr = <-owner.serveDone:
			if !errors.Is(serveErr, http.ErrServerClosed) {
				serveErr = fmt.Errorf("routing live HTTP Serve returned %v", serveErr)
			} else {
				serveErr = nil
			}
		case <-time.After(routingControlTimeout):
			serveErr = errors.New("routing live HTTP server did not terminate within its join bound")
		}
		owner.closeErr = errors.Join(shutdownErr, fallbackErr, serveErr)
	})
	return owner.closeErr
}

func runRoutingLivePeer(testContext *testing.T, config routingLiveConfig) {
	testContext.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), routingLiveTimeout)
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
	control, err := newRoutingControl(config.PreparationID, preparation.operations())
	if err != nil {
		testContext.Fatal(err)
	}
	listener, err := net.Listen("tcp", net.JoinHostPort(config.PeerIP.String(), "8080"))
	if err != nil {
		testContext.Fatal(err)
	}
	server := &http.Server{
		Handler:           control.handler(),
		ReadHeaderTimeout: routingControlTimeout,
		ReadTimeout:       routingControlTimeout,
		WriteTimeout:      routingControlTimeout,
		IdleTimeout:       routingControlTimeout,
	}
	serverOwner := startRoutingLiveHTTPServer(server, listener)
	defer func() {
		if err := serverOwner.Close(); err != nil {
			testContext.Errorf("routing live HTTP cleanup: %v", err)
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
	peerInventoryDTOs := routingLivePeerInventoryDTOs(testContext, peerInventory)
	select {
	case <-peers.Done():
	case <-ctx.Done():
		testContext.Fatal(ctx.Err())
	}
	if err := serverOwner.Close(); err != nil {
		testContext.Fatal(err)
	}
	routingLiveWriteResult(testContext, config, routingLiveResult{
		Role: config.Role, Case: config.Case, Associations: len(peerInventoryDTOs),
		PeerInventory: peerInventoryDTOs, OwnerClosed: true, ServerJoined: true,
	})
}

func runRoutingLiveSender(testContext *testing.T, config routingLiveConfig) {
	testContext.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), routingLiveTimeout)
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
	client, err := newRoutingPreparationHTTPClient(config.ControlURL, config.PreparationID)
	if err != nil {
		testContext.Fatal(err)
	}
	remoteStopped := false
	defer func() {
		if !remoteStopped {
			stopCtx, cancelStop := context.WithTimeout(context.Background(), routingControlTimeout)
			_ = client.Stop(stopCtx)
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
	senderInventoryDTOs := routingLiveSenderInventoryDTOs(testContext, senderInventory)
	routingLiveWaitPeerReady(testContext, ctx, client)
	if config.Case == "cancel-after-prepare" {
		runRoutingLiveCanceledSender(testContext, config, ctx, cancel, topology, sender, client, senderInventoryDTOs)
		remoteStopped = true
		return
	}
	evidence, err := prepareRoutingSSNM(ctx, topology, sender, client)
	if err != nil {
		testContext.Fatal(err)
	}
	finalSnapshot := sender.SSNMKnowledge()
	destinationCount := routingLiveValidateFinal(testContext, evidence, finalSnapshot)
	stopCtx, cancelStop := context.WithTimeout(context.Background(), routingControlTimeout)
	stopErr := client.Stop(stopCtx)
	cancelStop()
	remoteStopped = stopErr == nil
	closeErr := sender.Close()
	if stopErr != nil || closeErr != nil {
		testContext.Fatalf("stop=%v sender close=%v", stopErr, closeErr)
	}
	routingLiveWriteResult(testContext, config, routingLiveResult{
		Role: config.Role, Case: config.Case, Associations: len(senderInventoryDTOs), SenderInventory: senderInventoryDTOs,
		InitialRevision: evidence.InitialRevision, FinalRevision: evidence.FinalRevision,
		Receipts: evidence.ReceiptCount, Partitions: len(evidence.Partitions), Destinations: destinationCount,
		PrepareCalls: 1, PublishCalls: routingSSNMPublicationCount, StopCalls: 1, OwnerClosed: true,
		Evidence: &evidence, FinalSnapshot: &finalSnapshot,
	})
}

type routingLiveCancelAfterPrepare struct {
	inner        *routingPreparationHTTPClient
	cancel       context.CancelFunc
	prepareCalls atomic.Uint32
	publishCalls atomic.Uint32
	stopCalls    atomic.Uint32
}

func (control *routingLiveCancelAfterPrepare) Inventory(ctx context.Context) (routingInventoryDTO, error) {
	return control.inner.Inventory(ctx)
}

func (control *routingLiveCancelAfterPrepare) Prepare(ctx context.Context, transports []routingTransportDTO) error {
	control.prepareCalls.Add(1)
	if err := control.inner.Prepare(ctx, transports); err != nil {
		return err
	}
	control.cancel()
	return context.Canceled
}

func (control *routingLiveCancelAfterPrepare) Publish(ctx context.Context, ordinal uint8) error {
	control.publishCalls.Add(1)
	return control.inner.Publish(ctx, ordinal)
}

func (control *routingLiveCancelAfterPrepare) Stop(ctx context.Context) error {
	control.stopCalls.Add(1)
	return control.inner.Stop(ctx)
}

func runRoutingLiveCanceledSender(testContext *testing.T, config routingLiveConfig, ctx context.Context, cancel context.CancelFunc, topology routingTopology, sender *routingM3UASenderPreparation, client *routingPreparationHTTPClient, senderInventory []routingTransportDTO) {
	testContext.Helper()
	control := &routingLiveCancelAfterPrepare{inner: client, cancel: cancel}
	_, err := prepareRoutingSSNM(ctx, topology, sender, control)
	if !errors.Is(err, context.Canceled) {
		testContext.Fatalf("prepare error=%v; want context cancellation", err)
	}
	if control.prepareCalls.Load() != 1 || control.publishCalls.Load() != 0 || control.stopCalls.Load() != 1 {
		testContext.Fatalf("calls prepare=%d publish=%d stop=%d", control.prepareCalls.Load(), control.publishCalls.Load(), control.stopCalls.Load())
	}
	select {
	case <-sender.set.Done():
	case <-time.After(routingControlTimeout):
		testContext.Fatal("sender owner did not close")
	}
	routingLiveWriteResult(testContext, config, routingLiveResult{
		Role: config.Role, Case: config.Case, Associations: len(senderInventory), SenderInventory: senderInventory,
		PrepareCalls: control.prepareCalls.Load(), PublishCalls: control.publishCalls.Load(), StopCalls: control.stopCalls.Load(),
		CancellationObserved: true, OwnerClosed: true,
	})
}

func routingLiveSenderInventoryDTOs(testContext *testing.T, inventory []routingSenderInventory) []routingTransportDTO {
	testContext.Helper()
	result := make([]routingTransportDTO, len(inventory))
	for index, entry := range inventory {
		value, err := routingTransportDTOFromSender(entry)
		if err != nil {
			testContext.Fatal(err)
		}
		result[index] = value
	}
	return result
}

func routingLivePeerInventoryDTOs(testContext *testing.T, inventory []routingPeerInventory) []routingTransportDTO {
	testContext.Helper()
	result := make([]routingTransportDTO, len(inventory))
	for index, entry := range inventory {
		value, err := routingTransportDTOFromPeer(entry)
		if err != nil {
			testContext.Fatal(err)
		}
		result[index] = value
	}
	return result
}

func routingLiveWaitPeerReady(testContext *testing.T, ctx context.Context, client *routingPreparationHTTPClient) {
	testContext.Helper()
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		requestCtx, cancel := context.WithTimeout(ctx, routingControlTimeout)
		inventory, err := client.Inventory(requestCtx)
		cancel()
		if err == nil && inventory.Ready && len(inventory.Transports) == 8 && len(inventory.ASPStatuses) == 16 {
			return
		}
		select {
		case <-ctx.Done():
			testContext.Fatalf("peer inventory never became ready: %v", errors.Join(ctx.Err(), err))
		case <-ticker.C:
		}
	}
}

func routingLiveValidateFinal(testContext *testing.T, evidence routingSSNMPreparationEvidence, snapshot m3ua.SSNMSnapshot) int {
	testContext.Helper()
	if evidence.ReceiptCount != routingSSNMReceiptCount || evidence.FinalRevision != evidence.InitialRevision+routingSSNMReceiptCount {
		testContext.Fatalf("receipt evidence=%d revisions=%d..%d", evidence.ReceiptCount, evidence.InitialRevision, evidence.FinalRevision)
	}
	if snapshot.Revision != evidence.FinalRevision || snapshot.RecordsRefused != 0 || snapshot.ReportsRefused != 0 ||
		snapshot.PartitionsInvalidated != 0 || snapshot.LastResourceLoss != "" || len(snapshot.Partitions) != routingSSNMPartitionCount {
		testContext.Fatalf("final snapshot metadata is invalid: %+v", snapshot)
	}
	latest := make(map[m3ua.SSNMPartition]routingSSNMReceipt, routingSSNMPartitionCount)
	partitionEpochs := make(map[m3ua.SSNMPartition]uint64, routingSSNMPartitionCount)
	for _, partition := range evidence.Partitions {
		if partition.Partition.Kind != m3ua.SSNMCanonicalPartition || partition.Epoch == 0 || partitionEpochs[partition.Partition] != 0 {
			testContext.Fatalf("partition evidence is invalid: %+v", partition)
		}
		partitionEpochs[partition.Partition] = partition.Epoch
	}
	for index := 0; index < evidence.ReceiptCount; index++ {
		receipt := evidence.Receipts[index]
		if receipt.Revision > latest[receipt.Partition].Revision {
			latest[receipt.Partition] = receipt
		}
	}
	seen := make(map[m3ua.SSNMPartition]bool, routingSSNMPartitionCount)
	destinationCount := 0
	for _, partition := range snapshot.Partitions {
		receipt, present := latest[partition.Partition]
		if !present || seen[partition.Partition] || partition.Partition.Kind != m3ua.SSNMCanonicalPartition ||
			partition.Epoch == 0 || partitionEpochs[partition.Partition] != partition.Epoch || !partition.TrafficAuthorized ||
			len(partition.Bindings) != 4 || len(partition.Destinations) != routingRouteCount {
			testContext.Fatalf("final partition is invalid: %+v", partition.Partition)
		}
		seen[partition.Partition] = true
		bindings := append([]m3ua.SSNMBinding(nil), partition.Bindings...)
		sort.Slice(bindings, func(first, second int) bool { return bindings[first].Association < bindings[second].Association })
		for index, binding := range bindings {
			if binding.Association == 0 || binding.Pending || (index > 0 && binding.Association == bindings[index-1].Association) {
				testContext.Fatalf("final binding is invalid: %+v", binding)
			}
		}
		for index, destination := range partition.Destinations {
			availability := destination.Availability
			if destination.Destination != (m3ua.PointCodeRange{PointCode: 0x220000 + uint32(index)}) ||
				!destination.AvailabilitySet || destination.CongestionSet || availability.State != m3ua.DestinationAvailable ||
				availability.Kind != m3ua.SSNMDestinationAvailableReport || availability.Source != m3ua.SSNMPeerReport ||
				availability.Association != receipt.Association || availability.Epoch != receipt.Epoch || availability.Revision != receipt.Revision ||
				!sameRoutingSSNMScope(availability.Scope, receipt.Scope) {
				testContext.Fatalf("final destination %d in %+v is invalid", index, partition.Partition)
			}
		}
		destinationCount += len(partition.Destinations)
	}
	if destinationCount != routingSSNMPartitionCount*routingRouteCount {
		testContext.Fatalf("destination count=%d", destinationCount)
	}
	return destinationCount
}

func routingLiveWriteResult(testContext *testing.T, config routingLiveConfig, result routingLiveResult) {
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

func routingLiveWriteAtomic(path string, contents []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".routing-live-*")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer func() { _ = os.Remove(temporaryName) }()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(contents); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if _, err := os.Lstat(path); err == nil {
		return fmt.Errorf("routing live result path already exists: %s", path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return os.Rename(temporaryName, path)
}
