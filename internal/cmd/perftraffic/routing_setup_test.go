package main

import (
	"context"
	"errors"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua"
	"github.com/gomaja/go-sctp"
)

type fakeRoutingAssociation struct {
	id      m3ua.AssociationID
	epoch   uint64
	maximum uint16
}

func (association *fakeRoutingAssociation) ID() m3ua.AssociationID { return association.id }
func (association *fakeRoutingAssociation) Epoch() uint64          { return association.epoch }
func (association *fakeRoutingAssociation) MaxMessageStreamID() uint16 {
	return association.maximum
}

type fakeRoutingListener struct {
	mutex       sync.Mutex
	incoming    chan routingSetupAssociation
	closed      chan struct{}
	closeOne    sync.Once
	contexts    []context.Context
	active      int
	closeErr    error
	exitGate    <-chan struct{}
	exitStarted chan struct{}
}

func (listener *fakeRoutingListener) Accept(ctx context.Context) (routingSetupAssociation, error) {
	listener.mutex.Lock()
	listener.contexts = append(listener.contexts, ctx)
	listener.active++
	listener.mutex.Unlock()
	defer func() {
		listener.mutex.Lock()
		gate, started := listener.exitGate, listener.exitStarted
		listener.mutex.Unlock()
		if gate != nil {
			close(started)
			<-gate
		}
		listener.mutex.Lock()
		listener.active--
		listener.mutex.Unlock()
	}()
	select {
	case association := <-listener.incoming:
		return association, nil
	case <-listener.closed:
		return nil, errors.New("listener closed")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (listener *fakeRoutingListener) Close() error {
	listener.closeOne.Do(func() { close(listener.closed) })
	return listener.closeErr
}

type fakeRoutingEndpoint struct {
	mutex       sync.Mutex
	listener    *fakeRoutingListener
	snapshots   map[m3ua.AssociationID]m3ua.AssociationSnapshot
	dialed      []routingSetupAssociation
	dialCtxs    []context.Context
	activeDials int
	dialConfig  []*m3ua.AssociationConfig
	dialLocal   []*sctp.SCTPAddr
	listenAddr  *sctp.SCTPAddr
	listenCfg   *m3ua.ListenerConfig
	listenErr   error
	dialErr     error
	failDial    int
	blockDial   bool
	closeErr    error
	closed      chan struct{}
	closeOne    sync.Once
}

func (endpoint *fakeRoutingEndpoint) Listen(address *sctp.SCTPAddr, config *m3ua.ListenerConfig) (routingSetupListener, error) {
	endpoint.listenAddr = address
	endpoint.listenCfg = config
	if endpoint.listenErr != nil {
		return nil, endpoint.listenErr
	}
	return endpoint.listener, nil
}

func (endpoint *fakeRoutingEndpoint) Dial(ctx context.Context, local, _ *sctp.SCTPAddr, config *m3ua.AssociationConfig) (routingSetupAssociation, error) {
	endpoint.mutex.Lock()
	index := len(endpoint.dialCtxs)
	endpoint.dialCtxs = append(endpoint.dialCtxs, ctx)
	endpoint.dialConfig = append(endpoint.dialConfig, config)
	endpoint.dialLocal = append(endpoint.dialLocal, local)
	endpoint.activeDials++
	endpoint.mutex.Unlock()
	defer func() {
		endpoint.mutex.Lock()
		endpoint.activeDials--
		endpoint.mutex.Unlock()
	}()
	if endpoint.blockDial {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if endpoint.failDial > 0 && index+1 == endpoint.failDial {
		return nil, endpoint.dialErr
	}
	if index >= len(endpoint.dialed) {
		return nil, errors.New("unexpected extra dial")
	}
	return endpoint.dialed[index], nil
}

func (endpoint *fakeRoutingEndpoint) AssociationStatus(identifier m3ua.AssociationID) (m3ua.AssociationSnapshot, bool) {
	endpoint.mutex.Lock()
	defer endpoint.mutex.Unlock()
	snapshot, present := endpoint.snapshots[identifier]
	return snapshot, present
}

func (endpoint *fakeRoutingEndpoint) Close() error {
	endpoint.closeOne.Do(func() {
		if endpoint.listener != nil {
			_ = endpoint.listener.Close()
		}
		close(endpoint.closed)
	})
	return endpoint.closeErr
}

type fakeRoutingFactory struct {
	endpoints []*fakeRoutingEndpoint
	configs   []m3ua.EndpointConfig
	failAt    int
	err       error
}

func (factory *fakeRoutingFactory) create(config m3ua.EndpointConfig) (routingSetupEndpoint, error) {
	index := len(factory.configs)
	factory.configs = append(factory.configs, config)
	if factory.failAt > 0 && index+1 == factory.failAt {
		return nil, factory.err
	}
	if index >= len(factory.endpoints) {
		return nil, errors.New("unexpected extra endpoint")
	}
	return factory.endpoints[index], nil
}

func routingSetupFixture(testContext *testing.T) (routingTopology, []*sctp.SCTPAddr, *fakeRoutingFactory, *fakeRoutingFactory) {
	testContext.Helper()
	topology, senders, peers := routingInventoryFixture(testContext)
	addresses := make([]*sctp.SCTPAddr, 4)
	peerFactory := &fakeRoutingFactory{}
	senderFactory := &fakeRoutingFactory{endpoints: []*fakeRoutingEndpoint{{snapshots: make(map[m3ua.AssociationID]m3ua.AssociationSnapshot), closed: make(chan struct{})}}}
	for peerIndex := range topology.Peers {
		addresses[peerIndex] = routingTestAddress("192.0.2.2", 2905+peerIndex)
		endpoint := &fakeRoutingEndpoint{
			listener:  &fakeRoutingListener{incoming: make(chan routingSetupAssociation, 3), closed: make(chan struct{})},
			snapshots: make(map[m3ua.AssociationID]m3ua.AssociationSnapshot), closed: make(chan struct{}),
		}
		for member := 0; member < 2; member++ {
			slot := peerIndex*2 + member
			peer := peers[slot]
			endpoint.listener.incoming <- &fakeRoutingAssociation{id: peer.Snapshot.Association, epoch: peer.Epoch, maximum: peer.MaxMessageStreamID}
			endpoint.snapshots[peer.Snapshot.Association] = peer.Snapshot
			sender := senders[slot]
			senderFactory.endpoints[0].dialed = append(senderFactory.endpoints[0].dialed, &fakeRoutingAssociation{id: sender.Snapshot.Association, epoch: sender.Epoch, maximum: sender.MaxMessageStreamID})
			senderFactory.endpoints[0].snapshots[sender.Snapshot.Association] = sender.Snapshot
		}
		peerFactory.endpoints = append(peerFactory.endpoints, endpoint)
	}
	return topology, addresses, peerFactory, senderFactory
}

func requireRoutingResourcesClosed(testContext *testing.T, factory *fakeRoutingFactory, count int) {
	testContext.Helper()
	for index := 0; index < count; index++ {
		endpoint := factory.endpoints[index]
		endpoint.mutex.Lock()
		activeDials := endpoint.activeDials
		endpoint.mutex.Unlock()
		if activeDials != 0 {
			testContext.Fatalf("endpoint %d retains %d unjoined Dial calls", index, activeDials)
		}
		select {
		case <-endpoint.closed:
		default:
			testContext.Fatalf("endpoint %d not closed", index)
		}
		if endpoint.listener != nil {
			endpoint.listener.mutex.Lock()
			active := endpoint.listener.active
			endpoint.listener.mutex.Unlock()
			if active != 0 {
				testContext.Fatalf("endpoint %d retains %d unjoined Accept calls", index, active)
			}
		}
	}
}

func TestRoutingSetupKeepsLifetimeAndExportsPairableOwnedInventory(testContext *testing.T) {
	topology, addresses, peerFactory, senderFactory := routingSetupFixture(testContext)
	peers, err := startRoutingPeerSet(context.Background(), topology, addresses, peerFactory.create)
	if err != nil {
		testContext.Fatal(err)
	}
	testContext.Cleanup(func() { _ = peers.Close() })
	senders, err := startRoutingSenderSet(context.Background(), topology, routingTestAddress("192.0.2.1", 0), addresses, senderFactory.create)
	if err != nil {
		testContext.Fatal(err)
	}
	testContext.Cleanup(func() { _ = senders.Close() })
	setupContext, cancelSetup := context.WithTimeout(context.Background(), time.Second)
	defer cancelSetup()
	if err := peers.WaitReady(setupContext); err != nil {
		testContext.Fatal(err)
	}
	if err := senders.WaitReady(setupContext); err != nil {
		testContext.Fatal(err)
	}
	cancelSetup()
	for _, endpoint := range peerFactory.endpoints {
		endpoint.listener.mutex.Lock()
		contexts := append([]context.Context(nil), endpoint.listener.contexts...)
		endpoint.listener.mutex.Unlock()
		for _, lifetime := range contexts {
			if lifetime.Err() != nil {
				testContext.Fatalf("successful Accept lifetime canceled: %v", lifetime.Err())
			}
		}
	}
	for _, lifetime := range senderFactory.endpoints[0].dialCtxs {
		if lifetime.Err() != nil {
			testContext.Fatalf("successful Dial lifetime canceled: %v", lifetime.Err())
		}
	}
	for slot, local := range senderFactory.endpoints[0].dialLocal {
		if local == nil || local.Port != 0 || len(local.IPAddrs) != 1 || !local.IPAddrs[0].IP.Equal(routingTestAddress("192.0.2.1", 0).IPAddrs[0].IP) {
			testContext.Fatalf("dial %d lost its explicit ephemeral-port bind: %+v", slot, local)
		}
		config := senderFactory.endpoints[0].dialConfig[slot]
		if config.PeerSGP == nil || *config.PeerSGP != topology.Peers[slot/2].Identity || config.ASPIdentifier == nil || config.ASPIdentifier.AspIdentifier() != uint32(1001+slot) || !reflect.DeepEqual(config.ApplicationServers, topology.Peers[slot/2].ApplicationServers) {
			testContext.Fatalf("dial %d scope or identity differs: %+v", slot, config)
		}
	}
	for index, endpoint := range peerFactory.endpoints {
		if endpoint.listenAddr == nil || endpoint.listenAddr.Port != 2905+index || len(endpoint.listenAddr.IPAddrs) != 1 || !endpoint.listenAddr.IPAddrs[0].IP.Equal(addresses[index].IPAddrs[0].IP) {
			testContext.Fatalf("listener %d lost its concrete bind: %+v", index, endpoint.listenAddr)
		}
		if endpoint.listenCfg == nil || endpoint.listenCfg.DefaultAssociationConfig == nil || !reflect.DeepEqual(endpoint.listenCfg.DefaultAssociationConfig.ApplicationServers, topology.Peers[index].ApplicationServers) || endpoint.listenCfg.DefaultAssociationConfig.PeerSGP != nil {
			testContext.Fatalf("listener %d scope differs: %+v", index, endpoint.listenCfg)
		}
	}
	peerInventory, err := peers.Inventory()
	if err != nil {
		testContext.Fatal(err)
	}
	senderInventory, err := senders.Inventory()
	if err != nil {
		testContext.Fatal(err)
	}
	if pairs, err := pairRoutingInventory(topology, senderInventory, peerInventory); err != nil || len(pairs) != 8 {
		testContext.Fatalf("live inventory seam is not pairable: count=%d error=%v", len(pairs), err)
	}
	peerInventory[0].Snapshot.SCTP.InboundStreams = 1
	clear(peerInventory[0].Snapshot.LocalAddr.IPAddrs[0].IP)
	fresh, err := peers.Inventory()
	if err != nil || fresh[0].Snapshot.SCTP.InboundStreams != 17 || fresh[0].Snapshot.LocalAddr.IPAddrs[0].IP.IsUnspecified() {
		testContext.Fatalf("exported inventory aliases runtime ownership: %+v %v", fresh, err)
	}
	if err := senders.Close(); err != nil {
		testContext.Fatal(err)
	}
	if err := peers.Close(); err != nil {
		testContext.Fatal(err)
	}
	requireRoutingResourcesClosed(testContext, peerFactory, 4)
	requireRoutingResourcesClosed(testContext, senderFactory, 1)
}

func TestRoutingSetupPartialBindFailureClosesEarlierResources(testContext *testing.T) {
	topology, addresses, factory, _ := routingSetupFixture(testContext)
	original := errors.New("third listener bind failed")
	cleanup := errors.New("first endpoint cleanup failed")
	factory.endpoints[2].listenErr = original
	factory.endpoints[0].closeErr = cleanup
	owner, err := startRoutingPeerSet(context.Background(), topology, addresses, factory.create)
	if owner != nil || !errors.Is(err, original) || !errors.Is(err, cleanup) {
		testContext.Fatalf("failure lost original or cleanup error: owner=%v error=%v", owner, err)
	}
	requireRoutingResourcesClosed(testContext, factory, 3)
}

func TestRoutingSetupPartialDialFailureClosesAndJoins(testContext *testing.T) {
	topology, addresses, _, factory := routingSetupFixture(testContext)
	original := errors.New("fourth dial failed")
	cleanup := errors.New("sender endpoint cleanup failed")
	factory.endpoints[0].failDial = 4
	factory.endpoints[0].dialErr = original
	factory.endpoints[0].closeErr = cleanup
	owner, err := startRoutingSenderSet(context.Background(), topology, routingTestAddress("192.0.2.1", 0), addresses, factory.create)
	if err != nil {
		testContext.Fatal(err)
	}
	setupContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := owner.WaitReady(setupContext); !errors.Is(err, original) || !errors.Is(err, cleanup) {
		testContext.Fatalf("failure lost original or cleanup error: %v", err)
	}
	requireRoutingResourcesClosed(testContext, factory, 1)
}

func TestRoutingSetupCancellationUnblocksAcceptAndDial(testContext *testing.T) {
	for _, side := range []string{"peer", "sender"} {
		testContext.Run(side, func(testContext *testing.T) {
			topology, addresses, peerFactory, senderFactory := routingSetupFixture(testContext)
			setupContext, cancel := context.WithCancel(context.Background())
			cancel()
			if side == "peer" {
				for _, endpoint := range peerFactory.endpoints {
					<-endpoint.listener.incoming
					<-endpoint.listener.incoming
				}
				owner, err := startRoutingPeerSet(context.Background(), topology, addresses, peerFactory.create)
				if err != nil {
					testContext.Fatal(err)
				}
				if err := owner.WaitReady(setupContext); !errors.Is(err, context.Canceled) {
					testContext.Fatalf("cancellation not preserved: %v", err)
				}
				requireRoutingResourcesClosed(testContext, peerFactory, 4)
			} else {
				senderFactory.endpoints[0].blockDial = true
				owner, err := startRoutingSenderSet(context.Background(), topology, routingTestAddress("192.0.2.1", 0), addresses, senderFactory.create)
				if err != nil {
					testContext.Fatal(err)
				}
				if err := owner.WaitReady(setupContext); !errors.Is(err, context.Canceled) {
					testContext.Fatalf("cancellation not preserved: %v", err)
				}
				requireRoutingResourcesClosed(testContext, senderFactory, 1)
			}
		})
	}
}

func TestRoutingSetupExpiredDeadlineClosesAndJoins(testContext *testing.T) {
	topology, addresses, factory, _ := routingSetupFixture(testContext)
	for _, endpoint := range factory.endpoints {
		<-endpoint.listener.incoming
		<-endpoint.listener.incoming
	}
	owner, err := startRoutingPeerSet(context.Background(), topology, addresses, factory.create)
	if err != nil {
		testContext.Fatal(err)
	}
	setupContext, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if err := owner.WaitReady(setupContext); !errors.Is(err, context.DeadlineExceeded) {
		testContext.Fatalf("deadline not preserved: %v", err)
	}
	requireRoutingResourcesClosed(testContext, factory, 4)
}

func TestRoutingSetupLifetimeCancellationClosesReadyOwners(testContext *testing.T) {
	topology, addresses, factory, _ := routingSetupFixture(testContext)
	lifetimeContext, cancelLifetime := context.WithCancel(context.Background())
	defer cancelLifetime()
	owner, err := startRoutingPeerSet(lifetimeContext, topology, addresses, factory.create)
	if err != nil {
		testContext.Fatal(err)
	}
	testContext.Cleanup(func() { _ = owner.Close() })
	setupContext, cancelSetup := context.WithTimeout(context.Background(), time.Second)
	defer cancelSetup()
	if err := owner.WaitReady(setupContext); err != nil {
		testContext.Fatal(err)
	}
	cancelLifetime()
	select {
	case <-owner.Done():
	case <-setupContext.Done():
		testContext.Fatal("owner outlived canceled lifetime context")
	}
	requireRoutingResourcesClosed(testContext, factory, 4)
}

func TestRoutingSetupUnexpectedThirdAssociationInvalidatesOwner(testContext *testing.T) {
	topology, addresses, factory, _ := routingSetupFixture(testContext)
	owner, err := startRoutingPeerSet(context.Background(), topology, addresses, factory.create)
	if err != nil {
		testContext.Fatal(err)
	}
	testContext.Cleanup(func() { _ = owner.Close() })
	setupContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := owner.WaitReady(setupContext); err != nil {
		testContext.Fatal(err)
	}
	factory.endpoints[0].listener.incoming <- &fakeRoutingAssociation{id: 3, epoch: 1, maximum: 8}
	select {
	case <-owner.Done():
	case <-setupContext.Done():
		testContext.Fatal("unexpected third association was silently ignored")
	}
	if inventory, err := owner.Inventory(); err == nil || len(inventory) != 0 {
		testContext.Fatalf("invalid owner exported inventory: %+v %v", inventory, err)
	}
	requireRoutingResourcesClosed(testContext, factory, 4)
}

func TestRoutingSetupConcurrentCloseJoinsAllAcceptors(testContext *testing.T) {
	topology, addresses, factory, _ := routingSetupFixture(testContext)
	owner, err := startRoutingPeerSet(context.Background(), topology, addresses, factory.create)
	if err != nil {
		testContext.Fatal(err)
	}
	var closers sync.WaitGroup
	for index := 0; index < 8; index++ {
		closers.Add(1)
		go func() { defer closers.Done(); _ = owner.Close() }()
	}
	closers.Wait()
	requireRoutingResourcesClosed(testContext, factory, 4)
	select {
	case <-owner.Done():
	default:
		testContext.Fatal("Close returned before the shared cleanup completed")
	}
}

func TestRoutingSetupRejectsImplicitOrAmbiguousBindsBeforeOpeningResources(testContext *testing.T) {
	for _, address := range []*sctp.SCTPAddr{nil, routingTestAddress("0.0.0.0", 0), routingTestAddress("::", 0), routingTestAddress("192.0.2.1", 40000), {IPAddrs: []net.IPAddr{{IP: net.ParseIP("192.0.2.1")}, {IP: net.ParseIP("192.0.2.3")}}}} {
		topology, addresses, _, factory := routingSetupFixture(testContext)
		owner, err := startRoutingSenderSet(context.Background(), topology, address, addresses, factory.create)
		if owner != nil || err == nil || len(factory.configs) != 0 {
			testContext.Fatalf("unsupported sender bind opened resources: %+v %v", address, err)
		}
	}
	for _, address := range []*sctp.SCTPAddr{nil, routingTestAddress("0.0.0.0", 2905), routingTestAddress("::", 2905), routingTestAddress("192.0.2.2", 0)} {
		topology, addresses, factory, _ := routingSetupFixture(testContext)
		addresses[0] = address
		owner, err := startRoutingPeerSet(context.Background(), topology, addresses, factory.create)
		if owner != nil || err == nil || len(factory.configs) != 0 {
			testContext.Fatalf("unsupported listener bind opened resources: %+v %v", address, err)
		}
	}
}

func TestRoutingSetupCloseWaitsForWorkerExit(testContext *testing.T) {
	topology, addresses, factory, _ := routingSetupFixture(testContext)
	owner, err := startRoutingPeerSet(context.Background(), topology, addresses, factory.create)
	if err != nil {
		testContext.Fatal(err)
	}
	setupContext, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := owner.WaitReady(setupContext); err != nil {
		testContext.Fatal(err)
	}
	gate := make(chan struct{})
	started := make(chan struct{})
	listener := factory.endpoints[0].listener
	listener.mutex.Lock()
	listener.exitGate, listener.exitStarted = gate, started
	listener.mutex.Unlock()
	closed := make(chan struct{})
	go func() { _ = owner.Close(); close(closed) }()
	defer func() { close(gate); <-closed }()
	select {
	case <-started:
	case <-setupContext.Done():
		testContext.Fatal("Close failed to unblock its Accept worker")
	}
	select {
	case <-closed:
		testContext.Fatal("Close returned before its worker exited")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestRoutingSetupRejectsUnexpectedActualLocalAddressAndPort(testContext *testing.T) {
	for _, testCase := range []struct {
		name    string
		address *sctp.SCTPAddr
	}{
		{name: "zero-actual-port", address: routingTestAddress("192.0.2.1", 0)},
		{name: "different-actual-IP", address: routingTestAddress("192.0.2.3", 40000)},
		{name: "wildcard-actual-IP", address: routingTestAddress("0.0.0.0", 40000)},
	} {
		testContext.Run(testCase.name, func(testContext *testing.T) {
			topology, addresses, _, factory := routingSetupFixture(testContext)
			snapshot := factory.endpoints[0].snapshots[101]
			snapshot.LocalAddr = testCase.address
			factory.endpoints[0].snapshots[101] = snapshot
			owner, err := startRoutingSenderSet(context.Background(), topology, routingTestAddress("192.0.2.1", 0), addresses, factory.create)
			if err != nil {
				testContext.Fatal(err)
			}
			setupContext, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := owner.WaitReady(setupContext); err == nil {
				_ = owner.Close()
				testContext.Fatal("setup accepted unverified actual local address")
			}
			requireRoutingResourcesClosed(testContext, factory, 1)
		})
	}
}
