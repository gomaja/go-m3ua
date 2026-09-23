package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"sort"
	"sync"

	"github.com/gomaja/go-m3ua"
	"github.com/gomaja/go-sctp"
)

type routingSetupAssociation interface {
	ID() m3ua.AssociationID
	Epoch() uint64
	MaxMessageStreamID() uint16
}

type routingSetupListener interface {
	Accept(context.Context) (routingSetupAssociation, error)
	Close() error
}

type routingSetupEndpoint interface {
	Listen(*sctp.SCTPAddr, *m3ua.ListenerConfig) (routingSetupListener, error)
	Dial(context.Context, *sctp.SCTPAddr, *sctp.SCTPAddr, *m3ua.AssociationConfig) (routingSetupAssociation, error)
	AssociationStatus(m3ua.AssociationID) (m3ua.AssociationSnapshot, bool)
	Close() error
}

type routingSetupFactory func(m3ua.EndpointConfig) (routingSetupEndpoint, error)

type routingM3UAEndpoint struct {
	*m3ua.Endpoint
}

type routingM3UAListener struct {
	*m3ua.Listener
}

func (endpoint routingM3UAEndpoint) Listen(address *sctp.SCTPAddr, config *m3ua.ListenerConfig) (routingSetupListener, error) {
	listener, err := endpoint.Endpoint.Listen("m3ua", address, config)
	if err != nil {
		return nil, err
	}
	return routingM3UAListener{listener}, nil
}

func (endpoint routingM3UAEndpoint) Dial(ctx context.Context, local, remote *sctp.SCTPAddr, config *m3ua.AssociationConfig) (routingSetupAssociation, error) {
	association, err := endpoint.Endpoint.Dial(ctx, "m3ua", local, remote, config)
	if err != nil {
		return nil, err
	}
	return association, nil
}

func (listener routingM3UAListener) Accept(ctx context.Context) (routingSetupAssociation, error) {
	association, err := listener.Listener.Accept(ctx)
	if err != nil {
		return nil, err
	}
	return association, nil
}

func newRoutingSetupEndpoint(config m3ua.EndpointConfig) (routingSetupEndpoint, error) {
	endpoint, err := m3ua.NewEndpoint(config)
	if err != nil {
		return nil, err
	}
	return routingM3UAEndpoint{endpoint}, nil
}

type routingSetupEntry struct {
	peerIndex   int
	identity    m3ua.SGPIdentity
	identifier  uint32
	endpoint    routingSetupEndpoint
	association routingSetupAssociation
	local       routingAddress
	remote      routingAddress
}

type routingSetupOwner struct {
	ctx       context.Context
	cancel    context.CancelCauseFunc
	mutex     sync.Mutex
	entries   []routingSetupEntry
	role      m3ua.Role
	endpoints []routingSetupEndpoint
	listeners []routingSetupListener
	workers   sync.WaitGroup
	ready     chan struct{}
	done      chan struct{}
	err       error
}

type routingPeerSet struct{ *routingSetupOwner }
type routingSenderSet struct{ *routingSetupOwner }

var errRoutingSetupClosed = errors.New("routing setup owner closed")

func newRoutingSetupOwner(ctx context.Context, role m3ua.Role, endpoints []routingSetupEndpoint, listeners []routingSetupListener) *routingSetupOwner {
	lifetime, cancel := context.WithCancelCause(ctx)
	return &routingSetupOwner{ctx: lifetime, cancel: cancel, role: role, endpoints: endpoints, listeners: listeners, ready: make(chan struct{}), done: make(chan struct{})}
}

func closeRoutingSetupResources(endpoints []routingSetupEndpoint, listeners []routingSetupListener) error {
	var failures []error
	for _, listener := range listeners {
		failures = append(failures, listener.Close())
	}
	for _, endpoint := range endpoints {
		failures = append(failures, endpoint.Close())
	}
	return errors.Join(failures...)
}

func (owner *routingSetupOwner) finish() {
	<-owner.ctx.Done()
	cause := context.Cause(owner.ctx)
	if cause == errRoutingSetupClosed {
		cause = nil
	}
	cleanup := closeRoutingSetupResources(owner.endpoints, owner.listeners)
	owner.workers.Wait()
	owner.mutex.Lock()
	owner.err = errors.Join(cause, cleanup)
	owner.mutex.Unlock()
	close(owner.done)
}

func (owner *routingSetupOwner) Done() <-chan struct{} { return owner.done }

func (owner *routingSetupOwner) Close() error {
	owner.cancel(errRoutingSetupClosed)
	<-owner.done
	owner.mutex.Lock()
	defer owner.mutex.Unlock()
	return owner.err
}

func (owner *routingSetupOwner) WaitReady(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		owner.cancel(err)
		return errors.Join(err, owner.Close())
	}
	select {
	case <-ctx.Done():
		owner.cancel(ctx.Err())
		return errors.Join(ctx.Err(), owner.Close())
	case <-owner.done:
		return errors.Join(errRoutingSetupClosed, owner.Close())
	case <-owner.ready:
		if err := ctx.Err(); err != nil {
			owner.cancel(err)
			return errors.Join(err, owner.Close())
		}
		if owner.ctx.Err() != nil {
			return errors.Join(errRoutingSetupClosed, owner.Close())
		}
		return nil
	}
}

func (owner *routingSetupOwner) admit(entry routingSetupEntry) error {
	if _, _, _, err := captureRoutingSetupEntry(entry, owner.role); err != nil {
		return err
	}
	owner.mutex.Lock()
	defer owner.mutex.Unlock()
	if owner.ctx.Err() != nil {
		return context.Cause(owner.ctx)
	}
	for _, previous := range owner.entries {
		if (owner.role == m3ua.RoleASP || previous.peerIndex == entry.peerIndex) && previous.association.ID() == entry.association.ID() {
			return errors.New("routing setup received a duplicate association identity")
		}
	}
	owner.entries = append(owner.entries, entry)
	if len(owner.entries) == 8 {
		close(owner.ready)
	}
	return nil
}

func routingSetupInputs(topology routingTopology, addresses []*sctp.SCTPAddr) (routingTopology, []routingAddress, error) {
	if topology.ASP == nil || topology.ASP.Routing == nil || len(topology.ASP.Routing.Paths) != 2 || len(topology.ASP.Routing.Paths[0].ApplicationServers) != 2 || len(addresses) != 4 {
		return routingTopology{}, nil, errors.New("routing setup inventory is incomplete")
	}
	owned, err := newRoutingTopology(topology.ASP.Routing.Paths[0].ApplicationServers[0])
	if err != nil || !reflect.DeepEqual(topology, owned) {
		return routingTopology{}, nil, errors.New("routing setup topology differs from the approved fixture")
	}
	concrete := make([]routingAddress, len(addresses))
	seen := make(map[routingAddress]bool)
	for index, address := range addresses {
		value, err := canonicalRoutingAddress(address)
		if err != nil || seen[value] {
			return routingTopology{}, nil, errors.New("routing setup addresses must be concrete and distinct")
		}
		concrete[index] = value
		seen[value] = true
	}
	return owned, concrete, nil
}

func routingSCTPAddress(address routingAddress) *sctp.SCTPAddr {
	return &sctp.SCTPAddr{IPAddrs: []net.IPAddr{{IP: net.IP(address.Address.AsSlice())}}, Port: int(address.Port)}
}

func routingSetupAssociationConfig(peer routingPeer, identifier uint32, sender bool) *m3ua.AssociationConfig {
	config := m3ua.NewAssociationConfig().SetSCTPNoDelay(sctpNoDelay).SetSCTPSACK(sctpSACKDelay, sctpSACKFrequency).SetApplicationServers(peer.ApplicationServers...)
	config.HeartbeatInfo = &m3ua.HeartbeatInfo{Enabled: false}
	config.DataQueueSize = 1024
	if sender {
		identity := peer.Identity
		config.PeerSGP = &identity
		config.SetASPIdentifier(identifier)
	}
	return config
}

func startRoutingPeerSet(ctx context.Context, topology routingTopology, addresses []*sctp.SCTPAddr, factory routingSetupFactory) (*routingPeerSet, error) {
	owned, concrete, err := routingSetupInputs(topology, addresses)
	if err != nil {
		return nil, err
	}
	if factory == nil {
		factory = newRoutingSetupEndpoint
	}
	var endpoints []routingSetupEndpoint
	var listeners []routingSetupListener
	for index, peer := range owned.Peers {
		if err := ctx.Err(); err != nil {
			return nil, errors.Join(err, closeRoutingSetupResources(endpoints, listeners))
		}
		endpoint, err := factory(m3ua.EndpointConfig{Role: m3ua.RoleSGP})
		if err != nil {
			return nil, errors.Join(err, closeRoutingSetupResources(endpoints, listeners))
		}
		endpoints = append(endpoints, endpoint)
		listener, err := endpoint.Listen(routingSCTPAddress(concrete[index]), m3ua.NewListenerConfig(routingSetupAssociationConfig(peer, 0, false)))
		if err != nil {
			return nil, errors.Join(err, closeRoutingSetupResources(endpoints, listeners))
		}
		listeners = append(listeners, listener)
	}
	owner := newRoutingSetupOwner(ctx, m3ua.RoleSGP, endpoints, listeners)
	owner.workers.Add(len(listeners))
	for index, listener := range listeners {
		go func(peerIndex int, source routingSetupListener) {
			defer owner.workers.Done()
			for count := 0; ; count++ {
				association, err := source.Accept(owner.ctx)
				if err != nil {
					owner.cancel(err)
					return
				}
				if count >= 2 {
					owner.cancel(errors.New("routing SGP accepted an unexpected third association"))
					return
				}
				entry := routingSetupEntry{peerIndex: peerIndex, identity: owned.Peers[peerIndex].Identity, endpoint: endpoints[peerIndex], association: association, local: concrete[peerIndex]}
				if err := owner.admit(entry); err != nil {
					owner.cancel(err)
					return
				}
			}
		}(index, listener)
	}
	go owner.finish()
	return &routingPeerSet{owner}, nil
}

func startRoutingSenderSet(ctx context.Context, topology routingTopology, local *sctp.SCTPAddr, addresses []*sctp.SCTPAddr, factory routingSetupFactory) (*routingSenderSet, error) {
	owned, concrete, err := routingSetupInputs(topology, addresses)
	if err != nil {
		return nil, err
	}
	if local == nil || local.Port != 0 {
		return nil, errors.New("routing sender requires a concrete local address with an ephemeral port")
	}
	probe := *local
	probe.Port = 1
	localAddress, err := canonicalRoutingAddress(&probe)
	if err != nil {
		return nil, err
	}
	localAddress.Port = 0
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if factory == nil {
		factory = newRoutingSetupEndpoint
	}
	endpoint, err := factory(m3ua.EndpointConfig{Role: m3ua.RoleASP, ASP: owned.ASP})
	if err != nil {
		return nil, err
	}
	owner := newRoutingSetupOwner(ctx, m3ua.RoleASP, []routingSetupEndpoint{endpoint}, nil)
	owner.workers.Add(1)
	go func() {
		defer owner.workers.Done()
		for slot := 0; slot < 8; slot++ {
			peerIndex := slot / 2
			identifier := uint32(1001 + slot)
			association, err := endpoint.Dial(owner.ctx, routingSCTPAddress(localAddress), routingSCTPAddress(concrete[peerIndex]), routingSetupAssociationConfig(owned.Peers[peerIndex], identifier, true))
			if err != nil {
				owner.cancel(err)
				return
			}
			entry := routingSetupEntry{peerIndex: peerIndex, identity: owned.Peers[peerIndex].Identity, identifier: identifier, endpoint: endpoint, association: association, local: localAddress, remote: concrete[peerIndex]}
			if err := owner.admit(entry); err != nil {
				owner.cancel(err)
				return
			}
		}
	}()
	go owner.finish()
	return &routingSenderSet{owner}, nil
}

func cloneRoutingSetupAddress(source *sctp.SCTPAddr) *sctp.SCTPAddr {
	if source == nil {
		return nil
	}
	result := &sctp.SCTPAddr{Port: source.Port, IPAddrs: make([]net.IPAddr, len(source.IPAddrs))}
	for index, address := range source.IPAddrs {
		result.IPAddrs[index] = net.IPAddr{IP: append(net.IP(nil), address.IP...), Zone: address.Zone}
	}
	return result
}

func captureRoutingSetupEntry(entry routingSetupEntry, role m3ua.Role) (m3ua.AssociationSnapshot, uint64, uint16, error) {
	if entry.association == nil {
		return m3ua.AssociationSnapshot{}, 0, 0, errors.New("routing setup returned no association")
	}
	epoch := entry.association.Epoch()
	maximum := entry.association.MaxMessageStreamID()
	snapshot, present := entry.endpoint.AssociationStatus(entry.association.ID())
	if !present || snapshot.Association != entry.association.ID() || epoch != entry.association.Epoch() || maximum != entry.association.MaxMessageStreamID() {
		return m3ua.AssociationSnapshot{}, 0, 0, errors.New("routing association disappeared or changed during inventory snapshot")
	}
	addresses, err := validateRoutingInventorySnapshot(snapshot, role, epoch, maximum)
	if err != nil {
		return m3ua.AssociationSnapshot{}, 0, 0, err
	}
	if addresses.Local.Address != entry.local.Address || role == m3ua.RoleSGP && addresses.Local.Port != entry.local.Port || role == m3ua.RoleASP && addresses.Remote != entry.remote {
		return m3ua.AssociationSnapshot{}, 0, 0, errors.New("routing actual transport does not match its explicit bind or destination")
	}
	if role == m3ua.RoleSGP && !snapshot.PeerASPIdentifierSet {
		return m3ua.AssociationSnapshot{}, 0, 0, errors.New("routing peer did not supply its ASP identifier")
	}
	snapshot.LocalAddr = cloneRoutingSetupAddress(snapshot.LocalAddr)
	snapshot.RemoteAddr = cloneRoutingSetupAddress(snapshot.RemoteAddr)
	transport := *snapshot.SCTP
	snapshot.SCTP = &transport
	return snapshot, epoch, maximum, nil
}

func (owner *routingSetupOwner) inventoryEntries() ([]routingSetupEntry, error) {
	owner.mutex.Lock()
	defer owner.mutex.Unlock()
	if owner.ctx.Err() != nil || len(owner.entries) != 8 {
		return nil, errors.New("routing inventory is not ready or has been closed")
	}
	entries := append([]routingSetupEntry(nil), owner.entries...)
	sort.Slice(entries, func(first, second int) bool {
		if entries[first].peerIndex != entries[second].peerIndex {
			return entries[first].peerIndex < entries[second].peerIndex
		}
		return entries[first].association.ID() < entries[second].association.ID()
	})
	return entries, nil
}

func (owner *routingPeerSet) Inventory() ([]routingPeerInventory, error) {
	entries, err := owner.inventoryEntries()
	if err != nil {
		return nil, err
	}
	result := make([]routingPeerInventory, 0, len(entries))
	for _, entry := range entries {
		snapshot, epoch, maximum, err := captureRoutingSetupEntry(entry, m3ua.RoleSGP)
		if err != nil {
			return nil, fmt.Errorf("peer routing inventory: %w", err)
		}
		result = append(result, routingPeerInventory{SGP: entry.identity, Snapshot: snapshot, Epoch: epoch, MaxMessageStreamID: maximum})
	}
	if owner.ctx.Err() != nil {
		return nil, context.Cause(owner.ctx)
	}
	return result, nil
}

func (owner *routingSenderSet) Inventory() ([]routingSenderInventory, error) {
	entries, err := owner.inventoryEntries()
	if err != nil {
		return nil, err
	}
	result := make([]routingSenderInventory, 0, len(entries))
	for _, entry := range entries {
		snapshot, epoch, maximum, err := captureRoutingSetupEntry(entry, m3ua.RoleASP)
		if err != nil {
			return nil, fmt.Errorf("sender routing inventory: %w", err)
		}
		result = append(result, routingSenderInventory{SGP: entry.identity, ASPIdentifier: entry.identifier, Snapshot: snapshot, Epoch: epoch, MaxMessageStreamID: maximum})
	}
	if owner.ctx.Err() != nil {
		return nil, context.Cause(owner.ctx)
	}
	return result, nil
}
