package main

import (
	"errors"
	"net/netip"
	"sort"

	"github.com/gomaja/go-m3ua"
	"github.com/gomaja/go-sctp"
)

type routingAddress struct {
	Address netip.Addr
	Port    uint16
}

type routingSenderInventory struct {
	SGP                m3ua.SGPIdentity
	ASPIdentifier      uint32
	Snapshot           m3ua.AssociationSnapshot
	Epoch              uint64
	MaxMessageStreamID uint16
}

type routingPeerInventory struct {
	SGP                m3ua.SGPIdentity
	Snapshot           m3ua.AssociationSnapshot
	Epoch              uint64
	MaxMessageStreamID uint16
}

type routingAssociationPair struct {
	Binding                routingBinding
	ASPIdentifier          uint32
	SenderEpoch            uint64
	SenderAddress          routingAddress
	PeerAddress            routingAddress
	SenderInboundStreams   uint16
	SenderOutboundStreams  uint16
	PeerInboundStreams     uint16
	PeerOutboundStreams    uint16
	PeerMaxMessageStreamID uint16
}

type routingAddressPair struct {
	Local  routingAddress
	Remote routingAddress
}

func canonicalRoutingAddress(source *sctp.SCTPAddr) (routingAddress, error) {
	if source == nil || len(source.IPAddrs) != 1 || source.Port < 1 || source.Port > 65535 || source.IPAddrs[0].Zone != "" {
		return routingAddress{}, errors.New("routing transport requires one concrete unzoned address and port")
	}
	address, valid := netip.AddrFromSlice(source.IPAddrs[0].IP)
	address = address.Unmap()
	if !valid || address.IsUnspecified() || address.IsMulticast() || address == netip.AddrFrom4([4]byte{255, 255, 255, 255}) {
		return routingAddress{}, errors.New("routing transport address is not unicast")
	}
	return routingAddress{Address: address, Port: uint16(source.Port)}, nil
}

func validateRoutingInventorySnapshot(snapshot m3ua.AssociationSnapshot, role m3ua.Role, epoch uint64, maximumStream uint16) (routingAddressPair, error) {
	if snapshot.Association == 0 || snapshot.Role != role || snapshot.State != m3ua.StateASPActive || epoch == 0 ||
		snapshot.SCTP == nil || snapshot.SCTPError != nil || snapshot.SCTP.State != "ESTABLISHED" ||
		snapshot.SCTP.InboundStreams < 2 || snapshot.SCTP.OutboundStreams < 2 || maximumStream != snapshot.SCTP.OutboundStreams-1 {
		return routingAddressPair{}, errors.New("routing association status, epoch or stream inventory is invalid")
	}
	local, err := canonicalRoutingAddress(snapshot.LocalAddr)
	if err != nil {
		return routingAddressPair{}, err
	}
	remote, err := canonicalRoutingAddress(snapshot.RemoteAddr)
	if err != nil {
		return routingAddressPair{}, err
	}
	return routingAddressPair{Local: local, Remote: remote}, nil
}

func pairRoutingInventory(topology routingTopology, senders []routingSenderInventory, peers []routingPeerInventory) ([]routingAssociationPair, error) {
	if len(topology.Peers) != 4 || len(senders) != 8 || len(peers) != 8 {
		return nil, errors.New("routing pairing requires four SGPs and eight associations at each side")
	}
	knownPeers := make(map[m3ua.SGPIdentity]bool)
	for _, peer := range topology.Peers {
		if peer.Associations != 2 || peer.Identity.SignallingGateway == "" || peer.Identity.SignallingGatewayProcess == "" || knownPeers[peer.Identity] {
			return nil, errors.New("routing peer topology is invalid or duplicated")
		}
		knownPeers[peer.Identity] = true
	}
	peerByIdentifier := make(map[uint32]routingPeerInventory)
	peerAddresses := make(map[uint32]routingAddressPair)
	peerTransports := make(map[routingTransport]bool)
	peerCounts := make(map[m3ua.SGPIdentity]int)
	for _, peer := range peers {
		identifier := peer.Snapshot.PeerASPIdentifier
		transport := routingTransport{SGP: peer.SGP, Association: peer.Snapshot.Association}
		_, duplicateIdentifier := peerByIdentifier[identifier]
		if !knownPeers[peer.SGP] || !peer.Snapshot.PeerASPIdentifierSet || duplicateIdentifier || peerTransports[transport] {
			return nil, errors.New("routing peer identity is missing, unknown or duplicated")
		}
		addresses, err := validateRoutingInventorySnapshot(peer.Snapshot, m3ua.RoleSGP, peer.Epoch, peer.MaxMessageStreamID)
		if err != nil {
			return nil, err
		}
		peerByIdentifier[identifier] = peer
		peerAddresses[identifier] = addresses
		peerTransports[transport] = true
		peerCounts[peer.SGP]++
	}
	senderIDs := make(map[m3ua.AssociationID]bool)
	senderIdentifiers := make(map[uint32]bool)
	senderAddresses := make(map[routingAddressPair]bool)
	senderCounts := make(map[m3ua.SGPIdentity]int)
	pairs := make([]routingAssociationPair, 0, len(senders))
	for _, sender := range senders {
		peer, found := peerByIdentifier[sender.ASPIdentifier]
		if !knownPeers[sender.SGP] || !found || peer.SGP != sender.SGP || senderIDs[sender.Snapshot.Association] || senderIdentifiers[sender.ASPIdentifier] {
			return nil, errors.New("routing sender identity is unmatched or duplicated")
		}
		addresses, err := validateRoutingInventorySnapshot(sender.Snapshot, m3ua.RoleASP, sender.Epoch, sender.MaxMessageStreamID)
		if err != nil {
			return nil, err
		}
		peerAddress := peerAddresses[sender.ASPIdentifier]
		if addresses.Local != peerAddress.Remote || addresses.Remote != peerAddress.Local || senderAddresses[addresses] {
			return nil, errors.New("routing transport tuple does not reverse uniquely")
		}
		if sender.Snapshot.SCTP.OutboundStreams != peer.Snapshot.SCTP.InboundStreams || sender.Snapshot.SCTP.InboundStreams != peer.Snapshot.SCTP.OutboundStreams {
			return nil, errors.New("routing negotiated streams disagree across the transport")
		}
		pairs = append(pairs, routingAssociationPair{
			Binding: routingBinding{
				SenderAssociation: sender.Snapshot.Association,
				Peer:              routingTransport{SGP: peer.SGP, Association: peer.Snapshot.Association},
				PeerEpoch:         peer.Epoch, MaxMessageStreamID: sender.MaxMessageStreamID,
			},
			ASPIdentifier: sender.ASPIdentifier, SenderEpoch: sender.Epoch,
			SenderAddress: addresses.Local, PeerAddress: addresses.Remote,
			SenderInboundStreams: sender.Snapshot.SCTP.InboundStreams, SenderOutboundStreams: sender.Snapshot.SCTP.OutboundStreams,
			PeerInboundStreams: peer.Snapshot.SCTP.InboundStreams, PeerOutboundStreams: peer.Snapshot.SCTP.OutboundStreams,
			PeerMaxMessageStreamID: peer.MaxMessageStreamID,
		})
		senderIDs[sender.Snapshot.Association] = true
		senderIdentifiers[sender.ASPIdentifier] = true
		senderAddresses[addresses] = true
		senderCounts[sender.SGP]++
	}
	for peer := range knownPeers {
		if senderCounts[peer] != 2 || peerCounts[peer] != 2 {
			return nil, errors.New("routing pairing requires two associations at every SGP")
		}
	}
	sort.Slice(pairs, func(first, second int) bool {
		return pairs[first].Binding.SenderAssociation < pairs[second].Binding.SenderAssociation
	})
	return pairs, nil
}
