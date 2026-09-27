package main

import (
	"bytes"
	"errors"
	"net"
	"net/netip"
	"reflect"
	"testing"

	"github.com/gomaja/go-m3ua"
	"github.com/gomaja/go-sctp"
)

func routingTestAddress(address string, port int) *sctp.Addr {
	return &sctp.Addr{IPs: []netip.Addr{netip.MustParseAddr(address)}, Port: uint16(port)}
}

func TestRoutingAddressCanonicalizesOwnedSingleAddress(testContext *testing.T) {
	for _, testCase := range []struct {
		input string
		want  string
	}{
		{input: "192.0.2.1", want: "192.0.2.1"},
		{input: "::ffff:192.0.2.1", want: "192.0.2.1"},
		{input: "2001:0db8:0:0::1", want: "2001:db8::1"},
		{input: "127.0.0.1", want: "127.0.0.1"},
		{input: "::1", want: "::1"},
	} {
		source := routingTestAddress(testCase.input, 65535)
		actual, err := canonicalRoutingAddress(source)
		if err != nil || actual.Address != netip.MustParseAddr(testCase.want) || actual.Port != 65535 {
			testContext.Fatalf("address %q: result=%+v error=%v", testCase.input, actual, err)
		}
		source.IPs[0] = netip.Addr{}
		source.Port = 1
		if actual.Address != netip.MustParseAddr(testCase.want) || actual.Port != 65535 {
			testContext.Fatal("canonical address aliases caller-owned memory")
		}
	}
	compact := routingTestAddress("192.0.2.1", 2905)
	wide := routingTestAddress("::ffff:192.0.2.1", 2905)
	first, firstErr := canonicalRoutingAddress(compact)
	second, secondErr := canonicalRoutingAddress(wide)
	if firstErr != nil || secondErr != nil || first != second {
		testContext.Fatalf("equivalent IP encodings differ: %+v/%v %+v/%v", first, firstErr, second, secondErr)
	}
}

func TestRoutingAddressRejectsUnsupportedOrAmbiguousInventory(testContext *testing.T) {
	for _, source := range []*sctp.Addr{
		nil, {},
		{IPs: []netip.Addr{{}}, Port: 2905},
		{IPs: []netip.Addr{netip.MustParseAddr("192.0.2.1"), netip.MustParseAddr("192.0.2.2")}, Port: 2905},
		{IPs: []netip.Addr{netip.MustParseAddr("192.0.2.1"), netip.MustParseAddr("192.0.2.1")}, Port: 2905},
		{IPs: []netip.Addr{netip.MustParseAddr("fe80::1%en0")}, Port: 2905},
		routingTestAddress("0.0.0.0", 2905),
		routingTestAddress("::", 2905),
		routingTestAddress("224.0.0.1", 2905),
		routingTestAddress("ff02::1", 2905),
		routingTestAddress("255.255.255.255", 2905),
		routingTestAddress("192.0.2.1", 0),
	} {
		if _, err := canonicalRoutingAddress(source); err == nil {
			testContext.Fatalf("invalid address accepted: %+v", source)
		}
	}
}

func FuzzRoutingAddress(fuzzContext *testing.F) {
	fuzzContext.Add([]byte{192, 0, 2, 1}, uint16(2905))
	fuzzContext.Add([]byte(net.ParseIP("::ffff:192.0.2.1")), uint16(65535))
	fuzzContext.Add([]byte{}, uint16(0))
	fuzzContext.Fuzz(func(testContext *testing.T, addressBytes []byte, port uint16) {
		original := append([]byte(nil), addressBytes...)
		ip, _ := netip.AddrFromSlice(addressBytes)
		source := &sctp.Addr{IPs: []netip.Addr{ip}, Port: port}
		address, err := canonicalRoutingAddress(source)
		if !bytes.Equal(original, addressBytes) {
			testContext.Fatal("canonicalization mutated its input")
		}
		if err != nil {
			return
		}
		if port == 0 || address.Port != port || !address.Address.IsValid() || address.Address.IsUnspecified() || address.Address.IsMulticast() || address.Address.Is4In6() {
			testContext.Fatalf("accepted noncanonical address: %+v", address)
		}
		roundTrip, err := canonicalRoutingAddress(&sctp.Addr{IPs: []netip.Addr{address.Address}, Port: address.Port})
		if err != nil || roundTrip != address {
			testContext.Fatalf("canonical address did not round trip: %+v %v", roundTrip, err)
		}
	})
}

func routingInventoryFixture(testContext *testing.T) (routingTopology, []routingSenderInventory, []routingPeerInventory) {
	testContext.Helper()
	topology, err := newRoutingTopology("primary")
	if err != nil {
		testContext.Fatal(err)
	}
	senders := make([]routingSenderInventory, 8)
	peers := make([]routingPeerInventory, 8)
	for slot := range senders {
		identity := topology.Peers[slot/2].Identity
		senders[slot] = routingSenderInventory{
			SGP: identity, ASPIdentifier: uint32(1001 + slot), Epoch: uint64(10 + slot), MaxMessageStreamID: 16,
			Snapshot: m3ua.AssociationSnapshot{
				Association: m3ua.AssociationID(101 + slot), Role: m3ua.RoleASP, State: m3ua.StateASPActive,
				LocalAddr: routingTestAddress("192.0.2.1", 40000+slot), RemoteAddr: routingTestAddress("192.0.2.2", 2905+slot/2),
				SCTP: &m3ua.AssociationStatus{State: "ESTABLISHED", InboundStreams: 9, OutboundStreams: 17},
			},
		}
		peers[slot] = routingPeerInventory{
			SGP: identity, Epoch: uint64(30 + slot), MaxMessageStreamID: 8,
			Snapshot: m3ua.AssociationSnapshot{
				Association: m3ua.AssociationID(1 + slot%2), Role: m3ua.RoleSGP, State: m3ua.StateASPActive,
				LocalAddr: routingTestAddress("192.0.2.2", 2905+slot/2), RemoteAddr: routingTestAddress("192.0.2.1", 40000+slot),
				PeerASPIdentifier: uint32(1001 + slot), PeerASPIdentifierSet: true,
				SCTP: &m3ua.AssociationStatus{State: "ESTABLISHED", InboundStreams: 17, OutboundStreams: 9},
			},
		}
	}
	return topology, senders, peers
}

func TestRoutingInventoryPairsByIdentityAndReversedActualTransport(testContext *testing.T) {
	topology, senders, peers := routingInventoryFixture(testContext)
	for index := 0; index < len(peers)/2; index++ {
		other := len(peers) - 1 - index
		peers[index], peers[other] = peers[other], peers[index]
	}
	senders[0], senders[7] = senders[7], senders[0]
	pairs, err := pairRoutingInventory(topology, senders, peers)
	if err != nil || len(pairs) != 8 {
		testContext.Fatalf("pairs=%+v error=%v", pairs, err)
	}
	for slot, pair := range pairs {
		if pair.Binding.SenderAssociation != m3ua.AssociationID(101+slot) ||
			pair.Binding.Peer != (routingTransport{SGP: topology.Peers[slot/2].Identity, Association: m3ua.AssociationID(1 + slot%2)}) ||
			pair.ASPIdentifier != uint32(1001+slot) || pair.SenderEpoch != uint64(10+slot) || pair.Binding.PeerEpoch != uint64(30+slot) ||
			pair.Binding.MaxMessageStreamID != 16 || pair.PeerMaxMessageStreamID != 8 ||
			pair.SenderInboundStreams != 9 || pair.SenderOutboundStreams != 17 || pair.PeerInboundStreams != 17 || pair.PeerOutboundStreams != 9 ||
			pair.SenderAddress.Address != netip.MustParseAddr("192.0.2.1") || pair.SenderAddress.Port != uint16(40000+slot) ||
			pair.PeerAddress.Address != netip.MustParseAddr("192.0.2.2") || pair.PeerAddress.Port != uint16(2905+slot/2) {
			testContext.Fatalf("slot %d has wrong pairing or lost provenance: %+v", slot, pair)
		}
	}
	before := append([]routingAssociationPair(nil), pairs...)
	senders[0].Snapshot.LocalAddr.IPs[0] = netip.Addr{}
	peers[0].Snapshot.SCTP.InboundStreams = 1
	senders[0].Epoch++
	peers[0].Epoch++
	topology.Peers[0].Identity.SignallingGateway = "changed"
	if !reflect.DeepEqual(pairs, before) {
		testContext.Fatal("returned pairs retain caller-owned mutable provenance")
	}
}

func TestRoutingInventoryRejectsIdentityTransportAndNegotiationMismatch(testContext *testing.T) {
	for _, testCase := range []struct {
		name   string
		change func(*routingTopology, *[]routingSenderInventory, *[]routingPeerInventory)
	}{
		{name: "missing-sender", change: func(_ *routingTopology, senders *[]routingSenderInventory, _ *[]routingPeerInventory) {
			*senders = (*senders)[:7]
		}},
		{name: "extra-peer", change: func(_ *routingTopology, _ *[]routingSenderInventory, peers *[]routingPeerInventory) {
			*peers = append(*peers, (*peers)[0])
		}},
		{name: "duplicate-sender-ID", change: func(_ *routingTopology, senders *[]routingSenderInventory, _ *[]routingPeerInventory) {
			(*senders)[1].Snapshot.Association = (*senders)[0].Snapshot.Association
		}},
		{name: "duplicate-peer-ID-in-SGP", change: func(_ *routingTopology, _ *[]routingSenderInventory, peers *[]routingPeerInventory) {
			(*peers)[1].Snapshot.Association = (*peers)[0].Snapshot.Association
		}},
		{name: "zero-sender-ID", change: func(_ *routingTopology, senders *[]routingSenderInventory, _ *[]routingPeerInventory) {
			(*senders)[0].Snapshot.Association = 0
		}},
		{name: "zero-peer-ID", change: func(_ *routingTopology, _ *[]routingSenderInventory, peers *[]routingPeerInventory) {
			(*peers)[0].Snapshot.Association = 0
		}},
		{name: "duplicate-configured-ASP-ID", change: func(_ *routingTopology, senders *[]routingSenderInventory, _ *[]routingPeerInventory) {
			(*senders)[1].ASPIdentifier = (*senders)[0].ASPIdentifier
		}},
		{name: "duplicate-observed-ASP-ID", change: func(_ *routingTopology, _ *[]routingSenderInventory, peers *[]routingPeerInventory) {
			(*peers)[1].Snapshot.PeerASPIdentifier = (*peers)[0].Snapshot.PeerASPIdentifier
		}},
		{name: "missing-observed-ASP-ID", change: func(_ *routingTopology, _ *[]routingSenderInventory, peers *[]routingPeerInventory) {
			(*peers)[0].Snapshot.PeerASPIdentifierSet = false
		}},
		{name: "wrong-observed-ASP-ID", change: func(_ *routingTopology, _ *[]routingSenderInventory, peers *[]routingPeerInventory) {
			(*peers)[0].Snapshot.PeerASPIdentifier += 100
		}},
		{name: "swapped-ASP-IDs-same-SGP", change: func(_ *routingTopology, _ *[]routingSenderInventory, peers *[]routingPeerInventory) {
			(*peers)[0].Snapshot.PeerASPIdentifier, (*peers)[1].Snapshot.PeerASPIdentifier = (*peers)[1].Snapshot.PeerASPIdentifier, (*peers)[0].Snapshot.PeerASPIdentifier
		}},
		{name: "duplicate-transport-tuple", change: func(_ *routingTopology, senders *[]routingSenderInventory, peers *[]routingPeerInventory) {
			(*senders)[1].Snapshot.LocalAddr.Port = (*senders)[0].Snapshot.LocalAddr.Port
			(*peers)[1].Snapshot.RemoteAddr.Port = (*peers)[0].Snapshot.RemoteAddr.Port
		}},
		{name: "wrong-peer-SGP", change: func(_ *routingTopology, _ *[]routingSenderInventory, peers *[]routingPeerInventory) {
			(*peers)[0].SGP = (*peers)[2].SGP
		}},
		{name: "unknown-sender-SGP", change: func(_ *routingTopology, senders *[]routingSenderInventory, _ *[]routingPeerInventory) {
			(*senders)[0].SGP.SignallingGateway = "unknown"
		}},
		{name: "zero-sender-epoch", change: func(_ *routingTopology, senders *[]routingSenderInventory, _ *[]routingPeerInventory) {
			(*senders)[0].Epoch = 0
		}},
		{name: "zero-peer-epoch", change: func(_ *routingTopology, _ *[]routingSenderInventory, peers *[]routingPeerInventory) {
			(*peers)[0].Epoch = 0
		}},
		{name: "wrong-local-port", change: func(_ *routingTopology, _ *[]routingSenderInventory, peers *[]routingPeerInventory) {
			(*peers)[0].Snapshot.LocalAddr.Port++
		}},
		{name: "wrong-remote-port", change: func(_ *routingTopology, _ *[]routingSenderInventory, peers *[]routingPeerInventory) {
			(*peers)[0].Snapshot.RemoteAddr.Port++
		}},
		{name: "wrong-local-IP", change: func(_ *routingTopology, _ *[]routingSenderInventory, peers *[]routingPeerInventory) {
			(*peers)[0].Snapshot.LocalAddr.IPs[0] = netip.MustParseAddr("192.0.2.3")
		}},
		{name: "wrong-remote-IP", change: func(_ *routingTopology, _ *[]routingSenderInventory, peers *[]routingPeerInventory) {
			(*peers)[0].Snapshot.RemoteAddr.IPs[0] = netip.MustParseAddr("192.0.2.3")
		}},
		{name: "missing-sender-status", change: func(_ *routingTopology, senders *[]routingSenderInventory, _ *[]routingPeerInventory) {
			(*senders)[0].Snapshot.SCTP = nil
		}},
		{name: "peer-status-error", change: func(_ *routingTopology, _ *[]routingSenderInventory, peers *[]routingPeerInventory) {
			(*peers)[0].Snapshot.SCTPError = errors.New("status unavailable")
		}},
		{name: "inactive-sender", change: func(_ *routingTopology, senders *[]routingSenderInventory, _ *[]routingPeerInventory) {
			(*senders)[0].Snapshot.State = m3ua.StateASPInactive
		}},
		{name: "inactive-peer", change: func(_ *routingTopology, _ *[]routingSenderInventory, peers *[]routingPeerInventory) {
			(*peers)[0].Snapshot.State = m3ua.StateASPInactive
		}},
		{name: "wrong-sender-role", change: func(_ *routingTopology, senders *[]routingSenderInventory, _ *[]routingPeerInventory) {
			(*senders)[0].Snapshot.Role = m3ua.RoleSGP
		}},
		{name: "wrong-peer-role", change: func(_ *routingTopology, _ *[]routingSenderInventory, peers *[]routingPeerInventory) {
			(*peers)[0].Snapshot.Role = m3ua.RoleASP
		}},
		{name: "SCTP-not-established", change: func(_ *routingTopology, _ *[]routingSenderInventory, peers *[]routingPeerInventory) {
			(*peers)[0].Snapshot.SCTP.State = "SHUTDOWN-PENDING"
		}},
		{name: "outbound-inbound-mismatch", change: func(_ *routingTopology, _ *[]routingSenderInventory, peers *[]routingPeerInventory) {
			(*peers)[0].Snapshot.SCTP.InboundStreams++
		}},
		{name: "reverse-outbound-inbound-mismatch", change: func(_ *routingTopology, _ *[]routingSenderInventory, peers *[]routingPeerInventory) {
			(*peers)[0].Snapshot.SCTP.OutboundStreams++
		}},
		{name: "sender-stream-limit-mismatch", change: func(_ *routingTopology, senders *[]routingSenderInventory, _ *[]routingPeerInventory) {
			(*senders)[0].MaxMessageStreamID--
		}},
		{name: "peer-stream-limit-mismatch", change: func(_ *routingTopology, _ *[]routingSenderInventory, peers *[]routingPeerInventory) {
			(*peers)[0].MaxMessageStreamID--
		}},
		{name: "no-sender-DATA-stream", change: func(_ *routingTopology, senders *[]routingSenderInventory, peers *[]routingPeerInventory) {
			(*senders)[0].MaxMessageStreamID = 0
			(*senders)[0].Snapshot.SCTP.OutboundStreams = 1
			(*peers)[0].Snapshot.SCTP.InboundStreams = 1
		}},
		{name: "wrong-topology-count", change: func(topology *routingTopology, _ *[]routingSenderInventory, _ *[]routingPeerInventory) {
			topology.Peers = topology.Peers[:3]
		}},
		{name: "duplicate-topology-SGP", change: func(topology *routingTopology, _ *[]routingSenderInventory, _ *[]routingPeerInventory) {
			topology.Peers[1].Identity = topology.Peers[0].Identity
		}},
		{name: "wrong-associations-per-SGP", change: func(topology *routingTopology, _ *[]routingSenderInventory, _ *[]routingPeerInventory) {
			topology.Peers[0].Associations = 1
		}},
	} {
		testContext.Run(testCase.name, func(testContext *testing.T) {
			topology, senders, peers := routingInventoryFixture(testContext)
			testCase.change(&topology, &senders, &peers)
			if pairs, err := pairRoutingInventory(topology, senders, peers); err == nil || len(pairs) != 0 {
				testContext.Fatalf("invalid inventory produced pairs=%+v error=%v", pairs, err)
			}
		})
	}
}
