package main

import (
	"bytes"
	"errors"
	"testing"

	"github.com/gomaja/go-m3ua"
)

func routingPreflightFixture(testContext *testing.T, preferred m3ua.RemoteASID) (routingTopology, []routingBinding, []routingPreflight) {
	testContext.Helper()
	topology, err := newRoutingTopology(preferred)
	if err != nil {
		testContext.Fatal(err)
	}
	bindings := make([]routingBinding, 8)
	for slot := range bindings {
		bindings[slot] = routingBinding{
			SenderAssociation: m3ua.AssociationID(101 + slot),
			Peer:              routingTransport{SGP: topology.Peers[slot/2].Identity, Association: m3ua.AssociationID(1 + slot%2)},
			PeerEpoch:         uint64(20 + slot), MaxMessageStreamID: 16,
		}
	}
	observations := make([]routingPreflight, 1000)
	for route := range observations {
		binding := bindings[route%8]
		peerIndex := (route % 8) / 2
		scopeIndex := 0
		if preferred == "secondary" {
			scopeIndex = 1
		}
		key := m3ua.ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContext: uint32(100 + 10*peerIndex + scopeIndex), RoutingContextSet: true}
		target := m3ua.MTPTransferPath{Path: topology.ASP.Routing.Paths[peerIndex/2].ID,
			SGP: binding.Peer.SGP, ApplicationServer: preferred, AS: key, Association: binding.SenderAssociation, Epoch: uint64(500 + 2*(peerIndex/2) + scopeIndex)}
		identity := planRouteMessage("preflight", 7, uint64(route))
		message := routingReceivedMessage(testContext, identity, 128, target, binding)
		observations[route] = routingPreflight{Route: uint16(route), Result: m3ua.MTPTransferResult{UserDataOctets: 128, SuccessfulPaths: []m3ua.MTPTransferPath{target}}, Transport: binding.Peer, Message: message}
	}
	return topology, bindings, observations
}

func routingReceivedMessage(testContext *testing.T, identity routingIdentity, size int, target m3ua.MTPTransferPath, binding routingBinding) *m3ua.DataMessage {
	testContext.Helper()
	payload, err := buildRoutePayload(identity, size)
	if err != nil {
		testContext.Fatal(err)
	}
	data, err := routeProtocolData(identity.Route, payload)
	if err != nil {
		testContext.Fatal(err)
	}
	return &m3ua.DataMessage{
		ProtocolData: &data, AS: target.AS,
		Scope: m3ua.WireScope{NetworkAppearance: target.AS.NetworkAppearance, NetworkAppearanceSet: true,
			RoutingContextSet: true, RoutingContexts: []uint32{target.AS.RoutingContext}},
		Association: binding.Peer.Association, Epoch: binding.PeerEpoch,
		Stream: uint16(data.SignallingLinkSelection)%binding.MaxMessageStreamID + 1,
	}
}

func TestRoutingPathsFreezeExplicitPreflightAndOwnTheirInventory(testContext *testing.T) {
	for _, preferred := range []m3ua.RemoteASID{"primary", "secondary"} {
		testContext.Run(string(preferred), func(testContext *testing.T) {
			topology, bindings, observations := routingPreflightFixture(testContext, preferred)
			for index := 0; index < len(observations)/2; index++ {
				other := len(observations) - 1 - index
				observations[index], observations[other] = observations[other], observations[index]
			}
			paths, err := freezeRoutingPaths(topology, bindings, observations, "preflight", 7)
			if err != nil {
				testContext.Fatal(err)
			}
			for _, observation := range observations {
				path, err := paths.path(observation.Route)
				if err != nil || path.Target != observation.Result.SuccessfulPaths[0] || path.Binding != bindings[int(observation.Route)%8] {
					testContext.Fatalf("route %d frozen path=%+v error=%v", observation.Route, path, err)
				}
				if _, err := validateRouteMessage(observation.Message, observation.Transport, "preflight", 7, workload128, &paths); err != nil {
					testContext.Fatalf("known reader-bound route %d rejected: %v", observation.Route, err)
				}
			}
			before, err := paths.path(999)
			if err != nil {
				testContext.Fatal(err)
			}
			observations[0].Result.SuccessfulPaths[0].Association = 9999
			observations[0].Message.Scope.RoutingContexts[0] = 9999
			clear(observations[0].Message.ProtocolData.Data)
			bindings[7].Peer.Association = 9999
			topology.ASP.Routing.Paths[1].ApplicationServers[0] = "changed"
			after, err := paths.path(999)
			if err != nil || before != after {
				testContext.Fatalf("frozen path aliases caller inputs: before=%+v after=%+v error=%v", before, after, err)
			}
			after.Target.Association = 7777
			again, err := paths.path(999)
			if err != nil || again != before {
				testContext.Fatal("path getter exposes mutable internal state")
			}
			if _, err := paths.path(1000); err == nil {
				testContext.Fatal("out-of-range route resolved")
			}
		})
	}
}

func TestRoutingPathsRejectIncompleteOrUntrustedPreflight(testContext *testing.T) {
	for _, testCase := range []struct {
		name   string
		change func(*routingTopology, *[]routingBinding, *[]routingPreflight)
	}{
		{name: "missing-route", change: func(_ *routingTopology, _ *[]routingBinding, observations *[]routingPreflight) {
			*observations = (*observations)[:999]
		}},
		{name: "duplicate-route", change: func(_ *routingTopology, _ *[]routingBinding, observations *[]routingPreflight) {
			(*observations)[999] = (*observations)[0]
		}},
		{name: "out-of-range-route", change: func(_ *routingTopology, _ *[]routingBinding, observations *[]routingPreflight) {
			(*observations)[0].Route = 1000
		}},
		{name: "missing-binding", change: func(_ *routingTopology, bindings *[]routingBinding, _ *[]routingPreflight) {
			*bindings = (*bindings)[:7]
		}},
		{name: "duplicate-sender", change: func(_ *routingTopology, bindings *[]routingBinding, _ *[]routingPreflight) {
			(*bindings)[1].SenderAssociation = (*bindings)[0].SenderAssociation
		}},
		{name: "duplicate-peer-transport", change: func(_ *routingTopology, bindings *[]routingBinding, _ *[]routingPreflight) {
			(*bindings)[1].Peer = (*bindings)[0].Peer
		}},
		{name: "unknown-peer", change: func(_ *routingTopology, bindings *[]routingBinding, _ *[]routingPreflight) {
			(*bindings)[0].Peer.SGP.SignallingGateway = "unknown"
		}},
		{name: "zero-sender", change: func(_ *routingTopology, bindings *[]routingBinding, _ *[]routingPreflight) {
			(*bindings)[0].SenderAssociation = 0
		}},
		{name: "zero-peer", change: func(_ *routingTopology, bindings *[]routingBinding, _ *[]routingPreflight) {
			(*bindings)[0].Peer.Association = 0
		}},
		{name: "zero-stream-limit", change: func(_ *routingTopology, bindings *[]routingBinding, _ *[]routingPreflight) {
			(*bindings)[0].MaxMessageStreamID = 0
		}},
		{name: "nil-topology", change: func(topology *routingTopology, _ *[]routingBinding, _ *[]routingPreflight) { topology.ASP = nil }},
		{name: "unknown-allowed", change: func(topology *routingTopology, _ *[]routingBinding, _ *[]routingPreflight) {
			topology.ASP.Routing.AllowUnknownDestinations = true
		}},
		{name: "send-error", change: func(_ *routingTopology, _ *[]routingBinding, observations *[]routingPreflight) {
			(*observations)[0].Err = errors.New("send failed")
		}},
		{name: "missing-target", change: func(_ *routingTopology, _ *[]routingBinding, observations *[]routingPreflight) {
			(*observations)[0].Result.SuccessfulPaths = nil
		}},
		{name: "multiple-targets", change: func(_ *routingTopology, _ *[]routingBinding, observations *[]routingPreflight) {
			first := &(*observations)[0]
			first.Result.SuccessfulPaths = append(first.Result.SuccessfulPaths, first.Result.SuccessfulPaths[0])
		}},
		{name: "short-write", change: func(_ *routingTopology, _ *[]routingBinding, observations *[]routingPreflight) {
			(*observations)[0].Result.UserDataOctets--
		}},
		{name: "unknown-sender", change: func(_ *routingTopology, _ *[]routingBinding, observations *[]routingPreflight) {
			(*observations)[0].Result.SuccessfulPaths[0].Association = 9999
		}},
		{name: "wrong-SGP", change: func(_ *routingTopology, _ *[]routingBinding, observations *[]routingPreflight) {
			(*observations)[0].Result.SuccessfulPaths[0].SGP = (*observations)[4].Result.SuccessfulPaths[0].SGP
		}},
		{name: "wrong-path", change: func(_ *routingTopology, _ *[]routingBinding, observations *[]routingPreflight) {
			(*observations)[0].Result.SuccessfulPaths[0].Path = "unknown"
		}},
		{name: "wrong-AS", change: func(_ *routingTopology, _ *[]routingBinding, observations *[]routingPreflight) {
			(*observations)[0].Result.SuccessfulPaths[0].ApplicationServer = "secondary"
		}},
		{name: "zero-knowledge-epoch", change: func(_ *routingTopology, _ *[]routingBinding, observations *[]routingPreflight) {
			(*observations)[0].Result.SuccessfulPaths[0].Epoch = 0
		}},
		{name: "changed-knowledge-epoch", change: func(_ *routingTopology, _ *[]routingBinding, observations *[]routingPreflight) {
			(*observations)[8].Result.SuccessfulPaths[0].Epoch++
		}},
		{name: "coherent-wrong-scope", change: func(_ *routingTopology, _ *[]routingBinding, observations *[]routingPreflight) {
			first := &(*observations)[0]
			first.Result.SuccessfulPaths[0].AS.RoutingContext++
			first.Message.AS.RoutingContext++
			first.Message.Scope.RoutingContexts[0]++
		}},
		{name: "wrong-scheduled-route", change: func(_ *routingTopology, _ *[]routingBinding, observations *[]routingPreflight) {
			(*observations)[0].Route, (*observations)[8].Route = (*observations)[8].Route, (*observations)[0].Route
		}},
		{name: "wrong-scheduled-sequence", change: func(_ *routingTopology, bindings *[]routingBinding, observations *[]routingPreflight) {
			first := &(*observations)[0]
			first.Message = routingReceivedMessage(testContext, planRouteMessage("preflight", 7, 1000), 128, first.Result.SuccessfulPaths[0], (*bindings)[0])
		}},
		{name: "wrong-reader", change: func(_ *routingTopology, _ *[]routingBinding, observations *[]routingPreflight) {
			(*observations)[0].Transport = (*observations)[2].Transport
		}},
		{name: "missing-DATA", change: func(_ *routingTopology, _ *[]routingBinding, observations *[]routingPreflight) {
			(*observations)[0].Message = nil
		}},
	} {
		testContext.Run(testCase.name, func(testContext *testing.T) {
			topology, bindings, observations := routingPreflightFixture(testContext, "primary")
			testCase.change(&topology, &bindings, &observations)
			if _, err := freezeRoutingPaths(topology, bindings, observations, "preflight", 7); err == nil {
				testContext.Fatal("invalid preflight accepted")
			}
		})
	}
}

func TestRoutingPathsRequireTrafficOnAllEightAssociations(testContext *testing.T) {
	topology, bindings, observations := routingPreflightFixture(testContext, "primary")
	for index := 7; index < len(observations); index += 8 {
		previous := observations[index-1]
		observations[index].Result = previous.Result
		observations[index].Transport = previous.Transport
		observations[index].Message = routingReceivedMessage(testContext, planRouteMessage("preflight", 7, uint64(index)), 128, previous.Result.SuccessfulPaths[0], bindings[6])
	}
	if _, err := freezeRoutingPaths(topology, bindings, observations, "preflight", 7); err == nil {
		testContext.Fatal("eight bindings but only seven used associations accepted")
	}
}

func TestRoutingMessageValidationRejectsMisdeliveryAndCorruption(testContext *testing.T) {
	topology, bindings, observations := routingPreflightFixture(testContext, "primary")
	paths, err := freezeRoutingPaths(topology, bindings, observations, "preflight", 7)
	if err != nil {
		testContext.Fatal(err)
	}
	path, err := paths.path(999)
	if err != nil {
		testContext.Fatal(err)
	}
	for _, testCase := range []struct {
		name   string
		change func(*m3ua.DataMessage)
	}{
		{name: "nil-protocol-data", change: func(message *m3ua.DataMessage) { message.ProtocolData = nil }},
		{name: "wrong-DPC", change: func(message *m3ua.DataMessage) { message.ProtocolData.DestinationPointCode-- }},
		{name: "wrong-OPC", change: func(message *m3ua.DataMessage) { message.ProtocolData.OriginatingPointCode-- }},
		{name: "wrong-SI", change: func(message *m3ua.DataMessage) { message.ProtocolData.ServiceIndicator ^= 1 }},
		{name: "wrong-NI", change: func(message *m3ua.DataMessage) { message.ProtocolData.NetworkIndicator ^= 1 }},
		{name: "wrong-priority", change: func(message *m3ua.DataMessage) { message.ProtocolData.MessagePriority ^= 1 }},
		{name: "wrong-SLS", change: func(message *m3ua.DataMessage) { message.ProtocolData.SignallingLinkSelection ^= 1 }},
		{name: "omitted-NA", change: func(message *m3ua.DataMessage) { message.Scope.NetworkAppearanceSet = false }},
		{name: "wrong-NA", change: func(message *m3ua.DataMessage) { message.Scope.NetworkAppearance++ }},
		{name: "omitted-RC", change: func(message *m3ua.DataMessage) { message.Scope.RoutingContextSet = false }},
		{name: "wrong-RC", change: func(message *m3ua.DataMessage) { message.Scope.RoutingContexts[0]++ }},
		{name: "extra-RC", change: func(message *m3ua.DataMessage) {
			message.Scope.RoutingContexts = append(message.Scope.RoutingContexts, message.Scope.RoutingContexts[0])
		}},
		{name: "wrong-resolved-AS", change: func(message *m3ua.DataMessage) { message.AS.RoutingContext++ }},
		{name: "wrong-association", change: func(message *m3ua.DataMessage) { message.Association++ }},
		{name: "wrong-epoch", change: func(message *m3ua.DataMessage) { message.Epoch++ }},
		{name: "zero-stream", change: func(message *m3ua.DataMessage) { message.Stream = 0 }},
		{name: "wrong-stream", change: func(message *m3ua.DataMessage) { message.Stream++ }},
		{name: "unexpected-correlation", change: func(message *m3ua.DataMessage) { message.CorrelationIDSet = true }},
		{name: "corrupt-body", change: func(message *m3ua.DataMessage) { message.ProtocolData.Data[127] ^= 1 }},
		{name: "zero-body", change: func(message *m3ua.DataMessage) { clear(message.ProtocolData.Data[48:]) }},
	} {
		testContext.Run(testCase.name, func(testContext *testing.T) {
			message := routingReceivedMessage(testContext, planRouteMessage("measured", 19, 999), 128, path.Target, path.Binding)
			if _, err := validateRouteMessage(message, path.Binding.Peer, "measured", 19, workload128, &paths); err != nil {
				testContext.Fatalf("unmodified message rejected: %v", err)
			}
			testCase.change(message)
			if _, err := validateRouteMessage(message, path.Binding.Peer, "measured", 19, workload128, &paths); err == nil {
				testContext.Fatal("misdelivered or corrupted message accepted")
			}
		})
	}
	message := routingReceivedMessage(testContext, planRouteMessage("measured", 19, 999), 128, path.Target, path.Binding)
	if _, err := validateRouteMessage(message, bindings[1].Peer, "measured", 19, workload128, &paths); err == nil {
		testContext.Fatal("same peer-local association ID from a different SGP accepted")
	}
	if _, err := validateRouteMessage(message, path.Binding.Peer, "other", 19, workload128, &paths); err == nil {
		testContext.Fatal("wrong cohort accepted")
	}
	if _, err := validateRouteMessage(message, path.Binding.Peer, "measured", 20, workload128, &paths); err == nil {
		testContext.Fatal("wrong seed accepted")
	}
	if _, err := validateRouteMessage(message, path.Binding.Peer, "measured", 19, workload512, &paths); err == nil {
		testContext.Fatal("wrong workload size accepted")
	}
	if _, err := validateRouteMessage(nil, path.Binding.Peer, "measured", 19, workload128, &paths); err == nil {
		testContext.Fatal("nil DATA accepted")
	}
	if _, err := validateRouteMessage(message, path.Binding.Peer, "measured", 19, workload128, nil); err == nil {
		testContext.Fatal("nil frozen map accepted")
	}
	original := bytes.Clone(message.ProtocolData.Data)
	if _, err := validateRouteMessage(message, path.Binding.Peer, "measured", 19, workload128, &paths); err != nil || !bytes.Equal(original, message.ProtocolData.Data) {
		testContext.Fatal("validation mutates owned DATA")
	}
}
