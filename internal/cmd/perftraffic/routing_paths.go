package main

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"

	"github.com/gomaja/go-m3ua"
)

type routingTransport struct {
	SGP         m3ua.SGPIdentity
	Association m3ua.AssociationID
}

type routingBinding struct {
	SenderAssociation  m3ua.AssociationID
	Peer               routingTransport
	PeerEpoch          uint64
	MaxMessageStreamID uint16
}

type routingPreflight struct {
	Route     uint16
	Result    m3ua.MTPTransferResult
	Err       error
	Transport routingTransport
	Message   *m3ua.DataMessage
}

type routingResolvedPath struct {
	Target  m3ua.MTPTransferPath
	Binding routingBinding
}

type routingPathMap struct {
	paths [routingRouteCount]routingResolvedPath
	ready bool
}

func (paths *routingPathMap) path(route uint16) (routingResolvedPath, error) {
	if paths == nil || !paths.ready || route >= routingRouteCount {
		return routingResolvedPath{}, errors.New("routing path is not frozen")
	}
	return paths.paths[route], nil
}

func freezeRoutingPaths(topology routingTopology, bindings []routingBinding, observations []routingPreflight, cohort string, seed uint64) (routingPathMap, error) {
	if topology.ASP == nil || topology.ASP.Routing == nil || len(topology.ASP.Routing.Paths) != 2 || len(topology.ASP.Routing.Paths[0].ApplicationServers) != 2 {
		return routingPathMap{}, errors.New("routing topology is incomplete")
	}
	preferred := topology.ASP.Routing.Paths[0].ApplicationServers[0]
	expected, err := newRoutingTopology(preferred)
	if err != nil || !reflect.DeepEqual(topology, expected) {
		return routingPathMap{}, errors.New("routing topology differs from the frozen workload")
	}
	if len(bindings) != 8 || len(observations) != routingRouteCount || cohort == "" {
		return routingPathMap{}, errors.New("routing preflight inventory is incomplete")
	}
	knownPeers := make(map[m3ua.SGPIdentity]int)
	for index, peer := range topology.Peers {
		knownPeers[peer.Identity] = index
	}
	senders := make(map[m3ua.AssociationID]routingBinding)
	transports := make(map[routingTransport]bool)
	peerCounts := make(map[m3ua.SGPIdentity]int)
	for _, binding := range bindings {
		_, known := knownPeers[binding.Peer.SGP]
		if !known || binding.SenderAssociation == 0 || binding.Peer.Association == 0 || binding.MaxMessageStreamID == 0 ||
			senders[binding.SenderAssociation].SenderAssociation != 0 || transports[binding.Peer] {
			return routingPathMap{}, errors.New("routing transport binding is invalid or duplicated")
		}
		senders[binding.SenderAssociation] = binding
		transports[binding.Peer] = true
		peerCounts[binding.Peer.SGP]++
	}
	for _, peer := range topology.Peers {
		if peerCounts[peer.Identity] != peer.Associations {
			return routingPathMap{}, errors.New("routing SGP association inventory differs")
		}
	}
	var paths routingPathMap
	var seen [routingRouteCount]bool
	used := make(map[m3ua.AssociationID]bool)
	epochs := make(map[m3ua.SGASKey]uint64)
	for _, observation := range observations {
		if observation.Route >= routingRouteCount || seen[observation.Route] || observation.Err != nil || len(observation.Result.SuccessfulPaths) != 1 || observation.Result.UserDataOctets != 128 {
			return routingPathMap{}, errors.New("routing preflight route or send outcome is invalid")
		}
		target := observation.Result.SuccessfulPaths[0]
		binding, known := senders[target.Association]
		if !known || target.SGP != binding.Peer.SGP || target.ApplicationServer != preferred || target.Epoch == 0 {
			return routingPathMap{}, errors.New("routing preflight target is not an authorized preferred AS")
		}
		peerIndex := knownPeers[binding.Peer.SGP]
		scopeIndex := 0
		if preferred == "secondary" {
			scopeIndex = 1
		}
		if target.AS != topology.Peers[peerIndex].ApplicationServers[scopeIndex].ASKey || target.Path != topology.ASP.Routing.Paths[peerIndex/2].ID {
			return routingPathMap{}, errors.New("routing preflight path or AS scope differs")
		}
		partition := m3ua.SGASKey{SignallingGateway: target.SGP.SignallingGateway, ApplicationServer: target.ApplicationServer}
		if previous := epochs[partition]; previous != 0 && previous != target.Epoch {
			return routingPathMap{}, errors.New("routing knowledge epoch changed during preflight")
		}
		epochs[partition] = target.Epoch
		path := routingResolvedPath{Target: target, Binding: binding}
		identity, err := validateRoutingArrival(observation.Message, observation.Transport, cohort, seed, workload128, path)
		if err != nil {
			return routingPathMap{}, fmt.Errorf("preflight route %d: %w", observation.Route, err)
		}
		if identity.Route != observation.Route || identity.Sequence != 0 {
			return routingPathMap{}, errors.New("routing preflight identity differs from its scheduled route")
		}
		paths.paths[observation.Route] = path
		seen[observation.Route] = true
		used[target.Association] = true
	}
	if len(used) != len(bindings) {
		return routingPathMap{}, errors.New("routing preflight did not exercise all eight associations")
	}
	paths.ready = true
	return paths, nil
}

func validateRouteMessage(message *m3ua.DataMessage, transport routingTransport, cohort string, seed uint64, workload workload, paths *routingPathMap) (routingIdentity, error) {
	if message == nil || message.ProtocolData == nil {
		return routingIdentity{}, errors.New("routing DATA is missing")
	}
	identity, err := parseRoutePayload(message.ProtocolData.Data)
	if err != nil {
		return routingIdentity{}, err
	}
	path, err := paths.path(identity.Route)
	if err != nil {
		return routingIdentity{}, err
	}
	return validateRoutingArrival(message, transport, cohort, seed, workload, path)
}

func validateRoutingArrival(message *m3ua.DataMessage, transport routingTransport, cohort string, seed uint64, workload workload, path routingResolvedPath) (routingIdentity, error) {
	if message == nil || message.ProtocolData == nil {
		return routingIdentity{}, errors.New("routing DATA is missing")
	}
	identity, err := parseRoutePayload(message.ProtocolData.Data)
	if err != nil {
		return routingIdentity{}, err
	}
	index, err := routingGlobalIndex(identity)
	if err != nil || identity.CohortHash != cohortHash(cohort) || identity.Seed != seed || workload.size(index) != len(message.ProtocolData.Data) {
		return routingIdentity{}, errors.New("routing cohort, seed or workload differs")
	}
	if transport != path.Binding.Peer || message.Association != transport.Association || message.Epoch != path.Binding.PeerEpoch {
		return routingIdentity{}, errors.New("routing reader transport or epoch differs")
	}
	tuple, err := routeProtocolData(identity.Route, nil)
	if err != nil {
		return routingIdentity{}, err
	}
	data := message.ProtocolData
	if data.OriginatingPointCode != tuple.OriginatingPointCode || data.DestinationPointCode != tuple.DestinationPointCode ||
		data.ServiceIndicator != tuple.ServiceIndicator || data.NetworkIndicator != tuple.NetworkIndicator ||
		data.MessagePriority != tuple.MessagePriority || data.SignallingLinkSelection != tuple.SignallingLinkSelection {
		return routingIdentity{}, errors.New("routing DATA label differs")
	}
	if !message.Scope.NetworkAppearanceSet || message.Scope.NetworkAppearance != path.Target.AS.NetworkAppearance ||
		!message.Scope.RoutingContextSet || len(message.Scope.RoutingContexts) != 1 || message.Scope.RoutingContexts[0] != path.Target.AS.RoutingContext || message.AS != path.Target.AS {
		return routingIdentity{}, errors.New("routing DATA scope differs")
	}
	if path.Binding.MaxMessageStreamID == 0 || message.Stream != uint16(tuple.SignallingLinkSelection)%path.Binding.MaxMessageStreamID+1 || message.CorrelationIDSet || message.CorrelationID != 0 {
		return routingIdentity{}, errors.New("routing stream or correlation metadata differs")
	}
	identity.Cohort = cohort
	expected, err := buildRoutePayload(identity, len(data.Data))
	if err != nil || !bytes.Equal(expected, data.Data) {
		return routingIdentity{}, errors.New("routing deterministic payload differs")
	}
	return identity, nil
}
