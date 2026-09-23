package main

import (
	"errors"
	"fmt"
	"reflect"
	"sort"

	"github.com/gomaja/go-m3ua"
)

const (
	routingSSNMPublicationCount       = 8
	routingSSNMReceiptsPerPublication = 2
	routingSSNMReceiptCount           = routingSSNMPublicationCount * routingSSNMReceiptsPerPublication
	routingSSNMPartitionCount         = 4
)

type routingSSNMPublication struct {
	Ordinal                    uint8
	SGP                        m3ua.SGPIdentity
	ApplicationServer          m3ua.RemoteASID
	AS                         m3ua.ASKey
	ExpectedSenderAssociations [routingSSNMReceiptsPerPublication]m3ua.AssociationID
}

func (publication routingSSNMPublication) partition() m3ua.SSNMPartition {
	return m3ua.SSNMPartition{
		Kind:              m3ua.SSNMCanonicalPartition,
		SignallingGateway: publication.SGP.SignallingGateway,
		ApplicationServer: publication.ApplicationServer,
	}
}

func (publication routingSSNMPublication) scope() m3ua.WireScope {
	return m3ua.WireScope{
		NetworkAppearance:    publication.AS.NetworkAppearance,
		NetworkAppearanceSet: publication.AS.NetworkAppearanceSet,
		RoutingContexts:      []uint32{publication.AS.RoutingContext},
		RoutingContextSet:    publication.AS.RoutingContextSet,
	}
}

func (publication routingSSNMPublication) request() m3ua.DestinationAvailabilityRequest {
	destinations := make([]m3ua.PointCodeRange, routingRouteCount)
	for route := range routingRouteCount {
		destinations[route] = m3ua.PointCodeRange{PointCode: 0x220000 + uint32(route)}
	}
	return m3ua.DestinationAvailabilityRequest{
		Scope:        publication.scope(),
		Destinations: destinations,
		Availability: m3ua.DestinationAvailable,
	}
}

type routingSSNMAssociation struct {
	Association   m3ua.AssociationID
	SGP           m3ua.SGPIdentity
	ASPIdentifier uint32
	SenderEpoch   uint64
}

type routingSSNMPartitionExpectation struct {
	Partition    m3ua.SSNMPartition
	Associations [4]m3ua.AssociationID
}

type routingSSNMPreparationPlan struct {
	publications [routingSSNMPublicationCount]routingSSNMPublication
	associations [8]routingSSNMAssociation
	partitions   [routingSSNMPartitionCount]routingSSNMPartitionExpectation
	destinations [routingRouteCount]m3ua.PointCodeRange
	ready        bool
}

func newRoutingSSNMPreparationPlan(topology routingTopology, pairs []routingAssociationPair) (routingSSNMPreparationPlan, error) {
	if topology.ASP == nil || topology.ASP.Routing == nil || len(topology.ASP.Routing.Paths) != 2 ||
		len(topology.ASP.Routing.Paths[0].ApplicationServers) != 2 || len(topology.Peers) != 4 || len(pairs) != 8 {
		return routingSSNMPreparationPlan{}, errors.New("routing SSNM topology or association inventory is incomplete")
	}
	preferred := topology.ASP.Routing.Paths[0].ApplicationServers[0]
	expectedTopology, err := newRoutingTopology(preferred)
	if err != nil || !reflect.DeepEqual(topology, expectedTopology) {
		return routingSSNMPreparationPlan{}, errors.New("routing SSNM topology differs from the frozen workload")
	}
	knownPeers := make(map[m3ua.SGPIdentity]routingPeer, len(topology.Peers))
	for _, peer := range topology.Peers {
		knownPeers[peer.Identity] = peer
	}
	byPeer := make(map[m3ua.SGPIdentity][]routingSSNMAssociation, len(topology.Peers))
	seenAssociations := make(map[m3ua.AssociationID]bool, len(pairs))
	seenIdentifiers := make(map[uint32]bool, len(pairs))
	all := make([]routingSSNMAssociation, 0, len(pairs))
	for _, pair := range pairs {
		peer, known := knownPeers[pair.Binding.Peer.SGP]
		association := routingSSNMAssociation{
			Association:   pair.Binding.SenderAssociation,
			SGP:           pair.Binding.Peer.SGP,
			ASPIdentifier: pair.ASPIdentifier,
			SenderEpoch:   pair.SenderEpoch,
		}
		if !known || peer.Associations != routingSSNMReceiptsPerPublication || association.Association == 0 ||
			pair.Binding.Peer.Association == 0 || association.ASPIdentifier == 0 || association.SenderEpoch == 0 ||
			seenAssociations[association.Association] || seenIdentifiers[association.ASPIdentifier] {
			return routingSSNMPreparationPlan{}, errors.New("routing SSNM association inventory is invalid or duplicated")
		}
		seenAssociations[association.Association] = true
		seenIdentifiers[association.ASPIdentifier] = true
		byPeer[association.SGP] = append(byPeer[association.SGP], association)
		all = append(all, association)
	}
	for peer := range knownPeers {
		if len(byPeer[peer]) != routingSSNMReceiptsPerPublication {
			return routingSSNMPreparationPlan{}, errors.New("routing SSNM requires two sender associations at every SGP")
		}
		sort.Slice(byPeer[peer], func(first, second int) bool {
			return byPeer[peer][first].Association < byPeer[peer][second].Association
		})
	}
	sort.Slice(all, func(first, second int) bool { return all[first].Association < all[second].Association })

	var plan routingSSNMPreparationPlan
	copy(plan.associations[:], all)
	for route := range routingRouteCount {
		plan.destinations[route] = m3ua.PointCodeRange{PointCode: 0x220000 + uint32(route)}
	}
	servers := [...]m3ua.RemoteASID{"primary", "secondary"}
	publicationIndex := 0
	for _, peer := range topology.Peers {
		if len(peer.ApplicationServers) != len(servers) {
			return routingSSNMPreparationPlan{}, errors.New("routing SSNM Application Server inventory is incomplete")
		}
		for serverIndex, server := range servers {
			key := peer.ApplicationServers[serverIndex].ASKey
			if !key.NetworkAppearanceSet || key.NetworkAppearance != 7 || !key.RoutingContextSet {
				return routingSSNMPreparationPlan{}, errors.New("routing SSNM Application Server scope is invalid")
			}
			publication := routingSSNMPublication{
				Ordinal:           uint8(publicationIndex),
				SGP:               peer.Identity,
				ApplicationServer: server,
				AS:                key,
			}
			for associationIndex, association := range byPeer[peer.Identity] {
				publication.ExpectedSenderAssociations[associationIndex] = association.Association
			}
			plan.publications[publicationIndex] = publication
			publicationIndex++
		}
	}
	partitionIndex := 0
	for _, gateway := range []m3ua.SignallingGatewayID{"sg-a", "sg-b"} {
		for _, server := range servers {
			expectation := routingSSNMPartitionExpectation{Partition: m3ua.SSNMPartition{
				Kind:              m3ua.SSNMCanonicalPartition,
				SignallingGateway: gateway,
				ApplicationServer: server,
			}}
			associations := make([]m3ua.AssociationID, 0, len(expectation.Associations))
			for _, association := range all {
				if association.SGP.SignallingGateway == gateway {
					associations = append(associations, association.Association)
				}
			}
			if len(associations) != len(expectation.Associations) {
				return routingSSNMPreparationPlan{}, errors.New("routing SSNM canonical partition binding inventory is incomplete")
			}
			sort.Slice(associations, func(first, second int) bool { return associations[first] < associations[second] })
			copy(expectation.Associations[:], associations)
			plan.partitions[partitionIndex] = expectation
			partitionIndex++
		}
	}
	plan.ready = true
	return plan, nil
}

func (plan routingSSNMPreparationPlan) publication(ordinal uint8) (routingSSNMPublication, error) {
	if !plan.ready || ordinal >= routingSSNMPublicationCount {
		return routingSSNMPublication{}, errors.New("routing SSNM publication ordinal is invalid")
	}
	return plan.publications[ordinal], nil
}

func (plan routingSSNMPreparationPlan) publicationFor(sgp m3ua.SGPIdentity, server m3ua.RemoteASID) (routingSSNMPublication, bool) {
	if !plan.ready {
		return routingSSNMPublication{}, false
	}
	for _, publication := range plan.publications {
		if publication.SGP == sgp && publication.ApplicationServer == server {
			return publication, true
		}
	}
	return routingSSNMPublication{}, false
}

func (plan routingSSNMPreparationPlan) partitionExpectation(partition m3ua.SSNMPartition) (routingSSNMPartitionExpectation, bool) {
	for _, expected := range plan.partitions {
		if expected.Partition == partition {
			return expected, true
		}
	}
	return routingSSNMPartitionExpectation{}, false
}

func (plan routingSSNMPreparationPlan) associationExpectation(id m3ua.AssociationID) (routingSSNMAssociation, bool) {
	for _, association := range plan.associations {
		if association.Association == id {
			return association, true
		}
	}
	return routingSSNMAssociation{}, false
}

type routingSSNMReceipt struct {
	Ordinal     uint8
	Association m3ua.AssociationID
	Partition   m3ua.SSNMPartition
	Epoch       uint64
	Revision    uint64
	Scope       m3ua.WireScope
}

type routingSSNMPartitionEvidence struct {
	Partition m3ua.SSNMPartition
	Epoch     uint64
}

type routingSSNMPreparationEvidence struct {
	InitialRevision uint64
	FinalRevision   uint64
	Partitions      [routingSSNMPartitionCount]routingSSNMPartitionEvidence
	Receipts        [routingSSNMReceiptCount]routingSSNMReceipt
	ReceiptCount    int
}

type routingSSNMOracle struct {
	plan            routingSSNMPreparationPlan
	initialRevision uint64
	nextRevision    uint64
	partitions      [routingSSNMPartitionCount]routingSSNMPartitionEvidence
	receipts        [routingSSNMReceiptCount]routingSSNMReceipt
	receiptCount    int
	seen            [routingSSNMPublicationCount][routingSSNMReceiptsPerPublication]bool
	currentOrdinal  uint8
	terminal        error
	finished        bool
}

func (plan routingSSNMPreparationPlan) begin(statuses []m3ua.ASPStatus, initial m3ua.SSNMSnapshot) (*routingSSNMOracle, error) {
	if !plan.ready {
		return nil, errors.New("routing SSNM preparation plan is not ready")
	}
	if err := plan.validateStatuses(statuses); err != nil {
		return nil, err
	}
	if initial.Revision == 0 || initial.Revision > ^uint64(0)-routingSSNMReceiptCount {
		return nil, errors.New("routing SSNM initial revision is invalid")
	}
	if initial.RecordsRefused != 0 || initial.ReportsRefused != 0 || initial.PartitionsInvalidated != 0 || initial.LastResourceLoss != "" {
		return nil, errors.New("routing SSNM initial snapshot contains resource loss")
	}
	if len(initial.Partitions) != routingSSNMPartitionCount {
		return nil, errors.New("routing SSNM initial snapshot has an unexpected partition inventory")
	}
	oracle := &routingSSNMOracle{
		plan:            plan,
		initialRevision: initial.Revision,
		nextRevision:    initial.Revision + 1,
	}
	seen := make(map[m3ua.SSNMPartition]bool, routingSSNMPartitionCount)
	for _, knowledge := range initial.Partitions {
		expected, known := plan.partitionExpectation(knowledge.Partition)
		if !known || seen[knowledge.Partition] || knowledge.Epoch == 0 || !knowledge.TrafficAuthorized || len(knowledge.Destinations) != 0 ||
			!sameRoutingSSNMBindings(knowledge.Bindings, expected.Associations) {
			return nil, errors.New("routing SSNM initial partition is invalid")
		}
		seen[knowledge.Partition] = true
		for index, partition := range plan.partitions {
			if partition.Partition == knowledge.Partition {
				oracle.partitions[index] = routingSSNMPartitionEvidence{Partition: knowledge.Partition, Epoch: knowledge.Epoch}
				break
			}
		}
	}
	return oracle, nil
}

func (plan routingSSNMPreparationPlan) validateStatuses(statuses []m3ua.ASPStatus) error {
	type statusExpectation struct {
		Association m3ua.AssociationID
		AS          m3ua.ASKey
	}
	expected := make(map[statusExpectation]uint32, 16)
	for _, publication := range plan.publications {
		for _, associationID := range publication.ExpectedSenderAssociations {
			association, known := plan.associationExpectation(associationID)
			if !known {
				return errors.New("routing SSNM status association is unknown")
			}
			expected[statusExpectation{Association: associationID, AS: publication.AS}] = association.ASPIdentifier
		}
	}
	if len(expected) != 16 || len(statuses) != len(expected) {
		return errors.New("routing SSNM ASP status inventory is incomplete")
	}
	seen := make(map[statusExpectation]bool, len(expected))
	for _, status := range statuses {
		key := statusExpectation{Association: status.Key.Association, AS: status.Key.AS}
		identifier, known := expected[key]
		if !known || seen[key] || !status.LocalStateSet || status.LocalState != m3ua.StateASPActive ||
			!status.LocalASPIdentifierSet || status.LocalASPIdentifier != identifier {
			return errors.New("routing SSNM ASP status is missing, inactive or contradictory")
		}
		seen[key] = true
	}
	return nil
}

func sameRoutingSSNMBindings(actual []m3ua.SSNMBinding, expected [4]m3ua.AssociationID) bool {
	if len(actual) != len(expected) {
		return false
	}
	seen := make(map[m3ua.AssociationID]bool, len(actual))
	for _, binding := range actual {
		if binding.Association == 0 || binding.Pending || seen[binding.Association] {
			return false
		}
		seen[binding.Association] = true
	}
	for _, association := range expected {
		if !seen[association] {
			return false
		}
	}
	return true
}

func (oracle *routingSSNMOracle) partitionEpoch(partition m3ua.SSNMPartition) uint64 {
	if oracle == nil {
		return 0
	}
	for _, expected := range oracle.partitions {
		if expected.Partition == partition {
			return expected.Epoch
		}
	}
	return 0
}

func (oracle *routingSSNMOracle) reject(err error) error {
	if oracle == nil {
		return err
	}
	if oracle.terminal == nil {
		oracle.terminal = err
	}
	return oracle.terminal
}

func (oracle *routingSSNMOracle) observe(ordinal uint8, event m3ua.SSNMEvent) error {
	if oracle == nil {
		return errors.New("routing SSNM oracle is nil")
	}
	if oracle.terminal != nil {
		return oracle.terminal
	}
	if oracle.finished || oracle.currentOrdinal >= routingSSNMPublicationCount || ordinal != oracle.currentOrdinal {
		return oracle.reject(errors.New("routing SSNM report belongs to an inactive publication"))
	}
	publication := oracle.plan.publications[ordinal]
	if event.Kind != m3ua.SSNMReportEvent || event.ContinuityLost || !event.ReportSet || event.Reason != "" ||
		event.Binding != (m3ua.SSNMBinding{}) {
		return oracle.reject(fmt.Errorf("routing SSNM publication received terminal event %s", event.Kind))
	}
	expectedPartition := publication.partition()
	expectedEpoch := oracle.partitionEpoch(expectedPartition)
	if event.Revision != oracle.nextRevision || event.Report.Revision != event.Revision ||
		event.Partition != expectedPartition || event.Report.Partition != expectedPartition ||
		event.Epoch != expectedEpoch || event.Report.Epoch != expectedEpoch {
		return oracle.reject(errors.New("routing SSNM report revision, partition or epoch differs"))
	}
	report := event.Report
	if report.Kind != m3ua.SSNMDestinationAvailableReport || report.Source != m3ua.SSNMPeerReport ||
		!sameRoutingSSNMScope(report.Scope, publication.scope()) || report.CongestionLevelSet || report.CongestionLevel != 0 ||
		report.UserCauseSet || report.UserCause != 0 || report.ConcernedDestinationSet || report.ConcernedDestination != 0 || report.PeerReported {
		return oracle.reject(errors.New("routing SSNM report type, source, scope or payload metadata differs"))
	}
	associationIndex := -1
	for index, association := range publication.ExpectedSenderAssociations {
		if report.Association == association {
			associationIndex = index
			break
		}
	}
	if associationIndex < 0 || oracle.seen[ordinal][associationIndex] {
		return oracle.reject(errors.New("routing SSNM report association is unexpected or duplicated"))
	}
	if !sameRoutingSSNMDestinations(report.Destinations, oracle.plan.destinations) ||
		!sameRoutingSSNMEventUpdates(event.Updated, oracle.plan.destinations, report) {
		return oracle.reject(errors.New("routing SSNM report destinations or resulting states differ"))
	}
	if oracle.receiptCount >= len(oracle.receipts) {
		return oracle.reject(errors.New("routing SSNM receipt inventory overflowed"))
	}
	receipt := routingSSNMReceipt{
		Ordinal: ordinal, Association: report.Association, Partition: expectedPartition,
		Epoch: expectedEpoch, Revision: event.Revision, Scope: copyRoutingSSNMScope(report.Scope),
	}
	oracle.receipts[oracle.receiptCount] = receipt
	oracle.receiptCount++
	oracle.seen[ordinal][associationIndex] = true
	oracle.nextRevision++
	return nil
}

func (oracle *routingSSNMOracle) publicationComplete(ordinal uint8) error {
	if oracle == nil {
		return errors.New("routing SSNM oracle is nil")
	}
	if oracle.terminal != nil {
		return oracle.terminal
	}
	if oracle.finished || oracle.currentOrdinal >= routingSSNMPublicationCount || ordinal != oracle.currentOrdinal ||
		!oracle.seen[ordinal][0] || !oracle.seen[ordinal][1] {
		return oracle.reject(errors.New("routing SSNM publication does not have two exact receipts"))
	}
	oracle.currentOrdinal++
	return nil
}

func (oracle *routingSSNMOracle) finish(final m3ua.SSNMSnapshot) (routingSSNMPreparationEvidence, error) {
	if oracle == nil {
		return routingSSNMPreparationEvidence{}, errors.New("routing SSNM oracle is nil")
	}
	if oracle.terminal != nil {
		return routingSSNMPreparationEvidence{}, oracle.terminal
	}
	if oracle.finished {
		return routingSSNMPreparationEvidence{}, oracle.reject(errors.New("routing SSNM oracle is already finished"))
	}
	if oracle.currentOrdinal != routingSSNMPublicationCount || oracle.receiptCount != routingSSNMReceiptCount {
		return routingSSNMPreparationEvidence{}, oracle.reject(errors.New("routing SSNM receipt inventory is incomplete"))
	}
	expectedRevision := oracle.initialRevision + routingSSNMReceiptCount
	if final.Revision != expectedRevision || final.Revision != oracle.nextRevision-1 ||
		final.RecordsRefused != 0 || final.ReportsRefused != 0 || final.PartitionsInvalidated != 0 || final.LastResourceLoss != "" ||
		len(final.Partitions) != routingSSNMPartitionCount {
		return routingSSNMPreparationEvidence{}, oracle.reject(errors.New("routing SSNM final snapshot revision or resource state differs"))
	}
	seen := make(map[m3ua.SSNMPartition]bool, routingSSNMPartitionCount)
	for _, knowledge := range final.Partitions {
		expected, known := oracle.plan.partitionExpectation(knowledge.Partition)
		latest, hasReceipt := oracle.latestReceipt(knowledge.Partition)
		if !known || !hasReceipt || seen[knowledge.Partition] || knowledge.Epoch != oracle.partitionEpoch(knowledge.Partition) ||
			!knowledge.TrafficAuthorized || !sameRoutingSSNMBindings(knowledge.Bindings, expected.Associations) ||
			!sameRoutingSSNMFinalDestinations(knowledge.Destinations, oracle.plan.destinations, latest) {
			return routingSSNMPreparationEvidence{}, oracle.reject(errors.New("routing SSNM final partition knowledge differs"))
		}
		seen[knowledge.Partition] = true
	}
	oracle.finished = true
	return routingSSNMPreparationEvidence{
		InitialRevision: oracle.initialRevision,
		FinalRevision:   final.Revision,
		Partitions:      oracle.partitions,
		Receipts:        oracle.receipts,
		ReceiptCount:    oracle.receiptCount,
	}, nil
}

func (oracle *routingSSNMOracle) latestReceipt(partition m3ua.SSNMPartition) (routingSSNMReceipt, bool) {
	var latest routingSSNMReceipt
	for index := 0; index < oracle.receiptCount; index++ {
		receipt := oracle.receipts[index]
		if receipt.Partition == partition && receipt.Revision > latest.Revision {
			latest = receipt
		}
	}
	return latest, latest.Revision != 0
}

func sameRoutingSSNMScope(actual, expected m3ua.WireScope) bool {
	if actual.NetworkAppearanceSet != expected.NetworkAppearanceSet || actual.NetworkAppearance != expected.NetworkAppearance ||
		actual.RoutingContextSet != expected.RoutingContextSet || len(actual.RoutingContexts) != len(expected.RoutingContexts) {
		return false
	}
	for index := range actual.RoutingContexts {
		if actual.RoutingContexts[index] != expected.RoutingContexts[index] {
			return false
		}
	}
	return true
}

func copyRoutingSSNMScope(scope m3ua.WireScope) m3ua.WireScope {
	scope.RoutingContexts = append([]uint32(nil), scope.RoutingContexts...)
	return scope
}

func sameRoutingSSNMDestinations(actual []m3ua.PointCodeRange, expected [routingRouteCount]m3ua.PointCodeRange) bool {
	if len(actual) != len(expected) {
		return false
	}
	for index := range expected {
		if actual[index] != expected[index] {
			return false
		}
	}
	return true
}

// sameRoutingSSNMEventUpdates checks a report event's delta. Every report
// names every planned destination, so the event updates each of them, once and
// in point-code then mask order, to the availability the report carried.
func sameRoutingSSNMEventUpdates(actual []m3ua.SSNMDestinationKnowledge, expected [routingRouteCount]m3ua.PointCodeRange, report m3ua.SSNMReport) bool {
	if len(actual) != len(expected) {
		return false
	}
	for index, state := range actual {
		availability := state.Availability
		if state.Destination != expected[index] || !state.AvailabilitySet || state.CongestionSet ||
			availability.State != m3ua.DestinationAvailable || availability.Kind != report.Kind || availability.Source != report.Source ||
			!sameRoutingSSNMScope(availability.Scope, report.Scope) || availability.Association != report.Association ||
			availability.Epoch != report.Epoch || availability.Revision != report.Revision {
			return false
		}
	}
	return true
}

func sameRoutingSSNMFinalDestinations(actual []m3ua.SSNMDestinationKnowledge, expected [routingRouteCount]m3ua.PointCodeRange, latest routingSSNMReceipt) bool {
	if len(actual) != len(expected) {
		return false
	}
	for index, state := range actual {
		availability := state.Availability
		if state.Destination != expected[index] || !state.AvailabilitySet || state.CongestionSet ||
			availability.State != m3ua.DestinationAvailable || availability.Kind != m3ua.SSNMDestinationAvailableReport ||
			availability.Source != m3ua.SSNMPeerReport || !sameRoutingSSNMScope(availability.Scope, latest.Scope) ||
			availability.Association != latest.Association || availability.Epoch != latest.Epoch || availability.Revision != latest.Revision {
			return false
		}
	}
	return true
}
