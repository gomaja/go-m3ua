package main

import (
	"reflect"
	"testing"

	"github.com/gomaja/go-m3ua"
)

type routingSSNMTestFixture struct {
	plan     routingSSNMPreparationPlan
	statuses []m3ua.ASPStatus
	initial  m3ua.SSNMSnapshot
}

func newRoutingSSNMTestFixture(t *testing.T) routingSSNMTestFixture {
	t.Helper()
	topology, err := newRoutingTopology("primary")
	if err != nil {
		t.Fatalf("newRoutingTopology: %v", err)
	}
	pairs := make([]routingAssociationPair, 0, 8)
	for peerIndex, peer := range topology.Peers {
		for associationIndex := range 2 {
			identifier := uint32(1000 + 2*peerIndex + associationIndex)
			pairs = append(pairs, routingAssociationPair{
				Binding: routingBinding{
					SenderAssociation: m3ua.AssociationID(1 + 2*peerIndex + associationIndex),
					Peer: routingTransport{
						SGP:         peer.Identity,
						Association: m3ua.AssociationID(101 + 2*peerIndex + associationIndex),
					},
				},
				ASPIdentifier: identifier,
				SenderEpoch:   uint64(200 + 2*peerIndex + associationIndex),
			})
		}
	}
	plan, err := newRoutingSSNMPreparationPlan(topology, pairs)
	if err != nil {
		t.Fatalf("newRoutingSSNMPreparationPlan: %v", err)
	}
	statuses := make([]m3ua.ASPStatus, 0, 16)
	for _, association := range plan.associations {
		for _, server := range []m3ua.RemoteASID{"primary", "secondary"} {
			publication, ok := plan.publicationFor(association.SGP, server)
			if !ok {
				t.Fatalf("publicationFor(%+v, %q) is missing", association.SGP, server)
			}
			statuses = append(statuses, m3ua.ASPStatus{
				Key:                   m3ua.ASPStatusKey{Association: association.Association, AS: publication.AS},
				LocalState:            m3ua.StateASPActive,
				LocalStateSet:         true,
				LocalASPIdentifier:    association.ASPIdentifier,
				LocalASPIdentifierSet: true,
			})
		}
	}
	initial := m3ua.SSNMSnapshot{Revision: 40}
	for index, expected := range plan.partitions {
		bindings := make([]m3ua.SSNMBinding, len(expected.Associations))
		for bindingIndex, association := range expected.Associations {
			bindings[bindingIndex] = m3ua.SSNMBinding{Association: association}
		}
		initial.Partitions = append(initial.Partitions, m3ua.SSNMPartitionKnowledge{
			Partition:         expected.Partition,
			Epoch:             uint64(500 + index),
			Bindings:          bindings,
			TrafficAuthorized: true,
		})
	}
	return routingSSNMTestFixture{plan: plan, statuses: statuses, initial: initial}
}

func routingSSNMTestEvent(publication routingSSNMPublication, association m3ua.AssociationID, epoch, revision uint64) m3ua.SSNMEvent {
	request := publication.request()
	report := m3ua.SSNMReport{
		Kind:         m3ua.SSNMDestinationAvailableReport,
		Source:       m3ua.SSNMPeerReport,
		Scope:        request.Scope,
		Partition:    publication.partition(),
		Association:  association,
		Epoch:        epoch,
		Revision:     revision,
		Destinations: append([]m3ua.PointCodeRange(nil), request.Destinations...),
	}
	states := make([]m3ua.SSNMDestinationKnowledge, len(request.Destinations))
	for index, destination := range request.Destinations {
		states[index] = m3ua.SSNMDestinationKnowledge{
			Destination: destination,
			Availability: m3ua.SSNMAvailability{
				State:       m3ua.DestinationAvailable,
				Kind:        report.Kind,
				Source:      report.Source,
				Scope:       request.Scope,
				Association: association,
				Epoch:       epoch,
				Revision:    revision,
			},
			AvailabilitySet: true,
		}
	}
	return m3ua.SSNMEvent{
		Kind:      m3ua.SSNMReportEvent,
		Revision:  revision,
		Partition: report.Partition,
		Epoch:     epoch,
		Report:    report,
		ReportSet: true,
		Updated:   states,
	}
}

func completeRoutingSSNMTestOracle(t *testing.T, fixture routingSSNMTestFixture) (*routingSSNMOracle, m3ua.SSNMSnapshot) {
	t.Helper()
	oracle, err := fixture.plan.begin(fixture.statuses, fixture.initial)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	for ordinal := uint8(0); ordinal < routingSSNMPublicationCount; ordinal++ {
		publication, err := fixture.plan.publication(ordinal)
		if err != nil {
			t.Fatalf("publication(%d): %v", ordinal, err)
		}
		epoch := oracle.partitionEpoch(publication.partition())
		for index := len(publication.ExpectedSenderAssociations) - 1; index >= 0; index-- {
			event := routingSSNMTestEvent(publication, publication.ExpectedSenderAssociations[index], epoch, oracle.nextRevision)
			if err := oracle.observe(ordinal, event); err != nil {
				t.Fatalf("observe(%d, %d): %v", ordinal, index, err)
			}
		}
		if err := oracle.publicationComplete(ordinal); err != nil {
			t.Fatalf("publicationComplete(%d): %v", ordinal, err)
		}
	}
	final := routingSSNMFinalSnapshot(t, oracle)
	return oracle, final
}

func routingSSNMFinalSnapshot(t *testing.T, oracle *routingSSNMOracle) m3ua.SSNMSnapshot {
	t.Helper()
	final := m3ua.SSNMSnapshot{Revision: oracle.nextRevision - 1}
	for _, expected := range oracle.plan.partitions {
		epoch := oracle.partitionEpoch(expected.Partition)
		latest := routingSSNMReceipt{}
		for index := 0; index < oracle.receiptCount; index++ {
			receipt := oracle.receipts[index]
			if receipt.Partition == expected.Partition && receipt.Revision > latest.Revision {
				latest = receipt
			}
		}
		bindings := make([]m3ua.SSNMBinding, len(expected.Associations))
		for index, association := range expected.Associations {
			bindings[index] = m3ua.SSNMBinding{Association: association}
		}
		destinations := make([]m3ua.SSNMDestinationKnowledge, routingRouteCount)
		for index := range routingRouteCount {
			destinations[index] = m3ua.SSNMDestinationKnowledge{
				Destination: oracle.plan.destinations[index],
				Availability: m3ua.SSNMAvailability{
					State:       m3ua.DestinationAvailable,
					Kind:        m3ua.SSNMDestinationAvailableReport,
					Source:      m3ua.SSNMPeerReport,
					Scope:       latest.Scope,
					Association: latest.Association,
					Epoch:       epoch,
					Revision:    latest.Revision,
				},
				AvailabilitySet: true,
			}
		}
		final.Partitions = append(final.Partitions, m3ua.SSNMPartitionKnowledge{
			Partition: expected.Partition, Epoch: epoch, Bindings: bindings,
			TrafficAuthorized: true, Destinations: destinations,
		})
	}
	return final
}

func TestRoutingSSNMPreparationPlanBuildsEightExactPublications(t *testing.T) {
	fixture := newRoutingSSNMTestFixture(t)
	wantContexts := []uint32{100, 101, 110, 111, 120, 121, 130, 131}
	for ordinal, wantContext := range wantContexts {
		publication, err := fixture.plan.publication(uint8(ordinal))
		if err != nil {
			t.Fatalf("publication(%d): %v", ordinal, err)
		}
		request := publication.request()
		if publication.Ordinal != uint8(ordinal) || len(publication.ExpectedSenderAssociations) != 2 ||
			!request.Scope.NetworkAppearanceSet || request.Scope.NetworkAppearance != 7 ||
			!request.Scope.RoutingContextSet || !reflect.DeepEqual(request.Scope.RoutingContexts, []uint32{wantContext}) ||
			request.Availability != m3ua.DestinationAvailable || request.Info != "" || len(request.Destinations) != routingRouteCount {
			t.Fatalf("publication %d differs: descriptor=%+v request=%+v", ordinal, publication, request)
		}
		for route, destination := range request.Destinations {
			if destination != (m3ua.PointCodeRange{PointCode: 0x220000 + uint32(route)}) {
				t.Fatalf("publication %d destination %d = %+v", ordinal, route, destination)
			}
		}
		request.Scope.RoutingContexts[0] = 999
		request.Destinations[0].PointCode = 1
		fresh := publication.request()
		if fresh.Scope.RoutingContexts[0] != wantContext || fresh.Destinations[0].PointCode != 0x220000 {
			t.Fatal("publication request did not return owned scope and destinations")
		}
	}
}

func TestRoutingSSNMOracleAcceptsExactReceiptsAndLatestWriterKnowledge(t *testing.T) {
	fixture := newRoutingSSNMTestFixture(t)
	oracle, final := completeRoutingSSNMTestOracle(t, fixture)
	evidence, err := oracle.finish(final)
	if err != nil {
		t.Fatalf("finish: %v", err)
	}
	if evidence.InitialRevision != fixture.initial.Revision || evidence.FinalRevision != fixture.initial.Revision+routingSSNMReceiptCount ||
		evidence.ReceiptCount != routingSSNMReceiptCount {
		t.Fatalf("evidence = %+v", evidence)
	}
	for index, partition := range evidence.Partitions {
		if partition.Epoch != uint64(500+index) {
			t.Fatalf("partition epoch %d = %d", index, partition.Epoch)
		}
	}
}

func TestRoutingSSNMOracleKeepsPartitionAndAssociationEpochsSeparate(t *testing.T) {
	fixture := newRoutingSSNMTestFixture(t)
	for _, association := range fixture.plan.associations {
		if association.SenderEpoch >= 500 && association.SenderEpoch < 504 {
			t.Fatal("fixture accidentally equates association and partition epochs")
		}
	}
	oracle, final := completeRoutingSSNMTestOracle(t, fixture)
	if _, err := oracle.finish(final); err != nil {
		t.Fatalf("finish with distinct generation domains: %v", err)
	}
}

func TestRoutingSSNMOracleRejectsIncompleteReceiptDespiteValidFinalKnowledge(t *testing.T) {
	fixture := newRoutingSSNMTestFixture(t)
	oracle, err := fixture.plan.begin(fixture.statuses, fixture.initial)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	for ordinal := uint8(0); ordinal < routingSSNMPublicationCount; ordinal++ {
		publication, err := fixture.plan.publication(ordinal)
		if err != nil {
			t.Fatalf("publication(%d): %v", ordinal, err)
		}
		count := 2
		if ordinal == routingSSNMPublicationCount-1 {
			count = 1
		}
		for index := range count {
			event := routingSSNMTestEvent(publication, publication.ExpectedSenderAssociations[index], oracle.partitionEpoch(publication.partition()), oracle.nextRevision)
			if err := oracle.observe(ordinal, event); err != nil {
				t.Fatalf("observe(%d, %d): %v", ordinal, index, err)
			}
		}
		if count == 2 {
			if err := oracle.publicationComplete(ordinal); err != nil {
				t.Fatalf("publicationComplete(%d): %v", ordinal, err)
			}
		}
	}
	final := routingSSNMFinalSnapshot(t, oracle)
	final.Revision = fixture.initial.Revision + routingSSNMReceiptCount
	if _, err := oracle.finish(final); err == nil {
		t.Fatal("final last-writer knowledge substituted for a missing association receipt")
	}
}

func TestRoutingSSNMOracleRejectsContradictoryReports(t *testing.T) {
	tests := map[string]func(*m3ua.SSNMEvent){
		"missing report":  func(event *m3ua.SSNMEvent) { event.ReportSet = false },
		"wrong kind":      func(event *m3ua.SSNMEvent) { event.Report.Kind = m3ua.SSNMDestinationUnavailableReport },
		"wrong source":    func(event *m3ua.SSNMEvent) { event.Report.Source = m3ua.SSNMLocalReport },
		"wrong context":   func(event *m3ua.SSNMEvent) { event.Report.Scope.RoutingContexts[0]++ },
		"wrong partition": func(event *m3ua.SSNMEvent) { event.Report.Partition.ApplicationServer = "other" },
		"wrong association": func(event *m3ua.SSNMEvent) {
			event.Report.Association = 999
			for index := range event.Updated {
				event.Updated[index].Availability.Association = 999
			}
		},
		"wrong epoch":  func(event *m3ua.SSNMEvent) { event.Report.Epoch++ },
		"revision gap": func(event *m3ua.SSNMEvent) { event.Revision++; event.Report.Revision++ },
		"missing destination": func(event *m3ua.SSNMEvent) {
			event.Report.Destinations = event.Report.Destinations[:routingRouteCount-1]
		},
		"duplicate destination": func(event *m3ua.SSNMEvent) { event.Report.Destinations[1] = event.Report.Destinations[0] },
		"wrong mask":            func(event *m3ua.SSNMEvent) { event.Report.Destinations[0].Mask = 1 },
		"contradictory state":   func(event *m3ua.SSNMEvent) { event.Updated[0].Availability.State = m3ua.DestinationUnavailable },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			fixture := newRoutingSSNMTestFixture(t)
			oracle, err := fixture.plan.begin(fixture.statuses, fixture.initial)
			if err != nil {
				t.Fatalf("begin: %v", err)
			}
			publication, err := fixture.plan.publication(0)
			if err != nil {
				t.Fatalf("publication: %v", err)
			}
			event := routingSSNMTestEvent(publication, publication.ExpectedSenderAssociations[0], oracle.partitionEpoch(publication.partition()), oracle.nextRevision)
			mutate(&event)
			if err := oracle.observe(0, event); err == nil {
				t.Fatal("contradictory report was accepted")
			}
			valid := routingSSNMTestEvent(publication, publication.ExpectedSenderAssociations[0], oracle.partitionEpoch(publication.partition()), oracle.nextRevision)
			if err := oracle.observe(0, valid); err == nil {
				t.Fatal("terminal oracle failure was repaired by a later report")
			}
		})
	}
}

func TestRoutingSSNMOracleRejectsDuplicateLateAndCrossOrdinalReports(t *testing.T) {
	tests := map[string]func(*testing.T, *routingSSNMOracle, routingSSNMPublication){
		"duplicate": func(t *testing.T, oracle *routingSSNMOracle, publication routingSSNMPublication) {
			event := routingSSNMTestEvent(publication, publication.ExpectedSenderAssociations[0], oracle.partitionEpoch(publication.partition()), oracle.nextRevision)
			if err := oracle.observe(0, event); err != nil {
				t.Fatalf("first receipt: %v", err)
			}
			event = routingSSNMTestEvent(publication, publication.ExpectedSenderAssociations[0], oracle.partitionEpoch(publication.partition()), oracle.nextRevision)
			if err := oracle.observe(0, event); err == nil {
				t.Fatal("duplicate receipt was accepted")
			}
		},
		"cross ordinal": func(t *testing.T, oracle *routingSSNMOracle, _ routingSSNMPublication) {
			next, err := oracle.plan.publication(1)
			if err != nil {
				t.Fatalf("publication(1): %v", err)
			}
			event := routingSSNMTestEvent(next, next.ExpectedSenderAssociations[0], oracle.partitionEpoch(next.partition()), oracle.nextRevision)
			if err := oracle.observe(1, event); err == nil {
				t.Fatal("cross-ordinal receipt was accepted")
			}
		},
		"late": func(t *testing.T, oracle *routingSSNMOracle, publication routingSSNMPublication) {
			for _, association := range publication.ExpectedSenderAssociations {
				event := routingSSNMTestEvent(publication, association, oracle.partitionEpoch(publication.partition()), oracle.nextRevision)
				if err := oracle.observe(0, event); err != nil {
					t.Fatalf("receipt: %v", err)
				}
			}
			if err := oracle.publicationComplete(0); err != nil {
				t.Fatalf("publicationComplete: %v", err)
			}
			event := routingSSNMTestEvent(publication, publication.ExpectedSenderAssociations[0], oracle.partitionEpoch(publication.partition()), oracle.nextRevision)
			if err := oracle.observe(0, event); err == nil {
				t.Fatal("late receipt was accepted")
			}
		},
	}
	for name, run := range tests {
		t.Run(name, func(t *testing.T) {
			fixture := newRoutingSSNMTestFixture(t)
			oracle, err := fixture.plan.begin(fixture.statuses, fixture.initial)
			if err != nil {
				t.Fatalf("begin: %v", err)
			}
			publication, err := fixture.plan.publication(0)
			if err != nil {
				t.Fatalf("publication(0): %v", err)
			}
			run(t, oracle, publication)
		})
	}
}

func TestRoutingSSNMOracleRejectsLifecycleResourceAndContinuityEvents(t *testing.T) {
	for _, kind := range []m3ua.SSNMEventKind{
		m3ua.SSNMBindingAdmittedEvent,
		m3ua.SSNMBindingActivatedEvent,
		m3ua.SSNMBindingRetiredEvent,
		m3ua.SSNMPartitionRetiredEvent,
		m3ua.SSNMPartitionInvalidatedEvent,
		m3ua.SSNMResourceLossEvent,
		m3ua.SSNMContinuityLostEvent,
		m3ua.SSNMEventKind(255),
	} {
		t.Run(kind.String(), func(t *testing.T) {
			fixture := newRoutingSSNMTestFixture(t)
			oracle, err := fixture.plan.begin(fixture.statuses, fixture.initial)
			if err != nil {
				t.Fatalf("begin: %v", err)
			}
			if err := oracle.observe(0, m3ua.SSNMEvent{Kind: kind, ContinuityLost: kind == m3ua.SSNMContinuityLostEvent}); err == nil {
				t.Fatalf("%v event was accepted", kind)
			}
		})
	}
}

func TestRoutingSSNMOracleRejectsInvalidPreflight(t *testing.T) {
	tests := map[string]func(*routingSSNMTestFixture){
		"missing status":         func(fixture *routingSSNMTestFixture) { fixture.statuses = fixture.statuses[:len(fixture.statuses)-1] },
		"inactive status":        func(fixture *routingSSNMTestFixture) { fixture.statuses[0].LocalState = m3ua.StateASPInactive },
		"unset status":           func(fixture *routingSSNMTestFixture) { fixture.statuses[0].LocalStateSet = false },
		"wrong identifier":       func(fixture *routingSSNMTestFixture) { fixture.statuses[0].LocalASPIdentifier++ },
		"missing partition":      func(fixture *routingSSNMTestFixture) { fixture.initial.Partitions = fixture.initial.Partitions[:3] },
		"zero partition epoch":   func(fixture *routingSSNMTestFixture) { fixture.initial.Partitions[0].Epoch = 0 },
		"pending binding":        func(fixture *routingSSNMTestFixture) { fixture.initial.Partitions[0].Bindings[0].Pending = true },
		"unauthorized partition": func(fixture *routingSSNMTestFixture) { fixture.initial.Partitions[0].TrafficAuthorized = false },
		"stale destination": func(fixture *routingSSNMTestFixture) {
			fixture.initial.Partitions[0].Destinations = []m3ua.SSNMDestinationKnowledge{{Destination: m3ua.PointCodeRange{PointCode: 1}}}
		},
		"resource history": func(fixture *routingSSNMTestFixture) { fixture.initial.ReportsRefused = 1 },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			fixture := newRoutingSSNMTestFixture(t)
			mutate(&fixture)
			if _, err := fixture.plan.begin(fixture.statuses, fixture.initial); err == nil {
				t.Fatal("invalid preflight was accepted")
			}
		})
	}
}

func TestRoutingSSNMOracleRejectsFinalSnapshotChanges(t *testing.T) {
	tests := map[string]func(*m3ua.SSNMSnapshot){
		"revision":        func(snapshot *m3ua.SSNMSnapshot) { snapshot.Revision++ },
		"partition epoch": func(snapshot *m3ua.SSNMSnapshot) { snapshot.Partitions[0].Epoch++ },
		"pending binding": func(snapshot *m3ua.SSNMSnapshot) { snapshot.Partitions[0].Bindings[0].Pending = true },
		"unavailable": func(snapshot *m3ua.SSNMSnapshot) {
			snapshot.Partitions[0].Destinations[0].Availability.State = m3ua.DestinationUnavailable
		},
		"wrong provenance": func(snapshot *m3ua.SSNMSnapshot) {
			snapshot.Partitions[0].Destinations[0].Availability.Association = 999
		},
		"missing destination": func(snapshot *m3ua.SSNMSnapshot) {
			snapshot.Partitions[0].Destinations = snapshot.Partitions[0].Destinations[:routingRouteCount-1]
		},
		"resource loss": func(snapshot *m3ua.SSNMSnapshot) { snapshot.RecordsRefused = 1; snapshot.LastResourceLoss = "loss" },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			fixture := newRoutingSSNMTestFixture(t)
			oracle, final := completeRoutingSSNMTestOracle(t, fixture)
			mutate(&final)
			if _, err := oracle.finish(final); err == nil {
				t.Fatal("contradictory final snapshot was accepted")
			}
		})
	}
}

func TestRoutingSSNMOracleRetainsOnlyCompactOwnedReceipt(t *testing.T) {
	fixture := newRoutingSSNMTestFixture(t)
	oracle, err := fixture.plan.begin(fixture.statuses, fixture.initial)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	publication, err := fixture.plan.publication(0)
	if err != nil {
		t.Fatalf("publication: %v", err)
	}
	event := routingSSNMTestEvent(publication, publication.ExpectedSenderAssociations[0], oracle.partitionEpoch(publication.partition()), oracle.nextRevision)
	if err := oracle.observe(0, event); err != nil {
		t.Fatalf("observe: %v", err)
	}
	event.Report.Scope.RoutingContexts[0] = 999
	event.Report.Destinations[0].PointCode = 1
	event.Updated[0].Availability.Scope.RoutingContexts[0] = 999
	receipt := oracle.receipts[0]
	if !reflect.DeepEqual(receipt.Scope.RoutingContexts, []uint32{100}) || receipt.Association != publication.ExpectedSenderAssociations[0] {
		t.Fatalf("receipt aliases the raw event: %+v", receipt)
	}
}
