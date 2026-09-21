package m3ua

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"
)

func ssnmOwnershipReport(count int, kind SSNMReportKind) SSNMReport {
	destinations := make([]PointCodeRange, count)
	for index := range destinations {
		mask := uint8(8)
		if index%2 != 0 {
			mask = 4
		}
		destinations[index] = PointCodeRange{PointCode: 0x200000 + uint32(index/2)*256, Mask: mask}
	}
	return SSNMReport{
		Kind: kind, Source: SSNMPeerReport,
		Partition:   SSNMPartition{Kind: SSNMStandalonePartition, Association: 1},
		Association: 1,
		Scope: WireScope{
			NetworkAppearance: 7, NetworkAppearanceSet: true,
			RoutingContexts: []uint32{1, 2}, RoutingContextSet: true,
		},
		Destinations: destinations, CongestionLevel: 2, CongestionLevelSet: true,
	}
}

func newSSNMOwnershipEndpoint(testContext *testing.T, count int, pending bool, limits *SSNMStateConfig) *Endpoint {
	testContext.Helper()
	endpoint := newSSNMStateEndpoint(testContext, nil, limits)
	partition := ssnmOwnershipReport(count, SSNMDestinationUnavailableReport).Partition
	if err := endpoint.ssnm.bind(partition, 1, pending); err != nil {
		testContext.Fatal(err)
	}
	for _, kind := range []SSNMReportKind{SSNMDestinationUnavailableReport, SSNMSignallingCongestionReport} {
		if err := endpoint.ssnm.apply(ssnmOwnershipReport(count, kind)); err != nil {
			testContext.Fatal(err)
		}
	}
	return endpoint
}

func ssnmOwnershipValue(testContext *testing.T, value any) string {
	testContext.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		testContext.Fatal(err)
	}
	return string(encoded)
}

func requireSSNMOwnershipValue(testContext *testing.T, label string, value any, expected string) {
	testContext.Helper()
	if actual := ssnmOwnershipValue(testContext, value); actual != expected {
		testContext.Fatalf("%s changed across an ownership boundary", label)
	}
}

func nextSSNMOwnershipEvent(testContext *testing.T, subscription *SSNMSubscription) SSNMEvent {
	testContext.Helper()
	event, err := drainSSNMEvent(testContext, subscription)
	if err != nil {
		testContext.Fatal(err)
	}
	return event
}

func mutateSSNMOwnershipStates(states []SSNMDestinationKnowledge) {
	for index := range states {
		state := &states[index]
		state.Destination.PointCode = 0xffffff
		state.Availability.State = DestinationRestricted
		state.Congestion.Level = 255
		for contextIndex := range state.Availability.Scope.RoutingContexts {
			state.Availability.Scope.RoutingContexts[contextIndex] = 0xfeed0001
		}
		for contextIndex := range state.Congestion.Scope.RoutingContexts {
			state.Congestion.Scope.RoutingContexts[contextIndex] = 0xfeed0002
		}
	}
}

func mutateSSNMOwnershipReport(report SSNMReport) {
	for index := range report.Scope.RoutingContexts {
		report.Scope.RoutingContexts[index] = 0xbeef0001
	}
	for index := range report.Destinations {
		report.Destinations[index].PointCode = 0xffffff
	}
}

func TestSSNMPublicationPreservesIndependentOwners(testContext *testing.T) {
	for _, count := range []int{1, 1024} {
		for _, kind := range []SSNMReportKind{SSNMDestinationAvailableReport, SSNMSignallingCongestionReport} {
			testContext.Run(fmt.Sprintf("APC%d/%s", count, kind), func(testContext *testing.T) {
				endpoint := newSSNMOwnershipEndpoint(testContext, count, false, nil)
				first := mustSubscribeSSNM(testContext, endpoint)
				second := mustSubscribeSSNM(testContext, endpoint)
				before := endpoint.SSNMKnowledge()
				report := ssnmOwnershipReport(count, kind)
				expectedReport := report
				expectedReport.Revision = before.Revision + 1
				expectedReport.Epoch = before.Partitions[0].Epoch
				wantReport := ssnmOwnershipValue(testContext, expectedReport)
				if err := endpoint.ssnm.apply(report); err != nil {
					testContext.Fatal(err)
				}
				snapshot := endpoint.SSNMKnowledge()
				wantSnapshot := ssnmOwnershipValue(testContext, snapshot)
				mutateSSNMOwnershipReport(report)
				firstEvent := nextSSNMOwnershipEvent(testContext, first)
				requireSSNMOwnershipValue(testContext, "queued report after input mutation", firstEvent.Report, wantReport)
				if firstEvent.Kind != SSNMReportEvent || !firstEvent.ReportSet || len(firstEvent.States) != count {
					testContext.Fatalf("incomplete publication: kind=%v report=%v states=%d", firstEvent.Kind, firstEvent.ReportSet, len(firstEvent.States))
				}
				wantEvent := ssnmOwnershipValue(testContext, firstEvent)
				wantStates := ssnmOwnershipValue(testContext, firstEvent.States)
				mutateSSNMOwnershipReport(firstEvent.Report)
				requireSSNMOwnershipValue(testContext, "event states after report mutation", firstEvent.States, wantStates)
				wantCongestion := ssnmOwnershipValue(testContext, firstEvent.States[0].Congestion)
				wantOtherStates := ssnmOwnershipValue(testContext, firstEvent.States[1:])
				firstEvent.States[0].Availability.Scope.RoutingContexts[0] = 0xface0001
				requireSSNMOwnershipValue(testContext, "other dimension", firstEvent.States[0].Congestion, wantCongestion)
				requireSSNMOwnershipValue(testContext, "other destinations", firstEvent.States[1:], wantOtherStates)
				requireSSNMOwnershipValue(testContext, "retained state after one nested event mutation", endpoint.SSNMKnowledge(), wantSnapshot)
				requireSSNMOwnershipValue(testContext, "existing snapshot after one nested event mutation", snapshot, wantSnapshot)
				secondEvent := nextSSNMOwnershipEvent(testContext, second)
				requireSSNMOwnershipValue(testContext, "second subscriber after one nested event mutation", secondEvent, wantEvent)
				mutateSSNMOwnershipStates(firstEvent.States)
				requireSSNMOwnershipValue(testContext, "second subscriber", secondEvent, wantEvent)
				requireSSNMOwnershipValue(testContext, "retained state", endpoint.SSNMKnowledge(), wantSnapshot)
				otherSnapshot := endpoint.SSNMKnowledge()
				snapshot.Partitions[0].Destinations[0].Availability.Scope.RoutingContexts[0] = 0xface0002
				requireSSNMOwnershipValue(testContext, "store after one snapshot availability mutation", endpoint.SSNMKnowledge(), wantSnapshot)
				requireSSNMOwnershipValue(testContext, "other snapshot after one availability mutation", otherSnapshot, wantSnapshot)
				requireSSNMOwnershipValue(testContext, "subscriber after one snapshot availability mutation", secondEvent, wantEvent)
				snapshot.Partitions[0].Destinations[0].Congestion.Scope.RoutingContexts[0] = 0xface0003
				requireSSNMOwnershipValue(testContext, "store after one snapshot congestion mutation", endpoint.SSNMKnowledge(), wantSnapshot)
				requireSSNMOwnershipValue(testContext, "other snapshot after one congestion mutation", otherSnapshot, wantSnapshot)
				requireSSNMOwnershipValue(testContext, "subscriber after one snapshot congestion mutation", secondEvent, wantEvent)
				mutateSSNMOwnershipStates(snapshot.Partitions[0].Destinations)
				snapshot.Partitions[0].Bindings[0].Association = 99
				requireSSNMOwnershipValue(testContext, "snapshot isolation", endpoint.SSNMKnowledge(), wantSnapshot)
				if err := endpoint.ssnm.apply(ssnmOwnershipReport(count, SSNMDestinationRestrictedReport)); err != nil {
					testContext.Fatal(err)
				}
				requireSSNMOwnershipValue(testContext, "older delivered event", secondEvent, wantEvent)
			})
		}
	}
}

func TestSSNMBindingPublicationPreservesIndependentOwners(testContext *testing.T) {
	for _, count := range []int{1, 1024} {
		testContext.Run(fmt.Sprintf("APC%d", count), func(testContext *testing.T) {
			endpoint := newSSNMOwnershipEndpoint(testContext, count, true, nil)
			first := mustSubscribeSSNM(testContext, endpoint)
			second := mustSubscribeSSNM(testContext, endpoint)
			partition := ssnmOwnershipReport(count, SSNMDestinationUnavailableReport).Partition
			before := endpoint.SSNMKnowledge()
			previousRevision := before.Revision
			epoch := before.Partitions[0].Epoch
			steps := []struct {
				kind  SSNMEventKind
				apply func() error
			}{
				{SSNMBindingActivatedEvent, func() error { return endpoint.ssnm.bind(partition, 1, false) }},
				{SSNMBindingAdmittedEvent, func() error { return endpoint.ssnm.bind(partition, 2, false) }},
				{SSNMBindingRetiredEvent, func() error { endpoint.ssnm.retire(partition, 1); return nil }},
				{SSNMPartitionRetiredEvent, func() error { endpoint.ssnm.retire(partition, 2); return nil }},
			}
			for _, step := range steps {
				if err := step.apply(); err != nil {
					testContext.Fatal(err)
				}
				wantSnapshot := ssnmOwnershipValue(testContext, endpoint.SSNMKnowledge())
				event := nextSSNMOwnershipEvent(testContext, first)
				if event.Kind != step.kind || event.Revision != previousRevision+1 || event.Epoch != epoch {
					testContext.Fatalf("lifecycle event: kind=%v revision=%d epoch=%d", event.Kind, event.Revision, event.Epoch)
				}
				previousRevision = event.Revision
				if step.kind != SSNMPartitionRetiredEvent && len(event.States) != count {
					testContext.Fatalf("lifecycle lost retained destinations: %d", len(event.States))
				}
				wantEvent := ssnmOwnershipValue(testContext, event)
				mutateSSNMOwnershipStates(event.States)
				requireSSNMOwnershipValue(testContext, "lifecycle second subscriber", nextSSNMOwnershipEvent(testContext, second), wantEvent)
				requireSSNMOwnershipValue(testContext, "lifecycle retained state", endpoint.SSNMKnowledge(), wantSnapshot)
			}
		})
	}
}

func TestSSNMEventOnlyPublicationPreservesInputOwnership(testContext *testing.T) {
	for _, count := range []int{1, 1024} {
		for _, kind := range []SSNMReportKind{SSNMDestinationUserPartUnavailableReport, SSNMDestinationStateAuditReport, SSNMSignallingCongestionReport} {
			testContext.Run(fmt.Sprintf("APC%d/%s", count, kind), func(testContext *testing.T) {
				endpoint := newSSNMOwnershipEndpoint(testContext, count, false, nil)
				first := mustSubscribeSSNM(testContext, endpoint)
				second := mustSubscribeSSNM(testContext, endpoint)
				before := endpoint.SSNMKnowledge()
				wantKnowledge := ssnmOwnershipValue(testContext, before.Partitions)
				report := ssnmOwnershipReport(count, kind)
				report.PeerReported = kind == SSNMSignallingCongestionReport
				expected := report
				expected.Epoch = before.Partitions[0].Epoch
				expected.Revision = before.Revision + 1
				wantReport := ssnmOwnershipValue(testContext, expected)
				if err := endpoint.ssnm.apply(report); err != nil {
					testContext.Fatal(err)
				}
				mutateSSNMOwnershipReport(report)
				event := nextSSNMOwnershipEvent(testContext, first)
				if event.Kind != SSNMReportEvent || !event.ReportSet || len(event.States) != 0 {
					testContext.Fatal("event-only report gained retained state or lost report metadata")
				}
				requireSSNMOwnershipValue(testContext, "event-only input", event.Report, wantReport)
				wantEvent := ssnmOwnershipValue(testContext, event)
				mutateSSNMOwnershipReport(event.Report)
				requireSSNMOwnershipValue(testContext, "event-only fanout", nextSSNMOwnershipEvent(testContext, second), wantEvent)
				requireSSNMOwnershipValue(testContext, "event-only retained knowledge", endpoint.SSNMKnowledge().Partitions, wantKnowledge)
			})
		}
	}
}

func TestSSNMUnboundPublicationPreservesInputOwnership(testContext *testing.T) {
	for _, count := range []int{1, 1024} {
		testContext.Run(fmt.Sprintf("APC%d", count), func(testContext *testing.T) {
			endpoint := newSSNMStateEndpoint(testContext, nil, nil)
			first := mustSubscribeSSNM(testContext, endpoint)
			second := mustSubscribeSSNM(testContext, endpoint)
			before := endpoint.SSNMKnowledge()
			report := ssnmOwnershipReport(count, SSNMDestinationUnavailableReport)
			report.Epoch = 77
			expected := report
			expected.Revision = before.Revision + 1
			wantReport := ssnmOwnershipValue(testContext, expected)
			if err := endpoint.ssnm.apply(report); err != nil {
				testContext.Fatal(err)
			}
			mutateSSNMOwnershipReport(report)
			event := nextSSNMOwnershipEvent(testContext, first)
			if event.Kind != SSNMReportEvent || !event.ReportSet || len(event.States) != 0 ||
				event.Epoch != expected.Epoch || event.Revision != expected.Revision {
				testContext.Fatal("unbound publication changed metadata or invented retained state")
			}
			requireSSNMOwnershipValue(testContext, "unbound report after input mutation", event.Report, wantReport)
			wantEvent := ssnmOwnershipValue(testContext, event)
			mutateSSNMOwnershipReport(event.Report)
			otherEvent := nextSSNMOwnershipEvent(testContext, second)
			requireSSNMOwnershipValue(testContext, "unbound subscriber isolation", otherEvent, wantEvent)
			before.Revision++
			requireSSNMOwnershipValue(testContext, "unbound store", endpoint.SSNMKnowledge(), ssnmOwnershipValue(testContext, before))
			if err := endpoint.ssnm.bind(report.Partition, 1, false); err != nil {
				testContext.Fatal(err)
			}
			if err := endpoint.ssnm.apply(ssnmOwnershipReport(count, SSNMDestinationAvailableReport)); err != nil {
				testContext.Fatal(err)
			}
			requireSSNMOwnershipValue(testContext, "unbound event after later admission", otherEvent, wantEvent)
		})
	}
}

func TestSSNMPartitionPublicationOrdersPointCodeThenMask(testContext *testing.T) {
	endpoint := newSSNMStateEndpoint(testContext, nil, nil)
	availability := ssnmOwnershipReport(1, SSNMDestinationUnavailableReport)
	availability.Destinations = []PointCodeRange{{PointCode: 0x120100, Mask: 8}, {PointCode: 0x120000, Mask: 8}}
	congestion := ssnmOwnershipReport(1, SSNMSignallingCongestionReport)
	congestion.Destinations = []PointCodeRange{{PointCode: 0x120200}, {PointCode: 0x120000, Mask: 4}, {PointCode: 0x120100, Mask: 8}}
	if err := endpoint.ssnm.bind(availability.Partition, 1, false); err != nil {
		testContext.Fatal(err)
	}
	for _, report := range []SSNMReport{availability, congestion} {
		if err := endpoint.ssnm.apply(report); err != nil {
			testContext.Fatal(err)
		}
	}
	expected := []PointCodeRange{
		{PointCode: 0x120000, Mask: 4},
		{PointCode: 0x120000, Mask: 8},
		{PointCode: 0x120100, Mask: 8},
		{PointCode: 0x120200},
	}
	requireOrder := func(states []SSNMDestinationKnowledge) {
		testContext.Helper()
		actual := make([]PointCodeRange, len(states))
		for index, state := range states {
			actual[index] = state.Destination
		}
		if !reflect.DeepEqual(actual, expected) {
			testContext.Fatalf("destination union order = %+v, want %+v", actual, expected)
		}
	}
	snapshot, subscription, err := endpoint.SubscribeSSNM()
	if err != nil {
		testContext.Fatal(err)
	}
	testContext.Cleanup(func() { _ = subscription.Close() })
	requireOrder(snapshot.Partitions[0].Destinations)
	for range 16 {
		if err := endpoint.ssnm.apply(availability); err != nil {
			testContext.Fatal(err)
		}
		requireOrder(nextSSNMOwnershipEvent(testContext, subscription).States)
		requireOrder(endpoint.SSNMKnowledge().Partitions[0].Destinations)
	}
}

func TestSSNMPublicationOverflowAndResyncOwnership(testContext *testing.T) {
	for _, count := range []int{1, 1024} {
		for _, byteBound := range []bool{false, true} {
			testContext.Run(fmt.Sprintf("APC%d/bytes=%v", count, byteBound), func(testContext *testing.T) {
				limits := &SSNMStateConfig{SubscriptionQueueSize: 1}
				if byteBound {
					limits.SubscriptionQueueSize = 256
					limits.SubscriptionQueueBytes = 520 + count*280
				}
				endpoint := newSSNMOwnershipEndpoint(testContext, count, false, limits)
				slow := mustSubscribeSSNM(testContext, endpoint)
				healthy := mustSubscribeSSNM(testContext, endpoint)
				var firstEvent string
				for _, kind := range []SSNMReportKind{SSNMDestinationAvailableReport, SSNMDestinationRestrictedReport} {
					if err := endpoint.ssnm.apply(ssnmOwnershipReport(count, kind)); err != nil {
						testContext.Fatal(err)
					}
					event := nextSSNMOwnershipEvent(testContext, healthy)
					if event.Kind != SSNMReportEvent || event.Report.Kind != kind || len(event.States) != count {
						testContext.Fatal("healthy subscriber lost a complete report")
					}
					if firstEvent == "" {
						firstEvent = ssnmOwnershipValue(testContext, event)
					}
					wantRetained := ssnmOwnershipValue(testContext, endpoint.SSNMKnowledge())
					event.States[0].Availability.Scope.RoutingContexts[0] = 0xface0004
					requireSSNMOwnershipValue(testContext, "overflow store after one nested event mutation", endpoint.SSNMKnowledge(), wantRetained)
					mutateSSNMOwnershipReport(event.Report)
					mutateSSNMOwnershipStates(event.States)
				}
				requireSSNMOwnershipValue(testContext, "queued before loss", nextSSNMOwnershipEvent(testContext, slow), firstEvent)
				loss := nextSSNMOwnershipEvent(testContext, slow)
				if loss.Kind != SSNMContinuityLostEvent || !loss.ContinuityLost {
					testContext.Fatal("overflow did not report continuity loss after the queued event")
				}
				snapshot, err := slow.Resync()
				if err != nil {
					testContext.Fatal(err)
				}
				wantSnapshot := ssnmOwnershipValue(testContext, endpoint.SSNMKnowledge())
				requireSSNMOwnershipValue(testContext, "Resync result", snapshot, wantSnapshot)
				otherSnapshot := endpoint.SSNMKnowledge()
				snapshot.Partitions[0].Destinations[0].Availability.Scope.RoutingContexts[0] = 0xface0005
				requireSSNMOwnershipValue(testContext, "store after one Resync availability mutation", endpoint.SSNMKnowledge(), wantSnapshot)
				requireSSNMOwnershipValue(testContext, "other snapshot after one Resync availability mutation", otherSnapshot, wantSnapshot)
				snapshot.Partitions[0].Destinations[0].Congestion.Scope.RoutingContexts[0] = 0xface0006
				requireSSNMOwnershipValue(testContext, "store after one Resync congestion mutation", endpoint.SSNMKnowledge(), wantSnapshot)
				requireSSNMOwnershipValue(testContext, "other snapshot after one Resync congestion mutation", otherSnapshot, wantSnapshot)
				mutateSSNMOwnershipStates(snapshot.Partitions[0].Destinations)
				requireSSNMOwnershipValue(testContext, "Resync snapshot isolation", endpoint.SSNMKnowledge(), wantSnapshot)
				if err := endpoint.ssnm.apply(ssnmOwnershipReport(count, SSNMDestinationAvailableReport)); err != nil {
					testContext.Fatal(err)
				}
				event := nextSSNMOwnershipEvent(testContext, slow)
				if event.Revision != snapshot.Revision+1 || event.Kind != SSNMReportEvent {
					testContext.Fatal("Resync did not restore the next delta")
				}
				wantEvent := ssnmOwnershipValue(testContext, event)
				mutateSSNMOwnershipStates(event.States)
				requireSSNMOwnershipValue(testContext, "post-Resync healthy subscriber", nextSSNMOwnershipEvent(testContext, healthy), wantEvent)
			})
		}
	}
}

func TestSSNMPublicationConcurrentOwnedConsumers(testContext *testing.T) {
	const count, reports = 1024, 8
	endpoint := newSSNMOwnershipEndpoint(testContext, count, false, &SSNMStateConfig{SubscriptionQueueBytes: 8 << 20})
	first := mustSubscribeSSNM(testContext, endpoint)
	second := mustSubscribeSSNM(testContext, endpoint)
	recovery := mustSubscribeSSNM(testContext, endpoint)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	start := make(chan struct{})
	results := make(chan error, 3)
	for _, subscription := range []*SSNMSubscription{first, second} {
		go func() {
			<-start
			var previousRevision uint64
			for range reports {
				event, err := subscription.Next(ctx)
				if err != nil {
					results <- err
					return
				}
				if event.Kind != SSNMReportEvent || len(event.States) != count || event.Revision <= previousRevision {
					results <- fmt.Errorf("incomplete or unordered concurrent publication")
					return
				}
				previousRevision = event.Revision
				for _, state := range event.States {
					if !reflect.DeepEqual(state.Availability.Scope.RoutingContexts, []uint32{1, 2}) ||
						!reflect.DeepEqual(state.Congestion.Scope.RoutingContexts, []uint32{1, 2}) {
						results <- fmt.Errorf("another consumer mutated a retained scope")
						return
					}
				}
				mutateSSNMOwnershipReport(event.Report)
				mutateSSNMOwnershipStates(event.States)
			}
			results <- nil
		}()
	}
	go func() {
		<-start
		for range reports {
			snapshot, err := recovery.Resync()
			if err != nil {
				results <- err
				return
			}
			mutateSSNMOwnershipStates(snapshot.Partitions[0].Destinations)
		}
		results <- recovery.Close()
	}()
	close(start)
	for range reports {
		if err := endpoint.ssnm.apply(ssnmOwnershipReport(count, SSNMDestinationUnavailableReport)); err != nil {
			testContext.Fatal(err)
		}
	}
	for range 3 {
		select {
		case err := <-results:
			if err != nil {
				testContext.Error(err)
			}
		case <-ctx.Done():
			testContext.Fatal(ctx.Err())
		}
	}
	if _, err := recovery.Resync(); !errors.Is(err, ErrSSNMSubscriptionClosed) {
		testContext.Fatalf("Resync after Close: %v", err)
	}
	for _, state := range endpoint.SSNMKnowledge().Partitions[0].Destinations {
		if !reflect.DeepEqual(state.Availability.Scope.RoutingContexts, []uint32{1, 2}) ||
			!reflect.DeepEqual(state.Congestion.Scope.RoutingContexts, []uint32{1, 2}) {
			testContext.Fatal("concurrent caller mutation reached retained knowledge")
		}
	}
}
