package m3ua

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestSingleDestinationHistoryEventsFitAndReproduceStore(t *testing.T) {
	e := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), nil)
	now := time.Date(2026, 11, 16, 0, 0, 0, 0, time.UTC)
	configureAutomationForTest(e, &now)
	contexts := make([]uint32, 16300)
	for i := range contexts {
		contexts[i] = 1
	}
	var last *Association
	for i := 0; i < 21; i++ {
		a := attachSSNMAssociation(t, e, SGPIdentity{"sg-a", "sgp-a1"}, 7, 1)
		if i == 20 {
			last = a
			break
		}
		m := messages.NewSignallingCongestion(params.NewNetworkAppearance(7), params.NewRoutingContext(contexts...), params.NewAffectedPointCode(100), nil, params.NewCongestionIndications(2), nil)
		if wire, err := m.MarshalBinary(); err != nil || len(wire) > 65535 {
			t.Fatalf("invalid fixture wire: size=%d error=%v", len(wire), err)
		}
		if i%2 == 0 {
			if err := a.handleSignallingCongestion(m); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := a.handleDestinationUnavailable(messages.NewDestinationUnavailable(params.NewNetworkAppearance(7), params.NewRoutingContext(contexts...), params.NewAffectedPointCode(100), nil)); err != nil {
				t.Fatal(err)
			}
		}
	}
	before := e.SSNMKnowledge()
	measure := newSSNMSubscription(e.ssnm, 256, 16<<20)
	e.ssnm.mu.Lock()
	e.ssnm.subscribers[measure] = struct{}{}
	e.ssnm.mu.Unlock()
	defer func() { _ = measure.Close() }()
	sendSCON(t, last, 7, 1, congestionLevel(3), 100)
	events, _, lost := outcomeProbeEvents(measure)
	if lost {
		t.Fatal("measurement subscription lost continuity")
	}
	raw := 0
	previous := before.Revision
	for _, event := range events {
		if event.Kind != SSNMReportObservedEvent && event.Kind != SSNMReportPatchEvent {
			continue
		}
		if event.Kind == SSNMReportObservedEvent && event.ReportPatch != nil {
			t.Fatal("report observation carried a partial patch")
		}
		if event.Kind == SSNMReportPatchEvent && (event.ReportSet || len(event.DestinationUpdates) != 0 || event.ReportPatch == nil) {
			t.Fatal("patch reused complete report shape")
		}
		if _, fits := ssnmEventAccountedBytes(event, DefaultSSNMSubscriptionQueueBytes); !fits {
			t.Fatal("single destination history event exceeds default byte budget")
		}
		if event.Revision <= previous {
			t.Fatal("fragment event revisions do not increase")
		}
		previous = event.Revision
		if event.ReportSet {
			raw++
		}
		probe := newSSNMSubscription(nil, 256, 1<<20)
		probe.enqueue(event)
		if _, ready, err := probe.take(); !ready || err != nil || probe.continuityLost {
			t.Fatal("draining subscriber lost one report fragment")
		}
		// The interface keeps this regression executable against the older surface.
		applier, ok := any(event).(interface {
			ApplyReport(*SSNMPartitionKnowledge) bool
		})
		if !ok {
			t.Fatal("bounded report fragments lack an exact replica operation")
		}
		if applier.ApplyReport(nil) {
			t.Fatal("report replica helper accepted a nil target")
		}
		wrong := SSNMPartitionKnowledge{Partition: event.Partition, Epoch: event.Epoch + 1}
		if applier.ApplyReport(&wrong) {
			t.Fatal("report replica helper crossed a partition epoch")
		}
		applied := false
		for i := range before.Partitions {
			applied = applier.ApplyReport(&before.Partitions[i]) || applied
		}
		if !applied {
			t.Fatal("report fragment did not apply to its partition")
		}
	}
	if raw != 1 {
		t.Fatalf("raw report observations=%d, want one", raw)
	}
	after := e.SSNMKnowledge()
	if !reflect.DeepEqual(before.Partitions, after.Partitions) {
		t.Fatal("report fragments do not reproduce exact retained source history")
	}
	for _, event := range events {
		updates := event.DestinationUpdates
		if event.ReportPatch != nil {
			updates = []SSNMDestinationKnowledge{event.ReportPatch.State}
		}
		for _, update := range updates {
			for _, source := range update.AvailabilitySources {
				source.Scope.RoutingContexts[0] = 999
			}
			for _, source := range update.CongestionSources {
				source.Scope.RoutingContexts[0] = 999
			}
		}
	}
	if !reflect.DeepEqual(before.Partitions, after.Partitions) {
		t.Fatal("replica aliases fragment source scopes")
	}
}

func TestReportFragmentByteCeilingAndCompletion(t *testing.T) {
	s, err := newSSNMState(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	sub := newSSNMSubscription(s, 256, 8<<20)
	s.subscribers[sub] = struct{}{}
	partition := canonicalSSNMPartition(SignallingGatewayID(strings.Repeat("g", 255)), RemoteASID(strings.Repeat("a", 253)))
	update := SSNMDestinationKnowledge{Destination: PointCodeRange{PointCode: 100}, CongestionSet: true, Congestion: SSNMCongestion{Scope: testWireScope(7, true, 1)}}
	for i := 0; i < 17; i++ {
		count := 16382
		if i == 3 {
			count = 15798
		}
		update.CongestionSources = append(update.CongestionSources, SSNMCongestion{Scope: WireScope{RoutingContextSet: true, RoutingContexts: make([]uint32, count)}})
	}
	event := SSNMEvent{Kind: SSNMReportObservedEvent, Revision: 1, Epoch: 1, Partition: partition, ReportSet: true, Report: SSNMReport{Partition: partition, Scope: testWireScope(7, true, 1), Destinations: []PointCodeRange{{PointCode: 100}}}, DestinationUpdates: []SSNMDestinationKnowledge{update}}
	s.mu.Lock()
	s.revision = 1
	s.publishLocked(event)
	s.mu.Unlock()
	events, _, lost := outcomeProbeEvents(sub)
	if lost || len(events) < 3 {
		t.Fatal("maximum source history did not produce bounded report fragments")
	}
	sawCeiling := false
	for i, e := range events {
		if i > 0 {
			cost, fits := ssnmEventAccountedBytes(e, 262144)
			if !fits {
				t.Fatal("report patch exceeded 256KiB bound")
			}
			sawCeiling = sawCeiling || cost == 262144
		}
		v := reflect.ValueOf(e)
		more := v.FieldByName("ReportContinues")
		if !more.IsValid() || more.Bool() != (i+1 < len(events)) {
			t.Fatal("report fragments lack an exact completion boundary")
		}
	}
	if !sawCeiling {
		t.Fatal("fragment fixture did not reach its exact byte ceiling")
	}
}

func TestMultiDestinationFragmentsReproduceStore(t *testing.T) {
	s, err := newSSNMState(&SSNMStateConfig{MaxRecordsPerPartition: 8192, MaxAffectedPointCodes: 4097})
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	partition := canonicalSSNMPartition("sg", "as")
	s.registerSource(1, 1)
	if err = s.bind(partition, 1, false, 1); err != nil {
		t.Fatal(err)
	}
	before := s.snapshot().Partitions[0]
	sub := newSSNMSubscription(s, 8192, 8<<20)
	s.subscribers[sub] = struct{}{}
	points := make([]PointCodeRange, 4097)
	for i := range points {
		points[i].PointCode = uint32(i)
	}
	report := SSNMReport{Kind: SSNMSignallingCongestionReport, Source: SSNMPeerReport, Association: 1, Partition: partition, Scope: testWireScope(7, true, 1), Destinations: points, CongestionLevel: 2, CongestionLevelSet: true}
	if err = s.apply(report); err != nil {
		t.Fatal(err)
	}
	events, _, lost := outcomeProbeEvents(sub)
	if lost || len(events) < 3 {
		t.Fatal("large multi-destination report was not fragmented")
	}
	raw := 0
	for i, e := range events {
		if _, fits := ssnmEventAccountedBytes(e, 1<<20); !fits {
			t.Fatal("multi-destination fragment exceeds the default budget")
		}
		more := reflect.ValueOf(e).FieldByName("ReportContinues")
		if !more.IsValid() || more.Bool() != (i+1 < len(events)) {
			t.Fatal("multi-destination report completed before its last patch")
		}
		if e.ReportSet {
			raw++
		}
		applier, ok := any(e).(interface {
			ApplyReport(*SSNMPartitionKnowledge) bool
		})
		if !ok || !applier.ApplyReport(&before) {
			t.Fatal("multi-destination patch did not apply")
		}
	}
	if raw != 1 || !reflect.DeepEqual(before, s.snapshot().Partitions[0]) {
		t.Fatal("multi-destination fragments do not reproduce exact store state")
	}
}
