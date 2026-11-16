package m3ua

import (
	"strings"
	"testing"
)

func TestMaximumSingleSSNMEventsFitDefaultSubscription(t *testing.T) {
	partition := canonicalSSNMPartition(SignallingGatewayID(strings.Repeat("g", 255)), RemoteASID(strings.Repeat("a", 255)))
	scope := WireScope{RoutingContextSet: true, RoutingContexts: make([]uint32, 16382)}
	raw := SSNMEvent{Kind: SSNMReportObservedEvent, Partition: partition, Report: SSNMReport{Partition: partition, Scope: scope, Destinations: make([]PointCodeRange, 16382), ProfileMismatches: make([]CongestionProfileMismatch, 16382)}}
	// This largest even-sized fast-path shape remains below the strict ceiling.
	fastPartition := partition
	fastPartition.ApplicationServer = RemoteASID(strings.Repeat("a", 254))
	fast := SSNMEvent{Kind: SSNMReportObservedEvent, Partition: fastPartition, Report: SSNMReport{Partition: fastPartition, Scope: testWireScope(7, true, 1), Destinations: make([]PointCodeRange, 3153)}, DestinationUpdates: make([]SSNMDestinationKnowledge, 3153)}
	for i := range fast.DestinationUpdates {
		fast.Report.Destinations[i].PointCode = uint32(i)
		fast.DestinationUpdates[i].Destination.PointCode = uint32(i)
		fast.DestinationUpdates[i].Congestion.Scope = testWireScope(7, true, 1)
	}
	fast.DestinationUpdates[0].Availability.Scope = WireScope{RoutingContextSet: true, RoutingContexts: make([]uint32, 61)}
	publication := func(count int) SSNMEvent {
		p := CongestionPublicationState{Scope: WireScope{RoutingContextSet: true, RoutingContexts: make([]uint32, count)}, Destinations: []PointCodeRange{{PointCode: 100}}, LastError: strings.Repeat("e", 256)}
		return SSNMEvent{Kind: SSNMPublicationOutcomeEvent, Reason: "reporter-retired", Report: SSNMReport{Scope: p.Scope}, CongestionPublication: &p}
	}
	cases := []struct {
		name  string
		event SSNMEvent
		bytes int
	}{
		{"raw-report", raw, 722340}, {"complete-report", fast, 1048574},
		{"binding-admitted", SSNMEvent{Kind: SSNMBindingAdmittedEvent, Partition: partition}, 1022},
		{"binding-activated", SSNMEvent{Kind: SSNMBindingActivatedEvent, Partition: partition}, 1022},
		{"binding-retired", SSNMEvent{Kind: SSNMBindingRetiredEvent, Partition: partition}, 1022},
		{"partition-retired", SSNMEvent{Kind: SSNMPartitionRetiredEvent, Partition: partition}, 1022},
		{"peer-added", SSNMEvent{Kind: SSNMPeerCongestionEvent, Partition: partition, PeerCongestion: &PeerCongestionState{Scope: scope}}, 66806},
		{"peer-removed", SSNMEvent{Kind: SSNMPeerCongestionEvent, Partition: partition, PeerCongestion: &PeerCongestionState{Scope: scope}, PeerCongestionRemoved: true}, 66806},
		{"invalidation", SSNMEvent{Kind: SSNMDestinationsInvalidatedEvent, Reason: strings.Repeat("r", 160), Invalidated: make([]SSNMDestinationInvalidation, 2048), CongestionSourcesInvalidated: make([]SSNMCongestionSourceInvalidation, 2048), AvailabilitySourcesInvalidated: make([]SSNMAvailabilitySourceInvalidation, 2048)}, 230048},
		{"expiry", SSNMEvent{Kind: SSNMCongestionExpiredEvent, Expired: make([]SSNMCongestionExpiry, 256)}, 14848},
		{"resource-loss", SSNMEvent{Kind: SSNMResourceLossEvent, Reason: strings.Repeat("r", 160)}, 672},
		{"audit-outcome", SSNMEvent{Kind: SSNMPublicationOutcomeEvent, Reason: "reporter-retired", Report: SSNMReport{Scope: WireScope{RoutingContextSet: true, RoutingContexts: make([]uint32, 64)}}}, 784},
		{"publication-default", publication(8095), 65808}, {"publication-wire-maximum", publication(16382), 132104},
		{"continuity-marker", SSNMEvent{Kind: SSNMContinuityLostEvent, ContinuityLost: true}, 512},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			bytes, fits := ssnmEventAccountedBytes(c.event, 1<<20)
			if !fits || bytes != c.bytes || bytes >= 1<<20 {
				t.Fatalf("maximum %s event: bytes=%d fit=%v want=%d below default", c.name, bytes, fits, c.bytes)
			}
			sub := newSSNMSubscription(nil, 256, 1<<20)
			sub.enqueue(c.event)
			if _, ready, err := sub.take(); !ready || err != nil || sub.continuityLost {
				t.Fatal("one maximum event lost continuity")
			}
		})
	}
}

func TestMaximumMTPIndicationsFitAndOverflowExplicitly(t *testing.T) {
	r, err := newASPRoutes(nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []MTPIndicationKind{MTPPauseIndication, MTPResumeIndication, MTPStatusIndication} {
		i := &MTPIndication{Kind: kind, Destination: MTPDestinationStatus{Destination: MTPDestination{MTPRoute: MTPRouteID(strings.Repeat("r", 255))}}}
		r.publishOneLocked(i)
		if got := <-r.indications; got != i || got.ResyncRequired {
			t.Fatal("one maximum MTP indication was lost")
		}
	}
	for i := 0; i < 257; i++ {
		r.publishOneLocked(&MTPIndication{Kind: MTPStatusIndication})
	}
	if len(r.indications) != 1 || !(<-r.indications).ResyncRequired {
		t.Fatal("MTP burst did not use its existing resync barrier")
	}
}

func TestExistingDiagnosticAndRequestStringBounds(t *testing.T) {
	if len(boundedPublicationError(&longSubscriptionError{})) != 256 {
		t.Fatal("publication error exceeded its 256-byte bound")
	}
	if len(boundSSNMReason(strings.Repeat("r", 256))) != 160 {
		t.Fatal("resource reason exceeded its 160-byte bound")
	}
	if _, err := buildSSNMInfoString(strings.Repeat("i", 255)); err != nil {
		t.Fatal("maximum INFO refused", err)
	}
	if _, err := buildSSNMInfoString(strings.Repeat("i", 256)); err == nil {
		t.Fatal("overlong INFO accepted")
	}
	e, c, key := localCongestionFixture(t)
	if err := e.ReportLocalCongestion(localRequest(c, key, strings.Repeat("s", 64), 2)); err != nil {
		t.Fatal("maximum local source refused", err)
	}
	if err := e.ReportLocalCongestion(localRequest(c, key, strings.Repeat("s", 65), 2)); err == nil {
		t.Fatal("overlong local source accepted")
	}
}

type longSubscriptionError struct{}

func (*longSubscriptionError) Error() string { return strings.Repeat("e", 300) }

func TestMaximumInvalidationProducerChunksFitDefault(t *testing.T) {
	s, sub := outcomeProbeSubscription()
	d := make([]SSNMDestinationInvalidation, 2049)
	c := make([]SSNMCongestionSourceInvalidation, 2049)
	a := make([]SSNMAvailabilitySourceInvalidation, 2049)
	for i := range d {
		d[i].Destination.PointCode = uint32(i)
		c[i].Destination.PointCode = uint32(i)
		a[i].Destination.PointCode = uint32(i)
	}
	s.publishDestinationRemovalsLocked(&ssnmPartitionState{epoch: 1}, d, c, strings.Repeat("r", 200), a)
	events, _, lost := outcomeProbeEvents(sub)
	if lost || len(events) != 2 {
		t.Fatal("invalidation did not retain its two bounded chunks")
	}
	for i, e := range events {
		if len(e.Invalidated) > 2048 || len(e.CongestionSourcesInvalidated) > 2048 || len(e.AvailabilitySourcesInvalidated) > 2048 {
			t.Fatal("invalidation producer exceeded its 2048-identity bound")
		}
		bytes, ok := ssnmEventAccountedBytes(e, 230048)
		if !ok || i == 0 && bytes != 230048 {
			t.Fatal("maximum invalidation producer exceeded its byte bound", bytes)
		}
	}
}
