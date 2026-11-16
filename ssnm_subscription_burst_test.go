package m3ua

import (
	"context"
	"errors"
	"fmt"
	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"reflect"
	"testing"
	"time"
)

func assertBurstResyncExact(t *testing.T, e *Endpoint, sub *SSNMSubscription) {
	t.Helper()
	queued, _, lost := outcomeProbeEvents(sub)
	if !lost {
		t.Fatal("burst did not lose continuity")
	}
	previous := uint64(0)
	for i := 0; i <= len(queued); i++ {
		event, ready, err := sub.take()
		if err != nil || !ready {
			t.Fatal("missing queued delta or continuity barrier", err)
		}
		if i == len(queued) {
			if event.Kind != SSNMContinuityLostEvent || !event.ContinuityLost {
				t.Fatal("overflow was not reported explicitly")
			}
		} else if event.Revision <= previous {
			t.Fatal("queued event ordering changed")
		} else {
			previous = event.Revision
		}
	}
	if _, ready, _ := sub.take(); ready {
		t.Fatal("continuity barrier was repeated or deltas escaped it")
	}
	want := e.SSNMKnowledge()
	got, err := sub.Resync()
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("Resync does not reproduce exact retained state", err)
	}
	sub.mu.Lock()
	clean := !sub.continuityLost && !sub.pendingLoss && sub.queuedBytes == 0 && len(sub.queue) == 0
	sub.mu.Unlock()
	if !clean {
		t.Fatal("Resync did not reset the queue and continuity barrier")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := sub.Next(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("Resync replayed stale deltas", err)
	}
}

// TestMaximumReportAndExpiryResyncReproducesStore exercises the real
// retention/report/expiry methods with every SSNM limit left at its default.
// It verifies that a legal final report plus expiry can overflow an empty
// default subscription and that the explicit barrier permits exact recovery.
func TestMaximumReportAndExpiryResyncReproducesStore(t *testing.T) {
	store, err := newSSNMState(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer store.close()
	now := time.Date(2026, 11, 15, 0, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	store.validity = time.Second
	points := make([]PointCodeRange, 1024)
	for i := range points {
		points[i].PointCode = uint32(i)
	}
	reports := make([]SSNMReport, 0, 8)
	keys := make([]ASKey, 0, 8)
	for gateway := 0; gateway < 2; gateway++ {
		association := AssociationID(gateway + 1)
		store.registerSource(association, 1)
		for server := 0; server < 4; server++ {
			partition := SSNMPartition{Kind: SSNMCanonicalPartition, SignallingGateway: SignallingGatewayID(string(rune('a' + gateway))), ApplicationServer: RemoteASID(string(rune('a' + server)))}
			key := ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContext: uint32(server + 1), RoutingContextSet: true}
			if err := store.bind(partition, association, false, 1); err != nil {
				t.Fatal(err)
			}
			reports = append(reports, SSNMReport{Kind: SSNMSignallingCongestionReport, Source: SSNMPeerReport, Association: association, Partition: partition, Scope: WireScope{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContexts: []uint32{key.RoutingContext}, RoutingContextSet: true}, Destinations: points, CongestionLevel: 2, CongestionLevelSet: true})
			keys = append(keys, key)
		}
	}
	// Bind all partitions before subscribing, then seed seven independent
	// source partitions. None of their earlier events is queued for this sub.
	for i := 0; i < 7; i++ {
		if err := store.apply(reports[i], []ASKey{keys[i]}); err != nil {
			t.Fatal(err)
		}
	}
	endpoint := &Endpoint{ssnm: store}
	_, subscription, err := endpoint.SubscribeSSNM()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = subscription.Close() }()
	if err := store.apply(reports[7], []ASKey{keys[7]}); err != nil {
		t.Fatal(err)
	}
	if store.records != 16384 || store.bytes != 2426880 || len(store.partitions) != 8 {
		t.Fatalf("fixture did not legally fill default retention: records=%d bytes=%d partitions=%d", store.records, store.bytes, len(store.partitions))
	}
	for _, records := range store.peerRecords {
		if records != 8192 {
			t.Fatalf("fixture peer reservation=%d, want8192", records)
		}
	}
	events, reportBytes, lost := outcomeProbeEvents(subscription)
	if lost || len(events) != 1 || events[0].Kind != SSNMReportObservedEvent || len(events[0].Report.Destinations) != 1024 || len(events[0].DestinationUpdates) != 1024 {
		t.Fatalf("one legal final report did not fit alone: events=%d bytes=%d lost=%v", len(events), reportBytes, lost)
	}
	t.Logf("legal report: events=%d accounted_bytes=%d retained_records=%d retained_bytes=%d", len(events), reportBytes, store.records, store.bytes)
	now = now.Add(time.Second)
	changed := store.expireCongestion(now)
	if len(changed) != 8 {
		t.Fatalf("full expiry did not visit all8partitions: %d", len(changed))
	}
	events, bytes, lost := outcomeProbeEvents(subscription)
	expired := 0
	for _, event := range events {
		expired += len(event.Expired)
	}
	t.Logf("report plus expiry: queued_events=%d accounted_bytes=%d queued_expiry_ids=%d continuity_lost=%v", len(events), bytes, expired, lost)
	if !lost {
		t.Fatal("legal report plus expiry must exercise the default continuity barrier")
	}
	assertBurstResyncExact(t, endpoint, subscription)
}

// TestMultiASReportResyncReproducesStore drives the receive handler,
// including real scope resolution and per-canonical-partition publication.
// All store/subscription/APC limits keep their defaults. Configuring256ASes is
// legal and creates exactly the default maximum256canonical partitions.
func TestMultiASReportResyncReproducesStore(t *testing.T) {
	servers := make([]RemoteASConfig, 256)
	contexts := make([]uint32, 256)
	for i := range servers {
		contexts[i] = uint32(i + 1)
		servers[i] = RemoteASConfig{ID: RemoteASID(fmt.Sprintf("%03d", i)), ASKey: staticASKey(7, contexts[i])}
	}
	cfg := &ASPConfig{SignallingGateways: []SignallingGatewayConfig{{ID: "a", SGPs: []SignallingGatewayProcessConfig{{ID: "p", ApplicationServers: servers}}}}}
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: cfg, Congestion: &CongestionConfig{Default: CongestionProfile{Method: CongestionNationalWithPriorities, MaxLevel: 3}, Validity: &CongestionValidityConfig{After: time.Hour}}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = endpoint.Close() }()
	association := attachSSNMAssociation(t, endpoint, SGPIdentity{SignallingGateway: "a", SignallingGatewayProcess: "p"}, 7, contexts...)
	_, subscription, err := endpoint.SubscribeSSNM()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = subscription.Close() }()
	message := messages.NewSignallingCongestion(params.NewNetworkAppearance(7), params.NewRoutingContext(contexts...), params.NewAffectedPointCode(0x123456), nil, params.NewCongestionIndications(2), nil)
	wire, err := message.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if len(wire) != 1060 {
		t.Fatalf("fixture wire size=%d, want1060", len(wire))
	}
	if err := association.handleSignallingCongestion(message); err != nil {
		t.Fatalf("legal multi-AS report refused: %v", err)
	}
	if endpoint.ssnm.records != 512 || endpoint.ssnm.bytes != 663552 {
		t.Fatalf("unexpected retained shape: records=%d bytes=%d", endpoint.ssnm.records, endpoint.ssnm.bytes)
	}
	events, bytes, lost := outcomeProbeEvents(subscription)
	t.Logf("one legal multi-AS report: queued_events=%d accounted_bytes=%d lost=%v", len(events), bytes, lost)
	if !lost {
		t.Fatal("legal multi-AS report must exercise the default continuity barrier")
	}
	assertBurstResyncExact(t, endpoint, subscription)
}
