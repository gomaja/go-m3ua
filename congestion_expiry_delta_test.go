package m3ua

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestExpiryDeltaReplaysOnlyMatchingVersions(t *testing.T) {
	key := routingContextASKey(7)
	other := routingContextASKey(8)
	record := SSNMCongestion{AS: key, Revision: 11, Congested: true, Level: 2, LevelSet: true, Scope: testWireScope(7, true, 7), Association: 4, Epoch: 9}
	renewed := record
	renewed.AS, renewed.Revision, renewed.Level = other, 12, 3
	peer := PeerCongestionState{AS: key, Revision: 13, Destination: PointCodeRange{PointCode: 200}, Association: 5, Epoch: 99, Scope: testWireScope(7, true, 7), Congestion: CongestionState{Congested: true, Level: 2, LevelSet: true}}
	snapshot := SSNMSnapshot{Revision: 20, Partitions: []SSNMPartitionKnowledge{
		{Epoch: 1, Destinations: []SSNMDestinationKnowledge{{Destination: PointCodeRange{PointCode: 100, Mask: 1}, Congestion: record, CongestionSet: true, CongestionSources: []SSNMCongestion{record, renewed}}}},
		{Epoch: 2},
	}, PeerCongestion: []PeerCongestionState{peer}}
	event := SSNMEvent{Kind: SSNMCongestionExpiredEvent, Revision: 21, Expired: []SSNMCongestionExpiry{
		{Epoch: 1, Destination: PointCodeRange{PointCode: 100, Mask: 1}, AS: key, Revision: 11},
		{Epoch: 2, Destination: peer.Destination, AS: key, Revision: 13, Peer: true},
	}}
	if !event.ApplyExpiry(&snapshot) {
		t.Fatal("matching expiry was not accepted")
	}
	if !event.ApplyExpiry(&snapshot) {
		t.Fatal("matching expiry was not accepted idempotently")
	}
	if snapshot.Revision != 20 {
		t.Fatal("expiry helper changed caller-owned stream revision")
	}
	destination := snapshot.Partitions[0].Destinations[0]
	gotScalar := destination.Congestion
	gotScalar.Freshness = CongestionFresh
	gotSource := destination.CongestionSources[0]
	gotSource.Freshness = CongestionFresh
	gotPeer := snapshot.PeerCongestion[0]
	gotPeer.Freshness = CongestionFresh
	if destination.Congestion.Freshness != CongestionExpired || destination.CongestionSources[0].Freshness != CongestionExpired || snapshot.PeerCongestion[0].Freshness != CongestionExpired ||
		!reflect.DeepEqual(gotScalar, record) || !reflect.DeepEqual(gotSource, record) || !reflect.DeepEqual(gotPeer, peer) || !reflect.DeepEqual(destination.CongestionSources[1], renewed) {
		t.Fatal("expiry replay changed values/provenance or expired a newer independent source", snapshot)
	}
}

func TestExpiryDeltaRejectsStaleIdentities(t *testing.T) {
	for _, peer := range []bool{false, true} {
		for _, mismatch := range []string{"epoch", "zero-epoch", "revision", "AS", "destination", "kind"} {
			t.Run(fmt.Sprintf("peer=%v/%s", peer, mismatch), func(t *testing.T) {
				key := routingContextASKey(7)
				id := SSNMCongestionExpiry{Epoch: 10, Destination: PointCodeRange{PointCode: 100}, AS: key, Revision: 20, Peer: peer}
				record := SSNMCongestion{AS: key, Revision: 20, Congested: true}
				snapshot := SSNMSnapshot{Partitions: []SSNMPartitionKnowledge{{Epoch: 10, Destinations: []SSNMDestinationKnowledge{{Destination: id.Destination, Congestion: record, CongestionSet: true, CongestionSources: []SSNMCongestion{record}}}}}, PeerCongestion: []PeerCongestionState{{AS: key, Revision: 20, Destination: id.Destination}}}
				event := SSNMEvent{Kind: SSNMCongestionExpiredEvent, Expired: []SSNMCongestionExpiry{id}}
				switch mismatch {
				case "epoch":
					event.Expired[0].Epoch++
				case "zero-epoch":
					event.Expired[0].Epoch = 0
				case "revision":
					event.Expired[0].Revision++
				case "AS":
					event.Expired[0].AS.RoutingContext++
				case "destination":
					event.Expired[0].Destination.PointCode++
				case "kind":
					event.Kind = SSNMReportEvent
				}
				if event.ApplyExpiry(&snapshot) || snapshot.Partitions[0].Destinations[0].Congestion.Freshness != CongestionFresh || snapshot.Partitions[0].Destinations[0].CongestionSources[0].Freshness != CongestionFresh || snapshot.PeerCongestion[0].Freshness != CongestionFresh {
					t.Fatal("stale identity changed retained knowledge")
				}
				if event.ApplyExpiry(nil) {
					t.Fatal("nil snapshot accepted")
				}
			})
		}
	}
}

func TestExpiryDeltaOwnsIdentitiesAndAccountsTheirBound(t *testing.T) {
	state, one := outcomeProbeSubscription()
	two := newSSNMSubscription(state, DefaultSSNMSubscriptionQueueSize, DefaultSSNMSubscriptionQueueBytes)
	state.subscribers[two] = struct{}{}
	event := SSNMEvent{Kind: SSNMCongestionExpiredEvent, Expired: make([]SSNMCongestionExpiry, 256)}
	for i := range event.Expired {
		event.Expired[i] = SSNMCongestionExpiry{Epoch: 1, Revision: uint64(i + 1), AS: routingContextASKey(7), Destination: PointCodeRange{PointCode: uint32(i)}}
	}
	if got, fits := ssnmEventAccountedBytes(event, 512+256*56); !fits || got != 512+256*56 {
		t.Fatal("expiry identity charge changed", got, fits)
	}
	if _, fits := ssnmEventAccountedBytes(event, 512+256*56-1); fits {
		t.Fatal("expiry event fitted below its exact charge")
	}
	state.publishLocked(event)
	event.Expired[0].Revision = 999
	first, _, _ := outcomeProbeEvents(one)
	second, _, _ := outcomeProbeEvents(two)
	if first[0].Expired[0].Revision != 1 || second[0].Expired[0].Revision != 1 {
		t.Fatal("queued expiry identities alias publication input")
	}
	first[0].Expired[0].AS.RoutingContext = 999
	if second[0].Expired[0].AS.RoutingContext != 7 {
		t.Fatal("expiry identities alias another subscriber")
	}
	again, _, _ := outcomeProbeEvents(one)
	if again[0].Expired[0].AS.RoutingContext != 7 {
		t.Fatal("owned event aliases its queued expiry identities")
	}
}

func TestWholeRecordBoundExpiryFitsDefaultSubscription(t *testing.T) {
	// Raise only the retention byte limit so the full default record ceiling
	// can be populated with peer records plus partition overhead. The default
	// queue and default record/partition/peer caps are exercised unchanged.
	store, template, bindings := peerRetentionFixture(t, &SSNMStateConfig{MaxBytes: 8 << 20}, 1, routingContextASKey(7))
	now := time.Now()
	store.now = func() time.Time { return now }
	store.validity = time.Second
	for association := AssociationID(1); association <= 8; association++ {
		report := template
		report.Association = association
		report.Partition = SSNMPartition{Kind: SSNMStandalonePartition, Association: association}
		if association != 1 {
			store.registerSource(association)
			if err := store.bind(report.Partition, association, false); err != nil {
				t.Fatal(err)
			}
			store.syncPeerCongestion(association, 1, bindings)
		}
		for half := 0; half < 2; half++ {
			report.Destinations = make([]PointCodeRange, 1024)
			for i := range report.Destinations {
				report.Destinations[i].PointCode = uint32(half*1024 + i)
			}
			peerRetentionApply(t, store, report, 1, bindings)
		}
	}
	if store.records != DefaultMaxSSNMStateStoreRecords {
		t.Fatal("fixture did not fill the record ceiling", store.records)
	}
	endpoint := &Endpoint{ssnm: store}
	replica, sub, err := endpoint.SubscribeSSNM()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close() }()
	initialRevision := replica.Revision
	now = now.Add(time.Second)
	if changed := store.expireCongestion(now); len(changed) != 0 {
		t.Fatal("peer-local expiry requested destination projection", changed)
	}
	events, bytes, lost := outcomeProbeEvents(sub)
	if lost || len(events) != 64 || bytes != 950272 || bytes > DefaultSSNMSubscriptionQueueBytes {
		t.Fatal("whole-record expiry exceeded default queue envelope", len(events), bytes, lost)
	}
	count := 0
	for _, event := range events {
		if event.Kind != SSNMCongestionExpiredEvent || len(event.Expired) == 0 || len(event.Expired) > 256 || event.Partition != (SSNMPartition{}) || event.Epoch != 0 || len(event.Updated) != 0 || event.PeerCongestion != nil || !event.ApplyExpiry(&replica) {
			t.Fatalf("expiry replay failed or replayed retained values: %+v", event)
		}
		count += len(event.Expired)
	}
	after := endpoint.SSNMKnowledge()
	if replica.Revision != initialRevision || count != DefaultMaxSSNMStateStoreRecords || !reflect.DeepEqual(replica.Partitions, after.Partitions) || !reflect.DeepEqual(replica.PeerCongestion, after.PeerCongestion) {
		t.Fatal("whole-pass expiry replay differs from final snapshot", count)
	}
	for _, record := range after.PeerCongestion {
		if record.Freshness != CongestionExpired {
			t.Fatal("whole-pass expiry missed a record")
		}
	}
}

func TestReceivedExpiryChunksReplaySnapshot(t *testing.T) {
	e := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), nil)
	now := time.Now()
	e.ssnm.now = func() time.Time { return now }
	e.ssnm.validity = time.Second
	a := attachSSNMAssociation(t, e, SGPIdentity{"sg-a", "sgp-a1"}, 7, 1)
	points := make([]uint32, 1024)
	for i := range points {
		points[i] = uint32(i)
	}
	sendSCON(t, a, 7, 1, congestionLevel(2), points...)
	replica, sub, err := e.SubscribeSSNM()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close() }()
	now = now.Add(time.Second)
	e.runCongestionAutomation(now)
	events, bytes, lost := outcomeProbeEvents(sub)
	if lost || len(events) != 4 || bytes != 4*512+1024*56 {
		t.Fatal("one received report did not produce four compact chunks", len(events), bytes, lost)
	}
	for _, event := range events {
		if !event.ApplyExpiry(&replica) {
			t.Fatal("received expiry did not match its original source snapshot")
		}
	}
	if after := e.SSNMKnowledge(); !reflect.DeepEqual(replica.Partitions, after.Partitions) {
		t.Fatal("received expiry did not replay scalar and source freshness")
	}
	assertSSNMScopeAccounting(t, e)
}

func TestExpiryOverflowStillRequiresResync(t *testing.T) {
	e := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), &SSNMStateConfig{SubscriptionQueueSize: 1})
	now := time.Now()
	e.ssnm.now = func() time.Time { return now }
	e.ssnm.validity = time.Second
	a := attachSSNMAssociation(t, e, SGPIdentity{"sg-a", "sgp-a1"}, 7, 1)
	points := make([]uint32, 1024)
	for i := range points {
		points[i] = uint32(i)
	}
	sendSCON(t, a, 7, 1, congestionLevel(2), points...)
	_, sub, err := e.SubscribeSSNM()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close() }()
	now = now.Add(time.Second)
	e.runCongestionAutomation(now)
	events, _, lost := outcomeProbeEvents(sub)
	if !lost || len(events) != 1 || len(events[0].Expired) != 256 {
		t.Fatal("real queue overflow did not preserve the loss barrier", len(events), lost)
	}
	_ = peerRetentionNext(t, sub)
	if event := peerRetentionNext(t, sub); !event.ContinuityLost || event.Kind != SSNMContinuityLostEvent {
		t.Fatal("expiry overflow omitted continuity loss", event)
	}
	sendSCON(t, a, 7, 1, congestionLevel(0), 1)
	if queued, _, stillLost := outcomeProbeEvents(sub); !stillLost || len(queued) != 0 {
		t.Fatal("loss barrier admitted a new report before resync")
	}
	if _, err := sub.Resync(); err != nil {
		t.Fatal(err)
	}
	if _, _, stillLost := outcomeProbeEvents(sub); stillLost {
		t.Fatal("resync did not restore continuity")
	}
}

func TestUnmaskedIPSPExpiryReportsChangedDestinationsOnce(t *testing.T) {
	const destinations = 512
	key := routingContextASKey(1)
	config := make([]IPSPDestinationConfig, destinations)
	points := make([]uint32, destinations)
	for i := range config {
		points[i] = uint32(i)
		config[i] = IPSPDestinationConfig{Destination: MTPDestination{MTPRoute: MTPRouteID(fmt.Sprintf("mapped-%d", i)), PointCode: points[i]}, AS: key}
	}
	e, err := NewEndpoint(nationalCongestionEndpointConfig(EndpointConfig{Role: RoleIPSP, IPSPDestinations: config}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	// Set before admitting any association; the normal stream's default cap
	// would hide burst cardinality behind its independent overflow marker.
	e.ipspMTP.stream.indications = make(chan *MTPIndication, 4*destinations)
	now := time.Now()
	e.ssnm.now = func() time.Time { return now }
	e.ssnm.validity = time.Second
	a, _ := newTestConnWithContexts(t, StateASPActive, RoleIPSP, 1)
	if !e.trackAssociation(a) {
		t.Fatal("track")
	}
	t.Cleanup(func() { _ = a.Close() })
	drain := func() int {
		t.Helper()
		count := 0
		for {
			select {
			case indication := <-e.MTPIndications():
				if indication == nil || indication.ResyncRequired {
					t.Fatal("MTP stream lost the bounded burst")
				}
				count++
			default:
				return count
			}
		}
	}
	_ = drain()
	if err := a.handleSignallingCongestion(messages.NewSignallingCongestion(nil, params.NewRoutingContext(1), params.NewAffectedPointCode(points...), nil, params.NewCongestionIndications(2), nil)); err != nil {
		t.Fatal(err)
	}
	received := drain()
	if received == 0 {
		t.Fatal("receipt fixture produced no MTP indications")
	}
	now = now.Add(time.Second)
	e.runCongestionAutomation(now)
	if expired := drain(); expired != destinations {
		t.Fatal("unmasked expiry did not report every changed destination once", expired, destinations)
	}
	for _, mapping := range config {
		status, found := e.MTPDestinationStatus(mapping.Destination)
		if !found || status.Congested || !status.ReceivedCongestionUnknown {
			t.Fatal("expiry did not commit mapped unknown status", status)
		}
	}
}
