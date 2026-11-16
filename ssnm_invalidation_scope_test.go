package m3ua

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// Different ASes share one standalone partition, but never one loss scope.
func TestOversizedSSNMStandalonePreservesOtherAS(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, nil, &SSNMStateConfig{MaxAffectedPointCodes: 1})
	peer, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1, 2)
	setInventoryNetworkAppearance(&peer.cfg.ApplicationServers, params.NewNetworkAppearance(7))
	peer.noteRoutingContextsAcked(params.NewRoutingContext(1, 2))
	if !endpoint.trackAssociation(peer) {
		t.Fatal("track association")
	}
	t.Cleanup(func() { _ = peer.Close() })
	sendDUNA(t, peer, 7, 1, 101)
	sendDRST(t, peer, 7, 2, 102)
	level := uint8(2)
	sendSCON(t, peer, 7, 2, &level, 102)
	partition := SSNMPartition{Kind: SSNMStandalonePartition, Association: peer.ID()}
	before := ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(), partition)
	expected := ssnmDestination(t, before, 102, 0)
	err := peer.handleDestinationUnavailable(messages.NewDestinationUnavailable(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCode(201, 202), nil))
	if !errors.Is(err, ErrSSNMOversizedReport) {
		t.Fatalf("oversized: %v", err)
	}
	after := ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(), partition)
	if len(after.Destinations) != 1 || !reflect.DeepEqual(after.Destinations[0], expected) {
		t.Fatalf("RC1 refusal deleted unrelated RC2 availability/restriction/congestion: want %+v, got %+v", expected, after.Destinations)
	}
}

func TestOversizedDAUDPreservesDestinationKnowledge(t *testing.T) {
	endpoint, peer, _, _, _ := peerRefusalFixture(t)
	partition := SSNMPartition{Kind: SSNMStandalonePartition, Association: peer.ID()}
	for _, kind := range []SSNMReportKind{SSNMDestinationUnavailableReport, SSNMSignallingCongestionReport} {
		if err := endpoint.ssnm.apply(SSNMReport{Kind: kind, Source: SSNMLocalReport, Association: peer.ID(), Partition: partition,
			Scope: WireScope{RoutingContextSet: true, RoutingContexts: []uint32{1}}, Destinations: []PointCodeRange{{PointCode: 101}}}); err != nil {
			t.Fatal(err)
		}
	}
	before, sub, err := endpoint.SubscribeSSNM()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close() }()
	err = peer.handleDestinationStateAudit(messages.NewDestinationStateAudit(nil, params.NewRoutingContext(1), params.NewAffectedPointCode(101, 102), nil))
	if !errors.Is(err, ErrSSNMOversizedReport) {
		t.Fatal(err)
	}
	after := endpoint.SSNMKnowledge()
	if !reflect.DeepEqual(before.Partitions, after.Partitions) || !reflect.DeepEqual(before.PeerCongestion, after.PeerCongestion) || after.PartitionsInvalidated != before.PartitionsInvalidated {
		t.Fatalf("oversized query discarded knowledge: before %+v, after %+v", before, after)
	}
	if after.Revision != before.Revision+1 || after.ReportsRefused != before.ReportsRefused+1 {
		t.Fatal("query refusal must publish only resource loss")
	}
}

func assertSSNMScopeAccounting(t *testing.T, endpoint *Endpoint) {
	t.Helper()
	snapshot := endpoint.SSNMKnowledge()
	records, bytes := len(snapshot.PeerCongestion), len(snapshot.Partitions)*ssnmPartitionBaseBytes
	for _, record := range snapshot.PeerCongestion {
		bytes += peerCongestionRecordBytes(record)
	}
	for _, partition := range snapshot.Partitions {
		for _, record := range partition.Destinations {
			for _, source := range record.AvailabilitySources {
				records++
				bytes += 144 + 4*len(source.Scope.RoutingContexts)
			}
			for _, source := range record.CongestionSources {
				records++
				bytes += 144 + 4*len(source.Scope.RoutingContexts)
			}
			if record.AvailabilitySet {
				records++
				bytes += ssnmScopeBytes(record.Availability.Scope)
			}
			if record.CongestionSet {
				records++
				bytes += ssnmScopeBytes(record.Congestion.Scope)
			}
		}
	}
	endpoint.ssnm.mu.Lock()
	defer endpoint.ssnm.mu.Unlock()
	for _, state := range endpoint.ssnm.partitions {
		for _, r := range state.availability {
			bytes += ssnmResolvedASBytes * len(r.resolved)
		}
		for _, r := range state.congestion {
			bytes += ssnmResolvedASBytes * len(r.resolved)
		}
	}
	for _, job := range endpoint.ssnm.audits {
		bytes += 256 + len(job.info) + 8*len(job.scope.RoutingContexts) + 144*len(job.bindings) + 64*len(job.partitions)
	}
	if endpoint.ssnm.records != records || endpoint.ssnm.bytes != bytes {
		t.Fatalf("retained accounting: got %d/%d, want %d/%d", endpoint.ssnm.records, endpoint.ssnm.bytes, records, bytes)
	}
	perPeer := 0
	for _, count := range endpoint.ssnm.peerRecords {
		if count < 0 {
			t.Fatal("negative per-peer count")
		}
		perPeer += count
	}
	if perPeer != records {
		t.Fatalf("per-peer records %d, want %d", perPeer, records)
	}
	for _, state := range endpoint.ssnm.partitions {
		charged := ssnmPartitionBaseBytes
		for _, r := range state.availability {
			charged += ssnmScopeBytes(r.Scope, r.resolved)
		}
		for _, r := range state.congestion {
			charged += ssnmScopeBytes(r.Scope, r.resolved)
		}
		for _, source := range state.receivedAvailability {
			charged += 144 + 4*len(source.Scope.RoutingContexts)
		}
		for _, source := range state.receivedCongestion {
			charged += 144 + 4*len(source.Scope.RoutingContexts)
		}
		for _, r := range state.peerCongestion {
			charged += peerCongestionRecordBytes(r)
		}
		if state.bytes != charged {
			t.Fatalf("partition bytes %d, want %d", state.bytes, charged)
		}
	}
}

func ssnmStandaloneScopeFixture(t *testing.T, contexts ...uint32) (*Endpoint, *Association, SSNMPartition) {
	t.Helper()
	endpoint := newSSNMStateEndpoint(t, nil, &SSNMStateConfig{MaxAffectedPointCodes: 1})
	peer, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, contexts...)
	setInventoryNetworkAppearance(&peer.cfg.ApplicationServers, params.NewNetworkAppearance(7))
	peer.noteRoutingContextsAcked(params.NewRoutingContext(contexts...))
	if !endpoint.trackAssociation(peer) {
		t.Fatal("track")
	}
	t.Cleanup(func() { _ = peer.Close() })
	return endpoint, peer, SSNMPartition{Kind: SSNMStandalonePartition, Association: peer.ID()}
}

func TestOversizedSSNMScopeMatrix(t *testing.T) {
	tests := []struct {
		name     string
		wire     WireScope
		contexts []uint32
		keep     []uint32
	}{
		{"RC1", WireScope{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContextSet: true, RoutingContexts: []uint32{1}}, []uint32{1, 2}, []uint32{103, 104}},
		{"duplicate RC", WireScope{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContextSet: true, RoutingContexts: []uint32{1, 1}}, []uint32{1, 2}, []uint32{103, 104}},
		{"RC list", WireScope{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContextSet: true, RoutingContexts: []uint32{2, 1, 2}}, []uint32{1, 2}, []uint32{104}},
		{"inherited NA", WireScope{RoutingContextSet: true, RoutingContexts: []uint32{1}}, []uint32{1, 2}, []uint32{103, 104}},
		{"explicit zero", WireScope{NetworkAppearanceSet: true, RoutingContextSet: true, RoutingContexts: []uint32{1}}, []uint32{1, 2}, []uint32{101, 102, 103, 104, 105}},
		{"inherited single RC", WireScope{}, []uint32{1}, []uint32{103, 104}},
		{"unresolved multi RC", WireScope{}, []uint32{1, 2}, nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			endpoint, peer, partition := ssnmStandaloneScopeFixture(t, test.contexts...)
			scopes := []WireScope{
				{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContextSet: true, RoutingContexts: []uint32{1}},
				{RoutingContextSet: true, RoutingContexts: []uint32{1}},
				{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContextSet: true, RoutingContexts: []uint32{2}},
				{NetworkAppearance: 8, NetworkAppearanceSet: true, RoutingContextSet: true, RoutingContexts: []uint32{1}},
				{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContextSet: true, RoutingContexts: []uint32{2, 1, 2}},
			}
			for i, scope := range scopes {
				for _, kind := range []SSNMReportKind{SSNMDestinationRestrictedReport, SSNMSignallingCongestionReport} {
					if err := endpoint.ssnm.apply(SSNMReport{Kind: kind, Source: SSNMPeerReport, Association: peer.ID(), Partition: partition, Scope: scope, Destinations: []PointCodeRange{{PointCode: uint32(101 + i)}}}, peer.ssnmASKeys(scope)); err != nil {
						t.Fatal(err)
					}
				}
			}
			before := ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(), partition)
			if err := peer.refuseOversizedSSNM(test.wire, 2, 1, SSNMDestinationUnavailableReport); !errors.Is(err, ErrSSNMOversizedReport) {
				t.Fatal(err)
			}
			after := ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(), partition)
			if len(after.Destinations) != len(test.keep) {
				t.Fatalf("scope removed wrong destinations: %+v", after.Destinations)
			}
			for i, pc := range test.keep {
				expected := ssnmDestination(t, before, pc, 0)
				if !reflect.DeepEqual(after.Destinations[i], expected) {
					t.Fatalf("unrelated record changed: %+v", after.Destinations[i])
				}
			}
			assertSSNMScopeAccounting(t, endpoint)
		})
	}
}

func TestOversizedSSNMInvalidationDeltasAndResync(t *testing.T) {
	endpoint, peer, partition := ssnmStandaloneScopeFixture(t, 1, 2)
	sendDUNA(t, peer, 7, 2, 123)
	level := uint8(2)
	sendSCON(t, peer, 7, 1, &level, 123)
	sendDRST(t, peer, 7, 1, 124)
	before, sub, err := endpoint.SubscribeSSNM()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close() }()
	replica := newSSNMReplica(before)
	if err := peer.handleDestinationUnavailable(messages.NewDestinationUnavailable(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCode(201, 202), nil)); !errors.Is(err, ErrSSNMOversizedReport) {
		t.Fatal(err)
	}
	after := endpoint.SSNMKnowledge()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	loss, err := sub.Next(ctx)
	if err != nil || loss.Kind != SSNMResourceLossEvent {
		t.Fatalf("resource loss first: %+v %v", loss, err)
	}
	event, err := sub.Next(ctx)
	if err != nil || event.Kind != SSNMDestinationsInvalidatedEvent || len(event.Invalidated) != 2 || event.ReportSet {
		t.Fatalf("exact invalidation: %+v %v", event, err)
	}
	if event.Invalidated[0].AvailabilityRemoved || !event.Invalidated[0].CongestionRemoved || !event.Invalidated[1].AvailabilityRemoved || event.Invalidated[1].CongestionRemoved {
		t.Fatalf("wrong removed dimensions/scope: %+v", event.Invalidated)
	}
	for _, delta := range []SSNMEvent{loss, event} {
		if err := replica.apply(delta); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(replica.knowledge(), canonicalSSNMTestKnowledge(after)) {
		t.Fatal("invalidation replay differs from surviving store")
	}
	resync, err := sub.Resync()
	if err != nil || !reflect.DeepEqual(resync, after) {
		t.Fatalf("resync lost survivors: %+v %v", resync, err)
	}
	surviving := ssnmPartitionKnowledge(t, resync, partition).Destinations
	if len(surviving) != 1 || !surviving[0].AvailabilitySet || surviving[0].CongestionSet {
		t.Fatalf("mixed dimension survivor: %+v", surviving)
	}
	assertSSNMScopeAccounting(t, endpoint)
}

func TestOversizedSSNMCanonicalScopeUsesIdentityAndNetwork(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), &SSNMStateConfig{MaxAffectedPointCodes: 1})
	first := attachSSNMAssociation(t, endpoint, SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}, 7, 1)
	sibling := attachSSNMAssociation(t, endpoint, SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a2"}, 7, 2)
	partition := canonicalSSNMPartition("sg-a", "as-core")
	sendDUNA(t, sibling, 7, 2, 101)
	// A different registered appearance of the same canonical AS must survive.
	sibling.addDynamicASKey(*staticASKey(8, 2), RoutingKey{}, false)
	if err := sibling.noteCanonicalRemoteAS(2, "as-core"); err != nil {
		t.Fatal(err)
	}
	sendDUNA(t, sibling, 8, 2, 102)
	before := ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(), partition)
	if err := first.handleDestinationUnavailable(messages.NewDestinationUnavailable(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCode(201, 202), nil)); !errors.Is(err, ErrSSNMOversizedReport) {
		t.Fatal(err)
	}
	after := ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(), partition)
	if len(after.Destinations) != 1 || !reflect.DeepEqual(after.Destinations[0], ssnmDestination(t, before, 102, 0)) {
		t.Fatalf("canonical refusal ignored AS identity/NA: %+v", after)
	}
	assertSSNMScopeAccounting(t, endpoint)
}

func TestOversizedSSNMWildcardNetworkScope(t *testing.T) {
	for _, omitted := range []bool{false, true} {
		t.Run(map[bool]string{false: "concrete", true: "all appearances"}[omitted], func(t *testing.T) {
			endpoint, peer, partition := ssnmStandaloneScopeFixture(t, 1, 2)
			wildcard := ASKey{RoutingContext: 1, RoutingContextSet: true}
			peer.addDynamicASKey(wildcard, RoutingKey{}, false)
			for i, scope := range []WireScope{
				{RoutingContextSet: true, RoutingContexts: []uint32{1}},
				{NetworkAppearanceSet: true, RoutingContextSet: true, RoutingContexts: []uint32{1}},
				{NetworkAppearance: 8, NetworkAppearanceSet: true, RoutingContextSet: true, RoutingContexts: []uint32{1}},
				{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContextSet: true, RoutingContexts: []uint32{2}},
			} {
				if err := peer.publishSSNMReport(SSNMReport{Kind: SSNMDestinationUnavailableReport, Source: SSNMPeerReport, Scope: scope, Destinations: []PointCodeRange{{PointCode: uint32(101 + i)}}}); err != nil {
					t.Fatal(err)
				}
			}
			before := ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(), partition)
			scope := WireScope{NetworkAppearanceSet: !omitted, RoutingContextSet: true, RoutingContexts: []uint32{1}}
			if err := peer.refuseOversizedSSNM(scope, 2, 1, SSNMDestinationUnavailableReport); !errors.Is(err, ErrSSNMOversizedReport) {
				t.Fatal(err)
			}
			after := ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(), partition)
			keep := []uint32{103, 104}
			if omitted {
				keep = []uint32{104}
			}
			if len(after.Destinations) != len(keep) {
				t.Fatalf("wildcard overlap: %+v", after.Destinations)
			}
			for i, pc := range keep {
				if !reflect.DeepEqual(after.Destinations[i], ssnmDestination(t, before, pc, 0)) {
					t.Fatal("wildcard refusal changed survivor")
				}
			}
			assertSSNMScopeAccounting(t, endpoint)
		})
	}
}

func TestSSNMInvalidationPayloadOwnershipAndBudget(t *testing.T) {
	event := SSNMEvent{Kind: SSNMDestinationsInvalidatedEvent, Epoch: 1, Invalidated: []SSNMDestinationInvalidation{{Destination: PointCodeRange{PointCode: 101}, AvailabilityRemoved: true, CongestionRemoved: true}}}
	want := ssnmEventBaseBytes + ssnmEventInvalidationBytes
	if bytes, ok := ssnmEventAccountedBytes(event, want); !ok || bytes != want {
		t.Fatalf("invalidation charge %d/%v, want %d", bytes, ok, want)
	}
	if _, ok := ssnmEventAccountedBytes(event, want-1); ok {
		t.Fatal("invalidation exceeds queue byte budget")
	}
	clone := event.clone()
	clone.Invalidated[0].AvailabilityRemoved = false
	if !event.Invalidated[0].AvailabilityRemoved {
		t.Fatal("invalidation entries share caller storage")
	}

}

func TestSSNMScopedInvalidationOverflowResync(t *testing.T) {
	endpoint, peer, partition := ssnmStandaloneScopeFixture(t, 1, 2)
	endpoint.ssnm.limits.SubscriptionQueueBytes = 1024
	for _, pc := range []uint32{101, 102, 103, 104} {
		sendDUNA(t, peer, 7, 1, pc)
	}
	sendDRST(t, peer, 7, 2, 105)
	before, sub, err := endpoint.SubscribeSSNM()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close() }()
	if err := peer.handleDestinationUnavailable(messages.NewDestinationUnavailable(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCode(201, 202), nil)); !errors.Is(err, ErrSSNMOversizedReport) {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resource, err := sub.Next(ctx)
	if err != nil || resource.Kind != SSNMResourceLossEvent {
		t.Fatalf("queued resource loss: %+v %v", resource, err)
	}
	event, err := sub.Next(ctx)
	if err != nil || event.Kind != SSNMContinuityLostEvent || !event.ContinuityLost {
		t.Fatalf("invalidation payload bypassed loss barrier: %+v %v", event, err)
	}
	snapshot, err := sub.Resync()
	if err != nil || !reflect.DeepEqual(snapshot, endpoint.SSNMKnowledge()) {
		t.Fatal("overflow Resync differs from store")
	}
	survivors := ssnmPartitionKnowledge(t, snapshot, partition).Destinations
	if len(survivors) != 1 || !reflect.DeepEqual(survivors[0], ssnmDestination(t, ssnmPartitionKnowledge(t, before, partition), 105, 0)) {
		t.Fatalf("overflow erased RC2 survivor: %+v", survivors)
	}
	sub.mu.Lock()
	if sub.queuedBytes != 0 || len(sub.queue) != 0 {
		t.Error("Resync retained queue charges")
	}
	sub.mu.Unlock()
	assertSSNMScopeAccounting(t, endpoint)
}

func TestOversizedSSNMContextlessScope(t *testing.T) {
	endpoint, peer, partition := ssnmStandaloneScopeFixture(t)
	// Contextless AS knowledge and an unrelated historical numeric scope share
	// the partition; an omitted RC resolves to the contextless AS only.
	for _, scope := range []WireScope{{NetworkAppearance: 7, NetworkAppearanceSet: true}, {NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContextSet: true, RoutingContexts: []uint32{2}}} {
		pc := uint32(101)
		if scope.RoutingContextSet {
			pc = 102
		}
		if err := peer.publishSSNMReport(SSNMReport{Kind: SSNMDestinationRestrictedReport, Source: SSNMPeerReport, Scope: scope, Destinations: []PointCodeRange{{PointCode: pc}}}); err != nil {
			t.Fatal(err)
		}
	}
	before := ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(), partition)
	if err := peer.refuseOversizedSSNM(WireScope{}, 2, 1, SSNMDestinationUnavailableReport); !errors.Is(err, ErrSSNMOversizedReport) {
		t.Fatal(err)
	}
	after := ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(), partition)
	if len(after.Destinations) != 1 || !reflect.DeepEqual(after.Destinations[0], ssnmDestination(t, before, 102, 0)) {
		t.Fatalf("omitted RC lost contextless AS identity: %+v", after)
	}
}

func TestOversizedSSNMResolvedNetworkPreservesHistoricalScope(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), &SSNMStateConfig{MaxAffectedPointCodes: 1})
	peer := attachSSNMAssociation(t, endpoint, SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}, 7, 1)
	partition := canonicalSSNMPartition("sg-a", "as-core")
	// The partition can outlive the contributing association's configuration.
	// Its exact wire NA still proves disjointness when the new report inherits NA.
	endpoint.ssnm.registerSource(99)
	for i, na := range []uint32{7, 8} {
		if err := endpoint.ssnm.apply(SSNMReport{Kind: SSNMDestinationRestrictedReport, Source: SSNMPeerReport, Association: 99, Partition: partition, Scope: WireScope{NetworkAppearance: na, NetworkAppearanceSet: true}, Destinations: []PointCodeRange{{PointCode: uint32(101 + i)}}}); err != nil {
			t.Fatal(err)
		}
	}
	before := ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(), partition)
	if err := peer.handleDestinationUnavailable(messages.NewDestinationUnavailable(nil, params.NewRoutingContext(1), params.NewAffectedPointCode(201, 202), nil)); !errors.Is(err, ErrSSNMOversizedReport) {
		t.Fatal(err)
	}
	after := ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(), partition)
	if len(after.Destinations) != 1 || !reflect.DeepEqual(after.Destinations[0], ssnmDestination(t, before, 102, 0)) {
		t.Fatalf("inherited NA crossed exact historical network: %+v", after.Destinations)
	}
	assertSSNMScopeAccounting(t, endpoint)
}
