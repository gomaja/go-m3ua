package m3ua

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestSSNMInvalidationUsesAdmissionScopeAfterRCReuse(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), &SSNMStateConfig{MaxAffectedPointCodes: 1})
	first := attachSSNMAssociation(t, endpoint, SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}, 7, 1)
	sibling := attachSSNMAssociation(t, endpoint, SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a2"}, 7, 2)
	sibling.addDynamicASKey(*staticASKey(7, 2), RoutingKey{}, false)
	sibling.noteCanonicalRemoteAS(2, "as-core")
	if err := sibling.handleDestinationAvailable(messages.NewDestinationAvailable(nil, params.NewRoutingContext(2), params.NewAffectedPointCode(123), nil)); err != nil {
		t.Fatal(err)
	}
	core := canonicalSSNMPartition("sg-a", "as-core")
	original := ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(), core)
	if len(original.Destinations) != 1 || original.Destinations[0].Availability.Scope.NetworkAppearanceSet {
		t.Fatal("omitted-NA report was not retained")
	}
	// The other SGP keeps the old partition's authority through deregistration.
	endpoint.ssnm.retire(core, sibling.ID())
	sibling.forgetCanonicalRemoteAS(2)
	sibling.removeDynamicASKey(2, false)
	sibling.addDynamicASKey(*staticASKey(8, 2), RoutingKey{}, false)
	sibling.noteCanonicalRemoteAS(2, "as-edge")
	if err := sibling.handleDestinationAvailable(messages.NewDestinationAvailable(nil, params.NewRoutingContext(2), params.NewAffectedPointCode(456), nil)); err != nil {
		t.Fatal(err)
	}
	edge := canonicalSSNMPartition("sg-a", "as-edge")
	expected := ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(), edge)
	if err := first.handleDestinationUnavailable(messages.NewDestinationUnavailable(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCode(123, 124), nil)); !errors.Is(err, ErrSSNMOversizedReport) {
		t.Fatal(err)
	}
	after := endpoint.SSNMKnowledge()
	if len(ssnmPartitionKnowledge(t, after, core).Destinations) != 0 {
		t.Fatal("reused RC reinterpreted historical NA and kept contradicted DAVA")
	}
	if !reflect.DeepEqual(ssnmPartitionKnowledge(t, after, edge), expected) {
		t.Fatal("old-AS refusal erased replacement AS")
	}
	assertSSNMScopeAccounting(t, endpoint)
}

func TestSSNMInvalidationKindRequiresMigration(t *testing.T) {
	file, err := parser.ParseFile(token.NewFileSet(), "ssnm_subscription.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	ast.Inspect(file, func(node ast.Node) bool {
		if spec, ok := node.(*ast.ValueSpec); ok {
			for _, name := range spec.Names {
				if name.Name == "SSNMPartitionInvalidatedEvent" {
					t.Error("old whole-partition event identifier still exported; stale consumer still compiles")
				}
			}
		}
		return true
	})
}

func TestSSNMDefaultQueueAcceptsWorstInvalidation(t *testing.T) {
	for _, survivor := range []bool{false, true} {
		t.Run(map[bool]string{false: "all removed", true: "unrelated survivor"}[survivor], func(t *testing.T) {
			endpoint := newSSNMStateEndpoint(t, nil, nil)
			partition := SSNMPartition{Kind: SSNMStandalonePartition, Association: 1}
			endpoint.ssnm.registerSource(1)
			if err := endpoint.ssnm.bind(partition, 1, false); err != nil {
				t.Fatal(err)
			}
			contexts := make([]uint32, 128)
			for i := range contexts {
				contexts[i] = uint32(i + 1)
			}
			for i := 0; i < DefaultMaxSSNMPartitionRecords; i++ {
				scope := WireScope{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContextSet: true, RoutingContexts: contexts}
				if survivor && i == DefaultMaxSSNMPartitionRecords-1 {
					scope.RoutingContexts = []uint32{999}
				}
				if err := endpoint.ssnm.apply(SSNMReport{Kind: SSNMDestinationUnavailableReport, Source: SSNMPeerReport, Association: 1, Partition: partition, Scope: scope, Destinations: []PointCodeRange{{PointCode: uint32(i + 1)}}}); err != nil {
					t.Fatalf("seed %d: %v", i, err)
				}
			}
			before, sub, err := endpoint.SubscribeSSNM()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = sub.Close() }()
			if len(ssnmPartitionKnowledge(t, before, partition).Destinations) != 2048 {
				t.Fatal("default fixture not full")
			}
			refusal := ssnmDestinationRefusal{resolved: true, keys: []ASKey{{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true}}}
			if err := endpoint.ssnm.oversized([]SSNMPartition{partition}, 1025, 1024, 1, refusal); !errors.Is(err, ErrSSNMOversizedReport) {
				t.Fatal(err)
			}
			after := endpoint.SSNMKnowledge()
			replica := newSSNMReplica(before)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			for replica.revision < after.Revision {
				event, err := sub.Next(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if event.Kind == SSNMContinuityLostEvent {
					t.Fatal("single invalidation exceeds the default empty queue")
				}
				if _, fits := ssnmEventAccountedBytes(event, DefaultSSNMSubscriptionQueueBytes); !fits {
					t.Fatal("single publication exceeds default queue bound")
				}
				if err := replica.apply(event); err != nil {
					t.Fatal(err)
				}
			}
			if !reflect.DeepEqual(replica.knowledge(), canonicalSSNMTestKnowledge(after)) {
				t.Fatal("bounded invalidation replay differs from store")
			}
			remaining := len(ssnmPartitionKnowledge(t, after, partition).Destinations)
			if remaining != map[bool]int{false: 0, true: 1}[survivor] {
				t.Fatalf("surviving records %d", remaining)
			}
		})
	}
}

func TestSSNMInvalidationPreservesOriginalDisjointNetwork(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), &SSNMStateConfig{MaxAffectedPointCodes: 1})
	first := attachSSNMAssociation(t, endpoint, SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}, 7, 1)
	sibling := attachSSNMAssociation(t, endpoint, SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a2"}, 7, 2)
	sibling.addDynamicASKey(*staticASKey(8, 2), RoutingKey{}, false)
	sibling.noteCanonicalRemoteAS(2, "as-core")
	if err := sibling.handleDestinationAvailable(messages.NewDestinationAvailable(nil, params.NewRoutingContext(2), params.NewAffectedPointCode(123), nil)); err != nil {
		t.Fatal(err)
	}
	partition := canonicalSSNMPartition("sg-a", "as-core")
	before := ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(), partition)
	endpoint.ssnm.retire(partition, sibling.ID())
	sibling.removeDynamicASKey(2, false)
	sibling.addDynamicASKey(*staticASKey(7, 2), RoutingKey{}, false)
	sibling.noteCanonicalRemoteAS(2, "as-edge")
	if err := first.handleDestinationUnavailable(messages.NewDestinationUnavailable(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCode(123, 124), nil)); !errors.Is(err, ErrSSNMOversizedReport) {
		t.Fatal(err)
	}
	after := ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(), partition)
	if !reflect.DeepEqual(before.Destinations, after.Destinations) {
		t.Fatal("RC reuse erased unrelated historical network")
	}
}

func TestSSNMInvalidationResolutionOwnsAdmissionKeys(t *testing.T) {
	endpoint, peer, partition := ssnmStandaloneScopeFixture(t, 1, 2)
	keys := []ASKey{*staticASKey(7, 1)}
	if err := endpoint.ssnm.apply(SSNMReport{Kind: SSNMDestinationAvailableReport, Source: SSNMPeerReport, Association: peer.ID(), Partition: partition, Scope: WireScope{RoutingContextSet: true, RoutingContexts: []uint32{1}}, Destinations: []PointCodeRange{{PointCode: 123}}}, keys); err != nil {
		t.Fatal(err)
	}
	keys[0].NetworkAppearance = 8
	if err := peer.refuseOversizedSSNM(WireScope{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContextSet: true, RoutingContexts: []uint32{1}}, 2, 1, SSNMDestinationUnavailableReport); !errors.Is(err, ErrSSNMOversizedReport) {
		t.Fatal(err)
	}
	if len(ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(), partition).Destinations) != 0 {
		t.Fatal("caller mutation changed retained admission scope")
	}
	assertSSNMScopeAccounting(t, endpoint)
}

func TestSSNMCompactInvalidationBoundsAndApply(t *testing.T) {
	for _, hugeNames := range []bool{false, true} {
		t.Run(map[bool]string{false: "custom record cap", true: "unbounded names"}[hugeNames], func(t *testing.T) {
			const records = 4097
			endpoint := newSSNMStateEndpoint(t, nil, &SSNMStateConfig{MaxRecordsPerPartition: records})
			partition := SSNMPartition{Kind: SSNMStandalonePartition, Association: 1}
			if hugeNames {
				partition = canonicalSSNMPartition(SignallingGatewayID(strings.Repeat("g", DefaultSSNMSubscriptionQueueBytes+1)), RemoteASID(strings.Repeat("a", DefaultSSNMSubscriptionQueueBytes+1)))
			}
			endpoint.ssnm.registerSource(1)
			if err := endpoint.ssnm.bind(partition, 1, false); err != nil {
				t.Fatal(err)
			}
			destinations := make([]PointCodeRange, records)
			for i := range destinations {
				destinations[i] = PointCodeRange{PointCode: uint32(i + 1)}
			}
			if err := endpoint.ssnm.apply(SSNMReport{Kind: SSNMDestinationRestrictedReport, Source: SSNMPeerReport, Association: 1, Partition: partition, Destinations: destinations}); err != nil {
				t.Fatal(err)
			}
			before, sub, err := endpoint.SubscribeSSNM()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = sub.Close() }()
			target := ssnmPartitionKnowledge(t, before, partition)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := endpoint.ssnm.oversized([]SSNMPartition{partition}, 1025, 1024, 1, ssnmDestinationRefusal{}); !errors.Is(err, ErrSSNMOversizedReport) {
				t.Fatal(err)
			}
			resource, err := sub.Next(ctx)
			if err != nil || resource.Kind != SSNMResourceLossEvent {
				t.Fatal("missing resource event", err)
			}
			revision := resource.Revision
			events, removed := 0, 0
			for len(target.Destinations) > 0 {
				event, err := sub.Next(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if event.Kind != SSNMDestinationsInvalidatedEvent || event.Partition != (SSNMPartition{}) {
					t.Fatal("invalidation leaked variable partition identity")
				}
				if event.Revision != revision+1 {
					t.Fatal("chunk revisions not contiguous")
				}
				revision = event.Revision
				bytes, fits := ssnmEventAccountedBytes(event, DefaultSSNMSubscriptionQueueBytes)
				if !fits || bytes > 33440 || len(event.Invalidated) > 2048 {
					t.Fatalf("invalidation publication exceeds 33440-byte bound: %d/%v", bytes, fits)
				}
				other := target
				other.Epoch++
				if event.ApplyInvalidation(&other) {
					t.Fatal("invalidation applied to a different epoch")
				}
				if !event.ApplyInvalidation(&target) {
					t.Fatal("invalidation did not match its partition epoch")
				}
				if !event.ApplyInvalidation(&target) {
					t.Fatal("invalidation replay not idempotent")
				}
				events++
				removed += len(event.Invalidated)
			}
			after := endpoint.SSNMKnowledge()
			if events != 3 || removed != records || after.PartitionsInvalidated != 3 {
				t.Fatalf("bounded chunk counts: %d/%d/%d", events, removed, after.PartitionsInvalidated)
			}
			if !reflect.DeepEqual(target, ssnmPartitionKnowledge(t, after, partition)) {
				t.Fatal("apply helper disagrees with surviving store")
			}
		})
	}
}

func TestSSNMCompactInvalidationApplyPreservesOtherDimensions(t *testing.T) {
	target := SSNMPartitionKnowledge{Epoch: 17, Bindings: []SSNMBinding{{Association: 1}}, Destinations: []SSNMDestinationKnowledge{
		{Destination: PointCodeRange{PointCode: 101}, AvailabilitySet: true, Availability: SSNMAvailability{Revision: 1}, CongestionSet: true, Congestion: SSNMCongestion{Revision: 2}},
		{Destination: PointCodeRange{PointCode: 102}, AvailabilitySet: true, Availability: SSNMAvailability{Revision: 3}},
		{Destination: PointCodeRange{PointCode: 103}, AvailabilitySet: true, Availability: SSNMAvailability{Revision: 4}},
	}}
	event := SSNMEvent{Kind: SSNMDestinationsInvalidatedEvent, Epoch: 17, Invalidated: []SSNMDestinationInvalidation{{Destination: PointCodeRange{PointCode: 101}, CongestionRemoved: true}, {Destination: PointCodeRange{PointCode: 102}, AvailabilityRemoved: true}}}
	if (SSNMEvent{Epoch: 17}).ApplyInvalidation(&target) || event.ApplyInvalidation(nil) {
		t.Fatal("invalid apply target accepted")
	}
	if !event.ApplyInvalidation(&target) || len(target.Destinations) != 2 || !target.Destinations[0].AvailabilitySet || target.Destinations[0].CongestionSet || target.Destinations[0].Availability.Revision != 1 || target.Destinations[1].Availability.Revision != 4 || len(target.Bindings) != 1 {
		t.Fatalf("apply helper changed unrelated knowledge: %+v", target)
	}
}

func TestSSNMUpdatedCongestionScopeOwnership(t *testing.T) {
	event := SSNMEvent{Updated: []SSNMDestinationKnowledge{{CongestionSet: true, Congestion: SSNMCongestion{Scope: WireScope{RoutingContexts: []uint32{1, 2}, RoutingContextSet: true}}}}}
	clone := event.clone()
	clone.Updated[0].Congestion.Scope.RoutingContexts[0] = 99
	if event.Updated[0].Congestion.Scope.RoutingContexts[0] != 1 {
		t.Fatal("updated congestion scope shares caller storage")
	}
}

func TestSSNMInvalidationMinimumByteBudgetAccountsResolution(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, nil, &SSNMStateConfig{MaxBytes: ssnmPartitionBaseBytes + ssnmMinimumRecordBytes})
	peer, _ := newTestConnWithContexts(t, StateASPActive, RoleASP)
	setInventoryNetworkAppearance(&peer.cfg.ApplicationServers, params.NewNetworkAppearance(7))
	peer.noteRoutingContextsAcked(nil)
	if !endpoint.trackAssociation(peer) {
		t.Fatal("track contextless source")
	}
	t.Cleanup(func() { _ = peer.Close() })
	if err := peer.handleDestinationUnavailable(messages.NewDestinationUnavailable(params.NewNetworkAppearance(7), nil, params.NewAffectedPointCode(123), nil)); err != nil {
		t.Fatal("minimum budget cannot admit its promised record", err)
	}
	if len(endpoint.SSNMKnowledge().Partitions[0].Destinations) != 1 {
		t.Fatal("minimum budget lost record")
	}
	assertSSNMScopeAccounting(t, endpoint)
}

func TestSSNMInvalidationResolvesCanonicalPartitionsIndependently(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), &SSNMStateConfig{MaxAffectedPointCodes: 1})
	first := attachSSNMAssociation(t, endpoint, SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}, 7, 1, 3)
	sibling := attachSSNMAssociation(t, endpoint, SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a2"}, 7, 2)
	first.addDynamicASKey(*staticASKey(8, 3), RoutingKey{}, false)
	first.noteCanonicalRemoteAS(3, "as-edge")
	sibling.addDynamicASKey(*staticASKey(8, 2), RoutingKey{}, false)
	sibling.noteCanonicalRemoteAS(2, "as-core")
	sendDUNA(t, sibling, 8, 2, 101)
	if err := first.handleDestinationUnavailable(messages.NewDestinationUnavailable(nil, params.NewRoutingContext(3), params.NewAffectedPointCode(102), nil)); err != nil {
		t.Fatal(err)
	}
	core := canonicalSSNMPartition("sg-a", "as-core")
	edge := canonicalSSNMPartition("sg-a", "as-edge")
	before := ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(), core)
	// Exercise the ownership-resolution seam directly: the wire handler
	// separately rejects a missing NA spanning different configured networks.
	if err := first.refuseOversizedSSNM(WireScope{RoutingContextSet: true, RoutingContexts: []uint32{1, 3}}, 2, 1, SSNMDestinationUnavailableReport); !errors.Is(err, ErrSSNMOversizedReport) {
		t.Fatal(err)
	}
	after := endpoint.SSNMKnowledge()
	if !reflect.DeepEqual(before, ssnmPartitionKnowledge(t, after, core)) {
		t.Fatal("another AS network widened canonical invalidation scope")
	}
	if len(ssnmPartitionKnowledge(t, after, edge).Destinations) != 0 {
		t.Fatal("affected canonical AS retained contradicted knowledge")
	}
	assertSSNMScopeAccounting(t, endpoint)
}
