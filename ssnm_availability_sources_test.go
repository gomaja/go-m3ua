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

func TestAvailabilitySourcesRetainIndependentScopesAndReplayRemoval(t *testing.T) {
	r := newScopedAutomationRig(t)
	r.e.ssnm.validity = 0
	sendDUNA(t, r.a, 7, 1, 100)
	sendDAVA(t, r.a, 7, 2, 100)
	partition := r.a.ssnmPartitionFor(*staticASKey(7, 1))
	one := r.e.ssnm.destinationKnowledge(partition, 100, *staticASKey(7, 1))
	two := r.e.ssnm.destinationKnowledge(partition, 100, *staticASKey(7, 2))
	if !one.availabilitySet || one.availability != DestinationUnavailable || !two.availabilitySet || two.availability != DestinationAvailable {
		t.Fatal("same-prefix AS statements lost applicability", one, two)
	}
	before, sub, err := r.e.SubscribeSSNM()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close() }()
	if len(before.Partitions) != 1 || len(before.Partitions[0].Destinations) != 1 || len(before.Partitions[0].Destinations[0].AvailabilitySources) != 2 {
		t.Fatal("snapshot lost independent availability sources", before)
	}
	owned := r.e.SSNMKnowledge()
	owned.Partitions[0].Destinations[0].AvailabilitySources[0].Scope.RoutingContexts[0] = 999
	if reflect.DeepEqual(owned.Partitions, r.e.SSNMKnowledge().Partitions) {
		t.Fatal("snapshot source scope aliases retained state")
	}
	state := r.e.ssnm
	state.mu.Lock()
	var outcomes auditOutcomeBatch
	sources, dimensions := state.retireReceivedAvailabilityLocked(state.partitions[partition], func(_ receivedCongestionKey, record SSNMAvailability) bool { return record.AS.RoutingContext == 1 }, &outcomes)
	state.publishDestinationRemovalsLocked(state.partitions[partition], dimensions, nil, "source removed", sources)
	outcomes.publishLocked(state)
	state.mu.Unlock()
	events, _, lost := outcomeProbeEvents(sub)
	if lost {
		t.Fatal("compact source removal lost continuity")
	}
	replica := before.Partitions[0]
	removed := 0
	for _, event := range events {
		if event.Kind != SSNMDestinationsInvalidatedEvent {
			continue
		}
		removed += len(event.AvailabilitySourcesInvalidated)
		if !event.ApplyInvalidation(&replica) {
			t.Fatal("source invalidation did not match partition")
		}
		if !event.ApplyInvalidation(&replica) {
			t.Fatal("source invalidation not idempotent")
		}
	}
	after := r.e.SSNMKnowledge()
	if removed != 1 || !reflect.DeepEqual(replica, after.Partitions[0]) {
		t.Fatal("source removal replay diverged", replica, after.Partitions[0])
	}
	one = state.destinationKnowledge(partition, 100, *staticASKey(7, 1))
	two = state.destinationKnowledge(partition, 100, *staticASKey(7, 2))
	if one.availabilitySet || !two.availabilitySet || two.availability != DestinationAvailable {
		t.Fatal("retired AS inherited sibling authority", one, two)
	}
	assertSSNMScopeAccounting(t, r.e)
}

func TestAuditingAvailabilityFanoutIsAdmittedWhole(t *testing.T) {
	r := newScopedAutomationRig(t)
	r.e.ssnm.limits.MaxRecordsPerPartition = 2
	err := r.a.handleDestinationUnavailable(messages.NewDestinationUnavailable(params.NewNetworkAppearance(7), params.NewRoutingContext(1, 2), params.NewAffectedPointCode(100), nil))
	if !errors.Is(err, ErrSSNMStateLimit) {
		t.Fatalf("scalar plus two sources must exceed two records: %v", err)
	}
	snapshot := r.e.SSNMKnowledge()
	if r.e.ssnm.records != 0 || len(snapshot.Partitions[0].Destinations) != 0 || len(r.e.ssnm.audits) != 0 {
		t.Fatal("refused fanout partially retained source/scalar/audit", snapshot)
	}
	assertSSNMScopeAccounting(t, r.e)
}

func TestAuditingWithoutValidityRetainsCongestionWithoutExpiry(t *testing.T) {
	r := newScopedAutomationRig(t)
	r.e.ssnm.validity = 0
	sendSCON(t, r.a, 7, 1, congestionLevel(2), 100)
	sendSCON(t, r.a, 7, 2, congestionLevel(0), 100)
	snapshot := r.e.SSNMKnowledge()
	sources := snapshot.Partitions[0].Destinations[0].CongestionSources
	if len(sources) != 2 {
		t.Fatal("auditing-only sources were collapsed", sources)
	}
	for _, source := range sources {
		if !source.ExpiresAt.IsZero() || source.Freshness != CongestionFresh {
			t.Fatal("auditing enabled a validity timer", source)
		}
	}
	r.e.ssnm.expireCongestion(r.now.Add(24 * time.Hour))
	if !reflect.DeepEqual(snapshot.Partitions, r.e.SSNMKnowledge().Partitions) {
		t.Fatal("auditing-only source expired")
	}
	assertSSNMScopeAccounting(t, r.e)
}

func TestScalarDestinationCacheDoesNotBroadenASApplicability(t *testing.T) {
	for _, congestion := range []bool{false, true} {
		t.Run(map[bool]string{false: "availability", true: "congestion"}[congestion], func(t *testing.T) {
			r := newScopedAutomationRig(t)
			r.e.ssnm.validity = 0
			r.e.ssnm.auditing = nil
			partition := r.a.ssnmPartitionFor(*staticASKey(7, 1))
			if congestion {
				sendSCON(t, r.a, 7, 1, congestionLevel(2), 100)
			} else {
				sendDUNA(t, r.a, 7, 1, 100)
			}
			other := r.e.ssnm.destinationKnowledge(partition, 100, *staticASKey(7, 2))
			if other.availabilitySet || other.congestionSet || other.congested {
				t.Fatal("scalar cache crossed AS", other)
			}
			if congestion {
				sendSCON(t, r.a, 7, 2, congestionLevel(0), 100)
			} else {
				sendDAVA(t, r.a, 7, 2, 100)
			}
			displaced := r.e.ssnm.destinationKnowledge(partition, 100, *staticASKey(7, 1))
			if displaced.availabilitySet || displaced.congestionSet {
				t.Fatal("displaced AS inherited the replacement statement", displaced)
			}
			snapshot := r.e.SSNMKnowledge()
			destination := snapshot.Partitions[0].Destinations[0]
			if r.e.ssnm.records != 1 || len(destination.AvailabilitySources) != 0 || len(destination.CongestionSources) != 0 {
				t.Fatal("nil policy retained unrequested source history", snapshot, r.e.ssnm.records)
			}
			assertSSNMScopeAccounting(t, r.e)
		})
	}
}

func TestAvailabilityRemovalIDsAreOwnedAcrossSubscribers(t *testing.T) {
	r := newScopedAutomationRig(t)
	r.e.ssnm.limits.MaxAffectedPointCodes = 1
	sendDUNA(t, r.a, 7, 1, 100)
	_, one, err := r.e.SubscribeSSNM()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = one.Close() }()
	_, two, err := r.e.SubscribeSSNM()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = two.Close() }()
	err = r.a.handleDestinationUnavailable(messages.NewDestinationUnavailable(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCode(200, 201), nil))
	if !errors.Is(err, ErrSSNMOversizedReport) {
		t.Fatal(err)
	}
	next := func(sub *SSNMSubscription) SSNMEvent {
		for {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			event, err := sub.Next(ctx)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			if event.Kind == SSNMDestinationsInvalidatedEvent {
				return event
			}
		}
	}
	a := next(one)
	b := next(two)
	a.AvailabilitySourcesInvalidated[0].AS.RoutingContext = 999
	if b.AvailabilitySourcesInvalidated[0].AS.RoutingContext != 1 {
		t.Fatal("availability source removal IDs alias another subscriber")
	}
}

func TestAvailabilityHistoryOnlyDestinationSurvivesAndReplaysForget(t *testing.T) {
	r := newScopedAutomationRig(t)
	r.e.ssnm.validity = 0
	sendDUNA(t, r.a, 7, 1, 100)
	sendDAVA(t, r.a, 7, 2, 100)
	before, sub, err := r.e.SubscribeSSNM()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Close() }()
	partition := r.a.ssnmPartitionFor(*staticASKey(7, 1))
	state := r.e.ssnm
	state.mu.Lock()
	var outcomes auditOutcomeBatch
	ids, dims := state.retireReceivedAvailabilityLocked(state.partitions[partition], func(_ receivedCongestionKey, r SSNMAvailability) bool { return r.AS.RoutingContext == 2 }, &outcomes)
	state.publishDestinationRemovalsLocked(state.partitions[partition], dims, nil, "latest source retired", ids)
	outcomes.publishLocked(state)
	state.mu.Unlock()
	replica := before.Partitions[0]
	apply := func() {
		events, _, lost := outcomeProbeEvents(sub)
		if lost {
			t.Fatal("availability source replay lost continuity")
		}
		for _, e := range events {
			if e.Kind == SSNMDestinationsInvalidatedEvent {
				e.ApplyInvalidation(&replica)
			}
		}
	}
	apply()
	after := r.e.SSNMKnowledge()
	if !reflect.DeepEqual(replica, after.Partitions[0]) {
		t.Fatal("availability history-only replay differs from snapshot")
	}
	entry := after.Partitions[0].Destinations[0]
	if entry.AvailabilitySet || len(entry.AvailabilitySources) != 1 || entry.AvailabilitySources[0].AS.RoutingContext != 1 {
		t.Fatal("availability history-only destination disappeared", entry)
	}
	if status := state.destinationKnowledge(partition, 100, *staticASKey(7, 1)); !status.availabilitySet || status.availability != DestinationUnavailable {
		t.Fatal("history-only AS lost authority", status)
	}
	assertSSNMScopeAccounting(t, r.e)
	r.a.ForgetDestinations()
	apply()
	if len(replica.Destinations) != 0 || len(r.e.SSNMKnowledge().Partitions[0].Destinations) != 0 || r.e.ssnm.records != 0 {
		t.Fatal("forget did not remove last availability history/charges")
	}
	assertSSNMScopeAccounting(t, r.e)
}

func TestGroupedAvailabilityInvalidationKeepsOtherASAudit(t *testing.T) {
	r := newScopedAutomationRig(t)
	r.e.ssnm.validity = 0
	if err := r.a.handleDestinationUnavailable(messages.NewDestinationUnavailable(params.NewNetworkAppearance(7), params.NewRoutingContext(1, 2), params.NewAffectedPointCode(100), nil)); err != nil {
		t.Fatal(err)
	}
	r.e.ssnm.limits.MaxAffectedPointCodes = 1
	err := r.a.handleDestinationUnavailable(messages.NewDestinationUnavailable(params.NewNetworkAppearance(7), params.NewRoutingContext(1), params.NewAffectedPointCode(200, 201), nil))
	if !errors.Is(err, ErrSSNMOversizedReport) {
		t.Fatal(err)
	}
	partition := r.a.ssnmPartitionFor(*staticASKey(7, 2))
	two := r.e.ssnm.destinationKnowledge(partition, 100, *staticASKey(7, 2))
	if !two.availabilitySet || two.availability != DestinationUnavailable {
		t.Fatal("scoped invalidation removed other AS source evidence", two)
	}
	if len(r.e.ssnm.audits) != 1 {
		t.Fatal("grouped scalar invalidation retired another AS audit", len(r.e.ssnm.audits))
	}
	r.advance(30 * time.Second)
	if len(r.sent) != 1 {
		t.Fatal("remaining AS audit did not submit", len(r.sent))
	}
	audit, ok := r.sent[0].(*messages.DestinationStateAudit)
	if !ok || !reflect.DeepEqual(audit.RoutingContext.RoutingContexts(), []uint32{2}) {
		t.Fatal("remaining audit did not narrow to preserved AS", r.sent)
	}
	assertSSNMScopeAccounting(t, r.e)
}
