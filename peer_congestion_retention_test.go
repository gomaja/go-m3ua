// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func peerRetentionFixture(t *testing.T, config *SSNMStateConfig, association AssociationID, keys ...ASKey) (*ssnmState, SSNMReport, map[ASKey]uint64) {
	t.Helper()
	store, err := newSSNMState(config)
	if err != nil {
		t.Fatal(err)
	}
	report := SSNMReport{Kind: SSNMSignallingCongestionReport, Source: SSNMPeerReport, PeerReported: true, Association: association, Partition: SSNMPartition{Kind: SSNMStandalonePartition, Association: association}, Scope: WireScope{RoutingContextSet: true, RoutingContexts: []uint32{7}}, Destinations: []PointCodeRange{{PointCode: 123}}, CongestionLevel: 2, CongestionLevelSet: true}
	store.registerSource(association)
	if err := store.bind(report.Partition, association, false); err != nil {
		t.Fatal(err)
	}
	bindings := make(map[ASKey]uint64, len(keys))
	for _, key := range keys {
		bindings[key] = 1
	}
	store.syncPeerCongestion(association, 1, bindings)
	return store, report, bindings
}

func peerRetentionApply(t *testing.T, store *ssnmState, report SSNMReport, epoch uint64, bindings map[ASKey]uint64) {
	t.Helper()
	if err := store.applyPeerReport(report, epoch, bindings); err != nil {
		t.Fatal(err)
	}
}

func peerRetentionNext(t *testing.T, subscription *SSNMSubscription) SSNMEvent {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	event, err := subscription.Next(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return event
}

func TestPeerSCONSurvivesSubscriptionResync(t *testing.T) {
	key := routingContextASKey(7)
	store, report, bindings := peerRetentionFixture(t, nil, 1, key)
	endpoint := &Endpoint{ssnm: store}
	_, subscription, err := endpoint.SubscribeSSNM()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = subscription.Close() }()
	peerRetentionApply(t, store, report, 1, bindings)
	changed := peerRetentionNext(t, subscription)
	raw := peerRetentionNext(t, subscription)
	if changed.Kind != SSNMPeerCongestionEvent || changed.PeerCongestion == nil || raw.Kind != SSNMReportObservedEvent || !raw.Report.PeerReported || raw.Revision != changed.Revision+1 {
		t.Fatalf("peer delta must precede raw report with successive revisions: %+v %+v", changed, raw)
	}
	snapshot, err := subscription.Resync()
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.PeerCongestion) != 1 {
		t.Fatalf("resync retained %d peer records, want 1", len(snapshot.PeerCongestion))
	}
	if snapshot.Partitions[0].Destinations != nil {
		t.Fatalf("peer-only report changed authoritative destination snapshot: %+v", snapshot.Partitions[0].Destinations)
	}
	record := snapshot.PeerCongestion[0]
	if record.Association != 1 || record.Epoch != 1 || record.AS != key || record.Congestion.Level != 2 || !record.Congestion.LevelSet || !record.Congestion.Congested || record.Freshness != CongestionFresh || record.ReceivedAt.IsZero() || record.Revision != changed.Revision || snapshot.Revision != raw.Revision {
		t.Fatalf("wrong retained record: %+v, snapshot revision %d", record, snapshot.Revision)
	}
	snapshot.PeerCongestion[0].Scope.RoutingContexts[0] = 999
	changed.PeerCongestion.Scope.RoutingContexts[0] = 998
	report.Scope.RoutingContexts[0] = 997
	if got := store.snapshot().PeerCongestion[0].Scope.RoutingContexts[0]; got != 7 {
		t.Fatalf("caller mutated retained RC: %d", got)
	}
}

func TestPeerRetentionNeverChangesDestinationKnowledge(t *testing.T) {
	store, report, bindings := peerRetentionFixture(t, nil, 1, routingContextASKey(7))
	authority := report
	authority.PeerReported = false
	authority.CongestionLevel = 1
	if err := store.apply(authority); err != nil {
		t.Fatal(err)
	}
	before := store.snapshot().Partitions[0].Destinations
	peerRetentionApply(t, store, report, 1, bindings)
	after := store.snapshot()
	if !reflect.DeepEqual(before, after.Partitions[0].Destinations) {
		t.Fatalf("peer changed authoritative congestion: before %+v, after %+v", before, after.Partitions[0].Destinations)
	}
	if len(after.PeerCongestion) != 1 {
		t.Fatal("peer state absent")
	}
}

func TestPeerCongestionIsScopedToAssociationEpochASAndDestination(t *testing.T) {
	first, second := routingContextASKey(7), routingContextASKey(8)
	store, report, bindings := peerRetentionFixture(t, nil, 1, first, second)
	report.Destinations = []PointCodeRange{{PointCode: 0x1200, Mask: 8}, {PointCode: 0x1234}, {PointCode: 0x1234}}
	peerRetentionApply(t, store, report, 1, bindings)
	if got := len(store.snapshot().PeerCongestion); got != 4 {
		t.Fatalf("AS/range expansion retained %d records, want 4", got)
	}
	report.Destinations = []PointCodeRange{{PointCode: 0x1234}}
	report.Scope.NetworkAppearanceSet = true
	report.Scope.NetworkAppearance = 0
	peerRetentionApply(t, store, report, 1, map[ASKey]uint64{first: 1})
	if got := len(store.snapshot().PeerCongestion); got != 5 {
		t.Fatalf("explicit NA zero merged omitted NA: %d records", got)
	}
	report.Scope.NetworkAppearanceSet = false
	report.Scope.RoutingContexts = []uint32{8, 7}
	peerRetentionApply(t, store, report, 1, map[ASKey]uint64{first: 1})
	report.Scope.RoutingContexts = []uint32{7, 8, 7}
	report.CongestionLevel = 0
	peerRetentionApply(t, store, report, 1, map[ASKey]uint64{first: 1})
	snapshot := store.snapshot()
	if len(snapshot.PeerCongestion) != 6 {
		t.Fatalf("RC set order/duplicates created extra records: %d", len(snapshot.PeerCongestion))
	}
	store.syncPeerCongestion(1, 2, bindings)
	if len(store.snapshot().PeerCongestion) != 0 {
		t.Fatal("old epoch records survived restart")
	}
	if err := store.applyPeerReport(report, 1, bindings); !errors.Is(err, ErrAssociationClosed) {
		t.Fatalf("old epoch apply: %v", err)
	}
	store.syncPeerCongestion(1, 1, bindings)
	if err := store.applyPeerReport(report, 1, bindings); !errors.Is(err, ErrAssociationClosed) {
		t.Fatalf("late sync rolled epoch backwards: %v", err)
	}
	peerRetentionApply(t, store, report, 2, bindings)
	for _, record := range store.snapshot().PeerCongestion {
		if record.Epoch != 2 {
			t.Fatalf("wrong epoch: %+v", record)
		}
	}
}

func TestPeerRetentionRejectsAWholeOverBudgetReport(t *testing.T) {
	for _, limit := range []string{"partition", "peer", "global", "bytes"} {
		t.Run(limit, func(t *testing.T) {
			config := &SSNMStateConfig{MaxRecords: 8, MaxRecordsPerPeer: 8, MaxRecordsPerPartition: 8}
			switch limit {
			case "partition":
				config.MaxRecordsPerPartition = 3
			case "peer":
				config.MaxRecordsPerPeer = 3
				config.MaxRecordsPerPartition = 3
			case "global":
				config.MaxRecords = 3
				config.MaxRecordsPerPeer = 3
				config.MaxRecordsPerPartition = 3
			case "bytes":
				config.MaxBytes = ssnmPartitionBaseBytes + 4*264 - 1
			}
			store, report, bindings := peerRetentionFixture(t, config, 1, routingContextASKey(7), routingContextASKey(8))
			report.Destinations = []PointCodeRange{{PointCode: 1}, {PointCode: 2}}
			if err := store.applyPeerReport(report, 1, bindings); !errors.Is(err, ErrSSNMStateLimit) {
				t.Fatalf("expected bounded whole-report refusal: %v", err)
			}
			snapshot := store.snapshot()
			if len(snapshot.PeerCongestion) != 0 || store.records != 0 || snapshot.ReportsRefused != 1 || snapshot.RecordsRefused != 4 || snapshot.LastResourceLoss == "" {
				t.Fatalf("partial report retained or refusal unreported: %+v, records %d", snapshot, store.records)
			}
		})
	}
}

func TestPeerRetentionSharesBudgetWithDestinationKnowledge(t *testing.T) {
	config := &SSNMStateConfig{MaxRecords: 2, MaxRecordsPerPeer: 2, MaxRecordsPerPartition: 2}
	store, report, bindings := peerRetentionFixture(t, config, 1, routingContextASKey(7))
	authority := report
	authority.PeerReported = false
	if err := store.apply(authority); err != nil {
		t.Fatal(err)
	}
	peerRetentionApply(t, store, report, 1, bindings)
	report.Destinations = []PointCodeRange{{PointCode: 124}}
	if err := store.applyPeerReport(report, 1, bindings); !errors.Is(err, ErrSSNMStateLimit) {
		t.Fatalf("peer bypassed shared budget: %v", err)
	}
	report.PeerReported = false
	if err := store.apply(report); !errors.Is(err, ErrSSNMStateLimit) {
		t.Fatalf("authority bypassed peer charge: %v", err)
	}
	if store.records != 2 {
		t.Fatalf("wrong shared records: %d", store.records)
	}
}

func TestPeerWithdrawalRetiresOnlyItsScopes(t *testing.T) {
	first, second := routingContextASKey(7), routingContextASKey(8)
	store, report, bindings := peerRetentionFixture(t, nil, 1, first, second)
	peerRetentionApply(t, store, report, 1, bindings)
	store.syncPeerCongestion(1, 1, map[ASKey]uint64{second: 1})
	snapshot := store.snapshot()
	if len(snapshot.PeerCongestion) != 1 || snapshot.PeerCongestion[0].AS != second || store.records != 1 {
		t.Fatalf("withdrawal affected unrelated AS: %+v", snapshot)
	}
	if err := store.applyPeerReport(report, 1, map[ASKey]uint64{first: 1}); !errors.Is(err, ErrRoutingContextNotActive) {
		t.Fatalf("withdrawn AS accepted late report: %v", err)
	}
	store.syncPeerCongestion(1, 1, map[ASKey]uint64{first: 2, second: 1})
	if err := store.applyPeerReport(report, 1, bindings); !errors.Is(err, ErrRoutingContextNotActive) {
		t.Fatalf("reused RC accepted old generation: %v", err)
	}
	peerRetentionApply(t, store, report, 1, map[ASKey]uint64{first: 2})
	store.syncPeerCongestion(1, 1, map[ASKey]uint64{first: 3, second: 1})
	snapshot = store.snapshot()
	if len(snapshot.PeerCongestion) != 1 || snapshot.PeerCongestion[0].AS != second {
		t.Fatalf("binding replacement kept old scope or lost sibling: %+v", snapshot)
	}
	store.retireAssociation(1)
	if snapshot := store.snapshot(); len(snapshot.PeerCongestion) != 0 || store.records != 0 || store.bytes != 0 {
		t.Fatalf("teardown retained records: %+v, records %d bytes %d", snapshot, store.records, store.bytes)
	}
	if err := store.applyPeerReport(report, 1, bindings); !errors.Is(err, ErrAssociationClosed) {
		t.Fatalf("late closed-source report: %v", err)
	}
}

func TestPeerRetentionClearUnknownAndConcernedTriggerIsolation(t *testing.T) {
	store, report, bindings := peerRetentionFixture(t, nil, 1, routingContextASKey(7))
	for _, level := range []uint8{2, 0} {
		report.CongestionLevel = level
		peerRetentionApply(t, store, report, 1, bindings)
		record := store.snapshot().PeerCongestion[0]
		if record.Congestion.Congested != (level != 0) || !record.Congestion.LevelSet || record.Congestion.Level != level {
			t.Fatalf("clear/level not retained: %+v", record)
		}
	}
	report.CongestionLevelSet = false
	report.CongestionLevel = 0
	for origin := uint32(1); origin <= 64; origin++ {
		report.ConcernedDestinationSet = true
		report.ConcernedDestination = origin
		peerRetentionApply(t, store, report, 1, bindings)
	}
	snapshot := store.snapshot()
	if len(snapshot.PeerCongestion) != 1 || !snapshot.PeerCongestion[0].Congestion.Congested || snapshot.PeerCongestion[0].Congestion.LevelSet || store.records != 1 {
		t.Fatalf("omitted level or CD-key isolation failed: %+v", snapshot)
	}
}

func TestConcernedTriggerIsNotReplayedAfterResync(t *testing.T) {
	store, report, bindings := peerRetentionFixture(t, &SSNMStateConfig{SubscriptionQueueSize: 1}, 1, routingContextASKey(7))
	endpoint := &Endpoint{ssnm: store}
	_, subscription, err := endpoint.SubscribeSSNM()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = subscription.Close() }()
	report.ConcernedDestinationSet = true
	report.ConcernedDestination = 999
	peerRetentionApply(t, store, report, 1, bindings)
	changed := peerRetentionNext(t, subscription)
	lost := peerRetentionNext(t, subscription)
	if changed.Kind != SSNMPeerCongestionEvent || changed.ReportSet || changed.Report.ConcernedDestinationSet || lost.Kind != SSNMContinuityLostEvent {
		t.Fatalf("unexpected loss sequence: %+v %+v", changed, lost)
	}
	snapshot, err := subscription.Resync()
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.PeerCongestion) != 1 {
		t.Fatal("resync lost current state")
	}
	// The snapshot record type intentionally has no raw report or Concerned
	// Destination; resync restores state without a historical TFC request.
	for _, name := range []string{"ConcernedDestination", "ConcernedDestinationSet", "Report"} {
		if _, found := reflect.TypeFor[PeerCongestionState]().FieldByName(name); found {
			t.Fatalf("snapshot exposes replayable trigger %s", name)
		}
	}
	subscription.mu.Lock()
	queued := len(subscription.queue)
	subscription.mu.Unlock()
	if queued != 0 {
		t.Fatalf("resync queued historical triggers: %d events", queued)
	}
}

func TestPeerRetentionSnapshotsObserveWholeReportTransactions(t *testing.T) {
	store, report, bindings := peerRetentionFixture(t, nil, 1, routingContextASKey(7), routingContextASKey(8))
	report.Destinations = []PointCodeRange{{PointCode: 1}, {PointCode: 2}}
	var wait sync.WaitGroup
	firstCommitted := make(chan struct{})
	wait.Add(1)
	go func() {
		defer wait.Done()
		for index := 0; index < 100; index++ {
			report.CongestionLevel = uint8(index % 4)
			if err := store.applyPeerReport(report, 1, bindings); err != nil {
				t.Error(err)
				if index == 0 {
					close(firstCommitted)
				}
				return
			}
			if index == 0 {
				close(firstCommitted)
			}
		}
	}()
	<-firstCommitted
	runtime.Gosched()
	for index := 0; index < 100; index++ {
		snapshot := store.snapshot()
		if len(snapshot.PeerCongestion) == 0 {
			continue
		}
		if len(snapshot.PeerCongestion) != 4 {
			t.Fatalf("snapshot observed report prefix: %+v", snapshot)
		}
		level := snapshot.PeerCongestion[0].Congestion.Level
		for _, record := range snapshot.PeerCongestion {
			if record.Congestion.Level != level || record.Revision >= snapshot.Revision {
				t.Fatalf("snapshot observed mixed transaction: %+v", snapshot)
			}
		}
	}
	wait.Wait()
}

func TestPeerRetentionEmptyActiveEpochCannotResurrect(t *testing.T) {
	key := routingContextASKey(7)
	store, report, bindings := peerRetentionFixture(t, nil, 1, key)
	peerRetentionApply(t, store, report, 1, bindings)
	store.syncPeerCongestion(1, 2, nil)
	if err := store.applyPeerReport(report, 2, nil); !errors.Is(err, ErrRoutingContextNotActive) {
		t.Fatalf("empty scope published withdrawn report: %v", err)
	}
	store.syncPeerCongestion(1, 1, bindings)
	if err := store.applyPeerReport(report, 1, bindings); !errors.Is(err, ErrAssociationClosed) {
		t.Fatalf("empty-active epoch allowed stale resurrection: %v", err)
	}
	store.syncPeerCongestion(1, 2, map[ASKey]uint64{key: 2})
	if err := store.applyPeerReport(report, 2, bindings); !errors.Is(err, ErrRoutingContextNotActive) {
		t.Fatalf("reactivation allowed old generation: %v", err)
	}
	peerRetentionApply(t, store, report, 2, map[ASKey]uint64{key: 2})
	store.close()
	if len(store.snapshot().PeerCongestion) != 0 || store.records != 0 || store.bytes != 0 {
		t.Fatal("endpoint close retained peer records")
	}
	if err := store.applyPeerReport(report, 2, bindings); !errors.Is(err, ErrEndpointClosed) {
		t.Fatalf("closed endpoint apply: %v", err)
	}
}

func TestPeerRetentionAssociationIdentityAndBindingRetirement(t *testing.T) {
	key := routingContextASKey(7)
	store, report, bindings := peerRetentionFixture(t, nil, 1, key)
	// A shared canonical partition must still retain association-local inputs.
	partition := canonicalSSNMPartition("sg", "as")
	if err := store.bind(partition, 1, false); err != nil {
		t.Fatal(err)
	}
	report.Partition = partition
	peerRetentionApply(t, store, report, 1, bindings)
	store.registerSource(2)
	if err := store.bind(partition, 2, false); err != nil {
		t.Fatal(err)
	}
	store.syncPeerCongestion(2, 1, bindings)
	report.Association = 2
	report.CongestionLevel = 1
	peerRetentionApply(t, store, report, 1, bindings)
	if records := store.snapshot().PeerCongestion; len(records) != 2 || records[0].Association == records[1].Association {
		t.Fatalf("association reports merged: %+v", records)
	}
	endpoint := &Endpoint{ssnm: store}
	_, subscription, err := endpoint.SubscribeSSNM()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = subscription.Close() }()
	store.retire(partition, 1)
	removed := peerRetentionNext(t, subscription)
	retired := peerRetentionNext(t, subscription)
	if removed.Kind != SSNMPeerCongestionEvent || !removed.PeerCongestionRemoved || removed.PeerCongestion.Association != 1 || retired.Kind != SSNMBindingRetiredEvent || retired.Revision != removed.Revision+1 {
		t.Fatalf("wrong retirement event order: %+v %+v", removed, retired)
	}
	records := store.snapshot().PeerCongestion
	if len(records) != 1 || records[0].Association != 2 || records[0].Congestion.Level != 1 {
		t.Fatalf("retirement lost sibling: %+v", records)
	}
}

func TestPeerRetentionRefusalKeepsPriorStateAndReleasesCharges(t *testing.T) {
	store, report, bindings := peerRetentionFixture(t, &SSNMStateConfig{MaxRecords: 3, MaxRecordsPerPeer: 3, MaxRecordsPerPartition: 3}, 1, routingContextASKey(7))
	peerRetentionApply(t, store, report, 1, bindings)
	before := store.snapshot().PeerCongestion
	report.Destinations = []PointCodeRange{{PointCode: 124}, {PointCode: 125}, {PointCode: 126}}
	if err := store.applyPeerReport(report, 1, bindings); !errors.Is(err, ErrSSNMStateLimit) {
		t.Fatalf("expected refusal: %v", err)
	}
	if after := store.snapshot().PeerCongestion; !reflect.DeepEqual(before, after) {
		t.Fatalf("refusal changed prior state: %+v -> %+v", before, after)
	}
	store.mu.Lock()
	store.invalidateLocked(report.Partition, "synthetic resource loss")
	store.mu.Unlock()
	if store.records != 0 || store.bytes != ssnmPartitionBaseBytes || len(store.peerRecords) != 0 || len(store.snapshot().PeerCongestion) != 0 {
		t.Fatalf("invalidation leaked charges: records %d bytes %d peers %+v", store.records, store.bytes, store.peerRecords)
	}
}

func TestPeerRetentionDeltaPayloadUsesSubscriptionByteBudget(t *testing.T) {
	event := SSNMEvent{Kind: SSNMPeerCongestionEvent, PeerCongestion: &PeerCongestionState{Scope: WireScope{RoutingContexts: []uint32{7, 8}}}}
	// The documented portable charge is 512 event + 256 peer + 4 per RC.
	want := 512 + 256 + 2*4
	if got, fits := ssnmEventAccountedBytes(event, want); !fits || got != want {
		t.Fatalf("wrong peer delta charge: %d %v, want %d", got, fits, want)
	}
	if _, fits := ssnmEventAccountedBytes(event, want-1); fits {
		t.Fatal("peer delta bypassed queue byte limit")
	}
	if got := SSNMPeerCongestionEvent.String(); got != "peer-congestion" {
		t.Fatalf("event kind: %s", got)
	}
}

func TestPeerRetentionAppliesEachSharedRecordLimitIndependently(t *testing.T) {
	t.Run("partition", func(t *testing.T) {
		store, report, bindings := peerRetentionFixture(t, &SSNMStateConfig{MaxRecords: 8, MaxRecordsPerPeer: 8, MaxRecordsPerPartition: 3}, 1, routingContextASKey(7))
		peerRetentionApply(t, store, report, 1, bindings)
		report.Destinations = []PointCodeRange{{PointCode: 124}, {PointCode: 125}, {PointCode: 126}}
		if err := store.applyPeerReport(report, 1, bindings); !errors.Is(err, ErrSSNMStateLimit) {
			t.Fatalf("partition limit bypassed: %v", err)
		}
		if store.records != 1 {
			t.Fatalf("partition report partly applied: %d", store.records)
		}
	})
	t.Run("peer", func(t *testing.T) {
		store, report, bindings := peerRetentionFixture(t, &SSNMStateConfig{MaxRecords: 8, MaxRecordsPerPeer: 4, MaxRecordsPerPartition: 3}, 1, routingContextASKey(7))
		first := canonicalSSNMPartition("sg", "first")
		second := canonicalSSNMPartition("sg", "second")
		if err := store.bind(first, 1, false); err != nil {
			t.Fatal(err)
		}
		if err := store.bind(second, 1, false); err != nil {
			t.Fatal(err)
		}
		report.Destinations = []PointCodeRange{{PointCode: 1}, {PointCode: 2}}
		report.Partition = first
		peerRetentionApply(t, store, report, 1, bindings)
		report.Partition = second
		peerRetentionApply(t, store, report, 1, bindings)
		report.Destinations = []PointCodeRange{{PointCode: 3}}
		if err := store.applyPeerReport(report, 1, bindings); !errors.Is(err, ErrSSNMStateLimit) {
			t.Fatalf("peer limit bypassed: %v", err)
		}
		if store.records != 4 {
			t.Fatalf("peer report partly applied: %d", store.records)
		}
	})
	t.Run("global", func(t *testing.T) {
		store, report, bindings := peerRetentionFixture(t, &SSNMStateConfig{MaxRecords: 3, MaxRecordsPerPeer: 3, MaxRecordsPerPartition: 3}, 1, routingContextASKey(7))
		report.Destinations = []PointCodeRange{{PointCode: 1}, {PointCode: 2}}
		peerRetentionApply(t, store, report, 1, bindings)
		store.registerSource(2)
		report.Association = 2
		report.Partition = SSNMPartition{Kind: SSNMStandalonePartition, Association: 2}
		if err := store.bind(report.Partition, 2, false); err != nil {
			t.Fatal(err)
		}
		store.syncPeerCongestion(2, 1, bindings)
		report.Destinations = []PointCodeRange{{PointCode: 1}}
		peerRetentionApply(t, store, report, 1, bindings)
		report.Destinations = []PointCodeRange{{PointCode: 2}}
		if err := store.applyPeerReport(report, 1, bindings); !errors.Is(err, ErrSSNMStateLimit) {
			t.Fatalf("global limit bypassed: %v", err)
		}
		if store.records != 3 {
			t.Fatalf("global report partly applied: %d", store.records)
		}
	})
}

func FuzzPeerCongestionRetentionTranscript(f *testing.F) {
	f.Add([]byte{0, 1, 2, 0, 3, 2, 0, 1, 0, 2, 1, 0, 2, 3, 4})
	f.Add([]byte{0, 0, 0, 0, 0, 0, 1, 1, 1, 2, 2, 2})
	f.Fuzz(func(t *testing.T, transcript []byte) {
		if len(transcript) > 256 {
			return
		}
		first, second := routingContextASKey(7), routingContextASKey(8)
		store, report, bindings := peerRetentionFixture(t, &SSNMStateConfig{MaxRecords: 8, MaxRecordsPerPeer: 8, MaxRecordsPerPartition: 8, MaxBytes: 4096}, 1, first, second)
		epoch := uint64(1)
		generation := uint64(1)
		for _, action := range transcript {
			switch action % 4 {
			case 0:
				report.Destinations = []PointCodeRange{{PointCode: uint32(action % 7)}}
				report.CongestionLevel = (action >> 2) % 4
				report.CongestionLevelSet = action&16 == 0
				report.ConcernedDestination = uint32(action)
				report.ConcernedDestinationSet = true
				if err := store.applyPeerReport(report, epoch, bindings); err != nil && !errors.Is(err, ErrSSNMStateLimit) {
					t.Fatal(err)
				}
			case 1:
				epoch++
				store.syncPeerCongestion(1, epoch, bindings)
			case 2:
				generation++
				if _, active := bindings[first]; active {
					delete(bindings, first)
				} else {
					bindings[first] = generation
				}
				store.syncPeerCongestion(1, epoch, bindings)
			case 3:
				authority := report
				authority.PeerReported = false
				authority.Kind = SSNMDestinationAvailableReport
				if err := store.apply(authority); err != nil && !errors.Is(err, ErrSSNMStateLimit) {
					t.Fatal(err)
				}
			}
			snapshot := store.snapshot()
			records, bytes := 0, 0
			for _, state := range store.partitions {
				records += state.records()
				bytes += state.bytes
				for _, record := range state.peerCongestion {
					if record.Epoch != epoch {
						t.Fatalf("old epoch survived: %+v", record)
					}
					if _, active := bindings[record.AS]; !active {
						t.Fatalf("withdrawn AS survived: %+v", record)
					}
				}
			}
			if records != store.records || bytes != store.bytes || records > 8 || bytes > 4096 {
				t.Fatalf("accounting drift: records %d/%d bytes %d/%d", records, store.records, bytes, store.bytes)
			}
			if len(snapshot.PeerCongestion) > records {
				t.Fatal("peer snapshot exceeds shared record count")
			}
		}
	})
}

func TestPeerRetentionImmediateASAndDynamicContextWithdrawal(t *testing.T) {
	first := *staticASKey(7, 1)
	secondAppearance := *staticASKey(8, 1)
	sibling := *staticASKey(7, 2)
	store, report, bindings := peerRetentionFixture(t, nil, 1, first, secondAppearance, sibling)
	peerRetentionApply(t, store, report, 1, bindings)
	store.retirePeerCongestionAS(1, first)
	if err := store.applyPeerReport(report, 1, map[ASKey]uint64{first: 1}); !errors.Is(err, ErrRoutingContextNotActive) {
		t.Fatalf("withdrawn member accepted old token: %v", err)
	}
	records := store.snapshot().PeerCongestion
	if len(records) != 2 {
		t.Fatalf("AS withdrawal lost sibling appearances: %+v", records)
	}
	store.syncPeerCongestion(1, 1, map[ASKey]uint64{first: 2, secondAppearance: 1, sibling: 1})
	if err := store.applyPeerReport(report, 1, map[ASKey]uint64{first: 1}); !errors.Is(err, ErrRoutingContextNotActive) {
		t.Fatalf("reactivation accepted pre-withdrawal token: %v", err)
	}
	peerRetentionApply(t, store, report, 1, map[ASKey]uint64{first: 2})
	store.retirePeerCongestionContext(1, 1)
	records = store.snapshot().PeerCongestion
	if len(records) != 1 || records[0].AS != sibling {
		t.Fatalf("dynamic RC retirement crossed unrelated scope: %+v", records)
	}
	for _, key := range []ASKey{first, secondAppearance} {
		if err := store.applyPeerReport(report, 1, map[ASKey]uint64{key: 2}); !errors.Is(err, ErrRoutingContextNotActive) {
			t.Fatalf("retired RC accepted %v: %v", key, err)
		}
	}
}

func TestPeerRetentionDoesNotRetainLocallyOriginatedReport(t *testing.T) {
	store, report, bindings := peerRetentionFixture(t, nil, 1, routingContextASKey(7))
	report.Source = SSNMLocalReport
	peerRetentionApply(t, store, report, 1, bindings)
	if snapshot := store.snapshot(); len(snapshot.PeerCongestion) != 0 || len(snapshot.Partitions[0].Destinations) != 0 || store.records != 0 {
		t.Fatalf("local report became peer authority: %+v", snapshot)
	}
}

func peerRetentionHandlerFixture(t *testing.T) (*Endpoint, *Association, *Association, ASKey, ASKey) {
	t.Helper()
	listener, _, peer, _ := distributionFixtureForContexts(t, params.TrafficModeLoadshare, []uint32{1, 2}, nil)
	sibling, _ := addDistributionASP(t, listener, StateASPInactive, 1, 2)
	for _, association := range []*Association{peer, sibling} {
		if err := association.handleAspActive(messages.NewAspActive(params.NewTrafficModeType(params.TrafficModeLoadshare), params.NewRoutingContext(1, 2), nil)); err != nil {
			t.Fatalf("activate fixture: %v", err)
		}
		if association.State() != StateASPActive {
			t.Fatalf("activation did not commit state: %v", association.State())
		}
	}
	return listener.endpoint, peer, sibling, associationConfigASKey(peer.cfg, 1), associationConfigASKey(peer.cfg, 2)
}

func peerRetentionHandleSCON(t *testing.T, association *Association, level uint8, contexts ...uint32) {
	t.Helper()
	if err := association.handleSignallingCongestion(messages.NewSignallingCongestion(params.NewNetworkAppearance(0), params.NewRoutingContext(contexts...), params.NewAffectedPointCode(123), nil, params.NewCongestionIndications(level), nil)); err != nil {
		t.Fatalf("SCON handler: %v", err)
	}
}

func peerRetentionLateReport(scope WireScope) SSNMReport {
	return SSNMReport{Kind: SSNMSignallingCongestionReport, Source: SSNMPeerReport, PeerReported: true, Scope: scope, Destinations: []PointCodeRange{{PointCode: 123}}, CongestionLevel: 3, CongestionLevelSet: true, ConcernedDestination: 321, ConcernedDestinationSet: true}
}

func peerRetentionHasRecord(snapshot SSNMSnapshot, association AssociationID, key ASKey) bool {
	for _, record := range snapshot.PeerCongestion {
		if record.Association == association && record.AS == key {
			return true
		}
	}
	return false
}

func TestPeerRetentionHandlerWithdrawalPreservesOtherASAndTransport(t *testing.T) {
	endpoint, peer, sibling, first, second := peerRetentionHandlerFixture(t)
	peerRetentionHandleSCON(t, peer, 2, 1, 2)
	peerRetentionHandleSCON(t, sibling, 1, 1)
	transport := TransportCongestionState{Association: peer.ID(), Epoch: peer.Epoch(), Known: true, Level: 3}
	peer.transportSnapshot.Store(&transport)
	scope := WireScope{NetworkAppearanceSet: true, RoutingContextSet: true, RoutingContexts: []uint32{1}}
	admission := peer.capturePeerCongestionAdmission(scope)
	if len(admission.bindings) != 1 {
		t.Fatalf("capture did not resolve one active AS: %+v", admission)
	}
	if err := peer.handleAspInactive(messages.NewAspInactive(params.NewRoutingContext(1), nil)); err != nil {
		t.Fatal(err)
	}
	snapshot := endpoint.SSNMKnowledge()
	if peerRetentionHasRecord(snapshot, peer.ID(), first) || !peerRetentionHasRecord(snapshot, peer.ID(), second) || !peerRetentionHasRecord(snapshot, sibling.ID(), first) {
		t.Fatalf("per-AS withdrawal crossed scopes: %+v", snapshot.PeerCongestion)
	}
	if got := peer.TransportCongestion(); !reflect.DeepEqual(got, transport) {
		t.Fatalf("AS withdrawal cleared transport input: %+v", got)
	}
	inputs, err := endpoint.CongestionInputs(second)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, input := range inputs.Peers {
		if input.Association == peer.ID() {
			found = input.Active && len(input.PeerCongestion) == 1 && input.PeerCongestion[0].AS == second && input.Transport.Level == 3
		}
	}
	if !found {
		t.Fatalf("public inputs lost surviving AS/transport: %+v", inputs)
	}
	revision := snapshot.Revision
	if err := peer.publishPeerCongestionReport(peerRetentionLateReport(scope), admission); err != nil {
		t.Fatalf("late local admission became protocol failure: %v", err)
	}
	if got := endpoint.SSNMKnowledge(); got.Revision != revision || peerRetentionHasRecord(got, peer.ID(), first) {
		t.Fatalf("withdrawn old admission republished state/trigger: %+v", got)
	}
	if err := peer.handleAspActive(messages.NewAspActive(params.NewTrafficModeType(params.TrafficModeLoadshare), params.NewRoutingContext(1), nil)); err != nil {
		t.Fatal(err)
	}
	revision = endpoint.SSNMKnowledge().Revision
	if err := peer.publishPeerCongestionReport(peerRetentionLateReport(scope), admission); err != nil {
		t.Fatal(err)
	}
	if got := endpoint.SSNMKnowledge(); got.Revision != revision || peerRetentionHasRecord(got, peer.ID(), first) {
		t.Fatalf("reactivation reused old membership token: %+v", got)
	}
	peerRetentionHandleSCON(t, peer, 0, 1)
	found = false
	for _, record := range endpoint.SSNMKnowledge().PeerCongestion {
		if record.Association == peer.ID() && record.AS == first {
			found = record.Congestion.LevelSet && !record.Congestion.Congested && record.Congestion.Level == 0
		}
	}
	if !found {
		t.Fatal("fresh handler did not retain clear after reactivation")
	}
}

func TestPeerRetentionHandlerRestartRejectsLateAdmission(t *testing.T) {
	endpoint, peer, sibling, first, second := peerRetentionHandlerFixture(t)
	peerRetentionHandleSCON(t, peer, 2, 1, 2)
	peerRetentionHandleSCON(t, sibling, 1, 1)
	scope := WireScope{NetworkAppearanceSet: true, RoutingContextSet: true, RoutingContexts: []uint32{1, 2}}
	admission := peer.capturePeerCongestionAdmission(scope)
	oldEpoch := peer.Epoch()
	peer.handleSCTPRestart()
	if peer.Epoch() != oldEpoch+1 {
		t.Fatal("restart did not advance association epoch")
	}
	snapshot := endpoint.SSNMKnowledge()
	if peerRetentionHasRecord(snapshot, peer.ID(), first) || peerRetentionHasRecord(snapshot, peer.ID(), second) || !peerRetentionHasRecord(snapshot, sibling.ID(), first) {
		t.Fatalf("restart retained old peer or lost sibling: %+v", snapshot.PeerCongestion)
	}
	revision := snapshot.Revision
	if err := peer.publishPeerCongestionReport(peerRetentionLateReport(scope), admission); err != nil {
		t.Fatal(err)
	}
	if got := endpoint.SSNMKnowledge(); got.Revision != revision || len(got.PeerCongestion) != len(snapshot.PeerCongestion) {
		t.Fatalf("old restart admission changed current state/trigger: %+v", got)
	}
	// Resume the synthetic ASPSM fixture at the committed inactive boundary;
	// the fresh SCON still enters through the complete receive handler.
	peer.commitState(StateASPInactive)
	if err := peer.handleAspActive(messages.NewAspActive(params.NewTrafficModeType(params.TrafficModeLoadshare), params.NewRoutingContext(1, 2), nil)); err != nil {
		t.Fatal(err)
	}
	peerRetentionHandleSCON(t, peer, 3, 1)
	for _, record := range endpoint.SSNMKnowledge().PeerCongestion {
		if record.Association == peer.ID() && record.Epoch != peer.Epoch() {
			t.Fatalf("fresh handler installed old epoch: %+v", record)
		}
	}
}

func TestPeerRetentionHandlerDynamicSameASReplacementRejectsLateAdmission(t *testing.T) {
	endpoint, peer, _, first, second := peerRetentionHandlerFixture(t)
	peer.addDynamicASKey(first, RoutingKey{}, false)
	peerRetentionHandleSCON(t, peer, 2, 1, 2)
	scope := WireScope{NetworkAppearanceSet: true, RoutingContextSet: true, RoutingContexts: []uint32{1}}
	admission := peer.capturePeerCongestionAdmission(scope)
	peer.addDynamicASKey(first, RoutingKey{}, false)
	snapshot := endpoint.SSNMKnowledge()
	if peerRetentionHasRecord(snapshot, peer.ID(), first) || !peerRetentionHasRecord(snapshot, peer.ID(), second) {
		t.Fatalf("dynamic replacement crossed AS scope: %+v", snapshot.PeerCongestion)
	}
	revision := snapshot.Revision
	if err := peer.publishPeerCongestionReport(peerRetentionLateReport(scope), admission); err != nil {
		t.Fatal(err)
	}
	if got := endpoint.SSNMKnowledge(); got.Revision != revision || peerRetentionHasRecord(got, peer.ID(), first) {
		t.Fatalf("same-AS replacement accepted old dynamic token: %+v", got)
	}
	peerRetentionHandleSCON(t, peer, 1, 1)
	if !peerRetentionHasRecord(endpoint.SSNMKnowledge(), peer.ID(), first) {
		t.Fatal("fresh replacement SCON not retained")
	}
	admission = peer.capturePeerCongestionAdmission(scope)
	peer.removeDynamicASKey(first.RoutingContext, false)
	snapshot = endpoint.SSNMKnowledge()
	if peerRetentionHasRecord(snapshot, peer.ID(), first) || !peerRetentionHasRecord(snapshot, peer.ID(), second) {
		t.Fatalf("dynamic retirement crossed AS scope: %+v", snapshot.PeerCongestion)
	}
	if err := peer.publishPeerCongestionReport(peerRetentionLateReport(scope), admission); err != nil {
		t.Fatal(err)
	}
	if peerRetentionHasRecord(endpoint.SSNMKnowledge(), peer.ID(), first) {
		t.Fatal("removed dynamic binding accepted stale admission")
	}
	peer.addDynamicASKey(first, RoutingKey{}, false)
	peerRetentionHandleSCON(t, peer, 0, 1)
	if !peerRetentionHasRecord(endpoint.SSNMKnowledge(), peer.ID(), first) {
		t.Fatal("new dynamic binding did not accept fresh handler report")
	}
}

func TestPeerRetentionWireScopePresence(t *testing.T) {
	store, report, bindings := peerRetentionFixture(t, nil, 1, routingContextASKey(7))
	for _, scope := range []WireScope{{}, {RoutingContextSet: true}, {NetworkAppearanceSet: true}, {NetworkAppearanceSet: true, RoutingContextSet: true}} {
		report.Scope = scope
		peerRetentionApply(t, store, report, 1, bindings)
	}
	if records := store.snapshot().PeerCongestion; len(records) != 4 {
		t.Fatalf("omitted/explicit zero wire scope collapsed: %+v", records)
	}
}

func TestPeerRetentionReplacementByteBudgetIsAtomic(t *testing.T) {
	store, report, bindings := peerRetentionFixture(t, &SSNMStateConfig{MaxBytes: ssnmPartitionBaseBytes + 264}, 1, routingContextASKey(7))
	peerRetentionApply(t, store, report, 1, bindings)
	before := store.snapshot().PeerCongestion
	report.Scope.RoutingContexts = []uint32{7, 7}
	report.CongestionLevel = 0
	if err := store.applyPeerReport(report, 1, bindings); !errors.Is(err, ErrSSNMStateLimit) {
		t.Fatalf("larger replacement bypassed byte bound: %v", err)
	}
	snapshot := store.snapshot()
	if !reflect.DeepEqual(before, snapshot.PeerCongestion) || snapshot.RecordsRefused != 1 || snapshot.ReportsRefused != 1 || store.bytes != ssnmPartitionBaseBytes+264 {
		t.Fatalf("replacement refusal changed state/accounting: %+v, bytes %d", snapshot, store.bytes)
	}
}

func TestPeerRetentionKeepsAssociationAndPartitionEpochsDistinct(t *testing.T) {
	store, report, bindings := peerRetentionFixture(t, nil, 1, routingContextASKey(7))
	store.syncPeerCongestion(1, 9, bindings)
	endpoint := &Endpoint{ssnm: store}
	before, subscription, err := endpoint.SubscribeSSNM()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = subscription.Close() }()
	peerRetentionApply(t, store, report, 9, bindings)
	changed, raw := peerRetentionNext(t, subscription), peerRetentionNext(t, subscription)
	partitionEpoch := before.Partitions[0].Epoch
	if changed.PeerCongestion == nil || changed.PeerCongestion.Epoch != 9 || changed.Epoch != partitionEpoch || raw.Report.Epoch != partitionEpoch || raw.Epoch != partitionEpoch {
		t.Fatalf("association epoch and partition epoch were conflated: peer %+v raw %+v", changed, raw)
	}
	snapshot := store.snapshot()
	if snapshot.PeerCongestion[0].Epoch != 9 || snapshot.Partitions[0].Epoch != partitionEpoch {
		t.Fatalf("snapshot lost independent epochs: %+v", snapshot)
	}
}

func TestPeerSCONNeverChangesAuthoritativeDAUDState(t *testing.T) {
	endpoint, peer, _, first, _ := peerRetentionHandlerFixture(t)
	seedDestinationRangeInScope(peer.destinations, 0, 1, 123, 0, DestinationNetworkState{Availability: DestinationAvailable})
	capture := new(distributionCapture)
	peer.signalWriter = capture.write
	audit := func() [][]byte {
		t.Helper()
		capture.reset()
		if err := peer.handleDestinationStateAudit(messages.NewDestinationStateAudit(params.NewNetworkAppearance(0), params.NewRoutingContext(1), params.NewAffectedPointCode(123), nil)); err != nil {
			t.Fatal(err)
		}
		var encoded [][]byte
		for _, message := range capture.snapshot() {
			if message.MessageClass() != messages.MsgClassSSNM {
				continue
			}
			if _, available := message.(*messages.DestinationAvailable); !available {
				t.Fatalf("DAUD authority acquired peer congestion: %T", message)
			}
			raw, err := message.MarshalBinary()
			if err != nil {
				t.Fatal(err)
			}
			encoded = append(encoded, raw)
		}
		if len(encoded) != 1 {
			t.Fatalf("DAUD returned %d availability replies, want one", len(encoded))
		}
		return encoded
	}
	before := audit()
	peerRetentionHandleSCON(t, peer, 3, 1)
	after := audit()
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("peer SCON changed authoritative DAUD encoding: %x -> %x", before, after)
	}
	if !peerRetentionHasRecord(endpoint.SSNMKnowledge(), peer.ID(), first) {
		t.Fatal("authority isolation test did not retain peer input")
	}
}

func TestPeerRetentionHandlerDynamicNetworkAppearanceReuse(t *testing.T) {
	endpoint, peer, _, first, second := peerRetentionHandlerFixture(t)
	peer.addDynamicASKey(first, RoutingKey{}, false)
	peerRetentionHandleSCON(t, peer, 2, 1, 2)
	oldScope := WireScope{NetworkAppearanceSet: true, RoutingContextSet: true, RoutingContexts: []uint32{1}}
	oldAdmission := peer.capturePeerCongestionAdmission(oldScope)
	replacement := first
	replacement.NetworkAppearance = 9
	replacement.NetworkAppearanceSet = true
	peer.addDynamicASKey(replacement, RoutingKey{}, false)
	replacementAS := peer.as.get(replacement)
	replacementAS.setTrafficMode(params.TrafficModeLoadshare)
	replacementAS.setASPState(peer, StateASPActive, time.Hour)
	snapshot := endpoint.SSNMKnowledge()
	if peerRetentionHasRecord(snapshot, peer.ID(), first) || !peerRetentionHasRecord(snapshot, peer.ID(), second) {
		t.Fatalf("NA replacement retained old scope or lost sibling: %+v", snapshot.PeerCongestion)
	}
	if err := peer.publishPeerCongestionReport(peerRetentionLateReport(oldScope), oldAdmission); err != nil {
		t.Fatal(err)
	}
	if peerRetentionHasRecord(endpoint.SSNMKnowledge(), peer.ID(), first) {
		t.Fatal("old NA admission survived RC reuse")
	}
	if err := peer.handleSignallingCongestion(messages.NewSignallingCongestion(params.NewNetworkAppearance(9), params.NewRoutingContext(1), params.NewAffectedPointCode(123), nil, params.NewCongestionIndications(1), nil)); err != nil {
		t.Fatalf("fresh replacement-NA SCON: %v", err)
	}
	found := false
	for _, record := range endpoint.SSNMKnowledge().PeerCongestion {
		if record.Association == peer.ID() && record.AS == replacement {
			found = record.Scope.NetworkAppearanceSet && record.Scope.NetworkAppearance == 9
		}
	}
	if !found {
		t.Fatal("fresh replacement NA did not retain resolved AS and wire scope")
	}
}

func TestPeerRetentionMissingPartitionBudgetCountsExpandedReport(t *testing.T) {
	for _, resource := range []string{"partitions", "partition-metadata-bytes"} {
		t.Run(resource, func(t *testing.T) {
			config := &SSNMStateConfig{MaxPartitions: 1}
			if resource == "partition-metadata-bytes" {
				config.MaxPartitions = 16
				config.MaxBytes = 2*ssnmPartitionBaseBytes - 1
			}
			store, report, bindings := peerRetentionFixture(t, config, 1, routingContextASKey(7), routingContextASKey(8))
			authority := report
			authority.PeerReported = false
			authority.Kind = SSNMDestinationUnavailableReport
			authority.Scope = WireScope{}
			if err := store.apply(authority); err != nil {
				t.Fatal(err)
			}
			store.registerSource(2)
			report.Association = 2
			report.Partition = SSNMPartition{Kind: SSNMStandalonePartition, Association: 2}
			report.Destinations = []PointCodeRange{{PointCode: 123}, {PointCode: 0x1200, Mask: 8}, {PointCode: 0x1234, Mask: 8}}
			report.Scope.RoutingContexts = []uint32{7, 8, 7}
			store.syncPeerCongestion(2, 1, bindings)
			before := store.snapshot()
			endpoint := &Endpoint{ssnm: store}
			_, subscription, err := endpoint.SubscribeSSNM()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = subscription.Close() }()
			// Refusal accounts for the complete report before creating a partition.
			if err := store.bindPeerReport(report, bindings); !errors.Is(err, ErrSSNMStateLimit) {
				t.Fatalf("missing partition unexpectedly admitted: %v", err)
			}
			snapshot := store.snapshot()
			if snapshot.RecordsRefused != 4 || snapshot.ReportsRefused != 1 {
				t.Fatalf("missing partition refusal must count 2 AS x 2 unique ranges as one whole report: records=%d reports=%d", snapshot.RecordsRefused, snapshot.ReportsRefused)
			}
			if !reflect.DeepEqual(before.Partitions, snapshot.Partitions) || !reflect.DeepEqual(before.PeerCongestion, snapshot.PeerCongestion) {
				t.Fatalf("metadata refusal changed existing knowledge: before %+v after %+v", before, snapshot)
			}
			event := peerRetentionNext(t, subscription)
			if event.Kind != SSNMResourceLossEvent {
				t.Fatalf("missing resource loss event: %+v", event)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if extra, err := subscription.Next(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("report refusal emitted more than one event: %+v %v", extra, err)
			}
		})
	}
}

func TestPeerRetentionMissingPartitionChecksSourceBeforeBudget(t *testing.T) {
	for _, condition := range []string{"departed-source", "closed-store"} {
		t.Run(condition, func(t *testing.T) {
			store, report, bindings := peerRetentionFixture(t, &SSNMStateConfig{MaxPartitions: 1}, 1, routingContextASKey(7))
			report.Association = 2
			report.Partition = SSNMPartition{Kind: SSNMStandalonePartition, Association: 2}
			expected := ErrAssociationClosed
			if condition == "closed-store" {
				store.registerSource(2)
				store.mu.Lock()
				store.closed = true
				store.mu.Unlock()
				expected = ErrEndpointClosed
			}
			before := store.snapshot()
			if err := store.bindPeerReport(report, bindings); !errors.Is(err, expected) {
				t.Fatalf("lifecycle refusal lost to metadata budget: %v, want %v", err, expected)
			}
			after := store.snapshot()
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("lifecycle refusal consumed report budget: before %+v after %+v", before, after)
			}
		})
	}
}

func TestPeerRetentionReportBindingKeepsExistingAdmissionSemantics(t *testing.T) {
	store, report, bindings := peerRetentionFixture(t, nil, 1, routingContextASKey(7))
	store.registerSource(2)
	report.Association = 2
	report.Partition = SSNMPartition{Kind: SSNMStandalonePartition, Association: 2}
	before := store.snapshot()
	if err := store.bindPeerReport(report, bindings); err != nil {
		t.Fatal(err)
	}
	after := store.snapshot()
	if len(after.Partitions) != 2 || after.Revision != before.Revision+1 || after.Partitions[1].Epoch <= before.Partitions[0].Epoch || len(after.Partitions[1].Bindings) != 1 || after.Partitions[1].Bindings[0] != (SSNMBinding{Association: 2}) || store.bytes != 2*ssnmPartitionBaseBytes {
		t.Fatalf("peer bind changed admission semantics: %+v, bytes %d", after, store.bytes)
	}
	if err := store.bindPeerReport(report, bindings); err != nil {
		t.Fatal(err)
	}
	if repeated := store.snapshot(); !reflect.DeepEqual(after, repeated) {
		t.Fatalf("idempotent report binding republished admission: %+v -> %+v", after, repeated)
	}
}
