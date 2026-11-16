package m3ua

import (
	"cmp"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"
	"time"
)

// The scanReference helpers below freeze the pre-index read paths. They read
// only authoritative maps; they must never call a destination bucket helper.
// Admission, canonical key construction and sorting comparisons stay shared.
func (p *ssnmPartitionState) scanReferenceRetainedKnowledge(key ssnmDestinationKey) SSNMDestinationKnowledge {
	entry := SSNMDestinationKnowledge{Destination: key.pointCodeRange()}
	for source, record := range p.receivedAvailability {
		if source.destination == key {
			entry.AvailabilitySources = append(entry.AvailabilitySources, record)
		}
	}
	slices.SortFunc(entry.AvailabilitySources, func(a, b SSNMAvailability) int {
		if order := cmp.Compare(a.Revision, b.Revision); order != 0 {
			return order
		}
		if order := cmp.Compare(a.Association, b.Association); order != 0 {
			return order
		}
		return compareASKey(a.AS, b.AS)
	})
	for source, record := range p.receivedCongestion {
		if source.destination == key {
			entry.CongestionSources = append(entry.CongestionSources, record)
		}
	}
	slices.SortFunc(entry.CongestionSources, func(a, b SSNMCongestion) int {
		if order := cmp.Compare(a.Revision, b.Revision); order != 0 {
			return order
		}
		if order := cmp.Compare(a.Association, b.Association); order != 0 {
			return order
		}
		return compareASKey(a.AS, b.AS)
	})
	if availability, held := p.availability[key]; held {
		entry.Availability = availability.SSNMAvailability
		entry.AvailabilitySet = true
	}
	if congestion, held := p.congestion[key]; held {
		entry.Congestion = congestion.SSNMCongestion
		entry.CongestionSet = true
	}
	return entry
}

func (s *ssnmState) scanReferencePartitionDestinationsLocked(state *ssnmPartitionState) []SSNMDestinationKnowledge {
	if state == nil || len(state.availability)+len(state.congestion)+len(state.receivedCongestion)+len(state.receivedAvailability) == 0 {
		return nil
	}
	keys := make([]ssnmDestinationKey, 0, len(state.availability)+len(state.congestion)+len(state.receivedCongestion)+len(state.receivedAvailability))
	for key := range state.availability {
		keys = append(keys, key)
	}
	for key := range state.congestion {
		if _, duplicate := state.availability[key]; !duplicate {
			keys = append(keys, key)
		}
	}
	for key := range state.receivedCongestion {
		keys = append(keys, key.destination)
	}
	for key := range state.receivedAvailability {
		keys = append(keys, key.destination)
	}
	slices.SortFunc(keys, compareSSNMDestinationKeys)
	keys = slices.Compact(keys)
	knowledge := make([]SSNMDestinationKnowledge, len(keys))
	for index, key := range keys {
		entry := state.scanReferenceRetainedKnowledge(key)
		entry.Availability.Scope = entry.Availability.Scope.clone()
		entry.Congestion.Scope = entry.Congestion.Scope.clone()
		for i := range entry.AvailabilitySources {
			entry.AvailabilitySources[i].Scope = entry.AvailabilitySources[i].Scope.clone()
		}
		for i := range entry.CongestionSources {
			entry.CongestionSources[i].Scope = entry.CongestionSources[i].Scope.clone()
		}
		knowledge[index] = entry
	}
	return knowledge
}

func (s *ssnmState) scanReferenceDestinationKnowledge(partition SSNMPartition, pointCode uint32, query ...ASKey) aspSelectionStatus {
	if s == nil {
		return aspSelectionStatus{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, exists := s.partitions[partition]
	if !exists {
		return aspSelectionStatus{}
	}
	status := aspSelectionStatus{epoch: state.epoch}
	availabilityRevision := uint64(0)
	congestionRevision := uint64(0)
	for mask := uint8(0); mask <= maxPointCodeMask; mask++ {
		key := ssnmDestinationKey{pointCode: destinationRangePrefix(pointCode, mask), mask: mask}
		if record, held := state.availability[key]; held && scanReferenceScalarApplies(state.partition, record.Scope, record.resolved, query...) &&
			(!status.availabilitySet || record.Revision > availabilityRevision) {
			availabilityRevision = record.Revision
			status.availability = record.State
			status.availabilitySet = true
		}
		if record, held := state.congestion[key]; held && scanReferenceScalarApplies(state.partition, record.Scope, record.resolved, query...) &&
			(!status.congestionSet || record.Revision > congestionRevision) {
			congestionRevision = record.Revision
			status.congestionSet = true
			status.congested = record.Congested
			status.congestionLevel = record.Level
			status.congestionLevelSet = record.LevelSet
		}
	}
	if len(state.receivedAvailability) > 0 {
		status = state.scanReferenceAvailabilityStatus(pointCode, status, query...)
	}
	if len(state.receivedCongestion) > 0 {
		status = state.scanReferenceCongestionStatus(pointCode, status, query...)
	}
	return status
}

func scanReferenceScalarApplies(partition SSNMPartition, scope WireScope, resolved []ASKey, query ...ASKey) bool {
	if len(query) == 0 {
		return true
	}
	if len(resolved) == 0 {
		// Omission without captured resolution cannot establish disjointness.
		// Keep the pre-existing implicit-scope fallback used by direct store inputs.
		return (ssnmDestinationRefusal{resolved: true, keys: query[:1]}).matches(partition, scope, nil)
	}
	for _, as := range resolved {
		if scanReferenceCongestionApplies(SSNMCongestion{AS: as, Scope: scope}, query[0], partition.Kind == SSNMStandalonePartition) {
			return true
		}
	}
	return false
}

func (p *ssnmPartitionState) scanReferenceAvailabilityStatus(pc uint32, status aspSelectionStatus, query ...ASKey) aspSelectionStatus {
	status.availabilitySet = false
	status.availability = DestinationAvailability(0)
	var revision uint64
	for key, record := range p.receivedAvailability {
		if len(query) > 0 && !scanReferenceCongestionApplies(SSNMCongestion{AS: record.AS, Scope: record.Scope}, query[0], p.partition.Kind == SSNMStandalonePartition) {
			continue
		}
		if destinationRangePrefix(pc, key.destination.mask) != key.destination.pointCode || record.Revision <= revision {
			continue
		}
		revision = record.Revision
		status.availabilitySet = true
		status.availability = record.State
	}
	return status
}

func (p *ssnmPartitionState) scanReferenceCongestionStatus(pointCode uint32, status aspSelectionStatus, query ...ASKey) aspSelectionStatus {
	latest := make(map[AssociationID]SSNMCongestion)
	for key, record := range p.receivedCongestion {
		if len(query) > 0 && !scanReferenceCongestionApplies(record, query[0], p.partition.Kind == SSNMStandalonePartition) {
			continue
		}
		if destinationRangePrefix(pointCode, key.destination.mask) != key.destination.pointCode {
			continue
		}
		if old, held := latest[key.association]; !held || record.Revision > old.Revision {
			latest[key.association] = record
		}
	}
	status.congestionSet = false
	status.congested = false
	status.congestionLevel = 0
	status.congestionLevelSet = false
	for _, record := range latest {
		if record.Freshness == CongestionExpired {
			status.receivedUnknown = true
			continue
		}
		status.congestionSet = true
		if record.Congested {
			if !status.congested || status.congestionLevelSet && (!record.LevelSet || record.Level > status.congestionLevel) {
				status.congestionLevel = record.Level
				status.congestionLevelSet = record.LevelSet
			}
			status.congested = true
		} else if !status.congested {
			status.congestionLevelSet = record.LevelSet
		}
	}
	return status
}

func scanReferenceCongestionApplies(record SSNMCongestion, key ASKey, standalone bool) bool {
	if standalone && (record.AS.RoutingContextSet != key.RoutingContextSet || record.AS.RoutingContext != key.RoutingContext) {
		return false
	}
	appearance, set := record.AS.NetworkAppearance, record.AS.NetworkAppearanceSet
	if record.Scope.NetworkAppearanceSet {
		appearance, set = record.Scope.NetworkAppearance, true
	}
	if key.NetworkAppearanceSet {
		return set && appearance == key.NetworkAppearance
	}
	return !standalone || !set
}

// All inputs are store reports. No Endpoint is started and no Association or
// transport is constructed; the Endpoint below only exposes SubscribeSSNM.
func TestDestinationReadsMatchAuthoritativeScans(t *testing.T) {
	for _, mode := range []struct {
		name               string
		validity, auditing bool
	}{
		{"scalar", false, false},
		{"validity", true, false},
		{"auditing", false, true},
		{"validity-and-auditing", true, true},
	} {
		t.Run(mode.name, func(t *testing.T) {
			s, err := newSSNMState(&SSNMStateConfig{SubscriptionQueueSize: 256, SubscriptionQueueBytes: 1 << 20})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(s.close)
			now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			s.now = func() time.Time { return now }
			if mode.validity {
				s.validity = time.Minute
			}
			if mode.auditing {
				s.auditing = &DestinationAuditConfig{}
			}
			partitions := []SSNMPartition{
				{Kind: SSNMStandalonePartition, Association: 1},
				{Kind: SSNMCanonicalPartition, SignallingGateway: "sg-a", ApplicationServer: "as-a"},
				{Kind: SSNMCanonicalPartition, SignallingGateway: "sg-a", ApplicationServer: "as-b"},
				{Kind: SSNMCanonicalPartition, SignallingGateway: "sg-b", ApplicationServer: "as-a"},
			}
			for association := AssociationID(1); association <= 3; association++ {
				s.registerSource(association)
			}
			for _, partition := range partitions {
				for association := AssociationID(1); association <= 3; association++ {
					if err := s.bind(partition, association, association == 3); err != nil {
						t.Fatal(err)
					}
				}
			}
			endpoint := &Endpoint{ssnm: s}
			_, first, err := endpoint.SubscribeSSNM()
			if err != nil {
				t.Fatal(err)
			}
			_, second, err := endpoint.SubscribeSSNM()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = first.Close(); _ = second.Close() })
			lastRevision := s.currentRevision()
			keys := []ASKey{
				{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true},
				{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContext: 2, RoutingContextSet: true},
				{NetworkAppearance: 8, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true},
				{RoutingContext: 1, RoutingContextSet: true},
				{},
			}
			check := func(label string) {
				t.Helper()
				t.Run(label, func(t *testing.T) {
					assertDestinationScans(t, s, partitions, keys)
					for {
						event, ready, err := first.take()
						if err != nil {
							t.Fatal(err)
						}
						if !ready {
							break
						}
						other, otherReady, err := second.take()
						if err != nil || !otherReady || !reflect.DeepEqual(event, other) {
							t.Fatalf("subscriber mismatch: %v, %t", err, otherReady)
						}
						if event.Revision != lastRevision+1 {
							t.Fatalf("revision %d after %d", event.Revision, lastRevision)
						}
						lastRevision = event.Revision
						var want []SSNMDestinationKnowledge
						if event.Kind == SSNMReportObservedEvent {
							s.mu.Lock()
							if state := s.partitions[event.Partition]; state != nil && (event.Report.retainsAvailability() || event.Report.retainsCongestion()) {
								var destinations []ssnmDestinationKey
								for _, d := range event.Report.Destinations {
									destinations = append(destinations, ssnmDestinationKeyFor(d))
								}
								slices.SortFunc(destinations, compareSSNMDestinationKeys)
								for _, d := range slices.Compact(destinations) {
									want = append(want, state.scanReferenceRetainedKnowledge(d))
								}
							}
							s.mu.Unlock()
						}
						if !reflect.DeepEqual(event.DestinationUpdates, want) {
							t.Fatalf("event delta differs from scan\ngot %#v\nwant %#v", event.DestinationUpdates, want)
						}
						// One consumer may freely edit scopes and source slices. The
						// second consumer and store must retain their original data.
						mutateDestinationCopies(event.DestinationUpdates)
						if !reflect.DeepEqual(other.DestinationUpdates, want) {
							t.Fatal("event consumers share mutable destination fields")
						}
					}
					if _, ready, err := second.take(); ready || err != nil {
						t.Fatalf("extra second-subscriber delta: ready=%t err=%v", ready, err)
					}
					if lastRevision != s.currentRevision() {
						t.Fatal("missing event revisions")
					}
					assertDestinationScans(t, s, partitions, keys)
				})
			}
			apply := func(label string, partition SSNMPartition, association AssociationID, kind SSNMReportKind, source SSNMReportSource, scope WireScope, level uint8, levelSet bool, resolved []ASKey, destinations ...PointCodeRange) {
				t.Helper()
				err := s.apply(SSNMReport{Kind: kind, Source: source, Partition: partition, Association: association, Scope: scope, Destinations: destinations, CongestionLevel: level, CongestionLevelSet: levelSet}, resolved)
				if err != nil {
					t.Fatal(err)
				}
				check(label)
			}
			check("empty-bound-partitions")
			for i, partition := range partitions {
				prefix := fmt.Sprintf("partition-%d-", i)
				// Masked aliases and high-bit aliases collapse to canonical ranges.
				apply(prefix+"broad", partition, 1, SSNMDestinationUnavailableReport, SSNMPeerReport, testWireScope(7, true, 1), 0, false, keys[:1], PointCodeRange{PointCode: 0x12abcd, Mask: 8}, PointCodeRange{PointCode: 0xff12ab00, Mask: 8})
				apply(prefix+"fanout", partition, 1, SSNMSignallingCongestionReport, SSNMPeerReport, testWireScope(7, true, 2, 1, 2), 2, true, []ASKey{keys[1], keys[0], keys[1]}, PointCodeRange{PointCode: 0x12ab01}, PointCodeRange{PointCode: 0x12ab00, Mask: 8})
				// A sibling SGP may use another RC label for the same canonical AS.
				apply(prefix+"second-reporter", partition, 2, SSNMSignallingCongestionReport, SSNMPeerReport, testWireScope(7, true, 99), 3, true, []ASKey{{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContext: 99, RoutingContextSet: true}}, PointCodeRange{PointCode: 0x12ab01})
				apply(prefix+"omitted-scope", partition, 3, SSNMDestinationRestrictedReport, SSNMPeerReport, WireScope{}, 0, false, keys[:2], PointCodeRange{PointCode: 0x12ab01})
				apply(prefix+"different-network", partition, 3, SSNMSignallingCongestionReport, SSNMPeerReport, testWireScope(8, true, 1), 1, true, keys[2:3], PointCodeRange{PointCode: 0x12ab01})
				apply(prefix+"implicit-resolution", partition, 1, SSNMSignallingCongestionReport, SSNMPeerReport, WireScope{}, 0, false, nil, PointCodeRange{PointCode: 0x34ab01})
				apply(prefix+"local-scalar", partition, 2, SSNMDestinationAvailableReport, SSNMLocalReport, testWireScope(7, true, 2), 0, false, keys[1:2], PointCodeRange{PointCode: 0x56ab01})
				for _, mask := range []uint8{1, 16, 23} {
					apply(prefix+fmt.Sprintf("mask-%d", mask), partition, 2, SSNMSignallingCongestionReport, SSNMPeerReport, testWireScope(7, true, 1), 1, true, keys[:1], PointCodeRange{PointCode: 0x12ab01, Mask: mask})
				}
				apply(prefix+"max-mask", partition, 2, SSNMDestinationAvailableReport, SSNMPeerReport, testWireScope(7, true, 1), 0, false, keys[:1], PointCodeRange{PointCode: 0xffffff, Mask: 24}, PointCodeRange{PointCode: 0xabcdef, Mask: 255})
			}
			// A refused insertion must not leave a destination index row behind.
			oldMax := s.limits.MaxRecords
			s.limits.MaxRecords = s.records
			err = s.apply(SSNMReport{Kind: SSNMSignallingCongestionReport, Source: SSNMPeerReport, Partition: partitions[0], Association: 1, Scope: testWireScope(7, true, 1), Destinations: []PointCodeRange{{PointCode: 0x987654}}, CongestionLevel: 2, CongestionLevelSet: true}, keys[:1])
			s.limits.MaxRecords = oldMax
			if !errors.Is(err, ErrSSNMStateLimit) {
				t.Fatalf("expected full-store refusal, got %v", err)
			}
			check("refused-insertion")
			now = now.Add(30 * time.Second)
			apply("refresh-source-before-expiry", partitions[0], 1, SSNMSignallingCongestionReport, SSNMPeerReport, testWireScope(7, true, 1), 1, true, keys[:1], PointCodeRange{PointCode: 0x12ab01})
			now = now.Add(31 * time.Second)
			s.expireCongestion(now)
			check("expired-and-fresh-sources")
			apply("explicit-clear-replaces-source", partitions[0], 1, SSNMSignallingCongestionReport, SSNMPeerReport, testWireScope(7, true, 1), 0, true, keys[:1], PointCodeRange{PointCode: 0x12ab01})
			// Retiring the most recent source removes its scalar copy while
			// retaining older reporters' history for the same destination.
			s.retireReceivedEpoch(3, 1)
			check("source-epoch-retired-history-only")
			s.mu.Lock()
			s.invalidateScopedDestinationKnowledgeLocked(s.partitions[partitions[0]], "scoped test refusal", ssnmDestinationRefusal{resolved: true, keys: keys[1:2]})
			s.mu.Unlock()
			check("scoped-invalidation")
			s.retire(partitions[1], 2)
			check("sibling-reporter-retired")
			s.mu.Lock()
			s.invalidateDestinationKnowledgeLocked(s.partitions[partitions[2]], "whole partition test refusal")
			s.mu.Unlock()
			check("whole-partition-invalidation")
			for association := AssociationID(1); association <= 3; association++ {
				s.retire(partitions[3], association)
				check(fmt.Sprintf("retire-binding-%d", association))
			}
			if err := s.bind(partitions[3], 1, false); err != nil {
				t.Fatal(err)
			}
			check("recreated-partition-new-epoch")
			apply("new-generation-report", partitions[3], 1, SSNMSignallingCongestionReport, SSNMPeerReport, testWireScope(7, true, 1), 2, true, keys[:1], PointCodeRange{PointCode: 0x12ab01})
			now = now.Add(time.Minute)
			s.expireCongestion(now)
			check("all-due-sources-expired")
		})
	}
}

// captureScanReadState includes the subscription's undelivered queue and loss
// barrier as well as retained rows, revisions and resource accounting.
func captureScanReadState(s *ssnmState) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	type queueState struct {
		Queue         []queuedSSNMEvent
		Bytes         int
		Lost, Pending bool
	}
	queues := make(map[*SSNMSubscription]queueState)
	for sub := range s.subscribers {
		sub.mu.Lock()
		queue := make([]queuedSSNMEvent, len(sub.queue))
		for i, value := range sub.queue {
			queue[i] = queuedSSNMEvent{event: value.event.clone(), bytes: value.bytes}
		}
		queues[sub] = queueState{queue, sub.queuedBytes, sub.continuityLost, sub.pendingLoss}
		sub.mu.Unlock()
	}
	partitionBytes := make(map[SSNMPartition]int)
	for p, state := range s.partitions {
		partitionBytes[p] = state.bytes
	}
	return struct {
		Snapshot       SSNMSnapshot
		Records, Bytes int
		Epochs         uint64
		PeerRecords    map[ssnmPeerKey]int
		PartitionBytes map[SSNMPartition]int
		Queues         map[*SSNMSubscription]queueState
	}{s.snapshotLocked(), s.records, s.bytes, s.epochs, cloneScanPeerRecords(s.peerRecords), partitionBytes, queues}
}

func cloneScanPeerRecords(values map[ssnmPeerKey]int) map[ssnmPeerKey]int {
	copy := make(map[ssnmPeerKey]int, len(values))
	for key, value := range values {
		copy[key] = value
	}
	return copy
}

func assertDestinationScans(t *testing.T, s *ssnmState, partitions []SSNMPartition, queries []ASKey) {
	t.Helper()
	assertHistoryAccountingAndIndexes(t, s)
	before := captureScanReadState(s)
	s.mu.Lock()
	for _, partition := range partitions {
		state := s.partitions[partition]
		got := s.partitionDestinationsLocked(state)
		want := s.scanReferencePartitionDestinationsLocked(state)
		if !reflect.DeepEqual(got, want) {
			s.mu.Unlock()
			t.Fatalf("snapshot differs for %#v\ngot %#v\nwant %#v", partition, got, want)
		}
		if state != nil {
			for _, row := range want {
				key := ssnmDestinationKeyFor(row.Destination)
				if !reflect.DeepEqual(state.retainedKnowledge(key), state.scanReferenceRetainedKnowledge(key)) {
					s.mu.Unlock()
					t.Fatalf("retained row differs for %#v", key)
				}
			}
		}
		mutateDestinationCopies(got)
		if !reflect.DeepEqual(s.scanReferencePartitionDestinationsLocked(state), want) {
			s.mu.Unlock()
			t.Fatal("snapshot shares mutable destination fields with store")
		}
	}
	s.mu.Unlock()
	for _, partition := range append(slices.Clone(partitions), SSNMPartition{Kind: SSNMStandalonePartition, Association: 999}) {
		for _, pc := range []uint32{0, 0x12aaff, 0x12ab00, 0x12ab01, 0x12abff, 0x12ac00, 0x34ab01, 0x56ab01, 0xffffff} {
			for i := -1; i < len(queries); i++ {
				var query []ASKey
				if i >= 0 {
					query = queries[i : i+1]
				}
				got := s.destinationKnowledge(partition, pc, query...)
				want := s.scanReferenceDestinationKnowledge(partition, pc, query...)
				if got != want {
					t.Fatalf("projection differs: partition=%#v pc=%x query=%#v got=%#v want=%#v", partition, pc, query, got, want)
				}
			}
		}
	}
	if after := captureScanReadState(s); !reflect.DeepEqual(before, after) {
		t.Fatalf("read changed rows/accounting/revision/queue/loss barrier\nbefore %#v\nafter %#v", before, after)
	}
}

func mutateDestinationCopies(rows []SSNMDestinationKnowledge) {
	mutate := func(scope *WireScope) {
		if len(scope.RoutingContexts) > 0 {
			scope.RoutingContexts[0] ^= 0xffffffff
		}
	}
	for i := range rows {
		mutate(&rows[i].Availability.Scope)
		mutate(&rows[i].Congestion.Scope)
		for j := range rows[i].AvailabilitySources {
			mutate(&rows[i].AvailabilitySources[j].Scope)
		}
		for j := range rows[i].CongestionSources {
			mutate(&rows[i].CongestionSources[j].Scope)
		}
		rows[i].AvailabilitySources = nil
		rows[i].CongestionSources = nil
	}
}

func TestDestinationScansPreserveSubscriptionLossBarrier(t *testing.T) {
	s, err := newSSNMState(&SSNMStateConfig{SubscriptionQueueSize: 1, SubscriptionQueueBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.close)
	s.validity = time.Minute
	s.registerSource(1)
	partition := SSNMPartition{Kind: SSNMStandalonePartition, Association: 1}
	if err := s.bind(partition, 1, false); err != nil {
		t.Fatal(err)
	}
	endpoint := &Endpoint{ssnm: s}
	_, sub, err := endpoint.SubscribeSSNM()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	for _, pc := range []uint32{100, 101} {
		if err := s.apply(SSNMReport{Kind: SSNMSignallingCongestionReport, Source: SSNMPeerReport, Partition: partition, Association: 1, Scope: testWireScope(7, true, 1), Destinations: []PointCodeRange{{PointCode: pc}}, CongestionLevel: 2, CongestionLevelSet: true}); err != nil {
			t.Fatal(err)
		}
	}
	sub.mu.Lock()
	lost, pending := sub.continuityLost, sub.pendingLoss
	sub.mu.Unlock()
	if !lost || !pending {
		t.Fatal("setup did not overflow subscription queue")
	}
	assertDestinationScans(t, s, []SSNMPartition{partition}, []ASKey{{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true}})
	if event, ready, err := sub.take(); err != nil || !ready || event.Kind != SSNMReportObservedEvent {
		t.Fatal("queued report lost", event, ready, err)
	}
	if event, ready, err := sub.take(); err != nil || !ready || !event.ContinuityLost {
		t.Fatal("loss marker changed", event, ready, err)
	}
	if _, ready, err := sub.take(); err != nil || ready {
		t.Fatal("unexpected extra event", ready, err)
	}
	if _, err := sub.Resync(); err != nil {
		t.Fatal(err)
	}
}

// The independent charge model is the unchanged published contract, not the
// private mutation helpers. Derived buckets and caches add no logical records.
func assertHistoryAccountingAndIndexes(t *testing.T, s *ssnmState) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	records, bytes := 0, 0
	peers := make(map[ssnmPeerKey]int)
	for _, p := range s.partitions {
		count, cost := 0, 256
		for _, r := range p.availability {
			count++
			cost += 128 + 4*len(r.Scope.RoutingContexts) + 16*len(r.resolved)
		}
		for _, r := range p.congestion {
			count++
			cost += 128 + 4*len(r.Scope.RoutingContexts) + 16*len(r.resolved)
		}
		for key, r := range p.receivedAvailability {
			count++
			cost += 144 + 4*len(r.Scope.RoutingContexts)
			if _, ok := p.availabilitySources[key.destination][key]; !ok {
				t.Fatal("availability bucket missing source")
			}
		}
		for key, r := range p.receivedCongestion {
			count++
			cost += 144 + 4*len(r.Scope.RoutingContexts)
			if _, ok := p.congestionSources[key.destination][key]; !ok {
				t.Fatal("congestion bucket missing source")
			}
		}
		for _, r := range p.peerCongestion {
			count++
			cost += 256 + 8*len(r.Scope.RoutingContexts)
		}
		a, c := 0, 0
		for _, bucket := range p.availabilitySources {
			if len(bucket) == 0 {
				t.Fatal("empty availability bucket retained")
			}
			a += len(bucket)
		}
		for _, bucket := range p.congestionSources {
			if len(bucket) == 0 {
				t.Fatal("empty congestion bucket retained")
			}
			c += len(bucket)
		}
		if a != len(p.receivedAvailability) || c != len(p.receivedCongestion) {
			t.Fatal("orphan source identities")
		}
		if p.sourceProjection != nil {
			nodes := 0
			for _, tree := range p.sourceProjection.trees {
				nodes += len(tree.nodes)
			}
			if nodes > 50*(a+c) || len(p.sourceProjection.trees) > a+c+1 {
				t.Fatal("source projection index exceeds retained-prefix bound")
			}
		}
		if p.records() != count || p.bytes != cost {
			t.Fatalf("partition accounting differs: records%d/%d bytes%d/%d", p.records(), count, p.bytes, cost)
		}
		records += count
		bytes += cost
		if count > 0 {
			peers[p.partition.peerKey()] += count
		}
	}
	for key, job := range s.audits {
		bytes += 256 + len(job.info) + 8*len(job.scope.RoutingContexts) + 144*len(job.bindings) + 64*len(job.partitions)
		if _, held := s.auditByAssociation[key.association][key]; !held {
			t.Fatal("job missing association index")
		}
		if _, held := s.auditBySource[key.sourceKey()][key]; !held {
			t.Fatal("job missing source index")
		}
	}
	indexedA, indexedS := 0, 0
	for _, keys := range s.auditByAssociation {
		indexedA += len(keys)
	}
	for _, keys := range s.auditBySource {
		if len(keys) > 1 {
			t.Fatal("source job index retained more than one live epoch")
		}
		indexedS += len(keys)
	}
	if indexedA != len(s.audits) || indexedS != len(s.audits) {
		t.Fatal("orphan job keys")
	}
	if records != s.records || bytes != s.bytes || !reflect.DeepEqual(peers, s.peerRecords) {
		t.Fatalf("store accounting differs: records%d/%d bytes%d/%d peers%v/%v", s.records, records, s.bytes, bytes, s.peerRecords, peers)
	}
}
