// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"reflect"
	"runtime"
	"slices"
	"sort"
	"testing"
	"time"
)

// An SSNM subscription is a snapshot followed by deltas. A consumer that
// applies every delivered event, in order, to the snapshot it started from
// must hold exactly what the store holds; and an event must carry what it
// changed, not the partition it changed it in, or a one-destination report
// costs as much as the whole partition for every subscriber.

// ssnmEventUpdates is the one place these tests read an event's destination
// delta.
func ssnmEventUpdates(event SSNMEvent) []SSNMDestinationKnowledge { return event.Updated }

// ssnmTestDestinationKey is the canonical identity of an Affected Point Code
// range, computed here independently of the store: the effective mask is at
// most 24 and the masked-out bits of the point code are not part of it. RFC
// 4666 Section 3.4.1: 'a mask of "8" indicates that the last eight bits of the
// PC are "wildcarded"'.
func ssnmTestDestinationKey(destination PointCodeRange) PointCodeRange {
	mask := destination.Mask
	if mask > 24 {
		mask = 24
	}
	pointCode := destination.PointCode & 0x00ffffff
	if mask == 24 {
		pointCode = 0
	} else {
		pointCode &= uint32(0x00ffffff) << mask
	}
	return PointCodeRange{PointCode: pointCode, Mask: mask}
}

func compareSSNMTestDestinations(first, second PointCodeRange) int {
	if comparison := cmp.Compare(first.PointCode, second.PointCode); comparison != 0 {
		return comparison
	}
	return cmp.Compare(first.Mask, second.Mask)
}

// ssnmTestWritesAvailability and ssnmTestWritesCongestion restate which
// reports move which dimension (RFC 4666 Section 4.5.2.2 keeps them apart), so
// the oracle does not borrow the classification it is checking.
func ssnmTestWritesAvailability(report SSNMReport) bool {
	switch report.Kind {
	case SSNMDestinationUnavailableReport, SSNMDestinationAvailableReport, SSNMDestinationRestrictedReport:
		return true
	default:
		return false
	}
}

func ssnmTestWritesCongestion(report SSNMReport) bool {
	return report.Kind == SSNMSignallingCongestionReport && !report.PeerReported
}

type ssnmReplicaPartition struct {
	epoch        uint64
	bindings     map[AssociationID]bool
	destinations map[PointCodeRange]SSNMDestinationKnowledge
}

// ssnmReplica is what a consumer holds: a starting snapshot with every
// delivered event applied in order.
type ssnmReplica struct {
	revision              uint64
	partitions            map[SSNMPartition]*ssnmReplicaPartition
	partitionsInvalidated uint64
	lastResourceLoss      string
}

func newSSNMReplica(snapshot SSNMSnapshot) *ssnmReplica {
	replica := &ssnmReplica{
		revision:              snapshot.Revision,
		partitions:            make(map[SSNMPartition]*ssnmReplicaPartition, len(snapshot.Partitions)),
		partitionsInvalidated: snapshot.PartitionsInvalidated,
		lastResourceLoss:      snapshot.LastResourceLoss,
	}
	for _, knowledge := range snapshot.Partitions {
		partition := &ssnmReplicaPartition{
			epoch:        knowledge.Epoch,
			bindings:     make(map[AssociationID]bool, len(knowledge.Bindings)),
			destinations: make(map[PointCodeRange]SSNMDestinationKnowledge, len(knowledge.Destinations)),
		}
		for _, binding := range knowledge.Bindings {
			partition.bindings[binding.Association] = binding.Pending
		}
		for _, destination := range knowledge.Destinations {
			partition.destinations[destination.Destination] = destination
		}
		replica.partitions[knowledge.Partition] = partition
	}
	return replica
}

// apply folds one delivered event into the replica, checking on the way that
// the event carries exactly what it changed.
func (r *ssnmReplica) apply(event SSNMEvent) error {
	if event.Revision != r.revision+1 {
		return fmt.Errorf("revision %d does not immediately follow %d", event.Revision, r.revision)
	}
	r.revision = event.Revision
	updates := ssnmEventUpdates(event)
	if event.Kind != SSNMReportEvent && len(updates) != 0 {
		return fmt.Errorf("%v event carries %d destination states, but only a report writes destination knowledge",
			event.Kind, len(updates))
	}
	partition := r.partitions[event.Partition]
	switch event.Kind {
	case SSNMReportEvent:
		if err := checkSSNMReportDelta(event, partition); err != nil {
			return err
		}
		for _, update := range updates {
			partition.destinations[update.Destination] = update
		}
	case SSNMBindingAdmittedEvent:
		if partition == nil {
			partition = &ssnmReplicaPartition{
				epoch:        event.Epoch,
				bindings:     make(map[AssociationID]bool),
				destinations: make(map[PointCodeRange]SSNMDestinationKnowledge),
			}
			r.partitions[event.Partition] = partition
		} else if partition.epoch != event.Epoch {
			return fmt.Errorf("admission under epoch %d into a partition of epoch %d", event.Epoch, partition.epoch)
		}
		partition.bindings[event.Binding.Association] = event.Binding.Pending
	case SSNMBindingActivatedEvent:
		if partition == nil || !partition.bindings[event.Binding.Association] || event.Binding.Pending {
			return errors.New("activation of a binding that was not pending")
		}
		partition.bindings[event.Binding.Association] = false
	case SSNMBindingRetiredEvent:
		if partition == nil || len(partition.bindings) < 2 {
			return errors.New("binding retired from a partition without a surviving sibling")
		}
		if _, bound := partition.bindings[event.Binding.Association]; !bound {
			return errors.New("retired binding was not held")
		}
		delete(partition.bindings, event.Binding.Association)
	case SSNMPartitionRetiredEvent:
		if partition == nil {
			return errors.New("retired an unknown partition")
		}
		delete(r.partitions, event.Partition)
		r.partitionsInvalidated++
	case SSNMPartitionInvalidatedEvent:
		if partition == nil {
			return errors.New("invalidated an unknown partition")
		}
		clear(partition.destinations)
		r.partitionsInvalidated++
	case SSNMResourceLossEvent:
		r.lastResourceLoss = event.Reason
	default:
		return fmt.Errorf("unexpected %v event in a delta stream", event.Kind)
	}
	return nil
}

// checkSSNMReportDelta holds a report event to its contract: it names every
// destination the report wrote and nothing else, once each, in point-code then
// mask order; the written dimension carries this revision; the other dimension
// is the one already retained.
func checkSSNMReportDelta(event SSNMEvent, partition *ssnmReplicaPartition) error {
	if !event.ReportSet {
		return errors.New("report event without a report")
	}
	report := event.Report
	if report.Revision != event.Revision {
		return fmt.Errorf("report revision %d, event revision %d", report.Revision, event.Revision)
	}
	writesAvailability := ssnmTestWritesAvailability(report)
	writesCongestion := ssnmTestWritesCongestion(report)
	written := make(map[PointCodeRange]struct{}, len(report.Destinations))
	if partition != nil && (writesAvailability || writesCongestion) {
		if event.Epoch != partition.epoch {
			return fmt.Errorf("report under epoch %d into a partition of epoch %d", event.Epoch, partition.epoch)
		}
		for _, destination := range report.Destinations {
			written[ssnmTestDestinationKey(destination)] = struct{}{}
		}
	}
	updates := ssnmEventUpdates(event)
	if len(updates) != len(written) {
		return fmt.Errorf("%s report wrote %d destinations, event carries %d", report.Kind, len(written), len(updates))
	}
	for index, update := range updates {
		if _, wrote := written[update.Destination]; !wrote {
			return fmt.Errorf("event carries %+v, which the report did not write", update.Destination)
		}
		if index > 0 && compareSSNMTestDestinations(updates[index-1].Destination, update.Destination) >= 0 {
			return fmt.Errorf("destinations %+v and %+v are not in strict point-code then mask order",
				updates[index-1].Destination, update.Destination)
		}
		previous, held := partition.destinations[update.Destination]
		if writesAvailability {
			if !update.AvailabilitySet || update.Availability.Revision != event.Revision {
				return fmt.Errorf("%+v does not carry the availability the report wrote", update.Destination)
			}
			if update.CongestionSet != (held && previous.CongestionSet) ||
				!reflect.DeepEqual(normalizedSSNMCongestion(update.Congestion), normalizedSSNMCongestion(previous.Congestion)) {
				return fmt.Errorf("%+v does not carry its retained congestion", update.Destination)
			}
		}
		if writesCongestion {
			if !update.CongestionSet || update.Congestion.Revision != event.Revision {
				return fmt.Errorf("%+v does not carry the congestion the report wrote", update.Destination)
			}
			if update.AvailabilitySet != (held && previous.AvailabilitySet) ||
				!reflect.DeepEqual(normalizedSSNMAvailability(update.Availability), normalizedSSNMAvailability(previous.Availability)) {
				return fmt.Errorf("%+v does not carry its retained availability", update.Destination)
			}
		}
	}
	return nil
}

func normalizedSSNMScope(scope WireScope) WireScope {
	if len(scope.RoutingContexts) == 0 {
		scope.RoutingContexts = nil
	}
	return scope
}

func normalizedSSNMAvailability(availability SSNMAvailability) SSNMAvailability {
	availability.Scope = normalizedSSNMScope(availability.Scope)
	return availability
}

func normalizedSSNMCongestion(congestion SSNMCongestion) SSNMCongestion {
	congestion.Scope = normalizedSSNMScope(congestion.Scope)
	return congestion
}

// ssnmTestKnowledge is the part of a snapshot a delta stream is able to
// reproduce. RecordsRefused and ReportsRefused are store diagnostics counted
// in records and in reports; a resource-loss event reports the loss and its
// reason, not those tallies, so neither is compared.
type ssnmTestKnowledge struct {
	Revision              uint64
	Partitions            []SSNMPartitionKnowledge
	PartitionsInvalidated uint64
	LastResourceLoss      string
}

func canonicalSSNMTestKnowledge(snapshot SSNMSnapshot) ssnmTestKnowledge {
	knowledge := ssnmTestKnowledge{
		Revision:              snapshot.Revision,
		PartitionsInvalidated: snapshot.PartitionsInvalidated,
		LastResourceLoss:      snapshot.LastResourceLoss,
	}
	for _, partition := range snapshot.Partitions {
		partition.Bindings = slices.Clone(partition.Bindings)
		sort.Slice(partition.Bindings, func(i, j int) bool {
			return partition.Bindings[i].Association < partition.Bindings[j].Association
		})
		destinations := make([]SSNMDestinationKnowledge, 0, len(partition.Destinations))
		for _, destination := range partition.Destinations {
			destination.Availability = normalizedSSNMAvailability(destination.Availability)
			destination.Congestion = normalizedSSNMCongestion(destination.Congestion)
			destinations = append(destinations, destination)
		}
		slices.SortFunc(destinations, func(first, second SSNMDestinationKnowledge) int {
			return compareSSNMTestDestinations(first.Destination, second.Destination)
		})
		partition.Destinations = nil
		if len(destinations) > 0 {
			partition.Destinations = destinations
		}
		if len(partition.Bindings) == 0 {
			partition.Bindings = nil
		}
		knowledge.Partitions = append(knowledge.Partitions, partition)
	}
	sort.Slice(knowledge.Partitions, func(i, j int) bool {
		return lessSSNMPartition(knowledge.Partitions[i].Partition, knowledge.Partitions[j].Partition)
	})
	return knowledge
}

func (r *ssnmReplica) knowledge() ssnmTestKnowledge {
	snapshot := SSNMSnapshot{
		Revision:              r.revision,
		PartitionsInvalidated: r.partitionsInvalidated,
		LastResourceLoss:      r.lastResourceLoss,
	}
	for key, partition := range r.partitions {
		knowledge := SSNMPartitionKnowledge{Partition: key, Epoch: partition.epoch}
		for association, pending := range partition.bindings {
			knowledge.Bindings = append(knowledge.Bindings, SSNMBinding{Association: association, Pending: pending})
			knowledge.TrafficAuthorized = knowledge.TrafficAuthorized || !pending
		}
		for _, destination := range partition.destinations {
			knowledge.Destinations = append(knowledge.Destinations, destination)
		}
		snapshot.Partitions = append(snapshot.Partitions, knowledge)
	}
	return canonicalSSNMTestKnowledge(snapshot)
}

// describeSSNMKnowledgeDifference names the first difference instead of
// printing two whole stores.
func describeSSNMKnowledgeDifference(got, want ssnmTestKnowledge) string {
	if got.Revision != want.Revision {
		return fmt.Sprintf("revision %d, store %d", got.Revision, want.Revision)
	}
	if got.PartitionsInvalidated != want.PartitionsInvalidated || got.LastResourceLoss != want.LastResourceLoss {
		return fmt.Sprintf("diagnostics %d %q, store %d %q",
			got.PartitionsInvalidated, got.LastResourceLoss, want.PartitionsInvalidated, want.LastResourceLoss)
	}
	if len(got.Partitions) != len(want.Partitions) {
		return fmt.Sprintf("%d partitions, store %d", len(got.Partitions), len(want.Partitions))
	}
	for index := range got.Partitions {
		first, second := got.Partitions[index], want.Partitions[index]
		if first.Partition != second.Partition || first.Epoch != second.Epoch ||
			first.TrafficAuthorized != second.TrafficAuthorized || !reflect.DeepEqual(first.Bindings, second.Bindings) {
			return fmt.Sprintf("partition %+v epoch %d bindings %+v, store %+v epoch %d bindings %+v",
				first.Partition, first.Epoch, first.Bindings, second.Partition, second.Epoch, second.Bindings)
		}
		if len(first.Destinations) != len(second.Destinations) {
			return fmt.Sprintf("partition %+v holds %d destinations, store %d",
				first.Partition, len(first.Destinations), len(second.Destinations))
		}
		for destination := range first.Destinations {
			if !reflect.DeepEqual(first.Destinations[destination], second.Destinations[destination]) {
				return fmt.Sprintf("partition %+v destination\n got %+v\nwant %+v",
					first.Partition, first.Destinations[destination], second.Destinations[destination])
			}
		}
	}
	return "equal"
}

// ssnmDeltaConsumer is one subscriber folding its stream into a replica.
type ssnmDeltaConsumer struct {
	name         string
	subscription *SSNMSubscription
	replica      *ssnmReplica
	events       int
	resyncs      int
	kinds        map[SSNMEventKind]int
	coverage     map[string]int
}

func newSSNMDeltaConsumer(t *testing.T, name string, endpoint *Endpoint) *ssnmDeltaConsumer {
	t.Helper()
	snapshot, subscription, err := endpoint.SubscribeSSNM()
	if err != nil {
		t.Fatalf("%s: SubscribeSSNM: %v", name, err)
	}
	t.Cleanup(func() { _ = subscription.Close() })
	return &ssnmDeltaConsumer{
		name:         name,
		subscription: subscription,
		replica:      newSSNMReplica(snapshot),
		kinds:        make(map[SSNMEventKind]int),
		coverage:     make(map[string]int),
	}
}

// drain applies every queued event without blocking, through the public Next,
// and recovers continuity with Resync exactly as an application would.
func (c *ssnmDeltaConsumer) drain(t *testing.T, operation string) {
	t.Helper()
	ready, cancel := context.WithCancel(context.Background())
	cancel()
	for {
		event, err := c.subscription.Next(ready)
		if errors.Is(err, context.Canceled) {
			return
		}
		if err != nil {
			t.Fatalf("%s after %s: Next: %v", c.name, operation, err)
		}
		c.kinds[event.Kind]++
		if event.ContinuityLost {
			snapshot, err := c.subscription.Resync()
			if err != nil {
				t.Fatalf("%s after %s: Resync: %v", c.name, operation, err)
			}
			c.replica = newSSNMReplica(snapshot)
			c.resyncs++
			continue
		}
		c.events++
		if err := c.replica.apply(event); err != nil {
			t.Fatalf("%s after %s: %v event at revision %d: %v", c.name, operation, event.Kind, event.Revision, err)
		}
		c.noteCoverage(event)
	}
}

// noteCoverage records which shapes of delta the stream actually exercised,
// so a run that never reached one cannot pass silently.
func (c *ssnmDeltaConsumer) noteCoverage(event SSNMEvent) {
	updates := ssnmEventUpdates(event)
	if len(updates) == 0 {
		return
	}
	if len(updates) > 1 {
		c.coverage["multi-destination delta"]++
	}
	if len(updates) < len(event.Report.Destinations) {
		c.coverage["duplicate APCs folded"]++
	}
	partition := c.replica.partitions[event.Partition]
	for _, update := range updates {
		if update.AvailabilitySet && update.CongestionSet {
			c.coverage["update carrying both dimensions"]++
		}
		for key := range partition.destinations {
			if key != update.Destination && ssnmTestRangesOverlap(key, update.Destination) {
				c.coverage["update overlapping a retained range"]++
				break
			}
		}
	}
}

func ssnmTestRangesOverlap(first, second PointCodeRange) bool {
	wider := max(first.Mask, second.Mask)
	return ssnmTestDestinationKey(PointCodeRange{PointCode: first.PointCode, Mask: wider}) ==
		ssnmTestDestinationKey(PointCodeRange{PointCode: second.PointCode, Mask: wider})
}

func (c *ssnmDeltaConsumer) requireStore(t *testing.T, endpoint *Endpoint, operation string) {
	t.Helper()
	got, want := c.replica.knowledge(), canonicalSSNMTestKnowledge(endpoint.SSNMKnowledge())
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s after %s: replica diverged from SSNMKnowledge: %s",
			c.name, operation, describeSSNMKnowledgeDifference(got, want))
	}
}

// ssnmDeltaWorld drives randomized store operations.
type ssnmDeltaWorld struct {
	random     *rand.Rand
	store      *ssnmState
	partitions []SSNMPartition
	coverage   map[string]int
}

var ssnmDeltaPartitions = []SSNMPartition{
	{Kind: SSNMCanonicalPartition, SignallingGateway: "sg-a", ApplicationServer: "as-core"},
	{Kind: SSNMCanonicalPartition, SignallingGateway: "sg-a", ApplicationServer: "as-edge"},
	{Kind: SSNMCanonicalPartition, SignallingGateway: "sg-b", ApplicationServer: "as-core"},
	{Kind: SSNMStandalonePartition, Association: 1},
	{Kind: SSNMStandalonePartition, Association: 2},
}

func (w *ssnmDeltaWorld) partition() SSNMPartition {
	return w.partitions[w.random.IntN(len(w.partitions))]
}

// association returns an Association that may bind the partition: a
// standalone partition belongs to its own Association.
func (w *ssnmDeltaWorld) association(partition SSNMPartition) AssociationID {
	if partition.Kind == SSNMStandalonePartition {
		return partition.Association
	}
	return AssociationID(1 + w.random.IntN(3))
}

func (w *ssnmDeltaWorld) destination() PointCodeRange {
	prefixes := []uint32{0x010000, 0x010100, 0x020000, 0x3fff00}
	members := []uint32{0x00, 0x01, 0x80}
	masks := []uint8{0, 0, 0, 1, 8, 8, 16, 24, 30}
	pointCode := prefixes[w.random.IntN(len(prefixes))] | members[w.random.IntN(len(members))]
	if w.random.IntN(16) == 0 {
		pointCode |= 0x7f000000
	}
	return PointCodeRange{PointCode: pointCode, Mask: masks[w.random.IntN(len(masks))]}
}

func (w *ssnmDeltaWorld) report() SSNMReport {
	kinds := []SSNMReportKind{
		SSNMDestinationUnavailableReport, SSNMDestinationUnavailableReport,
		SSNMDestinationAvailableReport, SSNMDestinationAvailableReport,
		SSNMDestinationRestrictedReport,
		SSNMSignallingCongestionReport, SSNMSignallingCongestionReport, SSNMSignallingCongestionReport,
		SSNMDestinationUserPartUnavailableReport,
		SSNMDestinationStateAuditReport,
	}
	partition := w.partition()
	report := SSNMReport{
		Kind:        kinds[w.random.IntN(len(kinds))],
		Source:      SSNMPeerReport,
		Partition:   partition,
		Association: w.association(partition),
		Epoch:       uint64(w.random.IntN(3)),
	}
	if w.random.IntN(8) == 0 {
		report.Source = SSNMLocalReport
	}
	if w.random.IntN(2) == 0 {
		report.Scope.NetworkAppearance, report.Scope.NetworkAppearanceSet = 7, true
	}
	if contexts := w.random.IntN(4); contexts > 0 {
		report.Scope.RoutingContextSet = true
		for range contexts {
			report.Scope.RoutingContexts = append(report.Scope.RoutingContexts, []uint32{1, 2, 3, 0xffffffff}[w.random.IntN(4)])
		}
	}
	count := 1
	switch roll := w.random.IntN(10); {
	case roll >= 9:
		count = 5 + w.random.IntN(8)
	case roll >= 7:
		count = 2 + w.random.IntN(3)
	}
	for range count {
		report.Destinations = append(report.Destinations, w.destination())
	}
	switch report.Kind {
	case SSNMSignallingCongestionReport:
		report.PeerReported = w.random.IntN(5) == 0
		if w.random.IntN(3) != 0 {
			report.CongestionLevel, report.CongestionLevelSet = uint8(w.random.IntN(4)), true
		}
	case SSNMDestinationUserPartUnavailableReport:
		report.UserCause, report.UserCauseSet = uint32(w.random.IntN(1<<20)), true
	}
	return report
}

func (w *ssnmDeltaWorld) note(label string) { w.coverage[label]++ }

// step performs one randomized operation and describes it.
func (w *ssnmDeltaWorld) step() string {
	switch roll := w.random.IntN(100); {
	case roll < 50:
		report := w.report()
		err := w.store.apply(report)
		if errors.Is(err, ErrSSNMStateLimit) {
			w.note("report refused")
		} else if err != nil {
			panic(err)
		}
		if len(report.Destinations) > 1 {
			w.note("multi-APC report")
		}
		return fmt.Sprintf("apply %s of %d APCs to %+v", report.Kind, len(report.Destinations), report.Partition)
	case roll < 72:
		partition := w.partition()
		association := w.association(partition)
		pending := w.random.IntN(3) == 0
		if err := w.store.bind(partition, association, pending); errors.Is(err, ErrSSNMStateLimit) {
			w.note("binding refused")
		} else if err != nil {
			panic(err)
		}
		return fmt.Sprintf("bind %d to %+v pending=%v", association, partition, pending)
	case roll < 88:
		partition := w.partition()
		association := w.association(partition)
		w.store.retire(partition, association)
		return fmt.Sprintf("retire %d from %+v", association, partition)
	case roll < 93:
		association := AssociationID(1 + w.random.IntN(3))
		w.store.retireAssociation(association)
		return fmt.Sprintf("retire Association %d", association)
	default:
		var partitions []SSNMPartition
		for _, partition := range w.partitions {
			if w.random.IntN(3) == 0 {
				partitions = append(partitions, partition)
			}
		}
		association := AssociationID(1 + w.random.IntN(3))
		if err := w.store.oversized(partitions, 2000, 1024, association); !errors.Is(err, ErrSSNMOversizedReport) {
			panic(err)
		}
		return fmt.Sprintf("oversized SSNM for %d partitions", len(partitions))
	}
}

// TestSSNMEventDeltasReplayToKnowledge is the delta oracle. Randomized reports
// in both dimensions, with overlapping and duplicate ranges and multi-APC
// messages, interleave with admissions, activations, retirements,
// invalidations and resource refusals. Three consumers follow the stream: one
// drains after every operation, one subscribes part-way through, and one is
// slow enough to lose continuity and Resync. Each must reproduce
// SSNMKnowledge exactly whenever it has drained.
func TestSSNMEventDeltasReplayToKnowledge(t *testing.T) {
	configurations := []struct {
		name   string
		limits SSNMStateConfig
	}{
		{"record limits", SSNMStateConfig{
			MaxRecords: 30, MaxRecordsPerPeer: 20, MaxRecordsPerPartition: 12, MaxPartitions: 4,
			SubscriptionQueueSize: 12,
		}},
		{"byte limits", SSNMStateConfig{
			MaxBytes: 4096, SubscriptionQueueSize: 64, SubscriptionQueueBytes: 6 << 10,
		}},
		{"defaults", SSNMStateConfig{SubscriptionQueueSize: 16}},
	}
	operations := 3000
	if testing.Short() {
		operations = 600
	}
	for _, configuration := range configurations {
		for seed := uint64(1); seed <= 6; seed++ {
			t.Run(fmt.Sprintf("%s/seed=%d", configuration.name, seed), func(t *testing.T) {
				limits := configuration.limits
				endpoint := newSSNMStateEndpoint(t, nil, &limits)
				world := &ssnmDeltaWorld{
					random:     rand.New(rand.NewPCG(seed, 0x55a1)),
					store:      endpoint.ssnm,
					partitions: ssnmDeltaPartitions,
					coverage:   make(map[string]int),
				}
				healthy := newSSNMDeltaConsumer(t, "healthy", endpoint)
				slow := newSSNMDeltaConsumer(t, "slow", endpoint)
				var late *ssnmDeltaConsumer
				slowDrainAt := 1 + world.random.IntN(40)
				for index := range operations {
					operation := fmt.Sprintf("operation %d (%s)", index, world.step())
					if index == operations/3 {
						late = newSSNMDeltaConsumer(t, "late", endpoint)
					}
					healthy.drain(t, operation)
					healthy.requireStore(t, endpoint, operation)
					if late != nil {
						late.drain(t, operation)
						late.requireStore(t, endpoint, operation)
					}
					if index == slowDrainAt {
						slow.drain(t, operation)
						slow.requireStore(t, endpoint, operation)
						slowDrainAt = index + 1 + world.random.IntN(40)
					}
				}
				if healthy.resyncs != 0 || (late != nil && late.resyncs != 0) {
					t.Fatalf("a draining consumer lost continuity: healthy %d, late %d", healthy.resyncs, late.resyncs)
				}
				for _, kind := range []SSNMEventKind{
					SSNMReportEvent, SSNMBindingAdmittedEvent, SSNMBindingActivatedEvent, SSNMBindingRetiredEvent,
					SSNMPartitionRetiredEvent, SSNMPartitionInvalidatedEvent, SSNMResourceLossEvent,
				} {
					if healthy.kinds[kind] == 0 {
						t.Errorf("the run never exercised a %v event", kind)
					}
				}
				for _, label := range []string{
					"multi-destination delta", "duplicate APCs folded",
					"update carrying both dimensions", "update overlapping a retained range",
				} {
					if healthy.coverage[label] == 0 {
						t.Errorf("the run never exercised a %s", label)
					}
				}
				if configuration.name != "defaults" && (world.coverage["report refused"] == 0 || world.coverage["binding refused"] == 0) {
					t.Errorf("the run did not refuse both a report and a binding for a resource bound: %v", world.coverage)
				}
				if slow.resyncs == 0 {
					t.Error("the slow consumer never lost continuity")
				}
				t.Logf("events: healthy %d, late %d, slow %d with %d Resyncs; operations %v; deltas %v",
					healthy.events, late.events, slow.events, slow.resyncs, world.coverage, healthy.coverage)
			})
		}
	}
}

// The same oracle through the Association: real DUNA, DAVA, DRST and SCON
// messages with masked Affected Point Codes arrive over SGPs of two Signalling
// Gateways, so canonical partitions shared by sibling SGPs and association
// loss are exercised on the path production takes.
func TestSSNMEventDeltasReplayThroughAssociations(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), &SSNMStateConfig{SubscriptionQueueSize: 8})
	healthy := newSSNMDeltaConsumer(t, "healthy", endpoint)
	slow := newSSNMDeltaConsumer(t, "slow", endpoint)
	random := rand.New(rand.NewPCG(7, 0x55a1))
	type peer struct {
		identity SGPIdentity
		contexts []uint32
	}
	peers := []peer{
		{SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}, []uint32{1, 3}},
		{SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a2"}, []uint32{2}},
		{SGPIdentity{SignallingGateway: "sg-b", SignallingGatewayProcess: "sgp-b1"}, []uint32{1}},
	}
	associations := make([]*Association, len(peers))
	attach := func(index int) {
		associations[index] = attachSSNMAssociation(t, endpoint, peers[index].identity, 7, peers[index].contexts...)
	}
	for index := range peers {
		attach(index)
	}
	pointCodes := func() []uint32 {
		codes := make([]uint32, 1+random.IntN(3))
		for index := range codes {
			mask := []uint32{0, 0, 3, 8}[random.IntN(4)]
			codes[index] = mask<<24 | []uint32{0x123400, 0x123456, 0x123480, 0x223344}[random.IntN(4)]
		}
		return codes
	}
	for index := range 600 {
		which := random.IntN(len(peers))
		association := associations[which]
		contexts := peers[which].contexts
		context := contexts[random.IntN(len(contexts))]
		operation := fmt.Sprintf("operation %d on %s", index, peers[which].identity.SignallingGatewayProcess)
		switch roll := random.IntN(20); {
		case roll < 5:
			sendDUNA(t, association, 7, context, pointCodes()...)
		case roll < 9:
			sendDAVA(t, association, 7, context, pointCodes()...)
		case roll < 11:
			sendDRST(t, association, 7, context, pointCodes()...)
		case roll < 17:
			var level *uint8
			if random.IntN(3) != 0 {
				level = congestionLevel(uint8(random.IntN(4)))
			}
			sendSCON(t, association, 7, context, level, pointCodes()...)
		default:
			_ = association.Close()
			endpoint.forgetAssociation(association)
			attach(which)
			operation += " (association replaced)"
		}
		healthy.drain(t, operation)
		healthy.requireStore(t, endpoint, operation)
		if index%25 == 24 {
			slow.drain(t, operation)
			slow.requireStore(t, endpoint, operation)
		}
	}
	if healthy.kinds[SSNMReportEvent] == 0 || healthy.kinds[SSNMPartitionRetiredEvent] == 0 ||
		healthy.kinds[SSNMBindingAdmittedEvent] == 0 || slow.resyncs == 0 {
		t.Fatalf("scenario did not exercise reports, retirement, admission and Resync: %v, %d Resyncs",
			healthy.kinds, slow.resyncs)
	}
}

// ssnmPublicationFixture is a partition holding the given number of retained
// dimension records, half availability and half congestion, over overlapping
// ranges: every other destination is a /8 range covering the one before it.
// Subscribers are opened after the fill, so the fill is not charged to them.
type ssnmPublicationFixture struct {
	store         *ssnmState
	subscriptions []*SSNMSubscription
	reports       []SSNMReport
	next          int
}

func newSSNMPublicationFixture(tb testing.TB, records, subscribers int) *ssnmPublicationFixture {
	tb.Helper()
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, SSNMState: &SSNMStateConfig{
		MaxRecords: 16384, MaxRecordsPerPeer: 16384, MaxRecordsPerPartition: 16384, MaxBytes: 16 << 20,
		MaxSubscribers: subscribers,
		// Large enough to queue the whole-partition events of a store that
		// published them, so such a store is measured rather than dropped.
		SubscriptionQueueBytes: 64 << 20,
	}})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = endpoint.Close() })
	partition := SSNMPartition{Kind: SSNMCanonicalPartition, SignallingGateway: "sg-a", ApplicationServer: "as-core"}
	if err := endpoint.ssnm.bind(partition, 1, false); err != nil {
		tb.Fatal(err)
	}
	scope := WireScope{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContexts: []uint32{1}, RoutingContextSet: true}
	destinations := make([]PointCodeRange, records/2)
	for index := range destinations {
		mask := uint8(0)
		if index%2 != 0 {
			mask = 8
		}
		destinations[index] = PointCodeRange{PointCode: 0x100000 + uint32(index/2)*256, Mask: mask}
	}
	report := func(kind SSNMReportKind, targets []PointCodeRange, level uint8) SSNMReport {
		return SSNMReport{
			Kind: kind, Source: SSNMPeerReport, Scope: scope, Partition: partition, Association: 1,
			Destinations: targets, CongestionLevel: level, CongestionLevelSet: kind == SSNMSignallingCongestionReport,
		}
	}
	for _, fill := range []SSNMReport{
		report(SSNMDestinationUnavailableReport, destinations, 0),
		report(SSNMSignallingCongestionReport, destinations, 1),
	} {
		if err := endpoint.ssnm.apply(fill); err != nil {
			tb.Fatal(err)
		}
	}
	snapshot := endpoint.SSNMKnowledge()
	if held := len(snapshot.Partitions[0].Destinations); held != records/2 {
		tb.Fatalf("fixture holds %d destinations, want %d", held, records/2)
	}
	fixture := &ssnmPublicationFixture{
		store: endpoint.ssnm,
		// One APC at a time, alternating dimensions and alternating between a
		// point and the /8 range covering it.
		reports: []SSNMReport{
			report(SSNMDestinationUnavailableReport, destinations[0:1], 0),
			report(SSNMSignallingCongestionReport, destinations[1:2], 2),
			report(SSNMDestinationAvailableReport, destinations[0:1], 0),
			report(SSNMSignallingCongestionReport, destinations[1:2], 0),
			report(SSNMDestinationRestrictedReport, destinations[1:2], 0),
			report(SSNMSignallingCongestionReport, destinations[0:1], 3),
		},
	}
	for range subscribers {
		_, subscription, err := endpoint.SubscribeSSNM()
		if err != nil {
			tb.Fatal(err)
		}
		tb.Cleanup(func() { _ = subscription.Close() })
		fixture.subscriptions = append(fixture.subscriptions, subscription)
	}
	return fixture
}

// publish applies one one-APC report and has every subscriber take its event.
// Taking allocates nothing; it is included because a subscriber that never
// took would stop costing anything once its bounded queue had overflowed.
func (f *ssnmPublicationFixture) publish(ctx context.Context) {
	if err := f.store.apply(f.reports[f.next%len(f.reports)]); err != nil {
		panic(err)
	}
	f.next++
	for _, subscription := range f.subscriptions {
		event, err := subscription.Next(ctx)
		if err != nil || event.Kind != SSNMReportEvent {
			panic(fmt.Sprintf("subscriber missed the report: %v %v", event.Kind, err))
		}
	}
}

// measureSSNMOneAPCPublication reports the allocations and bytes one
// one-APC report costs from the validated report to its owned delivery to
// every subscriber, averaged over many reports after a warm-up.
func measureSSNMOneAPCPublication(t *testing.T, records, subscribers int) (allocations, bytes float64, p99 time.Duration) {
	t.Helper()
	fixture := newSSNMPublicationFixture(t, records, subscribers)
	ctx := context.Background()
	for range 64 {
		fixture.publish(ctx)
	}
	const runs = 256
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range runs {
		fixture.publish(ctx)
	}
	runtime.ReadMemStats(&after)
	allocations = float64(after.Mallocs-before.Mallocs) / runs
	bytes = float64(after.TotalAlloc-before.TotalAlloc) / runs

	durations := make([]time.Duration, 1000)
	for index := range durations {
		start := time.Now()
		fixture.publish(ctx)
		durations[index] = time.Since(start)
	}
	slices.Sort(durations)
	return allocations, bytes, durations[len(durations)*99/100]
}

// Applying and publishing a one-APC SSNM is budgeted at 32 allocations and
// 8 KiB per message. The measurement runs from the validated report to its
// committed state and owned delivery to each of 8 subscribers, against a
// partition of 16 and of 16,384 retained records; a delta costs what it
// changed, so the two must be the same.
func TestSSNMOneAPCPublicationCostIsIndependentOfPartitionSize(t *testing.T) {
	const (
		subscribers    = 8
		maxAllocations = 32
		maxBytes       = 8 << 10
	)
	smallAllocations, smallBytes, smallP99 := measureSSNMOneAPCPublication(t, 16, subscribers)
	largeAllocations, largeBytes, largeP99 := measureSSNMOneAPCPublication(t, 16384, subscribers)
	t.Logf("16 records: %.2f allocations, %.0f bytes per report, p99 %v", smallAllocations, smallBytes, smallP99)
	t.Logf("16384 records: %.2f allocations, %.0f bytes per report, p99 %v", largeAllocations, largeBytes, largeP99)
	for _, measured := range []struct {
		records            int
		allocations, bytes float64
	}{{16, smallAllocations, smallBytes}, {16384, largeAllocations, largeBytes}} {
		if measured.allocations > maxAllocations || measured.bytes > maxBytes {
			t.Errorf("%d records: %.2f allocations and %.0f bytes per one-APC report with %d subscribers, budget %d and %d",
				measured.records, measured.allocations, measured.bytes, subscribers, maxAllocations, maxBytes)
		}
	}
	if difference := largeAllocations - smallAllocations; difference > 0.5 || difference < -0.5 {
		t.Errorf("one-APC report allocations depend on partition size: %.2f at 16 records, %.2f at 16384",
			smallAllocations, largeAllocations)
	}
	if difference := largeBytes - smallBytes; difference > 64 || difference < -64 {
		t.Errorf("one-APC report bytes depend on partition size: %.0f at 16 records, %.0f at 16384",
			smallBytes, largeBytes)
	}
}

func BenchmarkSSNMOneAPCPublication(b *testing.B) {
	for _, records := range []int{16, 16384} {
		b.Run(fmt.Sprintf("records=%d/subscribers=8", records), func(b *testing.B) {
			fixture := newSSNMPublicationFixture(b, records, 8)
			ctx := context.Background()
			for range 64 {
				fixture.publish(ctx)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				fixture.publish(ctx)
			}
		})
	}
}

// Each subscriber's copy holds all of its Routing Context lists in one array.
// That is only safe if every list is its own: writing through one, or
// appending to one, must leave the others -- and the source -- untouched.
func TestSSNMEventCopyOwnsEachRoutingContextList(t *testing.T) {
	source := SSNMEvent{
		Report: SSNMReport{
			Scope:        WireScope{RoutingContexts: []uint32{1, 2}, RoutingContextSet: true},
			Destinations: []PointCodeRange{{PointCode: 7}},
		},
		Updated: []SSNMDestinationKnowledge{
			{
				Availability: SSNMAvailability{Scope: WireScope{RoutingContexts: []uint32{3}}},
				Congestion:   SSNMCongestion{Scope: WireScope{RoutingContexts: []uint32{4, 5}}},
			},
			{
				Availability: SSNMAvailability{Scope: WireScope{RoutingContexts: []uint32{}, RoutingContextSet: true}},
				Congestion:   SSNMCongestion{Scope: WireScope{RoutingContexts: []uint32{6}}},
			},
		},
	}
	want := fmt.Sprintf("%+v", source)
	lists := func(event *SSNMEvent) []*[]uint32 {
		return []*[]uint32{
			&event.Report.Scope.RoutingContexts,
			&event.Updated[0].Availability.Scope.RoutingContexts,
			&event.Updated[0].Congestion.Scope.RoutingContexts,
			&event.Updated[1].Congestion.Scope.RoutingContexts,
		}
	}
	for target := range lists(&source) {
		for _, grow := range []bool{false, true} {
			owned := source.clone()
			before := fmt.Sprintf("%+v", owned)
			list := lists(&owned)[target]
			if len(*list) != cap(*list) {
				t.Fatalf("list %d has spare capacity %d beyond its length %d", target, cap(*list), len(*list))
			}
			if grow {
				*list = append(*list, 0xdead)
			} else {
				(*list)[0] = 0xdead
			}
			for other, otherList := range lists(&owned) {
				if other == target {
					continue
				}
				for _, value := range *otherList {
					if value == 0xdead {
						t.Fatalf("writing list %d (append=%v) reached list %d", target, grow, other)
					}
				}
			}
			if fmt.Sprintf("%+v", source) != want {
				t.Fatalf("writing list %d (append=%v) reached the source event", target, grow)
			}
			if fmt.Sprintf("%+v", source.clone()) != before {
				t.Fatal("a later copy differs from an earlier one")
			}
		}
	}
	if owned := source.clone(); owned.Updated[1].Availability.Scope.RoutingContexts != nil {
		t.Fatal("an empty Routing Context list was not copied as nil, as WireScope.clone copies it")
	}
	if owned := (SSNMEvent{}).clone(); owned.Updated != nil || owned.Report.Destinations != nil ||
		owned.Report.Scope.RoutingContexts != nil {
		t.Fatalf("an event without slices gained some: %+v", owned)
	}
}

// A consumer that keeps up queues each event into the array the previous one
// left, so its queue costs no allocation; the larger array a burst grew is not
// kept once drained, and nothing a drained slot held stays reachable.
func TestSSNMSubscriptionReusesOnlyASmallDrainedQueue(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, nil, nil)
	_, subscription, err := endpoint.SubscribeSSNM()
	if err != nil {
		t.Fatal(err)
	}
	var revision uint64
	cycle := func() {
		revision++
		subscription.enqueue(SSNMEvent{Revision: revision})
		event, ready, err := subscription.take()
		if !ready || err != nil || event.Revision != revision {
			panic(fmt.Sprintf("take = %+v %v %v, want revision %d", event, ready, err, revision))
		}
	}
	cycle()
	if allocations := testing.AllocsPerRun(1000, cycle); allocations != 0 {
		t.Fatalf("a consumer that keeps up costs %.2f allocations per event to queue for, want 0", allocations)
	}
	if subscription.spare == nil || len(subscription.spare) != 0 {
		t.Fatalf("drained queue array was not kept for reuse: %d/%d", len(subscription.spare), cap(subscription.spare))
	}

	for index := range 3 {
		subscription.enqueue(SSNMEvent{Revision: uint64(100 + index), Reason: "burst"})
	}
	for index := range 3 {
		if event, _, _ := subscription.take(); event.Revision != uint64(100+index) {
			t.Fatalf("event %d out of order: %d", index, event.Revision)
		}
	}
	if spare := subscription.spare[:cap(subscription.spare)]; len(spare) > ssnmSpareQueueSlots {
		t.Fatalf("kept a %d-slot array", len(spare))
	} else {
		for index := range spare {
			if !reflect.DeepEqual(spare[index], queuedSSNMEvent{}) {
				t.Fatalf("kept slot %d still holds an event", index)
			}
		}
	}

	for index := range 3 * ssnmSpareQueueSlots {
		subscription.enqueue(SSNMEvent{Revision: uint64(200 + index)})
	}
	backing := subscription.queue[:cap(subscription.queue)]
	for index := range 3 * ssnmSpareQueueSlots {
		if event, _, _ := subscription.take(); event.Revision != uint64(200+index) {
			t.Fatalf("burst event %d out of order: %d", index, event.Revision)
		}
	}
	if subscription.spare != nil {
		t.Fatalf("kept a burst's %d-slot array after it drained", cap(subscription.spare))
	}
	for index := range backing {
		if !reflect.DeepEqual(backing[index], queuedSSNMEvent{}) {
			t.Fatalf("released burst slot %d still holds an event", index)
		}
	}
}

// Subscription byte budgets charge what an event retains. A one-APC delta
// retains one report destination and one updated destination whatever the
// partition holds, and binding lifecycle retains none, so both cost the same
// against a partition of 16,384 records as against one of 4.
func TestSSNMDeltaEventsAreChargedForWhatTheyCarry(t *testing.T) {
	for _, records := range []int{4, 16384} {
		t.Run(fmt.Sprintf("records=%d", records), func(t *testing.T) {
			fixture := newSSNMPublicationFixture(t, records, 1)
			subscription := fixture.subscriptions[0]
			report := fixture.reports[0]
			if err := fixture.store.apply(report); err != nil {
				t.Fatal(err)
			}
			// The destination carries availability and congestion, each with
			// the fixture's one Routing Context, as does the report scope.
			partitionBytes := 2 * (len(report.Partition.SignallingGateway) + len(report.Partition.ApplicationServer))
			if want := 512 + 8 + 256 + 4*3 + partitionBytes; subscription.queuedBytes != want {
				t.Fatalf("one-APC delta charged %d bytes, want %d", subscription.queuedBytes, want)
			}
			event, err := subscription.Next(context.Background())
			if err != nil || len(event.Updated) != 1 || !event.Updated[0].AvailabilitySet || !event.Updated[0].CongestionSet {
				t.Fatalf("one-APC delta = %+v, %v", event.Updated, err)
			}
			if err := fixture.store.bind(report.Partition, 2, true); err != nil {
				t.Fatal(err)
			}
			if want := 512 + partitionBytes/2; subscription.queuedBytes != want {
				t.Fatalf("binding admission charged %d bytes, want %d", subscription.queuedBytes, want)
			}
		})
	}
}
