// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"cmp"
	"encoding/binary"
	"fmt"
	"maps"
	"slices"
	"time"
)

// CongestionFreshness identifies whether retained congestion is current.
type CongestionFreshness uint8

const (
	// CongestionFresh identifies a current received congestion statement.
	CongestionFresh CongestionFreshness = iota
	// CongestionExpired identifies a statement whose validity has expired.
	// Expiration means unknown congestion, not measured clear.
	CongestionExpired
)

// PeerCongestionState is one peer-local congestion statement. RFC 4666
// Sections 1.3.2.4 and 3.4.4 distinguish ASP congestion from SS7 destination
// authority. This state never supplies an authoritative DAUD response and
// contains no Concerned Destination trigger that could be replayed after loss.
// Identity uses association, epoch, AS, wire NA/RC presence and values, and
// destination; RC list order and duplicates do not change that identity. Scope
// still preserves the original wire list for inspection.
type PeerCongestionState struct {
	Association AssociationID
	Epoch       uint64
	AS          ASKey
	Scope       WireScope
	Destination PointCodeRange
	Congestion  CongestionState
	ReceivedAt  time.Time
	// ExpiresAt is the local validity deadline; zero disables expiry.
	ExpiresAt time.Time
	Freshness CongestionFreshness
	Revision  uint64
}

func (p PeerCongestionState) clone() PeerCongestionState {
	p.Scope = p.Scope.clone()
	return p
}

type peerCongestionKey struct {
	association AssociationID
	epoch       uint64
	as          ASKey
	scope       string
	destination ssnmDestinationKey
}

type peerCongestionSource struct {
	epoch   uint64
	active  map[ASKey]uint64
	records map[ASKey]map[peerCongestionRecordIdentity]struct{}
}

// Each admitted peer record occurs in exactly one AS bucket. The authoritative
// record remains in its partition; the index borrows its immutable identity.
type peerCongestionRecordIdentity struct {
	partition SSNMPartition
	key       peerCongestionKey
}

func (s *ssnmState) indexPeerRecordLocked(partition SSNMPartition, key peerCongestionKey) {
	source := s.peerSources[key.association]
	if source.records == nil {
		source.records = make(map[ASKey]map[peerCongestionRecordIdentity]struct{})
	}
	if source.records[key.as] == nil {
		source.records[key.as] = make(map[peerCongestionRecordIdentity]struct{})
	}
	source.records[key.as][peerCongestionRecordIdentity{partition, key}] = struct{}{}
	s.peerSources[key.association] = source
}

func (s *ssnmState) forgetPeerRecordIndexLocked(partition SSNMPartition, key peerCongestionKey) {
	source, live := s.peerSources[key.association]
	if !live {
		return
	}
	bucket := source.records[key.as]
	delete(bucket, peerCongestionRecordIdentity{partition, key})
	if len(bucket) == 0 {
		delete(source.records, key.as)
	}
	if len(source.records) == 0 {
		source.records = nil
	}
	s.peerSources[key.association] = source
}

// Resolved but empty admission cannot authorize removal of any AS's knowledge.
// Unresolved loss snapshots all source tokens to fence delayed global refusal.
type peerCongestionRefusal struct {
	scope      WireScope
	admission  peerCongestionAdmission
	unresolved bool
}

func (s *ssnmState) captureUnresolvedPeerCongestionRefusal(association AssociationID, epoch uint64, scope WireScope) peerCongestionRefusal {
	refusal := peerCongestionRefusal{scope: scope, admission: peerCongestionAdmission{epoch: epoch}, unresolved: true}
	if s == nil {
		return refusal
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if source, live := s.peerSources[association]; live && source.epoch == epoch {
		refusal.admission.bindings = maps.Clone(source.active)
	}
	return refusal
}

// retireRefusedPeerCongestionLocked forgets only congestion a refused ASP
// SCON could have contradicted (RFC 4666 Section 3.4.4). An audit or an SS7
// report never enters this path. Refusal does not retire membership tokens.
func (s *ssnmState) retireRefusedPeerCongestionLocked(association AssociationID, refusal peerCongestionRefusal) {
	source, live := s.peerSources[association]
	if !live || source.epoch != refusal.admission.epoch {
		return
	}
	if _, live := s.liveSources[association]; !live {
		return
	}
	if refusal.unresolved && !maps.Equal(source.active, refusal.admission.bindings) {
		return
	}
	for key, token := range refusal.admission.bindings {
		if held, exists := source.active[key]; !exists || held != token {
			return
		}
	}
	for _, partition := range s.sortedPeerPartitionsLocked() {
		s.retirePeerRecordsLocked(s.partitions[partition], func(record PeerCongestionState) bool {
			if record.Association != association || record.Epoch != refusal.admission.epoch {
				return false
			}
			if refusal.unresolved {
				return true
			}
			if _, resolved := refusal.admission.bindings[record.AS]; !resolved {
				return false
			}
			// An omitted NA inherits a concrete AS network. All-appearance ASes
			// retain their exact wire network independently; an omitted network
			// overlaps every appearance of that AS, not other ASes.
			appearance, set := refusal.scope.NetworkAppearance, refusal.scope.NetworkAppearanceSet
			if !set {
				appearance, set = record.AS.NetworkAppearance, record.AS.NetworkAppearanceSet
			}
			return !set || !record.Scope.NetworkAppearanceSet || record.Scope.NetworkAppearance == appearance
		})
	}
}

// peerCongestionScopeKey treats RC lists as sets while the retained Scope
// preserves their original wire order. Presence of omitted/explicit NA and RC
// remains distinct (RFC 4666 Sections 3.3.1 and 3.4.4).
func peerCongestionScopeKey(scope WireScope) string {
	contexts := append([]uint32(nil), scope.RoutingContexts...)
	slices.Sort(contexts)
	contexts = slices.Compact(contexts)
	value := make([]byte, 6+4*len(contexts))
	if scope.NetworkAppearanceSet {
		value[0] = 1
	}
	if scope.RoutingContextSet {
		value[1] = 1
	}
	binary.BigEndian.PutUint32(value[2:], scope.NetworkAppearance)
	for index, context := range contexts {
		binary.BigEndian.PutUint32(value[6+4*index:], context)
	}
	return string(value)
}

// This portable budget includes fixed record/key metadata plus the retained
// Routing Context list and its normalized scope key; it is not a heap size.
func peerCongestionRecordBytes(record PeerCongestionState) int {
	return 256 + 8*len(record.Scope.RoutingContexts)
}

// syncPeerCongestion commits the active AS/binding generations of a live
// association. A later report must carry exactly the generations under which
// its scope was admitted; a delayed old epoch or reused RC cannot reinstall it.
func (s *ssnmState) syncPeerCongestion(association AssociationID, epoch uint64, active map[ASKey]uint64) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	if _, live := s.liveSources[association]; !live {
		return
	}
	previous := s.peerSources[association]
	if epoch < previous.epoch {
		return
	}
	for _, partition := range s.sortedPeerPartitionsLocked() {
		state := s.partitions[partition]
		s.retirePeerRecordsLocked(state, func(record PeerCongestionState) bool {
			if record.Association != association {
				return false
			}
			generation, held := active[record.AS]
			return record.Epoch != epoch || !held || previous.active[record.AS] != generation
		})
	}
	owned := make(map[ASKey]uint64, len(active))
	for key, generation := range active {
		owned[key] = generation
	}
	if s.peerSources == nil {
		s.peerSources = make(map[AssociationID]peerCongestionSource)
	}
	s.peerSources[association] = peerCongestionSource{epoch: epoch, active: owned, records: s.peerSources[association].records}
}

// applyPeerReport commits all expanded AS/range records and then the raw
// report under one lock. Snapshots can observe only the final revision, while
// subscribers see each record delta before the raw report and its CD trigger.
func (s *ssnmState) applyPeerReport(report SSNMReport, epoch uint64, bindings map[ASKey]uint64) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrEndpointClosed
	}
	if _, live := s.liveSources[report.Association]; !live {
		return ErrAssociationClosed
	}
	source, live := s.peerSources[report.Association]
	if !live || source.epoch != epoch {
		return ErrAssociationClosed
	}
	for key, generation := range bindings {
		held, exists := source.active[key]
		if !exists || held != generation {
			return ErrRoutingContextNotActive
		}
	}
	state, exists := s.partitions[report.Partition]
	if !exists {
		return ErrRoutingContextNotActive
	}
	if _, bound := state.bindings[report.Association]; !bound {
		return ErrRoutingContextNotActive
	}
	report.Epoch = state.epoch
	if report.Kind != SSNMSignallingCongestionReport || !report.PeerReported || report.Source != SSNMPeerReport {
		return s.publishEventOnlyLocked(report)
	}
	if len(bindings) == 0 {
		return ErrRoutingContextNotActive
	}
	// Deduplicate ranges before expansion, and refuse an expansion that could
	// never fit even if it replaced existing records. Work stays proportional
	// to the validated input and configured retention limit, not AS x APC.
	destinations := make([]ssnmDestinationKey, 0, len(report.Destinations))
	for _, destination := range report.Destinations {
		destinations = append(destinations, ssnmDestinationKeyFor(destination))
	}
	slices.SortFunc(destinations, compareSSNMDestinationKeys)
	destinations = slices.Compact(destinations)
	if len(destinations) == 0 {
		return s.publishEventOnlyLocked(report)
	}
	if len(bindings) > s.limits.MaxRecordsPerPartition/len(destinations) {
		maximumInt := int(^uint(0) >> 1)
		count := maximumInt
		if len(bindings) <= maximumInt/len(destinations) {
			count = len(bindings) * len(destinations)
		}
		return s.refuseReportLocked(count, fmt.Sprintf("peer report expansion exceeds partition record limit %d", s.limits.MaxRecordsPerPartition))
	}
	scopeKey := peerCongestionScopeKey(report.Scope)
	keys := make([]peerCongestionKey, 0, len(bindings)*len(destinations))
	for as := range bindings {
		for _, destination := range destinations {
			keys = append(keys, peerCongestionKey{association: report.Association, epoch: epoch, as: as, scope: scopeKey, destination: destination})
		}
	}
	slices.SortFunc(keys, comparePeerCongestionKeys)
	keys = slices.Compact(keys)
	if len(keys) == 0 {
		return s.publishEventOnlyLocked(report)
	}
	received := s.automationNow()
	scope := report.Scope.clone()
	record := PeerCongestionState{Association: report.Association, Epoch: epoch, Scope: scope, ReceivedAt: received, Freshness: CongestionFresh, Congestion: CongestionState{Congested: !report.CongestionLevelSet || report.CongestionLevel != 0, Level: report.CongestionLevel, LevelSet: report.CongestionLevelSet}}
	newRecords, newBytes := 0, 0
	for _, key := range keys {
		old, held := state.peerCongestion[key]
		if !held {
			newRecords++
		} else {
			newBytes -= peerCongestionRecordBytes(old)
		}
		newBytes += peerCongestionRecordBytes(record)
	}
	peer := report.Partition.peerKey()
	reason := ""
	switch {
	case state.records()+newRecords > s.limits.MaxRecordsPerPartition:
		reason = fmt.Sprintf("peer report would exceed partition record limit %d", s.limits.MaxRecordsPerPartition)
	case s.peerRecords[peer]+newRecords > s.limits.MaxRecordsPerPeer:
		reason = fmt.Sprintf("peer report would exceed peer record limit %d", s.limits.MaxRecordsPerPeer)
	case s.records+newRecords > s.limits.MaxRecords:
		reason = fmt.Sprintf("peer report would exceed store record limit %d", s.limits.MaxRecords)
	case s.bytes+newBytes > s.limits.MaxBytes:
		reason = fmt.Sprintf("peer report would exceed store byte limit %d", s.limits.MaxBytes)
	}
	if reason != "" {
		return s.refuseReportLocked(len(keys), reason)
	}
	if state.peerCongestion == nil {
		state.peerCongestion = make(map[peerCongestionKey]PeerCongestionState)
	}
	s.records += newRecords
	s.peerRecords[peer] += newRecords
	s.bytes += newBytes
	state.bytes += newBytes
	for _, key := range keys {
		record.AS = key.as
		record.Destination = key.destination.pointCodeRange()
		if s.validity > 0 && record.Congestion.Congested {
			record.ExpiresAt = record.ReceivedAt.Add(s.validity)
		}
		record.Revision = s.nextRevisionLocked()
		state.peerCongestion[key] = record
		s.indexPeerRecordLocked(state.partition, key)
		changed := record
		s.publishLocked(SSNMEvent{Kind: SSNMPeerCongestionEvent, Revision: record.Revision, Partition: report.Partition, Epoch: state.epoch, PeerCongestion: &changed})
	}
	return s.publishEventOnlyLocked(report)
}

func comparePeerCongestionKeys(a, b peerCongestionKey) int {
	if value := cmp.Compare(a.association, b.association); value != 0 {
		return value
	}
	if value := cmp.Compare(a.epoch, b.epoch); value != 0 {
		return value
	}
	if value := compareASKey(a.as, b.as); value != 0 {
		return value
	}
	if value := cmp.Compare(a.scope, b.scope); value != 0 {
		return value
	}
	return compareSSNMDestinationKeys(a.destination, b.destination)
}

func (s *ssnmState) sortedPeerPartitionsLocked() []SSNMPartition {
	partitions := make([]SSNMPartition, 0, len(s.partitions))
	for partition, state := range s.partitions {
		if len(state.peerCongestion) > 0 {
			partitions = append(partitions, partition)
		}
	}
	sortSSNMPartitions(partitions)
	return partitions
}

func (s *ssnmState) retirePeerRecordsLocked(state *ssnmPartitionState, remove func(PeerCongestionState) bool) {
	keys := make([]peerCongestionKey, 0)
	for key, record := range state.peerCongestion {
		if s.work != nil {
			s.work.peerRows++
		}
		if remove(record) {
			keys = append(keys, key)
		}
	}
	slices.SortFunc(keys, comparePeerCongestionKeys)
	for _, key := range keys {
		s.retirePeerRecordLocked(state, key)
	}
}

func (s *ssnmState) retirePeerRecordLocked(state *ssnmPartitionState, key peerCongestionKey) {
	record := state.peerCongestion[key]
	delete(state.peerCongestion, key)
	s.forgetPeerRecordIndexLocked(state.partition, key)
	cost := peerCongestionRecordBytes(record)
	state.bytes -= cost
	s.bytes -= cost
	s.records--
	peer := state.partition.peerKey()
	s.peerRecords[peer]--
	if s.peerRecords[peer] <= 0 {
		delete(s.peerRecords, peer)
	}
	record.Revision = s.nextRevisionLocked()
	s.publishLocked(SSNMEvent{Kind: SSNMPeerCongestionEvent, Revision: record.Revision, Partition: state.partition, Epoch: state.epoch, PeerCongestion: &record, PeerCongestionRemoved: true})
}

func (s *ssnmState) peerCongestionSnapshotLocked() []PeerCongestionState {
	var records []PeerCongestionState
	for _, partition := range s.sortedPeerPartitionsLocked() {
		state := s.partitions[partition]
		keys := make([]peerCongestionKey, 0, len(state.peerCongestion))
		for key := range state.peerCongestion {
			keys = append(keys, key)
		}
		slices.SortFunc(keys, comparePeerCongestionKeys)
		for _, key := range keys {
			records = append(records, state.peerCongestion[key].clone())
		}
	}
	return records
}

// retirePeerCongestionAS removes one member's authority to contribute peer
// congestion before an AS withdrawal commits. Other AS inputs and transport
// observations are independent. The source epoch remains as a stale-work fence.
func (s *ssnmState) retirePeerCongestionAS(association AssociationID, key ASKey) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	source := s.peerSources[association]
	if s.work != nil {
		s.work.peerASRows++
	}
	delete(source.active, key)
	selected := make(map[SSNMPartition][]peerCongestionKey)
	s.selectPeerRecordsLocked(source.records[key], selected)
	s.retireSelectedPeerRecordsLocked(selected)
}

// retirePeerCongestionContext withdraws all appearances of a retired dynamic
// RC. Its replacement is admitted only with a newly captured binding token.
func (s *ssnmState) retirePeerCongestionContext(association AssociationID, routingContext uint32) {
	s.retirePeerCongestionContexts(association, map[uint32]struct{}{routingContext: {}})
}

// retirePeerCongestionContexts removes one registration event's contexts in a
// single pass. Its caller fences the selected registrations through replacement.
func (s *ssnmState) retirePeerCongestionContexts(association AssociationID, contexts map[uint32]struct{}) {
	if len(contexts) == 0 {
		return
	}
	s.retirePeerCongestionScopes(association, func(key ASKey) bool {
		_, selected := contexts[key.RoutingContext]
		return key.RoutingContextSet && selected
	})
}

func (s *ssnmState) retirePeerCongestionScopes(association AssociationID, matches func(ASKey) bool) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.retireLocalCongestionLocked(association, matches)
	source := s.peerSources[association]
	selected := make(map[SSNMPartition][]peerCongestionKey)
	for key := range source.active {
		if s.work != nil {
			s.work.peerASRows++
		}
		if matches(key) {
			delete(source.active, key)
			s.selectPeerRecordsLocked(source.records[key], selected)
		}
	}
	s.retireSelectedPeerRecordsLocked(selected)
}

func (s *ssnmState) selectPeerRecordsLocked(bucket map[peerCongestionRecordIdentity]struct{}, selected map[SSNMPartition][]peerCongestionKey) {
	for identity := range bucket {
		if s.work != nil {
			s.work.peerRows++
		}
		selected[identity.partition] = append(selected[identity.partition], identity.key)
	}
}

// Preserve the former partition/key event order while visiting only records
// owned by the selected ASes. No index survives its authoritative record.
func (s *ssnmState) retireSelectedPeerRecordsLocked(selected map[SSNMPartition][]peerCongestionKey) {
	partitions := make([]SSNMPartition, 0, len(selected))
	for partition := range selected {
		partitions = append(partitions, partition)
	}
	sortSSNMPartitions(partitions)
	for _, partition := range partitions {
		keys := selected[partition]
		slices.SortFunc(keys, comparePeerCongestionKeys)
		for _, key := range keys {
			s.retirePeerRecordLocked(s.partitions[partition], key)
		}
	}
}

// bindPeerReport accounts a failed metadata admission as one whole peer
// report, rather than as one refused binding. RFC 4666 Section 3.8.1's peer
// Error conditions do not describe this local resource loss. Successful
// admission retains bindLocked's existing binding and epoch semantics.
func (s *ssnmState) bindPeerReport(report SSNMReport, bindings map[ASKey]uint64) error {
	if s == nil || report.Partition.Kind == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrEndpointClosed
	}
	if _, live := s.liveSources[report.Association]; !live {
		return ErrAssociationClosed
	}
	if _, exists := s.partitions[report.Partition]; !exists {
		reason := ""
		switch {
		case len(s.partitions) >= s.limits.MaxPartitions:
			reason = fmt.Sprintf("partition limit %d reached; %s refused", s.limits.MaxPartitions, describeSSNMPartition(report.Partition))
		case s.bytes+ssnmPartitionBaseBytes > s.limits.MaxBytes:
			reason = fmt.Sprintf("byte limit %d reached; %s refused", s.limits.MaxBytes, describeSSNMPartition(report.Partition))
		}
		if reason != "" {
			return s.refuseReportLocked(peerCongestionReportRecords(report, bindings), reason)
		}
	}
	return s.bindLocked(report.Partition, report.Association, false)
}

// peerCongestionReportRecords counts resolved AS/ranges without expanding
// their product. Duplicate APCs and equivalent masked prefixes cost one record
// per AS, matching applyPeerReport's prepared keys.
func peerCongestionReportRecords(report SSNMReport, bindings map[ASKey]uint64) int {
	destinations := make([]ssnmDestinationKey, 0, len(report.Destinations))
	for _, destination := range report.Destinations {
		destinations = append(destinations, ssnmDestinationKeyFor(destination))
	}
	slices.SortFunc(destinations, compareSSNMDestinationKeys)
	destinations = slices.Compact(destinations)
	if len(destinations) == 0 {
		return 0
	}
	maximumInt := int(^uint(0) >> 1)
	if len(bindings) > maximumInt/len(destinations) {
		return maximumInt
	}
	return len(bindings) * len(destinations)
}
