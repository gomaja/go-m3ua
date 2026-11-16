package m3ua

import "slices"

// SSNMCongestionSourceInvalidation removes one admitted source statement.
// Revision is the Endpoint-unique report revision; AS distinguishes the AS
// fan-out of a report. Destination identifies its compact range. No wire
// scope or configured identity string is replayed.
type SSNMCongestionSourceInvalidation struct {
	Destination PointCodeRange
	AS          ASKey
	Revision    uint64
}

func (s *ssnmState) releaseDestinationRecordLocked(state *ssnmPartitionState, bytes int) {
	s.records--
	peer := state.partition.peerKey()
	s.peerRecords[peer]--
	if s.peerRecords[peer] == 0 {
		delete(s.peerRecords, peer)
	}
	s.bytes -= bytes
	state.bytes -= bytes
}

// retireReceivedSourcesLocked removes only selected source statements. The
// scalar copy is removed if it is the same statement; other AS/source history
// remains authoritative even when no scalar copy remains.
func (s *ssnmState) retireReceivedSourcesLocked(state *ssnmPartitionState, matches func(receivedCongestionKey, SSNMCongestion) bool, completeSource bool, retirement ...*auditOutcomeBatch) ([]SSNMCongestionSourceInvalidation, []SSNMDestinationInvalidation) {
	var local auditOutcomeBatch
	batch := &local
	if len(retirement) > 0 {
		batch = retirement[0]
	} else {
		defer local.publishLocked(s)
	}
	var sources []SSNMCongestionSourceInvalidation
	var dimensions []SSNMDestinationInvalidation
	for key, record := range state.receivedCongestion {
		s.work.visitSource()
		if !matches(key, record) {
			continue
		}
		sources = append(sources, SSNMCongestionSourceInvalidation{Destination: key.destination.pointCodeRange(), AS: record.AS, Revision: record.Revision})
		destination := key.destination.pointCodeRange()
		scope := statusRecordScope(record)
		// Removing a whole source cannot uncover an older statement of it.
		// A fresh clear contributes no positive or expired input; preserve a
		// surviving sibling's interval. Partial removals must still mark a
		// clear because an older covering positive can remain. If all received
		// history disappears, the scalar-fallback change marks the partition.
		finishStatus := func() {}
		if !completeSource || record.Congested || record.Freshness == CongestionExpired {
			finishStatus = s.statusMarks.beginChange(state.partition, &destination, &scope)
		}
		state.removeStatusSource(key)
		state.deleteReceivedCongestion(key)
		batch.retireSourceLocked(s, state.partition, key, true)
		s.releaseDestinationRecordLocked(state, receivedCongestionBytes(record))
		if scalar, held := state.congestion[key.destination]; held && scalar.Revision == record.Revision && scalar.AS == record.AS {
			delete(state.congestion, key.destination)
			s.releaseDestinationRecordLocked(state, ssnmScopeBytes(scalar.Scope, scalar.resolved))
			dimensions = append(dimensions, SSNMDestinationInvalidation{Destination: key.destination.pointCodeRange(), CongestionRemoved: true})
		}
		finishStatus()
	}
	if len(sources) > 0 && completeSource && len(state.receivedCongestion) == 0 {
		s.statusMarks.change(state.partition, nil, nil)
	}
	return sources, dimensions
}

func compareSourceInvalidations(a, b SSNMCongestionSourceInvalidation) int {
	if order := compareSSNMDestinationKeys(ssnmDestinationKeyFor(a.Destination), ssnmDestinationKeyFor(b.Destination)); order != 0 {
		return order
	}
	if a.Revision < b.Revision {
		return -1
	}
	if a.Revision > b.Revision {
		return 1
	}
	return compareASKey(a.AS, b.AS)
}

// Each stream is bounded independently; even custom retention limits publish
// at most 512+160+2048*(16+48+48)=230048 bytes in one removal event. Source IDs
// never replay RC lists, severity, or configured names (RFC 4666 §3.4.4).
func (s *ssnmState) publishDestinationRemovalsLocked(state *ssnmPartitionState, dimensions []SSNMDestinationInvalidation, sources []SSNMCongestionSourceInvalidation, reason string, availability ...[]SSNMAvailabilitySourceInvalidation) {
	var available []SSNMAvailabilitySourceInvalidation
	if len(availability) > 0 {
		available = availability[0]
	}
	slices.SortFunc(available, compareAvailabilitySourceInvalidations)
	slices.SortFunc(dimensions, func(a, b SSNMDestinationInvalidation) int {
		return compareSSNMDestinationKeys(ssnmDestinationKeyFor(a.Destination), ssnmDestinationKeyFor(b.Destination))
	})
	kept := dimensions[:0]
	for _, value := range dimensions {
		if len(kept) > 0 && kept[len(kept)-1].Destination == value.Destination {
			last := &kept[len(kept)-1]
			last.AvailabilityRemoved = last.AvailabilityRemoved || value.AvailabilityRemoved
			last.CongestionRemoved = last.CongestionRemoved || value.CongestionRemoved
		} else {
			kept = append(kept, value)
		}
	}
	dimensions = kept
	slices.SortFunc(sources, compareSourceInvalidations)
	for len(dimensions) > 0 || len(sources) > 0 || len(available) > 0 {
		nd, ns := min(len(dimensions), ssnmInvalidationBatchSize), min(len(sources), ssnmInvalidationBatchSize)
		na := min(len(available), ssnmInvalidationBatchSize)
		s.partitionsInvalidated++
		s.publishLocked(SSNMEvent{Kind: SSNMDestinationsInvalidatedEvent, Revision: s.nextRevisionLocked(), Epoch: state.epoch, Reason: boundSSNMReason(reason), Invalidated: dimensions[:nd:nd], CongestionSourcesInvalidated: sources[:ns:ns], AvailabilitySourcesInvalidated: available[:na:na]})
		dimensions, sources, available = dimensions[nd:], sources[ns:], available[na:]
	}
}

// retireReceivedAssociationLocked is also used before a source epoch changes.
// It never joins a scheduler pass or transport submission.
func (s *ssnmState) retireReceivedAssociationLocked(association AssociationID, partition *SSNMPartition, retirement ...*auditOutcomeBatch) []SSNMPartition {
	var local auditOutcomeBatch
	outcomes := &local
	if len(retirement) > 0 {
		outcomes = retirement[0]
	} else {
		defer local.publishLocked(s)
	}
	var changed []SSNMPartition
	states := s.partitions
	if partition != nil {
		state := s.partitions[*partition]
		if state == nil {
			return nil
		}
		states = map[SSNMPartition]*ssnmPartitionState{*partition: state}
	}
	for key, state := range states {
		s.work.visitPartition()
		sources, dimensions := s.retireReceivedSourcesLocked(state, func(key receivedCongestionKey, _ SSNMCongestion) bool { return key.association == association }, true, outcomes)
		available, availableDimensions := s.retireReceivedAvailabilityLocked(state, func(key receivedCongestionKey, _ SSNMAvailability) bool { return key.association == association }, outcomes)
		dimensions = append(dimensions, availableDimensions...)
		s.publishDestinationRemovalsLocked(state, dimensions, sources, "received source retired", available)
		if len(sources) > 0 || len(dimensions) > 0 || len(available) > 0 {
			changed = append(changed, key)
		}
	}
	return changed
}

func (s *ssnmState) retireReceivedEpoch(association AssociationID, epoch uint64) []SSNMPartition {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if current, live := s.liveSources[association]; s.closed || !live || epoch <= current {
		return nil
	}
	s.liveSources[association] = epoch
	s.retireDestinationAuditsLocked(association, nil)
	s.retireLocalCongestionLocked(association, nil)
	return s.retireReceivedAssociationLocked(association, nil)
}

// Called inside the event-wide registration submission fence, before any
// replacement commits and with tracker/dynamic locks released. Source history
// belongs to the receiving (peer registration) direction; static membership in
// the same AS survives a dynamic registration's removal.
func (c *Association) retireReceivedRegistration(routingContext uint32, local bool) []SSNMPartition {
	if local {
		return nil
	}
	return c.retireReceivedRegistrations(map[uint32]struct{}{routingContext: {}})
}

func (c *Association) retireReceivedRegistrations(routingContexts map[uint32]struct{}) []SSNMPartition {
	var changed []SSNMPartition
	if c == nil || c.role == RoleSGP || len(routingContexts) == 0 {
		return nil
	}
	s := c.ssnmStore()
	if s == nil {
		return nil
	}
	static := newReceivedStaticASIndex(c.staticallyConfiguredASKeys(), s.work)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	var outcomes auditOutcomeBatch
	defer outcomes.publishLocked(s)
	for partition, state := range s.partitions {
		sources, dimensions := s.retireReceivedSourcesLocked(state, func(key receivedCongestionKey, record SSNMCongestion) bool {
			_, selected := routingContexts[record.AS.RoutingContext]
			return key.association == c.ID() && record.AS.RoutingContextSet && selected && !receivedStaticASPresent(static, record.AS, s.work)
		}, false, &outcomes)
		available, availableDimensions := s.retireReceivedAvailabilityLocked(state, func(key receivedCongestionKey, record SSNMAvailability) bool {
			_, selected := routingContexts[record.AS.RoutingContext]
			return key.association == c.ID() && record.AS.RoutingContextSet && selected && !receivedStaticASPresent(static, record.AS, s.work)
		}, &outcomes)
		dimensions = append(dimensions, availableDimensions...)
		s.publishDestinationRemovalsLocked(state, dimensions, sources, "AS registration retired", available)
		if len(sources) > 0 || len(dimensions) > 0 || len(available) > 0 {
			changed = append(changed, partition)
		}
	}
	s.retireAuditRegistrationsLocked(c.ID(), routingContexts, static, &outcomes)
	return changed
}

// Project only after store and registration-map locks have been released.
// A departed reporter can change shared SG knowledge used by active siblings.
func (c *Association) projectReceivedChanges(partitions []SSNMPartition) {
	if c == nil || c.endpoint == nil || len(partitions) == 0 {
		return
	}
	if c.endpoint.aspRoutes != nil {
		c.endpoint.aspRoutes.canonicalStateChanged(partitions)
	}
	c.endpoint.refreshIPSPDestinations()
}
