package m3ua

// forgetReceivedDestinations reclaims this reporter's destination knowledge.
// Canonical siblings, peer ASP congestion and configured isolation are separate
// inputs. Bindings remain live so a genuinely new report can be admitted.
func (c *Association) forgetReceivedDestinations() []SSNMPartition {
	s := c.ssnmStore()
	if s == nil || c.role == RoleSGP {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	var outcomes auditOutcomeBatch
	for _, old := range s.audits {
		if old.key.association != c.ID() {
			continue
		}
		next := old.clone()
		for i := range next.origins {
			next.origins[i].congestion = false
			next.origins[i].availability = false
		}
		s.replaceAuditLocked(old, next, &outcomes)
	}
	var changed []SSNMPartition
	for partition, state := range s.partitions {
		sources, dimensions := s.retireReceivedSourcesLocked(state, func(key receivedCongestionKey, _ SSNMCongestion) bool { return key.association == c.ID() }, false, &outcomes)
		available, availableDimensions := s.retireReceivedAvailabilityLocked(state, func(key receivedCongestionKey, _ SSNMAvailability) bool { return key.association == c.ID() }, &outcomes)
		dimensions = append(dimensions, availableDimensions...)
		for key, record := range state.availability {
			if record.Association == c.ID() {
				destination := key.pointCodeRange()
				finishStatus := s.statusMarks.beginChange(partition, &destination, nil)
				delete(state.availability, key)
				finishStatus()
				s.releaseDestinationRecordLocked(state, ssnmScopeBytes(record.Scope, record.resolved))
				dimensions = append(dimensions, SSNMDestinationInvalidation{Destination: key.pointCodeRange(), AvailabilityRemoved: true})
			}
		}
		for key, record := range state.congestion {
			if record.Association == c.ID() {
				destination := key.pointCodeRange()
				finishStatus := s.statusMarks.beginChange(partition, &destination, nil)
				delete(state.congestion, key)
				finishStatus()
				s.releaseDestinationRecordLocked(state, ssnmScopeBytes(record.Scope, record.resolved))
				dimensions = append(dimensions, SSNMDestinationInvalidation{Destination: key.pointCodeRange(), CongestionRemoved: true})
			}
		}
		if len(sources) > 0 || len(dimensions) > 0 || len(available) > 0 {
			changed = append(changed, partition)
			s.publishDestinationRemovalsLocked(state, dimensions, sources, "received destinations forgotten", available)
		}
	}
	delete(s.auditActivated, c.ID())
	outcomes.publishLocked(s)
	sortSSNMPartitions(changed)
	return changed
}
