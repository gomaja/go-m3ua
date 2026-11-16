package m3ua

import "slices"

func receivedAvailabilityBytes(record SSNMAvailability) int {
	return ssnmScopeBytes(record.Scope, []ASKey{record.AS})
}

// Each compact statement retains its resolved AS independently. RC-scoped
// updates concern only those traffic flows (RFC 4666 §§1.4.2.1, 3.4.1, 3.4.2).
// A grouped report is admitted whole against the existing source-record limits.
func (s *ssnmState) applyReceivedAvailabilityLocked(state *ssnmPartitionState, report SSNMReport, resolvedAS []ASKey) error {
	keys := make([]ssnmDestinationKey, 0, len(report.Destinations))
	for _, d := range report.Destinations {
		keys = append(keys, ssnmDestinationKeyFor(d))
	}
	slices.SortFunc(keys, compareSSNMDestinationKeys)
	keys = slices.Compact(keys)
	resolved := append([]ASKey(nil), resolvedAS...)
	state.work.copyAS(len(resolved))
	if len(resolved) == 0 {
		key := ASKey{NetworkAppearance: report.Scope.NetworkAppearance, NetworkAppearanceSet: report.Scope.NetworkAppearanceSet}
		if report.Scope.RoutingContextSet && len(report.Scope.RoutingContexts) == 1 {
			key.RoutingContext = report.Scope.RoutingContexts[0]
			key.RoutingContextSet = true
		}
		resolved = append(resolved, key)
	}
	slices.SortFunc(resolved, compareASKey)
	resolved = slices.Compact(resolved)
	if len(keys) > 0 && len(resolved) > s.limits.MaxRecordsPerPartition/len(keys) {
		maximumInt := int(^uint(0) >> 1)
		refused := maximumInt
		if len(resolved) <= maximumInt/len(keys) {
			refused = len(resolved) * len(keys)
		}
		return s.refuseReportLocked(refused, "resolved availability source report exceeds partition record limit")
	}
	scopeKey := peerCongestionScopeKey(report.Scope)
	record := SSNMAvailability{State: report.availabilityState(), Kind: report.Kind, Source: report.Source, Scope: report.Scope.clone(), Association: report.Association, Epoch: report.Epoch}
	added, bytes := 0, 0
	for _, d := range keys {
		if old, held := state.availability[d]; !held {
			added++
			bytes += ssnmScopeBytes(record.Scope, resolved)
		} else {
			bytes += ssnmScopeBytes(record.Scope, resolved) - ssnmScopeBytes(old.Scope, old.resolved)
		}
		for _, as := range resolved {
			old, held := state.receivedAvailability[receivedCongestionKey{association: report.Association, as: as, scope: scopeKey, destination: d}]
			if !held {
				added++
			} else {
				bytes -= receivedAvailabilityBytes(old)
			}
			bytes += receivedAvailabilityBytes(record)
		}
	}
	if err := s.admitLocked(state, report.Partition.peerKey(), added, bytes, report); err != nil {
		return err
	}
	if state.receivedAvailability == nil {
		state.receivedAvailability = make(map[receivedCongestionKey]SSNMAvailability)
	}
	record.Revision = s.nextRevisionLocked()
	report.Revision = record.Revision
	for _, d := range keys {
		for _, as := range resolved {
			record.AS = as
			state.setReceivedAvailability(receivedCongestionKey{association: report.Association, as: as, scope: scopeKey, destination: d}, record)

		}
		state.availability[d] = ssnmAvailabilityRecord{SSNMAvailability: record, resolved: resolved}
	}
	s.records += added
	s.peerRecords[report.Partition.peerKey()] += added
	s.bytes += bytes
	state.bytes += bytes
	event := SSNMEvent{Kind: SSNMReportEvent, Revision: record.Revision, Partition: report.Partition, Epoch: report.Epoch, Report: report, ReportSet: true}
	if len(s.subscribers) > 0 {
		for _, d := range keys {
			event.Updated = append(event.Updated, state.retainedKnowledge(d))
		}
	}
	s.publishLocked(event)
	return nil
}

// SSNMAvailabilitySourceInvalidation identifies one retired availability
// statement without replaying its wire scope. Revision is Endpoint-unique;
// AS distinguishes the resolved fan-out of the original report.
type SSNMAvailabilitySourceInvalidation struct {
	Destination PointCodeRange
	AS          ASKey
	Revision    uint64
}

func (s *ssnmState) retireReceivedAvailabilityLocked(state *ssnmPartitionState, matches func(receivedCongestionKey, SSNMAvailability) bool, outcomes *auditOutcomeBatch) ([]SSNMAvailabilitySourceInvalidation, []SSNMDestinationInvalidation) {
	var sources []SSNMAvailabilitySourceInvalidation
	var dimensions []SSNMDestinationInvalidation
	for key, record := range state.receivedAvailability {
		s.work.visitSource()
		if !matches(key, record) {
			continue
		}
		sources = append(sources, SSNMAvailabilitySourceInvalidation{Destination: key.destination.pointCodeRange(), AS: record.AS, Revision: record.Revision})
		state.deleteReceivedAvailability(key)
		s.releaseDestinationRecordLocked(state, receivedAvailabilityBytes(record))
		outcomes.retireSourceLocked(s, state.partition, key, false)
		if scalar, held := state.availability[key.destination]; held && scalar.Revision == record.Revision && scalar.AS == record.AS {
			delete(state.availability, key.destination)
			s.releaseDestinationRecordLocked(state, ssnmScopeBytes(scalar.Scope, scalar.resolved))
			dimensions = append(dimensions, SSNMDestinationInvalidation{Destination: key.destination.pointCodeRange(), AvailabilityRemoved: true})
		}
	}
	return sources, dimensions
}

func compareAvailabilitySourceInvalidations(a, b SSNMAvailabilitySourceInvalidation) int {
	return compareSourceInvalidations(SSNMCongestionSourceInvalidation(a), SSNMCongestionSourceInvalidation(b))
}
