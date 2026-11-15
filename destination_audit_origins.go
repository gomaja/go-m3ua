package m3ua

import (
	"slices"
	"time"
)

type destinationAuditOrigin struct {
	partition                           SSNMPartition
	due                                 time.Time
	congestion, availability, isolation bool
}

func (o destinationAuditOrigin) needed() bool { return o.congestion || o.availability || o.isolation }

func (j *destinationAuditJob) clone() *destinationAuditJob {
	j.work.copyOrigins(len(j.origins))
	return &destinationAuditJob{originIndex: j.originIndex, work: j.work, info: j.info, key: j.key, scope: j.scope, bindings: j.bindings, partitions: j.partitions, due: j.due, congestion: j.congestion, availability: j.availability, isolation: j.isolation, generation: j.generation, outcome: j.outcome, origins: slices.Clone(j.origins), congestionRevision: j.congestionRevision, availabilityRevision: j.availabilityRevision, isolationRevision: j.isolationRevision}
}
func (j *destinationAuditJob) summarizeOrigins() {
	j.congestion, j.availability, j.isolation = false, false, false
	j.due = time.Time{}
	for _, o := range j.origins {
		j.congestion = j.congestion || o.congestion
		j.availability = j.availability || o.availability
		j.isolation = j.isolation || o.isolation
		if o.needed() && (j.due.IsZero() || o.due.Before(j.due)) {
			j.due = o.due
		}
	}
}
func (j *destinationAuditJob) originAS(index int) ASKey {
	key := j.bindings[index].key
	if j.scope.NetworkAppearanceSet {
		key.NetworkAppearance, key.NetworkAppearanceSet = j.scope.NetworkAppearance, true
	}
	return key
}
func (j *destinationAuditJob) originDue(index int, now time.Time) bool {
	return index < len(j.origins) && j.origins[index].needed() && !j.origins[index].due.After(now)
}
func (j *destinationAuditJob) submissionScope(now time.Time) WireScope {
	all := true
	for i := range j.bindings {
		all = all && j.originDue(i, now)
	}
	// Preserve original omitted scope only while it still covers every origin.
	// Explicit lists are rebuilt as sets, including duplicate-RC source reports.
	if all && !j.scope.RoutingContextSet {
		return j.scope
	}
	scope := j.scope
	scope.RoutingContexts = nil
	for i, binding := range j.bindings {
		if j.originDue(i, now) && binding.key.RoutingContextSet {
			scope.RoutingContexts = append(scope.RoutingContexts, binding.key.RoutingContext)
		}
	}
	if len(scope.RoutingContexts) > 0 {
		scope.RoutingContextSet = true
		slices.Sort(scope.RoutingContexts)
		scope.RoutingContexts = slices.Compact(scope.RoutingContexts)
	}
	return scope
}
func (s *ssnmState) replaceAuditLocked(old, next *destinationAuditJob, retirement ...*auditOutcomeBatch) {
	old.live.Store(false)
	next.summarizeOrigins()
	if !next.congestion && !next.availability && !next.isolation {
		if len(retirement) > 0 {
			s.removeAuditLocked(old.key, "reporter-retired", retirement[0])
		} else {
			s.removeAuditLocked(old.key, "")
		}
		return
	}
	next.live.Store(true)
	s.retainAuditLocked(next)
}

// Clears accumulate on each AS of the original job. Other AS deadlines and
// origins survive. A new positive statement moves only the covered origin to
// its own exact report key instead of accumulating duplicate periodic work.
func (s *ssnmState) updateAuditOriginsLocked(association AssociationID, epoch uint64, report SSNMReport, keys []ASKey, scopeKey string, now time.Time, saved ...map[destinationAuditKey]*destinationAuditJob) {
	view := s.prepareAuditOriginUpdateLocked(association, epoch, report, keys)
	s.updateAuditOriginsFromIndexLocked(association, epoch, report, scopeKey, now, view, saved...)
}
func (s *ssnmState) updateAuditOriginsFromIndexLocked(association AssociationID, epoch uint64, report SSNMReport, scopeKey string, now time.Time, view *auditOriginUpdate, saved ...map[destinationAuditKey]*destinationAuditJob) {
	reportPrefixes := &auditPrefixNode{}
	for _, d := range report.Destinations {
		node := reportPrefixes.insert(ssnmDestinationKeyFor(d), s.work)
		node.hasStatement = true
	}
	candidates := view.jobKeys
	if view.positive && len(report.Destinations) == 1 {
		candidates = view.jobs.coveredJobs(ssnmDestinationKeyFor(report.Destinations[0]), s.work)
	}
	for _, key := range candidates {
		s.work.visitJob()
		old := s.audits[key]
		if old == nil || key.association != association || key.epoch != epoch {
			continue
		}

		covered, intersects := reportPrefixes.relation(key.destination, s.work)
		if !intersects {
			continue
		}
		next := old.clone()
		changed := false
		same := key.scope == scopeKey && len(report.Destinations) == 1 && key.destination == ssnmDestinationKeyFor(report.Destinations[0])
		for i := range next.origins {
			s.work.visitAS()
			_, held := view.keys[old.originAS(i)]
			if !held {
				continue
			}
			o := &next.origins[i]
			switch report.Kind {
			case SSNMDestinationAvailableReport:
				if covered || !view.stillNeeded(s, old, i, false, old.availabilityRevision) {
					o.availability = false
				}
				if covered || !view.stillNeeded(s, old, i, false, old.isolationRevision) {
					o.isolation = false
				}
				if covered || !o.availability && !o.isolation {
					o.due = now.Add(s.auditing.Period)
				}
				changed = true
			case SSNMSignallingCongestionReport:
				if report.CongestionLevelSet && report.CongestionLevel == 0 {
					if covered || !view.stillNeeded(s, old, i, true, old.congestionRevision) {
						o.congestion = false
					}
					changed = true
				} else {
					if !covered {
						continue
					}
					if !same {
						o.congestion = false
					} else {
						o.due = now.Add(s.auditing.Period)
					}
					changed = true
				}
			case SSNMDestinationUnavailableReport, SSNMDestinationRestrictedReport:
				if !covered {
					continue
				}
				if !same {
					o.availability = false
				} else {
					o.due = now.Add(s.auditing.Period)
				}
				changed = true
			}
		}
		if changed {
			if len(saved) > 0 {
				saved[0][key] = old
			}
			s.replaceAuditLocked(old, next)
		}
	}
}

func (s *ssnmState) retireAuditRegistrationsLocked(association AssociationID, contexts map[uint32]struct{}, static receivedStaticASIndex, retirement ...*auditOutcomeBatch) {
	var local auditOutcomeBatch
	outcomes := &local
	if len(retirement) > 0 {
		outcomes = retirement[0]
	} else {
		defer local.publishLocked(s)
	}
	for key := range s.auditByAssociation[association] {
		old := s.audits[key]
		s.work.visitJob()
		if key.association != association {
			continue
		}
		var next *destinationAuditJob
		for i, binding := range old.bindings {
			_, selected := contexts[binding.key.RoutingContext]
			if binding.key.RoutingContextSet && selected && !receivedStaticASPresent(static, old.originAS(i), s.work) {
				if next == nil {
					next = old.clone()
				}
				next.origins[i].congestion = false
				next.origins[i].availability = false
				next.origins[i].isolation = false
			}
		}
		if next != nil {
			next.summarizeOrigins()
			if !next.congestion && !next.availability && !next.isolation {
				s.removeAuditLocked(key, "reporter-retired", outcomes)
			} else {
				s.replaceAuditLocked(old, next)
			}
		}
	}
}

func (s *ssnmState) retireAuditDimensionLocked(partition SSNMPartition, destination ssnmDestinationKey, congestion, availability bool, scope WireScope, resolved []ASKey, retirement ...*auditOutcomeBatch) {
	var local auditOutcomeBatch
	batch := &local
	if len(retirement) > 0 {
		batch = retirement[0]
	} else {
		defer local.publishLocked(s)
	}
	for _, old := range s.audits {
		s.work.visitJob()
		if old.key.destination != destination {
			continue
		}
		next := old.clone()
		changed := false
		// Reuse the frozen-dimension overlap rules, without rereading config.
		for i, o := range next.origins {
			if o.partition != partition || !(ssnmDestinationRefusal{resolved: true, keys: []ASKey{old.originAS(i)}}).matches(partition, scope, resolved) {
				continue
			}
			if congestion {
				next.origins[i].congestion = false
			}
			if availability {
				next.origins[i].availability = false
			}
			changed = true
		}
		if changed {
			s.replaceAuditLocked(old, next, batch)
		}
	}
}

// Periodic origins also retain their report's availability/congestion scope.
// A later scalar update for another AS can replace the destination dimension
// without answering this origin. A scoped refusal contradicts these timers
// too, while configured isolation remains independent repair work.
func (s *ssnmState) invalidateAuditScopesLocked(partitions []SSNMPartition, refusal ssnmDestinationRefusal, outcomes *auditOutcomeBatch) map[SSNMPartition]ssnmScopeMatcher {
	matchers := refusal.matchers(partitions)
	if len(partitions) == 0 {
		return matchers
	}
	for _, old := range s.audits {
		s.work.visitJob()
		var next *destinationAuditJob
		for i, origin := range old.origins {
			s.work.visitAS()
			matcher, affected := matchers[origin.partition]
			if !affected || !origin.congestion && !origin.availability || !matcher.matches(old.scope, []ASKey{old.originAS(i)}) {
				continue
			}
			if next == nil {
				next = old.clone()
			}
			next.origins[i].congestion = false
			next.origins[i].availability = false
		}
		if next != nil {
			s.replaceAuditLocked(old, next, outcomes)
		}
	}
	return matchers
}
