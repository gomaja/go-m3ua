package m3ua

// The source key deliberately omits the epoch: retiring a retained statement
// must find every job that still names that exact reporter/scope/range.
type destinationAuditSourceKey struct {
	association AssociationID
	scope       string
	destination ssnmDestinationKey
}

func (k destinationAuditKey) sourceKey() destinationAuditSourceKey {
	return destinationAuditSourceKey{k.association, k.scope, k.destination}
}
func (s *ssnmState) retainAuditLocked(job *destinationAuditJob) {
	job.work = s.work
	if job.originIndex == nil {
		job.originIndex = make(map[auditOriginIdentity]struct{}, len(job.origins))
		for i, origin := range job.origins {
			s.work.visitAS()
			job.originIndex[auditOriginIdentity{origin.partition, job.originAS(i)}] = struct{}{}
		}
	}
	if s.audits == nil {
		s.audits = make(map[destinationAuditKey]*destinationAuditJob)
	}
	if s.auditByAssociation == nil {
		s.auditByAssociation = make(map[AssociationID]map[destinationAuditKey]struct{})
	}
	if s.auditBySource == nil {
		s.auditBySource = make(map[destinationAuditSourceKey]map[destinationAuditKey]struct{})
	}
	key := job.key
	if s.auditByAssociation[key.association] == nil {
		s.auditByAssociation[key.association] = make(map[destinationAuditKey]struct{})
	}
	if s.auditBySource[key.sourceKey()] == nil {
		s.auditBySource[key.sourceKey()] = make(map[destinationAuditKey]struct{})
	}
	s.audits[key] = job
	s.auditByAssociation[key.association][key] = struct{}{}
	s.auditBySource[key.sourceKey()][key] = struct{}{}
}
func (s *ssnmState) forgetAuditIndexLocked(key destinationAuditKey) {
	delete(s.auditByAssociation[key.association], key)
	if len(s.auditByAssociation[key.association]) == 0 {
		delete(s.auditByAssociation, key.association)
	}
	delete(s.auditBySource[key.sourceKey()], key)
	if len(s.auditBySource[key.sourceKey()]) == 0 {
		delete(s.auditBySource, key.sourceKey())
	}
}

// Source retirement is staged across the caller's whole locked operation.
// A grouped job is cloned once even when its origins span several partitions.
type auditSourceRetirementKey struct {
	partition SSNMPartition
	source    receivedCongestionKey
}

const (
	auditRetireCongestion   uint8 = 1
	auditRetireAvailability uint8 = 2
)

// Invalidate captured submissions at the same source-deletion point as
// before batching, even when that cause is already clear on an isolation job.
type auditOriginIdentity struct {
	partition SSNMPartition
	as        ASKey
}

func (b *auditOutcomeBatch) retireSourceLocked(s *ssnmState, partition SSNMPartition, key receivedCongestionKey, congestion bool) {
	if b.sources == nil {
		b.sources = make(map[auditSourceRetirementKey]uint8)
	}
	flag := auditRetireAvailability
	if congestion {
		flag = auditRetireCongestion
	}
	b.sources[auditSourceRetirementKey{partition, key}] |= flag
	source := destinationAuditSourceKey{key.association, key.scope, key.destination}
	for jobKey := range s.auditBySource[source] {
		s.work.visitJob()
		job := s.audits[jobKey]
		if _, matched := job.originIndex[auditOriginIdentity{partition, key.as}]; !matched {
			continue
		}
		s.work.visitAS()
		job.live.Store(false)
		if b.sourceJobs == nil {
			b.sourceJobs = make(map[destinationAuditKey]struct{})
		}
		b.sourceJobs[jobKey] = struct{}{}
	}
}
func (s *ssnmState) retireAuditSourcesLocked(removed map[auditSourceRetirementKey]uint8, candidates map[destinationAuditKey]struct{}, outcomes *auditOutcomeBatch) {
	if len(removed) == 0 {
		return
	}
	for key := range candidates {
		s.work.visitJob()
		old := s.audits[key]
		if old == nil {
			continue
		}
		var next *destinationAuditJob
		for i, origin := range old.origins {
			s.work.visitAS()
			source := receivedCongestionKey{association: key.association, scope: key.scope, destination: key.destination, as: old.originAS(i)}
			flags := removed[auditSourceRetirementKey{origin.partition, source}]
			if flags == 0 {
				continue
			}
			congestion := flags&auditRetireCongestion != 0
			availability := flags&auditRetireAvailability != 0
			if next == nil {
				next = old.clone()
			}
			if congestion {
				next.origins[i].congestion = false
			}
			if availability {
				next.origins[i].availability = false
			}
		}
		if next != nil {
			s.replaceAuditLocked(old, next, outcomes)
		}
	}
}
