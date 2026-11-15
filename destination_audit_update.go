package m3ua

// One received report shares its AS membership, job-prefix index and coverage
// summaries. The index stores keys so staged replacement/rollback always reads
// the current immutable job from the authoritative map.
type auditOriginUpdate struct {
	keys          map[ASKey]struct{}
	jobs          auditPrefixNode
	jobKeys       []destinationAuditKey
	seen          map[destinationAuditKey]struct{}
	partitions    map[SSNMPartition]struct{}
	coverage      auditCoverageIndex
	coverageBuilt bool
	positive      bool
}

func auditReportPositive(report SSNMReport) bool {
	return report.Kind == SSNMDestinationUnavailableReport || report.Kind == SSNMDestinationRestrictedReport || report.Kind == SSNMSignallingCongestionReport && (!report.CongestionLevelSet || report.CongestionLevel != 0)
}
func (s *ssnmState) prepareAuditOriginUpdateLocked(association AssociationID, epoch uint64, report SSNMReport, keys []ASKey) *auditOriginUpdate {
	view := &auditOriginUpdate{keys: make(map[ASKey]struct{}, len(keys)), seen: make(map[destinationAuditKey]struct{}), partitions: make(map[SSNMPartition]struct{}), positive: auditReportPositive(report)}
	for _, as := range keys {
		s.work.visitAS()
		view.keys[as] = struct{}{}
	}
	for key := range s.auditByAssociation[association] {
		s.work.visitJob()
		if key.epoch != epoch {
			continue
		}
		view.addJob(key, s.work)
		for _, origin := range s.audits[key].origins {
			view.partitions[origin.partition] = struct{}{}
		}
	}
	return view
}
func (v *auditOriginUpdate) addJob(key destinationAuditKey, work *ssnmWorkCount) {
	if _, held := v.seen[key]; held {
		return
	}
	v.seen[key] = struct{}{}
	v.jobKeys = append(v.jobKeys, key)
	if v.positive {
		node := v.jobs.insert(key.destination, work)
		node.jobs = append(node.jobs, key)
	}
}
func (v *auditOriginUpdate) stillNeeded(s *ssnmState, job *destinationAuditJob, i int, congestion bool, after uint64) bool {
	if !v.coverageBuilt {
		v.coverage = s.buildAuditCoverageLocked(job.key.association, v.keys, v.partitions, congestion)
		v.coverageBuilt = true
	}
	return v.coverage.stillNeeded(job, i, congestion, after, s.work)
}
