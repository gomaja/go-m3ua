package m3ua

// A statement is a retained prefix, not an expanded point-code history.
// Revisions below a cause's originating report cannot answer that cause.
type auditRangeStatement struct {
	destination ssnmDestinationKey
	revision    uint64
	needed      bool
}

// Build once; summarize the least recent final clear over every sparse region.
// A query needs repair if any region is still positive, unknown, or no newer
// than the cause. Newer children shadow ancestors before evaluating that test.
func auditRangeStillNeeded(target ssnmDestinationKey, statements []auditRangeStatement, probes ...*ssnmWorkCount) bool {
	var work *ssnmWorkCount
	if len(probes) > 0 {
		work = probes[0]
	}
	tree := &auditPrefixNode{}
	for _, statement := range statements {
		tree.add(statement, work)
	}
	tree.summarize(auditRangeStatement{needed: true}, work)
	return tree.clearAt(target, work) == 0
}

type auditCoverageKey struct {
	partition   SSNMPartition
	association AssociationID
	as          ASKey
	congestion  bool
}
type auditCoverageIndex map[auditCoverageKey]*auditPrefixNode

// All origins of a report share this single partition/source traversal. The
// query threshold remains per cause, so isolation and availability reuse the
// same minimum-clear summary without merging their report versions.
func (s *ssnmState) buildAuditCoverageLocked(association AssociationID, keys map[ASKey]struct{}, partitions map[SSNMPartition]struct{}, congestion bool) auditCoverageIndex {
	index := make(auditCoverageIndex)
	add := func(partition SSNMPartition, as ASKey, destination ssnmDestinationKey, revision uint64, needed bool) {
		if _, wanted := keys[as]; !wanted {
			return
		}
		key := auditCoverageKey{partition, association, as, congestion}
		tree := index[key]
		if tree == nil {
			tree = &auditPrefixNode{}
			index[key] = tree
		}
		tree.add(auditRangeStatement{destination, revision, needed}, s.work)
	}
	for partition := range partitions {
		state := s.partitions[partition]
		if state == nil {
			continue
		}
		if congestion && (s.validity > 0 || s.auditing != nil) {
			for key, record := range state.receivedCongestion {
				s.work.visitSource()
				if key.association == association {
					add(partition, record.AS, key.destination, record.Revision, record.Congested)
				}
			}
		} else if !congestion && s.auditing != nil {
			for key, record := range state.receivedAvailability {
				s.work.visitSource()
				if key.association == association {
					add(partition, record.AS, key.destination, record.Revision, record.State != DestinationAvailable)
				}
			}

		}
	}
	for _, tree := range index {
		tree.summarize(auditRangeStatement{needed: true}, s.work)
	}
	return index
}
func (index auditCoverageIndex) stillNeeded(job *destinationAuditJob, i int, congestion bool, after uint64, work *ssnmWorkCount) bool {
	tree := index[auditCoverageKey{job.origins[i].partition, job.key.association, job.originAS(i), congestion}]
	return tree == nil || tree.clearAt(job.key.destination, work) <= after
}
