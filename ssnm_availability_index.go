package m3ua

import "container/list"

// Availability selects the contributing AS independently of congestion (RFC
// 4666 §4.5.2.2). At most two links per retained source keep covering-input
// comparisons bounded to 25 prefixes, including after the scalar is removed.
// All-source and normalized scoped queries share this partition index with
// offered-status marks; no availability aggregate duplicates it. Exact source
// buckets remain separate for owned event/snapshot enumeration.
type ssnmAvailabilityPrefix struct {
	group       ssnmSourceProjectionGroup
	destination ssnmDestinationKey
}
type ssnmAvailabilityRecordIndex struct {
	record      SSNMAvailability
	links       [2]*list.Element
	prefixes    [2]ssnmAvailabilityPrefix
	memberships int
}
type ssnmAvailabilityIndex struct {
	records  map[receivedCongestionKey]*ssnmAvailabilityRecordIndex
	prefixes map[ssnmAvailabilityPrefix]*list.List
}

func (p *ssnmPartitionState) replaceAvailabilitySource(key receivedCongestionKey, record SSNMAvailability) {
	if p.availabilityProjection == nil {
		p.availabilityProjection = &ssnmAvailabilityIndex{records: make(map[receivedCongestionKey]*ssnmAvailabilityRecordIndex), prefixes: make(map[ssnmAvailabilityPrefix]*list.List)}
	}
	p.removeAvailabilitySource(key)
	next := &ssnmAvailabilityRecordIndex{record: record, memberships: 1}
	next.prefixes[0] = ssnmAvailabilityPrefix{group: ssnmSourceProjectionGroup{all: true}, destination: key.destination}
	if group, scoped := sourceProjectionRecordGroup(p.partition.Kind == SSNMStandalonePartition, record.AS, record.Scope); scoped {
		next.prefixes[1] = ssnmAvailabilityPrefix{group: group, destination: key.destination}
		next.memberships++
	}
	for i := 0; i < next.memberships; i++ {
		prefix := next.prefixes[i]
		bucket := p.availabilityProjection.prefixes[prefix]
		if bucket == nil {
			bucket = list.New()
			p.availabilityProjection.prefixes[prefix] = bucket
		}
		// Receive commits assign increasing revisions under the store mutex.
		// Availability has no revision-preserving expiry mutation.
		next.links[i] = bucket.PushBack(next)
	}
	p.availabilityProjection.records[key] = next
}
func (p *ssnmPartitionState) removeAvailabilitySource(key receivedCongestionKey) {
	if p.availabilityProjection == nil {
		return
	}
	old := p.availabilityProjection.records[key]
	if old == nil {
		return
	}
	for i := 0; i < old.memberships; i++ {
		prefix := old.prefixes[i]
		bucket := p.availabilityProjection.prefixes[prefix]
		bucket.Remove(old.links[i])
		if bucket.Len() == 0 {
			delete(p.availabilityProjection.prefixes, prefix)
		}
	}
	delete(p.availabilityProjection.records, key)
}
func (p *ssnmPartitionState) availabilityInput(pc uint32, query ...ASKey) (SSNMAvailability, bool) {
	var current SSNMAvailability
	if p.availabilityProjection == nil {
		return current, false
	}
	group := sourceProjectionQueryGroup(p.partition.Kind == SSNMStandalonePartition, query)
	held := false
	for mask := uint8(0); mask <= maxPointCodeMask; mask++ {
		p.work.visitPrefix()
		prefix := ssnmAvailabilityPrefix{group: group, destination: ssnmDestinationKey{pointCode: destinationRangePrefix(pc, mask), mask: mask}}
		bucket := p.availabilityProjection.prefixes[prefix]
		if bucket == nil {
			continue
		}
		record := bucket.Back().Value.(*ssnmAvailabilityRecordIndex).record
		if record.Revision > current.Revision {
			current, held = record, true
		}
	}
	return current, held
}
