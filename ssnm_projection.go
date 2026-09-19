// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

// maxPointCodeMask is the widest Affected Point Code mask, which wildcards
// every bit of the 24-bit point code.
const maxPointCodeMask = 24

// destinationKnowledge projects one canonical SSNM partition's retained
// knowledge onto one destination point code.
//
// RFC 4666 Section 3.4.1 lets an Affected Point Code carry a mask, so a peer
// may report a whole range and then a single member of it, in either order.
// Every retained range covering the point code is therefore a report about it,
// and the latest one this node validated decides -- the same rule the store
// itself applies within one partition and one dimension. Specificity does not
// override recency: a later report about a range including this destination is
// the peer's newer word about this destination.
//
// The two dimensions are resolved independently, as RFC 4666 Section 4.5.2.2
// requires, so a congestion report neither installs nor clears availability.
//
// A partition with no binding is not knowledge this node holds: it reports
// nothing, and the caller decides what to do about that.
func (s *ssnmState) destinationKnowledge(partition SSNMPartition, pointCode uint32) aspSelectionStatus {
	if s == nil {
		return aspSelectionStatus{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	state, exists := s.partitions[partition]
	if !exists {
		return aspSelectionStatus{}
	}
	status := aspSelectionStatus{epoch: state.epoch}
	availabilityRevision := uint64(0)
	congestionRevision := uint64(0)
	for mask := uint8(0); mask <= maxPointCodeMask; mask++ {
		key := ssnmDestinationKey{pointCode: destinationRangePrefix(pointCode, mask), mask: mask}
		if record, held := state.availability[key]; held &&
			(!status.availabilitySet || record.Revision > availabilityRevision) {
			availabilityRevision = record.Revision
			status.availability = record.State
			status.availabilitySet = true
		}
		if record, held := state.congestion[key]; held &&
			(!status.congestionSet || record.Revision > congestionRevision) {
			congestionRevision = record.Revision
			status.congestionSet = true
			status.congested = record.Congested
			status.congestionLevel = record.Level
			status.congestionLevelSet = record.LevelSet
		}
	}
	return status
}

// currentRevision reports the store revision an outbound selection was decided
// against, so a remembered traffic-flow assignment can be recognised as stale
// after any change to the knowledge that decided it.
func (s *ssnmState) currentRevision() uint64 {
	if s == nil {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.revision
}
