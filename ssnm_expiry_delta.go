package m3ua

// SSNMCongestionExpiry identifies one retained statement whose local validity
// ended (RFC 4666 §3.4.4). Epoch is the Endpoint-unique partition generation,
// not the reporting association's epoch. Revision is the original statement's
// revision; the enclosing event has its own later revision. No severity, wire
// scope or provenance is replayed. Peer distinguishes peer-local statements
// from authoritative destination source history.
type SSNMCongestionExpiry struct {
	Epoch       uint64
	Destination PointCodeRange
	AS          ASKey
	Revision    uint64
	Peer        bool
}

// ApplyExpiry marks the exact records named by this expiry event as expired in
// a snapshot owned by the caller. It preserves all other fields, including the
// snapshot revision: the caller orders the subscription stream. A renewed
// statement or a different partition generation cannot be expired by this
// event. Matching scalar congestion copies are updated alongside source history.
// Peer statement revisions are Endpoint-unique and require the named partition
// epoch to remain present. Reapplying a matching event is idempotent.
// It returns false for another event kind, a nil snapshot, or no matching record.
func (e SSNMEvent) ApplyExpiry(snapshot *SSNMSnapshot) bool {
	if e.Kind != SSNMCongestionExpiredEvent || snapshot == nil || len(e.Expired) == 0 {
		return false
	}
	type partitionIndex struct {
		partition    *SSNMPartitionKnowledge
		destinations map[ssnmDestinationKey]int
	}
	partitions := make(map[uint64]*partitionIndex, len(snapshot.Partitions))
	for i := range snapshot.Partitions {
		partition := &snapshot.Partitions[i]
		if partition.Epoch != 0 {
			partitions[partition.Epoch] = &partitionIndex{partition: partition}
		}
	}
	var peers map[uint64]int
	matched := false
	for _, expiry := range e.Expired {
		index := partitions[expiry.Epoch]
		if index == nil || expiry.Revision == 0 {
			continue
		}
		key := ssnmDestinationKeyFor(expiry.Destination)
		if expiry.Peer {
			if peers == nil {
				peers = make(map[uint64]int, len(snapshot.PeerCongestion))
				for i, record := range snapshot.PeerCongestion {
					peers[record.Revision] = i
				}
			}
			if i, held := peers[expiry.Revision]; held {
				record := &snapshot.PeerCongestion[i]
				if record.AS == expiry.AS && ssnmDestinationKeyFor(record.Destination) == key {
					record.Freshness = CongestionExpired
					matched = true
				}
			}
			continue
		}
		if index.destinations == nil {
			index.destinations = make(map[ssnmDestinationKey]int, len(index.partition.Destinations))
			for i, destination := range index.partition.Destinations {
				index.destinations[ssnmDestinationKeyFor(destination.Destination)] = i
			}
		}
		if i, held := index.destinations[key]; held {
			destination := &index.partition.Destinations[i]
			if destination.CongestionSet && destination.Congestion.Revision == expiry.Revision && destination.Congestion.AS == expiry.AS {
				destination.Congestion.Freshness = CongestionExpired
				matched = true
			}
			for j := range destination.CongestionSources {
				record := &destination.CongestionSources[j]
				if record.Revision == expiry.Revision && record.AS == expiry.AS {
					record.Freshness = CongestionExpired
					matched = true
				}
			}
		}
	}
	return matched
}
