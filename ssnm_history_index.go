package m3ua

// Buckets contain identities, never replacement values or expanded point codes.
// The authoritative maps own each statement; all mutations update both under
// the store mutex. Each identity occurs in exactly one destination bucket.
type ssnmSourceBucket map[receivedCongestionKey]struct{}

func (p *ssnmPartitionState) setReceivedCongestion(key receivedCongestionKey, record SSNMCongestion) {
	p.sourceProjection = nil
	if p.receivedCongestion == nil {
		p.receivedCongestion = make(map[receivedCongestionKey]SSNMCongestion)
	}
	if p.congestionSources == nil {
		p.congestionSources = make(map[ssnmDestinationKey]ssnmSourceBucket)
	}
	if p.congestionSources[key.destination] == nil {
		p.congestionSources[key.destination] = make(ssnmSourceBucket)
	}
	p.receivedCongestion[key] = record
	p.congestionSources[key.destination][key] = struct{}{}
}
func (p *ssnmPartitionState) setReceivedAvailability(key receivedCongestionKey, record SSNMAvailability) {
	if p.receivedAvailability == nil {
		p.receivedAvailability = make(map[receivedCongestionKey]SSNMAvailability)
	}
	if p.availabilitySources == nil {
		p.availabilitySources = make(map[ssnmDestinationKey]ssnmSourceBucket)
	}
	if p.availabilitySources[key.destination] == nil {
		p.availabilitySources[key.destination] = make(ssnmSourceBucket)
	}
	p.replaceAvailabilitySource(key, record)
	p.receivedAvailability[key] = record
	p.availabilitySources[key.destination][key] = struct{}{}
}
func (p *ssnmPartitionState) deleteReceivedCongestion(key receivedCongestionKey) {
	p.sourceProjection = nil
	delete(p.receivedCongestion, key)
	bucket := p.congestionSources[key.destination]
	delete(bucket, key)
	if len(bucket) == 0 {
		delete(p.congestionSources, key.destination)
	}
}
func (p *ssnmPartitionState) deleteReceivedAvailability(key receivedCongestionKey) {
	p.removeAvailabilitySource(key)
	delete(p.receivedAvailability, key)
	bucket := p.availabilitySources[key.destination]
	delete(bucket, key)
	if len(bucket) == 0 {
		delete(p.availabilitySources, key.destination)
	}
}
