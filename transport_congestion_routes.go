package m3ua

// transportChanged projects a detector transition, never DATA. The transport
// revision is separate from canonical received SSNM knowledge.
func (r *aspRoutes) transportChanged(a *Association) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.associations[a]; !ok {
		return
	}
	r.transportRevision++
	r.publish(r.recomputeLocked(r.associationEligibleRoutes[a]))
}

// Canonical knowledge belongs to an SG/AS, not to the reporting association's
// active routes. Reports admitted before ASPAC Ack and knowledge invalidation
// can therefore change the active siblings' eligibility (RFC 4666 §4.5.2.2).
func (r *aspRoutes) canonicalStateChanged(partitions []SSNMPartition) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	affected := make(map[MTPRouteID]struct{})
	for _, partition := range partitions {
		if partition.Kind != SSNMCanonicalPartition {
			continue
		}
		for _, route := range r.config.mtpRoutes {
			for _, gateway := range route.gateways {
				if gateway.id != partition.SignallingGateway {
					continue
				}
				for _, candidate := range gateway.candidates {
					if candidate.applicationServer == partition.ApplicationServer {
						affected[route.id] = struct{}{}
						break
					}
				}
			}
		}
	}
	r.publish(r.recomputeLocked(affected))
}

func transportASPStatus(a *Association) aspDestinationStatus {
	s := a.TransportCongestion()
	// No observation has been made yet; the public detector snapshot still
	// reports Known=false. A failed observation carries explicit uncertainty.
	if a.transportSnapshot.Load() == nil {
		return aspDestinationStatus{}
	}
	if !s.Known {
		return aspDestinationStatus{transportUnknown: true}
	}
	return aspDestinationStatus{congested: s.Level > 0, congestionLevel: s.Level, congestionLevelSet: true, transportLevel: s.Level}
}

func combineASPTransport(received, transport aspDestinationStatus) aspDestinationStatus {
	received.transportLevel = transport.transportLevel
	received.transportUnknown = transport.transportUnknown
	if !transport.congested {
		if !received.congested && transport.congestionLevelSet {
			received.congestionLevel = 0
			received.congestionLevelSet = true
		}
		return received
	}
	if !received.congested || received.congestionLevelSet && transport.congestionLevel > received.congestionLevel {
		received.congested = true
		received.congestionLevel = transport.congestionLevel
		received.congestionLevelSet = true
	}
	return received
}

type aspGatewayTransportStatus struct {
	pressure         aspDestinationStatus
	availabilityRank int
	usable           bool
}

func (r *aspRoutes) gatewayTransportLocked(gateway aspSignallingGatewayConfig, route MTPRouteID, pointCode uint32) aspGatewayTransportStatus {
	best := aspDestinationStatus{}
	found := false
	bestAvailability := 0
	for _, sgp := range gateway.sgps {
		identity := SGPIdentity{SignallingGateway: gateway.id, SignallingGatewayProcess: sgp.id}
		// Use MTPTransfer's ordered AS, binding, active-state and destination
		// availability checks. A DUNA through an earlier AS must not hide the
		// usable AS's transport contribution (RFC 4666 §4.5.2.2).
		selected, usable, _ := r.transferSGPCandidateLocked(sgp, identity, route, pointCode, aspCongestionDecision{}, r.ssnm)
		if !usable {
			continue
		}
		memberBest := aspDestinationStatus{}
		memberFound := false
		// Canonical received congestion belongs to this selected AS. Combine
		// it with each member before comparing alternatives (RFC 4666 §1.4.6).
		received := aspDestinationStatus{
			congested:          selected.status.congested,
			congestionLevel:    selected.status.congestionLevel,
			congestionLevelSet: selected.status.congestionLevelSet,
		}
		for _, member := range selected.members {
			status := combineASPTransport(received, transportASPStatus(member.association))
			if !memberFound || aspCongestionRank(status) < aspCongestionRank(memberBest) {
				memberBest = status
				memberFound = true
			}
		}

		if !memberFound {
			continue
		}
		if !found {
			best = memberBest
			bestAvailability = selected.status.availabilityRank()
			found = true
			continue
		}
		if gateway.sgpSelection == RouteSelectionBroadcast {
			best = mostCongestedASPStatus(best, memberBest)
			bestAvailability = min(bestAvailability, selected.status.availabilityRank())
		} else if rank := selected.status.availabilityRank(); rank < bestAvailability {
			best, bestAvailability = memberBest, rank
		} else if rank == bestAvailability {
			best = leastCongestedASPStatus(best, memberBest)
		}
	}
	return aspGatewayTransportStatus{pressure: best, availabilityRank: bestAvailability, usable: found}
}

func (r *aspRoutes) transportSelectionStatus(a *Association, status aspSelectionStatus) aspSelectionStatus {
	t := transportASPStatus(a)
	if r.config.preferUncongestedTransport && a.transportSnapshot.Load() == nil {
		t.transportUnknown = true
	}
	status.transportUnknown = t.transportUnknown
	if !t.congested {
		return status
	}
	status.congestionSet = true
	if !status.congested || status.congestionLevelSet && t.congestionLevel > status.congestionLevel {
		status.congested = true
		status.congestionLevel = t.congestionLevel
		status.congestionLevelSet = true
	}
	return status
}

func mostCongestedASPStatus(first, second aspDestinationStatus) aspDestinationStatus {
	first.transportLevel = max(first.transportLevel, second.transportLevel)
	first.transportUnknown = first.transportUnknown || second.transportUnknown
	if second.congested && (!first.congested || first.congestionLevelSet && (!second.congestionLevelSet || second.congestionLevel > first.congestionLevel)) {
		first.congested = true
		first.congestionLevel = second.congestionLevel
		first.congestionLevelSet = second.congestionLevelSet
	}
	return first
}

func requiredASPSelectionStatus(first, second aspSelectionStatus) aspSelectionStatus {
	if second.availabilityRank() < first.availabilityRank() {
		first.availability = second.availability
		first.availabilitySet = second.availabilitySet
	}
	first.transportUnknown = first.transportUnknown || second.transportUnknown
	if second.congested && (!first.congested || first.congestionLevelSet && (!second.congestionLevelSet || second.congestionLevel > first.congestionLevel)) {
		first.congested = true
		first.congestionSet = true
		first.congestionLevel = second.congestionLevel
		first.congestionLevelSet = second.congestionLevelSet
	}
	return first
}
