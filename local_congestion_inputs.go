package m3ua

import (
	"cmp"
	"slices"
)

// Local ASP and DE traffic-to-local scopes need not have an Endpoint-wide AS
// object. Copy only association pointers under e.mu; scope resolution, version
// collection and snapshots happen after releasing it.
func (e *Endpoint) localCongestionInputVersions(key ASKey) (CongestionInputVersion, []congestionMembership, uint32) {
	e.mu.Lock()
	associations := make([]*Association, 0, len(e.associationsByID))
	for _, c := range e.associationsByID {
		associations = append(associations, c)
	}
	e.mu.Unlock()
	slices.SortFunc(associations, func(a, b *Association) int { return cmp.Compare(a.ID(), b.ID()) })
	version := CongestionInputVersion{Revision: e.ssnm.currentRevision()}
	var members []congestionMembership
	var mode uint32
	modeSet := false
	for _, c := range associations {
		if !slices.Contains(c.configuredLocalASKeysForStatus(), key) {
			continue
		}
		active := !c.terminating.Load() && c.localASPStateForStatus(key) == StateASPActive
		peer := PeerCongestionVersion{LocalGeneration: c.localCongestionInputGeneration(), Association: c.ID(), Epoch: c.Epoch(), Active: active, BindingGeneration: c.queueCongestionBinding(key, WireScope{}).version}
		if state := c.transportSnapshot.Load(); state != nil {
			peer.TransportPresent = true
			peer.Transport = *state
		}
		version.Peers = append(version.Peers, peer)
		version.MembershipGeneration += c.localQueueLifetimeRevision.Load()
		members = append(members, congestionMembership{association: c, active: active})
		configured, _ := c.localTrafficModePolicy().configuredForASKey(key)
		if !modeSet {
			mode = configured
			modeSet = true
		} else if mode != configured {
			mode = 0
		}
	}
	return version, members, mode
}

func (e *Endpoint) collectLocalCongestionInputs(key ASKey) (CongestionInputSnapshot, error) {
	var result CongestionInputSnapshot
	for range 2 {
		before, members, mode := e.localCongestionInputVersions(key)
		if len(members) == 0 {
			return CongestionInputSnapshot{}, ErrNoConfiguredAS
		}
		result = CongestionInputSnapshot{AS: key, TrafficMode: mode, Revision: before.Revision, MembershipGeneration: before.MembershipGeneration, VersionsBefore: before, Local: e.ssnm.localCongestionSnapshot(key), Peers: make([]PeerCongestionInput, 0, len(members))}
		for _, member := range members {
			c := member.association
			result.Peers = append(result.Peers, PeerCongestionInput{Association: c.ID(), Epoch: c.Epoch(), Active: member.active, Transport: c.TransportCongestion()})
		}
		result.VersionsAfter, _, _ = e.localCongestionInputVersions(key)
		result.Consistent = sameCongestionVersions(before, result.VersionsAfter) && coherentCongestionVersion(before) && coherentCongestionVersion(result.VersionsAfter)
		if result.Consistent {
			break
		}
	}
	return result, nil
}

// Retained local DE inputs can belong to an association absent from the peer
// AS membership. Only bucket identities are collected under the store lock;
// owned pointer lookups finish before any association version is read.
func (e *Endpoint) appendLocalSourceVersions(key ASKey, version *CongestionInputVersion) {
	type seed struct {
		association    AssociationID
		epoch, binding uint64
	}
	s := e.ssnm
	s.mu.Lock()
	seeds := make([]seed, 0)
	for association, byAS := range s.localByAssociation {
		if bucket := byAS[key]; bucket != nil {
			seeds = append(seeds, seed{association, bucket.binding.epoch, bucket.binding.version})
		}
	}
	s.mu.Unlock()
	if len(seeds) == 0 {
		return
	}
	e.mu.Lock()
	owned := make(map[AssociationID]*Association, len(seeds))
	for _, source := range seeds {
		owned[source.association] = e.associationsByID[source.association]
	}
	e.mu.Unlock()
	indices := make(map[AssociationID]int, len(version.Peers))
	for i, peer := range version.Peers {
		indices[peer.Association] = i
	}
	for _, source := range seeds {
		c := owned[source.association]
		if i, member := indices[source.association]; member {
			if c == nil {
				version.Peers[i].LocalGeneration |= 1
			}
			continue
		}
		peer := PeerCongestionVersion{Association: source.association, Epoch: source.epoch, BindingGeneration: source.binding}
		if c == nil {
			peer.LocalGeneration = 1
		} else {
			peer.Epoch = c.Epoch()
			peer.LocalGeneration = c.localCongestionInputGeneration()
			if state := c.transportSnapshot.Load(); state != nil {
				peer.TransportPresent = true
				peer.Transport = *state
			}
		}
		version.Peers = append(version.Peers, peer)
	}
	slices.SortFunc(version.Peers, func(a, b PeerCongestionVersion) int { return cmp.Compare(a.Association, b.Association) })
}
