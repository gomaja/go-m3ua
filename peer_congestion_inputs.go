// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"cmp"
	"context"
	"errors"
	"slices"

	"github.com/gomaja/go-m3ua/messages/params"
)

// PeerCongestionInput keeps one member's received congestion separate from its
// local outbound transport observation. Active requires active membership in
// the current AS binding.
// Absence of a retained record means unknown peer congestion, not measured clear.
type PeerCongestionInput struct {
	Association    AssociationID
	Epoch          uint64
	Active         bool
	PeerCongestion []PeerCongestionState
	Transport      TransportCongestionState
}

// PeerCongestionVersion identifies one member and the independent transport
// observation collected for it. Active includes the current binding's active
// scope. TransportPresent distinguishes absent detector state from a present
// unknown state, including initialization before the first sample. Transport includes its epoch,
// configuration generation, sample timestamps and missed-sample counters.
type PeerCongestionVersion struct {
	Association       AssociationID
	Epoch             uint64
	Active            bool
	BindingGeneration uint64
	TransportPresent  bool
	Transport         TransportCongestionState
}

// CongestionInputVersion is a component version vector for one collection.
// Revision covers the SSNM store only; MembershipGeneration covers this AS.
type CongestionInputVersion struct {
	Revision             uint64
	MembershipGeneration uint64
	Peers                []PeerCongestionVersion
}

// CongestionInputSnapshot supplies bounded Endpoint-local inputs for an
// application or NIF decision, never an SPMC verdict or permission to replay
// SS7 TFC. RFC 4666 §1.4.6 leaves TFC triggering implementation-dependent (MAY).
// All slices and wire scopes are caller-owned. VersionsBefore and VersionsAfter
// bracket collection; a changed vector is retried once, then Consistent=false
// exposes both vectors. Revision is the retained SSNM revision collected.
type CongestionInputSnapshot struct {
	AS                   ASKey
	TrafficMode          uint32
	Peers                []PeerCongestionInput
	Revision             uint64
	MembershipGeneration uint64
	Consistent           bool
	VersionsBefore       CongestionInputVersion
	VersionsAfter        CongestionInputVersion
}

// SGPCongestionCandidate contains copied inputs for one SGP transit candidate.
// PeerCongestion contains only records matching the AS, wire network and DPC;
// it contains no Concerned Destination or historical trigger. The application
// or NIF owns any SPMC, transit-discard or TFC decision (RFC 4666 §1.4.6).
type SGPCongestionCandidate struct {
	AS              ASKey
	Association     AssociationID
	Epoch           uint64
	Destination     uint32
	MessagePriority uint8
	PeerCongestion  []PeerCongestionState
	Transport       TransportCongestionState
}

// SGPCongestionPolicy is an optional additional veto on SGP transit. False
// refuses this member with a known-not-sent DATA error. It cannot enable an
// inactive scope. Calls use copies outside Endpoint, AS and delivery locks,
// may run concurrently and may be repeated once when inputs change; they must
// not depend on invocation count. Nil leaves transit admission unchanged.
type SGPCongestionPolicy func(SGPCongestionCandidate) bool

type congestionMembership struct {
	association *Association
	active      bool
}

func (as *applicationServer) congestionMembership() ([]congestionMembership, uint64, uint32) {
	as.mu.Lock()
	defer as.mu.Unlock()
	members := make([]congestionMembership, 0, len(as.asps))
	for a, state := range as.asps {
		members = append(members, congestionMembership{a, state == StateASPActive && !as.closed})
	}
	slices.SortFunc(members, func(a, b congestionMembership) int { return cmp.Compare(a.association.ID(), b.association.ID()) })
	return members, as.membershipGeneration, as.trafficMode
}

func (e *Endpoint) congestionVersions(as *applicationServer) (CongestionInputVersion, []congestionMembership, uint32) {
	members, generation, mode := as.congestionMembership()
	v := CongestionInputVersion{MembershipGeneration: generation, Revision: e.ssnm.currentRevision(), Peers: make([]PeerCongestionVersion, 0, len(members))}
	for _, member := range members {
		a := member.association
		p := PeerCongestionVersion{Association: a.ID(), Epoch: a.Epoch()}
		p.Active = member.active && a.congestionBindingActive(as.key, as.key)
		if as.key.RoutingContextSet {
			p.BindingGeneration = a.dynamicASKeyVersionFor(as.key.RoutingContext, false)
		}
		if state := a.transportSnapshot.Load(); state != nil {
			p.TransportPresent = true
			p.Transport = *state
		}
		v.Peers = append(v.Peers, p)
	}
	return v, members, mode
}

func sameCongestionVersions(a, b CongestionInputVersion) bool {
	return a.Revision == b.Revision && a.MembershipGeneration == b.MembershipGeneration && slices.Equal(a.Peers, b.Peers)
}

func coherentCongestionVersion(v CongestionInputVersion) bool {
	for _, peer := range v.Peers {
		if peer.TransportPresent && peer.Transport.Epoch != peer.Epoch {
			return false
		}
	}
	return true
}

// CongestionInputs collects membership, retained peer reports and outbound
// transport pressure for a registered AS. It retries a changing component
// vector once. Consistent means these Endpoint-local observations were stable
// across collection; it does not certify cluster capacity. An absent key
// returns ErrNoConfiguredAS. No application callback runs during collection.
func (e *Endpoint) CongestionInputs(key ASKey) (CongestionInputSnapshot, error) {
	if e == nil {
		return CongestionInputSnapshot{}, ErrEndpointClosed
	}
	if !e.beginOperation() {
		return CongestionInputSnapshot{}, ErrEndpointClosed
	}
	defer e.endOperation()
	as, exists := e.as.lookup(normalizeOutboundASKey(key))
	if !exists {
		return CongestionInputSnapshot{}, ErrNoConfiguredAS
	}
	return e.collectCongestionInputs(as), nil
}

func (e *Endpoint) collectCongestionInputs(as *applicationServer) CongestionInputSnapshot {
	var result CongestionInputSnapshot
	for range 2 {
		before, members, mode := e.congestionVersions(as)
		retained := e.ssnm.snapshot()
		result = CongestionInputSnapshot{AS: as.key, TrafficMode: mode, Revision: retained.Revision, MembershipGeneration: before.MembershipGeneration, VersionsBefore: before, Peers: make([]PeerCongestionInput, 0, len(members))}
		byAssociation := make(map[AssociationID]int, len(members))
		for _, member := range members {
			a := member.association
			input := PeerCongestionInput{Association: a.ID(), Epoch: a.Epoch(), Active: member.active && a.congestionBindingActive(as.key, as.key), Transport: a.TransportCongestion()}
			byAssociation[input.Association] = len(result.Peers)
			result.Peers = append(result.Peers, input)
		}
		for _, record := range retained.PeerCongestion {
			if index, exists := byAssociation[record.Association]; exists && record.Epoch == result.Peers[index].Epoch && record.AS == as.key {
				result.Peers[index].PeerCongestion = append(result.Peers[index].PeerCongestion, record)
			}
		}
		result.VersionsAfter, _, _ = e.congestionVersions(as)
		result.Consistent = sameCongestionVersions(before, result.VersionsAfter) && coherentCongestionVersion(before) && coherentCongestionVersion(result.VersionsAfter)
		if result.Consistent {
			break
		}
	}
	return result
}

type sgpCongestionDecision struct {
	recoveryGeneration uint64
	unrestricted       bool
	consistent         bool
	version            CongestionInputVersion
	members            map[*Association]bool
	ranks              map[*Association]int
	endpoint           *Endpoint
	as                 *applicationServer
}

// prepareSGPCongestion runs before deliveryMu, including recovery delivery.
// RFC 4666 §§1.3.2.4 and 1.4.2.4 describe maintaining peer congestion and
// considering it in distribution; opt-in ranking is an engineering policy.
func (as *applicationServer) prepareSGPCongestion(data *params.ProtocolDataPayload, wireAS ASKey) *sgpCongestionDecision {
	policy := as.distribution
	if policy.congestionPolicy == nil && !policy.preferUncongestedPeers {
		return nil
	}
	members, initialGeneration, _ := as.congestionMembership()
	var e *Endpoint
	for _, member := range members {
		if member.association.endpoint != nil {
			e = member.association.endpoint
			break
		}
	}
	if e == nil {
		return nil
	}
	snapshot := e.collectCongestionInputs(as)
	decision := &sgpCongestionDecision{consistent: snapshot.Consistent && initialGeneration == snapshot.MembershipGeneration, version: snapshot.VersionsAfter, endpoint: e, as: as, members: make(map[*Association]bool), ranks: make(map[*Association]int)}
	byID := make(map[AssociationID]*Association, len(members))
	for _, member := range members {
		byID[member.association.ID()] = member.association
	}
	for _, input := range snapshot.Peers {
		if !input.Active {
			continue
		}
		a := byID[input.Association]
		if a == nil {
			continue
		}
		candidate := SGPCongestionCandidate{AS: wireAS, Association: input.Association, Epoch: input.Epoch, Destination: data.DestinationPointCode, MessagePriority: data.MessagePriority, Transport: input.Transport}
		for _, record := range input.PeerCongestion {
			if peerCongestionMatches(record, wireAS, data.DestinationPointCode) {
				candidate.PeerCongestion = append(candidate.PeerCongestion, record.clone())
			}
		}
		decision.ranks[a] = sgpCongestionRank(candidate, a.transportSnapshot.Load() != nil)
		decision.members[a] = policy.congestionPolicy == nil || policy.congestionPolicy(candidate)
	}
	return decision
}

func peerCongestionMatches(record PeerCongestionState, key ASKey, dpc uint32) bool {
	if record.Scope.NetworkAppearanceSet && (!key.NetworkAppearanceSet || record.Scope.NetworkAppearance != key.NetworkAppearance) {
		return false
	}
	mask := uint32(0xffffff)
	if record.Destination.Mask >= 24 {
		mask = 0
	} else {
		mask &^= (1 << record.Destination.Mask) - 1
	}
	return dpc&mask == record.Destination.PointCode&mask
}

func sgpCongestionRank(candidate SGPCongestionCandidate, transportPresent bool) int {
	rank := 0
	var latest *PeerCongestionState
	for _, record := range candidate.PeerCongestion {
		if latest == nil || record.Revision > latest.Revision {
			copy := record
			latest = &copy
		}
	}
	// Locally accepted chronology resolves overlapping ranges and RC subsets
	// for this peer/AS/DPC, just as for SSNM destination knowledge. A newer
	// clear supersedes an older matching onset (RFC 4666 §4.5.1).
	if latest != nil {
		if latest.Freshness == CongestionExpired || !latest.Congestion.LevelSet {
			rank = max(rank, 4)
		} else {
			rank = max(rank, int(latest.Congestion.Level))
		}
	}
	if transportPresent {
		if !candidate.Transport.Known {
			rank = max(rank, 4)
		} else {
			rank = max(rank, int(candidate.Transport.Level))
		}
	}
	return rank
}

func (d *sgpCongestionDecision) current() bool {
	if d == nil {
		return true
	}
	if d.unrestricted {
		return true
	}
	if !d.consistent {
		return false
	}
	current, _, _ := d.endpoint.congestionVersions(d.as)
	return sameCongestionVersions(d.version, current)
}

// withSGPCongestionAdmission releases every traffic barrier before invoking a
// policy again. A stale result from the final socket gate is known not sent.
func (c *Association) withSGPCongestionAdmission(ctx context.Context, key ASKey, payload *params.ProtocolDataPayload, stream uint16, submit func(*sgpCongestionDecision) error) error {
	var as *applicationServer
	if c.role == RoleSGP && c.as != nil {
		binding, err := c.resolveOutboundDataBinding(normalizeOutboundASKey(key))
		if err != nil {
			return newDataNotSent(key, stream, err)
		}
		as, _ = c.as.lookup(binding)
	}
	for range 2 {
		var decision *sgpCongestionDecision
		if as != nil {
			decision = as.prepareSGPCongestion(payload, key)
		}
		release, err := c.admitDataWriteContext(ctx, key)
		if err != nil {
			return newDataNotSent(key, stream, err)
		}
		if decision != nil && !decision.current() {
			release()
			continue
		}
		if decision != nil && !decision.members[c] {
			release()
			return newDataNotSent(key, stream, ErrNoMTPRoute)
		}
		err = submit(decision)
		release()
		if !errors.Is(err, ErrCongestionDecisionStale) {
			return err
		}
	}
	return newDataNotSent(key, stream, ErrCongestionDecisionStale)
}

func (c *Association) checkSGPCongestionDecision(key ASKey, stream uint16, decision *sgpCongestionDecision) error {
	if decision == nil {
		return nil
	}
	if !decision.current() {
		return newDataNotSent(key, stream, ErrCongestionDecisionStale)
	}
	decision.as.mu.Lock()
	active := !decision.as.closed && decision.as.state == ASActive && decision.as.asps[c] == StateASPActive
	expired := decision.recoveryGeneration != 0 && decision.recoveryGeneration <= decision.as.recoveryExpiredGen
	decision.as.mu.Unlock()
	if expired {
		return newDataNotSent(key, stream, errRecoveryDataExpired)
	}
	if !active {
		return newDataNotSent(key, stream, ErrRoutingContextNotActive)
	}
	if !c.congestionBindingActive(key, decision.as.key) {
		return newDataNotSent(key, stream, ErrRoutingContextNotActive)
	}
	if !decision.unrestricted && !decision.members[c] {
		return newDataNotSent(key, stream, ErrNoMTPRoute)
	}
	return nil
}

func (c *Association) congestionBindingActive(key, expected ASKey) bool {
	binding, err := c.resolveOutboundDataBinding(normalizeOutboundASKey(key))
	return err == nil && binding == expected && c.outboundASKeyActive(binding)
}

func (as *applicationServer) fenceRecoveryDecision(decision *sgpCongestionDecision, generation uint64) *sgpCongestionDecision {
	if generation == 0 {
		return decision
	}
	if decision == nil {
		decision = &sgpCongestionDecision{as: as, consistent: true, unrestricted: true}
	}
	decision.recoveryGeneration = generation
	return decision
}
