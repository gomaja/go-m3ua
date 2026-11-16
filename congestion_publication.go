package m3ua

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// CongestionPublicationConfig bounds automatic SCON and DAUD submission work.
// Nil and zero fields select engineering defaults: 64 slots, 32 per association,
// 64 KiB, 100 ms retry and 1 s between submitted reports per association. Negative fields and a
// per-association slot limit exceeding the total are invalid. Reservations are
// portable metadata charges, not a heap or kernel-memory reservation.
type CongestionPublicationConfig struct {
	MaxPending               int
	MaxPendingPerAssociation int
	MaxBytes                 int
	RetryInterval            time.Duration
	MinReportInterval        time.Duration
}

// CongestionPublicationKind identifies the automatic optional operation.
type CongestionPublicationKind uint8

const (
	// CongestionPublicationLocalSCON reports local queue/application congestion.
	CongestionPublicationLocalSCON CongestionPublicationKind = iota + 1
	// CongestionPublicationDAUD audits current destination knowledge.
	CongestionPublicationDAUD
)

// CongestionPublicationState is an owned optional-submission observation.
// SubmittedRevision means local transport acceptance, never peer delivery.
// LastError contains at most 256 diagnostic bytes and no DATA payload.
type CongestionPublicationState struct {
	Association  AssociationID
	Epoch        uint64
	Scope        WireScope
	Destinations []PointCodeRange
	Kind         CongestionPublicationKind
	// ConcernedDestination identifies the reservation's original DATA trigger.
	// A clear or status without that live queue source omits CD on the wire.
	ConcernedDestination    uint32
	ConcernedDestinationSet bool
	DesiredCongestion       CongestionState
	DesiredRevision         uint64
	SubmittedRevision       uint64
	Pending                 bool
	Attempts                uint64
	LastAttempt             time.Time
	LastError               string
}

// CongestionPublicationCounters counts automatic local work, not peer delivery.
// Coalesced includes repeated state; Deferred includes contention, rate and space
// deferral. Failed counts actual transport failures, including EAGAIN. Refused
// counts new reservations that could not fit; existing clears are never evicted.
type CongestionPublicationCounters struct{ Queued, Submitted, Coalesced, Deferred, Refused, Failed uint64 }

type congestionPublicationKey struct {
	association  AssociationID
	epoch        uint64
	as           ASKey
	scope        string
	destination  uint32
	mask         uint8
	concerned    uint32
	concernedSet bool
	kind         CongestionPublicationKind
}

type publicationRevision struct {
	live      atomic.Bool
	revision  uint64
	desired   CongestionState
	source    CongestionState
	concerned bool
}
type congestionPublication struct {
	key                                   congestionPublicationKey
	association                           *Association
	binding                               queueCongestionBinding
	partition                             SSNMPartition
	state                                 CongestionPublicationState
	job                                   *publicationRevision
	submitted                             CongestionState
	hasSubmitted, inflight, validateLocal bool
	next                                  time.Time
	charge                                int
}

func resolveCongestionPublication(config *CongestionPublicationConfig) (CongestionPublicationConfig, error) {
	c := CongestionPublicationConfig{}
	if config != nil {
		c = *config
	}
	if c.MaxPending < 0 || c.MaxPendingPerAssociation < 0 || c.MaxBytes < 0 || c.RetryInterval < 0 || c.MinReportInterval < 0 {
		return c, fmt.Errorf("%w: negative congestion publication limit", ErrInvalidRoleConfiguration)
	}
	if c.MaxPending == 0 {
		c.MaxPending = 64
	}
	if c.MaxPendingPerAssociation == 0 {
		c.MaxPendingPerAssociation = 32
	}
	if c.MaxBytes == 0 {
		c.MaxBytes = 64 << 10
	}
	if c.RetryInterval == 0 {
		c.RetryInterval = 100 * time.Millisecond
	}
	if c.MinReportInterval == 0 {
		c.MinReportInterval = time.Second
	}
	if c.MaxPendingPerAssociation > c.MaxPending {
		return c, fmt.Errorf("%w: publication per-association slots exceed total", ErrInvalidRoleConfiguration)
	}
	return c, nil
}

func clonePublicationState(p CongestionPublicationState) CongestionPublicationState {
	p.Scope = p.Scope.clone()
	p.Destinations = slices.Clone(p.Destinations)
	return p
}

// CongestionPublications returns owned current reservations, including admitted
// transient audits. Audit deadlines waiting for capacity remain in the separate
// bounded audit store; completed clears and lifecycle retirements leave no slot.
func (e *Endpoint) CongestionPublications() []CongestionPublicationState {
	if e == nil || e.ssnm == nil {
		return nil
	}
	s := e.ssnm
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]CongestionPublicationState, 0, len(s.publications))
	for _, p := range s.publications {
		out = append(out, clonePublicationState(p.state))
	}
	slices.SortFunc(out, func(a, b CongestionPublicationState) int {
		if n := cmp.Compare(a.Association, b.Association); n != 0 {
			return n
		}
		if n := cmp.Compare(a.DesiredRevision, b.DesiredRevision); n != 0 {
			return n
		}
		return cmp.Compare(a.ConcernedDestination, b.ConcernedDestination)
	})
	return out
}

// CongestionPublicationCounters returns the Endpoint-lifetime optional work counters.
func (e *Endpoint) CongestionPublicationCounters() CongestionPublicationCounters {
	if e == nil || e.ssnm == nil {
		return CongestionPublicationCounters{}
	}
	e.ssnm.mu.Lock()
	defer e.ssnm.mu.Unlock()
	return e.ssnm.publicationCounters
}

func publicationCharge(scope WireScope, destinations int) int {
	return 768 + 8*len(scope.RoutingContexts) + 8*destinations
}
func (s *ssnmState) publicationFitsLocked(association AssociationID, charge int) bool {
	cfg := s.publicationConfig
	if len(s.publications) >= cfg.MaxPending || charge > cfg.MaxBytes-s.publicationBytes {
		return false
	}
	count := 0
	for key := range s.publications {
		if key.association == association {
			count++
		}
	}
	return count < cfg.MaxPendingPerAssociation
}
func (s *ssnmState) removePublicationLocked(p *congestionPublication) {
	if s.publications[p.key] != p {
		return
	}
	if p.job != nil {
		p.job.live.Store(false)
	}
	p.state.Pending = false
	delete(s.publications, p.key)
	s.publicationBytes -= p.charge
}
func wirePublicationState(c *Association, scope WireScope, binding queueCongestionBinding, desired CongestionState) (CongestionState, bool) {
	key := binding.key
	if !key.NetworkAppearanceSet && scope.NetworkAppearanceSet {
		key.NetworkAppearance, key.NetworkAppearanceSet = scope.NetworkAppearance, true
	}
	profile := c.congestionProfile(key)
	if profile.Method == CongestionInternational {
		return CongestionState{Congested: desired.Congested}, false
	}
	if !desired.Congested {
		return CongestionState{LevelSet: true}, true
	}
	if desired.LevelSet {
		if desired.Level > profile.MaxLevel {
			desired.Level = profile.MaxLevel
		}
	}
	return desired, true
}

// updateLocalCongestionPublication consumes an already committed local group
// revision. Only the fixed-size reservation table is visited under store.mu;
// source history and Endpoint state are never scanned here.
func (e *Endpoint) updateLocalCongestionPublication(c *Association, scope WireScope, binding queueCongestionBinding, dpc, concerned uint32, concernedSet bool, desired CongestionState, revision uint64) {
	e.enqueueCongestionPublication(c, scope, binding, dpc, concerned, concernedSet, desired, revision, true)
}
func (e *Endpoint) enqueueCongestionPublication(c *Association, scope WireScope, binding queueCongestionBinding, dpc, concerned uint32, concernedSet bool, desired CongestionState, revision uint64, validateLocal bool) {
	if e == nil || e.ssnm == nil || c.role == RoleSGP {
		return
	}
	s := e.ssnm
	partition := c.ssnmPartitionFor(binding.key)
	key := congestionPublicationKey{association: c.ID(), epoch: binding.epoch, as: binding.key, scope: peerCongestionScopeKey(scope), destination: dpc, concerned: concerned, concernedSet: concernedSet, kind: CongestionPublicationLocalSCON}
	var observation *CongestionPublicationState
	var cause error
	s.mu.Lock()
	if s.closed || validateLocal && !s.localCongestionCurrentLocked(c.ID(), binding.epoch, binding.key, dpc, revision, desired) {
		s.mu.Unlock()
		return
	}
	if s.publicationConfig.MaxPending == 0 {
		s.publicationConfig, _ = resolveCongestionPublication(nil)
	}
	if s.publications == nil {
		s.publications = make(map[congestionPublicationKey]*congestionPublication)
	}
	found := false
	for k, p := range s.publications {
		if k.kind != CongestionPublicationLocalSCON || k.association != key.association || k.epoch != key.epoch || k.as != key.as || k.destination != key.destination {
			continue
		}
		if p.binding != binding {
			s.removePublicationLocked(p)
			continue
		}
		if k == key {
			found = true
		}
		if revision < p.state.DesiredRevision {
			continue
		}
		state, national := wirePublicationState(c, p.state.Scope, p.binding, desired)
		if !national && !state.Congested {
			s.removePublicationLocked(p)
			continue
		}
		same := state == p.state.DesiredCongestion
		if same && k == key {
			s.publicationCounters.Coalesced++
		}
		if p.job != nil {
			p.job.live.Store(false)
		}
		p.job = &publicationRevision{revision: revision, desired: state, source: desired, concerned: p.key.concernedSet && (!p.validateLocal || s.localQueueTriggerCurrentLocked(p.key.association, p.key.epoch, p.binding.key, p.key.scope, p.key.destination, p.key.concerned))}
		p.job.live.Store(true)
		p.state.DesiredCongestion, p.state.DesiredRevision = state, revision
		p.state.Pending = !p.hasSubmitted || p.submitted != state
		if !state.Congested && !p.hasSubmitted && !p.inflight {
			s.removePublicationLocked(p)
			continue
		}
		if k == key && !same {
			v := clonePublicationState(p.state)
			observation = &v
		}
	}
	if !found && desired.Congested {
		state, _ := wirePublicationState(c, scope, binding, desired)
		v := CongestionPublicationState{Association: c.ID(), Epoch: binding.epoch, Kind: CongestionPublicationLocalSCON, Scope: scope.clone(), Destinations: []PointCodeRange{{PointCode: dpc}}, ConcernedDestination: concerned, ConcernedDestinationSet: concernedSet, DesiredCongestion: state, DesiredRevision: revision, Pending: true}
		charge := publicationCharge(scope, 1)
		if !s.publicationFitsLocked(c.ID(), charge) {
			s.publicationCounters.Refused++
			v.Pending = false
			cause = fmt.Errorf("%w: optional publication reservation full", ErrSSNMResourceLoss)
			v.LastError = cause.Error()
			observation = &v
			s.publishLocked(SSNMEvent{Kind: SSNMResourceLossEvent, Revision: s.nextRevisionLocked(), Reason: "optional publication reservation full"})
		} else {
			job := &publicationRevision{revision: revision, desired: state, source: desired, concerned: concernedSet && (!validateLocal || s.localQueueTriggerCurrentLocked(c.ID(), binding.epoch, binding.key, key.scope, dpc, concerned))}
			job.live.Store(true)
			p := &congestionPublication{key: key, association: c, binding: binding, partition: partition, state: v, job: job, charge: charge, validateLocal: validateLocal}
			s.publications[key] = p
			s.publicationBytes += charge
			s.publicationCounters.Queued++
			v = clonePublicationState(v)
			observation = &v
		}
	}
	s.mu.Unlock()
	if observation != nil {
		c.notifyManagement(&ManagementIndication{Kind: ManagementCongestionPublication, CongestionPublication: observation, Cause: cause, Description: "optional congestion publication state"})
	}
}

func publicationMessage(p *congestionPublication, job *publicationRevision) *messages.SignallingCongestion {
	var na, rc, cd, level *params.Param
	if p.state.Scope.NetworkAppearanceSet {
		na = params.NewNetworkAppearance(p.state.Scope.NetworkAppearance)
	}
	if p.state.Scope.RoutingContextSet {
		rc = params.NewRoutingContext(p.state.Scope.RoutingContexts...)
	}
	// RFC 4666 §3.4.4: CD identifies an actual ASP DATA trigger. Recovery has
	// no new triggering OPC, while its reservation retains the original key.
	if job.desired.Congested && job.concerned && p.association.role == RoleASP {
		cd = params.NewConcernedDestination(p.key.concerned)
	}
	if job.desired.LevelSet {
		level = params.NewCongestionIndications(job.desired.Level)
	}
	return messages.NewSignallingCongestion(na, rc, params.NewAffectedPointCode(p.key.destination), cd, level, nil)
}
func permanentPublicationError(err error) bool {
	return errors.Is(err, ErrAssociationClosed) || errors.Is(err, ErrEndpointClosed) || errors.Is(err, ErrInvalidState) || errors.Is(err, ErrRoutingContextNotActive) || errors.Is(err, ErrNotEstablished)
}
func (s *ssnmState) retireCongestionPublicationsLocked(association AssociationID, matches func(ASKey) bool) {
	for _, p := range s.publications {
		if p.key.association == association && (matches == nil || matches(p.binding.key)) {
			s.removePublicationLocked(p)
		}
	}
}

func (e *Endpoint) runLocalCongestionPublications(now time.Time, pass *optionalPublicationPass) {
	s := e.ssnm
	s.mu.Lock()
	type candidate struct {
		p        *congestionPublication
		next     time.Time
		revision uint64
	}
	pending := make([]candidate, 0, len(s.publications))
	for _, p := range s.publications {
		if p.key.kind == CongestionPublicationLocalSCON && p.state.Pending {
			pending = append(pending, candidate{p, p.next, p.state.DesiredRevision})
		}
	}
	s.mu.Unlock()
	slices.SortFunc(pending, func(a, b candidate) int {
		if n := a.next.Compare(b.next); n != 0 {
			return n
		}
		return cmp.Compare(a.revision, b.revision)
	})
	for _, candidate := range pending {
		p := candidate.p
		if e.ctx.Err() != nil {
			return
		}
		s.mu.Lock()
		if s.publications[p.key] != p || !p.state.Pending || p.inflight {
			s.mu.Unlock()
			continue
		}
		if pass.used[p.key.association] || pass.remaining <= 0 || p.charge > pass.bytes || now.Before(p.next) || now.Before(p.association.optionalPublicationNext) {
			s.publicationCounters.Deferred++
			s.mu.Unlock()
			continue
		}
		job := p.job
		p.inflight = true
		pass.used[p.key.association] = true
		pass.remaining--
		pass.bytes -= p.charge
		s.mu.Unlock()
		current := func() bool {
			if !job.live.Load() {
				return false
			}
			if p.validateLocal {
				valid, acquired := s.tryLocalCongestionCurrent(p.key.association, p.key.epoch, p.binding.key, p.key.destination, job.revision, job.source, p.key.scope, p.key.concerned, job.concerned)
				return acquired && valid
			}
			return true
		}
		attempted, err := p.association.tryOptionalSSNMFrame(publicationMessage(p, job), []queueCongestionBinding{p.binding}, p.key.epoch, current)
		issuedAt := s.automationNow()
		if issuedAt.Before(now) {
			issuedAt = now
		}
		var observation CongestionPublicationState
		s.mu.Lock()
		p.inflight = false
		if attempted || sendBufferFull(err) {
			p.state.Attempts++
			p.state.LastAttempt = issuedAt
			s.publicationCounters.Failed += boolCount(err != nil)
			if sendBufferFull(err) {
				s.publicationCounters.Deferred++
			}
			if err != nil {
				p.state.LastError = boundedPublicationError(err)
				p.next = issuedAt.Add(s.publicationConfig.RetryInterval)
				p.association.optionalPublicationNext = p.next
			} else {
				p.state.LastError = ""
				p.hasSubmitted = true
				p.submitted = job.desired
				p.state.SubmittedRevision = job.revision
				p.next = issuedAt.Add(s.publicationConfig.MinReportInterval)
				p.association.optionalPublicationNext = p.next
				s.publicationCounters.Submitted++
			}
		}
		if s.publications[p.key] == p {
			if s.closed || e.ctx.Err() != nil || associationDone(p.association) || p.job == nil || !p.job.live.Load() {
				s.removePublicationLocked(p)
			} else if permanentPublicationError(err) {
				p.state.Pending = false
				p.state.LastError = boundedPublicationError(err)
				s.removePublicationLocked(p)
			} else {
				p.state.Pending = !p.hasSubmitted || p.submitted != p.state.DesiredCongestion
				if !p.state.DesiredCongestion.Congested && (!p.hasSubmitted || !p.state.Pending) {
					s.removePublicationLocked(p)
				}
			}
		}
		if !attempted && !sendBufferFull(err) && !permanentPublicationError(err) {
			s.publicationCounters.Deferred++
		}
		observation = clonePublicationState(p.state)
		if attempted || sendBufferFull(err) {
			s.publishPublicationStateLocked(observation, err)
		}
		s.mu.Unlock()
		if attempted || sendBufferFull(err) {
			p.association.notifyManagement(&ManagementIndication{Kind: ManagementCongestionPublication, CongestionPublication: &observation, Cause: err, Description: "optional congestion publication attempt"})
		}
	}
}
func boolCount(b bool) uint64 {
	if b {
		return 1
	}
	return 0
}
func boundedPublicationError(err error) string {
	if err == nil {
		return ""
	}
	s := err.Error()
	if len(s) > 256 {
		s = s[:256]
	}
	return strings.Clone(s)
}

type optionalPublicationPass struct {
	used      map[AssociationID]bool
	remaining int
	bytes     int
}

func (s *ssnmState) publishPublicationStateLocked(state CongestionPublicationState, err error) {
	reason := "submitted"
	if err != nil {
		reason = "send-failed"
	}
	if sendBufferFull(err) {
		reason = "send-deferred"
	}
	if permanentPublicationError(err) {
		reason = "reporter-retired"
	}
	state = clonePublicationState(state)
	s.publishLocked(SSNMEvent{Kind: SSNMPublicationOutcomeEvent, PublicationKind: state.Kind, CongestionPublication: &state, PublicationCount: 1, Revision: s.nextRevisionLocked(), Reason: reason, Report: SSNMReport{Association: state.Association, Scope: state.Scope.clone()}})
}

// One lifecycle event scans bounded publication capacity once, alongside the
// deadline-store retirement. Nil selects the whole reporter.
func (s *ssnmState) retirePublicationPartitionsLocked(association AssociationID, partitions map[SSNMPartition]struct{}) {
	for _, p := range s.publications {
		s.work.visitJob()
		_, selected := partitions[p.partition]
		if p.key.association == association && (partitions == nil || selected) {
			s.removePublicationLocked(p)
		}
	}
}
