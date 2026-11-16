package m3ua

import (
	"cmp"
	"errors"
	"slices"
	"sync/atomic"
	"time"

	"github.com/gomaja/go-m3ua/messages"
)

type destinationAuditKey struct {
	association AssociationID
	epoch       uint64
	scope       string
	destination ssnmDestinationKey
}

type destinationAuditJob struct {
	originIndex                                                 map[auditOriginIdentity]struct{}
	work                                                        *ssnmWorkCount
	info                                                        string
	key                                                         destinationAuditKey
	scope                                                       WireScope
	bindings                                                    []queueCongestionBinding
	partitions                                                  []SSNMPartition
	origins                                                     []destinationAuditOrigin
	due                                                         time.Time
	congestion, availability, isolation                         bool
	generation                                                  uint64
	congestionRevision, availabilityRevision, isolationRevision uint64
	live                                                        atomic.Bool
	outcome                                                     string
}

// Each immutable origin adds 80 accounted bytes (partition, deadline and
// cause flags) to its 64-byte lifetime binding. The source scope and unique
// partition list retain their original separate charges.
func (j *destinationAuditJob) bytes() int {
	return 256 + len(j.info) + 8*len(j.scope.RoutingContexts) + 144*len(j.bindings) + 64*len(j.partitions)
}

func (s *ssnmState) auditOutcomeLocked(job *destinationAuditJob, outcome string, batches ...*auditOutcomeBatch) {
	if job.outcome == outcome {
		return
	}
	job.outcome = outcome
	if len(batches) > 0 {
		batches[0].add(job, outcome)
		return
	}
	var batch auditOutcomeBatch
	batch.add(job, outcome)
	batch.publishLocked(s)
}

func (s *ssnmState) removeAuditLocked(key destinationAuditKey, outcome string, batches ...*auditOutcomeBatch) {
	if job := s.audits[key]; job != nil {
		if outcome != "" {
			s.auditOutcomeLocked(job, outcome, batches...)
		}
		job.live.Store(false)
		s.bytes -= job.bytes()
		delete(s.audits, key)
		s.forgetAuditIndexLocked(key)
	}
}

func (c *Association) scheduleDestinationAudit(report SSNMReport, isolation bool, info ...string) {
	if c.role != RoleASP {
		return
	}
	admission := c.captureReceivedSSNMAdmission(report.Scope)
	text := ""
	if len(info) > 0 {
		text = info[0]
	}
	_ = c.withReceivedSSNMAdmission(admission, func() error { c.scheduleDestinationAuditPrepared(report, isolation, text, admission); return nil })
}

// scheduleDestinationAuditPrepared consumes the same frozen receive-side
// membership generation that admitted the report. Its caller fences binding
// replacement while this helper admits the corresponding audit origins.
func (c *Association) scheduleDestinationAuditPrepared(report SSNMReport, isolation bool, info string, admission receivedSSNMAdmission) {
	s := c.ssnmStore()
	if s == nil || s.auditing == nil || c.role != RoleASP || report.Source != SSNMPeerReport {
		return
	}
	if !isolation {
		switch report.Kind {
		case SSNMDestinationAvailableReport, SSNMDestinationUnavailableReport, SSNMDestinationRestrictedReport, SSNMSignallingCongestionReport:
		default:
			return
		}
	}
	keys, bindings, partitions := admission.keys, admission.bindings, admission.partitions
	originPartitions := admission.bindingPartitions
	if len(bindings) == 0 || len(originPartitions) != len(bindings) || len(admission.nationalByBinding) != len(bindings) {
		return
	}
	// The report retains its original wire list. DAUD submits the same logical
	// scope as a set, including accepted duplicate RC values (RFC 4666 §4.5.3).
	scope := report.Scope.clone()
	scopeKey := peerCongestionScopeKey(scope)
	epoch := admission.epoch
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	if current, live := s.liveSources[c.ID()]; !live || current != admission.epoch {
		return
	}
	if s.audits == nil {
		s.audits = make(map[destinationAuditKey]*destinationAuditJob)
	}
	now := s.automationNow()
	var outcomes auditOutcomeBatch
	defer outcomes.publishLocked(s)
	if !isolation && (report.Kind == SSNMDestinationAvailableReport || report.Kind == SSNMSignallingCongestionReport && report.CongestionLevelSet && report.CongestionLevel == 0) {
		s.updateAuditOriginsLocked(c.ID(), epoch, report, keys, scopeKey, now)
		return
	}
	originView := s.prepareAuditOriginUpdateLocked(c.ID(), epoch, report, keys)
	seenDestinations := make(map[ssnmDestinationKey]bool, len(report.Destinations))
	for _, destination := range report.Destinations {
		destinationKey := ssnmDestinationKeyFor(destination)
		if seenDestinations[destinationKey] {
			continue
		}
		seenDestinations[destinationKey] = true
		// Stage the move while holding the store lock. A refused replacement must
		// retain its previous origins and their charges rather than dropping work.
		moved := make(map[destinationAuditKey]*destinationAuditJob)
		beforeBytes := s.bytes
		if !isolation {
			single := report
			single.Destinations = []PointCodeRange{destination}
			s.updateAuditOriginsFromIndexLocked(c.ID(), epoch, single, scopeKey, now, originView, moved)
		}
		key := destinationAuditKey{c.ID(), epoch, scopeKey, destinationKey}
		old := s.audits[key]
		job := &destinationAuditJob{key: key, scope: scope, bindings: bindings, partitions: partitions, origins: make([]destinationAuditOrigin, len(bindings))}
		if old != nil {
			job.info = old.info
			job.outcome = old.outcome
			job.congestionRevision, job.availabilityRevision, job.isolationRevision = old.congestionRevision, old.availabilityRevision, old.isolationRevision
		}
		oldOrigins := make(map[queueCongestionBinding]destinationAuditOrigin)
		if old != nil {
			for i, binding := range old.bindings {
				s.work.visitAS()
				if i < len(old.origins) {
					if _, held := oldOrigins[binding]; !held {
						oldOrigins[binding] = old.origins[i]
					}
				}
			}
		}
		for i, binding := range bindings {
			origin := destinationAuditOrigin{partition: originPartitions[i], due: now.Add(s.auditing.Period)}
			s.work.visitAS()
			if previous, held := oldOrigins[binding]; held {
				origin = previous
			}
			switch {
			case isolation:
				job.isolationRevision = s.revision
				job.info = info
				origin.isolation = true
				origin.due = now
			case report.Kind == SSNMSignallingCongestionReport:
				job.congestionRevision = s.revision
				origin.congestion = admission.nationalByBinding[i]
				origin.due = now.Add(s.auditing.Period)
			case report.Kind == SSNMDestinationUnavailableReport || report.Kind == SSNMDestinationRestrictedReport:
				job.availabilityRevision = s.revision
				origin.availability = true
				origin.due = now.Add(s.auditing.Period)
			default:
				continue
			}
			job.origins[i] = origin
		}
		job.summarizeOrigins()
		if !job.congestion && !job.availability && !job.isolation {
			s.removeAuditLocked(key, "")
			continue
		}
		extra := job.bytes()
		if old != nil {
			extra -= old.bytes()
		}
		if old == nil && len(s.audits) >= s.auditing.MaxPending || s.bytes+extra > s.limits.MaxBytes {
			for movedKey, original := range moved {
				if current := s.audits[movedKey]; current != nil {
					current.live.Store(false)
				}
				original.live.Store(true)
				s.retainAuditLocked(original)
			}
			s.bytes = beforeBytes
			s.auditOutcomeLocked(job, "queue-full", &outcomes)
			continue
		}
		if old != nil {
			old.live.Store(false)
		}
		s.auditGeneration++
		job.generation = s.auditGeneration
		job.live.Store(true)
		s.retainAuditLocked(job)
		originView.addJob(job.key, s.work)
		s.bytes += extra
	}
}

// reconcileDestinationAudits runs after binding/state commits. It never writes
// or joins the worker. Activation requests are scheduled once per live binding
// generation, including additional AS activation on an already active ASP.
func (c *Association) reconcileDestinationAudits() {
	s := c.ssnmStore()
	if s == nil || s.auditing == nil || c.role != RoleASP {
		return
	}
	state := c.State()
	active := state == StateASPActive && !c.terminating.Load()
	bindings := make(map[ASKey]queueCongestionBinding)
	eligible := make(map[ASKey]queueCongestionBinding)
	if !c.terminating.Load() && (state == StateASPActive || state == StateASPInactive) {
		pending := make(map[uint32]bool)
		for _, rc := range c.pendingTAckRoutingContexts(requestAspActive) {
			pending[rc] = true
		}
		contextlessPending := c.pendingTAckContextless(requestAspActive)
		for _, key := range c.configuredLocalASKeysForStatus() {
			acknowledged := c.optionalAuditASActive(key)
			awaiting := contextlessPending && !key.RoutingContextSet || key.RoutingContextSet && pending[key.RoutingContext]
			binding := c.queueCongestionBinding(key, WireScope{})
			if active && acknowledged {
				bindings[key] = binding
				eligible[key] = binding
			}
			if awaiting || state == StateASPInactive && c.optionalAuditPositiveAck(key) {
				eligible[key] = binding
			}
		}
	}
	// Preserve activation bookkeeping while a lifetime change closes receive
	// admission. Tracker -> store orders this check with reset/forget cleanup;
	// post-closure binding/state reconciliation can schedule configured work.
	tracker := &c.sconSubmissions
	tracker.mu.Lock()
	if tracker.optionalChanging != nil {
		tracker.mu.Unlock()
		return
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		tracker.mu.Unlock()
		return
	}
	var outcomes auditOutcomeBatch
	for key := range s.auditByAssociation[c.ID()] {
		job := s.audits[key]
		s.work.visitJob()
		if key.association != c.ID() {
			continue
		}
		next := job.clone()
		changed := false
		for i, binding := range job.bindings {
			if key.epoch != c.Epoch() || eligible[binding.key] != binding {
				next.origins[i] = destinationAuditOrigin{partition: next.origins[i].partition}
				changed = true
			}
		}
		if changed {
			next.summarizeOrigins()
			if !next.congestion && !next.availability && !next.isolation {
				s.removeAuditLocked(key, "reporter-retired", &outcomes)
			} else {
				s.replaceAuditLocked(job, next)
			}
		}
	}
	outcomes.publishLocked(s)
	if s.auditActivated == nil {
		s.auditActivated = make(map[AssociationID]map[ASKey]queueCongestionBinding)
	}
	old := s.auditActivated[c.ID()]
	changed := make(map[ASKey]bool)
	for key, binding := range bindings {
		if previous, held := old[key]; !held || previous != binding {
			changed[key] = true
		}
	}
	if len(bindings) == 0 {
		delete(s.auditActivated, c.ID())
	} else {
		s.auditActivated[c.ID()] = bindings
	}
	s.mu.Unlock()
	tracker.mu.Unlock()
	if len(changed) == 0 {
		return
	}
	for _, request := range s.auditing.OnActivation {
		relevant := false
		requested, _ := outboundSSNMASKeys(request.Scope, c.configuredLocalASKeysForStatus())
		for _, key := range requested {
			relevant = relevant || changed[key]
		}
		if !relevant {
			continue
		}
		parameters, err := c.prepareASPSSNM(request.Scope, request.Destinations, request.Info)
		if err != nil {
			continue
		}
		_ = parameters
		c.scheduleDestinationAudit(SSNMReport{Kind: SSNMDestinationStateAuditReport, Source: SSNMPeerReport, Scope: request.Scope, Destinations: request.Destinations}, true, request.Info)
	}
}

func (s *ssnmState) retireDestinationAuditsLocked(association AssociationID, partition *SSNMPartition) {
	var partitions map[SSNMPartition]struct{}
	if partition != nil {
		partitions = map[SSNMPartition]struct{}{*partition: {}}
	}
	s.retireDestinationAuditPartitionsLocked(association, partitions)
}

// A lifecycle event selects all withdrawn partitions before visiting jobs.
// Nil selects the whole reporter; each immutable origin slice is copied once.
func (s *ssnmState) retireDestinationAuditPartitionsLocked(association AssociationID, partitions map[SSNMPartition]struct{}) {
	if partitions != nil && len(partitions) == 0 {
		return
	}
	s.retirePublicationPartitionsLocked(association, partitions)
	var outcomes auditOutcomeBatch
	for key := range s.auditByAssociation[association] {
		job := s.audits[key]
		s.work.visitJob()
		if key.association != association {
			continue
		}
		var next *destinationAuditJob
		for i, origin := range job.origins {
			_, selected := partitions[origin.partition]
			if partitions == nil || selected {
				if next == nil {
					next = job.clone()
				}
				next.origins[i] = destinationAuditOrigin{partition: origin.partition}
			}
		}
		if next != nil {
			next.summarizeOrigins()
			if !next.congestion && !next.availability && !next.isolation {
				s.removeAuditLocked(key, "reporter-retired", &outcomes)
			} else {
				s.replaceAuditLocked(job, next)
			}
		}
	}
	if partitions == nil {
		delete(s.auditActivated, association)
	}
	outcomes.publishLocked(s)
}

func (e *Endpoint) startCongestionAutomation() {
	if e.ssnm.validity == 0 && e.ssnm.auditing == nil {
		return
	}
	e.ensureCongestionAutomation()
}

func (e *Endpoint) ensureCongestionAutomation() {
	e.congestionAutomationOnce.Do(func() {
		interval := 100 * time.Millisecond
		if after := e.ssnm.validity; after > 0 && after < interval {
			interval = after
		}
		if cfg := e.ssnm.auditing; cfg != nil && cfg.Period < interval {
			interval = cfg.Period
		}
		if interval < time.Millisecond {
			interval = time.Millisecond
		}
		if !e.beginOperation() {
			return
		}
		done := make(chan struct{})
		e.congestionAutomationDone = done
		go func() {
			defer e.endOperation()
			defer close(done)
			timer := time.NewTicker(interval)
			defer timer.Stop()
			for {
				select {
				case <-e.ctx.Done():
					return
				case <-timer.C:
					if e.ctx.Err() != nil {
						return
					}
					e.runCongestionAutomation(e.ssnm.automationNow())
				}
			}
		}()
	})
}

func (e *Endpoint) runCongestionAutomation(now time.Time) {
	s := e.ssnm
	if !s.publicationPass.TryLock() {
		return
	}
	defer s.publicationPass.Unlock()
	pass := &optionalPublicationPass{used: make(map[AssociationID]bool), remaining: s.publicationConfig.MaxPending, bytes: s.publicationConfig.MaxBytes}
	e.runLocalCongestionPublications(now, pass)
	select {
	case <-e.ctx.Done():
		return
	default:
	}
	var outcomes auditOutcomeBatch
	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if !s.closed && e.ctx.Err() == nil {
			outcomes.publishLocked(s)
		}
	}()
	changed := s.expireCongestion(now, e.ctx.Done())
	if e.ctx.Err() != nil {
		return
	}
	if e.aspRoutes != nil && len(changed) > 0 {
		e.aspRoutes.canonicalStateChanged(changed, MTPStatusValidityExpired)
	}
	if e.role == RoleIPSP && len(changed) > 0 {
		e.refreshIPSPDestinations(MTPStatusValidityExpired)
	}
	s.mu.Lock()
	if s.closed || s.auditing == nil {
		s.mu.Unlock()
		return
	}
	var due []*destinationAuditJob
	for _, job := range s.audits {
		s.work.visitJob()
		if !now.Before(job.due) {
			due = append(due, job)
		}
	}
	s.mu.Unlock()
	sortDestinationAudits(due)
	type groupKey struct {
		association AssociationID
		epoch       uint64
		scope       string
		info        string
	}
	var groups [][]*destinationAuditJob
	indices := make(map[groupKey]int)
	for _, job := range due {
		scope := job.submissionScope(now)
		key := groupKey{job.key.association, job.key.epoch, peerCongestionScopeKey(scope), job.info}
		index, held := indices[key]
		limit := min(s.limits.MaxAffectedPointCodes, max(1, (DefaultReadBufferSize-32-4*len(scope.RoutingContexts)-len(job.info)-4)/4))
		if !held || len(groups[index]) >= limit {
			index = len(groups)
			indices[key] = index
			groups = append(groups, nil)
		}
		groups[index] = append(groups[index], job)
	}
	for _, group := range groups {
		select {
		case <-e.ctx.Done():
			return
		default:
		}
		s.mu.Lock()
		jobs := group[:0]
		for _, job := range group {
			if job.live.Load() {
				jobs = append(jobs, job)
			}
		}
		if s.closed || len(jobs) == 0 {
			s.mu.Unlock()
			continue
		}
		if now.Before(s.auditNext) {
			for _, job := range jobs {
				s.auditOutcomeLocked(job, "rate-deferred", &outcomes)
			}
			s.mu.Unlock()
			continue
		}
		s.mu.Unlock()
		first := jobs[0]
		e.mu.Lock()
		c := e.associationsByID[first.key.association]
		e.mu.Unlock()
		if c == nil {
			s.mu.Lock()
			for _, job := range jobs {
				if s.audits[job.key] == job {
					s.removeAuditLocked(job.key, "reporter-retired", &outcomes)
				}
			}
			s.mu.Unlock()
			continue
		}
		s.mu.Lock()
		charge := publicationCharge(first.submissionScope(now), len(jobs))
		allowed := !pass.used[c.ID()] && pass.remaining > 0 && charge <= pass.bytes && !now.Before(c.optionalPublicationNext) && s.publicationFitsLocked(c.ID(), charge)
		if !allowed {
			s.publicationCounters.Deferred++
			for _, job := range jobs {
				s.auditOutcomeLocked(job, "send-deferred", &outcomes)
			}
			s.mu.Unlock()
			continue
		}
		auditState := CongestionPublicationState{Association: c.ID(), Epoch: first.key.epoch, Kind: CongestionPublicationDAUD, Scope: first.submissionScope(now).clone(), DesiredRevision: first.generation, Pending: true}
		for _, job := range jobs {
			auditState.Destinations = append(auditState.Destinations, job.key.destination.pointCodeRange())
		}
		publicationKey := congestionPublicationKey{association: c.ID(), epoch: first.key.epoch, kind: CongestionPublicationDAUD, scope: peerCongestionScopeKey(auditState.Scope), destination: first.key.destination.pointCode, mask: first.key.destination.mask}
		transient := &congestionPublication{key: publicationKey, state: auditState, charge: charge}
		s.publications[publicationKey] = transient
		s.publicationBytes += charge
		s.publicationCounters.Queued++
		pass.used[c.ID()] = true
		pass.remaining--
		pass.bytes -= charge
		s.mu.Unlock()
		submitted, err := c.tryOptionalSSNMBatch(jobs, now)
		issuedAt := s.automationNow()
		if issuedAt.Before(now) {
			issuedAt = now
		}
		s.mu.Lock()
		s.removePublicationLocked(transient)
		if submitted || sendBufferFull(err) {
			auditState.Attempts = 1
			auditState.LastAttempt = issuedAt
			auditState.LastError = boundedPublicationError(err)
			if err != nil {
				c.optionalPublicationNext = issuedAt.Add(s.publicationConfig.RetryInterval)
			} else {
				c.optionalPublicationNext = issuedAt.Add(s.publicationConfig.MinReportInterval)
			}
			if err == nil {
				auditState.SubmittedRevision = auditState.DesiredRevision
				s.publicationCounters.Submitted++
			} else {
				s.publicationCounters.Failed++
				if sendBufferFull(err) {
					s.publicationCounters.Deferred++
				}
			}
		}
		if submitted || sendBufferFull(err) {
			step := auditRateInterval(s.auditing.MaxMessagesPerSecond)
			s.auditNext = issuedAt.Add(step)
		}
		if submitted && err == nil && !s.closed && e.ctx.Err() == nil && c.Epoch() == first.key.epoch {
			_ = s.publishEventOnlyLocked(SSNMReport{Kind: SSNMDestinationStateAuditReport, Source: SSNMLocalReport, Association: c.ID(), Scope: auditState.Scope.clone(), Destinations: slices.Clone(auditState.Destinations)})
		}

		for _, job := range jobs {
			if s.closed {
				continue
			}
			current := s.audits[job.key]
			if associationDone(c) {
				// Done fences every later submission before transport release and
				// store retirement. Preserve an admitted attempt's actual result,
				// then remove even a newer deadline without a second outcome.
				outcome := "reporter-retired"
				if submitted || sendBufferFull(err) {
					reason := "submitted"
					if err != nil {
						reason = "send-failed"
					}
					if sendBufferFull(err) {
						reason = "send-deferred"
					}
					outcomes.add(job, reason)
					outcome = ""
				}
				if current != nil {
					s.removeAuditLocked(job.key, outcome, &outcomes)
				}
				continue
			}
			permanent := errors.Is(err, ErrAssociationClosed) || errors.Is(err, ErrEndpointClosed) || errors.Is(err, ErrRoutingContextNotActive) || errors.Is(err, ErrInvalidState) && (submitted || c.Epoch() != job.key.epoch)
			if permanent {
				if current == job {
					s.removeAuditLocked(job.key, "reporter-retired", &outcomes)
				}
				continue
			}

			if submitted || sendBufferFull(err) {
				reason := "submitted"
				if err != nil {
					reason = "send-failed"
				}
				if sendBufferFull(err) {
					reason = "send-deferred"
				}
				// Every actual completion counts, including consecutive equal
				// outcomes. Deferral-transition suppression is separate.
				outcomes.add(job, reason)
				if current == nil {
					continue
				}
				next := current.clone()
				next.outcome = reason
				if current == job {
					for i := range next.origins {
						if job.originDue(i, now) {
							if submitted && err == nil {
								next.origins[i].due = issuedAt.Add(s.auditing.Period)
							} else {
								next.origins[i].due = issuedAt.Add(s.publicationConfig.RetryInterval)
							}
						}
					}
				}
				// A newer received status owns its own deadline and causes.
				s.replaceAuditLocked(current, next)
				continue
			}
			if current != job {
				continue
			}
			next := job.clone()
			switch {
			case err != nil:
				s.auditOutcomeLocked(next, "send-deferred", &outcomes)
			default:
				s.auditOutcomeLocked(next, "contended", &outcomes)
			}
			s.replaceAuditLocked(job, next)
		}
		// The admitted attempt can outlive a covering clear or retirement.
		// Describe retry work after reconciling its surviving deadline jobs;
		// the actual outcome above still counts when no cause survives.
		auditState.Pending = false
		if err != nil && !permanentPublicationError(err) && !s.closed && e.ctx.Err() == nil && !associationDone(c) {
			for _, job := range jobs {
				if current := s.audits[job.key]; current != nil && current.live.Load() {
					auditState.Pending = true
					break
				}
			}
		}
		s.mu.Unlock()
		if submitted || sendBufferFull(err) {
			c.notifyManagement(&ManagementIndication{Kind: ManagementCongestionPublication, CongestionPublication: &auditState, Cause: err, Description: "optional destination audit attempt"})
		}
	}
}

// tryOptionalSSNM has no wait for traffic or writer locks and never calls the
// synchronous public DAUD operation. Lock order: aspTransferMu -> sendMu ->
// muState (try only, released) -> sconSubmissions.mu -> muDynamicASKeys ->
// muAckedRCs (all try only). No store, state, binding or tracker lock crosses
// transport. The existing submission tracker cancels retirement/replacement;
// aspTransferMu orders withdrawal. NoWait admits exactly one transport attempt
// independent of the application's write deadline (RFC 4666 §4.5.3).
// RKM correlation/lifecycle callers may join its cancellation completion while
// replacing a binding. This path and its completion never acquire those locks.
func (c *Association) tryOptionalSSNM(job *destinationAuditJob, at ...time.Time) (bool, error) {
	return c.tryOptionalSSNMBatch([]*destinationAuditJob{job}, at...)
}

func (c *Association) tryOptionalSSNMBatch(jobs []*destinationAuditJob, at ...time.Time) (bool, error) {
	if len(jobs) == 0 {
		return false, nil
	}
	job := jobs[0]
	now := job.due
	if len(at) > 0 {
		now = at[0]
	}
	scope := job.submissionScope(now)
	scopeKey := peerCongestionScopeKey(scope)
	currentJobs := func() bool {
		for _, candidate := range jobs {
			if !candidate.live.Load() || candidate.key.association != job.key.association || candidate.key.epoch != job.key.epoch || candidate.info != job.info || peerCongestionScopeKey(candidate.submissionScope(now)) != scopeKey {
				return false
			}
			due := false
			for i := range candidate.bindings {
				due = due || candidate.originDue(i, now)
			}
			if !due {
				return false
			}
		}
		return true
	}
	bindings := make([]queueCongestionBinding, 0, len(job.bindings))
	seen := make(map[queueCongestionBinding]bool)
	for _, candidate := range jobs {
		for i, binding := range candidate.bindings {
			if !candidate.originDue(i, now) {
				continue
			}
			if !seen[binding] {
				seen[binding] = true
				bindings = append(bindings, binding)
			}
		}
	}
	if len(bindings) == 0 {
		return false, nil
	}
	destinations := make([]PointCodeRange, 0, len(jobs))
	for _, candidate := range jobs {
		destinations = append(destinations, candidate.key.destination.pointCodeRange())
	}

	if c.role != RoleASP {
		return false, ErrInvalidState
	}
	parameters, err := buildSSNMParameters(scope, destinations, job.info)
	if err != nil {
		return false, err
	}
	msg := messages.NewDestinationStateAudit(parameters.networkAppearance, parameters.routingContext, parameters.affectedPointCode, parameters.info)
	return c.tryOptionalSSNMFrame(msg, bindings, job.key.epoch, currentJobs)
}

func sortDestinationAudits(jobs []*destinationAuditJob) {
	slices.SortFunc(jobs, func(a, b *destinationAuditJob) int {
		if order := a.due.Compare(b.due); order != 0 {
			return order
		}
		return cmp.Compare(a.generation, b.generation)
	})
}

func (c *Association) optionalAuditASActive(key ASKey) bool {
	if key.RoutingContextSet {
		return c.routingContextAcked(key.RoutingContext) && !c.routingContextOverridden(key.RoutingContext)
	}
	return c.contextlessASAcked()
}

// Round up: truncating a three-per-second interval permits a fourth attempt
// just before the one-second boundary. Rates above clock resolution still
// use a positive interval rather than an unbounded immediate loop.
func auditRateInterval(rate int) time.Duration {
	if rate < 1 {
		return time.Second
	}
	value := time.Duration(rate)
	interval := time.Second / value
	if time.Second%value != 0 {
		interval++
	}
	return max(time.Nanosecond, interval)
}

// Ack bookkeeping precedes the ASP-ACTIVE commit. Keep previously admitted
// early SSNM work through that bookkeeping window without authorizing a send.
func (c *Association) optionalAuditPositiveAck(key ASKey) bool {
	c.muAckedRCs.RLock()
	defer c.muAckedRCs.RUnlock()
	if !c.ackedRCsScoped {
		return false
	}
	if !key.RoutingContextSet {
		return c.ackedContextlessAS
	}
	_, held := c.ackedRCs[key.RoutingContext]
	_, overridden := c.overriddenRCs[key.RoutingContext]
	return held && !overridden
}

// Scope marks retire only matching origins in the bounded deadline table. This
// does not inspect destination/source history or acquire Endpoint.mu. A later
// reconciliation can see reactivation, so it cannot substitute for this fence.
func (s *ssnmState) retireDestinationAuditScopesLocked(association AssociationID, matches func(ASKey) bool) {
	var outcomes auditOutcomeBatch
	for key, job := range s.audits {
		if key.association != association {
			continue
		}
		var next *destinationAuditJob
		for i, binding := range job.bindings {
			if !job.origins[i].needed() || matches != nil && !matches(binding.key) {
				continue
			}
			if next == nil {
				next = job.clone()
			}
			next.origins[i] = destinationAuditOrigin{partition: job.origins[i].partition}
		}
		if next != nil {
			s.replaceAuditLocked(job, next, &outcomes)
		}
	}
	for key := range s.auditActivated[association] {
		if matches == nil || matches(key) {
			delete(s.auditActivated[association], key)
		}
	}
	if len(s.auditActivated[association]) == 0 {
		delete(s.auditActivated, association)
	}
	outcomes.publishLocked(s)
}
