package m3ua

import (
	"cmp"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// LocalCongestionRequest identifies one application-owned overload source.
// Source is 1..64 UTF-8 bytes of letters, digits, _, -, ., :, or /. The '$'
// namespace is reserved for queue evidence. AS and Destination name an exact
// current local scope; Destination has no range mask. Contributions have no TTL.
type LocalCongestionRequest struct {
	Source      string
	Association AssociationID
	Epoch       uint64
	AS          ASKey
	Destination uint32
	Congestion  CongestionState
}

// LocalCongestionContribution is retained local evidence, independently of
// wire publication success. Revision is assigned by the library. Queue sources
// use $queue:<OPC> and preserve the received wire scope. Scope is caller-owned.
// Neither this evidence nor its source identifier authorizes SS7 TFC.
type LocalCongestionContribution struct {
	Request  LocalCongestionRequest
	Scope    WireScope
	Revision uint64
}

type localCongestionBucketKey struct {
	association AssociationID
	as          ASKey
}
type localCongestionRecordKey struct {
	destination   uint32
	source, scope string
}
type localCongestionRecord struct {
	LocalCongestionContribution
	generation, knowledge uint64
	lifetime              *localQueueLifetime
	episode               *localQueueEpisode
	concerned             uint32
	queue                 bool
	scopeKey              string
}
type localCongestionCounts struct {
	unknown int
	levels  [4]int
}
type localCongestionGroup struct {
	records            int
	application, queue localCongestionCounts
	episode            *localQueueEpisode
	national           bool
	desired            CongestionState
	revision           uint64
}
type localCongestionBucket struct {
	association AssociationID
	partition   SSNMPartition
	binding     queueCongestionBinding
	knowledge   uint64
	lifetime    *localQueueLifetime
	records     map[localCongestionRecordKey]localCongestionRecord
	queues      map[localCongestionRecordKey]struct{}
	groups      map[uint32]*localCongestionGroup
	bytes       int
}
type localCongestionUpdate struct {
	scope                  WireScope
	binding                queueCongestionBinding
	destination, concerned uint32
	concernedSet           bool
	desired                CongestionState
	revision               uint64
}

func validLocalCongestionSource(source string) bool {
	if len(source) == 0 || len(source) > 64 || !utf8.ValidString(source) {
		return false
	}
	for _, r := range source {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && !strings.ContainsRune("_-.:/", r) {
			return false
		}
	}
	return true
}

// RFC 4666 §3.4.4 distinguishes national numeric levels from omitted-level
// methods. Agreement between Congested and an explicit level is this local
// declaration API's validation rule.
func validateLocalCongestionState(profile CongestionProfile, state CongestionState) error {
	if profile.Method == CongestionInternational {
		if !state.LevelSet && state.Level == 0 {
			return nil
		}
	} else if state.LevelSet {
		if state.Level <= profile.MaxLevel && state.Congested == (state.Level != 0) {
			return nil
		}
	} else if state.Congested && state.Level == 0 {
		return nil
	}
	return fmt.Errorf("%w: contradictory local congestion state", ErrInvalidParameterValue)
}
func localCongestionScope(key ASKey) WireScope {
	scope := WireScope{NetworkAppearance: key.NetworkAppearance, NetworkAppearanceSet: key.NetworkAppearanceSet, RoutingContextSet: key.RoutingContextSet}
	if key.RoutingContextSet {
		scope.RoutingContexts = []uint32{key.RoutingContext}
	}
	return scope
}
func localCongestionScopeKey(scope WireScope) string {
	if !scope.NetworkAppearanceSet {
		scope.NetworkAppearance = 0
	}
	if !scope.RoutingContextSet {
		scope.RoutingContexts = nil
	}
	return peerCongestionScopeKey(scope)
}
func localCongestionRecordBytes(record localCongestionRecord) int {
	// Portable record, key, indexes and fixed group-counter allowance. This
	// includes both the original RC list and normalized key, not Go heap size.
	return 320 + len(record.Request.Source) + 8*len(record.Scope.RoutingContexts)
}
func (counts *localCongestionCounts) change(state CongestionState, delta int) {
	if !state.Congested {
		return
	}
	if !state.LevelSet {
		counts.unknown += delta
	} else {
		counts.levels[state.Level] += delta
	}
}
func (g *localCongestionGroup) change(record localCongestionRecord, delta int) {
	g.records += delta
	if !record.queue {
		g.application.change(record.Request.Congestion, delta)
		return
	}
	if delta > 0 && record.episode != g.episode {
		g.queue = localCongestionCounts{}
		g.episode = record.episode
	}
	if record.episode == g.episode {
		g.queue.change(record.Request.Congestion, delta)
	}
}
func (g *localCongestionGroup) aggregate() CongestionState {
	queue := localCongestionCounts{}
	if g.episode != nil && g.episode.live.Load() {
		queue = g.queue
	}
	if g.application.unknown+queue.unknown > 0 {
		return CongestionState{Congested: true}
	}
	for level := 3; level > 0; level-- {
		if g.application.levels[level]+queue.levels[level] > 0 {
			return CongestionState{Congested: true, Level: uint8(level), LevelSet: true}
		}
	}
	return CongestionState{LevelSet: g.national}
}
func (e *Endpoint) localCongestionAssociation(id AssociationID) *Association {
	e.mu.Lock()
	c := e.associationsByID[id]
	e.mu.Unlock()
	return c
}
func (c *Association) localCongestionBinding(key ASKey) (queueCongestionBinding, error) {
	key = normalizeOutboundASKey(key)
	if !slices.Contains(c.configuredLocalASKeysForStatus(), key) {
		return queueCongestionBinding{}, ErrNoConfiguredAS
	}
	binding := c.queueCongestionBinding(key, WireScope{})
	if binding.key != key {
		return queueCongestionBinding{}, ErrNoConfiguredAS
	}
	return binding, nil
}

// ReportLocalCongestion retains one declarative application contribution.
// A national explicit level must agree with Congested; an omitted level means
// unknown congestion. International input has no numeric level, including its
// local clear. No host detector, implicit TTL or synchronous wire send is added.
// RFC 4666 §1.4.6 permits ASP/IPSP local SCON; SGP input remains local.
func (e *Endpoint) ReportLocalCongestion(request LocalCongestionRequest) error {
	if e == nil || !e.beginOperation() {
		return ErrEndpointClosed
	}
	defer e.endOperation()
	if err := validateIdentityLength("LocalCongestionRequest.Source", request.Source, 64); err != nil {
		return err
	}
	if !validLocalCongestionSource(request.Source) || request.Destination > 0xffffff {
		return fmt.Errorf("%w: local congestion identifier or destination", ErrInvalidParameterValue)
	}
	c := e.localCongestionAssociation(request.Association)
	if c == nil {
		return ErrAssociationClosed
	}
	if request.Epoch != c.Epoch() {
		return ErrCongestionDecisionStale
	}
	binding, err := c.localCongestionBinding(request.AS)
	if err != nil {
		return err
	}
	request.AS = binding.key
	if err = validateLocalCongestionState(c.congestionProfile(binding.key), request.Congestion); err != nil {
		return err
	}
	request.Source = strings.Clone(request.Source)
	record := localCongestionRecord{LocalCongestionContribution: LocalCongestionContribution{Request: request, Scope: localCongestionScope(binding.key)}}
	update, err := c.commitLocalCongestion(binding, record, false, true)
	if err != nil {
		return err
	}
	c.publishLocalCongestionUpdate(update)
	e.ensureCongestionAutomation()
	return nil
}

// WithdrawLocalCongestion removes only the named application source. Queue
// evidence and other sources remain independent. A current active binding and
// epoch are required; an absent source in that binding is an idempotent success.
func (e *Endpoint) WithdrawLocalCongestion(association AssociationID, epoch uint64, as ASKey, destination uint32, source string) error {
	if e == nil || !e.beginOperation() {
		return ErrEndpointClosed
	}
	defer e.endOperation()
	if err := validateIdentityLength("WithdrawLocalCongestion.Source", source, 64); err != nil {
		return err
	}
	if !validLocalCongestionSource(source) || destination > 0xffffff {
		return fmt.Errorf("%w: local congestion identifier or destination", ErrInvalidParameterValue)
	}
	c := e.localCongestionAssociation(association)
	if c == nil {
		return ErrAssociationClosed
	}
	if epoch != c.Epoch() {
		return ErrCongestionDecisionStale
	}
	binding, err := c.localCongestionBinding(as)
	if err != nil {
		return err
	}
	record := localCongestionRecord{LocalCongestionContribution: LocalCongestionContribution{Request: LocalCongestionRequest{Source: source, Association: association, Epoch: epoch, AS: binding.key, Destination: destination}, Scope: localCongestionScope(binding.key)}}
	update, err := c.commitLocalCongestion(binding, record, true, true)
	if err != nil {
		return err
	}
	c.publishLocalCongestionUpdate(update)
	e.ensureCongestionAutomation()
	return nil
}

// Resolve registry identity first, then use only try operations for state,
// exact SGP/Single Exchange AS, tracker, dynamic and Ack ledgers. State entry can wait on a
// mandatory writer, so queue processing must never wait for muState. The final
// store mutation is bounded bookkeeping, with no transport or callback under
// these locks. Order: state -> AS -> tracker -> dynamic -> Ack -> store.
func (c *Association) commitLocalCongestion(binding queueCongestionBinding, record localCongestionRecord, remove, reportLoss bool) (localCongestionUpdate, error) {
	record.scopeKey = localCongestionScopeKey(record.Scope)
	if !record.queue {
		c.reconcileQueueCongestionLifetimes()
	}
	var server *applicationServer
	if c.role == RoleSGP || c.role == RoleIPSP && !c.isIPSPDoubleExchange() {
		var acquired bool
		server, _, acquired = c.as.tryLookupOptionalAS(binding.key)
		if !acquired {
			return localCongestionUpdate{}, ErrCongestionDecisionStale
		}
		if server == nil {
			return localCongestionUpdate{}, ErrNoConfiguredAS
		}
	}
	if !c.muState.TryRLock() {
		return localCongestionUpdate{}, ErrCongestionDecisionStale
	}
	defer c.muState.RUnlock()
	if server != nil {
		if !server.mu.TryLock() {
			return localCongestionUpdate{}, ErrCongestionDecisionStale
		}
		defer server.mu.Unlock()
		if server.closed || server.asps[c] != StateASPActive {
			return localCongestionUpdate{}, ErrRoutingContextNotActive
		}
	}
	tracker := &c.sconSubmissions
	if !tracker.mu.TryLock() {
		return localCongestionUpdate{}, ErrCongestionDecisionStale
	}
	defer tracker.mu.Unlock()
	if tracker.optionalChanging != nil || tracker.changing[sconBindingID{binding.key.RoutingContext, binding.local}] != nil {
		return localCongestionUpdate{}, ErrCongestionDecisionStale
	}
	partition := c.ssnmPartitionFor(binding.key)
	if !c.muDynamicASKeys.TryRLock() {
		return localCongestionUpdate{}, ErrCongestionDecisionStale
	}
	defer c.muDynamicASKeys.RUnlock()
	if !c.queueCongestionBindingCurrent(binding) {
		return localCongestionUpdate{}, ErrCongestionDecisionStale
	}
	if !c.muAckedRCs.TryRLock() {
		return localCongestionUpdate{}, ErrCongestionDecisionStale
	}
	defer c.muAckedRCs.RUnlock()
	if !c.localCongestionScopeActiveLocked(binding) {
		return localCongestionUpdate{}, ErrRoutingContextNotActive
	}
	select {
	case <-c.done:
		return localCongestionUpdate{}, ErrAssociationClosed
	default:
	}
	knowledge := tracker.knowledgeGeneration.Load()
	if record.queue && (record.lifetime == nil || !record.lifetime.live.Load() || record.lifetime.binding != binding || record.knowledge != knowledge || record.episode == nil || !record.episode.live.Load()) {
		return localCongestionUpdate{}, ErrCongestionDecisionStale
	}
	if !record.queue {
		record.lifetime = c.captureQueueCongestionLifetime(binding)
		if record.lifetime == nil {
			return localCongestionUpdate{}, ErrCongestionDecisionStale
		}
	}
	record.knowledge = knowledge
	s := c.ssnmStore()
	if s == nil {
		return localCongestionUpdate{}, ErrEndpointClosed
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return localCongestionUpdate{}, ErrEndpointClosed
	}
	epoch, live := s.liveSources[c.ID()]
	if !live || epoch != record.Request.Epoch {
		return localCongestionUpdate{}, ErrAssociationClosed
	}
	return s.commitLocalCongestionLocked(partition, binding, record, remove, reportLoss, c.congestionProfile(binding.key).Method != CongestionInternational)
}

func (s *ssnmState) localRefusalLocked(reason string, report bool) error {
	if report {
		return s.refuseReportLocked(1, reason)
	}
	return fmt.Errorf("%w: %s", ErrSSNMStateLimit, reason)
}
func (s *ssnmState) commitLocalCongestionLocked(partition SSNMPartition, binding queueCongestionBinding, record localCongestionRecord, remove, reportLoss, national bool) (localCongestionUpdate, error) {
	key := localCongestionRecordKey{record.Request.Destination, record.Request.Source, record.scopeKey}
	byAS := s.localByAssociation[record.Request.Association]
	bucket := byAS[binding.key]
	if bucket != nil && (bucket.binding != binding || bucket.knowledge != record.knowledge || bucket.lifetime != record.lifetime || bucket.lifetime == nil || !bucket.lifetime.live.Load()) {
		s.dropLocalCongestionBucketLocked(bucket)
		bucket = nil
	}
	if remove {
		if bucket == nil {
			return localCongestionUpdate{}, nil
		}
		return s.removeLocalCongestionRecordLocked(bucket, key), nil
	}
	var previous localCongestionRecord
	var held bool
	if bucket != nil {
		previous, held = bucket.records[key]
	}
	if held && previous.Request == record.Request && previous.generation == record.generation && previous.lifetime == record.lifetime {
		group := bucket.groups[record.Request.Destination]
		return localCongestionUpdate{scope: previous.Scope, binding: binding, destination: record.Request.Destination, concerned: record.concerned, concernedSet: record.queue, desired: group.desired, revision: group.revision}, nil
	}
	state := s.partitions[partition]
	newRecords := 0
	cost := localCongestionRecordBytes(record)
	extra := cost
	if held {
		extra -= localCongestionRecordBytes(previous)
	} else {
		newRecords = 1
	}
	records := 0
	partitionBytes := 0
	if state != nil {
		records = state.records()
	} else {
		partitionBytes = ssnmPartitionBaseBytes
	}
	reason := ""
	switch {
	case state == nil && len(s.partitions) >= s.limits.MaxPartitions:
		reason = "local congestion partition limit reached"
	case newRecords > s.limits.MaxRecordsPerPartition-records:
		reason = "local congestion partition record limit reached"
	case newRecords > s.limits.MaxRecordsPerPeer-s.peerRecords[partition.peerKey()]:
		reason = "local congestion peer record limit reached"
	case newRecords > s.limits.MaxRecords-s.records:
		reason = "local congestion store record limit reached"
	case extra+partitionBytes > s.limits.MaxBytes-s.bytes:
		reason = "local congestion store byte limit reached"
	}
	if reason != "" {
		return localCongestionUpdate{}, s.localRefusalLocked(reason, reportLoss)
	}
	pending := false
	if state != nil {
		if held, bound := state.bindings[record.Request.Association]; bound {
			pending = held
		}
	}
	// Resource ownership must not promote received-knowledge admission.
	if err := s.bindLocked(partition, record.Request.Association, pending); err != nil {
		return localCongestionUpdate{}, err
	}
	state = s.partitions[partition]
	if bucket == nil {
		bucket = &localCongestionBucket{association: record.Request.Association, partition: partition, binding: binding, knowledge: record.knowledge, lifetime: record.lifetime, records: make(map[localCongestionRecordKey]localCongestionRecord), queues: make(map[localCongestionRecordKey]struct{}), groups: make(map[uint32]*localCongestionGroup)}
		if s.localByAssociation == nil {
			s.localByAssociation = make(map[AssociationID]map[ASKey]*localCongestionBucket)
		}
		if s.localByAssociation[bucket.association] == nil {
			s.localByAssociation[bucket.association] = make(map[ASKey]*localCongestionBucket)
		}
		s.localByAssociation[bucket.association][binding.key] = bucket
		if state.localCongestion == nil {
			state.localCongestion = make(map[localCongestionBucketKey]*localCongestionBucket)
		}
		state.localCongestion[localCongestionBucketKey{bucket.association, binding.key}] = bucket
	}
	group := bucket.groups[record.Request.Destination]
	if group == nil {
		group = &localCongestionGroup{national: national}
		bucket.groups[record.Request.Destination] = group
	}
	if held {
		group.change(previous, -1)
	}
	group.change(record, 1)
	group.desired = group.aggregate()
	group.revision = s.nextRevisionLocked()
	record.Revision = group.revision
	record.Scope = record.Scope.clone()
	bucket.records[key] = record
	if record.queue {
		bucket.queues[key] = struct{}{}
	}
	bucket.bytes += extra
	state.bytes += extra
	s.bytes += extra
	state.localRecords += newRecords
	s.records += newRecords
	s.peerRecords[partition.peerKey()] += newRecords
	return localCongestionUpdate{scope: record.Scope, binding: binding, destination: record.Request.Destination, concerned: record.concerned, concernedSet: record.queue, desired: group.desired, revision: group.revision}, nil
}

func (s *ssnmState) removeLocalCongestionRecordLocked(bucket *localCongestionBucket, key localCongestionRecordKey) localCongestionUpdate {
	record, held := bucket.records[key]
	if !held {
		return localCongestionUpdate{}
	}
	delete(bucket.records, key)
	delete(bucket.queues, key)
	group := bucket.groups[record.Request.Destination]
	group.change(record, -1)
	group.desired = group.aggregate()
	group.revision = s.nextRevisionLocked()
	update := localCongestionUpdate{scope: record.Scope, binding: bucket.binding, destination: record.Request.Destination, concerned: record.concerned, concernedSet: record.queue, desired: group.desired, revision: group.revision}
	if group.records == 0 {
		delete(bucket.groups, record.Request.Destination)
	}
	cost := localCongestionRecordBytes(record)
	bucket.bytes -= cost
	state := s.partitions[bucket.partition]
	state.bytes -= cost
	state.localRecords--
	s.bytes -= cost
	s.records--
	s.peerRecords[bucket.partition.peerKey()]--
	if s.peerRecords[bucket.partition.peerKey()] == 0 {
		delete(s.peerRecords, bucket.partition.peerKey())
	}
	if len(bucket.records) == 0 {
		s.deleteLocalCongestionBucketIndexLocked(bucket)
	}
	return update
}
func (s *ssnmState) deleteLocalCongestionBucketIndexLocked(bucket *localCongestionBucket) {
	delete(s.localByAssociation[bucket.association], bucket.binding.key)
	if len(s.localByAssociation[bucket.association]) == 0 {
		delete(s.localByAssociation, bucket.association)
	}
	if state := s.partitions[bucket.partition]; state != nil {
		delete(state.localCongestion, localCongestionBucketKey{bucket.association, bucket.binding.key})
	}
}
func (s *ssnmState) dropLocalCongestionBucketLocked(bucket *localCongestionBucket) {
	count := len(bucket.records)
	if state := s.partitions[bucket.partition]; state != nil {
		state.localRecords -= count
		state.bytes -= bucket.bytes
	}
	s.records -= count
	s.bytes -= bucket.bytes
	s.peerRecords[bucket.partition.peerKey()] -= count
	if s.peerRecords[bucket.partition.peerKey()] <= 0 {
		delete(s.peerRecords, bucket.partition.peerKey())
	}
	s.deleteLocalCongestionBucketIndexLocked(bucket)
	s.nextRevisionLocked()
}
func (s *ssnmState) retireLocalCongestionLocked(association AssociationID, matches func(ASKey) bool) {
	for key, bucket := range s.localByAssociation[association] {
		if matches == nil || matches(key) {
			s.dropLocalCongestionBucketLocked(bucket)
		}
	}
}
func (c *Association) publishLocalCongestionUpdate(update localCongestionUpdate) {
	if update.revision == 0 || c.endpoint == nil {
		return
	}
	c.endpoint.updateLocalCongestionPublication(c, update.scope, update.binding, update.destination, update.concerned, update.concernedSet && c.role == RoleASP, update.desired, update.revision)
}

func (c *Association) reportQueueLocalCongestion(ind dataOverflowScope) {
	c.reportQueueLocalCongestionAttempt(ind, true)
}
func (c *Association) reportQueueLocalCongestionAttempt(ind dataOverflowScope, reportLoss bool) {
	if c == nil || c.endpoint == nil {
		return
	}
	profile := c.overflowCongestionProfile(ind.Scope)
	state := CongestionState{Congested: true}
	if profile.Method != CongestionInternational {
		state.LevelSet = true
		state.Level = profile.MaxLevel
	}
	record := localCongestionRecord{LocalCongestionContribution: LocalCongestionContribution{Request: LocalCongestionRequest{Source: "$queue:" + strconv.FormatUint(uint64(ind.OriginatingPointCode), 10), Association: c.ID(), Epoch: ind.binding.epoch, AS: ind.binding.key, Destination: ind.DestinationPointCode, Congestion: state}, Scope: ind.Scope}, queue: true, concerned: ind.OriginatingPointCode, generation: ind.generation, knowledge: ind.knowledgeGeneration, lifetime: ind.lifetime, episode: ind.episode}
	update, err := c.commitLocalCongestion(ind.binding, record, false, reportLoss)
	if err != nil {
		if reportLoss && err == ErrCongestionDecisionStale {
			s := c.ssnmStore()
			s.mu.Lock()
			if !s.closed {
				_ = s.refuseReportLocked(1, "local queue congestion retention contended or stale")
			}
			s.mu.Unlock()
		}
		return
	}
	c.publishLocalCongestionUpdate(update)
}
func (c *Association) clearQueueLocalCongestion(ind dataOverflowScope) {
	s := c.ssnmStore()
	if s == nil {
		return
	}
	key := localCongestionRecordKey{ind.DestinationPointCode, "$queue:" + strconv.FormatUint(uint64(ind.OriginatingPointCode), 10), localCongestionScopeKey(ind.Scope)}
	s.mu.Lock()
	var update localCongestionUpdate
	if bucket := s.localByAssociation[c.ID()][ind.binding.key]; bucket != nil && bucket.binding == ind.binding {
		if record, held := bucket.records[key]; held && record.queue && record.generation == ind.generation && record.knowledge == ind.knowledgeGeneration && record.lifetime == ind.lifetime && record.episode == ind.episode {
			update = s.removeLocalCongestionRecordLocked(bucket, key)
		}
	}
	s.mu.Unlock()
	c.publishLocalCongestionUpdate(update)
}

// Worker-only recovery of initially refused queue observations. It snapshots
// at most the existing 32 overflow scopes and never holds their mutex across
// source admission, store iteration or publication.
func (c *Association) reconcileQueueLocalCongestion() {
	if c == nil || c.endpoint == nil || !c.muDataOverflow.TryLock() {
		return
	}
	scopes := make([]dataOverflowScope, 0, len(c.dataOverflowScopes))
	for _, scope := range c.dataOverflowScopes {
		scopes = append(scopes, scope)
	}
	c.muDataOverflow.Unlock()
	type currentKey struct {
		as                    ASKey
		key                   localCongestionRecordKey
		generation, knowledge uint64
		lifetime              *localQueueLifetime
	}
	current := make(map[currentKey]bool, len(scopes))
	for _, ind := range scopes {
		current[currentKey{ind.binding.key, localCongestionRecordKey{ind.DestinationPointCode, "$queue:" + strconv.FormatUint(uint64(ind.OriginatingPointCode), 10), localCongestionScopeKey(ind.Scope)}, ind.generation, ind.knowledgeGeneration, ind.lifetime}] = true
	}
	s := c.ssnmStore()
	if !s.mu.TryLock() {
		return
	}
	var updates []localCongestionUpdate
	for _, bucket := range s.localByAssociation[c.ID()] {
		for key := range bucket.queues {
			record := bucket.records[key]
			scopeLive := record.lifetime != nil && record.lifetime.live.Load() && record.knowledge == c.sconSubmissions.knowledgeGeneration.Load()
			live := scopeLive && record.episode != nil && record.episode.live.Load()
			if !live || !current[currentKey{bucket.binding.key, key, record.generation, record.knowledge, record.lifetime}] {
				update := s.removeLocalCongestionRecordLocked(bucket, key)
				if scopeLive {
					updates = append(updates, update)
				}
			}
		}
	}
	s.mu.Unlock()
	for _, update := range updates {
		c.publishLocalCongestionUpdate(update)
	}
	for _, ind := range scopes {
		if repaired, ok := c.repairQueueLocalLifetime(ind); ok {
			c.reportQueueLocalCongestionAttempt(repaired, false)
		}
	}
}

func (c *Association) reconcileLocalCongestion() {
	s := c.ssnmStore()
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, bucket := range s.localByAssociation[c.ID()] {
		if bucket.lifetime == nil || !bucket.lifetime.live.Load() || bucket.binding.epoch != c.Epoch() || bucket.knowledge != c.sconSubmissions.knowledgeGeneration.Load() {
			s.dropLocalCongestionBucketLocked(bucket)
		}
	}
}

// Final optional admission distinguishes store contention from an outdated
// revision. An absent group admits only its already reserved clear obligation.
func (s *ssnmState) tryLocalCongestionCurrent(association AssociationID, epoch uint64, as ASKey, destination uint32, revision uint64, desired CongestionState, scope string, origin uint32, needsTrigger bool) (bool, bool) {
	if s == nil || !s.mu.TryLock() {
		return false, false
	}
	defer s.mu.Unlock()
	valid := s.localCongestionCurrentLocked(association, epoch, as, destination, revision, desired)
	if valid && needsTrigger {
		valid = s.localQueueTriggerCurrentLocked(association, epoch, as, scope, destination, origin)
	}
	return valid, true
}

func (s *ssnmState) localCongestionCurrentLocked(association AssociationID, epoch uint64, as ASKey, destination uint32, revision uint64, desired CongestionState) bool {
	if current, live := s.liveSources[association]; s.closed || !live || current != epoch {
		return false
	}
	bucket := s.localByAssociation[association][as]
	if bucket != nil && (bucket.lifetime == nil || !bucket.lifetime.live.Load()) {
		return false
	}
	if bucket == nil || bucket.groups[destination] == nil {
		return !desired.Congested
	}
	group := bucket.groups[destination]
	return group.revision == revision && group.aggregate() == group.desired
}

func (s *ssnmState) localCongestionSnapshot(as ASKey) []LocalCongestionContribution {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var result []LocalCongestionContribution
	for _, byAS := range s.localByAssociation {
		if bucket := byAS[as]; bucket != nil && bucket.lifetime != nil && bucket.lifetime.live.Load() {
			for _, record := range bucket.records {
				if record.queue && (record.episode == nil || !record.episode.live.Load()) {
					continue
				}
				value := record.LocalCongestionContribution
				value.Scope = value.Scope.clone()
				result = append(result, value)
			}
		}
	}
	slices.SortFunc(result, func(a, b LocalCongestionContribution) int {
		if n := cmp.Compare(a.Request.Association, b.Request.Association); n != 0 {
			return n
		}
		if n := cmp.Compare(a.Request.Destination, b.Request.Destination); n != 0 {
			return n
		}
		if n := cmp.Compare(a.Request.Source, b.Request.Source); n != 0 {
			return n
		}
		return cmp.Compare(localCongestionScopeKey(a.Scope), localCongestionScopeKey(b.Scope))
	})
	return result
}

// The publication table already owns the normalized scope key. This lookup
// never scans contributions or replays an origin whose queue evidence ended.
func (s *ssnmState) localQueueTriggerCurrentLocked(association AssociationID, epoch uint64, as ASKey, scopeKey string, destination, origin uint32) bool {
	bucket := s.localByAssociation[association][as]
	if bucket == nil || bucket.binding.epoch != epoch || bucket.lifetime == nil || !bucket.lifetime.live.Load() {
		return false
	}
	record, held := bucket.records[localCongestionRecordKey{destination, "$queue:" + strconv.FormatUint(uint64(origin), 10), scopeKey}]
	return held && record.queue && record.Request.Epoch == epoch && record.lifetime == bucket.lifetime && record.knowledge == bucket.knowledge && record.episode != nil && record.episode.live.Load()
}

// An unresolved observation can acquire a token later only without crossing
// any lifetime mark and while the exact episode still owns the overflow slot.
func (c *Association) repairQueueLocalLifetime(ind dataOverflowScope) (dataOverflowScope, bool) {
	if ind.episode == nil || !ind.episode.live.Load() {
		return ind, false
	}
	if ind.lifetime != nil {
		return ind, ind.lifetime.live.Load()
	}
	if c.localQueueLifetimeRevision.Load() != ind.lifetimeRevision || c.Epoch() != ind.binding.epoch || c.sconSubmissions.knowledgeGeneration.Load() != ind.knowledgeGeneration {
		return ind, false
	}
	token := c.captureQueueCongestionLifetime(ind.binding)
	if token == nil || !c.muDataOverflow.TryLock() {
		return ind, false
	}
	defer c.muDataOverflow.Unlock()
	key := overflowKey(ind.Scope, ind.DestinationPointCode, ind.OriginatingPointCode)
	current, held := c.dataOverflowScopes[key]
	if !held || !c.dataOverflow.Load() || current.binding != ind.binding || current.generation != ind.generation || current.knowledgeGeneration != ind.knowledgeGeneration || current.lifetimeRevision != ind.lifetimeRevision || current.episode != ind.episode || !current.episode.live.Load() || current.lifetime != nil || c.localQueueLifetimeRevision.Load() != ind.lifetimeRevision || !token.live.Load() {
		return ind, false
	}
	current.lifetime = token
	c.dataOverflowScopes[key] = current
	return current, true
}
