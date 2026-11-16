package m3ua

import (
	"container/list"
	"sync"
)

// Commit bookkeeping preserves SSNM input changes before aggregate projection
// (RFC 4666 §§3.4.4, 4.5.2.2); the restart marks are a local procedure choice.
// Each retained interval owns one mark. The registry contains no Association
// snapshots or destination-source signatures. Its size is the Endpoint's LRU
// budget; matching uses only the immutable routing configuration and the change
// scope, never a projected aggregate or retained source-history scan.
type mtpStatusMark struct {
	route     MTPRouteID
	pointCode uint32
	restart   bool // mtpStatusMarks.mu
}
type mtpStatusMarks struct {
	mu      sync.Mutex
	config  aspRoutingConfig
	entries map[*mtpStatusMark]struct{}
	closed  bool
}

func (m *mtpStatusMarks) register(route MTPRouteID, pc uint32) *mtpStatusMark {
	if m == nil {
		return nil
	}
	mark := &mtpStatusMark{route: route, pointCode: pc}
	m.mu.Lock()
	if !m.closed {
		if m.entries == nil {
			m.entries = make(map[*mtpStatusMark]struct{})
		}
		m.entries[mark] = struct{}{}
	}
	m.mu.Unlock()
	return mark
}
func (m *mtpStatusMarks) forget(mark *mtpStatusMark) {
	if m == nil || mark == nil {
		return
	}
	m.mu.Lock()
	delete(m.entries, mark)
	m.mu.Unlock()
}
func (m *mtpStatusMarks) consume(mark *mtpStatusMark) bool {
	if m == nil || mark == nil {
		return false
	}
	m.mu.Lock()
	restart := mark.restart
	mark.restart = false
	m.mu.Unlock()
	return restart
}
func (m *mtpStatusMarks) close() {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.closed = true
	m.entries = nil
	m.mu.Unlock()
}

// No route, state, AS, registration, store or consumer lock is acquired while
// the mark mutex is held. Store callers already hold store mu; offers hold
// route mu. Both commit and offer consume the mark through this short lock.
func (m *mtpStatusMarks) change(partition SSNMPartition, destination *PointCodeRange, scope *ASKey) {
	finish := m.beginChange(partition, destination, scope)
	finish()
}
func (m *mtpStatusMarks) beginChange(partition SSNMPartition, destination *PointCodeRange, scope *ASKey) func() {
	if m == nil || partition.Kind != SSNMCanonicalPartition {
		return func() {}
	}
	m.mu.Lock()
	for mark := range m.entries {
		if destination != nil && destinationRangePrefix(mark.pointCode, destination.Mask) != destinationRangePrefix(destination.PointCode, destination.Mask) {
			continue
		}
		if m.servesPartition(mark.route, partition, scope) {
			mark.restart = true
		}
	}
	return m.mu.Unlock
}
func (m *mtpStatusMarks) servesPartition(routeID MTPRouteID, partition SSNMPartition, _ *ASKey) bool {
	// Canonical readers use the configured SG/AS aggregate. Wire NA and RC
	// labels belong to each SGP and cannot narrow that view (RFC 4666 §§1.2,
	// 1.4.2.1, 3.3.1). Source-scoped invalidation remains independently scoped.
	return m.partitionScopes(routeID, partition, func(ASKey) bool { return true })
}
func statusRecordScope(record SSNMCongestion) ASKey {
	key := record.AS
	if record.Scope.NetworkAppearanceSet {
		key.NetworkAppearance = record.Scope.NetworkAppearance
		key.NetworkAppearanceSet = true
	}
	return key
}
func sameStatusCongestion(a, b SSNMCongestion) bool {
	return a.Congested == b.Congested && a.Level == b.Level && a.LevelSet == b.LevelSet && a.Freshness == b.Freshness
}

func (c *Association) statusMarks() *mtpStatusMarks {
	if c == nil || c.role != RoleASP || c.endpoint == nil || c.endpoint.aspRoutes == nil {
		return nil
	}
	return c.endpoint.aspRoutes.statusMarks
}
func (m *mtpStatusMarks) associationLocked(c *Association, routes map[MTPRouteID]bool) {
	if c.cfg.PeerSGP == nil {
		return
	}
	sgp, held := m.config.sgpByIdentity[*c.cfg.PeerSGP]
	if !held {
		return
	}
	for mark := range m.entries {
		if sgp.carries(mark.route) && (routes == nil || routes[mark.route]) {
			mark.restart = true
		}
	}
}
func (c *Association) invalidateMTPStatusState() {
	// Callers have committed state and still hold muState.
	c.mtpStatusActive.Store(c.state == StateASPActive)
	if marks := c.statusMarks(); marks != nil {
		marks.mu.Lock()
		marks.associationLocked(c, nil)
		marks.mu.Unlock()
	}
}

// Done is an eligibility commit in its own right, before transport release and
// binding reconciliation. Serialize it with mark consumption: an offer during
// an unprojected clear cannot consume the witness before Done becomes visible.
func (c *Association) closeMTPStatusDone() {
	marks := c.statusMarks()
	if marks == nil {
		close(c.done)
		return
	}
	marks.mu.Lock()
	c.mtpStatusActive.Store(false)
	close(c.done)
	marks.associationLocked(c, nil)
	marks.mu.Unlock()
}

type mtpStatusScope struct {
	route MTPRouteID
	key   ASKey
}

// Called with the registration maps read-locked, before the mark mutex.
// Holding these read locks through the permission commit prevents a newly
// registered scope from slipping between key capture and mark publication.
func (c *Association) statusScopesLocked(marks *mtpStatusMarks) []mtpStatusScope {
	if c.cfg.PeerSGP == nil {
		return nil
	}
	sgp, held := marks.config.sgpByIdentity[*c.cfg.PeerSGP]
	if !held {
		return nil
	}
	var scopes []mtpStatusScope
	for _, route := range sgp.routeOrder {
		for _, candidate := range sgp.candidatesFor(route) {
			as, found := marks.config.remoteASFor(*c.cfg.PeerSGP, candidate.applicationServer)
			if !found {
				continue
			}
			if as.asKeyStatic {
				scopes = append(scopes, mtpStatusScope{route, as.asKey})
				continue
			}
			for rc, id := range c.canonicalRemoteAS {
				if id == candidate.applicationServer {
					if key, held := c.dynamicPeerASKeys[rc]; held {
						scopes = append(scopes, mtpStatusScope{route, key})
					}
				}
			}
		}
	}
	return scopes
}
func (c *Association) statusScopePermittedLocked(key ASKey) bool {
	if !key.RoutingContextSet {
		return !c.ackedRCsScoped || c.ackedContextlessAS
	}
	_, acked := c.ackedRCs[key.RoutingContext]
	_, overridden := c.overriddenRCs[key.RoutingContext]
	return (!c.ackedRCsScoped || acked) && !overridden
}

// Static route scopes need no registration-map locks. Dynamic scopes keep
// the existing ACK -> dynamic -> canonical order through permission commit.
func (c *Association) dynamicMTPStatusScopes(marks *mtpStatusMarks) bool {
	if c.cfg.PeerSGP == nil {
		return false
	}
	sgp := marks.config.sgpByIdentity[*c.cfg.PeerSGP]
	for _, route := range sgp.routeOrder {
		for _, candidate := range sgp.candidatesFor(route) {
			as, held := marks.config.remoteASFor(*c.cfg.PeerSGP, candidate.applicationServer)
			if held && !as.asKeyStatic {
				return true
			}
		}
	}
	return false
}

// The caller holds muAckedRCs through the completion. The mark mutex also
// fences first interval registration, including when the registry is empty.
// Store retirement must finish before this span. Neither its caller nor its
// completion may acquire store, state, AS, traffic or registration locks while
// holding marks; received commits acquire store before marks. The completion
// releases marks and the optional dynamic/canonical read locks together.
func (c *Association) beginMTPStatusScopeChangeLocked() func() {
	marks := c.statusMarks()
	if marks == nil {
		return func() {}
	}
	dynamic := c.dynamicMTPStatusScopes(marks)
	if dynamic {
		c.muDynamicASKeys.RLock()
		c.muCanonicalAS.RLock()
	}
	scopes := c.statusScopesLocked(marks)
	marks.mu.Lock()
	wasActive := c.mtpStatusActive.Load()
	previous := make([]bool, len(scopes))
	for i, scope := range scopes {
		previous[i] = c.statusScopePermittedLocked(scope.key)
	}
	return func() {
		if wasActive || c.mtpStatusActive.Load() {
			changed := make(map[MTPRouteID]bool)
			for i, scope := range scopes {
				if previous[i] != c.statusScopePermittedLocked(scope.key) {
					changed[scope.route] = true
				}
			}
			marks.associationLocked(c, changed)
		}
		marks.mu.Unlock()
		if dynamic {
			c.muCanonicalAS.RUnlock()
			c.muDynamicASKeys.RUnlock()
		}
	}
}

// This index resolves only one received source's newest covering input. It is
// not a destination aggregate. Two intrusive links per retained statement
// handle equivalent wire-scope aliases and canonical wildcard appearances;
// replacement/removal is constant work and lookup takes at most 25 prefixes.
// No interval retains a source, association or signature. The index is not a
// claim that SSNM's portable byte accounting measures Go heap allocations.
type mtpStatusPrefix struct {
	association                  AssociationID
	destination                  ssnmDestinationKey
	appearance                   uint32
	appearanceSet, anyAppearance bool
}
type mtpStatusSourceRecord struct {
	record   SSNMCongestion
	links    [2]*list.Element
	prefixes [2]mtpStatusPrefix
}

// Aliases from one grouped report share a revision but expire separately.
// Track tied freshness in constant work: any expired newest alias requires a
// conservative restart on renewal, regardless of map iteration order in the
// canonical projection. These counts retain no additional statements.
type mtpStatusRevision struct{ records, expired int }
type mtpStatusSourceBucket struct {
	inputs    list.List
	revisions map[uint64]*mtpStatusRevision
}
type mtpStatusSourceIndex struct {
	records  map[receivedCongestionKey]*mtpStatusSourceRecord
	prefixes map[mtpStatusPrefix]*mtpStatusSourceBucket
}

func (p *ssnmPartitionState) replaceStatusSource(key receivedCongestionKey, record SSNMCongestion) {
	if p.statusSources == nil {
		p.statusSources = &mtpStatusSourceIndex{records: make(map[receivedCongestionKey]*mtpStatusSourceRecord), prefixes: make(map[mtpStatusPrefix]*mtpStatusSourceBucket)}
	}
	p.removeStatusSource(key)
	scope := statusRecordScope(record)
	next := &mtpStatusSourceRecord{record: record, prefixes: [2]mtpStatusPrefix{
		{association: key.association, destination: key.destination, anyAppearance: true},
		{association: key.association, destination: key.destination, appearance: scope.NetworkAppearance, appearanceSet: scope.NetworkAppearanceSet},
	}}
	for i, prefix := range next.prefixes {
		bucket := p.statusSources.prefixes[prefix]
		if bucket == nil {
			bucket = &mtpStatusSourceBucket{revisions: make(map[uint64]*mtpStatusRevision)}
			p.statusSources.prefixes[prefix] = bucket
		}
		revision := bucket.revisions[record.Revision]
		if revision == nil {
			revision = &mtpStatusRevision{}
			bucket.revisions[record.Revision] = revision
		}
		revision.records++
		if record.Freshness == CongestionExpired {
			revision.expired++
		}
		next.links[i] = bucket.inputs.PushBack(next)
	}
	p.statusSources.records[key] = next
}
func (p *ssnmPartitionState) removeStatusSource(key receivedCongestionKey) {
	if p.statusSources == nil {
		return
	}
	old := p.statusSources.records[key]
	if old == nil {
		return
	}
	for i, prefix := range old.prefixes {
		bucket := p.statusSources.prefixes[prefix]
		bucket.inputs.Remove(old.links[i])
		revision := bucket.revisions[old.record.Revision]
		revision.records--
		if old.record.Freshness == CongestionExpired {
			revision.expired--
		}
		if revision.records == 0 {
			delete(bucket.revisions, old.record.Revision)
		}
		if bucket.inputs.Len() == 0 {
			delete(p.statusSources.prefixes, prefix)
		}
	}
	delete(p.statusSources.records, key)
}
func (p *ssnmPartitionState) statusSourceInput(association AssociationID, pc uint32, query ASKey) (SSNMCongestion, bool) {
	var current SSNMCongestion
	held := false
	if p.statusSources == nil {
		return current, false
	}
	for mask := uint8(0); mask <= maxPointCodeMask; mask++ {
		prefix := mtpStatusPrefix{association: association, destination: ssnmDestinationKey{pointCode: destinationRangePrefix(pc, mask), mask: mask}, anyAppearance: !query.NetworkAppearanceSet}
		if query.NetworkAppearanceSet {
			prefix.appearance = query.NetworkAppearance
			prefix.appearanceSet = true
		}
		bucket := p.statusSources.prefixes[prefix]
		if bucket == nil {
			continue
		}
		record := bucket.inputs.Back().Value.(*mtpStatusSourceRecord).record
		if bucket.revisions[record.Revision].expired > 0 {
			record.Freshness = CongestionExpired
		}
		if !held || record.Revision > current.Revision {
			current = record
			held = true
		} else if record.Revision == current.Revision && record.Freshness == CongestionExpired {
			current.Freshness = CongestionExpired
		}
	}
	return current, held
}
func (m *mtpStatusMarks) partitionScopes(routeID MTPRouteID, partition SSNMPartition, visit func(ASKey) bool) bool {
	route, held := m.config.mtpRoute(routeID)
	if !held {
		return false
	}
	for _, gateway := range route.gateways {
		if gateway.id != partition.SignallingGateway {
			continue
		}
		for _, candidate := range gateway.candidates {
			if candidate.applicationServer != partition.ApplicationServer {
				continue
			}
			for identity, sgp := range m.config.sgpByIdentity {
				if identity.SignallingGateway != gateway.id || !sgp.carries(routeID) {
					continue
				}
				as, found := m.config.remoteASFor(identity, candidate.applicationServer)
				if !found {
					continue
				}
				key := as.asKey
				if !as.asKeyStatic {
					key = ASKey{NetworkAppearance: as.routingKey.NetworkAppearance, NetworkAppearanceSet: as.routingKey.NetworkAppearanceSet}
				}
				if visit(key) {
					return true
				}
			}
		}
	}
	return false
}
func (s *ssnmState) beginReceivedStatusChangeLocked(state *ssnmPartitionState, key receivedCongestionKey, next SSNMCongestion) func() {
	marks := s.statusMarks
	if marks == nil || state.partition.Kind != SSNMCanonicalPartition {
		return func() {}
	}
	marks.mu.Lock()
	for mark := range marks.entries {
		if destinationRangePrefix(mark.pointCode, key.destination.mask) != key.destination.pointCode {
			continue
		}
		if marks.partitionScopes(mark.route, state.partition, func(ASKey) bool {
			previous, held := state.statusSourceInput(key.association, mark.pointCode, ASKey{})
			return !held || !sameStatusCongestion(previous, next)
		}) {
			mark.restart = true
		}
	}
	return marks.mu.Unlock
}

// Scalar availability and non-validity congestion choose the newest covering
// range. Equal broad reassertions can replace a newer narrow clear, so compare
// that input using fixed prefix probes rather than just the overwritten key.
func (s *ssnmState) beginScalarStatusChangeLocked(state *ssnmPartitionState, key ssnmDestinationKey, report SSNMReport) func() {
	marks := s.statusMarks
	if marks == nil || state.partition.Kind != SSNMCanonicalPartition {
		return func() {}
	}
	marks.mu.Lock()
	for mark := range marks.entries {
		if destinationRangePrefix(mark.pointCode, key.mask) != key.pointCode || !marks.servesPartition(mark.route, state.partition, nil) {
			continue
		}
		var revision uint64
		held, equal := false, false
		if report.retainsAvailability() && len(state.receivedAvailability) > 0 {
			previous, found := state.availabilityInput(mark.pointCode)
			if !found || previous.State != report.availabilityState() {
				mark.restart = true
			}
			continue
		}
		for mask := uint8(0); mask <= maxPointCodeMask; mask++ {
			prefix := ssnmDestinationKey{pointCode: destinationRangePrefix(mark.pointCode, mask), mask: mask}
			if report.retainsAvailability() {
				if old, found := state.availability[prefix]; found && (!held || old.Revision > revision) {
					held = true
					revision = old.Revision
					equal = old.State == report.availabilityState()
				}
			} else {
				if old, found := state.congestion[prefix]; found && (!held || old.Revision > revision) {
					held = true
					revision = old.Revision
					equal = old.Congested == (!report.CongestionLevelSet || report.CongestionLevel != 0) && old.Level == report.CongestionLevel && old.LevelSet == report.CongestionLevelSet && old.Freshness == CongestionFresh
				}
			}
		}
		if !held || !equal {
			mark.restart = true
		}
	}
	return marks.mu.Unlock
}

// Registration-map callers hold their existing dynamic/canonical map locks.
// Only configured AS/route matching occurs under the mark mutex; no map lock
// is acquired from it. The completion fences the actual mapping commit.
func (c *Association) beginMTPStatusBindingChangeLocked(ids ...RemoteASID) func() {
	marks := c.statusMarks()
	if marks == nil || c.cfg.PeerSGP == nil {
		return func() {}
	}
	sgp, held := marks.config.sgpByIdentity[*c.cfg.PeerSGP]
	if !held {
		return func() {}
	}
	marks.mu.Lock()
	if c.mtpStatusActive.Load() {
		routes := make(map[MTPRouteID]bool)
		for _, route := range sgp.routeOrder {
			for _, candidate := range sgp.candidatesFor(route) {
				for _, id := range ids {
					if id != "" && id == candidate.applicationServer {
						routes[route] = true
					}
				}
			}
		}
		marks.associationLocked(c, routes)
	}
	return marks.mu.Unlock
}

// The caller holds muDynamicASKeys. Canonical writers use that same lock
// before muCanonicalAS, so the captured identity cannot change before commit.
func (c *Association) beginMTPStatusDynamicChangeLocked(rc uint32, local bool) func() {
	marks := c.statusMarks()
	if marks == nil || local || c.cfg.PeerSGP == nil {
		return func() {}
	}
	c.muCanonicalAS.RLock()
	ids := []RemoteASID{c.canonicalRemoteAS[rc]}
	c.muCanonicalAS.RUnlock()
	sgp := marks.config.sgpByIdentity[*c.cfg.PeerSGP]
	for _, as := range sgp.applicationServers {
		if as.asKeyStatic && as.asKey.RoutingContextSet && as.asKey.RoutingContext == rc {
			ids = append(ids, as.id)
		}
	}
	return c.beginMTPStatusBindingChangeLocked(ids...)
}

func (p *ssnmPartitionState) expireStatusSource(key receivedCongestionKey, record SSNMCongestion) {
	if p.statusSources == nil {
		return
	}
	input := p.statusSources.records[key]
	if input == nil {
		return
	}
	if input.record.Freshness != CongestionExpired && record.Freshness == CongestionExpired {
		for _, prefix := range input.prefixes {
			p.statusSources.prefixes[prefix].revisions[input.record.Revision].expired++
		}
	}
	input.record = record
}
