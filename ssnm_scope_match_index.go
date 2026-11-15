package m3ua

// Scope matching compiles one incoming AS set, rather than joining that set
// separately to every retained statement and job origin (RFC 4666 §3.4).
type ssnmContextIdentity struct {
	value uint32
	set   bool
}
type ssnmAppearanceSet struct {
	any, wildcard bool
	named         map[uint32]struct{}
}

func (s *ssnmAppearanceSet) add(key ASKey) {
	s.any = true
	if !key.NetworkAppearanceSet {
		s.wildcard = true
		return
	}
	if s.named == nil {
		s.named = make(map[uint32]struct{})
	}
	s.named[key.NetworkAppearance] = struct{}{}
}
func (s ssnmAppearanceSet) overlaps(key ASKey) bool {
	if !key.NetworkAppearanceSet {
		return s.any
	}
	_, held := s.named[key.NetworkAppearance]
	return s.wildcard || held
}

type ssnmScopeMatchIndex struct {
	all      ssnmAppearanceSet
	contexts map[ssnmContextIdentity]ssnmAppearanceSet
}
type ssnmScopeMatcher struct {
	refusal   ssnmDestinationRefusal
	partition SSNMPartition
	index     *ssnmScopeMatchIndex
}

func compileSSNMScopeIndex(keys []ASKey, work *ssnmWorkCount) *ssnmScopeMatchIndex {
	index := &ssnmScopeMatchIndex{contexts: make(map[ssnmContextIdentity]ssnmAppearanceSet)}
	for _, key := range keys {
		work.visitAS()
		index.all.add(key)
		id := ssnmContextIdentity{key.RoutingContext, key.RoutingContextSet}
		group := index.contexts[id]
		group.add(key)
		index.contexts[id] = group
	}
	return index
}
func (r ssnmDestinationRefusal) matchers(partitions []SSNMPartition) map[SSNMPartition]ssnmScopeMatcher {
	matchers := make(map[SSNMPartition]ssnmScopeMatcher, len(partitions))
	var shared *ssnmScopeMatchIndex
	if r.resolved && r.byPartition == nil {
		shared = compileSSNMScopeIndex(r.keys, r.work)
	}
	for _, partition := range partitions {
		index := shared
		if r.resolved && r.byPartition != nil {
			index = compileSSNMScopeIndex(r.byPartition[partition], r.work)
		}
		matchers[partition] = ssnmScopeMatcher{r, partition, index}
	}
	return matchers
}
func (m ssnmScopeMatcher) matches(scope WireScope, retained []ASKey) bool {
	// Unresolved admission retains the conservative original rule. A compiled
	// empty resolved set is distinct from this absence of a resolution.
	if m.index == nil {
		return m.refusal.matches(m.partition, scope, retained)
	}
	matches := func(key ASKey) bool {
		m.refusal.work.visitAS()
		if m.partition.Kind != SSNMStandalonePartition {
			return m.index.all.overlaps(key)
		}
		return m.index.contexts[ssnmContextIdentity{key.RoutingContext, key.RoutingContextSet}].overlaps(key)
	}
	if len(retained) == 0 {
		if !scope.RoutingContextSet {
			m.refusal.work.visitAS()
			return m.index.all.overlaps(ASKey{NetworkAppearance: scope.NetworkAppearance, NetworkAppearanceSet: scope.NetworkAppearanceSet})
		}
		for _, rc := range scope.RoutingContexts {
			if matches(ASKey{RoutingContext: rc, RoutingContextSet: true, NetworkAppearance: scope.NetworkAppearance, NetworkAppearanceSet: scope.NetworkAppearanceSet}) {
				return true
			}
		}
		return false
	}
	for _, key := range retained {
		if matches(key) {
			return true
		}
	}
	return false
}
