package m3ua

import "math/bits"

// ssnmSourceProjection is an immutable derived index. Source mutations discard
// it under ssnmState.mu; reads build it under that same lock. It owns no wire
// scopes, identities, timestamps, or additional authoritative history.
type ssnmSourceProjection struct {
	trees                    map[ssnmSourceProjectionGroup]ssnmSourceProjectionTree
	availability, congestion bool
}

type ssnmSourceProjectionGroup struct {
	all bool
	as  ASKey
}

type ssnmSourceProjectionTree struct {
	nodes []ssnmSourceProjectionNode
}

type ssnmSourceProjectionNode struct {
	children [2]int // One-based indexes; zero means the inherited status applies.
	status   ssnmSourceProjectionStatus
}

type ssnmSourceProjectionStatus struct {
	availability                                                 DestinationAvailability
	level                                                        uint8
	availabilitySet, congestionSet, congested, levelSet, unknown bool
}

// Unscoped queries include every source. A canonical query without an NA is
// also unscoped; canonical RC labels never distinguish this partition's owner.
func sourceProjectionQueryGroup(standalone bool, query []ASKey) ssnmSourceProjectionGroup {
	if len(query) == 0 || !standalone && !query[0].NetworkAppearanceSet {
		return ssnmSourceProjectionGroup{all: true}
	}
	return ssnmSourceProjectionGroup{as: sourceProjectionGroupAS(standalone, query[0])}
}

func sourceProjectionRecordGroup(standalone bool, as ASKey, scope WireScope) (ssnmSourceProjectionGroup, bool) {
	if scope.NetworkAppearanceSet {
		as.NetworkAppearance, as.NetworkAppearanceSet = scope.NetworkAppearance, true
	}
	if !standalone && !as.NetworkAppearanceSet {
		return ssnmSourceProjectionGroup{}, false
	}
	return ssnmSourceProjectionGroup{as: sourceProjectionGroupAS(standalone, as)}, true
}

func sourceProjectionGroupAS(standalone bool, as ASKey) ASKey {
	if !standalone {
		as.RoutingContext, as.RoutingContextSet = 0, false
	}
	if !as.NetworkAppearanceSet {
		as.NetworkAppearance = 0
	}
	return as
}

func (p *ssnmPartitionState) sourceStatus(pointCode uint32, status aspSelectionStatus, query ...ASKey) aspSelectionStatus {
	if p.sourceProjection == nil {
		p.sourceProjection = p.buildSourceProjection()
	}
	projection := p.sourceProjection
	group := sourceProjectionQueryGroup(p.partition.Kind == SSNMStandalonePartition, query)
	var value ssnmSourceProjectionStatus
	if tree, held := projection.trees[group]; held {
		value = tree.lookup(pointCode, p.work)
	}
	// History replaces a whole dimension even when this query has no matching
	// group or covering source. It must not uncover a displaced scalar value.
	if projection.availability {
		status.availability, status.availabilitySet = value.availability, value.availabilitySet
	}
	if projection.congestion {
		status.congestionSet, status.congested = value.congestionSet, value.congested
		status.congestionLevel, status.congestionLevelSet = value.level, value.levelSet
		status.receivedUnknown = status.receivedUnknown || value.unknown
	}
	return status
}

func (t ssnmSourceProjectionTree) lookup(pointCode uint32, work *ssnmWorkCount) ssnmSourceProjectionStatus {
	index := 1
	for mask := uint8(maxPointCodeMask); ; mask-- {
		work.visitPrefix()
		node := t.nodes[index-1]
		if mask == 0 {
			return node.status
		}
		child := node.children[pointCode>>(mask-1)&1]
		if child == 0 {
			return node.status
		}
		index = child
	}
}

// Each source contributes to the all-source tree and at most one scoped tree.
// Every insertion creates at most 25 nodes, including a previously absent root.
func (p *ssnmPartitionState) buildSourceProjection() *ssnmSourceProjection {
	builders := make(map[ssnmSourceProjectionGroup]*ssnmSourceProjectionBuilder)
	builder := func(group ssnmSourceProjectionGroup) *ssnmSourceProjectionBuilder {
		b := builders[group]
		if b == nil {
			b = &ssnmSourceProjectionBuilder{nodes: make([]ssnmSourceProjectionBuildNode, 1)}
			builders[group] = b
		}
		return b
	}
	standalone := p.partition.Kind == SSNMStandalonePartition
	all := ssnmSourceProjectionGroup{all: true}
	for key, record := range p.receivedAvailability {
		p.work.visitSource()
		builder(all).addAvailability(key.destination, record.Revision, record.State, p.work)
		if group, scoped := sourceProjectionRecordGroup(standalone, record.AS, record.Scope); scoped {
			builder(group).addAvailability(key.destination, record.Revision, record.State, p.work)
		}
	}
	for key, record := range p.receivedCongestion {
		p.work.visitSource()
		value := ssnmSourceProjectionCongestion{revision: record.Revision, level: record.Level, levelSet: record.LevelSet, congested: record.Congested, expired: record.Freshness == CongestionExpired}
		builder(all).addCongestion(key.destination, key.association, value, p.work)
		if group, scoped := sourceProjectionRecordGroup(standalone, record.AS, record.Scope); scoped {
			builder(group).addCongestion(key.destination, key.association, value, p.work)
		}
	}
	projection := &ssnmSourceProjection{trees: make(map[ssnmSourceProjectionGroup]ssnmSourceProjectionTree, len(builders)), availability: len(p.receivedAvailability) != 0, congestion: len(p.receivedCongestion) != 0}
	state := ssnmSourceProjectionWalk{latest: make(map[AssociationID]ssnmSourceProjectionCongestion)}
	for group, b := range builders {
		projection.trees[group] = b.finish(&state, p.work)
	}
	return projection
}

type ssnmSourceProjectionBuildNode struct {
	children             [2]int
	head                 int // One-based congestion entry index.
	availabilityRevision uint64
	availability         DestinationAvailability
}

type ssnmSourceProjectionCongestion struct {
	revision                     uint64
	level                        uint8
	levelSet, congested, expired bool
}

type ssnmSourceProjectionEntry struct {
	value       ssnmSourceProjectionCongestion
	association AssociationID
	next        int
}

type ssnmSourceProjectionBuilder struct {
	nodes   []ssnmSourceProjectionBuildNode
	entries []ssnmSourceProjectionEntry
}

func (b *ssnmSourceProjectionBuilder) nodeFor(destination ssnmDestinationKey, work *ssnmWorkCount) int {
	index := 1
	for mask := uint8(maxPointCodeMask); ; mask-- {
		work.visitPrefix()
		if mask <= destination.mask {
			return index
		}
		branch := destination.pointCode >> (mask - 1) & 1
		child := b.nodes[index-1].children[branch]
		if child == 0 {
			b.nodes = append(b.nodes, ssnmSourceProjectionBuildNode{})
			child = len(b.nodes)
			b.nodes[index-1].children[branch] = child
		}
		index = child
	}
}

func (b *ssnmSourceProjectionBuilder) addAvailability(destination ssnmDestinationKey, revision uint64, state DestinationAvailability, work *ssnmWorkCount) {
	index := b.nodeFor(destination, work)
	node := &b.nodes[index-1]
	if revision > node.availabilityRevision {
		node.availabilityRevision, node.availability = revision, state
	}
}

func (b *ssnmSourceProjectionBuilder) addCongestion(destination ssnmDestinationKey, association AssociationID, value ssnmSourceProjectionCongestion, work *ssnmWorkCount) {
	index := b.nodeFor(destination, work)
	node := &b.nodes[index-1]
	b.entries = append(b.entries, ssnmSourceProjectionEntry{value: value, association: association, next: node.head})
	node.head = len(b.entries)
}

func (b *ssnmSourceProjectionBuilder) finish(state *ssnmSourceProjectionWalk, work *ssnmWorkCount) ssnmSourceProjectionTree {
	tree := ssnmSourceProjectionTree{nodes: make([]ssnmSourceProjectionNode, len(b.nodes))}
	b.project(1, 0, 0, state, tree.nodes, work)
	return tree
}

type ssnmSourceProjectionUndo struct {
	association AssociationID
	previous    ssnmSourceProjectionCongestion
	held        bool
}

type ssnmSourceProjectionWalk struct {
	latest                         map[AssociationID]ssnmSourceProjectionCongestion
	undo                           []ssnmSourceProjectionUndo
	fresh, expired, clearWithLevel int
	levels, unspecified            ssnmSourceProjectionLevels
}

// Roll back just the reporters changed by this node. Copying the whole latest
// map at each branch would multiply broad-source work by the number of leaves.
func (b *ssnmSourceProjectionBuilder) project(index int, availabilityRevision uint64, availability DestinationAvailability, state *ssnmSourceProjectionWalk, nodes []ssnmSourceProjectionNode, work *ssnmWorkCount) {
	work.visitPrefix()
	node := b.nodes[index-1]
	if node.availabilityRevision > availabilityRevision {
		availabilityRevision, availability = node.availabilityRevision, node.availability
	}
	mark := len(state.undo)
	for entryIndex := node.head; entryIndex != 0; {
		work.visitSource()
		entry := b.entries[entryIndex-1]
		entryIndex = entry.next
		old, held := state.latest[entry.association]
		if held && old.revision >= entry.value.revision {
			continue
		}
		state.undo = append(state.undo, ssnmSourceProjectionUndo{association: entry.association, previous: old, held: held})
		if held {
			state.adjust(old, -1)
		}
		state.latest[entry.association] = entry.value
		state.adjust(entry.value, 1)
	}
	value := state.status()
	value.availability, value.availabilitySet = availability, availabilityRevision != 0
	nodes[index-1] = ssnmSourceProjectionNode{children: node.children, status: value}
	for _, child := range node.children {
		if child != 0 {
			b.project(child, availabilityRevision, availability, state, nodes, work)
		}
	}
	for len(state.undo) > mark {
		work.visitPrefix()
		last := len(state.undo) - 1
		undo := state.undo[last]
		state.undo = state.undo[:last]
		state.adjust(state.latest[undo.association], -1)
		if undo.held {
			state.latest[undo.association] = undo.previous
			state.adjust(undo.previous, 1)
		} else {
			delete(state.latest, undo.association)
		}
	}
}

func (s *ssnmSourceProjectionWalk) adjust(value ssnmSourceProjectionCongestion, delta int) {
	if value.expired {
		s.expired += delta
		return
	}
	s.fresh += delta
	if value.congested {
		if value.levelSet {
			s.levels.adjust(value.level, delta)
		} else {
			s.unspecified.adjust(value.level, delta)
		}
	} else if value.levelSet {
		s.clearWithLevel += delta
	}
}

func (s *ssnmSourceProjectionWalk) status() ssnmSourceProjectionStatus {
	value := ssnmSourceProjectionStatus{congestionSet: s.fresh != 0, unknown: s.expired != 0}
	if level, found := s.unspecified.maximum(); found {
		value.congested, value.level = true, level
	} else if level, found := s.levels.maximum(); found {
		value.congested, value.level, value.levelSet = true, level, true
	} else {
		value.levelSet = s.clearWithLevel != 0
	}
	return value
}

// A fixed 256-value histogram covers the uint8 storage domain, including
// directly supplied values outside the usual configured congestion levels.
// The four-word occupancy bitmap makes selection independent of reporter count.
type ssnmSourceProjectionLevels struct {
	counts   [256]int
	occupied [4]uint64
}

func (s *ssnmSourceProjectionLevels) adjust(level uint8, delta int) {
	s.counts[level] += delta
	word, bit := level/64, uint64(1)<<(level%64)
	if s.counts[level] == 0 {
		s.occupied[word] &^= bit
	} else {
		s.occupied[word] |= bit
	}
}

func (s *ssnmSourceProjectionLevels) maximum() (uint8, bool) {
	for index := len(s.occupied) - 1; index >= 0; index-- {
		if word := s.occupied[index]; word != 0 {
			return uint8(index*64 + bits.Len64(word) - 1), true
		}
	}
	return 0, false
}
