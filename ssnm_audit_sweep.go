package m3ua

import (
	"cmp"
	"container/heap"
	"slices"
)

type auditInterval struct{ start, end uint32 }
type auditRestartRange struct {
	value     destinationRange
	epoch     int
	completed bool
}
type auditEvent struct {
	point uint32
	index int
	kind  uint8
	delta int
}
type auditPriority struct {
	sequence uint64
	index    int
}
type auditHeap []*auditPriority

func (h auditHeap) Len() int           { return len(h) }
func (h auditHeap) Less(i, j int) bool { return h[i].sequence > h[j].sequence }
func (h auditHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *auditHeap) Push(v any)        { *h = append(*h, v.(*auditPriority)) }
func (h *auditHeap) Pop() any {
	a := *h
	last := len(a) - 1
	v := a[last]
	a[last] = nil
	*h = a[:last]
	return v
}
func (h *auditHeap) prune(active []bool) {
	for len(*h) > 0 && !active[(*h)[0].index] {
		heap.Pop(h)
	}
}

// destinationAuditScratch belongs to one plan and is reused across its scopes.
// Heap elements point into priorities, avoiding per-record interface boxing.
// Results have separate backing storage and survive the next scope's reset.
type destinationAuditScratch struct {
	events                   []auditEvent
	boundaries               []uint32
	active                   []bool
	priorities               [][2]auditPriority
	availability, congestion auditHeap
	affected, completed      []int
}

// sweepDestinationAudit resolves each dimension from the newest active record.
// RFC 4666 §3.4.1 ranges are aligned dyadic intervals: any two nest or are
// disjoint. Start/end events therefore make record membership constant between
// boundaries. Each record enters/leaves the heaps once; no interval rescans the
// store or restart registry. Standard sorting and heap updates cost
// O((R+B) log(R+B)) per scope, including request and restart boundaries.
// RFC 4666 §4.6 isolation is an affected-minus-completed overlay per epoch;
// one epoch's recovery must never release another epoch's isolation.
func (scratch *destinationAuditScratch) sweepDestinationAudit(records []destinationRecord, indices []int, restarts []auditRestartRange, scope destinationKey, requested []auditInterval, explicit map[uint32]bool, maxBytes uint64) ([]destinationAuditReply, error) {
	events := slices.Grow(scratch.events[:0], 2*(len(indices)+len(restarts)))
	boundaries := slices.Grow(scratch.boundaries[:0], 2*(len(indices)+len(restarts)+len(requested))+len(explicit))
	for _, q := range requested {
		boundaries = append(boundaries, q.start, q.end)
	}
	for boundary := range explicit {
		boundaries = append(boundaries, boundary)
	}
	add := func(value destinationRange, index int, kind uint8) {
		start := destinationRangePrefix(value.PointCode, value.Mask)
		end := start + uint32(1)<<effectiveDestinationMask(value.Mask)
		events = append(events, auditEvent{start, index, kind, 1}, auditEvent{end, index, kind, -1})
		boundaries = append(boundaries, start, end)
	}
	for local, i := range indices {
		add(records[i].rangeValue, local, 0)
	}
	epochs := 0
	probe := destinationRange{NetworkAppearance: scope.networkAppearance, NetworkAppearanceSet: scope.networkAppearanceSet, RoutingContext: scope.routingContext, RoutingContextSet: scope.routingContextSet, Mask: 24}
	for _, r := range restarts {
		epochs = max(epochs, r.epoch+1)
		if r.completed {
			// Completion requires containment, unlike affected-range overlap. In
			// particular an explicit-RC recovery does not release a contextless query.
			if destinationScopeMatches(r.value, scope) {
				add(r.value, r.epoch, 2)
			}
		} else if destinationRangesOverlap(r.value, probe) {
			add(r.value, r.epoch, 1)
		}
	}
	// Equal-point event order does not matter: apply every event before
	// sampling the next interval. uint32 also retains endpoint 1<<24.
	slices.SortFunc(events, func(a, b auditEvent) int { return cmp.Compare(a.point, b.point) })
	slices.Sort(boundaries)
	boundaries = slices.Compact(boundaries)
	scratch.events, scratch.boundaries = events, boundaries
	scratch.active = slices.Grow(scratch.active[:0], len(indices))[:len(indices)]
	active := scratch.active
	clear(active)
	clear(scratch.availability)
	clear(scratch.congestion)
	scratch.availability, scratch.congestion = scratch.availability[:0], scratch.congestion[:0]
	scratch.priorities = slices.Grow(scratch.priorities[:0], len(indices))[:len(indices)]
	availability, congestion := &scratch.availability, &scratch.congestion
	scratch.affected = slices.Grow(scratch.affected[:0], epochs)[:epochs]
	scratch.completed = slices.Grow(scratch.completed[:0], epochs)[:epochs]
	affected, completed := scratch.affected, scratch.completed
	clear(affected)
	clear(completed)
	known, isolated, event, query := 0, 0, 0, 0
	var previous DestinationNetworkState
	var groupStart, groupEnd uint32
	pending := false
	var replies []destinationAuditReply
	flush := func() error {
		if !pending {
			return nil
		}
		var err error
		replies, err = appendDestinationAuditReplies(replies, previous, groupStart, groupEnd, maxBytes)
		return err
	}
	for b := 0; b+1 < len(boundaries); b++ {
		start, end := boundaries[b], boundaries[b+1]
		for event < len(events) && events[event].point <= start {
			e := events[event]
			event++
			if e.kind == 0 {
				active[e.index] = e.delta > 0
				known += e.delta
				if e.delta > 0 {
					record := records[indices[e.index]]
					scratch.priorities[e.index] = [2]auditPriority{{record.availabilitySequence, e.index}, {record.congestionSequence, e.index}}
					availability.prune(active)
					congestion.prune(active)
					if record.availabilitySequence > 0 && record.dimensions.carries(destinationAvailabilityDimension) {
						heap.Push(availability, &scratch.priorities[e.index][0])
					}
					if record.congestionSequence > 0 && record.dimensions.carries(destinationCongestionDimension) {
						heap.Push(congestion, &scratch.priorities[e.index][1])
					}
				}
			} else {
				was := affected[e.index] > 0 && completed[e.index] == 0
				if e.kind == 1 {
					affected[e.index] += e.delta
				} else {
					completed[e.index] += e.delta
				}
				now := affected[e.index] > 0 && completed[e.index] == 0
				if was && !now {
					isolated--
				}
				if !was && now {
					isolated++
				}
			}
		}
		for query < len(requested) && requested[query].end <= start {
			query++
		}
		if query == len(requested) || requested[query].start > start {
			continue
		}
		state := DestinationNetworkState{Availability: DestinationUnavailable}
		if known > 0 {
			state = DestinationNetworkState{}
			availability.prune(active)
			congestion.prune(active)
			if len(*availability) > 0 {
				state.Availability = records[indices[(*availability)[0].index]].rangeValue.State.Availability
			}
			if len(*congestion) > 0 {
				state.Congestion = records[indices[(*congestion)[0].index]].rangeValue.State.Congestion
			}
		}
		if isolated > 0 {
			state = DestinationNetworkState{Availability: DestinationUnavailable}
		}
		if pending && groupEnd == start && !explicit[start] && previous == state {
			groupEnd = end
			continue
		}
		if err := flush(); err != nil {
			return nil, err
		}
		pending = true
		previous, groupStart, groupEnd = state, start, end
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return replies, nil
}
