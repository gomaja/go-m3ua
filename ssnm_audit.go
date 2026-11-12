package m3ua

import (
	"cmp"
	"encoding/binary"
	"math/bits"
	"slices"

	"github.com/gomaja/go-m3ua/messages"
)

type destinationAuditReply struct {
	destinationAuditGroup
	pointCode uint32
	mask      uint8
}
type auditReplyBudget struct{ messages, bytes uint64 }

func auditMultiply(a, b uint64) uint64 {
	if a != 0 && b > ^uint64(0)/a {
		return ^uint64(0)
	}
	return a * b
}
func auditAdd(a, b uint64) uint64 {
	if b > ^uint64(0)-a {
		return ^uint64(0)
	}
	return a + b
}

// RFC 4666 §3.4.1 aligned ranges nest or are disjoint. Removing one
// subrange from a 24-bit range needs at most 24 aligned pieces. Allow twice
// that per input and three record budgets for retained, affected and completed
// restart ranges. Ordinary state fits this envelope; restart history surviving
// repeated ForgetDestinations can exceed it (RFC 4666 §§4.5.3, 3.8.1).
func destinationAuditPieces(records, requests int) uint64 {
	return auditMultiply(48, auditAdd(auditAdd(auditMultiply(3, uint64(records)), uint64(requests)), 1))
}

// This coarse planning envelope charges two single-APC messages per scope and
// piece. The final budget accounts for actual RC/header sizes and list chunks.
func destinationAuditPlanningBytes(records, requests int) uint64 {
	return auditMultiply(128, destinationAuditPieces(records, requests))
}

func (c *Association) destinationAuditSnapshot() ([]destinationRecord, []auditRestartRange, int) {
	var records []destinationRecord
	limit := DefaultMaxSSNMDestinationRecords
	if c.destinations != nil {
		c.destinations.mu.RLock()
		limit = c.destinations.recordLimitLocked()
		records = make([]destinationRecord, 0, len(c.destinations.state))
		for _, r := range c.destinations.state {
			records = append(records, r)
		}
		c.destinations.mu.RUnlock()
	}
	var restarts []auditRestartRange
	if registry := c.mtp3Restarts; registry != nil {
		registry.mu.Lock()
		epoch := 0
		for _, r := range registry.active {
			for _, value := range r.affected {
				restarts = append(restarts, auditRestartRange{value, epoch, false})
			}
			for _, value := range r.completed {
				restarts = append(restarts, auditRestartRange{value, epoch, true})
			}
			epoch++
		}
		registry.mu.Unlock()
	}
	return records, restarts, limit
}

// RFC 4666 §§3.4.1, 4.5.3: plan disjoint aligned ranges before writing any
// reply. Scope-local sweeps replace per-interval store/registry scans. Equal
// complete scope plans share RC lists, bounding state groups to 48 per scope.
func (c *Association) destinationAuditReplies(d *messages.DestinationStateAudit, pcs []uint32, masks []uint8) ([]destinationAuditReply, error) {
	records, restarts, limit := c.destinationAuditSnapshot()
	return c.planDestinationAudit(d, pcs, masks, records, restarts, limit)
}

func (c *Association) planDestinationAudit(d *messages.DestinationStateAudit, pcs []uint32, masks []uint8, records []destinationRecord, restarts []auditRestartRange, recordLimit int) ([]destinationAuditReply, error) {
	appearance := c.destinationKey(d.NetworkAppearance, 0)
	contexts, contextSet := c.destinationRoutingContexts(d.RoutingContext)
	slices.Sort(contexts)
	contexts = slices.Compact(contexts)
	scopes := make([]destinationKey, 0, max(1, len(contexts)))
	scopeIndex := make(map[uint32]int, len(contexts))
	for _, rc := range contexts {
		scopeIndex[rc] = len(scopes)
		scope := appearance
		scope.routingContext, scope.routingContextSet = rc, true
		scopes = append(scopes, scope)
	}
	if !contextSet {
		scopes = append(scopes, appearance)
	}
	indices := make([][]int, len(scopes))
	// Index membership once. Contextless records apply to every requested RC;
	// explicit/list records only enter their matching scope's sweep.
	for i, r := range records {
		if r.rangeValue.NetworkAppearance != appearance.networkAppearance || r.rangeValue.NetworkAppearanceSet != appearance.networkAppearanceSet {
			continue
		}
		switch {
		case len(r.routingContexts) > 0:
			if contextSet {
				for _, rc := range r.routingContexts {
					if s, ok := scopeIndex[rc]; ok {
						indices[s] = append(indices[s], i)
					}
				}
			}
		case r.rangeValue.RoutingContextSet:
			if s, ok := scopeIndex[r.rangeValue.RoutingContext]; ok && contextSet {
				indices[s] = append(indices[s], i)
			}
		default:
			for s := range scopes {
				indices[s] = append(indices[s], i)
			}
		}
	}
	requested := make([]auditInterval, len(pcs))
	explicit := make(map[uint32]bool, 2*len(pcs))
	for i, pc := range pcs {
		start := destinationRangePrefix(pc, masks[i])
		end := start + uint32(1)<<effectiveDestinationMask(masks[i])
		requested[i] = auditInterval{start, end}
		explicit[start], explicit[end] = true, true
	}
	slices.SortFunc(requested, func(a, b auditInterval) int { return cmp.Compare(a.start, b.start) })
	union := requested[:0]
	for _, q := range requested {
		if len(union) > 0 && q.start <= union[len(union)-1].end {
			union[len(union)-1].end = max(union[len(union)-1].end, q.end)
		} else {
			union = append(union, q)
		}
	}
	maxBytes := destinationAuditPlanningBytes(recordLimit, len(pcs))
	type plan struct {
		entries  []destinationAuditReply
		contexts []uint32
	}
	var plans []plan
	seen := make(map[string]int)
	var scratch destinationAuditScratch
	for s, scope := range scopes {
		entries, err := scratch.sweepDestinationAudit(records, indices[s], restarts, scope, union, explicit, maxBytes)
		if err != nil {
			return nil, err
		}
		// The key encodes complete ordered state/range plans, not one interval.
		// Thus RC grouping cannot create exponentially many scope subsets.
		key := make([]byte, 12*len(entries))
		for i, r := range entries {
			b := key[i*12:]
			binary.BigEndian.PutUint32(b, r.pointCode)
			b[4], b[5], b[6] = r.mask, byte(r.state.Availability), r.state.Congestion.Level
			if r.state.Congestion.Congested {
				b[7] = 1
			}
			if r.state.Congestion.LevelSet {
				b[8] = 1
			}
		}
		if index, ok := seen[string(key)]; ok {
			if contextSet {
				plans[index].contexts = append(plans[index].contexts, scope.routingContext)
			}
			continue
		}
		seen[string(key)] = len(plans)
		p := plan{entries: entries}
		if contextSet {
			p.contexts = []uint32{scope.routingContext}
		}
		plans = append(plans, p)
	}
	var replies []destinationAuditReply
	for _, p := range plans {
		for _, entry := range p.entries {
			entry.routingContexts = p.contexts
			replies = append(replies, entry)
		}
	}
	return replies, nil
}

func appendDestinationAuditReplies(replies []destinationAuditReply, state DestinationNetworkState, start, end uint32, maxBytes uint64) ([]destinationAuditReply, error) {
	for start < end {
		mask := min(min(bits.TrailingZeros32(start), bits.Len32(end-start)-1), 24)
		// Every range needs at least four APC bytes on the wire, even after
		// grouping. This is a necessary byte check, not an arbitrary entry cap.
		if uint64(len(replies)) >= maxBytes/4 {
			return nil, ErrSSNMResourceLoss
		}
		replies = append(replies, destinationAuditReply{destinationAuditGroup: destinationAuditGroup{state: state}, pointCode: start, mask: uint8(mask)})
		start += uint32(1) << mask
	}
	return replies, nil
}
