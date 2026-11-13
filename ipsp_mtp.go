package m3ua

import (
	"fmt"
	"sync"
)

// IPSPDestinationConfig explicitly maps a peer-directed IPSP AS to an MTP
// destination. It does not infer peer reachability from received DATA.
// Selection controls aggregation: Broadcast includes every active copy;
// PrimaryBackup and Loadshare treat matching peers as alternatives.
type IPSPDestinationConfig struct {
	Destination MTPDestination
	AS          ASKey
	Selection   RouteSelectionMode
}

type ipspMTPState struct {
	mu     sync.Mutex
	config []IPSPDestinationConfig
	stream *aspRoutes
}

func newIPSPMTPState(role Role, config []IPSPDestinationConfig) (*ipspMTPState, error) {
	if role != RoleIPSP {
		if len(config) > 0 {
			return nil, fmt.Errorf("%w: IPSP destinations require IPSP role", ErrInvalidRoleConfiguration)
		}
		return nil, nil
	}
	stream, err := newASPRoutes(nil)
	if err != nil {
		return nil, err
	}
	if len(config) > DefaultMaxSSNMDestinationRecords {
		return nil, fmt.Errorf("%w: too many IPSP destinations", ErrInvalidRoleConfiguration)
	}
	state := &ipspMTPState{config: append([]IPSPDestinationConfig(nil), config...), stream: stream}
	stream.config.mtpRouteByID = make(map[MTPRouteID]int, len(config))
	for index, c := range state.config {
		d := c.Destination
		if _, duplicate := stream.config.mtpRouteByID[d.MTPRoute]; duplicate || d.MTPRoute == "" || d.PointCode > 0xffffff || d.Mask > 24 || destinationRangePrefix(d.PointCode, d.Mask) != d.PointCode || normalizeOutboundASKey(c.AS) != c.AS {
			return nil, fmt.Errorf("%w: invalid or duplicate IPSP destination", ErrInvalidRoleConfiguration)
		}
		if c.Selection == 0 {
			c.Selection = RouteSelectionLoadshare
			state.config[index] = c
		}
		if !validRouteSelectionMode(c.Selection) {
			return nil, fmt.Errorf("%w: invalid IPSP selection", ErrInvalidRoleConfiguration)
		}
		stream.config.mtpRouteByID[d.MTPRoute] = index
		stream.config.mtpRoutes = append(stream.config.mtpRoutes, aspMTPRoute{id: d.MTPRoute, destinationPointCode: d.PointCode, mask: d.Mask})
		stream.derived[d.MTPRoute] = map[aspDerivedRangeKey]aspDestinationStatus{{mtpRoute: d.MTPRoute, pointCode: d.PointCode, mask: d.Mask}: {availability: DestinationUnavailable}}
	}
	return state, nil
}

func (e *Endpoint) refreshIPSPDestinations() {
	if e == nil || e.ipspMTP == nil {
		return
	}
	state := e.ipspMTP
	// Serialize snapshots as well as publication, so an older membership view
	// cannot replace a newer teardown/restart view.
	state.mu.Lock()
	defer state.mu.Unlock()
	e.mu.Lock()
	associations := make([]*Association, 0, len(e.associations))
	for a := range e.associations {
		associations = append(associations, a)
	}
	closed := e.closed
	e.mu.Unlock()
	if closed {
		associations = nil
	}
	r := state.stream
	r.mu.Lock()
	defer r.mu.Unlock()
	for index, c := range state.config {
		route := r.config.mtpRoutes[index]
		updated := make(map[aspDerivedRangeKey]aspDestinationStatus)
		scope := destinationKey{networkAppearance: c.AS.NetworkAppearance, networkAppearanceSet: c.AS.NetworkAppearanceSet, routingContext: c.AS.RoutingContext, routingContextSet: c.AS.RoutingContextSet}
		// Split on retained SSNM prefixes without expanding a point-code range.
		tree := aspDerivedStatusTree{nodes: make([]aspDerivedStatusNode, 1)}
		eligible := make([]*Association, 0, len(associations))
		for _, a := range associations {
			if associationDone(a) || !aspAssociationEligibleForAS(a, c.AS) {
				continue
			}
			eligible = append(eligible, a)
			for _, part := range a.destinations.rangesForScope(scope) {
				if part.Mask < c.Destination.Mask && destinationRangeCovers(destinationRange{PointCode: c.Destination.PointCode, Mask: c.Destination.Mask}, part.PointCode, part.Mask) {
					tree.nodeForRange(route.mask, part.PointCode, part.Mask)
				}
			}
		}
		var visit func(int, uint32, uint8)
		visit = func(node int, pc uint32, mask uint8) {
			if mask > 0 && tree.nodeHasChildren(node) {
				for branch := 0; branch < 2; branch++ {
					child := 0
					if node != 0 {
						child = tree.nodes[node-1].children[branch]
					}
					visit(child, pc|uint32(branch)<<(mask-1), mask-1)
				}
				return
			}
			best := aspDestinationStatus{availability: DestinationUnavailable}
			found := false
			for _, a := range eligible {
				s := aspDestinationStatus{availability: DestinationAvailable}
				if received, ok := a.destinations.lookupRange(scope, pc, mask); ok {
					s.congested = received.Congestion.Congested
					s.congestionLevel = received.Congestion.Level
					s.congestionLevelSet = received.Congestion.LevelSet
				}
				s = combineASPTransport(s, transportASPStatus(a))
				if !found || aspAvailabilityRank(s.availability) < aspAvailabilityRank(best.availability) {
					best = s
					found = true
					continue
				}
				if s.availability == best.availability {
					if c.Selection == RouteSelectionBroadcast {
						best = mostCongestedASPStatus(best, s)
					} else {
						best = leastCongestedASPStatus(best, s)
					}
				}
			}
			updated[aspDerivedRangeKey{mtpRoute: route.id, pointCode: pc, mask: mask}] = best
		}
		visit(1, route.destinationPointCode, route.mask)
		r.publish(r.derivedStatusIndicationsLocked(route, updated))
		r.derived[route.id] = updated
	}
}
