package m3ua

import (
	"sort"

	"github.com/gomaja/go-sctp"
)

// AssociationID is the stable Endpoint-local identity of one M3UA
// Association. It is not the kernel SCTP association identifier.
type AssociationID uint64

// AssociationSnapshot is the M-SCTP_STATUS and current M3UA state of one
// Association at the time of an Endpoint query.
//
// RFC 4666 Sections 1.6.3 and 4.2 define status as a local Layer Management
// query. No peer message is sent.
type AssociationSnapshot struct {
	Association AssociationID
	Role        Role
	State       State
	IPSPState   IPSPState
	LocalAddr   *sctp.SCTPAddr
	RemoteAddr  *sctp.SCTPAddr

	PeerASPIdentifier    uint32
	PeerASPIdentifierSet bool

	SCTP      *AssociationStatus
	SCTPError error
}

// ASPStatusKey identifies one local or peer ASP state in one exact Application
// Server scope.
type ASPStatusKey struct {
	Association AssociationID
	AS          ASKey
}

// ASPStatus is the M-ASP_STATUS result for one Association and AS.
//
// RFC 4666 Sections 4.3 and 5.6.2 give IPSP Double Exchange independent local
// and peer directions, so presence is explicit for both states and both ASP
// Identifiers.
type ASPStatus struct {
	Key ASPStatusKey

	LocalState    State
	LocalStateSet bool
	PeerState     State
	PeerStateSet  bool

	LocalASPIdentifier    uint32
	LocalASPIdentifierSet bool
	PeerASPIdentifier     uint32
	PeerASPIdentifierSet  bool
}

// ApplicationServerStatus is the M-AS_STATUS result for one exact ASKey.
type ApplicationServerStatus struct {
	AS                 ASKey
	State              ASState
	TrafficMode        uint32
	RequiredActiveASPs int
	ActiveASPs         []AssociationID
}

// MTPRouteStatus is the ASP Endpoint's current view of one configured MTP
// Route and the Associations eligible to carry it.
type MTPRouteStatus struct {
	MTPRoute     MTPRouteID
	Destinations []MTPDestinationStatus
	Associations []AssociationID
}

// DestinationStatusKey identifies one destination query in an exact Network
// Appearance and Routing Context scope.
type DestinationStatusKey struct {
	NetworkAppearance    uint32
	NetworkAppearanceSet bool
	RoutingContext       uint32
	RoutingContextSet    bool
	PointCode            uint32
	Mask                 uint8
}

// DestinationStatusSnapshot is one SGP destination record used to answer
// RFC 4666 Section 4.5.3 destination audits. State carries both dimensions the
// audit has to answer, whichever of them was reported last.
type DestinationStatusSnapshot struct {
	Key   DestinationStatusKey
	State DestinationNetworkState
}

// AssociationStatus returns one Endpoint-owned Association status by its
// stable identity. The boolean is false when the identity is unknown or no
// longer tracked.
func (e *Endpoint) AssociationStatus(id AssociationID) (AssociationSnapshot, bool) {
	if e == nil || id == 0 {
		return AssociationSnapshot{}, false
	}
	e.mu.Lock()
	association := e.associationsByID[id]
	e.mu.Unlock()
	if association == nil {
		return AssociationSnapshot{}, false
	}
	return snapshotAssociation(association), true
}

// AssociationStatuses returns every currently tracked Association in stable
// AssociationID order. The returned slice and all nested values are owned by
// the caller.
func (e *Endpoint) AssociationStatuses() []AssociationSnapshot {
	if e == nil {
		return nil
	}
	e.mu.Lock()
	associations := make([]*Association, 0, len(e.associationsByID))
	for _, association := range e.associationsByID {
		associations = append(associations, association)
	}
	e.mu.Unlock()
	sort.Slice(associations, func(i, j int) bool {
		return associations[i].ID() < associations[j].ID()
	})
	statuses := make([]AssociationSnapshot, 0, len(associations))
	for _, association := range associations {
		statuses = append(statuses, snapshotAssociation(association))
	}
	return statuses
}

func snapshotAssociation(association *Association) AssociationSnapshot {
	status := AssociationSnapshot{
		Association: association.ID(),
		Role:        association.role,
		State:       association.State(),
		IPSPState:   association.IPSPState(),
	}
	status.PeerASPIdentifier, status.PeerASPIdentifierSet = association.PeerASPIdentifier()
	if association.sctpConn != nil {
		status.LocalAddr = cloneSCTPAddrFromNetAddr(association.sctpConn.LocalAddr())
		status.RemoteAddr = cloneSCTPAddrFromNetAddr(association.sctpConn.RemoteAddr())
	}
	status.SCTP, status.SCTPError = association.AssociationStatus()
	return status
}

// ASPStatus returns the status of one ASP and exact Application Server.
func (e *Endpoint) ASPStatus(key ASPStatusKey) (ASPStatus, bool) {
	if e == nil || key.Association == 0 {
		return ASPStatus{}, false
	}
	e.mu.Lock()
	association := e.associationsByID[key.Association]
	e.mu.Unlock()
	if association == nil {
		return ASPStatus{}, false
	}
	return e.aspStatusForAssociation(association, key.AS)
}

// ASPStatuses returns every local or peer ASP status in deterministic
// AssociationID and ASKey order.
func (e *Endpoint) ASPStatuses() []ASPStatus {
	if e == nil {
		return nil
	}
	e.mu.Lock()
	associations := make([]*Association, 0, len(e.associationsByID))
	for _, association := range e.associationsByID {
		associations = append(associations, association)
	}
	e.mu.Unlock()
	sort.Slice(associations, func(i, j int) bool {
		return associations[i].ID() < associations[j].ID()
	})

	statuses := make([]ASPStatus, 0)
	for _, association := range associations {
		keys := endpointASPStatusKeys(association)
		for _, key := range keys {
			if status, ok := e.aspStatusForAssociation(association, key); ok {
				statuses = append(statuses, status)
			}
		}
	}
	return statuses
}

func endpointASPStatusKeys(association *Association) []ASKey {
	keys := association.configuredASKeys()
	if association.role == RoleASP || association.isIPSPDoubleExchange() {
		keys = append(keys, association.configuredLocalASKeysForStatus()...)
	}
	keys = uniqueASKeys(keys)
	sort.Slice(keys, func(i, j int) bool { return compareASKey(keys[i], keys[j]) < 0 })
	return keys
}

func (e *Endpoint) aspStatusForAssociation(association *Association, key ASKey) (ASPStatus, bool) {
	status := ASPStatus{Key: ASPStatusKey{Association: association.ID(), AS: key}}

	if association.role == RoleASP || association.role == RoleIPSP {
		if containsASKey(association.configuredLocalASKeysForStatus(), key) {
			status.LocalState = association.localASPStateForStatus(key)
			status.LocalStateSet = true
			if association.cfg != nil && association.cfg.ASPIdentifier != nil {
				status.LocalASPIdentifier = association.cfg.ASPIdentifier.AspIdentifier()
				status.LocalASPIdentifierSet = true
			}
		}
	}

	if association.role == RoleSGP || association.role == RoleIPSP {
		if e.as != nil {
			if applicationServer, ok := e.as.lookup(key); ok {
				applicationServer.mu.Lock()
				state, member := applicationServer.asps[association]
				applicationServer.mu.Unlock()
				if member {
					status.PeerState = state
					status.PeerStateSet = true
					status.PeerASPIdentifier, status.PeerASPIdentifierSet =
						association.PeerASPIdentifier()
				}
			}
		}
	}

	if association.role == RoleIPSP && !association.isIPSPDoubleExchange() {
		if status.LocalStateSet && !status.PeerStateSet {
			status.PeerState, status.PeerStateSet = status.LocalState, true
		}
		if status.PeerStateSet && !status.LocalStateSet {
			status.LocalState, status.LocalStateSet = status.PeerState, true
		}
	}

	return status, status.LocalStateSet || status.PeerStateSet
}

func (c *Association) configuredLocalASKeysForStatus() []ASKey {
	if c == nil {
		return nil
	}
	if !c.isIPSPDoubleExchange() {
		// RFC 4666 Sections 4.4.1 and 4.3.4.3 make the Routing Context
		// assigned by successful registration part of the requester's AS
		// membership and available to its subsequent ASPTM procedures.
		return c.configuredASKeys()
	}
	if !c.hasLocalIPSPTrafficDirection() {
		return nil
	}
	routingContexts := asConfigRoutingContexts(c.applicationServerInventory(true))
	keys := make([]ASKey, 0, len(routingContexts)+1)
	if len(routingContexts) == 0 {
		keys = append(keys, c.contextlessASKey(true))
	} else {
		for _, routingContext := range routingContexts {
			keys = append(keys, c.staticASKeyForRoutingContext(routingContext, true))
		}
	}
	c.muDynamicASKeys.RLock()
	for _, key := range c.dynamicLocalASKeys {
		keys = append(keys, key)
	}
	c.muDynamicASKeys.RUnlock()
	return uniqueASKeys(keys)
}

func (c *Association) localASPStateForStatus(key ASKey) State {
	state := c.State()
	if c.isIPSPDoubleExchange() {
		state = c.localIPSPStateValue()
	}
	if state != StateASPActive {
		return state
	}
	if !key.RoutingContextSet {
		if c.contextlessASAcked() {
			return StateASPActive
		}
		return StateASPInactive
	}
	if c.routingContextAcked(key.RoutingContext) && !c.routingContextOverridden(key.RoutingContext) {
		return StateASPActive
	}
	return StateASPInactive
}

// ApplicationServerStatus returns the status of one exact Application Server.
func (e *Endpoint) ApplicationServerStatus(key ASKey) (ApplicationServerStatus, bool) {
	if e == nil || e.as == nil {
		return ApplicationServerStatus{}, false
	}
	applicationServer, ok := e.as.lookup(key)
	if !ok {
		return ApplicationServerStatus{}, false
	}
	return snapshotApplicationServer(applicationServer), true
}

// ApplicationServerStatuses returns every Application Server in ASKey order.
func (e *Endpoint) ApplicationServerStatuses() []ApplicationServerStatus {
	if e == nil || e.as == nil {
		return nil
	}
	keys := e.as.keys()
	statuses := make([]ApplicationServerStatus, 0, len(keys))
	for _, key := range keys {
		if status, ok := e.ApplicationServerStatus(key); ok {
			statuses = append(statuses, status)
		}
	}
	return statuses
}

func snapshotApplicationServer(applicationServer *applicationServer) ApplicationServerStatus {
	applicationServer.mu.Lock()
	defer applicationServer.mu.Unlock()
	active := make([]AssociationID, 0)
	for association, state := range applicationServer.asps {
		if state == StateASPActive && association.ID() != 0 {
			active = append(active, association.ID())
		}
	}
	sort.Slice(active, func(i, j int) bool { return active[i] < active[j] })
	return ApplicationServerStatus{
		AS:                 applicationServer.key,
		State:              applicationServer.state,
		TrafficMode:        applicationServer.trafficMode,
		RequiredActiveASPs: applicationServer.activationPolicy.requiredActive(),
		ActiveASPs:         active,
	}
}

// MTPRouteStatus returns one configured ASP MTP Route by its local key.
//
// It reads aspRoutes.derived in one pass, collecting only this route's own
// keys, rather than building every route's destinations
// to answer one of them: the cost is proportional to the total number of
// derived destination records, not to their product with the route count.
func (e *Endpoint) MTPRouteStatus(id MTPRouteID) (MTPRouteStatus, bool) {
	if e == nil || e.role != RoleASP || e.aspRoutes == nil {
		return MTPRouteStatus{}, false
	}
	routes := e.aspRoutes
	routes.mu.RLock()
	defer routes.mu.RUnlock()
	if _, exists := routes.config.mtpRouteByID[id]; !exists {
		return MTPRouteStatus{}, false
	}
	associations := make([]AssociationID, 0)
	for association, eligible := range routes.associationEligibleRoutes {
		if _, ok := eligible[id]; ok && association.ID() != 0 {
			associations = append(associations, association.ID())
		}
	}
	sort.Slice(associations, func(i, j int) bool { return associations[i] < associations[j] })

	keys := make([]aspDerivedRangeKey, 0)
	for key := range routes.derived {
		if key.mtpRoute == id {
			keys = append(keys, key)
		}
	}

	return MTPRouteStatus{
		MTPRoute:     id,
		Destinations: sortedDestinationStatuses(routes.derived, keys),
		Associations: associations,
	}, true
}

// MTPRouteStatuses returns every configured ASP MTP Route in configuration
// order. Every nested slice is caller-owned.
//
// It builds every route's destinations and eligible Associations in one pass
// each under one read lock — one pass over
// aspRoutes.derived grouping keys by route, and one pass over
// associationEligibleRoutes grouping Associations by route — rather than
// querying one route at a time: the cost is proportional to the configured
// routes, Associations and destination records, not to their product.
func (e *Endpoint) MTPRouteStatuses() []MTPRouteStatus {
	if e == nil || e.role != RoleASP || e.aspRoutes == nil {
		return nil
	}
	routes := e.aspRoutes
	routes.mu.RLock()
	defer routes.mu.RUnlock()

	destinationsByRoute := make(map[MTPRouteID][]aspDerivedRangeKey, len(routes.config.mtpRoutes))
	for key := range routes.derived {
		destinationsByRoute[key.mtpRoute] = append(destinationsByRoute[key.mtpRoute], key)
	}

	associationsByRoute := make(map[MTPRouteID][]AssociationID, len(routes.config.mtpRoutes))
	for association, eligible := range routes.associationEligibleRoutes {
		id := association.ID()
		if id == 0 {
			continue
		}
		for mtpRoute := range eligible {
			associationsByRoute[mtpRoute] = append(associationsByRoute[mtpRoute], id)
		}
	}
	for mtpRoute := range associationsByRoute {
		associations := associationsByRoute[mtpRoute]
		sort.Slice(associations, func(i, j int) bool { return associations[i] < associations[j] })
	}

	statuses := make([]MTPRouteStatus, 0, len(routes.config.mtpRoutes))
	for _, route := range routes.config.mtpRoutes {
		associations := associationsByRoute[route.id]
		if associations == nil {
			associations = make([]AssociationID, 0)
		}
		statuses = append(statuses, MTPRouteStatus{
			MTPRoute:     route.id,
			Destinations: sortedDestinationStatuses(routes.derived, destinationsByRoute[route.id]),
			Associations: associations,
		})
	}
	return statuses
}

// DestinationStatus returns an SGP destination's state in the exact Network
// Appearance and Routing Context scope requested, composed from the newest
// record covering that range in each dimension.
//
// The two dimensions resolve independently, so they need not come from the same
// record — a SCON naming one point code does not hide the DUNA a broader range
// carries — and the key returned is therefore the range that was asked about
// rather than any one record's. DestinationStatuses enumerates the records
// themselves.
func (e *Endpoint) DestinationStatus(key DestinationStatusKey) (DestinationStatusSnapshot, bool) {
	if e == nil || e.role != RoleSGP || e.destinations == nil ||
		key.PointCode > 0x00ffffff || key.Mask > 24 {
		return DestinationStatusSnapshot{}, false
	}
	scope := destinationKey{
		networkAppearance:    key.NetworkAppearance,
		networkAppearanceSet: key.NetworkAppearanceSet,
		routingContext:       key.RoutingContext,
		routingContextSet:    key.RoutingContextSet,
	}
	state, ok := e.destinations.lookupRange(scope, key.PointCode, key.Mask)
	if !ok {
		return DestinationStatusSnapshot{}, false
	}
	return DestinationStatusSnapshot{Key: key, State: state}, true
}

// DestinationStatuses returns every retained SGP destination record in
// deterministic traffic-scope and point-code order, each with both dimensions
// resolved from the records sharing its exact key.
func (e *Endpoint) DestinationStatuses() []DestinationStatusSnapshot {
	if e == nil || e.role != RoleSGP || e.destinations == nil {
		return nil
	}
	e.destinations.mu.RLock()
	records := make([]destinationRecord, 0, len(e.destinations.state))
	for _, record := range e.destinations.state {
		record.routingContexts = append([]uint32(nil), record.routingContexts...)
		records = append(records, record)
	}
	e.destinations.mu.RUnlock()

	// Availability and congestion are separate statements (RFC 4666 Section
	// 4.5.2.2) and are retained in separate records, so a key's snapshot takes
	// the newest record for each dimension rather than the newest record.
	type sequencedStatus struct {
		status          DestinationStatusSnapshot
		availabilitySeq uint64
		congestionSeq   uint64
	}
	latest := make(map[DestinationStatusKey]sequencedStatus, len(records))
	keepNewest := func(rangeValue destinationRange, dimensions destinationDimensions, sequence uint64) {
		key := destinationStatusKeyFromRange(rangeValue)
		current := latest[key]
		current.status.Key = key
		if dimensions.carries(destinationAvailabilityDimension) && sequence > current.availabilitySeq {
			current.availabilitySeq = sequence
			current.status.State.Availability = rangeValue.State.Availability
		}
		if dimensions.carries(destinationCongestionDimension) && sequence > current.congestionSeq {
			current.congestionSeq = sequence
			current.status.State.Congestion = rangeValue.State.Congestion
		}
		latest[key] = current
	}
	for _, record := range records {
		if len(record.routingContexts) == 0 {
			keepNewest(record.rangeValue, record.dimensions, record.sequence)
			continue
		}
		for _, routingContext := range record.routingContexts {
			rangeValue := record.rangeValue
			rangeValue.RoutingContext = routingContext
			rangeValue.RoutingContextSet = true
			keepNewest(rangeValue, record.dimensions, record.sequence)
		}
	}
	statuses := make([]DestinationStatusSnapshot, 0, len(latest))
	for _, retained := range latest {
		statuses = append(statuses, retained.status)
	}
	sort.Slice(statuses, func(i, j int) bool {
		return compareDestinationStatusKey(statuses[i].Key, statuses[j].Key) < 0
	})
	return statuses
}

func destinationStatusKeyFromRange(rangeValue destinationRange) DestinationStatusKey {
	return DestinationStatusKey{
		NetworkAppearance:    rangeValue.NetworkAppearance,
		NetworkAppearanceSet: rangeValue.NetworkAppearanceSet,
		RoutingContext:       rangeValue.RoutingContext,
		RoutingContextSet:    rangeValue.RoutingContextSet,
		PointCode:            rangeValue.PointCode,
		Mask:                 rangeValue.Mask,
	}
}

func compareDestinationStatusKey(first, second DestinationStatusKey) int {
	firstAS := ASKey{
		NetworkAppearance:    first.NetworkAppearance,
		NetworkAppearanceSet: first.NetworkAppearanceSet,
		RoutingContext:       first.RoutingContext,
		RoutingContextSet:    first.RoutingContextSet,
	}
	secondAS := ASKey{
		NetworkAppearance:    second.NetworkAppearance,
		NetworkAppearanceSet: second.NetworkAppearanceSet,
		RoutingContext:       second.RoutingContext,
		RoutingContextSet:    second.RoutingContextSet,
	}
	if comparison := compareASKey(firstAS, secondAS); comparison != 0 {
		return comparison
	}
	if first.PointCode < second.PointCode {
		return -1
	}
	if first.PointCode > second.PointCode {
		return 1
	}
	if first.Mask < second.Mask {
		return -1
	}
	if first.Mask > second.Mask {
		return 1
	}
	return 0
}
