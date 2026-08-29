// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"container/list"
	"fmt"
	"sort"
	"sync"
)

type aspRouteRangeKey struct {
	signallingGateway SignallingGatewayID
	mtpRoute          MTPRouteID
	pointCode         uint32
	mask              uint8
}

type aspRouteStateBudgetKey struct {
	signallingGateway SignallingGatewayID
	mtpRoute          MTPRouteID
}

type aspAvailabilityRecord struct {
	availability DestinationState
	sequence     uint64
}

type aspCongestionRecord struct {
	congested bool
	level     uint8
	levelSet  bool
	sequence  uint64
}

type aspDestinationStatus struct {
	availability       DestinationState
	congested          bool
	congestionLevel    uint8
	congestionLevelSet bool
}

type aspRouteUpdateKind uint8

const (
	aspRouteAvailabilityUpdate aspRouteUpdateKind = iota + 1
	aspRouteCongestionUpdate
)

type aspRouteUpdate struct {
	kind               aspRouteUpdateKind
	availability       DestinationState
	congested          bool
	congestionLevel    uint8
	congestionLevelSet bool
}

type aspDerivedRangeKey struct {
	mtpRoute  MTPRouteID
	pointCode uint32
	mask      uint8
}

type aspDerivedStatusNode struct {
	children  [2]int
	status    aspDestinationStatus
	statusSet bool
}

type aspDerivedStatusTree struct {
	nodes []aspDerivedStatusNode
}

type aspRouteStateIndexNode struct {
	children     [2]*aspRouteStateIndexNode
	availability map[SignallingGatewayID]aspAvailabilityRecord
	congestion   map[SignallingGatewayID]aspCongestionRecord
}

type aspGatewayRouteSnapshot struct {
	id      SignallingGatewayID
	capable bool
}

type aspTransferFlowLock struct {
	mu         sync.Mutex
	references int
}

// aspRoutes owns ASP-wide route state. RFC 4666 Section 4.5.2.2 scopes SSNM
// updates to the originating SG, while Section 1.3.2.5 requires the ASP to
// derive one destination state from all such routes.
type aspRoutes struct {
	mu sync.RWMutex

	config aspRoutingConfig

	associations              map[*Association]SGPIdentity
	associationsBySGP         map[SGPIdentity]map[*Association]struct{}
	associationEligibleRoutes map[*Association]map[MTPRouteID]struct{}
	associationOrder          map[*Association]uint64
	nextAssociationOrder      uint64
	availability              map[aspRouteRangeKey]aspAvailabilityRecord
	congestion                map[aspRouteRangeKey]aspCongestionRecord
	// stateIndex mirrors the bounded records above in point-code prefix form,
	// avoiding a full record scan for every derived leaf on each SSNM update.
	stateIndex                       map[MTPRouteID]*aspRouteStateIndexNode
	stateRecordsPerRoute             map[aspRouteStateBudgetKey]int
	stateRecordsPerSignallingGateway map[SignallingGatewayID]int
	stateRecordCount                 int
	derived                          map[aspDerivedRangeKey]aspDestinationStatus
	sequence                         uint64
	transferRouteGeneration          map[MTPRouteID]uint64
	transferFlows                    map[aspTransferFlowKey]*list.Element
	transferFlowLRU                  *list.List
	// transferSequences serializes concurrent MTP-TRANSFER requests sharing
	// one RFC 4666 Section 3.3.1 traffic flow. In broadcast mode this ensures
	// every selected SGP observes the same order, as Appendix A.2.2 requires
	// when minimizing missequencing.
	transferSequenceMu sync.Mutex
	transferSequences  map[aspTransferFlowKey]*aspTransferFlowLock

	indicationMu      sync.Mutex
	indications       chan *MTPIndication
	indicationsClosed bool
	resyncPending     bool
}

func newASPRoutes(config *ASPConfig) (*aspRoutes, error) {
	snapshot, err := snapshotASPConfig(config)
	if err != nil {
		return nil, err
	}
	queueSize := snapshot.mtpIndicationQueueSize
	if queueSize <= 0 {
		queueSize = DefaultMTPIndicationQueueSize
	}
	routes := &aspRoutes{
		config:                           snapshot,
		associations:                     make(map[*Association]SGPIdentity),
		associationsBySGP:                make(map[SGPIdentity]map[*Association]struct{}),
		associationEligibleRoutes:        make(map[*Association]map[MTPRouteID]struct{}),
		associationOrder:                 make(map[*Association]uint64),
		availability:                     make(map[aspRouteRangeKey]aspAvailabilityRecord),
		congestion:                       make(map[aspRouteRangeKey]aspCongestionRecord),
		stateIndex:                       make(map[MTPRouteID]*aspRouteStateIndexNode, len(snapshot.mtpRoutes)),
		stateRecordsPerRoute:             make(map[aspRouteStateBudgetKey]int),
		stateRecordsPerSignallingGateway: make(map[SignallingGatewayID]int),
		derived:                          make(map[aspDerivedRangeKey]aspDestinationStatus),
		transferRouteGeneration:          make(map[MTPRouteID]uint64, len(snapshot.mtpRoutes)),
		transferFlows:                    make(map[aspTransferFlowKey]*list.Element),
		transferFlowLRU:                  list.New(),
		transferSequences:                make(map[aspTransferFlowKey]*aspTransferFlowLock),
		indications:                      make(chan *MTPIndication, queueSize),
	}
	for _, mtpRoute := range snapshot.mtpRoutes {
		routes.stateIndex[mtpRoute.id] = &aspRouteStateIndexNode{}
		routes.derived[aspDerivedRangeKey{
			mtpRoute:  mtpRoute.id,
			pointCode: mtpRoute.destinationPointCode,
			mask:      mtpRoute.mask,
		}] = aspDestinationStatus{availability: DestinationUnavailable}
	}
	return routes, nil
}

func (r *aspRoutes) attach(association *Association) bool {
	if r == nil || association == nil {
		return false
	}
	// An ASP Endpoint without routing policy remains usable for the existing
	// single-Association APIs. It owns no Endpoint-level route state.
	if len(r.config.sgpByIdentity) == 0 {
		return true
	}
	if association.cfg == nil || association.cfg.PeerSGP == nil {
		return false
	}
	identity := *association.cfg.PeerSGP
	if _, exists := r.config.sgpByIdentity[identity]; !exists {
		return false
	}

	r.mu.Lock()
	r.associations[association] = identity
	r.nextAssociationOrder++
	r.associationOrder[association] = r.nextAssociationOrder
	if r.associationsBySGP[identity] == nil {
		r.associationsBySGP[identity] = make(map[*Association]struct{})
	}
	r.associationsBySGP[identity][association] = struct{}{}
	eligibleRoutes := r.eligibleAssociationRoutesLocked(association, identity)
	r.associationEligibleRoutes[association] = eligibleRoutes
	indications := r.recomputeLocked(eligibleRoutes)
	r.publish(indications)
	r.mu.Unlock()
	return true
}

func (r *aspRoutes) detach(association *Association) {
	if r == nil || association == nil {
		return
	}
	r.mu.Lock()
	identity, exists := r.associations[association]
	affectedMTPRoutes := r.associationEligibleRoutes[association]
	if exists {
		delete(r.associations, association)
		delete(r.associationEligibleRoutes, association)
		delete(r.associationOrder, association)
		r.invalidateAssociationTransferFlowsLocked(association)
		delete(r.associationsBySGP[identity], association)
		if len(r.associationsBySGP[identity]) == 0 {
			delete(r.associationsBySGP, identity)
		}
		if !r.signallingGatewayAttachedLocked(identity.SignallingGateway) {
			if affectedMTPRoutes == nil {
				affectedMTPRoutes = make(map[MTPRouteID]struct{})
			}
			for _, gateway := range r.config.signallingGateways {
				if gateway.id != identity.SignallingGateway {
					continue
				}
				for _, sgp := range gateway.sgps {
					for _, route := range sgp.routes {
						affectedMTPRoutes[route.mtpRoute] = struct{}{}
					}
				}
				break
			}
			r.reclaimSignallingGatewayStateLocked(identity.SignallingGateway)
		}
	}
	indications := r.recomputeLocked(affectedMTPRoutes)
	r.publish(indications)
	r.mu.Unlock()
}

func (r *aspRoutes) eligibleAssociationRoutesLocked(
	association *Association,
	identity SGPIdentity,
) map[MTPRouteID]struct{} {
	eligible := make(map[MTPRouteID]struct{})
	sgp, exists := r.config.sgpByIdentity[identity]
	if !exists {
		return eligible
	}
	for _, route := range sgp.routes {
		if aspAssociationEligibleForAS(association, route.as) {
			eligible[route.mtpRoute] = struct{}{}
		}
	}
	return eligible
}

func changedASPAssociationRoutes(
	previous map[MTPRouteID]struct{},
	current map[MTPRouteID]struct{},
) map[MTPRouteID]struct{} {
	changed := make(map[MTPRouteID]struct{})
	for mtpRoute := range previous {
		if _, exists := current[mtpRoute]; !exists {
			changed[mtpRoute] = struct{}{}
		}
	}
	for mtpRoute := range current {
		if _, exists := previous[mtpRoute]; !exists {
			changed[mtpRoute] = struct{}{}
		}
	}
	return changed
}

func (r *aspRoutes) signallingGatewayAttachedLocked(signallingGateway SignallingGatewayID) bool {
	for identity, associations := range r.associationsBySGP {
		if identity.SignallingGateway == signallingGateway && len(associations) > 0 {
			return true
		}
	}
	return false
}

func (r *aspRoutes) reclaimSignallingGatewayStateLocked(signallingGateway SignallingGatewayID) {
	for key := range r.availability {
		if key.signallingGateway == signallingGateway {
			delete(r.availability, key)
		}
	}
	for key := range r.congestion {
		if key.signallingGateway == signallingGateway {
			delete(r.congestion, key)
		}
	}
	r.rebuildRouteStateAccountingLocked()
	r.rebuildRouteStateIndexLocked()
}

func (r *aspRoutes) rebuildRouteStateAccountingLocked() {
	r.stateRecordsPerRoute = make(map[aspRouteStateBudgetKey]int)
	r.stateRecordsPerSignallingGateway = make(map[SignallingGatewayID]int)
	r.stateRecordCount = 0
	for key := range r.availability {
		r.recordRouteStateAccountingLocked(key)
	}
	for key := range r.congestion {
		r.recordRouteStateAccountingLocked(key)
	}
}

func (r *aspRoutes) recordRouteStateAccountingLocked(key aspRouteRangeKey) {
	budgetKey := aspRouteStateBudgetKey{
		signallingGateway: key.signallingGateway,
		mtpRoute:          key.mtpRoute,
	}
	r.stateRecordsPerRoute[budgetKey]++
	r.stateRecordsPerSignallingGateway[key.signallingGateway]++
	r.stateRecordCount++
}

func (r *aspRoutes) rebuildRouteStateIndexLocked() {
	r.stateIndex = make(map[MTPRouteID]*aspRouteStateIndexNode, len(r.config.mtpRoutes))
	for _, mtpRoute := range r.config.mtpRoutes {
		r.stateIndex[mtpRoute.id] = &aspRouteStateIndexNode{}
	}
	for key, record := range r.availability {
		r.indexAvailabilityRecordLocked(key, record)
	}
	for key, record := range r.congestion {
		r.indexCongestionRecordLocked(key, record)
	}
}

func (r *aspRoutes) apply(
	association *Association,
	statuses []*DestinationStatus,
	update aspRouteUpdate,
) error {
	if r == nil || association == nil || len(statuses) == 0 {
		return nil
	}
	r.mu.Lock()
	identity, exists := r.associations[association]
	if !exists {
		r.mu.Unlock()
		return nil
	}
	sgp, exists := r.config.sgpByIdentity[identity]
	if !exists {
		r.mu.Unlock()
		return nil
	}
	if len(statuses) > r.config.maxAffectedPointCodesPerSSNM {
		r.mu.Unlock()
		return fmt.Errorf("%w: SSNM carries %d Affected Point Codes, limit %d",
			ErrASPRouteStateLimit, len(statuses), r.config.maxAffectedPointCodesPerSSNM)
	}
	pendingKeys := make([]aspRouteRangeKey, 0, len(statuses))
	pendingSet := make(map[aspRouteRangeKey]struct{}, len(statuses))
	newRecordsPerRoute := make(map[aspRouteStateBudgetKey]int)
	newRecordsPerSignallingGateway := make(map[SignallingGatewayID]int)
	newRecordCount := 0
	affectedMTPRoutes := make(map[MTPRouteID]struct{})

	for _, status := range statuses {
		if status == nil || status.UserPartUnavailable {
			continue
		}
		for _, route := range sgp.routes {
			if !aspRouteASMatchesStatus(association, route.as, status) {
				continue
			}
			mtpRoute, ok := r.mtpRoute(route.mtpRoute)
			if !ok {
				continue
			}
			pointCode, mask, overlaps := aspRouteIntersection(mtpRoute, status.PointCode, status.Mask)
			if !overlaps {
				continue
			}
			key := aspRouteRangeKey{
				signallingGateway: identity.SignallingGateway,
				mtpRoute:          route.mtpRoute,
				pointCode:         pointCode,
				mask:              mask,
			}
			if _, duplicate := pendingSet[key]; duplicate {
				continue
			}
			pendingSet[key] = struct{}{}
			if !r.hasRouteStateRecordLocked(key, update.kind) {
				budgetKey := aspRouteStateBudgetKey{
					signallingGateway: key.signallingGateway,
					mtpRoute:          key.mtpRoute,
				}
				if r.stateRecordsPerRoute[budgetKey]+newRecordsPerRoute[budgetKey]+1 >
					r.config.maxSSNMStateRecordsPerRoute {
					r.mu.Unlock()
					return fmt.Errorf("%w: SG %q MTP Route %q would exceed the %d-record limit",
						ErrASPRouteStateLimit,
						budgetKey.signallingGateway,
						budgetKey.mtpRoute,
						r.config.maxSSNMStateRecordsPerRoute)
				}
				if r.stateRecordsPerSignallingGateway[budgetKey.signallingGateway]+
					newRecordsPerSignallingGateway[budgetKey.signallingGateway]+1 >
					r.config.maxSSNMStateRecordsPerSignallingGateway {
					r.mu.Unlock()
					return fmt.Errorf("%w: SG %q would exceed the %d-record limit",
						ErrASPRouteStateLimit,
						budgetKey.signallingGateway,
						r.config.maxSSNMStateRecordsPerSignallingGateway)
				}
				if r.stateRecordCount+newRecordCount+1 > r.config.maxSSNMStateRecords {
					r.mu.Unlock()
					return fmt.Errorf("%w: Endpoint would exceed the %d-record limit",
						ErrASPRouteStateLimit, r.config.maxSSNMStateRecords)
				}
				newRecordsPerRoute[budgetKey]++
				newRecordsPerSignallingGateway[budgetKey.signallingGateway]++
				newRecordCount++
			}
			pendingKeys = append(pendingKeys, key)
			affectedMTPRoutes[route.mtpRoute] = struct{}{}
		}
	}

	for _, key := range pendingKeys {
		newRecord := !r.hasRouteStateRecordLocked(key, update.kind)
		r.sequence++
		if r.sequence == 0 {
			r.renumberLocked()
			r.sequence++
		}
		switch update.kind {
		case aspRouteAvailabilityUpdate:
			record := aspAvailabilityRecord{
				availability: update.availability,
				sequence:     r.sequence,
			}
			r.availability[key] = record
			r.indexAvailabilityRecordLocked(key, record)
		case aspRouteCongestionUpdate:
			record := aspCongestionRecord{
				congested: update.congested,
				level:     update.congestionLevel,
				levelSet:  update.congestionLevelSet,
				sequence:  r.sequence,
			}
			r.congestion[key] = record
			r.indexCongestionRecordLocked(key, record)
		}
		if newRecord {
			budgetKey := aspRouteStateBudgetKey{
				signallingGateway: key.signallingGateway,
				mtpRoute:          key.mtpRoute,
			}
			r.stateRecordsPerRoute[budgetKey]++
			r.stateRecordsPerSignallingGateway[budgetKey.signallingGateway]++
			r.stateRecordCount++
		}
	}
	indications := r.recomputeLocked(affectedMTPRoutes)
	r.publish(indications)
	r.mu.Unlock()
	return nil
}

func (r *aspRoutes) hasRouteStateRecordLocked(key aspRouteRangeKey, kind aspRouteUpdateKind) bool {
	switch kind {
	case aspRouteAvailabilityUpdate:
		_, exists := r.availability[key]
		return exists
	case aspRouteCongestionUpdate:
		_, exists := r.congestion[key]
		return exists
	default:
		return false
	}
}

func (r *aspRoutes) indexAvailabilityRecordLocked(key aspRouteRangeKey, record aspAvailabilityRecord) {
	node := r.stateIndexNodeForRangeLocked(key)
	if node == nil {
		return
	}
	if node.availability == nil {
		node.availability = make(map[SignallingGatewayID]aspAvailabilityRecord)
	}
	node.availability[key.signallingGateway] = record
}

func (r *aspRoutes) indexCongestionRecordLocked(key aspRouteRangeKey, record aspCongestionRecord) {
	node := r.stateIndexNodeForRangeLocked(key)
	if node == nil {
		return
	}
	if node.congestion == nil {
		node.congestion = make(map[SignallingGatewayID]aspCongestionRecord)
	}
	node.congestion[key.signallingGateway] = record
}

func (r *aspRoutes) stateIndexNodeForRangeLocked(key aspRouteRangeKey) *aspRouteStateIndexNode {
	mtpRoute, exists := r.mtpRoute(key.mtpRoute)
	if !exists {
		return nil
	}
	root := r.stateIndex[key.mtpRoute]
	if root == nil {
		root = &aspRouteStateIndexNode{}
		r.stateIndex[key.mtpRoute] = root
	}
	return aspRouteStateNodeForRange(root, mtpRoute.mask, key.pointCode, key.mask)
}

func (r *aspRoutes) associationStateChanged(association *Association) bool {
	if r == nil || association == nil {
		return false
	}
	r.mu.Lock()
	identity, exists := r.associations[association]
	if !exists {
		r.mu.Unlock()
		return false
	}
	current := r.eligibleAssociationRoutesLocked(association, identity)
	affectedMTPRoutes := changedASPAssociationRoutes(r.associationEligibleRoutes[association], current)
	if len(affectedMTPRoutes) == 0 {
		r.mu.Unlock()
		return false
	}
	r.associationEligibleRoutes[association] = current
	indications := r.recomputeLocked(affectedMTPRoutes)
	r.publish(indications)
	r.mu.Unlock()
	return true
}

func (r *aspRoutes) renumberLocked() {
	sequence := uint64(0)
	availabilityKeys := make([]aspRouteRangeKey, 0, len(r.availability))
	for key := range r.availability {
		availabilityKeys = append(availabilityKeys, key)
	}
	sort.Slice(availabilityKeys, func(first, second int) bool {
		firstRecord := r.availability[availabilityKeys[first]]
		secondRecord := r.availability[availabilityKeys[second]]
		if firstRecord.sequence != secondRecord.sequence {
			return firstRecord.sequence < secondRecord.sequence
		}
		return lessASPRouteRangeKey(availabilityKeys[first], availabilityKeys[second])
	})
	for _, key := range availabilityKeys {
		record := r.availability[key]
		sequence++
		record.sequence = sequence
		r.availability[key] = record
		r.indexAvailabilityRecordLocked(key, record)
	}
	congestionKeys := make([]aspRouteRangeKey, 0, len(r.congestion))
	for key := range r.congestion {
		congestionKeys = append(congestionKeys, key)
	}
	sort.Slice(congestionKeys, func(first, second int) bool {
		firstRecord := r.congestion[congestionKeys[first]]
		secondRecord := r.congestion[congestionKeys[second]]
		if firstRecord.sequence != secondRecord.sequence {
			return firstRecord.sequence < secondRecord.sequence
		}
		return lessASPRouteRangeKey(congestionKeys[first], congestionKeys[second])
	})
	for _, key := range congestionKeys {
		record := r.congestion[key]
		sequence++
		record.sequence = sequence
		r.congestion[key] = record
		r.indexCongestionRecordLocked(key, record)
	}
	r.sequence = sequence
}

func lessASPRouteRangeKey(first, second aspRouteRangeKey) bool {
	if first.signallingGateway != second.signallingGateway {
		return first.signallingGateway < second.signallingGateway
	}
	if first.mtpRoute != second.mtpRoute {
		return first.mtpRoute < second.mtpRoute
	}
	if first.pointCode != second.pointCode {
		return first.pointCode < second.pointCode
	}
	return first.mask < second.mask
}

func (r *aspRoutes) mtpRoute(id MTPRouteID) (aspMTPRoute, bool) {
	index, exists := r.config.mtpRouteByID[id]
	if !exists || index < 0 || index >= len(r.config.mtpRoutes) {
		return aspMTPRoute{}, false
	}
	return r.config.mtpRoutes[index], true
}

func (r *aspRoutes) validateAssociationConfig(config *AssociationConfig) error {
	if r == nil || len(r.config.sgpByIdentity) == 0 {
		return nil
	}
	if config == nil || config.PeerSGP == nil {
		return ErrMissingSGPIdentity
	}
	sgp, exists := r.config.sgpByIdentity[*config.PeerSGP]
	if !exists {
		return ErrUnknownSGP
	}
	configuredKeys := associationConfigASKeys(config)
	if len(configuredKeys) == 0 {
		return ErrSGPRouteScopeMismatch
	}
	for _, configuredKey := range configuredKeys {
		matched := false
		for _, route := range sgp.routes {
			if route.as == configuredKey {
				matched = true
				break
			}
		}
		if !matched {
			return ErrSGPRouteScopeMismatch
		}
	}
	return nil
}

func associationConfigASKeys(config *AssociationConfig) []ASKey {
	if config == nil || config.RoutingContexts == nil || len(config.RoutingContexts.RoutingContexts()) == 0 {
		return []ASKey{contextlessASKeyForConfig(config)}
	}
	routingContexts := config.RoutingContexts.RoutingContexts()
	keys := make([]ASKey, 0, len(routingContexts))
	for _, routingContext := range routingContexts {
		keys = append(keys, asKeyForConfigRoutingContext(config, routingContext))
	}
	return keys
}

func (r *aspRoutes) destinationStatus(mtpRouteID MTPRouteID, pointCode uint32, mask uint8) (aspDestinationStatus, bool) {
	if r == nil {
		return aspDestinationStatus{}, false
	}
	mtpRoute, exists := r.mtpRoute(mtpRouteID)
	if !exists || !aspMTPRouteCoversRange(mtpRoute, pointCode, mask) {
		return aspDestinationStatus{}, false
	}

	r.mu.RLock()
	defer r.mu.RUnlock()
	var result aspDestinationStatus
	found := false
	for key, status := range r.derived {
		if key.mtpRoute != mtpRouteID || !aspRangesOverlap(key.pointCode, key.mask, pointCode, mask) {
			continue
		}
		if found && status != result {
			return aspDestinationStatus{}, false
		}
		result = status
		found = true
	}
	return result, found
}

func (r *aspRoutes) mtpDestinationStatus(destination MTPDestination) (MTPDestinationStatus, bool) {
	if destination.PointCode > 0xffffff || destination.Mask > 24 ||
		destination.PointCode&lowPointCodeBits(destination.Mask) != 0 {
		return MTPDestinationStatus{}, false
	}
	status, known := r.destinationStatus(destination.MTPRoute, destination.PointCode, destination.Mask)
	if !known {
		return MTPDestinationStatus{}, false
	}
	return newMTPDestinationStatus(destination, status), true
}

func (r *aspRoutes) mtpDestinationStatuses() []MTPDestinationStatus {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()

	statuses := make([]MTPDestinationStatus, 0, len(r.derived))
	for _, mtpRoute := range r.config.mtpRoutes {
		keys := make([]aspDerivedRangeKey, 0)
		for key := range r.derived {
			if key.mtpRoute == mtpRoute.id {
				keys = append(keys, key)
			}
		}
		sort.Slice(keys, func(first, second int) bool {
			if keys[first].pointCode != keys[second].pointCode {
				return keys[first].pointCode < keys[second].pointCode
			}
			return keys[first].mask < keys[second].mask
		})
		for _, key := range keys {
			statuses = append(statuses, newMTPDestinationStatus(MTPDestination{
				MTPRoute:  key.mtpRoute,
				PointCode: key.pointCode,
				Mask:      key.mask,
			}, r.derived[key]))
		}
	}
	return statuses
}

func (r *aspRoutes) recomputeLocked(only map[MTPRouteID]struct{}) []*MTPIndication {
	var indications []*MTPIndication
	for _, mtpRoute := range r.config.mtpRoutes {
		if only != nil {
			if _, exists := only[mtpRoute.id]; !exists {
				continue
			}
		}
		r.advanceTransferRouteGenerationLocked(mtpRoute.id)
		updated := r.recomputeMTPRouteLocked(mtpRoute)
		routeIndications := r.derivedStatusIndicationsLocked(mtpRoute, updated)
		indications = append(indications, routeIndications...)
		for key := range r.derived {
			if key.mtpRoute == mtpRoute.id {
				delete(r.derived, key)
			}
		}
		for key, status := range updated {
			r.derived[key] = status
		}
	}
	return indications
}

func (r *aspRoutes) recomputeMTPRouteLocked(
	mtpRoute aspMTPRoute,
) map[aspDerivedRangeKey]aspDestinationStatus {
	gateways := r.gatewayRouteSnapshotsLocked(mtpRoute.id)
	updated := make(map[aspDerivedRangeKey]aspDestinationStatus)
	if len(gateways) == 0 {
		return updated
	}
	root := r.stateIndex[mtpRoute.id]
	if root == nil {
		root = &aspRouteStateIndexNode{}
	}

	r.appendIndexedDerivedLeavesLocked(
		mtpRoute,
		root,
		mtpRoute.destinationPointCode,
		mtpRoute.mask,
		gateways,
		make([]*aspRouteStateIndexNode, 0, int(mtpRoute.mask)+1),
		updated,
	)
	return updated
}

func (r *aspRoutes) gatewayRouteSnapshotsLocked(
	mtpRoute MTPRouteID,
) []aspGatewayRouteSnapshot {
	snapshots := make([]aspGatewayRouteSnapshot, 0, len(r.config.signallingGateways))
	for _, gateway := range r.config.signallingGateways {
		configured, capable := r.signallingGatewayRouteCapabilityLocked(gateway, mtpRoute)
		if !configured {
			continue
		}
		snapshots = append(snapshots, aspGatewayRouteSnapshot{id: gateway.id, capable: capable})
	}
	return snapshots
}

func aspRouteStateNodeForRange(
	root *aspRouteStateIndexNode,
	rootMask uint8,
	pointCode uint32,
	mask uint8,
) *aspRouteStateIndexNode {
	node := root
	for currentMask := rootMask; currentMask > mask; currentMask-- {
		branch := int(pointCode >> (currentMask - 1) & 1)
		if node.children[branch] == nil {
			node.children[branch] = &aspRouteStateIndexNode{}
		}
		node = node.children[branch]
	}
	return node
}

func (r *aspRoutes) appendIndexedDerivedLeavesLocked(
	mtpRoute aspMTPRoute,
	node *aspRouteStateIndexNode,
	pointCode uint32,
	mask uint8,
	gateways []aspGatewayRouteSnapshot,
	path []*aspRouteStateIndexNode,
	updated map[aspDerivedRangeKey]aspDestinationStatus,
) {
	path = append(path, node)

	if mask == 0 || node.children[0] == nil && node.children[1] == nil {
		r.appendIndexedDerivedLeafLocked(mtpRoute, pointCode, mask, gateways, path, updated)
		return
	}
	childMask := mask - 1
	for branch := 0; branch < 2; branch++ {
		childPointCode := pointCode | uint32(branch)<<childMask
		child := node.children[branch]
		if child == nil {
			r.appendIndexedDerivedLeafLocked(
				mtpRoute, childPointCode, childMask, gateways, path, updated,
			)
			continue
		}
		r.appendIndexedDerivedLeavesLocked(
			mtpRoute, child, childPointCode, childMask, gateways, path, updated,
		)
	}
}

func (r *aspRoutes) appendIndexedDerivedLeafLocked(
	mtpRoute aspMTPRoute,
	pointCode uint32,
	mask uint8,
	gateways []aspGatewayRouteSnapshot,
	path []*aspRouteStateIndexNode,
	updated map[aspDerivedRangeKey]aspDestinationStatus,
) {
	current := aspDestinationStatusFromGatewayPath(gateways, path)
	key := aspDerivedRangeKey{mtpRoute: mtpRoute.id, pointCode: pointCode, mask: mask}
	updated[key] = current
}

func aspDestinationStatusFromGatewayPath(
	gateways []aspGatewayRouteSnapshot,
	path []*aspRouteStateIndexNode,
) aspDestinationStatus {
	bestSet := false
	best := aspDestinationStatus{availability: DestinationUnavailable}
	for _, gateway := range gateways {
		status := aspDestinationStatus{availability: DestinationUnavailable}
		if gateway.capable {
			status.availability = DestinationAvailable
			var availability aspAvailabilityRecord
			availabilitySet := false
			var congestion aspCongestionRecord
			congestionSet := false
			for _, node := range path {
				if record, exists := node.availability[gateway.id]; exists &&
					(!availabilitySet || record.sequence > availability.sequence) {
					availability = record
					availabilitySet = true
				}
				if record, exists := node.congestion[gateway.id]; exists &&
					(!congestionSet || record.sequence > congestion.sequence) {
					congestion = record
					congestionSet = true
				}
			}
			if availabilitySet {
				status.availability = availability.availability
			}
			if congestionSet {
				status.congested = congestion.congested
				status.congestionLevel = congestion.level
				status.congestionLevelSet = congestion.levelSet
			}
		}
		if !bestSet || aspAvailabilityRank(status.availability) < aspAvailabilityRank(best.availability) {
			best = status
			bestSet = true
			continue
		}
		if status.availability == best.availability {
			best = leastCongestedASPStatus(best, status)
		}
	}
	return best
}

func (r *aspRoutes) derivedStatusIndicationsLocked(
	mtpRoute aspMTPRoute,
	updated map[aspDerivedRangeKey]aspDestinationStatus,
) []*MTPIndication {
	previousTree := derivedStatusTree(mtpRoute, r.derived)
	currentTree := derivedStatusTree(mtpRoute, updated)
	indications := make([]*MTPIndication, 0)
	appendDerivedStatusIndications(
		mtpRoute.id,
		mtpRoute.destinationPointCode,
		mtpRoute.mask,
		&previousTree,
		1,
		&currentTree,
		1,
		aspDestinationStatus{},
		false,
		aspDestinationStatus{},
		false,
		&indications,
	)
	return indications
}

func derivedStatusTree(
	mtpRoute aspMTPRoute,
	statuses map[aspDerivedRangeKey]aspDestinationStatus,
) aspDerivedStatusTree {
	entryCount := 0
	for key := range statuses {
		if key.mtpRoute == mtpRoute.id && aspMTPRouteCoversRange(mtpRoute, key.pointCode, key.mask) {
			entryCount++
		}
	}
	tree := aspDerivedStatusTree{
		nodes: make([]aspDerivedStatusNode, 1, 1+entryCount*2),
	}
	for key, status := range statuses {
		if key.mtpRoute != mtpRoute.id || !aspMTPRouteCoversRange(mtpRoute, key.pointCode, key.mask) {
			continue
		}
		node := tree.nodeForRange(mtpRoute.mask, key.pointCode, key.mask)
		tree.nodes[node-1].status = status
		tree.nodes[node-1].statusSet = true
	}
	return tree
}

func (tree *aspDerivedStatusTree) nodeForRange(
	rootMask uint8,
	pointCode uint32,
	mask uint8,
) int {
	node := 1
	for currentMask := rootMask; currentMask > mask; currentMask-- {
		branch := int(pointCode >> (currentMask - 1) & 1)
		child := tree.nodes[node-1].children[branch]
		if child == 0 {
			tree.nodes = append(tree.nodes, aspDerivedStatusNode{})
			child = len(tree.nodes)
			tree.nodes[node-1].children[branch] = child
		}
		node = child
	}
	return node
}

func appendDerivedStatusIndications(
	mtpRoute MTPRouteID,
	pointCode uint32,
	mask uint8,
	previousTree *aspDerivedStatusTree,
	previousNode int,
	currentTree *aspDerivedStatusTree,
	currentNode int,
	previous aspDestinationStatus,
	previousSet bool,
	current aspDestinationStatus,
	currentSet bool,
	indications *[]*MTPIndication,
) {
	if previousNode != 0 && previousTree.nodes[previousNode-1].statusSet {
		previous = previousTree.nodes[previousNode-1].status
		previousSet = true
	}
	if currentNode != 0 && currentTree.nodes[currentNode-1].statusSet {
		current = currentTree.nodes[currentNode-1].status
		currentSet = true
	}
	previousHasChildren := previousTree.nodeHasChildren(previousNode)
	currentHasChildren := currentTree.nodeHasChildren(currentNode)
	if mask == 0 || !previousHasChildren && !currentHasChildren {
		if !previousSet {
			previous = aspDestinationStatus{availability: DestinationUnavailable}
		}
		if !currentSet {
			current = aspDestinationStatus{availability: DestinationUnavailable}
		}
		key := aspDerivedRangeKey{mtpRoute: mtpRoute, pointCode: pointCode, mask: mask}
		if indication := newMTPIndication(key, previous, current); indication != nil {
			*indications = append(*indications, indication)
		}
		return
	}

	childMask := mask - 1
	for branch := 0; branch < 2; branch++ {
		previousChild := 0
		if previousNode != 0 {
			previousChild = previousTree.nodes[previousNode-1].children[branch]
		}
		currentChild := 0
		if currentNode != 0 {
			currentChild = currentTree.nodes[currentNode-1].children[branch]
		}
		appendDerivedStatusIndications(
			mtpRoute,
			pointCode|uint32(branch)<<childMask,
			childMask,
			previousTree,
			previousChild,
			currentTree,
			currentChild,
			previous,
			previousSet,
			current,
			currentSet,
			indications,
		)
	}
}

func (tree *aspDerivedStatusTree) nodeHasChildren(node int) bool {
	if node == 0 {
		return false
	}
	children := tree.nodes[node-1].children
	return children[0] != 0 || children[1] != 0
}

func newMTPIndication(
	key aspDerivedRangeKey,
	previous,
	current aspDestinationStatus,
) *MTPIndication {
	if previous == current {
		return nil
	}
	kind := MTPStatusIndication
	switch {
	case previous.availability != DestinationUnavailable && current.availability == DestinationUnavailable:
		kind = MTPPauseIndication
	case previous.availability == DestinationUnavailable && current.availability != DestinationUnavailable:
		kind = MTPResumeIndication
	}
	return &MTPIndication{
		Kind: kind,
		Destination: newMTPDestinationStatus(MTPDestination{
			MTPRoute:  key.mtpRoute,
			PointCode: key.pointCode,
			Mask:      key.mask,
		}, current),
	}
}

func newMTPDestinationStatus(destination MTPDestination, status aspDestinationStatus) MTPDestinationStatus {
	return MTPDestinationStatus{
		Destination:        destination,
		Availability:       status.availability,
		Congested:          status.congested,
		CongestionLevel:    status.congestionLevel,
		CongestionLevelSet: status.congestionLevelSet,
	}
}

func (r *aspRoutes) publish(indications []*MTPIndication) {
	if r == nil || len(indications) == 0 {
		return
	}
	r.indicationMu.Lock()
	defer r.indicationMu.Unlock()
	for _, indication := range indications {
		r.publishOneLocked(indication)
	}
}

func (r *aspRoutes) publishOne(indication *MTPIndication) {
	if r == nil || indication == nil {
		return
	}
	r.indicationMu.Lock()
	defer r.indicationMu.Unlock()
	r.publishOneLocked(indication)
}

func (r *aspRoutes) publishOneLocked(indication *MTPIndication) {
	if indication == nil || r.indicationsClosed {
		return
	}
	if r.resyncPending {
		if len(r.indications) != 0 {
			return
		}
		r.resyncPending = false
	}
	if len(r.indications) == cap(r.indications) {
		for {
			select {
			case <-r.indications:
				continue
			default:
			}
			break
		}
		r.indications <- &MTPIndication{ResyncRequired: true}
		r.resyncPending = true
		return
	}
	select {
	case r.indications <- indication:
	default:
	}
}

func (r *aspRoutes) closeIndications() {
	if r == nil {
		return
	}
	r.indicationMu.Lock()
	defer r.indicationMu.Unlock()
	if r.indicationsClosed {
		return
	}
	r.indicationsClosed = true
	close(r.indications)
}

func (r *aspRoutes) signallingGatewayStatusLocked(
	gateway aspSignallingGatewayConfig,
	mtpRoute MTPRouteID,
	pointCode uint32,
	mask uint8,
) (aspDestinationStatus, bool) {
	configured, capable := r.signallingGatewayRouteCapabilityLocked(gateway, mtpRoute)
	if !configured {
		return aspDestinationStatus{}, false
	}
	if !capable {
		return aspDestinationStatus{availability: DestinationUnavailable}, true
	}

	status := aspDestinationStatus{availability: DestinationAvailable}
	availability, availabilitySet, congestion, congestionSet := r.indexedRouteStateForRangeLocked(
		gateway.id, mtpRoute, pointCode, mask,
	)
	if availabilitySet {
		status.availability = availability.availability
	}
	if congestionSet {
		status.congested = congestion.congested
		status.congestionLevel = congestion.level
		status.congestionLevelSet = congestion.levelSet
	}
	return status, true
}

func (r *aspRoutes) signallingGatewayRouteCapabilityLocked(
	gateway aspSignallingGatewayConfig,
	mtpRoute MTPRouteID,
) (bool, bool) {
	configured := false
	capable := false
	for _, sgp := range gateway.sgps {
		route, exists := aspSGPRouteForMTPRoute(sgp, mtpRoute)
		if !exists {
			continue
		}
		configured = true
		identity := SGPIdentity{
			SignallingGateway:        gateway.id,
			SignallingGatewayProcess: sgp.id,
		}
		for association := range r.associationsBySGP[identity] {
			if aspAssociationEligibleForAS(association, route.as) {
				capable = true
				break
			}
		}
		if capable {
			break
		}
	}
	return configured, capable
}

func (r *aspRoutes) indexedRouteStateForRangeLocked(
	signallingGateway SignallingGatewayID,
	mtpRouteID MTPRouteID,
	pointCode uint32,
	mask uint8,
) (aspAvailabilityRecord, bool, aspCongestionRecord, bool) {
	mtpRoute, exists := r.mtpRoute(mtpRouteID)
	if !exists || !aspMTPRouteCoversRange(mtpRoute, pointCode, mask) {
		return aspAvailabilityRecord{}, false, aspCongestionRecord{}, false
	}
	node := r.stateIndex[mtpRouteID]
	var availability aspAvailabilityRecord
	availabilitySet := false
	var congestion aspCongestionRecord
	congestionSet := false
	for currentMask := mtpRoute.mask; ; currentMask-- {
		if node == nil {
			break
		}
		if record, found := node.availability[signallingGateway]; found &&
			(!availabilitySet || record.sequence > availability.sequence) {
			availability = record
			availabilitySet = true
		}
		if record, found := node.congestion[signallingGateway]; found &&
			(!congestionSet || record.sequence > congestion.sequence) {
			congestion = record
			congestionSet = true
		}
		if currentMask == mask {
			break
		}
		branch := int(pointCode >> (currentMask - 1) & 1)
		node = node.children[branch]
	}
	return availability, availabilitySet, congestion, congestionSet
}

func aspRouteASMatchesStatus(association *Association, key ASKey, status *DestinationStatus) bool {
	if association == nil || status == nil {
		return false
	}
	networkAppearance := status.NetworkAppearance
	networkAppearanceSet := status.NetworkAppearanceSet
	if !networkAppearanceSet && association.cfg != nil {
		networkAppearance, networkAppearanceSet = appearanceOf(association.cfg.NetworkAppearance)
	}
	if key.NetworkAppearanceSet != networkAppearanceSet || key.NetworkAppearance != networkAppearance {
		return false
	}

	routingContexts := status.RoutingContexts
	routingContextSet := status.RoutingContextSet
	if !routingContextSet {
		routingContexts, routingContextSet = association.destinationRoutingContexts(nil)
	}
	if key.RoutingContextSet != routingContextSet {
		return false
	}
	if !routingContextSet {
		return true
	}
	for _, routingContext := range routingContexts {
		if routingContext == key.RoutingContext {
			return true
		}
	}
	return false
}

func aspRouteIntersection(mtpRoute aspMTPRoute, pointCode uint32, mask uint8) (uint32, uint8, bool) {
	statusMask := effectiveDestinationMask(mask)
	statusPointCode := destinationRangePrefix(pointCode, statusMask)
	if !aspRangesOverlap(
		mtpRoute.destinationPointCode, mtpRoute.mask,
		statusPointCode, statusMask,
	) {
		return 0, 0, false
	}
	if mtpRoute.mask <= statusMask {
		return mtpRoute.destinationPointCode, mtpRoute.mask, true
	}
	return statusPointCode, statusMask, true
}

func aspRangesOverlap(firstPointCode uint32, firstMask uint8, secondPointCode uint32, secondMask uint8) bool {
	return aspRangeCovers(firstPointCode, firstMask, secondPointCode, secondMask) ||
		aspRangeCovers(secondPointCode, secondMask, firstPointCode, firstMask)
}

func aspRangeCovers(storedPointCode uint32, storedMask uint8, pointCode uint32, mask uint8) bool {
	if effectiveDestinationMask(storedMask) < effectiveDestinationMask(mask) {
		return false
	}
	return destinationRangePrefix(storedPointCode, storedMask) == destinationRangePrefix(pointCode, storedMask)
}

func aspMTPRouteCoversRange(mtpRoute aspMTPRoute, pointCode uint32, mask uint8) bool {
	return aspRangeCovers(mtpRoute.destinationPointCode, mtpRoute.mask, pointCode, mask)
}

func aspSGPRouteForMTPRoute(sgp aspSGPConfig, mtpRoute MTPRouteID) (aspSGPRoute, bool) {
	for _, route := range sgp.routes {
		if route.mtpRoute == mtpRoute {
			return route, true
		}
	}
	return aspSGPRoute{}, false
}

func aspAssociationEligibleForAS(association *Association, key ASKey) bool {
	if association == nil || association.State() != StateASPActive {
		return false
	}
	configured := false
	for _, configuredKey := range association.configuredASKeys() {
		if configuredKey == key {
			configured = true
			break
		}
	}
	if !configured {
		return false
	}
	if !key.RoutingContextSet {
		return true
	}
	return association.outboundRoutingContextActive(key.RoutingContext)
}

func aspAvailabilityRank(state DestinationState) int {
	switch state {
	case DestinationAvailable:
		return 0
	case DestinationRestricted:
		return 1
	default:
		return 2
	}
}

func leastCongestedASPStatus(first, second aspDestinationStatus) aspDestinationStatus {
	firstRank := aspCongestionRank(first)
	secondRank := aspCongestionRank(second)
	if secondRank < firstRank {
		return second
	}
	return first
}

func aspCongestionRank(status aspDestinationStatus) int {
	if !status.congested {
		return 0
	}
	if !status.congestionLevelSet {
		return 4
	}
	return int(status.congestionLevel)
}
