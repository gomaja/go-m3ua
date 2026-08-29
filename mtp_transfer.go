// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"container/list"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// MTPTransferRequest is the MTP-TRANSFER request primitive described by RFC
// 4666 Sections 1.6.1 and 5.5.1.1.1. MTPRoute may be omitted when the MTP
// routing label has one unambiguous best match in the ASP routing table.
type MTPTransferRequest struct {
	MTPRoute     MTPRouteID
	ProtocolData *params.ProtocolDataPayload
}

// MTPTransferResult reports successful transfer of the MTP3-User payload.
type MTPTransferResult struct {
	UserDataOctets          int
	TransmittedAssociations int
}

// MTPTransferFailure identifies one selected SGP Association whose DATA write
// failed. A write is never retried automatically because failure does not prove
// that the peer received no DATA.
type MTPTransferFailure struct {
	SGP SGPIdentity
	Err error
}

// MTPTransferError reports a failed or partially failed MTP-TRANSFER request.
// The slices are owned by the error and ordered by configured SG and SGP order.
type MTPTransferError struct {
	SuccessfulSGPs []SGPIdentity
	Failures       []MTPTransferFailure
}

func (e *MTPTransferError) Error() string {
	if e == nil {
		return "MTP-TRANSFER failed"
	}
	return fmt.Sprintf("MTP-TRANSFER succeeded through %d SGPs and failed through %d SGPs",
		len(e.SuccessfulSGPs), len(e.Failures))
}

func (e *MTPTransferError) Unwrap() error {
	if e == nil || len(e.Failures) == 0 {
		return nil
	}
	errorsToJoin := make([]error, 0, len(e.Failures))
	for _, failure := range e.Failures {
		if failure.Err != nil {
			errorsToJoin = append(errorsToJoin, failure.Err)
		}
	}
	return errors.Join(errorsToJoin...)
}

type aspTransferFlowKey struct {
	mtpRoute MTPRouteID
	opc      uint32
	dpc      uint32
	si       uint8
	ni       uint8
	mp       uint8
	sls      uint8
}

type aspTransferTarget struct {
	identity    SGPIdentity
	association *Association
	as          ASKey
	routeStatus aspDestinationStatus
}

type aspTransferAssignment struct {
	key     aspTransferFlowKey
	targets []aspTransferTarget
}

type aspTransferSGP struct {
	identity     SGPIdentity
	as           ASKey
	associations []*Association
}

type aspTransferGateway struct {
	config aspSignallingGatewayConfig
	status aspDestinationStatus
	sgps   []aspTransferSGP
}

type aspCongestionDecision struct {
	enabled      bool
	unknownLevel bool
	levels       [4]bool
}

// MTPTransfer selects the SGP, Association, and SCTP stream for one
// MTP-TRANSFER request as required by RFC 4666 Section 5.5.1.1.1.
func (e *Endpoint) MTPTransfer(request MTPTransferRequest) (MTPTransferResult, error) {
	if e == nil || e.role != RoleASP || e.aspRoutes == nil {
		return MTPTransferResult{}, ErrUnsupportedRole
	}
	if request.ProtocolData == nil {
		return MTPTransferResult{}, ErrMissingProtocolData
	}
	if !e.beginOperation() {
		return MTPTransferResult{}, ErrEndpointClosed
	}
	defer e.endOperation()

	targets, err := e.aspRoutes.selectTransfer(request)
	if err != nil {
		return MTPTransferResult{}, err
	}
	result := MTPTransferResult{}
	successful := make([]SGPIdentity, 0, len(targets))
	failures := make([]MTPTransferFailure, 0)
	for _, target := range targets {
		written, writeErr := target.association.writeMTPTransfer(request.ProtocolData, target.as)
		if writeErr != nil {
			failures = append(failures, MTPTransferFailure{SGP: target.identity, Err: writeErr})
			continue
		}
		result.TransmittedAssociations++
		result.UserDataOctets = written
		successful = append(successful, target.identity)
	}
	if len(failures) > 0 {
		return result, &MTPTransferError{
			SuccessfulSGPs: append([]SGPIdentity(nil), successful...),
			Failures:       append([]MTPTransferFailure(nil), failures...),
		}
	}
	return result, nil
}

func (r *aspRoutes) selectTransfer(request MTPTransferRequest) ([]aspTransferTarget, error) {
	congestionDecision := evaluateASPCongestionPolicy(
		r.config.congestionPolicy,
		request.ProtocolData.MessagePriority,
	)
	r.mu.Lock()
	defer r.mu.Unlock()

	mtpRoute, err := r.resolveTransferMTPRouteLocked(request.MTPRoute, request.ProtocolData)
	if err != nil {
		return nil, err
	}
	flowKey := newASPTransferFlowKey(mtpRoute.id, request.ProtocolData)
	if element := r.transferFlows[flowKey]; element != nil {
		assignment := element.Value.(*aspTransferAssignment)
		if r.transferTargetsEligibleLocked(
			assignment.targets, mtpRoute.id, request.ProtocolData, congestionDecision,
		) {
			r.transferFlowLRU.MoveToFront(element)
			return append([]aspTransferTarget(nil), assignment.targets...), nil
		}
		r.removeTransferFlowLocked(element)
	}

	gatewayCandidates := r.transferGatewayCandidatesLocked(
		mtpRoute.id, request.ProtocolData, congestionDecision,
	)
	if len(gatewayCandidates) == 0 {
		return nil, ErrNoMTPRoute
	}
	gatewayCandidates = bestASPTransferGateways(gatewayCandidates)
	selectedGateways := selectASPTransferGateways(
		gatewayCandidates,
		r.config.signallingGatewaySelection,
		hashASPTransferFlow(flowKey, "signalling-gateway"),
	)
	targets := make([]aspTransferTarget, 0)
	for _, gateway := range selectedGateways {
		selectedSGPs := selectASPTransferSGPs(
			gateway.sgps,
			gateway.config.sgpSelection,
			hashASPTransferFlow(flowKey, string(gateway.config.id)),
		)
		for _, sgp := range selectedSGPs {
			associationHash := hashASPTransferFlow(flowKey,
				string(sgp.identity.SignallingGateway)+"/"+string(sgp.identity.SignallingGatewayProcess))
			association := sgp.associations[int(associationHash%uint64(len(sgp.associations)))]
			targets = append(targets, aspTransferTarget{
				identity:    sgp.identity,
				association: association,
				as:          sgp.as,
				routeStatus: gateway.status,
			})
		}
	}
	if len(targets) == 0 {
		return nil, ErrNoMTPRoute
	}
	r.rememberTransferFlowLocked(flowKey, targets)
	return append([]aspTransferTarget(nil), targets...), nil
}

func (r *aspRoutes) resolveTransferMTPRouteLocked(
	requested MTPRouteID,
	protocolData *params.ProtocolDataPayload,
) (aspMTPRoute, error) {
	if protocolData == nil {
		return aspMTPRoute{}, ErrMissingProtocolData
	}
	if protocolData.OriginatingPointCode > 0xffffff || protocolData.DestinationPointCode > 0xffffff {
		return aspMTPRoute{}, ErrInvalidMTPTransfer
	}
	if requested != "" {
		mtpRoute, exists := r.mtpRoute(requested)
		if !exists {
			return aspMTPRoute{}, ErrUnknownMTPRoute
		}
		if !aspMTPRouteMatchesProtocolData(mtpRoute, protocolData) {
			return aspMTPRoute{}, ErrMTPTransferOutsideRoute
		}
		return mtpRoute, nil
	}

	bestMask := uint8(0xff)
	matches := make([]aspMTPRoute, 0, 1)
	for _, mtpRoute := range r.config.mtpRoutes {
		if !aspMTPRouteMatchesProtocolData(mtpRoute, protocolData) {
			continue
		}
		if mtpRoute.mask < bestMask {
			bestMask = mtpRoute.mask
			matches = matches[:0]
		}
		if mtpRoute.mask == bestMask {
			matches = append(matches, mtpRoute)
		}
	}
	switch len(matches) {
	case 0:
		return aspMTPRoute{}, ErrNoMatchingMTPRoute
	case 1:
		return matches[0], nil
	default:
		return aspMTPRoute{}, ErrAmbiguousMTPRoute
	}
}

func aspMTPRouteMatchesProtocolData(mtpRoute aspMTPRoute, protocolData *params.ProtocolDataPayload) bool {
	if protocolData == nil ||
		!aspRangeCovers(mtpRoute.destinationPointCode, mtpRoute.mask, protocolData.DestinationPointCode, 0) {
		return false
	}
	if len(mtpRoute.serviceIndicators) > 0 &&
		!containsUint8(mtpRoute.serviceIndicators, protocolData.ServiceIndicator) {
		return false
	}
	return len(mtpRoute.originatingPointCodes) == 0 ||
		containsUint32(mtpRoute.originatingPointCodes, protocolData.OriginatingPointCode)
}

func containsUint8(values []uint8, wanted uint8) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func containsUint32(values []uint32, wanted uint32) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func (r *aspRoutes) transferGatewayCandidatesLocked(
	mtpRoute MTPRouteID,
	protocolData *params.ProtocolDataPayload,
	congestionDecision aspCongestionDecision,
) []aspTransferGateway {
	candidates := make([]aspTransferGateway, 0, len(r.config.signallingGateways))
	for _, gateway := range r.config.signallingGateways {
		status, configured := r.signallingGatewayStatusLocked(
			gateway, mtpRoute, protocolData.DestinationPointCode, 0,
		)
		if !configured || status.availability == DestinationUnavailable {
			continue
		}
		if !congestionDecision.permits(status) {
			continue
		}
		candidate := aspTransferGateway{config: gateway, status: status}
		for _, sgp := range gateway.sgps {
			route, exists := aspSGPRouteForMTPRoute(sgp, mtpRoute)
			if !exists {
				continue
			}
			identity := SGPIdentity{
				SignallingGateway:        gateway.id,
				SignallingGatewayProcess: sgp.id,
			}
			associations := make([]*Association, 0, len(r.associationsBySGP[identity]))
			for association := range r.associationsBySGP[identity] {
				if aspAssociationEligibleForAS(association, route.as) {
					associations = append(associations, association)
				}
			}
			sort.Slice(associations, func(first, second int) bool {
				return r.associationOrder[associations[first]] < r.associationOrder[associations[second]]
			})
			if len(associations) > 0 {
				candidate.sgps = append(candidate.sgps, aspTransferSGP{
					identity: identity, as: route.as, associations: associations,
				})
			}
		}
		if len(candidate.sgps) > 0 {
			candidates = append(candidates, candidate)
		}
	}
	return candidates
}

func bestASPTransferGateways(candidates []aspTransferGateway) []aspTransferGateway {
	bestAvailability := 3
	bestCongestion := 5
	for _, candidate := range candidates {
		availability := aspAvailabilityRank(candidate.status.availability)
		congestion := aspCongestionRank(candidate.status)
		if availability < bestAvailability || availability == bestAvailability && congestion < bestCongestion {
			bestAvailability = availability
			bestCongestion = congestion
		}
	}
	best := make([]aspTransferGateway, 0, len(candidates))
	for _, candidate := range candidates {
		if aspAvailabilityRank(candidate.status.availability) == bestAvailability &&
			aspCongestionRank(candidate.status) == bestCongestion {
			best = append(best, candidate)
		}
	}
	return best
}

func selectASPTransferGateways(
	candidates []aspTransferGateway,
	mode RouteSelectionMode,
	hash uint64,
) []aspTransferGateway {
	switch mode {
	case RouteSelectionPrimaryBackup:
		return candidates[:1]
	case RouteSelectionLoadshare:
		return candidates[int(hash%uint64(len(candidates))) : int(hash%uint64(len(candidates)))+1]
	case RouteSelectionBroadcast:
		return candidates
	default:
		return nil
	}
}

func selectASPTransferSGPs(candidates []aspTransferSGP, mode RouteSelectionMode, hash uint64) []aspTransferSGP {
	switch mode {
	case RouteSelectionPrimaryBackup:
		return candidates[:1]
	case RouteSelectionLoadshare:
		return candidates[int(hash%uint64(len(candidates))) : int(hash%uint64(len(candidates)))+1]
	case RouteSelectionBroadcast:
		return candidates
	default:
		return nil
	}
}

func newASPTransferFlowKey(mtpRoute MTPRouteID, protocolData *params.ProtocolDataPayload) aspTransferFlowKey {
	return aspTransferFlowKey{
		mtpRoute: mtpRoute,
		opc:      protocolData.OriginatingPointCode,
		dpc:      protocolData.DestinationPointCode,
		si:       protocolData.ServiceIndicator,
		ni:       protocolData.NetworkIndicator,
		mp:       protocolData.MessagePriority,
		sls:      protocolData.SignallingLinkSelection,
	}
}

func hashASPTransferFlow(key aspTransferFlowKey, salt string) uint64 {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(key.mtpRoute))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(salt))
	var encoded [12]byte
	binary.BigEndian.PutUint32(encoded[0:4], key.opc)
	binary.BigEndian.PutUint32(encoded[4:8], key.dpc)
	encoded[8] = key.si
	encoded[9] = key.ni
	encoded[10] = key.mp
	encoded[11] = key.sls
	_, _ = hash.Write(encoded[:])
	return hash.Sum64()
}

func (r *aspRoutes) transferTargetsEligibleLocked(
	targets []aspTransferTarget,
	mtpRoute MTPRouteID,
	protocolData *params.ProtocolDataPayload,
	congestionDecision aspCongestionDecision,
) bool {
	if len(targets) == 0 {
		return false
	}
	for _, target := range targets {
		identity, attached := r.associations[target.association]
		if !attached || identity != target.identity || !aspAssociationEligibleForAS(target.association, target.as) {
			return false
		}
		gateway, found := r.signallingGatewayConfig(target.identity.SignallingGateway)
		if !found {
			return false
		}
		status, configured := r.signallingGatewayStatusLocked(
			gateway, mtpRoute, protocolData.DestinationPointCode, 0,
		)
		if !configured || status.availability == DestinationUnavailable {
			return false
		}
		if status != target.routeStatus {
			return false
		}
		if !congestionDecision.permits(status) {
			return false
		}
	}
	return true
}

func evaluateASPCongestionPolicy(policy ASPCongestionPolicy, messagePriority uint8) aspCongestionDecision {
	if policy == nil {
		return aspCongestionDecision{}
	}
	decision := aspCongestionDecision{
		enabled:      true,
		unknownLevel: policy(messagePriority, 0, false),
	}
	for level := uint8(1); level <= 3; level++ {
		decision.levels[level] = policy(messagePriority, level, true)
	}
	return decision
}

func (decision aspCongestionDecision) permits(status aspDestinationStatus) bool {
	if !decision.enabled || !status.congested {
		return true
	}
	if !status.congestionLevelSet {
		return decision.unknownLevel
	}
	if status.congestionLevel > 3 {
		return false
	}
	return decision.levels[status.congestionLevel]
}

func (r *aspRoutes) signallingGatewayConfig(id SignallingGatewayID) (aspSignallingGatewayConfig, bool) {
	for _, gateway := range r.config.signallingGateways {
		if gateway.id == id {
			return gateway, true
		}
	}
	return aspSignallingGatewayConfig{}, false
}

func (r *aspRoutes) rememberTransferFlowLocked(key aspTransferFlowKey, targets []aspTransferTarget) {
	if r.config.transferFlowCacheEntries <= 0 {
		return
	}
	assignment := &aspTransferAssignment{key: key, targets: append([]aspTransferTarget(nil), targets...)}
	element := r.transferFlowLRU.PushFront(assignment)
	r.transferFlows[key] = element
	for r.transferFlowLRU.Len() > r.config.transferFlowCacheEntries {
		r.removeTransferFlowLocked(r.transferFlowLRU.Back())
	}
}

func (r *aspRoutes) removeTransferFlowLocked(element *list.Element) {
	if element == nil {
		return
	}
	assignment := element.Value.(*aspTransferAssignment)
	delete(r.transferFlows, assignment.key)
	r.transferFlowLRU.Remove(element)
}

func (r *aspRoutes) invalidateAssociationTransferFlowsLocked(association *Association) {
	for element := r.transferFlowLRU.Front(); element != nil; {
		next := element.Next()
		assignment := element.Value.(*aspTransferAssignment)
		for _, target := range assignment.targets {
			if target.association == association {
				r.removeTransferFlowLocked(element)
				break
			}
		}
		element = next
	}
}

func (c *Association) writeMTPTransfer(protocolData *params.ProtocolDataPayload, key ASKey) (int, error) {
	if c == nil || c.role != RoleASP || protocolData == nil {
		return 0, ErrInvalidMTPTransfer
	}
	c.aspTransferMu.RLock()
	defer c.aspTransferMu.RUnlock()
	if !aspAssociationEligibleForAS(c, key) {
		return 0, ErrRoutingContextNotActive
	}
	stream := c.streamFor(protocolData.SignallingLinkSelection)
	if err := c.checkDataStream(stream); err != nil {
		return 0, err
	}
	var networkAppearance *params.Param
	if key.NetworkAppearanceSet {
		networkAppearance = params.NewNetworkAppearance(key.NetworkAppearance)
	}
	protocolDataParam := params.NewProtocolData(
		protocolData.OriginatingPointCode,
		protocolData.DestinationPointCode,
		protocolData.ServiceIndicator,
		protocolData.NetworkIndicator,
		protocolData.MessagePriority,
		protocolData.SignallingLinkSelection,
		protocolData.Data,
	)
	encoded, err := messages.NewData(
		networkAppearance,
		routingContextParamForASKey(key),
		protocolDataParam,
		c.cfg.CorrelationID.Copy(),
	).MarshalBinary()
	if err != nil {
		return 0, err
	}
	info := *c.sctpInfo
	info.Stream = stream
	if _, err := c.writeSCTPData(encoded, &info); err != nil {
		return 0, err
	}
	return len(protocolData.Data), nil
}

func (c *Association) lockASPTransferMutation() func() {
	if c == nil || c.role != RoleASP {
		return func() {}
	}
	c.aspTransferMu.Lock()
	return c.aspTransferMu.Unlock
}
