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

	"github.com/gomaja/go-m3ua/messages/params"
)

// MTPTransferRequest is the MTP-TRANSFER request primitive described by RFC
// 4666 Sections 1.6.1 and 5.5.1.1.1. MTPRoute may be omitted when the MTP
// routing label has one unambiguous best match in the ASP routing table.
type MTPTransferRequest struct {
	MTPRoute     MTPRouteID
	ProtocolData *params.ProtocolDataPayload
	// CorrelationID carries the RFC 4666 Section 3.3.1 Correlation Id on every
	// DATA this request produces. CorrelationIDSet distinguishes an explicit
	// zero from an omitted parameter.
	CorrelationID    uint32
	CorrelationIDSet bool
}

// ErrDestinationStateUnknown reports that no Signalling Gateway has told this
// ASP whether the requested destination is reachable through the candidate that
// would have carried it.
//
// Unknown is not available. RFC 4666 Section 4.5.2.2 has the ASP maintain the
// status of each destination from what the Signalling Gateways report, and a
// destination none of them has reported on has no status to maintain.
// Presuming it reachable would send traffic on the strength of nothing, so the
// default is to refuse. ASPRoutingConfig.AllowUnknownDestinations is the
// explicit opt-in for a deployment that would rather try.
//
// Endpoint.MTPDestinationStatus answers a different question and may report the
// same destination Available, because RFC 4666 Appendix A.2.2 defines an SG's
// capability as the absence of an inaccessibility report rather than the
// presence of an availability one. That is the MTP3-User's aggregate status
// over every route; this is one candidate's selection. The disagreement is
// confined to a destination whose Signalling Gateway is established, activated
// and silent, and is documented on MTPDestinationStatus.
var ErrDestinationStateUnknown = errors.New("m3ua: destination state is unknown")

// MTPTransferPath is one concrete target an MTP-TRANSFER request was admitted
// to: the provisioned path it came from, the SGP and Application Server it
// resolved to, the wire scope that names that Application Server on that
// Association, and the SSNM partition generation the availability decision was
// made under.
type MTPTransferPath struct {
	Path              MTPRoutePathID
	SGP               SGPIdentity
	ApplicationServer RemoteASID
	AS                ASKey
	Association       AssociationID
	// Epoch is the RFC 4666 Section 4.5.1 binding generation of the canonical
	// SSNM partition the decision read. A target frozen under one epoch is
	// recognisable after a source reset replaced it.
	Epoch uint64
}

// MTPTransferResult reports successful transfer of the MTP3-User payload.
//
// SuccessfulPaths names every target the payload reached, in selection order.
// It reports what this node sent, not what the far end received: a successful
// write is a handover to SCTP and nothing more.
type MTPTransferResult struct {
	UserDataOctets  int
	SuccessfulPaths []MTPTransferPath
}

// MTPTransferFailure identifies one selected target whose DATA write failed. A
// write is never retried automatically because failure does not prove that the
// peer received no DATA.
type MTPTransferFailure struct {
	Target MTPTransferPath
	Err    error
}

// MTPTransferError reports a failed or partially failed MTP-TRANSFER request.
// The slices are owned by the error and ordered by selection order.
type MTPTransferError struct {
	SuccessfulPaths []MTPTransferPath
	Failures        []MTPTransferFailure
}

func (e *MTPTransferError) Error() string {
	if e == nil {
		return "MTP-TRANSFER failed"
	}
	return fmt.Sprintf("MTP-TRANSFER succeeded through %d targets and failed through %d targets",
		len(e.SuccessfulPaths), len(e.Failures))
}

// MTPSelectionReason names why one provisioned candidate could not carry a
// request. The reasons are evaluated in declaration order, so the first one
// that applies is the one reported.
type MTPSelectionReason uint8

const (
	// MTPCandidateNotBound reports a candidate no Association of the SGP is
	// configured for, or whose RFC 4666 Section 4.4.1 registration has not
	// assigned it a Routing Context yet.
	MTPCandidateNotBound MTPSelectionReason = iota + 1
	// MTPCandidateNotActive reports a candidate whose Association has not
	// completed the RFC 4666 Section 4.3.4.3 activation for it.
	MTPCandidateNotActive
	// MTPCandidateStateUnknown reports a candidate this ASP has no destination
	// status for and that AllowUnknownDestinations does not permit.
	MTPCandidateStateUnknown
	// MTPCandidateUnavailable reports a candidate whose Signalling Gateway
	// reported the destination unavailable.
	MTPCandidateUnavailable
	// MTPCandidateCongested reports a candidate the application's congestion
	// policy refused for this Message Priority.
	MTPCandidateCongested
)

func (r MTPSelectionReason) String() string {
	switch r {
	case MTPCandidateNotBound:
		return "not-bound"
	case MTPCandidateNotActive:
		return "not-active"
	case MTPCandidateStateUnknown:
		return "state-unknown"
	case MTPCandidateUnavailable:
		return "unavailable"
	case MTPCandidateCongested:
		return "congested"
	default:
		return "unknown"
	}
}

// MTPCandidateRejection is one provisioned candidate the route could not use.
type MTPCandidateRejection struct {
	Path              MTPRoutePathID
	SGP               SGPIdentity
	ApplicationServer RemoteASID
	Reason            MTPSelectionReason
}

// MTPSelectionError reports that no provisioned candidate of one MTP Route
// could carry a request, and why each was refused.
//
// The rejections are in the route's own candidate order: the paths in
// reference order, the SGPs of each Signalling Gateway in configured order, and
// the Application Servers of each path in preference order.
type MTPSelectionError struct {
	MTPRoute   MTPRouteID
	Rejections []MTPCandidateRejection
}

func (e *MTPSelectionError) Error() string {
	if e == nil {
		return "MTP-TRANSFER selected no route"
	}
	return fmt.Sprintf("MTP Route %q has no usable candidate: %d refused", e.MTPRoute, len(e.Rejections))
}

// Unwrap reports ErrDestinationStateUnknown when the route was refused only
// because nothing is known about the destination, and ErrNoMTPRoute otherwise.
// A candidate refused for any other reason is a route that exists and cannot
// be used, which is not the same as one this node has no information about.
func (e *MTPSelectionError) Unwrap() error {
	if e == nil {
		return nil
	}
	unknown := false
	for _, rejection := range e.Rejections {
		switch rejection.Reason {
		case MTPCandidateStateUnknown:
			unknown = true
		default:
			return ErrNoMTPRoute
		}
	}
	if unknown {
		return ErrDestinationStateUnknown
	}
	return ErrNoMTPRoute
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
	sls      uint8
}

// aspSelectionStatus is what the canonical SSNM store retains about one
// destination inside one candidate's partition.
//
// Availability is explicit rather than defaulted: no report is not a report of
// reachability, so the absent case has to be distinguishable from an arriving
// DAVA. Congestion is a separate dimension, as RFC 4666 Section 4.5.2.2
// requires, and an absent congestion record says nothing about availability.
type aspSelectionStatus struct {
	epoch              uint64
	availabilitySet    bool
	availability       DestinationAvailability
	congestionSet      bool
	congested          bool
	congestionLevel    uint8
	congestionLevelSet bool
}

// usable reports whether the availability dimension permits this candidate.
// The three answers are distinct: a reported unavailability refuses, no report
// at all refuses unless the deployment opted in, and anything else permits.
func (status aspSelectionStatus) usable(allowUnknown bool) (bool, MTPSelectionReason) {
	if !status.availabilitySet {
		if allowUnknown {
			return true, 0
		}
		return false, MTPCandidateStateUnknown
	}
	if status.availability == DestinationUnavailable {
		return false, MTPCandidateUnavailable
	}
	return true, 0
}

// availabilityRank orders candidates for preference. Unknown ranks behind
// restricted: a destination a peer called restricted is one it says it can
// still reach, while one nobody has reported on is not known to be reachable
// at all. The order is explicit library policy, not an RFC algorithm.
func (status aspSelectionStatus) availabilityRank() int {
	if !status.availabilitySet {
		return 2
	}
	switch status.availability {
	case DestinationAvailable:
		return 0
	case DestinationRestricted:
		return 1
	default:
		return 3
	}
}

func (status aspSelectionStatus) congestionRank() int {
	if !status.congested {
		return 0
	}
	if !status.congestionLevelSet {
		return 4
	}
	return int(status.congestionLevel)
}

// betterThan orders two candidate statuses by availability, then congestion.
func (status aspSelectionStatus) betterThan(other aspSelectionStatus) bool {
	if status.availabilityRank() != other.availabilityRank() {
		return status.availabilityRank() < other.availabilityRank()
	}
	return status.congestionRank() < other.congestionRank()
}

func (status aspSelectionStatus) sameRankAs(other aspSelectionStatus) bool {
	return status.availabilityRank() == other.availabilityRank() &&
		status.congestionRank() == other.congestionRank()
}

type aspTransferTarget struct {
	identity          SGPIdentity
	path              MTPRoutePathID
	applicationServer RemoteASID
	association       *Association
	as                ASKey
	routeStatus       aspSelectionStatus
}

// aspTransferAssignment is one remembered traffic-flow assignment.
//
// It is valid while the Endpoint still has the targets, the knowledge that
// chose them has not moved, and the congestion decision is the same. The store
// revision covers the second: it advances on every change the SSNM store
// accepted, so an assignment made at one revision is known to have been made
// against knowledge that has not changed while the revision has not.
type aspTransferAssignment struct {
	key                aspTransferFlowKey
	targets            []aspTransferTarget
	storeRevision      uint64
	congestionDecision aspCongestionDecision
}

// aspTransferMember is one Association of one SGP together with the wire scope
// it carries the selected candidate in. The scope belongs to the Association
// because an RFC 4666 Section 4.4.1 registration assigns it per Association.
type aspTransferMember struct {
	association *Association
	as          ASKey
}

type aspTransferSGP struct {
	identity          SGPIdentity
	path              MTPRoutePathID
	applicationServer RemoteASID
	status            aspSelectionStatus
	members           []aspTransferMember
}

type aspTransferGateway struct {
	config aspSignallingGatewayConfig
	status aspSelectionStatus
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
	if !e.aspRoutes.routingConfigured() {
		// Application-managed routing owns outbound candidate selection, so
		// naming no route is a configuration answer, not an empty inventory.
		return MTPTransferResult{}, ErrRoutingNotConfigured
	}
	if request.ProtocolData == nil {
		return MTPTransferResult{}, ErrMissingProtocolData
	}
	if !e.beginOperation() {
		return MTPTransferResult{}, ErrEndpointClosed
	}
	defer e.endOperation()

	// The congestion policy is evaluated before any Endpoint lock is taken and
	// for every level it could be asked about, so it never runs inside the
	// route state it filters and can ask this Endpoint what it knows.
	congestionDecision := evaluateASPCongestionPolicy(
		e.aspRoutes.config.congestionPolicy,
		request.ProtocolData.MessagePriority,
	)
	unlockSequence, err := e.aspRoutes.lockTransferSequence(request)
	if err != nil {
		return MTPTransferResult{}, err
	}
	defer unlockSequence()

	targets, err := e.aspRoutes.selectTransfer(request, congestionDecision, e.ssnm)
	if err != nil {
		return MTPTransferResult{}, err
	}
	// The concrete targets are frozen here. A target that fails is reported as
	// it is: nothing else is tried for this request, because a failed write
	// does not prove the peer received no DATA and a second attempt through
	// another Application Server could duplicate it.
	result := MTPTransferResult{SuccessfulPaths: make([]MTPTransferPath, 0, len(targets))}
	failures := make([]MTPTransferFailure, 0)
	for _, target := range targets {
		description := target.describe()
		written, writeErr := target.association.writeMTPTransfer(request, target.as)
		if writeErr != nil {
			failures = append(failures, MTPTransferFailure{Target: description, Err: writeErr})
			continue
		}
		result.UserDataOctets = written
		result.SuccessfulPaths = append(result.SuccessfulPaths, description)
	}
	if len(failures) > 0 {
		return result, &MTPTransferError{
			SuccessfulPaths: append([]MTPTransferPath(nil), result.SuccessfulPaths...),
			Failures:        append([]MTPTransferFailure(nil), failures...),
		}
	}
	return result, nil
}

func (target aspTransferTarget) describe() MTPTransferPath {
	return MTPTransferPath{
		Path:              target.path,
		SGP:               target.identity,
		ApplicationServer: target.applicationServer,
		AS:                target.as,
		Association:       target.association.ID(),
		Epoch:             target.routeStatus.epoch,
	}
}

func (r *aspRoutes) lockTransferSequence(request MTPTransferRequest) (func(), error) {
	r.mu.RLock()
	mtpRoute, err := r.resolveTransferMTPRouteLocked(request.MTPRoute, request.ProtocolData)
	r.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	flowKey := newASPTransferFlowKey(mtpRoute.id, request.ProtocolData)

	r.transferSequenceMu.Lock()
	flowLock := r.transferSequences[flowKey]
	if flowLock == nil {
		flowLock = &aspTransferFlowLock{}
		r.transferSequences[flowKey] = flowLock
	}
	flowLock.references++
	r.transferSequenceMu.Unlock()

	flowLock.mu.Lock()
	return func() {
		flowLock.mu.Unlock()
		r.transferSequenceMu.Lock()
		flowLock.references--
		if flowLock.references == 0 {
			delete(r.transferSequences, flowKey)
		}
		r.transferSequenceMu.Unlock()
	}, nil
}

func (r *aspRoutes) selectTransfer(
	request MTPTransferRequest,
	congestionDecision aspCongestionDecision,
	store *ssnmState,
) ([]aspTransferTarget, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	mtpRoute, err := r.resolveTransferMTPRouteLocked(request.MTPRoute, request.ProtocolData)
	if err != nil {
		return nil, err
	}
	storeRevision := store.currentRevision()
	flowKey := newASPTransferFlowKey(mtpRoute.id, request.ProtocolData)
	var previousTargets []aspTransferTarget
	var cachedElement *list.Element
	cachedEligible := false
	if element := r.transferFlows[flowKey]; element != nil {
		assignment := element.Value.(*aspTransferAssignment)
		cachedEligible = r.transferTargetsStillHeldLocked(assignment.targets)
		if cachedEligible &&
			assignment.storeRevision == storeRevision &&
			assignment.congestionDecision == congestionDecision &&
			r.transferTargetsUseStickyLoadshare(assignment.targets) {
			r.transferFlowLRU.MoveToFront(element)
			return append([]aspTransferTarget(nil), assignment.targets...), nil
		}
		previousTargets = append(previousTargets, assignment.targets...)
		cachedElement = element
	}

	gatewayCandidates, rejections := r.transferCandidatesLocked(
		mtpRoute, request.ProtocolData.DestinationPointCode, congestionDecision, store,
	)
	if len(gatewayCandidates) == 0 {
		if cachedElement != nil {
			r.removeTransferFlowLocked(cachedElement)
		}
		return nil, &MTPSelectionError{MTPRoute: mtpRoute.id, Rejections: rejections}
	}
	if r.config.signallingGatewaySelection != RouteSelectionBroadcast {
		gatewayCandidates = bestASPTransferGateways(gatewayCandidates)
	}
	selectedGateways := selectASPTransferGatewaysWithPrevious(
		gatewayCandidates,
		r.config.signallingGatewaySelection,
		hashASPTransferFlow(flowKey, "signalling-gateway"),
		previousTargets,
	)
	targets := make([]aspTransferTarget, 0)
	for _, gateway := range selectedGateways {
		sgpCandidates := gateway.sgps
		if gateway.config.sgpSelection != RouteSelectionBroadcast {
			sgpCandidates = bestASPTransferSGPs(sgpCandidates)
		}
		selectedSGPs := selectASPTransferSGPsWithPrevious(
			sgpCandidates,
			gateway.config.sgpSelection,
			hashASPTransferFlow(flowKey, string(gateway.config.id)),
			previousTargets,
			gateway.config.id,
		)
		for _, sgp := range selectedSGPs {
			associationHash := hashASPTransferFlow(flowKey,
				string(sgp.identity.SignallingGateway)+"/"+string(sgp.identity.SignallingGatewayProcess))
			member := sgp.members[int(associationHash%uint64(len(sgp.members)))]
			if previous, held := previousASPTransferMember(previousTargets, sgp); held {
				member = previous
			}
			targets = append(targets, aspTransferTarget{
				identity:          sgp.identity,
				path:              sgp.path,
				applicationServer: sgp.applicationServer,
				association:       member.association,
				as:                member.as,
				routeStatus:       sgp.status,
			})
		}
	}
	if len(targets) == 0 {
		if cachedElement != nil {
			r.removeTransferFlowLocked(cachedElement)
		}
		return nil, &MTPSelectionError{MTPRoute: mtpRoute.id, Rejections: rejections}
	}
	if cachedElement != nil {
		assignment := cachedElement.Value.(*aspTransferAssignment)
		if cachedEligible && sameASPTransferTargets(assignment.targets, targets) {
			assignment.storeRevision = storeRevision
			assignment.congestionDecision = congestionDecision
			r.transferFlowLRU.MoveToFront(cachedElement)
			return append([]aspTransferTarget(nil), assignment.targets...), nil
		}
		r.removeTransferFlowLocked(cachedElement)
	}
	r.rememberTransferFlowLocked(flowKey, targets, congestionDecision, storeRevision)
	return append([]aspTransferTarget(nil), targets...), nil
}

// transferTargetsUseStickyLoadshare reports whether this route's selection
// modes are the ones that keep a traffic flow on the route it was assigned.
//
// Only loadsharing does. RFC 4666 Appendix A.2.2 has the distribution of
// MTP3-User messages over the SGPs "done in such a way to minimize message
// missequencing", which a flow that moves between paths does not, so a
// remembered assignment is reused there. Every other mode is decided again
// from the current candidates, which is what lets a primary come back the
// moment it can carry traffic again without waiting for anything else to move.
func (r *aspRoutes) transferTargetsUseStickyLoadshare(targets []aspTransferTarget) bool {
	if r.config.signallingGatewaySelection != RouteSelectionLoadshare {
		return false
	}
	for _, target := range targets {
		gateway, found := r.signallingGatewayConfig(target.identity.SignallingGateway)
		if !found || gateway.sgpSelection != RouteSelectionLoadshare {
			return false
		}
	}
	return len(targets) > 0
}

func sameASPTransferTargets(first, second []aspTransferTarget) bool {
	if len(first) != len(second) {
		return false
	}
	for index := range first {
		if first[index] != second[index] {
			return false
		}
	}
	return true
}

// previousASPTransferMember keeps a loadshared traffic flow on the Association
// it was assigned to, where that Association still carries the candidate.
//
// The member comes from the current candidate, so the wire scope it carries is
// the current one: an Application Server re-registered into a different Routing
// Context keeps the flow where it was and sends it in the label the SGP assigns
// now.
func previousASPTransferMember(
	targets []aspTransferTarget,
	sgp aspTransferSGP,
) (aspTransferMember, bool) {
	for _, target := range targets {
		if target.identity != sgp.identity || target.applicationServer != sgp.applicationServer {
			continue
		}
		for _, member := range sgp.members {
			if member.association == target.association {
				return member, true
			}
		}
	}
	return aspTransferMember{}, false
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

// transferCandidatesLocked walks one route's provisioned candidates in order
// and reports the ones that can carry the request, together with why every
// other one was refused.
//
// The order is the approved one: within each SGP the first eligible configured
// Application Server, and only then the Signalling Gateway and SGP selection
// modes. Each candidate is judged on the canonical SSNM partition of its own
// Application Server, so one Application Server reported unavailable is not the
// SGP, the Signalling Gateway or the route going away.
func (r *aspRoutes) transferCandidatesLocked(
	mtpRoute aspMTPRoute,
	pointCode uint32,
	congestionDecision aspCongestionDecision,
	store *ssnmState,
) ([]aspTransferGateway, []MTPCandidateRejection) {
	candidates := make([]aspTransferGateway, 0, len(mtpRoute.gateways))
	rejections := make([]MTPCandidateRejection, 0)
	for _, routeGateway := range mtpRoute.gateways {
		gateway, provisioned := r.config.signallingGateway(routeGateway.id)
		if !provisioned {
			continue
		}
		candidate := aspTransferGateway{config: gateway}
		for _, sgp := range gateway.sgps {
			identity := SGPIdentity{
				SignallingGateway:        gateway.id,
				SignallingGatewayProcess: sgp.id,
			}
			selected, chosen, refused := r.transferSGPCandidateLocked(
				sgp, identity, mtpRoute.id, pointCode, congestionDecision, store,
			)
			rejections = append(rejections, refused...)
			if chosen {
				candidate.sgps = append(candidate.sgps, selected)
			}
		}
		if len(candidate.sgps) == 0 {
			continue
		}
		candidate.status = candidate.sgps[0].status
		for _, sgp := range candidate.sgps[1:] {
			if sgp.status.betterThan(candidate.status) {
				candidate.status = sgp.status
			}
		}
		candidates = append(candidates, candidate)
	}
	return candidates, rejections
}

// transferSGPCandidateLocked chooses the first eligible configured Application
// Server within one SGP and reports the Associations that carry it.
//
// The refusals are ordered as the approved contract requires: binding and
// authorization first, then active state, then the destination's availability,
// and only then the application's congestion policy.
func (r *aspRoutes) transferSGPCandidateLocked(
	sgp aspSGPConfig,
	identity SGPIdentity,
	mtpRoute MTPRouteID,
	pointCode uint32,
	congestionDecision aspCongestionDecision,
	store *ssnmState,
) (aspTransferSGP, bool, []MTPCandidateRejection) {
	candidates := sgp.candidatesFor(mtpRoute)
	if len(candidates) == 0 {
		// This SGP serves none of the route's Application Servers, so it is not
		// a candidate and has nothing to refuse.
		return aspTransferSGP{}, false, nil
	}
	associations := make([]*Association, 0, len(r.associationsBySGP[identity]))
	for association := range r.associationsBySGP[identity] {
		associations = append(associations, association)
	}
	sort.Slice(associations, func(first, second int) bool {
		return r.associationOrder[associations[first]] < r.associationOrder[associations[second]]
	})
	rejections := make([]MTPCandidateRejection, 0)
	for _, candidate := range candidates {
		rejection := MTPCandidateRejection{
			Path:              candidate.path,
			SGP:               identity,
			ApplicationServer: candidate.applicationServer,
		}
		members, reason := r.transferMembersLocked(associations, identity, candidate.applicationServer)
		if len(members) == 0 {
			rejection.Reason = reason
			rejections = append(rejections, rejection)
			continue
		}
		status := store.destinationKnowledge(SSNMPartition{
			Kind:              SSNMCanonicalPartition,
			SignallingGateway: identity.SignallingGateway,
			ApplicationServer: candidate.applicationServer,
		}, pointCode)
		if usable, refusal := status.usable(r.config.allowUnknownDestinations); !usable {
			rejection.Reason = refusal
			rejections = append(rejections, rejection)
			continue
		}
		if !congestionDecision.permits(status) {
			rejection.Reason = MTPCandidateCongested
			rejections = append(rejections, rejection)
			continue
		}
		return aspTransferSGP{
			identity:          identity,
			path:              candidate.path,
			applicationServer: candidate.applicationServer,
			status:            status,
			members:           members,
		}, true, rejections
	}
	return aspTransferSGP{}, false, rejections
}

// transferMembersLocked reports the Associations of one SGP that carry one
// canonical Application Server now, and why none does when none does.
func (r *aspRoutes) transferMembersLocked(
	associations []*Association,
	identity SGPIdentity,
	applicationServer RemoteASID,
) ([]aspTransferMember, MTPSelectionReason) {
	members := make([]aspTransferMember, 0, len(associations))
	bound := false
	for _, association := range associations {
		for _, key := range r.config.asKeysFor(association, identity, applicationServer) {
			if !aspAssociationBoundToAS(association, key) {
				continue
			}
			bound = true
			if aspAssociationEligibleForAS(association, key) {
				members = append(members, aspTransferMember{association: association, as: key})
				break
			}
		}
	}
	if len(members) > 0 {
		return members, 0
	}
	if bound {
		return nil, MTPCandidateNotActive
	}
	return nil, MTPCandidateNotBound
}

// bestASPTransferSGPs keeps the best-ranked SGP candidates of one Signalling
// Gateway, so a selection mode chooses among equals.
func bestASPTransferSGPs(candidates []aspTransferSGP) []aspTransferSGP {
	if len(candidates) == 0 {
		return candidates
	}
	best := candidates[0].status
	for _, candidate := range candidates[1:] {
		if candidate.status.betterThan(best) {
			best = candidate.status
		}
	}
	kept := make([]aspTransferSGP, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.status.sameRankAs(best) {
			kept = append(kept, candidate)
		}
	}
	return kept
}

func bestASPTransferGateways(candidates []aspTransferGateway) []aspTransferGateway {
	if len(candidates) == 0 {
		return candidates
	}
	best := candidates[0].status
	for _, candidate := range candidates[1:] {
		if candidate.status.betterThan(best) {
			best = candidate.status
		}
	}
	kept := make([]aspTransferGateway, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.status.sameRankAs(best) {
			kept = append(kept, candidate)
		}
	}
	return kept
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

func selectASPTransferGatewaysWithPrevious(
	candidates []aspTransferGateway,
	mode RouteSelectionMode,
	hash uint64,
	previous []aspTransferTarget,
) []aspTransferGateway {
	if mode == RouteSelectionLoadshare {
		for _, target := range previous {
			for _, candidate := range candidates {
				if candidate.config.id == target.identity.SignallingGateway {
					return []aspTransferGateway{candidate}
				}
			}
		}
	}
	return selectASPTransferGateways(candidates, mode, hash)
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

func selectASPTransferSGPsWithPrevious(
	candidates []aspTransferSGP,
	mode RouteSelectionMode,
	hash uint64,
	previous []aspTransferTarget,
	gateway SignallingGatewayID,
) []aspTransferSGP {
	if mode == RouteSelectionLoadshare {
		for _, target := range previous {
			if target.identity.SignallingGateway != gateway {
				continue
			}
			for _, candidate := range candidates {
				if candidate.identity == target.identity {
					return []aspTransferSGP{candidate}
				}
			}
		}
	}
	return selectASPTransferSGPs(candidates, mode, hash)
}

func newASPTransferFlowKey(mtpRoute MTPRouteID, protocolData *params.ProtocolDataPayload) aspTransferFlowKey {
	return aspTransferFlowKey{
		mtpRoute: mtpRoute,
		opc:      protocolData.OriginatingPointCode,
		dpc:      protocolData.DestinationPointCode,
		si:       protocolData.ServiceIndicator,
		ni:       protocolData.NetworkIndicator,
		sls:      protocolData.SignallingLinkSelection,
	}
}

func hashASPTransferFlow(key aspTransferFlowKey, salt string) uint64 {
	hash := fnv.New64a()
	_, _ = hash.Write([]byte(key.mtpRoute))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(salt))
	var encoded [11]byte
	binary.BigEndian.PutUint32(encoded[0:4], key.opc)
	binary.BigEndian.PutUint32(encoded[4:8], key.dpc)
	encoded[8] = key.si
	encoded[9] = key.ni
	encoded[10] = key.sls
	_, _ = hash.Write(encoded[:])
	return hash.Sum64()
}

// transferTargetsStillHeldLocked re-checks a remembered assignment against the
// membership that decided it.
//
// It asks only whether every target is still this Endpoint's to use. What the
// peers have since said about the destination is the store revision the caller
// compares, and the congestion policy's answer is the decision it compares, so
// neither is re-derived here.
func (r *aspRoutes) transferTargetsStillHeldLocked(targets []aspTransferTarget) bool {
	if len(targets) == 0 {
		return false
	}
	for _, target := range targets {
		identity, attached := r.associations[target.association]
		if !attached || identity != target.identity || !aspAssociationEligibleForAS(target.association, target.as) {
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

func (decision aspCongestionDecision) permits(status aspSelectionStatus) bool {
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

func (r *aspRoutes) rememberTransferFlowLocked(
	key aspTransferFlowKey,
	targets []aspTransferTarget,
	congestionDecision aspCongestionDecision,
	storeRevision uint64,
) {
	if r.config.transferFlowCacheEntries <= 0 {
		return
	}
	assignment := &aspTransferAssignment{
		key:                key,
		targets:            append([]aspTransferTarget(nil), targets...),
		storeRevision:      storeRevision,
		congestionDecision: congestionDecision,
	}
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

func (c *Association) writeMTPTransfer(request MTPTransferRequest, key ASKey) (int, error) {
	if c == nil || c.role != RoleASP || request.ProtocolData == nil {
		return 0, ErrInvalidMTPTransfer
	}
	c.aspTransferMu.RLock()
	defer c.aspTransferMu.RUnlock()
	if !aspAssociationEligibleForAS(c, key) {
		return 0, newDataNotSent(key, 0, ErrRoutingContextNotActive)
	}
	// The same encoding, stream selection and outcome classification as a
	// direct WriteData: an Endpoint-selected send differs only in who chose the
	// association and the Application Server.
	data := DataRequest{
		AS:               key,
		ProtocolData:     *request.ProtocolData,
		CorrelationID:    request.CorrelationID,
		CorrelationIDSet: request.CorrelationIDSet,
	}
	if len(data.ProtocolData.Data) > maxProtocolDataOctets {
		return 0, newDataNotSent(key, 0, ErrProtocolDataTooLarge)
	}
	stream := c.streamFor(data.ProtocolData.SignallingLinkSelection)
	if err := c.checkDataStream(stream); err != nil {
		return 0, newDataNotSent(key, 0, err)
	}
	if err := c.submitData(c.encodeDataFrame(&data), stream); err != nil {
		return 0, newDataSendIndeterminate(key, stream, err)
	}
	return len(data.ProtocolData.Data), nil
}

func (c *Association) lockASPTransferMutation() func() {
	if c == nil || c.role != RoleASP {
		return func() {}
	}
	c.aspTransferMu.Lock()
	return c.aspTransferMu.Unlock
}
