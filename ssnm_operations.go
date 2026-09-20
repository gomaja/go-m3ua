package m3ua

import (
	"fmt"
	"sort"
	"sync"
	"unicode/utf8"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// DestinationStateAuditRequest is an RFC 4666 Sections 3.4.3 and 4.5.3 DAUD
// request from an ASP to an SGP.
type DestinationStateAuditRequest struct {
	Scope        WireScope
	Destinations []PointCodeRange
	Info         string
}

// SignallingCongestionRequest is an RFC 4666 Section 3.4.4 SCON request.
// Concerned Destination is valid only in the ASP-to-SGP direction.
type SignallingCongestionRequest struct {
	Scope        WireScope
	Destinations []PointCodeRange

	CongestionLevel    uint8
	CongestionLevelSet bool

	ConcernedDestination    uint32
	ConcernedDestinationSet bool
	Info                    string
}

// DestinationAvailabilityRequest is an SGP's own statement about whether one or
// more SS7 destinations are reachable: the RFC 4666 Sections 3.4.1, 3.4.2 and
// 3.4.6 DUNA, DAVA and DRST procedures. It carries the availability dimension
// only; congestion is reported through SignallingCongestion.
type DestinationAvailabilityRequest struct {
	Scope        WireScope
	Destinations []PointCodeRange
	Availability DestinationAvailability
	Info         string
}

// DestinationUserPartUnavailableRequest is an RFC 4666 Section 3.4.5 DUPU
// request from an SGP to its concerned active ASPs.
type DestinationUserPartUnavailableRequest struct {
	Scope       WireScope
	Destination PointCodeRange
	User        uint16
	Cause       uint16
	Info        string
}

// SSNMDeliveryFailure identifies one association that did not accept its
// mandatory SSNM batch.
type SSNMDeliveryFailure struct {
	Association AssociationID
	Cause       error
}

// SSNMDeliveryError reports a partial SGP fan-out. Successful associations are
// not replayed; the caller receives stable Endpoint-local identities for both
// outcomes.
type SSNMDeliveryError struct {
	Successful []AssociationID
	Failed     []SSNMDeliveryFailure
}

func (e *SSNMDeliveryError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("SSNM delivery failed for %d association(s)", len(e.Failed))
}

// Unwrap exposes every association write cause to errors.Is and errors.As.
func (e *SSNMDeliveryError) Unwrap() []error {
	if e == nil {
		return nil
	}
	causes := make([]error, 0, len(e.Failed))
	for _, failure := range e.Failed {
		if failure.Cause != nil {
			causes = append(causes, failure.Cause)
		}
	}
	return causes
}

type ssnmParameters struct {
	networkAppearance *params.Param
	routingContext    *params.Param
	affectedPointCode *params.Param
	info              *params.Param
}

// DestinationStateAudit originates one DAUD on an active ASP Association.
// RFC 4666 Section 4.5.3 permits the ASP to audit destination state after it
// becomes active.
func (c *Association) DestinationStateAudit(request DestinationStateAuditRequest) error {
	parameters, err := c.prepareASPSSNM(request.Scope, request.Destinations, request.Info)
	if err != nil {
		return err
	}
	if _, err = c.WriteSignal(messages.NewDestinationStateAudit(
		parameters.networkAppearance,
		parameters.routingContext,
		parameters.affectedPointCode,
		parameters.info,
	)); err != nil {
		return err
	}
	// The audit is on the wire. A local observability bound that refuses to
	// record it does not un-send it, and the refusal is already reported
	// through the SSNM subscription, so it is not this call's failure.
	_ = c.publishSSNMReport(SSNMReport{
		Kind:         SSNMDestinationStateAuditReport,
		Source:       SSNMLocalReport,
		Scope:        request.Scope.clone(),
		Destinations: append([]PointCodeRange(nil), request.Destinations...),
	})
	return nil
}

// SignallingCongestion originates the optional ASP-to-SGP SCON described by
// RFC 4666 Section 3.4.4.
func (c *Association) SignallingCongestion(request SignallingCongestionRequest) error {
	parameters, err := c.prepareASPSSNM(request.Scope, request.Destinations, request.Info)
	if err != nil {
		return err
	}
	concernedDestination, congestion, err := buildSignallingCongestionParameters(request, true)
	if err != nil {
		return err
	}
	if _, err = c.WriteSignal(messages.NewSignallingCongestion(
		parameters.networkAppearance,
		parameters.routingContext,
		parameters.affectedPointCode,
		concernedDestination,
		congestion,
		parameters.info,
	)); err != nil {
		return err
	}
	// An ASP-originated SCON reports "the congestion level of the M3UA layer
	// or the ASP" (RFC 4666 Section 3.4.4). It describes this node, not a
	// destination beyond a peer.
	_ = c.publishSSNMReport(SSNMReport{
		Kind:                    SSNMSignallingCongestionReport,
		Source:                  SSNMLocalReport,
		Scope:                   request.Scope.clone(),
		Destinations:            append([]PointCodeRange(nil), request.Destinations...),
		CongestionLevel:         request.CongestionLevel,
		CongestionLevelSet:      request.CongestionLevelSet,
		ConcernedDestination:    request.ConcernedDestination,
		ConcernedDestinationSet: request.ConcernedDestinationSet,
		PeerReported:            true,
	})
	return nil
}

// SignallingCongestion records an SGP destination congestion state before it
// is delivered to concerned active ASPs. RFC 4666 Sections 3.4.4 and 4.5.1
// make an explicit level zero congestion abatement; an omitted level remains a
// congestion report.
//
// Congestion is recorded against the availability the SG already holds. RFC
// 4666 Section 4.5.2.2 makes availability and congestion two separate statuses
// of the same destination, so neither a congestion report nor the explicit
// level zero that abates it returns an unavailable destination to service —
// only DAVA does, and Section 4.5.3 has the audit answered accordingly.
func (e *Endpoint) SignallingCongestion(request SignallingCongestionRequest) error {
	if e == nil || e.role != RoleSGP {
		return ErrUnsupportedRole
	}
	if !e.beginOperation() {
		return ErrEndpointClosed
	}
	defer e.endOperation()

	parameters, err := prepareEndpointSSNM(e, request.Scope, request.Destinations, request.Info)
	if err != nil {
		return err
	}
	_, congestion, err := buildSignallingCongestionParameters(request, false)
	if err != nil {
		return err
	}
	return publishEndpointDestinationState(e, request.Scope, request.Destinations,
		DestinationNetworkState{
			Congestion: congestionStateFor(request.CongestionLevel, request.CongestionLevelSet),
		},
		destinationCongestionDimension,
		func(routingContext, affectedPointCode *params.Param) messages.M3UA {
			return messages.NewSignallingCongestion(
				parameters.networkAppearance.Copy(),
				routingContext,
				affectedPointCode,
				nil,
				congestion.Copy(),
				parameters.info.Copy(),
			)
		})
}

// ReportDestinationAvailability publishes this SGP's own view of one or more SS7
// destinations' reachability to every concerned active ASP the Endpoint owns,
// and records it as the state a later RFC 4666 Section 4.5.3 audit is answered
// from.
//
// The SG's own report is authoritative for that record: Sections 3.4.1 and
// 3.4.2 have the SG state what it "has determined", and Section 4.5.3 has it
// answer an audit from exactly that. The report moves the availability
// dimension alone; Section 4.5.2.2 keeps congestion separate, so a destination
// returning to service does not silently become uncongested and a congested one
// does not become unreachable.
//
// Destinations inside an MTP3 restart are staged instead of published, in the
// availability dimension only, and are released by MTP3Restart.Complete
// (Section 4.6).
//
// A fan-out that fails for some ASPs returns an *SSNMDeliveryError naming both
// outcomes; the successful associations are never replayed.
func (e *Endpoint) ReportDestinationAvailability(request DestinationAvailabilityRequest) error {
	if e == nil || e.role != RoleSGP {
		return ErrUnsupportedRole
	}
	if !e.beginOperation() {
		return ErrEndpointClosed
	}
	defer e.endOperation()

	if !validDestinationAvailability(request.Availability) {
		return fmt.Errorf("%w: destination availability %d",
			ErrInvalidParameterValue, request.Availability)
	}
	parameters, err := prepareEndpointSSNM(e, request.Scope, request.Destinations, request.Info)
	if err != nil {
		return err
	}
	availability := request.Availability
	return publishEndpointDestinationState(e, request.Scope, request.Destinations,
		DestinationNetworkState{Availability: availability},
		destinationAvailabilityDimension,
		func(routingContext, affectedPointCode *params.Param) messages.M3UA {
			// The scope on the wire is the exact scope the caller named. An
			// omitted Network Appearance is omitted here too: RFC 4666 Section
			// 1.4.2.1 makes it a local value, and the resolved appearance is
			// what keys the record, not what the peer is told.
			switch availability {
			case DestinationUnavailable:
				return messages.NewDestinationUnavailable(
					parameters.networkAppearance.Copy(), routingContext,
					affectedPointCode, parameters.info.Copy(),
				)
			case DestinationRestricted:
				return messages.NewDestinationRestricted(
					parameters.networkAppearance.Copy(), routingContext,
					affectedPointCode, parameters.info.Copy(),
				)
			default:
				return messages.NewDestinationAvailable(
					parameters.networkAppearance.Copy(), routingContext,
					affectedPointCode, parameters.info.Copy(),
				)
			}
		})
}

// endpointSSNMGroup is one Routing Context scope together with the destinations
// that are still publishable in it. A destination inside an MTP3 restart is
// staged rather than published, and the restart is scoped, so two Routing
// Contexts of the same request can end up with different destination lists.
type endpointSSNMGroup struct {
	routingContexts []uint32
	destinations    []PointCodeRange
}

// appendEndpointSSNMGroup coalesces Routing Contexts that end up with the same
// publishable destinations, so a request that no restart has split still goes
// out as one message naming every context it asked for.
func appendEndpointSSNMGroup(
	groups []endpointSSNMGroup,
	group endpointSSNMGroup,
	scoped bool,
) []endpointSSNMGroup {
	if !scoped {
		return append(groups, group)
	}
	for index := range groups {
		if !samePointCodeRanges(groups[index].destinations, group.destinations) {
			continue
		}
		groups[index].routingContexts = append(groups[index].routingContexts, group.routingContexts...)
		return groups
	}
	return append(groups, group)
}

func samePointCodeRanges(first, second []PointCodeRange) bool {
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

// publishEndpointDestinationState is the owner-level publication path shared by
// the SGP's availability and congestion statements. It records the dimension
// being reported, holds back whatever an MTP3 restart has isolated, and fans the
// rest out to the concerned active ASPs of every Listener and dialed
// Association the Endpoint owns.
func publishEndpointDestinationState(
	endpoint *Endpoint,
	scope WireScope,
	destinations []PointCodeRange,
	state DestinationNetworkState,
	dimensions destinationDimensions,
	build func(routingContext, affectedPointCode *params.Param) messages.M3UA,
) error {
	storageScope := scope
	if !storageScope.NetworkAppearanceSet {
		networkAppearance, networkAppearanceSet, err :=
			resolveEndpointSSNMNetworkAppearance(endpoint, scope)
		if err != nil {
			return err
		}
		storageScope.NetworkAppearance, storageScope.NetworkAppearanceSet =
			networkAppearance, networkAppearanceSet
	}

	restarts := endpoint.mtp3Restarts
	if restarts != nil {
		// Held across the stage-or-publish decision and the write, which is what
		// keeps a destination from being published by this call and completed by
		// a concurrent MTP3Restart.Complete at the same time.
		restarts.procedureMu.RLock()
		defer restarts.procedureMu.RUnlock()
	}

	groups, retained := stageOrGroupEndpointSSNM(
		endpoint, restarts, storageScope, scope, destinations, state, dimensions,
	)
	// An SG that cannot retain the report must not deliver it either: the audit
	// it owes its ASPs afterwards would contradict what it had just sent.
	if retained != nil {
		return retained
	}

	failure := &SSNMDeliveryError{}
	for _, group := range groups {
		if len(group.destinations) == 0 {
			continue
		}
		affectedPointCode, err := buildAffectedPointCodes(group.destinations)
		if err != nil {
			return err
		}
		groupScope := scope
		if len(group.routingContexts) > 0 {
			groupScope.RoutingContexts = group.routingContexts
			groupScope.RoutingContextSet = true
		}
		mergeSSNMDeliveryOutcome(failure, fanoutEndpointSSNM(
			endpoint, groupScope, func(routingContext *params.Param) messages.M3UA {
				return build(routingContext, affectedPointCode.Copy())
			},
		))
	}
	return ssnmDeliveryResult(failure)
}

// stageOrGroupEndpointSSNM records the publishable part of a statement and
// stages the part an MTP3 restart has isolated, returning the groups still to be
// published.
func stageOrGroupEndpointSSNM(
	endpoint *Endpoint,
	restarts *mtp3RestartRegistry,
	storageScope, scope WireScope,
	destinations []PointCodeRange,
	state DestinationNetworkState,
	dimensions destinationDimensions,
) ([]endpointSSNMGroup, error) {
	routingContexts := []uint32{0}
	scoped := scope.RoutingContextSet && len(scope.RoutingContexts) > 0
	if scoped {
		routingContexts = scope.RoutingContexts
	}

	groups := make([]endpointSSNMGroup, 0, len(routingContexts))
	records := make([]destinationRange, 0, len(destinations)*len(routingContexts))
	for _, routingContext := range routingContexts {
		group := endpointSSNMGroup{destinations: make([]PointCodeRange, 0, len(destinations))}
		if scoped {
			group.routingContexts = []uint32{routingContext}
		}
		for _, destination := range destinations {
			rangeValue := normalizeDestinationRange(destinationRange{
				NetworkAppearance:    storageScope.NetworkAppearance,
				NetworkAppearanceSet: storageScope.NetworkAppearanceSet,
				RoutingContext:       routingContext,
				RoutingContextSet:    scoped,
				PointCode:            destination.PointCode,
				Mask:                 destination.Mask,
				State:                state,
			})
			if stageAnyMTP3RestartRangeLocked(restarts, stagedDestination{
				rangeValue: rangeValue,
				dimensions: dimensions,
			}) {
				continue
			}
			group.destinations = append(group.destinations, destination)
			records = append(records, rangeValue)
		}
		groups = appendEndpointSSNMGroup(groups, group, scoped)
	}

	if len(records) == 0 {
		return groups, nil
	}
	endpoint.destinations.mu.Lock()
	if endpoint.destinations.state == nil {
		endpoint.destinations.state = make(map[destinationKey]destinationRecord)
	}
	refused := 0
	for _, rangeValue := range records {
		if !endpoint.destinations.storeLocked(destinationRecord{
			rangeValue: rangeValue,
			dimensions: dimensions,
		}) {
			refused++
		}
	}
	err := endpoint.destinations.recordLimitErrorLocked(refused)
	endpoint.destinations.mu.Unlock()
	return groups, err
}

// DestinationUserPartUnavailable originates RFC 4666 Section 3.4.5 DUPU from
// an SGP to concerned active ASPs. DUPU does not change destination
// reachability, so no destination-state record is written.
func (e *Endpoint) DestinationUserPartUnavailable(request DestinationUserPartUnavailableRequest) error {
	if e == nil || e.role != RoleSGP {
		return ErrUnsupportedRole
	}
	if !e.beginOperation() {
		return ErrEndpointClosed
	}
	defer e.endOperation()

	if request.Destination.Mask != 0 {
		return fmt.Errorf("%w: DUPU does not permit an Affected Point Code mask", ErrInvalidParameterValue)
	}
	if !validMTP3User(request.User) || request.Cause > params.Inaccessible {
		return fmt.Errorf("%w: invalid DUPU User/Cause", ErrInvalidParameterValue)
	}
	parameters, err := prepareEndpointSSNM(
		e, request.Scope, []PointCodeRange{request.Destination}, request.Info,
	)
	if err != nil {
		return err
	}
	return ssnmDeliveryResult(fanoutEndpointSSNM(e, request.Scope, func(routingContext *params.Param) messages.M3UA {
		return messages.NewDestinationUserPartUnavailable(
			parameters.networkAppearance.Copy(),
			routingContext,
			parameters.affectedPointCode.Copy(),
			params.NewUserCause(request.User, request.Cause),
			parameters.info.Copy(),
		)
	}))
}

func (c *Association) prepareASPSSNM(
	scope WireScope,
	destinations []PointCodeRange,
	info string,
) (ssnmParameters, error) {
	if c == nil {
		return ssnmParameters{}, ErrAssociationClosed
	}
	if c.role != RoleASP {
		return ssnmParameters{}, ErrUnsupportedRole
	}
	select {
	case <-c.done:
		if err := c.Err(); err != nil {
			return ssnmParameters{}, err
		}
		return ssnmParameters{}, ErrAssociationClosed
	default:
	}
	if c.State() != StateASPActive {
		return ssnmParameters{}, ErrInvalidState
	}

	parameters, err := buildSSNMParameters(scope, destinations, info)
	if err != nil {
		return ssnmParameters{}, err
	}
	if err := c.validateOutboundASPSSNMScope(scope, parameters.routingContext); err != nil {
		return ssnmParameters{}, err
	}
	return parameters, nil
}

func prepareEndpointSSNM(
	endpoint *Endpoint,
	scope WireScope,
	destinations []PointCodeRange,
	info string,
) (ssnmParameters, error) {
	parameters, err := buildSSNMParameters(scope, destinations, info)
	if err != nil {
		return ssnmParameters{}, err
	}
	if err := validateEndpointSSNMScope(endpoint, scope); err != nil {
		return ssnmParameters{}, err
	}
	return parameters, nil
}

func buildSSNMParameters(
	scope WireScope,
	destinations []PointCodeRange,
	info string,
) (ssnmParameters, error) {
	networkAppearance, routingContext, err := buildWireScope(scope)
	if err != nil {
		return ssnmParameters{}, err
	}
	affectedPointCode, err := buildAffectedPointCodes(destinations)
	if err != nil {
		return ssnmParameters{}, err
	}
	infoString, err := buildSSNMInfoString(info)
	if err != nil {
		return ssnmParameters{}, err
	}
	return ssnmParameters{
		networkAppearance: networkAppearance,
		routingContext:    routingContext,
		affectedPointCode: affectedPointCode,
		info:              infoString,
	}, nil
}

func buildWireScope(scope WireScope) (*params.Param, *params.Param, error) {
	var networkAppearance *params.Param
	if scope.NetworkAppearanceSet {
		networkAppearance = params.NewNetworkAppearance(scope.NetworkAppearance)
	}
	if !scope.RoutingContextSet {
		return networkAppearance, nil, nil
	}
	if len(scope.RoutingContexts) == 0 {
		return nil, nil, ErrMissingRoutingContext
	}
	routingContexts := append([]uint32(nil), scope.RoutingContexts...)
	sort.Slice(routingContexts, func(i, j int) bool { return routingContexts[i] < routingContexts[j] })
	for index := 1; index < len(routingContexts); index++ {
		if routingContexts[index] == routingContexts[index-1] {
			return nil, nil, fmt.Errorf("%w: duplicate Routing Context %d",
				ErrInvalidParameterValue, routingContexts[index])
		}
	}
	return networkAppearance, params.NewRoutingContext(routingContexts...), nil
}

func buildAffectedPointCodes(destinations []PointCodeRange) (*params.Param, error) {
	if len(destinations) == 0 {
		return nil, ErrMissingAffectedPointCode
	}
	encoded := make([]uint32, len(destinations))
	for index, destination := range destinations {
		if destination.PointCode > 0x00ffffff || destination.Mask > 24 {
			return nil, fmt.Errorf("%w: invalid Affected Point Code %#x/%d",
				ErrInvalidParameterValue, destination.PointCode, destination.Mask)
		}
		encoded[index] = uint32(destination.Mask)<<24 | destination.PointCode
	}
	return params.NewAffectedPointCode(encoded...), nil
}

func buildSSNMInfoString(info string) (*params.Param, error) {
	if len(info) > 255 || !utf8.ValidString(info) {
		return nil, fmt.Errorf("%w: Info String must be valid UTF-8 and at most 255 octets",
			ErrInvalidParameterValue)
	}
	if info == "" {
		return nil, nil
	}
	return params.NewInfoString(info), nil
}

func buildSignallingCongestionParameters(
	request SignallingCongestionRequest,
	allowConcernedDestination bool,
) (*params.Param, *params.Param, error) {
	var concernedDestination *params.Param
	if request.ConcernedDestinationSet {
		if !allowConcernedDestination || request.ConcernedDestination > 0x00ffffff {
			return nil, nil, fmt.Errorf("%w: invalid Concerned Destination",
				ErrInvalidParameterValue)
		}
		concernedDestination = params.NewConcernedDestination(request.ConcernedDestination)
	}
	var congestion *params.Param
	if request.CongestionLevelSet {
		if request.CongestionLevel > 3 {
			return nil, nil, fmt.Errorf("%w: congestion level %d",
				ErrInvalidParameterValue, request.CongestionLevel)
		}
		congestion = params.NewCongestionIndications(request.CongestionLevel)
	}
	return concernedDestination, congestion, nil
}

func (c *Association) validateOutboundASPSSNMScope(scope WireScope, routingContext *params.Param) error {
	if err := c.validateSSNMRoutingContext(routingContext); err != nil {
		return err
	}
	configured := c.configuredLocalASKeysForStatus()
	if len(configured) == 0 {
		return ErrNoConfiguredAS
	}

	requested, err := outboundSSNMASKeys(scope, configured)
	if err != nil {
		return err
	}
	for _, key := range requested {
		if !c.activeForASKey(key) {
			return ErrInvalidState
		}
	}
	return nil
}

func outboundSSNMASKeys(scope WireScope, configured []ASKey) ([]ASKey, error) {
	if !scope.RoutingContextSet {
		if len(configured) != 1 {
			return nil, ErrMissingRoutingContext
		}
		key := configured[0]
		if scope.NetworkAppearanceSet &&
			(key.NetworkAppearanceSet != scope.NetworkAppearanceSet ||
				key.NetworkAppearance != scope.NetworkAppearance) {
			return nil, ErrInvalidNetworkAppearance
		}
		return []ASKey{key}, nil
	}

	requested := make([]ASKey, 0, len(scope.RoutingContexts))
	for _, routingContext := range scope.RoutingContexts {
		candidates := make([]ASKey, 0, 1)
		for _, key := range configured {
			if key.RoutingContextSet && key.RoutingContext == routingContext {
				candidates = append(candidates, key)
			}
		}
		if len(candidates) == 0 {
			return nil, NewInvalidRoutingContextError(routingContext)
		}
		if scope.NetworkAppearanceSet {
			matched := false
			for _, key := range candidates {
				if key.NetworkAppearanceSet && key.NetworkAppearance == scope.NetworkAppearance {
					requested = append(requested, key)
					matched = true
					break
				}
			}
			if !matched {
				return nil, ErrInvalidNetworkAppearance
			}
			continue
		}
		if len(candidates) != 1 {
			return nil, ErrInvalidNetworkAppearance
		}
		requested = append(requested, candidates[0])
	}
	return requested, nil
}

func validateEndpointSSNMScope(endpoint *Endpoint, scope WireScope) error {
	if endpoint == nil || endpoint.as == nil {
		return nil
	}
	configured := endpoint.as.keys()
	if len(configured) == 0 {
		return nil
	}
	if !scope.RoutingContextSet {
		if scope.NetworkAppearanceSet {
			for _, key := range configured {
				if key.NetworkAppearanceSet && key.NetworkAppearance == scope.NetworkAppearance {
					return nil
				}
			}
			return ErrInvalidNetworkAppearance
		}
		_, _, err := resolveEndpointSSNMNetworkAppearance(endpoint, scope)
		return err
	}

	for _, routingContext := range scope.RoutingContexts {
		candidates := make([]ASKey, 0, 1)
		for _, key := range configured {
			if key.RoutingContextSet && key.RoutingContext == routingContext {
				candidates = append(candidates, key)
			}
		}
		if len(candidates) == 0 {
			return NewInvalidRoutingContextError(routingContext)
		}
		if scope.NetworkAppearanceSet {
			matched := false
			for _, key := range candidates {
				if key.NetworkAppearanceSet && key.NetworkAppearance == scope.NetworkAppearance {
					matched = true
					break
				}
			}
			if !matched {
				return ErrInvalidNetworkAppearance
			}
			continue
		}
		if len(candidates) != 1 {
			return ErrInvalidNetworkAppearance
		}
	}
	_, _, err := resolveEndpointSSNMNetworkAppearance(endpoint, scope)
	return err
}

func resolveEndpointSSNMNetworkAppearance(
	endpoint *Endpoint,
	scope WireScope,
) (uint32, bool, error) {
	if scope.NetworkAppearanceSet {
		return scope.NetworkAppearance, true, nil
	}
	if endpoint == nil || endpoint.as == nil {
		return 0, false, nil
	}
	configured := endpoint.as.keys()
	resolved := false
	var networkAppearance uint32
	var networkAppearanceSet bool
	for _, key := range configured {
		if scope.RoutingContextSet &&
			(!key.RoutingContextSet || !containsRoutingContext(scope.RoutingContexts, key.RoutingContext)) {
			continue
		}
		if !resolved {
			networkAppearance = key.NetworkAppearance
			networkAppearanceSet = key.NetworkAppearanceSet
			resolved = true
			continue
		}
		if networkAppearanceSet != key.NetworkAppearanceSet ||
			networkAppearance != key.NetworkAppearance {
			return 0, false, ErrInvalidNetworkAppearance
		}
	}
	if !resolved {
		return 0, false, nil
	}
	return networkAppearance, networkAppearanceSet, nil
}

// fanoutEndpointSSNM writes one built message to every concerned active ASP and
// reports the outcome for all of them. The result is always non-nil, so a caller
// combining several fan-outs keeps the successful associations of each; callers
// turn it into an error with ssnmDeliveryResult.
func fanoutEndpointSSNM(
	endpoint *Endpoint,
	scope WireScope,
	build func(*params.Param) messages.M3UA,
) *SSNMDeliveryError {
	deliveryError := &SSNMDeliveryError{}
	targets := endpointActiveSSNMTargets(endpoint, scope)
	if len(targets) == 0 {
		return deliveryError
	}
	type delivery struct {
		association *Association
		messages    []messages.M3UA
	}
	deliveries := make([]delivery, 0, len(targets))
	for _, target := range targets {
		batch := delivery{association: target.association}
		for _, routingContexts := range ssnmTargetRoutingContextScopes(target) {
			var routingContext *params.Param
			if len(routingContexts) > 0 {
				routingContext = params.NewRoutingContext(routingContexts...)
			}
			batch.messages = append(batch.messages, build(routingContext))
		}
		if len(batch.messages) > 0 {
			deliveries = append(deliveries, batch)
		}
	}

	results := make([]error, len(deliveries))
	var waitGroup sync.WaitGroup
	waitGroup.Add(len(deliveries))
	for index := range deliveries {
		index := index
		go func() {
			defer waitGroup.Done()
			results[index] = deliveries[index].association.writeMandatoryControls(
				deliveries[index].messages, true, true,
			)
		}()
	}
	waitGroup.Wait()

	for index, err := range results {
		associationID := deliveries[index].association.ID()
		if err == nil {
			deliveryError.Successful = append(deliveryError.Successful, associationID)
			continue
		}
		deliveryError.Failed = append(deliveryError.Failed, SSNMDeliveryFailure{
			Association: associationID,
			Cause:       err,
		})
	}
	return deliveryError
}

func endpointActiveSSNMTargets(endpoint *Endpoint, scope WireScope) []activeSSNMTarget {
	if endpoint == nil || endpoint.as == nil {
		return nil
	}
	networkAppearance, networkAppearanceSet, err :=
		resolveEndpointSSNMNetworkAppearance(endpoint, scope)
	if err != nil {
		return nil
	}
	base := destinationKey{
		networkAppearance:    networkAppearance,
		networkAppearanceSet: networkAppearanceSet,
	}
	if !scope.RoutingContextSet {
		return endpoint.as.activeSSNMTargets(base)
	}

	targetsByAssociation := make(map[*Association]*activeSSNMTarget)
	for _, routingContext := range scope.RoutingContexts {
		scoped := base
		scoped.routingContext = routingContext
		scoped.routingContextSet = true
		for _, target := range endpoint.as.activeSSNMTargets(scoped) {
			combined := targetsByAssociation[target.association]
			if combined == nil {
				combined = &activeSSNMTarget{association: target.association}
				targetsByAssociation[target.association] = combined
			}
			combined.routingContexts = appendRoutingContexts(
				combined.routingContexts, target.routingContexts,
			)
			combined.contextless = combined.contextless || target.contextless
		}
	}
	targets := make([]activeSSNMTarget, 0, len(targetsByAssociation))
	for _, target := range targetsByAssociation {
		targets = append(targets, *target)
	}
	sort.Slice(targets, func(i, j int) bool {
		return targets[i].association.ID() < targets[j].association.ID()
	})
	return targets
}

func validMTP3User(user uint16) bool {
	switch user {
	case params.SCCP,
		params.TUP,
		params.ISUP,
		params.BroadbandISUP,
		params.SatelliteISUP,
		params.AAL2Signalling,
		params.BICC,
		params.GatewayControlProtocol:
		return true
	default:
		return false
	}
}
