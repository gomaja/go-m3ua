package m3ua

import (
	"fmt"
	"sort"
	"sync"
	"unicode/utf8"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// SSNMScope is the Network Appearance and Application Server scope carried by
// an RFC 4666 Section 3.4 Signalling Network Management message.
type SSNMScope struct {
	NetworkAppearance    uint32
	NetworkAppearanceSet bool
	RoutingContexts      []uint32
	RoutingContextSet    bool
}

// DestinationStateAuditRequest is an RFC 4666 Sections 3.4.3 and 4.5.3 DAUD
// request from an ASP to an SGP.
type DestinationStateAuditRequest struct {
	Scope        SSNMScope
	Destinations []PointCodeRange
	Info         string
}

// SignallingCongestionRequest is an RFC 4666 Section 3.4.4 SCON request.
// Concerned Destination is valid only in the ASP-to-SGP direction.
type SignallingCongestionRequest struct {
	Scope        SSNMScope
	Destinations []PointCodeRange

	CongestionLevel    uint8
	CongestionLevelSet bool

	ConcernedDestination    uint32
	ConcernedDestinationSet bool
	Info                    string
}

// DestinationUserPartUnavailableRequest is an RFC 4666 Section 3.4.5 DUPU
// request from an SGP to its concerned active ASPs.
type DestinationUserPartUnavailableRequest struct {
	Scope       SSNMScope
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
	_, err = c.WriteSignal(messages.NewDestinationStateAudit(
		parameters.networkAppearance,
		parameters.routingContext,
		parameters.affectedPointCode,
		parameters.info,
	))
	return err
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
	_, err = c.WriteSignal(messages.NewSignallingCongestion(
		parameters.networkAppearance,
		parameters.routingContext,
		parameters.affectedPointCode,
		concernedDestination,
		congestion,
		parameters.info,
	))
	return err
}

// SignallingCongestion records an SGP destination congestion state before it
// is delivered to concerned active ASPs. RFC 4666 Sections 3.4.4 and 4.5.1
// make an explicit level zero congestion abatement; an omitted level remains a
// congestion report.
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

	state := DestinationCongested
	if request.CongestionLevelSet && request.CongestionLevel == 0 {
		state = DestinationAvailable
	}
	storageScope := request.Scope
	if !storageScope.NetworkAppearanceSet {
		storageScope.NetworkAppearance, storageScope.NetworkAppearanceSet, err =
			resolveEndpointSSNMNetworkAppearance(e, request.Scope)
		if err != nil {
			return err
		}
	}
	ranges := destinationRangesForSSNM(
		storageScope, request.Destinations, state,
		request.CongestionLevel, request.CongestionLevelSet,
	)
	if request.Scope.RoutingContextSet {
		e.destinations.setScopedRanges(request.Scope.RoutingContexts, ranges)
	} else {
		e.destinations.setRanges(ranges)
	}
	return fanoutEndpointSSNM(e, request.Scope, func(routingContext *params.Param) messages.M3UA {
		return messages.NewSignallingCongestion(
			parameters.networkAppearance.Copy(),
			routingContext,
			parameters.affectedPointCode.Copy(),
			nil,
			congestion.Copy(),
			parameters.info.Copy(),
		)
	})
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
	return fanoutEndpointSSNM(e, request.Scope, func(routingContext *params.Param) messages.M3UA {
		return messages.NewDestinationUserPartUnavailable(
			parameters.networkAppearance.Copy(),
			routingContext,
			parameters.affectedPointCode.Copy(),
			params.NewUserCause(request.User, request.Cause),
			parameters.info.Copy(),
		)
	})
}

func (c *Association) prepareASPSSNM(
	scope SSNMScope,
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
	scope SSNMScope,
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
	scope SSNMScope,
	destinations []PointCodeRange,
	info string,
) (ssnmParameters, error) {
	networkAppearance, routingContext, err := buildSSNMScope(scope)
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

func buildSSNMScope(scope SSNMScope) (*params.Param, *params.Param, error) {
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

func (c *Association) validateOutboundASPSSNMScope(scope SSNMScope, routingContext *params.Param) error {
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

func outboundSSNMASKeys(scope SSNMScope, configured []ASKey) ([]ASKey, error) {
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

func validateEndpointSSNMScope(endpoint *Endpoint, scope SSNMScope) error {
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
	scope SSNMScope,
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

func destinationRangesForSSNM(
	scope SSNMScope,
	destinations []PointCodeRange,
	state DestinationState,
	congestionLevel uint8,
	congestionLevelSet bool,
) []DestinationRange {
	ranges := make([]DestinationRange, len(destinations))
	for index, destination := range destinations {
		ranges[index] = DestinationRange{
			NetworkAppearance:    scope.NetworkAppearance,
			NetworkAppearanceSet: scope.NetworkAppearanceSet,
			PointCode:            destination.PointCode,
			Mask:                 destination.Mask,
			State:                state,
			CongestionLevel:      congestionLevel,
			CongestionLevelSet:   congestionLevelSet,
		}
	}
	return ranges
}

func fanoutEndpointSSNM(
	endpoint *Endpoint,
	scope SSNMScope,
	build func(*params.Param) messages.M3UA,
) error {
	targets := endpointActiveSSNMTargets(endpoint, scope)
	if len(targets) == 0 {
		return nil
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

	deliveryError := &SSNMDeliveryError{}
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
	if len(deliveryError.Failed) == 0 {
		return nil
	}
	return deliveryError
}

func endpointActiveSSNMTargets(endpoint *Endpoint, scope SSNMScope) []activeSSNMTarget {
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
