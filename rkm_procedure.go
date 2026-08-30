package m3ua

import (
	"context"
	"fmt"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

const (
	rkmAwaitingNone uint32 = iota
	rkmAwaitingRegistrationResponse
	rkmAwaitingDeregistrationResponse
)

// RoutingKeyRegistration describes one Routing Key to register. A requested
// Routing Context is an RFC 4666 re-registration request; omission asks the
// peer to select the Routing Context.
type RoutingKeyRegistration struct {
	RoutingKey              RoutingKey
	RequestedRoutingContext uint32
	RoutingContextRequested bool
}

// RegisterRoutingKeys performs the RFC 4666 Section 4.4.1 Registration
// procedure and returns one result in input order for every Routing Key.
func (c *Association) RegisterRoutingKeys(ctx context.Context, registrations ...RoutingKeyRegistration) ([]RoutingKeyRegistrationResult, error) {
	if c == nil || (c.role != RoleASP && c.role != RoleIPSP) {
		return nil, ErrUnsupportedRole
	}
	if len(registrations) == 0 {
		return nil, fmt.Errorf("registration request requires at least one routing key")
	}
	if c.rkmRequesterState() == StateASPDown {
		return nil, ErrNotEstablished
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	c.rkmRequestMu.Lock()
	defer c.rkmRequestMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	requests := make([]RoutingKeyRegistrationRequest, len(registrations))
	parameters := make([]*params.Param, len(registrations))
	pending := make(map[uint32]int, len(registrations))
	for index, registration := range registrations {
		if _, err := canonicalizeRoutingKey(registration.RoutingKey); err != nil {
			return nil, fmt.Errorf("routing key %d: %w", index, err)
		}
		identifier := c.nextLocalRoutingKeyIdentifier()
		request := RoutingKeyRegistrationRequest{
			LocalRoutingKeyIdentifier: identifier,
			RequestedRoutingContext:   registration.RequestedRoutingContext,
			RoutingContextRequested:   registration.RoutingContextRequested,
			RoutingKey:                snapshotRoutingKey(registration.RoutingKey),
		}
		parameter, err := routingKeyParameter(request)
		if err != nil {
			return nil, fmt.Errorf("routing key %d: %w", index, err)
		}
		requests[index] = request
		parameters[index] = parameter
		pending[identifier] = index
	}

	responses := c.beginRegistrationResponseCorrelation(pending)
	defer c.endRKMResponseCorrelation()
	if _, err := c.WriteSignal(messages.NewRegistrationRequest(parameters...)); err != nil {
		return nil, err
	}

	results := make([]RoutingKeyRegistrationResult, len(registrations))
	for len(pending) > 0 {
		response, err := c.waitForRKMResponse(ctx, responses, rkmAwaitingRegistrationResponse)
		if err != nil {
			return nil, err
		}
		registrationResponse := response.(*messages.RegistrationResponse)
		for _, parameter := range registrationResponse.RegistrationResults {
			payload, err := parameter.RegistrationResult()
			if err != nil {
				return nil, err
			}
			identifier := payload.LocalRoutingKeyIdentifier.LocalRoutingKeyIdentifier()
			index, expected := pending[identifier]
			if !expected {
				// RFC 4666 Sections 3.6.1 and 4.4.1 use Local-RK-Identifier
				// to correlate a REG RSP with its REG REQ. A delayed result from
				// an earlier canceled procedure does not belong to this one.
				if c.localRoutingKeyIdentifierWasIssued(identifier) {
					continue
				}
				return nil, fmt.Errorf("unexpected Registration Result Local RK Identifier %d", identifier)
			}
			result := RoutingKeyRegistrationResult{
				LocalRoutingKeyIdentifier: identifier,
				Status:                    RegistrationStatus(payload.RegistrationStatus.RegistrationStatus()),
				RoutingContext:            payload.RoutingContext.RoutingContext(),
			}
			results[index] = result
			delete(pending, identifier)
		}
	}
	for index, result := range results {
		if result.Status != RegistrationSuccessfullyRegistered && result.Status != RegistrationRoutingKeyAlreadyRegistered {
			continue
		}
		key := ASKey{
			RoutingContext:    result.RoutingContext,
			RoutingContextSet: true,
		}
		effectiveRoutingKey, _ := routingKeyWithImpliedNetworkAppearance(requests[index].RoutingKey, c.localNetworkAppearance())
		key.NetworkAppearance = effectiveRoutingKey.NetworkAppearance
		key.NetworkAppearanceSet = effectiveRoutingKey.NetworkAppearanceSet
		c.addDynamicASKey(key, effectiveRoutingKey, c.isIPSPDoubleExchange())
	}
	return results, nil
}

// DeregisterRoutingContexts performs the RFC 4666 Section 4.4.2
// Deregistration procedure and returns one result in input order.
func (c *Association) DeregisterRoutingContexts(ctx context.Context, routingContexts ...uint32) ([]RoutingKeyDeregistrationResult, error) {
	if c == nil || (c.role != RoleASP && c.role != RoleIPSP) {
		return nil, ErrUnsupportedRole
	}
	if len(routingContexts) == 0 {
		return nil, fmt.Errorf("deregistration request requires at least one routing context")
	}
	if c.rkmRequesterState() == StateASPDown {
		return nil, ErrNotEstablished
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	c.rkmRequestMu.Lock()
	defer c.rkmRequestMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	pending := make(map[uint32]int, len(routingContexts))
	for index, routingContext := range routingContexts {
		if _, duplicate := pending[routingContext]; duplicate {
			return nil, fmt.Errorf("duplicate Routing Context %d", routingContext)
		}
		pending[routingContext] = index
	}

	responses, err := c.beginDeregistrationResponseCorrelation(pending)
	if err != nil {
		return nil, err
	}
	requestWritten := false
	defer func() { c.endDeregistrationResponseCorrelation(requestWritten) }()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, err := c.WriteSignal(messages.NewDeregistrationRequest(params.NewRoutingContext(routingContexts...))); err != nil {
		return nil, err
	}
	requestWritten = true

	results := make([]RoutingKeyDeregistrationResult, len(routingContexts))
	for len(pending) > 0 {
		response, err := c.waitForRKMResponse(ctx, responses, rkmAwaitingDeregistrationResponse)
		if err != nil {
			return nil, err
		}
		deregistrationResponse := response.(*messages.DeregistrationResponse)
		for _, parameter := range deregistrationResponse.DeregistrationResults {
			payload, err := parameter.DeregistrationResult()
			if err != nil {
				return nil, err
			}
			routingContext := payload.RoutingContext.RoutingContext()
			index, expected := pending[routingContext]
			if !expected {
				return nil, fmt.Errorf("unexpected Deregistration Result Routing Context %d", routingContext)
			}
			result := RoutingKeyDeregistrationResult{
				RoutingContext: routingContext,
				Status:         DeregistrationStatus(payload.DeregistrationStatus.DeregistrationStatus()),
			}
			results[index] = result
			delete(pending, routingContext)
		}
	}
	return results, nil
}

func (c *Association) handleRegistrationRequest(message *messages.RegistrationRequest) error {
	if c.role != RoleSGP && c.role != RoleIPSP {
		return NewUnexpectedMessageError(message)
	}
	registry := c.routingKeyRegistry()
	if !registry.enabled() {
		return NewUnsupportedClassError(message)
	}
	if c.State() == StateASPDown {
		return NewUnexpectedMessageError(message)
	}
	requests := make([]RoutingKeyRegistrationRequest, len(message.RoutingKeys))
	peer := c.routingKeyPeer()
	identifiers := make(map[uint32]struct{}, len(message.RoutingKeys))
	for index, parameter := range message.RoutingKeys {
		payload, err := parameter.RoutingKey()
		if err != nil {
			return err
		}
		request, err := routingKeyFromPayload(payload)
		if err != nil {
			return err
		}
		request.Peer = peer
		if _, duplicate := identifiers[request.LocalRoutingKeyIdentifier]; duplicate {
			return fmt.Errorf(
				"%w: duplicate Local RK Identifier %d",
				ErrInvalidParameterValue,
				request.LocalRoutingKeyIdentifier,
			)
		}
		identifiers[request.LocalRoutingKeyIdentifier] = struct{}{}
		request.RoutingKey, request.NetworkAppearanceImplied = routingKeyWithImpliedNetworkAppearance(
			request.RoutingKey,
			c.applicationServerNetworkAppearance(),
		)
		requests[index] = request
	}
	results := registry.register(c, requests)
	c.rkmLifecycleMu.Lock()
	defer c.rkmLifecycleMu.Unlock()
	select {
	case <-c.done:
		if err := c.Err(); err != nil {
			return err
		}
		return ErrAssociationClosed
	default:
	}
	parameters := make([]*params.Param, len(results))
	type successfulRegistration struct {
		key        ASKey
		routingKey RoutingKey
	}
	successful := make([]successfulRegistration, 0, len(results))
	for index, result := range results {
		parameters[index] = params.NewRegistrationResult(params.NewRegistrationResultPayload(
			params.NewLocalRoutingKeyIdentifier(result.LocalRoutingKeyIdentifier),
			params.NewRegistrationStatus(registrationStatusParam(result.Status)),
			params.NewRoutingContext(result.RoutingContext),
		))
		if result.Status == RegistrationSuccessfullyRegistered || result.Status == RegistrationRoutingKeyAlreadyRegistered {
			if key, ok := registry.asKey(result.RoutingContext); ok {
				routingKey, _ := registry.routingKey(result.RoutingContext)
				if requests[index].RoutingKey.TrafficModeSet {
					routingKey.TrafficMode = requests[index].RoutingKey.TrafficMode
					routingKey.TrafficModeSet = true
				}
				successful = append(successful, successfulRegistration{key: key, routingKey: routingKey})
			}
		}
	}
	if _, err := c.WriteSignal(messages.NewRegistrationResponse(parameters...)); err != nil {
		return err
	}
	select {
	case <-c.done:
		if err := c.Err(); err != nil {
			return err
		}
		return ErrAssociationClosed
	default:
	}
	for _, registration := range successful {
		c.addDynamicASKey(registration.key, registration.routingKey, false)
		if c.as != nil {
			c.as.registerDynamicASP(c, registration.key)
		}
	}
	return nil
}

func (c *Association) handleRegistrationResponse(message *messages.RegistrationResponse) error {
	if c.role != RoleASP && c.role != RoleIPSP {
		return NewUnexpectedMessageError(message)
	}
	return c.deliverRegistrationResponse(message)
}

func (c *Association) handleDeregistrationRequest(message *messages.DeregistrationRequest) error {
	if c.role != RoleSGP && c.role != RoleIPSP {
		return NewUnexpectedMessageError(message)
	}
	registry := c.routingKeyRegistry()
	if !registry.enabled() {
		return NewUnsupportedClassError(message)
	}
	if c.State() == StateASPDown {
		return NewUnexpectedMessageError(message)
	}
	routingContexts := message.RoutingContext.RoutingContexts()
	seen := make(map[uint32]struct{}, len(routingContexts))
	for _, routingContext := range routingContexts {
		if _, duplicate := seen[routingContext]; duplicate {
			return fmt.Errorf(
				"%w: duplicate Deregistration Routing Context %d",
				ErrInvalidParameterValue,
				routingContext,
			)
		}
		seen[routingContext] = struct{}{}
	}
	results := registry.deregister(c, routingContexts)
	parameters := make([]*params.Param, len(results))
	type successfulDeregistration struct {
		key      ASKey
		removeAS bool
	}
	successful := make([]successfulDeregistration, 0, len(results))
	for index, result := range results {
		parameters[index] = params.NewDeregistrationResult(params.NewDeregResultPayload(
			params.NewRoutingContext(result.RoutingContext),
			params.NewDeregistrationStatus(deregistrationStatusParam(result.Status)),
		))
		if result.Status == DeregistrationSuccessfullyDeregistered {
			if result.asKey.RoutingContextSet {
				successful = append(successful, successfulDeregistration{key: result.asKey, removeAS: result.removeAS})
			}
		}
	}
	if _, err := c.WriteSignal(messages.NewDeregistrationResponse(parameters...)); err != nil {
		return err
	}
	for _, deregistration := range successful {
		c.removeDynamicASKey(deregistration.key.RoutingContext, false)
		if c.as != nil {
			c.as.deregisterDynamicASP(c, deregistration.key, deregistration.removeAS)
		}
	}
	return nil
}

func (c *Association) handleDeregistrationResponse(message *messages.DeregistrationResponse) error {
	if c.role != RoleASP && c.role != RoleIPSP {
		return NewUnexpectedMessageError(message)
	}
	return c.deliverDeregistrationResponse(message)
}

func (c *Association) deliverDeregistrationResponse(message *messages.DeregistrationResponse) error {
	type deregistrationResult struct {
		routingContext uint32
		status         DeregistrationStatus
		parameter      *params.Param
	}
	results := make([]deregistrationResult, len(message.DeregistrationResults))
	for index, parameter := range message.DeregistrationResults {
		payload, err := parameter.DeregistrationResult()
		if err != nil {
			return err
		}
		results[index] = deregistrationResult{
			routingContext: payload.RoutingContext.RoutingContext(),
			status:         DeregistrationStatus(payload.DeregistrationStatus.DeregistrationStatus()),
			parameter:      parameter,
		}
	}

	c.rkmCorrelationMu.Lock()

	awaiting := c.rkmAwaiting == rkmAwaitingDeregistrationResponse && c.rkmResponseChan != nil
	filtered := make([]*params.Param, 0, len(results))
	seenPending := make(map[uint32]struct{}, len(results))
	pendingStatus := make(map[uint32]DeregistrationStatus, len(results))
	staleStatus := make(map[uint32]DeregistrationStatus, len(results))
	for _, result := range results {
		if _, stale := c.rkmUnresolvedDeregistrationRCs[result.routingContext]; stale {
			if _, duplicate := staleStatus[result.routingContext]; duplicate {
				continue
			}
			staleStatus[result.routingContext] = result.status
			continue
		}
		if !awaiting {
			c.rkmCorrelationMu.Unlock()
			return NewUnexpectedMessageError(message)
		}
		if _, expected := c.rkmPendingDeregistrationRCs[result.routingContext]; expected {
			if _, duplicate := seenPending[result.routingContext]; duplicate {
				continue
			}
			seenPending[result.routingContext] = struct{}{}
			pendingStatus[result.routingContext] = result.status
			filtered = append(filtered, result.parameter.Copy())
			continue
		}
		if _, duplicate := c.rkmDeliveredDeregistrationStatus[result.routingContext]; duplicate {
			continue
		}
		c.rkmCorrelationMu.Unlock()
		return fmt.Errorf("unexpected Deregistration Result Routing Context %d", result.routingContext)
	}
	if len(filtered) > 0 {
		filteredResponse := messages.NewDeregistrationResponse(filtered...)
		select {
		case c.rkmResponseChan <- filteredResponse:
		default:
			c.rkmCorrelationMu.Unlock()
			return NewUnexpectedMessageError(message)
		}
	}

	for routingContext, status := range staleStatus {
		// RFC 4666 Section 4.4.2 makes a successful DEREG RSP the peer's
		// confirmation that this ASP was removed from the related AS. A late
		// response therefore has to repair the local dynamic scope before the
		// Routing Context becomes eligible for another procedure.
		if status == DeregistrationSuccessfullyDeregistered {
			c.removeDynamicASKey(routingContext, c.isIPSPDoubleExchange())
		}
		delete(c.rkmUnresolvedDeregistrationRCs, routingContext)
	}
	if len(filtered) > 0 {
		for routingContext, status := range pendingStatus {
			delete(c.rkmPendingDeregistrationRCs, routingContext)
			c.rkmDeliveredDeregistrationStatus[routingContext] = status
		}
	}
	c.rkmCorrelationMu.Unlock()
	return nil
}

func (c *Association) deliverRegistrationResponse(message *messages.RegistrationResponse) error {
	identifiers := make([]uint32, len(message.RegistrationResults))
	for index, parameter := range message.RegistrationResults {
		payload, err := parameter.RegistrationResult()
		if err != nil {
			return err
		}
		identifiers[index] = payload.LocalRoutingKeyIdentifier.LocalRoutingKeyIdentifier()
	}

	c.rkmCorrelationMu.Lock()
	defer c.rkmCorrelationMu.Unlock()

	pending := make([]uint32, 0, len(identifiers))
	awaiting := c.rkmAwaiting == rkmAwaitingRegistrationResponse
	for _, identifier := range identifiers {
		if awaiting {
			if _, ok := c.rkmPendingLocalIDs[identifier]; ok {
				pending = append(pending, identifier)
				continue
			}
		}
		if c.localRoutingKeyIdentifierWasIssuedLocked(identifier) {
			continue
		}
		return fmt.Errorf("unexpected Registration Result Local RK Identifier %d", identifier)
	}
	if len(pending) == 0 {
		return nil
	}
	if c.rkmResponseChan == nil {
		return NewUnexpectedMessageError(message)
	}

	select {
	case c.rkmResponseChan <- message:
		for _, identifier := range pending {
			delete(c.rkmPendingLocalIDs, identifier)
		}
		return nil
	default:
		return NewUnexpectedMessageError(message)
	}
}

func (c *Association) waitForRKMResponse(
	ctx context.Context,
	responses <-chan messages.M3UA,
	expected uint32,
) (messages.M3UA, error) {
	select {
	case response := <-responses:
		switch expected {
		case rkmAwaitingRegistrationResponse:
			if _, ok := response.(*messages.RegistrationResponse); !ok {
				return nil, fmt.Errorf("unexpected RKM response %T", response)
			}
		case rkmAwaitingDeregistrationResponse:
			if _, ok := response.(*messages.DeregistrationResponse); !ok {
				return nil, fmt.Errorf("unexpected RKM response %T", response)
			}
		}
		return response, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		if err := c.Err(); err != nil {
			return nil, err
		}
		return nil, ErrAssociationClosed
	}
}

func (c *Association) nextLocalRoutingKeyIdentifier() uint32 {
	c.rkmCorrelationMu.Lock()
	defer c.rkmCorrelationMu.Unlock()
	c.rkmNextLocalID++
	if c.rkmNextLocalID == 0 {
		c.rkmNextLocalID++
	}
	return c.rkmNextLocalID
}

func (c *Association) localRoutingKeyIdentifierWasIssued(identifier uint32) bool {
	c.rkmCorrelationMu.Lock()
	defer c.rkmCorrelationMu.Unlock()
	return c.localRoutingKeyIdentifierWasIssuedLocked(identifier)
}

func (c *Association) localRoutingKeyIdentifierWasIssuedLocked(identifier uint32) bool {
	if identifier == 0 {
		return false
	}
	// Serial-number arithmetic keeps the classification correct across the
	// uint32 wrap without retaining an unbounded history for the Association.
	return c.rkmNextLocalID-identifier < 1<<31
}

func (c *Association) beginRegistrationResponseCorrelation(pending map[uint32]int) chan messages.M3UA {
	c.rkmCorrelationMu.Lock()
	defer c.rkmCorrelationMu.Unlock()
	c.rkmResponseChan = make(chan messages.M3UA, len(pending))
	c.rkmPendingLocalIDs = make(map[uint32]struct{}, len(pending))
	for identifier := range pending {
		c.rkmPendingLocalIDs[identifier] = struct{}{}
	}
	c.rkmAwaiting = rkmAwaitingRegistrationResponse
	return c.rkmResponseChan
}

func (c *Association) beginDeregistrationResponseCorrelation(pending map[uint32]int) (chan messages.M3UA, error) {
	c.rkmCorrelationMu.Lock()
	defer c.rkmCorrelationMu.Unlock()
	for routingContext := range pending {
		if _, unresolved := c.rkmUnresolvedDeregistrationRCs[routingContext]; unresolved {
			return nil, fmt.Errorf("routing context %d: %w", routingContext, ErrDeregistrationOutcomeUnknown)
		}
	}
	c.rkmResponseChan = make(chan messages.M3UA, len(pending))
	c.rkmPendingLocalIDs = nil
	c.rkmPendingDeregistrationRCs = make(map[uint32]struct{}, len(pending))
	for routingContext := range pending {
		c.rkmPendingDeregistrationRCs[routingContext] = struct{}{}
	}
	c.rkmDeliveredDeregistrationStatus = make(map[uint32]DeregistrationStatus, len(pending))
	c.rkmAwaiting = rkmAwaitingDeregistrationResponse
	return c.rkmResponseChan, nil
}

func (c *Association) endDeregistrationResponseCorrelation(requestWritten bool) {
	c.rkmCorrelationMu.Lock()
	successful := make([]uint32, 0, len(c.rkmDeliveredDeregistrationStatus))
	for routingContext, status := range c.rkmDeliveredDeregistrationStatus {
		if status == DeregistrationSuccessfullyDeregistered {
			successful = append(successful, routingContext)
		}
	}
	if requestWritten && len(c.rkmPendingDeregistrationRCs) > 0 {
		if c.rkmUnresolvedDeregistrationRCs == nil {
			c.rkmUnresolvedDeregistrationRCs = make(map[uint32]struct{}, len(c.rkmPendingDeregistrationRCs))
		}
		for routingContext := range c.rkmPendingDeregistrationRCs {
			c.rkmUnresolvedDeregistrationRCs[routingContext] = struct{}{}
		}
	}
	c.rkmAwaiting = rkmAwaitingNone
	c.rkmPendingDeregistrationRCs = nil
	c.rkmDeliveredDeregistrationStatus = nil
	c.rkmResponseChan = nil
	c.rkmCorrelationMu.Unlock()

	for _, routingContext := range successful {
		c.removeDynamicASKey(routingContext, c.isIPSPDoubleExchange())
	}
}

func (c *Association) endRKMResponseCorrelation() {
	c.rkmCorrelationMu.Lock()
	defer c.rkmCorrelationMu.Unlock()
	c.rkmAwaiting = rkmAwaitingNone
	c.rkmPendingLocalIDs = nil
	c.rkmPendingDeregistrationRCs = nil
	c.rkmDeliveredDeregistrationStatus = nil
	c.rkmResponseChan = nil
}

func (c *Association) routingKeyRegistry() *routingKeyRegistry {
	if c == nil || c.endpoint == nil {
		return nil
	}
	return c.endpoint.routingKeys
}

func (c *Association) rkmRequesterState() State {
	if c.isIPSPDoubleExchange() {
		return c.localIPSPStateValue()
	}
	return c.State()
}

func (c *Association) routingKeyPeer() RoutingKeyPeer {
	identifier, identifierSet := c.PeerASPIdentifier()
	peerRole := RoleASP
	if c.role == RoleIPSP {
		peerRole = RoleIPSP
	}
	peer := RoutingKeyPeer{
		Role:             peerRole,
		ASPIdentifier:    identifier,
		ASPIdentifierSet: identifierSet,
	}
	if c.sctpConn != nil {
		peer.RemoteAddr = cloneSCTPAddrFromNetAddr(c.sctpConn.RemoteAddr())
	}
	return peer
}

func routingKeyParameter(request RoutingKeyRegistrationRequest) (*params.Param, error) {
	groups := make([]params.RoutingKeyGroup, len(request.RoutingKey.Groups))
	for index, group := range request.RoutingKey.Groups {
		var serviceIndicators *params.Param
		if len(group.ServiceIndicators) > 0 {
			serviceIndicators = params.NewServiceIndicators(group.ServiceIndicators...)
		}
		var originatingPointCodes *params.Param
		if len(group.OriginatingPointCodes) > 0 {
			entries := make([]params.PointCodeWithMask, len(group.OriginatingPointCodes))
			for pointCodeIndex, pointCode := range group.OriginatingPointCodes {
				entries[pointCodeIndex] = params.PointCodeWithMask{
					Mask:      pointCode.Mask,
					PointCode: pointCode.PointCode,
				}
			}
			originatingPointCodes = params.NewOriginatingPointCodeListWithMasks(entries...)
		}
		groups[index] = params.NewRoutingKeyGroup(
			params.NewDestinationPointCode(group.DestinationPointCode),
			serviceIndicators,
			originatingPointCodes,
		)
	}
	var routingContext *params.Param
	if request.RoutingContextRequested {
		routingContext = params.NewRoutingContext(request.RequestedRoutingContext)
	}
	var trafficMode *params.Param
	if request.RoutingKey.TrafficModeSet {
		trafficMode = params.NewTrafficModeType(request.RoutingKey.TrafficMode)
	}
	var networkAppearance *params.Param
	if request.RoutingKey.NetworkAppearanceSet {
		networkAppearance = params.NewNetworkAppearance(request.RoutingKey.NetworkAppearance)
	}
	parameter := params.NewRoutingKey(params.NewRoutingKeyPayloadWithGroups(
		params.NewLocalRoutingKeyIdentifier(request.LocalRoutingKeyIdentifier),
		routingContext,
		trafficMode,
		networkAppearance,
		groups...,
	))
	if _, err := parameter.MarshalBinary(); err != nil {
		return nil, err
	}
	return parameter, nil
}
