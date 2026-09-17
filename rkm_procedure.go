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
	rkmUnresolvedOutcomeLimit = 1024
)

// RoutingKeyRegistration describes one Routing Key to register. A requested
// Routing Context is an RFC 4666 re-registration request; omission asks the
// peer to select the Routing Context.
//
// RemoteAS names the canonical Application Server this Routing Key binds,
// within the Signalling Gateway of the Association's provisioned SGP. On an
// Endpoint that provisions peers it must be an Application Server that SGP
// binds dynamically; on one that does not, it is local metadata carried back
// on the result.
type RoutingKeyRegistration struct {
	RoutingKey              RoutingKey
	RemoteAS                RemoteASID
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

	if err := c.acquireRKMRequest(ctx); err != nil {
		return nil, err
	}
	defer c.releaseRKMRequest()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.rkmRequesterState() == StateASPDown {
		return nil, ErrNotEstablished
	}
	requests := make([]RoutingKeyRegistrationRequest, len(registrations))
	remoteASKeys := make([]SGASKey, len(registrations))
	parameters := make([]*params.Param, len(registrations))
	pending := make(map[uint32]int, len(registrations))
	requestsByIdentifier := make(map[uint32]RoutingKeyRegistrationRequest, len(registrations))
	for index, registration := range registrations {
		if _, err := canonicalizeRoutingKey(registration.RoutingKey); err != nil {
			return nil, fmt.Errorf("routing key %d: %w", index, err)
		}
		remoteAS, err := c.remoteASKeyFor(registration.RemoteAS)
		if err != nil {
			return nil, fmt.Errorf("routing key %d: %w", index, err)
		}
		remoteASKeys[index] = remoteAS
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
		requestsByIdentifier[identifier] = request
	}

	responses, err := c.beginRegistrationResponseCorrelation(pending, requestsByIdentifier)
	if err != nil {
		return nil, err
	}
	requestWritten := false
	completed := false
	defer func() { c.endRegistrationResponseCorrelation(requestWritten, completed) }()
	if _, err := c.WriteSignal(messages.NewRegistrationRequest(parameters...)); err != nil {
		return nil, err
	}
	requestWritten = true

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
				RemoteAS:                  remoteASKeys[index],
			}
			result.ASKey = c.registeredASKey(requests[index], result)
			results[index] = result
			delete(pending, identifier)
		}
	}
	completed = true
	applications := make([]registrationResultApplication, len(results))
	for index, result := range results {
		applications[index] = registrationResultApplication{request: requests[index], result: result}
	}
	if err := c.applyRegistrationResults(applications); err != nil {
		return nil, err
	}
	return results, nil
}

// DeregisterApplicationServers performs the RFC 4666 Section 4.4.2
// Deregistration procedure for the named Application Servers and returns one
// result in input order.
//
// Each ASKey names the Application Server to deregister by the wire scope its
// registration confirmed. A scope without a Routing Context, a scope that
// contradicts the binding this Association already holds for that Routing
// Context, and a repeated Routing Context are all refused before anything is
// submitted to the transport: RFC 4666 Section 3.6.3 carries only Routing
// Context in DEREG REQ, so the peer would act on the Routing Context whatever
// Application Server the caller believed it was naming, and a submitted
// request cannot be taken back.
func (c *Association) DeregisterApplicationServers(ctx context.Context, keys ...ASKey) ([]RoutingKeyDeregistrationResult, error) {
	if c == nil || (c.role != RoleASP && c.role != RoleIPSP) {
		return nil, ErrUnsupportedRole
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("deregistration request requires at least one Application Server")
	}
	routingContexts, err := c.confirmedDeregistrationScopes(keys)
	if err != nil {
		return nil, err
	}
	if c.rkmRequesterState() == StateASPDown {
		return nil, ErrNotEstablished
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if err := c.acquireRKMRequest(ctx); err != nil {
		return nil, err
	}
	defer c.releaseRKMRequest()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.rkmRequesterState() == StateASPDown {
		return nil, ErrNotEstablished
	}
	pending := make(map[uint32]int, len(routingContexts))
	for index, routingContext := range routingContexts {
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
				ASKey:          keys[index],
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
	registry.registrationResponseWritten(c, requests, results)
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
			if result.ASKey.RoutingContextSet {
				successful = append(successful, successfulDeregistration{key: result.ASKey, removeAS: result.removeAS})
			}
		}
	}
	if _, err := c.WriteSignal(messages.NewDeregistrationResponse(parameters...)); err != nil {
		return err
	}
	registry.deregistrationResponseWritten(c, results)
	for _, deregistration := range successful {
		c.removeDynamicASKey(deregistration.key.RoutingContext, false)
		c.forgetCanonicalRemoteAS(deregistration.key.RoutingContext)
		if c.as != nil && !c.hasStaticApplicationServerMembership(deregistration.key) {
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
	pendingStatus := make(map[uint32]DeregistrationStatus, len(results))
	staleStatus := make(map[uint32]DeregistrationStatus, len(results))
	for _, result := range results {
		if _, stale := c.rkmUnresolvedDeregistrationRCs[result.routingContext]; stale {
			if previous, duplicate := staleStatus[result.routingContext]; duplicate {
				if previous != result.status {
					c.rkmCorrelationMu.Unlock()
					return conflictingDeregistrationResultError(result.routingContext)
				}
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
			if previous, duplicate := pendingStatus[result.routingContext]; duplicate {
				if previous != result.status {
					c.rkmCorrelationMu.Unlock()
					return conflictingDeregistrationResultError(result.routingContext)
				}
				continue
			}
			pendingStatus[result.routingContext] = result.status
			filtered = append(filtered, result.parameter.Copy())
			continue
		}
		if previous, duplicate := c.rkmDeliveredDeregistrationStatus[result.routingContext]; duplicate {
			if previous != result.status {
				c.rkmCorrelationMu.Unlock()
				return conflictingDeregistrationResultError(result.routingContext)
			}
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
			c.removeRequesterRoutingKeyVersion(
				routingContext,
				c.rkmUnresolvedDeregistrationRCs[routingContext],
			)
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
	type registrationResult struct {
		result    RoutingKeyRegistrationResult
		parameter *params.Param
	}
	results := make([]registrationResult, len(message.RegistrationResults))
	for index, parameter := range message.RegistrationResults {
		payload, err := parameter.RegistrationResult()
		if err != nil {
			return err
		}
		results[index] = registrationResult{
			result: RoutingKeyRegistrationResult{
				LocalRoutingKeyIdentifier: payload.LocalRoutingKeyIdentifier.LocalRoutingKeyIdentifier(),
				Status:                    RegistrationStatus(payload.RegistrationStatus.RegistrationStatus()),
				RoutingContext:            payload.RoutingContext.RoutingContext(),
			},
			parameter: parameter,
		}
	}

	c.rkmCorrelationMu.Lock()
	awaiting := c.rkmAwaiting == rkmAwaitingRegistrationResponse && c.rkmResponseChan != nil
	filtered := make([]*params.Param, 0, len(results))
	pendingResults := make(map[uint32]RoutingKeyRegistrationResult, len(results))
	lateResults := make(map[uint32]RoutingKeyRegistrationResult, len(results))
	lateRequests := make(map[uint32]RoutingKeyRegistrationRequest, len(results))
	for _, decoded := range results {
		identifier := decoded.result.LocalRoutingKeyIdentifier
		if request, late := c.rkmUnresolvedRegistrations[identifier]; late {
			if previous, duplicate := lateResults[identifier]; duplicate {
				if previous != decoded.result {
					c.rkmCorrelationMu.Unlock()
					return conflictingRegistrationResultError(identifier)
				}
				continue
			}
			lateResults[identifier] = decoded.result
			lateRequests[identifier] = request
			continue
		}
		if awaiting {
			if _, pending := c.rkmPendingLocalIDs[identifier]; pending {
				if previous, duplicate := pendingResults[identifier]; duplicate {
					if previous != decoded.result {
						c.rkmCorrelationMu.Unlock()
						return conflictingRegistrationResultError(identifier)
					}
					continue
				}
				pendingResults[identifier] = decoded.result
				filtered = append(filtered, decoded.parameter.Copy())
				continue
			}
			if previous, duplicate := c.rkmDeliveredRegistrationResults[identifier]; duplicate {
				if previous != decoded.result {
					c.rkmCorrelationMu.Unlock()
					return conflictingRegistrationResultError(identifier)
				}
				continue
			}
		}
		if c.localRoutingKeyIdentifierWasIssuedLocked(identifier) {
			continue
		}
		c.rkmCorrelationMu.Unlock()
		return fmt.Errorf("unexpected Registration Result Local RK Identifier %d", identifier)
	}
	if len(filtered) > 0 {
		filteredResponse := messages.NewRegistrationResponse(filtered...)
		select {
		case c.rkmResponseChan <- filteredResponse:
			for identifier, result := range pendingResults {
				delete(c.rkmPendingLocalIDs, identifier)
				c.rkmDeliveredRegistrationResults[identifier] = result
			}
		default:
			c.rkmCorrelationMu.Unlock()
			return NewUnexpectedMessageError(message)
		}
	}
	for identifier := range lateResults {
		delete(c.rkmUnresolvedRegistrations, identifier)
	}
	c.rkmCorrelationMu.Unlock()

	applications := make([]registrationResultApplication, 0, len(lateResults))
	for identifier, result := range lateResults {
		applications = append(applications, registrationResultApplication{
			request: lateRequests[identifier],
			result:  result,
		})
	}
	return c.applyRegistrationResults(applications)
}

func conflictingRegistrationResultError(identifier uint32) error {
	// RFC 4666 Section 3.6.2 says one Local RK Identifier SHOULD occur in
	// only one REG RSP. Identical replay is harmless; contradictory outcomes
	// cannot be correlated safely and are an invalid parameter value.
	return fmt.Errorf(
		"%w: conflicting Registration Results for Local RK Identifier %d",
		ErrInvalidParameterValue,
		identifier,
	)
}

func conflictingDeregistrationResultError(routingContext uint32) error {
	// RFC 4666 Section 3.6.4 gives the same one-result rule to each Routing
	// Context in DEREG RSP, so contradictory duplicate statuses are invalid.
	return fmt.Errorf(
		"%w: conflicting Deregistration Results for Routing Context %d",
		ErrInvalidParameterValue,
		routingContext,
	)
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

func (c *Association) acquireRKMRequest(ctx context.Context) error {
	select {
	case c.rkmRequestGate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		if err := c.Err(); err != nil {
			return err
		}
		return ErrAssociationClosed
	}
}

func (c *Association) releaseRKMRequest() {
	<-c.rkmRequestGate
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

func (c *Association) beginRegistrationResponseCorrelation(
	pending map[uint32]int,
	requests map[uint32]RoutingKeyRegistrationRequest,
) (chan messages.M3UA, error) {
	c.rkmCorrelationMu.Lock()
	defer c.rkmCorrelationMu.Unlock()
	if c.unresolvedRKMOutcomeCountLocked()+len(pending) > rkmUnresolvedOutcomeLimit {
		return nil, ErrRKMOutcomeLimit
	}
	c.rkmResponseChan = make(chan messages.M3UA, len(pending))
	c.rkmPendingLocalIDs = make(map[uint32]struct{}, len(pending))
	c.rkmRegistrationRequests = make(map[uint32]RoutingKeyRegistrationRequest, len(requests))
	c.rkmDeliveredRegistrationResults = make(map[uint32]RoutingKeyRegistrationResult, len(requests))
	for identifier := range pending {
		c.rkmPendingLocalIDs[identifier] = struct{}{}
	}
	for identifier, request := range requests {
		c.rkmRegistrationRequests[identifier] = snapshotRoutingKeyRegistrationRequest(request)
	}
	c.rkmAwaiting = rkmAwaitingRegistrationResponse
	return c.rkmResponseChan, nil
}

func (c *Association) beginDeregistrationResponseCorrelation(pending map[uint32]int) (chan messages.M3UA, error) {
	c.rkmCorrelationMu.Lock()
	defer c.rkmCorrelationMu.Unlock()
	for routingContext := range pending {
		if _, unresolved := c.rkmUnresolvedDeregistrationRCs[routingContext]; unresolved {
			return nil, fmt.Errorf("routing context %d: %w", routingContext, ErrDeregistrationOutcomeUnknown)
		}
	}
	if c.unresolvedRKMOutcomeCountLocked()+len(pending) > rkmUnresolvedOutcomeLimit {
		return nil, ErrRKMOutcomeLimit
	}
	c.rkmResponseChan = make(chan messages.M3UA, len(pending))
	c.rkmPendingLocalIDs = nil
	c.rkmPendingDeregistrationRCs = make(map[uint32]struct{}, len(pending))
	c.rkmPendingDeregistrationVersions = make(map[uint32]uint64, len(pending))
	local := c.isIPSPDoubleExchange()
	for routingContext := range pending {
		c.rkmPendingDeregistrationRCs[routingContext] = struct{}{}
		c.rkmPendingDeregistrationVersions[routingContext] = c.dynamicASKeyVersionFor(routingContext, local)
	}
	c.rkmDeliveredDeregistrationStatus = make(map[uint32]DeregistrationStatus, len(pending))
	c.rkmAwaiting = rkmAwaitingDeregistrationResponse
	return c.rkmResponseChan, nil
}

func (c *Association) unresolvedRKMOutcomeCountLocked() int {
	return len(c.rkmUnresolvedRegistrations) + len(c.rkmUnresolvedDeregistrationRCs)
}

func (c *Association) endDeregistrationResponseCorrelation(requestWritten bool) {
	c.rkmCorrelationMu.Lock()
	type successfulDeregistration struct {
		routingContext uint32
		version        uint64
	}
	successful := make([]successfulDeregistration, 0, len(c.rkmDeliveredDeregistrationStatus))
	for routingContext, status := range c.rkmDeliveredDeregistrationStatus {
		if status == DeregistrationSuccessfullyDeregistered {
			successful = append(successful, successfulDeregistration{
				routingContext: routingContext,
				version:        c.rkmPendingDeregistrationVersions[routingContext],
			})
		}
	}
	if requestWritten && len(c.rkmPendingDeregistrationRCs) > 0 {
		if c.rkmUnresolvedDeregistrationRCs == nil {
			c.rkmUnresolvedDeregistrationRCs = make(map[uint32]uint64, len(c.rkmPendingDeregistrationRCs))
		}
		for routingContext := range c.rkmPendingDeregistrationRCs {
			c.rkmUnresolvedDeregistrationRCs[routingContext] =
				c.rkmPendingDeregistrationVersions[routingContext]
		}
	}
	c.rkmAwaiting = rkmAwaitingNone
	c.rkmPendingDeregistrationRCs = nil
	c.rkmPendingDeregistrationVersions = nil
	c.rkmDeliveredDeregistrationStatus = nil
	c.rkmResponseChan = nil
	c.rkmCorrelationMu.Unlock()

	for _, deregistration := range successful {
		c.removeRequesterRoutingKeyVersion(deregistration.routingContext, deregistration.version)
	}
}

func (c *Association) removeRequesterRoutingKeyVersion(routingContext uint32, version uint64) {
	c.rkmLifecycleMu.Lock()
	defer c.rkmLifecycleMu.Unlock()
	local := c.isIPSPDoubleExchange()
	key, removed := c.removeDynamicASKeyVersion(routingContext, local, version)
	if !local && removed {
		c.forgetCanonicalRemoteAS(routingContext)
		if c.as != nil && !c.hasStaticApplicationServerMembership(key) {
			c.as.deregisterDynamicASP(c, key, true)
		}
	}
	if removed {
		c.syncSSNMBindings()
	}
}

func (c *Association) endRegistrationResponseCorrelation(requestWritten, completed bool) {
	c.rkmCorrelationMu.Lock()
	delivered := make(map[uint32]RoutingKeyRegistrationResult, len(c.rkmDeliveredRegistrationResults))
	requests := make(map[uint32]RoutingKeyRegistrationRequest, len(c.rkmRegistrationRequests))
	for identifier, result := range c.rkmDeliveredRegistrationResults {
		delivered[identifier] = result
	}
	for identifier, request := range c.rkmRegistrationRequests {
		requests[identifier] = request
	}
	if requestWritten && !completed && len(c.rkmPendingLocalIDs) > 0 {
		if c.rkmUnresolvedRegistrations == nil {
			c.rkmUnresolvedRegistrations = make(map[uint32]RoutingKeyRegistrationRequest, len(c.rkmPendingLocalIDs))
		}
		for identifier := range c.rkmPendingLocalIDs {
			c.rkmUnresolvedRegistrations[identifier] = requests[identifier]
		}
	}
	c.rkmAwaiting = rkmAwaitingNone
	c.rkmPendingLocalIDs = nil
	c.rkmRegistrationRequests = nil
	c.rkmDeliveredRegistrationResults = nil
	c.rkmPendingDeregistrationRCs = nil
	c.rkmPendingDeregistrationVersions = nil
	c.rkmDeliveredDeregistrationStatus = nil
	c.rkmResponseChan = nil
	c.rkmCorrelationMu.Unlock()

	if completed {
		return
	}
	applications := make([]registrationResultApplication, 0, len(delivered))
	for identifier, result := range delivered {
		applications = append(applications, registrationResultApplication{
			request: requests[identifier],
			result:  result,
		})
	}
	_ = c.applyRegistrationResults(applications)
}

type registrationResultApplication struct {
	request RoutingKeyRegistrationRequest
	result  RoutingKeyRegistrationResult
}

type preparedRegistrationResult struct {
	key        ASKey
	routingKey RoutingKey
	local      bool
	// remoteAS is the canonical Application Server this registration named. It
	// is what resolves a dynamically assigned Routing Context back to the
	// identity that owns SSNM knowledge carried under it.
	remoteAS RemoteASID
}

type assignedRegistrationResultScope struct {
	key            ASKey
	trafficMode    uint32
	trafficModeSet bool
}

func (c *Association) applyRegistrationResults(applications []registrationResultApplication) error {
	c.rkmLifecycleMu.Lock()
	defer c.rkmLifecycleMu.Unlock()
	if associationEnded(c) {
		return nil
	}

	local := c.isIPSPDoubleExchange()
	configuredContexts := c.staticallyConfiguredRoutingContexts()
	configuredAppearance := c.applicationServerNetworkAppearance()
	if local {
		configuredContexts = routingContextsFromIPSPTrafficConfig(c.cfg.IPSP.TrafficToLocal)
		configuredAppearance = c.localNetworkAppearance()
	}
	trafficModes := c.trafficModes.get(c.cfg)
	if local {
		trafficModes = c.localIPSPTrafficModes.get(nil)
	}
	appearance, appearanceSet := appearanceOf(configuredAppearance)
	assigned := make(map[uint32]assignedRegistrationResultScope, len(configuredContexts)+len(applications))
	for _, routingContext := range configuredContexts {
		trafficMode, trafficModeSet := trafficModes.configured(routingContext)
		assigned[routingContext] = assignedRegistrationResultScope{
			key: ASKey{
				NetworkAppearance:    appearance,
				NetworkAppearanceSet: appearanceSet,
				RoutingContext:       routingContext,
				RoutingContextSet:    true,
			},
			trafficMode:    trafficMode,
			trafficModeSet: trafficModeSet,
		}
	}
	c.muDynamicASKeys.RLock()
	dynamic := c.dynamicPeerASKeys
	dynamicTrafficModes := c.dynamicPeerTrafficModes
	if local {
		dynamic = c.dynamicLocalASKeys
		dynamicTrafficModes = c.dynamicLocalTrafficModes
	}
	for routingContext, key := range dynamic {
		scope := assigned[routingContext]
		if scope.key.RoutingContextSet && scope.key != key {
			c.muDynamicASKeys.RUnlock()
			return NewInvalidRoutingContextError(routingContext)
		}
		scope.key = key
		if trafficMode, ok := dynamicTrafficModes[routingContext]; ok {
			if scope.trafficModeSet && scope.trafficMode != trafficMode {
				c.muDynamicASKeys.RUnlock()
				return NewInvalidRoutingContextError(routingContext)
			}
			scope.trafficMode = trafficMode
			scope.trafficModeSet = true
		}
		assigned[routingContext] = scope
	}
	c.muDynamicASKeys.RUnlock()

	prepared := make([]preparedRegistrationResult, 0, len(applications))
	for _, application := range applications {
		result := application.result
		if result.Status != RegistrationSuccessfullyRegistered && result.Status != RegistrationRoutingKeyAlreadyRegistered {
			continue
		}
		effectiveRoutingKey, _ := routingKeyWithImpliedNetworkAppearance(
			application.request.RoutingKey,
			c.localNetworkAppearance(),
		)
		key := ASKey{
			NetworkAppearance:    effectiveRoutingKey.NetworkAppearance,
			NetworkAppearanceSet: effectiveRoutingKey.NetworkAppearanceSet,
			RoutingContext:       result.RoutingContext,
			RoutingContextSet:    true,
		}
		// RFC 4666 Sections 1.4.2.1 and 3.7.1 require one Routing Context on
		// one Association to identify one AS traffic flow. Validate the whole
		// successful result batch before publishing any requester scope.
		existing, exists := assigned[result.RoutingContext]
		if exists {
			if existing.key != key || existing.trafficModeSet && effectiveRoutingKey.TrafficModeSet &&
				existing.trafficMode != effectiveRoutingKey.TrafficMode {
				return NewInvalidRoutingContextError(result.RoutingContext)
			}
		} else {
			existing.key = key
		}
		if effectiveRoutingKey.TrafficModeSet {
			existing.trafficMode = effectiveRoutingKey.TrafficMode
			existing.trafficModeSet = true
		}
		assigned[result.RoutingContext] = existing
		prepared = append(prepared, preparedRegistrationResult{
			key:        key,
			routingKey: effectiveRoutingKey,
			local:      local,
			remoteAS:   result.RemoteAS.ApplicationServer,
		})
	}

	for _, registration := range prepared {
		c.addDynamicASKey(registration.key, registration.routingKey, registration.local)
		if !registration.local {
			c.noteCanonicalRemoteAS(registration.key.RoutingContext, registration.remoteAS)
			if c.as != nil {
				c.as.registerDynamicASP(c, registration.key)
			}
		}
	}
	c.syncSSNMBindings()
	return nil
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
	parameter := params.NewRoutingKey(params.NewRoutingKeyPayload(
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

// remoteASKeyFor resolves the canonical identity of one Application Server this
// Association may register a Routing Key for.
func (c *Association) remoteASKeyFor(id RemoteASID) (SGASKey, error) {
	if id == "" {
		if c.aspPeerInventory() != nil && c.cfg != nil && c.cfg.PeerSGP != nil {
			return SGASKey{}, fmt.Errorf("%w: a provisioned Association must name one", ErrUnknownRemoteAS)
		}
		return SGASKey{}, nil
	}
	if c.cfg == nil || c.cfg.PeerSGP == nil {
		return SGASKey{ApplicationServer: id}, nil
	}
	key := SGASKey{
		SignallingGateway: c.cfg.PeerSGP.SignallingGateway,
		ApplicationServer: id,
	}
	inventory := c.aspPeerInventory()
	if inventory == nil {
		return key, nil
	}
	applicationServer, served := inventory.config.remoteASFor(*c.cfg.PeerSGP, id)
	if !served || !applicationServer.routingKeySet {
		return SGASKey{}, fmt.Errorf("%w: %q is not bound dynamically by SGP %q of Signalling Gateway %q",
			ErrUnknownRemoteAS, id,
			c.cfg.PeerSGP.SignallingGatewayProcess, c.cfg.PeerSGP.SignallingGateway)
	}
	return key, nil
}

// aspPeerInventory reports the ASP peer inventory of this Association's
// Endpoint, or nil when it provisions none.
func (c *Association) aspPeerInventory() *aspRoutes {
	if c == nil || c.endpoint == nil || !c.endpoint.aspRoutes.peerInventoryConfigured() {
		return nil
	}
	return c.endpoint.aspRoutes
}

// registeredASKey reports the exact wire scope a Registration Result assigned,
// or the zero scope when the result registered nothing.
func (c *Association) registeredASKey(
	request RoutingKeyRegistrationRequest,
	result RoutingKeyRegistrationResult,
) ASKey {
	if result.Status != RegistrationSuccessfullyRegistered &&
		result.Status != RegistrationRoutingKeyAlreadyRegistered {
		return ASKey{}
	}
	// RFC 4666 Section 3.6.1 lets a Routing Key omit Network Appearance when
	// the Association configures one, so the scope the peer assigned is that
	// implied appearance together with the Routing Context it chose.
	effective, _ := routingKeyWithImpliedNetworkAppearance(request.RoutingKey, c.localNetworkAppearance())
	return ASKey{
		NetworkAppearance:    effective.NetworkAppearance,
		NetworkAppearanceSet: effective.NetworkAppearanceSet,
		RoutingContext:       result.RoutingContext,
		RoutingContextSet:    true,
	}
}

// confirmedDeregistrationScopes resolves every named Application Server to the
// Routing Context that deregisters it, refusing anything the wire cannot
// express or this Association contradicts before any of it reaches the
// transport.
func (c *Association) confirmedDeregistrationScopes(keys []ASKey) ([]uint32, error) {
	routingContexts := make([]uint32, len(keys))
	seen := make(map[uint32]struct{}, len(keys))
	for index, key := range keys {
		if !key.RoutingContextSet {
			return nil, fmt.Errorf("%w: Application Server %d", ErrContextlessApplicationServer, index)
		}
		if conflict, held := c.conflictingApplicationServerScope(key); conflict {
			return nil, fmt.Errorf("%w: %+v names Routing Context %d, which this Association holds as %+v",
				ErrUnknownApplicationServerScope, key, key.RoutingContext, held)
		}
		if _, duplicate := seen[key.RoutingContext]; duplicate {
			return nil, fmt.Errorf("duplicate Routing Context %d", key.RoutingContext)
		}
		seen[key.RoutingContext] = struct{}{}
		routingContexts[index] = key.RoutingContext
	}
	return routingContexts, nil
}

// conflictingApplicationServerScope reports a scope that contradicts the
// binding this Association already holds for the same Routing Context, whether
// a registration assigned it or the Association configures it statically. A
// Routing Context this Association holds no binding for is not a conflict: the
// peer may still hold one, and RFC 4666 Section 3.6.4 answers Not Registered
// when it does not.
func (c *Association) conflictingApplicationServerScope(key ASKey) (bool, ASKey) {
	local := c.isIPSPDoubleExchange()
	if bound, exists := c.dynamicASKey(key.RoutingContext, local); exists {
		return bound != key, bound
	}
	for _, configured := range c.staticallyConfiguredASKeys() {
		if configured.RoutingContextSet && configured.RoutingContext == key.RoutingContext {
			return configured != key, configured
		}
	}
	return false, ASKey{}
}
