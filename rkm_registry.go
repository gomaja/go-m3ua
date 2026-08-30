package m3ua

import (
	"slices"
	"sort"
	"sync"
)

const (
	defaultMaxDynamicRoutingKeys = 1024
	registrationReplayLimit      = 1024
	deregistrationReplayLimit    = 1024
)

// RoutingKeyRegistrationResult is one Registration Result returned for a
// Routing Key in an RFC 4666 Registration Request.
type RoutingKeyRegistrationResult struct {
	LocalRoutingKeyIdentifier uint32
	Status                    RegistrationStatus
	RoutingContext            uint32
}

// RoutingKeyDeregistrationResult is one Deregistration Result returned for a
// Routing Context in an RFC 4666 Deregistration Request.
type RoutingKeyDeregistrationResult struct {
	RoutingContext uint32
	Status         DeregistrationStatus
	asKey          ASKey
	removeAS       bool
}

type routingKeyRegistry struct {
	mu           sync.Mutex
	revision     uint64
	config       *RoutingKeyManagementConfig
	entries      map[uint32]*routingKeyEntry
	replays      map[*Association]*registrationReplayState
	deregReplays map[*Association]*deregistrationReplayState
	dynamic      int
}

type routingKeyEntry struct {
	routingKey     RoutingKey
	canonical      canonicalRoutingKey
	routingContext uint32
	provisioned    bool
	members        map[*Association]struct{}
}

type registrationReplayState struct {
	byIdentifier map[uint32]registrationReplay
	order        []uint32
	pending      map[uint32]struct{}
}

type registrationReplay struct {
	request RoutingKeyRegistrationRequest
	result  RoutingKeyRegistrationResult
}

type deregistrationReplayState struct {
	byRoutingContext map[uint32]RoutingKeyDeregistrationResult
	order            []uint32
	pending          map[uint32]struct{}
}

func newRoutingKeyRegistry(config *RoutingKeyManagementConfig) (*routingKeyRegistry, error) {
	config = snapshotRoutingKeyManagementConfig(config)
	if err := validateRoutingKeyManagementConfig(config); err != nil {
		return nil, err
	}
	registry := &routingKeyRegistry{
		config:       config,
		entries:      make(map[uint32]*routingKeyEntry),
		replays:      make(map[*Association]*registrationReplayState),
		deregReplays: make(map[*Association]*deregistrationReplayState),
	}
	if config == nil {
		return registry, nil
	}
	for _, provisioned := range config.ProvisionedRoutingKeys {
		canonical, err := canonicalizeRoutingKey(provisioned.RoutingKey)
		if err != nil {
			return nil, err
		}
		registry.entries[provisioned.RoutingContext] = &routingKeyEntry{
			routingKey:     snapshotRoutingKey(provisioned.RoutingKey),
			canonical:      canonical,
			routingContext: provisioned.RoutingContext,
			provisioned:    true,
			members:        make(map[*Association]struct{}),
		}
	}
	return registry, nil
}

func (registry *routingKeyRegistry) register(association *Association, requests []RoutingKeyRegistrationRequest) []RoutingKeyRegistrationResult {
	results := make([]RoutingKeyRegistrationResult, len(requests))
	if registry == nil || registry.config == nil {
		for index, request := range requests {
			results[index] = registrationResult(request.LocalRoutingKeyIdentifier, RegistrationStatusUnknown, 0)
		}
		return results
	}

	authorizationSet := make([]bool, len(requests))
	authorizations := make([]RegistrationStatus, len(requests))
	type allocationDecision struct {
		set            bool
		routingContext uint32
		err            error
		inUse          []uint32
	}
	allocations := make([]allocationDecision, len(requests))
	forcedASPIdentifierConflicts := make(map[ASKey]struct{})

	for {
		if associationEnded(association) {
			return make([]RoutingKeyRegistrationResult, len(requests))
		}
		registry.mu.Lock()
		revision := registry.revision
		entries := cloneRoutingKeyEntries(registry.entries)
		dynamicCount := registry.dynamic
		replayState := cloneRegistrationReplayState(registry.replays[association])
		deregistrationReplays := cloneDeregistrationReplayState(registry.deregReplays[association])
		registry.mu.Unlock()

		results = make([]RoutingKeyRegistrationResult, len(requests))
		var configuredRoutingContexts []uint32
		var staticallyConfiguredASKeys []ASKey
		if association != nil && association.as != nil {
			configuredRoutingContexts = association.as.routingContexts()
		}
		if association != nil {
			staticallyConfiguredASKeys = association.staticallyConfiguredASKeys()
		}

		for index, originalRequest := range requests {
			request := snapshotRoutingKeyRegistrationRequest(originalRequest)
			if replay, ok := replayState.byIdentifier[request.LocalRoutingKeyIdentifier]; ok &&
				routingKeyRegistrationRequestsEqual(replay.request, request) {
				results[index] = replay.result
				storeRegistrationReplay(replayState, request, results[index])
				continue
			}

			canonical, err := canonicalizeRoutingKey(request.RoutingKey)
			if err != nil {
				results[index] = registrationResult(request.LocalRoutingKeyIdentifier, RegistrationInvalidRoutingKey, 0)
				storeRegistrationReplay(replayState, request, results[index])
				continue
			}
			if associationHasNetworkAppearanceConflict(entries, association, canonical) {
				results[index] = registrationResult(request.LocalRoutingKeyIdentifier, RegistrationCannotSupportUniqueRouting, 0)
				storeRegistrationReplay(replayState, request, results[index])
				continue
			}

			authorize := func() RegistrationStatus {
				if !authorizationSet[index] {
					authorizations[index] = registry.authorize(request)
					authorizationSet[index] = true
				}
				return authorizations[index]
			}
			if request.RoutingContextRequested {
				entry := entries[request.RequestedRoutingContext]
				switch {
				case entry == nil:
					results[index] = registrationResult(request.LocalRoutingKeyIdentifier, RegistrationRoutingKeyChangeRefused, 0)
				case !entry.canonical.equal(canonical):
					results[index] = registrationResult(request.LocalRoutingKeyIdentifier, RegistrationRoutingKeyChangeRefused, 0)
				case routingContextConflictsWithAssociationScope(staticallyConfiguredASKeys, entry.asKey()):
					results[index] = registrationResult(request.LocalRoutingKeyIdentifier, RegistrationCannotSupportUniqueRouting, 0)
				case !registrationTrafficModeCompatible(entry.routingKey, request.RoutingKey):
					results[index] = registrationResult(request.LocalRoutingKeyIdentifier, RegistrationUnsupportedTrafficHandlingMode, 0)
				case association.hasStaticApplicationServerMembership(entry.asKey()):
					// RFC 4666 Section 4.4.1 classifies an exact static or
					// dynamic registration as Routing Key Already Registered.
					entry.adoptTrafficMode(request.RoutingKey)
					entry.members[association] = struct{}{}
					clearDeregistrationReplayState(deregistrationReplays, entry.routingContext)
					results[index] = registrationResult(request.LocalRoutingKeyIdentifier, RegistrationRoutingKeyAlreadyRegistered, entry.routingContext)
				case entry.hasMember(association):
					entry.adoptTrafficMode(request.RoutingKey)
					results[index] = registrationResult(request.LocalRoutingKeyIdentifier, RegistrationRoutingKeyAlreadyRegistered, entry.routingContext)
				case entry.hasASPIdentifierConflict(association) ||
					containsASKeySet(forcedASPIdentifierConflicts, entry.asKey()):
					results[index] = registrationResult(request.LocalRoutingKeyIdentifier, RegistrationPermissionDenied, 0)
				default:
					status := authorize()
					if status == RegistrationSuccessfullyRegistered {
						entry.adoptTrafficMode(request.RoutingKey)
						entry.members[association] = struct{}{}
						clearDeregistrationReplayState(deregistrationReplays, entry.routingContext)
						results[index] = registrationResult(request.LocalRoutingKeyIdentifier, status, entry.routingContext)
					} else {
						results[index] = registrationResult(request.LocalRoutingKeyIdentifier, status, 0)
					}
				}
				storeRegistrationReplay(replayState, request, results[index])
				continue
			}

			exact, overlap := findRoutingKeyEntry(entries, canonical)
			switch {
			case exact != nil && routingContextConflictsWithAssociationScope(staticallyConfiguredASKeys, exact.asKey()):
				results[index] = registrationResult(request.LocalRoutingKeyIdentifier, RegistrationCannotSupportUniqueRouting, 0)
			case exact != nil && !registrationTrafficModeCompatible(exact.routingKey, request.RoutingKey):
				results[index] = registrationResult(request.LocalRoutingKeyIdentifier, RegistrationUnsupportedTrafficHandlingMode, 0)
			case exact != nil && association.hasStaticApplicationServerMembership(exact.asKey()):
				// RFC 4666 Section 4.4.1 classifies an exact static or
				// dynamic registration as Routing Key Already Registered.
				exact.adoptTrafficMode(request.RoutingKey)
				exact.members[association] = struct{}{}
				clearDeregistrationReplayState(deregistrationReplays, exact.routingContext)
				results[index] = registrationResult(request.LocalRoutingKeyIdentifier, RegistrationRoutingKeyAlreadyRegistered, exact.routingContext)
			case exact != nil && exact.hasMember(association):
				exact.adoptTrafficMode(request.RoutingKey)
				results[index] = registrationResult(request.LocalRoutingKeyIdentifier, RegistrationRoutingKeyAlreadyRegistered, exact.routingContext)
			case exact != nil && (exact.hasASPIdentifierConflict(association) ||
				containsASKeySet(forcedASPIdentifierConflicts, exact.asKey())):
				results[index] = registrationResult(request.LocalRoutingKeyIdentifier, RegistrationPermissionDenied, 0)
			case exact != nil:
				status := authorize()
				if status == RegistrationSuccessfullyRegistered {
					exact.adoptTrafficMode(request.RoutingKey)
					exact.members[association] = struct{}{}
					clearDeregistrationReplayState(deregistrationReplays, exact.routingContext)
					results[index] = registrationResult(request.LocalRoutingKeyIdentifier, status, exact.routingContext)
				} else {
					results[index] = registrationResult(request.LocalRoutingKeyIdentifier, status, 0)
				}
			case overlap:
				results[index] = registrationResult(request.LocalRoutingKeyIdentifier, RegistrationCannotSupportUniqueRouting, 0)
			default:
				status := authorize()
				if status != RegistrationSuccessfullyRegistered {
					results[index] = registrationResult(request.LocalRoutingKeyIdentifier, status, 0)
					break
				}
				if !registry.config.AllowDynamicRoutingKeys {
					results[index] = registrationResult(request.LocalRoutingKeyIdentifier, RegistrationRoutingKeyNotCurrentlyProvisioned, 0)
					break
				}
				if dynamicCount >= registry.maxDynamicRoutingKeys() {
					results[index] = registrationResult(request.LocalRoutingKeyIdentifier, RegistrationInsufficientResources, 0)
					break
				}
				allocator := registry.config.AllocateRoutingContext
				if allocator != nil {
					allocator = func(allocation RoutingContextAllocationRequest) (uint32, error) {
						if !allocations[index].set || !slices.Equal(allocations[index].inUse, allocation.InUseRoutingContexts) {
							allocations[index].routingContext, allocations[index].err = registry.config.AllocateRoutingContext(allocation)
							allocations[index].inUse = append(allocations[index].inUse[:0], allocation.InUseRoutingContexts...)
							allocations[index].set = true
						}
						return allocations[index].routingContext, allocations[index].err
					}
				}
				routingContext, ok := registry.allocateRoutingContext(request, entries, configuredRoutingContexts, allocator)
				if !ok {
					results[index] = registrationResult(request.LocalRoutingKeyIdentifier, RegistrationInsufficientResources, 0)
					break
				}
				entries[routingContext] = &routingKeyEntry{
					routingKey:     snapshotRoutingKey(request.RoutingKey),
					canonical:      canonical,
					routingContext: routingContext,
					members:        map[*Association]struct{}{association: {}},
				}
				dynamicCount++
				clearDeregistrationReplayState(deregistrationReplays, routingContext)
				results[index] = registrationResult(request.LocalRoutingKeyIdentifier, RegistrationSuccessfullyRegistered, routingContext)
			}
			storeRegistrationReplay(replayState, request, results[index])
		}

		if associationEnded(association) {
			return make([]RoutingKeyRegistrationResult, len(requests))
		}
		applicationServers := associationApplicationServers(association)
		if applicationServers != nil {
			applicationServers.mu.Lock()
		}
		registry.mu.Lock()
		if registry.revision != revision {
			registry.mu.Unlock()
			if applicationServers != nil {
				applicationServers.mu.Unlock()
			}
			continue
		}
		if associationEnded(association) {
			registry.mu.Unlock()
			if applicationServers != nil {
				applicationServers.mu.Unlock()
			}
			return make([]RoutingKeyRegistrationResult, len(requests))
		}
		identifierConflicts := registrationASPIdentifierConflictsLocked(
			applicationServers,
			association,
			registry.entries,
			entries,
			results,
		)
		if len(identifierConflicts) > 0 {
			registry.mu.Unlock()
			applicationServers.mu.Unlock()
			for _, key := range identifierConflicts {
				forcedASPIdentifierConflicts[key] = struct{}{}
			}
			continue
		}
		registry.entries = entries
		registry.dynamic = dynamicCount
		registry.replays[association] = replayState
		registry.deregReplays[association] = deregistrationReplays
		registry.revision++
		registry.mu.Unlock()
		if applicationServers != nil {
			applicationServers.mu.Unlock()
		}
		return results
	}
}

func associationApplicationServers(association *Association) *applicationServers {
	if association == nil {
		return nil
	}
	return association.as
}

func registrationASPIdentifierConflictsLocked(
	applicationServers *applicationServers,
	association *Association,
	currentEntries map[uint32]*routingKeyEntry,
	nextEntries map[uint32]*routingKeyEntry,
	results []RoutingKeyRegistrationResult,
) []ASKey {
	if applicationServers == nil || association == nil {
		return nil
	}
	identifier, identifierSet := association.PeerASPIdentifier()
	if !identifierSet {
		return nil
	}
	conflicts := make([]ASKey, 0)
	for _, result := range results {
		if result.Status != RegistrationSuccessfullyRegistered {
			continue
		}
		if current := currentEntries[result.RoutingContext]; current != nil && current.hasMember(association) {
			continue
		}
		entry := nextEntries[result.RoutingContext]
		if entry != nil && applicationServers.hasASPIdentifierConflictLocked(association, entry.asKey(), identifier) {
			conflicts = append(conflicts, entry.asKey())
		}
	}
	return conflicts
}

func containsASKeySet(keys map[ASKey]struct{}, candidate ASKey) bool {
	_, ok := keys[candidate]
	return ok
}

func registrationTrafficModeCompatible(existing, requested RoutingKey) bool {
	return !existing.TrafficModeSet || !requested.TrafficModeSet || existing.TrafficMode == requested.TrafficMode
}

func (registry *routingKeyRegistry) deregister(association *Association, routingContexts []uint32) []RoutingKeyDeregistrationResult {
	results := make([]RoutingKeyDeregistrationResult, len(routingContexts))
	if registry == nil || registry.config == nil {
		for index, routingContext := range routingContexts {
			results[index] = RoutingKeyDeregistrationResult{RoutingContext: routingContext, Status: DeregistrationStatusUnknown}
		}
		return results
	}

	type authorizationDecision struct {
		set     bool
		request RoutingKeyDeregistrationRequest
		allow   bool
	}
	authorizations := make([]authorizationDecision, len(routingContexts))
	for {
		if associationEnded(association) {
			return unknownDeregistrationResults(routingContexts)
		}
		registry.mu.Lock()
		revision := registry.revision
		entries := cloneRoutingKeyEntries(registry.entries)
		dynamicCount := registry.dynamic
		deregistrationReplays := cloneDeregistrationReplayState(registry.deregReplays[association])
		registrationReplays := cloneRegistrationReplayState(registry.replays[association])
		registry.mu.Unlock()

		results = make([]RoutingKeyDeregistrationResult, len(routingContexts))
		peer := association.routingKeyPeer()
		batchResults := make(map[uint32]RoutingKeyDeregistrationResult, len(routingContexts))
		for index, routingContext := range routingContexts {
			if previous, duplicate := batchResults[routingContext]; duplicate {
				results[index] = previous
				continue
			}
			result := RoutingKeyDeregistrationResult{RoutingContext: routingContext}
			if replay, ok := deregistrationReplays.byRoutingContext[routingContext]; ok {
				result = replay
				storeDeregistrationReplay(deregistrationReplays, result)
			} else {
				entry := entries[routingContext]
				switch {
				case entry == nil:
					result.Status = DeregistrationInvalidRoutingContext
				case !entry.hasMember(association):
					result.Status = DeregistrationNotRegistered
				case association != nil && association.State() == StateASPActive && association.activeForASKey(entry.asKey()):
					result.Status = DeregistrationASPActiveForRoutingContext
				default:
					request := RoutingKeyDeregistrationRequest{
						Peer:           peer,
						RoutingContext: routingContext,
						RoutingKey:     snapshotRoutingKey(entry.routingKey),
						Provisioned:    entry.provisioned,
					}
					allowed := true
					if registry.config.AuthorizeDeregistration != nil {
						decision := &authorizations[index]
						if !decision.set || !routingKeyDeregistrationRequestsEqual(decision.request, request) {
							decision.request = snapshotRoutingKeyDeregistrationRequest(request)
							decision.allow = registry.config.AuthorizeDeregistration(decision.request)
							decision.set = true
						}
						allowed = decision.allow
					}
					if !allowed {
						result.Status = DeregistrationPermissionDenied
						break
					}
					delete(entry.members, association)
					result.Status = DeregistrationSuccessfullyDeregistered
					result.asKey = entry.asKey()
					storeDeregistrationReplay(deregistrationReplays, result)
					purgeRegistrationReplayState(registrationReplays, routingContext)
					if !entry.provisioned && len(entry.members) == 0 && registry.config.RemoveUnusedRoutingKeys {
						result.removeAS = true
						storeDeregistrationReplay(deregistrationReplays, result)
						delete(entries, routingContext)
						dynamicCount--
					}
				}
			}
			results[index] = result
			batchResults[routingContext] = result
		}

		if associationEnded(association) {
			return unknownDeregistrationResults(routingContexts)
		}
		registry.mu.Lock()
		if registry.revision != revision {
			registry.mu.Unlock()
			continue
		}
		if associationEnded(association) {
			registry.mu.Unlock()
			return unknownDeregistrationResults(routingContexts)
		}
		registry.entries = entries
		registry.dynamic = dynamicCount
		registry.replays[association] = registrationReplays
		registry.deregReplays[association] = deregistrationReplays
		registry.revision++
		registry.mu.Unlock()
		return results
	}
}

func unknownDeregistrationResults(routingContexts []uint32) []RoutingKeyDeregistrationResult {
	results := make([]RoutingKeyDeregistrationResult, len(routingContexts))
	for index, routingContext := range routingContexts {
		results[index] = RoutingKeyDeregistrationResult{
			RoutingContext: routingContext,
			Status:         DeregistrationStatusUnknown,
		}
	}
	return results
}

func (registry *routingKeyRegistry) authorize(request RoutingKeyRegistrationRequest) RegistrationStatus {
	status := registry.config.AuthorizeRegistration(snapshotRoutingKeyRegistrationRequest(request))
	if status >= RegistrationRoutingKeyAlreadyRegistered {
		return RegistrationInvalidRoutingKey
	}
	return status
}

func (registry *routingKeyRegistry) allocateRoutingContext(
	request RoutingKeyRegistrationRequest,
	entries map[uint32]*routingKeyEntry,
	configuredRoutingContexts []uint32,
	allocator RoutingContextAllocator,
) (uint32, bool) {
	usedSet := make(map[uint32]struct{}, len(entries)+len(configuredRoutingContexts))
	for routingContext := range entries {
		usedSet[routingContext] = struct{}{}
	}
	for _, routingContext := range configuredRoutingContexts {
		usedSet[routingContext] = struct{}{}
	}
	used := make([]uint32, 0, len(usedSet))
	for routingContext := range usedSet {
		used = append(used, routingContext)
	}
	sort.Slice(used, func(i, j int) bool { return used[i] < used[j] })
	if allocator != nil {
		routingContext, err := allocator(RoutingContextAllocationRequest{
			Registration:         snapshotRoutingKeyRegistrationRequest(request),
			InUseRoutingContexts: append([]uint32(nil), used...),
		})
		_, inUse := usedSet[routingContext]
		if err != nil || routingContext == 0 || inUse {
			return 0, false
		}
		return routingContext, true
	}
	candidate := uint32(1)
	for _, routingContext := range used {
		if routingContext < candidate {
			continue
		}
		if routingContext > candidate {
			return candidate, true
		}
		if candidate == ^uint32(0) {
			return 0, false
		}
		candidate++
	}
	if candidate == 0 {
		return 0, false
	}
	return candidate, true
}

func (registry *routingKeyRegistry) maxDynamicRoutingKeys() int {
	if registry.config.MaxDynamicRoutingKeys > 0 {
		return registry.config.MaxDynamicRoutingKeys
	}
	return defaultMaxDynamicRoutingKeys
}

func (registry *routingKeyRegistry) dynamicCount() int {
	if registry == nil {
		return 0
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	return registry.dynamic
}

func (registry *routingKeyRegistry) enabled() bool {
	return registry != nil && registry.config != nil
}

func (registry *routingKeyRegistry) asKey(routingContext uint32) (ASKey, bool) {
	if registry == nil {
		return ASKey{}, false
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	entry := registry.entries[routingContext]
	if entry == nil {
		return ASKey{}, false
	}
	return entry.asKey(), true
}

func (registry *routingKeyRegistry) routingKey(routingContext uint32) (RoutingKey, bool) {
	if registry == nil {
		return RoutingKey{}, false
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	entry := registry.entries[routingContext]
	if entry == nil {
		return RoutingKey{}, false
	}
	return snapshotRoutingKey(entry.routingKey), true
}

func (registry *routingKeyRegistry) matchingASKeys(
	networkAppearance uint32,
	networkAppearanceSet bool,
	originatingPointCode uint32,
	destinationPointCode uint32,
	serviceIndicator uint8,
) ([]ASKey, bool) {
	if registry == nil {
		return nil, false
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if len(registry.entries) == 0 {
		return nil, false
	}
	matches := make([]ASKey, 0, 1)
	for _, entry := range registry.entries {
		if entry.canonical.matchesTraffic(
			networkAppearance,
			networkAppearanceSet,
			originatingPointCode,
			destinationPointCode,
			serviceIndicator,
		) {
			matches = append(matches, entry.asKey())
		}
	}
	sort.Slice(matches, func(i, j int) bool { return compareASKey(matches[i], matches[j]) < 0 })
	return matches, true
}

func (registry *routingKeyRegistry) forgetAssociation(association *Association) []ASKey {
	if registry == nil || association == nil {
		return nil
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	removed := make([]ASKey, 0)
	for routingContext, entry := range registry.entries {
		if !entry.hasMember(association) {
			continue
		}
		delete(entry.members, association)
		if !entry.provisioned && len(entry.members) == 0 && registry.config.RemoveUnusedRoutingKeys {
			removed = append(removed, entry.asKey())
			delete(registry.entries, routingContext)
			registry.dynamic--
		}
	}
	delete(registry.replays, association)
	delete(registry.deregReplays, association)
	registry.revision++
	return removed
}

func associationEnded(association *Association) bool {
	if association == nil {
		return false
	}
	select {
	case <-association.done:
		return true
	default:
		return false
	}
}

func clearDeregistrationReplayState(state *deregistrationReplayState, routingContext uint32) {
	if state == nil {
		return
	}
	delete(state.byRoutingContext, routingContext)
	delete(state.pending, routingContext)
	filtered := state.order[:0]
	for _, retained := range state.order {
		if retained != routingContext {
			filtered = append(filtered, retained)
		}
	}
	state.order = filtered
}

func purgeRegistrationReplayState(state *registrationReplayState, routingContext uint32) {
	if state == nil {
		return
	}
	filtered := state.order[:0]
	for _, identifier := range state.order {
		replay, ok := state.byIdentifier[identifier]
		if !ok {
			continue
		}
		if replay.result.RoutingContext == routingContext {
			delete(state.byIdentifier, identifier)
			delete(state.pending, identifier)
			continue
		}
		filtered = append(filtered, identifier)
	}
	state.order = filtered
}

func cloneDeregistrationReplayState(state *deregistrationReplayState) *deregistrationReplayState {
	clone := &deregistrationReplayState{
		byRoutingContext: make(map[uint32]RoutingKeyDeregistrationResult),
		pending:          make(map[uint32]struct{}),
	}
	if state == nil {
		return clone
	}
	clone.order = append([]uint32(nil), state.order...)
	for routingContext, result := range state.byRoutingContext {
		clone.byRoutingContext[routingContext] = result
	}
	for routingContext := range state.pending {
		clone.pending[routingContext] = struct{}{}
	}
	return clone
}

func storeDeregistrationReplay(state *deregistrationReplayState, result RoutingKeyDeregistrationResult) {
	if state.pending == nil {
		state.pending = make(map[uint32]struct{})
	}
	if _, exists := state.byRoutingContext[result.RoutingContext]; !exists {
		state.order = append(state.order, result.RoutingContext)
	}
	state.byRoutingContext[result.RoutingContext] = result
	state.pending[result.RoutingContext] = struct{}{}
	trimDeregistrationReplayState(state)
}

func trimDeregistrationReplayState(state *deregistrationReplayState) {
	excess := len(state.order) - deregistrationReplayLimit
	if excess <= 0 {
		return
	}
	retained := state.order[:0]
	for _, routingContext := range state.order {
		if _, pending := state.pending[routingContext]; excess > 0 && !pending {
			delete(state.byRoutingContext, routingContext)
			excess--
			continue
		}
		retained = append(retained, routingContext)
	}
	state.order = retained
}

func registrationResult(identifier uint32, status RegistrationStatus, routingContext uint32) RoutingKeyRegistrationResult {
	return RoutingKeyRegistrationResult{
		LocalRoutingKeyIdentifier: identifier,
		Status:                    status,
		RoutingContext:            routingContext,
	}
}

func findRoutingKeyEntry(entries map[uint32]*routingKeyEntry, key canonicalRoutingKey) (*routingKeyEntry, bool) {
	var exact *routingKeyEntry
	overlap := false
	for _, entry := range entries {
		if entry.canonical.equal(key) {
			exact = entry
			continue
		}
		if entry.canonical.overlaps(key) {
			overlap = true
		}
	}
	return exact, overlap
}

func associationHasNetworkAppearanceConflict(entries map[uint32]*routingKeyEntry, association *Association, requested canonicalRoutingKey) bool {
	for _, entry := range entries {
		if !entry.hasMember(association) || entry.canonical.equal(requested) {
			continue
		}
		if !entry.canonical.networkAppearanceSet || !requested.networkAppearanceSet {
			return true
		}
	}
	return false
}

func routingContextConflictsWithAssociationScope(configured []ASKey, candidate ASKey) bool {
	for _, key := range configured {
		if !key.RoutingContextSet || key.RoutingContext != candidate.RoutingContext {
			continue
		}
		// RFC 4666 Sections 1.4.2.1 and 3.7.1 require a Routing Context on
		// one association to identify one AS traffic flow. The same numeric
		// value cannot name a second Network Appearance on that association.
		return key != candidate
	}
	return false
}

func (entry *routingKeyEntry) hasMember(association *Association) bool {
	if entry == nil {
		return false
	}
	_, member := entry.members[association]
	return member
}

func (entry *routingKeyEntry) hasASPIdentifierConflict(association *Association) bool {
	if entry == nil || association == nil {
		return false
	}
	identifier, identifierSet := association.PeerASPIdentifier()
	if !identifierSet {
		return false
	}
	for member := range entry.members {
		if member == association {
			continue
		}
		memberIdentifier, memberIdentifierSet := member.PeerASPIdentifier()
		if memberIdentifierSet && memberIdentifier == identifier {
			// RFC 4666 Section 3.5.1 defines the ASP Identifier as unique among
			// the ASPs supporting an AS. RKM can make previously disjoint ASPs
			// converge on one AS, so the invariant must be rechecked here.
			return true
		}
	}
	return false
}

func (registry *routingKeyRegistry) associationsShareRoutingKey(first, second *Association) bool {
	if registry == nil || first == nil || second == nil {
		return false
	}
	firstKeys := first.configuredASKeys()
	secondKeys := second.configuredASKeys()
	registry.mu.Lock()
	defer registry.mu.Unlock()
	for _, entry := range registry.entries {
		_, firstMember := entry.members[first]
		_, secondMember := entry.members[second]
		if firstMember && (secondMember || containsASKey(secondKeys, entry.asKey())) {
			return true
		}
		if secondMember && containsASKey(firstKeys, entry.asKey()) {
			return true
		}
	}
	return false
}

func containsASKey(keys []ASKey, candidate ASKey) bool {
	for _, key := range keys {
		if key == candidate {
			return true
		}
	}
	return false
}

func (entry *routingKeyEntry) adoptTrafficMode(requested RoutingKey) {
	if entry == nil || entry.routingKey.TrafficModeSet || !requested.TrafficModeSet {
		return
	}
	entry.routingKey.TrafficMode = requested.TrafficMode
	entry.routingKey.TrafficModeSet = true
}

func (entry *routingKeyEntry) asKey() ASKey {
	return ASKey{
		NetworkAppearance:    entry.routingKey.NetworkAppearance,
		NetworkAppearanceSet: entry.routingKey.NetworkAppearanceSet,
		RoutingContext:       entry.routingContext,
		RoutingContextSet:    true,
	}
}

func cloneRoutingKeyEntries(entries map[uint32]*routingKeyEntry) map[uint32]*routingKeyEntry {
	clone := make(map[uint32]*routingKeyEntry, len(entries))
	for routingContext, entry := range entries {
		members := make(map[*Association]struct{}, len(entry.members))
		for association := range entry.members {
			members[association] = struct{}{}
		}
		clone[routingContext] = &routingKeyEntry{
			routingKey:     snapshotRoutingKey(entry.routingKey),
			canonical:      entry.canonical,
			routingContext: entry.routingContext,
			provisioned:    entry.provisioned,
			members:        members,
		}
	}
	return clone
}

func cloneRegistrationReplayState(state *registrationReplayState) *registrationReplayState {
	clone := &registrationReplayState{
		byIdentifier: make(map[uint32]registrationReplay),
		pending:      make(map[uint32]struct{}),
	}
	if state == nil {
		return clone
	}
	clone.order = append([]uint32(nil), state.order...)
	for identifier, replay := range state.byIdentifier {
		clone.byIdentifier[identifier] = registrationReplay{
			request: snapshotRoutingKeyRegistrationRequest(replay.request),
			result:  replay.result,
		}
	}
	for identifier := range state.pending {
		clone.pending[identifier] = struct{}{}
	}
	return clone
}

func storeRegistrationReplay(state *registrationReplayState, request RoutingKeyRegistrationRequest, result RoutingKeyRegistrationResult) {
	if state.pending == nil {
		state.pending = make(map[uint32]struct{})
	}
	if _, exists := state.byIdentifier[request.LocalRoutingKeyIdentifier]; !exists {
		state.order = append(state.order, request.LocalRoutingKeyIdentifier)
	}
	state.byIdentifier[request.LocalRoutingKeyIdentifier] = registrationReplay{
		request: snapshotRoutingKeyRegistrationRequest(request),
		result:  result,
	}
	state.pending[request.LocalRoutingKeyIdentifier] = struct{}{}
	trimRegistrationReplayState(state)
}

func trimRegistrationReplayState(state *registrationReplayState) {
	excess := len(state.order) - registrationReplayLimit
	if excess <= 0 {
		return
	}
	retained := state.order[:0]
	for _, identifier := range state.order {
		if _, pending := state.pending[identifier]; excess > 0 && !pending {
			delete(state.byIdentifier, identifier)
			excess--
			continue
		}
		retained = append(retained, identifier)
	}
	state.order = retained
}

func (registry *routingKeyRegistry) registrationResponseWritten(
	association *Association,
	requests []RoutingKeyRegistrationRequest,
	results []RoutingKeyRegistrationResult,
) {
	if registry == nil {
		return
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	state := registry.replays[association]
	if state == nil {
		return
	}
	changed := false
	for index, request := range requests {
		if index >= len(results) {
			break
		}
		replay, ok := state.byIdentifier[request.LocalRoutingKeyIdentifier]
		if !ok || replay.result != results[index] || !routingKeyRegistrationRequestsEqual(replay.request, request) {
			continue
		}
		if _, pending := state.pending[request.LocalRoutingKeyIdentifier]; pending {
			delete(state.pending, request.LocalRoutingKeyIdentifier)
			changed = true
		}
	}
	before := len(state.order)
	trimRegistrationReplayState(state)
	if changed || len(state.order) != before {
		registry.revision++
	}
}

func (registry *routingKeyRegistry) deregistrationResponseWritten(
	association *Association,
	results []RoutingKeyDeregistrationResult,
) {
	if registry == nil {
		return
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	state := registry.deregReplays[association]
	if state == nil {
		return
	}
	changed := false
	for _, result := range results {
		replay, ok := state.byRoutingContext[result.RoutingContext]
		if !ok || replay != result {
			continue
		}
		if _, pending := state.pending[result.RoutingContext]; pending {
			delete(state.pending, result.RoutingContext)
			changed = true
		}
	}
	before := len(state.order)
	trimDeregistrationReplayState(state)
	if changed || len(state.order) != before {
		registry.revision++
	}
}

func snapshotRoutingKeyRegistrationRequest(request RoutingKeyRegistrationRequest) RoutingKeyRegistrationRequest {
	snapshot := request
	snapshot.Peer.RemoteAddr = cloneSCTPAddr(request.Peer.RemoteAddr)
	snapshot.RoutingKey = snapshotRoutingKey(request.RoutingKey)
	return snapshot
}

func routingKeyRegistrationRequestsEqual(first, second RoutingKeyRegistrationRequest) bool {
	if first.LocalRoutingKeyIdentifier != second.LocalRoutingKeyIdentifier ||
		first.RequestedRoutingContext != second.RequestedRoutingContext ||
		first.RoutingContextRequested != second.RoutingContextRequested ||
		first.RoutingKey.TrafficMode != second.RoutingKey.TrafficMode ||
		first.RoutingKey.TrafficModeSet != second.RoutingKey.TrafficModeSet {
		return false
	}
	firstCanonical, firstErr := canonicalizeRoutingKey(first.RoutingKey)
	secondCanonical, secondErr := canonicalizeRoutingKey(second.RoutingKey)
	return firstErr == nil && secondErr == nil && firstCanonical.equal(secondCanonical)
}

func snapshotRoutingKeyDeregistrationRequest(request RoutingKeyDeregistrationRequest) RoutingKeyDeregistrationRequest {
	snapshot := request
	snapshot.Peer.RemoteAddr = cloneSCTPAddr(request.Peer.RemoteAddr)
	snapshot.RoutingKey = snapshotRoutingKey(request.RoutingKey)
	return snapshot
}

func routingKeyDeregistrationRequestsEqual(first, second RoutingKeyDeregistrationRequest) bool {
	if first.RoutingContext != second.RoutingContext || first.Provisioned != second.Provisioned ||
		first.RoutingKey.TrafficMode != second.RoutingKey.TrafficMode ||
		first.RoutingKey.TrafficModeSet != second.RoutingKey.TrafficModeSet ||
		!routingKeyPeersEqual(first.Peer, second.Peer) {
		return false
	}
	firstCanonical, firstErr := canonicalizeRoutingKey(first.RoutingKey)
	secondCanonical, secondErr := canonicalizeRoutingKey(second.RoutingKey)
	return firstErr == nil && secondErr == nil && firstCanonical.equal(secondCanonical)
}

func routingKeyPeersEqual(first, second RoutingKeyPeer) bool {
	if first.Role != second.Role || first.ASPIdentifier != second.ASPIdentifier ||
		first.ASPIdentifierSet != second.ASPIdentifierSet {
		return false
	}
	if first.RemoteAddr == nil || second.RemoteAddr == nil {
		return first.RemoteAddr == nil && second.RemoteAddr == nil
	}
	if first.RemoteAddr.Port != second.RemoteAddr.Port || len(first.RemoteAddr.IPAddrs) != len(second.RemoteAddr.IPAddrs) {
		return false
	}
	for index, firstIP := range first.RemoteAddr.IPAddrs {
		secondIP := second.RemoteAddr.IPAddrs[index]
		if firstIP.Zone != secondIP.Zone || !firstIP.IP.Equal(secondIP.IP) {
			return false
		}
	}
	return true
}
