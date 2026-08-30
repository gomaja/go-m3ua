package m3ua

import (
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
	operations   sync.Mutex
	mu           sync.Mutex
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
}

type registrationReplay struct {
	request RoutingKeyRegistrationRequest
	result  RoutingKeyRegistrationResult
}

type deregistrationReplayState struct {
	byRoutingContext map[uint32]RoutingKeyDeregistrationResult
	order            []uint32
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

	registry.operations.Lock()
	defer registry.operations.Unlock()

	registry.mu.Lock()
	entries := cloneRoutingKeyEntries(registry.entries)
	dynamicCount := registry.dynamic
	replayState := cloneRegistrationReplayState(registry.replays[association])
	registry.mu.Unlock()
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
			case entry.hasMember(association):
				entry.adoptTrafficMode(request.RoutingKey)
				results[index] = registrationResult(request.LocalRoutingKeyIdentifier, RegistrationRoutingKeyAlreadyRegistered, entry.routingContext)
			case entry.hasASPIdentifierConflict(association):
				results[index] = registrationResult(request.LocalRoutingKeyIdentifier, RegistrationPermissionDenied, 0)
			default:
				status := registry.authorize(request)
				if status == RegistrationSuccessfullyRegistered {
					entry.adoptTrafficMode(request.RoutingKey)
					entry.members[association] = struct{}{}
					registry.clearDeregistrationReplay(association, entry.routingContext)
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
		case exact != nil && exact.hasMember(association):
			exact.adoptTrafficMode(request.RoutingKey)
			results[index] = registrationResult(request.LocalRoutingKeyIdentifier, RegistrationRoutingKeyAlreadyRegistered, exact.routingContext)
		case exact != nil && exact.hasASPIdentifierConflict(association):
			results[index] = registrationResult(request.LocalRoutingKeyIdentifier, RegistrationPermissionDenied, 0)
		case exact != nil:
			status := registry.authorize(request)
			if status == RegistrationSuccessfullyRegistered {
				exact.adoptTrafficMode(request.RoutingKey)
				exact.members[association] = struct{}{}
				registry.clearDeregistrationReplay(association, exact.routingContext)
				results[index] = registrationResult(request.LocalRoutingKeyIdentifier, status, exact.routingContext)
			} else {
				results[index] = registrationResult(request.LocalRoutingKeyIdentifier, status, 0)
			}
		case overlap:
			results[index] = registrationResult(request.LocalRoutingKeyIdentifier, RegistrationCannotSupportUniqueRouting, 0)
		default:
			status := registry.authorize(request)
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
			routingContext, ok := registry.allocateRoutingContext(request, entries, configuredRoutingContexts)
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
			registry.clearDeregistrationReplay(association, routingContext)
			results[index] = registrationResult(request.LocalRoutingKeyIdentifier, RegistrationSuccessfullyRegistered, routingContext)
		}
		storeRegistrationReplay(replayState, request, results[index])
	}

	registry.mu.Lock()
	registry.entries = entries
	registry.dynamic = dynamicCount
	registry.replays[association] = replayState
	registry.mu.Unlock()
	return results
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

	registry.operations.Lock()
	defer registry.operations.Unlock()

	registry.mu.Lock()
	entries := cloneRoutingKeyEntries(registry.entries)
	dynamicCount := registry.dynamic
	deregistrationReplays := cloneDeregistrationReplayState(registry.deregReplays[association])
	registrationReplays := cloneRegistrationReplayState(registry.replays[association])
	registry.mu.Unlock()

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
		} else {
			entry := entries[routingContext]
			switch {
			case entry == nil:
				result.Status = DeregistrationInvalidRoutingContext
			case !entry.hasMember(association):
				result.Status = DeregistrationNotRegistered
			case association != nil && association.State() == StateASPActive && association.activeForASKey(entry.asKey()):
				result.Status = DeregistrationASPActiveForRoutingContext
			case registry.config.AuthorizeDeregistration != nil && !registry.config.AuthorizeDeregistration(RoutingKeyDeregistrationRequest{
				Peer:           peer,
				RoutingContext: routingContext,
				RoutingKey:     snapshotRoutingKey(entry.routingKey),
				Provisioned:    entry.provisioned,
			}):
				result.Status = DeregistrationPermissionDenied
			default:
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

	registry.mu.Lock()
	registry.entries = entries
	registry.dynamic = dynamicCount
	registry.replays[association] = registrationReplays
	registry.deregReplays[association] = deregistrationReplays
	registry.mu.Unlock()
	return results
}

func (registry *routingKeyRegistry) clearDeregistrationReplay(association *Association, routingContext uint32) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if replay := registry.deregReplays[association]; replay != nil {
		delete(replay.byRoutingContext, routingContext)
		filtered := replay.order[:0]
		for _, retained := range replay.order {
			if retained != routingContext {
				filtered = append(filtered, retained)
			}
		}
		replay.order = filtered
	}
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
	if registry.config.AllocateRoutingContext != nil {
		routingContext, err := registry.config.AllocateRoutingContext(RoutingContextAllocationRequest{
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
	registry.operations.Lock()
	defer registry.operations.Unlock()
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
	return removed
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
			continue
		}
		filtered = append(filtered, identifier)
	}
	state.order = filtered
}

func cloneDeregistrationReplayState(state *deregistrationReplayState) *deregistrationReplayState {
	clone := &deregistrationReplayState{byRoutingContext: make(map[uint32]RoutingKeyDeregistrationResult)}
	if state == nil {
		return clone
	}
	clone.order = append([]uint32(nil), state.order...)
	for routingContext, result := range state.byRoutingContext {
		clone.byRoutingContext[routingContext] = result
	}
	return clone
}

func storeDeregistrationReplay(state *deregistrationReplayState, result RoutingKeyDeregistrationResult) {
	if _, exists := state.byRoutingContext[result.RoutingContext]; !exists {
		state.order = append(state.order, result.RoutingContext)
	}
	state.byRoutingContext[result.RoutingContext] = result
	if len(state.order) <= deregistrationReplayLimit {
		return
	}
	evicted := state.order[0]
	state.order = state.order[1:]
	delete(state.byRoutingContext, evicted)
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
	clone := &registrationReplayState{byIdentifier: make(map[uint32]registrationReplay)}
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
	return clone
}

func storeRegistrationReplay(state *registrationReplayState, request RoutingKeyRegistrationRequest, result RoutingKeyRegistrationResult) {
	if _, exists := state.byIdentifier[request.LocalRoutingKeyIdentifier]; !exists {
		state.order = append(state.order, request.LocalRoutingKeyIdentifier)
	}
	state.byIdentifier[request.LocalRoutingKeyIdentifier] = registrationReplay{
		request: snapshotRoutingKeyRegistrationRequest(request),
		result:  result,
	}
	if len(state.order) <= registrationReplayLimit {
		return
	}
	evicted := state.order[0]
	state.order = state.order[1:]
	delete(state.byIdentifier, evicted)
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
