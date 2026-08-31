package m3ua

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

var (
	// ErrMTP3RestartInProgress reports an affected scope that overlaps another
	// restart procedure which has not completed yet.
	ErrMTP3RestartInProgress = errors.New("overlapping MTP3 restart already in progress")
	// ErrMTP3RestartScope reports an update outside the affected destinations
	// atomically declared when the restart began.
	ErrMTP3RestartScope = errors.New("destination is outside the MTP3 restart scope")
	// ErrStaleMTP3Restart reports a handle whose restart has already completed
	// or no longer belongs to its owning SGP state generation.
	ErrStaleMTP3Restart = errors.New("stale MTP3 restart handle")
	// ErrEmptyMTP3Restart reports a restart declaration with no affected scope.
	ErrEmptyMTP3Restart = errors.New("MTP3 restart has no affected destinations")
)

// AffectedDestination identifies one destination range affected by an MTP3
// restart. Mask is the number of wildcarded low-order point-code bits.
type AffectedDestination struct {
	NetworkAppearance    uint32
	NetworkAppearanceSet bool
	RoutingContext       uint32
	RoutingContextSet    bool
	PointCode            uint32
	Mask                 uint8
}

// MTP3Restart is an opaque generation handle for one SGP restart procedure.
// Its methods are safe to call concurrently.
type MTP3Restart struct {
	target     mtp3RestartTarget
	generation uint64
	mu         sync.Mutex
	completed  bool
}

type mtp3RestartTarget struct {
	registry     *mtp3RestartRegistry
	closed       func() bool
	prepare      func(DestinationRange) (DestinationRange, error)
	destinations func() *destinations
	publish      func([]DestinationRange, bool, bool, bool) error
}

type mtp3RestartRegistry struct {
	procedureMu sync.RWMutex
	mu          sync.Mutex
	generation  uint64
	active      map[uint64]*mtp3RestartEpoch
}

type mtp3RestartEpoch struct {
	affected []DestinationRange
	updates  []DestinationRange
}

func invalidateMTP3RestartRegistry(registry *mtp3RestartRegistry) {
	if registry == nil {
		return
	}
	registry.procedureMu.Lock()
	registry.mu.Lock()
	registry.active = nil
	registry.mu.Unlock()
	registry.procedureMu.Unlock()
}

// BeginMTP3Restart starts the MTP3 restart procedure in RFC 4666 Section 4.6.
// Validation and isolation-state publication are atomic. A non-nil handle is
// returned even when one or more ASP writes fail, so the procedure can still
// be updated and completed.
func (l *Listener) BeginMTP3Restart(affected ...AffectedDestination) (*MTP3Restart, error) {
	if l == nil {
		return nil, errors.New("nil Listener")
	}
	if l.Role() != RoleSGP {
		return nil, ErrUnsupportedRole
	}
	return beginMTP3Restart(l.mtp3RestartTarget(), affected...)
}

// BeginMTP3Restart starts the MTP3 restart procedure in RFC 4666 Section 4.6
// for an SGP Association. Accepted and SCTP-initiating Associations use the
// restart state owned by their SGP Endpoint.
func (c *Association) BeginMTP3Restart(affected ...AffectedDestination) (*MTP3Restart, error) {
	if c == nil || c.Role() != RoleSGP {
		return nil, ErrUnsupportedRole
	}
	select {
	case <-c.done:
		return nil, ErrAssociationClosed
	default:
	}
	return beginMTP3Restart(c.mtp3RestartTarget(), affected...)
}

func beginMTP3Restart(target mtp3RestartTarget, affected ...AffectedDestination) (*MTP3Restart, error) {
	if len(affected) == 0 {
		return nil, ErrEmptyMTP3Restart
	}
	if target.registry == nil || target.closed == nil || target.prepare == nil ||
		target.destinations == nil || target.publish == nil {
		return nil, ErrNotEstablished
	}

	ranges := make([]DestinationRange, len(affected))
	for index, destination := range affected {
		rangeValue, err := target.prepare(DestinationRange{
			NetworkAppearance:    destination.NetworkAppearance,
			NetworkAppearanceSet: destination.NetworkAppearanceSet,
			RoutingContext:       destination.RoutingContext,
			RoutingContextSet:    destination.RoutingContextSet,
			PointCode:            destination.PointCode,
			Mask:                 destination.Mask,
			State:                DestinationUnavailable,
		})
		if err != nil {
			return nil, err
		}
		for prior := 0; prior < index; prior++ {
			if destinationRangesOverlap(ranges[prior], rangeValue) {
				return nil, fmt.Errorf("%w: duplicate or overlapping affected destinations", ErrMTP3RestartInProgress)
			}
		}
		ranges[index] = rangeValue
	}

	registry := target.registry
	registry.procedureMu.Lock()
	defer registry.procedureMu.Unlock()
	if target.closed() {
		return nil, ErrAssociationClosed
	}

	registry.mu.Lock()
	for _, epoch := range registry.active {
		for _, existing := range epoch.affected {
			for _, candidate := range ranges {
				if destinationRangesOverlap(existing, candidate) {
					registry.mu.Unlock()
					return nil, ErrMTP3RestartInProgress
				}
			}
		}
	}
	registry.generation++
	if registry.generation == 0 {
		registry.generation++
	}
	generation := registry.generation
	if registry.active == nil {
		registry.active = make(map[uint64]*mtp3RestartEpoch)
	}
	registry.active[generation] = &mtp3RestartEpoch{
		affected: append([]DestinationRange(nil), ranges...),
	}
	registry.mu.Unlock()

	destinations := target.destinations()
	destinations.setRanges(ranges)
	handle := &MTP3Restart{target: target, generation: generation}
	return handle, target.publish(ranges, false, false, true)
}

// Update stages a destination's final state. No recovery SSNM is emitted until
// Complete, and DAUD continues to report DUNA for the affected scope meanwhile.
func (r *MTP3Restart) Update(destination AffectedDestination, state DestinationState) error {
	if r == nil || r.target.registry == nil {
		return ErrStaleMTP3Restart
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.completed {
		return ErrStaleMTP3Restart
	}
	rangeValue, err := r.target.prepare(DestinationRange{
		NetworkAppearance:    destination.NetworkAppearance,
		NetworkAppearanceSet: destination.NetworkAppearanceSet,
		RoutingContext:       destination.RoutingContext,
		RoutingContextSet:    destination.RoutingContextSet,
		PointCode:            destination.PointCode,
		Mask:                 destination.Mask,
		State:                state,
	})
	if err != nil {
		return err
	}
	return stageMTP3RestartRange(r.target.registry, r.generation, rangeValue)
}

// Complete atomically publishes all staged final states, then sends recovery
// SSNM to the currently active and concerned ASPs. Calling it again is a no-op.
func (r *MTP3Restart) Complete() error {
	if r == nil || r.target.registry == nil {
		return ErrStaleMTP3Restart
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.completed {
		return nil
	}
	err := completeMTP3Restart(r.target, r.generation)
	if !errors.Is(err, ErrStaleMTP3Restart) {
		r.completed = true
	}
	return err
}

func stageMTP3RestartRange(registry *mtp3RestartRegistry, generation uint64, rangeValue DestinationRange) error {
	registry.procedureMu.RLock()
	defer registry.procedureMu.RUnlock()
	registry.mu.Lock()
	defer registry.mu.Unlock()
	epoch, ok := registry.active[generation]
	if !ok {
		return ErrStaleMTP3Restart
	}
	if !restartEpochCovers(epoch, rangeValue) {
		return ErrMTP3RestartScope
	}
	epoch.updates = appendRestartUpdate(epoch.updates, rangeValue)
	return nil
}

// stageAnyMTP3RestartRangeLocked requires registry.procedureMu to be held for
// reading, keeping the stage-or-publish decision atomic against Complete.
func stageAnyMTP3RestartRangeLocked(registry *mtp3RestartRegistry, rangeValue DestinationRange) bool {
	if registry == nil {
		return false
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	for _, epoch := range registry.active {
		if restartEpochCovers(epoch, rangeValue) {
			epoch.updates = appendRestartUpdate(epoch.updates, rangeValue)
			return true
		}
	}
	return false
}

func appendRestartUpdate(updates []DestinationRange, rangeValue DestinationRange) []DestinationRange {
	key := destinationRangeKey(rangeValue)
	for index := range updates {
		if destinationRangeKey(updates[index]) != key {
			continue
		}
		copy(updates[index:], updates[index+1:])
		updates = updates[:len(updates)-1]
		break
	}
	return append(updates, rangeValue)
}

func completeMTP3Restart(target mtp3RestartTarget, generation uint64) error {
	registry := target.registry
	registry.procedureMu.Lock()
	defer registry.procedureMu.Unlock()
	registry.mu.Lock()
	epoch, ok := registry.active[generation]
	if !ok {
		registry.mu.Unlock()
		return ErrStaleMTP3Restart
	}
	updates := append([]DestinationRange(nil), epoch.updates...)
	delete(registry.active, generation)
	registry.mu.Unlock()

	target.destinations().setRanges(updates)
	return target.publish(updates, true, false, true)
}

func (l *Listener) mtp3RestartTarget() mtp3RestartTarget {
	l.registry()
	return mtp3RestartTarget{
		registry: l.mtp3Restarts,
		closed: func() bool {
			l.muConns.Lock()
			defer l.muConns.Unlock()
			return l.closed
		},
		prepare:      l.prepareLocalDestinationRange,
		destinations: l.destinationRegistry,
		publish:      l.publishDestinationRanges,
	}
}

func (c *Association) mtp3RestartTarget() mtp3RestartTarget {
	if c.listener != nil {
		return c.listener.mtp3RestartTarget()
	}
	return mtp3RestartTarget{
		registry: c.mtp3Restarts,
		closed: func() bool {
			select {
			case <-c.done:
				return true
			default:
				return false
			}
		},
		prepare: c.prepareLocalDestinationRange,
		destinations: func() *destinations {
			return c.destinations
		},
		publish: func(ranges []DestinationRange, completion, abateCongestion, wait bool) error {
			return publishDestinationRanges(c.as, ranges, completion, abateCongestion, wait)
		},
	}
}

func (l *Listener) destinationRegistry() *destinations {
	l.muConns.Lock()
	defer l.muConns.Unlock()
	if l.destinations == nil {
		l.destinations = newDestinations()
	}
	return l.destinations
}

func (l *Listener) prepareLocalDestinationRange(rangeValue DestinationRange) (DestinationRange, error) {
	l.muConns.Lock()
	registry := l.as
	l.muConns.Unlock()
	return prepareLocalDestinationRange(l.AssociationConfig, registry, rangeValue)
}

func (c *Association) prepareLocalDestinationRange(rangeValue DestinationRange) (DestinationRange, error) {
	return prepareLocalDestinationRange(c.cfg, c.as, rangeValue)
}

func prepareLocalDestinationRange(config *AssociationConfig, registry *applicationServers, rangeValue DestinationRange) (DestinationRange, error) {
	if !validDestinationState(rangeValue.State) {
		return DestinationRange{}, fmt.Errorf("%w: destination state %d", ErrInvalidParameterValue, rangeValue.State)
	}
	if !rangeValue.NetworkAppearanceSet && config != nil {
		rangeValue.NetworkAppearance, rangeValue.NetworkAppearanceSet = appearanceOf(config.NetworkAppearance)
	}
	rangeValue = normalizeDestinationRange(rangeValue)
	if !rangeValue.RoutingContextSet {
		return rangeValue, nil
	}
	if !hasLocalASKey(config, registry, ASKey{
		NetworkAppearance:    rangeValue.NetworkAppearance,
		NetworkAppearanceSet: rangeValue.NetworkAppearanceSet,
		RoutingContext:       rangeValue.RoutingContext,
		RoutingContextSet:    true,
	}) {
		return DestinationRange{}, NewInvalidRoutingContextError(rangeValue.RoutingContext)
	}
	return rangeValue, nil
}

func hasLocalASKey(config *AssociationConfig, registry *applicationServers, key ASKey) bool {
	if registry == nil {
		if config == nil || config.RoutingContexts == nil {
			return false
		}
		for _, routingContext := range config.RoutingContexts.RoutingContexts() {
			if routingContext == key.RoutingContext {
				return true
			}
		}
		return false
	}
	if _, ok := registry.lookup(key); ok {
		return true
	}
	if config == nil || config.RoutingContexts == nil {
		return false
	}
	networkAppearance, networkAppearanceSet := appearanceOf(config.NetworkAppearance)
	if key.NetworkAppearanceSet != networkAppearanceSet ||
		key.NetworkAppearanceSet && key.NetworkAppearance != networkAppearance {
		return false
	}
	for _, routingContext := range config.RoutingContexts.RoutingContexts() {
		if routingContext == key.RoutingContext {
			return true
		}
	}
	return false
}

func restartEpochCovers(epoch *mtp3RestartEpoch, rangeValue DestinationRange) bool {
	for _, affected := range epoch.affected {
		if destinationRangeContains(affected, rangeValue) {
			return true
		}
	}
	return false
}

func destinationRangeContains(outer, inner DestinationRange) bool {
	if outer.NetworkAppearance != inner.NetworkAppearance ||
		outer.NetworkAppearanceSet != inner.NetworkAppearanceSet {
		return false
	}
	if outer.RoutingContextSet &&
		(!inner.RoutingContextSet || outer.RoutingContext != inner.RoutingContext) {
		return false
	}
	return destinationRangeCovers(outer, inner.PointCode, inner.Mask)
}

func destinationRangesOverlap(first, second DestinationRange) bool {
	if first.NetworkAppearance != second.NetworkAppearance ||
		first.NetworkAppearanceSet != second.NetworkAppearanceSet {
		return false
	}
	if first.RoutingContextSet && second.RoutingContextSet &&
		first.RoutingContext != second.RoutingContext {
		return false
	}
	return destinationRangeCovers(first, second.PointCode, second.Mask) ||
		destinationRangeCovers(second, first.PointCode, first.Mask)
}

func restartForcesUnavailable(registry *mtp3RestartRegistry, scope destinationKey, pointCode uint32, mask uint8) bool {
	if registry == nil {
		return false
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	query := DestinationRange{
		NetworkAppearance:    scope.networkAppearance,
		NetworkAppearanceSet: scope.networkAppearanceSet,
		RoutingContext:       scope.routingContext,
		RoutingContextSet:    scope.routingContextSet,
		PointCode:            pointCode,
		Mask:                 mask,
	}
	for _, epoch := range registry.active {
		for _, affected := range epoch.affected {
			if destinationRangesOverlap(affected, query) {
				return true
			}
		}
	}
	return false
}

func writeMTP3RestartStatusBeforeAck(registry *mtp3RestartRegistry, association *Association, served []uint32) error {
	if registry == nil || association == nil || len(served) == 0 {
		return nil
	}
	registry.procedureMu.RLock()
	defer registry.procedureMu.RUnlock()
	registry.mu.Lock()
	var affected []DestinationRange
	generations := make([]uint64, 0, len(registry.active))
	for generation := range registry.active {
		generations = append(generations, generation)
	}
	sort.Slice(generations, func(i, j int) bool { return generations[i] < generations[j] })
	for _, generation := range generations {
		epoch := registry.active[generation]
		affected = append(affected, epoch.affected...)
	}
	registry.mu.Unlock()

	messagesToWrite := make([]messages.M3UA, 0, len(affected))
	for _, rangeValue := range affected {
		contexts := served
		if rangeValue.RoutingContextSet {
			if !containsRoutingContext(served, rangeValue.RoutingContext) {
				continue
			}
			contexts = []uint32{rangeValue.RoutingContext}
		}
		messagesToWrite = append(messagesToWrite, destinationStateSSNMs(
			rangeValue, contexts, DestinationUnavailable, false,
		)...)
	}
	return association.writeMandatoryControls(messagesToWrite, false, true)
}

func containsRoutingContext(routingContexts []uint32, want uint32) bool {
	for _, routingContext := range routingContexts {
		if routingContext == want {
			return true
		}
	}
	return false
}

func (l *Listener) publishDestinationRanges(ranges []DestinationRange, completion, abateCongestion, wait bool) error {
	l.muConns.Lock()
	registry := l.as
	l.muConns.Unlock()
	return publishDestinationRanges(registry, ranges, completion, abateCongestion, wait)
}

func publishDestinationRanges(registry *applicationServers, ranges []DestinationRange, completion, abateCongestion, wait bool) error {
	if len(ranges) == 0 {
		return nil
	}
	if registry == nil {
		return nil
	}

	type batch struct {
		association *Association
		messages    []messages.M3UA
		contexts    []uint32
	}
	batches := make([]batch, 0)
	indices := make(map[*Association]int)
	for _, rangeValue := range ranges {
		if completion && rangeValue.State == DestinationUnavailable {
			continue
		}
		scope := destinationKey{
			networkAppearance:    rangeValue.NetworkAppearance,
			networkAppearanceSet: rangeValue.NetworkAppearanceSet,
		}
		if rangeValue.RoutingContextSet {
			scope.routingContext = rangeValue.RoutingContext
			scope.routingContextSet = true
		}
		for _, target := range registry.activeSSNMTargets(scope) {
			index, ok := indices[target.association]
			if !ok {
				index = len(batches)
				indices[target.association] = index
				batches = append(batches, batch{association: target.association})
			}
			batches[index].contexts = appendRoutingContexts(
				batches[index].contexts, target.routingContexts,
			)
			for _, routingContexts := range ssnmTargetRoutingContextScopes(target) {
				if abateCongestion {
					batches[index].messages = append(batches[index].messages,
						destinationCongestionAbatementSSNM(rangeValue, routingContexts),
					)
				}
				batches[index].messages = append(batches[index].messages,
					destinationStateSSNMs(rangeValue, routingContexts, rangeValue.State, completion)...)
			}
		}
	}

	var waitGroup sync.WaitGroup
	errorsByBatch := make([]error, len(batches))
	for index := range batches {
		index := index
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			errorsByBatch[index] = batches[index].association.writeMandatoryControls(
				batches[index].messages, false, wait,
			)
		}()
	}
	waitGroup.Wait()
	var writeErrors []error
	for index, err := range errorsByBatch {
		if err != nil {
			writeErrors = append(writeErrors, fmt.Errorf(
				"notify active ASP for Routing Contexts %v: %w", batches[index].contexts, err,
			))
		}
	}
	return errors.Join(writeErrors...)
}

func ssnmTargetRoutingContextScopes(target activeSSNMTarget) [][]uint32 {
	scopes := make([][]uint32, 0, 2)
	if target.contextless {
		scopes = append(scopes, nil)
	}
	if len(target.routingContexts) > 0 {
		scopes = append(scopes, append([]uint32(nil), target.routingContexts...))
	}
	return scopes
}

func destinationCongestionAbatementSSNM(rangeValue DestinationRange, routingContexts []uint32) messages.M3UA {
	var networkAppearance *params.Param
	if rangeValue.NetworkAppearanceSet {
		networkAppearance = params.NewNetworkAppearance(rangeValue.NetworkAppearance)
	}
	var routingContext *params.Param
	if len(routingContexts) > 0 {
		routingContext = params.NewRoutingContext(routingContexts...)
	}
	return messages.NewSignallingCongestion(
		networkAppearance,
		routingContext,
		params.NewAffectedPointCodeWithMask(rangeValue.Mask, rangeValue.PointCode),
		nil,
		params.NewCongestionIndications(0),
		nil,
	)
}

// destinationStateSSNMs builds the complete wire representation of a state.
// Congestion is orthogonal to reachability, so SCON precedes the DAVA which
// confirms that the congested route remains reachable.
func destinationStateSSNMs(rangeValue DestinationRange, routingContexts []uint32, state DestinationState, confirmReachability bool) []messages.M3UA {
	var networkAppearance *params.Param
	if rangeValue.NetworkAppearanceSet {
		networkAppearance = params.NewNetworkAppearance(rangeValue.NetworkAppearance)
	}
	var routingContext *params.Param
	if len(routingContexts) > 0 {
		routingContext = params.NewRoutingContext(routingContexts...)
	}
	affectedPointCode := params.NewAffectedPointCodeWithMask(rangeValue.Mask, rangeValue.PointCode)
	switch state {
	case DestinationUnavailable:
		return []messages.M3UA{messages.NewDestinationUnavailable(
			networkAppearance, routingContext, affectedPointCode, nil,
		)}
	case DestinationRestricted:
		return []messages.M3UA{messages.NewDestinationRestricted(
			networkAppearance, routingContext, affectedPointCode, nil,
		)}
	case DestinationCongested:
		var congestionIndications *params.Param
		if rangeValue.CongestionLevelSet {
			congestionIndications = params.NewCongestionIndications(rangeValue.CongestionLevel)
		}
		congestion := messages.NewSignallingCongestion(
			networkAppearance, routingContext, affectedPointCode, nil, congestionIndications, nil,
		)
		if !confirmReachability {
			return []messages.M3UA{congestion}
		}
		return []messages.M3UA{congestion, messages.NewDestinationAvailable(
			networkAppearance.Copy(), routingContext.Copy(), affectedPointCode.Copy(), nil,
		)}
	default:
		return []messages.M3UA{messages.NewDestinationAvailable(
			networkAppearance, routingContext, affectedPointCode, nil,
		)}
	}
}
