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
	// or no longer belongs to this Listener generation.
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

// MTP3Restart is an opaque generation handle for one Listener restart
// procedure. Its methods are safe to call concurrently.
type MTP3Restart struct {
	listener   *Listener
	generation uint64
	mu         sync.Mutex
	completed  bool
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

// BeginMTP3Restart starts the MTP3 restart procedure in RFC 4666 Section 4.6.
// Validation and isolation-state publication are atomic. A non-nil handle is
// returned even when one or more ASP writes fail, so the procedure can still
// be updated and completed.
func (l *Listener) BeginMTP3Restart(affected ...AffectedDestination) (*MTP3Restart, error) {
	if l == nil {
		return nil, errors.New("nil Listener")
	}
	if len(affected) == 0 {
		return nil, ErrEmptyMTP3Restart
	}

	ranges := make([]DestinationRange, len(affected))
	for index, destination := range affected {
		rangeValue, err := l.prepareLocalDestinationRange(DestinationRange{
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

	registry := &l.mtp3Restarts
	registry.procedureMu.Lock()
	defer registry.procedureMu.Unlock()
	l.muConns.Lock()
	closed := l.closed
	l.muConns.Unlock()
	if closed {
		return nil, ErrConnClosed
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

	destinations := l.destinationRegistry()
	destinations.setRanges(ranges)
	handle := &MTP3Restart{listener: l, generation: generation}
	return handle, l.publishDestinationRanges(ranges, false, false, true)
}

// Update stages a destination's final state. No recovery SSNM is emitted until
// Complete, and DAUD continues to report DUNA for the affected scope meanwhile.
func (r *MTP3Restart) Update(destination AffectedDestination, state DestinationState) error {
	if r == nil || r.listener == nil {
		return ErrStaleMTP3Restart
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.completed {
		return ErrStaleMTP3Restart
	}
	rangeValue, err := r.listener.prepareLocalDestinationRange(DestinationRange{
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
	return r.listener.stageMTP3RestartRange(r.generation, rangeValue)
}

// Complete atomically publishes all staged final states, then sends recovery
// SSNM to the currently active and concerned ASPs. Calling it again is a no-op.
func (r *MTP3Restart) Complete() error {
	if r == nil || r.listener == nil {
		return ErrStaleMTP3Restart
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.completed {
		return nil
	}
	err := r.listener.completeMTP3Restart(r.generation)
	if !errors.Is(err, ErrStaleMTP3Restart) {
		r.completed = true
	}
	return err
}

func (l *Listener) stageMTP3RestartRange(generation uint64, rangeValue DestinationRange) error {
	registry := &l.mtp3Restarts
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

// stageAnyMTP3RestartRangeLocked requires procedureMu to be held for reading,
// keeping the stage-or-publish decision atomic against Complete.
func (l *Listener) stageAnyMTP3RestartRangeLocked(rangeValue DestinationRange) bool {
	registry := &l.mtp3Restarts
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

func (l *Listener) completeMTP3Restart(generation uint64) error {
	registry := &l.mtp3Restarts
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

	l.destinationRegistry().setRanges(updates)
	return l.publishDestinationRanges(updates, true, false, true)
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
	if !validDestinationState(rangeValue.State) {
		return DestinationRange{}, fmt.Errorf("%w: destination state %d", ErrInvalidParameterValue, rangeValue.State)
	}
	if !rangeValue.NetworkAppearanceSet && l.Config != nil {
		rangeValue.NetworkAppearance, rangeValue.NetworkAppearanceSet = appearanceOf(l.Config.NetworkAppearance)
	}
	rangeValue = normalizeDestinationRange(rangeValue)
	if !rangeValue.RoutingContextSet {
		return rangeValue, nil
	}
	if !l.hasLocalRoutingContext(rangeValue.RoutingContext) {
		return DestinationRange{}, NewInvalidRoutingContextError(rangeValue.RoutingContext)
	}
	return rangeValue, nil
}

func (l *Listener) hasLocalRoutingContext(routingContext uint32) bool {
	if l == nil {
		return false
	}
	if l.Config != nil && l.Config.RoutingContexts != nil {
		configured := l.Config.RoutingContexts.RoutingContexts()
		if len(configured) > 0 {
			for _, candidate := range configured {
				if candidate == routingContext {
					return true
				}
			}
		}
	}
	l.muConns.Lock()
	registry := l.as
	l.muConns.Unlock()
	if registry == nil {
		return false
	}
	return len(registry.asKeysForRoutingContext(routingContext)) > 0
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

func (l *Listener) restartForcesUnavailable(scope destinationKey, pointCode uint32, mask uint8) bool {
	if l == nil {
		return false
	}
	registry := &l.mtp3Restarts
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

func (l *Listener) writeMTP3RestartStatusBeforeAck(connection *Conn, served []uint32) error {
	if l == nil || connection == nil || len(served) == 0 {
		return nil
	}
	registry := &l.mtp3Restarts
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
	return connection.writeMandatoryControls(messagesToWrite, false, true)
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
	if len(ranges) == 0 {
		return nil
	}
	l.muConns.Lock()
	registry := l.as
	l.muConns.Unlock()
	if registry == nil {
		return nil
	}

	type batch struct {
		connection *Conn
		messages   []messages.M3UA
		contexts   []uint32
	}
	batches := make([]batch, 0)
	indices := make(map[*Conn]int)
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
			index, ok := indices[target.connection]
			if !ok {
				index = len(batches)
				indices[target.connection] = index
				batches = append(batches, batch{connection: target.connection})
			}
			batches[index].contexts = append([]uint32(nil), target.routingContexts...)
			if abateCongestion {
				batches[index].messages = append(batches[index].messages,
					destinationCongestionAbatementSSNM(rangeValue, target.routingContexts),
				)
			}
			batches[index].messages = append(batches[index].messages,
				destinationStateSSNMs(rangeValue, target.routingContexts, rangeValue.State, completion)...)
		}
	}

	var waitGroup sync.WaitGroup
	errorsByBatch := make([]error, len(batches))
	for index := range batches {
		index := index
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			errorsByBatch[index] = batches[index].connection.writeMandatoryControls(
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
		congestion := messages.NewSignallingCongestion(
			networkAppearance, routingContext, affectedPointCode, nil, nil, nil,
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
