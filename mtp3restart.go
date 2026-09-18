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
	publish      func(staged []stagedDestination, completion, wait bool) *SSNMDeliveryError
}

type mtp3RestartRegistry struct {
	procedureMu sync.RWMutex
	mu          sync.Mutex
	generation  uint64
	active      map[uint64]*mtp3RestartEpoch
}

// stagedDestination is one destination's final state held back until the
// restart completes, together with the dimensions that were actually staged.
// RFC 4666 Section 4.5.2.2 keeps availability and congestion apart, so a
// congestion report staged during a restart must not publish an availability
// nobody asked for, and the reverse.
type stagedDestination struct {
	rangeValue DestinationRange
	dimensions destinationDimensions
}

type mtp3RestartEpoch struct {
	affected []DestinationRange
	updates  []stagedDestination
	// completed are the destinations whose recovery has already been published.
	// A partially delivered completion keeps the rest outstanding, and these
	// are released from the restart's DUNA isolation so a later audit answers
	// what was published rather than the isolation state.
	completed []DestinationRange
	// attempted records that a completion has run and left work behind, which
	// is what distinguishes a staged destination from an outstanding one.
	attempted bool
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
			State:                DestinationNetworkState{Availability: DestinationUnavailable},
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
	staged := make([]stagedDestination, len(ranges))
	for index, rangeValue := range ranges {
		staged[index] = stagedDestination{
			rangeValue: rangeValue,
			dimensions: destinationAvailabilityDimension,
		}
	}
	// The handle is live whatever the isolation fan-out reported: the procedure
	// has started, the state is recorded, and a caller that cannot reach some
	// ASPs must still be able to stage and complete it.
	return handle, ssnmDeliveryResult(target.publish(staged, false, true))
}

// Update stages a destination's final state in both dimensions. No recovery
// SSNM is emitted until Complete, and DAUD keeps reporting DUNA for the
// affected scope meanwhile.
func (r *MTP3Restart) Update(destination AffectedDestination, state DestinationNetworkState) error {
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
	// An update always states an availability. It states a congestion only when
	// the CongestionState says something: the zero value is the absence of a
	// congestion statement, not a report of no congestion, so staging it would
	// publish a SCON the caller never asked for and clear a level the SG still
	// holds.
	dimensions := destinationAvailabilityDimension
	if state.Congestion.reported() {
		dimensions |= destinationCongestionDimension
	}
	return stageMTP3RestartRange(r.target.registry, r.generation, stagedDestination{
		rangeValue: rangeValue,
		dimensions: dimensions,
	})
}

// Complete publishes the staged final states and sends the recovery SSNM to the
// currently active and concerned ASPs. Only staged knowledge is published: a
// destination the caller never updated stays unavailable and is never announced
// as recovered.
//
// A fan-out that fails for some ASPs leaves the destinations it could not
// deliver outstanding and the restart alive, so Complete can be called again.
// The retry publishes only what is still outstanding, and the destinations that
// did complete are already out of the restart's DUNA isolation. The returned
// error identifies the associations that failed.
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
	if err == nil {
		r.completed = true
	}
	return err
}

// Outstanding reports the destinations whose recovery a partially delivered
// Complete could not publish. It is empty for a restart that has completed or
// has never been completed.
func (r *MTP3Restart) Outstanding() []AffectedDestination {
	if r == nil || r.target.registry == nil {
		return nil
	}
	registry := r.target.registry
	registry.mu.Lock()
	defer registry.mu.Unlock()
	epoch, ok := registry.active[r.generation]
	if !ok || !epoch.attempted {
		return nil
	}
	outstanding := make([]AffectedDestination, 0, len(epoch.updates))
	for _, staged := range epoch.updates {
		outstanding = append(outstanding, affectedDestinationOf(staged.rangeValue))
	}
	return outstanding
}

// Completed reports the destinations whose recovery has already been published.
func (r *MTP3Restart) Completed() []AffectedDestination {
	if r == nil || r.target.registry == nil {
		return nil
	}
	registry := r.target.registry
	registry.mu.Lock()
	defer registry.mu.Unlock()
	epoch, ok := registry.active[r.generation]
	if !ok {
		return nil
	}
	completed := make([]AffectedDestination, 0, len(epoch.completed))
	for _, rangeValue := range epoch.completed {
		completed = append(completed, affectedDestinationOf(rangeValue))
	}
	return completed
}

func affectedDestinationOf(rangeValue DestinationRange) AffectedDestination {
	return AffectedDestination{
		NetworkAppearance:    rangeValue.NetworkAppearance,
		NetworkAppearanceSet: rangeValue.NetworkAppearanceSet,
		RoutingContext:       rangeValue.RoutingContext,
		RoutingContextSet:    rangeValue.RoutingContextSet,
		PointCode:            rangeValue.PointCode,
		Mask:                 rangeValue.Mask,
	}
}

func stageMTP3RestartRange(registry *mtp3RestartRegistry, generation uint64, staged stagedDestination) error {
	registry.procedureMu.RLock()
	defer registry.procedureMu.RUnlock()
	registry.mu.Lock()
	defer registry.mu.Unlock()
	epoch, ok := registry.active[generation]
	if !ok {
		return ErrStaleMTP3Restart
	}
	if !restartEpochCovers(epoch, staged.rangeValue) {
		return ErrMTP3RestartScope
	}
	epoch.updates = appendRestartUpdate(epoch.updates, staged)
	return nil
}

// stageAnyMTP3RestartRangeLocked requires registry.procedureMu to be held for
// reading, keeping the stage-or-publish decision atomic against Complete.
func stageAnyMTP3RestartRangeLocked(registry *mtp3RestartRegistry, staged stagedDestination) bool {
	if registry == nil {
		return false
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	for _, epoch := range registry.active {
		if restartEpochIsolates(epoch, staged.rangeValue) {
			epoch.updates = appendRestartUpdate(epoch.updates, staged)
			return true
		}
	}
	return false
}

// appendRestartUpdate replaces a destination's staged state, keeping the
// dimensions this update does not carry as the previous one left them and
// moving the entry to the back so the publication order follows the staging
// order.
func appendRestartUpdate(updates []stagedDestination, staged stagedDestination) []stagedDestination {
	key := destinationRangeKey(staged.rangeValue)
	for index := range updates {
		if destinationRangeKey(updates[index].rangeValue) != key {
			continue
		}
		previous := updates[index]
		if !staged.dimensions.carries(destinationAvailabilityDimension) {
			staged.rangeValue.State.Availability = previous.rangeValue.State.Availability
		}
		if !staged.dimensions.carries(destinationCongestionDimension) {
			staged.rangeValue.State.Congestion = previous.rangeValue.State.Congestion
		}
		staged.dimensions |= previous.dimensions
		copy(updates[index:], updates[index+1:])
		updates = updates[:len(updates)-1]
		break
	}
	return append(updates, staged)
}

// completeMTP3Restart publishes each staged destination on its own, so a
// fan-out that fails for one destination leaves the others completed. What
// could not be published stays staged and stays inside the restart's DUNA
// isolation; what was published is recorded as completed and leaves it.
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
	updates := append([]stagedDestination(nil), epoch.updates...)
	registry.mu.Unlock()

	destinations := target.destinations()
	outstanding := make([]stagedDestination, 0, len(updates))
	completed := make([]DestinationRange, 0, len(updates))
	failure := &SSNMDeliveryError{}
	for _, staged := range updates {
		outcome := target.publish([]stagedDestination{staged}, true, true)
		mergeSSNMDeliveryOutcome(failure, outcome)
		if outcome != nil && len(outcome.Failed) > 0 {
			outstanding = append(outstanding, staged)
			continue
		}
		applyStagedDestination(destinations, staged)
		completed = append(completed, staged.rangeValue)
	}

	registry.mu.Lock()
	defer registry.mu.Unlock()
	epoch, ok = registry.active[generation]
	if !ok {
		return ErrStaleMTP3Restart
	}
	epoch.completed = append(epoch.completed, completed...)
	if len(outstanding) > 0 {
		epoch.updates = outstanding
		epoch.attempted = true
		return failure
	}
	delete(registry.active, generation)
	return nil
}

// applyStagedDestination records a staged destination's state in the dimensions
// it actually staged.
func applyStagedDestination(destinations *destinations, staged stagedDestination) {
	if destinations == nil {
		return
	}
	record := destinationRecord{
		rangeValue: normalizeDestinationRange(staged.rangeValue),
		dimensions: staged.dimensions,
	}
	destinations.mu.Lock()
	defer destinations.mu.Unlock()
	if destinations.state == nil {
		destinations.state = make(map[destinationKey]destinationRecord)
	}
	// Every key here is already held by the isolation record the restart wrote,
	// so no record is refused.
	_ = destinations.storeLocked(record)
}

// mergeSSNMDeliveryOutcome folds one fan-out's outcome into the accumulated one,
// keeping every association identifiable on both sides.
func mergeSSNMDeliveryOutcome(into, outcome *SSNMDeliveryError) {
	if outcome == nil {
		return
	}
	into.Successful = appendUniqueAssociationIDs(into.Successful, outcome.Successful)
	into.Failed = append(into.Failed, outcome.Failed...)
}

// ssnmDeliveryResult reports a fan-out outcome as an error only when something
// actually failed. The concrete type is converted here rather than returned
// directly, so a wholly successful fan-out is a nil error rather than a non-nil
// interface holding a nil pointer.
func ssnmDeliveryResult(outcome *SSNMDeliveryError) error {
	if outcome == nil || len(outcome.Failed) == 0 {
		return nil
	}
	return outcome
}

func appendUniqueAssociationIDs(into []AssociationID, more []AssociationID) []AssociationID {
	for _, candidate := range more {
		duplicate := false
		for _, held := range into {
			if held == candidate {
				duplicate = true
				break
			}
		}
		if !duplicate {
			into = append(into, candidate)
		}
	}
	return into
}

// BeginMTP3Restart starts the MTP3 restart procedure of RFC 4666 Section 4.6
// for this SGP Endpoint. Validation and the publication of the isolation state
// are atomic, and the affected scope is isolated across every Listener and
// dialed Association the Endpoint owns.
//
// A non-nil handle is returned even when the isolation fan-out fails for some
// ASPs: the procedure has started and must still be stageable and completable.
// The error identifies the associations that did not accept it.
func (e *Endpoint) BeginMTP3Restart(affected ...AffectedDestination) (*MTP3Restart, error) {
	if e == nil || e.role != RoleSGP {
		return nil, ErrUnsupportedRole
	}
	if !e.beginOperation() {
		return nil, ErrEndpointClosed
	}
	defer e.endOperation()
	return beginMTP3Restart(e.mtp3RestartTarget(), affected...)
}

func (e *Endpoint) mtp3RestartTarget() mtp3RestartTarget {
	return mtp3RestartTarget{
		registry: e.mtp3Restarts,
		closed: func() bool {
			e.mu.Lock()
			defer e.mu.Unlock()
			return e.closed
		},
		prepare: e.prepareLocalDestinationRange,
		destinations: func() *destinations {
			return e.destinations
		},
		publish: func(staged []stagedDestination, completion, wait bool) *SSNMDeliveryError {
			return publishDestinationRanges(e.as, staged, completion, wait)
		},
	}
}

func (l *Listener) destinationRegistry() *destinations {
	l.muConns.Lock()
	defer l.muConns.Unlock()
	if l.destinations == nil {
		l.destinations = newDestinations()
		if l.endpoint != nil {
			l.destinations.setRecordLimit(l.endpoint.destinationRecords)
		}
	}
	return l.destinations
}

// prepareLocalDestinationRange resolves an owner-level destination update
// against the Application Servers this Endpoint knows about. An omitted Network
// Appearance is filled in only when every candidate agrees on one; a named
// Routing Context must be one the Endpoint serves.
func (e *Endpoint) prepareLocalDestinationRange(rangeValue DestinationRange) (DestinationRange, error) {
	if !validDestinationNetworkState(rangeValue.State) {
		return DestinationRange{}, fmt.Errorf(
			"%w: destination availability %d congestion level %d",
			ErrInvalidParameterValue, rangeValue.State.Availability, rangeValue.State.Congestion.Level,
		)
	}
	scope := WireScope{
		NetworkAppearance:    rangeValue.NetworkAppearance,
		NetworkAppearanceSet: rangeValue.NetworkAppearanceSet,
	}
	if rangeValue.RoutingContextSet {
		scope.RoutingContexts = []uint32{rangeValue.RoutingContext}
		scope.RoutingContextSet = true
	}
	if err := validateEndpointSSNMScope(e, scope); err != nil {
		return DestinationRange{}, err
	}
	if !rangeValue.NetworkAppearanceSet {
		networkAppearance, networkAppearanceSet, err :=
			resolveEndpointSSNMNetworkAppearance(e, scope)
		if err != nil {
			return DestinationRange{}, err
		}
		rangeValue.NetworkAppearance, rangeValue.NetworkAppearanceSet =
			networkAppearance, networkAppearanceSet
	}
	return normalizeDestinationRange(rangeValue), nil
}

// restartEpochIsolates reports whether a destination is still held back by this
// restart. A destination whose recovery has already been published has left the
// isolation even while the restart runs on, so a later statement about it is
// published rather than staged again.
func restartEpochIsolates(epoch *mtp3RestartEpoch, rangeValue DestinationRange) bool {
	if !restartEpochCovers(epoch, rangeValue) {
		return false
	}
	for _, completed := range epoch.completed {
		if destinationRangeContains(completed, rangeValue) {
			return false
		}
	}
	return true
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
		isolated := false
		for _, affected := range epoch.affected {
			if destinationRangesOverlap(affected, query) {
				isolated = true
				break
			}
		}
		if !isolated {
			continue
		}
		// A destination whose recovery has already been published has left the
		// isolation, even though the restart it belonged to is still running
		// because other destinations could not be delivered. Answering DUNA for
		// it would contradict the DAVA its own ASPs were just given.
		released := false
		for _, completed := range epoch.completed {
			if destinationRangeContains(completed, query) {
				released = true
				break
			}
		}
		if !released {
			return true
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
			rangeValue, contexts, destinationAvailabilityDimension,
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

// publishDestinationRanges fans one SGP destination statement out to every
// concerned active ASP of the Endpoint's Application Server registry, and
// reports the outcome per association so no successful peer is replayed.
func publishDestinationRanges(
	registry *applicationServers,
	staged []stagedDestination,
	completion, wait bool,
) *SSNMDeliveryError {
	if len(staged) == 0 || registry == nil {
		return nil
	}

	type batch struct {
		association *Association
		messages    []messages.M3UA
	}
	batches := make([]batch, 0)
	indices := make(map[*Association]int)
	for _, entry := range staged {
		dimensions := entry.dimensions
		if completion && entry.rangeValue.State.Availability == DestinationUnavailable {
			// A restart completes by announcing what recovered. A destination
			// left unavailable is already the isolation state every concerned
			// ASP was given, so repeating it announces nothing.
			dimensions &^= destinationAvailabilityDimension
		}
		if dimensions == 0 {
			continue
		}
		scope := destinationKey{
			networkAppearance:    entry.rangeValue.NetworkAppearance,
			networkAppearanceSet: entry.rangeValue.NetworkAppearanceSet,
		}
		if entry.rangeValue.RoutingContextSet {
			scope.routingContext = entry.rangeValue.RoutingContext
			scope.routingContextSet = true
		}
		for _, target := range registry.activeSSNMTargets(scope) {
			index, ok := indices[target.association]
			if !ok {
				index = len(batches)
				indices[target.association] = index
				batches = append(batches, batch{association: target.association})
			}
			for _, routingContexts := range ssnmTargetRoutingContextScopes(target) {
				batches[index].messages = append(batches[index].messages,
					destinationStateSSNMs(entry.rangeValue, routingContexts, dimensions)...)
			}
		}
	}
	if len(batches) == 0 {
		return nil
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

	failure := &SSNMDeliveryError{}
	for index, err := range errorsByBatch {
		associationID := batches[index].association.ID()
		if err == nil {
			failure.Successful = append(failure.Successful, associationID)
			continue
		}
		failure.Failed = append(failure.Failed, SSNMDeliveryFailure{
			Association: associationID,
			Cause:       err,
		})
	}
	return failure
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

// destinationStateSSNMs builds the wire representation of one destination
// statement, in the dimensions it actually makes.
//
// RFC 4666 Section 4.5.2.2 keeps availability and congestion apart, so a
// statement about one of them emits nothing about the other. When a statement
// carries both, SCON precedes the availability message: Section 3.4.4's
// congestion report says nothing about reachability, so the DAVA that follows
// it is what confirms the congested route is still there.
func destinationStateSSNMs(
	rangeValue DestinationRange,
	routingContexts []uint32,
	dimensions destinationDimensions,
) []messages.M3UA {
	var networkAppearance *params.Param
	if rangeValue.NetworkAppearanceSet {
		networkAppearance = params.NewNetworkAppearance(rangeValue.NetworkAppearance)
	}
	var routingContext *params.Param
	if len(routingContexts) > 0 {
		routingContext = params.NewRoutingContext(routingContexts...)
	}
	affectedPointCode := params.NewAffectedPointCodeWithMask(rangeValue.Mask, rangeValue.PointCode)

	out := make([]messages.M3UA, 0, 2)
	if dimensions.carries(destinationCongestionDimension) {
		var congestionIndications *params.Param
		if rangeValue.State.Congestion.LevelSet {
			congestionIndications = params.NewCongestionIndications(rangeValue.State.Congestion.Level)
		}
		out = append(out, messages.NewSignallingCongestion(
			networkAppearance.Copy(), routingContext.Copy(), affectedPointCode.Copy(),
			nil, congestionIndications, nil,
		))
	}
	if !dimensions.carries(destinationAvailabilityDimension) {
		return out
	}
	switch rangeValue.State.Availability {
	case DestinationUnavailable:
		return append(out, messages.NewDestinationUnavailable(
			networkAppearance.Copy(), routingContext.Copy(), affectedPointCode.Copy(), nil,
		))
	case DestinationRestricted:
		return append(out, messages.NewDestinationRestricted(
			networkAppearance.Copy(), routingContext.Copy(), affectedPointCode.Copy(), nil,
		))
	default:
		return append(out, messages.NewDestinationAvailable(
			networkAppearance.Copy(), routingContext.Copy(), affectedPointCode.Copy(), nil,
		))
	}
}
