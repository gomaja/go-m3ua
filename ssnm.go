// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"encoding/binary"
	"fmt"
	"sort"
	"sync"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// DestinationState is the availability of an SS7 destination as most recently
// reported by the peer through the SSNM procedures of RFC 4666 Section 4.5.
type DestinationState uint8

// Destination state definitions.
const (
	// DestinationAvailable is the initial assumption for any destination the
	// peer has not reported on, and the state a DAVA restores.
	DestinationAvailable DestinationState = iota
	// DestinationUnavailable is set by DUNA: the SG cannot reach the
	// destination and the MTP3-User is expected to stop traffic to it.
	DestinationUnavailable
	// DestinationRestricted is set by DRST: the destination is reachable but
	// the SG would prefer traffic went elsewhere.
	DestinationRestricted
	// DestinationCongested is set by SCON: traffic should be reduced.
	DestinationCongested
)

func (s DestinationState) String() string {
	switch s {
	case DestinationAvailable:
		return "Available"
	case DestinationUnavailable:
		return "Unavailable"
	case DestinationRestricted:
		return "Restricted"
	case DestinationCongested:
		return "Congested"
	default:
		return "Unknown"
	}
}

func validDestinationState(state DestinationState) bool {
	return state >= DestinationAvailable && state <= DestinationCongested
}

// DestinationStatus is a change in an SS7 destination's availability, reported
// through SSNM. It is what an MTP3-User needs in order to stop, restart, or
// throttle traffic to a point code.
type DestinationStatus struct {
	// ResyncRequired means one or more preceding status indications were
	// evicted because the receiver did not keep up. Query destination state and
	// treat peer-only SCON or DUPU information as unknown until the peer reports
	// it again.
	ResyncRequired bool
	// PointCode is the affected SS7 destination.
	PointCode uint32
	// Mask is the Affected Point Code mask. It names how many low-order bits
	// of PointCode are wildcarded; values of 24 or greater cover the entire
	// Network Appearance.
	Mask uint8
	// NetworkAppearance is the SS7 network containing PointCode, valid only
	// when NetworkAppearanceSet is true. The parameter is optional, and zero is
	// a legitimate explicit value, so presence cannot be inferred from it.
	NetworkAppearance    uint32
	NetworkAppearanceSet bool
	// RoutingContexts identify the Application Server traffic flows this status
	// concerns, valid only when RoutingContextSet is true. SSNM permits a list,
	// and zero is a legitimate value, so neither length nor value substitutes
	// for an explicit presence bit.
	RoutingContexts   []uint32
	RoutingContextSet bool
	// State is the destination's availability after this message.
	State DestinationState
	// CongestionLevel is the congestion level reported by SCON, if the peer
	// included the Congestion Indications parameter. Zero otherwise.
	CongestionLevel uint8
	// UserCause carries the MTP3-User identity and unavailability cause from
	// DUPU, which reports that a user part — not the destination itself — is
	// unavailable. Zero for other messages.
	UserCause uint32
	// UserPartUnavailable is true when this status came from DUPU. The
	// destination itself remains reachable, so State is left Available and the
	// MTP3-User is expected to act on UserCause instead.
	UserPartUnavailable bool
	// PeerReported is true when this status describes the peer rather than the
	// SS7 network.
	//
	// An SGP receiving a SCON is the case that matters. RFC 4666 Section 3.4.4
	// allows an ASP to send one "indicating that the congestion level of the
	// M3UA layer or the ASP has changed", which says nothing about whether the
	// named destination is reachable through this SG. Such a report is passed
	// on but deliberately kept out of this node's own destination state, so it
	// cannot reach the answer another ASP gets from a DAUD.
	PeerReported bool
	// ConcernedDestination is the originator of the message that triggered an
	// ASP-to-SGP SCON, valid only when ConcernedDestinationSet is true. RFC 4666
	// Section 3.4.4 permits this parameter only in that direction.
	ConcernedDestination    uint32
	ConcernedDestinationSet bool
}

// DestinationRange is one recorded SS7 destination-state update. Mask names
// how many low-order bits of PointCode are wildcarded; values of 24 or greater
// cover the entire Network Appearance. A range without RoutingContextSet is an
// all-Routing-Context baseline and is considered alongside a scoped range.
type DestinationRange struct {
	// NetworkAppearance identifies the SS7 network, valid only when
	// NetworkAppearanceSet is true.
	NetworkAppearance    uint32
	NetworkAppearanceSet bool
	// RoutingContext identifies one Application Server flow, valid only when
	// RoutingContextSet is true. An absent scope is an all-context baseline.
	RoutingContext    uint32
	RoutingContextSet bool
	// PointCode is the affected 24-bit SS7 destination or range member.
	PointCode uint32
	// Mask is the number of wildcarded low-order point-code bits.
	Mask uint8
	// State is the availability installed by this update.
	State DestinationState
}

type destinationKey struct {
	networkAppearance    uint32
	networkAppearanceSet bool
	routingContext       uint32
	routingContextSet    bool
	routingContextScope  string
	pointCode            uint32
	mask                 uint8
}

type destinationRecord struct {
	rangeValue          DestinationRange
	routingContexts     []uint32
	routingContextScope string
	sequence            uint64
}

type destinationPause struct {
	rangeValue      DestinationRange
	routingContexts []uint32
}

// destinations tracks destination ranges by Network Appearance and Routing
// Context. Updates are sequenced so the newest range covering a query wins.
type destinations struct {
	mu       sync.RWMutex
	state    map[destinationKey]destinationRecord
	sequence uint64
}

func newDestinations() *destinations {
	return &destinations{state: make(map[destinationKey]destinationRecord)}
}

// The nil receiver checks below keep an Association that was assembled directly, rather
// than through Dial or Accept, from crashing on the first SSNM message a peer
// sends. Tracking is inert in that case; state reads report the default.

func (d *destinations) set(key destinationKey, state DestinationState) {
	d.setRanges([]DestinationRange{{
		NetworkAppearance:    key.networkAppearance,
		NetworkAppearanceSet: key.networkAppearanceSet,
		RoutingContext:       key.routingContext,
		RoutingContextSet:    key.routingContextSet,
		PointCode:            key.pointCode,
		Mask:                 key.mask,
		State:                state,
	}})
}

func (d *destinations) get(key destinationKey) DestinationState {
	state, _ := d.lookup(key)
	return state
}

func (d *destinations) setRanges(ranges []DestinationRange) {
	if d == nil || len(ranges) == 0 {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.state == nil {
		d.state = make(map[destinationKey]destinationRecord)
	}
	for _, rangeValue := range ranges {
		d.storeLocked(destinationRecord{rangeValue: normalizeDestinationRange(rangeValue)})
	}
}

// setScopedRanges records point-code updates sharing one explicit Routing
// Context scope. One SSNM message applies every listed point code to the same
// set of contexts; storing that set once avoids materializing the product of
// the two legal variable-length parameter lists.
func (d *destinations) setScopedRanges(routingContexts []uint32, ranges []DestinationRange) {
	if d == nil || len(routingContexts) == 0 || len(ranges) == 0 {
		return
	}

	canonical, scope := canonicalRoutingContextScope(routingContexts)
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.state == nil {
		d.state = make(map[destinationKey]destinationRecord)
	}
	for _, rangeValue := range ranges {
		rangeValue = normalizeDestinationRange(rangeValue)
		rangeValue.RoutingContext = 0
		rangeValue.RoutingContextSet = true
		d.storeLocked(destinationRecord{
			rangeValue:          rangeValue,
			routingContexts:     canonical,
			routingContextScope: scope,
		})
	}
}

func canonicalRoutingContextScope(routingContexts []uint32) ([]uint32, string) {
	canonical := append([]uint32(nil), routingContexts...)
	sort.Slice(canonical, func(i, j int) bool { return canonical[i] < canonical[j] })
	unique := canonical[:0]
	for _, routingContext := range canonical {
		if len(unique) == 0 || unique[len(unique)-1] != routingContext {
			unique = append(unique, routingContext)
		}
	}
	canonical = unique
	encoded := make([]byte, len(canonical)*4)
	for index, routingContext := range canonical {
		binary.BigEndian.PutUint32(encoded[index*4:], routingContext)
	}
	return canonical, string(encoded)
}

func (d *destinations) storeLocked(record destinationRecord) {
	d.sequence++
	if d.sequence == 0 {
		d.renumberLocked()
		d.sequence++
	}
	record.sequence = d.sequence
	d.state[destinationRecordKey(record)] = record
}

func (d *destinations) renumberLocked() {
	records := make([]destinationRecord, 0, len(d.state))
	for _, record := range d.state {
		records = append(records, record)
	}
	sort.Slice(records, func(i, j int) bool {
		return records[i].sequence < records[j].sequence
	})
	d.sequence = 0
	for _, record := range records {
		d.sequence++
		record.sequence = d.sequence
		d.state[destinationRecordKey(record)] = record
	}
}

func normalizeDestinationRange(rangeValue DestinationRange) DestinationRange {
	rangeValue.PointCode &= 0x00ffffff
	if !rangeValue.NetworkAppearanceSet {
		rangeValue.NetworkAppearance = 0
	}
	if !rangeValue.RoutingContextSet {
		rangeValue.RoutingContext = 0
	}
	return rangeValue
}

func destinationRangeKey(rangeValue DestinationRange) destinationKey {
	return destinationKey{
		networkAppearance:    rangeValue.NetworkAppearance,
		networkAppearanceSet: rangeValue.NetworkAppearanceSet,
		routingContext:       rangeValue.RoutingContext,
		routingContextSet:    rangeValue.RoutingContextSet,
		pointCode:            destinationRangePrefix(rangeValue.PointCode, rangeValue.Mask),
		mask:                 rangeValue.Mask,
	}
}

func destinationRecordKey(record destinationRecord) destinationKey {
	key := destinationRangeKey(record.rangeValue)
	key.routingContextScope = record.routingContextScope
	return key
}

func effectiveDestinationMask(mask uint8) uint8 {
	if mask > 24 {
		return 24
	}
	return mask
}

func destinationRangePrefix(pointCode uint32, mask uint8) uint32 {
	pointCode &= 0x00ffffff
	effectiveMask := effectiveDestinationMask(mask)
	if effectiveMask == 24 {
		return 0
	}
	return pointCode & (uint32(0x00ffffff) << effectiveMask)
}

func destinationRangeCovers(stored DestinationRange, pointCode uint32, mask uint8) bool {
	if effectiveDestinationMask(stored.Mask) < effectiveDestinationMask(mask) {
		return false
	}
	return destinationRangePrefix(stored.PointCode, stored.Mask) ==
		destinationRangePrefix(pointCode, stored.Mask)
}

func destinationScopeMatches(stored DestinationRange, query destinationKey) bool {
	if stored.NetworkAppearance != query.networkAppearance ||
		stored.NetworkAppearanceSet != query.networkAppearanceSet {
		return false
	}
	if !stored.RoutingContextSet {
		return true
	}
	return query.routingContextSet && stored.RoutingContext == query.routingContext
}

func destinationRecordScopeMatches(record destinationRecord, query destinationKey) bool {
	if len(record.routingContexts) == 0 {
		return destinationScopeMatches(record.rangeValue, query)
	}
	if record.rangeValue.NetworkAppearance != query.networkAppearance ||
		record.rangeValue.NetworkAppearanceSet != query.networkAppearanceSet ||
		!query.routingContextSet {
		return false
	}
	index := sort.Search(len(record.routingContexts), func(index int) bool {
		return record.routingContexts[index] >= query.routingContext
	})
	return index < len(record.routingContexts) && record.routingContexts[index] == query.routingContext
}

func destinationRecordRangeForScope(record destinationRecord, scope destinationKey) DestinationRange {
	rangeValue := record.rangeValue
	if len(record.routingContexts) > 0 {
		rangeValue.RoutingContext = scope.routingContext
		rangeValue.RoutingContextSet = true
	}
	return rangeValue
}

func destinationRecordRoutingContexts(record destinationRecord) []uint32 {
	if len(record.routingContexts) > 0 {
		return record.routingContexts
	}
	if record.rangeValue.RoutingContextSet {
		return []uint32{record.rangeValue.RoutingContext}
	}
	return nil
}

// lookup is get, and also reports whether the destination is one this node has
// ever been told about.
//
// The distinction matters when answering a DAUD: RFC 4666 Section 4.5.3 says
// "An SG SHOULD respond with a DUNA message when DAUD was received with an
// unknown Signalling Point Code", which cannot be honoured if an unknown point
// code is indistinguishable from a known reachable one.
func (d *destinations) lookup(key destinationKey) (DestinationState, bool) {
	return d.lookupRange(key, key.pointCode, key.mask)
}

func (d *destinations) lookupRange(scope destinationKey, pointCode uint32, mask uint8) (DestinationState, bool) {
	if d == nil {
		return DestinationAvailable, false
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.lookupRangeLocked(scope, pointCode, mask)
}

func (d *destinations) lookupRangeLocked(scope destinationKey, pointCode uint32, mask uint8) (DestinationState, bool) {
	var newest destinationRecord
	known := false
	for _, record := range d.state {
		if !destinationRecordScopeMatches(record, scope) ||
			!destinationRangeCovers(record.rangeValue, pointCode, mask) {
			continue
		}
		if !known || record.sequence > newest.sequence {
			newest = record
			known = true
		}
	}
	if known {
		return newest.rangeValue.State, true
	}
	return DestinationAvailable, false
}

func (d *destinations) snapshotForScope(scope destinationKey) map[uint32]DestinationState {
	if d == nil {
		return map[uint32]DestinationState{}
	}
	d.mu.RLock()
	defer d.mu.RUnlock()

	out := make(map[uint32]DestinationState)
	sequences := make(map[uint32]uint64)
	for _, record := range d.state {
		if record.rangeValue.Mask != 0 || !destinationRecordScopeMatches(record, scope) {
			continue
		}
		pointCode := record.rangeValue.PointCode
		if sequence, ok := sequences[pointCode]; !ok || record.sequence > sequence {
			out[pointCode] = record.rangeValue.State
			sequences[pointCode] = record.sequence
		}
	}
	return out
}

func (d *destinations) rangesForScope(scope destinationKey) []DestinationRange {
	if d == nil {
		return []DestinationRange{}
	}
	d.mu.RLock()
	defer d.mu.RUnlock()

	records := make([]destinationRecord, 0, len(d.state))
	for _, record := range d.state {
		if destinationRecordScopeMatches(record, scope) {
			records = append(records, record)
		}
	}
	sort.Slice(records, func(i, j int) bool {
		return records[i].sequence < records[j].sequence
	})
	out := make([]DestinationRange, len(records))
	for i, record := range records {
		out[i] = destinationRecordRangeForScope(record, scope)
	}
	return out
}

func (d *destinations) pause() []destinationPause {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	records := make([]destinationRecord, 0, len(d.state))
	for _, record := range d.state {
		if record.rangeValue.State != DestinationUnavailable {
			records = append(records, record)
		}
	}
	sort.Slice(records, func(i, j int) bool {
		return records[i].sequence < records[j].sequence
	})
	paused := make([]destinationPause, 0, len(records))
	for _, record := range records {
		record.rangeValue.State = DestinationUnavailable
		d.storeLocked(record)
		paused = append(paused, destinationPause{
			rangeValue:      record.rangeValue,
			routingContexts: destinationRecordRoutingContexts(record),
		})
	}
	return paused
}

func appearanceOf(param *params.Param) (uint32, bool) {
	if param == nil || param.Tag != params.NetworkAppearance || len(param.Data) != 4 {
		return 0, false
	}
	return param.NetworkAppearance(), true
}

func (c *Association) destinationKey(networkAppearance *params.Param, pointCode uint32) destinationKey {
	appearance, set := appearanceOf(networkAppearance)
	if !set && c.cfg != nil {
		appearance, set = appearanceOf(c.cfg.NetworkAppearance)
	}
	return destinationKey{
		networkAppearance:    appearance,
		networkAppearanceSet: set,
		pointCode:            pointCode,
	}
}

func (c *Association) legacyDestinationScope(networkAppearance *params.Param) destinationKey {
	scope := c.destinationKey(networkAppearance, 0)
	configured := c.configuredRoutingContexts()
	if len(configured) == 1 {
		scope.routingContext = configured[0]
		scope.routingContextSet = true
	}
	return scope
}

func (c *Association) destinationRoutingContexts(routingContext *params.Param) ([]uint32, bool) {
	if routingContext != nil {
		return append([]uint32(nil), routingContext.RoutingContexts()...), true
	}
	configured := c.configuredRoutingContexts()
	if len(configured) != 1 {
		return nil, false
	}
	return []uint32{configured[0]}, true
}

// DestinationState reports the availability of an SS7 destination as most
// recently advertised by the peer. On a single-Routing-Context association it
// resolves that flow. On a multi-Routing-Context association the legacy API is
// ambiguous and therefore sees only all-context baselines; use
// DestinationStateForNetworkAndRoutingContext for a per-flow answer.
// Destinations the peer has not reported on are Available, since SSNM carries
// changes rather than a full inventory.
func (c *Association) DestinationState(pointCode uint32) DestinationState {
	scope := c.legacyDestinationScope(nil)
	return c.destinations.get(destinationKey{
		networkAppearance:    scope.networkAppearance,
		networkAppearanceSet: scope.networkAppearanceSet,
		routingContext:       scope.routingContext,
		routingContextSet:    scope.routingContextSet,
		pointCode:            pointCode,
	})
}

// DestinationStateForNetwork reports a destination's state within an explicit
// Network Appearance. On a multi-Routing-Context association it sees only
// all-context baselines; use DestinationStateForNetworkAndRoutingContext for a
// per-flow answer.
func (c *Association) DestinationStateForNetwork(networkAppearance, pointCode uint32) DestinationState {
	scope := c.legacyDestinationScope(params.NewNetworkAppearance(networkAppearance))
	return c.destinations.get(destinationKey{
		networkAppearance:    scope.networkAppearance,
		networkAppearanceSet: scope.networkAppearanceSet,
		routingContext:       scope.routingContext,
		routingContextSet:    scope.routingContextSet,
		pointCode:            pointCode,
	})
}

// DestinationStateForNetworkAndRoutingContext reports a destination's state
// in one explicit Network Appearance and Routing Context. All-context baseline
// ranges participate, and the newest covering update wins.
func (c *Association) DestinationStateForNetworkAndRoutingContext(networkAppearance, routingContext, pointCode uint32) DestinationState {
	return c.destinations.get(destinationKey{
		networkAppearance:    networkAppearance,
		networkAppearanceSet: true,
		routingContext:       routingContext,
		routingContextSet:    true,
		pointCode:            pointCode,
	})
}

// DestinationStates returns the exact, Mask-zero destination records visible
// through DestinationState. The legacy map cannot represent ranges; use
// DestinationRanges for a lossless snapshot.
func (c *Association) DestinationStates() map[uint32]DestinationState {
	return c.destinations.snapshotForScope(c.legacyDestinationScope(nil))
}

// DestinationStatesForNetwork returns the exact, Mask-zero records visible for
// an explicit Network Appearance. On a multi-Routing-Context association it is
// limited to all-context baselines. Use
// DestinationRangesForNetworkAndRoutingContext for a lossless per-flow view.
func (c *Association) DestinationStatesForNetwork(networkAppearance uint32) map[uint32]DestinationState {
	return c.destinations.snapshotForScope(
		c.legacyDestinationScope(params.NewNetworkAppearance(networkAppearance)))
}

// DestinationRanges returns every range visible through DestinationState in
// update order. On a multi-Routing-Context association it contains only
// all-context baselines.
func (c *Association) DestinationRanges() []DestinationRange {
	return c.destinations.rangesForScope(c.legacyDestinationScope(nil))
}

// DestinationRangesForNetwork returns every range visible for an explicit
// Network Appearance in update order. On a multi-Routing-Context association
// it contains only all-context baselines.
func (c *Association) DestinationRangesForNetwork(networkAppearance uint32) []DestinationRange {
	return c.destinations.rangesForScope(
		c.legacyDestinationScope(params.NewNetworkAppearance(networkAppearance)))
}

// DestinationRangesForNetworkAndRoutingContext returns every all-context and
// per-context range visible in one Network Appearance and Routing Context, in
// update order.
func (c *Association) DestinationRangesForNetworkAndRoutingContext(networkAppearance, routingContext uint32) []DestinationRange {
	return c.destinations.rangesForScope(destinationKey{
		networkAppearance:    networkAppearance,
		networkAppearanceSet: true,
		routingContext:       routingContext,
		routingContextSet:    true,
	})
}

// SignallingStatus returns the channel on which SSNM status changes are
// delivered. An MTP3-User reads it to stop, restart, or throttle traffic to a
// point code as RFC 4666 Section 4.5 requires.
//
// The channel is bounded so a peer cannot block the dispatcher. If the reader
// falls behind, the oldest queued status is replaced with a ResyncRequired
// marker; callers must then query destination state and treat peer-only SCON
// and DUPU information as unknown until the peer reports it again.
//
// Closing the Association closes the channel, so
//
//	for st := range association.SignallingStatus() { ... }
//
// terminates with the association rather than parking forever. Anything already
// buffered is still delivered before the range ends.
func (c *Association) SignallingStatus() <-chan *DestinationStatus {
	return c.statusChan
}

// notifyStatus delivers an SSNM status change without blocking the dispatcher.
//
// The lock is held only across a non-blocking send, and exists so that SSNM
// arriving while the association is being torn down cannot send on the channel
// Close has just closed.
func (c *Association) notifyStatus(s *DestinationStatus) {
	c.muStatus.Lock()
	defer c.muStatus.Unlock()

	if c.statusClosed {
		return
	}
	if cap(c.statusChan) > 0 && len(c.statusChan) == cap(c.statusChan) {
		select {
		case <-c.statusChan:
		default:
		}
		select {
		case c.statusChan <- &DestinationStatus{ResyncRequired: true}:
		default:
		}
		return
	}
	status := copyDestinationStatus(s)

	select {
	case c.statusChan <- status:
	default:
	}
}

func copyDestinationStatus(status *DestinationStatus) *DestinationStatus {
	if status == nil {
		return nil
	}
	copy := *status
	copy.RoutingContexts = append([]uint32(nil), status.RoutingContexts...)
	return &copy
}

// pauseDestinations reports every destination the peer had told this ASP about
// as unavailable, and records it.
//
// RFC 4666 Section 4.3.3, on the association going away: "At an ASP, the
// MTP3-User will be informed of the unavailability of any affected SS7
// destinations through the use of MTP-PAUSE indication primitives." This
// library's equivalent of that primitive is a status on SignallingStatus()
// backed by DestinationState, and neither said anything: a user told a
// destination was available kept being told so long after the only route to it
// had gone.
//
// Scoped to the ASP, as the RFC scopes it. An SGP's recorded states are its own
// view for answering audits, not something a peer told it.
func (c *Association) pauseDestinations() {
	if c.role != RoleASP {
		return
	}

	for _, paused := range c.destinations.pause() {
		rangeValue := paused.rangeValue
		status := &DestinationStatus{
			PointCode:            rangeValue.PointCode,
			Mask:                 rangeValue.Mask,
			NetworkAppearance:    rangeValue.NetworkAppearance,
			NetworkAppearanceSet: rangeValue.NetworkAppearanceSet,
			State:                DestinationUnavailable,
		}
		if len(paused.routingContexts) > 0 {
			status.RoutingContexts = paused.routingContexts
			status.RoutingContextSet = true
		}
		c.notifyStatus(status)
	}
}

// closeStatus closes statusChan exactly once, so a caller ranging over
// SignallingStatus() sees the association end.
func (c *Association) closeStatus() {
	c.muStatus.Lock()
	defer c.muStatus.Unlock()

	if c.statusClosed {
		return
	}
	c.statusClosed = true
	close(c.statusChan)
}

// applySSNM records a destination state change and reports it to the user.
// Affected Point Code is Mandatory in every SSNM message (RFC 4666 Sections
// 3.4.1 to 3.4.6) and may carry several point codes, each of which is updated.
func (c *Association) applySSNM(networkAppearance, routingContext, apc *params.Param, state DestinationState, mutate func(*DestinationStatus)) error {
	if apc == nil {
		return ErrMissingAffectedPointCode
	}

	pcs := apc.AffectedPointCodes()
	if len(pcs) == 0 {
		return ErrMissingAffectedPointCode
	}
	masks := apc.AffectedPointCodeMasks()
	if len(masks) != len(pcs) {
		return ErrInvalidParameterValue
	}
	routingContexts, routingContextSet := c.destinationRoutingContexts(routingContext)
	appearance := c.destinationKey(networkAppearance, 0)
	statusScope := newDestinationStatusScope(networkAppearance, routingContext)
	statuses := make([]*DestinationStatus, 0, len(pcs))
	updates := make([]DestinationRange, 0, len(pcs))

	for index, pc := range pcs {
		// DUPU reports an unavailable user part at a destination that is
		// itself still reachable, so it must not overwrite the destination's
		// own availability. mutate marks those.
		status := &DestinationStatus{PointCode: pc, Mask: masks[index], State: state}
		statusScope.apply(status)
		if mutate != nil {
			mutate(status)
		}
		if !status.UserPartUnavailable {
			updates = append(updates, DestinationRange{
				NetworkAppearance:    appearance.networkAppearance,
				NetworkAppearanceSet: appearance.networkAppearanceSet,
				PointCode:            pc,
				Mask:                 masks[index],
				State:                status.State,
			})
		}
		statuses = append(statuses, status)
	}

	if routingContextSet {
		c.destinations.setScopedRanges(routingContexts, updates)
	} else {
		c.destinations.setRanges(updates)
	}
	for _, status := range statuses {
		c.notifyStatus(status)
	}

	return nil
}

// reportSSNM reports a peer's SSNM message to the user without recording it as
// this node's own view of the SS7 network.
//
// It is what an SGP does with an ASP's SCON: the report is real and worth
// surfacing, but it describes the ASP rather than a destination, so it must not
// reach the map the SGP answers a DAUD from.
func (c *Association) reportSSNM(networkAppearance, routingContext, apc *params.Param, mutate func(*DestinationStatus)) error {
	if apc == nil {
		return ErrMissingAffectedPointCode
	}
	pcs := apc.AffectedPointCodes()
	if len(pcs) == 0 {
		return ErrMissingAffectedPointCode
	}
	masks := apc.AffectedPointCodeMasks()
	if len(masks) != len(pcs) {
		return ErrInvalidParameterValue
	}

	statusScope := newDestinationStatusScope(networkAppearance, routingContext)
	for index, pc := range pcs {
		status := &DestinationStatus{
			PointCode: pc,
			Mask:      masks[index],
			State:     DestinationCongested,
		}
		statusScope.apply(status)
		if mutate != nil {
			mutate(status)
		}
		c.notifyStatus(status)
	}
	return nil
}

type destinationStatusScope struct {
	networkAppearance    uint32
	networkAppearanceSet bool
	routingContexts      []uint32
	routingContextSet    bool
}

func newDestinationStatusScope(networkAppearance, routingContext *params.Param) destinationStatusScope {
	scope := destinationStatusScope{}
	if networkAppearance != nil {
		scope.networkAppearance = networkAppearance.NetworkAppearance()
		scope.networkAppearanceSet = true
	}
	if routingContext != nil {
		scope.routingContexts = append([]uint32(nil), routingContext.RoutingContexts()...)
		scope.routingContextSet = true
	}
	return scope
}

func (s destinationStatusScope) apply(status *DestinationStatus) {
	status.NetworkAppearance = s.networkAppearance
	status.NetworkAppearanceSet = s.networkAppearanceSet
	status.RoutingContexts = s.routingContexts
	status.RoutingContextSet = s.routingContextSet
}

// ssnmAllowed reports whether an SSNM message may be acted on in the current
// state.
//
// RFC 4666 Section 4.3.1: while ASP-INACTIVE "the ASP/IPSP SHOULD NOT be sent
// any DATA or SSNM messages for the AS for which the ASP/IPSP is inactive", and
// an ASP-DOWN peer "SHOULD NOT be sent any M3UA messages, with the exception of
// Heartbeat, ASP Down Ack, and Error messages". Receiving one anyway is a
// protocol error on the peer's part, so it is reported rather than applied:
// acting on destination state we are not entitled to receive would let an
// out-of-state peer steer traffic.
func (c *Association) ssnmAllowed() bool {
	return c.State() == StateASPActive
}

// ssnmAllowedDuringActivation is ssnmAllowed widened by the window RFC 4666
// Section 4.5.1 opens: "For the newly activating ASP from which the SGP has
// received an ASP Active message, these DUNA, DRST, and SCON messages MAY be
// sent before sending the ASP Active Ack that completes the activation
// procedure."
//
// The ASP is in ASP-INACTIVE for the whole of that window — it reaches
// ASP-ACTIVE only on the Ack that follows — so requiring ASP-ACTIVE discarded
// exactly the messages the section exists to deliver, whose stated purpose is
// "to prevent the ASP from sending traffic for destinations that it might not
// otherwise know that are inaccessible, restricted, or congested".
//
// The exception is tied to an actual outstanding ASP Active request. Merely
// being ASP-INACTIVE does not prove that the peer has received one, and a stray
// SSNM must not be allowed to steer local routing state.
func (c *Association) ssnmAllowedDuringActivation() bool {
	if c.State() == StateASPActive {
		return true
	}
	return c.role == RoleASP && c.State() == StateASPInactive &&
		len(c.pendingTAckRoutingContexts(requestAspActive)) > 0
}

// validateSSNMRoutingContext applies both parts of SSNM's Conditional Routing
// Context rule. A present value must be configured, while omission is valid
// only when the association does not carry several traffic flows.
func (c *Association) validateSSNMRoutingContext(routingContext *params.Param) error {
	if err := c.validateRoutingContext(routingContext); err != nil {
		return err
	}
	if routingContext != nil {
		return nil
	}
	if len(c.configuredRoutingContexts()) > 1 {
		return ErrMissingRoutingContext
	}
	return nil
}

// validateSSNMScope reports whether an otherwise valid SSNM message may be
// applied to the Routing Contexts it names. The bool is false only for the
// RFC-permitted ASP-side silent-discard case; an SGP reports an Unexpected
// Message when an ASP originates SSNM for an AS in which it is inactive.
func (c *Association) validateSSNMScope(msg messages.M3UA, routingContext *params.Param, duringActivation bool) (bool, error) {
	if err := c.validateSSNMRoutingContext(routingContext); err != nil {
		return false, err
	}
	if c.ssnmRoutingContextsAllowed(routingContext, duringActivation) {
		return true, nil
	}
	if c.role == RoleASP {
		return false, nil
	}
	return false, NewUnexpectedMessageError(msg)
}

func (c *Association) ssnmRoutingContextsAllowed(routingContext *params.Param, duringActivation bool) bool {
	routingContexts := routingContext.RoutingContexts()
	if routingContext == nil {
		routingContexts = c.configuredRoutingContexts()
	}

	pendingActivation := make(map[uint32]struct{})
	if duringActivation && c.role == RoleASP {
		for _, rtCtx := range c.pendingTAckRoutingContexts(requestAspActive) {
			pendingActivation[rtCtx] = struct{}{}
		}
	}

	for _, rtCtx := range routingContexts {
		if c.role == RoleSGP {
			if !c.activeForRoutingContext(rtCtx) {
				return false
			}
			continue
		}
		// Section 4.5.1 opens this window before the first ASP Active Ack,
		// while the association is ASP-INACTIVE. On an association already
		// active for another AS, only RCs in the still-pending ASP Active
		// request receive the same exception.
		if _, ok := pendingActivation[rtCtx]; ok {
			continue
		}
		if c.State() == StateASPActive && c.routingContextAcked(rtCtx) {
			continue
		}
		return false
	}
	return true
}

// handleDestinationUnavailable processes a DUNA.
//
// RFC 4666 Section 3.4.1: "The DUNA message is sent from an SGP in an SG to all
// concerned ASPs to indicate that the SG has determined that one or more SS7
// destinations are unreachable." The MTP3-User at the ASP "is expected to stop
// traffic to the affected destination via the SG".
//
// SGP to ASP, so an SGP that receives one reports an Error instead of applying
// it: a peer must not be able to steer an SG's own routing state.
func (c *Association) handleDestinationUnavailable(d *messages.DestinationUnavailable) error {
	if c.role != RoleASP {
		return NewUnexpectedMessageError(d)
	}
	if !c.ssnmAllowedDuringActivation() {
		return NewUnexpectedMessageError(d)
	}
	if err := c.validateNetworkAppearance(d.NetworkAppearance); err != nil {
		return err
	}

	allowed, err := c.validateSSNMScope(d, d.RoutingContext, true)
	if err != nil {
		return err
	}
	if !allowed {
		return nil
	}

	return c.applySSNM(d.NetworkAppearance, d.RoutingContext, d.AffectedPointCode, DestinationUnavailable, nil)
}

// handleDestinationAvailable processes a DAVA.
//
// RFC 4666 Section 3.4.2: sent from an SGP "to indicate that the SG has
// determined that one or more SS7 destinations are now reachable", restoring
// traffic the matching DUNA stopped.
func (c *Association) handleDestinationAvailable(d *messages.DestinationAvailable) error {
	if c.role != RoleASP {
		return NewUnexpectedMessageError(d)
	}
	if !c.ssnmAllowedDuringActivation() {
		return NewUnexpectedMessageError(d)
	}
	if err := c.validateNetworkAppearance(d.NetworkAppearance); err != nil {
		return err
	}

	// Section 4.6 permits DAVA before ASP Active Ack when the SGP is completing
	// an MTP3 restart, in the same real pending-activation scope Section 4.5.1
	// uses for DUNA, DRST, and SCON.
	allowed, err := c.validateSSNMScope(d, d.RoutingContext, true)
	if err != nil {
		return err
	}
	if !allowed {
		return nil
	}

	return c.applySSNM(d.NetworkAppearance, d.RoutingContext, d.AffectedPointCode, DestinationAvailable, nil)
}

// handleDestinationRestricted processes a DRST.
//
// RFC 4666 Section 3.4.6: an optional message telling the ASP that a
// destination is reachable but that traffic should preferably be sent
// elsewhere.
func (c *Association) handleDestinationRestricted(d *messages.DestinationRestricted) error {
	if c.role != RoleASP {
		return NewUnexpectedMessageError(d)
	}
	if !c.ssnmAllowedDuringActivation() {
		return NewUnexpectedMessageError(d)
	}
	if err := c.validateNetworkAppearance(d.NetworkAppearance); err != nil {
		return err
	}

	allowed, err := c.validateSSNMScope(d, d.RoutingContext, true)
	if err != nil {
		return err
	}
	if !allowed {
		return nil
	}

	return c.applySSNM(d.NetworkAppearance, d.RoutingContext, d.AffectedPointCode, DestinationRestricted, nil)
}

// handleSignallingCongestion processes a SCON.
//
// RFC 4666 Section 3.4.4: sent to indicate congestion towards a destination so
// the MTP3-User can reduce traffic. The Congestion Indications parameter is
// optional, so its absence is not an error.
func (c *Association) handleSignallingCongestion(s *messages.SignallingCongestion) error {
	// SCON is the one SSNM message that travels in both directions. RFC 4666
	// Section 3.4.4: "The SCON message MAY also be sent from the M3UA layer of
	// an ASP to an M3UA peer, indicating that the congestion level of the M3UA
	// layer or the ASP has changed." Gating it to the ASP made an SGP answer a
	// congested ASP with "Unexpected Message" and learn nothing from it.
	if err := c.validateNetworkAppearance(s.NetworkAppearance); err != nil {
		return err
	}
	if c.role == RoleASP && !c.ssnmAllowedDuringActivation() {
		return NewUnexpectedMessageError(s)
	}
	if c.role == RoleSGP && !c.ssnmAllowed() {
		return NewUnexpectedMessageError(s)
	}
	allowed, err := c.validateSSNMScope(s, s.RoutingContext, c.role == RoleASP)
	if err != nil {
		return err
	}
	if !allowed {
		return nil
	}
	if c.role == RoleASP && s.ConcernedDestination != nil {
		return ErrInvalidParameterValue
	}

	// The Congestion Level table in Section 3.4.4 makes 0 "No Congestion or
	// Undefined" — the report that congestion has cleared, not a report of
	// congestion. Section 4.5.3's implementation note reads it the same way,
	// telling an ASP not to start an audit "for the case of a received SCON
	// message containing a congestion level value of 'no congestion' or
	// 'undefined' (i.e., congestion Level = "0")". Recording it as congestion
	// throttled a destination on the very message announcing its recovery.
	//
	// The parameter is optional and absent in networks without multiple
	// congestion levels, where the message itself is the congestion report, so
	// only an explicit 0 clears.
	level := uint8(0)
	congested := true
	if s.CongestionIndications != nil {
		congestionLevel := s.CongestionIndications.CongestionLevel()
		if congestionLevel > 3 {
			return ErrInvalidParameterValue
		}
		level = uint8(congestionLevel)
		congested = level != 0
	}

	// The two directions do not mean the same thing. From an SGP the message
	// is about an SS7 destination the SG has observed; from an ASP it is about
	// "the congestion level of the M3UA layer or the ASP" — a statement about
	// that one peer. Writing a peer's report into this node's destination map
	// let any ASP make the SG report SS7 congestion that does not exist, to
	// every other ASP that audited it (Section 4.5.3).
	if c.role == RoleSGP {
		c.peerCongestion.Store(uint32(level))
		return c.reportSSNM(s.NetworkAppearance, s.RoutingContext, s.AffectedPointCode, func(st *DestinationStatus) {
			st.CongestionLevel = level
			st.PeerReported = true
			if s.ConcernedDestination != nil {
				st.ConcernedDestination = s.ConcernedDestination.ConcernedDestination()
				st.ConcernedDestinationSet = true
			}
		})
	}

	state := DestinationCongested
	if !congested {
		state = DestinationAvailable
	}
	return c.applySSNM(s.NetworkAppearance, s.RoutingContext, s.AffectedPointCode, state, func(st *DestinationStatus) {
		st.CongestionLevel = level
	})
}

// handleDestinationUserPartUnavailable processes a DUPU.
//
// RFC 4666 Section 3.4.5: reports that a *user part* at an otherwise reachable
// destination is unavailable, so the destination's own availability is left
// alone and the cause is passed to the MTP3-User instead.
func (c *Association) handleDestinationUserPartUnavailable(d *messages.DestinationUserPartUnavailable) error {
	if c.role != RoleASP {
		return NewUnexpectedMessageError(d)
	}
	if !c.ssnmAllowed() {
		return NewUnexpectedMessageError(d)
	}
	if err := c.validateNetworkAppearance(d.NetworkAppearance); err != nil {
		return err
	}

	// RFC 4666 Section 3.4.5 lists User/Cause as Mandatory in DUPU, alongside
	// Affected Point Code. A DUPU without it says a user part is unavailable
	// without saying which or why, which is not actionable by an MTP3-User —
	// and it was accepted, reporting a cause of 0 as though the peer had sent
	// one. Its sibling mandatory parameter was already enforced.
	//
	// Affected Point Code is checked first because it is Mandatory in every
	// SSNM message (Sections 3.4.1 to 3.4.6), so a message missing both is
	// reported against the requirement they all share.
	if d.AffectedPointCode == nil {
		return ErrMissingAffectedPointCode
	}
	if d.UserCause == nil {
		return ErrMissingUserCause
	}
	allowed, err := c.validateSSNMScope(d, d.RoutingContext, false)
	if err != nil {
		return err
	}
	if !allowed {
		return nil
	}

	// Section 3.4.5 narrows the Affected Point Code parameter for DUPU alone:
	// the format is DUNA's "except that the Mask field is not used and only a
	// single Affected DPC is included.  Ranges and lists of Affected DPCs
	// cannot be signaled in a DUPU message". Section 3.8.1 uses this very case
	// as its example of Invalid Parameter Value.
	if pcs := d.AffectedPointCode.AffectedPointCodes(); len(pcs) > 1 {
		return ErrInvalidParameterValue
	}
	for _, m := range d.AffectedPointCode.AffectedPointCodeMasks() {
		if m != 0 {
			return ErrInvalidParameterValue
		}
	}

	return c.applySSNM(d.NetworkAppearance, d.RoutingContext, d.AffectedPointCode, DestinationAvailable, func(st *DestinationStatus) {
		st.UserPartUnavailable = true
		st.UserCause = d.UserCause.UserCause()
	})
}

// handleDestinationStateAudit processes a DAUD.
//
// RFC 4666 Section 3.4.3: "The DAUD message MAY be sent from the ASP to the SGP
// to audit the availability/congestion state of SS7 routes" — the one SSNM
// message that travels ASP to SGP. An ASP that receives one reports an Error.
//
// At an SGP the audit is answered from the destination state we hold: Section
// 4.4.2 has the SG respond with DUNA for unavailable destinations and DAVA for
// available ones, so a restarting ASP can resynchronise without waiting for the
// next spontaneous update.
func (c *Association) handleDestinationStateAudit(d *messages.DestinationStateAudit) error {
	if c.role != RoleSGP {
		return NewUnexpectedMessageError(d)
	}
	if err := c.validateNetworkAppearance(d.NetworkAppearance); err != nil {
		return err
	}
	if !c.ssnmAllowed() {
		return NewUnexpectedMessageError(d)
	}

	allowed, err := c.validateSSNMScope(d, d.RoutingContext, false)
	if err != nil {
		return err
	}
	if !allowed {
		return nil
	}
	if c.mtp3Restarts != nil {
		c.mtp3Restarts.procedureMu.RLock()
		defer c.mtp3Restarts.procedureMu.RUnlock()
	}

	if d.AffectedPointCode == nil {
		return ErrMissingAffectedPointCode
	}
	pcs := d.AffectedPointCode.AffectedPointCodes()
	if len(pcs) == 0 {
		return ErrMissingAffectedPointCode
	}
	masks := d.AffectedPointCode.AffectedPointCodeMasks()
	if len(masks) != len(pcs) {
		return ErrInvalidParameterValue
	}
	appearance := c.destinationKey(d.NetworkAppearance, 0)
	routingContexts, routingContextSet := c.destinationRoutingContexts(d.RoutingContext)

	for index, pc := range pcs {
		groups := make([]destinationAuditGroup, 0, max(1, len(routingContexts)))
		if routingContextSet {
			for _, rtCtx := range routingContexts {
				scope := destinationKey{
					networkAppearance:    appearance.networkAppearance,
					networkAppearanceSet: appearance.networkAppearanceSet,
					routingContext:       rtCtx,
					routingContextSet:    true,
				}
				state, known := c.destinations.lookupRange(scope, pc, masks[index])
				if restartForcesUnavailable(c.mtp3Restarts, scope, pc, masks[index]) {
					state, known = DestinationUnavailable, true
				}
				if !known {
					state = DestinationUnavailable
				}
				groups = appendDestinationAuditGroup(groups, state, rtCtx)
			}
		} else {
			scope := destinationKey{
				networkAppearance:    appearance.networkAppearance,
				networkAppearanceSet: appearance.networkAppearanceSet,
			}
			state, known := c.destinations.lookupRange(scope, pc, masks[index])
			if restartForcesUnavailable(c.mtp3Restarts, scope, pc, masks[index]) {
				state, known = DestinationUnavailable, true
			}
			if !known {
				state = DestinationUnavailable
			}
			groups = append(groups, destinationAuditGroup{state: state})
		}

		for _, group := range groups {
			if err := c.writeDestinationAuditReply(
				d.NetworkAppearance, d.RoutingContext != nil, group.routingContexts,
				pc, masks[index], group.state,
			); err != nil {
				return err
			}
		}
	}

	return nil
}

type destinationAuditGroup struct {
	state           DestinationState
	routingContexts []uint32
}

func appendDestinationAuditGroup(groups []destinationAuditGroup, state DestinationState, routingContext uint32) []destinationAuditGroup {
	for index := range groups {
		if groups[index].state == state {
			groups[index].routingContexts = append(groups[index].routingContexts, routingContext)
			return groups
		}
	}
	return append(groups, destinationAuditGroup{
		state:           state,
		routingContexts: []uint32{routingContext},
	})
}

func (c *Association) writeDestinationAuditReply(
	networkAppearance *params.Param,
	routingContextPresent bool,
	routingContexts []uint32,
	pointCode uint32,
	mask uint8,
	state DestinationState,
) error {
	routingContext := func() *params.Param {
		if !routingContextPresent {
			return nil
		}
		return params.NewRoutingContext(routingContexts...)
	}
	affectedPointCode := func() *params.Param {
		return params.NewAffectedPointCodeWithMask(mask, pointCode)
	}

	var reply messages.M3UA
	switch state {
	case DestinationUnavailable:
		reply = messages.NewDestinationUnavailable(
			networkAppearance.Copy(), routingContext(), affectedPointCode(), nil)
	case DestinationRestricted:
		reply = messages.NewDestinationRestricted(
			networkAppearance.Copy(), routingContext(), affectedPointCode(), nil)
	case DestinationCongested:
		if _, err := c.WriteSignal(messages.NewSignallingCongestion(
			networkAppearance.Copy(), routingContext(), affectedPointCode(), nil, nil, nil,
		)); err != nil {
			return err
		}
		reply = messages.NewDestinationAvailable(
			networkAppearance.Copy(), routingContext(), affectedPointCode(), nil)
	default:
		reply = messages.NewDestinationAvailable(
			networkAppearance.Copy(), routingContext(), affectedPointCode(), nil)
	}

	_, err := c.WriteSignal(reply)
	return err
}

// SetDestinationState records a destination's availability at an SGP so that
// DAUD audits can be answered from it. An SG learns this state from the SS7
// network, which is outside M3UA, so the application supplies it.
//
// On an accepted association this writes the listener's node-wide view, which is
// shared by every ASP it serves and outlives any one of them. Prefer
// Listener.SetDestinationState, which says so; this form remains because an
// operator holding an Association should not have to find the Listener to record what
// the SS7 network just told it.
func (c *Association) SetDestinationState(pointCode uint32, state DestinationState) {
	c.SetDestinationRange(pointCode, 0, state)
}

// SetDestinationRange records an all-Routing-Context destination range in the
// configured Network Appearance. Mask wildcards that many low-order bits.
func (c *Association) SetDestinationRange(pointCode uint32, mask uint8, state DestinationState) {
	scope := c.destinationKey(nil, pointCode)
	_ = c.applyDestinationRange(DestinationRange{
		NetworkAppearance:    scope.networkAppearance,
		NetworkAppearanceSet: scope.networkAppearanceSet,
		PointCode:            pointCode,
		Mask:                 mask,
		State:                state,
	}, false)
}

// SetDestinationStateForNetwork records an SGP destination state within an
// explicit Network Appearance.
func (c *Association) SetDestinationStateForNetwork(networkAppearance, pointCode uint32, state DestinationState) {
	c.SetDestinationRangeForNetwork(networkAppearance, pointCode, 0, state)
}

// SetDestinationRangeForNetwork records an all-Routing-Context range within
// an explicit Network Appearance.
func (c *Association) SetDestinationRangeForNetwork(networkAppearance, pointCode uint32, mask uint8, state DestinationState) {
	_ = c.applyDestinationRange(DestinationRange{
		NetworkAppearance:    networkAppearance,
		NetworkAppearanceSet: true,
		PointCode:            pointCode,
		Mask:                 mask,
		State:                state,
	}, false)
}

// SetDestinationStateForNetworkAndRoutingContext records one exact destination
// in an explicit Network Appearance and Routing Context.
func (c *Association) SetDestinationStateForNetworkAndRoutingContext(networkAppearance, routingContext, pointCode uint32, state DestinationState) {
	c.SetDestinationRangeForNetworkAndRoutingContext(
		networkAppearance, routingContext, pointCode, 0, state)
}

// SetDestinationRangeForNetworkAndRoutingContext records a destination range
// in one explicit Network Appearance and Routing Context.
func (c *Association) SetDestinationRangeForNetworkAndRoutingContext(networkAppearance, routingContext, pointCode uint32, mask uint8, state DestinationState) {
	_ = c.applyDestinationRange(DestinationRange{
		NetworkAppearance:    networkAppearance,
		NetworkAppearanceSet: true,
		RoutingContext:       routingContext,
		RoutingContextSet:    true,
		PointCode:            pointCode,
		Mask:                 mask,
		State:                state,
	}, false)
}

// ReportDestinationState records and synchronously reports a destination.
func (c *Association) ReportDestinationState(pointCode uint32, state DestinationState) error {
	return c.ReportDestinationRange(pointCode, 0, state)
}

// ReportDestinationRange records and synchronously reports a destination range.
func (c *Association) ReportDestinationRange(pointCode uint32, mask uint8, state DestinationState) error {
	scope := c.destinationKey(nil, pointCode)
	return c.applyDestinationRange(DestinationRange{
		NetworkAppearance:    scope.networkAppearance,
		NetworkAppearanceSet: scope.networkAppearanceSet,
		PointCode:            pointCode,
		Mask:                 mask,
		State:                state,
	}, true)
}

// ReportDestinationStateForNetwork records and synchronously reports a
// destination in an explicit Network Appearance.
func (c *Association) ReportDestinationStateForNetwork(networkAppearance, pointCode uint32, state DestinationState) error {
	return c.ReportDestinationRangeForNetwork(networkAppearance, pointCode, 0, state)
}

// ReportDestinationRangeForNetwork records and synchronously reports a range
// in an explicit Network Appearance.
func (c *Association) ReportDestinationRangeForNetwork(networkAppearance, pointCode uint32, mask uint8, state DestinationState) error {
	return c.applyDestinationRange(DestinationRange{
		NetworkAppearance:    networkAppearance,
		NetworkAppearanceSet: true,
		PointCode:            pointCode,
		Mask:                 mask,
		State:                state,
	}, true)
}

// ReportDestinationStateForNetworkAndRoutingContext records and synchronously
// reports one destination in one explicit traffic scope.
func (c *Association) ReportDestinationStateForNetworkAndRoutingContext(networkAppearance, routingContext, pointCode uint32, state DestinationState) error {
	return c.ReportDestinationRangeForNetworkAndRoutingContext(
		networkAppearance, routingContext, pointCode, 0, state,
	)
}

// ReportDestinationRangeForNetworkAndRoutingContext records and synchronously
// reports a destination range in one explicit traffic scope.
func (c *Association) ReportDestinationRangeForNetworkAndRoutingContext(networkAppearance, routingContext, pointCode uint32, mask uint8, state DestinationState) error {
	return c.applyDestinationRange(DestinationRange{
		NetworkAppearance:    networkAppearance,
		NetworkAppearanceSet: true,
		RoutingContext:       routingContext,
		RoutingContextSet:    true,
		PointCode:            pointCode,
		Mask:                 mask,
		State:                state,
	}, true)
}

func (c *Association) applyDestinationRange(rangeValue DestinationRange, wait bool) error {
	if c != nil && c.role == RoleSGP && c.listener != nil {
		return c.listener.applyDestinationRange(rangeValue, wait)
	}
	if !validDestinationState(rangeValue.State) {
		return fmt.Errorf("%w: destination state %d", ErrInvalidParameterValue, rangeValue.State)
	}
	if c == nil || c.destinations == nil {
		return nil
	}
	c.destinations.setRanges([]DestinationRange{rangeValue})
	return nil
}

// PeerCongestionLevel returns the congestion level the peer last reported about
// itself, or 0 if it has reported none.
//
// This is the ASP-to-peer direction of SCON, which RFC 4666 Section 3.4.4
// describes as "indicating that the congestion level of the M3UA layer or the
// ASP has changed". It says nothing about whether any SS7 destination is
// reachable, which is why it is reported separately from DestinationState: an
// SGP that folded the two together would answer another ASP's DAUD with
// congestion this ASP invented.
//
// The values are those of Section 3.4.4: 0 "No Congestion or Undefined", and 1
// to 3 for the national congestion levels.
func (c *Association) PeerCongestionLevel() uint8 {
	return uint8(c.peerCongestion.Load())
}
