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

// DestinationAvailability is whether an SS7 destination is reachable. It is the
// dimension DUNA, DAVA and DRST move, and the only one they move: RFC 4666
// Section 4.5.2.2 keeps availability and congestion as two separate statuses of
// the same destination, so congestion is carried by CongestionState instead of
// occupying a value here.
type DestinationAvailability uint8

// Destination availability definitions.
const (
	// DestinationAvailable is the initial assumption for any destination
	// nothing has been reported about, and the state a DAVA restores.
	DestinationAvailable DestinationAvailability = iota
	// DestinationUnavailable is set by DUNA: the destination cannot be reached
	// and the MTP3-User is expected to stop traffic to it.
	DestinationUnavailable
	// DestinationRestricted is set by DRST: the destination is reachable but
	// the SG would prefer traffic went elsewhere.
	DestinationRestricted
)

func (s DestinationAvailability) String() string {
	switch s {
	case DestinationAvailable:
		return "Available"
	case DestinationUnavailable:
		return "Unavailable"
	case DestinationRestricted:
		return "Restricted"
	default:
		return "Unknown"
	}
}

func validDestinationAvailability(availability DestinationAvailability) bool {
	return availability >= DestinationAvailable && availability <= DestinationRestricted
}

// CongestionState is the congestion status of an SS7 destination, independent
// of whether it is reachable.
//
// RFC 4666 Section 3.4.4 makes the Congestion Indications parameter optional
// and its level 0 "No Congestion or Undefined", so an explicit zero is
// congestion abatement while an absent parameter is congestion without a level.
// Neither can be expressed by a level alone, which is why presence is carried
// beside it.
type CongestionState struct {
	// Congested reports that traffic to the destination should be reduced.
	Congested bool
	// Level is the reported congestion level, valid only when LevelSet is true.
	Level uint8
	// LevelSet distinguishes an omitted Congestion Indications parameter from
	// an explicit level zero.
	LevelSet bool
}

// reported distinguishes a congestion statement from the absence of one. An
// explicit level, including RFC 4666 Section 3.4.4's level 0 abatement, and an
// omitted Congestion Indications parameter are both statements; the zero value
// is not, and must not be put on the wire as one.
func (c CongestionState) reported() bool {
	return c.Congested || c.LevelSet
}

// congestionStateFor builds the congestion dimension a Signalling Congestion
// report installs. An explicit level 0 abates congestion; any other level, and
// an omitted parameter, report it.
func congestionStateFor(level uint8, levelSet bool) CongestionState {
	return CongestionState{
		Congested: !levelSet || level != 0,
		Level:     level,
		LevelSet:  levelSet,
	}
}

func validCongestionState(congestion CongestionState) bool {
	return !congestion.LevelSet || congestion.Level <= 3
}

// DestinationNetworkState is an SS7 destination's state in both of the
// dimensions RFC 4666 Section 4.5 names: its availability and its congestion.
//
// They are answered together and independently. Section 4.5.3 has an audit
// answered with "a DUNA message (if unavailable), a DAVA message (if
// available), or a DRST (if restricted...)" and, "additionally... a SCON message
// (if the destination is congested) before the DAVA or DRST", so a node that
// cannot state one without the other cannot answer the audit it owes. A
// congestion report therefore never restores reachability and an availability
// report never clears congestion.
type DestinationNetworkState struct {
	Availability DestinationAvailability
	Congestion   CongestionState
}

func validDestinationNetworkState(state DestinationNetworkState) bool {
	return validDestinationAvailability(state.Availability) && validCongestionState(state.Congestion)
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
	// State is the destination's network state after this message. A DUNA, DAVA
	// or DRST moves only Availability and a SCON moves only Congestion (RFC
	// 4666 Section 4.5.2.2), so the dimension this message did not carry is
	// filled in from what this node has retained about the destination, in the
	// exact scope the message named. It is left at its zero value when the
	// message named several Routing Contexts, which need not agree, and for a
	// report that describes the peer rather than a destination.
	State DestinationNetworkState
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
	// State is the network state installed by this update. Which of its two
	// dimensions the update actually carries is recorded beside the record, so
	// a congestion report does not restate an availability it never learned.
	State DestinationNetworkState
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

// destinationDimensions records which dimensions of a destinationRecord carry
// knowledge. RFC 4666 Section 4.5.3 has an audit answered with the destination's
// availability and, separately and additionally, its congestion, so a record
// written by one of them must not be read as a statement about the other: an
// availability-only record leaves congestion to whatever a congestion report
// last said, and the reverse.
type destinationDimensions uint8

const (
	destinationAvailabilityDimension destinationDimensions = 1 << iota
	destinationCongestionDimension
)

func (d destinationDimensions) carries(dimension destinationDimensions) bool {
	return d&dimension != 0
}

type destinationRecord struct {
	rangeValue          DestinationRange
	routingContexts     []uint32
	routingContextScope string
	// dimensions is the set of dimensions this record is a statement about.
	dimensions destinationDimensions
	sequence   uint64
}

type destinationPause struct {
	rangeValue      DestinationRange
	routingContexts []uint32
}

// ErrSSNMDestinationRecordLimit reports that retained SSNM destination state
// has reached its budget, so the update was refused rather than grown into.
//
// The Affected Point Codes of an SSNM message are chosen by the peer and need
// not correspond to anything this node has a route to, so retention has to be
// bounded to stay independent of what the peer sends.
//
// It wraps ErrSSNMResourceLoss: refusing to grow is a local decision, not a
// fault the peer can correct, so it produces no protocol Error.
var ErrSSNMDestinationRecordLimit = fmt.Errorf("%w: SSNM destination record limit exceeded", ErrSSNMResourceLoss)

// destinations tracks destination ranges by Network Appearance and Routing
// Context. Updates are sequenced so the newest range covering a query wins.
//
// maxRecords bounds how many records the store retains; see storeLocked for the
// overflow policy and ForgetDestinations for the reclaim path. Zero resolves to
// DefaultMaxSSNMDestinationRecords, so a store assembled without a limit is
// bounded rather than unbounded.
type destinations struct {
	mu         sync.RWMutex
	state      map[destinationKey]destinationRecord
	sequence   uint64
	maxRecords int
}

func newDestinations() *destinations {
	return &destinations{
		state:      make(map[destinationKey]destinationRecord),
		maxRecords: DefaultMaxSSNMDestinationRecords,
	}
}

// setRecordLimit installs the configured retained-record budget.
//
// It is resolved on first use rather than at construction because
// newAssociation builds the store before the Association is bound to the
// Endpoint whose ASPConfig carries the value.
func (d *destinations) setRecordLimit(limit int) {
	if d == nil || limit <= 0 {
		return
	}
	d.mu.RLock()
	unchanged := d.maxRecords == limit
	d.mu.RUnlock()
	if unchanged {
		return
	}
	d.mu.Lock()
	d.maxRecords = limit
	d.mu.Unlock()
}

func (d *destinations) recordLimitLocked() int {
	if d.maxRecords > 0 {
		return d.maxRecords
	}
	return DefaultMaxSSNMDestinationRecords
}

func (d *destinations) recordLimitErrorLocked(refused int) error {
	if refused == 0 {
		return nil
	}
	return fmt.Errorf("%w: %d record(s) refused, %d retained, limit %d",
		ErrSSNMDestinationRecordLimit, refused, len(d.state), d.recordLimitLocked())
}

// forget discards every retained record and reports how many were released.
func (d *destinations) forget() int {
	if d == nil {
		return 0
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	released := len(d.state)
	d.state = make(map[destinationKey]destinationRecord)
	return released
}

// The nil receiver checks below keep an Association that was assembled directly, rather
// than through Dial or Accept, from crashing on the first SSNM message a peer
// sends. Tracking is inert in that case; state reads report the default.

func (d *destinations) set(key destinationKey, availability DestinationAvailability) {
	d.setRanges([]DestinationRange{{
		NetworkAppearance:    key.networkAppearance,
		NetworkAppearanceSet: key.networkAppearanceSet,
		RoutingContext:       key.routingContext,
		RoutingContextSet:    key.routingContextSet,
		PointCode:            key.pointCode,
		Mask:                 key.mask,
		State:                DestinationNetworkState{Availability: availability},
	}})
}

func (d *destinations) setRanges(ranges []DestinationRange) {
	_ = d.setRangesWithinBudget(ranges)
}

// setRangesWithinBudget is setRanges with the record budget reported. The
// callers that can surface a refusal — the SSNM receive path and the local
// destination-report API — use this form; the rest retain what they can and
// carry on, because there is no peer or caller left to tell.
func (d *destinations) setRangesWithinBudget(ranges []DestinationRange) error {
	if d == nil || len(ranges) == 0 {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.state == nil {
		d.state = make(map[destinationKey]destinationRecord)
	}
	refused := 0
	for _, rangeValue := range ranges {
		if !d.storeLocked(destinationRecord{
			rangeValue: normalizeDestinationRange(rangeValue),
			dimensions: destinationAvailabilityDimension,
		}) {
			refused++
		}
	}
	return d.recordLimitErrorLocked(refused)
}

// setScopedRanges records point-code updates sharing one explicit Routing
// Context scope. One SSNM message applies every listed point code to the same
// set of contexts; storing that set once avoids materializing the product of
// the two legal variable-length parameter lists.
func (d *destinations) setScopedRanges(routingContexts []uint32, ranges []DestinationRange) {
	_ = d.setScopedRangesWithinBudget(routingContexts, ranges)
}

// setScopedRangesWithinBudget is setScopedRanges with the record budget
// reported.
func (d *destinations) setScopedRangesWithinBudget(routingContexts []uint32, ranges []DestinationRange) error {
	if d == nil || len(routingContexts) == 0 || len(ranges) == 0 {
		return nil
	}

	canonical, scope := canonicalRoutingContextScope(routingContexts)
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.state == nil {
		d.state = make(map[destinationKey]destinationRecord)
	}
	refused := 0
	for _, rangeValue := range ranges {
		rangeValue = normalizeDestinationRange(rangeValue)
		rangeValue.RoutingContext = 0
		rangeValue.RoutingContextSet = true
		if !d.storeLocked(destinationRecord{
			rangeValue:          rangeValue,
			routingContexts:     canonical,
			routingContextScope: scope,
			dimensions:          destinationAvailabilityDimension,
		}) {
			refused++
		}
	}
	return d.recordLimitErrorLocked(refused)
}

// setCongestionRangesWithinBudget records an RFC 4666 Section 3.4.4 congestion
// report against the availability the store already holds.
//
// Section 4.5.2.2 keeps availability and congestion apart as two statuses of
// the same destination, and the Section 3.4.4 Congestion Level table makes
// level 0 "No Congestion or Undefined" — a report about congestion, never about
// reachability. A SCON therefore never restores a destination the peer has
// reported unavailable: Section 4.5.1 makes DUNA followed by SCON an ordinary
// sequence, and Section 4.5.3 has the SG keep answering a DAUD for that
// destination with DUNA until a DAVA arrives.
func (d *destinations) setCongestionRangesWithinBudget(ranges []DestinationRange) error {
	if d == nil || len(ranges) == 0 {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.state == nil {
		d.state = make(map[destinationKey]destinationRecord)
	}
	refused := 0
	for _, rangeValue := range ranges {
		if !d.storeLocked(destinationRecord{
			rangeValue: normalizeDestinationRange(rangeValue),
			dimensions: destinationCongestionDimension,
		}) {
			refused++
		}
	}
	return d.recordLimitErrorLocked(refused)
}

// setScopedCongestionRangesWithinBudget is setCongestionRangesWithinBudget for
// a report naming several Routing Contexts. One message applies to every
// context it lists, and the contexts share one record.
func (d *destinations) setScopedCongestionRangesWithinBudget(
	routingContexts []uint32,
	ranges []DestinationRange,
) error {
	if d == nil || len(routingContexts) == 0 || len(ranges) == 0 {
		return nil
	}

	canonical, scope := canonicalRoutingContextScope(routingContexts)
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.state == nil {
		d.state = make(map[destinationKey]destinationRecord)
	}
	refused := 0
	for _, rangeValue := range ranges {
		rangeValue = normalizeDestinationRange(rangeValue)
		rangeValue.RoutingContext = 0
		rangeValue.RoutingContextSet = true
		if !d.storeLocked(destinationRecord{
			rangeValue:          rangeValue,
			routingContexts:     canonical,
			routingContextScope: scope,
			dimensions:          destinationCongestionDimension,
		}) {
			refused++
		}
	}
	return d.recordLimitErrorLocked(refused)
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

// storeLocked installs one record and reports whether it was retained. It is
// the only writer of d.state, and therefore the point at which the record
// budget is enforced.
//
// Overflow policy: a record whose key is already held always replaces it, since
// replacing costs no memory and discarding an update for a destination the peer
// has already established would be worse than refusing a new one. A record for
// a new key is refused once the store holds its budget, and the caller reports
// the refusal as ErrSSNMDestinationRecordLimit. Refusing is deliberately
// preferred to evicting the oldest record: eviction would let a peer that names
// enough unknown point codes push out a genuine DUNA and so restart traffic
// into a destination the SG has reported unreachable.
func (d *destinations) storeLocked(record destinationRecord) bool {
	key := destinationRecordKey(record)
	held, exists := d.state[key]
	if !exists && len(d.state) >= d.recordLimitLocked() {
		return false
	}
	if exists {
		// A record states only the dimensions its update carried. Keeping the
		// other dimension as the last statement about it left it is what keeps
		// the two statuses of RFC 4666 Section 4.5 independent: a congestion
		// report restates no availability, and an availability report clears no
		// congestion. Section 4.5.1 makes DUNA followed by SCON an ordinary
		// sequence, and Section 4.5.3 keeps answering that destination's audit
		// with DUNA until a DAVA arrives.
		if !record.dimensions.carries(destinationAvailabilityDimension) {
			record.rangeValue.State.Availability = held.rangeValue.State.Availability
		}
		if !record.dimensions.carries(destinationCongestionDimension) {
			record.rangeValue.State.Congestion = held.rangeValue.State.Congestion
		}
		record.dimensions |= held.dimensions
	}
	d.sequence++
	if d.sequence == 0 {
		d.renumberLocked()
		d.sequence++
	}
	record.sequence = d.sequence
	d.state[key] = record
	return true
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
func (d *destinations) lookup(key destinationKey) (DestinationNetworkState, bool) {
	return d.lookupRange(key, key.pointCode, key.mask)
}

// destinationQuery is one destination to resolve, with the exact scope to
// resolve it in.
type destinationQuery struct {
	scope     destinationKey
	pointCode uint32
	mask      uint8
}

type destinationCoverKey struct {
	prefix uint32
	mask   uint8
}

// resolveMany answers a batch of destination queries in one pass over the store.
//
// One SSNM message may name up to MaxAffectedPointCodesPerSSNM destinations, and
// resolving each of them with its own scan is quadratic in what the peer sends
// against what this node retains. A stored range covers a query only at a mask
// the query itself can name, and there are at most 25 of those, so the queries
// index by covering prefix and the traversal costs one map lookup per record.
func (d *destinations) resolveMany(queries []destinationQuery) []DestinationNetworkState {
	if d == nil {
		return make([]DestinationNetworkState, len(queries))
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.resolveManyLocked(queries)
}

func (d *destinations) resolveManyLocked(queries []destinationQuery) []DestinationNetworkState {
	states := make([]DestinationNetworkState, len(queries))
	if len(queries) == 0 || len(d.state) == 0 {
		return states
	}

	covering := make(map[destinationCoverKey][]int, len(queries))
	for index, query := range queries {
		for mask := effectiveDestinationMask(query.mask); mask <= 24; mask++ {
			key := destinationCoverKey{
				prefix: destinationRangePrefix(query.pointCode, mask),
				mask:   mask,
			}
			covering[key] = append(covering[key], index)
		}
	}

	availability := make([]uint64, len(queries))
	congestion := make([]uint64, len(queries))
	for _, record := range d.state {
		key := destinationCoverKey{
			prefix: destinationRangePrefix(record.rangeValue.PointCode, record.rangeValue.Mask),
			mask:   effectiveDestinationMask(record.rangeValue.Mask),
		}
		for _, index := range covering[key] {
			if !destinationRecordScopeMatches(record, queries[index].scope) {
				continue
			}
			if record.dimensions.carries(destinationAvailabilityDimension) &&
				record.sequence > availability[index] {
				availability[index] = record.sequence
				states[index].Availability = record.rangeValue.State.Availability
			}
			if record.dimensions.carries(destinationCongestionDimension) &&
				record.sequence > congestion[index] {
				congestion[index] = record.sequence
				states[index].Congestion = record.rangeValue.State.Congestion
			}
		}
	}
	return states
}

func (d *destinations) lookupRange(scope destinationKey, pointCode uint32, mask uint8) (DestinationNetworkState, bool) {
	if d == nil {
		return DestinationNetworkState{}, false
	}
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.lookupRangeLocked(scope, pointCode, mask)
}

// lookupRangeLocked composes both dimensions of a destination's state, each
// from the newest record covering it that is a statement about that dimension.
//
// Resolving them separately is what answering an RFC 4666 Section 4.5.3 audit
// requires: a SCON naming one point code must not hide the DUNA that a broader
// range carries, and that DUNA must not withdraw the congestion the SCON
// reported. A dimension nothing has reported on resolves to its zero value,
// which is the documented initial assumption: reachable and uncongested.
func (d *destinations) lookupRangeLocked(scope destinationKey, pointCode uint32, mask uint8) (DestinationNetworkState, bool) {
	var (
		state           DestinationNetworkState
		availabilitySeq uint64
		congestionSeq   uint64
		known           bool
	)
	for _, record := range d.state {
		if !destinationRecordScopeMatches(record, scope) ||
			!destinationRangeCovers(record.rangeValue, pointCode, mask) {
			continue
		}
		known = true
		if record.dimensions.carries(destinationAvailabilityDimension) &&
			record.sequence > availabilitySeq {
			availabilitySeq = record.sequence
			state.Availability = record.rangeValue.State.Availability
		}
		if record.dimensions.carries(destinationCongestionDimension) &&
			record.sequence > congestionSeq {
			congestionSeq = record.sequence
			state.Congestion = record.rangeValue.State.Congestion
		}
	}
	return state, known
}

func (d *destinations) snapshotForScope(scope destinationKey) map[uint32]DestinationNetworkState {
	if d == nil {
		return map[uint32]DestinationNetworkState{}
	}
	d.mu.RLock()
	defer d.mu.RUnlock()

	out := make(map[uint32]DestinationNetworkState)
	availability := make(map[uint32]uint64)
	congestion := make(map[uint32]uint64)
	for _, record := range d.state {
		if record.rangeValue.Mask != 0 || !destinationRecordScopeMatches(record, scope) {
			continue
		}
		pointCode := record.rangeValue.PointCode
		state := out[pointCode]
		if record.dimensions.carries(destinationAvailabilityDimension) &&
			record.sequence > availability[pointCode] {
			availability[pointCode] = record.sequence
			state.Availability = record.rangeValue.State.Availability
		}
		if record.dimensions.carries(destinationCongestionDimension) &&
			record.sequence > congestion[pointCode] {
			congestion[pointCode] = record.sequence
			state.Congestion = record.rangeValue.State.Congestion
		}
		out[pointCode] = state
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
		if record.rangeValue.State.Availability != DestinationUnavailable {
			records = append(records, record)
		}
	}
	sort.Slice(records, func(i, j int) bool {
		return records[i].sequence < records[j].sequence
	})
	paused := make([]destinationPause, 0, len(records))
	for _, record := range records {
		record.rangeValue.State.Availability = DestinationUnavailable
		record.dimensions |= destinationAvailabilityDimension
		// Every key here is already held, so no record is refused.
		_ = d.storeLocked(record)
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
	if !set {
		appearance, set = c.outboundNetworkAppearance()
	}
	return destinationKey{
		networkAppearance:    appearance,
		networkAppearanceSet: set,
		pointCode:            pointCode,
	}
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
	if c.role != RoleASP && c.role != RoleIPSP {
		return
	}

	for _, paused := range c.destinations.pause() {
		rangeValue := paused.rangeValue
		status := &DestinationStatus{
			PointCode:            rangeValue.PointCode,
			Mask:                 rangeValue.Mask,
			NetworkAppearance:    rangeValue.NetworkAppearance,
			NetworkAppearanceSet: rangeValue.NetworkAppearanceSet,
			State:                DestinationNetworkState{Availability: DestinationUnavailable},
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
func (c *Association) applySSNM(
	report SSNMReport,
	networkAppearance,
	routingContext,
	apc *params.Param,
	state DestinationNetworkState,
	dimensions destinationDimensions,
	update *aspRouteUpdate,
	mutate func(*DestinationStatus),
) error {
	report.Scope = c.ssnmWireScope(networkAppearance, routingContext)
	report.Source = SSNMPeerReport
	pcs, masks, err := c.ssnmAffectedPointCodes(apc, report.Scope)
	if err != nil {
		return err
	}
	report.Destinations = ssnmDestinationsFrom(pcs, masks)
	routingContexts, routingContextSet := c.destinationRoutingContexts(routingContext)
	appearance := c.destinationKey(networkAppearance, 0)
	statusScope := newDestinationStatusScope(networkAppearance, routingContext)
	statuses := make([]*DestinationStatus, 0, len(pcs))
	updates := make([]DestinationRange, 0, len(pcs))
	// SCON is the one message that reports congestion rather than reachability.
	// Its record carries the level and leaves the destination's availability to
	// the availability messages, as RFC 4666 Section 4.5.2.2 requires.
	congestionUpdate := dimensions.carries(destinationCongestionDimension)

	for index, pc := range pcs {
		// DUPU reports an unavailable user part at a destination that is
		// itself still reachable, so it must not overwrite the destination's
		// own availability. mutate marks those.
		status := &DestinationStatus{
			PointCode: pc,
			Mask:      masks[index],
			State:     state,
		}
		statusScope.apply(status)
		if mutate != nil {
			mutate(status)
		}
		if !status.UserPartUnavailable {
			applied := DestinationRange{
				NetworkAppearance:    appearance.networkAppearance,
				NetworkAppearanceSet: appearance.networkAppearanceSet,
				PointCode:            pc,
				Mask:                 masks[index],
				State:                status.State,
			}
			updates = append(updates, applied)
		}
		statuses = append(statuses, status)
	}

	if update != nil && c.endpoint != nil && c.endpoint.aspRoutes != nil {
		if err := c.endpoint.aspRoutes.apply(c, statuses, *update); err != nil {
			return err
		}
	}
	c.destinations.setRecordLimit(c.destinationRecordLimit())
	var retained error
	switch {
	case congestionUpdate && routingContextSet:
		retained = c.destinations.setScopedCongestionRangesWithinBudget(routingContexts, updates)
	case congestionUpdate:
		retained = c.destinations.setCongestionRangesWithinBudget(updates)
	case routingContextSet:
		retained = c.destinations.setScopedRangesWithinBudget(routingContexts, updates)
	default:
		retained = c.destinations.setRangesWithinBudget(updates)
	}
	// The status channel reports the destination's state in both dimensions
	// after this message, not just the dimension the message carried: RFC 4666
	// Section 4.5.2.2 keeps the two apart, so the one this message says nothing
	// about is read back rather than restated as its zero value.
	c.resolveStatusDimensions(statuses, appearance, routingContexts, routingContextSet, dimensions)
	// What the peer said stands whether or not there was room to retain it, so
	// the report goes out before a refused record is reported back to the peer
	// as an Error.
	for _, status := range statuses {
		c.notifyStatus(status)
	}
	if err := c.publishSSNMReport(report); err != nil && retained == nil {
		retained = err
	}

	return retained
}

// resolveStatusDimensions fills in the dimension a status does not itself carry
// from what this node has retained about the destination.
func (c *Association) resolveStatusDimensions(
	statuses []*DestinationStatus,
	appearance destinationKey,
	routingContexts []uint32,
	routingContextSet bool,
	dimensions destinationDimensions,
) {
	missing := (destinationAvailabilityDimension | destinationCongestionDimension) &^ dimensions
	if missing == 0 || c.destinations == nil {
		return
	}
	scope := destinationKey{
		networkAppearance:    appearance.networkAppearance,
		networkAppearanceSet: appearance.networkAppearanceSet,
	}
	switch {
	case !routingContextSet:
	case len(routingContexts) == 1:
		scope.routingContext = routingContexts[0]
		scope.routingContextSet = true
	default:
		// Several Routing Contexts need not hold the same state for the same
		// destination, and one status carries one value, so nothing is filled in
		// rather than picking one of them.
		return
	}

	queries := make([]destinationQuery, 0, len(statuses))
	resolving := make([]*DestinationStatus, 0, len(statuses))
	for _, status := range statuses {
		// DUPU reports an unavailable user part at a destination that is itself
		// still reachable, and writes no record, so neither dimension is filled
		// in for it.
		if status.UserPartUnavailable {
			continue
		}
		queries = append(queries, destinationQuery{
			scope:     scope,
			pointCode: status.PointCode,
			mask:      status.Mask,
		})
		resolving = append(resolving, status)
	}
	if len(queries) == 0 {
		return
	}
	for index, retained := range c.destinations.resolveMany(queries) {
		if missing.carries(destinationAvailabilityDimension) {
			resolving[index].State.Availability = retained.Availability
		}
		if missing.carries(destinationCongestionDimension) {
			resolving[index].State.Congestion = retained.Congestion
		}
	}
}

// ssnmAffectedPointCodes bounds an SSNM message's Affected Point Code list
// before expanding it.
//
// The count comes from the encoded parameter, so a message naming more point
// codes than this node accepts costs nothing proportional to what the peer
// claimed. Exceeding the bound is a local resource condition rather than a
// fault in the message: it is reported as loss, the partitions it concerned
// are invalidated, and no prefix of it is applied.
func (c *Association) ssnmAffectedPointCodes(apc *params.Param, scope WireScope) ([]uint32, []uint8, error) {
	if apc == nil {
		return nil, nil, ErrMissingAffectedPointCode
	}
	count := apc.AffectedPointCodeCount()
	if count == 0 {
		return nil, nil, ErrMissingAffectedPointCode
	}
	if limit := c.ssnmAffectedPointCodeLimit(); count > limit {
		return nil, nil, c.refuseOversizedSSNM(scope, count, limit)
	}
	pcs := apc.AffectedPointCodes()
	if len(pcs) == 0 {
		return nil, nil, ErrMissingAffectedPointCode
	}
	masks := apc.AffectedPointCodeMasks()
	if len(masks) != len(pcs) {
		return nil, nil, ErrInvalidParameterValue
	}
	return pcs, masks, nil
}

// destinationRecordLimit resolves the retained-record budget for this
// association's SSNM state store. An ASP Endpoint's ASPConfig owns the value;
// an Association assembled without one keeps the package default.
func (c *Association) destinationRecordLimit() int {
	// aspRoutes.config is written once, by NewEndpoint, before any Association
	// can reach it.
	if c != nil && c.endpoint != nil && c.endpoint.aspRoutes != nil &&
		c.endpoint.aspRoutes.config.maxSSNMDestinationRecords > 0 {
		return c.endpoint.aspRoutes.config.maxSSNMDestinationRecords
	}
	return DefaultMaxSSNMDestinationRecords
}

// reportSSNM reports a peer's SSNM message to the user without recording it as
// this node's own view of the SS7 network.
//
// It is what an SGP does with an ASP's SCON: the report is real and worth
// surfacing, but it describes the ASP rather than a destination, so it must not
// reach the map the SGP answers a DAUD from.
func (c *Association) reportSSNM(
	report SSNMReport,
	networkAppearance, routingContext, apc *params.Param,
	mutate func(*DestinationStatus),
) error {
	report.Scope = c.ssnmWireScope(networkAppearance, routingContext)
	report.Source = SSNMPeerReport
	pcs, masks, err := c.ssnmAffectedPointCodes(apc, report.Scope)
	if err != nil {
		return err
	}
	report.Destinations = ssnmDestinationsFrom(pcs, masks)

	statusScope := newDestinationStatusScope(networkAppearance, routingContext)
	for index, pc := range pcs {
		status := &DestinationStatus{
			PointCode: pc,
			Mask:      masks[index],
		}
		statusScope.apply(status)
		if mutate != nil {
			mutate(status)
		}
		c.notifyStatus(status)
	}
	return c.publishSSNMReport(report)
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
		if c.role == RoleIPSP {
			if c.State() != StateASPActive || !c.activeForRoutingContext(rtCtx) ||
				c.peerRoutingContextOverridden(rtCtx) {
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
	if err := c.validateSSNMNetworkAppearance(d.NetworkAppearance, d.RoutingContext); err != nil {
		return err
	}

	allowed, err := c.validateSSNMScope(d, d.RoutingContext, true)
	if err != nil {
		return err
	}
	if !allowed {
		return nil
	}

	return c.applySSNM(
		SSNMReport{Kind: SSNMDestinationUnavailableReport},
		d.NetworkAppearance,
		d.RoutingContext,
		d.AffectedPointCode,
		DestinationNetworkState{Availability: DestinationUnavailable},
		destinationAvailabilityDimension,
		&aspRouteUpdate{kind: aspRouteAvailabilityUpdate, availability: DestinationUnavailable},
		nil,
	)
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
	if err := c.validateSSNMNetworkAppearance(d.NetworkAppearance, d.RoutingContext); err != nil {
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

	return c.applySSNM(
		SSNMReport{Kind: SSNMDestinationAvailableReport},
		d.NetworkAppearance,
		d.RoutingContext,
		d.AffectedPointCode,
		DestinationNetworkState{Availability: DestinationAvailable},
		destinationAvailabilityDimension,
		&aspRouteUpdate{kind: aspRouteAvailabilityUpdate, availability: DestinationAvailable},
		nil,
	)
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
	if err := c.validateSSNMNetworkAppearance(d.NetworkAppearance, d.RoutingContext); err != nil {
		return err
	}

	allowed, err := c.validateSSNMScope(d, d.RoutingContext, true)
	if err != nil {
		return err
	}
	if !allowed {
		return nil
	}

	return c.applySSNM(
		SSNMReport{Kind: SSNMDestinationRestrictedReport},
		d.NetworkAppearance,
		d.RoutingContext,
		d.AffectedPointCode,
		DestinationNetworkState{Availability: DestinationRestricted},
		destinationAvailabilityDimension,
		&aspRouteUpdate{kind: aspRouteAvailabilityUpdate, availability: DestinationRestricted},
		nil,
	)
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
	if err := c.validateSSNMNetworkAppearance(s.NetworkAppearance, s.RoutingContext); err != nil {
		return err
	}
	if c.role == RoleASP && !c.ssnmAllowedDuringActivation() {
		return NewUnexpectedMessageError(s)
	}
	if c.role == RoleSGP && !c.ssnmAllowed() {
		return NewUnexpectedMessageError(s)
	}
	if c.role == RoleIPSP && !c.ssnmAllowed() {
		return NewUnexpectedMessageError(s)
	}
	allowed, err := c.validateSSNMScope(s, s.RoutingContext, c.role == RoleASP)
	if err != nil {
		return err
	}
	if !allowed {
		return nil
	}
	if (c.role == RoleASP || c.role == RoleIPSP) && s.ConcernedDestination != nil {
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
	levelSet := s.CongestionIndications != nil
	congested := true
	if levelSet {
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
		peerReport := SSNMReport{
			Kind:               SSNMSignallingCongestionReport,
			CongestionLevel:    level,
			CongestionLevelSet: levelSet,
			PeerReported:       true,
		}
		if s.ConcernedDestination != nil {
			peerReport.ConcernedDestination = s.ConcernedDestination.ConcernedDestination()
			peerReport.ConcernedDestinationSet = true
		}
		return c.reportSSNM(peerReport, s.NetworkAppearance, s.RoutingContext, s.AffectedPointCode, func(st *DestinationStatus) {
			st.State.Congestion = CongestionState{Congested: congested, Level: level, LevelSet: levelSet}
			st.PeerReported = true
			if s.ConcernedDestination != nil {
				st.ConcernedDestination = s.ConcernedDestination.ConcernedDestination()
				st.ConcernedDestinationSet = true
			}
		})
	}

	// This is the state reported to the MTP3-User, which is this message's own
	// report: congestion, or the abatement an explicit level 0 announces. It is
	// deliberately not the destination's availability. Section 4.5.2.2 makes the
	// two separate statuses, so the record applySSNM writes keeps the
	// availability the peer last reported and only DAVA restores reachability —
	// writing this value into it made an SG answer a later DAUD for an
	// unavailable destination with DAVA (Section 4.5.3).
	congestion := CongestionState{Congested: congested, Level: level, LevelSet: levelSet}
	return c.applySSNM(SSNMReport{
		Kind:               SSNMSignallingCongestionReport,
		CongestionLevel:    level,
		CongestionLevelSet: levelSet,
	}, s.NetworkAppearance, s.RoutingContext, s.AffectedPointCode,
		DestinationNetworkState{Congestion: congestion},
		destinationCongestionDimension,
		&aspRouteUpdate{
			kind:               aspRouteCongestionUpdate,
			congested:          congested,
			congestionLevel:    level,
			congestionLevelSet: levelSet,
		}, nil)
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
	if err := c.validateSSNMNetworkAppearance(d.NetworkAppearance, d.RoutingContext); err != nil {
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

	return c.applySSNM(SSNMReport{
		Kind:         SSNMDestinationUserPartUnavailableReport,
		UserCause:    d.UserCause.UserCause(),
		UserCauseSet: true,
	}, d.NetworkAppearance, d.RoutingContext, d.AffectedPointCode,
		DestinationNetworkState{}, 0, nil, func(st *DestinationStatus) {
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
// 4.5.3 indicates the status of each requested destination "in a DUNA message
// (if unavailable), a DAVA message (if available), or a DRST (if restricted
// ...)", so a restarting ASP can resynchronise without waiting for the next
// spontaneous update.
func (c *Association) handleDestinationStateAudit(d *messages.DestinationStateAudit) error {
	if c.role != RoleSGP {
		return NewUnexpectedMessageError(d)
	}
	if err := c.validateSSNMNetworkAppearance(d.NetworkAppearance, d.RoutingContext); err != nil {
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

	scope := c.ssnmWireScope(d.NetworkAppearance, d.RoutingContext)
	pcs, masks, err := c.ssnmAffectedPointCodes(d.AffectedPointCode, scope)
	if err != nil {
		return err
	}
	// An audit is a request for what this node holds, not knowledge about a
	// destination. It is published as an event and retained by nobody: RFC
	// 4666 Section 4.5.3 has the ASP request "the current availability and
	// congestion status" and the SGP answer from the state it already had.
	if err := c.publishSSNMReport(SSNMReport{
		Kind:         SSNMDestinationStateAuditReport,
		Source:       SSNMPeerReport,
		Scope:        scope,
		Destinations: ssnmDestinationsFrom(pcs, masks),
	}); err != nil {
		return err
	}
	appearance := c.destinationKey(d.NetworkAppearance, 0)
	routingContexts, routingContextSet := c.destinationRoutingContexts(d.RoutingContext)

	auditState := func(scope destinationKey, pointCode uint32, mask uint8) DestinationNetworkState {
		// RFC 4666 Section 4.5.3: "An SG SHOULD respond with a DUNA message
		// when DAUD was received with an unknown Signalling Point Code", so an
		// unknown destination answers unavailable rather than available.
		state := DestinationNetworkState{Availability: DestinationUnavailable}
		if retained, known := c.destinations.lookupRange(scope, pointCode, mask); known {
			state = retained
		}
		if restartForcesUnavailable(c.mtp3Restarts, scope, pointCode, mask) {
			// Section 4.6 isolates the affected scope until the restart
			// completes, and an isolated destination reports nothing about
			// congestion either.
			state = DestinationNetworkState{Availability: DestinationUnavailable}
		}
		return state
	}

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
				groups = appendDestinationAuditGroup(
					groups, auditState(scope, pc, masks[index]), rtCtx,
				)
			}
		} else {
			scope := destinationKey{
				networkAppearance:    appearance.networkAppearance,
				networkAppearanceSet: appearance.networkAppearanceSet,
			}
			groups = append(groups, destinationAuditGroup{
				state: auditState(scope, pc, masks[index]),
			})
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
	state           DestinationNetworkState
	routingContexts []uint32
}

func appendDestinationAuditGroup(
	groups []destinationAuditGroup,
	state DestinationNetworkState,
	routingContext uint32,
) []destinationAuditGroup {
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

// writeDestinationAuditReply answers one audited destination with what this SG
// holds in both dimensions.
//
// RFC 4666 Section 4.5.3: "The status of each SS7 destination requested is
// indicated in a DUNA message (if unavailable), a DAVA message (if available),
// or a DRST (if restricted...). For national networks, the SGP SHOULD
// additionally respond with a SCON message (if the destination is congested)
// before the DAVA or DRST." The congestion report therefore precedes an
// available or restricted answer and is not sent with an unavailable one, for
// which congestion says nothing.
func (c *Association) writeDestinationAuditReply(
	networkAppearance *params.Param,
	routingContextPresent bool,
	routingContexts []uint32,
	pointCode uint32,
	mask uint8,
	state DestinationNetworkState,
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

	if state.Congestion.Congested && state.Availability != DestinationUnavailable {
		var congestion *params.Param
		if state.Congestion.LevelSet {
			congestion = params.NewCongestionIndications(state.Congestion.Level)
		}
		if _, err := c.WriteSignal(messages.NewSignallingCongestion(
			networkAppearance.Copy(), routingContext(), affectedPointCode(), nil, congestion, nil,
		)); err != nil {
			return err
		}
	}

	var reply messages.M3UA
	switch state.Availability {
	case DestinationUnavailable:
		reply = messages.NewDestinationUnavailable(
			networkAppearance.Copy(), routingContext(), affectedPointCode(), nil)
	case DestinationRestricted:
		reply = messages.NewDestinationRestricted(
			networkAppearance.Copy(), routingContext(), affectedPointCode(), nil)
	default:
		reply = messages.NewDestinationAvailable(
			networkAppearance.Copy(), routingContext(), affectedPointCode(), nil)
	}

	_, err := c.WriteSignal(reply)
	return err
}

// ForgetDestinations discards every SSNM destination record the association's
// state store retains and reports how many were released.
//
// It is the reclaim path for the record budget: a store holding
// MaxSSNMDestinationRecords, or DefaultMaxSSNMDestinationRecords where no ASP
// Endpoint configured one, refuses further destinations until something
// releases the space. Nothing is sent on the wire. An ASP re-learns what it
// forgot with a DAUD (RFC 4666 Section 4.5.3); at an SGP the next audit a peer
// sends is answered DUNA for every forgotten point code, because Section 4.5.3
// makes an unknown Signalling Point Code unavailable.
//
// On an accepted association this clears the node-wide view the owning SGP
// Endpoint holds, which is shared by every Listener, Association and ASP it
// serves and is the view Endpoint.ReportDestinationAvailability writes.
func (c *Association) ForgetDestinations() int {
	if c == nil {
		return 0
	}
	return c.destinations.forget()
}
