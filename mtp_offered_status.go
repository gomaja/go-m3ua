package m3ua

import "container/list"

// The routing configuration is immutable for an Endpoint lifetime. Its owner
// supplies the route generation; lifecycle capability changes retire entries.
// The method snapshot identity and value fence method generations without a
// wrapping integer. The DPC is exact even when retained SSNM covers a range.
type mtpStatusKey struct {
	route     MTPRouteID
	pointCode uint32
	methods   *congestionMethodConfig
	profile   CongestionProfile
}
type mtpStatusEntry struct {
	key      mtpStatusKey
	interval mtpStatusInterval
	mark     *mtpStatusMark
}

// Called once per original request, under aspRoutes.mu, before selection can
// fan it out or write DATA. No callback, transport call, or consumer wait occurs
// here. Lock order is existing flow lock -> route mu -> indicationMu.
func (r *aspRoutes) offerMTPTransferLocked(route aspMTPRoute, request MTPTransferRequest) {
	r.retireTransportStatusProceduresLocked()
	profile := r.routeCongestionProfile(route.id)
	if len(request.ProtocolData.Data) > maxProtocolDataOctets || !profile.allowsPriority(request.ProtocolData.MessagePriority) {
		return
	}
	status, known := offeredDestinationStatus(route, request.ProtocolData.DestinationPointCode, r.derived[route.id])
	if !known {
		return
	}
	destination := MTPDestination{MTPRoute: route.id, PointCode: request.ProtocolData.DestinationPointCode}
	current := newMTPDestinationStatus(destination, status)
	if !continuedMTPStatusKnown(current) || profile.Method == CongestionNationalWithPriorities {
		return
	}
	key := mtpStatusKey{route: route.id, pointCode: destination.PointCode, methods: r.methods, profile: profile}
	if r.statusLRU == nil {
		r.statusLRU = list.New()
		r.statusProcedures = make(map[mtpStatusKey]*list.Element)
	}
	element := r.statusProcedures[key]
	if element == nil {
		limit := 4096
		if r.methods != nil {
			limit = r.methods.maxStatusDestinations
		}
		if len(r.statusProcedures) >= limit {
			r.removeStatusProcedureLocked(r.statusLRU.Back())
		}
		element = r.statusLRU.PushFront(&mtpStatusEntry{key: key})
		r.statusProcedures[key] = element
	} else {
		r.statusLRU.MoveToFront(element)
	}
	entry := element.Value.(*mtpStatusEntry)
	if entry.mark == nil {
		entry.mark = r.statusMarks.register(route.id, destination.PointCode)
	}
	if r.statusMarks.consume(entry.mark) {
		entry.interval = mtpStatusInterval{}
	}
	r.publishOne(entry.interval.offered(profile, current))
}

// Preserve every semantic transport input change, including an onset after
// an offer consumed an unprojected clear. Equal observations do not restart.
// Consumers use the Active mirror without state/route/AS waits in the sampler.
// Even an inactive-started sample is fenced: activation can cross its commit.
func (a *Association) beginTransportStatusChange(before, after TransportCongestionState, initial bool) func() {
	changed := initial || before.Known != after.Known || (before.Known || after.Known) && before.Level != after.Level
	if !changed {
		return func() {}
	}
	return a.beginTransportStatusReset(false)
}

// The committing bit bridges snapshot publication and pending-flag delivery.
// A route consumer cannot erase it; while set it keeps the route hint live.
// Completion posts another pending reset before clearing the bit. Thus offers
// before and after the atomic snapshot store cannot consume a future onset's
// only witness. Storage is four booleans per association, with no history queue.
// Forced epoch retirement is independent of current Active eligibility; an
// ordinary observation only retires entries while its association is Active.
// Caller holds transportMu; neither phase acquires any additional lock.
func (a *Association) beginTransportStatusReset(forced bool) func() {
	if a.endpoint == nil || a.endpoint.aspRoutes == nil {
		return func() {}
	}
	routes := a.endpoint.aspRoutes
	if forced {
		a.transportStatusForcedCommitting.Store(true)
	} else {
		a.transportStatusCommitting.Store(true)
	}
	routes.transportStatusResetPending.Store(true)
	return func() {
		if forced {
			a.transportStatusForcedPending.Store(true)
			a.transportStatusForcedCommitting.Store(false)
		} else {
			a.transportStatusResetPending.Store(true)
			a.transportStatusCommitting.Store(false)
		}
		routes.transportStatusResetPending.Store(true)
	}
}

// Called with route mu held, before an offer or transport projection. A source
// change invalidates its configured routes conservatively: another contributor
// may have sustained congestion, in which case an extra initial reminder is
// safe under Q.704 (07/96) §§11.2.3.1 and 11.2.5's maximum eight-offer interval.
// It never synthesizes a clear indication. Unrelated associations affect no
// route, and the dirty hint keeps ordinary offers from scanning associations.
func (r *aspRoutes) retireTransportStatusProceduresLocked() {
	if !r.transportStatusResetPending.Swap(false) {
		return
	}
	affected := make(map[MTPRouteID]struct{})
	committing := false
	for a, identity := range r.associations {
		// Read the in-progress bit before consuming pending. A completed
		// producer has already posted pending; a live producer retains the
		// hint so the next offer also witnesses its commit window.
		changing := a.transportStatusCommitting.Load()
		forcedChanging := a.transportStatusForcedCommitting.Load()
		forcedPending := a.transportStatusForcedPending.Swap(false)
		pending := a.transportStatusResetPending.Swap(false)
		if forcedChanging || forcedPending || a.mtpStatusActive.Load() && (pending || changing) {
			sgp := r.config.sgpByIdentity[identity]
			for _, route := range sgp.routeOrder {
				affected[route] = struct{}{}
			}
		}
		committing = committing || changing || forcedChanging
	}
	if committing {
		r.transportStatusResetPending.Store(true)
	}
	r.retireRouteStatusProceduresLocked(affected)
}

// Canonical leaves are disjoint; at most 25 exact prefix lookups find a DPC.
func offeredDestinationStatus(route aspMTPRoute, pc uint32, statuses map[aspDerivedRangeKey]aspDestinationStatus) (aspDestinationStatus, bool) {
	for mask := uint8(0); mask <= route.mask; mask++ {
		if status, ok := statuses[aspDerivedRangeKey{mtpRoute: route.id, pointCode: destinationRangePrefix(pc, mask), mask: mask}]; ok {
			return status, true
		}
	}
	return aspDestinationStatus{}, false
}

func (r *aspRoutes) removeStatusProcedureLocked(element *list.Element) {
	entry := element.Value.(*mtpStatusEntry)
	r.statusMarks.forget(entry.mark)
	delete(r.statusProcedures, entry.key)
	r.statusLRU.Remove(element)
}

// Retire an ended effective episode even if clear and recongestion occur with
// no intervening offer. Equal reports and unrelated timers retain the interval.
func (r *aspRoutes) retireEndedStatusProceduresLocked(route aspMTPRoute, updated map[aspDerivedRangeKey]aspDestinationStatus, reason ...MTPStatusReason) {
	expiryProjection := len(reason) > 0 && reason[0] == MTPStatusValidityExpired
	for key, element := range r.statusProcedures {
		if key.route != route.id {
			continue
		}
		current, known := offeredDestinationStatus(route, key.pointCode, updated)
		ended := !known || !current.congested || current.receivedUnknown || current.transportUnknown || key.methods != r.methods || key.profile != r.routeCongestionProfile(route.id)
		if !ended && expiryProjection {
			previous, previousKnown := offeredDestinationStatus(route, key.pointCode, r.derived[route.id])
			ended = !previousKnown || previous != current
		}
		if ended {
			r.removeStatusProcedureLocked(element)
		}
	}
}

func (r *aspRoutes) retireRouteStatusProceduresLocked(routes map[MTPRouteID]struct{}) {
	for key, element := range r.statusProcedures {
		if _, ok := routes[key.route]; ok {
			r.removeStatusProcedureLocked(element)
		}
	}
}
