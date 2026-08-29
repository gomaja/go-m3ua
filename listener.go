// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

// Listener accepts SCTP associations for an Endpoint.
//
// Listener is a transport-orientation type, not an M3UA protocol role. The
// Endpoint that created it determines whether accepted associations run ASP,
// SGP, or (through explicit Single Exchange model or Double Exchange model
// APIs) IPSP procedures.
type Listener struct {
	sctpListener *sctp.SCTPListener
	endpoint     *Endpoint
	*AssociationConfig
	listenerConfig *ListenerConfig

	// muConns guards conns, which tracks the associations this listener has
	// accepted so Close can take them down with it.
	muConns sync.Mutex
	conns   map[*Association]struct{}
	// pendingSCTP contains accepted SCTP associations that have not yet been
	// promoted to M3UA Associations. Close owns these sockets too, including
	// while SelectAssociationConfig is running.
	pendingSCTP map[*sctp.SCTPConn]struct{}
	// activeAccept counts accepted SCTP associations still inside Accept,
	// including peer selection and M3UA establishment.
	activeAccept int
	closed       bool
	closeDone    chan struct{}
	closeErr     error

	// restarts turns SCTP association-change events into M-SCTP_RESTART
	// indications. One per Listener, because the dependency gives every
	// accepted association the listener's handler; it routes by association ID.
	restarts *restartWatcher

	// nif records isolation from the nodal interworking function, which
	// RFC 4666 Section 4.7 makes the SGP answer differently.
	nif *nifAvailability

	// destinations is the SG's view of which SS7 destinations are reachable,
	// shared by every Association owned by the SGP Endpoint.
	//
	// It belongs to the node, not to any one ASP: an SG learns it from the SS7
	// network, and Section 4.5.3 has it answer every ASP's DAUD from the same
	// view. Held per-Association, it was lost whenever an ASP reconnected, and the
	// audit a recovering ASP sends was then answered DUNA for destinations the
	// SG knew were reachable.
	destinations *destinations

	// as references the Application Servers owned by the SGP or IPSP Endpoint,
	// keyed by ASKey.
	//
	// The AS state machine of RFC 4666 Section 4.3.2 is a property of the group
	// of ASPs serving a Routing Context, not of any one association, so it
	// lives on Endpoint rather than on an Association: "the last remaining active
	// ASP in the AS", Override's "previously active ASP in the AS", and a Notify
	// sent "to all ASPs in the AS" are all statements about the set.
	as *applicationServers

	// mtp3Restarts coordinates the node-wide MTP3 restart procedure of RFC 4666
	// Section 4.6 without changing the ASP, AS, T(ack), or NIF state machines.
	mtp3Restarts *mtp3RestartRegistry
}

// ApplicationServerState returns this Endpoint's AS state for a Routing
// Context. RFC 4666 Section 4.3.2 defines the SGP's AS state machine, while
// Sections 4.3.1 and 4.3.4.3 require an IPSP to retain the corresponding
// per-AS state of its remote IPSPs.
func (l *Listener) ApplicationServerState(rtCtx uint32) ASState {
	as := l.applicationServers()
	if as == nil {
		return ASDown
	}
	_, scoped, ok, ambiguous := as.lookupRoutingContext(rtCtx)
	if !ok || ambiguous {
		return ASDown
	}
	return scoped.State()
}

// ApplicationServerStateForAS returns the AS state for an exact ASKey.
func (l *Listener) ApplicationServerStateForAS(key ASKey) ASState {
	as := l.applicationServers()
	if as == nil {
		return ASDown
	}
	scoped, ok := as.lookup(key)
	if !ok {
		return ASDown
	}
	return scoped.State()
}

// applicationServers returns the SGP or IPSP Endpoint's Application Server
// registry, or nil when this Listener belongs to an ASP Endpoint.
func (l *Listener) applicationServers() *applicationServers {
	l.muConns.Lock()
	defer l.muConns.Unlock()

	return l.as
}

// track registers an accepted Association so Close can shut it down, and
// reports whether the listener is still open. An Association accepted while
// Close is running
// would otherwise outlive the listener that produced it.
func (l *Listener) track(c *Association) bool {
	l.muConns.Lock()
	defer l.muConns.Unlock()

	if l.closed {
		return false
	}
	if l.conns == nil {
		l.conns = make(map[*Association]struct{})
	}
	l.conns[c] = struct{}{}

	return true
}

// beginAccept makes the Listener own an accepted SCTP association before any
// peer configuration or M3UA establishment work starts.
func (l *Listener) beginAccept(sctpAssociation *sctp.SCTPConn) bool {
	l.muConns.Lock()
	defer l.muConns.Unlock()

	if l.closed {
		return false
	}
	if l.pendingSCTP == nil {
		l.pendingSCTP = make(map[*sctp.SCTPConn]struct{})
	}
	l.pendingSCTP[sctpAssociation] = struct{}{}
	l.activeAccept++
	return true
}

// rejectPendingSCTP removes a socket that will not become an M3UA Association.
func (l *Listener) rejectPendingSCTP(sctpAssociation *sctp.SCTPConn) {
	l.muConns.Lock()
	delete(l.pendingSCTP, sctpAssociation)
	l.muConns.Unlock()
}

// promoteAcceptedAssociation atomically attaches shared SGP state and moves a
// socket from the pre-M3UA set into the Listener's Association set.
func (l *Listener) promoteAcceptedAssociation(association *Association) bool {
	l.muConns.Lock()
	defer l.muConns.Unlock()

	delete(l.pendingSCTP, association.sctpConn)
	if l.closed {
		return false
	}
	switch association.role {
	case RoleSGP:
		association.as, association.nif, association.destinations = l.registryLocked()
		association.mtp3Restarts = l.mtp3Restarts
	case RoleIPSP:
		association.as = l.as
	}
	registration := &applicationServerReservation{}
	if association.as != nil {
		registration = association.as.reserve(association.configuredASKeys())
	}
	if l.endpoint == nil || !l.endpoint.trackAssociation(association) {
		registration.rollback()
		return false
	}
	association.asReservation = registration
	if l.conns == nil {
		l.conns = make(map[*Association]struct{})
	}
	l.conns[association] = struct{}{}
	return true
}

func (l *Listener) finishAccept() {
	l.muConns.Lock()
	l.activeAccept--
	l.muConns.Unlock()
}

// registry attaches the SGP Endpoint's Application Server, NIF, destination,
// and MTP3 restart state to this Listener.
//
// Acceptance promotion invokes registryLocked before starting the
// Association's goroutines, because the dispatcher reads Association.as on
// every state change and Association.nif on every ASP Up. Assigning them after
// monitor starts is a data race.
func (l *Listener) registry() (*applicationServers, *nifAvailability, *destinations) {
	l.muConns.Lock()
	defer l.muConns.Unlock()
	return l.registryLocked()
}

func (l *Listener) registryLocked() (*applicationServers, *nifAvailability, *destinations) {
	return l.as, l.nif, l.destinations
}

func newListener(endpoint *Endpoint, config *ListenerConfig) *Listener {
	listenerConfig := NewListenerConfig(nil)
	if config != nil {
		listenerConfig = NewListenerConfig(config.DefaultAssociationConfig)
		listenerConfig.SelectAssociationConfig = config.SelectAssociationConfig
	}
	listener := &Listener{
		AssociationConfig: listenerConfig.DefaultAssociationConfig,
		listenerConfig:    listenerConfig,
		endpoint:          endpoint,
		closeDone:         make(chan struct{}),
	}
	if endpoint != nil {
		switch endpoint.Role() {
		case RoleSGP:
			listener.as, listener.nif, listener.destinations, listener.mtp3Restarts = endpoint.sgpRegistry()
		case RoleIPSP:
			listener.as = endpoint.applicationServerRegistry()
		}
	}
	return listener
}

// Role returns the immutable M3UA protocol role of associations accepted by
// this Listener.
func (l *Listener) Role() Role {
	if l == nil || l.endpoint == nil {
		return 0
	}
	return l.endpoint.Role()
}

// SetDestinationState records a destination's availability at this SG, for every
// association it serves and every one it will serve.
//
// RFC 4666 Section 4.5.3 has an SG answer a DAUD from what it knows of the SS7
// network, and that knowledge is a property of the node: it does not arrive over
// any ASP's association and does not leave with one. Recording it per Association meant
// an ASP that reconnected — which Section 4.4.2 has it do precisely so it can
// resynchronise — was answered DUNA for destinations this SG knew were
// reachable, until an operator happened to set them again on the new
// association.
//
// It may be called before any association exists.
func (l *Listener) SetDestinationState(pointCode uint32, state DestinationState) {
	l.SetDestinationRange(pointCode, 0, state)
}

// SetDestinationRange records an all-Routing-Context destination range in the
// configured Network Appearance. Mask wildcards that many low-order bits.
func (l *Listener) SetDestinationRange(pointCode uint32, mask uint8, state DestinationState) {
	var configured *params.Param
	if l.AssociationConfig != nil {
		configured = l.AssociationConfig.NetworkAppearance
	}
	appearance, set := appearanceOf(configured)
	_ = l.applyDestinationRange(DestinationRange{
		NetworkAppearance:    appearance,
		NetworkAppearanceSet: set,
		PointCode:            pointCode,
		Mask:                 mask,
		State:                state,
	}, false)
}

// SetDestinationStateForNetwork records this SG's state for a destination in
// an explicit Network Appearance.
func (l *Listener) SetDestinationStateForNetwork(networkAppearance, pointCode uint32, state DestinationState) {
	l.SetDestinationRangeForNetwork(networkAppearance, pointCode, 0, state)
}

// SetDestinationRangeForNetwork records an all-Routing-Context destination
// range in an explicit Network Appearance.
func (l *Listener) SetDestinationRangeForNetwork(networkAppearance, pointCode uint32, mask uint8, state DestinationState) {
	_ = l.applyDestinationRange(DestinationRange{
		NetworkAppearance:    networkAppearance,
		NetworkAppearanceSet: true,
		PointCode:            pointCode,
		Mask:                 mask,
		State:                state,
	}, false)
}

// SetDestinationStateForNetworkAndRoutingContext records one exact destination
// in an explicit Network Appearance and Routing Context.
func (l *Listener) SetDestinationStateForNetworkAndRoutingContext(networkAppearance, routingContext, pointCode uint32, state DestinationState) {
	l.SetDestinationRangeForNetworkAndRoutingContext(
		networkAppearance, routingContext, pointCode, 0, state)
}

// SetDestinationRangeForNetworkAndRoutingContext records a destination range
// in one explicit Network Appearance and Routing Context.
func (l *Listener) SetDestinationRangeForNetworkAndRoutingContext(networkAppearance, routingContext, pointCode uint32, mask uint8, state DestinationState) {
	_ = l.applyDestinationRange(DestinationRange{
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
func (l *Listener) ReportDestinationState(pointCode uint32, state DestinationState) error {
	return l.ReportDestinationRange(pointCode, 0, state)
}

// ReportDestinationRange records and synchronously reports a destination range.
func (l *Listener) ReportDestinationRange(pointCode uint32, mask uint8, state DestinationState) error {
	var configured *params.Param
	if l.AssociationConfig != nil {
		configured = l.AssociationConfig.NetworkAppearance
	}
	appearance, set := appearanceOf(configured)
	return l.applyDestinationRange(DestinationRange{
		NetworkAppearance:    appearance,
		NetworkAppearanceSet: set,
		PointCode:            pointCode,
		Mask:                 mask,
		State:                state,
	}, true)
}

// ReportDestinationStateForNetwork records and synchronously reports a
// destination in an explicit Network Appearance.
func (l *Listener) ReportDestinationStateForNetwork(networkAppearance, pointCode uint32, state DestinationState) error {
	return l.ReportDestinationRangeForNetwork(networkAppearance, pointCode, 0, state)
}

// ReportDestinationRangeForNetwork records and synchronously reports a range
// in an explicit Network Appearance.
func (l *Listener) ReportDestinationRangeForNetwork(networkAppearance, pointCode uint32, mask uint8, state DestinationState) error {
	return l.applyDestinationRange(DestinationRange{
		NetworkAppearance:    networkAppearance,
		NetworkAppearanceSet: true,
		PointCode:            pointCode,
		Mask:                 mask,
		State:                state,
	}, true)
}

// ReportDestinationStateForNetworkAndRoutingContext records and synchronously
// reports one destination in one explicit traffic scope.
func (l *Listener) ReportDestinationStateForNetworkAndRoutingContext(networkAppearance, routingContext, pointCode uint32, state DestinationState) error {
	return l.ReportDestinationRangeForNetworkAndRoutingContext(
		networkAppearance, routingContext, pointCode, 0, state,
	)
}

// ReportDestinationRangeForNetworkAndRoutingContext records and synchronously
// reports a destination range to the currently concerned active ASPs.
func (l *Listener) ReportDestinationRangeForNetworkAndRoutingContext(networkAppearance, routingContext, pointCode uint32, mask uint8, state DestinationState) error {
	return l.applyDestinationRange(DestinationRange{
		NetworkAppearance:    networkAppearance,
		NetworkAppearanceSet: true,
		RoutingContext:       routingContext,
		RoutingContextSet:    true,
		PointCode:            pointCode,
		Mask:                 mask,
		State:                state,
	}, true)
}

func (l *Listener) applyDestinationRange(rangeValue DestinationRange, wait bool) error {
	if l == nil || l.Role() != RoleSGP {
		if wait {
			return ErrUnsupportedRole
		}
		return nil
	}
	l.registry()
	prepared, err := l.prepareLocalDestinationRange(rangeValue)
	if err != nil {
		return err
	}
	restarts := l.mtp3Restarts
	restarts.procedureMu.RLock()
	defer restarts.procedureMu.RUnlock()
	l.muConns.Lock()
	closed := l.closed
	l.muConns.Unlock()
	if closed {
		return ErrAssociationClosed
	}
	if stageAnyMTP3RestartRangeLocked(restarts, prepared) {
		return nil
	}
	destinations := l.destinationRegistry()
	previous, known := destinations.lookupRange(
		destinationRangeKey(prepared), prepared.PointCode, prepared.Mask,
	)
	destinations.setRanges([]DestinationRange{prepared})
	abateCongestion := known && previous == DestinationCongested && prepared.State != DestinationCongested
	return l.publishDestinationRanges([]DestinationRange{prepared}, false, abateCongestion, wait)
}

func (l *Listener) legacyDestinationScope(networkAppearance uint32, networkAppearanceSet bool) destinationKey {
	scope := destinationKey{
		networkAppearance:    networkAppearance,
		networkAppearanceSet: networkAppearanceSet,
	}
	if l.AssociationConfig == nil || l.AssociationConfig.RoutingContexts == nil {
		return scope
	}
	configured := l.AssociationConfig.RoutingContexts.RoutingContexts()
	if len(configured) == 1 {
		scope.routingContext = configured[0]
		scope.routingContextSet = true
	}
	return scope
}

// DestinationState reports what this SG last recorded for a destination, and
// whether anything was recorded at all. With several configured Routing
// Contexts the legacy API sees only all-context baselines; use
// DestinationStateForNetworkAndRoutingContext for a per-flow answer.
func (l *Listener) DestinationState(pointCode uint32) (DestinationState, bool) {
	l.muConns.Lock()
	d := l.destinations
	l.muConns.Unlock()

	if d == nil {
		return DestinationUnavailable, false
	}
	var configured *params.Param
	if l.AssociationConfig != nil {
		configured = l.AssociationConfig.NetworkAppearance
	}
	appearance, set := appearanceOf(configured)
	scope := l.legacyDestinationScope(appearance, set)
	scope.pointCode = pointCode
	return d.lookup(scope)
}

// DestinationStateForNetwork reports this SG's state for a destination in an
// explicit Network Appearance, and whether it has been recorded. With several
// configured Routing Contexts the legacy API sees only all-context baselines.
func (l *Listener) DestinationStateForNetwork(networkAppearance, pointCode uint32) (DestinationState, bool) {
	l.muConns.Lock()
	d := l.destinations
	l.muConns.Unlock()

	if d == nil {
		return DestinationUnavailable, false
	}
	scope := l.legacyDestinationScope(networkAppearance, true)
	scope.pointCode = pointCode
	return d.lookup(scope)
}

// DestinationStateForNetworkAndRoutingContext reports this SG's state for one
// destination in an explicit Network Appearance and Routing Context.
func (l *Listener) DestinationStateForNetworkAndRoutingContext(networkAppearance, routingContext, pointCode uint32) (DestinationState, bool) {
	l.muConns.Lock()
	d := l.destinations
	l.muConns.Unlock()

	if d == nil {
		return DestinationUnavailable, false
	}
	return d.lookup(destinationKey{
		networkAppearance:    networkAppearance,
		networkAppearanceSet: true,
		routingContext:       routingContext,
		routingContextSet:    true,
		pointCode:            pointCode,
	})
}

// DestinationRanges returns every range visible through DestinationState in
// update order. With several configured Routing Contexts it contains only
// all-context baselines.
func (l *Listener) DestinationRanges() []DestinationRange {
	l.muConns.Lock()
	d := l.destinations
	l.muConns.Unlock()

	if d == nil {
		return []DestinationRange{}
	}
	var configured *params.Param
	if l.AssociationConfig != nil {
		configured = l.AssociationConfig.NetworkAppearance
	}
	appearance, set := appearanceOf(configured)
	return d.rangesForScope(l.legacyDestinationScope(appearance, set))
}

// DestinationRangesForNetwork returns every range visible for an explicit
// Network Appearance in update order. With several configured Routing Contexts
// it contains only all-context baselines.
func (l *Listener) DestinationRangesForNetwork(networkAppearance uint32) []DestinationRange {
	l.muConns.Lock()
	d := l.destinations
	l.muConns.Unlock()

	if d == nil {
		return []DestinationRange{}
	}
	return d.rangesForScope(l.legacyDestinationScope(networkAppearance, true))
}

// DestinationRangesForNetworkAndRoutingContext returns every all-context and
// per-context range visible in one Network Appearance and Routing Context, in
// update order.
func (l *Listener) DestinationRangesForNetworkAndRoutingContext(networkAppearance, routingContext uint32) []DestinationRange {
	l.muConns.Lock()
	d := l.destinations
	l.muConns.Unlock()

	if d == nil {
		return []DestinationRange{}
	}
	return d.rangesForScope(destinationKey{
		networkAppearance:    networkAppearance,
		networkAppearanceSet: true,
		routingContext:       routingContext,
		routingContextSet:    true,
	})
}

// forget drops an Association from the listener's set, so a long-lived Listener does
// not accumulate every association it has ever accepted.
func (l *Listener) forget(c *Association) {
	l.muConns.Lock()
	delete(l.conns, c)
	l.muConns.Unlock()
}

// Listen returns an SCTP listener whose accepted associations run this
// Endpoint's M3UA role.
func (e *Endpoint) Listen(network string, laddr *sctp.SCTPAddr, cfg *ListenerConfig) (*Listener, error) {
	if !e.beginOperation() {
		return nil, ErrEndpointClosed
	}
	defer e.endOperation()

	var err error
	role, err := e.associationRole()
	if err != nil {
		return nil, err
	}
	l := newListener(e, cfg)
	if err := validateAssociationConfigForRole(role, l.AssociationConfig); err != nil {
		return nil, err
	}

	n, ok := netMap[network]
	if !ok {
		return nil, fmt.Errorf("invalid network: %s", network)
	}
	// Through SocketConfig rather than ListenSCTP so a notification handler can
	// be installed. The dependency fixes a listener's handler at construction
	// and gives the same one to every association it accepts, so the handler
	// routes by association ID; see restartWatcher.
	l.restarts = &restartWatcher{}
	l.restarts.setRoute(l.associationForSCTPID)
	scfg := &sctp.SocketConfig{NotificationHandler: l.restarts.handle}

	l.sctpListener, err = scfg.Listen(n, laddr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen SCTP: %w", err)
	}
	if !e.trackListener(l) {
		_ = l.sctpListener.Close()
		return nil, ErrEndpointClosed
	}
	return l, nil
}

// associationForSCTPID finds the accepted Association with the given SCTP
// association identifier, or nil.
//
// Linear over the tracked associations rather than a second map keyed by ID:
// the set is the ASPs a single SGP serves, it is walked only when the kernel
// reports an association event, and a second index would be one more thing to
// keep in step with track and forget.
func (l *Listener) associationForSCTPID(id sctp.SCTPAssocID) *Association {
	l.muConns.Lock()
	defer l.muConns.Unlock()

	for c := range l.conns {
		if c.assocID.Load() == int32(id) {
			return c
		}
	}
	return nil
}

// Accept waits for and returns the next M3UA association.
// After establishment, DATA can be read through Association.Read; M3UA control
// procedures continue in background goroutines.
//
// Accept does not return until the M3UA handshake for that peer has completed,
// or until it gives up on it after ten seconds. A single accept loop therefore
// serves peers strictly one at a time, and one silent peer holds up every other
// ASP waiting behind it for the whole of that budget.
//
// Accept is safe for concurrent use, so an endpoint expecting several SCTP
// associations should run several Accepts rather than one loop:
//
//	for i := 0; i < concurrency; i++ {
//		go func() {
//			for {
//				association, err := l.Accept(ctx)
//				if err != nil {
//					return
//				}
//				go serve(association)
//			}
//		}()
//	}
//
// Nothing in Accept writes to shared AssociationConfig, and each accepted
// Association owns its SCTP association; TestConcurrentAcceptsAreIndependent
// covers this.
//
// Cancelling ctx does not interrupt an Accept that is blocked waiting for a peer
// to connect — only Close does. Once a peer has connected, ctx bounds the
// handshake, alongside AssociationConfig.EstablishTimeout.
//
// A failure after SCTP accept and before M3UA establishment is returned as an
// AssociationEstablishmentError. An SCTP listener failure is returned directly.
func (l *Listener) Accept(ctx context.Context) (*Association, error) {
	role, err := l.endpoint.associationRole()
	if err != nil {
		return nil, err
	}

	// The SCTP association is accepted before the Association is built, so
	// nothing has to be unwound if the accept itself fails.
	sctpAssociation, err := l.sctpListener.AcceptSCTP()
	if err != nil {
		return nil, err
	}
	if !l.beginAccept(sctpAssociation) {
		_ = sctpAssociation.Close()
		return nil, net.ErrClosed
	}
	defer l.finishAccept()

	// Every Association this Listener produces can share AssociationConfig, so
	// this function must treat it as read-only. The SCTP association and the
	// settings derived from it
	// live on the Association: while they lived on AssociationConfig, Accept
	// rebound the previously accepted Association's socket to the SCTP
	// association just taken, and that Association served the wrong ASP.
	acceptInfo := newAcceptInfo(sctpAssociation.LocalAddr(), sctpAssociation.RemoteAddr())
	associationConfig, err := l.listenerConfig.associationConfigForAccept(acceptInfo)
	if err != nil {
		l.rejectPendingSCTP(sctpAssociation)
		_ = sctpAssociation.Close()
		return nil, &AssociationEstablishmentError{RemoteAddr: acceptInfo.RemoteAddr, Err: err}
	}
	if err := validateAssociationConfigForRole(role, associationConfig); err != nil {
		l.rejectPendingSCTP(sctpAssociation)
		_ = sctpAssociation.Close()
		return nil, &AssociationEstablishmentError{RemoteAddr: acceptInfo.RemoteAddr, Err: err}
	}
	if err := l.endpoint.validateAssociationConfig(associationConfig); err != nil {
		l.rejectPendingSCTP(sctpAssociation)
		_ = sctpAssociation.Close()
		return nil, &AssociationEstablishmentError{RemoteAddr: acceptInfo.RemoteAddr, Err: err}
	}
	association := newAssociationWithTrafficModePolicy(role, associationConfig, newTrafficModePolicy(associationConfig))
	association.sctpConn = sctpAssociation
	// Set at construction, before any goroutine can observe this Association, so
	// the field is immutable for its lifetime: Close reads it concurrently
	// with Accept, and assigning it later is a data race. The same applies to
	// the Application Server registry and the NIF state, which the dispatcher
	// reads as soon as monitor() starts.
	association.listener = l
	if !l.promoteAcceptedAssociation(association) {
		_ = association.closeWith(ErrFailedToEstablish)
		return nil, &AssociationEstablishmentError{
			RemoteAddr: acceptInfo.RemoteAddr,
			Err:        ErrFailedToEstablish,
		}
	}

	if err := association.setUpSocket(); err != nil {
		_ = association.closeWith(err)
		return nil, &AssociationEstablishmentError{RemoteAddr: acceptInfo.RemoteAddr, Err: err}
	}

	// The opening ASP-DOWN transition is applied by monitor() itself, before it
	// starts dispatching, rather than published from a goroutine here that
	// raced the reader for it.
	go association.monitor(ctx)
	establishTimeout := association.cfg.EstablishTimeout
	if establishTimeout <= 0 {
		establishTimeout = DefaultEstablishTimeout
	}

	select {
	case <-association.established:
		association.asReservation.commit()
		return association, nil
	case <-association.done:
		// done closes at the beginning of closeWith. Join the once-only teardown
		// before returning, so Endpoint membership and provisional AS scopes have
		// both been released.
		_ = association.closeWith(nil)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := association.Err(); err != nil {
			return nil, &AssociationEstablishmentError{RemoteAddr: acceptInfo.RemoteAddr, Err: err}
		}
		return nil, &AssociationEstablishmentError{RemoteAddr: acceptInfo.RemoteAddr, Err: ErrFailedToEstablish}
	case <-ctx.Done():
		_ = association.closeWith(ctx.Err())
		return nil, ctx.Err()
	case <-time.After(establishTimeout):
		_ = association.closeWith(ErrTimeout)
		return nil, &AssociationEstablishmentError{RemoteAddr: acceptInfo.RemoteAddr, Err: ErrTimeout}
	}
}

// Close closes the listener.
func (l *Listener) Close() error {
	// Take the accepted associations down with the listener. Closing only the
	// SCTP listener left every Association it had produced running: their
	// monitor and reader goroutines carried on against peers that had no idea
	// the service was gone, so closing a Listener leaked one set of goroutines
	// per accepted association and left half-open associations behind.
	l.muConns.Lock()
	if l.closed {
		closeDone := l.closeDone
		l.muConns.Unlock()
		<-closeDone
		l.muConns.Lock()
		closeErr := l.closeErr
		l.muConns.Unlock()
		return closeErr
	}
	l.closed = true
	conns := make([]*Association, 0, len(l.conns))
	for c := range l.conns {
		conns = append(conns, c)
	}
	l.conns = nil
	pendingSCTP := make([]*sctp.SCTPConn, 0, len(l.pendingSCTP))
	for association := range l.pendingSCTP {
		pendingSCTP = append(pendingSCTP, association)
	}
	l.pendingSCTP = nil
	l.muConns.Unlock()

	var firstErr error
	if l.sctpListener != nil {
		if err := l.sctpListener.Close(); err != nil {
			firstErr = err
		}
	}
	for _, association := range pendingSCTP {
		if err := association.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	// Association.Close is idempotent. The listening socket is already closed, so no
	// new association can enter while the existing set is being released.
	for _, c := range conns {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if l.endpoint != nil {
		l.endpoint.forgetListener(l)
	}
	l.muConns.Lock()
	l.closeErr = firstErr
	close(l.closeDone)
	l.muConns.Unlock()

	return firstErr
}

// Addr returns the listener's network address.
func (l *Listener) Addr() net.Addr {
	return l.sctpListener.Addr()
}

// ActiveASPs returns the associations currently able to carry traffic for a
// Routing Context, in a stable order.
//
// It is the raw form of the distribution function RFC 4666 Section 1.4.2.4
// describes: "To direct messages received from the SS7 MTP3 network to the
// appropriate IP destination, the SGP must perform a message distribution
// function using information from the received MTP3-User message." Which
// Routing Key an inbound MTP3 message matches is decided above this library,
// which has no MTP3 ingress of its own; what the library can answer, and could
// not before, is which ASPs are presently serving the AS that Routing Key maps
// to.
func (l *Listener) ActiveASPs(rtCtx uint32) []*Association {
	as := l.applicationServers()
	if as == nil {
		return nil
	}
	_, scoped, ok, ambiguous := as.lookupRoutingContext(rtCtx)
	if !ok || ambiguous {
		return nil
	}
	return scoped.activeASPs()
}

// ActiveASPsForAS returns the associations currently able to carry traffic for
// an exact ASKey.
func (l *Listener) ActiveASPsForAS(key ASKey) []*Association {
	as := l.applicationServers()
	if as == nil {
		return nil
	}
	scoped, ok := as.lookup(key)
	if !ok {
		return nil
	}
	return scoped.activeASPs()
}

// ASPsForTraffic returns the associations a message for this Routing Context
// should be sent to, applying the AS's traffic mode.
//
// RFC 4666 Section 4.3.4.3 gives one rule per mode:
//
//   - Override: "receipt of an ASP Active message at an SGP causes the
//     (re)direction of all traffic for the AS to the ASP that sent the ASP
//     Active message", so exactly one ASP carries traffic.
//   - Loadshare: traffic goes to all active ASPs, and "The algorithm at the SGP
//     for loadsharing traffic within an AS to all the active ASPs is
//     implementation dependent.  The algorithm could, for example, be
//     round-robin or based on information in the Data message (e.g., the SLS
//     ...)". This uses the SLS, because Section 1.4.7 requires that "Traffic
//     that requires sequencing SHOULD be assigned to the same stream" and the
//     same reasoning applies to the choice of ASP: consecutive messages sharing
//     an SLS must not be split across ASPs.
//   - Broadcast: "a simple broadcast algorithm, where every message is sent to
//     each of the active ASPs", so all of them.
//
// An empty result means the AS has no ASP able to take traffic, which for an
// AS-PENDING AS is the point: Section 4.3.2 has the SGP queue rather than
// discard while T(r) runs.
func (l *Listener) ASPsForTraffic(rtCtx uint32, sls uint8) []*Association {
	registry := l.applicationServers()
	if registry == nil {
		return nil
	}
	_, as, ok, ambiguous := registry.lookupRoutingContext(rtCtx)
	if !ok || ambiguous {
		return nil
	}
	return aspsForTraffic(as, sls)
}

// ASPsForTrafficForAS returns the associations a message for this exact ASKey
// should be sent to, applying the AS's traffic mode.
func (l *Listener) ASPsForTrafficForAS(key ASKey, sls uint8) []*Association {
	registry := l.applicationServers()
	if registry == nil {
		return nil
	}
	as, ok := registry.lookup(key)
	if !ok {
		return nil
	}
	return aspsForTraffic(as, sls)
}

func aspsForTraffic(as *applicationServer, sls uint8) []*Association {
	if as == nil {
		return nil
	}
	active := as.activeASPs()
	if len(active) == 0 {
		return nil
	}

	switch as.TrafficMode() {
	case params.TrafficModeBroadcast:
		return active
	case params.TrafficModeOverride:
		// Override leaves at most one ASP active, but if configuration has not
		// yet converged the first in the stable order is the deterministic
		// choice rather than an arbitrary one.
		return active[:1]
	default:
		// Loadshare, and the unset case: one ASP, chosen by SLS so a given SLS
		// always lands on the same one.
		return []*Association{active[int(sls)%len(active)]}
	}
}

// SetNIFAvailable declares whether this SGP can still reach the SS7 network
// through its nodal interworking function.
//
// RFC 4666 Section 4.7 describes what an SGP should do when it cannot: "If an
// SGP is isolated entirely from the NIF, the SGP should send ASP Down Ack to
// all its connected ASPs.  Upon receiving an ASP Up message while isolated from
// the NIF, the SGP should respond with an Error ("Refused - Management
// Blocking")."
//
// The NIF is above this library — it is where MTP3 meets M3UA, and this package
// has no MTP3 side — so only the application knows when it has gone. Declaring
// it here is what lets the library produce the behaviour the section describes;
// without it, error code 0x0d could never be sent at all. It returns
// ErrUnsupportedRole for a Listener whose Endpoint is not an SGP.
func (l *Listener) SetNIFAvailable(available bool) error {
	if l.Role() != RoleSGP {
		return ErrUnsupportedRole
	}
	l.muConns.Lock()
	if l.closed {
		l.muConns.Unlock()
		return ErrAssociationClosed
	}
	endpoint := l.endpoint
	l.muConns.Unlock()
	if endpoint == nil {
		return ErrNotEstablished
	}
	return endpoint.setNIFAvailable(available)
}

// SetASAvailable declares whether this SGP can still service one Application
// Server, for the partial-failure case of RFC 4666 Section 4.7:
//
//	If an SGP suffers a partial failure (where an SGP can continue to
//	service one or more active AS but due to a partial failure it is
//	unable to service one or more other active AS), the SGP should send
//	ASP Inactive Ack to all its connected ASPs for the affected AS.
//	Upon receiving an ASP Active message for an affected AS while still
//	partially isolated from the NIF, the SGP should respond with an
//	Error ("Refused - Management Blocking").
//
// It returns ErrUnsupportedRole for a Listener whose Endpoint is not an SGP.
func (l *Listener) SetASAvailable(rtCtx uint32, available bool) error {
	endpoint, err := l.openAvailabilityEndpoint()
	if err != nil {
		return err
	}
	return endpoint.setASAvailable(rtCtx, available)
}

// SetASAvailableForAS declares whether this SGP can still service one exact
// Application Server. It returns ErrUnsupportedRole for a Listener whose
// Endpoint is not an SGP.
func (l *Listener) SetASAvailableForAS(key ASKey, available bool) error {
	endpoint, err := l.openAvailabilityEndpoint()
	if err != nil {
		return err
	}
	return endpoint.setASAvailableForAS(key, available)
}

func (l *Listener) openAvailabilityEndpoint() (*Endpoint, error) {
	if l.Role() != RoleSGP {
		return nil, ErrUnsupportedRole
	}
	l.muConns.Lock()
	defer l.muConns.Unlock()
	if l.closed {
		return nil, ErrAssociationClosed
	}
	if l.endpoint == nil {
		return nil, ErrNotEstablished
	}
	return l.endpoint, nil
}

// SetNIFAvailable declares whether this SGP Endpoint can reach the SS7 network
// through its nodal interworking function. The state applies to every SGP
// Association owned by the Endpoint. RFC 4666 Section 4.7
// defines the resulting ASP Down Ack and management-blocking procedures.
func (c *Association) SetNIFAvailable(available bool) error {
	if err := c.validateSGPAvailabilityControl(); err != nil {
		return err
	}
	if c.endpoint != nil {
		return c.endpoint.setNIFAvailable(available)
	}
	c.nif.setIsolated(!available)
	if !available {
		isolateNIFConnection(c)
	}
	return nil
}

// SetASAvailable declares whether this SGP Endpoint can service one
// unambiguous Routing Context.
func (c *Association) SetASAvailable(rtCtx uint32, available bool) error {
	if err := c.validateSGPAvailabilityControl(); err != nil {
		return err
	}
	if c.endpoint != nil {
		return c.endpoint.setASAvailable(rtCtx, available)
	}
	registryKey, _, registryOK, registryAmbiguous := c.as.lookupRoutingContext(rtCtx)
	if registryAmbiguous {
		return nil
	}
	configuredKey, configuredOK, configuredAmbiguous := c.singleConfiguredASKeyForRoutingContext(rtCtx)
	if configuredAmbiguous {
		return nil
	}
	if registryOK && configuredOK && registryKey != configuredKey {
		return nil
	}
	if registryOK {
		return c.SetASAvailableForAS(registryKey, available)
	}
	if configuredOK {
		return c.SetASAvailableForAS(configuredKey, available)
	}
	return c.SetASAvailableForAS(c.as.routingContextASKey(rtCtx), available)
}

// SetASAvailableForAS declares whether this SGP Endpoint can service one exact
// Application Server.
func (c *Association) SetASAvailableForAS(key ASKey, available bool) error {
	if err := c.validateSGPAvailabilityControl(); err != nil {
		return err
	}
	if c.endpoint != nil {
		return c.endpoint.setASAvailableForAS(key, available)
	}
	c.nif.setASAvailableForAS(key, available)
	if available {
		return nil
	}
	for _, configuredKey := range c.configuredASKeys() {
		if configuredKey == key {
			isolateApplicationServerConnection(c, c.as, key)
			break
		}
	}
	return nil
}

func (c *Association) validateSGPAvailabilityControl() error {
	if c == nil || c.Role() != RoleSGP {
		return ErrUnsupportedRole
	}
	select {
	case <-c.done:
		return ErrAssociationClosed
	default:
	}
	if c.nif == nil || c.as == nil {
		return ErrNotEstablished
	}
	return nil
}

func (c *Association) singleConfiguredASKeyForRoutingContext(rtCtx uint32) (ASKey, bool, bool) {
	var found ASKey
	foundSet := false
	for _, key := range c.configuredASKeys() {
		if !key.RoutingContextSet || key.RoutingContext != rtCtx {
			continue
		}
		if foundSet && key != found {
			return ASKey{}, false, true
		}
		found = key
		foundSet = true
	}
	return found, foundSet, false
}

func isolateNIFConnection(c *Association) {
	if c == nil {
		return
	}
	c.commitState(StateASPDown)
	postAckNotify := func() {}
	if c.as != nil {
		postAckNotify = c.as.quiesceASPDown(c)
	}
	c.quiesceUnscopedTraffic()
	_ = c.writeMandatoryControls([]messages.M3UA{messages.NewAspDownAck(nil)}, false, true)
	postAckNotify()
	c.sendState(StateASPDown)
}

func isolateApplicationServerConnection(c *Association, as *applicationServers, key ASKey) {
	if c == nil {
		return
	}
	postAckNotify := func() {}
	if c.State() == StateASPActive {
		if key.RoutingContextSet {
			c.noteRoutingContextsInactive([]uint32{key.RoutingContext})
		} else {
			c.noteRoutingContextsInactive(nil)
		}
		if as != nil {
			if key.RoutingContextSet {
				postAckNotify = as.quiesceASPFor(c, []uint32{key.RoutingContext})
			} else {
				postAckNotify = as.quiesceASPFor(c, nil)
			}
		}
	}
	c.quiesceUnscopedTraffic()
	_ = c.writeMandatoryControls([]messages.M3UA{
		messages.NewAspInactiveAck(routingContextParamForASKey(key), nil),
	}, false, true)
	postAckNotify()
	if c.stateForActiveRoutingContexts() == StateASPInactive {
		c.sendState(StateASPInactive)
	}
}
