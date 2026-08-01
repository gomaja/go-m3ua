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

// Listener is a M3UA listener.
type Listener struct {
	sctpListener *sctp.SCTPListener
	*Config
	// trafficModes is copied from Config when Listen constructs the Listener.
	// Every accepted Conn and the shared AS registry inherit this one immutable
	// policy, even if the caller later reuses or mutates the Config.
	trafficModes trafficModeSnapshot

	// muConns guards conns, which tracks the associations this listener has
	// accepted so Close can take them down with it.
	muConns sync.Mutex
	conns   map[*Conn]struct{}
	closed  bool

	// restarts turns SCTP association-change events into M-SCTP_RESTART
	// indications. One per Listener, because the dependency gives every
	// accepted association the listener's handler; it routes by association ID.
	restarts *restartWatcher

	// nif records isolation from the nodal interworking function, which
	// RFC 4666 Section 4.7 makes the SGP answer differently.
	nif *nifAvailability

	// destinations is the SG's view of which SS7 destinations are reachable,
	// shared by every association this listener accepts.
	//
	// It belongs to the node, not to any one ASP: an SG learns it from the SS7
	// network, and Section 4.5.3 has it answer every ASP's DAUD from the same
	// view. Held per-Conn, it was lost whenever an ASP reconnected, and the
	// audit a recovering ASP sends was then answered DUNA for destinations the
	// SG knew were reachable.
	destinations *destinations

	// as holds the Application Servers this listener serves, one per Routing
	// Context.
	//
	// The AS state machine of RFC 4666 Section 4.3.2 is a property of the group
	// of ASPs serving a Routing Context, not of any one association, so it
	// lives here rather than on a Conn: "the last remaining active ASP in the
	// AS", Override's "previously active ASP in the AS", and a Notify sent "to
	// all ASPs in the AS" are all statements about the set.
	as *applicationServers

	// mtp3Restarts coordinates the node-wide MTP3 restart procedure of RFC 4666
	// Section 4.6 without changing the ASP, AS, T(ack), or NIF state machines.
	mtp3Restarts mtp3RestartRegistry
}

// ApplicationServerState returns the AS state for a Routing Context, as
// maintained by RFC 4666 Section 4.3.2: "The state of the AS is maintained in
// the M3UA layer on the SGPs."
func (l *Listener) ApplicationServerState(rtCtx uint32) ASState {
	as := l.applicationServers()
	if as == nil {
		return ASDown
	}
	return as.get(rtCtx).State()
}

// applicationServers returns the Application Server registry, or nil if no
// association has been accepted yet.
//
// registry() creates l.as under muConns on the first Accept, so every other
// reader has to take the same lock. Three exported query methods read the field
// directly, which is a data race against that first Accept -- one the race
// detector never reported because no test called them while accepting.
func (l *Listener) applicationServers() *applicationServers {
	l.muConns.Lock()
	defer l.muConns.Unlock()

	return l.as
}

// track registers an accepted Conn so Close can shut it down, and reports
// whether the listener is still open. A Conn accepted while Close is running
// would otherwise outlive the listener that produced it.
func (l *Listener) track(c *Conn) bool {
	l.muConns.Lock()
	defer l.muConns.Unlock()

	if l.closed {
		return false
	}
	if l.conns == nil {
		l.conns = make(map[*Conn]struct{})
	}
	l.conns[c] = struct{}{}

	return true
}

// registry returns the listener's Application Server registry and NIF state,
// creating them on first use.
//
// Accept calls this before starting the Conn's goroutines, because the fields it
// copies onto the Conn have to be in place before anything can read them: the
// dispatcher reads Conn.as on every state change and Conn.nif on every ASP Up,
// and assigning them after monitor() is running is a data race — which is
// exactly what happened when they were set in track(), since track runs only
// once the association is established.
func (l *Listener) registry() (*applicationServers, *nifAvailability, *destinations) {
	l.muConns.Lock()
	defer l.muConns.Unlock()

	if l.destinations == nil {
		l.destinations = newDestinations()
	}
	if l.as == nil {
		l.as = newApplicationServersWithTrafficModePolicy(
			l.RecoveryTimer, l.Config, l.trafficModePolicy(),
		)
	}
	if l.nif == nil {
		l.nif = &nifAvailability{}
	}
	return l.as, l.nif, l.destinations
}

func newListener(config *Config) *Listener {
	listener := &Listener{Config: config}
	listener.trafficModes.freeze(newTrafficModePolicy(config))
	return listener
}

func (l *Listener) trafficModePolicy() trafficModePolicy {
	if l == nil {
		return trafficModePolicy{}
	}
	return l.trafficModes.get(l.Config)
}

// SetDestinationState records a destination's availability at this SG, for every
// association it serves and every one it will serve.
//
// RFC 4666 Section 4.5.3 has an SG answer a DAUD from what it knows of the SS7
// network, and that knowledge is a property of the node: it does not arrive over
// any ASP's association and does not leave with one. Recording it per Conn meant
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
	if l.Config != nil {
		configured = l.Config.NetworkAppearance
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
	if l.Config != nil {
		configured = l.Config.NetworkAppearance
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
	prepared, err := l.prepareLocalDestinationRange(rangeValue)
	if err != nil {
		return err
	}
	restarts := &l.mtp3Restarts
	restarts.procedureMu.RLock()
	defer restarts.procedureMu.RUnlock()
	l.muConns.Lock()
	closed := l.closed
	l.muConns.Unlock()
	if closed {
		return ErrConnClosed
	}
	if l.stageAnyMTP3RestartRangeLocked(prepared) {
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
	if l.Config == nil || l.Config.RoutingContexts == nil {
		return scope
	}
	configured := l.Config.RoutingContexts.RoutingContexts()
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
	if l.Config != nil {
		configured = l.Config.NetworkAppearance
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
	if l.Config != nil {
		configured = l.Config.NetworkAppearance
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

// forget drops a Conn from the listener's set, so a long-lived listener does
// not accumulate every association it has ever accepted.
func (l *Listener) forget(c *Conn) {
	l.muConns.Lock()
	as := l.as
	delete(l.conns, c)
	l.muConns.Unlock()

	// An association that has gone is no longer an ASP of any Application
	// Server, and its departure may be what takes the AS out of AS-ACTIVE.
	if as != nil {
		as.forget(c)
	}
}

// Listen returns a M3UA listener.
func Listen(net string, laddr *sctp.SCTPAddr, cfg *Config) (*Listener, error) {
	var err error
	l := newListener(cfg)

	n, ok := netMap[net]
	if !ok {
		return nil, fmt.Errorf("invalid network: %s", net)
	}

	// Through SocketConfig rather than ListenSCTP so a notification handler can
	// be installed. The dependency fixes a listener's handler at construction
	// and gives the same one to every association it accepts, so the handler
	// routes by association ID; see restartWatcher.
	l.restarts = &restartWatcher{}
	l.restarts.setRoute(l.connForAssoc)
	scfg := &sctp.SocketConfig{NotificationHandler: l.restarts.handle}

	l.sctpListener, err = scfg.Listen(n, laddr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen SCTP: %w", err)
	}
	return l, nil
}

// connForAssoc finds the accepted Conn owning an association, or nil.
//
// Linear over the tracked associations rather than a second map keyed by ID:
// the set is the ASPs a single SGP serves, it is walked only when the kernel
// reports an association event, and a second index would be one more thing to
// keep in step with track and forget.
func (l *Listener) connForAssoc(id sctp.SCTPAssocID) *Conn {
	l.muConns.Lock()
	defer l.muConns.Unlock()

	for c := range l.conns {
		if c.assocID.Load() == int32(id) {
			return c
		}
	}
	return nil
}

// Accept waits for and returns the next connection to the listener.
// After successfully establishing the association with peer, Payload can be read with Read() func.
// Other signals are automatically handled background in another goroutine.
//
// Accept does not return until the M3UA handshake for that peer has completed,
// or until it gives up on it after ten seconds. A single accept loop therefore
// serves peers strictly one at a time, and one silent peer holds up every other
// ASP waiting behind it for the whole of that budget.
//
// Accept is safe for concurrent use, so a server expecting several ASPs should
// run several Accepts rather than one loop:
//
//	for i := 0; i < concurrency; i++ {
//		go func() {
//			for {
//				conn, err := l.Accept(ctx)
//				if err != nil {
//					return
//				}
//				go serve(conn)
//			}
//		}()
//	}
//
// Nothing in Accept writes to the shared Config, and each accepted Conn owns its
// own association; TestConcurrentAcceptsAreIndependent covers this.
//
// Cancelling ctx does not interrupt an Accept that is blocked waiting for a peer
// to connect — only Close does. Once a peer has connected, ctx bounds the
// handshake, alongside Config.EstablishTimeout.
func (l *Listener) Accept(ctx context.Context) (*Conn, error) {

	// The association is accepted before the Conn is built, so nothing has to be
	// unwound if the accept itself fails.
	c, err := l.sctpListener.AcceptSCTP()
	if err != nil {
		return nil, err
	}

	// Every Conn this Listener produces shares l.Config, so this function must
	// treat it as read-only. The association and the settings derived from it
	// live on the Conn: while they lived on the Config, this Accept would
	// rebind the previously accepted Conn's socket to the association just
	// taken, and that Conn would go on to serve the wrong ASP.
	conn := newConnWithTrafficModePolicy(modeServer, l.Config, l.trafficModePolicy())
	conn.sctpConn = c
	// Set at construction, before any goroutine can observe this Conn, so
	// the field is immutable for its lifetime: Close reads it concurrently
	// with Accept, and assigning it later is a data race. The same applies to
	// the Application Server registry and the NIF state, which the dispatcher
	// reads as soon as monitor() starts.
	conn.listener = l
	conn.as, conn.nif, conn.destinations = l.registry()

	if err := conn.setUpSocket(); err != nil {
		return nil, err
	}

	// The opening ASP-DOWN transition is applied by monitor() itself, before it
	// starts dispatching, rather than published from a goroutine here that
	// raced the reader for it.
	go conn.monitor(ctx)
	establishTimeout := l.Config.EstablishTimeout
	if establishTimeout <= 0 {
		establishTimeout = DefaultEstablishTimeout
	}

	select {
	case <-conn.established:
		// Register only once established: a Conn that never came up is closed
		// on the failure paths below and has nothing for Close to do.
		if !l.track(conn) {
			// The listener closed while this association was coming up, so it
			// would never be shut down by anything else.
			_ = conn.closeWith(ErrFailedToEstablish)
			return nil, ErrFailedToEstablish
		}
		return conn, nil
	case <-conn.done:
		if err := conn.Err(); err != nil {
			return nil, err
		}
		return nil, ErrFailedToEstablish
	case <-ctx.Done():
		_ = conn.closeWith(ctx.Err())
		return nil, ctx.Err()
	case <-time.After(establishTimeout):
		_ = conn.closeWith(ErrTimeout)
		return nil, ErrTimeout
	}
}

// Close closes the listener.
func (l *Listener) Close() error {
	// Take the accepted associations down with the listener. Closing only the
	// SCTP listener left every Conn it had produced running: their monitor and
	// reader goroutines carried on against peers that had no idea the service
	// was gone, so a server shutdown leaked one set of goroutines per client
	// and left half-open associations behind.
	l.mtp3Restarts.procedureMu.Lock()
	l.muConns.Lock()
	if l.closed {
		l.muConns.Unlock()
		l.mtp3Restarts.procedureMu.Unlock()
		return nil
	}
	l.closed = true
	as := l.as
	conns := make([]*Conn, 0, len(l.conns))
	for c := range l.conns {
		conns = append(conns, c)
	}
	l.conns = nil
	l.muConns.Unlock()
	l.mtp3Restarts.mu.Lock()
	l.mtp3Restarts.active = nil
	l.mtp3Restarts.mu.Unlock()
	l.mtp3Restarts.procedureMu.Unlock()
	if as != nil {
		as.close()
	}

	var firstErr error
	if l.sctpListener != nil {
		if err := l.sctpListener.Close(); err != nil {
			firstErr = err
		}
	}

	// Conn.Close is idempotent. The listening socket is already closed, so no
	// new association can enter while the existing set is being released.
	for _, c := range conns {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

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
func (l *Listener) ActiveASPs(rtCtx uint32) []*Conn {
	as := l.applicationServers()
	if as == nil {
		return nil
	}
	return as.get(rtCtx).activeASPs()
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
func (l *Listener) ASPsForTraffic(rtCtx uint32, sls uint8) []*Conn {
	registry := l.applicationServers()
	if registry == nil {
		return nil
	}
	as := registry.get(rtCtx)
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
		return []*Conn{active[int(sls)%len(active)]}
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
// without it, error code 0x0d could never be sent at all.
func (l *Listener) SetNIFAvailable(available bool) {
	_, nif, _ := l.registry()

	l.muConns.Lock()
	conns := make([]*Conn, 0, len(l.conns))
	for c := range l.conns {
		conns = append(conns, c)
	}
	l.muConns.Unlock()

	nif.setIsolated(!available)
	if available {
		return
	}

	// "the SGP should send ASP Down Ack to all its connected ASPs".
	var isolated sync.WaitGroup
	isolated.Add(len(conns))
	for _, c := range conns {
		go func(c *Conn) {
			defer isolated.Done()
			isolateNIFConnection(c)
		}(c)
	}
	isolated.Wait()
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
func (l *Listener) SetASAvailable(rtCtx uint32, available bool) {
	as, nif, _ := l.registry()

	l.muConns.Lock()
	conns := make([]*Conn, 0, len(l.conns))
	for c := range l.conns {
		conns = append(conns, c)
	}
	l.muConns.Unlock()

	nif.setASAvailable(rtCtx, available)
	if available {
		return
	}

	// "the SGP should send ASP Inactive Ack to all its connected ASPs for the
	// affected AS" — only those serving it.
	var isolated sync.WaitGroup
	for _, c := range conns {
		for _, rc := range c.configuredRoutingContexts() {
			if rc != rtCtx {
				continue
			}
			isolated.Add(1)
			go func(c *Conn) {
				defer isolated.Done()
				isolateApplicationServerConnection(c, as, rtCtx)
			}(c)
			break
		}
	}
	isolated.Wait()
}

func isolateNIFConnection(c *Conn) {
	if c == nil {
		return
	}
	c.commitState(StateAspDown)
	postAckNotify := func() {}
	if c.as != nil {
		postAckNotify = c.as.quiesceASPDown(c)
	}
	c.quiesceUnscopedTraffic()
	_ = c.writeMandatoryControls([]messages.M3UA{messages.NewAspDownAck(nil)}, false, true)
	postAckNotify()
	c.sendState(StateAspDown)
}

func isolateApplicationServerConnection(c *Conn, as *applicationServers, rtCtx uint32) {
	if c == nil {
		return
	}
	postAckNotify := func() {}
	if c.State() == StateAspActive {
		c.noteRoutingContextsInactive([]uint32{rtCtx})
		if as != nil {
			postAckNotify = as.quiesceASPFor(c, []uint32{rtCtx})
		}
	}
	c.quiesceUnscopedTraffic()
	_ = c.writeMandatoryControls([]messages.M3UA{
		messages.NewAspInactiveAck(params.NewRoutingContext(rtCtx), nil),
	}, false, true)
	postAckNotify()
	if c.stateForActiveRoutingContexts() == StateAspInactive {
		c.sendState(StateAspInactive)
	}
}
