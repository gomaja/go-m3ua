// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"sort"
	"sync"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// ASState is the state of an Application Server, as distinct from the state of
// any one ASP serving it.
//
// RFC 4666 Section 4.3.2: "The state of the AS is maintained in the M3UA layer
// on the SGPs.  The state of an AS changes due to events.  These events
// include: * ASP state transitions * Recovery timer triggers".
type ASState uint8

// The Application Server states of RFC 4666 Section 4.3.2.
const (
	// ASDown is "The Application Server is unavailable.  This state implies
	// that all related ASPs are in ASP-DOWN state for this AS.  Initially the
	// AS will be in this state."
	ASDown ASState = iota
	// ASInactive is "The Application Server is available, but no application
	// traffic is active.  One or more related ASPs are in ASP-INACTIVE state,
	// and/or the number of related ASPs in ASP-ACTIVE state has not reached n".
	ASInactive
	// ASActive is "The Application Server is available and application traffic
	// is active."
	ASActive
	// ASPending is "An active ASP has transitioned to ASP-INACTIVE or ASP DOWN
	// and it was the last remaining active ASP in the AS.  A recovery timer
	// T(r) SHOULD be started, and all incoming signalling messages SHOULD be
	// queued by the SGP."
	ASPending
)

func (s ASState) String() string {
	switch s {
	case ASDown:
		return "AS-DOWN"
	case ASInactive:
		return "AS-INACTIVE"
	case ASActive:
		return "AS-ACTIVE"
	case ASPending:
		return "AS-PENDING"
	default:
		return "unknown"
	}
}

// statusInformation maps an AS state onto the Status Information value a Notify
// carries for it, from the table in RFC 4666 Section 3.8.2:
//
//	1    Reserved
//	2    Application Server Inactive (AS-INACTIVE)
//	3    Application Server Active (AS-ACTIVE)
//	4    Application Server Pending (AS-PENDING)
//
// AS-DOWN has no value in that table, so it produces no Notify: there would in
// any case be nobody to send it to, since every ASP in the AS is ASP-DOWN and
// Section 4.3.4.5 excludes those.
//
// The params constants pack the Status Type into the same word as the Status
// Information, which is the shape NewStatus takes.
func (s ASState) statusInformation() (uint32, bool) {
	switch s {
	case ASInactive:
		return params.AsStateInactive, true
	case ASActive:
		return params.AsStateActive, true
	case ASPending:
		return params.AsStatePending, true
	default:
		return 0, false
	}
}

// DefaultRecoveryTimer is the default for T(r), the AS-PENDING recovery timer
// of RFC 4666 Section 4.3.2.
//
// The RFC gives no value for T(r) as it does for T(ack), leaving it to
// configuration; two seconds matches T(ack)'s default and keeps a failed
// changeover short.
const DefaultRecoveryTimer = 2 * time.Second

// applicationServer is one Application Server: the set of ASPs serving a
// Routing Context, and the AS state their individual states add up to.
//
// It exists because the AS state machine of RFC 4666 Section 4.3.2 is a
// property of the group rather than of any one association. Nothing about "the
// last remaining active ASP in the AS", about Override's "previously active ASP
// in the AS", or about a Notify going "to all ASPs in the AS" can be decided
// from inside a single Conn.
type applicationServer struct {
	// key is the Network Appearance plus Routing Context scope this AS serves.
	key ASKey

	// deliveryMu orders DATA messages within this AS. State changes use mu
	// independently, so a peer that stops reading cannot hold teardown behind a
	// blocked socket write; closing that socket is what releases the write.
	deliveryMu sync.Mutex
	mu         sync.Mutex
	// asps is every association that has claimed this Routing Context, and the
	// ASP state each is in.
	asps map[*Conn]State
	// active is the stable distribution order derived only when membership
	// changes, rather than rebuilt and sorted for every DATA message.
	active              []*Conn
	connectionOrder     map[*Conn]uint64
	nextConnectionOrder uint64
	// state is the AS state last computed from asps.
	state  ASState
	closed bool
	// notifications records every AS transition in state order. Each event is
	// released only after its related Ack; a later transition can become ready
	// without overtaking an earlier Ack-gated event.
	notifications      []*asStateNotification
	notificationWorker bool
	// trafficMode is the mode in force, which decides how many ASPs must be
	// active and whether a new one overrides the rest.
	trafficMode uint32
	// recovery is T(r), running only while AS-PENDING.
	recovery *time.Timer
	// recoveryFor guards against a late timer firing after the AS has already
	// left AS-PENDING.
	recoveryGen uint64
	// recoveryExpiredGen is the newest T(r) generation whose retained suffix
	// was discarded. An in-flight write from an older generation must not put
	// that DATA back after a later activation.
	recoveryExpiredGen uint64
	// recoveryQueue owns DATA accepted while AS-PENDING, in arrival order.
	recoveryQueue      []queuedData
	recoveryQueueBytes int
	// deliveryInFlightBytes accounts for either the ordinary active send or the
	// recovery item currently on the wire. Counting it against the same limits
	// prevents a blocked ASP from accumulating an unbounded set of waiting
	// DistributeData callers outside the FIFO.
	deliveryInFlightBytes int
	activeSending         bool
	// draining keeps new DATA behind the recovery backlog after the AS becomes
	// active, while a dedicated goroutine performs potentially blocking writes.
	draining bool
	// drainRetry restarts a retained active backlog after a transient write
	// failure without requiring fresh traffic to wake it.
	drainRetry      *time.Timer
	drainRetryDelay time.Duration
	// broadcastEpoch advances whenever another ASP becomes active in a
	// Broadcast AS. broadcastTagged records which independently sequenced
	// traffic flows have received the mandatory first-message marker.
	broadcastEpoch  uint64
	broadcastTagged map[broadcastFlowKey]uint64
	// broadcastFlowLimit bounds broadcastTagged. Clearing at the limit is safe:
	// it may add synchronization markers, but cannot omit a mandatory one.
	broadcastFlowLimit int
	// nextCorrelationID allocates identifiers unique within this AS until the
	// 32-bit wire field necessarily wraps.
	nextCorrelationID uint32
	// recoveryBudget is shared by every AS owned by the Listener.
	recoveryBudget *recoveryBudget
}

type recoveryBudget struct {
	mu           sync.Mutex
	messageLimit int
	byteLimit    int
	messages     int
	bytes        int
}

func newRecoveryBudget(config *Config) *recoveryBudget {
	budget := &recoveryBudget{
		messageLimit: DefaultRecoveryQueueTotalMessages,
		byteLimit:    DefaultRecoveryQueueTotalBytes,
	}
	if config == nil {
		return budget
	}
	if config.RecoveryQueueTotalMessages > 0 {
		budget.messageLimit = config.RecoveryQueueTotalMessages
	}
	if config.RecoveryQueueTotalBytes > 0 {
		budget.byteLimit = config.RecoveryQueueTotalBytes
	}
	return budget
}

func (b *recoveryBudget) claim(size int) bool {
	if b == nil {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.messages >= b.messageLimit || size > b.byteLimit-b.bytes {
		return false
	}
	b.messages++
	b.bytes += size
	return true
}

func (b *recoveryBudget) release(messages, bytes int) {
	if b == nil || messages == 0 {
		return
	}
	b.mu.Lock()
	b.messages -= messages
	b.bytes -= bytes
	if b.messages < 0 || b.bytes < 0 {
		b.mu.Unlock()
		panic("m3ua: recovery budget accounting underflow")
	}
	b.mu.Unlock()
}

type queuedData struct {
	data                *messages.Data
	flow                broadcastFlowKey
	size                int
	recoveryGen         uint64
	broadcastTargets    []*Conn
	broadcastTargetsSet bool
	broadcastEpoch      uint64
	broadcastMarker     bool
}

type asStateNotification struct {
	state         ASState
	targets       []*Conn
	key           ASKey
	aspIdentifier *params.Param
	ready         chan struct{}
	done          chan struct{}
	releaseOnce   sync.Once
	waitOnRelease bool
}

// applicationServers is the registry a Listener keeps, one entry per
// Application Server traffic scope it serves.
type applicationServers struct {
	mu     sync.Mutex
	as     map[ASKey]*applicationServer
	closed bool
	// aspIdentifiers is the identifier each association supplied in ASP Up.
	// Uniqueness is required only among ASPs that support a common AS.
	aspIdentifiers map[*Conn]uint32
	// recoveryTimer is T(r); zero means DefaultRecoveryTimer.
	recoveryTimer time.Duration
	// defaultNetworkAppearance is used only by legacy Routing Context-only
	// accessors. Exact ASKey callers do not consult it.
	defaultNetworkAppearance *params.Param
	// distribution is immutable after construction, so DistributeData never
	// races an application mutating its Config after the Listener starts.
	distribution distributionPolicy
	// trafficModes is the listener-wide traffic-handling policy captured with
	// the registry. ASP Active messages are processed concurrently, so agreement
	// must not consult the caller-owned Config map or Param after construction.
	trafficModes trafficModePolicy
	// recoveryBudget bounds retained DATA across the entire Listener, in
	// addition to each AS's own distributionPolicy limits.
	recoveryBudget *recoveryBudget
}

type activeSSNMTarget struct {
	connection      *Conn
	routingContexts []uint32
}

func newApplicationServers(recovery time.Duration, configs ...*Config) *applicationServers {
	var config *Config
	if len(configs) > 0 {
		config = configs[0]
	}
	return newApplicationServersWithTrafficModePolicy(
		recovery, config, newTrafficModePolicy(config),
	)
}

func newApplicationServersWithTrafficModePolicy(
	recovery time.Duration,
	config *Config,
	trafficModes trafficModePolicy,
) *applicationServers {
	if recovery <= 0 {
		recovery = DefaultRecoveryTimer
	}
	budget := newRecoveryBudget(config)
	return &applicationServers{
		as:             make(map[ASKey]*applicationServer),
		aspIdentifiers: make(map[*Conn]uint32),
		recoveryTimer:  recovery,
		defaultNetworkAppearance: func() *params.Param {
			if config == nil {
				return nil
			}
			return config.NetworkAppearance.Copy()
		}(),
		distribution:   newDistributionPolicy(config),
		trafficModes:   trafficModes,
		recoveryBudget: budget,
	}
}

// claimASPIdentifier atomically saves an ASP's identifier if no other ASP in
// any shared Application Server already owns it.
func (r *applicationServers) claimASPIdentifier(conn *Conn, identifier uint32) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return false
	}
	if r.aspIdentifiers == nil {
		r.aspIdentifiers = make(map[*Conn]uint32)
	}
	for peer, peerIdentifier := range r.aspIdentifiers {
		if peer != conn && peerIdentifier == identifier && connsShareApplicationServer(conn, peer) {
			return false
		}
	}
	r.aspIdentifiers[conn] = identifier
	return true
}

func connsShareApplicationServer(first, second *Conn) bool {
	if first == nil || second == nil {
		return true
	}
	firstKeys := first.configuredASKeys()
	secondKeys := second.configuredASKeys()
	if len(firstKeys) == 0 || len(secondKeys) == 0 {
		return true
	}
	set := make(map[ASKey]struct{}, len(firstKeys))
	for _, key := range firstKeys {
		set[key] = struct{}{}
	}
	for _, key := range secondKeys {
		if _, ok := set[key]; ok {
			return true
		}
	}
	return false
}

// get returns the AS for a traffic scope, creating it if this is the first ASP
// to name it.
func (r *applicationServers) get(scope any) *applicationServer {
	key := r.normalizeASKey(scope)
	legacyRoutingContext, legacy := legacyRoutingContextScope(scope)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		if as, ok := r.as[key]; ok {
			return as
		}
		return &applicationServer{key: key, asps: make(map[*Conn]State), state: ASDown, closed: true}
	}
	if r.as == nil {
		r.as = make(map[ASKey]*applicationServer)
	}
	if legacy {
		if _, as, ok, ambiguous := r.lookupRoutingContextLocked(legacyRoutingContext); ok && !ambiguous {
			return as
		}
	}
	as, ok := r.as[key]
	if !ok {
		as = &applicationServer{
			key:                key,
			asps:               make(map[*Conn]State),
			broadcastFlowLimit: r.distribution.broadcastFlowCacheEntries,
			recoveryBudget:     r.recoveryBudget,
		}
		r.as[key] = as
	}
	return as
}

// lookup returns an existing AS without creating one. Local distribution uses it
// so an invalid traffic scope cannot grow the registry merely by being queried.
func (r *applicationServers) lookup(scope any) (*applicationServer, bool) {
	key := r.normalizeASKey(scope)
	legacyRoutingContext, legacy := legacyRoutingContextScope(scope)
	if r == nil {
		return nil, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, false
	}
	if legacy {
		if _, as, ok, ambiguous := r.lookupRoutingContextLocked(legacyRoutingContext); ok && !ambiguous {
			return as, true
		}
		if _, _, _, ambiguous := r.lookupRoutingContextLocked(legacyRoutingContext); ambiguous {
			return nil, false
		}
	}
	as, ok := r.as[key]
	return as, ok
}

func legacyRoutingContextScope(scope any) (uint32, bool) {
	switch value := scope.(type) {
	case uint32:
		return value, true
	case int:
		return uint32(value), true
	default:
		return 0, false
	}
}

func (r *applicationServers) normalizeASKey(scope any) ASKey {
	switch value := scope.(type) {
	case uint32:
		return r.routingContextASKey(value)
	case int:
		return r.routingContextASKey(uint32(value))
	default:
		return normalizeASKey(scope)
	}
}

func (r *applicationServers) routingContextASKey(routingContext uint32) ASKey {
	key := routingContextASKey(routingContext)
	if r != nil {
		key.NetworkAppearance, key.NetworkAppearanceSet = appearanceOf(r.defaultNetworkAppearance)
	}
	return key
}

func (r *applicationServers) lookupRoutingContext(rtCtx uint32) (ASKey, *applicationServer, bool, bool) {
	if r == nil {
		return ASKey{}, nil, false, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ASKey{}, nil, false, false
	}
	return r.lookupRoutingContextLocked(rtCtx)
}

func (r *applicationServers) lookupRoutingContextLocked(rtCtx uint32) (ASKey, *applicationServer, bool, bool) {
	var foundKey ASKey
	var found *applicationServer
	for key, as := range r.as {
		if !key.RoutingContextSet || key.RoutingContext != rtCtx {
			continue
		}
		if found != nil {
			return ASKey{}, nil, false, true
		}
		foundKey = key
		found = as
	}
	return foundKey, found, found != nil, false
}

// activeSSNMTargets snapshots each currently active association and the ASes
// a destination-state update concerns for it. A Conn serving several ASes is
// returned once with a Routing Context list, so one SS7 event cannot be
// duplicated merely because the ASP is active in several Application Servers.
func (r *applicationServers) activeSSNMTargets(routingContext *uint32) []activeSSNMTarget {
	if r == nil {
		return nil
	}
	type scopedApplicationServer struct {
		key               ASKey
		applicationServer *applicationServer
	}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	applicationServers := make([]scopedApplicationServer, 0, len(r.as))
	if routingContext != nil {
		for key, applicationServer := range r.as {
			if key.RoutingContextSet && key.RoutingContext == *routingContext {
				applicationServers = append(applicationServers, scopedApplicationServer{
					key:               key,
					applicationServer: applicationServer,
				})
			}
		}
	} else {
		for key, applicationServer := range r.as {
			applicationServers = append(applicationServers, scopedApplicationServer{
				key:               key,
				applicationServer: applicationServer,
			})
		}
	}
	r.mu.Unlock()

	sort.Slice(applicationServers, func(i, j int) bool {
		return compareASKey(applicationServers[i].key, applicationServers[j].key) < 0
	})
	targets := make([]activeSSNMTarget, 0)
	targetIndex := make(map[*Conn]int)
	for _, scoped := range applicationServers {
		for _, connection := range scoped.applicationServer.activeASPs() {
			if connection == nil || !connection.activeForASKey(scoped.key) {
				continue
			}
			index, exists := targetIndex[connection]
			if !exists {
				index = len(targets)
				targetIndex[connection] = index
				targets = append(targets, activeSSNMTarget{connection: connection})
			}
			if scoped.key.RoutingContextSet {
				targets[index].routingContexts = append(
					targets[index].routingContexts, scoped.key.RoutingContext,
				)
			}
		}
	}
	return targets
}

// sole returns the only registered AS, when omission of Routing Context is
// unambiguous even without a Config value.
func (r *applicationServers) sole() (ASKey, *applicationServer, bool) {
	if r == nil {
		return ASKey{}, nil, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ASKey{}, nil, false
	}
	if len(r.as) != 1 {
		return ASKey{}, nil, false
	}
	for key, as := range r.as {
		return key, as, true
	}
	return ASKey{}, nil, false
}

// close ends every AS-owned timer and releases all retained traffic. The
// Listener owns the registry, so none of this state may outlive Listener.Close.
func (r *applicationServers) close() {
	if r == nil {
		return
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	all := make([]*applicationServer, 0, len(r.as))
	for _, applicationServer := range r.as {
		all = append(all, applicationServer)
	}
	r.aspIdentifiers = nil
	r.mu.Unlock()

	for _, applicationServer := range all {
		applicationServer.close()
	}
}

// forget removes a Conn from every AS it belonged to, recomputing each.
func (r *applicationServers) forget(c *Conn) {
	r.mu.Lock()
	if r.closed {
		delete(r.aspIdentifiers, c)
		r.mu.Unlock()
		return
	}
	all := make([]*applicationServer, 0, len(r.as))
	for _, as := range r.as {
		all = append(all, as)
	}
	delete(r.aspIdentifiers, c)
	recovery := r.recoveryTimer
	r.mu.Unlock()

	for _, as := range all {
		as.remove(c, recovery)
	}
}

// restrictASP removes an accepted association from every listener-wide AS its
// immutable ASP Up authorization did not grant. Leaving DOWN placeholders in
// those ASes would still leak their state Notify messages to another tenant.
func (r *applicationServers) restrictASP(c *Conn) {
	if r == nil || c == nil {
		return
	}
	allowed := make(map[ASKey]struct{})
	for _, key := range c.configuredASKeys() {
		allowed[key] = struct{}{}
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	unauthorized := make([]*applicationServer, 0)
	for key, applicationServer := range r.as {
		if _, ok := allowed[key]; !ok {
			unauthorized = append(unauthorized, applicationServer)
		}
	}
	recovery := r.recoveryTimer
	r.mu.Unlock()
	for _, applicationServer := range unauthorized {
		applicationServer.remove(c, recovery)
	}
}

func (r *applicationServers) refreshASPOrdering(c *Conn) {
	if r == nil || c == nil {
		return
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	servers := make([]*applicationServer, 0, len(r.as))
	for _, applicationServer := range r.as {
		servers = append(servers, applicationServer)
	}
	r.mu.Unlock()
	for _, applicationServer := range servers {
		applicationServer.mu.Lock()
		if _, member := applicationServer.asps[c]; member {
			applicationServer.rebuildActiveLocked()
		}
		applicationServer.mu.Unlock()
	}
}

// aspStateChanged records an ASP's new state in every AS it serves and emits
// whatever Notify messages the change calls for.
func (r *applicationServers) aspStateChanged(c *Conn, st State) {
	r.aspStateChangedFrom(c, st, false)
}

// aspStateChangedPublished records a monitor publication only while that
// publication is still the association's committed state. The check is held
// across each AS mutation, so ASP Down cannot race an older ASP Active tail and
// leave a membership active after its Ack.
func (r *applicationServers) aspStateChangedPublished(c *Conn, st State) {
	r.aspStateChangedFrom(c, st, true)
}

func (r *applicationServers) aspStateChangedFrom(c *Conn, st State, published bool) {
	if c == nil {
		return
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	recovery := r.recoveryTimer
	r.mu.Unlock()

	for _, key := range c.configuredASKeys() {
		// RFC 4666 Section 4.3.1: "The state of each remote ASP/IPSP, in each AS
		// that it is configured to operate, is maintained in the peer M3UA
		// layer", and Figure 3 is "ASP State Transition Diagram, per AS". An ASP
		// Active naming a subset of this association's Routing Contexts makes
		// the ASP active in those Application Servers only; in the others it is
		// still ASP-INACTIVE, and the same section says such an ASP "SHOULD NOT
		// be sent any DATA or SSNM messages for the AS for which the ASP/IPSP is
		// inactive".
		//
		// One association-wide value was written into every AS, so an ASP that
		// activated for one Routing Context was recorded active in all of them,
		// and ASPsForTraffic then handed it traffic for Application Servers it
		// had never asked to serve.
		//
		// Only ASP-ACTIVE is narrowed: ASP-DOWN and ASP-INACTIVE are properties
		// of the association -- Figure 3 reaches ASP-DOWN by ASP Down or SCTP
		// CDI, neither of which is per-AS -- so they apply everywhere.
		state := st
		if st == StateAspActive && !c.activeForASKey(key) {
			state = StateAspInactive
		}
		if published {
			r.get(key).setASPStateIfAssociationState(c, st, state, recovery)
		} else {
			r.get(key).setASPState(c, state, recovery)
		}
	}
}

// agreeTrafficMode validates and records the mode for every acknowledged AS
// before ASP Active Ack is sent. All AS locks are held in Routing Context order,
// so simultaneous first activations with conflicting modes cannot both win.
func (r *applicationServers) agreeTrafficMode(rtCtxs []uint32, requested *params.Param) (*params.Param, error) {
	keys := make([]ASKey, 0, len(rtCtxs))
	seen := make(map[ASKey]struct{}, len(rtCtxs))
	for _, rtCtx := range rtCtxs {
		key := routingContextASKey(rtCtx)
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	return r.agreeTrafficModeForKeys(keys, r.trafficModes, requested)
}

func (r *applicationServers) agreeTrafficModeForConn(c *Conn, rtCtxs []uint32, requested *params.Param) (*params.Param, error) {
	keys := c.asKeysForRoutingContexts(rtCtxs)
	return r.agreeTrafficModeForKeys(keys, c.trafficModePolicy(), requested)
}

func (r *applicationServers) agreeTrafficModeForKeys(keys []ASKey, trafficModes trafficModePolicy, requested *params.Param) (*params.Param, error) {
	sort.Slice(keys, func(i, j int) bool { return compareASKey(keys[i], keys[j]) < 0 })
	servers := make([]*applicationServer, 0, len(keys))
	for _, key := range keys {
		servers = append(servers, r.get(key))
	}
	for _, applicationServer := range servers {
		applicationServer.mu.Lock()
	}
	defer func() {
		for index := len(servers) - 1; index >= 0; index-- {
			servers[index].mu.Unlock()
		}
	}()

	requestedMode := uint32(0)
	requestedSet := requested != nil
	if requestedSet {
		requestedMode = requested.TrafficModeType()
	}
	agreedModes := make([]uint32, len(servers))
	for index, applicationServer := range servers {
		if applicationServer.closed {
			return nil, ErrConnClosed
		}
		configuredMode, configured := trafficModes.defaultMode, trafficModes.defaultModeSet
		if applicationServer.key.RoutingContextSet {
			configuredMode, configured = trafficModes.configured(applicationServer.key.RoutingContext)
		}
		if configured && !validTrafficMode(configuredMode) {
			return nil, ErrUnsupportedTrafficMode
		}
		if configured && requestedSet && configuredMode != requestedMode {
			return nil, ErrUnsupportedTrafficMode
		}
		desired := requestedMode
		if configured {
			desired = configuredMode
		}
		if desired == 0 {
			desired = applicationServer.trafficMode
		}
		if applicationServer.trafficMode != 0 && desired != 0 && applicationServer.trafficMode != desired {
			return nil, ErrUnsupportedTrafficMode
		}
		agreedModes[index] = desired
	}
	for index, applicationServer := range servers {
		if applicationServer.trafficMode == 0 {
			applicationServer.trafficMode = agreedModes[index]
		}
	}

	if requestedSet {
		return params.NewTrafficModeType(requestedMode), nil
	}
	if len(agreedModes) == 0 || agreedModes[0] == 0 {
		return nil, nil
	}
	for _, mode := range agreedModes[1:] {
		if mode != agreedModes[0] {
			return nil, nil
		}
	}
	return params.NewTrafficModeType(agreedModes[0]), nil
}

func validTrafficMode(mode uint32) bool {
	switch mode {
	case params.TrafficModeOverride, params.TrafficModeLoadshare, params.TrafficModeBroadcast:
		return true
	default:
		return false
	}
}

// quiesceASPFor removes an ASP from every named AS before waiting for any
// already-snapshotted DATA write to finish. The returned closure emits state
// Notify messages and must run only after the related ASP Inactive Ack.
func (r *applicationServers) quiesceASPFor(c *Conn, rtCtxs []uint32) func() {
	if r == nil || c == nil || len(rtCtxs) == 0 {
		return func() {}
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return func() {}
	}
	recovery := r.recoveryTimer
	r.mu.Unlock()

	type transition struct {
		applicationServer *applicationServer
		notify            func()
	}
	keys := c.asKeysForRoutingContexts(rtCtxs)
	seen := make(map[ASKey]struct{}, len(keys))
	transitions := make([]transition, 0, len(keys))
	for _, key := range keys {
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		seen[key] = struct{}{}
		applicationServer := r.get(key)
		transitions = append(transitions, transition{
			applicationServer: applicationServer,
			notify:            applicationServer.markASPsInactive([]*Conn{c}, recovery),
		})
	}

	// Every AS is already marked before the first wait, so a DATA call that
	// starts now cannot select this ASP in another named Routing Context.
	for _, transition := range transitions {
		waitForTrafficBarrier(&transition.applicationServer.deliveryMu)
	}
	return func() {
		for _, transition := range transitions {
			transition.notify()
		}
	}
}

func waitForTrafficBarrier(mu *sync.Mutex) {
	mu.Lock()
	defer mu.Unlock()
}

// quiesceASPDown marks an ASP down in every Application Server it belongs to,
// then waits for DATA that selected it before that mark to finish. The returned
// closure releases AS-state Notify messages and must run only after ASP Down Ack.
func (r *applicationServers) quiesceASPDown(c *Conn) func() {
	if r == nil || c == nil {
		return func() {}
	}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return func() {}
	}
	recovery := r.recoveryTimer
	servers := make([]*applicationServer, 0, len(r.as))
	for _, applicationServer := range r.as {
		servers = append(servers, applicationServer)
	}
	r.mu.Unlock()

	type transition struct {
		applicationServer *applicationServer
		notify            func()
	}
	transitions := make([]transition, 0, len(servers))
	for _, applicationServer := range servers {
		notify, member := applicationServer.markASPDown(c, recovery)
		if !member {
			continue
		}
		transitions = append(transitions, transition{
			applicationServer: applicationServer,
			notify:            notify,
		})
	}

	// All memberships are down before the first wait, so no other AS can select
	// the ASP while a blocked write in an earlier AS is being drained.
	for _, transition := range transitions {
		waitForTrafficBarrier(&transition.applicationServer.deliveryMu)
	}
	return func() {
		for _, transition := range transitions {
			transition.notify()
		}
	}
}

// markASPDown updates one existing AS membership without waiting for DATA or
// emitting Notify. ASP Down applies to every AS, but must not create membership
// in an AS this association never belonged to.
func (as *applicationServer) markASPDown(connection *Conn, recovery time.Duration) (func(), bool) {
	as.mu.Lock()
	defer as.mu.Unlock()
	if as.closed {
		return func() {}, false
	}
	current, member := as.asps[connection]
	if !member {
		return func() {}, false
	}
	if current == StateAspDown {
		return func() {}, true
	}
	as.asps[connection] = StateAspDown
	return as.recomputeLocked(recovery, nil), true
}

// markASPsInactive updates membership and derives the AS transition without
// waiting for DATA or emitting Notify. Callers use the split phases to satisfy
// RFC 4666's halt-before-Ack and Ack-before-Notify ordering simultaneously.
func (as *applicationServer) markASPsInactive(connections []*Conn, recovery time.Duration) func() {
	as.mu.Lock()
	defer as.mu.Unlock()
	if as.closed {
		return func() {}
	}
	changed := false
	for _, connection := range connections {
		if current, ok := as.asps[connection]; !ok || current != StateAspInactive {
			as.asps[connection] = StateAspInactive
			changed = true
		}
	}
	if !changed {
		return func() {}
	}
	return as.recomputeLocked(recovery, nil)
}

func (as *applicationServer) close() {
	as.mu.Lock()
	if as.closed {
		as.mu.Unlock()
		return
	}
	as.closed = true
	as.state = ASDown
	as.stopRecoveryLocked()
	as.stopDrainRetryLocked()
	as.recoveryGen++
	as.recoveryExpiredGen = as.recoveryGen
	queuedMessages := len(as.recoveryQueue)
	queuedBytes := as.recoveryQueueBytes - as.deliveryInFlightBytes
	as.recoveryQueue = nil
	as.recoveryQueueBytes = as.deliveryInFlightBytes
	as.draining = false
	as.active = nil
	as.connectionOrder = nil
	for _, event := range as.notifications {
		event.releaseOnce.Do(func() { close(event.ready) })
	}
	as.mu.Unlock()
	as.recoveryBudget.release(queuedMessages, queuedBytes)
}

// activateOverride makes challenger the sole active ASP in one indivisible AS
// transition. Splitting activation, peer discovery, and displacement across
// locks lets simultaneous challengers each select the other and leave the AS
// with nobody active.
func (as *applicationServer) activateOverride(challenger *Conn, recovery time.Duration) ([]*Conn, func(), bool) {
	if challenger == nil {
		return nil, func() {}, false
	}

	as.mu.Lock()
	defer as.mu.Unlock()
	if as.closed {
		return nil, func() {}, false
	}

	changed := as.asps[challenger] != StateAspActive
	as.asps[challenger] = StateAspActive
	displaced := make([]*Conn, 0, len(as.asps)-1)
	for peer, state := range as.asps {
		if peer == challenger || state != StateAspActive {
			continue
		}
		as.asps[peer] = StateAspInactive
		displaced = append(displaced, peer)
		changed = true
	}

	notify := func() {}
	if changed {
		notify = as.recomputeLocked(recovery, nil)
	}
	startDrain := as.state == ASActive && len(as.recoveryQueue) > 0 && !as.draining && !as.activeSending
	if startDrain {
		as.stopDrainRetryLocked()
		as.draining = true
	}
	return displaced, notify, startDrain
}

// remove drops an ASP from the AS and recomputes.
func (as *applicationServer) remove(c *Conn, recovery time.Duration) {
	as.mu.Lock()
	if as.closed {
		as.mu.Unlock()
		return
	}
	failedState, ok := as.asps[c]
	if !ok {
		as.mu.Unlock()
		return
	}
	delete(as.asps, c)
	delete(as.connectionOrder, c)
	failedIdentifier := c.peerASPIdentifierParam()
	var stateIdentifier *params.Param
	if failedState != StateAspDown {
		stateIdentifier = failedIdentifier
	}
	notify := as.recomputeLocked(recovery, stateIdentifier)
	failureTargets := as.notifyTargetsLocked()
	as.mu.Unlock()

	notify()
	if failedState != StateAspDown {
		notifyASPFailure(failureTargets, as.key, failedIdentifier)
	}
}

// setASPState records an ASP's state and recomputes the AS state.
func (as *applicationServer) setASPState(c *Conn, st State, recovery time.Duration) {
	as.setASPStateGuarded(c, st, recovery, 0, false)
}

func (as *applicationServer) setASPStateIfAssociationState(c *Conn, associationState, st State, recovery time.Duration) {
	as.setASPStateGuarded(c, st, recovery, associationState, true)
}

func (as *applicationServer) setASPStateGuarded(c *Conn, st State, recovery time.Duration, associationState State, guard bool) {
	if c == nil {
		return
	}
	if guard {
		c.muState.Lock()
		if c.state != associationState {
			c.muState.Unlock()
			return
		}
	}

	as.mu.Lock()
	if as.closed {
		as.mu.Unlock()
		if guard {
			c.muState.Unlock()
		}
		return
	}
	current, known := as.asps[c]
	if known && current == st {
		as.mu.Unlock()
		if guard {
			c.muState.Unlock()
		}
		return
	}
	as.asps[c] = st
	if st == StateAspActive && (!known || current != StateAspActive) {
		as.noteBroadcastActivationLocked()
	}
	notify := as.recomputeLocked(recovery, nil)
	startDrain := as.state == ASActive && len(as.recoveryQueue) > 0 && !as.draining && !as.activeSending
	if startDrain {
		as.stopDrainRetryLocked()
		as.draining = true
	}
	as.mu.Unlock()
	if guard {
		c.muState.Unlock()
	}

	notify()
	if startDrain {
		go as.drainRecoveryQueue()
	}
}

// setTrafficMode records the mode in force for the AS.
//
// RFC 4666 Section 4.3.4.3: "If the traffic handling mode of the Application
// Server is not already known via configuration data, then the traffic handling
// mode indicated in the first ASP Active message causing the transition of the
// Application Server state to AS-ACTIVE MAY be used to set the mode."
func (as *applicationServer) setTrafficMode(mode uint32) {
	as.mu.Lock()
	defer as.mu.Unlock()
	if as.trafficMode == 0 {
		as.trafficMode = mode
		if mode == params.TrafficModeBroadcast && as.hasActiveASPLocked() {
			as.noteBroadcastActivationLocked()
		}
	}
}

// TrafficMode returns the mode in force for the AS, or 0 if none is known yet.
func (as *applicationServer) TrafficMode() uint32 {
	as.mu.Lock()
	defer as.mu.Unlock()
	return as.trafficMode
}

// requiredActive is n from RFC 4666 Section 4.3.2: "the number of ASPs required
// to be in ASP-ACTIVE state before AS can transition to AS-ACTIVE; n = 1 for
// Override Traffic Mode".
//
// The same section permits one for the other modes too: "When one ASP is
// considered enough to handle traffic (smooth start), the AS in AS-INACTIVE MAY
// reach the AS-ACTIVE as soon as the first ASP moves to the ASP-ACTIVE state."
func (as *applicationServer) requiredActive() int { return 1 }

// recomputeLocked derives the AS state from its ASPs and returns a closure that
// emits the resulting Notify messages.
//
// The emission is deferred out of the lock because writing to a peer can block,
// and holding the AS lock across a socket write would stall every other
// association in the same Application Server.
func (as *applicationServer) recomputeLocked(recovery time.Duration, aspIdentifier *params.Param) func() {
	as.rebuildActiveLocked()
	active, inactive := 0, 0
	for _, st := range as.asps {
		switch st {
		case StateAspActive:
			active++
		case StateAspInactive:
			inactive++
		}
	}

	previous := as.state
	var next ASState

	switch {
	case active >= as.requiredActive():
		next = ASActive
	case previous == ASActive:
		// "An active ASP has transitioned to ASP-INACTIVE or ASP DOWN and it
		// was the last remaining active ASP in the AS.  A recovery timer T(r)
		// SHOULD be started" (Section 4.3.2).
		next = ASPending
	case previous == ASPending:
		// Still pending; T(r) decides.
		next = ASPending
	case inactive > 0:
		next = ASInactive
	default:
		next = ASDown
	}

	if next == previous {
		return func() {}
	}
	as.state = next
	as.stopRecoveryLocked()
	if next == ASPending {
		as.startRecoveryLocked(recovery)
	}
	if next != ASActive {
		as.stopDrainRetryLocked()
	}

	return as.enqueueStateNotificationLocked(next, as.notifyTargetsLocked(), aspIdentifier)
}

// enqueueStateNotificationLocked appends one mandatory AS-state event while mu
// is held. The returned release function is called only after the related Ack.
// A head event waits for delivery to preserve existing synchronous procedure
// ordering; an event queued behind a gated predecessor merely becomes ready, so
// an unrelated association is never blocked on that predecessor's DATA barrier.
func (as *applicationServer) enqueueStateNotificationLocked(state ASState, targets []*Conn, aspIdentifier *params.Param) func() {
	event := &asStateNotification{
		state:         state,
		targets:       append([]*Conn(nil), targets...),
		key:           as.key,
		aspIdentifier: aspIdentifier.Copy(),
		ready:         make(chan struct{}),
		done:          make(chan struct{}),
		waitOnRelease: len(as.notifications) == 0,
	}
	as.notifications = append(as.notifications, event)
	if !as.notificationWorker {
		as.notificationWorker = true
		go as.deliverStateNotifications()
	}
	return func() {
		event.releaseOnce.Do(func() { close(event.ready) })
		if event.waitOnRelease {
			<-event.done
		}
	}
}

func (as *applicationServer) deliverStateNotifications() {
	for {
		as.mu.Lock()
		if len(as.notifications) == 0 {
			as.notificationWorker = false
			as.mu.Unlock()
			return
		}
		event := as.notifications[0]
		as.mu.Unlock()

		<-event.ready
		as.mu.Lock()
		closed := as.closed
		as.mu.Unlock()
		if !closed {
			notifyASState(event.targets, event.state, event.key, event.aspIdentifier)
		}
		close(event.done)

		as.mu.Lock()
		if len(as.notifications) > 0 && as.notifications[0] == event {
			as.notifications[0] = nil
			as.notifications = as.notifications[1:]
		}
		as.mu.Unlock()
	}
}

// notifyTargetsLocked lists the associations a Notify must go to.
//
// RFC 4666 Section 4.3.4.5: "A Notify message reflecting a change in the AS
// state MUST be sent to all ASPs in the AS, except those in the ASP-DOWN
// state".
func (as *applicationServer) notifyTargetsLocked() []*Conn {
	targets := make([]*Conn, 0, len(as.asps))
	for c, st := range as.asps {
		if st == StateAspDown {
			continue
		}
		targets = append(targets, c)
	}
	return targets
}

// startRecoveryLocked starts T(r).
func (as *applicationServer) startRecoveryLocked(recovery time.Duration) {
	as.recoveryGen++
	gen := as.recoveryGen
	for index := range as.recoveryQueue {
		as.recoveryQueue[index].recoveryGen = gen
	}
	as.recovery = time.AfterFunc(recovery, func() { as.recoveryExpired(gen) })
}

func (as *applicationServer) stopRecoveryLocked() {
	if as.recovery != nil {
		as.recovery.Stop()
		as.recovery = nil
	}
}

// recoveryExpired applies the outcome RFC 4666 Section 4.3.2 gives T(r):
//
//	If T(r) expires before an ASP becomes ASP-ACTIVE, and the SGP has no
//	alternative, the SGP may stop queuing messages and discard all
//	previously queued messages.  The AS will move to the AS-INACTIVE
//	state if at least one ASP is in ASP-INACTIVE; otherwise, it will move
//	to AS-DOWN state.
func (as *applicationServer) recoveryExpired(gen uint64) {
	as.mu.Lock()
	if gen != as.recoveryGen || as.state != ASPending {
		// An ASP became active, or the AS moved on, after this timer was armed.
		as.mu.Unlock()
		return
	}
	as.recovery = nil

	inactive := 0
	for _, st := range as.asps {
		if st == StateAspInactive {
			inactive++
		}
	}
	next := ASDown
	if inactive > 0 {
		next = ASInactive
	}
	as.state = next
	as.recoveryExpiredGen = gen
	queuedMessages := len(as.recoveryQueue)
	queuedBytes := as.recoveryQueueBytes - as.deliveryInFlightBytes
	as.recoveryQueue = nil
	as.recoveryQueueBytes = as.deliveryInFlightBytes
	as.draining = false
	as.stopDrainRetryLocked()
	notify := as.enqueueStateNotificationLocked(next, as.notifyTargetsLocked(), nil)
	as.mu.Unlock()
	as.recoveryBudget.release(queuedMessages, queuedBytes)
	notify()
}

// State returns the AS state.
func (as *applicationServer) State() ASState {
	as.mu.Lock()
	defer as.mu.Unlock()
	return as.state
}

// notifyASState sends an AS-State_Change Notify to each target.
//
// RFC 4666 Section 3.8.2 fixes the Status Type: "1  Application Server State
// Change (AS-State_Change)", with the new state in Status Information.
func notifyASState(targets []*Conn, state ASState, key ASKey, aspIdentifier *params.Param) {
	info, ok := state.statusInformation()
	if !ok {
		return
	}

	for _, c := range targets {
		// Best effort: a peer that cannot be told is a problem for that
		// association, not for the Application Server or its other members.
		c.enqueueNotify(messages.NewNotify(
			params.NewStatus(info),
			aspIdentifier.Copy(),
			routingContextParamForASKey(key),
			nil,
		))
	}
}

func notifyASPFailure(targets []*Conn, key ASKey, failedIdentifier *params.Param) {
	for _, target := range targets {
		target.enqueueNotify(messages.NewNotify(
			params.NewStatus(params.AspFailure),
			failedIdentifier.Copy(),
			routingContextParamForASKey(key),
			nil,
		))
	}
}

// notifyAlternateASPActive tells an ASP that another has overridden it.
//
// RFC 4666 Section 4.3.4.3, on an Override mode AS: "Any previously active ASP
// in the AS is now considered to be in the state ASP-INACTIVE and SHOULD no
// longer receive traffic from the SGP within the AS.  The SGP or IPSP then MUST
// send a Notify message ("Alternate ASP_Active") to the previously active ASP
// in the AS and SHOULD stop traffic to/from that ASP."
//
// Status Type is "Other" here rather than AS-State_Change, since Section 3.8.2
// puts "2  Alternate ASP Active" under that type: it reports what another ASP
// did, not a change in the AS's own state.
func notifyAlternateASPActive(target *Conn, key ASKey, overriding *params.Param) {
	target.enqueueNotify(messages.NewNotify(
		params.NewStatus(params.AlternateAspActive),
		// "The ASP Identifier (if available) of the [overriding ASP]" — the
		// receiver needs to know which ASP took over, not its own identity.
		overriding.Copy(),
		routingContextParamForASKey(key),
		nil,
	))
}

// activeASPs returns the associations currently ASP-ACTIVE in the AS, in a
// stable order so a load-sharing choice is repeatable for the same key.
//
// The order is by ASP Identifier where one is configured, falling back to the
// pointer as a tiebreak. Repeatability is what makes SLS-based load-sharing
// preserve MTP3 sequencing: RFC 4666 Section 1.4.7, "Traffic that requires
// sequencing SHOULD be assigned to the same stream", is defeated if consecutive
// messages with one SLS land on different ASPs.
func (as *applicationServer) activeASPs() []*Conn {
	as.mu.Lock()
	defer as.mu.Unlock()

	return as.activeASPsLocked()
}

// rebuildActiveLocked refreshes the stable distribution order after a state
// change. A registry-local sequence is the allocation-free tiebreak when peers
// omit or duplicate ASP Identifier.
func (as *applicationServer) rebuildActiveLocked() {
	if as.connectionOrder == nil {
		as.connectionOrder = make(map[*Conn]uint64, len(as.asps))
	}
	active := make([]*Conn, 0, len(as.asps))
	for connection, state := range as.asps {
		if _, known := as.connectionOrder[connection]; !known {
			as.nextConnectionOrder++
			as.connectionOrder[connection] = as.nextConnectionOrder
		}
		if state == StateAspActive {
			active = append(active, connection)
		}
	}
	sort.Slice(active, func(i, j int) bool {
		a, b := aspIDOf(active[i]), aspIDOf(active[j])
		if a != b {
			return a < b
		}
		return as.connectionOrder[active[i]] < as.connectionOrder[active[j]]
	})
	as.active = active
}

func aspIDOf(c *Conn) uint32 {
	if c == nil {
		return 0
	}
	identifier, _ := c.PeerASPIdentifier()
	return identifier
}

// nifAvailability records whether the SGP can still reach the SS7 network
// through its nodal interworking function, in whole or per Application Server.
//
// RFC 4666 Section 4.7 leaves the NIF itself implementation dependent but gives
// guidance for when it is gone, and the behaviour it describes cannot be
// produced without somewhere to record the isolation.
type nifAvailability struct {
	mu sync.RWMutex
	// isolated is true when the SGP is cut off from the NIF entirely.
	isolated bool
	// unavailable holds the Routing Contexts the SGP can no longer service
	// while it is only partially isolated.
	unavailable map[ASKey]struct{}
}

func (n *nifAvailability) setIsolated(isolated bool) {
	n.mu.Lock()
	n.isolated = isolated
	n.mu.Unlock()
}

func (n *nifAvailability) setASAvailable(rtCtx uint32, available bool) {
	n.setASAvailableForAS(routingContextASKey(rtCtx), available)
}

func (n *nifAvailability) setASAvailableForAS(key ASKey, available bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if available {
		delete(n.unavailable, key)
		return
	}
	if n.unavailable == nil {
		n.unavailable = make(map[ASKey]struct{})
	}
	n.unavailable[key] = struct{}{}
}

// isolatedEntirely reports full isolation from the NIF.
func (n *nifAvailability) isolatedEntirely() bool {
	if n == nil {
		return false
	}
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.isolated
}

func (n *nifAvailability) servicableASKeys(keys []ASKey) bool {
	if n == nil {
		return true
	}
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.isolated {
		return false
	}
	for _, key := range keys {
		if _, bad := n.unavailable[key]; bad {
			return false
		}
	}
	return true
}
