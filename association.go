// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

// M3UAPPID is the SCTP Payload Protocol Identifier IANA assigned to M3UA.
//
// RFC 4666 Section 7.1 says value 3 SHOULD be included in each SCTP DATA chunk.
// The value is in host order here, as expected by sctp.SCTPWrite; that method
// performs the conversion required for the SCTP ancillary data on the wire.
const M3UAPPID uint32 = 3

// Association is one M3UA association and satisfies net.Conn.
type Association struct {
	// sctpConn is the SCTP association this Association owns.
	//
	// It belongs to the Association and not to AssociationConfig, because one
	// AssociationConfig can be shared by every Association a Listener accepts:
	// while it lived on the configuration,
	// each Accept overwrote it, so the second accepted association rebound the
	// first Association's socket. That Association then reported the wrong peer, read the
	// other ASP's traffic, wrote to the other ASP, and closed the other ASP's
	// socket on Close. See multi_asp_test.go.
	sctpConn *sctp.SCTPConn
	// sctpInfo is the SndRcvInfo template for sends on this association. It is
	// per-Association for the same reason as sctpConn, and is copied by value at each
	// send so the stream ID can vary per message.
	//
	// This is the SCTP_SNDRCV control message, which RFC 6458 Section 5.3.2
	// deprecates in favour of SCTP_SNDINFO. The receive side has moved — see
	// SetRecvRcvInfo in setUpSocket — but the send side deliberately has not.
	//
	// SCTPWrite byte-swaps PPID per message and SCTPWriteInfo passes it through
	// untouched, matching SetDefaultSndInfo; the dependency documents the
	// difference on both. Moving would put PPID 3 on the wire as 0x03000000
	// unless this package byte-swapped it itself, which is knowledge of network
	// byte order that the current call does not require of it, for no
	// behavioural gain. The kernel accepts either form, and both can be mixed
	// on one association.
	sctpInfo *sctp.SndRcvInfo
	// lastRecv is when the last successfully parsed M3UA message with PPID 0 or
	// M3UAPPID was received from the peer, in Unix nanoseconds. RFC 4666 Section
	// 4.3.4.6 counts "any other M3UA message" as evidence the peer is alive, so
	// T(beat) consults this rather than treating a missing BEAT Ack as proof of
	// death.
	lastRecv atomic.Int64
	// recvStream is the SCTP stream the message being handled arrived on.
	//
	// It is not sctpInfo.Stream: that is the *send* template, fixed at 0 for
	// this Association's lifetime because every send copies it by value before setting
	// a stream. The receive-side checks in aspsm.go read this instead — they
	// used to read the send template, so they compared 0 against 0 and could
	// never fire. Written by the dispatcher before each handler runs, and read
	// by the handlers on that same goroutine; atomic so a reader elsewhere
	// cannot race the write.
	recvStream atomic.Uint32
	// hb is this Association's resolved M3UA BEAT settings, copied by value from
	// AssociationConfig at construction. It is not SCTP HEARTBEAT configuration.
	//
	// Resolving it per Association keeps Accept from writing Enabled back onto
	// shared AssociationConfig, and makes a configuration built by
	// NewAssociationConfig — which leaves
	// HeartbeatInfo nil — usable instead of a nil dereference.
	hb HeartbeatInfo
	// maxMessageStreamID is the maximum negotiated sctp stream ID used,
	// must not be zero, must vary from 1 to maxMessageStreamID
	maxMessageStreamID uint16
	// muState is to Lock when updating state
	muState *sync.RWMutex
	// role is the immutable RFC 4666 endpoint role on this association.
	role Role
	// aspTransferMu keeps a selected ASP MTP-TRANSFER write ordered before a
	// concurrent loss of the Association or its active Application Server scope.
	aspTransferMu sync.RWMutex
	// state is to see the current state
	state State
	// localIPSPState is the local IPSP's state for traffic directed to this
	// IPSP in the RFC 4666 Double Exchange model. state retains the remote
	// IPSP's state for traffic directed to the peer.
	localIPSPState State
	// appliedState is the state the entry-action pass last ran for.
	//
	// It exists because state is now committed by sendState, on the dispatch
	// goroutine, before the transition is published for its entry actions. That
	// ordering is what stops a peer's next message being judged against a state
	// this end has already decided to leave but not yet recorded. It does mean
	// applyStateUpdate can no longer read the state it is moving away from out
	// of state itself -- by then state holds the new value -- so the previous
	// one is kept here. Guarded by muState, like state.
	appliedState State
	// stateEntered records that at least one state has been published, so the
	// first update is treated as an entry rather than a restatement of the
	// zero value. Guarded by muState, like state itself.
	stateEntered bool
	// resumeTo is how far an ASP climbs when it comes up: ASP-ACTIVE for an
	// ordinary Dial, or ASP-INACTIVE when RFC 4666 Section 4.3.4.2 has it
	// return to a previous state that was only ASP-INACTIVE. Guarded by muState.
	resumeTo State
	// stateChan is to update the state and handle it
	stateChan chan State
	// inboundChan serialises SCTP notifications with M3UA messages in the exact
	// order the association reader observed them. It is deliberately
	// unbuffered in a live Association: the reader cannot move past a restart and hand
	// the dispatcher a later M3UA message before the restart marker has entered
	// the same queue. The inbound value is extensible for further per-message
	// SCTP metadata.
	inboundChan chan inbound
	// established closes when the M3UA association is established.
	established chan struct{}
	// beatAckChan notifies the M3UA BEAT loop that a valid BEAT Ack arrived.
	beatAckChan chan struct{}
	// dataChan passes received DATA (payload plus its network and traffic flow)
	// to the user. Its bounded capacity is resolved from AssociationConfig.DataQueueSize.
	dataChan chan *DataMessage
	// errChan is to pass errors to a goroutine that monitors status
	errChan chan error
	// done is closed once on Close() to signal all goroutines to stop sending on channels
	done chan struct{}
	// closeOnce ensures Close() channel cleanup runs exactly once
	closeOnce sync.Once
	// closeErr records why the association ended, reported by Err(). Written
	// once, inside closeOnce, so the first cause is the one kept: a later Close
	// must not overwrite the reason the association actually died.
	closeErr atomic.Value
	// cfg is a configuration required to communicate between M3UA endpoints
	cfg *AssociationConfig
	// trafficModes is the immutable Traffic Mode policy copied from cfg at
	// construction. Keeping the public AssociationConfig pointer is necessary for the
	// remaining settings, but ASPTM runs concurrently with caller code and must
	// never read a mutable Param or map from it.
	trafficModes          trafficModeSnapshot
	localIPSPTrafficModes trafficModeSnapshot
	peerIPSPTrafficModes  trafficModeSnapshot
	// Dynamic Routing Keys are keyed by Routing Context because one
	// Association may coordinate keys in several Network Appearances.
	muDynamicASKeys          sync.RWMutex
	dynamicPeerASKeys        map[uint32]ASKey
	dynamicLocalASKeys       map[uint32]ASKey
	dynamicPeerTrafficModes  map[uint32]uint32
	dynamicLocalTrafficModes map[uint32]uint32
	// peerASPIdentifier is the identifier an ASP supplied in ASP Up. It is
	// distinct from cfg.ASPIdentifier, which is this endpoint's own optional
	// identifier and is shared by every association a Listener accepts.
	muPeerASPIdentifier sync.RWMutex
	peerASPIdentifier   *params.Param
	// beatStart gates the M3UA BEAT loop until the ASP is ASP-ACTIVE. Closing a
	// channel records the transition permanently, unlike a condition-variable
	// broadcast that can be missed when a fast transition wins the scheduler.
	beatStart     chan struct{}
	beatStartOnce sync.Once
	// muBeat guards beatData: heartbeat() registers it, while the dispatch
	// goroutine's handleHeartbeatAck compares the peer's echo against it.
	muBeat sync.RWMutex
	// beatData is the Heartbeat Data of the BEAT awaiting its Ack; empty
	// when no BEAT is outstanding.
	beatData []byte
	// destinations tracks SS7 destination availability as reported by the peer
	// through SSNM (RFC 4666 Section 4.5).
	// resumeStray records that this Association was pushed out of ASP-ACTIVE by a
	// stray acknowledgement rather than by the peer taking traffic away.
	//
	// RFC 4666 Section 4.3.4.1 asks for a return in that case: "If the ASP
	// receives an unexpected ASP Up Ack message, the ASP should consider itself
	// in the ASP-INACTIVE state.  If the ASP was not in the ASP-INACTIVE state,
	// it SHOULD send an Error message and then initiate procedures to return
	// itself to its previous state." The peer-driven routes into ASP-INACTIVE —
	// an ASP Inactive, an ASP Inactive Ack, an "Alternate ASP Active" Notify —
	// must not resume, or an Override-mode AS ping-pongs between two ASPs, so
	// the two causes are distinguished here rather than by the previous state
	// alone.
	//
	// Atomic rather than guarded by muState: the entry action that reads it
	// runs inside handleStateUpdate, which already holds that lock, and
	// sync.RWMutex is not reentrant.
	resumeStray atomic.Bool
	// terminating suppresses the automatic ASP Up climb while Shutdown is
	// completing the ASP Inactive/ASP Down sequence.
	terminating atomic.Bool

	// ackedRCs is the set of Routing Contexts the peer has acknowledged in an
	// ASP Active Ack.
	//
	// Activation is per Routing Context: Section 4.3.4.3 has the SGP answer
	// "For the Application Servers for which the ASP can be activated", so a
	// partial Ack leaves the rest inactive. Tracking one state for the whole
	// association sent DATA for contexts the SGP had never agreed to carry.
	//
	// muAckedRCs rather than muState: the entry actions that reset this run
	// inside handleStateUpdate, which already holds muState, and sync.RWMutex
	// is not reentrant.
	muAckedRCs sync.RWMutex
	ackedRCs   map[uint32]struct{}
	// ackedRCsScoped distinguishes "no ASP Active Ack scope recorded" from
	// "no Routing Context is active". Both have an empty map, but the former is
	// the compatibility fallback for an association placed directly into
	// ASP-ACTIVE while the latter follows an ASP Inactive Ack covering every AS.
	ackedRCsScoped bool
	// overriddenRCs are the Routing Contexts an alternate ASP has taken over.
	// For IPSP Double Exchange they belong only to TrafficToLocal, alongside
	// ackedRCs; the independent TrafficToPeer inventory remains in activeRCs.
	// Guarded by muAckedRCs, which covers both inventories.
	overriddenRCs map[uint32]struct{}

	// activeRCs are the Routing Contexts this peer has activated, at an SGP.
	//
	// RFC 4666 Section 4.3.1 keeps ASP state per Application Server -- "The
	// state of each remote ASP/IPSP, in each AS that it is configured to
	// operate, is maintained in the peer M3UA layer", and Figure 3 is titled
	// "ASP State Transition Diagram, per AS". An ASP Active naming a subset of
	// the association's Routing Contexts activates the ASP in those Application
	// Servers alone; in the rest it stays ASP-INACTIVE, which the same section
	// says "SHOULD NOT be sent any DATA or SSNM messages for the AS for which
	// the ASP/IPSP is inactive".
	//
	// nil means the whole configured set, which is what an ASP Active naming no
	// context asks for. Guarded by muAckedRCs.
	activeRCs map[uint32]struct{}
	// activeRCsScoped distinguishes "activated for everything" from "activated
	// for nothing", since both leave activeRCs empty.
	activeRCsScoped bool
	// activeScopeInitialized distinguishes the initial compatibility fallback
	// from an ASP Active that explicitly activated every configured AS.
	activeScopeInitialized bool
	// contextlessASActive preserves an active contextless AS when a later
	// scoped procedure changes only Routing-Context-qualified AS state.
	contextlessASActive bool
	// inactiveDynamicRCs excludes Routing Contexts registered after an unscoped
	// ASP Active. RFC 4666 Section 4.3.4.3 requires a subsequent ASP Active
	// procedure before the new Application Server becomes active.
	inactiveDynamicRCs map[uint32]struct{}

	// nif is the SGP's view of its nodal interworking function. It is nil for
	// an ASP role.
	nif *nifAvailability

	// as is the Application Server registry this Association belongs to. It is
	// nil for an ASP and shared by all Associations owned by an SGP Endpoint.
	//
	// The AS state machine of RFC 4666 Section 4.3.2 lives on the SGP and spans
	// every ASP serving a Routing Context, so an Association reports its own ASP state
	// changes into it rather than deciding anything itself.
	as *applicationServers
	// asReservation keeps an accepted SGP or IPSP association's configured AS
	// scopes provisional until M3UA establishment succeeds. A failed handshake
	// rolls them back after Endpoint membership has been removed, so a listener
	// whose selector returns distinct scopes cannot grow the shared registry
	// without bound.
	asReservation *applicationServerReservation
	// unscopedDeliveryMu covers peer-directed DATA on an IPSP and DATA/SSNM on
	// a dedicated association where no Routing Context was coordinated.
	unscopedDeliveryMu sync.Mutex
	// localIPSPSSNMDeliveryMu is the independent Double Exchange barrier for
	// local-directed SCON. RFC 4666 Section 5.6.2 gives DATA and SCON opposite
	// traffic directions, so withdrawing one direction must not wait for the
	// other while still draining messages already admitted in its own direction.
	localIPSPSSNMDeliveryMu sync.Mutex
	// authorizedRCs is the immutable per-peer subset of the listener's Routing
	// Context inventory resolved when ASP Up is received. Before resolution, an
	// SGP retains the all-configured policy; an ASP uses its own
	// AssociationConfig directly.
	muAuthorizedRCs            sync.RWMutex
	authorizedRCs              []uint32
	authorizationResolved      bool
	authorizationExplicit      bool
	authorizationIdentifier    uint32
	authorizationIdentifierSet bool

	// peerCongestion is the congestion level the peer last reported about
	// itself, from a SCON received at an SGP.
	//
	// It is kept apart from destinations because it means something different:
	// RFC 4666 Section 3.4.4's ASP-to-peer SCON reports "the congestion level
	// of the M3UA layer or the ASP", not the reachability of an SS7
	// destination.
	peerCongestion atomic.Uint32

	// selectedRC is the Routing Context outbound DATA names, when the
	// association carries more than one.
	//
	// RFC 4666 Section 3.3.1 makes the parameter singular and says what it is
	// for: "Where multiple Routing Keys and Routing Contexts are used across a
	// common association, the Routing Context MUST be sent to identify the
	// traffic flow". Which flow a payload belongs to is the caller's knowledge,
	// not this package's, so with several configured the caller chooses through
	// SelectRoutingContext. Guarded by muState.
	selectedRC    uint32
	selectedRCSet bool

	destinations *destinations
	// mtp3Restarts coordinates RFC 4666 Section 4.6 restart state for an SGP.
	// It exists for both accepted and initiated SCTP associations so protocol
	// behavior never depends on SCTP association orientation.
	mtp3Restarts *mtp3RestartRegistry
	// tack retransmits unacknowledged ASPSM/ASPTM requests (T(ack)).
	tack *tackRetransmitter
	// listener is present only when the SCTP association was accepted, so Close
	// can deregister it from the Listener that owns the listening socket.
	// Protocol behavior must not branch on this transport-lifecycle link.
	listener *Listener
	// endpoint owns node-wide state and the complete Listener/Association
	// lifecycle independently of SCTP association initiation.
	endpoint *Endpoint
	// readDeadline bounds Read, ReadPD and ReadData, as Unix nanoseconds with
	// zero meaning none. See SetReadDeadline for why it is not pushed down to
	// the SCTP socket.
	readDeadline atomic.Int64
	// dataOverflow records that the inbound DATA queue is full, so the
	// condition is reported on its onset rather than per discarded payload.
	dataOverflow atomic.Bool
	// malformedLogs bounds peer-triggered diagnostic logging per association.
	malformedLogs malformedLogLimiter
	// statusChan delivers SSNM destination state changes to the user. It is
	// buffered; an overflow is represented by a ResyncRequired status.
	statusChan chan *DestinationStatus
	// muStatus guards statusChan's closure. Close closes the channel so a
	// caller ranging over SignallingStatus() terminates instead of parking
	// forever, and a send racing that close would otherwise panic.
	muStatus sync.Mutex
	// statusClosed records that statusChan has been closed, since a closed
	// channel cannot be detected from the sending side.
	statusClosed bool
	// stateEventChan delivers ASP state transitions to the user. An overflow
	// closes the association so Layer Management cannot mistake a partial event
	// history for a complete one.
	stateEventChan chan State
	// muStateEvent guards stateEventChan's closure, as muStatus does for
	// statusChan.
	muStateEvent sync.Mutex
	// stateEventClosed records that stateEventChan has been closed.
	stateEventClosed bool
	// mgmtChan delivers the M3UA-to-Layer-Management indications of RFC 4666
	// Section 1.6.3. An overflow closes the association rather than dropping an
	// unrecoverable indication.
	mgmtChan chan *ManagementIndication
	// muMgmt guards mgmtChan's closure, as muStatus does for statusChan.
	muMgmt sync.Mutex
	// mgmtClosed records that mgmtChan has been closed.
	mgmtClosed bool
	// indicationOverflow ensures one state or management queue overflow starts
	// one asynchronous association teardown.
	indicationOverflow atomic.Bool
	// assocID is the kernel's ID for this association, recorded at setup so a
	// notification handler shared across a Listener's associations can route an
	// event to the Association it concerns. Atomic: written on the goroutine that set
	// the socket up, read on whichever reader receives a notification.
	assocID atomic.Int32
	// signalWriter, when non-nil, replaces the SCTP write performed by
	// WriteSignal. It is only set in tests, where the M3UA state machine is
	// exercised without an SCTP association.
	signalWriter func(m3 messages.M3UA) (int, error)
	// dataWriter is the raw DATA write test seam. Production leaves it nil and
	// writes through sctpConn.
	dataWriter      func([]byte, *sctp.SndRcvInfo) (int, error)
	transportCloser func() error
	// notificationQueue keeps peer-controlled socket backpressure out of the AS
	// state machine and proactive SSNM paths. A full queue closes the association
	// rather than silently dropping mandatory ordered control traffic.
	notificationQueue    chan mandatoryControl
	notificationOnce     sync.Once
	notificationOverflow atomic.Bool
	// notificationWriter is the asynchronous control-worker test seam. Production
	// leaves it nil and writes through WriteSignal.
	notificationWriter func(messages.M3UA) (int, error)
	// rkmRequestMu serializes local RFC 4666 Registration and Deregistration
	// procedures; RKM responses have no association-wide transaction identifier.
	rkmRequestMu sync.Mutex
	// rkmLifecycleMu serializes responder-side RKM state publication with
	// association teardown so an in-flight REG RSP cannot recreate membership
	// after the Endpoint has forgotten the association.
	rkmLifecycleMu sync.Mutex
	// rkmCorrelationMu guards Registration response correlation state shared by
	// the requesting goroutine and the association monitor.
	rkmCorrelationMu                 sync.Mutex
	rkmPendingLocalIDs               map[uint32]struct{}
	rkmRegistrationRequests          map[uint32]RoutingKeyRegistrationRequest
	rkmDeliveredRegistrationResults  map[uint32]RoutingKeyRegistrationResult
	rkmUnresolvedRegistrations       map[uint32]RoutingKeyRegistrationRequest
	rkmPendingDeregistrationRCs      map[uint32]struct{}
	rkmDeliveredDeregistrationStatus map[uint32]DeregistrationStatus
	rkmUnresolvedDeregistrationRCs   map[uint32]struct{}
	rkmAwaiting                      uint32
	rkmNextLocalID                   uint32
	rkmResponseChan                  chan messages.M3UA
}

var netMap = map[string]string{
	"m3ua":  "sctp",
	"m3ua4": "sctp4",
	"m3ua6": "sctp6",
}

const defaultNotificationQueueSize = 64

type mandatoryControl struct {
	messages            []messages.M3UA
	enforceTrafficScope bool
	result              chan error
}

// newAssociation builds an unconnected Association for the given endpoint role.
//
// Dial and Accept previously each constructed an Association inline, so every
// field added to one had to be remembered in the other; the shared
// AssociationConfig write that
// broke multi-ASP serving lived in exactly that duplicated code. Both now go
// through here, and nothing in this function writes to cfg.
func newAssociation(role Role, cfg *AssociationConfig) *Association {
	return newAssociationWithTrafficModePolicy(role, cfg, newTrafficModePolicy(cfg))
}

func newAssociationWithTrafficModePolicy(role Role, cfg *AssociationConfig, trafficModes trafficModePolicy) *Association {
	dataQueueSize := cfg.DataQueueSize
	if dataQueueSize <= 0 {
		dataQueueSize = DefaultDataQueueSize
	}

	c := &Association{
		muState:      new(sync.RWMutex),
		role:         role,
		stateChan:    make(chan State),
		inboundChan:  make(chan inbound),
		established:  make(chan struct{}, 1),
		done:         make(chan struct{}),
		errChan:      make(chan error),
		dataChan:     make(chan *DataMessage, dataQueueSize),
		beatAckChan:  make(chan struct{}, 1), // see notifyBeatAck: buffers an Ack that beats heartbeat() to its select
		beatStart:    make(chan struct{}),
		destinations: newDestinations(),
		tack:         newTAckRetransmitter(),
		statusChan:   make(chan *DestinationStatus, 64),
		// Sized for the handful of transitions an association makes in its
		// lifetime rather than for an unbounded stream.
		stateEventChan: make(chan State, 16),
		// A peer under stress can emit Notifies steadily, so this is sized like
		// statusChan rather than like the state channel.
		mgmtChan:                 make(chan *ManagementIndication, 64),
		notificationQueue:        make(chan mandatoryControl, defaultNotificationQueueSize),
		cfg:                      cfg,
		sctpInfo:                 &sctp.SndRcvInfo{PPID: M3UAPPID, Stream: 0},
		dynamicPeerASKeys:        make(map[uint32]ASKey),
		dynamicLocalASKeys:       make(map[uint32]ASKey),
		dynamicPeerTrafficModes:  make(map[uint32]uint32),
		dynamicLocalTrafficModes: make(map[uint32]uint32),
		// A dialing ASP wants to carry traffic; Dial does not return until
		// it does.
		resumeTo: StateASPActive,
	}
	c.trafficModes.freeze(trafficModes)
	c.freezeTrafficModePolicies()

	// A nil HeartbeatInfo means the caller never asked for M3UA BEATs —
	// NewAssociationConfig leaves it nil — so it resolves to disabled rather
	// than dereferencing.
	if cfg.HeartbeatInfo != nil {
		c.hb = *cfg.HeartbeatInfo
	}
	// An interval of zero would make heartbeat() spin, so it disables BEATs
	// however the AssociationConfig was built.
	if c.hb.Interval == 0 {
		c.hb.Enabled = false
	}
	// RFC 4666 Section 4.3.4.6 derives the deadline from the interval —
	// "within 2*T(beat)" — so an unset Timer means that, not zero. time.After(0)
	// fires immediately, so enabling BEATs without a Timer used to tear the
	// association down on the first round.
	if c.hb.Timer <= 0 {
		c.hb.Timer = 2 * c.hb.Interval
	}

	return c
}

// Role returns this association's immutable M3UA protocol role.
func (c *Association) Role() Role {
	if c == nil {
		return 0
	}
	return c.role
}

// ApplicationServerState returns the state of the unambiguous Application
// Server identified by routingContext. RFC 4666 Section 4.3.2 requires an SGP
// to maintain AS state; a Routing Context shared by multiple Network
// Appearances is ambiguous and therefore reports AS-DOWN rather than guessing.
func (c *Association) ApplicationServerState(routingContext uint32) ASState {
	if c == nil || c.as == nil {
		return ASDown
	}
	_, applicationServer, ok, ambiguous := c.as.lookupRoutingContext(routingContext)
	if !ok || ambiguous {
		return ASDown
	}
	return applicationServer.State()
}

// ApplicationServerStateForAS returns the RFC 4666 Section 4.3.2 state for an
// exact ASKey, including its Network Appearance and contextless-AS identity.
func (c *Association) ApplicationServerStateForAS(key ASKey) ASState {
	if c == nil || c.as == nil {
		return ASDown
	}
	applicationServer, ok := c.as.lookup(key)
	if !ok {
		return ASDown
	}
	return applicationServer.State()
}

func (c *Association) trafficModePolicy() trafficModePolicy {
	if c == nil {
		return trafficModePolicy{}
	}
	if c.isIPSPDoubleExchange() {
		return c.withDynamicTrafficModes(c.peerIPSPTrafficModes.get(nil), false)
	}
	return c.withDynamicTrafficModes(c.trafficModes.get(c.cfg), false)
}

func (c *Association) localTrafficModePolicy() trafficModePolicy {
	if c == nil {
		return trafficModePolicy{}
	}
	if c.isIPSPDoubleExchange() {
		return c.withDynamicTrafficModes(c.localIPSPTrafficModes.get(nil), true)
	}
	return c.withDynamicTrafficModes(c.trafficModes.get(c.cfg), true)
}

func (c *Association) withDynamicTrafficModes(policy trafficModePolicy, local bool) trafficModePolicy {
	c.muDynamicASKeys.RLock()
	dynamic := c.dynamicPeerTrafficModes
	if local {
		dynamic = c.dynamicLocalTrafficModes
	}
	if len(dynamic) == 0 {
		c.muDynamicASKeys.RUnlock()
		return policy
	}
	modes := make(map[uint32]uint32, len(policy.modes)+len(dynamic))
	for routingContext, mode := range policy.modes {
		modes[routingContext] = mode
	}
	for routingContext, mode := range dynamic {
		modes[routingContext] = mode
	}
	c.muDynamicASKeys.RUnlock()
	policy.modes = modes
	return policy
}

func (c *Association) freezeTrafficModePolicies() {
	if c == nil || c.cfg == nil || c.cfg.IPSP == nil || c.cfg.IPSP.ExchangeModel != IPSPExchangeDouble {
		return
	}
	c.localIPSPTrafficModes.freeze(newIPSPTrafficModePolicy(c.cfg.IPSP.TrafficToLocal))
	c.peerIPSPTrafficModes.freeze(newIPSPTrafficModePolicy(c.cfg.IPSP.TrafficToPeer))
}

func (c *Association) isIPSPDoubleExchange() bool {
	return c != nil && c.role == RoleIPSP && c.cfg != nil && c.cfg.IPSP != nil &&
		c.cfg.IPSP.ExchangeModel == IPSPExchangeDouble
}

func (c *Association) usesSingleASPSMExchange() bool {
	return c.isIPSPDoubleExchange() && c.cfg.IPSP.ASPSMExchange == IPSPASPSMExchangeSingle
}

func (c *Association) hasLocalIPSPTrafficDirection() bool {
	return c.isIPSPDoubleExchange() && c.cfg.IPSP.TrafficToLocal != nil
}

func (c *Association) hasPeerIPSPTrafficDirection() bool {
	return c.isIPSPDoubleExchange() && c.cfg.IPSP.TrafficToPeer != nil
}

// IPSPState returns the two independent traffic-direction states of an IPSP
// Double Exchange Association. For Single Exchange both fields contain State().
func (c *Association) IPSPState() IPSPState {
	if c == nil {
		return IPSPState{}
	}
	c.muState.RLock()
	defer c.muState.RUnlock()
	if !c.isIPSPDoubleExchange() {
		return IPSPState{TrafficToLocal: c.state, TrafficToPeer: c.state}
	}
	return IPSPState{TrafficToLocal: c.localIPSPState, TrafficToPeer: c.state}
}

func (c *Association) localIPSPStateValue() State {
	c.muState.RLock()
	defer c.muState.RUnlock()
	return c.localIPSPState
}

func (c *Association) commitLocalIPSPState(state State) bool {
	unlockTransfer := c.lockASPTransferMutation()
	c.muState.Lock()
	select {
	case <-c.done:
		c.muState.Unlock()
		unlockTransfer()
		return false
	default:
	}
	changed := c.localIPSPState != state
	c.localIPSPState = state
	c.muState.Unlock()
	unlockTransfer()
	if changed {
		c.notifyASPRouteStateChanged()
	}
	if state == StateASPActive {
		c.notifyEstablished()
		c.allowHeartbeat()
	}
	return true
}

func (c *Association) enterLocalIPSPState(state State) error {
	previous := c.localIPSPStateValue()
	if !c.commitLocalIPSPState(state) {
		return nil
	}
	if state == StateASPInactive && previous == StateASPDown && c.cfg.IPSP.InitiateASPTM && !c.terminating.Load() {
		return c.initiateASPTM()
	}
	return nil
}

// setUpSocket applies the configured SCTP options to the SCTP association this
// Association
// has just been given and records the negotiated stream count. On any failure
// the association is closed, since the Association is not usable without it.
func (c *Association) setUpSocket() error {
	if sack := c.cfg.SCTPSACKInfo; sack != nil && sack.Enabled {
		if err := validateSackDelay(sack.SackDelay); err != nil {
			_ = c.sctpConn.Close()
			return err
		}
		if err := c.sctpConn.SetSackTimer(&sctp.SackTimer{
			SackDelay:     sack.SackDelay,
			SackFrequency: sack.SackFrequency,
		}); err != nil {
			_ = c.sctpConn.Close()
			return fmt.Errorf("failed to set sack timer: %w", err)
		}
	}

	if nd := c.cfg.SCTPNoDelayInfo; nd != nil && nd.Enabled {
		optval := 0
		if nd.NoDelay {
			optval = 1
		}
		if err := c.sctpConn.SetNoDelay(optval); err != nil {
			_ = c.sctpConn.Close()
			return fmt.Errorf("failed to set no delay: %w", err)
		}
	}

	// The stream a message arrived on is part of what makes it valid (RFC 4666
	// Section 1.4.7 rules 1 to 3, and Section 3.8.1's "Invalid Stream
	// Identifier" error), and the kernel reports it only when asked: without
	// this, every read comes back with no ancillary data and the receive-side
	// stream checks have nothing to check.
	//
	// SCTP_RECVRCVINFO is the non-deprecated way to ask. RFC 6458 Section 5.3.2
	// splits the old sctp_sndrcvinfo into SCTP_SNDINFO for sending and
	// SCTP_RCVINFO for receiving, and the read path accepts either form, so this
	// asks for exactly the per-message information it needs. The alternative,
	// SubscribeEvents(SCTP_EVENT_DATA_IO), goes through the deprecated
	// SCTP_EVENTS and would also have to be kept clear of the notification
	// flags, which deliver association and peer-address events into the same
	// read stream where the dispatcher would try to parse them as M3UA.
	if err := c.sctpConn.SetRecvRcvInfo(true); err != nil {
		_ = c.sctpConn.Close()
		return fmt.Errorf("failed to enable SCTP_RECVRCVINFO: %w", err)
	}

	r, err := c.sctpConn.GetStatus()
	if err != nil {
		_ = c.sctpConn.Close()
		return fmt.Errorf("failed to get SCTP association status: %w", err)
	}
	// One less than the peer's outbound stream count, since stream 0 carries no
	// DATA (RFC 4666 Section 1.4.7 rule 1). Guarded: RFC 9260 Section 3.3.2
	// forbids advertising zero streams, but the count comes back from a
	// getsockopt and an unsigned 0-1 would wrap to 65535 — every DATA would
	// then go out on a stream the peer never opened and be discarded by it,
	// silently and completely.
	if r.Ostreams == 0 {
		_ = c.sctpConn.Close()
		return fmt.Errorf("peer negotiated %d outbound streams", r.Ostreams)
	}
	c.maxMessageStreamID = r.Ostreams - 1
	// Recorded so a shared notification handler can tell which association an
	// event belongs to: a Listener installs one handler for every association
	// it accepts, and the kernel names the association only by this ID.
	c.assocID.Store(int32(r.AssocID))
	// Optional, and deliberately not fatal; see subscribeRestart.
	c.subscribeRestart()

	return nil
}

// DataMessage is one received DATA message: the MTP3 payload together with the
// SS7 network and traffic flow the sending peer named for it.
//
// RFC 4666 Section 3.3.1 gives Network Appearance and Routing Context exactly
// these jobs: Network Appearance identifies the SS7 network, while "Where
// multiple Routing Keys and Routing Contexts are used across a common
// association, the Routing Context MUST be sent to identify the traffic flow,
// assisting in the internal distribution of Data messages" — so a receiver that
// is handed the payload alone cannot perform the distribution those parameters
// exist for, and cannot tell which network or flow to answer on either.
type DataMessage struct {
	// ProtocolData is the MTP3 payload and its routing label.
	ProtocolData *params.ProtocolDataPayload

	// NetworkAppearance is the SS7 network the DATA belongs to, valid only
	// when NetworkAppearanceSet is true. Presence is separate because zero is
	// a legitimate configured value and omission is permitted on a dedicated
	// association.
	NetworkAppearance    uint32
	NetworkAppearanceSet bool

	// RoutingContext is the traffic flow the DATA named, valid only when
	// RoutingContextSet is true.
	//
	// The parameter is Conditional, so its absence is not a fault: "Where a
	// Routing Key has not been coordinated between the SGP and ASP, sending of
	// Routing Context is not required." A separate bool rather than a zero
	// value, because 0 is a Routing Context a peer may legitimately use.
	RoutingContext    uint32
	RoutingContextSet bool
}

// Read reads data from the association.
//
// M3UA is message-oriented, so one Read yields the payload of one DATA message
// and never joins two together. If b is too small for it, the payload is
// truncated to fit and io.ErrShortBuffer is returned alongside the number of
// bytes actually written: the remainder cannot be recovered, because the
// message has already left the queue. Use ReadPD to take the payload whole
// without sizing a buffer for it, or ReadData when Network Appearance or
// Routing Context matters.
//
// Read used to return len(pd.Data) regardless of how much it had copied, so a
// caller with a short buffer was told it had received more bytes than exist in
// b — and the idiomatic b[:n] then panicked with a slice-bounds error, on data
// chosen by the peer.
func (c *Association) Read(b []byte) (n int, err error) {
	d, err := c.ReadData()
	if err != nil {
		return 0, err
	}

	n = copy(b, d.ProtocolData.Data)
	if n < len(d.ProtocolData.Data) {
		return n, io.ErrShortBuffer
	}
	return n, nil
}

// ReadPD reads the next ProtocolDataPayload from the association.
//
// The Network Appearance and Routing Context the message named are not reported
// here; use ReadData when the association carries more than one SS7 network or
// traffic flow.
func (c *Association) ReadPD() (pd *params.ProtocolDataPayload, err error) {
	d, err := c.ReadData()
	if err != nil {
		return nil, err
	}
	return d.ProtocolData, nil
}

// ReadData reads the next DATA message from the association, reporting the
// payload together with its Network Appearance and Routing Context.
//
// It is the read-side counterpart of the WithRoutingContext writes: an
// application serving several networks or Routing Contexts over one association
// needs this to distribute an inbound message to the right network and flow,
// and to answer in the scope the request arrived on. Read and ReadPD take from
// the same queue, so a caller that does not care about that scope can keep using
// either.
func (c *Association) ReadData() (*DataMessage, error) {
	if !c.inboundDataActive() {
		return nil, ErrNotEstablished
	}

	timeout, stop, expired := c.readTimeout()
	if expired {
		return nil, os.ErrDeadlineExceeded
	}
	defer stop()

	select {
	case d, ok := <-c.dataChan:
		if !ok {
			return nil, ErrNotEstablished
		}
		return d, nil
	case <-timeout:
		// Recoverable, unlike every other error here: the association is
		// healthy and the caller may read again.
		return nil, os.ErrDeadlineExceeded
	case <-c.done:
		return nil, ErrNotEstablished
	}
}

// Write writes data to the association.
//
// A successful call returns len(b), as io.Writer requires: the payload is
// wrapped in an M3UA DATA message before it goes out, but the count reported is
// the caller's, not the message's.
func (c *Association) Write(b []byte) (n int, err error) {
	// Checked before choosing a stream: on an Association that never established,
	// maxMessageStreamID is still zero and there is no stream to choose.
	if c.State() != StateASPActive {
		return 0, ErrNotEstablished
	}

	// One AssociationConfig, one SLS, so every Write shares a stream and stays ordered.
	return c.WriteToStream(b, c.streamFor(c.cfg.SignallingLinkSelection))
}

// WriteWithRoutingContext writes data to the association, naming the traffic flow
// it belongs to.
//
// This is the concurrency-safe form of SelectRoutingContext followed by Write.
// RFC 4666 Section 3.3.1 makes the Routing Context a property of the message —
// it identifies "the traffic flow" the payload belongs to — so on an association
// carrying several flows it has to travel with the payload. Held on the Association
// instead, a second goroutine's selection can land between this goroutine's
// selection and its write, and the payload goes out naming the other flow.
//
// As with Write, a successful call returns len(b).
func (c *Association) WriteWithRoutingContext(b []byte, rtCtx uint32) (n int, err error) {
	if c.State() != StateASPActive {
		return 0, ErrNotEstablished
	}

	return c.WriteToStreamWithRoutingContext(b, c.streamFor(c.cfg.SignallingLinkSelection), rtCtx)
}

// WriteToStream writes data to the association and specific stream.
//
// streamID must be between 1 and MaxMessageStreamID, inclusive. Stream 0
// returns ErrNoDataStream; a value above the negotiated maximum returns an
// *InvalidSCTPStreamIDError.
//
// As with Write, a successful call returns len(b): the count is the payload the
// caller handed over, not the size of the M3UA message it was wrapped in.
func (c *Association) WriteToStream(b []byte, streamID uint16) (n int, err error) {
	if c.State() != StateASPActive {
		return 0, ErrNotEstablished
	}

	return c.writeData(b, streamID, nil)
}

// WriteToStreamWithRoutingContext writes data on a specific stream, naming the
// traffic flow it belongs to.
//
// See WriteWithRoutingContext for why the flow belongs on the message and
// WriteToStream for the permitted stream range.
func (c *Association) WriteToStreamWithRoutingContext(b []byte, streamID uint16, rtCtx uint32) (n int, err error) {
	if c.State() != StateASPActive {
		return 0, ErrNotEstablished
	}

	return c.writeData(b, streamID, &rtCtx)
}

// writeData is the shared body of the four payload writes. rtCtx names the
// traffic flow for this one message, or is nil to fall back to the
// association-wide selection.
func (c *Association) writeData(b []byte, streamID uint16, rtCtx *uint32) (int, error) {
	if err := c.checkDataStream(streamID); err != nil {
		return 0, err
	}
	// The AssociationConfig Params are copied because NewData calls SetLength on each one,
	// which writes to the caller's Param: two goroutines sending concurrently
	// would otherwise write to the same shared config Param.
	rc, err := c.resolveRoutingContext(rtCtx)
	if err != nil {
		return 0, err
	}
	release, err := c.lockResolvedOutboundDataScope(rc)
	if err != nil {
		return 0, err
	}
	defer release()
	d, err := messages.NewData(
		c.networkAppearanceForRoutingContext(rc, false).Copy(), rc, params.NewProtocolData(
			c.cfg.OriginatingPointCode, c.cfg.DestinationPointCode,
			c.cfg.ServiceIndicator, c.cfg.NetworkIndicator,
			c.cfg.MessagePriority, c.cfg.SignallingLinkSelection, b,
		), c.cfg.CorrelationID.Copy(),
	).MarshalBinary()
	if err != nil {
		return 0, err
	}

	// taken by value to avoid race condition on the stream id
	info := *c.sctpInfo
	info.Stream = streamID
	if _, err := c.writeSCTPData(d, &info); err != nil {
		return 0, err
	}

	// io.Writer's contract: a successful Write reports the whole of b. The
	// count used to be the encoded message length added to itself, which is
	// both larger than b and unrelated to it.
	return len(b), nil
}

// WritePD writes data with specific MTP3 Protocol Data to the association.
//
// A successful call reports the number of SS7 user octets carried inside
// protocolData, so the count means the same thing as Write's.
func (c *Association) WritePD(protocolData *params.Param) (n int, err error) {
	// As in Write: refuse before choosing a stream, since an unestablished
	// Association has no negotiated stream count to choose from.
	if c.State() != StateASPActive {
		return 0, ErrNotEstablished
	}

	pd, err := protocolData.ProtocolData()
	if err != nil {
		return 0, fmt.Errorf("invalid protocol data: %w", err)
	}

	// The routing label travels with the message here rather than coming from
	// AssociationConfig, so the stream follows this message's own SLS.
	return c.writePD(protocolData, pd, c.streamFor(pd.SignallingLinkSelection), nil)
}

// WritePDWithRoutingContext writes data with a specific mtp3 protocol data,
// naming the traffic flow it belongs to.
//
// See WriteWithRoutingContext for why the flow belongs on the message. This is
// the form to use when one association carries several Routing Contexts and
// more than one goroutine writes to it.
func (c *Association) WritePDWithRoutingContext(protocolData *params.Param, rtCtx uint32) (n int, err error) {
	if c.State() != StateASPActive {
		return 0, ErrNotEstablished
	}

	pd, err := protocolData.ProtocolData()
	if err != nil {
		return 0, fmt.Errorf("invalid protocol data: %w", err)
	}

	return c.writePD(protocolData, pd, c.streamFor(pd.SignallingLinkSelection), &rtCtx)
}

// WritePDToStream writes data with a specific mtp3 protocol data to the
// association and specific stream.
//
// The stream range and errors are the same as WriteToStream.
//
// As with WritePD, a successful call reports the SS7 user octets carried.
func (c *Association) WritePDToStream(protocolData *params.Param, streamID uint16) (n int, err error) {
	if c.State() != StateASPActive {
		return 0, ErrNotEstablished
	}

	// Peeled before the send so a Protocol Data that marshals but cannot be
	// parsed back is refused rather than put on the wire, and so the reported
	// count is the SS7 user octets carried.
	pd, err := protocolData.ProtocolData()
	if err != nil {
		return 0, fmt.Errorf("invalid protocol data: %w", err)
	}

	return c.writePD(protocolData, pd, streamID, nil)
}

// WritePDToStreamWithRoutingContext writes data with a specific mtp3 protocol
// data on a specific stream, naming the traffic flow it belongs to.
//
// See WriteWithRoutingContext for why the flow belongs on the message and
// WriteToStream for the permitted stream range.
func (c *Association) WritePDToStreamWithRoutingContext(protocolData *params.Param, streamID uint16, rtCtx uint32) (n int, err error) {
	if c.State() != StateASPActive {
		return 0, ErrNotEstablished
	}

	pd, err := protocolData.ProtocolData()
	if err != nil {
		return 0, fmt.Errorf("invalid protocol data: %w", err)
	}

	return c.writePD(protocolData, pd, streamID, &rtCtx)
}

// writePD is the shared body of the Protocol Data writes, taking the already
// peeled payload so none of them has to parse it twice. rtCtx names the traffic
// flow for this one message, or is nil to fall back to the association-wide
// selection.
func (c *Association) writePD(protocolData *params.Param, pd *params.ProtocolDataPayload, streamID uint16, rtCtx *uint32) (int, error) {
	if err := c.checkDataStream(streamID); err != nil {
		return 0, err
	}

	// Copied for the same reason as in writeData: NewData writes to every
	// Param it is given, and these are shared across every send on this Association.
	rc, err := c.resolveRoutingContext(rtCtx)
	if err != nil {
		return 0, err
	}
	release, err := c.lockResolvedOutboundDataScope(rc)
	if err != nil {
		return 0, err
	}
	defer release()
	d, err := messages.NewData(
		c.networkAppearanceForRoutingContext(rc, false).Copy(),
		rc,           // the one context identifying this traffic flow
		protocolData, // custom mtp3 protocol data OPC, DPC, SI, NI, MP, and SLS, flexible on active connections
		c.cfg.CorrelationID.Copy(),
	).MarshalBinary()
	if err != nil {
		return 0, err
	}

	// taken by value to avoid race condition on the stream id
	info := *c.sctpInfo
	info.Stream = streamID
	if _, err := c.writeSCTPData(d, &info); err != nil {
		return 0, err
	}

	return len(pd.Data), nil
}

func (c *Association) writeSCTPData(data []byte, info *sctp.SndRcvInfo) (int, error) {
	if c.dataWriter != nil {
		return c.dataWriter(data, info)
	}
	return c.sctpConn.SCTPWrite(data, info)
}

func (c *Association) inboundDataActive() bool {
	if c.isIPSPDoubleExchange() {
		return c.localIPSPStateValue() == StateASPActive
	}
	return c.State() == StateASPActive
}

func (c *Association) outboundNetworkAppearance() *params.Param {
	if c == nil || c.cfg == nil {
		return nil
	}
	if c.isIPSPDoubleExchange() {
		if c.cfg.IPSP.TrafficToPeer == nil {
			return nil
		}
		return c.cfg.IPSP.TrafficToPeer.NetworkAppearance
	}
	return c.cfg.NetworkAppearance
}

func (c *Association) localNetworkAppearance() *params.Param {
	if c == nil || c.cfg == nil {
		return nil
	}
	if c.isIPSPDoubleExchange() {
		if c.cfg.IPSP.TrafficToLocal == nil {
			return nil
		}
		return c.cfg.IPSP.TrafficToLocal.NetworkAppearance
	}
	return c.cfg.NetworkAppearance
}

// WriteSignal writes an M3UA message on the SCTP association.
//
// It takes a message rather than a buffer, so a successful call reports the
// encoded length of that message.
func (c *Association) WriteSignal(m3 messages.M3UA) (n int, err error) {
	return c.writeSignal(m3, true)
}

// writeDistributedSignal writes a message whose Application Server has already
// been selected and whose deliveryMu is already held by the distribution
// engine. Re-entering the public scope barrier there would deadlock.
func (c *Association) writeDistributedSignal(m3 messages.M3UA) (n int, err error) {
	return c.writeSignal(m3, false)
}

// writeMandatoryControls writes one ordered control batch. The queue-backed
// implementation is shared by proactive SSNM and Notify; hand-built test Associations
// without that queue retain synchronous observability.
func (c *Association) writeMandatoryControls(control []messages.M3UA, enforceTrafficScope, wait bool) error {
	if c == nil || len(control) == 0 {
		return nil
	}
	writeDirect := func() error {
		for _, message := range control {
			if _, err := c.writeSignal(message, enforceTrafficScope); err != nil {
				return err
			}
		}
		return nil
	}
	// Hand-built state-machine tests intentionally omit the production queue so
	// their signalWriter remains synchronous and directly observable.
	if c.notificationQueue == nil {
		return writeDirect()
	}

	entry := mandatoryControl{
		messages:            append([]messages.M3UA(nil), control...),
		enforceTrafficScope: enforceTrafficScope,
	}
	if wait {
		entry.result = make(chan error, 1)
	}
	c.notificationOnce.Do(func() { go c.writeNotifications() })
	select {
	case <-c.done:
		if err := c.Err(); err != nil {
			return err
		}
		return ErrAssociationClosed
	default:
	}
	select {
	case c.notificationQueue <- entry:
	case <-c.done:
		if err := c.Err(); err != nil {
			return err
		}
		return ErrAssociationClosed
	default:
		if c.notificationOverflow.CompareAndSwap(false, true) {
			go func() { _ = c.closeWith(ErrNotificationQueueFull) }()
		}
		return ErrNotificationQueueFull
	}
	if !wait {
		return nil
	}
	select {
	case err := <-entry.result:
		return err
	case <-c.done:
		if err := c.Err(); err != nil {
			return err
		}
		return ErrAssociationClosed
	}
}

func (c *Association) writeSignal(m3 messages.M3UA, enforceTrafficScope bool) (n int, err error) {
	if m3 == nil {
		return 0, errors.New("cannot write a nil M3UA signal")
	}
	if data, ok := m3.(*messages.Data); ok &&
		(data.ProtocolData == nil || data.ProtocolData.Tag != params.ProtocolData) {
		return 0, ErrMissingProtocolData
	}

	n = m3.MarshalLen()
	if n < 0 {
		return 0, fmt.Errorf("invalid encoded length %d for %T", n, m3)
	}
	buf := make([]byte, n)
	if err := m3.MarshalTo(buf); err != nil {
		return 0, fmt.Errorf("failed to create %T: %w", m3, err)
	}
	payloadData := isPayloadData(buf)
	trafficScoped := payloadData || isSSNM(buf)
	if enforceTrafficScope && payloadData && c.State() != StateASPActive {
		return 0, ErrNotEstablished
	}

	if c.signalWriter != nil {
		if enforceTrafficScope && trafficScoped {
			release, err := c.lockOutboundTrafficScope(buf)
			if err != nil {
				return 0, err
			}
			defer release()
		}
		return c.signalWriter(m3)
	}

	// taken by value to avoid race condition on the stream id
	sctpInfo := *c.sctpInfo
	sctpInfo.Stream, err = c.outboundSignalStream(buf)
	if err != nil {
		return 0, err
	}
	if enforceTrafficScope && trafficScoped {
		release, err := c.lockOutboundTrafficScope(buf)
		if err != nil {
			return 0, err
		}
		defer release()
	}

	if _, err := c.sctpConn.SCTPWrite(buf, &sctpInfo); err != nil {
		return 0, fmt.Errorf("failed to write M3UA: %w", err)
	}

	// The encoded length, counted once. It used to have the SCTPWrite return
	// added to it, which is the same number again.
	return n, nil
}

func (c *Association) lockOutboundTrafficScope(raw []byte) (func(), error) {
	if isPayloadData(raw) {
		return c.lockOutboundDataScope(raw)
	}
	return c.lockOutboundSSNMScope(raw)
}

// lockOutboundDataScope validates the DATA traffic flow and, at an SGP, holds
// that AS's delivery barrier across the socket write. ASP Inactive/Down first
// removes the ASP from the AS and then waits for this lock, so its Ack cannot
// overtake a direct public WriteSignal(DATA).
func (c *Association) lockOutboundDataScope(raw []byte) (func(), error) {
	decoded, err := messages.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid DATA: %w", err)
	}
	data, ok := decoded.(*messages.Data)
	if !ok {
		return nil, errors.New("transfer payload did not decode as DATA")
	}
	// In RFC 4666 Section 5.6.2 Double Exchange, this DATA belongs to the
	// peer-directed traffic flow. Its Network Appearance is independent of the
	// local-directed flow used by outbound SCON.
	configuredNetworkAppearance, allNetworkAppearances, err := c.resolveNetworkAppearanceScope(data.RoutingContext, false)
	if err != nil {
		return nil, err
	}
	if err := validateOutboundNetworkAppearanceAgainst(
		data.NetworkAppearance,
		configuredNetworkAppearance,
		allNetworkAppearances,
	); err != nil {
		return nil, err
	}

	configured := c.configuredRoutingContexts()
	var rtCtx uint32
	if data.RoutingContext == nil {
		switch len(configured) {
		case 0:
			return c.lockResolvedOutboundDataScope(nil)
		case 1:
			rtCtx = configured[0]
		default:
			return nil, ErrMissingRoutingContext
		}
	} else {
		routingContexts := data.RoutingContext.RoutingContexts()
		if len(routingContexts) != 1 {
			return nil, NewInvalidRoutingContextError(routingContexts...)
		}
		rtCtx = routingContexts[0]
	}

	if _, err := c.routingContextFor(rtCtx); err != nil {
		return nil, err
	}
	return c.lockResolvedOutboundDataScope(params.NewRoutingContext(rtCtx))
}

func (c *Association) lockResolvedOutboundDataScope(routingContext *params.Param) (func(), error) {
	if c.role == RoleIPSP {
		c.unscopedDeliveryMu.Lock()
		if c.State() != StateASPActive {
			c.unscopedDeliveryMu.Unlock()
			return nil, ErrNotEstablished
		}
		if routingContext != nil {
			for _, rtCtx := range routingContext.RoutingContexts() {
				if !c.outboundRoutingContextActive(rtCtx) {
					c.unscopedDeliveryMu.Unlock()
					return nil, ErrRoutingContextNotActive
				}
			}
		}
		return c.unscopedDeliveryMu.Unlock, nil
	}
	if c.role != RoleSGP {
		return func() {}, nil
	}
	if routingContext == nil {
		c.unscopedDeliveryMu.Lock()
		if c.State() != StateASPActive {
			c.unscopedDeliveryMu.Unlock()
			return nil, ErrNotEstablished
		}
		return c.unscopedDeliveryMu.Unlock, nil
	}
	release, err := c.lockSGPApplicationServers(routingContext.RoutingContexts())
	if err != nil {
		if c.State() != StateASPActive {
			return nil, ErrNotEstablished
		}
		return nil, err
	}
	return release, nil
}

func (c *Association) quiesceUnscopedTraffic() {
	if c == nil || (c.role != RoleSGP && c.role != RoleIPSP) {
		return
	}
	waitForTrafficBarrier(&c.unscopedDeliveryMu)
}

func (c *Association) quiesceLocalIPSPSSNMTraffic() {
	if c == nil || !c.isIPSPDoubleExchange() {
		return
	}
	waitForTrafficBarrier(&c.localIPSPSSNMDeliveryMu)
}

func (c *Association) lockOutboundSSNMScope(raw []byte) (func(), error) {
	decoded, err := messages.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid SSNM: %w", err)
	}
	if c.role == RoleIPSP {
		if _, ok := decoded.(*messages.SignallingCongestion); !ok {
			return nil, ErrUnsupportedRole
		}
	}
	routingContext, known := ssnmRoutingContext(decoded)
	if !known {
		return func() {}, nil
	}
	// RFC 4666 Section 5.6.2 defines SCON as flowing opposite DATA between
	// IPSPs, so Double Exchange validates it against the local-directed flow.
	localScope := c.isIPSPDoubleExchange()
	configuredNetworkAppearance, allNetworkAppearances, err := c.resolveNetworkAppearanceScope(routingContext, localScope)
	if err != nil {
		return nil, err
	}
	if err := validateOutboundNetworkAppearanceAgainst(
		ssnmNetworkAppearance(decoded),
		configuredNetworkAppearance,
		allNetworkAppearances,
	); err != nil {
		return nil, err
	}
	if err := c.validateOutboundSSNMRoutingContext(routingContext); err != nil {
		return nil, err
	}

	var routingContexts []uint32
	if routingContext != nil {
		routingContexts = routingContext.RoutingContexts()
	} else {
		routingContexts = c.configuredLocalRoutingContexts()
	}
	if len(routingContexts) == 0 {
		state := c.State()
		if c.isIPSPDoubleExchange() {
			state = c.localIPSPStateValue()
		}
		if state != StateASPActive {
			return nil, ErrNotEstablished
		}
		if c.role == RoleSGP && c.cfg != nil && c.cfg.RoutingContexts != nil &&
			len(c.cfg.RoutingContexts.RoutingContexts()) > 0 {
			return nil, ErrNoConfiguredAS
		}
		if c.role == RoleSGP {
			return c.lockResolvedOutboundDataScope(nil)
		}
		if c.role == RoleIPSP {
			if c.isIPSPDoubleExchange() {
				return c.lockResolvedOutboundIPSPSSNMScope(nil)
			}
			return c.lockResolvedOutboundDataScope(nil)
		}
		return func() {}, nil
	}
	if c.isIPSPDoubleExchange() {
		return c.lockResolvedOutboundIPSPSSNMScope(params.NewRoutingContext(routingContexts...))
	}
	for _, rtCtx := range routingContexts {
		if _, err := c.routingContextFor(rtCtx); err != nil {
			return nil, err
		}
	}
	if c.role == RoleIPSP {
		return c.lockResolvedOutboundDataScope(params.NewRoutingContext(routingContexts...))
	}
	return c.lockSGPApplicationServers(routingContexts)
}

func (c *Association) validateOutboundSSNMRoutingContext(routingContext *params.Param) error {
	configured := c.configuredRoutingContexts()
	if c.isIPSPDoubleExchange() {
		configured = c.configuredLocalRoutingContexts()
	}
	if err := validateRoutingContextAgainst(routingContext, configured); err != nil {
		return err
	}
	if routingContext == nil && len(configured) > 1 {
		return ErrMissingRoutingContext
	}
	return nil
}

func (c *Association) lockResolvedOutboundIPSPSSNMScope(routingContext *params.Param) (func(), error) {
	c.localIPSPSSNMDeliveryMu.Lock()
	if c.localIPSPStateValue() != StateASPActive {
		c.localIPSPSSNMDeliveryMu.Unlock()
		return nil, ErrNotEstablished
	}
	if routingContext != nil {
		for _, rtCtx := range routingContext.RoutingContexts() {
			if !c.routingContextAcked(rtCtx) || c.routingContextOverridden(rtCtx) {
				c.localIPSPSSNMDeliveryMu.Unlock()
				return nil, ErrRoutingContextNotActive
			}
		}
	}
	return c.localIPSPSSNMDeliveryMu.Unlock, nil
}

func ssnmNetworkAppearance(message messages.M3UA) *params.Param {
	switch message := message.(type) {
	case *messages.DestinationUnavailable:
		return message.NetworkAppearance
	case *messages.DestinationAvailable:
		return message.NetworkAppearance
	case *messages.DestinationStateAudit:
		return message.NetworkAppearance
	case *messages.SignallingCongestion:
		return message.NetworkAppearance
	case *messages.DestinationUserPartUnavailable:
		return message.NetworkAppearance
	case *messages.DestinationRestricted:
		return message.NetworkAppearance
	default:
		return nil
	}
}

func validateOutboundNetworkAppearanceAgainst(networkAppearance, configured *params.Param, allNetworkAppearances bool) error {
	if networkAppearance == nil {
		return nil
	}
	if networkAppearance.Tag != params.NetworkAppearance || len(networkAppearance.Data) != 4 {
		return ErrInvalidNetworkAppearance
	}
	if allNetworkAppearances {
		return nil
	}
	if configured == nil || configured.Tag != params.NetworkAppearance ||
		len(configured.Data) != 4 ||
		configured.NetworkAppearance() != networkAppearance.NetworkAppearance() {
		return NewInvalidNetworkAppearanceError(networkAppearance.NetworkAppearance())
	}
	return nil
}

func ssnmRoutingContext(message messages.M3UA) (*params.Param, bool) {
	switch message := message.(type) {
	case *messages.DestinationUnavailable:
		return message.RoutingContext, true
	case *messages.DestinationAvailable:
		return message.RoutingContext, true
	case *messages.DestinationStateAudit:
		return message.RoutingContext, true
	case *messages.SignallingCongestion:
		return message.RoutingContext, true
	case *messages.DestinationUserPartUnavailable:
		return message.RoutingContext, true
	case *messages.DestinationRestricted:
		return message.RoutingContext, true
	default:
		return nil, false
	}
}

func (c *Association) lockSGPApplicationServers(routingContexts []uint32) (func(), error) {
	if c.role != RoleSGP || c.as == nil || len(routingContexts) == 0 {
		return func() {}, nil
	}

	ordered := c.asKeysForRoutingContexts(routingContexts)
	sort.Slice(ordered, func(i, j int) bool { return compareASKey(ordered[i], ordered[j]) < 0 })

	applicationServers := make([]*applicationServer, 0, len(ordered))
	for _, key := range ordered {
		applicationServer, ok := c.as.lookup(key)
		if !ok {
			return nil, ErrRoutingContextNotActive
		}
		applicationServers = append(applicationServers, applicationServer)
	}
	for _, applicationServer := range applicationServers {
		applicationServer.deliveryMu.Lock()
	}
	unlock := func() {
		for index := len(applicationServers) - 1; index >= 0; index-- {
			applicationServers[index].deliveryMu.Unlock()
		}
	}
	for index, applicationServer := range applicationServers {
		applicationServer.mu.Lock()
		active := !applicationServer.closed && applicationServer.asps[c] == StateASPActive
		applicationServer.mu.Unlock()
		if !active || !c.activeForASKey(ordered[index]) {
			unlock()
			return nil, ErrRoutingContextNotActive
		}
	}
	return unlock, nil
}

// enqueueNotify preserves Notify order per association without letting one peer
// that stopped reading block another association's state transition or Close.
func (c *Association) enqueueNotify(notification *messages.Notify) {
	if c == nil || notification == nil {
		return
	}
	_ = c.writeMandatoryControls([]messages.M3UA{notification}, true, false)
}

func (c *Association) enqueueNotifyToAvailablePeer(notification *messages.Notify) {
	if c == nil || notification == nil {
		return
	}
	c.muState.RLock()
	if c.state == StateASPDown {
		c.muState.RUnlock()
		return
	}
	c.enqueueNotify(notification)
	c.muState.RUnlock()
}

func (c *Association) writeNotifications() {
	for {
		select {
		case <-c.done:
			return
		case control := <-c.notificationQueue:
			var writeErr error
			for _, message := range control.messages {
				if c.notificationWriter != nil {
					_, writeErr = c.notificationWriter(message)
				} else {
					_, writeErr = c.writeSignal(message, control.enforceTrafficScope)
				}
				if writeErr != nil {
					break
				}
			}
			if control.result != nil {
				control.result <- writeErr
			}
			if writeErr != nil {
				_ = c.closeWith(writeErr)
				return
			}
		}
	}
}

// outboundSignalStream chooses from the bytes that will actually be written,
// not from interface methods a custom M3UA value could make disagree with its
// encoded header. Management remains on stream 0. DATA follows the SLS in its
// own Protocol Data, exactly as WritePD does, so mixing the two write APIs
// cannot split one sequenced traffic flow across SCTP streams.
func (c *Association) outboundSignalStream(raw []byte) (uint16, error) {
	if len(raw) < 4 {
		return 0, messages.ErrTooShortToParse
	}
	if !isPayloadData(raw) {
		return 0, nil
	}

	decoded, err := messages.Parse(raw)
	if err != nil {
		if errors.Is(err, messages.ErrMissingParameter) {
			return 0, ErrMissingProtocolData
		}
		return 0, fmt.Errorf("invalid DATA: %w", err)
	}
	data, ok := decoded.(*messages.Data)
	if !ok {
		return 0, errors.New("transfer payload did not decode as DATA")
	}
	if data.ProtocolData == nil {
		return 0, ErrMissingProtocolData
	}
	pd, err := data.ProtocolData.ProtocolData()
	if err != nil {
		return 0, fmt.Errorf("invalid DATA Protocol Data: %w", err)
	}

	stream := c.streamFor(pd.SignallingLinkSelection)
	if err := c.checkDataStream(stream); err != nil {
		return 0, err
	}
	return stream, nil
}

func isPayloadData(raw []byte) bool {
	return len(raw) >= 4 && raw[2] == messages.MsgClassTransfer &&
		raw[3] == messages.MsgTypePayloadData
}

func isSSNM(raw []byte) bool {
	return len(raw) >= 4 && raw[2] == messages.MsgClassSSNM
}

// Close closes the M3UA association.
//
// Err then reports ErrAssociationClosed, unless the association had already
// ended for some other reason, in which case that reason is kept.
func (c *Association) Close() error {
	return c.closeWith(ErrAssociationClosed)
}

// Done returns a channel closed when the association ends, for whatever reason.
//
// It follows context.Context's shape: select on Done, then ask Err what
// happened. Without it the only way to notice an association had gone was to
// poll State until it read ASP-DOWN, or to wait for a Read or Write to start
// failing — neither of which says why.
func (c *Association) Done() <-chan struct{} {
	return c.done
}

// Err reports why the association ended, or nil while it is still up.
//
// It distinguishes the cases an application has to tell apart:
// ErrAssociationClosed for its own shutdown, ErrHeartbeatExpired for an
// expired M3UA T(beat), a context error for a cancelled owner, and the
// underlying read or protocol error otherwise. Read and Write report
// ErrNotEstablished for all of them.
func (c *Association) Err() error {
	if v := c.closeErr.Load(); v != nil {
		if err, ok := v.(error); ok {
			return err
		}
	}
	return nil
}

// closeWith closes the association, recording cause as the reason.
func (c *Association) closeWith(cause error) error {
	var err error
	c.closeOnce.Do(func() {
		if cause != nil {
			c.closeErr.Store(cause)
		}
		close(c.done)
		// Retransmitters select on done, but cancel them explicitly so a
		// pending request cannot be resent onto a socket that is closing.
		c.stopAllTAck()
		err = c.closeTransport()
		unlockTransfer := c.lockASPTransferMutation()
		c.muState.Lock()
		previousState := c.state
		c.state = StateASPDown
		c.localIPSPState = StateASPDown
		c.appliedState = StateASPDown
		c.muState.Unlock()
		unlockTransfer()
		if c.isIPSPDoubleExchange() {
			c.muAckedRCs.Lock()
			c.ackedRCs = nil
			c.ackedRCsScoped = true
			c.activeRCs = nil
			c.activeRCsScoped = true
			c.activeScopeInitialized = true
			c.contextlessASActive = false
			c.inactiveDynamicRCs = nil
			c.overriddenRCs = nil
			c.muAckedRCs.Unlock()
		}
		c.notifyASPRouteStateChanged()
		if previousState != StateASPDown {
			c.notifyStateChange(StateASPDown)
		}
		// The association is the only route to whatever the peer had reported
		// on, so those destinations are now unavailable and the MTP3-User is
		// told before the channel closes behind them.
		c.pauseDestinations()
		// Ends any range over SignallingStatus(); see closeStatus.
		c.closeStatus()
		// Ends any range over StateChanges(); see closeStateChanges.
		c.closeStateChanges()
		c.notifyManagement(&ManagementIndication{
			Kind:        ManagementSCTPRelease,
			Description: causeDescription(cause),
		})
		// Ends any range over ManagementIndications().
		c.closeManagement()
		if c.listener != nil {
			c.listener.forget(c)
		}
		if c.endpoint != nil {
			c.endpoint.forgetAssociation(c)
		}
		if c.asReservation != nil {
			c.asReservation.rollback()
		}
	})
	return err
}

func (c *Association) closeTransport() error {
	if c.transportCloser != nil {
		return c.transportCloser()
	}
	if c.sctpConn != nil {
		return c.sctpConn.Close()
	}
	return nil
}

// LocalAddr returns the local network address.
func (c *Association) LocalAddr() net.Addr {
	return c.sctpConn.LocalAddr()
}

// RemoteAddr returns the remote network address.
func (c *Association) RemoteAddr() net.Addr {
	return c.sctpConn.RemoteAddr()
}

// SetDeadline sets the read and write deadlines associated.
func (c *Association) SetDeadline(t time.Time) error {
	c.setReadDeadline(t)
	return c.sctpConn.SetWriteDeadline(t)
}

// SetReadDeadline sets the deadline for future Read, ReadPD and ReadData calls.
// A zero time removes it. An expired deadline makes those calls return
// os.ErrDeadlineExceeded, which reports Timeout() true, and leaves the
// association usable so the caller can read again.
//
// The deadline is kept here rather than pushed down to the SCTP socket. The only
// reader of that socket is this package's own receive loop, which is meant to
// wait indefinitely for the peer; a deadline reaching it did not bound the
// caller's Read at all — Read has never consulted one — and instead expired the
// receive loop, whose error path closes the association. Setting a read deadline
// on an idle, healthy, ASP-ACTIVE association therefore tore it down, took the
// state to ASP-DOWN, and turned every subsequent Read and Write into
// ErrNotEstablished. That is the opposite of what net.Conn promises, where a
// read timeout is recoverable.
func (c *Association) SetReadDeadline(t time.Time) error {
	c.setReadDeadline(t)
	return nil
}

// setReadDeadline stores the deadline as Unix nanoseconds, with zero meaning
// none. time.Time has no atomic form, and a mutex here would be taken on every
// read of an association carrying traffic.
func (c *Association) setReadDeadline(t time.Time) {
	if t.IsZero() {
		c.readDeadline.Store(0)
		return
	}
	c.readDeadline.Store(t.UnixNano())
}

// readTimeout returns a channel that fires when the read deadline expires, and
// whether the deadline has already passed. A nil channel blocks forever, which
// is what "no deadline" means to a select.
func (c *Association) readTimeout() (<-chan time.Time, func(), bool) {
	d := c.readDeadline.Load()
	if d == 0 {
		return nil, func() {}, false
	}
	remaining := time.Until(time.Unix(0, d))
	if remaining <= 0 {
		return nil, func() {}, true
	}
	timer := time.NewTimer(remaining)
	return timer.C, func() { timer.Stop() }, false
}

// SetWriteDeadline sets the deadline for future Write calls.
func (c *Association) SetWriteDeadline(t time.Time) error {
	return c.sctpConn.SetWriteDeadline(t)
}

// State returns the current RFC 4666 ASP/IPSP state of the Association. For
// IPSP Double Exchange it is the remote IPSP state governing TrafficToPeer;
// IPSPState returns both independent directions.
func (c *Association) State() State {
	c.muState.RLock()
	defer c.muState.RUnlock()
	return c.state
}

// StreamID returns the outbound SCTP stream template. Association writes copy
// this template before selecting a message-specific stream; received stream
// identifiers are validated internally and are not exposed by this method.
func (c *Association) StreamID() uint16 {
	return c.sctpInfo.Stream
}

// PeerASPIdentifier returns the ASP Identifier the peer supplied in ASP Up.
// The boolean is false when the peer omitted the optional parameter.
func (c *Association) PeerASPIdentifier() (uint32, bool) {
	c.muPeerASPIdentifier.RLock()
	defer c.muPeerASPIdentifier.RUnlock()
	if c.peerASPIdentifier == nil {
		return 0, false
	}
	return c.peerASPIdentifier.AspIdentifier(), true
}

// configuredRoutingContexts returns the traffic flows this association is
// configured to carry. At an SGP that is the immutable per-peer authorization
// resolved at ASP Up, not the Listener's aggregate inventory.
func (c *Association) configuredRoutingContexts() []uint32 {
	return appendRoutingContexts(c.staticallyConfiguredRoutingContexts(), c.dynamicRoutingContexts(false))
}

func (c *Association) staticallyConfiguredRoutingContexts() []uint32 {
	if c == nil {
		return nil
	}
	if c.role == RoleSGP {
		c.muAuthorizedRCs.RLock()
		if c.authorizationResolved {
			configured := append([]uint32(nil), c.authorizedRCs...)
			c.muAuthorizedRCs.RUnlock()
			return configured
		}
		c.muAuthorizedRCs.RUnlock()
	}
	if c.isIPSPDoubleExchange() {
		return routingContextsFromIPSPTrafficConfig(c.cfg.IPSP.TrafficToPeer)
	}
	if c.cfg == nil || c.cfg.RoutingContexts == nil {
		return nil
	}
	return append([]uint32(nil), c.cfg.RoutingContexts.RoutingContexts()...)
}

func (c *Association) configuredLocalRoutingContexts() []uint32 {
	if c == nil {
		return nil
	}
	if c.isIPSPDoubleExchange() {
		return appendRoutingContexts(
			routingContextsFromIPSPTrafficConfig(c.cfg.IPSP.TrafficToLocal),
			c.dynamicRoutingContexts(true),
		)
	}
	return c.configuredRoutingContexts()
}

func routingContextsFromIPSPTrafficConfig(config *IPSPTrafficConfig) []uint32 {
	if config == nil || config.RoutingContexts == nil {
		return nil
	}
	return append([]uint32(nil), config.RoutingContexts.RoutingContexts()...)
}

func (c *Association) configuredRoutingContextParam() *params.Param {
	configured := c.configuredRoutingContexts()
	if len(configured) == 0 {
		return nil
	}
	return params.NewRoutingContext(configured...)
}

func (c *Association) configuredLocalRoutingContextParam() *params.Param {
	configured := c.configuredLocalRoutingContexts()
	if len(configured) == 0 {
		return nil
	}
	return params.NewRoutingContext(configured...)
}

func (c *Association) configuredASKeys() []ASKey {
	if c == nil {
		return nil
	}
	if c.hasExplicitlyEmptyASPAuthorization() && len(c.dynamicRoutingContexts(false)) == 0 {
		return nil
	}
	if c.isIPSPDoubleExchange() && !c.hasPeerIPSPTrafficDirection() {
		return nil
	}
	return c.asKeysForRoutingContexts(c.configuredRoutingContexts())
}

func (c *Association) staticallyConfiguredASKeys() []ASKey {
	if c == nil {
		return nil
	}
	routingContexts := c.staticallyConfiguredRoutingContexts()
	if len(routingContexts) == 0 {
		return nil
	}
	appearance, appearanceSet := appearanceOf(c.applicationServerNetworkAppearance())
	keys := make([]ASKey, 0, len(routingContexts))
	seen := make(map[uint32]struct{}, len(routingContexts))
	for _, routingContext := range routingContexts {
		if _, duplicate := seen[routingContext]; duplicate {
			continue
		}
		seen[routingContext] = struct{}{}
		keys = append(keys, ASKey{
			NetworkAppearance:    appearance,
			NetworkAppearanceSet: appearanceSet,
			RoutingContext:       routingContext,
			RoutingContextSet:    true,
		})
	}
	return keys
}

func (c *Association) asKeysForRoutingContexts(routingContexts []uint32) []ASKey {
	if c == nil {
		return nil
	}
	appearance, appearanceSet := appearanceOf(c.applicationServerNetworkAppearance())
	if len(routingContexts) == 0 {
		return []ASKey{{NetworkAppearance: appearance, NetworkAppearanceSet: appearanceSet}}
	}
	keys := make([]ASKey, 0, len(routingContexts))
	seen := make(map[ASKey]struct{}, len(routingContexts))
	for _, routingContext := range routingContexts {
		if dynamic, ok := c.dynamicASKey(routingContext, false); ok {
			if _, duplicate := seen[dynamic]; duplicate {
				continue
			}
			seen[dynamic] = struct{}{}
			keys = append(keys, dynamic)
			continue
		}
		key := ASKey{
			NetworkAppearance:    appearance,
			NetworkAppearanceSet: appearanceSet,
			RoutingContext:       routingContext,
			RoutingContextSet:    true,
		}
		if _, duplicate := seen[key]; duplicate {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	return keys
}

func appendRoutingContexts(configured, dynamic []uint32) []uint32 {
	combined := make([]uint32, 0, len(configured)+len(dynamic))
	seen := make(map[uint32]struct{}, len(configured)+len(dynamic))
	for _, routingContext := range configured {
		if _, duplicate := seen[routingContext]; duplicate {
			continue
		}
		seen[routingContext] = struct{}{}
		combined = append(combined, routingContext)
	}
	for _, routingContext := range dynamic {
		if _, duplicate := seen[routingContext]; duplicate {
			continue
		}
		seen[routingContext] = struct{}{}
		combined = append(combined, routingContext)
	}
	return combined
}

func (c *Association) addDynamicASKey(key ASKey, routingKey RoutingKey, local bool) {
	if c == nil || !key.RoutingContextSet {
		return
	}
	if !local && (c.role == RoleSGP || c.role == RoleIPSP) {
		c.muAckedRCs.Lock()
		c.muDynamicASKeys.Lock()
		_, existed := c.dynamicPeerASKeys[key.RoutingContext]
		if !existed && !c.activeRCsScoped {
			if c.inactiveDynamicRCs == nil {
				c.inactiveDynamicRCs = make(map[uint32]struct{})
			}
			c.inactiveDynamicRCs[key.RoutingContext] = struct{}{}
		}
		c.dynamicPeerASKeys[key.RoutingContext] = key
		if routingKey.TrafficModeSet {
			c.dynamicPeerTrafficModes[key.RoutingContext] = routingKey.TrafficMode
		}
		c.muDynamicASKeys.Unlock()
		c.muAckedRCs.Unlock()
		return
	}
	c.muDynamicASKeys.Lock()
	if local {
		c.dynamicLocalASKeys[key.RoutingContext] = key
		if routingKey.TrafficModeSet {
			c.dynamicLocalTrafficModes[key.RoutingContext] = routingKey.TrafficMode
		}
	} else {
		c.dynamicPeerASKeys[key.RoutingContext] = key
		if routingKey.TrafficModeSet {
			c.dynamicPeerTrafficModes[key.RoutingContext] = routingKey.TrafficMode
		}
	}
	c.muDynamicASKeys.Unlock()
}

func (c *Association) removeDynamicASKey(routingContext uint32, local bool) {
	if c == nil {
		return
	}
	if !local && (c.role == RoleSGP || c.role == RoleIPSP) {
		c.muAckedRCs.Lock()
		c.muDynamicASKeys.Lock()
		delete(c.dynamicPeerASKeys, routingContext)
		delete(c.dynamicPeerTrafficModes, routingContext)
		delete(c.inactiveDynamicRCs, routingContext)
		c.muDynamicASKeys.Unlock()
		c.muAckedRCs.Unlock()
		return
	}
	c.muDynamicASKeys.Lock()
	if local {
		delete(c.dynamicLocalASKeys, routingContext)
		delete(c.dynamicLocalTrafficModes, routingContext)
	} else {
		delete(c.dynamicPeerASKeys, routingContext)
		delete(c.dynamicPeerTrafficModes, routingContext)
	}
	c.muDynamicASKeys.Unlock()
}

func (c *Association) dynamicASKey(routingContext uint32, local bool) (ASKey, bool) {
	if c == nil {
		return ASKey{}, false
	}
	c.muDynamicASKeys.RLock()
	var key ASKey
	var ok bool
	if local {
		key, ok = c.dynamicLocalASKeys[routingContext]
	} else {
		key, ok = c.dynamicPeerASKeys[routingContext]
	}
	c.muDynamicASKeys.RUnlock()
	return key, ok
}

func (c *Association) dynamicRoutingContexts(local bool) []uint32 {
	if c == nil {
		return nil
	}
	c.muDynamicASKeys.RLock()
	target := c.dynamicPeerASKeys
	if local {
		target = c.dynamicLocalASKeys
	}
	routingContexts := make([]uint32, 0, len(target))
	for routingContext := range target {
		routingContexts = append(routingContexts, routingContext)
	}
	c.muDynamicASKeys.RUnlock()
	sort.Slice(routingContexts, func(i, j int) bool { return routingContexts[i] < routingContexts[j] })
	return routingContexts
}

func (c *Association) networkAppearanceForRoutingContext(routingContext *params.Param, local bool) *params.Param {
	networkAppearance, _, _ := c.resolveNetworkAppearanceScope(routingContext, local)
	return networkAppearance
}

func (c *Association) resolveNetworkAppearanceScope(
	routingContext *params.Param,
	local bool,
) (*params.Param, bool, error) {
	configured := c.outboundNetworkAppearance()
	if local {
		configured = c.localNetworkAppearance()
	}
	var contexts []uint32
	if routingContext != nil {
		contexts = routingContext.RoutingContexts()
	} else {
		contexts = c.configuredRoutingContexts()
		if local && c.isIPSPDoubleExchange() {
			contexts = c.configuredLocalRoutingContexts()
		}
	}
	if len(contexts) == 0 {
		return configured, false, nil
	}

	configuredValue, configuredSet := appearanceOf(configured)
	var resolvedValue uint32
	var resolvedSet bool
	var allNetworkAppearances bool
	resolved := false
	for _, routingContextValue := range contexts {
		value := configuredValue
		valueSet := configuredSet
		all := false
		if key, ok := c.dynamicASKey(routingContextValue, local); ok {
			value = key.NetworkAppearance
			valueSet = key.NetworkAppearanceSet
			all = !key.NetworkAppearanceSet
		}
		if !resolved {
			resolvedValue = value
			resolvedSet = valueSet
			allNetworkAppearances = all
			resolved = true
			continue
		}
		if allNetworkAppearances != all || resolvedSet != valueSet || resolvedSet && resolvedValue != value {
			// RFC 4666 Section 3.4 gives one Network Appearance parameter to
			// an SSNM message. Every Routing Context named by that message must
			// therefore resolve to the same appearance scope.
			return nil, false, ErrInvalidNetworkAppearance
		}
	}
	if allNetworkAppearances {
		return nil, true, nil
	}
	if !resolvedSet {
		return nil, false, nil
	}
	return params.NewNetworkAppearance(resolvedValue), false, nil
}

func (c *Association) applicationServerNetworkAppearance() *params.Param {
	if c == nil || c.cfg == nil {
		return nil
	}
	if c.isIPSPDoubleExchange() {
		if c.cfg.IPSP.TrafficToPeer == nil {
			return nil
		}
		return c.cfg.IPSP.TrafficToPeer.NetworkAppearance
	}
	return c.cfg.NetworkAppearance
}

func (c *Association) resolveASPAuthorization(identifier *params.Param) error {
	identifierSet := identifier != nil
	identifierValue := uint32(0)
	if identifierSet {
		identifierValue = identifier.AspIdentifier()
	}

	c.muAuthorizedRCs.RLock()
	if c.authorizationResolved {
		sameIdentity := c.authorizationIdentifierSet == identifierSet &&
			(!identifierSet || c.authorizationIdentifier == identifierValue)
		c.muAuthorizedRCs.RUnlock()
		if !sameIdentity {
			return ErrInvalidASPIdentifier
		}
		return nil
	}
	c.muAuthorizedRCs.RUnlock()

	configured := []uint32(nil)
	if c.cfg != nil && c.cfg.RoutingContexts != nil {
		configured = append(configured, c.cfg.RoutingContexts.RoutingContexts()...)
	}
	authorized := append([]uint32(nil), configured...)
	explicitAuthorization := c.cfg != nil && c.cfg.AuthorizeASP != nil
	if explicitAuthorization {
		var remoteAddress net.Addr
		if c.sctpConn != nil {
			remoteAddress = c.sctpConn.RemoteAddr()
		}
		authorized = append([]uint32(nil), c.cfg.AuthorizeASP(ASPIdentity{
			ASPIdentifier:    identifierValue,
			ASPIdentifierSet: identifierSet,
			RemoteAddr:       remoteAddress,
		})...)
	}

	configuredSet := make(map[uint32]struct{}, len(configured))
	for _, rtCtx := range configured {
		configuredSet[rtCtx] = struct{}{}
	}
	authorizedSet := make(map[uint32]struct{}, len(authorized))
	for _, rtCtx := range authorized {
		if _, exists := configuredSet[rtCtx]; !exists {
			return ErrInvalidASPIdentifier
		}
		authorizedSet[rtCtx] = struct{}{}
	}
	owned := make([]uint32, 0, len(authorizedSet))
	for _, rtCtx := range configured {
		if _, allowed := authorizedSet[rtCtx]; allowed {
			owned = append(owned, rtCtx)
		}
	}

	c.muAuthorizedRCs.Lock()
	if c.authorizationResolved {
		sameIdentity := c.authorizationIdentifierSet == identifierSet &&
			(!identifierSet || c.authorizationIdentifier == identifierValue)
		c.muAuthorizedRCs.Unlock()
		if !sameIdentity {
			return ErrInvalidASPIdentifier
		}
		return nil
	}
	c.authorizedRCs = owned
	c.authorizationResolved = true
	c.authorizationExplicit = explicitAuthorization
	c.authorizationIdentifier = identifierValue
	c.authorizationIdentifierSet = identifierSet
	c.muAuthorizedRCs.Unlock()
	return nil
}

func (c *Association) hasExplicitlyEmptyASPAuthorization() bool {
	c.muAuthorizedRCs.RLock()
	defer c.muAuthorizedRCs.RUnlock()
	return c.authorizationResolved && c.authorizationExplicit && len(c.authorizedRCs) == 0
}

func (c *Association) savePeerASPIdentifier(identifier *params.Param) {
	c.muPeerASPIdentifier.Lock()
	c.peerASPIdentifier = identifier.Copy()
	c.muPeerASPIdentifier.Unlock()
}

func (c *Association) peerASPIdentifierParam() *params.Param {
	c.muPeerASPIdentifier.RLock()
	defer c.muPeerASPIdentifier.RUnlock()
	return c.peerASPIdentifier.Copy()
}

// notePeerActivity records a successfully parsed M3UA message received with
// PPID 0 or M3UAPPID. RFC 4666 Section 4.3.4.6 treats that as evidence the peer
// is alive; callers must reject every other PPID and malformed octets first.
func (c *Association) notePeerActivity() {
	c.lastRecv.Store(time.Now().UnixNano())
}

// heardFromPeerSince reports whether any M3UA message arrived from the peer
// after t.
//
// Measured against a point in time rather than a rolling window: the message
// that proves the peer is alive typically arrives right at the start of the
// window, where "within the last T(beat)" lands on the boundary and decides the
// association's fate on a scheduling margin.
func (c *Association) heardFromPeerSince(t time.Time) bool {
	last := c.lastRecv.Load()
	if last == 0 {
		return false
	}
	return last > t.UnixNano()
}

// receivedStreamID reports the SCTP stream the message currently being handled
// arrived on.
func (c *Association) receivedStreamID() uint16 {
	return uint16(c.recvStream.Load())
}

// MaxMessageStreamID returns the highest negotiated SCTP stream ID DATA may use.
// Valid explicit DATA stream IDs run from 1 through this value; stream 0 is
// reserved for management, and a zero result means no DATA stream is available.
func (c *Association) MaxMessageStreamID() uint16 {
	return c.maxMessageStreamID
}

// maxSackDelayMillis is the ceiling RFC 9260 Section 6.2 puts on 'SACK.Delay'.
const maxSackDelayMillis = 500

// validateSackDelay refuses a delayed-SACK timer the current SCTP specification
// does not allow to be configured.
//
// RFC 9260 Section 6.2: "An implementation MUST NOT allow the maximum delay
// (protocol parameter 'SACK.Delay') to be configured to be more than 500 ms."
// There is no lower bound to enforce — the 200 ms in the same section is the
// target for generating an acknowledgement, not a floor, and zero is the
// documented way to disable the delay.
func validateSackDelay(sackDelay uint32) error {
	if sackDelay > maxSackDelayMillis {
		return fmt.Errorf("%w: %d ms requested", ErrSackDelayTooLarge, sackDelay)
	}
	return nil
}

// SetSCTPSACK sets the SCTP SACK timer configuration on an active association.
//
// sackDelay is the number of milliseconds for the delayed SACK timer. RFC 9260
// Section 6.2 allows any value up to 500 ms and forbids more; anything above
// that is refused with ErrSackDelayTooLarge rather than passed to the kernel.
//
// sackFrequency is the number of packets to receive before sending a SACK
// without waiting for the delay timer. Setting to 1 disables the delayed
// SACK algorithm.
//
// Note: sackDelay=0, sackFrequency=1 (disables delayed SACK)
func (c *Association) SetSCTPSACK(sackDelay, sackFrequency uint32) error {
	if err := validateSackDelay(sackDelay); err != nil {
		return err
	}
	return c.sctpConn.SetSackTimer(&sctp.SackTimer{
		SackDelay:     sackDelay,
		SackFrequency: sackFrequency,
	})
}

// SetSCTPNoDelay sets the SCTP_NODELAY option on an active association.
//
// When noDelay is true, the Nagle-like bundling algorithm is disabled and
// user messages are sent as soon as possible. When false, small messages
// may be bundled to improve throughput.
func (c *Association) SetSCTPNoDelay(noDelay bool) error {
	optval := 0
	if noDelay {
		optval = 1
	}
	return c.sctpConn.SetNoDelay(optval)
}

// streamFor maps a Signalling Link Selection value onto a data stream.
//
// RFC 4666 Section 1.4.7 (SCTP Stream Mapping): "Traffic that requires
// sequencing SHOULD be assigned to the same stream. To accomplish this,
// MTP3-User traffic may be assigned to individual streams based on, for
// example, the SLS value in the MTP3 Routing Label, subject of course to the
// maximum number of streams supported by the underlying SCTP association."
//
// SCTP guarantees ordering only within a stream, so every message sharing an
// SLS has to travel the same one. This used to pick a stream at random per
// message — from a math/rand source reseeded off the wall clock on every call —
// which spread a single SLS across streams and discarded the sequencing the
// same section asks for.
//
// maxMessageStreamID is the peer's outbound stream count less one, since
// stream 0 is not available to DATA (see writeStream). A peer that negotiates a
// single stream leaves it at 0, meaning there is no data stream at all; the
// zero returned here is refused by the write paths rather than put on the wire.
func (c *Association) streamFor(sls uint8) uint16 {
	if c.maxMessageStreamID <= 1 {
		return c.maxMessageStreamID
	}
	return uint16(sls)%c.maxMessageStreamID + 1
}

// checkDataStream refuses a stream DATA may not travel on.
//
// RFC 4666 Section 1.4.7 rule 1 is unconditional: "The DATA message MUST NOT be
// sent on stream 0." A peer that negotiates a single outbound stream therefore
// offers nowhere legal to send DATA, and the association is unusable for
// traffic; saying so is the only conformant answer, and beats quietly breaking
// the MUST. Section 1.4.7 also limits selection to the streams supported by the
// association, so an explicit stream above the negotiated maximum is reported
// as Invalid Stream Identifier rather than handed to the kernel.
func (c *Association) checkDataStream(streamID uint16) error {
	if streamID == 0 {
		return ErrNoDataStream
	}
	if streamID > c.maxMessageStreamID {
		return NewInvalidSCTPStreamIDError(streamID)
	}
	return nil
}

// setResumeTo records how far an ASP should climb when it next comes up.
func (c *Association) setResumeTo(s State) {
	c.muState.Lock()
	defer c.muState.Unlock()
	c.resumeTo = s
}

// SelectRoutingContext chooses which Routing Context outbound DATA names by
// default.
//
// It sets association state, so it suits a caller that carries one traffic flow
// per association. It is NOT a way to switch flows from message to message:
// where several goroutines write to one Association, a second goroutine's selection can
// land between this goroutine's selection and its write, and the payload goes
// out naming the other flow — a silent mis-identification of exactly the thing
// Section 3.3.1 requires the parameter to get right. Callers sending for more
// than one flow use the WithRoutingContext writes, which carry the context on
// the message it describes.
//
// It is needed only when several Routing Contexts are configured for the
// association. RFC 4666 Section 3.3.1 declares the parameter singular —
// "Routing Context: 32 bits (unsigned integer)" — and requires it to pick one
// flow out of several: "Where multiple Routing Keys and Routing Contexts are
// used across a common association, the Routing Context MUST be sent to
// identify the traffic flow, assisting in the internal distribution of Data
// messages." Sending the whole configured set instead identifies nothing.
//
// With one Routing Context configured there is nothing to choose and DATA uses
// it; with none, the parameter is omitted, which the same section permits:
// "Where a Routing Key has not been coordinated between the SGP and ASP,
// sending of Routing Context is not required."
//
// The context must be one of those configured for this association.
func (c *Association) SelectRoutingContext(rtCtx uint32) error {
	configured := c.configuredRoutingContexts()
	for _, rc := range configured {
		if rc == rtCtx {
			c.muState.Lock()
			c.selectedRC, c.selectedRCSet = rtCtx, true
			c.muState.Unlock()
			return nil
		}
	}
	return NewInvalidRoutingContextError(rtCtx)
}

// resolveRoutingContext returns the Routing Context parameter for one DATA:
// the flow the caller named for that message, or the association-wide selection
// when it named none.
func (c *Association) resolveRoutingContext(rtCtx *uint32) (*params.Param, error) {
	if rtCtx == nil {
		return c.dataRoutingContext()
	}
	return c.routingContextFor(*rtCtx)
}

// routingContextFor returns the Routing Context parameter naming one traffic
// flow, for a caller that has said which flow this message belongs to.
//
// Nothing is read from the Association's own selection, which is what makes it safe to
// call concurrently for different flows on one association.
func (c *Association) routingContextFor(rtCtx uint32) (*params.Param, error) {
	for _, rc := range c.configuredRoutingContexts() {
		if rc != rtCtx {
			continue
		}
		// Activation is per Routing Context (Section 4.3.4.3). At an ASP the SGP
		// must have acknowledged the context and no alternate may have taken it;
		// at an SGP this particular ASP must still be active in that AS.
		if !c.outboundRoutingContextActive(rtCtx) {
			return nil, ErrRoutingContextNotActive
		}
		return params.NewRoutingContext(rtCtx), nil
	}
	// Including the case where no Routing Key was coordinated at all: naming a
	// flow the association never agreed to carry is not the same as omitting
	// the parameter, and must not be silently downgraded to it.
	return nil, NewInvalidRoutingContextError(rtCtx)
}

func (c *Association) outboundRoutingContextActive(rtCtx uint32) bool {
	if c.role == RoleIPSP && c.isIPSPDoubleExchange() {
		return c.activeForRoutingContext(rtCtx)
	}
	if c.role == RoleSGP || c.role == RoleIPSP {
		return c.activeForRoutingContext(rtCtx) && !c.routingContextOverridden(rtCtx)
	}
	return c.routingContextAcked(rtCtx) && !c.routingContextOverridden(rtCtx)
}

// dataRoutingContext returns the Routing Context parameter to put on a DATA, or
// nil if none is due.
//
// It reports an error only in the case the RFC leaves no default for: several
// Routing Contexts coordinated on the association and none chosen, where any
// choice this package made would be a guess at which traffic flow the payload
// belongs to.
func (c *Association) dataRoutingContext() (*params.Param, error) {
	configured := c.configuredRoutingContexts()

	switch {
	case len(configured) == 0:
		// Conditional, and no Routing Key has been coordinated: omit it. A
		// zero-length parameter is not the same thing — it names no context
		// while still claiming to.
		return nil, nil
	case len(configured) == 1:
		return c.routingContextFor(configured[0])
	}

	c.muState.RLock()
	rc, ok := c.selectedRC, c.selectedRCSet
	c.muState.RUnlock()
	if !ok {
		return nil, ErrAmbiguousRoutingContext
	}
	return c.routingContextFor(rc)
}

// setState replaces the Association's state without going through the dispatcher.
// It exists for tests that need to place an Association in a given state directly.
func (c *Association) setState(s State) {
	unlockTransfer := c.lockASPTransferMutation()
	c.muState.Lock()
	c.state = s
	// Kept in step so a later transition measures itself against the state the
	// Association was actually placed in, not against a stale applied value.
	c.appliedState = s
	c.muState.Unlock()
	unlockTransfer()
	c.notifyASPRouteStateChanged()
}

// resumeAfterStrayAck reports whether a return to ASP-ACTIVE is owed because a
// stray acknowledgement pushed this Association out of it.
func (c *Association) resumeAfterStrayAck() bool {
	return c.resumeStray.Load()
}

// armResumeAfterStrayAck records that the next entry into ASP-INACTIVE was
// caused by a stray acknowledgement, so the entry action re-initiates.
func (c *Association) armResumeAfterStrayAck() {
	c.resumeStray.Store(true)
}

// clearResumeAfterStrayAck cancels an armed return.
func (c *Association) clearResumeAfterStrayAck() {
	c.resumeStray.Store(false)
}

// noteRoutingContextsAcked records the Routing Contexts an ASP Active Ack
// acknowledged. An Ack with no Routing Context parameter acknowledges whatever
// the association carries, so every configured context is marked.
func (c *Association) noteRoutingContextsAcked(acked *params.Param) {
	rcs := c.configuredLocalRoutingContexts()
	if acked != nil {
		if named := acked.RoutingContexts(); len(named) > 0 {
			rcs = named
		}
	}

	unlockTransfer := c.lockASPTransferMutation()
	c.muAckedRCs.Lock()
	changed := !c.ackedRCsScoped
	if c.ackedRCs == nil {
		c.ackedRCs = make(map[uint32]struct{})
	}
	c.ackedRCsScoped = true
	for _, rc := range rcs {
		if _, exists := c.ackedRCs[rc]; !exists {
			changed = true
		}
		if _, exists := c.overriddenRCs[rc]; exists {
			changed = true
		}
		c.ackedRCs[rc] = struct{}{}
		delete(c.overriddenRCs, rc)
	}
	c.muAckedRCs.Unlock()
	unlockTransfer()
	if changed {
		c.notifyASPRouteStateChanged()
	}
}

// routingContextAcked reports whether the peer has acknowledged this context,
// or whether no Ack has named any context yet, in which case there is nothing
// to withhold.
func (c *Association) routingContextAcked(rc uint32) bool {
	c.muAckedRCs.RLock()
	defer c.muAckedRCs.RUnlock()
	if !c.ackedRCsScoped {
		return true
	}
	_, ok := c.ackedRCs[rc]
	return ok
}

// forgetAckedRoutingContexts drops the acknowledged set, so a new activation
// starts from nothing. An override recorded against the old activation goes with
// it: the next ASP Active decides afresh which contexts this ASP may carry.
func (c *Association) forgetAckedRoutingContexts() {
	unlockTransfer := c.lockASPTransferMutation()
	c.forgetAckedRoutingContextsWithoutTransferBarrier()
	unlockTransfer()
	c.notifyASPRouteStateChanged()
}

func (c *Association) forgetAckedRoutingContextsWithoutTransferBarrier() {
	c.muAckedRCs.Lock()
	c.ackedRCs = nil
	c.ackedRCsScoped = false
	c.overriddenRCs = nil
	c.muAckedRCs.Unlock()
}

// noteRoutingContextsOverridden records that an alternate ASP has taken over
// these Routing Contexts.
//
// RFC 4666 Section 4.3.4.3 makes the receiving ASP "consider itself now in the
// ASP-INACTIVE state" when overridden, and Errata ID 2065 is about the scope of
// that: an ASP serving several Application Servers becomes inactive "only for
// that particular Application Server, rather than all of them". The ASP state
// machine here is per association rather than per Routing Context, so the
// association-wide move is kept only for an override that covers everything it
// carries; a partial override is recorded here instead, which stops traffic for
// those contexts and leaves the rest running.
//
// What this does not do is maintain a separate ASP state per Routing Context, as
// Section 4.3.1 describes. State() therefore still reports ASP-ACTIVE while some
// of the contexts are not sendable, and the per-context truth is what
// routingContextFor enforces on each write.
func (c *Association) noteRoutingContextsOverridden(rcs []uint32) {
	unlockTransfer := c.lockASPTransferMutation()
	c.muAckedRCs.Lock()
	if c.role == RoleIPSP && !c.isIPSPDoubleExchange() {
		// Single Exchange uses the same per-AS state in both directions. An
		// Alternate ASP Active Notify therefore makes each named context inactive,
		// not merely unsendable. Materialize an association-wide active set before
		// subtracting a partial override so later acknowledgements cannot count the
		// overridden context as the last active AS.
		if !c.activeRCsScoped {
			c.materializeUnscopedActiveRoutingContextsLocked()
		}
		for _, rc := range rcs {
			delete(c.activeRCs, rc)
		}
	}
	if c.overriddenRCs == nil {
		c.overriddenRCs = make(map[uint32]struct{})
	}
	for _, rc := range rcs {
		c.overriddenRCs[rc] = struct{}{}
	}
	c.muAckedRCs.Unlock()
	unlockTransfer()
	c.notifyASPRouteStateChanged()
}

func (c *Association) notifyASPRouteStateChanged() {
	if c == nil || c.role != RoleASP || c.endpoint == nil || c.endpoint.aspRoutes == nil {
		return
	}
	c.endpoint.aspRoutes.associationStateChanged(c)
}

// noteRoutingContextsActive records which Application Servers the peer has just
// been activated in. At an SGP the peer is an ASP; in IPSP Single Exchange the
// same state controls traffic in both directions. An empty set means every
// configured one, which is what an ASP Active naming no Routing Context asks
// for.
func (c *Association) noteRoutingContextsActive(rcs []uint32) {
	c.muAckedRCs.Lock()
	defer c.muAckedRCs.Unlock()

	if len(rcs) == 0 {
		c.activeRCs, c.activeRCsScoped = nil, false
		c.activeScopeInitialized = true
		c.contextlessASActive = false
		c.inactiveDynamicRCs = nil
		if !c.isIPSPDoubleExchange() {
			c.overriddenRCs = nil
		}
		return
	}
	if !c.activeRCsScoped && c.activeScopeInitialized {
		c.materializeUnscopedActiveRoutingContextsLocked()
	}
	if c.activeRCs == nil {
		c.activeRCs = make(map[uint32]struct{}, len(rcs))
	}
	c.activeRCsScoped = true
	c.activeScopeInitialized = true
	for _, rc := range rcs {
		c.activeRCs[rc] = struct{}{}
		delete(c.inactiveDynamicRCs, rc)
		if !c.isIPSPDoubleExchange() {
			delete(c.overriddenRCs, rc)
		}
	}
}

// noteRoutingContextsInactive records that this ASP has stood down in these
// Application Servers. An empty set means all of them.
func (c *Association) noteRoutingContextsInactive(rcs []uint32) {
	c.muAckedRCs.Lock()
	defer c.muAckedRCs.Unlock()

	if len(rcs) == 0 {
		c.activeRCs, c.activeRCsScoped = nil, true
		c.activeScopeInitialized = true
		c.contextlessASActive = false
		c.inactiveDynamicRCs = nil
		return
	}
	if !c.activeRCsScoped {
		// Was active everywhere; narrowing it needs the full set to subtract
		// from, so start from what the association carries.
		c.materializeUnscopedActiveRoutingContextsLocked()
	} else if c.activeRCs == nil {
		c.activeRCs = make(map[uint32]struct{})
	}
	c.activeRCsScoped = true
	c.activeScopeInitialized = true
	c.inactiveDynamicRCs = nil
	for _, rc := range rcs {
		delete(c.activeRCs, rc)
	}
}

// forgetActiveRoutingContexts drops the record, so a peer returning to
// ASP-INACTIVE or ASP-DOWN starts again from nothing.
func (c *Association) forgetActiveRoutingContexts() {
	c.muAckedRCs.Lock()
	c.activeRCs, c.activeRCsScoped = nil, false
	c.activeScopeInitialized = false
	c.contextlessASActive = false
	c.inactiveDynamicRCs = nil
	c.muAckedRCs.Unlock()
}

func (c *Association) materializeUnscopedActiveRoutingContextsLocked() {
	c.activeRCs = make(map[uint32]struct{})
	for _, routingContext := range c.configuredRoutingContexts() {
		if _, inactive := c.inactiveDynamicRCs[routingContext]; inactive {
			continue
		}
		c.activeRCs[routingContext] = struct{}{}
	}
	c.activeRCsScoped = true
	c.activeScopeInitialized = true
	c.contextlessASActive = c.hasStaticallyConfiguredContextlessAS()
	c.inactiveDynamicRCs = nil
}

func (c *Association) hasStaticallyConfiguredContextlessAS() bool {
	if c == nil || c.hasExplicitlyEmptyASPAuthorization() {
		return false
	}
	if c.isIPSPDoubleExchange() && !c.hasPeerIPSPTrafficDirection() {
		return false
	}
	return len(c.staticallyConfiguredRoutingContexts()) == 0
}

// activeForRoutingContext reports whether this ASP is ASP-ACTIVE in the
// Application Server serving rtCtx. With nothing recorded it is active in all of
// them, which is the answer for an ASP Active that named no context.
func (c *Association) activeForRoutingContext(rtCtx uint32) bool {
	c.muAckedRCs.RLock()
	defer c.muAckedRCs.RUnlock()

	if !c.activeRCsScoped {
		_, inactive := c.inactiveDynamicRCs[rtCtx]
		return !inactive
	}
	_, ok := c.activeRCs[rtCtx]
	return ok
}

func (c *Association) activeForASKey(key ASKey) bool {
	if !key.RoutingContextSet {
		c.muAckedRCs.RLock()
		defer c.muAckedRCs.RUnlock()
		return !c.activeRCsScoped || c.contextlessASActive
	}
	return c.activeForRoutingContext(key.RoutingContext)
}

// routingContextOverridden reports whether an alternate ASP holds this context.
func (c *Association) routingContextOverridden(rc uint32) bool {
	c.muAckedRCs.RLock()
	defer c.muAckedRCs.RUnlock()
	_, ok := c.overriddenRCs[rc]
	return ok
}

func (c *Association) peerRoutingContextOverridden(rc uint32) bool {
	if c.isIPSPDoubleExchange() {
		return false
	}
	return c.routingContextOverridden(rc)
}

// Shutdown ends the association the orderly way, then closes it.
//
// RFC 4666 Section 4.9 offers a node two ways to stop communicating:
//
//	a) Send the sequence of ASP-INACTIVE, DEREG (optionally whenever
//	   dynamic registration is used), and ASP-DOWN messages and perform
//	   the SCTP Shutdown procedure after that.
//
//	b) Just do the SCTP Shutdown procedure.
//
// Close is (b), which is why it stays abrupt. Shutdown is (a): the peer is told
// traffic is stopping and that this ASP is going down before the association
// disappears underneath it, so it can move traffic rather than discover the
// loss from a vanished socket. DEREG is not sent, since this library does not
// support dynamic registration and the RFC makes it optional in any case.
//
// Each request completes its T(ack) procedure before the next is sent. Use
// ShutdownContext to bound the operation more tightly than the configured
// retry budget.
func (c *Association) Shutdown() error {
	return c.ShutdownContext(context.Background())
}

// ShutdownContext is Shutdown with caller-controlled cancellation.
// Cancellation stops the outstanding T(ack) request and still releases SCTP.
func (c *Association) ShutdownContext(ctx context.Context) error {
	if ctx == nil {
		return errors.New("nil shutdown context")
	}
	if !c.terminating.CompareAndSwap(false, true) {
		select {
		case <-c.done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		return c.finishTermination(err)
	}

	// ASP Inactive and ASP Down are initiated by an ASP or, under RFC 4666
	// Sections 4.3.4.1.2 and 4.3.4.4.1, by either IPSP. An SGP has no mirror
	// request to send; Section 4.9's orderly option for it is SCTP Shutdown.
	if c.role != RoleASP && c.role != RoleIPSP {
		return c.Close()
	}

	state := c.State()
	if c.isIPSPDoubleExchange() {
		state = c.localIPSPStateValue()
	}
	if state == StateASPActive {
		routingContext := c.cfg.RoutingContexts
		if c.isIPSPDoubleExchange() {
			routingContext = c.configuredLocalRoutingContextParam()
		}
		request, err := c.beginASPInactive(routingContext)
		if err == nil {
			err = c.waitTAck(ctx, request)
		}
		if err != nil {
			return c.finishTermination(err)
		}
	}

	if state == StateASPActive || state == StateASPInactive {
		request, err := c.initiateASPDown()
		if err == nil {
			err = c.waitTAck(ctx, request)
		}
		if err != nil {
			return c.finishTermination(err)
		}
	}

	return c.Close()
}

func (c *Association) finishTermination(cause error) error {
	closeErr := c.closeWith(cause)
	if closeErr != nil {
		return errors.Join(cause, closeErr)
	}
	return cause
}

// StateChanges reports every ASP state transition this association makes, in
// order, and is closed when the association ends. For IPSP Double Exchange it
// reports the remote IPSP transitions governing TrafficToPeer; query IPSPState
// when both independent directions matter.
//
// RFC 4666 Section 1.6.3 gives Layer Management an indication for each of these
// transitions -- M-ASP_UP, M-ASP_DOWN, M-ASP_ACTIVE and M-ASP_INACTIVE, in
// their confirm and indication forms -- and Section 4.2.1 has the M3UA layer
// invoke them "upon successful state changes". State() could only answer the
// question when asked, so a management layer had to poll for edges the library
// already knew about, and a transition that came and went between two polls was
// simply lost.
//
// Delivery never stalls the dispatcher. A reader that allows the bounded queue
// to fill closes the association with ErrIndicationQueueFull, rather than
// receiving a partial transition history. Read it in a dedicated goroutine if
// every edge matters.
//
//	go func() {
//		for st := range association.StateChanges() {
//			log.Printf("ASP state: %v", st)
//		}
//		// the channel closes with the association
//	}()
func (c *Association) StateChanges() <-chan State {
	return c.stateEventChan
}

// notifyStateChange delivers a transition without blocking, and cannot send on
// a channel that Close has already closed.
func (c *Association) notifyStateChange(s State) {
	c.muStateEvent.Lock()
	defer c.muStateEvent.Unlock()

	if c.stateEventClosed {
		return
	}

	select {
	case c.stateEventChan <- s:
	default:
		c.closeForIndicationOverflow()
	}
}

func (c *Association) closeForIndicationOverflow() {
	if c != nil && c.indicationOverflow.CompareAndSwap(false, true) {
		go func() { _ = c.closeWith(ErrIndicationQueueFull) }()
	}
}

// closeStateChanges closes stateEventChan exactly once, so a caller ranging
// over StateChanges() sees the association end rather than parking forever.
func (c *Association) closeStateChanges() {
	c.muStateEvent.Lock()
	defer c.muStateEvent.Unlock()

	if c.stateEventClosed {
		return
	}
	c.stateEventClosed = true
	close(c.stateEventChan)
}

// AssociationStatus is the SCTP-level status of the association underneath an
// M3UA Association.
//
// RFC 4666 Section 4.2 describes M-SCTP_STATUS as a Layer Management query that
// "supports a Layer Management query of the local status of a particular SCTP
// association", answered by mapping the SCTP layer's own status into a confirm
// primitive; no peer protocol is involved. The values below are that status,
// read from the association at the moment of the call.
//
// Round-trip time and retransmission timeout describe the primary path only.
// SCTP associations are multi-homed, and a secondary path can be in a quite
// different condition; this reports the path traffic is actually taking.
type AssociationStatus struct {
	// State is the association's SCTP state, as a name (for example
	// "ESTABLISHED"). RFC 9260 Section 4 defines the state machine.
	State string
	// ReceiverWindow is the peer's advertised receiver window, in octets.
	ReceiverWindow uint32
	// UnackedDataChunks is how many DATA chunks this end has sent and not yet
	// had acknowledged. A number that stays high is the peer, or the path, not
	// keeping up.
	UnackedDataChunks uint16
	// PendingDataChunks is how many DATA chunks are queued here awaiting a
	// send, which is this end's own backlog rather than the peer's.
	PendingDataChunks uint16
	// InboundStreams and OutboundStreams are the stream counts negotiated at
	// association setup. OutboundStreams bounds the stream a message may be
	// sent on; see MaxMessageStreamID.
	InboundStreams  uint16
	OutboundStreams uint16
	// FragmentationPoint is the largest payload that will be sent without
	// being fragmented across DATA chunks, in octets.
	FragmentationPoint uint32
	// PrimaryCongestionWindow is the primary path's congestion window, in
	// octets.
	PrimaryCongestionWindow uint32
	// PrimarySmoothedRTT is the primary path's smoothed round-trip time.
	PrimarySmoothedRTT time.Duration
	// PrimaryRetransmissionTimeout is the primary path's current RTO. It is the
	// delay before an unacknowledged chunk is resent, so it bounds how long a
	// lost message can go unnoticed.
	PrimaryRetransmissionTimeout time.Duration
	// PrimaryMTU is the primary path's maximum transmission unit, in octets.
	PrimaryMTU uint32
}

// associationStateName renders an SCTP association state as the name the
// kernel's enum gives it.
//
// Written against the named constants rather than the numbers: the enum in
// linux/sctp.h begins at SCTP_EMPTY, not SCTP_CLOSED, so a table numbered from
// CLOSED = 0 is off by one throughout and reports an established association as
// COOKIE_ECHOED. The dependency documents that trap on the constant block.
func associationStateName(s sctp.StatusState) string {
	switch s {
	case sctp.SCTP_EMPTY:
		return "EMPTY"
	case sctp.SCTP_CLOSED:
		return "CLOSED"
	case sctp.SCTP_COOKIE_WAIT:
		return "COOKIE-WAIT"
	case sctp.SCTP_COOKIE_ECHOED:
		return "COOKIE-ECHOED"
	case sctp.SCTP_ESTABLISHED:
		return "ESTABLISHED"
	case sctp.SCTP_SHUTDOWN_PENDING:
		return "SHUTDOWN-PENDING"
	case sctp.SCTP_SHUTDOWN_SENT:
		return "SHUTDOWN-SENT"
	case sctp.SCTP_SHUTDOWN_RECEIVED:
		return "SHUTDOWN-RECEIVED"
	case sctp.SCTP_SHUTDOWN_ACK_SENT:
		return "SHUTDOWN-ACK-SENT"
	default:
		return fmt.Sprintf("unknown(%d)", int32(s))
	}
}

// AssociationStatus reports the SCTP association's own status, which is what
// RFC 4666 Section 4.2 calls an M-SCTP_STATUS request and its confirm. No M3UA
// peer protocol is invoked: the answer comes from the local SCTP layer.
//
// setUpSocket has always read this at construction and kept only the negotiated
// outbound stream count, discarding the rest, so an operator had no way to see
// the round-trip time, the retransmission timeout, or the queue depths of an
// association they were running. Those are exactly the numbers that distinguish
// a link that is slow from one that is failing.
//
// It returns an error once the association is gone.
func (c *Association) AssociationStatus() (*AssociationStatus, error) {
	if c.sctpConn == nil {
		return nil, ErrAssociationClosed
	}

	r, err := c.sctpConn.GetStatus()
	if err != nil {
		return nil, err
	}

	return &AssociationStatus{
		State:                        associationStateName(r.State),
		ReceiverWindow:               r.RWND,
		UnackedDataChunks:            r.Unackdata,
		PendingDataChunks:            r.Penddata,
		InboundStreams:               r.Instreams,
		OutboundStreams:              r.Ostreams,
		FragmentationPoint:           r.FragmentationPoint,
		PrimaryCongestionWindow:      r.PrimaryPeerAddr.CWND,
		PrimarySmoothedRTT:           time.Duration(r.PrimaryPeerAddr.SRTT) * time.Millisecond,
		PrimaryRetransmissionTimeout: time.Duration(r.PrimaryPeerAddr.RTO) * time.Millisecond,
		PrimaryMTU:                   r.PrimaryPeerAddr.MTU,
	}, nil
}

// ManagementIndicationKind identifies which of RFC 4666's Layer Management
// indications a ManagementIndication carries.
type ManagementIndicationKind uint8

const (
	// ManagementNotify is the M-NOTIFY indication of RFC 4666 Section 1.6.3:
	// "M3UA reports that it has received a Notify message from its peer."
	ManagementNotify ManagementIndicationKind = iota
	// ManagementError is the M-ERROR indication: "M3UA reports that it has
	// received an Error message from its peer or that a local operation has
	// been unsuccessful."
	ManagementError
	// ManagementSCTPRestart is the M-SCTP_RESTART indication: "M3UA informs LM
	// that an SCTP restart indication has been received."
	ManagementSCTPRestart
	// ManagementSCTPRelease is the M-SCTP_RELEASE indication/confirm emitted
	// when the association is released locally, remotely, or by failure.
	ManagementSCTPRelease
)

// String names the indication as Section 1.6.3 does.
func (k ManagementIndicationKind) String() string {
	switch k {
	case ManagementNotify:
		return "M-NOTIFY"
	case ManagementError:
		return "M-ERROR"
	case ManagementSCTPRestart:
		return "M-SCTP_RESTART"
	case ManagementSCTPRelease:
		return "M-SCTP_RELEASE"
	default:
		return fmt.Sprintf("unknown(%d)", uint8(k))
	}
}

func causeDescription(cause error) string {
	if cause == nil {
		return "SCTP association released"
	}
	return cause.Error()
}

// ManagementIndication is one report from the M3UA layer to Layer Management.
//
// Which fields carry meaning depends on Kind; the rest are zero. Description is
// always set, and names the value as the RFC names it, so an indication can be
// logged without a lookup table.
type ManagementIndication struct {
	Kind ManagementIndicationKind

	// StatusType and StatusInfo are the two halves of the Status parameter of
	// a received Notify (RFC 4666 Section 3.8.2). Set for ManagementNotify.
	StatusType uint16
	StatusInfo uint16

	// ErrorCode is the Error Code of a received Error message (Section 3.8.1).
	// Set for ManagementError.
	ErrorCode uint32

	// RoutingContexts is the complete Application Server scope of the
	// indication. For ManagementNotify it contains every explicitly named
	// Routing Context, or the configured AS memberships inferred under RFC 4666
	// Section 4.3.4.5 when NTFY omitted the parameter. For ManagementError it
	// contains every Routing Context the peer explicitly named. The slice is
	// owned by the indication and may be retained or modified by the caller.
	RoutingContexts []uint32

	// RoutingContext is the first explicitly named traffic flow, valid only
	// when RoutingContextSet is true. It is retained for source compatibility;
	// RoutingContexts carries the complete explicit or inferred scope.
	//
	// It is what makes the indication actionable on an association carrying
	// more than one Application Server. RFC 4666 Errata ID 2065 asks for the
	// parameter to be Conditional rather than Optional in Notify for exactly
	// this reason: when a second ASP becomes active for one of several
	// Application Servers, the Notify names which one, "allow[ing] the first
	// ASP to become inactive only for that particular Application Server,
	// rather than all of them". Section 3.8.1 goes further for Error and makes
	// it Mandatory for specific codes — an "Invalid Routing Context" error
	// carries the context that was invalid.
	RoutingContext    uint32
	RoutingContextSet bool

	// ASPIdentifier is the ASP the indication concerns, valid only when
	// ASPIdentifierSet is true. Set for ManagementNotify: Section 3.8.2 lists
	// it Conditional, and an "Alternate ASP Active" notification uses it to name
	// the ASP that took the traffic over.
	ASPIdentifier    uint32
	ASPIdentifierSet bool

	// NetworkAppearance is the network the indication concerns, valid only when
	// NetworkAppearanceSet is true. Section 3.8.1 makes it Mandatory for the
	// "Invalid Network Appearance" error, which "MUST be included in the
	// Network Appearance parameter".
	NetworkAppearance    uint32
	NetworkAppearanceSet bool

	// AffectedPointCodes are the destinations an Error concerns, empty when it
	// named none. Section 3.8.1 on "Destination Status Unknown": "the invalid
	// or unauthorized Point Code(s) MUST be included along with the Network
	// Appearance and/or Routing Context associated with the Point Code(s)."
	AffectedPointCodes []uint32

	// Description names the status or the error code in the RFC's own words.
	Description string
}

// firstRoutingContext projects a management message's first explicitly named
// Routing Context into the compatibility fields on ManagementIndication. The
// complete explicit or inferred scope is carried by RoutingContexts.
//
// The bool means the value was decoded, not merely that a parameter was
// present: a value that is not a whole number of 32-bit words yields nothing,
// and reporting a zero for it would be indistinguishable from Routing Context 0,
// which a peer may legitimately use.
func firstRoutingContext(p *params.Param) (uint32, bool) {
	if p == nil {
		return 0, false
	}
	rcs := p.RoutingContexts()
	if len(rcs) == 0 {
		return 0, false
	}
	return rcs[0], true
}

func routingContextsOf(p *params.Param) []uint32 {
	if p == nil {
		return nil
	}
	return append([]uint32(nil), p.RoutingContexts()...)
}

// uint32ParamOf reports a single-word parameter's value, and whether it decoded.
// The accessors return zero for a mismatched tag, which is a real value for
// every one of these fields, so the tag is checked here instead.
func uint32ParamOf(p *params.Param, tag uint16, read func(*params.Param) uint32) (uint32, bool) {
	if p == nil || p.Tag != tag {
		return 0, false
	}
	return read(p), true
}

// ManagementIndications reports what RFC 4666 Section 1.6.3 calls the M3UA to
// Layer Management indications, and is closed when the association ends.
//
// Section 4.2: "M-NOTIFY indication and M-ERROR indication primitives indicate
// to Layer Management the notification or error information contained in a
// received M3UA Notify or Error message, respectively." Both messages were
// decoded correctly and then written to a log line and dropped, so the
// information reached an operator reading logs and nothing else: an application
// could not see that the peer had reported AS-INACTIVE, or Insufficient ASP
// Resources, or refused something with an error code.
//
// Delivery never stalls the dispatcher. A reader that allows the bounded queue
// to fill closes the association with ErrIndicationQueueFull, rather than
// silently losing M-NOTIFY or M-ERROR.
//
//	go func() {
//		for ind := range association.ManagementIndications() {
//			log.Printf("%v: %s", ind.Kind, ind.Description)
//		}
//	}()
func (c *Association) ManagementIndications() <-chan *ManagementIndication {
	return c.mgmtChan
}

// notifyManagement delivers an indication without blocking, and cannot send on
// a channel that Close has already closed.
func (c *Association) notifyManagement(ind *ManagementIndication) {
	c.muMgmt.Lock()
	defer c.muMgmt.Unlock()

	if c.mgmtClosed {
		return
	}

	select {
	case c.mgmtChan <- ind:
	default:
		c.closeForIndicationOverflow()
	}
}

// closeManagement closes mgmtChan exactly once, so a caller ranging over
// ManagementIndications() sees the association end.
func (c *Association) closeManagement() {
	c.muMgmt.Lock()
	defer c.muMgmt.Unlock()

	if c.mgmtClosed {
		return
	}
	c.mgmtClosed = true
	close(c.mgmtChan)
}
