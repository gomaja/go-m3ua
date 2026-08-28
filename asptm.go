// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"crypto/rand"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func (c *Association) initiateASPTM() error {
	return c.initiateASPActive(c.configuredRoutingContextParam())
}

func (c *Association) initiateASPActive(routingContext *params.Param) error {
	requests, err := c.aspActiveRequests(routingContext)
	if err != nil {
		return err
	}
	pending := make([]*pendingRequest, 0, len(requests))
	for _, activeRequest := range requests {
		aspActive := messages.NewAspActive(
			activeRequest.trafficMode.Copy(), activeRequest.routingContext.Copy(), nil,
		)
		request := c.startTAck(aspActive, requestAspActive)
		pending = append(pending, request)
		if _, err := c.WriteSignal(aspActive); err != nil {
			for _, started := range pending {
				c.cancelTAckRequest(started)
			}
			return err
		}
	}
	return nil
}

type aspActiveRequest struct {
	trafficMode    *params.Param
	routingContext *params.Param
}

func (c *Association) aspActiveRequests(routingContext *params.Param) ([]aspActiveRequest, error) {
	trafficModes := c.trafficModePolicy()
	var routingContexts []uint32
	omitted := routingContext == nil
	if routingContext != nil {
		routingContexts = routingContext.RoutingContexts()
	} else {
		routingContexts = c.configuredRoutingContexts()
	}

	if len(routingContexts) == 0 {
		trafficMode := trafficModes.defaultParam()
		if trafficMode != nil && !validTrafficMode(trafficMode.TrafficModeType()) {
			return nil, ErrUnsupportedTrafficMode
		}
		return []aspActiveRequest{{trafficMode: trafficMode, routingContext: routingContext.Copy()}}, nil
	}

	type modeKey struct {
		value uint32
		set   bool
	}
	type group struct {
		key             modeKey
		routingContexts []uint32
	}
	groups := make([]group, 0, len(routingContexts))
	indices := make(map[modeKey]int)
	for _, rtCtx := range routingContexts {
		mode, configured := trafficModes.configured(rtCtx)
		if configured && !validTrafficMode(mode) {
			return nil, ErrUnsupportedTrafficMode
		}
		key := modeKey{value: mode, set: configured}
		index, exists := indices[key]
		if !exists {
			index = len(groups)
			indices[key] = index
			groups = append(groups, group{key: key})
		}
		groups[index].routingContexts = append(groups[index].routingContexts, rtCtx)
	}

	requests := make([]aspActiveRequest, 0, len(groups))
	for _, grouped := range groups {
		var trafficMode *params.Param
		if grouped.key.set {
			trafficMode = params.NewTrafficModeType(grouped.key.value)
		}
		var scoped *params.Param
		if !omitted || len(groups) > 1 {
			scoped = params.NewRoutingContext(grouped.routingContexts...)
		}
		requests = append(requests, aspActiveRequest{
			trafficMode:    trafficMode,
			routingContext: scoped,
		})
	}
	return requests, nil
}

func (c *Association) initiateASPInactive(routingContext *params.Param) error {
	_, err := c.beginASPInactive(routingContext)
	return err
}

func (c *Association) beginASPInactive(routingContext *params.Param) (*pendingRequest, error) {
	aspInactive := messages.NewAspInactive(routingContext.Copy(), nil)
	request := c.startTAck(aspInactive, requestAspInactive)
	if _, err := c.WriteSignal(aspInactive); err != nil {
		c.cancelTAckRequest(request)
		return nil, err
	}
	return request, nil
}

// ActivateRoutingContexts starts the ASP Active procedure for the named
// Application Servers. With no arguments it requests every configured Routing
// Context. The request remains protected by T(ack) until all scoped Acks arrive.
func (c *Association) ActivateRoutingContexts(routingContexts ...uint32) error {
	if c.role != RoleASP {
		return ErrUnsupportedRole
	}
	if state := c.State(); state != StateASPInactive && state != StateASPActive {
		return ErrInvalidState
	}
	routingContext, err := c.outboundRoutingContexts(routingContexts)
	if err != nil {
		return err
	}
	return c.initiateASPActive(routingContext)
}

// DeactivateRoutingContexts starts the ASP Inactive procedure for the named
// Application Servers. With no arguments it requests every configured Routing
// Context. Local traffic eligibility changes only as the scoped Acks arrive.
func (c *Association) DeactivateRoutingContexts(routingContexts ...uint32) error {
	if c.role != RoleASP {
		return ErrUnsupportedRole
	}
	if c.State() != StateASPActive {
		return ErrInvalidState
	}
	routingContext, err := c.outboundRoutingContexts(routingContexts)
	if err != nil {
		return err
	}
	return c.initiateASPInactive(routingContext)
}

func (c *Association) outboundRoutingContexts(routingContexts []uint32) (*params.Param, error) {
	if len(routingContexts) == 0 {
		return c.configuredRoutingContextParam(), nil
	}
	routingContext := params.NewRoutingContext(routingContexts...)
	if err := c.validateRoutingContext(routingContext); err != nil {
		return nil, err
	}
	return routingContext, nil
}

// setBeatData registers the Heartbeat Data whose echo the next BEAT Ack must
// carry. heartbeat() and the dispatch goroutine run concurrently, so all
// access goes through muBeat.
func (c *Association) setBeatData(data []byte) {
	c.muBeat.Lock()
	c.beatData = data
	c.muBeat.Unlock()
}

// currentBeatData returns the outstanding Heartbeat Data, or an empty slice
// when no BEAT is awaiting its Ack.
func (c *Association) currentBeatData() []byte {
	c.muBeat.RLock()
	defer c.muBeat.RUnlock()
	return c.beatData
}

// consumeBeatData clears the outstanding Heartbeat Data, reporting whether
// this call was the one that cleared it. An Ack answers exactly one BEAT, so
// the echo is retired the moment it is accepted: leaving it registered would
// let a peer replay a captured Ack to satisfy every later T(beat) and keep a
// dead link looking alive indefinitely.
func (c *Association) consumeBeatData() bool {
	c.muBeat.Lock()
	defer c.muBeat.Unlock()
	if len(c.beatData) == 0 {
		return false
	}
	c.beatData = nil
	return true
}

// heartbeat runs the RFC 4666 M3UA BEAT/BEAT Ack liveness loop. SCTP
// HEARTBEAT chunks are transport-layer path management and are not handled here.
func (c *Association) heartbeat(ctx context.Context) {
	c.beatAllow.Wait()
	if !c.hb.Enabled {
		return
	}
	for {
		// A fresh slice every round: the previous round's data may still be
		// under comparison in a concurrent handleHeartbeatAck.
		data := make([]byte, 128)
		if _, err := rand.Read(data); err != nil {
			c.sendErr(err)
			return
		}
		// Register the expected echo before the BEAT can reach the wire, so
		// an Ack racing straight back is never compared against stale data.
		c.setBeatData(data)
		sentAt := time.Now()
		if _, err := c.WriteSignal(
			messages.NewHeartbeat(params.NewHeartbeatData(data)),
		); err != nil {
			c.sendErr(ErrFailedToWriteSignal)
			return
		}

		// wait for response
		select {
		case <-ctx.Done():
			return
		case <-c.done:
			return
		case _, ok := <-c.beatAckChan: // got valid BEAT response from peer
			if !ok {
				return
			}
		case <-time.After(c.hb.Timer): // timer expired
			// RFC 4666 Section 4.3.4.6: "If no Heartbeat Ack message (or any
			// other M3UA message) is received from the M3UA peer within
			// 2*T(beat), the remote M3UA peer is considered unavailable."
			//
			// The parenthetical is the point. A BEAT Ack is one small message
			// competing with everything else on the association, so treating
			// its absence as proof of death tore down busy links that were
			// demonstrably alive — and the busier the link, the likelier it was.
			if c.heardFromPeerSince(sentAt) {
				break
			}
			c.sendErr(ErrHeartbeatExpired)
			return
		}

		// wait while next time
		select {
		case <-ctx.Done():
			return
		case <-c.done:
			return
		case <-time.After(c.hb.Interval):
			continue
		}
	}
}

// handleAspActive answers an incoming ASP Active.
//
// RFC 4666 Section 4.3.4.3 states "Independently of the RC, the SGP MUST send
// an ASP Active Ack message in response to a received ASP Active message from
// the ASP, if the ASP is already marked in the ASP-ACTIVE state." Withholding
// the Ack because the ASP is already ASP-ACTIVE leaves the peer retransmitting
// on T(ack) forever — the same interop deadlock ASP Up suffered.
//
// These are SGP procedures ("from the ASP"), so an ASP that receives an ASP
// Active reports an Error and holds its state rather than acking.
func (c *Association) handleAspActive(aspActive *messages.AspActive) error {
	if c.role != RoleSGP {
		return NewUnexpectedMessageError(aspActive)
	}

	// Figure 3, the ASP state transition diagram of Section 4.3.1, has exactly
	// one edge out of ASP-DOWN — ASPUP/[ASPUP-Ack], to ASP-INACTIVE — so an ASP
	// Active from an ASP-DOWN peer names a transition it does not define.
	// Section 4.3.1 also forbids the reply: such a peer "SHOULD NOT be sent any
	// M3UA messages, with the exception of Heartbeat, ASP Down Ack, and Error
	// messages". This is checked before the Ack is built, because the Ack used
	// to go out first and only then draw the state error, acknowledging a peer
	// that had never sent an ASP Up into carrying traffic.
	if c.State() == StateASPDown {
		return NewUnexpectedMessageError(aspActive)
	}
	if err := validateRoutingContextShape(aspActive.RoutingContext); err != nil {
		return err
	}

	// Section 4.7: "Upon receiving an ASP Active message for an affected AS
	// while still partially isolated from the NIF, the SGP should respond with
	// an Error ("Refused - Management Blocking")." Acknowledging it would
	// promise traffic the SGP has no route for.
	requested := c.configuredRoutingContexts()
	if aspActive.RoutingContext != nil {
		if named := aspActive.RoutingContext.RoutingContexts(); len(named) > 0 {
			requested = named
		}
	}
	if !c.nif.servicableASKeys(c.asKeysForRoutingContexts(requested)) {
		return ErrManagementBlocking
	}

	// Section 4.3.4.3: "If the SGP determines that the mode indicated in an ASP
	// Active message is unsupported or incompatible with the mode currently
	// configured for the AS, the SGP responds with an Error message
	// ('Unsupported / Invalid Traffic Handling Mode')." The mode is validated
	// against local configuration before the Ack, so an incompatible peer is
	// refused rather than silently acked with our own mode echoed back.
	if err := c.validateTrafficMode(aspActive.TrafficModeType); err != nil {
		return err
	}

	// The Ack names the contexts the ASP asked about, not this SGP's whole
	// inventory. See answerRoutingContexts.
	served, rcErr := c.answerRoutingContexts(aspActive.RoutingContext, NewNoConfiguredASError, NewInvalidRoutingContextError)
	acknowledged := served
	if served != nil {
		if err := c.validateTrafficModeForRoutingContexts(
			aspActive.TrafficModeType, served.RoutingContexts(),
		); err != nil {
			return err
		}
	}

	// Section 4.3.4.3: "Independently of the RC, the SGP MUST send an ASP Active
	// Ack message in response to a received ASP Active message from the ASP, if
	// the ASP is already marked in the APS-ACTIVE state." So a context we have
	// no Routing Key for withholds the Ack only while the ASP is not already
	// active; once it is, the Ack is owed and the Error accompanies it.
	if rcErr != nil && served == nil {
		// Nothing could be activated. Section 4.3.4.3 still owes the Ack if the
		// ASP is already ASP-ACTIVE, independently of the Routing Context.
		if c.State() != StateASPActive {
			return rcErr
		}
		acknowledged = aspActive.RoutingContext.Copy()
	}

	acknowledgedMode := c.trafficModePolicy().defaultParam()
	if aspActive.TrafficModeType != nil {
		acknowledgedMode = aspActive.TrafficModeType.Copy()
	}
	contextlessServed := served == nil && rcErr == nil &&
		aspActive.RoutingContext == nil && len(c.configuredRoutingContexts()) == 0
	if (served != nil || contextlessServed) && c.as != nil {
		var err error
		var servedContexts []uint32
		if served != nil {
			servedContexts = served.RoutingContexts()
		}
		acknowledgedMode, err = c.as.agreeTrafficModeForAssociation(c, servedContexts, aspActive.TrafficModeType)
		if err != nil {
			return err
		}
	}
	if served != nil && c.mtp3Restarts != nil {
		if err := writeMTP3RestartStatusBeforeAck(c.mtp3Restarts, c, served.RoutingContexts()); err != nil {
			return err
		}
	}

	if _, err := c.WriteSignal(
		messages.NewAspActiveAck(acknowledgedMode, acknowledged, nil),
	); err != nil {
		return err
	}

	// Which Application Servers this ASP is now active in. Section 4.3.1 keeps
	// the ASP state per AS, so an ASP Active naming a subset activates it there
	// and leaves it ASP-INACTIVE in the rest; naming none means all of them.
	//
	// Apply the served subset even when another requested context is unserved.
	// The Ack has already told the peer that this subset is active; returning the
	// accompanying Error before recording it would leave the two ends with
	// opposite state for the same AS.
	if served != nil || contextlessServed {
		var servedContexts []uint32
		if served != nil {
			servedContexts = served.RoutingContexts()
		}
		c.noteRoutingContextsActive(servedContexts)

		// Override, after the Ack and only after it. Section 4.3.4.3: "In the
		// case of an Override mode AS, receipt of an ASP Active message at an SGP
		// causes the (re)direction of all traffic for the AS to the ASP that sent
		// the ASP Active message. Any previously active ASP in the AS is now
		// considered to be in the state ASP-INACTIVE and SHOULD no longer receive
		// traffic from the SGP within the AS."
		//
		// The ordering is Section 4.3.4.5's: the Notify follows the related Ack.
		if len(servedContexts) > 0 || contextlessServed {
			c.overrideOtherASPs(aspActive, servedContexts)
		}
	}
	if rcErr != nil {
		return rcErr
	}

	return nil
}

// validateTrafficMode validates the peer's Traffic Mode Type value. Agreement
// with local policy is resolved later, after the Routing Context scope is
// known, by applicationServers.agreeTrafficMode.
// AssociationConfig.TrafficModes takes
// precedence over the default TrafficModeType, so comparing against the default
// before resolving scope rejects a valid per-AS override.
func (c *Association) validateTrafficMode(peer *params.Param) error {
	if peer == nil {
		return nil
	}

	// The table in Section 3.7.1 defines 1 Override, 2 Loadshare and 3
	// Broadcast, and nothing else. A value outside it is "unsupported" in the
	// plainest sense, which Section 4.3.4.3 answers with "an Error message
	// ('Unsupported / Invalid Traffic Handling Mode')". This used to
	// short-circuit to success whenever no mode was configured locally, so an
	// undefined mode was acknowledged as though it had been agreed.
	switch peer.TrafficModeType() {
	case params.TrafficModeOverride, params.TrafficModeLoadshare, params.TrafficModeBroadcast:
	default:
		return ErrUnsupportedTrafficMode
	}

	return nil
}

func (c *Association) validateTrafficModeForRoutingContexts(peer *params.Param, routingContexts []uint32) error {
	if peer == nil {
		return nil
	}
	requestedMode := peer.TrafficModeType()
	trafficModes := c.trafficModePolicy()
	if len(routingContexts) == 0 {
		if trafficModes.defaultModeSet && trafficModes.defaultMode != requestedMode {
			return ErrUnsupportedTrafficMode
		}
		return nil
	}
	for _, routingContext := range routingContexts {
		configuredMode, configured := trafficModes.configured(routingContext)
		if configured && configuredMode != requestedMode {
			return ErrUnsupportedTrafficMode
		}
	}
	return nil
}

// handleAspActiveAck validates the ASP Active Ack that answers our ASP Active.
//
// The Ack travels SGP to ASP ("At the ASP, the ASP Active Ack message received
// is not acknowledged" — RFC 4666 Section 4.3.4.3), so an SGP that receives one
// reports an Error and holds its state: it never sent an ASP Active, so nothing
// authorises a stray Ack to move it to ASP-ACTIVE.
func (c *Association) handleAspActiveAck(aspAcAck *messages.AspActiveAck) error {
	if c.role != RoleASP {
		return NewUnexpectedMessageError(aspAcAck)
	}
	if c.rejectStaleASPTMAck(requestAspActive) {
		return NewUnexpectedMessageError(aspAcAck)
	}

	// ASP-INACTIVE is where the first Ack lands, but not the only place an Ack
	// is legitimate. Activation is per Routing Context — Section 4.3.4.3 has the
	// SGP answer "For the Application Servers for which the ASP can be
	// activated" — so an SGP that could serve one context now and another later
	// sends a second Ack while this ASP is already ASP-ACTIVE. Requiring
	// ASP-INACTIVE threw that second Ack away, leaving the context it granted
	// unusable.
	switch c.State() {
	case StateASPInactive, StateASPActive:
	default:
		return NewUnexpectedMessageError(aspAcAck)
	}

	// Section 4.3.4.3: the SGP answers with the traffic mode in force for the
	// AS, so an Ack naming a mode we cannot operate in is not agreement — it
	// means the two ends would run different traffic handling for the same AS.
	if err := c.validateAspActiveAckTrafficMode(aspAcAck); err != nil {
		return err
	}

	// The Ack must concern the Routing Contexts we asked about. An Ack for
	// somebody else's RC would otherwise take our data path active on the
	// strength of a message that was never about us.
	if err := c.validateRoutingContext(aspAcAck.RoutingContext); err != nil {
		return err
	}
	if err := c.validateTAckRoutingContexts(requestAspActive, aspAcAck.RoutingContext); err != nil {
		return err
	}

	// Only what this Ack named is active. A partial Ack used to take the whole
	// association active, so DATA went out for contexts the SGP had never
	// agreed to carry.
	c.noteRoutingContextsAcked(aspAcAck.RoutingContext)
	c.acknowledgeTAck(requestAspActive, aspAcAck.RoutingContext)

	return nil
}

func (c *Association) validateAspActiveAckTrafficMode(ack *messages.AspActiveAck) error {
	if ack.TrafficModeType == nil {
		return nil
	}
	acknowledgedMode := ack.TrafficModeType.TrafficModeType()
	if !validTrafficMode(acknowledgedMode) {
		return ErrUnsupportedTrafficMode
	}

	acknowledgedContexts := ack.RoutingContext.RoutingContexts()
	if c.tack != nil {
		c.tack.mu.Lock()
		for _, entry := range c.pendingRequestEntriesLocked(requestAspActive) {
			request := entry.request
			matched := false
			if ack.RoutingContext == nil {
				matched = request.routingContextOmitted
			} else {
				for _, rtCtx := range acknowledgedContexts {
					if _, ok := request.routingContexts[rtCtx]; ok {
						matched = true
						break
					}
				}
			}
			if !matched {
				continue
			}
			active, ok := request.msg.(*messages.AspActive)
			if ok && active.TrafficModeType != nil &&
				active.TrafficModeType.TrafficModeType() != acknowledgedMode {
				c.tack.mu.Unlock()
				return ErrUnsupportedTrafficMode
			}
		}
		c.tack.mu.Unlock()
	}

	if len(acknowledgedContexts) == 0 {
		acknowledgedContexts = c.configuredRoutingContexts()
	}
	trafficModes := c.trafficModePolicy()
	for _, rtCtx := range acknowledgedContexts {
		configuredMode, configured := trafficModes.configured(rtCtx)
		if configured && configuredMode != acknowledgedMode {
			return ErrUnsupportedTrafficMode
		}
	}
	if len(acknowledgedContexts) == 0 && trafficModes.defaultModeSet &&
		trafficModes.defaultMode != acknowledgedMode {
		return ErrUnsupportedTrafficMode
	}
	return nil
}

// answerRoutingContexts builds the Routing Context parameter for an Ack the SGP
// is about to send, given what the ASP asked about.
//
// RFC 4666 Section 4.3.4.3: "For the Application Servers for which the ASP can
// be successfully activated, the SGP or IPSP responds with one or more ASP
// Active Ack messages, including the associated Routing Context(s) [...]. The
// Routing Context parameter MUST be included in the ASP Active Ack message(s)
// if the received ASP Active message contained any Routing Contexts."
//
// An SGP fronting several tenants is configured for all of their contexts while
// each ASP is registered for its own, so echoing the SGP's whole configured set
// names contexts that are not the ones being activated — and the ASP, which
// checks the Ack against the contexts it requested (validateRoutingContext),
// then refuses it. In the multi-tenant deployment that meant no ASP could reach
// ASP-ACTIVE at all.
//
// An ASP that omits the parameter is answered with the configured set, since
// there is nothing else to name. A context this SGP does not serve is refused
// outright: acknowledging it would activate the ASP for traffic that will never
// be routed to it.
// unservedError builds the error for Routing Contexts this node has no Routing
// Key for. The two ASPTM messages are given different error codes by the RFC and
// a single shared code cannot satisfy both:
//
//	Section 4.3.4.3 (ASP Active):   "the peer MUST respond with an ERROR
//	  message with the Error Code 'No configured AS for ASP'."
//	Section 4.3.4.4 (ASP Inactive): "the SGP/IPSP MUST respond with an ERROR
//	  message with the Error Code 'Invalid Routing Context'."
type unservedError func(rcs ...uint32) *RoutingContextError

func (c *Association) answerRoutingContexts(requested *params.Param, unservedAs, noKeysAs unservedError) (*params.Param, error) {
	ours := make(map[uint32]struct{})
	for _, rc := range c.configuredRoutingContexts() {
		ours[rc] = struct{}{}
	}

	var asked []uint32
	if requested != nil {
		asked = requested.RoutingContexts()
	}

	if len(asked) == 0 {
		// Section 4.3.4.3: "If the RC parameter is not included in the ASP
		// Active message and there are no RKs defined, the peer node SHOULD
		// respond with and ERROR message with the Error Code 'Invalid Routing
		// Context'." Section 4.3.4.4 says the same for ASP Inactive, with "No
		// configured AS for ASP".
		//
		// That is the no-RK case, not the dedicated-association case where local
		// configuration intentionally carries a single AS without assigning it a
		// numeric Routing Context. AssociationConfig.RoutingContexts nil, and the empty
		// parameter produced by the public constructors from an empty slice, both
		// represent that contextless AS. The only "no AS" shape this API can
		// express after ASP Up is an explicit authorizer that returned no
		// membership.
		if len(ours) == 0 {
			if c.hasExplicitlyEmptyASPAuthorization() {
				return nil, noKeysAs()
			}
			return nil, nil
		}
		// Otherwise the receiver knows by configuration which AS the ASP
		// belongs to, and answers for the whole configured set.
		return c.configuredRoutingContextParam(), nil
	}

	// An explicit context never creates its own Routing Key. With no static RK
	// configured (and RKM unsupported), the ASP is not authorized for any AS.
	if len(ours) == 0 {
		return nil, unservedAs(asked...)
	}

	// Section 4.3.4.3 acknowledges "the Application Servers for which the ASP
	// can be successfully activated" and allows "Multiple ASP Active Ack
	// messages [...] for different (sets of) Routing Contexts", so a request
	// mixing served and unserved contexts is answered for the served ones and
	// refused for the rest. Aborting on the first unserved context denied the
	// ASP an activation the RFC makes a MUST.
	served := make([]uint32, 0, len(asked))
	bad := make([]uint32, 0, len(asked))
	for _, rc := range asked {
		if _, ok := ours[rc]; ok {
			served = append(served, rc)
		} else {
			bad = append(bad, rc)
		}
	}

	if len(served) == 0 {
		return nil, unservedAs(bad...)
	}
	if len(bad) > 0 {
		return params.NewRoutingContext(served...), unservedAs(bad...)
	}
	return params.NewRoutingContext(served...), nil
}

// validateRoutingContextShape distinguishes an omitted optional Routing
// Context parameter from a parameter that is present but contains no complete
// 32-bit value. RFC 4666 Section 3.8.1 calls the latter an Invalid Routing
// Context; it must never inherit the omitted parameter's "all configured ASes"
// meaning.
func validateRoutingContextShape(peer *params.Param) error {
	if peer != nil && len(peer.RoutingContexts()) == 0 {
		return NewInvalidRoutingContextError()
	}
	return nil
}

// validateRoutingContext checks that a peer's Routing Context parameter refers
// to contexts this association is configured for.
//
// The parameter is Optional (RFC 4666 Sections 3.7.1 and 3.7.2): a peer that omits it
// is answering for the single configured context, which is not an error. When
// present, every context named must be one of ours.
func (c *Association) validateRoutingContext(peer *params.Param) error {
	// A parameter that is present and decodes to nothing is not the same as one
	// that was omitted. Section 3.8.1: "The 'Invalid Routing Context' error is
	// sent if a message is received with an invalid or unconfigured routing
	// context value" — an empty value, or one that is not a whole number of
	// 32-bit words, is invalid on its face. It was previously read as though the
	// peer had sent no context at all, so a DATA carrying a malformed Routing
	// Context was delivered to the application as unattributed traffic, and a
	// malformed one on an ASPTM or SSNM message was silently accepted.
	//
	// Judged before the local configuration, because this is the value being
	// malformed rather than the value being one we do not serve: it is wrong
	// whether or not any Routing Key has been coordinated.
	if err := validateRoutingContextShape(peer); err != nil {
		return err
	}
	if peer == nil {
		return nil
	}
	theirs := peer.RoutingContexts()

	if c.cfg == nil || c.cfg.RoutingContexts == nil {
		return NewInvalidRoutingContextError(theirs...)
	}

	ours := make(map[uint32]struct{})
	for _, rc := range c.configuredRoutingContexts() {
		ours[rc] = struct{}{}
	}
	// An empty local set means no Routing Key exists, not that every peer value
	// becomes valid by default.
	if len(ours) == 0 {
		return NewInvalidRoutingContextError(theirs...)
	}

	// The offending contexts travel with the error. Section 3.8.1 requires the
	// Error message to quote them — "For this error, the invalid or
	// unconfigured Routing Context value(s) MUST be included in the Routing
	// Context parameter" — and only the value the peer sent will do; answering
	// with our own configured set tells it nothing about what it got wrong.
	var offending []uint32
	for _, rc := range theirs {
		if _, ok := ours[rc]; !ok {
			offending = append(offending, rc)
		}
	}
	if len(offending) > 0 {
		return NewInvalidRoutingContextError(offending...)
	}

	return nil
}

// handleAspInactive answers an incoming ASP Inactive.
//
// RFC 4666 Section 4.3.4.4 states "The SGP MUST send an ASP Inactive Ack
// message in response to a received ASP Inactive message from the ASP; the ASP
// is already marked as ASP-INACTIVE at the SGP." As with ASP Active, the Ack is
// owed even when the requested state is the current one, so a peer that has
// lost its state converges instead of retransmitting on T(ack).
//
// These are SGP procedures, so an ASP that receives an ASP Inactive reports an
// Error and holds its state rather than acking.
func (c *Association) handleAspInactive(aspInactive *messages.AspInactive) error {
	if c.role != RoleSGP {
		return NewUnexpectedMessageError(aspInactive)
	}

	// As with ASP Active: Figure 3 defines no ASPTM transition out of ASP-DOWN,
	// and Section 4.3.1 allows an ASP-DOWN peer only "Heartbeat, ASP Down Ack,
	// and Error messages", so the Ack must not be written.
	if c.State() == StateASPDown {
		return NewUnexpectedMessageError(aspInactive)
	}
	if err := validateRoutingContextShape(aspInactive.RoutingContext); err != nil {
		return err
	}

	served, rcErr := c.answerRoutingContexts(aspInactive.RoutingContext, NewInvalidRoutingContextError, NewNoConfiguredASError)
	acknowledged := served
	contextlessServed := served == nil && rcErr == nil &&
		aspInactive.RoutingContext == nil && len(c.configuredRoutingContexts()) == 0

	// Section 4.3.4.4 owes the Ack even when the ASP is already ASP-INACTIVE,
	// so an unknown context withholds it only outside that state.
	if rcErr != nil && served == nil {
		if c.State() != StateASPInactive {
			return rcErr
		}
		acknowledged = aspInactive.RoutingContext.Copy()
	}

	// Section 4.3.4.4 sends the Ack only "after all traffic is halted". First
	// remove this ASP from every served AS atomically with respect to target
	// snapshots, then wait for any DATA that selected it before that mark. No AS
	// mutex is held while waiting for the socket write.
	postAckNotify := func() {}
	if (served != nil || contextlessServed) && c.State() == StateASPActive {
		var routingContexts []uint32
		if served != nil {
			routingContexts = served.RoutingContexts()
		}
		c.noteRoutingContextsInactive(routingContexts)
		if c.as != nil {
			postAckNotify = c.as.quiesceASPFor(c, routingContexts)
		}
	}
	// A dedicated association has no Application Server deliveryMu to drain.
	// Its direct DATA and SSNM writes use the unscoped barrier instead, and the
	// Ack is not permitted to overtake one already admitted there.
	c.quiesceUnscopedTraffic()

	ackErr := c.writeMandatoryControls([]messages.M3UA{
		messages.NewAspInactiveAck(acknowledged, nil),
	}, false, true)
	postAckNotify()
	if ackErr != nil {
		return ackErr
	}

	// The mirror of handleAspActive: the ASP stands down in the Application
	// Servers the Ack named, and in all of them if it named none. This precedes
	// the accompanying Error for a mixed request: the peer has already accepted
	// the Acked subset and the SGP must apply the same transition locally.
	if rcErr != nil {
		return rcErr
	}

	return nil
}

// handleAspInactiveAck validates the ASP Inactive Ack that answers our ASP
// Inactive.
//
// As with ASP Active Ack this is an SGP-to-ASP message (RFC 4666 Section
// 4.3.4.4), so an SGP that receives one reports an Error and holds its state.
func (c *Association) handleAspInactiveAck(aspAcAck *messages.AspInactiveAck) error {
	if c.role != RoleASP {
		return NewUnexpectedMessageError(aspAcAck)
	}
	if c.rejectStaleASPTMAck(requestAspInactive) {
		return NewUnexpectedMessageError(aspAcAck)
	}
	previousState := c.State()
	if previousState != StateASPDown && previousState != StateASPInactive && previousState != StateASPActive {
		return NewUnexpectedMessageError(aspAcAck)
	}

	// ASP Inactive Ack carries no Traffic Mode Type (Section 3.7.4), so only
	// the Routing Context is checked: the Ack must concern contexts we asked
	// to deactivate.
	if err := c.validateRoutingContext(aspAcAck.RoutingContext); err != nil {
		return err
	}
	if err := c.validateTAckRoutingContexts(requestAspInactive, aspAcAck.RoutingContext); err != nil {
		return err
	}

	// Traffic has stopped only for the contexts this Ack names. Clearing every
	// acknowledged context here made an RC-scoped ASP Inactive tear down the
	// unaffected Application Servers carried by the same association.
	if previousState == StateASPActive {
		c.noteRoutingContextsUnacked(aspAcAck.RoutingContext)
	} else {
		c.noteNoRoutingContextsAcked()
	}
	solicited := c.acknowledgeTAck(requestAspInactive, aspAcAck.RoutingContext)
	if solicited || previousState != StateASPActive {
		return nil
	}

	// An unsolicited Ack first makes the affected AS inactive, then Section
	// 4.3.4.4 asks an ASP that was active to return to its previous state. If a
	// sibling RC remains active, the association never enters ASP-INACTIVE, so
	// restart only the displaced scope here. Otherwise the ASP-INACTIVE entry
	// action initiates the return after that required intermediate state.
	if c.hasAcknowledgedRoutingContexts() {
		return c.initiateASPActive(aspAcAck.RoutingContext)
	}
	c.armResumeAfterStrayAck()

	return nil
}

// noteRoutingContextsUnacked removes the contexts an ASP Inactive Ack covered
// from the set an ASP Active Ack granted. An omitted parameter covers every
// configured AS; a named parameter affects only that subset.
func (c *Association) noteRoutingContextsUnacked(inactive *params.Param) {
	rcs := c.configuredRoutingContexts()
	if inactive != nil {
		rcs = inactive.RoutingContexts()
	}

	c.muAckedRCs.Lock()
	defer c.muAckedRCs.Unlock()

	if !c.ackedRCsScoped {
		// An Association placed directly into ASP-ACTIVE has the compatibility meaning
		// "active everywhere". Materialise that set before subtracting a scoped
		// deactivation so the unaffected contexts remain active.
		c.ackedRCs = make(map[uint32]struct{})
		for _, rc := range c.configuredRoutingContexts() {
			c.ackedRCs[rc] = struct{}{}
		}
	}
	c.ackedRCsScoped = true
	for _, rc := range rcs {
		delete(c.ackedRCs, rc)
	}
}

func (c *Association) noteNoRoutingContextsAcked() {
	c.muAckedRCs.Lock()
	c.ackedRCs = make(map[uint32]struct{})
	c.ackedRCsScoped = true
	c.muAckedRCs.Unlock()
}

// hasAcknowledgedRoutingContexts reports whether at least one Application
// Application Server remains active at an ASP after a scoped ASP Inactive Ack.
func (c *Association) hasAcknowledgedRoutingContexts() bool {
	c.muAckedRCs.RLock()
	defer c.muAckedRCs.RUnlock()
	return !c.ackedRCsScoped || len(c.ackedRCs) > 0
}

// handleHeartbeat answers an incoming BEAT.
//
// RFC 4666 Section 3.5.5 states "The receiver MUST respond with a BEAT Ack
// message", and Section 4.3.4.6 repeats "Upon receiving a Heartbeat message,
// the M3UA peer MUST respond with a Heartbeat Ack message". Neither is
// qualified by ASP state, and Section 4.3.4.6 closes with "Note: Heartbeat-
// related events are not shown in Figure 3 'ASP state transition diagram'".
// The BEAT Ack is therefore unconditional: withholding it because the ASP is
// momentarily ASP-INACTIVE lets the peer's T(beat) expire and tears down an
// otherwise healthy association.
func (c *Association) handleHeartbeat(beat *messages.Heartbeat) error {
	// No need to create new HeartbeatAck, as it's identical to Heartbeat except the MessageType.
	beat.Type = messages.MsgTypeHeartbeatAck
	if _, err := c.WriteSignal(beat); err != nil {
		return err
	}
	return nil
}

// handleHeartbeatAck validates the BEAT Ack echoed back by the peer.
//
// Like the BEAT itself this is outside the ASP state machine (RFC 4666 Section
// 4.3.4.6), so a correctly echoed Ack is accepted in any state. Rejecting it
// while ASP-INACTIVE would expire our own T(beat) and tear down the
// association. Only a mismatched echo is an error, per "The Heartbeat message
// may optionally contain an opaque Heartbeat Data parameter that MUST be echoed
// back unchanged in the related Heartbeat Ack message."
func (c *Association) handleHeartbeatAck(beatAck *messages.HeartbeatAck) error {
	// An Ack is only meaningful against a BEAT we actually sent. Without
	// outstanding data any peer could forge a liveness token and defeat the
	// T(beat) detection of a dead link.
	myData := c.currentBeatData()
	if len(myData) == 0 {
		return NewUnexpectedMessageError(beatAck)
	}

	// The Heartbeat Data parameter is optional on the wire (RFC 4666 Section
	// 4.3.4.6), so a peer may omit it entirely: reject rather than dereference.
	if beatAck.HeartbeatData == nil {
		return NewUnexpectedMessageError(beatAck)
	}

	dataFromPeer := beatAck.HeartbeatData.HeartbeatData()
	if len(dataFromPeer) != len(myData) {
		return NewUnexpectedMessageError(beatAck)
	}
	for i, p := range dataFromPeer {
		if p != myData[i] {
			return NewUnexpectedMessageError(beatAck)
		}
	}

	// The echo is genuine, so retire it: this Ack has answered its BEAT and no
	// copy of it may satisfy the next one. Retiring only after the comparison
	// keeps a bogus Ack from clearing the data the real Ack still needs, and
	// the compare-and-clear is atomic so a duplicate racing in on another
	// goroutine cannot also claim the same outstanding BEAT.
	if !c.consumeBeatData() {
		return NewUnexpectedMessageError(beatAck)
	}

	return nil
}

// stateForActiveRoutingContexts maps per-AS SGP bookkeeping onto the
// association-wide compatibility state. The association remains ASP-ACTIVE
// while at least one Application Server is active and becomes ASP-INACTIVE only
// when the last one stands down.
func (c *Association) stateForActiveRoutingContexts() State {
	c.muAckedRCs.RLock()
	scoped, active := c.activeRCsScoped, len(c.activeRCs)
	c.muAckedRCs.RUnlock()

	if !scoped {
		return c.State()
	}
	if active > 0 {
		return StateASPActive
	}
	return StateASPInactive
}

// overrideOtherASPs carries out the Override procedure of RFC 4666 Section
// 4.3.4.3 for the Routing Contexts this ASP successfully activated.
//
// It does nothing outside Override mode: in Loadshare and Broadcast, "receipt
// of an ASP Active message at an SGP or IPSP causes direction of traffic to the
// ASP sending the ASP Active message, in addition to all the other ASPs that
// are active", so nobody is displaced.
func (c *Association) overrideOtherASPs(aspActive *messages.AspActive, activated []uint32) {
	if c.as == nil {
		return
	}

	for _, key := range c.asKeysForRoutingContexts(activated) {
		as := c.as.get(key)
		if as.TrafficMode() != params.TrafficModeOverride {
			continue
		}
		// The Ack has already made the challenger active from the peer's point
		// of view. Activate it and displace every incumbent under one AS lock so
		// two simultaneous challengers cannot each remove the other.
		peers, postBarrierNotify, startDrain := as.activateOverride(c, c.as.recoveryTimer)
		for _, peer := range peers {
			// "Any previously active ASP in the AS is now considered to be in
			// the state ASP-INACTIVE" in this AS, not every AS on the
			// association. Record the scoped state before Notify so no further
			// traffic is selected for the displaced ASP after it is told.
			if key.RoutingContextSet {
				peer.noteRoutingContextsInactive([]uint32{key.RoutingContext})
			} else {
				peer.noteRoutingContextsInactive(nil)
			}
		}
		waitForTrafficBarrier(&as.deliveryMu)
		postBarrierNotify()
		if startDrain {
			go as.drainRecoveryQueue()
		}
		for _, peer := range peers {
			if peer.stateForActiveRoutingContexts() == StateASPInactive {
				peer.sendState(StateASPInactive)
			}
			notifyAlternateASPActive(peer, key, c.peerASPIdentifierParam())
		}
	}
}
