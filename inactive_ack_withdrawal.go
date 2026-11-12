package m3ua

import (
	"fmt"

	"github.com/gomaja/go-m3ua/messages/params"
)

// claimInactiveAckWithdrawal classifies unsolicited per-AS withdrawal and
// claims local confirmations in one pending-inventory transaction. Only the
// outstanding part of a deactivation is solicited (RFC 4666 §4.3.4.4).
func (c *Association) claimInactiveAckWithdrawal(ack, wire *params.Param) (pendingTAckAcknowledgement, []ASKey, []ASKey) {
	keys := c.managementASKeys(nil, ack)
	active := make(map[ASKey]bool, len(keys))
	state := c.localASPProcedureState()
	for _, key := range keys {
		if state != StateASPActive {
			continue
		}
		if c.role == RoleIPSP && !c.isIPSPDoubleExchange() {
			active[key] = c.activeForASKey(key)
		} else if key.RoutingContextSet {
			active[key] = c.routingContextAcked(key.RoutingContext)
		} else {
			active[key] = c.contextlessASAcked()
		}
		if key.RoutingContextSet && c.routingContextOverridden(key.RoutingContext) {
			active[key] = false
		}
	}
	c.tack.mu.Lock()
	defer c.tack.mu.Unlock()
	var withdrawn, affected []ASKey
	history := c.tack.acknowledgedASPTM[requestAspInactive]
	for _, key := range keys {
		solicited := false
		for _, req := range c.tack.pending {
			if req.kind != requestAspInactive {
				continue
			}
			if key.RoutingContextSet {
				_, solicited = req.remaining[key.RoutingContext]
			} else {
				solicited = req.routingContextOmitted
			}
			if solicited {
				break
			}
		}
		repeated := false
		if !solicited && history != nil {
			if key.RoutingContextSet {
				_, repeated = history.routingContexts[key.RoutingContext]
			} else {
				repeated = history.routingContextOmitted
			}
		}
		if repeated || (!solicited && !active[key]) {
			continue
		}
		affected = append(affected, key)
		if !solicited {
			withdrawn = append(withdrawn, key)
		}
	}
	if len(affected) == 0 {
		return pendingTAckAcknowledgement{}, nil, nil
	}
	acknowledgement := c.claimTAckAcknowledgementLocked(requestAspInactive, ack, wire)
	return acknowledgement, withdrawn, affected
}

// A mixed Ack can confirm an outstanding portion and repeat an older one.
// Match original Active wire scopes against only the effective Ack scopes,
// including contextless membership, without canceling independent application
// activation (RFC 4666 §4.3.4.3, §4.3.4.4).
func (c *Association) stopInactiveAckActivations(affected []ASKey) []*pendingRequest {
	if len(affected) == 0 {
		return nil
	}
	return c.cancelASPActivations(affected, "ASP Inactive Ack")
}

// cancelASPActivations stops overlapping requests, then waits only for their
// transport calls. No shared writer mutex is acquired (RFC 4666 §4.3.4.4).
func (c *Association) cancelASPActivations(affected []ASKey, reason string) []*pendingRequest {
	if c.tack == nil {
		return nil
	}
	c.tack.mu.Lock()
	var cancelled []*pendingRequest
	for message, request := range c.tack.pending {
		if request.kind != requestAspActive {
			continue
		}
		overlaps := affected == nil || request.routingContextOmitted
		for _, key := range affected {
			if key.RoutingContextSet {
				if _, named := request.routingContexts[key.RoutingContext]; named {
					overlaps = true
				}
			}
		}
		if overlaps {
			cause := fmt.Errorf("%w by %s for %v", ErrASPProcedureSuperseded, reason, affected)
			request.failure.Store(&cause)
			request.stopRetries()
			cancelled = append(cancelled, request)
			delete(c.tack.pending, message)
		}
	}
	c.tack.mu.Unlock()
	for _, request := range cancelled {
		request.joinControlWrite()
	}
	return cancelled
}

// M-ERROR reports an unsuccessful local cancellation (RFC 4666 §1.6.3).
// It records transport acceptance now, without interpreting a later Active Ack.
// Complete the normal result after these reports, using the same separate
// retry stop and result completion as a claimed acknowledgement. A waiting
// ASPActive preserves M-ERROR order while its context and association remain
// live (RFC 4666 §1.6.3).
func (c *Association) reportCancelledActivations(requests []*pendingRequest) {
	for _, request := range requests {
		call := request.joinControlWrite()
		if !call.accepted {
			continue
		}
		c.notifyManagement(&ManagementIndication{
			Kind: ManagementError, ASKeys: call.keys, RoutingContexts: inactiveAckRoutingContexts(call.keys),
			Cause:       fmt.Errorf("%w: cancelled ASP Active was already accepted by SCTP; peer may consider these scopes active", ErrASPProcedureSuperseded),
			Description: "cancelled ASP Active was already accepted by SCTP; application must decide whether to send ASP Inactive or ASP Active",
		})
	}
	for _, request := range requests {
		request.finish(*request.failure.Load())
	}
}

func inactiveAckRoutingContexts(keys []ASKey) []uint32 {
	var contexts []uint32
	seen := make(map[uint32]struct{})
	for _, key := range keys {
		if key.RoutingContextSet {
			if _, duplicate := seen[key.RoutingContext]; !duplicate {
				seen[key.RoutingContext] = struct{}{}
				contexts = append(contexts, key.RoutingContext)
			}
		}
	}
	return contexts
}

// Recovery is an application decision. RFC 4666 §4.3.4.4 uses lowercase
// "should" for returning to the previous state; §4.3.4.3 permits Layer
// Management to initiate ASP Active. §1.6.3 defines no unsolicited-withdrawal
// primitive, so this report uses an implementation name.
func (c *Association) reportInactiveAckWithdrawal(keys []ASKey) {
	if len(keys) == 0 {
		return
	}
	c.notifyManagement(&ManagementIndication{Kind: ManagementASPWithdrawal, ASKeys: keys, RoutingContexts: inactiveAckRoutingContexts(keys), Description: "peer reported Application Servers inactive with an unsolicited ASP Inactive Ack; reactivation is an application decision"})
}

// The parameter is a local traffic-gate selection, not a new wire request.
func inactiveAckEffectiveScope(keys []ASKey) *params.Param {
	for _, key := range keys {
		if !key.RoutingContextSet {
			return nil
		}
	}
	return params.NewRoutingContext(inactiveAckRoutingContexts(keys)...)
}
