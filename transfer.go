// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func (c *Association) handleData(ctx context.Context, data *messages.Data) {
	if !c.inboundDataActive() {
		c.sendErrForMessage(data, NewUnexpectedMessageError(data))
		return
	}

	// RFC 4666 Section 1.4.7 rule 1: "DATA messages MUST NOT be sent on stream
	// 0." The rule binds both ends, and the ASPSM handlers already enforce the
	// mirror of it from the same recorded arrival stream; leaving this
	// direction unchecked meant a peer that broke the rule was rewarded with
	// delivery.
	if c.receivedStreamID() == 0 {
		c.sendErrForMessage(data, NewInvalidSCTPStreamIDError(0))
		return
	}

	if err := c.validateDataNetworkAppearance(data.NetworkAppearance, data.RoutingContext); err != nil {
		c.sendErrForMessage(data, err)
		return
	}

	if err := c.validateDataRoutingContext(data.RoutingContext); err != nil {
		c.sendErrForMessage(data, err)
		return
	}

	// RFC 4666 Section 4.3.1 maintains ASP state per Application Server. The
	// association-wide State remains ASP-ACTIVE while any Routing Context is
	// active, so it cannot by itself authorize this DATA's flow. An SGP may
	// report DATA from an ASP-INACTIVE flow as Unexpected Message; an ASP is the
	// explicit silent-discard example in Section 3.8.1.
	if rtCtx, ok := c.receivedDataRoutingContext(data.RoutingContext); ok {
		switch c.role {
		case RoleSGP:
			if !c.activeForRoutingContext(rtCtx) {
				c.sendErrForMessage(data, NewUnexpectedMessageError(data))
				return
			}
		case RoleASP:
			if !c.routingContextAcked(rtCtx) || c.routingContextOverridden(rtCtx) {
				return
			}
		case RoleIPSP:
			active := c.activeForRoutingContext(rtCtx)
			if c.isIPSPDoubleExchange() {
				active = c.routingContextAcked(rtCtx)
			}
			if !active || c.routingContextOverridden(rtCtx) {
				c.sendErrForMessage(data, NewUnexpectedMessageError(data))
				return
			}
		}
	}

	// RFC 4666 Section 3.3.1 lists Protocol Data as Mandatory in DATA, so only
	// a broken or hostile peer omits it. handleData runs on its own goroutine
	// and the package installs no recover(), so dereferencing the absent
	// parameter would terminate the process and every association it serves.
	if data.ProtocolData == nil {
		c.sendErrForMessage(data, ErrMissingProtocolData)
		return
	}

	pd, err := data.ProtocolData.ProtocolData()
	if err != nil {
		c.sendErrForMessage(data, ErrFailedToPeelOff)
		return
	}

	// The traffic flow travels with the payload rather than being dropped here.
	// validateDataRoutingContext has already established that a present DATA
	// Routing Context contains exactly one value.
	msg := &DataMessage{ProtocolData: pd}
	if data.NetworkAppearance != nil {
		msg.NetworkAppearance = data.NetworkAppearance.NetworkAppearance()
		msg.NetworkAppearanceSet = true
	}
	if data.RoutingContext != nil {
		msg.RoutingContext = data.RoutingContext.RoutingContexts()[0]
		msg.RoutingContextSet = true
	}

	// Never block the dispatcher.
	//
	// handleData runs inline on the single dispatch goroutine, because that is
	// what keeps DATA in the order the peer sent it. The cost is that parking
	// here parks everything: with the queue full, no message of any class is
	// parsed, so the ASP Up Ack and ASP Down Ack that RFC 4666 Sections 4.3.4.1
	// and 4.3.4.2 make mandatory are never sent, the BEAT Ack that Section 3.5.5
	// requires is never sent, and the peer concludes a perfectly healthy node is
	// dead and tears the association down — losing the whole queue along with
	// everything else.
	//
	// dataChan holds the configured, bounded number of payloads. Once an
	// application has fallen that far behind, the excess is discarded and
	// reported instead. Section 3.4.4's SCON is the RFC's way of telling the
	// peer about local congestion, and ErrDataQueueFull originates one.
	select {
	case c.dataChan <- msg:
		c.dataOverflow.Store(false)
		return
	case <-c.done:
		return
	case <-ctx.Done():
		return
	default:
	}

	// Report the onset of overflow once, not once per discarded payload: under
	// sustained overflow the report would otherwise be the loudest thing in the
	// log and would occupy the dispatcher it exists to keep free.
	if !c.dataOverflow.Swap(true) {
		c.sendErr(ErrDataQueueFull)
	}
}

// validateDataRoutingContext applies DATA's narrower use of Routing Context.
// The general parameter can carry a list for ASPTM and SSNM messages, but RFC
// 4666 Section 3.3.1 defines exactly one 32-bit value for DATA. It is optional
// on a dedicated or uncoordinated association and mandatory when several
// Routing Keys share the association, because without it the traffic flow is
// unknowable.
func (c *Association) validateDataRoutingContext(peer *params.Param) error {
	configured := c.configuredRoutingContexts()
	if c.isIPSPDoubleExchange() {
		configured = c.configuredLocalRoutingContexts()
	}
	if peer == nil {
		if len(configured) > 1 {
			return ErrMissingRoutingContext
		}
		return nil
	}

	if err := validateRoutingContextAgainst(peer, configured); err != nil {
		return err
	}
	routingContexts := peer.RoutingContexts()
	if len(routingContexts) != 1 {
		return NewInvalidRoutingContextError(routingContexts...)
	}
	return nil
}

// receivedDataRoutingContext returns the DATA's explicit flow, or the single
// configured flow an omitted parameter unambiguously implies. With no
// coordinated Routing Key there is no per-AS state to consult.
func (c *Association) receivedDataRoutingContext(peer *params.Param) (uint32, bool) {
	if peer != nil {
		return peer.RoutingContexts()[0], true
	}
	configured := c.configuredRoutingContexts()
	if c.isIPSPDoubleExchange() {
		configured = c.configuredLocalRoutingContexts()
	}
	if len(configured) == 1 {
		return configured[0], true
	}
	return 0, false
}

func (c *Association) validateDataNetworkAppearance(peer, routingContext *params.Param) error {
	configured, allNetworkAppearances, err := c.resolveNetworkAppearanceScope(routingContext, c.isIPSPDoubleExchange())
	if err != nil {
		return err
	}
	return c.validateNetworkAppearanceAgainst(peer, configured, allNetworkAppearances)
}

func (c *Association) validateSSNMNetworkAppearance(peer, routingContext *params.Param) error {
	configured, allNetworkAppearances, err := c.resolveNetworkAppearanceScope(routingContext, false)
	if err != nil {
		return err
	}
	return c.validateNetworkAppearanceAgainst(peer, configured, allNetworkAppearances)
}

func (c *Association) validateNetworkAppearanceAgainst(peer, configured *params.Param, allNetworkAppearances bool) error {
	if peer == nil {
		return nil
	}
	if peer.Tag != params.NetworkAppearance {
		return NewParameterFaultErrorFor(nil, params.ErrInvalidType)
	}
	if len(peer.Data) != 4 {
		return NewParameterFaultErrorFor(nil, params.ErrInvalidLength)
	}

	// The Invalid Network Appearance Error is explicitly originated by an SGP
	// when an ASP sends an invalid value. An ASP receiving an SGP's value keeps
	// it for the MTP3-User rather than reflecting Error code 21 back.
	if c.role != RoleSGP && c.role != RoleIPSP {
		return nil
	}
	if allNetworkAppearances {
		return nil
	}

	if configured == nil || configured.Tag != params.NetworkAppearance ||
		len(configured.Data) != 4 ||
		configured.NetworkAppearance() != peer.NetworkAppearance() {
		return NewInvalidNetworkAppearanceError(peer.NetworkAppearance())
	}

	return nil
}
