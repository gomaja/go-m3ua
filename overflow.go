// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-pointcode"
	"github.com/gomaja/go-sctp"
)

// These are receive-queue engineering defaults, not RFC-assigned values.
// Capacity is association-wide; the scopes identify discarded traffic, not
// independently measured per-AS queues. Retain at most 32 scopes per episode.
const (
	dataOverflowScopeLimit    = 32
	dataOverflowDrainDivisor  = 2 // at or below 50% of message capacity
	dataOverflowDrainDwell    = time.Second
	dataOverflowCheckInterval = 100 * time.Millisecond
)

// DataQueueCongestionIndication reports a local receive-queue transition to
// Layer Management, independently of whether an overflow SCON reaches the peer.
// This is local queue evidence under RFC 4666 Section 1.4.6, not SS7 network
// congestion and not an independently measured queue for the named AS.
type DataQueueCongestionIndication struct {
	// Scope is the exact received NA/RC, including omitted versus explicit zero.
	Scope WireScope
	// AS is the resolved Application Server of the discarded DATA.
	AS ASKey
	// DestinationPointCode is the discarded DATA's DPC, the affected destination.
	DestinationPointCode uint32
	// OriginatingPointCode is the discarded DATA's OPC.
	OriginatingPointCode uint32
	// NetworkIndicator is the discarded DATA's NI.
	NetworkIndicator pointcode.NetworkIndicator
	// Congested is true on onset and false after a sustained drain.
	Congested bool
	// TrackingLost marks the first untracked discard when the 32-scope episode
	// limit is exceeded. No per-scope end is promised for untracked traffic.
	// DataQueueStats.ScopeTrackingLosses counts every such discard.
	TrackingLost bool
}

type dataOverflowKey struct {
	networkAppearance, routingContext, dpc, opc uint32
	networkAppearanceSet, routingContextSet     bool
}

func overflowKey(s WireScope, dpc, opc uint32) dataOverflowKey {
	key := dataOverflowKey{networkAppearance: s.NetworkAppearance, networkAppearanceSet: s.NetworkAppearanceSet,
		routingContextSet: s.RoutingContextSet, dpc: dpc, opc: opc}
	if s.RoutingContextSet && len(s.RoutingContexts) == 1 {
		key.routingContext = s.RoutingContexts[0]
	}
	return key
}

func (c *Association) recordDataOverflow(msg *DataMessage) *DataQueueOverflowError {
	c.muDataOverflowEvents.Lock()
	defer c.muDataOverflowEvents.Unlock()
	c.muDataOverflow.Lock()
	overflow, indication, start := c.recordDataOverflowLocked(msg)
	c.muDataOverflow.Unlock()
	if start {
		go c.watchDataOverflow()
	}
	if indication != nil {
		c.notifyDataQueueCongestion(*indication)
	}
	return overflow
}

func (c *Association) recordDataOverflowLocked(msg *DataMessage) (*DataQueueOverflowError, *DataQueueCongestionIndication, bool) {
	if !c.dataOverflow.Load() {
		c.dataOverflowGeneration++
		c.dataOverflowSeenRefill = c.dataOverflowRefills.Add(1)
	}
	c.dataOverflow.Store(true)
	// Every discard interrupts a low-occupancy dwell, even if a concurrent
	// consumer has drained the queue before this observation.
	c.dataOverflowLowSince = time.Time{}
	key := overflowKey(msg.Scope, msg.ProtocolData.DestinationPointCode, msg.ProtocolData.OriginatingPointCode)
	if _, exists := c.dataOverflowScopes[key]; exists {
		return nil, nil, false
	}
	if len(c.dataOverflowScopes) == dataOverflowScopeLimit {
		c.dataScopeTrackingLosses.Add(1)
		if !c.dataOverflowTrackingLost {
			c.dataOverflowTrackingLost = true
			ind := congestionFromData(msg)
			ind.TrackingLost = true
			return nil, &ind, false
		}
		return nil, nil, false
	}
	if c.dataOverflowScopes == nil {
		c.dataOverflowScopes = make(map[dataOverflowKey]DataQueueCongestionIndication)
	}
	ind := congestionFromData(msg)
	c.dataOverflowScopes[key] = ind
	start := !c.dataOverflowWorker
	c.dataOverflowWorker = true
	return &DataQueueOverflowError{generation: c.dataOverflowGeneration, Scope: msg.Scope.clone(),
		DestinationPointCode: ind.DestinationPointCode, OriginatingPointCode: ind.OriginatingPointCode,
		NetworkIndicator: ind.NetworkIndicator}, &ind, start
}

func congestionFromData(msg *DataMessage) DataQueueCongestionIndication {
	return DataQueueCongestionIndication{Scope: msg.Scope.clone(), AS: msg.AS,
		DestinationPointCode: msg.ProtocolData.DestinationPointCode,
		OriginatingPointCode: msg.ProtocolData.OriginatingPointCode,
		NetworkIndicator:     msg.ProtocolData.NetworkIndicator, Congested: true}
}

func (c *Association) notifyDataQueueCongestion(ind DataQueueCongestionIndication) {
	c.notifyManagement(&ManagementIndication{Kind: ManagementDataQueueCongestion,
		DataQueueCongestion: &ind, Description: "inbound DATA queue congestion"})
}

// recordDataQueueRefill retains an actual send-side high-water observation.
// Revision changes precede the tracker wait; end uses CAS so it cannot pass a
// concurrently recorded refill. The even revision excludes an already ended
// episode, including the brief interval before its active flag is cleared.
func (c *Association) recordDataQueueRefill() {
	for {
		revision := c.dataOverflowRefills.Load()
		if revision&1 == 0 {
			return
		}
		if c.dataOverflowRefills.CompareAndSwap(revision, revision+2) {
			return
		}
	}
}

func (c *Association) observeDataQueue() {
	if !c.dataOverflow.Load() {
		return
	}
	c.muDataOverflow.Lock()
	now, queued := time.Now(), len(c.dataChan)
	// Routine observations do not wait for a management publisher. Only an
	// end transition needs the ordering gate, acquired without the tracker lock.
	if queued > cap(c.dataChan)/dataOverflowDrainDivisor || c.dataOverflowLowSince.IsZero() || now.Sub(c.dataOverflowLowSince) < dataOverflowDrainDwell {
		c.observeDataQueueLocked(now, queued)
		c.muDataOverflow.Unlock()
		return
	}
	c.muDataOverflow.Unlock()
	if !c.muDataOverflowEvents.TryLock() {
		return
	}
	defer c.muDataOverflowEvents.Unlock()
	c.muDataOverflow.Lock()
	// Recheck occupancy and dwell after admission: another discard/refill may
	// have cancelled the candidate while the ordering gate was unavailable.
	ended := c.observeDataQueueLocked(time.Now(), len(c.dataChan))
	c.muDataOverflow.Unlock()
	for _, ind := range ended {
		c.notifyDataQueueCongestion(ind)
	}
}

func (c *Association) observeDataQueueLocked(now time.Time, queued int) []DataQueueCongestionIndication {
	if !c.dataOverflow.Load() {
		return nil
	}
	revision := c.dataOverflowRefills.Load()
	if revision != c.dataOverflowSeenRefill {
		c.dataOverflowSeenRefill = revision
		c.dataOverflowLowSince = time.Time{}
	}
	if queued > cap(c.dataChan)/dataOverflowDrainDivisor {
		c.dataOverflowLowSince = time.Time{}
		return nil
	}
	if c.dataOverflowLowSince.IsZero() {
		c.dataOverflowLowSince = now
		return nil
	}
	if now.Sub(c.dataOverflowLowSince) < dataOverflowDrainDwell {
		return nil
	}
	// A refill can arrive after the sample above without taking the tracker.
	// Commit end only if that sample's revision is still current.
	if !c.dataOverflowRefills.CompareAndSwap(revision, revision+1) {
		c.dataOverflowLowSince = time.Time{}
		return nil
	}
	c.dataOverflow.Store(false)
	ended := make([]DataQueueCongestionIndication, 0, len(c.dataOverflowScopes))
	for _, ind := range c.dataOverflowScopes {
		ind.Congested = false
		ended = append(ended, ind)
	}
	clear(c.dataOverflowScopes)
	c.dataOverflowTrackingLost = false
	c.dataOverflowLowSince = time.Time{}
	// RFC 4666 Section 3.4.4: the international method omits Congestion
	// Indications, so it cannot encode a level-zero clear. Local recovery sends
	// nothing; the peer needs its validity timer or audit/current MTP procedure.
	return ended
}

func (c *Association) watchDataOverflow() {
	ticker := time.NewTicker(dataOverflowCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.done:
			c.muDataOverflow.Lock()
			c.dataOverflowWorker = false
			c.muDataOverflow.Unlock()
			return
		case <-ticker.C:
			c.observeDataQueue()
			c.muDataOverflow.Lock()
			if !c.dataOverflow.Load() {
				c.dataOverflowWorker = false
				c.muDataOverflow.Unlock()
				return
			}
			c.muDataOverflow.Unlock()
		}
	}
}

// writeOverflowSCON preserves the received NA/RC while using normal SSNM live
// state and delivery barriers (RFC 4666 Sections 4.3.1 and 5.6.2). Lock order is
// the same as WriteSignal: delivery barrier, then sendMu. Recheck state after
// waiting for the writer; withdrawal may already have marked the direction down.
func (c *Association) writeOverflowSCON(overflow *DataQueueOverflowError) error {
	message := c.overflowSCON(overflow)
	raw, err := message.MarshalBinary()
	if err != nil {
		return err
	}
	release, err := c.lockOutboundSSNMContexts(message.RoutingContext)
	if err != nil {
		return err
	}
	defer release()
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	c.muDataOverflow.Lock()
	defer c.muDataOverflow.Unlock()
	if !c.dataOverflow.Load() || overflow.generation != c.dataOverflowGeneration {
		return nil
	}
	if err := c.overflowSCONScopeActive(message.RoutingContext); err != nil {
		return err
	}
	// The generation check and transport admission share the tracker lock with
	// episode end. No writer wait holds that lock. Optional SCON never waits for
	// socket space, so recovery cannot pass an admitted onset and then announce
	// end before its transport submission.
	if c.signalWriter != nil {
		_, err = c.signalWriter(message)
		return err
	}
	info := *c.sctpInfo
	_, err = c.controlSender().SendMsg(raw, sctp.SendOptions{Info: &info, NoWait: true})
	return err
}

func (c *Association) overflowSCONScopeActive(routingContext *params.Param) error {
	if err := c.validateOutboundSSNMRoutingContext(routingContext); err != nil {
		return err
	}
	state := c.State()
	if c.isIPSPDoubleExchange() {
		state = c.localIPSPStateValue()
	}
	if state != StateASPActive {
		return ErrNotEstablished
	}
	contexts := c.configuredLocalRoutingContexts()
	if routingContext != nil {
		contexts = routingContext.RoutingContexts()
	}
	for _, rc := range contexts {
		active := c.outboundRoutingContextActive(rc)
		if c.isIPSPDoubleExchange() {
			active = c.routingContextAcked(rc) && !c.routingContextOverridden(rc)
		}
		if !active {
			return ErrRoutingContextNotActive
		}
	}
	return nil
}

func (c *Association) overflowSCON(e *DataQueueOverflowError) *messages.SignallingCongestion {
	var na, rc, concerned *params.Param
	if e.Scope.NetworkAppearanceSet {
		na = params.NewNetworkAppearance(e.Scope.NetworkAppearance)
	}
	if e.Scope.RoutingContextSet {
		rc = params.NewRoutingContext(e.Scope.RoutingContexts...)
	}
	// RFC 4666 Section 3.4.4 uses Concerned Destination only ASP -> SGP,
	// naming the originator of the DATA that triggered this report.
	if c.role == RoleASP {
		concerned = params.NewConcernedDestination(e.OriginatingPointCode)
	}
	return messages.NewSignallingCongestion(na, rc,
		params.NewAffectedPointCodeWithMask(0, e.DestinationPointCode), concerned, nil, nil)
}
