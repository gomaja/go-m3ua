// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

// A Routing Context is unique within each registration direction. Network
// Appearance and the registration lifetime remain in each captured token.
type sconBindingID struct {
	routingContext uint32
	local          bool
}

type sconSubmissionTracker struct {
	mu                  sync.Mutex
	active              map[*sconSubmission]struct{}
	changing            map[sconBindingID]chan struct{}
	optionalChanging    chan struct{}
	optionalChanges     int
	forgetChanges       int
	knowledgeGeneration atomic.Uint64
}

type sconSubmission struct {
	optionalAudit bool
	ctx           context.Context
	cancel        context.CancelCauseFunc
	done          chan struct{}
	bindings      []queueCongestionBinding
}

// beginSCONSubmission runs after sendMu. A change in progress has already
// cancelled the previous send; joining it waits only for that cancellation and
// short binding bookkeeping, never for peer progress. Check the original token
// after the change commits (RFC 4666 §§4.4.1, 4.4.2).
func (c *Association) beginSCONSubmission(bindings []queueCongestionBinding) (*sconSubmission, error) {
	if len(bindings) == 0 {
		return nil, nil
	}
	tracker := &c.sconSubmissions
	for {
		tracker.mu.Lock()
		var changing <-chan struct{}
		for _, binding := range bindings {
			if gate := tracker.changing[sconBindingID{binding.key.RoutingContext, binding.local}]; binding.key.RoutingContextSet && gate != nil {
				changing = gate
				break
			}
		}
		if changing != nil {
			tracker.mu.Unlock()
			<-changing
			continue
		}
		c.muDynamicASKeys.RLock()
		current := true
		for _, binding := range bindings {
			if !c.queueCongestionBindingCurrent(binding) {
				current = false
				break
			}
		}
		c.muDynamicASKeys.RUnlock()
		if !current {
			tracker.mu.Unlock()
			return nil, ErrRoutingContextNotActive
		}
		ctx, cancel := context.WithCancelCause(context.Background())
		submission := &sconSubmission{ctx: ctx, cancel: cancel, done: make(chan struct{}), bindings: bindings}
		if tracker.active == nil {
			tracker.active = make(map[*sconSubmission]struct{})
		}
		tracker.active[submission] = struct{}{}
		tracker.mu.Unlock()
		return submission, nil
	}
}

// finishSCONSubmission publishes transport completion before any binding
// replacement commits. It acquires no state, TAck, registry or delivery lock.
func (c *Association) finishSCONSubmission(submission *sconSubmission) {
	if submission == nil {
		return
	}
	tracker := &c.sconSubmissions
	tracker.mu.Lock()
	delete(tracker.active, submission)
	close(submission.done)
	tracker.mu.Unlock()
	submission.cancel(nil)
}

func sconSubmissionCancelled(submission *sconSubmission, err error) bool {
	return submission != nil && errors.Is(err, context.Canceled) && errors.Is(context.Cause(submission.ctx), ErrRoutingContextNotActive)
}

func sconSubmissionError(submission *sconSubmission, err error) error {
	if sconSubmissionCancelled(submission, err) {
		return ErrRoutingContextNotActive
	}
	return err
}

// Binding changes fence only the selected registration directions/RCs. The
// dependency's SendMsgContext cancels a buffer or send-lock wait, preserves the
// latest application deadline, and leaves no pending send after returning.
// Known acceptance wins cancellation, so an accepted current SCON settles before
// the replacement commits. Tracker and dynamic mutexes are released while joining.
// RKM correlation and lifecycle caller locks can remain held, including on the
// receive dispatcher for late REG/DEREG responses. A cancelled optional audit
// completes through the tracker alone; it never acquires either RKM lock.
type sconBindingChange struct {
	id              sconBindingID
	removing        bool
	expectedVersion *uint64
}

func (c *Association) beginSCONBindingChange(routingContext uint32, local, removing bool, expectedVersion *uint64) (func(), bool) {
	id := sconBindingID{routingContext, local}
	release, admitted := c.beginSCONBindingChanges([]sconBindingChange{{id, removing, expectedVersion}})
	return release, admitted[id]
}

// Acquire the whole event's gates together. Never wait while owning a subset:
// overlapping multi-context events must not depend on one another's gates.
// History and jobs are retired once before any replacement becomes visible.
func (c *Association) beginSCONBindingChanges(changes []sconBindingChange) (func(), map[sconBindingID]bool) {
	tracker := &c.sconSubmissions
	for {
		tracker.mu.Lock()
		var blocked <-chan struct{}
		for _, change := range changes {
			if gate := tracker.changing[change.id]; gate != nil {
				blocked = gate
				break
			}
		}
		if blocked != nil {
			tracker.mu.Unlock()
			<-blocked
			continue
		}
		admitted := make(map[sconBindingID]bool, len(changes))
		c.muDynamicASKeys.RLock()
		for _, change := range changes {
			keys, versions := c.dynamicPeerASKeys, c.dynamicPeerASKeyVersions
			if change.id.local {
				keys, versions = c.dynamicLocalASKeys, c.dynamicLocalASKeyVersions
			}
			_, present := keys[change.id.routingContext]
			if !change.removing || present && (change.expectedVersion == nil || versions[change.id.routingContext] == *change.expectedVersion) {
				admitted[change.id] = true
			}
		}
		c.muDynamicASKeys.RUnlock()
		if len(admitted) == 0 {
			tracker.mu.Unlock()
			return noDeliveryBarrier, admitted
		}
		if tracker.changing == nil {
			tracker.changing = make(map[sconBindingID]chan struct{})
		}
		gate := make(chan struct{})
		received := make(map[uint32]struct{}, len(admitted))
		for id := range admitted {
			tracker.changing[id] = gate
			if !id.local {
				received[id.routingContext] = struct{}{}
			}
		}
		var pending []*sconSubmission
		for submission := range tracker.active {
			for _, binding := range submission.bindings {
				if binding.key.RoutingContextSet && admitted[sconBindingID{binding.key.RoutingContext, binding.local}] {
					submission.cancel(ErrRoutingContextNotActive)
					pending = append(pending, submission)
					break
				}
			}
		}
		tracker.mu.Unlock()
		for _, submission := range pending {
			<-submission.done
		}
		// Local overload follows only the outbound registration direction. The
		// whole admitted set is fenced once, including when an event also names
		// the other DE direction or a stale version that was not admitted.
		localDirection := c.isIPSPDoubleExchange()
		localChange := false
		for id := range admitted {
			localChange = localChange || id.local == localDirection
		}
		if localChange {
			c.retireLocalCongestionInputs(func(key ASKey) bool {
				return key.RoutingContextSet && admitted[sconBindingID{key.RoutingContext, localDirection}]
			}, false)
		}
		changedPartitions := c.retireReceivedRegistrations(received)
		peerChange := c.role == RoleSGP && len(received) > 0
		if peerChange {
			// Peer admission does not use the SCON tracker. Keep its snapshot mutex
			// through map publication so old tokens cannot be restored in this gap.
			// All tracked submissions have completed before acquiring this mutex.
			c.peerRetentionMu.Lock()
			if store := c.ssnmStore(); store != nil {
				store.retirePeerCongestionContexts(c.ID(), received)
			}
		}
		return func() {
			if peerChange {
				c.peerRetentionMu.Unlock()
			}
			tracker.mu.Lock()
			for id := range admitted {
				delete(tracker.changing, id)
			}
			close(gate)
			tracker.mu.Unlock()
			c.projectReceivedChanges(changedPartitions)
		}, admitted
	}
}
