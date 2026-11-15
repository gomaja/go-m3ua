// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"sync"
)

// A Routing Context is unique within each registration direction. Network
// Appearance and the registration lifetime remain in each captured token.
type sconBindingID struct {
	routingContext uint32
	local          bool
}

type sconSubmissionTracker struct {
	mu       sync.Mutex
	active   map[*sconSubmission]struct{}
	changing map[sconBindingID]chan struct{}
}

type sconSubmission struct {
	ctx      context.Context
	cancel   context.CancelCauseFunc
	done     chan struct{}
	bindings []queueCongestionBinding
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

// beginSCONBindingChange fences only this registration direction/RC. The
// dependency's SendMsgContext cancels a buffer or send-lock wait, preserves the
// latest application deadline, and leaves no pending send after returning.
// Known acceptance wins cancellation, so an accepted current SCON settles before
// the replacement commits. No short mutex is retained while joining the call.
func (c *Association) beginSCONBindingChange(routingContext uint32, local, removing bool, expectedVersion *uint64) (func(), bool) {
	tracker := &c.sconSubmissions
	id := sconBindingID{routingContext, local}
	for {
		tracker.mu.Lock()
		if gate := tracker.changing[id]; gate != nil {
			tracker.mu.Unlock()
			<-gate
			continue
		}
		if removing {
			c.muDynamicASKeys.RLock()
			keys, versions := c.dynamicPeerASKeys, c.dynamicPeerASKeyVersions
			if local {
				keys, versions = c.dynamicLocalASKeys, c.dynamicLocalASKeyVersions
			}
			_, present := keys[routingContext]
			current := present && (expectedVersion == nil || versions[routingContext] == *expectedVersion)
			c.muDynamicASKeys.RUnlock()
			if !current {
				tracker.mu.Unlock()
				return noDeliveryBarrier, false
			}
		}
		if tracker.changing == nil {
			tracker.changing = make(map[sconBindingID]chan struct{})
		}
		gate := make(chan struct{})
		tracker.changing[id] = gate
		var pending []*sconSubmission
		for submission := range tracker.active {
			for _, binding := range submission.bindings {
				if binding.key.RoutingContextSet && binding.key.RoutingContext == routingContext && binding.local == local {
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
		return func() { tracker.mu.Lock(); delete(tracker.changing, id); close(gate); tracker.mu.Unlock() }, true
	}
}
