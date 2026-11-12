// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"errors"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// RFC 4666 §3.4.1 permits omitted RC; §4.5.1 permits DUNA, DRST and
// SCON before ASP Active Ack, and §4.6 permits DAVA during restart too.
func TestContextlessActivationAdmitsEarlySSNM(t *testing.T) {
	for _, test := range []struct {
		name         string
		send         func(*Association) error
		availability DestinationAvailability
		congestion   bool
	}{
		{"DUNA", func(a *Association) error {
			return a.handleDestinationUnavailable(messages.NewDestinationUnavailable(nil, nil, apc(100), nil))
		}, DestinationUnavailable, false},
		{"DRST", func(a *Association) error {
			return a.handleDestinationRestricted(messages.NewDestinationRestricted(nil, nil, apc(100), nil))
		}, DestinationRestricted, false},
		{"SCON", func(a *Association) error {
			return a.handleSignallingCongestion(messages.NewSignallingCongestion(nil, nil, apc(100), nil, params.NewCongestionIndications(2), nil))
		}, DestinationAvailable, true},
		{"DAVA", func(a *Association) error {
			return a.handleDestinationAvailable(messages.NewDestinationAvailable(nil, nil, apc(100), nil))
		}, DestinationAvailable, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			a, _ := newTestConnWithContexts(t, StateASPInactive, RoleASP)
			observeSSNM(t, a)
			a.cfg.TAck = time.Hour
			request := a.startTAck(messages.NewAspActive(nil, nil, nil), requestAspActive)
			if request == nil || !request.routingContextOmitted || len(a.configuredRoutingContexts()) != 0 {
				t.Fatal("fixture is not a pending contextless activation")
			}
			if test.name == "DAVA" {
				seedDestinationAvailability(a, 100, DestinationUnavailable)
			}
			if err := test.send(a); err != nil {
				t.Fatalf("permitted early %s rejected: %v", test.name, err)
			}
			partition := a.ssnmPartitionFor(contextlessASKeyForConfig(a.cfg))
			knowledge := ssnmPartitionKnowledge(t, a.endpoint.SSNMKnowledge(), partition)
			if knowledge.TrafficAuthorized || len(knowledge.Bindings) != 1 || !knowledge.Bindings[0].Pending {
				t.Fatalf("early knowledge authorized traffic: %+v", knowledge)
			}
			report := nextSSNMReport(t, a)
			if report.Scope.RoutingContextSet {
				t.Fatalf("report invented an RC: %+v", report.Scope)
			}
			if got := retainedAvailability(a, 100); got != test.availability {
				t.Fatalf("availability=%v, want %v", got, test.availability)
			}
			if test.congestion {
				congestion := reportedSSNMCongestion(t, report)
				if !congestion.Congested || congestion.Level != 2 || retainedDestinationState(a, 100).Congestion != congestion {
					t.Fatalf("congestion=%+v", congestion)
				}
			}
			if err := a.handleAspActiveAck(messages.NewAspActiveAck(nil, nil, nil)); err != nil {
				t.Fatal(err)
			}
			a.sendState(StateASPActive)
			knowledge = ssnmPartitionKnowledge(t, a.endpoint.SSNMKnowledge(), partition)
			if !knowledge.TrafficAuthorized || len(knowledge.Destinations) != 1 {
				t.Fatalf("Ack lost or failed to activate knowledge: %+v", knowledge)
			}
			if got := retainedAvailability(a, 100); got != test.availability {
				t.Fatalf("Ack lost early state: %v", got)
			}
			if test.congestion && retainedDestinationState(a, 100).Congestion != reportedSSNMCongestion(t, report) {
				t.Fatal("Ack lost early congestion")
			}
			if err := test.send(a); err != nil {
				t.Fatalf("SSNM after activation: %v", err)
			}
		})
	}
}

func TestContextlessEarlySSNMRequiresOutstandingActivation(t *testing.T) {
	for _, state := range []State{StateASPDown, StateASPInactive} {
		t.Run(state.String(), func(t *testing.T) {
			a, _ := newTestConnWithContexts(t, state, RoleASP)
			var unexpected *UnexpectedMessageError
			if err := a.handleDestinationUnavailable(messages.NewDestinationUnavailable(nil, nil, apc(100), nil)); !errors.As(err, &unexpected) {
				t.Fatalf("DUNA without pending activation: %v", err)
			}
			if got := retainedAvailability(a, 100); got != DestinationAvailable {
				t.Fatalf("unrequested DUNA changed state: %v", got)
			}
		})
	}
}

func TestContextlessEarlySSNMRejectsUnconfiguredRoutingContext(t *testing.T) {
	a, _ := newTestConnWithContexts(t, StateASPInactive, RoleASP)
	a.cfg.TAck = time.Hour
	a.startTAck(messages.NewAspActive(nil, nil, nil), requestAspActive)
	// RFC 4666 §3.8.1: the offending RC must be returned in Invalid Routing Context.
	err := a.handleDestinationUnavailable(messages.NewDestinationUnavailable(nil, params.NewRoutingContext(99), apc(100), nil))
	var scopeError *RoutingContextError
	if !errors.As(err, &scopeError) || !errors.Is(err, ErrInvalidRoutingContext) || len(scopeError.Contexts) != 1 || scopeError.Contexts[0] != 99 {
		t.Fatalf("unknown RC: %v", err)
	}
	if got := retainedAvailability(a, 100); got != DestinationAvailable {
		t.Fatalf("unknown RC changed state: %v", got)
	}
}

func TestContextlessActivationBindsPendingSSNMScope(t *testing.T) {
	a, _ := newTestConnWithContexts(t, StateASPInactive, RoleASP)
	if scopes := a.ssnmBindingScopes(); len(scopes) != 0 {
		t.Fatalf("inactive scopes=%+v", scopes)
	}
	a.cfg.TAck = time.Hour
	request := a.startTAck(messages.NewAspActive(nil, nil, nil), requestAspActive)
	scopes := a.ssnmBindingScopes()
	if len(scopes) != 1 || !scopes[0].pending || scopes[0].partition != a.ssnmPartitionFor(contextlessASKeyForConfig(a.cfg)) {
		t.Fatalf("pending contextless binding=%+v", scopes)
	}
	a.cancelTAckRequest(request)
	var unexpected *UnexpectedMessageError
	if err := a.handleDestinationUnavailable(messages.NewDestinationUnavailable(nil, nil, apc(100), nil)); !errors.As(err, &unexpected) {
		t.Fatalf("canceled activation still admits DUNA: %v", err)
	}
	if scopes := a.ssnmBindingScopes(); len(scopes) != 0 {
		t.Fatalf("canceled activation retained scopes=%+v", scopes)
	}
}
