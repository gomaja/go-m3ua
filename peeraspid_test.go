// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestSGPSavesThePeerASPIdentifier(t *testing.T) {
	registry := newApplicationServers(time.Hour)
	conn, _ := asTestConn(t, registry, StateASPDown, 1)
	conn.cfg.ASPIdentifier = params.NewAspIdentifier(0xaaaa)

	if err := conn.handleAspUp(messages.NewAspUp(params.NewAspIdentifier(0x1234), nil)); err != nil {
		t.Fatalf("handleAspUp() error = %v", err)
	}
	if got, ok := conn.PeerASPIdentifier(); !ok || got != 0x1234 {
		t.Errorf("PeerASPIdentifier() = %#x, %v; want 0x1234, true", got, ok)
	}
}

func TestDuplicatePeerASPIdentifierIsRejected(t *testing.T) {
	registry := newApplicationServers(time.Hour)
	first, _ := asTestConn(t, registry, StateASPDown, 1)
	second, secondSent := asTestConn(t, registry, StateASPDown, 1)

	if err := first.handleAspUp(messages.NewAspUp(params.NewAspIdentifier(7), nil)); err != nil {
		t.Fatalf("first handleAspUp() error = %v", err)
	}
	err := second.handleAspUp(messages.NewAspUp(params.NewAspIdentifier(7), nil))
	if !errors.Is(err, ErrInvalidASPIdentifier) {
		t.Fatalf("duplicate handleAspUp() error = %v, want ErrInvalidAspIdentifier", err)
	}
	if _, ok := second.PeerASPIdentifier(); ok {
		t.Error("the rejected association saved the duplicate ASP Identifier")
	}
	if got := countType(*secondSent, "ASP Up Ack"); got != 0 {
		t.Errorf("duplicate ASP Identifier drew %d ASP Up Acks, want none", got)
	}
	if err := second.handleErrors(err); err != nil {
		t.Fatal(err)
	}
	if codes := errorCodes(*secondSent); len(codes) != 1 || codes[0] != params.ErrInvalidAspIdentifier {
		t.Errorf("error codes = %v, want [%d] (Invalid ASP Identifier)", codes, params.ErrInvalidAspIdentifier)
	}
}

func TestASPIdentifierNeedOnlyBeUniqueWithinAnAS(t *testing.T) {
	registry := newApplicationServers(time.Hour)
	first, _ := asTestConn(t, registry, StateASPDown, 1)
	second, _ := asTestConn(t, registry, StateASPDown, 2)

	for index, conn := range []*Association{first, second} {
		if err := conn.handleAspUp(messages.NewAspUp(params.NewAspIdentifier(7), nil)); err != nil {
			t.Fatalf("connection %d handleAspUp() error = %v", index, err)
		}
	}
}

func TestDedicatedAssociationsStillRequireUniqueASPIdentifiers(t *testing.T) {
	registry := newApplicationServers(time.Hour)
	first, _ := asTestConn(t, registry, StateASPDown)
	second, _ := asTestConn(t, registry, StateASPDown)

	if err := first.handleAspUp(messages.NewAspUp(params.NewAspIdentifier(7), nil)); err != nil {
		t.Fatalf("first handleAspUp() error = %v", err)
	}
	if err := second.handleAspUp(messages.NewAspUp(params.NewAspIdentifier(7), nil)); !errors.Is(err, ErrInvalidASPIdentifier) {
		t.Fatalf("duplicate dedicated handleAspUp() error = %v, want ErrInvalidAspIdentifier", err)
	}
}

func TestConcurrentDuplicateASPIdentifierClaimsHaveOneWinner(t *testing.T) {
	registry := newApplicationServers(time.Hour)
	first, _ := asTestConn(t, registry, StateASPDown, 1)
	second, _ := asTestConn(t, registry, StateASPDown, 1)
	first.signalWriter = func(message messages.M3UA) (int, error) { return message.MarshalLen(), nil }
	second.signalWriter = first.signalWriter

	start := make(chan struct{})
	results := make(chan error, 2)
	var wait sync.WaitGroup
	for _, conn := range []*Association{first, second} {
		wait.Add(1)
		go func(conn *Association) {
			defer wait.Done()
			<-start
			results <- conn.handleAspUp(messages.NewAspUp(params.NewAspIdentifier(9), nil))
		}(conn)
	}
	close(start)
	wait.Wait()
	close(results)

	accepted, rejected := 0, 0
	for err := range results {
		switch {
		case err == nil:
			accepted++
		case errors.Is(err, ErrInvalidASPIdentifier):
			rejected++
		default:
			t.Fatalf("unexpected claim result: %v", err)
		}
	}
	if accepted != 1 || rejected != 1 {
		t.Errorf("accepted %d and rejected %d duplicate claims; want one each", accepted, rejected)
	}
}

func TestClosedRegistryRejectsNewASPIdentifierClaims(t *testing.T) {
	registry := newApplicationServers(time.Hour)
	conn, _ := asTestConn(t, registry, StateASPDown, 1)
	registry.close()

	if registry.claimASPIdentifier(conn, 7) {
		t.Fatal("closed Application Server registry accepted a new ASP Identifier")
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if len(registry.aspIdentifiers) != 0 {
		t.Fatalf("closed registry retained %d ASP Identifier claims, want 0", len(registry.aspIdentifiers))
	}
}

func TestOverrideNotifyNamesTheRemoteOverridingASP(t *testing.T) {
	registry := newApplicationServers(time.Hour)
	incumbent, incumbentSent := asTestConn(t, registry, StateASPActive, 1)
	incumbent.cfg.TrafficModeType = params.NewTrafficModeType(params.TrafficModeOverride)

	challenger, _ := asTestConn(t, registry, StateASPInactive, 1)
	challenger.cfg.TrafficModeType = params.NewTrafficModeType(params.TrafficModeOverride)
	challenger.cfg.ASPIdentifier = params.NewAspIdentifier(0xaaaa)
	if err := challenger.handleAspUp(messages.NewAspUp(params.NewAspIdentifier(0x1234), nil)); err != nil {
		t.Fatalf("handleAspUp() error = %v", err)
	}

	before := len(notifies(*incumbentSent))
	if err := challenger.handleAspActive(messages.NewAspActive(
		params.NewTrafficModeType(params.TrafficModeOverride),
		params.NewRoutingContext(1), nil,
	)); err != nil {
		t.Fatalf("handleAspActive() error = %v", err)
	}
	notifications := notifies(*incumbentSent)
	if len(notifications) <= before {
		t.Fatal("the displaced ASP received no Notify")
	}
	got := notifications[len(notifications)-1].AspIdentifier
	if got == nil || got.AspIdentifier() != 0x1234 {
		t.Errorf("Alternate ASP Active named %v, want the remote overriding ASP 0x1234", got)
	}
}

func TestActiveASPsAreOrderedBySavedPeerIdentifier(t *testing.T) {
	registry := newApplicationServers(time.Hour)
	first, _ := asTestConn(t, registry, StateASPActive, 1)
	second, _ := asTestConn(t, registry, StateASPActive, 1)

	if err := first.handleAspUp(messages.NewAspUp(params.NewAspIdentifier(20), nil)); err == nil {
		t.Fatal("ASP Up received while active should also report Unexpected Message")
	}
	if err := second.handleAspUp(messages.NewAspUp(params.NewAspIdentifier(10), nil)); err == nil {
		t.Fatal("ASP Up received while active should also report Unexpected Message")
	}
	ordered := registry.get(1).activeASPs()
	if len(ordered) != 2 || ordered[0] != second || ordered[1] != first {
		t.Errorf("active ASP order = %p, %p; want peer IDs 10 then 20", ordered[0], ordered[1])
	}
}

func TestRemovingAnASPNotifiesItsSurvivingPeersOfFailure(t *testing.T) {
	registry := newApplicationServers(time.Hour)
	failed, _ := asTestConn(t, registry, StateASPActive, 1)
	_, survivorSent := asTestConn(t, registry, StateASPActive, 1)
	if err := failed.handleAspUp(messages.NewAspUp(params.NewAspIdentifier(0x55), nil)); err == nil {
		t.Fatal("ASP Up received while active should also report Unexpected Message")
	}

	before := len(notifies(*survivorSent))
	registry.forget(failed)
	notifications := notifies(*survivorSent)
	if len(notifications) <= before {
		t.Fatal("surviving ASP received no ASP Failure Notify")
	}

	var failure *messages.Notify
	for _, notification := range notifications[before:] {
		statusType, information := statusOf(t, notification)
		if statusType == params.Other && information == uint16(params.AspFailure&0xffff) {
			failure = notification
			break
		}
	}
	if failure == nil {
		t.Fatal("surviving ASP received no Notify with status ASP Failure")
	}
	if failure.AspIdentifier == nil || failure.AspIdentifier.AspIdentifier() != 0x55 {
		t.Errorf("ASP Failure named %v, want failed peer ASP Identifier 0x55", failure.AspIdentifier)
	}
	if got := failure.RoutingContext.RoutingContexts(); len(got) != 1 || got[0] != 1 {
		t.Errorf("ASP Failure Routing Contexts = %v, want [1]", got)
	}
}

func TestFailureDrivenASStateNotifyNamesTheFailedASP(t *testing.T) {
	registry := newApplicationServers(time.Hour)
	failed, _ := asTestConn(t, registry, StateASPActive, 1)
	_, survivorSent := asTestConn(t, registry, StateASPInactive, 1)
	if err := failed.handleAspUp(messages.NewAspUp(params.NewAspIdentifier(0x55), nil)); err == nil {
		t.Fatal("ASP Up received while active should also report Unexpected Message")
	}

	before := len(notifies(*survivorSent))
	registry.forget(failed)
	for _, notification := range notifies(*survivorSent)[before:] {
		statusType, information := statusOf(t, notification)
		if statusType != params.AsStateChange || information != uint16(params.AsStatePending&0xffff) {
			continue
		}
		if notification.AspIdentifier == nil || notification.AspIdentifier.AspIdentifier() != 0x55 {
			t.Errorf("failure-driven AS-PENDING named %v, want failed ASP Identifier 0x55", notification.AspIdentifier)
		}
		return
	}
	t.Fatal("surviving ASP received no failure-driven AS-PENDING Notify")
}

func TestRemovingAnASPDownPeerDoesNotReportASPFailure(t *testing.T) {
	registry := newApplicationServers(time.Hour)
	failed, _ := asTestConn(t, registry, StateASPDown, 1)
	_, survivorSent := asTestConn(t, registry, StateASPActive, 1)

	before := len(notifies(*survivorSent))
	registry.forget(failed)
	for _, notification := range notifies(*survivorSent)[before:] {
		statusType, information := statusOf(t, notification)
		if statusType == params.Other && information == uint16(params.AspFailure&0xffff) {
			t.Fatal("removing an ASP already down emitted ASP Failure")
		}
	}
}

func TestForgottenASPIdentifierCanBeReused(t *testing.T) {
	registry := newApplicationServers(time.Hour)
	first, _ := asTestConn(t, registry, StateASPDown, 1)
	if err := first.handleAspUp(messages.NewAspUp(params.NewAspIdentifier(7), nil)); err != nil {
		t.Fatal(err)
	}
	registry.forget(first)

	replacement, _ := asTestConn(t, registry, StateASPDown, 1)
	if err := replacement.handleAspUp(messages.NewAspUp(params.NewAspIdentifier(7), nil)); err != nil {
		t.Errorf("replacement handleAspUp() error = %v; forgotten identifier should be reusable", err)
	}
}

func TestASStateNotifyDoesNotInventAnASPIdentifier(t *testing.T) {
	registry := newApplicationServers(time.Hour)
	conn, sent := asTestConn(t, registry, StateASPInactive, 1)
	conn.cfg.ASPIdentifier = params.NewAspIdentifier(0xaaaa)

	notifications := notifies(*sent)
	if len(notifications) == 0 {
		t.Fatal("AS state change produced no Notify")
	}
	if got := notifications[len(notifications)-1].AspIdentifier; got != nil {
		t.Errorf("AS state Notify invented ASP Identifier %v", got)
	}
}
