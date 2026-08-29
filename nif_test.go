// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"errors"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// TestAspUpWhileIsolatedFromTheNIFIsRefused covers the first guideline of RFC
// 4666 Section 4.7:
//
//	If an SGP is isolated entirely from the NIF, the SGP should send
//	ASP Down Ack to all its connected ASPs.  Upon receiving an ASP Up
//	message while isolated from the NIF, the SGP should respond with an
//	Error ("Refused - Management Blocking").
//
// Error code 0x0d was never sent by any path, because there was nothing that
// could record the SGP being cut off from the SS7 network in the first place.
func TestAspUpWhileIsolatedFromTheNIFIsRefused(t *testing.T) {
	sgp, sent := newTestConn(t, StateASPDown, RoleSGP)
	sgp.nif = &nifAvailability{}
	sgp.nif.setIsolated(true)

	err := sgp.handleAspUp(messages.NewAspUp(sgp.cfg.ASPIdentifier, nil))
	if err == nil {
		t.Fatal("an ASP Up was accepted while the SGP was isolated from the NIF")
	}
	if !errors.Is(err, ErrManagementBlocking) {
		t.Fatalf("error = %v, want ErrManagementBlocking", err)
	}
	for _, name := range typeNames(*sent) {
		if name == "ASP Up Ack" {
			t.Error("an ASP Up Ack was sent while isolated; the SGP has no route " +
				"to the SS7 network to bring up")
		}
	}

	if e := sgp.handleErrors(err); e != nil {
		t.Fatal(e)
	}
	codes := errorCodes(*sent)
	if len(codes) != 1 || codes[0] != params.ErrRefusedManagementBlocking {
		t.Errorf("error code = %v, want [%d] (Refused - Management Blocking)",
			codes, params.ErrRefusedManagementBlocking)
	}
}

// With the NIF back, the same ASP Up is answered normally.
func TestAspUpIsAcceptedOnceTheNIFReturns(t *testing.T) {
	sgp, sent := newTestConn(t, StateASPDown, RoleSGP)
	sgp.nif = &nifAvailability{}
	sgp.nif.setIsolated(true)
	sgp.nif.setIsolated(false)

	if err := sgp.handleAspUp(messages.NewAspUp(sgp.cfg.ASPIdentifier, nil)); err != nil {
		t.Fatalf("handleAspUp: %v", err)
	}
	if got := typeNames(*sent); len(got) != 1 || got[0] != "ASP Up Ack" {
		t.Errorf("sent %v, want [ASP Up Ack]", got)
	}
}

// TestAspActiveForAnUnservicableASIsRefused covers the partial-failure
// guideline of the same section:
//
//	Upon receiving an ASP Active message for an affected AS while still
//	partially isolated from the NIF, the SGP should respond with an
//	Error ("Refused - Management Blocking").
func TestAspActiveForAnUnservicableASIsRefused(t *testing.T) {
	sgp, sent := newTestConn(t, StateASPInactive, RoleSGP)
	sgp.nif = &nifAvailability{}
	sgp.nif.setASAvailable(1, false)

	err := sgp.handleAspActive(messages.NewAspActive(
		params.NewTrafficModeType(params.TrafficModeLoadshare),
		params.NewRoutingContext(1), nil))
	if err == nil {
		t.Fatal("an ASP Active was accepted for an AS the SGP cannot service")
	}
	if !errors.Is(err, ErrManagementBlocking) {
		t.Fatalf("error = %v, want ErrManagementBlocking", err)
	}
	for _, name := range typeNames(*sent) {
		if name == "ASP Active Ack" {
			t.Error("an ASP Active Ack was sent for an AS with no route to the " +
				"SS7 network; the SGP would be promising traffic it cannot carry")
		}
	}
}

// An Application Server the SGP can still service is unaffected, so a partial
// failure stays partial.
func TestAspActiveForAServicableASIsUnaffected(t *testing.T) {
	sgp, sent := newTestConn(t, StateASPInactive, RoleSGP)
	sgp.cfg.RoutingContexts = params.NewRoutingContext(1, 2)
	sgp.nif = &nifAvailability{}
	sgp.nif.setASAvailable(2, false) // a different AS is the broken one

	if err := sgp.handleAspActive(messages.NewAspActive(
		params.NewTrafficModeType(params.TrafficModeLoadshare),
		params.NewRoutingContext(1), nil)); err != nil {
		t.Fatalf("ASP Active for a servicable AS was refused: %v", err)
	}
	if got := typeNames(*sent); len(got) != 1 || got[0] != "ASP Active Ack" {
		t.Errorf("sent %v, want [ASP Active Ack]", got)
	}
}

// TestSetNIFAvailableTellsEveryASP covers the other half of the first
// guideline: "the SGP should send ASP Down Ack to all its connected ASPs".
func TestSetNIFAvailableTellsEveryASP(t *testing.T) {
	config := newSGPAssociationConfigForTest(
		&HeartbeatInfo{Enabled: false},
		0x111111, 0x222222, 1, params.TrafficModeLoadshare, 0, 0,
		[]uint32{1}, 3, 2, 1, 0,
	)
	l := newSGPListener(NewListenerConfig(config))

	var sentPerConn []*[]messages.M3UA
	for i := 0; i < 2; i++ {
		c, sent := newTestConn(t, StateASPActive, RoleSGP)
		c.cfg.RoutingContexts = params.NewRoutingContext(1)
		if !l.track(c) {
			t.Fatal("track refused an association")
		}
		sentPerConn = append(sentPerConn, sent)
	}

	if err := l.SetNIFAvailable(false); err != nil {
		t.Fatalf("SetNIFAvailable: %v", err)
	}

	for i, sent := range sentPerConn {
		found := false
		for _, name := range typeNames(*sent) {
			if name == "ASP Down Ack" {
				found = true
			}
		}
		if !found {
			t.Errorf("ASP #%d was not sent an ASP Down Ack when the SGP lost its "+
				"NIF (sent %v)", i+1, typeNames(*sent))
		}
	}
}

// The partial case tells only the ASPs serving the affected Application Server.
func TestSetASAvailableTellsOnlyTheAffectedASPs(t *testing.T) {
	config := newSGPAssociationConfigForTest(
		&HeartbeatInfo{Enabled: false},
		0x111111, 0x222222, 1, params.TrafficModeLoadshare, 0, 0,
		[]uint32{1}, 3, 2, 1, 0,
	)
	l := newSGPListener(NewListenerConfig(config))

	affected, affectedSent := newTestConn(t, StateASPActive, RoleSGP)
	affected.cfg.RoutingContexts = params.NewRoutingContext(1)
	l.track(affected)

	other, otherSent := newTestConn(t, StateASPActive, RoleSGP)
	other.cfg.RoutingContexts = params.NewRoutingContext(2)
	l.track(other)

	if err := l.SetASAvailable(1, false); err != nil {
		t.Fatalf("SetASAvailable: %v", err)
	}

	found := false
	for _, name := range typeNames(*affectedSent) {
		if name == "ASP Inactive Ack" {
			found = true
		}
	}
	if !found {
		t.Errorf("the ASP serving the affected AS was not told (sent %v)",
			typeNames(*affectedSent))
	}
	for _, name := range typeNames(*otherSent) {
		if name == "ASP Inactive Ack" {
			t.Error("an ASP serving an unaffected AS was deactivated; the failure " +
				"is partial and must stay so")
		}
	}
}
