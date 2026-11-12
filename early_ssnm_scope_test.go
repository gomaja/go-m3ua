// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"strconv"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func pendingSSNMFixture(t *testing.T, contexts ...uint32) (*Association, *pendingRequest) {
	t.Helper()
	var keys []ASKey
	if len(contexts) == 0 {
		keys = []ASKey{{}}
	} else {
		for _, rc := range contexts {
			keys = append(keys, ASKey{RoutingContext: rc, RoutingContextSet: true})
		}
	}
	servers := make([]RemoteASConfig, 0, len(keys))
	for index := range keys {
		servers = append(servers, RemoteASConfig{ID: RemoteASID("as" + strconv.Itoa(index)), ASKey: &keys[index]})
	}
	e := newSSNMStateEndpoint(t, &ASPConfig{SignallingGateways: []SignallingGatewayConfig{{ID: "sg", SGPs: []SignallingGatewayProcessConfig{{ID: "sgp", ApplicationServers: servers}}}}}, nil)
	a, _ := newTestConnWithContexts(t, StateASPInactive, RoleASP, contexts...)
	a.cfg.PeerSGP = &SGPIdentity{SignallingGateway: "sg", SignallingGatewayProcess: "sgp"}
	if !e.trackAssociation(a) {
		t.Fatal("track association")
	}
	observeSSNM(t, a)
	a.cfg.TAck = time.Hour
	a.recvStream.Store(0)
	var rc *params.Param
	if len(contexts) > 0 {
		rc = params.NewRoutingContext(contexts...)
	}
	return a, a.startTAck(messages.NewAspActive(nil, rc, nil), requestAspActive)
}

func TestEarlySSNMKnowledgeSurvivesActivationCancellation(t *testing.T) {
	for _, expiry := range []bool{false, true} {
		t.Run(map[bool]string{false: "cancel", true: "expiry"}[expiry], func(t *testing.T) {
			a, request := pendingSSNMFixture(t)
			if err := a.handleDestinationUnavailable(messages.NewDestinationUnavailable(nil, nil, apc(100), nil)); err != nil {
				t.Fatal(err)
			}
			if expiry {
				a.forgetTAckRequest(request, requestAspActive, ErrTAckExpired)
			} else {
				a.cancelTAckRequest(request)
			}
			if a.ssnmAllowedDuringActivation() {
				t.Error("retired request still supplies the early window")
			}
			if retainedAvailability(a, 100) != DestinationUnavailable {
				t.Error("retirement erased the SGP's latest destination report")
			}
			knowledge := a.endpoint.SSNMKnowledge()
			if len(knowledge.Partitions) != 1 || len(knowledge.Partitions[0].Destinations) != 1 {
				t.Fatalf("retirement erased retained knowledge: %+v", knowledge)
			}
			if knowledge.Partitions[0].TrafficAuthorized {
				t.Fatal("retained early knowledge authorized traffic")
			}
			a.startTAck(messages.NewAspActive(nil, nil, nil), requestAspActive)
			if err := a.handleAspActiveAck(messages.NewAspActiveAck(nil, nil, nil)); err != nil {
				t.Fatal(err)
			}
			a.sendState(StateASPActive)
			if retainedAvailability(a, 100) != DestinationUnavailable {
				t.Fatal("reactivation lost the SGP's latest report")
			}
		})
	}
}

func TestActivationRefusalRemainsALayerManagementIndication(t *testing.T) {
	for _, diagnostic := range []string{"absent", "truncated", "exact", "ambiguous"} {
		t.Run(diagnostic, func(t *testing.T) {
			a, request := pendingSSNMFixture(t)
			management := a.ManagementIndications()
			raw, err := request.msg.MarshalBinary()
			if err != nil {
				t.Fatal(err)
			}
			var parameter *params.Param
			switch diagnostic {
			case "truncated":
				parameter = params.NewDiagnosticInformation(raw[:4])
			case "exact", "ambiguous":
				parameter = params.NewDiagnosticInformation(raw)
			}
			if diagnostic == "ambiguous" {
				a.startTAck(messages.NewAspActive(nil, nil, nil), requestAspActive)
			}
			if err := a.handleError(messages.NewError(params.NewErrorCode(params.ErrUnsupportedTrafficModeType), nil, nil, nil, parameter)); err != nil {
				t.Fatal(err)
			}
			select {
			case indication := <-management:
				if indication.Kind != ManagementError || indication.ErrorCode != params.ErrUnsupportedTrafficModeType {
					t.Fatalf("LM indication: %+v", indication)
				}
			default:
				t.Fatal("refusal not reported to Layer Management")
			}
			if !a.ssnmAllowedDuringActivation() {
				t.Error("optional diagnostic bytes retired the activation")
			}
			select {
			case <-request.stop:
				t.Error("refusal stopped ASPAC retries without a transaction identifier")
			default:
			}
		})
	}
}

func TestWithdrawalStopsPartiallyAcknowledgedActivationBatch(t *testing.T) {
	for _, down := range []bool{false, true} {
		t.Run(map[bool]string{false: "inactive", true: "down"}[down], func(t *testing.T) {
			a, request := pendingSSNMFixture(t, 1, 2)
			independent := a.startTAck(messages.NewAspActive(nil, params.NewRoutingContext(2), nil), requestAspActive)
			if err := a.handleAspActiveAck(messages.NewAspActiveAck(nil, params.NewRoutingContext(1), nil)); err != nil {
				t.Fatal(err)
			}
			a.sendState(StateASPActive)
			if down {
				a.startTAck(messages.NewAspDown(nil), requestAspDown)
				if err := a.handleAspDownAck(messages.NewAspDownAck(nil)); err != nil {
					t.Fatal(err)
				}
			} else {
				a.startTAck(messages.NewAspInactive(params.NewRoutingContext(1), nil), requestAspInactive)
				if err := a.handleAspInactiveAck(messages.NewAspInactiveAck(params.NewRoutingContext(1), nil)); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case <-request.stop:
			default:
				t.Fatal("withdrawn RC survives in the ASPAC retransmission batch")
			}
			select {
			case <-independent.stop:
				if !down {
					t.Error("scoped withdrawal stopped an independent ASPAC")
				}
			default:
				if down {
					t.Error("ASP Down kept an independent ASPAC pending")
				}
			}
			a.cancelTAckRequest(independent)
			a.sendState(StateASPInactive)
			if a.ssnmAllowedDuringActivation() {
				t.Fatal("withdrawn ASPAC reopened the early window")
			}
		})
	}
}

func TestOmittedActivationIncludesContextlessAndRegisteredScopes(t *testing.T) {
	e, a, key, _ := newDynamicallyRegisteredASPManagementFixture(t, RoleASP, StateASPInactive)
	a.cfg.TAck = time.Hour
	observeSSNM(t, a)
	a.startTAck(messages.NewAspActive(nil, nil, nil), requestAspActive)
	if err := a.handleDestinationUnavailable(messages.NewDestinationUnavailable(params.NewNetworkAppearance(key.NetworkAppearance), params.NewRoutingContext(key.RoutingContext), apc(101), nil)); err != nil {
		t.Fatal(err)
	}
	knowledge := e.SSNMKnowledge()
	found := false
	for _, partition := range knowledge.Partitions {
		if partition.TrafficAuthorized {
			t.Error("registered early scope authorized traffic")
		}
		for _, destination := range partition.Destinations {
			if destination.Destination.PointCode == 101 {
				found = true
			}
		}
	}
	if !found {
		t.Error("omitted ASPAC did not admit its registered RC")
	}
	if err := a.handleDestinationUnavailable(messages.NewDestinationUnavailable(nil, nil, apc(100), nil)); err != nil {
		t.Fatal(err)
	}
	found = false
	for _, partition := range e.SSNMKnowledge().Partitions {
		for _, destination := range partition.Destinations {
			if destination.Destination.PointCode == 100 {
				found = true
			}
		}
	}
	if !found {
		t.Error("omitted ASPAC lost early contextless knowledge beside its registered RC")
	}
	scopes := a.ssnmASKeys(WireScope{})
	if len(scopes) != 1 || scopes[0].RoutingContextSet {
		t.Fatalf("omitted SSNM lost its contextless scope: %+v", scopes)
	}
}

func TestEarlySSNMInFlightReportRemainsPendingAfterCancellation(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "contextless", true: "explicit"}[explicit], func(t *testing.T) {
			var contexts []uint32
			var rc *params.Param
			if explicit {
				contexts = []uint32{1}
				rc = params.NewRoutingContext(1)
			}
			a, request := pendingSSNMFixture(t, contexts...)
			reached, release := make(chan struct{}), make(chan struct{})
			a.endpoint.aspRoutes.applyResolved = func() { close(reached); <-release }
			done := make(chan error, 1)
			go func() {
				done <- a.handleDestinationUnavailable(messages.NewDestinationUnavailable(nil, rc, apc(100), nil))
			}()
			select {
			case <-reached:
			case <-time.After(time.Second):
				close(release)
				t.Fatal("report did not reach cache publication")
			}
			a.cancelTAckRequest(request)
			close(release)
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if a.ssnmAllowedDuringActivation() {
				t.Error("cancelled request still opens the window")
			}
			knowledge := a.endpoint.SSNMKnowledge()
			if len(knowledge.Partitions) != 1 || len(knowledge.Partitions[0].Destinations) != 1 {
				t.Fatalf("admitted report was discarded: %+v", knowledge)
			}
			if knowledge.Partitions[0].TrafficAuthorized || !knowledge.Partitions[0].Bindings[0].Pending {
				t.Fatalf("in-flight report became active after cancellation: %+v", knowledge)
			}
			if retainedAvailability(a, 100) != DestinationUnavailable {
				t.Fatal("admitted report lost its private cache update")
			}
		})
	}
}

func TestEarlySSNMDiscardsDifferentNetworkAppearance(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "contextless", true: "explicit"}[explicit], func(t *testing.T) {
			key := ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true}
			var contexts []uint32
			var rc *params.Param
			if explicit {
				key.RoutingContext, key.RoutingContextSet = 1, true
				contexts = []uint32{1}
				rc = params.NewRoutingContext(1)
			}
			e := newSSNMStateEndpoint(t, &ASPConfig{SignallingGateways: []SignallingGatewayConfig{{ID: "sg", SGPs: []SignallingGatewayProcessConfig{{ID: "sgp", ApplicationServers: []RemoteASConfig{{ID: "as", ASKey: &key}}}}}}}, nil)
			a, _ := newTestConnWithContexts(t, StateASPInactive, RoleASP, contexts...)
			a.cfg.TAck = time.Hour
			setInventoryNetworkAppearance(&a.cfg.ApplicationServers, params.NewNetworkAppearance(7))
			a.cfg.PeerSGP = &SGPIdentity{SignallingGateway: "sg", SignallingGatewayProcess: "sgp"}
			if !e.trackAssociation(a) {
				t.Fatal("track association")
			}
			a.startTAck(messages.NewAspActive(nil, rc, nil), requestAspActive)
			if err := a.handleDestinationUnavailable(messages.NewDestinationUnavailable(params.NewNetworkAppearance(9), rc, apc(100), nil)); err != nil {
				t.Fatal(err)
			}
			for _, p := range e.SSNMKnowledge().Partitions {
				if len(p.Destinations) > 0 || p.TrafficAuthorized {
					t.Errorf("NA mismatch mutated canonical state: %+v", p)
				}
			}
			if retainedAvailabilityForNetwork(a, 9, 100) != DestinationAvailable {
				t.Error("NA mismatch mutated private state")
			}
		})
	}
}

func TestEarlySSNMWindowClosesOnActualTAckExhaustion(t *testing.T) {
	a, _ := newTestConnWithContexts(t, StateASPInactive, RoleASP)
	observeSSNM(t, a)
	a.cfg.TAck = 25 * time.Millisecond
	a.cfg.TAckRetries = 1
	retried := make(chan struct{}, 1)
	a.signalWriter = func(message messages.M3UA) (int, error) { retried <- struct{}{}; return message.MarshalLen(), nil }
	request := a.startTAck(messages.NewAspActive(nil, nil, nil), requestAspActive)
	if err := a.handleDestinationUnavailable(messages.NewDestinationUnavailable(nil, nil, apc(100), nil)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-request.result:
		if err != ErrTAckExpired {
			t.Fatalf("expiry: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("T(ack) did not expire")
	}
	select {
	case <-retried:
	default:
		t.Error("runTAck never retransmitted ASPAC")
	}
	if a.ssnmAllowedDuringActivation() {
		t.Error("exhausted T(ack) still supplies the early window")
	}
	if retainedAvailability(a, 100) != DestinationUnavailable {
		t.Error("T(ack) exhaustion erased the latest report")
	}
}

func TestBlockedRetryResetDoesNotStallAnotherAssociationSSNM(t *testing.T) {
	a, _ := newTestConnWithContexts(t, StateASPInactive, RoleASP)
	observeSSNM(t, a)
	b, _ := newTestConnWithContexts(t, StateASPInactive, RoleASP)
	if !a.endpoint.trackAssociation(b) {
		t.Fatal("track sibling")
	}
	b.cfg.TAck = time.Hour
	b.startTAck(messages.NewAspActive(nil, nil, nil), requestAspActive)
	a.cfg.TAck = 5 * time.Millisecond
	a.cfg.TAckRetries = 1
	written, release := make(chan struct{}), make(chan struct{})
	a.signalWriter = func(message messages.M3UA) (int, error) { close(written); <-release; return message.MarshalLen(), nil }
	a.startTAck(messages.NewAspActive(nil, nil, nil), requestAspActive)
	select {
	case <-written:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("retry did not reach the writer")
	}
	resetDone := make(chan struct{})
	go func() { a.resetTAckEpoch(); close(resetDone) }()
	select {
	case <-resetDone:
		close(release)
		t.Fatal("reset did not drain the blocked retry")
	case <-time.After(20 * time.Millisecond):
	}
	reportDone := make(chan error, 1)
	go func() {
		reportDone <- b.handleDestinationUnavailable(messages.NewDestinationUnavailable(nil, nil, apc(100), nil))
	}()
	progressed := false
	select {
	case err := <-reportDone:
		progressed = true
		if err != nil {
			t.Error(err)
		}
	case <-time.After(time.Second):
		t.Error("one blocked retry stalled a sibling Association's SSNM")
	}
	close(release)
	<-resetDone
	if !progressed {
		if err := <-reportDone; err != nil {
			t.Error(err)
		}
	}
}

func TestOmittedActivationWaitsForItsContextlessAcknowledgement(t *testing.T) {
	_, a, key, _ := newDynamicallyRegisteredASPManagementFixture(t, RoleASP, StateASPInactive)
	a.cfg.TAck = time.Hour
	a.startTAck(messages.NewAspActive(nil, nil, nil), requestAspActive)
	if err := a.handleAspActiveAck(messages.NewAspActiveAck(nil, params.NewRoutingContext(key.RoutingContext), nil)); err != nil {
		t.Fatal(err)
	}
	a.setState(StateASPActive)
	if !a.pendingTAckContextless(requestAspActive) {
		t.Fatal("Ack for registered RC closed the unacknowledged contextless window")
	}
	if err := a.handleAspActiveAck(messages.NewAspActiveAck(nil, nil, nil)); err != nil {
		t.Fatal(err)
	}
	if a.pendingTAck() != 0 {
		t.Fatal("omitted Ack did not complete the whole activation")
	}
}

func TestEarlyContextlessSSNMMatchesNetworkAfterRegisteredAck(t *testing.T) {
	e, a, key, _ := newDynamicallyRegisteredASPManagementFixture(t, RoleASP, StateASPInactive)
	setInventoryNetworkAppearance(&a.cfg.ApplicationServers, params.NewNetworkAppearance(7))
	a.cfg.TAck = time.Hour
	a.startTAck(messages.NewAspActive(nil, nil, nil), requestAspActive)
	if err := a.handleAspActiveAck(messages.NewAspActiveAck(nil, params.NewRoutingContext(key.RoutingContext), nil)); err != nil {
		t.Fatal(err)
	}
	a.setState(StateASPActive)
	if err := a.handleDestinationUnavailable(messages.NewDestinationUnavailable(params.NewNetworkAppearance(9), nil, apc(100), nil)); err != nil {
		t.Fatal(err)
	}
	for _, partition := range e.SSNMKnowledge().Partitions {
		if len(partition.Destinations) != 0 {
			t.Fatalf("registered Ack bypassed pending contextless NA validation: %+v", partition)
		}
	}
}
