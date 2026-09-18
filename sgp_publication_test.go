// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// newSGPOwnerEndpoint is an SGP Endpoint that owns the destination state, the
// Application Server registry and the MTP3 restart registry its Listeners and
// dialed Associations share.
func newSGPOwnerEndpoint(t *testing.T) *Endpoint {
	t.Helper()
	endpoint, err := NewEndpoint(EndpointConfig{
		Role:               RoleSGP,
		SGP:                &SGPConfig{},
		ApplicationServers: &ApplicationServerConfig{RecoveryTimer: time.Hour},
	})
	if err != nil {
		t.Fatalf("NewEndpoint(RoleSGP): %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	return endpoint
}

// addOwnedListener attaches a Listener to an SGP Endpoint without a socket. The
// Listener shares the Endpoint's registries, which is the ownership this slice
// is about.
func addOwnedListener(
	t *testing.T,
	endpoint *Endpoint,
	networkAppearance uint32,
	routingContexts ...uint32,
) *Listener {
	t.Helper()
	config := newSGPAssociationConfigForTest(
		&HeartbeatInfo{Enabled: false}, 0, params.TrafficModeLoadshare,
		networkAppearance, routingContexts,
	)
	listener := newListener(endpoint, NewListenerConfig(config))
	listener.as, listener.nif, listener.destinations = listener.registry()
	if !endpoint.trackListener(listener) {
		t.Fatal("Endpoint refused to track the Listener")
	}
	t.Cleanup(func() { endpoint.forgetListener(listener) })
	return listener
}

// addActiveASP attaches an ASP to a Listener and takes it ASP-ACTIVE in the
// named Application Servers, which is what makes it a concerned ASP for SSNM.
func addActiveASP(
	t *testing.T,
	listener *Listener,
	networkAppearance uint32,
	routingContexts ...uint32,
) (*Association, *distributionCapture) {
	t.Helper()
	asp, sent := addDistributionASP(t, listener, StateASPInactive, routingContexts...)
	asp.listener = listener
	asp.mtp3Restarts = listener.mtp3Restarts
	asp.destinations = listener.destinations
	asp.noteRoutingContextsActive(routingContexts)
	asp.setState(StateASPActive)
	for _, routingContext := range routingContexts {
		proactiveSSNMApplicationServer(listener, networkAppearance, routingContext).
			setASPState(asp, StateASPActive, time.Hour)
	}
	sent.reset()
	return asp, sent
}

// failWrites makes an ASP refuse every message, which is how a partial fan-out
// is produced deterministically.
func failWrites(asp *Association, sent *distributionCapture, cause error) {
	asp.signalWriter = func(message messages.M3UA) (int, error) {
		_, _ = sent.write(message)
		return 0, cause
	}
}

// failWritesFor refuses only the messages naming one Affected Point Code, so a
// completion can succeed for one destination and fail for another.
func failWritesFor(asp *Association, sent *distributionCapture, pointCode uint32, cause error) {
	asp.signalWriter = func(message messages.M3UA) (int, error) {
		_, _ = sent.write(message)
		if affectedPointCodeOf(message) == pointCode {
			return 0, cause
		}
		return message.MarshalLen(), nil
	}
}

func affectedPointCodeOf(message messages.M3UA) uint32 {
	var affected *params.Param
	switch typed := message.(type) {
	case *messages.DestinationUnavailable:
		affected = typed.AffectedPointCode
	case *messages.DestinationAvailable:
		affected = typed.AffectedPointCode
	case *messages.DestinationRestricted:
		affected = typed.AffectedPointCode
	case *messages.SignallingCongestion:
		affected = typed.AffectedPointCode
	default:
		return 0
	}
	codes := affected.AffectedPointCodes()
	if len(codes) == 0 {
		return 0
	}
	return codes[0]
}

func ownerDestination(networkAppearance, routingContext, pointCode uint32, mask uint8) AffectedDestination {
	return AffectedDestination{
		NetworkAppearance:    networkAppearance,
		NetworkAppearanceSet: true,
		RoutingContext:       routingContext,
		RoutingContextSet:    true,
		PointCode:            pointCode,
		Mask:                 mask,
	}
}

func ownerStatusKey(networkAppearance, routingContext, pointCode uint32, mask uint8) DestinationStatusKey {
	return DestinationStatusKey{
		NetworkAppearance:    networkAppearance,
		NetworkAppearanceSet: true,
		RoutingContext:       routingContext,
		RoutingContextSet:    true,
		PointCode:            pointCode,
		Mask:                 mask,
	}
}

// --- Exact scoped publication, omission and explicit zero, invalid requests ---

func TestEndpointAvailabilityPublicationUsesTheExactRequestedScope(t *testing.T) {
	for _, test := range []struct {
		name         string
		availability DestinationAvailability
		kind         any
	}{
		{"unavailable", DestinationUnavailable, (*messages.DestinationUnavailable)(nil)},
		{"available", DestinationAvailable, (*messages.DestinationAvailable)(nil)},
		{"restricted", DestinationRestricted, (*messages.DestinationRestricted)(nil)},
	} {
		t.Run(test.name, func(t *testing.T) {
			endpoint := newSGPOwnerEndpoint(t)
			listener := addOwnedListener(t, endpoint, 7, 1, 2)
			concerned, concernedSent := addActiveASP(t, listener, 7, 1)
			unrelated, unrelatedSent := addActiveASP(t, listener, 7, 2)

			if err := reportAvailability(
				endpoint, testWireScope(7, true, 1), 0x123456, 8, test.availability,
			); err != nil {
				t.Fatalf("ReportDestinationAvailability: %v", err)
			}

			written := ssnmMessages(concernedSent.snapshot())
			if len(written) != 1 {
				t.Fatalf("concerned ASP received %d SSNM messages, want 1", len(written))
			}
			if !sameSSNMKind(written[0], test.kind) {
				t.Fatalf("availability %v emitted %T, want %T", test.availability, written[0], test.kind)
			}
			networkAppearance, routingContext, affectedPointCode := ssnmScope(t, written[0])
			if networkAppearance == nil || networkAppearance.NetworkAppearance() != 7 {
				t.Fatalf("Network Appearance = %v, want 7", networkAppearance)
			}
			if got := routingContext.RoutingContexts(); len(got) != 1 || got[0] != 1 {
				t.Fatalf("Routing Contexts = %v, want [1]", got)
			}
			if got := affectedPointCode.AffectedPointCodes(); len(got) != 1 || got[0] != 0x123456 {
				t.Fatalf("Affected Point Codes = %#v, want [0x123456]", got)
			}
			if got := affectedPointCode.AffectedPointCodeMasks(); len(got) != 1 || got[0] != 8 {
				t.Fatalf("Affected Point Code masks = %v, want [8]", got)
			}
			if got := len(ssnmMessages(unrelatedSent.snapshot())); got != 0 {
				t.Fatalf("unrelated Application Server's ASP received %d SSNM messages, want 0", got)
			}
			if unrelated.Err() != nil || concerned.Err() != nil {
				t.Fatalf("publication closed an Association: %v / %v", concerned.Err(), unrelated.Err())
			}
		})
	}
}

func TestEndpointAvailabilityPublicationResolvesAnOmittedNetworkAppearance(t *testing.T) {
	endpoint := newSGPOwnerEndpoint(t)
	listener := addOwnedListener(t, endpoint, 7, 1)
	_, sent := addActiveASP(t, listener, 7, 1)

	if err := reportAvailability(
		endpoint, testWireScope(0, false, 1), 0x123456, 0, DestinationUnavailable,
	); err != nil {
		t.Fatalf("ReportDestinationAvailability with an omitted Network Appearance: %v", err)
	}

	written := ssnmMessages(sent.snapshot())
	if len(written) != 1 {
		t.Fatalf("ASP received %d SSNM messages, want 1", len(written))
	}
	// The scope on the wire is the exact scope the caller named: RFC 4666
	// Section 1.4.2.1 makes Network Appearance a local value, so an omitted one
	// stays omitted.
	networkAppearance, _, _ := ssnmScope(t, written[0])
	if networkAppearance != nil {
		t.Fatalf("omitted Network Appearance was sent as %v", networkAppearance)
	}
	// The record is nonetheless keyed by the appearance the Endpoint resolved,
	// so the audit that follows answers in the scope the ASP is configured for.
	status, ok := endpoint.DestinationStatus(ownerStatusKey(7, 1, 0x123456, 0))
	if !ok || status.State.Availability != DestinationUnavailable {
		t.Fatalf("DestinationStatus = (%+v, %t), want Unavailable", status, ok)
	}
}

func TestEndpointAvailabilityPublicationKeepsExplicitZeroNetworkAppearance(t *testing.T) {
	endpoint := newSGPOwnerEndpoint(t)
	listener := addOwnedListener(t, endpoint, 0, 1)
	_, sent := addActiveASP(t, listener, 0, 1)

	if err := reportAvailability(
		endpoint, testWireScope(0, true, 1), 0x123456, 0, DestinationUnavailable,
	); err != nil {
		t.Fatalf("ReportDestinationAvailability with explicit Network Appearance 0: %v", err)
	}

	written := ssnmMessages(sent.snapshot())
	if len(written) != 1 {
		t.Fatalf("ASP received %d SSNM messages, want 1", len(written))
	}
	// RFC 4666 Section 3.3.1 makes Network Appearance optional and zero a
	// legitimate value, so an explicit zero is carried rather than omitted.
	networkAppearance, _, _ := ssnmScope(t, written[0])
	if networkAppearance == nil {
		t.Fatal("explicit Network Appearance 0 was omitted from the message")
	}
	if got := networkAppearance.NetworkAppearance(); got != 0 {
		t.Fatalf("Network Appearance = %d, want 0", got)
	}
}

func TestEndpointAvailabilityPublicationRejectsInvalidRequests(t *testing.T) {
	endpoint := newSGPOwnerEndpoint(t)
	listener := addOwnedListener(t, endpoint, 7, 1)
	addActiveASP(t, listener, 7, 1)

	aspEndpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP})
	if err != nil {
		t.Fatalf("NewEndpoint(RoleASP): %v", err)
	}
	t.Cleanup(func() { _ = aspEndpoint.Close() })

	longInfo := make([]byte, 256)
	for index := range longInfo {
		longInfo[index] = 'a'
	}

	for _, test := range []struct {
		name     string
		endpoint *Endpoint
		request  DestinationAvailabilityRequest
		want     error
	}{
		{
			name:     "ASP role",
			endpoint: aspEndpoint,
			request: DestinationAvailabilityRequest{
				Scope:        testWireScope(7, true, 1),
				Destinations: []PointCodeRange{{PointCode: 0x123456}},
			},
			want: ErrUnsupportedRole,
		},
		{
			name:     "availability out of range",
			endpoint: endpoint,
			request: DestinationAvailabilityRequest{
				Scope:        testWireScope(7, true, 1),
				Destinations: []PointCodeRange{{PointCode: 0x123456}},
				Availability: DestinationAvailability(9),
			},
			want: ErrInvalidParameterValue,
		},
		{
			name:     "no destination",
			endpoint: endpoint,
			request: DestinationAvailabilityRequest{
				Scope: testWireScope(7, true, 1),
			},
			want: ErrMissingAffectedPointCode,
		},
		{
			name:     "point code out of range",
			endpoint: endpoint,
			request: DestinationAvailabilityRequest{
				Scope:        testWireScope(7, true, 1),
				Destinations: []PointCodeRange{{PointCode: 0x01000000}},
			},
			want: ErrInvalidParameterValue,
		},
		{
			name:     "mask out of range",
			endpoint: endpoint,
			request: DestinationAvailabilityRequest{
				Scope:        testWireScope(7, true, 1),
				Destinations: []PointCodeRange{{PointCode: 0x123456, Mask: 25}},
			},
			want: ErrInvalidParameterValue,
		},
		{
			name:     "empty Routing Context list",
			endpoint: endpoint,
			request: DestinationAvailabilityRequest{
				Scope:        WireScope{RoutingContextSet: true},
				Destinations: []PointCodeRange{{PointCode: 0x123456}},
			},
			want: ErrMissingRoutingContext,
		},
		{
			name:     "duplicate Routing Context",
			endpoint: endpoint,
			request: DestinationAvailabilityRequest{
				Scope:        testWireScope(7, true, 1, 1),
				Destinations: []PointCodeRange{{PointCode: 0x123456}},
			},
			want: ErrInvalidParameterValue,
		},
		{
			name:     "unknown Routing Context",
			endpoint: endpoint,
			request: DestinationAvailabilityRequest{
				Scope:        testWireScope(7, true, 9),
				Destinations: []PointCodeRange{{PointCode: 0x123456}},
			},
			want: ErrInvalidRoutingContext,
		},
		{
			name:     "wrong Network Appearance",
			endpoint: endpoint,
			request: DestinationAvailabilityRequest{
				Scope:        testWireScope(9, true, 1),
				Destinations: []PointCodeRange{{PointCode: 0x123456}},
			},
			want: ErrInvalidNetworkAppearance,
		},
		{
			name:     "Info String too long",
			endpoint: endpoint,
			request: DestinationAvailabilityRequest{
				Scope:        testWireScope(7, true, 1),
				Destinations: []PointCodeRange{{PointCode: 0x123456}},
				Info:         string(longInfo),
			},
			want: ErrInvalidParameterValue,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := test.endpoint.ReportDestinationAvailability(test.request); !errors.Is(err, test.want) {
				t.Fatalf("ReportDestinationAvailability error = %v, want %v", err, test.want)
			}
		})
	}
}

// --- Owner authority across several children ---

func TestEndpointPublicationSpansEveryChildWithoutCrossSignalling(t *testing.T) {
	endpoint := newSGPOwnerEndpoint(t)
	first := addOwnedListener(t, endpoint, 7, 1)
	second := addOwnedListener(t, endpoint, 7, 2)
	firstASP, firstSent := addActiveASP(t, first, 7, 1)
	secondASP, secondSent := addActiveASP(t, second, 7, 2)

	// A dialed SGP Association belongs to no Listener and shares the same
	// Endpoint-owned registries.
	dialed, dialedSent := addDistributionASP(t, second, StateASPInactive, 2)
	dialed.listener = nil
	dialed.endpoint = endpoint
	dialed.destinations = endpoint.destinations
	dialed.mtp3Restarts = endpoint.mtp3Restarts
	dialed.noteRoutingContextsActive([]uint32{2})
	dialed.setState(StateASPActive)
	proactiveSSNMApplicationServer(second, 7, 2).setASPState(dialed, StateASPActive, time.Hour)
	dialedSent.reset()
	firstSent.reset()
	secondSent.reset()

	if err := reportAvailability(
		endpoint, testWireScope(7, true, 1), 0x123456, 0, DestinationUnavailable,
	); err != nil {
		t.Fatalf("ReportDestinationAvailability for Routing Context 1: %v", err)
	}
	if got := len(ssnmMessages(firstSent.snapshot())); got != 1 {
		t.Fatalf("first Listener's ASP received %d SSNM messages, want 1", got)
	}
	for name, sent := range map[string]*distributionCapture{
		"second Listener's ASP": secondSent,
		"dialed Association":    dialedSent,
	} {
		if got := len(ssnmMessages(sent.snapshot())); got != 0 {
			t.Fatalf("%s received %d SSNM messages for an unrelated Routing Context, want 0", name, got)
		}
	}
	for name, association := range map[string]*Association{
		"first":  firstASP,
		"second": secondASP,
		"dialed": dialed,
	} {
		if association.Err() != nil {
			t.Fatalf("%s Association was closed by an unrelated publication: %v", name, association.Err())
		}
	}

	firstSent.reset()
	secondSent.reset()
	dialedSent.reset()
	if err := reportAvailability(
		endpoint, testWireScope(7, true, 2), 0x123456, 0, DestinationUnavailable,
	); err != nil {
		t.Fatalf("ReportDestinationAvailability for Routing Context 2: %v", err)
	}
	for name, sent := range map[string]*distributionCapture{
		"second Listener's ASP": secondSent,
		"dialed Association":    dialedSent,
	} {
		if got := len(ssnmMessages(sent.snapshot())); got != 1 {
			t.Fatalf("%s received %d SSNM messages, want 1", name, got)
		}
	}
	if got := len(ssnmMessages(firstSent.snapshot())); got != 0 {
		t.Fatalf("first Listener's ASP received %d SSNM messages for an unrelated Routing Context, want 0", got)
	}
}

func TestEndpointPublicationReportsPartialFanOutWithoutReplayingSuccess(t *testing.T) {
	endpoint := newSGPOwnerEndpoint(t)
	listener := addOwnedListener(t, endpoint, 7, 1)
	healthy, healthySent := addActiveASP(t, listener, 7, 1)
	broken, brokenSent := addActiveASP(t, listener, 7, 1)
	writeFailure := errors.New("peer refused the SSNM batch")
	failWrites(broken, brokenSent, writeFailure)

	err := reportAvailability(endpoint, testWireScope(7, true, 1), 0x123456, 0, DestinationUnavailable)
	var delivery *SSNMDeliveryError
	if !errors.As(err, &delivery) {
		t.Fatalf("ReportDestinationAvailability error = %v, want *SSNMDeliveryError", err)
	}
	if !errors.Is(err, writeFailure) {
		t.Fatalf("partial fan-out error does not unwrap to the write failure: %v", err)
	}
	if len(delivery.Successful) != 1 || delivery.Successful[0] != healthy.ID() {
		t.Fatalf("successful associations = %v, want [%d]", delivery.Successful, healthy.ID())
	}
	if len(delivery.Failed) != 1 || delivery.Failed[0].Association != broken.ID() {
		t.Fatalf("failed associations = %+v, want [%d]", delivery.Failed, broken.ID())
	}
	if got := len(ssnmMessages(healthySent.snapshot())); got != 1 {
		t.Fatalf("healthy ASP received %d SSNM messages, want 1", got)
	}
}

// --- Dimension independence and dimension-complete snapshots ---

func TestEndpointAvailabilityAndCongestionMoveIndependently(t *testing.T) {
	endpoint := newSGPOwnerEndpoint(t)
	listener := addOwnedListener(t, endpoint, 7, 1)
	_, sent := addActiveASP(t, listener, 7, 1)
	scope := testWireScope(7, true, 1)
	key := ownerStatusKey(7, 1, 0x123456, 0)

	if err := reportAvailability(endpoint, scope, 0x123456, 0, DestinationUnavailable); err != nil {
		t.Fatalf("report unavailable: %v", err)
	}
	if err := reportCongestion(endpoint, scope, 0x123456, 0, 2, true); err != nil {
		t.Fatalf("report congestion: %v", err)
	}
	// RFC 4666 Section 4.5.2.2 keeps the two as separate statuses of the same
	// destination, so the congestion report leaves the destination unavailable.
	assertOwnerState(t, endpoint, key, DestinationNetworkState{
		Availability: DestinationUnavailable,
		Congestion:   CongestionState{Congested: true, Level: 2, LevelSet: true},
	})

	sent.reset()
	if err := reportAvailability(endpoint, scope, 0x123456, 0, DestinationAvailable); err != nil {
		t.Fatalf("report available: %v", err)
	}
	// Returning the destination to service says nothing about congestion, so
	// the retained level stands and no abating SCON is emitted.
	assertOwnerState(t, endpoint, key, DestinationNetworkState{
		Availability: DestinationAvailable,
		Congestion:   CongestionState{Congested: true, Level: 2, LevelSet: true},
	})
	written := ssnmMessages(sent.snapshot())
	if len(written) != 1 {
		t.Fatalf("returning to service emitted %d SSNM messages, want 1", len(written))
	}
	if !sameSSNMKind(written[0], (*messages.DestinationAvailable)(nil)) {
		t.Fatalf("returning to service emitted %T, want *messages.DestinationAvailable", written[0])
	}

	sent.reset()
	if err := reportCongestion(endpoint, scope, 0x123456, 0, 0, true); err != nil {
		t.Fatalf("report congestion abatement: %v", err)
	}
	// Section 3.4.4 makes level 0 "No Congestion or Undefined", so an explicit
	// zero abates congestion and does not touch reachability.
	assertOwnerState(t, endpoint, key, DestinationNetworkState{
		Availability: DestinationAvailable,
		Congestion:   CongestionState{Congested: false, Level: 0, LevelSet: true},
	})

	if err := reportCongestion(endpoint, scope, 0x123456, 0, 0, false); err != nil {
		t.Fatalf("report congestion without a level: %v", err)
	}
	// An absent Congestion Indications parameter is congestion without a level,
	// which the same section keeps distinct from an explicit zero.
	assertOwnerState(t, endpoint, key, DestinationNetworkState{
		Availability: DestinationAvailable,
		Congestion:   CongestionState{Congested: true, Level: 0, LevelSet: false},
	})
}

func assertOwnerState(t *testing.T, endpoint *Endpoint, key DestinationStatusKey, want DestinationNetworkState) {
	t.Helper()
	status, ok := endpoint.DestinationStatus(key)
	if !ok {
		t.Fatalf("DestinationStatus(%+v) not found", key)
	}
	if status.State != want {
		t.Fatalf("DestinationStatus state = %+v, want %+v", status.State, want)
	}
	for _, snapshot := range endpoint.DestinationStatuses() {
		if snapshot.Key != key {
			continue
		}
		if snapshot.State != want {
			t.Fatalf("DestinationStatuses state = %+v, want %+v", snapshot.State, want)
		}
		return
	}
	t.Fatalf("DestinationStatuses does not contain %+v", key)
}

func TestEndpointDestinationSnapshotsAreOwnedByTheCaller(t *testing.T) {
	endpoint := newSGPOwnerEndpoint(t)
	listener := addOwnedListener(t, endpoint, 7, 1)
	addActiveASP(t, listener, 7, 1)
	scope := testWireScope(7, true, 1)
	if err := reportAvailability(endpoint, scope, 0x123456, 0, DestinationUnavailable); err != nil {
		t.Fatalf("report unavailable: %v", err)
	}

	first := endpoint.DestinationStatuses()
	if len(first) != 1 {
		t.Fatalf("DestinationStatuses returned %d snapshots, want 1", len(first))
	}
	first[0].State.Availability = DestinationAvailable
	first[0].Key.PointCode = 0
	second := endpoint.DestinationStatuses()
	if len(second) != 1 {
		t.Fatalf("second DestinationStatuses returned %d snapshots, want 1", len(second))
	}
	if second[0].Key.PointCode != 0x123456 || second[0].State.Availability != DestinationUnavailable {
		t.Fatalf("mutating a returned snapshot changed the Endpoint's state: %+v", second[0])
	}
}

// --- MTP3 restart ---

func TestEndpointRestartReturnsALiveHandleOnPartialIsolationFanOut(t *testing.T) {
	endpoint := newSGPOwnerEndpoint(t)
	listener := addOwnedListener(t, endpoint, 7, 1)
	healthy, healthySent := addActiveASP(t, listener, 7, 1)
	broken, brokenSent := addActiveASP(t, listener, 7, 1)
	if err := reportAvailability(
		endpoint, testWireScope(7, true, 1), 0x123456, 0, DestinationAvailable,
	); err != nil {
		t.Fatalf("seed available destination: %v", err)
	}
	writeFailure := errors.New("peer refused the isolation batch")
	failWrites(broken, brokenSent, writeFailure)
	healthySent.reset()

	restart, err := endpoint.BeginMTP3Restart(ownerDestination(7, 1, 0x123456, 0))
	if restart == nil {
		t.Fatalf("BeginMTP3Restart returned no handle: %v", err)
	}
	var delivery *SSNMDeliveryError
	if !errors.As(err, &delivery) {
		t.Fatalf("BeginMTP3Restart error = %v, want *SSNMDeliveryError", err)
	}
	if len(delivery.Successful) != 1 || delivery.Successful[0] != healthy.ID() {
		t.Fatalf("successful associations = %v, want [%d]", delivery.Successful, healthy.ID())
	}
	if len(delivery.Failed) != 1 || delivery.Failed[0].Association != broken.ID() {
		t.Fatalf("failed associations = %+v, want [%d]", delivery.Failed, broken.ID())
	}

	// RFC 4666 Section 4.6 isolates the affected scope until the restart
	// completes, so the audit answers DUNA for a destination the SG has
	// otherwise recorded as available.
	healthySent.reset()
	auditFrom(t, healthy, 7, 1, 0x123456)
	assertSSNMKinds(t, healthySent.snapshot(), []any{(*messages.DestinationUnavailable)(nil)})

	if err := restart.Update(
		ownerDestination(7, 1, 0x123456, 0),
		DestinationNetworkState{Availability: DestinationAvailable},
	); err != nil {
		t.Fatalf("Update through the handle returned by a partial fan-out: %v", err)
	}
	broken.signalWriter = brokenSent.write
	if err := restart.Complete(); err != nil {
		t.Fatalf("Complete after the isolation fan-out failed for one ASP: %v", err)
	}
	healthySent.reset()
	auditFrom(t, healthy, 7, 1, 0x123456)
	assertSSNMKinds(t, healthySent.snapshot(), []any{(*messages.DestinationAvailable)(nil)})
}

func TestEndpointRestartDoesNotRecoverUnstagedDestinations(t *testing.T) {
	endpoint := newSGPOwnerEndpoint(t)
	listener := addOwnedListener(t, endpoint, 7, 1)
	asp, sent := addActiveASP(t, listener, 7, 1)
	scope := testWireScope(7, true, 1)
	for _, pointCode := range []uint32{0x123456, 0x654321} {
		if err := reportAvailability(endpoint, scope, pointCode, 0, DestinationAvailable); err != nil {
			t.Fatalf("seed available destination %#x: %v", pointCode, err)
		}
	}

	restart, err := endpoint.BeginMTP3Restart(
		ownerDestination(7, 1, 0x123456, 0),
		ownerDestination(7, 1, 0x654321, 0),
	)
	if err != nil {
		t.Fatalf("BeginMTP3Restart: %v", err)
	}
	if err := restart.Update(
		ownerDestination(7, 1, 0x123456, 0),
		DestinationNetworkState{Availability: DestinationAvailable},
	); err != nil {
		t.Fatalf("Update: %v", err)
	}
	sent.reset()
	if err := restart.Complete(); err != nil {
		t.Fatalf("Complete: %v", err)
	}

	// Only the staged destination is announced. The one the caller never
	// updated stays unavailable, which is what the isolation already told every
	// ASP, so nothing about it goes on the wire.
	written := ssnmMessages(sent.snapshot())
	if len(written) != 1 {
		t.Fatalf("completion emitted %d SSNM messages, want 1", len(written))
	}
	if !sameSSNMKind(written[0], (*messages.DestinationAvailable)(nil)) {
		t.Fatalf("completion emitted %T, want *messages.DestinationAvailable", written[0])
	}
	if got := affectedPointCodeOf(written[0]); got != 0x123456 {
		t.Fatalf("completion announced %#x, want 0x123456", got)
	}
	assertOwnerState(t, endpoint, ownerStatusKey(7, 1, 0x654321, 0), DestinationNetworkState{
		Availability: DestinationUnavailable,
	})

	sent.reset()
	auditFrom(t, asp, 7, 1, 0x654321)
	assertSSNMKinds(t, sent.snapshot(), []any{(*messages.DestinationUnavailable)(nil)})
}

func TestEndpointRestartRetryPublishesOnlyWhatIsOutstanding(t *testing.T) {
	endpoint := newSGPOwnerEndpoint(t)
	listener := addOwnedListener(t, endpoint, 7, 1)
	asp, sent := addActiveASP(t, listener, 7, 1)

	restart, err := endpoint.BeginMTP3Restart(
		ownerDestination(7, 1, 0x123456, 0),
		ownerDestination(7, 1, 0x654321, 0),
	)
	if err != nil {
		t.Fatalf("BeginMTP3Restart: %v", err)
	}
	for _, pointCode := range []uint32{0x123456, 0x654321} {
		if err := restart.Update(
			ownerDestination(7, 1, pointCode, 0),
			DestinationNetworkState{Availability: DestinationAvailable},
		); err != nil {
			t.Fatalf("Update %#x: %v", pointCode, err)
		}
	}

	writeFailure := errors.New("peer refused the recovery for one destination")
	failWritesFor(asp, sent, 0x654321, writeFailure)
	sent.reset()
	err = restart.Complete()
	var delivery *SSNMDeliveryError
	if !errors.As(err, &delivery) {
		t.Fatalf("Complete error = %v, want *SSNMDeliveryError", err)
	}
	if len(delivery.Failed) != 1 || delivery.Failed[0].Association != asp.ID() {
		t.Fatalf("failed associations = %+v, want [%d]", delivery.Failed, asp.ID())
	}
	assertAffectedDestinations(t, "completed", restart.Completed(), []uint32{0x123456})
	assertAffectedDestinations(t, "outstanding", restart.Outstanding(), []uint32{0x654321})

	// The destination that completed is out of the restart's isolation; the one
	// that did not is still inside it.
	sent.reset()
	asp.signalWriter = sent.write
	auditFrom(t, asp, 7, 1, 0x123456)
	assertSSNMKinds(t, sent.snapshot(), []any{(*messages.DestinationAvailable)(nil)})
	sent.reset()
	auditFrom(t, asp, 7, 1, 0x654321)
	assertSSNMKinds(t, sent.snapshot(), []any{(*messages.DestinationUnavailable)(nil)})

	sent.reset()
	if err := restart.Complete(); err != nil {
		t.Fatalf("retried Complete: %v", err)
	}
	written := ssnmMessages(sent.snapshot())
	if len(written) != 1 {
		t.Fatalf("retry emitted %d SSNM messages, want 1", len(written))
	}
	if got := affectedPointCodeOf(written[0]); got != 0x654321 {
		t.Fatalf("retry announced %#x, want 0x654321 only", got)
	}
	if err := restart.Complete(); err != nil {
		t.Fatalf("Complete after success is not a no-op: %v", err)
	}
	if got := restart.Outstanding(); len(got) != 0 {
		t.Fatalf("outstanding after a completed restart = %+v, want none", got)
	}
}

func TestEndpointRestartStagesAvailabilityAndCongestionSeparately(t *testing.T) {
	endpoint := newSGPOwnerEndpoint(t)
	listener := addOwnedListener(t, endpoint, 7, 1)
	_, sent := addActiveASP(t, listener, 7, 1)
	scope := testWireScope(7, true, 1)

	restart, err := endpoint.BeginMTP3Restart(ownerDestination(7, 1, 0x123456, 0))
	if err != nil {
		t.Fatalf("BeginMTP3Restart: %v", err)
	}
	sent.reset()

	// A congestion report inside the restart stages congestion only. Publishing
	// it now would contradict the DUNA every concerned ASP was just given.
	if err := reportCongestion(endpoint, scope, 0x123456, 0, 2, true); err != nil {
		t.Fatalf("report congestion during the restart: %v", err)
	}
	if got := len(ssnmMessages(sent.snapshot())); got != 0 {
		t.Fatalf("congestion inside the restart emitted %d SSNM messages, want 0", got)
	}
	assertOwnerState(t, endpoint, ownerStatusKey(7, 1, 0x123456, 0), DestinationNetworkState{
		Availability: DestinationUnavailable,
	})

	// An availability report inside the restart stages availability only.
	if err := reportAvailability(endpoint, scope, 0x123456, 0, DestinationAvailable); err != nil {
		t.Fatalf("report availability during the restart: %v", err)
	}
	if got := len(ssnmMessages(sent.snapshot())); got != 0 {
		t.Fatalf("availability inside the restart emitted %d SSNM messages, want 0", got)
	}

	sent.reset()
	if err := restart.Complete(); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	assertOwnerState(t, endpoint, ownerStatusKey(7, 1, 0x123456, 0), DestinationNetworkState{
		Availability: DestinationAvailable,
		Congestion:   CongestionState{Congested: true, Level: 2, LevelSet: true},
	})
	// Congestion is orthogonal to reachability, so the SCON precedes the DAVA
	// that confirms the congested route is there.
	assertSSNMKinds(t, sent.snapshot(), []any{
		(*messages.SignallingCongestion)(nil),
		(*messages.DestinationAvailable)(nil),
	})
}

func assertAffectedDestinations(t *testing.T, what string, got []AffectedDestination, want []uint32) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s destinations = %+v, want %d entries", what, got, len(want))
	}
	for index, pointCode := range want {
		if got[index].PointCode != pointCode {
			t.Fatalf("%s destination %d = %#x, want %#x", what, index, got[index].PointCode, pointCode)
		}
	}
}

func auditFrom(t *testing.T, asp *Association, networkAppearance, routingContext, pointCode uint32) {
	t.Helper()
	if err := asp.handleDestinationStateAudit(messages.NewDestinationStateAudit(
		params.NewNetworkAppearance(networkAppearance),
		params.NewRoutingContext(routingContext),
		params.NewAffectedPointCode(pointCode),
		nil,
	)); err != nil {
		t.Fatalf("handle DAUD: %v", err)
	}
}

func assertSSNMKinds(t *testing.T, sent []messages.M3UA, want []any) {
	t.Helper()
	written := ssnmMessages(sent)
	if len(written) != len(want) {
		t.Fatalf("SSNM messages = %v, want %d", typeNames(written), len(want))
	}
	for index, kind := range want {
		if !sameSSNMKind(written[index], kind) {
			t.Fatalf("SSNM message %d = %T, want %T", index, written[index], kind)
		}
	}
}

// --- Ownership on close ---

func TestEndpointCloseEndsOwnerPublicationAndRestart(t *testing.T) {
	endpoint := newSGPOwnerEndpoint(t)
	listener := addOwnedListener(t, endpoint, 7, 1)
	addActiveASP(t, listener, 7, 1)

	restart, err := endpoint.BeginMTP3Restart(ownerDestination(7, 1, 0x123456, 0))
	if err != nil {
		t.Fatalf("BeginMTP3Restart: %v", err)
	}
	if err := endpoint.Close(); err != nil {
		t.Fatalf("Endpoint.Close: %v", err)
	}

	if err := reportAvailability(
		endpoint, testWireScope(7, true, 1), 0x123456, 0, DestinationAvailable,
	); !errors.Is(err, ErrEndpointClosed) {
		t.Fatalf("ReportDestinationAvailability after Close = %v, want %v", err, ErrEndpointClosed)
	}
	if _, err := endpoint.BeginMTP3Restart(
		ownerDestination(7, 1, 0x654321, 0),
	); !errors.Is(err, ErrEndpointClosed) {
		t.Fatalf("BeginMTP3Restart after Close = %v, want %v", err, ErrEndpointClosed)
	}
	// The restart registry belongs to the Endpoint, so a handle taken out
	// before the close does not outlive it.
	if err := restart.Update(
		ownerDestination(7, 1, 0x123456, 0),
		DestinationNetworkState{Availability: DestinationAvailable},
	); !errors.Is(err, ErrStaleMTP3Restart) {
		t.Fatalf("Update after Endpoint.Close = %v, want %v", err, ErrStaleMTP3Restart)
	}
	if err := restart.Complete(); !errors.Is(err, ErrStaleMTP3Restart) {
		t.Fatalf("Complete after Endpoint.Close = %v, want %v", err, ErrStaleMTP3Restart)
	}
}

func TestListenerAndChildCloseLeaveTheOwnerPublishing(t *testing.T) {
	endpoint := newSGPOwnerEndpoint(t)
	first := addOwnedListener(t, endpoint, 7, 1)
	second := addOwnedListener(t, endpoint, 7, 2)
	firstASP, firstSent := addActiveASP(t, first, 7, 1)
	secondASP, secondSent := addActiveASP(t, second, 7, 2)

	if err := first.Close(); err != nil {
		t.Fatalf("Listener.Close: %v", err)
	}
	select {
	case <-endpoint.Done():
		t.Fatal("closing a Listener closed its Endpoint")
	default:
	}
	if secondASP.Err() != nil {
		t.Fatalf("closing a Listener closed another Listener's Association: %v", secondASP.Err())
	}
	secondSent.reset()
	if err := reportAvailability(
		endpoint, testWireScope(7, true, 2), 0x123456, 0, DestinationUnavailable,
	); err != nil {
		t.Fatalf("ReportDestinationAvailability after a Listener closed: %v", err)
	}
	if got := len(ssnmMessages(secondSent.snapshot())); got != 1 {
		t.Fatalf("surviving ASP received %d SSNM messages, want 1", got)
	}

	// A child that closes stops being a publication target without taking the
	// owner's state with it.
	_ = firstASP.closeWith(ErrAssociationClosed)
	endpoint.as.forget(firstASP)
	firstSent.reset()
	secondSent.reset()
	if err := reportAvailability(
		endpoint, testWireScope(7, true, 2), 0x654321, 0, DestinationUnavailable,
	); err != nil {
		t.Fatalf("ReportDestinationAvailability after a child closed: %v", err)
	}
	if got := len(ssnmMessages(firstSent.snapshot())); got != 0 {
		t.Fatalf("closed Association received %d SSNM messages, want 0", got)
	}
	if got := len(ssnmMessages(secondSent.snapshot())); got != 1 {
		t.Fatalf("surviving ASP received %d SSNM messages, want 1", got)
	}
	if _, ok := endpoint.DestinationStatus(ownerStatusKey(7, 2, 0x123456, 0)); !ok {
		t.Fatal("closing a child discarded the owner's retained destination state")
	}
}

// --- Removed surface ---

func TestRemovedDestinationMethodsAreGone(t *testing.T) {
	removed := []string{
		"SetDestinationState",
		"SetDestinationRange",
		"SetDestinationStateForNetwork",
		"SetDestinationRangeForNetwork",
		"SetDestinationStateForNetworkAndRoutingContext",
		"SetDestinationRangeForNetworkAndRoutingContext",
		"ReportDestinationState",
		"ReportDestinationRange",
		"ReportDestinationStateForNetwork",
		"ReportDestinationRangeForNetwork",
		"ReportDestinationStateForNetworkAndRoutingContext",
		"ReportDestinationRangeForNetworkAndRoutingContext",
		"DestinationState",
		"DestinationStateForNetwork",
		"DestinationStateForNetworkAndRoutingContext",
		"DestinationStates",
		"DestinationStatesForNetwork",
		"DestinationRanges",
		"DestinationRangesForNetwork",
		"DestinationRangesForNetworkAndRoutingContext",
		"PeerCongestionLevel",
		"BeginMTP3Restart",
	}
	for _, receiver := range []struct {
		name string
		typ  reflect.Type
	}{
		{"*Association", reflect.TypeOf((*Association)(nil))},
		{"*Listener", reflect.TypeOf((*Listener)(nil))},
	} {
		for _, name := range removed {
			if _, ok := receiver.typ.MethodByName(name); ok {
				t.Errorf("%s.%s survives the removal", receiver.name, name)
			}
		}
	}

	// The replacements are the owner-level operations, and they report failure
	// rather than discarding it.
	endpointType := reflect.TypeOf((*Endpoint)(nil))
	for _, name := range []string{
		"ReportDestinationAvailability",
		"SignallingCongestion",
		"DestinationUserPartUnavailable",
		"BeginMTP3Restart",
		"DestinationStatus",
		"DestinationStatuses",
	} {
		method, ok := endpointType.MethodByName(name)
		if !ok {
			t.Fatalf("*Endpoint.%s is missing", name)
		}
		last := method.Type.Out(method.Type.NumOut() - 1)
		switch name {
		case "DestinationStatus", "DestinationStatuses":
			continue
		default:
			if last != reflect.TypeOf((*error)(nil)).Elem() {
				t.Errorf("*Endpoint.%s does not report an error, it returns %v", name, last)
			}
		}
	}
}

func TestEndpointPublicationNamesEveryRequestedRoutingContextInOneMessage(t *testing.T) {
	endpoint := newSGPOwnerEndpoint(t)
	listener := addOwnedListener(t, endpoint, 7, 1, 2)
	_, sent := addActiveASP(t, listener, 7, 1, 2)

	if err := reportAvailability(
		endpoint, testWireScope(7, true, 1, 2), 0x123456, 0, DestinationUnavailable,
	); err != nil {
		t.Fatalf("ReportDestinationAvailability for two Routing Contexts: %v", err)
	}
	written := ssnmMessages(sent.snapshot())
	if len(written) != 1 {
		t.Fatalf("two Routing Contexts emitted %d SSNM messages, want 1", len(written))
	}
	_, routingContext, _ := ssnmScope(t, written[0])
	if got := routingContext.RoutingContexts(); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("Routing Contexts = %v, want [1 2]", got)
	}
	// Each named Application Server holds its own record, so the audit answers
	// per Routing Context rather than from one shared entry.
	for _, routingContextValue := range []uint32{1, 2} {
		assertOwnerState(t, endpoint,
			ownerStatusKey(7, routingContextValue, 0x123456, 0),
			DestinationNetworkState{Availability: DestinationUnavailable},
		)
	}
}

func TestEndpointPublicationSplitsRoutingContextsAnIsolationSeparates(t *testing.T) {
	endpoint := newSGPOwnerEndpoint(t)
	listener := addOwnedListener(t, endpoint, 7, 1, 2)
	_, sent := addActiveASP(t, listener, 7, 1, 2)

	if _, err := endpoint.BeginMTP3Restart(ownerDestination(7, 1, 0x123456, 0)); err != nil {
		t.Fatalf("BeginMTP3Restart: %v", err)
	}
	sent.reset()

	if err := reportAvailability(
		endpoint, testWireScope(7, true, 1, 2), 0x123456, 0, DestinationAvailable,
	); err != nil {
		t.Fatalf("ReportDestinationAvailability across an isolated Routing Context: %v", err)
	}
	// Routing Context 1 is inside the restart, so its statement is staged. The
	// message that does go out names only the context that is not isolated.
	written := ssnmMessages(sent.snapshot())
	if len(written) != 1 {
		t.Fatalf("partially isolated publication emitted %d SSNM messages, want 1", len(written))
	}
	_, routingContext, _ := ssnmScope(t, written[0])
	if got := routingContext.RoutingContexts(); len(got) != 1 || got[0] != 2 {
		t.Fatalf("Routing Contexts = %v, want [2]", got)
	}
	assertOwnerState(t, endpoint, ownerStatusKey(7, 1, 0x123456, 0), DestinationNetworkState{
		Availability: DestinationUnavailable,
	})
	assertOwnerState(t, endpoint, ownerStatusKey(7, 2, 0x123456, 0), DestinationNetworkState{
		Availability: DestinationAvailable,
	})
}

func TestEndpointRestartOutstandingIsEmptyBeforeACompletionAttempt(t *testing.T) {
	endpoint := newSGPOwnerEndpoint(t)
	listener := addOwnedListener(t, endpoint, 7, 1)
	addActiveASP(t, listener, 7, 1)

	restart, err := endpoint.BeginMTP3Restart(ownerDestination(7, 1, 0x123456, 0))
	if err != nil {
		t.Fatalf("BeginMTP3Restart: %v", err)
	}
	if err := restart.Update(
		ownerDestination(7, 1, 0x123456, 0),
		DestinationNetworkState{Availability: DestinationAvailable},
	); err != nil {
		t.Fatalf("Update: %v", err)
	}
	// A staged destination is not an outstanding one: nothing has failed to be
	// published yet.
	if got := restart.Outstanding(); len(got) != 0 {
		t.Fatalf("Outstanding before a completion attempt = %+v, want none", got)
	}
	if got := restart.Completed(); len(got) != 0 {
		t.Fatalf("Completed before a completion attempt = %+v, want none", got)
	}
}

func TestEndpointRestartOutstandingSurvivesAFullyFailedCompletion(t *testing.T) {
	endpoint := newSGPOwnerEndpoint(t)
	listener := addOwnedListener(t, endpoint, 7, 1)
	asp, sent := addActiveASP(t, listener, 7, 1)

	restart, err := endpoint.BeginMTP3Restart(ownerDestination(7, 1, 0x123456, 0))
	if err != nil {
		t.Fatalf("BeginMTP3Restart: %v", err)
	}
	if err := restart.Update(
		ownerDestination(7, 1, 0x123456, 0),
		DestinationNetworkState{Availability: DestinationAvailable},
	); err != nil {
		t.Fatalf("Update: %v", err)
	}
	failWrites(asp, sent, errors.New("peer refused every recovery message"))

	if err := restart.Complete(); err == nil {
		t.Fatal("Complete reported success although no ASP accepted the recovery")
	}
	assertAffectedDestinations(t, "outstanding", restart.Outstanding(), []uint32{0x123456})
	if got := restart.Completed(); len(got) != 0 {
		t.Fatalf("Completed after a wholly failed completion = %+v, want none", got)
	}

	asp.signalWriter = sent.write
	if err := restart.Complete(); err != nil {
		t.Fatalf("retried Complete: %v", err)
	}
	assertOwnerState(t, endpoint, ownerStatusKey(7, 1, 0x123456, 0), DestinationNetworkState{
		Availability: DestinationAvailable,
	})
}

func TestEndpointRestartCompletionOfCongestionAloneAnnouncesNoAvailability(t *testing.T) {
	endpoint := newSGPOwnerEndpoint(t)
	listener := addOwnedListener(t, endpoint, 7, 1)
	_, sent := addActiveASP(t, listener, 7, 1)
	scope := testWireScope(7, true, 1)

	restart, err := endpoint.BeginMTP3Restart(ownerDestination(7, 1, 0x123456, 0))
	if err != nil {
		t.Fatalf("BeginMTP3Restart: %v", err)
	}
	if err := reportCongestion(endpoint, scope, 0x123456, 0, 2, true); err != nil {
		t.Fatalf("report congestion during the restart: %v", err)
	}
	sent.reset()

	if err := restart.Complete(); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	// RFC 4666 Section 4.6: "No message is necessary for those destinations
	// still unavailable after the restart procedure." The caller staged
	// congestion and no availability, so the completion says exactly that and
	// announces no recovery the caller never asked for.
	assertSSNMKinds(t, sent.snapshot(), []any{(*messages.SignallingCongestion)(nil)})
	assertOwnerState(t, endpoint, ownerStatusKey(7, 1, 0x123456, 0), DestinationNetworkState{
		Availability: DestinationUnavailable,
		Congestion:   CongestionState{Congested: true, Level: 2, LevelSet: true},
	})
}

func TestEndpointRestartUpdateWithoutCongestionKeepsTheRetainedLevel(t *testing.T) {
	endpoint := newSGPOwnerEndpoint(t)
	listener := addOwnedListener(t, endpoint, 7, 1)
	_, sent := addActiveASP(t, listener, 7, 1)
	scope := testWireScope(7, true, 1)
	if err := reportCongestion(endpoint, scope, 0x123456, 0, 2, true); err != nil {
		t.Fatalf("seed congestion: %v", err)
	}

	restart, err := endpoint.BeginMTP3Restart(ownerDestination(7, 1, 0x123456, 0))
	if err != nil {
		t.Fatalf("BeginMTP3Restart: %v", err)
	}
	// A zero CongestionState is the absence of a congestion statement, not a
	// report of no congestion, so the level the SG still holds survives.
	if err := restart.Update(
		ownerDestination(7, 1, 0x123456, 0),
		DestinationNetworkState{Availability: DestinationAvailable},
	); err != nil {
		t.Fatalf("Update: %v", err)
	}
	sent.reset()
	if err := restart.Complete(); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	assertSSNMKinds(t, sent.snapshot(), []any{(*messages.DestinationAvailable)(nil)})
	assertOwnerState(t, endpoint, ownerStatusKey(7, 1, 0x123456, 0), DestinationNetworkState{
		Availability: DestinationAvailable,
		Congestion:   CongestionState{Congested: true, Level: 2, LevelSet: true},
	})
}

func TestEndpointRestartUpdateAbatesCongestionWhenItSaysSo(t *testing.T) {
	endpoint := newSGPOwnerEndpoint(t)
	listener := addOwnedListener(t, endpoint, 7, 1)
	_, sent := addActiveASP(t, listener, 7, 1)
	scope := testWireScope(7, true, 1)
	if err := reportCongestion(endpoint, scope, 0x123456, 0, 2, true); err != nil {
		t.Fatalf("seed congestion: %v", err)
	}

	restart, err := endpoint.BeginMTP3Restart(ownerDestination(7, 1, 0x123456, 0))
	if err != nil {
		t.Fatalf("BeginMTP3Restart: %v", err)
	}
	// RFC 4666 Section 3.4.4 makes an explicit level 0 the abatement, so a
	// caller that states it does clear the retained level.
	if err := restart.Update(ownerDestination(7, 1, 0x123456, 0), DestinationNetworkState{
		Availability: DestinationAvailable,
		Congestion:   congestionStateFor(0, true),
	}); err != nil {
		t.Fatalf("Update: %v", err)
	}
	sent.reset()
	if err := restart.Complete(); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	assertSSNMKinds(t, sent.snapshot(), []any{
		(*messages.SignallingCongestion)(nil),
		(*messages.DestinationAvailable)(nil),
	})
	assertOwnerState(t, endpoint, ownerStatusKey(7, 1, 0x123456, 0), DestinationNetworkState{
		Availability: DestinationAvailable,
		Congestion:   CongestionState{Congested: false, Level: 0, LevelSet: true},
	})
}

func TestDestinationAuditOfACongestedUnavailableDestinationReportsOnlyDUNA(t *testing.T) {
	endpoint := newSGPOwnerEndpoint(t)
	listener := addOwnedListener(t, endpoint, 7, 1)
	asp, sent := addActiveASP(t, listener, 7, 1)
	scope := testWireScope(7, true, 1)
	if err := reportCongestion(endpoint, scope, 0x123456, 0, 2, true); err != nil {
		t.Fatalf("report congestion: %v", err)
	}
	if err := reportAvailability(endpoint, scope, 0x123456, 0, DestinationUnavailable); err != nil {
		t.Fatalf("report unavailable: %v", err)
	}

	sent.reset()
	auditFrom(t, asp, 7, 1, 0x123456)
	// RFC 4666 Section 4.5.3 asks for the SCON "before the DAVA or DRST". An
	// unavailable destination is answered with the DUNA alone: congestion says
	// nothing about a destination that cannot be reached at all.
	assertSSNMKinds(t, sent.snapshot(), []any{(*messages.DestinationUnavailable)(nil)})
}
