package m3ua

import (
	"errors"
	"reflect"
	"testing"

	"github.com/gomaja/go-m3ua/messages/params"
)

func TestEndpointAssociationStatusUsesStableOwnedIDs(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })

	first, _ := newTestConn(t, StateASPInactive, RoleASP)
	second, _ := newTestConn(t, StateASPActive, RoleASP)
	if !endpoint.trackAssociation(first) || !endpoint.trackAssociation(second) {
		t.Fatal("trackAssociation returned false")
	}

	if first.ID() == 0 || second.ID() <= first.ID() {
		t.Fatalf("Association IDs = %d, %d, want increasing non-zero values", first.ID(), second.ID())
	}
	if !endpoint.trackAssociation(first) {
		t.Fatal("tracking an already-owned Association returned false")
	}
	if got := first.ID(); got == 0 || got >= second.ID() {
		t.Fatalf("retracked Association ID = %d, want original ID below %d", got, second.ID())
	}

	statuses := endpoint.AssociationStatuses()
	if len(statuses) != 2 {
		t.Fatalf("AssociationStatuses length = %d, want 2", len(statuses))
	}
	if statuses[0].Association != first.ID() || statuses[1].Association != second.ID() {
		t.Fatalf("AssociationStatuses IDs = %d, %d, want %d, %d",
			statuses[0].Association, statuses[1].Association, first.ID(), second.ID())
	}
	if statuses[0].Role != RoleASP || statuses[0].State != StateASPInactive {
		t.Fatalf("first status = %+v", statuses[0])
	}
	if !errors.Is(statuses[0].SCTPError, ErrAssociationClosed) {
		t.Fatalf("first SCTP error = %v, want ErrAssociationClosed", statuses[0].SCTPError)
	}

	byID, ok := endpoint.AssociationStatus(second.ID())
	if !ok {
		t.Fatal("AssociationStatus did not find the second Association")
	}
	if byID.Association != second.ID() || byID.State != StateASPActive {
		t.Fatalf("AssociationStatus = %+v", byID)
	}
	if _, ok := endpoint.AssociationStatus(AssociationID(999)); ok {
		t.Fatal("AssociationStatus found an unknown AssociationID")
	}
}

func TestAssociationIDIsNotReusedAfterClose(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })

	first, _ := newTestConn(t, StateASPActive, RoleASP)
	if !endpoint.trackAssociation(first) {
		t.Fatal("track first Association")
	}
	firstID := first.ID()
	if err := first.Close(); err != nil {
		t.Fatalf("close first Association: %v", err)
	}
	if _, ok := endpoint.AssociationStatus(firstID); ok {
		t.Fatal("closed Association remains in Endpoint status")
	}

	second, _ := newTestConn(t, StateASPActive, RoleASP)
	if !endpoint.trackAssociation(second) {
		t.Fatal("track second Association")
	}
	if second.ID() <= firstID {
		t.Fatalf("second Association ID = %d, want greater than retired ID %d", second.ID(), firstID)
	}
}

func TestEndpointApplicationServerStatusesSeparateNetworkAppearances(t *testing.T) {
	firstKey := ASKey{
		NetworkAppearance:    10,
		NetworkAppearanceSet: true,
		RoutingContext:       1,
		RoutingContextSet:    true,
	}
	secondKey := ASKey{
		NetworkAppearance:    20,
		NetworkAppearanceSet: true,
		RoutingContext:       1,
		RoutingContextSet:    true,
	}
	endpoint, err := NewEndpoint(EndpointConfig{
		Role: RoleSGP,
		ApplicationServers: &ApplicationServerConfig{
			ActivationPolicies: map[ASKey]ASActivationPolicy{
				firstKey:  {RequiredActiveASPs: 1},
				secondKey: {RequiredActiveASPs: 2},
			},
		},
	})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	endpoint.as.register([]ASKey{secondKey, firstKey})

	association, _ := newTestConn(t, StateASPActive, RoleSGP)
	association.cfg.NetworkAppearance = params.NewNetworkAppearance(firstKey.NetworkAppearance)
	association.cfg.RoutingContexts = params.NewRoutingContext(firstKey.RoutingContext)
	association.noteRoutingContextsActive([]uint32{firstKey.RoutingContext})
	association.as = endpoint.as
	if !endpoint.trackAssociation(association) {
		t.Fatal("track Association")
	}
	endpoint.as.get(firstKey).setASPState(association, StateASPActive, 0)

	statuses := endpoint.ApplicationServerStatuses()
	if len(statuses) != 2 {
		t.Fatalf("ApplicationServerStatuses length = %d, want 2", len(statuses))
	}
	if statuses[0].AS != firstKey || statuses[1].AS != secondKey {
		t.Fatalf("ApplicationServerStatuses keys = %+v, want %v then %v", statuses, firstKey, secondKey)
	}
	if statuses[0].State != ASActive || len(statuses[0].ActiveASPs) != 1 ||
		statuses[0].ActiveASPs[0] != association.ID() {
		t.Fatalf("first Application Server status = %+v", statuses[0])
	}
	if statuses[1].State != ASDown || statuses[1].RequiredActiveASPs != 2 {
		t.Fatalf("second Application Server status = %+v", statuses[1])
	}

	first, ok := endpoint.ApplicationServerStatus(firstKey)
	if !ok || first.AS != firstKey {
		t.Fatalf("ApplicationServerStatus = %+v, %v", first, ok)
	}
	if _, ok := endpoint.ApplicationServerStatus(ASKey{RoutingContext: 99, RoutingContextSet: true}); ok {
		t.Fatal("ApplicationServerStatus found an unknown ASKey")
	}
}

func TestEndpointASPStatusesPreserveDirectionAndExactASKey(t *testing.T) {
	key := ASKey{
		NetworkAppearance:    10,
		NetworkAppearanceSet: true,
		RoutingContext:       7,
		RoutingContextSet:    true,
	}

	t.Run("ASP reports local state and identifier", func(t *testing.T) {
		endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP})
		if err != nil {
			t.Fatalf("NewEndpoint: %v", err)
		}
		t.Cleanup(func() { _ = endpoint.Close() })
		association, _ := newTestConn(t, StateASPActive, RoleASP)
		association.cfg.NetworkAppearance = params.NewNetworkAppearance(key.NetworkAppearance)
		association.cfg.RoutingContexts = params.NewRoutingContext(key.RoutingContext)
		association.cfg.ASPIdentifier = params.NewAspIdentifier(42)
		association.noteRoutingContextsAcked(params.NewRoutingContext(key.RoutingContext))
		if !endpoint.trackAssociation(association) {
			t.Fatal("track Association")
		}

		status, ok := endpoint.ASPStatus(ASPStatusKey{Association: association.ID(), AS: key})
		if !ok {
			t.Fatal("ASPStatus did not find local ASP")
		}
		if !status.LocalStateSet || status.LocalState != StateASPActive || status.PeerStateSet {
			t.Fatalf("local ASP status = %+v", status)
		}
		if !status.LocalASPIdentifierSet || status.LocalASPIdentifier != 42 {
			t.Fatalf("local ASP Identifier = (%d, %v), want (42, true)",
				status.LocalASPIdentifier, status.LocalASPIdentifierSet)
		}
	})

	t.Run("SGP reports peer state and identifier", func(t *testing.T) {
		endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
		if err != nil {
			t.Fatalf("NewEndpoint: %v", err)
		}
		t.Cleanup(func() { _ = endpoint.Close() })
		endpoint.as.register([]ASKey{key})
		association, _ := newTestConn(t, StateASPInactive, RoleSGP)
		association.cfg.NetworkAppearance = params.NewNetworkAppearance(key.NetworkAppearance)
		association.cfg.RoutingContexts = params.NewRoutingContext(key.RoutingContext)
		association.as = endpoint.as
		association.savePeerASPIdentifier(params.NewAspIdentifier(99))
		if !endpoint.trackAssociation(association) {
			t.Fatal("track Association")
		}
		endpoint.as.get(key).setASPState(association, StateASPInactive, 0)

		status, ok := endpoint.ASPStatus(ASPStatusKey{Association: association.ID(), AS: key})
		if !ok {
			t.Fatal("ASPStatus did not find peer ASP")
		}
		if status.LocalStateSet || !status.PeerStateSet || status.PeerState != StateASPInactive {
			t.Fatalf("peer ASP status = %+v", status)
		}
		if !status.PeerASPIdentifierSet || status.PeerASPIdentifier != 99 {
			t.Fatalf("peer ASP Identifier = (%d, %v), want (99, true)",
				status.PeerASPIdentifier, status.PeerASPIdentifierSet)
		}
	})
}

func TestEndpointASPStatusKeysAreDeterministic(t *testing.T) {
	association, _ := newTestConn(t, StateASPActive, RoleASP)
	want := []ASKey{
		{RoutingContext: 1, RoutingContextSet: true},
		{RoutingContext: 2, RoutingContextSet: true},
		{NetworkAppearance: 10, NetworkAppearanceSet: true, RoutingContext: 3, RoutingContextSet: true},
		{NetworkAppearance: 10, NetworkAppearanceSet: true, RoutingContext: 5, RoutingContextSet: true},
		{NetworkAppearance: 20, NetworkAppearanceSet: true, RoutingContext: 4, RoutingContextSet: true},
	}
	association.dynamicPeerASKeys = map[uint32]ASKey{
		3: want[2],
		4: want[4],
		5: want[3],
	}

	for iteration := 0; iteration < 100; iteration++ {
		if got := endpointASPStatusKeys(association); !reflect.DeepEqual(got, want) {
			t.Fatalf("endpointASPStatusKeys iteration %d = %v, want %v", iteration, got, want)
		}
	}
}

func TestEndpointMTPRouteStatusIsKeyedAndCallerOwned(t *testing.T) {
	endpoint, first, second := newASPMultiSGFixture(t)

	status, ok := endpoint.MTPRouteStatus("sccp-a")
	if !ok {
		t.Fatal("MTPRouteStatus did not find sccp-a")
	}
	if status.MTPRoute != "sccp-a" || len(status.Destinations) == 0 {
		t.Fatalf("MTPRouteStatus = %+v", status)
	}
	if len(status.Associations) != 2 ||
		status.Associations[0] != first.ID() || status.Associations[1] != second.ID() {
		t.Fatalf("MTPRouteStatus Associations = %v, want [%d %d]",
			status.Associations, first.ID(), second.ID())
	}

	status.Destinations[0].Availability = DestinationUnavailable
	status.Associations[0] = 0
	again, ok := endpoint.MTPRouteStatus("sccp-a")
	if !ok {
		t.Fatal("second MTPRouteStatus did not find sccp-a")
	}
	if again.Destinations[0].Availability == DestinationUnavailable || again.Associations[0] == 0 {
		t.Fatal("caller mutation changed the Endpoint MTP Route snapshot")
	}
	if _, ok := endpoint.MTPRouteStatus("unknown"); ok {
		t.Fatal("MTPRouteStatus found an unknown route")
	}

	statuses := endpoint.MTPRouteStatuses()
	if len(statuses) != 1 || statuses[0].MTPRoute != "sccp-a" {
		t.Fatalf("MTPRouteStatuses = %+v", statuses)
	}
}

func TestEndpointDestinationStatusPreservesExactScopeAndCongestion(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })

	first := DestinationRange{
		NetworkAppearance:    10,
		NetworkAppearanceSet: true,
		RoutingContext:       1,
		RoutingContextSet:    true,
		PointCode:            0x123400,
		Mask:                 8,
		State:                DestinationCongested,
		CongestionLevel:      2,
		CongestionLevelSet:   true,
	}
	second := DestinationRange{
		NetworkAppearance:    20,
		NetworkAppearanceSet: true,
		RoutingContext:       1,
		RoutingContextSet:    true,
		PointCode:            0x123456,
		State:                DestinationUnavailable,
	}
	endpoint.destinations.setRanges([]DestinationRange{second, first})

	key := DestinationStatusKey{
		NetworkAppearance:    10,
		NetworkAppearanceSet: true,
		RoutingContext:       1,
		RoutingContextSet:    true,
		PointCode:            0x123456,
	}
	status, ok := endpoint.DestinationStatus(key)
	if !ok {
		t.Fatal("DestinationStatus did not find a destination covered by the stored range")
	}
	if status.Key.PointCode != first.PointCode || status.Key.Mask != first.Mask ||
		status.State != DestinationCongested || !status.CongestionLevelSet ||
		status.CongestionLevel != 2 {
		t.Fatalf("DestinationStatus = %+v", status)
	}
	if _, ok := endpoint.DestinationStatus(DestinationStatusKey{
		NetworkAppearance:    30,
		NetworkAppearanceSet: true,
		RoutingContext:       1,
		RoutingContextSet:    true,
		PointCode:            0x123456,
	}); ok {
		t.Fatal("DestinationStatus crossed a Network Appearance boundary")
	}

	statuses := endpoint.DestinationStatuses()
	if len(statuses) != 2 {
		t.Fatalf("DestinationStatuses length = %d, want 2: %+v", len(statuses), statuses)
	}
	if statuses[0].Key.NetworkAppearance != 10 || statuses[1].Key.NetworkAppearance != 20 {
		t.Fatalf("DestinationStatuses order/scope = %+v", statuses)
	}
}

func TestEndpointDestinationStatusesKeepNewestPerExpandedScope(t *testing.T) {
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })

	rangeValue := DestinationRange{
		NetworkAppearance:    10,
		NetworkAppearanceSet: true,
		PointCode:            0x123456,
		Mask:                 4,
		State:                DestinationUnavailable,
	}
	endpoint.destinations.setScopedRanges([]uint32{1, 2}, []DestinationRange{rangeValue})
	rangeValue.State = DestinationAvailable
	endpoint.destinations.setScopedRanges([]uint32{1}, []DestinationRange{rangeValue})

	want := []DestinationStatusSnapshot{
		{
			Key: DestinationStatusKey{
				NetworkAppearance: 10, NetworkAppearanceSet: true,
				RoutingContext: 1, RoutingContextSet: true,
				PointCode: 0x123456, Mask: 4,
			},
			State: DestinationAvailable,
		},
		{
			Key: DestinationStatusKey{
				NetworkAppearance: 10, NetworkAppearanceSet: true,
				RoutingContext: 2, RoutingContextSet: true,
				PointCode: 0x123456, Mask: 4,
			},
			State: DestinationUnavailable,
		},
	}
	if got := endpoint.DestinationStatuses(); !reflect.DeepEqual(got, want) {
		t.Fatalf("DestinationStatuses = %+v, want %+v", got, want)
	}
}

func TestEndpointRoleInvalidStatusQueriesReturnNoResult(t *testing.T) {
	asp, err := NewEndpoint(EndpointConfig{Role: RoleASP})
	if err != nil {
		t.Fatalf("NewEndpoint(RoleASP): %v", err)
	}
	t.Cleanup(func() { _ = asp.Close() })
	if _, ok := asp.DestinationStatus(DestinationStatusKey{}); ok {
		t.Fatal("ASP Endpoint returned SGP destination status")
	}
	if statuses := asp.DestinationStatuses(); statuses != nil {
		t.Fatalf("ASP DestinationStatuses = %+v, want nil", statuses)
	}

	sgp, err := NewEndpoint(EndpointConfig{Role: RoleSGP})
	if err != nil {
		t.Fatalf("NewEndpoint(RoleSGP): %v", err)
	}
	t.Cleanup(func() { _ = sgp.Close() })
	if _, ok := sgp.MTPRouteStatus("route"); ok {
		t.Fatal("SGP Endpoint returned ASP MTP Route status")
	}
	if statuses := sgp.MTPRouteStatuses(); statuses != nil {
		t.Fatalf("SGP MTPRouteStatuses = %+v, want nil", statuses)
	}
}
