package m3ua

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestManagementIndicationCarriesExactAssociationAndASKeys(t *testing.T) {
	_, association := trackedManagementAssociation(t, StateASPActive, 10, 1, 2)

	if err := association.handleNotify(messages.NewNotify(
		params.NewStatus(params.AlternateAspActive),
		params.NewAspIdentifier(42),
		params.NewRoutingContext(2),
		nil,
	)); err != nil {
		t.Fatalf("handleNotify: %v", err)
	}
	indication := <-association.ManagementIndications()
	wantKey := ASKey{
		NetworkAppearance: 10, NetworkAppearanceSet: true,
		RoutingContext: 2, RoutingContextSet: true,
	}
	if indication.Association != association.ID() {
		t.Fatalf("Association = %d, want %d", indication.Association, association.ID())
	}
	if !reflect.DeepEqual(indication.ASKeys, []ASKey{wantKey}) {
		t.Fatalf("ASKeys = %+v, want [%+v]", indication.ASKeys, wantKey)
	}

	if err := association.handleNotify(messages.NewNotify(
		params.NewStatus(params.AsStatePending), nil, nil, nil,
	)); err != nil {
		t.Fatalf("contextless handleNotify: %v", err)
	}
	indication = <-association.ManagementIndications()
	wantKeys := []ASKey{
		{NetworkAppearance: 10, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true},
		{NetworkAppearance: 10, NetworkAppearanceSet: true, RoutingContext: 2, RoutingContextSet: true},
	}
	if !reflect.DeepEqual(indication.ASKeys, wantKeys) {
		t.Fatalf("contextless Notify ASKeys = %+v, want %+v", indication.ASKeys, wantKeys)
	}
}

func TestManagementErrorCarriesExactDestinations(t *testing.T) {
	_, association := trackedManagementAssociation(t, StateASPActive, 10, 1, 2)
	if err := association.handleError(messages.NewError(
		params.NewErrorCode(params.ErrDestinationStatusUnknown),
		params.NewRoutingContext(9, 10),
		params.NewNetworkAppearance(7),
		params.NewAffectedPointCode(
			uint32(0)<<24|0x123456,
			uint32(4)<<24|0x654321,
		),
		nil,
	)); err != nil {
		t.Fatalf("handleError: %v", err)
	}
	indication := <-association.ManagementIndications()
	wantKeys := []ASKey{
		{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContext: 9, RoutingContextSet: true},
		{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContext: 10, RoutingContextSet: true},
	}
	if !reflect.DeepEqual(indication.ASKeys, wantKeys) {
		t.Fatalf("Error ASKeys = %+v, want %+v", indication.ASKeys, wantKeys)
	}
	wantDestinations := []AffectedDestination{
		{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContext: 9, RoutingContextSet: true, PointCode: 0x123456},
		{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContext: 10, RoutingContextSet: true, PointCode: 0x123456},
		{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContext: 9, RoutingContextSet: true, PointCode: 0x654321, Mask: 4},
		{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContext: 10, RoutingContextSet: true, PointCode: 0x654321, Mask: 4},
	}
	if !reflect.DeepEqual(indication.AffectedDestinations, wantDestinations) {
		t.Fatalf("AffectedDestinations = %+v, want %+v",
			indication.AffectedDestinations, wantDestinations)
	}
}

func TestManagementIndicationOwnsAllSlices(t *testing.T) {
	_, association := trackedManagementAssociation(t, StateASPActive, 10, 1)
	indication := &ManagementIndication{
		Kind:               ManagementError,
		ASKeys:             []ASKey{{NetworkAppearance: 10, NetworkAppearanceSet: true, RoutingContext: 1, RoutingContextSet: true}},
		RoutingContexts:    []uint32{1},
		AffectedPointCodes: []uint32{0x123456},
		AffectedDestinations: []AffectedDestination{{
			NetworkAppearance: 10, NetworkAppearanceSet: true,
			RoutingContext: 1, RoutingContextSet: true,
			PointCode: 0x123456,
		}},
	}
	association.notifyManagement(indication)
	indication.ASKeys[0].RoutingContext = 99
	indication.RoutingContexts[0] = 99
	indication.AffectedPointCodes[0] = 99
	indication.AffectedDestinations[0].PointCode = 99

	got := <-association.ManagementIndications()
	if got == indication {
		t.Fatal("notifyManagement exposed the producer's indication pointer")
	}
	if got.Association != association.ID() ||
		got.ASKeys[0].RoutingContext != 1 ||
		got.RoutingContexts[0] != 1 ||
		got.AffectedPointCodes[0] != 0x123456 ||
		got.AffectedDestinations[0].PointCode != 0x123456 {
		t.Fatalf("snapshotted indication changed with producer mutation: %+v", got)
	}
}

func TestManagementReleaseCarriesCauseAndScope(t *testing.T) {
	_, association := trackedManagementAssociation(t, StateASPActive, 10, 1, 2)
	if err := association.closeWith(ErrHeartbeatExpired); err != nil {
		t.Fatalf("closeWith: %v", err)
	}
	indication := <-association.ManagementIndications()
	if indication.Kind != ManagementSCTPRelease {
		t.Fatalf("Kind = %v, want ManagementSCTPRelease", indication.Kind)
	}
	if indication.Association != association.ID() || !errors.Is(indication.Cause, ErrHeartbeatExpired) {
		t.Fatalf("release indication = %+v", indication)
	}
	if len(indication.ASKeys) != 2 {
		t.Fatalf("release ASKeys = %+v, want two configured ASes", indication.ASKeys)
	}
}

func TestManagementRestartCarriesExactAssociationAndScope(t *testing.T) {
	_, association := trackedManagementAssociation(t, StateASPActive, 10, 1, 2)

	association.handleSCTPRestart()
	indication := <-association.ManagementIndications()
	if indication.Kind != ManagementSCTPRestart {
		t.Fatalf("Kind = %v, want ManagementSCTPRestart", indication.Kind)
	}
	if indication.Association != association.ID() || indication.Cause != nil {
		t.Fatalf("restart indication = %+v", indication)
	}
	if len(indication.ASKeys) != 2 {
		t.Fatalf("restart ASKeys = %+v, want two configured ASes", indication.ASKeys)
	}
}

func TestInvalidVersionManagementErrorCarriesExactAssociationAndScope(t *testing.T) {
	_, association := trackedManagementAssociation(t, StateASPActive, 10, 1, 2)
	raw := []byte{0x02, 0x00, messages.MsgClassASPSM, messages.MsgTypeAspUp, 0, 0, 0, 8}

	association.dispatchRaw(context.Background(), inbound{data: raw, ppid: M3UAPPID})
	indication := <-association.ManagementIndications()
	if indication.Kind != ManagementError || indication.ErrorCode != params.InvalidVersionError {
		t.Fatalf("indication = %#v, want M-ERROR Invalid Version", indication)
	}
	if indication.Association != association.ID() || indication.Cause != nil {
		t.Fatalf("invalid-version indication = %+v", indication)
	}
	if len(indication.ASKeys) != 2 {
		t.Fatalf("invalid-version ASKeys = %+v, want two configured ASes", indication.ASKeys)
	}
}

func TestPeerManagementErrorDoesNotReportLocalCause(t *testing.T) {
	_, association := trackedManagementAssociation(t, StateASPActive, 10, 1)

	if err := association.handleError(messages.NewError(
		params.NewErrorCode(params.UnexpectedMessageError), nil, nil, nil, nil,
	)); err != nil {
		t.Fatalf("handleError: %v", err)
	}
	indication := <-association.ManagementIndications()
	if indication.Kind != ManagementError || indication.ErrorCode != params.UnexpectedMessageError {
		t.Fatalf("indication = %#v, want peer M-ERROR Unexpected Message", indication)
	}
	if indication.Association != association.ID() || indication.Cause != nil {
		t.Fatalf("peer Error indication = %+v", indication)
	}
}

func TestExplicitASPProcedureFailuresPublishLocalManagementError(t *testing.T) {
	t.Run("canceled ASP Up", func(t *testing.T) {
		_, association := trackedManagementAssociation(t, StateASPDown, 10, 1)
		writes := 0
		association.signalWriter = func(message messages.M3UA) (int, error) {
			writes++
			return message.MarshalLen(), nil
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := association.ASPUp(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("ASPUp error = %v, want context.Canceled", err)
		}
		if writes != 0 {
			t.Fatalf("canceled ASPUp wrote %d messages, want 0", writes)
		}
		assertLocalManagementError(t, association, context.Canceled, []ASKey{{
			NetworkAppearance: 10, NetworkAppearanceSet: true,
			RoutingContext: 1, RoutingContextSet: true,
		}})
	})

	t.Run("wrong Network Appearance ASP Active", func(t *testing.T) {
		_, association := trackedManagementAssociation(t, StateASPInactive, 10, 1)
		writes := 0
		association.signalWriter = func(message messages.M3UA) (int, error) {
			writes++
			return message.MarshalLen(), nil
		}
		requested := ASKey{
			NetworkAppearance: 20, NetworkAppearanceSet: true,
			RoutingContext: 1, RoutingContextSet: true,
		}
		if err := association.ASPActive(context.Background(), requested); !errors.Is(err, ErrInvalidNetworkAppearance) {
			t.Fatalf("ASPActive error = %v, want ErrInvalidNetworkAppearance", err)
		}
		if writes != 0 {
			t.Fatalf("invalid ASPActive wrote %d messages, want 0", writes)
		}
		assertLocalManagementError(t, association, ErrInvalidNetworkAppearance, []ASKey{requested})
	})

	t.Run("T ack expiry", func(t *testing.T) {
		_, association := trackedManagementAssociation(t, StateASPDown, 10, 1)
		association.cfg.TAck = time.Millisecond
		association.cfg.TAckRetries = 1
		association.signalWriter = func(message messages.M3UA) (int, error) {
			return message.MarshalLen(), nil
		}
		if err := association.ASPUp(context.Background()); !errors.Is(err, ErrTAckExpired) {
			t.Fatalf("ASPUp error = %v, want ErrTAckExpired", err)
		}
		assertLocalManagementError(t, association, ErrTAckExpired, []ASKey{{
			NetworkAppearance: 10, NetworkAppearanceSet: true,
			RoutingContext: 1, RoutingContextSet: true,
		}})
	})
}

func trackedManagementAssociation(
	t *testing.T,
	state State,
	networkAppearance uint32,
	routingContexts ...uint32,
) (*Endpoint, *Association) {
	t.Helper()
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	association, _ := newTestConnWithContexts(t, state, RoleASP, routingContexts...)
	association.cfg.NetworkAppearance = params.NewNetworkAppearance(networkAppearance)
	if state == StateASPActive {
		association.noteRoutingContextsActive(routingContexts)
	}
	if !endpoint.trackAssociation(association) {
		t.Fatal("trackAssociation")
	}
	return endpoint, association
}

func assertLocalManagementError(
	t *testing.T,
	association *Association,
	wantCause error,
	wantKeys []ASKey,
) {
	t.Helper()
	select {
	case indication := <-association.ManagementIndications():
		if indication.Kind != ManagementError ||
			indication.Association != association.ID() ||
			!errors.Is(indication.Cause, wantCause) ||
			!reflect.DeepEqual(indication.ASKeys, wantKeys) {
			t.Fatalf("local M-ERROR = %+v, want cause %v and ASKeys %+v",
				indication, wantCause, wantKeys)
		}
	case <-time.After(time.Second):
		t.Fatal("local operation failure produced no M-ERROR indication")
	}
}
