package m3ua

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestASPProcedurePolicyValidationAndSnapshot(t *testing.T) {
	automatic := &ASPProcedurePolicy{
		ASPUp:       ASPProcedureAutomatic,
		ASPDown:     ASPProcedureAutomatic,
		ASPActive:   ASPProcedureAutomatic,
		ASPInactive: ASPProcedureAutomatic,
	}
	config := NewAssociationConfig(0, 0, 0, 0, 0, 0)
	config.ASPProcedures = automatic
	if err := validateAssociationConfigForRole(RoleASP, config); err != nil {
		t.Fatalf("valid ASP procedure policy: %v", err)
	}
	snapshot := snapshotAssociationConfig(config)
	if snapshot.ASPProcedures == automatic {
		t.Fatal("ASPProcedurePolicy was not copied")
	}
	automatic.ASPUp = ASPProcedureExplicit
	if snapshot.ASPProcedures.ASPUp != ASPProcedureAutomatic {
		t.Fatal("caller mutation changed snapshotted ASPProcedurePolicy")
	}

	incomplete := NewAssociationConfig(0, 0, 0, 0, 0, 0)
	incomplete.ASPProcedures = &ASPProcedurePolicy{ASPUp: ASPProcedureAutomatic}
	if err := validateAssociationConfigForRole(RoleASP, incomplete); !errors.Is(err, ErrInvalidRoleConfiguration) {
		t.Fatalf("incomplete policy error = %v, want ErrInvalidRoleConfiguration", err)
	}

	sgp := NewAssociationConfig(0, 0, 0, 0, 0, 0)
	sgp.ASPIdentifier = nil
	sgp.ASPProcedures = automatic
	if err := validateAssociationConfigForRole(RoleSGP, sgp); !errors.Is(err, ErrInvalidRoleConfiguration) {
		t.Fatalf("SGP policy error = %v, want ErrInvalidRoleConfiguration", err)
	}
}

func TestASPProcedurePolicyHistoricalDefaults(t *testing.T) {
	asp := newAssociation(RoleASP, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	if asp.aspProcedureMode(aspProcedureUp) != ASPProcedureAutomatic ||
		asp.aspProcedureMode(aspProcedureActive) != ASPProcedureAutomatic ||
		asp.aspProcedureMode(aspProcedureInactive) != ASPProcedureAutomatic ||
		asp.aspProcedureMode(aspProcedureDown) != ASPProcedureAutomatic {
		t.Fatal("nil ASP procedure policy did not preserve automatic ASP lifecycle")
	}

	ipspConfig := NewAssociationConfig(0, 0, 0, 0, 0, 0)
	ipspConfig.IPSP = &IPSPConfig{
		ExchangeModel: IPSPExchangeSingle,
		InitiateASPSM: true,
	}
	ipsp := newAssociation(RoleIPSP, ipspConfig)
	if ipsp.aspProcedureMode(aspProcedureUp) != ASPProcedureAutomatic {
		t.Fatal("legacy InitiateASPSM=true did not select automatic ASP Up")
	}
	if ipsp.aspProcedureMode(aspProcedureActive) != ASPProcedureExplicit {
		t.Fatal("legacy InitiateASPTM=false did not select explicit ASP Active")
	}
	if ipsp.aspProcedureMode(aspProcedureInactive) != ASPProcedureAutomatic ||
		ipsp.aspProcedureMode(aspProcedureDown) != ASPProcedureAutomatic {
		t.Fatal("legacy IPSP shutdown did not remain automatic")
	}
}

func TestAssociationASPUpWaitsForAcknowledgement(t *testing.T) {
	association, _ := newTestConn(t, StateASPDown, RoleASP)
	writes := make(chan messages.M3UA, 1)
	association.signalWriter = func(message messages.M3UA) (int, error) {
		writes <- message
		return message.MarshalLen(), nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- association.ASPUp(ctx) }()

	select {
	case message := <-writes:
		if _, ok := message.(*messages.AspUp); !ok {
			t.Fatalf("ASPUp wrote %T, want *messages.AspUp", message)
		}
	case <-ctx.Done():
		t.Fatal("ASPUp did not write ASP Up")
	}
	select {
	case err := <-result:
		t.Fatalf("ASPUp returned before ASP Up Ack: %v", err)
	default:
	}
	if err := association.handleAspUpAck(messages.NewAspUpAck(nil, nil)); err != nil {
		t.Fatalf("handleAspUpAck: %v", err)
	}
	if err := <-result; err != nil {
		t.Fatalf("ASPUp: %v", err)
	}
}

func TestAssociationASPDownWaitsForAcknowledgement(t *testing.T) {
	association, _ := newTestConn(t, StateASPActive, RoleASP)
	writes := make(chan messages.M3UA, 1)
	association.signalWriter = func(message messages.M3UA) (int, error) {
		writes <- message
		return message.MarshalLen(), nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- association.ASPDown(ctx) }()

	select {
	case message := <-writes:
		if _, ok := message.(*messages.AspDown); !ok {
			t.Fatalf("ASPDown wrote %T, want *messages.AspDown", message)
		}
	case <-ctx.Done():
		t.Fatal("ASPDown did not write ASP Down")
	}
	select {
	case err := <-result:
		t.Fatalf("ASPDown returned before ASP Down Ack: %v", err)
	default:
	}
	if err := association.handleAspDownAck(messages.NewAspDownAck(nil)); err != nil {
		t.Fatalf("handleAspDownAck: %v", err)
	}
	if err := <-result; err != nil {
		t.Fatalf("ASPDown: %v", err)
	}
}

func TestAssociationASPSMExplicitOperationsRejectBeforeWrite(t *testing.T) {
	tests := []struct {
		name        string
		association func(*testing.T) *Association
		call        func(*Association, context.Context) error
		want        error
	}{
		{
			name: "SGP ASP Up",
			association: func(t *testing.T) *Association {
				association, _ := newTestConn(t, StateASPDown, RoleSGP)
				return association
			},
			call: func(association *Association, ctx context.Context) error {
				return association.ASPUp(ctx)
			},
			want: ErrUnsupportedRole,
		},
		{
			name: "ASP Up from ASP-INACTIVE",
			association: func(t *testing.T) *Association {
				association, _ := newTestConn(t, StateASPInactive, RoleASP)
				return association
			},
			call: func(association *Association, ctx context.Context) error {
				return association.ASPUp(ctx)
			},
			want: ErrInvalidState,
		},
		{
			name: "ASP Down from ASP-DOWN",
			association: func(t *testing.T) *Association {
				association, _ := newTestConn(t, StateASPDown, RoleASP)
				return association
			},
			call: func(association *Association, ctx context.Context) error {
				return association.ASPDown(ctx)
			},
			want: ErrInvalidState,
		},
		{
			name: "already canceled ASP Up",
			association: func(t *testing.T) *Association {
				association, _ := newTestConn(t, StateASPDown, RoleASP)
				return association
			},
			call: func(association *Association, _ context.Context) error {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return association.ASPUp(ctx)
			},
			want: context.Canceled,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			association := test.association(t)
			writes := 0
			association.signalWriter = func(message messages.M3UA) (int, error) {
				writes++
				return message.MarshalLen(), nil
			}
			err := test.call(association, context.Background())
			if !errors.Is(err, test.want) {
				t.Fatalf("operation error = %v, want %v", err, test.want)
			}
			if writes != 0 {
				t.Fatalf("rejected operation wrote %d messages, want 0", writes)
			}
		})
	}
}

func TestAssociationASPUpCancellationStopsRetransmission(t *testing.T) {
	association, _ := newTestConn(t, StateASPDown, RoleASP)
	association.cfg.TAck = 10 * time.Millisecond
	association.cfg.TAckRetries = 3
	writes := make(chan messages.M3UA, 8)
	association.signalWriter = func(message messages.M3UA) (int, error) {
		writes <- message
		return message.MarshalLen(), nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- association.ASPUp(ctx) }()
	<-writes
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("ASPUp cancellation error = %v, want context.Canceled", err)
	}
	time.Sleep(4 * association.cfg.TAck)
	select {
	case message := <-writes:
		t.Fatalf("ASPUp retransmitted %T after cancellation", message)
	default:
	}
}

func TestAssociationASPUpReportsTAckExpiry(t *testing.T) {
	association, _ := newTestConn(t, StateASPDown, RoleASP)
	association.cfg.TAck = 5 * time.Millisecond
	association.cfg.TAckRetries = 1
	association.signalWriter = func(message messages.M3UA) (int, error) {
		return message.MarshalLen(), nil
	}
	if err := association.ASPUp(context.Background()); !errors.Is(err, ErrTAckExpired) {
		t.Fatalf("ASPUp error = %v, want ErrTAckExpired", err)
	}
}

func TestAssociationExplicitASPProceduresSerializeWithoutSuperseding(t *testing.T) {
	association, _ := newTestConn(t, StateASPDown, RoleASP)
	writes := make(chan messages.M3UA, 2)
	association.signalWriter = func(message messages.M3UA) (int, error) {
		writes <- message
		return message.MarshalLen(), nil
	}

	firstResult := make(chan error, 1)
	go func() { firstResult <- association.ASPUp(context.Background()) }()
	select {
	case message := <-writes:
		if _, ok := message.(*messages.AspUp); !ok {
			t.Fatalf("first explicit procedure wrote %T, want *messages.AspUp", message)
		}
	case <-time.After(time.Second):
		t.Fatal("first explicit ASP Up did not write")
	}

	secondContext, cancelSecond := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancelSecond()
	if err := association.ASPUp(secondContext); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("overlapping ASPUp error = %v, want context deadline exceeded", err)
	}
	select {
	case message := <-writes:
		t.Fatalf("overlapping ASPUp wrote %T before the first procedure completed", message)
	default:
	}

	if err := association.handleAspUpAck(messages.NewAspUpAck(nil, nil)); err != nil {
		t.Fatalf("handleAspUpAck: %v", err)
	}
	if err := <-firstResult; err != nil {
		t.Fatalf("first ASPUp was superseded: %v", err)
	}
}

func TestAssociationASPActiveAndInactiveWaitForExactAcknowledgement(t *testing.T) {
	key := ASKey{
		NetworkAppearance:    10,
		NetworkAppearanceSet: true,
		RoutingContext:       7,
		RoutingContextSet:    true,
	}

	association, _ := newTestConn(t, StateASPInactive, RoleASP)
	association.cfg.NetworkAppearance = params.NewNetworkAppearance(10)
	association.cfg.RoutingContexts = params.NewRoutingContext(7)
	writes := make(chan messages.M3UA, 2)
	association.signalWriter = func(message messages.M3UA) (int, error) {
		writes <- message
		return message.MarshalLen(), nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	activeResult := make(chan error, 1)
	go func() { activeResult <- association.ASPActive(ctx, key) }()
	active := <-writes
	activeMessage, ok := active.(*messages.AspActive)
	if !ok {
		t.Fatalf("ASPActive wrote %T, want *messages.AspActive", active)
	}
	if got := activeMessage.RoutingContext.RoutingContexts(); len(got) != 1 || got[0] != 7 {
		t.Fatalf("ASP Active Routing Contexts = %v, want [7]", got)
	}
	if err := association.handleAspActiveAck(messages.NewAspActiveAck(
		nil, params.NewRoutingContext(7), nil,
	)); err != nil {
		t.Fatalf("handleAspActiveAck: %v", err)
	}
	if err := <-activeResult; err != nil {
		t.Fatalf("ASPActive: %v", err)
	}

	association.setState(StateASPActive)
	inactiveResult := make(chan error, 1)
	go func() { inactiveResult <- association.ASPInactive(ctx, key) }()
	inactive := <-writes
	inactiveMessage, ok := inactive.(*messages.AspInactive)
	if !ok {
		t.Fatalf("ASPInactive wrote %T, want *messages.AspInactive", inactive)
	}
	if got := inactiveMessage.RoutingContext.RoutingContexts(); len(got) != 1 || got[0] != 7 {
		t.Fatalf("ASP Inactive Routing Contexts = %v, want [7]", got)
	}
	if err := association.handleAspInactiveAck(messages.NewAspInactiveAck(
		params.NewRoutingContext(7), nil,
	)); err != nil {
		t.Fatalf("handleAspInactiveAck: %v", err)
	}
	if err := <-inactiveResult; err != nil {
		t.Fatalf("ASPInactive: %v", err)
	}
}

func TestAssociationASPActiveGroupsTrafficModesAndWaitsForEveryAck(t *testing.T) {
	association, _ := newTestConn(t, StateASPInactive, RoleASP)
	association.cfg.NetworkAppearance = params.NewNetworkAppearance(10)
	association.cfg.RoutingContexts = params.NewRoutingContext(7, 8)
	association.cfg.TrafficModeType = nil
	association.trafficModes.freeze(newTrafficModePolicy(&AssociationConfig{
		TrafficModes: map[uint32]uint32{
			7: params.TrafficModeLoadshare,
			8: params.TrafficModeBroadcast,
		},
	}))
	writes := make(chan messages.M3UA, 2)
	association.signalWriter = func(message messages.M3UA) (int, error) {
		writes <- message
		return message.MarshalLen(), nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- association.ASPActive(ctx,
			ASKey{NetworkAppearance: 10, NetworkAppearanceSet: true, RoutingContext: 7, RoutingContextSet: true},
			ASKey{NetworkAppearance: 10, NetworkAppearanceSet: true, RoutingContext: 8, RoutingContextSet: true},
		)
	}()

	first := (<-writes).(*messages.AspActive)
	second := (<-writes).(*messages.AspActive)
	byRoutingContext := map[uint32]*messages.AspActive{
		first.RoutingContext.RoutingContexts()[0]:  first,
		second.RoutingContext.RoutingContexts()[0]: second,
	}
	if byRoutingContext[7].TrafficModeType.TrafficModeType() != params.TrafficModeLoadshare ||
		byRoutingContext[8].TrafficModeType.TrafficModeType() != params.TrafficModeBroadcast {
		t.Fatalf("ASP Active requests = %+v, %+v", first, second)
	}

	if err := association.handleAspActiveAck(messages.NewAspActiveAck(
		params.NewTrafficModeType(params.TrafficModeBroadcast),
		params.NewRoutingContext(8), nil,
	)); err != nil {
		t.Fatalf("out-of-order ASP Active Ack: %v", err)
	}
	select {
	case err := <-result:
		t.Fatalf("ASPActive returned before every Ack: %v", err)
	default:
	}
	if err := association.handleAspActiveAck(messages.NewAspActiveAck(
		params.NewTrafficModeType(params.TrafficModeLoadshare),
		params.NewRoutingContext(7), nil,
	)); err != nil {
		t.Fatalf("final ASP Active Ack: %v", err)
	}
	if err := <-result; err != nil {
		t.Fatalf("ASPActive: %v", err)
	}
}

func TestAssociationASPTMExplicitOperationsRejectBeforeWrite(t *testing.T) {
	association, _ := newTestConn(t, StateASPInactive, RoleASP)
	association.cfg.NetworkAppearance = params.NewNetworkAppearance(10)
	association.cfg.RoutingContexts = params.NewRoutingContext(7)
	writes := 0
	association.signalWriter = func(message messages.M3UA) (int, error) {
		writes++
		return message.MarshalLen(), nil
	}
	wrongAppearance := ASKey{
		NetworkAppearance:    20,
		NetworkAppearanceSet: true,
		RoutingContext:       7,
		RoutingContextSet:    true,
	}
	if err := association.ASPActive(context.Background(), wrongAppearance); !errors.Is(err, ErrInvalidNetworkAppearance) {
		t.Fatalf("ASPActive wrong Network Appearance error = %v, want ErrInvalidNetworkAppearance", err)
	}
	if writes != 0 {
		t.Fatalf("wrong Network Appearance wrote %d messages, want 0", writes)
	}

	association.setState(StateASPDown)
	if err := association.ASPActive(context.Background()); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("ASPActive from ASP-DOWN error = %v, want ErrInvalidState", err)
	}
	association.setState(StateASPInactive)
	if err := association.ASPInactive(context.Background()); !errors.Is(err, ErrInvalidState) {
		t.Fatalf("ASPInactive from ASP-INACTIVE error = %v, want ErrInvalidState", err)
	}

	sgp, _ := newTestConn(t, StateASPActive, RoleSGP)
	sgpWrites := 0
	sgp.signalWriter = func(message messages.M3UA) (int, error) {
		sgpWrites++
		return message.MarshalLen(), nil
	}
	if err := sgp.ASPInactive(context.Background()); !errors.Is(err, ErrUnsupportedRole) {
		t.Fatalf("SGP ASPInactive error = %v, want ErrUnsupportedRole", err)
	}
	if sgpWrites != 0 {
		t.Fatalf("SGP ASPInactive wrote %d messages, want 0", sgpWrites)
	}
}

func TestAssociationReadinessFollowsASPProcedurePolicy(t *testing.T) {
	t.Run("nil policy IPSP Double Exchange preserves directional readiness", func(t *testing.T) {
		tests := []struct {
			name  string
			state IPSPState
		}{
			{"local direction", IPSPState{
				TrafficToLocal: StateASPActive,
				TrafficToPeer:  StateASPDown,
			}},
			{"peer direction", IPSPState{
				TrafficToLocal: StateASPDown,
				TrafficToPeer:  StateASPActive,
			}},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				association, _ := newDoubleExchangeIPSPForTest(t)
				association.setIPSPState(test.state)

				association.notifyReady()
				requireAssociationReady(t, association)
			})
		}
	})

	t.Run("explicit ASP Up returns at ASP-DOWN", func(t *testing.T) {
		association, _ := newTestConn(t, StateASPDown, RoleASP)
		association.cfg.ASPProcedures = explicitASPProcedurePolicy()
		writes := 0
		association.signalWriter = func(message messages.M3UA) (int, error) {
			writes++
			return message.MarshalLen(), nil
		}
		if err := association.handleStateUpdate(StateASPDown); err != nil {
			t.Fatalf("handleStateUpdate: %v", err)
		}
		requireAssociationReady(t, association)
		if writes != 0 {
			t.Fatalf("explicit ASP Up policy wrote %d messages, want 0", writes)
		}
	})

	t.Run("explicit ASP Active returns at ASP-INACTIVE", func(t *testing.T) {
		association, _ := newTestConn(t, StateASPDown, RoleASP)
		association.cfg.ASPProcedures = &ASPProcedurePolicy{
			ASPUp:       ASPProcedureAutomatic,
			ASPDown:     ASPProcedureAutomatic,
			ASPActive:   ASPProcedureExplicit,
			ASPInactive: ASPProcedureAutomatic,
		}
		writes := make([]messages.M3UA, 0, 1)
		association.signalWriter = func(message messages.M3UA) (int, error) {
			writes = append(writes, message)
			return message.MarshalLen(), nil
		}
		if err := association.handleStateUpdate(StateASPDown); err != nil {
			t.Fatalf("ASP-DOWN entry: %v", err)
		}
		select {
		case <-association.established:
			t.Fatal("Association became ready before ASP Up Ack")
		default:
		}
		if len(writes) != 1 {
			t.Fatalf("ASP-DOWN entry wrote %d messages, want ASP Up only", len(writes))
		}
		if err := association.handleStateUpdate(StateASPInactive); err != nil {
			t.Fatalf("ASP-INACTIVE entry: %v", err)
		}
		requireAssociationReady(t, association)
		if len(writes) != 1 {
			t.Fatalf("explicit ASP Active policy wrote %d messages, want ASP Up only", len(writes))
		}
		select {
		case <-association.beatStart:
			t.Fatal("M3UA BEAT started before ASP-ACTIVE")
		default:
		}
	})

	t.Run("SGP still waits for peer ASP-ACTIVE", func(t *testing.T) {
		association, _ := newTestConn(t, StateASPDown, RoleSGP)
		if err := association.handleStateUpdate(StateASPDown); err != nil {
			t.Fatalf("ASP-DOWN entry: %v", err)
		}
		if err := association.handleStateUpdate(StateASPInactive); err != nil {
			t.Fatalf("ASP-INACTIVE entry: %v", err)
		}
		select {
		case <-association.established:
			t.Fatal("SGP Association became ready before peer ASP-ACTIVE")
		default:
		}
		if err := association.handleStateUpdate(StateASPActive); err != nil {
			t.Fatalf("ASP-ACTIVE entry: %v", err)
		}
		requireAssociationReady(t, association)
	})
}

func TestShutdownContextRunsOnlyAutomaticASPProcedures(t *testing.T) {
	t.Run("both explicit", func(t *testing.T) {
		association, _ := newTestConn(t, StateASPActive, RoleASP)
		association.cfg.ASPProcedures = explicitASPProcedurePolicy()
		writes := 0
		association.signalWriter = func(message messages.M3UA) (int, error) {
			writes++
			return message.MarshalLen(), nil
		}
		if err := association.ShutdownContext(context.Background()); err != nil {
			t.Fatalf("ShutdownContext: %v", err)
		}
		if writes != 0 {
			t.Fatalf("explicit shutdown policy wrote %d messages, want 0", writes)
		}
	})

	t.Run("automatic ASP Inactive and explicit ASP Down", func(t *testing.T) {
		association, _ := newTestConn(t, StateASPActive, RoleASP)
		association.cfg.ASPProcedures = &ASPProcedurePolicy{
			ASPUp:       ASPProcedureExplicit,
			ASPDown:     ASPProcedureExplicit,
			ASPActive:   ASPProcedureExplicit,
			ASPInactive: ASPProcedureAutomatic,
		}
		writes := make(chan messages.M3UA, 2)
		association.signalWriter = func(message messages.M3UA) (int, error) {
			writes <- message
			return message.MarshalLen(), nil
		}
		result := make(chan error, 1)
		go func() { result <- association.ShutdownContext(context.Background()) }()
		message := <-writes
		if _, ok := message.(*messages.AspInactive); !ok {
			t.Fatalf("Shutdown wrote %T, want ASP Inactive", message)
		}
		if err := association.handleAspInactiveAck(messages.NewAspInactiveAck(
			association.cfg.RoutingContexts.Copy(), nil,
		)); err != nil {
			t.Fatalf("handleAspInactiveAck: %v", err)
		}
		if err := <-result; err != nil {
			t.Fatalf("ShutdownContext: %v", err)
		}
		select {
		case extra := <-writes:
			t.Fatalf("explicit ASP Down policy wrote extra %T", extra)
		default:
		}
	})

	t.Run("explicit ASP Inactive and automatic ASP Down", func(t *testing.T) {
		association, _ := newTestConn(t, StateASPActive, RoleASP)
		association.cfg.ASPProcedures = &ASPProcedurePolicy{
			ASPUp:       ASPProcedureExplicit,
			ASPDown:     ASPProcedureAutomatic,
			ASPActive:   ASPProcedureExplicit,
			ASPInactive: ASPProcedureExplicit,
		}
		writes := make(chan messages.M3UA, 2)
		association.signalWriter = func(message messages.M3UA) (int, error) {
			writes <- message
			return message.MarshalLen(), nil
		}
		result := make(chan error, 1)
		go func() { result <- association.ShutdownContext(context.Background()) }()
		message := <-writes
		if _, ok := message.(*messages.AspDown); !ok {
			t.Fatalf("Shutdown wrote %T, want ASP Down", message)
		}
		if err := association.handleAspDownAck(messages.NewAspDownAck(nil)); err != nil {
			t.Fatalf("handleAspDownAck: %v", err)
		}
		if err := <-result; err != nil {
			t.Fatalf("ShutdownContext: %v", err)
		}
		select {
		case extra := <-writes:
			t.Fatalf("explicit ASP Inactive policy wrote extra %T", extra)
		default:
		}
	})
}

func explicitASPProcedurePolicy() *ASPProcedurePolicy {
	return &ASPProcedurePolicy{
		ASPUp:       ASPProcedureExplicit,
		ASPDown:     ASPProcedureExplicit,
		ASPActive:   ASPProcedureExplicit,
		ASPInactive: ASPProcedureExplicit,
	}
}

func requireAssociationReady(t *testing.T, association *Association) {
	t.Helper()
	select {
	case <-association.established:
	case <-time.After(time.Second):
		t.Fatal("Association did not signal policy-selected readiness")
	}
}
