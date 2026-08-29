// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

func TestNewEndpointRejectsInvalidRole(t *testing.T) {
	endpoint, err := NewEndpoint(Role(0xff))
	if endpoint != nil {
		t.Fatalf("NewEndpoint returned endpoint %v for an invalid role", endpoint)
	}
	if !errors.Is(err, ErrUnsupportedRole) {
		t.Fatalf("NewEndpoint error = %v, want ErrUnsupportedRole", err)
	}
}

func TestASPListenerDoesNotRunSGPAvailabilityProcedures(t *testing.T) {
	tests := []struct {
		name string
		call func(*Listener) error
	}{
		{
			name: "NIF isolation",
			call: func(listener *Listener) error {
				return listener.SetNIFAvailable(false)
			},
		},
		{
			name: "Application Server isolation",
			call: func(listener *Listener) error {
				return listener.SetASAvailableForAS(ASKey{RoutingContext: 1, RoutingContextSet: true}, false)
			},
		},
		{
			name: "legacy Routing Context Application Server isolation",
			call: func(listener *Listener) error {
				return listener.SetASAvailable(1, false)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			endpoint, err := NewEndpoint(RoleASP)
			if err != nil {
				t.Fatalf("NewEndpoint(RoleASP): %v", err)
			}
			listener := newListener(endpoint, NewListenerConfig(NewAssociationConfig(0, 0, 0, 0, 0, 0)))
			association, sent := newTestConn(t, StateASPActive, RoleASP)
			association.cfg.RoutingContexts = params.NewRoutingContext(1)
			association.noteRoutingContextsActive([]uint32{1})
			if !listener.track(association) {
				t.Fatal("failed to track test Association")
			}

			if err := test.call(listener); !errors.Is(err, ErrUnsupportedRole) {
				t.Fatalf("availability control error = %v, want ErrUnsupportedRole", err)
			}

			if got := association.State(); got != StateASPActive {
				t.Fatalf("ASP Association state = %v, want ASP-ACTIVE", got)
			}
			if got := len(*sent); got != 0 {
				t.Fatalf("ASP Listener emitted %d SGP control messages, want 0", got)
			}
			if listener.as != nil || listener.nif != nil {
				t.Fatal("ASP Listener initialized SGP shared state")
			}
		})
	}
}

func TestSGPEndpointStateOwnerReservationIsExclusive(t *testing.T) {
	endpoint, err := NewEndpoint(RoleSGP)
	if err != nil {
		t.Fatalf("NewEndpoint(RoleSGP): %v", err)
	}
	firstRelease, err := endpoint.reserveSGPStateOwner()
	if err != nil {
		t.Fatalf("reserve first SGP state owner: %v", err)
	}
	if _, err := endpoint.reserveSGPStateOwner(); !errors.Is(err, ErrEndpointStateInUse) {
		t.Fatalf("reserve second SGP state owner error = %v, want ErrEndpointStateInUse", err)
	}
	firstRelease()
	secondRelease, err := endpoint.reserveSGPStateOwner()
	if err != nil {
		t.Fatalf("reserve SGP state owner after release: %v", err)
	}
	secondRelease()
}

func TestSGPEndpointDialOwnsStateUntilAssociationCloses(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	aspEndpoint, err := NewEndpoint(RoleASP)
	if err != nil {
		t.Fatalf("NewEndpoint(RoleASP): %v", err)
	}
	aspConfig := mcASPConfig(0xABCD0001)
	aspConfig.RoutingContexts = params.NewRoutingContext(1)
	address := mcAddr(0, "127.0.0.1")
	listener, err := aspEndpoint.Listen("m3ua", address, NewListenerConfig(aspConfig))
	if err != nil {
		skipIfSCTPUnsupported(t, err)
		t.Fatalf("ASP Listen: %v", err)
	}
	defer func() { _ = listener.Close() }()

	type acceptResult struct {
		association *Association
		err         error
	}
	accepted := make(chan acceptResult, 1)
	go func() {
		association, acceptErr := listener.Accept(ctx)
		accepted <- acceptResult{association: association, err: acceptErr}
	}()

	sgpEndpoint, err := NewEndpoint(RoleSGP)
	if err != nil {
		t.Fatalf("NewEndpoint(RoleSGP): %v", err)
	}
	sgpConfig := mcSGPConfig()
	sgpConfig.RoutingContexts = params.NewRoutingContext(1)
	dialed, err := sgpEndpoint.Dial(ctx, "m3ua", nil, listener.Addr().(*sctp.SCTPAddr), sgpConfig)
	if err != nil {
		t.Fatalf("SGP Dial: %v", err)
	}

	result := <-accepted
	if result.err != nil {
		t.Fatalf("ASP Accept: %v", result.err)
	}
	defer func() { _ = result.association.Close() }()

	cancelled, cancelDial := context.WithCancel(context.Background())
	cancelDial()
	if _, err := sgpEndpoint.Dial(cancelled, "m3ua", nil, nil, sgpConfig); !errors.Is(err, ErrEndpointStateInUse) {
		t.Fatalf("second SGP Dial error = %v, want ErrEndpointStateInUse", err)
	}
	if err := dialed.Close(); err != nil {
		t.Fatalf("close first dialed SGP Association: %v", err)
	}
	if _, err := sgpEndpoint.Dial(cancelled, "m3ua", nil, nil, sgpConfig); !errors.Is(err, context.Canceled) {
		t.Fatalf("SGP Dial after Association close error = %v, want context.Canceled", err)
	}
}

func TestSGPListenerRetainsStateOwnershipUntilInFlightAcceptReturns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	selectorEntered := make(chan struct{})
	selectorRelease := make(chan struct{})
	config := NewListenerConfig(mcSGPConfig())
	config.SelectAssociationConfig = func(AcceptInfo) (*AssociationConfig, error) {
		close(selectorEntered)
		<-selectorRelease
		return mcSGPConfig(), nil
	}

	endpoint, err := NewEndpoint(RoleSGP)
	if err != nil {
		t.Fatalf("NewEndpoint(RoleSGP): %v", err)
	}
	address := mcAddr(0, "127.0.0.1")
	listener, err := endpoint.Listen("m3ua", address, config)
	if err != nil {
		skipIfSCTPUnsupported(t, err)
		t.Fatalf("SGP Listen: %v", err)
	}

	accepted := make(chan error, 1)
	go func() {
		association, acceptErr := listener.Accept(ctx)
		if association != nil {
			_ = association.Close()
		}
		accepted <- acceptErr
	}()

	raw, err := sctp.DialSCTP("sctp", nil, listener.Addr().(*sctp.SCTPAddr))
	if err != nil {
		_ = listener.Close()
		t.Fatalf("raw SCTP association: %v", err)
	}
	defer func() { _ = raw.Close() }()

	select {
	case <-selectorEntered:
	case <-ctx.Done():
		_ = listener.Close()
		t.Fatalf("selector was not entered: %v", ctx.Err())
	}

	if err := listener.Close(); err != nil {
		t.Fatalf("close Listener: %v", err)
	}
	cancelled, cancelDial := context.WithCancel(context.Background())
	cancelDial()
	if _, err := endpoint.Dial(cancelled, "m3ua", nil, nil, mcSGPConfig()); !errors.Is(err, ErrEndpointStateInUse) {
		t.Fatalf("SGP Dial while Accept is in flight error = %v, want ErrEndpointStateInUse", err)
	}

	close(selectorRelease)
	select {
	case err := <-accepted:
		if err == nil {
			t.Fatal("Accept succeeded after Listener.Close")
		}
	case <-ctx.Done():
		t.Fatalf("Accept did not return after selector release: %v", ctx.Err())
	}
	if _, err := endpoint.Dial(cancelled, "m3ua", nil, nil, mcSGPConfig()); !errors.Is(err, context.Canceled) {
		t.Fatalf("SGP Dial after Accept returned error = %v, want context.Canceled", err)
	}
}

func TestSGPListenerCloseStopsAssociationDuringM3UAEstablishment(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	endpoint, err := NewEndpoint(RoleSGP)
	if err != nil {
		t.Fatalf("NewEndpoint(RoleSGP): %v", err)
	}
	config := mcSGPConfig()
	config.EstablishTimeout = 30 * time.Second
	listener, err := endpoint.Listen(
		"m3ua", mcAddr(0, "127.0.0.1"), NewListenerConfig(config),
	)
	if err != nil {
		skipIfSCTPUnsupported(t, err)
		t.Fatalf("SGP Listen: %v", err)
	}

	accepted := make(chan error, 1)
	go func() {
		association, acceptErr := listener.Accept(ctx)
		if association != nil {
			_ = association.Close()
		}
		accepted <- acceptErr
	}()
	raw, err := sctp.DialSCTP("sctp", nil, listener.Addr().(*sctp.SCTPAddr))
	if err != nil {
		_ = listener.Close()
		t.Fatalf("raw SCTP association: %v", err)
	}
	defer func() { _ = raw.Close() }()

	if !waitFor(func() bool {
		listener.muConns.Lock()
		defer listener.muConns.Unlock()
		return listener.activeAccept == 1 && len(listener.conns) == 1
	}, 5*time.Second) {
		_ = listener.Close()
		t.Fatal("accepted SCTP association never entered M3UA establishment")
	}

	if err := listener.Close(); err != nil {
		t.Fatalf("close Listener: %v", err)
	}
	select {
	case err := <-accepted:
		if err == nil {
			t.Fatal("Accept succeeded for a peer that never sent ASP Up")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Accept remained in M3UA establishment after Listener.Close")
	}

	cancelled, cancelDial := context.WithCancel(context.Background())
	cancelDial()
	if _, err := endpoint.Dial(cancelled, "m3ua", nil, nil, mcSGPConfig()); !errors.Is(err, context.Canceled) {
		t.Fatalf("SGP Dial after in-flight establishment stopped error = %v, want context.Canceled", err)
	}
}

func TestIPSPDialAndListenRequireAnExplicitExchangeModel(t *testing.T) {
	endpoint, err := NewEndpoint(RoleIPSP)
	if err != nil {
		t.Fatalf("NewEndpoint(RoleIPSP): %v", err)
	}

	if _, err := endpoint.Dial(context.Background(), "m3ua", nil, nil, NewAssociationConfig(0, 0, 0, 0, 0, 0)); !errors.Is(err, ErrUnsupportedRole) {
		t.Fatalf("Dial error = %v, want ErrUnsupportedRole", err)
	}
	if _, err := endpoint.Listen("m3ua", nil, NewListenerConfig(nil)); !errors.Is(err, ErrUnsupportedRole) {
		t.Fatalf("Listen error = %v, want ErrUnsupportedRole", err)
	}
}

func TestIPSPAssociationRejectsSGPDestinationProcedures(t *testing.T) {
	association, _ := newTestConnWithContexts(t, StateASPActive, RoleIPSP, 1)
	const (
		networkAppearance = uint32(7)
		routingContext    = uint32(1)
		pointCode         = uint32(0x123456)
	)

	if err := association.ReportDestinationStateForNetworkAndRoutingContext(
		networkAppearance, routingContext, pointCode, DestinationRestricted,
	); !errors.Is(err, ErrUnsupportedRole) {
		t.Fatalf("IPSP destination report error = %v, want ErrUnsupportedRole", err)
	}
	association.SetDestinationStateForNetworkAndRoutingContext(
		networkAppearance, routingContext, pointCode, DestinationRestricted,
	)
	if got := association.DestinationStateForNetworkAndRoutingContext(
		networkAppearance, routingContext, pointCode,
	); got != DestinationAvailable {
		t.Fatalf("IPSP destination state = %v after unsupported update, want available", got)
	}
}

func TestASPAssociationRejectsSGPDestinationReports(t *testing.T) {
	association, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, 1)
	const (
		networkAppearance = uint32(7)
		routingContext    = uint32(1)
		pointCode         = uint32(0x123456)
	)
	association.SetDestinationStateForNetworkAndRoutingContext(
		networkAppearance, routingContext, pointCode, DestinationRestricted,
	)

	if err := association.ReportDestinationStateForNetworkAndRoutingContext(
		networkAppearance, routingContext, pointCode, DestinationUnavailable,
	); !errors.Is(err, ErrUnsupportedRole) {
		t.Fatalf("ASP destination report error = %v, want ErrUnsupportedRole", err)
	}
	if got := association.DestinationStateForNetworkAndRoutingContext(
		networkAppearance, routingContext, pointCode,
	); got != DestinationRestricted {
		t.Fatalf("ASP destination state = %v after rejected report, want restricted", got)
	}
}

func TestASPListenerRejectsSGPDestinationProcedures(t *testing.T) {
	endpoint, err := NewEndpoint(RoleASP)
	if err != nil {
		t.Fatalf("NewEndpoint(RoleASP): %v", err)
	}
	config := mcASPConfig(1)
	config.NetworkAppearance = params.NewNetworkAppearance(7)
	listener := newListener(endpoint, NewListenerConfig(config))
	const pointCode = uint32(0x123456)

	if err := listener.ReportDestinationStateForNetworkAndRoutingContext(
		7, 1, pointCode, DestinationRestricted,
	); !errors.Is(err, ErrUnsupportedRole) {
		t.Fatalf("ASP Listener destination report error = %v, want ErrUnsupportedRole", err)
	}
	listener.SetDestinationStateForNetworkAndRoutingContext(7, 1, pointCode, DestinationRestricted)
	if state, known := listener.DestinationStateForNetworkAndRoutingContext(7, 1, pointCode); known {
		t.Fatalf("ASP Listener destination state = (%v, %v) after unsupported update, want unknown", state, known)
	}
}

func TestDialNormalizesAZeroAssociationConfigBeforeTransport(t *testing.T) {
	endpoint, err := NewEndpoint(RoleASP)
	if err != nil {
		t.Fatalf("NewEndpoint(RoleASP): %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := endpoint.Dial(ctx, "m3ua", nil, nil, &AssociationConfig{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Dial error = %v, want context.Canceled", err)
	}
}

func TestDialRejectsNilAssociationConfigBeforeTransport(t *testing.T) {
	endpoint, err := NewEndpoint(RoleASP)
	if err != nil {
		t.Fatalf("NewEndpoint(RoleASP): %v", err)
	}
	if _, err := endpoint.Dial(context.Background(), "m3ua", nil, nil, nil); !errors.Is(err, ErrNilAssociationConfig) {
		t.Fatalf("Dial error = %v, want ErrNilAssociationConfig", err)
	}
}

func TestAssociationConfigRejectsRoleSpecificSettings(t *testing.T) {
	tests := []struct {
		name   string
		role   Role
		config *AssociationConfig
	}{
		{
			name: "ASP with SGP authorization policy",
			role: RoleASP,
			config: func() *AssociationConfig {
				config := NewAssociationConfig(0, 0, 0, 0, 0, 0)
				config.AuthorizeASP = func(ASPIdentity) []uint32 { return nil }
				return config
			}(),
		},
		{
			name: "ASP with SGP recovery policy",
			role: RoleASP,
			config: func() *AssociationConfig {
				config := NewAssociationConfig(0, 0, 0, 0, 0, 0)
				config.RecoveryTimer = time.Second
				return config
			}(),
		},
		{
			name: "SGP with local ASP Identifier",
			role: RoleSGP,
			config: NewAssociationConfig(0, 0, 0, 0, 0, 0).
				SetASPIdentifier(7),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := validateAssociationConfigForRole(test.role, test.config); !errors.Is(err, ErrInvalidRoleConfiguration) {
				t.Fatalf("validation error = %v, want ErrInvalidRoleConfiguration", err)
			}
		})
	}
}

func TestEndpointRoleIsIndependentOfSCTPOrientation(t *testing.T) {
	tests := []struct {
		name         string
		port         int
		acceptRole   Role
		dialRole     Role
		acceptConfig *AssociationConfig
		dialConfig   *AssociationConfig
	}{
		{
			name:         "ASP dials SGP",
			port:         3301,
			acceptRole:   RoleSGP,
			dialRole:     RoleASP,
			acceptConfig: mcSGPConfig(),
			dialConfig:   mcASPConfig(0x11111111),
		},
		{
			name:         "SGP dials ASP",
			port:         3302,
			acceptRole:   RoleASP,
			dialRole:     RoleSGP,
			acceptConfig: mcASPConfig(0x11111111),
			dialConfig:   mcSGPConfig(),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			acceptEndpoint, err := NewEndpoint(test.acceptRole)
			if err != nil {
				t.Fatalf("NewEndpoint(%v): %v", test.acceptRole, err)
			}
			dialEndpoint, err := NewEndpoint(test.dialRole)
			if err != nil {
				t.Fatalf("NewEndpoint(%v): %v", test.dialRole, err)
			}

			address := mcAddr(test.port, "127.0.0.2")
			listener, err := acceptEndpoint.Listen(
				"m3ua", address, NewListenerConfig(test.acceptConfig),
			)
			if err != nil {
				skipIfSCTPUnsupported(t, err)
				t.Fatalf("Listen: %v", err)
			}
			defer func() { _ = listener.Close() }()

			type acceptResult struct {
				association *Association
				err         error
			}
			accepted := make(chan acceptResult, 1)
			go func() {
				association, acceptErr := listener.Accept(ctx)
				accepted <- acceptResult{association: association, err: acceptErr}
			}()

			dialed, err := dialEndpoint.Dial(
				ctx, "m3ua", mcAddr(test.port+100, "127.0.0.1"), address, test.dialConfig,
			)
			if err != nil {
				t.Fatalf("Dial: %v", err)
			}
			defer func() { _ = dialed.Close() }()

			var acceptedAssociation *Association
			select {
			case result := <-accepted:
				if result.err != nil {
					t.Fatalf("Accept: %v", result.err)
				}
				acceptedAssociation = result.association
				defer func() { _ = acceptedAssociation.Close() }()
				if got := acceptedAssociation.Role(); got != test.acceptRole {
					t.Errorf("accepted role = %v, want %v", got, test.acceptRole)
				}
			case <-ctx.Done():
				t.Fatalf("Accept did not complete: %v", ctx.Err())
			}

			if got := dialed.Role(); got != test.dialRole {
				t.Errorf("dialed role = %v, want %v", got, test.dialRole)
			}

			if test.dialRole == RoleSGP {
				const pointCode = 0x1234
				dialed.SetDestinationStateForNetworkAndRoutingContext(
					0, 1, pointCode, DestinationAvailable,
				)
				if _, err := acceptedAssociation.WriteSignal(
					messages.NewDestinationStateAudit(
						nil,
						params.NewRoutingContext(1),
						params.NewAffectedPointCode(pointCode),
						nil,
					),
				); err != nil {
					t.Fatalf("ASP write DAUD: %v", err)
				}
				waitForEndpointDestinationStatus(
					t, ctx, acceptedAssociation, pointCode, DestinationAvailable, "DAUD response",
				)

				restartPointCode := uint32(0x2345)
				restartDestination := AffectedDestination{
					NetworkAppearance:    0,
					NetworkAppearanceSet: true,
					RoutingContext:       1,
					RoutingContextSet:    true,
					PointCode:            restartPointCode,
				}
				restart, err := dialed.BeginMTP3Restart(restartDestination)
				if err != nil {
					t.Fatalf("dialing SGP begin MTP3 restart: %v", err)
				}
				waitForEndpointDestinationStatus(
					t, ctx, acceptedAssociation, restartPointCode, DestinationUnavailable,
					"restart isolation status",
				)
				if err := restart.Update(restartDestination, DestinationAvailable); err != nil {
					t.Fatalf("dialing SGP stage MTP3 restart recovery: %v", err)
				}
				if err := restart.Complete(); err != nil {
					t.Fatalf("dialing SGP complete MTP3 restart: %v", err)
				}
				waitForEndpointDestinationStatus(
					t, ctx, acceptedAssociation, restartPointCode, DestinationAvailable,
					"restart recovery status",
				)

				if err := dialed.SetASAvailable(1, false); err != nil {
					t.Fatalf("dialing SGP isolate Application Server: %v", err)
				}
				if dialed.activeForRoutingContext(1) {
					t.Fatal("dialing SGP retained active traffic scope after AS isolation")
				}
			}
		})
	}
}

func waitForEndpointDestinationStatus(
	t *testing.T,
	ctx context.Context,
	association *Association,
	pointCode uint32,
	state DestinationState,
	description string,
) {
	t.Helper()
	for {
		select {
		case status, ok := <-association.SignallingStatus():
			if !ok {
				t.Fatalf("Association closed before %s arrived", description)
			}
			if status != nil && status.PointCode == pointCode && status.State == state {
				return
			}
		case <-ctx.Done():
			t.Fatalf("%s for point code %#x and state %v did not arrive: %v",
				description, pointCode, state, ctx.Err())
		}
	}
}
