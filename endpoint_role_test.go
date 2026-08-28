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

func TestIPSPRequiresExplicitExchangeMode(t *testing.T) {
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
				select {
				case status := <-acceptedAssociation.SignallingStatus():
					if status.PointCode != pointCode || status.State != DestinationAvailable {
						t.Fatalf("DAUD response = %+v, want point code %#x available", status, pointCode)
					}
				case <-ctx.Done():
					t.Fatalf("DAUD response did not arrive: %v", ctx.Err())
				}
			}
		})
	}
}
