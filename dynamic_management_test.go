package m3ua

import (
	"context"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// RFC 4666 Sections 4.4.1 and 4.3.4.3 make the Routing Context assigned by a
// successful Registration Response available to the requester for subsequent
// ASPTM procedures. Section 4.5.3 likewise permits an active ASP to audit the
// destinations it expects to communicate with.
func TestDynamicallyRegisteredASAvailableToASPManagement(t *testing.T) {
	for _, role := range []Role{RoleASP, RoleIPSP} {
		t.Run(role.String(), func(t *testing.T) {
			t.Run("status", func(t *testing.T) {
				endpoint, association, key, _ := newDynamicallyRegisteredASPManagementFixture(
					t, role, StateASPInactive,
				)

				status, ok := endpoint.ASPStatus(ASPStatusKey{Association: association.ID(), AS: key})
				if !ok {
					t.Fatal("ASPStatus did not find the dynamically registered AS")
				}
				if !status.LocalStateSet || status.LocalState != StateASPInactive {
					t.Fatalf("local ASP status = %+v, want ASP-INACTIVE", status)
				}
				statuses := endpoint.ASPStatuses()
				if len(statuses) != 1 || statuses[0].Key.AS != key {
					t.Fatalf("ASPStatuses = %+v, want dynamic ASKey %+v", statuses, key)
				}
			})

			t.Run("ASP Active", func(t *testing.T) {
				_, association, key, writes := newDynamicallyRegisteredASPManagementFixture(
					t, role, StateASPInactive,
				)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				result := make(chan error, 1)
				go func() { result <- association.ASPActive(ctx, key) }()

				var active *messages.AspActive
				select {
				case err := <-result:
					t.Fatalf("ASPActive returned before writing the request: %v", err)
				case message := <-writes:
					var ok bool
					active, ok = message.(*messages.AspActive)
					if !ok {
						t.Fatalf("ASPActive wrote %T, want *messages.AspActive", message)
					}
				case <-ctx.Done():
					t.Fatal("ASPActive did not write a request")
				}
				if got := active.RoutingContext.RoutingContexts(); len(got) != 1 || got[0] != key.RoutingContext {
					t.Fatalf("ASP Active Routing Contexts = %v, want [%d]", got, key.RoutingContext)
				}
				if err := association.handleAspActiveAck(messages.NewAspActiveAck(
					nil, params.NewRoutingContext(key.RoutingContext), nil,
				)); err != nil {
					t.Fatalf("handleAspActiveAck: %v", err)
				}
				if err := <-result; err != nil {
					t.Fatalf("ASPActive: %v", err)
				}
			})

			if role != RoleASP {
				return
			}
			t.Run("destination audit", func(t *testing.T) {
				_, association, key, writes := newDynamicallyRegisteredASPManagementFixture(
					t, role, StateASPActive,
				)
				association.noteRoutingContextsActive([]uint32{key.RoutingContext})
				request := DestinationStateAuditRequest{
					Scope: SSNMScope{
						NetworkAppearance:    key.NetworkAppearance,
						NetworkAppearanceSet: true,
						RoutingContexts:      []uint32{key.RoutingContext},
						RoutingContextSet:    true,
					},
					Destinations: []PointCodeRange{{PointCode: 0x123456}},
				}
				if err := association.DestinationStateAudit(request); err != nil {
					t.Fatalf("DestinationStateAudit: %v", err)
				}
				audit, ok := (<-writes).(*messages.DestinationStateAudit)
				if !ok {
					t.Fatalf("DestinationStateAudit wrote an unexpected message")
				}
				if audit.RoutingContext == nil || audit.RoutingContext.RoutingContext() != key.RoutingContext {
					t.Fatalf("DAUD Routing Context = %v, want %d", audit.RoutingContext, key.RoutingContext)
				}
			})
		})
	}
}

func newDynamicallyRegisteredASPManagementFixture(
	t *testing.T,
	role Role,
	state State,
) (*Endpoint, *Association, ASKey, <-chan messages.M3UA) {
	t.Helper()
	endpoint, err := NewEndpoint(EndpointConfig{Role: role})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })

	association := newAssociation(role, NewAssociationConfig(0, 0, 0, 0, 0, 0))
	association.muState.Lock()
	association.state = state
	association.muState.Unlock()
	writes := make(chan messages.M3UA, 1)
	association.signalWriter = func(message messages.M3UA) (int, error) {
		writes <- message
		return message.MarshalLen(), nil
	}
	if !endpoint.trackAssociation(association) {
		t.Fatal("track Association")
	}

	routingKey := testRoutingKey(10, 0x654321, params.ServiceIndSCCP)
	const routingContext = 100
	if err := association.applyRegistrationResults([]registrationResultApplication{{
		request: RoutingKeyRegistrationRequest{RoutingKey: routingKey},
		result: RoutingKeyRegistrationResult{
			Status:         RegistrationSuccessfullyRegistered,
			RoutingContext: routingContext,
		},
	}}); err != nil {
		t.Fatalf("applyRegistrationResults: %v", err)
	}
	key := ASKey{
		NetworkAppearance:    routingKey.NetworkAppearance,
		NetworkAppearanceSet: true,
		RoutingContext:       routingContext,
		RoutingContextSet:    true,
	}
	return endpoint, association, key, writes
}
