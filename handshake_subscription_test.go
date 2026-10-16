package m3ua

import (
	"context"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
)

func TestOpeningASPDownIsNotAStateChange(t *testing.T) {
	for _, test := range []struct {
		name  string
		role  Role
		model IPSPExchangeModel
	}{
		{"ASP", RoleASP, 0},
		{"SGP", RoleSGP, 0},
		{"IPSP single", RoleIPSP, IPSPExchangeSingle},
		{"IPSP double", RoleIPSP, IPSPExchangeDouble},
	} {
		t.Run(test.name, func(t *testing.T) {
			cfg := NewAssociationConfig()
			cfg.ASPProcedures = ipspInitiationPolicy(true, false)
			if test.model != 0 {
				cfg.IPSP = &IPSPConfig{ExchangeModel: test.model}
			}
			a := newAssociation(test.role, cfg)
			t.Cleanup(func() { _ = a.Close() })
			var upRequests int
			a.signalWriter = func(message messages.M3UA) (int, error) {
				if _, ok := message.(*messages.AspUp); ok {
					upRequests++
				}
				return message.MarshalLen(), nil
			}
			states := a.StateChanges()
			if err := a.handleStateUpdate(StateASPDown); err != nil {
				t.Fatal(err)
			}
			wantRequests := 1
			if test.role == RoleSGP {
				wantRequests = 0
			}
			if upRequests != wantRequests {
				t.Fatalf("opening entry sent %d ASP Up requests, want %d", upRequests, wantRequests)
			}
			if len(states) != 0 {
				t.Fatalf("opening state was reported as a transition: %v", <-states)
			}
			if err := a.handleStateUpdate(StateASPInactive); err != nil {
				t.Fatal(err)
			}
			select {
			case state := <-states:
				if state != StateASPInactive {
					t.Fatalf("transition = %v", state)
				}
			default:
				t.Fatal("real transition was lost")
			}
		})
	}
}

func TestClosedOpeningStateDoesNotPublishReadiness(t *testing.T) {
	for _, test := range []struct {
		name   string
		role   Role
		double bool
	}{{"ASP", RoleASP, false}, {"SGP", RoleSGP, false}, {"IPSP single", RoleIPSP, false}, {"IPSP double", RoleIPSP, true}} {
		t.Run(test.name, func(t *testing.T) {
			role := test.role
			cfg := NewAssociationConfig()
			if test.double {
				cfg = newDoubleExchangeAssociationConfigForTest()
			}
			if role != RoleSGP {
				cfg.ASPProcedures = ipspInitiationPolicy(false, false)
			}
			if role == RoleIPSP && !test.double {
				cfg.IPSP = &IPSPConfig{ExchangeModel: IPSPExchangeSingle}
			}
			a := newAssociation(role, cfg)
			// Subscribe may overlap Endpoint teardown before monitor applies
			// the opening state. Explicit ASP Up normally returns at ASP-DOWN,
			// but a closed association has not reached usable readiness.
			if err := a.Close(); err != nil {
				t.Fatal(err)
			}
			if err := a.handleStateUpdate(StateASPDown); err != nil {
				t.Fatal(err)
			}
			select {
			case <-a.established:
				t.Fatal("closed association published establishment readiness")
			default:
			}
		})
	}
}

// A bounded receive also makes a missing callback fail rather than hanging.
func receiveSubscription[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value, ok := <-ch:
		if !ok {
			t.Fatal("subscription closed early")
		}
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("subscription did not deliver")
		var zero T
		return zero
	}
}

func subscriptionContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}
