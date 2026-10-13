package m3ua

import (
	"context"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

// RFC 4666 §5.6.1: A initiates ASPSM, B initiates ASPTM and eventually takes
// the communication down. Compare both Double Exchange agreements as well.
func TestIPSPPeerDownAcrossExchangeModels(t *testing.T) {
	for _, model := range ipspDownModels {
		t.Run(model.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			endpointA, err := NewEndpoint(EndpointConfig{Role: RoleIPSP})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = endpointA.Close() })
			endpointB, err := NewEndpoint(EndpointConfig{Role: RoleIPSP})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = endpointB.Close() })
			configA := integrationIPSPConfig(0, 0, true, false)
			configB := integrationIPSPConfig(0, 0, false, true)
			for _, config := range []*AssociationConfig{configA, configB} {
				config.HeartbeatInfo = &HeartbeatInfo{Enabled: false}
				config.IPSP.ExchangeModel = model.exchange
				config.IPSP.ASPSMExchange = model.aspsm
				setInventoryNetworkAppearance(&config.ApplicationServers, nil)
				setInventoryRoutingContexts(&config.ApplicationServers, params.NewRoutingContext(7))
				if model.exchange == IPSPExchangeDouble {
					config.IPSP.TrafficToLocal = &IPSPTrafficConfig{ApplicationServers: config.ApplicationServers}
					config.IPSP.TrafficToPeer = &IPSPTrafficConfig{ApplicationServers: config.ApplicationServers}
					config.ApplicationServers = nil
				}
			}
			// Normal Double Exchange needs B's own ASPSM before B can initiate
			// ASPTM; the opposite direction has its own ASP Up exchange (§4.3).
			if !model.sharedASPSM {
				configB.ASPProcedures.ASPUp = ASPProcedureAutomatic
			}
			listener, err := endpointB.Listen("m3ua", mcAddr(0, "127.0.0.3"), NewListenerConfig(configB))
			if err != nil {
				if isSCTPUnsupported(err) {
					t.Skipf("skipping socket-backed test: %v", err)
				}
				t.Fatalf("Listen: %v", err)
			}
			t.Cleanup(func() { _ = listener.Close() })
			accepted := make(chan associationResult, 1)
			go func() {
				association, acceptErr := listener.Accept(ctx)
				accepted <- associationResult{association: association, err: acceptErr}
			}()
			associationA, err := endpointA.Dial(ctx, "m3ua", mcAddr(0, "127.0.0.2"), listener.Addr().(*sctp.Addr), configA)
			if err != nil {
				t.Fatalf("Dial: %v", err)
			}
			var associationB *Association
			select {
			case result := <-accepted:
				if result.err != nil {
					t.Fatalf("Accept: %v", result.err)
				}
				associationB = result.association
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			t.Logf("IPSP-A=%s IPSP-B=%s", associationA.LocalAddr(), associationB.LocalAddr())
			if !waitFor(func() bool {
				return associationA.IPSPState().TrafficToPeer == StateASPActive &&
					associationB.IPSPState().TrafficToLocal == StateASPActive
			}, 3*time.Second) {
				t.Fatalf("startup: A=%+v B=%+v", associationA.IPSPState(), associationB.IPSPState())
			}
			for _, procedure := range []func(context.Context, ...ASKey) error{
				associationB.ASPInactive, associationB.ASPActive, associationB.ASPInactive,
			} {
				if err := procedure(ctx); err != nil {
					t.Fatalf("peer ASPTM procedure: %v", err)
				}
			}
			if !waitFor(func() bool {
				associationA.muState.RLock()
				defer associationA.muState.RUnlock()
				return associationA.state == StateASPInactive && associationA.appliedState == StateASPInactive
			}, 3*time.Second) {
				t.Fatal("peer ASP Inactive entry was not applied")
			}
			// Observe only edges following the final ASP Inactive Ack. Any
			// climb after ASP Down exposes the unwanted automatic ASP Up.
			changes := associationA.StateChanges()
			for len(changes) > 0 {
				<-changes
			}
			if err := associationB.ASPDown(ctx); err != nil {
				t.Fatalf("peer ASPDown: %v", err)
			}
			observation := time.NewTimer(3 * configA.TAck)
			defer observation.Stop()
			for {
				select {
				case state := <-changes:
					if state != StateASPDown {
						t.Fatalf("peer ASP Down resumed communication: A entered %v", state)
					}
				case <-observation.C:
					if got := associationA.State(); got != StateASPDown {
						t.Fatalf("state after peer ASP Down = %v, want ASP-DOWN", got)
					}
					return
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-associationA.Done():
					t.Fatal("IPSP-A closed after peer ASP Down")
				case <-associationB.Done():
					t.Fatal("IPSP-B closed after its ASP Down")
				}
			}
		})
	}
}
