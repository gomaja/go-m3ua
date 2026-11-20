package m3ua

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

// RFC 4666 §§4.3, 4.3.2, 4.3.4.3 and 1.4.8: the AS distributor must work
// across separate peer addresses and both SCTP initiation orientations.
func TestIPSPManagedDistributionSCTP(t *testing.T) {
	for _, initiator := range []string{"peers", "distributor"} {
		for _, mode := range []uint32{params.TrafficModeLoadshare, params.TrafficModeOverride} {
			t.Run(fmt.Sprintf("%s/%d", initiator, mode), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				central, err := NewEndpoint(nationalCongestionEndpointConfig(EndpointConfig{Role: RoleIPSP,
					ApplicationServers: &ApplicationServerConfig{RecoveryTimer: 10 * time.Second}}))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = central.Close() })
				cfg := integrationIPSPConfig(1, 2, false, false)
				setInventoryTrafficModeType(&cfg.ApplicationServers, params.NewTrafficModeType(mode))
				listener, err := central.Listen("m3ua", mcAddr(0, "127.0.0.1"), NewListenerConfig(cfg))
				if err != nil {
					skipIfSCTPUnsupported(t, err)
					t.Fatal(err)
				}
				var local, remote []*Association
				for i, address := range []string{"127.0.0.2", "127.0.0.3"} {
					e, err := NewEndpoint(nationalCongestionEndpointConfig(EndpointConfig{Role: RoleIPSP}))
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = e.Close() })
					peerCfg := integrationIPSPConfig(uint32(i+2), 1, true, false)
					setInventoryTrafficModeType(&peerCfg.ApplicationServers, params.NewTrafficModeType(mode))
					acceptor, dialer := listener, e
					dialCfg, bind := peerCfg, mcAddr(0, address)
					if initiator == "distributor" {
						acceptor, err = e.Listen("m3ua", mcAddr(0, address), NewListenerConfig(peerCfg))
						if err != nil {
							t.Fatal(err)
						}
						dialer, dialCfg, bind = central, cfg, mcAddr(0, "127.0.0.1")
					}
					accepted := make(chan associationResult, 1)
					go func() {
						a, err := acceptor.Accept(ctx)
						accepted <- associationResult{association: a, err: err}
					}()
					dialed, err := dialer.Dial(ctx, "m3ua", bind, acceptor.Addr().(*sctp.Addr), dialCfg)
					if err != nil {
						t.Fatal(err)
					}
					select {
					case result := <-accepted:
						if result.err != nil {
							t.Fatal(result.err)
						}
						if initiator == "peers" {
							local, remote = append(local, result.association), append(remote, dialed)
						} else {
							local, remote = append(local, dialed), append(remote, result.association)
						}
					case <-ctx.Done():
						t.Fatal(ctx.Err())
					}
				}
				activate := func(index int) {
					t.Helper()
					if err := remote[index].ASPActive(ctx); err != nil {
						t.Fatal(err)
					}
					if !waitFor(func() bool { return local[index].State() == StateASPActive }, time.Second) {
						t.Fatal("peer activation was not committed")
					}
				}
				receive := func(index int, payload string) {
					t.Helper()
					data, err := remote[index].ReadData(ctx)
					if err != nil || string(data.ProtocolData.Data) != payload {
						t.Fatalf("peer %d receive %q: %v, %v", index, payload, data, err)
					}
				}
				send := func(sls uint8, payload string, throughAssociation bool) TrafficDistribution {
					t.Helper()
					data := distributionData(1, sls, payload)
					data.NetworkAppearance = params.NewNetworkAppearance(7)
					distribute := listener.DistributeData
					if throughAssociation {
						distribute = local[0].DistributeData
					}
					result, err := distribute(data)
					if err != nil {
						t.Fatal(err)
					}
					return result
				}
				activate(0)
				if mode == params.TrafficModeLoadshare {
					activate(1)
					ordered := central.as.get(*staticASKey(7, 1)).activeASPs()
					for sequence, sls := range []uint8{3, 0, 7, 2, 3, 2, 6, 1, 5, 4, 7, 0, 4, 5, 6, 1} {
						payload := fmt.Sprintf("IPSP-WIRE-%s-loadshare-%02d-%02d", initiator, sequence, sls)
						if result := send(sls, payload, sls%2 == 1); result.Delivered != 1 || result.Queued {
							t.Fatalf("Loadshare result %+v", result)
						}
						index := 0
						if ordered[int(sls)%len(ordered)] == local[1] {
							index = 1
						}
						receive(index, payload)
					}
					// Every send path refuses SI 0 before submission, including
					// both managed IPSP entry points (RFC 4666 §§1.4.3.2, 1.4.3.4).
					management := distributionData(1, 0, "")
					management.NetworkAppearance = params.NewNetworkAppearance(7)
					management.ProtocolData = params.NewProtocolData(1, 2, 0, 0, 0, 0, []byte("IPSP-WIRE-"+initiator+"-management"))
					for api, distribute := range ipspDistributors(listener, local[0]) {
						result, err := distribute(management)
						requireDataWriteError(t, err, DataNotSent, ErrMTPManagementData)
						if result != (TrafficDistribution{}) {
							t.Fatalf("%s refused SI-0 result = %+v", api, result)
						}
					}
				} else {
					before := "IPSP-WIRE-" + initiator + "-override-before"
					if result := send(0, before, false); result.Delivered != 1 || result.Queued {
						t.Fatalf("Override result %+v", result)
					}
					receive(0, before)
					activate(1)
					if !waitFor(func() bool { return remote[0].State() == StateASPInactive }, time.Second) {
						t.Fatal("overridden peer did not become inactive")
					}
					second := "IPSP-WIRE-" + initiator + "-override-second"
					if result := send(0, second, true); result.Delivered != 1 || result.Queued {
						t.Fatalf("new Override result %+v", result)
					}
					receive(1, second)
					if err := remote[1].ASPInactive(ctx); err != nil {
						t.Fatal(err)
					}
					if central.as.get(*staticASKey(7, 1)).State() != ASPending {
						t.Fatal("last inactive IPSP did not leave AS-PENDING")
					}
					for _, api := range []string{"listener", "association"} {
						if result := send(0, "IPSP-WIRE-"+initiator+"-pending-"+api, api == "association"); !result.Queued || result.Delivered != 0 {
							t.Fatalf("pending result %+v", result)
						}
					}
					activate(0)
					for _, api := range []string{"listener", "association"} {
						receive(0, "IPSP-WIRE-"+initiator+"-pending-"+api)
					}
					as := central.as.get(*staticASKey(7, 1))
					if !waitFor(func() bool { as.mu.Lock(); defer as.mu.Unlock(); return !as.draining }, time.Second) {
						t.Fatal("recovery drain did not finish")
					}
					after := "IPSP-WIRE-" + initiator + "-override-failover"
					if result := send(0, after, true); result.Delivered != 1 || result.Queued {
						t.Fatalf("failover result %+v", result)
					}
					receive(0, after)
				}
				for _, a := range remote {
					quiet, stop := context.WithTimeout(ctx, 20*time.Millisecond)
					_, err := a.ReadData(quiet)
					stop()
					if !errors.Is(err, context.DeadlineExceeded) && (a.State() != StateASPInactive || !errors.Is(err, ErrNotEstablished)) {
						t.Fatalf("unexpected extra DATA: %v", err)
					}
				}
				for index, a := range local {
					backlog, err := a.SCTPSendBacklog()
					if err != nil {
						t.Fatal(err)
					}
					t.Logf("peer %d SCTP send backlog: charged=%d limit=%d", index, backlog.Charged, backlog.Limit)
				}
			})
		}
	}
}
