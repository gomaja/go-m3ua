package m3ua

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

// These fixtures use decoded/frame recorders only; no SCTP socket is opened.
func ipspDistributionFixture(t *testing.T, model string, mode uint32) (*Listener, *Association, *distributionCapture) {
	t.Helper()
	return ipspDistributionFixtureWithCongestion(t, model, mode, CongestionNationalWithoutPriorities)
}

func ipspDistributionFixtureWithCongestion(t *testing.T, model string, mode uint32, method CongestionMethod) (*Listener, *Association, *distributionCapture) {
	t.Helper()
	e, err := NewEndpoint(EndpointConfig{Role: RoleIPSP,
		Congestion:         &CongestionConfig{Default: CongestionProfile{Method: method, MaxLevel: 3}},
		ApplicationServers: &ApplicationServerConfig{RecoveryTimer: time.Hour}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	cfg := NewAssociationConfig()
	cfg.ApplicationServers = buildTestInventory(0, true, mode, []uint32{1})
	cfg.IPSP = &IPSPConfig{ExchangeModel: IPSPExchangeSingle}
	cfg.ASPProcedures = ipspInitiationPolicy(false, false)
	if model != "SE" {
		cfg.IPSP = &IPSPConfig{ExchangeModel: IPSPExchangeDouble, ASPSMExchange: IPSPASPSMExchangeDouble,
			TrafficToPeer:  &IPSPTrafficConfig{ApplicationServers: cfg.ApplicationServers},
			TrafficToLocal: &IPSPTrafficConfig{ApplicationServers: buildTestInventory(9, true, mode, []uint32{9})}}
		cfg.ApplicationServers = nil
		if model == "shared DE" {
			cfg.IPSP.ASPSMExchange = IPSPASPSMExchangeSingle
		}
		if model == "peer DE" {
			cfg.IPSP.TrafficToLocal = nil
		}
	}
	l := newListener(e, NewListenerConfig(cfg))
	a, sent := addIPSPDistributionPeer(t, l, cfg)
	return l, a, sent
}

func addIPSPDistributionPeer(t *testing.T, l *Listener, cfg *AssociationConfig) (*Association, *distributionCapture) {
	t.Helper()
	a, _ := newTestConn(t, StateASPInactive, RoleIPSP)
	a.cfg = snapshotAssociationConfig(cfg)
	a.freezeTrafficModePolicies()
	a.as = l.as
	a.as.register(a.configuredASKeys())
	capture := new(distributionCapture)
	a.signalWriter = capture.write
	if !l.endpoint.trackAssociation(a) {
		t.Fatal("track IPSP")
	}
	return a, capture
}

func activateIPSPDistributionPeer(t *testing.T, a *Association, mode uint32, rc uint32) {
	t.Helper()
	if err := a.handleAspActive(messages.NewAspActive(params.NewTrafficModeType(mode), params.NewRoutingContext(rc), nil)); err != nil {
		t.Fatal(err)
	}
}

func ipspDistributors(l *Listener, a *Association) map[string]func(*messages.Data) (TrafficDistribution, error) {
	return map[string]func(*messages.Data) (TrafficDistribution, error){"Listener": l.DistributeData, "Association": a.DistributeData}
}

func TestIPSPDistributionTrafficModes(t *testing.T) {
	for _, model := range []string{"SE", "DE", "shared DE", "peer DE"} {
		for _, mode := range []uint32{params.TrafficModeOverride, params.TrafficModeLoadshare, params.TrafficModeBroadcast} {
			t.Run(fmt.Sprintf("%s/%d", model, mode), func(t *testing.T) {
				l, first, firstSent := ipspDistributionFixture(t, model, mode)
				second, secondSent := addIPSPDistributionPeer(t, l, first.cfg)
				third, thirdSent := addIPSPDistributionPeer(t, l, first.cfg)
				for _, a := range []*Association{first, second, third} {
					activateIPSPDistributionPeer(t, a, mode, 1)
				}
				as := l.as.get(*staticASKey(0, 1))
				ordered := as.activeASPs()
				captures := map[*Association]*distributionCapture{first: firstSent, second: secondSent, third: thirdSent}
				for api, distribute := range ipspDistributors(l, first) {
					t.Run(api, func(t *testing.T) {
						for _, sls := range []uint8{5, 0, 2, 5, 1, 0, 4, 3, 4} {
							for _, capture := range captures {
								capture.reset()
							}
							result, err := distribute(distributionData(1, sls, "selected"))
							wantDeliveries := 1
							if mode == params.TrafficModeBroadcast {
								wantDeliveries = 3
							}
							if err != nil || result.Delivered != wantDeliveries || result.Queued {
								t.Fatalf("distribution = %+v, %v", result, err)
							}
							for a, capture := range captures {
								want := 0
								if mode == params.TrafficModeBroadcast || mode == params.TrafficModeOverride && a == third || mode == params.TrafficModeLoadshare && a == ordered[int(sls)%len(ordered)] {
									want = 1
								}
								if capture.dataCount() != want {
									t.Fatalf("SLS %d peer %v got %d DATA, want %d", sls, a.ID(), capture.dataCount(), want)
								}
							}
						}
					})
				}
			})
		}
	}
}

func TestIPSPDistributionSingleExchangeInitiatorOwnsPeerState(t *testing.T) {
	for _, mode := range []uint32{params.TrafficModeOverride, params.TrafficModeLoadshare, params.TrafficModeBroadcast} {
		for _, policy := range [][2]bool{{false, false}, {false, true}, {true, false}, {true, true}} {
			t.Run(fmt.Sprintf("%d/%v", mode, policy), func(t *testing.T) {
				l, a, capture := ipspDistributionFixture(t, "SE", mode)
				a.cfg.ASPProcedures = ipspInitiationPolicy(policy[0], policy[1])
				if err := a.handleAspActiveAck(messages.NewAspActiveAck(params.NewTrafficModeType(mode), params.NewRoutingContext(1), nil)); err != nil {
					t.Fatal(err)
				}
				for api, distribute := range ipspDistributors(l, a) {
					capture.reset()
					result, err := distribute(distributionData(1, 1, api))
					if err != nil || result.Delivered != 1 || capture.dataCount() != 1 {
						t.Fatalf("SE initiating %s = %+v, %v, DATA=%d", api, result, err, capture.dataCount())
					}
				}
				if err := a.handleAspInactiveAck(messages.NewAspInactiveAck(params.NewRoutingContext(1), nil)); err != nil {
					t.Fatal(err)
				}
				capture.reset()
				for api, distribute := range ipspDistributors(l, a) {
					if result, err := distribute(distributionData(1, 1, api)); err != nil || !result.Queued || result.Delivered != 0 || capture.dataCount() != 0 {
						t.Fatalf("SE withdrawal %s = %+v, %v", api, result, err)
					}
				}
				activateIPSPDistributionPeer(t, a, mode, 1)
				if !waitFor(func() bool { return capture.dataCount() == 2 }, time.Second) {
					t.Fatal("SE requester queue was not flushed after peer reactivation")
				}
			})
		}
	}
}

func TestIPSPDistributionRequiresPeerApplicationServer(t *testing.T) {
	for _, model := range []string{"DE", "shared DE"} {
		t.Run(model, func(t *testing.T) {
			e, err := NewEndpoint(nationalCongestionEndpointConfig(EndpointConfig{Role: RoleIPSP}))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = e.Close() })
			cfg := newDoubleExchangeAssociationConfigForTest()
			cfg.IPSP.TrafficToPeer = nil
			cfg.ASPProcedures = ipspInitiationPolicy(false, false)
			if model == "shared DE" {
				cfg.IPSP.ASPSMExchange = IPSPASPSMExchangeSingle
			}
			l := newListener(e, NewListenerConfig(cfg))
			a, capture := addIPSPDistributionPeer(t, l, cfg)
			for api, distribute := range ipspDistributors(l, a) {
				if result, err := distribute(distributionData(1, 1, api)); !errors.Is(err, ErrNoConfiguredAS) || result != (TrafficDistribution{}) {
					t.Fatalf("%s local-only = %+v, %v; want ErrNoConfiguredAS", api, result, err)
				}
			}
			if capture.dataCount() != 0 {
				t.Fatal("local-only IPSP sent DATA")
			}
			// The handle uses the Endpoint registry, including a sibling's remote AS.
			peerCfg := snapshotAssociationConfig(cfg)
			peerCfg.IPSP.TrafficToPeer = &IPSPTrafficConfig{ApplicationServers: buildTestInventory(0, true, params.TrafficModeLoadshare, []uint32{1})}
			peer, peerSent := addIPSPDistributionPeer(t, l, peerCfg)
			activateIPSPDistributionPeer(t, peer, params.TrafficModeLoadshare, 1)
			for api, distribute := range ipspDistributors(l, a) {
				peerSent.reset()
				if result, err := distribute(distributionData(1, 1, api)); err != nil || result.Delivered != 1 || peerSent.dataCount() != 1 || capture.dataCount() != 0 {
					t.Fatalf("%s sibling AS = %+v, %v", api, result, err)
				}
			}
		})
	}
}

func TestIPSPDistributionRecoveryAndExpiry(t *testing.T) {
	for _, model := range []string{"SE", "DE", "shared DE"} {
		for _, expire := range []bool{false, true} {
			for _, api := range []string{"Listener", "Association"} {
				t.Run(fmt.Sprintf("%s/expire=%t/%s", model, expire, api), func(t *testing.T) {
					l, a, capture := ipspDistributionFixture(t, model, params.TrafficModeLoadshare)
					activateIPSPDistributionPeer(t, a, params.TrafficModeLoadshare, 1)
					if err := a.handleAspInactive(messages.NewAspInactive(params.NewRoutingContext(1), nil)); err != nil {
						t.Fatal(err)
					}
					as := l.as.get(*staticASKey(0, 1))
					if as.State() != ASPending {
						t.Fatal("last inactive peer did not enter AS-PENDING")
					}
					as.mu.Lock()
					armed, generation := as.recovery != nil, as.recoveryGen
					as.mu.Unlock()
					if !armed {
						t.Fatal("T(r) was not started")
					}
					capture.reset()
					data := distributionData(1, 1, "retained")
					result, err := ipspDistributors(l, a)[api](data)
					if err != nil || !result.Queued || result.Delivered != 0 || capture.dataCount() != 0 {
						t.Fatalf("pending = %+v, %v, DATA=%d", result, err, capture.dataCount())
					}
					data.ProtocolData.Data[len(data.ProtocolData.Data)-1] ^= 1
					if expire {
						as.mu.Lock()
						as.recovery.Stop()
						as.mu.Unlock()
						as.recoveryExpired(generation)
						if as.State() != ASInactive {
							t.Fatal("expired AS with inactive peer did not become AS-INACTIVE")
						}
					}
					activateIPSPDistributionPeer(t, a, params.TrafficModeLoadshare, 1)
					// Wait for the drain to finish even for an empty queue.
					if !waitFor(func() bool { as.mu.Lock(); defer as.mu.Unlock(); return !as.draining }, time.Second) {
						t.Fatal("recovery did not finish")
					}
					want := 1
					if expire {
						want = 0
					}
					if capture.dataCount() != want {
						t.Fatalf("recovered DATA=%d, want %d", capture.dataCount(), want)
					}
					if !expire && !bytes.Equal(dataPayload(t, onlyData(t, capture.data())), []byte("retained")) {
						t.Fatal("queued message retained caller memory")
					}
					if messages, bytes := recoveryBudgetUsage(l.as.recoveryBudget); messages != 0 || bytes != 0 {
						t.Fatalf("recovery retained budget %d/%d", messages, bytes)
					}
				})
			}
		}
	}
}

func TestIPSPDistributionBroadcastSynchronization(t *testing.T) {
	for _, api := range []string{"Listener", "Association"} {
		t.Run(api, func(t *testing.T) {
			l, first, firstSent := ipspDistributionFixture(t, "DE", params.TrafficModeBroadcast)
			activateIPSPDistributionPeer(t, first, params.TrafficModeBroadcast, 1)
			distribute := ipspDistributors(l, first)[api]
			if _, err := distribute(distributionData(1, 1, "initial")); err != nil {
				t.Fatal(err)
			}
			initial := onlyData(t, firstSent.data()).CorrelationID
			if initial == nil {
				t.Fatal("initial Broadcast DATA has no marker")
			}
			firstSent.reset()
			if _, err := distribute(distributionData(1, 1, "ordinary")); err != nil {
				t.Fatal(err)
			}
			if onlyData(t, firstSent.data()).CorrelationID != nil {
				t.Fatal("ordinary Broadcast DATA unexpectedly tagged")
			}
			second, secondSent := addIPSPDistributionPeer(t, l, first.cfg)
			activateIPSPDistributionPeer(t, second, params.TrafficModeBroadcast, 1)
			firstSent.reset()
			if _, err := distribute(distributionData(1, 1, "joined")); err != nil {
				t.Fatal(err)
			}
			one, two := onlyData(t, firstSent.data()), onlyData(t, secondSent.data())
			if one.CorrelationID == nil || two.CorrelationID == nil || one.CorrelationID.CorrelationID() != two.CorrelationID.CorrelationID() || one.CorrelationID.CorrelationID() == initial.CorrelationID() {
				t.Fatal("join did not establish one fresh marker for every Broadcast copy")
			}
		})
	}
}

func TestIPSPDistributionSeparatesEndpointNetworkAndDirection(t *testing.T) {
	l, first, firstSent := ipspDistributionFixture(t, "DE", params.TrafficModeLoadshare)
	activateIPSPDistributionPeer(t, first, params.TrafficModeLoadshare, 1)
	peers := []*Association{first}
	captures := []*distributionCapture{firstSent}
	for _, key := range []ASKey{*staticASKey(7, 1), *staticASKey(0, 2)} {
		cfg := snapshotAssociationConfig(first.cfg)
		cfg.IPSP.TrafficToPeer.ApplicationServers = []ASConfig{{ASKey: key, TrafficMode: params.TrafficModeLoadshare}}
		a, sent := addIPSPDistributionPeer(t, l, cfg)
		activateIPSPDistributionPeer(t, a, params.TrafficModeLoadshare, key.RoutingContext)
		peers, captures = append(peers, a), append(captures, sent)
	}
	other, sibling, otherSent := ipspDistributionFixture(t, "DE", params.TrafficModeLoadshare)
	activateIPSPDistributionPeer(t, sibling, params.TrafficModeLoadshare, 1)
	_ = other
	for api, distribute := range ipspDistributors(l, first) {
		for i, key := range []ASKey{*staticASKey(0, 1), *staticASKey(7, 1), *staticASKey(0, 2)} {
			for _, sent := range captures {
				sent.reset()
			}
			data := distributionData(key.RoutingContext, 0, api)
			data.NetworkAppearance = params.NewNetworkAppearance(key.NetworkAppearance)
			if result, err := distribute(data); err != nil || result.Delivered != 1 {
				t.Fatalf("%s scope %+v = %+v, %v", api, key, result, err)
			}
			for j, sent := range captures {
				want := 0
				if i == j {
					want = 1
				}
				if sent.dataCount() != want {
					t.Fatalf("scope %d delivered %d to peer %d, want %d", i, sent.dataCount(), j, want)
				}
			}
		}
		data := distributionData(1, 0, "ambiguous NA")
		data.NetworkAppearance = nil
		if _, err := distribute(data); !errors.Is(err, ErrInvalidNetworkAppearance) {
			t.Fatalf("ambiguous NA = %v", err)
		}
		data = distributionData(9, 0, "local direction")
		data.NetworkAppearance = params.NewNetworkAppearance(9)
		if _, err := distribute(data); !errors.Is(err, ErrInvalidRoutingContext) {
			t.Fatalf("local-directed AS selected for outbound DATA: %v", err)
		}
	}
	if otherSent.dataCount() != 0 {
		t.Fatal("distribution crossed Endpoint boundary")
	}
	// A local grant cannot make a configured peer-directed AS active.
	inactive := peers[2].as.get(*staticASKey(0, 2))
	inactive.setASPState(peers[2], StateASPInactive, time.Hour)
	inactive.mu.Lock()
	generation := inactive.recoveryGen
	inactive.recovery.Stop()
	inactive.mu.Unlock()
	inactive.recoveryExpired(generation)
	peers[2].setIPSPState(IPSPState{TrafficToLocal: StateASPActive, TrafficToPeer: StateASPInactive})
	for api, distribute := range ipspDistributors(l, first) {
		if _, err := distribute(distributionData(2, 0, api)); !errors.Is(err, ErrNoActiveASP) {
			t.Fatalf("local activation enabled peer AS: %v", err)
		}
	}
}

func TestIPSPDistributionRefusesManagementData(t *testing.T) {
	for _, model := range []string{"SGP", "SE", "DE", "shared DE", "peer DE"} {
		for _, mode := range []uint32{params.TrafficModeOverride, params.TrafficModeLoadshare, params.TrafficModeBroadcast} {
			for _, state := range []string{"active", "pending", "inactive"} {
				for _, frames := range []bool{false, true} {
					for _, api := range []string{"Listener", "Association"} {
						t.Run(fmt.Sprintf("%s/%d/%s/frames=%t/%s", model, mode, state, frames, api), func(t *testing.T) {
							var l *Listener
							var a *Association
							var capture *distributionCapture
							if model == "SGP" {
								l, _, a, capture = distributionFixture(t, mode)
							} else {
								l, a, capture = ipspDistributionFixture(t, model, mode)
							}
							as := l.as.get(*staticASKey(0, 1))
							as.setASPState(a, StateASPInactive, time.Hour)
							activate := func() {
								if model == "SGP" {
									as.setASPState(a, StateASPActive, time.Hour)
								} else {
									activateIPSPDistributionPeer(t, a, mode, 1)
								}
							}
							if state != "inactive" {
								activate()
							}
							if state == "pending" {
								if model == "SGP" {
									as.setASPState(a, StateASPInactive, time.Hour)
								} else if err := a.handleAspInactive(messages.NewAspInactive(params.NewRoutingContext(1), nil)); err != nil {
									t.Fatal(err)
								}
							}
							if frames {
								a.signalWriter = nil
								controls := newFullSendBuffer()
								controls.firstErr = nil
								a.controlTransport = controls
								a.dataWriter = func(frame []byte, _ *sctp.SndInfo) (int, error) {
									m, err := messages.Parse(frame)
									if err != nil {
										return 0, err
									}
									return capture.write(m)
								}
							}
							capture.reset()
							distribute := ipspDistributors(l, a)[api]
							management := distributionData(1, 0, "management")
							management.ProtocolData = params.NewProtocolData(1, 2, 0, 0, 0, 0, []byte("management"))
							result, err := distribute(management)
							requireDataWriteError(t, err, DataNotSent, ErrMTPManagementData)
							if result != (TrafficDistribution{}) || capture.dataCount() != 0 {
								t.Fatalf("refused management = %+v, DATA=%d", result, capture.dataCount())
							}
							as.mu.Lock()
							queued, queuedBytes := len(as.recoveryQueue), as.recoveryQueueBytes
							as.mu.Unlock()
							if queued != 0 || queuedBytes != 0 {
								t.Fatalf("refused management retained queue %d/%d", queued, queuedBytes)
							}
							if messages, bytes := recoveryBudgetUsage(l.as.recoveryBudget); messages != 0 || bytes != 0 {
								t.Fatalf("refused management charged recovery budget %d/%d", messages, bytes)
							}
							// The SI-0 refusal must not suppress MTP3-User DATA.
							result, err = distribute(distributionData(1, 0, "user"))
							switch state {
							case "active":
								if err != nil || result.Delivered != 1 || result.Queued || capture.dataCount() != 1 {
									t.Fatalf("active user DATA = %+v, %v, DATA=%d", result, err, capture.dataCount())
								}
							case "pending":
								if err != nil || !result.Queued || result.Delivered != 0 || capture.dataCount() != 0 {
									t.Fatalf("pending user DATA = %+v, %v, DATA=%d", result, err, capture.dataCount())
								}
								activate()
								if !waitFor(func() bool { as.mu.Lock(); defer as.mu.Unlock(); return !as.draining }, time.Second) {
									t.Fatal("user DATA recovery did not finish")
								}
								if capture.dataCount() != 1 {
									t.Fatalf("recovered user DATA=%d, want 1", capture.dataCount())
								}
							case "inactive":
								if !errors.Is(err, ErrNoActiveASP) || result != (TrafficDistribution{}) || capture.dataCount() != 0 {
									t.Fatalf("inactive user DATA = %+v, %v, DATA=%d", result, err, capture.dataCount())
								}
								activate()
								if result, err = distribute(distributionData(1, 0, "user")); err != nil || result.Delivered != 1 || result.Queued || capture.dataCount() != 1 {
									t.Fatalf("activated user DATA = %+v, %v, DATA=%d", result, err, capture.dataCount())
								}
							}
							if !bytes.Equal(dataPayload(t, onlyData(t, capture.data())), []byte("user")) {
								t.Fatal("sent management instead of user DATA")
							}
						})
					}
				}
			}
		}
	}
}

func TestIPSPManagedRawDataRefusesManagementData(t *testing.T) {
	for _, model := range []string{"SE", "DE", "shared DE", "peer DE"} {
		for _, frames := range []bool{false, true} {
			for _, managed := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/frames=%t/managed=%t", model, frames, managed), func(t *testing.T) {
					_, a, capture := ipspDistributionFixture(t, model, params.TrafficModeLoadshare)
					activateIPSPDistributionPeer(t, a, params.TrafficModeLoadshare, 1)
					if frames {
						a.signalWriter = nil
						controls := newFullSendBuffer()
						controls.firstErr = nil
						a.controlTransport = controls
						a.dataWriter = func(frame []byte, _ *sctp.SndInfo) (int, error) {
							m, err := messages.Parse(frame)
							if err != nil {
								return 0, err
							}
							return capture.write(m)
						}
					}
					write := a.WriteSignal
					if managed {
						// Exercise the trusted submission boundary separately from the
						// distributor's earlier refusal, including recovery drains.
						write = func(data messages.M3UA) (int, error) {
							frame, err := data.MarshalBinary()
							if err != nil {
								return 0, err
							}
							return a.writeRawData(data, frame, len(frame), false, staticASKey(0, 1))
						}
					}
					capture.reset()
					management := distributionData(1, 0, "management")
					management.ProtocolData = params.NewProtocolData(1, 2, 0, 0, 0, 0, []byte("management"))
					written, err := write(management)
					requireDataWriteError(t, err, DataNotSent, ErrMTPManagementData)
					if written != 0 || capture.dataCount() != 0 {
						t.Fatalf("refused raw management wrote %d bytes, DATA=%d", written, capture.dataCount())
					}
					if written, err = write(distributionData(1, 0, "user")); err != nil || written == 0 || capture.dataCount() != 1 {
						t.Fatalf("raw user DATA wrote %d bytes, error=%v, DATA=%d", written, err, capture.dataCount())
					}
				})
			}
		}
	}
}

func TestIPSPDistributionKeepsCongestionAdmission(t *testing.T) {
	for _, frames := range []bool{false, true} {
		l, a, capture := ipspDistributionFixtureWithCongestion(t, "DE", params.TrafficModeLoadshare, CongestionNationalWithPriorities)
		activateIPSPDistributionPeer(t, a, params.TrafficModeLoadshare, 1)
		if err := a.handleSignallingCongestion(messages.NewSignallingCongestion(params.NewNetworkAppearance(0), params.NewRoutingContext(1), params.NewAffectedPointCode(2), nil, params.NewCongestionIndications(2), nil)); err != nil {
			t.Fatal(err)
		}
		if frames {
			a.signalWriter = nil
			a.dataWriter = func(frame []byte, _ *sctp.SndInfo) (int, error) {
				m, err := messages.Parse(frame)
				if err != nil {
					return 0, err
				}
				return capture.write(m)
			}
		}
		for api, distribute := range ipspDistributors(l, a) {
			capture.reset()
			if _, err := distribute(distributionData(1, 0, "low priority")); !errors.Is(err, ErrNoMTPRoute) || capture.dataCount() != 0 {
				t.Fatalf("%s peer SCON gate = %v, DATA=%d", api, err, capture.dataCount())
			}
			data := distributionData(1, 0, "permitted priority")
			data.ProtocolData = params.NewProtocolData(1, 2, params.ServiceIndSCCP, 0, 3, 0, []byte("permitted"))
			if result, err := distribute(data); err != nil || result.Delivered != 1 || capture.dataCount() != 1 {
				t.Fatalf("%s priority exception = %+v, %v", api, result, err)
			}
			a.cfg.CongestionAdmission = &CongestionAdmissionConfig{}
			now := admissionFrozenClock(a)
			admissionCachedSample(a, now, 90, 100)
			capture.reset()
			if _, err := distribute(data); !errors.Is(err, ErrDataCongestionRefused) || capture.dataCount() != 0 {
				t.Fatalf("%s cached DATA gate = %v, DATA=%d", api, err, capture.dataCount())
			}
			admissionCachedSample(a, now, 89, 100)
			if result, err := distribute(data); err != nil || result.Delivered != 1 || capture.dataCount() != 1 {
				t.Fatalf("%s fresh sample = %+v, %v", api, result, err)
			}
			a.cfg.CongestionAdmission = nil
		}
	}
}

func TestIPSPDistributionLeavesDirectDataUnchanged(t *testing.T) {
	for _, model := range []string{"SE", "DE", "shared DE"} {
		l, a, capture := ipspDistributionFixture(t, model, params.TrafficModeLoadshare)
		activateIPSPDistributionPeer(t, a, params.TrafficModeLoadshare, 1)
		_, other := addIPSPDistributionPeer(t, l, a.cfg)
		capture.reset()
		if _, err := a.WriteSignal(distributionData(1, 1, "direct")); err != nil || capture.dataCount() != 1 || other.dataCount() != 0 {
			t.Fatalf("direct DATA %s changed: %v", model, err)
		}
		if err := a.handleAspInactive(messages.NewAspInactive(params.NewRoutingContext(1), nil)); err != nil {
			t.Fatal(err)
		}
		capture.reset()
		if _, err := a.WriteSignal(distributionData(1, 1, "inactive direct")); err == nil || capture.dataCount() != 0 {
			t.Fatal("inactive direct DATA was sent or queued")
		}
		as := l.as.get(*staticASKey(0, 1))
		as.mu.Lock()
		queued := len(as.recoveryQueue)
		as.mu.Unlock()
		if queued != 0 {
			t.Fatal("direct DATA entered managed recovery queue")
		}
	}
}

func TestIPSPDistributionHandleErrors(t *testing.T) {
	var nilListener *Listener
	var nilAssociation *Association
	if _, err := nilListener.DistributeData(nil); err == nil {
		t.Fatal("nil Listener accepted distribution")
	}
	if _, err := nilAssociation.DistributeData(nil); err == nil {
		t.Fatal("nil Association accepted distribution")
	}
	l, a, capture := ipspDistributionFixture(t, "SE", params.TrafficModeLoadshare)
	registry := l.as
	l.as, a.as = nil, nil
	for api, distribute := range ipspDistributors(l, a) {
		if _, err := distribute(distributionData(1, 0, api)); !errors.Is(err, ErrNoConfiguredAS) {
			t.Fatalf("%s missing registry = %v", api, err)
		}
	}
	l.as, a.as = registry, registry
	for api, distribute := range ipspDistributors(l, a) {
		if _, err := distribute(distributionData(1, 0, api)); !errors.Is(err, ErrNoActiveASP) {
			t.Fatalf("%s configured inactive AS = %v", api, err)
		}
	}
	l.closed = true
	if _, err := l.DistributeData(distributionData(1, 0, "closed")); !errors.Is(err, ErrAssociationClosed) {
		t.Fatalf("closed Listener = %v", err)
	}
	if err := a.closeWith(ErrAssociationClosed); err != nil {
		t.Fatal(err)
	}
	if _, err := a.DistributeData(distributionData(1, 0, "closed")); !errors.Is(err, ErrAssociationClosed) {
		t.Fatalf("closed Association = %v", err)
	}
	if capture.dataCount() != 0 {
		t.Fatal("invalid handle submitted DATA")
	}
}

func TestIPSPDistributionRoleBoundary(t *testing.T) {
	for _, role := range []Role{RoleASP, RoleSGP, RoleIPSP} {
		var l *Listener
		var a *Association
		var capture *distributionCapture
		switch role {
		case RoleSGP:
			var as *applicationServer
			l, as, a, capture = distributionFixture(t, params.TrafficModeLoadshare)
			as.setASPState(a, StateASPActive, time.Hour)
		case RoleIPSP:
			l, a, capture = ipspDistributionFixture(t, "SE", params.TrafficModeLoadshare)
			activateIPSPDistributionPeer(t, a, params.TrafficModeLoadshare, 1)
		case RoleASP:
			e, err := NewEndpoint(nationalCongestionEndpointConfig(EndpointConfig{Role: RoleASP}))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = e.Close() })
			a, _ = newTestConn(t, StateASPActive, RoleASP)
			capture = new(distributionCapture)
			a.signalWriter = capture.write
			l = newListener(e, NewListenerConfig(a.cfg))
		}
		for api, distribute := range ipspDistributors(l, a) {
			capture.reset()
			result, err := distribute(distributionData(1, 0, api))
			if role == RoleASP {
				if !errors.Is(err, ErrUnsupportedRole) || capture.dataCount() != 0 {
					t.Fatalf("ASP %s distribution = %+v, %v", api, result, err)
				}
			} else if err != nil || result.Delivered != 1 || capture.dataCount() != 1 {
				t.Fatalf("%s %s distribution = %+v, %v", role, api, result, err)
			}
		}
	}
}

func TestIPSPDistributionRecoveryScopeAndBudget(t *testing.T) {
	l, first, firstSent := ipspDistributionFixture(t, "DE", params.TrafficModeLoadshare)
	cfg := snapshotAssociationConfig(first.cfg)
	cfg.IPSP.TrafficToPeer.ApplicationServers[0].ASKey = *staticASKey(7, 1)
	second, secondSent := addIPSPDistributionPeer(t, l, cfg)
	// IPSP defaults share one Endpoint-wide budget, just as SGP queues do.
	l.as.recoveryBudget.messageLimit = 1
	for _, a := range []*Association{first, second} {
		activateIPSPDistributionPeer(t, a, params.TrafficModeLoadshare, 1)
		if err := a.handleAspInactive(messages.NewAspInactive(params.NewRoutingContext(1), nil)); err != nil {
			t.Fatal(err)
		}
	}
	firstSent.reset()
	secondSent.reset()
	if result, err := l.DistributeData(distributionData(1, 0, "first network")); err != nil || !result.Queued || result.Delivered != 0 {
		t.Fatalf("first network = %+v, %v", result, err)
	}
	data := distributionData(1, 0, "second network")
	data.NetworkAppearance = params.NewNetworkAppearance(7)
	if _, err := second.DistributeData(data); !errors.Is(err, ErrRecoveryQueueFull) {
		t.Fatalf("second network did not share aggregate budget: %v", err)
	}
	as := l.as.get(*staticASKey(0, 1))
	as.mu.Lock()
	gen := as.recoveryGen
	as.recovery.Stop()
	as.mu.Unlock()
	as.recoveryExpired(gen)
	if result, err := second.DistributeData(data); err != nil || !result.Queued || result.Delivered != 0 {
		t.Fatalf("second network after first expiry = %+v, %v", result, err)
	}
	activateIPSPDistributionPeer(t, first, params.TrafficModeLoadshare, 1)
	if firstSent.dataCount() != 0 || secondSent.dataCount() != 0 {
		t.Fatal("reactivating first network flushed second network's queue")
	}
	activateIPSPDistributionPeer(t, second, params.TrafficModeLoadshare, 1)
	if !waitFor(func() bool { return secondSent.dataCount() == 1 }, time.Second) {
		t.Fatal("second network recovery was not flushed")
	}
	if firstSent.dataCount() != 0 || !bytes.Equal(dataPayload(t, onlyData(t, secondSent.data())), []byte("second network")) {
		t.Fatal("network-specific recovery leaked or changed DATA")
	}
}

func TestIPSPDistributionTimerExpiry(t *testing.T) {
	l, a, capture := ipspDistributionFixture(t, "SE", params.TrafficModeLoadshare)
	l.as.recoveryTimer = 200 * time.Millisecond
	activateIPSPDistributionPeer(t, a, params.TrafficModeLoadshare, 1)
	if err := a.handleAspInactive(messages.NewAspInactive(params.NewRoutingContext(1), nil)); err != nil {
		t.Fatal(err)
	}
	if result, err := a.DistributeData(distributionData(1, 0, "expiry")); err != nil || !result.Queued {
		t.Fatalf("pending before timer = %+v, %v", result, err)
	}
	as := l.as.get(*staticASKey(0, 1))
	if !waitFor(func() bool {
		messages, _ := recoveryBudgetUsage(l.as.recoveryBudget)
		return as.State() == ASInactive && messages == 0
	}, time.Second) {
		t.Fatal("T(r) did not expire")
	}
	activateIPSPDistributionPeer(t, a, params.TrafficModeLoadshare, 1)
	if capture.dataCount() != 0 {
		t.Fatal("expired timer retained DATA for reactivation")
	}
	if messages, bytes := recoveryBudgetUsage(l.as.recoveryBudget); messages != 0 || bytes != 0 {
		t.Fatalf("timer retained recovery budget %d/%d", messages, bytes)
	}
}

func TestIPSPDistributionPeerDownExpiry(t *testing.T) {
	for _, model := range []string{"SE", "DE", "shared DE"} {
		t.Run(model, func(t *testing.T) {
			l, a, capture := ipspDistributionFixture(t, model, params.TrafficModeLoadshare)
			activateIPSPDistributionPeer(t, a, params.TrafficModeLoadshare, 1)
			a.stateEntered, a.appliedState = true, StateASPActive
			a.handleReceivedSignals(context.Background(), messages.NewAspDown(nil), nil)
			applyIPSPDownPublicationForTest(t, a, StateASPDown)
			as := l.as.get(*staticASKey(0, 1))
			if as.State() != ASPending {
				t.Fatal("last peer Down did not enter AS-PENDING")
			}
			capture.reset()
			for api, distribute := range ipspDistributors(l, a) {
				if result, err := distribute(distributionData(1, 0, api)); err != nil || !result.Queued || result.Delivered != 0 {
					t.Fatalf("peer Down %s = %+v, %v", api, result, err)
				}
			}
			as.mu.Lock()
			generation := as.recoveryGen
			as.recovery.Stop()
			as.mu.Unlock()
			as.recoveryExpired(generation)
			as.mu.Lock()
			state, retained := as.state, len(as.recoveryQueue)
			as.mu.Unlock()
			if state != ASDown || retained != 0 || capture.dataCount() != 0 {
				t.Fatalf("all-DOWN expiry: state=%v retained=%d DATA=%d", state, retained, capture.dataCount())
			}
			if messages, bytes := recoveryBudgetUsage(l.as.recoveryBudget); messages != 0 || bytes != 0 {
				t.Fatalf("all-DOWN expiry retained budget %d/%d", messages, bytes)
			}
		})
	}
}
