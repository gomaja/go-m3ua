package m3ua

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

func TestDistributionConsultsSeparatePeerAndTransportInputs(t *testing.T) {
	for _, source := range []string{"peer", "transport"} {
		t.Run(source, func(t *testing.T) {
			l, as, first, firstSent := distributionFixtureConfigured(t, params.TrafficModeLoadshare, func(c *distributionFixtureConfig) {
				c.PreferUncongestedPeers = true
			})
			second, secondSent := addDistributionASP(t, l, StateASPInactive, 1)
			as.setASPState(first, StateASPActive, time.Hour)
			as.setASPState(second, StateASPActive, time.Hour)
			as.mu.Lock()
			congested := as.active[0]
			as.mu.Unlock()
			if source == "peer" {
				congested.muState.Lock()
				congested.state = StateASPActive
				congested.muState.Unlock()
				err := congested.handleSignallingCongestion(messages.NewSignallingCongestion(params.NewNetworkAppearance(0), params.NewRoutingContext(1), params.NewAffectedPointCode(2), nil, params.NewCongestionIndications(3), nil))
				if err != nil {
					t.Fatal(err)
				}
			} else {
				congested.transportSnapshot.Store(&TransportCongestionState{Association: congested.ID(), Epoch: congested.Epoch(), Known: true, Level: 3})
			}
			firstSent.reset()
			secondSent.reset()
			result, err := l.DistributeData(distributionData(1, 0, "prefer healthy"))
			if err != nil || result.Delivered != 1 {
				t.Fatalf("distribution = %+v, %v", result, err)
			}
			sent := firstSent
			if congested == second {
				sent = secondSent
			}
			if sent.dataCount() != 0 {
				t.Fatal("congested member received DATA while a healthy admitted alternative exists")
			}
		})
	}
}

func TestSGPTransitVetoAppliesWithoutPeerPreference(t *testing.T) {
	calls := 0
	l, as, peer, sent := distributionFixtureConfigured(t, params.TrafficModeLoadshare, func(c *distributionFixtureConfig) {
		c.CongestionPolicy = func(SGPCongestionCandidate) bool { calls++; return false }
	})
	as.setASPState(peer, StateASPActive, time.Hour)
	sent.reset()
	result, err := l.DistributeData(distributionData(1, 0, "veto"))
	if calls == 0 || err == nil || result.Delivered != 0 || sent.dataCount() != 0 {
		t.Fatalf("transit veto not enforced: calls=%d result=%+v err=%v DATA=%d", calls, result, err, sent.dataCount())
	}
}

func reportPeerPressure(t *testing.T, a *Association, level *params.Param, dpc uint32) {
	t.Helper()
	// distributionFixture updates AS membership independently of the synthetic
	// association state. Match the established peer before receiving SSNM.
	a.muState.Lock()
	a.state = StateASPActive
	a.muState.Unlock()
	if err := a.handleSignallingCongestion(messages.NewSignallingCongestion(params.NewNetworkAppearance(0), params.NewRoutingContext(1), params.NewAffectedPointCode(dpc), nil, level, nil)); err != nil {
		t.Fatal(err)
	}
}

func TestPeerPreferenceDefaultPreservesSLSAndEqualRanks(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprint(enabled), func(t *testing.T) {
			l, as, first, firstSent := distributionFixtureConfigured(t, params.TrafficModeLoadshare, func(c *distributionFixtureConfig) { c.PreferUncongestedPeers = enabled })
			second, secondSent := addDistributionASP(t, l, StateASPInactive, 1)
			as.setASPState(first, StateASPActive, time.Hour)
			as.setASPState(second, StateASPActive, time.Hour)
			as.mu.Lock()
			initial := append([]*Association(nil), as.active...)
			as.mu.Unlock()
			for _, a := range initial {
				reportPeerPressure(t, a, params.NewCongestionIndications(2), 2)
			}
			for sls := uint8(0); sls < 16; sls++ {
				firstSent.reset()
				secondSent.reset()
				if _, err := l.DistributeData(distributionData(1, sls, "equal ranks")); err != nil {
					t.Fatal(err)
				}
				want := firstSent
				other := secondSent
				if initial[int(sls)%len(initial)] == second {
					want, other = other, want
				}
				if want.dataCount() != 1 || other.dataCount() != 0 {
					t.Fatalf("SLS %d changed within equal rank", sls)
				}
			}
			// Updating the same rank or another destination must preserve affinity.
			reportPeerPressure(t, initial[0], params.NewCongestionIndications(2), 2)
			reportPeerPressure(t, initial[0], params.NewCongestionIndications(3), 99)
			firstSent.reset()
			secondSent.reset()
			if _, err := l.DistributeData(distributionData(1, 0, "same rank")); err != nil {
				t.Fatal(err)
			}
			selected := firstSent
			if initial[0] == second {
				selected = secondSent
			}
			if selected.dataCount() != 1 {
				t.Fatal("same rank invalidated SLS assignment")
			}
			if !enabled {
				reportPeerPressure(t, initial[0], params.NewCongestionIndications(3), 2)
				firstSent.reset()
				secondSent.reset()
				if _, err := l.DistributeData(distributionData(1, 0, "disabled")); err != nil {
					t.Fatal(err)
				}
				if selected.dataCount() != 1 {
					t.Fatal("default preference rerouted DATA")
				}
			}
		})
	}
}

func TestPeerPreferenceUsesHigherPressureAndMatchingDestination(t *testing.T) {
	l, as, first, firstSent := distributionFixtureConfigured(t, params.TrafficModeLoadshare, func(c *distributionFixtureConfig) { c.PreferUncongestedPeers = true })
	second, secondSent := addDistributionASP(t, l, StateASPInactive, 1)
	as.setASPState(first, StateASPActive, time.Hour)
	as.setASPState(second, StateASPActive, time.Hour)
	reportPeerPressure(t, first, params.NewCongestionIndications(1), 2)
	reportPeerPressure(t, second, params.NewCongestionIndications(2), 2)
	first.transportSnapshot.Store(&TransportCongestionState{Association: first.ID(), Epoch: first.Epoch(), Known: true, Level: 3})
	firstSent.reset()
	secondSent.reset()
	if _, err := l.DistributeData(distributionData(1, 0, "maximum")); err != nil {
		t.Fatal(err)
	}
	if firstSent.dataCount() != 0 || secondSent.dataCount() != 1 {
		t.Fatal("peer level hid higher transport pressure")
	}
	first.transportSnapshot.Store(&TransportCongestionState{Association: first.ID(), Epoch: first.Epoch(), Known: true})
	firstSent.reset()
	secondSent.reset()
	if _, err := l.DistributeData(distributionData(1, 0, "peer dominates")); err != nil {
		t.Fatal(err)
	}
	if firstSent.dataCount() != 1 || secondSent.dataCount() != 0 {
		t.Fatal("transport clear hid higher matching peer pressure")
	}
	// An omitted peer level remains unknown and is ranked conservatively.
	reportPeerPressure(t, first, nil, 2)
	firstSent.reset()
	secondSent.reset()
	if _, err := l.DistributeData(distributionData(1, 0, "omitted")); err != nil {
		t.Fatal(err)
	}
	if secondSent.dataCount() != 1 {
		t.Fatal("omitted peer level was treated as measured clear")
	}
	// Explicit SCON(0) makes only this peer contribution clear.
	reportPeerPressure(t, first, params.NewCongestionIndications(0), 2)
	firstSent.reset()
	secondSent.reset()
	if _, err := l.DistributeData(distributionData(1, 0, "clear")); err != nil {
		t.Fatal(err)
	}
	if firstSent.dataCount() != 1 {
		t.Fatal("known peer clear was not selectable")
	}
}

func TestSGPTransitPolicyKeepsOverrideAndBroadcastObligations(t *testing.T) {
	for _, mode := range []uint32{params.TrafficModeOverride, params.TrafficModeBroadcast} {
		t.Run(fmt.Sprint(mode), func(t *testing.T) {
			var refused AssociationID
			l, as, first, firstSent := distributionFixtureConfigured(t, mode, func(c *distributionFixtureConfig) {
				c.PreferUncongestedPeers = true
				c.CongestionPolicy = func(candidate SGPCongestionCandidate) bool { return candidate.Association != refused }
			})
			second, secondSent := addDistributionASP(t, l, StateASPInactive, 1)
			as.setASPState(first, StateASPActive, time.Hour)
			if mode == params.TrafficModeBroadcast {
				as.setASPState(second, StateASPActive, time.Hour)
			}
			refused = first.ID()
			reportPeerPressure(t, first, params.NewCongestionIndications(3), 2)
			firstSent.reset()
			secondSent.reset()
			result, err := l.DistributeData(distributionData(1, 0, "obligations"))
			requireDataWriteError(t, err, DataNotSent, ErrNoMTPRoute)
			want := 0
			if mode == params.TrafficModeBroadcast {
				want = 1
			}
			if result.Delivered != want || firstSent.dataCount() != 0 || secondSent.dataCount() != want {
				t.Fatalf("obligated membership changed: %+v err=%v first=%d second=%d", result, err, firstSent.dataCount(), secondSent.dataCount())
			}
			if mode == params.TrafficModeBroadcast && !strings.Contains(err.Error(), fmt.Sprintf("association %d", first.ID())) {
				t.Fatal("broadcast refusal omitted member identity")
			}
		})
	}
}

func TestSGPTransitPolicyRunsOutsideStateAndDeliveryLocks(t *testing.T) {
	var l *Listener
	var as *applicationServer
	var peer *Association
	var lockFailure atomic.Bool
	var calls atomic.Int32
	listener, server, a, sent := distributionFixtureConfigured(t, params.TrafficModeLoadshare, func(c *distributionFixtureConfig) {
		c.CongestionPolicy = func(candidate SGPCongestionCandidate) bool {
			calls.Add(1)
			if !l.endpoint.mu.TryLock() {
				lockFailure.Store(true)
			} else {
				l.endpoint.mu.Unlock()
			}
			if !as.mu.TryLock() {
				lockFailure.Store(true)
				return false
			} else {
				as.mu.Unlock()
			}
			if !as.deliveryMu.TryLock() {
				lockFailure.Store(true)
			} else {
				as.deliveryMu.Unlock()
			}
			if !peer.muDynamicASKeys.TryLock() {
				lockFailure.Store(true)
			} else {
				peer.muDynamicASKeys.Unlock()
			}
			if !peer.transportMu.TryLock() {
				lockFailure.Store(true)
			} else {
				peer.transportMu.Unlock()
			}
			if _, err := l.endpoint.CongestionInputs(candidate.AS); err != nil {
				t.Error(err)
			}
			if len(candidate.PeerCongestion) > 0 {
				candidate.PeerCongestion[0].Scope.RoutingContexts[0] = 999
				candidate.PeerCongestion[0].Congestion.Level = 0
			}
			return true
		}
	})
	l, as, peer = listener, server, a
	as.setASPState(peer, StateASPActive, time.Hour)
	reportPeerPressure(t, peer, params.NewCongestionIndications(2), 2)
	sent.reset()
	_, writeErr := l.DistributeData(distributionData(1, 0, "outside locks"))
	if lockFailure.Load() || calls.Load() == 0 {
		t.Fatal("policy invoked under a state or delivery lock")
	}
	if writeErr != nil {
		t.Fatal(writeErr)
	}
	snapshot, err := l.endpoint.CongestionInputs(as.key)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Peers[0].PeerCongestion[0].Scope.RoutingContexts[0] != 1 || snapshot.Peers[0].PeerCongestion[0].Congestion.Level != 2 {
		t.Fatal("callback changed retained values")
	}
}

func TestSGPTransitPolicySnapshotPrecedesBusyDeliveryBarrier(t *testing.T) {
	called := make(chan struct{}, 1)
	l, as, peer, _ := distributionFixtureConfigured(t, params.TrafficModeLoadshare, func(c *distributionFixtureConfig) {
		c.CongestionPolicy = func(SGPCongestionCandidate) bool {
			select {
			case called <- struct{}{}:
			default:
			}
			return false
		}
	})
	as.setASPState(peer, StateASPActive, time.Hour)
	as.deliveryMu.Lock()
	done := make(chan error, 1)
	go func() { _, err := l.DistributeData(distributionData(1, 0, "barrier")); done <- err }()
	select {
	case <-called:
	case <-time.After(time.Second):
		as.deliveryMu.Unlock()
		if err := <-done; err != nil {
			t.Logf("send after barrier release: %v", err)
		}
		t.Fatal("policy waited behind delivery barrier")
	}
	as.deliveryMu.Unlock()
	requireDataWriteError(t, <-done, DataNotSent, ErrNoMTPRoute)
}

func TestSGPTransitPolicyReevaluatesOnceThenRefusesStale(t *testing.T) {
	for _, keepChanging := range []bool{false, true} {
		t.Run(fmt.Sprint(keepChanging), func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var enabled atomic.Bool
			l, as, peer, sent := distributionFixtureConfigured(t, params.TrafficModeLoadshare, func(c *distributionFixtureConfig) {
				c.CongestionPolicy = func(SGPCongestionCandidate) bool {
					if enabled.Load() {
						entered <- struct{}{}
						<-release
					}
					return true
				}
			})
			as.setASPState(peer, StateASPActive, time.Hour)
			sent.reset()
			enabled.Store(true)
			done := make(chan error, 1)
			go func() { _, err := l.DistributeData(distributionData(1, 0, "changing")); done <- err }()
			for attempt := 0; attempt < 2; attempt++ {
				select {
				case <-entered:
				case <-time.After(time.Second):
					t.Fatal("policy was not evaluated")
				}
				if attempt == 0 || keepChanging {
					reportPeerPressure(t, peer, params.NewCongestionIndications(1), 2)
				}
				release <- struct{}{}
			}
			select {
			case err := <-done:
				if keepChanging {
					requireDataWriteError(t, err, DataNotSent, ErrCongestionDecisionStale)
					if sent.dataCount() != 0 {
						t.Fatal("stale DATA was sent")
					}
				} else if err != nil || sent.dataCount() != 1 {
					t.Fatalf("single change was not reevaluated: err=%v sent=%d", err, sent.dataCount())
				}
			case <-time.After(time.Second):
				t.Fatal("policy reevaluation was unbounded")
			}
		})
	}
}

func TestSGPTransitPolicyAppliesToTypedAndRawAssociationWrites(t *testing.T) {
	for _, raw := range []bool{false, true} {
		t.Run(fmt.Sprint(raw), func(t *testing.T) {
			l, as, peer, sent := distributionFixtureConfigured(t, params.TrafficModeLoadshare, func(c *distributionFixtureConfig) {
				c.CongestionPolicy = func(SGPCongestionCandidate) bool { return false }
			})
			_ = l
			as.setASPState(peer, StateASPActive, time.Hour)
			peer.muState.Lock()
			peer.state = StateASPActive
			peer.muState.Unlock()
			sent.reset()
			var err error
			if raw {
				_, err = peer.WriteSignal(distributionData(1, 0, "raw veto"))
			} else {
				payload, _ := distributionData(1, 0, "typed veto").ProtocolData.ProtocolData()
				_, err = peer.WriteData(DataRequest{AS: as.key, ProtocolData: *payload})
			}
			requireDataWriteError(t, err, DataNotSent, ErrNoMTPRoute)
			if sent.dataCount() != 0 {
				t.Fatal("association veto sent DATA")
			}
		})
	}
}

func TestSGPTransitPolicyRevalidatesAfterWaitingForSocketGate(t *testing.T) {
	seen := make(chan SGPCongestionCandidate, 4)
	l, as, peer, _ := distributionFixtureConfigured(t, params.TrafficModeLoadshare, func(c *distributionFixtureConfig) {
		c.CongestionPolicy = func(candidate SGPCongestionCandidate) bool { seen <- candidate; return true }
	})
	_ = l
	as.setASPState(peer, StateASPActive, time.Hour)
	peer.muState.Lock()
	peer.state = StateASPActive
	peer.muState.Unlock()
	peer.signalWriter = nil
	var submissions atomic.Int32
	peer.dataWriter = func(frame []byte, _ *sctp.SndInfo) (int, error) { submissions.Add(1); return len(frame), nil }
	peer.sendMu.Lock()
	done := make(chan error, 1)
	go func() {
		payload, _ := distributionData(1, 0, "socket fence").ProtocolData.ProtocolData()
		_, err := peer.WriteData(DataRequest{AS: as.key, ProtocolData: *payload})
		done <- err
	}()
	select {
	case <-seen:
	case <-time.After(time.Second):
		peer.sendMu.Unlock()
		t.Fatal("policy not called before socket gate")
	}
	if !waitFor(func() bool {
		if as.deliveryMu.TryLock() {
			as.deliveryMu.Unlock()
			return false
		}
		return true
	}, time.Second) {
		peer.sendMu.Unlock()
		t.Fatal("DATA never waited on socket gate under delivery barrier")
	}
	// Receive-path retention must progress while the application sender waits.
	reportPeerPressure(t, peer, params.NewCongestionIndications(2), 2)
	peer.sendMu.Unlock()
	select {
	case candidate := <-seen:
		if len(candidate.PeerCongestion) != 1 || candidate.PeerCongestion[0].Congestion.Level != 2 {
			t.Fatal("reevaluation missed receive revision")
		}
	case <-time.After(time.Second):
		t.Fatal("socket gate accepted a stale policy decision")
	}
	if err := <-done; err != nil || submissions.Load() != 1 {
		t.Fatalf("final send=%d err=%v", submissions.Load(), err)
	}
}

func TestSGPTransitPolicyCannotAuthorizeWithdrawnMember(t *testing.T) {
	l, as, first, sent := distributionFixtureConfigured(t, params.TrafficModeLoadshare, func(c *distributionFixtureConfig) {
		c.CongestionPolicy = func(SGPCongestionCandidate) bool { return true }
	})
	second, _ := addDistributionASP(t, l, StateASPInactive, 1)
	as.setASPState(first, StateASPActive, time.Hour)
	as.setASPState(second, StateASPActive, time.Hour)
	as.setASPState(first, StateASPInactive, time.Hour)
	payload, _ := distributionData(1, 0, "withdrawn").ProtocolData.ProtocolData()
	decision := as.prepareSGPCongestion(payload, as.key)
	// Even an allow result with an otherwise current vector cannot authorize
	// the inactive member at the final submission seam.
	decision.members[first] = true
	if !decision.current() {
		t.Fatal("test decision vector is not current")
	}
	requireDataWriteError(t, first.checkSGPCongestionDecision(as.key, 1, decision), DataNotSent, ErrRoutingContextNotActive)
	first.muState.Lock()
	first.state = StateASPActive
	first.muState.Unlock()
	sent.reset()
	_, err := first.WriteData(DataRequest{AS: as.key, ProtocolData: *payload})
	requireDataWriteError(t, err, DataNotSent, ErrRoutingContextNotActive)
	if sent.dataCount() != 0 {
		t.Fatal("policy authorized an inactive AS membership")
	}
}

func TestSGPBroadcastStaleDecisionReportsEveryUnsentMember(t *testing.T) {
	var endpoint *Endpoint
	l, as, first, firstSent := distributionFixtureConfigured(t, params.TrafficModeBroadcast, func(c *distributionFixtureConfig) {
		c.CongestionPolicy = func(SGPCongestionCandidate) bool {
			// Move only the input version, leaving every member/value identical.
			endpoint.ssnm.mu.Lock()
			endpoint.ssnm.revision++
			endpoint.ssnm.mu.Unlock()
			return true
		}
	})
	endpoint = l.endpoint
	second, secondSent := addDistributionASP(t, l, StateASPInactive, 1)
	as.setASPState(first, StateASPActive, time.Hour)
	as.setASPState(second, StateASPActive, time.Hour)
	firstSent.reset()
	secondSent.reset()
	result, err := l.DistributeData(distributionData(1, 0, "stale broadcast"))
	requireDataWriteError(t, err, DataNotSent, ErrCongestionDecisionStale)
	if result.Delivered != 0 || firstSent.dataCount() != 0 || secondSent.dataCount() != 0 {
		t.Fatal("stale broadcast submitted a copy")
	}
	for _, peer := range []*Association{first, second} {
		if !strings.Contains(err.Error(), fmt.Sprintf("association %d:", peer.ID())) {
			t.Fatalf("stale refusal omitted member %d: %v", peer.ID(), err)
		}
	}
}

func TestRecoveryBroadcastStaleDecisionPreservesFirstCopyMarker(t *testing.T) {
	var endpoint *Endpoint
	var changing atomic.Bool
	var calls atomic.Int32
	changing.Store(true)
	l, as, peer, sent := distributionFixtureConfigured(t, params.TrafficModeBroadcast, func(c *distributionFixtureConfig) {
		c.CongestionPolicy = func(SGPCongestionCandidate) bool {
			if changing.Load() {
				endpoint.ssnm.mu.Lock()
				endpoint.ssnm.revision++
				endpoint.ssnm.mu.Unlock()
				calls.Add(1)
			}
			return true
		}
	})
	endpoint = l.endpoint
	as.setASPState(peer, StateASPActive, time.Hour)
	as.setASPState(peer, StateASPInactive, time.Hour)
	if result, err := l.DistributeData(distributionData(1, 0, "first accepted copy")); err != nil || !result.Queued {
		t.Fatalf("queue=%+v %v", result, err)
	}
	sent.reset()
	as.setASPState(peer, StateASPActive, time.Hour)
	if !waitFor(func() bool { return calls.Load() >= 2 }, time.Second) {
		changing.Store(false)
		t.Fatal("stale recovery decision was not exercised")
	}
	changing.Store(false)
	if !waitFor(func() bool {
		messages, bytes := recoveryBudgetUsage(l.as.recoveryBudget)
		return messages == 0 && bytes == 0
	}, time.Second) {
		t.Fatal("recovery did not settle")
	}
	if data := onlyData(t, sent.data()); data.CorrelationID == nil {
		t.Fatal("stale pre-send decision lost the first Broadcast copy's synchronization marker")
	}
}

func TestSGPTransitPolicyCannotAuthorizeUnsupportedTrafficMode(t *testing.T) {
	l, as, peer, sent := distributionFixtureConfigured(t, params.TrafficModeLoadshare, func(c *distributionFixtureConfig) {
		c.CongestionPolicy = func(SGPCongestionCandidate) bool { return true }
	})
	as.setASPState(peer, StateASPActive, time.Hour)
	as.mu.Lock()
	as.trafficMode = 99
	as.rebuildActiveLocked()
	as.mu.Unlock()
	sent.reset()
	_, err := l.DistributeData(distributionData(1, 0, "unsupported mode"))
	if !errors.Is(err, ErrUnsupportedTrafficMode) || sent.dataCount() != 0 {
		t.Fatalf("veto authorized unsupported mode: err=%v copies=%d", err, sent.dataCount())
	}
}
