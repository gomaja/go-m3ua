// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages/params"
)

func TestCongestionInputsOwnsCopiesAndDistinguishesUnknownClearAndInactive(t *testing.T) {
	l, as, first, _ := distributionFixture(t, params.TrafficModeLoadshare)
	second, _ := addDistributionASP(t, l, StateASPInactive, 1)
	as.setASPState(second, StateASPInactive, time.Hour)
	as.setASPState(first, StateASPActive, time.Hour)
	reportPeerPressure(t, first, params.NewCongestionIndications(0), 2)
	first.transportSnapshot.Store(&TransportCongestionState{Association: first.ID(), Epoch: first.Epoch(), Known: true, Level: 3, Generation: 4, Samples: 5, SampledAt: time.Now()})
	snapshot, err := l.endpoint.CongestionInputs(as.key)
	if err != nil {
		t.Fatal(err)
	}
	if !snapshot.Consistent || snapshot.Revision != snapshot.VersionsBefore.Revision || !sameCongestionVersions(snapshot.VersionsBefore, snapshot.VersionsAfter) {
		t.Fatalf("unstable quiescent snapshot: %+v", snapshot)
	}
	if len(snapshot.Peers) != 2 {
		t.Fatalf("membership=%+v", snapshot.Peers)
	}
	for i := range snapshot.Peers {
		input := &snapshot.Peers[i]
		if input.Association == first.ID() {
			if !input.Active || input.Epoch != first.Epoch() || len(input.PeerCongestion) != 1 || !input.PeerCongestion[0].Congestion.LevelSet || input.PeerCongestion[0].Congestion.Level != 0 || input.PeerCongestion[0].Congestion.Congested || input.Transport.Level != 3 {
				t.Fatalf("lost independent clear/transport: %+v", input)
			}
			input.PeerCongestion[0].Scope.RoutingContexts[0] = 999
			input.PeerCongestion[0].Congestion.Level = 3
		} else if input.Association == second.ID() {
			if input.Active || len(input.PeerCongestion) != 0 || input.Transport.Known {
				t.Fatalf("unknown/inactive peer became clear or active: %+v", input)
			}
		}
	}
	snapshot.VersionsBefore.Peers[0].Epoch = 999
	again, err := l.endpoint.CongestionInputs(as.key)
	if err != nil {
		t.Fatal(err)
	}
	if again.VersionsBefore.Peers[0].Epoch == 999 {
		t.Fatal("version vector aliases caller")
	}
	for _, peer := range again.Peers {
		if peer.Association == first.ID() && (peer.PeerCongestion[0].Scope.RoutingContexts[0] != 1 || peer.PeerCongestion[0].Congestion.Level != 0) {
			t.Fatal("input snapshot aliases retained report")
		}
	}
}

func TestCongestionInputsRequiresRegisteredASAndClosedEndpoint(t *testing.T) {
	l, as, peer, _ := distributionFixture(t, params.TrafficModeLoadshare)
	as.setASPState(peer, StateASPActive, time.Hour)
	missing := as.key
	missing.RoutingContext = 999
	if _, err := l.endpoint.CongestionInputs(missing); !errors.Is(err, ErrNoConfiguredAS) {
		t.Fatalf("absent AS error=%v", err)
	}
	if err := l.endpoint.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := l.endpoint.CongestionInputs(as.key); !errors.Is(err, ErrEndpointClosed) {
		t.Fatalf("closed snapshot error=%v", err)
	}
}

func TestCongestionInputSnapshotMarksConcurrentRevisions(t *testing.T) {
	l, as, peer, _ := distributionFixture(t, params.TrafficModeLoadshare)
	as.setASPState(peer, StateASPActive, time.Hour)
	// Many bounded retained inputs make the collection span repeated actual
	// receive-path updates, rather than only comparing synthetic counters.
	for dpc := uint32(0); dpc < 512; dpc++ {
		reportPeerPressure(t, peer, params.NewCongestionIndications(1), dpc)
	}
	stop, done := make(chan struct{}), make(chan struct{})
	var updates atomic.Uint64
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			// Same level and destination: only the receive revision/timestamp moves.
			reportPeerPressure(t, peer, params.NewCongestionIndications(1), 2)
			updates.Add(1)
			runtime.Gosched()
		}
	}()
	defer func() { close(stop); <-done }()
	deadline := time.Now().Add(5 * time.Second)
	for updates.Load() == 0 && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	inconsistent := false
	for attempts := 0; attempts < 200 && time.Now().Before(deadline); attempts++ {
		snapshot, err := l.endpoint.CongestionInputs(as.key)
		if err != nil {
			t.Fatal(err)
		}
		if snapshot.Consistent != sameCongestionVersions(snapshot.VersionsBefore, snapshot.VersionsAfter) {
			t.Fatal("consistency flag disagrees with component vector")
		}
		if !snapshot.Consistent {
			inconsistent = true
			break
		}
	}
	if !inconsistent {
		t.Fatal("concurrent receive revisions were returned as a coherent snapshot")
	}
}

func TestCongestionInputVersionsIncludeTransportEpochConfigurationAndSampling(t *testing.T) {
	l, as, peer, _ := distributionFixture(t, params.TrafficModeLoadshare)
	as.setASPState(peer, StateASPActive, time.Hour)
	before, err := l.endpoint.CongestionInputs(as.key)
	if err != nil {
		t.Fatal(err)
	}
	state := TransportCongestionState{Association: peer.ID(), Epoch: peer.Epoch(), Generation: 3, SampledAt: time.Now(), Samples: 4, MissedSamples: 5, Known: false, Level: 2}
	peer.transportSnapshot.Store(&state)
	after, err := l.endpoint.CongestionInputs(as.key)
	if err != nil {
		t.Fatal(err)
	}
	version := after.VersionsBefore.Peers[0]
	if !version.TransportPresent || version.Transport != state || sameCongestionVersions(before.VersionsBefore, after.VersionsBefore) {
		t.Fatal("transport component was omitted from vector")
	}
	peer.resetTransportCongestionEpoch()
	restarted, err := l.endpoint.CongestionInputs(as.key)
	if err != nil {
		t.Fatal(err)
	}
	if restarted.Peers[0].Epoch == before.Peers[0].Epoch || restarted.Peers[0].Transport.Epoch != restarted.Peers[0].Epoch || sameCongestionVersions(after.VersionsBefore, restarted.VersionsBefore) {
		t.Fatal("transport restart was omitted from vector")
	}
}

func TestPeerCongestionDeltasReconstructSnapshotAcrossWithdrawal(t *testing.T) {
	l, as, peer, _ := distributionFixture(t, params.TrafficModeLoadshare)
	as.setASPState(peer, StateASPActive, time.Hour)
	consumer := newSSNMDeltaConsumer(t, "peer", l.endpoint)
	for _, level := range []uint8{3, 0, 2} {
		reportPeerPressure(t, peer, params.NewCongestionIndications(level), 2)
		consumer.drain(t, "peer report")
		consumer.requireStore(t, l.endpoint, "peer report")
	}
	as.setASPState(peer, StateASPInactive, time.Hour)
	consumer.drain(t, "AS withdrawal")
	consumer.requireStore(t, l.endpoint, "AS withdrawal")
	if consumer.kinds[SSNMPeerCongestionEvent] != 4 || len(consumer.replica.peerCongestion) != 0 {
		t.Fatal("peer changes/removal did not rebuild current state")
	}
}

func TestCongestionInputsMarksAnEpochResetInProgressInconsistent(t *testing.T) {
	l, as, peer, _ := distributionFixture(t, params.TrafficModeLoadshare)
	as.setASPState(peer, StateASPActive, time.Hour)
	peer.transportMu.Lock()
	defer peer.transportMu.Unlock()
	peer.transportSnapshot.Store(&TransportCongestionState{Association: peer.ID(), Epoch: peer.Epoch(), Known: true, Level: 3, Generation: 1})
	// Pause at the production reset's atomic epoch advance, before replacing
	// its old sample. Public collection must neither wait for this lock nor
	// label a sample from the old epoch coherent with the new membership.
	peer.epoch.Add(1)
	snapshot, err := l.endpoint.CongestionInputs(as.key)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Consistent || snapshot.Peers[0].Transport.Known || snapshot.VersionsAfter.Peers[0].Transport.Epoch == snapshot.VersionsAfter.Peers[0].Epoch {
		t.Fatalf("mixed epoch snapshot claimed consistency: %+v", snapshot)
	}
}

func TestSGPPolicyAndInputsCannotAuthorizeAReplacedBinding(t *testing.T) {
	for _, name := range []string{"same AS", "different network"} {
		t.Run(name, func(t *testing.T) {
			l, as, peer, _ := distributionFixtureConfigured(t, params.TrafficModeLoadshare, func(c *distributionFixtureConfig) {
				c.CongestionPolicy = func(SGPCongestionCandidate) bool { return true }
			})
			as.setASPState(peer, StateASPActive, time.Hour)
			oldKey := as.key
			replacement := oldKey
			if name == "different network" {
				replacement.NetworkAppearance = 9
			}
			peer.addDynamicASKey(replacement, RoutingKey{}, false)
			// Keep the old AS membership to exercise the current binding independently
			// of AS membership and congestion-version changes.
			snapshot, err := l.endpoint.CongestionInputs(oldKey)
			if err != nil {
				t.Fatal(err)
			}
			if snapshot.Peers[0].Active || snapshot.VersionsAfter.Peers[0].Active {
				t.Fatal("replaced inactive binding was reported active by NIF inputs")
			}
			payload, _ := distributionData(1, 0, "old binding").ProtocolData.ProtocolData()
			decision := as.prepareSGPCongestion(payload, oldKey)
			decision.members[peer] = true
			if !decision.current() {
				t.Fatal("test vector should match the replacement generation")
			}
			requireDataWriteError(t, peer.checkSGPCongestionDecision(oldKey, 1, decision), DataNotSent, ErrRoutingContextNotActive)
		})
	}
}
