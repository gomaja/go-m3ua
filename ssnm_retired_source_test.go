// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"errors"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestSSNMReportInFlightCannotRebindClosedAssociation(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(map[bool]string{false: "contextless", true: "explicit"}[explicit], func(t *testing.T) {
			var contexts []uint32
			var rc *params.Param
			if explicit {
				contexts = []uint32{1}
				rc = params.NewRoutingContext(1)
			}
			a, _ := pendingSSNMFixture(t, contexts...)
			endpoint := a.endpoint
			reached, release := make(chan struct{}), make(chan struct{})
			endpoint.aspRoutes.applyResolved = func() { close(reached); <-release }
			reportDone := make(chan error, 1)
			go func() {
				reportDone <- a.handleDestinationUnavailable(messages.NewDestinationUnavailable(nil, rc, apc(100), nil))
			}()
			select {
			case <-reached:
			case <-time.After(time.Second):
				close(release)
				t.Fatal("report did not reach publication")
			}
			if err := a.Close(); err != nil {
				close(release)
				t.Fatal(err)
			}
			close(release)
			if err := <-reportDone; err != nil && !errors.Is(err, ErrAssociationClosed) {
				t.Fatal(err)
			}
			if len(endpoint.SSNMKnowledge().Partitions) != 0 {
				t.Error("in-flight report recreated the closed source's binding")
			}
			b, _ := newTestConnWithContexts(t, StateASPInactive, RoleASP, contexts...)
			b.cfg.PeerSGP = a.cfg.PeerSGP
			b.cfg.TAck = time.Hour
			if !endpoint.trackAssociation(b) {
				t.Fatal("track replacement")
			}
			b.startTAck(messages.NewAspActive(nil, rc, nil), requestAspActive)
			if err := b.handleAspActiveAck(messages.NewAspActiveAck(nil, rc, nil)); err != nil {
				t.Fatal(err)
			}
			b.sendState(StateASPActive)
			for _, partition := range endpoint.SSNMKnowledge().Partitions {
				if len(partition.Destinations) != 0 {
					t.Errorf("replacement inherited a departed source's report: %+v", partition)
				}
				for _, binding := range partition.Bindings {
					if binding.Association == a.ID() {
						t.Error("departed source remains bound")
					}
				}
			}
		})
	}
}

func TestSSNMRetiredSourceCannotApplyToReplacementPartition(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, nil, nil)
	store := endpoint.ssnm
	partition := canonicalSSNMPartition("sg", "as")
	store.registerSource(1)
	store.registerSource(2)
	if err := store.bind(partition, 1, true); err != nil {
		t.Fatal(err)
	}
	store.retireAssociation(1)
	if err := store.bind(partition, 2, false); err != nil {
		t.Fatal(err)
	}
	report := SSNMReport{Kind: SSNMDestinationUnavailableReport, Source: SSNMPeerReport, Partition: partition, Association: 1, Destinations: []PointCodeRange{{PointCode: 100}}}
	if err := store.apply(report); !errors.Is(err, ErrAssociationClosed) {
		t.Errorf("retired source apply: %v", err)
	}
	if len(ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(), partition).Destinations) != 0 {
		t.Error("retired source wrote into the replacement partition")
	}
	if err := store.bind(partition, 1, true); !errors.Is(err, ErrAssociationClosed) {
		t.Errorf("retired source bind: %v", err)
	}
}

// Source metadata must follow concurrent ownership rather than departed IDs.
func TestSSNMSourceStateIsBoundedByLiveAssociations(t *testing.T) {
	for _, keepLive := range []bool{false, true} {
		t.Run(map[bool]string{false: "no survivor", true: "one survivor"}[keepLive], func(t *testing.T) {
			endpoint := newSSNMStateEndpoint(t, nil, nil)
			cfg := newASPAssociationConfigForTest(&HeartbeatInfo{Enabled: false}, 1, params.TrafficModeLoadshare, 0, nil)
			wantSources := 0
			var survivor *Association
			if keepLive {
				survivor = newAssociation(RoleASP, cfg)
				if !endpoint.trackAssociation(survivor) {
					t.Fatal("track survivor")
				}
				wantSources = 1
			}
			const departures = 4096
			for index := range departures {
				association := newAssociation(RoleASP, cfg)
				if !endpoint.trackAssociation(association) {
					t.Fatal("track association")
				}
				endpoint.ssnm.mu.Lock()
				registered := len(endpoint.ssnm.liveSources)
				endpoint.ssnm.mu.Unlock()
				if registered != wantSources+1 {
					t.Fatalf("source registration: got %d live sources, want %d", registered, wantSources+1)
				}
				if index%2 == 0 {
					if err := endpoint.ssnm.bind(canonicalSSNMPartition("sg", "as"), association.ID(), true); err != nil {
						t.Fatal(err)
					}
				}
				if err := association.Close(); err != nil {
					t.Fatal(err)
				}
				endpoint.ssnm.mu.Lock()
				sources := len(endpoint.ssnm.liveSources)
				_, survivorLive := endpoint.ssnm.liveSources[survivor.ID()]
				endpoint.ssnm.mu.Unlock()
				if sources != wantSources {
					t.Fatalf("%d departures left %d source entries with %d live associations", index+1, sources, wantSources)
				}
				if keepLive && !survivorLive {
					t.Fatal("retiring another source removed the live survivor")
				}
			}
			if survivor != nil {
				if err := survivor.Close(); err != nil {
					t.Fatal(err)
				}
			}
			endpoint.ssnm.mu.Lock()
			released := endpoint.ssnm.liveSources == nil
			partitions, records := len(endpoint.ssnm.partitions), endpoint.ssnm.records
			endpoint.ssnm.mu.Unlock()
			if !released {
				t.Error("source map retained after the last association departed")
			}
			if partitions != 0 || records != 0 {
				t.Errorf("departed associations retained %d partitions and %d records", partitions, records)
			}
		})
	}
}
