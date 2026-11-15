// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func peerRefusalFixture(t *testing.T) (*Endpoint, *Association, *Association, ASKey, ASKey) {
	t.Helper()
	endpoint, peer, sibling, first, second := peerRetentionHandlerFixture(t)
	endpoint.ssnm.mu.Lock()
	endpoint.ssnm.limits.MaxAffectedPointCodes = 1
	endpoint.ssnm.mu.Unlock()
	peerRetentionHandleSCON(t, peer, 3, 1, 2)
	peerRetentionHandleSCON(t, sibling, 1, 1, 2)
	return endpoint, peer, sibling, first, second
}

func peerRefusalSCON(appearance, contexts *params.Param) *messages.SignallingCongestion {
	return messages.NewSignallingCongestion(appearance, contexts, params.NewAffectedPointCode(123, 124), nil, params.NewCongestionIndications(2), nil)
}

func TestOversizedPeerSCONRetiresOnlyResolvedAS(t *testing.T) {
	endpoint, peer, _, first, second := peerRefusalFixture(t)
	transport := TransportCongestionState{Association: peer.ID(), Epoch: peer.Epoch(), Known: true, Level: 2}
	peer.transportSnapshot.Store(&transport)
	before, subscription, err := endpoint.SubscribeSSNM()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = subscription.Close() }()
	var removed PeerCongestionState
	var expected []PeerCongestionState
	for _, record := range before.PeerCongestion {
		if record.Association == peer.ID() && record.AS == first {
			removed = record
		} else {
			expected = append(expected, record)
		}
	}
	if removed.Association == 0 || len(expected) != 3 {
		t.Fatalf("fixture did not retain both peers and ASes: %+v", before.PeerCongestion)
	}
	if err := peer.handleSignallingCongestion(peerRefusalSCON(params.NewNetworkAppearance(0), params.NewRoutingContext(1, 1))); !errors.Is(err, ErrSSNMOversizedReport) {
		t.Fatalf("oversized SCON: %v", err)
	}
	after := endpoint.SSNMKnowledge()
	if !reflect.DeepEqual(expected, after.PeerCongestion) {
		t.Fatalf("RC1 refusal changed unrelated peer/RC2 state: want %+v, got %+v", expected, after.PeerCongestion)
	}
	if after.ReportsRefused != before.ReportsRefused+1 || after.RecordsRefused != before.RecordsRefused || after.LastResourceLoss == "" {
		t.Fatalf("whole oversized report accounting: before %+v, after %+v", before, after)
	}
	if !reflect.DeepEqual(before.Partitions, after.Partitions) || peer.TransportCongestion() != transport || peer.State() != StateASPActive {
		t.Fatal("refusal changed bindings, epoch, transport or active state")
	}
	inputs, err := endpoint.CongestionInputs(second)
	if err != nil {
		t.Fatal(err)
	}
	var peerInput *PeerCongestionInput
	for index := range inputs.Peers {
		if inputs.Peers[index].Association == peer.ID() {
			peerInput = &inputs.Peers[index]
		}
	}
	if peerInput == nil || !peerInput.Active || len(peerInput.PeerCongestion) != 1 || peerInput.PeerCongestion[0].Congestion.Level != 3 || peerInput.Transport != transport {
		t.Fatalf("RC2 congestion input lost pressure after RC1 refusal: %+v", inputs)
	}
	previous := before.Revision
	var replay []PeerCongestionState
	replay = append(replay, before.PeerCongestion...)
	removals := 0
	for previous < after.Revision {
		event := peerRetentionNext(t, subscription)
		if event.Revision != previous+1 || event.ReportSet {
			t.Fatalf("refusal published raw report or discontinuous revision: %+v", event)
		}
		previous = event.Revision
		if event.Kind == SSNMPeerCongestionEvent {
			if !event.PeerCongestionRemoved || event.PeerCongestion == nil || event.PeerCongestion.Association != peer.ID() || event.PeerCongestion.AS != first {
				t.Fatalf("unrelated or non-removal peer delta: %+v", event)
			}
			removals++
			for index, record := range replay {
				if record.Association == peer.ID() && record.AS == first {
					replay = append(replay[:index], replay[index+1:]...)
					break
				}
			}
		}
	}
	if removals != 1 || !reflect.DeepEqual(replay, expected) {
		t.Fatalf("removal deltas do not reconstruct snapshot: %d %+v", removals, replay)
	}
	resync, err := subscription.Resync()
	if err != nil || !reflect.DeepEqual(resync, after) {
		t.Fatalf("resync differs from committed refusal snapshot: %+v, %v", resync, err)
	}
	endpoint.ssnm.mu.Lock()
	if endpoint.ssnm.records != len(expected) || endpoint.ssnm.bytes != 2*ssnmPartitionBaseBytes+3*peerCongestionRecordBytes(expected[0]) {
		t.Errorf("refusal released unrelated charges: records %d bytes %d", endpoint.ssnm.records, endpoint.ssnm.bytes)
	}
	endpoint.ssnm.mu.Unlock()
	peerRetentionHandleSCON(t, peer, 1, 1)
	if !peerRetentionHasRecord(endpoint.SSNMKnowledge(), peer.ID(), first) {
		t.Fatal("refusal withdrew the active AS token")
	}
}

func TestOversizedDAUDPreservesPeerCongestion(t *testing.T) {
	endpoint, peer, _, _, _ := peerRefusalFixture(t)
	before := endpoint.SSNMKnowledge()
	err := peer.handleDestinationStateAudit(messages.NewDestinationStateAudit(params.NewNetworkAppearance(0), params.NewRoutingContext(1), params.NewAffectedPointCode(123, 124), nil))
	if !errors.Is(err, ErrSSNMOversizedReport) {
		t.Fatalf("oversized DAUD: %v", err)
	}
	after := endpoint.SSNMKnowledge()
	if !reflect.DeepEqual(before.PeerCongestion, after.PeerCongestion) {
		t.Fatalf("DAUD request erased peer congestion: %+v -> %+v", before.PeerCongestion, after.PeerCongestion)
	}
}

func TestOversizedNonPeerSSNMPreservesPeerCongestion(t *testing.T) {
	endpoint, peer, _, _, _ := peerRefusalFixture(t)
	before := endpoint.SSNMKnowledge()
	err := peer.reportSSNM(SSNMReport{Kind: SSNMDestinationUserPartUnavailableReport}, params.NewNetworkAppearance(0), params.NewRoutingContext(1), params.NewAffectedPointCode(123, 124))
	if !errors.Is(err, ErrSSNMOversizedReport) {
		t.Fatalf("oversized event-only SSNM: %v", err)
	}
	if after := endpoint.SSNMKnowledge(); !reflect.DeepEqual(before.PeerCongestion, after.PeerCongestion) {
		t.Fatalf("non-peer congestion report erased peer state: %+v -> %+v", before.PeerCongestion, after.PeerCongestion)
	}
}

func TestInvalidOversizedPeerSCONPreservesPeerCongestion(t *testing.T) {
	for _, invalid := range []struct {
		name string
		na   *params.Param
		rc   *params.Param
	}{
		{"missing RC", params.NewNetworkAppearance(0), nil},
		{"unknown RC", params.NewNetworkAppearance(0), params.NewRoutingContext(999)},
		{"unknown NA", params.NewNetworkAppearance(9), params.NewRoutingContext(1)},
	} {
		t.Run(invalid.name, func(t *testing.T) {
			endpoint, peer, _, _, _ := peerRefusalFixture(t)
			before := endpoint.SSNMKnowledge()
			if err := peer.handleSignallingCongestion(peerRefusalSCON(invalid.na, invalid.rc)); err == nil || errors.Is(err, ErrSSNMOversizedReport) {
				t.Fatalf("scope validation must precede resource refusal: %v", err)
			}
			if after := endpoint.SSNMKnowledge(); !reflect.DeepEqual(before, after) {
				t.Fatal("invalid scope changed retained knowledge or resource counters")
			}
		})
	}
}

func TestOversizedPeerSCONOmittedNAResolvesConfiguredAS(t *testing.T) {
	endpoint, peer, _, first, second := peerRefusalFixture(t)
	if err := peer.handleSignallingCongestion(peerRefusalSCON(nil, params.NewRoutingContext(1))); !errors.Is(err, ErrSSNMOversizedReport) {
		t.Fatalf("oversized SCON without NA: %v", err)
	}
	snapshot := endpoint.SSNMKnowledge()
	if peerRetentionHasRecord(snapshot, peer.ID(), first) || !peerRetentionHasRecord(snapshot, peer.ID(), second) {
		t.Fatalf("omitted NA lost resolved RC scope: %+v", snapshot.PeerCongestion)
	}
}

func TestOversizedPeerSCONDoesNotPublishSuccessfulReport(t *testing.T) {
	endpoint, peer, _, _, _ := peerRefusalFixture(t)
	_, subscription, err := endpoint.SubscribeSSNM()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = subscription.Close() }()
	if err := peer.handleSignallingCongestion(peerRefusalSCON(nil, params.NewRoutingContext(1))); !errors.Is(err, ErrSSNMOversizedReport) {
		t.Fatal(err)
	}
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		event, err := subscription.Next(ctx)
		cancel()
		if errors.Is(err, context.DeadlineExceeded) {
			break
		}
		if err != nil || event.ReportSet {
			t.Fatalf("refused report was published: %+v, %v", event, err)
		}
	}
}

func TestOversizedPeerSCONScopesWildcardNetworkAppearance(t *testing.T) {
	for _, omitted := range []bool{false, true} {
		t.Run(map[bool]string{false: "explicit network", true: "omitted network"}[omitted], func(t *testing.T) {
			endpoint, peer, _, first, second := peerRefusalFixture(t)
			wildcard := first
			wildcard.NetworkAppearance, wildcard.NetworkAppearanceSet = 0, false
			peer.addDynamicASKey(wildcard, RoutingKey{}, false)
			as := peer.as.get(wildcard)
			as.setTrafficMode(params.TrafficModeLoadshare)
			as.setASPState(peer, StateASPActive, time.Hour)
			for _, appearance := range []*params.Param{nil, params.NewNetworkAppearance(7), params.NewNetworkAppearance(8)} {
				if err := peer.handleSignallingCongestion(messages.NewSignallingCongestion(appearance, params.NewRoutingContext(1), params.NewAffectedPointCode(123), nil, params.NewCongestionIndications(3), nil)); err != nil {
					t.Fatal(err)
				}
			}
			before := endpoint.SSNMKnowledge()
			var expected []PeerCongestionState
			wildcardRecords := 0
			for _, record := range before.PeerCongestion {
				if record.Association == peer.ID() && record.AS == wildcard {
					wildcardRecords++
					if omitted || !record.Scope.NetworkAppearanceSet || record.Scope.NetworkAppearance == 7 {
						continue
					}
				}
				expected = append(expected, record)
			}
			if wildcardRecords != 3 {
				t.Fatalf("fixture did not retain three independent wire networks: %+v", before.PeerCongestion)
			}
			appearance := params.NewNetworkAppearance(7)
			if omitted {
				appearance = nil
			}
			if err := peer.handleSignallingCongestion(peerRefusalSCON(appearance, params.NewRoutingContext(1))); !errors.Is(err, ErrSSNMOversizedReport) {
				t.Fatal(err)
			}
			after := endpoint.SSNMKnowledge()
			if !reflect.DeepEqual(expected, after.PeerCongestion) || !peerRetentionHasRecord(after, peer.ID(), second) {
				t.Fatalf("wildcard refusal crossed network/AS/peer scope: want %+v, got %+v", expected, after.PeerCongestion)
			}
		})
	}
}

func TestOversizedPeerSCONOmittedRCResolvesSingleConfiguredAS(t *testing.T) {
	listener, _, peer, _ := distributionFixture(t, params.TrafficModeLoadshare)
	if err := peer.handleAspActive(messages.NewAspActive(params.NewTrafficModeType(params.TrafficModeLoadshare), params.NewRoutingContext(1), nil)); err != nil {
		t.Fatal(err)
	}
	endpoint := listener.endpoint
	endpoint.ssnm.mu.Lock()
	endpoint.ssnm.limits.MaxAffectedPointCodes = 1
	endpoint.ssnm.mu.Unlock()
	peerRetentionHandleSCON(t, peer, 3, 1)
	before := endpoint.SSNMKnowledge()
	if len(before.PeerCongestion) != 1 {
		t.Fatal("fixture did not retain peer congestion")
	}
	if err := peer.handleSignallingCongestion(peerRefusalSCON(nil, nil)); !errors.Is(err, ErrSSNMOversizedReport) {
		t.Fatal(err)
	}
	if after := endpoint.SSNMKnowledge(); len(after.PeerCongestion) != 0 || !reflect.DeepEqual(before.Partitions, after.Partitions) {
		t.Fatalf("omitted RC did not retire exactly its configured AS: %+v", after)
	}
}

func TestUnresolvedOversizedPeerSCONRetiresOnlyItsAssociation(t *testing.T) {
	endpoint, peer, _, _, _ := peerRefusalFixture(t)
	before := endpoint.SSNMKnowledge()
	var expected []PeerCongestionState
	for _, record := range before.PeerCongestion {
		if record.Association != peer.ID() {
			expected = append(expected, record)
		}
	}
	registry := peer.as
	peer.as = nil
	defer func() { peer.as = registry }()
	err := peer.reportSSNM(SSNMReport{Kind: SSNMSignallingCongestionReport, PeerReported: true}, nil, nil, params.NewAffectedPointCode(123, 124))
	if !errors.Is(err, ErrSSNMOversizedReport) {
		t.Fatal(err)
	}
	if after := endpoint.SSNMKnowledge(); !reflect.DeepEqual(expected, after.PeerCongestion) {
		t.Fatalf("unresolved peer loss crossed association boundary: want %+v, got %+v", expected, after.PeerCongestion)
	}
}

func TestDelayedOversizedPeerSCONCannotEraseReplacementKnowledge(t *testing.T) {
	for _, change := range []string{"epoch", "binding", "unresolved binding", "empty admission", "departed source"} {
		t.Run(change, func(t *testing.T) {
			endpoint, peer, _, first, _ := peerRefusalFixture(t)
			scope := WireScope{NetworkAppearanceSet: true, RoutingContextSet: true, RoutingContexts: []uint32{1}}
			refusal := peerCongestionRefusal{scope: scope, admission: peer.capturePeerCongestionAdmission(scope)}
			if change == "unresolved binding" {
				refusal = endpoint.ssnm.captureUnresolvedPeerCongestionRefusal(peer.ID(), peer.Epoch(), scope)
			}
			switch change {
			case "epoch":
				peer.resetTransportCongestionEpoch()
				peerRetentionHandleSCON(t, peer, 1, 1, 2)
			case "binding", "unresolved binding":
				peer.addDynamicASKey(first, RoutingKey{}, false)
				peerRetentionHandleSCON(t, peer, 1, 1, 2)
			case "empty admission":
				refusal.admission.bindings = map[ASKey]uint64{}
			case "departed source":
				endpoint.ssnm.retireAssociation(peer.ID())
			}
			before := endpoint.SSNMKnowledge()
			if err := peer.refuseOversizedSSNM(scope, 2, 1, SSNMSignallingCongestionReport, refusal); !errors.Is(err, ErrSSNMOversizedReport) {
				t.Fatal(err)
			}
			if after := endpoint.SSNMKnowledge(); !reflect.DeepEqual(before.PeerCongestion, after.PeerCongestion) {
				t.Fatalf("delayed refusal erased current source knowledge: %+v -> %+v", before.PeerCongestion, after.PeerCongestion)
			}
		})
	}
}

func TestOversizedRefusalKeepsPeerAndDestinationAccountingSeparate(t *testing.T) {
	for _, peerSCON := range []bool{false, true} {
		t.Run(map[bool]string{false: "DAUD", true: "peer SCON"}[peerSCON], func(t *testing.T) {
			endpoint, peer, _, _, second := peerRefusalFixture(t)
			partition := SSNMPartition{Kind: SSNMStandalonePartition, Association: peer.ID()}
			for _, kind := range []SSNMReportKind{SSNMDestinationUnavailableReport, SSNMSignallingCongestionReport} {
				if err := endpoint.ssnm.apply(SSNMReport{Kind: kind, Source: SSNMPeerReport, Association: peer.ID(), Partition: partition, Scope: WireScope{RoutingContextSet: true, RoutingContexts: []uint32{1}}, Destinations: []PointCodeRange{{PointCode: 123}}, CongestionLevelSet: true, CongestionLevel: 2}); err != nil {
					t.Fatal(err)
				}
			}
			before := endpoint.SSNMKnowledge()
			if len(ssnmPartitionKnowledge(t, before, partition).Destinations) != 1 {
				t.Fatal("fixture did not retain destination authority")
			}
			var err error
			if peerSCON {
				err = peer.handleSignallingCongestion(peerRefusalSCON(nil, params.NewRoutingContext(1)))
			} else {
				err = peer.handleDestinationStateAudit(messages.NewDestinationStateAudit(nil, params.NewRoutingContext(1), params.NewAffectedPointCode(123, 124), nil))
			}
			if !errors.Is(err, ErrSSNMOversizedReport) {
				t.Fatal(err)
			}
			after := endpoint.SSNMKnowledge()
			if peerSCON {
				if len(ssnmPartitionKnowledge(t, after, partition).Destinations) != 0 || after.PartitionsInvalidated != before.PartitionsInvalidated+1 {
					t.Fatalf("scoped report did not invalidate destination authority: %+v", after)
				}
			} else if !reflect.DeepEqual(before.Partitions, after.Partitions) || after.PartitionsInvalidated != before.PartitionsInvalidated {
				t.Fatal("DAUD discarded authoritative knowledge")
			}
			if !peerRetentionHasRecord(after, peer.ID(), second) {
				t.Fatal("destination invalidation erased independent RC2 peer record")
			}
			if !peerSCON && !reflect.DeepEqual(before.PeerCongestion, after.PeerCongestion) {
				t.Fatal("DAUD destination invalidation erased peer state")
			}
			assertSSNMScopeAccounting(t, endpoint)
		})
	}
}
