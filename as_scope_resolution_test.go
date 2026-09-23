package m3ua

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestRawDataResolvesOmittedNetworkAppearanceByRoutingContext(test *testing.T) {
	for _, appearance := range []uint32{0, 7} {
		for _, dynamic := range []bool{false, true} {
			test.Run(fmt.Sprintf("appearance=%d/dynamic=%t", appearance, dynamic), func(test *testing.T) {
				association, capture := twoNetworkASPAssociation(test)
				key := *staticASKey(appearance, 1)
				association.cfg.ApplicationServers[0].ASKey = key
				if dynamic {
					association.addDynamicASKey(key, RoutingKey{}, false)
				}
				association.signalWriter = nil
				message := messages.NewData(nil, params.NewRoutingContext(1),
					params.NewProtocolData(1, 2, params.ServiceIndSCCP, 0, 0, 1, []byte("payload")), nil)
				written, err := association.WriteSignal(message)
				if err != nil {
					test.Fatalf("WriteSignal with omitted Network Appearance: %v", err)
				}
				if written != message.MarshalLen() || capture.submissions() != 1 {
					test.Fatalf("write length=%d submissions=%d", written, capture.submissions())
				}
				resolved, err := association.outboundDataScopeForMessage(message)
				if err != nil || resolved != key {
					test.Fatalf("resolved scope=%+v error=%v, want %+v", resolved, err, key)
				}
			})
		}
	}
}

func newMixedNetworkScopeAssociation(test *testing.T, appearance uint32, dynamic bool) (*Endpoint, *Association) {
	test.Helper()
	config := &ASPConfig{SignallingGateways: []SignallingGatewayConfig{{
		ID: "sg-a",
		SGPs: []SignallingGatewayProcessConfig{{ID: "sgp-a1", ApplicationServers: []RemoteASConfig{
			{ID: "as-core", ASKey: staticASKey(appearance, 1)},
			{ID: "as-edge", ASKey: staticASKey(appearance, 3)},
			{ID: "as-other", ASKey: staticASKey(appearance+1, 5)},
		}}},
	}}}
	endpoint := newSSNMStateEndpoint(test, config, nil)
	association, _ := newTestConnWithContexts(test, StateASPActive, RoleASP, 1, 3, 5)
	association.cfg.ApplicationServers = []ASConfig{
		{ASKey: *staticASKey(appearance, 1)},
		{ASKey: *staticASKey(appearance, 3)},
		{ASKey: *staticASKey(appearance+1, 5)},
	}
	association.cfg.PeerSGP = &SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}
	if dynamic {
		for _, entry := range config.SignallingGateways[0].SGPs[0].ApplicationServers {
			association.addDynamicASKey(*entry.ASKey, RoutingKey{}, false)
			association.noteCanonicalRemoteAS(entry.ASKey.RoutingContext, entry.ID)
		}
	}
	association.noteRoutingContextsAcked(params.NewRoutingContext(1, 3, 5))
	if !endpoint.trackAssociation(association) {
		test.Fatal("Endpoint refused the mixed-network Association")
	}
	return endpoint, association
}

func TestSSNMResolvesNetworkAppearanceByNamedRoutingContexts(test *testing.T) {
	for _, appearance := range []uint32{0, 7} {
		for _, explicit := range []bool{false, true} {
			for _, dynamic := range []bool{false, true} {
				for _, routingContexts := range [][]uint32{{1}, {1, 3}} {
					name := fmt.Sprintf("appearance=%d/explicit=%t/dynamic=%t/contexts=%v", appearance, explicit, dynamic, routingContexts)
					test.Run(name, func(test *testing.T) {
						endpoint, association := newMixedNetworkScopeAssociation(test, appearance, dynamic)
						var networkAppearance *params.Param
						if explicit {
							networkAppearance = params.NewNetworkAppearance(appearance)
						}
						if err := association.handleDestinationUnavailable(messages.NewDestinationUnavailable(
							networkAppearance, params.NewRoutingContext(routingContexts...), params.NewAffectedPointCode(123), nil,
						)); err != nil {
							test.Fatalf("DUNA: %v", err)
						}
						snapshot := endpoint.SSNMKnowledge()
						if len(snapshot.Partitions) != 3 {
							test.Fatalf("partitions=%+v, want only the three canonical partitions", snapshot.Partitions)
						}
						for _, entry := range []struct {
							id             RemoteASID
							routingContext uint32
						}{{"as-core", 1}, {"as-edge", 3}, {"as-other", 5}} {
							knowledge := ssnmPartitionKnowledge(test, snapshot, canonicalSSNMPartition("sg-a", entry.id))
							if !containsRoutingContext(routingContexts, entry.routingContext) {
								if len(knowledge.Destinations) != 0 {
									test.Fatalf("unrelated AS %s acquired knowledge: %+v", entry.id, knowledge)
								}
								continue
							}
							destination := ssnmDestination(test, knowledge, 123, 0)
							local, held := association.destinations.lookup(destinationKey{
								networkAppearance: appearance, networkAppearanceSet: true,
								routingContext: entry.routingContext, routingContextSet: true, pointCode: 123,
							})
							if !held || local.Availability != DestinationUnavailable {
								test.Fatalf("AS %s local dimensions lost DUNA: %+v held=%t", entry.id, local, held)
							}
							if !destination.AvailabilitySet || destination.Availability.State != DestinationUnavailable {
								test.Fatalf("AS %s lost DUNA: %+v", entry.id, destination)
							}
							scope := destination.Availability.Scope
							if scope.NetworkAppearanceSet != explicit || !reflect.DeepEqual(scope.RoutingContexts, routingContexts) ||
								explicit && scope.NetworkAppearance != appearance {
								test.Fatalf("wire scope changed: %+v", scope)
							}
						}
					})
				}
			}
		}
	}
}

func TestMTPTransferHonorsDUNAWithOmittedNetworkAppearance(test *testing.T) {
	config := ssnmPeerInventoryConfig()
	config.SignallingGateways = config.SignallingGateways[:1]
	config.SignallingGateways[0].SGPs = config.SignallingGateways[0].SGPs[:1]
	config.SignallingGateways[0].SGPs[0].ApplicationServers[1].ASKey = staticASKey(8, 3)
	config.Routing = &ASPRoutingConfig{
		SignallingGatewaySelection:        RouteSelectionPrimaryBackup,
		SignallingGatewayProcessSelection: map[SignallingGatewayID]RouteSelectionMode{"sg-a": RouteSelectionPrimaryBackup},
		Paths:                             []MTPRoutePath{{ID: "core", SignallingGateway: "sg-a", ApplicationServers: []RemoteASID{"as-core"}}},
		MTPRoutes:                         []MTPRouteConfig{{ID: "core", DestinationPointCode: 123, Paths: []MTPRoutePathID{"core"}}},
	}
	endpoint := newSSNMStateEndpoint(test, config, nil)
	association, capture := twoNetworkASPAssociation(test)
	association.cfg.ApplicationServers = []ASConfig{{ASKey: *staticASKey(7, 1)}, {ASKey: *staticASKey(8, 3)}}
	association.cfg.PeerSGP = &SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}
	association.noteRoutingContextsAcked(params.NewRoutingContext(1, 3))
	if !endpoint.trackAssociation(association) {
		test.Fatal("Endpoint refused the mixed-network Association")
	}
	sendDAVA(test, association, 7, 1, 123)
	drainMTPIndications(endpoint.MTPIndications())
	drainSignallingStatuses(association.SignallingStatus())
	request := MTPTransferRequest{ProtocolData: transferProtocolData(123, 1, []byte("payload"))}
	if _, err := endpoint.MTPTransfer(request); err != nil {
		test.Fatalf("initial available transfer: %v", err)
	}
	if err := association.handleDestinationUnavailable(messages.NewDestinationUnavailable(nil, params.NewRoutingContext(1), params.NewAffectedPointCode(123), nil)); err != nil {
		test.Fatalf("DUNA: %v", err)
	}
	_, err := endpoint.MTPTransfer(request)
	if !errors.Is(err, ErrNoMTPRoute) || capture.submissions() != 1 {
		test.Fatalf("transfer after DUNA: error=%v submissions=%d", err, capture.submissions())
	}
	status, known := endpoint.MTPDestinationStatus(MTPDestination{MTPRoute: "core", PointCode: 123})
	if !known || status.Availability != DestinationUnavailable {
		test.Errorf("aggregate after DUNA: status=%+v known=%t", status, known)
	}
	select {
	case indication := <-endpoint.MTPIndications():
		if indication.Kind != MTPPauseIndication || indication.Destination.Availability != DestinationUnavailable {
			test.Errorf("indication after DUNA: %+v", indication)
		}
	default:
		test.Error("DUNA did not emit MTP-PAUSE")
	}
	statuses := endpoint.MTPDestinationStatuses()
	if len(statuses) != 1 || statuses[0] != status {
		test.Errorf("aggregate snapshot after DUNA: %+v", statuses)
	}
	select {
	case status := <-association.SignallingStatus():
		if status.NetworkAppearanceSet || !status.RoutingContextSet || !reflect.DeepEqual(status.RoutingContexts, []uint32{1}) {
			test.Errorf("DUNA wire scope changed: %+v", status)
		}
	default:
		test.Error("DUNA did not emit a signalling status")
	}
	if err := association.handleDestinationAvailable(messages.NewDestinationAvailable(nil, params.NewRoutingContext(1), params.NewAffectedPointCode(123), nil)); err != nil {
		test.Fatalf("DAVA: %v", err)
	}
	requireMTPIndication(test, endpoint.MTPIndications(), MTPResumeIndication, "core", 123, 0, DestinationAvailable, false, 0, false)
	if _, err := endpoint.MTPTransfer(request); err != nil || capture.submissions() != 2 {
		test.Fatalf("transfer after DAVA: error=%v submissions=%d", err, capture.submissions())
	}
}

func TestASPRouteStatusResolvesNetworkAppearanceByRoutingContext(test *testing.T) {
	for _, appearance := range []uint32{0, 7} {
		for _, explicit := range []bool{false, true} {
			for _, dynamic := range []bool{false, true} {
				for _, contexts := range [][]uint32{{1}, {1, 3}} {
					name := fmt.Sprintf("appearance=%d/explicit=%t/dynamic=%t/contexts=%v", appearance, explicit, dynamic, contexts)
					test.Run(name, func(test *testing.T) {
						_, association := newMixedNetworkScopeAssociation(test, appearance, dynamic)
						status := &DestinationStatus{RoutingContexts: contexts, RoutingContextSet: true}
						if explicit {
							status.NetworkAppearance, status.NetworkAppearanceSet = appearance, true
						}
						for _, candidate := range []ASKey{*staticASKey(appearance, 1), *staticASKey(appearance, 3), *staticASKey(appearance+1, 5), *staticASKey(appearance+1, 1), {RoutingContext: 1, RoutingContextSet: true}} {
							want := candidate.NetworkAppearanceSet && candidate.NetworkAppearance == appearance && (candidate.RoutingContext == 1 || len(contexts) == 2 && candidate.RoutingContext == 3)
							if got := aspRouteASMatchesStatus(association, candidate, status); got != want {
								test.Errorf("candidate=%+v matched=%t, want %t", candidate, got, want)
							}
						}
						if status.NetworkAppearanceSet != explicit || !reflect.DeepEqual(status.RoutingContexts, contexts) {
							test.Errorf("status wire scope changed: %+v", status)
						}
					})
				}
			}
		}
	}
}

func TestASPRouteStatusResolvesOmittedRoutingContext(test *testing.T) {
	for _, scenario := range []struct {
		name    string
		servers []ASConfig
		key     ASKey
		want    bool
	}{
		{name: "contextless", key: ASKey{}, want: true},
		{name: "single", servers: []ASConfig{{ASKey: *staticASKey(0, 1)}}, key: *staticASKey(0, 1), want: true},
		{name: "wrong-appearance", servers: []ASConfig{{ASKey: *staticASKey(0, 1)}}, key: *staticASKey(7, 1)},
		{name: "ambiguous", servers: []ASConfig{{ASKey: *staticASKey(7, 1)}, {ASKey: *staticASKey(8, 3)}}, key: *staticASKey(7, 1)},
	} {
		test.Run(scenario.name, func(test *testing.T) {
			association := &Association{cfg: &AssociationConfig{ApplicationServers: scenario.servers}}
			if got := aspRouteASMatchesStatus(association, scenario.key, &DestinationStatus{}); got != scenario.want {
				test.Errorf("matched=%t, want %t", got, scenario.want)
			}
		})
	}
}

func TestSSNMRejectsRoutingContextsInDifferentNetworkAppearances(test *testing.T) {
	for _, explicit := range []bool{false, true} {
		test.Run(fmt.Sprintf("explicit=%t", explicit), func(test *testing.T) {
			endpoint, association := newMixedNetworkScopeAssociation(test, 7, false)
			before := endpoint.SSNMKnowledge()
			var appearance *params.Param
			if explicit {
				appearance = params.NewNetworkAppearance(7)
			}
			err := association.handleDestinationUnavailable(messages.NewDestinationUnavailable(
				appearance, params.NewRoutingContext(1, 5), params.NewAffectedPointCode(123), nil,
			))
			if !errors.Is(err, ErrInvalidNetworkAppearance) {
				test.Fatalf("cross-network DUNA error=%v, want ErrInvalidNetworkAppearance", err)
			}
			if !reflect.DeepEqual(before, endpoint.SSNMKnowledge()) {
				test.Fatal("cross-network DUNA changed canonical knowledge")
			}
			association.destinations.mu.RLock()
			records := len(association.destinations.state)
			association.destinations.mu.RUnlock()
			if records != 0 {
				test.Fatalf("cross-network DUNA retained %d local records", records)
			}
		})
	}
}

func TestPartialOverrideWithdrawsSSNMBinding(test *testing.T) {
	for _, sibling := range []bool{false, true} {
		test.Run(fmt.Sprintf("sibling=%t", sibling), func(test *testing.T) {
			endpoint := newSSNMStateEndpoint(test, ssnmPeerInventoryConfig(), nil)
			association := attachSSNMAssociation(test, endpoint, SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}, 7, 1, 3)
			var surviving *Association
			if sibling {
				surviving = attachSSNMAssociation(test, endpoint, SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a2"}, 7, 2)
			}
			partition := canonicalSSNMPartition("sg-a", "as-core")
			sendDAVA(test, association, 7, 1, 123)
			epoch := ssnmPartitionKnowledge(test, endpoint.SSNMKnowledge(), partition).Epoch
			association.handleSignals(context.Background(), messages.NewNotify(params.NewStatus(params.AlternateAspActive), nil, params.NewRoutingContext(1), nil))
			if !association.routingContextOverridden(1) {
				test.Fatal("Notify did not apply the partial Override")
			}
			snapshot := endpoint.SSNMKnowledge()
			if sibling {
				knowledge := ssnmPartitionKnowledge(test, snapshot, partition)
				if len(knowledge.Bindings) != 1 || knowledge.Bindings[0].Association != surviving.ID() || knowledge.Epoch != epoch {
					test.Fatalf("only the valid sibling should retain ownership: %+v", knowledge)
				}
			} else if ssnmPartitionPresent(snapshot, partition) {
				test.Fatalf("last binding was not retired: %+v", snapshot.Partitions)
			}
			sendDUNA(test, association, 7, 1, 123)
			if !reflect.DeepEqual(snapshot, endpoint.SSNMKnowledge()) {
				test.Fatal("SSNM from the overridden context changed retained knowledge")
			}
			association.noteRoutingContextsAcked(params.NewRoutingContext(1))
			knowledge := ssnmPartitionKnowledge(test, endpoint.SSNMKnowledge(), partition)
			if sibling {
				if knowledge.Epoch != epoch || len(knowledge.Destinations) != 1 || knowledge.Destinations[0].Availability.State != DestinationAvailable {
					test.Fatalf("reactivation lost the valid sibling's knowledge: %+v", knowledge)
				}
			} else if knowledge.Epoch <= epoch || len(knowledge.Destinations) != 0 {
				test.Fatalf("reactivation resurrected withdrawn knowledge: %+v", knowledge)
			}
		})
	}
}

func TestPartialOverrideReadmitsSSNMOnlyDuringFreshActivation(test *testing.T) {
	endpoint := newSSNMStateEndpoint(test, ssnmPeerInventoryConfig(), nil)
	association := attachSSNMAssociation(test, endpoint, SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}, 7, 1, 3)
	association.handleSignals(context.Background(), messages.NewNotify(params.NewStatus(params.AlternateAspActive), nil, params.NewRoutingContext(1), nil))
	association.startTAck(messages.NewAspActive(params.NewTrafficModeType(params.TrafficModeOverride), params.NewRoutingContext(1), nil), requestAspActive)
	association.syncSSNMBindings()
	sendDUNA(test, association, 7, 1, 123)
	partition := canonicalSSNMPartition("sg-a", "as-core")
	knowledge := ssnmPartitionKnowledge(test, endpoint.SSNMKnowledge(), partition)
	if knowledge.TrafficAuthorized || len(knowledge.Bindings) != 1 || !knowledge.Bindings[0].Pending || len(knowledge.Destinations) != 1 {
		test.Fatalf("fresh activation did not admit pending knowledge: %+v", knowledge)
	}
	association.noteRoutingContextsAcked(params.NewRoutingContext(1))
	activated := ssnmPartitionKnowledge(test, endpoint.SSNMKnowledge(), partition)
	if !activated.TrafficAuthorized || activated.Epoch != knowledge.Epoch || len(activated.Destinations) != 1 {
		test.Fatalf("activation failed to preserve pending knowledge: %+v", activated)
	}
}

func FuzzSSNMScopedNetworkAppearance(fuzz *testing.F) {
	fuzz.Add(uint32(0), uint32(1), uint32(0), uint32(7), false, false)
	fuzz.Add(uint32(1), uint32(3), uint32(7), uint32(8), true, false)
	fuzz.Add(^uint32(0), uint32(0), uint32(7), uint32(0), false, true)
	fuzz.Fuzz(func(test *testing.T, firstContext, secondContext, firstAppearance, secondAppearance uint32, explicit, dynamic bool) {
		if firstContext == secondContext {
			return
		}
		first := *staticASKey(firstAppearance, firstContext)
		second := *staticASKey(secondAppearance, secondContext)
		association := &Association{cfg: &AssociationConfig{ApplicationServers: []ASConfig{{ASKey: first}, {ASKey: second}}}}
		if dynamic {
			first.NetworkAppearance = secondAppearance
			association.dynamicPeerASKeys = map[uint32]ASKey{firstContext: first}
		}
		scope := WireScope{RoutingContexts: []uint32{firstContext, secondContext}, RoutingContextSet: true,
			NetworkAppearance: firstAppearance, NetworkAppearanceSet: explicit}
		if explicit {
			first.NetworkAppearance = firstAppearance
			second.NetworkAppearance = firstAppearance
		}
		if got := association.ssnmASKeys(scope); !reflect.DeepEqual(got, []ASKey{first, second}) {
			test.Fatalf("resolved=%+v, want %+v", got, []ASKey{first, second})
		}
		status := &DestinationStatus{NetworkAppearance: scope.NetworkAppearance, NetworkAppearanceSet: explicit,
			RoutingContexts: scope.RoutingContexts, RoutingContextSet: true}
		for _, candidate := range []ASKey{first, second} {
			if !aspRouteASMatchesStatus(association, candidate, status) {
				test.Fatalf("resolved status did not match candidate %+v", candidate)
			}
			candidate.NetworkAppearance++
			if aspRouteASMatchesStatus(association, candidate, status) {
				test.Fatalf("resolved status matched wrong appearance %+v", candidate)
			}
		}
	})
}
