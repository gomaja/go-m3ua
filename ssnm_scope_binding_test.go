package m3ua

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func TestSSNMDynamicBindingRequiresRegisteredAppearance(t *testing.T) {
	for _, all := range []bool{false, true} {
		t.Run(fmt.Sprintf("all-appearances=%v", all), func(t *testing.T) {
			endpoint, association := newDynamicSSNMBinding(t, all)
			association.handleSignals(context.Background(), messages.NewAspActiveAck(nil, params.NewRoutingContext(7), nil))
			if association.State() != StateASPActive {
				t.Fatal("dynamic activation failed")
			}
			if err := association.handleDestinationAvailable(messages.NewDestinationAvailable(params.NewNetworkAppearance(10), params.NewRoutingContext(7), params.NewAffectedPointCode(0x123456), nil)); err != nil {
				t.Fatal(err)
			}
			err := association.handleDestinationUnavailable(messages.NewDestinationUnavailable(
				params.NewNetworkAppearance(20), params.NewRoutingContext(7), params.NewAffectedPointCode(0x123456), nil,
			))
			if err != nil {
				t.Fatalf("foreign-appearance report: %v", err)
			}
			knowledge := ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(), canonicalSSNMPartition("sg-a", "as-core"))
			if !knowledge.TrafficAuthorized {
				t.Fatal("active dynamic binding lost traffic authorization")
			}
			want := DestinationAvailable
			if all {
				want = DestinationUnavailable
			}
			if len(knowledge.Destinations) != 1 || knowledge.Destinations[0].Availability.State != want {
				t.Fatalf("canonical knowledge: %+v want %v", knowledge, want)
			}
			status, known := endpoint.MTPDestinationStatus(MTPDestination{MTPRoute: "sccp-a", PointCode: 0x123456})
			if !known || status.Availability != want {
				t.Fatalf("aggregate status=%+v known=%v want=%v", status, known, want)
			}
			targets, err := endpoint.aspRoutes.selectTransfer(MTPTransferRequest{MTPRoute: "sccp-a", ProtocolData: transferProtocolData(0x123456, 1, nil)}, aspCongestionDecision{}, endpoint.ssnm)
			if all {
				if err == nil || len(targets) != 0 {
					t.Fatalf("unavailable wildcard route selected: targets=%v err=%v", targets, err)
				}
			} else if err != nil || len(targets) != 1 {
				t.Fatalf("foreign appearance changed selection: targets=%v err=%v", targets, err)
			}
			if !all && !ssnmPartitionPresent(endpoint.SSNMKnowledge(), SSNMPartition{Kind: SSNMStandalonePartition, Association: association.ID()}) {
				t.Fatal("foreign appearance did not retain static-path standalone outcome")
			}

		})
	}
}

func newDynamicSSNMBinding(t *testing.T, all bool) (*Endpoint, *Association) {
	t.Helper()
	routingKey := testRoutingKey(10, 0x123456, 3)
	routingKey.NetworkAppearanceSet = !all
	config := validASPConfig()
	useSignallingGateways(config, "sg-a")
	config.SignallingGateways[0].SGPs[0].ApplicationServers = []RemoteASConfig{{ID: "as-core", RoutingKey: &routingKey}}
	endpoint := newSSNMStateEndpoint(t, config, nil)
	association, _ := newTestConnWithContexts(t, StateASPInactive, RoleASP)
	// newAssociation initializes this gate; the older handler-only helper does not.
	association.rkmRequestGate = make(chan struct{}, 1)
	association.cfg.PeerSGP = &SGPIdentity{SignallingGateway: "sg-a", SignallingGatewayProcess: "sgp-a1"}
	if !endpoint.trackAssociation(association) {
		t.Fatal("dynamic association refused")
	}
	setRegistrationResultWriter(t, association, params.SuccessfullyRegistered, 7)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	results, err := association.RegisterRoutingKeys(ctx, RoutingKeyRegistration{RemoteAS: "as-core", RoutingKey: routingKey})
	if err != nil {
		t.Fatalf("register dynamic canonical AS: %v", err)
	}
	if len(results) != 1 || results[0].RoutingContext != 7 {
		t.Fatalf("registration results %+v", results)
	}
	association.cfg.TAck = time.Hour
	association.startTAck(messages.NewAspActive(nil, params.NewRoutingContext(7), nil), requestAspActive)
	association.syncSSNMBindings()
	return endpoint, association
}

func TestSSNMDynamicAppearanceDuringActivation(t *testing.T) {
	for _, all := range []bool{false, true} {
		t.Run(fmt.Sprintf("all-appearances=%v", all), func(t *testing.T) {
			endpoint, association := newDynamicSSNMBinding(t, all)
			if err := association.handleDestinationUnavailable(messages.NewDestinationUnavailable(params.NewNetworkAppearance(20), params.NewRoutingContext(7), params.NewAffectedPointCode(0x123456), nil)); err != nil {
				t.Fatal(err)
			}
			knowledge := ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(), canonicalSSNMPartition("sg-a", "as-core"))
			if (len(knowledge.Destinations) != 0) != all {
				t.Fatalf("early all=%v knowledge=%+v", all, knowledge)
			}
			if knowledge.TrafficAuthorized {
				t.Fatal("early report authorized traffic")
			}
		})
	}
}

func TestSSNMDynamicBindingInfersOmittedAppearance(t *testing.T) {
	endpoint, association := newDynamicSSNMBinding(t, false)
	association.handleSignals(context.Background(), messages.NewAspActiveAck(nil, params.NewRoutingContext(7), nil))
	if err := association.handleDestinationUnavailable(messages.NewDestinationUnavailable(nil, params.NewRoutingContext(7), params.NewAffectedPointCode(0x123456), nil)); err != nil {
		t.Fatal(err)
	}
	knowledge := ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(), canonicalSSNMPartition("sg-a", "as-core"))
	if len(knowledge.Destinations) != 1 || knowledge.Destinations[0].Availability.State != DestinationUnavailable {
		t.Fatalf("inferred appearance knowledge=%+v", knowledge)
	}
}
