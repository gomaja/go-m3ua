package m3ua

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/gomaja/go-m3ua/messages/params"
)

func TestMTPTransferHierarchicalLoadshareUsesEveryEligibleAssociation(testContext *testing.T) {
	for _, preferred := range []RemoteASID{"primary", "secondary"} {
		for _, variedLabels := range []bool{false, true} {
			testContext.Run(fmt.Sprintf("%s/varied-labels=%t", preferred, variedLabels), func(testContext *testing.T) {
				endpoint, associations, captures := newHierarchicalLoadshareFixture(testContext, preferred)
				preferredContext := uint32(1)
				if preferred == "secondary" {
					preferredContext = 2
				}
				observed := make(map[AssociationID]int)
				flowCount := 256
				if variedLabels {
					flowCount = 4096
				}
				for flowIndex := 0; flowIndex < flowCount; flowIndex++ {
					protocolData := params.NewProtocolDataPayload(0, 1, params.ServiceIndSCCP, 0, 0, uint8(flowIndex), nil)
					if variedLabels {
						protocolData.OriginatingPointCode = uint32(flowIndex / 256)
						protocolData.DestinationPointCode = 1 + uint32((flowIndex/16)%16)
					}
					var original MTPTransferPath
					for replay := 0; replay < 2; replay++ {
						protocolData.Data = []byte{byte(flowIndex >> 8), byte(flowIndex), byte(replay)}
						result, err := endpoint.MTPTransfer(MTPTransferRequest{ProtocolData: protocolData})
						if err != nil || result.UserDataOctets != len(protocolData.Data) || len(result.SuccessfulPaths) != 1 {
							testContext.Fatalf("flow %d replay %d: result=%+v error=%v", flowIndex, replay, result, err)
						}
						target := result.SuccessfulPaths[0]
						if replay == 0 {
							original = target
						} else if target != original {
							testContext.Fatalf("flow %d moved on replay: %+v -> %+v", flowIndex, original, target)
						}
						association, known := associations[target.Association]
						if !known || target.ApplicationServer != preferred || target.AS != *staticASKey(7, preferredContext) {
							testContext.Fatalf("unexpected target: %+v", target)
						}
						observed[target.Association]++
						data, stream := captures[target.Association].lastData(testContext)
						decoded, decodeErr := data.ProtocolData.ProtocolData()
						if decodeErr != nil || !bytes.Equal(decoded.Data, protocolData.Data) ||
							decoded.OriginatingPointCode != protocolData.OriginatingPointCode || decoded.DestinationPointCode != protocolData.DestinationPointCode ||
							decoded.SignallingLinkSelection != protocolData.SignallingLinkSelection || stream != association.streamFor(protocolData.SignallingLinkSelection) {
							testContext.Fatalf("flow %d replay %d captured DATA differs: decoded=%+v stream=%d error=%v", flowIndex, replay, decoded, stream, decodeErr)
						}
						if data.NetworkAppearance == nil || data.NetworkAppearance.NetworkAppearance() != 7 || data.RoutingContext == nil ||
							len(data.RoutingContext.RoutingContexts()) != 1 || data.RoutingContext.RoutingContexts()[0] != target.AS.RoutingContext {
							testContext.Fatalf("flow %d captured scope differs from selected AS %+v", flowIndex, target.AS)
						}
					}
				}
				for associationID := AssociationID(1); associationID <= 8; associationID++ {
					association := associations[associationID]
					count := captures[associationID].count()
					testContext.Logf("association=%d peer=%s/%s writes=%d", associationID,
						association.cfg.PeerSGP.SignallingGateway, association.cfg.PeerSGP.SignallingGatewayProcess, count)
					if count != observed[associationID] {
						testContext.Errorf("association %d captured %d writes, results name %d", associationID, count, observed[associationID])
					}
				}
				if len(observed) != len(associations) {
					testContext.Fatalf("hierarchical loadshare used %d of %d active, available associations across %d flows; counts=%v", len(observed), len(associations), flowCount, observed)
				}
			})
		}
	}
}

func newHierarchicalLoadshareFixture(testContext *testing.T, preferred RemoteASID) (*Endpoint, map[AssociationID]*Association, map[AssociationID]*mtpTransferCapture) {
	testContext.Helper()
	applicationServers := []RemoteASID{"primary", "secondary"}
	if preferred == "secondary" {
		applicationServers[0], applicationServers[1] = applicationServers[1], applicationServers[0]
	}
	config := &ASPConfig{Routing: &ASPRoutingConfig{
		SignallingGatewaySelection:        RouteSelectionLoadshare,
		SignallingGatewayProcessSelection: make(map[SignallingGatewayID]RouteSelectionMode),
		MTPRoutes: []MTPRouteConfig{{ID: "r", DestinationPointCode: 0, Mask: 8,
			Paths: []MTPRoutePathID{"sg-a-path", "sg-b-path"}}},
	}}
	for _, gatewayID := range []SignallingGatewayID{"sg-a", "sg-b"} {
		gateway := SignallingGatewayConfig{ID: gatewayID}
		for _, processID := range []SignallingGatewayProcessID{"p0", "p1"} {
			gateway.SGPs = append(gateway.SGPs, SignallingGatewayProcessConfig{ID: processID,
				ApplicationServers: []RemoteASConfig{{ID: "primary", ASKey: staticASKey(7, 1)}, {ID: "secondary", ASKey: staticASKey(7, 2)}}})
		}
		config.SignallingGateways = append(config.SignallingGateways, gateway)
		config.Routing.SignallingGatewayProcessSelection[gatewayID] = RouteSelectionLoadshare
		config.Routing.Paths = append(config.Routing.Paths, MTPRoutePath{ID: MTPRoutePathID(string(gatewayID) + "-path"),
			SignallingGateway: gatewayID, ApplicationServers: applicationServers})
	}
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: config})
	if err != nil {
		testContext.Fatalf("NewEndpoint: %v", err)
	}
	testContext.Cleanup(func() { _ = endpoint.Close() })
	associations := make(map[AssociationID]*Association)
	captures := make(map[AssociationID]*mtpTransferCapture)
	for _, gateway := range config.SignallingGateways {
		for _, process := range gateway.SGPs {
			for range 2 {
				association, capture := attachMultiScopeASPAssociation(testContext, endpoint,
					SGPIdentity{SignallingGateway: gateway.ID, SignallingGatewayProcess: process.ID}, 7, 1, 2)
				associations[association.ID()], captures[association.ID()] = association, capture
				for _, routingContext := range []uint32{1, 2} {
					applyASPDAVA(testContext, association, 7, routingContext, 0, 8)
					status, found := endpoint.ASPStatus(ASPStatusKey{Association: association.ID(), AS: *staticASKey(7, routingContext)})
					if !found || !status.LocalStateSet || status.LocalState != StateASPActive {
						testContext.Fatalf("association %d scope %d is not active: %+v found=%t", association.ID(), routingContext, status, found)
					}
				}
			}
		}
	}
	knowledge := endpoint.SSNMKnowledge()
	if len(associations) != 8 || len(knowledge.Partitions) != 4 || knowledge.ReportsRefused != 0 || knowledge.RecordsRefused != 0 || knowledge.PartitionsInvalidated != 0 {
		testContext.Fatalf("unexpected inventory: associations=%d knowledge=%+v", len(associations), knowledge)
	}
	for _, partition := range knowledge.Partitions {
		if !partition.TrafficAuthorized || len(partition.Bindings) != 4 || len(partition.Destinations) != 1 ||
			!partition.Destinations[0].AvailabilitySet || partition.Destinations[0].Availability.State != DestinationAvailable ||
			partition.Destinations[0].Destination != (PointCodeRange{PointCode: 0, Mask: 8}) {
			testContext.Fatalf("partition not fully available: %+v", partition)
		}
		for _, binding := range partition.Bindings {
			if binding.Pending || associations[binding.Association] == nil {
				testContext.Fatalf("binding not active or not in fixture: %+v", binding)
			}
		}
	}
	return endpoint, associations, captures
}

func TestMTPTransferHierarchicalLoadsharePreservesHealthyTargetsAfterMembershipChanges(testContext *testing.T) {
	endpoint, associations, _ := newHierarchicalLoadshareFixture(testContext, "primary")
	const flowCount = 256
	targets := make([]MTPTransferPath, flowCount)
	requests := make([]MTPTransferRequest, flowCount)
	for flowIndex := range requests {
		requests[flowIndex] = MTPTransferRequest{ProtocolData: params.NewProtocolDataPayload(
			0, 1, params.ServiceIndSCCP, 0, 0, uint8(flowIndex), []byte{byte(flowIndex)})}
		result, err := endpoint.MTPTransfer(requests[flowIndex])
		if err != nil || len(result.SuccessfulPaths) != 1 {
			testContext.Fatalf("initial flow %d: result=%+v error=%v", flowIndex, result, err)
		}
		targets[flowIndex] = result.SuccessfulPaths[0]
	}
	for _, gatewayID := range []SignallingGatewayID{"sg-a", "sg-b"} {
		for _, processID := range []SignallingGatewayProcessID{"p0", "p1"} {
			association, _ := attachMultiScopeASPAssociation(testContext, endpoint,
				SGPIdentity{SignallingGateway: gatewayID, SignallingGatewayProcess: processID}, 7, 1, 2)
			associations[association.ID()] = association
			applyASPDAVA(testContext, association, 7, 1, 0, 8)
		}
	}
	for flowIndex, request := range requests {
		result, err := endpoint.MTPTransfer(request)
		if err != nil || len(result.SuccessfulPaths) != 1 || result.SuccessfulPaths[0] != targets[flowIndex] {
			testContext.Fatalf("healthy flow %d moved after arrival/SSNM reevaluation: old=%+v result=%+v error=%v", flowIndex, targets[flowIndex], result, err)
		}
	}
	retired := targets[0].Association
	if err := associations[retired].Close(); err != nil {
		testContext.Fatalf("close selected association: %v", err)
	}
	for flowIndex, request := range requests {
		result, err := endpoint.MTPTransfer(request)
		if err != nil || len(result.SuccessfulPaths) != 1 {
			testContext.Fatalf("flow %d after retirement: result=%+v error=%v", flowIndex, result, err)
		}
		target := result.SuccessfulPaths[0]
		if target.Association == retired || target.SGP != targets[flowIndex].SGP || target.AS != targets[flowIndex].AS {
			testContext.Fatalf("flow %d did not retain its eligible SGP/AS: old=%+v new=%+v", flowIndex, targets[flowIndex], target)
		}
		if targets[flowIndex].Association != retired && target != targets[flowIndex] {
			testContext.Fatalf("healthy flow %d moved after sibling retirement: old=%+v new=%+v", flowIndex, targets[flowIndex], target)
		}
	}
}
