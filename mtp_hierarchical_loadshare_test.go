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
	return newNamedHierarchicalLoadshareFixture(testContext, preferred,
		[]SignallingGatewayID{"sg-a", "sg-b"}, []SignallingGatewayProcessID{"p0", "p1"})
}

func newNamedHierarchicalLoadshareFixture(testContext *testing.T, preferred RemoteASID, gateways []SignallingGatewayID, processes []SignallingGatewayProcessID) (*Endpoint, map[AssociationID]*Association, map[AssociationID]*mtpTransferCapture) {
	testContext.Helper()
	applicationServers := []RemoteASID{"primary", "secondary"}
	if preferred == "secondary" {
		applicationServers[0], applicationServers[1] = applicationServers[1], applicationServers[0]
	}
	config := &ASPConfig{Routing: &ASPRoutingConfig{
		SignallingGatewaySelection:        RouteSelectionLoadshare,
		SignallingGatewayProcessSelection: make(map[SignallingGatewayID]RouteSelectionMode),
		MTPRoutes:                         []MTPRouteConfig{{ID: "r", DestinationPointCode: 0, Mask: 8}},
	}}
	for _, gatewayID := range gateways {
		gateway := SignallingGatewayConfig{ID: gatewayID}
		for _, processID := range processes {
			gateway.SGPs = append(gateway.SGPs, SignallingGatewayProcessConfig{ID: processID,
				ApplicationServers: []RemoteASConfig{{ID: "primary", ASKey: staticASKey(7, 1)}, {ID: "secondary", ASKey: staticASKey(7, 2)}}})
		}
		config.SignallingGateways = append(config.SignallingGateways, gateway)
		config.Routing.SignallingGatewayProcessSelection[gatewayID] = RouteSelectionLoadshare
		config.Routing.Paths = append(config.Routing.Paths, MTPRoutePath{ID: MTPRoutePathID(string(gatewayID) + "-path"),
			SignallingGateway: gatewayID, ApplicationServers: applicationServers})
		config.Routing.MTPRoutes[0].Paths = append(config.Routing.MTPRoutes[0].Paths, MTPRoutePathID(string(gatewayID)+"-path"))
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

func TestMTPTransferHierarchicalLoadshareSeparatesSelectorNames(testContext *testing.T) {
	for _, fixture := range []struct {
		name      string
		gateways  []SignallingGatewayID
		processes []SignallingGatewayProcessID
	}{
		{name: "gateway-matches-selector", gateways: []SignallingGatewayID{"signalling-gateway", "sg-b"}, processes: []SignallingGatewayProcessID{"p0", "p1"}},
		{name: "slash-boundaries", gateways: []SignallingGatewayID{"a/b", "a"}, processes: []SignallingGatewayProcessID{"c", "b/c"}},
		{name: "nul-boundaries", gateways: []SignallingGatewayID{"a\x00b", "a"}, processes: []SignallingGatewayProcessID{"c", "b\x00c"}},
	} {
		testContext.Run(fixture.name, func(testContext *testing.T) {
			endpoint, associations, captures := newNamedHierarchicalLoadshareFixture(testContext, "primary", fixture.gateways, fixture.processes)
			observed := make(map[AssociationID]int)
			for flowIndex := 0; flowIndex < 4096; flowIndex++ {
				protocolData := params.NewProtocolDataPayload(uint32(flowIndex/256), 1+uint32((flowIndex/16)%16),
					params.ServiceIndSCCP, 0, 0, uint8(flowIndex), []byte{byte(flowIndex >> 8), byte(flowIndex)})
				var previous MTPTransferPath
				for replay := 0; replay < 2; replay++ {
					result, err := endpoint.MTPTransfer(MTPTransferRequest{ProtocolData: protocolData})
					if err != nil || len(result.SuccessfulPaths) != 1 || result.UserDataOctets != len(protocolData.Data) {
						testContext.Fatalf("flow %d: result=%+v error=%v", flowIndex, result, err)
					}
					target := result.SuccessfulPaths[0]
					if associations[target.Association] == nil || target.ApplicationServer != "primary" || target.AS != *staticASKey(7, 1) {
						testContext.Fatalf("unexpected target %+v", target)
					}
					if replay != 0 && target != previous {
						testContext.Fatalf("flow %d moved: %+v -> %+v", flowIndex, previous, target)
					}
					previous = target
					observed[target.Association]++
				}
			}
			for associationID := AssociationID(1); associationID <= 8; associationID++ {
				count := captures[associationID].count()
				testContext.Logf("association=%d peer=%q/%q writes=%d", associationID,
					associations[associationID].cfg.PeerSGP.SignallingGateway,
					associations[associationID].cfg.PeerSGP.SignallingGatewayProcess, count)
				if count != observed[associationID] {
					testContext.Errorf("association %d captured %d writes, results name %d", associationID, count, observed[associationID])
				}
			}
			if len(observed) != 8 {
				testContext.Fatalf("selector names restricted loadshare to %d of 8 eligible associations: %v", len(observed), observed)
			}
		})
	}
}

func TestASPTransferHashDoesNotAllocate(testContext *testing.T) {
	key := newASPTransferFlowKey("route\x00with/slashes", params.NewProtocolDataPayload(0x110000, 0x220000, params.ServiceIndSCCP, 0, 0, 15, nil))
	var checksum uint64
	allocations := testing.AllocsPerRun(1000, func() {
		checksum ^= hashASPTransferFlow(key, aspTransferGatewayHash, "", "")
		checksum ^= hashASPTransferFlow(key, aspTransferSGPHash, "sg-a", "")
		checksum ^= hashASPTransferFlow(key, aspTransferAssociationHash, "sg-a", "sgp-a1")
	})
	testContext.Logf("three selector hashes: allocations=%g checksum=%x", allocations, checksum)
	if allocations != 0 {
		testContext.Fatalf("three selector hashes allocate %g times, want zero", allocations)
	}
}

func TestASPTransferHashSeparatesDomainsAndIdentityFields(testContext *testing.T) {
	type hashInput struct {
		route   MTPRouteID
		domain  aspTransferHashDomain
		gateway SignallingGatewayID
		process SignallingGatewayProcessID
	}
	for _, testCase := range []struct {
		name   string
		first  hashInput
		second hashInput
	}{
		{name: "gateway-vs-SGP-domain", first: hashInput{route: "r", domain: aspTransferGatewayHash}, second: hashInput{route: "r", domain: aspTransferSGPHash}},
		{name: "SGP-vs-member-domain", first: hashInput{route: "r", domain: aspTransferSGPHash, gateway: "sg"}, second: hashInput{route: "r", domain: aspTransferAssociationHash, gateway: "sg"}},
		{name: "literal-selector-name", first: hashInput{route: "r", domain: aspTransferGatewayHash}, second: hashInput{route: "r", domain: aspTransferSGPHash, gateway: "signalling-gateway"}},
		{name: "slash-members", first: hashInput{route: "r", domain: aspTransferAssociationHash, gateway: "a/b", process: "c"}, second: hashInput{route: "r", domain: aspTransferAssociationHash, gateway: "a", process: "b/c"}},
		{name: "nul-route-gateway", first: hashInput{route: "a\x00b", domain: aspTransferSGPHash, gateway: "c"}, second: hashInput{route: "a", domain: aspTransferSGPHash, gateway: "b\x00c"}},
		{name: "unseparated-route-gateway", first: hashInput{route: "ab", domain: aspTransferSGPHash, gateway: "c"}, second: hashInput{route: "a", domain: aspTransferSGPHash, gateway: "bc"}},
		{name: "unseparated-gateway-process", first: hashInput{route: "r", domain: aspTransferAssociationHash, gateway: "ab", process: "c"}, second: hashInput{route: "r", domain: aspTransferAssociationHash, gateway: "a", process: "bc"}},
		{name: "nul-gateway-process", first: hashInput{route: "r", domain: aspTransferAssociationHash, gateway: "a\x00b", process: "c"}, second: hashInput{route: "r", domain: aspTransferAssociationHash, gateway: "a", process: "b\x00c"}},
	} {
		testContext.Run(testCase.name, func(testContext *testing.T) {
			protocolData := params.NewProtocolDataPayload(0x110000, 0x220000, params.ServiceIndSCCP, 0, 0, 15, nil)
			firstKey := newASPTransferFlowKey(testCase.first.route, protocolData)
			secondKey := newASPTransferFlowKey(testCase.second.route, protocolData)
			firstHash := hashASPTransferFlow(firstKey, testCase.first.domain, testCase.first.gateway, testCase.first.process)
			secondHash := hashASPTransferFlow(secondKey, testCase.second.domain, testCase.second.gateway, testCase.second.process)
			if firstHash == secondHash {
				testContext.Fatalf("distinct selector identities encode to the same hash: %+v and %+v", testCase.first, testCase.second)
			}
			if firstHash != hashASPTransferFlow(firstKey, testCase.first.domain, testCase.first.gateway, testCase.first.process) {
				testContext.Fatal("hash is not deterministic")
			}
		})
	}
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
