package main

import (
	"errors"
	"testing"

	"github.com/gomaja/go-m3ua"
)

func TestPortsIdentifyEveryStableAssociationOnce(t *testing.T) {
	seen := make(map[int]bool)
	servedBySGP := make(map[[2]int]int)
	for sgp := 0; sgp < sgpCount; sgp++ {
		for index := 0; index < stablePerSGP; index++ {
			role, err := classifyPort(stablePort(sgp, index))
			if err != nil || !role.Stable || role.SGP != sgp || role.Index != index || len(role.AS) != asPerStableAssociation {
				t.Fatalf("stable port %d: %+v, %v", stablePort(sgp, index), role, err)
			}
			if seen[role.StableIndex] {
				t.Fatalf("stable index %d twice", role.StableIndex)
			}
			seen[role.StableIndex] = true
			for _, as := range role.AS {
				servedBySGP[[2]int{sgp, as}]++
			}
		}
	}
	if len(seen) != stableAssociations || len(servedBySGP) != sgpCount*asPerGateway {
		t.Fatalf("%d stable associations serve %d SGP Application Servers", len(seen), len(servedBySGP))
	}
	for key, count := range servedBySGP {
		if count != 1 {
			t.Fatalf("SGP %d AS %d served by %d stable associations", key[0], key[1], count)
		}
	}
}

func TestChurnCyclesUseFreshPortsAndEveryCloseMode(t *testing.T) {
	ports := make(map[int]bool)
	modes := make(map[closeMode]int)
	for cycle := 0; cycle < maxChurnCycles; cycle++ {
		role, port, err := churnCycle(cycle)
		if err != nil || role.Stable || len(role.AS) != 1 || role.StableIndex != -1 {
			t.Fatalf("cycle %d: %+v, %v", cycle, role, err)
		}
		if ports[port] {
			t.Fatalf("cycle %d reuses port %d", cycle, port)
		}
		ports[port] = true
		if cycle < 40 {
			modes[role.CloseMode]++
		}
	}
	if modes[closeASPGraceful] == 0 || modes[closeASPAbrupt] == 0 || modes[closePeer] == 0 {
		t.Fatalf("the first 40 cycles close by %v", modes)
	}
	if _, _, err := churnCycle(maxChurnCycles); err == nil {
		t.Fatal("cycle beyond the port space accepted")
	}
	for _, port := range []int{portBase - 1, portBase + stablePerSGP, portBase + churnPortOffset + churnPortsPerSGP, portBase + portsPerSGP*sgpCount} {
		if _, err := classifyPort(port); !errors.Is(err, errUnknownPeerPort) {
			t.Fatalf("port %d classified: %v", port, err)
		}
	}
}

func TestPopulationCoversTheReferenceStoreExactlyOnce(t *testing.T) {
	type record struct {
		gateway, as int
		pointCode   uint32
	}
	records := make(map[record]bool)
	routeRecords, available := 0, 0
	for sgp := 0; sgp < sgpCount; sgp++ {
		for as := 0; as < asPerGateway; as++ {
			up, down := populationShare(sgp, as)
			available += len(up)
			for _, pointCode := range append(append([]uint32(nil), up...), down...) {
				key := record{sgp / processesPerGateway, as, pointCode}
				if records[key] {
					t.Fatalf("record %+v reported twice", key)
				}
				records[key] = true
				if pointCode < plainDestinationBase {
					routeRecords++
				}
			}
		}
	}
	if len(records) != stateRecords || available != stateRecords/2 {
		t.Fatalf("%d records, %d available", len(records), available)
	}
	if routeRecords != gatewayCount*referenceRouteCount {
		t.Fatalf("%d records intersect a route, want %d", routeRecords, gatewayCount*referenceRouteCount)
	}
}

func TestInventoryIsAcceptedByTheLibrary(t *testing.T) {
	for _, routes := range []int{0, referenceRouteCount} {
		inventory := aspInventory(routes)
		endpoint, err := m3ua.NewEndpoint(m3ua.EndpointConfig{Role: m3ua.RoleASP, ASP: inventory, SSNMState: ssnmStateConfig()})
		if err != nil {
			t.Fatalf("routes %d: %v", routes, err)
		}
		if routes == 0 && inventory.Routing != nil || routes != 0 && len(inventory.Routing.MTPRoutes) != routes {
			t.Fatalf("routes %d: routing %+v", routes, inventory.Routing)
		}
		// One route is looked up rather than all of them: with 1,000 routes
		// MTPRouteStatuses takes seconds at this revision.
		if _, known := endpoint.MTPRouteStatus("route-0999"); known != (routes == referenceRouteCount) {
			t.Fatalf("routes %d: route-0999 known %t", routes, known)
		}
		_ = endpoint.Close()
	}
	peer, err := m3ua.NewEndpoint(m3ua.EndpointConfig{Role: m3ua.RoleSGP, SGP: sgpEndpointConfig()})
	if err != nil {
		t.Fatalf("peer endpoint: %v", err)
	}
	_ = peer.Close()
}
