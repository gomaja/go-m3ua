package main

import (
	"errors"
	"fmt"

	"github.com/gomaja/go-m3ua"
	"github.com/gomaja/go-m3ua/messages/params"
)

// The reference memory inventory of performance-budgets.md section 4: 32
// associations, 128 SSNM partitions, 16,384 retained state records, 8
// subscriptions and, when routing is enabled, 1,000 routes.
//
// The partitions are canonical Signalling Gateway and Application Server
// pairs, so the inventory needs provisioned peers: two Signalling Gateways of
// two SGPs each (the routed topology of the router fixture, Network
// Appearance 7), each gateway serving 64 Application Servers. Every SGP labels
// its Application Servers with its own Routing Contexts, as RFC 4666 Section
// 1.4.2.1 permits, while the ASP resolves both labels to one canonical
// partition per gateway and Application Server.
const (
	gatewayCount             = 2
	processesPerGateway      = 2
	sgpCount                 = gatewayCount * processesPerGateway
	asPerGateway             = 64
	stablePerSGP             = 8
	stableAssociations       = sgpCount * stablePerSGP
	asPerStableAssociation   = asPerGateway / stablePerSGP
	partitionCount           = gatewayCount * asPerGateway
	destinationsPerPartition = 128
	stateRecords             = partitionCount * destinationsPerPartition
	subscriberCount          = 8
	referenceRouteCount      = 1000
	networkAppearance        = uint32(7)

	// Each SGP dials from its own local port range, so the ASP listener's
	// selector identifies the SGP, the Application Servers and the close mode
	// of an accepted association from the peer port alone, before any M3UA
	// message arrives. Stable associations use the first stablePerSGP ports of
	// the range; churn associations use churnPortsPerSGP ports after
	// churnPortOffset, so no local port is reused within a run.
	portBase         = 30000
	portsPerSGP      = 1000
	churnPortOffset  = 100
	churnPortsPerSGP = 768
	maxChurnCycles   = sgpCount * churnPortsPerSGP

	routeDestinationBase = uint32(0x220000)
	plainDestinationBase = uint32(0x400000)
)

// closeMode is how one churn cycle ends. Two of the three are the
// independent child close of the ASP listener's accepted association; the
// third has the peer end it.
type closeMode int

const (
	// closeASPGraceful ends the child with Association.ShutdownContext: ASP
	// Inactive and ASP Down, then SCTP release (RFC 4666 Section 4.9 option a).
	closeASPGraceful closeMode = iota
	// closeASPAbrupt ends the child with Association.Close (option b).
	closeASPAbrupt
	// closePeer has the SGP close its side; the ASP observes the end and
	// releases its child.
	closePeer
	closeModeCount
)

func (mode closeMode) String() string {
	switch mode {
	case closeASPGraceful:
		return "asp-graceful"
	case closeASPAbrupt:
		return "asp-abrupt"
	case closePeer:
		return "peer"
	default:
		return fmt.Sprintf("close-mode-%d", int(mode))
	}
}

var errUnknownPeerPort = errors.New("peer port is outside every provisioned SGP range")

// portRole is what the listener selector derives from one peer port.
type portRole struct {
	SGP         int
	Stable      bool
	Index       int // stable association index within the SGP, or churn port offset
	AS          []int
	CloseMode   closeMode
	StableIndex int // global stable index, valid when Stable
}

func gatewayID(gateway int) m3ua.SignallingGatewayID {
	return m3ua.SignallingGatewayID(fmt.Sprintf("sg-%c", 'a'+gateway))
}

func processID(process int) m3ua.SignallingGatewayProcessID {
	return m3ua.SignallingGatewayProcessID(fmt.Sprintf("p%d", process))
}

func sgpIdentity(sgp int) m3ua.SGPIdentity {
	return m3ua.SGPIdentity{
		SignallingGateway:        gatewayID(sgp / processesPerGateway),
		SignallingGatewayProcess: processID(sgp % processesPerGateway),
	}
}

func remoteASID(as int) m3ua.RemoteASID {
	return m3ua.RemoteASID(fmt.Sprintf("as-%02d", as))
}

// routingContext is the wire label one SGP uses for one Application Server.
func routingContext(sgp, as int) uint32 {
	return uint32(1000*(sgp+1) + as)
}

func asKey(sgp, as int) m3ua.ASKey {
	return m3ua.ASKey{
		NetworkAppearance:    networkAppearance,
		NetworkAppearanceSet: true,
		RoutingContext:       routingContext(sgp, as),
		RoutingContextSet:    true,
	}
}

// stableASes returns the Application Servers stable association index of one
// SGP carries: every eighth one, so the 8 associations of an SGP together
// carry all 64 of its gateway's Application Servers exactly once.
func stableASes(index int) []int {
	servers := make([]int, 0, asPerStableAssociation)
	for as := index; as < asPerGateway; as += stablePerSGP {
		servers = append(servers, as)
	}
	return servers
}

func stablePort(sgp, index int) int {
	return portBase + portsPerSGP*sgp + index
}

// churnCycle maps one global churn cycle number to the SGP that dials it, its
// local port, the one Application Server it joins and its close mode. Cycles
// rotate over the SGPs; within one SGP the port offset advances by one per
// cycle. The Application Server is offset modulo 64 and the close mode offset
// modulo 3, which are coprime, so every combination recurs every 192 cycles
// of one SGP and even a short run exercises all three close modes.
func churnCycle(cycle int) (portRole, int, error) {
	if cycle < 0 || cycle >= maxChurnCycles {
		return portRole{}, 0, fmt.Errorf("churn cycle %d outside [0, %d)", cycle, maxChurnCycles)
	}
	sgp := cycle % sgpCount
	offset := cycle / sgpCount
	port := portBase + portsPerSGP*sgp + churnPortOffset + offset
	role, err := classifyPort(port)
	return role, port, err
}

// classifyPort is the listener selector's whole decision.
func classifyPort(port int) (portRole, error) {
	relative := port - portBase
	if relative < 0 || relative >= portsPerSGP*sgpCount {
		return portRole{}, fmt.Errorf("%w: %d", errUnknownPeerPort, port)
	}
	sgp := relative / portsPerSGP
	within := relative % portsPerSGP
	switch {
	case within < stablePerSGP:
		return portRole{SGP: sgp, Stable: true, Index: within, AS: stableASes(within),
			StableIndex: sgp*stablePerSGP + within}, nil
	case within >= churnPortOffset && within < churnPortOffset+churnPortsPerSGP:
		offset := within - churnPortOffset
		return portRole{SGP: sgp, Index: offset, AS: []int{offset % asPerGateway},
			CloseMode: closeMode(offset % int(closeModeCount)), StableIndex: -1}, nil
	default:
		return portRole{}, fmt.Errorf("%w: %d", errUnknownPeerPort, port)
	}
}

func applicationServers(role portRole) []m3ua.ASConfig {
	servers := make([]m3ua.ASConfig, 0, len(role.AS))
	for _, as := range role.AS {
		servers = append(servers, m3ua.ASConfig{ASKey: asKey(role.SGP, as), TrafficMode: params.TrafficModeLoadshare})
	}
	return servers
}

// associationBase is the transport configuration both sides share.
func associationBase() *m3ua.AssociationConfig {
	config := m3ua.NewAssociationConfig()
	config.SetSCTPNoDelay(true).SetSCTPSACK(0, 1)
	config.HeartbeatInfo = &m3ua.HeartbeatInfo{Enabled: false}
	config.DataQueueSize = dataQueueSize
	config.EstablishTimeout = establishTimeout
	return config
}

// aspAssociationConfig is what the ASP listener selector returns for one peer.
func aspAssociationConfig(role portRole) *m3ua.AssociationConfig {
	config := associationBase().SetApplicationServers(applicationServers(role)...)
	identity := sgpIdentity(role.SGP)
	config.PeerSGP = &identity
	return config
}

// sgpAssociationConfig is the peer SGP's configuration for the same
// association.
func sgpAssociationConfig(role portRole) *m3ua.AssociationConfig {
	return associationBase().SetApplicationServers(applicationServers(role)...)
}

// partitionDestinations returns the 128 destinations retained in the
// partition of one Application Server, identical for both gateways. Every
// route whose number is congruent to the Application Server modulo 64 comes
// first, so 2,000 of the 16,384 records intersect a provisioned route when
// routing is enabled; the rest are outside every route. The zero-route run
// retains exactly the same destinations.
func partitionDestinations(as int) []uint32 {
	destinations := make([]uint32, 0, destinationsPerPartition)
	for route := as; route < referenceRouteCount; route += asPerGateway {
		destinations = append(destinations, routeDestinationBase+uint32(route))
	}
	for index := len(destinations); index < destinationsPerPartition; index++ {
		destinations = append(destinations, plainDestinationBase+uint32(as*destinationsPerPartition+index))
	}
	return destinations
}

// populationShare returns the destinations one SGP reports for one
// Application Server: each SGP of a gateway reports its own half of the
// partition, split by availability so the store holds both DAVA and DUNA
// knowledge.
func populationShare(sgp, as int) (available, unavailable []uint32) {
	destinations := partitionDestinations(as)
	half := destinationsPerPartition / processesPerGateway
	start := (sgp % processesPerGateway) * half
	for index := start; index < start+half; index++ {
		if index%2 == 0 {
			available = append(available, destinations[index])
		} else {
			unavailable = append(unavailable, destinations[index])
		}
	}
	return available, unavailable
}

// aspInventory provisions the ASP Endpoint's peers and, with routes, the
// 1,000-route outbound inventory: route r reaches destination 0x220000+r
// through Application Server r mod 64 of either gateway.
func aspInventory(routes int) *m3ua.ASPConfig {
	config := &m3ua.ASPConfig{
		MTPIndicationQueueSize:                  mtpIndicationQueueSize,
		MaxAffectedPointCodesPerSSNM:            maxAffectedPointCodes,
		MaxSSNMStateRecordsPerRoute:             m3ua.DefaultMaxSSNMStateRecordsPerRoute,
		MaxSSNMStateRecordsPerSignallingGateway: m3ua.DefaultMaxSSNMStateRecords / gatewayCount,
		MaxSSNMStateRecords:                     m3ua.DefaultMaxSSNMStateRecords,
		MaxSSNMDestinationRecords:               m3ua.DefaultMaxSSNMDestinationRecords,
	}
	for gateway := 0; gateway < gatewayCount; gateway++ {
		gatewayConfig := m3ua.SignallingGatewayConfig{ID: gatewayID(gateway)}
		for process := 0; process < processesPerGateway; process++ {
			sgp := gateway*processesPerGateway + process
			processConfig := m3ua.SignallingGatewayProcessConfig{ID: processID(process)}
			for as := 0; as < asPerGateway; as++ {
				key := asKey(sgp, as)
				processConfig.ApplicationServers = append(processConfig.ApplicationServers,
					m3ua.RemoteASConfig{ID: remoteASID(as), ASKey: &key})
			}
			gatewayConfig.SGPs = append(gatewayConfig.SGPs, processConfig)
		}
		config.SignallingGateways = append(config.SignallingGateways, gatewayConfig)
	}
	if routes == 0 {
		return config
	}
	routing := &m3ua.ASPRoutingConfig{
		SignallingGatewaySelection:        m3ua.RouteSelectionLoadshare,
		SignallingGatewayProcessSelection: make(map[m3ua.SignallingGatewayID]m3ua.RouteSelectionMode, gatewayCount),
		TransferFlowCacheEntries:          m3ua.DefaultTransferFlowCacheEntries,
	}
	for gateway := 0; gateway < gatewayCount; gateway++ {
		routing.SignallingGatewayProcessSelection[gatewayID(gateway)] = m3ua.RouteSelectionLoadshare
		for as := 0; as < asPerGateway; as++ {
			routing.Paths = append(routing.Paths, m3ua.MTPRoutePath{
				ID: pathID(gateway, as), SignallingGateway: gatewayID(gateway),
				ApplicationServers: []m3ua.RemoteASID{remoteASID(as)},
			})
		}
	}
	for route := 0; route < routes; route++ {
		as := route % asPerGateway
		routing.MTPRoutes = append(routing.MTPRoutes, m3ua.MTPRouteConfig{
			ID:                   m3ua.MTPRouteID(fmt.Sprintf("route-%04d", route)),
			DestinationPointCode: routeDestinationBase + uint32(route),
			Paths:                []m3ua.MTPRoutePathID{pathID(0, as), pathID(1, as)},
		})
	}
	config.Routing = routing
	return config
}

func pathID(gateway, as int) m3ua.MTPRoutePathID {
	return m3ua.MTPRoutePathID(fmt.Sprintf("%s/%s", gatewayID(gateway), remoteASID(as)))
}

// ssnmStateConfig is the reference store: 16,384 records in 128 partitions,
// accounted retention capped at 16 MiB, 8 subscriptions of 256 queued events.
// The per-peer budget is one gateway's half of the store.
func ssnmStateConfig() *m3ua.SSNMStateConfig {
	return &m3ua.SSNMStateConfig{
		MaxRecords:             stateRecords,
		MaxBytes:               ssnmMaxBytes,
		MaxRecordsPerPartition: m3ua.DefaultMaxSSNMPartitionRecords,
		MaxRecordsPerPeer:      stateRecords / gatewayCount,
		MaxPartitions:          partitionCount,
		MaxSubscribers:         subscriberCount,
		SubscriptionQueueSize:  subscriptionQueueSize,
		MaxAffectedPointCodes:  maxAffectedPointCodes,
	}
}

func sgpEndpointConfig() *m3ua.SGPConfig {
	return &m3ua.SGPConfig{
		RecoveryQueueMessages:      m3ua.DefaultRecoveryQueueMessages,
		RecoveryQueueBytes:         m3ua.DefaultRecoveryQueueBytes,
		RecoveryQueueTotalMessages: m3ua.DefaultRecoveryQueueTotalMessages,
		RecoveryQueueTotalBytes:    pendingRecoveryTotalBytes,
		MaxSSNMDestinationRecords:  m3ua.DefaultMaxSSNMDestinationRecords,
	}
}

// dataTuple is the fixed routing label of one stable association's ledgered
// DATA in one direction. One SLS per association keeps the flow on one SCTP
// stream, so the ledger can require strict order.
func dataTuple(stableIndex int, towardSGP bool) (m3ua.ASKey, params.ProtocolDataPayload) {
	sgp := stableIndex / stablePerSGP
	index := stableIndex % stablePerSGP
	local := uint32(0x100000 + stableIndex)
	remote := uint32(0x200000 + stableIndex)
	if !towardSGP {
		local, remote = remote, local
	}
	return asKey(sgp, stableASes(index)[0]), params.ProtocolDataPayload{
		OriginatingPointCode:    local,
		DestinationPointCode:    remote,
		ServiceIndicator:        3,
		NetworkIndicator:        2,
		MessagePriority:         0,
		SignallingLinkSelection: uint8(index),
	}
}
