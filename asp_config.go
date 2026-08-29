// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import "fmt"

const (
	// DefaultTransferFlowCacheEntries bounds stable ASP MTP traffic-flow
	// assignments retained by one Endpoint.
	DefaultTransferFlowCacheEntries = 65536
	// DefaultMTPIndicationQueueSize bounds derived MTP3-User indications retained
	// while the application is not reading them.
	DefaultMTPIndicationQueueSize = 256
	// DefaultMaxAffectedPointCodesPerSSNM bounds work caused by one peer SSNM
	// message before any route state is changed.
	DefaultMaxAffectedPointCodesPerSSNM = 1024
	// DefaultMaxSSNMStateRecordsPerRoute bounds retained state for one SG and
	// MTP Route.
	DefaultMaxSSNMStateRecordsPerRoute = 2048
	// DefaultMaxSSNMStateRecords bounds retained SSNM route state at one ASP
	// Endpoint.
	DefaultMaxSSNMStateRecords = 16384
)

// SignallingGatewayID is the local identity of an RFC 4666 Signalling Gateway.
type SignallingGatewayID string

// SignallingGatewayProcessID is the local identity of an RFC 4666 Signalling
// Gateway Process within one Signalling Gateway.
type SignallingGatewayProcessID string

// MTPRouteID identifies one local ASP MTP route. It is distinct from a
// peer-specific Routing Key, which is represented by SGPRoute.AS.
type MTPRouteID string

// SGPIdentity identifies one Signalling Gateway Process and its containing
// Signalling Gateway.
type SGPIdentity struct {
	SignallingGateway        SignallingGatewayID
	SignallingGatewayProcess SignallingGatewayProcessID
}

// RouteSelectionMode controls deterministic selection among SGs or among the
// SGPs of one SG. The modes are the redundancy models described by RFC 4666
// Appendix A.2.2.
type RouteSelectionMode uint8

const (
	// RouteSelectionPrimaryBackup chooses the first eligible configured route.
	RouteSelectionPrimaryBackup RouteSelectionMode = iota + 1
	// RouteSelectionLoadshare assigns each MTP traffic flow to one eligible route.
	RouteSelectionLoadshare
	// RouteSelectionBroadcast sends each MTP traffic flow through every eligible route.
	RouteSelectionBroadcast
)

func validRouteSelectionMode(mode RouteSelectionMode) bool {
	switch mode {
	case RouteSelectionPrimaryBackup, RouteSelectionLoadshare, RouteSelectionBroadcast:
		return true
	default:
		return false
	}
}

// ASPCongestionPolicy decides whether an ASP may use a route carrying the
// reported congestion state for a Protocol Data Message Priority. A nil policy
// permits every reachable route. The policy runs without Endpoint locks and
// may be evaluated concurrently for candidate levels before route selection;
// it must not depend on invocation count.
type ASPCongestionPolicy func(messagePriority, congestionLevel uint8, levelSet bool) bool

// MTPRouteConfig describes the MTP routing-label fields used to select one
// local route, as required by RFC 4666 Sections 1.4.2.5 and 5.5.1.1.1.
type MTPRouteConfig struct {
	ID                    MTPRouteID
	DestinationPointCode  uint32
	Mask                  uint8
	ServiceIndicators     []uint8
	OriginatingPointCodes []uint32
}

// SGPRoute maps one local MTP Route to the peer-specific Application Server
// identity used with an SGP. Network Appearance and Routing Context are not
// global route identifiers; their presence and values belong to this mapping.
type SGPRoute struct {
	MTPRoute MTPRouteID
	AS       ASKey
}

// SignallingGatewayProcessConfig configures one SGP and the traffic it can
// transfer for this ASP.
type SignallingGatewayProcessConfig struct {
	ID     SignallingGatewayProcessID
	Routes []SGPRoute
}

// SignallingGatewayConfig configures one SG and its ordered SGP inventory.
type SignallingGatewayConfig struct {
	ID           SignallingGatewayID
	SGPSelection RouteSelectionMode
	SGPs         []SignallingGatewayProcessConfig
}

// ASPConfig configures route state and MTP-TRANSFER selection shared by every
// Association owned by one ASP Endpoint. Zero-valued size and record limits use
// their corresponding Default constants.
type ASPConfig struct {
	SignallingGatewaySelection RouteSelectionMode
	MTPRoutes                  []MTPRouteConfig
	SignallingGateways         []SignallingGatewayConfig
	CongestionPolicy           ASPCongestionPolicy
	TransferFlowCacheEntries   int
	MTPIndicationQueueSize     int
	// MaxAffectedPointCodesPerSSNM bounds the number of Affected Point Code
	// values accepted from one SSNM message before route matching.
	MaxAffectedPointCodesPerSSNM int
	// MaxSSNMStateRecordsPerRoute bounds retained availability and congestion
	// records for one route between the ASP and an SG. Availability and
	// congestion records are independent and each consumes one record.
	MaxSSNMStateRecordsPerRoute int
	// MaxSSNMStateRecords bounds those retained route records across the ASP
	// Endpoint.
	MaxSSNMStateRecords int
}

type aspMTPRoute struct {
	id                    MTPRouteID
	destinationPointCode  uint32
	mask                  uint8
	serviceIndicators     []uint8
	originatingPointCodes []uint32
}

type aspSGPRoute struct {
	mtpRoute MTPRouteID
	as       ASKey
}

type aspSGPConfig struct {
	id     SignallingGatewayProcessID
	routes []aspSGPRoute
}

type aspSignallingGatewayConfig struct {
	id           SignallingGatewayID
	sgpSelection RouteSelectionMode
	sgps         []aspSGPConfig
}

type aspRoutingConfig struct {
	signallingGatewaySelection   RouteSelectionMode
	mtpRoutes                    []aspMTPRoute
	mtpRouteByID                 map[MTPRouteID]int
	signallingGateways           []aspSignallingGatewayConfig
	sgpByIdentity                map[SGPIdentity]aspSGPConfig
	congestionPolicy             ASPCongestionPolicy
	transferFlowCacheEntries     int
	mtpIndicationQueueSize       int
	maxAffectedPointCodesPerSSNM int
	maxSSNMStateRecordsPerRoute  int
	maxSSNMStateRecords          int
}

func snapshotASPConfig(config *ASPConfig) (aspRoutingConfig, error) {
	if config == nil {
		return aspRoutingConfig{}, nil
	}
	if !validRouteSelectionMode(config.SignallingGatewaySelection) {
		return aspRoutingConfig{}, invalidASPConfig("unsupported Signalling Gateway selection mode %d", config.SignallingGatewaySelection)
	}
	if len(config.MTPRoutes) == 0 {
		return aspRoutingConfig{}, invalidASPConfig("no MTP Routes configured")
	}
	if len(config.SignallingGateways) == 0 {
		return aspRoutingConfig{}, invalidASPConfig("no Signalling Gateways configured")
	}
	if config.TransferFlowCacheEntries < 0 {
		return aspRoutingConfig{}, invalidASPConfig("negative transfer flow cache size %d", config.TransferFlowCacheEntries)
	}
	if config.MTPIndicationQueueSize < 0 {
		return aspRoutingConfig{}, invalidASPConfig("negative MTP indication queue size %d", config.MTPIndicationQueueSize)
	}
	if config.MaxAffectedPointCodesPerSSNM < 0 {
		return aspRoutingConfig{}, invalidASPConfig("negative Affected Point Codes per SSNM %d", config.MaxAffectedPointCodesPerSSNM)
	}
	if config.MaxSSNMStateRecordsPerRoute < 0 {
		return aspRoutingConfig{}, invalidASPConfig("negative SSNM state records per route %d", config.MaxSSNMStateRecordsPerRoute)
	}
	if config.MaxSSNMStateRecords < 0 {
		return aspRoutingConfig{}, invalidASPConfig("negative SSNM state records %d", config.MaxSSNMStateRecords)
	}

	snapshot := aspRoutingConfig{
		signallingGatewaySelection:   config.SignallingGatewaySelection,
		mtpRoutes:                    make([]aspMTPRoute, 0, len(config.MTPRoutes)),
		mtpRouteByID:                 make(map[MTPRouteID]int, len(config.MTPRoutes)),
		signallingGateways:           make([]aspSignallingGatewayConfig, 0, len(config.SignallingGateways)),
		sgpByIdentity:                make(map[SGPIdentity]aspSGPConfig),
		congestionPolicy:             config.CongestionPolicy,
		transferFlowCacheEntries:     config.TransferFlowCacheEntries,
		mtpIndicationQueueSize:       config.MTPIndicationQueueSize,
		maxAffectedPointCodesPerSSNM: config.MaxAffectedPointCodesPerSSNM,
		maxSSNMStateRecordsPerRoute:  config.MaxSSNMStateRecordsPerRoute,
		maxSSNMStateRecords:          config.MaxSSNMStateRecords,
	}
	if snapshot.transferFlowCacheEntries == 0 {
		snapshot.transferFlowCacheEntries = DefaultTransferFlowCacheEntries
	}
	if snapshot.mtpIndicationQueueSize == 0 {
		snapshot.mtpIndicationQueueSize = DefaultMTPIndicationQueueSize
	}
	if snapshot.maxAffectedPointCodesPerSSNM == 0 {
		snapshot.maxAffectedPointCodesPerSSNM = DefaultMaxAffectedPointCodesPerSSNM
	}
	if snapshot.maxSSNMStateRecordsPerRoute == 0 {
		snapshot.maxSSNMStateRecordsPerRoute = DefaultMaxSSNMStateRecordsPerRoute
	}
	if snapshot.maxSSNMStateRecords == 0 {
		snapshot.maxSSNMStateRecords = DefaultMaxSSNMStateRecords
	}

	for _, mtpRoute := range config.MTPRoutes {
		if mtpRoute.ID == "" {
			return aspRoutingConfig{}, invalidASPConfig("empty MTP Route ID")
		}
		if _, exists := snapshot.mtpRouteByID[mtpRoute.ID]; exists {
			return aspRoutingConfig{}, invalidASPConfig("duplicate MTP Route %q", mtpRoute.ID)
		}
		if mtpRoute.DestinationPointCode > 0xffffff {
			return aspRoutingConfig{}, invalidASPConfig("MTP Route %q Destination Point Code %#x exceeds 24 bits", mtpRoute.ID, mtpRoute.DestinationPointCode)
		}
		if mtpRoute.Mask > 24 {
			return aspRoutingConfig{}, invalidASPConfig("MTP Route %q mask %d exceeds 24 bits", mtpRoute.ID, mtpRoute.Mask)
		}
		if mtpRoute.DestinationPointCode&lowPointCodeBits(mtpRoute.Mask) != 0 {
			return aspRoutingConfig{}, invalidASPConfig("MTP Route %q Destination Point Code %#x is not aligned to mask %d", mtpRoute.ID, mtpRoute.DestinationPointCode, mtpRoute.Mask)
		}
		if duplicateUint8(mtpRoute.ServiceIndicators) {
			return aspRoutingConfig{}, invalidASPConfig("MTP Route %q contains duplicate Service Indicators", mtpRoute.ID)
		}
		if duplicateOrInvalidPointCode(mtpRoute.OriginatingPointCodes) {
			return aspRoutingConfig{}, invalidASPConfig("MTP Route %q contains duplicate or invalid Originating Point Codes", mtpRoute.ID)
		}
		compiled := aspMTPRoute{
			id:                    mtpRoute.ID,
			destinationPointCode:  mtpRoute.DestinationPointCode,
			mask:                  mtpRoute.Mask,
			serviceIndicators:     append([]uint8(nil), mtpRoute.ServiceIndicators...),
			originatingPointCodes: append([]uint32(nil), mtpRoute.OriginatingPointCodes...),
		}
		snapshot.mtpRouteByID[compiled.id] = len(snapshot.mtpRoutes)
		snapshot.mtpRoutes = append(snapshot.mtpRoutes, compiled)
	}

	signallingGatewayIDs := make(map[SignallingGatewayID]struct{}, len(config.SignallingGateways))
	mappedMTPRoutes := make(map[MTPRouteID]struct{}, len(config.MTPRoutes))
	for _, signallingGateway := range config.SignallingGateways {
		if signallingGateway.ID == "" {
			return aspRoutingConfig{}, invalidASPConfig("empty Signalling Gateway ID")
		}
		if _, exists := signallingGatewayIDs[signallingGateway.ID]; exists {
			return aspRoutingConfig{}, invalidASPConfig("duplicate Signalling Gateway %q", signallingGateway.ID)
		}
		signallingGatewayIDs[signallingGateway.ID] = struct{}{}
		if !validRouteSelectionMode(signallingGateway.SGPSelection) {
			return aspRoutingConfig{}, invalidASPConfig("Signalling Gateway %q has unsupported SGP selection mode %d", signallingGateway.ID, signallingGateway.SGPSelection)
		}
		if len(signallingGateway.SGPs) == 0 {
			return aspRoutingConfig{}, invalidASPConfig("Signalling Gateway %q has no SGPs", signallingGateway.ID)
		}

		compiledGateway := aspSignallingGatewayConfig{
			id:           signallingGateway.ID,
			sgpSelection: signallingGateway.SGPSelection,
			sgps:         make([]aspSGPConfig, 0, len(signallingGateway.SGPs)),
		}
		sgpIDs := make(map[SignallingGatewayProcessID]struct{}, len(signallingGateway.SGPs))
		for _, sgp := range signallingGateway.SGPs {
			if sgp.ID == "" {
				return aspRoutingConfig{}, invalidASPConfig("Signalling Gateway %q has an empty SGP ID", signallingGateway.ID)
			}
			if _, exists := sgpIDs[sgp.ID]; exists {
				return aspRoutingConfig{}, invalidASPConfig("Signalling Gateway %q has duplicate SGP %q", signallingGateway.ID, sgp.ID)
			}
			sgpIDs[sgp.ID] = struct{}{}
			if len(sgp.Routes) == 0 {
				return aspRoutingConfig{}, invalidASPConfig("SGP %q in Signalling Gateway %q has no routes", sgp.ID, signallingGateway.ID)
			}

			compiledSGP := aspSGPConfig{id: sgp.ID, routes: make([]aspSGPRoute, 0, len(sgp.Routes))}
			routeKeys := make(map[MTPRouteID]struct{}, len(sgp.Routes))
			for _, route := range sgp.Routes {
				if _, exists := snapshot.mtpRouteByID[route.MTPRoute]; !exists {
					return aspRoutingConfig{}, invalidASPConfig("SGP %q in Signalling Gateway %q references unknown MTP Route %q", sgp.ID, signallingGateway.ID, route.MTPRoute)
				}
				if _, exists := routeKeys[route.MTPRoute]; exists {
					return aspRoutingConfig{}, invalidASPConfig("SGP %q in Signalling Gateway %q has duplicate mapping for MTP Route %q", sgp.ID, signallingGateway.ID, route.MTPRoute)
				}
				routeKeys[route.MTPRoute] = struct{}{}
				mappedMTPRoutes[route.MTPRoute] = struct{}{}
				compiledSGP.routes = append(compiledSGP.routes, aspSGPRoute{mtpRoute: route.MTPRoute, as: route.AS})
			}
			compiledGateway.sgps = append(compiledGateway.sgps, compiledSGP)
			snapshot.sgpByIdentity[SGPIdentity{
				SignallingGateway:        signallingGateway.ID,
				SignallingGatewayProcess: sgp.ID,
			}] = compiledSGP
		}
		snapshot.signallingGateways = append(snapshot.signallingGateways, compiledGateway)
	}
	for _, mtpRoute := range snapshot.mtpRoutes {
		if _, exists := mappedMTPRoutes[mtpRoute.id]; !exists {
			return aspRoutingConfig{}, invalidASPConfig("MTP Route %q has no SGP mapping", mtpRoute.id)
		}
	}

	return snapshot, nil
}

func invalidASPConfig(format string, values ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidASPConfig, fmt.Sprintf(format, values...))
}

func lowPointCodeBits(mask uint8) uint32 {
	if mask == 0 {
		return 0
	}
	return uint32(1<<mask) - 1
}

func duplicateUint8(values []uint8) bool {
	seen := make(map[uint8]struct{}, len(values))
	for _, value := range values {
		if _, exists := seen[value]; exists {
			return true
		}
		seen[value] = struct{}{}
	}
	return false
}

func duplicateOrInvalidPointCode(values []uint32) bool {
	seen := make(map[uint32]struct{}, len(values))
	for _, value := range values {
		if value > 0xffffff {
			return true
		}
		if _, exists := seen[value]; exists {
			return true
		}
		seen[value] = struct{}{}
	}
	return false
}
