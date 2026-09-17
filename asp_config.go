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
	// DefaultMaxSSNMDestinationRecords bounds the destination records one SSNM
	// state store retains. It counts every Affected Point Code a peer reports,
	// including the ones outside every provisioned MTP Route, which the
	// route-state limits above never see.
	DefaultMaxSSNMDestinationRecords = 16384
)

// SignallingGatewayID is the local identity of an RFC 4666 Signalling Gateway.
type SignallingGatewayID string

// SignallingGatewayProcessID is the local identity of an RFC 4666 Signalling
// Gateway Process within one Signalling Gateway.
type SignallingGatewayProcessID string

// RemoteASID is the local name this ASP gives one Application Server it
// reaches through one Signalling Gateway.
//
// RFC 4666 Section 1.4.2 makes an Application Server a logical entity of the
// signalling network, reached through the SGPs of its Signalling Gateway. The
// name is local: it is not carried on the wire, and equal names in different
// Signalling Gateways are different Application Servers.
type RemoteASID string

// SGASKey is the canonical identity of one Application Server this ASP reaches
// through one Signalling Gateway.
//
// The wire scope that names that Application Server belongs to the individual
// SGP, not to this identity: RFC 4666 Section 3.6.1 makes Routing Context a
// label the peer assigns, so two SGPs of one Signalling Gateway may label the
// same Application Server differently, and the same Routing Context value in
// another Signalling Gateway means something else entirely.
type SGASKey struct {
	SignallingGateway SignallingGatewayID
	ApplicationServer RemoteASID
}

// MTPRouteID identifies one local ASP MTP route. It is distinct from a
// peer-specific Routing Key, which one SGP binds to an Application Server.
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

// RemoteASConfig binds one Application Server served by one SGP to the wire
// scope that names it on that SGP's Association.
//
// Exactly one binding applies. ASKey is the statically provisioned scope; a
// non-nil zero ASKey is an explicit contextless Application Server, the one
// RFC 4666 Section 3.6.1 describes when a Routing Context is not needed
// because the SGP serves a single Routing Key, and is not missing
// configuration. RoutingKey is the dynamic binding of RFC 4666 Section 4.4.1:
// the wire Routing Context is the one the SGP assigns during registration.
type RemoteASConfig struct {
	ID         RemoteASID
	ASKey      *ASKey
	RoutingKey *RoutingKey
}

// SignallingGatewayProcessConfig provisions one SGP peer and the Application
// Servers it serves for this ASP.
type SignallingGatewayProcessConfig struct {
	ID                 SignallingGatewayProcessID
	ApplicationServers []RemoteASConfig
}

// SignallingGatewayConfig provisions one SG peer and its ordered SGP
// inventory.
type SignallingGatewayConfig struct {
	ID   SignallingGatewayID
	SGPs []SignallingGatewayProcessConfig
}

// MTPRouteBinding carries one local MTP Route over one canonical Signalling
// Gateway and Application Server. Several bindings may name one Application
// Server, and several may name one MTP Route.
type MTPRouteBinding struct {
	MTPRoute MTPRouteID
	AS       SGASKey
}

// ASPRoutingConfig is the optional outbound route inventory of one ASP
// Endpoint. It selects library-managed MTP-TRANSFER: the Endpoint matches an
// MTP routing label to a route, then chooses among the Signalling Gateways and
// SGPs that carry it.
//
// A route may only name an Application Server that every SGP serving it binds
// statically. A dynamically bound Application Server has no wire Routing
// Context until RFC 4666 Section 4.4.1 registration assigns one, so a route
// through it would be a configuration that never carries traffic.
type ASPRoutingConfig struct {
	// SignallingGatewaySelection chooses among the Signalling Gateways that
	// carry one route.
	SignallingGatewaySelection RouteSelectionMode
	// SignallingGatewayProcessSelection chooses among the SGPs of one
	// Signalling Gateway. Every Signalling Gateway that carries a route needs
	// an entry.
	SignallingGatewayProcessSelection map[SignallingGatewayID]RouteSelectionMode
	// MTPRoutes is the local MTP routing-label inventory.
	MTPRoutes []MTPRouteConfig
	// Routes binds those routes to the canonical Application Servers that
	// carry them. Every MTP Route needs at least one binding.
	Routes []MTPRouteBinding
	// CongestionPolicy filters candidate routes by reported congestion.
	CongestionPolicy ASPCongestionPolicy
	// TransferFlowCacheEntries bounds stable traffic-flow assignments. Zero
	// selects DefaultTransferFlowCacheEntries.
	TransferFlowCacheEntries int
}

// ASPConfig provisions the peers and Application Servers of one ASP Endpoint,
// and optionally its outbound routes. Zero-valued size and record limits use
// their corresponding Default constants.
//
// A nil ASPConfig keeps the Endpoint's Associations standalone: they need no
// provisioned SGP identity and the application owns everything outside one
// Association. A non-nil ASPConfig provisions peers, so every Association of
// that Endpoint must name a provisioned SGP.
type ASPConfig struct {
	// SignallingGateways provisions the peers of this ASP and the Application
	// Servers each of their SGPs serves. It is required.
	SignallingGateways []SignallingGatewayConfig
	// Routing is the optional outbound route inventory. A nil Routing selects
	// application-managed routing: the library keeps peer and Application
	// Server inventory, procedures and authorization, and the application owns
	// outbound candidate selection.
	Routing *ASPRoutingConfig
	// MTPIndicationQueueSize bounds derived MTP3-User indications retained
	// while the application is not reading them.
	MTPIndicationQueueSize int
	// MaxAffectedPointCodesPerSSNM bounds the number of Affected Point Code
	// values accepted from one SSNM message before route matching.
	MaxAffectedPointCodesPerSSNM int
	// MaxSSNMStateRecordsPerRoute bounds retained availability and congestion
	// records for one route between the ASP and an SG. Availability and
	// congestion records are independent and each consumes one record.
	MaxSSNMStateRecordsPerRoute int
	// MaxSSNMStateRecordsPerSignallingGateway reserves a separate retained-state
	// budget for every provisioned SG. Zero partitions MaxSSNMStateRecords
	// equally across the configured SGs.
	MaxSSNMStateRecordsPerSignallingGateway int
	// MaxSSNMStateRecords bounds those retained route records across the ASP
	// Endpoint.
	MaxSSNMStateRecords int
	// MaxSSNMDestinationRecords bounds the destination records retained by the
	// SSNM state store of one Association. The limits above count only records
	// that intersect a provisioned MTP Route; this one counts every Affected
	// Point Code the peer reports, so a peer cannot grow retained state without
	// bound by naming destinations this ASP has no route to.
	MaxSSNMDestinationRecords int
}

type aspMTPRoute struct {
	id                    MTPRouteID
	destinationPointCode  uint32
	mask                  uint8
	serviceIndicators     []uint8
	originatingPointCodes []uint32
}

// aspRemoteAS is one provisioned Application Server as served by one SGP.
type aspRemoteAS struct {
	id            RemoteASID
	asKey         ASKey
	asKeyStatic   bool
	routingKey    RoutingKey
	routingKeySet bool
}

type aspSGPRoute struct {
	mtpRoute          MTPRouteID
	applicationServer RemoteASID
	as                ASKey
}

type aspSGPConfig struct {
	id                 SignallingGatewayProcessID
	applicationServers []aspRemoteAS
	asByID             map[RemoteASID]int
	routes             []aspSGPRoute
}

type aspSignallingGatewayConfig struct {
	id           SignallingGatewayID
	sgpSelection RouteSelectionMode
	sgps         []aspSGPConfig
}

type aspRoutingConfig struct {
	routingConfigured                       bool
	signallingGatewaySelection              RouteSelectionMode
	mtpRoutes                               []aspMTPRoute
	mtpRouteByID                            map[MTPRouteID]int
	signallingGateways                      []aspSignallingGatewayConfig
	sgpByIdentity                           map[SGPIdentity]aspSGPConfig
	congestionPolicy                        ASPCongestionPolicy
	transferFlowCacheEntries                int
	mtpIndicationQueueSize                  int
	maxAffectedPointCodesPerSSNM            int
	maxSSNMStateRecordsPerRoute             int
	maxSSNMStateRecordsPerSignallingGateway int
	maxSSNMStateRecords                     int
	maxSSNMDestinationRecords               int
}

// staticASKeyFor reports the statically provisioned wire scope one SGP uses
// for one canonical Application Server.
func (c aspRoutingConfig) staticASKeyFor(identity SGPIdentity, id RemoteASID) (ASKey, bool) {
	applicationServer, exists := c.remoteASFor(identity, id)
	if !exists || !applicationServer.asKeyStatic {
		return ASKey{}, false
	}
	return applicationServer.asKey, true
}

func (c aspRoutingConfig) remoteASFor(identity SGPIdentity, id RemoteASID) (aspRemoteAS, bool) {
	sgp, exists := c.sgpByIdentity[identity]
	if !exists {
		return aspRemoteAS{}, false
	}
	index, served := sgp.asByID[id]
	if !served {
		return aspRemoteAS{}, false
	}
	return sgp.applicationServers[index], true
}

func snapshotASPConfig(config *ASPConfig) (aspRoutingConfig, error) {
	if config == nil {
		return aspRoutingConfig{}, nil
	}
	snapshot, err := snapshotASPLimits(config)
	if err != nil {
		return aspRoutingConfig{}, err
	}
	if err := compilePeerInventory(config, &snapshot); err != nil {
		return aspRoutingConfig{}, err
	}
	if err := compileRoutingInventory(config.Routing, &snapshot); err != nil {
		return aspRoutingConfig{}, err
	}
	return snapshot, nil
}

func snapshotASPLimits(config *ASPConfig) (aspRoutingConfig, error) {
	if len(config.SignallingGateways) == 0 {
		return aspRoutingConfig{}, invalidASPConfig("no Signalling Gateways configured")
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
	if config.MaxSSNMStateRecordsPerSignallingGateway < 0 {
		return aspRoutingConfig{}, invalidASPConfig("negative SSNM state records per Signalling Gateway %d", config.MaxSSNMStateRecordsPerSignallingGateway)
	}
	if config.MaxSSNMStateRecords < 0 {
		return aspRoutingConfig{}, invalidASPConfig("negative SSNM state records %d", config.MaxSSNMStateRecords)
	}
	if config.MaxSSNMDestinationRecords < 0 {
		return aspRoutingConfig{}, invalidASPConfig("negative SSNM destination records %d", config.MaxSSNMDestinationRecords)
	}

	snapshot := aspRoutingConfig{
		signallingGateways:                      make([]aspSignallingGatewayConfig, 0, len(config.SignallingGateways)),
		sgpByIdentity:                           make(map[SGPIdentity]aspSGPConfig),
		mtpRouteByID:                            make(map[MTPRouteID]int),
		mtpIndicationQueueSize:                  config.MTPIndicationQueueSize,
		maxAffectedPointCodesPerSSNM:            config.MaxAffectedPointCodesPerSSNM,
		maxSSNMStateRecordsPerRoute:             config.MaxSSNMStateRecordsPerRoute,
		maxSSNMStateRecordsPerSignallingGateway: config.MaxSSNMStateRecordsPerSignallingGateway,
		maxSSNMStateRecords:                     config.MaxSSNMStateRecords,
		maxSSNMDestinationRecords:               config.MaxSSNMDestinationRecords,
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
	if snapshot.maxSSNMDestinationRecords == 0 {
		snapshot.maxSSNMDestinationRecords = DefaultMaxSSNMDestinationRecords
	}
	if snapshot.maxSSNMStateRecords < len(config.SignallingGateways) {
		return aspRoutingConfig{}, invalidASPConfig(
			"SSNM state record limit %d cannot reserve one record for each of %d Signalling Gateways",
			snapshot.maxSSNMStateRecords, len(config.SignallingGateways))
	}
	if snapshot.maxSSNMStateRecordsPerSignallingGateway == 0 {
		snapshot.maxSSNMStateRecordsPerSignallingGateway =
			snapshot.maxSSNMStateRecords / len(config.SignallingGateways)
	}
	if snapshot.maxSSNMStateRecordsPerSignallingGateway >
		snapshot.maxSSNMStateRecords/len(config.SignallingGateways) {
		return aspRoutingConfig{}, invalidASPConfig(
			"%d SSNM state records per Signalling Gateway cannot fit %d reservations in Endpoint limit %d",
			snapshot.maxSSNMStateRecordsPerSignallingGateway,
			len(config.SignallingGateways), snapshot.maxSSNMStateRecords)
	}
	return snapshot, nil
}

func compilePeerInventory(config *ASPConfig, snapshot *aspRoutingConfig) error {
	signallingGatewayIDs := make(map[SignallingGatewayID]struct{}, len(config.SignallingGateways))
	for _, signallingGateway := range config.SignallingGateways {
		if signallingGateway.ID == "" {
			return invalidASPConfig("empty Signalling Gateway ID")
		}
		if _, exists := signallingGatewayIDs[signallingGateway.ID]; exists {
			return invalidASPConfig("duplicate Signalling Gateway %q", signallingGateway.ID)
		}
		signallingGatewayIDs[signallingGateway.ID] = struct{}{}
		if len(signallingGateway.SGPs) == 0 {
			return invalidASPConfig("Signalling Gateway %q has no SGPs", signallingGateway.ID)
		}

		compiledGateway := aspSignallingGatewayConfig{
			id:   signallingGateway.ID,
			sgps: make([]aspSGPConfig, 0, len(signallingGateway.SGPs)),
		}
		sgpIDs := make(map[SignallingGatewayProcessID]struct{}, len(signallingGateway.SGPs))
		for _, sgp := range signallingGateway.SGPs {
			if sgp.ID == "" {
				return invalidASPConfig("Signalling Gateway %q has an empty SGP ID", signallingGateway.ID)
			}
			if _, exists := sgpIDs[sgp.ID]; exists {
				return invalidASPConfig("Signalling Gateway %q has duplicate SGP %q", signallingGateway.ID, sgp.ID)
			}
			sgpIDs[sgp.ID] = struct{}{}
			compiledSGP, err := compileSGPApplicationServers(signallingGateway.ID, sgp)
			if err != nil {
				return err
			}
			compiledGateway.sgps = append(compiledGateway.sgps, compiledSGP)
			snapshot.sgpByIdentity[SGPIdentity{
				SignallingGateway:        signallingGateway.ID,
				SignallingGatewayProcess: sgp.ID,
			}] = compiledSGP
		}
		snapshot.signallingGateways = append(snapshot.signallingGateways, compiledGateway)
	}
	return nil
}

func compileSGPApplicationServers(
	signallingGateway SignallingGatewayID,
	sgp SignallingGatewayProcessConfig,
) (aspSGPConfig, error) {
	if len(sgp.ApplicationServers) == 0 {
		return aspSGPConfig{}, invalidASPConfig(
			"SGP %q in Signalling Gateway %q serves no Application Server", sgp.ID, signallingGateway)
	}
	compiled := aspSGPConfig{
		id:                 sgp.ID,
		applicationServers: make([]aspRemoteAS, 0, len(sgp.ApplicationServers)),
		asByID:             make(map[RemoteASID]int, len(sgp.ApplicationServers)),
	}
	staticKeys := make(map[ASKey]RemoteASID, len(sgp.ApplicationServers))
	dynamicKeys := make([]canonicalRoutingKey, 0, len(sgp.ApplicationServers))
	contextless := false
	for _, applicationServer := range sgp.ApplicationServers {
		if applicationServer.ID == "" {
			return aspSGPConfig{}, invalidASPConfig(
				"SGP %q in Signalling Gateway %q has an unnamed Application Server", sgp.ID, signallingGateway)
		}
		if _, exists := compiled.asByID[applicationServer.ID]; exists {
			return aspSGPConfig{}, invalidASPConfig(
				"SGP %q in Signalling Gateway %q has duplicate Application Server %q",
				sgp.ID, signallingGateway, applicationServer.ID)
		}
		switch {
		case applicationServer.ASKey != nil && applicationServer.RoutingKey != nil:
			return aspSGPConfig{}, invalidASPConfig(
				"Application Server %q of SGP %q in Signalling Gateway %q has both a static ASKey and a Routing Key",
				applicationServer.ID, sgp.ID, signallingGateway)
		case applicationServer.ASKey == nil && applicationServer.RoutingKey == nil:
			return aspSGPConfig{}, invalidASPConfig(
				"Application Server %q of SGP %q in Signalling Gateway %q has no wire binding",
				applicationServer.ID, sgp.ID, signallingGateway)
		}

		entry := aspRemoteAS{id: applicationServer.ID}
		if applicationServer.ASKey != nil {
			key := *applicationServer.ASKey
			if !key.NetworkAppearanceSet && key.NetworkAppearance != 0 {
				return aspSGPConfig{}, invalidASPConfig(
					"Application Server %q of SGP %q in Signalling Gateway %q has Network Appearance %d without presence",
					applicationServer.ID, sgp.ID, signallingGateway, key.NetworkAppearance)
			}
			if !key.RoutingContextSet && key.RoutingContext != 0 {
				return aspSGPConfig{}, invalidASPConfig(
					"Application Server %q of SGP %q in Signalling Gateway %q has Routing Context %d without presence",
					applicationServer.ID, sgp.ID, signallingGateway, key.RoutingContext)
			}
			if owner, exists := staticKeys[key]; exists {
				return aspSGPConfig{}, invalidASPConfig(
					"Application Servers %q and %q of SGP %q in Signalling Gateway %q share one wire scope",
					owner, applicationServer.ID, sgp.ID, signallingGateway)
			}
			staticKeys[key] = applicationServer.ID
			if !key.RoutingContextSet {
				// RFC 4666 Sections 4.3.4.1 to 4.3.4.4 let an ASPTM message
				// omit Routing Context, in which case it applies to every
				// Application Server the Association serves. A contextless
				// Application Server beside any other one therefore cannot be
				// told apart from that whole-Association scope.
				contextless = true
			}
			entry.asKey = key
			entry.asKeyStatic = true
		} else {
			canonical, err := canonicalizeRoutingKey(*applicationServer.RoutingKey)
			if err != nil {
				return aspSGPConfig{}, invalidASPConfig(
					"Application Server %q of SGP %q in Signalling Gateway %q has an invalid Routing Key: %v",
					applicationServer.ID, sgp.ID, signallingGateway, err)
			}
			for _, existing := range dynamicKeys {
				if canonical.overlaps(existing) {
					// RFC 4666 Section 3.6.2 rejects a registration this ASP
					// cannot route uniquely; provisioning one is the same fault.
					return aspSGPConfig{}, invalidASPConfig(
						"Application Server %q of SGP %q in Signalling Gateway %q has a Routing Key overlapping another",
						applicationServer.ID, sgp.ID, signallingGateway)
				}
			}
			dynamicKeys = append(dynamicKeys, canonical)
			entry.routingKey = snapshotRoutingKey(*applicationServer.RoutingKey)
			entry.routingKeySet = true
		}
		compiled.asByID[applicationServer.ID] = len(compiled.applicationServers)
		compiled.applicationServers = append(compiled.applicationServers, entry)
	}
	if contextless && len(compiled.applicationServers) > 1 {
		return aspSGPConfig{}, invalidASPConfig(
			"SGP %q in Signalling Gateway %q serves a contextless Application Server beside %d others",
			sgp.ID, signallingGateway, len(compiled.applicationServers)-1)
	}
	return compiled, nil
}

func compileRoutingInventory(routing *ASPRoutingConfig, snapshot *aspRoutingConfig) error {
	if routing == nil {
		return nil
	}
	if routing.TransferFlowCacheEntries < 0 {
		return invalidASPConfig("negative transfer flow cache size %d", routing.TransferFlowCacheEntries)
	}
	snapshot.routingConfigured = true
	snapshot.signallingGatewaySelection = routing.SignallingGatewaySelection
	snapshot.congestionPolicy = routing.CongestionPolicy
	snapshot.transferFlowCacheEntries = routing.TransferFlowCacheEntries
	if snapshot.transferFlowCacheEntries == 0 {
		snapshot.transferFlowCacheEntries = DefaultTransferFlowCacheEntries
	}
	if !validRouteSelectionMode(routing.SignallingGatewaySelection) {
		return invalidASPConfig("unsupported Signalling Gateway selection mode %d", routing.SignallingGatewaySelection)
	}
	if len(routing.MTPRoutes) == 0 {
		return invalidASPConfig("no MTP Routes configured")
	}
	if err := compileMTPRoutes(routing.MTPRoutes, snapshot); err != nil {
		return err
	}
	if err := applySGPSelection(routing, snapshot); err != nil {
		return err
	}
	return bindMTPRoutes(routing, snapshot)
}

func compileMTPRoutes(mtpRoutes []MTPRouteConfig, snapshot *aspRoutingConfig) error {
	snapshot.mtpRoutes = make([]aspMTPRoute, 0, len(mtpRoutes))
	for _, mtpRoute := range mtpRoutes {
		if mtpRoute.ID == "" {
			return invalidASPConfig("empty MTP Route ID")
		}
		if _, exists := snapshot.mtpRouteByID[mtpRoute.ID]; exists {
			return invalidASPConfig("duplicate MTP Route %q", mtpRoute.ID)
		}
		if mtpRoute.DestinationPointCode > 0xffffff {
			return invalidASPConfig("MTP Route %q Destination Point Code %#x exceeds 24 bits", mtpRoute.ID, mtpRoute.DestinationPointCode)
		}
		if mtpRoute.Mask > 24 {
			return invalidASPConfig("MTP Route %q mask %d exceeds 24 bits", mtpRoute.ID, mtpRoute.Mask)
		}
		if mtpRoute.DestinationPointCode&lowPointCodeBits(mtpRoute.Mask) != 0 {
			return invalidASPConfig("MTP Route %q Destination Point Code %#x is not aligned to mask %d", mtpRoute.ID, mtpRoute.DestinationPointCode, mtpRoute.Mask)
		}
		if duplicateUint8(mtpRoute.ServiceIndicators) {
			return invalidASPConfig("MTP Route %q contains duplicate Service Indicators", mtpRoute.ID)
		}
		if duplicateOrInvalidPointCode(mtpRoute.OriginatingPointCodes) {
			return invalidASPConfig("MTP Route %q contains duplicate or invalid Originating Point Codes", mtpRoute.ID)
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
	return nil
}

func applySGPSelection(routing *ASPRoutingConfig, snapshot *aspRoutingConfig) error {
	provisioned := make(map[SignallingGatewayID]struct{}, len(snapshot.signallingGateways))
	for _, gateway := range snapshot.signallingGateways {
		provisioned[gateway.id] = struct{}{}
	}
	for signallingGateway, mode := range routing.SignallingGatewayProcessSelection {
		if _, exists := provisioned[signallingGateway]; !exists {
			return invalidASPConfig("SGP selection names unprovisioned Signalling Gateway %q", signallingGateway)
		}
		if !validRouteSelectionMode(mode) {
			return invalidASPConfig("Signalling Gateway %q has unsupported SGP selection mode %d", signallingGateway, mode)
		}
	}
	for index := range snapshot.signallingGateways {
		snapshot.signallingGateways[index].sgpSelection =
			routing.SignallingGatewayProcessSelection[snapshot.signallingGateways[index].id]
	}
	return nil
}

func bindMTPRoutes(routing *ASPRoutingConfig, snapshot *aspRoutingConfig) error {
	type sgpRouteKey struct {
		identity SGPIdentity
		mtpRoute MTPRouteID
	}
	bound := make(map[MTPRouteBinding]struct{}, len(routing.Routes))
	boundPerSGP := make(map[sgpRouteKey]RemoteASID, len(routing.Routes))
	routedGateways := make(map[SignallingGatewayID]struct{}, len(routing.Routes))
	mappedMTPRoutes := make(map[MTPRouteID]struct{}, len(routing.MTPRoutes))
	for _, binding := range routing.Routes {
		if _, exists := snapshot.mtpRouteByID[binding.MTPRoute]; !exists {
			return invalidASPConfig("route binding references unknown MTP Route %q", binding.MTPRoute)
		}
		if _, duplicate := bound[binding]; duplicate {
			return invalidASPConfig("duplicate route binding for MTP Route %q and Application Server %q of Signalling Gateway %q",
				binding.MTPRoute, binding.AS.ApplicationServer, binding.AS.SignallingGateway)
		}
		bound[binding] = struct{}{}

		gatewayIndex := -1
		for index, gateway := range snapshot.signallingGateways {
			if gateway.id == binding.AS.SignallingGateway {
				gatewayIndex = index
				break
			}
		}
		if gatewayIndex < 0 {
			return invalidASPConfig("route binding references unprovisioned Signalling Gateway %q", binding.AS.SignallingGateway)
		}
		serving := 0
		for sgpIndex := range snapshot.signallingGateways[gatewayIndex].sgps {
			sgp := &snapshot.signallingGateways[gatewayIndex].sgps[sgpIndex]
			asIndex, served := sgp.asByID[binding.AS.ApplicationServer]
			if !served {
				continue
			}
			serving++
			applicationServer := sgp.applicationServers[asIndex]
			if !applicationServer.asKeyStatic {
				return invalidASPConfig(
					"route binding for MTP Route %q names dynamically bound Application Server %q of SGP %q in Signalling Gateway %q",
					binding.MTPRoute, binding.AS.ApplicationServer, sgp.id, binding.AS.SignallingGateway)
			}
			identity := SGPIdentity{
				SignallingGateway:        binding.AS.SignallingGateway,
				SignallingGatewayProcess: sgp.id,
			}
			routeKey := sgpRouteKey{identity: identity, mtpRoute: binding.MTPRoute}
			if owner, exists := boundPerSGP[routeKey]; exists {
				return invalidASPConfig(
					"SGP %q in Signalling Gateway %q carries MTP Route %q through both Application Servers %q and %q",
					sgp.id, binding.AS.SignallingGateway, binding.MTPRoute, owner, binding.AS.ApplicationServer)
			}
			boundPerSGP[routeKey] = binding.AS.ApplicationServer
			sgp.routes = append(sgp.routes, aspSGPRoute{
				mtpRoute:          binding.MTPRoute,
				applicationServer: binding.AS.ApplicationServer,
				as:                applicationServer.asKey,
			})
			snapshot.sgpByIdentity[identity] = *sgp
		}
		if serving == 0 {
			return invalidASPConfig(
				"route binding references Application Server %q, which no SGP of Signalling Gateway %q serves",
				binding.AS.ApplicationServer, binding.AS.SignallingGateway)
		}
		routedGateways[binding.AS.SignallingGateway] = struct{}{}
		mappedMTPRoutes[binding.MTPRoute] = struct{}{}
	}
	for _, mtpRoute := range snapshot.mtpRoutes {
		if _, exists := mappedMTPRoutes[mtpRoute.id]; !exists {
			return invalidASPConfig("MTP Route %q has no Application Server binding", mtpRoute.id)
		}
	}
	for signallingGateway := range routedGateways {
		if _, configured := routing.SignallingGatewayProcessSelection[signallingGateway]; !configured {
			return invalidASPConfig("Signalling Gateway %q carries routes without an SGP selection mode", signallingGateway)
		}
	}
	return nil
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
