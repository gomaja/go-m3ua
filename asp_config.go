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
// RFC 4666 Section 1.2 makes an Application Server "a logical entity serving a
// specific Routing Key", and has the SGPs of one Signalling Gateway
// "coordinated into a single management view to the SS7 network and to the
// supported Application Servers". The name is local: it is not carried on the
// wire, and equal names in different Signalling Gateways are different
// Application Servers.
type RemoteASID string

// SGASKey is the canonical identity of one Application Server this ASP reaches
// through one Signalling Gateway.
//
// The wire scope that names that Application Server belongs to the individual
// SGP, not to this identity: RFC 4666 Section 1.4.2.1 makes a Routing Context
// "an index into a sending node's Message Distribution Table", so two SGPs of
// one Signalling Gateway may label the same Application Server differently,
// and the same Routing Context value in another Signalling Gateway means
// something else entirely.
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
// local route, as required by RFC 4666 Sections 1.4.2.5 and 5.5.1.1.1, and the
// provisioned path candidates that carry it.
type MTPRouteConfig struct {
	ID                    MTPRouteID
	DestinationPointCode  uint32
	Mask                  uint8
	ServiceIndicators     []uint8
	OriginatingPointCodes []uint32
	// Paths names the provisioned MTPRoutePath candidates this route may use,
	// in preference order. One route may name several Signalling Gateways and
	// several Application Servers, and several routes may name one path.
	Paths []MTPRoutePathID
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

// MTPRoutePathID is the local name of one provisioned outbound path candidate.
type MTPRoutePathID string

// MTPRoutePath is one provisioned outbound candidate: the ordered Application
// Servers of one Signalling Gateway that may carry a local MTP Route.
//
// A path is provisioned once and referenced by name, so several routes share
// one candidate instead of repeating the wire scope that names it. The wire
// scope stays with the SGP that uses it: RFC 4666 Section 1.4.2.1 makes a
// Routing Context "an index into a sending node's Message Distribution Table",
// so two SGPs of one Signalling Gateway may label one Application Server
// differently and the path names the canonical Application Server instead.
//
// ApplicationServers is a preference order, applied within each SGP of the
// Signalling Gateway. The first one an SGP can currently carry is the one that
// SGP uses; the rest are its failback.
type MTPRoutePath struct {
	ID                 MTPRoutePathID
	SignallingGateway  SignallingGatewayID
	ApplicationServers []RemoteASID
}

// ASPRoutingConfig is the optional outbound route inventory of one ASP
// Endpoint. It selects library-managed MTP-TRANSFER: the Endpoint matches an
// MTP routing label to a route, then chooses among the provisioned path
// candidates that carry it.
//
// A path may name an Application Server an SGP binds dynamically. The wire
// Routing Context of such an Application Server is the one the SGP assigns
// during RFC 4666 Section 4.4.1 registration, so it is resolved per Association
// when a request is selected rather than provisioned here.
type ASPRoutingConfig struct {
	// SignallingGatewaySelection chooses among the Signalling Gateways that
	// carry one route.
	SignallingGatewaySelection RouteSelectionMode
	// SignallingGatewayProcessSelection chooses among the SGPs of one
	// Signalling Gateway. Every Signalling Gateway that carries a route needs
	// an entry.
	SignallingGatewayProcessSelection map[SignallingGatewayID]RouteSelectionMode
	// Paths is the provisioned outbound candidate inventory. Routes reference
	// these by name.
	Paths []MTPRoutePath
	// MTPRoutes is the local MTP routing-label inventory. Every MTP Route needs
	// at least one path.
	MTPRoutes []MTPRouteConfig
	// CongestionPolicy filters candidate paths by reported congestion.
	CongestionPolicy ASPCongestionPolicy
	// AllowUnknownDestinations lets a candidate whose destination state this
	// Endpoint has never been told carry traffic.
	//
	// It is off by default, so a destination no Signalling Gateway has reported
	// fails closed with ErrDestinationStateUnknown instead of being presumed
	// reachable. Turning it on never fabricates an availability report and
	// never overrides one: a destination a peer reported unavailable stays
	// unavailable, and a congestion policy still applies.
	AllowUnknownDestinations bool
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

// aspRouteCandidate is one provisioned (path, Application Server) pair a route
// may use. The wire scope it resolves to belongs to the SGP that carries it.
type aspRouteCandidate struct {
	path              MTPRoutePathID
	applicationServer RemoteASID
}

// aspRouteGateway is the ordered candidate list one MTP Route has through one
// Signalling Gateway, flattened from the route's paths in reference order.
type aspRouteGateway struct {
	id         SignallingGatewayID
	candidates []aspRouteCandidate
}

type aspMTPRoute struct {
	id                    MTPRouteID
	destinationPointCode  uint32
	mask                  uint8
	serviceIndicators     []uint8
	originatingPointCodes []uint32
	gateways              []aspRouteGateway
}

// aspRemoteAS is one provisioned Application Server as served by one SGP.
type aspRemoteAS struct {
	id            RemoteASID
	asKey         ASKey
	asKeyStatic   bool
	routingKey    RoutingKey
	routingKeySet bool
}

type aspSGPConfig struct {
	id                 SignallingGatewayProcessID
	applicationServers []aspRemoteAS
	asByID             map[RemoteASID]int
	// asByStaticKey resolves one exact wire scope back to the canonical
	// Application Server it was provisioned for. SSNM arrives labelled with
	// the scope, while the knowledge it carries is owned by the identity.
	asByStaticKey map[ASKey]RemoteASID
	// routeCandidates is the ordered subset of each MTP Route's candidates this
	// SGP can serve at all. Which of them it can serve now is an Association
	// question, answered when a request is selected.
	routeCandidates map[MTPRouteID][]aspRouteCandidate
	// routeOrder lists those MTP Routes in configuration order, so every walk
	// over this SGP's routes is deterministic.
	routeOrder []MTPRouteID
}

// candidatesFor reports the ordered Application Server candidates this SGP may
// use for one MTP Route.
func (sgp aspSGPConfig) candidatesFor(mtpRoute MTPRouteID) []aspRouteCandidate {
	return sgp.routeCandidates[mtpRoute]
}

// carries reports whether this SGP is provisioned to serve one MTP Route.
func (sgp aspSGPConfig) carries(mtpRoute MTPRouteID) bool {
	return len(sgp.routeCandidates[mtpRoute]) > 0
}

type aspSignallingGatewayConfig struct {
	id           SignallingGatewayID
	sgpSelection RouteSelectionMode
	sgps         []aspSGPConfig
}

type aspRoutingConfig struct {
	routingConfigured                       bool
	allowUnknownDestinations                bool
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

// mtpRoute reports one compiled MTP Route. The compiled inventory is written
// once, before any Association can reach it, so it needs no lock.
func (c aspRoutingConfig) mtpRoute(id MTPRouteID) (aspMTPRoute, bool) {
	index, exists := c.mtpRouteByID[id]
	if !exists || index < 0 || index >= len(c.mtpRoutes) {
		return aspMTPRoute{}, false
	}
	return c.mtpRoutes[index], true
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

// asKeysFor resolves the wire scopes one Association may use for one canonical
// Application Server of the SGP it is attached to, in a deterministic order.
//
// A statically provisioned Application Server carries the scope its
// configuration named. A dynamically bound one carries the Routing Context the
// SGP assigned during RFC 4666 Section 4.4.1 registration, so it has no scope
// at all until that registration succeeded and the scope belongs to the
// Association that registered it, not to this inventory.
func (c aspRoutingConfig) asKeysFor(
	association *Association,
	identity SGPIdentity,
	id RemoteASID,
) []ASKey {
	var keys []ASKey
	c.visitASKeys(association, identity, id, func(key ASKey) bool {
		keys = append(keys, key)
		return true
	})
	return keys
}

// visitASKeys calls visit with each scope asKeysFor resolves, in the same
// order, until visit returns false. A statically provisioned Application
// Server, the case every transfer and status projection meets, costs no
// allocation.
func (c aspRoutingConfig) visitASKeys(
	association *Association,
	identity SGPIdentity,
	id RemoteASID,
	visit func(ASKey) bool,
) {
	applicationServer, exists := c.remoteASFor(identity, id)
	if !exists {
		return
	}
	if applicationServer.asKeyStatic {
		visit(applicationServer.asKey)
		return
	}
	for _, key := range association.dynamicASKeysForRemoteAS(id) {
		if !visit(key) {
			return
		}
	}
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
		asByStaticKey:      make(map[ASKey]RemoteASID, len(sgp.ApplicationServers)),
		routeCandidates:    make(map[MTPRouteID][]aspRouteCandidate),
	}
	staticKeys := compiled.asByStaticKey
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
	snapshot.allowUnknownDestinations = routing.AllowUnknownDestinations
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
	paths, err := compileRoutePaths(routing.Paths, snapshot)
	if err != nil {
		return err
	}
	return bindMTPRoutePaths(routing, paths, snapshot)
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
		if len(mtpRoute.Paths) == 0 {
			return invalidASPConfig("MTP Route %q names no path", mtpRoute.ID)
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

// compileRoutePaths compiles the provisioned outbound candidate inventory.
//
// A path names canonical Application Servers rather than wire scopes, so a
// dynamically bound Application Server is provisioned here exactly like a
// statically bound one. RFC 4666 Section 4.4.1 has the SGP assign its Routing
// Context during registration, and that label is resolved per Association when
// a request is selected.
func compileRoutePaths(
	paths []MTPRoutePath,
	snapshot *aspRoutingConfig,
) (map[MTPRoutePathID]aspRoutePath, error) {
	if len(paths) == 0 {
		return nil, invalidASPConfig("no route paths configured")
	}
	compiled := make(map[MTPRoutePathID]aspRoutePath, len(paths))
	for _, path := range paths {
		if path.ID == "" {
			return nil, invalidASPConfig("empty route path ID")
		}
		if _, exists := compiled[path.ID]; exists {
			return nil, invalidASPConfig("duplicate route path %q", path.ID)
		}
		gateway, provisioned := snapshot.signallingGateway(path.SignallingGateway)
		if !provisioned {
			return nil, invalidASPConfig(
				"route path %q references unprovisioned Signalling Gateway %q",
				path.ID, path.SignallingGateway)
		}
		if len(path.ApplicationServers) == 0 {
			return nil, invalidASPConfig("route path %q names no Application Server", path.ID)
		}
		named := make(map[RemoteASID]struct{}, len(path.ApplicationServers))
		for _, applicationServer := range path.ApplicationServers {
			if _, duplicate := named[applicationServer]; duplicate {
				return nil, invalidASPConfig(
					"route path %q names Application Server %q twice", path.ID, applicationServer)
			}
			named[applicationServer] = struct{}{}
			if !gatewayServes(gateway, applicationServer) {
				return nil, invalidASPConfig(
					"route path %q names Application Server %q, which no SGP of Signalling Gateway %q serves",
					path.ID, applicationServer, path.SignallingGateway)
			}
		}
		compiled[path.ID] = aspRoutePath{
			id:                 path.ID,
			signallingGateway:  path.SignallingGateway,
			applicationServers: append([]RemoteASID(nil), path.ApplicationServers...),
		}
	}
	return compiled, nil
}

// aspRoutePath is one compiled outbound candidate.
type aspRoutePath struct {
	id                 MTPRoutePathID
	signallingGateway  SignallingGatewayID
	applicationServers []RemoteASID
}

func (c aspRoutingConfig) signallingGateway(id SignallingGatewayID) (aspSignallingGatewayConfig, bool) {
	for _, gateway := range c.signallingGateways {
		if gateway.id == id {
			return gateway, true
		}
	}
	return aspSignallingGatewayConfig{}, false
}

func gatewayServes(gateway aspSignallingGatewayConfig, applicationServer RemoteASID) bool {
	for _, sgp := range gateway.sgps {
		if _, served := sgp.asByID[applicationServer]; served {
			return true
		}
	}
	return false
}

// bindMTPRoutePaths resolves each route's referenced candidates into the
// ordered per-Signalling-Gateway and per-SGP candidate lists selection reads.
func bindMTPRoutePaths(
	routing *ASPRoutingConfig,
	paths map[MTPRoutePathID]aspRoutePath,
	snapshot *aspRoutingConfig,
) error {
	referenced := make(map[MTPRoutePathID]struct{}, len(paths))
	for index, mtpRoute := range routing.MTPRoutes {
		gateways, err := bindOneMTPRoutePaths(mtpRoute, paths, snapshot, referenced)
		if err != nil {
			return err
		}
		snapshot.mtpRoutes[index].gateways = gateways
		for _, gateway := range gateways {
			if _, configured := routing.SignallingGatewayProcessSelection[gateway.id]; !configured {
				return invalidASPConfig(
					"Signalling Gateway %q carries routes without an SGP selection mode", gateway.id)
			}
		}
	}
	for id := range paths {
		if _, used := referenced[id]; !used {
			return invalidASPConfig("route path %q is referenced by no MTP Route", id)
		}
	}
	return nil
}

func bindOneMTPRoutePaths(
	mtpRoute MTPRouteConfig,
	paths map[MTPRoutePathID]aspRoutePath,
	snapshot *aspRoutingConfig,
	referenced map[MTPRoutePathID]struct{},
) ([]aspRouteGateway, error) {
	gateways := make([]aspRouteGateway, 0, len(mtpRoute.Paths))
	index := make(map[SignallingGatewayID]int, len(mtpRoute.Paths))
	named := make(map[MTPRoutePathID]struct{}, len(mtpRoute.Paths))
	type canonical struct {
		signallingGateway SignallingGatewayID
		applicationServer RemoteASID
	}
	bound := make(map[canonical]struct{}, len(mtpRoute.Paths))
	for _, id := range mtpRoute.Paths {
		path, provisioned := paths[id]
		if !provisioned {
			return nil, invalidASPConfig("MTP Route %q references unprovisioned route path %q", mtpRoute.ID, id)
		}
		if _, duplicate := named[id]; duplicate {
			return nil, invalidASPConfig("MTP Route %q references route path %q twice", mtpRoute.ID, id)
		}
		named[id] = struct{}{}
		referenced[id] = struct{}{}
		position, exists := index[path.signallingGateway]
		if !exists {
			position = len(gateways)
			index[path.signallingGateway] = position
			gateways = append(gateways, aspRouteGateway{id: path.signallingGateway})
		}
		for _, applicationServer := range path.applicationServers {
			key := canonical{signallingGateway: path.signallingGateway, applicationServer: applicationServer}
			if _, duplicate := bound[key]; duplicate {
				return nil, invalidASPConfig(
					"MTP Route %q reaches Application Server %q of Signalling Gateway %q through two paths",
					mtpRoute.ID, applicationServer, path.signallingGateway)
			}
			bound[key] = struct{}{}
			candidate := aspRouteCandidate{path: id, applicationServer: applicationServer}
			gateways[position].candidates = append(gateways[position].candidates, candidate)
			recordSGPRouteCandidate(snapshot, path.signallingGateway, mtpRoute.ID, candidate)
		}
	}
	return gateways, nil
}

// recordSGPRouteCandidate gives every SGP that serves one candidate's
// Application Server its own ordered view of the route.
func recordSGPRouteCandidate(
	snapshot *aspRoutingConfig,
	signallingGateway SignallingGatewayID,
	mtpRoute MTPRouteID,
	candidate aspRouteCandidate,
) {
	for gatewayIndex := range snapshot.signallingGateways {
		if snapshot.signallingGateways[gatewayIndex].id != signallingGateway {
			continue
		}
		for sgpIndex := range snapshot.signallingGateways[gatewayIndex].sgps {
			sgp := &snapshot.signallingGateways[gatewayIndex].sgps[sgpIndex]
			if _, served := sgp.asByID[candidate.applicationServer]; !served {
				continue
			}
			if len(sgp.routeCandidates[mtpRoute]) == 0 {
				sgp.routeOrder = append(sgp.routeOrder, mtpRoute)
			}
			sgp.routeCandidates[mtpRoute] = append(sgp.routeCandidates[mtpRoute], candidate)
			snapshot.sgpByIdentity[SGPIdentity{
				SignallingGateway:        signallingGateway,
				SignallingGatewayProcess: sgp.id,
			}] = *sgp
		}
		return
	}
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
