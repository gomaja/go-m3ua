package m3ua

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// CongestionMethod selects the procedures read in Q.704 (07/96) §§11.2.3–11.2.5.
// The application must choose it; zero is invalid. It does not certify a
// national variant or the unavailable covering-note erratum.
type CongestionMethod uint8

const (
	// CongestionInternational uses omitted-level SCON and no message priorities.
	CongestionInternational CongestionMethod = iota + 1
	// CongestionNationalWithPriorities applies the local-user MP gate of §11.2.4.
	CongestionNationalWithPriorities
	// CongestionNationalWithoutPriorities uses levels without priority discards (§11.2.5).
	CongestionNationalWithoutPriorities
)

// CongestionProfile configures one network's method, independently of observed traffic.
type CongestionProfile struct {
	Method CongestionMethod
	// MaxLevel is 1–3 nationally; zero selects 3 as an engineering default.
	// International requires zero.
	MaxLevel uint8
}

// NetworkCongestionProfile matches a resolved network, preserving NA presence.
// A peer-qualified entry takes precedence over an unqualified one.
type NetworkCongestionProfile struct {
	// PeerSGP is allowed only in an ASP Endpoint table and must be provisioned there.
	PeerSGP              *SGPIdentity
	NetworkAppearance    uint32
	NetworkAppearanceSet bool
	Profile              CongestionProfile
}

// CongestionConfig selects immutable method profiles. Default.Method is required
// for every role; there is no inferred method. Duplicate selectors are invalid.
type CongestionConfig struct {
	// MaxStatusDestinations bounds the Endpoint-wide exact-DPC offered-status LRU.
	// Zero selects 4096 as an engineering default; negative values are invalid.
	MaxStatusDestinations int

	// Publication bounds automatic optional SCON and DAUD work; nil selects defaults.
	Publication *CongestionPublicationConfig
	// Validity enables local expiry of received congestion; nil disables it.
	Validity *CongestionValidityConfig
	// Auditing enables optional ASP audits; nil disables them.
	Auditing *DestinationAuditConfig
	Default  CongestionProfile
	Networks []NetworkCongestionProfile
}

// ErrCongestionMethodMismatch reports incompatible profiles for one logical route.
// No DATA is submitted for the incompatible binding.
var ErrCongestionMethodMismatch = errors.New("congestion methods differ across route alternatives")

type congestionMethodConfig struct {
	publication           CongestionPublicationConfig
	maxStatusDestinations int
	validity              time.Duration
	auditing              *DestinationAuditConfig
	defaultProfile        CongestionProfile
	networks              []NetworkCongestionProfile
	routeProfiles         map[MTPRouteID]CongestionProfile
}

func normalizeCongestionProfile(profile CongestionProfile) (CongestionProfile, error) {
	if profile.Method < CongestionInternational || profile.Method > CongestionNationalWithoutPriorities || profile.Method < CongestionNationalWithPriorities && profile.MaxLevel != 0 || profile.MaxLevel > 3 {
		return profile, fmt.Errorf("%w: invalid congestion profile %+v", ErrInvalidRoleConfiguration, profile)
	}
	if profile.Method >= CongestionNationalWithPriorities && profile.MaxLevel == 0 {
		profile.MaxLevel = 3
	}
	return profile, nil
}

func snapshotCongestionProfiles(input []NetworkCongestionProfile) []NetworkCongestionProfile {
	if input == nil {
		return nil
	}
	output := append([]NetworkCongestionProfile(nil), input...)
	for i := range output {
		if output[i].PeerSGP != nil {
			peer := *output[i].PeerSGP
			output[i].PeerSGP = &peer
		}
		output[i].Profile, _ = normalizeCongestionProfile(output[i].Profile)
	}
	return output
}

func validateCongestionProfiles(input []NetworkCongestionProfile, role Role, routes *aspRoutes, association bool) error {
	type selector struct {
		peer       SGPIdentity
		qualified  bool
		appearance uint32
		set        bool
	}
	seen := make(map[selector]bool, len(input))
	for _, entry := range input {
		if _, err := normalizeCongestionProfile(entry.Profile); err != nil {
			return err
		}
		if !entry.NetworkAppearanceSet && entry.NetworkAppearance != 0 {
			return fmt.Errorf("%w: congestion NA value without presence", ErrInvalidRoleConfiguration)
		}
		key := selector{appearance: entry.NetworkAppearance, set: entry.NetworkAppearanceSet}
		if entry.PeerSGP != nil {
			if err := validateIdentityLength("CongestionProfile.PeerSGP.SignallingGateway", string(entry.PeerSGP.SignallingGateway), maxObservationIdentityBytes); err != nil {
				return fmt.Errorf("%w: %w", ErrInvalidRoleConfiguration, err)
			}
			if association || role != RoleASP || routes == nil {
				return fmt.Errorf("%w: peer-qualified congestion selector", ErrInvalidRoleConfiguration)
			}
			if _, ok := routes.config.sgpByIdentity[*entry.PeerSGP]; !ok {
				return fmt.Errorf("%w: unprovisioned congestion peer", ErrInvalidRoleConfiguration)
			}
			key.peer, key.qualified = *entry.PeerSGP, true
		}
		if seen[key] {
			return fmt.Errorf("%w: duplicate congestion selector", ErrInvalidRoleConfiguration)
		}
		seen[key] = true
	}
	return nil
}

func newCongestionMethodConfig(role Role, input *CongestionConfig, routes *aspRoutes) (*congestionMethodConfig, error) {
	if input == nil {
		return nil, fmt.Errorf("%w: an explicit congestion method is required", ErrInvalidRoleConfiguration)
	}
	limit := input.MaxStatusDestinations
	if limit < 0 {
		return nil, fmt.Errorf("%w: negative status destination limit", ErrInvalidRoleConfiguration)
	}
	if limit == 0 {
		limit = 4096
	}
	profile, err := normalizeCongestionProfile(input.Default)
	if err != nil {
		return nil, err
	}
	if err = validateCongestionProfiles(input.Networks, role, routes, false); err != nil {
		return nil, err
	}
	after, audits, err := resolveCongestionAutomation(role, input)
	if err != nil {
		return nil, err
	}
	publication, err := resolveCongestionPublication(input.Publication)
	if err != nil {
		return nil, err
	}
	return &congestionMethodConfig{publication: publication, maxStatusDestinations: limit, validity: after, auditing: audits, defaultProfile: profile, networks: snapshotCongestionProfiles(input.Networks)}, nil
}

func lookupCongestionProfile(table []NetworkCongestionProfile, peer *SGPIdentity, key ASKey) (CongestionProfile, bool) {
	key = normalizeOutboundASKey(key)
	var fallback CongestionProfile
	found := false
	for _, entry := range table {
		if entry.NetworkAppearanceSet != key.NetworkAppearanceSet || entry.NetworkAppearance != key.NetworkAppearance {
			continue
		}
		if entry.PeerSGP != nil {
			if peer != nil && *entry.PeerSGP == *peer {
				return entry.Profile, true
			}
			continue
		}
		fallback, found = entry.Profile, true
	}
	return fallback, found
}

func (cfg *congestionMethodConfig) profile(peer *SGPIdentity, key ASKey) CongestionProfile {
	raw := CongestionProfile{}
	if cfg != nil {
		raw = cfg.defaultProfile
		if selected, ok := lookupCongestionProfile(cfg.networks, peer, key); ok {
			raw = selected
		}
	}
	return raw
}

func (c *Association) congestionProfile(key ASKey) CongestionProfile {
	if c.cfg != nil {
		if selected, ok := lookupCongestionProfile(c.cfg.CongestionProfiles, nil, key); ok {
			return selected
		}
	}
	var cfg *congestionMethodConfig
	var peer *SGPIdentity
	if c.endpoint != nil {
		cfg = c.endpoint.congestion
	}
	if c.cfg != nil {
		peer = c.cfg.PeerSGP
	}
	return cfg.profile(peer, key)
}

// congestionSCONKeys follows the validated binding, including the reverse
// local-directed SCON flow in IPSP Double Exchange (RFC 4666 §5.6.2).
func (c *Association) congestionSCONKeys(na, rc *params.Param, local bool) []ASKey {
	var contexts []uint32
	if rc != nil {
		contexts = rc.RoutingContexts()
	} else if local {
		contexts = c.configuredLocalRoutingContexts()
	} else {
		contexts = c.configuredRoutingContexts()
	}
	contextsSet := len(contexts) > 0
	if len(contexts) == 0 {
		contexts = []uint32{0}
	}
	keys := make([]ASKey, 0, len(contexts))
	for _, context := range contexts {
		key := c.contextlessASKey(local)
		if contextsSet {
			key = c.staticASKeyForRoutingContext(context, local)
			if dynamic, ok := c.dynamicASKey(context, local); ok {
				key = dynamic
			}
			key.RoutingContext, key.RoutingContextSet = context, true
		}
		if !key.NetworkAppearanceSet && na != nil {
			key.NetworkAppearance, key.NetworkAppearanceSet = na.NetworkAppearance(), true
		}
		keys = append(keys, key)
	}
	return keys
}

// RFC 4666 §3.3.1 carries the original MP with unused bits zero. These
// selected procedures use the MP ranges read in Q.704 (07/96) §§11.2.3–11.2.5.
func (profile CongestionProfile) allowsPriority(mp uint8) bool {
	switch profile.Method {
	case CongestionInternational, CongestionNationalWithoutPriorities:
		return mp == 0
	case CongestionNationalWithPriorities:
		return mp <= profile.MaxLevel
	default:
		return false
	}
}

// RFC 4666 §3.4.4 omits Congestion Indications internationally and permits
// national levels 0–3; MaxLevel narrows the configured national method.
func (profile CongestionProfile) allowsLevel(level uint8, set bool) bool {
	switch profile.Method {
	case CongestionInternational:
		return !set
	case CongestionNationalWithPriorities, CongestionNationalWithoutPriorities:
		return !set || level <= profile.MaxLevel
	default:
		return false
	}
}

func (c *Association) validateCongestionSCON(s *messages.SignallingCongestion) error {
	level := uint8(0)
	if s.CongestionIndications != nil {
		level = uint8(s.CongestionIndications.CongestionLevel())
	}
	for _, key := range c.congestionSCONKeys(s.NetworkAppearance, s.RoutingContext, c.isIPSPDoubleExchange()) {
		if !c.congestionProfile(key).allowsLevel(level, s.CongestionIndications != nil) {
			return ErrInvalidParameterValue
		}
	}
	return nil
}

// CongestionProfileMismatch identifies a selected profile incompatible with an
// accepted SCON. Its original level remains in SSNMReport and retained state.
type CongestionProfileMismatch struct {
	AS      ASKey
	Profile CongestionProfile
}

func (c *Association) reportCongestionProfileMismatch(report *SSNMReport) {
	if report.Kind != SSNMSignallingCongestionReport {
		return
	}
	for _, key := range c.congestionSCONKeys(paramAppearance(report.Scope), paramContexts(report.Scope), false) {
		profile := c.congestionProfile(key)
		if !profile.allowsLevel(report.CongestionLevel, report.CongestionLevelSet) {
			report.ProfileMismatches = append(report.ProfileMismatches, CongestionProfileMismatch{AS: key, Profile: profile})
		}
	}
}

func paramAppearance(scope WireScope) *params.Param {
	if scope.NetworkAppearanceSet {
		return params.NewNetworkAppearance(scope.NetworkAppearance)
	}
	return nil
}
func paramContexts(scope WireScope) *params.Param {
	if scope.RoutingContextSet {
		return params.NewRoutingContext(scope.RoutingContexts...)
	}
	return nil
}

// DataCongestionProfileMismatch describes accepted peer DATA or SGP NIF transit,
// never a protocol Error or a request to drop traffic. Scope preserves wire NA/RC.
type DataCongestionProfileMismatch struct {
	AS              ASKey
	Scope           WireScope
	Profile         CongestionProfile
	MessagePriority uint8
	Epoch           uint64
}

// CongestionProfileDiagnostics is bounded association-lifetime DATA diagnostic
// state. ReceivedDataMismatches counts accepted, well-formed profile mismatches
// from peer DATA and SGP NIF transit;
// LastDataMismatch owns its scope slice. Counters are not reset by SCTP restart.
type CongestionProfileDiagnostics struct {
	ReceivedDataMismatches uint64
	LastDataMismatch       *DataCongestionProfileMismatch
}

type congestionProfileDiagnostics struct {
	mu    sync.Mutex
	count uint64
	last  *DataCongestionProfileMismatch
}

// CongestionProfileDiagnostics returns the mismatch count and latest sample.
// ManagementIndications reports only the first DATA mismatch per Association;
// polling this snapshot observes subsequent mismatches without queue flooding.
func (c *Association) CongestionProfileDiagnostics() CongestionProfileDiagnostics {
	c.profileDiagnostics.mu.Lock()
	defer c.profileDiagnostics.mu.Unlock()
	result := CongestionProfileDiagnostics{ReceivedDataMismatches: c.profileDiagnostics.count}
	if c.profileDiagnostics.last != nil {
		last := *c.profileDiagnostics.last
		last.Scope = last.Scope.clone()
		result.LastDataMismatch = &last
	}
	return result
}

func (c *Association) observeDataProfile(msg *DataMessage) {
	profile := c.congestionProfile(msg.AS)
	if profile.allowsPriority(msg.ProtocolData.MessagePriority) {
		return
	}
	mismatch := DataCongestionProfileMismatch{AS: msg.AS, Scope: msg.Scope.clone(), Profile: profile, MessagePriority: msg.ProtocolData.MessagePriority, Epoch: msg.Epoch}
	c.recordDataProfileMismatch(mismatch)
}

func (c *Association) recordDataProfileMismatch(mismatch DataCongestionProfileMismatch) {
	mismatch.Scope = mismatch.Scope.clone()
	c.profileDiagnostics.mu.Lock()
	first := c.profileDiagnostics.count == 0
	c.profileDiagnostics.count++
	c.profileDiagnostics.last = &mismatch
	c.profileDiagnostics.mu.Unlock()
	if first {
		c.notifyManagement(&ManagementIndication{Kind: ManagementCongestionProfileMismatch, CongestionProfileMismatch: &mismatch, Description: "accepted DATA does not suit the configured congestion profile"})
	}
}

// methodPermits implements only the local-User-Part route-set boundary of
// Q.704 (07/96) §11.2.4. Unknown present congestion is conservatively refused
// (engineering policy). The maximum priority survives known excessive levels.
func methodPermits(profile CongestionProfile, mp uint8, status aspSelectionStatus) bool {
	if profile.Method != CongestionNationalWithPriorities {
		return true
	}
	if status.transportUnknown || status.receivedUnknown || status.congested && !status.congestionLevelSet {
		return false
	}
	return !status.congested || mp >= minCongestionLevel(status.congestionLevel, profile.MaxLevel)
}

// The effective transport-to-method mapping is an engineering adaptation of
// RFC 4666 §1.4.6, not evidence of SS7 link discard thresholds. Raw detector
// severity and received reports are retained independently.
func adaptMethodStatus(profile CongestionProfile, status aspSelectionStatus) aspSelectionStatus {
	if profile.Method == CongestionInternational {
		status.congestionLevel, status.congestionLevelSet = 0, false
	}
	if profile.Method >= CongestionNationalWithPriorities && status.congestionLevelSet {
		status.congestionLevel = minCongestionLevel(status.congestionLevel, profile.MaxLevel)
	}
	return status
}

func (c *Association) validateMethodData(key ASKey, payload *params.ProtocolDataPayload) error {
	key = normalizeOutboundASKey(key)
	profile := c.congestionProfile(key)
	if profile.Method < CongestionInternational || profile.Method > CongestionNationalWithoutPriorities {
		return ErrInvalidRoleConfiguration
	}
	// RFC 4666 §3.3.1 carries the original SS7 MP with unused bits zero.
	// The supported relay ceiling is the two-bit fallback (0–3), not certification of every ANSI/TTC national variant.
	if c.role == RoleSGP {
		if payload.MessagePriority > 3 {
			return ErrInvalidParameterValue
		}
		return nil
	}
	if !profile.allowsPriority(payload.MessagePriority) {
		return ErrInvalidParameterValue
	}
	if profile.Method != CongestionNationalWithPriorities {
		return nil
	}

	scope := destinationKey{networkAppearance: key.NetworkAppearance, networkAppearanceSet: key.NetworkAppearanceSet, routingContext: key.RoutingContext, routingContextSet: key.RoutingContextSet}
	received, known := c.destinations.lookupRange(scope, payload.DestinationPointCode, 0)
	status := aspSelectionStatus{}
	if known {
		status.congested = received.Congestion.Congested
		status.congestionLevel = received.Congestion.Level
		status.congestionLevelSet = received.Congestion.LevelSet
	}
	if _, canonical := c.canonicalRemoteASFor(key); c.ssnmStore() != nil && (canonical || c.ssnmStore().validity > 0) {
		if canonical {
			// Direct DATA shares its configured SG/AS view across peer-local
			// labels, just as route selection does (RFC 4666 §§1.2, 3.3.1).
			status = c.ssnmStore().destinationKnowledge(c.ssnmPartitionFor(key), payload.DestinationPointCode)
		} else {
			status = c.ssnmStore().destinationKnowledge(c.ssnmPartitionFor(key), payload.DestinationPointCode, key)
		}
	}
	status = methodTransportStatus(c, status)
	if !methodPermits(profile, payload.MessagePriority, status) {
		return fmt.Errorf("%w: local-user priority below effective congestion", ErrNoMTPRoute)
	}
	return nil
}

func methodTransportStatus(c *Association, status aspSelectionStatus) aspSelectionStatus {
	t := transportASPStatus(c)
	status.transportUnknown = t.transportUnknown
	if t.congested && (!status.congested || status.congestionLevelSet && t.congestionLevel > status.congestionLevel) {
		status.congested = true
		status.congestionLevel = t.congestionLevel
		status.congestionLevelSet = t.congestionLevelSet
	}
	return status
}

func minCongestionLevel(a, b uint8) uint8 {
	if a < b {
		return a
	}
	return b
}

func (cfg *congestionMethodConfig) validateRoutes(routes *aspRoutes) error {
	if routes == nil {
		return nil
	}
	cfg.routeProfiles = make(map[MTPRouteID]CongestionProfile, len(routes.config.mtpRoutes))
	for _, route := range routes.config.mtpRoutes {
		var expected CongestionProfile
		found := false
		for _, gateway := range route.gateways {
			for identity, sgp := range routes.config.sgpByIdentity {
				if identity.SignallingGateway != gateway.id {
					continue
				}
				for _, candidate := range sgp.candidatesFor(route.id) {
					remote, ok := routes.config.remoteASFor(identity, candidate.applicationServer)
					if !ok {
						continue
					}
					key := remote.asKey
					if !remote.asKeyStatic {
						key = ASKey{NetworkAppearance: remote.routingKey.NetworkAppearance, NetworkAppearanceSet: remote.routingKey.NetworkAppearanceSet}
					}
					profile := cfg.profile(&identity, key)
					if found && profile != expected {
						return fmt.Errorf("%w: route %s", ErrCongestionMethodMismatch, route.id)
					}
					expected, found = profile, true
				}
			}
		}
		cfg.routeProfiles[route.id] = expected
	}
	return nil
}

func (r *aspRoutes) routeCongestionProfile(route MTPRouteID) CongestionProfile {
	if r.methods != nil {
		if profile, ok := r.methods.routeProfiles[route]; ok {
			return profile
		}
	}
	return r.methods.profile(nil, ASKey{})
}

func (r *aspRoutes) requiredCongestionRecipient(identity SGPIdentity) bool {
	if r.config.signallingGatewaySelection == RouteSelectionBroadcast {
		return true
	}
	gateway, ok := r.config.signallingGateway(identity.SignallingGateway)
	return ok && gateway.sgpSelection == RouteSelectionBroadcast
}

// validateMethodDataFrame is pure: raw precheck and final admission must not
// count the same NIF message twice. Only the actual submission path records the
// returned diagnostic. Scope/state checks retain their existing precedence.
func (c *Association) validateMethodDataFrame(key ASKey, frame []byte) (*DataCongestionProfileMismatch, error) {
	// Both callers already establish that this is DATA. Keep the complete typed
	// framing checks while decoding the message and payload into local values;
	// mandatory method validation must preserve the existing send byte budget.
	var data messages.Data
	if err := data.UnmarshalBinary(frame); err != nil {
		if errors.Is(err, messages.ErrMissingParameter) {
			return nil, ErrMissingProtocolData
		}
		return nil, err
	}
	if data.ProtocolData == nil {
		return nil, ErrMissingProtocolData
	}
	var payload params.ProtocolDataPayload
	if err := payload.UnmarshalBinary(data.ProtocolData.Data); err != nil {
		return nil, err
	}
	if err := c.validateMethodData(key, &payload); err != nil {
		return nil, err
	}
	profile := c.congestionProfile(key)
	if c.role != RoleSGP || profile.allowsPriority(payload.MessagePriority) {
		return nil, nil
	}
	return &DataCongestionProfileMismatch{AS: normalizeOutboundASKey(key), Scope: c.ssnmWireScope(data.NetworkAppearance, data.RoutingContext), Profile: profile, MessagePriority: payload.MessagePriority, Epoch: c.Epoch()}, nil
}

func adaptMethodDestination(profile CongestionProfile, status aspDestinationStatus) aspDestinationStatus {
	effective := adaptMethodStatus(profile, aspSelectionStatus{congestionLevel: status.congestionLevel, congestionLevelSet: status.congestionLevelSet})
	status.congestionLevel, status.congestionLevelSet = effective.congestionLevel, effective.congestionLevelSet
	return status
}
