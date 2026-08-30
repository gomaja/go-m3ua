package m3ua

import (
	"fmt"
	"slices"
	"sort"

	"github.com/gomaja/go-m3ua/messages/params"
	"github.com/gomaja/go-sctp"
)

// RegistrationStatus is a Registration Status value from RFC 4666 Section 3.6.2.
type RegistrationStatus uint32

// Registration Status values from RFC 4666 Section 3.6.2.
const (
	RegistrationSuccessfullyRegistered RegistrationStatus = iota
	RegistrationStatusUnknown
	RegistrationInvalidDestinationPointCode
	RegistrationInvalidNetworkAppearance
	RegistrationInvalidRoutingKey
	RegistrationPermissionDenied
	RegistrationCannotSupportUniqueRouting
	RegistrationRoutingKeyNotCurrentlyProvisioned
	RegistrationInsufficientResources
	RegistrationUnsupportedRoutingKeyParameterField
	RegistrationUnsupportedTrafficHandlingMode
	RegistrationRoutingKeyChangeRefused
	RegistrationRoutingKeyAlreadyRegistered
)

// DeregistrationStatus is a Deregistration Status value from RFC 4666 Section 3.6.4.
type DeregistrationStatus uint32

// Deregistration Status values from RFC 4666 Section 3.6.4.
const (
	DeregistrationSuccessfullyDeregistered DeregistrationStatus = iota
	DeregistrationStatusUnknown
	DeregistrationInvalidRoutingContext
	DeregistrationPermissionDenied
	DeregistrationNotRegistered
	DeregistrationASPActiveForRoutingContext
)

// PointCodeRange is one Originating Point Code List entry in an RFC 4666
// Routing Key. Mask is the number of low-order PointCode bits that are not
// significant; zero identifies one exact point code.
type PointCodeRange struct {
	PointCode uint32
	Mask      uint8
}

// RoutingKeyGroup is one repeatable Destination Point Code, Service
// Indicators, and Originating Point Code List grouping from RFC 4666 Section
// 3.6.1. Empty ServiceIndicators and OriginatingPointCodes mean any value.
type RoutingKeyGroup struct {
	DestinationPointCode  uint32
	ServiceIndicators     []uint8
	OriginatingPointCodes []PointCodeRange
}

// RoutingKey is the traffic selector carried by the Routing Key parameter of
// RFC 4666 Section 3.6.1. TrafficMode is registration policy and does not
// participate in traffic-overlap comparisons.
type RoutingKey struct {
	NetworkAppearance    uint32
	NetworkAppearanceSet bool
	TrafficMode          uint32
	TrafficModeSet       bool
	Groups               []RoutingKeyGroup
}

// ProvisionedRoutingKey binds a statically provisioned Routing Key to its
// Routing Context for coexistence with dynamically registered Routing Keys.
type ProvisionedRoutingKey struct {
	RoutingContext uint32
	RoutingKey     RoutingKey
}

// RoutingKeyPeer identifies the remote M3UA endpoint asking to register a
// Routing Key. ASPIdentifierSet distinguishes omission from the valid value 0;
// RemoteAddr is an owned copy preserving every SCTP peer address.
type RoutingKeyPeer struct {
	Role             Role
	ASPIdentifier    uint32
	ASPIdentifierSet bool
	RemoteAddr       *sctp.SCTPAddr
}

// RoutingKeyRegistrationRequest is the immutable request passed to a Routing
// Key registration authorization policy.
type RoutingKeyRegistrationRequest struct {
	Peer                      RoutingKeyPeer
	LocalRoutingKeyIdentifier uint32
	RequestedRoutingContext   uint32
	RoutingContextRequested   bool
	// NetworkAppearanceImplied reports that the wire Routing Key omitted
	// Network Appearance and the Association's single configured appearance
	// supplied the RFC 4666 Section 3.6.1 implied value.
	NetworkAppearanceImplied bool
	RoutingKey               RoutingKey
}

// RoutingKeyRegistrationAuthorizer decides whether a peer may register a
// Routing Key. RegistrationSuccessfullyRegistered approves the request; a
// defined failure RegistrationStatus rejects it with that status.
// RegistrationRoutingKeyAlreadyRegistered is derived by the registry and is
// not a policy result; returning it or an undefined value is normalized to
// RegistrationInvalidRoutingKey.
type RoutingKeyRegistrationAuthorizer func(RoutingKeyRegistrationRequest) RegistrationStatus

// RoutingKeyDeregistrationRequest is the immutable request passed to a Routing
// Key deregistration authorization policy.
type RoutingKeyDeregistrationRequest struct {
	Peer           RoutingKeyPeer
	RoutingContext uint32
	RoutingKey     RoutingKey
	Provisioned    bool
}

// RoutingKeyDeregistrationAuthorizer decides whether a registered peer may
// deregister one Routing Context. Returning false produces the RFC 4666
// Permission Denied status without changing registry membership.
type RoutingKeyDeregistrationAuthorizer func(RoutingKeyDeregistrationRequest) bool

// RoutingContextAllocationRequest is passed to a custom Routing Context
// allocator. InUseRoutingContexts is an owned, sorted snapshot.
type RoutingContextAllocationRequest struct {
	Registration         RoutingKeyRegistrationRequest
	InUseRoutingContexts []uint32
}

// RoutingContextAllocator chooses a non-zero Routing Context for a new Routing
// Key. Returning an error or a value already in use produces Insufficient
// Resources for that registration.
type RoutingContextAllocator func(RoutingContextAllocationRequest) (uint32, error)

// RoutingKeyManagementConfig enables the optional RFC 4666 Routing Key
// Management procedures on an SGP or IPSP Endpoint.
type RoutingKeyManagementConfig struct {
	// AuthorizeRegistration is required and returns the REG RSP status for
	// each requested Routing Key.
	AuthorizeRegistration RoutingKeyRegistrationAuthorizer
	// AuthorizeDeregistration optionally applies deployment permission policy
	// after the RFC registration and ASP-state checks. Nil permits it.
	AuthorizeDeregistration RoutingKeyDeregistrationAuthorizer
	// AllocateRoutingContext optionally selects a non-zero unused Routing
	// Context. Nil selects the lowest available non-zero value.
	AllocateRoutingContext RoutingContextAllocator
	// ProvisionedRoutingKeys is the immutable static Routing Key inventory.
	ProvisionedRoutingKeys []ProvisionedRoutingKey
	// AllowDynamicRoutingKeys permits a successfully authorized unprovisioned
	// Routing Key to create a registry entry.
	AllowDynamicRoutingKeys bool
	// MaxDynamicRoutingKeys bounds dynamically created registry entries. Zero
	// selects the library default.
	MaxDynamicRoutingKeys int
	// RemoveUnusedRoutingKeys removes an unprovisioned dynamic entry after its
	// final ASP/IPSP deregisters or its Association ends.
	RemoveUnusedRoutingKeys bool
}

type canonicalRoutingKey struct {
	networkAppearance    uint32
	networkAppearanceSet bool
	groups               []canonicalRoutingKeyGroup
}

type canonicalRoutingKeyGroup struct {
	destinationPointCode  uint32
	serviceIndicators     []uint8
	originatingPointCodes []canonicalPointCodeRange
}

type canonicalPointCodeRange struct {
	pointCode uint32
	mask      uint8
}

func canonicalizeRoutingKey(key RoutingKey) (canonicalRoutingKey, error) {
	if key.TrafficModeSet && !validTrafficMode(key.TrafficMode) {
		return canonicalRoutingKey{}, fmt.Errorf("unsupported Traffic Mode %d", key.TrafficMode)
	}
	if len(key.Groups) == 0 {
		return canonicalRoutingKey{}, fmt.Errorf("routing key has no destination point code group")
	}
	canonical := canonicalRoutingKey{
		networkAppearance:    key.NetworkAppearance,
		networkAppearanceSet: key.NetworkAppearanceSet,
		groups:               make([]canonicalRoutingKeyGroup, 0, len(key.Groups)),
	}
	for _, group := range key.Groups {
		if group.DestinationPointCode > 0x00ffffff {
			return canonicalRoutingKey{}, fmt.Errorf("destination point code %#x exceeds 24 bits", group.DestinationPointCode)
		}
		serviceIndicators := append([]uint8(nil), group.ServiceIndicators...)
		sort.Slice(serviceIndicators, func(i, j int) bool { return serviceIndicators[i] < serviceIndicators[j] })
		serviceIndicators = slices.Compact(serviceIndicators)
		for _, serviceIndicator := range serviceIndicators {
			if serviceIndicator == 0 {
				return canonicalRoutingKey{}, fmt.Errorf("service indicators contains MTP management SI 0")
			}
		}

		originatingPointCodes := make([]canonicalPointCodeRange, 0, len(group.OriginatingPointCodes))
		for _, pointCode := range group.OriginatingPointCodes {
			if pointCode.PointCode > 0x00ffffff {
				return canonicalRoutingKey{}, fmt.Errorf("originating point code %#x exceeds 24 bits", pointCode.PointCode)
			}
			mask := pointCode.Mask
			if mask > 24 {
				mask = 24
			}
			originatingPointCodes = append(originatingPointCodes, canonicalPointCodeRange{
				pointCode: canonicalPointCode(pointCode.PointCode, mask),
				mask:      mask,
			})
		}
		sort.Slice(originatingPointCodes, func(i, j int) bool {
			if originatingPointCodes[i].pointCode != originatingPointCodes[j].pointCode {
				return originatingPointCodes[i].pointCode < originatingPointCodes[j].pointCode
			}
			return originatingPointCodes[i].mask < originatingPointCodes[j].mask
		})
		originatingPointCodes = slices.Compact(originatingPointCodes)
		canonical.groups = append(canonical.groups, canonicalRoutingKeyGroup{
			destinationPointCode:  group.DestinationPointCode,
			serviceIndicators:     serviceIndicators,
			originatingPointCodes: originatingPointCodes,
		})
	}
	sort.Slice(canonical.groups, func(i, j int) bool {
		return compareCanonicalRoutingKeyGroup(canonical.groups[i], canonical.groups[j]) < 0
	})
	canonical.groups = slices.CompactFunc(canonical.groups, func(first, second canonicalRoutingKeyGroup) bool {
		return first.equal(second)
	})
	return canonical, nil
}

func canonicalPointCode(pointCode uint32, mask uint8) uint32 {
	if mask >= 24 {
		return 0
	}
	if mask == 0 {
		return pointCode
	}
	return pointCode &^ uint32(1<<mask-1)
}

func (first canonicalRoutingKey) equal(second canonicalRoutingKey) bool {
	if first.networkAppearanceSet != second.networkAppearanceSet ||
		first.networkAppearance != second.networkAppearance ||
		len(first.groups) != len(second.groups) {
		return false
	}
	for index := range first.groups {
		if !first.groups[index].equal(second.groups[index]) {
			return false
		}
	}
	return true
}

func (first canonicalRoutingKeyGroup) equal(second canonicalRoutingKeyGroup) bool {
	return first.destinationPointCode == second.destinationPointCode &&
		slices.Equal(first.serviceIndicators, second.serviceIndicators) &&
		slices.Equal(first.originatingPointCodes, second.originatingPointCodes)
}

func (first canonicalRoutingKey) overlaps(second canonicalRoutingKey) bool {
	if first.networkAppearanceSet && second.networkAppearanceSet &&
		first.networkAppearance != second.networkAppearance {
		return false
	}
	for _, firstGroup := range first.groups {
		for _, secondGroup := range second.groups {
			if firstGroup.overlaps(secondGroup) {
				return true
			}
		}
	}
	return false
}

func (key canonicalRoutingKey) matchesTraffic(
	networkAppearance uint32,
	networkAppearanceSet bool,
	originatingPointCode uint32,
	destinationPointCode uint32,
	serviceIndicator uint8,
) bool {
	if networkAppearanceSet && key.networkAppearanceSet && key.networkAppearance != networkAppearance {
		return false
	}
	originatingPointCode &= 0x00ffffff
	destinationPointCode &= 0x00ffffff
	for _, group := range key.groups {
		if group.destinationPointCode != destinationPointCode ||
			!setContains(group.serviceIndicators, serviceIndicator) ||
			!pointCodeRangesContain(group.originatingPointCodes, originatingPointCode) {
			continue
		}
		return true
	}
	return false
}

func setContains(values []uint8, value uint8) bool {
	return len(values) == 0 || slices.Contains(values, value)
}

func pointCodeRangesContain(ranges []canonicalPointCodeRange, pointCode uint32) bool {
	if len(ranges) == 0 {
		return true
	}
	for _, pointCodeRange := range ranges {
		low, high := pointCodeRange.bounds()
		if pointCode >= low && pointCode <= high {
			return true
		}
	}
	return false
}

func (first canonicalRoutingKeyGroup) overlaps(second canonicalRoutingKeyGroup) bool {
	return first.destinationPointCode == second.destinationPointCode &&
		setsOverlap(first.serviceIndicators, second.serviceIndicators) &&
		pointCodeRangesOverlap(first.originatingPointCodes, second.originatingPointCodes)
}

func setsOverlap(first, second []uint8) bool {
	if len(first) == 0 || len(second) == 0 {
		return true
	}
	firstIndex, secondIndex := 0, 0
	for firstIndex < len(first) && secondIndex < len(second) {
		switch {
		case first[firstIndex] == second[secondIndex]:
			return true
		case first[firstIndex] < second[secondIndex]:
			firstIndex++
		default:
			secondIndex++
		}
	}
	return false
}

func pointCodeRangesOverlap(first, second []canonicalPointCodeRange) bool {
	if len(first) == 0 || len(second) == 0 {
		return true
	}
	for _, firstRange := range first {
		firstLow, firstHigh := firstRange.bounds()
		for _, secondRange := range second {
			secondLow, secondHigh := secondRange.bounds()
			if firstLow <= secondHigh && secondLow <= firstHigh {
				return true
			}
		}
	}
	return false
}

func (pointCode canonicalPointCodeRange) bounds() (uint32, uint32) {
	if pointCode.mask >= 24 {
		return 0, 0x00ffffff
	}
	if pointCode.mask == 0 {
		return pointCode.pointCode, pointCode.pointCode
	}
	wildcard := uint32(1<<pointCode.mask) - 1
	return pointCode.pointCode, pointCode.pointCode | wildcard
}

func compareCanonicalRoutingKeyGroup(first, second canonicalRoutingKeyGroup) int {
	if first.destinationPointCode != second.destinationPointCode {
		if first.destinationPointCode < second.destinationPointCode {
			return -1
		}
		return 1
	}
	if comparison := slices.Compare(first.serviceIndicators, second.serviceIndicators); comparison != 0 {
		return comparison
	}
	limit := min(len(first.originatingPointCodes), len(second.originatingPointCodes))
	for index := 0; index < limit; index++ {
		if first.originatingPointCodes[index].pointCode != second.originatingPointCodes[index].pointCode {
			if first.originatingPointCodes[index].pointCode < second.originatingPointCodes[index].pointCode {
				return -1
			}
			return 1
		}
		if first.originatingPointCodes[index].mask != second.originatingPointCodes[index].mask {
			if first.originatingPointCodes[index].mask < second.originatingPointCodes[index].mask {
				return -1
			}
			return 1
		}
	}
	return len(first.originatingPointCodes) - len(second.originatingPointCodes)
}

func snapshotRoutingKeyManagementConfig(config *RoutingKeyManagementConfig) *RoutingKeyManagementConfig {
	if config == nil {
		return nil
	}
	snapshot := *config
	snapshot.ProvisionedRoutingKeys = make([]ProvisionedRoutingKey, len(config.ProvisionedRoutingKeys))
	for index, provisioned := range config.ProvisionedRoutingKeys {
		snapshot.ProvisionedRoutingKeys[index] = ProvisionedRoutingKey{
			RoutingContext: provisioned.RoutingContext,
			RoutingKey:     snapshotRoutingKey(provisioned.RoutingKey),
		}
	}
	return &snapshot
}

func snapshotRoutingKey(key RoutingKey) RoutingKey {
	snapshot := key
	snapshot.Groups = make([]RoutingKeyGroup, len(key.Groups))
	for index, group := range key.Groups {
		snapshot.Groups[index] = RoutingKeyGroup{
			DestinationPointCode:  group.DestinationPointCode,
			ServiceIndicators:     append([]uint8(nil), group.ServiceIndicators...),
			OriginatingPointCodes: append([]PointCodeRange(nil), group.OriginatingPointCodes...),
		}
	}
	return snapshot
}

func routingKeyWithImpliedNetworkAppearance(key RoutingKey, configured *params.Param) (RoutingKey, bool) {
	snapshot := snapshotRoutingKey(key)
	if snapshot.NetworkAppearanceSet || configured == nil || configured.Tag != params.NetworkAppearance || len(configured.Data) != 4 {
		return snapshot, false
	}
	snapshot.NetworkAppearance = configured.NetworkAppearance()
	snapshot.NetworkAppearanceSet = true
	return snapshot, true
}

func validateRoutingKeyManagementConfig(config *RoutingKeyManagementConfig) error {
	if config == nil {
		return nil
	}
	if config.AuthorizeRegistration == nil {
		return fmt.Errorf("missing Routing Key registration authorization policy")
	}
	if config.MaxDynamicRoutingKeys < 0 {
		return fmt.Errorf("negative maximum dynamic Routing Key count")
	}
	seenRoutingContexts := make(map[uint32]struct{}, len(config.ProvisionedRoutingKeys))
	canonicalKeys := make([]canonicalRoutingKey, 0, len(config.ProvisionedRoutingKeys))
	for _, provisioned := range config.ProvisionedRoutingKeys {
		if provisioned.RoutingContext == 0 {
			return fmt.Errorf("provisioned Routing Context 0 cannot represent RKM success")
		}
		if _, duplicate := seenRoutingContexts[provisioned.RoutingContext]; duplicate {
			return fmt.Errorf("duplicate provisioned Routing Context %d", provisioned.RoutingContext)
		}
		seenRoutingContexts[provisioned.RoutingContext] = struct{}{}
		canonical, err := canonicalizeRoutingKey(provisioned.RoutingKey)
		if err != nil {
			return err
		}
		for _, existing := range canonicalKeys {
			if canonical.overlaps(existing) {
				return fmt.Errorf("overlapping provisioned Routing Keys")
			}
		}
		canonicalKeys = append(canonicalKeys, canonical)
	}
	return nil
}

func registrationStatusParam(status RegistrationStatus) uint32 { return uint32(status) }

func deregistrationStatusParam(status DeregistrationStatus) uint32 { return uint32(status) }

func routingKeyFromPayload(payload *params.RoutingKeyPayload) (RoutingKeyRegistrationRequest, error) {
	if payload == nil || payload.LocalRoutingKeyIdentifier == nil {
		return RoutingKeyRegistrationRequest{}, fmt.Errorf("missing Local RK Identifier")
	}
	request := RoutingKeyRegistrationRequest{
		LocalRoutingKeyIdentifier: payload.LocalRoutingKeyIdentifier.LocalRoutingKeyIdentifier(),
	}
	if payload.RoutingContext != nil {
		request.RequestedRoutingContext = payload.RoutingContext.RoutingContext()
		request.RoutingContextRequested = true
	}
	if payload.TrafficModeType != nil {
		request.RoutingKey.TrafficMode = payload.TrafficModeType.TrafficModeType()
		request.RoutingKey.TrafficModeSet = true
	}
	if payload.NetworkAppearance != nil {
		request.RoutingKey.NetworkAppearance = payload.NetworkAppearance.NetworkAppearance()
		request.RoutingKey.NetworkAppearanceSet = true
	}
	request.RoutingKey.Groups = make([]RoutingKeyGroup, 0, len(payload.Groups))
	for _, group := range payload.Groups {
		decoded := RoutingKeyGroup{DestinationPointCode: group.DestinationPointCode.DestinationPointCode()}
		if group.ServiceIndicators != nil {
			decoded.ServiceIndicators = append([]uint8(nil), group.ServiceIndicators.ServiceIndicators()...)
		}
		if group.OriginatingPointCodeList != nil {
			entries := group.OriginatingPointCodeList.OriginatingPointCodeListEntries()
			decoded.OriginatingPointCodes = make([]PointCodeRange, len(entries))
			for index, entry := range entries {
				decoded.OriginatingPointCodes[index] = PointCodeRange{
					PointCode: entry.PointCode,
					Mask:      entry.Mask,
				}
			}
		}
		request.RoutingKey.Groups = append(request.RoutingKey.Groups, decoded)
	}
	if _, err := canonicalizeRoutingKey(request.RoutingKey); err != nil {
		return RoutingKeyRegistrationRequest{}, err
	}
	return request, nil
}
