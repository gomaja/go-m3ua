// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"sort"
	"sync"
)

// Bounds for the route-independent SSNM state store.
//
// Every Affected Point Code in an SSNM message is chosen by the peer and need
// not correspond to anything this node routes to, so retention that follows
// what a peer reports has to be bounded in every dimension a peer can grow.
const (
	// DefaultMaxSSNMStateRecords bounds the dimension records one Endpoint's
	// SSNM state store retains across every partition.
	DefaultMaxSSNMStateStoreRecords = 16384
	// DefaultMaxSSNMStateStoreBytes bounds the accounted retention of that same
	// store. Records are not uniform — a wire scope carries a Routing Context
	// list — so a record count alone does not bound memory.
	DefaultMaxSSNMStateStoreBytes = 4 << 20
	// DefaultMaxSSNMPartitionRecords bounds the dimension records retained for
	// one canonical Signalling Gateway and Application Server.
	DefaultMaxSSNMPartitionRecords = 2048
	// DefaultMaxSSNMPeerRecords bounds the dimension records retained for all
	// partitions of one peer, so one Signalling Gateway cannot consume the
	// whole Endpoint budget.
	DefaultMaxSSNMPeerRecords = 8192
	// DefaultMaxSSNMPartitions bounds how many canonical or standalone
	// partitions the store will create.
	DefaultMaxSSNMPartitions = 256
	// DefaultMaxSSNMSubscribers bounds concurrent SSNM subscriptions.
	DefaultMaxSSNMSubscribers = 16
	// DefaultSSNMSubscriptionQueueSize bounds the deltas one subscription
	// retains while the application is not reading them.
	DefaultSSNMSubscriptionQueueSize = 256
	// DefaultSSNMSubscriptionQueueBytes bounds the accounted bytes queued by
	// one subscription, independently of its event-count limit.
	DefaultSSNMSubscriptionQueueBytes = 1 << 20
)

// Accounted retention of one stored item. The values are deliberately fixed
// rather than measured: a byte budget has to be predictable from configuration
// and identical on every platform, and Go gives no portable retained size.
const (
	ssnmRecordBaseBytes      = 128
	ssnmResolvedASBytes      = 16
	ssnmPartitionBaseBytes   = 256
	ssnmRoutingContextBytes  = 4
	ssnmMinimumRecordBytes   = ssnmRecordBaseBytes + ssnmResolvedASBytes
	ssnmResourceReasonLength = 160
)

// SSNMStateConfig bounds the route-independent SSNM state store of one
// Endpoint. A field less than or equal to zero selects its default; a negative
// field is a configuration error.
type SSNMStateConfig struct {
	// MaxRecords bounds retained availability, destination congestion, and
	// peer congestion records across every partition. Each peer AS/range and
	// wire scope costs one record independently of destination knowledge.
	MaxRecords int
	// MaxBytes bounds the accounted retention of those records and of the
	// partitions holding them. A destination dimension costs 128 bytes plus
	// 4 per wire RC and 16 per admission-time resolved AS entry. The private
	// resolution survives source reconfiguration; wire Scope stays unchanged.
	// Peer congestion charges 256 bytes plus 8 per
	// Routing Context for its retained scope and normalized scope key.
	MaxBytes int
	// MaxRecordsPerPartition bounds the records of one canonical Signalling
	// Gateway and Application Server, or of one standalone Association.
	MaxRecordsPerPartition int
	// MaxRecordsPerPeer bounds the records of every partition of one peer.
	MaxRecordsPerPeer int
	// MaxPartitions bounds how many partitions the store will create.
	MaxPartitions int
	// MaxSubscribers bounds concurrent SSNM subscriptions.
	MaxSubscribers int
	// SubscriptionQueueSize bounds the deltas one subscription retains while
	// the application is not reading them. Overflow is reported as continuity
	// loss and cleared only by a successful Resync.
	SubscriptionQueueSize int
	// SubscriptionQueueBytes bounds the portable accounted payload of queued
	// events. Zero selects DefaultSSNMSubscriptionQueueBytes; a positive limit
	// must be at least 512 bytes. Accounting charges 512 bytes per event,
	// 8 per report destination, 32 per profile mismatch, 256 per updated
	// destination or peer record, 16 per invalidation entry, and 4 per Routing
	// Context in the report, both dimensions of each update, and each peer record.
	// Destination invalidations carry only Epoch, bounded Reason and at most
	// 2048 entries: at most 512 + 160 + 2048*16 = 33440 bytes, including for
	// custom retention limits. Configured partition names are not replayed.
	// Total queued events/bytes can still overflow; such loss requires Resync.
	// It also charges the byte lengths of Reason and the event/report
	// partition identity strings.
	// Snapshot results, the fixed continuity-loss marker, queue backing-array
	// capacity, and allocator overhead are outside this accounting. It is not
	// a Go heap or RSS limit. Either queue limit losing continuity requires
	// Resync; successfully queued events remain deliverable first.
	SubscriptionQueueBytes int
	// MaxAffectedPointCodes bounds the Affected Point Codes accepted from one
	// SSNM message. The count is taken from the encoded parameter before the
	// point codes are expanded, so an oversized message costs no allocation
	// proportional to what the peer claimed.
	MaxAffectedPointCodes int
}

// SSNMPartitionKind names the ownership scope of one bounded SSNM partition.
type SSNMPartitionKind uint8

const (
	// SSNMCanonicalPartition is one Application Server reached through one
	// Signalling Gateway. RFC 4666 Section 1.2 has an SG contain a set of
	// SGPs and, "Where an SG contains more than one SGP", has those SGPs
	// "coordinated into a single management view to the SS7 network and to the
	// supported Application Servers", so every SGP of one Signalling Gateway
	// contributes to one partition.
	SSNMCanonicalPartition SSNMPartitionKind = iota + 1
	// SSNMStandalonePartition is one Association that resolves to no
	// provisioned Application Server. Its knowledge is its own and cannot be
	// preserved by any sibling.
	SSNMStandalonePartition
)

func (k SSNMPartitionKind) String() string {
	switch k {
	case SSNMCanonicalPartition:
		return "canonical"
	case SSNMStandalonePartition:
		return "standalone"
	default:
		return "unknown"
	}
}

// SSNMPartition is the ownership scope of retained SSNM knowledge.
//
// It is deliberately not the wire scope. RFC 4666 Section 1.4.2.1 makes a
// Routing Context "an index into a sending node's Message Distribution Table",
// so the same value on two Signalling Gateways names different Application
// Servers, and two SGPs of one Signalling Gateway may label one Application
// Server differently. Knowledge is therefore owned by the canonical identity,
// not by the label that carried it.
type SSNMPartition struct {
	Kind              SSNMPartitionKind
	SignallingGateway SignallingGatewayID
	ApplicationServer RemoteASID
	// Association is set only for a standalone partition.
	Association AssociationID
}

// ssnmPeerKey is the retention-budget owner of one partition. Canonical
// partitions of one Signalling Gateway share a budget; a standalone
// Association owns its own.
type ssnmPeerKey struct {
	signallingGateway SignallingGatewayID
	association       AssociationID
}

func (p SSNMPartition) peerKey() ssnmPeerKey {
	if p.Kind == SSNMStandalonePartition {
		return ssnmPeerKey{association: p.Association}
	}
	return ssnmPeerKey{signallingGateway: p.SignallingGateway}
}

// SSNMReportKind identifies the RFC 4666 Section 3.4 message a report came
// from.
type SSNMReportKind uint8

const (
	// SSNMDestinationUnavailableReport is DUNA, RFC 4666 Section 3.4.1.
	SSNMDestinationUnavailableReport SSNMReportKind = iota + 1
	// SSNMDestinationAvailableReport is DAVA, RFC 4666 Section 3.4.2.
	SSNMDestinationAvailableReport
	// SSNMDestinationStateAuditReport is DAUD, RFC 4666 Section 3.4.3.
	SSNMDestinationStateAuditReport
	// SSNMSignallingCongestionReport is SCON, RFC 4666 Section 3.4.4.
	SSNMSignallingCongestionReport
	// SSNMDestinationUserPartUnavailableReport is DUPU, RFC 4666 Section 3.4.5.
	SSNMDestinationUserPartUnavailableReport
	// SSNMDestinationRestrictedReport is DRST, RFC 4666 Section 3.4.6.
	SSNMDestinationRestrictedReport
)

func (k SSNMReportKind) String() string {
	switch k {
	case SSNMDestinationUnavailableReport:
		return "DUNA"
	case SSNMDestinationAvailableReport:
		return "DAVA"
	case SSNMDestinationStateAuditReport:
		return "DAUD"
	case SSNMSignallingCongestionReport:
		return "SCON"
	case SSNMDestinationUserPartUnavailableReport:
		return "DUPU"
	case SSNMDestinationRestrictedReport:
		return "DRST"
	default:
		return "unknown"
	}
}

// SSNMReportSource names which side originated a report.
type SSNMReportSource uint8

const (
	// SSNMPeerReport was received from the peer and locally validated.
	SSNMPeerReport SSNMReportSource = iota + 1
	// SSNMLocalReport was originated by this node.
	SSNMLocalReport
)

func (s SSNMReportSource) String() string {
	switch s {
	case SSNMPeerReport:
		return "peer"
	case SSNMLocalReport:
		return "local"
	default:
		return "unknown"
	}
}

// SSNMReport is one locally validated RFC 4666 Section 3.4 report, carried to
// the application with everything needed to attribute it.
//
// Scope is the exact wire scope as it arrived; Partition is the canonical
// identity it resolved to. Epoch is the binding generation the report was
// validated under, so a report that predates a source reset is recognisable
// after one.
type SSNMReport struct {
	// ProfileMismatches reports incompatible method profiles to Layer Management.
	// CongestionLevel and retained state preserve the received value unchanged.
	ProfileMismatches []CongestionProfileMismatch
	Kind              SSNMReportKind
	Source            SSNMReportSource
	Scope             WireScope
	Partition         SSNMPartition
	Association       AssociationID
	Epoch             uint64
	// Revision is the store revision this report produced. It is zero for a
	// report the store did not retain or publish.
	Revision uint64
	// Destinations are the Affected Point Codes the report named.
	Destinations []PointCodeRange
	// CongestionLevel and CongestionLevelSet carry the RFC 4666 Section 3.4.4
	// Congestion Indications parameter. The parameter is optional and level
	// zero is "No Congestion or Undefined", so presence is explicit.
	CongestionLevel    uint8
	CongestionLevelSet bool
	// UserCause carries the RFC 4666 Section 3.4.5 MTP3-User identity and
	// unavailability cause of a DUPU.
	UserCause    uint32
	UserCauseSet bool
	// ConcernedDestination is the RFC 4666 Section 3.4.4 ASP-to-SGP TFC
	// trigger, set only with exactly one unmasked affected destination.
	ConcernedDestination    uint32
	ConcernedDestinationSet bool
	// UnmappedConcernedDestination is the raw received Concerned Destination
	// when a congestion list or range prevents that single-DPC TFC mapping
	// (RFC 4666 Section 3.4.4). SubscribeSSNM carries this diagnostic to Layer
	// Management alongside all valid congestion information. It is not a TFC
	// trigger; ConcernedDestinationSet is false. Nil means no mapping mismatch,
	// while a pointer to zero preserves an explicitly received point code zero.
	// Each subscription owns its scalar copy.
	UnmappedConcernedDestination *uint32
	// PeerReported marks a report that describes the peer rather than the SS7
	// network: the SCON RFC 4666 Section 3.4.4 lets an ASP send "indicating
	// that the congestion level of the M3UA layer or the ASP has changed". It
	// says nothing about destination reachability, so it is delivered as an
	// event and retained separately from destination knowledge.
	PeerReported bool
}

// retainsAvailability reports whether this report installs the availability
// and restriction dimension.
//
// RFC 4666 Section 4.5.2.2 makes availability and congestion two separate
// statuses of one destination, so only the availability messages move this
// dimension.
func (r SSNMReport) retainsAvailability() bool {
	switch r.Kind {
	case SSNMDestinationUnavailableReport,
		SSNMDestinationAvailableReport,
		SSNMDestinationRestrictedReport:
		return true
	default:
		return false
	}
}

// retainsCongestion reports whether this report installs the congestion
// dimension.
//
// SCON is the only message that can be about an M3UA layer rather than a
// destination -- RFC 4666 Section 3.4.4 lets one be sent "indicating that the
// congestion level of the M3UA layer or the ASP has changed" -- so it is the
// only dimension that has to ask.
func (r SSNMReport) retainsCongestion() bool {
	return r.Kind == SSNMSignallingCongestionReport && !r.PeerReported
}

func (r SSNMReport) availabilityState() DestinationAvailability {
	switch r.Kind {
	case SSNMDestinationUnavailableReport:
		return DestinationUnavailable
	case SSNMDestinationRestrictedReport:
		return DestinationRestricted
	default:
		return DestinationAvailable
	}
}

// SSNMAvailability is the retained availability and restriction dimension of
// one destination, with the provenance of the report that installed it.
type SSNMAvailability struct {
	State       DestinationAvailability
	Kind        SSNMReportKind
	Source      SSNMReportSource
	Scope       WireScope
	Association AssociationID
	Epoch       uint64
	Revision    uint64
}

// SSNMCongestion is the retained congestion dimension of one destination.
type SSNMCongestion struct {
	Congested   bool
	Level       uint8
	LevelSet    bool
	Source      SSNMReportSource
	Scope       WireScope
	Association AssociationID
	Epoch       uint64
	Revision    uint64
}

// SSNMDestinationKnowledge is both dimensions of one destination range within
// one partition. Either dimension may be absent; neither implies the other.
type SSNMDestinationKnowledge struct {
	Destination     PointCodeRange
	Availability    SSNMAvailability
	AvailabilitySet bool
	Congestion      SSNMCongestion
	CongestionSet   bool
}

// SSNMBinding is one Association admitted to a partition.
type SSNMBinding struct {
	Association AssociationID
	// Pending is true for a binding admitted under the RFC 4666 Section 4.5.1
	// activation window, before the ASP Active Ack that completes activation.
	// Its knowledge is retained but authorizes no traffic.
	Pending bool
}

// SSNMPartitionKnowledge is the owned snapshot of one partition.
type SSNMPartitionKnowledge struct {
	Partition SSNMPartition
	// Epoch is the binding generation, numbered across the whole store. A
	// partition that loses its last binding is retired; the next binding
	// starts a new epoch, so knowledge from before a source reset is never
	// mistaken for knowledge after one.
	Epoch uint64
	// Bindings are the Associations currently admitted, in AssociationID order.
	Bindings []SSNMBinding
	// TrafficAuthorized is true only when at least one binding has completed
	// activation. RFC 4666 Section 4.3.4.3: "The ASP SHOULD NOT send Data or
	// SSNM messages for the related Routing Context(s) before receiving an ASP
	// Active Ack message, or it will risk message loss." Reports admitted
	// during the Section 4.5.1 window are retained but authorize nothing on
	// their own.
	TrafficAuthorized bool
	// Destinations are the retained dimensions, in point-code then mask order.
	Destinations []SSNMDestinationKnowledge
}

// SSNMSnapshot is an owned, atomic view of the whole SSNM state store.
type SSNMSnapshot struct {
	// Revision is the store revision this snapshot was taken at. Every event a
	// subscription delivers afterwards carries a strictly greater revision.
	Revision   uint64
	Partitions []SSNMPartitionKnowledge
	// PeerCongestion is peer-local state, separate from authoritative SS7
	// destination knowledge. It contains no replayable Concerned Destination
	// triggers. Every scope is owned by the caller.
	PeerCongestion []PeerCongestionState
	// RecordsRefused counts dimension and peer records a resource bound refused since
	// the store was created.
	RecordsRefused uint64
	// ReportsRefused counts whole reports a resource bound refused.
	ReportsRefused uint64
	// PartitionsInvalidated counts destination invalidation events and
	// last-binding partition retirements. Oversized refusals that remove only
	// peer records or no knowledge do not count as destination invalidations.
	PartitionsInvalidated uint64
	// LastResourceLoss is the most recent bounded resource diagnostic, empty
	// when none has occurred.
	LastResourceLoss string
}

// Resource and subscription errors.
var (
	// ErrSSNMResourceLoss reports that an otherwise valid SSNM message could
	// not be retained within this node's own budget.
	//
	// It is a local resource condition, not a fault in the message. RFC 4666
	// Section 3.8.1 lists the Error conditions, and "the receiver ran out of
	// its own memory" is not among them, so the association stays up and no
	// Error is fabricated for the peer.
	ErrSSNMResourceLoss = errors.New("SSNM resource loss")
	// ErrSSNMStateLimit reports that a report would exceed a configured bound
	// of the route-independent SSNM state store.
	ErrSSNMStateLimit = fmt.Errorf("%w: SSNM state store limit exceeded", ErrSSNMResourceLoss)
	// ErrSSNMOversizedReport reports an SSNM message naming more Affected
	// Point Codes than the configured limit accepts.
	ErrSSNMOversizedReport = fmt.Errorf("%w: SSNM Affected Point Code limit exceeded", ErrSSNMResourceLoss)
	// ErrInvalidSSNMStateConfig reports an unusable SSNM state configuration,
	// including a reservation that cannot fit inside the budget reserving it.
	ErrInvalidSSNMStateConfig = errors.New("invalid SSNM state configuration")
)

type ssnmDestinationKey struct {
	pointCode uint32
	mask      uint8
}

func ssnmDestinationKeyFor(rangeValue PointCodeRange) ssnmDestinationKey {
	mask := effectiveDestinationMask(rangeValue.Mask)
	return ssnmDestinationKey{
		pointCode: destinationRangePrefix(rangeValue.PointCode, mask),
		mask:      mask,
	}
}

func (k ssnmDestinationKey) pointCodeRange() PointCodeRange {
	return PointCodeRange{PointCode: k.pointCode, Mask: k.mask}
}

// Private wrappers keep the received provenance public and the effective
// admission scope immutable, including after deregistration and RC reuse.
type ssnmAvailabilityRecord struct {
	SSNMAvailability
	resolved []ASKey
}
type ssnmCongestionRecord struct {
	SSNMCongestion
	resolved []ASKey
}

type ssnmPartitionState struct {
	partition      SSNMPartition
	epoch          uint64
	bindings       map[AssociationID]bool // value reports a pending binding
	availability   map[ssnmDestinationKey]ssnmAvailabilityRecord
	congestion     map[ssnmDestinationKey]ssnmCongestionRecord
	peerCongestion map[peerCongestionKey]PeerCongestionState
	bytes          int
}

func (p *ssnmPartitionState) records() int {
	return len(p.availability) + len(p.congestion) + len(p.peerCongestion)
}

func (p *ssnmPartitionState) trafficAuthorized() bool {
	for _, pending := range p.bindings {
		if !pending {
			return true
		}
	}
	return false
}

// ssnmState is the Endpoint's bounded, route-independent SSNM knowledge.
//
// Every mutation and every read runs under one mutex. That is what makes a
// subscription's snapshot and its delta stream atomic with respect to each
// other: the snapshot is taken and the subscriber registered in the same
// critical section, so no report can slip between them.
type ssnmState struct {
	mu          sync.Mutex
	limits      SSNMStateConfig
	partitions  map[SSNMPartition]*ssnmPartitionState
	peerRecords map[ssnmPeerKey]int
	records     int
	bytes       int
	revision    uint64
	// epochs numbers binding generations across the whole store rather than
	// per partition. A partition that loses its last binding is deleted, so a
	// per-partition counter would restart at the same value the retired
	// generation had used and knowledge from before a source reset would be
	// indistinguishable from knowledge after one.
	epochs      uint64
	subscribers map[*SSNMSubscription]struct{}
	closed      bool
	// liveSources admits work only from Associations currently owned by the Endpoint.
	liveSources map[AssociationID]struct{}
	peerSources map[AssociationID]peerCongestionSource

	recordsRefused        uint64
	reportsRefused        uint64
	partitionsInvalidated uint64
	lastResourceLoss      string
}

// resolveSSNMStateConfig fills in defaults and refuses a configuration whose
// reservations cannot hold.
func resolveSSNMStateConfig(config *SSNMStateConfig) (SSNMStateConfig, error) {
	var resolved SSNMStateConfig
	if config != nil {
		resolved = *config
	}
	fields := []struct {
		name  string
		value *int
	}{
		{"MaxRecords", &resolved.MaxRecords},
		{"MaxBytes", &resolved.MaxBytes},
		{"MaxRecordsPerPartition", &resolved.MaxRecordsPerPartition},
		{"MaxRecordsPerPeer", &resolved.MaxRecordsPerPeer},
		{"MaxPartitions", &resolved.MaxPartitions},
		{"MaxSubscribers", &resolved.MaxSubscribers},
		{"SubscriptionQueueSize", &resolved.SubscriptionQueueSize},
		{"SubscriptionQueueBytes", &resolved.SubscriptionQueueBytes},
		{"MaxAffectedPointCodes", &resolved.MaxAffectedPointCodes},
	}
	for _, field := range fields {
		if *field.value < 0 {
			return SSNMStateConfig{}, fmt.Errorf("%w: negative %s %d",
				ErrInvalidSSNMStateConfig, field.name, *field.value)
		}
	}
	explicitPeerRecords := resolved.MaxRecordsPerPeer != 0
	explicitPartitionRecords := resolved.MaxRecordsPerPartition != 0
	defaults := []struct {
		value *int
		fill  int
	}{
		{&resolved.MaxRecords, DefaultMaxSSNMStateStoreRecords},
		{&resolved.MaxBytes, DefaultMaxSSNMStateStoreBytes},
		{&resolved.MaxRecordsPerPartition, DefaultMaxSSNMPartitionRecords},
		{&resolved.MaxRecordsPerPeer, DefaultMaxSSNMPeerRecords},
		{&resolved.MaxPartitions, DefaultMaxSSNMPartitions},
		{&resolved.MaxSubscribers, DefaultMaxSSNMSubscribers},
		{&resolved.SubscriptionQueueSize, DefaultSSNMSubscriptionQueueSize},
		{&resolved.SubscriptionQueueBytes, DefaultSSNMSubscriptionQueueBytes},
		{&resolved.MaxAffectedPointCodes, DefaultMaxAffectedPointCodesPerSSNM},
	}
	for _, field := range defaults {
		if *field.value == 0 {
			*field.value = field.fill
		}
	}

	// A default reservation is a starting point, not a demand. Tightening the
	// store budget alone is an ordinary thing to do, and it should not be
	// refused because a reservation nobody asked for no longer fits.
	if !explicitPeerRecords && resolved.MaxRecordsPerPeer > resolved.MaxRecords {
		resolved.MaxRecordsPerPeer = resolved.MaxRecords
	}
	if !explicitPartitionRecords && resolved.MaxRecordsPerPartition > resolved.MaxRecordsPerPeer {
		resolved.MaxRecordsPerPartition = resolved.MaxRecordsPerPeer
	}

	// An explicit reservation larger than the budget it is carved from can
	// never be reached, so it is not a conservative bound but a
	// misconfiguration that silently disables the inner limit. A partition
	// belongs to one peer and a peer to one store, so these two checks cover
	// the partition against the store as well.
	if resolved.MaxRecordsPerPeer > resolved.MaxRecords {
		return SSNMStateConfig{}, fmt.Errorf(
			"%w: %d records per peer cannot fit in the %d-record store",
			ErrInvalidSSNMStateConfig, resolved.MaxRecordsPerPeer, resolved.MaxRecords)
	}
	if resolved.MaxRecordsPerPartition > resolved.MaxRecordsPerPeer {
		return SSNMStateConfig{}, fmt.Errorf(
			"%w: %d records per partition cannot fit in the %d-record peer budget",
			ErrInvalidSSNMStateConfig, resolved.MaxRecordsPerPartition, resolved.MaxRecordsPerPeer)
	}
	// A byte budget below the accounted cost of one partition and one record
	// admits nothing at all, which is a store that cannot be used rather than
	// one that is tightly bounded.
	if minimum := ssnmPartitionBaseBytes + ssnmMinimumRecordBytes; resolved.MaxBytes < minimum {
		return SSNMStateConfig{}, fmt.Errorf(
			"%w: %d bytes cannot hold one partition and one record, which need %d",
			ErrInvalidSSNMStateConfig, resolved.MaxBytes, minimum)
	}
	if resolved.SubscriptionQueueBytes < ssnmEventBaseBytes {
		return SSNMStateConfig{}, fmt.Errorf("%w: subscription byte budget %d cannot hold one event, which needs at least %d",
			ErrInvalidSSNMStateConfig, resolved.SubscriptionQueueBytes, ssnmEventBaseBytes)
	}
	return resolved, nil
}

func newSSNMState(config *SSNMStateConfig) (*ssnmState, error) {
	limits, err := resolveSSNMStateConfig(config)
	if err != nil {
		return nil, err
	}
	return &ssnmState{
		limits:      limits,
		partitions:  make(map[SSNMPartition]*ssnmPartitionState),
		peerRecords: make(map[ssnmPeerKey]int),
		subscribers: make(map[*SSNMSubscription]struct{}),
	}, nil
}

func ssnmScopeBytes(scope WireScope, resolved ...[]ASKey) int {
	bytes := ssnmRecordBaseBytes + ssnmRoutingContextBytes*len(scope.RoutingContexts)
	if len(resolved) > 0 {
		bytes += ssnmResolvedASBytes * len(resolved[0])
	}
	return bytes
}

// affectedPointCodeLimit reports the configured Affected Point Code bound.
func (s *ssnmState) affectedPointCodeLimit() int {
	if s == nil {
		return DefaultMaxAffectedPointCodesPerSSNM
	}
	return s.limits.MaxAffectedPointCodes
}

func (s *ssnmState) nextRevisionLocked() uint64 {
	s.revision++
	return s.revision
}

// bind admits one Association to a partition.
//
// A partition with no binding at all starts a new epoch: its previous
// knowledge was retired with its last binding, so what a later binding learns
// is not a continuation of it. Numbering those generations is a library
// choice, not an RFC procedure; it exists so a report validated under one
// binding is recognisable after a reactivation replaced it.
func (s *ssnmState) bind(partition SSNMPartition, association AssociationID, pending bool) error {
	if s == nil || partition.Kind == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bindLocked(partition, association, pending)
}

// bindLocked retains bind's admission and lifecycle behavior under s.mu.
func (s *ssnmState) bindLocked(partition SSNMPartition, association AssociationID, pending bool) error {
	if s.closed {
		return ErrEndpointClosed
	}
	if _, live := s.liveSources[association]; !live {
		return ErrAssociationClosed
	}
	state, exists := s.partitions[partition]
	if !exists {
		if len(s.partitions) >= s.limits.MaxPartitions {
			s.refuseLocked(1, fmt.Sprintf("partition limit %d reached; %s refused",
				s.limits.MaxPartitions, describeSSNMPartition(partition)))
			return fmt.Errorf("%w: %d partitions retained, limit %d",
				ErrSSNMStateLimit, len(s.partitions), s.limits.MaxPartitions)
		}
		if s.bytes+ssnmPartitionBaseBytes > s.limits.MaxBytes {
			s.refuseLocked(1, fmt.Sprintf("byte limit %d reached; %s refused",
				s.limits.MaxBytes, describeSSNMPartition(partition)))
			return fmt.Errorf("%w: %d bytes retained, limit %d",
				ErrSSNMStateLimit, s.bytes, s.limits.MaxBytes)
		}
		// A partition exists only while something binds it, so creating one
		// is exactly where a binding generation begins.
		s.epochs++
		state = &ssnmPartitionState{
			partition:    partition,
			epoch:        s.epochs,
			bindings:     make(map[AssociationID]bool),
			availability: make(map[ssnmDestinationKey]ssnmAvailabilityRecord),
			congestion:   make(map[ssnmDestinationKey]ssnmCongestionRecord),
			bytes:        ssnmPartitionBaseBytes,
		}
		s.partitions[partition] = state
		s.bytes += ssnmPartitionBaseBytes
	}
	previous, bound := state.bindings[association]
	if bound && previous == pending {
		return nil
	}
	state.bindings[association] = pending
	// A binding that was pending and is now complete is the activation
	// acknowledgment arriving, not a fresh admission. It keeps what the
	// Section 4.5.1 window admitted.
	kind := SSNMBindingAdmittedEvent
	if bound && previous && !pending {
		kind = SSNMBindingActivatedEvent
	}
	revision := s.nextRevisionLocked()
	s.publishLocked(SSNMEvent{
		Kind:      kind,
		Revision:  revision,
		Partition: partition,
		Epoch:     state.epoch,
		Binding:   SSNMBinding{Association: association, Pending: pending},
	})
	return nil
}

// retire withdraws one binding. Losing the last binding of a partition retires
// the partition and invalidates its knowledge in the same critical section, so
// no reader ever sees knowledge with no owner.
//
// A sibling binding of the same canonical Signalling Gateway and Application
// Server preserves that knowledge; an unrelated Application Server is a
// different partition and cannot.
func (s *ssnmState) retire(partition SSNMPartition, association AssociationID) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.retireLocked(partition, association)
}

func (s *ssnmState) retireLocked(partition SSNMPartition, association AssociationID) {
	state, exists := s.partitions[partition]
	if !exists {
		return
	}
	if _, bound := state.bindings[association]; !bound {
		return
	}
	s.retirePeerRecordsLocked(state, func(record PeerCongestionState) bool { return record.Association == association })
	delete(state.bindings, association)
	epoch := state.epoch
	if len(state.bindings) > 0 {
		revision := s.nextRevisionLocked()
		s.publishLocked(SSNMEvent{
			Kind:      SSNMBindingRetiredEvent,
			Revision:  revision,
			Partition: partition,
			Epoch:     epoch,
			Binding:   SSNMBinding{Association: association},
		})
		return
	}
	s.dropDestinationRecordsLocked(state)
	delete(s.partitions, partition)
	s.bytes -= ssnmPartitionBaseBytes
	s.partitionsInvalidated++
	revision := s.nextRevisionLocked()
	s.publishLocked(SSNMEvent{
		Kind:      SSNMPartitionRetiredEvent,
		Revision:  revision,
		Partition: partition,
		Epoch:     epoch,
		Binding:   SSNMBinding{Association: association},
	})
}

// registerSource runs when the Endpoint first owns an Association, before it
// exposes the Association or reconciles bindings. Report processing must never
// register a source: late work cannot restore a permanently departed owner.
func (s *ssnmState) registerSource(association AssociationID) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	if s.liveSources == nil {
		s.liveSources = make(map[AssociationID]struct{})
	}
	s.liveSources[association] = struct{}{}
}

// retireAssociation withdraws every binding one Association holds and removes
// its source membership in the same critical section. Late bind and apply
// operations are refused without retaining departed Association IDs.
func (s *ssnmState) retireAssociation(association AssociationID) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.peerSources, association)
	delete(s.liveSources, association)
	if len(s.liveSources) == 0 {
		s.liveSources = nil
	}
	partitions := make([]SSNMPartition, 0, len(s.partitions))
	for partition, state := range s.partitions {
		if _, bound := state.bindings[association]; bound {
			partitions = append(partitions, partition)
		}
	}
	sortSSNMPartitions(partitions)
	for _, partition := range partitions {
		s.retireLocked(partition, association)
	}
}

// dropDestinationRecordsLocked leaves independently scoped peer congestion
// intact. Callers retiring its owner remove peer records separately.
func (s *ssnmState) dropDestinationRecordsLocked(state *ssnmPartitionState) {
	peer := state.partition.peerKey()
	released := len(state.availability) + len(state.congestion)
	releasedBytes := 0
	for _, record := range state.availability {
		releasedBytes += ssnmScopeBytes(record.Scope, record.resolved)
	}
	for _, record := range state.congestion {
		releasedBytes += ssnmScopeBytes(record.Scope, record.resolved)
	}
	s.records -= released
	s.peerRecords[peer] -= released
	if s.peerRecords[peer] <= 0 {
		delete(s.peerRecords, peer)
	}
	s.bytes -= releasedBytes
	state.bytes -= releasedBytes
	state.availability = make(map[ssnmDestinationKey]ssnmAvailabilityRecord)
	state.congestion = make(map[ssnmDestinationKey]ssnmCongestionRecord)
}

// invalidateLocked discards one partition's retained knowledge without
// retiring its bindings. It is the conservative answer to a report this node
// could not apply: holding state that a report we declined to read may have
// contradicted is worse than holding none.
func (s *ssnmState) invalidateLocked(partition SSNMPartition, reason string) {
	state, exists := s.partitions[partition]
	if !exists {
		return
	}
	if state.records() == 0 {
		return
	}
	s.retirePeerRecordsLocked(state, func(PeerCongestionState) bool { return true })
	s.invalidateDestinationKnowledgeLocked(state, reason)
}

// invalidateDestinationKnowledgeLocked preserves peer congestion: RFC 4666
// Sections 1.3.2.4 and 3.4.4 distinguish ASP state from SS7 destination state.
func (s *ssnmState) invalidateDestinationKnowledgeLocked(state *ssnmPartitionState, reason string) {
	s.invalidateScopedDestinationKnowledgeLocked(state, reason, ssnmDestinationRefusal{})
}

// invalidateScopedDestinationKnowledgeLocked removes whole dimensions whose
// retained scopes overlap the refused report. APCs are never expanded here.
func (s *ssnmState) invalidateScopedDestinationKnowledgeLocked(state *ssnmPartitionState, reason string, refusal ssnmDestinationRefusal) {
	var keys []ssnmDestinationKey
	for key, record := range state.availability {
		if refusal.matches(state.partition, record.Scope, record.resolved) {
			keys = append(keys, key)
		}
	}
	for key, record := range state.congestion {
		if refusal.matches(state.partition, record.Scope, record.resolved) {
			keys = append(keys, key)
		}
	}
	slices.SortFunc(keys, compareSSNMDestinationKeys)
	keys = slices.Compact(keys)
	removed := make([]SSNMDestinationInvalidation, 0, len(keys))
	release := func(scope WireScope, resolved []ASKey) {
		s.records--
		peer := state.partition.peerKey()
		s.peerRecords[peer]--
		if s.peerRecords[peer] == 0 {
			delete(s.peerRecords, peer)
		}
		bytes := ssnmScopeBytes(scope, resolved)
		s.bytes -= bytes
		state.bytes -= bytes
	}
	for _, key := range keys {
		delta := SSNMDestinationInvalidation{Destination: key.pointCodeRange()}
		if record, held := state.availability[key]; held && refusal.matches(state.partition, record.Scope, record.resolved) {
			delta.AvailabilityRemoved = true
			delete(state.availability, key)
			release(record.Scope, record.resolved)
		}
		if record, held := state.congestion[key]; held && refusal.matches(state.partition, record.Scope, record.resolved) {
			delta.CongestionRemoved = true
			delete(state.congestion, key)
			release(record.Scope, record.resolved)
		}
		if delta.AvailabilityRemoved || delta.CongestionRemoved {
			removed = append(removed, delta)
		}
	}
	// Epochs uniquely identify partitions across this store's lifetime. Leaving
	// Partition empty avoids replaying unbounded configured identity strings.
	// Each publication is bounded independently of custom retention limits.
	for len(removed) > 0 {
		count := min(len(removed), ssnmInvalidationBatchSize)
		s.partitionsInvalidated++
		revision := s.nextRevisionLocked()
		s.publishLocked(SSNMEvent{Kind: SSNMDestinationsInvalidatedEvent, Revision: revision,
			Epoch: state.epoch, Invalidated: removed[:count:count], Reason: boundSSNMReason(reason)})
		removed = removed[count:]
	}
}

// ssnmDimensionWrite is one prepared record write, resolved before anything is
// stored so a report is applied whole or not at all.
type ssnmDimensionWrite struct {
	key          ssnmDestinationKey
	availability bool
	newRecord    bool
	bytes        int
	previous     int
}

// apply retains one locally validated report and publishes it.
//
// Within one canonical partition and one dimension the last locally validated
// report wins. That is a local ordering over what this node accepted, not a
// claim about the order in which the peers produced them. RFC 4666 Section
// 4.5.1 orders one SGP's own stream -- "DUNA, DAVA, SCON, and DRST messages
// may be sent sequentially and processed at the receiver in the order sent",
// while "Sequencing is not required for the DUPU or DAUD messages" -- and says
// nothing about order between the SGPs of one Signalling Gateway, so there is
// no remote causal order to reconstruct.
func (s *ssnmState) apply(report SSNMReport, resolution ...[]ASKey) error {
	if s == nil || report.Partition.Kind == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrEndpointClosed
	}
	if _, live := s.liveSources[report.Association]; !live {
		return ErrAssociationClosed
	}
	state, exists := s.partitions[report.Partition]
	if !exists {
		// A report for a partition with no binding is knowledge with no owner.
		// It is delivered as an event and retained by nobody.
		return s.publishEventOnlyLocked(report)
	}
	report.Epoch = state.epoch
	if !report.retainsAvailability() && !report.retainsCongestion() {
		return s.publishEventOnlyLocked(report)
	}

	var resolved []ASKey
	if len(resolution) > 0 {
		resolved = resolution[0]
	}
	scopeBytes := ssnmScopeBytes(report.Scope, resolved)
	availability := report.retainsAvailability()
	// The written keys are deduplicated by sorting rather than through a set,
	// so a one-APC report builds nothing proportional to anything but itself,
	// and in point-code then mask order, which is the order they are
	// published in.
	var small [4]ssnmDimensionWrite
	writes := small[:0]
	if len(report.Destinations) > len(small) {
		writes = make([]ssnmDimensionWrite, 0, len(report.Destinations))
	}
	for _, destination := range report.Destinations {
		writes = append(writes, ssnmDimensionWrite{key: ssnmDestinationKeyFor(destination)})
	}
	slices.SortFunc(writes, func(first, second ssnmDimensionWrite) int {
		return compareSSNMDestinationKeys(first.key, second.key)
	})
	writes = slices.CompactFunc(writes, func(first, second ssnmDimensionWrite) bool {
		return first.key == second.key
	})
	newRecords := 0
	newBytes := 0
	for index := range writes {
		write := &writes[index]
		write.availability = availability
		write.bytes = scopeBytes
		if availability {
			existing, held := state.availability[write.key]
			write.newRecord = !held
			if held {
				write.previous = ssnmScopeBytes(existing.Scope, existing.resolved)
			}
		} else {
			existing, held := state.congestion[write.key]
			write.newRecord = !held
			if held {
				write.previous = ssnmScopeBytes(existing.Scope, existing.resolved)
			}
		}
		if write.newRecord {
			newRecords++
		}
		newBytes += write.bytes - write.previous
	}
	if len(writes) == 0 {
		return s.publishEventOnlyLocked(report)
	}

	peer := report.Partition.peerKey()
	if err := s.admitLocked(state, peer, newRecords, newBytes, report); err != nil {
		return err
	}

	revision := s.nextRevisionLocked()
	report.Revision = revision
	// Records are replaced whole and never written in place, so every record
	// this report installs can share one owned copy of its wire scope.
	scope := report.Scope.clone()
	resolved = slices.Clone(resolved)
	for _, write := range writes {
		if write.availability {
			state.availability[write.key] = ssnmAvailabilityRecord{resolved: resolved, SSNMAvailability: SSNMAvailability{
				State:       report.availabilityState(),
				Kind:        report.Kind,
				Source:      report.Source,
				Scope:       scope,
				Association: report.Association,
				Epoch:       report.Epoch,
				Revision:    revision,
			}}
		} else {
			state.congestion[write.key] = ssnmCongestionRecord{resolved: resolved, SSNMCongestion: SSNMCongestion{
				Congested:   !report.CongestionLevelSet || report.CongestionLevel != 0,
				Level:       report.CongestionLevel,
				LevelSet:    report.CongestionLevelSet,
				Source:      report.Source,
				Scope:       scope,
				Association: report.Association,
				Epoch:       report.Epoch,
				Revision:    revision,
			}}
		}
		delta := write.bytes - write.previous
		state.bytes += delta
		s.bytes += delta
		if write.newRecord {
			s.records++
			s.peerRecords[peer]++
		}
	}
	event := SSNMEvent{
		Kind:      SSNMReportEvent,
		Revision:  revision,
		Partition: report.Partition,
		Epoch:     report.Epoch,
		Report:    report,
		ReportSet: true,
	}
	// The delta is read back from the store after the commit, so it is the
	// retained knowledge of exactly the destinations written, in both
	// dimensions. Every subscriber takes its own copy of it.
	if len(s.subscribers) > 0 {
		event.Updated = make([]SSNMDestinationKnowledge, len(writes))
		for index, write := range writes {
			event.Updated[index] = state.retainedKnowledge(write.key)
		}
	}
	s.publishLocked(event)
	return nil
}

// admitLocked checks every bound before anything is written. A report that
// cannot be retained whole is refused whole: applying the part that fits would
// leave retained state that no peer ever reported.
func (s *ssnmState) admitLocked(
	state *ssnmPartitionState,
	peer ssnmPeerKey,
	newRecords, newBytes int,
	report SSNMReport,
) error {
	if state.records()+newRecords > s.limits.MaxRecordsPerPartition {
		return s.refuseReportLocked(newRecords, fmt.Sprintf(
			"%s would hold %d records, partition limit %d",
			describeSSNMPartition(report.Partition), state.records()+newRecords,
			s.limits.MaxRecordsPerPartition))
	}
	if s.peerRecords[peer]+newRecords > s.limits.MaxRecordsPerPeer {
		return s.refuseReportLocked(newRecords, fmt.Sprintf(
			"peer of %s would hold %d records, peer limit %d",
			describeSSNMPartition(report.Partition), s.peerRecords[peer]+newRecords,
			s.limits.MaxRecordsPerPeer))
	}
	if s.records+newRecords > s.limits.MaxRecords {
		return s.refuseReportLocked(newRecords, fmt.Sprintf(
			"store would hold %d records, limit %d",
			s.records+newRecords, s.limits.MaxRecords))
	}
	if s.bytes+newBytes > s.limits.MaxBytes {
		return s.refuseReportLocked(newRecords, fmt.Sprintf(
			"store would hold %d bytes, limit %d",
			s.bytes+newBytes, s.limits.MaxBytes))
	}
	return nil
}

// refuseReportLocked records a refused report and publishes the loss.
//
// Nothing is evicted to make room. Eviction would let a peer naming point
// codes this node has never routed to push out a destination report it
// genuinely depends on, which is the opposite of a bound.
func (s *ssnmState) refuseReportLocked(records int, reason string) error {
	s.reportsRefused++
	s.refuseLocked(records, reason)
	return fmt.Errorf("%w: %s", ErrSSNMStateLimit, reason)
}

func (s *ssnmState) refuseLocked(records int, reason string) {
	if records > 0 {
		s.recordsRefused += uint64(records)
	}
	s.lastResourceLoss = boundSSNMReason(reason)
	revision := s.nextRevisionLocked()
	s.publishLocked(SSNMEvent{
		Kind:     SSNMResourceLossEvent,
		Revision: revision,
		Reason:   s.lastResourceLoss,
	})
}

// oversized handles an otherwise valid SSNM message naming more Affected Point
// Codes than this node accepts.
//
// It is a local resource event, not a protocol fault: the message is
// well-formed and RFC 4666 Section 3.8.1 has no Error condition for a receiver
// that will not expand it. The partitions the message concerned are
// invalidated only within the resolved AS/network scope because their state
// may contradict a report this node refused to read; the association
// stays up, and no truncated prefix of the message is applied.
// Peer congestion is retired separately, only for an ASP SCON with captured
// source ownership; other SSNM messages cannot contradict that peer state.
// DAUD is a query, not a statement, and invalidates nothing (RFC 4666 §4.5.3).
func (s *ssnmState) oversized(partitions []SSNMPartition, count, limit int, association AssociationID, refusal ssnmDestinationRefusal, peerRefusal ...peerCongestionRefusal) error {
	if s == nil {
		return nil
	}
	reason := fmt.Sprintf("SSNM on Association %d named %d Affected Point Codes, limit %d",
		association, count, limit)
	s.mu.Lock()
	s.reportsRefused++
	s.refuseLocked(0, reason)
	if !refusal.query {
		if len(peerRefusal) != 0 {
			s.retireRefusedPeerCongestionLocked(association, peerRefusal[0])
		}
		for _, partition := range partitions {
			if state := s.partitions[partition]; state != nil {
				s.invalidateScopedDestinationKnowledgeLocked(state, reason, refusal)
			}
		}
	}
	s.mu.Unlock()
	return fmt.Errorf("%w: %s", ErrSSNMOversizedReport, reason)
}

func (s *ssnmState) publishEventOnlyLocked(report SSNMReport) error {
	revision := s.nextRevisionLocked()
	report.Revision = revision
	s.publishLocked(SSNMEvent{
		Kind:      SSNMReportEvent,
		Revision:  revision,
		Partition: report.Partition,
		Epoch:     report.Epoch,
		Report:    report,
		ReportSet: true,
	})
	return nil
}

// boundSSNMReason keeps a resource diagnostic from growing with what the peer
// sent. A diagnostic that quotes an attacker's input without a bound is the
// resource problem it was added to report.
func boundSSNMReason(reason string) string {
	if len(reason) <= ssnmResourceReasonLength {
		return reason
	}
	return reason[:ssnmResourceReasonLength-3] + "..."
}

func describeSSNMPartition(partition SSNMPartition) string {
	if partition.Kind == SSNMStandalonePartition {
		return fmt.Sprintf("standalone Association %d", partition.Association)
	}
	return fmt.Sprintf("Application Server %q of Signalling Gateway %q",
		partition.ApplicationServer, partition.SignallingGateway)
}

// retainedKnowledge returns both retained dimensions of one destination.
// The scopes are the store's own and must be copied before they leave it.
func (p *ssnmPartitionState) retainedKnowledge(key ssnmDestinationKey) SSNMDestinationKnowledge {
	entry := SSNMDestinationKnowledge{Destination: key.pointCodeRange()}
	if availability, held := p.availability[key]; held {
		entry.Availability = availability.SSNMAvailability
		entry.AvailabilitySet = true
	}
	if congestion, held := p.congestion[key]; held {
		entry.Congestion = congestion.SSNMCongestion
		entry.CongestionSet = true
	}
	return entry
}

func compareSSNMDestinationKeys(first, second ssnmDestinationKey) int {
	if comparison := cmp.Compare(first.pointCode, second.pointCode); comparison != 0 {
		return comparison
	}
	return cmp.Compare(first.mask, second.mask)
}

// partitionDestinationsLocked returns an owned copy of every destination one
// partition retains, in point-code then mask order.
func (s *ssnmState) partitionDestinationsLocked(state *ssnmPartitionState) []SSNMDestinationKnowledge {
	if state == nil || len(state.availability)+len(state.congestion) == 0 {
		return nil
	}
	keys := make([]ssnmDestinationKey, 0, len(state.availability)+len(state.congestion))
	for key := range state.availability {
		keys = append(keys, key)
	}
	for key := range state.congestion {
		if _, duplicate := state.availability[key]; !duplicate {
			keys = append(keys, key)
		}
	}
	slices.SortFunc(keys, compareSSNMDestinationKeys)
	knowledge := make([]SSNMDestinationKnowledge, len(keys))
	for index, key := range keys {
		entry := state.retainedKnowledge(key)
		entry.Availability.Scope = entry.Availability.Scope.clone()
		entry.Congestion.Scope = entry.Congestion.Scope.clone()
		knowledge[index] = entry
	}
	return knowledge
}

func sortSSNMPartitions(partitions []SSNMPartition) {
	sort.Slice(partitions, func(i, j int) bool {
		return lessSSNMPartition(partitions[i], partitions[j])
	})
}

func lessSSNMPartition(first, second SSNMPartition) bool {
	if first.Kind != second.Kind {
		return first.Kind < second.Kind
	}
	if first.SignallingGateway != second.SignallingGateway {
		return first.SignallingGateway < second.SignallingGateway
	}
	if first.ApplicationServer != second.ApplicationServer {
		return first.ApplicationServer < second.ApplicationServer
	}
	return first.Association < second.Association
}

// snapshotLocked builds an owned view of the whole store.
func (s *ssnmState) snapshotLocked() SSNMSnapshot {
	partitions := make([]SSNMPartition, 0, len(s.partitions))
	for partition := range s.partitions {
		partitions = append(partitions, partition)
	}
	sortSSNMPartitions(partitions)
	knowledge := make([]SSNMPartitionKnowledge, 0, len(partitions))
	for _, partition := range partitions {
		state := s.partitions[partition]
		bindings := make([]SSNMBinding, 0, len(state.bindings))
		for association, pending := range state.bindings {
			bindings = append(bindings, SSNMBinding{Association: association, Pending: pending})
		}
		sort.Slice(bindings, func(i, j int) bool {
			return bindings[i].Association < bindings[j].Association
		})
		knowledge = append(knowledge, SSNMPartitionKnowledge{
			Partition:         partition,
			Epoch:             state.epoch,
			Bindings:          bindings,
			TrafficAuthorized: state.trafficAuthorized(),
			Destinations:      s.partitionDestinationsLocked(state),
		})
	}
	return SSNMSnapshot{
		Revision:              s.revision,
		Partitions:            knowledge,
		PeerCongestion:        s.peerCongestionSnapshotLocked(),
		RecordsRefused:        s.recordsRefused,
		ReportsRefused:        s.reportsRefused,
		PartitionsInvalidated: s.partitionsInvalidated,
		LastResourceLoss:      s.lastResourceLoss,
	}
}

func (s *ssnmState) snapshot() SSNMSnapshot {
	if s == nil {
		return SSNMSnapshot{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshotLocked()
}

// close releases the store and wakes every subscription with its terminal
// state.
func (s *ssnmState) close() {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.liveSources = nil
	s.peerSources = nil
	s.partitions = make(map[SSNMPartition]*ssnmPartitionState)
	s.peerRecords = make(map[ssnmPeerKey]int)
	s.records, s.bytes = 0, 0
	subscriptions := make([]*SSNMSubscription, 0, len(s.subscribers))
	for subscription := range s.subscribers {
		subscriptions = append(subscriptions, subscription)
	}
	s.subscribers = make(map[*SSNMSubscription]struct{})
	s.mu.Unlock()
	for _, subscription := range subscriptions {
		subscription.terminate(ErrEndpointClosed)
	}
}
