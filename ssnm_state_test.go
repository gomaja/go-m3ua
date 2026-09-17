package m3ua

import (
	"errors"
	"strings"
	"testing"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// ssnmPeerInventoryConfig provisions peers without any outbound route
// inventory. Two SGPs of one Signalling Gateway serve one Application Server
// under different Routing Contexts, a second Application Server shares one of
// those SGPs, and a second Signalling Gateway reuses the first one's wire
// scope for an Application Server of its own.
func ssnmPeerInventoryConfig() *ASPConfig {
	return &ASPConfig{
		SignallingGateways: []SignallingGatewayConfig{
			{
				ID: "sg-a",
				SGPs: []SignallingGatewayProcessConfig{
					{
						ID: "sgp-a1",
						ApplicationServers: []RemoteASConfig{
							{ID: "as-core", ASKey: staticASKey(7, 1)},
							{ID: "as-edge", ASKey: staticASKey(7, 3)},
						},
					},
					{
						ID:                 "sgp-a2",
						ApplicationServers: []RemoteASConfig{{ID: "as-core", ASKey: staticASKey(7, 2)}},
					},
				},
			},
			{
				ID: "sg-b",
				SGPs: []SignallingGatewayProcessConfig{
					{
						ID:                 "sgp-b1",
						ApplicationServers: []RemoteASConfig{{ID: "as-core", ASKey: staticASKey(7, 1)}},
					},
				},
			},
		},
	}
}

func newSSNMStateEndpoint(t *testing.T, config *ASPConfig, limits *SSNMStateConfig) *Endpoint {
	t.Helper()
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: config, SSNMState: limits})
	if err != nil {
		t.Fatalf("NewEndpoint: %v", err)
	}
	t.Cleanup(func() { _ = endpoint.Close() })
	return endpoint
}

// attachSSNMAssociation attaches an active Association whose Routing Contexts
// the peer has already acknowledged.
func attachSSNMAssociation(
	t *testing.T,
	endpoint *Endpoint,
	identity SGPIdentity,
	networkAppearance uint32,
	routingContexts ...uint32,
) *Association {
	t.Helper()
	association, _ := newTestConnWithContexts(t, StateASPActive, RoleASP, routingContexts...)
	association.cfg.NetworkAppearance = params.NewNetworkAppearance(networkAppearance)
	peer := identity
	association.cfg.PeerSGP = &peer
	association.noteRoutingContextsAcked(params.NewRoutingContext(routingContexts...))
	if !endpoint.trackAssociation(association) {
		t.Fatalf("failed to attach Association to SGP %+v", identity)
	}
	t.Cleanup(func() { _ = association.Close() })
	return association
}

func canonicalSSNMPartition(signallingGateway SignallingGatewayID, applicationServer RemoteASID) SSNMPartition {
	return SSNMPartition{
		Kind:              SSNMCanonicalPartition,
		SignallingGateway: signallingGateway,
		ApplicationServer: applicationServer,
	}
}

func ssnmPartitionKnowledge(
	t *testing.T,
	snapshot SSNMSnapshot,
	partition SSNMPartition,
) SSNMPartitionKnowledge {
	t.Helper()
	for _, knowledge := range snapshot.Partitions {
		if knowledge.Partition == partition {
			return knowledge
		}
	}
	t.Fatalf("partition %+v is absent from snapshot %+v", partition, snapshot.Partitions)
	return SSNMPartitionKnowledge{}
}

func ssnmPartitionPresent(snapshot SSNMSnapshot, partition SSNMPartition) bool {
	for _, knowledge := range snapshot.Partitions {
		if knowledge.Partition == partition {
			return true
		}
	}
	return false
}

func ssnmDestination(
	t *testing.T,
	knowledge SSNMPartitionKnowledge,
	pointCode uint32,
	mask uint8,
) SSNMDestinationKnowledge {
	t.Helper()
	for _, destination := range knowledge.Destinations {
		if destination.Destination.PointCode == pointCode && destination.Destination.Mask == mask {
			return destination
		}
	}
	t.Fatalf("destination %#x/%d is absent from partition %+v: %+v",
		pointCode, mask, knowledge.Partition, knowledge.Destinations)
	return SSNMDestinationKnowledge{}
}

func sendDUNA(t *testing.T, c *Association, networkAppearance, routingContext uint32, pointCodes ...uint32) {
	t.Helper()
	if err := c.handleDestinationUnavailable(messages.NewDestinationUnavailable(
		params.NewNetworkAppearance(networkAppearance),
		params.NewRoutingContext(routingContext),
		params.NewAffectedPointCode(pointCodes...),
		nil,
	)); err != nil {
		t.Fatalf("DUNA: %v", err)
	}
}

func sendDAVA(t *testing.T, c *Association, networkAppearance, routingContext uint32, pointCodes ...uint32) {
	t.Helper()
	if err := c.handleDestinationAvailable(messages.NewDestinationAvailable(
		params.NewNetworkAppearance(networkAppearance),
		params.NewRoutingContext(routingContext),
		params.NewAffectedPointCode(pointCodes...),
		nil,
	)); err != nil {
		t.Fatalf("DAVA: %v", err)
	}
}

func sendDRST(t *testing.T, c *Association, networkAppearance, routingContext uint32, pointCodes ...uint32) {
	t.Helper()
	if err := c.handleDestinationRestricted(messages.NewDestinationRestricted(
		params.NewNetworkAppearance(networkAppearance),
		params.NewRoutingContext(routingContext),
		params.NewAffectedPointCode(pointCodes...),
		nil,
	)); err != nil {
		t.Fatalf("DRST: %v", err)
	}
}

func sendSCON(
	t *testing.T,
	c *Association,
	networkAppearance, routingContext uint32,
	level *uint8,
	pointCodes ...uint32,
) {
	t.Helper()
	var indications *params.Param
	if level != nil {
		indications = params.NewCongestionIndications(*level)
	}
	if err := c.handleSignallingCongestion(messages.NewSignallingCongestion(
		params.NewNetworkAppearance(networkAppearance),
		params.NewRoutingContext(routingContext),
		params.NewAffectedPointCode(pointCodes...),
		nil,
		indications,
		nil,
	)); err != nil {
		t.Fatalf("SCON: %v", err)
	}
}

func congestionLevel(level uint8) *uint8 { return &level }

// Bullet: SSNM before local route creation and zero-route retention; later
// routes see retained valid state.
//
// RFC 4666 Section 3.4.1 has the SG report destinations it has determined are
// unreachable. Nothing in Section 3.4 conditions that report on the receiver
// owning a route to the destination, so knowledge that arrives before any
// outbound route exists has to be kept.
func TestSSNMStateRetainsKnowledgeWithoutAnyOutboundRoute(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), nil)
	if endpoint.aspRoutes.routingConfigured() {
		t.Fatal("fixture provisions an outbound route inventory; it must not")
	}
	association := attachSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}, 7, 1)

	sendDUNA(t, association, 7, 1, 0x123456)

	partition := canonicalSSNMPartition("sg-a", "as-core")
	knowledge := ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(), partition)
	destination := ssnmDestination(t, knowledge, 0x123456, 0)
	if !destination.AvailabilitySet || destination.Availability.State != DestinationUnavailable {
		t.Fatalf("availability = %+v, set %v, want Unavailable",
			destination.Availability, destination.AvailabilitySet)
	}
	if destination.Availability.Kind != SSNMDestinationUnavailableReport {
		t.Errorf("report kind = %v, want DUNA", destination.Availability.Kind)
	}
	if destination.Availability.Source != SSNMPeerReport {
		t.Errorf("source = %v, want peer", destination.Availability.Source)
	}
	if destination.Availability.Association != association.ID() {
		t.Errorf("association = %d, want %d", destination.Availability.Association, association.ID())
	}
	if !destination.Availability.Scope.NetworkAppearanceSet ||
		destination.Availability.Scope.NetworkAppearance != 7 ||
		!destination.Availability.Scope.RoutingContextSet ||
		len(destination.Availability.Scope.RoutingContexts) != 1 ||
		destination.Availability.Scope.RoutingContexts[0] != 1 {
		t.Errorf("wire scope = %+v, want exactly NA 7 and RC 1", destination.Availability.Scope)
	}
	if knowledge.Epoch == 0 {
		t.Error("epoch = 0, want a binding generation")
	}
}

// A consumer that appears only after the reports did still sees them: the
// knowledge is the store's, not the reporting message's.
func TestSSNMStateRetainedKnowledgeReachesLaterConsumers(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), nil)
	association := attachSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}, 7, 1)
	sendDUNA(t, association, 7, 1, 0x123456)

	snapshot, subscription, err := endpoint.SubscribeSSNM()
	if err != nil {
		t.Fatalf("SubscribeSSNM: %v", err)
	}
	defer func() { _ = subscription.Close() }()

	knowledge := ssnmPartitionKnowledge(t, snapshot, canonicalSSNMPartition("sg-a", "as-core"))
	destination := ssnmDestination(t, knowledge, 0x123456, 0)
	if destination.Availability.State != DestinationUnavailable {
		t.Fatalf("late consumer saw %v, want Unavailable", destination.Availability.State)
	}
}

// Bullet: dimension-specific updates cannot accidentally clear other
// conditions.
//
// RFC 4666 Section 4.5.2.2 keeps availability and congestion as two statuses
// of one destination, and Section 3.4.4 makes congestion level zero "No
// Congestion or Undefined" — an abatement, not a restoration of reachability.
func TestSSNMStateKeepsAvailabilityAndCongestionIndependent(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), nil)
	association := attachSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}, 7, 1)
	partition := canonicalSSNMPartition("sg-a", "as-core")

	sendDUNA(t, association, 7, 1, 0x123456)
	sendSCON(t, association, 7, 1, congestionLevel(2), 0x123456)

	destination := ssnmDestination(t,
		ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(), partition), 0x123456, 0)
	if destination.Availability.State != DestinationUnavailable {
		t.Fatalf("SCON changed availability to %v", destination.Availability.State)
	}
	if !destination.CongestionSet || !destination.Congestion.Congested ||
		destination.Congestion.Level != 2 || !destination.Congestion.LevelSet {
		t.Fatalf("congestion = %+v, set %v, want level 2 congested", destination.Congestion, destination.CongestionSet)
	}

	// An explicit level zero abates congestion and must leave the destination
	// unavailable: only DAVA restores reachability.
	sendSCON(t, association, 7, 1, congestionLevel(0), 0x123456)
	destination = ssnmDestination(t,
		ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(), partition), 0x123456, 0)
	if destination.Availability.State != DestinationUnavailable {
		t.Fatalf("SCON level 0 restored availability to %v", destination.Availability.State)
	}
	if destination.Congestion.Congested || !destination.Congestion.LevelSet ||
		destination.Congestion.Level != 0 {
		t.Fatalf("congestion after abatement = %+v, want an explicit uncongested level 0", destination.Congestion)
	}

	// DAVA restores reachability and must leave the congestion dimension alone.
	sendSCON(t, association, 7, 1, congestionLevel(3), 0x123456)
	sendDAVA(t, association, 7, 1, 0x123456)
	destination = ssnmDestination(t,
		ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(), partition), 0x123456, 0)
	if destination.Availability.State != DestinationAvailable {
		t.Fatalf("DAVA left availability at %v", destination.Availability.State)
	}
	if !destination.Congestion.Congested || destination.Congestion.Level != 3 {
		t.Fatalf("DAVA cleared congestion: %+v", destination.Congestion)
	}

	// DRST moves only the availability dimension.
	sendDRST(t, association, 7, 1, 0x123456)
	destination = ssnmDestination(t,
		ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(), partition), 0x123456, 0)
	if destination.Availability.State != DestinationRestricted {
		t.Fatalf("DRST left availability at %v", destination.Availability.State)
	}
	if !destination.Congestion.Congested || destination.Congestion.Level != 3 {
		t.Fatalf("DRST cleared congestion: %+v", destination.Congestion)
	}
}

// A SCON without the optional Congestion Indications parameter reports
// congestion without a level, which is not the same as an explicit zero.
func TestSSNMStateDistinguishesAbsentCongestionLevelFromZero(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), nil)
	association := attachSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}, 7, 1)
	partition := canonicalSSNMPartition("sg-a", "as-core")

	sendSCON(t, association, 7, 1, nil, 0x123456)
	destination := ssnmDestination(t,
		ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(), partition), 0x123456, 0)
	if !destination.Congestion.Congested {
		t.Fatal("SCON without a level was not recorded as congestion")
	}
	if destination.Congestion.LevelSet {
		t.Fatalf("absent Congestion Indications reported as an explicit level: %+v", destination.Congestion)
	}
}

// Overlapping ranges are separate records. A report on a narrow range must not
// silently restate a wider one, or a DAVA for one point code would clear a
// cluster-wide DUNA.
func TestSSNMStateKeepsOverlappingRangesDistinct(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), nil)
	association := attachSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}, 7, 1)
	partition := canonicalSSNMPartition("sg-a", "as-core")

	if err := association.handleDestinationUnavailable(messages.NewDestinationUnavailable(
		params.NewNetworkAppearance(7),
		params.NewRoutingContext(1),
		params.NewAffectedPointCodeWithMask(8, 0x123400),
		nil,
	)); err != nil {
		t.Fatalf("cluster DUNA: %v", err)
	}
	sendDUNA(t, association, 7, 1, 0x123456)
	sendDAVA(t, association, 7, 1, 0x123456)

	knowledge := ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(), partition)
	if len(knowledge.Destinations) != 2 {
		t.Fatalf("retained %d destinations, want the cluster range and the member separately: %+v",
			len(knowledge.Destinations), knowledge.Destinations)
	}
	cluster := ssnmDestination(t, knowledge, 0x123400, 8)
	if cluster.Availability.State != DestinationUnavailable {
		t.Fatalf("member DAVA cleared the cluster range: %v", cluster.Availability.State)
	}
	member := ssnmDestination(t, knowledge, 0x123456, 0)
	if member.Availability.State != DestinationAvailable {
		t.Fatalf("member availability = %v, want Available", member.Availability.State)
	}
}

// Bullet: APC and every resource limit below, at and above the cap.
func TestSSNMAffectedPointCodeLimitBelowAtAndAboveTheCap(t *testing.T) {
	for _, tt := range []struct {
		name      string
		pointCode []uint32
		refused   bool
	}{
		{"below the cap", []uint32{0x123456, 0x123457}, false},
		{"at the cap", []uint32{0x123456, 0x123457, 0x123458}, false},
		{"above the cap", []uint32{0x123456, 0x123457, 0x123458, 0x123459}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(),
				&SSNMStateConfig{MaxAffectedPointCodes: 3})
			association := attachSSNMAssociation(t, endpoint, SGPIdentity{
				SignallingGateway:        "sg-a",
				SignallingGatewayProcess: "sgp-a1",
			}, 7, 1)
			err := association.handleDestinationUnavailable(messages.NewDestinationUnavailable(
				params.NewNetworkAppearance(7),
				params.NewRoutingContext(1),
				params.NewAffectedPointCode(tt.pointCode...),
				nil,
			))
			if tt.refused {
				if !errors.Is(err, ErrSSNMOversizedReport) {
					t.Fatalf("error = %v, want ErrSSNMOversizedReport", err)
				}
				snapshot := endpoint.SSNMKnowledge()
				if snapshot.ReportsRefused != 1 {
					t.Errorf("reports refused = %d, want 1", snapshot.ReportsRefused)
				}
				if ssnmPartitionPresent(snapshot, canonicalSSNMPartition("sg-a", "as-core")) {
					knowledge := ssnmPartitionKnowledge(t, snapshot, canonicalSSNMPartition("sg-a", "as-core"))
					if len(knowledge.Destinations) != 0 {
						t.Errorf("oversized report applied a truncated prefix: %+v", knowledge.Destinations)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("error = %v, want the report accepted", err)
			}
			knowledge := ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(),
				canonicalSSNMPartition("sg-a", "as-core"))
			if len(knowledge.Destinations) != len(tt.pointCode) {
				t.Fatalf("retained %d destinations, want %d", len(knowledge.Destinations), len(tt.pointCode))
			}
		})
	}
}

func TestSSNMStateRecordLimitBelowAtAndAboveTheCap(t *testing.T) {
	for _, tt := range []struct {
		name       string
		pointCodes []uint32
		refused    bool
	}{
		{"below the cap", []uint32{0x123456}, false},
		{"at the cap", []uint32{0x123456, 0x123457}, false},
		{"above the cap", []uint32{0x123456, 0x123457, 0x123458}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), &SSNMStateConfig{
				MaxRecords:             2,
				MaxRecordsPerPartition: 2,
				MaxRecordsPerPeer:      2,
			})
			association := attachSSNMAssociation(t, endpoint, SGPIdentity{
				SignallingGateway:        "sg-a",
				SignallingGatewayProcess: "sgp-a1",
			}, 7, 1)
			err := association.handleDestinationUnavailable(messages.NewDestinationUnavailable(
				params.NewNetworkAppearance(7),
				params.NewRoutingContext(1),
				params.NewAffectedPointCode(tt.pointCodes...),
				nil,
			))
			snapshot := endpoint.SSNMKnowledge()
			if tt.refused {
				if !errors.Is(err, ErrSSNMStateLimit) {
					t.Fatalf("error = %v, want ErrSSNMStateLimit", err)
				}
				knowledge := ssnmPartitionKnowledge(t, snapshot, canonicalSSNMPartition("sg-a", "as-core"))
				if len(knowledge.Destinations) != 0 {
					t.Fatalf("refused report was partially applied: %+v", knowledge.Destinations)
				}
				if snapshot.RecordsRefused == 0 {
					t.Error("refused records were not counted")
				}
				return
			}
			if err != nil {
				t.Fatalf("error = %v, want the report retained", err)
			}
			knowledge := ssnmPartitionKnowledge(t, snapshot, canonicalSSNMPartition("sg-a", "as-core"))
			if len(knowledge.Destinations) != len(tt.pointCodes) {
				t.Fatalf("retained %d destinations, want %d", len(knowledge.Destinations), len(tt.pointCodes))
			}
		})
	}
}

// The two dimensions are independent records, so a destination at the
// per-partition record cap can still be refused its congestion record.
func TestSSNMStatePerPartitionRecordLimitCountsBothDimensions(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), &SSNMStateConfig{
		MaxRecords:             8,
		MaxRecordsPerPartition: 1,
		MaxRecordsPerPeer:      8,
	})
	association := attachSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}, 7, 1)

	sendDUNA(t, association, 7, 1, 0x123456)
	err := association.handleSignallingCongestion(messages.NewSignallingCongestion(
		params.NewNetworkAppearance(7),
		params.NewRoutingContext(1),
		params.NewAffectedPointCode(0x123456),
		nil,
		params.NewCongestionIndications(2),
		nil,
	))
	if !errors.Is(err, ErrSSNMStateLimit) {
		t.Fatalf("congestion record at the partition cap: error = %v, want ErrSSNMStateLimit", err)
	}
	destination := ssnmDestination(t,
		ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(), canonicalSSNMPartition("sg-a", "as-core")),
		0x123456, 0)
	if destination.CongestionSet {
		t.Fatalf("refused congestion record was stored: %+v", destination.Congestion)
	}
	// Nothing is evicted to make room: a peer naming destinations this node
	// does not route to must not be able to push out a genuine DUNA.
	if destination.Availability.State != DestinationUnavailable {
		t.Fatalf("the refusal evicted the retained DUNA: %v", destination.Availability.State)
	}
}

// The peer budget is shared by every partition of one Signalling Gateway, so
// one Signalling Gateway cannot consume the whole Endpoint budget.
func TestSSNMStatePeerRecordLimitIsSharedAcrossOnePeersPartitions(t *testing.T) {
	// A partition belongs to exactly one peer, so a per-partition reservation
	// above the peer budget is impossible and is refused as configuration. The
	// peer cap is therefore isolated by giving one peer two partitions.
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), &SSNMStateConfig{
		MaxRecords:             8,
		MaxRecordsPerPartition: 1,
		MaxRecordsPerPeer:      1,
	})
	first := attachSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}, 7, 1, 3)
	sendDUNA(t, first, 7, 1, 0x123456)

	// A different Application Server of the same Signalling Gateway shares the
	// peer budget the first one has already filled.
	err := first.handleDestinationUnavailable(messages.NewDestinationUnavailable(
		params.NewNetworkAppearance(7),
		params.NewRoutingContext(3),
		params.NewAffectedPointCode(0x123457),
		nil,
	))
	if !errors.Is(err, ErrSSNMStateLimit) {
		t.Fatalf("second Application Server of one peer: error = %v, want ErrSSNMStateLimit", err)
	}
	// Its own partition is empty, so only the shared peer budget can have
	// refused it.
	if !strings.Contains(err.Error(), "peer limit") {
		t.Fatalf("refusal = %v, want the peer budget to be the one that refused", err)
	}

	// A different Signalling Gateway has its own budget and is unaffected.
	second := attachSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-b",
		SignallingGatewayProcess: "sgp-b1",
	}, 7, 1)
	if err := second.handleDestinationUnavailable(messages.NewDestinationUnavailable(
		params.NewNetworkAppearance(7),
		params.NewRoutingContext(1),
		params.NewAffectedPointCode(0x123458),
		nil,
	)); err != nil {
		t.Fatalf("alternative Signalling Gateway was refused its own budget: %v", err)
	}
}

func TestSSNMStatePartitionLimitRefusesANewPartition(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), &SSNMStateConfig{MaxPartitions: 1})
	first := attachSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}, 7, 1)
	sendDUNA(t, first, 7, 1, 0x123456)

	second := attachSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-b",
		SignallingGatewayProcess: "sgp-b1",
	}, 7, 1)
	if err := second.handleDestinationUnavailable(messages.NewDestinationUnavailable(
		params.NewNetworkAppearance(7),
		params.NewRoutingContext(1),
		params.NewAffectedPointCode(0x123457),
		nil,
	)); !errors.Is(err, ErrSSNMStateLimit) {
		t.Fatalf("partition beyond the cap: error = %v, want ErrSSNMStateLimit", err)
	}
	snapshot := endpoint.SSNMKnowledge()
	if len(snapshot.Partitions) != 1 {
		t.Fatalf("store holds %d partitions, want 1", len(snapshot.Partitions))
	}
	if ssnmPartitionPresent(snapshot, canonicalSSNMPartition("sg-b", "as-core")) {
		t.Fatal("refused partition was created anyway")
	}
	// The partition that was already there keeps its knowledge.
	knowledge := ssnmPartitionKnowledge(t, snapshot, canonicalSSNMPartition("sg-a", "as-core"))
	if len(knowledge.Destinations) != 1 {
		t.Fatalf("existing partition lost knowledge to the refusal: %+v", knowledge.Destinations)
	}
}

func TestSSNMStateByteLimitRefusesBeyondTheBudget(t *testing.T) {
	// One partition and one record fit exactly; the second record does not.
	limits := &SSNMStateConfig{MaxBytes: ssnmPartitionBaseBytes + ssnmRecordBaseBytes + 4}
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(), limits)
	association := attachSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}, 7, 1)

	sendDUNA(t, association, 7, 1, 0x123456)
	if err := association.handleDestinationUnavailable(messages.NewDestinationUnavailable(
		params.NewNetworkAppearance(7),
		params.NewRoutingContext(1),
		params.NewAffectedPointCode(0x123457),
		nil,
	)); !errors.Is(err, ErrSSNMStateLimit) {
		t.Fatalf("record beyond the byte budget: error = %v, want ErrSSNMStateLimit", err)
	}
	knowledge := ssnmPartitionKnowledge(t, endpoint.SSNMKnowledge(),
		canonicalSSNMPartition("sg-a", "as-core"))
	if len(knowledge.Destinations) != 1 {
		t.Fatalf("retained %d destinations, want the one that fit", len(knowledge.Destinations))
	}
}

// A reservation larger than the budget it is carved from is impossible: it can
// never bind, so it silently disables the inner limit rather than tightening it.
func TestSSNMStateConfigRejectsImpossibleReservations(t *testing.T) {
	for _, tt := range []struct {
		name   string
		limits SSNMStateConfig
	}{
		{"negative record limit", SSNMStateConfig{MaxRecords: -1}},
		{"negative byte limit", SSNMStateConfig{MaxBytes: -1}},
		{"negative partition limit", SSNMStateConfig{MaxPartitions: -1}},
		{"negative subscriber limit", SSNMStateConfig{MaxSubscribers: -1}},
		{"negative queue size", SSNMStateConfig{SubscriptionQueueSize: -1}},
		{"negative Affected Point Codes", SSNMStateConfig{MaxAffectedPointCodes: -1}},
		{"partition reservation exceeds the store", SSNMStateConfig{
			MaxRecords: 4, MaxRecordsPerPartition: 8, MaxRecordsPerPeer: 4,
		}},
		{"peer reservation exceeds the store", SSNMStateConfig{
			MaxRecords: 4, MaxRecordsPerPartition: 4, MaxRecordsPerPeer: 8,
		}},
		{"partition reservation exceeds its peer", SSNMStateConfig{
			MaxRecords: 8, MaxRecordsPerPartition: 4, MaxRecordsPerPeer: 2,
		}},
		{"byte budget cannot hold one record", SSNMStateConfig{MaxBytes: 1}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			limits := tt.limits
			_, err := NewEndpoint(EndpointConfig{
				Role:      RoleASP,
				ASP:       ssnmPeerInventoryConfig(),
				SSNMState: &limits,
			})
			if !errors.Is(err, ErrInvalidSSNMStateConfig) {
				t.Fatalf("NewEndpoint error = %v, want ErrInvalidSSNMStateConfig", err)
			}
		})
	}
}

// A reservation that exactly fills its budget is usable, so the refusal above
// is about impossibility rather than about the boundary itself.
func TestSSNMStateConfigAcceptsExactReservations(t *testing.T) {
	limits := SSNMStateConfig{MaxRecords: 4, MaxRecordsPerPartition: 4, MaxRecordsPerPeer: 4}
	if _, err := NewEndpoint(EndpointConfig{
		Role: RoleASP, ASP: ssnmPeerInventoryConfig(), SSNMState: &limits,
	}); err != nil {
		t.Fatalf("exact reservation refused: %v", err)
	}
	minimum := SSNMStateConfig{MaxBytes: ssnmPartitionBaseBytes + ssnmRecordBaseBytes}
	if _, err := NewEndpoint(EndpointConfig{
		Role: RoleASP, ASP: ssnmPeerInventoryConfig(), SSNMState: &minimum,
	}); err != nil {
		t.Fatalf("smallest usable byte budget refused: %v", err)
	}
}

// Bullet: oversized otherwise-valid SSNM invalidates the affected canonical
// partitions, preserves unrelated ones, reports loss and keeps the association
// up without fabricating a protocol Error.
func TestOversizedSSNMInvalidatesOnlyTheAffectedPartitions(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(),
		&SSNMStateConfig{MaxAffectedPointCodes: 1})
	first := attachSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}, 7, 1)
	second := attachSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-b",
		SignallingGatewayProcess: "sgp-b1",
	}, 7, 1)
	sendDUNA(t, first, 7, 1, 0x123456)
	sendDUNA(t, second, 7, 1, 0x123457)

	err := first.handleDestinationUnavailable(messages.NewDestinationUnavailable(
		params.NewNetworkAppearance(7),
		params.NewRoutingContext(1),
		params.NewAffectedPointCode(0x123458, 0x123459),
		nil,
	))
	if !errors.Is(err, ErrSSNMOversizedReport) {
		t.Fatalf("error = %v, want ErrSSNMOversizedReport", err)
	}

	snapshot := endpoint.SSNMKnowledge()
	affected := ssnmPartitionKnowledge(t, snapshot, canonicalSSNMPartition("sg-a", "as-core"))
	if len(affected.Destinations) != 0 {
		t.Fatalf("affected partition kept knowledge a refused report may contradict: %+v",
			affected.Destinations)
	}
	unrelated := ssnmPartitionKnowledge(t, snapshot, canonicalSSNMPartition("sg-b", "as-core"))
	if len(unrelated.Destinations) != 1 {
		t.Fatalf("unrelated partition lost knowledge: %+v", unrelated.Destinations)
	}
	if snapshot.PartitionsInvalidated != 1 {
		t.Errorf("partitions invalidated = %d, want 1", snapshot.PartitionsInvalidated)
	}
	if snapshot.LastResourceLoss == "" {
		t.Error("resource loss was not reported")
	}
	// The binding survives: the message was valid and the budget is local.
	if len(affected.Bindings) != 1 || affected.Bindings[0].Association != first.ID() {
		t.Errorf("bindings = %+v, want the reporting Association still bound", affected.Bindings)
	}
}

// A resource loss is not a protocol fault: RFC 4666 Section 3.8.1 has no Error
// condition for a receiver that will not expand an otherwise valid message.
func TestOversizedSSNMSendsNoProtocolErrorAndKeepsTheAssociation(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(),
		&SSNMStateConfig{MaxAffectedPointCodes: 1})
	association := attachSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}, 7, 1)

	association.reportSSNMFailure(nil, nil, association.handleDestinationUnavailable(
		messages.NewDestinationUnavailable(
			params.NewNetworkAppearance(7),
			params.NewRoutingContext(1),
			params.NewAffectedPointCode(0x123458, 0x123459),
			nil,
		)))
	if err := firstErr(association); err != nil {
		t.Fatalf("a local resource bound produced a protocol Error for the peer: %v", err)
	}
	if association.State() != StateASPActive {
		t.Fatalf("association left ASP-ACTIVE over a local resource bound: %v", association.State())
	}
}

// Bullet: fresh malformed or scope-invalid messages follow ordinary protocol
// validation rather than the resource-tolerance path.
func TestMalformedSSNMStillFollowsProtocolValidation(t *testing.T) {
	endpoint := newSSNMStateEndpoint(t, ssnmPeerInventoryConfig(),
		&SSNMStateConfig{MaxAffectedPointCodes: 1, MaxRecords: 1, MaxRecordsPerPartition: 1, MaxRecordsPerPeer: 1})
	association := attachSSNMAssociation(t, endpoint, SGPIdentity{
		SignallingGateway:        "sg-a",
		SignallingGatewayProcess: "sgp-a1",
	}, 7, 1)

	for _, tt := range []struct {
		name string
		send func() error
		want error
	}{
		{"DUNA without Affected Point Code", func() error {
			return association.handleDestinationUnavailable(messages.NewDestinationUnavailable(
				params.NewNetworkAppearance(7), params.NewRoutingContext(1), nil, nil))
		}, ErrMissingAffectedPointCode},
		{"DUPU without User/Cause", func() error {
			return association.handleDestinationUserPartUnavailable(
				messages.NewDestinationUserPartUnavailable(
					params.NewNetworkAppearance(7), params.NewRoutingContext(1),
					params.NewAffectedPointCode(0x123456), nil, nil))
		}, ErrMissingUserCause},
		{"SCON with a reserved congestion level", func() error {
			return association.handleSignallingCongestion(messages.NewSignallingCongestion(
				params.NewNetworkAppearance(7), params.NewRoutingContext(1),
				params.NewAffectedPointCode(0x123456), nil,
				params.NewCongestionIndications(4), nil))
		}, ErrInvalidParameterValue},
		{"DUPU naming a range", func() error {
			return association.handleDestinationUserPartUnavailable(
				messages.NewDestinationUserPartUnavailable(
					params.NewNetworkAppearance(7), params.NewRoutingContext(1),
					params.NewAffectedPointCodeWithMask(8, 0x123400),
					params.NewUserCause(1, 2), nil))
		}, ErrInvalidParameterValue},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.send()
			if !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want %v", err, tt.want)
			}
			if errors.Is(err, ErrSSNMResourceLoss) {
				t.Fatalf("a malformed message took the resource-tolerance path: %v", err)
			}
			// Resource tolerance must not become general tolerance: the peer
			// is still told about a message that was in error.
			for firstErr(association) != nil {
			}
			association.reportSSNMFailure(nil, nil, err)
			if reported := firstErr(association); reported == nil {
				t.Fatal("a malformed message produced no protocol Error for the peer")
			} else if !errors.Is(reported, tt.want) {
				t.Fatalf("reported error = %v, want %v", reported, tt.want)
			}
		})
	}
}

// A scope this Association may not act in is a protocol matter too, answered
// by RFC 4666 Section 3.8.1's Unexpected Message rather than as resource loss.
func TestScopeInvalidSSNMStillFollowsProtocolValidation(t *testing.T) {
	sgp, _ := newTestConnWithContexts(t, StateASPActive, RoleSGP, 1)
	err := sgp.handleDestinationUnavailable(messages.NewDestinationUnavailable(
		nil, params.NewRoutingContext(1), params.NewAffectedPointCode(0x123456), nil))
	var unexpected *UnexpectedMessageError
	if !errors.As(err, &unexpected) {
		t.Fatalf("SGP receiving DUNA: error = %v, want an Unexpected Message error", err)
	}
	if errors.Is(err, ErrSSNMResourceLoss) {
		t.Fatalf("a scope-invalid message took the resource-tolerance path: %v", err)
	}
}
