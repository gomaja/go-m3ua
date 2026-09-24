// Copyright 2018-2024 go-m3ua authors. All rights reserved.
// Use of this source code is governed by a MIT-style license that can be
// found in the LICENSE file.

package m3ua

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

func verifySSNMOperation(event, wantEvent SSNMEvent) error {
	if event.ContinuityLost || !reflect.DeepEqual(event, wantEvent) {
		return fmt.Errorf("typed event mismatch: kind=%s revision=%d updated=%d; want kind=%s revision=%d updated=%d", event.Kind, event.Revision, len(event.Updated), wantEvent.Kind, wantEvent.Revision, len(wantEvent.Updated))
	}
	return nil
}

type ssnmOperationCollector[Value any] struct {
	work    chan context.Context
	ready   chan struct{}
	done    chan struct{}
	stopped chan struct{}
	values  []Value
	count   int
	err     error
}

func newSSNMOperationCollector[Value any](owner testing.TB, capacity int, read func(context.Context) (Value, error)) *ssnmOperationCollector[Value] {
	collector := &ssnmOperationCollector[Value]{work: make(chan context.Context), ready: make(chan struct{}), done: make(chan struct{}, 1), stopped: make(chan struct{}), values: make([]Value, capacity)}
	go func() {
		defer close(collector.stopped)
		for ctx := range collector.work {
			collector.err = nil
			collector.ready <- struct{}{}
			for index := 0; index < collector.count; index++ {
				collector.values[index], collector.err = read(ctx)
				if collector.err != nil {
					break
				}
			}
			collector.done <- struct{}{}
		}
	}()
	owner.Cleanup(func() { close(collector.work); <-collector.stopped })
	return collector
}

func (collector *ssnmOperationCollector[Value]) start(ctx context.Context, count int) {
	collector.count = count
	collector.work <- ctx
	<-collector.ready
}

func (collector *ssnmOperationCollector[Value]) finish() ([]Value, error) {
	<-collector.done
	return collector.values[:collector.count], collector.err
}

type ssnmOperationPartition struct {
	association *Association
	identity    SSNMPartition
	scope       WireScope
	epoch       uint64
	ranges      []PointCodeRange
	records     map[PointCodeRange]SSNMDestinationKnowledge
}

type ssnmOperationCachedState struct {
	Destination PointCodeRange
	State       DestinationNetworkState
}

type ssnmOperationFixture struct {
	endpoint     *Endpoint
	partitions   []*ssnmOperationPartition
	revision     uint64
	subscription *SSNMSubscription
	typed        *ssnmOperationCollector[SSNMEvent]
}

type ssnmOperationInput struct {
	report         SSNMReport
	appearance     *params.Param
	routingContext *params.Param
	affected       *params.Param
	state          DestinationNetworkState
	dimensions     destinationDimensions
	routeUpdate    aspRouteUpdate
}

func ssnmOperationRanges(count int) []PointCodeRange {
	ranges := make([]PointCodeRange, count)
	for index := range ranges {
		mask := uint8(8)
		if index%2 != 0 {
			mask = 4
		}
		ranges[index] = PointCodeRange{PointCode: 0x10000 + uint32(index/2)*256, Mask: mask}
	}
	return ranges
}

func newSSNMOperationInput(partition *ssnmOperationPartition, ranges []PointCodeRange, congestion, asserted bool) ssnmOperationInput {
	words := make([]uint32, len(ranges))
	for index, destination := range ranges {
		words[index] = uint32(destination.Mask)<<24 | destination.PointCode
	}
	input := ssnmOperationInput{
		report:         SSNMReport{Kind: SSNMDestinationAvailableReport},
		appearance:     params.NewNetworkAppearance(partition.scope.NetworkAppearance),
		routingContext: params.NewRoutingContext(partition.scope.RoutingContexts...),
		affected:       params.NewAffectedPointCode(words...),
		dimensions:     destinationAvailabilityDimension,
		routeUpdate:    aspRouteUpdate{kind: aspRouteAvailabilityUpdate, availability: DestinationAvailable},
	}
	if asserted {
		input.report.Kind = SSNMDestinationUnavailableReport
		input.state.Availability = DestinationUnavailable
		input.routeUpdate.availability = DestinationUnavailable
	}
	if congestion {
		level := uint8(0)
		if asserted {
			level = 1
		}
		input.report = SSNMReport{Kind: SSNMSignallingCongestionReport, CongestionLevel: level, CongestionLevelSet: true}
		input.state = DestinationNetworkState{Congestion: CongestionState{Congested: asserted, Level: level, LevelSet: true}}
		input.dimensions = destinationCongestionDimension
		input.routeUpdate = aspRouteUpdate{kind: aspRouteCongestionUpdate, congested: asserted, congestionLevel: level, congestionLevelSet: true}
	}
	return input
}

func (input ssnmOperationInput) apply(association *Association) error {
	return association.applySSNM(input.report, input.appearance, input.routingContext, input.affected, input.state, input.dimensions, &input.routeUpdate)
}

func (input ssnmOperationInput) validate(association *Association) error {
	var message messages.M3UA
	switch input.report.Kind {
	case SSNMDestinationUnavailableReport:
		message = messages.NewDestinationUnavailable(input.appearance, input.routingContext, input.affected, nil)
	case SSNMDestinationAvailableReport:
		message = messages.NewDestinationAvailable(input.appearance, input.routingContext, input.affected, nil)
	case SSNMSignallingCongestionReport:
		message = messages.NewSignallingCongestion(input.appearance, input.routingContext, input.affected, nil, params.NewCongestionIndications(input.report.CongestionLevel), nil)
	default:
		return errors.New("unsupported fixture report")
	}
	encoded, err := messages.MarshalBinary(message)
	if err != nil {
		return err
	}
	decoded, err := messages.Parse(encoded)
	if err != nil {
		return err
	}
	if err := association.validateSSNMNetworkAppearance(input.appearance, input.routingContext); err != nil {
		return err
	}
	allowed, err := association.validateSSNMScope(decoded, input.routingContext, true)
	if err != nil {
		return err
	}
	if !allowed || !association.ssnmAllowedDuringActivation() {
		return errors.New("fixture report is not admitted")
	}
	_, _, err = association.ssnmAffectedPointCodes(input.affected, association.ssnmWireScope(input.appearance, input.routingContext))
	return err
}

func newSSNMOperationFixture(owner testing.TB, peers, partitionsPerPeer, rangesPerPartition int, subscribe bool) *ssnmOperationFixture {
	owner.Helper()
	previousProcs := runtime.GOMAXPROCS(4)
	owner.Cleanup(func() { runtime.GOMAXPROCS(previousProcs) })
	inventory := &ASPConfig{}
	for peerIndex := 0; peerIndex < peers; peerIndex++ {
		gateway := SignallingGatewayConfig{ID: SignallingGatewayID(fmt.Sprintf("fixture-sg-%02d", peerIndex))}
		process := SignallingGatewayProcessConfig{ID: "fixture-sgp"}
		for partitionIndex := 0; partitionIndex < partitionsPerPeer; partitionIndex++ {
			key := ASKey{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContext: uint32(partitionIndex + 1), RoutingContextSet: true}
			process.ApplicationServers = append(process.ApplicationServers, RemoteASConfig{ID: RemoteASID(fmt.Sprintf("fixture-as-%02d", partitionIndex)), ASKey: &key})
		}
		gateway.SGPs = []SignallingGatewayProcessConfig{process}
		inventory.SignallingGateways = append(inventory.SignallingGateways, gateway)
	}
	endpoint, err := NewEndpoint(EndpointConfig{Role: RoleASP, ASP: inventory, SSNMState: &SSNMStateConfig{MaxRecords: 16384, MaxRecordsPerPartition: 2048, MaxRecordsPerPeer: 8192, MaxPartitions: 128, MaxBytes: 16 << 20, MaxSubscribers: 8, SubscriptionQueueSize: 256, SubscriptionQueueBytes: 1 << 20, MaxAffectedPointCodes: 1024}})
	if err != nil {
		owner.Fatal(err)
	}
	owner.Cleanup(func() { _ = endpoint.Close() })
	fixture := &ssnmOperationFixture{endpoint: endpoint}
	for _, gateway := range inventory.SignallingGateways {
		config := NewAssociationConfig()
		config.PeerSGP = &SGPIdentity{SignallingGateway: gateway.ID, SignallingGatewayProcess: gateway.SGPs[0].ID}
		contexts := make([]uint32, 0, partitionsPerPeer)
		for _, remote := range gateway.SGPs[0].ApplicationServers {
			config.ApplicationServers = append(config.ApplicationServers, ASConfig{ASKey: *remote.ASKey})
			contexts = append(contexts, remote.ASKey.RoutingContext)
		}
		association := newAssociation(RoleASP, config)
		association.state = StateASPActive
		association.maxMessageStreamID = 15
		association.noteRoutingContextsAcked(params.NewRoutingContext(contexts...))
		if !endpoint.trackAssociation(association) {
			owner.Fatal("attach fixture association")
		}
		owner.Cleanup(func() { _ = association.Close() })
		for _, remote := range gateway.SGPs[0].ApplicationServers {
			fixture.partitions = append(fixture.partitions, &ssnmOperationPartition{association: association, identity: SSNMPartition{Kind: SSNMCanonicalPartition, SignallingGateway: gateway.ID, ApplicationServer: remote.ID}, scope: WireScope{NetworkAppearance: 7, NetworkAppearanceSet: true, RoutingContexts: []uint32{remote.ASKey.RoutingContext}, RoutingContextSet: true}, ranges: ssnmOperationRanges(rangesPerPartition), records: make(map[PointCodeRange]SSNMDestinationKnowledge)})
		}
	}
	initial := endpoint.SSNMKnowledge()
	if len(initial.Partitions) != len(fixture.partitions) {
		owner.Fatalf("initial partitions: got %d, want %d", len(initial.Partitions), len(fixture.partitions))
	}
	fixture.revision = initial.Revision
	for _, partition := range fixture.partitions {
		for _, actual := range initial.Partitions {
			if actual.Partition == partition.identity {
				partition.epoch = actual.Epoch
			}
		}
		if partition.epoch == 0 {
			owner.Fatal("missing initial partition epoch")
		}
		for offset := 0; offset < len(partition.ranges); offset += 32 {
			ranges := partition.ranges[offset:min(offset+32, len(partition.ranges))]
			for _, congestion := range []bool{false, true} {
				input := newSSNMOperationInput(partition, ranges, congestion, true)
				fixture.runUntimed(owner, partition, ranges, input)
			}
		}
	}
	if err := fixture.verifySnapshot(endpoint.SSNMKnowledge()); err != nil {
		owner.Fatal(err)
	}
	if subscribe {
		_, fixture.subscription, err = endpoint.SubscribeSSNM()
		if err != nil {
			owner.Fatal(err)
		}
		owner.Cleanup(func() { _ = fixture.subscription.Close() })
		fixture.typed = newSSNMOperationCollector(owner, 1, fixture.subscription.Next)
	}
	return fixture
}

func (fixture *ssnmOperationFixture) expected(partition *ssnmOperationPartition, ranges []PointCodeRange, input ssnmOperationInput) ([]ssnmOperationCachedState, SSNMEvent) {
	fixture.revision++
	report := input.report
	report.Source, report.Scope, report.Partition = SSNMPeerReport, partition.scope, partition.identity
	report.Association, report.Epoch, report.Revision = partition.association.ID(), partition.epoch, fixture.revision
	report.Destinations = ranges
	for _, destination := range ranges {
		knowledge := partition.records[destination]
		knowledge.Destination = destination
		if input.report.Kind == SSNMSignallingCongestionReport {
			knowledge.CongestionSet = true
			knowledge.Congestion = SSNMCongestion{Congested: input.state.Congestion.Congested, Level: report.CongestionLevel, LevelSet: true, Source: SSNMPeerReport, Scope: partition.scope, Association: report.Association, Epoch: report.Epoch, Revision: report.Revision}
		} else {
			knowledge.AvailabilitySet = true
			knowledge.Availability = SSNMAvailability{State: input.state.Availability, Kind: report.Kind, Source: SSNMPeerReport, Scope: partition.scope, Association: report.Association, Epoch: report.Epoch, Revision: report.Revision}
		}
		partition.records[destination] = knowledge
	}
	cached := make([]ssnmOperationCachedState, len(ranges))
	for index, destination := range ranges {
		state := input.state
		var availabilityRevision, congestionRevision uint64
		for held, knowledge := range partition.records {
			queryEnd := uint64(destination.PointCode) + (uint64(1) << destination.Mask) - 1
			heldEnd := uint64(held.PointCode) + (uint64(1) << held.Mask) - 1
			if held.PointCode > destination.PointCode || heldEnd < queryEnd {
				continue
			}
			if input.report.Kind == SSNMSignallingCongestionReport && knowledge.AvailabilitySet && knowledge.Availability.Revision > availabilityRevision {
				state.Availability, availabilityRevision = knowledge.Availability.State, knowledge.Availability.Revision
			}
			if input.report.Kind != SSNMSignallingCongestionReport && knowledge.CongestionSet && knowledge.Congestion.Revision > congestionRevision {
				state.Congestion = CongestionState{Congested: knowledge.Congestion.Congested, Level: knowledge.Congestion.Level, LevelSet: knowledge.Congestion.LevelSet}
				congestionRevision = knowledge.Congestion.Revision
			}
		}
		cached[index] = ssnmOperationCachedState{Destination: destination, State: state}
	}
	return cached, SSNMEvent{Kind: SSNMReportEvent, Revision: fixture.revision, Partition: partition.identity, Epoch: partition.epoch, Report: report, ReportSet: true, Updated: partition.expectedUpdated(ranges)}
}

// expectedUpdated is the delta a retained report publishes: the destinations
// it wrote, each once, in point-code then mask order, as retained after it.
func (partition *ssnmOperationPartition) expectedUpdated(ranges []PointCodeRange) []SSNMDestinationKnowledge {
	updated := make([]SSNMDestinationKnowledge, 0, len(ranges))
	seen := make(map[PointCodeRange]bool, len(ranges))
	for _, destination := range ranges {
		if seen[destination] {
			continue
		}
		seen[destination] = true
		updated = append(updated, partition.records[destination])
	}
	slices.SortFunc(updated, func(left, right SSNMDestinationKnowledge) int {
		if left.Destination.PointCode < right.Destination.PointCode {
			return -1
		}
		if left.Destination.PointCode > right.Destination.PointCode {
			return 1
		}
		return int(left.Destination.Mask) - int(right.Destination.Mask)
	})
	return updated
}

func (partition *ssnmOperationPartition) expectedStates() []SSNMDestinationKnowledge {
	states := make([]SSNMDestinationKnowledge, 0, len(partition.records))
	for _, state := range partition.records {
		states = append(states, state)
	}
	slices.SortFunc(states, func(left, right SSNMDestinationKnowledge) int {
		if left.Destination.PointCode < right.Destination.PointCode {
			return -1
		}
		if left.Destination.PointCode > right.Destination.PointCode {
			return 1
		}
		return int(left.Destination.Mask) - int(right.Destination.Mask)
	})
	return states
}

func (fixture *ssnmOperationFixture) verifySnapshot(snapshot SSNMSnapshot) error {
	return verifySSNMOperationSnapshot(snapshot, fixture.expectedSnapshot())
}

func (fixture *ssnmOperationFixture) expectedSnapshot() SSNMSnapshot {
	snapshot := SSNMSnapshot{Revision: fixture.revision, Partitions: make([]SSNMPartitionKnowledge, 0, len(fixture.partitions))}
	for _, partition := range fixture.partitions {
		snapshot.Partitions = append(snapshot.Partitions, SSNMPartitionKnowledge{Partition: partition.identity, Epoch: partition.epoch, Bindings: []SSNMBinding{{Association: partition.association.ID()}}, TrafficAuthorized: true, Destinations: partition.expectedStates()})
	}
	return snapshot
}

func verifySSNMOperationSnapshot(snapshot, expected SSNMSnapshot) error {
	if snapshot.Revision != expected.Revision || snapshot.RecordsRefused != 0 || snapshot.ReportsRefused != 0 || snapshot.PartitionsInvalidated != 0 || snapshot.LastResourceLoss != "" || len(snapshot.Partitions) != len(expected.Partitions) {
		return fmt.Errorf("snapshot revision/population/resource mismatch: revision=%d want=%d partitions=%d", snapshot.Revision, expected.Revision, len(snapshot.Partitions))
	}
	seen := make(map[SSNMPartition]bool)
	for _, actual := range snapshot.Partitions {
		if seen[actual.Partition] {
			return errors.New("duplicate snapshot partition")
		}
		seen[actual.Partition] = true
		var want *SSNMPartitionKnowledge
		for index := range expected.Partitions {
			if expected.Partitions[index].Partition == actual.Partition {
				want = &expected.Partitions[index]
				break
			}
		}
		if want == nil {
			return errors.New("unexpected snapshot partition")
		}
		if !reflect.DeepEqual(actual, *want) {
			return fmt.Errorf("snapshot partition mismatch: %+v", actual.Partition)
		}
	}
	return nil
}

func (fixture *ssnmOperationFixture) runUntimed(owner testing.TB, partition *ssnmOperationPartition, ranges []PointCodeRange, input ssnmOperationInput) {
	owner.Helper()
	if err := input.validate(partition.association); err != nil {
		owner.Fatal(err)
	}
	wantCached, wantEvent := fixture.expected(partition, ranges, input)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if fixture.typed != nil {
		fixture.typed.start(ctx, 1)
	}
	applyErr := input.apply(partition.association)
	if err := fixture.finish(partition, wantCached, wantEvent); err != nil {
		owner.Fatal(err)
	}
	if applyErr != nil {
		owner.Fatal(applyErr)
	}
}

func (fixture *ssnmOperationFixture) finish(partition *ssnmOperationPartition, wantCached []ssnmOperationCachedState, wantEvent SSNMEvent) error {
	if fixture.typed != nil {
		events, err := fixture.typed.finish()
		if err != nil {
			return fmt.Errorf("typed collector: %w", err)
		}
		if err := verifySSNMOperation(events[0], wantEvent); err != nil {
			return err
		}
	}
	if fixture.subscription != nil {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := fixture.subscription.Next(ctx); !errors.Is(err, context.Canceled) {
			return fmt.Errorf("extra typed event or continuity loss: %v", err)
		}
	}
	scope := destinationKey{
		networkAppearance:    partition.scope.NetworkAppearance,
		networkAppearanceSet: partition.scope.NetworkAppearanceSet,
		routingContext:       partition.scope.RoutingContexts[0],
		routingContextSet:    partition.scope.RoutingContextSet,
	}
	for _, expected := range wantCached {
		state, known := partition.association.destinations.lookupRange(scope, expected.Destination.PointCode, expected.Destination.Mask)
		if !known || state != expected.State {
			return fmt.Errorf("cached destination %+v: got %+v known=%v, want %+v", expected.Destination, state, known, expected.State)
		}
	}
	return nil
}

// BenchmarkSSNMOperationUpdate measures applying one parsed DUNA/DAVA or SCON
// through the production handlers, from decoded input to committed state and
// the enqueued typed event (section 3 of the performance budgets: one APC
// within 32 allocations, 8 KiB and a p99 of 1 ms; a valid 1,024-APC message
// within 4,096 allocations, 1 MiB and a p99 of 100 ms). Every iteration is
// checked by an independent oracle against the cached destination state and
// the subscriber's delta; a failed check invalidates the run. Updates use
// nested ranges and alternate between the availability and congestion
// dimensions. The latency percentiles come from the recorded per-operation
// durations, at least 1,000 per percentile at the default benchtime.
func BenchmarkSSNMOperationUpdate(benchmark *testing.B) {
	for _, count := range []int{1, 1024} {
		for _, congestion := range []bool{false, true} {
			dimension := "availability"
			if congestion {
				dimension = "congestion"
			}
			benchmark.Run(fmt.Sprintf("APC%d/%s", count, dimension), func(benchmark *testing.B) {
				benchmark.StopTimer()
				fixture := newSSNMOperationFixture(benchmark, 1, 1, max(2, count), true)
				partition := fixture.partitions[0]
				ranges := partition.ranges[:count]
				inputs := []ssnmOperationInput{newSSNMOperationInput(partition, ranges, congestion, false), newSSNMOperationInput(partition, ranges, congestion, true)}
				for _, input := range inputs {
					if err := input.validate(partition.association); err != nil {
						benchmark.Fatal(err)
					}
				}
				fixture.runUntimed(benchmark, partition, ranges, inputs[1])
				latencies := make([]int64, min(benchmark.N, 100000))
				benchmark.ReportAllocs()
				benchmark.ResetTimer()
				for iteration := 0; iteration < benchmark.N; iteration++ {
					input := inputs[iteration%2]
					wantCached, wantEvent := fixture.expected(partition, ranges, input)
					ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
					_ = ctx.Done()
					fixture.typed.start(ctx, 1)
					benchmark.StartTimer()
					started := time.Now()
					applyErr := input.apply(partition.association)
					elapsed := time.Since(started)
					benchmark.StopTimer()
					if iteration < len(latencies) {
						latencies[iteration] = int64(elapsed)
					}
					validationErr := fixture.finish(partition, wantCached, wantEvent)
					cancel()
					if applyErr != nil || validationErr != nil {
						benchmark.Fatalf("fixture invalid: apply=%v validation=%v", applyErr, validationErr)
					}
				}
				if err := fixture.verifySnapshot(fixture.endpoint.SSNMKnowledge()); err != nil {
					benchmark.Fatal(err)
				}
				reportSSNMOperationLatencies(benchmark, latencies)
			})
		}
	}
}

// BenchmarkSSNMOperationSnapshot measures the atomic snapshot and subscription
// of the approved population: 16,384 dimension records across 128 partitions
// and 32 peers (section 3: 32,768 allocations, 16 MiB and a p99 of 100 ms per
// call). Each snapshot is validated against the oracle's expected knowledge,
// and the subscription it returns is closed before the next call.
func BenchmarkSSNMOperationSnapshot(benchmark *testing.B) {
	benchmark.StopTimer()
	fixture := newSSNMOperationFixture(benchmark, 32, 4, 64, false)
	warmupSnapshot, warmupSubscription, err := fixture.endpoint.SubscribeSSNM()
	if err != nil {
		benchmark.Fatal(err)
	}
	if err := warmupSubscription.Close(); err != nil {
		benchmark.Fatal(err)
	}
	if err := fixture.verifySnapshot(warmupSnapshot); err != nil {
		benchmark.Fatal(err)
	}
	latencies := make([]int64, min(benchmark.N, 100000))
	benchmark.ReportAllocs()
	benchmark.ResetTimer()
	for iteration := 0; iteration < benchmark.N; iteration++ {
		benchmark.StartTimer()
		started := time.Now()
		snapshot, subscription, err := fixture.endpoint.SubscribeSSNM()
		elapsed := time.Since(started)
		benchmark.StopTimer()
		if iteration < len(latencies) {
			latencies[iteration] = int64(elapsed)
		}
		if err != nil {
			benchmark.Fatal(err)
		}
		if err := subscription.Close(); err != nil {
			benchmark.Fatal(err)
		}
		if err := fixture.verifySnapshot(snapshot); err != nil {
			benchmark.Fatal(err)
		}
	}
	reportSSNMOperationLatencies(benchmark, latencies)
}

func reportSSNMOperationLatencies(benchmark *testing.B, latencies []int64) {
	benchmark.Helper()
	benchmark.ReportMetric(float64(runtime.GOMAXPROCS(0)), "gomaxprocs")
	benchmark.ReportMetric(float64(len(latencies)), "latency-samples")
	if len(latencies) == benchmark.N && len(latencies) >= 1000 {
		ordered := slices.Clone(latencies)
		slices.Sort(ordered)
		benchmark.ReportMetric(float64(ordered[len(ordered)-len(ordered)/100-1]), "p99-ns/op")
	}
	if directory := os.Getenv("M3UA_SSNM_BENCH_RAW_DIR"); directory != "" {
		result := struct {
			Name        string  `json:"name"`
			Iterations  int     `json:"iterations"`
			Complete    bool    `json:"complete"`
			GOMAXPROCS  int     `json:"gomaxprocs"`
			Nanoseconds []int64 `json:"nanoseconds"`
		}{benchmark.Name(), benchmark.N, len(latencies) == benchmark.N, runtime.GOMAXPROCS(0), latencies}
		data, err := json.Marshal(result)
		if err != nil {
			benchmark.Fatal(err)
		}
		name := strings.ReplaceAll(benchmark.Name(), "/", "_")
		file, err := os.CreateTemp(filepath.Clean(directory), fmt.Sprintf("%s-%d-*.json", name, benchmark.N))
		if err != nil {
			benchmark.Fatal(err)
		}
		_, writeErr := file.Write(data)
		closeErr := file.Close()
		if writeErr != nil || closeErr != nil {
			benchmark.Fatalf("raw latency output: write=%v close=%v", writeErr, closeErr)
		}
		benchmark.Logf("raw latency samples: %s", file.Name())
	}
}
