package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gomaja/go-m3ua"
	"github.com/gomaja/go-sctp"
)

// processedRoutingAssociation acknowledges the previous arrival on entry to
// ReadData. The production reader has then returned from recordRouted, including
// validation and ledger updates. The fence counts processing, not correctness:
// invalid and missing deliveries still fail the cohort's independent assertions.
type processedRoutingAssociation struct {
	routingDataAssociation
	processed atomic.Uint64
	pending   bool
}

func (association *processedRoutingAssociation) ReadData(ctx context.Context) (*m3ua.DataMessage, error) {
	if association.pending {
		association.processed.Add(1)
		association.pending = false
	}
	message, err := association.routingDataAssociation.ReadData(ctx)
	association.pending = err == nil
	return message, err
}

// Automatic transport sampling has an independent real-time worker pool.
// Missed samples under host starvation are outside the cohort's logical clock.
// This fixture disables that producer through the supported configuration; the
// command's real-clock acceptance path retains its default monitor. Every
// indication delivered by an enabled producer still fails the cohort.
func newCohortRoutingEndpoint(config m3ua.EndpointConfig) (routingSetupEndpoint, error) {
	config.TransportCongestion = &m3ua.TransportCongestionConfig{Disabled: true}
	endpoint, err := m3ua.NewEndpoint(config)
	if err != nil {
		return nil, err
	}
	concrete := routingM3UAEndpoint{endpoint}
	if config.Role == m3ua.RoleASP {
		return cohortRoutingEndpoint{concrete}, nil
	}
	return concrete, nil
}

// ASP Active has no transaction identifier. Keep retries beyond the overall
// fixture deadline so an older matching Ack cannot satisfy a later fence.
const cohortTAck = 2 * time.Minute

type cohortRoutingEndpoint struct{ routingM3UAEndpoint }

func (endpoint cohortRoutingEndpoint) Dial(ctx context.Context, local, remote *sctp.Addr, config *m3ua.AssociationConfig) (routingSetupAssociation, error) {
	copied := *config
	copied.TAck = cohortTAck
	return endpoint.routingM3UAEndpoint.Dial(ctx, local, remote, &copied)
}

type cohortConnector struct{ m3uaConnector }

func (connector cohortConnector) Dial(ctx context.Context, network string, local, remote *sctp.Addr, config *m3ua.AssociationConfig) (*m3ua.Association, error) {
	copied := *config
	copied.TAck = cohortTAck
	return connector.m3uaConnector.Dial(ctx, network, local, remote, &copied)
}

type routeReferenceLiveFixture struct {
	ctx        context.Context
	clock      *cohortTestClock
	control    *receiverControl
	timed      *routingTimedSender
	references *routeReferenceRun
	processed  [routedAssociations]*processedRoutingAssociation
	fatal      <-chan error
	peers      *routingPeerSet
}

func newRouteReferenceLiveFixture(testContext *testing.T, mode string) *routeReferenceLiveFixture {
	testContext.Helper()
	base := routedLoopbackPortBase(testContext)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	testContext.Cleanup(cancel)
	clock := newCohortTestClock()
	topology, err := newRoutingTopology("primary")
	if err != nil {
		testContext.Fatal(err)
	}
	addresses, err := routedPeerAddresses(fmt.Sprintf("127.0.0.1:%d", base))
	if err != nil {
		testContext.Fatal(err)
	}
	peers, err := startRoutingPeerSet(ctx, topology, addresses, newCohortRoutingEndpoint)
	if err != nil {
		testContext.Fatal(err)
	}
	testContext.Cleanup(func() { _ = peers.Close() })
	control := newReceiverControl(routedAssociations, maxOutstanding)
	control.clock = clock
	testContext.Cleanup(func() {
		control.mutex.Lock()
		defer control.mutex.Unlock()
		if control.memory != nil {
			control.memory.stop()
			control.memory = nil
		}
	})
	control.enableRouted(modeRoutedDirect)
	source, err := newRoutingM3UAPeerPreparationSource(topology, peers)
	if err != nil {
		testContext.Fatal(err)
	}
	preparation, err := newRoutingPeerPreparation(topology, source)
	if err != nil {
		testContext.Fatal(err)
	}
	fatal := make(chan error, 1)
	operations := &routedPeerOperations{ctx: ctx, topology: topology, peers: peers, preparation: preparation, control: control, fatal: fatal}
	testContext.Cleanup(func() { _ = operations.shutdown() })
	local, err := routedLocalAddress("127.0.0.1:0")
	if err != nil {
		testContext.Fatal(err)
	}
	senders, err := startRoutingSenderSet(ctx, topology, local, addresses, newCohortRoutingEndpoint)
	if err != nil {
		testContext.Fatal(err)
	}
	testContext.Cleanup(func() { _ = senders.Close() })
	if err := senders.WaitReady(ctx); err != nil {
		testContext.Fatal(err)
	}
	senderPreparation, err := newRoutingM3UASenderPreparation(senders)
	if err != nil {
		testContext.Fatal(err)
	}
	peerControl := cohortRoutingControl{operations: operations}
	var latestInventoryError error
	for {
		inventory, err := peerControl.Inventory(ctx)
		if err == nil && inventory.Ready && len(inventory.Transports) == routedAssociations && len(inventory.ASPStatuses) == 2*routedAssociations {
			break
		}
		if err != nil {
			latestInventoryError = err
		}
		select {
		case <-ctx.Done():
			testContext.Fatalf("peer readiness: %v (latest inventory error: %v)", ctx.Err(), latestInventoryError)
		case <-peers.Done():
			testContext.Fatalf("peers ended before readiness: %v", latestInventoryError)
		case <-time.After(time.Millisecond):
		}
	}
	if _, err := prepareCohortRoutingSSNM(ctx, topology, senderPreparation, peerControl); err != nil {
		testContext.Fatalf("SSNM preparation: %v", err)
	}
	// prepare has built the preflight plane from the original associations.
	// Replace only the slice completePreflight will use for its timed readers.
	bySender := make(map[m3ua.AssociationID]*processedRoutingAssociation)
	operations.mutex.Lock()
	for index, association := range operations.associations {
		wrapped := &processedRoutingAssociation{routingDataAssociation: association}
		operations.associations[index] = wrapped
		bySender[operations.pairs[index].Binding.SenderAssociation] = wrapped
	}
	operations.mutex.Unlock()
	remote, err := peerControl.Inventory(ctx)
	if err != nil {
		testContext.Fatal(err)
	}
	peerInventory, err := routingPeerInventoryFromDTOs(remote.Transports)
	if err != nil {
		testContext.Fatal(err)
	}
	senderInventory, err := senderPreparation.Inventory()
	if err != nil {
		testContext.Fatal(err)
	}
	pairs, err := pairRoutingInventory(topology, senderInventory, peerInventory)
	if err != nil {
		testContext.Fatal(err)
	}
	plane, associations, err := routedSenderPlane(senders, pairs)
	if err != nil {
		testContext.Fatal(err)
	}
	paths, writer, err := prepareRoutingData(ctx, topology, "references-preflight", 11, maxOutstanding, plane, peerControl)
	if err != nil {
		testContext.Fatal(err)
	}
	timed, err := newRoutingTimedSender(routingTimedDirect, plane, writer, paths, workloadMix)
	if err != nil {
		testContext.Fatal(err)
	}
	if ready := control.ready(); !ready.Ready {
		testContext.Fatalf("receiver readiness: %+v", ready)
	}
	table, err := newApplicationRouteTable(paths)
	if err != nil {
		testContext.Fatal(err)
	}
	writer.table = table
	clock.observe(associations)
	run := &routeReferenceRun{spec: *routeReferenceConfig{Mode: mode, Rate: 1000}.spec(), observer: clock.observer, endpoint: senders.endpoints[0].(routeReferenceStatusSource), associations: associations}
	if mode == routeReferencesChurn {
		targets := make([]routeReferenceTarget, len(associations))
		for index, association := range associations {
			targets[index] = association
		}
		run.churner, err = newRouteReferenceChurner(table, targets, clock, 1000, time.Minute)
		if err != nil {
			testContext.Fatal(err)
		}
		if err := run.churner.start(ctx); err != nil {
			testContext.Fatal(err)
		}
		testContext.Cleanup(run.churner.stop)
	}
	fixture := &routeReferenceLiveFixture{ctx: ctx, clock: clock, control: control, timed: timed, references: run, fatal: fatal, peers: peers}
	for index, binding := range plane.bindings {
		fixture.processed[index] = bySender[binding.SenderAssociation]
		if fixture.processed[index] == nil {
			testContext.Fatal("missing receiver processing fence")
		}
	}
	return fixture
}

func (fixture *routeReferenceLiveFixture) snapshot() routeReferenceSnapshot {
	associations := make([]routeReferenceStateAssociation, len(fixture.references.associations))
	for index, association := range fixture.references.associations {
		associations[index] = association
	}
	snapshot := routeReferenceSnapshot{Sender: captureRouteReferenceState([]routeReferenceStatusSource{fixture.references.endpoint}, associations)}
	receiver, err := capturePeerRouteReferenceState(fixture.peers)
	if err != nil {
		snapshot.Error = err.Error()
	}
	snapshot.Receiver = receiver
	return snapshot
}

func (fixture *routeReferenceLiveFixture) await(testContext *testing.T, wanted [routedAssociations]uint64) {
	testContext.Helper()
	now, _ := fixture.clock.Now()
	waitCohortWork(testContext, fixture.ctx, fixture.clock, "route operations and receiver processing", func() bool {
		if churner := fixture.references.churner; churner != nil {
			due := uint64(now-churner.anchor)*churner.rate/uint64(time.Second) + 1
			churner.mutex.Lock()
			completed := uint64(len(churner.completed))
			churner.mutex.Unlock()
			if completed < due {
				return false
			}
		}
		for index, association := range fixture.processed {
			if association.processed.Load() < wanted[index] {
				return false
			}
		}
		return true
	})
}

// TestRouteReferencesLiveOverLoopback retains real SCTP, application-table
// lookups/writes and the production churn/evaluators. The clock advances only
// after due operations and DATA processing complete. Scheduling delays consume
// the overall deadline, rather than invalidating logical workload intensity.
func TestRouteReferencesLiveOverLoopback(testContext *testing.T) {
	if testing.Short() {
		testContext.Skip("the route-reference workload needs live SCTP cohorts")
	}
	for _, mode := range []string{routeReferencesChurn, routeReferencesStatic} {
		testContext.Run(mode, func(testContext *testing.T) {
			fixture := newRouteReferenceLiveFixture(testContext, mode)
			var wanted [routedAssociations]uint64
			fixture.await(testContext, wanted)
			for _, phase := range []struct {
				name     string
				duration time.Duration
			}{{"warmup", 2 * time.Second}, {"measurement", 4 * time.Second}} {
				before := fixture.snapshot()
				specification := routedSpec(modeRoutedDirect)
				specification.Cohort = "route-references-" + mode + "-" + phase.name
				specification.Seed = 11
				specification.Rate = 2000
				specification.Duration = phase.duration
				specification.Expected = uint64(phase.duration/time.Second) * specification.Rate
				specification.RouteReferences = &fixture.references.spec
				now, _ := fixture.clock.Now()
				domain, _ := fixture.clock.Domain()
				start := now + int64(time.Millisecond)
				specification.Clock = &sharedClockWindow{Domain: domain, Start: start, End: start + int64(phase.duration)}
				if err := fixture.control.reset(specification); err != nil {
					testContext.Fatal(err)
				}
				if err := fixture.control.start(); err != nil {
					testContext.Fatal(err)
				}
				counters := newSenderCounters(maxOutstanding)
				runClock := &sharedRunClock{source: fixture.clock, window: *specification.Clock, drain: specification.Drain}
				var index uint64
				if err := fixture.clock.boundary(fixture.ctx, start); err != nil {
					testContext.Fatal(err)
				}
				for elapsed := 10 * time.Millisecond; elapsed <= phase.duration+specification.Drain; elapsed += 10 * time.Millisecond {
					instant := start + int64(elapsed) - 1
					fixture.clock.advance(instant)
					for index < specification.Expected && int64(index*uint64(time.Second)/specification.Rate) < instant-start {
						offset := time.Duration(index * uint64(time.Second) / specification.Rate)
						job := fixture.timed.job(specification.Cohort, specification.Seed, index, time.Time{}, runClock, offset)
						counters.schedule()
						submitted := counters.submittedCount()
						if counters.reserve() {
							fixture.timed.send(fixture.ctx, job.queue, job, counters)
						}
						if counters.submittedCount() > submitted {
							wanted[job.queue]++
						}
						index++
					}
					fixture.await(testContext, wanted)
				}
				closing := fixture.snapshot()
				// Join inbound indication production before sealing the inclusive
				// activity window, even when no DATA remains to be processed.
				if err := fixture.clock.boundary(fixture.ctx, specification.Clock.End+int64(specification.Drain)+1); err != nil {
					testContext.Fatal(err)
				}
				fixture.await(testContext, wanted)
				if err := fixture.control.stop(); err != nil {
					testContext.Error(err)
				}
				receiver := fixture.control.result()
				sender := counters.result(specification, phase.duration, specification.Drain, 0)
				if sender.Spec.RouteReferences == nil || receiver.Spec.RouteReferences == nil || !sameRunSpec(sender.Spec, receiver.Spec) {
					testContext.Errorf("%s workload declaration differs: sender %+v; receiver %+v", phase.name, sender.Spec, receiver.Spec)
				}
				sender.Delivery = receiver.Delivery
				sender.evaluate()
				if sender.FixtureVerdict != verdictPass || receiver.FixtureVerdict != verdictPass {
					testContext.Errorf("%s fixture %s/%s: sender %v; receiver %v", phase.name, sender.FixtureVerdict, receiver.FixtureVerdict, sender.Reasons, receiver.Reasons)
				}
				if sender.Scheduled != specification.Expected || sender.Submitted != specification.Expected || sender.SendErrors != 0 || sender.Capped != 0 || sender.FatalError != "" {
					testContext.Errorf("%s sender: scheduled %d submitted %d errors %d capped %d fatal %q", phase.name, sender.Scheduled, sender.Submitted, sender.SendErrors, sender.Capped, sender.FatalError)
				}
				if receiver.FatalError != "" || receiver.Delivery.Unique != specification.Expected || receiver.Delivery.Missing != 0 || receiver.Delivery.Invalid != 0 || receiver.Delivery.Duplicate != 0 || receiver.Delivery.Reordered != 0 {
					testContext.Errorf("%s delivery %+v fatal %q", phase.name, receiver.Delivery, receiver.FatalError)
				}
				// Check both sides of the mutating protocol fence. In particular, a
				// partial withdrawal must fail even if ASP Active restores it.
				for _, after := range []routeReferenceSnapshot{closing, fixture.snapshot()} {
					record := fixture.references.evaluate(specification, before, after)
					if (mode == routeReferencesChurn) != (record.Churn != nil) {
						testContext.Errorf("%s churn evidence present = %v", phase.name, record.Churn != nil)
					}
					expectedCriteria := 3
					if mode == routeReferencesChurn {
						expectedCriteria = 5
					}
					if len(record.Criteria) != expectedCriteria {
						testContext.Errorf("%s criteria: %+v", phase.name, record.Criteria)
					}
					for _, criterion := range record.Criteria {
						testContext.Logf("%s %s: %s — %s", phase.name, criterion.Name, criterion.Outcome, criterion.Detail)
						if criterion.Outcome != failoverPass {
							testContext.Errorf("%s criterion %s = %s: %s", phase.name, criterion.Name, criterion.Outcome, criterion.Detail)
							for _, event := range record.Events {
								testContext.Logf("association %d at %d: %s: %s", event.Association, event.At, event.Kind, event.Detail)
							}
						}
					}
					if record.Verdict != failoverPass {
						testContext.Errorf("%s verdict %s", phase.name, record.Verdict)
					}
				}
				select {
				case err := <-fixture.fatal:
					testContext.Error(err)
				default:
				}
			}
		})
	}
}
