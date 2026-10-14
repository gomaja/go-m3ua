package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gomaja/go-m3ua"
)

// routedPeerReadyTimeout bounds the wait for the receiver to report all eight
// associations ASP-Active in every configured AS scope.
const routedPeerReadyTimeout = 30 * time.Second

// runRoutedSender prepares the routed topology and then runs the ordinary
// warm-up and measurement cohorts over it:
//
//  1. dial the eight associations of the 2 SG x 2 SGP x 2 association
//     topology from one ASP Endpoint configured with the 1,000 exact-DPC
//     routes;
//  2. make every destination explicitly Available through SSNM DAVA from the
//     peers, verifying each report the ASP receives;
//  3. send one preflight DATA per route through MTPTransfer, check each
//     receipt against the path MTPTransfer reported, and freeze that path map
//     (every association must carry traffic);
//  4. run the cohorts, sending through MTPTransfer (routed) or through
//     Association.WriteData on the frozen path (routed-direct).
//
// Both variants share topology, scopes, traffic, queues and paths; only the
// timed send call differs.
func runRoutedSender(ctx context.Context, config commandConfig) (combinedResult, error) {
	return runRoutedSenderWith(ctx, config, routedSenderEnvironment{})
}

// routedSenderEnvironment supplies the sender's M3UA dependencies. The zero
// value uses real endpoints; tests substitute them to reach every preparation
// exit without SCTP.
type routedSenderEnvironment struct {
	factory     routingSetupFactory
	preparation func(*routingSenderSet) (routingPreparationSender, error)
}

func runRoutedSenderWith(ctx context.Context, config commandConfig, environment routedSenderEnvironment) (result combinedResult, err error) {
	client, err := newRoutingPreparationHTTPClient(config.PeerControl, routedPreparationID)
	if err != nil {
		return combinedResult{}, err
	}
	// Until the cohorts start, every failure exit tears the receiver's routed
	// topology down through /routing/stop, so a failed preparation never
	// leaves the receiver waiting for a sender that has gone. The stop
	// precedes closing the sender's own associations, so the receiver reports
	// the sender's stop rather than the resulting association failures.
	var set *routingSenderSet
	prepared := false
	defer func() {
		if !prepared {
			err = stopRoutedPeer(client, err)
		}
		if set != nil {
			_ = set.Close()
		}
	}()
	topology, err := newRoutingTopology("primary")
	if err != nil {
		return combinedResult{}, err
	}
	local, err := routedLocalAddress(config.LocalAddress)
	if err != nil {
		return combinedResult{}, fmt.Errorf("resolve routed ASP local address: %w", err)
	}
	peers, err := routedPeerAddresses(config.SCTPAddress)
	if err != nil {
		return combinedResult{}, fmt.Errorf("resolve routed SGP addresses: %w", err)
	}
	set, err = startRoutingSenderSet(ctx, topology, local, peers, environment.factory)
	if err != nil {
		return combinedResult{}, fmt.Errorf("start routed associations: %w", err)
	}
	waitContext, cancelWait := context.WithTimeout(ctx, associationAcceptTimeout)
	err = set.WaitReady(waitContext)
	cancelWait()
	if err != nil {
		return combinedResult{}, fmt.Errorf("establish routed associations: %w", err)
	}
	newPreparation := environment.preparation
	if newPreparation == nil {
		newPreparation = func(set *routingSenderSet) (routingPreparationSender, error) {
			return newRoutingM3UASenderPreparation(set)
		}
	}
	preparation, err := newPreparation(set)
	if err != nil {
		return combinedResult{}, err
	}
	if err := waitRoutedPeerInventory(ctx, client, routedPeerReadyTimeout); err != nil {
		return combinedResult{}, err
	}
	if _, err := prepareRoutingSSNM(ctx, topology, preparation, client); err != nil {
		return combinedResult{}, fmt.Errorf("routed SSNM preparation: %w", err)
	}
	remote, err := client.Inventory(ctx)
	if err != nil {
		return combinedResult{}, fmt.Errorf("routed peer inventory: %w", err)
	}
	peerInventory, err := routingPeerInventoryFromDTOs(remote.Transports)
	if err != nil {
		return combinedResult{}, err
	}
	senderInventory, err := preparation.Inventory()
	if err != nil {
		return combinedResult{}, err
	}
	pairs, err := pairRoutingInventory(topology, senderInventory, peerInventory)
	if err != nil {
		return combinedResult{}, err
	}
	plane, associations, err := routedSenderPlane(set, pairs)
	if err != nil {
		return combinedResult{}, err
	}
	dataClient, err := newRoutingDataHTTPClient(config.PeerControl, routedPreparationID)
	if err != nil {
		return combinedResult{}, err
	}
	paths, writer, err := prepareRoutingData(ctx, topology, config.Cohort+"-preflight", config.Seed, config.Outstanding, plane, dataClient)
	if err != nil {
		return combinedResult{}, fmt.Errorf("routed preflight: %w", err)
	}
	variant := routingTimedRouted
	if config.Mode == modeRoutedDirect {
		variant = routingTimedDirect
	}
	timed, err := newRoutingTimedSender(variant, plane, writer, paths, config.Workload)
	if err != nil {
		return combinedResult{}, err
	}
	if err := waitForReady(ctx, config.PeerControl, config.Associations); err != nil {
		return combinedResult{}, err
	}
	var references *routeReferenceRun
	if config.RouteReferences.enabled() {
		if references, err = startRouteReferences(ctx, config, set, associations, paths, writer); err != nil {
			return combinedResult{}, err
		}
		defer references.churner.stop()
	}
	prepared = true
	runCohort := func(cohortConfig commandConfig, phase, cohort string, duration time.Duration) (cohortResult, error) {
		// Only the measurement cohort of a failure trial declares the
		// failure; the warm-up runs the nominal routed workload.
		cohortConfig.sgpFailureCohort = cohortConfig.SGPFailure > 0 && phase == "measurement"
		var before routeReferenceSnapshot
		if references != nil {
			before = references.snapshot(ctx)
		}
		sender, receiver, err := runSenderCohortWith(ctx, cohortConfig, associations, nil, timed, cohort, duration)
		if references != nil {
			sender.RouteReferences = references.evaluate(sender.Spec, before, references.snapshot(ctx))
			if _, nominal := sender.UnsupportedModes["ssnm_or_churn_workload"]; nominal {
				delete(sender.UnsupportedModes, "ssnm_or_churn_workload")
				sender.UnsupportedModes["ssnm_workload"] = "unavailable: this cohort exercises application route-reference churn, not SSNM storms"
			}
		}
		return newCohortResult(phase, sender, receiver, err), err
	}
	return runWarmupAndMeasurement(config, runCohort, nil)
}

// startRouteReferences puts the application route table on the direct
// writer's path, starts draining every association's indications, and for
// churn starts the reference churn. Nothing it does calls a library routing
// API: the table is the application's own.
func startRouteReferences(ctx context.Context, config commandConfig, set *routingSenderSet, associations []*m3ua.Association, paths routingPathMap, writer *routingDirectWriter) (*routeReferenceRun, error) {
	clock, err := newMeasurementClock()
	if err != nil {
		return nil, fmt.Errorf("route references: %w", err)
	}
	table, err := newApplicationRouteTable(paths)
	if err != nil {
		return nil, err
	}
	endpoint, valid := set.endpoints[0].(routeReferenceStatusSource)
	if !valid || writer == nil {
		return nil, errors.New("route references need the ASP Endpoint status and the direct writer")
	}
	run := &routeReferenceRun{
		spec: *config.RouteReferences.spec(), observer: newRouteReferenceObserver(clock, associations),
		endpoint: endpoint, associations: associations, peerControl: config.PeerControl,
	}
	writer.table = table
	if config.RouteReferences.Mode != routeReferencesChurn {
		return run, nil
	}
	targets := make([]routeReferenceTarget, len(associations))
	for index, association := range associations {
		targets[index] = association
	}
	horizon := config.Warmup + config.Duration + 2*config.Drain + time.Minute
	if run.churner, err = newRouteReferenceChurner(table, targets, clock, config.RouteReferences.Rate, horizon); err != nil {
		return nil, err
	}
	return run, run.churner.start(ctx)
}

// stopRoutedPeer asks the receiver to tear its routed topology down after a
// failed preparation. It uses its own deadline, so a canceled run still
// reaches the receiver.
func stopRoutedPeer(client *routingPreparationHTTPClient, cause error) error {
	stopContext, cancelStop := context.WithTimeout(context.Background(), routingControlTimeout)
	defer cancelStop()
	return errors.Join(cause, client.Stop(stopContext))
}

// waitRoutedPeerInventory polls the receiver until its four SGP endpoints have
// accepted all eight associations and report every ASP Active in both AS
// scopes.
func waitRoutedPeerInventory(ctx context.Context, client *routingPreparationHTTPClient, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		requestContext, cancel := context.WithTimeout(ctx, routingControlTimeout)
		inventory, err := client.Inventory(requestContext)
		cancel()
		if err == nil && inventory.Ready && len(inventory.Transports) == routedAssociations && len(inventory.ASPStatuses) == 2*routedAssociations {
			return nil
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("routed peer inventory never became ready: %w", errors.Join(err, errors.New("deadline exceeded")))
		}
		timer := time.NewTimer(25 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// routedSenderPlane builds the sender DATA plane from the established set and
// returns the concrete associations in the plane's binding order, which is the
// timed sender's queue order.
func routedSenderPlane(set *routingSenderSet, pairs []routingAssociationPair) (*routingDataSenderPlane, []*m3ua.Association, error) {
	if set == nil || len(set.endpoints) != 1 {
		return nil, nil, errors.New("routed sender set is incomplete")
	}
	endpoint, valid := set.endpoints[0].(routingDataTransferEndpoint)
	if !valid {
		return nil, nil, errors.New("routed sender endpoint does not expose MTPTransfer")
	}
	entries, err := set.inventoryEntries()
	if err != nil {
		return nil, nil, err
	}
	associations := make([]routingDataAssociation, len(entries))
	concrete := make(map[m3ua.AssociationID]*m3ua.Association, len(entries))
	for index, entry := range entries {
		association, valid := entry.association.(*m3ua.Association)
		if !valid {
			return nil, nil, errors.New("routed sender association is not an M3UA association")
		}
		associations[index] = association
		concrete[association.ID()] = association
	}
	plane, err := newRoutingDataSenderPlane(endpoint, associations, pairs, set.Close)
	if err != nil {
		return nil, nil, err
	}
	ordered := make([]*m3ua.Association, len(plane.bindings))
	for index, binding := range plane.bindings {
		if ordered[index] = concrete[binding.SenderAssociation]; ordered[index] == nil {
			return nil, nil, errors.New("routed sender binding has no association")
		}
	}
	return plane, ordered, nil
}

// startRoutedSendWorkers starts one worker per sender association. Every route
// maps to exactly one worker, so one route's messages are submitted in order.
func startRoutedSendWorkers(ctx context.Context, sender *routingTimedSender, config commandConfig, counters *senderCounters) ([]chan routingTimedJob, <-chan struct{}) {
	count := len(sender.plane.bindings)
	capacities := queueCapacities(count, config.Outstanding)
	queues := make([]chan routingTimedJob, count)
	var workers sync.WaitGroup
	workers.Add(count)
	for index := range queues {
		queues[index] = make(chan routingTimedJob, capacities[index])
		go func(queue uint8, jobs <-chan routingTimedJob) {
			defer workers.Done()
			for job := range jobs {
				sender.send(ctx, queue, job, counters)
			}
		}(uint8(index), queues[index])
	}
	done := make(chan struct{})
	go func() {
		workers.Wait()
		close(done)
	}()
	return queues, done
}

// dispatchRouted offers the routed schedule on the shared open-loop scheduler.
// Message index i is route i mod 1,000, so route hits are uniform, and each
// job carries its route's frozen path and worker queue.
func dispatchRouted(ctx context.Context, config commandConfig, sender *routingTimedSender, cohort string, duration time.Duration, started time.Time, expected uint64, queues []chan routingTimedJob, counters *senderCounters, clock *sharedRunClock) {
	dispatchOpenLoop(ctx, config.Rate, duration, started, expected, clock, counters, func(index uint64, offset time.Duration, scheduled time.Time) {
		job := sender.job(cohort, config.Seed, index, scheduled, clock, offset)
		if !counters.reserve() {
			return
		}
		select {
		case queues[job.queue] <- job:
		default:
			counters.rejectReservation()
		}
	})
}
