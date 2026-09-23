package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gomaja/go-m3ua"
)

// failoverCloser is the one library call the fault injection makes on each
// association of the failed SGP.
type failoverCloser interface {
	Close() error
}

// failoverReceiver is a routed receiver's side of the SGP failure trial: the
// inventory frozen from preflight, the one-shot fault injection, and the
// active cohort's arrival evidence. Cohort fields are guarded by the receiver
// control mutex.
type failoverReceiver struct {
	ctx    context.Context
	offset time.Duration

	// Frozen at preflight completion and never changed afterwards.
	frozen          bool
	failed          map[routingTransport]failoverCloser
	alternatives    map[routingTransport]routingBinding
	alternativeKey  m3ua.ASKey
	alternativePath m3ua.MTPRoutePathID
	affected        [routingRouteCount]bool

	// injected is set, once per process, immediately before the failed SGP's
	// associations are closed. From then on their read failures are the
	// injected fault rather than fatal errors, and the alternative SGP may
	// carry the affected routes.
	injected atomic.Bool

	cohort *failoverCohort
}

// failoverCohort is the evidence of one failure-trial measurement cohort.
type failoverCohort struct {
	spec                 sgpFailureSpec
	generation           uint64
	cancel               context.CancelFunc
	fault                *failoverFault
	readerEnds           []failoverReaderEnd
	alternativeTransport [routingRouteCount]routingTransport
	perTransport         map[routingTransport]uint64
	arrival              []uint64
	surviving            []uint64
	scheduled            []uint64
	failoverReordered    uint64
	alternativeArrivals  uint64
	movedRoutes          int
}

// failoverFault is the injected fault on the shared clock: Before is read
// immediately before the first Close is issued and After once every Close has
// returned.
type failoverFault struct {
	Kind         string           `json:"kind"`
	SGP          m3ua.SGPIdentity `json:"sgp"`
	Due          int64            `json:"due_ns"`
	Before       int64            `json:"before_ns"`
	After        int64            `json:"after_ns"`
	Associations []failoverClose  `json:"associations"`
}

// failoverClose is one Association.Close of the failed SGP.
type failoverClose struct {
	Association m3ua.AssociationID `json:"association"`
	Started     int64              `json:"started_ns"`
	Returned    int64              `json:"returned_ns"`
	Error       string             `json:"error,omitempty"`
}

// failoverReaderEnd is the end of a failed SGP association's read loop after
// the injected fault.
type failoverReaderEnd struct {
	SGP         m3ua.SGPIdentity   `json:"sgp"`
	Association m3ua.AssociationID `json:"association"`
	At          int64              `json:"at_ns"`
	Error       string             `json:"error"`
}

// failoverTransportCount is one peer transport's unique deliveries.
type failoverTransportCount struct {
	SGP         m3ua.SGPIdentity   `json:"sgp"`
	Association m3ua.AssociationID `json:"association"`
	Failed      bool               `json:"failed"`
	Alternative bool               `json:"alternative"`
	Unique      uint64             `json:"unique"`
}

// failoverReceiverRecord is the receiver's evidence. ArrivalBins and
// SurvivingBins count unique deliveries by shared-clock arrival time in
// Bin-wide bins from the window start through the drain, SurvivingBins only
// those on the surviving SGPs. ScheduledBins count unique deliveries by the
// scheduled offset of each message, so a bin whose count equals its offered
// count delivered everything scheduled in it.
type failoverReceiverRecord struct {
	Fault               *failoverFault           `json:"fault,omitempty"`
	ReaderEnds          []failoverReaderEnd      `json:"reader_ends,omitempty"`
	AffectedRoutes      int                      `json:"affected_routes"`
	MovedRoutes         int                      `json:"moved_routes"`
	AlternativeArrivals uint64                   `json:"alternative_arrivals"`
	FailoverReordered   uint64                   `json:"failover_reordered"`
	PerTransport        []failoverTransportCount `json:"per_transport"`
	Bin                 time.Duration            `json:"bin_ns"`
	ArrivalBins         []uint64                 `json:"arrival_bins"`
	SurvivingBins       []uint64                 `json:"surviving_bins"`
	ScheduledBins       []uint64                 `json:"scheduled_bins"`
}

func (control *receiverControl) enableFailover(ctx context.Context, offset time.Duration) {
	control.mutex.Lock()
	defer control.mutex.Unlock()
	if control.routed != nil && offset > 0 {
		control.routed.failover = &failoverReceiver{ctx: ctx, offset: offset}
	}
}

// freezeFailover records the trial inventory from the frozen preflight: the
// routes whose path is on the failed SGP, the failed SGP's associations and
// the alternative SGP's bindings, AS scope and path.
func (control *receiverControl) freezeFailover(topology routingTopology, pairs []routingAssociationPair, associations []routingDataAssociation) error {
	control.mutex.Lock()
	defer control.mutex.Unlock()
	if control.routed == nil || control.routed.failover == nil {
		return nil
	}
	return control.routed.failover.freeze(topology, pairs, associations, &control.routed.paths)
}

func (failover *failoverReceiver) freeze(topology routingTopology, pairs []routingAssociationPair, associations []routingDataAssociation, paths *routingPathMap) error {
	if failover.frozen || len(pairs) != routedAssociations || len(associations) != len(pairs) || len(topology.Peers) != routedPeerCount || topology.ASP == nil || topology.ASP.Routing == nil {
		return errors.New("SGP failure inventory is incomplete or already frozen")
	}
	alternativeIndex := -1
	for index, peer := range topology.Peers {
		if peer.Identity == sgpFailureAlternative {
			alternativeIndex = index
		}
	}
	if alternativeIndex < 0 || len(topology.Peers[alternativeIndex].ApplicationServers) != 2 {
		return errors.New("SGP failure alternative is not in the topology")
	}
	failover.failed = make(map[routingTransport]failoverCloser, 2)
	failover.alternatives = make(map[routingTransport]routingBinding, 2)
	for index, pair := range pairs {
		switch pair.Binding.Peer.SGP {
		case sgpFailureFailed:
			closer, valid := associations[index].(failoverCloser)
			if !valid || associations[index].ID() != pair.Binding.Peer.Association {
				return errors.New("failed SGP association cannot be closed")
			}
			failover.failed[pair.Binding.Peer] = closer
		case sgpFailureAlternative:
			failover.alternatives[pair.Binding.Peer] = pair.Binding
		}
	}
	if len(failover.failed) != 2 || len(failover.alternatives) != 2 {
		return errors.New("SGP failure needs two associations at the failed and at the alternative SGP")
	}
	failover.alternativeKey = topology.Peers[alternativeIndex].ApplicationServers[0].ASKey
	failover.alternativePath = topology.ASP.Routing.Paths[alternativeIndex/2].ID
	affected := 0
	for route := range routingRouteCount {
		path, err := paths.path(uint16(route))
		if err != nil {
			return err
		}
		if path.Binding.Peer.SGP == sgpFailureFailed {
			failover.affected[route] = true
			affected++
		}
	}
	if affected == 0 {
		return errors.New("no route is frozen on the failed SGP")
	}
	failover.frozen = true
	return nil
}

// acceptFailoverSpecLocked checks a cohort's failure declaration without
// committing anything.
func (control *receiverControl) acceptFailoverSpecLocked(specification runSpec) error {
	var failover *failoverReceiver
	if control.routed != nil {
		failover = control.routed.failover
	}
	if failover == nil {
		if specification.SGPFailure != nil {
			return errors.New("this receiver was not started with -sgp-failure and injects no failure")
		}
		return nil
	}
	if err := validateSGPFailureSpec(specification, failover.offset); err != nil {
		return err
	}
	if specification.SGPFailure != nil && (!failover.frozen || failover.injected.Load()) {
		return errors.New("the SGP failure inventory is not frozen or the failure was already injected")
	}
	return nil
}

// resetFailoverLocked replaces the cohort evidence for an accepted cohort.
func (control *receiverControl) resetFailoverLocked(specification runSpec) {
	if control.routed == nil || control.routed.failover == nil {
		return
	}
	failover := control.routed.failover
	if failover.cohort != nil && failover.cohort.cancel != nil {
		failover.cohort.cancel()
	}
	failover.cohort = nil
	if specification.SGPFailure == nil {
		return
	}
	window := specification.Duration + specification.Drain
	failover.cohort = &failoverCohort{
		spec: *specification.SGPFailure, generation: control.generation,
		perTransport: make(map[routingTransport]uint64, routedAssociations),
		arrival:      make([]uint64, binCount(window, sgpFailureBin)),
		surviving:    make([]uint64, binCount(window, sgpFailureBin)),
		scheduled:    make([]uint64, binCount(specification.Duration, sgpFailureBin)),
	}
}

func binCount(window, bin time.Duration) int {
	return int((window + bin - 1) / bin)
}

// startFailoverLocked schedules the injection of an armed cohort.
func (control *receiverControl) startFailoverLocked() {
	if control.routed == nil || control.routed.failover == nil || control.routed.failover.cohort == nil || control.spec.SGPFailure == nil {
		return
	}
	failover := control.routed.failover
	lifetime, cancel := context.WithCancel(failover.ctx)
	failover.cohort.cancel = cancel
	go failover.inject(lifetime, control, control.generation, sgpFailureInstant(control.spec))
}

// stopFailoverLocked cancels an injection that has not fired.
func (control *receiverControl) stopFailoverLocked() {
	if control.routed != nil && control.routed.failover != nil && control.routed.failover.cohort != nil && control.routed.failover.cohort.cancel != nil {
		control.routed.failover.cohort.cancel()
	}
}

// inject waits for the declared shared-clock instant and ends every
// association of the failed SGP with Association.Close, concurrently, timing
// each call on the shared clock.
func (failover *failoverReceiver) inject(ctx context.Context, control *receiverControl, generation uint64, due int64) {
	clock := control.clock
	if err := waitSharedInstant(ctx, clock, due); err != nil {
		if ctx.Err() == nil {
			control.setFatal("SGP failure injection clock failed: " + err.Error())
		}
		return
	}
	failover.injected.Store(true)
	transports := make([]routingTransport, 0, len(failover.failed))
	for transport := range failover.failed {
		transports = append(transports, transport)
	}
	sort.Slice(transports, func(first, second int) bool { return transports[first].Association < transports[second].Association })
	closes := make([]failoverClose, len(transports))
	before, beforeErr := clock.Now()
	var closing sync.WaitGroup
	for index, transport := range transports {
		closing.Add(1)
		go func(index int, transport routingTransport) {
			defer closing.Done()
			started, _ := clock.Now()
			err := failover.failed[transport].Close()
			returned, _ := clock.Now()
			closes[index] = failoverClose{Association: transport.Association, Started: started, Returned: returned}
			if err != nil {
				closes[index].Error = err.Error()
			}
		}(index, transport)
	}
	closing.Wait()
	after, afterErr := clock.Now()
	control.mutex.Lock()
	defer control.mutex.Unlock()
	if beforeErr != nil || afterErr != nil {
		control.fatal = "SGP failure injection clock failed"
		return
	}
	if failover.cohort == nil || failover.cohort.generation != generation || control.generation != generation {
		control.fatal = "SGP failure was injected after its cohort ended"
		return
	}
	failover.cohort.fault = &failoverFault{Kind: failover.cohort.spec.Kind, SGP: sgpFailureFailed, Due: due, Before: before, After: after, Associations: closes}
}

// sgpFailureWaitSlice bounds one sleep of the injection wait, so the shared
// clock is re-read at least this often.
const sgpFailureWaitSlice = 50 * time.Millisecond

// waitSharedInstant sleeps until the shared clock reaches target. Go timers
// are only wake-up hints; the shared clock decides.
func waitSharedInstant(ctx context.Context, clock measurementClock, target int64) error {
	if clock == nil {
		return errors.New("shared clock is unavailable")
	}
	for {
		now, err := clock.Now()
		if err != nil || now <= 0 {
			return errors.New("shared clock read failed")
		}
		if now >= target {
			return nil
		}
		timer := time.NewTimer(min(time.Duration(target-now), sgpFailureWaitSlice))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// failoverReaderEnded reports whether a read failure is the injected fault,
// and records it on the cohort when it is.
func (control *receiverControl) failoverReaderEnded(transport routingTransport, err error) bool {
	control.mutex.Lock()
	defer control.mutex.Unlock()
	if control.routed == nil || control.routed.failover == nil {
		return false
	}
	failover := control.routed.failover
	if _, failed := failover.failed[transport]; !failed || !failover.injected.Load() {
		return false
	}
	if failover.cohort != nil {
		now, _ := control.clock.Now()
		failover.cohort.readerEnds = append(failover.cohort.readerEnds, failoverReaderEnd{
			SGP: transport.SGP, Association: transport.Association, At: now, Error: fmt.Sprint(err),
		})
	}
	return true
}

// validate classifies one arrival of a failure cohort. An affected route may
// arrive on the alternative SGP, in its preferred AS scope and on its own
// binding, but only once the fault has been injected; every other arrival is
// validated against the frozen path exactly as in the routed mode.
func (failover *failoverReceiver) validate(message *m3ua.DataMessage, transport routingTransport, specification runSpec, paths *routingPathMap) (routingIdentity, bool, error) {
	binding, alternative := failover.alternatives[transport]
	if message == nil || message.ProtocolData == nil {
		return routingIdentity{}, false, errors.New("routing DATA is missing")
	}
	identity, err := parseRoutePayload(message.ProtocolData.Data)
	if err != nil {
		return routingIdentity{}, false, err
	}
	if !alternative || !failover.affected[identity.Route] {
		identity, err = validateRouteMessage(message, transport, specification.Cohort, specification.Seed, specification.Payload, paths)
		return identity, false, err
	}
	if !failover.injected.Load() {
		return routingIdentity{}, false, errors.New("an affected route arrived on the alternative SGP before the fault")
	}
	path := routingResolvedPath{
		Target: m3ua.MTPTransferPath{
			Path: failover.alternativePath, SGP: sgpFailureAlternative, ApplicationServer: "primary",
			AS: failover.alternativeKey, Association: binding.SenderAssociation,
		},
		Binding: binding,
	}
	identity, err = validateRoutingArrival(message, transport, specification.Cohort, specification.Seed, specification.Payload, path)
	return identity, true, err
}

// claimAlternativeLocked keeps each moved route on the one alternative
// association it first arrived on: a loadshared flow is assigned once.
func (failover *failoverReceiver) claimAlternativeLocked(route uint16, transport routingTransport) bool {
	cohort := failover.cohort
	if cohort == nil || route >= routingRouteCount {
		return false
	}
	switch cohort.alternativeTransport[route] {
	case routingTransport{}:
		cohort.alternativeTransport[route] = transport
		cohort.movedRoutes++
		return true
	case transport:
		return true
	default:
		return false
	}
}

// uniqueLocked moves a reordering of an affected route out of the nominal
// reorder count: at the failover the failed SGP may still hand over messages
// it read before the fault after the alternative has delivered later ones.
// Healthy routes keep the nominal zero-reorder rule.
func (failover *failoverReceiver) uniqueLocked(ledger *routingLedger, identity routingIdentity, reorderedBefore uint64, alternative bool) {
	cohort := failover.cohort
	if cohort == nil {
		return
	}
	if ledger.snapshotData.Reordered > reorderedBefore && failover.affected[identity.Route] {
		ledger.snapshotData.Reordered--
		cohort.failoverReordered++
	}
	if alternative {
		cohort.alternativeArrivals++
	}
}

// deliveredLocked bins one committed unique delivery.
func (failover *failoverReceiver) deliveredLocked(specification runSpec, received int64, transport routingTransport, identity routingIdentity) {
	cohort := failover.cohort
	if cohort == nil || specification.Clock == nil {
		return
	}
	cohort.perTransport[transport]++
	if offset := received - specification.Clock.Start; offset >= 0 {
		if bin := offset / int64(sgpFailureBin); bin < int64(len(cohort.arrival)) {
			cohort.arrival[bin]++
			if transport.SGP != sgpFailureFailed {
				cohort.surviving[bin]++
			}
		}
	}
	if index, err := routingGlobalIndex(identity); err == nil && specification.Rate > 0 {
		scheduled := time.Duration(index * uint64(time.Second) / specification.Rate)
		if bin := int(scheduled / sgpFailureBin); bin < len(cohort.scheduled) {
			cohort.scheduled[bin]++
		}
	}
}

// failoverRecordLocked is the receiver record's failure evidence.
func (control *receiverControl) failoverRecordLocked() *failoverRecord {
	if control.routed == nil || control.routed.failover == nil || control.routed.failover.cohort == nil || control.spec.SGPFailure == nil {
		return nil
	}
	failover := control.routed.failover
	cohort := failover.cohort
	receiver := &failoverReceiverRecord{
		ReaderEnds: append([]failoverReaderEnd(nil), cohort.readerEnds...), MovedRoutes: cohort.movedRoutes,
		AlternativeArrivals: cohort.alternativeArrivals, FailoverReordered: cohort.failoverReordered, Bin: sgpFailureBin,
		ArrivalBins: append([]uint64(nil), cohort.arrival...), SurvivingBins: append([]uint64(nil), cohort.surviving...),
		ScheduledBins: append([]uint64(nil), cohort.scheduled...),
	}
	if cohort.fault != nil {
		fault := *cohort.fault
		fault.Associations = append([]failoverClose(nil), cohort.fault.Associations...)
		receiver.Fault = &fault
	}
	for _, affected := range failover.affected {
		if affected {
			receiver.AffectedRoutes++
		}
	}
	for _, path := range control.routed.paths.paths {
		transport := path.Binding.Peer
		if transport == (routingTransport{}) {
			continue
		}
		if _, listed := cohort.perTransport[transport]; !listed {
			cohort.perTransport[transport] = 0
		}
	}
	for transport, unique := range cohort.perTransport {
		_, failed := failover.failed[transport]
		_, alternative := failover.alternatives[transport]
		receiver.PerTransport = append(receiver.PerTransport, failoverTransportCount{
			SGP: transport.SGP, Association: transport.Association, Failed: failed, Alternative: alternative, Unique: unique,
		})
	}
	sort.Slice(receiver.PerTransport, func(first, second int) bool {
		a, b := receiver.PerTransport[first], receiver.PerTransport[second]
		if a.SGP != b.SGP {
			return a.SGP.SignallingGateway < b.SGP.SignallingGateway ||
				a.SGP.SignallingGateway == b.SGP.SignallingGateway && a.SGP.SignallingGatewayProcess < b.SGP.SignallingGatewayProcess
		}
		return a.Association < b.Association
	})
	return &failoverRecord{Spec: cohort.spec, Receiver: receiver}
}
