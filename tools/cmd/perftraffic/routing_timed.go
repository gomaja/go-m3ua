package main

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/gomaja/go-m3ua"
)

type routingDirectWriteOutcome struct {
	requested          int
	written            int
	writeErr           error
	validationErr      error
	expectedEpoch      uint64
	expectedMaxStream  uint16
	afterEpoch         uint64
	afterMaxStream     uint16
	afterMaxStreamRead bool
}

type routingTimedVariant uint8

const (
	routingTimedRouted routingTimedVariant = iota + 1
	routingTimedDirect
)

type routingTimedJob struct {
	identity  routingIdentity
	routeID   m3ua.MTPRouteID
	scheduled time.Time
	clock     *sharedRunClock
	offset    time.Duration
	size      int
	path      routingResolvedPath
	queue     uint8
}

type routingTimedSender struct {
	variant  routingTimedVariant
	plane    *routingDataSenderPlane
	direct   *routingDirectWriter
	paths    routingPathMap
	routeIDs [routingRouteCount]m3ua.MTPRouteID
	queues   [routingRouteCount]uint8
	workload workload
	now      func() time.Time
	// failover classifies every call of an SGP failure cohort instead of
	// requiring the frozen path; nil for every other cohort.
	failover *failoverTracker
}

type routingTimedRoutedOutcome struct {
	result  m3ua.MTPTransferResult
	err     error
	started time.Time
	ended   time.Time
}

func newRoutingTimedSender(variant routingTimedVariant, plane *routingDataSenderPlane, direct *routingDirectWriter, paths routingPathMap, selectedWorkload workload) (*routingTimedSender, error) {
	if (variant != routingTimedRouted && variant != routingTimedDirect) || selectedWorkload != workloadMix || plane == nil || plane.endpoint == nil ||
		len(plane.associations) != 8 || len(plane.epochs) != 8 || len(plane.bindings) != 8 || !paths.ready {
		return nil, errors.New("routing timed sender inventory or workload is invalid")
	}
	sender := &routingTimedSender{variant: variant, plane: plane, direct: direct, paths: paths, workload: selectedWorkload, now: time.Now}
	queues := make(map[m3ua.AssociationID]uint8, len(plane.bindings))
	for index, binding := range plane.bindings {
		association := plane.associations[binding.SenderAssociation]
		epoch := plane.epochs[binding.SenderAssociation]
		if binding.SenderAssociation == 0 || binding.MaxMessageStreamID == 0 || association == nil || association.ID() != binding.SenderAssociation ||
			epoch == 0 || association.Epoch() != epoch || association.MaxMessageStreamID() != binding.MaxMessageStreamID {
			return nil, errors.New("routing timed sender association inventory is invalid")
		}
		if _, exists := queues[binding.SenderAssociation]; exists {
			return nil, errors.New("routing timed sender association inventory is duplicated")
		}
		queues[binding.SenderAssociation] = uint8(index)
	}
	if len(queues) != 8 {
		return nil, errors.New("routing timed sender association inventory is incomplete")
	}
	var coverage [8]bool
	for route := range routingRouteCount {
		path, err := paths.path(uint16(route))
		if err != nil {
			return nil, err
		}
		queue, known := queues[path.Target.Association]
		if !known || path.Binding != plane.bindings[queue] {
			return nil, errors.New("routing timed path differs from the sender inventory")
		}
		sender.routeIDs[route] = m3ua.MTPRouteID(fmt.Sprintf("route-%04d", route))
		sender.queues[route] = queue
		coverage[queue] = true
	}
	if coverage != [8]bool{true, true, true, true, true, true, true, true} {
		return nil, errors.New("routing timed paths do not cover every sender association")
	}
	if variant == routingTimedDirect {
		if direct == nil || direct.admission == nil || direct.paths != paths || len(direct.associations) != len(plane.associations) || len(direct.epochs) != len(plane.epochs) {
			return nil, errors.New("routing timed direct writer inventory is invalid")
		}
		for associationID, association := range plane.associations {
			directAssociation := direct.associations[associationID]
			associationType := reflect.TypeOf(association)
			if directAssociation == nil || associationType == nil || !associationType.Comparable() || directAssociation != association ||
				direct.epochs[associationID] != plane.epochs[associationID] {
				return nil, errors.New("routing timed direct writer differs from the sender inventory")
			}
		}
	}
	return sender, nil
}

func (sender *routingTimedSender) job(cohort string, seed, index uint64, scheduled time.Time, clock *sharedRunClock, offset time.Duration) routingTimedJob {
	identity := planRouteMessage(cohort, seed, index)
	path, _ := sender.paths.path(identity.Route)
	return routingTimedJob{
		identity: identity, routeID: sender.routeIDs[identity.Route], scheduled: scheduled, clock: clock,
		offset: offset, size: sender.workload.size(index), path: path, queue: sender.queues[identity.Route],
	}
}

func (job routingTimedJob) dispatchDelay() (time.Duration, error) {
	if job.clock == nil {
		return time.Since(job.scheduled), nil
	}
	elapsed, err := job.clock.elapsed()
	if err != nil || elapsed < job.offset {
		return 0, errors.New("shared dispatch clock failed or precedes schedule")
	}
	return elapsed - job.offset, nil
}

func (sender *routingTimedSender) routedSubmission(job routingTimedJob, payload []byte) routingTimedRoutedOutcome {
	outcome := routingTimedRoutedOutcome{started: sender.now()}
	protocolData, err := routeProtocolData(job.identity.Route, payload)
	if err != nil {
		outcome.err = err
		outcome.ended = sender.now()
		return outcome
	}
	request := m3ua.MTPTransferRequest{MTPRoute: job.routeID, ProtocolData: &protocolData}
	outcome.result, outcome.err = sender.plane.endpoint.MTPTransfer(request)
	outcome.ended = sender.now()
	return outcome
}

// partialTransferError is an MTP-TRANSFER that succeeded on some path and
// failed on another. It hides the failures' causes, so a transfer that was
// partly delivered stays a fault even when the drain deadline cut its other
// paths off. Its text is the transfer error's own.
type partialTransferError struct{ transfer *m3ua.MTPTransferError }

func (err partialTransferError) Error() string { return err.transfer.Error() }

// transferOutcome returns an MTP-TRANSFER error as a send outcome. A transfer
// that failed on every path unwraps to its causes, so one the drain deadline
// cut off is counted like a direct write it cut off.
func transferOutcome(err error) error {
	var transfer *m3ua.MTPTransferError
	if errors.As(err, &transfer) && len(transfer.SuccessfulPaths) != 0 {
		return partialTransferError{transfer}
	}
	return err
}

func (sender *routingTimedSender) send(ctx context.Context, queue uint8, job routingTimedJob, counters *senderCounters) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		counters.complete(err, 0, 0)
		return
	}
	dispatchLag, err := job.dispatchDelay()
	if err != nil {
		counters.complete(err, 0, 0)
		return
	}
	if sender == nil || job.identity.Route >= routingRouteCount || queue != job.queue || sender.queues[job.identity.Route] != queue ||
		sender.routeIDs[job.identity.Route] != job.routeID || sender.paths.paths[job.identity.Route] != job.path {
		counters.complete(errors.New("routing timed job differs from its worker or frozen path"), dispatchLag, 0)
		return
	}
	payload, err := buildRoutePayload(job.identity, job.size)
	if err != nil {
		counters.complete(err, dispatchLag, 0)
		return
	}
	var sendErr error
	var sendStarted, sendEnded time.Time
	switch sender.variant {
	case routingTimedRouted:
		if sender.failover != nil {
			sender.sendFailover(job, payload, dispatchLag, counters)
			return
		}
		outcome := sender.routedSubmission(job, payload)
		result := outcome.result
		sendStarted, sendEnded, sendErr = outcome.started, outcome.ended, transferOutcome(outcome.err)
		if sendErr == nil && result.UserDataOctets != job.size {
			sendErr = fmt.Errorf("MTPTransfer wrote %d bytes, want %d", result.UserDataOctets, job.size)
		}
		if sendErr == nil && len(result.SuccessfulPaths) != 1 {
			sendErr = fmt.Errorf("MTPTransfer used %d paths, want 1", len(result.SuccessfulPaths))
		}
		if sendErr == nil && result.SuccessfulPaths[0] != job.path.Target {
			sendErr = errors.New("MTPTransfer used a path other than the frozen route")
		}
	case routingTimedDirect:
		// Admission, the context and the frozen-path revalidation run before
		// the send clock and the after-write revalidation after it, as routed's
		// path check does, so both timed regions hold Protocol Data
		// construction and one library call.
		write := sender.direct.begin(ctx, job.identity.Route, len(payload))
		if write.ready() {
			sendStarted = sender.now()
			write.submit(job.identity.Route, payload)
			sendEnded = sender.now()
		}
		_, sendErr = validateRoutingDirectOutcome(write.finish())
	default:
		counters.complete(errors.New("routing timed sender variant is invalid"), dispatchLag, 0)
		return
	}
	if job.clock != nil {
		sendErr = errors.Join(sendErr, job.clock.withinDrain(job.offset+dispatchLag))
	}
	counters.complete(sendErr, dispatchLag, sendEnded.Sub(sendStarted))
}

// withFailover returns a copy of the timed sender whose routed calls are
// classified by tracker, leaving the shared sender untouched for other
// cohorts.
func (sender *routingTimedSender) withFailover(tracker *failoverTracker) *routingTimedSender {
	copied := *sender
	copied.failover = tracker
	return &copied
}

// sendFailover submits one routed message of an SGP failure cohort. The call
// is timed on the shared clock around the same Protocol Data construction and
// MTPTransfer call as routed, and its outcome is classified instead of being
// required to use the frozen path. A failed-path outcome is accounted, never
// retried.
func (sender *routingTimedSender) sendFailover(job routingTimedJob, payload []byte, dispatchLag time.Duration, counters *senderCounters) {
	tracker := sender.failover
	started, startErr := tracker.clock.source.Now()
	outcome := sender.routedSubmission(job, payload)
	returned, returnErr := tracker.clock.source.Now()
	failed, err := tracker.classify(job.identity.Route, job.identity.Sequence, job.size, outcome.result, outcome.err, started, returned)
	if startErr != nil || returnErr != nil {
		err = errors.Join(err, errors.New("shared clock read around MTPTransfer failed"))
	}
	if job.clock != nil {
		err = errors.Join(err, job.clock.withinDrain(job.offset+dispatchLag))
	}
	counters.completeOutcome(failed, err, dispatchLag, outcome.ended.Sub(outcome.started))
}
