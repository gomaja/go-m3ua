package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gomaja/go-m3ua"
	"github.com/gomaja/go-m3ua/internal/perfstats"
	"github.com/gomaja/go-sctp"
)

// Failed-path and path-selection outcome classes of one MTPTransfer call.
const (
	failoverClassFrozen        = "frozen"
	failoverClassFailedSGP     = "failed-sgp"
	failoverClassAlternative   = "alternative"
	failoverClassNotSent       = "not-sent"
	failoverClassIndeterminate = "indeterminate"
	failoverClassOtherFailure  = "other-failure"
	failoverClassRefused       = "selection-refused"
)

const (
	failoverPass        = "pass"
	failoverFail        = "fail"
	failoverNotMeasured = "not-measured"
)

// failoverRecord is the SGP failure evidence. The receiver record carries
// only Receiver; the sender record also carries its own accounting, the
// criteria and the trial verdict.
type failoverRecord struct {
	Spec     sgpFailureSpec          `json:"spec"`
	Receiver *failoverReceiverRecord `json:"receiver,omitempty"`
	Sender   *failoverSenderRecord   `json:"sender,omitempty"`
	Criteria []failoverCriterion     `json:"criteria,omitempty"`
	Verdict  string                  `json:"verdict,omitempty"`
}

// failoverCriterion is one pass/fail judgement with the numbers behind it.
type failoverCriterion struct {
	Name     string `json:"name"`
	Outcome  string `json:"outcome"`
	Measured *int64 `json:"measured_ns,omitempty"`
	Budget   *int64 `json:"budget_ns,omitempty"`
	Detail   string `json:"detail"`
}

// failoverOutcomes counts every MTPTransfer call of the cohort by class.
type failoverOutcomes struct {
	Calls         uint64 `json:"calls"`
	Frozen        uint64 `json:"frozen"`
	FailedSGP     uint64 `json:"failed_peer_other_association"`
	Alternative   uint64 `json:"alternative"`
	NotSent       uint64 `json:"not_sent"`
	Indeterminate uint64 `json:"indeterminate"`
	OtherFailure  uint64 `json:"other_failure"`
	Refused       uint64 `json:"selection_refused"`
	Unexpected    uint64 `json:"unexpected"`
}

func (outcomes failoverOutcomes) sum() uint64 {
	return outcomes.Frozen + outcomes.FailedSGP + outcomes.Alternative + outcomes.NotSent + outcomes.Indeterminate +
		outcomes.OtherFailure + outcomes.Refused + outcomes.Unexpected
}

// failoverOutcomeSample is one failed-path outcome in detail.
type failoverOutcomeSample struct {
	Route       uint16             `json:"route"`
	Sequence    uint64             `json:"sequence"`
	Class       string             `json:"class"`
	Association m3ua.AssociationID `json:"association,omitempty"`
	CallStarted int64              `json:"call_started_ns"`
	Error       string             `json:"error,omitempty"`
}

// failoverNotification is one sender association ending during the cohort:
// the earliest public observation of the transport failure, Association.Done.
// The flags classify Error when the watch fired, and are what the
// failure_kind_observed criterion judges.
type failoverNotification struct {
	Association m3ua.AssociationID `json:"association"`
	SGP         m3ua.SGPIdentity   `json:"sgp"`
	At          int64              `json:"at_ns"`
	Error       string             `json:"error"`
	// EndOfStream is the io.EOF a completed SHUTDOWN leaves the reader.
	EndOfStream bool `json:"end_of_stream"`
	// CommunicationLost is the SCTP_COMM_LOST an ABORT or a path failure
	// raises, which the library reports as ErrSCTPNotAlive.
	CommunicationLost bool `json:"communication_lost"`
	// UserAbort is that loss carrying the User-Initiated Abort cause RFC 9260
	// Section 9.1 has an ABORT the peer's upper layer requested carry. The
	// library names the cause only in the error text.
	UserAbort bool `json:"user_abort"`
}

// newFailoverNotification records how one sender association ended.
func newFailoverNotification(association m3ua.AssociationID, sgp m3ua.SGPIdentity, at int64, err error) failoverNotification {
	lost := errors.Is(err, m3ua.ErrSCTPNotAlive)
	return failoverNotification{
		Association: association, SGP: sgp, At: at, Error: fmt.Sprint(err),
		EndOfStream: errors.Is(err, io.EOF), CommunicationLost: lost,
		UserAbort: lost && strings.Contains(err.Error(), sctp.ErrorCauseString(uint32(sctp.SCTP_ERROR_USER_ABORT))),
	}
}

// failoverAssociation is one sender association's submissions and the
// deliveries on its paired peer transport.
type failoverAssociation struct {
	Association     m3ua.AssociationID `json:"association"`
	SGP             m3ua.SGPIdentity   `json:"sgp"`
	PeerAssociation m3ua.AssociationID `json:"peer_association"`
	Failed          bool               `json:"failed"`
	Submitted       uint64             `json:"submitted"`
	Indeterminate   uint64             `json:"indeterminate"`
	Delivered       uint64             `json:"delivered"`
}

// failoverSenderRecord is the ASP's accounting and measurements.
type failoverSenderRecord struct {
	Notifications     []failoverNotification  `json:"notifications,omitempty"`
	Outcomes          failoverOutcomes        `json:"outcomes"`
	Associations      []failoverAssociation   `json:"associations"`
	FailedPath        failoverFailedPath      `json:"failed_path"`
	FaultToNotifyNS   []int64                 `json:"fault_to_notification_ns,omitempty"`
	Notification      int64                   `json:"notification_ns,omitempty"`
	FirstNotification int64                   `json:"first_notification_ns,omitempty"`
	FirstAlternative  *failoverFirst          `json:"first_alternative,omitempty"`
	RouteSwitch       failoverRouteSwitch     `json:"route_switch"`
	LastFailedSGPCall int64                   `json:"last_failed_peer_call_ns,omitempty"`
	LastFailureCall   int64                   `json:"last_failure_call_ns,omitempty"`
	Recovery          failoverRecovery        `json:"recovery"`
	Samples           []failoverOutcomeSample `json:"failed_path_samples,omitempty"`
	TransportTimers   failoverTimers          `json:"transport_timers"`
	// AssociationEvents is whether this kernel reports SCTP association
	// events, without which an ABORT is not seen as SCTP_COMM_LOST.
	AssociationEvents string `json:"kernel_association_events"`
	UnexpectedError   string `json:"unexpected_error,omitempty"`
	// LongestCallAfterFault is the MTPTransfer call that took longest among
	// those started at or after the declared fault instant.
	LongestCallAfterFault *failoverCall `json:"longest_call_after_fault,omitempty"`
}

// failoverCall is one timed MTPTransfer call on the shared clock.
type failoverCall struct {
	Route       uint16             `json:"route"`
	Class       string             `json:"class"`
	Association m3ua.AssociationID `json:"association,omitempty"`
	CallStarted int64              `json:"call_started_ns"`
	Returned    int64              `json:"returned_ns"`
	Duration    int64              `json:"duration_ns"`
}

// failoverFailedPath accounts every outcome on the failed path separately.
// Undelivered is what the failed path took: submitted plus indeterminate
// sends to the failed SGP that it never delivered.
type failoverFailedPath struct {
	Submitted     uint64 `json:"submitted"`
	Indeterminate uint64 `json:"indeterminate"`
	NotSent       uint64 `json:"not_sent"`
	OtherFailure  uint64 `json:"other_failure"`
	Refused       uint64 `json:"selection_refused"`
	Delivered     uint64 `json:"delivered"`
	Undelivered   uint64 `json:"undelivered"`
	Missing       uint64 `json:"receiver_missing"`
	Unexplained   int64  `json:"unexplained"`
}

// failoverFirst is the first successful MTPTransfer through the alternative.
type failoverFirst struct {
	Route       uint16             `json:"route"`
	Association m3ua.AssociationID `json:"association"`
	CallStarted int64              `json:"call_started_ns"`
	Returned    int64              `json:"returned_ns"`
	// Latency runs from the notification to the call's return.
	Latency int64 `json:"latency_ns"`
}

// failoverRouteSwitch summarizes the per-route move to the alternative.
type failoverRouteSwitch struct {
	AffectedRoutes int   `json:"affected_routes"`
	MovedRoutes    int   `json:"moved_routes"`
	MaxLatency     int64 `json:"max_latency_ns"`
	MedianLatency  int64 `json:"median_latency_ns"`
}

// failoverRecovery is the delivery-rate evidence from the receiver bins.
type failoverRecovery struct {
	PreFailureBins        int          `json:"pre_failure_bins"`
	PreFailurePerBin      float64      `json:"pre_failure_per_bin"`
	RecoveryBin           int          `json:"recovery_bin"`
	RecoveryBinSurviving  uint64       `json:"recovery_bin_surviving"`
	Recovery              int64        `json:"recovery_ns"`
	Milestone             int64        `json:"milestone_offset_ns"`
	PostScheduled         uint64       `json:"post_milestone_scheduled"`
	PostDelivered         uint64       `json:"post_milestone_delivered"`
	PostTrend             backlogTrend `json:"post_milestone_backlog_trend"`
	PostTrendSampleWindow int64        `json:"post_milestone_window_ns"`
}

// failoverTimers are the transport timer values in force: the kernel SCTP
// defaults of the sender's network namespace and each association's current
// retransmission timeout at cohort start.
type failoverTimers struct {
	Kernel      map[string]string `json:"kernel_sctp,omitempty"`
	PrimaryRTO  map[string]int64  `json:"primary_rto_ns,omitempty"`
	KernelError string            `json:"kernel_error,omitempty"`
}

var failoverKernelTimers = []string{
	"rto_initial", "rto_min", "rto_max", "path_max_retrans", "association_max_retrans", "hb_interval", "pf_retrans",
}

// The answers of the association events probe other than an error.
const (
	associationEventsSupported   = "supported"
	associationEventsUnsupported = "unsupported"
)

// failoverAssociationEventsProbe reports whether this kernel lets a socket
// subscribe to SCTP_ASSOC_CHANGE through SCTP_EVENT (RFC 6458 Section 6.2.2),
// which the library needs to report an ABORT as SCTP_COMM_LOST. Linux added
// SCTP_EVENT in 5.0; before it the library runs without the subscription and
// an ABORT surfaces as whichever error reaches the reader first. A variable so
// a test can stand in for the kernel.
var failoverAssociationEventsProbe = probeSCTPAssociationEvents

// probeSCTPAssociationEvents asks the question the library asks, the same
// way: a socket subscribing before it listens. It is closed at once.
func probeSCTPAssociationEvents() string {
	subscription := sctp.PreAssociationConfig{Notifications: []sctp.NotificationSubscription{
		{Type: sctp.SCTP_ASSOC_CHANGE, State: sctp.SocketOptionEnable},
	}}
	listener, err := (&sctp.SocketConfig{}).WithPreAssociation(subscription).Listen("sctp4", nil)
	switch {
	case err == nil:
		_ = listener.Close()
		return associationEventsSupported
	case errors.Is(err, syscall.ENOPROTOOPT):
		return associationEventsUnsupported
	default:
		return "unknown: " + err.Error()
	}
}

// failoverTracker classifies every MTPTransfer call of a failure cohort and
// watches the sender associations for the transport-failure notification.
type failoverTracker struct {
	spec               sgpFailureSpec
	clock              *sharedRunClock
	due                int64
	affected           [routingRouteCount]bool
	frozen             [routingRouteCount]m3ua.MTPTransferPath
	bindings           []routingBinding
	sgps               map[m3ua.AssociationID]m3ua.SGPIdentity
	failedSenders      map[m3ua.AssociationID]bool
	alternativeSenders map[m3ua.AssociationID]bool
	alternativeKey     m3ua.ASKey
	alternativePath    m3ua.MTPRoutePathID

	mutex             sync.Mutex
	outcomes          failoverOutcomes
	submitted         map[m3ua.AssociationID]uint64
	indeterminate     map[m3ua.AssociationID]uint64
	alternativeOf     [routingRouteCount]m3ua.AssociationID
	firstAlternative  [routingRouteCount]int64
	first             *failoverFirst
	lastFailedSGPCall int64
	lastFailureCall   int64
	samples           []failoverOutcomeSample
	longest           *failoverCall
	unexpectedError   string
	notifications     []failoverNotification
	timers            failoverTimers
	associationEvents string

	stop    chan struct{}
	watches sync.WaitGroup
}

func newFailoverTracker(spec sgpFailureSpec, clock *sharedRunClock, plane *routingDataSenderPlane, paths routingPathMap) (*failoverTracker, error) {
	topology, err := newRoutingTopology("primary")
	if err != nil || clock == nil || plane == nil || len(plane.bindings) != routedAssociations || !paths.ready || len(topology.Peers) != routedPeerCount {
		return nil, errors.New("SGP failure tracker needs the shared clock, the sender plane and frozen paths")
	}
	tracker := &failoverTracker{
		spec: spec, clock: clock, due: clock.window.Start + int64(spec.Offset), bindings: append([]routingBinding(nil), plane.bindings...),
		sgps: make(map[m3ua.AssociationID]m3ua.SGPIdentity), failedSenders: make(map[m3ua.AssociationID]bool), alternativeSenders: make(map[m3ua.AssociationID]bool),
		submitted: make(map[m3ua.AssociationID]uint64), indeterminate: make(map[m3ua.AssociationID]uint64), stop: make(chan struct{}),
	}
	for _, binding := range plane.bindings {
		tracker.sgps[binding.SenderAssociation] = binding.Peer.SGP
		switch binding.Peer.SGP {
		case sgpFailureFailed:
			tracker.failedSenders[binding.SenderAssociation] = true
		case sgpFailureAlternative:
			tracker.alternativeSenders[binding.SenderAssociation] = true
		}
	}
	if len(tracker.failedSenders) != 2 || len(tracker.alternativeSenders) != 2 {
		return nil, errors.New("SGP failure needs two associations at the failed and at the alternative SGP")
	}
	for index, peer := range topology.Peers {
		if peer.Identity == sgpFailureAlternative {
			tracker.alternativeKey = peer.ApplicationServers[0].ASKey
			tracker.alternativePath = topology.ASP.Routing.Paths[index/2].ID
		}
	}
	for route := range routingRouteCount {
		path, err := paths.path(uint16(route))
		if err != nil {
			return nil, err
		}
		tracker.frozen[route] = path.Target
		tracker.affected[route] = path.Target.SGP == sgpFailureFailed
	}
	return tracker, nil
}

// watch records, for every sender association, the shared-clock instant its
// Done channel closes while the cohort runs, and the transport timers in
// force at its start.
func (tracker *failoverTracker) watch(associations []*m3ua.Association) {
	tracker.timers = readFailoverTimers(associations)
	tracker.associationEvents = failoverAssociationEventsProbe()
	for _, association := range associations {
		tracker.watches.Add(1)
		go func(association *m3ua.Association) {
			defer tracker.watches.Done()
			select {
			case <-association.Done():
				now, _ := tracker.clock.source.Now()
				notification := newFailoverNotification(association.ID(), tracker.sgps[association.ID()], now, association.Err())
				tracker.mutex.Lock()
				tracker.notifications = append(tracker.notifications, notification)
				tracker.mutex.Unlock()
			case <-tracker.stop:
			}
		}(association)
	}
}

// finish stops the watchers. Associations still up are not notifications.
func (tracker *failoverTracker) finish() {
	close(tracker.stop)
	tracker.watches.Wait()
}

func readFailoverTimers(associations []*m3ua.Association) failoverTimers {
	timers := failoverTimers{Kernel: make(map[string]string), PrimaryRTO: make(map[string]int64)}
	var failures []string
	for _, name := range failoverKernelTimers {
		value, err := os.ReadFile("/proc/sys/net/sctp/" + name)
		if err != nil {
			failures = append(failures, name)
			continue
		}
		timers.Kernel[name] = strings.TrimSpace(string(value))
	}
	if len(failures) > 0 {
		timers.KernelError = "unreadable: " + strings.Join(failures, ",")
	}
	for _, association := range associations {
		if status, err := association.AssociationStatus(); err == nil && status != nil {
			timers.PrimaryRTO[fmt.Sprint(association.ID())] = int64(status.PrimaryRetransmissionTimeout)
		}
	}
	return timers
}

// classify accounts one MTPTransfer call. It reports whether the call is a
// failed-path outcome (accounted, never retried) and an error for any outcome
// the trial does not allow: a healthy route off its frozen path, a failure on
// a surviving path, or a route that moved anywhere but its same-SG/AS
// alternative.
func (tracker *failoverTracker) classify(route uint16, sequence uint64, size int, result m3ua.MTPTransferResult, sendErr error, started, returned int64) (bool, error) {
	tracker.mutex.Lock()
	defer tracker.mutex.Unlock()
	tracker.outcomes.Calls++
	class, association, err := tracker.classLocked(route, size, result, sendErr, started)
	if err != nil {
		tracker.outcomes.Unexpected++
		if tracker.unexpectedError == "" {
			tracker.unexpectedError = fmt.Sprintf("route %d sequence %d: %v", route, sequence, err)
		}
		return false, err
	}
	if started >= tracker.due && (tracker.longest == nil || returned-started > tracker.longest.Duration) {
		tracker.longest = &failoverCall{Route: route, Class: class, Association: association, CallStarted: started, Returned: returned, Duration: returned - started}
	}
	failed := false
	switch class {
	case failoverClassFrozen:
		tracker.outcomes.Frozen++
		tracker.submitted[association]++
	case failoverClassFailedSGP:
		tracker.outcomes.FailedSGP++
		tracker.submitted[association]++
	case failoverClassAlternative:
		tracker.outcomes.Alternative++
		tracker.submitted[association]++
		if tracker.firstAlternative[route] == 0 {
			tracker.firstAlternative[route] = returned
		}
		if tracker.first == nil || returned < tracker.first.Returned {
			tracker.first = &failoverFirst{Route: route, Association: association, CallStarted: started, Returned: returned}
		}
	case failoverClassNotSent:
		tracker.outcomes.NotSent++
		failed = true
	case failoverClassIndeterminate:
		tracker.outcomes.Indeterminate++
		tracker.indeterminate[association]++
		failed = true
	case failoverClassOtherFailure:
		tracker.outcomes.OtherFailure++
		failed = true
	case failoverClassRefused:
		tracker.outcomes.Refused++
		failed = true
	}
	if tracker.failedSenders[association] || class == failoverClassRefused {
		tracker.lastFailedSGPCall = max(tracker.lastFailedSGPCall, started)
	}
	if failed {
		tracker.lastFailureCall = max(tracker.lastFailureCall, started)
	}
	if (failed || class == failoverClassFailedSGP) && len(tracker.samples) < sgpFailureOutcomeSamples {
		sample := failoverOutcomeSample{Route: route, Sequence: sequence, Class: class, Association: association, CallStarted: started}
		if sendErr != nil {
			sample.Error = sendErr.Error()
		}
		tracker.samples = append(tracker.samples, sample)
	}
	return failed, nil
}

func (tracker *failoverTracker) classLocked(route uint16, size int, result m3ua.MTPTransferResult, sendErr error, started int64) (string, m3ua.AssociationID, error) {
	if route >= routingRouteCount {
		return "", 0, errors.New("route is out of range")
	}
	affected := tracker.affected[route]
	if sendErr == nil {
		if len(result.SuccessfulPaths) != 1 || result.UserDataOctets != size {
			return "", 0, fmt.Errorf("MTPTransfer used %d paths for %d octets, want 1 path and %d octets", len(result.SuccessfulPaths), result.UserDataOctets, size)
		}
		target := result.SuccessfulPaths[0]
		switch {
		case target == tracker.frozen[route]:
			return failoverClassFrozen, target.Association, nil
		case affected && tracker.onFailedSGP(route, target):
			return failoverClassFailedSGP, target.Association, nil
		case affected && tracker.onAlternative(target):
			if started < tracker.due {
				return "", 0, errors.New("MTPTransfer selected the alternative before the fault was due")
			}
			if previous := tracker.alternativeOf[route]; previous != 0 && previous != target.Association {
				return "", 0, errors.New("a moved route changed alternative association")
			}
			tracker.alternativeOf[route] = target.Association
			return failoverClassAlternative, target.Association, nil
		default:
			return "", 0, errors.New("MTPTransfer used a path that is neither the frozen route nor its same-SG/AS alternative")
		}
	}
	var selection *m3ua.MTPSelectionError
	if errors.As(sendErr, &selection) {
		if !affected {
			return "", 0, fmt.Errorf("a healthy route was refused: %w", sendErr)
		}
		return failoverClassRefused, 0, nil
	}
	var transfer *m3ua.MTPTransferError
	if !errors.As(sendErr, &transfer) || len(transfer.SuccessfulPaths) != 0 || len(transfer.Failures) != 1 {
		return "", 0, fmt.Errorf("MTPTransfer failed outside the failed path: %w", sendErr)
	}
	target := transfer.Failures[0].Target
	if !affected || !tracker.onFailedSGP(route, target) && target != tracker.frozen[route] {
		return "", 0, fmt.Errorf("MTPTransfer failed on a surviving path: %w", sendErr)
	}
	var write *m3ua.DataWriteError
	if !errors.As(transfer.Failures[0].Err, &write) {
		return failoverClassOtherFailure, target.Association, nil
	}
	switch write.Outcome {
	case m3ua.DataNotSent:
		return failoverClassNotSent, target.Association, nil
	case m3ua.DataSendIndeterminate:
		return failoverClassIndeterminate, target.Association, nil
	default:
		return failoverClassOtherFailure, target.Association, nil
	}
}

// onFailedSGP reports a target on the failed SGP in the route's own path and
// AS scope, on either of that SGP's associations.
func (tracker *failoverTracker) onFailedSGP(route uint16, target m3ua.MTPTransferPath) bool {
	frozen := tracker.frozen[route]
	return target.SGP == sgpFailureFailed && tracker.failedSenders[target.Association] && target.Path == frozen.Path &&
		target.ApplicationServer == frozen.ApplicationServer && target.AS == frozen.AS
}

// onAlternative reports a target on the same-SG/AS alternative.
func (tracker *failoverTracker) onAlternative(target m3ua.MTPTransferPath) bool {
	return target.SGP == sgpFailureAlternative && tracker.alternativeSenders[target.Association] && target.Path == tracker.alternativePath &&
		target.ApplicationServer == "primary" && target.AS == tracker.alternativeKey && target.Epoch != 0
}

// drained reports whether every submission on a surviving association has
// been delivered on its paired peer transport. The failed SGP's in-flight
// work is accounted, not awaited.
func (tracker *failoverTracker) drained(receiver runRecord) bool {
	if receiver.Failover == nil || receiver.Failover.Receiver == nil {
		return false
	}
	delivered := failoverDeliveredByTransport(receiver.Failover.Receiver)
	tracker.mutex.Lock()
	defer tracker.mutex.Unlock()
	for _, binding := range tracker.bindings {
		if tracker.failedSenders[binding.SenderAssociation] {
			continue
		}
		if delivered[binding.Peer] < tracker.submitted[binding.SenderAssociation] {
			return false
		}
	}
	return true
}

func failoverDeliveredByTransport(receiver *failoverReceiverRecord) map[routingTransport]uint64 {
	delivered := make(map[routingTransport]uint64, len(receiver.PerTransport))
	for _, count := range receiver.PerTransport {
		delivered[routingTransport{SGP: count.SGP, Association: count.Association}] = count.Unique
	}
	return delivered
}

// waitFailoverDrain is waitReceiverDrain for a failure cohort: it waits for
// every surviving-path submission rather than for every submission.
func waitFailoverDrain(ctx context.Context, baseURL string, tracker *failoverTracker, deadline time.Time) (runRecord, error) {
	for {
		requestContext, cancelRequest := context.WithDeadline(ctx, deadline)
		receiver, err := getReceiverResult(requestContext, baseURL)
		cancelRequest()
		if err != nil {
			return runRecord{}, err
		}
		if tracker.drained(receiver) {
			return receiver, nil
		}
		if !time.Now().Before(deadline) {
			return receiver, errors.New("receiver drain deadline exceeded")
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return receiver, ctx.Err()
		case <-timer.C:
		}
	}
}

// failoverInputs are the cohort observations the evaluation reads besides
// the tracker.
type failoverInputs struct {
	specification runSpec
	scheduled     uint64
	capped        uint64
	receiver      runRecord
	accounting    windowAccounting
}

// evaluate builds the sender failover record and judges every criterion.
func (tracker *failoverTracker) evaluate(inputs failoverInputs) *failoverRecord {
	tracker.mutex.Lock()
	defer tracker.mutex.Unlock()
	record := &failoverRecord{Spec: tracker.spec, Sender: &failoverSenderRecord{
		Outcomes: tracker.outcomes, LastFailedSGPCall: tracker.lastFailedSGPCall, LastFailureCall: tracker.lastFailureCall,
		Samples: append([]failoverOutcomeSample(nil), tracker.samples...), TransportTimers: tracker.timers, AssociationEvents: tracker.associationEvents,
		UnexpectedError: tracker.unexpectedError,
		Notifications:   append([]failoverNotification(nil), tracker.notifications...),
	}}
	if tracker.longest != nil {
		longest := *tracker.longest
		record.Sender.LongestCallAfterFault = &longest
	}
	sender := record.Sender
	sort.Slice(sender.Notifications, func(first, second int) bool { return sender.Notifications[first].At < sender.Notifications[second].At })
	if inputs.receiver.Failover != nil {
		record.Receiver = inputs.receiver.Failover.Receiver
	}
	receiver := record.Receiver
	clock := inputs.specification.Clock
	if receiver == nil || clock == nil {
		record.Criteria = []failoverCriterion{{Name: "fault_injected", Outcome: failoverFail, Detail: "the receiver record carries no failure evidence"}}
		record.Verdict = failoverFail
		return record
	}
	tracker.accountLocked(record, inputs)
	record.Criteria = append(record.Criteria, tracker.faultCriterion(receiver, clock))
	notified := tracker.notificationCriterionLocked(record, receiver)
	record.Criteria = append(record.Criteria, notified, tracker.failureKindCriterionLocked(record, receiver))
	notification := sender.Notification
	if notified.Outcome != failoverPass {
		notification = 0
	}
	record.Criteria = append(record.Criteria,
		failoverPreFailureCriterion(record, inputs),
		tracker.selectionCriterionLocked(record, notification),
		failoverRecoveryCriterion(record, inputs, notification),
		failoverFullRateCriterion(record, inputs, notification),
		failoverHealthyCriterion(record, inputs),
		failoverAccountingCriterion(record, inputs),
	)
	record.Verdict = failoverPass
	for _, criterion := range record.Criteria {
		switch {
		case criterion.Outcome == failoverFail:
			record.Verdict = failoverFail
		case criterion.Outcome == failoverNotMeasured && record.Verdict == failoverPass:
			record.Verdict = verdictInconclusive
		}
	}
	return record
}

// accountLocked fills the per-association and failed-path accounting.
func (tracker *failoverTracker) accountLocked(record *failoverRecord, inputs failoverInputs) {
	sender, receiver := record.Sender, record.Receiver
	delivered := failoverDeliveredByTransport(receiver)
	var failedDelivered uint64
	for _, binding := range tracker.bindings {
		failed := tracker.failedSenders[binding.SenderAssociation]
		association := failoverAssociation{
			Association: binding.SenderAssociation, SGP: binding.Peer.SGP, PeerAssociation: binding.Peer.Association, Failed: failed,
			Submitted: tracker.submitted[binding.SenderAssociation], Indeterminate: tracker.indeterminate[binding.SenderAssociation],
			Delivered: delivered[binding.Peer],
		}
		sender.Associations = append(sender.Associations, association)
		if failed {
			sender.FailedPath.Submitted += association.Submitted
			sender.FailedPath.Indeterminate += association.Indeterminate
			failedDelivered += association.Delivered
		}
	}
	outcomes := tracker.outcomes
	path := &sender.FailedPath
	path.NotSent, path.OtherFailure, path.Refused, path.Delivered = outcomes.NotSent, outcomes.OtherFailure, outcomes.Refused, failedDelivered
	if path.Submitted+path.Indeterminate >= failedDelivered {
		path.Undelivered = path.Submitted + path.Indeterminate - failedDelivered
	}
	path.Missing = inputs.receiver.Delivery.Missing
	attributed := path.Undelivered + path.NotSent + path.OtherFailure + path.Refused + inputs.capped + outcomes.Unexpected
	path.Unexplained = int64(path.Missing) - int64(attributed)
}

func (tracker *failoverTracker) faultCriterion(receiver *failoverReceiverRecord, clock *sharedClockWindow) failoverCriterion {
	criterion := failoverCriterion{Name: "fault_injected", Outcome: failoverFail}
	fault := receiver.Fault
	switch {
	case fault == nil:
		criterion.Detail = "the receiver never injected the declared fault"
	case fault.Due != tracker.due || fault.Before < fault.Due || fault.After < fault.Before || fault.After >= clock.End || len(fault.Associations) != 2:
		criterion.Detail = fmt.Sprintf("the fault ran outside its declared instant or window: due %d, before %d, after %d, %d associations", fault.Due, fault.Before, fault.After, len(fault.Associations))
	default:
		lateness := fault.Before - fault.Due
		criterion.Outcome = failoverPass
		criterion.Measured = &lateness
		method := sgpFailureMethod(fault.Kind)
		criterion.Detail = fmt.Sprintf("%s of %s/%s at offset %s (%d ns after due); both Association.%s calls returned within %d ns",
			fault.Kind, fault.SGP.SignallingGateway, fault.SGP.SignallingGatewayProcess, time.Duration(fault.Before-clock.Start), lateness, method, fault.After-fault.Before)
		// A call that returned an error may not have done what the kind
		// declares: an Abort whose SO_LINGER failed closes gracefully.
		for _, closed := range fault.Associations {
			if closed.Error != "" {
				criterion.Outcome = failoverFail
				criterion.Detail += fmt.Sprintf("; association %d %s: %s", closed.Association, method, closed.Error)
			}
		}
	}
	return criterion
}

// failureKindCriterionLocked requires the failure that reached the ASP to be
// the declared one. The declaration says what the SGP was asked to do; how
// both failed-SGP associations ended says what its SCTP layer did. A close
// trial needs the end of stream a completed SHUTDOWN leaves, and an abort
// trial the SCTP_COMM_LOST with the User-Initiated Abort cause an ABORT raises
// (RFC 9260 Section 9.1). An SGP whose Abort fell back to a SHUTDOWN, or
// whose Close ended in the dependency's ABORT fallback, fails here. An abort
// is visible as SCTP_COMM_LOST only where the kernel reports association
// events, so an abort trial on a kernel without them is not measured rather
// than passed; a SHUTDOWN's end of stream needs no events.
func (tracker *failoverTracker) failureKindCriterionLocked(record *failoverRecord, receiver *failoverReceiverRecord) failoverCriterion {
	criterion := failoverCriterion{Name: "failure_kind_observed", Outcome: failoverNotMeasured}
	if receiver.Fault == nil {
		criterion.Detail = "no fault was injected"
		return criterion
	}
	var ended []failoverNotification
	for _, notification := range record.Sender.Notifications {
		if tracker.failedSenders[notification.Association] {
			ended = append(ended, notification)
		}
	}
	if len(ended) != 2 {
		criterion.Detail = fmt.Sprintf("%d of the 2 failed-SGP associations reported their end", len(ended))
		return criterion
	}
	abort := tracker.spec.Kind == sgpFailureKindAbort
	if abort && record.Sender.AssociationEvents != associationEventsSupported {
		criterion.Detail = fmt.Sprintf("SCTP association events are %s on this kernel, so an ABORT cannot be told from any other loss", record.Sender.AssociationEvents)
		return criterion
	}
	criterion.Outcome = failoverFail
	for _, notification := range ended {
		switch {
		case abort && (!notification.CommunicationLost || !notification.UserAbort):
			criterion.Detail = fmt.Sprintf("association %d ended with %q, not the SCTP_COMM_LOST with the User-Initiated Abort cause an ABORT raises", notification.Association, notification.Error)
			return criterion
		case !abort && !notification.EndOfStream:
			criterion.Detail = fmt.Sprintf("association %d ended with %q, not the end of stream a completed SHUTDOWN leaves", notification.Association, notification.Error)
			return criterion
		}
	}
	criterion.Outcome = failoverPass
	criterion.Detail = "both failed-SGP associations ended at the end of stream: the SGP's SHUTDOWN completed"
	if abort {
		criterion.Detail = "both failed-SGP associations ended on SCTP_COMM_LOST with the User-Initiated Abort cause: the SGP's ABORT reached the ASP"
	}
	return criterion
}

// notificationCriterionLocked requires both failed-SGP associations, and no
// other, to have ended after the fault. The failure-detection time is
// recorded, not budgeted: section 4 excludes it from the failover budgets.
func (tracker *failoverTracker) notificationCriterionLocked(record *failoverRecord, receiver *failoverReceiverRecord) failoverCriterion {
	sender := record.Sender
	criterion := failoverCriterion{Name: "transport_failure_notified", Outcome: failoverFail}
	if receiver.Fault == nil {
		criterion.Outcome, criterion.Detail = failoverNotMeasured, "no fault was injected"
		return criterion
	}
	var failed, other int
	for _, notification := range sender.Notifications {
		if !tracker.failedSenders[notification.Association] {
			other++
			continue
		}
		if notification.At < receiver.Fault.Before {
			criterion.Detail = fmt.Sprintf("association %d ended before the fault", notification.Association)
			return criterion
		}
		failed++
		if sender.FirstNotification == 0 {
			sender.FirstNotification = notification.At
		}
		sender.Notification = notification.At
		sender.FaultToNotifyNS = append(sender.FaultToNotifyNS, notification.At-receiver.Fault.Before)
	}
	switch {
	case other != 0:
		criterion.Detail = fmt.Sprintf("%d surviving association(s) ended during the cohort", other)
	case failed != 2:
		criterion.Detail = fmt.Sprintf("%d of the 2 failed-SGP associations reported the failure", failed)
	default:
		detection := sender.Notification - receiver.Fault.Before
		criterion.Outcome = failoverPass
		criterion.Measured = &detection
		criterion.Detail = fmt.Sprintf("Association.Done closed on both failed-SGP associations %d ns and %d ns after the fault (recorded, not budgeted); notification is the later", sender.FaultToNotifyNS[0], sender.FaultToNotifyNS[1])
	}
	return criterion
}

// selectionCriterionLocked judges "usable alternative selection within 100
// ms": the first successful MTPTransfer through the alternative returns
// within the budget of the notification, and no call started after the
// budget touched the failed SGP or failed.
func (tracker *failoverTracker) selectionCriterionLocked(record *failoverRecord, notification int64) failoverCriterion {
	sender := record.Sender
	budget := int64(tracker.spec.SelectionBudget)
	criterion := failoverCriterion{Name: "alternative_selection", Outcome: failoverFail, Budget: &budget}
	var latencies []int64
	for route, affected := range tracker.affected {
		if !affected {
			continue
		}
		sender.RouteSwitch.AffectedRoutes++
		if tracker.firstAlternative[route] != 0 {
			sender.RouteSwitch.MovedRoutes++
			if notification != 0 {
				latencies = append(latencies, tracker.firstAlternative[route]-notification)
			}
		}
	}
	sort.Slice(latencies, func(first, second int) bool { return latencies[first] < latencies[second] })
	if len(latencies) > 0 {
		sender.RouteSwitch.MaxLatency = latencies[len(latencies)-1]
		sender.RouteSwitch.MedianLatency = latencies[len(latencies)/2]
	}
	if notification == 0 {
		criterion.Outcome, criterion.Detail = failoverNotMeasured, "no transport-failure notification to measure from"
		return criterion
	}
	if tracker.first == nil {
		criterion.Detail = "no MTPTransfer ever succeeded through the alternative"
		return criterion
	}
	first := *tracker.first
	first.Latency = first.Returned - notification
	sender.FirstAlternative = &first
	// The notification is the later of the failed SGP's two association ends,
	// so the alternative can already be in use when it arrives. The evidence
	// keeps the signed latency; the judged one is zero.
	measured := max(first.Latency, 0)
	criterion.Measured = &measured
	returned := fmt.Sprintf("returned %d ns after the notification", first.Latency)
	if first.Latency < 0 {
		returned = fmt.Sprintf("returned %d ns before the notification, the later of the failed SGP's association ends", -first.Latency)
	}
	limit := notification + budget
	switch {
	case first.Latency > budget:
		criterion.Detail = fmt.Sprintf("first alternative MTPTransfer returned %d ns after the notification", first.Latency)
	case tracker.lastFailedSGPCall > limit:
		criterion.Detail = fmt.Sprintf("a call started %d ns after the notification still used or was refused on the failed SGP", tracker.lastFailedSGPCall-notification)
	case tracker.lastFailureCall > limit:
		criterion.Detail = fmt.Sprintf("a call started %d ns after the notification failed", tracker.lastFailureCall-notification)
	case sender.RouteSwitch.MovedRoutes != sender.RouteSwitch.AffectedRoutes:
		criterion.Detail = fmt.Sprintf("%d of %d affected routes moved to the alternative", sender.RouteSwitch.MovedRoutes, sender.RouteSwitch.AffectedRoutes)
	default:
		criterion.Outcome = failoverPass
		criterion.Detail = fmt.Sprintf("first alternative MTPTransfer started %d ns after the notification and %s; all %d affected routes moved (per-route median %d ns, max %d ns, bounded below by each route's own send interval); last failed-SGP call started %s and last failed call %s",
			first.CallStarted-notification, returned, sender.RouteSwitch.AffectedRoutes, sender.RouteSwitch.MedianLatency, sender.RouteSwitch.MaxLatency,
			failoverRelative(tracker.lastFailedSGPCall, notification), failoverRelative(tracker.lastFailureCall, notification))
	}
	return criterion
}

// failoverRecoveryCriterion judges "healthy-path delivery returns to at least
// 90% of pre-failure rate within 1 s" on the receiver's arrival bins: the
// pre-failure rate is the mean over whole bins from one second after the
// window start to the fault, and recovery is the end of the first whole bin
// starting at or after the notification whose surviving-SGP deliveries reach
// the percentage.
func failoverRecoveryCriterion(record *failoverRecord, inputs failoverInputs, notification int64) failoverCriterion {
	receiver, recovery := record.Receiver, &record.Sender.Recovery
	budget := int64(record.Spec.RecoveryBudget)
	criterion := failoverCriterion{Name: "healthy_path_recovery", Outcome: failoverNotMeasured, Budget: &budget}
	clock := inputs.specification.Clock
	bin := int64(receiver.Bin)
	if receiver.Fault == nil || notification == 0 || bin <= 0 || len(receiver.ArrivalBins) != len(receiver.SurvivingBins) {
		criterion.Detail = "no fault, notification or arrival bins to measure from"
		return criterion
	}
	firstBin := int((int64(sgpFailurePreRateSkip) + bin - 1) / bin)
	lastBin := int((receiver.Fault.Before - clock.Start) / bin)
	var total uint64
	for index := firstBin; index < lastBin && index < len(receiver.ArrivalBins); index++ {
		total += receiver.ArrivalBins[index]
		recovery.PreFailureBins++
	}
	if recovery.PreFailureBins < 5 {
		criterion.Detail = fmt.Sprintf("only %d whole pre-failure bins", recovery.PreFailureBins)
		return criterion
	}
	recovery.PreFailurePerBin = float64(total) / float64(recovery.PreFailureBins)
	notified := notification - clock.Start
	start := int((notified + bin - 1) / bin)
	recovery.RecoveryBin = -1
	for index := start; index < len(receiver.SurvivingBins) && int64(index+1)*bin <= int64(inputs.specification.Duration); index++ {
		if float64(receiver.SurvivingBins[index])*100 >= float64(record.Spec.RecoveryPercent)*recovery.PreFailurePerBin {
			recovery.RecoveryBin = index
			recovery.RecoveryBinSurviving = receiver.SurvivingBins[index]
			recovery.Recovery = int64(index+1)*bin - notified
			break
		}
	}
	criterion.Outcome = failoverFail
	if recovery.RecoveryBin < 0 {
		criterion.Detail = fmt.Sprintf("surviving-SGP delivery never reached %d%% of the pre-failure %.1f per %s", record.Spec.RecoveryPercent, recovery.PreFailurePerBin, receiver.Bin)
		return criterion
	}
	criterion.Measured = &recovery.Recovery
	criterion.Detail = fmt.Sprintf("pre-failure %.1f deliveries per %s over %d bins; surviving SGPs delivered %d in the bin ending %d ns after the notification",
		recovery.PreFailurePerBin, receiver.Bin, recovery.PreFailureBins, recovery.RecoveryBinSurviving, recovery.Recovery)
	if recovery.Recovery <= budget {
		criterion.Outcome = failoverPass
	}
	return criterion
}

// failoverPreFailureCriterion requires the period before the fault to be
// nominal: every message scheduled in the whole bins that end at least one bin
// before the fault is delivered, on whatever path and however late. The guard
// bin leaves out what may still have been in flight to the failed SGP when it
// failed. Loss before that is not the failure's; without this criterion it
// would be absorbed into the failed path's accounting and would lower the
// pre-failure rate the recovery is judged against.
func failoverPreFailureCriterion(record *failoverRecord, inputs failoverInputs) failoverCriterion {
	receiver, specification := record.Receiver, inputs.specification
	criterion := failoverCriterion{Name: "pre_failure_nominal", Outcome: failoverNotMeasured}
	if receiver.Fault == nil || receiver.Bin <= 0 {
		criterion.Detail = "no fault or scheduled bins to measure from"
		return criterion
	}
	bins := int((receiver.Fault.Before-specification.Clock.Start)/int64(receiver.Bin)) - 1
	if bins < 1 || bins > len(receiver.ScheduledBins) {
		criterion.Detail = fmt.Sprintf("no whole pre-failure bin outside the guard bin (%d)", bins)
		return criterion
	}
	end := time.Duration(bins) * receiver.Bin
	scheduled := failoverScheduledIn(specification.Rate, specification.Expected, 0, end)
	var delivered uint64
	for _, count := range receiver.ScheduledBins[:bins] {
		delivered += count
	}
	missing := int64(scheduled) - int64(delivered)
	criterion.Measured = &missing
	criterion.Detail = fmt.Sprintf("%d of %d messages scheduled before offset %s, at least %s before the fault, delivered", delivered, scheduled, end, receiver.Bin)
	criterion.Outcome = failoverFail
	if missing == 0 {
		criterion.Outcome = failoverPass
	}
	return criterion
}

// failoverScheduledIn counts the messages the open-loop schedule places in
// [from, to) of the window: message i is scheduled at floor(i*1s/rate).
func failoverScheduledIn(rate, expected uint64, from, to time.Duration) uint64 {
	index := func(offset time.Duration) uint64 {
		if offset <= 0 {
			return 0
		}
		first := (uint64(offset)*rate + uint64(time.Second) - 1) / uint64(time.Second)
		return min(first, expected)
	}
	if to <= from {
		return 0
	}
	return index(to) - index(from)
}

// failoverFullRateCriterion judges "surviving-path fixed-load traffic must
// return to full offered-rate delivery with stable backlog": every message
// scheduled from the recovery milestone (notification plus the recovery
// budget) to the window end is delivered, and the sender-window backlog
// trend over that interval is not growing.
func failoverFullRateCriterion(record *failoverRecord, inputs failoverInputs, notification int64) failoverCriterion {
	receiver, recovery := record.Receiver, &record.Sender.Recovery
	criterion := failoverCriterion{Name: "full_rate_after_recovery", Outcome: failoverNotMeasured}
	specification := inputs.specification
	if notification == 0 || receiver.Bin <= 0 {
		criterion.Detail = "no notification to measure from"
		return criterion
	}
	milestone := time.Duration(notification-specification.Clock.Start) + record.Spec.RecoveryBudget
	firstBin := int((milestone + receiver.Bin - 1) / receiver.Bin)
	recovery.Milestone = int64(time.Duration(firstBin) * receiver.Bin)
	if time.Duration(recovery.Milestone) >= specification.Duration {
		criterion.Detail = "the recovery milestone is outside the measurement window"
		return criterion
	}
	for index := firstBin; index < len(receiver.ScheduledBins); index++ {
		from := time.Duration(index) * receiver.Bin
		recovery.PostScheduled += failoverScheduledIn(specification.Rate, specification.Expected, from, min(from+receiver.Bin, specification.Duration))
		recovery.PostDelivered += receiver.ScheduledBins[index]
	}
	window := specification.Duration - time.Duration(recovery.Milestone)
	recovery.PostTrendSampleWindow = int64(window)
	var observations []backlogInterval
	for _, sample := range inputs.accounting.Samples {
		if sample.Before > time.Duration(recovery.Milestone) && sample.After < specification.Duration {
			shifted := sample
			shifted.Before -= time.Duration(recovery.Milestone)
			shifted.After -= time.Duration(recovery.Milestone)
			observations = append(observations, shifted)
		}
	}
	recovery.PostTrend = describeBacklogTrend(observations, window, specification.Rate)
	loss := int64(recovery.PostScheduled) - int64(recovery.PostDelivered)
	criterion.Measured = &loss
	criterion.Detail = fmt.Sprintf("from offset %s: %d scheduled, %d delivered; backlog trend %s over %d samples (growth %.1f..%.1f, floor %.1f)",
		time.Duration(recovery.Milestone), recovery.PostScheduled, recovery.PostDelivered, recovery.PostTrend.Status,
		recovery.PostTrend.SampleCount, recovery.PostTrend.GrowthLower, recovery.PostTrend.GrowthUpper, recovery.PostTrend.Floor)
	switch {
	case loss != 0 || recovery.PostTrend.Status == string(perfstats.BacklogGrowing):
		criterion.Outcome = failoverFail
	case recovery.PostTrend.Status == string(perfstats.BacklogNotGrowing):
		criterion.Outcome = failoverPass
	}
	return criterion
}

// failoverHealthyCriterion requires the routes and paths the failure did not
// touch to stay nominal: no unexpected outcome, every surviving association's
// submissions delivered on its own transport, and no invalid, duplicate or
// nominal reordered delivery.
func failoverHealthyCriterion(record *failoverRecord, inputs failoverInputs) failoverCriterion {
	criterion := failoverCriterion{Name: "healthy_routes_nominal", Outcome: failoverFail}
	delivery := inputs.receiver.Delivery
	for _, association := range record.Sender.Associations {
		if !association.Failed && (association.Delivered != association.Submitted || association.Indeterminate != 0) {
			criterion.Detail = fmt.Sprintf("surviving association %d: %d submitted, %d delivered, %d indeterminate", association.Association, association.Submitted, association.Delivered, association.Indeterminate)
			return criterion
		}
	}
	switch {
	case record.Sender.Outcomes.Unexpected != 0:
		criterion.Detail = fmt.Sprintf("%d unexpected outcomes: %s", record.Sender.Outcomes.Unexpected, record.Sender.UnexpectedError)
	case delivery.Invalid != 0 || delivery.Duplicate != 0 || delivery.Reordered != 0 || delivery.LateAfterStop != 0:
		criterion.Detail = fmt.Sprintf("receiver: %d invalid, %d duplicate, %d reordered outside the failover, %d late", delivery.Invalid, delivery.Duplicate, delivery.Reordered, delivery.LateAfterStop)
	default:
		criterion.Outcome = failoverPass
		criterion.Detail = fmt.Sprintf("every surviving-association submission delivered on its transport; %d failover reorders on moved routes", record.Receiver.FailoverReordered)
	}
	return criterion
}

// failoverAccountingCriterion requires every scheduled message to have
// exactly one accounted outcome, one MTPTransfer call per admitted message
// (no retry), and every receiver-missing message to be a failed-path outcome.
func failoverAccountingCriterion(record *failoverRecord, inputs failoverInputs) failoverCriterion {
	criterion := failoverCriterion{Name: "failed_path_accounted", Outcome: failoverFail}
	outcomes, path := record.Sender.Outcomes, record.Sender.FailedPath
	switch {
	case inputs.scheduled != inputs.specification.Expected:
		criterion.Detail = fmt.Sprintf("%d of %d messages were scheduled", inputs.scheduled, inputs.specification.Expected)
	case inputs.capped != 0:
		criterion.Detail = fmt.Sprintf("%d messages were refused by the outstanding cap and never sent", inputs.capped)
	case outcomes.Calls != inputs.scheduled || outcomes.sum() != outcomes.Calls:
		criterion.Detail = fmt.Sprintf("%d scheduled, %d MTPTransfer calls, %d classified outcomes", inputs.scheduled, outcomes.Calls, outcomes.sum())
	case path.Delivered > path.Submitted+path.Indeterminate:
		criterion.Detail = fmt.Sprintf("the failed SGP delivered %d, more than %d submitted and %d indeterminate", path.Delivered, path.Submitted, path.Indeterminate)
	case path.Unexplained != 0:
		criterion.Detail = fmt.Sprintf("%d receiver-missing messages are not failed-path outcomes", path.Unexplained)
	default:
		criterion.Outcome = failoverPass
		criterion.Detail = fmt.Sprintf("%d calls for %d scheduled messages, none retried; failed path: %d submitted (%d delivered, %d undelivered), %d not sent, %d indeterminate, %d other failures, %d refused",
			outcomes.Calls, inputs.scheduled, path.Submitted, path.Delivered, path.Undelivered, path.NotSent, path.Indeterminate, path.OtherFailure, path.Refused)
	}
	return criterion
}

// evaluateFailover replaces the nominal evaluation for a failure cohort's
// records: the trial deliberately loses the failed path's work, so the
// nominal zero-loss rule does not apply; the failover criteria and the
// fixture's own validity do.
func (record *runRecord) evaluateFailover() {
	record.UnsupportedModes = map[string]string{
		"capacity":                    "unavailable: a failure trial is correctness and recovery evidence, not a capacity probe",
		"independent_peer_validation": "unavailable: both endpoints use this binary",
	}
	// Each trial measures one kind of failure; the record names the other.
	if record.Failover.Spec.Kind == sgpFailureKindAbort {
		record.UnsupportedModes["graceful_failure"] = "unavailable in this trial: the failed SGP ends its associations with Association.Abort (SCTP ABORT); -sgp-failure-kind=close measures Association.Close (SCTP SHUTDOWN)"
	} else {
		record.UnsupportedModes["abortive_failure"] = "unavailable in this trial: the failed SGP ends its associations with Association.Close (SCTP SHUTDOWN); -sgp-failure-kind=abort measures Association.Abort (SCTP ABORT)"
	}
	record.CapacityVerdict = "unavailable"
	record.Reasons = nil
	invalid := func(reason string) {
		record.Reasons = append(record.Reasons, reason)
		record.Verdict, record.FixtureVerdict = verdictInvalid, verdictInvalid
	}
	if record.FatalError != "" {
		invalid("fatal network fixture error")
	}
	if record.Side == "receiver" {
		if record.Failover.Receiver == nil || record.Failover.Receiver.Fault == nil {
			invalid("the declared fault was not injected")
		}
		if record.Delivery.Duplicate != 0 || record.Delivery.Invalid != 0 || record.Delivery.Reordered != 0 || record.Delivery.LateAfterStop != 0 {
			invalid("receiver observed duplicate, invalid, reordered, or late traffic")
		}
		if record.Verdict == verdictInvalid {
			return
		}
		record.Verdict, record.FixtureVerdict = verdictPass, verdictPass
		return
	}
	if record.Verdict == verdictInvalid {
		return
	}
	record.FixtureVerdict = verdictPass
	switch record.Failover.Verdict {
	case failoverPass:
		record.Verdict = verdictPass
	case failoverFail:
		record.Verdict = verdictFail
		for _, criterion := range record.Failover.Criteria {
			if criterion.Outcome == failoverFail {
				record.Reasons = append(record.Reasons, "failover criterion failed: "+criterion.Name)
			}
		}
	default:
		record.Verdict = verdictInconclusive
		record.Reasons = append(record.Reasons, "a failover criterion could not be measured")
	}
	if record.Verdict == verdictPass && validNondecreasingCPUCounter(record.CPU, "nr_throttled") && record.CPU.After["nr_throttled"] != record.CPU.Before["nr_throttled"] {
		record.Verdict = verdictInconclusive
		record.Reasons = append(record.Reasons, "cgroup reported CPU throttling")
	}
}

// completeOutcome finishes one classified send: a failed-path outcome counts
// as a send error without being a fixture failure, and only an error the
// classification refused is fatal.
func (counters *senderCounters) completeOutcome(failed bool, err error, dispatchLag, sendDuration time.Duration) {
	if err != nil || !failed {
		counters.complete(err, dispatchLag, sendDuration)
		return
	}
	counters.mutex.Lock()
	defer counters.mutex.Unlock()
	if counters.outstanding > 0 {
		counters.outstanding--
	}
	counters.sendErrors++
	counters.dispatchLag.record(dispatchLag)
	counters.sendTime.record(sendDuration)
}

// failoverRelative renders a shared-clock instant relative to the
// notification, or "none" for an instant never observed.
func failoverRelative(at, notification int64) string {
	if at == 0 {
		return "none"
	}
	return fmt.Sprintf("%d ns relative to the notification", at-notification)
}
