package main

import (
	"sync"
	"time"

	"github.com/gomaja/go-m3ua"
)

// echoRequestDeadline is the approved two-second per-request deadline.
// Requests that outlive it are counted failures, never omitted from the
// report and never included in the RTT percentiles.
const echoRequestDeadline = 2 * time.Second

const echoSweepInterval = 25 * time.Millisecond

// echoResult is the sender-side echo cohort report. RTT percentiles cover
// validated replies only, measured from each request's scheduled dispatch
// time on the sender's own monotonic clock; they are round-trip times and
// are never divided or relabeled as one-way latency.
type echoResult struct {
	Scope                 string              `json:"scope"`
	Deadline              time.Duration       `json:"deadline_ns"`
	OutstandingLimit      int                 `json:"outstanding_limit"`
	Requests              uint64              `json:"requests"`
	Validated             uint64              `json:"validated"`
	Capped                uint64              `json:"capped"`
	DeadlineExceeded      uint64              `json:"deadline_exceeded"`
	Invalid               uint64              `json:"invalid"`
	OutstandingAfterDrain uint64              `json:"outstanding_after_drain"`
	RTT                   durationPercentiles `json:"rtt"`
}

// receiverEchoResult is the receiver-side echo reply report. Replies are not
// useful deliveries and never add to the offered-load count. RepliesDropped
// counts validated requests whose reply was never written — reply queue full
// or the cohort ended first; drops are losses and fail fixture validity.
type receiverEchoResult struct {
	Scope          string `json:"scope"`
	Replies        uint64 `json:"replies"`
	ReplyErrors    uint64 `json:"reply_errors"`
	RepliesDropped uint64 `json:"replies_dropped"`
}

const (
	echoSenderScope   = "round-trip scheduled-request-to-validated-reply on the sender monotonic clock; never one-way latency"
	echoReceiverScope = "echo replies are not useful deliveries and are excluded from the offered-load count"
)

// echoTracker tracks one cohort's outstanding echo requests. Outstanding
// admission replaces the throughput outstanding reservation in echo mode;
// the cap is counted, not hidden.
type echoTracker struct {
	mutex        sync.Mutex
	cohort       string
	cohortHash   [16]byte
	seed         uint64
	associations int
	workload     workload
	limit        int
	deadline     time.Duration
	outstanding  map[uint64]time.Time
	validated    uint64
	capped       uint64
	deadlined    uint64
	invalid      uint64
	rtt          *durationHistogram
}

func newEchoTracker(cohort string, seed uint64, associations int, workload workload, limit int, deadline time.Duration) *echoTracker {
	return &echoTracker{
		cohort:       cohort,
		cohortHash:   cohortHash(cohort),
		seed:         seed,
		associations: associations,
		workload:     workload,
		limit:        limit,
		deadline:     deadline,
		outstanding:  make(map[uint64]time.Time),
		rtt:          newDurationHistogram(),
	}
}

// admit registers one scheduled request. It returns false when the
// outstanding cap is reached; the caller counts a cap failure and does not
// send the request.
func (tracker *echoTracker) admit(index uint64, scheduled time.Time) bool {
	tracker.mutex.Lock()
	defer tracker.mutex.Unlock()
	if len(tracker.outstanding) >= tracker.limit {
		tracker.capped++
		return false
	}
	tracker.outstanding[index] = scheduled
	return true
}

// fail removes a request whose send failed; the send error is counted in the
// sender counters, not here.
func (tracker *echoTracker) fail(index uint64) {
	tracker.mutex.Lock()
	delete(tracker.outstanding, index)
	tracker.mutex.Unlock()
}

// complete validates one reply arrival. A reply whose request is no longer
// outstanding (unknown, duplicated, or already deadline-swept) is invalid.
func (tracker *echoTracker) complete(index uint64, now time.Time) {
	tracker.mutex.Lock()
	defer tracker.mutex.Unlock()
	scheduled, ok := tracker.outstanding[index]
	if !ok {
		tracker.invalid++
		return
	}
	delete(tracker.outstanding, index)
	rtt := now.Sub(scheduled)
	if rtt > tracker.deadline {
		tracker.deadlined++
		return
	}
	tracker.validated++
	tracker.rtt.record(rtt)
}

// sweep removes every request older than the deadline and counts each as a
// deadline failure.
func (tracker *echoTracker) sweep(now time.Time) {
	tracker.mutex.Lock()
	defer tracker.mutex.Unlock()
	for index, scheduled := range tracker.outstanding {
		if now.Sub(scheduled) > tracker.deadline {
			delete(tracker.outstanding, index)
			tracker.deadlined++
		}
	}
}

// countInvalid records an unparseable or mis-scoped reply that cannot be
// attributed to a scheduled request.
func (tracker *echoTracker) countInvalid() {
	tracker.mutex.Lock()
	tracker.invalid++
	tracker.mutex.Unlock()
}

func (tracker *echoTracker) outstandingCount() int {
	tracker.mutex.Lock()
	defer tracker.mutex.Unlock()
	return len(tracker.outstanding)
}

func (tracker *echoTracker) result(requests uint64) echoResult {
	tracker.mutex.Lock()
	defer tracker.mutex.Unlock()
	return echoResult{
		Scope:                 echoSenderScope,
		Deadline:              tracker.deadline,
		OutstandingLimit:      tracker.limit,
		Requests:              requests,
		Validated:             tracker.validated,
		Capped:                tracker.capped,
		DeadlineExceeded:      tracker.deadlined,
		Invalid:               tracker.invalid,
		OutstandingAfterDrain: uint64(len(tracker.outstanding)),
		RTT:                   tracker.rtt.percentiles(),
	}
}

// echoRegistry routes inbound replies to the cohort tracker that owns their
// identity. Trackers stay registered after their cohort drains so a late
// reply is still attributed and counted as a failure of its own cohort
// instead of disappearing.
type echoRegistry struct {
	mutex    sync.Mutex
	trackers map[[16]byte]*echoTracker
	newest   *echoTracker
	fatal    string
}

func newEchoRegistry() *echoRegistry {
	return &echoRegistry{trackers: make(map[[16]byte]*echoTracker)}
}

func (registry *echoRegistry) register(tracker *echoTracker) {
	registry.mutex.Lock()
	registry.trackers[tracker.cohortHash] = tracker
	registry.newest = tracker
	registry.mutex.Unlock()
}

func (registry *echoRegistry) trackerFor(hash [16]byte) *echoTracker {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	return registry.trackers[hash]
}

// unattributed counts a reply that matches no registered cohort on the most
// recent tracker; a correct peer never produces one, so it is always a
// failure of the active measurement.
func (registry *echoRegistry) unattributed() {
	registry.mutex.Lock()
	tracker := registry.newest
	registry.mutex.Unlock()
	if tracker != nil {
		tracker.countInvalid()
	}
}

func (registry *echoRegistry) setFatal(reason string) {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	if registry.fatal == "" {
		registry.fatal = reason
	}
}

func (registry *echoRegistry) fatalError() string {
	registry.mutex.Lock()
	defer registry.mutex.Unlock()
	return registry.fatal
}

// echoReplyJob is one validated echo request awaiting its reply. The payload
// is built by the writer, not at enqueue, so a full queue costs only the
// identity and size per entry.
type echoReplyJob struct {
	identity messageIdentity
	size     int
}

// offerEchoReply hands a reply job to the writer without ever blocking the
// read loop. A full queue drops the job and counts the drop; the matching
// request then dies on its deadline at the sender, which is the honest
// accounting for a reply the association could not carry.
func offerEchoReply(queue chan<- echoReplyJob, job echoReplyJob, control *receiverControl) {
	select {
	case queue <- job:
	default:
		control.recordEchoReplyDropped()
	}
}

// startEchoReplyWriter runs one dedicated reply writer per association fed
// by a bounded queue, so reply-write backpressure can never stall the read
// loop or starve the control endpoint. The writer drains the queue when it
// is closed at read-loop exit.
func startEchoReplyWriter(association *m3ua.Association, control *receiverControl, capacity int) chan<- echoReplyJob {
	queue := make(chan echoReplyJob, max(capacity, 1))
	go runEchoReplyWriter(queue, control, newAssociationReplyWriter(association))
	return queue
}

// runEchoReplyWriter writes queued replies until the queue is closed and
// empty. Replies for a cohort that is no longer measuring are dropped and
// counted; write failures are counted through recordEchoReply.
func runEchoReplyWriter(queue <-chan echoReplyJob, control *receiverControl, write func(job echoReplyJob, deadline time.Time, generation uint64) error) {
	for job := range queue {
		active, generation, deadline := control.echoReplyContext()
		if !active {
			control.recordEchoReplyDropped()
			continue
		}
		control.recordEchoReply(write(job, deadline, generation))
	}
}

// newAssociationReplyWriter builds the per-association write closure. The
// SCTP write deadline is refreshed once per cohort generation, not per
// message; a write that outlives it fails and is counted, so a wedged peer
// costs one blocked writer goroutine, never the read loop or the process.
func newAssociationReplyWriter(association *m3ua.Association) func(job echoReplyJob, deadline time.Time, generation uint64) error {
	deadlineGeneration := uint64(0)
	return func(job echoReplyJob, deadline time.Time, generation uint64) error {
		if generation != deadlineGeneration {
			if err := association.SetWriteDeadline(deadline); err != nil {
				return err
			}
			deadlineGeneration = generation
		}
		return writeEchoReply(association, job.identity, job.size)
	}
}
