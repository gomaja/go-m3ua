package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/gomaja/go-m3ua"
)

type receiverPhase string

const (
	receiverIdle      receiverPhase = "idle"
	receiverArmed     receiverPhase = "armed"
	receiverMeasuring receiverPhase = "measuring"
	receiverStopped   receiverPhase = "stopped"
)

var errInvalidRunSpec = errors.New("invalid run specification")

type readyResult struct {
	Ready                     bool   `json:"ready"`
	Phase                     string `json:"phase"`
	Associations              int    `json:"associations"`
	ExpectedAssociations      int    `json:"expected_associations"`
	NegotiatedOutboundStreams int    `json:"negotiated_outbound_streams,omitempty"`
	Error                     string `json:"error,omitempty"`
}

type receiverControl struct {
	mutex                sync.Mutex
	clock                measurementClock
	lastClock            int64
	stoppedClock         int64
	measurementLower     uint64
	measurementUpper     uint64
	clockEvidence        *sharedClockEvidence
	now                  func() time.Time
	expectedAssociations int
	ledgerWindow         int
	readyAssociations    int
	minimumMaxStreamID   int
	phase                receiverPhase
	spec                 runSpec
	ledger               *receiveLedger
	transportToLogical   []int
	logicalToTransport   []int
	started              time.Time
	firstArrival         time.Time
	stopped              time.Time
	uniqueMeasurement    uint64
	uniqueDrain          uint64
	lateAfterStop        uint64
	echoReplies          uint64
	echoReplyErrors      uint64
	echoRepliesDropped   uint64
	fatal                string
	cpuStatPath          string
	cpuBefore            map[string]uint64
	cpuAfter             map[string]uint64
	cpuError             string
	allocBefore          runtimeCounters
	allocAfter           runtimeCounters
	series               []seriesPoint
	generation           uint64
	driver               *reverseDriver
	// reverseControl is the only control endpoint this receiver may drive a
	// reverse cohort against, taken from this process's own configuration.
	// The control listener is unauthenticated and binds every interface, so
	// the destination must never be taken from a request body.
	reverseControl  string
	reverseSender   *runRecord
	reverseReceiver *runRecord
	reverseError    string
	// ssnm is the SGP's SSNM load generator, nil without SSNM load.
	ssnm *ssnmGenerator
}

// reverseDriver lets the bidirectional SGP run the reverse (SGP-to-ASP)
// cohort against the ASP's own control endpoint. The reverse cohort reuses
// the exact sender-side measurement path; only the direction, cohort name
// and peer differ.
type reverseDriver struct {
	ctx          context.Context
	associations []*m3ua.Association
	cpuStatPath  string
}

func (driver *reverseDriver) addAssociation(association *m3ua.Association) {
	driver.associations = append(driver.associations, association)
}

// registerReverseAssociation tracks an association for the bidirectional
// reverse cohort. A control without a reverse driver — throughput and echo
// receivers, the ASP's local control, and test-constructed controls — has no
// reverse cohort to feed, so registration is a deliberate no-op there rather
// than a nil dereference.
//
// Registration takes the control mutex and must run BEFORE
// setAssociationReady: a /reset that observes full readiness under that
// mutex can lead to /start spawning the reverse cohort, which reads
// driver.associations. Registering first makes the mutex unlock in
// setAssociationReady publish the append to every later reader.
func (control *receiverControl) registerReverseAssociation(association *m3ua.Association) {
	control.mutex.Lock()
	defer control.mutex.Unlock()
	control.ssnm.addAssociation(association)
	if control.driver == nil {
		return
	}
	control.driver.addAssociation(association)
}

func newReceiverControl(expectedAssociations, window int) *receiverControl {
	return &receiverControl{
		now:                  time.Now,
		expectedAssociations: expectedAssociations,
		ledgerWindow:         window,
		minimumMaxStreamID:   -1,
		phase:                receiverIdle,
		transportToLogical:   filledInts(expectedAssociations, -1),
		logicalToTransport:   filledInts(expectedAssociations, -1),
	}
}

func filledInts(length, value int) []int {
	result := make([]int, length)
	for index := range result {
		result[index] = value
	}
	return result
}

func (control *receiverControl) setAssociationReady(index, maxMessageStreamID int) {
	control.mutex.Lock()
	defer control.mutex.Unlock()
	if index >= control.expectedAssociations {
		control.fatal = fmt.Sprintf("association index %d exceeds configured count", index)
		return
	}
	control.readyAssociations++
	if control.minimumMaxStreamID < 0 || maxMessageStreamID < control.minimumMaxStreamID {
		control.minimumMaxStreamID = maxMessageStreamID
	}
}

func (control *receiverControl) setFatal(reason string) {
	control.mutex.Lock()
	defer control.mutex.Unlock()
	if control.fatal == "" {
		control.fatal = reason
	}
}

func (control *receiverControl) ready() readyResult {
	control.mutex.Lock()
	defer control.mutex.Unlock()
	streams := 0
	if control.minimumMaxStreamID >= 0 {
		streams = control.minimumMaxStreamID + 1
	}
	return readyResult{
		Ready:                     control.fatal == "" && control.readyAssociations == control.expectedAssociations,
		Phase:                     string(control.phase),
		Associations:              control.readyAssociations,
		ExpectedAssociations:      control.expectedAssociations,
		NegotiatedOutboundStreams: streams,
		Error:                     control.fatal,
	}
}

func (control *receiverControl) reset(specification runSpec) error {
	control.mutex.Lock()
	defer control.mutex.Unlock()
	if specification.Associations < 1 || specification.Associations > maxAssociations {
		return fmt.Errorf("%w: associations must be between 1 and %d", errInvalidRunSpec, maxAssociations)
	}
	if control.phase == receiverMeasuring {
		return errors.New("cannot reset an active measurement")
	}
	if control.fatal != "" {
		return errors.New(control.fatal)
	}
	if control.readyAssociations != control.expectedAssociations {
		return errors.New("not all associations are ready")
	}
	if (specification.Clock != nil) != (control.clock != nil) {
		return fmt.Errorf("%w: both peers must explicitly enable the same-host clock", errInvalidRunSpec)
	}
	if specification.Clock != nil {
		domain, err := control.clock.Domain()
		now, readErr := control.clock.Now()
		if err != nil || readErr != nil || !specification.Clock.valid(specification.Duration) || !specification.Clock.validDrain(specification.Drain) || specification.Clock.Domain != domain || now <= 0 || now >= specification.Clock.Start-domain.Resolution || specification.Clock.Start-now > int64(sharedClockLead) || specification.Mode == modeEcho {
			return fmt.Errorf("%w: shared clock domain or future window mismatch", errInvalidRunSpec)
		}
	}
	expected, expectedErr := scheduledMessages(specification.Rate, specification.Duration)
	if specification.Cohort == "" || specification.Associations != control.expectedAssociations ||
		specification.Expected == 0 || specification.Duration <= 0 || specification.Duration > maxRunWindow ||
		specification.Payload.size(0) == 0 || specification.Rate > maxOfferedRate ||
		specification.Drain < 0 || specification.Drain > maxRunWindow ||
		specification.Outstanding < 1 || specification.Outstanding > maxOutstanding ||
		expectedErr != nil || expected != specification.Expected {
		return errInvalidRunSpec
	}
	switch specification.Mode {
	case "", modeThroughput, modeEcho, modeBidirectional:
	default:
		return errInvalidRunSpec
	}
	switch specification.Direction {
	case "", directionASPToSGP, directionSGPToASP:
	default:
		return errInvalidRunSpec
	}
	switch specification.Initiation {
	case "", initiationASPDial, initiationSGPDial:
	default:
		return errInvalidRunSpec
	}
	if specification.Mode == modeBidirectional {
		// /start turns an armed bidirectional cohort into outbound HTTP
		// requests, so the destination is pinned to this process's own
		// configuration. A specification may only name that destination; it
		// can never introduce another one.
		if specification.PeerControl == "" {
			return fmt.Errorf("%w: bidirectional runs require the peer control URL", errInvalidRunSpec)
		}
		if control.reverseControl == "" {
			return fmt.Errorf("%w: this receiver has no configured reverse control destination", errInvalidRunSpec)
		}
		if specification.PeerControl != control.reverseControl {
			return fmt.Errorf("%w: the peer control URL is not this receiver's configured reverse control destination", errInvalidRunSpec)
		}
	}
	if err := control.ssnm.acceptSpec(specification); err != nil {
		return fmt.Errorf("%w: %v", errInvalidRunSpec, err)
	}
	control.spec = copyRunSpec(specification)
	control.lastClock = 0
	control.stoppedClock = 0
	control.measurementLower = 0
	control.measurementUpper = 0
	control.clockEvidence = nil
	control.ledger = newLedger(specification.Associations, specification.Expected, control.ledgerWindow)
	control.transportToLogical = filledInts(specification.Associations, -1)
	control.logicalToTransport = filledInts(specification.Associations, -1)
	control.started = time.Time{}
	control.firstArrival = time.Time{}
	control.stopped = time.Time{}
	control.uniqueMeasurement = 0
	control.uniqueDrain = 0
	control.lateAfterStop = 0
	control.echoReplies = 0
	control.echoReplyErrors = 0
	control.echoRepliesDropped = 0
	control.reverseSender = nil
	control.reverseReceiver = nil
	control.reverseError = ""
	control.cpuBefore = nil
	control.cpuAfter = nil
	control.cpuError = ""
	control.series = nil
	control.generation++
	control.phase = receiverArmed
	return nil
}

func (control *receiverControl) start() error {
	control.mutex.Lock()
	if control.phase != receiverArmed {
		control.mutex.Unlock()
		return errors.New("receiver is not armed")
	}
	control.started = control.now()
	if control.spec.Clock != nil {
		domain, err := control.clock.Domain()
		now, readErr := control.sharedNowLocked()
		if err != nil || readErr != nil || domain != control.spec.Clock.Domain || now >= control.spec.Clock.Start-domain.Resolution {
			control.mutex.Unlock()
			return errors.New("shared clock changed or measurement window already started")
		}
		control.clockEvidence = &sharedClockEvidence{Before: domain}
	}
	control.allocBefore = readRuntimeCounters()
	var err error
	control.cpuBefore, err = readCPUStat(control.cpuStatPath)
	if err != nil {
		control.cpuError = err.Error()
	}
	control.phase = receiverMeasuring
	specification := control.spec
	driver := control.driver
	// Everything the reverse cohort is pinned to is captured here, under the
	// mutex that commits the cohort.
	run := reverseRun{specification: specification, reverseControl: control.reverseControl, generation: control.generation}
	control.mutex.Unlock()
	if specification.SSNM.enabled() {
		control.ssnm.begin()
	}
	if specification.Mode == modeBidirectional && driver != nil {
		go control.runReverseCohort(driver, run)
	}
	return nil
}

// reverseRun is the reverse cohort as start committed it: the specification,
// the configured control destination, and the cohort generation the run
// belongs to. The generation is captured at start rather than read at
// completion, because the run outlives the call that launched it.
type reverseRun struct {
	specification  runSpec
	reverseControl string
	generation     uint64
}

// runReverseCohort drives the SGP-to-ASP direction of a bidirectional cohort
// against the configured reverse control endpoint with the same sender-side
// measurement path as the forward direction. Its records are reported under
// reverse and reverse_receiver in this receiver's results; its errors never
// replace the forward records.
func (control *receiverControl) runReverseCohort(driver *reverseDriver, run reverseRun) {
	specification := run.specification
	reverseConfig := commandConfig{
		Mode:        modeThroughput,
		Direction:   directionSGPToASP,
		Initiation:  specification.Initiation,
		Rate:        specification.Rate,
		Workload:    specification.Payload,
		Seed:        specification.Seed,
		Outstanding: specification.Outstanding,
		Drain:       specification.Drain,
		// The destination is the configured one, never the one the run
		// specification carries: reset accepts only a specification naming
		// it, and taking it from configuration here means a specification can
		// never redirect this request even if it reached the cohort by some
		// other path.
		PeerControl:   run.reverseControl,
		CPUStatPath:   driver.cpuStatPath,
		SameHostClock: specification.Clock != nil,
		clockWindow:   specification.Clock,
	}
	sender, receiver, err := runSenderCohort(driver.ctx, reverseConfig, driver.associations, nil, specification.Cohort+"-reverse", specification.Duration)
	control.mutex.Lock()
	defer control.mutex.Unlock()
	// A reverse cohort outlives the start that launched it. If a reset has
	// advanced the generation meanwhile, the cohort these records describe no
	// longer exists — the reset cleared its reverse fields — and the records
	// belong to no live cohort rather than to the one that replaced it.
	if control.generation != run.generation {
		return
	}
	control.reverseSender = &sender
	control.reverseReceiver = &receiver
	if err != nil {
		control.reverseError = err.Error()
	}
}

func (control *receiverControl) stop() error {
	control.mutex.Lock()
	defer control.mutex.Unlock()
	if control.phase != receiverMeasuring {
		return errors.New("receiver is not measuring")
	}
	control.stopped = control.now()
	if control.spec.Clock != nil {
		var clockErr error
		control.stoppedClock, clockErr = control.sharedNowLocked()
		if clockErr != nil {
			control.phase = receiverStopped
			return clockErr
		}
	}
	var err error
	control.cpuAfter, err = readCPUStat(control.cpuStatPath)
	if err != nil && control.cpuError == "" {
		control.cpuError = err.Error()
	}
	control.allocAfter = readRuntimeCounters()
	control.phase = receiverStopped
	if control.spec.Clock != nil {
		domain, domainErr := control.clock.Domain()
		if control.clockEvidence == nil {
			control.clockEvidence = &sharedClockEvidence{}
		}
		control.clockEvidence.After = domain
		control.clockEvidence.Verified = domainErr == nil && domain == control.spec.Clock.Domain && domain == control.clockEvidence.Before
		if !control.clockEvidence.Verified {
			control.fatal = "shared clock domain changed during measurement"
			return errors.New(control.fatal)
		}
	}
	return nil
}

// recordOutcome classifies one arrival so the echo reply path can distinguish
// a newly validated delivery from ignored, duplicate or invalid traffic.
type recordOutcome uint8

const (
	recordIgnored recordOutcome = iota
	recordInvalid
	recordNotUnique
	recordUnique
)

// arrival is one classified inbound message together with the cohort
// generation it was committed under. The two always travel together, so work
// scheduled from an arrival — the echo reply — cannot be separated from its
// cohort: a reset in between can then neither credit nor charge it to the next
// cohort.
type arrival struct {
	identity   messageIdentity
	generation uint64
}

// replyJob builds the echo reply job for this arrival, carrying the cohort
// generation through to the reply writer.
func (received arrival) replyJob(size int) echoReplyJob {
	return echoReplyJob{identity: received.identity, size: size, generation: received.generation}
}

// record classifies one arrival and reports the cohort generation it was
// committed under.
func (control *receiverControl) record(transportIndex int, message receivedMessage) (arrival, recordOutcome) {
	control.mutex.Lock()
	if control.phase == receiverStopped {
		control.lateAfterStop++
		generation := control.generation
		control.mutex.Unlock()
		return arrival{generation: generation}, recordIgnored
	}
	if control.phase != receiverMeasuring || control.ledger == nil {
		generation := control.generation
		control.mutex.Unlock()
		return arrival{generation: generation}, recordIgnored
	}
	specification := control.spec
	generation := control.generation
	control.mutex.Unlock()

	expectedKind := kindData
	if specification.Mode == modeEcho {
		expectedKind = kindEchoRequest
	}
	identity, err := validateMessage(message, specification.Cohort, specification.Seed,
		specification.Associations, specification.Payload, expectedKind, specification.Direction == directionSGPToASP)
	var received time.Time
	if specification.Clock == nil {
		received = control.now()
	}
	control.mutex.Lock()
	defer control.mutex.Unlock()
	if control.phase != receiverMeasuring || control.generation != generation {
		if control.ledger != nil {
			control.ledger.snapshotData.Invalid++
		}
		return arrival{generation: control.generation}, recordIgnored
	}
	if err != nil || !control.bindAssociation(transportIndex, int(identity.Association)) {
		control.ledger.snapshotData.Invalid++
		return arrival{identity: identity, generation: generation}, recordInvalid
	}
	identity.Cohort = specification.Cohort
	if control.firstArrival.IsZero() {
		control.firstArrival = received
	}
	status := control.ledger.record(identity)
	if status != ledgerUnique {
		return arrival{identity: identity, generation: generation}, recordNotUnique
	}
	if control.spec.Clock != nil {
		sharedReceived, clockErr := control.sharedNowLocked()
		if clockErr != nil || sharedReceived > control.spec.Clock.End+int64(control.spec.Drain)-control.spec.Clock.Domain.Resolution {
			control.ledger.snapshotData.Unique--
			control.ledger.snapshotData.Invalid++
			if clockErr == nil {
				control.fatal = "delivery exceeds shared drain deadline"
			}
			return arrival{identity: identity, generation: generation}, recordInvalid
		}
		control.classifySharedDeliveryLocked(sharedReceived)
		return arrival{identity: identity, generation: generation}, recordUnique
	}
	if received.Before(control.firstArrival.Add(control.spec.Duration)) {
		control.uniqueMeasurement++
	} else {
		control.uniqueDrain++
	}
	return arrival{identity: identity, generation: generation}, recordUnique
}

func (control *receiverControl) bindAssociation(transportIndex, logicalIndex int) bool {
	if transportIndex < 0 || transportIndex >= len(control.transportToLogical) ||
		logicalIndex < 0 || logicalIndex >= len(control.logicalToTransport) {
		return false
	}
	boundLogical := control.transportToLogical[transportIndex]
	boundTransport := control.logicalToTransport[logicalIndex]
	if boundLogical == -1 && boundTransport == -1 {
		control.transportToLogical[transportIndex] = logicalIndex
		control.logicalToTransport[logicalIndex] = transportIndex
		return true
	}
	return boundLogical == logicalIndex && boundTransport == transportIndex
}

// echoMode reports whether the active cohort expects echo requests.
func (control *receiverControl) echoMode() bool {
	control.mutex.Lock()
	defer control.mutex.Unlock()
	return control.phase == receiverMeasuring && control.spec.Mode == modeEcho
}

// recordEchoReply counts one reply write against the cohort that enqueued it.
// A write can begin before a reset and finish after it, so the generation is
// checked under the same mutex that advances it: the previous cohort's
// counters are already cleared, and the new cohort must not inherit the
// outcome of work it never scheduled.
func (control *receiverControl) recordEchoReply(generation uint64, err error) {
	control.mutex.Lock()
	defer control.mutex.Unlock()
	if control.generation != generation {
		return
	}
	if err != nil {
		control.echoReplyErrors++
		return
	}
	control.echoReplies++
}

// recordEchoReplyDropped counts a validated request whose reply was never
// written — queue full or the cohort ended first. Drops are losses and are
// always counted for their own cohort, never silent; a drop belonging to a
// cohort that has already been reset away is charged to no cohort at all.
func (control *receiverControl) recordEchoReplyDropped(generation uint64) {
	control.mutex.Lock()
	defer control.mutex.Unlock()
	if control.generation != generation {
		return
	}
	control.echoRepliesDropped++
}

// currentGeneration reports the active cohort generation under the mutex that
// advances it.
func (control *receiverControl) currentGeneration() uint64 {
	control.mutex.Lock()
	defer control.mutex.Unlock()
	return control.generation
}

// echoCounts returns the reply counters atomically. The reply writer
// goroutine mutates them under the same mutex, so every reader — result
// collection, tests, diagnostics — must take it too.
func (control *receiverControl) echoCounts() (replies, replyErrors, dropped uint64) {
	control.mutex.Lock()
	defer control.mutex.Unlock()
	return control.echoReplies, control.echoReplyErrors, control.echoRepliesDropped
}

// echoReplyContext reports, atomically, whether the active cohort accepts
// echo replies, its generation, and the reply write deadline. The reply
// writer uses the generation to refresh the association write deadline once
// per cohort rather than per message.
func (control *receiverControl) echoReplyContext() (active bool, generation uint64, deadline time.Time) {
	control.mutex.Lock()
	defer control.mutex.Unlock()
	return control.phase == receiverMeasuring && control.spec.Mode == modeEcho,
		control.generation,
		control.started.Add(control.spec.Duration + control.spec.Drain + echoRequestDeadline)
}

func (control *receiverControl) result() runRecord {
	control.mutex.Lock()
	defer control.mutex.Unlock()
	record := runRecord{
		Side:                "receiver",
		Spec:                copyRunSpec(control.spec),
		Expected:            control.spec.Expected,
		FatalError:          control.fatal,
		AcceptanceScope:     baselineFixtureScope,
		IndependentPeer:     false,
		BacklogAssessment:   "unavailable: fixture tooling does not yet evaluate the paired end-to-end series",
		MeasurementDuration: control.spec.Duration,
		CPU:                 CPUObservation{Scope: wholeProcessScope, Before: control.cpuBefore, After: control.cpuAfter, Error: control.cpuError},
		Allocations:         AllocationObservation{Scope: wholeProcessScope, Before: control.allocBefore, After: control.allocAfter},
		Series:              append([]seriesPoint(nil), control.series...),
	}
	if control.clockEvidence != nil {
		evidence := *control.clockEvidence
		record.ClockEvidence = &evidence
	}
	if control.spec.Clock != nil {
		record.WindowAlignment = "verified same-host CLOCK_MONOTONIC window; boundary counts retain clock-resolution uncertainty"
		record.ClockBoundary = &sharedClockSnapshot{Domain: control.spec.Clock.Domain, Captured: control.lastClock, MeasurementLower: control.measurementLower, MeasurementUpper: control.measurementUpper}
	}
	if control.ledger != nil {
		snapshot := control.ledger.snapshot()
		record.Delivery = deliveryResult{
			Unique:            snapshot.Unique,
			UniqueMeasurement: control.uniqueMeasurement,
			UniqueDrain:       control.uniqueDrain,
			Missing:           snapshot.Missing,
			Duplicate:         snapshot.Duplicate,
			Invalid:           snapshot.Invalid,
			Reordered:         snapshot.Reordered,
			LateAfterStop:     control.lateAfterStop,
		}
	}
	if control.spec.Mode == modeEcho {
		record.ReceiverEcho = &receiverEchoResult{
			Scope:          echoReceiverScope,
			Replies:        control.echoReplies,
			ReplyErrors:    control.echoReplyErrors,
			RepliesDropped: control.echoRepliesDropped,
		}
	}
	record.Reverse = control.reverseSender
	record.ReverseReceiver = control.reverseReceiver
	record.ReverseError = control.reverseError
	if control.spec.Clock != nil {
		record.DrainDuration = time.Duration(max(control.stoppedClock-control.spec.Clock.End, 0))
	} else if !control.started.IsZero() && !control.stopped.IsZero() {
		measurementEnd := control.firstArrival.Add(control.spec.Duration)
		if control.firstArrival.IsZero() {
			measurementEnd = control.started.Add(control.spec.Duration)
		}
		if control.stopped.After(measurementEnd) {
			record.DrainDuration = control.stopped.Sub(measurementEnd)
		}
	}
	if generator := control.ssnm.cohortRecord(control.spec); generator != nil {
		record.SSNM = &ssnmRecord{Generator: generator}
		record.UnsupportedModes = ssnmUnsupportedModes()
	}
	record.CPU = newCPUObservation(record.CPU.Before, record.CPU.After, errorFromString(record.CPU.Error), nil, record.Delivery.Unique)
	record.Allocations.Delta = runtimeDelta(record.Allocations.Before, record.Allocations.After)
	if record.MeasurementDuration > 0 {
		record.ValidatedPerSecond = float64(record.Delivery.UniqueMeasurement) / record.MeasurementDuration.Seconds()
		if control.spec.Clock != nil {
			record.ValidatedPerSecond = float64(control.measurementLower) / record.MeasurementDuration.Seconds()
		}
	}
	record.evaluate()
	return record
}

func errorFromString(message string) error {
	if message == "" {
		return nil
	}
	return errors.New(message)
}

func (control *receiverControl) sample(now time.Time) {
	control.mutex.Lock()
	defer control.mutex.Unlock()
	if control.phase != receiverMeasuring || control.ledger == nil || len(control.series) >= 601 {
		return
	}
	snapshot := control.ledger.snapshot()
	origin := control.firstArrival
	if origin.IsZero() {
		origin = control.started
	}
	offset := time.Duration(0)
	if now.After(origin) {
		offset = now.Sub(origin)
	}
	if control.spec.Clock != nil {
		sharedNow, err := control.sharedNowLocked()
		if err != nil || sharedNow < control.spec.Clock.Start {
			return
		}
		offset = time.Duration(sharedNow - control.spec.Clock.Start)
	}
	scheduled := scheduledAt(control.spec.Rate, offset, control.spec.Duration, control.spec.Expected)
	missing := uint64(0)
	if snapshot.Unique < scheduled {
		missing = scheduled - snapshot.Unique
	}
	control.series = append(control.series, seriesPoint{
		OffsetMillis: uint64(offset / time.Millisecond),
		Scheduled:    scheduled,
		Unique:       snapshot.Unique,
		Missing:      missing,
		Duplicate:    snapshot.Duplicate,
		Invalid:      snapshot.Invalid,
		Outstanding:  outstandingAt(control.spec.Rate, offset, control.spec.Duration, snapshot.Unique),
	})
}

func scheduledAt(rate uint64, elapsed, duration time.Duration, expected uint64) uint64 {
	if elapsed <= 0 {
		return 0
	}
	if elapsed > duration {
		elapsed = duration
	}
	scheduled, err := scheduledMessages(rate, elapsed)
	if err != nil || scheduled > expected {
		return expected
	}
	return scheduled
}

func outstandingAt(rate uint64, elapsed, duration time.Duration, unique uint64) uint64 {
	scheduled := scheduledAt(rate, elapsed, duration, ^uint64(0))
	if unique >= scheduled {
		return 0
	}
	return scheduled - unique
}

func sampleReceiver(ctx context.Context, control *receiverControl) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			control.sample(now)
		case <-ctx.Done():
			return
		}
	}
}

func (control *receiverControl) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /clock", func(writer http.ResponseWriter, _ *http.Request) {
		snapshot, err := control.clockSnapshot()
		if err != nil {
			http.Error(writer, err.Error(), http.StatusConflict)
			return
		}
		writeJSON(writer, http.StatusOK, snapshot)
	})
	mux.HandleFunc("GET /progress", func(writer http.ResponseWriter, _ *http.Request) {
		writeJSON(writer, http.StatusOK, control.progress())
	})
	mux.HandleFunc("GET /ready", func(writer http.ResponseWriter, _ *http.Request) {
		writeJSON(writer, http.StatusOK, control.ready())
	})
	mux.HandleFunc("POST /reset", func(writer http.ResponseWriter, request *http.Request) {
		var specification runSpec
		decoder := json.NewDecoder(io.LimitReader(request.Body, 64<<10))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&specification); err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
		if err := control.reset(specification); err != nil {
			status := http.StatusConflict
			if errors.Is(err, errInvalidRunSpec) {
				status = http.StatusBadRequest
			}
			http.Error(writer, err.Error(), status)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /start", func(writer http.ResponseWriter, _ *http.Request) {
		if err := control.start(); err != nil {
			http.Error(writer, err.Error(), http.StatusConflict)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("POST /stop", func(writer http.ResponseWriter, _ *http.Request) {
		if err := control.stop(); err != nil {
			http.Error(writer, err.Error(), http.StatusConflict)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /results", func(writer http.ResponseWriter, _ *http.Request) {
		writeJSON(writer, http.StatusOK, control.result())
	})
	control.ssnm.register(mux)
	return mux
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}
