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
	fatal                string
	cpuStatPath          string
	cpuBefore            map[string]uint64
	cpuAfter             map[string]uint64
	cpuError             string
	allocBefore          runtimeCounters
	allocAfter           runtimeCounters
	series               []seriesPoint
	generation           uint64
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
	expected, expectedErr := scheduledMessages(specification.Rate, specification.Duration)
	if specification.Cohort == "" || specification.Associations != control.expectedAssociations ||
		specification.Expected == 0 || specification.Duration <= 0 || specification.Duration > maxRunWindow ||
		specification.Payload.size(0) == 0 || specification.Rate > maxOfferedRate ||
		specification.Drain < 0 || specification.Drain > maxRunWindow ||
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
	control.spec = specification
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
	defer control.mutex.Unlock()
	if control.phase != receiverArmed {
		return errors.New("receiver is not armed")
	}
	control.started = control.now()
	control.allocBefore = readRuntimeCounters()
	var err error
	control.cpuBefore, err = readCPUStat(control.cpuStatPath)
	if err != nil {
		control.cpuError = err.Error()
	}
	control.phase = receiverMeasuring
	return nil
}

func (control *receiverControl) stop() error {
	control.mutex.Lock()
	defer control.mutex.Unlock()
	if control.phase != receiverMeasuring {
		return errors.New("receiver is not measuring")
	}
	control.stopped = control.now()
	var err error
	control.cpuAfter, err = readCPUStat(control.cpuStatPath)
	if err != nil && control.cpuError == "" {
		control.cpuError = err.Error()
	}
	control.allocAfter = readRuntimeCounters()
	control.phase = receiverStopped
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

func (control *receiverControl) record(transportIndex int, message receivedMessage) (messageIdentity, recordOutcome) {
	control.mutex.Lock()
	if control.phase == receiverStopped {
		control.lateAfterStop++
		control.mutex.Unlock()
		return messageIdentity{}, recordIgnored
	}
	if control.phase != receiverMeasuring || control.ledger == nil {
		control.mutex.Unlock()
		return messageIdentity{}, recordIgnored
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
	arrival := control.now()
	control.mutex.Lock()
	defer control.mutex.Unlock()
	if control.phase != receiverMeasuring || control.generation != generation {
		if control.ledger != nil {
			control.ledger.snapshotData.Invalid++
		}
		return messageIdentity{}, recordIgnored
	}
	if err != nil || !control.bindAssociation(transportIndex, int(identity.Association)) {
		control.ledger.snapshotData.Invalid++
		return identity, recordInvalid
	}
	identity.Cohort = specification.Cohort
	if control.firstArrival.IsZero() {
		control.firstArrival = arrival
	}
	status := control.ledger.record(identity)
	if status != ledgerUnique {
		return identity, recordNotUnique
	}
	if arrival.Before(control.firstArrival.Add(control.spec.Duration)) {
		control.uniqueMeasurement++
	} else {
		control.uniqueDrain++
	}
	return identity, recordUnique
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

// echoReplyDeadline bounds echo reply writes. Replies get the full cohort
// window plus drain plus one extra request deadline of slack so a reply for
// a late-arriving request is not cut off before the sender stops accepting
// it.
func (control *receiverControl) echoReplyDeadline() time.Time {
	control.mutex.Lock()
	defer control.mutex.Unlock()
	return control.started.Add(control.spec.Duration + control.spec.Drain + echoRequestDeadline)
}

func (control *receiverControl) recordEchoReply(err error) {
	control.mutex.Lock()
	defer control.mutex.Unlock()
	if err != nil {
		control.echoReplyErrors++
		return
	}
	control.echoReplies++
}

func (control *receiverControl) result() runRecord {
	control.mutex.Lock()
	defer control.mutex.Unlock()
	record := runRecord{
		Side:                "receiver",
		Spec:                control.spec,
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
			Scope:       echoReceiverScope,
			Replies:     control.echoReplies,
			ReplyErrors: control.echoReplyErrors,
		}
	}
	if !control.started.IsZero() && !control.stopped.IsZero() {
		measurementEnd := control.firstArrival.Add(control.spec.Duration)
		if control.firstArrival.IsZero() {
			measurementEnd = control.started.Add(control.spec.Duration)
		}
		if control.stopped.After(measurementEnd) {
			record.DrainDuration = control.stopped.Sub(measurementEnd)
		}
	}
	record.CPU = newCPUObservation(record.CPU.Before, record.CPU.After, errorFromString(record.CPU.Error), nil, record.Delivery.Unique)
	record.Allocations.Delta = runtimeDelta(record.Allocations.Before, record.Allocations.After)
	if record.MeasurementDuration > 0 {
		record.ValidatedPerSecond = float64(record.Delivery.UniqueMeasurement) / record.MeasurementDuration.Seconds()
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
	return mux
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}
