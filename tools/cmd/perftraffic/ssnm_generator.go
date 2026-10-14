package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/gomaja/go-m3ua"
	"github.com/gomaja/go-m3ua/messages"
	"github.com/gomaja/go-m3ua/messages/params"
)

// ssnmSignalWriter is the SGP Association operation the generator drives:
// one message on one association.
type ssnmSignalWriter interface {
	WriteSignal(message messages.M3UA) (int, error)
}

const (
	ssnmGeneratorIdle     = "idle"
	ssnmGeneratorRunning  = "running"
	ssnmGeneratorComplete = "complete"
	ssnmGeneratorStopped  = "stopped"

	ssnmMessageOK     = uint8(1)
	ssnmMessageFailed = uint8(2)

	// ssnmMaxReportsRange bounds one /ssnm/reports response.
	ssnmMaxReportsRange = 2_000_000
	// ssnmGeneratorPollCeiling bounds one generator sleep so a newly declared
	// end is noticed promptly.
	ssnmGeneratorPollCeiling = 10 * time.Millisecond
	// ssnmWriteWait bounds how long one message waits for SCTP send-buffer
	// space: the bound the library applies to the writes it makes on its own
	// behalf (m3ua.DefaultControlWriteTimeout). WriteSignal is an application
	// write, which reports a full send buffer at once instead of waiting.
	ssnmWriteWait = m3ua.DefaultControlWriteTimeout
	// ssnmWriteBackoffCeiling bounds one wait between send-buffer retries.
	ssnmWriteBackoffCeiling = time.Millisecond
)

// ssnmGenerator is the SGP's open-loop SSNM source. It reports generator
// message m at anchor + floor(m * 1s / total rate) on the shared clock, on
// association m mod associations only, in order, catching up when it falls
// behind rather than skipping, from the first SSNM cohort's start through the
// measurement cohort's end. Every message's report start and completion are
// retained so the ASP can join them with subscriber receipts.
//
// A message is one RFC 4666 Section 3.4.1 DUNA or Section 3.4.2 DAVA written
// with Association.WriteSignal, carrying the fixture's Network Appearance,
// Routing Context and the message's Affected Point Codes: the same message
// Endpoint.ReportDestinationAvailability builds, but on one association
// instead of every concerned one. The generator is load, not a Signalling
// Gateway under test, so it keeps no SG-side destination record; the ASP
// never audits it.
type ssnmGenerator struct {
	ctx          context.Context
	rate         uint64
	apcs         int
	records      int
	associations int
	plan         ssnmPlan
	clock        measurementClock
	// pointCodes lists every generated destination once, association-major;
	// a message names a contiguous slice of it.
	pointCodes []uint32
	// sleep waits between schedule decisions and send-buffer retries. Tests
	// replace it to step an injected clock.
	sleep func(ctx context.Context, duration time.Duration)

	mutex   sync.Mutex
	targets []ssnmSignalWriter
	state   string
	reason  string
	preload *ssnmPreloadRecord
	// preloadNext is the next preload step the SGP accepts; preloadBusy is set
	// while one is being written.
	preloadNext    uint64
	preloadBusy    bool
	preloadStarted int64
	anchor         int64
	end            int64
	stopAt         int64
	started        bool
	done           chan struct{}
	// reports, completions and statuses are indexed by generator message.
	reports        []int64
	completions    []int64
	statuses       []uint8
	bufferWaits    uint64
	failedMessages uint64
	firstError     string
}

type ssnmPreloadRecord struct {
	Messages      uint64 `json:"messages"`
	Updates       uint64 `json:"updates"`
	Failed        uint64 `json:"failed"`
	DurationNS    int64  `json:"duration_ns"`
	Error         string `json:"error,omitempty"`
	CompletedAtNS int64  `json:"completed_at_ns"`
}

// newSSNMGenerator returns nil when the SSNM workload is off, so every
// generator hook is a no-op in the existing modes.
func newSSNMGenerator(ctx context.Context, config commandConfig, clock measurementClock) *ssnmGenerator {
	if !config.SSNM.enabled() {
		return nil
	}
	plan := newSSNMPlan(config.SSNM, config.Associations)
	pointCodes := make([]uint32, plan.associations*plan.records)
	for index := range pointCodes {
		pointCodes[index] = ssnmPointCodeBase + uint32(index)
	}
	return &ssnmGenerator{
		ctx:          ctx,
		rate:         config.SSNM.TotalRate,
		apcs:         config.SSNM.APCs,
		records:      config.SSNM.Records,
		associations: config.Associations,
		plan:         plan,
		clock:        clock,
		pointCodes:   pointCodes,
		sleep:        sleepContext,
		targets:      make([]ssnmSignalWriter, config.Associations),
		state:        ssnmGeneratorIdle,
		done:         make(chan struct{}),
	}
}

// sleepContext waits for duration or until ctx ends.
func sleepContext(ctx context.Context, duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

// track registers the SGP association serving one transport index, the
// association every message m with m mod associations == index goes to.
func (generator *ssnmGenerator) track(index int, writer ssnmSignalWriter) {
	if generator == nil {
		return
	}
	generator.mutex.Lock()
	defer generator.mutex.Unlock()
	if index >= 0 && index < len(generator.targets) {
		generator.targets[index] = writer
	}
}

// readyTargetsLocked returns every association's writer, or false while any
// is missing. The caller holds the mutex.
func (generator *ssnmGenerator) readyTargetsLocked() ([]ssnmSignalWriter, bool) {
	for _, target := range generator.targets {
		if target == nil {
			return nil, false
		}
	}
	return append([]ssnmSignalWriter(nil), generator.targets...), true
}

// acceptSpec checks one cohort's SSNM declaration against this process's own
// flags and pins the generator anchor and end. It runs under the receiver
// control mutex, before the cohort is committed.
func (generator *ssnmGenerator) acceptSpec(specification runSpec) error {
	workload := specification.SSNM
	if generator == nil {
		if workload.enabled() {
			return errors.New("SSNM load is declared but this receiver runs without -ssnm-total-rate")
		}
		return nil
	}
	if !workload.enabled() {
		return errors.New("this receiver runs SSNM load but the cohort declares none")
	}
	if workload.TotalRate != generator.rate || workload.APCs != generator.apcs || workload.Records != generator.records {
		return fmt.Errorf("SSNM workload %d/s in total x %d APCs over %d records per association does not match this receiver's %d/s x %d APCs over %d records",
			workload.TotalRate, workload.APCs, workload.Records, generator.rate, generator.apcs, generator.records)
	}
	if specification.Associations != generator.associations {
		return fmt.Errorf("SSNM load spreads over %d associations here but the cohort declares %d", generator.associations, specification.Associations)
	}
	if specification.Clock == nil {
		return errors.New("SSNM load requires a shared clock window")
	}
	if workload.Phase != ssnmPhaseWarmup && workload.Phase != ssnmPhaseMeasurement {
		return fmt.Errorf("SSNM phase %q is not warmup or measurement", workload.Phase)
	}
	generator.mutex.Lock()
	defer generator.mutex.Unlock()
	if generator.preload == nil || generator.preload.CompletedAtNS == 0 || generator.preload.Error != "" {
		return errors.New("SSNM store was not preloaded")
	}
	if generator.started {
		if workload.Anchor != generator.anchor {
			return errors.New("SSNM anchor differs from the running generator's")
		}
		if generator.end != 0 {
			return errors.New("SSNM generator already ran its measurement cohort")
		}
	} else if workload.Anchor != specification.Clock.Start {
		return errors.New("the first SSNM cohort must anchor the generator at its own shared start")
	}
	generator.anchor = workload.Anchor
	generator.stopAt = workload.Anchor + int64(ssnmGeneratorHorizon)
	if workload.Phase == ssnmPhaseMeasurement {
		generator.end = specification.Clock.End
		generator.stopAt = min(generator.stopAt, specification.Clock.End+int64(specification.Drain))
	}
	return nil
}

// begin starts the generator on the first SSNM cohort start. Later cohorts
// only extend what acceptSpec pinned.
func (generator *ssnmGenerator) begin() {
	if generator == nil {
		return
	}
	generator.mutex.Lock()
	if generator.started {
		generator.mutex.Unlock()
		return
	}
	generator.started = true
	generator.state = ssnmGeneratorRunning
	generator.mutex.Unlock()
	go generator.run()
}

// preloadStep sends one preload message: step s is position s mod
// preloadMessages() of association s / preloadMessages(), reporting its
// destinations Unavailable (pass 0), 1,024 per message. The ASP requests the
// steps in order and waits until every subscriber has consumed each before
// requesting the next, so a subscription never holds more than one preload
// event. The preload completes with its last step.
func (generator *ssnmGenerator) preloadStep(step uint64) error {
	generator.mutex.Lock()
	switch {
	case generator.started:
		generator.mutex.Unlock()
		return errors.New("SSNM generator already started")
	case generator.preload != nil && generator.preload.Error != "":
		generator.mutex.Unlock()
		return errors.New("SSNM preload already failed: " + generator.preload.Error)
	case generator.preload != nil && generator.preload.CompletedAtNS != 0:
		generator.mutex.Unlock()
		return errors.New("SSNM store was already preloaded")
	case generator.preloadBusy:
		generator.mutex.Unlock()
		return errors.New("an SSNM preload step is already in progress")
	case step != generator.preloadNext:
		generator.mutex.Unlock()
		return fmt.Errorf("SSNM preload step %d is out of order, want %d", step, generator.preloadNext)
	}
	targets, ready := generator.readyTargetsLocked()
	if !ready {
		generator.mutex.Unlock()
		return errors.New("SGP associations are not ready")
	}
	if generator.preload == nil {
		generator.preload = &ssnmPreloadRecord{}
	}
	record := generator.preload
	generator.preloadBusy = true
	generator.mutex.Unlock()

	started, err := generator.clock.Now()
	if err != nil {
		generator.failPreload(record, err)
		return err
	}
	association, position := generator.plan.preloadStep(step)
	chunk := generator.plan.chunk(position)
	_, err = generator.write(targets[association], generator.message(association, chunk))
	finished, clockErr := generator.clock.Now()
	err = errors.Join(err, clockErr)

	generator.mutex.Lock()
	defer generator.mutex.Unlock()
	generator.preloadBusy = false
	if step == 0 {
		generator.preloadStarted = started
	}
	record.Messages++
	record.Updates += uint64(chunk.count)
	if err != nil {
		record.Failed++
		record.Error = fmt.Sprintf("preload step %d (association %d): %v", step, association, err)
		return err
	}
	generator.preloadNext++
	if generator.preloadNext == generator.plan.preloadSteps() {
		record.DurationNS = finished - generator.preloadStarted
		record.CompletedAtNS = finished
	}
	return nil
}

func (generator *ssnmGenerator) failPreload(record *ssnmPreloadRecord, err error) {
	generator.mutex.Lock()
	defer generator.mutex.Unlock()
	generator.preloadBusy = false
	record.Error = err.Error()
}

// message is the DUNA or DAVA carrying chunk on association's partition.
func (generator *ssnmGenerator) message(association int, chunk ssnmChunk) messages.M3UA {
	first := association*generator.records + chunk.first
	affected := params.NewAffectedPointCode(generator.pointCodes[first : first+chunk.count]...)
	networkAppearance := params.NewNetworkAppearance(testNetworkAppearance)
	routingContext := params.NewRoutingContext(ssnmRoutingContext)
	if chunk.availability == m3ua.DestinationUnavailable {
		return messages.NewDestinationUnavailable(networkAppearance, routingContext, affected, nil)
	}
	return messages.NewDestinationAvailable(networkAppearance, routingContext, affected, nil)
}

// write sends one message, waiting for SCTP send-buffer space the way the
// library waits for its own writes: WriteSignal reports a full send buffer at
// once and sends nothing, so the generator retries the same message until it
// is accepted or ssnmWriteWait has passed. waited reports that the send
// buffer was full at least once. Any other error is returned as it is.
func (generator *ssnmGenerator) write(target ssnmSignalWriter, message messages.M3UA) (waited bool, err error) {
	var deadline int64
	backoff := 50 * time.Microsecond
	for {
		_, err = target.WriteSignal(message)
		if err == nil || !ssnmSendBufferFull(err) {
			return waited, err
		}
		now, clockErr := generator.clock.Now()
		if clockErr != nil {
			return true, errors.Join(err, clockErr)
		}
		if !waited {
			waited = true
			deadline = now + int64(ssnmWriteWait)
		}
		if now >= deadline {
			return true, fmt.Errorf("SCTP send buffer stayed full for %s: %w", ssnmWriteWait, err)
		}
		if ctxErr := generator.ctx.Err(); ctxErr != nil {
			return true, errors.Join(err, ctxErr)
		}
		generator.sleep(generator.ctx, backoff)
		backoff = min(2*backoff, ssnmWriteBackoffCeiling)
	}
}

// ssnmSendBufferFull reports a write refused for want of SCTP send-buffer
// space, which WriteSignal without a write deadline reports at once.
func ssnmSendBufferFull(err error) bool {
	return errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK)
}

// run is the open-loop schedule. Sleeps are only wake-up hints; each decision
// rereads the shared clock.
func (generator *ssnmGenerator) run() {
	defer close(generator.done)
	generator.mutex.Lock()
	targets, ready := generator.readyTargetsLocked()
	generator.mutex.Unlock()
	if !ready {
		generator.stop(ssnmGeneratorStopped, "SGP associations are not ready")
		return
	}
	var next uint64
	for {
		if err := generator.ctx.Err(); err != nil {
			generator.stop(ssnmGeneratorStopped, "process context ended: "+err.Error())
			return
		}
		now, err := generator.clock.Now()
		if err != nil {
			generator.stop(ssnmGeneratorStopped, "shared clock read failed: "+err.Error())
			return
		}
		generator.mutex.Lock()
		anchor, end, stopAt := generator.anchor, generator.end, generator.stopAt
		generator.mutex.Unlock()
		limit := stopAt
		if end != 0 {
			limit = end
		}
		total := ssnmDue(generator.rate, limit-anchor-1)
		if next >= total && end != 0 {
			generator.stop(ssnmGeneratorComplete, "")
			return
		}
		if now >= stopAt {
			generator.stop(ssnmGeneratorStopped, fmt.Sprintf("generator hard stop reached with %d of %d scheduled messages unsent", total-next, total))
			return
		}
		due := min(ssnmDue(generator.rate, now-anchor), total)
		if next < due {
			generator.send(targets, next)
			next++
			continue
		}
		wait := time.Duration(anchor + ssnmScheduled(generator.rate, next) - now)
		generator.sleep(generator.ctx, min(max(wait, 50*time.Microsecond), ssnmGeneratorPollCeiling))
	}
}

// send reports generator message m on its association.
func (generator *ssnmGenerator) send(targets []ssnmSignalWriter, message uint64) {
	association, position := generator.plan.target(message)
	chunk := generator.plan.chunk(position)
	started, startErr := generator.clock.Now()
	waited, err := generator.write(targets[association], generator.message(association, chunk))
	completed, completeErr := generator.clock.Now()
	err = errors.Join(err, startErr, completeErr)
	generator.mutex.Lock()
	defer generator.mutex.Unlock()
	generator.reports = append(generator.reports, started)
	generator.completions = append(generator.completions, completed)
	if waited {
		generator.bufferWaits++
	}
	status := ssnmMessageOK
	if err != nil {
		status = ssnmMessageFailed
		generator.failedMessages++
		if generator.firstError == "" {
			generator.firstError = fmt.Sprintf("message %d on association %d: %v", message, association, err)
		}
	}
	generator.statuses = append(generator.statuses, status)
}

func (generator *ssnmGenerator) stop(state, reason string) {
	generator.mutex.Lock()
	defer generator.mutex.Unlock()
	generator.state = state
	generator.reason = reason
}

// ssnmGeneratorRecord is the SGP record's ssnm object: what the generator
// offered and actually reported inside this cohort's shared window.
// OfferedPerAssociation splits Offered by the association each message went
// to; SendBufferWaits counts the messages that found an SCTP send buffer
// full and waited for space.
type ssnmGeneratorRecord struct {
	Scope                 string              `json:"scope"`
	Workload              ssnmWorkload        `json:"workload"`
	Associations          int                 `json:"associations"`
	PointCodeBase         uint32              `json:"point_code_base"`
	RoutingContext        uint32              `json:"routing_context"`
	NetworkAppearance     uint32              `json:"network_appearance"`
	Preload               *ssnmPreloadRecord  `json:"preload,omitempty"`
	State                 string              `json:"state"`
	StateReason           string              `json:"state_reason,omitempty"`
	AnchorNS              int64               `json:"anchor_ns"`
	WindowStartNS         int64               `json:"window_start_ns"`
	WindowEndNS           int64               `json:"window_end_ns"`
	FirstMessage          uint64              `json:"first_message"`
	EndMessage            uint64              `json:"end_message"`
	Offered               uint64              `json:"offered"`
	OfferedPerAssociation []uint64            `json:"offered_per_association"`
	OfferedPerSecond      float64             `json:"offered_per_second"`
	ReportedInWindow      uint64              `json:"reported_in_window"`
	ActualPerSecond       float64             `json:"actual_per_second"`
	Late                  uint64              `json:"late"`
	Unsent                uint64              `json:"unsent"`
	Failed                uint64              `json:"failed"`
	SendBufferWaits       uint64              `json:"send_buffer_waits"`
	FirstError            string              `json:"first_error,omitempty"`
	SentTotal             uint64              `json:"sent_total"`
	FailedTotal           uint64              `json:"failed_total"`
	IntensityHeld         bool                `json:"intensity_held"`
	DispatchTolerance     time.Duration       `json:"dispatch_tolerance_ns"`
	DispatchLag           durationPercentiles `json:"dispatch_lag"`
	ReportDuration        durationPercentiles `json:"report_duration"`
}

const ssnmGeneratorScope = "SGP open-loop DUNA/DAVA generator on the shared clock at the total rate, message m written with WriteSignal on association m mod associations only; offered counts messages scheduled in the window, reported_in_window counts those whose successful report started inside it"

// cohortRecord summarizes the generator for one cohort window.
func (generator *ssnmGenerator) cohortRecord(specification runSpec) *ssnmGeneratorRecord {
	if generator == nil || specification.Clock == nil || !specification.SSNM.enabled() {
		return nil
	}
	generator.mutex.Lock()
	defer generator.mutex.Unlock()
	record := &ssnmGeneratorRecord{
		Scope:             ssnmGeneratorScope,
		Workload:          *specification.SSNM,
		Associations:      generator.associations,
		PointCodeBase:     ssnmPointCodeBase,
		RoutingContext:    ssnmRoutingContext,
		NetworkAppearance: testNetworkAppearance,
		State:             generator.state,
		StateReason:       generator.reason,
		AnchorNS:          specification.SSNM.Anchor,
		WindowStartNS:     specification.Clock.Start,
		WindowEndNS:       specification.Clock.End,
		SentTotal:         uint64(len(generator.reports)),
		FailedTotal:       generator.failedMessages,
		SendBufferWaits:   generator.bufferWaits,
		FirstError:        generator.firstError,
	}
	if generator.preload != nil {
		preload := *generator.preload
		record.Preload = &preload
	}
	summarizeSSNMWindow(record, generator.rate, generator.reports, generator.completions, generator.statuses)
	return record
}

// summarizeSSNMWindow fills the per-window counts from the per-message log.
// record.Associations must be set.
func summarizeSSNMWindow(record *ssnmGeneratorRecord, rate uint64, reports, completions []int64, statuses []uint8) {
	start, end := record.WindowStartNS, record.WindowEndNS
	first, last := ssnmWindowMessages(rate, record.AnchorNS, start, end)
	record.FirstMessage, record.EndMessage = first, last
	record.Offered = last - first
	if record.Associations > 0 {
		plan := ssnmPlan{associations: record.Associations}
		record.OfferedPerAssociation = make([]uint64, record.Associations)
		for association := range record.OfferedPerAssociation {
			record.OfferedPerAssociation[association] = plan.messagesFor(association, last) - plan.messagesFor(association, first)
		}
	}
	seconds := time.Duration(end - start).Seconds()
	if seconds > 0 {
		record.OfferedPerSecond = float64(record.Offered) / seconds
	}
	lag := newDurationHistogram()
	duration := newDurationHistogram()
	for message := first; message < last; message++ {
		if message >= uint64(len(reports)) {
			record.Unsent++
			continue
		}
		lag.record(time.Duration(reports[message] - record.AnchorNS - ssnmScheduled(rate, message)))
		duration.record(time.Duration(completions[message] - reports[message]))
		switch {
		case statuses[message] != ssnmMessageOK:
			record.Failed++
		case reports[message] >= end:
			record.Late++
		}
	}
	for message := range reports {
		if statuses[message] == ssnmMessageOK && reports[message] >= start && reports[message] < end {
			record.ReportedInWindow++
		}
	}
	if seconds > 0 {
		record.ActualPerSecond = float64(record.ReportedInWindow) / seconds
	}
	record.DispatchLag = lag.percentiles()
	record.ReportDuration = duration.percentiles()
	record.DispatchTolerance = ssnmDispatchTolerance(rate)
	record.IntensityHeld = record.Offered > 0 && record.Unsent == 0 && record.Failed == 0 &&
		record.DispatchLag.Max <= record.DispatchTolerance
}

// ssnmDispatchTolerance is how late a report may start and still count as
// holding the fixed intensity: one scheduling interval of the total rate, and
// never less than 100 ms. A report scheduled just before the window end and
// started just after it is counted late but does not by itself break the
// intensity.
func ssnmDispatchTolerance(rate uint64) time.Duration {
	return max(100*time.Millisecond, time.Second/time.Duration(rate))
}

// ssnmReportsResponse carries the per-message log for one message range.
type ssnmReportsResponse struct {
	State       string   `json:"state"`
	SentTotal   uint64   `json:"sent_total"`
	FailedTotal uint64   `json:"failed_total"`
	From        uint64   `json:"from"`
	Reports     []int64  `json:"reports_ns"`
	Completions []int64  `json:"completions_ns"`
	Failed      []uint64 `json:"failed_messages,omitempty"`
}

func (generator *ssnmGenerator) reportsRange(from, to uint64) (ssnmReportsResponse, error) {
	if to < from || to-from > ssnmMaxReportsRange {
		return ssnmReportsResponse{}, fmt.Errorf("message range must be ascending and at most %d messages", ssnmMaxReportsRange)
	}
	generator.mutex.Lock()
	defer generator.mutex.Unlock()
	response := ssnmReportsResponse{
		State:       generator.state,
		SentTotal:   uint64(len(generator.reports)),
		FailedTotal: generator.failedMessages,
		From:        from,
	}
	stop := min(to, uint64(len(generator.reports)))
	if from < stop {
		response.Reports = append([]int64(nil), generator.reports[from:stop]...)
		response.Completions = append([]int64(nil), generator.completions[from:stop]...)
		for message := from; message < stop; message++ {
			if generator.statuses[message] != ssnmMessageOK {
				response.Failed = append(response.Failed, message)
			}
		}
	}
	return response, nil
}

// register adds the SSNM control operations. The routes exist only when the
// workload is on, so the existing control surface is unchanged.
func (generator *ssnmGenerator) register(mux *http.ServeMux) {
	if generator == nil {
		return
	}
	mux.HandleFunc("POST /ssnm/preload", func(writer http.ResponseWriter, request *http.Request) {
		step, err := strconv.ParseUint(request.URL.Query().Get("step"), 10, 64)
		if err != nil {
			http.Error(writer, "the preload step index is required", http.StatusBadRequest)
			return
		}
		if err := generator.preloadStep(step); err != nil {
			http.Error(writer, err.Error(), http.StatusConflict)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /ssnm/reports", func(writer http.ResponseWriter, request *http.Request) {
		from, fromErr := strconv.ParseUint(request.URL.Query().Get("from"), 10, 64)
		to, toErr := strconv.ParseUint(request.URL.Query().Get("to"), 10, 64)
		if fromErr != nil || toErr != nil {
			http.Error(writer, "from and to message indices are required", http.StatusBadRequest)
			return
		}
		response, err := generator.reportsRange(from, to)
		if err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
		writeJSON(writer, http.StatusOK, response)
	})
}
