package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gomaja/go-m3ua"
)

// ssnmReporter is the SGP Endpoint operation the generator drives.
type ssnmReporter interface {
	ReportDestinationAvailability(request m3ua.DestinationAvailabilityRequest) error
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
)

// ssnmGenerator is the SGP's open-loop SSNM source. It reports generator
// message m at anchor + floor(m * 1s / rate) on the shared clock, in order,
// catching up when it falls behind rather than skipping, from the first SSNM
// cohort's start through the measurement cohort's end. Every message's report
// start and completion are retained so the ASP can join them with subscriber
// receipts.
type ssnmGenerator struct {
	ctx          context.Context
	rate         uint64
	apcs         int
	records      int
	plan         ssnmPlan
	clock        measurementClock
	destinations []m3ua.PointCodeRange

	mutex    sync.Mutex
	reporter ssnmReporter
	state    string
	reason   string
	preload  *ssnmPreloadRecord
	anchor   int64
	end      int64
	stopAt   int64
	started  bool
	done     chan struct{}
	// reports, completions and statuses are indexed by generator message.
	reports        []int64
	completions    []int64
	statuses       []uint8
	fanoutFailures uint64
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
	return &ssnmGenerator{
		ctx:          ctx,
		rate:         config.SSNM.Rate,
		apcs:         config.SSNM.APCs,
		records:      config.SSNM.Records,
		plan:         ssnmPlan{records: config.SSNM.Records, apcs: config.SSNM.APCs},
		clock:        clock,
		destinations: ssnmDestinations(config.SSNM.Records),
		state:        ssnmGeneratorIdle,
		done:         make(chan struct{}),
	}
}

func (generator *ssnmGenerator) setReporter(reporter ssnmReporter) {
	if generator == nil {
		return
	}
	generator.mutex.Lock()
	defer generator.mutex.Unlock()
	generator.reporter = reporter
}

// acceptSpec checks one cohort's SSNM declaration against this process's own
// flags and pins the generator anchor and end. It runs under the receiver
// control mutex, before the cohort is committed.
func (generator *ssnmGenerator) acceptSpec(specification runSpec) error {
	workload := specification.SSNM
	if generator == nil {
		if workload.enabled() {
			return errors.New("SSNM load is declared but this receiver runs without -ssnm-rate")
		}
		return nil
	}
	if !workload.enabled() {
		return errors.New("this receiver runs SSNM load but the cohort declares none")
	}
	if workload.Rate != generator.rate || workload.APCs != generator.apcs || workload.Records != generator.records {
		return fmt.Errorf("SSNM workload %d/s x %d APCs over %d records does not match this receiver's %d/s x %d APCs over %d records",
			workload.Rate, workload.APCs, workload.Records, generator.rate, generator.apcs, generator.records)
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

// preloadStore fills the store with pass 0 before any cohort: every
// destination is reported Unavailable once, 1,024 per message.
func (generator *ssnmGenerator) preloadStore() error {
	generator.mutex.Lock()
	if generator.preload != nil || generator.started {
		generator.mutex.Unlock()
		return errors.New("SSNM store was already preloaded")
	}
	reporter := generator.reporter
	record := &ssnmPreloadRecord{}
	generator.preload = record
	generator.mutex.Unlock()
	if reporter == nil {
		generator.finishPreload(record, 0, errors.New("SGP endpoint is not ready"))
		return errors.New("SGP endpoint is not ready")
	}
	started, err := generator.clock.Now()
	if err != nil {
		generator.finishPreload(record, 0, err)
		return err
	}
	var firstErr error
	for position := uint64(0); position < generator.plan.preloadMessages(); position++ {
		chunk := generator.plan.chunk(position)
		reportErr := reporter.ReportDestinationAvailability(generator.request(chunk))
		generator.mutex.Lock()
		record.Messages++
		record.Updates += uint64(chunk.count)
		if reportErr != nil {
			record.Failed++
			if firstErr == nil {
				firstErr = reportErr
			}
		}
		generator.mutex.Unlock()
	}
	finished, err := generator.clock.Now()
	if err != nil && firstErr == nil {
		firstErr = err
	}
	generator.finishPreload(record, finished-started, firstErr)
	if firstErr != nil {
		return firstErr
	}
	return nil
}

func (generator *ssnmGenerator) finishPreload(record *ssnmPreloadRecord, duration int64, err error) {
	completed, _ := generator.clock.Now()
	generator.mutex.Lock()
	defer generator.mutex.Unlock()
	record.DurationNS = duration
	record.CompletedAtNS = completed
	if err != nil {
		record.Error = err.Error()
	}
}

func (generator *ssnmGenerator) request(chunk ssnmChunk) m3ua.DestinationAvailabilityRequest {
	return m3ua.DestinationAvailabilityRequest{
		Scope:        ssnmScope(),
		Destinations: generator.destinations[chunk.first : chunk.first+chunk.count],
		Availability: chunk.availability,
	}
}

// run is the open-loop schedule. Timers are only wake-up hints; each decision
// rereads the shared clock.
func (generator *ssnmGenerator) run() {
	defer close(generator.done)
	generator.mutex.Lock()
	reporter := generator.reporter
	generator.mutex.Unlock()
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
			generator.send(reporter, next, anchor)
			next++
			continue
		}
		wait := time.Duration(anchor + ssnmScheduled(generator.rate, next) - now)
		wait = min(max(wait, 50*time.Microsecond), ssnmGeneratorPollCeiling)
		timer := time.NewTimer(wait)
		select {
		case <-generator.ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
}

func (generator *ssnmGenerator) send(reporter ssnmReporter, message uint64, anchor int64) {
	chunk := generator.plan.chunk(generator.plan.preloadMessages() + message)
	started, startErr := generator.clock.Now()
	err := reporter.ReportDestinationAvailability(generator.request(chunk))
	completed, completeErr := generator.clock.Now()
	err = errors.Join(err, startErr, completeErr)
	generator.mutex.Lock()
	defer generator.mutex.Unlock()
	generator.reports = append(generator.reports, started)
	generator.completions = append(generator.completions, completed)
	status := ssnmMessageOK
	if err != nil {
		status = ssnmMessageFailed
		generator.failedMessages++
		var delivery *m3ua.SSNMDeliveryError
		if errors.As(err, &delivery) {
			generator.fanoutFailures += uint64(len(delivery.Failed))
		}
		if generator.firstError == "" {
			generator.firstError = fmt.Sprintf("message %d: %v", message, err)
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
type ssnmGeneratorRecord struct {
	Scope             string              `json:"scope"`
	Workload          ssnmWorkload        `json:"workload"`
	PointCodeBase     uint32              `json:"point_code_base"`
	RoutingContext    uint32              `json:"routing_context"`
	NetworkAppearance uint32              `json:"network_appearance"`
	Preload           *ssnmPreloadRecord  `json:"preload,omitempty"`
	State             string              `json:"state"`
	StateReason       string              `json:"state_reason,omitempty"`
	AnchorNS          int64               `json:"anchor_ns"`
	WindowStartNS     int64               `json:"window_start_ns"`
	WindowEndNS       int64               `json:"window_end_ns"`
	FirstMessage      uint64              `json:"first_message"`
	EndMessage        uint64              `json:"end_message"`
	Offered           uint64              `json:"offered"`
	OfferedPerSecond  float64             `json:"offered_per_second"`
	ReportedInWindow  uint64              `json:"reported_in_window"`
	ActualPerSecond   float64             `json:"actual_per_second"`
	Late              uint64              `json:"late"`
	Unsent            uint64              `json:"unsent"`
	Failed            uint64              `json:"failed"`
	FanoutFailures    uint64              `json:"fanout_failures"`
	FirstError        string              `json:"first_error,omitempty"`
	SentTotal         uint64              `json:"sent_total"`
	FailedTotal       uint64              `json:"failed_total"`
	IntensityHeld     bool                `json:"intensity_held"`
	DispatchLag       durationPercentiles `json:"dispatch_lag"`
	ReportDuration    durationPercentiles `json:"report_duration"`
}

const ssnmGeneratorScope = "SGP open-loop ReportDestinationAvailability generator on the shared clock; offered counts messages scheduled in the window, reported_in_window counts those whose successful report started inside it"

// cohortRecord summarizes the generator for one cohort window.
func (generator *ssnmGenerator) cohortRecord(specification runSpec) *ssnmGeneratorRecord {
	if generator == nil || specification.Clock == nil || !specification.SSNM.enabled() {
		return nil
	}
	generator.mutex.Lock()
	defer generator.mutex.Unlock()
	record := &ssnmGeneratorRecord{
		Scope:             ssnmGeneratorScope,
		Workload:          specification.SSNM,
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
		FanoutFailures:    generator.fanoutFailures,
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
func summarizeSSNMWindow(record *ssnmGeneratorRecord, rate uint64, reports, completions []int64, statuses []uint8) {
	start, end := record.WindowStartNS, record.WindowEndNS
	first, last := ssnmWindowMessages(rate, record.AnchorNS, start, end)
	record.FirstMessage, record.EndMessage = first, last
	record.Offered = last - first
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
	record.IntensityHeld = record.Offered > 0 && record.Unsent == 0 && record.Late == 0 && record.Failed == 0
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
	mux.HandleFunc("POST /ssnm/preload", func(writer http.ResponseWriter, _ *http.Request) {
		if err := generator.preloadStore(); err != nil {
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
