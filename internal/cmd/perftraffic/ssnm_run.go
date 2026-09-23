package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gomaja/go-m3ua"
)

// Accounted retention the library charges per SSNM partition and per record
// carrying one Routing Context (ssnm_state.go: 256 and 96 + 4 bytes). The
// fixture sizes the store from them and records the limits it chose; a
// mismatch shows up as refused records, never as silent eviction.
const (
	ssnmAccountedPartitionBytes = 256
	ssnmAccountedRecordBytes    = 96 + 4
)

const (
	ssnmPreloadWait    = 60 * time.Second
	ssnmCompletionWait = 10 * time.Second
	ssnmVerdictPass    = "pass"
	ssnmVerdictFail    = "fail"
	ssnmVerdictUnknown = "inconclusive"
)

var ssnmControlClient = &http.Client{Timeout: ssnmPreloadWait}

// senderEndpointConfig is the ASP Endpoint configuration. Without SSNM load
// it is exactly the standalone configuration the fixture always used.
func senderEndpointConfig(config commandConfig) m3ua.EndpointConfig {
	endpoint := m3ua.EndpointConfig{Role: m3ua.RoleASP, ASP: nil}
	if config.SSNM.enabled() {
		limits := ssnmStoreLimits(config.SSNM, config.Associations)
		endpoint.SSNMState = &limits
	}
	return endpoint
}

// ssnmStoreLimits sizes the ASP store for the workload. A standalone ASP
// Association is one partition and its own retention peer, and the generator
// reports every destination in every association's scope, so each partition
// must hold all records and the store all partitions. Subscriptions keep the
// approved 256-event queue.
func ssnmStoreLimits(config ssnmConfig, associations int) m3ua.SSNMStateConfig {
	needed := associations * (ssnmAccountedPartitionBytes + config.Records*ssnmAccountedRecordBytes)
	return m3ua.SSNMStateConfig{
		MaxRecords:             associations * config.Records,
		MaxBytes:               max(needed, m3ua.DefaultMaxSSNMStateStoreBytes),
		MaxRecordsPerPartition: config.Records,
		MaxRecordsPerPeer:      config.Records,
		MaxPartitions:          m3ua.DefaultMaxSSNMPartitions,
		MaxSubscribers:         config.Subscribers,
		SubscriptionQueueSize:  m3ua.DefaultSSNMSubscriptionQueueSize,
		MaxAffectedPointCodes:  ssnmMaxAPCs,
	}
}

// ssnmSenderRun owns the ASP subscribers for the whole process run.
type ssnmSenderRun struct {
	config       ssnmConfig
	plan         ssnmPlan
	associations int
	drain        time.Duration
	endpoint     *m3ua.Endpoint
	clock        measurementClock
	peerControl  string
	limits       m3ua.SSNMStateConfig
	cancel       context.CancelFunc
	group        sync.WaitGroup
	subscribers  []*ssnmSubscriber
	pauseAt      atomic.Int64
	closeOnce    sync.Once

	mutex       sync.Mutex
	anchor      int64
	windowStart int64
	windowEnd   int64
	first       uint64
	last        uint64
	preload     ssnmSenderPreload
	// associationErrors lists associations that ended during the run.
	associationErrors []string
}

type ssnmSenderPreload struct {
	Positions           uint64 `json:"positions"`
	DurationNS          int64  `json:"duration_ns"`
	StoreRecords        int    `json:"store_records"`
	InitialDestinations int    `json:"initial_destinations"`
}

// startSSNMLoad opens the subscriptions, has the SGP preload the store and
// waits until every subscriber has consumed the preload, so DATA starts
// against a full store. It returns nil when the workload is off.
func startSSNMLoad(ctx context.Context, config commandConfig, endpoint *m3ua.Endpoint, associations []*m3ua.Association) (*ssnmSenderRun, error) {
	if !config.SSNM.enabled() {
		return nil, nil
	}
	clock, err := newMeasurementClock()
	if err != nil {
		return nil, fmt.Errorf("SSNM load clock: %w", err)
	}
	runContext, cancel := context.WithCancel(ctx)
	run := &ssnmSenderRun{
		config:       config.SSNM,
		plan:         ssnmPlan{records: config.SSNM.Records, apcs: config.SSNM.APCs},
		associations: config.Associations,
		drain:        config.Drain,
		endpoint:     endpoint,
		clock:        clock,
		peerControl:  config.PeerControl,
		limits:       ssnmStoreLimits(config.SSNM, config.Associations),
		cancel:       cancel,
	}
	for index := 0; index < config.SSNM.Subscribers; index++ {
		subscriber := newSSNMSubscriber(index, index == 0 && config.SSNM.Pause.enabled(), run.plan, config.SSNM.Rate, config.Associations, run.limits.SubscriptionQueueSize)
		snapshot, subscription, err := endpoint.SubscribeSSNM()
		if err != nil {
			run.close()
			return nil, fmt.Errorf("subscribe SSNM subscriber %d: %w", index, err)
		}
		for _, partition := range snapshot.Partitions {
			run.preload.InitialDestinations += len(partition.Destinations)
		}
		subscriber.subscription = subscription
		run.subscribers = append(run.subscribers, subscriber)
		run.group.Add(1)
		go func() {
			defer run.group.Done()
			subscriber.run(runContext, clock, run.pauseAt.Load, config.SSNM.Pause)
		}()
	}
	for index, association := range associations {
		run.group.Add(1)
		go func() {
			defer run.group.Done()
			run.watchAssociation(runContext, index, association)
		}()
	}
	if run.preload.InitialDestinations != 0 {
		run.close()
		return nil, errors.New("SSNM store was not empty before the preload")
	}
	started, err := clock.Now()
	if err != nil {
		run.close()
		return nil, err
	}
	if err := postSSNMPreload(ctx, config.PeerControl); err != nil {
		run.close()
		return nil, fmt.Errorf("SSNM preload: %w", err)
	}
	positions := run.plan.preloadMessages()
	if err := run.waitPositions(ctx, positions, ssnmPreloadWait); err != nil {
		run.close()
		return nil, fmt.Errorf("SSNM preload was not consumed: %w", err)
	}
	finished, err := clock.Now()
	if err != nil {
		run.close()
		return nil, err
	}
	run.preload.Positions = positions
	run.preload.DurationNS = finished - started
	run.preload.StoreRecords = ssnmStoreRecords(endpoint.SSNMKnowledge())
	if want := config.Associations * config.SSNM.Records; run.preload.StoreRecords != want {
		run.close()
		return nil, fmt.Errorf("SSNM store holds %d records after the preload, want %d", run.preload.StoreRecords, want)
	}
	return run, nil
}

// watchAssociation records an association that ends while SSNM load runs,
// with the library's close cause, which ReadData and WriteData failures on
// either side do not carry.
func (run *ssnmSenderRun) watchAssociation(ctx context.Context, index int, association *m3ua.Association) {
	select {
	case <-ctx.Done():
		return
	case <-association.Done():
	}
	cause := "closed without a recorded cause"
	if err := association.Err(); err != nil {
		cause = err.Error()
	}
	run.mutex.Lock()
	run.associationErrors = append(run.associationErrors, fmt.Sprintf("association %d: %s", index, cause))
	run.mutex.Unlock()
	writeSSNMDiagnostic("association-closed", index, cause)
}

// writeSSNMDiagnostic emits one JSON line to stderr, like the receiver's
// startup diagnostics, so a failure before any record is written keeps its
// cause.
func writeSSNMDiagnostic(event string, association int, cause string) {
	_ = json.NewEncoder(startupDiagnosticWriter).Encode(map[string]any{
		"ssnm_diagnostic": event,
		"association":     association,
		"error":           cause,
	})
}

func postSSNMPreload(ctx context.Context, baseURL string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/ssnm/preload", nil)
	if err != nil {
		return err
	}
	response, err := ssnmControlClient.Do(request)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusNoContent {
		message, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("%s: %s", response.Status, message)
	}
	return nil
}

func ssnmStoreRecords(snapshot m3ua.SSNMSnapshot) int {
	records := 0
	for _, partition := range snapshot.Partitions {
		records += len(partition.Destinations)
	}
	return records
}

// waitPositions waits until every subscriber has consumed positions on every
// association's partition. It runs before the preload and after the
// measurement, never during the F3 pause.
func (run *ssnmSenderRun) waitPositions(ctx context.Context, positions uint64, window time.Duration) error {
	deadline := time.Now().Add(window)
	for {
		pending := 0
		for _, subscriber := range run.subscribers {
			if !subscriber.reachedAll(positions, run.associations) {
				pending++
			}
		}
		if pending == 0 {
			return nil
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("%d subscriber(s) did not reach position %d within %s", pending, positions, window)
		}
		timer := time.NewTimer(20 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// attach declares the SSNM workload in a cohort specification. The first
// cohort's shared start anchors the generator; the measurement cohort arms
// the receipt stores and the F3 pause.
func (run *ssnmSenderRun) attach(specification *runSpec, phase string) error {
	if run == nil {
		return nil
	}
	if specification.Clock == nil {
		return errors.New("SSNM load requires the shared clock window")
	}
	if phase == "" {
		phase = ssnmPhaseMeasurement
	}
	run.mutex.Lock()
	defer run.mutex.Unlock()
	if run.anchor == 0 {
		run.anchor = specification.Clock.Start
	}
	workload := run.config.workload(phase, run.anchor)
	specification.SSNM = &workload
	if phase != ssnmPhaseMeasurement {
		return nil
	}
	run.windowStart, run.windowEnd = specification.Clock.Start, specification.Clock.End
	run.first, run.last = ssnmWindowMessages(run.config.Rate, run.anchor, run.windowStart, run.windowEnd)
	for _, subscriber := range run.subscribers {
		subscriber.armReceipts(run.anchor, run.first, run.last, !subscriber.paused)
	}
	if run.config.Pause.enabled() {
		run.pauseAt.Store(specification.Clock.Start + int64(run.config.Pause.Offset))
	}
	return nil
}

func (run *ssnmSenderRun) close() {
	if run == nil {
		return
	}
	run.closeOnce.Do(func() {
		run.cancel()
		for _, subscriber := range run.subscribers {
			_ = subscriber.subscription.Close()
		}
		run.group.Wait()
		// One stderr line keeps the subscribers' progress even when a cohort
		// failed before any record could carry it.
		summary := make([]map[string]any, 0, len(run.subscribers))
		for _, subscriber := range run.subscribers {
			subscriber.mutex.Lock()
			summary = append(summary, map[string]any{"index": subscriber.index, "events": subscriber.counts.Events, "accepted": subscriber.counts.Accepted, "continuity_lost": subscriber.counts.ContinuityLost})
			subscriber.mutex.Unlock()
		}
		_ = json.NewEncoder(startupDiagnosticWriter).Encode(map[string]any{"ssnm_diagnostic": "subscribers-closed", "subscribers": summary})
	})
}

// ssnmRecord is the ssnm object of a sender or receiver record. The SGP
// record carries the generator's view; the ASP record carries the
// subscribers, the store, the delay join and the final generator view.
type ssnmRecord struct {
	Scope             string                 `json:"scope,omitempty"`
	Workload          *ssnmWorkload          `json:"workload,omitempty"`
	Generator         *ssnmGeneratorRecord   `json:"generator,omitempty"`
	Store             *ssnmStoreRecord       `json:"store,omitempty"`
	Preload           *ssnmSenderPreload     `json:"preload,omitempty"`
	Subscribers       []ssnmSubscriberRecord `json:"subscribers,omitempty"`
	Delay             *ssnmDelayRecord       `json:"delay,omitempty"`
	Pause             *ssnmPauseRecord       `json:"pause,omitempty"`
	AssociationErrors []string               `json:"association_errors,omitempty"`
	Verdict           string                 `json:"verdict,omitempty"`
	Reasons           []string               `json:"reasons,omitempty"`
}

type ssnmStoreRecord struct {
	Limits                ssnmLimitsRecord `json:"limits"`
	RecordsAtEnd          int              `json:"records_at_end"`
	Partitions            int              `json:"partitions"`
	RecordsRefused        uint64           `json:"records_refused"`
	ReportsRefused        uint64           `json:"reports_refused"`
	PartitionsInvalidated uint64           `json:"partitions_invalidated"`
	LastResourceLoss      string           `json:"last_resource_loss,omitempty"`
}

type ssnmLimitsRecord struct {
	MaxRecords             int `json:"max_records"`
	MaxBytes               int `json:"max_bytes"`
	MaxRecordsPerPartition int `json:"max_records_per_partition"`
	MaxRecordsPerPeer      int `json:"max_records_per_peer"`
	MaxPartitions          int `json:"max_partitions"`
	MaxSubscribers         int `json:"max_subscribers"`
	SubscriptionQueueSize  int `json:"subscription_queue_size"`
	MaxAffectedPointCodes  int `json:"max_affected_point_codes"`
}

type ssnmDelayRecord struct {
	Scope       string              `json:"scope"`
	Subscribers int                 `json:"subscribers"`
	Messages    uint64              `json:"messages"`
	Delay       durationPercentiles `json:"report_to_receipt"`
}

const (
	ssnmSenderScope = "ASP SubscribeSSNM consumers checked against the deterministic update plan; verdict covers indication delivery and SSNM intensity, not DATA capacity"
	ssnmDelayScope  = "SGP ReportDestinationAvailability start to healthy-subscriber Next return on the shared clock, microsecond receipt resolution; an upper bound on apply-and-publish time"
)

// finish completes the SSNM evidence after the measurement cohort: it reads
// the SGP's final per-message log, waits for the subscribers to consume every
// reported message, joins receipts with report timestamps and attaches the
// ssnm object to the measurement sender record.
func (run *ssnmSenderRun) finish(ctx context.Context, measurement *cohortResult) {
	if run == nil || measurement == nil {
		return
	}
	run.mutex.Lock()
	anchor, first, last, windowStart, windowEnd := run.anchor, run.first, run.last, run.windowStart, run.windowEnd
	run.mutex.Unlock()
	workload := run.config.workload(ssnmPhaseMeasurement, anchor)
	record := &ssnmRecord{Scope: ssnmSenderScope, Workload: &workload, Preload: &run.preload}
	var reasons []string
	fail := func(reason string) { reasons = append(reasons, "fail: "+reason) }
	unknown := func(reason string) { reasons = append(reasons, "inconclusive: "+reason) }

	log, logErr := run.completedLog(ctx, last)
	if logErr != nil {
		unknown("generator log unavailable: " + logErr.Error())
	}
	failed := make(map[uint64]bool, len(log.Failed))
	for _, message := range log.Failed {
		failed[message] = true
	}
	if log.State == ssnmGeneratorRunning {
		unknown("generator was still running after the completion wait")
	}
	generator := &ssnmGeneratorRecord{
		Scope: ssnmGeneratorScope, Workload: workload, PointCodeBase: ssnmPointCodeBase,
		RoutingContext: ssnmRoutingContext, NetworkAppearance: testNetworkAppearance,
		State: log.State, AnchorNS: anchor, WindowStartNS: windowStart, WindowEndNS: windowEnd,
		SentTotal: log.SentTotal, FailedTotal: log.FailedTotal,
	}
	if measurement.Receiver.SSNM != nil && measurement.Receiver.SSNM.Generator != nil {
		generator.Preload = measurement.Receiver.SSNM.Generator.Preload
		generator.FanoutFailures = measurement.Receiver.SSNM.Generator.FanoutFailures
		generator.FirstError = measurement.Receiver.SSNM.Generator.FirstError
	} else {
		unknown("SGP record carries no SSNM generator evidence")
	}
	statuses := make([]uint8, len(log.Reports))
	for index := range statuses {
		statuses[index] = ssnmMessageOK
		if failed[uint64(index)] {
			statuses[index] = ssnmMessageFailed
		}
	}
	if log.From == 0 {
		summarizeSSNMWindow(generator, run.config.Rate, log.Reports, log.Completions, statuses)
	}
	record.Generator = generator
	if !generator.IntensityHeld {
		unknown("the generator did not report every message scheduled in the measurement window inside it")
	}
	if log.FailedTotal != 0 || generator.FanoutFailures != 0 {
		fail(fmt.Sprintf("the generator saw %d failed reports and %d association fan-out failures", log.FailedTotal, generator.FanoutFailures))
	}

	expected := run.plan.preloadMessages() + log.SentTotal
	if err := run.waitPositions(ctx, expected, ssnmCompletionWait); err != nil {
		fail("subscribers did not consume every reported message: " + err.Error())
	}
	run.close()

	knowledge := run.endpoint.SSNMKnowledge()
	record.Store = &ssnmStoreRecord{
		Limits:                limitsRecord(run.limits),
		RecordsAtEnd:          ssnmStoreRecords(knowledge),
		Partitions:            len(knowledge.Partitions),
		RecordsRefused:        knowledge.RecordsRefused,
		ReportsRefused:        knowledge.ReportsRefused,
		PartitionsInvalidated: knowledge.PartitionsInvalidated,
		LastResourceLoss:      knowledge.LastResourceLoss,
	}
	if knowledge.RecordsRefused != 0 || knowledge.ReportsRefused != 0 || knowledge.PartitionsInvalidated != 0 {
		fail("the ASP store refused or invalidated SSNM state")
	}

	histogram := newDurationHistogram()
	healthy := 0
	for _, subscriber := range run.subscribers {
		subscriberRecord := subscriber.record(expected)
		if !subscriber.paused {
			healthy++
			if logErr == nil {
				subscriberRecord.MissingReceipts = subscriber.joinDelays(histogram, log, failed)
			}
			reasons = append(reasons, healthySubscriberFailures(subscriberRecord, run.associations)...)
		} else {
			subscriber.mutex.Lock()
			if subscriber.pause != nil {
				pause := *subscriber.pause
				record.Pause = &pause
			}
			subscriber.mutex.Unlock()
			reasons = append(reasons, pausedSubscriberFailures(subscriberRecord, record.Pause, run.associations)...)
		}
		record.Subscribers = append(record.Subscribers, subscriberRecord)
	}
	if logErr == nil {
		record.Delay = &ssnmDelayRecord{Scope: ssnmDelayScope, Subscribers: healthy, Messages: last - first, Delay: histogram.percentiles()}
	}
	run.mutex.Lock()
	record.AssociationErrors = append([]string(nil), run.associationErrors...)
	run.mutex.Unlock()
	if len(record.AssociationErrors) != 0 {
		reasons = append(reasons, "fail: an association ended during SSNM load")
	}
	record.Verdict, record.Reasons = ssnmVerdict(reasons)
	measurement.Sender.SSNM = record
	measurement.Sender.UnsupportedModes = ssnmUnsupportedModes()
}

func limitsRecord(limits m3ua.SSNMStateConfig) ssnmLimitsRecord {
	return ssnmLimitsRecord{
		MaxRecords: limits.MaxRecords, MaxBytes: limits.MaxBytes,
		MaxRecordsPerPartition: limits.MaxRecordsPerPartition, MaxRecordsPerPeer: limits.MaxRecordsPerPeer,
		MaxPartitions: limits.MaxPartitions, MaxSubscribers: limits.MaxSubscribers,
		SubscriptionQueueSize: limits.SubscriptionQueueSize, MaxAffectedPointCodes: limits.MaxAffectedPointCodes,
	}
}

// healthySubscriberFailures lists why a subscriber that must stay lossless
// did not.
func healthySubscriberFailures(record ssnmSubscriberRecord, associations int) []string {
	var reasons []string
	prefix := fmt.Sprintf("fail: healthy subscriber %d ", record.Index)
	if record.Gaps != 0 || record.Duplicates != 0 || record.Unexpected != 0 {
		reasons = append(reasons, prefix+fmt.Sprintf("saw %d missing, %d duplicate and %d unexpected reports", record.Gaps, record.Duplicates, record.Unexpected))
	}
	if record.ContinuityLost != 0 || record.ResourceLoss != 0 || record.Invalidated != 0 {
		reasons = append(reasons, prefix+fmt.Sprintf("saw %d continuity-loss, %d resource-loss and %d invalidation events", record.ContinuityLost, record.ResourceLoss, record.Invalidated))
	}
	reasons = append(reasons, positionFailures(prefix, record, associations)...)
	if record.MissingReceipts != 0 {
		reasons = append(reasons, prefix+fmt.Sprintf("never received %d measurement messages", record.MissingReceipts))
	}
	if record.Error != "" {
		reasons = append(reasons, prefix+"failed: "+record.Error)
	}
	return reasons
}

func positionFailures(prefix string, record ssnmSubscriberRecord, associations int) []string {
	if record.Partitions != associations {
		return []string{prefix + fmt.Sprintf("saw %d partitions, want %d", record.Partitions, associations)}
	}
	for slot, position := range record.FinalPositions {
		if position != record.ExpectedFinalPosition {
			return []string{prefix + fmt.Sprintf("ended partition %d at position %d, want %d", slot, position, record.ExpectedFinalPosition)}
		}
	}
	return nil
}

// pausedSubscriberFailures checks the F3 contract: the paused subscriber
// observes its loss at the enforced cap, recovers by an authoritative
// snapshot and is lossless again afterwards.
func pausedSubscriberFailures(record ssnmSubscriberRecord, pause *ssnmPauseRecord, associations int) []string {
	prefix := fmt.Sprintf("fail: paused subscriber %d ", record.Index)
	if pause == nil {
		return []string{prefix + "never paused"}
	}
	var reasons []string
	switch {
	case pause.Error != "":
		reasons = append(reasons, prefix+"recovery failed: "+pause.Error)
	case !pause.ContinuityLossObserved:
		reasons = append(reasons, prefix+"observed no continuity loss")
	case !pause.CountCapEnforced:
		reasons = append(reasons, prefix+fmt.Sprintf("retained %d events at loss, cap %d", pause.QueuedAtLoss, pause.QueueLimit))
	case pause.SnapshotValidated != associations:
		reasons = append(reasons, prefix+fmt.Sprintf("validated %d of %d resynchronized partitions", pause.SnapshotValidated, associations))
	}
	if record.SnapshotMismatches != 0 {
		reasons = append(reasons, prefix+fmt.Sprintf("resynchronized snapshot disagreed with the plan at %d destinations", record.SnapshotMismatches))
	}
	if record.Gaps != 0 || record.Duplicates != 0 || record.Unexpected != 0 || record.ContinuityLost > 1 || record.ResourceLoss != 0 || record.Invalidated != 0 {
		reasons = append(reasons, prefix+"was not lossless outside its one observed overflow")
	}
	reasons = append(reasons, positionFailures(prefix, record, associations)...)
	if record.Error != "" {
		reasons = append(reasons, prefix+"failed: "+record.Error)
	}
	return reasons
}

func ssnmVerdict(reasons []string) (string, []string) {
	verdict := ssnmVerdictPass
	for _, reason := range reasons {
		if len(reason) >= 5 && reason[:5] == "fail:" {
			return ssnmVerdictFail, reasons
		}
		verdict = ssnmVerdictUnknown
	}
	return verdict, reasons
}

// completedLog waits for the generator to leave the running state and reads
// its whole per-message log.
func (run *ssnmSenderRun) completedLog(ctx context.Context, last uint64) (ssnmReportsResponse, error) {
	deadline := time.Now().Add(run.drain + ssnmCompletionWait)
	for {
		log, err := getSSNMReports(ctx, run.peerControl, 0, last)
		if err != nil {
			return ssnmReportsResponse{}, err
		}
		if log.State != ssnmGeneratorRunning || !time.Now().Before(deadline) {
			return log, nil
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return log, ctx.Err()
		case <-timer.C:
		}
	}
}

func getSSNMReports(ctx context.Context, baseURL string, from, to uint64) (ssnmReportsResponse, error) {
	url := fmt.Sprintf("%s/ssnm/reports?from=%d&to=%d", baseURL, from, to)
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return ssnmReportsResponse{}, err
	}
	response, err := ssnmControlClient.Do(request)
	if err != nil {
		return ssnmReportsResponse{}, err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return ssnmReportsResponse{}, fmt.Errorf("SSNM reports: %s", response.Status)
	}
	var log ssnmReportsResponse
	if err := json.NewDecoder(io.LimitReader(response.Body, 128<<20)).Decode(&log); err != nil {
		return ssnmReportsResponse{}, err
	}
	if len(log.Completions) != len(log.Reports) {
		return ssnmReportsResponse{}, errors.New("SSNM reports and completions differ in length")
	}
	return log, nil
}

// ssnmUnsupportedModes replaces the default unavailable-mode note when the
// run exercised the SSNM state API.
func ssnmUnsupportedModes() map[string]string {
	return map[string]string{
		"router_workload":             "unavailable: this fixture does not exercise the existing routing API",
		"independent_peer_validation": "unavailable: both endpoints use this binary",
	}
}
