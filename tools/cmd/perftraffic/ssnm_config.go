package main

import (
	"errors"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/gomaja/go-m3ua"
)

// SSNM load workload bounds. The workload is off unless -ssnm-total-rate is
// set, so every existing mode keeps its exact flags, specification and
// records.
const (
	// ssnmMaxTotalRate bounds the total generated message rate over every
	// association.
	ssnmMaxTotalRate = 10_000
	// ssnmMaxAPCs is the Affected Point Code count one generated message may
	// carry, matching the library's default per-message acceptance bound.
	ssnmMaxAPCs = m3ua.DefaultMaxAffectedPointCodesPerSSNM
	// ssnmMaxRecords is the per-association destination bound: each ASP
	// Association's partition retains at most this many destination records
	// by default, and one generated scope creates one per destination.
	ssnmMaxRecords         = m3ua.DefaultMaxSSNMDestinationRecords
	ssnmDefaultRecords     = 16_384
	ssnmDefaultSubscribers = 8
	ssnmMaxSubscribers     = m3ua.DefaultMaxSSNMSubscribers
	// ssnmPreloadChunk is the Affected Point Code count of each preload
	// message that fills the store before traffic starts.
	ssnmPreloadChunk = m3ua.DefaultMaxAffectedPointCodesPerSSNM
	// ssnmPointCodeBase starts the reported destination range: association a
	// reports 0x400000 + a*records + d, at most 32 x 16,384 destinations, so
	// the range ends by 0x47ffff. DATA uses DPCs 0x220000-0x221f1f and OPCs
	// 0x110000-0x111f1f, so the SSNM range never names a DATA destination and
	// cannot make the measured DATA ineligible.
	ssnmPointCodeBase = uint32(0x400000)
	// ssnmRoutingContext is the Application Server scope the generator
	// reports in: flow 0's AS, which every fixture association serves.
	ssnmRoutingContext = uint32(100)
	// ssnmMaxReceiptBytes bounds the ASP's per-event receipt storage used to
	// join subscriber receipts with the SGP's report timestamps.
	ssnmMaxReceiptBytes = 64 << 20
	// ssnmGeneratorHorizon bounds how long a generator runs after its anchor
	// when no measurement cohort ever names its end.
	ssnmGeneratorHorizon = maxRunWindow + time.Minute
	// ssnmMinSubscriptionQueueBytes is the smallest positive subscription
	// byte limit SSNMStateConfig.SubscriptionQueueBytes accepts: "a positive
	// limit must be at least 512 bytes", one event's fixed charge.
	ssnmMinSubscriptionQueueBytes = ssnmEventBaseBytes
)

const (
	ssnmPhaseWarmup      = "warmup"
	ssnmPhaseMeasurement = "measurement"
)

// The section 4 time budgets the ASP judges, at their contract values:
// "p99 apply time within 100 ms" for the large (1,024-APC) row, and for
// resynchronization "authoritative retained snapshot/subscription acquisition
// within 100 ms" and "consume retained snapshot and 256 queued indications
// within 1 s". The one-APC steady row has no apply-time budget there.
const (
	ssnmDefaultApplyP99Budget = 100 * time.Millisecond
	ssnmDefaultResyncBudget   = 100 * time.Millisecond
	ssnmDefaultRecoveryBudget = time.Second
)

// ssnmBudgets are the ASP's SSNM time budgets. The flags default to the
// contract values; the manifest records the values a run was judged against.
type ssnmBudgets struct {
	ApplyP99 time.Duration
	Resync   time.Duration
	Recovery time.Duration
}

// ssnmPause is one subscriber's F3 pause: it stops reading Offset into the
// measurement window for Duration, then recovers by Resync.
type ssnmPause struct {
	Offset   time.Duration
	Duration time.Duration
}

func (pause ssnmPause) enabled() bool { return pause.Duration > 0 }

// ssnmConfig is one process's SSNM load flags. TotalRate zero disables the
// whole workload; the struct is then zero so a disabled configuration is
// identical to one parsed before the workload existed.
//
// TotalRate is the number of SSNM messages per second the ASP receives over
// every association together: the generator schedules one open-loop sequence
// at that rate and sends each message on one association, round-robin.
// Records is the number of destinations of each association's partition, so
// the ASP store holds associations x Records records.
type ssnmConfig struct {
	TotalRate   uint64
	APCs        int
	Records     int
	Subscribers int
	Pause       ssnmPause
	// QueueBytes is the ASP subscriptions' accounted byte limit,
	// SSNMStateConfig.SubscriptionQueueBytes. Validation resolves zero to the
	// library default; it is zero on the SGP.
	QueueBytes int
	// Budgets are judged by the ASP only; they are zero on the SGP.
	Budgets ssnmBudgets
}

func (config ssnmConfig) enabled() bool { return config.TotalRate > 0 }

// subscriptionQueueBytes is the subscription byte limit in force: the flag,
// or the library default when it is unset.
func (config ssnmConfig) subscriptionQueueBytes() int {
	if config.QueueBytes > 0 {
		return config.QueueBytes
	}
	return m3ua.DefaultSSNMSubscriptionQueueBytes
}

// healthySubscribers counts the subscribers that must stay lossless.
func (config ssnmConfig) healthySubscribers() int {
	if config.Pause.enabled() {
		return config.Subscribers - 1
	}
	return config.Subscribers
}

// ssnmWorkload is the SSNM part of a cohort specification. The ASP declares
// it and the SGP accepts a cohort only when the total rate, APCs and records
// equal its own flags and the cohort's association count its own, so both
// processes provably ran the same disturbance. TotalRate is carried as
// total_rate: the earlier rate field meant a per-association broadcast
// intensity, and evidence under that meaning must never read as this one.
// Anchor is the shared-clock instant generator message zero is scheduled at:
// the first SSNM cohort's start, carried unchanged by every later cohort.
// SubscriptionQueueBytes is the ASP subscriptions' byte limit in force, the
// library default included, so the SGP record and perfcapacity see the limit
// the F3 overflow was judged against.
type ssnmWorkload struct {
	TotalRate              uint64        `json:"total_rate"`
	APCs                   int           `json:"apcs"`
	Records                int           `json:"records"`
	Subscribers            int           `json:"subscribers"`
	SubscriptionQueueBytes int           `json:"subscription_queue_bytes"`
	PauseOffset            time.Duration `json:"pause_offset_ns"`
	PauseDuration          time.Duration `json:"pause_duration_ns"`
	Phase                  string        `json:"phase"`
	Anchor                 int64         `json:"anchor_ns"`
}

func (workload *ssnmWorkload) enabled() bool { return workload != nil && workload.TotalRate > 0 }

// pauseFlag parses -pause-subscriber=<offset>/<duration>.
type pauseFlag struct {
	pause *ssnmPause
}

func (value pauseFlag) String() string {
	if value.pause == nil || !value.pause.enabled() {
		return ""
	}
	return value.pause.Offset.String() + "/" + value.pause.Duration.String()
}

func (value pauseFlag) Set(text string) error {
	offsetText, durationText, found := strings.Cut(text, "/")
	if !found {
		return errors.New("pause-subscriber must be <offset>/<duration>, for example 5s/10s")
	}
	offset, err := time.ParseDuration(offsetText)
	if err != nil {
		return fmt.Errorf("pause-subscriber offset: %w", err)
	}
	duration, err := time.ParseDuration(durationText)
	if err != nil {
		return fmt.Errorf("pause-subscriber duration: %w", err)
	}
	if offset < 0 || duration <= 0 {
		return errors.New("pause-subscriber offset must not be negative and duration must be positive")
	}
	*value.pause = ssnmPause{Offset: offset, Duration: duration}
	return nil
}

func registerSSNMFlags(flagSet *flag.FlagSet, config *ssnmConfig) {
	flagSet.Uint64Var(&config.TotalRate, "ssnm-total-rate", 0, "SSNM load: generated DUNA/DAVA messages per second in total, each sent on one association round-robin (0 disables the SSNM workload)")
	flagSet.IntVar(&config.APCs, "ssnm-apcs", 1, "SSNM load: Affected Point Codes per generated message (1 to 1024)")
	flagSet.IntVar(&config.Records, "ssnm-records", ssnmDefaultRecords, "SSNM load: distinct destinations cycled and retained in each association's partition")
	flagSet.IntVar(&config.Subscribers, "subscribers", 0, "SSNM load (ASP): SSNM subscriptions, default 8 when SSNM load is on")
	flagSet.Var(pauseFlag{pause: &config.Pause}, "pause-subscriber", "SSNM load (ASP): pause subscriber 0 at <offset>/<duration> into the measurement window, then Resync")
	flagSet.IntVar(&config.QueueBytes, "ssnm-subscription-queue-bytes", 0, "SSNM load (ASP): accounted byte limit of each subscription queue, SSNMStateConfig.SubscriptionQueueBytes (0 selects the library default, 1 MiB)")
	flagSet.DurationVar(&config.Budgets.ApplyP99, "ssnm-apply-p99-budget", ssnmDefaultApplyP99Budget, "SSNM load (ASP): p99 apply-time budget of 1,024-APC messages, judged on the report-to-receipt p99, which bounds apply time from above; other APC counts record the delay without a budget")
	flagSet.DurationVar(&config.Budgets.Resync, "ssnm-resync-budget", ssnmDefaultResyncBudget, "SSNM load (ASP): budget for the paused subscriber's Resync snapshot and subscription acquisition")
	flagSet.DurationVar(&config.Budgets.Recovery, "ssnm-recovery-budget", ssnmDefaultRecoveryBudget, "SSNM load (ASP): budget for the paused subscriber to consume its retained queued indications and the Resync snapshot after the pause")
}

// ssnmBudgetFlags are the ASP-only budget flags.
var ssnmBudgetFlags = []string{"ssnm-apply-p99-budget", "ssnm-resync-budget", "ssnm-recovery-budget"}

// validateSSNMConfig applies the SSNM defaults and bounds after the rest of
// the configuration has been validated.
func validateSSNMConfig(flagSet *flag.FlagSet, config *commandConfig) error {
	explicit := make(map[string]bool)
	flagSet.Visit(func(set *flag.Flag) { explicit[set.Name] = true })
	ssnm := &config.SSNM
	if !ssnm.enabled() {
		for _, name := range append([]string{"ssnm-apcs", "ssnm-records", "subscribers", "pause-subscriber", "ssnm-subscription-queue-bytes"}, ssnmBudgetFlags...) {
			if explicit[name] {
				return fmt.Errorf("-%s requires -ssnm-total-rate", name)
			}
		}
		*ssnm = ssnmConfig{}
		return nil
	}
	switch {
	case ssnm.TotalRate > ssnmMaxTotalRate:
		return fmt.Errorf("ssnm-total-rate must not exceed %d", ssnmMaxTotalRate)
	case ssnm.APCs < 1 || ssnm.APCs > ssnmMaxAPCs:
		return fmt.Errorf("ssnm-apcs must be between 1 and %d", ssnmMaxAPCs)
	case ssnm.Records < 1 || ssnm.Records > ssnmMaxRecords:
		return fmt.Errorf("ssnm-records must be between 1 and %d", ssnmMaxRecords)
	case ssnm.Records%ssnm.APCs != 0:
		return errors.New("ssnm-records must be a multiple of ssnm-apcs so every message reports one availability")
	case config.Mode != modeThroughput:
		return errors.New("SSNM load runs with the direct throughput mode only")
	case !config.SameHostClock:
		return errors.New("SSNM load requires -same-host-clock: its schedule and delays use the shared Linux monotonic clock")
	}
	if config.Role == "sgp" {
		if explicit["subscribers"] || explicit["pause-subscriber"] || explicit["ssnm-subscription-queue-bytes"] {
			return errors.New("-subscribers, -pause-subscriber and -ssnm-subscription-queue-bytes configure the ASP subscribers and are not SGP flags")
		}
		for _, name := range ssnmBudgetFlags {
			if explicit[name] {
				return fmt.Errorf("-%s is judged by the ASP and is not an SGP flag", name)
			}
		}
		ssnm.Budgets = ssnmBudgets{}
		return nil
	}
	if ssnm.Budgets.ApplyP99 <= 0 || ssnm.Budgets.Resync <= 0 || ssnm.Budgets.Recovery <= 0 {
		return errors.New("SSNM budgets must be positive durations")
	}
	if ssnm.Subscribers == 0 {
		ssnm.Subscribers = ssnmDefaultSubscribers
	}
	if ssnm.Subscribers < 1 || ssnm.Subscribers > ssnmMaxSubscribers {
		return fmt.Errorf("subscribers must be between 1 and %d", ssnmMaxSubscribers)
	}
	if err := validateSSNMQueueBytes(ssnm); err != nil {
		return err
	}
	if ssnm.Pause.enabled() {
		if ssnm.Subscribers < 2 {
			return errors.New("pause-subscriber needs at least two subscribers so one stays healthy")
		}
		if ssnm.Pause.Offset >= config.Duration || ssnm.Pause.Duration >= config.Duration-ssnm.Pause.Offset {
			return errors.New("pause-subscriber offset plus duration must end inside the measurement window")
		}
	}
	messages, err := scheduledMessages(ssnm.TotalRate, config.Duration)
	if err != nil {
		return fmt.Errorf("ssnm-total-rate: %w", err)
	}
	// Every generator message reaches one partition, so a healthy subscriber
	// keeps one receipt per measurement message whatever the association
	// count.
	receiptBytes := uint64(ssnm.healthySubscribers()) * (messages + 1) * 4
	if receiptBytes > ssnmMaxReceiptBytes {
		return fmt.Errorf("SSNM receipt storage of %d bytes exceeds %d; reduce subscribers, total rate or duration", receiptBytes, ssnmMaxReceiptBytes)
	}
	return nil
}

// validateSSNMQueueBytes resolves the subscription byte limit and refuses one
// the library would refuse or that cannot hold the workload's largest
// message. That message is the preload's: min(records, 1,024) destinations.
// A queue that cannot hold it loses continuity on every subscriber, the
// healthy ones included, before any traffic starts.
//
// The rule is also sufficient for the preload: every generator message
// reaches one association, so it becomes one event in each subscription, and
// the ASP requests the preload one message at a time, waiting until every
// subscriber has consumed each before asking for the next. A queue therefore
// never holds more than one preload event.
func validateSSNMQueueBytes(ssnm *ssnmConfig) error {
	switch {
	case ssnm.QueueBytes < 0:
		return errors.New("ssnm-subscription-queue-bytes must not be negative")
	case ssnm.QueueBytes == 0:
		ssnm.QueueBytes = m3ua.DefaultSSNMSubscriptionQueueBytes
	case ssnm.QueueBytes < ssnmMinSubscriptionQueueBytes:
		return fmt.Errorf("ssnm-subscription-queue-bytes must be 0 or at least %d", ssnmMinSubscriptionQueueBytes)
	}
	destinations := min(ssnm.Records, ssnmPreloadChunk)
	if largest := ssnmWorkloadEventBytes(destinations); ssnm.QueueBytes < largest {
		return fmt.Errorf("ssnm-subscription-queue-bytes %d cannot hold the %d-destination preload message, %d accounted bytes; every subscriber would lose continuity before traffic starts", ssnm.QueueBytes, destinations, largest)
	}
	return nil
}

// workload is the cohort specification this configuration declares for one
// phase. The anchor is filled in by the sender run.
func (config ssnmConfig) workload(phase string, anchor int64) ssnmWorkload {
	return ssnmWorkload{
		TotalRate:              config.TotalRate,
		APCs:                   config.APCs,
		Records:                config.Records,
		Subscribers:            config.Subscribers,
		SubscriptionQueueBytes: config.subscriptionQueueBytes(),
		PauseOffset:            config.Pause.Offset,
		PauseDuration:          config.Pause.Duration,
		Phase:                  phase,
		Anchor:                 anchor,
	}
}

// ssnmBudgetsRecord is the manifest's record of the budgets an SSNM-loaded
// ASP judged its run against.
type ssnmBudgetsRecord struct {
	ApplyP99 time.Duration `json:"apply_p99_ns"`
	Resync   time.Duration `json:"resync_ns"`
	Recovery time.Duration `json:"recovery_ns"`
	Scope    string        `json:"scope"`
}

const ssnmBudgetsScope = "apply_p99 gates 1,024-APC runs on the report-to-receipt p99 (an upper bound on apply time); resync and recovery gate the paused subscriber's Resync acquisition and its consumption of the retained queued indications plus the snapshot; other runs record these measurements without a gate"

// budgetsRecord is the manifest entry, nil when SSNM load is off.
func (config ssnmConfig) budgetsRecord() *ssnmBudgetsRecord {
	if !config.enabled() {
		return nil
	}
	return &ssnmBudgetsRecord{ApplyP99: config.Budgets.ApplyP99, Resync: config.Budgets.Resync, Recovery: config.Budgets.Recovery, Scope: ssnmBudgetsScope}
}

// ssnmScope is the wire scope every generated message names.
func ssnmScope() m3ua.WireScope {
	return m3ua.WireScope{
		NetworkAppearance:    testNetworkAppearance,
		NetworkAppearanceSet: true,
		RoutingContexts:      []uint32{ssnmRoutingContext},
		RoutingContextSet:    true,
	}
}
