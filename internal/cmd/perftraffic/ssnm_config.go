package main

import (
	"errors"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/gomaja/go-m3ua"
)

// SSNM load workload bounds. The workload is off unless -ssnm-rate is set, so
// every existing mode keeps its exact flags, specification and records.
const (
	ssnmMaxRate = 10_000
	// ssnmMaxAPCs is the Affected Point Code count one generated message may
	// carry, matching the library's default per-message acceptance bound.
	ssnmMaxAPCs = m3ua.DefaultMaxAffectedPointCodesPerSSNM
	// ssnmMaxRecords is the per-association destination bound on both sides:
	// each ASP and SGP Association retains at most this many destination
	// records by default, and one generated scope creates one per destination.
	ssnmMaxRecords         = m3ua.DefaultMaxSSNMDestinationRecords
	ssnmDefaultRecords     = 16_384
	ssnmDefaultSubscribers = 8
	ssnmMaxSubscribers     = m3ua.DefaultMaxSSNMSubscribers
	// ssnmPreloadChunk is the Affected Point Code count of each preload
	// message that fills the store before traffic starts.
	ssnmPreloadChunk = m3ua.DefaultMaxAffectedPointCodesPerSSNM
	// ssnmPointCodeBase starts the reported destination range. DATA uses DPCs
	// 0x220000-0x221f1f and OPCs 0x110000-0x111f1f, so the SSNM range never
	// names a DATA destination and cannot make the measured DATA ineligible.
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
)

const (
	ssnmPhaseWarmup      = "warmup"
	ssnmPhaseMeasurement = "measurement"
)

// ssnmPause is one subscriber's F3 pause: it stops reading Offset into the
// measurement window for Duration, then recovers by Resync.
type ssnmPause struct {
	Offset   time.Duration
	Duration time.Duration
}

func (pause ssnmPause) enabled() bool { return pause.Duration > 0 }

// ssnmConfig is one process's SSNM load flags. Rate zero disables the whole
// workload; the struct is then zero so a disabled configuration is identical
// to one parsed before the workload existed.
type ssnmConfig struct {
	Rate        uint64
	APCs        int
	Records     int
	Subscribers int
	Pause       ssnmPause
}

func (config ssnmConfig) enabled() bool { return config.Rate > 0 }

// healthySubscribers counts the subscribers that must stay lossless.
func (config ssnmConfig) healthySubscribers() int {
	if config.Pause.enabled() {
		return config.Subscribers - 1
	}
	return config.Subscribers
}

// ssnmWorkload is the SSNM part of a cohort specification. The ASP declares
// it and the SGP accepts a cohort only when rate, APCs and records equal its
// own flags, so both processes provably ran the same disturbance. Anchor is
// the shared-clock instant generator message zero is scheduled at: the first
// SSNM cohort's start, carried unchanged by every later cohort.
type ssnmWorkload struct {
	Rate          uint64        `json:"rate"`
	APCs          int           `json:"apcs"`
	Records       int           `json:"records"`
	Subscribers   int           `json:"subscribers"`
	PauseOffset   time.Duration `json:"pause_offset_ns"`
	PauseDuration time.Duration `json:"pause_duration_ns"`
	Phase         string        `json:"phase"`
	Anchor        int64         `json:"anchor_ns"`
}

func (workload ssnmWorkload) enabled() bool { return workload.Rate > 0 }

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
	flagSet.Uint64Var(&config.Rate, "ssnm-rate", 0, "SSNM load: generated DUNA/DAVA messages per second (0 disables the SSNM workload)")
	flagSet.IntVar(&config.APCs, "ssnm-apcs", 1, "SSNM load: Affected Point Codes per generated message (1 to 1024)")
	flagSet.IntVar(&config.Records, "ssnm-records", ssnmDefaultRecords, "SSNM load: distinct destinations cycled and retained per association")
	flagSet.IntVar(&config.Subscribers, "subscribers", 0, "SSNM load (ASP): SSNM subscriptions, default 8 when SSNM load is on")
	flagSet.Var(pauseFlag{pause: &config.Pause}, "pause-subscriber", "SSNM load (ASP): pause subscriber 0 at <offset>/<duration> into the measurement window, then Resync")
}

// validateSSNMConfig applies the SSNM defaults and bounds after the rest of
// the configuration has been validated.
func validateSSNMConfig(flagSet *flag.FlagSet, config *commandConfig) error {
	explicit := make(map[string]bool)
	flagSet.Visit(func(set *flag.Flag) { explicit[set.Name] = true })
	ssnm := &config.SSNM
	if !ssnm.enabled() {
		for _, name := range []string{"ssnm-apcs", "ssnm-records", "subscribers", "pause-subscriber"} {
			if explicit[name] {
				return fmt.Errorf("-%s requires -ssnm-rate", name)
			}
		}
		*ssnm = ssnmConfig{}
		return nil
	}
	switch {
	case ssnm.Rate > ssnmMaxRate:
		return fmt.Errorf("ssnm-rate must not exceed %d", ssnmMaxRate)
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
		if explicit["subscribers"] || explicit["pause-subscriber"] {
			return errors.New("-subscribers and -pause-subscriber configure the ASP subscribers and are not SGP flags")
		}
		return nil
	}
	if ssnm.Subscribers == 0 {
		ssnm.Subscribers = ssnmDefaultSubscribers
	}
	if ssnm.Subscribers < 1 || ssnm.Subscribers > ssnmMaxSubscribers {
		return fmt.Errorf("subscribers must be between 1 and %d", ssnmMaxSubscribers)
	}
	if ssnm.Pause.enabled() {
		if ssnm.Subscribers < 2 {
			return errors.New("pause-subscriber needs at least two subscribers so one stays healthy")
		}
		if ssnm.Pause.Offset >= config.Duration || ssnm.Pause.Duration >= config.Duration-ssnm.Pause.Offset {
			return errors.New("pause-subscriber offset plus duration must end inside the measurement window")
		}
	}
	messages, err := scheduledMessages(ssnm.Rate, config.Duration)
	if err != nil {
		return fmt.Errorf("ssnm-rate: %w", err)
	}
	receiptBytes := uint64(ssnm.healthySubscribers()) * uint64(config.Associations) * (messages + 1) * 4
	if receiptBytes > ssnmMaxReceiptBytes {
		return fmt.Errorf("SSNM receipt storage of %d bytes exceeds %d; reduce associations, subscribers, rate or duration", receiptBytes, ssnmMaxReceiptBytes)
	}
	return nil
}

// workload is the cohort specification this configuration declares for one
// phase. The anchor is filled in by the sender run.
func (config ssnmConfig) workload(phase string, anchor int64) ssnmWorkload {
	return ssnmWorkload{
		Rate:          config.Rate,
		APCs:          config.APCs,
		Records:       config.Records,
		Subscribers:   config.Subscribers,
		PauseOffset:   config.Pause.Offset,
		PauseDuration: config.Pause.Duration,
		Phase:         phase,
		Anchor:        anchor,
	}
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
