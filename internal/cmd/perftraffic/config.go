package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"
)

const (
	maxAssociations = 32
	maxRunWindow    = 10 * time.Minute
	maxOutstanding  = 8192
	maxOfferedRate  = 1_000_000
)

const (
	modeThroughput    = "throughput"
	modeEcho          = "echo"
	modeBidirectional = "bidirectional"
	// modeRouted sends through Endpoint.MTPTransfer over the fixed optional
	// router topology; modeRoutedDirect is its matched direct control on the
	// same topology and frozen paths through Association.WriteData.
	modeRouted       = "routed"
	modeRoutedDirect = "routed-direct"
)

// routedAssociations is the fixed optional-router topology: 2 SGs x 2 SGPs x
// 2 associations.
const routedAssociations = 8

func routedMode(mode string) bool {
	return mode == modeRouted || mode == modeRoutedDirect
}

const (
	directionASPToSGP = "asp-to-sgp"
	directionSGPToASP = "sgp-to-asp"
)

const (
	initiationASPDial = "asp-dial"
	initiationSGPDial = "sgp-dial"
)

type workload string

const (
	workload128  workload = "128"
	workload512  workload = "512"
	workload4096 workload = "4096"
	workloadMix  workload = "mix"
)

func (workload workload) size(sequence uint64) int {
	switch workload {
	case workload128:
		return 128
	case workload512:
		return 512
	case workload4096:
		return 4096
	case workloadMix:
		position := sequence % 100
		if position < 90 {
			return 128
		}
		if position < 99 {
			return 512
		}
		return 4096
	default:
		return 0
	}
}

type commandConfig struct {
	Role           string
	Transport      string
	Mode           string
	Direction      string
	Initiation     string
	SCTPAddress    string
	LocalAddress   string
	ControlAddress string
	ControlURL     string
	PeerControl    string
	Associations   int
	Workload       workload
	Rate           uint64
	Warmup         time.Duration
	Duration       time.Duration
	Drain          time.Duration
	Cohort         string
	Seed           uint64
	Outstanding    int
	CPUStatPath    string
	SameHostClock  bool
	clockWindow    *sharedClockWindow
	// SSNM is the opt-in SSNM load workload; zero when -ssnm-total-rate is unset.
	SSNM ssnmConfig
	// ssnmPhase names the cohort phase an SSNM cohort declares; empty is the
	// measurement cohort.
	ssnmPhase string
	// ssnmRun is the ASP's SSNM subscriber run, nil without SSNM load.
	ssnmRun *ssnmSenderRun
	// OverloadProfile turns a throughput sender into a DATA overload trial;
	// overload is its parsed form and overloadRole the role of the cohort
	// being run (warm-up or measurement).
	OverloadProfile string
	overload        *overloadProfile
	overloadRole    string
}

func parseConfig(arguments []string) (commandConfig, error) {
	flagSet := flag.NewFlagSet("perftraffic", flag.ContinueOnError)
	flagSet.SetOutput(io.Discard)
	return parseConfigWithFlagSet(flagSet, arguments)
}

func parseConfigWithFlagSet(flagSet *flag.FlagSet, arguments []string) (commandConfig, error) {
	config := commandConfig{}
	var workloadValue string
	flagSet.StringVar(&config.Role, "role", "sgp", "M3UA endpoint role: asp or sgp")
	flagSet.StringVar(&config.Transport, "transport", "listen", "SCTP initiation: listen or dial")
	flagSet.StringVar(&config.Mode, "mode", "throughput", "measurement mode: throughput, echo, bidirectional, routed, or routed-direct")
	flagSet.StringVar(&config.SCTPAddress, "sctp-address", "0.0.0.0:2905", "listen address or remote dial address")
	flagSet.StringVar(&config.LocalAddress, "local-address", "", "optional local SCTP dial address")
	flagSet.StringVar(&config.ControlAddress, "control-address", "0.0.0.0:8080", "receiver HTTP control address")
	flagSet.StringVar(&config.ControlURL, "control-url", "", "advertised HTTP control base URL for the peer (required for the bidirectional ASP)")
	flagSet.StringVar(&config.PeerControl, "peer-control", "", "receiver HTTP control base URL")
	flagSet.IntVar(&config.Associations, "associations", 1, "number of SCTP associations")
	flagSet.StringVar(&workloadValue, "payload", string(workload128), "payload workload: 128, 512, 4096, or mix")
	flagSet.Uint64Var(&config.Rate, "rate", 25000, "scheduled messages per second")
	flagSet.DurationVar(&config.Warmup, "warmup", 10*time.Second, "warmup duration")
	flagSet.DurationVar(&config.Duration, "duration", 2*time.Minute, "measurement duration")
	flagSet.DurationVar(&config.Drain, "drain", 2*time.Second, "post-measurement drain deadline")
	flagSet.StringVar(&config.Cohort, "cohort", "", "measurement cohort identifier")
	flagSet.Uint64Var(&config.Seed, "seed", 1, "deterministic workload seed")
	flagSet.IntVar(&config.Outstanding, "outstanding", maxOutstanding, "maximum scheduled but unfinished sends")
	flagSet.StringVar(&config.CPUStatPath, "cpu-stat", "/sys/fs/cgroup/cpu.stat", "cgroup v2 cpu.stat path")
	flagSet.BoolVar(&config.SameHostClock, "same-host-clock", false, "verify a shared Linux monotonic clock for throughput measurement")
	registerSSNMFlags(flagSet, &config.SSNM)
	flagSet.StringVar(&config.OverloadProfile, "overload-profile", "", "DATA overload trial: comma-separated MULTIPLIERx:DURATION phases of -rate, e.g. 2x:60s,0.5x:60s (ASP sender, throughput mode)")
	if err := flagSet.Parse(arguments); err != nil {
		return commandConfig{}, err
	}
	durationSet := false
	flagSet.Visit(func(set *flag.Flag) {
		if set.Name == "duration" {
			durationSet = true
		}
	})
	if flagSet.NArg() != 0 {
		return commandConfig{}, fmt.Errorf("unexpected positional arguments: %s", strings.Join(flagSet.Args(), " "))
	}
	config.Role = strings.ToLower(config.Role)
	config.Transport = strings.ToLower(config.Transport)
	config.Mode = strings.ToLower(config.Mode)
	config.Workload = workload(workloadValue)
	if config.SameHostClock && config.Mode == modeEcho {
		return commandConfig{}, errors.New("same-host clock mode supports throughput and bidirectional measurement, not echo")
	}
	switch config.Mode {
	case modeThroughput, modeEcho, modeBidirectional, modeRouted, modeRoutedDirect:
	default:
		return commandConfig{}, fmt.Errorf("mode %q is unavailable; throughput, echo, bidirectional, routed and routed-direct are implemented", config.Mode)
	}
	config.Direction = directionASPToSGP
	if config.Role != "asp" && config.Role != "sgp" {
		return commandConfig{}, fmt.Errorf("unsupported M3UA role %q", config.Role)
	}
	if config.Transport != "dial" && config.Transport != "listen" {
		return commandConfig{}, fmt.Errorf("unsupported SCTP initiation %q", config.Transport)
	}
	// SCTP initiation is recorded separately from the M3UA role: the run is
	// asp-dial when the ASP process dials, sgp-dial when the SGP process
	// dials. Each process derives it from its own role and transport.
	config.Initiation = initiationASPDial
	if (config.Role == "sgp") == (config.Transport == "dial") {
		config.Initiation = initiationSGPDial
	}
	if config.Associations < 1 || config.Associations > maxAssociations {
		return commandConfig{}, fmt.Errorf("associations must be between 1 and %d", maxAssociations)
	}
	if config.Workload.size(0) == 0 {
		return commandConfig{}, fmt.Errorf("unsupported payload workload %q", workloadValue)
	}
	if config.Rate == 0 {
		return commandConfig{}, errors.New("rate must be greater than zero")
	}
	if config.Rate > maxOfferedRate {
		return commandConfig{}, fmt.Errorf("rate must not exceed %d", maxOfferedRate)
	}
	if config.OverloadProfile != "" {
		if err := applyOverloadProfile(&config, durationSet); err != nil {
			return commandConfig{}, err
		}
	}
	if config.Warmup < 0 || config.Duration <= 0 || config.Drain < 0 {
		return commandConfig{}, errors.New("warmup and drain must be non-negative and duration must be positive")
	}
	if config.Warmup > maxRunWindow || config.Duration > maxRunWindow || config.Drain > maxRunWindow ||
		config.Warmup > maxRunWindow-config.Duration || config.Drain > (maxRunWindow-config.Warmup-config.Duration)/2 {
		return commandConfig{}, fmt.Errorf("warmup, duration, and drain exceed %s", maxRunWindow)
	}
	if config.Outstanding < 1 || config.Outstanding > maxOutstanding {
		return commandConfig{}, fmt.Errorf("outstanding must be between 1 and %d", maxOutstanding)
	}
	if config.SCTPAddress == "" {
		return commandConfig{}, errors.New("sctp-address is required")
	}
	for name, value := range map[string]string{"peer-control": config.PeerControl, "control-url": config.ControlURL} {
		if err := validateControlBaseURL(value); err != nil {
			return commandConfig{}, fmt.Errorf("%s: %w", name, err)
		}
	}
	if config.Role == "asp" {
		if config.PeerControl == "" {
			return commandConfig{}, errors.New("peer-control is required for the ASP sender")
		}
		if config.Cohort == "" {
			return commandConfig{}, errors.New("cohort is required for the ASP sender")
		}
		if config.Mode == modeBidirectional && config.ControlURL == "" {
			return commandConfig{}, errors.New("control-url is required for the bidirectional ASP so the peer can drive the reverse cohort")
		}
	}
	if config.Role == "sgp" && config.ControlAddress == "" {
		return commandConfig{}, errors.New("control-address is required for the SGP receiver")
	}
	if routedMode(config.Mode) {
		if err := validateRoutedConfig(config); err != nil {
			return commandConfig{}, err
		}
	}
	if err := validateSSNMConfig(flagSet, &config); err != nil {
		return commandConfig{}, err
	}
	return config, nil
}

// validateRoutedConfig bounds the routed modes to their one approved shape
// (performance budgets section 2): eight associations from one ASP to four SGP
// endpoints, the mixed payload, and the ASP dialling. The four SGP endpoints
// listen on four consecutive ports of one concrete address, and the ASP binds
// one concrete local address, because the fixture pairs every association by
// its exact transport addresses.
func validateRoutedConfig(config commandConfig) error {
	if config.Associations != routedAssociations {
		return fmt.Errorf("mode %s requires exactly %d associations (2 SGs x 2 SGPs x 2 associations)", config.Mode, routedAssociations)
	}
	if config.Initiation != initiationASPDial {
		return fmt.Errorf("mode %s supports asp-dial initiation only: the ASP dials and the SGP listens", config.Mode)
	}
	if _, _, err := splitRoutedAddress(config.SCTPAddress); err != nil {
		return fmt.Errorf("sctp-address: %w", err)
	}
	if config.Role == "sgp" {
		_, err := routedPeerAddresses(config.SCTPAddress)
		return err
	}
	if config.Workload != workloadMix {
		return fmt.Errorf("mode %s requires the mix payload", config.Mode)
	}
	if config.LocalAddress == "" {
		return fmt.Errorf("mode %s requires local-address: one concrete local IP with port 0", config.Mode)
	}
	if _, err := routedLocalAddress(config.LocalAddress); err != nil {
		return fmt.Errorf("local-address: %w", err)
	}
	return nil
}

// validateControlBaseURL bounds a control base URL to the shape the fixture
// builds requests from: scheme://host with the operation appended. Rejecting
// anything else keeps a configured destination comparable by value, so the
// receiver can pin the one reverse destination it is allowed to drive. An
// empty value is not configured and is checked by the caller that needs it.
func validateControlBaseURL(value string) error {
	if value == "" {
		return nil
	}
	parsed, err := url.Parse(value)
	if err != nil {
		return fmt.Errorf("%q is not a URL: %w", value, err)
	}
	switch {
	case parsed.Scheme != "http" && parsed.Scheme != "https":
		return fmt.Errorf("%q must use the http or https scheme", value)
	case parsed.Host == "":
		return fmt.Errorf("%q must name a host", value)
	case parsed.User != nil:
		return fmt.Errorf("%q must not carry credentials", value)
	case parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "":
		return fmt.Errorf("%q must be a scheme and host only, with no path, query or fragment", value)
	}
	return nil
}

// applyOverloadProfile validates an overload trial's configuration and fixes
// its measurement window to the concatenated phases. The drain is raised to
// outlast the two-second request deadline by the drain margin, so every
// request completes or is refused before the drain deadline.
func applyOverloadProfile(config *commandConfig, durationSet bool) error {
	if config.Role != "asp" {
		return errors.New("overload-profile is an ASP sender flag; the receiver learns the profile from each cohort's run specification")
	}
	if config.Mode != modeThroughput {
		return fmt.Errorf("overload-profile runs in throughput mode only, not %s", config.Mode)
	}
	if config.SSNM.enabled() {
		return errors.New(overloadSSNMRefusal)
	}
	profile, err := parseOverloadProfile(config.OverloadProfile, config.Rate)
	if err != nil {
		return err
	}
	if durationSet && config.Duration != profile.duration() {
		return fmt.Errorf("duration %s contradicts the overload profile's %s; omit -duration with -overload-profile", config.Duration, profile.duration())
	}
	config.Duration = profile.duration()
	config.Drain = max(config.Drain, overloadRequestDeadline+overloadDrainMargin)
	config.overload = profile
	return nil
}
