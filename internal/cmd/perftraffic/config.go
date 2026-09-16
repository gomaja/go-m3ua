package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
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
)

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
	flagSet.StringVar(&config.Mode, "mode", "throughput", "measurement mode: throughput, echo, or bidirectional")
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
	if err := flagSet.Parse(arguments); err != nil {
		return commandConfig{}, err
	}
	if flagSet.NArg() != 0 {
		return commandConfig{}, fmt.Errorf("unexpected positional arguments: %s", strings.Join(flagSet.Args(), " "))
	}
	config.Role = strings.ToLower(config.Role)
	config.Transport = strings.ToLower(config.Transport)
	config.Mode = strings.ToLower(config.Mode)
	config.Workload = workload(workloadValue)
	switch config.Mode {
	case modeThroughput, modeEcho, modeBidirectional:
	default:
		return commandConfig{}, fmt.Errorf("mode %q is unavailable; throughput, echo and bidirectional are implemented", config.Mode)
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
	return config, nil
}
