package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"time"
)

const mebibyte = uint64(1) << 20

// Pinned queue and accounting limits. The contract names the first four; the
// rest are pinned here so the manifest records every bounded queue the run
// could fill.
const (
	ssnmMaxBytes              = 16 << 20
	subscriptionQueueSize     = 256
	subscriptionByteCap       = 1 << 20
	dataQueueSize             = 1024
	pendingRecoveryTotalBytes = 16 << 20
	mtpIndicationQueueSize    = 256
	maxAffectedPointCodes     = 1024
	establishTimeout          = 10 * time.Second
)

// Process limits of performance-budgets.md section 4.
const (
	steadyLiveHeapLimit   = 256 * mebibyte
	steadyRSSLimit        = 512 * mebibyte
	overloadLiveHeapLimit = 512 * mebibyte
	overloadRSSLimit      = 1024 * mebibyte
	retainedAbsoluteSlack = 8 * mebibyte
	retainedPercentSlack  = 5
	retainWindow          = 60 * time.Second
	requiredBlocks        = 5
	finalBlocksChecked    = 3
	requiredChurnCycles   = 1000
	requiredChurnRate     = 4.0
	rssInterval           = time.Second
	heapInterval          = 10 * time.Second
	ledgerPayloadSize     = 128
	overloadPayloadSize   = 4096
)

// contractDataRate is section 4's "mixed traffic at 50% of its target": the
// section 2 deterministic mix targets 40,000 msg/s aggregate.
const (
	contractDataRate   = 20000
	defaultReverseRate = 320
	maxDataRate        = 100000
)

const (
	roleASP  = "asp"
	rolePeer = "peer"
)

type commandConfig struct {
	Role              string
	SCTPAddress       string
	LocalIP           string
	ControlAddress    string
	PeerControl       string
	Routes            int
	Steady            time.Duration
	DataRate          int
	ReverseRate       int
	Payload           string
	OverloadHold      time.Duration
	OverloadExtra     int
	SSNMToggles       int
	Blocks            int
	BlockCycles       int
	ChurnRate         float64
	ChurnGroup        int
	ChurnHold         time.Duration
	AcceptConcurrency int
	ImageDigest       string
	Label             string
}

func (config commandConfig) totalCycles() int { return config.Blocks * config.BlockCycles }

func parseConfig(arguments []string) (commandConfig, error) {
	flagSet := flag.NewFlagSet("perfchurn", flag.ContinueOnError)
	flagSet.SetOutput(io.Discard)
	config := commandConfig{}
	flagSet.StringVar(&config.Role, "role", "", "process role: asp (library process under measurement) or peer (SGP side)")
	flagSet.StringVar(&config.SCTPAddress, "sctp-address", "0.0.0.0:2905", "ASP: SCTP listen address; peer: the ASP address to dial")
	flagSet.StringVar(&config.LocalIP, "local-ip", "", "peer: the local IP every dial binds, with the port chosen per SGP and cycle")
	flagSet.StringVar(&config.ControlAddress, "control-address", "0.0.0.0:8080", "peer: HTTP control listen address")
	flagSet.StringVar(&config.PeerControl, "peer-control", "", "ASP: the peer control base URL, scheme://host:port")
	flagSet.IntVar(&config.Routes, "routes", referenceRouteCount, "ASP: provisioned MTP routes, 0 or 1000")
	flagSet.DurationVar(&config.Steady, "steady", 2*time.Minute, "ASP: steady phase duration")
	flagSet.IntVar(&config.DataRate, "data-rate", contractDataRate, "ASP: ledgered DATA messages/s from the ASP across the stable associations (section 4: the section 2 mix at 50% of 40,000/s)")
	flagSet.IntVar(&config.ReverseRate, "reverse-rate", defaultReverseRate, "ASP: ledgered DATA messages/s from the peer toward the ASP, a probe of the ASP's receive path")
	flagSet.StringVar(&config.Payload, "payload", string(workloadMix), "ASP: ledgered payload sizes: mix (section 2: 90% 128, 9% 512, 1% 4,096 bytes), 128, 512 or 4096")
	flagSet.DurationVar(&config.OverloadHold, "overload-hold", 20*time.Second, "ASP: time the bounded queues are held full")
	flagSet.IntVar(&config.OverloadExtra, "overload-extra", 256, "ASP: 4,096-byte messages sent to each stable association beyond its DATA queue capacity")
	flagSet.IntVar(&config.SSNMToggles, "ssnm-toggles", 320, "ASP: destination state toggles (two reports each) sent while subscribers are paused")
	flagSet.IntVar(&config.Blocks, "blocks", requiredBlocks, "ASP: churn blocks")
	flagSet.IntVar(&config.BlockCycles, "block-cycles", requiredChurnCycles/requiredBlocks, "ASP: establish/activate/close cycles per churn block")
	flagSet.Float64Var(&config.ChurnRate, "churn-rate", requiredChurnRate, "ASP: open-loop churn cycles per second")
	flagSet.IntVar(&config.ChurnGroup, "churn-group", 2, "ASP: cycles released together at each schedule point, so accepts overlap")
	flagSet.DurationVar(&config.ChurnHold, "churn-hold", time.Second, "ASP: how long each churn association stays active before it is closed")
	flagSet.IntVar(&config.AcceptConcurrency, "accept-concurrency", 4, "ASP: concurrent Accept calls on the listener")
	flagSet.StringVar(&config.ImageDigest, "image-digest", "", "container image digest recorded in the manifest")
	flagSet.StringVar(&config.Label, "label", "", "run label recorded in the output")
	if err := flagSet.Parse(arguments); err != nil {
		return commandConfig{}, err
	}
	if flagSet.NArg() != 0 {
		return commandConfig{}, fmt.Errorf("unexpected positional arguments: %s", strings.Join(flagSet.Args(), " "))
	}
	config.Role = strings.ToLower(config.Role)
	return config, config.validate()
}

func (config commandConfig) validate() error {
	if config.SCTPAddress == "" {
		return errors.New("sctp-address is required")
	}
	switch config.Role {
	case roleASP:
		return config.validateASP()
	case rolePeer:
		if config.ControlAddress == "" {
			return errors.New("control-address is required for the peer")
		}
		if ip := net.ParseIP(config.LocalIP); ip == nil || ip.IsUnspecified() {
			return fmt.Errorf("local-ip %q must be one specific IP address: the port is chosen per association", config.LocalIP)
		}
		return nil
	default:
		return fmt.Errorf("role %q is neither %s nor %s", config.Role, roleASP, rolePeer)
	}
}

func (config commandConfig) validateASP() error {
	if err := validateControlBaseURL(config.PeerControl); err != nil {
		return fmt.Errorf("peer-control: %w", err)
	}
	if _, err := parseWorkload(config.Payload); err != nil {
		return err
	}
	switch {
	case config.Routes != 0 && config.Routes != referenceRouteCount:
		return fmt.Errorf("routes must be 0 or %d", referenceRouteCount)
	case config.Steady < time.Second || config.Steady > 30*time.Minute:
		return errors.New("steady must be between 1s and 30m")
	case config.DataRate < stableAssociations || config.DataRate > maxDataRate:
		return fmt.Errorf("data-rate must be between %d (one per stable association) and %d", stableAssociations, maxDataRate)
	case config.ReverseRate < stableAssociations || config.ReverseRate > maxDataRate:
		return fmt.Errorf("reverse-rate must be between %d (one per stable association) and %d", stableAssociations, maxDataRate)
	case config.OverloadHold < time.Second || config.OverloadHold > 5*time.Minute:
		return errors.New("overload-hold must be between 1s and 5m")
	case config.OverloadExtra < 1 || config.OverloadExtra > dataQueueSize:
		return fmt.Errorf("overload-extra must be between 1 and %d", dataQueueSize)
	case config.SSNMToggles <= subscriptionQueueSize/2 || config.SSNMToggles > 4096:
		return fmt.Errorf("ssnm-toggles must exceed %d, so two reports each overflow a %d-event subscription, and not exceed 4096",
			subscriptionQueueSize/2, subscriptionQueueSize)
	case config.Blocks < 1 || config.Blocks > 20:
		return errors.New("blocks must be between 1 and 20")
	case config.BlockCycles < 1:
		return errors.New("block-cycles must be positive")
	case config.totalCycles() > maxChurnCycles:
		return fmt.Errorf("blocks × block-cycles must not exceed %d, the churn port space", maxChurnCycles)
	case !(config.ChurnRate > 0 && config.ChurnRate <= 50):
		return errors.New("churn-rate must be greater than 0 and at most 50 cycles/s")
	case config.ChurnGroup < 1 || config.ChurnGroup > 8:
		return errors.New("churn-group must be between 1 and 8")
	case config.ChurnHold < 0 || config.ChurnHold > 30*time.Second:
		return errors.New("churn-hold must be between 0 and 30s")
	case config.AcceptConcurrency < 1 || config.AcceptConcurrency > 16:
		return errors.New("accept-concurrency must be between 1 and 16")
	}
	return nil
}

// validateControlBaseURL accepts scheme://host[:port] only; operation paths
// are appended by the client.
func validateControlBaseURL(value string) error {
	if value == "" {
		return errors.New("is required")
	}
	parsed, err := url.Parse(value)
	if err != nil {
		return fmt.Errorf("%q is not a URL: %w", value, err)
	}
	switch {
	case parsed.Scheme != "http":
		return fmt.Errorf("%q must use the http scheme", value)
	case parsed.Host == "":
		return fmt.Errorf("%q must name a host", value)
	case parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "":
		return fmt.Errorf("%q must be a scheme and host only", value)
	}
	return nil
}
