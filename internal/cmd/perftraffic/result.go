package main

import "time"

const (
	verdictPass          = "pass"
	verdictInvalid       = "invalid"
	verdictInconclusive  = "inconclusive"
	baselineFixtureScope = "baseline fixture validity only; not independent-peer or candidate acceptance"
)

type runSpec struct {
	Cohort       string        `json:"cohort"`
	Seed         uint64        `json:"seed"`
	Associations int           `json:"associations"`
	Expected     uint64        `json:"expected"`
	Duration     time.Duration `json:"duration_ns"`
	Drain        time.Duration `json:"drain_ns,omitempty"`
	Rate         uint64        `json:"rate,omitempty"`
	// Outstanding is the run's scheduled-but-unfinished send limit. It is
	// carried in the specification rather than taken from each side's own
	// flags so that both directions of a bidirectional run are bounded
	// identically and report the same manifest.
	Outstanding int                `json:"outstanding"`
	Payload     workload           `json:"payload,omitempty"`
	Mode        string             `json:"mode,omitempty"`
	Direction   string             `json:"direction,omitempty"`
	Initiation  string             `json:"initiation,omitempty"`
	PeerControl string             `json:"peer_control,omitempty"`
	Clock       *sharedClockWindow `json:"shared_clock,omitempty"`
	// SSNM is the opt-in SSNM load declaration, nil and omitted when off. A
	// pointer rather than omitzero, which Go 1.23 does not implement.
	SSNM *ssnmWorkload `json:"ssnm,omitempty"`
}

type deliveryResult struct {
	Unique            uint64 `json:"unique"`
	UniqueMeasurement uint64 `json:"unique_measurement"`
	UniqueDrain       uint64 `json:"unique_drain"`
	Missing           uint64 `json:"missing"`
	Duplicate         uint64 `json:"duplicate"`
	Invalid           uint64 `json:"invalid"`
	Reordered         uint64 `json:"reordered"`
	LateAfterStop     uint64 `json:"late_after_stop"`
}

type seriesPoint struct {
	OffsetMillis uint64 `json:"offset_ms"`
	Scheduled    uint64 `json:"scheduled,omitempty"`
	Sent         uint64 `json:"sent,omitempty"`
	Submitted    uint64 `json:"submitted,omitempty"`
	SendErrors   uint64 `json:"send_errors,omitempty"`
	Capped       uint64 `json:"capped,omitempty"`
	Unique       uint64 `json:"unique,omitempty"`
	Missing      uint64 `json:"missing,omitempty"`
	Duplicate    uint64 `json:"duplicate,omitempty"`
	Invalid      uint64 `json:"invalid,omitempty"`
	Outstanding  uint64 `json:"outstanding"`
}

type runRecord struct {
	Side      string  `json:"side"`
	Spec      runSpec `json:"spec"`
	Expected  uint64  `json:"expected"`
	Scheduled uint64  `json:"scheduled,omitempty"`
	Sent      uint64  `json:"sent"`
	Submitted uint64  `json:"submitted"`
	// send_errors and capped are always serialized, including at zero. The
	// acceptance CLI at internal/cmd/perfcapacity requires both and treats an
	// absent counter as invalid input rather than as zero, and a loss-free run
	// is exactly the run whose counters are zero.
	SendErrors                uint64                `json:"send_errors"`
	Capped                    uint64                `json:"capped"`
	OutstandingAtWindowStart  uint64                `json:"outstanding_at_window_start"`
	OutstandingAtWindowEnd    uint64                `json:"outstanding_at_window_end"`
	OutstandingAfterDrain     uint64                `json:"outstanding_after_drain"`
	MeasurementDuration       time.Duration         `json:"measurement_duration_ns"`
	DrainDuration             time.Duration         `json:"drain_duration_ns"`
	Delivery                  deliveryResult        `json:"delivery"`
	SendDuration              durationPercentiles   `json:"send_duration"`
	DispatchLag               durationPercentiles   `json:"dispatch_lag"`
	Series                    []seriesPoint         `json:"series"`
	CPU                       CPUObservation        `json:"cpu"`
	Allocations               AllocationObservation `json:"allocations"`
	NegotiatedOutboundStreams []int                 `json:"negotiated_outbound_streams,omitempty"`
	FatalError                string                `json:"fatal_error,omitempty"`
	Verdict                   string                `json:"verdict"`
	FixtureVerdict            string                `json:"fixture_verdict"`
	CapacityVerdict           string                `json:"capacity_verdict"`
	Reasons                   []string              `json:"reasons,omitempty"`
	AcceptanceScope           string                `json:"acceptance_scope"`
	IndependentPeer           bool                  `json:"independent_peer"`
	UnsupportedModes          map[string]string     `json:"unsupported_modes"`
	Manifest                  fixtureManifest       `json:"manifest"`
	BacklogAssessment         string                `json:"backlog_assessment,omitempty"`
	WindowAlignment           string                `json:"window_alignment"`
	ValidatedPerSecond        float64               `json:"validated_per_second"`
	ProgressObservations      []progressObservation `json:"progress_observations,omitempty"`
	SenderWindow              *windowAccounting     `json:"sender_window,omitempty"`
	OutstandingScope          string                `json:"outstanding_scope"`
	Echo                      *echoResult           `json:"echo,omitempty"`
	ReceiverEcho              *receiverEchoResult   `json:"receiver_echo,omitempty"`
	Reverse                   *runRecord            `json:"reverse,omitempty"`
	ReverseReceiver           *runRecord            `json:"reverse_receiver,omitempty"`
	ReverseError              string                `json:"reverse_error,omitempty"`
	ClockEvidence             *sharedClockEvidence  `json:"shared_clock_evidence,omitempty"`
	ClockBoundary             *sharedClockSnapshot  `json:"shared_clock_boundary,omitempty"`
	SSNM                      *ssnmRecord           `json:"ssnm,omitempty"`
}

type fixtureManifest struct {
	GoVersion string `json:"go_version"`
	GoOS      string `json:"go_os"`
	GoArch    string `json:"go_arch"`
	// VCSRevision is the fixture binary's own head, stamped by the build. It
	// identifies the candidate this run measured.
	VCSRevision string `json:"vcs_revision,omitempty"`
	VCSModified bool   `json:"vcs_modified"`
	// AssessedBaselineRevision is the baseline commit this campaign is
	// assessed against, required by issue #36. It is a different commit from
	// VCSRevision above and the two are never interchangeable: the fixture is
	// built from the candidate, so nothing in the build stamp can name the
	// baseline.
	AssessedBaselineRevision string `json:"assessed_baseline_revision"`
	SCTPModule               string `json:"sctp_module"`
	SCTPVersion              string `json:"sctp_version"`
	GOMAXPROCS               int    `json:"gomaxprocs"`
	SCTPNoDelay              bool   `json:"sctp_nodelay"`
	SCTPSACKDelay            uint32 `json:"sctp_sack_delay_ms"`
	SCTPSACKFrequency        uint32 `json:"sctp_sack_frequency"`
	FlowCount                int    `json:"flow_count"`
	OutstandingLimit         int    `json:"outstanding_limit"`
	Initiation               string `json:"initiation,omitempty"`
	AccountingScope          string `json:"accounting_scope"`
	// SSNMBudgets are the SSNM time budgets an SSNM-loaded ASP judged its
	// run against, omitted otherwise.
	SSNMBudgets *ssnmBudgetsRecord `json:"ssnm_budgets,omitempty"`
}

func (record *runRecord) evaluate() {
	record.AcceptanceScope = baselineFixtureScope
	record.IndependentPeer = false
	if record.WindowAlignment == "" {
		record.WindowAlignment = "receiver window begins on first cohort arrival; sender and receiver monotonic clocks are not treated as synchronized"
	}
	if record.OutstandingScope == "" {
		record.OutstandingScope = "legacy local counters only; not end-to-end boundary observations"
	}
	if record.UnsupportedModes == nil && routedMode(record.Spec.Mode) {
		record.UnsupportedModes = map[string]string{
			"ssnm_or_churn_workload":      "unavailable: the routed workload keeps destinations Available and exercises neither SSNM storms nor reference churn",
			"alternate_or_partial_paths":  "unavailable: the routed workload uses the preferred AS on every path; alternate preference and partial failures are separate correctness cases",
			"independent_peer_validation": "unavailable: both endpoints use this binary",
		}
	}
	if record.UnsupportedModes == nil {
		record.UnsupportedModes = map[string]string{
			"router_or_ssnm_workload":     "unavailable: this fixture does not exercise the existing routing and state APIs",
			"independent_peer_validation": "unavailable: both endpoints use this binary",
		}
	}
	invalid := func(reason string) {
		record.Reasons = append(record.Reasons, reason)
		record.Verdict = verdictInvalid
		record.FixtureVerdict = verdictInvalid
		record.CapacityVerdict = "unavailable"
	}
	if record.FatalError != "" {
		invalid("fatal network fixture error")
	}
	if record.SendErrors != 0 || record.Capped != 0 {
		invalid("scheduled traffic was not submitted loss-free")
	}
	if record.Delivery.Unique != record.Expected || record.Delivery.Missing != 0 {
		invalid("receiver did not validate every delivery before the drain deadline")
	}
	if record.Delivery.Duplicate != 0 || record.Delivery.Invalid != 0 || record.Delivery.Reordered != 0 || record.Delivery.LateAfterStop != 0 {
		invalid("receiver observed duplicate, invalid, reordered, or late traffic")
	}
	if record.OutstandingAtWindowStart != 0 || record.OutstandingAfterDrain != 0 {
		invalid("outstanding work crossed the start boundary or survived the drain")
	}
	if record.Echo != nil {
		if record.Echo.Validated != record.Expected || record.Echo.Capped != 0 ||
			record.Echo.DeadlineExceeded != 0 || record.Echo.Invalid != 0 || record.Echo.OutstandingAfterDrain != 0 {
			invalid("echo requests were not all answered loss-free within the deadline")
		}
	}
	if record.ReceiverEcho != nil && (record.ReceiverEcho.ReplyErrors != 0 || record.ReceiverEcho.RepliesDropped != 0) {
		invalid("echo replies were not submitted loss-free")
	}
	if record.Verdict == verdictInvalid {
		return
	}
	record.FixtureVerdict = verdictPass
	record.CapacityVerdict = "unavailable"
	if record.BacklogAssessment == "insufficient samples" || record.BacklogAssessment == "uncertain" {
		record.Verdict = verdictInconclusive
		record.Reasons = append(record.Reasons, "outstanding series is insufficient for a stable-backlog conclusion")
		return
	}
	if record.CPU.Error != "" || !validIncreasingCPUUsage(record.CPU, record.Expected != 0) {
		record.Verdict = verdictInconclusive
		record.Reasons = append(record.Reasons, "cgroup CPU accounting unavailable")
		return
	}
	if !validNondecreasingCPUCounter(record.CPU, "nr_throttled") {
		record.Verdict = verdictInconclusive
		record.Reasons = append(record.Reasons, "cgroup throttling counter is missing or decreased")
		return
	}
	if record.CPU.After["nr_throttled"]-record.CPU.Before["nr_throttled"] != 0 {
		record.Verdict = verdictInconclusive
		record.Reasons = append(record.Reasons, "cgroup reported CPU throttling")
		return
	}
	record.Verdict = verdictInconclusive
	record.Reasons = append(record.Reasons, "capacity verdict unavailable because fixture tooling has not yet calibrated the paired end-to-end backlog series")
}

func validIncreasingCPUUsage(observation CPUObservation, requireIncrease bool) bool {
	before, beforeOK := observation.Before["usage_usec"]
	after, afterOK := observation.After["usage_usec"]
	return beforeOK && afterOK && (after > before || !requireIncrease && after == before)
}

func validNondecreasingCPUCounter(observation CPUObservation, name string) bool {
	before, beforeOK := observation.Before[name]
	after, afterOK := observation.After[name]
	return beforeOK && afterOK && after >= before
}
