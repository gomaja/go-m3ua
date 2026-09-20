// perfcapacity evaluates a bounded capacity search campaign. It reads one
// strict JSON request from standard input: the search parameters, the per-run
// fixture evidence for each probe in execution order, and optionally the five
// validation repetitions at the selected rate. Each probe must have run at
// exactly the rate the predeclared search selected; any deviation is invalid
// input, so a campaign cannot reorder or drop inconvenient probes.
//
// Exit statuses mirror perfratio: 0 pass, 1 fail, 2 inconclusive, 3 invalid
// input. A pass covers only the predeclared search and repetition rules; it
// does not establish environmental validity, latency budgets, CPU budgets or
// independent-peer behavior.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/gomaja/go-m3ua/internal/perfstats"
)

const (
	passingExitStatus = iota
	failingExitStatus
	inconclusiveExitStatus
	invalidInputExitStatus
)

const maximumJSONInputBytes = 16 * 1024 * 1024

const (
	echoEvidenceScope    = "round-trip scheduled-request-to-validated-reply on the sender monotonic clock; never one-way latency"
	echoEvidenceDeadline = 2 * time.Second
)

type request struct {
	Initial     *int      `json:"initial"`
	Maximum     *int      `json:"maximum"`
	MaxProbes   *int      `json:"max_probes"`
	Probes      []rateRun `json:"probes"`
	Repetitions []rateRun `json:"repetitions"`
}

type rateRun struct {
	Rate *int            `json:"rate"`
	Run  json.RawMessage `json:"run"`
}

type probeDecision struct {
	Rate                       int                         `json:"rate"`
	AggregateOfferedRate       uint64                      `json:"aggregate_offered_rate,omitempty"`
	AggregateAchievedRateLower *float64                    `json:"aggregate_achieved_rate_lower,omitempty"`
	AggregateAchievedRateUpper *float64                    `json:"aggregate_achieved_rate_upper,omitempty"`
	Decision                   string                      `json:"decision"`
	Backlog                    string                      `json:"backlog"`
	Reason                     string                      `json:"reason,omitempty"`
	Stall                      *perfstats.StallObservation `json:"stall,omitempty"`
	Directions                 []directionDecision         `json:"directions,omitempty"`
}

type directionDecision struct {
	Direction         string                      `json:"direction"`
	Decision          string                      `json:"decision"`
	Backlog           string                      `json:"backlog"`
	Reason            string                      `json:"reason,omitempty"`
	Stall             *perfstats.StallObservation `json:"stall,omitempty"`
	AchievedRateLower *float64                    `json:"achieved_rate_lower,omitempty"`
	AchievedRateUpper *float64                    `json:"achieved_rate_upper,omitempty"`
}

// runEnvironment is what the acceptance output states about where a campaign
// ran. AssessedBaselineRevision is the baseline commit the campaign assesses
// against; VCSRevision is the fixture binary's own head. They are different
// commits and are reported as separate fields.
type runEnvironment struct {
	GoVersion                string `json:"go_version"`
	GoOS                     string `json:"go_os"`
	GoArch                   string `json:"go_arch"`
	GOMAXPROCS               int    `json:"gomaxprocs"`
	SCTPModule               string `json:"sctp_module"`
	SCTPVersion              string `json:"sctp_version"`
	VCSRevision              string `json:"vcs_revision,omitempty"`
	VCSModified              bool   `json:"vcs_modified"`
	AssessedBaselineRevision string `json:"assessed_baseline_revision"`
}

type response struct {
	Decision             string                  `json:"decision"`
	Environments         []runEnvironment        `json:"environments,omitempty"`
	SearchStatus         perfstats.SearchStatus  `json:"search_status,omitempty"`
	SelectedRate         int                     `json:"selected_rate,omitempty"`
	AggregateOfferedRate uint64                  `json:"aggregate_offered_rate,omitempty"`
	Probes               []perfstats.ProbeRecord `json:"probes,omitempty"`
	ProbeDecisions       []probeDecision         `json:"probe_decisions,omitempty"`
	RepetitionDecisions  []probeDecision         `json:"repetition_decisions,omitempty"`
	Reason               string                  `json:"reason,omitempty"`
	Scope                string                  `json:"scope"`
	Error                string                  `json:"error,omitempty"`
}

const decisionScope = "capacity search and validation-repetition decision only; not absolute achieved-rate, environmental, latency, CPU or independent-peer acceptance"

func main() {
	os.Exit(run(os.Stdin, os.Stdout))
}

func run(input io.Reader, output io.Writer) int {
	decoded, err := decodeRequest(input)
	if err != nil {
		writeInvalidResponse(output, err)
		return invalidInputExitStatus
	}
	result, err := evaluate(decoded)
	if err != nil {
		writeInvalidResponse(output, err)
		return invalidInputExitStatus
	}
	result.Scope = decisionScope
	if err := json.NewEncoder(output).Encode(result); err != nil {
		return invalidInputExitStatus
	}
	switch result.Decision {
	case string(perfstats.Pass):
		return passingExitStatus
	case string(perfstats.Fail):
		return failingExitStatus
	case string(perfstats.Inconclusive):
		return inconclusiveExitStatus
	default:
		return invalidInputExitStatus
	}
}

func evaluate(decoded request) (response, error) {
	maximum := perfstats.DefaultMaximumRate
	if decoded.Maximum != nil {
		maximum = *decoded.Maximum
	}
	maxProbes := perfstats.DefaultMaxProbes
	if decoded.MaxProbes != nil {
		maxProbes = *decoded.MaxProbes
	}
	search, err := perfstats.NewCapacitySearch(*decoded.Initial, maximum, maxProbes)
	if err != nil {
		return response{}, err
	}

	result := response{ProbeDecisions: []probeDecision{}, RepetitionDecisions: []probeDecision{}}
	var campaign campaignIdentity
	for index, probe := range decoded.Probes {
		fixture, err := fixtureRunFromJSON(probe.Run, *probe.Rate)
		if err != nil {
			return response{}, fmt.Errorf("probe %d run: %w", index+1, err)
		}
		if err := campaign.add(fixture.identity); err != nil {
			return response{}, fmt.Errorf("probe %d run: %w", index+1, err)
		}
		decision := decideFixtureRun(fixture, *probe.Rate)
		result.ProbeDecisions = append(result.ProbeDecisions, decision)
		if err := search.Record(*probe.Rate, perfstats.ProbeOutcome(decision.Decision)); err != nil {
			return response{}, fmt.Errorf("probe %d: %w", index+1, err)
		}
	}
	result.SearchStatus = search.Status()
	result.Probes = search.Probes()

	var repetitionRates []int
	var repetitionDecisions []perfstats.Decision
	for index, repetition := range decoded.Repetitions {
		fixture, err := fixtureRunFromJSON(repetition.Run, *repetition.Rate)
		if err != nil {
			return response{}, fmt.Errorf("repetition %d run: %w", index+1, err)
		}
		if err := campaign.add(fixture.identity); err != nil {
			return response{}, fmt.Errorf("repetition %d run: %w", index+1, err)
		}
		decision := decideFixtureRun(fixture, *repetition.Rate)
		result.RepetitionDecisions = append(result.RepetitionDecisions, decision)
		repetitionRates = append(repetitionRates, *repetition.Rate)
		repetitionDecisions = append(repetitionDecisions, perfstats.Decision(decision.Decision))
	}

	result.Environments = campaign.environments()
	capacity := perfstats.DecideCapacity(search, repetitionRates, repetitionDecisions)
	result.Decision = string(capacity.Decision)
	result.SelectedRate = capacity.SelectedRate
	if campaign.set && campaign.workload.Mode == "bidirectional" && capacity.SelectedRate > 0 {
		aggregate, ok := checkedDouble(uint64(capacity.SelectedRate))
		if !ok {
			return response{}, errors.New("selected aggregate offered rate overflows uint64")
		}
		result.AggregateOfferedRate = aggregate
	}
	result.Reason = capacity.Reason
	return result, nil
}

func decideFixtureRun(fixture fixtureRun, rate int) probeDecision {
	forward := perfstats.DecideRun(fixture.forward)
	result := probeDecision{
		Rate: rate, Decision: string(forward.Decision), Backlog: string(forward.Backlog),
		Reason: forward.Reason, Stall: forward.Stall,
	}
	if fixture.reverse == nil {
		return result
	}
	reverse := perfstats.DecideRun(*fixture.reverse)
	result.AggregateOfferedRate = fixture.aggregateOfferedRate
	if fixture.aggregateAchieved != nil {
		result.AggregateAchievedRateLower = &fixture.aggregateAchieved.lower
		result.AggregateAchievedRateUpper = &fixture.aggregateAchieved.upper
	}
	forwardDirection := directionDecision{
		Direction: "asp-to-sgp", Decision: string(forward.Decision), Backlog: string(forward.Backlog), Reason: forward.Reason, Stall: forward.Stall,
	}
	if fixture.forwardAchieved != nil {
		forwardDirection.AchievedRateLower = &fixture.forwardAchieved.lower
		forwardDirection.AchievedRateUpper = &fixture.forwardAchieved.upper
	}
	reverseDirection := directionDecision{
		Direction: "sgp-to-asp", Decision: string(reverse.Decision), Backlog: string(reverse.Backlog), Reason: reverse.Reason, Stall: reverse.Stall,
	}
	if fixture.reverseAchieved != nil {
		reverseDirection.AchievedRateLower = &fixture.reverseAchieved.lower
		reverseDirection.AchievedRateUpper = &fixture.reverseAchieved.upper
	}
	result.Directions = []directionDecision{forwardDirection, reverseDirection}
	switch {
	case forward.Stall != nil && forward.Stall.Stalled() || reverse.Stall != nil && reverse.Stall.Stalled():
		result.Decision = string(perfstats.Inconclusive)
		result.Reason = "bidirectional-direction-inconclusive"
	case fixture.cohortError:
		result.Decision = string(perfstats.Fail)
		result.Reason = "bidirectional-cohort-error"
	case forward.Decision == perfstats.Fail || reverse.Decision == perfstats.Fail:
		result.Decision = string(perfstats.Fail)
		result.Reason = "bidirectional-direction-failed"
	case forward.Decision == perfstats.Inconclusive || reverse.Decision == perfstats.Inconclusive:
		result.Decision = string(perfstats.Inconclusive)
		result.Reason = "bidirectional-direction-inconclusive"
	default:
		result.Decision = string(perfstats.Pass)
		result.Reason = ""
	}
	result.Backlog = ""
	result.Stall = nil
	return result
}

// fixtureEvidence is the subset of one perftraffic sender or receiver record
// that the decision consumes. Unknown fields are ignored because the fixture
// emits a superset; every evidence field this command relies on is checked for
// presence explicitly.
type fixtureEvidence struct {
	NegotiatedOutboundStreams []int                 `json:"negotiated_outbound_streams"`
	Side                      *string               `json:"side"`
	Spec                      *fixtureSpec          `json:"spec"`
	Expected                  *uint64               `json:"expected"`
	Scheduled                 *uint64               `json:"scheduled"`
	Sent                      *uint64               `json:"sent"`
	Submitted                 *uint64               `json:"submitted"`
	FixtureVerdict            *string               `json:"fixture_verdict"`
	Capped                    *uint64               `json:"capped"`
	SendErrors                *uint64               `json:"send_errors"`
	OutstandingAtWindowStart  *uint64               `json:"outstanding_at_window_start"`
	OutstandingAtWindowEnd    *uint64               `json:"outstanding_at_window_end"`
	OutstandingAfterDrain     *uint64               `json:"outstanding_after_drain"`
	MeasurementDuration       *time.Duration        `json:"measurement_duration_ns"`
	DrainDuration             *time.Duration        `json:"drain_duration_ns"`
	FatalError                string                `json:"fatal_error"`
	Verdict                   *string               `json:"verdict"`
	Delivery                  *deliveryEvidence     `json:"delivery"`
	SenderWindow              *senderWindowEvidence `json:"sender_window"`
	Echo                      *struct {
		Scope                 *string        `json:"scope"`
		Deadline              *time.Duration `json:"deadline_ns"`
		OutstandingLimit      *int           `json:"outstanding_limit"`
		Requests              *uint64        `json:"requests"`
		Validated             *uint64        `json:"validated"`
		Capped                *uint64        `json:"capped"`
		DeadlineExceeded      *uint64        `json:"deadline_exceeded"`
		Invalid               *uint64        `json:"invalid"`
		OutstandingAfterDrain *uint64        `json:"outstanding_after_drain"`
	} `json:"echo"`
	ReceiverEcho *struct {
		Replies        *uint64 `json:"replies"`
		ReplyErrors    *uint64 `json:"reply_errors"`
		RepliesDropped *uint64 `json:"replies_dropped"`
	} `json:"receiver_echo"`
	SendDuration *struct {
		Max *time.Duration `json:"max_ns"`
	} `json:"send_duration"`
	Manifest           *fixtureManifest     `json:"manifest"`
	ClockEvidence      *sharedClockEvidence `json:"shared_clock_evidence"`
	ClockBoundary      *sharedClockBoundary `json:"shared_clock_boundary"`
	ValidatedPerSecond *float64             `json:"validated_per_second"`
}

type deliveryEvidence struct {
	Unique            *uint64 `json:"unique"`
	UniqueMeasurement *uint64 `json:"unique_measurement"`
	UniqueDrain       *uint64 `json:"unique_drain"`
	Missing           *uint64 `json:"missing"`
	Duplicate         *uint64 `json:"duplicate"`
	Invalid           *uint64 `json:"invalid"`
	Reordered         *uint64 `json:"reordered"`
	LateAfterStop     *uint64 `json:"late_after_stop"`
}

type senderWindowEvidence struct {
	Status           *string        `json:"status"`
	Reason           *string        `json:"reason"`
	Duration         *time.Duration `json:"duration_ns"`
	DeliveredLower   *uint64        `json:"delivered_lower"`
	DeliveredUpper   *uint64        `json:"delivered_upper"`
	OutstandingLower *uint64        `json:"outstanding_lower"`
	OutstandingUpper *uint64        `json:"outstanding_upper"`
	RateLower        *float64       `json:"rate_lower"`
	RateUpper        *float64       `json:"rate_upper"`
	BacklogChange    *struct {
		Status      string   `json:"status"`
		SampleCount int      `json:"sample_count"`
		Lower       *float64 `json:"mean_change_lower"`
		Upper       *float64 `json:"mean_change_upper"`
	} `json:"backlog_change"`
}

type sharedClockDomain struct {
	Clock         *string `json:"clock"`
	BootID        *string `json:"boot_id"`
	TimeNamespace *string `json:"time_namespace"`
	Resolution    *int64  `json:"resolution_ns"`
}

type sharedClockWindow struct {
	Domain *sharedClockDomain `json:"domain"`
	Start  *int64             `json:"start_ns"`
	End    *int64             `json:"end_ns"`
}

type sharedClockEvidence struct {
	Before   *sharedClockDomain   `json:"before"`
	After    *sharedClockDomain   `json:"after"`
	Verified *bool                `json:"verified"`
	Watchdog *sharedClockWatchdog `json:"watchdog"`
}

type sharedClockWatchdog struct {
	Before          *int64 `json:"before_ns"`
	After           *int64 `json:"after_ns"`
	Target          *int64 `json:"target_ns"`
	MaximumLateness *int64 `json:"maximum_lateness_ns"`
	Budget          *int64 `json:"budget_ns"`
}

type sharedClockBoundary struct {
	Domain           *sharedClockDomain `json:"domain"`
	Captured         *int64             `json:"captured_ns"`
	MeasurementLower *uint64            `json:"measurement_lower"`
	MeasurementUpper *uint64            `json:"measurement_upper"`
}

type fixtureCohort struct {
	Phase           *string          `json:"phase"`
	Sender          *fixtureEvidence `json:"sender"`
	Receiver        *fixtureEvidence `json:"receiver"`
	ReverseSender   *fixtureEvidence `json:"reverse_sender"`
	ReverseReceiver *fixtureEvidence `json:"reverse_receiver"`
	Verdict         *string          `json:"verdict"`
	Error           string           `json:"error"`
}

type fixtureSpec struct {
	Cohort       *string            `json:"cohort"`
	Seed         *uint64            `json:"seed"`
	Associations *int               `json:"associations"`
	Expected     *uint64            `json:"expected"`
	Duration     *time.Duration     `json:"duration_ns"`
	Drain        time.Duration      `json:"drain_ns"`
	Rate         *uint64            `json:"rate"`
	Outstanding  *int               `json:"outstanding"`
	Payload      *string            `json:"payload"`
	Mode         *string            `json:"mode"`
	Direction    *string            `json:"direction"`
	Initiation   *string            `json:"initiation"`
	PeerControl  string             `json:"peer_control"`
	SharedClock  *sharedClockWindow `json:"shared_clock"`
}

type fixtureManifest struct {
	GoVersion                *string `json:"go_version"`
	GoOS                     *string `json:"go_os"`
	GoArch                   *string `json:"go_arch"`
	VCSRevision              *string `json:"vcs_revision"`
	VCSModified              *bool   `json:"vcs_modified"`
	AssessedBaselineRevision *string `json:"assessed_baseline_revision"`
	SCTPModule               *string `json:"sctp_module"`
	SCTPVersion              *string `json:"sctp_version"`
	GOMAXPROCS               *int    `json:"gomaxprocs"`
	SCTPNoDelay              *bool   `json:"sctp_nodelay"`
	SCTPSACKDelay            *uint32 `json:"sctp_sack_delay_ms"`
	SCTPSACKFrequency        *uint32 `json:"sctp_sack_frequency"`
	FlowCount                *int    `json:"flow_count"`
	OutstandingLimit         *int    `json:"outstanding_limit"`
	Initiation               *string `json:"initiation"`
	AccountingScope          *string `json:"accounting_scope"`
}

// workloadIdentity excludes the per-run cohort and seed, the offered rate,
// and expected, which is derived from that rate and the fixed duration. Every
// other run specification field must remain identical within one campaign.
type workloadIdentity struct {
	Associations    int
	Duration        time.Duration
	Drain           time.Duration
	Outstanding     int
	Payload         string
	Mode            string
	Direction       string
	Initiation      string
	PeerControl     string
	Instrumentation string
}

type fixtureRun struct {
	forward              perfstats.RunEvidence
	reverse              *perfstats.RunEvidence
	identity             runIdentity
	aggregateOfferedRate uint64
	forwardAchieved      *achievedRateBounds
	reverseAchieved      *achievedRateBounds
	aggregateAchieved    *achievedRateBounds
	cohortError          bool
}

type achievedRateBounds struct {
	lower float64
	upper float64
}

type campaignEnvironment struct {
	reported          runEnvironment
	SCTPNoDelay       bool
	SCTPSACKDelay     uint32
	SCTPSACKFrequency uint32
	FlowCount         int
	OutstandingLimit  int
	Initiation        string
	AccountingScope   string
}

type runIdentity struct {
	workload       workloadIdentity
	environment    campaignEnvironment
	streams        string
	reverseStreams string
}

type campaignIdentity struct {
	set            bool
	workload       workloadIdentity
	environment    campaignEnvironment
	streams        string
	reverseStreams string
}

func (campaign *campaignIdentity) add(run runIdentity) error {
	if !campaign.set {
		campaign.set = true
		campaign.workload = run.workload
		campaign.environment = run.environment
		campaign.streams = run.streams
		campaign.reverseStreams = run.reverseStreams
		return nil
	}
	if run.environment.reported.VCSRevision != campaign.environment.reported.VCSRevision {
		return fmt.Errorf("candidate vcs_revision %q does not match campaign revision %q",
			run.environment.reported.VCSRevision, campaign.environment.reported.VCSRevision)
	}
	if run.environment.reported.AssessedBaselineRevision != campaign.environment.reported.AssessedBaselineRevision {
		return fmt.Errorf("assessed_baseline_revision %q does not match campaign baseline %q",
			run.environment.reported.AssessedBaselineRevision, campaign.environment.reported.AssessedBaselineRevision)
	}
	if run.workload != campaign.workload {
		return errors.New("run workload does not match campaign workload")
	}
	if run.streams != campaign.streams {
		return errors.New("negotiated outbound streams do not match campaign inventory")
	}
	if run.reverseStreams != campaign.reverseStreams {
		return errors.New("reverse negotiated outbound streams do not match campaign inventory")
	}
	if run.environment != campaign.environment {
		return errors.New("run environment does not match campaign environment")
	}
	return nil
}

func (campaign campaignIdentity) environments() []runEnvironment {
	if !campaign.set {
		return nil
	}
	return []runEnvironment{campaign.environment.reported}
}

// fixtureRunFromJSON accepts the sender record used by throughput and echo
// campaigns, or the complete measurement cohort emitted for bidirectional
// campaigns.
func fixtureRunFromJSON(raw json.RawMessage, declaredRate int) (fixtureRun, error) {
	if len(raw) == 0 {
		return fixtureRun{}, errors.New("run evidence is required")
	}
	var shape struct {
		Sender json.RawMessage `json:"sender"`
	}
	if err := json.Unmarshal(raw, &shape); err != nil {
		return fixtureRun{}, fmt.Errorf("decode run evidence: %w", err)
	}
	if len(shape.Sender) != 0 {
		return bidirectionalFixtureRun(raw, declaredRate)
	}
	evidence, identity, err := evidenceFromFixture(raw, declaredRate)
	if err != nil {
		return fixtureRun{}, err
	}
	return fixtureRun{forward: evidence, identity: identity}, nil
}

// evidenceFromFixture maps one per-run fixture sender record to the
// predeclared decision inputs and to the environment the run happened in. A
// missing or unbounded sender window, an insufficient-sample backlog change,
// or absent interval bounds is missing evidence: the run is inconclusive,
// never a pass. It is never an error.
//
// The transport-stall signal and the manifest are required rather than
// optional. A record without send_duration.max_ns cannot show whether a stall
// contaminated it, and a record without a manifest cannot say where it ran or
// which baseline it was assessed against; neither may be silently treated as
// absent.
func evidenceFromFixture(raw json.RawMessage, declaredRate int) (perfstats.RunEvidence, runIdentity, error) {
	if len(raw) == 0 {
		return perfstats.RunEvidence{}, runIdentity{}, errors.New("run evidence is required")
	}
	var record fixtureEvidence
	if err := json.Unmarshal(raw, &record); err != nil {
		return perfstats.RunEvidence{}, runIdentity{}, fmt.Errorf("decode run evidence: %w", err)
	}
	return evidenceFromSenderRecord(&record, declaredRate, false)
}

func evidenceFromSenderRecord(record *fixtureEvidence, declaredRate int, completeCohort bool) (perfstats.RunEvidence, runIdentity, error) {
	if record.Spec == nil {
		return perfstats.RunEvidence{}, runIdentity{}, errors.New("spec is required to identify the measured run")
	}
	workload, err := workloadFromSpec(record.Spec, declaredRate)
	if err != nil {
		return perfstats.RunEvidence{}, runIdentity{}, err
	}
	if record.MeasurementDuration == nil || *record.MeasurementDuration != workload.Duration {
		return perfstats.RunEvidence{}, runIdentity{}, errors.New("measurement_duration_ns must equal the workload duration")
	}
	if record.SenderWindow != nil && (record.SenderWindow.Duration == nil || *record.SenderWindow.Duration != workload.Duration) {
		return perfstats.RunEvidence{}, runIdentity{}, errors.New("sender_window.duration_ns must equal the workload duration")
	}
	if len(record.NegotiatedOutboundStreams) != workload.Associations {
		return perfstats.RunEvidence{}, runIdentity{}, errors.New("negotiated_outbound_streams must identify every workload association")
	}
	for _, count := range record.NegotiatedOutboundStreams {
		if count <= 0 {
			return perfstats.RunEvidence{}, runIdentity{}, errors.New("negotiated_outbound_streams counts must be positive")
		}
	}
	streams, err := json.Marshal(record.NegotiatedOutboundStreams)
	if err != nil {
		return perfstats.RunEvidence{}, runIdentity{}, fmt.Errorf("encode negotiated stream inventory: %w", err)
	}
	if workload.Mode == "bidirectional" && !completeCohort {
		return perfstats.RunEvidence{}, runIdentity{}, errors.New("bidirectional capacity evidence requires a complete cohort contract")
	}
	if completeCohort && record.ClockBoundary != nil {
		return perfstats.RunEvidence{}, runIdentity{}, errors.New("bidirectional sender record must not carry receiver boundary evidence")
	}
	if record.Expected == nil || *record.Expected != *record.Spec.Expected {
		return perfstats.RunEvidence{}, runIdentity{}, errors.New("record expected must equal workload spec.expected")
	}
	if err := validateFixtureValidity(record, workload.Mode); err != nil {
		return perfstats.RunEvidence{}, runIdentity{}, err
	}
	if record.SendDuration == nil || record.SendDuration.Max == nil {
		return perfstats.RunEvidence{}, runIdentity{}, errors.New("send_duration.max_ns is required as the transport-stall signal")
	}
	if *record.SendDuration.Max < 0 {
		return perfstats.RunEvidence{}, runIdentity{}, errors.New("send_duration.max_ns must not be negative")
	}
	environment, err := environmentFromManifest(record.Manifest)
	if err != nil {
		return perfstats.RunEvidence{}, runIdentity{}, err
	}
	if workload.Outstanding != environment.OutstandingLimit || workload.Initiation != environment.Initiation {
		return perfstats.RunEvidence{}, runIdentity{}, errors.New("run environment outstanding_limit and initiation must agree with the workload spec")
	}
	identity := runIdentity{workload: workload, environment: environment, streams: string(streams)}

	evidence := perfstats.RunEvidence{
		FixtureValid: *record.FixtureVerdict == "pass",
		Stall:        &perfstats.StallObservation{LongestSend: *record.SendDuration.Max},
		Counters: perfstats.RunCounters{
			Missing:       *record.Delivery.Missing,
			Duplicate:     *record.Delivery.Duplicate,
			Invalid:       *record.Delivery.Invalid,
			Reordered:     *record.Delivery.Reordered,
			LateAfterStop: *record.Delivery.LateAfterStop,
			Capped:        *record.Capped,
			SendErrors:    *record.SendErrors,
		},
	}
	if record.Echo != nil {
		evidence.Counters.Capped += *record.Echo.Capped
		evidence.Counters.DeadlineExceeded = *record.Echo.DeadlineExceeded
	}
	if record.Spec.SharedClock != nil {
		if _, err := validateSharedSenderRecord(record); err != nil {
			return perfstats.RunEvidence{}, runIdentity{}, err
		}
	}

	window := record.SenderWindow
	if window == nil || window.Status == nil || *window.Status != "bounded" ||
		window.BacklogChange == nil || window.BacklogChange.Status == "insufficient-samples" ||
		window.BacklogChange.SampleCount < 8 ||
		window.BacklogChange.Lower == nil || window.BacklogChange.Upper == nil {
		return evidence, identity, nil
	}
	evidence.Interval = &perfstats.BacklogInterval{
		Lower: *window.BacklogChange.Lower,
		Upper: *window.BacklogChange.Upper,
	}
	return evidence, identity, nil
}

type clockIdentity struct {
	Clock         string
	BootID        string
	TimeNamespace string
	Resolution    int64
	Start         int64
	End           int64
}

type specIdentity struct {
	Cohort   string
	Seed     uint64
	Expected uint64
	Rate     uint64
	Workload workloadIdentity
	Clock    clockIdentity
}

func bidirectionalFixtureRun(raw json.RawMessage, declaredRate int) (fixtureRun, error) {
	var cohort fixtureCohort
	if err := json.Unmarshal(raw, &cohort); err != nil {
		return fixtureRun{}, fmt.Errorf("decode bidirectional cohort: %w", err)
	}
	if cohort.Phase == nil || *cohort.Phase != "measurement" {
		return fixtureRun{}, errors.New("bidirectional cohort phase must be measurement")
	}
	if cohort.Sender == nil || cohort.Receiver == nil || cohort.ReverseSender == nil || cohort.ReverseReceiver == nil {
		return fixtureRun{}, errors.New("bidirectional cohort requires sender, receiver, reverse_sender and reverse_receiver records")
	}
	if cohort.Verdict == nil {
		return fixtureRun{}, errors.New("bidirectional cohort verdict is required")
	}

	forwardEvidence, forwardIdentity, err := evidenceFromSenderRecord(cohort.Sender, declaredRate, true)
	if err != nil {
		return fixtureRun{}, fmt.Errorf("forward sender: %w", err)
	}
	reverseEvidence, reverseIdentity, err := evidenceFromSenderRecord(cohort.ReverseSender, declaredRate, true)
	if err != nil {
		return fixtureRun{}, fmt.Errorf("reverse sender: %w", err)
	}
	forwardSpec, err := completeSpecIdentity(cohort.Sender.Spec, declaredRate)
	if err != nil {
		return fixtureRun{}, fmt.Errorf("forward sender: %w", err)
	}
	reverseSpec, err := completeSpecIdentity(cohort.ReverseSender.Spec, declaredRate)
	if err != nil {
		return fixtureRun{}, fmt.Errorf("reverse sender: %w", err)
	}
	forwardReceiverSpec, err := validateBidirectionalReceiver(cohort.Receiver, declaredRate)
	if err != nil {
		return fixtureRun{}, fmt.Errorf("forward receiver: %w", err)
	}
	reverseReceiverSpec, err := validateBidirectionalReceiver(cohort.ReverseReceiver, declaredRate)
	if err != nil {
		return fixtureRun{}, fmt.Errorf("reverse receiver: %w", err)
	}
	forwardEvidence.FixtureValid = forwardEvidence.FixtureValid && *cohort.Receiver.FixtureVerdict == "pass"
	reverseEvidence.FixtureValid = reverseEvidence.FixtureValid && *cohort.ReverseReceiver.FixtureVerdict == "pass"
	if forwardSpec != forwardReceiverSpec {
		return fixtureRun{}, errors.New("forward sender and receiver specs do not match")
	}
	if reverseSpec != reverseReceiverSpec {
		return fixtureRun{}, errors.New("reverse sender and receiver specs do not match")
	}
	if forwardSpec.Workload.Mode != "bidirectional" || forwardSpec.Workload.Direction != "asp-to-sgp" {
		return fixtureRun{}, errors.New("forward bidirectional record must use bidirectional mode and asp-to-sgp direction")
	}
	if reverseSpec.Workload.Mode != "throughput" || reverseSpec.Workload.Direction != "sgp-to-asp" {
		return fixtureRun{}, errors.New("reverse bidirectional record must use throughput mode and sgp-to-asp direction")
	}
	if forwardSpec.Workload.PeerControl == "" || reverseSpec.Workload.PeerControl != "" {
		return fixtureRun{}, errors.New("forward peer_control is required and reverse peer_control must be omitted")
	}
	if reverseSpec.Cohort != forwardSpec.Cohort+"-reverse" {
		return fixtureRun{}, errors.New("reverse cohort must use the forward cohort with the -reverse suffix")
	}
	if forwardSpec.Seed != reverseSpec.Seed || forwardSpec.Expected != reverseSpec.Expected || forwardSpec.Rate != reverseSpec.Rate ||
		forwardSpec.Clock != reverseSpec.Clock || !sameBidirectionalWorkload(forwardSpec.Workload, reverseSpec.Workload) {
		return fixtureRun{}, errors.New("forward and reverse workload identity does not match")
	}
	if forwardSpec.Clock.Clock == "" {
		return fixtureRun{}, errors.New("bidirectional capacity evidence requires a shared clock window")
	}
	if forwardIdentity.environment != reverseIdentity.environment {
		return fixtureRun{}, errors.New("forward and reverse sender environments do not match")
	}
	forwardIdentity.reverseStreams = reverseIdentity.streams
	if !equalDelivery(cohort.Sender.Delivery, cohort.Receiver.Delivery) ||
		!equalDelivery(cohort.ReverseSender.Delivery, cohort.ReverseReceiver.Delivery) {
		return fixtureRun{}, errors.New("sender and receiver delivery evidence does not match")
	}
	forwardAchieved, err := validateBidirectionalWindowPair(cohort.Sender, cohort.Receiver)
	if err != nil {
		return fixtureRun{}, fmt.Errorf("forward window: %w", err)
	}
	reverseAchieved, err := validateBidirectionalWindowPair(cohort.ReverseSender, cohort.ReverseReceiver)
	if err != nil {
		return fixtureRun{}, fmt.Errorf("reverse window: %w", err)
	}
	if err := validateCohortVerdict(&cohort); err != nil {
		return fixtureRun{}, err
	}
	aggregateOffered, ok := checkedDouble(uint64(declaredRate))
	if !ok {
		return fixtureRun{}, errors.New("aggregate offered rate overflows uint64")
	}
	if _, ok := checkedAdd(forwardSpec.Expected, reverseSpec.Expected); !ok {
		return fixtureRun{}, errors.New("aggregate expected count overflows uint64")
	}
	var aggregateAchieved *achievedRateBounds
	if forwardAchieved != nil && reverseAchieved != nil {
		aggregateDeliveredLower, ok := checkedAdd(*cohort.Sender.SenderWindow.DeliveredLower, *cohort.ReverseSender.SenderWindow.DeliveredLower)
		if !ok {
			return fixtureRun{}, errors.New("aggregate measurement-window lower count overflows uint64")
		}
		aggregateDeliveredUpper, ok := checkedAdd(*cohort.Sender.SenderWindow.DeliveredUpper, *cohort.ReverseSender.SenderWindow.DeliveredUpper)
		if !ok {
			return fixtureRun{}, errors.New("aggregate measurement-window upper count overflows uint64")
		}
		aggregateAchieved = &achievedRateBounds{
			lower: float64(aggregateDeliveredLower) / forwardSpec.Workload.Duration.Seconds(),
			upper: float64(aggregateDeliveredUpper) / forwardSpec.Workload.Duration.Seconds(),
		}
	}
	for _, pair := range [][2]uint64{
		{*cohort.Sender.Delivery.UniqueMeasurement, *cohort.ReverseSender.Delivery.UniqueMeasurement},
		{*cohort.Sender.Delivery.UniqueDrain, *cohort.ReverseSender.Delivery.UniqueDrain},
	} {
		if _, ok := checkedAdd(pair[0], pair[1]); !ok {
			return fixtureRun{}, errors.New("aggregate delivery count overflows uint64")
		}
	}
	return fixtureRun{
		forward: forwardEvidence, reverse: &reverseEvidence, identity: forwardIdentity,
		aggregateOfferedRate: aggregateOffered, forwardAchieved: forwardAchieved,
		reverseAchieved: reverseAchieved, aggregateAchieved: aggregateAchieved,
		cohortError: cohort.Error != "",
	}, nil
}

func sameBidirectionalWorkload(forward, reverse workloadIdentity) bool {
	return forward.Associations == reverse.Associations && forward.Duration == reverse.Duration &&
		forward.Drain == reverse.Drain && forward.Outstanding == reverse.Outstanding && forward.Payload == reverse.Payload &&
		forward.Initiation == reverse.Initiation && forward.Instrumentation == reverse.Instrumentation
}

func completeSpecIdentity(spec *fixtureSpec, declaredRate int) (specIdentity, error) {
	workload, err := workloadFromSpec(spec, declaredRate)
	if err != nil {
		return specIdentity{}, err
	}
	clock, err := clockFromSpec(spec)
	if err != nil {
		return specIdentity{}, err
	}
	return specIdentity{
		Cohort: *spec.Cohort, Seed: *spec.Seed, Expected: *spec.Expected, Rate: *spec.Rate,
		Workload: workload, Clock: clock,
	}, nil
}

func validateBidirectionalReceiver(record *fixtureEvidence, declaredRate int) (specIdentity, error) {
	if record.Spec == nil {
		return specIdentity{}, errors.New("spec is required")
	}
	spec, err := completeSpecIdentity(record.Spec, declaredRate)
	if err != nil {
		return specIdentity{}, err
	}
	if record.Side == nil || *record.Side != "receiver" {
		return specIdentity{}, errors.New("record must be a receiver record")
	}
	if nonzero(record.Scheduled) || nonzero(record.Sent) || nonzero(record.Submitted) || record.SenderWindow != nil ||
		record.Echo != nil || record.ReceiverEcho != nil {
		return specIdentity{}, errors.New("bidirectional receiver record carries sender-only or echo evidence")
	}
	if record.Expected == nil || *record.Expected != spec.Expected {
		return specIdentity{}, errors.New("record expected must equal workload spec.expected")
	}
	if err := validateReceiverValidity(record); err != nil {
		return specIdentity{}, err
	}
	if err := validateMeasuredDurations(record); err != nil {
		return specIdentity{}, err
	}
	if err := validateClockEvidence(record.ClockEvidence, spec.Clock); err != nil {
		return specIdentity{}, err
	}
	if record.ClockBoundary == nil || record.ClockBoundary.Domain == nil || record.ClockBoundary.Captured == nil ||
		record.ClockBoundary.MeasurementLower == nil || record.ClockBoundary.MeasurementUpper == nil {
		return specIdentity{}, errors.New("receiver shared_clock_boundary fields are required")
	}
	domain, err := clockDomainIdentity(record.ClockBoundary.Domain)
	if err != nil || domain != clockDomain(spec.Clock) {
		return specIdentity{}, errors.New("receiver shared_clock_boundary domain does not match the run window")
	}
	if *record.ClockBoundary.Captured <= spec.Clock.Resolution || *record.ClockBoundary.Captured > math.MaxInt64-spec.Clock.Resolution {
		return specIdentity{}, errors.New("receiver shared_clock_boundary captured_ns is invalid")
	}
	if *record.ClockBoundary.MeasurementLower > *record.ClockBoundary.MeasurementUpper ||
		*record.ClockBoundary.MeasurementLower > *record.Delivery.UniqueMeasurement ||
		*record.Delivery.UniqueMeasurement > *record.ClockBoundary.MeasurementUpper ||
		*record.ClockBoundary.MeasurementUpper > *record.Delivery.Unique {
		return specIdentity{}, errors.New("receiver shared_clock_boundary counts are invalid")
	}
	wantRate := float64(*record.ClockBoundary.MeasurementLower) / record.Spec.Duration.Seconds()
	if record.ValidatedPerSecond == nil || math.IsNaN(*record.ValidatedPerSecond) || math.IsInf(*record.ValidatedPerSecond, 0) ||
		*record.ValidatedPerSecond != wantRate {
		return specIdentity{}, errors.New("receiver validated_per_second must equal shared_clock_boundary.measurement_lower over the measurement duration")
	}
	return spec, nil
}

func nonzero(value *uint64) bool {
	return value != nil && *value != 0
}

func validateReceiverValidity(record *fixtureEvidence) error {
	if record.FixtureVerdict == nil || record.Verdict == nil {
		return errors.New("receiver fixture_verdict and verdict are required")
	}
	if record.Capped == nil || record.SendErrors == nil || record.OutstandingAtWindowStart == nil ||
		record.OutstandingAtWindowEnd == nil || record.OutstandingAfterDrain == nil {
		return errors.New("receiver failure and outstanding counters are required")
	}
	if err := validateDelivery(record.Delivery); err != nil {
		return err
	}
	if !sumEquals(*record.Delivery.Unique, *record.Delivery.UniqueMeasurement, *record.Delivery.UniqueDrain) {
		return errors.New("delivery unique_measurement and unique_drain do not reconcile with delivery.unique")
	}
	if (*record.FixtureVerdict == "pass" || record.FatalError == "") &&
		!sumEquals(*record.Expected, *record.Delivery.Unique, *record.Delivery.Missing) {
		return errors.New("delivery unique and missing do not reconcile with the expected workload")
	}
	if *record.FixtureVerdict != "pass" && *record.FixtureVerdict != "invalid" {
		return errors.New("fixture_verdict must be pass or invalid")
	}
	invalid := record.FatalError != "" || *record.Capped != 0 || *record.SendErrors != 0 ||
		*record.Delivery.Unique != *record.Expected || *record.Delivery.Missing != 0 || *record.Delivery.Duplicate != 0 ||
		*record.Delivery.Invalid != 0 || *record.Delivery.Reordered != 0 || *record.Delivery.LateAfterStop != 0 ||
		*record.OutstandingAtWindowStart != 0 || *record.OutstandingAfterDrain != 0
	if (*record.FixtureVerdict == "invalid") != invalid {
		return errors.New("fixture_verdict contradicts the fixture validity counters")
	}
	return validateRecordVerdict(record)
}

func validateDelivery(delivery *deliveryEvidence) error {
	if delivery == nil || delivery.Unique == nil || delivery.UniqueMeasurement == nil || delivery.UniqueDrain == nil ||
		delivery.Missing == nil || delivery.Duplicate == nil || delivery.Invalid == nil || delivery.Reordered == nil || delivery.LateAfterStop == nil {
		return errors.New("delivery counters are required")
	}
	return nil
}

func validateRecordVerdict(record *fixtureEvidence) error {
	if record.Verdict == nil {
		return errors.New("record verdict is required")
	}
	switch *record.Verdict {
	case "pass", "inconclusive":
		if record.FixtureVerdict == nil || *record.FixtureVerdict != "pass" {
			return errors.New("record verdict contradicts fixture_verdict")
		}
	case "invalid":
		if record.FixtureVerdict == nil || *record.FixtureVerdict != "invalid" {
			return errors.New("record verdict contradicts fixture_verdict")
		}
	default:
		return errors.New("record verdict must be pass, inconclusive or invalid")
	}
	return nil
}

func validateMeasuredDurations(record *fixtureEvidence) error {
	if record.MeasurementDuration == nil || record.DrainDuration == nil || record.OutstandingAtWindowEnd == nil {
		return errors.New("measurement duration, drain duration and end-window outstanding count are required")
	}
	if *record.MeasurementDuration != *record.Spec.Duration || *record.DrainDuration < 0 {
		return errors.New("record measurement or drain duration is invalid")
	}
	if record.FixtureVerdict != nil && *record.FixtureVerdict == "pass" && *record.DrainDuration > record.Spec.Drain {
		return errors.New("passing record exceeded its drain deadline")
	}
	return nil
}

func validateSharedSenderRecord(record *fixtureEvidence) (*achievedRateBounds, error) {
	clock, err := clockFromSpec(record.Spec)
	if err != nil {
		return nil, err
	}
	if err := validateClockEvidence(record.ClockEvidence, clock); err != nil {
		return nil, err
	}
	if err := validateSenderWatchdog(record.ClockEvidence.Watchdog, clock, record.Spec.Drain); err != nil {
		return nil, err
	}
	if record.SenderWindow == nil || record.SenderWindow.Status == nil || record.SenderWindow.Duration == nil ||
		record.SenderWindow.DeliveredLower == nil || record.SenderWindow.DeliveredUpper == nil ||
		record.SenderWindow.OutstandingLower == nil || record.SenderWindow.OutstandingUpper == nil ||
		record.SenderWindow.RateLower == nil || record.SenderWindow.RateUpper == nil {
		return nil, errors.New("shared-clock sender_window accounting fields are required")
	}
	if err := validateMeasuredDurations(record); err != nil {
		return nil, err
	}
	window := record.SenderWindow
	if *window.Duration != *record.Spec.Duration {
		return nil, errors.New("sender_window duration does not match the workload duration")
	}
	if *window.Status == "inconclusive" {
		if window.Reason == nil || *window.Reason == "" || *window.DeliveredLower != 0 || *window.DeliveredUpper != 0 ||
			*window.OutstandingLower != 0 || *window.OutstandingUpper != 0 || *window.RateLower != 0 || *window.RateUpper != 0 ||
			record.ValidatedPerSecond == nil || *record.ValidatedPerSecond != 0 || window.BacklogChange == nil ||
			window.BacklogChange.Status != "" || window.BacklogChange.SampleCount != 0 || window.BacklogChange.Lower == nil ||
			window.BacklogChange.Upper == nil || *window.BacklogChange.Lower != 0 || *window.BacklogChange.Upper != 0 {
			return nil, errors.New("inconclusive sender_window does not match unavailable producer accounting")
		}
		return nil, nil
	}
	if *window.Status != "bounded" {
		return nil, errors.New("sender_window status must be bounded or inconclusive")
	}
	if window.Reason != nil && *window.Reason != "" {
		return nil, errors.New("bounded sender_window must not carry an unavailable reason")
	}
	if *window.DeliveredLower > *window.DeliveredUpper || *window.DeliveredUpper > *record.Expected ||
		!sumEquals(*record.Expected, *window.DeliveredUpper, *window.OutstandingLower) ||
		!sumEquals(*record.Expected, *window.DeliveredLower, *window.OutstandingUpper) {
		return nil, errors.New("sender_window delivery and outstanding bounds are inconsistent")
	}
	durationSeconds := record.Spec.Duration.Seconds()
	wantLower := float64(*window.DeliveredLower) / durationSeconds
	wantUpper := float64(*window.DeliveredUpper) / durationSeconds
	if math.IsNaN(*window.RateLower) || math.IsNaN(*window.RateUpper) || math.IsInf(*window.RateLower, 0) || math.IsInf(*window.RateUpper, 0) ||
		*window.RateLower != wantLower || *window.RateUpper != wantUpper || *window.RateLower > *window.RateUpper {
		return nil, errors.New("sender_window achieved-rate bounds are inconsistent")
	}
	if record.ValidatedPerSecond == nil || *record.ValidatedPerSecond != *window.RateLower {
		return nil, errors.New("validated_per_second must equal sender_window.rate_lower")
	}
	return &achievedRateBounds{lower: *window.RateLower, upper: *window.RateUpper}, nil
}

func validateBidirectionalWindowPair(sender, receiver *fixtureEvidence) (*achievedRateBounds, error) {
	bounds, err := validateSharedSenderRecord(sender)
	if err != nil {
		return nil, err
	}
	if bounds == nil {
		return nil, nil
	}
	boundary := receiver.ClockBoundary
	if boundary == nil || boundary.Captured == nil || boundary.MeasurementLower == nil || boundary.MeasurementUpper == nil ||
		*boundary.MeasurementLower != *sender.SenderWindow.DeliveredLower ||
		*boundary.MeasurementUpper != *sender.SenderWindow.DeliveredUpper {
		return nil, errors.New("sender_window bounds do not match receiver shared_clock_boundary")
	}
	clock, err := clockFromSpec(sender.Spec)
	if err != nil {
		return nil, err
	}
	if *boundary.Captured < clock.End+clock.Resolution {
		return nil, errors.New("bounded sender window requires a resolution-safe post-window receiver capture")
	}
	if receiver.ValidatedPerSecond == nil || *receiver.ValidatedPerSecond != bounds.lower {
		return nil, errors.New("receiver validated_per_second must equal the conservative achieved rate")
	}
	return bounds, nil
}

func validateCohortVerdict(cohort *fixtureCohort) error {
	verdict := "pass"
	for _, record := range []*fixtureEvidence{cohort.Sender, cohort.Receiver, cohort.ReverseSender, cohort.ReverseReceiver} {
		if err := validateRecordVerdict(record); err != nil {
			return err
		}
		if *record.Verdict == "invalid" {
			verdict = "invalid"
		} else if *record.Verdict == "inconclusive" && verdict == "pass" {
			verdict = "inconclusive"
		}
	}
	if cohort.Error != "" {
		verdict = "invalid"
	}
	if *cohort.Verdict != verdict {
		return errors.New("cohort verdict contradicts its records or error")
	}
	return nil
}

func equalDelivery(first, second *deliveryEvidence) bool {
	if validateDelivery(first) != nil || validateDelivery(second) != nil {
		return false
	}
	return *first.Unique == *second.Unique && *first.UniqueMeasurement == *second.UniqueMeasurement &&
		*first.UniqueDrain == *second.UniqueDrain && *first.Missing == *second.Missing &&
		*first.Duplicate == *second.Duplicate && *first.Invalid == *second.Invalid &&
		*first.Reordered == *second.Reordered && *first.LateAfterStop == *second.LateAfterStop
}

func clockFromSpec(spec *fixtureSpec) (clockIdentity, error) {
	if spec == nil || spec.SharedClock == nil {
		return clockIdentity{}, nil
	}
	window := spec.SharedClock
	if window.Domain == nil || window.Start == nil || window.End == nil || spec.Duration == nil {
		return clockIdentity{}, errors.New("shared_clock domain, start_ns and end_ns are required")
	}
	domain, err := clockDomainIdentity(window.Domain)
	if err != nil {
		return clockIdentity{}, err
	}
	if *window.Start <= domain.Resolution || *window.End <= *window.Start || *window.End > math.MaxInt64-domain.Resolution ||
		*window.End-*window.Start != int64(*spec.Duration) {
		return clockIdentity{}, errors.New("shared_clock window is invalid for the run duration")
	}
	domain.Start = *window.Start
	domain.End = *window.End
	return domain, nil
}

func clockDomainIdentity(domain *sharedClockDomain) (clockIdentity, error) {
	if domain == nil || domain.Clock == nil || domain.BootID == nil || domain.TimeNamespace == nil || domain.Resolution == nil {
		return clockIdentity{}, errors.New("shared clock domain fields are required")
	}
	if *domain.Clock != "CLOCK_MONOTONIC" || *domain.BootID == "" || *domain.TimeNamespace == "" ||
		*domain.Resolution <= 0 || *domain.Resolution > int64(time.Second) {
		return clockIdentity{}, errors.New("shared clock domain is invalid")
	}
	return clockIdentity{Clock: *domain.Clock, BootID: *domain.BootID, TimeNamespace: *domain.TimeNamespace, Resolution: *domain.Resolution}, nil
}

func clockDomain(clock clockIdentity) clockIdentity {
	clock.Start = 0
	clock.End = 0
	return clock
}

func validateClockEvidence(evidence *sharedClockEvidence, clock clockIdentity) error {
	if evidence == nil || evidence.Before == nil || evidence.After == nil || evidence.Verified == nil || !*evidence.Verified {
		return errors.New("verified shared_clock_evidence is required")
	}
	before, err := clockDomainIdentity(evidence.Before)
	if err != nil {
		return err
	}
	after, err := clockDomainIdentity(evidence.After)
	if err != nil {
		return err
	}
	if before != clockDomain(clock) || after != clockDomain(clock) {
		return errors.New("shared_clock_evidence domain does not match the run window")
	}
	return nil
}

func validateSenderWatchdog(watchdog *sharedClockWatchdog, clock clockIdentity, drain time.Duration) error {
	if watchdog == nil || watchdog.Before == nil || watchdog.After == nil || watchdog.Target == nil ||
		watchdog.MaximumLateness == nil || watchdog.Budget == nil {
		return errors.New("shared-clock sender watchdog fields are required")
	}
	if drain < 0 || int64(drain) > math.MaxInt64-clock.End {
		return errors.New("shared-clock sender watchdog target overflows")
	}
	target := clock.End + int64(drain)
	if target > math.MaxInt64-clock.Resolution {
		return errors.New("shared-clock sender watchdog target leaves no resolution bound")
	}
	if *watchdog.Before <= 0 || *watchdog.After < *watchdog.Before || *watchdog.After >= target ||
		*watchdog.Target != target || *watchdog.Budget != int64(time.Millisecond) {
		return errors.New("shared-clock sender watchdog is inconsistent with the run deadline")
	}
	uncertainty := 2 * clock.Resolution
	span := *watchdog.After - *watchdog.Before
	if span > math.MaxInt64-uncertainty {
		return errors.New("shared-clock sender watchdog lateness overflows")
	}
	maximumLateness := span + uncertainty
	if *watchdog.MaximumLateness != maximumLateness || maximumLateness > *watchdog.Budget {
		return errors.New("shared-clock sender watchdog exceeds its uncertainty budget")
	}
	return nil
}

func checkedAdd(first, second uint64) (uint64, bool) {
	if second > math.MaxUint64-first {
		return 0, false
	}
	return first + second, true
}

func checkedDouble(value uint64) (uint64, bool) {
	return checkedAdd(value, value)
}

func validateFixtureValidity(record *fixtureEvidence, mode string) error {
	if record.Side == nil || *record.Side != "sender" {
		return errors.New("run evidence must be a sender record")
	}
	if record.FixtureVerdict == nil {
		return errors.New("fixture_verdict is required")
	}
	if *record.FixtureVerdict != "pass" && *record.FixtureVerdict != "invalid" {
		return errors.New("fixture_verdict must be pass or invalid")
	}
	if record.Scheduled == nil || record.Sent == nil || record.Submitted == nil || record.Capped == nil || record.SendErrors == nil ||
		record.OutstandingAtWindowStart == nil || record.OutstandingAfterDrain == nil {
		return errors.New("sender submission and outstanding counters are required")
	}
	if record.Delivery == nil || record.Delivery.Unique == nil || record.Delivery.UniqueMeasurement == nil || record.Delivery.UniqueDrain == nil ||
		record.Delivery.Missing == nil || record.Delivery.Duplicate == nil || record.Delivery.Invalid == nil ||
		record.Delivery.Reordered == nil || record.Delivery.LateAfterStop == nil {
		return errors.New("delivery counters are required")
	}
	if *record.Scheduled != *record.Expected || *record.Sent != *record.Submitted ||
		!sumEquals(*record.Scheduled, *record.Submitted, *record.SendErrors, *record.Capped) {
		return errors.New("sender submission counters do not reconcile with the expected workload")
	}
	if !sumEquals(*record.Delivery.Unique, *record.Delivery.UniqueMeasurement, *record.Delivery.UniqueDrain) {
		return errors.New("delivery unique_measurement and unique_drain do not reconcile with delivery.unique")
	}
	if (*record.FixtureVerdict == "pass" || record.FatalError == "") && !sumEquals(*record.Expected, *record.Delivery.Unique, *record.Delivery.Missing) {
		return errors.New("delivery unique and missing do not reconcile with the expected workload")
	}

	fixtureInvalid := record.FatalError != "" || *record.Capped != 0 || *record.SendErrors != 0 ||
		*record.Delivery.Unique != *record.Expected || *record.Delivery.Missing != 0 || *record.Delivery.Duplicate != 0 ||
		*record.Delivery.Invalid != 0 || *record.Delivery.Reordered != 0 || *record.Delivery.LateAfterStop != 0 ||
		*record.OutstandingAtWindowStart != 0 || *record.OutstandingAfterDrain != 0

	switch mode {
	case "echo":
		if record.Echo == nil || record.Echo.Scope == nil || record.Echo.Deadline == nil || record.Echo.OutstandingLimit == nil || record.Echo.Requests == nil || record.Echo.Validated == nil || record.Echo.Capped == nil || record.Echo.DeadlineExceeded == nil ||
			record.Echo.Invalid == nil || record.Echo.OutstandingAfterDrain == nil {
			return errors.New("echo runs require scope, deadline_ns, outstanding_limit, requests, validated, capped, deadline_exceeded, invalid and outstanding_after_drain counters")
		}
		if *record.Echo.Scope != echoEvidenceScope || *record.Echo.Deadline != echoEvidenceDeadline {
			return errors.New("echo scope and deadline_ns must match the producer contract")
		}
		if *record.Echo.OutstandingLimit != *record.Spec.Outstanding {
			return errors.New("echo outstanding_limit must equal workload spec.outstanding")
		}
		if *record.Echo.Requests != *record.Submitted {
			return errors.New("echo requests must equal sender submissions")
		}
		if record.ReceiverEcho != nil {
			return errors.New("sender records must not carry receiver_echo evidence")
		}
		fixtureInvalid = fixtureInvalid || *record.Echo.Validated != *record.Expected || *record.Echo.Capped != 0 ||
			*record.Echo.DeadlineExceeded != 0 || *record.Echo.Invalid != 0 || *record.Echo.OutstandingAfterDrain != 0
	case "throughput", "bidirectional":
		if record.Echo != nil || record.ReceiverEcho != nil {
			return errors.New("throughput sender records must not carry echo evidence")
		}
	}

	if (*record.FixtureVerdict == "invalid") != fixtureInvalid {
		return errors.New("fixture_verdict contradicts the fixture validity counters")
	}
	return nil
}

func sumEquals(total uint64, parts ...uint64) bool {
	sum := uint64(0)
	for _, part := range parts {
		if part > math.MaxUint64-sum {
			return false
		}
		sum += part
	}
	return sum == total
}

func workloadFromSpec(spec *fixtureSpec, declaredRate int) (workloadIdentity, error) {
	if spec.Rate == nil {
		return workloadIdentity{}, errors.New("spec.rate is required and cannot be null")
	}
	if uint64(declaredRate) != *spec.Rate {
		return workloadIdentity{}, fmt.Errorf("declared rate %d does not match measured spec.rate %d", declaredRate, *spec.Rate)
	}
	if spec.Cohort == nil || *spec.Cohort == "" || spec.Seed == nil || spec.Associations == nil ||
		spec.Expected == nil || spec.Duration == nil || spec.Outstanding == nil ||
		spec.Payload == nil || spec.Mode == nil || spec.Direction == nil || spec.Initiation == nil {
		return workloadIdentity{}, errors.New("spec cohort, seed, associations, expected, duration_ns, rate, outstanding, payload, mode, direction and initiation are required")
	}
	if *spec.Associations <= 0 || *spec.Expected == 0 || *spec.Duration <= 0 || spec.Drain < 0 || *spec.Outstanding <= 0 {
		return workloadIdentity{}, errors.New("spec associations, expected, duration_ns and outstanding must be positive and drain_ns must not be negative")
	}
	if uint64(*spec.Duration) > math.MaxUint64 / *spec.Rate || uint64(*spec.Duration)**spec.Rate/uint64(time.Second) != *spec.Expected {
		return workloadIdentity{}, errors.New("spec.expected does not match the measured rate and duration")
	}
	if *spec.Payload == "" || *spec.Mode == "" || *spec.Direction == "" || *spec.Initiation == "" {
		return workloadIdentity{}, errors.New("spec payload, mode, direction and initiation must not be empty")
	}
	switch *spec.Payload {
	case "128", "512", "4096", "mix":
	default:
		return workloadIdentity{}, errors.New("unsupported payload workload")
	}
	switch *spec.Mode {
	case "throughput", "echo", "bidirectional":
	default:
		return workloadIdentity{}, errors.New("unsupported workload mode")
	}
	switch *spec.Direction {
	case "asp-to-sgp", "sgp-to-asp":
	default:
		return workloadIdentity{}, errors.New("unsupported workload direction")
	}
	switch *spec.Initiation {
	case "asp-dial", "sgp-dial":
	default:
		return workloadIdentity{}, errors.New("unsupported workload initiation")
	}
	instrumentation := "http-progress"
	if spec.SharedClock != nil {
		if _, err := clockFromSpec(spec); err != nil {
			return workloadIdentity{}, err
		}
		instrumentation = "shared-clock"
	}
	return workloadIdentity{
		Associations:    *spec.Associations,
		Duration:        *spec.Duration,
		Drain:           spec.Drain,
		Outstanding:     *spec.Outstanding,
		Payload:         *spec.Payload,
		Mode:            *spec.Mode,
		Direction:       *spec.Direction,
		Initiation:      *spec.Initiation,
		PeerControl:     spec.PeerControl,
		Instrumentation: instrumentation,
	}, nil
}

// environmentFromManifest requires and normalizes every fixture-provided
// environment field. Dirty builds cannot provide exact-head evidence.
func environmentFromManifest(manifest *fixtureManifest) (campaignEnvironment, error) {
	if manifest == nil {
		return campaignEnvironment{}, errors.New("manifest is required to state the run environment")
	}
	if manifest.GoVersion == nil || manifest.GoOS == nil || manifest.GoArch == nil || manifest.VCSRevision == nil ||
		manifest.VCSModified == nil || manifest.AssessedBaselineRevision == nil || manifest.SCTPModule == nil ||
		manifest.SCTPVersion == nil || manifest.GOMAXPROCS == nil || manifest.SCTPNoDelay == nil ||
		manifest.SCTPSACKDelay == nil || manifest.SCTPSACKFrequency == nil || manifest.FlowCount == nil ||
		manifest.OutstandingLimit == nil || manifest.Initiation == nil || manifest.AccountingScope == nil {
		return campaignEnvironment{}, errors.New("manifest environment fields are all required")
	}
	if *manifest.GoVersion == "" || *manifest.GoOS == "" || *manifest.GoArch == "" || *manifest.VCSRevision == "" ||
		*manifest.AssessedBaselineRevision == "" || *manifest.SCTPModule == "" || *manifest.SCTPVersion == "" ||
		*manifest.Initiation == "" || *manifest.AccountingScope == "" {
		return campaignEnvironment{}, errors.New("manifest string environment fields must not be empty")
	}
	if *manifest.VCSModified {
		return campaignEnvironment{}, errors.New("manifest vcs_modified must be false for exact-head acceptance")
	}
	if *manifest.GOMAXPROCS <= 0 || *manifest.SCTPSACKFrequency == 0 || *manifest.FlowCount <= 0 || *manifest.OutstandingLimit <= 0 {
		return campaignEnvironment{}, errors.New("manifest gomaxprocs, sctp_sack_frequency, flow_count and outstanding_limit must be positive")
	}
	return campaignEnvironment{
		reported: runEnvironment{
			GoVersion:                *manifest.GoVersion,
			GoOS:                     *manifest.GoOS,
			GoArch:                   *manifest.GoArch,
			GOMAXPROCS:               *manifest.GOMAXPROCS,
			SCTPModule:               *manifest.SCTPModule,
			SCTPVersion:              *manifest.SCTPVersion,
			VCSRevision:              *manifest.VCSRevision,
			VCSModified:              *manifest.VCSModified,
			AssessedBaselineRevision: *manifest.AssessedBaselineRevision,
		},
		SCTPNoDelay:       *manifest.SCTPNoDelay,
		SCTPSACKDelay:     *manifest.SCTPSACKDelay,
		SCTPSACKFrequency: *manifest.SCTPSACKFrequency,
		FlowCount:         *manifest.FlowCount,
		OutstandingLimit:  *manifest.OutstandingLimit,
		Initiation:        *manifest.Initiation,
		AccountingScope:   *manifest.AccountingScope,
	}, nil
}

func decodeRequest(input io.Reader) (request, error) {
	data, err := io.ReadAll(io.LimitReader(input, maximumJSONInputBytes+1))
	if err != nil {
		return request{}, fmt.Errorf("read request: %w", err)
	}
	if len(data) > maximumJSONInputBytes {
		return request{}, fmt.Errorf("request exceeds %d-byte limit", maximumJSONInputBytes)
	}
	if err := rejectDuplicateJSONKeys(data); err != nil {
		return request{}, fmt.Errorf("decode request: %w", err)
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var decoded request
	if err := decoder.Decode(&decoded); err != nil {
		return request{}, fmt.Errorf("decode request: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return request{}, errors.New("decode request: trailing JSON value")
		}
		return request{}, fmt.Errorf("decode request: %w", err)
	}
	if decoded.Initial == nil {
		return request{}, errors.New("initial is required and cannot be null")
	}
	for index, entry := range append(append([]rateRun(nil), decoded.Probes...), decoded.Repetitions...) {
		if entry.Rate == nil {
			return request{}, fmt.Errorf("entry %d rate is required and cannot be null", index+1)
		}
		if *entry.Rate <= 0 {
			return request{}, fmt.Errorf("entry %d rate must be a positive integer", index+1)
		}
		if len(entry.Run) == 0 {
			return request{}, fmt.Errorf("entry %d run evidence is required", index+1)
		}
	}
	return decoded, nil
}

func rejectDuplicateJSONKeys(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := scanJSONValue(decoder); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("trailing JSON value")
		}
		return err
	}
	return nil
}

func scanJSONValue(decoder *json.Decoder) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delimiter, isDelimiter := token.(json.Delim)
	if !isDelimiter {
		return nil
	}

	switch delimiter {
	case '{':
		keys := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, isString := keyToken.(string)
			if !isString {
				return errors.New("object key is not a string")
			}
			foldedKey := foldJSONKey(key)
			if _, exists := keys[foldedKey]; exists {
				return fmt.Errorf("duplicate object key %q", key)
			}
			keys[foldedKey] = struct{}{}
			if err := scanJSONValue(decoder); err != nil {
				return err
			}
		}
	case '[':
		for decoder.More() {
			if err := scanJSONValue(decoder); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unexpected delimiter %q", delimiter)
	}

	closingToken, err := decoder.Token()
	if err != nil {
		return err
	}
	closingDelimiter, valid := closingToken.(json.Delim)
	if !valid || (delimiter == '{' && closingDelimiter != '}') || (delimiter == '[' && closingDelimiter != ']') {
		return errors.New("mismatched JSON delimiter")
	}
	return nil
}

func foldJSONKey(key string) string {
	var folded strings.Builder
	folded.Grow(len(key))
	for _, character := range key {
		if character < utf8.RuneSelf {
			if 'a' <= character && character <= 'z' {
				character -= 'a' - 'A'
			}
		} else {
			for {
				next := unicode.SimpleFold(character)
				if next <= character {
					character = next
					break
				}
				character = next
			}
		}
		folded.WriteRune(character)
	}
	return folded.String()
}

func writeInvalidResponse(output io.Writer, err error) {
	_ = json.NewEncoder(output).Encode(response{
		Decision: "invalid-input",
		Scope:    decisionScope,
		Error:    err.Error(),
	})
}
