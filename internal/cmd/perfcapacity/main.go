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
	"time"

	"github.com/gomaja/go-m3ua/internal/perfstats"
)

const (
	passingExitStatus = iota
	failingExitStatus
	inconclusiveExitStatus
	invalidInputExitStatus
)

const maximumJSONInputBytes = 16 * 1024 * 1024

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
	Rate     int                         `json:"rate"`
	Decision string                      `json:"decision"`
	Backlog  string                      `json:"backlog"`
	Reason   string                      `json:"reason,omitempty"`
	Stall    *perfstats.StallObservation `json:"stall,omitempty"`
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
	Decision            string                  `json:"decision"`
	Environments        []runEnvironment        `json:"environments,omitempty"`
	SearchStatus        perfstats.SearchStatus  `json:"search_status,omitempty"`
	SelectedRate        int                     `json:"selected_rate,omitempty"`
	Probes              []perfstats.ProbeRecord `json:"probes,omitempty"`
	ProbeDecisions      []probeDecision         `json:"probe_decisions,omitempty"`
	RepetitionDecisions []probeDecision         `json:"repetition_decisions,omitempty"`
	Reason              string                  `json:"reason,omitempty"`
	Scope               string                  `json:"scope"`
	Error               string                  `json:"error,omitempty"`
}

const decisionScope = "capacity search and validation-repetition decision only; not environmental, latency, CPU or independent-peer acceptance"

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
		evidence, identity, err := evidenceFromFixture(probe.Run, *probe.Rate)
		if err != nil {
			return response{}, fmt.Errorf("probe %d run: %w", index+1, err)
		}
		if err := campaign.add(identity); err != nil {
			return response{}, fmt.Errorf("probe %d run: %w", index+1, err)
		}
		decision := perfstats.DecideRun(evidence)
		result.ProbeDecisions = append(result.ProbeDecisions, probeDecision{
			Rate: *probe.Rate, Decision: string(decision.Decision), Backlog: string(decision.Backlog),
			Reason: decision.Reason, Stall: decision.Stall,
		})
		if err := search.Record(*probe.Rate, perfstats.ProbeOutcome(decision.Decision)); err != nil {
			return response{}, fmt.Errorf("probe %d: %w", index+1, err)
		}
	}
	result.SearchStatus = search.Status()
	result.Probes = search.Probes()

	var repetitionRates []int
	var repetitionDecisions []perfstats.Decision
	for index, repetition := range decoded.Repetitions {
		evidence, identity, err := evidenceFromFixture(repetition.Run, *repetition.Rate)
		if err != nil {
			return response{}, fmt.Errorf("repetition %d run: %w", index+1, err)
		}
		if err := campaign.add(identity); err != nil {
			return response{}, fmt.Errorf("repetition %d run: %w", index+1, err)
		}
		decision := perfstats.DecideRun(evidence)
		result.RepetitionDecisions = append(result.RepetitionDecisions, probeDecision{
			Rate: *repetition.Rate, Decision: string(decision.Decision), Backlog: string(decision.Backlog),
			Reason: decision.Reason, Stall: decision.Stall,
		})
		repetitionRates = append(repetitionRates, *repetition.Rate)
		repetitionDecisions = append(repetitionDecisions, decision.Decision)
	}

	result.Environments = campaign.environments()
	capacity := perfstats.DecideCapacity(search, repetitionRates, repetitionDecisions)
	result.Decision = string(capacity.Decision)
	result.SelectedRate = capacity.SelectedRate
	result.Reason = capacity.Reason
	return result, nil
}

// fixtureEvidence is the subset of the perftraffic sender-side JSON record the
// decision consumes. Unknown fields are ignored because the fixture emits a
// superset; every evidence field this command relies on is checked for
// presence explicitly.
type fixtureEvidence struct {
	Spec           *fixtureSpec `json:"spec"`
	FixtureVerdict *string      `json:"fixture_verdict"`
	Capped         *uint64      `json:"capped"`
	SendErrors     *uint64      `json:"send_errors"`
	Delivery       *struct {
		Missing       *uint64 `json:"missing"`
		Duplicate     *uint64 `json:"duplicate"`
		Invalid       *uint64 `json:"invalid"`
		Reordered     *uint64 `json:"reordered"`
		LateAfterStop *uint64 `json:"late_after_stop"`
	} `json:"delivery"`
	SenderWindow *struct {
		Status        *string `json:"status"`
		BacklogChange *struct {
			Status      string   `json:"status"`
			SampleCount int      `json:"sample_count"`
			Lower       *float64 `json:"mean_change_lower"`
			Upper       *float64 `json:"mean_change_upper"`
		} `json:"backlog_change"`
	} `json:"sender_window"`
	Echo *struct {
		Capped           *uint64 `json:"capped"`
		DeadlineExceeded *uint64 `json:"deadline_exceeded"`
	} `json:"echo"`
	SendDuration *struct {
		Max *time.Duration `json:"max_ns"`
	} `json:"send_duration"`
	Manifest *fixtureManifest `json:"manifest"`
}

type fixtureSpec struct {
	Cohort       *string        `json:"cohort"`
	Seed         *uint64        `json:"seed"`
	Associations *int           `json:"associations"`
	Expected     *uint64        `json:"expected"`
	Duration     *time.Duration `json:"duration_ns"`
	Drain        time.Duration  `json:"drain_ns"`
	Rate         *uint64        `json:"rate"`
	Outstanding  *int           `json:"outstanding"`
	Payload      *string        `json:"payload"`
	Mode         *string        `json:"mode"`
	Direction    *string        `json:"direction"`
	Initiation   *string        `json:"initiation"`
	PeerControl  string         `json:"peer_control"`
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
	Associations int
	Duration     time.Duration
	Drain        time.Duration
	Outstanding  int
	Payload      string
	Mode         string
	Direction    string
	Initiation   string
	PeerControl  string
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
	workload    workloadIdentity
	environment campaignEnvironment
}

type campaignIdentity struct {
	set         bool
	workload    workloadIdentity
	environment campaignEnvironment
}

func (campaign *campaignIdentity) add(run runIdentity) error {
	if !campaign.set {
		campaign.set = true
		campaign.workload = run.workload
		campaign.environment = run.environment
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
	if record.Spec == nil {
		return perfstats.RunEvidence{}, runIdentity{}, errors.New("spec is required to identify the measured run")
	}
	workload, err := workloadFromSpec(record.Spec, declaredRate)
	if err != nil {
		return perfstats.RunEvidence{}, runIdentity{}, err
	}
	if record.FixtureVerdict == nil {
		return perfstats.RunEvidence{}, runIdentity{}, errors.New("fixture_verdict is required")
	}
	if record.Capped == nil || record.SendErrors == nil {
		return perfstats.RunEvidence{}, runIdentity{}, errors.New("capped and send_errors are required")
	}
	if record.Delivery == nil || record.Delivery.Missing == nil || record.Delivery.Duplicate == nil ||
		record.Delivery.Invalid == nil || record.Delivery.Reordered == nil || record.Delivery.LateAfterStop == nil {
		return perfstats.RunEvidence{}, runIdentity{}, errors.New("delivery counters are required")
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
	identity := runIdentity{workload: workload, environment: environment}

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
		if record.Echo.Capped == nil || record.Echo.DeadlineExceeded == nil {
			return perfstats.RunEvidence{}, runIdentity{}, errors.New("echo runs require capped and deadline_exceeded counters")
		}
		evidence.Counters.Capped += *record.Echo.Capped
		evidence.Counters.DeadlineExceeded = *record.Echo.DeadlineExceeded
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
	return workloadIdentity{
		Associations: *spec.Associations,
		Duration:     *spec.Duration,
		Drain:        spec.Drain,
		Outstanding:  *spec.Outstanding,
		Payload:      *spec.Payload,
		Mode:         *spec.Mode,
		Direction:    *spec.Direction,
		Initiation:   *spec.Initiation,
		PeerControl:  spec.PeerControl,
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
			if _, exists := keys[key]; exists {
				return fmt.Errorf("duplicate object key %q", key)
			}
			keys[key] = struct{}{}
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

func writeInvalidResponse(output io.Writer, err error) {
	_ = json.NewEncoder(output).Encode(response{
		Decision: "invalid-input",
		Scope:    decisionScope,
		Error:    err.Error(),
	})
}
