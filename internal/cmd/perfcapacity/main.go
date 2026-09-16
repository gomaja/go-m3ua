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
	"os"

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
	Rate     int    `json:"rate"`
	Decision string `json:"decision"`
	Backlog  string `json:"backlog"`
	Reason   string `json:"reason,omitempty"`
}

type response struct {
	Decision            string                  `json:"decision"`
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
	for index, probe := range decoded.Probes {
		evidence, err := evidenceFromFixture(probe.Run)
		if err != nil {
			return response{}, fmt.Errorf("probe %d run: %w", index+1, err)
		}
		decision := perfstats.DecideRun(evidence)
		result.ProbeDecisions = append(result.ProbeDecisions, probeDecision{
			Rate: *probe.Rate, Decision: string(decision.Decision), Backlog: string(decision.Backlog), Reason: decision.Reason,
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
		evidence, err := evidenceFromFixture(repetition.Run)
		if err != nil {
			return response{}, fmt.Errorf("repetition %d run: %w", index+1, err)
		}
		decision := perfstats.DecideRun(evidence)
		result.RepetitionDecisions = append(result.RepetitionDecisions, probeDecision{
			Rate: *repetition.Rate, Decision: string(decision.Decision), Backlog: string(decision.Backlog), Reason: decision.Reason,
		})
		repetitionRates = append(repetitionRates, *repetition.Rate)
		repetitionDecisions = append(repetitionDecisions, decision.Decision)
	}

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
	FixtureVerdict *string `json:"fixture_verdict"`
	Capped         *uint64 `json:"capped"`
	SendErrors     *uint64 `json:"send_errors"`
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
}

// evidenceFromFixture maps one per-run fixture sender record to the
// predeclared decision inputs. A missing or unbounded sender window, an
// insufficient-sample backlog change, or absent interval bounds is missing
// evidence: the run is inconclusive, never a pass. It is never an error.
func evidenceFromFixture(raw json.RawMessage) (perfstats.RunEvidence, error) {
	if len(raw) == 0 {
		return perfstats.RunEvidence{}, errors.New("run evidence is required")
	}
	var record fixtureEvidence
	if err := json.Unmarshal(raw, &record); err != nil {
		return perfstats.RunEvidence{}, fmt.Errorf("decode run evidence: %w", err)
	}
	if record.FixtureVerdict == nil {
		return perfstats.RunEvidence{}, errors.New("fixture_verdict is required")
	}
	if record.Capped == nil || record.SendErrors == nil {
		return perfstats.RunEvidence{}, errors.New("capped and send_errors are required")
	}
	if record.Delivery == nil || record.Delivery.Missing == nil || record.Delivery.Duplicate == nil ||
		record.Delivery.Invalid == nil || record.Delivery.Reordered == nil || record.Delivery.LateAfterStop == nil {
		return perfstats.RunEvidence{}, errors.New("delivery counters are required")
	}

	evidence := perfstats.RunEvidence{
		FixtureValid: *record.FixtureVerdict == "pass",
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
			return perfstats.RunEvidence{}, errors.New("echo runs require capped and deadline_exceeded counters")
		}
		evidence.Counters.Capped += *record.Echo.Capped
		evidence.Counters.DeadlineExceeded = *record.Echo.DeadlineExceeded
	}

	window := record.SenderWindow
	if window == nil || window.Status == nil || *window.Status != "bounded" ||
		window.BacklogChange == nil || window.BacklogChange.Status == "insufficient-samples" ||
		window.BacklogChange.SampleCount < 8 ||
		window.BacklogChange.Lower == nil || window.BacklogChange.Upper == nil {
		return evidence, nil
	}
	evidence.Interval = &perfstats.BacklogInterval{
		Lower: *window.BacklogChange.Lower,
		Upper: *window.BacklogChange.Upper,
	}
	return evidence, nil
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
